"""Schema-drift tests: README declared schema vs connector's actual output.

These tests parse the README's per-table column lists and compare them to
the keys returned by the record-builder functions. Drift in either direction
(column in code but not docs, or column in docs but not code) is a bug.

Catches the class of issue raised by reviewers on PR #570 where
README WEATHER_EVENTS was missing status/message_type/category columns
that build_event_record() actually emits.
"""

import re
from pathlib import Path

import connector


README_PATH = Path(__file__).parent.parent.parent / "README.md"


def _extract_table_columns(table_section_name):
    """Parse README and return the set of column names listed under the
    `### TABLE_NAME` heading."""
    text = README_PATH.read_text()
    section_re = re.compile(
        rf"^### {re.escape(table_section_name)}\s*\n(.*?)(?=^###|\Z)",
        re.MULTILINE | re.DOTALL,
    )
    match = section_re.search(text)
    if not match:
        return set()
    body = match.group(1)
    # Match `- `column_name` (TYPE...): description`
    column_re = re.compile(r"^\s*-\s+`([a-z_][a-z0-9_]*)`\s*\(", re.MULTILINE)
    return set(column_re.findall(body))


class TestWeatherEventsSchemaParity:
    """The set of columns build_event_record() emits must match the
    README WEATHER_EVENTS schema exactly. Either direction of drift
    is a bug."""

    def test_build_event_record_columns_match_readme(self, minimal_noaa_alert_feature):
        record = connector.build_event_record(minimal_noaa_alert_feature, "seed")
        code_columns = set(record.keys())
        readme_columns = _extract_table_columns("WEATHER_EVENTS")

        in_code_not_readme = code_columns - readme_columns
        in_readme_not_code = readme_columns - code_columns

        assert not in_code_not_readme, (
            f"build_event_record emits columns missing from README: "
            f"{sorted(in_code_not_readme)}"
        )
        assert not in_readme_not_code, (
            f"README declares columns not emitted by build_event_record: "
            f"{sorted(in_readme_not_code)}"
        )
