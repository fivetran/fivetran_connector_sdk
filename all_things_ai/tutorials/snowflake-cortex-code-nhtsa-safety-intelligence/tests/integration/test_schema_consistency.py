"""Schema consistency tests — README columns ↔ upsert_* emitted keys.

Catches the documentation drift bug: a column added to upsert_*() but not
to the README, or vice versa. PR review is unreliable for this — humans
miss schema changes that don't change behavior. This test makes the parity
contract explicit.

For each of the five tables, parse the README's column section and compare
to the keys actually emitted by the corresponding upsert_*() helper.
"""

import re
from pathlib import Path

import pytest

import connector

README_PATH = Path(__file__).parent.parent.parent / "README.md"


def _extract_table_columns(table_name):
    """Extract backtick-delimited column names from a `### TABLE_NAME` README
    section. Returns a set of column names."""
    text = README_PATH.read_text()
    section = re.search(
        rf"^### {re.escape(table_name)}\s*\n(.*?)(?=^### |\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    if not section:
        return set()
    body = section.group(1)
    # Match table rows: `column_name` ... — pick the first backticked identifier per line.
    return set(re.findall(r"^\|\s*`([a-z_][a-z0-9_]*)`\s*\|", body, re.MULTILINE))


@pytest.fixture
def captured_for_each_table(captured_upserts):
    """Helper that returns the set of column keys for a given table after
    captured_upserts has been populated."""

    def for_table(table_name):
        cols = set()
        for u in captured_upserts["upserts"]:
            if u["table"] == table_name:
                cols.update(u["data"].keys())
        return cols

    return for_table


class TestRecallsSchemaParity:
    def test_recalls_columns_match_readme(self, captured_upserts, captured_for_each_table):
        # Provide a maximally-populated record so every key is exercised
        record = {
            "NHTSACampaignNumber": "22V001000",
            "Manufacturer": "Toyota",
            "Make": "Toyota",
            "Model": "Tundra",
            "ModelYear": "2022",
            "Component": "Brakes",
            "Summary": "x",
            "Consequence": "x",
            "Remedy": "x",
            "Notes": "x",
            "ReportReceivedDate": "01/01/2022",
            "parkIt": False,
            "parkOutSide": False,
            "overTheAirUpdate": False,
            "NHTSAActionNumber": "EA-22-001",
        }
        connector.upsert_recalls([record], "Toyota", "Tundra", "2022", "seed")

        code_columns = captured_for_each_table("recalls")
        readme_columns = _extract_table_columns("RECALLS")

        missing_from_readme = code_columns - readme_columns
        missing_from_code = readme_columns - code_columns

        assert (
            not missing_from_readme
        ), f"Code emits columns missing from README: {missing_from_readme}"
        assert not missing_from_code, f"README declares columns not in code: {missing_from_code}"


class TestComplaintsSchemaParity:
    def test_complaints_columns_match_readme(self, captured_upserts, captured_for_each_table):
        record = {
            "odiNumber": "10000001",
            "manufacturer": "Toyota",
            "crash": "No",
            "fire": "No",
            "numberOfInjuries": 0,
            "numberOfDeaths": 0,
            "dateOfIncident": "20220101",
            "dateComplaintFiled": "20220115",
            "vin": "TESTVIN1234567890",
            "components": "brakes",
            "summary": "x",
            "products": [],
        }
        connector.upsert_complaints([record], "seed")

        code_columns = captured_for_each_table("complaints")
        readme_columns = _extract_table_columns("COMPLAINTS")

        missing_from_readme = code_columns - readme_columns
        missing_from_code = readme_columns - code_columns

        assert (
            not missing_from_readme
        ), f"Code emits columns missing from README: {missing_from_readme}"
        assert not missing_from_code, f"README declares columns not in code: {missing_from_code}"


class TestVehicleSpecsSchemaParity:
    def test_vehicle_specs_columns_match_readme(self, captured_upserts, captured_for_each_table):
        record = {
            "Make_ID": 448,
            "Make_Name": "Toyota",
            "Model_ID": 1000,
            "Model_Name": "Tundra",
        }
        connector.upsert_vehicle_specs([record], "seed")

        code_columns = captured_for_each_table("vehicle_specs")
        readme_columns = _extract_table_columns("VEHICLE_SPECS")

        missing_from_readme = code_columns - readme_columns
        missing_from_code = readme_columns - code_columns

        assert (
            not missing_from_readme
        ), f"Code emits columns missing from README: {missing_from_readme}"
        assert not missing_from_code, f"README declares columns not in code: {missing_from_code}"


class TestDiscoveryInsightsSchemaParity:
    """discovery_insights and safety_analysis are emitted via op.upsert()
    inside run_discovery_phase / run_synthesis_phase, not via a separate
    helper. Walk the in-line dictionary literal in run_discovery_phase to
    derive the columns.

    This test runs run_discovery_phase with a stubbed call_cortex_agent and
    captures the emitted insight record."""

    def test_discovery_insights_columns_match_readme(
        self,
        base_config,
        captured_upserts,
        captured_for_each_table,
        monkeypatch,
        sample_discovery_response,
        recall_batch,
        complaint_batch,
    ):
        monkeypatch.setattr(
            connector, "call_cortex_agent", lambda *a, **kw: sample_discovery_response
        )
        monkeypatch.setattr(connector, "fetch_recalls", lambda *a, **kw: [])
        monkeypatch.setattr(connector, "fetch_complaints", lambda *a, **kw: [])
        monkeypatch.setattr(connector, "fetch_vehicle_specs", lambda *a, **kw: [])

        connector.run_discovery_phase(
            connector.create_session(),
            base_config,
            recall_batch,
            complaint_batch,
            {},
        )

        code_columns = captured_for_each_table("discovery_insights")
        readme_columns = _extract_table_columns("DISCOVERY_INSIGHTS")

        missing_from_readme = code_columns - readme_columns
        missing_from_code = readme_columns - code_columns

        assert (
            not missing_from_readme
        ), f"Code emits columns missing from README: {missing_from_readme}"
        assert not missing_from_code, f"README declares columns not in code: {missing_from_code}"


class TestSafetyAnalysisSchemaParity:
    def test_safety_analysis_columns_match_readme(
        self,
        base_config,
        captured_upserts,
        captured_for_each_table,
        monkeypatch,
        sample_synthesis_response,
    ):
        monkeypatch.setattr(
            connector, "call_cortex_agent", lambda *a, **kw: sample_synthesis_response
        )

        aggregates = connector._build_aggregates({}, {}, 0, 0, 10, 20)
        connector.run_synthesis_phase(
            base_config,
            [("Toyota", "Tundra", "2022"), ("Ford", "F-150", "2022")],
            aggregates,
            {},
        )

        code_columns = captured_for_each_table("safety_analysis")
        readme_columns = _extract_table_columns("SAFETY_ANALYSIS")

        missing_from_readme = code_columns - readme_columns
        missing_from_code = readme_columns - code_columns

        assert (
            not missing_from_readme
        ), f"Code emits columns missing from README: {missing_from_readme}"
        assert not missing_from_code, f"README declares columns not in code: {missing_from_code}"
