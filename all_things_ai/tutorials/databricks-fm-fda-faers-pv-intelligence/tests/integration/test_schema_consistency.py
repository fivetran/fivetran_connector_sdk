"""Schema-drift tests: README declared schema vs connector's actual output.

Parses each per-table column list in README.md and compares to the keys
returned by build_event_record() and the JSON shapes the prompt builders
declare.

# see databricks-fm-noaa-weather-risk-intelligence/tests/integration/
# test_schema_consistency.py for the analogous pattern.
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
    column_re = re.compile(r"^\s*-\s+`([a-z_][a-z0-9_]*)`\s*\(", re.MULTILINE)
    return set(column_re.findall(body))


class TestAdverseEventsSchemaParity:
    """build_event_record() output keys must match README ADVERSE_EVENTS."""

    def test_build_event_record_columns_match_readme(self, minimal_faers_event):
        record = connector.build_event_record(minimal_faers_event)
        code_columns = set(record.keys())
        readme_columns = _extract_table_columns("ADVERSE_EVENTS")

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


class TestPromptDeclaredColumnsAlign:
    """The prompt JSON keys the connector tells the LLM to return must
    appear in the corresponding README assessment table."""

    def test_safety_prompt_keys_in_safety_assessments_readme(self, minimal_faers_event):
        prompt = connector.build_safety_prompt(connector.build_event_record(minimal_faers_event))
        readme_columns = _extract_table_columns("SAFETY_ASSESSMENTS")
        for key in (
            "safety_signal_score",
            "signal_classification",
            "escalation_recommendation",
            "drug_interaction_concern",
            "vulnerable_population_flag",
            "reasoning",
        ):
            assert key in prompt, f"Safety prompt missing key {key!r}"
            assert (
                key in readme_columns
            ), f"Safety prompt asks for {key!r} but README does not declare it"

    def test_clinical_prompt_keys_in_clinical_assessments_readme(self, minimal_faers_event):
        prompt = connector.build_clinical_prompt(connector.build_event_record(minimal_faers_event))
        readme_columns = _extract_table_columns("CLINICAL_ASSESSMENTS")
        for key in (
            "clinical_risk_score",
            "causality_assessment",
            "expected_for_drug_class",
            "concomitant_medication_concern",
            "reporting_quality",
            "reasoning",
        ):
            assert key in prompt, f"Clinical prompt missing key {key!r}"
            assert (
                key in readme_columns
            ), f"Clinical prompt asks for {key!r} but README does not declare it"

    def test_consensus_prompt_keys_in_consensus_readme(
        self, minimal_faers_event, sample_safety_response, sample_clinical_response
    ):
        import json

        prompt = connector.build_consensus_prompt(
            connector.build_event_record(minimal_faers_event),
            json.loads(sample_safety_response),
            json.loads(sample_clinical_response),
        )
        readme_columns = _extract_table_columns("DEBATE_CONSENSUS")
        for key in (
            "final_severity",
            "consensus_risk_score",
            "debate_winner",
            "winner_rationale",
            "agreement_areas",
            "disagreement_areas",
            "disagreement_flag",
            "disagreement_severity",
            "pv_review_recommended",
            "recommended_action",
            "executive_summary",
        ):
            assert key in prompt, f"Consensus prompt missing key {key!r}"
            assert (
                key in readme_columns
            ), f"Consensus prompt asks for {key!r} but README does not declare it"


class TestConfigKeysDocumented:
    """Every key in configuration.json must be documented in README."""

    def test_all_config_keys_in_readme(self):
        import json as _json

        cfg_path = Path(__file__).parent.parent.parent / "configuration.json"
        cfg = _json.loads(cfg_path.read_text())
        readme = README_PATH.read_text()
        missing = [k for k in cfg if k not in readme]
        assert not missing, f"Config keys missing from README: {missing}"
