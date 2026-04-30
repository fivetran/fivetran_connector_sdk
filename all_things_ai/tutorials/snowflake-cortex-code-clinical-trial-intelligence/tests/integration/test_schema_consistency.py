"""Schema-drift tests: README ↔ build_trial_record() parity.

# see snowflake-cortex-code-nvd-cve-threat-intelligence/tests/integration/test_schema_consistency.py
# for the canonical pattern.
"""

import re
from pathlib import Path

import connector

README_PATH = Path(__file__).parent.parent.parent / "README.md"


def _extract_table_columns(table_name):
    text = README_PATH.read_text()
    section_re = re.compile(
        rf"^### {re.escape(table_name)}\s*\n(.*?)(?=^###|\Z)",
        re.MULTILINE | re.DOTALL,
    )
    match = section_re.search(text)
    if not match:
        return set()
    return set(re.findall(r"^\s*-\s+`([a-z_][a-z0-9_]*)`\s*\(", match.group(1), re.MULTILINE))


class TestTrialsSchemaParity:
    def test_build_trial_record_columns_match_readme(self, minimal_trial):
        record = connector.build_trial_record(minimal_trial)
        code_columns = set(record.keys())
        readme_columns = _extract_table_columns("TRIALS")

        in_code_not_readme = code_columns - readme_columns
        in_readme_not_code = readme_columns - code_columns

        assert (
            not in_code_not_readme
        ), f"build_trial_record emits columns missing from README: {sorted(in_code_not_readme)}"
        assert (
            not in_readme_not_code
        ), f"README declares columns not emitted by build_trial_record: {sorted(in_readme_not_code)}"


class TestPromptDeclaredColumnsAlign:
    def test_optimist_prompt_keys_in_readme(self, minimal_trial):
        prompt = connector.build_optimist_prompt(connector.build_trial_record(minimal_trial))
        readme_columns = _extract_table_columns("OPTIMIST_ASSESSMENTS")
        for key in (
            "design_strength_score",
            "sample_size_assessment",
            "randomization_quality",
            "blinding_quality",
            "endpoint_quality",
            "sponsor_strength",
            "success_probability",
            "key_strengths",
            "reasoning",
        ):
            assert key in prompt, f"Optimist prompt missing key {key!r}"
            assert (
                key in readme_columns
            ), f"Optimist prompt asks for {key!r} but README does not declare it"

    def test_skeptic_prompt_keys_in_readme(self, minimal_trial):
        prompt = connector.build_skeptic_prompt(connector.build_trial_record(minimal_trial))
        readme_columns = _extract_table_columns("SKEPTIC_ASSESSMENTS")
        for key in (
            "methodology_risk_score",
            "dropout_risk",
            "endpoint_concerns",
            "statistical_power_risk",
            "regulatory_hurdle_risk",
            "recruitment_feasibility",
            "failure_probability",
            "key_risks",
            "reasoning",
        ):
            assert key in prompt, f"Skeptic prompt missing key {key!r}"
            assert (
                key in readme_columns
            ), f"Skeptic prompt asks for {key!r} but README does not declare it"

    def test_consensus_prompt_keys_in_readme(
        self, minimal_trial, sample_optimist_response, sample_skeptic_response
    ):
        prompt = connector.build_consensus_prompt(
            connector.build_trial_record(minimal_trial),
            sample_optimist_response,
            sample_skeptic_response,
        )
        readme_columns = _extract_table_columns("DEBATE_CONSENSUS")
        for key in (
            "overall_quality",
            "consensus_score",
            "debate_winner",
            "winner_rationale",
            "agreement_areas",
            "disagreement_areas",
            "disagreement_flag",
            "disagreement_severity",
            "human_review_recommended",
            "monitoring_priority",
            "recommended_action",
            "executive_summary",
        ):
            assert key in prompt, f"Consensus prompt missing key {key!r}"
            assert (
                key in readme_columns
            ), f"Consensus prompt asks for {key!r} but README does not declare it"


class TestConfigKeysDocumented:
    def test_all_config_keys_in_readme(self):
        import json as _json

        cfg = _json.loads((Path(__file__).parent.parent.parent / "configuration.json").read_text())
        readme = README_PATH.read_text()
        missing = [k for k in cfg if k not in readme]
        assert not missing, f"Config keys missing from README: {missing}"
