"""Schema consistency tests — README ↔ build_recall_record + assessment tables."""

import re
from pathlib import Path

import connector

README_PATH = Path(__file__).parent.parent.parent / "README.md"


def _extract_table_columns(table_name):
    text = README_PATH.read_text()
    section = re.search(
        rf"^### {re.escape(table_name)}\s*\n(.*?)(?=^### |\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    if not section:
        return set()
    body = section.group(1)
    return set(re.findall(r"^\s*-\s+`([a-z_][a-z0-9_]*)`\s*\(", body, re.MULTILINE))


def _capture_upsert(monkeypatch):
    captured = []

    def fake_upsert(table=None, data=None, **_):
        captured.append({"table": table, "data": data})

    monkeypatch.setattr(connector.op, "upsert", fake_upsert)
    return captured


class TestProductRecallsSchemaParity:
    def test_columns_match_readme(self, minimal_recall):
        record = connector.build_recall_record(minimal_recall)
        code_columns = set(record.keys())
        readme_columns = _extract_table_columns("PRODUCT_RECALLS")

        in_code_not_readme = code_columns - readme_columns
        in_readme_not_code = readme_columns - code_columns

        assert (
            not in_code_not_readme
        ), f"Code emits columns missing from README: {sorted(in_code_not_readme)}"
        assert (
            not in_readme_not_code
        ), f"README declares columns not emitted: {sorted(in_readme_not_code)}"


class TestAssessmentTablesSchemaParity:
    def test_safety_assessments_columns_match_readme(self, monkeypatch):
        captured = _capture_upsert(monkeypatch)
        safety = {
            "safety_risk_score": 9,
            "worst_case_scenario": "Severe burns affecting children",
            "defect_classification": "SYSTEMIC_DESIGN",
            "consumer_urgency": "STOP_USE_IMMEDIATELY",
            "vulnerable_population_risk": True,
            "reasoning": "Hazard pattern matches prior recalls",
        }
        connector.upsert_assessment("safety_assessments", "RC-1", safety, "safety")
        code_columns = set(captured[0]["data"].keys())
        readme_columns = _extract_table_columns("SAFETY_ASSESSMENTS")
        assert (
            code_columns - readme_columns == set()
        ), f"safety_assessments emits unknown columns: {code_columns - readme_columns}"
        assert (
            readme_columns - code_columns == set()
        ), f"README has columns not emitted: {readme_columns - code_columns}"

    def test_quality_assessments_columns_match_readme(self, monkeypatch):
        captured = _capture_upsert(monkeypatch)
        quality = {
            "quality_risk_score": 6,
            "root_cause_assessment": "BATCH_DEFECT",
            "containment_feasibility": "FULLY_CONTAINED",
            "remedy_adequacy": "ADEQUATE",
            "recall_proportionality": "PROPORTIONATE",
            "reasoning": "Standard QC failure contained to one batch",
        }
        connector.upsert_assessment("quality_assessments", "RC-1", quality, "quality")
        code_columns = set(captured[0]["data"].keys())
        readme_columns = _extract_table_columns("QUALITY_ASSESSMENTS")
        assert code_columns - readme_columns == set()
        assert readme_columns - code_columns == set()

    def test_debate_consensus_columns_match_readme(self, monkeypatch):
        captured = _capture_upsert(monkeypatch)
        consensus = {
            "final_severity": "HIGH",
            "consensus_risk_score": 8,
            "debate_winner": "SAFETY",
            "winner_rationale": "Severity outweighed cost concerns",
            "agreement_areas": ["recall scope"],
            "disagreement_areas": ["timeline"],
            "disagreement_flag": True,
            "disagreement_severity": "MINOR",
            "human_review_recommended": False,
            "recommended_action": "Issue urgent recall",
            "executive_summary": "Recall is justified and urgent",
        }
        connector.upsert_assessment("debate_consensus", "RC-1", consensus, "consensus")
        code_columns = set(captured[0]["data"].keys())
        readme_columns = _extract_table_columns("DEBATE_CONSENSUS")
        assert code_columns - readme_columns == set()
        assert readme_columns - code_columns == set()


class TestNoSqlReservedKeywordsAsColumnNames:
    def test_recalls_no_reserved_keywords(self, minimal_recall):
        record = connector.build_recall_record(minimal_recall)
        sql_reserved = {"by", "from", "select", "where", "order", "group", "join", "table"}
        clashing = set(record.keys()) & sql_reserved
        assert not clashing, f"Column name(s) collide with SQL reserved keywords: {clashing}."
