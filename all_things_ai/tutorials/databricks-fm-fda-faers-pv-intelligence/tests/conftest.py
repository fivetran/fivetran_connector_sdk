"""Shared pytest fixtures for FDA FAERS Pharmacovigilance connector tests.

Mirrors the harness pattern from databricks-fm-noaa-weather-risk-intelligence
and databricks-fm-fda-drug-label-intelligence — same Databricks ai_query()
pattern, FAERS-specific event/state shape.
"""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

# Initialize SDK Logging level so log.info / log.warning work outside the
# fivetran debug harness.
from fivetran_connector_sdk import Logging as _sdk_logging  # noqa: E402

if _sdk_logging.LOG_LEVEL is None:
    _sdk_logging.LOG_LEVEL = _sdk_logging.Level.INFO


@pytest.fixture
def base_config():
    """Known-good configuration with both phases enabled and non-placeholder
    Databricks credentials. Individual tests override fields as needed."""
    return {
        "databricks_workspace_url": "https://example.cloud.databricks.com",
        "databricks_token": "dapi_test_token_abc123",
        "databricks_warehouse_id": "warehouse_test_id",
        "databricks_model": "databricks-claude-sonnet-4-6",
        "databricks_timeout": "120",
        "enable_enrichment": "true",
        "enable_genie_space": "false",
        "genie_table_identifier": "<OPTIONAL_GENIE_TABLE_IDENTIFIER>",
        "search_drug": "<DRUG_NAME_OR_EMPTY>",
        "max_events": "10",
        "max_enrichments": "5",
        "lookback_days": "30",
    }


def make_faers_event(report_id, receive_date, serious=True, drug="Lipitor", reaction="Headache"):
    """Build a minimal raw OpenFDA FAERS event matching the structure
    fetch_adverse_events returns and build_event_record consumes."""
    return {
        "safetyreportid": report_id,
        "receivedate": receive_date,
        "serious": "1" if serious else "2",
        "seriousnessdeath": "0",
        "seriousnesshospitalization": "1" if serious else "0",
        "seriousnesslifethreatening": "0",
        "seriousnessdisabling": "0",
        "reporttype": "1",
        "occurcountry": "US",
        "sender": {"senderorganization": "FDA-PUBLIC USE"},
        "patient": {
            "patientonsetage": "55",
            "patientonsetageunit": "801",
            "patientsex": "1",
            "patientweight": "75",
            "drug": [{"medicinalproduct": drug}],
            "reaction": [{"reactionmeddrapt": reaction}],
        },
    }


@pytest.fixture
def minimal_faers_event():
    return make_faers_event("RPT-001", "20260415")


@pytest.fixture
def faers_events_batch():
    """A batch of 10 serious FAERS events with sequential receive dates."""
    return [make_faers_event(f"RPT-{i:03d}", f"2026041{i % 10}", serious=True) for i in range(10)]


@pytest.fixture
def sample_safety_response():
    return json.dumps(
        {
            "safety_signal_score": 8,
            "signal_classification": "POTENTIAL_SIGNAL",
            "escalation_recommendation": "ROUTINE",
            "drug_interaction_concern": True,
            "vulnerable_population_flag": False,
            "reasoning": "Reaction not previously documented for this drug class.",
        }
    )


@pytest.fixture
def sample_clinical_response():
    return json.dumps(
        {
            "clinical_risk_score": 5,
            "causality_assessment": "POSSIBLE",
            "expected_for_drug_class": True,
            "concomitant_medication_concern": False,
            "reporting_quality": "MEDIUM",
            "reasoning": "Reaction is consistent with known drug class profile.",
        }
    )


@pytest.fixture
def sample_consensus_response():
    return json.dumps(
        {
            "final_severity": "MEDIUM",
            "consensus_risk_score": 6,
            "debate_winner": "CLINICAL",
            "winner_rationale": "Clinical context outweighs the risk-maximizing read.",
            "agreement_areas": ["seriousness classification"],
            "disagreement_areas": ["signal novelty"],
            "disagreement_flag": True,
            "disagreement_severity": "SIGNIFICANT",
            "pv_review_recommended": True,
            "recommended_action": "Schedule for PV team review",
            "executive_summary": "Disagreement on signal novelty warrants review.",
        }
    )


@pytest.fixture
def captured_upserts(monkeypatch):
    """Patch op.upsert and op.checkpoint to no-ops so update() can run
    outside the debug harness. Yields the captured records for assertions."""
    import connector

    captured = {"upserts": [], "checkpoints": []}

    def fake_upsert(table, data):
        captured["upserts"].append({"table": table, "data": data})

    def fake_checkpoint(state):
        captured["checkpoints"].append(dict(state) if state else {})

    monkeypatch.setattr(connector.op, "upsert", fake_upsert)
    monkeypatch.setattr(connector.op, "checkpoint", fake_checkpoint)
    monkeypatch.setattr(connector.time, "sleep", lambda *_: None)
    return captured
