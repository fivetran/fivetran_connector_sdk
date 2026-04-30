"""Shared pytest fixtures for Clinical Trial Snowflake Cortex connector tests.

Cortex pattern (PAT auth + SSE streaming + multi-agent debate). Mirrors the
NVD CVE conftest with #566-specific deltas: composite cursor (date,
nct_ids_at_date), pageToken pagination, and the 5-table schema (trials +
discovery_insights + optimist_assessments + skeptic_assessments +
debate_consensus).

# see snowflake-cortex-code-nvd-cve-threat-intelligence/tests/conftest.py for
# the canonical Cortex-side analogue with start_index pagination.
"""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fivetran_connector_sdk import Logging as _sdk_logging  # noqa: E402

if _sdk_logging.LOG_LEVEL is None:
    _sdk_logging.LOG_LEVEL = _sdk_logging.Level.INFO


@pytest.fixture
def base_config():
    """Known-good configuration with Cortex enabled and non-placeholder Snowflake creds."""
    return {
        "condition": "atrial fibrillation",
        "status_filter": "<OVERALL_STATUS>",
        "intervention_filter": "<INTERVENTION_SEARCH>",
        "sponsor_filter": "<SPONSOR_SEARCH>",
        "max_seed_trials": "10",
        "max_discoveries": "5",
        "max_debates": "3",
        "page_size": "20",
        "enable_cortex": "true",
        "snowflake_account": "abc12345.snowflakecomputing.com",
        "snowflake_pat_token": "snowflake_pat_test_abc123",
        "cortex_model": "claude-sonnet-4-6",
        "cortex_timeout": "60",
    }


def make_trial(
    nct_id, last_update_post_date, brief_title="Test Trial", overall_status="RECRUITING"
):
    """Build a minimal raw ClinicalTrials.gov v2 study record."""
    return {
        "protocolSection": {
            "identificationModule": {
                "nctId": nct_id,
                "briefTitle": brief_title,
                "officialTitle": f"Official: {brief_title}",
            },
            "statusModule": {
                "overallStatus": overall_status,
                "lastUpdatePostDateStruct": {"date": last_update_post_date},
                "startDateStruct": {"date": "2025-01-01"},
                "completionDateStruct": {"date": "2027-12-31"},
            },
            "sponsorCollaboratorsModule": {
                "leadSponsor": {"name": "Test Sponsor", "class": "INDUSTRY"},
            },
            "descriptionModule": {"briefSummary": "Brief summary text"},
            "conditionsModule": {"conditions": ["Atrial Fibrillation"], "keywords": ["AF"]},
            "designModule": {
                "studyType": "INTERVENTIONAL",
                "phases": ["PHASE3"],
                "designInfo": {
                    "allocation": "RANDOMIZED",
                    "interventionModel": "PARALLEL",
                    "primaryPurpose": "TREATMENT",
                    "maskingInfo": {"masking": "DOUBLE"},
                },
                "enrollmentInfo": {"count": 500, "type": "ESTIMATED"},
            },
            "eligibilityModule": {
                "eligibilityCriteria": "Adults",
                "sex": "ALL",
                "minimumAge": "18 Years",
                "maximumAge": "85 Years",
                "healthyVolunteers": False,
            },
            "armsInterventionsModule": {
                "armGroups": [{"label": "Treatment"}],
                "interventions": [{"type": "DRUG", "name": "TestDrug"}],
            },
        },
        "hasResults": False,
    }


@pytest.fixture
def minimal_trial():
    return make_trial("NCT06000001", "2026-04-15")


@pytest.fixture
def trial_batch():
    """10 trials with sequential lastUpdatePostDate values."""
    return [make_trial(f"NCT0600{i:04d}", f"2026-04-{(i % 28) + 1:02d}") for i in range(10)]


@pytest.fixture
def sample_optimist_response():
    return {
        "optimist_score": 8,
        "design_strengths": ["Adequate sample size", "Double-blind"],
        "endpoint_quality": "STRONG",
        "sponsor_track_record": "ESTABLISHED",
        "success_likelihood": "HIGH",
        "reasoning": "Well-designed Phase 3 with established sponsor.",
    }


@pytest.fixture
def sample_skeptic_response():
    return {
        "skeptic_score": 5,
        "methodology_risks": ["Dropout potential", "Endpoint selection"],
        "statistical_power_concerns": "MODERATE",
        "regulatory_hurdles": ["Endpoint pre-specification"],
        "failure_likelihood": "MEDIUM",
        "reasoning": "Standard Phase 3 risks present.",
    }


@pytest.fixture
def sample_consensus_response():
    return {
        "final_priority": "HIGH",
        "consensus_score": 7,
        "debate_winner": "OPTIMIST",
        "winner_rationale": "Design strengths outweigh standard risks.",
        "agreement_areas": ["sample size adequacy"],
        "disagreement_areas": ["endpoint quality"],
        "disagreement_flag": True,
        "disagreement_severity": "SIGNIFICANT",
        "human_review_recommended": True,
        "recommended_action": "Monitor closely",
        "executive_summary": "High priority with significant analyst disagreement.",
    }


def build_sse_lines(payload_dict):
    """Encode a JSON payload as the SSE byte-line stream the Cortex endpoint emits."""
    body = json.dumps(payload_dict)
    return [
        f"data: {json.dumps({'choices': [{'delta': {'content': body}}]})}".encode("utf-8"),
        b"event: done",
    ]


@pytest.fixture
def captured_upserts(monkeypatch):
    """Patch op.upsert and op.checkpoint to no-ops; yield captured records."""
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
