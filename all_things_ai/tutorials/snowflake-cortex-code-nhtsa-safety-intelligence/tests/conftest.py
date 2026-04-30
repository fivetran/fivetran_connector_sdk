"""Shared pytest fixtures for the NHTSA Snowflake Cortex connector tests.

Cortex Agent pattern (PAT auth + SSE streaming response). Mirrors the Best Buy
Databricks conftest with NHTSA-specific deltas: three NHTSA endpoints
(recalls/complaints/vehicle specs), vPIC make-in-path URL shape, and the
discovery → synthesis two-call Cortex flow gated by max_enrichments.
"""

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
        "seed_make": "Toyota",
        "seed_model": "Tundra",
        "seed_year": "2022",
        "discovery_depth": "2",
        "max_discoveries": "3",
        "max_enrichments": "3",
        "enable_cortex": "true",
        "snowflake_account": "abc12345.snowflakecomputing.com",
        "snowflake_pat_token": "snowflake_pat_test_token_abc",
        "cortex_model": "claude-sonnet-4-6",
        "cortex_timeout": "60",
    }


def make_recall(campaign_number, component="Brakes", make="Toyota", model="Tundra", year="2022"):
    """Build a minimal raw NHTSA recall record (capitalized API field names)."""
    return {
        "NHTSACampaignNumber": campaign_number,
        "Manufacturer": "Toyota Motor Corporation",
        "Make": make,
        "Model": model,
        "ModelYear": year,
        "Component": component,
        "Summary": "Test summary.",
        "Consequence": "Test consequence.",
        "Remedy": "Test remedy.",
        "Notes": None,
        "ReportReceivedDate": "01/01/2022",
        "parkIt": False,
        "parkOutSide": False,
        "overTheAirUpdate": False,
        "NHTSAActionNumber": None,
    }


def make_complaint(odi_number, components="brakes", crash="No", fire="No"):
    """Build a minimal raw NHTSA complaint record (lowercased API field names)."""
    return {
        "odiNumber": odi_number,
        "manufacturer": "Toyota Motor Corporation",
        "crash": crash,
        "fire": fire,
        "numberOfInjuries": 0,
        "numberOfDeaths": 0,
        "dateOfIncident": "20220101",
        "dateComplaintFiled": "20220115",
        "vin": "TESTVIN1234567890",
        "components": components,
        "summary": "Test complaint summary.",
        "products": [],
    }


def make_vehicle_spec(make_id, model_id, make_name="Toyota", model_name="Tundra"):
    """Build a minimal raw vPIC GetModelsForMake record (capitalized API field names)."""
    return {
        "Make_ID": make_id,
        "Make_Name": make_name,
        "Model_ID": model_id,
        "Model_Name": model_name,
    }


@pytest.fixture
def recall_batch():
    return [
        make_recall(f"22V{i:03d}000", component=("Brakes" if i % 2 == 0 else "Engine"))
        for i in range(5)
    ]


@pytest.fixture
def complaint_batch():
    return [
        make_complaint(f"100{i:04d}", components=("brakes" if i % 2 == 0 else "engine"))
        for i in range(5)
    ]


@pytest.fixture
def spec_batch():
    return [make_vehicle_spec(448 + i, 1000 + i, model_name=f"Model{i}") for i in range(5)]


@pytest.fixture
def sample_discovery_response():
    """Canonical Cortex Agent JSON content for the discovery prompt."""
    return {
        "top_components": [
            {"component": "Brakes", "recall_count": 3, "complaint_count": 2, "severity": "HIGH"},
        ],
        "severity_score": 7,
        "crash_count": 1,
        "fire_count": 0,
        "recommended_vehicles": [
            {"make": "Toyota", "model": "Sequoia", "year": "2022", "reason": "shared platform"},
            {"make": "Toyota", "model": "Tundra", "year": "2023", "reason": "adjacent year"},
            {"make": "Ford", "model": "F-150", "year": "2022", "reason": "competitor in class"},
        ],
        "analysis_summary": "Brake-related issues recurring.",
    }


@pytest.fixture
def sample_synthesis_response():
    """Canonical Cortex Agent JSON content for the synthesis prompt."""
    return {
        "component_risk_rankings": [
            {"component": "Brakes", "affected_vehicles": 3, "total_recalls": 9, "risk_score": 8},
        ],
        "manufacturer_response_score": 7,
        "systemic_issues": [
            {
                "issue": "Brake firmware regression",
                "affected_makes": ["Toyota"],
                "severity": "HIGH",
            },
        ],
        "fleet_safety_grade": "C+",
        "executive_summary": "Brake reliability needs review.",
    }


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
