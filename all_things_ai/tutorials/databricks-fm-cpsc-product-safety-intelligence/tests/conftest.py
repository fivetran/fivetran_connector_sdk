"""Shared pytest fixtures for CPSC Product Safety Intelligence tests."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from fivetran_connector_sdk import Logging as _sdk_logging  # noqa: E402

if _sdk_logging.LOG_LEVEL is None:
    _sdk_logging.LOG_LEVEL = _sdk_logging.Level.INFO


@pytest.fixture
def base_config():
    return {
        "databricks_workspace_url": "https://example.cloud.databricks.com",
        "databricks_token": "dapi_test_token",
        "databricks_warehouse_id": "warehouse_test",
        "databricks_model": "databricks-claude-sonnet-4-6",
        "databricks_timeout": "120",
        "enable_enrichment": "false",
        "enable_genie_space": "false",
        "max_recalls": "10",
        "max_enrichments": "5",
        "lookback_days": "90",
    }


@pytest.fixture
def captured_upserts(monkeypatch):
    import connector

    captured = {"upserts": [], "checkpoints": []}
    monkeypatch.setattr(
        connector.op, "upsert", lambda t, d: captured["upserts"].append({"table": t, "data": d})
    )
    monkeypatch.setattr(
        connector.op,
        "checkpoint",
        lambda state: captured["checkpoints"].append(dict(state) if state else {}),
    )
    return captured


@pytest.fixture
def minimal_recall():
    """Minimal CPSC recall record matching build_recall_record() input shape."""
    return {
        "RecallID": 12345,
        "RecallNumber": "24-001",
        "RecallDate": "2024-03-10",
        "Description": "Some safety hazard requiring corrective action.",
        "URL": "https://www.cpsc.gov/recalls/2024/24-001",
        "Title": "Test Recall Product",
        "ConsumerContact": "1-800-555-0100",
        "LastPublishDateTime": "2024-03-10T10:00:00",
        "Products": [{"Name": "Widget", "Description": "Test widget"}],
        "Manufacturers": [{"Name": "TestCo"}],
        "Hazards": [{"Name": "Burn Hazard"}],
        "Remedies": [{"Name": "Replacement"}],
        "Injuries": [],
        "RemedyOptions": [],
        "Retailers": [{"Name": "TestMart"}],
        "ManufacturerCountries": [{"Country": "USA"}],
        "ProductUPCs": [{"UPC": "012345678905"}],
        "Inconjunctions": [],
        "Images": [],
    }
