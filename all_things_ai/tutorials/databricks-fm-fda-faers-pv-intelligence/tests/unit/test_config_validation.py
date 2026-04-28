"""Unit tests for validate_configuration().

Locks the README contract for every configurable field so reviewer feedback
on edge cases gets encoded as a regression test alongside the fix.
"""

import pytest

import connector


class TestNumericValidation:
    def test_valid_config_passes(self, base_config):
        connector.validate_configuration(base_config)

    def test_max_events_negative_rejected(self, base_config):
        base_config["max_events"] = "-1"
        with pytest.raises(ValueError, match="positive integer"):
            connector.validate_configuration(base_config)

    def test_max_events_non_numeric_rejected(self, base_config):
        base_config["max_events"] = "not-a-number"
        with pytest.raises(ValueError, match="positive integer"):
            connector.validate_configuration(base_config)

    def test_max_events_ceiling_enforced(self, base_config):
        base_config["max_events"] = str(getattr(connector, "__MAX_EVENTS_CEILING") + 1)
        with pytest.raises(ValueError, match="exceeds ceiling"):
            connector.validate_configuration(base_config)

    def test_max_enrichments_ceiling_enforced(self, base_config):
        base_config["max_enrichments"] = str(getattr(connector, "__MAX_ENRICHMENTS_CEILING") + 1)
        with pytest.raises(ValueError, match="exceeds"):
            connector.validate_configuration(base_config)


class TestDatabricksCredentialsRequired:
    def test_missing_workspace_url_rejected(self, base_config):
        base_config["databricks_workspace_url"] = "<WORKSPACE_URL>"
        with pytest.raises(ValueError, match="databricks_workspace_url"):
            connector.validate_configuration(base_config)

    def test_missing_token_rejected(self, base_config):
        base_config["databricks_token"] = "<TOKEN>"
        with pytest.raises(ValueError, match="databricks_token"):
            connector.validate_configuration(base_config)

    def test_missing_warehouse_id_rejected(self, base_config):
        base_config["databricks_warehouse_id"] = "<WAREHOUSE>"
        with pytest.raises(ValueError, match="databricks_warehouse_id"):
            connector.validate_configuration(base_config)

    def test_non_https_workspace_url_rejected(self, base_config):
        base_config["databricks_workspace_url"] = "http://insecure.example.com"
        with pytest.raises(ValueError, match="https"):
            connector.validate_configuration(base_config)


class TestGenieSpaceValidation:
    def test_genie_enabled_requires_table_identifier(self, base_config):
        base_config["enable_genie_space"] = "true"
        base_config["genie_table_identifier"] = "<TABLE_IDENTIFIER>"
        with pytest.raises(ValueError, match="genie_table_identifier"):
            connector.validate_configuration(base_config)

    def test_genie_enabled_with_real_identifier_passes(self, base_config):
        base_config["enable_genie_space"] = "true"
        base_config["genie_table_identifier"] = "catalog.schema.faers_events"
        connector.validate_configuration(base_config)


class TestDataOnlyMode:
    """When enable_enrichment=false and enable_genie_space=false, Databricks
    credentials should not be required."""

    def test_enrichment_off_no_creds_needed(self, base_config):
        base_config["enable_enrichment"] = "false"
        base_config["enable_genie_space"] = "false"
        base_config["databricks_workspace_url"] = "<WORKSPACE_URL>"
        base_config["databricks_token"] = "<TOKEN>"
        base_config["databricks_warehouse_id"] = "<WAREHOUSE>"
        connector.validate_configuration(base_config)

    def test_genie_on_still_requires_creds(self, base_config):
        base_config["enable_enrichment"] = "false"
        base_config["enable_genie_space"] = "true"
        base_config["genie_table_identifier"] = "catalog.schema.faers_events"
        base_config["databricks_token"] = "<TOKEN>"
        with pytest.raises(ValueError, match="databricks_token"):
            connector.validate_configuration(base_config)


class TestLookbackDaysValidation:
    """Sahil/Copilot finding #4 — lookback_days was unvalidated; bad values
    silently fell back to the default via _optional_int()."""

    def test_lookback_days_negative_rejected(self, base_config):
        base_config["lookback_days"] = "-7"
        with pytest.raises(ValueError, match="positive integer"):
            connector.validate_configuration(base_config)

    def test_lookback_days_zero_rejected(self, base_config):
        base_config["lookback_days"] = "0"
        with pytest.raises(ValueError, match="positive integer"):
            connector.validate_configuration(base_config)

    def test_lookback_days_non_numeric_rejected(self, base_config):
        base_config["lookback_days"] = "recent"
        with pytest.raises(ValueError, match="positive integer"):
            connector.validate_configuration(base_config)

    def test_lookback_days_valid_value_accepted(self, base_config):
        base_config["lookback_days"] = "60"
        connector.validate_configuration(base_config)


class TestDatabricksTimeoutValidation:
    """Sahil/Copilot finding #4 — databricks_timeout was unvalidated."""

    def test_databricks_timeout_negative_rejected(self, base_config):
        base_config["databricks_timeout"] = "-30"
        with pytest.raises(ValueError, match="positive integer"):
            connector.validate_configuration(base_config)

    def test_databricks_timeout_zero_rejected(self, base_config):
        base_config["databricks_timeout"] = "0"
        with pytest.raises(ValueError, match="positive integer"):
            connector.validate_configuration(base_config)

    def test_databricks_timeout_non_numeric_rejected(self, base_config):
        base_config["databricks_timeout"] = "soon"
        with pytest.raises(ValueError, match="positive integer"):
            connector.validate_configuration(base_config)

    def test_databricks_timeout_valid_value_accepted(self, base_config):
        base_config["databricks_timeout"] = "60"
        connector.validate_configuration(base_config)


class TestBooleanFlagValidation:
    """Sahil/Copilot finding #4 — enable_enrichment and enable_genie_space
    silently fell back to defaults via _parse_bool() when set to invalid
    strings, hiding misconfiguration."""

    def test_enable_enrichment_invalid_string_rejected(self, base_config):
        base_config["enable_enrichment"] = "yes-please"
        with pytest.raises(ValueError, match="enable_enrichment"):
            connector.validate_configuration(base_config)

    def test_enable_enrichment_accepts_true_false(self, base_config):
        for value in ("true", "false", "TRUE", "False"):
            base_config["enable_enrichment"] = value
            # creds present in base_config so true is also valid
            connector.validate_configuration(base_config)

    def test_enable_genie_space_invalid_string_rejected(self, base_config):
        base_config["enable_genie_space"] = "maybe"
        with pytest.raises(ValueError, match="enable_genie_space"):
            connector.validate_configuration(base_config)

    def test_enable_genie_space_accepts_true_false(self, base_config):
        base_config["enable_genie_space"] = "false"
        connector.validate_configuration(base_config)
        base_config["enable_genie_space"] = "true"
        base_config["genie_table_identifier"] = "catalog.schema.faers_events"
        connector.validate_configuration(base_config)
