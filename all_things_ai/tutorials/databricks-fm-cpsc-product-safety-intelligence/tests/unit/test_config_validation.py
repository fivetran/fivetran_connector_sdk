"""Configuration validation tests."""

import pytest

import connector


class TestNumericValidation:
    @pytest.mark.parametrize("field", ["max_recalls", "max_enrichments", "lookback_days"])
    def test_zero_rejected(self, field, base_config):
        base_config[field] = "0"
        with pytest.raises(ValueError, match="positive integer"):
            connector.validate_configuration(base_config)


class TestSanityCeilings:
    def test_max_recalls_ceiling(self, base_config):
        base_config["max_recalls"] = "9999"
        with pytest.raises(ValueError, match="ceiling"):
            connector.validate_configuration(base_config)

    def test_max_enrichments_ceiling(self, base_config):
        base_config["max_enrichments"] = "9999"
        with pytest.raises(ValueError, match="ceiling"):
            connector.validate_configuration(base_config)


class TestDatabricksCredentialChain:
    def test_enrichment_enabled_requires_creds(self, base_config):
        base_config["enable_enrichment"] = "true"
        base_config["databricks_token"] = "<DATABRICKS_PAT_TOKEN>"
        with pytest.raises(ValueError, match="databricks_token"):
            connector.validate_configuration(base_config)

    def test_workspace_url_must_be_https(self, base_config):
        base_config["enable_enrichment"] = "true"
        base_config["databricks_workspace_url"] = "example.cloud.databricks.com"
        with pytest.raises(ValueError, match="https://"):
            connector.validate_configuration(base_config)

    def test_data_only_mode_skips_chain(self, base_config):
        base_config["enable_enrichment"] = "false"
        base_config["enable_genie_space"] = "false"
        base_config["databricks_token"] = "<DATABRICKS_PAT_TOKEN>"
        connector.validate_configuration(base_config)


class TestGenieSpaceValidation:
    def test_missing_table_rejected(self, base_config):
        base_config["enable_genie_space"] = "true"
        base_config["genie_table_identifier"] = "<CATALOG.SCHEMA.TABLE>"
        with pytest.raises(ValueError, match="genie_table_identifier"):
            connector.validate_configuration(base_config)
