"""Configuration validation tests.

Covers:
- Required seed_make / seed_model / seed_year
- Numeric validation for discovery_depth / max_discoveries / max_enrichments
- Cortex-enabled chain (account + token + suffix check)
- Snowflake account hostname **scheme rejection** (Hazard #4)
  This rejection is missing in the current connector (validator only enforces
  the .snowflakecomputing.com suffix). Test must fail RED until fixed.
"""

import pytest

import connector


class TestRequiredFields:
    def test_passes_with_all_required(self, base_config):
        connector.validate_configuration(base_config)  # must not raise

    def test_missing_seed_make_rejected(self, base_config):
        del base_config["seed_make"]
        with pytest.raises(ValueError, match="seed_make"):
            connector.validate_configuration(base_config)

    def test_empty_seed_model_rejected(self, base_config):
        base_config["seed_model"] = ""
        with pytest.raises(ValueError, match="seed_model"):
            connector.validate_configuration(base_config)

    def test_missing_seed_year_rejected(self, base_config):
        del base_config["seed_year"]
        with pytest.raises(ValueError, match="seed_year"):
            connector.validate_configuration(base_config)


class TestNumericValidation:
    @pytest.mark.parametrize("field", ["discovery_depth", "max_discoveries", "max_enrichments"])
    def test_negative_rejected(self, field, base_config):
        base_config[field] = "-1"
        with pytest.raises(ValueError, match="non-negative"):
            connector.validate_configuration(base_config)

    @pytest.mark.parametrize("field", ["discovery_depth", "max_discoveries", "max_enrichments"])
    def test_non_integer_rejected(self, field, base_config):
        base_config[field] = "abc"
        with pytest.raises(ValueError, match="must be a number"):
            connector.validate_configuration(base_config)

    def test_zero_accepted(self, base_config):
        # Zero is allowed (means "no enrichment / no discovery")
        base_config["max_enrichments"] = "0"
        connector.validate_configuration(base_config)

    def test_optional_field_when_missing(self, base_config):
        del base_config["discovery_depth"]
        connector.validate_configuration(base_config)  # should not raise


class TestCortexEnabledChain:
    """When enable_cortex=true, snowflake_account + snowflake_pat_token must
    both be set, and snowflake_account must end with snowflakecomputing.com."""

    def test_enabled_without_account_rejected(self, base_config):
        del base_config["snowflake_account"]
        with pytest.raises(ValueError, match="snowflake_account"):
            connector.validate_configuration(base_config)

    def test_enabled_without_token_rejected(self, base_config):
        del base_config["snowflake_pat_token"]
        with pytest.raises(ValueError, match="snowflake_pat_token"):
            connector.validate_configuration(base_config)

    def test_enabled_with_wrong_suffix_rejected(self, base_config):
        base_config["snowflake_account"] = "abc.example.com"
        with pytest.raises(ValueError, match="snowflakecomputing.com"):
            connector.validate_configuration(base_config)

    def test_disabled_skips_chain_validation(self, base_config):
        """When enable_cortex=false, skip snowflake account/token validation."""
        base_config["enable_cortex"] = "false"
        del base_config["snowflake_account"]
        del base_config["snowflake_pat_token"]
        connector.validate_configuration(base_config)  # must not raise


class TestSnowflakeAccountSchemeRejection:
    """Hazard #4 (api-profiling.md): URL-interpolated hostname configs MUST
    reject scheme prefixes. The connector builds:

        url = f"https://{snowflake_account}{__CORTEX_AGENT_ENDPOINT}"

    If the user supplies `https://abc.snowflakecomputing.com`, the URL becomes
    `https://https://abc.snowflakecomputing.com/...` and fails at runtime with
    confusing errors. The validator must reject scheme prefixes explicitly.

    api-profiling.md cites PR #561 specifically as motivation for this rule.

    These tests are expected to fail RED against the current connector — the
    fix is to add a scheme-prefix check in validate_configuration() alongside
    the existing suffix check.
    """

    def test_https_prefix_rejected(self, base_config):
        base_config["snowflake_account"] = "https://abc.snowflakecomputing.com"
        with pytest.raises(ValueError, match="hostname"):
            connector.validate_configuration(base_config)

    def test_http_prefix_rejected(self, base_config):
        base_config["snowflake_account"] = "http://abc.snowflakecomputing.com"
        with pytest.raises(ValueError, match="hostname"):
            connector.validate_configuration(base_config)
