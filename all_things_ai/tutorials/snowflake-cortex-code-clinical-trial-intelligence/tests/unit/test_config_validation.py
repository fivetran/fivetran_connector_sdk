"""Unit tests for validate_configuration().

Encodes the README contract for every configurable field plus the
sanity ceiling on max_seed_trials (PR #566 reviewer finding).

# see snowflake-cortex-code-nvd-cve-threat-intelligence/tests/unit/test_config_validation.py
# for the analogous Cortex-side validation tests.
"""

import pytest

import connector


class TestRequiredCondition:
    """Condition is the only always-required config field."""

    def test_valid_config_passes(self, base_config):
        connector.validate_configuration(base_config)

    def test_missing_condition_rejected(self, base_config):
        base_config["condition"] = "<THERAPEUTIC_AREA>"
        with pytest.raises(ValueError, match="condition"):
            connector.validate_configuration(base_config)


class TestCortexCredentialsRequiredWhenEnabled:
    def test_missing_account_rejected(self, base_config):
        base_config["snowflake_account"] = "<SNOWFLAKE_ACCOUNT_HOSTNAME>"
        with pytest.raises(ValueError, match="snowflake_account"):
            connector.validate_configuration(base_config)

    def test_missing_pat_token_rejected(self, base_config):
        base_config["snowflake_pat_token"] = "<SNOWFLAKE_PAT_TOKEN>"
        with pytest.raises(ValueError, match="snowflake_pat_token"):
            connector.validate_configuration(base_config)

    def test_cortex_disabled_does_not_require_creds(self, base_config):
        base_config["enable_cortex"] = "false"
        base_config["snowflake_account"] = "<SNOWFLAKE_ACCOUNT_HOSTNAME>"
        base_config["snowflake_pat_token"] = "<SNOWFLAKE_PAT_TOKEN>"
        connector.validate_configuration(base_config)


class TestSnowflakeAccountFormat:
    """Submission skill check #20 + #21 — hostname not URL."""

    def test_account_must_end_with_snowflakecomputing_com(self, base_config):
        base_config["snowflake_account"] = "abc12345.example.com"
        with pytest.raises(ValueError, match="snowflakecomputing"):
            connector.validate_configuration(base_config)

    def test_https_prefix_rejected(self, base_config):
        base_config["snowflake_account"] = "https://abc.snowflakecomputing.com"
        with pytest.raises(ValueError, match="hostname"):
            connector.validate_configuration(base_config)

    def test_http_prefix_rejected(self, base_config):
        base_config["snowflake_account"] = "http://abc.snowflakecomputing.com"
        with pytest.raises(ValueError, match="hostname"):
            connector.validate_configuration(base_config)


class TestNumericValidation:
    """Submission skill check #18 — every field consumed via int() must be in
    the validation loop. PR #566 b7101edd already includes max_seed_trials,
    max_discoveries, max_debates, page_size."""

    def test_max_seed_trials_negative_rejected(self, base_config):
        base_config["max_seed_trials"] = "-1"
        with pytest.raises(ValueError, match="positive integer"):
            connector.validate_configuration(base_config)

    def test_max_seed_trials_zero_rejected(self, base_config):
        base_config["max_seed_trials"] = "0"
        with pytest.raises(ValueError, match="positive integer"):
            connector.validate_configuration(base_config)

    def test_max_debates_zero_rejected(self, base_config):
        base_config["max_debates"] = "0"
        with pytest.raises(ValueError, match="positive integer"):
            connector.validate_configuration(base_config)

    def test_page_size_zero_rejected(self, base_config):
        base_config["page_size"] = "0"
        with pytest.raises(ValueError, match="positive integer"):
            connector.validate_configuration(base_config)

    def test_max_seed_trials_non_numeric_rejected(self, base_config):
        base_config["max_seed_trials"] = "many"
        with pytest.raises(ValueError, match="positive integer"):
            connector.validate_configuration(base_config)

    def test_placeholder_values_pass(self, base_config):
        base_config["max_seed_trials"] = "<MAX_SEED_TRIALS>"
        base_config["max_discoveries"] = "<MAX_DISCOVERIES>"
        base_config["max_debates"] = "<MAX_DEBATES>"
        base_config["page_size"] = "<PAGE_SIZE>"
        connector.validate_configuration(base_config)


class TestMaxSeedTrialsCeiling:
    """PR #566 reviewer finding: unbounded max_seed_trials risks memory blow-up."""

    # __MAX_SEED_TRIALS_CEILING uses Python name mangling (double underscore)
    # so we read it via the mangled name. See conftest for analogous handling.
    _CEILING = getattr(connector, "_connector__MAX_SEED_TRIALS_CEILING", None) or getattr(
        connector, "__MAX_SEED_TRIALS_CEILING", 500
    )

    def test_above_ceiling_rejected(self, base_config):
        base_config["max_seed_trials"] = str(self._CEILING + 1)
        with pytest.raises(ValueError, match="exceeds the connector ceiling"):
            connector.validate_configuration(base_config)

    def test_at_ceiling_accepted(self, base_config):
        base_config["max_seed_trials"] = str(self._CEILING)
        connector.validate_configuration(base_config)
