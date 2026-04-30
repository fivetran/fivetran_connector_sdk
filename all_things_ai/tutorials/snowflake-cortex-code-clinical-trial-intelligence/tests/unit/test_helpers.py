"""Unit tests for connector helper functions.

# see snowflake-cortex-code-nvd-cve-threat-intelligence/tests/unit/test_helpers.py
# for the analogous Cortex-side helper tests.
"""

import json

import connector


class TestIsPlaceholder:
    """Submission skill check #16 — _is_placeholder must be type-safe."""

    def test_none_is_placeholder(self):
        assert connector._is_placeholder(None) is True

    def test_empty_string_is_placeholder(self):
        assert connector._is_placeholder("") is True

    def test_angle_bracketed_is_placeholder(self):
        assert connector._is_placeholder("<THERAPEUTIC_AREA>") is True
        assert connector._is_placeholder("<SNOWFLAKE_PAT_TOKEN>") is True

    def test_real_value_is_not_placeholder(self):
        assert connector._is_placeholder("atrial fibrillation") is False
        assert connector._is_placeholder("abc12345.snowflakecomputing.com") is False

    def test_partial_brackets_are_not_placeholder(self):
        assert connector._is_placeholder("<only-open") is False
        assert connector._is_placeholder("only-close>") is False

    def test_non_string_non_none_is_not_placeholder(self):
        """Type-safety regression — must not AttributeError on int/bool inputs."""
        assert connector._is_placeholder(42) is False
        assert connector._is_placeholder(True) is False
        assert connector._is_placeholder(0) is False


class TestParseBool:
    def test_bool_passthrough(self):
        assert connector._parse_bool(True) is True
        assert connector._parse_bool(False) is False

    def test_placeholder_returns_default(self):
        assert connector._parse_bool("<TRUE_OR_FALSE>", default=True) is True
        assert connector._parse_bool("<TRUE_OR_FALSE>", default=False) is False

    def test_none_returns_default(self):
        assert connector._parse_bool(None, default=True) is True

    def test_string_true_variants(self):
        assert connector._parse_bool("true") is True
        assert connector._parse_bool("TRUE") is True
        assert connector._parse_bool("  true  ") is True

    def test_non_true_strings_are_false(self):
        assert connector._parse_bool("false") is False
        assert connector._parse_bool("yes") is False
        assert connector._parse_bool("1") is False


class TestOptionalInt:
    def test_placeholder_returns_default(self):
        assert connector._optional_int({}, "foo", 42) == 42
        assert connector._optional_int({"foo": "<X>"}, "foo", 42) == 42

    def test_valid_int_string(self):
        assert connector._optional_int({"foo": "7"}, "foo", 42) == 7

    def test_invalid_int_returns_default(self):
        assert connector._optional_int({"foo": "abc"}, "foo", 42) == 42

    def test_none_returns_default(self):
        assert connector._optional_int({"foo": None}, "foo", 42) == 42


class TestOptionalStr:
    def test_placeholder_returns_default(self):
        assert connector._optional_str({}, "foo", "default") == "default"
        assert connector._optional_str({"foo": "<X>"}, "foo", "default") == "default"

    def test_valid_string(self):
        assert connector._optional_str({"foo": "value"}, "foo", "default") == "value"


class TestFlattenDict:
    def test_flat_unchanged(self):
        assert connector.flatten_dict({"a": 1}) == {"a": 1}

    def test_nested_underscore(self):
        assert connector.flatten_dict({"a": {"b": 1}}) == {"a_b": 1}

    def test_list_serialized(self):
        result = connector.flatten_dict({"items": [1, 2]})
        assert json.loads(result["items"]) == [1, 2]

    def test_empty_list_becomes_none(self):
        result = connector.flatten_dict({"items": []})
        assert result["items"] is None


class TestShouldSkipForIncremental:
    """Composite cursor (date + nct_ids_at_date) for day-granularity dates.

    Reviewer finding (Copilot, line 415): single-key date cursor with `<=` filter
    drops same-day records when `lastUpdatePostDate` granularity is YYYY-MM-DD."""

    def test_no_cursor_never_skips(self):
        assert connector._should_skip_for_incremental("2026-04-15", "NCT001", None, set()) is False

    def test_newer_record_not_skipped(self):
        assert (
            connector._should_skip_for_incremental("2026-04-16", "NCT001", "2026-04-15", set())
            is False
        )

    def test_older_record_skipped(self):
        assert (
            connector._should_skip_for_incremental("2026-04-14", "NCT001", "2026-04-15", set())
            is True
        )

    def test_same_day_unsynced_not_skipped(self):
        # Same-day record whose nct_id is NOT in the cursor's synced set must NOT be skipped.
        assert (
            connector._should_skip_for_incremental(
                "2026-04-15", "NCT_NEW", "2026-04-15", {"NCT_OLD"}
            )
            is False
        )

    def test_same_day_already_synced_skipped(self):
        assert (
            connector._should_skip_for_incremental(
                "2026-04-15", "NCT001", "2026-04-15", {"NCT001"}
            )
            is True
        )


class TestBuildTrialRecord:
    def test_minimal_returns_required_keys(self, minimal_trial):
        record = connector.build_trial_record(minimal_trial)
        assert record["nct_id"] == "NCT06000001"
        assert record["brief_title"] == "Test Trial"
        assert record["overall_status"] == "RECRUITING"
        assert record["last_update_post_date"] == "2026-04-15"
        assert record["lead_sponsor_name"] == "Test Sponsor"

    def test_lists_serialized_as_json(self, minimal_trial):
        record = connector.build_trial_record(minimal_trial)
        assert json.loads(record["conditions"]) == ["Atrial Fibrillation"]
        assert json.loads(record["phases"]) == ["PHASE3"]

    def test_no_nct_id_returns_none(self):
        study = {"protocolSection": {"identificationModule": {"briefTitle": "X"}}}
        assert connector.build_trial_record(study) is None
