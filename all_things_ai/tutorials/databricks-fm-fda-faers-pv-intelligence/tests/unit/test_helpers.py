"""Unit tests for connector helper functions.

Mirrors databricks-fm-noaa-weather-risk-intelligence/tests/unit/test_helpers.py.
"""

import json

import connector


class TestIsPlaceholder:
    def test_none_is_placeholder(self):
        assert connector._is_placeholder(None) is True

    def test_empty_string_is_placeholder(self):
        assert connector._is_placeholder("") is True

    def test_angle_bracketed_is_placeholder(self):
        assert connector._is_placeholder("<DRUG_NAME_OR_EMPTY>") is True
        assert connector._is_placeholder("<DATABRICKS_PAT_TOKEN>") is True

    def test_real_value_is_not_placeholder(self):
        assert connector._is_placeholder("true") is False
        assert connector._is_placeholder("https://example.com") is False
        assert connector._is_placeholder("dapi_abc123") is False

    def test_partial_brackets_are_not_placeholder(self):
        assert connector._is_placeholder("<only-open") is False
        assert connector._is_placeholder("only-close>") is False

    def test_non_string_non_none_is_not_placeholder(self):
        assert connector._is_placeholder(42) is False
        assert connector._is_placeholder(True) is False


class TestParseBool:
    def test_bool_passthrough(self):
        assert connector._parse_bool(True) is True
        assert connector._parse_bool(False) is False

    def test_placeholder_returns_default(self):
        assert connector._parse_bool("<ENABLE_X>", default=True) is True
        assert connector._parse_bool("<ENABLE_X>", default=False) is False

    def test_none_returns_default(self):
        assert connector._parse_bool(None, default=True) is True
        assert connector._parse_bool(None, default=False) is False

    def test_string_true_variants(self):
        assert connector._parse_bool("true") is True
        assert connector._parse_bool("TRUE") is True
        assert connector._parse_bool("  true  ") is True

    def test_string_false_variants(self):
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
        assert connector._optional_int({"foo": "not-a-number"}, "foo", 42) == 42

    def test_none_returns_default(self):
        assert connector._optional_int({"foo": None}, "foo", 42) == 42


class TestOptionalStr:
    def test_placeholder_returns_default(self):
        assert connector._optional_str({}, "foo", "default") == "default"
        assert connector._optional_str({"foo": "<X>"}, "foo", "default") == "default"

    def test_valid_string(self):
        assert connector._optional_str({"foo": "value"}, "foo", "default") == "value"


class TestFlattenDict:
    def test_flat_dict_unchanged(self):
        result = connector.flatten_dict({"a": 1, "b": "two"})
        assert result == {"a": 1, "b": "two"}

    def test_nested_dict_flattens_with_underscore(self):
        result = connector.flatten_dict({"a": {"b": {"c": 1}}})
        assert result == {"a_b_c": 1}

    def test_list_serialized_as_json(self):
        result = connector.flatten_dict({"items": [1, 2, 3]})
        assert json.loads(result["items"]) == [1, 2, 3]

    def test_empty_list_becomes_none(self):
        result = connector.flatten_dict({"items": []})
        assert result["items"] is None

    def test_mixed_structures(self):
        result = connector.flatten_dict({"id": "x", "meta": {"tags": ["a", "b"], "count": 2}})
        assert result["id"] == "x"
        assert result["meta_count"] == 2
        assert json.loads(result["meta_tags"]) == ["a", "b"]


class TestExtractJsonFromContent:
    def test_returns_none_on_empty(self):
        assert connector.extract_json_from_content("") is None
        assert connector.extract_json_from_content(None) is None

    def test_extracts_clean_json(self):
        result = connector.extract_json_from_content('{"key": "value"}')
        assert result == {"key": "value"}

    def test_extracts_json_with_surrounding_prose(self):
        text = 'Here is the result:\n{"score": 7, "label": "ok"}\nThanks.'
        result = connector.extract_json_from_content(text)
        assert result == {"score": 7, "label": "ok"}

    def test_returns_none_on_invalid_json(self):
        assert connector.extract_json_from_content("not even close") is None


class TestBuildEventRecord:
    """The record builder must produce stable shape from raw FAERS events."""

    def test_minimal_event_returns_required_keys(self, minimal_faers_event):
        record = connector.build_event_record(minimal_faers_event)
        assert record["safety_report_id"] == "RPT-001"
        assert record["receive_date"] == "20260415"
        assert record["is_serious"] is True
        assert record["primary_drug"] == "Lipitor"
        assert record["primary_reaction"] == "Headache"

    def test_patient_sex_mapping(self, minimal_faers_event):
        record = connector.build_event_record(minimal_faers_event)
        assert record["patient_sex"] == "Male"

        minimal_faers_event["patient"]["patientsex"] = "2"
        assert connector.build_event_record(minimal_faers_event)["patient_sex"] == "Female"

        minimal_faers_event["patient"]["patientsex"] = "9"
        assert connector.build_event_record(minimal_faers_event)["patient_sex"] == "Unknown"

    def test_drug_and_reaction_lists_are_json_strings(self, minimal_faers_event):
        record = connector.build_event_record(minimal_faers_event)
        assert json.loads(record["drug_list"]) == ["Lipitor"]
        assert json.loads(record["reaction_list"]) == ["Headache"]
