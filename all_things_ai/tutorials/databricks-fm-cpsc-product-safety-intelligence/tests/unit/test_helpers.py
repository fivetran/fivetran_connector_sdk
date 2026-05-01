"""Unit tests for connector helpers."""

import json

import connector


class TestIsPlaceholder:
    def test_none(self):
        assert connector._is_placeholder(None) is True

    def test_empty(self):
        assert connector._is_placeholder("") is True

    def test_angle_bracketed(self):
        assert connector._is_placeholder("<X>") is True

    def test_real(self):
        assert connector._is_placeholder("real") is False

    def test_non_string(self):
        assert connector._is_placeholder(42) is False


class TestParseBool:
    def test_passthrough(self):
        assert connector._parse_bool(True) is True

    def test_string_true(self):
        assert connector._parse_bool("true") is True

    def test_placeholder_default(self):
        assert connector._parse_bool("<X>", default=True) is True


class TestOptionalInt:
    def test_placeholder_default(self):
        assert connector._optional_int({"x": "<Y>"}, "x", 42) == 42

    def test_valid(self):
        assert connector._optional_int({"x": "7"}, "x", 42) == 7


class TestOptionalStr:
    def test_placeholder_default(self):
        assert connector._optional_str({"x": "<Y>"}, "x", "default") == "default"

    def test_valid(self):
        assert connector._optional_str({"x": "value"}, "x", "default") == "value"


class TestFlattenDict:
    def test_flat(self):
        assert connector.flatten_dict({"a": 1}) == {"a": 1}

    def test_nested(self):
        assert connector.flatten_dict({"a": {"b": 1}}) == {"a_b": 1}

    def test_list_serialized(self):
        result = connector.flatten_dict({"x": [1, 2]})
        assert json.loads(result["x"]) == [1, 2]


class TestExtractJsonFromContent:
    def test_returns_none_for_no_json(self):
        assert connector.extract_json_from_content("plain text") is None

    def test_parses_clean(self):
        assert connector.extract_json_from_content('{"a":1}') == {"a": 1}

    def test_handles_malformed(self):
        assert connector.extract_json_from_content("{not valid") is None
