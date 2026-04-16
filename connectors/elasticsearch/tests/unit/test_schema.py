"""
Unit tests for schema building logic.
Tests: mapping_to_columns(), build_schema_from_mappings, schema()
"""
from unittest.mock import patch, MagicMock

import pytest
import connector


# ── mapping_to_columns ────────────────────────────────────────────────────────

class TestMappingToColumns:
    """Tests for connector.mapping_to_columns()."""

    def test_always_includes_id_as_string(self):
        columns = connector.mapping_to_columns({})
        assert "_id" in columns
        assert columns["_id"] == "STRING"

    def test_known_types_are_mapped_correctly(self):
        properties = {
            "name": {"type": "keyword"},
            "body": {"type": "text"},
            "score": {"type": "double"},
            "count": {"type": "integer"},
            "active": {"type": "boolean"},
            "created_at": {"type": "date"},
            "payload": {"type": "object"},
        }
        columns = connector.mapping_to_columns(properties)
        assert columns["name"] == "STRING"
        assert columns["body"] == "STRING"
        assert columns["score"] == "DOUBLE"
        assert columns["count"] == "INT"
        assert columns["active"] == "BOOLEAN"
        assert columns["created_at"] == "UTC_DATETIME"
        assert columns["payload"] == "JSON"

    def test_unsupported_types_are_excluded(self):
        properties = {
            "location": {"type": "geo_point"},
            "vector": {"type": "dense_vector"},
            "ip_addr": {"type": "ip"},
            "supported": {"type": "keyword"},
        }
        columns = connector.mapping_to_columns(properties)
        assert "location" not in columns
        assert "vector" not in columns
        assert "ip_addr" not in columns
        assert "supported" in columns

    def test_nested_object_without_explicit_type_is_json(self):
        # Object declared with sub-properties but no 'type' key
        properties = {
            "address": {
                "properties": {
                    "street": {"type": "keyword"},
                    "city": {"type": "keyword"},
                }
            }
        }
        columns = connector.mapping_to_columns(properties)
        assert columns["address"] == "JSON"

    def test_id_column_is_always_first_or_present(self):
        properties = {"title": {"type": "text"}, "views": {"type": "long"}}
        columns = connector.mapping_to_columns(properties)
        assert "_id" in columns
        assert columns["_id"] == "STRING"

    def test_all_string_subtypes_map_to_string(self):
        for es_type in ("text", "keyword", "constant_keyword", "wildcard"):
            properties = {"field": {"type": es_type}}
            columns = connector.mapping_to_columns(properties)
            assert columns["field"] == "STRING", f"{es_type} should map to STRING"

    def test_all_numeric_types_map_correctly(self):
        int_types = ("integer", "short", "byte")
        for es_type in int_types:
            cols = connector.mapping_to_columns({"f": {"type": es_type}})
            assert cols["f"] == "INT", f"{es_type} should map to INT"

        long_types = ("long", "unsigned_long")
        for es_type in long_types:
            cols = connector.mapping_to_columns({"f": {"type": es_type}})
            assert cols["f"] == "LONG", f"{es_type} should map to LONG"

        float_types = ("double", "float", "half_float", "scaled_float")
        for es_type in float_types:
            cols = connector.mapping_to_columns({"f": {"type": es_type}})
            assert cols["f"] == "DOUBLE", f"{es_type} should map to DOUBLE"

    def test_empty_properties_returns_only_id(self):
        columns = connector.mapping_to_columns({})
        assert columns == {"_id": "STRING"}


# ── schema() ─────────────────────────────────────────────────────────────────

class TestSchema:
    """Tests for connector.schema() — end-to-end with mocked discover_targets
    and get_all_mappings."""

    def _make_config(self):
        return {
            "host": "http://localhost:9200",
            "auth_method": "api_key",
            "api_key": "test-key",
        }

    @patch("connector.get_all_mappings")
    @patch("connector.discover_targets")
    def test_schema_returns_list_of_tables(self, mock_discover, mock_mappings):
        mock_discover.return_value = {"my_index": "index"}
        mock_mappings.return_value = {
            "my_index": {
                "title": {"type": "keyword"},
                "score": {"type": "float"},
            }
        }
        result = connector.schema(self._make_config())
        assert isinstance(result, list)
        assert len(result) == 1

    @patch("connector.get_all_mappings")
    @patch("connector.discover_targets")
    def test_schema_primary_key_is_id(self, mock_discover, mock_mappings):
        mock_discover.return_value = {"my_index": "index"}
        mock_mappings.return_value = {"my_index": {"name": {"type": "keyword"}}}
        tables = connector.schema(self._make_config())
        assert tables[0]["primary_key"] == ["_id"]

    @patch("connector.get_all_mappings")
    @patch("connector.discover_targets")
    def test_schema_table_name_matches_index(self, mock_discover, mock_mappings):
        mock_discover.return_value = {"products": "index"}
        mock_mappings.return_value = {"products": {"price": {"type": "double"}}}
        tables = connector.schema(self._make_config())
        assert tables[0]["table"] == "products"

    @patch("connector.get_all_mappings")
    @patch("connector.discover_targets")
    def test_schema_columns_includes_id(self, mock_discover, mock_mappings):
        mock_discover.return_value = {"events": "data_stream"}
        mock_mappings.return_value = {"events": {"message": {"type": "text"}}}
        tables = connector.schema(self._make_config())
        assert "_id" in tables[0]["columns"]
        assert tables[0]["columns"]["_id"] == "STRING"
        assert tables[0]["columns"]["message"] == "STRING"

    @patch("connector.get_all_mappings")
    @patch("connector.discover_targets")
    def test_schema_multiple_indices(self, mock_discover, mock_mappings):
        mock_discover.return_value = {
            "index_a": "index",
            "index_b": "index",
            "logs": "data_stream",
        }
        mock_mappings.return_value = {
            "index_a": {"f": {"type": "keyword"}},
            "index_b": {"n": {"type": "integer"}},
            "logs": {"@timestamp": {"type": "date"}},
        }
        tables = connector.schema(self._make_config())
        table_names = {t["table"] for t in tables}
        assert table_names == {"index_a", "index_b", "logs"}

    @patch("connector.get_all_mappings")
    @patch("connector.discover_targets")
    def test_schema_index_with_no_mapping_still_returns_id_column(
        self, mock_discover, mock_mappings
    ):
        mock_discover.return_value = {"sparse_index": "index"}
        # No mapping entry for this index
        mock_mappings.return_value = {}
        tables = connector.schema(self._make_config())
        assert len(tables) == 1
        assert tables[0]["columns"] == {"_id": "STRING"}

    @patch("connector.get_all_mappings")
    @patch("connector.discover_targets")
    def test_schema_excludes_unsupported_field_types(self, mock_discover, mock_mappings):
        mock_discover.return_value = {"geoindex": "index"}
        mock_mappings.return_value = {
            "geoindex": {
                "location": {"type": "geo_point"},
                "name": {"type": "keyword"},
            }
        }
        tables = connector.schema(self._make_config())
        cols = tables[0]["columns"]
        assert "location" not in cols
        assert "name" in cols

    @patch("connector.get_all_mappings")
    @patch("connector.discover_targets")
    def test_schema_empty_targets_returns_empty_list(self, mock_discover, mock_mappings):
        mock_discover.return_value = {}
        mock_mappings.return_value = {}
        tables = connector.schema(self._make_config())
        assert tables == []
