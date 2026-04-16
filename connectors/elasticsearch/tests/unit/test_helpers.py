"""
Unit tests for auth helpers, URL helpers, ES type mapping, and distribution detection.
Tests: get_headers(), base_url(), ES_TYPE_MAP / map_es_type, detect_distribution()
"""
import base64
from unittest.mock import patch, MagicMock

import pytest
import connector


# ── get_headers ───────────────────────────────────────────────────────────────

class TestGetHeaders:
    def test_api_key_returns_apikey_authorization_header(self):
        cfg = {"auth_method": "api_key", "api_key": "my-secret-key"}
        headers = connector.get_headers(cfg)
        assert headers["Authorization"] == "ApiKey my-secret-key"
        assert headers["Content-Type"] == "application/json"

    def test_api_key_is_default_auth_method_when_not_specified(self):
        # auth_method absent → defaults to api_key
        cfg = {"api_key": "default-key"}
        headers = connector.get_headers(cfg)
        assert headers["Authorization"] == "ApiKey default-key"

    def test_basic_auth_produces_base64_encoded_header(self):
        cfg = {
            "auth_method": "basic_auth",
            "username": "elastic",
            "password": "changeme",
        }
        headers = connector.get_headers(cfg)
        expected_raw = base64.b64encode(b"elastic:changeme").decode()
        assert headers["Authorization"] == f"Basic {expected_raw}"
        assert headers["Content-Type"] == "application/json"

    def test_basic_auth_with_special_characters_in_password(self):
        cfg = {
            "auth_method": "basic_auth",
            "username": "user",
            "password": "p@ss:w0rd!",
        }
        headers = connector.get_headers(cfg)
        raw = base64.b64decode(headers["Authorization"].replace("Basic ", "")).decode()
        assert raw == "user:p@ss:w0rd!"

    def test_auth_method_is_case_insensitive(self):
        cfg = {"auth_method": "API_KEY", "api_key": "k"}
        headers = connector.get_headers(cfg)
        assert headers["Authorization"] == "ApiKey k"

    def test_auth_method_strips_whitespace(self):
        cfg = {"auth_method": "  basic_auth  ", "username": "u", "password": "p"}
        headers = connector.get_headers(cfg)
        assert headers["Authorization"].startswith("Basic ")


# ── base_url ──────────────────────────────────────────────────────────────────

class TestBaseUrl:
    def test_strips_single_trailing_slash(self):
        cfg = {"host": "http://localhost:9200/"}
        assert connector.base_url(cfg) == "http://localhost:9200"

    def test_strips_multiple_trailing_slashes(self):
        cfg = {"host": "https://my-cluster.example.com///"}
        assert connector.base_url(cfg) == "https://my-cluster.example.com"

    def test_no_trailing_slash_is_unchanged(self):
        cfg = {"host": "http://localhost:9200"}
        assert connector.base_url(cfg) == "http://localhost:9200"

    def test_host_with_path_prefix_preserves_path(self):
        cfg = {"host": "https://cloud.elastic.co/mydeployment"}
        assert connector.base_url(cfg) == "https://cloud.elastic.co/mydeployment"


# ── ES_TYPE_MAP / type mapping ────────────────────────────────────────────────

class TestEsTypeMap:
    """Test that ES_TYPE_MAP contains the expected mappings."""

    @pytest.mark.parametrize("es_type,expected", [
        ("text", "STRING"),
        ("keyword", "STRING"),
        ("constant_keyword", "STRING"),
        ("wildcard", "STRING"),
        ("boolean", "BOOLEAN"),
        ("binary", "BINARY"),
        ("integer", "INT"),
        ("short", "INT"),
        ("byte", "INT"),
        ("long", "LONG"),
        ("unsigned_long", "LONG"),
        ("double", "DOUBLE"),
        ("float", "DOUBLE"),
        ("half_float", "DOUBLE"),
        ("scaled_float", "DOUBLE"),
        ("date", "UTC_DATETIME"),
        ("date_nanos", "UTC_DATETIME"),
        ("object", "JSON"),
        ("nested", "JSON"),
        ("flattened", "JSON"),
        ("join", "JSON"),
    ])
    def test_known_type_maps_correctly(self, es_type, expected):
        assert connector.ES_TYPE_MAP[es_type] == expected

    @pytest.mark.parametrize("unsupported_type", [
        "alias", "geo_point", "geo_shape", "dense_vector",
        "integer_range", "float_range", "date_range",
        "long_range", "double_range", "ip_range", "ip",
        "completion", "search_as_you_type", "rank_feature",
        "rank_features", "sparse_vector",
    ])
    def test_unsupported_type_not_in_map(self, unsupported_type):
        assert unsupported_type not in connector.ES_TYPE_MAP


# ── detect_distribution ────────────────────────────────────────────────────────

class TestDetectDistribution:
    """Tests for detect_distribution() using mocked es_request."""

    def _make_config(self):
        return {
            "host": "http://localhost:9200",
            "auth_method": "api_key",
            "api_key": "test-key",
        }

    @patch("connector.es_request")
    def test_returns_opensearch_when_distribution_field_is_opensearch(self, mock_req):
        mock_req.return_value = {
            "version": {
                "number": "2.18.0",
                "distribution": "opensearch",
            }
        }
        result = connector.detect_distribution(self._make_config())
        assert result == "opensearch"

    @patch("connector.es_request")
    def test_returns_elasticsearch_when_distribution_field_absent(self, mock_req):
        # Standard ES 8.x response — no 'distribution' key
        mock_req.return_value = {
            "version": {
                "number": "8.17.0",
            }
        }
        result = connector.detect_distribution(self._make_config())
        assert result == "elasticsearch"

    @patch("connector.es_request")
    def test_returns_elasticsearch_on_exception(self, mock_req):
        mock_req.side_effect = Exception("connection refused")
        result = connector.detect_distribution(self._make_config())
        assert result == "elasticsearch"

    @patch("connector.es_request")
    def test_distribution_check_is_case_insensitive(self, mock_req):
        mock_req.return_value = {"version": {"distribution": "OpenSearch"}}
        result = connector.detect_distribution(self._make_config())
        assert result == "opensearch"

    @patch("connector.es_request")
    def test_calls_get_on_base_url(self, mock_req):
        mock_req.return_value = {"version": {}}
        cfg = self._make_config()
        connector.detect_distribution(cfg)
        call_args = mock_req.call_args
        assert call_args[0][0] == "GET"
        assert call_args[0][1] == "http://localhost:9200"

    @patch("connector.es_request")
    def test_empty_version_block_defaults_to_elasticsearch(self, mock_req):
        mock_req.return_value = {}
        result = connector.detect_distribution(self._make_config())
        assert result == "elasticsearch"
