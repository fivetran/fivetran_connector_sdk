"""
Unit tests for Livestock Weather Intelligence connector.

Tests core utility functions to ensure correctness and maintainability.

Run tests:
    pytest test_connector.py -v

Run with coverage:
    pytest test_connector.py --cov=connector --cov-report=html
"""

import json
from unittest.mock import Mock, patch

import pytest

from connector import flatten_dict, parse_agent_response, parse_farm_zip_mapping

# ============================================================================
# UTILITY FUNCTION TESTS
# ============================================================================


class TestFlattenDict:
    """Tests for flatten_dict utility function."""

    def test_simple_flat_dict(self):
        """Test that already-flat dictionary passes through unchanged."""
        data = {"name": "John", "age": 30, "city": "Houston"}
        result = flatten_dict(data)
        assert result == data

    def test_nested_objects(self):
        """Test nested dictionary flattening with underscore separator."""
        nested = {
            "user": {"name": "John Doe", "age": 30},
            "company": {"name": "Acme Corp", "size": 100},
        }
        result = flatten_dict(nested)

        assert result["user_name"] == "John Doe"
        assert result["user_age"] == 30
        assert result["company_name"] == "Acme Corp"
        assert result["company_size"] == 100
        assert "user" not in result
        assert "company" not in result

    def test_arrays_serialized_to_json(self):
        """Test that arrays are converted to JSON strings."""
        data = {
            "id": 123,
            "tags": ["python", "api", "data"],
            "metadata": {"colors": ["red", "blue"]},
        }
        result = flatten_dict(data)

        assert result["id"] == 123
        assert result["tags"] == '["python", "api", "data"]'
        assert result["metadata_colors"] == '["red", "blue"]'

        # Verify JSON strings can be parsed back
        assert json.loads(result["tags"]) == ["python", "api", "data"]
        assert json.loads(result["metadata_colors"]) == ["red", "blue"]

    def test_empty_array_becomes_null(self):
        """Test that empty arrays become None."""
        data = {"empty_list": [], "empty_tuple": ()}
        result = flatten_dict(data)

        assert result["empty_list"] is None
        assert result["empty_tuple"] is None

    def test_deeply_nested_structure(self):
        """Test flattening of deeply nested structures."""
        data = {"level1": {"level2": {"level3": {"value": "deep"}}}}
        result = flatten_dict(data)

        assert result["level1_level2_level3_value"] == "deep"

    def test_null_values_preserved(self):
        """Test that None values are preserved."""
        data = {"name": "John", "middle_name": None, "age": 30}
        result = flatten_dict(data)

        assert result["name"] == "John"
        assert result["middle_name"] is None
        assert result["age"] == 30


class TestParseFarmZipMapping:
    """Tests for farm-to-ZIP mapping parser."""

    def test_single_zip_single_farm(self):
        """Test parsing single ZIP with single farm."""
        mapping = parse_farm_zip_mapping("77429:FARM_000000")
        assert mapping == {"77429": ["FARM_000000"]}

    def test_single_zip_multiple_farms(self):
        """Test parsing single ZIP with multiple farms."""
        mapping = parse_farm_zip_mapping("77429:FARM_000000,FARM_000001,FARM_000002")
        assert mapping == {"77429": ["FARM_000000", "FARM_000001", "FARM_000002"]}

    def test_multiple_zips(self):
        """Test parsing multiple ZIPs."""
        mapping = parse_farm_zip_mapping(
            "77429:FARM_000000,FARM_000001|77856:FARM_000002|81611:FARM_000003"
        )
        assert mapping == {
            "77429": ["FARM_000000", "FARM_000001"],
            "77856": ["FARM_000002"],
            "81611": ["FARM_000003"],
        }

    def test_handles_whitespace(self):
        """Test that parser handles extra whitespace."""
        mapping = parse_farm_zip_mapping(
            " 77429 : FARM_000000 , FARM_000001 | 77856 : FARM_000002 "
        )
        assert mapping == {
            "77429": ["FARM_000000", "FARM_000001"],
            "77856": ["FARM_000002"],
        }

    def test_empty_string_returns_empty_dict(self):
        """Test that empty string returns empty dictionary."""
        assert parse_farm_zip_mapping("") == {}
        assert parse_farm_zip_mapping(None) == {}

    def test_malformed_entry_skipped(self):
        """Test that malformed entries are skipped."""
        mapping = parse_farm_zip_mapping("77429:FARM_000000|INVALID|77856:FARM_000001")
        assert mapping == {"77429": ["FARM_000000"], "77856": ["FARM_000001"]}


class TestParseAgentResponse:
    """Tests for Cortex Agent response parser."""

    def test_extract_all_sections(self):
        """Test extraction of all expected sections."""
        response = """RISK ASSESSMENT:
High risk due to cold temperatures affecting 144 beef cattle

AFFECTED FARMS:
FARM_000000, FARM_000001

SPECIES RISK MATRIX:
Beef Cattle: High
Chickens: Medium

RECOMMENDED ACTIONS:
1. Move cattle indoors within 6 hours
2. Increase feed by 20%

HISTORICAL CORRELATION:
Similar conditions in Dec 2024 caused respiratory issues"""

        weather_record = {"zip_code": "77429", "period_name": "Tonight"}
        enriched = parse_agent_response(response, weather_record)

        assert enriched is not None
        assert "High risk" in enriched["agent_livestock_risk_assessment"]

        farms = json.loads(enriched["agent_affected_farms"])
        assert "FARM_000000" in farms
        assert "FARM_000001" in farms

        species_matrix = json.loads(enriched["agent_species_risk_matrix"])
        assert "Beef Cattle" in species_matrix
        assert species_matrix["Beef Cattle"] == "High"

        assert "Move cattle indoors" in enriched["agent_recommended_actions"]
        assert "Dec 2024" in enriched["agent_historical_correlation"]

    def test_no_affected_farms(self):
        """Test handling of 'no high-risk farms' response."""
        response = """RISK ASSESSMENT:
Low risk

AFFECTED FARMS:
No high-risk farms identified

SPECIES RISK MATRIX:
Beef Cattle: Low"""

        weather_record = {"zip_code": "77429"}
        enriched = parse_agent_response(response, weather_record)

        farms = json.loads(enriched["agent_affected_farms"])
        assert farms == []

    def test_partial_response(self):
        """Test handling of partial response with defaults."""
        response = """RISK ASSESSMENT:
Moderate risk"""

        weather_record = {"zip_code": "77429"}
        enriched = parse_agent_response(response, weather_record)

        assert "Moderate risk" in enriched["agent_livestock_risk_assessment"]
        assert enriched["agent_affected_farms"] == "[]"
        assert enriched["agent_species_risk_matrix"] == "{}"
        assert "Monitor livestock" in enriched["agent_recommended_actions"]

    def test_empty_response_returns_defaults(self):
        """Test that empty response returns default values."""
        weather_record = {"zip_code": "77429"}
        enriched = parse_agent_response("", weather_record)

        assert "No assessment available" in enriched["agent_livestock_risk_assessment"]
        assert enriched["agent_affected_farms"] == "[]"
        assert enriched["agent_species_risk_matrix"] == "{}"

    def test_farm_id_regex_extraction(self):
        """Test regex extraction of FARM_XXXXXX patterns."""
        response = """RISK ASSESSMENT:
High risk

AFFECTED FARMS:
Farms FARM_000000 and FARM_000001 are at elevated risk due to conditions

SPECIES RISK MATRIX:
Beef Cattle: High"""

        weather_record = {"zip_code": "77429"}
        enriched = parse_agent_response(response, weather_record)

        farms = json.loads(enriched["agent_affected_farms"])
        assert "FARM_000000" in farms
        assert "FARM_000001" in farms


class TestAPIIntegration:
    """Integration tests with mocked API responses."""

    @patch("requests.Session.get")
    def test_get_coordinates_success(self, mock_get):
        """Test successful coordinate fetch."""
        from connector import get_coordinates_from_zip

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "places": [
                {
                    "latitude": "29.8088",
                    "longitude": "-95.6391",
                    "place name": "Cypress",
                    "state": "Texas",
                    "state abbreviation": "TX",
                }
            ]
        }
        mock_get.return_value = mock_response

        session = Mock()
        session.get = mock_get

        result = get_coordinates_from_zip(session, "77429")

        assert result is not None
        assert result["zip_code"] == "77429"
        assert result["latitude"] == 29.8088
        assert result["place_name"] == "Cypress"


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
