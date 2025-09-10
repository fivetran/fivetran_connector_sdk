#!/usr/bin/env python3
"""
Test suite for Trustpilot API Connector
Tests connector functionality, configuration validation, and API connectivity
"""

import os
import sys

# Import connector functions for testing
from connector import (
    validate_configuration,
    get_trustpilot_endpoint,
    execute_api_request,
    get_time_range,
    schema,
)


def test_connector():
    """Test basic connector functionality"""
    print("Testing basic connector functionality...")

    # Test configuration validation
    test_config = {
        "api_key": "test_api_key",
        "business_unit_id": "test_business_unit_id",
        "consumer_id": "test_consumer_id",
        "sync_frequency_hours": "4",
        "initial_sync_days": "90",
        "max_records_per_page": "100",
        "request_timeout_seconds": "30",
        "retry_attempts": "3",
        "enable_incremental_sync": "true",
        "enable_consumer_reviews": "true",
        "enable_invitation_links": "true",
        "enable_categories": "true",
        "data_retention_days": "730",
        "enable_debug_logging": "false",
    }

    try:
        validate_configuration(test_config)
        print("✅ Configuration validation passed")
    except Exception as e:
        print(f"❌ Configuration validation failed: {e}")
        return False

    # Test endpoint function
    endpoint = get_trustpilot_endpoint()
    if endpoint == "https://api.trustpilot.com/v1":
        print("✅ Endpoint function passed")
    else:
        print(
            f"❌ Endpoint function failed: expected 'https://api.trustpilot.com/v1', got '{endpoint}'"
        )
        return False

    # Test time range function
    time_range = get_time_range()
    if "start" in time_range and "end" in time_range:
        print("✅ Time range function passed")
    else:
        print(f"❌ Time range function failed: {time_range}")
        return False

    # Test schema function
    schema_data = schema(test_config)
    if isinstance(schema_data, list) and len(schema_data) > 0:
        print("✅ Schema function passed")
    else:
        print(f"❌ Schema function failed: {schema_data}")
        return False

    return True


def test_schema_only():
    """Test schema function with various configurations"""
    print("\nTesting schema function...")

    test_config = {
        "api_key": "test_api_key",
        "business_unit_id": "test_business_unit_id",
        "consumer_id": "test_consumer_id",
        "sync_frequency_hours": "4",
        "initial_sync_days": "90",
        "max_records_per_page": "100",
        "request_timeout_seconds": "30",
        "retry_attempts": "3",
        "enable_incremental_sync": "true",
        "enable_consumer_reviews": "true",
        "enable_invitation_links": "true",
        "enable_categories": "true",
        "data_retention_days": "730",
        "enable_debug_logging": "false",
    }

    try:
        schema_data = schema(test_config)

        # Check if all expected tables are present
        expected_tables = [
            "review",
            "business_unit",
            "category",
            "consumer_review",
            "invitation_link",
        ]
        actual_tables = [table["table"] for table in schema_data]

        for expected_table in expected_tables:
            if expected_table in actual_tables:
                print(f"✅ Table '{expected_table}' found in schema")
            else:
                print(f"❌ Table '{expected_table}' missing from schema")
                return False

        # Check if tables have proper structure
        for table in schema_data:
            if "table" in table and "primary_key" in table and "columns" in table:
                print(f"✅ Table '{table['table']}' has proper structure")
            else:
                print(f"❌ Table '{table['table']}' missing required fields")
                return False

        print("✅ Schema validation passed")
        return True

    except Exception as e:
        print(f"❌ Schema test failed: {e}")
        return False


def test_configuration_validation():
    """Test configuration validation with various scenarios"""
    print("\nTesting configuration validation...")

    # Test valid configuration
    valid_config = {
        "api_key": "valid_api_key_123",
        "business_unit_id": "valid_business_unit_id_456",
        "consumer_id": "valid_consumer_id_789",
        "sync_frequency_hours": "4",
        "initial_sync_days": "90",
        "max_records_per_page": "100",
        "request_timeout_seconds": "30",
        "retry_attempts": "3",
        "enable_incremental_sync": "true",
        "enable_consumer_reviews": "true",
        "enable_invitation_links": "true",
        "enable_categories": "true",
        "data_retention_days": "730",
        "enable_debug_logging": "false",
    }

    try:
        validate_configuration(valid_config)
        print("✅ Valid configuration passed")
    except Exception as e:
        print(f"❌ Valid configuration failed: {e}")
        return False

    # Test missing api_key
    invalid_config_1 = {"business_unit_id": "valid_business_unit_id_456"}

    try:
        validate_configuration(invalid_config_1)
        print("❌ Missing api_key should have failed")
        return False
    except ValueError as e:
        if "api_key" in str(e):
            print("✅ Missing api_key correctly caught")
        else:
            print(f"❌ Unexpected error for missing api_key: {e}")
            return False

    # Test missing business_unit_id
    invalid_config_2 = {
        "api_key": "valid_api_key_123",
        "consumer_id": "valid_consumer_id_789",
        "sync_frequency_hours": "4",
        "initial_sync_days": "90",
        "max_records_per_page": "100",
        "request_timeout_seconds": "30",
        "retry_attempts": "3",
        "enable_incremental_sync": "true",
        "enable_consumer_reviews": "true",
        "enable_invitation_links": "true",
        "enable_categories": "true",
        "data_retention_days": "730",
        "enable_debug_logging": "false",
    }

    try:
        validate_configuration(invalid_config_2)
        print("❌ Missing business_unit_id should have failed")
        return False
    except ValueError as e:
        if "business_unit_id" in str(e):
            print("✅ Missing business_unit_id correctly caught")
        else:
            print(f"❌ Unexpected error for missing business_unit_id: {e}")
            return False

    # Test empty api_key
    invalid_config_3 = {
        "api_key": "",
        "business_unit_id": "valid_business_unit_id_456",
        "consumer_id": "valid_consumer_id_789",
        "sync_frequency_hours": "4",
        "initial_sync_days": "90",
        "max_records_per_page": "100",
        "request_timeout_seconds": "30",
        "retry_attempts": "3",
        "enable_incremental_sync": "true",
        "enable_consumer_reviews": "true",
        "enable_invitation_links": "true",
        "enable_categories": "true",
        "data_retention_days": "730",
        "enable_debug_logging": "false",
    }

    try:
        validate_configuration(invalid_config_3)
        print("❌ Empty api_key should have failed")
        return False
    except ValueError as e:
        if "empty" in str(e).lower():
            print("✅ Empty api_key correctly caught")
        else:
            print(f"❌ Unexpected error for empty api_key: {e}")
            return False

    # Test missing consumer_id
    invalid_config_4 = {
        "api_key": "valid_api_key_123",
        "business_unit_id": "valid_business_unit_id_456",
        "sync_frequency_hours": "4",
        "initial_sync_days": "90",
        "max_records_per_page": "100",
        "request_timeout_seconds": "30",
        "retry_attempts": "3",
        "enable_incremental_sync": "true",
        "enable_consumer_reviews": "true",
        "enable_invitation_links": "true",
        "enable_categories": "true",
        "data_retention_days": "730",
        "enable_debug_logging": "false",
    }

    try:
        validate_configuration(invalid_config_4)
        print("❌ Missing consumer_id should have failed")
        return False
    except ValueError as e:
        if "consumer_id" in str(e):
            print("✅ Missing consumer_id correctly caught")
        else:
            print(f"❌ Unexpected error for missing consumer_id: {e}")
            return False

    # Test empty consumer_id
    invalid_config_5 = {
        "api_key": "valid_api_key_123",
        "business_unit_id": "valid_business_unit_id_456",
        "consumer_id": "",
        "sync_frequency_hours": "4",
        "initial_sync_days": "90",
        "max_records_per_page": "100",
        "request_timeout_seconds": "30",
        "retry_attempts": "3",
        "enable_incremental_sync": "true",
        "enable_consumer_reviews": "true",
        "enable_invitation_links": "true",
        "enable_categories": "true",
        "data_retention_days": "730",
        "enable_debug_logging": "false",
    }

    try:
        validate_configuration(invalid_config_5)
        print("❌ Empty consumer_id should have failed")
        return False
    except ValueError as e:
        if "empty" in str(e).lower():
            print("✅ Empty consumer_id correctly caught")
        else:
            print(f"❌ Unexpected error for empty consumer_id: {e}")
            return False

    print("✅ Configuration validation tests passed")
    return True


def test_endpoint_functions():
    """Test endpoint and utility functions"""
    print("\nTesting endpoint and utility functions...")

    # Test endpoint function
    endpoint = get_trustpilot_endpoint()
    if endpoint == "https://api.trustpilot.com/v1":
        print("✅ Endpoint function returned correct URL")
    else:
        print(f"❌ Endpoint function failed: {endpoint}")
        return False

    # Test time range function
    time_range = get_time_range()
    if isinstance(time_range, dict) and "start" in time_range and "end" in time_range:
        print("✅ Time range function returned proper structure")
    else:
        print(f"❌ Time range function failed: {time_range}")
        return False

    # Test time range with last sync time
    last_sync = "2024-01-01T00:00:00"
    time_range_with_sync = get_time_range(last_sync)
    if time_range_with_sync["start"] == last_sync:
        print("✅ Time range function with last sync time passed")
    else:
        print(f"❌ Time range function with last sync time failed: {time_range_with_sync}")
        return False

    print("✅ Endpoint function tests passed")
    return True


def test_api_connectivity():
    """Test API connectivity with real credentials if available"""
    print("\nTesting API connectivity...")

    # Check if we have real credentials
    api_key = os.environ.get("TRUSTPILOT_API_KEY")
    business_unit_id = os.environ.get("TRUSTPILOT_BUSINESS_UNIT_ID")

    if not api_key or not business_unit_id:
        print("⚠️  Skipping API connectivity test - no credentials provided")
        print(
            "   Set TRUSTPILOT_API_KEY and TRUSTPILOT_BUSINESS_UNIT_ID environment variables to test"
        )
        return True

    try:
        # Test business unit endpoint
        endpoint = f"/business-units/{business_unit_id}"
        response = execute_api_request(endpoint, api_key)

        if response and "name" in response:
            print("✅ Business unit API call successful")
        else:
            print(f"❌ Business unit API call failed: {response}")
            return False

        # Test categories endpoint
        response = execute_api_request("/categories", api_key)

        if response and "categories" in response:
            print("✅ Categories API call successful")
        else:
            print(f"❌ Categories API call failed: {response}")
            return False

        print("✅ API connectivity tests passed")
        return True

    except Exception as e:
        print(f"❌ API connectivity test failed: {e}")
        return False


def main():
    """Main test runner"""
    print("🚀 Starting Trustpilot API Connector Test Suite")
    print("=" * 50)

    test_results = []

    # Run all tests
    test_results.append(("Basic Functionality", test_connector()))
    test_results.append(("Schema Validation", test_schema_only()))
    test_results.append(("Configuration Validation", test_configuration_validation()))
    test_results.append(("Endpoint Functions", test_endpoint_functions()))
    test_results.append(("API Connectivity", test_api_connectivity()))

    # Print summary
    print("\n" + "=" * 50)
    print("📊 Test Results Summary")
    print("=" * 50)

    passed = 0
    total = len(test_results)

    for test_name, result in test_results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name}: {status}")
        if result:
            passed += 1

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Connector is ready for deployment.")
        return 0
    else:
        print("⚠️  Some tests failed. Please review the issues above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
