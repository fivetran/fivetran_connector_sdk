#!/usr/bin/env python3
"""
Test suite for Quickbase API Connector
Tests connector functionality, configuration validation, and API connectivity
"""

import sys

# Import connector functions for testing
from connector import (
    validate_configuration,
    parse_configuration,
    get_time_range,
    schema,
    API_BASE_URL,
    SyncType,
    QuickBaseAPIClient,
)


def test_connector():
    """Test basic connector functionality"""
    print("Testing basic connector functionality...")

    # Test configuration validation
    test_config = {
        "user_token": "test_user_token_123",
        "realm_hostname": "test.quickbase.com",
        "app_id": "test_app_id_456",
        "table_ids": "table1,table2,table3",
        "sync_frequency_hours": "4",
        "initial_sync_days": "90",
        "max_records_per_page": "1000",
        "request_timeout_seconds": "30",
        "retry_attempts": "3",
        "enable_incremental_sync": "true",
        "enable_fields_sync": "true",
        "enable_records_sync": "true",
        "date_field_for_incremental": "3",
        "enable_debug_logging": "false",
    }

    try:
        config = parse_configuration(test_config)
        validate_configuration(config)
        print("✅ Configuration validation passed")
    except Exception as e:
        print(f"❌ Configuration validation failed: {e}")
        return False

    # Test endpoint constant
    endpoint = API_BASE_URL
    if endpoint == "https://api.quickbase.com/v1":
        print("✅ Endpoint constant passed")
    else:
        print(
            f"❌ Endpoint constant failed: expected 'https://api.quickbase.com/v1', got '{endpoint}'"
        )
        return False

    # Test time range function
    config = parse_configuration(test_config)
    time_range = get_time_range(SyncType.INITIAL, config)
    if "start" in time_range and "end" in time_range:
        print("✅ Time range function passed")
    else:
        print(f"❌ Time range function failed: {time_range}")
        return False

    # Test table IDs parsing (now part of parse_configuration)
    config = parse_configuration({"table_ids": "table1,table2, table3 ,table4"})
    if config.table_ids == ["table1", "table2", "table3", "table4"]:
        print("✅ Table IDs parsing passed")
    else:
        print(f"❌ Table IDs parsing failed: {config.table_ids}")
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
        "user_token": "test_user_token_123",
        "realm_hostname": "test.quickbase.com",
        "app_id": "test_app_id_456",
        "table_ids": "table1,table2,table3",
        "sync_frequency_hours": "4",
        "initial_sync_days": "90",
        "max_records_per_page": "1000",
        "request_timeout_seconds": "30",
        "retry_attempts": "3",
        "enable_incremental_sync": "true",
        "enable_fields_sync": "true",
        "enable_records_sync": "true",
        "date_field_for_incremental": "3",
        "enable_debug_logging": "false",
    }

    try:
        schema_result = schema(test_config)

        # Test that we get a list of tables
        if not isinstance(schema_result, list):
            print(f"❌ Schema should return a list, got {type(schema_result)}")
            return False

        # Test required tables
        expected_tables = [
            "applications",
            "tables",
            "fields",
            "records",
            "sync_metadata",
        ]
        actual_tables = [table["table"] for table in schema_result]

        for expected_table in expected_tables:
            if expected_table not in actual_tables:
                print(f"❌ Table '{expected_table}' not found in schema")
                return False
            else:
                print(f"✅ Table '{expected_table}' found in schema")

        # Test schema structure
        for table_schema in schema_result:
            if "table" not in table_schema:
                print(f"❌ Schema missing 'table' key: {table_schema}")
                return False

            if "primary_key" not in table_schema:
                print(
                    f"❌ Schema missing 'primary_key' key for table {table_schema['table']}"
                )
                return False

            if "columns" not in table_schema:
                print(
                    f"❌ Schema missing 'columns' key for table {table_schema['table']}"
                )
                return False

            print(f"✅ Table '{table_schema['table']}' has proper structure")

        print("✅ Schema validation passed")
        return True

    except Exception as e:
        print(f"❌ Schema function failed: {e}")
        return False


def test_configuration_validation():
    """Test configuration validation with edge cases"""
    print("\nTesting configuration validation...")

    # Test valid configuration
    valid_config = {
        "user_token": "valid_user_token_123",
        "realm_hostname": "valid.quickbase.com",
        "app_id": "valid_app_id_456",
        "sync_frequency_hours": "4",
        "initial_sync_days": "90",
        "max_records_per_page": "1000",
        "request_timeout_seconds": "30",
        "retry_attempts": "3",
        "enable_incremental_sync": "true",
        "enable_fields_sync": "true",
        "enable_records_sync": "true",
        "date_field_for_incremental": "3",
        "enable_debug_logging": "false",
    }

    try:
        config = parse_configuration(valid_config)
        validate_configuration(config)
        print("✅ Valid configuration passed")
    except Exception as e:
        print(f"❌ Valid configuration failed: {e}")
        return False

    # Test missing user_token
    invalid_config_1 = {
        "realm_hostname": "valid.quickbase.com",
        "app_id": "valid_app_id_456",
    }

    try:
        config = parse_configuration(invalid_config_1)
        validate_configuration(config)
        print("❌ Missing user_token should have failed")
        return False
    except ValueError as e:
        if "user_token" in str(e).lower():
            print("✅ Missing user_token correctly caught")
        else:
            print(f"❌ Unexpected error for missing user_token: {e}")
            return False

    # Test missing realm_hostname
    invalid_config_2 = {
        "user_token": "valid_user_token_123",
        "app_id": "valid_app_id_456",
    }

    try:
        config = parse_configuration(invalid_config_2)
        validate_configuration(config)
        print("❌ Missing realm_hostname should have failed")
        return False
    except ValueError as e:
        if "realm_hostname" in str(e).lower():
            print("✅ Missing realm_hostname correctly caught")
        else:
            print(f"❌ Unexpected error for missing realm_hostname: {e}")
            return False

    # Test missing app_id
    invalid_config_3 = {
        "user_token": "valid_user_token_123",
        "realm_hostname": "valid.quickbase.com",
    }

    try:
        config = parse_configuration(invalid_config_3)
        validate_configuration(config)
        print("❌ Missing app_id should have failed")
        return False
    except ValueError as e:
        if "app_id" in str(e).lower():
            print("✅ Missing app_id correctly caught")
        else:
            print(f"❌ Unexpected error for missing app_id: {e}")
            return False

    print("✅ Configuration validation tests passed")
    return True


def test_utility_functions():
    """Test utility functions"""
    print("\nTesting utility functions...")

    # Test endpoint constant
    endpoint = API_BASE_URL
    if endpoint == "https://api.quickbase.com/v1":
        print("✅ Endpoint constant returned correct URL")
    else:
        print(f"❌ Endpoint constant failed: {endpoint}")
        return False

    # Test time range function
    config = parse_configuration(
        {
            "user_token": "test",
            "realm_hostname": "test.com",
            "app_id": "test",
            "initial_sync_days": "30",
        }
    )
    time_range = get_time_range(SyncType.INITIAL, config)
    if isinstance(time_range, dict) and "start" in time_range and "end" in time_range:
        print("✅ Time range function returned proper structure")
    else:
        print(f"❌ Time range function failed: {time_range}")
        return False

    # Test incremental time range
    time_range_incremental = get_time_range(SyncType.INCREMENTAL, config)
    if (
        isinstance(time_range_incremental, dict)
        and "start" in time_range_incremental
        and "end" in time_range_incremental
    ):
        print("✅ Incremental time range function passed")
    else:
        print(f"❌ Incremental time range function failed: {time_range_incremental}")
        return False

    # Test table IDs parsing
    test_cases = [
        ("table1,table2,table3", ["table1", "table2", "table3"]),
        ("table1, table2 , table3", ["table1", "table2", "table3"]),
        ("", []),
        ("single_table", ["single_table"]),
    ]

    for input_str, expected in test_cases:
        config = parse_configuration({"table_ids": input_str})
        result = config.table_ids
        if result == expected:
            print(f"✅ Table IDs parsing for '{input_str}' passed")
        else:
            print(f"❌ Table IDs parsing failed for '{input_str}': {result}")
            return False

    print("✅ Utility function tests passed")
    return True


def test_api_client_initialization():
    """Test API client initialization"""
    print("\nTesting API client initialization...")

    test_config = {
        "user_token": "test_token_123",
        "realm_hostname": "test.quickbase.com",
        "app_id": "test_app_id_456",
    }

    try:
        config = parse_configuration(test_config)
        api_client = QuickBaseAPIClient(config)

        # Test that the client is initialized properly
        if api_client.config.user_token == "test_token_123":
            print("✅ API client user_token set correctly")
        else:
            print(f"❌ API client user_token failed: {api_client.config.user_token}")
            return False

        if api_client.config.realm_hostname == "test.quickbase.com":
            print("✅ API client realm_hostname set correctly")
        else:
            print(
                f"❌ API client realm_hostname failed: {api_client.config.realm_hostname}"
            )
            return False

        if api_client.base_url == API_BASE_URL:
            print("✅ API client base_url set correctly")
        else:
            print(f"❌ API client base_url failed: {api_client.base_url}")
            return False

        print("✅ API client initialization tests passed")
        return True

    except Exception as e:
        print(f"❌ API client initialization failed: {e}")
        return False


def main():
    """Main test function"""
    print("🚀 Starting Quickbase API Connector Test Suite")
    print("=" * 50)

    test_results = []

    # Run all tests
    test_results.append(("Basic Connector", test_connector()))
    test_results.append(("Schema Generation", test_schema_only()))
    test_results.append(("Configuration Validation", test_configuration_validation()))
    test_results.append(("Utility Functions", test_utility_functions()))
    test_results.append(("API Client Initialization", test_api_client_initialization()))

    # Print results summary
    print("\n" + "=" * 50)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 50)

    passed_tests = 0
    total_tests = len(test_results)

    for test_name, result in test_results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name:<30} {status}")
        if result:
            passed_tests += 1

    print("-" * 50)
    print(f"OVERALL RESULT: {passed_tests}/{total_tests} tests passed")

    if passed_tests == total_tests:
        print("🎉 ALL TESTS PASSED!")
        return 0
    else:
        print("⚠️  SOME TESTS FAILED. Please review the issues above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
