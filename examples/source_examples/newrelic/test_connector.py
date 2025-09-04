import os
import requests
from datetime import datetime, timezone
from connector import schema, validate_configuration, get_newrelic_endpoint, connector


def test_connector():
    """Test the connector locally using the connector object"""

    print("Testing New Relic Feature APIs Connector...")
    print(f"Test started at: {datetime.now(timezone.utc)}")

    # Sample configuration for testing
    test_config = {
        "api_key": "NRAK-TEST123456789",
        "account_id": "123456789",
        "region": "US",
    }

    print("Configuration loaded successfully")

    try:
        # Test schema function
        print("\n🔍 Testing schema function...")
        schema_result = schema(test_config)
        print(f"✅ Schema test passed! Generated {len(schema_result)} tables:")
        for table in schema_result:
            print(f"   - {table['table']}: {len(table['columns'])} columns")

        # Test connector debug mode
        print("\n🚀 Testing connector in debug mode...")
        connector.debug(configuration=test_config)
        print("✅ Connector debug test completed successfully!")

        return True

    except Exception as e:
        print(f"❌ Connector test failed: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        return False


def test_schema_only():
    """Test only the schema function without New Relic API calls"""

    print("Testing connector schema function only...")

    # Minimal test configuration
    test_config = {"api_key": "test", "account_id": "test", "region": "US"}

    try:
        schema_result = schema(test_config)
        print(f"✅ Schema test passed! Generated {len(schema_result)} tables:")

        # Display table details
        for table in schema_result:
            print(f"\n📊 Table: {table['table']}")
            print(f"   Primary Key: {', '.join(table['primary_key'])}")
            print(f"   Columns: {len(table['columns'])}")

            # Show first few columns
            columns = list(table["columns"].items())[:5]
            for col_name, col_type in columns:
                print(f"     - {col_name}: {col_type}")

            if len(table["columns"]) > 5:
                print(f"     ... and {len(table['columns']) - 5} more columns")

        return True

    except Exception as e:
        print(f"❌ Schema test failed: {str(e)}")
        return False


def test_configuration_validation():
    """Test configuration validation functions"""

    print("Testing configuration validation...")

    # Test validate_configuration function
    print("\n🔍 Testing validate_configuration function...")

    # Valid configurations
    valid_configs = [
        {"api_key": "NRAK-ABC123DEF456", "account_id": "123456789", "region": "US"},
        {"api_key": "NRAK-TEST123456789", "account_id": "987654321", "region": "EU"},
        # Test extended configuration with all optional parameters
        {
            "api_key": "NRAK-EXTENDED123456",
            "account_id": "123456789",
            "region": "US",
            "sync_frequency_minutes": "30",
            "initial_sync_days": "60",
            "max_records_per_query": "500",
            "enable_apm_data": "true",
            "enable_infrastructure_data": "false",
            "enable_browser_data": "true",
            "enable_mobile_data": "false",
            "enable_synthetic_data": "true",
            "timeout_seconds": "60",
            "retry_attempts": "5",
            "retry_delay_seconds": "10",
            "data_quality_threshold": "0.90",
            "alert_on_errors": "false",
            "log_level": "DEBUG",
        },
    ]

    for i, config in enumerate(valid_configs, 1):
        try:
            validate_configuration(config)
            print(f"   ✅ Valid config {i} passed validation")
        except Exception as e:
            print(f"   ❌ Valid config {i} failed validation: {str(e)}")

    # Invalid configurations
    invalid_configs = [
        {"api_key": "INVALID-KEY", "account_id": "123456789", "region": "US"},
        {
            "api_key": "NRAK-ABC123DEF456",
            "account_id": "123456789",
            "region": "INVALID",
        },
        {"account_id": "123456789", "region": "US"},
        {"api_key": "NRAK-ABC123DEF456", "region": "US"},
        # Test extended configuration validation
        {
            "api_key": "NRAK-TEST",
            "account_id": "123456",
            "region": "US",
            "sync_frequency_minutes": "0",
        },
        {
            "api_key": "NRAK-TEST",
            "account_id": "123456",
            "region": "US",
            "initial_sync_days": "0",
        },
        {
            "api_key": "NRAK-TEST",
            "account_id": "123456",
            "region": "US",
            "max_records_per_query": "0",
        },
        {
            "api_key": "NRAK-TEST",
            "account_id": "123456",
            "region": "US",
            "timeout_seconds": "0",
        },
        {
            "api_key": "NRAK-TEST",
            "account_id": "123456",
            "region": "US",
            "retry_attempts": "-1",
        },
        {
            "api_key": "NRAK-TEST",
            "account_id": "123456",
            "region": "US",
            "data_quality_threshold": "1.5",
        },
        {
            "api_key": "NRAK-TEST",
            "account_id": "123456",
            "region": "US",
            "log_level": "INVALID",
        },
    ]

    for i, config in enumerate(invalid_configs, 1):
        try:
            validate_configuration(config)
            print(f"   ❌ Invalid config {i} should have failed validation")
        except Exception as e:
            print(f"   ✅ Invalid config {i} correctly rejected: {str(e)}")

    print("\n✅ Configuration validation tests completed!")
    return True


def test_endpoint_functions():
    """Test utility functions"""

    print("Testing utility functions...")

    # Test get_newrelic_endpoint function
    print("\n🔍 Testing get_newrelic_endpoint function...")

    test_cases = [
        ("US", "https://api.newrelic.com"),
        ("EU", "https://api.eu.newrelic.com"),
    ]

    for region, expected in test_cases:
        result = get_newrelic_endpoint(region)
        status = "✅" if result == expected else "❌"
        print(
            f"   {status} get_newrelic_endpoint('{region}') = {result} (expected: {expected})"
        )

    print("\n✅ Utility function tests completed!")
    return True


def test_api_connectivity():
    """Test New Relic API connectivity (requires valid credentials)"""

    print("Testing New Relic API connectivity...")

    # Check if New Relic credentials are available
    api_key = os.environ.get("NEWRELIC_API_KEY")
    account_id = os.environ.get("NEWRELIC_ACCOUNT_ID")
    region = os.environ.get("NEWRELIC_REGION", "US")

    if not api_key or not account_id:
        print("❌ New Relic credentials not found in environment variables")
        print("   Set NEWRELIC_API_KEY and NEWRELIC_ACCOUNT_ID environment variables")
        print("   Optional: Set NEWRELIC_REGION (defaults to US)")
        return False

    print(f"✅ New Relic credentials found")
    print(f"   Account ID: {account_id}")
    print(f"   Region: {region}")

    try:
        # Test basic API connectivity
        endpoint = get_newrelic_endpoint(region)
        url = f"{endpoint}/graphql"

        headers = {"API-Key": api_key, "Content-Type": "application/json"}

        # Simple test query
        query = f"""
        {{
          actor {{
            account(id: {account_id}) {{
              id
              name
            }}
          }}
        }}
        """

        payload = {"query": query}

        print(f"\n🔍 Testing API connectivity to {url}...")
        response = requests.post(url, json=payload, headers=headers, timeout=10)

        if response.status_code == 200:
            data = response.json()
            if "data" in data and "actor" in data["data"]:
                print("✅ API connectivity test passed!")
                print(f"   Account name: {data['data']['actor']['account']['name']}")
                return True
            else:
                print("❌ API response format unexpected")
                print(f"   Response: {data}")
                return False
        else:
            print(f"❌ API request failed with status {response.status_code}")
            print(f"   Response: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"❌ API connectivity test failed: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error during API test: {str(e)}")
        return False


def main():
    """Main test runner"""

    print("=" * 60)
    print("New Relic Feature APIs Connector - Test Suite")
    print("=" * 60)

    # Test 1: Schema only (no API calls)
    print("\n" + "=" * 40)
    print("TEST 1: Schema Function Test")
    print("=" * 40)
    test_schema_only()

    # Test 2: Configuration validation
    print("\n" + "=" * 40)
    print("TEST 2: Configuration Validation Test")
    print("=" * 40)
    test_configuration_validation()

    # Test 3: Utility functions
    print("\n" + "=" * 40)
    print("TEST 3: Utility Functions Test")
    print("=" * 40)
    test_endpoint_functions()

    # Test 4: API connectivity (requires valid credentials)
    print("\n" + "=" * 40)
    print("TEST 4: API Connectivity Test")
    print("=" * 40)
    print("⚠️  This test requires valid New Relic credentials")
    print("   Set NEWRELIC_API_KEY and NEWRELIC_ACCOUNT_ID environment variables")
    print("   Optional: Set NEWRELIC_REGION (defaults to US)")

    # Check if New Relic credentials are available
    api_key = os.environ.get("NEWRELIC_API_KEY")
    account_id = os.environ.get("NEWRELIC_ACCOUNT_ID")

    if api_key and account_id:
        print(f"✅ New Relic credentials found in environment variables")
        print("   Running API connectivity test...")
        test_api_connectivity()
    else:
        print("❌ New Relic credentials not found in environment variables")
        print("   Skipping API connectivity test")
        print("\nTo run the API connectivity test:")
        print("1. Set NEWRELIC_API_KEY environment variable")
        print("2. Set NEWRELIC_ACCOUNT_ID environment variable")
        print("3. Optional: Set NEWRELIC_REGION (defaults to US)")
        print("4. Run: python test_connector.py")

    # Test 5: Full connector test (requires valid credentials)
    print("\n" + "=" * 40)
    print("TEST 5: Full Connector Test")
    print("=" * 40)
    print("⚠️  This test requires valid New Relic credentials")

    if api_key and account_id:
        print(f"✅ New Relic credentials found in environment variables")
        print("   Running full connector test...")
        test_connector()
    else:
        print("❌ New Relic credentials not found in environment variables")
        print("   Skipping full connector test")
        print("\nTo run the full connector test:")
        print("1. Set NEWRELIC_API_KEY environment variable")
        print("2. Set NEWRELIC_ACCOUNT_ID environment variable")
        print("3. Optional: Set NEWRELIC_REGION (defaults to US)")
        print("4. Run: python test_connector.py")

    print("\n" + "=" * 60)
    print("Test Suite Completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
