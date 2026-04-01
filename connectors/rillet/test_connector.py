#!/usr/bin/env python3
"""
Test script for Rillet connector validation.
This script validates the connector logic without making real API calls.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

# Mock the fivetran_connector_sdk to avoid import errors


class MockConnector:
    def __init__(self, update, schema):
        self.update = update
        self.schema = schema

    def debug(self, configuration):
        print("🔧 MockConnector.debug() called")
        print(f"Configuration: {configuration}")

        # Test schema function
        schema_result = self.schema(configuration)
        print(f"Schema result: {schema_result}")

        # Test update function with empty state
        try:
            self.update(configuration, {})
            print("✅ Update function completed without errors")
        except Exception as e:
            print(f"❌ Update function failed: {e}")
            return False

        return True


class MockOperations:
    def upsert(self, table, data):
        print(f"📝 Mock upsert: table={table}, data keys={list(data.keys()) if isinstance(data, dict) else 'non-dict'}")

    def checkpoint(self, state):
        print(f"💾 Mock checkpoint: state keys={list(state.keys())}")


class MockLogging:
    def warning(self, msg):
        print(f"⚠️  {msg}")

    def info(self, msg):
        print(f"ℹ️  {msg}")

    def severe(self, msg):
        print(f"🚨 {msg}")


# Monkey patch the imports in the connector module to use the mocks
sys.modules['fivetran_connector_sdk'] = type(sys)('fivetran_connector_sdk')
sys.modules['fivetran_connector_sdk'].Connector = MockConnector
sys.modules['fivetran_connector_sdk'].Operations = MockOperations()
sys.modules['fivetran_connector_sdk'].Logging = MockLogging()

# Now import and test the connector
try:
    from connector import validate_configuration, schema, connector

    print("✅ Connector imports successful")

    # Test configuration validation
    test_config = {
        "api_key": "test_key",
        "base_url": "https://api.rillet.com",
        "api_version": "3"
    }

    try:
        validate_configuration(test_config)
        print("✅ Configuration validation passed")
    except Exception as e:
        print(f"❌ Configuration validation failed: {e}")
        sys.exit(1)

    # Test schema function
    try:
        schema_result = schema(test_config)
        expected_tables = ["account", "customer", "invoice", "product", "bill"]
        actual_tables = [t["table"] for t in schema_result]
        if set(actual_tables) == set(expected_tables):
            print("✅ Schema function returned expected tables")
        else:
            print(f"❌ Schema mismatch. Expected: {expected_tables}, Got: {actual_tables}")
    except Exception as e:
        print(f"❌ Schema function failed: {e}")
        sys.exit(1)

    # Test connector debug mode
    print("\n🧪 Testing connector debug mode...")
    success = connector.debug(test_config)

    if success:
        print("✅ All tests passed!")
    else:
        print("❌ Tests failed!")
        sys.exit(1)

except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Unexpected error: {e}")
    sys.exit(1)
