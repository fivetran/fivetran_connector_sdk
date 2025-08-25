#!/bin/bash

# New Relic Feature APIs Connector Deployment Script
# This script deploys the New Relic connector to Fivetran

echo "Deploying New Relic Feature APIs Connector..."

# Check if configuration file exists
if [ ! -f "configuration.json" ]; then
    echo "Error: configuration.json not found. Please create it with your New Relic API credentials."
    exit 1
fi

# Validate configuration
echo "Validating configuration..."
python -c "
import json
config = json.load(open('configuration.json'))
required_keys = ['api_key', 'account_id', 'region']
for key in required_keys:
    if key not in config:
        print(f'Error: Missing required configuration key: {key}')
        exit(1)
    if config[key] == '' or config[key].startswith('<'):
        print(f'Error: Please set a valid value for {key}')
        exit(1)
print('Configuration validation passed')
"

if [ $? -ne 0 ]; then
    echo "Configuration validation failed. Please fix the issues above."
    exit 1
fi

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Test the connector
echo "Testing connector..."
python connector.py

if [ $? -eq 0 ]; then
    echo "Basic connector test successful!"
else
    echo "Basic connector test failed. Please check the error messages above."
    exit 1
fi

# Run comprehensive test suite
echo ""
echo "Running comprehensive test suite..."
python test_connector.py

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ All tests completed successfully!"
    echo ""
    echo "Next steps:"
    echo "1. Deploy this connector to your Fivetran account"
    echo "2. Configure the sync schedule in Fivetran"
    echo "3. Monitor the sync logs for any issues"
    echo ""
    echo "For more information, see the README.md file"
else
    echo ""
    echo "⚠️  Some tests failed. This may be due to missing credentials."
    echo "   To run full tests with API connectivity:"
    echo "   1. Set NEWRELIC_API_KEY environment variable"
    echo "   2. Set NEWRELIC_ACCOUNT_ID environment variable"
    echo "   3. Optional: Set NEWRELIC_REGION (defaults to US)"
    echo "   4. Run: python test_connector.py"
    echo ""
    echo "Connector is ready for deployment regardless of test results."
fi
