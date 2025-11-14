#!/bin/bash

# Usage function
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Deploy a Fivetran connector with specified destination and connection"
    echo
    echo "OPTIONS:"
    echo "  -d, --destination NAME    Destination name (e.g., KARUN_VELURU_AWS_MYSQL)"
    echo "  -c, --connection NAME     Connection name (e.g., kveluru_s3_mysql_custom_connection)"
    echo "  -h, --help                Show this help message"
    echo
    echo "EXAMPLES:"
    echo "  $0 -d KARUN_VELURU_AWS_MYSQL -c kveluru_s3_mysql_custom_connection"
    echo "  $0 --destination KARUN_VELURU_AWS_MYSQL --connection kveluru_s3_mysql_custom_connection"
    echo "  $0  # Interactive mode - will prompt for destination and connection"
    echo
}

# Parse command line arguments
DESTINATION_NAME=""
CONNECTION_NAME=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--destination)
            DESTINATION_NAME="$2"
            shift 2
            ;;
        -c|--connection)
            CONNECTION_NAME="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# API Key (base64 encoded)
BASE64_API_KEY='WFY3VmxzaVFWdkFUVGl2TTpwMjVWd1FwQXNLenhtbHA0Z3dDTGhwd3h4U2xuRWV4ZQ=='

# Interactive input if not provided via command line
if [[ -z "$DESTINATION_NAME" ]]; then
    echo -n "Enter destination name: "
    read -r DESTINATION_NAME
fi

if [[ -z "$CONNECTION_NAME" ]]; then
    echo -n "Enter connection name: "
    read -r CONNECTION_NAME
fi

# Validate inputs
if [[ -z "$DESTINATION_NAME" ]]; then
    echo "Error: Destination name is required"
    exit 1
fi

if [[ -z "$CONNECTION_NAME" ]]; then
    echo "Error: Connection name is required"
    exit 1
fi

echo
echo "Deploying connector ${CONNECTION_NAME}"
echo "======================================"
echo "Destination: ${DESTINATION_NAME}"
echo "Connection:  ${CONNECTION_NAME}"
echo

# Check if configuration.json exists
if [[ ! -f "configuration.json" ]]; then
    echo "Error: configuration.json not found in current directory"
    exit 1
fi

# Deploy the connector
fivetran deploy --api-key "${BASE64_API_KEY}" --destination "${DESTINATION_NAME}" --connection "${CONNECTION_NAME}" --configuration configuration.json
