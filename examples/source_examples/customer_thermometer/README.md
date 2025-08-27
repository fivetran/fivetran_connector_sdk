# Customer Thermometer API Connector Example

## Connector overview

The [Customer Thermometer](https://www.customerthermometer.com/) custom connector for Fivetran fetches customer feedback data from the Customer Thermometer API and syncs it to your destination. This connector supports multiple endpoints including comments, blast results, recipient lists, thermometers, and feedback metrics. 

The connector implements API key authentication, parses XML responses, and is stateless, following Fivetran best practices for reliability, security, and maintainability.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

The connector supports the following features:

- Multiple endpoints: Supports comments, blast results, recipient lists, thermometers, and metrics
- API key authentication: Secure authentication using Customer Thermometer API keys
- XML parsing: Converts XML API responses to structured records

- Error handling: Individual record and API error handling without stopping entire sync
- Comprehensive logging: Uses Fivetran's logging framework for troubleshooting and monitoring
- SDK v2.0.0+ compliance: Uses direct operation calls (no yield statements)

## Configuration file

The connector requires API key authentication for the Customer Thermometer API. Date filters are optional.

```json
{
  "api_key": "<YOUR_CUSTOMER_THERMOMETER_API_KEY>",
  "from_date": "<FROM_DATE AS YYYY-MM-DD>",
  "to_date": "<TO_DATE AS YYYY-MM-DD>"
}
```

- `api_key`: (Required) Your Customer Thermometer API key
- `from_date`: (Optional) Start date for data retrieval in YYYY-MM-DD format
- `to_date`: (Optional) End date for data retrieval in YYYY-MM-DD format

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector example uses standard libraries provided by Python and does not require any additional packages.

Note: The `fivetran_connector_sdk` and `requests` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

```
# Core dependencies are pre-installed in Fivetran environment
```

Note: The `fivetran_connector_sdk` and `requests` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses API key authentication with the Customer Thermometer API. Authentication is handled in the `make_api_request` and `validate_configuration` functions:

- Uses a pre-configured API key for all API requests
- Passes the key as a parameter in each request
- Validates key presence and format during connector initialization
- Handles authentication errors gracefully with detailed logging

To obtain the necessary credentials:
1. Log in to your Customer Thermometer account.
2. Navigate to **Account Settings** > **API**.
3. Generate a new API key or use an existing one.

## Data handling

The connector processes XML data from the Customer Thermometer API and transforms it into structured records using specialized functions:

- XML parsing: Uses `parse_xml_response` to convert XML to dictionaries
- Data extraction: Uses endpoint-specific functions (`fetch_comments`, `fetch_blast_results`, etc.)
- Data preservation: Maintains all original field names and values where possible
- Primary key generation: Uses natural keys from the API (id fields)
- Upsert operations: Delivers data to Fivetran using upsert operations for each record

## Error handling

The connector implements comprehensive error handling strategies in the `update` function and API request helpers:

- Individual record handling: Failed records don't stop the entire sync process
- API error management: HTTP and XML errors are caught and logged with severity levels
- Timeout configuration: 30-second timeout for API requests to prevent hanging
- Detailed logging: Uses Fivetran's logging framework (INFO, WARNING, ERROR levels)


Refer to the `update` and `make_api_request` functions for error handling and record processing logic.

## Tables created

The connector creates the following tables in your destination:

| Table name        | Primary key                              | Description                                 |
|-------------------|------------------------------------------|---------------------------------------------|
| `comments`        | `[response_id]`                          | Customer feedback comments                  |
| `blast_results`   | `[blast_id, response_id, thermometer_id]`| Results from feedback collection campaigns  |
| `recipient_lists` | `[id]`                                   | Lists of feedback recipients                |
| `thermometers`    | `[id]`                                   | Configured feedback collection tools        |
| `metrics`         | `[metric_name, recorded_at]`             | Aggregated feedback metrics                 |

All tables include flattened versions of complex nested objects where applicable.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.