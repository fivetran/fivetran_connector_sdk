# Lob Print & Mail API Connector

## Connector overview

This connector integrates with the Lob Print & Mail API to synchronize addresses and postcards data into your data warehouse. The Lob API provides access to print and mail services including direct mail automation, address verification, and tracking analytics. This connector enables data teams to analyze mail campaigns, track delivery performance, and maintain up-to-date address databases for compliance and analytics purposes.

The connector supports incremental syncing using date-based cursors and handles nested JSON structures by flattening them into warehouse-friendly formats.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Incremental data synchronization using date_created timestamps
- Automatic pagination handling for large datasets
- Flattening of nested JSON structures for optimal warehouse storage
- Retry logic with exponential backoff for API reliability
- Support for both test and live Lob API environments
- Configurable batch sizes for API requests
- Comprehensive error handling and logging

## Configuration file

The configuration file defines the connection parameters for the Lob API. Create a configuration.json file with the following structure:

```json
{
  "api_key": "<YOUR_LOB_API_KEY>",
  "start_date": "<YOUR_OPTIONAL_START_DATE>"
}
```

Configuration parameters:
- api_key (required): Your Lob API key (test_ or live_ prefixed)
- start_date (optional): Starting date for data synchronization in ISO format (default: 1970-01-01T00:00:00Z). This value is used as the "gt" (greater than) parameter in API calls

Note: The connector uses a default limit of 100 records per API request, which is defined as a constant in the code. Ensure that the configuration.json file is not checked into version control to protect sensitive information.

## Requirements file

The requirements.txt file specifies the Python libraries required by the connector:

```
requests==2.31.0
```

This connector uses the requests library for making HTTP calls to the Lob API with authentication and retry capabilities.

Note: The fivetran_connector_sdk:latest and requests:latest packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your requirements.txt.

## Authentication

The Lob API uses HTTP Basic authentication where your API key serves as the username and the password is left empty. The connector automatically handles this authentication method.

To obtain your API key:
1. Sign up for a Lob account at https://lob.com
2. Navigate to your Dashboard Settings
3. Find your API keys section
4. Use test keys for development and live keys for production

API keys are prefixed with either 'test_' for the testing environment or 'live_' for the production environment.

## Pagination

The connector implements cursor-based pagination using the Lob API's next_url field. Refer to the fetch_page_data function (lines 145-200) for the pagination implementation. The process:

1. Makes initial request with limit parameter
2. Processes returned data and extracts next_url from response
3. Continues fetching pages until next_url is null
4. Tracks total records processed across all pages

## Data handling

The connector processes data through several transformation steps and implements proper date range filtering:

1. **Date Range Filtering**: Every API request includes both 'gt' (greater than) and 'lt' (less than) parameters
  - gt: Uses last sync time from state, or configured start_date for first sync
  - lt: Uses current datetime to ensure we don't miss data created during sync
2. **Fetches raw JSON responses** from Lob API endpoints with proper date filtering
3. **Flattens nested dictionaries** using the flatten_record function (lines 324-360)
4. **Converts complex nested structures** into flat key-value pairs using underscore separation
5. **Handles arrays properly** by converting them to JSON strings for Fivetran compatibility
6. **Preserves data types** for warehouse optimization

**Array Field Handling**:
The Lob API returns several array fields (like `tracking_events`, `thumbnails`) that are not directly supported by Fivetran's data types. The connector handles these by:
- Converting arrays to JSON strings for storage
- Adding `_count` fields for important arrays like `tracking_events` and `thumbnails`
- Extracting useful metadata from array elements when applicable

Example transformation:
```json
// Original API response
{
  "tracking_events": [
    {"type": "processed", "date": "2025-09-20"},
    {"type": "shipped", "date": "2025-09-21"}
  ]
}

// Flattened for Fivetran
{
  "tracking_events": "[{\"type\":\"processed\",\"date\":\"2025-09-20\"},{\"type\":\"shipped\",\"date\":\"2025-09-21\"}]",
  "tracking_events_count": 2
}
```

**Incremental Sync Strategy**:
- First sync: Uses configured start_date parameter as gt, current time as lt
- Subsequent syncs: Uses previous sync end time as gt, current time as lt
- State management: Saves current time as endpoint_last_sync_time for next iteration

Schema mapping is handled automatically by Fivetran based on the flattened structure, with only primary keys explicitly defined in the schema function.

## Error handling

The connector implements comprehensive error handling strategies:

- HTTP status code handling with retry logic for transient errors (429, 500, 502, 503, 504)
- Exponential backoff retry strategy with configurable maximum attempts
- Request timeout handling for network issues
- Configuration validation with descriptive error messages
- Detailed logging for debugging and monitoring

Refer to the make_api_request and fetch_page_data functions for specific error handling implementations.

## Tables created

The connector creates the following tables in your data warehouse:

**addresses**
- Primary key: id
- Contains flattened address data including name, company, phone, email, and address components
- Tracks creation and modification timestamps

**postcards**
- Primary key: id
- Contains flattened postcard data including to/from addresses, status, URLs, and metadata
- Includes tracking events and delivery information as JSON strings
- Array fields like `tracking_events` and `thumbnails` are stored as JSON strings with accompanying `_count` fields

All nested objects (like 'to' and 'from' addresses in postcards) are flattened with underscore-separated field names for easy querying. Array fields are converted to JSON strings to ensure compatibility with Fivetran's supported data types.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.