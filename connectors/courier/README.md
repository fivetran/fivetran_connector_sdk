# Connector SDK Courier API Connector Example

## Connector overview

This connector fetches data from the Courier API, a multi-channel notification platform that enables businesses to send notifications across email, SMS, push, and other channels. The connector syncs key operational data including audit events for compliance tracking, brand configurations, audience segments, message logs, subscriber lists, and notification templates. This enables organizations to analyze notification delivery patterns, track user engagement, monitor system usage for compliance, and optimize their messaging strategies.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Syncs audit events with timestamp-based incremental updates for compliance tracking
- Syncs brand configuration data with update tracking
- Syncs audience segments using cursor-based pagination
- Syncs message logs for delivery tracking
- Syncs subscriber lists for contact management
- Syncs notification templates
- Implements exponential backoff retry logic for transient API failures
- Supports checkpointing for reliable state management and resumption
- Flattens nested JSON structures for simplified analytics

## Configuration file

The connector requires the following configuration parameters:

```json
{
  "api_key": "<YOUR_COURIER_API_KEY>"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector does not require any additional packages beyond the pre-installed libraries in the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector uses Bearer token authentication to access the Courier API. The API key is passed in the Authorization header as a Bearer token for all requests (refer to the `get_headers()` function).

To obtain your API key:

1. Log in to your Courier account at https://app.courier.com.
2. Navigate to **Settings > API Keys**.
3. Copy your API key (either test key for testing or production key for live data).
4. Add the API key to your `configuration.json` file.

## Pagination

The connector implements different pagination strategies based on the endpoint (refer to individual fetch functions):

- Cursor-based pagination - Used for the audiences endpoint. The connector processes pages sequentially using cursor tokens returned in the API response and checkpoints after each page (refer to the `fetch_audiences()` function).
- Offset pagination - Used for messages, notifications, lists, brands, and audit events endpoints. These endpoints return paginated results with a `more` indicator in the paging object.

## Data handling

The connector processes data as follows:

- Flattening - All nested JSON objects are flattened into single-level dictionaries with underscore-separated key names using the `flatten_nested_object()` function. For example, `settings.colors.primary` becomes `settings_colors_primary`.
- Incremental sync - The connector tracks the latest timestamp for brands and audit events in the state to enable incremental updates on subsequent syncs.
- Streaming - Data is processed record-by-record without loading entire datasets into memory, enabling efficient handling of large data volumes.
- Type inference - Column data types are automatically inferred by Fivetran based on the data values.

## Error handling

The connector implements comprehensive error handling strategies (refer to the `make_api_request()` function):

- Retry logic - Implements exponential backoff for retryable HTTP status codes (429, 500, 502, 503, 504) with up to 3 retry attempts. The delay doubles with each attempt starting from 1 second.
- Network errors - Automatically retries on timeout and connection errors using the same exponential backoff strategy.
- Non-retryable errors - Immediately raises exceptions for client errors (4xx status codes except 429) without retry attempts.
- Validation - Validates required configuration parameters at the start of each sync (refer to the `validate_configuration()` function).

## Tables created

The connector creates seven tables in the destination (refer to the `schema()` function):

```json
{
  "table": "brand",
  "primary_key": ["id"]
}
```

```json
{
  "table": "audience",
  "primary_key": ["id"]
}
```

```json
{
  "table": "audit_event",
  "primary_key": ["auditEventId"]
}
```

```json
{
  "table": "list",
  "primary_key": ["id"]
}
```

```json
{
  "table": "message",
  "primary_key": ["id"]
}
```

```json
{
  "table": "notification",
  "primary_key": ["id"]
}
```

```json
{
  "table": "tenant",
  "primary_key": ["id"]
}
```

Note: The connector uses schema inference, meaning column data types are automatically determined by Fivetran based on the API response data. Nested JSON objects from the API are flattened into single-level columns with underscore-separated names (refer to the `flatten_nested_object()` function).

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
