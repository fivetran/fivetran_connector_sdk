# AfterShip API Connector Example

## Connector overview

This connector syncs package tracking and courier data from the AfterShip API to your data warehouse. The connector fetches tracking information including checkpoints, delivery status, and shipment details, as well as courier capabilities and configuration data using AfterShip's tracking API endpoints. It supports both initial and incremental synchronization using page-based pagination and implements comprehensive error handling with exponential backoff retry logic.

The connector processes data using memory-efficient streaming patterns to handle large datasets without memory accumulation. It automatically maps AfterShip API response fields to normalized database columns and handles rate limiting, network timeouts, and authentication errors gracefully.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Syncs package trackings and courier data from AfterShip API
- API key authentication with aftership-api-key header format (refer to the `execute_api_request` function)
- Page-based pagination with automatic page traversal (refer to the `get_trackings` function)
- Memory-efficient streaming prevents data accumulation for large datasets
- Incremental synchronization using timestamp-based cursors (refer to the `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic (refer to the `__handle_rate_limit` and `__handle_request_error` functions)
- Configurable batch sizes, timeouts, and retry attempts for optimal performance
- Support for both trackings and couriers data synchronization

## Configuration file

```json
{
  "api_key": "<YOUR_AFTERSHIP_API_KEY>",
  "initial_sync_days": "<INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<ENABLE_INCREMENTAL_SYNC>",
  "enable_couriers": "<ENABLE_COURIERS>"
}
```

Configuration parameters:
- `api_key` (required): AfterShip API key for authentication
- `initial_sync_days` (optional): Number of days of historical data to fetch on first sync
- `max_records_per_page` (optional): Batch size for API requests (1-200)
- `request_timeout_seconds` (optional): HTTP request timeout in seconds (default: `30`)
- `retry_attempts` (optional): Number of retry attempts for failed requests (default: `3`)
- `enable_incremental_sync` (optional): Enable timestamp-based incremental synchronization (default: `true`)
- `enable_couriers` (optional): Include couriers data in synchronization (default: `true`)

## Requirements file

This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

1. Log in to the [AfterShip Developer Portal](https://admin.aftership.com/).
2. Navigate to API settings and create a new API key.
3. Make a note of the API key from your AfterShip account settings.
4. Ensure the API key has permissions for trackings and couriers endpoints.
5. Use sandbox credentials for testing, production credentials for live syncing.

Note: The connector uses `aftership-api-key` header authentication with automatic retry handling for expired or invalid tokens. API keys are never logged or exposed in plain text.

## Pagination

Page-based pagination with automatic page traversal (refer to the `get_trackings` function). Generator-based processing prevents memory accumulation for large tracking datasets. Processes pages sequentially while yielding individual records for immediate processing.

The connector uses the `page` and `limit` parameters provided by AfterShip API to navigate through paginated results efficiently, respecting the maximum limit of 200 records per page.

## Data handling

Tracking and courier data is mapped from AfterShip API format to normalized database columns (refer to the `__map_tracking_data` and `__map_courier_data` functions). Nested objects like checkpoints arrays are serialized as JSON strings, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 90 days using the `initial_sync_days` parameter.

## Error handling

- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues
- Authentication errors are logged with specific guidance for API key configuration
- Network connectivity issues trigger automatic retry with increasing delays

## Tables created

| Table | Primary Key | Description | Columns |
|-------|-------------|-------------|-------------|
| TRACKINGS | `id` | Package tracking information including status, checkpoints, and delivery details | `id`, `tracking_number`, `slug`, `active`, `customer_name`, `delivery_time`, `destination_country_iso3`, `expected_delivery`, `order_id`, `shipment_type`, `tag`, `checkpoints`, `timestamp` |
| COURIERS | `slug` | Shipping carrier information including capabilities and configuration | `slug`, `name`, `phone`, `web_url`, `required_fields`, `optional_fields`, `support_track`, `support_pickup`, `timestamp` |

Column types are automatically inferred by Fivetran. Checkpoint information in `TRACKINGS` is stored as JSON for flexible querying of nested tracking event data.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
