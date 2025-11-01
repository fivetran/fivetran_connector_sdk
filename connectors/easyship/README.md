# Easyship API Connector Example

## Connector overview
This connector syncs shipping data from Easyship API including boxes, manifests, and pickups information. The connector fetches box configurations with dimensions and courier details, shipping manifests with shipment counts and status tracking, and pickup schedules with contact information and delivery addresses. It supports incremental synchronization using timestamp-based cursors to efficiently process only updated records since the last sync.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs boxes, manifests, and pickups data from Easyship API
- Bearer token authentication with automatic header management (refer to `execute_api_request` function)
- Offset-based pagination with automatic page traversal (refer to `get_manifests_data` and `get_pickups_data` functions)
- Memory-efficient streaming prevents data accumulation for large datasets
- Incremental synchronization using timestamp-based cursors (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic
- Support for rate limiting with Retry-After header handling

## Configuration file
```json
{
  "api_key": "<YOUR_EASYSHIP_API_KEY>",
  "sync_frequency_hours": "<YOUR_EASYSHIP_API_SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<YOUR_EASYSHIP_API_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_EASYSHIP_API_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_EASYSHIP_API_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_EASYSHIP_API_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_EASYSHIP_API_ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<YOUR_EASYSHIP_API_ENABLE_DEBUG_LOGGING>"
}
```

### Configuration parameters
- `api_key`: Your Easyship API bearer token with required scopes
- `sync_frequency_hours`: How often to run full sync (default: 4 hours)
- `initial_sync_days`: Days of historical data to fetch on first sync (default: 90)
- `max_records_per_page`: Records per API request for pagination (default: 100, max: 1000)
- `request_timeout_seconds`: API request timeout in seconds (default: 30)
- `retry_attempts`: Number of retry attempts for failed requests (default: 3)
- `enable_incremental_sync`: Enable timestamp-based incremental sync (default: true)
- `enable_debug_logging`: Enable detailed debug logging (default: false)

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Easyship Developer Portal](https://www.easyship.com/developers).
2. Navigate to API settings and generate a new API key.
3. Ensure your API key has the following scopes: `public.box:read`, `public.manifest:read`, `public.pickup:read`.
4. Copy the bearer token to use as your `api_key` configuration parameter.
5. Use sandbox credentials for testing, production credentials for live syncing.

Note: The connector automatically handles bearer token authentication through request headers. API keys are never logged or exposed in plain text.

## Pagination
Offset-based pagination with automatic page traversal (refer to `get_manifests_data` and `get_pickups_data` functions). Generator-based processing prevents memory accumulation for large datasets. Processes pages sequentially while yielding individual records for immediate processing. Boxes endpoint returns all data in single response without pagination.

## Data handling
Shipping data is mapped from Easyship's API format to normalized database columns (refer to the `__map_box_data`, `__map_manifest_data`, and `__map_pickup_data` functions). Nested objects are flattened, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| BOXES | `id` | Box configurations with dimensions and courier information |
| MANIFESTS | `id` | Shipping manifests with shipment counts and status tracking |
| PICKUPS | `id` | Pickup schedules with contact information and delivery addresses |

Column types are automatically inferred by Fivetran. Sample columns include `slug`, `dimensions`, `courier_name`, `manifest_number`, `status`, `pickup_date`, `contact_name`, `address`, `shipment_count`.

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for Easyship API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.