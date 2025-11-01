# Dentally Connector Example

## Connector overview
This connector syncs dental practice management data from Dentally's API including practice sites, treatment rooms, and treatment procedures. The connector demonstrates OAuth2 authentication, memory-efficient streaming patterns, and comprehensive error handling for dental practice management systems.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs practice sites, treatment rooms, and treatment procedures from Dentally API
- OAuth2 authentication with automatic token refresh (refer to `refresh_access_token` function)
- Page-based pagination with automatic page traversal (refer to `get_rooms_data`, `get_sites_data`, and `get_treatments_data` functions)
- Memory-efficient streaming prevents data accumulation for large datasets
- Incremental synchronization using timestamp-based cursors (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic
- Supports both production and sandbox environments

## Configuration file
```json
{
  "client_id": "<YOUR_DENTALLY_CLIENT_ID>",
  "client_secret": "<YOUR_DENTALLY_CLIENT_SECRET>",
  "access_token": "<YOUR_DENTALLY_ACCESS_TOKEN>",
  "refresh_token": "<YOUR_DENTALLY_REFRESH_TOKEN>",
  "use_sandbox": "<YOUR_DENTALLY_USE_SANDBOX>",
  "sync_frequency_hours": "<YOUR_DENTALLY_SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<YOUR_DENTALLY_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_DENTALLY_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_DENTALLY_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_DENTALLY_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_DENTALLY_ENABLE_INCREMENTAL_SYNC>",
  "enable_rooms_sync": "<YOUR_DENTALLY_ENABLE_ROOMS_SYNC>",
  "enable_sites_sync": "<YOUR_DENTALLY_ENABLE_SITES_SYNC>",
  "enable_treatments_sync": "<YOUR_DENTALLY_ENABLE_TREATMENTS_SYNC>",
  "enable_debug_logging": "<YOUR_DENTALLY_ENABLE_DEBUG_LOGGING>"
}
```

**Configuration parameters:**
- `client_id`: OAuth2 client ID from Dentally Developer Portal
- `client_secret`: OAuth2 client secret from Dentally Developer Portal
- `access_token`: OAuth2 access token for API authentication
- `refresh_token`: OAuth2 refresh token for automatic token renewal
- `use_sandbox`: Set to "true" for sandbox environment testing
- `sync_frequency_hours`: How often to run sync (not used by connector logic)
- `initial_sync_days`: Number of days to sync for initial historical data
- `max_records_per_page`: Number of records per API request (1-100)
- `request_timeout_seconds`: HTTP request timeout in seconds
- `retry_attempts`: Number of retry attempts for failed requests
- `enable_incremental_sync`: Enable timestamp-based incremental synchronization
- `enable_rooms_sync`: Enable syncing of treatment room data
- `enable_sites_sync`: Enable syncing of practice site data
- `enable_treatments_sync`: Enable syncing of treatment procedure data
- `enable_debug_logging`: Enable detailed logging for troubleshooting

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Dentally Developer Portal](https://developer.dentally.co).
2. Register a new application to obtain OAuth2 credentials.
3. Make a note of the `client_id` and `client_secret` from your application settings.
4. Complete the OAuth2 authorization flow to obtain `access_token` and `refresh_token`.
5. Use sandbox credentials for testing, production credentials for live syncing.

Note: The connector automatically handles OAuth2 token refresh (tokens expire after 2 weeks). Credentials are never logged or exposed in plain text.

## Pagination
Page-based pagination with automatic page traversal (refer to `get_rooms_data`, `get_sites_data`, and `get_treatments_data` functions). Generator-based processing prevents memory accumulation for large datasets. Processes pages sequentially while yielding individual records for immediate processing.

## Data handling
Dental practice data is mapped from Dentally's API format to normalized database columns (refer to the `__map_room_data`, `__map_site_data`, and `__map_treatment_data` functions). Nested objects are flattened, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- 401 Unauthorized: Automatic token refresh and retry (refer to the `refresh_access_token` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| ROOMS | `id` | Treatment rooms and operatories in dental practices |
| SITES | `id` | Dental practice locations and clinic information |
| TREATMENTS | `id` | Dental procedures and treatment definitions |

Column types are automatically inferred by Fivetran. Sample columns include `name`, `room_number`, `equipment_type`, `address`, `phone`, `email`, `treatment_code`, `category`, `default_fee`, `duration_minutes`, `created_at`, `updated_at`.

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for Dentally API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.