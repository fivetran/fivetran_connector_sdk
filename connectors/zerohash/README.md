# ZeroHash API Connector Example

## Connector overview
This connector syncs participants, accounts, and assets data from the ZeroHash API. It demonstrates how to implement HMAC-SHA256 authentication, handle cryptocurrency and digital asset data, and process financial information using memory-efficient streaming patterns. The connector fetches participant information, account balances, and supported asset definitions from ZeroHash's certification environment.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs participants, accounts, and assets data from ZeroHash API
- HMAC-SHA256 authentication with automatic signature generation (refer to `__generate_signature` function)
- Memory-efficient streaming prevents data accumulation for large datasets (refer to `get_participants`, `get_accounts`, and `get_assets` functions)
- Comprehensive error handling with exponential backoff retry logic (refer to `__handle_rate_limit` and `__handle_request_error` functions)
- Incremental synchronization using timestamp-based cursors (refer to `get_time_range` function)
- Cryptocurrency and digital asset data processing with proper decimal handling

## Configuration file
```json
{
  "api_key": "<YOUR_ZEROHASH_API_KEY>",
  "secret_key": "<YOUR_ZEROHASH_SECRET_KEY>",
  "sync_frequency_hours": "<SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<INITIAL_SYNC_DAYS>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<ENABLE_DEBUG_LOGGING>"
}
```

### Configuration parameters
- `api_key`: Your ZeroHash API public key (required)
- `secret_key`: Your ZeroHash API secret key for signing requests (required)
- `sync_frequency_hours`: How often to sync data in hours
- `initial_sync_days`: Number of days to sync on first run
- `request_timeout_seconds`: Timeout for API requests
- `retry_attempts`: Number of retry attempts for failed requests
- `enable_incremental_sync`: Enable timestamp-based incremental syncing
- `enable_debug_logging`: Enable detailed logging for troubleshooting

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [ZeroHash Developer Portal](https://api.cert.zerohash.com).
2. Register a new application to obtain API credentials.
3. Make a note of the `api_key` (public key) and `secret_key` from your application settings.
4. Retrieve your organization details from ZeroHash administrators.
5. Use certification environment credentials for testing, production credentials for live syncing.

Note: The connector automatically handles HMAC-SHA256 signature generation for each request. Credentials are never logged or exposed in plain text. Each request is signed with a timestamp and request details for security.

## Pagination
ZeroHash API responses are processed as complete datasets without pagination. The connector uses generator-based processing (refer to `get_participants`, `get_accounts`, and `get_assets` functions) to prevent memory accumulation while yielding individual records for immediate processing.

## Data handling
Financial and cryptocurrency data is mapped from ZeroHash's API format to normalized database columns (refer to the `__map_participant_data`, `__map_account_data`, and `__map_asset_data` functions). Account balances and asset amounts are preserved as strings to maintain precision. All timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days.

## Error handling
- 429 Rate Limited: Automatic retry with exponential backoff (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Authentication errors with clear guidance for credential setup
- Network connectivity issues with exponential backoff and jitter
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| PARTICIPANTS | `id` | Participant information including customers and businesses |
| ACCOUNTS | `id` | Account balances and asset holdings for each participant |
| ASSETS | `id` | Supported cryptocurrency and digital asset definitions |

Column types are automatically inferred by Fivetran. Sample columns include `email`, `participant_code`, `name`, `status`, `account_id`, `asset_symbol`, `balance`, `available_balance`, `symbol`, `decimals`, `minimum_amount`, `maximum_amount`.

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for ZeroHash API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.