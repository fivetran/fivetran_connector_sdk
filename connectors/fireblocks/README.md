# Fireblocks API Connector Example

## Connector overview

This connector syncs vault accounts and wallet data from the Fireblocks API to your data warehouse. The connector fetches vault account information, cryptocurrency wallet balances, and asset details using Fireblocks API endpoints. It supports both initial and incremental synchronization using cursor-based pagination and implements comprehensive error handling with exponential backoff retry logic.

The connector processes data using memory-efficient streaming patterns to handle large datasets without memory accumulation. It automatically maps Fireblocks API response fields to normalized database columns and handles rate limiting, network timeouts, and authentication errors gracefully.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Syncs vault accounts and cryptocurrency wallet data from Fireblocks API
- Bearer token authentication with configurable API endpoints (see the `execute_api_request` function)
- Cursor-based pagination with automatic page traversal (see the `get_vault_accounts` and `get_vault_wallets` functions)
- Memory-efficient streaming prevents data accumulation for large datasets
- Incremental synchronization using timestamp-based cursors (see the `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic (see the `__handle_rate_limit` and `__handle_request_error` functions)
- Configurable batch sizes, timeouts, and retry attempts for optimal performance
- Support for both vault accounts and vault wallets data synchronization

## Configuration file

```json
{
  "api_key": "<YOUR_FIREBLOCKS_API_KEY>",
  "sync_frequency_hours": "<SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<ENABLE_INCREMENTAL_SYNC>",
  "enable_vault_wallets": "<ENABLE_VAULT_WALLETS>"
}
```

### Configuration Parameters
- `api_key` (required): Bearer token for Fireblocks API authentication
- `sync_frequency_hours` (optional): How often to run incremental syncs
- `initial_sync_days` (optional): Number of days of historical data to fetch on first sync
- `max_records_per_page` (optional): Batch size for API requests (1-1000)
- `request_timeout_seconds` (optional): HTTP request timeout in seconds
- `retry_attempts` (optional): Number of retry attempts for failed requests
- `enable_incremental_sync` (optional): Enable cursor-based incremental synchronization
- `enable_vault_wallets` (optional): Include vault wallets data in synchronization

## Requirements file

This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

1. Log in to the [Fireblocks Developer Portal](https://developers.fireblocks.com/).
2. Create a new API key or use existing credentials with appropriate permissions.
3. Make a note of the API key from your Fireblocks workspace settings.
4. Ensure the API key has permissions for vault accounts and wallets endpoints.
5. Use sandbox credentials for testing, production credentials for live syncing.

Note: The connector uses Bearer token authentication with automatic retry handling for expired or invalid tokens. API keys are never logged or exposed in plain text.

## Pagination

Cursor-based pagination with automatic page traversal (see the `get_vault_accounts` and `get_vault_wallets` functions). Generator-based processing prevents memory accumulation for large vault account and wallet datasets. Processes pages sequentially while yielding individual records for immediate processing.

The connector uses the `before` and `after` cursor parameters provided by the Fireblocks API to navigate through paginated results efficiently.

## Data handling

Vault account and wallet data is mapped from Fireblocks API format to normalized database columns (see the `__map_vault_account_data` and `__map_vault_wallet_data` functions). Nested objects like asset arrays are serialized as JSON strings, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (see the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days using the `initial_sync_days` parameter.

## Error handling

- 429 Rate Limited: Automatic retry with Retry-After header support (see the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (see the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues
- Authentication errors are logged with specific guidance for API key configuration
- Network connectivity issues trigger automatic retry with increasing delays

## Tables created

| Table | Primary Key | Description |
|-------|-------------|-------------|
| VAULT_ACCOUNTS | `id` | Vault account information, including names, settings, and associated assets |
| VAULT_WALLETS | `id` | Individual cryptocurrency wallet balances and blockchain details |

**VAULT_ACCOUNTS columns include:** `id`, `name`, `hiddenOnUI`, `customerRefId`, `autoFuel`, `assets`, `timestamp`

**VAULT_WALLETS columns include:** `id`, `vaultId`, `assetId`, `available`, `pending`, `staked`, `frozen`, `lockedAmount`, `blockHeight`, `blockHash`, `timestamp`

Column types are automatically inferred by Fivetran. Asset information in VAULT_ACCOUNTS is stored as JSON for flexible querying of nested cryptocurrency data.


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.