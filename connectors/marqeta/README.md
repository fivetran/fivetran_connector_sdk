# Marqeta API Connector Example

## Connector overview
This connector syncs users, businesses, and transactions data from the Marqeta Core API platform. Marqeta provides modern card issuing and payment processing infrastructure, enabling companies to build innovative fintech products. The connector fetches comprehensive data including user profiles, business entities, and detailed transaction records with support for incremental synchronization and memory-efficient processing of large datasets.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs user profiles, business entities, and transaction records from Marqeta Core API
- HTTP Basic authentication with automatic retry logic (refer to `execute_api_request` function)
- Index-based pagination with automatic page traversal (refer to `get_users`, `get_businesses`, and `get_transactions` functions)
- Memory-efficient streaming prevents data accumulation for large datasets
- Incremental synchronization using timestamp-based cursors (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic
- Rate limiting support with automatic retry after delays

## Configuration file
```json
{
  "username": "<YOUR_MARQETA_USERNAME>",
  "password": "<YOUR_MARQETA_PASSWORD>",
  "sync_frequency_hours": "<YOUR_MARQETA_API_SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<YOUR_MARQETA_API_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_MARQETA_API_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_MARQETA_API_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_MARQETA_API_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_MARQETA_API_ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<YOUR_MARQETA_API_ENABLE_DEBUG_LOGGING>"
}
```

### Configuration parameters
- `username`: Marqeta API username for HTTP Basic authentication
- `password`: Marqeta API password for HTTP Basic authentication
- `sync_frequency_hours`: How often to run sync operations
- `initial_sync_days`: Number of days to fetch for initial sync (1-365)
- `max_records_per_page`: Records per API page (1-500, default: 100)
- `request_timeout_seconds`: HTTP request timeout (10-120, default: 30)
- `retry_attempts`: Number of retry attempts for failed requests (1-5, default: 3)
- `enable_incremental_sync`: Enable timestamp-based incremental sync
- `enable_debug_logging`: Enable detailed logging for debugging

## Requirements file
This connector requires the `faker` package for testing mock data generation.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Marqeta Developer Portal](https://www.marqeta.com/docs).
2. Navigate to your application settings to obtain API credentials.
3. Make a note of the `username` and `password` for HTTP Basic authentication.
4. Retrieve your application token from Marqeta administrators.
5. Use sandbox credentials for testing, production credentials for live syncing.

Note: The connector uses HTTP Basic authentication with automatic retry logic. Credentials are never logged or exposed in plain text.

## Pagination
Index-based pagination with automatic page traversal (refer to `get_users`, `get_businesses`, and `get_transactions` functions). Generator-based processing prevents memory accumulation for large datasets. Processes pages sequentially while yielding individual records for immediate processing using `start_index` and `count` parameters.

## Data handling
User, business, and transaction data is mapped from Marqeta's API format to normalized database columns (refer to the `__map_user_data`, `__map_business_data`, and `__map_transaction_data` functions). Nested objects are flattened to JSON strings, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues
- Network connectivity errors with automatic retry logic

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| USERS | `token` | User profiles and personal information |
| BUSINESSES | `token` | Business entities and company information |
| TRANSACTIONS | `token` | Transaction records and payment data |

Column types are automatically inferred by Fivetran. Sample columns include:

**USERS table**: `token`, `first_name`, `last_name`, `email`, `phone`, `address1`, `city`, `state`, `created_time`, `last_modified_time`, `status`, `metadata`

**BUSINESSES table**: `token`, `business_name_legal`, `business_name_dba`, `business_type`, `ein`, `website`, `phone`, `address1`, `city`, `state`, `created_time`, `last_modified_time`, `status`, `metadata`

**TRANSACTIONS table**: `token`, `type`, `state`, `user_token`, `business_token`, `card_token`, `amount`, `currency_code`, `network`, `created_time`, `settlement_date`, `merchant`, `response`, `metadata`

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for Marqeta API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.