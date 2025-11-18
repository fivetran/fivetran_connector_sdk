# Razorpay API Connector Example

## Connector overview
This connector syncs orders, payments, settlements, and refunds data from Razorpay's payment gateway API. It provides comprehensive financial transaction data for businesses using Razorpay for payment processing, enabling detailed analysis of payment flows, settlement patterns, and refund management.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs orders, payments, settlements, and refunds data from Razorpay API
- Basic authentication with API key and secret (refer to the`execute_api_request` function)
- Skip/count-based pagination with automatic page traversal (refer to the `get_orders` function)
- Memory-efficient streaming prevents data accumulation for large datasets
- Incremental synchronization using timestamp-based cursors (refer to the `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic

## Configuration file
```json
{
  "api_key": "<YOUR_RAZORPAY_API_KEY>",
  "api_secret": "<YOUR_RAZORPAY_API_SECRET>",
  "sync_frequency_hours": "<YOUR_SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<YOUR_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<YOUR_ENABLE_DEBUG_LOGGING>"
}
```

### Configuration parameters
- `api_key` (required): Your Razorpay API key
- `api_secret` (required): Your Razorpay API secret
- `sync_frequency_hours` (optional): How often to sync data in hours (default: `4`)
- `initial_sync_days` (optional): Days of historical data to fetch on first sync (default: `90`)
- `max_records_per_page` (optional): Records per API page (1-100, default: `100`)
- `request_timeout_seconds` (optional): API request timeout (default: `30`)
- `retry_attempts` (optional): Number of retry attempts for failed requests (default: `3`)
- `enable_incremental_sync` (optional): Enable incremental sync with timestamps (default: `true`)
- `enable_debug_logging` (optional): Enable detailed debug logging (default: `false`)

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Razorpay Developer Portal](https://dashboard.razorpay.com/).
2. Navigate to **Settings** > **API Keys**.
3. Generate new API key or use an existing one.
4. Make a note of the **Key ID** (`api_key`) and **Key Secret** (`api_secret`).
5. Use test credentials for testing, live credentials for production syncing.

Note: The connector uses Basic authentication with base64-encoded credentials. Credentials are never logged or exposed in plain text.

## Pagination
Skip/count-based pagination with automatic page traversal (refer to `get_orders` function). Generator-based processing prevents memory accumulation for large transaction datasets. Processes pages sequentially while yielding individual records for immediate processing.

## Data handling
Transaction data is mapped from Razorpay's API format to normalized database columns (refer to the `__map_order_data` function). Nested objects like notes and acquirer_data are JSON-serialized, and all timestamps are preserved in Unix format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| ORDERS | `id` | Order information including amounts, currency, and status |
| PAYMENTS | `id` | Payment records with method, status, and transaction details |
| SETTLEMENTS | `id` | Settlement records with amounts, fees, and UTR numbers |
| REFUNDS | `id` | Refund records linked to payments with amounts and status |

Column types are automatically inferred by Fivetran.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
