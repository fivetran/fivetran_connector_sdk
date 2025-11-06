# AroFlo API Connector Example

## Connector overview
This connector synchronizes users, suppliers, and payments data from AroFlo's field service management platform. It demonstrates memory-efficient streaming patterns, comprehensive error handling, and incremental synchronization using the Fivetran Connector SDK. The connector is designed to handle real-world scenarios including API rate limiting, large datasets, and network connectivity issues while maintaining data integrity and sync reliability.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs user/employee data, supplier information, and payment transactions from AroFlo API
- Bearer token authentication with configurable timeout and retry settings (refer to the `execute_api_request` function)
- Page-based pagination with automatic page traversal for large datasets (refer to the `get_users_data`, `get_suppliers_data`, and `get_payments_data` functions)
- Memory-efficient streaming prevents data accumulation using generator patterns
- Incremental synchronization using timestamp-based cursors with configurable lookback periods (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic and rate limit support (refer to the `__handle_rate_limit` and `__handle_request_error` functions)
- Configurable sync options to enable/disable specific data types independently
- Production-ready logging with appropriate log levels for monitoring and debugging

## Configuration file
```json
{
  "api_key": "<YOUR_AROFLO_API_KEY>",
  "sync_frequency_hours": "<SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<RETRY_ATTEMPTS>",
  "enable_users_sync": "<ENABLE_USERS_SYNC>",
  "enable_suppliers_sync": "<ENABLE_SUPPLIERS_SYNC>",
  "enable_payments_sync": "<ENABLE_PAYMENTS_SYNC>",
  "enable_incremental_sync": "<ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<ENABLE_DEBUG_LOGGING>"
}
```

### Configuration parameters:
- `api_key` (required): Your AroFlo API authentication key
- `sync_frequency_hours` (optional): How often to run incremental syncs, defaults to 4 hours
- `initial_sync_days` (optional): How many days of historical data to fetch on first sync, defaults to 90 days
- `max_records_per_page` (optional): Number of records per API request, defaults to 100 (range: 1-1000)
- `request_timeout_seconds` (optional): HTTP request timeout in seconds, defaults to 30
- `retry_attempts` (optional): Number of retry attempts for failed requests, defaults to 3
- `enable_users_sync` (optional): Whether to sync users/employees data, defaults to true
- `enable_suppliers_sync` (optional): Whether to sync suppliers data, defaults to true
- `enable_payments_sync` (optional): Whether to sync payments data, defaults to true
- `enable_incremental_sync` (optional): Whether to use incremental sync, defaults to true
- `enable_debug_logging` (optional): Enable detailed debug logging, defaults to false

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [AroFlo Developer Portal](https://api.aroflo.com/v1).
2. Register a new application to obtain API credentials.
3. Make a note of the `api_key` from your application settings.
4. Retrieve your company ID from AroFlo administrators if required.
5. Use sandbox credentials for testing, production credentials for live syncing.

Note: The connector uses Bearer token authentication with automatic retry handling. Credentials are never logged or exposed in plain text.

## Pagination
Page-based pagination with automatic page traversal (refer to the `get_users_data`, `get_suppliers_data`, and `get_payments_data` functions). Generator-based processing prevents memory accumulation for large datasets. Processes pages sequentially while yielding individual records for immediate processing.

## Data handling
User, supplier, and payment data is mapped from AroFlo's API format to normalized database columns (refer to the `__map_user_data`, `__map_supplier_data`, and `__map_payment_data` functions). Nested objects are flattened, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days using the `initial_sync_days` parameter.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Network connectivity issues with automatic retry logic and detailed error logging
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues

## Tables created
| Table | Primary Key | Description | Sample Columns | 
|-------|-------------|-------------|-------------|
| USERS | `id` | Employee personal information, roles, and access levels | `first_name`, `last_name`, `email`, `phone`, `role`, `department`, `employee_id`, `status`, `access_level`, `territory`, `created_at`, `updated_at` |
| SUPPLIERS | `id` | Supplier contact information, payment terms, and status | `name`, `contact_person`, `email`, `phone`, `address`, `city`, `state`, `country`, `payment_terms`, `tax_id`, `account_code`, `status` |
| PAYMENTS | `id` | Payment transaction records, amounts, and reference data | `amount`, `currency`, `payment_date`, `payment_method`, `status`, `reference_number`, `customer_id`, `invoice_id`, `job_id`, `transaction_type`, `notes` |

Column types are automatically inferred by Fivetran. 

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
