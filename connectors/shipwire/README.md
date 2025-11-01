# Shipwire API Connector Example

## Connector overview

This connector syncs orders, purchase orders, and products data from Shipwire's logistics platform API. It fetches order management data, inventory tracking information, and product catalog details from Shipwire's fulfillment system using memory-efficient streaming patterns. The connector supports incremental synchronization based on updated timestamps and handles Shipwire's rate limiting automatically.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Syncs orders, purchase orders, and products data from Shipwire API
- HTTP Basic authentication with username and password credentials (refer to `execute_api_request` function)
- Offset-based pagination with automatic page traversal (refer to `get_orders`, `get_purchase_orders`, and `get_products` functions)
- Memory-efficient streaming prevents data accumulation for large datasets
- Incremental synchronization using timestamp-based cursors (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic
- Configurable sync options to enable/disable specific data types
- Rate limiting compliance with automatic retry delays

## Configuration file

```json
{
  "username": "<YOUR_SHIPWIRE_USERNAME>",
  "password": "<YOUR_SHIPWIRE_PASSWORD>",
  "sync_frequency_hours": "<YOUR_SHIPWIRE_API_SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<YOUR_SHIPWIRE_API_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_SHIPWIRE_API_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_SHIPWIRE_API_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_SHIPWIRE_API_RETRY_ATTEMPTS>",
  "enable_orders": "<YOUR_SHIPWIRE_API_ENABLE_ORDERS>",
  "enable_purchase_orders": "<YOUR_SHIPWIRE_API_ENABLE_PURCHASE_ORDERS>",
  "enable_products": "<YOUR_SHIPWIRE_API_ENABLE_PRODUCTS>",
  "enable_debug_logging": "<YOUR_SHIPWIRE_API_ENABLE_DEBUG_LOGGING>"
}
```

### Configuration parameters

- `username`: Shipwire API username for Basic authentication
- `password`: Shipwire API password for Basic authentication
- `sync_frequency_hours`: Hours between sync runs (default: 4)
- `initial_sync_days`: Days of historical data to fetch on first sync (default: 90)
- `max_records_per_page`: Records per API request page (default: 100, max: 1000)
- `request_timeout_seconds`: HTTP request timeout (default: 30)
- `retry_attempts`: Number of retry attempts for failed requests (default: 3)
- `enable_orders`: Enable orders data sync (default: true)
- `enable_purchase_orders`: Enable purchase orders data sync (default: true)
- `enable_products`: Enable products data sync (default: true)
- `enable_debug_logging`: Enable detailed debug logging (default: false)

## Requirements file

This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

1. Log in to the Shipwire Developer Portal at https://www.shipwire.com/developers/.
2. Create a new application or use existing API credentials.
3. Make a note of your API `username` and `password` from your account settings.
4. Ensure your account has appropriate permissions to access orders, purchase orders, and products endpoints.
5. Use sandbox credentials for testing, production credentials for live syncing.

Note: The connector uses HTTP Basic authentication with automatic retry handling. Credentials are never logged or exposed in plain text.

## Pagination

Offset-based pagination with automatic page traversal (refer to `get_orders`, `get_purchase_orders`, and `get_products` functions). Generator-based processing prevents memory accumulation for large datasets. Processes pages sequentially while yielding individual records for immediate processing.

## Data handling

Order, purchase order, and product data is mapped from Shipwire's API format to normalized database columns (refer to the `__map_order_data`, `__map_purchase_order_data`, and `__map_product_data` functions). Nested objects like shipTo addresses and item arrays are preserved as JSON. All timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days using the `initial_sync_days` parameter.

## Error handling

- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues
- Authentication errors provide clear guidance for credential verification

## Tables created

| Table | Primary Key | Description |
|-------|-------------|-------------|
| ORDERS | `id` | Order management data including status, items, shipping details |
| PURCHASE_ORDERS | `id` | Purchase order tracking with vendor and warehouse information |
| PRODUCTS | `id` | Product catalog with SKUs, dimensions, classifications, and inventory flags |

Column types are automatically inferred by Fivetran. Sample columns include `order_number`, `status`, `total_value`, `currency`, `ship_to`, `items`, `po_number`, `warehouse_id`, `vendor_id`, `sku`, `description`, `category`, `dimensions`, `values`, `flags`.

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for Shipwire API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.