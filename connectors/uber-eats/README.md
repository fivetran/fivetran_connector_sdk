# Uber Eats API Connector Example

## Connector overview

This connector syncs data from Uber Eats API including stores, orders, promotions, and menu items. The connector demonstrates how to fetch restaurant data, order information, promotional campaigns, and menu details using memory-efficient streaming patterns with comprehensive error handling and retry logic.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Syncs store data, order transactions, promotions, and menu items from Uber Eats API
- Bearer token authentication with automatic error handling (refer to `execute_api_request` function)
- Offset-based pagination with automatic page traversal (refer to `get_stores`, `get_orders`, `get_promotions`, and `get_menus` functions)
- Memory-efficient streaming prevents data accumulation for large datasets
- Incremental synchronization using timestamp-based cursors (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic (refer to `__handle_rate_limit` and `__handle_request_error` functions)
- Rate limiting detection and automatic retry with jitter to prevent thundering herd problems

## Configuration file

```json
{
  "api_key": "<YOUR_UBER_EATS_API_KEY>",
  "sync_frequency_hours": "<SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<ENABLE_DEBUG_LOGGING>"
}
```

### Configuration parameters

- `api_key` (required): Your Uber Eats API authentication key
- `base_url` (optional): Base URL for the Uber Eats API (defaults to `https://api.uber.com/v2/eats`)
- `sync_frequency_hours` (optional): How often to run syncs in hours (default: 4)
- `initial_sync_days` (optional): Number of days of historical data to fetch on first sync (default: 90)
- `max_records_per_page` (optional): Maximum records per API request (default: 100, range: 1-1000)
- `request_timeout_seconds` (optional): HTTP request timeout in seconds (default: 30)
- `retry_attempts` (optional): Number of retry attempts for failed requests (default: 3)
- `enable_incremental_sync` (optional): Enable incremental sync using timestamps (default: true)
- `enable_debug_logging` (optional): Enable detailed debug logging (default: false)

## Requirements file

This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

1. Log in to the [Uber Developer Portal](https://developer.uber.com/).
2. Register a new application to obtain API credentials.
3. Make a note of the API key from your application settings.
4. Retrieve your restaurant or partner account ID from Uber Eats administrators.
5. Use sandbox credentials for testing, production credentials for live syncing.

Note: The connector uses Bearer token authentication with automatic error handling. Credentials are never logged or exposed in plain text.

## Pagination

Offset-based pagination with automatic page traversal (refer to `get_stores`, `get_orders`, `get_promotions`, and `get_menus` functions). Generator-based processing prevents memory accumulation for large datasets. Processes pages sequentially while yielding individual records for immediate processing.

## Data handling

Restaurant and order data is mapped from Uber Eats API format to normalized database columns (refer to the `__map_store_data`, `__map_order_data`, `__map_promotion_data`, and `__map_menu_data` functions). Nested objects like addresses and delivery information are flattened, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days.

## Error handling

- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues
- Network connectivity issues with automatic retry logic
- Invalid authentication handling with clear error messages

## Tables created

| Table | Primary Key | Description |
|-------|-------------|-------------|
| STORES | `id` | Restaurant and store information including location, ratings, and operational details |
| ORDERS | `id` | Order transaction data with payment details, delivery information, and status |
| PROMOTIONS | `id` | Promotional campaigns and discount information with usage limits and validity periods |
| MENUS | `id` | Menu items with pricing, availability, nutritional information, and customization options |

Column types are automatically inferred by Fivetran. Sample columns include:

**STORES table**: `id`, `name`, `slug`, `address`, `city`, `state`, `postal_code`, `latitude`, `longitude`, `phone`, `cuisine_type`, `rating`, `review_count`, `delivery_fee`, `minimum_order`, `is_active`, `created_at`, `updated_at`, `synced_at`

**ORDERS table**: `id`, `store_id`, `customer_id`, `order_number`, `status`, `order_type`, `subtotal`, `tax_amount`, `delivery_fee`, `service_fee`, `tip_amount`, `total_amount`, `currency`, `payment_method`, `delivery_address`, `estimated_delivery_time`, `actual_delivery_time`, `created_at`, `updated_at`, `synced_at`

**PROMOTIONS table**: `id`, `store_id`, `name`, `description`, `promotion_type`, `discount_type`, `discount_value`, `minimum_order_value`, `maximum_discount`, `usage_limit`, `usage_count`, `is_active`, `start_date`, `end_date`, `terms_and_conditions`, `created_at`, `updated_at`, `synced_at`

**MENUS table**: `id`, `store_id`, `name`, `description`, `category`, `price`, `currency`, `image_url`, `calories`, `allergens`, `ingredients`, `customizations`, `is_available`, `preparation_time`, `spice_level`, `created_at`, `updated_at`, `synced_at`

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for Uber Eats API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.