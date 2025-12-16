# Omnisend Connector Example

## Connector overview
This connector syncs products, contacts, and campaigns data from Omnisend's marketing automation platform. The connector fetches product catalog information, customer contact details, and email campaign performance metrics using memory-efficient streaming patterns for large datasets. Supports both initial full sync and incremental synchronization using timestamp-based cursors.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs product catalog with variants, images, and category information from Omnisend API
- X-API-KEY authentication with automatic retry and rate limit handling (refer to `execute_api_request` function)
- Offset-based pagination with automatic page traversal (refer to `get_products`, `get_contacts`, and `get_campaigns` functions)
- Memory-efficient streaming prevents data accumulation for large product catalogs and contact lists
- Incremental synchronization using timestamp-based cursors (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic for API failures
- Configurable sync options for individual data types (products, contacts, campaigns)

## Configuration file
```json
{
  "api_key": "<YOUR_OMNISEND_API_KEY>",
  "sync_frequency_hours": "<YOUR_OMNISEND_API_SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<YOUR_OMNISEND_API_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_OMNISEND_API_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_OMNISEND_API_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_OMNISEND_API_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_OMNISEND_API_ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<YOUR_OMNISEND_API_ENABLE_DEBUG_LOGGING>",
  "sync_products": "<YOUR_OMNISEND_API_SYNC_PRODUCTS>",
  "sync_contacts": "<YOUR_OMNISEND_API_SYNC_CONTACTS>",
  "sync_campaigns": "<YOUR_OMNISEND_API_SYNC_CAMPAIGNS>"
}
```

### Configuration parameters
- `api_key`: Your Omnisend API key for authentication
- `sync_frequency_hours`: How often to run sync in hours (default: 4)
- `initial_sync_days`: Number of days to sync initially (default: 90)
- `max_records_per_page`: Maximum records per API request (default: 100)
- `request_timeout_seconds`: API request timeout in seconds (default: 30)
- `retry_attempts`: Number of retry attempts for failed requests (default: 3)
- `enable_incremental_sync`: Enable timestamp-based incremental sync (default: true)
- `enable_debug_logging`: Enable detailed debug logging (default: false)
- `sync_products`: Enable product data synchronization (default: true)
- `sync_contacts`: Enable contact data synchronization (default: true)
- `sync_campaigns`: Enable campaign data synchronization (default: true)

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Omnisend Developer Portal](https://api-docs.omnisend.com/reference/overview).
2. Navigate to your account settings and locate the API section.
3. Generate a new API key with the required permissions for products, contacts, and campaigns.
4. Make a note of the `api_key` value for your configuration.
5. Ensure your API key has read permissions for all required data types.

Note: The connector uses X-API-KEY header authentication for most endpoints. API keys are never logged or exposed in plain text.

## Pagination
Offset-based pagination with automatic page traversal (refer to `get_products`, `get_contacts`, and `get_campaigns` functions). Generator-based processing prevents memory accumulation for large datasets. Processes pages sequentially while yielding individual records for immediate processing.

## Data handling
Product, contact, and campaign data are mapped from Omnisend's API format to normalized database columns (refer to the `__map_product_data`, `__map_contact_data`, and `__map_campaign_data` functions). Nested objects like variants, custom properties, and statistics are serialized as JSON. All timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues
- Authentication errors are clearly identified and reported with actionable guidance

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| PRODUCTS | `id` | Product catalog with variants, images, and categories |
| CONTACTS | `contact_id` | Customer contact information and channel preferences |
| CAMPAIGNS | `id` | Email campaign details and performance statistics |

Column types are automatically inferred by Fivetran. Sample columns include:

**PRODUCTS**: `id`, `title`, `description`, `status`, `vendor`, `category_ids`, `variants`, `images`, `created_at`, `updated_at`

**CONTACTS**: `contact_id`, `email`, `phone`, `first_name`, `last_name`, `country`, `email_status`, `sms_status`, `custom_properties`, `created_at`, `updated_at`

**CAMPAIGNS**: `id`, `name`, `subject`, `type`, `status`, `send_count`, `delivered_count`, `opened_count`, `clicked_count`, `created_at`, `updated_at`

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for Omnisend API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.