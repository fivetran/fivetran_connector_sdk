# Xurrent API Connector Example

## Connector overview
This connector synchronizes organizations, products, and projects data from the Xurrent API into your data warehouse. It fetches organizational structures, product catalogs, and project management data using OAuth2 authentication with automatic token refresh. The connector implements memory-efficient streaming patterns to handle large datasets and supports incremental synchronization using timestamp-based cursors for optimal performance.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs organizations, products, and projects data from Xurrent API
- OAuth2 authentication with Bearer token and account ID authentication (refer to the `execute_api_request` function)
- Page-based pagination with automatic page traversal (refer to the `get_organizations`, `get_products`, and `get_projects` functions)
- Memory-efficient streaming prevents data accumulation for large datasets
- Incremental synchronization using timestamp-based cursors (refer to the `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic
- Rate limiting support with automatic retry after delays
- Configurable pagination limits and timeout settings

## Configuration file
```json
{
  "oauth_token": "<YOUR_XURRENT_OAUTH_TOKEN>",
  "account_id": "<YOUR_XURRENT_ACCOUNT_ID>",
  "sync_frequency_hours": "<YOUR_SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<YOUR_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_ENABLE_INCREMENTAL_SYNC>"
}
```

Configuration parameters:
- `oauth_token` (required): OAuth bearer token for API authentication
- `account_id` (required): Xurrent account identifier for `X-Xurrent-Account` header
- `sync_frequency_hours` (optional): How often to run sync in hours
- `initial_sync_days` (optional): Days of historical data to fetch on first sync (1-365)
- `max_records_per_page` (optional): Records per API request page (1-100, default: `25`)
- `request_timeout_seconds` (optional): HTTP request timeout in seconds (10-300, default: `30`)
- `retry_attempts` (optional): Number of retry attempts for failed requests (1-10, default: `3`)
- `enable_incremental_sync` (optional): Enable timestamp-based incremental sync (true/false)

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Xurrent Developer Portal](https://developer.xurrent.com/).
2. Register a new application to obtain OAuth credentials.
3. Generate a personal access token or create an OAuth application.
4. Make a note of the `oauth_token` from your application settings.
5. Retrieve your `account_id` from Xurrent administrators or account settings.
6. Use sandbox credentials for testing, production credentials for live syncing.

Note: The connector automatically handles OAuth2 authentication using Bearer tokens. Credentials are never logged or exposed in plain text.

## Pagination
Page-based pagination with automatic page traversal (refer to the `get_organizations`, `get_products`, and `get_projects` functions). Generator-based processing prevents memory accumulation for large datasets. Processes pages sequentially while yielding individual records for immediate processing. Supports configurable page sizes from 1 to 100 records per request.

## Data handling
Organization, product, and project data is mapped from Xurrent API format to normalized database columns (refer to the `__map_organization_data`, `__map_product_data`, and `__map_project_data` functions). Nested objects are flattened, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues
- Network connectivity errors with automatic retry logic
- Authentication failures with clear error reporting

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| ORGANIZATIONS | `id` | Organization hierarchy and management structure |
| PRODUCTS | `id` | Product catalog with categories and support information |
| PROJECTS | `id` | Project management data with status and timeline information |

Column types are automatically inferred by Fivetran. Sample columns include `name`, `status`, `category`, `created_at`, `updated_at`, `manager_id`, `service_id`, and `customer_id`.

Organizations table includes parent-child relationships, manager assignments, and business unit associations. Products table contains brand information, service relationships, and support team assignments. Projects table tracks status, completion targets, and customer relationships.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.