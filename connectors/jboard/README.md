# Jboard API Connector Example

## Connector overview
This connector syncs employers, job categories, and alert subscriptions data from Jboard API to your destination warehouse. The connector fetches employer profiles with their metadata, job categories with hierarchical structure, and user alert subscriptions with search criteria. It supports incremental synchronization using timestamp-based cursors and handles API rate limiting with automatic retry logic. The connector uses memory-efficient streaming to process large datasets without accumulation issues.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs employer data, job categories, and alert subscriptions from Jboard API
- Bearer token authentication with secure API key handling (refer to `execute_api_request` function)
- Page-based pagination with automatic page traversal (refer to `get_employers`, `get_categories`, and `get_alert_subscriptions` functions)
- Memory-efficient streaming prevents data accumulation for large datasets using generator patterns
- Incremental synchronization using timestamp-based cursors (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic (refer to `__handle_rate_limit` and `__handle_request_error` functions)
- Configurable sync parameters including page size, timeout, and retry attempts
- Cognitive complexity optimization with helper function extraction for maintainability

## Configuration file
```json
{
  "api_key": "<YOUR_JBOARD_API_KEY>",
  "sync_frequency_hours": "<YOUR_JBOARD_API_SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<YOUR_JBOARD_API_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_JBOARD_API_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_JBOARD_API_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_JBOARD_API_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_JBOARD_API_ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<YOUR_JBOARD_API_ENABLE_DEBUG_LOGGING>"
}
```

### Configuration parameters
- `api_key`: Your Jboard API authentication key (required)
- `sync_frequency_hours`: How often to sync data in hours (default: 4)
- `initial_sync_days`: Days of historical data to fetch on first sync (default: 90)
- `max_records_per_page`: Maximum records per API request (default: 100, range: 1-1000)
- `request_timeout_seconds`: API request timeout in seconds (default: 30)
- `retry_attempts`: Number of retry attempts for failed requests (default: 3)
- `enable_incremental_sync`: Enable timestamp-based incremental sync (default: true)
- `enable_debug_logging`: Enable detailed debug logs (default: false)

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Jboard API Developer Portal](https://app.jboard.io/api/documentation).
2. Navigate to your account settings page to access API credentials.
3. Generate a new API key or copy your existing API key.
4. Make a note of the API key - it will be used as the Bearer token for authentication.
5. Use sandbox credentials for testing, production credentials for live syncing.

Note: The connector automatically handles Bearer token authentication (API key never expires). Credentials are never logged or exposed in plain text for security.

## Pagination
Page-based pagination with automatic page traversal (refer to `get_employers`, `get_categories`, and `get_alert_subscriptions` functions). The connector uses `page` and `per_page` parameters to fetch data in configurable chunks. Generator-based processing prevents memory accumulation for large datasets by yielding individual records. Processes pages sequentially while yielding individual records for immediate processing, with pagination metadata used to determine when all data has been fetched.

## Data handling
Employer, category, and alert subscription data is mapped from Jboard API's format to normalized database columns (refer to the `__map_employer_data`, `__map_category_data`, and `__map_alert_subscription_data` functions). Nested objects like tags arrays are serialized to JSON strings, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days using the `initial_sync_days` configuration parameter.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support and exponential backoff (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts and exponential backoff with jitter (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- HTTP error handling with appropriate retry logic for temporary failures
- Parameter validation with descriptive error messages provides clear guidance for fixing configuration issues

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| EMPLOYERS | `id` | Employer profiles with company information and metadata |
| CATEGORIES | `id` | Job categories with hierarchical structure and settings |
| ALERT_SUBSCRIPTIONS | `id` | User alert subscriptions with search criteria and preferences |

Column types are automatically inferred by Fivetran. Sample columns include:

**EMPLOYERS**: `id`, `name`, `description`, `website`, `logo_url`, `featured`, `source`, `created_at`, `updated_at`, `have_posted_jobs`, `have_a_logo`, `sync_timestamp`

**CATEGORIES**: `id`, `name`, `description`, `parent_id`, `sort_order`, `is_active`, `created_at`, `updated_at`, `sync_timestamp`

**ALERT_SUBSCRIPTIONS**: `id`, `email`, `query`, `location`, `search_radius`, `remote_work_only`, `category_id`, `job_type`, `tags`, `is_active`, `created_at`, `updated_at`, `sync_timestamp`

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for Jboard API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.