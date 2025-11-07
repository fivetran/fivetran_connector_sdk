# Meltwater API Connector Example

## Connector overview

This connector syncs data from the Meltwater API, focusing on three core endpoints: hooks, searches, and tags. The connector demonstrates memory-efficient streaming patterns, comprehensive error handling, and incremental synchronization capabilities. It fetches webhook configurations, saved search definitions, and tagging information from Meltwater's media monitoring platform.

The connector uses bearer token authentication and implements automatic retry logic with exponential backoff for resilient data synchronization. All data is processed using generators to prevent memory accumulation issues with large datasets.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Syncs webhook configurations, saved searches, and tag definitions from Meltwater API
- Bearer token authentication with automatic request header management (refer to the `execute_api_request` function)
- Offset-based pagination with automatic page traversal (refer to the `get_hooks_data`, `get_searches_data`, and `get_tags_data` functions)
- Memory-efficient streaming prevents data accumulation for large datasets using generator patterns
- Incremental synchronization using timestamp-based cursors (refer to the `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic (refer to the `__handle_rate_limit` and `__handle_request_error` functions)
- Configurable data type selection with individual endpoint enablement controls
- Robust pagination handling across all three endpoint types

## Configuration file

```json
{
  "api_key": "<YOUR_MELTWATER_API_KEY>",
  "sync_frequency_hours": "<SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<ENABLE_DEBUG_LOGGING>",
  "enable_hooks": "<ENABLE_HOOKS>",
  "enable_searches": "<ENABLE_SEARCHES>",
  "enable_tags": "<ENABLE_TAGS>"
}
```

Configuration parameters:

- `api_key` (required): Your Meltwater API authentication token 
- `sync_frequency_hours`: How often to run incremental syncs (default: `4`)
- `initial_sync_days`: Days of historical data to fetch on first sync (default: `90`)
- `max_records_per_page`: Maximum records per API request (1-1000, default: `100`)
- `request_timeout_seconds`: HTTP request timeout in seconds (default: `30`)
- `retry_attempts`: Number of retry attempts for failed requests (default: `3`)
- `enable_incremental_sync`: Enable timestamp-based incremental synchronization (default: `true`)
- `enable_debug_logging`: Enable detailed debug logging (default: `false`)
- `enable_hooks`: Sync webhook configurations (default: `true`)
- `enable_searches`: Sync saved search definitions (default: `true`)
- `enable_tags`: Sync tag definitions (default: `true`)

## Requirements file

This connector requires the `faker` package for comprehensive testing functionality.

```
faker
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

1. Log in to the [Meltwater Developer Portal](https://developer.meltwater.com/).
2. Navigate to your application settings or API management section.
3. Generate a new API token or retrieve your existing authentication key.
4. Make a note of the API key for use in the connector configuration.
5. Ensure your API key has appropriate permissions for accessing hooks, searches, and tags endpoints.

Note: The connector automatically handles bearer token authentication by adding the Authorization header to all API requests. API keys are never logged or exposed in plain text for security purposes.

## Pagination

Offset-based pagination with automatic page traversal (refer to the `get_hooks_data`, `get_searches_data`, and `get_tags_data` functions). Generator-based processing prevents memory accumulation for large datasets. The connector processes pages sequentially while yielding individual records for immediate processing, supporting configurable page sizes from 1 to 1000 records per request.

## Data handling

Data from Meltwater's API format is mapped to normalized database columns (refer to the `__map_hook_data`, `__map_search_data`, and `__map_tag_data` functions). Nested objects like event types and source types are serialized as JSON arrays, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days using the `initial_sync_days` parameter.

## Error handling

- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing configuration issues
- Network connectivity errors are handled with progressive retry delays
- Invalid authentication responses provide clear troubleshooting guidance

## Tables created

| Table | Primary Key | Description | Sample Columns |
|-------|-------------|-------------|-------------|
| HOOKS | `id` | Webhook configurations and event subscriptions | `id`, `name`, `url`, `status`, `created_at`, `updated_at`, `search_id`, `event_types`, `timestamp` |
| SEARCHES | `id` | Saved search definitions and query parameters | `id`, `name`, `query`, `language`, `source_types`, `created_at`, `updated_at`, `is_active`, `tags`, `timestamp` |
| TAGS | `id` | Tag definitions and categorization metadata | `id`, `name`, `description`, `color`, `created_at`, `updated_at`, `usage_count`, `category`, `timestamp` |

Column types are automatically inferred by Fivetran.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
