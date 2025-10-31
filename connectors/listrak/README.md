# Listrak API Connector Example

## Connector overview

This connector syncs email marketing data from Listrak's API to your data warehouse. It fetches contacts, campaigns, and events data using memory-efficient streaming patterns with incremental synchronization support. The connector handles Bearer token authentication, implements exponential backoff retry logic for rate limiting, and processes data through paginated API endpoints to prevent memory accumulation issues with large datasets.

The connector provides comprehensive email marketing analytics by syncing subscriber information, campaign performance metrics, and detailed event tracking for opens, clicks, bounces, and other email interactions.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Syncs contacts, campaigns, and events data from Listrak API
- Bearer token authentication with automatic error handling (refer to `execute_api_request` function)
- Page-based pagination with automatic page traversal (refer to `get_contacts`, `get_campaigns`, and `get_events` functions)
- Memory-efficient streaming prevents data accumulation for large datasets
- Incremental synchronization using timestamp-based cursors (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic
- Configurable batch processing with incremental checkpointing

## Configuration file

```json
{
  "api_key": "<YOUR_LISTRAK_API_KEY>",
  "sync_frequency_hours": "4",
  "initial_sync_days": "<INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<ENABLE_DEBUG_LOGGING>"
}
```

**Parameter descriptions:**

- `api_key` (required): Your Listrak API authentication key
- `base_url` (optional): Listrak API base URL, defaults to https://api.listrak.com/email/v1
- `sync_frequency_hours` (optional): How often to run syncs, defaults to 4 hours
- `initial_sync_days` (optional): Historical data range for initial sync, defaults to 90 days
- `max_records_per_page` (optional): Number of records per API request, defaults to 100 (range: 1-1000)
- `request_timeout_seconds` (optional): API request timeout, defaults to 30 seconds
- `retry_attempts` (optional): Number of retry attempts for failed requests, defaults to 3
- `enable_incremental_sync` (optional): Enable timestamp-based incremental sync, defaults to true
- `enable_debug_logging` (optional): Enable detailed debug logging, defaults to false

## Requirements file

This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

1. Log in to the [Listrak Developer Portal](https://api.listrak.com/email).
2. Navigate to API Settings or Developer Console to generate API credentials.
3. Create a new API key or retrieve your existing API key.
4. Make a note of the `api_key` from your application settings.
5. Ensure your API key has permissions for contacts, campaigns, and events endpoints.

Note: The connector automatically handles Bearer token authentication with the provided API key. API keys are never logged or exposed in plain text.

## Pagination

Page-based pagination with automatic page traversal (refer to `get_contacts`, `get_campaigns`, and `get_events` functions). Generator-based processing prevents memory accumulation for large datasets. Processes pages sequentially while yielding individual records for immediate processing.

## Data handling

Email marketing data is mapped from Listrak's API format to normalized database columns (refer to the `__map_contact_data`, `__map_campaign_data`, and `__map_event_data` functions). Nested objects are flattened for metadata fields, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using table-specific `last_sync_time` state parameters (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days.

## Error handling

- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues

## Tables created

| Table | Primary Key | Description |
|-------|-------------|-------------|
| CONTACTS | `contact_id` | Subscriber contact information and status |
| CAMPAIGNS | `campaign_id` | Email campaign metadata and performance metrics |
| EVENTS | `event_id` | Email interaction events including opens, clicks, and bounces |

Column types are automatically inferred by Fivetran. Sample columns include `email_address`, `first_name`, `last_name`, `subscription_status`, `campaign_name`, `subject_line`, `event_type`, `event_timestamp`, `user_agent`.

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for Listrak API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.