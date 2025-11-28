# Listrak API Connector Example

## Connector overview

This connector syncs email marketing data from Listrak's API to your data warehouse. It fetches contacts, campaigns, and events data using memory-efficient streaming patterns with incremental synchronization support. The connector handles Bearer token authentication, implements exponential backoff retry logic for rate limiting, and processes data through paginated API endpoints to prevent memory accumulation issues with large datasets.

The connector provides comprehensive email marketing analytics by syncing subscriber information, campaign performance metrics, and detailed event tracking for opens, clicks, bounces, and other email interactions.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements):
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
  "initial_sync_days": "<INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_ENABLE_INCREMENTAL_SYNC>"
}
```

**Parameter descriptions:**

- `api_key` (required): Your Listrak API authentication key
- `initial_sync_days` (optional): Historical data range for initial sync, defaults to 90 days
- `max_records_per_page` (optional): Number of records per API request, defaults to 100 (range: 1-1000)
- `request_timeout_seconds` (optional): API request timeout, defaults to 30 seconds
- `retry_attempts` (optional): Number of retry attempts for failed requests, defaults to 3
- `enable_incremental_sync` (optional): Enable timestamp-based incremental sync, defaults to true

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

Column types are automatically inferred by Fivetran.

### CONTACTS

**Primary Key:** `contact_id`

**Description:** Subscriber contact information and status

**Columns:**
- `contact_id` - Unique identifier for the contact
- `email_address` - Contact email address
- `first_name` - Contact first name
- `last_name` - Contact last name
- `subscription_status` - Subscription status (active, unsubscribed, bounced, etc.)
- `created_date` - Date when contact was created
- `updated_date` - Date when contact was last updated
- `phone_number` - Contact phone number
- `city` - Contact city
- `state` - Contact state
- `zip_code` - Contact zip code
- `sync_timestamp` - Timestamp when record was synced

### CAMPAIGNS

**Primary Key:** `campaign_id`

**Description:** Email campaign metadata and performance metrics

**Columns:**
- `campaign_id` - Unique identifier for the campaign
- `campaign_name` - Campaign name
- `subject_line` - Email subject line
- `send_date` - Date when campaign was sent
- `campaign_type` - Type of campaign
- `status` - Campaign status
- `recipients_count` - Number of recipients
- `opens_count` - Number of email opens
- `clicks_count` - Number of email clicks
- `bounces_count` - Number of bounces
- `created_date` - Date when campaign was created
- `updated_date` - Date when campaign was last updated
- `sync_timestamp` - Timestamp when record was synced

### EVENTS

**Primary Key:** `event_id`

**Description:** Email interaction events including opens, clicks, bounces, and other email interactions

**Columns:**
- `event_id` - Unique identifier for the event
- `contact_id` - Reference to contact (foreign key to CONTACTS table)
- `campaign_id` - Reference to campaign (foreign key to CAMPAIGNS table)
- `event_type` - Type of event (open, click, bounce, unsubscribe, spam_complaint, etc.)
- `event_timestamp` - Timestamp when event occurred
- `email_address` - Email address associated with the event
- `user_agent` - User agent string from the event
- `ip_address` - IP address from the event
- `url` - URL associated with the event (for click events)
- `metadata` - Additional event metadata as JSON string
- `sync_timestamp` - Timestamp when record was synced

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.