# Kustomer API Connector Example

## Connector overview

This connector syncs customer experience data from the Kustomer API, including customers, companies, brands, and messages. It demonstrates how to fetch data from Kustomer APIs and upsert it into destination using memory-efficient streaming patterns. The connector supports incremental synchronization using timestamp-based cursors and handles Kustomer's rate limiting with exponential backoff retry logic.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements):
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Syncs customer data, company information, brand details, and message records from Kustomer APIs
- Bearer token authentication with automatic API key validation (refer to the `execute_api_request` function)
- Page-based pagination with automatic page traversal (refer to the `get_customers`, `get_companies`, `get_brands`, and `get_messages` functions)
- Memory-efficient streaming prevents data accumulation for large datasets using generator patterns
- Incremental synchronization using timestamp-based cursors (refer to the `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic for rate limits and network issues
- Configurable sync parameters including page size, retry attempts, and timeout settings

## Configuration file

```json
{
  "api_key": "<YOUR_KUSTOMER_API_KEY>",
  "max_records_per_page": "<MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<ENABLE_INCREMENTAL_SYNC>"
}
```

Note: Ensure that the 'configuration.json' file is not checked into version control to protect sensitive information.

Configuration parameters:

- `api_key` (required): Bearer token from Kustomer API Keys settings.
- `max_records_per_page` (optional): API page size for pagination (1-1000).
- `request_timeout_seconds` (optional): HTTP request timeout expressed on seconds.
- `retry_attempts` (optional): Number of retry attempts for failed requests.
- `enable_incremental_sync` (optional): Enable timestamp-based incremental sync.

## Requirements file

This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

1. Log in to the [Kustomer Developer Portal](https://developer.kustomer.com).
2. Navigate to **Settings** > **Security** > **API Keys** in your Kustomer instance.
3. Click **Create New API Key** to generate a new API token.
4. Select an appropriate API role with read permissions for:
   - Customers
   - Companies
   - Brands
   - Messages
5. Apply a descriptive label, such as "fivetran connector".
6. Make a note of the generated API key and use it as the `api_key` configuration parameter.

Note: The connector automatically handles Bearer token authentication with proper header formatting. API keys are never logged or exposed in plain text.

## Pagination

Page-based pagination with automatic page traversal (refer to the `get_customers`, `get_companies`, `get_brands`, and `get_messages` functions). Generator-based processing prevents memory accumulation for large datasets. Processes pages sequentially while yielding individual records for immediate processing. Supports configurable page sizes from 1-1000 records per page.

## Data handling

Customer, company, brand, and message data is mapped from Kustomer's API format to normalized database columns (refer to the `__map_customer_data`, `__map_company_data`, `__map_brand_data`, and `__map_message_data` functions). Nested objects like company domains are flattened to JSON strings, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days. The connector processes records individually using streaming patterns to prevent memory issues with large datasets.

## Error handling

- 429 Rate Limited: Automatic retry with exponential backoff and jitter, respects Kustomer's rate limit headers (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts and exponential backoff delays (refer to the `__handle_request_error` function)
- Network connectivity issues handled with progressive retry delays to prevent request storms
- Authentication errors provide clear guidance for API key configuration and permissions
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues

## Tables created

The connector creates the `CUSTOMERS`, `COMPANIES`, `BRANDS`, and `MESSAGES` tables.

Column types are automatically inferred by Fivetran.

### CUSTOMERS

| Column | Type | Description |
|--------|------|-------------|
| `id` | String | Primary key. Unique customer identifier. |
| `name` | String | Customer's full name. |
| `email` | String | Customer's email address. |
| `phone` | String | Customer's phone number. |
| `created_at` | Timestamp | ISO 8601 timestamp when the customer record was created. |
| `updated_at` | Timestamp | ISO 8601 timestamp when the customer record was last updated. |
| `company_id` | String | Foreign key reference to the company the customer belongs to. |
| `external_id` | String | External system identifier for the customer. |
| `verified` | Boolean | Indicates whether the customer's information has been verified. |
| `locked` | Boolean | Indicates whether the customer account is locked. |
| `fetch_timestamp` | Timestamp | ISO 8601 timestamp when the record was fetched from the API. |

### COMPANIES

| Column | Type | Description |
|--------|------|-------------|
| `id` | String | Primary key. Unique company identifier. |
| `name` | String | Company name. |
| `created_at` | Timestamp | ISO 8601 timestamp when the company record was created. |
| `updated_at` | Timestamp | ISO 8601 timestamp when the company record was last updated. |
| `external_id` | String | External system identifier for the company. |
| `website` | String | Company website URL. |
| `domains` | String (JSON) | JSON array of company domain names. |
| `fetch_timestamp` | Timestamp | ISO 8601 timestamp when the record was fetched from the API. |

### BRANDS

| Column | Type | Description |
|--------|------|-------------|
| `id` | String | Primary key. Unique brand identifier. |
| `name` | String | Brand name. |
| `created_at` | Timestamp | ISO 8601 timestamp when the brand record was created. |
| `updated_at` | Timestamp | ISO 8601 timestamp when the brand record was last updated. |
| `display_name` | String | Brand display name. |
| `is_default` | Boolean | Indicates whether this is the default brand. |
| `website_url` | String | Brand website URL. |
| `fetch_timestamp` | Timestamp | ISO 8601 timestamp when the record was fetched from the API. |

### MESSAGES

| Column | Type | Description |
|--------|------|-------------|
| `id` | String | Primary key. Unique message identifier. |
| `conversation_id` | String | Foreign key reference to the conversation this message belongs to. |
| `customer_id` | String | Foreign key reference to the customer who sent or received the message. |
| `channel` | String | Communication channel (e.g., "email", "chat", "sms"). |
| `direction` | String | Message direction: "inbound" or "outbound". |
| `body` | String | Message content/body text. |
| `created_at` | Timestamp | ISO 8601 timestamp when the message was created. |
| `updated_at` | Timestamp | ISO 8601 timestamp when the message was last updated. |
| `status` | String | Message status (e.g., "sent", "delivered", "read"). |
| `message_type` | String | Type of message (e.g., "text", "email", "note"). |
| `fetch_timestamp` | Timestamp | ISO 8601 timestamp when the record was fetched from the API. |


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
