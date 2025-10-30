# PayScore Connector Example

This connector syncs data from PayScore's income verification API to your destination using the Fivetran Connector SDK. PayScore provides automated income verification services for financial institutions, enabling streamlined income verification processes through API integration.

## Connector overview

The PayScore connector extracts data from PayScore's API and loads it into your configured destination warehouse. It supports incremental synchronization, handles API rate limiting, and provides comprehensive error handling for reliable data integration.

The connector retrieves data for:
- Screening groups: Organizational units for managing verification requests

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

The PayScore connector provides the following capabilities:

- **Incremental synchronization**: Uses timestamp-based incremental sync to efficiently retrieve only new or updated records since the last sync
- **Memory-efficient processing**: Implements streaming data processing using generators to handle large datasets without memory issues (refer to `get_screening_groups`)
- **Comprehensive error handling**: Robust retry logic for API failures, rate limiting, and network issues (refer to `execute_api_request` function)
- **Flexible configuration**: Configurable sync parameters, page sizes, and feature toggles
- **Production-ready architecture**: Low cognitive complexity design with helper functions for maintainability (refer to validation functions with `__` prefix)

## Configuration file

Create a `configuration.json` file with the following parameters:

```json
{
  "api_key": "<YOUR_PAYSCORE_API_KEY>",
  "environment": "<YOUR_ENVIRONMENT_NAME (STAGING_OR_PRODUCTION)>",
  "sync_frequency_hours": "<YOUR_SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<YOUR_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<YOUR_ENABLE_DEBUG_LOGGING>",
  "enable_screening_groups": "<YOUR_ENABLE_SCREENING_GROUPS>"
}
```

### Configuration parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `api_key` | String | Yes | Your PayScore API key for authentication |
| `environment` | String | Yes | API environment: "staging" or "production" |
| `sync_frequency_hours` | String | No | Hours between syncs (1-24, default: 4) |
| `initial_sync_days` | String | No | Days of historical data for first sync (1-365, default: 90) |
| `max_records_per_page` | String | No | Records per API request (1-1000, default: 100) |
| `request_timeout_seconds` | String | No | API request timeout (5-300, default: 30) |
| `retry_attempts` | String | No | Max retry attempts for failed requests (1-10, default: 3) |
| `enable_incremental_sync` | String | No | Enable incremental sync (true/false, default: true) |
| `enable_debug_logging` | String | No | Enable detailed logging (true/false, default: false) |
| `enable_screening_groups` | String | No | Sync screening groups (true/false, default: true) |

## Requirements file

This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The fivetran_connector_sdk:latest and requests:latest packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your requirements.txt.

## Authentication

PayScore uses Bearer token authentication. To set up authentication:

1. Log in to your PayScore dashboard
2. Navigate to API settings or developer section
3. Generate an API key with appropriate permissions for income verification data
4. Copy the API key to your `configuration.json` file
5. Select the appropriate environment (staging for testing, production for live data)

The connector will include the API key in the Authorization header for all requests: `Authorization: Bearer YOUR_API_KEY`

## Pagination

PayScore API uses page-based pagination with the following strategy (refer to `get_screening_groups`):

- **Page parameters**: Uses `page` (starting from 1) and `per_page` (configurable via `max_records_per_page`)
- **Pagination detection**: Continues fetching until no more data is returned or page count exceeds total pages
- **Memory efficiency**: Processes one page at a time using generators to prevent memory accumulation
- **Automatic traversal**: The connector automatically handles pagination without manual intervention

Example pagination flow:
1. Start with page 1
2. Fetch data with configured page size
3. Process records individually using generators
4. Continue to next page if more data exists
5. Stop when empty response or last page reached

## Data handling

The connector implements several data handling optimizations:

### Memory efficiency
- **Generator-based processing**: All data fetching functions use Python generators to yield individual records (refer to data mapping functions like `__map_screening_group_data`)
- **Streaming upserts**: Records are immediately upserted to destination without intermediate storage
- **No data accumulation**: Prevents out-of-memory issues with large datasets

### Field mapping
- **Nested object handling**: Complex nested objects are serialized to JSON for storage
- **Type preservation**: Maintains appropriate data types for numeric and timestamp fields
- **Metadata inclusion**: Includes Fivetran sync timestamps for tracking

### Incremental sync
- **Timestamp-based**: Uses `updated_at` fields for incremental synchronization
- **State management**: Maintains last sync timestamp in connector state
- **Time range filtering**: API requests include `updated_since` parameter for efficiency

## Error handling

The connector implements comprehensive error handling with modular design (refer to `execute_api_request` and helper functions):

### Retry logic
- **Exponential backoff**: Automatic retry with increasing delays for transient failures
- **Configurable attempts**: Maximum retry attempts configurable via `retry_attempts` parameter
- **Jitter implementation**: Adds randomness to prevent thundering herd problems

### Rate limiting
- **HTTP 429 detection**: Automatically detects rate limit responses
- **Retry-After headers**: Respects API-provided retry delays
- **Graceful backoff**: Waits appropriate time before retrying (refer to `__handle_rate_limit` function)

### Error categories
- **Authentication errors (401)**: Invalid API key, immediate failure with clear message
- **Permission errors (403)**: Insufficient API permissions
- **Network errors**: Connection timeouts, DNS failures with retry logic
- **Server errors (5xx)**: Temporary server issues with exponential backoff

### Logging
- Uses Fivetran SDK logging for consistent error reporting
- Includes context information (endpoints, attempt numbers, wait times)
- Escalates to severe logging for unrecoverable errors

## Tables created

The connector creates the following tables in your destination. Column types are automatically inferred by Fivetran:

### `screening_group`
Contains organizational units for managing verification requests.

| Column | Description |
|--------|-------------|
| `id` | Unique screening group identifier (Primary Key) |
| `name` | Screening group name |
| `description` | Group description |
| `status` | Group status (active, inactive, archived) |
| `created_at` | Group creation timestamp |
| `updated_at` | Last modification timestamp |
| `organization_id` | Associated organization identifier |
| `settings` | Group configuration settings (JSON) |
| `member_count` | Number of members in the group |
| `_fivetran_synced` | Fivetran sync timestamp |

 

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

* `requirements.txt` – Python dependency specification for PayScore API integration and connector requirements.

* `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
