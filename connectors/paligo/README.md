# Paligo API Connector Example

## Connector overview
This connector syncs data from Paligo API including users, taxonomies, and assignments. The connector demonstrates memory-efficient streaming patterns for large datasets, comprehensive error handling with exponential backoff retry logic, and token-based pagination with automatic page traversal. It supports both initial and incremental synchronization using timestamp-based cursors to optimize data transfer and reduce API usage.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs user profiles, taxonomy hierarchies, and assignment data from Paligo API
- Basic Authentication with automatic credential encoding (refer to `execute_api_request` function)
- Page-based pagination with automatic page traversal (refer to `get_users_data`, `get_taxonomies_data`, and `get_assignments_data` functions)
- Memory-efficient streaming prevents data accumulation for large datasets using generator patterns
- Incremental synchronization using timestamp-based cursors (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic (refer to `__handle_request_error` and `__handle_rate_limit` functions)
- Configurable request timeouts and retry attempts for robust API interaction
- Real-time checkpointing for resumable syncs and progress tracking

## Configuration file
```json
{
  "username": "<YOUR_PALIGO_USERNAME>",
  "api_key": "<YOUR_PALIGO_API_KEY>",
  "base_url": "https://api.paligo.net/v1",
  "sync_frequency_hours": "4",
  "initial_sync_days": "90",
  "max_records_per_page": "100",
  "request_timeout_seconds": "30",
  "retry_attempts": "3",
  "enable_incremental_sync": "true",
  "enable_debug_logging": "false"
}
```

### Configuration parameters
- `username` (required): Your Paligo username for Basic authentication
- `api_key` (required): Your Paligo API key for Basic authentication
- `base_url` (optional): API base URL, defaults to https://api.paligo.net/v1
- `sync_frequency_hours` (optional): Hours between syncs, defaults to 4
- `initial_sync_days` (optional): Days of historical data for initial sync, defaults to 90
- `max_records_per_page` (optional): Records per API page request, defaults to 100
- `request_timeout_seconds` (optional): HTTP request timeout, defaults to 30
- `retry_attempts` (optional): Number of retry attempts for failed requests, defaults to 3
- `enable_incremental_sync` (optional): Enable timestamp-based incremental sync, defaults to true
- `enable_debug_logging` (optional): Enable detailed debug logging, defaults to false

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Paligo Developer Portal](https://api.paligo.net/en/index-en.html).
2. Navigate to your account settings to locate your API credentials.
3. Make a note of your `username` and `api_key` from the API section.
4. Retrieve any additional access permissions from Paligo administrators if needed.
5. Use test credentials for development, production credentials for live syncing.

Note: The connector automatically handles Basic Authentication encoding. Credentials are never logged or exposed in plain text for security compliance.

## Pagination
Page-based pagination with automatic page traversal (refer to `get_users_data`, `get_taxonomies_data`, and `get_assignments_data` functions). Generator-based processing prevents memory accumulation for large datasets. Processes pages sequentially while yielding individual records for immediate processing to maintain memory efficiency.

## Data handling
Data is mapped from Paligo API format to normalized database columns (refer to the `__map_user_data`, `__map_taxonomy_data`, and `__map_assignment_data` functions). Nested objects are flattened where appropriate, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days using the `initial_sync_days` parameter.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues
- Network connectivity issues handled with automatic retry logic and proper error propagation

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| USERS | `id` | User profiles and account information |
| TAXONOMIES | `id` | Taxonomy hierarchies and categorization data |
| ASSIGNMENTS | `id` | Assignment records and task management data |

Column types are automatically inferred by Fivetran. Sample columns include:
- **USERS**: `id`, `name`, `email`, `role`, `status`, `created_at`, `updated_at`, `last_login`, `sync_timestamp`
- **TAXONOMIES**: `id`, `title`, `color`, `parent_id`, `level`, `path`, `created_at`, `updated_at`, `sync_timestamp`
- **ASSIGNMENTS**: `id`, `title`, `description`, `assignee_id`, `assigner_id`, `status`, `priority`, `due_date`, `created_at`, `updated_at`, `completed_at`, `sync_timestamp`

## Additional files
- `test_connector.py` - Comprehensive unit tests using faker-generated mock data for realistic testing scenarios
- `debug_connector.py` - Debug script with API simulation and comprehensive scenario testing including rate limiting and error conditions
- `faker_mock/mock_data_generator.py` - Realistic mock data generator for testing with proper hierarchical relationships and data consistency

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.