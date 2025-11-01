# Humaans Connector Example

## Connector overview
This connector syncs companies, documents, and job roles data from the Humaans HR platform API. It demonstrates how to implement memory-efficient streaming patterns for HR data synchronization using the Fivetran Connector SDK. The connector fetches organizational structure data, employee document records, and job role definitions with comprehensive field mapping and incremental sync capabilities.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs company information, employee documents, and job role data from Humaans API
- Bearer token authentication with automatic retry logic (refer to `execute_api_request` function)
- Offset-based pagination with automatic page traversal (refer to `get_companies`, `get_documents`, and `get_job_roles` functions)
- Memory-efficient streaming prevents data accumulation for large HR datasets
- Incremental synchronization using timestamp-based cursors (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic

## Configuration file
```json
{
  "api_token": "<YOUR_HUMAANS_API_TOKEN>",
  "sync_frequency_hours": "<YOUR_HUMAANS_API_SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<YOUR_HUMAANS_API_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_HUMAANS_API_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_HUMAANS_API_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_HUMAANS_API_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_HUMAANS_API_ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<YOUR_HUMAANS_API_ENABLE_DEBUG_LOGGING>"
}
```

### Configuration parameters
- `api_token` - Bearer token for Humaans API authentication
- `sync_frequency_hours` - How often to run sync (default: 4 hours)
- `initial_sync_days` - Historical data range for initial sync (default: 90 days)
- `max_records_per_page` - Page size for API requests (default: 100, max: 250)
- `request_timeout_seconds` - HTTP request timeout (default: 30 seconds)
- `retry_attempts` - Number of retry attempts for failed requests (default: 3)
- `enable_incremental_sync` - Enable timestamp-based incremental sync (default: true)
- `enable_debug_logging` - Enable detailed debug logs (default: false)

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Humaans Developer Portal](https://app.humaans.io/api).
2. Navigate to your account settings and generate a new API token.
3. Make a note of the Bearer token from your API settings.
4. Ensure the token has appropriate permissions for reading companies, documents, and job roles.
5. Use the token in the `api_token` configuration parameter.

Note: The connector automatically handles Bearer token authentication with each API request. Credentials are never logged or exposed in plain text.

## Pagination
Offset-based pagination with automatic page traversal (refer to `get_companies`, `get_documents`, and `get_job_roles` functions). Generator-based processing prevents memory accumulation for large HR datasets. Processes pages sequentially using `$skip` and `$limit` parameters while yielding individual records for immediate processing.

## Data handling
HR data is mapped from Humaans's API format to normalized database columns (refer to the `__map_company_data`, `__map_document_data`, and `__map_job_role_data` functions). Nested objects are flattened, complex fields like arrays are JSON-serialized, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| COMPANIES | `id` | Company information including name, address, and contact details |
| DOCUMENTS | `id` | Employee documents with metadata, file information, and expiry dates |
| JOB_ROLES | `id` | Job role definitions with salary ranges, requirements, and department information |

Column types are automatically inferred by Fivetran. Sample columns include `name`, `description`, `created_at`, `updated_at`, `address_city`, `file_size`, `mime_type`, `salary_range_min`, `employment_type`, `is_active`.

## Additional files
The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for Humaans API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.