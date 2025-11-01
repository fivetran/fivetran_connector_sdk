# Planday HR API Connector Example

## Connector overview
This connector syncs employee, department, and skills data from the Planday HR API to your destination warehouse. It demonstrates memory-efficient streaming patterns for large datasets, implements comprehensive error handling with exponential backoff retry logic, and supports both initial and incremental synchronization using timestamp-based cursors. The connector fetches data from three core endpoints: `/employees` for staff information, `/departments` for organizational structure, and `/skills` for competency tracking.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs employee personal information, job details, and compensation data from Planday HR API
- Bearer token authentication with automatic retry logic for expired tokens (refer to `execute_api_request` function)
- Offset-based pagination with automatic page traversal (refer to `get_employees`, `get_departments`, and `get_skills` functions)
- Memory-efficient streaming prevents data accumulation for large employee datasets
- Incremental synchronization using timestamp-based cursors (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic (refer to `__handle_rate_limit` and `__handle_request_error` functions)
- Data quality validation and field mapping for consistent database schema (refer to `__map_employee_data`, `__map_department_data`, and `__map_skill_data` functions)

## Configuration file
```json
{
  "api_key": "<YOUR_PLANDAY_HR_API_KEY>",
  "sync_frequency_hours": "<YOUR_PLANDAY_HR_API_SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<YOUR_PLANDAY_HR_API_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_PLANDAY_HR_API_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_PLANDAY_HR_API_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_PLANDAY_HR_API_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_PLANDAY_HR_API_ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<YOUR_PLANDAY_HR_API_ENABLE_DEBUG_LOGGING>"
}
```

### Configuration parameters
- `api_key` (required): Your Planday HR API authentication key
- `sync_frequency_hours` (optional): How often to run incremental syncs, defaults to 4 hours
- `initial_sync_days` (optional): Number of days to sync during initial sync, defaults to 90 days
- `max_records_per_page` (optional): Maximum records per API request, defaults to 100 (range: 1-1000)
- `request_timeout_seconds` (optional): HTTP request timeout in seconds, defaults to 30
- `retry_attempts` (optional): Number of retry attempts for failed requests, defaults to 3
- `enable_incremental_sync` (optional): Enable timestamp-based incremental sync, defaults to true
- `enable_debug_logging` (optional): Enable detailed debug logging, defaults to false

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Planday HR Developer Portal](https://openapi.planday.com/api/hr).
2. Navigate to your application settings or API credentials section.
3. Generate a new API key or retrieve your existing authentication token.
4. Make a note of the `api_key` from your application settings.
5. Ensure your API key has appropriate permissions for reading employee, department, and skills data.

Note: The connector uses Bearer token authentication with automatic retry logic for failed requests. API keys are never logged or exposed in plain text for security.

## Pagination
Offset-based pagination with automatic page traversal (refer to `get_employees`, `get_departments`, and `get_skills` functions). Generator-based processing prevents memory accumulation for large employee datasets. Processes pages sequentially while yielding individual records for immediate processing. Each endpoint supports configurable page sizes through the `max_records_per_page` setting.

## Data handling
Employee, department, and skills data is mapped from Planday HR API format to normalized database columns (refer to the `__map_employee_data`, `__map_department_data`, and `__map_skill_data` functions). Nested objects are flattened, and all timestamps are converted to UTC format for consistency across different time zones.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days. Memory-efficient streaming processes individual records without accumulating large datasets in memory.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues
- Network connectivity issues are handled with progressive retry delays and comprehensive logging

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| EMPLOYEES | `id` | Employee personal information, job details, and compensation data |
| DEPARTMENTS | `id` | Organizational departments with management structure and budget information |
| SKILLS | `id` | Employee skills and competencies with proficiency levels and categories |

Column types are automatically inferred by Fivetran. Sample columns include `first_name`, `last_name`, `email`, `department_id`, `hire_date`, `position`, `salary`, `hourly_rate`, `manager_id`, `name`, `description`, `budget`, `cost_center`, `category`, `level`, `certification_required`.

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for Planday HR API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.