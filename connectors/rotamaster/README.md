# Rotamaster API Connector Example

## Connector overview
This connector syncs employee, team, and role data from Rotamaster API into your data warehouse. The connector fetches data from three core endpoints: `/api/People` for employee information including personal details and employment status, `/api/Team` for organizational team structure, and `/api/Role` for role definitions and hierarchies. It supports incremental synchronization using timestamp-based cursors and includes comprehensive filtering options for employee status (active, archived, suspended, deleted).

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs employee data, team information, and role definitions from Rotamaster API
- Bearer token authentication with comprehensive error handling (refer to `execute_api_request` function)
- Record-based pagination with configurable limits (refer to `get_people_data` function)
- Memory-efficient streaming prevents data accumulation for large employee datasets
- Incremental synchronization using timestamp-based cursors (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic
- Configurable employee filtering by status (active, archived, suspended, deleted)
- Rate limiting handling with automatic retry delays (refer to `__handle_rate_limit` function)

## Configuration file
```json
{
  "api_key": "<YOUR_ROTAMASTER_API_KEY>",
  "sync_frequency_hours": "<SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<RETRY_ATTEMPTS>",
  "enable_people_sync": "<ENABLE_PEOPLE_SYNC>",
  "enable_teams_sync": "<ENABLE_TEAMS_SYNC>",
  "enable_roles_sync": "<ENABLE_ROLES_SYNC>",
  "include_active_people": "<INCLUDE_ACTIVE_PEOPLE>",
  "include_archived_people": "<INCLUDE_ARCHIVED_PEOPLE>",
  "include_suspended_people": "<INCLUDE_SUSPENDED_PEOPLE>",
  "include_deleted_people": "<INCLUDE_DELETED_PEOPLE>",
  "enable_debug_logging": "<ENABLE_DEBUG_LOGGING>"
}
```

### Configuration parameters
- `api_key` (required): Bearer token for Rotamaster API authentication
- `sync_frequency_hours` (optional): Hours between sync attempts, defaults to 4
- `initial_sync_days` (optional): Days of historical data for initial sync, defaults to 90
- `max_records_per_page` (optional): Records per API request (1-1000), defaults to 100
- `request_timeout_seconds` (optional): API request timeout, defaults to 30
- `retry_attempts` (optional): Failed request retry attempts, defaults to 3
- `enable_people_sync` (optional): Enable employee data sync, defaults to true
- `enable_teams_sync` (optional): Enable team data sync, defaults to true
- `enable_roles_sync` (optional): Enable role data sync, defaults to true
- `include_active_people` (optional): Include active employees, defaults to true
- `include_archived_people` (optional): Include archived employees, defaults to false
- `include_suspended_people` (optional): Include suspended employees, defaults to false
- `include_deleted_people` (optional): Include deleted employees, defaults to false

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Rotamaster Developer Portal](https://data-api.rotamasterweb.co.uk).
2. Generate an API key from your account settings or contact Rotamaster administrators.
3. Make a note of the Bearer token provided for API authentication.
4. Ensure your API key has permissions for People, Team, and Role endpoints.
5. Use test credentials for development, production credentials for live syncing.

Note: The connector handles authentication errors automatically with clear error messages. API keys are never logged or exposed in plain text.

## Pagination
Record-based pagination using `numberOfRecords` parameter with automatic handling (refer to `get_people_data` function). Generator-based processing prevents memory accumulation for large employee datasets. Processes API responses sequentially while yielding individual records for immediate processing.

## Data handling
Employee data is mapped from Rotamaster's API format to normalized database columns (refer to the `__map_people_data` function). Nested objects are flattened, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues
- Authentication errors with clear guidance for API key configuration

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| PEOPLE | `id` | Employee personal information, employment details, and status |
| TEAMS | `id` | Team and organizational group information |
| ROLES | `id` | Role definitions and organizational hierarchy |

Column types are automatically inferred by Fivetran. Sample columns include `first_name`, `last_name`, `email`, `employment_start_date`, `is_active`, `team_name`, `role_name`, `nominal_code`.

### PEOPLE table columns
- `id`: Unique employee identifier
- `external_id`: External employee reference
- `first_name`: Employee first name
- `last_name`: Employee last name
- `email`: Employee email address
- `date_of_birth`: Employee date of birth
- `gender`: Employee gender
- `mobile_number`: Employee mobile phone
- `landline_number`: Employee landline phone
- `employment_start_date`: Employment start date
- `employment_end_date`: Employment end date (if applicable)
- `is_active`: Active employment status
- `is_archived`: Archived status
- `is_suspended`: Suspended status
- `is_deleted`: Deleted status
- `sync_timestamp`: Record synchronization timestamp

### TEAMS table columns
- `id`: Unique team identifier
- `external_id`: External team reference
- `name`: Team name
- `sync_timestamp`: Record synchronization timestamp

### ROLES table columns
- `id`: Unique role identifier
- `name`: Role name
- `nominal_code`: Role nominal code (optional)
- `sync_timestamp`: Record synchronization timestamp

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for Rotamaster API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.