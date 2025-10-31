# Bitbucket API Connector Example

## Connector overview
This connector syncs workspaces, repositories, and projects data from Bitbucket Cloud using the Bitbucket REST API v2.0. It provides comprehensive data extraction for organizational analysis, repository management, and project tracking. The connector implements memory-efficient streaming patterns to handle large datasets and includes robust error handling with exponential backoff retry logic for production reliability.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs workspace information including names, privacy settings, and organizational details from Bitbucket API
- Basic authentication using username and app passwords with automatic retry logic (refer to `execute_api_request` function)
- Memory-efficient streaming prevents data accumulation for large datasets (refer to `get_workspaces`, `get_repositories`, and `get_projects` functions)
- Incremental synchronization using timestamp-based cursors for optimal performance (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic for rate limiting and network issues
- Support for both workspace-level repositories and project-organized repositories
- Configurable pagination with automatic page traversal to handle API limits
- Real-time progress tracking through incremental checkpointing during sync operations

## Configuration file
```json
{
  "username": "<YOUR_BITBUCKET_USERNAME>",
  "app_password": "<YOUR_BITBUCKET_APP_PASSWORD>",
  "sync_frequency_hours": "<SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<ENABLE_INCREMENTAL_SYNC>",
  "enable_repositories": "<ENABLE_REPOSITORIES>",
  "enable_projects": "<ENABLE_PROJECTS>",
  "enable_debug_logging": "<ENABLE_DEBUG_LOGGING>"
}
```

### Configuration parameters
- `username`: Your Bitbucket username for authentication
- `app_password`: Bitbucket app password (not your account password)
- `sync_frequency_hours`: How often to run full syncs (default: 4 hours)
- `initial_sync_days`: Number of days of historical data to fetch on first sync (default: 90 days)
- `max_records_per_page`: API pagination limit per request (1-100, default: 50)
- `request_timeout_seconds`: HTTP request timeout (default: 30 seconds)
- `retry_attempts`: Number of retry attempts for failed requests (default: 3)
- `enable_incremental_sync`: Enable timestamp-based incremental syncing (default: true)
- `enable_repositories`: Sync repository data (default: true)
- `enable_projects`: Sync project data (default: true)
- `enable_debug_logging`: Enable detailed debug logging (default: false)

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to your [Bitbucket account settings](https://bitbucket.org/account/settings/).
2. Navigate to "App passwords" in the left sidebar under "Access management".
3. Click "Create app password" to generate a new app password.
4. Select the required permissions: `Repositories: Read`, `Workspaces: Read`, `Projects: Read`.
5. Make a note of the generated app password (this cannot be viewed again).
6. Use your Bitbucket username and the generated app password in the configuration.

Note: The connector uses Basic HTTP authentication with username and app password. Regular account passwords are not supported for API access.

## Pagination
The connector implements page-based pagination with automatic page traversal (refer to `get_workspaces`, `get_repositories`, and `get_projects` functions). Generator-based processing prevents memory accumulation for large datasets by yielding individual records for immediate processing. Configurable page sizes allow optimization for different API rate limits and performance requirements.

## Data handling
Data is mapped from Bitbucket API's JSON format to normalized database columns with proper type conversion (refer to the `__map_workspace_data`, `__map_repository_data`, and `__map_project_data` functions). Nested objects are flattened into separate columns, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days. The connector maintains relationships between workspaces, projects, and repositories through UUID references.

## Error handling
- 429 Rate Limited: Automatic retry with exponential backoff and jitter to prevent thundering herd problems (refer to the `__handle_rate_limit` function)
- Network timeouts: Configurable timeout handling with retry attempts using exponential backoff (refer to the `__handle_request_error` function)
- Authentication errors: Clear error messages for invalid credentials or insufficient permissions
- API errors: Comprehensive error categorization with appropriate retry strategies for transient vs permanent failures
- Memory management: Generator-based processing prevents out-of-memory issues with large datasets

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| WORKSPACES | `uuid` | Bitbucket workspace information including names, privacy settings, and organizational details |
| REPOSITORIES | `uuid` | Repository metadata including names, descriptions, languages, and workspace relationships |
| PROJECTS | `uuid` | Project information including keys, names, descriptions, and workspace associations |

### WORKSPACES table columns
Sample columns include `uuid`, `name`, `slug`, `type`, `is_private`, `created_on`, `updated_on`, `website`, `location`, `has_publicly_visible_repos`, `synced_at`.

### REPOSITORIES table columns
Sample columns include `uuid`, `name`, `full_name`, `slug`, `workspace_uuid`, `is_private`, `description`, `scm`, `website`, `language`, `has_issues`, `has_wiki`, `size`, `created_on`, `updated_on`, `pushed_on`, `fork_policy`, `mainbranch_name`, `project_uuid`, `project_name`, `synced_at`.

### PROJECTS table columns
Sample columns include `uuid`, `key`, `name`, `workspace_uuid`, `description`, `is_private`, `created_on`, `updated_on`, `synced_at`.

Column types are automatically inferred by Fivetran based on the data content and structure.

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for Bitbucket API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.