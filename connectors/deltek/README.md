# Deltek ConceptShare Connector Example

## Connector overview

This connector syncs projects, project resources, and teams data from Deltek ConceptShare API into your destination. The connector fetches comprehensive project management data including project details, associated resources (files, images, documents), and team information to provide complete visibility into your ConceptShare workspace.

The connector supports incremental synchronization using timestamp-based cursors, memory-efficient streaming for large datasets, and comprehensive error handling with automatic retry logic. It implements three main data endpoints: projects (with metadata, status, and ownership), project resources (files and assets), and teams (with member information and privacy settings).

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Syncs projects, project resources, and teams data from Deltek ConceptShare API
- Bearer token authentication with secure credential handling (refer to the `execute_api_request` function)
- Offset-based pagination with automatic page traversal (refer to the `get_projects`, `get_project_resources`, and `get_teams` functions)
- Memory-efficient streaming prevents data accumulation for large datasets using generator patterns
- Incremental synchronization using timestamp-based cursors (refer to the `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic (refer to the `__handle_rate_limit` and `__handle_request_error` functions)
- Configurable sync parameters including page size, timeout settings, and retry attempts
- Three dedicated data endpoints with specialized field mapping for each data type

## Configuration file

```json
{
  "api_key": "<YOUR_DELTEK_CONCEPTSHARE_API_KEY>",
  "initial_sync_days": "<YOUR_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_ENABLE_INCREMENTAL_SYNC>"
}
```

### Configuration parameters

- `api_key` (required): Bearer token for authenticating with Deltek ConceptShare API
- `initial_sync_days` (optional): Historical date range for the initial sync expressed in days, defaults to `90`.
- `max_records_per_page` (optional): Page size for API requests, defaults to `100` (range: 1-1000)
- `request_timeout_seconds` (optional): HTTP request timeout expressed in seconds, defaults to `30`.
- `retry_attempts` (optional): Number of retry attempts for failed requests, defaults to `3`.
- `enable_incremental_sync` (optional): Enable timestamp-based incremental sync, defaults to `true`.

## Requirements file

This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

1. Log in to the [Deltek ConceptShare Developer Portal](https://api.conceptshare.com/v1).
2. Navigate to **API Settings** or **Developer** section in your account settings.
3. Generate a new API key or access token for your application.
4. Make a note of the `api_key` (bearer token) from your API settings.
5. Ensure your account has appropriate permissions to access projects, resources, and teams data.
6. Use sandbox credentials for testing, production credentials for live syncing.

Note: The connector automatically handles Bearer token authentication. Credentials are never logged or exposed in plain text. API keys should be kept secure and rotated regularly according to your organization's security policies.

## Pagination

Offset-based pagination with automatic page traversal (refer to the `get_projects`, `get_project_resources`, and `get_teams` functions). Generator-based processing prevents memory accumulation for large datasets. The connector processes pages sequentially while yielding individual records for immediate processing, automatically handling pagination until all data is retrieved.

## Data handling

Project, resource, and team data is mapped from the Deltek ConceptShare API format to normalized database columns (refer to the `__map_project_data`, `__map_project_resource_data`, and `__map_team_data` functions). Nested objects are flattened where appropriate, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days. The connector uses memory-efficient streaming patterns to handle large datasets without memory accumulation issues.

## Error handling

- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues
- Network connectivity errors are automatically retried with progressive delays
- Authentication failures provide clear feedback for credential verification

## Tables created

| Table | Primary Key | Description | Sample Columns |
|-------|-------------|-------------|-------------|
| `PROJECTS` | `id` | Project information including metadata, status, and ownership. | `id`, `name`, `description`, `status`, `created_date`, `modified_date`, `owner_id`, `owner_name`, `project_type`, `privacy_level`, `due_date`, `archived`, `synced_at` |
| `PROJECT_RESOURCES` | `id` | Files, images, and documents associated with projects. | `id`, `project_id`, `resource_type`, `resource_name`, `file_name`, `file_size`, `version`, `uploaded_date`, `uploaded_by`, `status`, `url`, `mime_type`, `synced_at` |
| `TEAMS` | `id` | Team information with member details and privacy settings. | `id`, `name`, `description`, `created_date`, `modified_date`, `owner_id`, `owner_name`, `member_count`, `privacy_level`, `synced_at` |

Note: Column types are automatically inferred by Fivetran.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
