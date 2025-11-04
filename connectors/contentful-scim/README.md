# Contentful SCIM Connector Example

## Connector overview

This connector syncs user and group data from Contentful's SCIM API including user profiles, group memberships, and organizational structure. The connector fetches employee information, team assignments, and access management data from Contentful organizations. It supports both full and incremental synchronization using SCIM 2.0 filtering capabilities to efficiently manage large user datasets while maintaining data consistency across Contentful organizational changes.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Syncs user data and group information from Contentful SCIM API
- Bearer token authentication with automatic error handling (refer to the `execute_api_request` function)
- SCIM 2.0 pagination with automatic page traversal (refer to the `get_users` and `get_groups` functions)
- Memory-efficient streaming prevents data accumulation for large user datasets
- Incremental synchronization using SCIM lastModified filtering (refer to the `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic
- Support for user profiles and group metadata

## Configuration file

```json
{
  "bearer_token": "<YOUR_CONTENTFUL_BEARER_TOKEN>",
  "org_id": "<YOUR_CONTENTFUL_ORG_ID>",
  "sync_frequency_hours": "<SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<ENABLE_DEBUG_LOGGING>"
}
```

Configuration parameters:

- `bearer_token`: Your Contentful access token with organization admin rights.
- `org_id`: Contentful organization ID for SCIM API access.
- `sync_frequency_hours`: How often to run sync operations (4-24 hours recommended)
- `initial_sync_days`: Days of historical data to fetch on first sync (up to 365)
- `max_records_per_page`: Records per API request (1-1000, default `100`)
- `request_timeout_seconds`: HTTP request timeout (10-60 seconds)
- `retry_attempts`: Number of retry attempts for failed requests (1-5)
- `enable_incremental_sync`: Use timestamp-based incremental sync for efficiency
- `enable_debug_logging`: Enable detailed logging for troubleshooting

## Requirements file

This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

1. Log in to the [Contentful Developer Portal](https://www.contentful.com/developers/).
2. Navigate to organization settings and access management.
3. Generate a personal access token with organization admin or owner permissions.
4. Make a note of your organization ID from the Contentful organization settings.
5. Retrieve your organization ID from Contentful administrators.
6. Use sandbox organization credentials for testing, production credentials for live syncing.

Note: The connector automatically handles authentication token validation and provides clear error messages for invalid credentials. Tokens are never logged or exposed in plain text.

## Pagination

SCIM 2.0 pagination with automatic page traversal (refer to the `get_users` and `get_groups` functions). Generator-based processing prevents memory accumulation for large user datasets. Processes pages sequentially while yielding individual records for immediate processing using startIndex and count parameters.

## Data handling

User and group data is mapped from Contentful's SCIM API format to normalized database columns (refer to the `__map_user_data` and `__map_group_data` functions). Nested SCIM objects are flattened, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `meta.lastModified` SCIM filter parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days with configurable pagination for memory efficiency.

## Error handling

- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Authentication validation with descriptive error messages provides clear guidance for fixing credential issues
- SCIM API error handling for invalid filters and malformed requests

## Tables created

| Table | Primary Key | Description |
|-------|-------------|-------------|
| USERS | `id` | User profile information and account status |
| GROUPS | `id` | Group definitions and metadata |

Column types are automatically inferred by Fivetran. Sample columns include `user_name`, `email`, `first_name`, `last_name`, `display_name`, `active`, `created`, `last_modified`, `member_count`.

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for Contentful SCIM API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
