# Contentful User Management API Connector Example

## Connector overview
This connector syncs user management data from Contentful's User Management API, including users, organization memberships, team memberships, and space memberships. The connector uses memory-efficient streaming patterns and supports incremental synchronization for large datasets.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs user data, organization memberships, team memberships, and space memberships from Contentful User Management API
- Bearer token authentication with automatic error handling (refer to the `execute_api_request` function)
- Skip/limit pagination with automatic page traversal (refer to the `get_users` function)
- Memory-efficient streaming prevents data accumulation for large user datasets
- Incremental synchronization using timestamp-based cursors (refer to the `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic
- Regional API support for EU and US endpoints

## Configuration file
```json
{
  "access_token": "<YOUR_CONTENTFUL_ACCESS_TOKEN>",
  "organization_id": "<YOUR_ORGANIZATION_ID>",
  "use_eu_region": "<YOUR_USE_EU_REGION>",
  "initial_sync_days": "<YOUR_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_RETRY_ATTEMPTS>",
  "enable_organization_memberships": "<YOUR_ENABLE_ORGANIZATION_MEMBERSHIPS>",
  "enable_team_memberships": "<YOUR_ENABLE_TEAM_MEMBERSHIPS>",
  "enable_space_memberships": "<YOUR_ENABLE_SPACE_MEMBERSHIPS>"
}
```

- `access_token` (required): Content Management API token from Contentful
- `organization_id` (required): The Contentful Organization ID to scope UMA API requests
- `use_eu_region` (required): Set to "true" for EU region API endpoint
- `initial_sync_days` (optional): Days of historical data to fetch on first sync (default: `90`)
- `max_records_per_page` (optional): Records per API request (default: `100`, max: `100`)
- `request_timeout_seconds` (optional): HTTP request timeout (default: `30`)
- `retry_attempts` (optional): Number of retry attempts for failed requests (default: `3`)
- `enable_organization_memberships` (optional): Enable organization memberships (default: `true`)
- `enable_team_memberships` (optional): Enable team memberships (default: `true`)
- `enable_space_memberships` (optional): Enable space memberships (default: `true`)

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Contentful web app](https://app.contentful.com).
2. Navigate to **Settings > API keys**.
3. Create a new Content Management API token or use an existing one.
4. Make a note of the access token from your API key settings.
5. Ensure your account has Premium/Enterprise access (required for User Management API).

Note: The connector automatically handles token authentication with bearer token headers. Credentials are never logged or exposed in plain text.

## Pagination
Skip/limit pagination with automatic page traversal (refer to the `get_users`, `get_organization_memberships`, `get_team_memberships`, and `get_space_memberships` functions). Generator-based processing prevents memory accumulation for large user datasets. Processes pages sequentially while yielding individual records for immediate processing.

## Data handling
User data is mapped from Contentful's API format to normalized database columns (refer to the `__map_user_data` function). Nested objects are flattened, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues

## Tables created

The connector creates the `USERS`, `ORGANIZATION_MEMBERSHIPS`, `TEAM_MEMBERSHIPS`, and `SPACE_MEMBERSHIPS`.

Column types are automatically inferred by Fivetran based on the data values.

### USERS

Description: User personal information and account status.

Primary key: `id`

Columns:
- `id` (string) - Unique user identifier.
- `email` (string) - User email address
- `first_name` (string) - User's first name
- `last_name` (string) - User's last name
- `avatar_url` (string) - URL to user's avatar image
- `activated` (boolean) - Whether the user account is activated
- `sign_in_count` (integer) - Number of times the user has signed in
- `confirmed` (boolean) - Whether the user account is confirmed
- `created_at` (string) - ISO 8601 timestamp of when the user was created
- `updated_at` (string) - ISO 8601 timestamp of when the user was last updated
- `last_activity` (string) - ISO 8601 timestamp of the user's last activity
- `timestamp` (string) - ISO 8601 timestamp of when the record was synced

### ORGANIZATION_MEMBERSHIPS

Description: Organization membership relationships and admin status.

Primary key: `id`

Columns:
- `id` (string) - Unique membership identifier.
- `type` (string) - Type of membership
- `admin` (boolean) - Whether the user has admin privileges in the organization
- `organization_membership_id` (string) - Organization membership identifier
- `created_at` (string) - ISO 8601 timestamp of when the membership was created
- `updated_at` (string) - ISO 8601 timestamp of when the membership was last updated
- `email` (string) - User email address associated with the membership
- `first_name` (string) - User's first name
- `last_name` (string) - User's last name
- `timestamp` (string) - ISO 8601 timestamp of when the record was synced

### TEAM_MEMBERSHIPS

Description: Team membership relationships and permissions.

Primary key: `id`

Columns:
- `id` (string) - Unique membership identifier
- `type` (string) - Type of membership
- `admin` (boolean) - Whether the user has admin privileges in the team
- `team_membership_id` (string) - Team membership identifier
- `organization_membership_id` (string) - Associated organization membership identifier
- `created_at` (string) - ISO 8601 timestamp of when the membership was created
- `updated_at` (string) - ISO 8601 timestamp of when the membership was last updated
- `timestamp` (string) - ISO 8601 timestamp of when the record was synced

### SPACE_MEMBERSHIPS

Description: Space membership relationships and access levels.

Primary key: `id`

Columns:
- `id` (string) - Unique membership identifier
- `type` (string) - Type of membership
- `admin` (boolean) - Whether the user has admin privileges in the space
- `space_id` (string) - Contentful space identifier
- `user_id` (string) - User identifier associated with the membership
- `created_at` (string) - ISO 8601 timestamp of when the membership was created
- `updated_at` (string) - ISO 8601 timestamp of when the membership was last updated
- `timestamp` (string) - ISO 8601 timestamp of when the record was synced

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
