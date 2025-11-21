# Countly Connector Example

## Connector overview
This connector syncs analytics data from Countly including apps, groups, and user profiles. It fetches app information, group data, and detailed user profiles from the Countly Analytics API. The connector supports incremental synchronization using timestamp-based cursors and implements memory-efficient streaming to handle large datasets without accumulation issues.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs app information, groups, and user profiles from the Countly Analytics API
- API key authentication with automatic request retry handling (refer to the `execute_api_request` function)
- Offset-based pagination with automatic page traversal (refer to the `get_apps_data`, `get_groups_data`, and `get_users_data` functions)
- Memory-efficient streaming prevents data accumulation for large analytics datasets
- Incremental synchronization using timestamp-based cursors (refer to the `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic for rate limiting and network issues
- Support for multiple data types including apps, groups, and user profiles with configurable sync options

## Configuration file
```json
{
  "api_key": "<YOUR_COUNTLY_API_KEY>",
  "app_id": "<YOUR_COUNTLY_APP_ID>",
  "max_records_per_page": "<MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<RETRY_ATTEMPTS>",
  "enable_apps_sync": "<ENABLE_APPS_SYNC>",
  "enable_groups_sync": "<ENABLE_GROUPS_SYNC>",
  "enable_users_sync": "<ENABLE_USERS_SYNC>"  
}
```

Note: Ensure that the 'configuration.json' file is not checked into version control to protect sensitive information.

### Configuration parameters
- `api_key` (required): Your Countly API key for authentication
- `app_id` (required): Countly application identifier to fetch data for specific app
- `max_records_per_page` (optional): Records per API request page (default: `100`, max: `1000`)
- `request_timeout_seconds` (optional): HTTP request timeout (default: `30`)
- `retry_attempts` (optional): Number of retry attempts for failed requests (default: `3`)
- `enable_apps_sync` (optional): Include app data in sync (default: `true`)
- `enable_groups_sync` (optional): Include group data in sync (default: `true`)
- `enable_users_sync` (optional): Include user profile data in sync (default: `true`)

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Countly Developer Portal](https://api.count.ly/reference/rest-api-reference).
2. Navigate to your application settings to obtain your API credentials.
3. Make a note of the `api_key` from your application's API section.
4. Retrieve your `app_id` from Countly administrators or application settings.
5. Use sandbox credentials for testing, production credentials for live syncing.

Note: The connector automatically handles API key authentication through query parameters. Credentials are never logged or exposed in plain text.

## Pagination
Offset-based pagination with automatic page traversal (refer to the `get_apps_data`, `get_groups_data`, and `get_users_data` functions). Generator-based processing prevents memory accumulation for large analytics datasets. Processes pages sequentially while yielding individual records for immediate processing.

## Data handling
Analytics data is mapped from Countly's API format to normalized database columns (refer to the `__map_app_data`, `__map_group_data`, and `__map_user_data` functions). Nested objects like metadata are serialized as JSON, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days.

## Error handling
- Automatic retry with exponential backoff and jitter (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues

## Tables created

The connector creates the `APPS`, `GROUPS`, and `USERS` tables. 

Note: Column types are automatically inferred by Fivetran based on the data types returned from the API.

### APPS

Primary key: `app_id`

Table description: App information including name, key, timezone, icon, and metadata

Columns:
- `app_id` (string): Unique identifier for the application
- `name` (string): Application name
- `key` (string): Application key/identifier
- `timezone` (string): Application timezone setting
- `created_at` (timestamp): Timestamp when the app was created
- `updated_at` (timestamp): Timestamp when the app was last updated
- `icon` (string): URL or path to the application icon
- `metadata` (json): Additional metadata as JSON object
- `processed_at` (timestamp): Timestamp when the record was processed by the connector

### GROUPS

Primary key: `group_id`

Table description: Group information including name, description, user count, and metadata

Columns:
- `group_id` (string): Unique identifier for the group
- `name` (string): Group name
- `description` (string): Group description
- `user_count` (integer): Number of users in the group
- `created_at` (timestamp): Timestamp when the group was created
- `updated_at` (timestamp): Timestamp when the group was last updated
- `metadata` (json): Additional metadata as JSON object
- `processed_at` (timestamp): Timestamp when the record was processed by the connector

### USERS

Primary key: `user_id`

Table description: User profile data including demographics, custom attributes, and usage statistics

Columns:
- `user_id` (string): Unique identifier for the user
- `app_id` (string): Application identifier the user belongs to
- `device_id` (string): Device identifier associated with the user
- `username` (string): User's display name
- `email` (string): User's email address
- `organization` (string): User's organization
- `phone` (string): User's phone number
- `picture` (string): URL or path to the user's profile picture
- `gender` (string): User's gender
- `byear` (integer): User's birth year
- `custom_data` (json): Custom user attributes as JSON object
- `first_seen` (timestamp): Timestamp when the user was first seen
- `last_seen` (timestamp): Timestamp when the user was last seen
- `session_count` (integer): Total number of sessions for the user
- `total_session_duration` (integer): Total session duration in seconds
- `processed_at` (timestamp): Timestamp when the record was processed by the connector

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
