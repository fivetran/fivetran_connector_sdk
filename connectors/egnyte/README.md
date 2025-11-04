# Egnyte API Connector Example

## Connector overview
This connector syncs user data, group information, notes, and bookmarks from the Egnyte cloud storage platform. The connector fetches data from four primary endpoints: `/users` for user account information, `/groups` for team and permission groups, `/notes` for file and folder annotations, and `/bookmarks` for saved file shortcuts. Data is synchronized using OAuth2 authentication with automatic token refresh and supports both initial full syncs and incremental updates.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements):
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs user accounts, groups, notes, and bookmarks from Egnyte API
- OAuth2 authentication with automatic token refresh (refer to `execute_api_request` function)
- Pagination-based data retrieval with automatic page traversal (refer to `get_users`, `get_groups`, `get_notes`, and `get_bookmarks` functions)
- Memory-efficient streaming prevents data accumulation for large datasets
- Incremental synchronization using timestamp-based cursors (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic (refer to `__handle_rate_limit` and `__handle_request_error` functions)
- Configurable sync parameters for batch sizes and timeouts

## Configuration file
```json
{
  "api_token": "<YOUR_EGNYTE_API_TOKEN>",
  "domain": "<YOUR_EGNYTE_DOMAIN>",
  "initial_sync_days": "<YOUR_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<RECORDS_PER_PAGE_1_TO_1000_DEFAULT_100>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_IN_SECONDS_DEFAULT_30>",
  "retry_attempts": "<NUMBER_OF_RETRY_ATTEMPTS_DEFAULT_3>",
  "enable_users": "<TRUE_OR_FALSE_DEFAULT_TRUE>",
  "enable_groups": "<TRUE_OR_FALSE_DEFAULT_TRUE>",
  "enable_notes": "<TRUE_OR_FALSE_DEFAULT_TRUE>",
  "enable_bookmarks": "<TRUE_OR_FALSE_DEFAULT_TRUE>"
}
```

### Configuration parameters
- `api_token` (required): OAuth2 access token for Egnyte API authentication
- `domain` (required): Your Egnyte domain name (e.g., "companyname" for companyname.egnyte.com)
- `initial_sync_days` (optional): Days of historical data to sync initially (default: 90)
- `max_records_per_page` (optional): Records per API request page (1-1000, default: 100)
- `request_timeout_seconds` (optional): HTTP request timeout in seconds (default: 30)
- `retry_attempts` (optional): Number of retry attempts for failed requests (default: 3)
- `enable_users` (optional): Enable user data synchronization (default: true)
- `enable_groups` (optional): Enable group data synchronization (default: true)
- `enable_notes` (optional): Enable notes data synchronization (default: true)
- `enable_bookmarks` (optional): Enable bookmarks data synchronization (default: true)

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Egnyte Developer Portal](https://developers.egnyte.com/).
2. Register a new application to obtain OAuth2 credentials.
3. Make a note of the `client_id` and `client_secret` from your application settings.
4. Generate an OAuth2 access token using your client credentials and user authorization.
5. Retrieve your Egnyte domain from Egnyte administrators.
6. Use sandbox credentials for testing, production credentials for live syncing.

Note: The connector uses Bearer token authentication with automatic retry handling. Tokens should have appropriate scopes for reading user data, groups, notes, and bookmarks.

## Pagination
Pagination strategies vary by endpoint with automatic page traversal. Users and groups use SCIM-style pagination with `startIndex` and `count` parameters (refer to `get_users` and `get_groups` functions). Notes and bookmarks use offset-based pagination with `offset` and `count` parameters (refer to `get_notes` and `get_bookmarks` functions). Generator-based processing prevents memory accumulation for large datasets while processing pages sequentially and yielding individual records for immediate processing.

## Data handling
User, group, note, and bookmark data is mapped from Egnyte's API format to normalized database columns (refer to the `__map_user_data`, `__map_group_data`, `__map_note_data`, and `__map_bookmark_data` functions). Nested objects are flattened, and all timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days through the `initial_sync_days` parameter.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues
- OAuth token validation and refresh handling ensures uninterrupted data access

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| USERS | `id` | User account information, including roles and authentication details |
| GROUPS | `id` | Team and permission groups with member counts |
| NOTES | `id` | File and folder annotations with author and timestamp information |
| BOOKMARKS | `id` | Saved file and folder shortcuts with creator details |

Column types are automatically inferred by Fivetran. Sample columns include `username`, `email`, `first_name`, `last_name`, `role`, `active`, `display_name`, `member_count`, `path`, `note_text`, `author`, `creator`, `created_date`, and `modified_date`.


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.