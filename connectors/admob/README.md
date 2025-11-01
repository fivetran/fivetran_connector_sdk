# AdMob API Connector Example

## Connector overview
This connector syncs advertising data from Google AdMob API including publisher accounts, network reports, and mediation analytics. It fetches ad performance metrics such as estimated earnings, ad requests, impressions, clicks, and detailed breakdowns by app, ad unit, country, platform, and ad type. The connector supports incremental synchronization using date-based cursors and handles OAuth2 authentication with automatic token refresh.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs publisher account information and advertising performance data from AdMob APIs
- OAuth2 authentication with automatic token refresh handling (refer to `__refresh_oauth_token` function)
- Memory-efficient streaming prevents data accumulation for large report datasets (refer to `get_network_reports` function)
- Incremental synchronization using date-based cursors for efficient data updates (refer to `get_time_range` function)
- Comprehensive error handling with exponential backoff retry logic (refer to `execute_api_request` function)
- Support for multiple publisher accounts and cross-account reporting
- Configurable date ranges for historical data sync and rate limiting management

## Configuration file
```json
{
  "client_id": "<YOUR_ADMOB_OAUTH_CLIENT_ID>",
  "client_secret": "<YOUR_ADMOB_OAUTH_CLIENT_SECRET>",
  "refresh_token": "<YOUR_ADMOB_OAUTH_REFRESH_TOKEN>",
  "sync_frequency_hours": "<YOUR_ADMOB_API_SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<YOUR_ADMOB_API_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_ADMOB_API_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_ADMOB_API_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_ADMOB_API_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_ADMOB_API_ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<YOUR_ADMOB_API_ENABLE_DEBUG_LOGGING>"
}
```

### Configuration parameters
- `client_id` (required): OAuth2 client identifier from Google Cloud Console
- `client_secret` (required): OAuth2 client secret from Google Cloud Console
- `refresh_token` (required): OAuth2 refresh token for automatic access token renewal
- `sync_frequency_hours`: How often to run incremental syncs (default: 4 hours)
- `initial_sync_days`: Number of days of historical data for initial sync (default: 90 days)
- `max_records_per_page`: Maximum records per API request for memory management (default: 100)
- `request_timeout_seconds`: HTTP request timeout in seconds (default: 30)
- `retry_attempts`: Number of retry attempts for failed requests (default: 3)
- `enable_incremental_sync`: Enable date-based incremental synchronization (default: true)
- `enable_debug_logging`: Enable detailed logging for troubleshooting (default: false)

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Google Cloud Console](https://console.cloud.google.com/).
2. Create a new project or select an existing project with AdMob API access.
3. Enable the AdMob Reporting API and AdMob Management API for your project.
4. Create OAuth2 credentials (Web application type) in the Credentials section.
5. Make a note of the `client_id` and `client_secret` from your OAuth2 credentials.
6. Generate a refresh token using the OAuth2 flow with the following scopes:
   - `https://www.googleapis.com/auth/admob.readonly`
   - `https://www.googleapis.com/auth/admob.report`
7. Use the refresh token for long-term authentication without manual intervention.

Note: The connector automatically handles OAuth2 token refresh (1-hour expiration). Credentials are never logged or exposed in plain text.

## Pagination
AdMob API uses report-based data retrieval with streaming response handling (refer to `get_network_reports` function). The connector processes report data as it's received from the API without accumulating large datasets in memory. Generator-based processing prevents memory accumulation for large advertising datasets.

## Data handling
Advertising data is mapped from AdMob's API format to normalized database columns (refer to the `__map_network_report_data` function). Nested metric objects are flattened, monetary values are converted from micro-units to decimal format, and all timestamps are converted to UTC format for consistency.

Supports date-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical advertising data up to 365 days. The connector handles multiple publisher accounts automatically.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- OAuth token expiration: Automatic token refresh using refresh token (refer to the `__refresh_oauth_token` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing OAuth setup issues

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| ACCOUNTS | `publisher_id` | AdMob publisher account information and settings |
| NETWORK_REPORTS | `id` | Daily advertising performance metrics and breakdowns |

Column types are automatically inferred by Fivetran. Sample columns include `estimated_earnings`, `ad_requests`, `impressions`, `clicks`, `show_rate`, `match_rate`, `ad_unit_name`, `app_name`, `country`, `platform`, `ad_type`, `date`.

## Additional files
The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for AdMob API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.