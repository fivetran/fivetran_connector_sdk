# Contentful CMA Connector Example

## Connector overview

This connector syncs data from Contentful's Content Management API, including spaces and organizations. The connector provides comprehensive access to your Contentful content infrastructure, enabling analytics and reporting on your content management operations. It supports synchronization of organization and space data efficiently.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements): **3.9-3.13**
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Syncs spaces and organizations from Contentful CMA API
- Personal Access Token authentication with automatic retry logic (refer to `execute_api_request` function)
- Memory-efficient streaming prevents data accumulation for large content datasets
- Comprehensive error handling with exponential backoff retry logic
- Organization and space synchronization with relationship preservation

## Configuration file

```json
{
  "access_token": "<YOUR_CONTENTFUL_CMA_ACCESS_TOKEN>",
  "environment_id": "<YOUR_CONTENTFUL_ENVIRONMENT_ID>",
  "sync_frequency_hours": "<SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<ENABLE_DEBUG_LOGGING>"
}
```

### Configuration parameters

- `access_token` (required): Contentful Content Management API Personal Access Token
- `environment_id` (optional): Contentful environment to sync from (defaults to "master")
- `sync_frequency_hours` (optional): How often to run incremental syncs in hours
- `initial_sync_days` (optional): Number of days of historical data to fetch on first sync
- `max_records_per_page` (optional): Number of records to fetch per API request (1-1000)
- `request_timeout_seconds` (optional): HTTP request timeout in seconds
- `retry_attempts` (optional): Number of retry attempts for failed requests
- `enable_incremental_sync` (optional): Enable timestamp-based incremental synchronization
- `enable_debug_logging` (optional): Enable detailed debug logging

## Requirements file

This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

1. Log in to the [Contentful web app](https://app.contentful.com/).
2. Open the space that you want to access using the space selector in the top left.
3. Click **Settings** and select **CMA tokens** from the drop-down list.
4. Click **Create personal access token**. The **Create personal access token** window is displayed.
5. Enter a custom name for your personal access token and click **Generate**. Your personal access token is created.
6. Copy your personal access token to clipboard and use it as the `access_token` in your configuration.

Note: The connector automatically handles API rate limiting (7 requests per second default). Personal Access Tokens have the same rights as the account owner and never expire unless manually revoked.

## Data handling

Data is mapped from Contentful's API format to normalized database columns (refer to the `__map_space_data` and `__map_organization_data` functions). All timestamps are converted to UTC format for consistency.

Supports timestamp-based incremental synchronization using the `last_sync_time` state parameter (refer to the `get_time_range` function). Initial sync can be configured to fetch historical data up to 365 days.

## Error handling

- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues

## Tables created

| Table | Primary Key | Description |
|-------|-------------|-------------|
| SPACES | `id` | Contentful spaces and their configuration |
| ORGANIZATIONS | `id` | Contentful organizations |

Column types are automatically inferred by Fivetran. Sample columns include `name`, `created_at`, `updated_at`, `version`, `type`, `organization_id`, `default_locale`.

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for Contentful CMA API integration and connector requirements including faker for mock testing.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
