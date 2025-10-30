# Ticketmaster Connector Example

## Connector overview

This Fivetran connector syncs data from the Ticketmaster Discovery API v2, providing comprehensive access to events, venues, attractions, and classifications data. The connector is designed to handle large datasets efficiently using memory-optimized streaming patterns and implements robust error handling for production environments.

The connector fetches data from four main Ticketmaster endpoints:
- Events – Concert, sports, and entertainment events with detailed venue, pricing, and scheduling information
- Venues – Performance venues with location data, capacity, and facility details
- Attractions – Artists, performers, teams, and entertainment acts with classification and external link information
- Classifications – Event categorization data including segments, genres, and sub-genres

Key features include intelligent rate limiting (5 requests/second, 5000/day), incremental sync capabilities for events, comprehensive error handling with exponential backoff, and memory-efficient processing using generator patterns to prevent out-of-memory issues with large datasets.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## Features

The connector provides the following key capabilities:

- Memory-Efficient Data Processing – Uses generator-based streaming in `get_events`, `get_venues`, `get_attractions`, and `get_classifications` functions to prevent memory accumulation
- Intelligent Rate Limiting – Automatic handling of Ticketmaster's 5 requests/second and 5000 requests/day limits with exponential backoff
- Incremental Sync – Time-based incremental synchronization for events using `get_time_range` function with configurable lookback periods
- Comprehensive Error Handling – Robust retry logic in `execute_api_request` function with specific handling for authentication, rate limiting, and network errors
- Configurable Data Selection – Enable/disable specific data types (events, venues, attractions, classifications) through configuration flags
- Production-Ready Logging – Structured logging with appropriate levels for monitoring and debugging
- Low Cognitive Complexity – All functions designed with complexity <15 using helper function extraction patterns

Refer to the `update` function for the main sync orchestration and individual `get_*` functions for data-specific processing logic.

## Configuration file

Create a `configuration.json` file with the following structure:

```json
{
  "api_key": "<YOUR_TICKETMASTER_API_KEY>",
  "sync_frequency_hours": "<YOUR_SYNC_FREQUENCY_HOURS>",
  "initial_sync_days": "<YOUR_INITIAL_SYNC_DAYS>",
  "max_records_per_page": "<YOUR_MAX_RECORDS_PER_PAGE>",
  "request_timeout_seconds": "<YOUR_REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<YOUR_RETRY_ATTEMPTS>",
  "enable_incremental_sync": "<YOUR_ENABLE_INCREMENTAL_SYNC>",
  "enable_debug_logging": "<YOUR_ENABLE_DEBUG_LOGGING>",
  "enable_events": "<YOUR_ENABLE_EVENTS>",
  "enable_venues": "<YOUR_ENABLE_VENUES>",
  "enable_attractions": "<YOUR_ENABLE_ATTRACTIONS>",
  "enable_classifications": "<YOUR_ENABLE_CLASSIFICATIONS>"
}
```

### Configuration parameters

- `api_key` (required) – Your Ticketmaster Discovery API v2 key. Obtain from [Ticketmaster Developer Portal](https://developer.ticketmaster.com/)
- `sync_frequency_hours` – How often to run full syncs (1-168 hours, default: 4)
- `initial_sync_days` – Historical data range for first sync (1-365 days, default: 90)
- `max_records_per_page` – API pagination size (1-200 records, default: 100)
- `request_timeout_seconds` – HTTP request timeout (1-300 seconds, default: 30)
- `retry_attempts` – Number of retry attempts for failed requests (1-10, default: 3)
- `enable_incremental_sync` – Enable time-based incremental sync (true/false, default: true)
- `enable_debug_logging` – Enable detailed debug logs (true/false, default: false)
- `enable_events` – Sync events data (true/false, default: true)
- `enable_venues` – Sync venues data (true/false, default: true)
- `enable_attractions` – Sync attractions data (true/false, default: true)
- `enable_classifications` – Sync classifications data (true/false, default: true)

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

To authenticate with the Ticketmaster Discovery API v2:

1. Create Developer Account – Register at [Ticketmaster Developer Portal](https://developer.ticketmaster.com/).
2. Generate API Key – Create a new application to obtain your API key.
3. Configure Rate Limits – Note your daily quota (default: 5000 requests/day) and rate limits (5 requests/second).
4. Test Authentication – Verify your API key works with a simple request to `/discovery/v2/events`.
5. Set Configuration – Add your API key to the `configuration.json` file.

The connector automatically includes the API key in all requests and handles authentication errors with clear error messages. API key validation occurs during the configuration validation step.

## Pagination

The connector implements comprehensive pagination handling for all Ticketmaster Discovery API v2 endpoints using page-based pagination. The strategy is implemented in the `get_events`, `get_venues`, `get_attractions`, and `get_classifications` functions.

Pagination Parameters:
- `page` – Page number starting from 0
- `size` – Number of records per page (1-200, configurable via `max_records_per_page`)

Implementation Strategy:
- Automatic pagination through all available pages until no more data is returned
- Page size optimization to minimize API calls while respecting rate limits
- Early termination when fewer records than requested are returned (indicating last page)
- Built-in rate limiting delay (200ms between requests) to respect API constraints

The connector processes each page immediately as it's received, using generator patterns to prevent memory accumulation with large datasets. Refer to the individual `get_*` functions for endpoint-specific pagination logic.

## Data handling

The connector implements several key data handling strategies:

Memory Efficiency – All data fetching functions (`get_events`, `get_venues`, `get_attractions`, `get_classifications`) use Python generators to process records individually, preventing memory accumulation issues with large datasets. Records are yielded one at a time and immediately upserted to the destination.

Field Mapping – Complex API responses are flattened into database-friendly formats using dedicated mapping functions (`__map_event_data`, `__map_venue_data`, `__map_attraction_data`, `__map_classification_data`). Nested objects are either flattened into separate columns or serialized as JSON strings for complex structures like images and external links.

Incremental Sync – Events support time-based incremental synchronization using the `get_time_range` function. The connector tracks `last_sync_time` in the state and only fetches events modified since the last successful sync. Other data types (venues, attractions, classifications) are synced in full due to their relatively stable nature.

Data Types – The connector automatically handles data type conversion and validation. Timestamps are normalized to UTC format, numeric fields are validated for range, and JSON serialization is used for complex nested structures.

## Error handling

The connector implements comprehensive error handling with specific strategies for different failure scenarios:

API Authentication Errors (401) – Immediate failure with clear messaging in `execute_api_request` function. Invalid API keys are detected early to prevent unnecessary retries.

Rate Limiting (429) – Intelligent handling using `__handle_rate_limit` function with respect for `Retry-After` headers and exponential backoff with jitter via `__calculate_wait_time` function.

Network Issues – Configurable retry logic in `__handle_request_error` function with exponential backoff for transient network failures, connection timeouts, and DNS resolution issues.

Server Errors (5xx) – Automatic retry with backoff for temporary server issues, with logging for monitoring purposes.

Data Validation Errors – Field-level validation during mapping with graceful degradation for missing or malformed data.

All error handling uses helper functions to maintain low cognitive complexity and enable comprehensive testing. Refer to the `execute_api_request` function for the main error handling orchestration.

## Tables created

The connector creates four main tables with the following structure. Column types are automatically inferred by Fivetran.

### EVENTS

Primary key: `id`

Sample columns: `id`, `name`, `type`, `url`, `start_date_time`, `start_local_date`, `timezone`, `status_code`, `venue_id`, `venue_name`, `venue_city`, `venue_state`, `venue_country`, `attraction_id`, `attraction_name`, `min_price`, `max_price`, `currency`, `public_sales_start`, `public_sales_end`, `info`, `please_note`, `images` (JSON), `classifications` (JSON), `synced_at`

### VENUES

Primary key: `id`

Sample columns: `id`, `name`, `type`, `url`, `timezone`, `city_name`, `state_name`, `state_code`, `country_name`, `country_code`, `address_line1`, `address_line2`, `postal_code`, `latitude`, `longitude`, `twitter`, `images` (JSON), `synced_at`

### ATTRACTIONS

Primary key: `id`

Sample columns: `id`, `name`, `type`, `url`, `locale`, `external_links` (JSON), `images` (JSON), `classifications` (JSON), `synced_at`

### CLASSIFICATIONS

Primary key: `id`

Sample columns: `id`, `name`, `segment_id`, `segment_name`, `genre_id`, `genre_name`, `sub_genre_id`, `sub_genre_name`, `synced_at`

## Additional files

The connector includes several additional files to support functionality, testing, and deployment:

- `requirements.txt` – Python dependency specification for Ticketmaster Discovery API v2 integration and connector requirements.

- `configuration.json` – Configuration template for API credentials and connector parameters (should be excluded from version control).


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.