# Nextdoor API Connector Example

## Connector overview
This connector syncs city, state, and trending post data from the Nextdoor API. It fetches lists of cities and states that have popular post data, then retrieves trending posts for each location. The connector supports OAuth2 authentication and implements memory-efficient streaming to handle large datasets without accumulation issues. It includes comprehensive error handling with exponential backoff retry logic for rate limiting and network failures.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements):
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Syncs city and state lists with popular post availability from Nextdoor API
- Fetches trending posts for cities and states using memory-efficient streaming patterns
- OAuth2 Bearer token authentication with automatic token refresh handling
- Memory-efficient streaming prevents data accumulation for large datasets (refer to `get_cities_data`, `get_states_data`, `get_city_posts_data`, and `get_state_posts_data` functions)
- Configurable sync options for selective data fetching (cities, states, city posts, state posts)
- Comprehensive error handling with exponential backoff retry logic (refer to `__handle_rate_limit` and `__handle_request_error` functions)
- State-based pagination for large state post datasets (refer to `get_state_posts_data` function)
- Incremental checkpointing prevents data loss during sync interruptions

## Configuration file
```json
{
  "api_key": "<YOUR_NEXTDOOR_API_KEY>",
  "initial_sync_days": "<INITIAL_SYNC_DAYS>",
  "request_timeout_seconds": "<REQUEST_TIMEOUT_SECONDS>",
  "retry_attempts": "<RETRY_ATTEMPTS>",
  "enable_cities_sync": "<ENABLE_CITIES_SYNC>",
  "enable_states_sync": "<ENABLE_STATES_SYNC>",
  "enable_city_posts_sync": "<ENABLE_CITY_POSTS_SYNC>",
  "enable_state_posts_sync": "<ENABLE_STATE_POSTS_SYNC>",
  "max_pages_per_state": "<MAX_PAGES_PER_STATE>"
}
```

### Configuration parameters
- `api_key`: OAuth2 Bearer token for Nextdoor API authentication
- `initial_sync_days`: Historical data range for initial sync (max 365 days)
- `request_timeout_seconds`: HTTP request timeout in seconds
- `retry_attempts`: Number of retry attempts for failed requests
- `enable_cities_sync`: Enable/disable cities data synchronization
- `enable_states_sync`: Enable/disable states data synchronization
- `enable_city_posts_sync`: Enable/disable city trending posts synchronization
- `enable_state_posts_sync`: Enable/disable state trending posts synchronization
- `max_pages_per_state`: Maximum pages to fetch per state for trending posts

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
1. Log in to the [Nextdoor Developer Portal](https://developer.nextdoor.com/).
2. Register a new application to obtain OAuth2 credentials.
3. Make a note of the `client_id` and `client_secret` from your application settings.
4. Follow the OAuth2 flow to obtain a Bearer token for API access.
5. Retrieve your API key from Nextdoor administrators or through the developer portal.
6. Use sandbox credentials for testing, production credentials for live syncing.

Note: The connector uses Bearer token authentication. Tokens are never logged or exposed in plain text. Ensure your API key has the necessary permissions to access city lists, state lists, and trending post data.

## Pagination
The connector implements different pagination strategies for different endpoints:
- City and state lists: Single request with full dataset
- City posts: Individual requests per city (refer to `get_city_posts_data` function)
- State posts: Page-based pagination with configurable page limits (refer to `get_state_posts_data` function)

Generator-based processing prevents memory accumulation for large datasets. The connector processes pages sequentially while yielding individual records for immediate processing.

## Data handling
Post data is mapped from Nextdoor's API format to normalized database columns (refer to the `__map_post_data`, `__map_city_data`, and `__map_state_data` functions). Nested objects are flattened, and all timestamps are converted to UTC format for consistency.

The connector supports selective synchronization through configuration flags. You can enable or disable specific data types (cities, states, city posts, state posts) based on your requirements.

Memory-efficient streaming ensures large datasets are processed without accumulation issues. Each record is processed individually as it's yielded from the API.

## Error handling
- 429 Rate Limited: Automatic retry with Retry-After header support (refer to the `__handle_rate_limit` function)
- Timeout handling with configurable retry attempts (refer to the `__handle_request_error` function)
- Exponential backoff with jitter prevents multiple clients from making requests at the same time
- Parameter validation with descriptive error messages provides clear guidance for fixing setup issues
- Network connectivity issues are handled with automatic retry logic
- Invalid authentication handling provides clear error messages for credential issues

## Tables created
| Table | Primary Key | Description |
|-------|-------------|-------------|
| CITIES | `id` | Cities with available popular post data |
| STATES | `id` | States with available popular post data |
| TRENDING_POSTS | `id` | Trending posts from cities and states |

Column types are automatically inferred by Fivetran. Sample columns include:
- Cities: `id`, `city_id`, `has_popular_posts`, `sync_timestamp`
- States: `id`, `state_id`, `state_name`, `has_popular_posts`, `sync_timestamp`
- Trending Posts: `id`, `post_id`, `location_id`, `location_type`, `title`, `content`, `author`, `created_at`, `engagement_score`, `category`, `sync_timestamp`


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.