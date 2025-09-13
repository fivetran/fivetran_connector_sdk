# Iterate NPS Survey Connector Example

## Connector overview
This connector extracts NPS survey data from [Iterate](https://iteratehq.com/) REST API and loads it into your Fivetran destination. The connector fetches NPS surveys and their individual responses, providing complete survey analytics data for downstream analysis.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Extracts survey metadata from `/surveys` endpoint
- Extracts individual survey responses from `/surveys/{id}/responses` endpoint
- **Incremental sync support**: Uses `start_date` parameter with strict UTC format validation
- **Automatic pagination handling**: Processes all paginated data using links object within single sync
- **Retry logic**: Implements exponential backoff for API reliability (3 retries with progressive delays)
- **Data transformation**: Flattens nested JSON structures into table columns automatically
- **State management**: Simple checkpointing only at successful sync completion
- **UTC datetime enforcement**: Strict format validation eliminates timezone ambiguity

## Configuration file
The configuration requires your Iterate API access token and optionally a start date for initial sync.

```
{
  "api_token": "<YOUR_ITERATE_API_TOKEN>",
  "start_date": "<YOUR_OPTIONAL_START_DATE>"
}
```

**Configuration Parameters:**
- `api_token` (required): Your Iterate API access token from your settings page
- `start_date` (optional): UTC datetime in ISO 8601 format with 'Z' suffix (e.g., "2023-01-01T00:00:00Z"). If not provided, sync starts from EPOCH time (1970-01-01T00:00:00Z) to capture all historical data

**Sync Behavior:**
- **First Sync**: Uses `start_date` if provided, otherwise starts from EPOCH time (captures all historical data)
- **Subsequent Syncs**: Always incremental from the last successful sync timestamp stored in state
- **State Management**: Contains only `last_survey_sync` timestamp for clean, reliable state tracking
- **Checkpointing**: Only occurs at successful completion to prevent data gaps from partial syncs
- **UTC Consistency**: All timestamps use consistent UTC format with 'Z' suffix throughout

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
The requirements.txt file specifies additional Python libraries required by the connector. Following Fivetran best practices, this connector doesn't require additional dependencies.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
The connector uses API Key authentication via the `x-api-key` header. To obtain your API access token:

1. Log into your Iterate account.
2. Navigate to your account settings page.
3. Find and copy your API access token.
4. Add the token to your `configuration.json` file as shown above.

## Pagination
The connector handles pagination automatically using the Iterate API's `links` object structure. When the API returns a `links.next` URL, the connector continues fetching additional pages until all data is retrieved within a single sync operation.

**Pagination Strategy:**
- **Within-sync pagination**: Each sync processes all paginated data completely using `fetch_survey_responses()`
- **No cross-sync state**: Pagination state is not persisted between sync runs for cleaner state management
- **Incremental filtering**: Uses the `start_date` Unix timestamp parameter to filter responses from API directly
- **Complete processing**: Each sync fetches all relevant data from start_date to current time

The `fetch_survey_responses()` function handles both initial requests and paginated follow-ups using the appropriate API call method.

## Data handling
The connector processes data in two main steps with optimized incremental sync:

- **Survey Data**: Fetches all surveys using `/surveys` endpoint and flattens nested JSON structures into columns
- **Response Data**: For each survey, fetches individual responses using `/surveys/{id}/responses` endpoint with date-based filtering and automatic pagination

**Enhanced Sync Strategy:**
- **Initial Sync**: Uses `start_date` from configuration (if provided) or EPOCH time (1970-01-01T00:00:00Z) as a fallback
- **Incremental Syncs**: Uses `last_survey_sync` timestamp from state to fetch only new responses since last successful sync
- **UTC Consistency**: All datetime operations use UTC timezone with 'Z' suffix format (`YYYY-MM-DDTHH:MM:SSZ`)
- **Single Checkpoint**: State is saved only after complete successful sync to prevent data gaps from partial failures

**Data Transformation:**
- **JSON Flattening**: Nested dictionaries become underscore-separated columns (e.g., `author.id` → `author_id`)
- **List Handling**: Arrays are converted to JSON strings for storage
- **Foreign Keys**: Each response includes `survey_id` field to maintain survey relationship
- **Type Safety**: Configuration validation ensures required fields exist before processing

**Key Functions:**
- `parse_iso_datetime_to_unix()`: Handles strict UTC datetime parsing with validation
- `make_iterate_api_call()`: Centralized API calling with common headers and error handling
- `flatten_dict()`: Recursive JSON structure flattening for table columns
- `validate_configuration()`: Comprehensive config validation with datetime format checking

The connector maintains clean state with only `last_survey_sync` timestamp, automatically advancing after each successful sync to ensure reliable incremental processing without data duplication or gaps.

## Error handling
The connector implements comprehensive error handling with multiple layers of protection:

**Configuration Validation (`validate_configuration()`):**
- Validates required `api_token` field exists and is not empty
- Enforces strict UTC datetime format for optional `start_date` (`YYYY-MM-DDTHH:MM:SSZ`)
- Provides clear error messages for configuration issues with format examples

**API Request Resilience (`make_api_request()`):**
- Implements retry logic with exponential backoff (3 attempts with progressive delays)
- Handles HTTP errors, timeouts, and network issues gracefully
- Detailed logging for debugging API connectivity problems

**Centralized API Logic (`make_iterate_api_call()`):**
- Consistent header management and authentication for all API calls
- Automatic API version parameter inclusion
- Unified error handling across all endpoints

**Data Processing Safeguards:**
- Graceful handling of missing or malformed API response structures
- Safe dictionary access patterns to prevent KeyError exceptions
- Proper exception propagation with descriptive RuntimeError messages

**Datetime Handling:**
- Strict UTC format parsing with clear validation messages
- Fallback to EPOCH time for invalid datetime configurations
- Consistent timezone handling throughout the connector

All exceptions are caught at the top level and re-raised as `RuntimeError` with descriptive messages, making troubleshooting easier for users and Fivetran support.

## Tables created
The connector creates two main tables with flattened column structures:

### SURVEY
Contains survey metadata and configuration with all nested JSON properties flattened into individual columns using underscore separation.
- **Primary key**: `id`
- **Structure**: All survey properties from Iterate API flattened (e.g., `author.id` becomes `author_id`)

### RESPONSE
Contains individual survey responses with relationship to parent survey and flattened response data.
- **Primary key**: `id`
- **Foreign key**: `survey_id` (links to `SURVEY` table)
- **Structure**: All response properties flattened, including question answers and user metadata

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.