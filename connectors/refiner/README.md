# Connector SDK Refiner Survey Analytics Connector Example

## Connector overview
This custom Fivetran connector extracts survey response data from the [Refiner](https://refiner.io) REST API and loads it into your destination. The connector fetches NPS surveys, questions, responses, and respondent data keyed by user ID, enabling product teams to analyze survey feedback alongside user behavior data for comprehensive product analytics.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Extracts survey metadata from the `/forms` endpoint with full configuration and question details
- Extracts survey responses from the `/responses` endpoint with incremental sync support
- Creates normalized tables for surveys, questions, responses, answers, and respondents
- Implements incremental syncs based on `updated_at` timestamps with automatic state management
- Processes all paginated data automatically using cursor-based pagination for large datasets
- Implements exponential backoff for API reliability (3 retries with progressive delays)
- Flattens nested JSON structures into table columns automatically
- Checkpoint strategy ensures resumability for large datasets (every 1000 records)
- Extracts and normalizes nested arrays (questions, answers) into child tables with foreign keys
- User-level data keyed by user ID for joining with product usage data

## Configuration file
The configuration requires your Refiner API key and optionally a start date for the initial sync.

```json
{
  "api_key": "<YOUR_REFINER_API_KEY>",
  "start_date": "<OPTIONAL_START_DATE_UTC_ISO8601_FORMAT>"
}
```

### Configuration parameters
- `api_key` (required) - Your Refiner API key for Bearer token authentication.
- `start_date` (optional) - UTC datetime in ISO 8601 format with 'Z' suffix (e.g., "2025-01-01T00:00:00Z"). If not provided, sync starts from EPOCH time (1970-01-01T00:00:00Z) to capture all historical data.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
The `requirements.txt` file specifies additional Python libraries required by the connector. Following Fivetran best practices, this connector does not require additional dependencies beyond the SDK-provided packages.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
The connector uses Bearer token authentication via the `Authorization` header. To obtain your API key:

1. Log in to your Refiner account.
2. Go to **Settings** > **Integrations** > **API**.
3. Copy your API key.
4. Add the API key to your `configuration.json` file as shown above.

The API key is included in every request as `Authorization: Bearer YOUR_API_KEY`.

## Pagination
The connector handles pagination automatically using the Refiner API's page-based pagination structure. The API supports the following pagination parameters:
- `page` - Current page number (starts at 1)
- `page_length` - Number of items per page (default: 100)
- `next_page_cursor` - Optional cursor for cursor-based pagination

The connector uses page-based pagination with automatic detection of the last page:
- Each sync processes all paginated data completely using the `pagination.current_page` and `pagination.last_page` response fields.
- Pagination state is not persisted between sync runs for cleaner state management.
- Uses the `date_range_start` parameter to filter responses from the API directly for incremental syncs.

Pagination logic is implemented in:
- `fetch_surveys()` - Paginate through all surveys
- `fetch_responses()` - Paginate through responses with date filtering

## Data handling
The connector processes survey and response data with an optimized incremental sync strategy:

### Tables and relationships
- **surveys** - Survey metadata including configuration (parent table)
- **questions** - Questions extracted from survey config (child of surveys)
- **responses** - Individual survey responses (linked to surveys and respondents)
- **answers** - Answer data for each question in a response (child of responses)
- **respondents** - User/contact information keyed by user ID (parent for responses)

### Incremental sync strategy
- Initial sync uses `start_date` from configuration (if provided) or EPOCH time (1970-01-01T00:00:00Z) as fallback
- Incremental syncs use `last_response_sync` timestamp from state to fetch only new/updated responses since last successful sync
- State tracks separate timestamps for surveys and responses
- Checkpoint every 1000 records during large response syncs to enable resumability
- Final checkpoint saves the complete state only after successful sync completion

### Data transformation
- **JSON flattening** - Nested dictionaries converted to underscore-separated columns (e.g., `config.theme.color` becomes `config_theme_color`)
- **Array handling** - Arrays converted to JSON strings when stored in parent tables, or normalized to child tables
- **Child table extraction** - Questions extracted from survey config, answers extracted from response data
- **Foreign keys** - Child tables maintain relationships via parent primary keys (`survey_uuid`, `response_uuid`)
- **Type safety** - Configuration validation ensures required fields exist before processing

### Key functions
- `validate_configuration()` - Validates required API key configuration
- `make_api_request()` - Centralized API calling with retry logic and error handling
- `flatten_dict()` - Recursive JSON structure flattening for table columns
- `fetch_surveys()` - Main survey sync with pagination and question extraction
- `fetch_questions()` - Extract questions from survey configuration
- `fetch_responses()` - Incremental response sync with date-based filtering
- `fetch_answers()` - Extract answers from response data
- `fetch_respondent()` - Extract or update respondent information

The connector maintains a clean state with `last_survey_sync` and `last_response_sync` timestamps, automatically advancing after each successful sync to ensure reliable incremental syncs without data duplication or gaps.

## Error handling
The connector implements comprehensive error handling with multiple layers of protection:

### Configuration validation (`validate_configuration()`)
- Validates the required `api_key` field exists and is not empty
- Provides clear error messages for configuration issues

### API request resilience (`make_api_request()`)
- Implements retry logic with exponential backoff (3 attempts with progressive delays: 2s, 4s, 8s)
- Handles HTTP errors (4xx, 5xx), timeouts, and network issues gracefully
- Fail-fast for permanent errors (401, 403, 404) without retrying
- Detailed logging for debugging API connectivity problems

### Data processing safeguards
- Graceful handling of missing or malformed API response structures
- Safe dictionary access patterns with `.get()` to prevent KeyError exceptions
- Skips records missing required identifiers (uuid) with warnings
- Proper exception propagation with descriptive RuntimeError messages

### Checkpoint recovery
- Checkpoints every 1000 records during large syncs enable recovery from interruptions
- State tracking allows sync to resume from the last successful checkpoint
- Final checkpoint only saved after a complete successful sync

All exceptions are caught at the top level in the `update()` function and re-raised as `RuntimeError` with descriptive messages, making troubleshooting easier for users and Fivetran support.

## Tables created

The connector creates the following tables in your destination:

| Table name | Primary key | Description |
|------------|-------------|-------------|
| `surveys` | `uuid` | Survey metadata and configuration with flattened JSON properties |
| `questions` | `survey_uuid`, `question_id` | Questions extracted from survey configuration (child table) |
| `responses` | `uuid` | Individual survey responses with foreign keys to surveys and respondents |
| `answers` | `response_uuid`, `question_id` | Answer data for each question (child table) |
| `respondents` | `user_id` | User/contact information for survey respondents |

### Schema details

**surveys** table:
- Contains flattened survey configuration and metadata
- Nested config properties are flattened to underscore-separated columns
- Primary key: `uuid`

**questions** table:
- Extracted from survey `config.form_elements`
- Foreign key: `survey_uuid` (references surveys.uuid)
- Composite primary key: `survey_uuid`, `question_id`
- Columns: `question_text`, `question_type`, `required`, `options` (JSON)

**responses** table:
- Individual survey submissions
- Foreign keys: `survey_uuid` (references surveys.uuid), `user_id` (references respondents.user_id)
- Primary key: `uuid`
- Key timestamp fields: `completed_at`, `last_shown_at`, `last_data_reception_at`, `created_at`, `updated_at`
- Score field for NPS/rating responses

**answers** table:
- Answer values for each question in a response
- Foreign key: `response_uuid` (references responses.uuid)
- Composite primary key: `response_uuid`, `question_id`
- `answer_value` stored as string or JSON depending on data type

**respondents** table:
- User/contact information keyed by user ID for joins with product usage data
- Primary key: `user_id`
- Columns: `email`, `name`, `first_seen_at`, `last_seen_at`, `attributes` (JSON)

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.