# SAM.gov Opportunities Connector Example

## Connector overview
This connector fetches government contracting opportunities from the SAM.gov (System for Award Management) API. It replicates opportunity data including solicitations, awards, contact information, and related metadata. The connector supports incremental synchronization and handles large datasets through pagination. Data is organized into multiple tables with proper foreign key relationships to maintain referential integrity while providing a normalized structure for analysis.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Configurable date range sync of government contracting opportunities from SAM.gov
- Pagination support for large datasets (up to 1000 records per API call)
- Upsert-based sync captures updates to existing opportunities within the configured date window
- State management and checkpointing for resumable syncs
- Normalized data structure with breakout tables for arrays
- Flattened nested objects for easier analysis
- Comprehensive error handling with retry logic
- Support for all SAM.gov opportunity types (solicitations, awards, notices, etc.)

## Configuration file
The configuration requires your SAM.gov API key and date range for opportunity posting dates. The API key can be obtained from your SAM.gov account.

```json
{
  "api_key": "<YOUR_SAM_GOV_API_KEY>",
  "posted_from": "<MM/DD/YYYY_START_DATE>",
  "posted_to": "<MM/DD/YYYY_END_DATE_WITHIN_ONE_YEAR>",
  "sync_mode": "initial",
  "incremental_window_days": "30"
}
```

### Configuration parameters
- `api_key`: Your SAM.gov public API key (required)
- `posted_from`: Start date for initial sync in MM/dd/yyyy format (required for first sync)
- `posted_to`: End date for initial sync in MM/dd/yyyy format (required for first sync)
- `sync_mode`: Sync mode - "initial" for historical backfill or "incremental" for ongoing sync (optional, defaults to "initial")
- `incremental_window_days`: Number of days to overlap in incremental syncs to capture updates (optional, defaults to "30")

### Important date range limitation
- The date range between `posted_from` and `posted_to` cannot exceed 1 year (365 days)
- This is a SAM.gov API limitation, not a connector limitation
- The connector will validate this requirement and throw an error if exceeded

### Sync strategy
The connector supports hybrid sliding window incremental sync:

**Initial Sync** (First Run):
- Uses `posted_from` and `posted_to` dates from configuration for historical backfill
- After completion, automatically switches to incremental mode

**Incremental Sync** (Subsequent Runs):
- Automatically calculates date window using `last_posted_to` from previous sync minus `incremental_window_days` overlap
- Advances forward to current date, capturing new opportunities and updates
- If date range exceeds 1 year, automatically chunks into 365-day windows

### Overlap window strategy
- Default 30-day overlap ensures recent opportunity updates are captured
- Configurable via `incremental_window_days` parameter
- Smaller overlap (7 days) = more API-efficient but shorter update window
- Larger overlap (90 days) = captures more updates but uses more API calls

### Example progression
```
Initial Sync: [01/01/2024 - 12/31/2024] → State saves: last_posted_to = 12/31/2024
Sync 2: [12/01/2024 - 10/30/2025] (30-day overlap) → Auto-chunked to [12/01/2024 - 12/01/2025]
Sync 3: [11/01/2025 - 10/30/2025] (30-day overlap from 12/01) → Captures recent data
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication
The SAM.gov API uses API key authentication. To obtain an API key:

1. Create an account on [SAM.gov](https://sam.gov)
2. Navigate to your Account Details page
3. Enter your account password to access API key information
4. Generate a new public API key
5. Copy the API key (it's visible until you navigate away from the page)

The API key should be included in every request as the `api_key` parameter. Different user roles (federal vs non-federal) have different rate limits.

## Pagination
The connector implements pagination using the SAM.gov API's `limit` and `offset` parameters (refer to the `fetch_opportunities_page` function and the pagination loop in the `update` function):
- Page size: Maximum 1000 records per request
- Pagination logic: Processes data page by page in a loop, tracking offset position
- State management: Tracks `last_offset` and `total_records_processed` for resumable syncs
- Checkpointing: Saves progress after each page of results to handle interruptions gracefully

## Data handling
The connector processes SAM.gov opportunity data through several transformation steps (refer to the `process_main_opportunity_record` and `process_breakout_tables` functions):

1. Main opportunities table - Stores core opportunity information with flattened nested objects
2. Breakout tables - Separate tables for array data with foreign key relationships:
  - `point_of_contact` - Contact information for each opportunity
  - `naics_codes` - NAICS classification codes
  - `links` - API self-reference links
  - `resource_links` - Document and resource URLs

Flattening strategy (implemented in the `flatten_dict` function):
- Nested objects like `placeOfPerformance` become `place_of_performance_*` columns
- Award information becomes `award_*` columns
- Empty objects are converted to NULL values

Data transformation (refer to the `process_main_opportunity_record` function):
- CamelCase API fields converted to snake_case database columns
- Array fields extracted to separate tables with composite primary keys
- Foreign key relationships maintained through `notice_id`

## Error handling
The connector implements comprehensive error handling with retry logic and detailed error messages (refer to the `make_api_request`, `make_api_request_with_retry`, and `validate_configuration` functions):

Configuration validation errors:
- Missing required fields (api_key, posted_from, posted_to)
- Invalid date format (must be MM/dd/yyyy)
- Date range exceeding 1 year

API request errors with specific handling:
- 401 Unauthorized - Invalid API key
- 403 Forbidden - Access denied or quota exceeded
- 404 Not Found - Invalid endpoint
- 429 Rate Limit - Too many requests
- 500+ Server errors - SAM.gov service issues
- Timeout errors - Slow API response
- Connection errors - Network issues

Additional features:
- Retry logic - Exponential backoff for transient errors (3 attempts maximum)
- Individual record resilience - Failed records don't stop entire sync
- Detailed logging - Comprehensive error tracking for troubleshooting

## Tables created
The connector creates the following tables (refer to the `schema` function):

### opportunities
- Primary key: `notice_id`
- Description: Core opportunity information with flattened nested objects
- Key columns: title, solicitation_number, posted_date, type, naics_code, response_dead_line, organization_type, office_address fields, place_of_performance fields, award fields

### point_of_contact
- Primary key: `notice_id`, `contact_index`
- Description: Contact information for each opportunity
- Foreign key: `notice_id` references opportunities table
- Key columns: type, full_name, email, phone, fax, title

### naics_codes
- Primary key: `notice_id`, `code_index`
- Description: NAICS classification codes for each opportunity
- Foreign key: `notice_id` references opportunities table
- Key columns: naics_code

### links
- Primary key: `notice_id`, `link_index`
- Description: API reference links
- Foreign key: `notice_id` references opportunities table
- Key columns: rel, href

### resource_links
- Primary key: `notice_id`, `link_index`
- Description: Document and resource URLs
- Foreign key: `notice_id` references opportunities table
- Key columns: resource_link

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
