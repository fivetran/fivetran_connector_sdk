# Smartsheet Connector Example

## Connector overview
This connector demonstrates how to sync data from Smartsheet sheets and reports using the Fivetran Connector SDK and the [Smartsheet API](https://smartsheet.redoc.ly/). It provides a comprehensive implementation with support for:

- **Multiple sheets and reports** - Sync from multiple sheets and reports in a single connector instance
- **Incremental syncs** - Uses `modifiedSince` parameter to fetch only updated rows based on state management
- **Deletion detection** - Tracks row IDs across syncs to detect and propagate deleted rows
- **Automatic pagination** - Handles large sheets with automatic page fetching
- **Dynamic schema** - Automatically generates table schemas based on configured sheets and reports
- **Rate limiting** - Respects Smartsheet API rate limits with configurable requests per minute
- **Retry logic** - Implements exponential backoff for transient API failures
- **Type handling** - Properly parses Smartsheet column types (dates, numbers, checkboxes, etc.)
- **Data flattening** - Normalizes nested structures and cell values into flat table rows

This connector is designed as a production-ready example that handles the complexity of syncing sheet and report data from Smartsheet, including edge cases like pagination, deletion tracking, and incremental updates.


## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)


## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## Features
- **Multi-resource sync** - Syncs multiple sheets and reports simultaneously with independent state tracking
- **Incremental updates** - Uses per-sheet `last_modified` timestamps to fetch only changed rows
- **Deletion tracking** - Compares row IDs between syncs to detect and delete removed rows in destination
- **Automatic pagination** - Fetches all pages automatically with duplicate detection to prevent infinite loops
- **Dynamic table creation** - Creates one table per sheet/report with normalized naming
- **Column type awareness** - Properly handles DATE, DATETIME, NUMBER, CURRENCY, PERCENT, CHECKBOX, and TEXT column types
- **Rate limit compliance** - Implements sliding window rate limiting with configurable requests per minute
- **Retry mechanism** - Uses exponential backoff for 429, 500, 502, 503, and 504 HTTP errors
- **State persistence** - Maintains per-sheet and per-report state for reliable incremental syncs
- **Cell value normalization** - Prefers `displayValue` over raw `value` for consistent data representation
- **MD5 hash IDs** - Generates deterministic primary keys from Smartsheet row IDs


## Configuration file
The connector requires the following configuration parameters:

```json
{
  "api_token": "YOUR_SMARTSHEET_API_TOKEN",
  "sheets": "1234567890123456:Sheet Name,9876543210987654:Another Sheet",
  "reports": "1111111111111111:Report Name,2222222222222222:Another Report",
  "requests_per_minute": "60"
}
```

### Configuration parameters

- **api_token** (required) - Your Smartsheet API access token. Generate one from **Account** > **Apps & Integrations** > **API Access** in Smartsheet.

- **sheets** (optional) - Comma-separated list of sheet configurations in format `sheet_id:display_name`. The display name is used for table naming. Example: `1234567:Sales Data,5678901:Inventory`. If omitted, no sheets will be synced.

- **reports** (optional) - Comma-separated list of report configurations in format `report_id:display_name`. Example: `1111111:Q1 Report,2222222:Executive Summary`. If omitted, no reports will be synced.

- **requests_per_minute** (optional) - Maximum API requests per minute. Default is 60. Adjust based on your Smartsheet plan's rate limits.

Note: Incremental syncs are automatically managed via state tracking - the connector uses `modifiedSince` internally based on the last successful sync timestamp.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file
This connector requires the following Python package:

```
requests
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Authentication
This connector uses **Bearer token authentication**. The API token is passed in the `Authorization` header for all Smartsheet API requests.

To obtain an API token:
1. Log in to your Smartsheet account.
2. Navigate to **Account** > **Apps & Integrations** > **API Access**.
3. Click **Generate new access token**.
4. Copy the token and add it to your `configuration.json` as the `api_token` value.

The token provides access to all sheets and reports accessible by your Smartsheet account.


## Pagination
The connector implements automatic pagination using Smartsheet's page-based pagination pattern:

- **Page size** - 100 rows per page (Smartsheet API default)
- **Page parameter** - Uses `page` and `pageSize` query parameters
- **Duplicate detection** - Tracks row IDs across pages to prevent infinite loops
- **Termination conditions** - Stops when:
  - No rows are returned
  - Fewer rows than page size are returned
  - All returned rows are duplicates of previously fetched rows

Pagination is handled transparently by the `_fetch_paginated_data()` method in the `SmartsheetAPI` class (lines 393-467).


## Data handling

### Row structure
Each row from a Smartsheet sheet or report is transformed into a flat dictionary with the following fields:

- **id** - MD5 hash of the Smartsheet row ID (primary key)
- **row_id** - Original Smartsheet row ID (string)
- **sheet_id** / **report_id** - Source resource identifier
- **sheet_name** / **report_name** - Source resource name
- **[column_name]** - One field per sheet column with normalized name

### Column mapping
- Column names are normalized: lowercase, spaces and special characters replaced with underscores
- Cell values use `displayValue` when available, falling back to `value`
- Empty cells are included as `null` values

### Data type handling
The connector preserves Smartsheet column types using the `DataTypeHandler` class (lines 554-604):

- **TEXT_NUMBER** - Converted to string
- **DATE / DATETIME** - Parsed to Python datetime objects
- **NUMBER / CURRENCY / PERCENT** - Converted to Python Decimal for precision
- **CHECKBOX** - Converted to boolean
- **PICKLIST** - Converted to string

### State management
The `StateManager` class (lines 35-193) maintains:

- **Global state** - Latest modification timestamp across all resources
- **Per-sheet state** - Last modified timestamp and list of current row IDs
- **Per-report state** - List of current row IDs for deletion detection

State is persisted via checkpointing and used for incremental syncs and deletion detection.


## Error handling
The connector implements comprehensive error handling:

### API errors
- **HTTP 429 (Rate Limited)** - Automatically retries after waiting the time specified in the `Retry-After` header
- **HTTP 5xx (Server Errors)** - Retries up to 3 times with exponential backoff (1s, 2s, 4s delays)
- **Request failures** - Uses `requests.Session` with automatic retry via `HTTPAdapter` and `urllib3.Retry`

### Row processing errors
- Individual row processing errors are logged as warnings and skipped
- Sheet processing continues even if one row fails
- Sheet processing errors are logged and the sync moves to the next sheet

### Configuration errors
- Missing `api_token` raises a clear error message in the `schema()` function
- Invalid sheet/report ID formats are handled gracefully

Error handling is distributed throughout the code, with primary retry logic in the `SmartsheetAPI.get()` method (lines 327-361).


## Tables created
The connector dynamically creates tables based on your configured sheets and reports:

### Sheet tables
For each configured sheet, a table is created with the naming pattern:

```
sheet_[normalized_sheet_name]
```

Example: Sheet "Sales Data" becomes table `sheet_sales_data`

### Report tables
For each configured report, a table is created with the naming pattern:

```
report_[normalized_report_name]
```

Example: Report "Q1 Summary" becomes table `report_q1_summary`

### Schema structure
All tables share a common base schema:

```json
{
  "table": "sheet_[name] or report_[name]",
  "primary_key": ["id"],
  "columns": {
    "id": "STRING"
  }
}
```

Additional columns are inferred dynamically from the actual data. The schema is intentionally minimal to allow for flexible column addition as sheets evolve.

### Example tables
```
sheet_sales_data
  - id (STRING, PRIMARY KEY)
  - row_id (STRING)
  - sheet_id (STRING)
  - sheet_name (STRING)
  - product_name (STRING)
  - quantity (DECIMAL)
  - price (DECIMAL)
  - order_date (DATETIME)

report_executive_summary
  - id (STRING, PRIMARY KEY)
  - row_id (STRING)
  - report_id (STRING)
  - report_name (STRING)
  - department (STRING)
  - total_revenue (DECIMAL)
  - status (STRING)
```


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

### Rate limits
Smartsheet enforces rate limits based on your plan type. The default configuration (60 requests/minute) is suitable for most plans. If you exceed rate limits, the connector will automatically back off and retry.

### Large sheets
For sheets with thousands of rows, the initial sync may take several minutes. Subsequent incremental syncs will be much faster as they only fetch modified rows.

### Deletion behavior
Deleted rows are detected by comparing row IDs between syncs. If a row exists in the previous state but not in the current API response, it is deleted from the destination. This requires row-level tracking and increases state size proportionally to sheet size.

### Test mode
The connector includes a test mode (disabled by default) that limits processing to 10 rows per sheet. Enable it by setting `_TEST_MODE = True` at the top of `connector.py` for development and testing purposes.
