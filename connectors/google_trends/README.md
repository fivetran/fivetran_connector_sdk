# Google Trends Connector Example

## Connector overview

This connector syncs Google Trends interest-over-time data into Fivetran, allowing you to track search interest for specific keywords across different regions and time periods. The connector uses a full refresh sync strategy, creating complete snapshots of trends data on each run. This append-only design with sync timestamps enables historical tracking of how Google Trends data changes over time.

The connector creates a single `google_trends` table with detailed interest scores for each keyword, date, and region combination. Each sync is tracked with a timestamp, allowing you to analyze how search interest evolves over time.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Full refresh sync strategy – Fetches complete timeframes on every sync to track historical changes
- Multiple keyword comparison – Query up to 5 keywords per search group with relative interest scores
- Multi-region support – Fetch data for worldwide or specific country regions in a single sync
- Search grouping – Keywords in the same search group are normalized together for accurate comparison
- Incremental historical tracking – Append-only design with sync timestamps preserves data evolution
- Exponential backoff retry – Automatic retry with randomized delays (60-120s, 120-180s, 240-300s) handles rate limiting
- Partial data detection – Identifies incomplete data points marked as partial by Google Trends
- Flexible timeframe formats – Supports both relative ("today 12-m") and absolute date ranges ("2024-01-01 2026-02-03")

## Configuration file

The connector requires a `configuration.json` file with a `searches` array. Each search defines a group of keywords to compare, the regions to query, and the timeframe to analyze.

```json
{
  "searches": [
    {
      "name": "ETL Tools Comparison",
      "keywords": [
        "Fivetran",
        "Airflow",
        "dbt"
      ],
      "regions": [
        {"name": "Worldwide", "code": ""},
        {"name": "United States", "code": "US"},
        {"name": "United Kingdom", "code": "GB"}
      ],
      "timeframe": "2024-01-01 today"
    }
  ]
}
```

Configuration parameters:
- `searches` – Array of search configurations (required)
  - `name` – Human-readable name for the search group
  - `keywords` – Array of 1-5 keywords to compare (required, Google Trends limit)
  - `regions` – Array of region objects (required)
    - `name` – Display name for the region
    - `code` – ISO country code (empty string "" for worldwide)
  - `timeframe` – Time period in Google Trends format (required)
    - Relative format: "today 12-m" (last 12 months), "today 3-m" (last 3 months)
    - Absolute format: "2024-01-01 2026-02-03" (specific date range)
    - Note: "today" keyword in absolute ranges is automatically converted to current date

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector. This connector requires the PyTrends library for accessing Google Trends data and Pandas for data processing.

```
# Google Trends API client library
pytrends==4.9.2

# Pandas for data processing (required by PyTrends)
pandas==3.0.0
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

This connector uses the public Google Trends API, which does not require authentication credentials. The connector accesses publicly available search interest data without needing API keys, OAuth tokens, or user credentials.

The connector configures HTTP headers compatible with the Google Trends web interface to ensure reliable access. Refer to `def initialize_pytrends()` in `connector.py` for the header configuration.

## Pagination

Pagination is not applicable for this connector. Google Trends returns complete datasets for the specified timeframe in a single API response. Each request fetches all available data points for the configured keywords, regions, and time period.

The connector uses a full refresh approach, pulling the entire timeframe on each sync rather than paginating through incremental updates.

## Data handling

The connector processes Google Trends data through the following steps:

1. Configuration validation – Validates search configurations including keyword limits (1-5 per search), required fields, and timeframe formats. Refer to `def validate_configuration()`.

2. Search ID generation – Creates unique, human-readable identifiers for each search group using the format `{search_name}_{timeframe}_{hash}`. This groups keywords that were queried together, ensuring their relative interest scores remain comparable. Refer to `def generate_search_id()`.

3. Timeframe conversion – Automatically converts "today" keywords in absolute timeframes to actual dates (e.g., "2024-01-01 today" becomes "2024-01-01 2026-02-03"). Refer to `def process_search_group()`.

4. Data fetching – Fetches interest-over-time data for each region independently using the PyTrends library. Refer to `def fetch_region_data_with_retry()`.

5. Data transformation – Converts pandas DataFrame responses into individual records with the following schema:
   - `search_id` – Groups keywords from the same search
   - `keyword` – The search term
   - `date` – ISO-formatted timestamp (UTC)
   - `region_name` – Display name of the region
   - `region_code` – ISO country code or "WORLDWIDE"
   - `sync_timestamp` – ISO timestamp of when this sync ran
   - `interest` – Interest score (0-100 scale)
   - `timeframe` – Original timeframe query string
   - `is_partial` – Boolean flag for incomplete data points

6. Data upserting – Each record is upserted to the `google_trends` table. The combination of `search_id`, `keyword`, `date`, `region_code`, and `sync_timestamp` forms the primary key, ensuring each sync creates new records for historical tracking. Refer to `def process_region()`.

## Error handling

The connector implements comprehensive error handling strategies to ensure reliable data synchronization:

### Retry logic with exponential backoff
The connector automatically retries failed API requests up to 5 times with exponential backoff and random jitter to prevent thundering herd problems. Refer to `def fetch_region_data_with_retry()`.

- Retry 1: wait 60-120 seconds (60s base + 0-60s jitter)
- Retry 2: wait 120-180 seconds (120s base + 0-60s jitter)
- Retry 3: wait 240-300 seconds (240s base + 0-60s jitter)
- Retry 4: wait 480-540 seconds (480s base + 0-60s jitter)
- Retry 5: wait 960-1020 seconds (960s base + 0-60s jitter)

Refer to `def calculate_retry_delay()` and `def handle_retry_sleep()` for retry delay calculations.

### Regional failure tolerance
If individual regions fail after all retries, the connector continues processing other regions and logs the failures. The sync only fails if all regions fail or no data is synced. Refer to `def update()` in `connector.py:270-356`.

### Detailed error logging
All errors are logged with comprehensive diagnostic information including HTTP status codes, response headers, request parameters, and full stack traces. Refer to `def log_error_details()` and `def log_http_response_details()`.

### Configuration validation
The connector validates all configuration parameters before attempting any API requests, providing clear error messages for missing or invalid fields. Refer to `def validate_configuration()`, `def _validate_search()`, and `def _validate_region()`.

### State checkpointing
After each successful sync, the connector checkpoints state information including the sync timestamp, total records synced, and any failed regions. Refer to `def update()` at `connector.py:336-344`.

## Tables created

The connector creates a single table named `google_trends` with the following schema:

### google_trends

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `search_id` | STRING | Unique identifier grouping keywords from the same search (primary key) |
| `keyword` | STRING | The search term being tracked (primary key) |
| `date` | UTC_DATETIME | The date of the interest measurement (primary key) |
| `region_code` | STRING | ISO country code or "WORLDWIDE" (primary key) |
| `sync_timestamp` | UTC_DATETIME | Timestamp when this record was synced (primary key) |
| `region_name` | STRING | Human-readable region name |
| `interest` | INT | Interest score on a 0-100 scale |
| `timeframe` | STRING | Original timeframe query used |
| `is_partial` | BOOLEAN | Indicates if the data point is incomplete |

The composite primary key `(search_id, keyword, date, region_code, sync_timestamp)` ensures that:
- Each sync creates new records, enabling historical tracking of data changes
- Keywords queried together (same search_id) remain grouped for comparative analysis
- Data is unique per keyword, date, region, and sync timestamp

Example data:

| search_id | keyword | date | region_code | sync_timestamp | region_name | interest | timeframe | is_partial |
|-----------|---------|------|-------------|----------------|-------------|----------|-----------|------------|
| etl_tools_comparison_2024-01-01_today_a1b2c3d4 | Fivetran | 2024-01-01 | US | 2026-02-03T12:00:00Z | United States | 75 | 2024-01-01 today | false |
| etl_tools_comparison_2024-01-01_today_a1b2c3d4 | Airflow | 2024-01-01 | US | 2026-02-03T12:00:00Z | United States | 100 | 2024-01-01 today | false |

## Additional files

- `config.py` – Defines default search configurations used during development and testing. The connector falls back to this file if searches are not provided in the Fivetran configuration. Contains a `SEARCHES` array with the same structure as the configuration.json searches parameter.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
