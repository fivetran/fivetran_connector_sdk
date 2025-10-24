# New York Times Connector Example

## Connector overview
This connector synchronizes data from the New York Times API, providing access to both historical articles through the Archive API and trending content through the Most Popular API. It enables users to retrieve and analyze news content and monitor popular articles.

## Features
- Archive API integration for historical article retrieval
- Most Popular API support for trending content analysis
- Incremental updates with state management
- Rate limit handling with exponential backoff
- Support for multiple data streams (emailed, shared, viewed)

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Configuration file
```json
{
    "api_key": "YOUR_API_KEY",
    "start_date": "2024-01",
    "period": "7",
    "share_type": "facebook"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.
```

Note: The `period` parameter must be provided as a string value.

- `api_key` - Your NY Times API key for authentication
- `start_date` - Initial sync date in YYYY-MM format
- `end_date` - (Optional) End date for sync in YYYY-MM format
- `period` - Time window for most popular articles (must be "1", "7", or "30")
- `share_type` - (Optional) Type of sharing for most popular articles

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Authentication
Authentication is handled using an API key obtained from the NY Times Developer Portal. The key is passed in the `api-key` parameter with each request. Refer to the `make_request` function in `connector.py` (lines 105-143) for implementation details.

## Pagination
The connector implements date-based pagination for the Archive API, processing one month of articles at a time. State management ensures efficient incremental updates. Refer to the `fetch_articles` function in `connector.py` (lines 145-196) for implementation details.

## Data handling

The connector processes two main data streams:

1. Article Archive Stream (lines 197-220):
   - Fetches historical articles month by month
   - Transforms raw API response into structured records
   - Handles nested JSON structures and date formatting

2. Most Popular Stream (lines 221-245):
   - Retrieves trending articles based on configured period
   - Supports multiple endpoints (emailed, shared, viewed)
   - Converts large ID values to strings for compatibility

Data transformation is handled by dedicated functions for each stream type. Refer to `transform_article` and `transform_popular_article` functions in `connector.py`.

## Error handling
The connector implements comprehensive error handling (refer to lines 105-143 in `connector.py`):
- Rate limit detection and exponential backoff with sync failure preservation
- Network error retry logic with configurable attempts
- Date format validation and transformation
- Configuration parameter validation
- State management for failed syncs (including rate limit failures)
- Secure error logging with API key redaction
- Detailed error logging for troubleshooting

### Rate Limit Handling
When encountering rate limits (HTTP 429), the connector:
1. Attempts retries with exponential backoff
2. If max retries are exceeded, fails the sync gracefully
3. Preserves the current state to resume from the same point in the next sync
4. Ensures no data loss during rate-limited scenarios

## Tables created
The connector creates two main tables:

## Requirements file
The connector requires additional Python packages specified in `requirements.txt`:

```
requests
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Data handling
Data processing is managed through two main functions in `connector.py`:
- `transform_article` - Handles Archive API response transformation
- `transform_popular_article` - Processes Most Popular API data
Both functions map source data to the defined schema and handle data type conversions.

### Archive Date Tracking
The connector now includes an `archive_date` column in the Articles table that:
- Records the first day of the month for which the article was retrieved
- Helps track which archive period each article belongs to
- Facilitates data analysis and verification of sync processes
- Format: ISO 8601 UTC datetime (e.g., "2023-03-01T00:00:00+00:00" for articles from March 2023)
- Uses midnight UTC (00:00:00) to represent the start of each month
- Includes timezone information (+00:00) for proper datetime handling

## Error handling
Error handling is implemented in the `make_request` function in `connector.py`, featuring rate limit detection, exponential backoff, and network error retries.

## Tables created

### Most Popular table
Primary key: `id`
- `id` (STRING) - Unique article identifier
- `url` (STRING) - Article URL
- `adx_keywords` (STRING) - Article keywords
- `section` (STRING) - Article section
- `byline` (STRING) - Author information
- `type` (STRING) - Content type
- `title` (STRING) - Article title
- `abstract` (STRING) - Article abstract
- `published_date` (UTC_DATETIME) - Publication date
- `source` (STRING) - Content source
- `updated` (UTC_DATETIME) - Last update timestamp
- `uri` (STRING) - Article URI


### Articles table
Primary key: `_id`
- `_id` (STRING) - Unique article identifier
- `web_url` (STRING) - Article URL
- `snippet` (STRING) - Article summary
- `print_page` (STRING) - Print edition page
- `print_section` (STRING) - Print edition section
- `source` (STRING) - Content source
- `pub_date` (UTC_DATETIME) - Publication date
- `document_type` (STRING) - Document type
- `news_desk` (STRING) - News desk category
- `section_name` (STRING) - Section name
- `type_of_material` (STRING) - Material type
- `word_count` (INT) - Article word count
- `uri` (STRING) - Article URI
- `abstract` (STRING) - Article abstract
- `archive_date` (UTC_DATETIME) - The first day of the month from which this article was retrieved from the archive API, stored as UTC midnight (e.g., "2025-10-01T00:00:00+00:00")


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.


## Error Handling
- Validates configuration parameters before sync
- Handles API errors with retries and exponential backoff
- Checkpoints state after each successful batch
- Gracefully handles rate limiting with configurable retry attempts
- Robust date format handling for various API response formats
- Comprehensive error logging and state management

## Development and Testing
1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Create configuration.json with your API credentials
4. Run the connector locally: `python connector.py`

## Additional Notes
- Keep your API key secure and never commit it to version control
- Monitor API usage to stay within rate limits
- Consider implementing more granular error handling for production use