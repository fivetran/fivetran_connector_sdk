# GNews Search Connector Example

## Connector overview
This example demonstrates how to build a Fivetran Connector SDK integration for [GNews v4 Search API](https://gnews.io/docs/v4#tag/Search), a REST API service to search articles from 60,000+ worldwide sources. The API provides access to real-time news and historical data, as well as top headlines based on Google News rankings. The connector pulls Google search information from GNews's API, and delivers it to your Fivetran destination.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- GNews Search: Fetches news for a given `search_term` and `from_date` via `/api/v4/search`.  
- Pagination: Iterates over pages until the end of the result set or an optional `max_pages` cap.  
- Error handling: Retries transient failures (429/5xx and network timeouts) with exponential backoff and jitter.  
- Data normalization: Flattens `articles[*]` and maps fields to stable column names (`image` → `urlToImage`).  
- Schema: Defines one destination table — `news_stories` with primary key (`url`, `publishedAt`).  
- Logging: Uses `fivetran_connector_sdk.Logging` for structured info and error logs, including plan-limit messages.

## Configuration file
The `configuration.json` file provides the GNews credentials and query parameters required for API requests.

```json
{
    "api_key": "<INSERT_YOUR_GNEWS_API_KEY>",
    "search_term": "<INSERT_SEARCH_TERM>",
    "from_date": "<YYYY-MM-DD_START_DATE>"
}
```
- `api_key` (required): Your GNews API key
- `search_query`(required): The query you are looking to get data on
- `from_date` (required): How far back the historical sync will go

Note: Ensure that `configuration.json` is not committed to version control.  


## Requirements file
The `requirements.txt` file lists external libraries needed for this connector.
Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Data handling
- Normalization: `normalize_articles` flattens each `articles[*]` object and maps:
  - `image` → `urlToImage` (to keep parity with existing schema/column names)
  - Adds pass-through fields: `gnews_id`, `lang`, `source_url`, `source_country`, plus `query`
  - Persists only scalar fields (nested values are JSON-encoded strings)

- Upserts: Each record is written via `op.upsert(...)` to `news_stories`.

- Stop conditions: `fetch_all_news` stops when:
  - Fewer than `page_size` results are returned, or  
  - `page * page_size >= totalArticles`, or  
  - `max_pages` is reached, or  
  - A page returns zero results.


## Error handling
- Transient errors (429/5xx, timeouts, connection issues):  
  Retried up to 5 times with exponential backoff and small random jitter.

- Fatal errors:  
  Non-200 responses (not in the retry allowlist) raise immediately after logging.

## Tables created
**Summary of the table replicated**

### `news_stories`
- Primary key: `url`, `publishedAt`
- Selected columns (not exhaustive):  
  `url`, `publishedAt`, `title`, `description`, `content`, `urlToImage`,  
  `source_id`, `source_name`, `author`, `query`

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.


