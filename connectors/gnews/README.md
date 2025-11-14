# GNews Search Connector Example

## Connector overview
This example demonstrates how to extract recent news articles from the [GNews v4 Search API](https://gnews.io/docs/v4#tag/Search) and load them into a destination using the Fivetran Connector SDK.  
The connector:
- Retrieves paginated GNews search results for a user-defined query and date.  
- Implements resilient HTTP calls with exponential backoff and retries (e.g., 429/5xx).  
- Flattens structured JSON responses into a clean, tabular format.  
- Performs idempotent upserts into a single destination table (`news_stories`) using a composite primary key (`url`, `publishedAt`).  
- Logs plan-limit messages returned by GNews (e.g., free-tier real-time delays, 30-day historical window).

Related functions in `connector.py`:  
`schema`, `update`, `fetch_news_page`, `fetch_all_news`, `normalize_articles`, `validate_configuration`.

## Requirements
- Python version ≥3.10 and ≤3.13
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
    "search_term": "<INSERT_SEARCH_TERM>"
}
```
- `api_key` (required): Your GNews API key
- `search_query`(required): The query you are looking to get data on

Note: Ensure that `configuration.json` is not committed to version control.  


## Requirements file
The `requirements.txt` file lists external libraries needed for this connector.

Example content of `requirements.txt`:
python-dotenv==1.1.1

- `python-dotenv==1.1.1` is required to load environment variables from a `.env` file.

## API calls
- **Endpoint:** `https://gnews.io/api/v4/search`

## Parameters

- api_key: Your GNews API key (`apikey` in requests).  
- search_term: Keyword or phrase to search.  
- from_date: ISO date (`YYYY-MM-DD`) or datetime. Defaults to `2025-10-15` in the example code.  
- page_size: Page size (max 100). Default: `100`.  
- sort_by: One of `popularity`, `publishedAt`, `relevance`. Default: `popularity`.  
- lang: ISO 639-1 language filter (e.g., `en`).  
- country: ISO 3166-1 alpha-2 country filter (e.g., `us`).  
- max_pages: Stop after this many pages even if more results exist.

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

## Schema definition

`schema(configuration)` defines one table:

### `news_stories`
- Primary key: `url`, `publishedAt`
- Representative columns (not exhaustive):
  - `url`
  - `publishedAt`
  - `title`
  - `description`
  - `content`
  - `urlToImage`
  - `source_id`
  - `source_name`
  - `source_url`
  - `source_country`
  - `lang`
  - `gnews_id`
  - `query`

Although GNews provides an article `id`, the connector keeps `url + publishedAt` as a composite key for cross-source stability and compatibility with prior NewsAPI-based schemas.

## Error handling

- Transient errors (429/5xx, timeouts, connection issues):  
  Retried up to 5 times with exponential backoff and small random jitter.

- Fatal errors:  
  Non-200 responses (not in the retry allowlist) raise immediately after logging.

- Configuration validation:  
  `validate_configuration` raises if required fields are missing.

- GNews plan-limit messages:  
  If the response includes `information` (e.g., “real-time news available only on paid plans”) or `articlesRemovedFromResponse` (e.g., “historical beyond 30 days”), the connector logs those messages for observability.

## Table created

**Summary of the table replicated**

### `news_stories`
- Primary key: `url`, `publishedAt`
- Selected columns (not exhaustive):  
  `url`, `publishedAt`, `title`, `description`, `content`, `urlToImage`,  
  `source_id`, `source_name`, `source_url`, `source_country`, `lang`, `gnews_id`, `query`

## Additional files

- `connector.py` – Core logic: `schema`, `update`, `fetch_news_page`, `fetch_all_news`, `normalize_articles`, `validate_configuration`.  
- `configuration.json` – API credentials and runtime parameters.  
- `requirements.txt` – Third-party Python dependencies (e.g., `requests`).

## Migration notes (from NewsAPI)

- Parameter name for key is `apikey` (not `apiKey`).  
- Count field is `totalArticles` (not `totalResults`).  
- Image field is `image` (mapped to `urlToImage`).  
- No top-level `status: ok`; rely on HTTP 200 + expected fields.  
- Optional `lang`/`country` filters are supported and pass-through.

## Additional considerations

This example serves as a reference integration of **GNews** with the **Fivetran Connector SDK**. Behavior may vary based on plan limits, rate limits, and content availability.  
For troubleshooting and enhancements, consult the [Fivetran Connector SDK documentation](https://fivetran.com/docs/connectors/connector-sdk) or contact Fivetran Support.


