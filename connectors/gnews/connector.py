"""
This example shows how to pull recent news articles from the NewsAPI service
and load them into a destination using the Fivetran Connector SDK.

This Fivetran Connector uses the NewsAPI `/v2/everything` endpoint to retrieve
articles for a given search term and date range. This connector demonstrates:
- Robust API communication with exponential backoff and retries for transient
  HTTP and network errors (e.g., 429, 5xx).
- Data normalization and flattening of nested JSON responses into a clean,
  tabular structure.
- Incremental-style paging with automatic stop conditions once all results
  are fetched or totalResults are reached.
- Idempotent upserts to the destination table (`news_stories`) using a
  composite primary key of `url` and `publishedAt`.
- Configurable parameters for search term, sorting, pagination, and date range.

Refer to NewsAPI documentation for details:
https://newsapi.org/docs/endpoints/everything

See the Fivetran Connector SDK Technical Reference:
https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

and Best Practices:
https://fivetran.com/docs/connectors/connector-sdk/best-practices
for additional implementation guidance.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import json
import time
import random
import requests

# Import required classes from the Fivetran Connector SDK.
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

__NEWSAPI_ENDPOINT = "https://newsapi.org/v2/everything"

# Default headers for API requests.
# User-Agent and Accept headers prevent 426 (Upgrade Required) responses.
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NewsFetcher/1.0; +https://newsapi.org)",
    "Accept": "application/json",
}


def schema(configuration: dict) -> List[Dict[str, Any]]:
    """
    Define the schema function which lets you configure the schema your connector delivers
    See the technical reference documentation for more details
    on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    """
    return [
        {
            "table": "news_stories",
            "primary_key": ["url", "publishedAt"],
        }
    ]


def normalize_articles(page_json: Dict[str, Any], query: str) -> List[Dict[str, Any]]:
    """
    Convert a NewsAPI response page into scalar-only rows.

    This function flattens nested JSON objects from the NewsAPI response
    and sanitizes fields to make them safe for database upserts.

    Args:
        page_json (dict): JSON response from NewsAPI for one page.
        query (str): The search term used in the API request.

    Returns:
        List of cleaned article rows suitable for upsert.
    """
    rows: List[Dict[str, Any]] = []
    for a in page_json.get("articles") or []:
        src = a.get("source") or {}

        # Build a single record.
        row = {
            "url": a.get("url"),
            "publishedAt": a.get("publishedAt"),
            "source_id": src.get("id"),
            "source_name": src.get("name"),
            "author": a.get("author"),
            "title": a.get("title"),
            "description": a.get("description"),
            "urlToImage": a.get("urlToImage"),
            "content": a.get("content"),
            "query": query,
        }

        # Sanitize nested and non-scalar fields by JSON encoding.
        clean_row: Dict[str, Any] = {}
        for k, v in row.items():
            if isinstance(v, (dict, list)):
                clean_row[k] = json.dumps(v, ensure_ascii=False)
            elif v is None or isinstance(v, (str, int, float, bool)):
                clean_row[k] = v
            else:
                clean_row[k] = str(v)

        rows.append(clean_row)

    return rows


def fetch_news_page(
    query: str,
    from_date: str,
    api_key: str,
    *,
    page: int = 1,
    page_size: int = 100,
    sort_by: str = "popularity",
    endpoint: str = __NEWSAPI_ENDPOINT,
    retries: int = 5,
    backoff_factor: float = 0.75,
    status_forcelist: Optional[set] = None,
    timeout: int = 20,
) -> Tuple[int, int, int]:
    """
    Fetches one page of articles from the NewsAPI, normalizes the data,
    and performs upserts into the destination table.

    Implements exponential backoff and retry logic for transient
    HTTP errors such as 429 (rate limit) or 5xx server errors.

    Args:
        query (str): Search keyword or phrase.
        from_date (str): Start date (YYYY-MM-DD) for filtering news.
        api_key (str): NewsAPI authentication key.
        page (int): The current page number to fetch.
        page_size (int): Number of articles per page (max 100).
        sort_by (str): Sorting method (e.g., 'popularity', 'publishedAt').
        endpoint (str): API endpoint URL.
        retries (int): Max number of retry attempts for transient errors.
        backoff_factor (float): Base factor for exponential backoff delay.
        status_forcelist (set): HTTP status codes that should trigger a retry.
        timeout (int): Request timeout in seconds.

    Returns:
        Tuple containing:
            (upserts_count, articles_fetched, total_results)
    """
    if status_forcelist is None:
        status_forcelist = {426, 429, 500, 502, 503, 504}

    # Define request parameters for NewsAPI query.
    params = {
        "q": query,
        "from": from_date,
        "sortBy": sort_by,
        "apiKey": api_key,
        "page": page,
        "pageSize": page_size,
    }

    attempt = 0
    while True:
        attempt += 1
        try:
            # Send the GET request to NewsAPI.
            resp = requests.get(endpoint, headers=DEFAULT_HEADERS, params=params, timeout=timeout)

            # Handle transient error codes with retries.
            if resp.status_code in status_forcelist:
                if attempt <= retries:
                    sleep_s = backoff_factor * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                    log.info(f"Transient HTTP {resp.status_code}. Retrying in {sleep_s:.2f}s...")
                    time.sleep(sleep_s)
                    continue
                else:
                    resp.raise_for_status()

            # Raise an error for any non-200 response.
            if resp.status_code != 200:
                resp.raise_for_status()

            data = resp.json()
            if data.get("status") != "ok":
                raise RuntimeError(f"NewsAPI error: {data}")

            # Extract and normalize article data.
            articles = data.get("articles", []) or []
            total_results = int(data.get("totalResults", 0))
            rows = normalize_articles(data, query)

            # Upsert normalized rows to destination.
            upserts_count = 0
            for rec in rows:
                try:
                    # The 'upsert' operation is used to insert or update data
                    # in the destination table. The first argument is the
                    # name of the destination table.
                    # The second argument is a dictionary containing the record to be upserted.
                    op.upsert(table="news_stories", data=rec)
                    upserts_count += 1
                except Exception as e:
                    log.error(f"Failed to upsert record: {rec}\nError: {e}")
                    continue

            log.info(
                f"Fetched {len(articles)} articles, upserted {upserts_count} "
                f"from page {page}. totalResults={total_results}"
            )
            return upserts_count, len(articles), total_results

        except (requests.Timeout, requests.ConnectionError) as e:
            # Handle connection/timeout errors with exponential retry.
            if attempt <= retries:
                sleep_s = backoff_factor * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                log.info(f"Network error: {e}. Retrying in {sleep_s:.2f}s...")
                time.sleep(sleep_s)
                continue
            raise


def fetch_all_news(
    query: str,
    from_date: str,
    api_key: str,
    *,
    page_size: int = 100,
    sort_by: str = "popularity",
    endpoint: str = __NEWSAPI_ENDPOINT,
    max_pages: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Iterates through all available pages of results for a given query.

    Stops when:
        - The current page returns fewer than `page_size` results.
        - All known results (totalResults) have been fetched.
        - max_pages limit (if specified) has been reached.

    Args:
        query (str): Search keyword or phrase.
        from_date (str): Date filter for results.
        api_key (str): API key for authentication.
        page_size (int): Number of articles per page.
        sort_by (str): Sort order.
        endpoint (str): NewsAPI endpoint.
        max_pages (int | None): Optional hard cap for paging.

    Returns:
        A summary dictionary with pages fetched and total upserts.
    """
    page = 1
    total_upserts = 0
    pages_fetched = 0
    total_results_seen: Optional[int] = None

    while True:
        log.info(f"Fetching page {page}...")

        # Fetch one page of results.
        upserts_count, articles_fetched, total_results = fetch_news_page(
            query=query,
            from_date=from_date,
            api_key=api_key,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            endpoint=endpoint,
        )

        # Store total result count after first page.
        if total_results_seen is None:
            total_results_seen = total_results

        # Stop if no data was returned.
        if articles_fetched == 0:
            break

        pages_fetched += 1
        total_upserts += upserts_count

        # Stop if we’ve clearly reached the end of the result set.
        if articles_fetched < page_size:
            break
        if total_results_seen is not None and page * page_size >= total_results_seen:
            break
        if max_pages and page >= max_pages:
            break

        page += 1

    return {
        "status": "ok",
        "pagesFetched": pages_fetched,
        "recordsSynced": total_upserts,
        "totalResults": total_results_seen or 0,
    }


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure
    how your connector fetches data.
    See the technical reference documentation for more details
    on the update function: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    args:
        configuration: A dictionary containing connection details.
        state: A dictionary containing state information from previous runs.
               The state dictionary is empty for the first sync or for any
               full re-sync.
    """

    api_key = configuration["api_key"]
    search_term = configuration["search_term"]
    from_date = configuration["from_date"]

    # Perform the NewsAPI sync and log summary statistics.
    result = fetch_all_news(search_term, from_date, api_key)
    log.info(
        f"Sync finished. pagesFetched={result['pagesFetched']} "
        f"recordsSynced={result['recordsSynced']}"
    )


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":

    try:
        with open("configuration.json", "r") as f:
            configuration = json.load(f)

        if not configuration.get("api_key"):
            log.warning("Please update configuration.json with a valid api_key")

        connector.debug(configuration=configuration)

    except FileNotFoundError:
        log.severe("configuration.json not found. Please create it for local testing.")
    except Exception as e:
        log.severe(f"Unexpected error during debug execution: {e}")

