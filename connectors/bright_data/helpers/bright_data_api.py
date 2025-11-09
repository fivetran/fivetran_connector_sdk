"""Bright Data API client wrapper and search functionality."""

import time
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote_plus

import requests
from requests import Response
from requests.exceptions import RequestException

from brightdata import bdclient
from fivetran_connector_sdk import Logging as log

BRIGHT_DATA_BASE_URL = "https://api.brightdata.com"
BRIGHT_DATA_REQUEST_URL = f"{BRIGHT_DATA_BASE_URL}/request"
BRIGHT_DATA_SCRAPER_TRIGGER_URL = f"{BRIGHT_DATA_BASE_URL}/datasets/v3/trigger"
BRIGHT_DATA_SCRAPER_DOWNLOAD_URL = (
    f"{BRIGHT_DATA_BASE_URL}/datasets/v3/download/{{snapshot_id}}"
)
DEFAULT_SERP_ZONE = "serp_api1"
DEFAULT_TIMEOUT_SECONDS = 120
RETRY_STATUS_CODES = {408, 429, 500, 502, 503, 504}


def perform_search(
    api_token: str,
    query: Union[str, List[str]],
    search_engine: Optional[str] = "google",
    country: Optional[str] = "us",
    zone: Optional[str] = DEFAULT_SERP_ZONE,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    retries: int = 3,
    backoff_factor: float = 1.5,
) -> Union[Dict[str, Any], List[Union[Dict[str, Any], List[Dict[str, Any]]]]]:
    """
    Perform a search using Bright Data's REST SERP endpoint.

    Supports both single query (string) and multiple queries (list). Each query is executed
    sequentially against the REST API and the responses are normalized to match the structure
    expected by downstream processing.

    Args:
        api_token: Bright Data API token used for Bearer authentication
        query: Search query string or list of query strings
        search_engine: Optional search engine - "google", "bing", or "yandex" (default: "google")
        country: Optional two-letter country code (default: "us")
        zone: Optional Bright Data zone identifier (default: "serp_api1")
        timeout: Request timeout per attempt in seconds (default: 120)
        retries: Number of retry attempts for transient failures (default: 3)
        backoff_factor: Multiplier for exponential backoff between retries (default: 1.5)

    Returns:
        Dictionary or list of dictionaries containing parsed search results. When multiple
        queries are provided, returns a list where each element corresponds to the results
        for the matching query.

    Raises:
        ValueError: If api_token, query, or search parameters are invalid
        TypeError: If query type is invalid
        RuntimeError: If the Bright Data API returns an error after retries
    """
    # Validate API token and query inputs
    if not api_token or not isinstance(api_token, str):
        raise ValueError("A valid Bright Data API token is required")

    if not query:
        raise ValueError("Query cannot be empty")

    if not isinstance(query, (str, list)):
        raise TypeError("Query must be a string or list of strings")

    # Normalize queries to a list for consistent processing
    queries: List[str]
    if isinstance(query, list):
        if not all(isinstance(item, str) for item in query):
            raise TypeError("All queries must be strings")
        queries = [item.strip() for item in query if item and item.strip()]
    else:
        queries = [query.strip()]

    if not queries:
        raise ValueError("At least one non-empty query must be provided")

    # Validate search engine selection
    valid_search_engines = {"google", "bing", "yandex"}
    if search_engine and search_engine.lower() not in valid_search_engines:
        log.info(
            f"Warning: Invalid search engine '{search_engine}'. Using default 'google'"
        )
        search_engine = "google"

    selected_engine = (search_engine or "google").lower()
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    zone_identifier = zone or DEFAULT_SERP_ZONE

    log.info(
        f"Executing Bright Data REST search for {len(queries)} query"
        f"{'ies' if len(queries) != 1 else ''} using zone '{zone_identifier}'"
    )

    results: List[Any] = []

    for single_query in queries:
        search_url = _build_search_url(single_query, selected_engine)
        payload: Dict[str, Any] = {
            "zone": zone_identifier,
            "url": search_url,
            "format": "json",
            "method": "GET",
        }

        if country:
            payload["country"] = country.lower()

        attempt = 0
        backoff = backoff_factor
        response_payload: Any = None

        while attempt <= retries:
            try:
                response = requests.post(
                    BRIGHT_DATA_REQUEST_URL,
                    headers=headers,
                    json=payload,
                    timeout=timeout,
                )

                if response.status_code == 200:
                    response_payload = _parse_serp_response(response)
                    break

                if response.status_code in RETRY_STATUS_CODES and attempt < retries:
                    log.info(
                        f"Bright Data SERP request retry "
                        f"{attempt + 1}/{retries} for query '{single_query}' "
                        f"(status code: {response.status_code})"
                    )
                    time.sleep(backoff)
                    backoff *= backoff_factor
                    attempt += 1
                    continue

                # Log error details and raise for non-retriable status codes
                error_detail = _extract_error_detail(response)
                log.info(
                    f"Bright Data SERP request failed for query '{single_query}': "
                    f"{error_detail}"
                )
                response.raise_for_status()

            except RequestException as exc:
                if attempt < retries:
                    log.info(
                        f"Error contacting Bright Data SERP API for query '{single_query}': "
                        f"{str(exc)}. Retrying ({attempt + 1}/{retries})"
                    )
                    time.sleep(backoff)
                    backoff *= backoff_factor
                    attempt += 1
                    continue
                raise RuntimeError(
                    f"Failed to execute Bright Data SERP request for query "
                    f"'{single_query}' after {retries} retries: {str(exc)}"
                ) from exc

        if response_payload is None:
            raise RuntimeError(
                f"Bright Data SERP request did not return a response for query "
                f"'{single_query}'"
            )

        normalized_results = _normalize_search_results(response_payload)
        results.append(normalized_results)

    if len(queries) == 1:
        # Return the first (and only) result set
        return results[0]

    return results


def _build_search_url(query: str, search_engine: str) -> str:
    """Build the target URL for a given search engine and query."""
    encoded_query = quote_plus(query)
    search_engine = search_engine.lower()

    engine_templates = {
        "google": "https://www.google.com/search?q={query}",
        "bing": "https://www.bing.com/search?q={query}",
        "yandex": "https://yandex.com/search/?text={query}",
    }

    template = engine_templates.get(search_engine, engine_templates["google"])
    return template.format(query=encoded_query)


def _parse_serp_response(response: Response) -> Any:
    """Return JSON payload when available, otherwise raw text."""
    try:
        return response.json()
    except ValueError:
        return response.text


def _extract_error_detail(response: Response) -> str:
    """Extract error detail from a failed Bright Data SERP response."""
    try:
        payload = response.json()
        if isinstance(payload, dict):
            for key in ("error", "message", "detail", "details"):
                if key in payload:
                    return str(payload[key])
            return str(payload)
        return str(payload)
    except ValueError:
        return response.text


def _normalize_search_results(payload: Any) -> List[Dict[str, Any]]:
    """
    Normalize Bright Data SERP responses into a list of dictionaries.

    The SERP API can return different structures depending on the endpoint configuration.
    This helper attempts to extract the list of result items, falling back to wrapping the
    payload in a list when necessary.
    """
    if isinstance(payload, list):
        return [
            item if isinstance(item, dict) else {"raw_response": str(item)}
            for item in payload
        ]

    if isinstance(payload, dict):
        for candidate_key in (
            "results",
            "organic_results",
            "organic",
            "data",
            "items",
            "serp",
        ):
            candidate_value = payload.get(candidate_key)
            if isinstance(candidate_value, list):
                return [
                    item if isinstance(item, dict) else {"raw_response": str(item)}
                    for item in candidate_value
                ]

        # Some responses may return nested structures under "data"
        data_field = payload.get("data")
        if isinstance(data_field, dict):
            for candidate_key in (
                "results",
                "organic_results",
                "organic",
                "items",
            ):
                candidate_value = data_field.get(candidate_key)
                if isinstance(candidate_value, list):
                    return [
                        item if isinstance(item, dict) else {"raw_response": str(item)}
                        for item in candidate_value
                    ]

        return [payload]

    return [{"raw_response": str(payload)}]


def perform_scrape(
    client: bdclient,
    url: Union[str, List[str]],
    country: Optional[str] = None,
    data_format: Optional[str] = None,
    format_param: Optional[str] = "json",
    method: Optional[str] = None,
    async_request: bool = True,
    max_poll_attempts: int = 60,
    poll_interval: int = 5,
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Scrape URLs using Bright Data SDK.

    Supports both single URL (string) and multiple URLs (list) for parallel processing.
    The SDK includes built-in retry logic with exponential backoff for robust error handling.

    When async_request=True, the function will handle snapshot_id responses and poll
    for results using download_snapshot() until the data is ready.

    Args:
        client: Initialized Bright Data client
        url: URL string or list of URLs to scrape
        country: Optional two-letter country code
        data_format: Optional data format - "markdown", "screenshot", etc.
        format_param: Optional response format - "json" or "raw"
        method: Optional HTTP method
        async_request: Enable async processing (default: False).
                       When True, returns snapshot_id that needs polling.
        max_poll_attempts: Maximum number of polling attempts for async requests (default: 60)
        poll_interval: Seconds to wait between polling attempts (default: 5)

    Returns:
        Dictionary or list of dictionaries containing parsed scrape results.
        For async requests, polls until results are available.

    Raises:
        ValueError: If URL is empty or invalid
        TypeError: If URL type is invalid
        RuntimeError: If async polling fails or times out
    """
    # Validate input
    if not url:
        raise ValueError("URL cannot be empty")

    if not isinstance(url, (str, list)):
        raise TypeError("URL must be a string or list of strings")

    try:
        # Build scrape parameters
        scrape_params: Dict[str, Any] = {}

        if country:
            scrape_params["country"] = country.lower()
        if data_format:
            scrape_params["data_format"] = data_format
        if format_param:
            scrape_params["format"] = format_param
        if method:
            scrape_params["method"] = method
        if async_request:
            scrape_params["async_request"] = True

        # Perform the scrape
        url_count = len(url) if isinstance(url, list) else 1
        request_type = "async" if async_request else "sync"
        log.info(
            f"Executing Bright Data scrape ({request_type}) for {url_count} URL"
            f"{'s' if url_count > 1 else ''}"
        )

        results = client.scrape(url, **scrape_params)

        # Handle async requests - check if snapshot_id is returned
        if async_request:
            snapshot_ids = _extract_snapshot_ids(results, url_count)

            if snapshot_ids:
                valid_snapshot_ids = [
                    sid
                    for sid in snapshot_ids
                    if isinstance(sid, str)
                    and len(sid) < 200
                    and not sid.strip().startswith("<")
                ]

                if valid_snapshot_ids:
                    results = _poll_snapshots(
                        client, valid_snapshot_ids, max_poll_attempts, poll_interval
                    )

        # Parse the content to extract structured data
        if isinstance(results, list) and len(results) > 0:
            parsed_results: List[Dict[str, Any]] = []
            for result in results:
                if not result or (isinstance(result, str) and len(result.strip()) == 0):
                    continue

                parsed = client.parse_content(
                    result,
                    extract_text=True,
                    extract_links=True,
                    extract_images=True,
                )
                if isinstance(parsed, list):
                    parsed_results.extend(parsed)
                else:
                    parsed_results.append(parsed)
        else:
            parsed_results = client.parse_content(
                results,
                extract_text=True,
                extract_links=True,
                extract_images=True,
            )
            if not isinstance(parsed_results, list):
                parsed_results = [parsed_results]

        result_count = len(parsed_results)
        log.info(f"Scrape completed successfully. Retrieved {result_count} result(s)")

        return parsed_results

    except ValueError as e:
        log.info(f"Validation error in Bright Data scrape: {str(e)}")
        raise
    except TypeError as e:
        log.info(f"Type error in Bright Data scrape: {str(e)}")
        raise
    except Exception as e:
        log.info(f"Error performing Bright Data scrape: {str(e)}")
        raise


def _extract_snapshot_ids(
    results: Union[Dict[str, Any], List[Dict[str, Any]], str], _url_count: int
) -> List[str]:
    """
    Extract snapshot_id(s) from async scrape response.

    Args:
        results: Response from client.scrape() with async_request=True
        _url_count: Number of URLs that were scraped (unused, kept for API consistency)

    Returns:
        List of valid snapshot_id strings (filtered to exclude HTML/content)
    """
    snapshot_ids = []

    def is_valid_snapshot_id(sid: str) -> bool:
        """Check if a string is a valid snapshot_id (not HTML content)."""
        if not isinstance(sid, str) or not sid.strip():
            return False
        if len(sid) > 200:
            return False
        if sid.strip().startswith("<") or sid.strip().startswith("<!"):
            return False
        if "<html" in sid.lower() or "<body" in sid.lower():
            return False
        return True

    if isinstance(results, list):
        for result in results:
            if isinstance(result, dict):
                snapshot_id = result.get("snapshot_id")
                if snapshot_id and is_valid_snapshot_id(str(snapshot_id)):
                    snapshot_ids.append(str(snapshot_id))
            elif isinstance(result, str):
                if is_valid_snapshot_id(result):
                    snapshot_ids.append(result)
    elif isinstance(results, dict):
        snapshot_id = results.get("snapshot_id")
        if snapshot_id and is_valid_snapshot_id(str(snapshot_id)):
            snapshot_ids.append(str(snapshot_id))
    elif isinstance(results, str):
        if is_valid_snapshot_id(results):
            snapshot_ids.append(results)

    return snapshot_ids


def _poll_snapshots(
    client: bdclient,
    snapshot_ids: List[str],
    max_attempts: int = 20,
    poll_interval: int = 5,
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Poll for snapshot results using download_snapshot().

    Checks snapshot status (running, ready, failed) and handles each state appropriately.
    Status enums: "running" (continue polling), "ready" (download data), "failed" (raise error).

    Args:
        client: Initialized Bright Data client
        snapshot_ids: List of snapshot_id strings to poll
        max_attempts: Maximum number of polling attempts per snapshot
        poll_interval: Seconds to wait between polling attempts

    Returns:
        Dictionary or list of dictionaries containing scrape results

    Raises:
        RuntimeError: If polling fails, times out, or snapshot status is "failed"
    """
    results = []
    completed_snapshots = set()
    failed_snapshots = []

    for attempt in range(max_attempts):
        if len(completed_snapshots) == len(snapshot_ids):
            break

        for snapshot_id in snapshot_ids:
            if snapshot_id in completed_snapshots or snapshot_id in failed_snapshots:
                continue

            try:
                snapshot_response = client.download_snapshot(snapshot_id)

                status = None
                snapshot_data = None

                if isinstance(snapshot_response, dict):
                    status = snapshot_response.get("status", "").lower()
                    if status == "ready":
                        snapshot_data = (
                            snapshot_response.get("data") or snapshot_response
                        )
                    elif status == "failed":
                        error_msg = snapshot_response.get("error", "Unknown error")
                        failed_snapshots.append(snapshot_id)
                        log.info(
                            f"Snapshot {snapshot_id[:8]}... status: failed - {error_msg}"
                        )
                        continue
                    elif status == "running":
                        log.info(
                            f"Snapshot {snapshot_id[:8]}... status: running (attempt {attempt + 1}/{max_attempts})"
                        )
                        continue
                elif snapshot_response:
                    snapshot_data = snapshot_response
                    status = "ready"

                if snapshot_data:
                    results.append(snapshot_data)
                    completed_snapshots.add(snapshot_id)
                    log.info(
                        f"Snapshot {snapshot_id[:8]}... status: ready - completed ({len(completed_snapshots)}/{len(snapshot_ids)})"
                    )
                else:
                    log.info(
                        f"Snapshot {snapshot_id[:8]}... still processing (attempt {attempt + 1}/{max_attempts})"
                    )

            except (ValueError, RuntimeError, KeyError) as e:
                error_msg = str(e).lower()

                if "failed" in error_msg:
                    failed_snapshots.append(snapshot_id)
                    log.info(f"Snapshot {snapshot_id[:8]}... failed: {str(e)}")
                elif (
                    "not ready" in error_msg
                    or "not found" in error_msg
                    or "pending" in error_msg
                    or "running" in error_msg
                ):
                    log.info(
                        f"Snapshot {snapshot_id[:8]}... status: running (attempt {attempt + 1}/{max_attempts})"
                    )
                else:
                    log.info(f"Error polling snapshot {snapshot_id[:8]}...: {str(e)}")

        if len(completed_snapshots) < len(snapshot_ids) and attempt < max_attempts - 1:
            time.sleep(poll_interval)

    if failed_snapshots:
        failed_list = ", ".join([sid[:8] + "..." for sid in failed_snapshots])
        raise RuntimeError(f"Snapshot(s) failed: {failed_list}")

    if len(completed_snapshots) < len(snapshot_ids):
        incomplete = len(snapshot_ids) - len(completed_snapshots)
        raise RuntimeError(
            f"Timeout: {incomplete} snapshot(s) did not complete after {max_attempts} attempts"
        )

    if len(results) == 1:
        return results[0]
    return results
