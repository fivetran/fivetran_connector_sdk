"""Bright Data Web Scraper helper functions."""

# For parsing JSON responses from the Bright Data API
import json

# For adding delays between retry attempts (exponential backoff)
import time

# For type hints in function signatures
from typing import Any, Dict, List, Union

# For making HTTP requests to the Bright Data API
import requests

# For catching network and HTTP request exceptions
from requests import RequestException

# For logging connector operations, errors, and status updates
from fivetran_connector_sdk import Logging as log

# Shared constants and utilities from the common module
from .common import (
    BRIGHT_DATA_BASE_URL,
    DEFAULT_TIMEOUT_SECONDS,
    RETRY_STATUS_CODES,
    extract_error_detail,
    parse_response_payload,
)

MAX_RESPONSE_SIZE_BYTES = 100 * 1024 * 1024
CHUNK_SIZE_BYTES = 8192


def _parse_large_json_array_streaming(
    response, max_size_bytes: int = MAX_RESPONSE_SIZE_BYTES
):
    """
    Parse a large JSON array response using chunked reading to manage memory.

    For very large responses (>100MB), this reads the response in chunks and parses
    incrementally. However, JSON arrays still require the full response for parsing.

    Note: For truly large datasets, JSONL format is recommended as it allows
    line-by-line streaming without loading the entire response.

    Args:
        response: requests.Response object to parse
        max_size_bytes: Threshold for using chunked reading (default 100MB)

    Returns:
        List of parsed JSON objects, or None if not applicable/parse failed
    """
    content_length = response.headers.get("Content-Length")

    # Only attempt chunked parsing for responses larger than threshold
    if not content_length or int(content_length) < max_size_bytes:
        return None

    try:
        # Read response in chunks to monitor progress and manage memory pressure
        buffer = ""
        chunk_count = 0
        for chunk in response.iter_content(
            chunk_size=CHUNK_SIZE_BYTES, decode_unicode=True
        ):
            if chunk:
                buffer += chunk
                chunk_count += 1
                # Log progress for very large responses
                if chunk_count % 1000 == 0:
                    log.info(
                        f"Reading large response: {len(buffer)} bytes read "
                        f"({chunk_count} chunks)"
                    )

        # Verify it looks like a JSON array
        buffer_stripped = buffer.strip()
        if not buffer_stripped.startswith("["):
            # Not a JSON array, fall back to standard parsing
            return None

        # Parse the accumulated JSON array
        # Note: We still need to load it all for JSON array parsing
        # JSONL format would be better for true streaming
        parsed_data = json.loads(buffer_stripped)

        if isinstance(parsed_data, list):
            log.info(
                f"Successfully parsed large JSON array with {len(parsed_data)} records "
                f"({len(buffer)} bytes)"
            )
            return parsed_data

    except (json.JSONDecodeError, ValueError, MemoryError) as e:
        log.warning(
            f"Failed to parse large JSON response: {str(e)}. "
            f"Consider using JSONL format for very large datasets."
        )
        return None
    except Exception as e:
        log.info(
            f"Chunked parse encountered error: {str(e)}, falling back to standard parsing"
        )
        return None

    return None


def perform_scrape(
    api_token: str,
    dataset_id: str,
    url: Union[str, List[str]],
    poll_interval: int = 30,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    retries: int = 3,
    backoff_factor: float = 1.5,
    extra_query_params: Dict[str, Any] = None,
) -> List[Dict[str, Any]]:
    """
    Scrape URLs using Bright Data's Web Scraper REST API.

    This function triggers a scrape job via POST /datasets/v3/trigger, receives a snapshot_id,
    and polls GET /datasets/v3/snapshot/{snapshot_id} until the status is "ready" or "failed".
    The endpoint handles polling internally, so we poll indefinitely until we receive a ready/failed status.

    Args:
        api_token: Bright Data API token
        dataset_id: ID of the dataset to use for scraping
        url: Single URL or list of URLs to scrape
        poll_interval: Interval between polling attempts in seconds
        timeout: Request timeout in seconds
        retries: Number of retries for failed requests
        backoff_factor: Backoff factor for exponential backoff
        extra_query_params: Optional dictionary of additional query parameters to include
                          in the scrape trigger request (e.g., {"discover_by": "profile_url", "type": "discover_new"})

    Returns:
        List of scraped results (each result is a dictionary)

    Raises:
        ValueError: If API token, dataset_id, or URL is invalid
        RuntimeError: If scrape trigger or polling fails
    """
    if not api_token or not isinstance(api_token, str):
        raise ValueError("A valid Bright Data API token is required")

    if not dataset_id or not isinstance(dataset_id, str):
        raise ValueError("dataset_id must be a non-empty string")

    if not url:
        raise ValueError("URL cannot be empty")

    if not isinstance(url, (str, list)):
        raise TypeError("URL must be a string or list of strings")

    # Normalize URLs to list
    urls: List[str]
    if isinstance(url, list):
        urls = [item.strip() for item in url if isinstance(item, str) and item.strip()]
    else:
        urls = [url.strip()]

    if not urls:
        raise ValueError("At least one non-empty URL must be provided")

    # Build payload for trigger request
    # Convert list of URL strings to API-required format:
    # [{"url": "https://example.com/1"}, {"url": "https://example.com/2"}, ...]
    # This matches the Bright Data API endpoint structure which accepts a list of objects
    payload = [{"url": single_url} for single_url in urls]

    # Trigger scrape job
    snapshot_id = _trigger_scrape(
        api_token=api_token,
        dataset_id=dataset_id,
        payload=payload,
        timeout=timeout,
        retries=retries,
        backoff_factor=backoff_factor,
        extra_query_params=extra_query_params or {},
    )

    if not snapshot_id:
        raise RuntimeError("Failed to trigger scrape job - no snapshot_id returned")

    # Poll snapshot until ready or failed
    results = _poll_snapshot(
        api_token=api_token,
        snapshot_id=snapshot_id,
        poll_interval=poll_interval,
        timeout=timeout,
    )

    # Normalize results to always be a list
    if not isinstance(results, list):
        results = [results] if results else []

    result_count = len(results)
    log.info(f"Scrape completed successfully. Retrieved {result_count} result(s)")

    return results


def _trigger_scrape(
    api_token: str,
    dataset_id: str,
    payload: List[Dict[str, Any]],
    timeout: int,
    retries: int,
    backoff_factor: float,
    extra_query_params: Dict[str, Any] = None,
) -> str:
    """
    Trigger a scrape job via POST /datasets/v3/trigger.

    Returns:
        snapshot_id string from the response

    Raises:
        ValueError: For 400/422 errors (invalid input)
        RuntimeError: For other API errors or network failures
    """
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    # Build query parameters
    # According to Bright Data API docs, trigger endpoint accepts:
    # dataset_id, format, and include_errors as query parameters
    params: Dict[str, Any] = {
        "dataset_id": dataset_id,
        "format": "json",
        "include_errors": "true",  # Include errors report with the results
    }

    # Add any additional query parameters from configuration
    # This allows dataset-specific parameters to be configured rather than hardcoded
    if extra_query_params:
        params.update(extra_query_params)

    # Build request body - according to Bright Data API docs, body should be:
    # [{"url": "https://..."}, {"url": "https://..."}, ...]
    # The API documentation shows that the body should only contain URL objects
    # as shown in the API examples
    body_payload = payload.copy()

    url_count = len(payload)
    log.info(
        f"Triggering Bright Data scrape for {url_count} URL{'s' if url_count > 1 else ''} "
        f"using dataset_id: {dataset_id}"
    )

    attempt = 0
    backoff = backoff_factor

    while attempt <= retries:
        try:
            # Log request details for debugging (without sensitive data)
            log.info(
                f"Sending POST request to {BRIGHT_DATA_BASE_URL}/datasets/v3/trigger "
                f"with params: {params}, payload containing {url_count} URL(s)"
            )

            response = requests.post(
                f"{BRIGHT_DATA_BASE_URL}/datasets/v3/trigger",
                headers=headers,
                params=params,
                json=body_payload,
                timeout=timeout,
            )

            if response.status_code == 200:
                response_data = parse_response_payload(response)
                if isinstance(response_data, dict):
                    snapshot_id = response_data.get("snapshot_id")
                    if snapshot_id:
                        log.info(
                            f"Scrape job triggered successfully. Snapshot ID: {snapshot_id[:8]}..."
                        )
                        return str(snapshot_id)
                    raise RuntimeError("Trigger response missing snapshot_id field")
                raise RuntimeError(
                    f"Unexpected trigger response format: {response_data}"
                )

            # Handle 400/422 as ValueError (invalid input)
            if response.status_code in (400, 422):
                error_detail = extract_error_detail(response)
                # Log the full response for debugging
                try:
                    response_json = response.json()
                    log.info(
                        f"Invalid scrape request (status {response.status_code}): {error_detail}. "
                        f"Request URL: {response.url}, "
                        f"Request payload (first URL): {body_payload[0] if body_payload else 'empty'}, "
                        f"Full response: {response_json}"
                    )
                except ValueError:
                    log.info(
                        f"Invalid scrape request (status {response.status_code}): {error_detail}. "
                        f"Request URL: {response.url}, "
                        f"Request payload (first URL): {body_payload[0] if body_payload else 'empty'}, "
                        f"Response text: {response.text[:500]}"
                    )
                raise ValueError(f"Invalid scrape request: {error_detail}")

            # Retry on certain status codes
            if response.status_code in RETRY_STATUS_CODES and attempt < retries:
                log.info(
                    f"Bright Data scrape trigger retry {attempt + 1}/{retries} "
                    f"(status code: {response.status_code})"
                )
                attempt += 1
                time.sleep(backoff)
                backoff *= backoff_factor
                continue

            # For other errors, raise with details
            error_detail = extract_error_detail(response)
            log.info(
                f"Bright Data scrape trigger failed (status {response.status_code}): "
                f"{error_detail}"
            )
            response.raise_for_status()

        except RequestException as exc:
            if attempt < retries:
                log.info(
                    f"Error contacting Bright Data Scraper API: {str(exc)}. "
                    f"Retrying ({attempt + 1}/{retries})"
                )
                attempt += 1
                time.sleep(backoff)
                backoff *= backoff_factor
                continue
            raise RuntimeError(
                f"Failed to trigger Bright Data scrape after {retries} retries: {str(exc)}"
            ) from exc

    raise RuntimeError("Failed to trigger Bright Data scrape after retries")


def _poll_snapshot(
    api_token: str,
    snapshot_id: str,
    poll_interval: int,
    timeout: int,
    max_attempts: int = 1000,
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Poll the snapshot endpoint until status is "ready" or "failed".

    According to API docs: GET /datasets/v3/snapshot/{snapshot_id}?format=json

    Args:
        api_token: Bright Data API token
        snapshot_id: Snapshot ID to poll
        poll_interval: Interval between polling attempts in seconds
        timeout: Request timeout in seconds
        max_attempts: Maximum number of polling attempts before raising an error (default: 1000)

    Returns:
        Snapshot data when ready (dict or list)

    Raises:
        RuntimeError: If snapshot fails or request errors occur, or if max_attempts is exceeded
    """
    headers = {
        "Authorization": f"Bearer {api_token}",
    }

    params: Dict[str, Any] = {
        "format": "json",
        "include_errors": "true",
    }

    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            response = requests.get(
                f"{BRIGHT_DATA_BASE_URL}/datasets/v3/snapshot/{snapshot_id}",
                headers=headers,
                params=params,
                timeout=timeout,
            )

            if response.status_code == 200:
                # According to Bright Data API, when ready the response can be:
                # - An array of objects: [{"id": "...", ...}, ...] (Content-Type: application/json)
                # - JSON Lines format: {"id":"..."}\n{"id":"..."}\n... (Content-Type: application/jsonl)
                # - Single JSON object as string
                content_type = response.headers.get("Content-Type", "").lower()

                # Check if it's JSON Lines format (application/jsonl)
                # Use streaming for JSONL to handle large responses without loading entire body into memory
                if "jsonl" in content_type or "json-lines" in content_type:
                    # Parse JSON Lines format using streaming - each line is a separate JSON object
                    results = []
                    parse_errors = []
                    for line_num, line in enumerate(
                        response.iter_lines(decode_unicode=True), 1
                    ):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            parsed_obj = json.loads(line)
                            results.append(parsed_obj)
                        except json.JSONDecodeError as e:
                            # Collect parse errors to log once after the loop
                            parse_errors.append((line_num, str(e), line[:200]))
                            # Continue with other lines even if one fails
                    if parse_errors:
                        log.warning(
                            f"Snapshot {snapshot_id[:8]}... JSONL parse errors on {len(parse_errors)} line(s). "
                            f"First error: line {parse_errors[0][0]}: {parse_errors[0][1]}"
                        )
                    if results:
                        log.info(
                            f"Snapshot {snapshot_id[:8]}... ready (JSONL format with {len(results)} records) "
                            f"after {attempt} attempt(s)"
                        )
                        return results

                # For non-JSONL responses, check content length before loading into memory
                # Set a reasonable maximum size (e.g., 100MB) to prevent memory issues
                content_length = response.headers.get("Content-Length")
                max_size_bytes = MAX_RESPONSE_SIZE_BYTES
                if content_length and int(content_length) > max_size_bytes:
                    log.warning(
                        f"Snapshot {snapshot_id[:8]}... response size ({content_length} bytes) exceeds "
                        f"maximum recommended size ({max_size_bytes} bytes). "
                        f"Consider using JSONL format for large datasets."
                    )

                # Log response details only on first attempt
                if attempt == 1:
                    size_info = (
                        f", size: {content_length} bytes" if content_length else ""
                    )
                    log.info(
                        f"Snapshot {snapshot_id[:8]}... poll response "
                        f"Content-Type: {content_type or 'unknown'}{size_info}"
                    )

                # For very large responses, try chunked parsing for JSON arrays
                # This reads in chunks and provides progress logging
                if content_length and int(content_length) > max_size_bytes:
                    chunked_data = _parse_large_json_array_streaming(
                        response, max_size_bytes
                    )
                    if chunked_data is not None:
                        log.info(
                            f"Snapshot {snapshot_id[:8]}... ready (chunked parse with {len(chunked_data)} records) "
                            f"after {attempt} attempt(s)"
                        )
                        return chunked_data
                    # If chunked parsing failed, continue with standard parsing

                # For smaller responses or when chunked parsing is not applicable,
                # use standard parsing
                raw_text = response.text

                # Try to parse as standard JSON (array or single object)
                try:
                    # Try requests' built-in JSON parser first
                    response_data = response.json()
                except (ValueError, json.JSONDecodeError):
                    # If that fails, try manual parsing of the raw text
                    try:
                        response_data = json.loads(raw_text)
                    except (json.JSONDecodeError, ValueError):
                        # If both fail, use raw text for further processing
                        response_data = raw_text

                # If response is a list (array of objects), snapshot is ready
                if isinstance(response_data, list):
                    log.info(
                        f"Snapshot {snapshot_id[:8]}... ready (array response with {len(response_data)} records) "
                        f"after {attempt} attempt(s)"
                    )
                    return response_data

                # If response is a string, try to parse it as JSON
                if isinstance(response_data, str):
                    response_lower = response_data.lower()
                    # Check if it's a "not ready" status message
                    if any(
                        phrase in response_lower
                        for phrase in (
                            "not ready",
                            "processing",
                            "pending",
                            "try again",
                        )
                    ):
                        log.info(
                            f"Snapshot {snapshot_id[:8]}... still processing: {response_data[:200]}... "
                            f"(attempt {attempt})"
                        )
                        # Continue to sleep and retry
                    else:
                        # Try parsing as JSON - check if string looks like JSON (starts with { or [)
                        trimmed_text = response_data.strip()
                        if trimmed_text.startswith(("{", "[")):
                            try:
                                parsed_data = json.loads(response_data)
                                # If parsed successfully, it's data - return it
                                log.info(
                                    f"Snapshot {snapshot_id[:8]}... ready (parsed JSON string response, "
                                    f"{len(parsed_data) if isinstance(parsed_data, list) else 1} records) "
                                    f"after {attempt} attempt(s)"
                                )
                                # Normalize to list format
                                if isinstance(parsed_data, list):
                                    return parsed_data
                                elif isinstance(parsed_data, dict):
                                    return [parsed_data]
                                else:
                                    return [parsed_data] if parsed_data else []
                            except json.JSONDecodeError as e:
                                # JSON parsing failed - might be JSONL format or truncated
                                error_pos = getattr(e, "pos", None)
                                error_msg = str(e)
                                # Check if error indicates multiple objects (JSONL format)
                                if "Extra data" in error_msg or error_pos:
                                    # Try parsing as JSONL format (line-by-line)
                                    try:
                                        results = []
                                        # Process line-by-line to avoid memory issues
                                        for line in raw_text.split("\n"):
                                            line = line.strip()
                                            if not line:
                                                continue
                                            try:
                                                parsed_obj = json.loads(line)
                                                results.append(parsed_obj)
                                            except json.JSONDecodeError:
                                                # Skip invalid lines silently - errors logged above
                                                continue
                                        if results:
                                            log.info(
                                                f"Snapshot {snapshot_id[:8]}... ready (detected JSONL format "
                                                f"with {len(results)} records) after {attempt} attempt(s)"
                                            )
                                            return results
                                    except Exception as ex:
                                        log.info(
                                            f"Unexpected error while parsing JSONL lines for snapshot {snapshot_id[:8]}...: {ex}",
                                            exc_info=True,
                                        )

                                log.info(
                                    f"Snapshot {snapshot_id[:8]}... JSON parse error at position {error_pos}: {error_msg}. "
                                    f"Response length: {len(response_data)}. "
                                    f"First 500 chars: {response_data[:500]}..."
                                )
                                # If it looks like JSON but parsing failed, might be truncated or malformed
                                # Continue polling in case it's incomplete data
                        else:
                            # Doesn't look like JSON - might be a plain text status message
                            log.info(
                                f"Snapshot {snapshot_id[:8]}... string response (not JSON-like): "
                                f"{response_data[:200]}..."
                            )
                            # Continue polling in case it's just a status message

                # If response is a dict, check for status
                elif isinstance(response_data, dict):
                    status = response_data.get("status", "").lower()

                    if status == "ready":
                        # Extract data from response
                        # Data might be in "data", "records", "results" keys
                        snapshot_data = (
                            response_data.get("data") or response_data.get("records") or response_data.get("results") or response_data)

                        # If no data key, remove status/metadata fields and use remaining fields as data
                        if snapshot_data is None:
                            metadata_keys = (
                                "status",
                                "id",
                                "snapshot_id",
                                "created",
                                "dataset_id",
                                "customer_id",
                                "cost",
                                "initiation_type",
                            )
                            snapshot_data = {
                                k: v
                                for k, v in response_data.items()
                                if k not in metadata_keys
                            }

                        # If still None, use entire response as data
                        if snapshot_data is None:
                            snapshot_data = response_data

                        log.info(
                            f"Snapshot {snapshot_id[:8]}... ready after {attempt} attempt(s)"
                        )

                        # Ensure we return a list
                        if isinstance(snapshot_data, list):
                            return snapshot_data
                        elif isinstance(snapshot_data, dict):
                            # If single dict, wrap in list
                            return [snapshot_data]
                        else:
                            # Other types, wrap in list
                            return [snapshot_data] if snapshot_data is not None else []

                    elif status == "failed":
                        # Extract error message
                        error_msg = None
                        for key in ("error", "warning", "message", "detail", "details"):
                            if key in response_data:
                                error_value = response_data[key]
                                if error_value:
                                    error_msg = str(error_value)
                                    break

                        if not error_msg:
                            error_msg = "Unknown error"

                        log.info(f"Snapshot {snapshot_id[:8]}... failed: {error_msg}")
                        raise RuntimeError(
                            f"Snapshot {snapshot_id[:8]}... failed: {error_msg}"
                        )

                    elif status in ("running", "pending", "processing", "scheduled"):
                        # Log status every 5 attempts to reduce log volume
                        if attempt % 5 == 0:
                            log.info(
                                f"Snapshot {snapshot_id[:8]}... status: {status} "
                                f"(attempt {attempt})"
                            )
                    else:
                        # Dict response but no status field or unknown status
                        # Might be data wrapped in a dict - check if it looks like data
                        if "status" not in response_data:
                            # Log once when treating as data
                            if attempt == 1:
                                log.info(
                                    f"Snapshot {snapshot_id[:8]}... dict response without status, "
                                    f"treating as data"
                                )
                            # Could be data - return it wrapped in list
                            return [response_data] if response_data else []
                        else:
                            # Log unknown status every 5 attempts
                            if attempt % 5 == 0:
                                log.info(
                                    f"Snapshot {snapshot_id[:8]}... status: {status} "
                                    f"(attempt {attempt})"
                                )

            elif response.status_code == 404:
                error_msg = f"Snapshot {snapshot_id[:8]}... not found"
                log.info(error_msg)
                raise RuntimeError(error_msg)

            else:
                error_detail = extract_error_detail(response)
                log.info(
                    f"Error polling snapshot {snapshot_id[:8]}... "
                    f"(status {response.status_code}): {error_detail}"
                )
                response.raise_for_status()

        except RequestException as exc:
            # Log network errors every 5 attempts to reduce log volume
            if attempt % 5 == 0:
                log.info(
                    f"Error polling snapshot {snapshot_id[:8]}...: {str(exc)}. "
                    f"Retrying (attempt {attempt})"
                )

        # Wait before next attempt
        # until we receive a ready/failed status or an error occurs
        time.sleep(poll_interval)

    raise RuntimeError(
        f"Snapshot {snapshot_id[:8]} polling timeed out after {max_attempts} attempts"
    )
