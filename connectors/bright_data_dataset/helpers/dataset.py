"""Bright Data Marketplace Dataset API helper functions."""

import json
import time
from typing import Any, Dict, List, Optional, Union

import requests
from requests import RequestException

from fivetran_connector_sdk import Logging as log

from .common import (
    BRIGHT_DATA_BASE_URL,
    DEFAULT_TIMEOUT_SECONDS,
    MAX_SNAPSHOT_WAIT_SECONDS,
    RETRY_STATUS_CODES,
    SNAPSHOT_POLL_INTERVAL_SECONDS,
    extract_error_detail,
    parse_response_payload,
)


def filter_dataset(
    api_token: str,
    dataset_id: str,
    filter_obj: Union[Dict[str, Any], str],
    records_limit: Optional[int] = None,
    size: Optional[int] = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    retries: int = 3,
    backoff_factor: float = 1.5,
) -> List[Dict[str, Any]]:
    """
    Filter a Bright Data dataset and retrieve the filtered records.

    This function creates a snapshot by filtering the dataset, waits for the snapshot
    to be ready, and then retrieves the filtered records.

    Args:
        api_token: Bright Data API token
        dataset_id: ID of the dataset to filter
        filter_obj: Filter object (dict or JSON string) containing filter criteria
        records_limit: Maximum number of records to include in the snapshot
        timeout: Request timeout in seconds
        retries: Number of retries for failed requests
        backoff_factor: Backoff factor for exponential backoff

    Returns:
        List of filtered dataset records

    Raises:
        ValueError: If API token, dataset_id, or filter is invalid
        RuntimeError: If snapshot creation or retrieval fails
    """
    if not api_token or not isinstance(api_token, str):
        raise ValueError("A valid Bright Data API token is required")

    if not dataset_id or not isinstance(dataset_id, str):
        raise ValueError("dataset_id must be a non-empty string")

    # Parse filter if it's a string
    if isinstance(filter_obj, str):
        try:
            filter_dict = json.loads(filter_obj)
        except json.JSONDecodeError as e:
            raise ValueError(f"filter must be valid JSON: {str(e)}") from e
    else:
        filter_dict = filter_obj

    if not isinstance(filter_dict, dict):
        raise ValueError("filter must be a dictionary or valid JSON string")

    # Create snapshot by filtering the dataset
    snapshot_id = _create_filtered_snapshot(
        api_token=api_token,
        dataset_id=dataset_id,
        filter_obj=filter_dict,
        records_limit=records_limit,
        size=size,
        timeout=timeout,
        retries=retries,
        backoff_factor=backoff_factor,
    )

    if not snapshot_id:
        raise RuntimeError("Failed to create filtered snapshot")

    # Wait for snapshot to be ready and retrieve records
    records = _wait_and_get_snapshot_content(
        api_token=api_token,
        snapshot_id=snapshot_id,
        timeout=timeout,
        retries=retries,
        backoff_factor=backoff_factor,
    )

    log.info(f"Retrieved {len(records)} records from filtered dataset snapshot")
    return records


def _create_filtered_snapshot(
    api_token: str,
    dataset_id: str,
    filter_obj: Dict[str, Any],
    records_limit: Optional[int],
    timeout: int,
    retries: int,
    backoff_factor: float,
) -> str:
    """Create a filtered snapshot and return the snapshot_id."""
    url = f"{BRIGHT_DATA_BASE_URL}/datasets/filter"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    payload: Dict[str, Any] = {
        "dataset_id": dataset_id,
        "filter": filter_obj,
    }

    if records_limit is not None:
        payload["records_limit"] = records_limit

    attempt = 0
    backoff = backoff_factor

    while attempt <= retries:
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=timeout,
            )

            if response.status_code == 200:
                result = parse_response_payload(response)
                if isinstance(result, dict):
                    snapshot_id = result.get("snapshot_id")
                    if snapshot_id:
                        log.info(f"Created filtered snapshot: {snapshot_id[:8]}...")
                        return str(snapshot_id)
                    raise RuntimeError("Response missing snapshot_id")

            if response.status_code in RETRY_STATUS_CODES and attempt < retries:
                log.info(
                    f"Bright Data filter request retry {attempt + 1}/{retries} "
                    f"(status code: {response.status_code})"
                )
                attempt += 1
                time.sleep(backoff)
                backoff *= backoff_factor
                continue

            error_detail = extract_error_detail(response)
            log.info(f"Bright Data filter request failed: {error_detail}")
            response.raise_for_status()

        except RequestException as exc:
            if attempt < retries:
                log.info(
                    f"Error contacting Bright Data API: {str(exc)}. "
                    f"Retrying ({attempt + 1}/{retries})"
                )
                attempt += 1
                time.sleep(backoff)
                backoff *= backoff_factor
                continue
            raise RuntimeError(
                f"Failed to create filtered snapshot after {retries} retries: {str(exc)}"
            ) from exc

    raise RuntimeError("Failed to create filtered snapshot after retries")


def _wait_and_get_snapshot_content(
    api_token: str,
    snapshot_id: str,
    timeout: int,
    retries: int,
    backoff_factor: float,
) -> List[Dict[str, Any]]:
    """Wait for snapshot to be ready and retrieve its content."""
    max_attempts = MAX_SNAPSHOT_WAIT_SECONDS // SNAPSHOT_POLL_INTERVAL_SECONDS
    attempt = 0

    while attempt < max_attempts:
        try:
            # Check snapshot status
            status_response = _get_snapshot_metadata(
                api_token=api_token,
                snapshot_id=snapshot_id,
                timeout=timeout,
                retries=retries,
                backoff_factor=backoff_factor,
            )

            if isinstance(status_response, dict):
                status = status_response.get("status", "").lower()
                if status == "ready":
                    # Snapshot is ready, get the content
                    return _get_snapshot_content(
                        api_token=api_token,
                        snapshot_id=snapshot_id,
                        timeout=timeout,
                        retries=retries,
                        backoff_factor=backoff_factor,
                    )
                elif status == "failed":
                    error_msg = status_response.get("error", "Unknown error")
                    raise RuntimeError(
                        f"Snapshot {snapshot_id[:8]}... failed: {error_msg}"
                    )
                elif status in ("running", "pending", "processing"):
                    log.info(
                        f"Snapshot {snapshot_id[:8]}... status: {status} "
                        f"(attempt {attempt + 1}/{max_attempts})"
                    )
                else:
                    log.info(
                        f"Snapshot {snapshot_id[:8]}... status: {status} "
                        f"(attempt {attempt + 1}/{max_attempts})"
                    )

            attempt += 1
            if attempt < max_attempts:
                time.sleep(SNAPSHOT_POLL_INTERVAL_SECONDS)

        except (ValueError, RuntimeError, KeyError) as e:
            error_msg = str(e).lower()
            if "failed" in error_msg:
                raise
            log.info(
                f"Error checking snapshot status: {str(e)}. "
                f"Retrying (attempt {attempt + 1}/{max_attempts})"
            )
            attempt += 1
            if attempt < max_attempts:
                time.sleep(SNAPSHOT_POLL_INTERVAL_SECONDS)

    raise RuntimeError(
        f"Snapshot {snapshot_id[:8]}... did not complete within "
        f"{MAX_SNAPSHOT_WAIT_SECONDS} seconds"
    )


def _get_snapshot_metadata(
    api_token: str,
    snapshot_id: str,
    timeout: int,
    retries: int,
    backoff_factor: float,
) -> Dict[str, Any]:
    """Get snapshot metadata to check its status."""
    url = f"{BRIGHT_DATA_BASE_URL}/datasets/snapshots/{snapshot_id}"
    headers = {
        "Authorization": f"Bearer {api_token}",
    }

    attempt = 0
    backoff = backoff_factor

    while attempt <= retries:
        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=timeout,
            )

            if response.status_code == 200:
                return parse_response_payload(response)

            if response.status_code in RETRY_STATUS_CODES and attempt < retries:
                log.info(
                    f"Bright Data snapshot metadata request retry {attempt + 1}/{retries} "
                    f"(status code: {response.status_code})"
                )
                attempt += 1
                time.sleep(backoff)
                backoff *= backoff_factor
                continue

            error_detail = extract_error_detail(response)
            log.info(f"Bright Data snapshot metadata request failed: {error_detail}")
            response.raise_for_status()

        except RequestException as exc:
            if attempt < retries:
                log.info(
                    f"Error contacting Bright Data API: {str(exc)}. "
                    f"Retrying ({attempt + 1}/{retries})"
                )
                attempt += 1
                time.sleep(backoff)
                backoff *= backoff_factor
                continue
            raise RuntimeError(
                f"Failed to get snapshot metadata after {retries} retries: {str(exc)}"
            ) from exc

    raise RuntimeError("Failed to get snapshot metadata after retries")


def _get_snapshot_content(
    api_token: str,
    snapshot_id: str,
    timeout: int,
    retries: int,
    backoff_factor: float,
) -> List[Dict[str, Any]]:
    """Get snapshot content (the filtered records)."""
    url = f"{BRIGHT_DATA_BASE_URL}/datasets/snapshots/{snapshot_id}/content"
    headers = {
        "Authorization": f"Bearer {api_token}",
    }

    attempt = 0
    backoff = backoff_factor

    while attempt <= retries:
        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=timeout,
            )

            if response.status_code == 200:
                content = parse_response_payload(response)
                # Handle different response formats
                if isinstance(content, list):
                    return content
                elif isinstance(content, dict):
                    # Check if records are nested in a 'data' or 'records' key
                    if "data" in content and isinstance(content["data"], list):
                        return content["data"]
                    elif "records" in content and isinstance(content["records"], list):
                        return content["records"]
                    elif "results" in content and isinstance(content["results"], list):
                        return content["results"]
                    # If the entire dict is a single record, wrap it in a list
                    return [content]
                else:
                    # If content is not a list or dict, wrap it
                    return [{"content": content}]

            if response.status_code in RETRY_STATUS_CODES and attempt < retries:
                log.info(
                    f"Bright Data snapshot content request retry {attempt + 1}/{retries} "
                    f"(status code: {response.status_code})"
                )
                attempt += 1
                time.sleep(backoff)
                backoff *= backoff_factor
                continue

            error_detail = extract_error_detail(response)
            log.info(f"Bright Data snapshot content request failed: {error_detail}")
            response.raise_for_status()

        except RequestException as exc:
            if attempt < retries:
                log.info(
                    f"Error contacting Bright Data API: {str(exc)}. "
                    f"Retrying ({attempt + 1}/{retries})"
                )
                attempt += 1
                time.sleep(backoff)
                backoff *= backoff_factor
                continue
            raise RuntimeError(
                f"Failed to get snapshot content after {retries} retries: {str(exc)}"
            ) from exc

    raise RuntimeError("Failed to get snapshot content after retries")
