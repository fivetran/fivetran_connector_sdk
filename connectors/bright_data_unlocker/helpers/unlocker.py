"""Bright Data Web Unlocker helper functions."""

import json
from typing import Any, Dict, List, Optional, Union

import requests
from requests import RequestException

from fivetran_connector_sdk import Logging as log

from .common import (
    BRIGHT_DATA_BASE_URL,
    DEFAULT_TIMEOUT_SECONDS,
    DEFAULT_UNLOCKER_ZONE,
    RETRY_STATUS_CODES,
    extract_error_detail,
    parse_response_payload,
)


def perform_web_unlocker(
    api_token: str,
    url: Union[str, List[str]],
    zone: Optional[str] = DEFAULT_UNLOCKER_ZONE,
    country: Optional[str] = "us",
    method: Optional[str] = "GET",
    format_param: Optional[str] = "json",
    data_format: Optional[str] = "markdown",
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    retries: int = 3,
    backoff_factor: float = 1.5,
) -> List[Dict[str, Any]]:
    """
    Invoke Bright Data's Web Unlocker REST API.
    """
    if not api_token or not isinstance(api_token, str):
        raise ValueError("A valid Bright Data API token is required")

    if not url:
        raise ValueError("URL cannot be empty")

    if not isinstance(url, (str, list)):
        raise TypeError("URL must be a string or list of strings")

    urls: List[str]
    if isinstance(url, list):
        urls = [item.strip() for item in url if isinstance(item, str) and item.strip()]
    else:
        urls = [url.strip()]

    if not urls:
        raise ValueError("At least one non-empty URL must be provided")

    zone_identifier = zone
    aggregated_results: List[Dict[str, Any]] = []

    for single_url in urls:
        payload: Dict[str, Any] = {
            "zone": zone_identifier,
            "url": single_url,
            "format": format_param or "json",
        }

        if country:
            payload["country"] = country.lower()
        if method:
            payload["method"] = method
        if data_format:
            payload["data_format"] = data_format

        response_payload = _execute_unlocker_request(
            api_token=api_token,
            payload=payload,
            timeout=timeout,
            retries=retries,
            backoff_factor=backoff_factor,
        )

        normalized = _normalize_unlocker_result(response_payload, single_url)
        aggregated_results.extend(normalized)

    log.info(
        f"Unlocker completed successfully. Retrieved {len(aggregated_results)} result(s)"
    )
    return aggregated_results


def _execute_unlocker_request(
    api_token: str,
    payload: Dict[str, Any],
    timeout: int,
    retries: int,
    backoff_factor: float,
) -> Any:
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    attempt = 0
    backoff = backoff_factor

    while attempt <= retries:
        try:
            response = requests.post(
                f"{BRIGHT_DATA_BASE_URL}/request?async=true",
                headers=headers,
                json=payload,
                timeout=timeout,
            )

            if response.status_code == 200:
                return parse_response_payload(response)

            if response.status_code in RETRY_STATUS_CODES and attempt < retries:
                log.info(
                    f"Bright Data Unlocker request retry {attempt + 1}/{retries} "
                    f"for URL '{payload.get('url')}' (status code: {response.status_code})"
                )
                attempt += 1
                backoff *= backoff_factor
                continue

            error_detail = extract_error_detail(response)
            log.info(
                f"Bright Data Unlocker request failed for URL '{payload.get('url')}': "
                f"{error_detail}"
            )
            response.raise_for_status()

        except RequestException as exc:
            if attempt < retries:
                log.info(
                    f"Error contacting Bright Data Unlocker API for URL '{payload.get('url')}': "
                    f"{str(exc)}. Retrying ({attempt + 1}/{retries})"
                )
                attempt += 1
                backoff *= backoff_factor
                continue
            raise RuntimeError(
                f"Failed to execute Bright Data Unlocker request for URL "
                f"'{payload.get('url')}' after {retries} retries: {str(exc)}"
            ) from exc

    raise RuntimeError("Failed to trigger Bright Data Unlocker request after retries")


def _normalize_unlocker_result(payload: Any, source_url: str) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []

    if isinstance(payload, list):
        for item in payload:
            normalized.extend(_normalize_unlocker_result(item, source_url))
        return normalized

    if isinstance(payload, dict):
        result = payload.copy()
        result.setdefault("requested_url", source_url)
        return [result]

    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
            return _normalize_unlocker_result(parsed, source_url)
        except ValueError:
            pass
    return [{"requested_url": source_url, "raw_response": payload}]
