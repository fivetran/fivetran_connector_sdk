"""Bright Data SERP (search) helper functions."""

import time
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote_plus

import requests
from requests import RequestException

from fivetran_connector_sdk import Logging as log

from .common import (
    BRIGHT_DATA_BASE_URL,
    DEFAULT_SERP_ZONE,
    DEFAULT_TIMEOUT_SECONDS,
    RETRY_STATUS_CODES,
    extract_error_detail,
    parse_response_payload,
)


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
    """Perform a search using Bright Data's SERP REST endpoint."""
    if not api_token or not isinstance(api_token, str):
        raise ValueError("A valid Bright Data API token is required")

    if not query:
        raise ValueError("Query cannot be empty")

    if not isinstance(query, (str, list)):
        raise TypeError("Query must be a string or list of strings")

    queries: List[str]
    if isinstance(query, list):
        if not all(isinstance(item, str) for item in query):
            raise TypeError("All queries must be strings")
        queries = [item.strip() for item in query if item and item.strip()]
    else:
        queries = [query.strip()]

    if not queries:
        raise ValueError("At least one non-empty query must be provided")

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
                    f"{BRIGHT_DATA_BASE_URL}/request",
                    headers=headers,
                    json=payload,
                    timeout=timeout,
                )

                if response.status_code == 200:
                    response_payload = parse_response_payload(response)
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

                error_detail = extract_error_detail(response)
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


def _normalize_search_results(payload: Any) -> List[Dict[str, Any]]:
    """Normalize the variety of SERP response structures into a list of dictionaries."""
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
