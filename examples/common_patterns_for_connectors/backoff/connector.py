"""
This example demonstrates six backoff strategies for handling 429 Too Many Requests responses.
The strategy is selected via the 'backoff_strategy' field in configuration.json.

Requires the fivetran-api-playground package to run:
  playground start --rate-limit --capacity 5

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

import json
import random
import time

import requests as rq
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

DEFAULT_API_BASE_URL = "http://127.0.0.1:5001"
PAGINATION_PATH = "/pagination/next_page_url"
MAX_RETRIES = 5
REQUEST_TIMEOUT_SECONDS = 10
FIXED_DELAY = 2
BASE_DELAY = 0.5
MAX_DELAY = 10
CHECKPOINT_INTERVAL = 50
BATCH_REQUESTS = 3

VALID_STRATEGIES = {
    "fixed",
    "linear",
    "exponential",
    "exponential_with_cap",
    "exponential_with_jitter",
    "retry_after",
}


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "user",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "email": "STRING",
                "address": "STRING",
                "company": "STRING",
                "job": "STRING",
                "updatedAt": "UTC_DATETIME",
                "createdAt": "UTC_DATETIME",
            },
        }
    ]


def validate_configuration(configuration: dict):
    """
    Validate that 'backoff_strategy' is present and one of the supported values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if the strategy is missing or unsupported.
    """
    strategy = configuration.get("backoff_strategy")
    if not strategy:
        raise ValueError("Missing required configuration value: 'backoff_strategy'")
    if strategy not in VALID_STRATEGIES:
        raise ValueError(
            f"Invalid backoff_strategy '{strategy}'. Must be one of: {sorted(VALID_STRATEGIES)}"
        )

    api_base_url = configuration.get("api_base_url", DEFAULT_API_BASE_URL)
    if not isinstance(api_base_url, str) or not api_base_url.strip():
        raise ValueError("Invalid configuration value: 'api_base_url' must be a non-empty string")


def get_base_url(configuration: dict) -> str:
    """Return the fully qualified pagination endpoint based on configuration."""
    api_base_url = configuration.get("api_base_url", DEFAULT_API_BASE_URL).rstrip("/")
    return f"{api_base_url}{PAGINATION_PATH}"


def compute_delay(strategy: str, attempt: int, retry_after_seconds: float = None) -> float:
    """
    Compute the delay in seconds before the next retry attempt.

    Strategies:
      fixed                  — constant delay regardless of attempt number
      linear                 — delay grows linearly with each attempt
      exponential            — delay doubles after each attempt
      exponential_with_cap   — exponential growth capped at MAX_DELAY
      exponential_with_jitter — randomised exponential to avoid thundering-herd
      retry_after            — honour the server's Retry-After header; fall back to exponential_with_cap

    Args:
        strategy: the backoff strategy name from configuration.
        attempt: 1-based retry attempt number.
        retry_after_seconds: value of the Retry-After response header, if present.
    Returns:
        Seconds to sleep before the next request.
    """
    if strategy == "fixed":
        return FIXED_DELAY

    if strategy == "linear":
        return BASE_DELAY * attempt

    if strategy == "exponential":
        return BASE_DELAY * (2**attempt)

    if strategy == "exponential_with_cap":
        return min(MAX_DELAY, BASE_DELAY * (2**attempt))

    if strategy == "exponential_with_jitter":
        return random.uniform(0, BASE_DELAY * (2**attempt))

    if strategy == "retry_after":
        if retry_after_seconds is not None:
            return retry_after_seconds
        # Fallback: exponential with cap when the header is absent
        return min(MAX_DELAY, BASE_DELAY * (2**attempt))

    raise ValueError(f"Unknown strategy: {strategy}")


def get_api_response(url: str, params: dict, strategy: str) -> dict:
    """
    Send a GET request and retry on 429 using the chosen backoff strategy.
    Args:
        url: the endpoint URL.
        params: query parameters for the request.
        strategy: the backoff strategy name.
    Returns:
        Parsed JSON response as a dictionary.
    Raises:
        Exception: when MAX_RETRIES is exceeded or a non-retryable HTTP error occurs.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        log.info(f"API call attempt {attempt}/{MAX_RETRIES}: {url}")
        try:
            response = rq.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        except rq.RequestException as exc:
            if attempt == MAX_RETRIES:
                raise Exception(
                    f"API request failed after {MAX_RETRIES} attempts for {url}: {exc}"
                ) from exc

            delay = compute_delay(strategy, attempt)
            log.warning(
                f"Request failed ({type(exc).__name__}): {exc}. "
                f"Strategy='{strategy}', attempt={attempt}, sleeping {delay:.2f}s"
            )
            time.sleep(delay)
            continue

        if response.status_code == 200:
            return response.json()

        if response.status_code == 429:
            retry_after = None
            retry_after_header = response.headers.get("Retry-After")
            if retry_after_header is not None:
                try:
                    retry_after = float(retry_after_header)
                except ValueError:
                    pass

            delay = compute_delay(strategy, attempt, retry_after)
            log.warning(
                f"Rate limited (429). Strategy='{strategy}', attempt={attempt}, "
                f"Retry-After header={retry_after_header}, sleeping {delay:.2f}s"
            )
            time.sleep(delay)
            continue

        response.raise_for_status()

    raise Exception(f"Exceeded {MAX_RETRIES} retries for {url}")


def sync_items(current_url: str, params: dict, state: dict, strategy: str):
    """
    Fetch all pages from the API, upsert rows, and checkpoint periodically.
    Args:
        current_url: starting endpoint URL.
        params: initial query parameters.
        state: connector state dict (modified in place).
        strategy: the backoff strategy name.
    """
    more_data = True
    rows_since_checkpoint = 0

    while more_data:
        for _ in range(BATCH_REQUESTS):
            response_page = get_api_response(current_url, params, strategy)

            items = response_page.get("data", [])
            if not items:
                more_data = False
                break

            log.info(f"Processing page with {len(items)} items")

            for user in items:
                op.upsert(table="user", data=user)
                state["last_updated_at"] = user["updatedAt"]
                rows_since_checkpoint += 1

                if rows_since_checkpoint >= CHECKPOINT_INTERVAL:
                    op.checkpoint(state)
                    log.info(f"Checkpoint saved at cursor: {state['last_updated_at']}")
                    rows_since_checkpoint = 0

            # Checkpoint at the end of every page as well
            op.checkpoint(state)
            log.info(f"Page complete. Cursor: {state['last_updated_at']}")
            rows_since_checkpoint = 0

            next_page_url = response_page.get("next_page_url")
            if next_page_url:
                current_url = next_page_url
                params = {}
            else:
                more_data = False
                break


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary containing state information from previous runs.
    """
    log.warning("Example: Common Patterns For Connectors - Backoff Strategies")

    validate_configuration(configuration)

    strategy = configuration["backoff_strategy"]
    log.info(f"Using backoff strategy: '{strategy}'")

    cursor = state.get("last_updated_at", "0001-01-01T00:00:00Z")
    params = {
        "order_by": "updatedAt",
        "order_type": "asc",
        "updated_since": cursor,
        "per_page": 50,
    }

    sync_items(get_base_url(configuration), params, state, strategy)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
