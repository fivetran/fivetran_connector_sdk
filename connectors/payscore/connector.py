"""PayScore connector for syncing data including income verification requests, screening groups, and reports.
This connector demonstrates how to fetch data from PayScore API and upsert it into destination using memory-efficient streaming patterns.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For making HTTP requests to PayScore API
import requests

# For handling dates and timestamps
from datetime import datetime, timedelta, timezone

# For type hints to improve code readability
from typing import List, Optional

# For time delays in rate limiting and retries
import time

""" ADD YOUR SOURCE-SPECIFIC IMPORTS HERE
Example: import pandas, boto3, etc.
Add comment for each import to explain its purpose for users to follow.
"""

# Private constants (use __ prefix)
__STAGING_API_ENDPOINT = "https://api.staging.payscore.com"
__PRODUCTION_API_ENDPOINT = "https://api.payscore.com"
__INVALID_LITERAL_ERROR = "invalid literal"


def __get_config_int(
    configuration: dict,
    key: str,
    default: int,
    min_val: Optional[int] = None,
    max_val: Optional[int] = None,
) -> int:
    """Centralized configuration parameter parsing with validation.

    Args:
        configuration: Configuration dictionary
        key: Configuration key to extract
        default: Default value if key not found
        min_val: Minimum allowed value
        max_val: Maximum allowed value

    Returns:
        Validated integer value

    Raises:
        ValueError: If value is invalid or out of range
    """
    value = configuration.get(key, str(default))

    try:
        int_value = int(value)

        if min_val is not None and int_value < min_val:
            raise ValueError(f"{key} must be at least {min_val}")
        if max_val is not None and int_value > max_val:
            raise ValueError(f"{key} must be at most {max_val}")

        return int_value
    except (ValueError, TypeError) as e:
        if __INVALID_LITERAL_ERROR in str(e):
            raise ValueError(f"{key} must be a valid integer")
        raise


def __calculate_wait_time(
    attempt: int, response_headers: dict, base_delay: int = 1, max_delay: int = 60
) -> int:
    """Calculate wait time with jitter for rate limiting and retries.

    Args:
        attempt: Current attempt number (0-based)
        response_headers: HTTP response headers
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds

    Returns:
        Wait time in seconds
    """
    # Check for Retry-After header
    retry_after = response_headers.get("Retry-After")
    if retry_after:
        try:
            return min(int(retry_after), max_delay)
        except (ValueError, TypeError):
            pass

    # Exponential backoff with jitter
    delay = min(base_delay * (2**attempt), max_delay)
    # Add 10% jitter to prevent thundering herd
    jitter = delay * 0.1
    return int(delay + jitter)


def __handle_rate_limit(attempt: int, response: requests.Response):
    """Handle HTTP 429 rate limiting.

    Args:
        attempt: Current attempt number
        response: HTTP response object
    """
    wait_time = __calculate_wait_time(attempt, response.headers, base_delay=2)
    log.warning(f"Rate limit hit (attempt {attempt + 1}), waiting {wait_time} seconds")
    time.sleep(wait_time)


def __handle_request_error(attempt: int, retry_attempts: int, error: Exception, endpoint: str):
    """Handle request errors with retry logic.

    Args:
        attempt: Current attempt number
        retry_attempts: Maximum retry attempts
        error: Exception that occurred
        endpoint: API endpoint being called
    """
    if attempt < retry_attempts - 1:
        wait_time = __calculate_wait_time(attempt, {}, base_delay=1)
        log.warning(
            f"Request to {endpoint} failed (attempt {attempt + 1}): {str(error)}. Retrying in {wait_time} seconds"
        )
        time.sleep(wait_time)
    else:
        log.warning(f"Request to {endpoint} failed after {retry_attempts} attempts: {str(error)}")
        raise RuntimeError(
            f"Failed to fetch data from {endpoint} after {retry_attempts} attempts: {str(error)}"
        )


def execute_api_request(
    endpoint: str,
    api_key: str,
    params: Optional[dict] = None,
    configuration: Optional[dict] = None,
) -> dict:
    """Main API request orchestrator - complexity <15.

    Args:
        endpoint: API endpoint path
        api_key: PayScore API key
        params: Query parameters
        configuration: Configuration dictionary

    Returns:
        API response data

    Raises:
        RuntimeError: If request fails after retries
    """
    # Determine base URL based on environment
    environment = (
        configuration.get("environment", "staging").lower() if configuration else "staging"
    )
    base_url = __PRODUCTION_API_ENDPOINT if environment == "production" else __STAGING_API_ENDPOINT

    url = f"{base_url}{endpoint}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    timeout = (
        __get_config_int(configuration, "request_timeout_seconds", 30) if configuration else 30
    )
    retry_attempts = __get_config_int(configuration, "retry_attempts", 3) if configuration else 3

    for attempt in range(retry_attempts):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)

            if response.status_code == 429:
                __handle_rate_limit(attempt, response)
                continue

            if response.status_code == 401:
                raise RuntimeError("Authentication failed. Please check your API key.")

            if response.status_code == 403:
                raise RuntimeError("Access forbidden. Please check your API permissions.")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            __handle_request_error(attempt, retry_attempts, e, endpoint)
            continue

    raise RuntimeError("Unexpected error in API request execution")


def get_time_range(
    last_sync_time: Optional[str] = None, configuration: Optional[dict] = None
) -> dict:
    """Generate dynamic time range - simplified logic with UTC.

    Args:
        last_sync_time: ISO timestamp of last sync
        configuration: Configuration dictionary

    Returns:
        Dictionary with start and end timestamps
    """
    end_time = datetime.now(timezone.utc).isoformat()

    if last_sync_time:
        start_time = last_sync_time
    else:
        initial_sync_days = (
            __get_config_int(configuration, "initial_sync_days", 90) if configuration else 90
        )
        start_time = (datetime.now(timezone.utc) - timedelta(days=initial_sync_days)).isoformat()

    return {"start": start_time, "end": end_time}


def __map_screening_group_data(record: dict, additional_context: Optional[dict] = None) -> dict:
    """Extract field mapping logic for screening groups maintainability."""
    return {
        "id": record.get("id", ""),
        "name": record.get("name", ""),
        "description": record.get("description", ""),
        "status": record.get("status", ""),
        "created_at": record.get("created_at", ""),
        "updated_at": record.get("updated_at", ""),
        "organization_id": record.get("organization_id", ""),
        "settings": json.dumps(record.get("settings", {})) if record.get("settings") else None,
        "member_count": record.get("member_count", 0),
        "_fivetran_synced": datetime.now(timezone.utc).isoformat(),
    }


def get_screening_groups(
    api_key: str,
    params: dict,
    last_sync_time: Optional[str] = None,
    configuration: Optional[dict] = None,
):
    """Fetch screening groups using streaming approach to prevent memory issues.

    Args:
        api_key: PayScore API key
        params: Base query parameters
        last_sync_time: Last sync timestamp for incremental sync
        configuration: Configuration dictionary

    Yields:
        Individual screening group records
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/api/v1/screening-groups"
    max_records = (
        __get_config_int(configuration, "max_records_per_page", 100) if configuration else 100
    )

    query_params = {
        "per_page": max_records,
        "page": 1,
        "updated_since": time_range["start"],
        **params,
    }

    page = 1
    while True:
        query_params["page"] = page
        response = execute_api_request(endpoint, api_key, query_params, configuration)

        data = response.get("data", [])
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_screening_group_data(record)

        # Check pagination
        pagination = response.get("pagination", {})
        if page >= pagination.get("total_pages", 1) or len(data) < max_records:
            break
        page += 1


def schema(configuration: dict) -> List[dict]:
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """

    return [
        {"table": "screening_group", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
     Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """

    log.info("Starting PayScore connector sync")

    # Configuration is auto-validated by SDK when configuration.json exists

    # Extract configuration parameters
    api_key = str(configuration.get("api_key", ""))
    enable_screening_groups = (
        configuration.get("enable_screening_groups", "true").lower() == "true"
    )
    # Requests and reports removed from this example

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time")

    try:
        # Base parameters for API requests
        base_params = {}

        # Fetch screening groups using generator to prevent memory accumulation
        if enable_screening_groups:
            log.info("Fetching screening groups...")
            for record in get_screening_groups(
                api_key, base_params, last_sync_time, configuration
            ):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="screening_group", data=record)

        # Requests and reports fetching removed

        # Update state and checkpoint
        new_state = {"last_sync_time": datetime.now(timezone.utc).isoformat()}
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)

        log.info("PayScore sync completed successfully")

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync PayScore data: {str(e)}")


# Create the connector instance
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
