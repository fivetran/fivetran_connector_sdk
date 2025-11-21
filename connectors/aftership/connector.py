"""AfterShip API connector for syncing trackings and couriers data.
This connector demonstrates how to fetch data from AfterShip API and upsert it into destination using memory-efficient streaming patterns.
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

# For making HTTP requests to AfterShip API
import requests

# For handling dates and timestamps
from datetime import datetime, timedelta, timezone

# For implementing delays in retry logic and rate limiting
import time

# For adding jitter to retry delays
import random

""" ADD YOUR SOURCE-SPECIFIC IMPORTS HERE
Example: import pandas, boto3, etc.
Add comment for each import to explain its purpose for users to follow.
"""
# Private constants (use __ prefix)
__API_ENDPOINT = "https://api.aftership.com/tracking/2025-07"


def __get_config_int(configuration, key, default, min_val=None, max_val=None):
    """
    Extract and validate integer configuration parameters with range checking.
    This function safely extracts integer values from configuration and applies validation.

    Args:
        configuration: Configuration dictionary containing connector settings.
        key: The configuration key to extract.
        default: Default value to return if key is missing or invalid.
        min_val: Minimum allowed value (optional).
        max_val: Maximum allowed value (optional).

    Returns:
        int: The validated integer value or default if validation fails.
    """
    try:
        value = int(configuration.get(key, default))
        if min_val is not None and value < min_val:
            return default
        if max_val is not None and value > max_val:
            return default
        return value
    except (ValueError, TypeError):
        return default


def __get_config_str(configuration, key, default=""):
    """
    Extract string configuration parameters with type safety.
    This function safely extracts string values from configuration dictionary.

    Args:
        configuration: Configuration dictionary containing connector settings.
        key: The configuration key to extract.
        default: Default value to return if key is missing.

    Returns:
        str: The string value or default if key is missing.
    """
    return str(configuration.get(key, default))


def __get_config_bool(configuration, key, default=False):
    """
    Extract and parse boolean configuration parameters from strings or boolean values.
    This function handles string representations of boolean values commonly used in JSON configuration.

    Args:
        configuration: Configuration dictionary containing connector settings.
        key: The configuration key to extract.
        default: Default boolean value to return if key is missing.

    Returns:
        bool: The parsed boolean value or default if key is missing.
    """
    value = configuration.get(key, default)
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
    return bool(value)


def __calculate_wait_time(attempt, response_headers, base_delay=1, max_delay=60):
    """
    Calculate exponential backoff wait time with jitter for retry attempts.
    This function implements exponential backoff with random jitter to prevent thundering herd problems.

    Args:
        attempt: Current attempt number (0-based).
        response_headers: HTTP response headers dictionary that may contain Retry-After.
        base_delay: Base delay in seconds for exponential backoff.
        max_delay: Maximum delay cap in seconds.

    Returns:
        float: Wait time in seconds before next retry attempt.
    """
    if "Retry-After" in response_headers:
        return min(int(response_headers["Retry-After"]), max_delay)

    # Exponential backoff with jitter
    wait_time = min(base_delay * (2**attempt), max_delay)
    jitter = random.uniform(0.1, 0.3) * wait_time
    return wait_time + jitter


def __handle_rate_limit(attempt, response):
    """
    Handle HTTP 429 rate limiting responses with appropriate delays.
    This function logs the rate limit and waits before allowing retry attempts.

    Args:
        attempt: Current attempt number for logging purposes.
        response: HTTP response object containing rate limit headers.
    """
    wait_time = __calculate_wait_time(attempt, response.headers)
    log.warning(f"Rate limit hit, waiting {wait_time:.1f} seconds before retry {attempt + 1}")
    time.sleep(wait_time)


def __handle_request_error(attempt, retry_attempts, error, endpoint):
    """
    Handle request errors with exponential backoff retry logic.
    This function manages retry attempts for failed API requests with appropriate delays.

    Args:
        attempt: Current attempt number (0-based).
        retry_attempts: Total number of retry attempts allowed.
        error: The exception that occurred during the request.
        endpoint: API endpoint that failed for logging purposes.

    Raises:
        Exception: Re-raises the original error after all retry attempts are exhausted.
    """
    if attempt < retry_attempts - 1:
        wait_time = __calculate_wait_time(attempt, {})
        log.warning(
            f"Request failed for {endpoint}: {str(error)}. Retrying in {wait_time:.1f} seconds..."
        )
        time.sleep(wait_time)
    else:
        log.severe(f"All retry attempts failed for {endpoint}: {str(error)}")
        raise error


def execute_api_request(endpoint, api_key, params=None, configuration=None):
    """
    Execute HTTP API requests with comprehensive error handling and retry logic.
    This function handles authentication, rate limiting, timeouts, and network errors.

    Args:
        endpoint: API endpoint path to request.
        api_key: Authentication key for API access.
        params: Query parameters for the request (optional).
        configuration: Configuration dictionary for timeout and retry settings.

    Returns:
        dict: Parsed JSON response from the API.

    Raises:
        RuntimeError: If all retry attempts fail or unexpected errors occur.
        requests.exceptions.RequestException: For unrecoverable HTTP errors.
    """
    url = f"{__API_ENDPOINT}{endpoint}"
    headers = {"aftership-api-key": api_key, "Content-Type": "application/json"}

    timeout = __get_config_int(configuration, "request_timeout_seconds", 30)
    retry_attempts = __get_config_int(configuration, "retry_attempts", 3)

    for attempt in range(retry_attempts):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)

            if response.status_code == 429:
                __handle_rate_limit(attempt, response)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            __handle_request_error(attempt, retry_attempts, e, endpoint)
            continue

    raise RuntimeError("Unexpected error in API request execution")


def get_time_range(last_sync_time=None, configuration=None):
    """
    Generate time range for incremental or initial data synchronization.
    This function creates start and end timestamps for API queries based on sync state.

    Args:
        last_sync_time: Timestamp of last successful sync (optional).
        configuration: Configuration dictionary containing sync settings.

    Returns:
        dict: Dictionary containing 'start' and 'end' timestamps in ISO format.
    """
    end_time = datetime.now(timezone.utc).isoformat()

    if last_sync_time:
        start_time = last_sync_time
    else:
        initial_sync_days = __get_config_int(configuration, "initial_sync_days", 30)
        start_time = (datetime.now(timezone.utc) - timedelta(days=initial_sync_days)).isoformat()

    return {"start": start_time, "end": end_time}


def __map_tracking_data(record):
    """
    Transform tracking API response record to destination table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary for tracking.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "tracking_number": record.get("tracking_number", ""),
        "slug": record.get("slug", ""),
        "active": record.get("active", True),
        "android": json.dumps(record.get("android", [])) if record.get("android") else "[]",
        "custom_fields": (
            json.dumps(record.get("custom_fields", {})) if record.get("custom_fields") else "{}"
        ),
        "customer_name": record.get("customer_name", ""),
        "delivery_time": record.get("delivery_time", 0),
        "destination_country_iso3": record.get("destination_country_iso3", ""),
        "emails": json.dumps(record.get("emails", [])) if record.get("emails") else "[]",
        "expected_delivery": record.get("expected_delivery", ""),
        "ios": json.dumps(record.get("ios", [])) if record.get("ios") else "[]",
        "note": record.get("note", ""),
        "order_id": record.get("order_id", ""),
        "order_id_path": record.get("order_id_path", ""),
        "origin_country_iso3": record.get("origin_country_iso3", ""),
        "shipment_package_count": record.get("shipment_package_count", 0),
        "shipment_pickup_date": record.get("shipment_pickup_date", ""),
        "shipment_delivery_date": record.get("shipment_delivery_date", ""),
        "shipment_type": record.get("shipment_type", ""),
        "shipment_weight": record.get("shipment_weight", ""),
        "shipment_weight_unit": record.get("shipment_weight_unit", ""),
        "signed_by": record.get("signed_by", ""),
        "smses": json.dumps(record.get("smses", [])) if record.get("smses") else "[]",
        "source": record.get("source", ""),
        "tag": record.get("tag", ""),
        "title": record.get("title", ""),
        "tracked_count": record.get("tracked_count", 0),
        "last_updated_at": record.get("last_updated_at", ""),
        "unique_token": record.get("unique_token", ""),
        "checkpoints": (
            json.dumps(record.get("checkpoints", [])) if record.get("checkpoints") else "[]"
        ),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_courier_data(record):
    """
    Transform courier API response record to destination table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary for courier.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "slug": record.get("slug", ""),
        "name": record.get("name", ""),
        "phone": record.get("phone", ""),
        "other_name": record.get("other_name", ""),
        "web_url": record.get("web_url", ""),
        "required_fields": (
            json.dumps(record.get("required_fields", []))
            if record.get("required_fields")
            else "[]"
        ),
        "optional_fields": (
            json.dumps(record.get("optional_fields", []))
            if record.get("optional_fields")
            else "[]"
        ),
        "default_language": record.get("default_language", ""),
        "support_pickup": record.get("support_pickup", False),
        "support_track": record.get("support_track", False),
        "support_unicode": record.get("support_unicode", False),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_trackings(api_key, params, last_sync_time=None, configuration=None):
    """
    Fetch trackings using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        params: Additional parameters for the API request.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual tracking records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/trackings"
    max_records = __get_config_int(configuration, "max_records_per_page", 100, 1, 200)

    params = {
        "limit": max_records,
        "page": 1,
    }

    # Add time filtering for incremental sync
    if last_sync_time:
        params["created_at_min"] = last_sync_time

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        # Handle AfterShip API response structure
        data = response.get("data", {})
        trackings = data.get("trackings", [])

        if not trackings:
            break

        # Yield individual records instead of accumulating
        for tracking in trackings:
            yield __map_tracking_data(tracking)

        # Check pagination info
        meta = response.get("meta", {})
        total_pages = meta.get("total_pages", 1)

        if page >= total_pages:
            break

        page += 1


def get_couriers(api_key, params, last_sync_time=None, configuration=None):
    """
    Fetch couriers using memory-efficient streaming approach.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        params: Additional parameters for the API request.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual courier records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/couriers"

    response = execute_api_request(endpoint, api_key, {}, configuration)

    # Handle AfterShip API response structure
    data = response.get("data", {})
    couriers = data.get("couriers", [])

    # Yield individual records instead of accumulating
    for courier in couriers:
        yield __map_courier_data(courier)


def schema(configuration: dict):
    """
    Define database schema with table names and primary keys for the connector.
    This function specifies the destination tables and their primary keys for Fivetran to create.

    Args:
        configuration: Configuration dictionary (not used but required by SDK).

    Returns:
        list: List of table schema dictionaries with table names and primary keys.
    """
    return [
        {"table": "trackings", "primary_key": ["id"]},
        {"table": "couriers", "primary_key": ["slug"]},
    ]


def update(configuration: dict, state: dict):
    """
    Main synchronization function that fetches and processes data from the AfterShip API.
    This function orchestrates the entire sync process using memory-efficient streaming patterns.

    Args:
        configuration: Configuration dictionary containing API credentials and settings.
        state: State dictionary containing sync cursors and checkpoints from previous runs.

    Raises:
        RuntimeError: If sync fails due to API errors or configuration issues.
    """
    log.info("Starting AfterShip API connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    api_key = __get_config_str(configuration, "api_key")
    max_records_per_page = __get_config_int(configuration, "max_records_per_page", 100, 1, 200)
    enable_incremental = __get_config_bool(configuration, "enable_incremental_sync", True)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time")

    try:
        # Fetch trackings using generator with incremental checkpointing
        log.info("Fetching trackings...")
        tracking_count = 0
        page = 1

        for tracking in get_trackings(api_key, {}, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="trackings", data=tracking)
            tracking_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if tracking_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": tracking.get(
                        "last_updated_at", datetime.now(timezone.utc).isoformat()
                    ),
                    "last_processed_tracking_page": page,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                page += 1

        # Fetch couriers if enabled
        if __get_config_bool(configuration, "enable_couriers", True):
            log.info("Fetching couriers...")
            courier_count = 0

            for courier in get_couriers(api_key, {}, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="couriers", data=courier)
                courier_count += 1

        # Final checkpoint with completion status
        final_state = {"last_sync_time": datetime.now(timezone.utc).isoformat()}
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info(
            f"Sync completed successfully. Processed {tracking_count} trackings and {courier_count if 'courier_count' in locals() else 0} couriers."
        )

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Create connector instance
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
