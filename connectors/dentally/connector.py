"""Dentally API connector for syncing dental practice data including rooms, sites, and treatments.
This connector demonstrates how to fetch data from Dentally API and upsert it into destination using memory-efficient streaming patterns.
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

# For making HTTP requests to Dentally API
import requests

# For handling dates and timestamps in OAuth and data sync
from datetime import datetime, timedelta, timezone

# For implementing delays in retry logic and rate limiting
import time

# For adding jitter to retry delays and OAuth state generation
import random

""" ADD YOUR SOURCE-SPECIFIC IMPORTS HERE
Example: import pandas, boto3, etc.
Add comment for each import to explain its purpose for users to follow.
"""
# Private constants (use __ prefix)
__API_ENDPOINT = "https://api.dentally.co"  # Production endpoint
__SANDBOX_ENDPOINT = "https://api.sandbox.dentally.co"  # Sandbox endpoint
__OAUTH_AUTHORIZE_URL = "https://api.dentally.co/oauth/authorize"
__OAUTH_TOKEN_URL = "https://api.dentally.co/oauth/token"


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


def refresh_access_token(configuration):
    """
    Refresh OAuth2 access token using refresh token.
    This function handles OAuth2 token refresh for maintaining API access.

    Args:
        configuration: Configuration dictionary containing OAuth credentials.

    Returns:
        str: New access token for API requests.

    Raises:
        RuntimeError: If token refresh fails after all retry attempts.
    """
    client_id = __get_config_str(configuration, "client_id")
    client_secret = __get_config_str(configuration, "client_secret")
    refresh_token = __get_config_str(configuration, "refresh_token")

    use_sandbox = __get_config_bool(configuration, "use_sandbox", False)
    token_url = (
        __OAUTH_TOKEN_URL.replace("api.dentally.co", "api.sandbox.dentally.co")
        if use_sandbox
        else __OAUTH_TOKEN_URL
    )

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Fivetran Dentally Connector/1.0",
    }

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }

    retry_attempts = __get_config_int(configuration, "retry_attempts", 3)

    for attempt in range(retry_attempts):
        try:
            response = requests.post(token_url, headers=headers, data=data, timeout=30)

            if response.status_code == 429:
                __handle_rate_limit(attempt, response)
                continue

            response.raise_for_status()
            token_data = response.json()

            if "access_token" not in token_data:
                raise RuntimeError("Access token not found in response")

            return token_data["access_token"]

        except requests.exceptions.RequestException as e:
            __handle_request_error(attempt, retry_attempts, e, "/oauth/token")
            continue

    raise RuntimeError("Failed to refresh access token after all retry attempts")


def execute_api_request(endpoint, access_token, params=None, configuration=None):
    """
    Execute HTTP API requests with comprehensive error handling and retry logic.
    This function handles authentication, rate limiting, timeouts, and network errors.

    Args:
        endpoint: API endpoint path to request.
        access_token: OAuth2 access token for API access.
        params: Query parameters for the request (optional).
        configuration: Configuration dictionary for timeout and retry settings.

    Returns:
        dict: Parsed JSON response from the API.

    Raises:
        RuntimeError: If all retry attempts fail or unexpected errors occur.
        requests.exceptions.RequestException: For unrecoverable HTTP errors.
    """
    use_sandbox = __get_config_bool(configuration, "use_sandbox", False)
    base_url = __SANDBOX_ENDPOINT if use_sandbox else __API_ENDPOINT
    url = f"{base_url}{endpoint}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "User-Agent": "Fivetran Dentally Connector/1.0",
        "Content-Type": "application/json",
    }

    timeout = __get_config_int(configuration, "request_timeout_seconds", 30)
    retry_attempts = __get_config_int(configuration, "retry_attempts", 3)

    for attempt in range(retry_attempts):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)

            if response.status_code == 401:
                # Token expired, refresh and retry
                log.warning("Access token expired, refreshing token...")
                new_token = refresh_access_token(configuration)
                headers["Authorization"] = f"Bearer {new_token}"
                continue

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
        initial_sync_days = __get_config_int(configuration, "initial_sync_days", 90)
        start_time = (datetime.now(timezone.utc) - timedelta(days=initial_sync_days)).isoformat()

    return {"start": start_time, "end": end_time}


def __map_room_data(record):
    """
    Transform API response record to ROOMS table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "name": record.get("name", ""),
        "room_number": record.get("room_number", ""),
        "equipment_type": record.get("equipment_type", ""),
        "site_id": record.get("site_id", ""),
        "status": record.get("status", "active"),
        "created_at": record.get("created_at", ""),
        "updated_at": record.get("updated_at", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_site_data(record):
    """
    Transform API response record to SITES table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "name": record.get("name", ""),
        "address": record.get("address", ""),
        "phone": record.get("phone", ""),
        "email": record.get("email", ""),
        "timezone": record.get("timezone", ""),
        "created_at": record.get("created_at", ""),
        "updated_at": record.get("updated_at", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_treatment_data(record):
    """
    Transform API response record to TREATMENTS table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "name": record.get("name", ""),
        "code": record.get("code", ""),
        "category": record.get("category", ""),
        "description": record.get("description", ""),
        "default_fee": record.get("default_fee", 0),
        "duration_minutes": record.get("duration_minutes", 0),
        "created_at": record.get("created_at", ""),
        "updated_at": record.get("updated_at", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_rooms_data(access_token, last_sync_time=None, configuration=None):
    """
    Fetch rooms data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        access_token: OAuth2 access token for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual room records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/rooms"
    max_records = __get_config_int(configuration, "max_records_per_page", 25, 1, 100)

    params = {
        "per_page": max_records,
        "page": 1,
    }

    # Add time filtering for incremental sync if available
    if last_sync_time:
        params["updated_since"] = last_sync_time

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, access_token, params, configuration)

        # Handle different possible response structures
        data = response.get("data", response.get("rooms", []))
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_room_data(record)

        # Check if we've reached the last page
        if len(data) < max_records:
            break
        page += 1


def get_sites_data(access_token, last_sync_time=None, configuration=None):
    """
    Fetch sites data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        access_token: OAuth2 access token for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual site records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/sites"
    max_records = __get_config_int(configuration, "max_records_per_page", 25, 1, 100)

    params = {
        "per_page": max_records,
        "page": 1,
    }

    # Add time filtering for incremental sync if available
    if last_sync_time:
        params["updated_since"] = last_sync_time

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, access_token, params, configuration)

        # Handle different possible response structures
        data = response.get("data", response.get("sites", []))
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_site_data(record)

        # Check if we've reached the last page
        if len(data) < max_records:
            break
        page += 1


def get_treatments_data(access_token, last_sync_time=None, configuration=None):
    """
    Fetch treatments data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        access_token: OAuth2 access token for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual treatment records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/treatments"
    max_records = __get_config_int(configuration, "max_records_per_page", 25, 1, 100)

    params = {
        "per_page": max_records,
        "page": 1,
    }

    # Add time filtering for incremental sync if available
    if last_sync_time:
        params["updated_since"] = last_sync_time

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, access_token, params, configuration)

        # Handle different possible response structures
        data = response.get("data", response.get("treatments", []))
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_treatment_data(record)

        # Check if we've reached the last page
        if len(data) < max_records:
            break
        page += 1


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
        {"table": "rooms", "primary_key": ["id"]},
        {"table": "sites", "primary_key": ["id"]},
        {"table": "treatments", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Main synchronization function that fetches and processes data from the Dentally API.
    This function orchestrates the entire sync process using memory-efficient streaming patterns.

    Args:
        configuration: Configuration dictionary containing API credentials and settings.
        state: State dictionary containing sync cursors and checkpoints from previous runs.

    Raises:
        RuntimeError: If sync fails due to API errors or configuration issues.
    """
    log.info("Starting Dentally connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    access_token = __get_config_str(configuration, "access_token")
    max_records_per_page = __get_config_int(configuration, "max_records_per_page", 25, 1, 100)
    enable_incremental = __get_config_bool(configuration, "enable_incremental_sync", True)
    enable_rooms = __get_config_bool(configuration, "enable_rooms_sync", True)
    enable_sites = __get_config_bool(configuration, "enable_sites_sync", True)
    enable_treatments = __get_config_bool(configuration, "enable_treatments_sync", True)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time") if enable_incremental else None

    try:
        total_record_count = 0

        # Fetch rooms data
        if enable_rooms:
            log.info("Fetching rooms data...")
            rooms_count = 0
            page = 1

            for record in get_rooms_data(access_token, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="rooms", data=record)
                rooms_count += 1
                total_record_count += 1

                # Checkpoint every page/batch to save progress incrementally
                if rooms_count % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": record.get(
                            "updated_at", datetime.now(timezone.utc).isoformat()
                        ),
                        "last_processed_page": page,
                        "table": "rooms",
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)
                    page += 1

            log.info(f"Completed rooms sync. Processed {rooms_count} records.")

        # Fetch sites data
        if enable_sites:
            log.info("Fetching sites data...")
            sites_count = 0
            page = 1

            for record in get_sites_data(access_token, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="sites", data=record)
                sites_count += 1
                total_record_count += 1

                # Checkpoint every page/batch to save progress incrementally
                if sites_count % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": record.get(
                            "updated_at", datetime.now(timezone.utc).isoformat()
                        ),
                        "last_processed_page": page,
                        "table": "sites",
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)
                    page += 1

            log.info(f"Completed sites sync. Processed {sites_count} records.")

        # Fetch treatments data
        if enable_treatments:
            log.info("Fetching treatments data...")
            treatments_count = 0
            page = 1

            for record in get_treatments_data(access_token, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="treatments", data=record)
                treatments_count += 1
                total_record_count += 1

                # Checkpoint every page/batch to save progress incrementally
                if treatments_count % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": record.get(
                            "updated_at", datetime.now(timezone.utc).isoformat()
                        ),
                        "last_processed_page": page,
                        "table": "treatments",
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)
                    page += 1

            log.info(f"Completed treatments sync. Processed {treatments_count} records.")

        # Final checkpoint with completion status
        final_state = {"last_sync_time": datetime.now(timezone.utc).isoformat()}
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info(f"Sync completed successfully. Processed {total_record_count} total records.")

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# This variable defines the entry point for the Fivetran SDK
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
