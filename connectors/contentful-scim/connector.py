"""Contentful SCIM connector for syncing user and group data.
This connector demonstrates how to fetch data from Contentful SCIM API and upsert it into destination using memory-efficient streaming patterns.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For adding jitter to retry delays
import random

# For making HTTP requests to Contentful SCIM API
import requests

# For implementing delays in retry logic and rate limiting
import time

# For handling dates and timestamps
from datetime import datetime, timedelta, timezone

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Private constants (use __ prefix)
__API_BASE_URL = "https://api.contentful.com/scim/v2"


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


def execute_api_request(endpoint, bearer_token, org_id, params=None, configuration=None):
    """
    Execute HTTP API requests with comprehensive error handling and retry logic.
    This function handles authentication, rate limiting, timeouts, and network errors.

    Args:
        endpoint: API endpoint path to request.
        bearer_token: Bearer token for API authentication.
        org_id: Contentful organization ID.
        params: Query parameters for the request (optional).
        configuration: Configuration dictionary for timeout and retry settings.

    Returns:
        dict: Parsed JSON response from the API.

    Raises:
        RuntimeError: If all retry attempts fail or unexpected errors occur.
        requests.exceptions.RequestException: For unrecoverable HTTP errors.
    """
    url = f"{__API_BASE_URL}/organizations/{org_id}{endpoint}"
    headers = {"Authorization": f"Bearer {bearer_token}", "Content-Type": "application/scim+json"}

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


def __map_user_data(user_record):
    """
    Transform Contentful SCIM user record to destination table schema format.
    This function maps raw SCIM user fields to normalized database column names and types.

    Args:
        user_record: Raw SCIM user response record dictionary.

    Returns:
        dict: Transformed user record ready for database insertion.
    """
    return {
        "id": user_record.get("id", ""),
        "user_name": user_record.get("userName", ""),
        "email": user_record.get("userName", ""),  # userName is email in SCIM
        "first_name": user_record.get("name", {}).get("givenName", ""),
        "last_name": user_record.get("name", {}).get("familyName", ""),
        "display_name": user_record.get("displayName", ""),
        "active": user_record.get("active", True),
        "created": user_record.get("meta", {}).get("created", ""),
        "last_modified": user_record.get("meta", {}).get("lastModified", ""),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_group_data(group_record):
    """
    Transform Contentful SCIM group record to destination table schema format.
    This function maps raw SCIM group fields to normalized database column names and types.

    Args:
        group_record: Raw SCIM group response record dictionary.

    Returns:
        dict: Transformed group record ready for database insertion.
    """
    return {
        "id": group_record.get("id", ""),
        "display_name": group_record.get("displayName", ""),
        "created": group_record.get("meta", {}).get("created", ""),
        "last_modified": group_record.get("meta", {}).get("lastModified", ""),
        "member_count": len(group_record.get("members", [])),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


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
        initial_sync_days = __get_config_int(configuration, "initial_sync_days", 365)
        start_time = (datetime.now(timezone.utc) - timedelta(days=initial_sync_days)).isoformat()

    return {"start": start_time, "end": end_time}


def get_users(bearer_token, org_id, params, last_sync_time=None, configuration=None):
    """
    Fetch user data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        bearer_token: Bearer token for API authentication.
        org_id: Contentful organization ID.
        params: Additional parameters for the API request.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual user records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/Users"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    # Build query parameters for SCIM filtering
    query_params = {
        "count": max_records,
        "startIndex": 1,
    }

    # Add incremental sync filter if available
    if last_sync_time and __get_config_bool(configuration, "enable_incremental_sync", True):
        # SCIM filter for lastModified greater than last sync time
        query_params["filter"] = f'meta.lastModified gt "{last_sync_time}"'

    start_index = 1
    while True:
        query_params["startIndex"] = start_index
        response = execute_api_request(endpoint, bearer_token, org_id, query_params, configuration)

        users = response.get("Resources", [])
        if not users:
            break

        # Yield individual records instead of accumulating
        for user in users:
            yield __map_user_data(user)

        # Check if we have more results
        total_results = response.get("totalResults", 0)
        items_per_page = response.get("itemsPerPage", len(users))

        if start_index + items_per_page > total_results:
            break

        start_index += items_per_page


def get_groups(bearer_token, org_id, params, last_sync_time=None, configuration=None):
    """
    Fetch group data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        bearer_token: Bearer token for API authentication.
        org_id: Contentful organization ID.
        params: Additional parameters for the API request.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual group records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/Groups"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    # Build query parameters for SCIM filtering
    query_params = {
        "count": max_records,
        "startIndex": 1,
    }

    # Add incremental sync filter if available
    if last_sync_time and __get_config_bool(configuration, "enable_incremental_sync", True):
        # SCIM filter for lastModified greater than last sync time
        query_params["filter"] = f'meta.lastModified gt "{last_sync_time}"'

    start_index = 1
    while True:
        query_params["startIndex"] = start_index
        response = execute_api_request(endpoint, bearer_token, org_id, query_params, configuration)

        groups = response.get("Resources", [])
        if not groups:
            break

        # Yield individual records instead of accumulating
        for group in groups:
            yield __map_group_data(group)

        # Check if we have more results
        total_results = response.get("totalResults", 0)
        items_per_page = response.get("itemsPerPage", len(groups))

        if start_index + items_per_page > total_results:
            break

        start_index += items_per_page


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
        {"table": "users", "primary_key": ["id"]},
        {"table": "groups", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Main synchronization function that fetches and processes data from the Contentful SCIM API.
    This function orchestrates the entire sync process using memory-efficient streaming patterns.

    Args:
        configuration: Configuration dictionary containing API credentials and settings.
        state: State dictionary containing sync cursors and checkpoints from previous runs.

    Raises:
        RuntimeError: If sync fails due to API errors or configuration issues.
    """
    log.info("Starting Contentful SCIM connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    bearer_token = __get_config_str(configuration, "bearer_token")
    org_id = __get_config_str(configuration, "org_id")
    max_records_per_page = __get_config_int(configuration, "max_records_per_page", 100, 1, 1000)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time")

    try:
        # Fetch user data using generator with incremental checkpointing
        log.info("Fetching user data...")
        user_count = 0
        page = 1

        for user_record in get_users(bearer_token, org_id, {}, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="users", data=user_record)
            user_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if user_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": user_record.get(
                        "last_modified", datetime.now(timezone.utc).isoformat()
                    ),
                    "last_processed_page": page,
                    "users_processed": user_count,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                page += 1

        # Fetch group data using generator
        log.info("Fetching group data...")
        group_count = 0

        for group_record in get_groups(bearer_token, org_id, {}, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="groups", data=group_record)
            group_count += 1

        # Final checkpoint with completion status
        final_state = {
            "last_sync_time": datetime.now(timezone.utc).isoformat(),
            "users_synced": user_count,
            "groups_synced": group_count,
        }
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info(
            f"Sync completed successfully. Processed {user_count} users, {group_count} groups."
        )

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync Contentful SCIM data: {str(e)}")


# Connector instance
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
