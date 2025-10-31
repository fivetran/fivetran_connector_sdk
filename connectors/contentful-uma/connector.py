"""Contentful User Management API connector for syncing user, team, and organization data.
This connector demonstrates how to fetch data from Contentful User Management API and upsert it into destination using memory-efficient streaming patterns.
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

# For making HTTP requests to Contentful User Management API
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
__API_ENDPOINT = "https://api.contentful.com"
__EU_API_ENDPOINT = "https://api.eu.contentful.com"


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


def execute_api_request(endpoint, access_token, params=None, configuration=None):
    """
    Execute HTTP API requests with comprehensive error handling and retry logic.
    This function handles authentication, rate limiting, timeouts, and network errors.

    Args:
        endpoint: API endpoint path to request.
        access_token: Authentication token for API access.
        params: Query parameters for the request (optional).
        configuration: Configuration dictionary for timeout and retry settings.

    Returns:
        dict: Parsed JSON response from the API.

    Raises:
        RuntimeError: If all retry attempts fail or unexpected errors occur.
        requests.exceptions.RequestException: For unrecoverable HTTP errors.
    """
    # Determine base URL based on region and scope requests to an organization
    use_eu = __get_config_bool(configuration, "use_eu_region", False)
    base_url = __EU_API_ENDPOINT if use_eu else __API_ENDPOINT
    organization_id = __get_config_str(configuration, "organization_id")

    # Scope all UMA requests to the organization
    url = f"{base_url}/organizations/{organization_id}{endpoint}"

    headers = {"Authorization": f"Bearer {access_token}"}

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
        last_sync_time: Timestamp of last successful sync in ISO format (optional).
        configuration: Configuration dictionary containing sync settings (optional).

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


def __map_user_data(record):
    """
    Transform API response record to user table schema format.
    This function maps raw API user fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary containing user data.

    Returns:
        dict: Transformed user record ready for database insertion with normalized field names.
    """
    return {
        "id": record.get("id", ""),
        "email": record.get("email", ""),
        "first_name": record.get("firstName", ""),
        "last_name": record.get("lastName", ""),
        "avatar_url": record.get("avatarUrl", ""),
        "activated": record.get("activated", False),
        "sign_in_count": record.get("signInCount", 0),
        "confirmed": record.get("confirmed", False),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "last_activity": record.get("lastActivity", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_organization_membership_data(record):
    """
    Transform API response record to organization membership table schema format.
    This function maps raw API organization membership fields to normalized database column names.

    Args:
        record: Raw API response record dictionary containing organization membership data.

    Returns:
        dict: Transformed organization membership record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "type": record.get("type", ""),
        "admin": record.get("admin", False),
        "organization_membership_id": record.get("organizationMembershipId", ""),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "email": record.get("email", ""),
        "first_name": record.get("firstName", ""),
        "last_name": record.get("lastName", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_team_membership_data(record):
    """
    Transform API response record to team membership table schema format.
    This function maps raw API team membership fields to normalized database column names.

    Args:
        record: Raw API response record dictionary containing team membership data.

    Returns:
        dict: Transformed team membership record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "type": record.get("type", ""),
        "admin": record.get("admin", False),
        "team_membership_id": record.get("teamMembershipId", ""),
        "organization_membership_id": record.get("organizationMembershipId", ""),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_space_membership_data(record):
    """
    Transform API response record to space membership table schema format.
    This function maps raw API space membership fields to normalized database column names.

    Args:
        record: Raw API response record dictionary containing space membership data.

    Returns:
        dict: Transformed space membership record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "type": record.get("type", ""),
        "admin": record.get("admin", False),
        "space_id": record.get("spaceId", ""),
        "user_id": record.get("userId", ""),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_users(access_token, last_sync_time=None, configuration=None):
    """
    Fetch user data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual user records.

    Args:
        access_token: API authentication token for making requests.
        last_sync_time: Timestamp for incremental sync filtering (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual user records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/users"
    max_records = __get_config_int(configuration, "max_records_per_page", 100, 1, 100)

    skip = 0
    while True:
        params = {
            "limit": max_records,
            "skip": skip,
        }

        # Add time-based filtering if available
        if last_sync_time:
            params["updatedAt[gte]"] = last_sync_time

        response = execute_api_request(endpoint, access_token, params, configuration)

        items = response.get("items", [])
        if not items:
            break

        # Yield individual records instead of accumulating
        for record in items:
            yield __map_user_data(record)

        # Check if we've received all records
        total = response.get("total", 0)
        if skip + len(items) >= total:
            break

        skip += len(items)


def get_organization_memberships(access_token, last_sync_time=None, configuration=None):
    """
    Fetch organization membership data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual membership records.

    Args:
        access_token: API authentication token for making requests.
        last_sync_time: Timestamp for incremental sync filtering (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual organization membership records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/organization_memberships"
    max_records = __get_config_int(configuration, "max_records_per_page", 100, 1, 100)

    skip = 0
    while True:
        params = {
            "limit": max_records,
            "skip": skip,
        }

        if last_sync_time:
            params["updatedAt[gte]"] = last_sync_time

        response = execute_api_request(endpoint, access_token, params, configuration)

        items = response.get("items", [])
        if not items:
            break

        for record in items:
            yield __map_organization_membership_data(record)

        total = response.get("total", 0)
        if skip + len(items) >= total:
            break

        skip += len(items)


def get_team_memberships(access_token, last_sync_time=None, configuration=None):
    """
    Fetch team membership data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual membership records.

    Args:
        access_token: API authentication token for making requests.
        last_sync_time: Timestamp for incremental sync filtering (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual team membership records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/team_memberships"
    max_records = __get_config_int(configuration, "max_records_per_page", 100, 1, 100)

    skip = 0
    while True:
        params = {
            "limit": max_records,
            "skip": skip,
        }

        if last_sync_time:
            params["updatedAt[gte]"] = last_sync_time

        response = execute_api_request(endpoint, access_token, params, configuration)

        items = response.get("items", [])
        if not items:
            break

        for record in items:
            yield __map_team_membership_data(record)

        total = response.get("total", 0)
        if skip + len(items) >= total:
            break

        skip += len(items)


def get_space_memberships(access_token, last_sync_time=None, configuration=None):
    """
    Fetch space membership data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual membership records.

    Args:
        access_token: API authentication token for making requests.
        last_sync_time: Timestamp for incremental sync filtering (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual space membership records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/space_memberships"
    max_records = __get_config_int(configuration, "max_records_per_page", 100, 1, 100)

    skip = 0
    while True:
        params = {
            "limit": max_records,
            "skip": skip,
        }

        if last_sync_time:
            params["updatedAt[gte]"] = last_sync_time

        response = execute_api_request(endpoint, access_token, params, configuration)

        items = response.get("items", [])
        if not items:
            break

        for record in items:
            yield __map_space_membership_data(record)

        total = response.get("total", 0)
        if skip + len(items) >= total:
            break

        skip += len(items)


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
        {"table": "organization_memberships", "primary_key": ["id"]},
        {"table": "team_memberships", "primary_key": ["id"]},
        {"table": "space_memberships", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Main synchronization function that fetches and processes data from the Contentful User Management API.
    This function orchestrates the entire sync process using memory-efficient streaming patterns.

    Args:
        configuration: Configuration dictionary containing API credentials and settings.
        state: State dictionary containing sync cursors and checkpoints from previous runs.

    Raises:
        RuntimeError: If sync fails due to API errors or configuration issues.
    """
    log.info("Starting Contentful User Management API connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    access_token = __get_config_str(configuration, "access_token")
    max_records_per_page = __get_config_int(configuration, "max_records_per_page", 100, 1, 100)
    enable_incremental = __get_config_bool(configuration, "enable_incremental_sync", True)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time")

    try:
        # Fetch users data with incremental checkpointing
        log.info("Fetching users data...")
        record_count = 0

        for record in get_users(access_token, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="users", data=record)
            record_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if record_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": record.get(
                        "updated_at", datetime.now(timezone.utc).isoformat()
                    ),
                    "last_processed_users": record_count,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)

        log.info(f"Processed {record_count} users")

        # Fetch organization memberships
        if __get_config_bool(configuration, "enable_organization_memberships", True):
            log.info("Fetching organization memberships...")
            org_count = 0
            for record in get_organization_memberships(
                access_token, last_sync_time, configuration
            ):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="organization_memberships", data=record)
                org_count += 1

                if org_count % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": record.get(
                            "updated_at", datetime.now(timezone.utc).isoformat()
                        ),
                        "last_processed_org_memberships": org_count,
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)

            log.info(f"Processed {org_count} organization memberships")

        # Fetch team memberships
        if __get_config_bool(configuration, "enable_team_memberships", True):
            log.info("Fetching team memberships...")
            team_count = 0
            for record in get_team_memberships(access_token, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="team_memberships", data=record)
                team_count += 1

                if team_count % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": record.get(
                            "updated_at", datetime.now(timezone.utc).isoformat()
                        ),
                        "last_processed_team_memberships": team_count,
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)

            log.info(f"Processed {team_count} team memberships")

        # Fetch space memberships
        if __get_config_bool(configuration, "enable_space_memberships", True):
            log.info("Fetching space memberships...")
            space_count = 0
            for record in get_space_memberships(access_token, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="space_memberships", data=record)
                space_count += 1

                if space_count % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": record.get(
                            "updated_at", datetime.now(timezone.utc).isoformat()
                        ),
                        "last_processed_space_memberships": space_count,
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)

            log.info(f"Processed {space_count} space memberships")

        # Final checkpoint with completion status
        final_state = {"last_sync_time": datetime.now(timezone.utc).isoformat()}
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        total_records = sum(
            [
                record_count,
                (org_count if "org_count" in locals() else 0),
                (team_count if "team_count" in locals() else 0),
                (space_count if "space_count" in locals() else 0),
            ]
        )
        log.info(f"Sync completed successfully. Processed {total_records} total records.")

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


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
