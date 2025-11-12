"""
Deltek ConceptShare API connector for syncing projects, project resources, and teams data.
This connector demonstrates how to fetch data from Deltek ConceptShare API and upsert it into destination using memory-efficient streaming patterns.
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

# For making HTTP requests to Deltek ConceptShare API
import requests

# For handling dates and timestamps
from datetime import datetime, timedelta, timezone

# For implementing delays in retry logic and rate limiting
import time

# For adding jitter to retry delays
import random

# Private constants
__API_ENDPOINT = "https://api.conceptshare.com/v1"


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
    headers = {"Authorization": f"Bearer {api_key}"}

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
        initial_sync_days = __get_config_int(configuration, "initial_sync_days", 90)
        start_time = (datetime.now(timezone.utc) - timedelta(days=initial_sync_days)).isoformat()

    return {"start": start_time, "end": end_time}


def __map_project_data(record):
    """
    Transform API response record to projects table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "name": record.get("name", ""),
        "description": record.get("description", ""),
        "status": record.get("status", ""),
        "created_date": record.get("created_date", ""),
        "modified_date": record.get("modified_date", ""),
        "owner_id": record.get("owner_id", ""),
        "owner_name": record.get("owner_name", ""),
        "project_type": record.get("project_type", ""),
        "privacy_level": record.get("privacy_level", ""),
        "due_date": record.get("due_date", ""),
        "archived": record.get("archived", False),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_project_resource_data(record):
    """
    Transform API response record to project_resources table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "project_id": record.get("project_id", ""),
        "resource_type": record.get("resource_type", ""),
        "resource_name": record.get("resource_name", ""),
        "file_name": record.get("file_name", ""),
        "file_size": record.get("file_size", 0),
        "version": record.get("version", ""),
        "uploaded_date": record.get("uploaded_date", ""),
        "uploaded_by": record.get("uploaded_by", ""),
        "status": record.get("status", ""),
        "url": record.get("url", ""),
        "mime_type": record.get("mime_type", ""),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_team_data(record):
    """
    Transform API response record to teams table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "name": record.get("name", ""),
        "description": record.get("description", ""),
        "created_date": record.get("created_date", ""),
        "modified_date": record.get("modified_date", ""),
        "owner_id": record.get("owner_id", ""),
        "owner_name": record.get("owner_name", ""),
        "member_count": record.get("member_count", 0),
        "privacy_level": record.get("privacy_level", ""),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def get_projects(api_key, last_sync_time=None, configuration=None):
    """
    Fetch projects data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual project records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/projects"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    params = {
        "limit": max_records,
        "offset": 0,
        "modified_since": time_range["start"],
    }

    while True:
        response = execute_api_request(endpoint, api_key, params, configuration)

        projects = response.get("projects", [])
        if not projects:
            break

        for project in projects:
            yield __map_project_data(project)

        if len(projects) < max_records:
            break

        params["offset"] += max_records


def get_project_resources(api_key, last_sync_time=None, configuration=None):
    """
    Fetch project resources data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual project resource records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/project-resources"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    params = {
        "limit": max_records,
        "offset": 0,
        "modified_since": time_range["start"],
    }

    while True:
        response = execute_api_request(endpoint, api_key, params, configuration)

        resources = response.get("resources", [])
        if not resources:
            break

        for resource in resources:
            yield __map_project_resource_data(resource)

        if len(resources) < max_records:
            break

        params["offset"] += max_records


def get_teams(api_key, last_sync_time=None, configuration=None):
    """
    Fetch teams data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual team records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/teams"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    params = {
        "limit": max_records,
        "offset": 0,
        "modified_since": time_range["start"],
    }

    while True:
        response = execute_api_request(endpoint, api_key, params, configuration)

        teams = response.get("teams", [])
        if not teams:
            break

        for team in teams:
            yield __map_team_data(team)

        if len(teams) < max_records:
            break

        params["offset"] += max_records


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "projects", "primary_key": ["id"]},
        {"table": "project_resources", "primary_key": ["id"]},
        {"table": "teams", "primary_key": ["id"]},
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
    log.info("Starting Deltek ConceptShare connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    api_key = __get_config_str(configuration, "api_key")
    max_records_per_page = __get_config_int(configuration, "max_records_per_page", 100, 1, 1000)
    enable_incremental = __get_config_bool(configuration, "enable_incremental_sync", True)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time") if enable_incremental else None

    try:
        # Fetch projects data
        log.info("Fetching projects data...")
        projects_count = 0

        for project in get_projects(api_key, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="projects", data=project)
            projects_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if projects_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": project.get(
                        "modified_date", datetime.now(timezone.utc).isoformat()
                    ),
                    "projects_processed": projects_count,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)

        log.info(f"Processed {projects_count} projects")

        # Fetch project resources data
        log.info("Fetching project resources data...")
        resources_count = 0

        for resource in get_project_resources(api_key, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="project_resources", data=resource)
            resources_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if resources_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": resource.get(
                        "uploaded_date", datetime.now(timezone.utc).isoformat()
                    ),
                    "projects_processed": projects_count,
                    "resources_processed": resources_count,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)

        log.info(f"Processed {resources_count} project resources")

        # Fetch teams data
        log.info("Fetching teams data...")
        teams_count = 0

        for team in get_teams(api_key, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="teams", data=team)
            teams_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if teams_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": team.get(
                        "modified_date", datetime.now(timezone.utc).isoformat()
                    ),
                    "projects_processed": projects_count,
                    "resources_processed": resources_count,
                    "teams_processed": teams_count,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)

        log.info(f"Processed {teams_count} teams")

        # Final checkpoint with completion status
        final_state = {
            "last_sync_time": datetime.now(timezone.utc).isoformat(),
            "projects_processed": projects_count,
            "resources_processed": resources_count,
            "teams_processed": teams_count,
        }
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info(
            f"Sync completed successfully. Total records: {projects_count + resources_count + teams_count}"
        )

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync Deltek ConceptShare data: {str(e)}")


# Creating the connector object and passing the update and schema functions to the constructor.
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
