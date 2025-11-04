"""Bitbucket API connector for syncing workspaces, repositories, and project data.
This connector demonstrates how to fetch data from Bitbucket API and upsert it into destination using memory-efficient streaming patterns.
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

# For making HTTP requests to Bitbucket API
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
__API_ENDPOINT = "https://api.bitbucket.org/2.0"


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


def execute_api_request(endpoint, username, app_password, params=None, configuration=None):
    """
    Execute HTTP API requests with comprehensive error handling and retry logic.
    This function handles authentication, rate limiting, timeouts, and network errors.

    Args:
        endpoint: API endpoint path to request.
        username: Bitbucket username for authentication.
        app_password: Bitbucket app password for authentication.
        params: Query parameters for the request (optional).
        configuration: Configuration dictionary for timeout and retry settings.

    Returns:
        dict: Parsed JSON response from the API.

    Raises:
        RuntimeError: If all retry attempts fail or unexpected errors occur.
        requests.exceptions.RequestException: For unrecoverable HTTP errors.
    """
    url = f"{__API_ENDPOINT}{endpoint}"

    timeout = __get_config_int(configuration, "request_timeout_seconds", 30)
    retry_attempts = __get_config_int(configuration, "retry_attempts", 3)

    for attempt in range(retry_attempts):
        try:
            response = requests.get(
                url, auth=(username, app_password), params=params, timeout=timeout
            )

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


def __map_workspace_data(workspace):
    """
    Transform API response workspace record to destination table schema format.
    This function maps raw API workspace fields to normalized database column names and types.

    Args:
        workspace: Raw API response workspace record dictionary.

    Returns:
        dict: Transformed workspace record ready for database insertion.
    """
    return {
        "uuid": workspace.get("uuid", ""),
        "name": workspace.get("name", ""),
        "slug": workspace.get("slug", ""),
        "type": workspace.get("type", ""),
        "is_private": workspace.get("is_private", False),
        "created_on": workspace.get("created_on", ""),
        "updated_on": workspace.get("updated_on", ""),
        "website": workspace.get("website", ""),
        "location": workspace.get("location", ""),
        "has_publicly_visible_repos": workspace.get("has_publicly_visible_repos", False),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_repository_data(repository, workspace_uuid):
    """
    Transform API response repository record to destination table schema format.
    This function maps raw API repository fields to normalized database column names and types.

    Args:
        repository: Raw API response repository record dictionary.
        workspace_uuid: UUID of the parent workspace.

    Returns:
        dict: Transformed repository record ready for database insertion.
    """
    return {
        "uuid": repository.get("uuid", ""),
        "name": repository.get("name", ""),
        "full_name": repository.get("full_name", ""),
        "slug": repository.get("slug", ""),
        "workspace_uuid": workspace_uuid,
        "is_private": repository.get("is_private", False),
        "description": repository.get("description", ""),
        "scm": repository.get("scm", ""),
        "website": repository.get("website", ""),
        "language": repository.get("language", ""),
        "has_issues": repository.get("has_issues", False),
        "has_wiki": repository.get("has_wiki", False),
        "size": repository.get("size", 0),
        "created_on": repository.get("created_on", ""),
        "updated_on": repository.get("updated_on", ""),
        "pushed_on": repository.get("pushed_on", ""),
        "fork_policy": repository.get("fork_policy", ""),
        "mainbranch_name": (
            repository.get("mainbranch", {}).get("name", "")
            if repository.get("mainbranch")
            else ""
        ),
        "mainbranch_type": (
            repository.get("mainbranch", {}).get("type", "")
            if repository.get("mainbranch")
            else ""
        ),
        "project_uuid": (
            repository.get("project", {}).get("uuid", "") if repository.get("project") else ""
        ),
        "project_name": (
            repository.get("project", {}).get("name", "") if repository.get("project") else ""
        ),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_project_data(project, workspace_uuid):
    """
    Transform API response project record to destination table schema format.
    This function maps raw API project fields to normalized database column names and types.

    Args:
        project: Raw API response project record dictionary.
        workspace_uuid: UUID of the parent workspace.

    Returns:
        dict: Transformed project record ready for database insertion.
    """
    return {
        "uuid": project.get("uuid", ""),
        "key": project.get("key", ""),
        "name": project.get("name", ""),
        "workspace_uuid": workspace_uuid,
        "description": project.get("description", ""),
        "is_private": project.get("is_private", False),
        "created_on": project.get("created_on", ""),
        "updated_on": project.get("updated_on", ""),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def get_workspaces(username, app_password, last_sync_time=None, configuration=None):
    """
    Fetch workspaces using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        username: Bitbucket username for authentication.
        app_password: Bitbucket app password for authentication.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual workspace records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/workspaces"
    max_records = __get_config_int(configuration, "max_records_per_page", 50, 1, 100)

    params = {
        "pagelen": max_records,
        "page": 1,
    }

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, username, app_password, params, configuration)

        workspaces = response.get("values", [])
        if not workspaces:
            break

        # Yield individual records instead of accumulating
        for workspace in workspaces:
            yield __map_workspace_data(workspace)

        # Check if there are more pages
        if len(workspaces) < max_records or not response.get("next"):
            break
        page += 1


def get_repositories(
    username, app_password, workspace_uuid, last_sync_time=None, configuration=None
):
    """
    Fetch repositories for a workspace using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        username: Bitbucket username for authentication.
        app_password: Bitbucket app password for authentication.
        workspace_uuid: UUID of the workspace to fetch repositories for.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual repository records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    # Extract workspace slug from UUID (Bitbucket accepts both)
    workspace_slug = workspace_uuid.strip("{}")
    endpoint = f"/repositories/{workspace_slug}"
    max_records = __get_config_int(configuration, "max_records_per_page", 50, 1, 100)

    params = {
        "pagelen": max_records,
        "page": 1,
    }

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, username, app_password, params, configuration)

        repositories = response.get("values", [])
        if not repositories:
            break

        # Yield individual records instead of accumulating
        for repository in repositories:
            yield __map_repository_data(repository, workspace_uuid)

        # Check if there are more pages
        if len(repositories) < max_records or not response.get("next"):
            break
        page += 1


def get_projects(username, app_password, workspace_uuid, last_sync_time=None, configuration=None):
    """
    Fetch projects for a workspace using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        username: Bitbucket username for authentication.
        app_password: Bitbucket app password for authentication.
        workspace_uuid: UUID of the workspace to fetch projects for.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual project records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    # Extract workspace slug from UUID (Bitbucket accepts both)
    workspace_slug = workspace_uuid.strip("{}")
    endpoint = f"/workspaces/{workspace_slug}/projects"
    max_records = __get_config_int(configuration, "max_records_per_page", 50, 1, 100)

    params = {
        "pagelen": max_records,
        "page": 1,
    }

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, username, app_password, params, configuration)

        projects = response.get("values", [])
        if not projects:
            break

        # Yield individual records instead of accumulating
        for project in projects:
            yield __map_project_data(project, workspace_uuid)

        # Check if there are more pages
        if len(projects) < max_records or not response.get("next"):
            break
        page += 1


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "workspaces", "primary_key": ["uuid"]},
        {"table": "repositories", "primary_key": ["uuid"]},
        {"table": "projects", "primary_key": ["uuid"]},
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
    log.info("Starting Bitbucket API connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    username = __get_config_str(configuration, "username")
    app_password = __get_config_str(configuration, "app_password")
    max_records_per_page = __get_config_int(configuration, "max_records_per_page", 50, 1, 100)
    enable_incremental = __get_config_bool(configuration, "enable_incremental_sync", True)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time")

    try:
        # Initialize counters for all data types
        workspace_count = 0
        repository_count = 0
        project_count = 0
        enable_repositories = __get_config_bool(configuration, "enable_repositories", True)
        enable_projects = __get_config_bool(configuration, "enable_projects", True)

        # Fetch workspaces and process related data immediately to avoid memory accumulation
        log.info("Fetching workspaces...")
        for workspace in get_workspaces(username, app_password, last_sync_time, configuration):
            workspace_uuid = workspace.get("uuid")

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="workspaces", data=workspace)
            workspace_count += 1

            # Process repositories for this workspace immediately to avoid memory accumulation
            if enable_repositories:
                for repository in get_repositories(
                    username, app_password, workspace_uuid, last_sync_time, configuration
                ):
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The op.upsert method is called with two arguments:
                    # - The first argument is the name of the table to upsert the data into.
                    # - The second argument is a dictionary containing the data to be upserted,
                    op.upsert(table="repositories", data=repository)
                    repository_count += 1

                    # Checkpoint every batch to save progress incrementally
                    if repository_count % max_records_per_page == 0:
                        checkpoint_state = {
                            "last_sync_time": repository.get(
                                "updated_on", datetime.now(timezone.utc).isoformat()
                            ),
                            "workspaces_processed": workspace_count,
                            "repositories_processed": repository_count,
                        }
                        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                        # from the correct position in case of next sync or interruptions.
                        # Learn more about how and where to checkpoint by reading our best practices documentation
                        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                        op.checkpoint(checkpoint_state)

            # Process projects for this workspace immediately to avoid memory accumulation
            if enable_projects:
                for project in get_projects(
                    username, app_password, workspace_uuid, last_sync_time, configuration
                ):
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The op.upsert method is called with two arguments:
                    # - The first argument is the name of the table to upsert the data into.
                    # - The second argument is a dictionary containing the data to be upserted,
                    op.upsert(table="projects", data=project)
                    project_count += 1

                    # Checkpoint every batch to save progress incrementally
                    if project_count % max_records_per_page == 0:
                        checkpoint_state = {
                            "last_sync_time": project.get(
                                "updated_on", datetime.now(timezone.utc).isoformat()
                            ),
                            "workspaces_processed": workspace_count,
                            "projects_processed": project_count,
                        }
                        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                        # from the correct position in case of next sync or interruptions.
                        # Learn more about how and where to checkpoint by reading our best practices documentation
                        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                        op.checkpoint(checkpoint_state)

            # Checkpoint every batch to save progress incrementally
            if workspace_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": workspace.get(
                        "updated_on", datetime.now(timezone.utc).isoformat()
                    ),
                    "workspaces_processed": workspace_count,
                    "repositories_processed": repository_count,
                    "projects_processed": project_count,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)

        log.info(f"Fetched {workspace_count} workspaces")
        if enable_repositories:
            log.info(f"Fetched {repository_count} repositories")
        if enable_projects:
            log.info(f"Fetched {project_count} projects")

        # Final checkpoint with completion status
        final_state = {"last_sync_time": datetime.now(timezone.utc).isoformat()}
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info(
            f"Sync completed successfully. Processed {workspace_count} workspaces, {repository_count} repositories, {project_count} projects."
        )

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Initialize the connector with schema and update functions
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
