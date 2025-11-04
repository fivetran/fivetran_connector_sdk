"""Xurrent API connector for syncing organizations, products, and projects data.
This connector demonstrates how to fetch data from Xurrent API and upsert it into destination using memory-efficient streaming patterns.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For adding jitter to retry delays
import random

# For making HTTP requests to Xurrent API
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
__API_BASE_URL = "https://api.xurrent.com/v1"
__DEFAULT_PAGE_SIZE = 25
__MAX_PAGE_SIZE = 100
__MAX_RETRY_ATTEMPTS = 3
__DEFAULT_REQUEST_TIMEOUT = 30


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


def execute_api_request(endpoint, oauth_token, account_id, params=None, configuration=None):
    """
    Execute HTTP API requests with comprehensive error handling and retry logic.
    This function handles authentication, rate limiting, timeouts, and network errors.

    Args:
        endpoint: API endpoint path to request.
        oauth_token: OAuth token for API authentication.
        account_id: Xurrent account ID for API access.
        params: Query parameters for the request (optional).
        configuration: Configuration dictionary for timeout and retry settings.

    Returns:
        dict: Parsed JSON response from the API.

    Raises:
        RuntimeError: If all retry attempts fail or unexpected errors occur.
        requests.exceptions.RequestException: For unrecoverable HTTP errors.
    """
    url = f"{__API_BASE_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {oauth_token}",
        "X-Xurrent-Account": account_id,
        "Accept": "application/json",
    }

    timeout = __get_config_int(configuration, "request_timeout_seconds", __DEFAULT_REQUEST_TIMEOUT)
    retry_attempts = __get_config_int(configuration, "retry_attempts", __MAX_RETRY_ATTEMPTS)

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


def __map_organization_data(record):
    """
    Transform API response record to organizations table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary for organization.

    Returns:
        dict: Transformed organization record ready for database insertion.
    """
    return {
        "id": record.get("id"),
        "name": record.get("name", ""),
        "source_id": record.get("sourceID", ""),
        "disabled": record.get("disabled", False),
        "parent_id": record.get("parent", {}).get("id") if record.get("parent") else None,
        "parent_name": record.get("parent", {}).get("name") if record.get("parent") else None,
        "manager_id": record.get("manager", {}).get("id") if record.get("manager") else None,
        "manager_name": record.get("manager", {}).get("name") if record.get("manager") else None,
        "business_unit_id": (
            record.get("business_unit_organization", {}).get("id")
            if record.get("business_unit_organization")
            else None
        ),
        "region": record.get("region", ""),
        "remarks": record.get("remarks", ""),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_product_data(record):
    """
    Transform API response record to products table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary for product.

    Returns:
        dict: Transformed product record ready for database insertion.
    """
    return {
        "id": record.get("id"),
        "name": record.get("name", ""),
        "source_id": record.get("sourceID", ""),
        "brand": record.get("brand", ""),
        "category": record.get("category", ""),
        "model": record.get("model", ""),
        "disabled": record.get("disabled", False),
        "service_id": record.get("service", {}).get("id") if record.get("service") else None,
        "service_name": record.get("service", {}).get("name") if record.get("service") else None,
        "support_team_id": (
            record.get("support_team", {}).get("id") if record.get("support_team") else None
        ),
        "support_team_name": (
            record.get("support_team", {}).get("name") if record.get("support_team") else None
        ),
        "financial_owner_id": (
            record.get("financial_owner", {}).get("id") if record.get("financial_owner") else None
        ),
        "supplier_id": record.get("supplier", {}).get("id") if record.get("supplier") else None,
        "useful_life": record.get("useful_life"),
        "depreciation_method": record.get("depreciation_method", ""),
        "remarks": record.get("remarks", ""),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_project_data(record):
    """
    Transform API response record to projects table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary for project.

    Returns:
        dict: Transformed project record ready for database insertion.
    """
    return {
        "id": record.get("id"),
        "subject": record.get("subject", ""),
        "source_id": record.get("sourceID", ""),
        "category": record.get("category", ""),
        "status": record.get("status", ""),
        "program": record.get("program", ""),
        "justification": record.get("justification", ""),
        "time_zone": record.get("time_zone", ""),
        "completion_target_at": record.get("completion_target_at"),
        "service_id": record.get("service", {}).get("id") if record.get("service") else None,
        "service_name": record.get("service", {}).get("name") if record.get("service") else None,
        "customer_id": record.get("customer", {}).get("id") if record.get("customer") else None,
        "customer_name": (
            record.get("customer", {}).get("name") if record.get("customer") else None
        ),
        "manager_id": record.get("manager", {}).get("id") if record.get("manager") else None,
        "manager_name": record.get("manager", {}).get("name") if record.get("manager") else None,
        "work_hours_id": (
            record.get("work_hours", {}).get("id") if record.get("work_hours") else None
        ),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
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
        initial_sync_days = __get_config_int(configuration, "initial_sync_days", 90)
        start_time = (datetime.now(timezone.utc) - timedelta(days=initial_sync_days)).isoformat()

    return {"start": start_time, "end": end_time}


def get_organizations(oauth_token, account_id, last_sync_time=None, configuration=None):
    """
    Fetch organizations using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        oauth_token: OAuth token for API authentication.
        account_id: Xurrent account ID for API access.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual organization records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/organizations"
    max_records = __get_config_int(
        configuration, "max_records_per_page", __DEFAULT_PAGE_SIZE, 1, __MAX_PAGE_SIZE
    )

    params = {"per_page": max_records, "page": 1}

    # Add incremental sync filter if available
    if last_sync_time:
        params["updated_at"] = f">{last_sync_time}"

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, oauth_token, account_id, params, configuration)

        data = response if isinstance(response, list) else response.get("data", [])
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_organization_data(record)

        if len(data) < max_records:
            break
        page += 1


def get_products(oauth_token, account_id, last_sync_time=None, configuration=None):
    """
    Fetch products using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        oauth_token: OAuth token for API authentication.
        account_id: Xurrent account ID for API access.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual product records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/products"
    max_records = __get_config_int(
        configuration, "max_records_per_page", __DEFAULT_PAGE_SIZE, 1, __MAX_PAGE_SIZE
    )

    params = {"per_page": max_records, "page": 1}

    # Add incremental sync filter if available
    if last_sync_time:
        params["updated_at"] = f">{last_sync_time}"

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, oauth_token, account_id, params, configuration)

        data = response if isinstance(response, list) else response.get("data", [])
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_product_data(record)

        if len(data) < max_records:
            break
        page += 1


def get_projects(oauth_token, account_id, last_sync_time=None, configuration=None):
    """
    Fetch projects using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        oauth_token: OAuth token for API authentication.
        account_id: Xurrent account ID for API access.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual project records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/projects"
    max_records = __get_config_int(
        configuration, "max_records_per_page", __DEFAULT_PAGE_SIZE, 1, __MAX_PAGE_SIZE
    )

    params = {"per_page": max_records, "page": 1}

    # Add incremental sync filter if available
    if last_sync_time:
        params["updated_at"] = f">{last_sync_time}"

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, oauth_token, account_id, params, configuration)

        data = response if isinstance(response, list) else response.get("data", [])
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_project_data(record)

        if len(data) < max_records:
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
        {"table": "organizations", "primary_key": ["id"]},
        {"table": "products", "primary_key": ["id"]},
        {"table": "projects", "primary_key": ["id"]},
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
    log.info("Starting Xurrent API connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    oauth_token = __get_config_str(configuration, "oauth_token")
    account_id = __get_config_str(configuration, "account_id")
    max_records_per_page = __get_config_int(
        configuration, "max_records_per_page", __DEFAULT_PAGE_SIZE, 1, __MAX_PAGE_SIZE
    )
    enable_incremental = __get_config_bool(configuration, "enable_incremental_sync", True)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time") if enable_incremental else None

    try:
        # Fetch organizations data using generator with incremental checkpointing
        log.info("Fetching organizations data...")
        org_count = 0
        org_page = 1

        for record in get_organizations(oauth_token, account_id, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="organizations", data=record)
            org_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if org_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": record.get(
                        "updated_at", datetime.now(timezone.utc).isoformat()
                    ),
                    "last_processed_organizations_page": org_page,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                org_page += 1

        # Fetch products data using generator with incremental checkpointing
        log.info("Fetching products data...")
        product_count = 0
        product_page = 1

        for record in get_products(oauth_token, account_id, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="products", data=record)
            product_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if product_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": record.get(
                        "updated_at", datetime.now(timezone.utc).isoformat()
                    ),
                    "last_processed_products_page": product_page,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                product_page += 1

        # Fetch projects data using generator with incremental checkpointing
        log.info("Fetching projects data...")
        project_count = 0
        project_page = 1

        for record in get_projects(oauth_token, account_id, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="projects", data=record)
            project_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if project_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": record.get(
                        "updated_at", datetime.now(timezone.utc).isoformat()
                    ),
                    "last_processed_projects_page": project_page,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                project_page += 1

        # Final checkpoint with completion status
        final_state = {"last_sync_time": datetime.now(timezone.utc).isoformat()}
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info(
            f"Sync completed successfully. Processed {org_count} organizations, {product_count} products, {project_count} projects."
        )

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Entry point for the connector
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
