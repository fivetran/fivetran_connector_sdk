"""Planday HR API connector for syncing employee, department, and skills data.
This connector demonstrates how to fetch data from Planday HR API and upsert it into destination using memory-efficient streaming patterns.
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

# For making HTTP requests to Planday HR API
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
__API_ENDPOINT = "https://openapi.planday.com/api/hr"  # Planday HR API base URL


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


def __map_employee_data(record):
    """
    Transform API response record to employee table schema format.
    This function maps raw API employee fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary for employee data.

    Returns:
        dict: Transformed employee record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "employee_number": record.get("employeeNumber", ""),
        "first_name": record.get("firstName", ""),
        "last_name": record.get("lastName", ""),
        "email": record.get("email", ""),
        "phone": record.get("phone", ""),
        "department_id": record.get("departmentId", ""),
        "position": record.get("position", ""),
        "hire_date": record.get("hireDate", ""),
        "employment_type": record.get("employmentType", ""),
        "status": record.get("status", ""),
        "manager_id": record.get("managerId", ""),
        "hourly_rate": record.get("hourlyRate", 0.0),
        "salary": record.get("salary", 0.0),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_department_data(record):
    """
    Transform API response record to department table schema format.
    This function maps raw API department fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary for department data.

    Returns:
        dict: Transformed department record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "name": record.get("name", ""),
        "description": record.get("description", ""),
        "manager_id": record.get("managerId", ""),
        "parent_department_id": record.get("parentDepartmentId", ""),
        "location": record.get("location", ""),
        "budget": record.get("budget", 0.0),
        "cost_center": record.get("costCenter", ""),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_skill_data(record):
    """
    Transform API response record to skill table schema format.
    This function maps raw API skill fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary for skill data.

    Returns:
        dict: Transformed skill record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "name": record.get("name", ""),
        "description": record.get("description", ""),
        "category": record.get("category", ""),
        "level": record.get("level", ""),
        "certification_required": record.get("certificationRequired", False),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
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


def get_employees(api_key, params, last_sync_time=None, configuration=None):
    """
    Fetch employee data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        params: Additional parameters for the API request.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual employee records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/employees"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    api_params = {
        "limit": max_records,
        "offset": 0,
        "updatedAfter": time_range["start"],
        "updatedBefore": time_range["end"],
    }
    api_params.update(params)

    offset = 0
    while True:
        api_params["offset"] = offset
        response = execute_api_request(endpoint, api_key, api_params, configuration)

        data = response.get("data", [])
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_employee_data(record)

        if len(data) < max_records:
            break
        offset += max_records


def get_departments(api_key, params, last_sync_time=None, configuration=None):
    """
    Fetch department data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        params: Additional parameters for the API request.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual department records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/departments"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    api_params = {
        "limit": max_records,
        "offset": 0,
        "updatedAfter": time_range["start"],
        "updatedBefore": time_range["end"],
    }
    api_params.update(params)

    offset = 0
    while True:
        api_params["offset"] = offset
        response = execute_api_request(endpoint, api_key, api_params, configuration)

        data = response.get("data", [])
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_department_data(record)

        if len(data) < max_records:
            break
        offset += max_records


def get_skills(api_key, params, last_sync_time=None, configuration=None):
    """
    Fetch skills data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        params: Additional parameters for the API request.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual skill records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/skills"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    api_params = {
        "limit": max_records,
        "offset": 0,
        "updatedAfter": time_range["start"],
        "updatedBefore": time_range["end"],
    }
    api_params.update(params)

    offset = 0
    while True:
        api_params["offset"] = offset
        response = execute_api_request(endpoint, api_key, api_params, configuration)

        data = response.get("data", [])
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_skill_data(record)

        if len(data) < max_records:
            break
        offset += max_records


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
        {"table": "employees", "primary_key": ["id"]},
        {"table": "departments", "primary_key": ["id"]},
        {"table": "skills", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Main synchronization function that fetches and processes data from the Planday HR API.
    This function orchestrates the entire sync process using memory-efficient streaming patterns.

    Args:
        configuration: Configuration dictionary containing API credentials and settings.
        state: State dictionary containing sync cursors and checkpoints from previous runs.

    Raises:
        RuntimeError: If sync fails due to API errors or configuration issues.
    """
    log.info("Starting Planday HR connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    api_key = __get_config_str(configuration, "api_key")
    max_records_per_page = __get_config_int(configuration, "max_records_per_page", 100, 1, 1000)
    enable_incremental = __get_config_bool(configuration, "enable_incremental_sync", True)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time") if enable_incremental else None

    try:
        # Fetch employees data using generator with incremental checkpointing
        log.info("Fetching employee data...")
        employee_count = 0
        employee_page = 1

        for employee in get_employees(api_key, {}, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="employees", data=employee)
            employee_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if employee_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": employee.get(
                        "updated_at", datetime.now(timezone.utc).isoformat()
                    ),
                    "last_processed_employee_page": employee_page,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                employee_page += 1

        # Fetch departments data
        log.info("Fetching department data...")
        department_count = 0

        for department in get_departments(api_key, {}, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="departments", data=department)
            department_count += 1

        # Fetch skills data
        log.info("Fetching skills data...")
        skill_count = 0

        for skill in get_skills(api_key, {}, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="skills", data=skill)
            skill_count += 1

        # Final checkpoint with completion status
        final_state = {"last_sync_time": datetime.now(timezone.utc).isoformat()}
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info(
            f"Sync completed successfully. Processed {employee_count} employees, {department_count} departments, {skill_count} skills."
        )

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


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
