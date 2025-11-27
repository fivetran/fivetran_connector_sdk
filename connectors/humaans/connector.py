"""Humaans connector for syncing companies, documents, and job-roles data.
This connector demonstrates how to fetch data from Humaans API and upsert it into destination using memory-efficient streaming patterns.
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

# For making HTTP requests to Humaans API
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
__API_ENDPOINT = "https://app.humaans.io/api"


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


def execute_api_request(endpoint, api_token, params=None, configuration=None):
    """
    Execute HTTP API requests with comprehensive error handling and retry logic.
    This function handles authentication, rate limiting, timeouts, and network errors.

    Args:
        endpoint: API endpoint path to request.
        api_token: Bearer token for API access.
        params: Query parameters for the request (optional).
        configuration: Configuration dictionary for timeout and retry settings.

    Returns:
        dict: Parsed JSON response from the API.

    Raises:
        RuntimeError: If all retry attempts fail or unexpected errors occur.
        requests.exceptions.RequestException: For unrecoverable HTTP errors.
    """
    url = f"{__API_ENDPOINT}{endpoint}"
    headers = {"Authorization": f"Bearer {api_token}"}

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


def __map_company_data(record):
    """
    Transform company API response record to destination table schema format.
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
        "website": record.get("website", ""),
        "phone": record.get("phone", ""),
        "address_line1": record.get("address", {}).get("line1", ""),
        "address_line2": record.get("address", {}).get("line2", ""),
        "address_city": record.get("address", {}).get("city", ""),
        "address_state": record.get("address", {}).get("state", ""),
        "address_postal_code": record.get("address", {}).get("postalCode", ""),
        "address_country": record.get("address", {}).get("country", ""),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_document_data(record):
    """
    Transform document API response record to destination table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "person_id": record.get("personId", ""),
        "name": record.get("name", ""),
        "description": record.get("description", ""),
        "type": record.get("type", ""),
        "filename": record.get("filename", ""),
        "file_size": record.get("fileSize", 0),
        "mime_type": record.get("mimeType", ""),
        "url": record.get("url", ""),
        "expiry_date": record.get("expiryDate", ""),
        "tags": json.dumps(record.get("tags", [])),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_job_role_data(record):
    """
    Transform job role API response record to destination table schema format.
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
        "department": record.get("department", ""),
        "location": record.get("location", ""),
        "employment_type": record.get("employmentType", ""),
        "salary_range_min": record.get("salaryRange", {}).get("min", 0),
        "salary_range_max": record.get("salaryRange", {}).get("max", 0),
        "salary_currency": record.get("salaryRange", {}).get("currency", ""),
        "requirements": json.dumps(record.get("requirements", [])),
        "responsibilities": json.dumps(record.get("responsibilities", [])),
        "benefits": json.dumps(record.get("benefits", [])),
        "is_active": record.get("isActive", False),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_companies(api_token, last_sync_time=None, configuration=None):
    """
    Fetch companies data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_token: API authentication token for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual company records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/companies"
    max_records = __get_config_int(configuration, "max_records_per_page", 100, 1, 250)

    skip = 0
    while True:
        params = {"$limit": max_records, "$skip": skip}

        response = execute_api_request(endpoint, api_token, params, configuration)

        data = response.get("data", [])
        if not data:
            break

        for record in data:
            yield __map_company_data(record)

        if len(data) < max_records:
            break
        skip += max_records


def get_documents(api_token, last_sync_time=None, configuration=None):
    """
    Fetch documents data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_token: API authentication token for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual document records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/documents"
    max_records = __get_config_int(configuration, "max_records_per_page", 100, 1, 250)

    skip = 0
    while True:
        params = {"$limit": max_records, "$skip": skip}

        response = execute_api_request(endpoint, api_token, params, configuration)

        data = response.get("data", [])
        if not data:
            break

        for record in data:
            yield __map_document_data(record)

        if len(data) < max_records:
            break
        skip += max_records


def get_job_roles(api_token, last_sync_time=None, configuration=None):
    """
    Fetch job roles data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_token: API authentication token for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual job role records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/job-roles"
    max_records = __get_config_int(configuration, "max_records_per_page", 100, 1, 250)

    skip = 0
    while True:
        params = {"$limit": max_records, "$skip": skip}

        response = execute_api_request(endpoint, api_token, params, configuration)

        data = response.get("data", [])
        if not data:
            break

        for record in data:
            yield __map_job_role_data(record)

        if len(data) < max_records:
            break
        skip += max_records


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
        {"table": "companies", "primary_key": ["id"]},
        {"table": "documents", "primary_key": ["id"]},
        {"table": "job_roles", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Main synchronization function that fetches and processes data from the Humaans API.
    This function orchestrates the entire sync process using memory-efficient streaming patterns.

    Args:
        configuration: Configuration dictionary containing API credentials and settings.
        state: State dictionary containing sync cursors and checkpoints from previous runs.

    Raises:
        RuntimeError: If sync fails due to API errors or configuration issues.
    """
    log.info("Starting Humaans connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    api_token = __get_config_str(configuration, "api_token")
    max_records_per_page = __get_config_int(configuration, "max_records_per_page", 100, 1, 250)
    enable_incremental = __get_config_bool(configuration, "enable_incremental_sync", True)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time") if enable_incremental else None

    try:
        # Fetch companies data
        log.info("Fetching companies data...")
        companies_count = 0
        companies_page = 1

        for record in get_companies(api_token, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="companies", data=record)
            companies_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if companies_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": record.get(
                        "updated_at", datetime.now(timezone.utc).isoformat()
                    ),
                    "last_processed_companies_page": companies_page,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                companies_page += 1

        log.info(f"Processed {companies_count} companies")

        # Fetch documents data
        log.info("Fetching documents data...")
        documents_count = 0
        documents_page = 1

        for record in get_documents(api_token, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="documents", data=record)
            documents_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if documents_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": record.get(
                        "updated_at", datetime.now(timezone.utc).isoformat()
                    ),
                    "last_processed_documents_page": documents_page,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                documents_page += 1

        log.info(f"Processed {documents_count} documents")

        # Fetch job roles data
        log.info("Fetching job roles data...")
        job_roles_count = 0
        job_roles_page = 1

        for record in get_job_roles(api_token, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="job_roles", data=record)
            job_roles_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if job_roles_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": record.get(
                        "updated_at", datetime.now(timezone.utc).isoformat()
                    ),
                    "last_processed_job_roles_page": job_roles_page,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                job_roles_page += 1

        log.info(f"Processed {job_roles_count} job roles")

        # Final checkpoint with completion status
        final_state = {"last_sync_time": datetime.now(timezone.utc).isoformat()}
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        total_records = companies_count + documents_count + job_roles_count
        log.info(f"Sync completed successfully. Processed {total_records} total records.")

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Create an instance of the Connector
# The Connector is callable and takes 2 parameters:
# The first is the "update" function. This function is called each time Fivetran wants to update the data.
# The second is the "schema" function. This function is called when Fivetran needs to know the schema.
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
