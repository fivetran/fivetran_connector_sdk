"""Kustomer API connector for syncing data including customers, companies, brands, and messages.
This connector demonstrates how to fetch data from Kustomer API and upsert it into destination using memory-efficient streaming patterns.
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

# For making HTTP requests to Kustomer API
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
__API_BASE_URL = "https://api.kustomerapp.com/v1"  # Standard Kustomer API base URL


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
        response_headers: HTTP response headers dictionary that may contain rate limit info.
        base_delay: Base delay in seconds for exponential backoff.
        max_delay: Maximum delay cap in seconds.

    Returns:
        float: Wait time in seconds before next retry attempt.
    """
    # Check for Kustomer-specific rate limit headers
    if "x-ratelimit-reset" in response_headers:
        try:
            reset_time = int(response_headers["x-ratelimit-reset"])
            return min(reset_time, max_delay)
        except (ValueError, TypeError):
            pass

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
    remaining = response.headers.get("x-ratelimit-remaining", "unknown")
    reset_time = response.headers.get("x-ratelimit-reset", "unknown")

    log.warning(
        f"Rate limit hit. Remaining: {remaining}, Reset: {reset_time}. "
        f"Waiting {wait_time:.1f} seconds before retry {attempt + 1}"
    )
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
            f"Request failed for {endpoint}: {str(error)}. "
            f"Retrying in {wait_time:.1f} seconds..."
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
    url = f"{__API_BASE_URL}{endpoint}"
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


def __map_customer_data(record):
    """
    Transform API response record to destination table schema format for customers.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary for customer data.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "name": record.get("name", ""),
        "email": record.get("email", ""),
        "phone": record.get("phone", ""),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "company_id": record.get("companyId", ""),
        "external_id": record.get("externalId", ""),
        "verified": record.get("verified", False),
        "locked": record.get("locked", False),
        "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_company_data(record):
    """
    Transform API response record to destination table schema format for companies.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary for company data.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "name": record.get("name", ""),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "external_id": record.get("externalId", ""),
        "website": record.get("website", ""),
        "domains": json.dumps(record.get("domains", [])),
        "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_brand_data(record):
    """
    Transform API response record to destination table schema format for brands.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary for brand data.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "name": record.get("name", ""),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "display_name": record.get("displayName", ""),
        "is_default": record.get("isDefault", False),
        "website_url": record.get("websiteUrl", ""),
        "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_message_data(record):
    """
    Transform API response record to destination table schema format for messages.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary for message data.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "conversation_id": record.get("conversationId", ""),
        "customer_id": record.get("customerId", ""),
        "channel": record.get("channel", ""),
        "direction": record.get("direction", ""),
        "body": record.get("body", ""),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "status": record.get("status", ""),
        "message_type": record.get("type", ""),
        "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_customers(api_key, last_sync_time=None, configuration=None):
    """
    Fetch customer data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual customer records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/customers"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    params = {
        "page": 1,
        "pageSize": max_records,
    }

    # Add time filtering if incremental sync
    if last_sync_time:
        params["updatedAt"] = f"gte:{last_sync_time}"

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        data = response.get("data", [])
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_customer_data(record)

        # Check if we've reached the last page
        if len(data) < max_records:
            break
        page += 1


def get_companies(api_key, last_sync_time=None, configuration=None):
    """
    Fetch company data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual company records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/companies"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    params = {
        "page": 1,
        "pageSize": max_records,
    }

    # Add time filtering if incremental sync
    if last_sync_time:
        params["updatedAt"] = f"gte:{last_sync_time}"

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        data = response.get("data", [])
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_company_data(record)

        # Check if we've reached the last page
        if len(data) < max_records:
            break
        page += 1


def get_brands(api_key, configuration=None):
    """
    Fetch brand data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual brand records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/brands"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    params = {
        "page": 1,
        "pageSize": max_records,
    }

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        data = response.get("data", [])
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_brand_data(record)

        # Check if we've reached the last page
        if len(data) < max_records:
            break
        page += 1


def get_messages(api_key, last_sync_time=None, configuration=None):
    """
    Fetch message data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual message records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/messages"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    params = {
        "page": 1,
        "pageSize": max_records,
    }

    # Add time filtering if incremental sync
    if last_sync_time:
        params["updatedAt"] = f"gte:{last_sync_time}"

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        data = response.get("data", [])
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_message_data(record)

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
        {"table": "customers", "primary_key": ["id"]},
        {"table": "companies", "primary_key": ["id"]},
        {"table": "brands", "primary_key": ["id"]},
        {"table": "messages", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """

    log.info("Starting Kustomer API connector sync")

    # Extract configuration parameters as required
    api_key = __get_config_str(configuration, "api_key")
    max_records_per_page = __get_config_int(configuration, "max_records_per_page", 100, 1, 1000)
    enable_incremental = __get_config_bool(configuration, "enable_incremental_sync", True)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time") if enable_incremental else None

    try:
        # Fetch customers data using generator with incremental checkpointing
        log.info("Fetching customers data...")
        customer_count = 0
        customer_page = 1

        for record in get_customers(api_key, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="customers", data=record)
            customer_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if customer_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": record.get(
                        "updated_at", datetime.now(timezone.utc).isoformat()
                    ),
                    "last_processed_customers_page": customer_page,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                customer_page += 1

        log.info(f"Completed customers sync. Processed {customer_count} records.")

        # Fetch companies data
        log.info("Fetching companies data...")
        company_count = 0
        company_page = 1

        for record in get_companies(api_key, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="companies", data=record)
            company_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if company_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": record.get(
                        "updated_at", datetime.now(timezone.utc).isoformat()
                    ),
                    "last_processed_companies_page": company_page,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                company_page += 1

        log.info(f"Completed companies sync. Processed {company_count} records.")

        # Fetch brands data (usually small dataset, but still use streaming)
        log.info("Fetching brands data...")
        brand_count = 0

        for record in get_brands(api_key, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="brands", data=record)
            brand_count += 1

        log.info(f"Completed brands sync. Processed {brand_count} records.")

        # Fetch messages data
        log.info("Fetching messages data...")
        message_count = 0
        message_page = 1

        for record in get_messages(api_key, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="messages", data=record)
            message_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if message_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": record.get(
                        "updated_at", datetime.now(timezone.utc).isoformat()
                    ),
                    "last_processed_messages_page": message_page,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                message_page += 1

        log.info(f"Completed messages sync. Processed {message_count} records.")

        # Final checkpoint with completion status
        final_state = {
            "last_sync_time": datetime.now(timezone.utc).isoformat(),
            "total_customers": customer_count,
            "total_companies": company_count,
            "total_brands": brand_count,
            "total_messages": message_count,
        }
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        total_records = customer_count + company_count + brand_count + message_count
        log.info(f"Sync completed successfully. Total records processed: {total_records}")

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Creating an instance of the Connector class with the update and schema functions
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
