"""AroFlo API connector for syncing users, suppliers, and payments data.
This connector demonstrates how to fetch data from AroFlo API and upsert it into destination using memory-efficient streaming patterns.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json
# For adding jitter to retry delays
import random
# For making HTTP requests to AroFlo API
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
__API_ENDPOINT = "https://api.aroflo.com/v1"
__DEFAULT_PAGE_SIZE = 100
__DEFAULT_TIMEOUT = 30
__DEFAULT_RETRY_ATTEMPTS = 3
__DEFAULT_INITIAL_SYNC_DAYS = 90


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
        return value.lower() in ('true', '1', 'yes', 'on')
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
    wait_time = min(base_delay * (2 ** attempt), max_delay)
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
        log.warning(f"Request failed for {endpoint}: {str(error)}. Retrying in {wait_time:.1f} seconds...")
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

    timeout = __get_config_int(configuration, "request_timeout_seconds", __DEFAULT_TIMEOUT)
    retry_attempts = __get_config_int(configuration, "retry_attempts", __DEFAULT_RETRY_ATTEMPTS)

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
        initial_sync_days = __get_config_int(configuration, "initial_sync_days", __DEFAULT_INITIAL_SYNC_DAYS)
        start_time = (datetime.now(timezone.utc) - timedelta(days=initial_sync_days)).isoformat()

    return {"start": start_time, "end": end_time}


def __map_user_data(record):
    """
    Transform API response record to users table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": str(record.get("id", "")),
        "first_name": record.get("first_name", ""),
        "last_name": record.get("last_name", ""),
        "email": record.get("email", ""),
        "phone": record.get("phone", ""),
        "role": record.get("role", ""),
        "department": record.get("department", ""),
        "employee_id": record.get("employee_id", ""),
        "status": record.get("status", ""),
        "access_level": record.get("access_level", ""),
        "territory": record.get("territory", ""),
        "created_at": record.get("created_at", ""),
        "updated_at": record.get("updated_at", ""),
        "sync_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_supplier_data(record):
    """
    Transform API response record to suppliers table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": str(record.get("id", "")),
        "name": record.get("name", ""),
        "contact_person": record.get("contact_person", ""),
        "email": record.get("email", ""),
        "phone": record.get("phone", ""),
        "address": record.get("address", ""),
        "city": record.get("city", ""),
        "state": record.get("state", ""),
        "country": record.get("country", ""),
        "postal_code": record.get("postal_code", ""),
        "payment_terms": record.get("payment_terms", ""),
        "tax_id": record.get("tax_id", ""),
        "account_code": record.get("account_code", ""),
        "status": record.get("status", ""),
        "created_at": record.get("created_at", ""),
        "updated_at": record.get("updated_at", ""),
        "sync_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_payment_data(record):
    """
    Transform API response record to payments table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": str(record.get("id", "")),
        "amount": record.get("amount", 0),
        "currency": record.get("currency", ""),
        "payment_date": record.get("payment_date", ""),
        "payment_method": record.get("payment_method", ""),
        "status": record.get("status", ""),
        "reference_number": record.get("reference_number", ""),
        "customer_id": record.get("customer_id", ""),
        "invoice_id": record.get("invoice_id", ""),
        "job_id": record.get("job_id", ""),
        "transaction_type": record.get("transaction_type", ""),
        "notes": record.get("notes", ""),
        "created_at": record.get("created_at", ""),
        "updated_at": record.get("updated_at", ""),
        "sync_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_users_data(api_key, last_sync_time=None, configuration=None):
    """
    Fetch users data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual user records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/users"
    max_records = __get_config_int(configuration, "max_records_per_page", __DEFAULT_PAGE_SIZE, 1, 1000)
    time_range = get_time_range(last_sync_time, configuration)

    params = {
        "per_page": max_records,
        "page": 1,
    }

    # Add incremental sync parameter if supported
    if last_sync_time:
        params["modified_since"] = time_range["start"]

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        data = response.get("data", response.get("users", []))
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_user_data(record)

        if len(data) < max_records:
            break
        page += 1


def get_suppliers_data(api_key, last_sync_time=None, configuration=None):
    """
    Fetch suppliers data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual supplier records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/suppliers"
    max_records = __get_config_int(configuration, "max_records_per_page", __DEFAULT_PAGE_SIZE, 1, 1000)
    time_range = get_time_range(last_sync_time, configuration)

    params = {
        "per_page": max_records,
        "page": 1,
    }

    # Add incremental sync parameter if supported
    if last_sync_time:
        params["modified_since"] = time_range["start"]

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        data = response.get("data", response.get("suppliers", []))
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_supplier_data(record)

        if len(data) < max_records:
            break
        page += 1


def get_payments_data(api_key, last_sync_time=None, configuration=None):
    """
    Fetch payments data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual payment records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/payments"
    max_records = __get_config_int(configuration, "max_records_per_page", __DEFAULT_PAGE_SIZE, 1, 1000)
    time_range = get_time_range(last_sync_time, configuration)

    params = {
        "per_page": max_records,
        "page": 1,
    }

    # Add incremental sync parameter if supported
    if last_sync_time:
        params["modified_since"] = time_range["start"]

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        data = response.get("data", response.get("payments", []))
        if not data:
            break

        # Yield individual records instead of accumulating
        for record in data:
            yield __map_payment_data(record)

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
        {
            "table": "users",
            "primary_key": ["id"]
        },
        {
            "table": "suppliers",
            "primary_key": ["id"]
        },
        {
            "table": "payments",
            "primary_key": ["id"]
        }
    ]


def update(configuration: dict, state: dict):
    """
    Main synchronization function that fetches and processes data from the AroFlo API.
    This function orchestrates the entire sync process using memory-efficient streaming patterns.

    Args:
        configuration: Configuration dictionary containing API credentials and settings.
        state: State dictionary containing sync cursors and checkpoints from previous runs.

    Raises:
        RuntimeError: If sync fails due to API errors or configuration issues.
    """
    log.info("Starting AroFlo API connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    api_key = __get_config_str(configuration, "api_key")
    max_records_per_page = __get_config_int(configuration, "max_records_per_page", __DEFAULT_PAGE_SIZE, 1, 1000)
    enable_users = __get_config_bool(configuration, "enable_users_sync", True)
    enable_suppliers = __get_config_bool(configuration, "enable_suppliers_sync", True)
    enable_payments = __get_config_bool(configuration, "enable_payments_sync", True)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time")

    try:
        # Fetch users data using generator with incremental checkpointing
        if enable_users:
            log.info("Fetching users data...")
            user_count = 0
            page = 1

            for record in get_users_data(api_key, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="users", data=record)
                user_count += 1

                # Checkpoint every page/batch to save progress incrementally
                if user_count % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": record.get("updated_at", datetime.now(timezone.utc).isoformat()),
                        "last_processed_users_page": page
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)
                    page += 1

            log.info(f"Users sync completed. Processed {user_count} records.")

        # Fetch suppliers data if enabled
        if enable_suppliers:
            log.info("Fetching suppliers data...")
            supplier_count = 0
            page = 1

            for record in get_suppliers_data(api_key, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="suppliers", data=record)
                supplier_count += 1

                # Checkpoint every page/batch to save progress incrementally
                if supplier_count % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": record.get("updated_at", datetime.now(timezone.utc).isoformat()),
                        "last_processed_suppliers_page": page
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)
                    page += 1

            log.info(f"Suppliers sync completed. Processed {supplier_count} records.")

        # Fetch payments data if enabled
        if enable_payments:
            log.info("Fetching payments data...")
            payment_count = 0
            page = 1

            for record in get_payments_data(api_key, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="payments", data=record)
                payment_count += 1

                # Checkpoint every page/batch to save progress incrementally
                if payment_count % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": record.get("updated_at", datetime.now(timezone.utc).isoformat()),
                        "last_processed_payments_page": page
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)
                    page += 1

            log.info(f"Payments sync completed. Processed {payment_count} records.")

        # Final checkpoint with completion status
        final_state = {"last_sync_time": datetime.now(timezone.utc).isoformat()}
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info("AroFlo API sync completed successfully.")

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Create connector instance with schema and update functions
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
