"""Razorpay API connector for syncing orders, payments, settlements, and refunds data.
This connector demonstrates how to fetch data from Razorpay API and upsert it into destination using memory-efficient streaming patterns.
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

# For making HTTP requests to Razorpay API
import requests

# For handling dates and timestamps
from datetime import datetime, timedelta, timezone

# For implementing delays in retry logic and rate limiting
import time

# For adding jitter to retry delays
import random

# For base64 encoding of API key
import base64

""" ADD YOUR SOURCE-SPECIFIC IMPORTS HERE
Example: import pandas, boto3, etc.
Add comment for each import to explain its purpose for users to follow.
"""
# Private constants (use __ prefix)
__API_BASE_URL = "https://api.razorpay.com/v1"


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


def execute_api_request(endpoint, api_key, api_secret, params=None, configuration=None):
    """
    Execute HTTP API requests with comprehensive error handling and retry logic.
    This function handles authentication, rate limiting, timeouts, and network errors.

    Args:
        endpoint: API endpoint path to request.
        api_key: Razorpay API key for authentication.
        api_secret: Razorpay API secret for authentication.
        params: Query parameters for the request (optional).
        configuration: Configuration dictionary for timeout and retry settings.

    Returns:
        dict: Parsed JSON response from the API.

    Raises:
        RuntimeError: If all retry attempts fail or unexpected errors occur.
        requests.exceptions.RequestException: For unrecoverable HTTP errors.
    """
    url = f"{__API_BASE_URL}{endpoint}"

    # Create basic auth credentials
    credentials = f"{api_key}:{api_secret}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    headers = {"Authorization": f"Basic {encoded_credentials}", "Content-Type": "application/json"}

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
        dict: Dictionary containing 'start' and 'end' timestamps in Unix format.
    """
    end_time = int(datetime.now(timezone.utc).timestamp())

    if last_sync_time:
        # Convert ISO string back to Unix timestamp if needed
        if isinstance(last_sync_time, str):
            start_time = int(
                datetime.fromisoformat(last_sync_time.replace("Z", "+00:00")).timestamp()
            )
        else:
            start_time = int(last_sync_time)
    else:
        initial_sync_days = __get_config_int(configuration, "initial_sync_days", 90)
        start_time = int(
            (datetime.now(timezone.utc) - timedelta(days=initial_sync_days)).timestamp()
        )

    return {"start": start_time, "end": end_time}


def __map_order_data(record):
    """
    Transform API response record to orders table schema format.
    This function maps raw Razorpay order fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "entity": record.get("entity", ""),
        "amount": record.get("amount", 0),
        "amount_paid": record.get("amount_paid", 0),
        "amount_due": record.get("amount_due", 0),
        "currency": record.get("currency", ""),
        "receipt": record.get("receipt", ""),
        "offer_id": record.get("offer_id", ""),
        "status": record.get("status", ""),
        "attempts": record.get("attempts", 0),
        "notes": json.dumps(record.get("notes", {})),
        "created_at": record.get("created_at", 0),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_payment_data(record):
    """
    Transform API response record to payments table schema format.
    This function maps raw Razorpay payment fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "entity": record.get("entity", ""),
        "amount": record.get("amount", 0),
        "currency": record.get("currency", ""),
        "status": record.get("status", ""),
        "order_id": record.get("order_id", ""),
        "invoice_id": record.get("invoice_id", ""),
        "international": record.get("international", False),
        "method": record.get("method", ""),
        "amount_refunded": record.get("amount_refunded", 0),
        "refund_status": record.get("refund_status", ""),
        "captured": record.get("captured", False),
        "description": record.get("description", ""),
        "card_id": record.get("card_id", ""),
        "bank": record.get("bank", ""),
        "wallet": record.get("wallet", ""),
        "vpa": record.get("vpa", ""),
        "email": record.get("email", ""),
        "contact": record.get("contact", ""),
        "notes": json.dumps(record.get("notes", {})),
        "fee": record.get("fee", 0),
        "tax": record.get("tax", 0),
        "error_code": record.get("error_code", ""),
        "error_description": record.get("error_description", ""),
        "error_source": record.get("error_source", ""),
        "error_step": record.get("error_step", ""),
        "error_reason": record.get("error_reason", ""),
        "acquirer_data": json.dumps(record.get("acquirer_data", {})),
        "created_at": record.get("created_at", 0),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_settlement_data(record):
    """
    Transform API response record to settlements table schema format.
    This function maps raw Razorpay settlement fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "entity": record.get("entity", ""),
        "amount": record.get("amount", 0),
        "status": record.get("status", ""),
        "fees": record.get("fees", 0),
        "tax": record.get("tax", 0),
        "utr": record.get("utr", ""),
        "created_at": record.get("created_at", 0),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_refund_data(record):
    """
    Transform API response record to refunds table schema format.
    This function maps raw Razorpay refund fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "entity": record.get("entity", ""),
        "amount": record.get("amount", 0),
        "currency": record.get("currency", ""),
        "payment_id": record.get("payment_id", ""),
        "notes": json.dumps(record.get("notes", {})),
        "receipt": record.get("receipt", ""),
        "acquirer_data": json.dumps(record.get("acquirer_data", {})),
        "created_at": record.get("created_at", 0),
        "speed_processed": record.get("speed_processed", ""),
        "speed_requested": record.get("speed_requested", ""),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def get_orders(api_key, api_secret, last_sync_time=None, configuration=None):
    """
    Fetch orders data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        api_secret: API authentication secret for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual order records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/orders"
    max_records = __get_config_int(configuration, "max_records_per_page", 100, 1, 100)

    params = {
        "count": max_records,
        "skip": 0,
        "from": time_range["start"],
        "to": time_range["end"],
    }

    skip = 0
    while True:
        params["skip"] = skip
        response = execute_api_request(endpoint, api_key, api_secret, params, configuration)

        items = response.get("items", [])
        if not items:
            break

        # Yield individual records instead of accumulating
        for record in items:
            yield __map_order_data(record)

        if len(items) < max_records:
            break
        skip += max_records


def get_payments(api_key, api_secret, last_sync_time=None, configuration=None):
    """
    Fetch payments data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        api_secret: API authentication secret for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual payment records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/payments"
    max_records = __get_config_int(configuration, "max_records_per_page", 100, 1, 100)

    params = {
        "count": max_records,
        "skip": 0,
        "from": time_range["start"],
        "to": time_range["end"],
    }

    skip = 0
    while True:
        params["skip"] = skip
        response = execute_api_request(endpoint, api_key, api_secret, params, configuration)

        items = response.get("items", [])
        if not items:
            break

        # Yield individual records instead of accumulating
        for record in items:
            yield __map_payment_data(record)

        if len(items) < max_records:
            break
        skip += max_records


def get_settlements(api_key, api_secret, last_sync_time=None, configuration=None):
    """
    Fetch settlements data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        api_secret: API authentication secret for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual settlement records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/settlements"
    max_records = __get_config_int(configuration, "max_records_per_page", 100, 1, 100)

    params = {
        "count": max_records,
        "skip": 0,
        "from": time_range["start"],
        "to": time_range["end"],
    }

    skip = 0
    while True:
        params["skip"] = skip
        response = execute_api_request(endpoint, api_key, api_secret, params, configuration)

        items = response.get("items", [])
        if not items:
            break

        # Yield individual records instead of accumulating
        for record in items:
            yield __map_settlement_data(record)

        if len(items) < max_records:
            break
        skip += max_records


def get_refunds(api_key, api_secret, last_sync_time=None, configuration=None):
    """
    Fetch refunds data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        api_secret: API authentication secret for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual refund records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/refunds"
    max_records = __get_config_int(configuration, "max_records_per_page", 100, 1, 100)

    params = {
        "count": max_records,
        "skip": 0,
        "from": time_range["start"],
        "to": time_range["end"],
    }

    skip = 0
    while True:
        params["skip"] = skip
        response = execute_api_request(endpoint, api_key, api_secret, params, configuration)

        items = response.get("items", [])
        if not items:
            break

        # Yield individual records instead of accumulating
        for record in items:
            yield __map_refund_data(record)

        if len(items) < max_records:
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
        {"table": "orders", "primary_key": ["id"]},
        {"table": "payments", "primary_key": ["id"]},
        {"table": "settlements", "primary_key": ["id"]},
        {"table": "refunds", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Main synchronization function that fetches and processes data from the Razorpay API.
    This function orchestrates the entire sync process using memory-efficient streaming patterns.

    Args:
        configuration: Configuration dictionary containing API credentials and settings.
        state: State dictionary containing sync cursors and checkpoints from previous runs.

    Raises:
        RuntimeError: If sync fails due to API errors or configuration issues.
    """
    log.info("Starting Razorpay API connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    api_key = __get_config_str(configuration, "api_key")
    api_secret = __get_config_str(configuration, "api_secret")
    max_records_per_page = __get_config_int(configuration, "max_records_per_page", 100, 1, 100)
    enable_incremental = __get_config_bool(configuration, "enable_incremental_sync", True)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time") if enable_incremental else None

    try:
        # Fetch orders data
        log.info("Fetching orders data...")
        record_count = 0
        page = 1

        for record in get_orders(api_key, api_secret, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="orders", data=record)
            record_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if record_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": datetime.now(timezone.utc).isoformat(),
                    "last_processed_page": page,
                    "orders_processed": record_count,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                page += 1

        log.info(f"Fetched {record_count} orders")

        # Fetch payments data
        log.info("Fetching payments data...")
        payment_count = 0
        page = 1

        for record in get_payments(api_key, api_secret, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="payments", data=record)
            payment_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if payment_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": datetime.now(timezone.utc).isoformat(),
                    "last_processed_page": page,
                    "payments_processed": payment_count,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                page += 1

        log.info(f"Fetched {payment_count} payments")

        # Fetch settlements data
        log.info("Fetching settlements data...")
        settlement_count = 0
        page = 1

        for record in get_settlements(api_key, api_secret, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="settlements", data=record)
            settlement_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if settlement_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": datetime.now(timezone.utc).isoformat(),
                    "last_processed_page": page,
                    "settlements_processed": settlement_count,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                page += 1

        log.info(f"Fetched {settlement_count} settlements")

        # Fetch refunds data
        log.info("Fetching refunds data...")
        refund_count = 0
        page = 1

        for record in get_refunds(api_key, api_secret, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="refunds", data=record)
            refund_count += 1

            # Checkpoint every page/batch to save progress incrementally
            if refund_count % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": datetime.now(timezone.utc).isoformat(),
                    "last_processed_page": page,
                    "refunds_processed": refund_count,
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)
                page += 1

        log.info(f"Fetched {refund_count} refunds")

        # Final checkpoint with completion status
        final_state = {
            "last_sync_time": datetime.now(timezone.utc).isoformat(),
            "orders_total": record_count,
            "payments_total": payment_count,
            "settlements_total": settlement_count,
            "refunds_total": refund_count,
        }
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        total_records = record_count + payment_count + settlement_count + refund_count
        log.info(f"Sync completed successfully. Processed {total_records} total records.")

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Create a connector object with update and schema functions
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
