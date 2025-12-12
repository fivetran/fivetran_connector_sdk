"""Shipwire API connector for syncing orders, purchase orders, and products data.
This connector demonstrates how to fetch data from Shipwire API and upsert it into destination using memory-efficient streaming patterns.
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

# For making HTTP requests to Shipwire API
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
__API_ENDPOINT = "https://api.shipwire.com/api/v3"


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


def execute_api_request(endpoint, username, password, params=None, configuration=None):
    """
    Execute HTTP API requests with comprehensive error handling and retry logic.
    This function handles authentication, rate limiting, timeouts, and network errors.

    Args:
        endpoint: API endpoint path to request.
        username: Shipwire API username for Basic authentication.
        password: Shipwire API password for Basic authentication.
        params: Query parameters for the request (optional).
        configuration: Configuration dictionary for timeout and retry settings.

    Returns:
        dict: Parsed JSON response from the API.

    Raises:
        RuntimeError: If all retry attempts fail or unexpected errors occur.
        requests.exceptions.RequestException: For unrecoverable HTTP errors.
    """
    url = f"{__API_ENDPOINT}{endpoint}"
    headers = {"Content-Type": "application/json"}

    timeout = __get_config_int(configuration, "request_timeout_seconds", 30)
    retry_attempts = __get_config_int(configuration, "retry_attempts", 3)

    for attempt in range(retry_attempts):
        try:
            response = requests.get(
                url, auth=(username, password), headers=headers, params=params, timeout=timeout
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


def __map_order_data(order_record):
    """
    Transform Shipwire order response record to destination table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        order_record: Raw Shipwire order response record dictionary.

    Returns:
        dict: Transformed order record ready for database insertion.
    """
    return {
        "id": order_record.get("id", ""),
        "external_id": order_record.get("externalId", ""),
        "order_number": order_record.get("orderNumber", ""),
        "status": order_record.get("status", ""),
        "holds": order_record.get("holds", []),
        "total_value": order_record.get("totalValue", 0),
        "currency": order_record.get("currency", ""),
        "ship_to": order_record.get("shipTo", {}),
        "items": order_record.get("items", []),
        "options": order_record.get("options", {}),
        "created_date": order_record.get("createdDate", ""),
        "updated_date": order_record.get("updatedDate", ""),
        "processed_date": order_record.get("processedDate", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_purchase_order_data(po_record):
    """
    Transform Shipwire purchase order response record to destination table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        po_record: Raw Shipwire purchase order response record dictionary.

    Returns:
        dict: Transformed purchase order record ready for database insertion.
    """
    return {
        "id": po_record.get("id", ""),
        "external_id": po_record.get("externalId", ""),
        "po_number": po_record.get("poNumber", ""),
        "status": po_record.get("status", ""),
        "warehouse_id": po_record.get("warehouseId", ""),
        "warehouse_external_id": po_record.get("warehouseExternalId", ""),
        "vendor_id": po_record.get("vendorId", ""),
        "vendor_external_id": po_record.get("vendorExternalId", ""),
        "expected_date": po_record.get("expectedDate", ""),
        "created_date": po_record.get("createdDate", ""),
        "updated_date": po_record.get("updatedDate", ""),
        "items": po_record.get("items", []),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_product_data(product_record):
    """
    Transform Shipwire product response record to destination table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        product_record: Raw Shipwire product response record dictionary.

    Returns:
        dict: Transformed product record ready for database insertion.
    """
    return {
        "id": product_record.get("id", ""),
        "sku": product_record.get("sku", ""),
        "external_id": product_record.get("externalId", ""),
        "description": product_record.get("description", ""),
        "status": product_record.get("status", ""),
        "category": product_record.get("category", ""),
        "classification": product_record.get("classification", ""),
        "batteryConfiguration": product_record.get("batteryConfiguration", {}),
        "dimensions": product_record.get("dimensions", {}),
        "values": product_record.get("values", {}),
        "flags": product_record.get("flags", {}),
        "created_date": product_record.get("createdDate", ""),
        "updated_date": product_record.get("updatedDate", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_orders(username, password, last_sync_time=None, configuration=None):
    """
    Fetch orders data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        username: Shipwire API username for authentication.
        password: Shipwire API password for authentication.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual order records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/orders"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    params = {
        "limit": max_records,
        "offset": 0,
        "updatedAfter": time_range["start"],
        "updatedBefore": time_range["end"],
        "expand": "items,shipTo,options,holds",
    }

    offset = 0
    while True:
        params["offset"] = offset
        response = execute_api_request(endpoint, username, password, params, configuration)

        resource_data = response.get("resource", {})
        items = resource_data.get("items", [])

        if not items:
            break

        # Yield individual records instead of accumulating
        for order in items:
            yield __map_order_data(order)

        if len(items) < max_records:
            break
        offset += max_records


def get_purchase_orders(username, password, last_sync_time=None, configuration=None):
    """
    Fetch purchase orders data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        username: Shipwire API username for authentication.
        password: Shipwire API password for authentication.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual purchase order records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/purchaseOrders"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    params = {
        "limit": max_records,
        "offset": 0,
        "updatedAfter": time_range["start"],
        "updatedBefore": time_range["end"],
        "expand": "items",
    }

    offset = 0
    while True:
        params["offset"] = offset
        response = execute_api_request(endpoint, username, password, params, configuration)

        resource_data = response.get("resource", {})
        items = resource_data.get("items", [])

        if not items:
            break

        # Yield individual records instead of accumulating
        for po in items:
            yield __map_purchase_order_data(po)

        if len(items) < max_records:
            break
        offset += max_records


def get_products(username, password, last_sync_time=None, configuration=None):
    """
    Fetch products data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        username: Shipwire API username for authentication.
        password: Shipwire API password for authentication.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual product records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/products"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    params = {
        "limit": max_records,
        "offset": 0,
        "updatedAfter": time_range["start"],
        "updatedBefore": time_range["end"],
        "expand": "dimensions,values,flags,batteryConfiguration",
    }

    offset = 0
    while True:
        params["offset"] = offset
        response = execute_api_request(endpoint, username, password, params, configuration)

        resource_data = response.get("resource", {})
        items = resource_data.get("items", [])

        if not items:
            break

        # Yield individual records instead of accumulating
        for product in items:
            yield __map_product_data(product)

        if len(items) < max_records:
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
        {"table": "orders", "primary_key": ["id"]},
        {"table": "purchase_orders", "primary_key": ["id"]},
        {"table": "products", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Main synchronization function that fetches and processes data from the Shipwire API.
    This function orchestrates the entire sync process using memory-efficient streaming patterns.

    Args:
        configuration: Configuration dictionary containing API credentials and settings.
        state: State dictionary containing sync cursors and checkpoints from previous runs.

    Raises:
        RuntimeError: If sync fails due to API errors or configuration issues.
    """
    log.info("Starting Shipwire API connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    username = __get_config_str(configuration, "username")
    password = __get_config_str(configuration, "password")
    max_records_per_page = __get_config_int(configuration, "max_records_per_page", 100, 1, 1000)
    enable_orders = __get_config_bool(configuration, "enable_orders", True)
    enable_purchase_orders = __get_config_bool(configuration, "enable_purchase_orders", True)
    enable_products = __get_config_bool(configuration, "enable_products", True)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time")

    try:
        total_records = 0

        # Fetch orders data if enabled
        if enable_orders:
            log.info("Fetching orders data...")
            orders_count = 0

            for order in get_orders(username, password, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="orders", data=order)
                orders_count += 1
                total_records += 1

                # Checkpoint every page/batch to save progress incrementally
                if orders_count % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": order.get(
                            "updated_date", datetime.now(timezone.utc).isoformat()
                        ),
                        "last_processed_orders": orders_count,
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)

            log.info(f"Processed {orders_count} orders")

        # Fetch purchase orders data if enabled
        if enable_purchase_orders:
            log.info("Fetching purchase orders data...")
            po_count = 0

            for po in get_purchase_orders(username, password, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="purchase_orders", data=po)
                po_count += 1
                total_records += 1

                # Checkpoint every page/batch to save progress incrementally
                if po_count % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": po.get(
                            "updated_date", datetime.now(timezone.utc).isoformat()
                        ),
                        "last_processed_purchase_orders": po_count,
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)

            log.info(f"Processed {po_count} purchase orders")

        # Fetch products data if enabled
        if enable_products:
            log.info("Fetching products data...")
            products_count = 0

            for product in get_products(username, password, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="products", data=product)
                products_count += 1
                total_records += 1

                # Checkpoint every page/batch to save progress incrementally
                if products_count % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": product.get(
                            "updated_date", datetime.now(timezone.utc).isoformat()
                        ),
                        "last_processed_products": products_count,
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)

            log.info(f"Processed {products_count} products")

        # Final checkpoint with completion status
        final_state = {"last_sync_time": datetime.now(timezone.utc).isoformat()}
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info(f"Sync completed successfully. Processed {total_records} total records.")

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Create the connector object with the update and schema functions
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
