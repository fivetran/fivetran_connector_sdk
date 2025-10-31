"""Uber Eats API connector for syncing data including stores, orders, promotions, and menus.
This connector demonstrates how to fetch data from Uber Eats API and upsert it into destination using memory-efficient streaming patterns.
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

# For making HTTP requests to Uber Eats API
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
__API_ENDPOINT = "https://api.uber.com/v2/eats"  # Base API URL for Uber Eats


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


def __map_store_data(record):
    """
    Transform API response record to stores table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "name": record.get("name", ""),
        "slug": record.get("slug", ""),
        "address": record.get("address", {}).get("formatted", ""),
        "city": record.get("address", {}).get("city", ""),
        "state": record.get("address", {}).get("state", ""),
        "postal_code": record.get("address", {}).get("postal_code", ""),
        "country": record.get("address", {}).get("country", ""),
        "latitude": record.get("location", {}).get("latitude"),
        "longitude": record.get("location", {}).get("longitude"),
        "phone": record.get("phone", ""),
        "cuisine_type": record.get("cuisine_type", ""),
        "rating": record.get("rating"),
        "review_count": record.get("review_count", 0),
        "delivery_fee": record.get("delivery_fee"),
        "minimum_order": record.get("minimum_order"),
        "eta_range": json.dumps(record.get("eta_range", {})),
        "is_active": record.get("is_active", True),
        "created_at": record.get("created_at", ""),
        "updated_at": record.get("updated_at", ""),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_order_data(record):
    """
    Transform API response record to orders table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "store_id": record.get("store_id", ""),
        "customer_id": record.get("customer_id", ""),
        "order_number": record.get("order_number", ""),
        "status": record.get("status", ""),
        "order_type": record.get("order_type", ""),
        "subtotal": record.get("subtotal"),
        "tax_amount": record.get("tax_amount"),
        "delivery_fee": record.get("delivery_fee"),
        "service_fee": record.get("service_fee"),
        "tip_amount": record.get("tip_amount"),
        "total_amount": record.get("total_amount"),
        "currency": record.get("currency", "USD"),
        "payment_method": record.get("payment_method", ""),
        "delivery_address": json.dumps(record.get("delivery_address", {})),
        "estimated_delivery_time": record.get("estimated_delivery_time", ""),
        "actual_delivery_time": record.get("actual_delivery_time", ""),
        "created_at": record.get("created_at", ""),
        "updated_at": record.get("updated_at", ""),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_promotion_data(record):
    """
    Transform API response record to promotions table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "store_id": record.get("store_id", ""),
        "name": record.get("name", ""),
        "description": record.get("description", ""),
        "promotion_type": record.get("promotion_type", ""),
        "discount_type": record.get("discount_type", ""),
        "discount_value": record.get("discount_value"),
        "minimum_order_value": record.get("minimum_order_value"),
        "maximum_discount": record.get("maximum_discount"),
        "usage_limit": record.get("usage_limit"),
        "usage_count": record.get("usage_count", 0),
        "is_active": record.get("is_active", True),
        "start_date": record.get("start_date", ""),
        "end_date": record.get("end_date", ""),
        "terms_and_conditions": record.get("terms_and_conditions", ""),
        "created_at": record.get("created_at", ""),
        "updated_at": record.get("updated_at", ""),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_menu_data(record):
    """
    Transform API response record to menus table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "store_id": record.get("store_id", ""),
        "name": record.get("name", ""),
        "description": record.get("description", ""),
        "category": record.get("category", ""),
        "price": record.get("price"),
        "currency": record.get("currency", "USD"),
        "image_url": record.get("image_url", ""),
        "calories": record.get("nutritional_info", {}).get("calories"),
        "allergens": json.dumps(record.get("allergens", [])),
        "ingredients": json.dumps(record.get("ingredients", [])),
        "customizations": json.dumps(record.get("customizations", [])),
        "is_available": record.get("is_available", True),
        "preparation_time": record.get("preparation_time"),
        "spice_level": record.get("spice_level", ""),
        "created_at": record.get("created_at", ""),
        "updated_at": record.get("updated_at", ""),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def get_stores(api_key, params, last_sync_time=None, configuration=None):
    """
    Fetch store data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        params: Additional parameters for the API request.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual store records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/stores"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    request_params = {
        "limit": max_records,
        "offset": 0,
        "updated_since": time_range["start"],
    }
    request_params.update(params)

    offset = 0
    while True:
        request_params["offset"] = offset
        response = execute_api_request(endpoint, api_key, request_params, configuration)

        stores = response.get("stores", [])
        if not stores:
            break

        # Yield individual records instead of accumulating
        for store in stores:
            yield __map_store_data(store)

        if len(stores) < max_records:
            break
        offset += max_records


def get_orders(api_key, store_id, params, last_sync_time=None, configuration=None):
    """
    Fetch order data for a specific store using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        store_id: Store ID to fetch orders for.
        params: Additional parameters for the API request.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual order records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = f"/stores/{store_id}/orders"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    request_params = {
        "limit": max_records,
        "offset": 0,
        "start_date": time_range["start"],
        "end_date": time_range["end"],
    }
    request_params.update(params)

    offset = 0
    while True:
        request_params["offset"] = offset
        response = execute_api_request(endpoint, api_key, request_params, configuration)

        orders = response.get("orders", [])
        if not orders:
            break

        # Yield individual records instead of accumulating
        for order in orders:
            yield __map_order_data(order)

        if len(orders) < max_records:
            break
        offset += max_records


def get_promotions(api_key, store_id, params, last_sync_time=None, configuration=None):
    """
    Fetch promotion data for a specific store using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        store_id: Store ID to fetch promotions for.
        params: Additional parameters for the API request.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual promotion records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = f"/stores/{store_id}/promotions"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    request_params = {
        "limit": max_records,
        "offset": 0,
        "updated_since": time_range["start"],
    }
    request_params.update(params)

    offset = 0
    while True:
        request_params["offset"] = offset
        response = execute_api_request(endpoint, api_key, request_params, configuration)

        promotions = response.get("promotions", [])
        if not promotions:
            break

        # Yield individual records instead of accumulating
        for promotion in promotions:
            yield __map_promotion_data(promotion)

        if len(promotions) < max_records:
            break
        offset += max_records


def get_menus(api_key, store_id, params, last_sync_time=None, configuration=None):
    """
    Fetch menu data for a specific store using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        store_id: Store ID to fetch menus for.
        params: Additional parameters for the API request.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual menu item records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = f"/stores/{store_id}/menus"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    request_params = {
        "limit": max_records,
        "offset": 0,
        "updated_since": time_range["start"],
    }
    request_params.update(params)

    offset = 0
    while True:
        request_params["offset"] = offset
        response = execute_api_request(endpoint, api_key, request_params, configuration)

        menu_items = response.get("menu_items", [])
        if not menu_items:
            break

        # Yield individual records instead of accumulating
        for menu_item in menu_items:
            yield __map_menu_data(menu_item)

        if len(menu_items) < max_records:
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
        {"table": "stores", "primary_key": ["id"]},
        {"table": "orders", "primary_key": ["id", "store_id"]},
        {"table": "promotions", "primary_key": ["id", "store_id"]},
        {"table": "menus", "primary_key": ["id", "store_id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Main synchronization function that fetches and processes data from the Uber Eats API.
    This function orchestrates the entire sync process using memory-efficient streaming patterns.

    Args:
        configuration: Configuration dictionary containing API credentials and settings.
        state: State dictionary containing sync cursors and checkpoints from previous runs.

    Raises:
        RuntimeError: If sync fails due to API errors or configuration issues.
    """
    log.info("Starting Uber Eats API connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    api_key = __get_config_str(configuration, "api_key")
    max_records_per_page = __get_config_int(configuration, "max_records_per_page", 100, 1, 1000)
    enable_incremental = __get_config_bool(configuration, "enable_incremental_sync", True)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time") if enable_incremental else None

    try:
        # Track record counts and processing
        record_counts = {"stores": 0, "orders": 0, "promotions": 0, "menus": 0}

        # First, fetch all stores to get store IDs
        log.info("Fetching stores data...")
        stores = []
        for record in get_stores(api_key, {}, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="stores", data=record)
            record_counts["stores"] += 1
            stores.append(record)

            # Checkpoint every page/batch to save progress incrementally
            if record_counts["stores"] % max_records_per_page == 0:
                checkpoint_state = {
                    "last_sync_time": record.get(
                        "updated_at", datetime.now(timezone.utc).isoformat()
                    ),
                    "stores_processed": record_counts["stores"],
                }
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)

        # Fetch orders, promotions, and menus for each store
        log.info(f"Fetching orders, promotions, and menus for {len(stores)} stores...")
        for store in stores:
            store_id = store.get("id")
            if not store_id:
                log.warning(f"Skipping store without ID: {store}")
                continue

            # Fetch orders data for this store
            log.info(f"Fetching orders for store {store_id}...")
            for record in get_orders(api_key, store_id, {}, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="orders", data=record)
                record_counts["orders"] += 1

                # Checkpoint every page/batch to save progress incrementally
                if record_counts["orders"] % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": record.get(
                            "updated_at", datetime.now(timezone.utc).isoformat()
                        ),
                        "orders_processed": record_counts["orders"],
                        "current_store_id": store_id,
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)

            # Fetch promotions data for this store
            log.info(f"Fetching promotions for store {store_id}...")
            for record in get_promotions(api_key, store_id, {}, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="promotions", data=record)
                record_counts["promotions"] += 1

                # Checkpoint every page/batch to save progress incrementally
                if record_counts["promotions"] % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": record.get(
                            "updated_at", datetime.now(timezone.utc).isoformat()
                        ),
                        "promotions_processed": record_counts["promotions"],
                        "current_store_id": store_id,
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)

            # Fetch menus data for this store
            log.info(f"Fetching menus for store {store_id}...")
            for record in get_menus(api_key, store_id, {}, last_sync_time, configuration):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="menus", data=record)
                record_counts["menus"] += 1

                # Checkpoint every page/batch to save progress incrementally
                if record_counts["menus"] % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": record.get(
                            "updated_at", datetime.now(timezone.utc).isoformat()
                        ),
                        "menus_processed": record_counts["menus"],
                        "current_store_id": store_id,
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)

        # Final checkpoint with completion status
        total_records = sum(record_counts.values())
        final_state = {
            "last_sync_time": datetime.now(timezone.utc).isoformat(),
            "total_records_processed": total_records,
            "stores_count": record_counts["stores"],
            "orders_count": record_counts["orders"],
            "promotions_count": record_counts["promotions"],
            "menus_count": record_counts["menus"],
        }
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info(
            f"Sync completed successfully. Processed {total_records} total records: "
            f"{record_counts['stores']} stores, {record_counts['orders']} orders, "
            f"{record_counts['promotions']} promotions, {record_counts['menus']} menu items."
        )

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Create the Connector object
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
