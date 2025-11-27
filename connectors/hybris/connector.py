"""
This Fivetran connector extracts order data from SAP Hybris Commerce Cloud API using OAuth2
authentication and incrementally syncs orders with related payment, line item, bundle, and
promotion details.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For making HTTP requests
import requests

# For handling HTTP Basic Authentication
from requests.auth import HTTPBasicAuth

# For date and time manipulations
from datetime import datetime, timedelta

# For retry logic
import time

# For type hinting
from typing import Dict, Any, Optional


__MAX_RETRIES = 3  # maximum number of retry attempts
__INITIAL_RETRY_DELAY = 1
__MAX_RETRY_DELAY = 15  # maximum delay between retries in seconds


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "orders_run_history",
            "primary_key": ["run_key"],
        },
        {
            "table": "orders_raw",
            "primary_key": ["order_key"],
        },
        {
            "table": "orders_payment_transactions",
            "primary_key": ["order_key"],
        },
        {
            "table": "orders_all_promotion_results",
            "primary_key": ["order_key"],
        },
        {
            "table": "orders_entries",
            "primary_key": ["order_key"],
        },
        {
            "table": "orders_entries_bundle_entries",
            "primary_key": ["order_key"],
        },
    ]


def _normalize_primitive_value(value: Any) -> Any:
    """
    Normalize a primitive value for storage.
    Args:
        value (Any): Primitive value to normalize
    Returns:
        Any: The value itself or "N/A" if empty
    """
    is_empty_value = value is None or value == ""
    if is_empty_value:
        return "N/A"
    else:
        return value


def _handle_dict_value(prefix: str, data: dict, result: Dict[str, Any]) -> None:
    """
    Handle flattening of a dictionary value.
    Args:
        prefix (str): Current key prefix
        data (dict): Dictionary to flatten
        result (Dict[str, Any]): Dictionary to accumulate flattened key-value pairs
    """
    if not data:
        result[prefix] = "N/A"
        return

    for key, value in data.items():
        new_key = f"{prefix}_{key}" if prefix else key
        flatten_dict(new_key, value, result)


def _handle_list_value(prefix: str, data: list, result: Dict[str, Any]) -> None:
    """
    Handle flattening of a list value.
    Args:
        prefix (str): Current key prefix
        data (list): List to process
        result (Dict[str, Any]): Dictionary to accumulate flattened key-value pairs
    """
    if not data:
        result[prefix] = "N/A"
    else:
        result[prefix] = json.dumps(data)


def flatten_dict(prefix: str, data: Any, result: Dict[str, Any]) -> None:
    """
    Recursively flatten nested dictionaries and lists into a single-level dictionary.
    This helper function processes complex nested API responses by converting them
    into a flat structure suitable for database insertion. Nested dictionaries are
    flattened with underscore-separated keys, and lists are JSON-serialized.
    Args:
        prefix (str): Current key prefix for nested structures (empty string for root level)
        data (Any): Data to flatten (dict, list, or primitive value)
        result (Dict[str, Any]): Dictionary to accumulate flattened key-value pairs
    Behavior:
        - Empty dicts/lists: Converted to "N/A" placeholder
        - Non-empty dicts: Recursively flattened with underscore-prefixed keys
        - Non-empty lists: JSON-serialized as strings
        - None/empty strings: Converted to "N/A"
        - Other values: Stored as-is
    """
    if isinstance(data, dict):
        _handle_dict_value(prefix, data, result)
    elif isinstance(data, list):
        _handle_list_value(prefix, data, result)
    else:
        result[prefix] = _normalize_primitive_value(data)


def _calculate_retry_delay(attempt: int) -> float:
    """
    Calculate exponential backoff delay for retry attempts.
    Args:
        attempt (int): Current retry attempt number (0-based)
    Returns:
        float: Delay in seconds (capped at __MAX_RETRY_DELAY)
    """
    delay = __INITIAL_RETRY_DELAY * (2**attempt)
    return min(delay, __MAX_RETRY_DELAY)


def _should_retry_request(exception: Exception) -> bool:
    """
    Determine if a request should be retried based on the exception.
    Args:
        exception (Exception): The exception raised during request
    Returns:
        bool: True if request should be retried, False otherwise
    """
    if not isinstance(exception, requests.exceptions.RequestException):
        return False

    if isinstance(exception, requests.exceptions.ConnectionError):
        return True

    if isinstance(exception, requests.exceptions.Timeout):
        return True

    if isinstance(exception, requests.exceptions.HTTPError):
        status_code = exception.response.status_code if exception.response else None
        return status_code in [429, 500, 502, 503, 504]

    return False


def _make_api_request_with_retry(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    data: Optional[Dict[str, Any]] = None,
    auth: Optional[HTTPBasicAuth] = None,
) -> requests.Response:
    """
    Make an API request with retry logic and exponential backoff.
    Args:
        method (str): HTTP method ("GET" or "POST")
        url (str): URL to request
        headers (Optional[Dict[str, str]]): HTTP headers
        data (Optional[Dict[str, Any]]): Request body data
        auth (Optional[HTTPBasicAuth]): Authentication objec
    Returns:
        requests.Response: Successful response object
    Raises:
        requests.exceptions.RequestException: If all retry attempts fail
    """
    last_exception = None

    for attempt in range(__MAX_RETRIES):
        try:
            if method == "GET":
                response = requests.get(url, headers=headers, auth=auth)
            else:
                response = requests.post(url, headers=headers, data=data, auth=auth)

            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as e:
            last_exception = e

            if not _should_retry_request(e):
                raise

            if attempt < __MAX_RETRIES - 1:
                delay = _calculate_retry_delay(attempt)
                log.warning(
                    f"Request failed (attempt {attempt + 1}/{__MAX_RETRIES}): {e}. Retrying in {delay}s..."
                )
                time.sleep(delay)
            else:
                log.severe(f"Request failed after {__MAX_RETRIES} attempts")

    raise last_exception


def validate_configuration(configuration: Dict[str, Any]) -> None:
    """
    Validate that all required configuration keys are present.
    Args:
        configuration (Dict[str, Any]): Configuration dictionary to validate
    Raises:
        ValueError: If any required configuration key is missing
    """
    required_keys = [
        "prod_client_id",
        "prod_client_secret",
        "prod_api_url",
        "prod_token_url",
        "orders_api_endpoint",
    ]

    missing_keys = [key for key in required_keys if key not in configuration]
    if missing_keys:
        raise ValueError(f"Missing required configuration keys: {", ".join(missing_keys)}")


def get_oauth_token(token_url: str, client_id: str, client_secret: str) -> str:
    """
    Obtain OAuth2 access token using client credentials grant with retry logic.
    Args:
        token_url (str): OAuth2 token endpoint URL
        client_id (str): OAuth2 client ID
        client_secret (str): OAuth2 client secret
    Returns:
        str: Access token for API authentication
    Raises:
        requests.exceptions.RequestException: If token acquisition fails after retries
    """
    auth = HTTPBasicAuth(client_id, client_secret)
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    token_data = {"grant_type": "client_credentials"}

    try:
        token_response = _make_api_request_with_retry(
            method="POST", url=token_url, headers=headers, data=token_data, auth=auth
        )
        return token_response.json()["access_token"]
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(f"Error getting access token: {e}") from e


def build_date_filters(cursor: str, current_date: datetime) -> tuple:
    """
    Build URL-encoded date filter strings for API requests.
    Args:
        cursor (str): Cursor timestamp string in format "YYYY-MM-DD HH:MM:SS.000000"
        current_date (datetime): Current datetime for the upper bound
    Returns:
        tuple: (from_date_cursor, to_date_cursor) URL-encoded filter strings
    """
    cursor_dt = datetime.strptime(cursor.split(".")[0], "%Y-%m-%d %H:%M:%S")
    cursor_date_str = cursor_dt.strftime("%Y-%m-%d")
    current_date_str = current_date.strftime("%Y-%m-%d")

    from_date_cursor = (
        f"fromDate={cursor_date_str}T{cursor_dt.strftime("%H")}%3A"
        f"{cursor_dt.strftime("%M")}%3A{cursor_dt.strftime("%S")}%2B0000"
    )
    to_date_cursor = (
        f"toDate={current_date_str}T{current_date.strftime("%H")}%3A"
        f"{current_date.strftime("%M")}%3A{current_date.strftime("%S")}%2B0000"
    )

    return from_date_cursor, to_date_cursor


def process_payment_transactions(order_key: str, order_data: Dict[str, Any]) -> None:
    """
    Process and upsert payment transactions for an order.
    Args:
        order_key (str): Primary order identifier
        order_data (Dict[str, Any]): Raw order data containing payment transactions
    """
    if "paymentTransactions" in order_data and order_data["paymentTransactions"] not in [None, []]:
        for payment in order_data["paymentTransactions"]:
            payment_data = {}
            flatten_dict("paymentTransactions", payment, payment_data)
            payment_transaction_order_key = f"{order_key}_{payment["transactionId"]}"
            # The "upsert" operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(
                table="orders_payment_transactions",
                data={
                    "order_key": payment_transaction_order_key,
                    "order_num": order_key,
                    **payment_data,
                },
            )


def process_order_entries(order_key: str, order_data: Dict[str, Any]) -> None:
    """
    Process and upsert order entries (line items) and their bundle entries.
    Args:
        order_key (str): Primary order identifier
        order_data (Dict[str, Any]): Raw order data containing entries
    """
    if "entries" in order_data and order_data["entries"] not in [None, []]:
        for entry in order_data["entries"]:
            entry_data = {}
            flatten_dict("entries", entry, entry_data)
            order_entries_key = f"{order_key}_{entry["orderLineNumber"]}"
            # The "upsert" operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(
                table="orders_entries",
                data={"order_key": order_entries_key, "order_num": order_key, **entry_data},
            )

            # Process bundle entries within this entry
            if "bundleEntries" in entry and entry["bundleEntries"] not in [None, []]:
                for bundle in entry["bundleEntries"]:
                    bundle_data = {}
                    flatten_dict("bundleEntries", bundle, bundle_data)
                    bundle_order_key = (
                        f"{order_key}_{entry["orderLineNumber"]}_{bundle["orderLineNumber"]}"
                    )
                    entry_line = entry["orderLineNumber"]
                    # The "upsert" operation is used to insert or update data in the destination table.
                    # The first argument is the name of the destination table.
                    # The second argument is a dictionary containing the record to be upserted.
                    op.upsert(
                        table="orders_entries_bundle_entries",
                        data={
                            "order_key": bundle_order_key,
                            "order_num": order_key,
                            "entry_line": entry_line,
                            **bundle_data,
                        },
                    )


def process_promotion_results(order_key: str, order_data: Dict[str, Any]) -> None:
    """
    Process and upsert promotion results for an order.
    Args:
        order_key (str): Primary order identifier
        order_data (Dict[str, Any]): Raw order data containing promotion results
    """
    if "allPromotionResults" in order_data and order_data["allPromotionResults"] not in [None, []]:
        for promotion in order_data["allPromotionResults"]:
            if promotion.get("promotion"):
                promotion_data = {}
                flatten_dict("promotion", promotion, promotion_data)
                promo_order_key = f"{order_key}_{promotion["promotion"]["name"]}"
                # The "upsert" operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(
                    table="orders_all_promotion_results",
                    data={"order_key": promo_order_key, "order_num": order_key, **promotion_data},
                )


def process_single_order(order: Dict[str, Any]) -> None:
    """
    Process a single order and all its related entities.
    Args:
        order (Dict[str, Any]): Raw order data from API
    """
    order_key = str(order["orderNumber"])
    order_data = {"order_key": order_key}
    flatten_dict("", order, order_data)

    # The "upsert" operation is used to insert or update data in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the record to be upserted.
    op.upsert(
        table="orders_raw",
        data={"order_key": order_data["order_key"], "order_num": order_key, **order_data},
    )

    # Process related entities
    process_payment_transactions(order_key=order_key, order_data=order)
    process_order_entries(order_key=order_key, order_data=order)
    process_promotion_results(order_key=order_key, order_data=order)


def initialize_api_session(token_url: str, client_id: str, client_secret: str) -> Dict[str, str]:
    """
    Initialize API session by obtaining OAuth token and building headers.

    Args:
        token_url (str): OAuth2 token endpoint URL
        client_id (str): OAuth2 client ID
        client_secret (str): OAuth2 client secret

    Returns:
        Dict[str, str]: HTTP headers with authorization token

    Raises:
        Exception: If OAuth token acquisition fails
    """
    try:
        access_token = get_oauth_token(token_url, client_id, client_secret)
        return {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    except Exception as e:
        log.severe(f"Failed to obtain OAuth token: {e}")
        raise


def fetch_initial_page_and_metadata(
    api_url: str,
    api_endpoint: str,
    from_date_cursor: str,
    to_date_cursor: str,
    headers: Dict[str, str],
) -> tuple:
    """
    Fetch first page of orders and extract pagination metadata.

    Args:
        api_url (str): Base API URL
        api_endpoint (str): API endpoint path
        from_date_cursor (str): URL-encoded from date filter
        to_date_cursor (str): URL-encoded to date filter
        headers (Dict[str, str]): HTTP headers including authorization

    Returns:
        tuple: (response_data, num_pages, page_size, total_results) or (None, 0, 0, 0) if no data

    Raises:
        requests.exceptions.RequestException: If API request fails
    """
    try:
        response_data = fetch_orders_page(
            api_url, api_endpoint, 0, from_date_cursor, to_date_cursor, headers
        )
    except requests.exceptions.RequestException as e:
        log.severe(f"Failed to fetch orders: {e}")
        raise requests.exceptions.RequestException(f"Error accessing API: {e}") from e

    if "paginationData" not in response_data:
        log.warning("No pagination data found in response")
        return None, 0, 0, 0

    pagination = response_data["paginationData"]
    page_size = pagination.get("pageSize", 0)
    total_results = pagination.get("totalNumberOfResults", 0)
    num_pages = pagination.get("numberOfPages", 0)

    log.info(f"Total pages: {num_pages}, Total results: {total_results}")
    return response_data, num_pages, page_size, total_results


def process_orders_on_page(orders_list: list, page_number: int) -> None:
    """
    Process all orders on a single page with error isolation.

    Args:
        orders_list (list): List of order dictionaries from API response
        page_number (int): Current page number for logging

    Note:
        Errors processing individual orders are logged but don"t stop processing
    """
    if not orders_list:
        log.info(f"No orders found on page {page_number}")
        return

    log.info(f"Processing {len(orders_list)} orders from page {page_number}")

    for order in orders_list:
        try:
            process_single_order(order)
        except Exception as e:
            log.warning(f"Error processing order {order.get("orderNumber", "unknown")}: {e}")
            # Continue processing other orders


def fetch_orders_page(
    api_url: str,
    api_endpoint: str,
    page: int,
    from_date_cursor: str,
    to_date_cursor: str,
    headers: Dict[str, str],
) -> Dict[str, Any]:
    """
    Fetch a single page of orders from the API with retry logic.
    Args:
        api_url (str): Base API URL
        api_endpoint (str): API endpoint path
        page (int): Page number to fetch
        from_date_cursor (str): URL-encoded from date filter
        to_date_cursor (str): URL-encoded to date filter
        headers (Dict[str, str]): HTTP headers including authorization
    Returns:
        Dict[str, Any]: API response containing orders and pagination data
    Raises:
        requests.exceptions.RequestException: If API request fails after retries
    """
    url = f"{api_url}{api_endpoint}&page={page}&{from_date_cursor}&{to_date_cursor}"
    log.info(f"{url} - API Call Count: {page + 1}")

    response = _make_api_request_with_retry(method="GET", url=url, headers=headers)
    return response.json()


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    log.warning("Example: Connectors : Hybris Orders")

    # Validate configuration
    validate_configuration(configuration)

    # Extract configuration
    api_url = configuration["prod_api_url"]
    api_endpoint = configuration["orders_api_endpoint"]

    # Get current date and cursor
    current_date = datetime.now()
    default_cursor = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d 00:00:00.000000")
    cursor = state.get("cursor", default_cursor)

    # Build date filters
    from_date_cursor, to_date_cursor = build_date_filters(cursor, current_date)

    # Initialize API session (get OAuth token and build headers)
    headers = initialize_api_session(
        token_url=configuration["prod_token_url"],
        client_id=configuration["prod_client_id"],
        client_secret=configuration["prod_client_secret"],
    )

    # Fetch first page and extract pagination metadata
    response_data, num_pages, page_size, total_results = fetch_initial_page_and_metadata(
        api_url=api_url,
        api_endpoint=api_endpoint,
        from_date_cursor=from_date_cursor,
        to_date_cursor=to_date_cursor,
        headers=headers,
    )

    if response_data is None:
        log.warning("No data to process for the given date range.")
        return

    # Record run history
    op.upsert(
        table="orders_run_history",
        data={
            "run_key": str(current_date),
            "page_size": str(page_size),
            "totalNumberOfPages": str(num_pages),
            "totalNumberOfResults": str(total_results),
        },
    )

    # Process all pages
    current_page = 0
    while current_page < num_pages:
        # Fetch next page if not the first page
        if current_page > 0:
            try:
                response_data = fetch_orders_page(
                    api_url=api_url,
                    api_endpoint=api_endpoint,
                    page=current_page,
                    from_date_cursor=from_date_cursor,
                    to_date_cursor=to_date_cursor,
                    headers=headers,
                )
            except requests.exceptions.RequestException as e:
                log.severe(f"Failed to fetch page {current_page}: {e}")
                raise requests.exceptions.RequestException(f"Error accessing API: {e}") from e

        # Process all orders on the current page
        process_orders_on_page(
            orders_list=response_data.get("orders", []), page_number=current_page
        )

        # Checkpoint after each page
        updated_state = {"cursor": current_date.strftime("%Y-%m-%d %H:%M:%S")}
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=updated_state)

        current_page += 1

    log.info(f"Completed processing {num_pages} page(s) with {total_results} total records")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)


# Check if the script is being run as the main module.
# This is Python"s standard entry method allowing your script to be run directly from the command line or IDE "run" button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug()
