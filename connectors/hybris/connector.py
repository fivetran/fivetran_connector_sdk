"""
A Fivetran connector that extracts order data from SAP Hybris Commerce Cloud API using OAuth2 
authentication and incrementally syncs orders with related payment, line item, bundle, and 
promotion details.

Note: This connector uses SDK v1 pattern with yield statements for operations. 
SDK v2+ users should remove yield keywords: use `op.upsert(...)` instead of `yield op.upsert(...)`.
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import other necessary libraries
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

# Constants
BATCH_SIZE_LIMIT = 1000  # Prevent memory issues with large datasets

def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "orders_run_history", "primary_key": ["run_key"],},
        {"table": "orders_raw", "primary_key": ["order_key"],},
        {"table": "orders_payment_transactions", "primary_key": ["order_key"],},
        {"table": "orders_all_promotion_results", "primary_key": ["order_key"],},
        {"table": "orders_entries", "primary_key": ["order_key"],},
        {"table": "orders_entries_bundle_entries", "primary_key": ["order_key"],}
    ]


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
        - Empty dicts/lists: Converted to 'N/A' placeholder
        - Non-empty dicts: Recursively flattened with underscore-prefixed keys
        - Non-empty lists: JSON-serialized as strings
        - None/empty strings: Converted to 'N/A'
        - Other values: Stored as-is
    
    Example:
        Input: prefix="", data={"user": {"name": "John", "age": 30}}, result={}
        Output: result={"user_name": "John", "user_age": 30}
    """
    if isinstance(data, dict):
        if not data:
            result[prefix] = 'N/A'
        else:
            for key, value in data.items():
                new_key = f"{prefix}_{key}" if prefix else key
                flatten_dict(new_key, value, result)
    elif isinstance(data, list):
        if not data:
            result[prefix] = 'N/A'
        else:
            result[prefix] = json.dumps(data)
    else:
        result[prefix] = data if data is not None and data != "" else "N/A"


def validate_configuration(configuration: Dict[str, Any]) -> None:
    """
    Validate that all required configuration keys are present.
    
    Args:
        configuration (Dict[str, Any]): Configuration dictionary to validate
    
    Raises:
        ValueError: If any required configuration key is missing
    """
    required_keys = [
        'prod_client_id',
        'prod_client_secret',
        'prod_api_url',
        'prod_token_url',
        'api_endpoint_o'
    ]
    
    missing_keys = [key for key in required_keys if key not in configuration]
    if missing_keys:
        raise ValueError(f"Missing required configuration keys: {', '.join(missing_keys)}")


def get_oauth_token(token_url: str, client_id: str, client_secret: str) -> str:
    """
    Obtain OAuth2 access token using client credentials grant.
    
    Args:
        token_url (str): OAuth2 token endpoint URL
        client_id (str): OAuth2 client ID
        client_secret (str): OAuth2 client secret
    
    Returns:
        str: Access token for API authentication
    
    Raises:
        Exception: If token acquisition fails
    """
    auth = HTTPBasicAuth(client_id, client_secret)
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    token_data = {'grant_type': 'client_credentials'}
    
    try:
        token_response = requests.post(token_url, auth=auth, headers=headers, data=token_data)
        token_response.raise_for_status()
        return token_response.json()['access_token']
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error getting access token: {e}") from e


def build_date_filters(cursor: str, current_date: datetime) -> tuple:
    """
    Build URL-encoded date filter strings for API requests.
    
    Args:
        cursor (str): Cursor timestamp string in format "YYYY-MM-DD HH:MM:SS.000000"
        current_date (datetime): Current datetime for the upper bound
    
    Returns:
        tuple: (from_date_cursor, to_date_cursor) URL-encoded filter strings
    """
    cursor_dt = datetime.strptime(cursor.split('.')[0], "%Y-%m-%d %H:%M:%S")
    cursor_date_str = cursor_dt.strftime("%Y-%m-%d")
    current_date_str = current_date.strftime("%Y-%m-%d")
    
    from_date_cursor = (
        f"fromDate={cursor_date_str}T{cursor_dt.strftime('%H')}%3A"
        f"{cursor_dt.strftime('%M')}%3A{cursor_dt.strftime('%S')}%2B0000"
    )
    to_date_cursor = (
        f"toDate={current_date_str}T{current_date.strftime('%H')}%3A"
        f"{current_date.strftime('%M')}%3A{current_date.strftime('%S')}%2B0000"
    )
    
    return from_date_cursor, to_date_cursor


def process_payment_transactions(order_key: str, order_data: Dict[str, Any]):
    """
    Process and upsert payment transactions for an order.
    
    Args:
        order_key (str): Primary order identifier
        order_data (Dict[str, Any]): Raw order data containing payment transactions
    
    Yields:
        Operation results from op.upsert calls
    """
    if 'paymentTransactions' in order_data and order_data['paymentTransactions'] not in [None, []]:
        for payment in order_data['paymentTransactions']:
            payment_data = {}
            flatten_dict("paymentTransactions", payment, payment_data)
            paymenttx_order_key = f"{order_key}_{payment['transactionId']}"
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            yield op.upsert(
                "orders_payment_transactions",
                {"order_key": paymenttx_order_key, "order_num": order_key, **payment_data}
            )


def process_order_entries(order_key: str, order_data: Dict[str, Any]):
    """
    Process and upsert order entries (line items) and their bundle entries.
    
    Args:
        order_key (str): Primary order identifier
        order_data (Dict[str, Any]): Raw order data containing entries
    
    Yields:
        Operation results from op.upsert calls
    """
    if 'entries' in order_data and order_data['entries'] not in [None, []]:
        for entry in order_data['entries']:
            entry_data = {}
            flatten_dict("entries", entry, entry_data)
            order_entries_key = f"{order_key}_{entry['orderLineNumber']}"
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            yield op.upsert(
                "orders_entries",
                {"order_key": order_entries_key, "order_num": order_key, **entry_data}
            )
            
            # Process bundle entries within this entry
            if 'bundleEntries' in entry and entry['bundleEntries'] not in [None, []]:
                for bundle in entry['bundleEntries']:
                    bundle_data = {}
                    flatten_dict("bundleEntries", bundle, bundle_data)
                    bundle_order_key = f"{order_key}_{entry['orderLineNumber']}_{bundle['orderLineNumber']}"
                    entry_line = entry['orderLineNumber']
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The first argument is the name of the destination table.
                    # The second argument is a dictionary containing the record to be upserted.
                    yield op.upsert(
                        "orders_entries_bundle_entries",
                        {
                            "order_key": bundle_order_key,
                            "order_num": order_key,
                            "entry_line": entry_line,
                            **bundle_data
                        }
                    )


def process_promotion_results(order_key: str, order_data: Dict[str, Any]):
    """
    Process and upsert promotion results for an order.
    
    Args:
        order_key (str): Primary order identifier
        order_data (Dict[str, Any]): Raw order data containing promotion results
    
    Yields:
        Operation results from op.upsert calls
    """
    if 'allPromotionResults' in order_data and order_data['allPromotionResults'] not in [None, []]:
        for promotion in order_data['allPromotionResults']:
            if promotion.get('promotion'):
                promotion_data = {}
                flatten_dict("promotion", promotion, promotion_data)
                promo_order_key = f"{order_key}_{promotion['promotion']['name']}"
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                yield op.upsert(
                    "orders_all_promotion_results",
                    {"order_key": promo_order_key, "order_num": order_key, **promotion_data}
                )


def process_single_order(order: Dict[str, Any]):
    """
    Process a single order and all its related entities.
    
    Args:
        order (Dict[str, Any]): Raw order data from API
    
    Yields:
        Operation results from op.upsert calls
    """
    order_key = str(order['orderNumber'])
    order_data = {"order_key": order_key}
    flatten_dict("", order, order_data)
    
    # The 'upsert' operation is used to insert or update data in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the record to be upserted.
    yield op.upsert(
        "orders_raw",
        {"order_key": order_data['order_key'], "order_num": order_key, **order_data}
    )
    
    # Process related entities
    yield from process_payment_transactions(order_key, order)
    yield from process_order_entries(order_key, order)
    yield from process_promotion_results(order_key, order)


def fetch_orders_page(
    api_url: str,
    api_endpoint: str,
    page: int,
    from_date_cursor: str,
    to_date_cursor: str,
    headers: Dict[str, str]
) -> Dict[str, Any]:
    """
    Fetch a single page of orders from the API.
    
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
        requests.exceptions.RequestException: If API request fails
    """
    url = f"{api_url}{api_endpoint}&page={page}&{from_date_cursor}&{to_date_cursor}"
    log.info(f"{url} - API Call Count: {page + 1}")
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
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
    client_id = configuration['prod_client_id']
    client_secret = configuration['prod_client_secret']
    api_url = configuration['prod_api_url']
    token_url = configuration['prod_token_url']
    api_endpoint = configuration['api_endpoint_o']
    
    # Get current date and cursor
    current_date = datetime.now()
    default_cursor = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d 00:00:00.000000")
    cursor = state.get('cursor', default_cursor)
    
    # Build date filters
    from_date_cursor, to_date_cursor = build_date_filters(cursor, current_date)
    
    # Get OAuth token
    try:
        access_token = get_oauth_token(token_url, client_id, client_secret)
    except Exception as e:
        log.severe(f"Failed to obtain OAuth token: {e}")
        raise
    
    headers = {'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'}
    
    # Fetch first page
    try:
        response_data = fetch_orders_page(
            api_url, api_endpoint, 0, from_date_cursor, to_date_cursor, headers
        )
    except requests.exceptions.RequestException as e:
        log.severe(f"Failed to fetch orders: {e}")
        raise Exception(f"Error accessing API: {e}") from e
    
    # Check for pagination data
    if 'paginationData' not in response_data:
        log.warning("No pagination data found in response")
        return
    
    pagination = response_data['paginationData']
    page_size = pagination.get('pageSize', 0)
    total_results = pagination.get('totalNumberOfResults', 0)
    num_pages = pagination.get('numberOfPages', 0)
    
    log.info(f"Total pages: {num_pages}, Total results: {total_results}")
    
    # The 'upsert' operation is used to insert or update data in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the record to be upserted.
    yield op.upsert(
        "orders_run_history",
        {
            "run_key": str(current_date),
            "page_size": str(page_size),
            "totalNumberOfPages": str(num_pages),
            "totalNumberOfResults": str(total_results)
        }
    )
    
    # Process all pages
    current_page = 0
    while current_page < num_pages:
        # If not first page, fetch next page
        if current_page > 0:
            try:
                response_data = fetch_orders_page(
                    api_url, api_endpoint, current_page, from_date_cursor, to_date_cursor, headers
                )
            except requests.exceptions.RequestException as e:
                log.severe(f"Failed to fetch page {current_page}: {e}")
                raise Exception(f"Error accessing API: {e}") from e
        
        orders_list = response_data.get('orders', [])
        
        if not orders_list:
            log.info(f"No orders found on page {current_page}")
        else:
            log.info(f"Processing {len(orders_list)} orders from page {current_page}")
            
            # Process each order
            for order in orders_list:
                try:
                    yield from process_single_order(order)
                except Exception as e:
                    log.warning(f"Error processing order {order.get('orderNumber', 'unknown')}: {e}")
                    # Continue processing other orders
        
        # Checkpoint after each page
        new_state = {"cursor": current_date.strftime("%Y-%m-%d %H:%M:%S")}
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        yield op.checkpoint(new_state)
        
        current_page += 1
    
    log.info(f"Completed processing {num_pages} page(s) with {total_results} total records")

# Create the connector object using the schema and update functions
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
    connector.debug()
