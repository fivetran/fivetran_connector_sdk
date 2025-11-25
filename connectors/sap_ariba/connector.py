"""
This connector demonstrates how to fetch purchase order data from the SAP Ariba API and
upsert it into a Fivetran destination using the Connector SDK.

See the Technical Reference documentation:
https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

See the Best Practices documentation:
https://fivetran.com/docs/connectors/connector-sdk/best-practices
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For making HTTP requests to Dgraph GraphQL API (provided by SDK runtime)
import requests

# For handling datetime parsing and formatting
from datetime import datetime, timezone

# For handling exponential backoff in retries
import time

# Constants
__MAX_RETRIES = 3
__RETRY_DELAY = 3  # seconds
__BASE_URL = "https://sandbox.api.sap.com/ariba/api/purchase-orders/v1/sandbox/"
__RECORDS_PER_PAGE = 100
__LAST_UPDATED_AT = "last_updated_at"
__CHECKPOINT_INTERVAL = 1000  # Checkpoint after processing every 1000 rows
__PAGE_OFFSET = "$skip"


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "order",  # Name of the table in the destination.
            "primary_key": ["payloadId", "revision", "rowId"],
            "columns": get_order_columns(),  # Define the columns and their data types.
        },
        {
            "table": "item",  # Name of the table in the destination.
            "primary_key": ["documentNumber", "lineNumber", "rowId"],
            "columns": get_item_columns(),  # Define the columns and their data types.
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.
        state: A dictionary that holds the state of the connector.
    """
    validate_configuration(configuration)
    log.warning("Example: connectors : sap_ariba")

    # Capture the sync start timestamp once and reuse it for all rows in this run.
    current_sync_start = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    headers = build_headers(configuration)

    log.info("Starting sync from SAP Ariba API...")

    base_params = {"$top": __RECORDS_PER_PAGE, "$count": True}

    # Run order and item syncs using separate copies of the base params.
    sync_orders(base_params.copy(), headers, state, current_sync_start)
    sync_items(base_params.copy(), headers, state, current_sync_start)


def validate_configuration(configuration: dict) -> None:
    """
    Validate the configuration dictionary to ensure it contains all required parameters.

    This function is called at the start of the update method to ensure that the connector
    has all necessary configuration values.

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: If any required configuration parameter is missing.
    """
    required_keys = ["api_key"]
    for key in required_keys:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def build_headers(configuration: dict) -> dict:
    """
    Build HTTP headers for SAP Ariba API requests.

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.

    Returns:
        A dictionary of headers to use in API requests.
    """
    return {
        "APIKey": configuration.get("api_key"),
        "Accept": "application/json",
        "DataServiceVersion": "2.0",
    }


def sync_rows(
    table_name: str,
    params: dict,
    headers: dict,
    state: dict,
    allowed_columns: dict,
    sync_start: str,
) -> None:
    """
    Fetch all rows for a table using pagination.

    Args:
        table_name: Name of the table to sync.
        params: API request parameters.
        headers: API request headers.
        state: State dictionary to track sync progress.
        allowed_columns: Dictionary of allowed columns for filtering.
        sync_start: Timestamp of the current sync start (ISO 8601 string).
    """
    row_count = 0
    total_records = None

    while total_records is None or row_count < total_records:
        data = fetch_page(table_name, params, headers)
        if not data:
            break

        total_records = update_total_records(table_name, data, total_records)
        items = data.get("content", [])

        if not items:
            # No more data returned from the API, stop pagination.
            break

        row_count = process_items(table_name, items, allowed_columns, sync_start, row_count)

        if should_checkpoint(row_count, total_records):
            checkpoint_state(state)
            if total_records and row_count >= total_records:
                break

        move_to_next_page(params)


def fetch_page(table_name: str, params: dict, headers: dict) -> dict:
    """
    Fetch a single page of results for a table.

    Args:
        table_name: Name of the table to sync.
        params: API request parameters.
        headers: API request headers.

    Returns:
        JSON response for the current page as a dictionary.
    """
    return make_api_request(table_name, params=params, headers=headers)


def update_total_records(table_name: str, data: dict, existing_total: int | None) -> int | None:
    """
    Update the total record count from the first page response.

    Args:
        table_name: Name of the table being processed.
        data: JSON response from the API.
        existing_total: Previously known total record count.

    Returns:
        Updated total record count or the existing value if unchanged.
    """
    if existing_total is not None:
        return existing_total

    if data.get("firstPage"):
        count = data.get("count", 0)
        log.info(f"Total records to process for table {table_name}: {count}")
        return count

    return existing_total


def process_items(
    table_name: str,
    items: list,
    allowed_columns: dict,
    sync_start: str,
    starting_row_count: int,
) -> int:
    """
    Process and upsert a page of items into the destination table.

    Args:
        table_name: Name of the table to sync.
        items: List of records from the API page.
        allowed_columns: Dictionary of allowed columns for filtering.
        sync_start: Timestamp of the current sync start.
        starting_row_count: Row count before processing this page.

    Returns:
        Updated row count after processing all items on the page.
    """
    row_count = starting_row_count

    for api_record in items:
        row_count += 1

        # Filter the record to keep only expected columns (and convert date fields).
        values = filter_columns(api_record, allowed_columns)
        # Row index in this sync; useful as a synthetic key.
        values["rowId"] = row_count
        # Track when this row was last updated by the connector.
        values["last_updated_at"] = sync_start

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=table_name, data=values)

    return row_count


def should_checkpoint(row_count: int, total_records: int | None) -> bool:
    """
    Determine whether a checkpoint should be written based on progress.

    Args:
        row_count: Number of rows processed so far.
        total_records: Total number of records expected (may be None).

    Returns:
        True if a checkpoint should be written, False otherwise.
    """
    if row_count == 0:
        return False

    if row_count % __CHECKPOINT_INTERVAL == 0:
        return True

    if total_records is not None and row_count >= total_records:
        return True

    return False


def checkpoint_state(state: dict) -> None:
    """
    Persist the connector state using checkpoint.

    Args:
        state: State dictionary to checkpoint.
    """
    op.checkpoint(state)


def move_to_next_page(params: dict) -> None:
    """
    Update pagination parameters to move to the next page.

    Args:
        params: API request parameters that include the page offset.
    """
    params[__PAGE_OFFSET] += __RECORDS_PER_PAGE


def convert_to_iso(date_str: str | None) -> str | None:
    """
    Convert a date string to ISO 8601 format.

    Args:
        date_str: Date string to convert to ISO 8601 format.

    Returns:
        The date string in ISO 8601 format, or the original string if conversion fails
        or the input is None.
    """
    if not date_str:
        return None
    try:
        parsed = datetime.strptime(date_str, "%d %b %Y %I:%M:%S %p")
        return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return date_str


def filter_columns(record: dict, allowed_columns: dict) -> dict:
    """
    Filter the record to include only allowed columns and convert date fields to ISO format.

    Args:
        record: Dictionary containing the record data to filter.
        allowed_columns: Dictionary defining allowed columns and their data types.

    Returns:
        Dictionary containing only allowed columns with converted date fields.
    """
    filtered = {}
    for col_name, col_type in allowed_columns.items():
        if col_name == "rowId":
            continue
        if col_type == "UTC_DATETIME":
            filtered[col_name] = convert_to_iso(record.get(col_name))
        elif col_name in record:
            filtered[col_name] = record[col_name]
    return filtered


def sync_orders(params: dict, headers: dict, state: dict, sync_start: str) -> None:
    """
    Fetch all rows for the 'order' table with pagination.

    Args:
        params: API request parameters.
        headers: API request headers.
        state: State dictionary to track sync progress.
        sync_start: Timestamp of the current sync start.
    """
    table_name = "order"

    params[__PAGE_OFFSET] = 0

    if state.get(table_name) is None:
        state[table_name] = {}

    sync_rows(table_name, params, headers, state, get_order_columns(), sync_start)

    checkpoint_state(state)


def sync_items(params: dict, headers: dict, state: dict, sync_start: str) -> None:
    """
    Fetch all rows for the 'item' table with pagination.

    Args:
        params: API request parameters.
        headers: API request headers.
        state: State dictionary to track sync progress.
        sync_start: Timestamp of the current sync start.
    """
    table_name = "item"

    params[__PAGE_OFFSET] = 0

    if state.get(table_name) is None:
        state[table_name] = {}

    sync_rows(table_name, params, headers, state, get_item_columns(), sync_start)

    checkpoint_state(state)


def make_api_request(
    endpoint: str,
    params: dict,
    headers: dict,
    retries: int = __MAX_RETRIES,
    delay: int = __RETRY_DELAY,
) -> dict:
    """
    Make a GET request with retry and backoff logic.

    Args:
        endpoint: The API endpoint to call (relative to BASE_URL).
        params: A dictionary of API request parameters.
        headers: A dictionary of API request headers.
        retries: The number of retries for failed requests.
        delay: The base delay (in seconds) between retries.

    Returns:
        JSON response from the API as a dictionary.

    Raises:
        requests.RequestException: If all retries fail or a non-retriable error occurs.
    """
    url = f"{__BASE_URL}{endpoint}"
    for attempt in range(1, retries + 1):
        try:
            response = perform_request(url, params, headers)
            return handle_response(url, params, response, attempt, delay)
        except requests.RequestException as exc:
            log.severe("Network or HTTP error", exc)
            time.sleep(delay * attempt)
    raise requests.RequestException(f"Failed to fetch {url} after {retries} retries")


def perform_request(url: str, params: dict, headers: dict) -> requests.Response:
    """
    Execute a single HTTP GET request.

    Args:
        url: Fully qualified URL for the API call.
        params: Query parameters for the request.
        headers: HTTP headers for the request.

    Returns:
        The HTTP response object.
    """
    log.info(f"Making API request to: {url}")
    return requests.get(url, params=params, headers=headers, timeout=30)


def handle_response(
    url: str,
    params: dict,
    response: requests.Response,
    attempt: int,
    delay: int,
) -> dict:
    """
    Handle HTTP response codes and apply backoff logic when needed.

    Args:
        url: Request URL.
        params: Request parameters.
        response: HTTP response object.
        attempt: Current retry attempt number.
        delay: Base delay between retries.

    Returns:
        Parsed JSON body if the response is successful.

    Raises:
        requests.RequestException: For fatal errors or when retries are exhausted.
    """
    if response.status_code == 200:
        return response.json()

    if response.status_code == 429:
        wait = delay * attempt
        log.warning(f"Rate limit hit for {url}. Retrying in {wait}s...")
        time.sleep(wait)
        raise requests.RequestException("Rate limit, retrying")

    if 400 <= response.status_code < 500:
        response.raise_for_status()

    if 500 <= response.status_code < 600:
        log.warning(f"Server error {response.status_code} for {url}, body: {response.text}")
        raise requests.RequestException("Server error, retrying")

    raise requests.HTTPError(
        f"Unexpected HTTP status code {response.status_code} for URL "
        f"{url} with params {params}. Response body: {response.text}",
        response=response,
    )


def get_order_columns() -> dict:
    """
    Define the 'order' table columns and their data types.

    Returns:
        Dictionary mapping column names to their data types for the order table.
    """
    return {
        "documentNumber": "STRING",
        "orderDate": "UTC_DATETIME",
        "supplierName": "STRING",
        "supplierANID": "STRING",
        "buyerANID": "STRING",
        "customerName": "STRING",
        "systemId": "STRING",
        "payloadId": "STRING",
        "revision": "STRING",
        "endpointId": "STRING",
        "created": "UTC_DATETIME",
        "status": "STRING",
        "documentStatus": "STRING",
        "amount": "DOUBLE",
        "numberOfInvoic
