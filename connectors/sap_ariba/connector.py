"""
    This connector demonstrates how to fetch purchase order data from the SAP Ariba API and upsert it into a Fivetran destination using the Connector SDK.
    See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
    and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""
# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import required libraries
import time  # Provides time-related functions (e.g., sleep, timestamps)
from datetime import datetime, timezone  # Handles date and time objects with timezone awareness
import requests  # Enables sending HTTP requests to external APIs
import json  # Handles JSON data serialization and deserialization

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
        configuration: a dictionary that holds the configuration settings for the connector.
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
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    validate_configuration(configuration)
    log.warning("Example: connectors : sap_ariba")
    current_sync_start = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    headers = {
        "APIKey": configuration.get("api_key"),
        "Accept": "application/json",
        "DataServiceVersion": "2.0",
    }

    log.info("Starting sync from SAP Ariba API...")

    params = {"$top": __RECORDS_PER_PAGE, "$count": True}

    sync_orders(params.copy(), headers, state, current_sync_start)
    sync_items(params.copy(), headers, state, current_sync_start)

def validate_configuration(configuration: dict) -> None:
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """

    required_keys = ["api_key"]
    for key in required_keys:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

def sync_rows(table_name, params, headers, state, allowed_columns, sync_start):
    """
    Fetches all rows for a table using pagination.
    Args:
        table_name: Name of the table to sync.
        params: API request parameters.
        headers: API request headers.
        state: State dictionary to track sync progress.
        allowed_columns: Dictionary of allowed columns for filtering.
        sync_start: Timestamp of the current sync start.
    """
    count = 0
    record_count = None
    while record_count is None or count < record_count:
        data = make_api_request(f"{table_name}", params=params, headers=headers)
        first_page = data.get("firstPage")

        items = data.get("content", [])
        if not items:
            break
        if first_page is True:
            record_count = data.get("count", 0)
            log.info(f"Total records to process for table {table_name}: {record_count}")

        for item in items:
            count += 1
            values = filter_columns(item, allowed_columns)
            values["rowId"] = count
            values["last_updated_at"] = sync_start

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table=table_name, data=values)

        if count % __CHECKPOINT_INTERVAL == 0:
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)

            if record_count and count >= record_count:
                break

        params[__PAGE_OFFSET] += __RECORDS_PER_PAGE


def convert_to_iso(date_str):
    """
    Convert a date string to ISO 8601 format.
    Args:
        date_str: Date string to convert to ISO 8601 format.
    Returns:
        The date string in ISO 8601 format, or the original string if conversion fails or input is None.
    """
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%d %b %Y %I:%M:%S %p")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return date_str  # fallback if it’s already ISO


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
    for col in allowed_columns.keys():
        if col == "rowId":
            continue
        if allowed_columns[col] == "UTC_DATETIME":
            filtered[col] = convert_to_iso(record.get(col))
        elif col in record:
            filtered[col] = record[col]
    return filtered


def sync_orders(params, headers, state, sync_start):
    """
    This function fetches all rows for a table with pagination.
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

    state[table_name][__LAST_UPDATED_AT] = sync_start
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)


def sync_items(params, headers, state, sync_start):
    """
    This function fetches all rows for the 'item' table with pagination.
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

    state[table_name][__LAST_UPDATED_AT] = sync_start
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)


def make_api_request(endpoint, params, headers, retries=__MAX_RETRIES, delay=__RETRY_DELAY):
    """
    This is a generic GET request with retry and backoff logic.
    Args:
        endpoint: The API endpoint to call.
        params: A dictionary of API request parameters.
        headers: A dictionary of API request headers.
        retries: The number of retries for failed requests.
        delay: The delay (in seconds) between retries.
    Returns:
        JSON response from the API.
    """
    url = f"{__BASE_URL}{endpoint}"
    for attempt in range(1, retries + 1):
        try:
            log.info(f"Making API request to: {url} (Attempt {attempt})")
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                wait = delay * attempt
                log.warning(f"Rate limit hit. Retrying in {wait}s...")
                time.sleep(wait)
            elif 400 <= response.status_code < 500:
                response.raise_for_status()  # Raises requests.HTTPError for client errors
            elif 500 <= response.status_code < 600:
                log.warning(f"Server error {response.status_code}, retrying...")
                time.sleep(delay * attempt)
            else:
                raise requests.HTTPError(
                    f"Unexpected HTTP status code {response.status_code} for URL {url} with params {params}. Response body: {response.text}", response=response
                )
        except requests.RequestException as e:
            log.severe(f"Network error: {e}")
            time.sleep(delay * attempt)
    raise requests.RequestException(f"Failed to fetch {url} after {retries} retries")


def get_order_columns():
    """
    Define the order table columns and their data types.
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
        "numberOfInvoices": "INT",
        "invoiced_amount": "DOUBLE",
        "company_code": "STRING",
        "rowId": "INT",
    }


def get_item_columns():
    """
    Define the item table columns and their data types.
    Returns:
        Dictionary mapping column names to their data types for the item table.
    """
    return {
        "documentNumber": "STRING",
        "lineNumber": "INT",
        "quantity": "DOUBLE",
        "unitOfMeasure": "STRING",
        "supplierPart": "STRING",
        "buyerPartId": "STRING",
        "manufacturerPartId": "STRING",
        "description": "STRING",
        "itemShipToName": "STRING",
        "itemShipToStreet": "STRING",
        "itemShipToCity": "STRING",
        "itemShipToState": "STRING",
        "itemShipToPostalCode": "STRING",
        "itemShipToCountry": "STRING",
        "isoCountryCode": "STRING",
        "itemShipToCode": "STRING",
        "itemLocation": "STRING",
        "requestedDeliveryDate": "UTC_DATETIME",
        "requestedShipmentDate": "UTC_DATETIME",
        "rowId": "INT",
    }


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Test the connector locally
    connector.debug()

