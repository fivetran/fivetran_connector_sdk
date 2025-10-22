# This is an example for how to work with the fivetran_connector_sdk module.
# It defines a simple 'update' method, which upserts GitHub repo traffic data from GitHub's API into a Fivetran connector.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import required libraries
import time
from datetime import datetime, timezone
import requests
import json

# Constants
__MAX_RETRIES = 3
__RETRY_DELAY = 3 # seconds
__BASE_URL = "https://sandbox.api.sap.com/ariba/api/purchase-orders/v1/sandbox/"
__RECORDS_PER_PAGE = 100
__EPOCH_START_DATE = "1970-01-01T00:00:00Z"
__LAST_UPDATED_AT = "last_updated_at"
__CHECKPOINT_INTERVAL = 1000  # Checkpoint after processing every 1000 rows

# date_format = "%d %b %Y %I:%M:%S %p"
# naive_datetime = datetime.strptime(date_string, date_format)
def schema(configuration: dict):
    return [{
    "table": "orders", # Name of the table in the destination.
    "primary_key": ["payload_id", "revision", "row_id"],
    "columns": getOrderColumns() # Define the columns and their data types.
    },
    {
    "table": "items", # Name of the table in the destination.
    "primary_key": ["document_number", "line_number", "row_id"],
    "columns": getItemColumns() # Define the columns and their data types.
    }]

def update(configuration: dict, state: dict):
    """Define the update function, which is a required function, and is called by Fivetran during each sync."""
    current_sync_start = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    headers = {
    "APIKey": configuration.get("APIKey"),
    "Accept": "application/json",
    "DataServiceVersion": "2.0"
    }

    log.info("Starting sync from SAP Ariba API...")

    params = {}
    params["$top"] = __RECORDS_PER_PAGE
    params["$$count"] = True

    sync_orders(params, headers, state, current_sync_start)
    sync_items(params, headers, state, current_sync_start)

    # Checkpoint state if needed
    op.checkpoint(state)

def sync_rows(table_name, params, headers, state, allowed_columns, sync_start):
    """Fetch all rows for a table with pagination"""
    count = 0
    record_count = None
    while record_count is None or count < record_count:
        data = make_api_request(f"{table_name}", params=params, headers=headers)
        firstPage = data.get("firstPage")

        items = data.get("content", [])
        if not items:
            break
        if firstPage is True:
            record_count = data.get("count", 0)
            log.info(f"Total records to process for table {table_name}: {record_count}")

        for item in items:
            count += 1
            values = filter_columns(item, allowed_columns)
            values["row_id"] = count
            values["last_updated_at"] = sync_start

            op.upsert(table=table_name, data=values)

            if count % __CHECKPOINT_INTERVAL == 0:
                op.checkpoint(state) # for frequent checkpointing

            if record_count and count >= record_count:
                break

        params["$skip"] += __RECORDS_PER_PAGE


def convert_to_iso(date_str):
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%d %b %Y %I:%M:%S %p")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return date_str  # fallback if it’s already ISO

def filter_columns(record: dict, allowed_columns: dict) -> dict:
    filtered = {}
    for col in allowed_columns.keys():
        if col == "row_id":
            continue
        if allowed_columns[col] == "UTC_DATETIME":
            filtered[col] = convert_to_iso(record.get(col))
        elif col in record:
            filtered[col] = record[col]
    return filtered

def sync_orders(params, headers, state, sync_start):
    """Fetch all rows for a table with pagination"""
    table_name = "orders"
    last_updated_at = last_updated_at = state.get(table_name, {}).get(__LAST_UPDATED_AT, __EPOCH_START_DATE)

    params["$skip"] = 0

    if state.get(table_name) is None:
        state[table_name] = {}

    sync_rows(table_name, params, headers, state, getOrderColumns(), sync_start)

    state[table_name][__LAST_UPDATED_AT] = sync_start
    op.checkpoint(state)


def sync_items(params, headers, state, sync_start):
    """Fetch all rows for a table with pagination"""
    table_name = "items"
    last_updated_at = last_updated_at = state.get(table_name, {}).get(__LAST_UPDATED_AT, __EPOCH_START_DATE)

    params["$skip"] = 0

    if state.get(table_name) is None:
            state[table_name] = {}

    sync_rows(table_name, params, headers, state, getItemColumns(), sync_start)

    state[table_name][__LAST_UPDATED_AT] = sync_start
    op.checkpoint(state)

def to_snake_case(s: str) -> str:
    # Replace spaces and hyphens with underscores
    s = re.sub(r'[\s\-]+', '_', s)
    # Convert CamelCase or mixed case to lowercase with underscores
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    return s.lower()

def make_api_request(endpoint, params, headers, retries=__MAX_RETRIES, delay=__RETRY_DELAY):
    """Generic GET with retry and backoff"""
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
                raise Exception(f"Client error {response.status_code}: {response.text}")
            elif 500 <= response.status_code < 600:
                log.warning(f"Server error {response.status_code}, retrying...")
                time.sleep(delay * attempt)
            else:
                raise Exception(f"Unexpected response: {response.status_code}")
        except requests.RequestException as e:
                log.severe(f"Network error: {e}")
                time.sleep(delay * attempt)
        raise Exception(f"Failed to fetch {url} after {retries} retries")

def getOrderColumns():
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
    "row_id": "INT"
    }

def getItemColumns():
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
    "row_id": "INT"
    }


# This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug(configuration=configuration)


# Fivetran debug results
# Operation       | Calls
# ----------------+------------
# Upserts         | 859
# Updates         | 0
# Deletes         | 0
# Truncates       | 0
# SchemaChanges   | 2
# Checkpoints     | 3