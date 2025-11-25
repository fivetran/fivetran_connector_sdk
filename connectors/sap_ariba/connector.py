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
__CHECKPOINT_INTERVAL = 1000
__PAGE_OFFSET = "$skip"


def schema(configuration: dict):
    return [
        {
            "table": "order",
            "primary_key": ["payloadId", "revision", "rowId"],
            "columns": get_order_columns(),
        },
        {
            "table": "item",
            "primary_key": ["documentNumber", "lineNumber", "rowId"],
            "columns": get_item_columns(),
        },
    ]


def update(configuration: dict, state: dict):
    validate_configuration(configuration)
    log.warning("Example: connectors : sap_ariba")
    sync_start = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    headers = {
        "APIKey": configuration.get("api_key"),
        "Accept": "application/json",
        "DataServiceVersion": "2.0",
    }
    params = {"$top": __RECORDS_PER_PAGE, "$count": True}

    sync_orders(params.copy(), headers, state, sync_start)
    sync_items(params.copy(), headers, state, sync_start)


def validate_configuration(configuration: dict):
    if "api_key" not in configuration:
        raise ValueError("Missing required configuration: api_key")


def sync_orders(params, headers, state, sync_start):
    table = "order"
    params[__PAGE_OFFSET] = 0
    if table not in state:
        state[table] = {}
    sync_rows(table, params, headers, state, get_order_columns(), sync_start)
    op.checkpoint(state)


def sync_items(params, headers, state, sync_start):
    table = "item"
    params[__PAGE_OFFSET] = 0
    if table not in state:
        state[table] = {}
    sync_rows(table, params, headers, state, get_item_columns(), sync_start)
    op.checkpoint(state)


def sync_rows(table, params, headers, state, allowed_columns, sync_start):
    count = 0
    total = None

    while total is None or count < total:
        data = make_api_request(table, params=params, headers=headers)
        if not data:
            break
        if data.get("firstPage"):
            total = data.get("count", 0)
            log.info(f"Total records to process for {table}: {total}")

        items = data.get("content", [])
        if not items:
            break

        for item in items:
            count += 1
            record = filter_columns(item, allowed_columns)
            record["rowId"] = count
            record["last_updated_at"] = sync_start
            op.upsert(table=table, data=record)

        if count % __CHECKPOINT_INTERVAL == 0:
            op.checkpoint(state)

        params[__PAGE_OFFSET] += __RECORDS_PER_PAGE


def make_api_request(endpoint, params, headers, retries=__MAX_RETRIES, delay=__RETRY_DELAY):
    url = f"{__BASE_URL}{endpoint}"
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                wait = delay * attempt
                log.warning(f"Rate limit hit. Retrying in {wait}s...")
                time.sleep(wait)
            elif 400 <= response.status_code < 500:
                response.raise_for_status()
            elif 500 <= response.status_code < 600:
                log.warning(f"Server error {response.status_code}, retrying...")
                time.sleep(delay * attempt)
            else:
                raise requests.HTTPError(
                    f"Unexpected HTTP status code: {response.status_code}", response=response
                )
        except requests.RequestException as e:
            log.severe(f"Network error: {e}")
            time.sleep(delay * attempt)
    raise requests.RequestException(f"Failed after {retries} retries")


def filter_columns(record: dict, allowed_columns: dict) -> dict:
    filtered = {}
    for col, col_type in allowed_columns.items():
        if col == "rowId":
            continue
        if col_type == "UTC_DATETIME":
            filtered[col] = convert_to_iso(record.get(col))
        elif col in record:
            filtered[col] = record[col]
    return filtered


def convert_to_iso(date_str):
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%d %b %Y %I:%M:%S %p")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return date_str


def get_order_columns():
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


# Create the connector object
connector = Connector(update=update, schema=schema)

# Local debug block
if __name__ == "__main__":
    with open("configuration.json") as f:
        config = json.load(f)
    connector.debug(config)
