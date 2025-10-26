# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

import requests  # For making HTTP requests to the Iterate API
import json  # For reading configuration from a JSON file
import time  # For handling retries and delays
from datetime import datetime, timezone  # For handling date and time operations
import re

__BASE_URL = "https://coda.io/apis/v1" # Base URL for Coda API
__MAX_RETRIES = 3  # Number of retries for API calls
__RETRY_DELAY = 2  # Initial delay between retries in seconds
__EPOCH_START_DATE = "1970-01-01T00:00:00Z"  # Default start date if none provided
__RECORDS_PER_PAGE = 1000  # Number of records to fetch per page
__NEXT_SYNC_TOKEN = "next_sync_token"  # Placeholder for next sync token
__LAST_UPDATED_AT = "last_updated_at"  # Placeholder for next sync token
__CHECKPOINT_INTERVAL = 1000  # Checkpoint after processing every 1000 rows


def make_api_request(endpoint, params, headers, retries=__MAX_RETRIES, delay=__RETRY_DELAY):
    """
    Make a GET request to the Coda API with retries and exponential backoff.
    Args: param endpoint: API endpoint to call
          param params: Query parameters for the API call
          param headers: Headers for the API call
          param retries: Number of retries for the API call
          param delay: Initial delay between retries in seconds
    Returns: JSON response from the API
    Raises: Exception if the API call fails after retries
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

def sync_rows(table_name, params, headers, last_updated_at, state, doc_id):
    """
    Generic function to sync rows for a given table
    Args: param table_name: Name of the table to sync
          param params: Query parameters for the API call
          param headers: Headers for the API call
          param last_updated_at: Timestamp of the last update
          param state: State dictionary to track sync progress
          param doc_id: Coda document ID
    Returns: Next sync token if available
    Raises: Exception if the API call fails
    """
    count = 0
    next_sync_token = None
    while True:
        data = make_api_request(f"/docs/{doc_id}/tables/{table_name}/rows", params=params, headers=headers)

        items = data.get("items", [])
        if not items:
            break
        next_sync_token = data.get("nextSyncToken")
        for item in items:
            updated_at = item.get("updatedAt")
            if updated_at < last_updated_at:
                break
            count += 1
            values = item.get("values", {})
            values = {to_snake_case(k): v for k, v in values.items()}
            values["row_id"] = item.get("id")
            values["created_at"] = item.get("createdAt")
            values["updated_at"] = item.get("updatedAt")

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted.
            op.upsert(table=table_name, data=values)

            if count % __CHECKPOINT_INTERVAL == 0:
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(state)

        next_page = data.get("nextPageToken")
        if not next_page:
            break
        params["pageToken"] = next_page
    return next_sync_token

def sync_orders(params, headers, state, sync_start, doc_id):
    """
    Sync orders table
    Args: param params: parameters for the API call
          param headers: headers for the API call
          param state: state dictionary to track sync progress
          param sync_start: sync start timestamp
          param doc_id: document ID
    """
    table_name = "order"
    last_updated_at = state.get(table_name, {}).get(__LAST_UPDATED_AT, __EPOCH_START_DATE)

    next_sync_token = state.get(table_name, {}).get(__NEXT_SYNC_TOKEN)
    if next_sync_token:
        params["nextSyncToken"] = next_sync_token
    if state.get(table_name) is None:
        state[table_name] = {}

    next_sync_token = sync_rows(table_name, params, headers, last_updated_at, state, doc_id)

    if next_sync_token:
        state[table_name][__NEXT_SYNC_TOKEN] = next_sync_token

    state[table_name][__LAST_UPDATED_AT] = sync_start
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)


def sync_customer_feedback(params, headers, state, sync_start, doc_id):
    """
    Sync customer_feedback table
    Args: param params: parameters for the API call
          param headers: headers for the API call
          param state: state dictionary to track sync progress
          param sync_start: sync start timestamp
          param doc_id: document ID
    """
    table_name = "customer_feedback"
    last_updated_at = state.get(table_name, {}).get(__LAST_UPDATED_AT, __EPOCH_START_DATE)

    next_sync_token = state.get(table_name, {}).get(__NEXT_SYNC_TOKEN)
    if next_sync_token:
        params["nextSyncToken"] = next_sync_token
    if state.get(table_name) is None:
            state[table_name] = {}
    next_sync_token = sync_rows(table_name, params, headers, last_updated_at, state, doc_id)

    if next_sync_token:
        state[table_name][__NEXT_SYNC_TOKEN] = next_sync_token

    state[table_name][__LAST_UPDATED_AT] = sync_start
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)

def schema(configuration: dict):
    return [
            {
                "table": "order",  # Name of the table in the destination, required.
                "primary_key": ["id"],  # Primary key column(s) for the table, optional.
                "columns": {
                    "id": "string",
                    "region": "string",
                    "rep": "string",
                    "item": "string",
                    "units": "double",
                    "unit_cost": "double",
                    "total": "double"
                    }
            },
            {
                "table": "customer_feedback",  # Name of the table in the destination, required.
                "primary_key": ["id"],  # Primary key column(s) for the table, optional.
                "columns": {
                    "id": "string",
                    "customer_id": "string",
                    "first_name": "string",
                    "last_name": "string",
                    "email_address": "string",
                    "number_of_complaints": "int"
                    }
            }
        ]

# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: a dictionary that contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary that contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: dict, state: dict):
    current_sync_start = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    log.info("Starting sync from Coda API...")

    api_token = configuration.get("api_token")
    if not api_token:
        raise ValueError("API token is required in configuration")

    headers = {"Authorization": f"Bearer {api_token}"}

    doc_id = configuration.get("doc_id")

    params = {}
    params["useColumnNames"] = "true"
    params["sortBy"] = "updatedAt" # this sorts the data in ascending order of updatedAt field
    params["pageSize"] = __RECORDS_PER_PAGE

    sync_orders(params, headers, state, current_sync_start, doc_id)
    sync_customer_feedback(params, headers, state, current_sync_start, doc_id)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)

def to_snake_case(s: str) -> str:
    """
    Convert a string to snake_case.
    Args: param s: Input string
    Returns: Snake_case string
    """
    # Replace spaces and hyphens with underscores
    s = re.sub(r'[\s\-]+', '_', s)
    # Convert CamelCase or mixed case to lowercase with underscores
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    return s.lower()


# Create connector instance
connector = Connector(update=update, schema=schema)

# For local testing
if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)

# Fivetran debug results:

# Oct 13, 2025 05:52:44 PM INFO: Fivetran-Tester-Process: Checkpoint: {"order": {"next_sync_token": "eyJsaW1pdCI6MjAwLCJvZmZzZXQiOjAsInNvcnRCeSI6InVwZGF0ZWRBdCIsInN0YXJ0QXQiOjE3NjAyNTg3NTUuMTc4LCJ1c2VDb2x1bW5OYW1lcyI6dHJ1ZX0", "last_updated_at": "2025-10-13T12:22:35Z"}, "customer_feedback": {"next_sync_token": "eyJsaW1pdCI6MjAwLCJvZmZzZXQiOjAsInNvcnRCeSI6InVwZGF0ZWRBdCIsInN0YXJ0QXQiOjE3NjAxOTI5OTYuMTE3LCJ1c2VDb2x1bW5OYW1lcyI6dHJ1ZX0", "last_updated_at": "2025-10-13T12:22:35Z"}}
# Oct 13, 2025 05:52:44 PM INFO: Fivetran-Tester-Process: SYNC PROGRESS:
# Operation       | Calls
# ----------------+------------
# Upserts         | 9
# Updates         | 0
# Deletes         | 0
# Truncates       | 0
# SchemaChanges   | 2
# Checkpoints     | 3
# Note: Fivetran debug's performance is limited by your local machine's resources. Your connector will run faster in production.
# read about production system resources at https://fivetran.com/docs/connector-sdk/working-with-connector-sdk#systemresources
# Oct 13, 2025 05:52:44 PM INFO: Fivetran-Tester-Process: Sync SUCCEEDED

