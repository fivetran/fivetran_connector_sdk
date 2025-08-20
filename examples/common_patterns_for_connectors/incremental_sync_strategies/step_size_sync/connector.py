# Example: Step-size Incremental Sync Strategy
# This connector demonstrates step-size incremental sync.
# It uses ID ranges to fetch records in batches when pagination/count is not supported, saving the current ID as state.

# Global configuration variables
BASE_URL = "http://127.0.0.1:5001/incremental/step"
INITIAL_ID = 1
STEP_SIZE = 1000
MAX_ID = 100000

# Importing requests for fetching data over api calls
import requests as rq

# Import required classes from fivetran_connector_sdk.
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings required for the connector.
    """
    return [
        {
            "table": "user",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "email": "STRING",
                "address": "STRING",
                "company": "STRING",
                "job": "STRING",
                "updatedAt": "UTC_DATETIME",
                "createdAt": "UTC_DATETIME",
            },
        }
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.info("Running step-size incremental sync")
    current_id = state.get("current_id", INITIAL_ID)

    while current_id <= MAX_ID:
        params = {"start_id": current_id, "end_id": current_id + STEP_SIZE - 1}
        response = rq.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json().get("data", [])
        if not data:
            break
        for user in data:
            op.upsert(table="user", data=user)
        current_id += STEP_SIZE
        state["current_id"] = current_id
        # Checkpoint the state after processing each batch to ensure progress is saved
        op.checkpoint(state)
        if len(data) < STEP_SIZE:
            break


# required inputs docs https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsrequiredobjectconnector
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug()
