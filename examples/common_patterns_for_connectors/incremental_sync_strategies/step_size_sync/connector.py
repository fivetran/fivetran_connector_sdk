# Example: Step-size Incremental Sync Strategy
# This connector demonstrates step-size incremental sync.
# It uses ID ranges to fetch records in batches when pagination/count is not supported, saving the current ID as state.

# Importing Json for parsing configuration
import json

# Importing requests for fetching data over api calls
import requests as rq

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector, Logging as log, Operations as op


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
    base_url = configuration.get("base_url", "http://127.0.0.1:5001/incremental/step")
    current_id = state.get("current_id", int(configuration.get("initial_id", 1)))
    step_size = int(configuration.get("step_size", 1000))
    max_id = int(configuration.get("max_id", 100000))  # Safety limit

    while current_id <= max_id:
        params = {"start_id": current_id, "end_id": current_id + step_size - 1}
        response = rq.get(base_url, params=params)
        response.raise_for_status()
        data = response.json().get("data", [])
        if not data:
            break
        for user in data:
            op.upsert(table="user", data=user)
        current_id += step_size
        state["current_id"] = current_id
        op.checkpoint(state)
        if len(data) < step_size:
            break


# required inputs docs https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsrequiredobjectconnector
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)
