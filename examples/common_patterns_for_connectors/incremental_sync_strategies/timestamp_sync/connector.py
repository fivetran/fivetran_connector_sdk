# Example: Timestamp-based Incremental Sync Strategy
# This connector demonstrates timestamp-based incremental sync.
# It uses a timestamp to fetch all records updated since the last sync, saving the latest timestamp as state.

# Global configuration variables
BASE_URL = "http://127.0.0.1:5001/incremental/timestamp"

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
    log.info("Running timestamp-based incremental sync")
    last_ts = state.get("last_timestamp", "0001-01-01T00:00:00Z")
    params = {"since": last_ts}

    while True:
        response = rq.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json().get("data", [])
        if not data:
            break
        for user in data:
            op.upsert(table="user", data=user)
            state["last_timestamp"] = user["updatedAt"]
        # Checkpoint the state after processing all records to ensure state is saved
        op.checkpoint(state)
        # Assume API returns all new/updated records since last_timestamp in one call
        break


# required inputs docs https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsrequiredobjectconnector
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug()
