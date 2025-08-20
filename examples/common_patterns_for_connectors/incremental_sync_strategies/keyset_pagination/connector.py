# Example: Keyset Pagination Incremental Sync Strategy
# This connector demonstrates keyset pagination for incremental syncs.
# It uses a cursor (e.g., updatedAt timestamp) to fetch new/updated records since the last sync.

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
    log.info("Running keyset pagination incremental sync")
    base_url = configuration.get("base_url", "http://127.0.0.1:5001/pagination/keyset")
    cursor = state.get("last_updated_at", "0001-01-01T00:00:00Z")
    params = {"updated_since": cursor}

    while True:
        response = rq.get(base_url, params=params)
        response.raise_for_status()
        data = response.json().get("data", [])
        if not data:
            break
        for user in data:
            op.upsert(table="user", data=user)
            state["last_updated_at"] = user["updatedAt"]
        op.checkpoint(state)
        scroll_param = response.json().get("scroll_param")
        if not scroll_param:
            break
        params = {"scroll_param": scroll_param}


# required inputs docs https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsrequiredobjectconnector
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)
