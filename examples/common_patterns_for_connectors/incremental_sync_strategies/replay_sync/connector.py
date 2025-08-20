# Example: Replay Incremental Sync Strategy (with Buffer)
# This connector demonstrates replay incremental sync with buffer.
# It uses timestamp-based sync with a buffer (goes back X hours from last timestamp) for read-replica scenarios with replication lag.

import json
import requests as rq
from datetime import datetime, timedelta
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
    log.info("Running replay incremental sync with buffer")
    base_url = configuration.get("base_url", "http://127.0.0.1:5001/incremental/replay")
    buffer_hours = configuration.get("buffer_hours", 2)
    last_ts = state.get("last_timestamp", "0001-01-01T00:00:00Z")

    # Apply buffer by going back buffer_hours from the last timestamp
    # This is useful for read-replica scenarios where there might be replication lag
    if last_ts != "0001-01-01T00:00:00Z":
        last_dt = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
        buffer_dt = last_dt - timedelta(hours=buffer_hours)
        buffer_ts = buffer_dt.isoformat().replace("+00:00", "Z")
    else:
        buffer_ts = last_ts

    params = {"since": buffer_ts}
    while True:
        response = rq.get(base_url, params=params)
        response.raise_for_status()
        data = response.json().get("data", [])
        if not data:
            break
        for user in data:
            op.upsert(table="user", data=user)
            state["last_timestamp"] = user["updatedAt"]
        op.checkpoint(state)
        # Assume API returns all records since buffer_ts in one call
        break


# required inputs docs https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsrequiredobjectconnector
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)
