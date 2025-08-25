"""This connector demonstrates replay incremental sync with buffer using the Fivetran Connector SDK.
It uses timestamp-based sync with a buffer (goes back X hours from last timestamp) for read-replica scenarios with replication lag.
This example is intended for learning purposes and uses the fivetran-api-playground package to mock the API responses locally. It is not meant for production use.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# Importing requests for fetching data over api calls
import requests as rq

# Importing datetime for timestamp manipulation
from datetime import datetime, timedelta

# Import required classes from fivetran_connector_sdk.
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Private global configuration variables
__BASE_URL = "http://127.0.0.1:5001/incremental/timestamp"
__BUFFER_HOURS = 2


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
    log.warning(
        "Example: Common Patterns For Connectors - Incremental Sync Strategies - Replay Sync Example"
    )

    last_ts = state.get("last_timestamp", "0001-01-01T00:00:00Z")

    # Apply buffer by going back buffer_hours from the last timestamp
    # This is useful for read-replica scenarios where there might be replication lag
    if last_ts != "0001-01-01T00:00:00Z":
        last_dt = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
        buffer_dt = last_dt - timedelta(hours=__BUFFER_HOURS)
        buffer_ts = buffer_dt.isoformat().replace("+00:00", "Z")
    else:
        buffer_ts = last_ts

    params = {"since": buffer_ts}
    response = rq.get(__BASE_URL, params=params)
    response.raise_for_status()
    data = response.json().get("data", [])
    for user in data:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted,
        op.upsert(table="user", data=user)
        state["last_timestamp"] = user["updatedAt"]

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Test the connector locally
    connector.debug()
