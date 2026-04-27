# This example demonstrates how to mimic Fivetran's History Mode using the Connector SDK.
#
# WHAT IS HISTORY MODE?
# Fivetran's History Mode is a sync mode that preserves all historical versions of a record
# in the destination. Instead of overwriting a row when the source record changes, History Mode
# inserts a new row for each observed state, keeping a full audit trail of every change.
#
# WHY THE CONNECTOR SDK DOES NOT NATIVELY SUPPORT HISTORY MODE
# The Connector SDK does not support History Mode. However, you can mimic its behavior by
# including a timestamp column (such as updatedAt) as part of a composite primary key.
#
# HOW THE COMPOSITE PRIMARY KEY APPROACH WORKS
# When the primary key is ["id", "updatedAt"], each unique (id, updatedAt) pair is treated
# as a distinct row. If a record's updatedAt value changes between syncs, Fivetran upserts a
# NEW row rather than overwriting the existing one — which is exactly what History Mode does.
#
# ROLE OF updatedAt WHEN createdAt IS ABSENT
# For sources that do not provide a createdAt field, updatedAt at the time of first sync equals
# the record's first-observed time — functionally equivalent to createdAt. So updatedAt alone
# is sufficient for both the composite primary key and the incremental cursor.
#
# KEY LIMITATION
# Only the state of a record at the time of each sync is captured. If a record is updated
# multiple times between two syncs, only the most recent updatedAt value will appear in the
# destination. Intermediate states are lost. For details, see:
# https://fivetran.com/docs/core-concepts/sync-modes/history-mode#changestodatabetweensyncs
#
# See the Technical Reference documentation
# (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# Import requests for fetching data over API calls
import requests as rq

# Import time for retry backoff on rate-limited responses
import time

from datetime import datetime, timedelta

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Base URL for the Fivetran API Playground incremental timestamp endpoint.
# Start the playground locally with: playground start
__BASE_URL = "http://127.0.0.1:5001/incremental/timestamp"
__MAX_RETRIES = 3
__CHECKPOINT_INTERVAL = 30


def get_with_retry(url, params):
    """Make a GET request, retrying on 429 using the Retry-After header."""
    for attempt in range(__MAX_RETRIES):
        response = rq.get(url, params=params)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            log.warning(
                f"Rate limited (429). Retrying after {retry_after} seconds (attempt {attempt + 1}/{__MAX_RETRIES})."
            )
            time.sleep(retry_after)
            continue
        response.raise_for_status()
        return response
    response.raise_for_status()
    return response


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
            "table": "user",
            # Composite primary key: each (id, updatedAt) pair is a distinct row.
            # This is the key difference from a standard upsert connector — it is what
            # produces History Mode-like behavior.
            "primary_key": ["id", "updatedAt"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "email": "STRING",
                "address": "STRING",
                "company": "STRING",
                "job": "STRING",
                # createdAt is intentionally excluded: for sources without createdAt,
                "updatedAt": "UTC_DATETIME",
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
        "Example: Common Patterns For Connectors - History Mode Mimicry Using Composite Primary Key"
    )

    # On the first sync, state is empty so the cursor defaults to the earliest possible timestamp,
    # ensuring all records are fetched. On subsequent syncs, only records updated since the last
    # checkpoint are fetched — this is standard incremental sync behavior.
    last_ts = state.get("last_timestamp", "0001-01-01T00:00:00Z")
    params = {"since": last_ts}

    while True:
        response = get_with_retry(__BASE_URL, params)
        data = response.json().get("data", [])
        if not data:
            break
        count = 0
        for record in data:
            # Strip createdAt before upserting — this connector treats updatedAt as the sole
            # timestamp. For sources that do provide createdAt, simply omit it from the schema
            # and remove it here so it does not land in the destination.
            record.pop("createdAt", None)

            # Upsert the record. Because the primary key is ["id", "updatedAt"], a record whose
            # updatedAt has changed since the last sync will produce a NEW row in the destination
            # rather than overwriting the existing one. This is History Mode behavior.
            op.upsert(table="user", data=record)
            count += 1

            # Advance the cursor to the latest updatedAt seen so far.
            # Prerequisite: records are returned sorted by updatedAt ascending (the playground guarantees this).
            state["last_timestamp"] = record["updatedAt"]

            if count == __CHECKPOINT_INTERVAL:
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(state)

        # Advance the cursor by 1 second so the next request is exclusive of the last seen
        # timestamp. Without this, a >= filter on the API side would re-fetch the boundary
        # record at the start of every page, producing harmless but wasteful duplicate upserts.
        last_dt = datetime.strptime(state["last_timestamp"], "%Y-%m-%dT%H:%M:%SZ")
        params["since"] = (last_dt + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)


# This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug()

# To observe History Mode behavior:
# 1. Start the playground: playground start
# 2. Run: fivetran debug  (first sync — all records loaded, cursor saved to state)
# 3. Run: fivetran debug  (incremental sync — records with a new updatedAt produce new rows)
# 4. Inspect warehouse.db:
#    SELECT id, updatedAt FROM user ORDER BY id, updatedAt;
#    Rows with the same id but different updatedAt values confirm that history is being preserved.
