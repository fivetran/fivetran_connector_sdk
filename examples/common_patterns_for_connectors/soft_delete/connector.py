"""
This example demonstrates Fivetran's soft delete feature using the truncate-and-reload pattern.
On each sync, op.truncate() marks all existing warehouse rows as soft-deleted
(_fivetran_deleted = True), then the full current snapshot is re-upserted.
Any row that reappears in the upsert has its _fivetran_deleted flag cleared automatically.
Rows absent from the new snapshot stay marked deleted — this is the soft-delete behaviour.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op


# Dummy data representing the source data
# Snapshot 1 : Initial full load with 5 employee records
__SNAPSHOT_1 = [
    {"id": 1, "name": "Alice", "department": "Engineering"},
    {"id": 2, "name": "Bob", "department": "Marketing"},
    {"id": 3, "name": "Charlie", "department": "Engineering"},
    {"id": 4, "name": "Diana", "department": "HR"},
    {"id": 5, "name": "Edward", "department": "Finance"},
]

# Snapshot 2 : Second sync with employee IDs 4 and 5 removed at the source
__SNAPSHOT_2 = [
    {"id": 1, "name": "Alice", "department": "Engineering"},
    {"id": 2, "name": "Bob", "department": "Marketing"},
    {"id": 3, "name": "Charlie", "department": "Engineering"},
    # IDs 4 and 5 are gone — op.truncate() will leave them soft-deleted after the upserts
]


def schema(configuration: dict):
    """
    Define the schema for the 'employees' table.
    The primary key must be declared so Fivetran can track individual rows for soft-delete.
    """
    return [
        {
            "table": "employees",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "name": "STRING",
                "department": "STRING",
            },
        }
    ]


def fetch_snapshot(sync_count: int) -> list:
    """
    Return dummy source data for the given sync number
    Args:
    sync_count: The current sync number, starting at 1 for the first sync.
    """
    return __SNAPSHOT_1 if sync_count == 1 else __SNAPSHOT_2


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync

    Soft-delete flow (truncate-and-reload pattern)
    -----------------------------------------------
    Step 1 — op.truncate(): marks every existing row in the warehouse table as
             _fivetran_deleted = True.  No rows are physically removed.
    Step 2 — op.upsert() for each record in the current snapshot: rows that still
             exist at the source are re-inserted / updated, and their
             _fivetran_deleted flag is cleared back to False automatically.
             Rows absent from the snapshot remain soft-deleted.
    Step 3 — op.checkpoint(): persists state so the next sync knows its sequence number.
    """
    log.warning(f"Example: Common patterns for connectors - Soft Delete")

    # Store the sync count in the state to determine which snapshot to fetch.
    # This is just for demonstration purposes
    sync_count = state.get("sync_count", 0) + 1

    # Fetch the current snapshot based on the sync count.
    # In a real connector, this would be where you connect to your source system and retrieve the latest data.
    current_snapshot = fetch_snapshot(sync_count)

    # Step 1 — truncate: mark all existing warehouse rows as soft-deleted.
    # Skipped on the first sync because the table is empty — nothing to mark.
    # From sync 2 onwards, op.truncate() sets _fivetran_deleted = True for
    # every row in the destination table.  The subsequent upserts clear that
    # flag for any row still present at the source.
    if state:
        # Call truncate when the state is not empty
        log.info("Truncating 'employees' table — all existing rows marked as soft-deleted.")
        op.truncate(table="employees")

    # Step 2 — upsert the full current snapshot.
    # Re-inserts surviving rows (clearing _fivetran_deleted) and
    # applies any field-level updates.  Rows not present here stay
    # soft-deleted from the truncate above.
    log.info(f"Upserting {len(current_snapshot)} record(s) from current snapshot.")
    for record in current_snapshot:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="employees", data=record)

    # Step 3 — checkpoint
    new_state = {"sync_count": sync_count}
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(new_state)
    log.info(f"Checkpoint saved: {new_state}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging during development,
# such as using IDE debug tools (breakpoints, step-through debugging, etc.).
# Note: This method is not called by Fivetran when executing your connector in production.
# Always test using 'fivetran debug' prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug()

# Tables created
# After Sync 1 — employees table:
# ┌────┬─────────┬─────────────┬──────────────────┐
# │ id │  name   │ department  │ _fivetran_deleted│
# ├────┼─────────┼─────────────┼──────────────────┤
# │  1 │ Alice   │ Engineering │      false       │
# │  2 │ Bob     │ Marketing   │      false       │
# │  3 │ Charlie │ Engineering │      false       │
# │  4 │ Diana   │ HR          │      false       │
# │  5 │ Edward  │ Finance     │      false       │
# └────┴─────────┴─────────────┴──────────────────┘
#
# After Sync 2 — IDs 4 & 5 soft-deleted
# ┌────┬─────────┬─────────────┬──────────────────┐
# │ id │  name   │ department  │ _fivetran_deleted│
# ├────┼─────────┼─────────────┼──────────────────┤
# │  1 │ Alice   │ Engineering │      false       │
# │  2 │ Bob     │ Marketing   │      false       │
# │  3 │ Charlie │ Engineering │      false       │
# │  4 │ Diana   │ HR          │      true        │  ← soft-deleted
# │  5 │ Edward  │ Finance     │      true        │  ← soft-deleted
# └────┴─────────┴─────────────┴──────────────────┘
