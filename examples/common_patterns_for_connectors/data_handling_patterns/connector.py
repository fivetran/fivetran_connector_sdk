"""
This connector demonstrates three common patterns for handling nested data from an API source.
Each sync function targets a different table and uses a different data handling technique:

Pattern 1 — Flatten into columns:
A single nested object is expanded into individual columns on the parent table.

Pattern 2 — Break out into child tables:
A list of nested objects is split into a parent table (users) and a child table (orders).
The child table carries the parent's primary key as a foreign key.

Pattern 3 — Write as a JSON blob:
A deeply nested or highly variable object is stored as a single JSON column.

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

# Import datetime utilities to record the sync timestamp in state.
from datetime import datetime, timezone

# Import required classes from fivetran_connector_sdk.
from fivetran_connector_sdk import Connector

# For enabling logs in your connector code.
from fivetran_connector_sdk import Logging as log

# For supporting data operations like upsert(), update(), delete() and checkpoint().
from fivetran_connector_sdk import Operations as op

# Import the mock API module that simulates the source API returning nested user data.
import mock_api

# Number of upsert operations to perform before emitting an intermediate checkpoint.
# Checkpointing at regular intervals prevents data loss if a sync is interrupted mid-table.
# Adjust this value based on the size of your dataset and the acceptable replay window.
__CHECKPOINT_INTERVAL = 50


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        # Pattern 1: Flatten into columns.
        # All nested structures are expanded into a single flat table.
        {
            # Name of the table in the destination, required.
            "table": "users_flattened",
            # Primary key column(s) for the table.
            # We recommend defining a primary_key for each table. If not provided, Fivetran
            # computes _fivetran_id from all column values.
            "primary_key": ["user_id", "order_id"],
            # Definition of columns and their types, optional.
            # For any columns not listed here, types will be inferred from the upserted data.
            # We recommend not defining all columns here to allow for schema evolution.
            "columns": {
                "user_id": "STRING",
                "name": "STRING",
                "address_city": "STRING",
                "address_zip": "STRING",
                "order_id": "STRING",
                "amount": "DOUBLE",
            },
        },
        # Pattern 2a: Parent table.
        # Contains all scalar fields from the record, including flattened address columns.
        # The 'orders' list is moved to a child table to avoid row duplication.
        {
            "table": "users",
            "primary_key": ["user_id"],
            "columns": {
                "user_id": "STRING",
                "name": "STRING",
                "address_city": "STRING",
                "address_zip": "STRING",
            },
        },
        # Pattern 2b: Child table.
        # Each order becomes its own row.
        # A composite primary key (user_id, order_id) is
        # required to uniquely identify each row
        {
            "table": "orders",
            "primary_key": ["user_id", "order_id"],
            "columns": {
                "user_id": "STRING",
                "order_id": "STRING",
                "amount": "DOUBLE",
            },
        },
        # Pattern 3: JSON blob column.
        # The entire nested portion of the record (address + orders) is stored as a single
        # JSON column, preserving the full structure without requiring schema changes as
        # the source evolves.
        {
            "table": "users_data",
            "primary_key": ["user_id"],
            "columns": {
                "user_id": "STRING",
                "name": "STRING",
                "data": "JSON",
            },
        },
    ]


def sync_flattened(new_state: dict, users: list):
    """
    Pattern 1 — Flatten all nested structures into a single flat table.
    The 'address' fields are promoted to individual top-level columns. Each order in the
    'orders' list becomes its own row, with parent user fields (user_id, name, address)
    repeated on every order row. A composite primary key (user_id, order_id) is required
    because order_id values are sequential per user and not globally unique.

    Args:
        new_state: the state dictionary to checkpoint; updated to the current time before each checkpoint.
        users: the list of user records returned by the single mock API call.
    """
    log.info("Syncing Pattern 1: Flatten all nested structures into a single flat table")
    row_count = 0

    for user in users:
        # Extract the nested address object, defaulting to an empty dict if absent.
        address = user.get("address", {})

        for order in user.get("orders", []):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(
                table="users_flattened",
                data={
                    "user_id": user["user_id"],
                    "name": user["name"],
                    # Flatten the nested address fields into individual top-level columns.
                    "address_city": address.get("city"),
                    "address_zip": address.get("zip"),
                    # Flatten each order into its own row alongside the parent user fields.
                    "order_id": order["order_id"],
                    "amount": order["amount"],
                },
            )
            row_count += 1

            # Checkpoint every __CHECKPOINT_INTERVAL upserts so the sync can safely resume
            # from this point if it is interrupted before the table finishes.
            if row_count % __CHECKPOINT_INTERVAL == 0:
                # Update the state timestamp to the current time before checkpointing,
                # so each checkpoint records exactly when it was written.
                new_state["last_synced_at"] = datetime.now(timezone.utc).isoformat()
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                op.checkpoint(new_state)

    # Update the state timestamp to the current time before checkpointing,
    # so each checkpoint records exactly when it was written.
    new_state["last_synced_at"] = datetime.now(timezone.utc).isoformat()
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(new_state)

    log.info(f"Upserted {row_count} row(s) into 'users_flattened'")


def sync_parent_child(new_state: dict, users: list):
    """
    Pattern 2 — Break a nested list out into a parent table and a child table.
    All scalar fields (including the flattened address) are written to the parent 'users'
    table. Each order in the nested list is written to the child 'orders' table with a
    composite primary key (user_id, order_id).

    Args:
        new_state: the state dictionary to checkpoint; updated to the current time before each checkpoint.
        users: the list of user records returned by the single mock API call.
    """
    log.info("Syncing Pattern 2: Break nested orders list into parent/child tables")
    # user_count tracks rows written to the parent 'users' table.
    user_count = 0
    # order_count tracks rows written to the child 'orders' table.
    order_count = 0

    for user in users:
        # Extract the nested address object, defaulting to an empty dict if absent.
        address = user.get("address", {})

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(
            table="users",
            data={
                "user_id": user["user_id"],
                "name": user["name"],
                # Include flattened address columns on the parent row so all scalar
                # data is accessible without joining to the child table.
                "address_city": address.get("city"),
                "address_zip": address.get("zip"),
            },
        )
        user_count += 1

        # Checkpoint every __CHECKPOINT_INTERVAL upserts so the sync can safely resume
        # from this point if it is interrupted before the table finishes.
        if user_count % __CHECKPOINT_INTERVAL == 0:
            # Update the state timestamp to the current time before checkpointing,
            # so each checkpoint records exactly when it was written.
            new_state["last_synced_at"] = datetime.now(timezone.utc).isoformat()
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(new_state)

        for order in user.get("orders", []):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            # user_id is included in every order row as part of the composite primary key,
            # linking each child row back to its parent in the 'users' table.
            op.upsert(
                table="orders",
                data={
                    "user_id": user["user_id"],
                    "order_id": order["order_id"],
                    "amount": order["amount"],
                },
            )
            order_count += 1

            # Checkpoint every __CHECKPOINT_INTERVAL upserts so the sync can safely resume
            # from this point if it is interrupted before the table finishes.
            if order_count % __CHECKPOINT_INTERVAL == 0:
                # Update the state timestamp to the current time before checkpointing,
                # so each checkpoint records exactly when it was written.
                new_state["last_synced_at"] = datetime.now(timezone.utc).isoformat()
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                op.checkpoint(new_state)

    # Update the state timestamp to the current time before checkpointing,
    new_state["last_synced_at"] = datetime.now(timezone.utc).isoformat()
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(new_state)

    log.info(f"Upserted {user_count} row(s) into 'users' and {order_count} row(s) into 'orders'")


def sync_json_blob(new_state: dict, users: list):
    """
    Pattern 3 — Store the entire nested portion of the record as a single JSON blob column.
    Both the 'address' object and the 'orders' list are combined and stored together in the
    'data' JSON column. The SDK handles serialization; no manual json.dumps() is required.

    Args:
        new_state: the state dictionary to checkpoint; updated to the current time before each checkpoint.
        users: the list of user records returned by the single mock API call.
    """
    log.info("Syncing Pattern 3: Write nested address and orders as a JSON blob")
    row_count = 0

    for user in users:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(
            table="users_data",
            data={
                "user_id": user["user_id"],
                "name": user["name"],
                # Combine the nested address and orders into a single JSON blob.
                # The SDK serializes the dict directly into the JSON column;
                # no manual json.dumps() call is needed.
                "data": {
                    "address": user.get("address", {}),
                    "orders": user.get("orders", []),
                },
            },
        )
        row_count += 1

        # Checkpoint every __CHECKPOINT_INTERVAL upserts so the sync can safely resume
        # from this point if it is interrupted before the table finishes.
        if row_count % __CHECKPOINT_INTERVAL == 0:
            # Update the state timestamp to the current time before checkpointing,
            # so each checkpoint records exactly when it was written.
            new_state["last_synced_at"] = datetime.now(timezone.utc).isoformat()
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(new_state)

    # Update the state timestamp to the current time before checkpointing,
    # so each checkpoint records exactly when it was written.
    new_state["last_synced_at"] = datetime.now(timezone.utc).isoformat()
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(new_state)

    log.info(f"Upserted {row_count} row(s) into 'users_data'")


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details.
        state: A dictionary containing state information from previous runs.
               The state dictionary is empty for the first sync or for any full re-sync.
    """

    log.warning("Example: Common Patterns For Connectors - Data Handling Patterns")

    # Initialise new_state from the last known sync timestamp.
    # state.get() reads the value saved by the previous sync; if this is the first sync
    # (state is empty), it defaults to the current UTC time.
    # new_state is passed to each sync function and updated to the current time before
    # every checkpoint, so each checkpoint records exactly when it was written.
    # For more information on state management, refer to:
    # https://fivetran.com/docs/connector-sdk/working-with-connector-sdk#workingwithstatejsonfile
    new_state = {
        "last_synced_at": state.get("last_synced_at", datetime.now(timezone.utc).isoformat())
    }

    # Make a single API call to fetch all users with their nested data.
    users = mock_api.get_users()

    # Sync Pattern 1: flatten the nested address object into individual columns.
    sync_flattened(new_state, users)

    # Sync Pattern 2: split the nested orders list into parent (users) and child (orders) tables.
    sync_parent_child(new_state, users)

    # Sync Pattern 3: store the nested address and orders as a single JSON blob column.
    sync_json_blob(new_state, users)

    log.info(f"Sync complete. Last synced at: {new_state['last_synced_at']}")


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
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from
    # your IDE.
    connector.debug()

# ── Resulting tables ──────────────────────────────────────────────────────────
#
# Pattern 1 — users_flattened (composite PK: user_id + order_id):
# ┌─────────┬───────┬──────────────┬─────────────┬──────────┬────────┐
# │ user_id │ name  │ address_city │ address_zip │ order_id │ amount │
# ├─────────┼───────┼──────────────┼─────────────┼──────────┼────────┤
# │  uuid   │ Alice │ New York     │ 10001       │  ORD-1   │  20.5  │
# │  uuid   │ Alice │ New York     │ 10001       │  ORD-2   │  35.0  │
# └─────────┴───────┴──────────────┴─────────────┴──────────┴────────┘
#
# Pattern 2 — users (parent):
# ┌─────────┬───────┬──────────────┬─────────────┐
# │ user_id │ name  │ address_city │ address_zip │
# ├─────────┼───────┼──────────────┼─────────────┤
# │  uuid   │ Alice │ New York     │ 10001       │
# └─────────┴───────┴──────────────┴─────────────┘
#
# Pattern 2 — orders (child, composite PK: user_id + order_id):
# ┌─────────┬──────────┬────────┐
# │ user_id │ order_id │ amount │
# ├─────────┼──────────┼────────┤
# │  uuid   │  ORD-1   │  20.5  │
# │  uuid   │  ORD-2   │  35.0  │
# └─────────┴──────────┴────────┘
#
# Pattern 3 — users_data:
# ┌─────────┬───────┬─────────────────────────────────────────────────────────┐
# │ user_id │ name  │                         data                            │
# ├─────────┼───────┼─────────────────────────────────────────────────────────┤
# │  uuid   │ Alice │ {"address": {"city": "New York", ...}, "orders": [...]} │
# └─────────┴───────┴─────────────────────────────────────────────────────────┘
