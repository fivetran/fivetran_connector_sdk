# This connector demonstrates three common patterns for handling nested data from an API source.
# Each sync function targets a different table and uses a different data handling technique:
#
# Pattern 1 — Flatten into columns:
#   A single nested object (address) is expanded into individual columns on the parent table.
#
# Pattern 2 — Break out into child tables:
#   A list of nested objects (orders) is split into a parent table (users) and a child table (orders).
#   The child table carries the parent's primary key as a foreign key.
#
# Pattern 3 — Write as a JSON blob:
#   A deeply nested or highly variable object (metadata) is stored as a single JSON column.
#
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# Import datetime utilities to record the sync timestamp in state.
from datetime import datetime, timezone

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk.
from fivetran_connector_sdk import Connector

# For enabling logs in your connector code.
from fivetran_connector_sdk import Logging as log

# For supporting data operations like upsert(), update(), delete() and checkpoint().
from fivetran_connector_sdk import Operations as op

# Import the mock API module that simulates the three different source data shapes.
import mock_api

# Number of upsert operations to perform before emitting an intermediate checkpoint.
# Checkpointing at regular intervals prevents data loss if a sync is interrupted mid-table.
# Adjust this value based on the size of your dataset and the acceptable replay window.
CHECKPOINT_INTERVAL = 50


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector
    has all necessary configuration values before attempting to fetch data.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    # Validate required configuration parameters.
    # No configuration is required for this example connector.
    # When building your own connector, uncomment and modify the example below:
    #
    # required_configs = ["api_key", "base_url"]
    # for key in required_configs:
    #     if key not in configuration:
    #         raise ValueError(f"Missing required configuration value: {key}")
    pass


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
        # The nested 'address' object is expanded into 'address_city' and 'address_zip' columns
        # directly on the parent table, avoiding any joins at query time.
        {
            # Name of the table in the destination, required.
            "table": "users_flattened",
            # Primary key column(s) for the table.
            # We recommend defining a primary_key for each table. If not provided, Fivetran
            # computes _fivetran_id from all column values.
            "primary_key": ["id"],
            # Definition of columns and their types, optional.
            # For any columns not listed here, types will be inferred from the upserted data.
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "address_city": "STRING",
                "address_zip": "STRING",
            },
        },
        # Pattern 2a: Parent table.
        # Contains only the top-level user fields. The nested 'orders' list is moved to its
        # own child table to avoid row duplication and allow independent querying of orders.
        {
            "table": "users",
            "primary_key": ["user_id"],
            "columns": {
                "user_id": "STRING",
                "name": "STRING",
            },
        },
        # Pattern 2b: Child table.
        # Each order from the nested list becomes its own row. The 'user_id' column acts as
        # a foreign key linking each order back to its parent row in the 'users' table.
        {
            "table": "orders",
            "primary_key": ["order_id"],
            "columns": {
                "order_id": "STRING",
                "user_id": "STRING",
                "amount": "DOUBLE",
            },
        },
        # Pattern 3: JSON blob column.
        # The complex, variable-structure 'metadata' object is stored as a single JSON column.
        # This avoids schema changes when the nested structure evolves over time.
        {
            "table": "users_metadata",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "metadata": "JSON",
            },
        },
    ]


def sync_flattened(state: dict, upsert_count: int) -> int:
    """
    Pattern 1 — Flatten a single nested object into additional columns.
    Fetches users where each record contains a nested 'address' object and writes
    the address fields as individual columns on the destination table.

    Source shape:
      {"id": "1", "name": "Alice", "address": {"city": "New York", "zip": "10001"}}

    Destination table — users_flattened:
      id | name  | address_city | address_zip
      1  | Alice | New York     | 10001

    Args:
        state: the current state dictionary, used for mid-sync checkpointing.
        upsert_count: running total of upserts performed so far across all tables.
    Returns:
        Updated upsert_count after processing this table.
    """
    log.warning("Syncing Pattern 1: Flatten nested object into columns")

    # Fetch users with a single nested address object from the mock API.
    users = mock_api.get_users_with_address()

    for user in users:
        # Extract the nested address object, defaulting to an empty dict if absent.
        address = user.get("address", {})

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(
            table="users_flattened",
            data={
                "id": user["id"],
                "name": user["name"],
                # Flatten the nested address fields into top-level columns.
                "address_city": address.get("city"),
                "address_zip": address.get("zip"),
            },
        )
        upsert_count += 1

        # Checkpoint every CHECKPOINT_INTERVAL upserts so the sync can safely resume
        # from this point if it is interrupted before the table finishes.
        if upsert_count % CHECKPOINT_INTERVAL == 0:
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state)

    log.info(f"Upserted {len(users)} row(s) into 'users_flattened'")
    return upsert_count


def sync_parent_child(state: dict, upsert_count: int) -> int:
    """
    Pattern 2 — Break a nested list out into a parent table and a child table.
    Fetches users where each record contains a nested 'orders' list. The top-level
    user fields are written to the 'users' table; each order is written to the 'orders'
    table with 'user_id' as a foreign key back to the parent row.

    Source shape:
      {"user_id": "1", "name": "Alice", "orders": [{"order_id": "A1", "amount": 20}, ...]}

    Destination tables:
      users:  user_id | name
      orders: order_id | user_id | amount

    Args:
        state: the current state dictionary, used for mid-sync checkpointing.
        upsert_count: running total of upserts performed so far across all tables.
    Returns:
        Updated upsert_count after processing both tables.
    """
    log.warning("Syncing Pattern 2: Break nested list into parent/child tables")

    # Fetch users with a nested orders list from the mock API.
    users = mock_api.get_users_with_orders()
    order_count = 0

    for user in users:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(
            table="users",
            data={
                "user_id": user["user_id"],
                "name": user["name"],
            },
        )
        upsert_count += 1

        # Checkpoint every CHECKPOINT_INTERVAL upserts so the sync can safely resume
        # from this point if it is interrupted before the table finishes.
        if upsert_count % CHECKPOINT_INTERVAL == 0:
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state)

        for order in user.get("orders", []):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(
                table="orders",
                data={
                    "order_id": order["order_id"],
                    "user_id": user["user_id"],
                    "amount": order["amount"],
                },
            )
            upsert_count += 1
            order_count += 1

            # Checkpoint every CHECKPOINT_INTERVAL upserts so the sync can safely resume
            # from this point if it is interrupted before the table finishes.
            if upsert_count % CHECKPOINT_INTERVAL == 0:
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                op.checkpoint(state)

    log.info(f"Upserted {len(users)} row(s) into 'users' and {order_count} row(s) into 'orders'")
    return upsert_count


def sync_json_blob(state: dict, upsert_count: int) -> int:
    """
    Pattern 3 — Store a complex nested object as a single JSON blob column.
    Fetches users where each record contains a deeply nested 'metadata' object whose
    structure may vary between records. The entire object is passed as-is to the
    destination JSON column; the SDK handles serialization automatically.

    Source shape:
      {
        "id": "1", "name": "Alice",
        "metadata": {"preferences": {"theme": "dark", ...}, "history": [...]}
      }

    Destination table — users_metadata:
      id | name  | metadata
      1  | Alice | {"preferences": {...}, "history": [...]}

    Args:
        state: the current state dictionary, used for mid-sync checkpointing.
        upsert_count: running total of upserts performed so far across all tables.
    Returns:
        Updated upsert_count after processing this table.
    """
    log.warning("Syncing Pattern 3: Write complex nested object as a JSON blob")

    # Fetch users with a complex, variable-structure metadata object from the mock API.
    users = mock_api.get_users_with_metadata()

    for user in users:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(
            table="users_metadata",
            data={
                "id": user["id"],
                "name": user["name"],
                # Pass the metadata dict directly. The SDK serializes it into the JSON column;
                # no manual json.dumps() call is needed.
                "metadata": user["metadata"],
            },
        )
        upsert_count += 1

        # Checkpoint every CHECKPOINT_INTERVAL upserts so the sync can safely resume
        # from this point if it is interrupted before the table finishes.
        if upsert_count % CHECKPOINT_INTERVAL == 0:
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state)

    log.info(f"Upserted {len(users)} row(s) into 'users_metadata'")
    return upsert_count


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
    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    log.warning("Example: Common Patterns For Connectors - Data Handling Patterns")

    # Build the new state using the current UTC datetime as the sync timestamp.
    # This records when the sync ran so downstream consumers can track freshness.
    # For more information on state management, refer to:
    # https://fivetran.com/docs/connector-sdk/working-with-connector-sdk#workingwithstatejsonfile
    new_state = {"last_synced_at": datetime.now(timezone.utc).isoformat()}

    # Track the total number of upsert operations performed across all tables.
    # This counter is passed into each sync function and used to trigger intermediate
    # checkpoints at every CHECKPOINT_INTERVAL upserts.
    upsert_count = 0

    # Sync Pattern 1: flatten the nested address object into individual columns.
    upsert_count = sync_flattened(new_state, upsert_count)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(new_state)

    # Sync Pattern 2: split the nested orders list into parent (users) and child (orders) tables.
    upsert_count = sync_parent_child(new_state, upsert_count)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(new_state)

    # Sync Pattern 3: store the complex metadata object as a single JSON blob column.
    upsert_count = sync_json_blob(new_state, upsert_count)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(new_state)

    log.info(
        f"Sync complete. Total upserts: {upsert_count}. Last synced at: {new_state['last_synced_at']}"
    )


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
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
    # Adding this code to your `connector.py` allows  you to test your connector by running your file directly from
    # your IDE.
    connector.debug()

# ── Resulting tables ──────────────────────────────────────────────────────────
#
# Pattern 1 — users_flattened:
# ┌──────┬───────┬──────────────┬─────────────┐
# │  id  │ name  │ address_city │ address_zip │
# ├──────┼───────┼──────────────┼─────────────┤
# │  1   │ Alice │ New York     │ 10001       │
# └──────┴───────┴──────────────┴─────────────┘
#
# Pattern 2 — users:            Pattern 2 — orders:
# ┌─────────┬───────┐           ┌──────────┬─────────┬────────┐
# │ user_id │ name  │           │ order_id │ user_id │ amount │
# ├─────────┼───────┤           ├──────────┼─────────┼────────┤
# │    1    │ Alice │           │    A1    │    1    │  20.0  │
# └─────────┴───────┘           │    B2    │    1    │  35.0  │
#                               └──────────┴─────────┴────────┘
#
# Pattern 3 — users_metadata:
# ┌──────┬───────┬─────────────────────────────────────────┐
# │  id  │ name  │                metadata                 │
# ├──────┼───────┼─────────────────────────────────────────┤
# │  1   │ Alice │ {"preferences": {...}, "history": [...]} │
# └──────┴───────┴─────────────────────────────────────────┘
