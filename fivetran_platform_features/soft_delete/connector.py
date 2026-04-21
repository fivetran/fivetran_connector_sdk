# This example demonstrates Fivetran's soft delete feature using the truncate-and-reload pattern.
# On each sync, op.truncate() marks all existing warehouse rows as soft-deleted
# (_fivetran_deleted = True), then the full current snapshot is re-upserted.
# Any row that reappears in the upsert has its _fivetran_deleted flag cleared automatically.
# Rows absent from the new snapshot stay marked deleted — this is the soft-delete behaviour.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# --------------------------------------------------------------------------- #
# Dummy data representing two snapshots of source data.
#
# Snapshot 1 (initial full load): 5 employees.
# Snapshot 2 (second sync): employee IDs 4 and 5 have been removed at the source.
# --------------------------------------------------------------------------- #

SNAPSHOT_1 = [
    {"id": 1, "name": "Alice",   "department": "Engineering"},
    {"id": 2, "name": "Bob",     "department": "Marketing"},
    {"id": 3, "name": "Charlie", "department": "Engineering"},
    {"id": 4, "name": "Diana",   "department": "HR"},
    {"id": 5, "name": "Edward",  "department": "Finance"},
]

SNAPSHOT_2 = [
    {"id": 1, "name": "Alice",   "department": "Engineering"},
    {"id": 2, "name": "Bob",     "department": "Marketing"},
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
                "id":         "INT",
                "name":       "STRING",
                "department": "STRING",
            },
        }
    ]


def fetch_snapshot(sync_count: int) -> list:
    """Return dummy source data for the given sync number (1-indexed)."""
    return SNAPSHOT_1 if sync_count == 1 else SNAPSHOT_2


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Soft-delete flow (truncate-and-reload pattern)
    -----------------------------------------------
    Step 1 — op.truncate(): marks every existing row in the warehouse table as
             _fivetran_deleted = True.  No rows are physically removed.
    Step 2 — op.upsert() for each record in the current snapshot: rows that still
             exist at the source are re-inserted / updated, and their
             _fivetran_deleted flag is cleared back to False automatically.
             Rows absent from the snapshot remain soft-deleted.
    Step 3 — op.checkpoint(): persists state so the next sync knows its sequence number.

    Args:
        configuration: dictionary containing any secrets or parameters.
        state: a dictionary containing the state from the previous sync.
               Empty on the first sync or after a full re-sync.
    """
    sync_count = state.get("sync_count", 0) + 1
    log.warning(f"Example: Fivetran Platform Features - Soft Delete (sync #{sync_count})")

    current_snapshot = fetch_snapshot(sync_count)

    # ------------------------------------------------------------------ #
    # Step 1 — truncate: mark all existing warehouse rows as soft-deleted.
    #
    # Skipped on the first sync because the table is empty — nothing to mark.
    # From sync 2 onwards, op.truncate() sets _fivetran_deleted = True for
    # every row in the destination table.  The subsequent upserts clear that
    # flag for any row still present at the source.
    # ------------------------------------------------------------------ #
    if state:
        log.info("Truncating 'employees' table — all existing rows marked as soft-deleted.")
        op.truncate(table="employees")

    # ------------------------------------------------------------------ #
    # Step 2 — upsert the full current snapshot.
    #          Re-inserts surviving rows (clearing _fivetran_deleted) and
    #          applies any field-level updates.  Rows not present here stay
    #          soft-deleted from the truncate above.
    # ------------------------------------------------------------------ #
    log.info(f"Upserting {len(current_snapshot)} record(s) from current snapshot.")
    for record in current_snapshot:
        op.upsert(table="employees", data=record)

    # ------------------------------------------------------------------ #
    # Step 3 — checkpoint: persist state for the next sync.
    # ------------------------------------------------------------------ #
    new_state = {"sync_count": sync_count}
    op.checkpoint(new_state)
    log.info(f"Checkpoint saved: {new_state}")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Sync 1: full load — all 5 employees upserted, state saved.
    print("\n========== SYNC 1: Full initial load ==========")
    connector.debug(state={})

    # Sync 2: IDs 4 & 5 are gone from the source.
    print("\n========== SYNC 2: IDs 4 & 5 removed at source ==========")
    connector.debug(state={"sync_count": 1})

    # After Sync 1 — employees table:
    # ┌────┬─────────┬─────────────┬──────────────────┐
    # │ id │  name   │ department  │ _fivetran_deleted │
    # ├────┼─────────┼─────────────┼──────────────────┤
    # │  1 │ Alice   │ Engineering │      false        │
    # │  2 │ Bob     │ Marketing   │      false        │
    # │  3 │ Charlie │ Engineering │      false        │
    # │  4 │ Diana   │ HR          │      false        │
    # │  5 │ Edward  │ Finance     │      false        │
    # └────┴─────────┴─────────────┴──────────────────┘
    #
    # After Sync 2 — IDs 4 & 5 soft-deleted
    # ┌────┬─────────┬─────────────┬──────────────────┐
    # │ id │  name   │ department  │ _fivetran_deleted │
    # ├────┼─────────┼─────────────┼──────────────────┤
    # │  1 │ Alice   │ Engineering │      false        │
    # │  2 │ Bob     │ Marketing   │      false        │
    # │  3 │ Charlie │ Engineering │      false        │
    # │  4 │ Diana   │ HR          │      true         │  ← soft-deleted
    # │  5 │ Edward  │ Finance     │      true         │  ← soft-deleted
    # └────┴─────────┴─────────────┴──────────────────┘
