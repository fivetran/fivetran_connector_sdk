# This example demonstrates Fivetran's soft delete feature using the truncate-and-reload pattern.
# On the first sync, a full set of records is upserted.
# On the second sync, we simulate a source where some records have been removed:
#   1. Records missing from the new snapshot are explicitly deleted via op.delete(),
#      which marks them with _fivetran_deleted = True in the warehouse.
#   2. The surviving records are re-upserted.
# This mirrors the "truncate + re-import" pattern described in Fivetran's soft-delete documentation.
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
    {"id": 1, "name": "Alice",   "department": "Engineering", "active": True},
    {"id": 2, "name": "Bob",     "department": "Marketing",   "active": True},
    {"id": 3, "name": "Charlie", "department": "Engineering", "active": True},
    {"id": 4, "name": "Diana",   "department": "HR",          "active": True},
    {"id": 5, "name": "Edward",  "department": "Finance",     "active": True},
]

SNAPSHOT_2 = [
    {"id": 1, "name": "Alice",   "department": "Engineering", "active": True},
    {"id": 2, "name": "Bob",     "department": "Data",        "active": True},  # department updated
    {"id": 3, "name": "Charlie", "department": "Engineering", "active": True},
    # IDs 4 and 5 are gone — they will be soft-deleted in the warehouse
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
                "active":     "BOOLEAN",
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
    First sync  : upsert all 5 records → checkpoint.
    Second sync : compare new snapshot to previous snapshot.
                  Call op.delete() for every ID present before but absent now —
                  Fivetran marks those rows _fivetran_deleted = True in the warehouse.
                  Upsert surviving / updated records → checkpoint.

    Args:
        configuration: dictionary containing any secrets or parameters.
        state: a dictionary containing the state from the previous sync.
               Empty on the first sync or after a full re-sync.
    """
    sync_count = state.get("sync_count", 0) + 1
    log.warning(f"Example: Fivetran Platform Features - Soft Delete (sync #{sync_count})")

    # IDs seen in the previous sync are carried in state so we can detect removals.
    previous_ids = set(state.get("seen_ids", []))

    current_snapshot = fetch_snapshot(sync_count)
    current_ids = {row["id"] for row in current_snapshot}

    # ------------------------------------------------------------------ #
    # Step 1 — soft-delete rows that disappeared from the source.
    #
    # op.delete() tells Fivetran to set _fivetran_deleted = True for the
    # given primary key.  This is the soft-delete equivalent of a
    # "truncate then re-upsert" pattern: rows are never physically removed;
    # instead the ones no longer present at the source are flagged.
    # ------------------------------------------------------------------ #
    removed_ids = previous_ids - current_ids
    if removed_ids:
        log.info(f"Soft-deleting {len(removed_ids)} record(s) no longer in source: {sorted(removed_ids)}")
        for missing_id in sorted(removed_ids):
            op.delete(table="employees", keys={"id": missing_id})
    else:
        log.info("No records removed since last sync.")

    # ------------------------------------------------------------------ #
    # Step 2 — upsert the full current snapshot.
    #          Inserts new rows and applies any field-level updates.
    # ------------------------------------------------------------------ #
    log.info(f"Upserting {len(current_snapshot)} record(s) from current snapshot.")
    for record in current_snapshot:
        op.upsert(table="employees", data=record)

    # ------------------------------------------------------------------ #
    # Step 3 — checkpoint: persist state so the next sync knows which IDs
    #          were present and how many syncs have completed.
    # ------------------------------------------------------------------ #
    new_state = {
        "sync_count": sync_count,
        "seen_ids": sorted(current_ids),
    }
    op.checkpoint(new_state)
    log.info(f"Checkpoint saved: {new_state}")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Sync 1: full load — all 5 employees upserted, state saved.
    print("\n========== SYNC 1: Full initial load ==========")
    connector.debug(state={})

    # Sync 2: incremental — IDs 4 & 5 are gone from the source.
    # Pass the state produced by sync 1 so the connector detects the removals
    # and soft-deletes them via op.delete().
    print("\n========== SYNC 2: IDs 4 & 5 removed at source ==========")
    connector.debug(state={"sync_count": 1, "seen_ids": [1, 2, 3, 4, 5]})

    # After Sync 1 — employees table:
    # ┌────┬─────────┬─────────────┬────────┬──────────────────┐
    # │ id │  name   │ department  │ active │ _fivetran_deleted │
    # ├────┼─────────┼─────────────┼────────┼──────────────────┤
    # │  1 │ Alice   │ Engineering │  true  │      false        │
    # │  2 │ Bob     │ Marketing   │  true  │      false        │
    # │  3 │ Charlie │ Engineering │  true  │      false        │
    # │  4 │ Diana   │ HR          │  true  │      false        │
    # │  5 │ Edward  │ Finance     │  true  │      false        │
    # └────┴─────────┴─────────────┴────────┴──────────────────┘
    #
    # After Sync 2 — IDs 4 & 5 soft-deleted, Bob's department updated:
    # ┌────┬─────────┬─────────────┬────────┬──────────────────┐
    # │ id │  name   │ department  │ active │ _fivetran_deleted │
    # ├────┼─────────┼─────────────┼────────┼──────────────────┤
    # │  1 │ Alice   │ Engineering │  true  │      false        │
    # │  2 │ Bob     │ Data        │  true  │      false        │  ← department changed
    # │  3 │ Charlie │ Engineering │  true  │      false        │
    # │  4 │ Diana   │ HR          │  true  │      true         │  ← soft-deleted
    # │  5 │ Edward  │ Finance     │  true  │      true         │  ← soft-deleted
    # └────┴─────────┴─────────────┴────────┴──────────────────┘
