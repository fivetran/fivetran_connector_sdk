"""This connector demonstrates keyset (seek method) pagination for syncing data from a database source.
It queries a local SQLite database, paging through rows using a WHERE (updated_at, id) > (?, ?) boundary
that advances after each page. The database is created and seeded automatically on the first run.
THIS EXAMPLE IS TO HELP YOU UNDERSTAND CONCEPTS USING DUMMY DATA.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

# Import sqlite3 for local database access (part of Python standard library, no installation required).
import sqlite3

# Import os to check whether the seed database file already exists.
import os

# Import required classes from fivetran_connector_sdk.
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__PAGE_SIZE = 25
__DB_FILE = "users.db"


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "user",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "name": "STRING",
                "email": "STRING",
                "updated_at": "UTC_DATETIME",
            },
        }
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: Common Patterns For Connectors - Pagination - Database Keyset")

    # Seed the local SQLite database with sample data on the first run.
    # In a real connector, you would connect to an external database instead.
    _seed_database_if_needed(__DB_FILE)

    # Retrieve the keyset boundary from state.
    # On the first sync, start before all records by using the earliest possible values.
    last_updated_at = state.get("updated_after", "0001-01-01T00:00:00+00:00")
    last_id = int(state.get("last_id", 0))

    try:
        sync_items(__DB_FILE, last_updated_at, last_id, state)
    except Exception as e:
        raise RuntimeError(f"Failed to sync data: {str(e)}") from e


def sync_items(db_file, last_updated_at, last_id, state):
    """
    The sync_items function handles the retrieval and processing of paginated database rows using keyset pagination.
    It performs the following tasks:
        1. Queries the database for rows whose (updated_at, id) key is greater than the last seen boundary.
        2. Processes the returned rows using upsert operations to send to Fivetran.
        3. Advances the keyset boundary to the last row of each page.
        4. Saves the boundary in state after each page so the sync can resume if interrupted.
        5. Continues until no more rows are returned.

    Keyset pagination uses a WHERE clause on a monotonic column (updated_at) and a tie-breaker (id)
    to skip already-seen rows without relying on OFFSET. This avoids the performance penalty and
    row-shift issues of LIMIT/OFFSET and is recommended for large or frequently updated tables.

    Query pattern:
        SELECT id, name, email, updated_at FROM users
        WHERE (updated_at, id) > (last_updated_at, last_id)
        ORDER BY updated_at, id
        LIMIT page_size;
    Args:
        db_file: Path to the SQLite database file.
        last_updated_at: The updated_at timestamp of the last processed row (keyset boundary).
        last_id: The id of the last processed row (tie-breaker for rows with identical updated_at).
        state: A dictionary representing the current state of the sync.
    """
    conn = sqlite3.connect(db_file)
    # row_factory enables column name access on rows (e.g. row["id"]) and allows dict(row) conversion.
    conn.row_factory = sqlite3.Row

    try:
        cursor = conn.cursor()

        while True:
            # Fetch the next page of rows beyond the current keyset boundary.
            # The (updated_at, id) > (?, ?) row-value comparison is supported in SQLite 3.15+,
            # which is bundled with Python 3.8 and later.
            cursor.execute(
                """
                SELECT id, name, email, updated_at
                FROM users
                WHERE (updated_at, id) > (?, ?)
                ORDER BY updated_at, id
                LIMIT ?
                """,
                (last_updated_at, last_id, __PAGE_SIZE),
            )
            rows = cursor.fetchall()

            if not rows:
                break  # No more rows — pagination complete.

            log.info(
                f"processing page of rows. First row id: {rows[0]['id']}, Total rows: {len(rows)}"
            )

            for row in rows:
                op.upsert(table="user", data=dict(row))

            # Advance the keyset boundary to the last row of this page.
            # Both updated_at and id are stored in state to handle rows with identical timestamps.
            last_updated_at = rows[-1]["updated_at"]
            last_id = rows[-1]["id"]
            state["updated_after"] = last_updated_at
            state["last_id"] = str(last_id)

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can
            # resume from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state)
    finally:
        conn.close()


def _seed_database_if_needed(db_file):
    """
    Creates and populates the SQLite database with sample data if it does not already exist.
    This function is called once on the first run and skipped on subsequent runs.
    In a real connector, you would connect to an external source database instead of seeding locally.
    Args:
        db_file: Path to the SQLite database file to create.
    """
    if os.path.exists(db_file):
        return

    log.info(f"Seeding local database '{db_file}' with sample data for the first time.")

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE users (
            id         INTEGER PRIMARY KEY,
            name       TEXT NOT NULL,
            email      TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    # Insert 200 rows with staggered timestamps so multiple pages are visible during fivetran debug.
    # Timestamps are 3 minutes apart starting from 2026-01-01, giving a spread across ~10 hours.
    rows = [
        (
            i,
            f"User {i}",
            f"user{i}@example.com",
            f"2026-01-01T{(i * 3) // 60:02d}:{(i * 3) % 60:02d}:00+00:00",
        )
        for i in range(1, 201)
    ]
    cursor.executemany("INSERT INTO users VALUES (?, ?, ?, ?)", rows)

    conn.commit()
    conn.close()

    log.info("Database seeded successfully with 200 rows.")


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
    connector.debug()

# Resulting table:
# ┌─────┬────────┬───────────────────────┬─────────────────────┐
# │ id  │  name  │         email         │      updated_at     │
# │ int │ string │        string         │  timestamp with UTC │
# ├─────┼────────┼───────────────────────┼─────────────────────┤
# │  1  │ User 1 │ user1@example.com     │ 2026-01-01T00:03:00+00:00 │
# │  2  │ User 2 │ user2@example.com     │ 2026-01-01T00:06:00+00:00 │
# ├─────┴────────┴───────────────────────┴─────────────────────┤
# │  2 rows                                          4 columns │
# └────────────────────────────────────────────────────────────┘
