"""This connector demonstrates LIMIT/OFFSET pagination for syncing data from a database source.
It queries a local SQLite database using LIMIT <N> OFFSET <k>, advancing the offset after each page.
The database is created and seeded automatically on the first run.
THIS EXAMPLE IS TO HELP YOU UNDERSTAND CONCEPTS USING DUMMY DATA.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

# Import sqlite3 for local database access (part of Python standard library, no installation required).
import sqlite3

# Import os to check whether the seed database file already exists.
import os

# Import required classes from fivetran_connector_sdk.
# For supporting Connector operations like update() and schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__ROWS_PER_PAGE = 25
__DB_FILE = "users.db"
__STATE_KEY_OFFSET = "offset"
__DEFAULT_OFFSET = 0


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    This example requires no configuration, so no validation is performed.
    When building your own connector, add validation for required keys here.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    pass


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
    log.warning("Example: Common Patterns For Connectors - Pagination - Database LIMIT/OFFSET")

    validate_configuration(configuration)

    # Seed the local SQLite database with sample data on the first run.
    # In a real connector, you would connect to an external database instead.
    _seed_database_if_needed(__DB_FILE)

    # Retrieve the current offset from state. On the first sync, start from the beginning.
    offset = int(state.get(__STATE_KEY_OFFSET, __DEFAULT_OFFSET))

    sync_items(__DB_FILE, offset, state)


def sync_items(db_file, offset, state):
    """
    The sync_items function handles the retrieval and processing of paginated database rows using LIMIT/OFFSET.
    It performs the following tasks:
        1. Queries the database for a page of rows starting at the current offset.
        2. Processes the returned rows using upsert operations to send to Fivetran.
        3. Advances the offset by the page size after each page.
        4. Saves the offset in state after each page so the sync can resume if interrupted.
        5. Continues until no more rows are returned.

    LIMIT/OFFSET pagination adds LIMIT <N> OFFSET <k> to every query. It is simple to implement
    but has two important limitations:
        1. Row-shift: if rows are inserted or deleted while paging, later offsets can shift,
           causing gaps or duplicate rows. Mitigate this with a deterministic ORDER BY.
        2. Performance: the database must scan and skip OFFSET rows for every page, so queries
           become slower as the offset grows. For large or frequently updated tables, prefer
           keyset pagination (see the database_keyset example).

    Query pattern:
        SELECT id, name, email, updated_at FROM users
        ORDER BY updated_at, id
        LIMIT rows_per_page OFFSET offset;
    Args:
        db_file: Path to the SQLite database file.
        offset: The row offset to start the current page from.
        state: A dictionary representing the current state of the sync.
    """
    conn = sqlite3.connect(db_file)
    # row_factory enables column name access on rows (e.g. row["id"]) and allows dict(row) conversion.
    conn.row_factory = sqlite3.Row

    try:
        cursor = conn.cursor()

        while True:
            # Fetch a page of rows. A deterministic ORDER BY is required so that OFFSET refers to a
            # consistent position across requests.
            cursor.execute(
                """
                SELECT id, name, email, updated_at
                FROM users
                ORDER BY updated_at, id
                LIMIT ? OFFSET ?
                """,
                (__ROWS_PER_PAGE, offset),
            )
            rows = cursor.fetchall()

            if not rows:
                break  # No more rows — pagination complete.

            log.info(
                f"processing page at offset {offset}. First row id: {rows[0]['id']}, Total rows: {len(rows)}"
            )

            for row in rows:
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted.
                op.upsert(table="user", data=dict(row))

            # Advance offset by the actual number of rows returned on this page.
            # Using len(rows) rather than __ROWS_PER_PAGE ensures the offset is accurate
            # when the last page contains fewer rows than the page size.
            offset += len(rows)
            state[__STATE_KEY_OFFSET] = offset

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
    try:
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
        # Timestamps are 3 minutes apart starting from 2024-01-01, giving a spread across ~10 hours.
        rows = [
            (
                i,
                f"User {i}",
                f"user{i}@example.com",
                f"2024-01-01T{(i * 3) // 60:02d}:{(i * 3) % 60:02d}:00+00:00",
            )
            for i in range(1, 201)
        ]
        cursor.executemany("INSERT INTO users VALUES (?, ?, ?, ?)", rows)

        conn.commit()
    finally:
        conn.close()

    log.info("Database seeded successfully with 200 rows.")


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module. This is Python's standard entry method allowing your script to
# be run directly from the command line or IDE 'run' button. This is useful for debugging while you write your code.
# Note this method is not called by Fivetran when executing your connector in production. Please test using the
# Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # This example does not require a configuration.json file.
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from
    # your IDE.
    connector.debug()

# Resulting table:
# ┌─────┬────────┬───────────────────────┬──────────────────────────────┐
# │ id  │  name  │         email         │          updated_at          │
# │ int │ string │        string         │      timestamp with UTC      │
# ├─────┼────────┼───────────────────────┼──────────────────────────────┤
# │  1  │ User 1 │ user1@example.com     │ 2024-01-01T00:03:00+00:00    │
# │  2  │ User 2 │ user2@example.com     │ 2024-01-01T00:06:00+00:00    │
# ├─────┴────────┴───────────────────────┴──────────────────────────────┤
# │  2 rows                                                   4 columns │
# └────────────────────────────────────────────────────────────────────┘
