# This example demonstrates how to mimic Fivetran's History Mode using the Connector SDK
# with a Microsoft SQL Server source.
#
# WHAT IS HISTORY MODE?
# Fivetran's History Mode is a sync mode that preserves all historical versions of a record
# in the destination. Instead of overwriting a row when the source record changes, History Mode
# inserts a new row for each observed state, keeping a full audit trail of every change.
#
# WHY THE CONNECTOR SDK DOES NOT NATIVELY SUPPORT HISTORY MODE
# The Connector SDK does not support History Mode. However, you can mimic its behavior by
# including a timestamp column as part of a composite primary key.
#
# HOW THE COMPOSITE PRIMARY KEY APPROACH WORKS
# This connector reads the incremental_column from configuration (e.g. _LastUpdatedInstant).
# For every table that contains that column, the connector:
#   1. Adds the column to the composite primary key alongside the table's natural PKs.
#   2. Uses it as an incremental cursor — only rows changed since the last sync are fetched.
# Because the incremental column is part of the PK, a row whose timestamp changed produces
# a NEW row in the destination rather than overwriting the existing one — History Mode behavior.
#
# ROLE OF THE INCREMENTAL COLUMN WHEN createdAt IS ABSENT
# For sources that do not track record creation time, the incremental column at first observation
# equals the first-seen time — functionally equivalent to createdAt. So it alone is sufficient
# for both the composite primary key and the incremental cursor.
#
# KEY LIMITATION
# Only the state of a record at the time of each sync is captured. If a record is updated
# multiple times between two syncs, only the most recent timestamp value will appear in the
# destination. Intermediate states are lost. For details, see:
# https://fivetran.com/docs/core-concepts/sync-modes/history-mode#changestodatabetweensyncs
#
# See the Technical Reference documentation
# (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# For JSON configuration parsing
import json

# Microsoft SQL Server database driver
import pytds

# For timestamp handling
from datetime import datetime

# For handling DECIMAL values returned by pytds
from decimal import Decimal

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Number of rows fetched from the database per round-trip
__BATCH_SIZE = 1000

# Number of records processed before saving a checkpoint
__CHECKPOINT_INTERVAL = 5000

# Maximum number of tables to discover in this example
__MAX_TABLES = 10

# Sentinel cursor for the very first sync — SQL Server DATETIME does not support timezone
# offsets, so use a plain datetime string
__INITIAL_CURSOR = "1970-01-01 00:00:00"


def _validate_configuration(configuration: dict) -> None:
    required = [
        "mssql_server",
        "mssql_database",
        "mssql_user",
        "mssql_password",
        "mssql_port",
        "incremental_column",
    ]
    for key in required:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def _connect(configuration: dict):
    """Open and return a pytds connection to SQL Server."""
    try:
        connection = pytds.connect(
            server=configuration["mssql_server"],
            database=configuration["mssql_database"],
            user=configuration["mssql_user"],
            password=configuration["mssql_password"],
            port=int(configuration["mssql_port"]),
            validate_host=False,
        )
        log.info(
            f"Connected to {configuration['mssql_database']} on {configuration['mssql_server']}"
        )
        return connection
    except Exception as e:
        log.severe(f"Failed to connect to SQL Server: {e}")
        raise


def _get_tables(connection, schema_name: str) -> list[str]:
    """Return up to __MAX_TABLES user table names from the given schema."""
    query = f"""
        SELECT TOP {__MAX_TABLES} TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND TABLE_SCHEMA = %s
          AND TABLE_NAME NOT LIKE 'sys%%'
        ORDER BY TABLE_NAME
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (schema_name,))
        tables = [row[0] for row in cursor.fetchall()]
    log.info(f"Discovered {len(tables)} tables in schema '{schema_name}': {tables}")
    return tables


def _get_natural_primary_keys(connection, schema_name: str, table_name: str) -> list[str]:
    """Return the natural primary key columns for a table."""
    query = """
        SELECT kcu.COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
          ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
          AND kcu.TABLE_SCHEMA = %s
          AND kcu.TABLE_NAME = %s
        ORDER BY kcu.ORDINAL_POSITION
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (schema_name, table_name))
        return [row[0] for row in cursor.fetchall()]


def _table_has_column(connection, schema_name: str, table_name: str, column_name: str) -> bool:
    """Return True if the table contains the given column."""
    query = """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (schema_name, table_name, column_name))
        return cursor.fetchone()[0] > 0


def _map_sql_type(sql_type: str) -> str:
    """Map a SQL Server data type string to a Fivetran SDK column type."""
    t = sql_type.lower()
    if t == "bit":
        return "BOOLEAN"
    if t in ("tinyint", "smallint"):
        return "SHORT"
    if t in ("int", "integer"):
        return "INT"
    if t == "bigint":
        return "LONG"
    if t in ("float", "real", "decimal", "numeric", "money", "smallmoney"):
        return "FLOAT"
    if t == "date":
        return "NAIVE_DATE"
    if t in ("datetime", "datetime2", "smalldatetime"):
        return "NAIVE_DATETIME"
    if t == "datetimeoffset":
        return "UTC_DATETIME"
    if t in ("binary", "varbinary", "image"):
        return "BINARY"
    return "STRING"


def _get_columns(connection, schema_name: str, table_name: str) -> dict[str, str]:
    """Return {column_name: fivetran_type} for all columns in the table."""
    query = """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (schema_name, table_name))
        return {row[0]: _map_sql_type(row[1]) for row in cursor.fetchall()}


def _serialize(value):
    """Convert Decimal to float for SDK compatibility."""
    if isinstance(value, Decimal):
        return float(value)
    return value


def _fetch_rows(
    connection,
    schema_name: str,
    table_name: str,
    incremental_col: str,
    cursor_value: str,
    natural_pks: list[str],
    last_pk_value=None,
):
    """
    Yield rows as dicts. When incremental_col is provided, fetches only rows updated after
    cursor_value using a keyset cursor to handle timestamp ties correctly.

    Keyset logic (single natural PK):
        WHERE (incremental_col > cursor)
           OR (incremental_col = cursor AND pk_col > last_pk_value)
        ORDER BY incremental_col, pk_col

    This ensures that if multiple rows share the same timestamp, none are skipped.
    For tables with no natural PK or a composite PK, falls back to a simple > filter.
    """
    qualified = f"[{schema_name}].[{table_name}]"
    with connection.cursor() as db_cursor:
        if incremental_col:
            pk_col = natural_pks[0] if len(natural_pks) == 1 else None

            if pk_col and last_pk_value is not None:
                # Keyset cursor: handles ties on incremental_col correctly.
                db_cursor.execute(
                    f"""SELECT * FROM {qualified}
                        WHERE ([{incremental_col}] > %s)
                           OR ([{incremental_col}] = %s AND [{pk_col}] > %s)
                        ORDER BY [{incremental_col}], [{pk_col}]""",
                    (cursor_value, cursor_value, last_pk_value),
                )
                log.info(
                    f"Incremental fetch {schema_name}.{table_name}: {incremental_col} > '{cursor_value}' OR (= '{cursor_value}' AND {pk_col} > {last_pk_value})"
                )
            else:
                db_cursor.execute(
                    f"SELECT * FROM {qualified} WHERE [{incremental_col}] > %s ORDER BY [{incremental_col}]",
                    (cursor_value,),
                )
                log.info(
                    f"Incremental fetch {schema_name}.{table_name}: {incremental_col} > '{cursor_value}'"
                )
        else:
            db_cursor.execute(f"SELECT * FROM {qualified}")
            log.info(f"Full scan of {schema_name}.{table_name}")

        col_names = [desc[0] for desc in db_cursor.description]
        while True:
            rows = db_cursor.fetchmany(__BATCH_SIZE)
            if not rows:
                break
            for row in rows:
                yield {col: _serialize(val) for col, val in zip(col_names, row)}


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema

    For every table that contains the incremental_column, the column is appended to the
    composite primary key. This is the change that produces History Mode-like behavior:
    each (natural_pk, incremental_column) pair is treated as a distinct row.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    _validate_configuration(configuration)
    incremental_col = configuration["incremental_column"]
    schema_name = configuration.get("mssql_schema", "dbo")

    connection = _connect(configuration)
    try:
        tables = _get_tables(connection, schema_name)
        schema_list = []

        for table_name in tables:
            natural_pks = _get_natural_primary_keys(connection, schema_name, table_name)
            log.fine("Natural Primary Keys: {}".format(natural_pks))
            columns = _get_columns(connection, schema_name, table_name)
            has_incremental = _table_has_column(
                connection, schema_name, table_name, incremental_col
            )

            if has_incremental and incremental_col not in natural_pks:
                # Append the incremental column to the composite primary key.
                # This is the key change that mimics History Mode: a new value of
                # incremental_col means a new composite PK, so a new row is inserted
                # rather than the existing row being overwritten.
                composite_pk = natural_pks + [incremental_col]
            else:
                composite_pk = natural_pks

            schema_list.append(
                {
                    "table": f"{schema_name}_{table_name}",
                    "primary_key": composite_pk if composite_pk else None,
                    "columns": columns if columns else None,
                }
            )

        return schema_list
    finally:
        connection.close()


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning(
        "Example: Common Patterns For Connectors - History Mode Mimicry Using Composite Primary Key"
    )

    _validate_configuration(configuration)
    incremental_col = configuration["incremental_column"]
    schema_name = configuration.get("mssql_schema", "dbo")

    connection = _connect(configuration)
    try:
        tables = _get_tables(connection, schema_name)

        for table_name in tables:
            destination_table = f"{schema_name}_{table_name}"
            natural_pks = _get_natural_primary_keys(connection, schema_name, table_name)
            has_incremental = _table_has_column(
                connection, schema_name, table_name, incremental_col
            )
            pk_col = natural_pks[0] if len(natural_pks) == 1 else None

            # State per table is a dict: {"cursor": <timestamp>, "last_pk": <pk_value>}
            # This supports keyset pagination to handle timestamp ties correctly.
            table_state = state.get(destination_table, {})
            cursor_value = table_state.get("cursor", __INITIAL_CURSOR) if has_incremental else None
            last_pk_value = table_state.get("last_pk") if has_incremental and pk_col else None

            log.info(
                f"Starting {destination_table}: cursor='{cursor_value}', last_pk={last_pk_value}"
            )

            records_processed = 0
            latest_cursor = cursor_value
            latest_pk = last_pk_value

            for record in _fetch_rows(
                connection,
                schema_name,
                table_name,
                incremental_col if has_incremental else None,
                cursor_value,
                natural_pks,
                last_pk_value,
            ):
                op.upsert(table=destination_table, data=record)
                records_processed += 1

                if has_incremental:
                    raw = record.get(incremental_col)
                    if raw is not None:
                        # Use str() not isoformat() — pytds datetime objects stringify with a space
                        # separator ("2026-04-24 04:57:58.588") which SQL Server accepts directly.
                        # isoformat() produces a T separator which causes SQL Server conversion errors.
                        latest_cursor = str(raw)
                    if pk_col:
                        latest_pk = record.get(pk_col)

                if records_processed % __CHECKPOINT_INTERVAL == 0:
                    if has_incremental:
                        state[destination_table] = {"cursor": latest_cursor, "last_pk": latest_pk}
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(state)

            # Advance the cursor for this table and checkpoint after all rows are processed.
            if has_incremental and latest_cursor:
                state[destination_table] = {"cursor": latest_cursor, "last_pk": latest_pk}

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)
            log.info(f"Finished {destination_table}: {records_processed} rows processed")

    except Exception as e:
        log.severe(f"Sync failed: {e}")
        raise
    finally:
        connection.close()


# This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
