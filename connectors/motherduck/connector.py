"""
This connector demonstrates how to fetch data from a MotherDuck (DuckDB Cloud)
database and upsert it into a Fivetran destination using the Connector SDK.

See Technical Reference:
https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

See Best Practices:
https://fivetran.com/docs/connectors/connector-sdk/best-practices
"""

import hashlib  # For generating row-level checksums
import json  # For reading configuration values and serializing data
import time  # For retry/wait logic
from datetime import datetime, timezone  # For timestamp handling
from typing import Any, Dict, List  # Type hints

import duckdb  # DuckDB client used for MotherDuck connections

# Fivetran SDK imports
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Constants
__BATCH_SIZE = 10000
__MAX_RETRIES = 3


def build_where(**kwargs) -> tuple:
    """
    Build a WHERE clause from key/value filters.
    Args:
        kwargs: Field/value filters for a SQL WHERE clause.
    Returns:
        Tuple of (where_string, param_list).
    """
    conditions = []
    params = []
    for field, value in kwargs.items():
        if value:
            conditions.append(f"{field} = ?")
            params.append(value)
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    return where_clause, params


def build_table_schema(conn, database: str, schema_name: str, table_name: str):
    """
    Build a Fivetran table schema dictionary from DuckDB metadata.

    Args:
        conn: DuckDB connection
        database: Database name
        schema_name: Schema name
        table_name: Table name

    Returns:
        Table schema dict or None if table has no columns.
    """
    columns = get_columns(conn, database, schema_name, table_name)
    if not columns:
        return None

    column_types: Dict[str, str] = {}
    primary_keys: List[str] = []

    for col in columns:
        col_name = col["name"]
        duck_type = col["type"].upper()
        fivetran_type = map_type(duck_type)
        column_types[col_name] = fivetran_type

        col_lower = col_name.lower()
        if col_lower in ("id", "pk") or col_lower.endswith("_id"):
            primary_keys.append(col_name)

    return {
        "table": get_fivetran_table_name(database, schema_name, table_name),
        "primary_key": primary_keys,
        "columns": column_types,
    }


def build_all_table_schemas(
    conn, database: str, schemas: List[str]
) -> List[Dict[str, Any]]:
    """
    Build table schema definitions for all tables across schemas.

    Args:
        conn: DuckDB connection
        database: Database name
        schemas: List of schema names

    Returns:
        List of Fivetran table schema dictionaries.
    """
    tables = []
    for schema_name in schemas:
        table_names = get_tables(conn, database, schema_name)
        for table_name in table_names:
            definition = build_table_schema(conn, database, schema_name, table_name)
            if definition:
                tables.append(definition)
    return tables


def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Define the connector schema based on MotherDuck metadata.

    Args:
        configuration: Connector configuration dict.

    Returns:
        List of Fivetran schema definitions.
    """
    token = configuration["motherduck_token"]
    database = configuration.get("database_name")
    schema_filter = configuration.get("schema_name")

    conn = connect(token, database)
    schemas = get_schemas(conn, database, schema_filter)
    tables = build_all_table_schemas(conn, database, schemas)

    conn.close()
    log.info(f"Discovered {len(tables)} tables")
    return tables


def map_type(duckdb_type: str) -> str:
    """
    Map DuckDB type into Fivetran SDK type.

    Args:
        duckdb_type: Type string from DuckDB

    Returns:
        Fivetran SDK type string.
    """
    duckdb_type = duckdb_type.upper()

    if "TINYINT" in duckdb_type or "SMALLINT" in duckdb_type:
        return "SHORT"
    if "INT" in duckdb_type and "BIG" not in duckdb_type:
        return "INT"
    if "BIGINT" in duckdb_type:
        return "LONG"
    if "FLOAT" in duckdb_type or "REAL" in duckdb_type:
        return "FLOAT"
    if "DOUBLE" in duckdb_type:
        return "DOUBLE"
    if "DECIMAL" in duckdb_type or "NUMERIC" in duckdb_type:
        return "DECIMAL"
    if "BOOLEAN" in duckdb_type:
        return "BOOLEAN"
    if "TIMESTAMP" in duckdb_type or "DATE" in duckdb_type:
        return "UTC_DATETIME"
    if "BLOB" in duckdb_type or "BYTEA" in duckdb_type:
        return "BINARY"
    if "JSON" in duckdb_type:
        return "JSON"
    return "STRING"


def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    """
    Update function that performs the core sync.

    Args:
        configuration: Connector configuration settings
        state: Persisted state across syncs
    """
    validate_configuration(configuration)
    log.warning("Example: connectors : motherduck")

    token = configuration["motherduck_token"]
    database = configuration.get("database_name")
    schema_filter = configuration.get("schema_name")
    incremental_column = configuration.get("incremental_column", "updated_at")

    conn = connect(token, database)
    schemas = get_schemas(conn, database, schema_filter)

    if state is None:
        state = {}

    for schema_name in schemas:
        table_names = get_tables(conn, database, schema_name)

        for table_name in table_names:
            fivetran_name = get_fivetran_table_name(database, schema_name, table_name)
            full_name = qualified_name(database, schema_name, table_name)

            log.info(f"Syncing table {full_name}")

            columns = get_columns(conn, database, schema_name, table_name)
            if not columns:
                log.warning(f"No columns found for {full_name}")
                continue

            if fivetran_name not in state:
                state[fivetran_name] = {}

            table_state = state[fivetran_name]

            incr = find_incremental(columns, incremental_column)
            if incr:
                incremental_sync(
                    conn, database, schema_name, table_name, incr, table_state, state
                )
            else:
                reimport_sync(
                    conn, database, schema_name, table_name, table_state, state
                )

    conn.close()


def incremental_sync(
    conn,
    database: str,
    schema_name: str,
    table_name: str,
    incr_column: str,
    table_state: Dict[str, Any],
    global_state: Dict[str, Any],
):
    """
    Perform incremental sync with delete capture.

    Args:
        conn: DuckDB connection
        database: DB name
        schema_name: Schema name
        table_name: Table name
        incr_column: Incremental timestamp column
        table_state: State dict for this table
        global_state: Full connector state
    """
    qualified = qualified_name(database, schema_name, table_name)
    fivetran_name = get_fivetran_table_name(database, schema_name, table_name)

    last_val = table_state.get("last_incremental_value")
    old_checksums = table_state.get("checksums", {})

    query = f"SELECT * FROM {qualified}"
    if last_val:
        query += f" WHERE {incr_column} >= '{last_val}'"
    query += f" ORDER BY {incr_column}"

    result = conn.execute(query)
    rows = result.fetchall()
    cols = [d[0] for d in result.description]

    new_checksums = {}
    max_seen = last_val

    for row_tuple in rows:
        row = serialize(dict(zip(cols, row_tuple)))
        rid = row_id(row)
        chk = checksum(row)
        new_checksums[rid] = chk

        # Change detection
        if rid not in old_checksums or old_checksums[rid] != chk:
            # The 'upsert' operation is used to insert or update data in the destination.
            op.upsert(fivetran_name, row)

        value = row.get(incr_column)
        if value and (max_seen is None or value > max_seen):
            max_seen = value

    # Detect deletions
    deleted = set(old_checksums.keys()) - set(new_checksums.keys())
    for rid in deleted:
        op.delete(fivetran_name, {"_row_id": rid})

    table_state.update(
        {
            "last_incremental_value": max_seen,
            "checksums": new_checksums,
            "last_synced_at": datetime.now(timezone.utc).isoformat(),
        }
    )

    op.checkpoint(global_state)


def reimport_sync(
    conn,
    database: str,
    schema_name: str,
    table_name: str,
    table_state: Dict[str, Any],
    global_state: Dict[str, Any],
):
    """
    Full reimport sync if no incremental column exists.

    Args:
        conn: DuckDB connection
        database: DB name
        schema_name: Schema name
        table_name: Table name
        table_state: State dict for this table
        global_state: Full connector state
    """
    qualified = qualified_name(database, schema_name, table_name)
    fivetran_name = get_fivetran_table_name(database, schema_name, table_name)

    old_checksums = table_state.get("checksums", {})

    query = f"SELECT * FROM {qualified}"
    result = conn.execute(query)
    rows = result.fetchall()
    cols = [d[0] for d in result.description]

    new_checksums = {}

    for row_tuple in rows:
        row = serialize(dict(zip(cols, row_tuple)))
        rid = row_id(row)
        chk = checksum(row)
        new_checksums[rid] = chk

        if rid not in old_checksums or old_checksums[rid] != chk:
            op.upsert(fivetran_name, row)

    deleted = set(old_checksums.keys()) - set(new_checksums.keys())
    for rid in deleted:
        op.delete(fivetran_name, {"_row_id": rid})

    table_state.update(
        {
            "checksums": new_checksums,
            "last_synced_at": datetime.now(timezone.utc).isoformat(),
        }
    )

    op.checkpoint(global_state)


def connect(token: str, database: str = None):
    """
    Create and return a DuckDB connection to MotherDuck.

    Args:
        token: MotherDuck token
        database: Optional DB name

    Returns:
        DuckDB connection object
    """
    conn_str = (
        f"md:{database}?motherduck_token={token}"
        if database
        else f"md:?motherduck_token={token}"
    )

    log.info(f"Connecting to MotherDuck ({database or 'default DB'})")

    for attempt in range(__MAX_RETRIES):
        try:
            return duckdb.connect(conn_str)
        except Exception as err:
            if attempt == __MAX_RETRIES - 1:
                log.severe(f"Connection failed after {__MAX_RETRIES} attempts: {err}")
                raise
            wait = min(60, 2**attempt)
            log.warning(
                f"Connection attempt {attempt + 1} failed, retrying in {wait}s: {err}"
            )
            time.sleep(wait)


def get_schemas(conn, database=None, schema_filter=None):
    """
    Retrieve all schema names.

    Args:
        conn: DuckDB connection
        database: DB name
        schema_filter: Optional schema filter

    Returns:
        List of schema names.
    """
    where, params = build_where(catalog_name=database, schema_name=schema_filter)
    query = f"SELECT schema_name FROM information_schema.schemata {where} ORDER BY schema_name"
    rows = conn.execute(query, params).fetchall()
    return [row[0] for row in rows] or ["main"]


def get_tables(conn, database=None, schema_name=None):
    """
    Retrieve all table names for a given schema.

    Args:
        conn: DuckDB connection
        database: DB name
        schema_name: Schema name

    Returns:
        List of table names.
    """
    where, params = build_where(table_catalog=database, table_schema=schema_name)
    query = f"""
        SELECT table_name
        FROM information_schema.tables
        {where}
        AND table_type='BASE TABLE'
        ORDER BY table_name
    """
    rows = conn.execute(query, params).fetchall()
    return [row[0] for row in rows]


def get_columns(conn, database=None, schema_name=None, table_name=None):
    """
    Retrieve column metadata for a given table.

    Args:
        conn: DuckDB connection
        database: DB name
        schema_name: Schema name
        table_name: Table name

    Returns:
        List of column dicts {name, type}.
    """
    where, params = build_where(
        table_catalog=database, table_schema=schema_name, table_name=table_name
    )
    query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        {where}
        ORDER BY ordinal_position
    """
    rows = conn.execute(query, params).fetchall()
    return [{"name": r[0], "type": r[1]} for r in rows]


def qualified_name(database: str, schema: str, table: str) -> str:
    """
    Build a fully qualified (quoted) table name.

    Args:
        database: DB name
        schema: Schema name
        table: Table name

    Returns:
        Qualified name string.
    """
    parts = [p for p in [database, schema, table] if p]
    return ".".join(f'"{p}"' for p in parts)


def get_fivetran_table_name(database: str, schema: str, table: str) -> str:
    """
    Build a Fivetran-friendly table name.

    Args:
        database: DB name
        schema: Schema name
        table: Table name

    Returns:
        A string like "db_schema_table".
    """
    parts = [p for p in [database, schema, table] if p]
    return "_".join(parts)


def find_incremental(columns: List[Dict[str, str]], default: str) -> str | None:
    """
    Identify an incremental timestamp column.

    Args:
        columns: Column metadata list
        default: Default incremental name to try

    Returns:
        Column name or None
    """
    column_names = [col["name"].lower() for col in columns]
    candidates = [
        default,
        "updated_at",
        "modified_at",
        "last_modified",
        "created_at",
    ]

    for candidate in candidates:
        if candidate.lower() in column_names:
            return candidate

    return None


def serialize(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Serialize a row to JSON-safe values.

    Args:
        row: Row dictionary

    Returns:
        JSON-safe dictionary.
    """
    serialized = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        elif isinstance(value, (list, dict)):
            serialized[key] = json.dumps(value)
        elif isinstance(value, bytes):
            serialized[key] = value.hex()
        else:
            serialized[key] = value
    return serialized


def row_id(row: Dict[str, Any]) -> str:
    """
    Generate a unique row ID based on row content.

    Args:
        row: Row dictionary

    Returns:
        MD5 hash string.
    """
    return hashlib.md5(
        json.dumps(row, sort_keys=True, default=str).encode()
    ).hexdigest()


def checksum(row: Dict[str, Any]) -> str:
    """
    Generate a checksum to detect changes in row data.

    Args:
        row: Row dictionary

    Returns:
        SHA256 hash string.
    """
    return hashlib.sha256(
        json.dumps(row, sort_keys=True, default=str).encode()
    ).hexdigest()


def validate_configuration(configuration: Dict[str, Any]):
    """
    Validate configuration fields.

    Args:
        configuration: Configuration dictionary.

    Raises:
        ValueError: If required fields are missing.
    """
    if not configuration.get("motherduck_token"):
        raise ValueError("Missing required configuration: motherduck_token")


# Create connector object
connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    # Manual local testing mode
    with open("configuration.json", "r") as cfg:
        config = json.load(cfg)

    connector.debug(configuration=config)
