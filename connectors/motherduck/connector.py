"""
 This connector demonstrates how to fetch data from motherduck database and upsert it into destination using duckdb library.
 See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
 and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
 """

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

import duckdb    # DuckDB in-process analytics database engine used to query local/Parquet data
import hashlib   # Provides hashing utilities (e.g., md5/sha256) for checksums or surrogate keys

from datetime import datetime, timezone  # datetime objects and UTC timezone handling for timestamps
from typing import Dict, List, Any  # Type hints for dictionaries, lists and generic objects


__BATCH_SIZE = 10000

def build_where(**kwargs) -> str:
    """
    Build a WHERE clause from provided keyword arguments, ignoring None or empty values.
    Args:
        kwargs: field-value pairs for the WHERE clause
    Returns: WHERE clause string
    """
    conditions = []
    for field, value in kwargs.items():
        if value:
            conditions.append(f"{field} = '{value}'")
    return "WHERE " + " AND ".join(conditions) if conditions else ""

def build_table_schema(conn,database: str, schema_name: str,table_name: str):
    """
    Build a single Fivetran table schema entry from DuckDB metadata.
    Returns None if the table has no columns.
    Args:
        conn: DuckDB connection
        database: Database name
        schema_name: Schema name
        table_name: Table name
    Returns: Fivetran table schema dictionary or None
    """
    cols = get_columns(conn, database, schema_name, table_name)
    if not cols:
        return None

    columns_map: Dict[str, str] = {}
    primary_keys: List[str] = []

    for col in cols:
        col_name = col["name"]
        duck_type = col["type"].upper()
        fivetran_type = map_type(duck_type)

        columns_map[col_name] = fivetran_type

        # Guess primary key (basic heuristic)
        col_name_l = col_name.lower()
        if col_name_l in ("id", "pk") or col_name_l.endswith("_id"):
            primary_keys.append(col_name)

    return {
        "table": f"{schema_name}_{table_name}" if schema_name else table_name,
        "primary_key": primary_keys,
        "columns": columns_map,
    }


def build_all_table_schemas(conn, database: str, schemas: List[str]) -> List[Dict]:
    """
    Iterate over schemas and tables to produce a list of Fivetran table schemas.
    Args:
        conn: DuckDB connection
        database: Database name
        schemas: List of schema names
    Returns: List of Fivetran table schema dictionaries
    """
    tables: List[Dict] = []

    for sname in schemas:
        table_names = get_tables(conn, database, sname)
        for tname in table_names:
            table_def = build_table_schema(conn, database, sname, tname)
            if table_def is not None:
                tables.append(table_def)

    return tables


def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
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
    Map DuckDB data types to Fivetran SDK compatible data types.
    Args:
        duckdb_type: DuckDB data type as a string
    Returns: Fivetran SDK compatible data type as a string
    """
    duckdb_type = duckdb_type.upper()
    if any(k in duckdb_type for k in ["INT", "BIGINT", "SMALLINT", "TINYINT"]):
        return "INT"
    if any(k in duckdb_type for k in ["DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC"]):
        return "FLOAT"
    if "BOOLEAN" in duckdb_type:
        return "BOOLEAN"
    if any(k in duckdb_type for k in ["TIMESTAMP", "DATE"]):
        return "UTC_DATETIME"
    if "BLOB" in duckdb_type or "BYTEA" in duckdb_type:
        return "BINARY"
    if "JSON" in duckdb_type:
        return "JSON"
    return "STRING"



def update(configuration, state):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    token = configuration["motherduck_token"]
    database = configuration.get("database_name")
    schema_filter = configuration.get("schema_name")
    incr_col = configuration.get("incremental_column", "updated_at")
    batch_size = __BATCH_SIZE

    conn = connect(token, database)
    schemas = get_schemas(conn, database, schema_filter)

    if state is None:
        state = {}

    for schema_name in schemas:
        tables = get_tables(conn, database, schema_name)
        for table_name in tables:
            full_table = qualified_name(database, schema_name, table_name)
            fivetran_table_name = get_fivetran_table_name(database, schema_name, table_name)
            log.info(f"Syncing table {full_table}")

            columns = get_columns(conn, database, schema_name, table_name)
            if not columns:
                log.warning(f"No columns found for {full_table}")
                continue

            incremental_field = find_incremental(columns, incr_col)

            if fivetran_table_name not in state:
                state[fivetran_table_name] = {}

            if incremental_field:
                incremental_sync(
                    conn, database, schema_name, table_name, incremental_field, state, batch_size)
            else:
                reimport_sync(conn, database, schema_name, table_name, state, batch_size)

    conn.close()


def incremental_sync(conn, db, schema_name, table_name, inc_col, table_state, batch_size):
    """
    Perform incremental sync for a given table using the specified incremental column.
    Args:
        conn: DuckDB connection
        db: Database name
        schema_name: Schema name
        table_name: Table name
        inc_col: Incremental column name
        table_state: State dictionary for the table
        batch_size: Number of rows to process in each batch
    """
    full = qualified_name(db, schema_name, table_name)
    fivetran_table_name = get_fivetran_table_name(db, schema_name, table_name)
    last_val = table_state.get("last_incremental_value")
    old_checksums = table_state.get("checksums", {})

    query = f"SELECT * FROM {full}"
    if last_val:
        query += f" WHERE {inc_col} > '{last_val}'"
    query += f" ORDER BY {inc_col} LIMIT {batch_size}"

    result = conn.execute(query)
    rows = result.fetchall()
    cols = [d[0] for d in result.description]

    new_checksums = {}
    max_val = last_val

    for row in rows:
        rec = serialize(dict(zip(cols, row)))
        rid = row_id(rec)
        chk = checksum(rec)
        new_checksums[rid] = chk

        if rid not in old_checksums or old_checksums[rid] != chk:
            op.upsert(fivetran_table_name, rec)

        val = rec.get(inc_col)
        if val and (not max_val or val > max_val):
            max_val = val

    # Delete capture
    deleted = set(old_checksums.keys()) - set(new_checksums.keys())
    for rid in deleted:
        op.delete(fivetran_table_name, {"_row_id": rid})

    table_state = {
        "last_incremental_value": max_val,
        "checksums": new_checksums,
        "last_synced_at": datetime.now(timezone.utc).isoformat(),
    }

    op.checkpoint(table_state)


def reimport_sync(conn, db, schema_name, table_name, table_state, batch_size):
    """
    Perform reimport sync for a given table without an incremental column.
    Args:
        conn: DuckDB connection
        db: Database name
        schema_name: Schema name
        table_name: Table name
        table_state: State dictionary for the table
        batch_size: Number of rows to process in each batch
    """
    full = qualified_name(db, schema_name, table_name)
    fivetran_table_name = get_fivetran_table_name(db, schema_name, table_name)
    old = table_state.get("checksums", {})

    query = f"SELECT * FROM {full} LIMIT {batch_size}"
    result = conn.execute(query)
    rows = result.fetchall()
    cols = [d[0] for d in result.description]

    new = {}
    for r in rows:
        rec = serialize(dict(zip(cols, r)))
        rid = row_id(rec)
        chk = checksum(rec)
        new[rid] = chk
        if rid not in old or old[rid] != chk:
            op.upsert(fivetran_table_name, rec)

    deleted = set(old.keys()) - set(new.keys())
    for rid in deleted:
        op.delete(fivetran_table_name, {"_row_id": rid})

    table_state = {
        "checksums": new,
        "last_synced_at": datetime.now(timezone.utc).isoformat(),
    }

    op.checkpoint(table_state)


def connect(token, db=None):
    """
    Create and return a DuckDB connection to MotherDuck.
    Args:
        token: MotherDuck authentication token
        db: Optional database name
    """
    conn_str = f"md:{db}?motherduck_token={token}" if db else f"md:?motherduck_token={token}"
    log.info(f"Connecting to MotherDuck ({'default DB' if not db else db})")
    try:
        return duckdb.connect(conn_str)
    except Exception as e:
        log.severe(f"Connection failed: {e}")
        raise


def get_schemas(conn, db=None, schema_filter=None):
    """
    Retrieve list of schemas from MotherDuck.
    Args:
        conn: DuckDB connection
        db: Optional database name
        schema_filter: Optional schema name to filter
    """
    where = build_where(catalog_name=db, schema_name=schema_filter)
    q = f"SELECT schema_name FROM information_schema.schemata {where} ORDER BY schema_name"
    rows = conn.execute(q).fetchall()
    return [r[0] for r in rows] or ["main"]


def get_tables(conn, db=None, schema_name=None):
    """
    Retrieve list of tables from MotherDuck.
    Args:
        db: Optional database name
        schema_name: Optional schema name to filter
        conn: DuckDB connection
    """

    where = build_where(table_catalog=db, table_schema=schema_name)
    q = f"""
        SELECT table_name
        FROM information_schema.tables
        {where}
        {'AND' if where else 'WHERE'} table_type='BASE TABLE'
        ORDER BY table_name
    """
    rows = conn.execute(q).fetchall()
    return [r[0] for r in rows]


def get_columns(conn, db=None, schema_name=None, table_name=None):
    """
    Retrieve list of columns for a given table from MotherDuck.
    Args:
        conn: DuckDB connection
        db: Optional database name
        schema_name: Schema name
        table_name: Table name
    """
    where = build_where(table_catalog=db, table_schema=schema_name, table_name=table_name)
    q = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        {where}
        ORDER BY ordinal_position
    """
    rows = conn.execute(q).fetchall()
    return [{"name": r[0], "type": r[1]} for r in rows]


def qualified_name(db, schema, table):
    """
    Construct a fully qualified table name with optional database and schema.
    Args:
        db: Database name
        schema: Schema name
        table: Table name
    Returns: Fully qualified table name
    """
    parts = [p for p in [db, schema, table] if p]
    return ".".join(f'"{p}"' for p in parts)

def get_fivetran_table_name(db, schema, table):
    """
    Construct a Fivetran-compatible table name with optional database and schema.
    Args:
        db: Database name
        schema: Schema name
        table: Table name
    """
    parts = [p for p in [db, schema, table] if p]
    return "_".join(f'{p}' for p in parts)


def find_incremental(cols, default):
    """
    Find an appropriate incremental column from the list of columns.
    It checks for common names like 'updated_at', 'modified_at', etc.
    Args:
        cols: List of column dictionaries
        default: Default incremental column name
    """
    names = [c["name"].lower() for c in cols]
    for candidate in [default, "updated_at", "modified_at", "last_modified", "created_at"]:
        if candidate.lower() in names:
            return candidate
    return None


def serialize(row):
    """
    Serialize a row dictionary to ensure all values are JSON serializable.
    This function converts datetime objects to ISO format strings,
    lists and dictionaries to JSON strings, and bytes to hex strings.
    Args:
        row: the data row as a dictionary
    """
    out = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, (list, dict)):
            out[k] = json.dumps(v)
        elif isinstance(v, bytes):
            out[k] = v.hex()
        else:
            out[k] = v
    return out


def row_id(row):
    """
    Generate a unique row identifier based on the row content.
    This is used for delete capture.
    Args:
        row: the data row as a dictionary
    """
    return hashlib.md5(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()


def checksum(row):
    """
    Generate a checksum for the given row.
    This is used to detect changes in the row data.
    Args:
        row: the data row as a dictionary
    """
    return hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module. This is Python's standard entry method allowing your script to
# be run directly from the command line or IDE 'run' button. This is useful for debugging while you write your code.
# Note this method is not called by Fivetran when executing your connector in production. Please test using the
# Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
