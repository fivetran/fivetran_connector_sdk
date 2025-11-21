# This is an example for how to work with the fivetran_connector_sdk module.
# This example demonstrates how to create a connector that connects to a Neo4j database and syncs data from it.
# It uses the public twitter database available at neo4j+s://demo.neo4jlabs.com:7687.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import the required classes from the connector SDK
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Import core libraries for database connection (duckdb), hashing, JSON serialization, datetime handling, and Python type annotations.
import duckdb
import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any


__BATCH_SIZE = 10000

def build_where(**kwargs) -> str:
    """
    Build a WHERE clause from provided keyword arguments, ignoring None or empty values.
    Args: param kwargs: field-value pairs for the WHERE clause
    Returns: WHERE clause string
    """
    conditions = []
    for field, value in kwargs.items():
        if value:
            conditions.append(f"{field} = '{value}'")
    return "WHERE " + " AND ".join(conditions) if conditions else ""


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
    tables = []

    for sname in schemas:
        table_names = get_tables(conn, database, sname)
        for tname in table_names:
            cols = get_columns(conn, database, sname, tname)
            if not cols:
                continue

            # Convert DuckDB types to Fivetran SDK compatible ones
            columns_map = {}
            primary_keys = []
            for col in cols:
                col_name = col["name"]
                duck_type = col["type"].upper()
                fivetran_type = map_type(duck_type)
                columns_map[col_name] = fivetran_type

                # Guess primary key (basic heuristic)
                if col_name.lower() in ("id", "pk") or col_name.lower().endswith("_id"):
                    primary_keys.append(col_name)

            tables.append(
                {
                    "table": f"{sname}_{tname}" if sname else tname,
                    "primary_key": primary_keys,
                    "columns": columns_map,
                }
            )

    conn.close()
    log.info(f"✅ Discovered {len(tables)} tables")
    return tables

def map_type(duckdb_type: str) -> str:
    """
    Map DuckDB data types to Fivetran SDK compatible data types.
    Args: param duckdb_type: DuckDB data type as a string
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
                yield from incremental_sync(
                    conn, database, schema_name, table_name, incremental_field, state, batch_size)
            else:
                yield from reimport_sync(conn, database, schema_name, table_name, state, batch_size)

    conn.close()


def incremental_sync(conn, db, schema_name, table_name, inc_col, table_state, batch_size):
    """
    Perform incremental sync for a given table using the specified incremental column.
    Args: param conn: DuckDB connection
    :param db: Database name
    :param schema_name: Schema name
    :param table_name: Table name
    :param inc_col: Incremental column name
    :param table_state: State dictionary for the table
    :param batch_size: Number of rows to process in each batch
    :return: Yields upsert and delete operations
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

    for r in rows:
        rec = serialize(dict(zip(cols, r)))
        rid = row_id(rec)
        chk = checksum(rec)
        new_checksums[rid] = chk

        if rid not in old_checksums or old_checksums[rid] != chk:
            yield op.upsert(fivetran_table_name, rec)

        val = rec.get(inc_col)
        if val and (not max_val or val > max_val):
            max_val = val

    # Delete capture
    deleted = set(old_checksums.keys()) - set(new_checksums.keys())
    for rid in deleted:
        yield op.delete(fivetran_table_name, {"_row_id": rid})

    table_state[fivetran_table_name] = {
        "last_incremental_value": max_val,
        "checksums": new_checksums,
        "last_synced_at": datetime.now(timezone.utc).isoformat(),
    }

    yield op.checkpoint(table_state)


def reimport_sync(conn, db, schema_name, table_name, table_state, batch_size):
    """
    Perform reimport sync for a given table without an incremental column.
    Args: param conn: DuckDB connection
    :param db: Database name
    :param schema_name: Schema name
    :param table_name: Table name
    :param table_state: State dictionary for the table
    :param batch_size: Number of rows to process in each batch
    :return: Yields upsert and delete operations
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
            yield op.upsert(fivetran_table_name, rec)

    deleted = set(old.keys()) - set(new.keys())
    for rid in deleted:
        yield op.delete(fivetran_table_name, {"_row_id": rid})

    table_state[fivetran_table_name] = {
        "checksums": new,
        "last_synced_at": datetime.now(timezone.utc).isoformat(),
    }

    yield op.checkpoint(table_state)


def connect(token, db=None):
    """
    Create and return a DuckDB connection to MotherDuck.
    Args: param: token: MotherDuck authentication token
          param: db: Optional database name
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
    Args: param db: Optional database name
          param schema_filter: Optional schema name to filter
    """
    where = build_where(catalog_name=db, schema_name=schema_filter)
    q = f"SELECT schema_name FROM information_schema.schemata {where} ORDER BY schema_name"
    rows = conn.execute(q).fetchall()
    return [r[0] for r in rows] or ["main"]


def get_tables(conn, db=None, schema_name=None):
    """
    Retrieve list of tables from MotherDuck.
    Args:  param db: Optional database name
           param schema_name: Optional schema name to filter
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
    Args: param conn: DuckDB connection
    :param db: Optional database name
    :param schema_name: Schema name
    :param table_name: Table name
    :return: List of columns with their names and data types
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
    :param: db: Database name
    :param: schema: Schema name
    :param: table: Table name
    :return: Fully qualified table name
    """
    parts = [p for p in [db, schema, table] if p]
    return ".".join(f'"{p}"' for p in parts)

def get_fivetran_table_name(db, schema, table):
    """
    Construct a Fivetran-compatible table name with optional database and schema.
    :param: db: Database name
    :param: schema: Schema name
    :param: table: Table name
    """
    parts = [p for p in [db, schema, table] if p]
    return "_".join(f'{p}' for p in parts)


def find_incremental(cols, default):
    """
    Find an appropriate incremental column from the list of columns.
    It checks for common names like 'updated_at', 'modified_at', etc.
    Args: param: cols: List of column dictionaries
    default: Default incremental column name
    :return: Incremental column name or None
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
    Args: param: row: the data row as a dictionary
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
    Args: param row: the data row as a dictionary
    """
    return hashlib.md5(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()


def checksum(row):
    """
    Generate a checksum for the given row.
    This is used to detect changes in the row data.
    Args: param row: the data row as a dictionary
    """
    return hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()


connector = Connector(
    schema=schema,
    update=update,
)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)

# Fivetran debug results:
# Operation       | Calls
# ----------------+------------
# Upserts         | 20
# Updates         | 0
# Deletes         | 0
# Truncates       | 0
# SchemaChanges   | 3
# Checkpoints     | 3
