import pymysql

from pymysql.cursors import DictCursor

from datetime import datetime, timedelta

import os

# Import required classes from fivetran_connector_sdk.
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__HOST = "host"
__PORT = "port"
__USER = "user"
__PASSWORD = "password"
__DATABASE = "database"

__ROW_ID_PK_COL = "_fivetran_row_id"
__CHECKPOINT_INTERVAL = 1000
__INTERVAL_HOURS = 6
__INITIAL_SYNC_START = "2025-10-01 00:00:00"
__UPDATED_AT_COLUMN = "updated_at"

def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary for required fields.
    Args:
        configuration (dict): A dictionary containing the connection parameters.
    Raises:
        ValueError: If any required field is missing or invalid.
    """
    required_fields = [__HOST, __PORT, __USER, __PASSWORD, __DATABASE]
    for field in required_fields:
        if field not in configuration or not configuration[field]:
            raise ValueError(f"Missing required configuration field: {field}")
    log.info("Configuration validation passed.")

def getConnection(configuration: dict):
    # CONNECT TO SingleStore
    port = int(configuration.get(__PORT))
    conn = pymysql.connect(
        host=configuration.get(__HOST),
        port=port,
        user=configuration.get(__USER),
        password=configuration.get(__PASSWORD),
        database=configuration.get(__DATABASE),  # <--- THIS IS REQUIRED
        cursorclass=DictCursor,
        autocommit=True,
        ssl={"cert_reqs": 0}  # disables certificate verification
    )
    return conn

def close_connection(conn):
    if conn:
        conn.close()
        log.info("Connection to SingleStoreDB closed.")

def execute_query(cursor, query: str, args: tuple = None):
    if args:
        cursor.execute(query, args)
    else:
        cursor.execute(query)
    return cursor.fetchall()

def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    conn = None
    try:
        conn = getConnection(configuration)
        cursor = conn.cursor()
        log.info("Connected to SingleStoreDB!")
    except Exception as e:
        log.severe(f"Error connecting to SingleStoreDB: {e}")
        raise e

    try:
        schema = configuration.get(__DATABASE)
        tables = get_tables(cursor, schema)
        schema_list = []

        for table in tables:
            primary_keys = get_primary_keys(cursor, schema, table)
            columns = get_all_columns(cursor, schema, table)
            if (len(columns) == 0):
                continue

            schema_list.append({
                "table": table,
                "primary_key": primary_keys,
                "columns": columns
            })

        close_connection(conn)
        return schema_list

    except Exception as e:
        log.severe(f"Error fetching tables from SingleStoreDB: {e}")
        raise e

def get_tables(cursor, schema: str):
    query = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{schema}';"
    rows = execute_query(cursor, query)
    tables = [row.get('TABLE_NAME') for row in rows]
    return tables

def get_primary_keys(cursor, schema, table):
    # Get columns and their kinds (partition_key, clustering, regular)
    query = f"""
    SELECT COLUMN_NAME, COLUMN_KEY
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'
    """

    columns = execute_query(cursor, query)
    pk_columns = []
    for col in columns:
        if col.get('COLUMN_KEY') == 'PRI':
            pk_columns.append(col.get('COLUMN_NAME'))

    if len(pk_columns) == 0:
        log.info(f"No primary keys found for table {table}, using default _fivetran_row_id as primary key.")
        pk_columns = [__ROW_ID_PK_COL]

    return pk_columns

def get_all_columns(cursor, schema, table):
    schema_map = {}

    # Fetch columns metadata
    query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'
    """
    columns = execute_query(cursor, query)

    col_map = {}

    for col in columns:
        # Handle naming and type conventions safely
        data_type = single_store_to_fivetran_type(getattr(col, "DATA_TYPE", None) or "varchar")
        col_map[col.get('COLUMN_NAME')] = data_type

    schema_map = col_map
    return schema_map

def single_store_to_fivetran_type(scylla_type: str) -> str:
    """
    Map SingleStoreDB data types to Fivetran data types.
    Args:
        SingleStoreDB (str): The ScyllaDB data type.
    Returns:
        str: The corresponding Fivetran data type.
    """
    type_mapping = {
        "varchar": "STRING",
        "text": "STRING",
        "int": "INTEGER",
        "bigint": "LONG",
        "float": "FLOAT",
        "double": "DOUBLE",
        "boolean": "BOOLEAN",
        "timestamp": "UTC_DATETIME",
        "date": "DATE",
        "time": "TIME",
        "enum": "STRING"
        # Add more mappings as needed
    }
    return type_mapping.get(scylla_type.lower(), "STRING")  # Default to STRING if unknown


def update(configuration: dict, state: dict):
    """
    Define the update function which contains the logic to extract data from the source
    and load it into the destination.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state information from previous runs.
    """
    conn = None
    try:
        conn = getConnection(configuration)
        cursor = conn.cursor()
        print("Connected to SingleStoreDB!")
    except Exception as e:
        log.severe(f"Error connecting to SingleStoreDB: {e}")
        raise e

    try:
        schema = configuration.get(__DATABASE)
        tables = get_tables(cursor, schema)

        sync_start = datetime.utcnow()

        for table in tables:
            sync_table(cursor, schema, table, state, sync_start)
            if state.get(table) is None:
                state[table] = {}
            state[table]["last_updated_at"] = datetime.strftime(sync_start, "%Y-%m-%d %H:%M:%S")
            op.checkpoint(state=state)

        close_connection(conn)

    except Exception as e:
        log.severe(f"Error during update from SingleStoreDB: {e}")
        raise e

def sync_table(cursor, schema, table, state, sync_start):
    columns = get_all_columns(cursor, schema, table)
    is_update_col_present = __UPDATED_AT_COLUMN in columns
    primary_keys = get_primary_keys(cursor, schema, table)

    if len(columns) == 0:
        log.info(f"No columns found for table {table}, skipping sync.")
        return

    count = 0
    columns_clause = get_columns_clause(columns)

    data_fetch_query = get_incremental_query(table, columns_clause, is_update_col_present)

    last_updated = state.get(table, {}).get("last_updated_at", __INITIAL_SYNC_START)
    start_time = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S")

    while start_time < sync_start:
        results = []
        end_time = min(start_time + timedelta(hours=__INTERVAL_HOURS), sync_start)
        log.info(f"Querying {table} from {start_time} to {end_time}, executing query is:'{data_fetch_query}'")
        start_time_dt = start_time.replace(microsecond=0)
        end_time_dt = end_time.replace(microsecond=0)

        rows = None

        if (is_update_col_present):
            rows = execute_query(cursor, data_fetch_query,
                    (start_time_dt, end_time_dt))
        else:
            rows = execute_query(cursor, data_fetch_query)

        results.extend(rows)

        log.info(f"Number of rows fetched: {len(results)}")
        for row in results:
            count += 1
            if (__ROW_ID_PK_COL in primary_keys):
                row[__ROW_ID_PK_COL] = str(count)
            op.upsert(table, row)

            if count % __CHECKPOINT_INTERVAL == 0:
                op.checkpoint(state)
                log.info(f"Processed {count} records for {table}")

        if not is_update_col_present:
            break
        start_time = end_time
        if (state.get(table) is None):
            state[table] = {}
        state[table]["last_updated_at"] = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        op.checkpoint(state)


def get_incremental_query(table, columns_clause, last_updated_column_present):
    """
    Dynamically build a CQL query to fetch only incrementally updated rows.
    Example:
        SELECT * FROM orders WHERE updated_at > %s AND updated_at <= %s
    """

    query = f"SELECT {columns_clause} FROM {table}"
    if last_updated_column_present:
        query += f" WHERE {__UPDATED_AT_COLUMN} > %s AND {__UPDATED_AT_COLUMN} <= %s"
    return query

def get_columns_clause(columns: dict):
    """
    Generate a comma-separated string of column names for CQL queries.
    Args:
        columns (dict): A dictionary of column names and their types.
    Returns:
        str: A comma-separated string of column names.
    """
    return ", ".join(quote_identifier(c) for c in columns.keys())

def quote_identifier(column_name: str) -> str:
    """
    Handle reserved column names by enclosing them in double quotes.
    Args:
        column_name (str): The original column name.
    Returns:
        str: The modified column name if it's reserved, otherwise the original name.
    """
    reserved_names = {"order", "select", "where", "table", "group", "user", "schema"}
    if column_name.lower() in reserved_names:
        return f'`{column_name}`'
    return column_name

# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    try:
        # Open the configuration.json file and load its contents
        with open("configuration.json", "r") as f:
            configuration = json.load(f)
    except FileNotFoundError:
        log.info("Using empty configuration!")
        configuration = {}

    # Test the connector locally
    connector.debug(configuration=configuration)

# Fivetran Debug Results:
# Operation       | Calls
# ----------------+------------
# Upserts         | 118276
# Updates         | 0
# Deletes         | 0
# Truncates       | 0
# SchemaChanges   | 1
# Checkpoints     | 121
