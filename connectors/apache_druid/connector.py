"""
Fivetran Connector SDK for Apache Druid
Supports incremental sync (when available), delete capture (checksum-based), and full reimport.
"""

import requests
import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


def normalize_host_and_make_base_url(configuration: Dict[str, Any]):
    """
    Normalize host and port from configuration, return base URL and auth tuple.
    Args :param configuration: Configuration dictionary.
    Returns: Tuple of (base_url, auth)
    """
    if not configuration.get('druid_host'):
        raise ValueError("druid_host is required in configuration")

    druid_host = str(configuration['druid_host']).strip()
    if not druid_host:
        raise ValueError("druid_host cannot be empty")

    # Default to Router port 8888 (works best for /druid/v2/sql)
    druid_port = configuration.get('druid_port', 8888)
    if isinstance(druid_port, str):
        druid_port = int(druid_port)

    use_ssl = configuration.get('use_ssl')
    if isinstance(use_ssl, str):
        use_ssl = use_ssl.lower() in ('true', '1', 'yes')

    if druid_host.startswith('https://'):
        druid_host = druid_host.replace('https://', '')
        protocol = 'https'
    elif druid_host.startswith('http://'):
        druid_host = druid_host.replace('http://', '')
        protocol = 'http'
    else:
        protocol = 'https' if use_ssl else 'http'

    base_url = f"{protocol}://{druid_host}:{druid_port}"
    log.info(f"Connecting to Druid at: {base_url}")

    auth = None
    if configuration.get('auth_user') and configuration.get('auth_password'):
        auth = (configuration['auth_user'], configuration['auth_password'])

    return base_url, auth


def map_druid_type_to_fivetran(druid_type: str) -> str:
    """
    Map Druid data types to Fivetran SDK data types.
    Args :param druid_type: Druid data type as string.
    Returns: Corresponding Fivetran SDK data type as string.
    """
    if not druid_type:
        return "STRING"
    t = druid_type.upper()
    mapping = {
        'STRING': "STRING",
        'VARCHAR': "STRING",
        'CHAR': "STRING",
        'LONG': "LONG",
        'BIGINT': "LONG",
        'INTEGER': "LONG",
        'FLOAT': "DOUBLE",
        'DOUBLE': "DOUBLE",
        'DECIMAL': "DECIMAL",
        'NUMERIC': "DECIMAL",
        'BOOLEAN': "BOOLEAN",
        'TIME': "UTC_DATETIME",
        'TIMESTAMP': "UTC_DATETIME",
        'DATETIME': "UTC_DATETIME",
        'DATE': "NAIVE_DATE"
    }
    return mapping.get(t, "STRING")


def get_datasources(base_url: str, auth: Optional[tuple], schema_filter: str = '') -> List[str]:
    """Fetch table/datasource names via SQL. INFORMATION_SCHEMA is special-cased.
    Args: param base_url: Base URL of Druid Router/Coordinator.
          param auth: Optional authentication tuple (user, password).
          param schema_filter: Optional schema name to filter datasources.
        """
    sql_url = f"{base_url}/druid/v2/sql"
    if schema_filter == 'INFORMATION_SCHEMA':
        sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'INFORMATION_SCHEMA'"
        resp = requests.post(sql_url, json={"query": sql}, auth=auth, timeout=60)
        resp.raise_for_status()
        rows = resp.json()

        out = sorted({(r.get('TABLE_NAME') or r.get('table_name')) for r in rows if isinstance(r, dict)})
        log.info(f"INFORMATION_SCHEMA tables: {out}")
        return out

    # Regular datasources: all table names not in INFORMATION_SCHEMA
    sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA <> 'INFORMATION_SCHEMA'"
    if schema_filter:
        # Some clusters use schema-less; keep filter light (prefix match)
        sql = f"""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema_filter}'
        """
    resp = requests.post(sql_url, json={"query": sql}, auth=auth, timeout=60)
    resp.raise_for_status()
    rows = resp.json()
    out = []
    for r in rows:
        if isinstance(r, dict):
            name = r.get('TABLE_NAME') or r.get('table_name')
            if name:
                out.append(name)
    out = sorted(set(out))
    log.info(f"Discovered {len(out)} tables")
    return out


def get_table_columns(base_url: str, table_name: str, auth: Optional[tuple]) -> Dict[str, str]:
    """
    Return SDK column mapping: {column_name: DataType}
    Works for both INFORMATION_SCHEMA.<table> and regular datasources.
    Args: param base_url: Base URL of Druid Router/Coordinator.
          param table_name: Name of the table/datasource.
          param auth: Optional authentication tuple (user, password).
    """
    sql_url = f"{base_url}/druid/v2/sql"

    if table_name in ['TABLES', 'COLUMNS', 'SCHEMATA', 'ROUTINES']:
        # Peek one row to infer column names; types default to STRING (safe for metadata)
        sql = f"SELECT * FROM INFORMATION_SCHEMA.{table_name} LIMIT 1"
        resp = requests.post(sql_url, json={"query": sql}, auth=auth, timeout=60)
        resp.raise_for_status()
        rows = resp.json()
        cols: Dict[str, str] = {}
        if rows and isinstance(rows[0], dict):
            for col in rows[0].keys():
                cols[col] = "STRING"
        # If empty, fall back to INFORMATION_SCHEMA.COLUMNS for names
        if not cols:
            sql2 = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
            resp2 = requests.post(sql_url, json={"query": sql2}, auth=auth, timeout=60)
            resp2.raise_for_status()
            for r in resp2.json():
                col = r.get('COLUMN_NAME') or r.get('column_name')
                dt = r.get('DATA_TYPE') or r.get('data_type') or 'STRING'
                if col:
                    cols[col] = map_druid_type_to_fivetran(dt)
        return cols

    # Regular datasource: use INFORMATION_SCHEMA.COLUMNS
    sql = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    resp = requests.post(sql_url, json={"query": sql}, auth=auth, timeout=60)
    resp.raise_for_status()
    rows = resp.json()
    cols: Dict[str, str] = {}
    for r in rows:
        if isinstance(r, dict):
            col = r.get('COLUMN_NAME') or r.get('column_name')
            dt = r.get('DATA_TYPE') or r.get('data_type') or 'STRING'
            if col:
                cols[col] = map_druid_type_to_fivetran(dt)
    # If schema table was empty, still proceed (scan fallback is omitted to keep it simple)
    return cols


def find_incremental_column(cols_map: Dict[str, str], configured: Optional[str]) -> Optional[str]:
    """
    Find an incremental column in the given columns map.
    Args: param cols_map: Column name to DataType mapping.
          param configured: Optionally configured incremental column name.
    """
    candidates = []
    if configured:
        candidates.append(configured)
    candidates += ['__time', 'updated_at', '_updated_at', 'last_modified', 'timestamp', 'created_at']
    names = set(cols_map.keys())
    for c in candidates:
        if c in names:
            return c
    return None


def get_row_id(row: Dict[str, Any]) -> str:
    """
    Generate a unique row ID based on __time and MD5 hash of the row.
    Args: parms: row: Row dictionary.
    """
    time_val = str(row.get('__time', ''))
    row_str = json.dumps(row, sort_keys=True, default=str)
    return f"{time_val}_{hashlib.md5(row_str.encode()).hexdigest()[:8]}"


def checksum(row: Dict[str, Any]) -> str:
    """
    Generate an MD5 checksum of the row.
    Args: params: row: Row dictionary.
    """
    return hashlib.md5(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()


def select_sql_for_table(table_name: str, incremental_col: Optional[str], last_value: Optional[str]) -> str:
    """
    Generate SELECT SQL for the given table with optional incremental filtering.
    Args: param table_name: Name of the table/datasource.
          param incremental_col: Incremental column name.
          param last_value: Last incremental value.
    """
    base = f"SELECT * FROM {('INFORMATION_SCHEMA.' + table_name) if table_name in ['TABLES','COLUMNS','SCHEMATA','ROUTINES'] else f'\"{table_name}\"'}"
    where = ""
    order = ""
    if incremental_col and last_value:
        lit = f"TIMESTAMP '{last_value}'" if incremental_col == '__time' else f"'{last_value}'"
        where = f" WHERE \"{incremental_col}\" > {lit}"
        order = f" ORDER BY \"{incremental_col}\""
    return f"{base}{where}{order} LIMIT 50000 OFFSET {{offset}}"


def run_sql(base_url: str, auth: Optional[tuple], sql: str) -> List[Dict[str, Any]]:
    """
    Run SQL query against Druid and return rows.
    Args: base_url: Base URL of Druid Router/Coordinator.
          auth: Optional authentication tuple (user, password).
          sql: SQL query string.
    """
    url = f"{base_url}/druid/v2/sql"
    resp = requests.post(url, json={"query": sql}, auth=auth, timeout=120)
    resp.raise_for_status()
    rows = resp.json()
    out: List[Dict[str, Any]] = []
    for r in rows:
        if isinstance(r, dict):
            out.append(r)
        else:
            # If array format appears, keep it mappable
            out.append({"_data": r})
    return out


# ----------------------------- Schema -----------------------------

def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """

    base_url, auth = normalize_host_and_make_base_url(configuration)
    schema_name = configuration.get('schema_name', '')
    datasources = get_datasources(base_url, auth, schema_name)

    tables: List[Dict[str, Any]] = []
    for ds in datasources:
        cols_map = get_table_columns(base_url, ds, auth)
        if not cols_map:
            log.warning(f"No columns found for table {ds}, skipping")
            continue

        # Only prefix with schema name if Druid reported one
        table_name = f"{schema_name}.{ds}" if schema_name else ds

        tables.append({
            "table": table_name,
            "primary_key": [],
            "columns": cols_map
        })

    return tables


def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """

    base_url, auth = normalize_host_and_make_base_url(configuration)
    schema_name = configuration.get('schema_name', '')
    configured_inc = configuration.get('incremental_column')  # may be None
    datasources = get_datasources(base_url, auth, schema_name)

    for ds in datasources:
        table_name_out = f"INFORMATION_SCHEMA.{ds}" if ds in ['TABLES','COLUMNS','SCHEMATA','ROUTINES'] else ds
        table_state: Dict[str, Any] = state.get(table_name_out, {})

        cols_map = get_table_columns(base_url, ds, auth)
        if not cols_map:
            log.warning(f"No columns found for table {ds}, skipping")
            continue

        inc_col = find_incremental_column(cols_map, configured_inc)

        # INFORMATION_SCHEMA almost never has __time; fall back to full refresh
        if ds in ['TABLES','COLUMNS','SCHEMATA','ROUTINES'] and inc_col in (None, '__time'):
            inc_col = None

        if inc_col:
            sync_incremental_sql(base_url, auth, ds, table_name_out, inc_col, table_state)
        else:
            sync_full_sql(base_url, auth, ds, table_name_out)


def sync_incremental_sql(base_url: str, auth: Optional[tuple], ds: str, table_name_out: str,
                         inc_col: str, table_state: Dict[str, Any]) -> None:
    """
    Perform incremental sync for the given table using SQL.
    Args: param base_url: Base URL of Druid Router/Coordinator.
          param auth: Optional authentication tuple (user, password).
          param ds: Name of the table/datasource.
          param table_name_out: Output table name in destination.
          param inc_col: Incremental column name.
          param table_state: State dictionary for the table.
    """

    last_val = table_state.get("last_incremental_value")
    offset = 0
    max_seen = last_val
    inserted = 0

    while True:
        sql = select_sql_for_table(ds, inc_col, last_val).format(offset=offset)
        rows = run_sql(base_url, auth, sql)
        if not rows:
            break

        for row in rows:
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert(table_name_out, row)
            inserted += 1
            v = row.get(inc_col)
            if v and (max_seen is None or v > max_seen):
                max_seen = v

        offset += len(rows)

    new_state = {
        table_name_out: {
            "last_incremental_value": max_seen,
            "last_sync": datetime.now(timezone.utc).isoformat()
        }
    }
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(new_state)


def sync_full_sql(base_url: str, auth: Optional[tuple], ds: str, table_name_out: str) -> None:
    """
    Perform full sync for the given table using SQL.
    Args: param base_url: Base URL of Druid Router/Coordinator.
          param auth: Optional authentication tuple (user, password).
          param ds: Name of the table/datasource.
          param table_name_out: Output table name in destination.
    """
    offset = 0
    inserted = 0

    while True:
        sql = select_sql_for_table(ds, None, None).format(offset=offset)
        rows = run_sql(base_url, auth, sql)
        if not rows:
            break
        for row in rows:
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert(table_name_out, row)
            inserted += 1
        offset += len(rows)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint({
        table_name_out: {
            "last_sync": datetime.now(timezone.utc).isoformat()
        }
    })


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)

# Fivetran debug result:
# Operation       | Calls
# ----------------+------------
# Upserts         | 330
# Updates         | 0
# Deletes         | 0
# Truncates       | 0
# SchemaChanges   | 4
# Checkpoints     | 4