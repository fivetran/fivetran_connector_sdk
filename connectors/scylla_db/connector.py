# This is an example for how to work with the fivetran_connector_sdk module.
# Imports classes and constants from the `cassandra.cluster` module, which provides
# the main interface for connecting to and interacting with an Apache Cassandra cluster.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details


from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime, timedelta, timezone

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import required libraries
import json

__CHECKPOINT_INTERVAL = 1000
__INTERVAL_HOURS = 6
__INITIAL_SYNC_START = "2025-10-19 00:00:00"
__UPDATED_AT_COLUMN = "updated_at"

def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary for required fields.
    Args:
        configuration (dict): A dictionary containing the connection parameters.
    Raises:
        ValueError: If any required field is missing or invalid.
    """
    required_fields = ["host_1", "port", "username", "password", "dc_region", "keyspace"]
    for field in required_fields:
        if field not in configuration or not configuration[field]:
            raise ValueError(f"Missing required configuration field: {field}")
    log.info("Configuration validation passed.")

def get_cluster(configuration: dict):
    try:
        profile = ExecutionProfile(load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='AWS_US_EAST_1')))
        return Cluster(
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        contact_points=[
            configuration.get("host_1"), configuration.get("host_2"), configuration.get("host_3")
        ],
        port=configuration.get("port", 9042),
        auth_provider = PlainTextAuthProvider(username=configuration.get("username"), password=configuration.get("password")))
        log.info('Connecting to cluster')
        return cluster
    except Exception as e:
        log.severe(f"Error while connecting with cluster: {e}")
        raise e

def cluster_shutdown(cluster):
    cluster.shutdown()
    log.info('Cluster shutdown')

def schema(configuration: dict):
    schema = []

    cluster = get_cluster(configuration)
    session = cluster.connect()
    keyspace = configuration["keyspace"]
    session.set_keyspace(keyspace)
    tables = get_tables(session, keyspace)

    for table in tables:
        pk_columns = get_primary_keys(session, keyspace, table)
        col_map = get_all_columns(session, keyspace, table)

        table_schema = {
            "table": table,
            "primary_key": pk_columns,
            "columns": col_map
        }

        schema.append(table_schema)
    cluster_shutdown(cluster)
    return schema

def get_tables(session, keyspace: str):
    # Fetch all table names in the specified keyspace
    rows = session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{keyspace}';")
    tables = [row.table_name for row in rows]
    return tables

def get_primary_keys(session, keyspace, table):
    # Get columns and their kinds (partition_key, clustering, regular)
    columns = session.execute(f"""
    SELECT column_name, kind
    FROM system_schema.columns
    WHERE keyspace_name='{keyspace}' AND table_name='{table}'
    """)
    pk_columns = [col.column_name for col in columns if col.kind in ('partition_key', 'clustering')]

    return pk_columns

def get_all_columns(session, keyspace, table):
    # Fetch columns metadata (some Scylla versions use type_name instead of type)
    query = f"""
        SELECT column_name, kind, type
        FROM system_schema.columns
        WHERE keyspace_name='{keyspace}' AND table_name='{table}'
    """
    columns = session.execute(query)

    col_map = {}

    for col in columns:
        # Handle naming and type conventions safely
        data_type = scylla_to_fivetran_type(getattr(col, "type", None) or "text")
        col_map[col.column_name] = data_type

    schema_map = col_map
    return schema_map

def scylla_to_fivetran_type(scylla_type: str) -> str:
    type_mapping = {
        "ascii": "STRING",
        "bigint": "LONG",
        "blob": "STRING",
        "boolean": "BOOLEAN",
        "date": "NAIVE_DATE",
        "decimal": "DOUBLE",
        "double": "DOUBLE",
        "float": "FLOAT",
        "inet": "STRING",
        "int": "INT",
        "smallint": "INT",
        "text": "STRING",
        "time": "TIME",
        "timestamp": "UTC_DATETIME",
        "timeuuid": "STRING",
        "tinyint": "INT",
        "uuid": "STRING",
        "varchar": "STRING",
        "varint": "LONG",
        # Add more mappings as needed
    }
    return type_mapping.get(scylla_type, "STRING")  # Default to STRING if type not found

def update(configuration: dict, state: dict):
    try:
        validate_configuration(configuration)
    except ValueError as ve:
        log.severe(f"Configuration validation error: {ve}")
        raise ve

    try:
        cluster = get_cluster(configuration)
    except Exception as e:
            log.severe(f"Error while connecting with cluster: {e}")
            raise e

    try:
        session = cluster.connect()
        keyspace = configuration["keyspace"]
        session.set_keyspace(keyspace)
        tables = get_tables(session, keyspace)

        sync_start = datetime.now()

        for table in tables:
            sync_table(session, state, keyspace, table, sync_start)
            if state.get(table) is None:
                state[table] = {}
            state[table]["last_updated_at"] = datetime.strftime(sync_start, "%Y-%m-%d %H:%M:%S")
            op.checkpoint(state=state)

    except Exception as e:
        log.severe(f"Error during update: {e}")
        raise e

    cluster_shutdown(cluster)

def sync_table(session, state: dict, keyspace: str, table: str, sync_start):
    columns = get_all_columns(session, keyspace, table)
    count = 0
    columns_clause = get_columns_clause(columns)
    data_fetch_query = get_incremental_query(table, columns_clause, __UPDATED_AT_COLUMN)

    last_updated = state.get(table, {}).get("last_updated_at", __INITIAL_SYNC_START)
    start_time = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S")

    while start_time < sync_start:
        results = []
        end_time = min(start_time + timedelta(hours=__INTERVAL_HOURS), sync_start)
        log.info(f"Querying {table} from {start_time} to {end_time}, executing query is:'{data_fetch_query}'")

        start_time_dt = start_time.replace(microsecond=0)
        end_time_dt = end_time.replace(microsecond=0)

        rows = session.execute(data_fetch_query,
        [start_time_dt, end_time_dt])

        results.extend(rows)

        log.info(f"Number of rows fetched: {len(results)}")
        for row in results:
            row_dict = dict(row._asdict())
            op.upsert(table, row_dict)
            count += 1

            if count % __CHECKPOINT_INTERVAL == 0:
                op.checkpoint()
                print(f"Processed {count} records for {table}")

        start_time = end_time


def get_incremental_query(table, columns_clause, last_updated_column=__UPDATED_AT_COLUMN):
    """
    Dynamically build a CQL query to fetch only incrementally updated rows.
    Example:
        SELECT * FROM orders WHERE updated_at > %s ALLOW FILTERING
    """

    query = f"SELECT {columns_clause} FROM {table} WHERE {last_updated_column} > %s AND {last_updated_column} <= %s ALLOW FILTERING"
    return query

def get_columns_clause(columns: dict) -> str:
    """
    Generate a comma-separated string of column names for CQL queries.
    Args:
        columns (dict): A dictionary of column names and their types.
    Returns:
        str: A comma-separated string of column names.
    """
    return ", ".join(columns.keys())

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

# Fivetran Debug Results
# Operation       | Calls
# ----------------+------------
# Upserts         | 112
# Updates         | 0
# Deletes         | 0
# Truncates       | 0
# SchemaChanges   | 3
# Checkpoints     | 3
