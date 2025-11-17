"""
This connector demonstrates how to fetch data from a ScyllaDB cluster using the Cassandra driver and upsert it into a Fivetran destination using the Connector SDK.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT  # Core Cassandra driver classes to create a cluster and define/load an execution profile
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy  # Load-balancing policies for distributing queries across nodes in a specific data center
from cassandra.auth import PlainTextAuthProvider  # Auth provider used to pass username/password credentials to Cassandra
from datetime import datetime, timedelta, timezone  # Date/time utilities for incremental cursors, TTL windows, and UTC-safe timestamps

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

import json # For reading configuration from JSON file

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
    """
    Create and return a Cassandra Cluster object using the provided configuration.
    Args:
        configuration (dict): A dictionary containing the connection parameters, including host(s), port, username, password, and other cluster settings.
    Returns:
        Cluster: An instance of the Cassandra Cluster class, connected using the provided configuration.
    Raises:
        Exception: If there is an error while connecting to the cluster.
    """
    try:
        data_center_region = configuration.get("data_center_region", "AWS_US_EAST_1")
        profile = ExecutionProfile(load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(data_center_region)))

        cluster = Cluster(
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            contact_points=[
                configuration.get("host_1"), configuration.get("host_2"), configuration.get("host_3")
            ],
            port=configuration.get("port", 9042),
            auth_provider=PlainTextAuthProvider(username=configuration.get("username"), password=configuration.get("password"))
        )

        log.info('Connecting to cluster')
        return cluster
    except Exception as e:
        log.severe(f"Error while connecting with cluster: {e}")
        raise e

def cluster_shutdown(cluster):
    """
    Shut down the provided Cassandra cluster connection and log the shutdown event.
    Args:
        cluster: The Cassandra Cluster object to be shut down.
    """
    cluster.shutdown()
    log.info('Cluster shutdown')

def schema(configuration: dict):
    """
     Define the schema function which lets you configure the schema your connector delivers.
     See the technical reference documentation for more details on the schema function:
     https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
     Args:
         configuration: a dictionary that holds the configuration settings for the connector.
     """
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
    """
    Fetch all table names in the specified keyspace.
    Args:
        session: The Cassandra/Scylla session object used to execute queries.
        keyspace (str): The name of the keyspace to retrieve tables from.
    Returns:
        list: A list of table names (strings) in the specified keyspace.
    """
    # Fetch all table names in the specified keyspace using a parameterized query to prevent SQL injection
    rows = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s;", (keyspace,))
    tables = [row.table_name for row in rows]
    return tables

def get_primary_keys(session, keyspace, table):
    """
    Retrieve the primary key columns (partition and clustering keys) for a given table in the specified keyspace.
    Args:
        session: The Cassandra/Scylla session object used to execute queries.
        keyspace: The name of the keyspace containing the table.
        table: The name of the table for which to retrieve primary key columns.
    Returns:
        A list of column names that make up the primary key (partition and clustering keys) for the table.
    """

    query = """
    SELECT column_name, kind
    FROM system_schema.columns
    WHERE keyspace_name=%s AND table_name=%s
    """
    columns = session.execute(query, (keyspace, table))
    pk_columns = [col.column_name for col in columns if col.kind in ('partition_key', 'clustering')]
    return pk_columns

def get_all_columns(session, keyspace, table):
    """
    Fetch all columns and their data types for a given table in a keyspace.
    Args:
        session: The Cassandra/Scylla session object used to execute queries.
        keyspace (str): The name of the keyspace containing the table.
        table (str): The name of the table whose columns are to be fetched.
    Returns:
        dict: A dictionary mapping column names to Fivetran-compatible data types.
    """
    query = """
        SELECT column_name, kind, type
        FROM system_schema.columns
        WHERE keyspace_name=%s AND table_name=%s
    """
    columns = session.execute(query, (keyspace, table))

    col_map = {}

    for col in columns:
        # Handle naming and type conventions safely
        data_type = scylla_to_fivetran_type(getattr(col, "type", None) or "text")
        col_map[col.column_name] = data_type

    return col_map

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
    """
      Define the update function, which is a required function, and is called by Fivetran during each sync.
     See the technical reference documentation for more details on the update function
     https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
     Args:
         configuration: A dictionary containing connection details
         state: A dictionary containing state information from previous runs
         The state dictionary is empty for the first sync or for any full re-sync
     """
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

        sync_start = datetime.now(timezone.utc)

        for table in tables:
            sync_table(session, state, keyspace, table, sync_start)
            if state.get(table) is None:
                state[table] = {}
            state[table]["last_updated_at"] = datetime.strftime(sync_start, "%Y-%m-%d %H:%M:%S")

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)

    except Exception as e:
        log.severe(f"Error during update: {e}")
        raise e

    cluster_shutdown(cluster)

def sync_table(session, state: dict, keyspace: str, table: str, sync_start):
    """
    Synchronize a single table from ScyllaDB to the destination.
    Args:
        session: The Cassandra/ScyllaDB session object used to execute queries.
        state: A dictionary holding the connector's state, including last sync timestamps.
        keyspace: The keyspace containing the table to sync.
        table: The name of the table to synchronize.
        sync_start: The datetime object representing the upper bound of the sync window.
    This function fetches records from the specified table in time intervals, upserts them into the destination,
    and checkpoints progress at regular intervals.
    """
    columns = get_all_columns(session, keyspace, table)
    count = 0
    columns_clause = get_columns_clause(columns)
    data_fetch_query = get_incremental_query(table, columns_clause, __UPDATED_AT_COLUMN)

    last_updated = state.get(table, {}).get("last_updated_at", __INITIAL_SYNC_START)
    start_time = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S")

    while start_time < sync_start:
        end_time = min(start_time + timedelta(hours=__INTERVAL_HOURS), sync_start)
        log.info(f"Querying {table} from {start_time} to {end_time}, executing query is:'{data_fetch_query}'")

        start_time_dt = start_time.replace(microsecond=0)
        end_time_dt = end_time.replace(microsecond=0)

        rows = session.execute(data_fetch_query,
        [start_time_dt, end_time_dt])

        log.info(f"Number of rows fetched: {len(rows)}")
        for row in rows:
            row_dict = dict(row._asdict())

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert(table, row_dict)
            count += 1

            if count % __CHECKPOINT_INTERVAL == 0:
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(state)
                log.info(f"Processed {count} records for {table}")

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
