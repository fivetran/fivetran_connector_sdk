"""YugabyteDB Connector for Fivetran Connector SDK.
This connector fetches data from YugabyteDB database and syncs it to Fivetran destinations.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For date/time operations and timestamp handling
from datetime import datetime

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For connecting to YugabyteDB using PostgreSQL-compatible driver
import psycopg2

# For dictionary cursor to get column names
from psycopg2.extras import RealDictCursor

# Constants for database configuration
__BATCH_SIZE = 100  # Number of records to fetch per batch
__CHECKPOINT_INTERVAL = 1000  # Number of records between checkpoints
__DEFAULT_START_DATE = "2024-01-01T00:00:00Z"  # Default starting point for incremental sync


def normalize_record(record: dict):
    """
    Normalize data types in a record for Fivetran compatibility.
    Converts datetime objects to ISO format strings and handles other type conversions.
    Args:
        record: Dictionary containing the record data.
    Returns:
        Normalized record with converted data types.
    """
    normalized = {}
    for key, value in record.items():
        if isinstance(value, datetime):
            normalized[key] = value.isoformat()
        elif value is None:
            normalized[key] = None
        else:
            normalized[key] = value
    return normalized


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    required_configs = ["host", "database", "user", "password"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def create_connection(configuration: dict):
    """
    Create and return a connection to YugabyteDB.
    Args:
        configuration: Dictionary containing database connection parameters.
    Returns:
        psycopg2 connection object.
    Raises:
        Exception: if connection fails.
    """
    try:
        connection = psycopg2.connect(
            host=configuration.get("host"),
            port=configuration.get("port", 5433),
            database=configuration.get("database"),
            user=configuration.get("user"),
            password=configuration.get("password"),
        )
        log.info("Successfully connected to YugabyteDB")
        return connection
    except psycopg2.Error as e:
        log.severe(f"Failed to connect to YugabyteDB: {e}")
        raise


def get_table_list(connection, schema_name: str):
    """
    Fetch list of tables from the specified schema.
    Args:
        connection: Active database connection.
        schema_name: Name of the schema to query.
    Returns:
        List of table names.
    """
    cursor = connection.cursor()
    try:
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        cursor.execute(query, (schema_name,))
        tables = [row[0] for row in cursor.fetchall()]
        log.info(f"Found {len(tables)} tables in schema '{schema_name}'")
        return tables
    finally:
        cursor.close()


def get_primary_key_columns(connection, schema_name: str, table_name: str):
    """
    Fetch primary key columns for a given table.
    Args:
        connection: Active database connection.
        schema_name: Name of the schema.
        table_name: Name of the table.
    Returns:
        List of primary key column names.
    """
    cursor = connection.cursor()
    try:
        query = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
            ORDER BY a.attnum
        """
        full_table_name = f"{schema_name}.{table_name}"
        cursor.execute(query, (full_table_name,))
        pk_columns = [row[0] for row in cursor.fetchall()]
        return pk_columns
    finally:
        cursor.close()


def check_incremental_column(connection, schema_name: str, table_name: str):
    """
    Check if table has updated_at column for incremental sync support.
    Args:
        connection: Active database connection.
        schema_name: Name of the schema.
        table_name: Name of the table.
    Returns:
        True if table has updated_at column, False otherwise.
    """
    cursor = connection.cursor()
    try:
        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = 'updated_at'
        """
        cursor.execute(query, (schema_name, table_name))
        return cursor.fetchone() is not None
    finally:
        cursor.close()


def sync_table(connection, schema_name: str, table_name: str, last_updated_at: str, state: dict):
    """
    Sync data from a single table with incremental support and proper checkpointing.
    Args:
        connection: Active database connection.
        schema_name: Name of the schema.
        table_name: Name of the table to sync.
        last_updated_at: ISO timestamp string of last sync for incremental updates.
        state: State dictionary for checkpointing progress.
    Returns:
        Number of records synced and the latest updated_at timestamp as ISO string.
    """
    has_updated_at = check_incremental_column(connection, schema_name, table_name)

    # Use server-side cursor with name for memory-efficient streaming
    cursor = connection.cursor(name=f"cursor_{table_name}", cursor_factory=RealDictCursor)
    cursor.itersize = __BATCH_SIZE  # Fetch records in batches from server

    try:
        # Build query based on whether incremental sync is possible
        if has_updated_at:
            query = f"""
                SELECT * FROM {schema_name}.{table_name}
                WHERE updated_at > %s
                ORDER BY updated_at ASC
            """
            cursor.execute(query, (last_updated_at,))
            log.info(f"Syncing table '{table_name}' incrementally from {last_updated_at}")
        else:
            query = f"SELECT * FROM {schema_name}.{table_name}"
            cursor.execute(query)
            log.info(f"Syncing table '{table_name}' (full sync - no updated_at column)")

        record_count = 0
        latest_timestamp = last_updated_at
        batch = []

        for row in cursor:
            record = dict(row)

            # Normalize data types before processing
            normalized_record = normalize_record(record)

            # Track latest timestamp for incremental sync using original datetime object
            if has_updated_at and "updated_at" in record and record["updated_at"]:
                current_timestamp = record["updated_at"]
                # Compare datetime objects if available, else compare ISO strings
                if isinstance(current_timestamp, datetime):
                    current_timestamp_str = current_timestamp.isoformat()
                else:
                    current_timestamp_str = str(current_timestamp)

                if current_timestamp_str > latest_timestamp:
                    latest_timestamp = current_timestamp_str

            batch.append(normalized_record)

            # Process batch when it reaches the defined size
            if len(batch) >= __BATCH_SIZE:
                for record_item in batch:
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The first argument is the name of the destination table.
                    # The second argument is a dictionary containing the record to be upserted.
                    op.upsert(table=table_name, data=record_item)
                record_count += len(batch)
                batch = []

                # Checkpoint at regular intervals for resumability
                if record_count % __CHECKPOINT_INTERVAL == 0:
                    state[f"{table_name}_last_updated_at"] = latest_timestamp
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(state)
                    log.info(f"Checkpointed at {record_count} records for '{table_name}'")

        # Process remaining records in batch
        if batch:
            for record_item in batch:
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table=table_name, data=record_item)
            record_count += len(batch)

        log.info(f"Completed syncing '{table_name}': {record_count} records")
        return record_count, latest_timestamp
    finally:
        cursor.close()


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    validate_configuration(configuration)
    connection = None
    try:
        connection = create_connection(configuration)
        schema_name = configuration.get("schema", "public")
        tables = get_table_list(connection, schema_name)

        schema_definitions = []
        for table_name in tables:
            pk_columns = get_primary_key_columns(connection, schema_name, table_name)

            table_def = {"table": table_name, "primary_key": pk_columns if pk_columns else None}
            schema_definitions.append(table_def)

        log.info(f"Schema defined for {len(schema_definitions)} tables")
        return schema_definitions
    finally:
        if connection:
            connection.close()


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    log.warning("Example: Database Examples : YugabyteDB Connector")

    validate_configuration(configuration)
    connection = None
    try:
        connection = create_connection(configuration)
        schema_name = configuration.get("schema", "public")
        tables = get_table_list(connection, schema_name)

        total_records = 0

        for table_name in tables:
            # Get last sync timestamp for this table from state
            table_state_key = f"{table_name}_last_updated_at"
            last_updated_at = state.get(table_state_key, __DEFAULT_START_DATE)

            log.info(f"Starting sync for table '{table_name}'")

            # Sync the table with state for intermediate checkpointing
            record_count, latest_timestamp = sync_table(
                connection, schema_name, table_name, last_updated_at, state
            )
            total_records += record_count

            # Update state for this table with final timestamp
            state[table_state_key] = latest_timestamp

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)
            log.info(
                f"Final checkpoint for table '{table_name}' with timestamp: {latest_timestamp}"
            )

        log.info(f"Sync completed. Total records synced: {total_records}")

    except psycopg2.OperationalError as e:
        log.severe(f"Database connection error: {e}")
        raise RuntimeError(
            f"Failed to connect to YugabyteDB. Please check host, port, and credentials: {str(e)}"
        )
    except psycopg2.ProgrammingError as e:
        log.severe(f"Database query error: {e}")
        raise RuntimeError(
            f"Query execution failed. Check if tables exist and user has permissions: {str(e)}"
        )
    except psycopg2.Error as e:
        log.severe(f"Database error occurred: {e}")
        raise RuntimeError(f"Database error: {str(e)}")
    except ValueError as e:
        log.severe(f"Configuration validation error: {e}")
        raise RuntimeError(f"Invalid configuration: {str(e)}")
    except Exception as e:
        log.severe(f"Unexpected error during sync: {e}")
        raise RuntimeError(f"Sync error: {str(e)}")
    finally:
        if connection:
            try:
                connection.close()
                log.info("Database connection closed successfully")
            except Exception as e:
                log.warning(f"Error closing connection: {e}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
