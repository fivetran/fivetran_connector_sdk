"""This is an example connector to sync data from RethinkDB using Fivetran Connector SDK.
RethinkDB is an open-source database designed for real-time applications with features like changefeeds for live data updates.
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

# For connecting to RethinkDB database
from rethinkdb import r
from rethinkdb.errors import (
    ReqlDriverError,
    ReqlAuthError,
    ReqlOpFailedError,
    ReqlRuntimeError,
)

# For handling SSL and connection errors
import ssl

__CHECKPOINT_INTERVAL = 100  # Checkpoint after processing every 100 records


def create_ssl_context():
    """
    Create SSL context for secure RethinkDB connections.
    Returns:
        ssl.SSLContext: SSL context configured for RethinkDB connections
    """
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    return ssl_context


def handle_rethinkdb_error(
    operation_name: str,
    error_exceptions: tuple = None,
    raise_error: bool = True,
    default_value=None,
):
    """
    Common error handler decorator for RethinkDB operations.
    Args:
        operation_name: Description of the operation for error messages
        error_exceptions: Tuple of exception types to catch (defaults to common RethinkDB errors)
        raise_error: If True, raises RuntimeError; if False, returns default_value on error
        default_value: Value to return when raise_error is False (defaults to None)
    Returns:
        Decorated function with error handling wrapper
    """
    if error_exceptions is None:
        error_exceptions = (ReqlOpFailedError, ReqlDriverError, ReqlRuntimeError, ConnectionError, OSError)

    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except error_exceptions as e:
                error_type = type(e).__name__
                error_msg = f"{error_type} during {operation_name}: {str(e)}"

                if raise_error:
                    log.severe(error_msg)
                    raise RuntimeError(f"{operation_name} failed: {str(e)}")
                else:
                    log.warning(error_msg)
                    return default_value

        return wrapper

    return decorator


def connect_to_rethinkdb(configuration: dict):
    """
    Establish connection to RethinkDB database using configuration parameters.
    Args:
        configuration: Dictionary containing connection parameters (host, port, database, username, password, use_ssl)
    Returns:
        Connection object for RethinkDB
    Raises:
        RuntimeError: If connection to RethinkDB fails
    """
    try:
        host = configuration.get("host")
        port = int(configuration.get("port", 28015))
        database = configuration.get("database")
        username = configuration.get("username")
        password = configuration.get("password")
        use_ssl = configuration.get("use_ssl", "false").lower() == "true"

        connection_params = {"host": host, "port": port, "db": database}

        if username:
            connection_params["user"] = username
            connection_params["password"] = password

        if use_ssl:
            connection_params["ssl"] = create_ssl_context()

        conn = r.connect(**connection_params)
        log.info(f"Successfully connected to RethinkDB at {host}:{port}, database: {database}")
        return conn

    except ReqlAuthError as e:
        log.severe(f"Authentication failed for RethinkDB: {str(e)}")
        raise RuntimeError(f"Unable to authenticate with RethinkDB: {str(e)}")
    except ReqlDriverError as e:
        log.severe(f"Driver error connecting to RethinkDB: {str(e)}")
        raise RuntimeError(f"Unable to establish RethinkDB connection: {str(e)}")
    except (ValueError, TypeError, OSError) as e:
        log.severe(f"Configuration or connection error: {str(e)}")
        raise RuntimeError(f"Unable to establish RethinkDB connection: {str(e)}")


@handle_rethinkdb_error("retrieving table list", (ReqlOpFailedError, ReqlDriverError, Exception))
def get_all_tables(conn, database: str) -> list:
    """
    Retrieve list of all tables in the RethinkDB database.
    Args:
        conn: RethinkDB connection object
        database: Name of the database to query
    Returns:
        list: List of table names in the database
    """
    tables = r.db(database).table_list().run(conn)
    log.info(f"Found {len(tables)} tables in database: {', '.join(tables)}")
    return tables


@handle_rethinkdb_error(
    "getting primary key",
    (ReqlOpFailedError, ReqlDriverError, Exception),
    raise_error=False,
    default_value=["id"],
)
def get_table_primary_key(conn, database: str, table_name: str) -> list:
    """
    Get the primary key field(s) for a RethinkDB table.
    Args:
        conn: RethinkDB connection object
        database: Name of the database
        table_name: Name of the table
    Returns:
        list: List containing primary key field name(s)
    Note:
        Defaults to 'id' if primary key cannot be determined. RethinkDB uses 'id' as the default primary key.
    """
    table_info = r.db(database).table(table_name).info().run(conn)
    primary_key = table_info.get("primary_key", "id")
    return [primary_key]


@handle_rethinkdb_error(
    "detecting timestamp field",
    (ReqlOpFailedError, ReqlDriverError, Exception),
    raise_error=False,
    default_value=None,
)
def get_timestamp_field(conn, database: str, table_name: str):
    """
    Detect timestamp field for incremental sync.
    Looks for common timestamp field names in the table.
    Args:
        conn: RethinkDB connection object
        database: Name of the database
        table_name: Name of the table
    Returns:
        str or None: Name of timestamp field, or None if not found
    """
    # Get a sample record to check for timestamp fields
    sample = r.db(database).table(table_name).limit(1).run(conn)
    sample_record = list(sample)

    if not sample_record:
        return None

    record = sample_record[0]

    # Common timestamp field names (in priority order)
    timestamp_fields = [
        "updated_at",
        "modified_at",
        "timestamp",
        "created_at",
        "last_modified",
    ]

    for field in timestamp_fields:
        if field in record:
            log.info(f"Found timestamp field '{field}' for incremental sync in table {table_name}")
            return field

    log.info(f"No timestamp field found for table {table_name}, will perform full sync")
    return None


def transform_record(record: dict) -> dict:
    """
    Transform a RethinkDB record to ensure all values are compatible with Fivetran SDK.
    Converts lists and nested dictionaries to JSON strings.
    Args:
        record: Dictionary containing the record data
    Returns:
        dict: Transformed record with compatible data types
    """
    transformed = {}
    for key, value in record.items():
        if isinstance(value, list):
            # Convert lists to JSON strings
            transformed[key] = json.dumps(value) if value else None
        elif isinstance(value, dict):
            # Convert nested dictionaries to JSON strings
            transformed[key] = json.dumps(value) if value else None
        else:
            # Keep primitive types as-is
            transformed[key] = value
    return transformed


@handle_rethinkdb_error(
    "syncing table data", (ReqlOpFailedError, ReqlRuntimeError, ReqlDriverError, Exception)
)
def sync_table_data(conn, database: str, table_name: str, state: dict) -> int:
    """
    Sync data from a RethinkDB table to destination with incremental sync support.
    Uses timestamp fields when available for incremental updates, otherwise performs full sync.
    Args:
        conn: RethinkDB connection object
        database: Name of the database
        table_name: Name of the table to sync
        state: State dictionary to track sync progress
    Returns:
        int: Number of records processed from the table
    """
    # Check if this is initial sync or incremental sync
    last_sync_timestamp_key = f"{table_name}_last_sync_timestamp"
    last_sync_timestamp = state.get(last_sync_timestamp_key)

    # Detect timestamp field for incremental sync
    timestamp_field = get_timestamp_field(conn, database, table_name)

    if last_sync_timestamp and timestamp_field:
        log.info(
            f"Starting incremental sync for table: {table_name} (changes since {last_sync_timestamp})"
        )
    else:
        log.info(f"Starting full sync for table: {table_name}")

    records_processed = 0
    max_timestamp = last_sync_timestamp

    # Build query with timestamp filter for incremental sync
    query = r.db(database).table(table_name)

    if last_sync_timestamp and timestamp_field:
        # Filter for records updated since last sync
        query = query.filter(r.row[timestamp_field] > last_sync_timestamp)

    cursor = query.run(conn)

    for record in cursor:
        # Transform record to handle lists and nested dictionaries
        transformed_record = transform_record(record)

        # Track the maximum timestamp for next incremental sync
        if timestamp_field and timestamp_field in record:
            record_timestamp = record[timestamp_field]
            if max_timestamp is None or record_timestamp > max_timestamp:
                max_timestamp = record_timestamp

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=table_name, data=transformed_record)
        records_processed += 1

        if records_processed % __CHECKPOINT_INTERVAL == 0:
            # Update state with current progress
            if max_timestamp:
                state[last_sync_timestamp_key] = max_timestamp

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)
            log.info(
                f"Checkpointed after processing {records_processed} records from {table_name}"
            )

    cursor.close()

    # Update final timestamp for next incremental sync
    if max_timestamp:
        state[last_sync_timestamp_key] = max_timestamp

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)

    if records_processed == 0 and last_sync_timestamp:
        log.info(f"No new records found for table {table_name} since last sync")
    else:
        log.info(f"Completed sync for table {table_name}: {records_processed} records processed")

    return records_processed


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
        conn = connect_to_rethinkdb(configuration)
        database = configuration.get("database")
        tables = get_all_tables(conn, database)

        schema_definition = []
        for table_name in tables:
            primary_key = get_table_primary_key(conn, database, table_name)
            schema_definition.append({"table": table_name, "primary_key": primary_key})

        return schema_definition

    except Exception as e:
        log.severe(f"Error generating schema: {str(e)}")
        raise RuntimeError(f"Schema generation failed: {str(e)}")

    finally:
        if conn:
            conn.close()


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    log.warning("Example: Source Examples : RethinkDB")

    conn = None
    try:
        conn = connect_to_rethinkdb(configuration)
        database = configuration.get("database")
        tables = get_all_tables(conn, database)

        total_records = 0
        for table_name in tables:
            records_count = sync_table_data(conn, database, table_name, state)
            total_records += records_count

        log.info(
            f"RethinkDB sync completed successfully. Total records synced: {total_records} across {len(tables)} tables"
        )

    except (ReqlOpFailedError, ReqlDriverError, ReqlRuntimeError, ReqlAuthError) as e:
        log.severe(f"Error during sync: {str(e)}")
        raise RuntimeError(f"Failed to sync data from RethinkDB: {str(e)}")

    finally:
        if conn:
            conn.close()
            log.info("RethinkDB connection closed")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the
# command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by
# Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
