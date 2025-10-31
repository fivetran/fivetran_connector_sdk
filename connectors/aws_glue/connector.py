# This is a simple example for how to work with the fivetran_connector_sdk module.
# This is an example to show how we can sync records from AWS Athena via Connector SDK using boto3.
# You need to provide your credentials for this example to work.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

import boto3   # AWS SDK for Python; used to interact with AWS services like Glue and S3.
import json    # Provides methods to parse, serialize, and manipulate JSON data.
import duckdb  # In-process analytical database used here to query data directly from S3 or files.
import hashlib # Used to generate unique hashes (e.g., for row IDs or checksums).
from datetime import datetime, timezone  # Handles date and time operations with timezone awareness.
from typing import Dict, List, Optional, Any  # Provides type hinting support for dictionaries, lists, optional values, and generic types.

# Import required classes from fivetran_connector_sdk.
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__BATCH_SIZE = 10000  # Number of records to fetch per batch

def validate_configuration(configuration: Dict[str, Any]):
    """
    Validate the configuration dictionary for required fields.
    Args: param configuration: a dictionary containing the connection parameters.
    Raises:
        ValueError: If any required field is missing or invalid.
    """
    if not configuration.get('aws_access_key_id'):
        raise ValueError("aws_access_key_id is required in configuration")

    if not configuration.get('aws_secret_access_key'):
        raise ValueError("aws_secret_access_key is required in configuration")

    if not configuration.get('glue_database'):
        raise ValueError("glue_database is required in configuration")

# Define the schema function, which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Validate required configuration
    validate_configuration(configuration)

    aws_access_key = configuration['aws_access_key_id']
    aws_secret_key = configuration['aws_secret_access_key']
    aws_region = configuration.get('aws_region', 'us-east-1')
    glue_database = configuration['glue_database']
    schema_filter = configuration.get('schema_name', '')

    try:
        # Initialize Glue client
        glue_client = get_glue_client(aws_access_key, aws_secret_key, aws_region)

        # Get all tables from Glue catalog
        tables_metadata = get_glue_tables(glue_client, glue_database, schema_filter)
        log.info(f"Found {len(tables_metadata)} tables in Glue catalog")

        schema_response = []

        for table_meta in tables_metadata:
            table_name = table_meta['Name']
            full_table_name = f"{glue_database}.{table_name}"

            log.info(f"Processing table: {full_table_name}")

            # Get column metadata from Glue
            columns = parse_glue_columns(table_meta)

            if not columns:
                log.warning(f"No columns found for table: {full_table_name}")
                continue

            # Build table schema in correct format
            table_schema = {
                'table': full_table_name,
                'primary_key': get_primary_keys(columns),
                'columns': {col['name']: col['type'] for col in columns}
            }

            schema_response.append(table_schema)

        log.info(f"Schema discovery complete: {len(schema_response)} tables found")
        return schema_response

    except Exception as e:
        log.severe(f"Failed to fetch schema: {str(e)}")
        raise


def get_primary_keys(columns: List[Dict[str, Any]]) -> List[str]:
    """
    Extract primary key column names from column definitions
    Args: param columns: List of column definitions
    """
    primary_keys = [col['name'] for col in columns if col.get('primary_key', False)]

    # If no primary keys found, use first column or 'id' if exists
    if not primary_keys:
        col_names = [col['name'] for col in columns]
        if 'id' in col_names:
            primary_keys = ['id']
        elif col_names:
            primary_keys = [col_names[0]]

    return primary_keys

# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: a dictionary that contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary that contains whatever state you have chosen to checkpoint during the prior sync.
# The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    # Validate required configuration
    validate_configuration(configuration)

    aws_access_key = configuration['aws_access_key_id']
    aws_secret_key = configuration['aws_secret_access_key']
    aws_region = configuration.get('aws_region', 'us-east-1')
    glue_database = configuration['glue_database']
    schema_filter = configuration.get('schema_name', '')
    incremental_column = configuration.get('incremental_column', 'updated_at')
    batch_size = __BATCH_SIZE

    # Handle string conversions
    if isinstance(batch_size, str):
        batch_size = int(batch_size)

    try:
        # Initialize Glue client
        glue_client = get_glue_client(aws_access_key, aws_secret_key, aws_region)

        # Initialize DuckDB connection
        conn = get_duckdb_connection(aws_access_key, aws_secret_key, aws_region)

        # Get all tables from Glue catalog
        tables_metadata = get_glue_tables(glue_client, glue_database, schema_filter)
        log.info(f"Found {len(tables_metadata)} tables to sync")

        table_count = 0

        for table_meta in tables_metadata:
            table_name = table_meta['Name']
            full_table_name = f"{glue_database}.{table_name}"
            s3_location = table_meta['StorageDescriptor']['Location'] + "/data/"

            log.info(f"Syncing table: {full_table_name} from {s3_location}")

            # Get table state
            table_state = state.get(full_table_name, {})

            # Parse columns from Glue metadata
            columns = parse_glue_columns(table_meta)

            if not columns:
                log.warning(f"No columns found for table: {full_table_name}, skipping")
                continue

            log.info(f"Found {len(columns)} columns in {full_table_name}")

            # Find incremental column
            incremental_col = find_incremental_column(columns, incremental_column)

            if incremental_col:
                log.info(f"Using incremental sync for {full_table_name} with column: {incremental_col}")
                # Incremental sync
                yield from sync_incremental(
                    conn, full_table_name, s3_location, table_meta,
                    incremental_col, table_state, batch_size
                )
            else:
                log.info(f"Using reimport (full refresh) for {full_table_name} - no incremental column found")
                # Reimport table - full refresh
                yield from sync_reimport(
                    conn, full_table_name, s3_location, table_meta,
                    table_state, batch_size
                )

            table_count += 1

        conn.close()
        log.info(f"Sync completed successfully for {table_count} tables")

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise


def get_glue_client(aws_access_key: str, aws_secret_key: str, aws_region: str):
    """
    Create AWS Glue client
    Args: param configuration: a dictionary containing the connection parameters.
          param aws_access_key: AWS access key ID
          param aws_secret_key: AWS secret access key
          param aws_region: AWS region name
    Returns: glue_client: AWS Glue client object
    """
    log.info(f"Connecting to AWS Glue in region: {aws_region}")

    try:
        glue_client = boto3.client(
            'glue',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )

        # Test connection
        glue_client.get_databases(MaxResults=1)
        log.info("Successfully connected to AWS Glue")

        return glue_client

    except Exception as e:
        log.severe(f"Failed to connect to AWS Glue: {str(e)}")
        raise


def get_duckdb_connection(aws_access_key: str, aws_secret_key: str, aws_region: str):
    """
    Create DuckDB connection with AWS credentials
    Args: param configuration: a dictionary containing the connection parameters.
          param aws_access_key: AWS access key ID
          param aws_secret_key: AWS secret access key
          param aws_region: AWS region name
    """
    log.info("Initializing DuckDB with AWS extensions")

    try:
        conn = duckdb.connect(':memory:')

        # Install and load httpfs extension for S3 access
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")

        # Set AWS credentials
        conn.execute(f"SET s3_access_key_id='{aws_access_key}'")
        conn.execute(f"SET s3_secret_access_key='{aws_secret_key}'")
        conn.execute(f"SET s3_region='{aws_region}'")

        log.info("DuckDB configured for S3 access")
        return conn

    except Exception as e:
        log.severe(f"Failed to initialize DuckDB: {str(e)}")
        raise


def get_glue_tables(glue_client, database_name: str, schema_filter: str = '') -> List[Dict]:
    """
    Retrieve all tables from Glue catalog for a given database
    Args: param glue_client: AWS Glue client object
          param database_name: Name of the Glue database
          param schema_filter: Optional filter to limit tables by name prefix
    Returns: List of table metadata dictionaries
    """
    try:
        tables = []
        paginator = glue_client.get_paginator('get_tables')

        page_iterator = paginator.paginate(
            DatabaseName=database_name,
            MaxResults=100
        )

        for page in page_iterator:
            for table in page['TableList']:
                # Apply schema filter if provided (filter by table name prefix)
                if schema_filter and not table['Name'].startswith(schema_filter):
                    continue

                tables.append(table)

        log.info(f"Found {len(tables)} tables in database '{database_name}'")
        return tables

    except Exception as e:
        log.severe(f"Failed to get tables from Glue: {e}")
        raise


def parse_glue_columns(table_meta: Dict) -> List[Dict[str, Any]]:
    """
    Parse column information from Glue table metadata
    Args: param table_meta: The Glue table metadata dictionary
    """
    try:
        columns = []

        # Get columns from StorageDescriptor
        if 'StorageDescriptor' in table_meta and 'Columns' in table_meta['StorageDescriptor']:
            for col in table_meta['StorageDescriptor']['Columns']:
                col_name = col['Name']
                col_type = col['Type']

                fivetran_type = map_glue_type_to_fivetran(col_type)

                col_def = {
                    'name': col_name,
                    'type': fivetran_type
                }

                # Detect primary key columns by naming patterns
                if col_name.lower() in ['id', 'pk', 'primary_key', 'row_id'] or col_name.lower().endswith('_id'):
                    col_def['primary_key'] = True

                columns.append(col_def)

        # Also check for partition keys
        if 'PartitionKeys' in table_meta:
            for col in table_meta['PartitionKeys']:
                col_name = col['Name']
                col_type = col['Type']

                fivetran_type = map_glue_type_to_fivetran(col_type)

                col_def = {
                    'name': col_name,
                    'type': fivetran_type
                }

                columns.append(col_def)

        return columns

    except Exception as e:
        log.severe(f"Failed to parse columns: {e}")
        raise


def map_glue_type_to_fivetran(glue_type: str) -> str:
    """
    Map AWS Glue/Hive data types to Fivetran types
    Args: param glue_type: The Glue/Hive data type string
    """
    if not glue_type:
        return "STRING"

    glue_type_upper = glue_type.upper()

    # Handle complex types
    if 'ARRAY' in glue_type_upper or 'LIST' in glue_type_upper:
        return "JSON"

    if 'MAP' in glue_type_upper or 'STRUCT' in glue_type_upper:
        return "JSON"

    type_mapping = {
        'BIGINT': "LONG",
        'INT': "INT",
        'INTEGER': "INT",
        'SMALLINT': "INT",
        'TINYINT': "INT",
        'DOUBLE': "DOUBLE",
        'FLOAT': "DOUBLE",
        'DECIMAL': "DECIMAL",
        'STRING': "STRING",
        'VARCHAR': "STRING",
        'CHAR': "STRING",
        'BOOLEAN': "BOOLEAN",
        'DATE': "DATE",
        'TIMESTAMP': "UTC_DATETIME",
        'DATETIME': "UTC_DATETIME",
        'INTERVAL': "STRING"
    }

    # Check for partial matches
    for key, value in type_mapping.items():
        if key in glue_type_upper:
            return value

    return "STRING"


def find_incremental_column(columns: List[Dict[str, Any]],
                            config_incremental_col: str) -> Optional[str]:
    """
    Identify the incremental column from the list of columns
    Args: param columns: List of column definitions
          param config_incremental_col: Configured incremental column name
    """
    incremental_candidates = [
        config_incremental_col,
        'updated_at',
        'modified_at',
        'last_modified',
        'last_updated',
        'created_at',
        'timestamp',
        'date',
        'datetime',
        'insert_time',
        'update_time',
        'event_time'
    ]

    col_names = [col['name'].lower() for col in columns]
    col_name_map = {col['name'].lower(): col['name'] for col in columns}

    for candidate in incremental_candidates:
        if candidate.lower() in col_names:
            log.info(f"Found incremental column: {candidate}")
            return col_name_map[candidate.lower()]

    log.info("No incremental column found")
    return None


def sync_incremental(conn, full_table_name: str, s3_location: str, table_meta: Dict,
                     incremental_col: str, table_state: Dict[str, Any], batch_size: int):
    """
    Perform incremental sync for a table
    Args: param conn: DuckDB connection object
          param full_table_name: Full table name in the format database.table
          param s3_location: S3 location of the table data
          param table_meta: Glue table metadata
          param incremental_col: Name of the incremental column
          param table_state: State dictionary for the table
          param batch_size: Number of records to fetch per batch
    Returns: Yields upsert, delete, and checkpoint operations
    """
    last_value = table_state.get('last_incremental_value')
    last_checksums = table_state.get('checksums', {})

    log.info(f"Last incremental value for {full_table_name}: {last_value}")
    log.info(f"Previous sync had {len(last_checksums)} rows")

    try:
        # Determine file format from Glue metadata
        input_format = table_meta['StorageDescriptor'].get('InputFormat', '')
        serde_info = table_meta['StorageDescriptor'].get('SerdeInfo', {})

        # Build query based on format
        read_func = get_read_function(input_format, serde_info)

        # Build incremental query
        if last_value:
            query = f"""
                SELECT * 
                FROM {read_func}('{s3_location}*')
                WHERE {incremental_col} > '{last_value}'
                ORDER BY {incremental_col}
                LIMIT {batch_size}
            """
        else:
            query = f"""
                SELECT * 
                FROM {read_func}('{s3_location}*')
                ORDER BY {incremental_col}
                LIMIT {batch_size}
            """

        log.info(f"Executing incremental query for {full_table_name}")
        log.info(f"Query: {query}")

        result = conn.execute(query)
        column_names = [desc[0] for desc in result.description]
        rows = result.fetchall()

        log.info(f"Fetched {len(rows)} rows from {full_table_name}")

        if not rows:
            log.info(f"No new rows to sync for {full_table_name}")
            if table_state is None:
                table_state = {}
            if full_table_name not in table_state:
                table_state[full_table_name] = {}

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            yield op.checkpoint(state=table_state)
            return

        # Track new checksums for delete detection
        current_checksums = {}
        max_incremental_value = last_value
        upsert_count = 0

        for row in rows:
            # Convert row tuple to dict
            row_dict = dict(zip(column_names, row))

            # Convert datetime objects to ISO format strings
            row_dict = serialize_row(row_dict)

            # Calculate row checksum for delete detection
            row_id = get_row_id(row_dict)
            row_checksum = calculate_checksum(row_dict)
            current_checksums[row_id] = row_checksum

            # Check if row was modified
            if row_id not in last_checksums or last_checksums[row_id] != row_checksum:

                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted
                yield op.upsert(table=full_table_name, data=row_dict)
                upsert_count += 1

            # Track max incremental value
            inc_value = row_dict.get(incremental_col)
            if inc_value:
                inc_value_str = str(inc_value)
                if not max_incremental_value or inc_value_str > str(max_incremental_value):
                    max_incremental_value = inc_value_str

        log.info(f"Upserted {upsert_count} rows for {full_table_name}")

        # Detect deletes - rows in last_checksums but not in current_checksums
        deleted_ids = set(last_checksums.keys()) - set(current_checksums.keys())

        if deleted_ids:
            log.info(f"Detected {len(deleted_ids)} deletions in {full_table_name}")
            for deleted_id in deleted_ids:
                deleted_row = {'_row_id': deleted_id}
                yield op.delete(full_table_name, deleted_row)

        # Yield checkpoint to save state
        new_state = {
            full_table_name: {
                'last_incremental_value': max_incremental_value,
                'checksums': current_checksums,
                'last_sync': datetime.now(timezone.utc).isoformat()
            }
        }
        log.info(f"Checkpointing state for {full_table_name} with max value: {max_incremental_value}")
        if table_state is None:
            table_state = {}
        if full_table_name not in table_state:
            table_state[full_table_name] = {}
        table_state[full_table_name] = new_state[full_table_name]

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        yield op.checkpoint(state=table_state)

    except Exception as e:
        log.severe(f"Failed to sync {full_table_name}: {e}")
        raise


def sync_reimport(conn, full_table_name: str, s3_location: str, table_meta: Dict,
                  table_state: Dict[str, Any], batch_size: int):
    """
    Perform reimport (full refresh) for a table
    Args: param conn: DuckDB connection object
          param full_table_name: Full table name in the format database.table
          param s3_location: S3 location of the table data
          param table_meta: Glue table metadata
          param table_state: State dictionary for the table
          param batch_size: Number of records to fetch per batch
    Returns: Yields upsert, delete, and checkpoint operations
    """

    last_checksums = table_state.get('checksums', {})

    log.info(f"Previous sync had {len(last_checksums)} rows for {full_table_name}")

    try:
        # Determine file format from Glue metadata
        input_format = table_meta['StorageDescriptor'].get('InputFormat', '')
        serde_info = table_meta['StorageDescriptor'].get('SerdeInfo', {})

        # Build query based on format
        read_func = get_read_function(input_format, serde_info)

        # Build full scan query
        query = f"""
            SELECT * 
            FROM {read_func}('{s3_location}*')
            LIMIT {batch_size}
        """

        log.info(f"Executing full refresh query for {full_table_name}")
        log.info(f"Query: {query}")

        result = conn.execute(query)
        column_names = [desc[0] for desc in result.description]
        rows = result.fetchall()

        log.info(f"Fetched {len(rows)} rows from {full_table_name} (full refresh)")

        if not rows:
            log.info(f"No rows found in {full_table_name}")
            if table_state is None:
                table_state = {}
            if full_table_name not in table_state:
                table_state[full_table_name] = {}

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            yield op.checkpoint(state={full_table_name: table_state})
            return

        # Track checksums for delete detection
        current_checksums = {}
        upsert_count = 0

        for row in rows:
            # Convert row tuple to dict
            row_dict = dict(zip(column_names, row))

            # Convert datetime objects to ISO format strings
            row_dict = serialize_row(row_dict)

            row_id = get_row_id(row_dict)
            row_checksum = calculate_checksum(row_dict)
            current_checksums[row_id] = row_checksum

            # Check if row was modified
            if row_id not in last_checksums or last_checksums[row_id] != row_checksum:

                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted
                yield op.upsert(table=full_table_name, data=row_dict)
                upsert_count += 1

        log.info(f"Upserted {upsert_count} rows for {full_table_name}")

        # Detect deletes
        deleted_ids = set(last_checksums.keys()) - set(current_checksums.keys())

        if deleted_ids:
            log.info(f"Detected {len(deleted_ids)} deletions in {full_table_name}")
            for deleted_id in deleted_ids:
                deleted_row = {'_row_id': deleted_id}
                yield op.delete(full_table_name, deleted_row)

        # Yield checkpoint to save state
        new_state = {
            full_table_name: {
                'checksums': current_checksums,
                'last_sync': datetime.now(timezone.utc).isoformat()
            }
        }
        log.info(f"Checkpointing state for {full_table_name}")
        if table_state is None:
            table_state = {}
        if full_table_name not in table_state:
            table_state[full_table_name] = {}
        table_state[full_table_name] = new_state[full_table_name]

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        yield op.checkpoint(state=table_state)

    except Exception as e:
        log.severe(f"Failed to sync {full_table_name}: {e}")
        raise


def get_read_function(input_format: str, serde_info: Dict) -> str:
    """
    Determine the appropriate DuckDB read function based on file format
    Args: param input_format: The input format string from Glue metadata
          param serde_info: The SerDe info dictionary from Glue metadata
    Returns: The DuckDB read function name
    """
    input_format_lower = input_format.lower()
    serde_lib = serde_info.get('SerializationLibrary', '').lower()

    # Parquet
    if 'parquet' in input_format_lower or 'parquet' in serde_lib:
        return 'read_parquet'

    # CSV
    if 'csv' in input_format_lower or 'csv' in serde_lib or 'opencsv' in serde_lib:
        return 'read_csv_auto'

    # JSON
    if 'json' in input_format_lower or 'json' in serde_lib:
        return 'read_json_auto'

    # ORC
    if 'orc' in input_format_lower or 'orc' in serde_lib:
        log.warning("ORC format detected - DuckDB support may be limited")
        return 'read_parquet'  # Fallback to parquet reader

    # Avro
    if 'avro' in input_format_lower or 'avro' in serde_lib:
        log.warning("Avro format detected - not natively supported by DuckDB")
        raise ValueError("Avro format is not supported. Please convert to Parquet, CSV, or JSON")

    # Default to Parquet (most common in AWS Glue)
    log.warning(f"Unknown format: {input_format}, defaulting to Parquet")
    return 'read_parquet'


def serialize_row(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Serialize row dictionary to handle non-serializable types
    Args: param row_dict: The row dictionary to serialize
    """
    serialized = {}
    for key, value in row_dict.items():
        if value is None:
            serialized[key] = None
        elif isinstance(value, datetime):
            serialized[key] = value.isoformat()
        elif isinstance(value, (list, dict)):
            serialized[key] = json.dumps(value)
        elif isinstance(value, bytes):
            serialized[key] = value.hex()
        elif isinstance(value, (int, float, str, bool)):
            serialized[key] = value
        else:
            # For any other type, convert to string
            serialized[key] = str(value)
    return serialized


def get_row_id(row: Dict[str, Any]) -> str:
    """
    Generate unique row identifier
    Args: param row: The row dictionary
    Returns: A unique string identifier for the row
    """
    # Use hash of all columns as unique ID
    row_str = json.dumps(row, sort_keys=True, default=str)
    return hashlib.md5(row_str.encode()).hexdigest()


def calculate_checksum(row: Dict[str, Any]) -> str:
    """
    Calculate checksum for row to detect changes
    Args: param row: The row dictionary
    Returns: A checksum string
    """
    row_str = json.dumps(row, sort_keys=True, default=str)
    return hashlib.md5(row_str.encode()).hexdigest()


# This creates the connector object that will use the update function defined in this connector.py file.
# This example does not use the schema() function. If it did, it would need to be included in the connector object definition.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Test it by using the `debug` command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug(configuration=configuration)



# Fivetran debug results:
# Operation       | Calls
# ----------------+------------
# Upserts         | 20206
# Updates         | 0
# Deletes         | 0
# Truncates       | 0
# SchemaChanges   | 4
# Checkpoints     | 4