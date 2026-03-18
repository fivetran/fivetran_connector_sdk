# This is an example for how to work with the fivetran_connector_sdk module.
# This shows how to fetch data from Sybase ASE and upsert it to destination using FreeTDS driver and pyodbc.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import required libraries
import pyodbc  # For connecting to Sybase ASE using FreeTDS
import json
import logging

# Safe logging wrapper for standalone testing
def safe_log(level, message):
    """Safely log messages - works in both Fivetran and standalone mode"""
    try:
        if level == "info":
            log.info(message)
        elif level == "warning":
            log.warning(message)
        elif level == "error":
            log.error(message)
    except (TypeError, AttributeError):
        # Fallback to standard logging if Fivetran logging not initialized
        logger = logging.getLogger(__name__)
        if level == "info":
            logger.info(message)
        elif level == "warning":
            logger.warning(message)
        elif level == "error":
            logger.error(message)


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary for required fields.
    Args:
        configuration (dict): A dictionary containing the connection parameters.
    Raises:
        ValueError: If any required field is missing or invalid.
    """
    required_fields = ["server", "port", "database", "user_id", "password"]
    for field in required_fields:
        if field not in configuration or not configuration[field]:
            raise ValueError(f"Missing required configuration field: {field}")
    safe_log("info", "Configuration validation passed.")


def get_sybase_tables(connection, database: str):
    """
    Retrieve all user tables from the Sybase ASE database.
    Args:
        connection: A connection object to the Sybase ASE database.
        database (str): The database name to query.
    Returns:
        list: A list of table names in the database.
    """
    cursor = connection.cursor()
    try:
        # Query to get all user tables from sysobjects
        query = """
        SELECT name 
        FROM sysobjects 
        WHERE type = 'U' 
        ORDER BY name
        """
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]
        safe_log("info", f"Found {len(tables)} tables in database '{database}'")
        return tables
    except Exception as e:
        safe_log("warning", f"Error fetching tables: {str(e)}")
        return []
    finally:
        cursor.close()


def get_table_primary_keys(connection, table_name: str):
    """
    Retrieve primary key columns for a specific table.
    Args:
        connection: A connection object to the Sybase ASE database.
        table_name (str): The name of the table.
    Returns:
        list: A list of primary key column names.
    """
    cursor = connection.cursor()
    try:
        # Use sp_helpindex to get index information
        # This returns indexes with their columns and properties
        cursor.execute(f"EXEC sp_helpindex '{table_name}'")
        
        # Check if there are any results
        if not cursor.description:
            # No indexes found for this table
            return []
        
        rows = cursor.fetchall()
        
        # Look for clustered unique index (primary key)
        # Column 2 contains index description (e.g., "clustered, unique")
        # Column 1 contains column names (e.g., "stor_id, ord_num")
        for row in rows:
            index_desc = row[2].lower() if len(row) > 2 else ""
            if "clustered" in index_desc and "unique" in index_desc:
                # This is the primary key - parse column names
                column_str = row[1].strip() if len(row) > 1 else ""
                pk_columns = [col.strip() for col in column_str.split(',')]
                return pk_columns
        
        # If no clustered unique index found, look for any unique index
        for row in rows:
            index_desc = row[2].lower() if len(row) > 2 else ""
            if "unique" in index_desc:
                column_str = row[1].strip() if len(row) > 1 else ""
                pk_columns = [col.strip() for col in column_str.split(',')]
                return pk_columns
        
        return []
    except pyodbc.Error as e:
        # Handle specific case where table has no indexes
        if "No indexes" in str(e) or "not found" in str(e).lower():
            return []
        safe_log("warning", f"Error fetching primary keys for table '{table_name}': {str(e)}")
        return []
    except Exception as e:
        safe_log("warning", f"Error fetching primary keys for table '{table_name}': {str(e)}")
        return []
    finally:
        cursor.close()


def get_table_columns(connection, table_name: str):
    """
    Retrieve column information for a specific table.
    Args:
        connection: A connection object to the Sybase ASE database.
        table_name (str): The name of the table.
    Returns:
        dict: A dictionary mapping column names to Fivetran data types.
    """
    cursor = connection.cursor()
    try:
        # Query to get column information from syscolumns
        query = f"""
        SELECT c.name, t.name as type_name, c.length, c.prec, c.scale
        FROM syscolumns c
        JOIN systypes t ON c.usertype = t.usertype
        JOIN sysobjects o ON c.id = o.id
        WHERE o.name = '{table_name}'
        ORDER BY c.colid
        """
        cursor.execute(query)
        columns = {}
        for row in cursor.fetchall():
            col_name = row[0]
            col_type = row[1]
            col_length = row[2]
            col_precision = row[3]
            col_scale = row[4]
            
            # Map Sybase types to Fivetran types
            fivetran_type = map_sybase_to_fivetran_type(col_type, col_precision, col_scale)
            columns[col_name] = fivetran_type
        return columns
    except Exception as e:
        safe_log("warning", f"Error fetching columns for table '{table_name}': {str(e)}")
        return {}
    finally:
        cursor.close()


def map_sybase_to_fivetran_type(sybase_type: str, precision: int = None, scale: int = None):
    """
    Map Sybase ASE data types to Fivetran data types.
    Args:
        sybase_type (str): The Sybase data type.
        precision (int): The precision for numeric types.
        scale (int): The scale for numeric types.
    Returns:
        str or dict: The corresponding Fivetran data type (string or dict for DECIMAL).
    """
    sybase_type_lower = sybase_type.lower()
    
    # Handle DECIMAL/NUMERIC types with precision and scale
    if sybase_type_lower in ("decimal", "numeric", "money", "smallmoney"):
        # Default precision and scale if not provided
        if precision is None or precision == 0:
            precision = 18
        if scale is None:
            scale = 0
        
        return {
            "type": "DECIMAL",
            "precision": precision,
            "scale": scale
        }
    
    # Map other types to simple strings
    type_mapping = {
        # String types
        "char": "STRING",
        "varchar": "STRING",
        "nchar": "STRING",
        "nvarchar": "STRING",
        "text": "STRING",
        "unichar": "STRING",
        "univarchar": "STRING",
        # Numeric types
        "int": "INT",
        "integer": "INT",
        "smallint": "SHORT",
        "tinyint": "SHORT",
        "bigint": "LONG",
        "float": "DOUBLE",
        "real": "FLOAT",
        # Date/Time types
        "date": "NAIVE_DATE",
        "time": "NAIVE_TIME",
        "datetime": "NAIVE_DATETIME",
        "smalldatetime": "NAIVE_DATETIME",
        "bigdatetime": "NAIVE_DATETIME",
        "bigtime": "NAIVE_TIME",
        # Binary types
        "binary": "BINARY",
        "varbinary": "BINARY",
        "image": "BINARY",
        # Boolean
        "bit": "BOOLEAN",
    }
    
    # Return mapped type or default to STRING
    return type_mapping.get(sybase_type_lower, "STRING")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    This function dynamically discovers all tables in the database and allows table selection.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    # Validate configuration first
    validate_configuration(configuration)
    
    # Create connection to discover schema
    connection = create_sybase_connection(configuration)
    database = configuration.get("database")
    
    try:
        # Get all tables from the database
        all_tables = get_sybase_tables(connection, database)
        
        # Check if user has specified tables to sync
        # If 'tables' is provided in configuration, only sync those tables
        # Otherwise, sync all tables
        selected_tables = configuration.get("tables", all_tables)
        
        # If selected_tables is a string (comma-separated), convert to list
        if isinstance(selected_tables, str):
            selected_tables = [t.strip() for t in selected_tables.split(",")]
        
        # Build schema for selected tables
        schema_list = []
        for table_name in selected_tables:
            if table_name not in all_tables:
                safe_log("warning", f"Table '{table_name}' not found in database. Skipping.")
                continue
            
            # Get primary keys for the table
            primary_keys = get_table_primary_keys(connection, table_name)
            
            # Get columns for the table
            columns = get_table_columns(connection, table_name)
            
            table_schema = {
                "table": table_name,
                "columns": columns,
            }
            
            # Only add primary_key if we found any
            if primary_keys:
                table_schema["primary_key"] = primary_keys
            
            schema_list.append(table_schema)
            safe_log("info", f"Added table '{table_name}' to schema with {len(columns)} columns")
        
        safe_log("info", f"Schema discovery complete. Syncing {len(schema_list)} tables.")
        return schema_list
        
    except Exception as e:
        safe_log("warning", f"Error during schema discovery: {str(e)}")
        raise
    finally:
        connection.close()


def create_sybase_connection(configuration: dict):
    """
    Create a connection to the Sybase ASE database using FreeTDS.
    This function reads the connection parameters from the provided configuration dictionary.
    Args:
        configuration (dict): A dictionary containing the connection parameters.
    Returns:
        connection: A connection object to the Sybase ASE database.
    """
    server = configuration.get("server")
    port = int(configuration.get("port"))
    database = configuration.get("database")
    user_id = configuration.get("user_id")
    password = configuration.get("password")

    try:
        connection_str = (
            "DRIVER=FreeTDS;"
            f"SERVER={server};"
            f"PORT={port};"
            f"DATABASE={database};"
            f"UID={user_id};"
            f"PWD={password};"
            "TDS_Version=5.0;"
            "ClientCharset=UTF-8;"
            "Timeout=30;"
            "LoginTimeout=30"
        )

        connection = pyodbc.connect(connection_str, autocommit=True)
        safe_log("info", "Connection to Sybase ASE established successfully.")
        return connection
    except Exception as e:
        raise RuntimeError("Connection to Sybase ASE failed") from e


def close_sybase_connection(connection, cursor):
    """
    Close the connection to the Sybase ASE database.
    Args:
        connection: A connection object to the Sybase ASE database.
        cursor: A cursor object (optional).
    """
    if cursor:
        cursor.close()
        safe_log("info", "Cursor closed successfully.")
    if connection:
        connection.close()
        safe_log("info", "Connection to Sybase ASE closed successfully.")


def get_table_incremental_column(connection, table_name: str):
    """
    Determine the best column to use for incremental sync.
    Looks for datetime columns or falls back to the first primary key column.
    Args:
        connection: A connection object to the Sybase ASE database.
        table_name (str): The name of the table.
    Returns:
        tuple: (column_name, column_type) or (None, None) if no suitable column found.
    """
    cursor = connection.cursor()
    try:
        # First, try to find datetime columns
        query = f"""
        SELECT c.name, t.name as type_name
        FROM syscolumns c
        JOIN systypes t ON c.usertype = t.usertype
        JOIN sysobjects o ON c.id = o.id
        WHERE o.name = '{table_name}'
        AND t.name IN ('datetime', 'smalldatetime', 'bigdatetime', 'date')
        ORDER BY c.colid
        """
        cursor.execute(query)
        result = cursor.fetchone()
        if result:
            return result[0], result[1]
        
        # If no datetime column, try to use first primary key
        primary_keys = get_table_primary_keys(connection, table_name)
        if primary_keys:
            return primary_keys[0], None
        
        return None, None
    except Exception as e:
        safe_log("warning", f"Error determining incremental column for '{table_name}': {str(e)}")
        return None, None
    finally:
        cursor.close()


def fetch_and_upsert(cursor, query, table_name: str, state: dict, incremental_column: str = None, batch_size: int = 1000):
    """
    Fetch data from the Sybase ASE database and upsert it into the destination table.
    This function executes the provided SQL query, fetches data in batches, and performs upsert operations.
    It also updates the state with the last processed row based on the incremental column.
    Args:
        cursor: A cursor object to the Sybase ASE database.
        query (str): The SQL query to execute for fetching data.
        table_name (str): The name of the destination table for upserting data.
        state (dict): A dictionary containing state information from previous runs.
        incremental_column (str): The column name to use for tracking progress (optional).
        batch_size (int): The number of rows to fetch in each batch.
    """
    # Get the state key for this table
    state_key = f"{table_name}_last_value"
    
    # Execute the SQL query to fetch data from the Sybase ASE database
    cursor.execute(query)
    # Fetch the column names from the cursor description
    # This is necessary to map the data to the correct columns in the upsert operation
    column_names = [col[0] for col in cursor.description]
    
    row_count = 0
    last_value = state.get(state_key)

    while True:
        # Fetch data in batches to handle large datasets efficiently
        # This ensures that entire data is not loaded into memory at once and makes it memory efficient
        results = cursor.fetchmany(batch_size)
        if not results:
            # No more data to fetch, exit the loop
            break

        for row in results:
            # Convert the row tuple to a dictionary using the column names
            row_data = dict(zip(column_names, row))
            
            # Convert special data types for Fivetran compatibility
            for key, value in row_data.items():
                if value is not None:
                    # Convert Decimal to string for Fivetran DECIMAL type
                    if hasattr(value, '__class__') and value.__class__.__name__ == 'Decimal':
                        row_data[key] = str(value)
                    # Convert datetime to ISO format string
                    elif hasattr(value, 'isoformat'):
                        row_data[key] = value.isoformat()
                    # Convert bytes to ensure proper binary handling
                    elif isinstance(value, bytes):
                        row_data[key] = value
            
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert(table=table_name, data=row_data)
            row_count += 1

            # Update the last value if incremental column is specified
            if incremental_column and incremental_column in row_data and row_data[incremental_column]:
                current_value = row_data[incremental_column]
                # Value is already converted to string format above
                if last_value is None or current_value > last_value:
                    last_value = current_value

        # Update the state with the last value after processing each batch
        if last_value is not None:
            state[state_key] = last_value
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

    # After processing all rows, update the state with the last value and checkpoint it.
    # this ensures that the next run will start from the correct position
    if last_value is not None:
        state[state_key] = last_value
    op.checkpoint(state)
    
    safe_log("info", f"Synced {row_count} rows from table '{table_name}'")


def sync_table(connection, table_name: str, state: dict):
    """
    Sync a single table from Sybase ASE to the destination.
    Args:
        connection: A connection object to the Sybase ASE database.
        table_name (str): The name of the table to sync.
        state (dict): A dictionary containing state information from previous runs.
    """
    cursor = connection.cursor()
    
    try:
        # Determine the incremental column for this table
        incremental_column, col_type = get_table_incremental_column(connection, table_name)
        
        # Get the state key for this table
        state_key = f"{table_name}_last_value"
        last_value = state.get(state_key, "1970-01-01T00:00:00" if col_type else None)
        
        # Build the query based on whether we have an incremental column
        if incremental_column and last_value:
            query = f"SELECT * FROM {table_name} WHERE {incremental_column} > '{last_value}' ORDER BY {incremental_column}"
            safe_log("info", f"Syncing table '{table_name}' incrementally using column '{incremental_column}'")
        else:
            query = f"SELECT * FROM {table_name}"
            safe_log("info", f"Syncing table '{table_name}' (full sync)")
        
        # Fetch and upsert data
        fetch_and_upsert(
            cursor=cursor,
            query=query,
            table_name=table_name,
            state=state,
            incremental_column=incremental_column,
            batch_size=1000
        )
        
    except Exception as e:
        safe_log("warning", f"Error syncing table '{table_name}': {str(e)}")
        raise
    finally:
        cursor.close()


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
    connection = None
    
    try:
        safe_log("info", "Sybase ASE Connector - Multi-Table Sync - Starting")

        # Validate the configuration
        safe_log("info", "Validating configuration...")
        validate_configuration(configuration=configuration)

        # Create a connection to the Sybase ASE database using the provided configuration
        safe_log("info", "Creating database connection...")
        connection = create_sybase_connection(configuration=configuration)
        database = configuration.get("database")
        
        # Get all tables from the database
        safe_log("info", "Discovering tables...")
        all_tables = get_sybase_tables(connection, database)
        
        # Check if user has specified tables to sync
        selected_tables = configuration.get("tables", all_tables)
        
        # If selected_tables is a string (comma-separated), convert to list
        if isinstance(selected_tables, str):
            selected_tables = [t.strip() for t in selected_tables.split(",")]
        
        safe_log("info", f"Starting sync for {len(selected_tables)} tables: {', '.join(selected_tables)}")
        
        # Sync each selected table
        for table_name in selected_tables:
            if table_name not in all_tables:
                safe_log("warning", f"Table '{table_name}' not found in database. Skipping.")
                continue
            
            safe_log("info", f"Syncing table: {table_name}")
            sync_table(connection, table_name, state)
            safe_log("info", f"Completed syncing table: {table_name}")
        
        safe_log("info", "Sync completed successfully")
        
    except ValueError as e:
        safe_log("error", f"Configuration error: {str(e)}")
        raise
    except pyodbc.Error as e:
        safe_log("error", f"Database error: {str(e)}")
        raise
    except Exception as e:
        safe_log("error", f"Unexpected error during sync: {str(e)}")
        import traceback
        safe_log("error", f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        # Close the connection to the Sybase ASE database
        if connection:
            try:
                close_sybase_connection(connection=connection, cursor=None)
            except Exception as e:
                safe_log("warning", f"Error closing connection: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    try:
        # Open the configuration.json file and load its contents
        with open("/configuration.json", "r") as f:
            configuration = json.load(f)
    except FileNotFoundError:
        safe_log("info", "Using empty configuration!")
        configuration = {}

    # Test the connector locally
    connector.debug(configuration=configuration)