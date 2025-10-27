"""
TiDB connector for Fivetran.

This connector enables incremental data synchronization from TiDB databases,
including support for vector embeddings stored as JSON columns.
"""

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For type hints
from typing import Dict, List, Any, Optional

# For reading configuration from a JSON file
import json

# CA bundle used for TLS connections (used by TiDB DSN)
import certifi

# TiDB client used to connect/query the TiDB cluster
from pytidb import TiDBClient

# for timestamp parsing/normalization
from datetime import datetime, timezone



# Module-level constants
TIDB_CONNECTION_KEYS = ["TIDB_HOST", "TIDB_USER", "TIDB_PASS", "TIDB_PORT", "TIDB_DATABASE"]
REQUIRED_CONFIG_KEYS = TIDB_CONNECTION_KEYS + ["TABLES_PRIMARY_KEY_COLUMNS"]
MAX_UPSERT_RETRIES = 3
FALLBACK_TIMESTAMP = datetime(1990, 1, 1, tzinfo=timezone.utc)


def validate_configuration(configuration: Dict[str, Any]):
    """
    Validate that required configuration keys are present.
    
    Args:
        configuration: Dictionary containing connector configuration
        
    Raises:
        ValueError: If any required configuration key is missing
    """
    missing = [k for k in REQUIRED_CONFIG_KEYS if not configuration.get(k)]
    if missing:
        raise ValueError(f"Missing required configuration keys: {', '.join(missing)}")


def parse_json_config(configuration: Dict[str, Any], key: str) -> Dict[str, Any]:
    """
    Parse a JSON string from configuration.
    
    Args:
        configuration: Dictionary containing connector configuration
        key: Configuration key to parse
        
    Returns:
        Parsed dictionary from JSON string
        
    Raises:
        ValueError: If JSON parsing fails
    """
    if key not in configuration:
        raise ValueError(f"Could not find '{key}' in configuration")
    
    try:
        return json.loads(configuration[key])
    except Exception as e:
        raise ValueError(f"Failed to parse {key} JSON") from e


def build_schema_entry(table_name: str, primary_key_column: str) -> Dict[str, Any]:
    """
    Build a schema entry for a regular table.
    
    Args:
        table_name: Name of the table
        primary_key_column: Primary key column name
        
    Returns:
        Schema dictionary with table name and primary key
    """
    return {
        "table": table_name,
        "primary_key": [primary_key_column]
    }


def build_vector_schema_entry(table_name: str, table_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Build a schema entry for a vector table with JSON typed column.
    
    Args:
        table_name: Name of the vector table
        table_data: Dictionary containing primary_key_column and vector_column
        
    Returns:
        Schema dictionary with table, primary key, and typed columns, or None if invalid
    """
    pk = table_data.get("primary_key_column")
    vector_col = table_data.get("vector_column")
    
    if not pk or not vector_col:
        log.info("Skipping vector table '%s' due to missing keys", table_name)
        return None
    
    return {
        "table": table_name,
        "primary_key": [pk],
        "columns": {vector_col: "JSON"}
    }


def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Declare the destination schema expected by Fivetran.
    
    Args:
        configuration: Dictionary containing connector configuration
        
    Returns:
        List of table schema definitions with table name and primary keys
    """
    tables_and_primary_key_columns = parse_json_config(configuration, "TABLES_PRIMARY_KEY_COLUMNS")
    
    schema_list = []
    for table_name, primary_key_column in tables_and_primary_key_columns.items():
        schema_list.append(build_schema_entry(table_name, primary_key_column))
    
    # Add vector tables if configured
    if configuration.get("VECTOR_TABLES_DATA"):
        try:
            vector_tables_data = json.loads(configuration["VECTOR_TABLES_DATA"])
            for table_name, table_data in vector_tables_data.items():
                entry = build_vector_schema_entry(table_name, table_data)
                if entry:
                    schema_list.append(entry)
        except Exception:
            log.info("Failed to parse VECTOR_TABLES_DATA; ignoring vector table configuration.")
    
    return schema_list


def parse_embedding_string_to_list(s: Optional[str]) -> Optional[List[float]]:
    """
    Parse an embedding string into a list of floats.
    
    Args:
        s: Embedding string in JSON or bracketed CSV format
        
    Returns:
        List of float values, or None if parsing fails
    """
    if s is None:
        return None
    
    # Try JSON parsing first
    try:
        parsed = json.loads(s)
        if isinstance(parsed, list):
            return [float(x) for x in parsed]
    except Exception:
        pass
    
    # Fallback parse for bracketed CSV format
    try:
        ss = s.strip()
        if ss.startswith("[") and ss.endswith("]"):
            inner = ss[1:-1].strip()
        else:
            inner = ss
        
        if inner == "":
            return []
        
        parts = [p.strip().strip('"').strip("'") for p in inner.split(",") if p.strip() != ""]
        out = []
        for p in parts:
            try:
                out.append(float(p))
            except Exception:
                log.info("Failed to parse embedding element '%s' in '%s'", p, s)
                return None
        return out
    except Exception:
        return None


def parse_state_timestamp(timestamp_str: Optional[str]) -> datetime:
    """
    Parse a timestamp string from state.
    
    Args:
        timestamp_str: ISO-format timestamp string
        
    Returns:
        Timezone-aware datetime object, or fallback datetime if parsing fails
    """
    if not timestamp_str:
        return FALLBACK_TIMESTAMP
    
    try:
        parsed = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        log.info("Failed to parse state timestamp '%s', using fallback.", timestamp_str)
        return FALLBACK_TIMESTAMP


def normalize_timestamp_field(row_data: Dict[str, Any], field_name: str, table_name: str):
    """
    Normalize a timestamp field to timezone-aware datetime.
    
    Args:
        row_data: Dictionary containing row data
        field_name: Name of the timestamp field
        table_name: Name of the table (for logging)
    """
    if field_name in row_data and row_data[field_name] is not None:
        val = row_data[field_name]
        
        # Handle datetime objects
        if hasattr(val, "tzinfo"):
            if val.tzinfo is None:
                row_data[field_name] = val.replace(tzinfo=timezone.utc)
        else:
            # Handle string timestamps
            try:
                parsed = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                row_data[field_name] = parsed
            except Exception:
                log.fine("Could not parse %s value for table %s: %s", field_name, table_name, val)


def parse_vector_column(row_data: Dict[str, Any], table_name: str, configuration: Dict[str, Any]):
    """
    Parse vector embedding column to list format.
    
    Args:
        row_data: Dictionary containing row data
        table_name: Name of the table
        configuration: Dictionary containing connector configuration
    """
    if not configuration.get("VECTOR_TABLES_DATA"):
        return
    
    try:
        vector_tables = json.loads(configuration["VECTOR_TABLES_DATA"])
        if table_name in vector_tables:
            embedding_column = vector_tables[table_name]["vector_column"]
            raw_embeddings = row_data.get(embedding_column)
            emb_list = parse_embedding_string_to_list(raw_embeddings)
            if emb_list is not None:
                row_data[embedding_column] = emb_list
    except Exception:
        log.fine("Skipping vector parse for table %s due to malformed VECTOR_TABLES_DATA", table_name)


def process_row(row_data: Dict[str, Any], table_name: str, configuration: Dict[str, Any], is_vector_table: bool) -> Dict[str, Any]:
    """
    Normalize row values before upsert.
    
    Args:
        row_data: Dictionary containing row data
        table_name: Name of the table
        configuration: Dictionary containing connector configuration
        is_vector_table: Whether this is a vector table
        
    Returns:
        Processed row data dictionary
    """
    # Normalize timestamp fields
    normalize_timestamp_field(row_data, "created_at", table_name)
    normalize_timestamp_field(row_data, "updated_at", table_name)
    
    # Parse vector columns if applicable
    if is_vector_table:
        parse_vector_column(row_data, table_name, configuration)
    
    return row_data


def escape_table_name(table_name: str) -> str:
    """
    Escape table name for SQL query to prevent injection.
    
    Args:
        table_name: Raw table name
        
    Returns:
        Escaped table name wrapped in backticks
    """
    # Remove any existing backticks and wrap in backticks
    clean_name = table_name.replace("`", "")
    return f"`{clean_name}`"


def build_incremental_query(table_name: str, last_created: str) -> str:
    """
    Build SQL query for incremental data fetch.
    
    Args:
        table_name: Name of the table to query
        last_created: Last processed timestamp in ISO format
        
    Returns:
        Tuple of (query string with named placeholders, params dict)
        
    Raises:
        ValueError: If query building fails
    """
    try:
        # Convert ISO timestamp to TiDB format
        tidb_timestamp = last_created.replace("T", " ").replace("Z", "")
        escaped_table = escape_table_name(table_name)
        query = f"SELECT * FROM {escaped_table} WHERE created_at > :last_created ORDER BY created_at"
        params = {"last_created": tidb_timestamp}
        return query, params
    except Exception as e:
        raise ValueError(f"Failed to build query for table {table_name}") from e


def execute_query(cursor: TiDBClient, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Execute a query and return results as list of dictionaries.
    
    Args:
        cursor: TiDB client connection
        query: SQL query to execute
        params: Optional dict of params to bind to the query
        
    Returns:
        List of row dictionaries
        
    Raises:
        Exception: If query execution fails
    """
    try:
        query_result = cursor.query(query, params)
    except Exception:
        # Re-raise the exception so caller can handle logging / checkpointing
        raise
    
    # Handle different result types
    if hasattr(query_result, "to_list"):
        return query_result.to_list()
    else:
        return list(query_result)


def attempt_upsert_with_retry(table_name: str, row_data: Dict[str, Any], row_id: Any) -> bool:
    """
    Attempt to upsert a row with retry logic.
    
    Args:
        table_name: Name of the table
        row_data: Dictionary containing row data
        row_id: Row identifier for logging
        
    Returns:
        True if upsert succeeded, False otherwise
    """
    # Retry upsert operation up to MAX_UPSERT_RETRIES times
    for attempt in range(1, MAX_UPSERT_RETRIES + 1):
        try:
            op.upsert(table=table_name, data=row_data)
            return True
        except Exception as upp_err:
            log.info("Upsert failed for table %s row %s (attempt %d): %s", table_name, row_id, attempt, upp_err)
            if attempt >= MAX_UPSERT_RETRIES:
                log.severe("Giving up on upsert for table %s row %s after %d attempts", table_name, row_id, MAX_UPSERT_RETRIES)
    
    return False


def extract_row_timestamp(row_data: Dict[str, Any]) -> Optional[datetime]:
    """
    Extract and parse created_at timestamp from row.
    
    Args:
        row_data: Dictionary containing row data
        
    Returns:
        Parsed datetime object or None if extraction fails
    """
    created_val = row_data.get("created_at")
    
    if not created_val:
        return None
    
    # Handle datetime objects
    if hasattr(created_val, "tzinfo"):
        if created_val.tzinfo is None:
            return created_val.replace(tzinfo=timezone.utc)
        return created_val
    
    # Handle string timestamps
    try:
        return parse_state_timestamp(str(created_val))
    except Exception:
        return None


def process_and_upsert_rows(rows: List[Dict[str, Any]], table_name: str, state: Dict[str, Any], 
                            configuration: Dict[str, Any], is_vector_table: bool, 
                            last_created_timestamp: datetime) -> datetime:
    """
    Process and upsert rows, tracking the maximum timestamp.
    
    Args:
        rows: List of row dictionaries to process
        table_name: Name of the table
        state: State dictionary for checkpointing
        configuration: Dictionary containing connector configuration
        is_vector_table: Whether this is a vector table
        last_created_timestamp: Current last processed timestamp
        
    Returns:
        Maximum timestamp seen across all rows
    """
    max_seen_timestamp = last_created_timestamp
    
    for row in rows:
        try:
            row_data = process_row(row, table_name, configuration, is_vector_table)
            row_id = row.get("id", "<no-id>")
            
            # Attempt upsert with retry logic
            upsert_success = attempt_upsert_with_retry(table_name, row_data, row_id)
            
            # Update max timestamp if upsert succeeded
            if upsert_success:
                row_timestamp = extract_row_timestamp(row_data)
                if row_timestamp and row_timestamp > max_seen_timestamp:
                    max_seen_timestamp = row_timestamp
        
        except Exception as row_err:
            # Log row-level errors and continue processing other rows
            log.severe("Error processing row for table %s: %s", table_name, row_err)
            
            # Store sample of problematic row for debugging
            try:
                sample_key = f"{table_name}_last_row_error_sample"
                state[sample_key] = json.dumps({k: str(v) for k, v in (list(row.items())[:10])})
            except Exception:
                pass
    
    return max_seen_timestamp


def update_state_timestamp(state: Dict[str, Any], table_name: str, timestamp: datetime):
    """
    Update state with the last processed timestamp.
    
    Args:
        state: State dictionary
        table_name: Name of the table
        timestamp: Datetime to store in state
    """
    try:
        state[f"{table_name}_last_created"] = timestamp.isoformat()
    except Exception:
        # Fallback to string representation if isoformat fails
        state[f"{table_name}_last_created"] = str(timestamp)


def fetch_and_upsert_data(cursor: TiDBClient, table_name: str, state: Dict[str, Any], 
                          configuration: Dict[str, Any], is_vector_table: bool = False):
    """
    Fetch incremental data and upsert to destination.
    
    Args:
        cursor: TiDB client connection
        table_name: Name of the table to sync
        state: State dictionary for checkpointing
        configuration: Dictionary containing connector configuration
        is_vector_table: Whether this is a vector table
    """
    # Retrieve last processed timestamp from state
    last_created = state.get(f"{table_name}_last_created", "1990-01-01T00:00:00Z")
    last_created_timestamp = parse_state_timestamp(last_created)
    
    # Build and execute query
    try:
        query, params = build_incremental_query(table_name, last_created)
    except Exception as e:
        log.severe("Failed to build query for table %s: %s", table_name, e)
        return
    
    try:
        rows = execute_query(cursor, query, params)
    except Exception as e:
        # Query execution failed, likely due to missing column
        log.severe("Failed to execute query for table %s: %s", table_name, e)
        state[f"{table_name}_last_error"] = str(e)
        op.checkpoint(state)
        return
    
    # Process rows and track maximum timestamp
    max_seen_timestamp = process_and_upsert_rows(
        rows, table_name, state, configuration, is_vector_table, last_created_timestamp
    )
    
    # Persist updated timestamp to state
    update_state_timestamp(state, table_name, max_seen_timestamp)
    
    # Checkpoint state to persist progress
    try:
        op.checkpoint(state)
    except Exception as chk_err:
        log.severe("Failed to checkpoint state after processing table %s: %s", table_name, chk_err)


def create_tidb_connection(configuration: Dict[str, Any]) -> TiDBClient:
    """
    Create TiDB database connection.
    
    Args:
        configuration: Dictionary containing connector configuration
        
    Returns:
        Connected TiDB client instance
        
    Raises:
        ValueError: If required connection parameters are missing
        Exception: If connection fails
    """
    missing = [k for k in TIDB_CONNECTION_KEYS if not configuration.get(k)]
    if missing:
        raise ValueError(f"Missing required TiDB configuration keys: {', '.join(missing)}")
    
    user = configuration["TIDB_USER"]
    password = configuration["TIDB_PASS"]
    host = configuration["TIDB_HOST"]
    port = configuration["TIDB_PORT"]
    database = configuration["TIDB_DATABASE"]
    
    try:
        TIDB_DATABASE_URL = (
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?ssl_ca={certifi.where()}"
        )
        connection = TiDBClient.connect(TIDB_DATABASE_URL)
        return connection
    except Exception as e:
        log.severe("Failed to create TiDB connection: %s", e)
        raise


def get_tables_to_sync(configuration: Dict[str, Any]) -> List[str]:
    """
    Extract list of table names from configuration.
    
    Args:
        configuration: Dictionary containing connector configuration
        
    Returns:
        List of table names to sync
    """
    try:
        tables_config = json.loads(configuration.get("TABLES_PRIMARY_KEY_COLUMNS", "{}"))
        return list(tables_config.keys())
    except Exception:
        log.severe("Failed to parse TABLES_PRIMARY_KEY_COLUMNS; nothing to do.")
        return []


def get_vector_tables_to_sync(configuration: Dict[str, Any]) -> List[str]:
    """
    Extract list of vector table names from configuration.
    
    Args:
        configuration: Dictionary containing connector configuration
        
    Returns:
        List of vector table names to sync
    """
    if not configuration.get("VECTOR_TABLES_DATA"):
        return []
    
    try:
        vector_tables_config = json.loads(configuration["VECTOR_TABLES_DATA"])
        return list(vector_tables_config.keys())
    except Exception:
        log.info("Failed to parse VECTOR_TABLES_DATA; skipping vector table processing.")
        return []


def sync_regular_tables(connection: TiDBClient, tables: List[str], state: Dict[str, Any], 
                       configuration: Dict[str, Any]):
    """
    Synchronize all regular tables.
    
    Args:
        connection: TiDB client connection
        tables: List of table names to sync
        state: State dictionary for checkpointing
        configuration: Dictionary containing connector configuration
    """
    for table_name in tables:
        try:
            fetch_and_upsert_data(
                cursor=connection, 
                table_name=table_name, 
                state=state, 
                configuration=configuration
            )
        except Exception as t_err:
            # Log table-level errors but continue with other tables
            log.severe("Unhandled error processing table %s: %s", table_name, t_err)
            state[f"{table_name}_last_error"] = str(t_err)
            try:
                op.checkpoint(state)
            except Exception:
                log.severe("Failed to checkpoint state after table-level error for %s", table_name)


def sync_vector_tables(connection: TiDBClient, vector_tables: List[str], state: Dict[str, Any], 
                      configuration: Dict[str, Any]):
    """
    Synchronize all vector tables.
    
    Args:
        connection: TiDB client connection
        vector_tables: List of vector table names to sync
        state: State dictionary for checkpointing
        configuration: Dictionary containing connector configuration
    """
    for table_name in vector_tables:
        try:
            fetch_and_upsert_data(
                cursor=connection, 
                table_name=table_name, 
                state=state, 
                configuration=configuration, 
                is_vector_table=True
            )
        except Exception as t_err:
            # Log vector table errors but continue with other tables
            log.severe("Unhandled error processing vector table %s: %s", table_name, t_err)
            state[f"{table_name}_last_error"] = str(t_err)
            try:
                op.checkpoint(state)
            except Exception:
                log.severe("Failed to checkpoint state after vector-table error for %s", table_name)


def close_connection(connection: TiDBClient):
    """
    Close TiDB connection if close method exists.
    
    Args:
        connection: TiDB client connection
    """
    try:
        if hasattr(connection, "close"):
            connection.close()
    except Exception:
        log.fine("Error while closing TiDB connection (non-fatal).")


def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    """
    Main sync function called by Fivetran runtime.
    
    Args:
        configuration: Dictionary containing connector configuration
        state: State dictionary for incremental sync tracking
    """
    # Validate configuration early
    validate_configuration(configuration)
    
    # Establish database connection
    try:
        connection = create_tidb_connection(configuration=configuration)
    except Exception as conn_err:
        log.severe("Could not connect to TiDB: %s", conn_err)
        state["last_connection_error"] = str(conn_err)
        try:
            op.checkpoint(state)
        except Exception:
            log.severe("Failed to checkpoint state after connection error.")
        raise
    
    # Get tables to sync
    tables = get_tables_to_sync(configuration)
    vector_tables = get_vector_tables_to_sync(configuration)
    
    # Synchronize regular tables
    sync_regular_tables(connection, tables, state, configuration)
    
    # Synchronize vector tables
    sync_vector_tables(connection, vector_tables, state, configuration)
    
    # Clean up connection
    close_connection(connection)


# Define connector instance
connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    """
    Local debug entry point.
    
    Loads configuration from configuration.json and runs connector in debug mode.
    """
    try:
        with open("configuration.json", "r") as f:
            configuration = json.load(f)
    except Exception as e:
        log.severe("Failed to load configuration.json: %s", e)
        raise
    
    # Test the connector locally
    connector.debug(configuration=configuration)
