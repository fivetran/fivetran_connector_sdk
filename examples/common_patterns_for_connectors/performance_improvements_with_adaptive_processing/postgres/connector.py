"""
PostgreSQL Connector with Adaptive Processing and Resource Monitoring.

This connector demonstrates best practices for syncing PostgreSQL databases to Fivetran
with advanced features including:
- Resource monitoring and adaptive processing
- Connection management with automatic reconnection
- Table size-based optimization
- Business logic transformations
- Comprehensive error handling and retry logic
"""

# Standard library imports
import os  # File path operations for loading configuration
import json  # Parse configuration.json file
import time  # Sleep between retries and timestamp generation
import random  # Add jitter to retry backoff delays
from datetime import datetime, timezone  # UTC timestamps for state management

# Fivetran Connector SDK imports
from fivetran_connector_sdk import Connector  # Main connector class for SDK integration
from fivetran_connector_sdk import Logging as log  # Logging functions (info, warning, severe)
from fivetran_connector_sdk import Operations as op  # Data operations (upsert, checkpoint)

# Configuration and constants
from utils.config import (
    MAX_RETRIES,  # Maximum number of retry attempts for failed operations (default: 5)
    BASE_RETRY_DELAY,  # Initial retry delay in seconds (default: 5)
    MAX_RETRY_DELAY,  # Maximum retry delay cap in seconds (default: 300)
    validate_configuration  # Validate required configuration parameters
)

# Resource monitoring utilities
from utils.resource_monitor import monitor_resources  # Monitor system memory, CPU, and disk usage

# Table analysis utilities
from utils.table_analysis import (
    get_table_sizes,  # Estimate row counts for all tables
    categorize_and_sort_tables  # Categorize tables by size and sort for optimal processing
)

# Connection management
from utils.connection_manager import ConnectionManager  # Thread-safe database connection manager with auto-reconnect

# Schema discovery and metadata utilities
from utils.schema_utils import (
    get_table_columns,  # Retrieve column metadata from information_schema
    get_table_comment,  # Fetch table-level comments from PostgreSQL
    should_process_table,  # Determine if a table should be included in sync
    build_schema  # Generate Fivetran schema definition from database metadata
)

# Data processing utilities
from utils.data_processor import (
    process_table_with_adaptive_parameters,  # Process table data with adaptive batching and resource awareness
    display_processing_plan  # Log detailed processing plan showing table categories and order
)


def schema(configuration: dict):
    """
    Define the schema function which configures the schema your connector delivers.
    
    This function is called by Fivetran to discover the structure of your source data.
    It returns a list of table definitions including metadata tables and all source tables.
    
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    
    Args:
        configuration: A dictionary containing connection details including:
            - host: PostgreSQL server hostname
            - port: PostgreSQL server port
            - database: Database name
            - username: Database username
            - password: Database password
    
    Returns:
        List of table schema definitions, each containing:
            - table: Table name in destination
            - primary_key: List of primary key columns
            - columns: Optional dictionary of column definitions
    
    Example:
        >>> config = {"host": "localhost", "port": "5432", ...}
        >>> tables = schema(config)
        >>> # Returns list of table definitions for Fivetran
    """
    return build_schema(configuration)


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function called by Fivetran during each sync.
    
    This function implements the core data extraction logic with adaptive processing,
    resource monitoring, and comprehensive error handling.
    
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
               The state dictionary is empty for the first sync or for any full re-sync
    
    Yields:
        Fivetran operations (upserts, checkpoints) for data sync
    
    Example:
        >>> config = {"host": "localhost", "port": "5432", ...}
        >>> state = {}
        >>> for operation in update(config, state):
        ...     # Fivetran executes each operation
        ...     pass
    """
    start_time = datetime.now(timezone.utc)
    
    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)
    
    # Configuration parameters - convert string values to appropriate types
    max_retries = int(configuration.get("max_retries", str(MAX_RETRIES)))
    sleep_seconds = float(configuration.get("retry_sleep_seconds", str(BASE_RETRY_DELAY)))
    
    log.info('Starting PostgreSQL sync with resource monitoring and adaptive processing...')
    
    # Initialize resource monitoring
    log.info("Resource Monitor: Initializing system monitoring...")
    initial_status = monitor_resources()
    if initial_status['status'] == 'active':
        log.info(
            f"Resource Monitor: Initial system status - "
            f"Memory {initial_status.get('memory_usage', 'N/A')}%, "
            f"CPU {initial_status.get('cpu_percent', 'N/A')}%, "
            f"Disk {initial_status.get('disk_usage', 'N/A')}%"
        )
    else:
        log.info(f"Resource Monitor: {initial_status.get('reason', 'Status unavailable')}")
    
    # Get schema and table information
    schema_list = schema(configuration)
    
    # Create connection manager for table discovery
    initial_conn_manager = ConnectionManager(configuration)
    
    # Get table list from database
    with initial_conn_manager.get_cursor() as cursor:
        cursor.execute("""
            SELECT table_name, table_schema
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            AND table_type = 'BASE TABLE'
        """)
        tables_data = cursor.fetchall()
        tables = [table[0] for table in tables_data if table[0]]
        schemas = [table[1] for table in tables_data if table[1]]
    
    log.info(f"Found {len(tables)} tables to process")
    
    # Get table sizes and categorize them for optimal processing
    log.info("Analyzing table sizes for optimal processing order...")
    table_sizes = get_table_sizes(initial_conn_manager.current_cursor, tables)
    categorized_tables = categorize_and_sort_tables(tables, table_sizes)
    
    # Display processing strategy
    display_processing_plan(categorized_tables)
    
    # Initialize state for tables
    if "last_updated_at" not in state:
        state["last_updated_at"] = datetime.now(timezone.utc).isoformat()
    
    timestamp = time.time()
    
    # STEP 1: Collect and upsert metadata about tables
    log.info('Collecting table metadata...')
    for table_name, schema_name in zip(tables, schemas):
        if should_process_table(table_name, schema_name, configuration):
            # Fetch table comment
            with initial_conn_manager.get_cursor() as cursor:
                comment = get_table_comment(cursor, table_name, schema_name)
                yield op.upsert("postgres_tables", {
                    "table_name": table_name,
                    "schema_name": schema_name,
                    "table_type": "BASE TABLE",
                    "timestamp": timestamp,
                    "comment": comment
                })

    # STEP 2: Collect and upsert metadata about columns
    log.info('Collecting column metadata...')
    for table_name, schema_name in zip(tables, schemas):
        if should_process_table(table_name, schema_name, configuration):
            with initial_conn_manager.get_cursor() as cursor:
                columns = get_table_columns(cursor, table_name, schema_name)
                for column in columns:
                    # Safely extract column metadata with proper indexing
                    column_name = column[0] if len(column) > 0 else None
                    data_type = column[1] if len(column) > 1 else None
                    column_default = column[2] if len(column) > 2 else None
                    is_nullable = column[3] if len(column) > 3 else None
                    char_max_length = column[4] if len(column) > 4 else None
                    
                    if column_name and data_type:  # Only process if we have essential data
                        yield op.upsert("postgres_columns", {
                            "table_name": table_name,
                            "column_name": column_name,
                            "data_type": data_type,
                            "column_default": column_default,
                            "is_nullable": is_nullable,
                            "character_max_length": char_max_length,
                            "timestamp": timestamp
                        })

    # STEP 3: Process tables by category (small first, then medium, then large)
    total_tables = len(categorized_tables)
    processed_tables = 0
    
    for table_name, category, row_count in categorized_tables:
        processed_tables += 1
        schema_name = next((s for t, s in zip(tables, schemas) if t == table_name), 'public')
        
        log.info(
            f"Processing table {processed_tables}/{total_tables}: {table_name} "
            f"({category}, {row_count:,} rows)"
        )
        
        # Create connection manager with table size context for adaptive timeouts
        conn_manager = ConnectionManager(configuration, row_count)
        
        # Retry loop for error handling
        for attempt in range(max_retries):
            try:
                # Process table with adaptive parameters
                for operation in process_table_with_adaptive_parameters(
                    table_name, schema_name, configuration, conn_manager, state
                ):
                    yield operation
                
                # Successful completion, exit retry loop
                log.info(f"Successfully processed table {table_name}")
                break
                
            except Exception as e:
                log.warning(
                    f"Error processing table {table_name}, "
                    f"attempt {attempt+1}/{max_retries}: {e}"
                )
                if attempt + 1 >= max_retries:
                    log.severe(f"Max retries exceeded for table {table_name}: {e}")
                    raise
                
                # Exponential backoff with jitter
                base_backoff = min(sleep_seconds * (2 ** attempt), MAX_RETRY_DELAY)
                delay = random.uniform(base_backoff * 0.5, base_backoff * 1.5)
                log.info(
                    f"Retrying {table_name} after backoff {delay:.2f}s "
                    f"(attempt {attempt+1}/{max_retries})"
                )
                time.sleep(delay)
                continue
        
        # Update state and checkpoint after table completion
        state[table_name] = datetime.now(timezone.utc).isoformat()
        yield op.checkpoint(state)
        
        # Progress update
        next_table = (
            categorized_tables[processed_tables][0] 
            if processed_tables < total_tables 
            else 'None'
        )
        log.info(
            f"Completed {processed_tables}/{total_tables} tables. "
            f"Next: {next_table}"
        )
        
        # Periodic resource monitoring check
        if processed_tables % 5 == 0:  # Check every 5 tables
            log.info("Resource Monitor: Periodic system check...")
            current_status = monitor_resources()
            if current_status.get('status') == 'active':
                log.info(
                    f"Resource Monitor: Current status - "
                    f"Memory {current_status['memory_usage']:.1f}%, "
                    f"CPU {current_status['cpu_percent']:.1f}%, "
                    f"Disk {current_status['disk_usage']:.1f}%"
                )
    
    # Final resource status
    final_status = monitor_resources()
    if final_status.get('status') == 'active':
        log.info(
            f"Resource Monitor: Final system status - "
            f"Memory {final_status['memory_usage']:.1f}%, "
            f"CPU {final_status['cpu_percent']:.1f}%, "
            f"Disk {final_status['disk_usage']:.1f}%"
        )
    
    total_time = (datetime.now(timezone.utc) - start_time).total_seconds()
    log.info(f'Data extraction completed successfully in {total_time:.2f} seconds.')


# Initialize the connector with the defined update and schema functions
connector = Connector(update=update, schema=schema)


# Main execution
if __name__ == "__main__":
    """
    Entry point for local testing and debugging.
    
    This block allows you to run the connector locally for development and testing.
    It loads configuration from configuration.json and runs the connector in debug mode.
    
    For production deployment, use: fivetran debug --configuration configuration.json
    """
    # Load configuration from configuration.json file
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "configuration.json")
    with open(config_path, 'r') as f:
        configuration = json.load(f)
    
    # Run connector in debug mode
    connector.debug(configuration=configuration)
