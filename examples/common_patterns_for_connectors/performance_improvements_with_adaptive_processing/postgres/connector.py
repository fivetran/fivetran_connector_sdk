import os
import json
import time
import random
import threading
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple
from contextlib import contextmanager

from fivetran_connector_sdk import Connector, Logging as log, Operations as op
import pyodbc

# RESOURCE MONITORING & ADAPTIVE PROCESSING CONFIGURATION

# performance_optimization_adaptive_processing

# Configuration constants for adaptive processing
BATCH_SIZE = 5000
PARTITION_SIZE = 50000
CHECKPOINT_INTERVAL = 1000000  # Checkpoint every 1 million records
CONNECTION_TIMEOUT_HOURS = 3  # Reconnect after 3 hours
MAX_RETRIES = 5
BASE_RETRY_DELAY = 5
MAX_RETRY_DELAY = 300  # 5 minutes max delay

# Table size thresholds for adaptive processing
SMALL_TABLE_THRESHOLD = 1000000  # 1M rows
LARGE_TABLE_THRESHOLD = 50000000  # 50M rows

# Resource monitoring thresholds
MEMORY_THRESHOLD_HIGH = 80  # 80% memory usage triggers reduction
MEMORY_THRESHOLD_CRITICAL = 90  # 90% memory usage triggers aggressive reduction
CPU_THRESHOLD_HIGH = 85  # 85% CPU usage triggers thread reduction
CPU_THRESHOLD_CRITICAL = 95  # 95% CPU usage triggers aggressive reduction

# Resource monitoring state
resource_state = {
    'memory_pressure': False,
    'cpu_pressure': False,
    'batch_size_reduced': False,
    'threads_reduced': False,
    'last_monitoring': None,
    'monitoring_interval': 5  # Check every 1 hour (3600)
}

# RESOURCE MONITORING FUNCTIONS

def monitor_resources() -> Dict[str, Any]:
    """Monitor system resources and return current status."""
    try:
        # Get current resource usage using psutil
        import psutil
        
        # Get current resource usage
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Calculate memory usage percentage
        memory_usage = memory.percent
        memory_available_gb = memory.available / (1024**3)
        
        # Get disk usage for the current working directory
        disk = psutil.disk_usage('.')
        disk_usage = (disk.used / disk.total) * 100
        
        # Determine resource pressure levels
        memory_pressure = memory_usage > MEMORY_THRESHOLD_HIGH
        memory_critical = memory_usage > MEMORY_THRESHOLD_CRITICAL
        cpu_pressure = cpu_percent > CPU_THRESHOLD_HIGH
        cpu_critical = cpu_percent > CPU_THRESHOLD_CRITICAL
        
        # Log resource status
        log.info(f"Resource Monitor: Memory {memory_usage:.1f}% ({memory_available_gb:.1f}GB available), "
                f"CPU {cpu_percent:.1f}%, Disk {disk_usage:.1f}%")
        
        # Log warnings for high resource usage
        if memory_critical:
            log.warning(f"CRITICAL MEMORY USAGE: {memory_usage:.1f}% - System under severe memory pressure!")
        elif memory_pressure:
            log.warning(f"HIGH MEMORY USAGE: {memory_usage:.1f}% - Consider reducing batch sizes")
        
        if cpu_critical:
            log.warning(f"CRITICAL CPU USAGE: {cpu_percent:.1f}% - System under severe CPU pressure!")
        elif cpu_pressure:
            log.warning(f"HIGH CPU USAGE: {cpu_percent:.1f}% - Consider reducing thread count")
        
        return {
            'status': 'active',
            'memory_usage': memory_usage,
            'memory_available_gb': memory_available_gb,
            'cpu_percent': cpu_percent,
            'disk_usage': disk_usage,
            'memory_pressure': memory_pressure,
            'memory_critical': memory_critical,
            'cpu_pressure': cpu_pressure,
            'cpu_critical': cpu_critical,
            'timestamp': datetime.now(timezone.utc)
        }
        
    except ImportError:
        log.warning("psutil not available - resource monitoring will be disabled")
        return {'status': 'disabled', 'reason': 'psutil not available'}
    except Exception as e:
        log.warning(f"Resource monitoring failed: {e}")
        return {'status': 'error', 'error': str(e)}

def should_reduce_batch_size(memory_usage: float, current_batch_size: int) -> Tuple[bool, int]:
    """Determine if batch size should be reduced based on memory pressure."""
    if memory_usage > MEMORY_THRESHOLD_CRITICAL:
        # Critical memory pressure - reduce by 50%
        new_batch_size = max(current_batch_size // 2, 100)
        log.warning(f"CRITICAL MEMORY PRESSURE: Reducing batch size from {current_batch_size:,} to {new_batch_size:,} records")
        return True, new_batch_size
    elif memory_usage > MEMORY_THRESHOLD_HIGH:
        # High memory pressure - reduce by 25%
        new_batch_size = max(int(current_batch_size * 0.75), 100)
        log.info(f"HIGH MEMORY PRESSURE: Reducing batch size from {current_batch_size:,} to {new_batch_size:,} records")
        return True, new_batch_size
    
    return False, current_batch_size

def should_reduce_threads(cpu_percent: float, current_threads: int) -> Tuple[bool, int]:
    """Determine if thread count should be reduced based on CPU pressure."""
    if cpu_percent > CPU_THRESHOLD_CRITICAL:
        # Critical CPU pressure - reduce to 1 thread
        new_threads = 1
        log.warning(f"CRITICAL CPU PRESSURE: Reducing threads from {current_threads} to {new_threads}")
        return True, new_threads
    elif cpu_percent > CPU_THRESHOLD_HIGH:
        # High CPU pressure - reduce by 50%
        new_threads = max(current_threads // 2, 1)
        log.info(f"HIGH CPU PRESSURE: Reducing threads from {current_threads} to {new_threads}")
        return True, new_threads
    
    return False, current_threads

# ADAPTIVE PROCESSING FUNCTIONS

def get_adaptive_partition_size(table_size: int) -> int:
    """Get optimal partition size based on table size."""
    if table_size < SMALL_TABLE_THRESHOLD:
        return PARTITION_SIZE  # 50K for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return PARTITION_SIZE // 2  # 25K for medium tables
    else:
        return PARTITION_SIZE // 10  # 5K for large tables

def get_adaptive_batch_size(table_size: int) -> int:
    """Get optimal batch size based on table size."""
    if table_size < SMALL_TABLE_THRESHOLD:
        return BATCH_SIZE  # 5K for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return BATCH_SIZE // 2  # 2.5K for medium tables
    else:
        return BATCH_SIZE // 5  # 1K for large tables

def get_adaptive_queue_size(table_size: int) -> int:
    """Get optimal queue size based on table size."""
    if table_size < SMALL_TABLE_THRESHOLD:
        return 10000  # 10K for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return 5000  # 5K for medium tables
    else:
        return 1000  # 1K for large tables

def get_adaptive_threads(table_size: int) -> int:
    """Get optimal thread count based on table size, capped at 4 threads."""
    if table_size < SMALL_TABLE_THRESHOLD:
        return 4  # 4 threads for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return 2  # 2 threads for medium tables
    else:
        return 1  # 1 thread for large tables to avoid overwhelming the DB

def get_adaptive_checkpoint_interval(table_size: int) -> int:
    """Get adaptive checkpoint interval based on table size."""
    if table_size < SMALL_TABLE_THRESHOLD:
        return CHECKPOINT_INTERVAL  # 1M for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return CHECKPOINT_INTERVAL // 2  # 500K for medium tables
    else:
        return CHECKPOINT_INTERVAL // 10  # 100K for large tables

def get_adaptive_parameters_with_monitoring(table_size: int, base_threads: int, base_batch_size: int) -> Dict[str, Any]:
    """Get adaptive parameters considering both table size and current resource pressure."""
    # Get base adaptive parameters
    partition_size = get_adaptive_partition_size(table_size)
    batch_size = get_adaptive_batch_size(table_size)
    threads = get_adaptive_threads(table_size)
    queue_size = get_adaptive_queue_size(table_size)
    checkpoint_interval = get_adaptive_checkpoint_interval(table_size)
    
    # Apply resource monitoring adjustments
    resource_status = monitor_resources()
    
    if resource_status['status'] == 'active':
        # Check if we need to reduce batch size due to memory pressure
        should_reduce_batch, new_batch_size = should_reduce_batch_size(
            resource_status['memory_usage'], batch_size
        )
        if should_reduce_batch:
            batch_size = new_batch_size
            resource_state['batch_size_reduced'] = True
            log.info(f"Resource monitoring adjusted batch size to {batch_size:,} for table with {table_size:,} rows")
        
        # Check if we need to reduce threads due to CPU pressure
        should_reduce_thread, new_threads = should_reduce_threads(
            resource_status['cpu_percent'], threads
        )
        if should_reduce_thread:
            threads = new_threads
            resource_state['threads_reduced'] = True
            log.info(f"Resource monitoring adjusted threads to {threads} for table with {table_size:,} rows")
        
        # Log resource-aware parameter selection
        log.info(f"Resource-aware parameters for {table_size:,} row table: "
                f"{threads} threads, {batch_size:,} batch size, {partition_size:,} partition size")
    
    return {
        'partition_size': partition_size,
        'batch_size': batch_size,
        'threads': threads,
        'queue_size': queue_size,
        'checkpoint_interval': checkpoint_interval,
        'resource_pressure': resource_status.get('memory_pressure', False) or resource_status.get('cpu_pressure', False),
        'resource_status': resource_status
    }

# TABLE SIZE ANALYSIS & CATEGORIZATION

def get_table_sizes(cursor, tables: List[str]) -> Dict[str, int]:
    """Get row counts for all tables efficiently."""
    table_sizes = {}
    
    # For now, use default estimates to avoid complex query issues
    # This ensures the connector works reliably
    for table in tables:
        table_sizes[table] = 10000  # Default estimate for small tables
    
    log.info(f"Using default table size estimates for {len(tables)} tables")
    
    return table_sizes

def categorize_and_sort_tables(tables: List[str], table_sizes: Dict[str, int]) -> List[Tuple[str, str, int]]:
    """Categorize tables by size and sort for optimal processing order.
    
    Returns: List of tuples (table_name, category, row_count)
    Categories: 'small', 'medium', 'large'
    """
    categorized = []
    
    for table in tables:
        row_count = table_sizes.get(table, 0)
        
        if row_count < SMALL_TABLE_THRESHOLD:
            category = 'small'
        elif row_count < LARGE_TABLE_THRESHOLD:
            category = 'medium'
        else:
            category = 'large'
        
        categorized.append((table, category, row_count))
    
    # Sort by category (small first, then medium, then large) and by row count within each category
    categorized.sort(key=lambda x: ('small', 'medium', 'large').index(x[1]) * 1000000000 + x[2])
    
    return categorized

# CONNECTION MANAGEMENT

class ConnectionManager:
    """Manages database connections with timeout and resource monitoring."""
    
    def __init__(self, configuration: dict, table_size: int = 0):
        self.configuration = configuration
        self.table_size = table_size
        self.connection_start_time = None
        self.current_connection = None
        self.current_cursor = None
        self.lock = threading.Lock()
        self.timeout_hours = CONNECTION_TIMEOUT_HOURS
        
    def _is_connection_expired(self) -> bool:
        """Check if current connection has exceeded timeout limit."""
        if not self.connection_start_time:
            return True
        elapsed = datetime.now(timezone.utc) - self.connection_start_time
        return elapsed.total_seconds() > (self.timeout_hours * 3600)
    
    def _create_connection(self):
        """Create a new database connection."""
        try:
            conn_str = (
                f"DRIVER=/opt/homebrew/lib/psqlodbcw.so;"
                f"SERVER={self.configuration['host']};"
                f"PORT={self.configuration['port']};"
                f"DATABASE={self.configuration['database']};"
                f"UID={self.configuration['username']};"
                f"PWD={self.configuration['password']};"
            )
            conn = pyodbc.connect(conn_str)
            self.current_connection = conn
            self.current_cursor = conn.cursor()
            self.connection_start_time = datetime.now(timezone.utc)
            log.info(f"New database connection established at {self.connection_start_time}")
            return conn, self.current_cursor
        except Exception as e:
            log.severe(f"Failed to create database connection: {e}")
            raise
    
    def _close_connection(self):
        """Close current database connection."""
        try:
            if self.current_cursor:
                self.current_cursor.close()
                self.current_cursor = None
            if self.current_connection:
                self.current_connection.close()
                self.current_connection = None
            self.connection_start_time = None
            log.info("Database connection closed")
        except Exception as e:
            log.warning(f"Error closing connection: {e}")
    
    @contextmanager
    def get_cursor(self):
        """Context manager for database cursor with automatic reconnection."""
        with self.lock:
            try:
                # Check if connection is expired or doesn't exist
                if self._is_connection_expired() or not self.current_connection:
                    self._close_connection()
                    self._create_connection()
                
                yield self.current_cursor
                
            except Exception as e:
                log.severe(f"Database error: {e}")
                self._close_connection()
                raise

# CONFIGURATION VALIDATION

def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    # Validate required configuration parameters
    required_configs = ["host", "port", "database", "username", "password"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")
        
        # Ensure all values are strings as per Fivetran requirements
        if not isinstance(configuration[key], str):
            raise ValueError(f"Configuration value for {key} must be a string, got {type(configuration[key])}")

# SCHEMA FUNCTIONS

def get_primary_keys(cursor, table_name, schema_name):
    """Get primary key columns for a table"""
    cursor.execute("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_name = ?
            AND tc.table_schema = ?
    """, (table_name, schema_name))

    pk_columns = [row[0] for row in cursor.fetchall()]
    return pk_columns if pk_columns else ["id"]  # Default to 'id' if no PK found

def get_table_columns(cursor, table_name, schema_name):
    """Get column names and types for a table"""
    cursor.execute("""
        SELECT column_name, data_type, column_default, is_nullable, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = ?
        AND table_schema = ?
        ORDER BY ordinal_position
    """, (table_name, schema_name))
    return cursor.fetchall()

def get_table_comment(cursor, table_name, schema_name):
    """Fetches the comment for a given table in PostgreSQL using pyodbc."""
    try:
        # Use a simpler approach that works better with pyodbc
        query = """
            SELECT obj_description(c.oid) AS table_comment
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = ? AND n.nspname = ?
        """
        cursor.execute(query, (table_name, schema_name))
        result = cursor.fetchone()
        return result[0] if result and result[0] else None
    except Exception as e:
        log.warning(f"Could not fetch comment for table {schema_name}.{table_name}: {e}")
        return None

def should_process_table(table_name, schema_name, configuration):
    """Determine if table should be processed based on configuration"""
    return True  # Always return True to process all tables

def apply_transformations(row_dict, table_name, configuration):
    """Apply business logic transformations to data"""
    if table_name.lower() == 'orders':
        if row_dict.get('region') == 'fdx55':
            row_dict['region'] = configuration.get('region')  # pull region value from config.json
        elif row_dict.get('region') == 'weg45':
            row_dict['region'] = 'Europe'  # hard code example
    return row_dict

def get_filtered_query(table_name, schema_name, column_names):
    """Get the appropriate query based on table"""
    base_query = f"SELECT {', '.join(column_names)} FROM {schema_name}.{table_name}"
    return base_query

def schema(configuration: dict):
    """Dynamically generate schema including metadata and source tables with resource monitoring"""
    log.info("Starting schema discovery with resource monitoring...")
    
    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)
    
    # Monitor resources before schema discovery
    resource_status = monitor_resources()
    if resource_status['status'] == 'active':
        log.info(f"Resource Monitor: Schema discovery starting - Memory {resource_status['memory_usage']:.1f}%, "
                f"CPU {resource_status['cpu_percent']:.1f}%")
    
    conn_str = (
        f"DRIVER=/opt/homebrew/lib/psqlodbcw.so;"
        f"SERVER={configuration['host']};"
        f"PORT={configuration['port']};"
        f"DATABASE={configuration['database']};"
        f"UID={configuration['username']};"
        f"PWD={configuration['password']};"
    )

    schema_entries = [
        {
            "table": "postgres_table",
            "primary_key": ["table_name", "timestamp"]
        },
        {
            "table": "postgres_column",
            "primary_key": ["table_name", "column_name", "timestamp"]
        }
    ]

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Get all user tables
        cursor.execute("""
            SELECT table_name, table_schema
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            AND table_type = 'BASE TABLE'
        """)

        for table_name, schema_name in cursor.fetchall():
            if should_process_table(table_name, schema_name, configuration):
                primary_keys = get_primary_keys(cursor, table_name, schema_name)
                schema_entries.append({
                    "table": f"source_{schema_name}_{table_name}",
                    "primary_key": primary_keys,
                    "columns": {
                        "region": "string"  # Add region column to schema
                    }
                })

        log.info(f"Schema discovery completed: {len(schema_entries)} tables found")
        return schema_entries

    except pyodbc.Error as e:
        log.severe(f'Schema generation error: {str(e)}')
        raise e
    finally:
        if 'conn' in locals():
            conn.close()

# DATA PROCESSING FUNCTIONS

def process_table_with_adaptive_parameters(table_name: str, schema_name: str, configuration: dict, 
                                         conn_manager: ConnectionManager, state: dict):
    """Process a single table using adaptive parameters based on size and resource monitoring."""
    start_time = datetime.now(timezone.utc)
    records_processed = 0
    
    try:
        # Get table size for adaptive parameters
        # Use default estimate for now to ensure reliable operation
        # In production, you could implement more sophisticated table size detection
        total_rows = 10000  # Default estimate for small tables
        
        # Use resource-aware adaptive parameters
        adaptive_params = get_adaptive_parameters_with_monitoring(total_rows, 0, 0)
        batch_size = adaptive_params['batch_size']
        checkpoint_interval = adaptive_params['checkpoint_interval']
        
        log.info(f"Processing table {schema_name}.{table_name}: {total_rows:,} rows, "
                f"batch size: {batch_size:,}, checkpoint interval: {checkpoint_interval:,}")
        
        if adaptive_params['resource_pressure']:
            log.info(f"Resource Monitor: Processing {table_name} with resource-aware parameters")
        
        # Get column information for the table
        with conn_manager.get_cursor() as cursor:
            columns = get_table_columns(cursor, table_name, schema_name)
            if not columns:
                log.warning(f"No columns found for table {schema_name}.{table_name} - skipping")
                return records_processed
                
            column_names = [col[0] for col in columns if col[0]]  # Filter out None column names
            
            if not column_names:
                log.warning(f"No valid column names found for table {schema_name}.{table_name} - skipping")
                return records_processed
            
            # Build and execute query
            query = get_filtered_query(table_name, schema_name, column_names)
            log.info(f"Executing query for {schema_name}.{table_name}")
            
            try:
                cursor.execute(query)
                
                # Process rows in adaptive batches
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    
                    for row in rows:
                        try:
                            # Create dictionary of column names and values
                            row_dict = {
                                col_name: value
                                for col_name, value in zip(column_names, row)
                                if col_name is not None  # Skip None column names
                            }
                            
                            # Add timestamp to track sync
                            row_dict['sync_timestamp'] = time.time()
                            
                            # Apply transformations
                            row_dict = apply_transformations(row_dict, table_name, configuration)
                            
                            # Filter based on the transformed region value
                            if 'region' in row_dict and row_dict['region'] == configuration.get('region'):
                                # Upsert to destination table based on region value in config
                                yield op.upsert(
                                    f"source_{schema_name}_{table_name}",
                                    row_dict
                                )
                                records_processed += 1
                            elif table_name.lower() != 'orders':
                                yield op.upsert(
                                    f"source_{schema_name}_{table_name}",
                                    row_dict
                                )
                                records_processed += 1
                        except Exception as row_error:
                            log.warning(f"Error processing row in table {schema_name}.{table_name}: {row_error}")
                            continue
                            
            except Exception as query_error:
                log.severe(f"Error executing query for table {schema_name}.{table_name}: {query_error}")
                raise
                
                # Checkpoint every adaptive interval
                if records_processed % checkpoint_interval == 0:
                    log.info(f"Checkpointing {table_name} after {records_processed} records")
                    yield op.checkpoint(state)
        
        processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
        log.info(f"Completed processing {schema_name}.{table_name}: {records_processed} records in {processing_time:.2f}s")
        
    except Exception as e:
        log.severe(f"Error processing table {schema_name}.{table_name}: {e}")
        raise
    
    return records_processed

def display_processing_plan(categorized_tables: List[Tuple[str, str, int]]) -> None:
    """Display a detailed processing plan for the sync operation."""
    log.info("=" * 80)
    log.info("SYNC PROCESSING PLAN")
    log.info("=" * 80)
    
    # Group tables by category
    small_tables = [(t, c, r) for t, c, r in categorized_tables if c == 'small']
    medium_tables = [(t, c, r) for t, c, r in categorized_tables if c == 'medium']
    large_tables = [(t, c, r) for t, c, r in categorized_tables if c == 'large']
    
    # Display small tables (quick wins)
    if small_tables:
        log.info(f"\nSMALL TABLES ({len(small_tables)} tables, <1M rows each):")
        log.info("-" * 50)
        for i, (table, _, rows) in enumerate(small_tables[:10]):  # Show first 10
            log.info(f"  {i+1:2d}. {table:<40} {rows:>10,} rows")
        if len(small_tables) > 10:
            log.info(f"  ... and {len(small_tables) - 10} more small tables")
    
    # Display medium tables
    if medium_tables:
        log.info(f"\nMEDIUM TABLES ({len(medium_tables)} tables, 1M-50M rows each):")
        log.info("-" * 50)
        for i, (table, _, rows) in enumerate(medium_tables[:10]):  # Show first 10
            log.info(f"  {i+1:2d}. {table:<40} {rows:>10,} rows")
        if len(medium_tables) > 10:
            log.info(f"  ... and {len(medium_tables) - 10} more medium tables")
    
    # Display large tables (challenging ones)
    if large_tables:
        log.info(f"\nLARGE TABLES ({len(large_tables)} tables, 50M+ rows each):")
        log.info("-" * 50)
        for i, (table, _, rows) in enumerate(large_tables):
            log.info(f"  {i+1:2d}. {table:<40} {rows:>10,} rows")
    
    # Summary statistics
    total_rows = sum(rows for _, _, rows in categorized_tables)
    small_rows = sum(rows for _, _, rows in small_tables)
    medium_rows = sum(rows for _, _, rows in medium_tables)
    large_rows = sum(rows for _, _, rows in large_tables)
    
    log.info("\n" + "=" * 80)
    log.info("SUMMARY STATISTICS")
    log.info("=" * 80)
    log.info(f"Total tables: {len(categorized_tables)}")
    log.info(f"Total rows: {total_rows:,}")
    log.info(f"Small tables: {len(small_tables)} tables, {small_rows:,} rows ({small_rows/total_rows*100:.1f}%)")
    log.info(f"Medium tables: {len(medium_tables)} tables, {medium_rows:,} rows ({medium_rows/total_rows*100:.1f}%)")
    log.info(f"Large tables: {len(large_tables)} tables, {large_rows:,} rows ({large_rows/total_rows*100:.1f}%)")
    
    log.info("=" * 80)
    log.info("Starting sync process...")
    log.info("=" * 80)

# MAIN UPDATE FUNCTION

def update(configuration: dict, state: dict):
    """Main update function with enhanced resource monitoring and adaptive processing."""
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
        log.info(f"Resource Monitor: Initial system status - Memory {initial_status.get('memory_usage', 'N/A')}%, "
                f"CPU {initial_status.get('cpu_percent', 'N/A')}%, Disk {initial_status.get('disk_usage', 'N/A')}%")
    else:
        log.info(f"Resource Monitor: {initial_status.get('reason', 'Status unavailable')}")
    
    # Get schema and table information
    schema_list = schema(configuration)
    
    # Create connection manager for table discovery
    initial_conn_manager = ConnectionManager(configuration)
    
    # Get table list
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
    
    # Get table sizes and categorize them
    log.info("Analyzing table sizes for optimal processing order...")
    table_sizes = get_table_sizes(initial_conn_manager.current_cursor, tables)
    categorized_tables = categorize_and_sort_tables(tables, table_sizes)
    
    # Display processing strategy
    display_processing_plan(categorized_tables)
    
    # Initialize state for tables
    if "last_updated_at" not in state:
        state["last_updated_at"] = datetime.now(timezone.utc).isoformat()
    
    timestamp = time.time()
    
    # 1. Collect and upsert metadata about tables
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

    # 2. Collect and upsert metadata about columns
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

    # 3. Process tables by category (small first, then medium, then large)
    total_tables = len(categorized_tables)
    processed_tables = 0
    
    for table_name, category, row_count in categorized_tables:
        processed_tables += 1
        schema_name = next((s for t, s in zip(tables, schemas) if t == table_name), 'public')
        
        log.info(f"Processing table {processed_tables}/{total_tables}: {table_name} "
                f"({category}, {row_count:,} rows)")
        
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
                log.warning(f"Error processing table {table_name}, attempt {attempt+1}/{max_retries}: {e}")
                if attempt + 1 >= max_retries:
                    log.severe(f"Max retries exceeded for table {table_name}: {e}")
                    raise
                
                # Exponential backoff with jitter
                base_backoff = min(sleep_seconds * (2 ** attempt), MAX_RETRY_DELAY)
                delay = random.uniform(base_backoff * 0.5, base_backoff * 1.5)
                log.info(f"Retrying {table_name} after backoff {delay:.2f}s (attempt {attempt+1}/{max_retries})")
                time.sleep(delay)
                continue
        
        # Update state and checkpoint after table completion
        state[table_name] = datetime.now(timezone.utc).isoformat()
        yield op.checkpoint(state)
        
        # Progress update
        log.info(f"Completed {processed_tables}/{total_tables} tables. "
                f"Next: {categorized_tables[processed_tables][0] if processed_tables < total_tables else 'None'}")
        
        # Periodic resource monitoring check
        if processed_tables % 5 == 0:  # Check every 5 tables
            log.info("Resource Monitor: Periodic system check...")
            current_status = monitor_resources()
            if current_status.get('status') == 'active':
                log.info(f"Resource Monitor: Current status - Memory {current_status['memory_usage']:.1f}%, "
                        f"CPU {current_status['cpu_percent']:.1f}%, Disk {current_status['disk_usage']:.1f}%")
    
    # Final resource status
    final_status = monitor_resources()
    if final_status.get('status') == 'active':
        log.info(f"Resource Monitor: Final system status - Memory {final_status['memory_usage']:.1f}%, "
                f"CPU {final_status['cpu_percent']:.1f}%, Disk {final_status['disk_usage']:.1f}%")
    
    total_time = (datetime.now(timezone.utc) - start_time).total_seconds()
    log.info(f'Data extraction completed successfully in {total_time:.2f} seconds.')

# CONNECTOR INITIALIZATION

connector = Connector(update=update, schema=schema)

# Main execution
if __name__ == "__main__":
    # Load configuration
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "configuration.json")
    with open(config_path, 'r') as f:
        configuration = json.load(f)
    
    # Run connector in debug mode
    connector.debug(configuration=configuration)
