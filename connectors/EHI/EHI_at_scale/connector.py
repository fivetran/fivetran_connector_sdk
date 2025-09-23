"""
Fivetran Connector for SQL Server Database with Adaptive Processing

This connector demonstrates how to efficiently sync data from SQL Server databases with 
adaptive processing based on table sizes and system resources. It includes:

- Adaptive batch sizes and thread counts based on table size
- Resource monitoring and automatic parameter adjustment
- Deadlock and timeout handling with automatic reconnection
- Incremental sync support with proper state management
- Optimized processing order (small tables first, then medium, then large)

For AI/ML data pipelines, this connector is optimized for:
- High-volume data processing with memory management
- Schema evolution handling
- Time-series data optimization
- Batch processing for large datasets

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Standard library imports for core functionality
# For file system operations and path handling
import os

# For JSON data serialization and configuration parsing
import json

# For regular expressions used in certificate parsing
import re

# For mathematical operations like ceiling calculations for partition sizing
import math

# For detecting operating system (Darwin for macOS)
import platform

# For creating temporary files for SSL certificate storage
import tempfile

# For executing OpenSSL commands to generate certificate chains
import subprocess

# For sleep operations in retry logic and timing
import time

# For adding jitter to exponential backoff retry delays
import random

# For thread-safe operations and connection management
import threading

# For thread-safe queue operations in multi-threaded data processing
import queue

# For creating context managers for database connections
from contextlib import contextmanager

# For timestamp handling and timezone-aware operations
from datetime import datetime, timedelta, timezone

# Third-party imports for advanced functionality
# For ThreadPoolExecutor to enable parallel processing of table partitions
import concurrent.futures

# For HTTP requests to fetch SSL certificate chains from DigiCert
import requests

# Microsoft SQL Server database driver for TDS protocol connections
import pytds

# Type hints for better code documentation and IDE support
# For type annotations in function signatures
from typing import Dict, List, Any, Optional, Tuple

# Optional resource monitoring import with graceful fallback
# psutil provides system resource monitoring (CPU, memory, disk usage) for adaptive processing
# This allows the connector to automatically adjust batch sizes and thread counts based on system load
try:
    # For monitoring system resources (CPU, memory, disk) to optimize processing parameters
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    log.warning("psutil not available - resource monitoring will be disabled")

# CONFIGURATION CONSTANTS - ADAPTIVE PROCESSING THRESHOLDS
# 
# THRESHOLD OPTIMIZATION GUIDE:
# These thresholds control how the connector adapts its processing strategy based on table size.
# Adjust these values based on your specific environment and data characteristics:
#
# SMALL_TABLE_THRESHOLD (1M rows): Tables smaller than this are processed with maximum parallelism
# LARGE_TABLE_THRESHOLD (50M rows): Tables larger than this are processed with minimal parallelism
# 
# For AI/ML data pipelines:
# - If your tables have many features (wide tables), consider reducing SMALL_TABLE_THRESHOLD
# - If your data has high cardinality, consider increasing LARGE_TABLE_THRESHOLD
# - For time-series data, these thresholds work well as-is
# - For sparse data, you may want to reduce thresholds by 50%

BATCH_SIZE = 5000
PARTITION_SIZE = 50000
CHECKPOINT_INTERVAL = 1000000  # Checkpoint every 1 million records
CONNECTION_TIMEOUT_HOURS = 3  # Reconnect after 3 hours
MAX_RETRIES = 5
BASE_RETRY_DELAY = 5
MAX_RETRY_DELAY = 300  # 5 minutes max delay

# Table size thresholds for adaptive processing
SMALL_TABLE_THRESHOLD = 1000000  # 1M rows - adjust based on your data characteristics
LARGE_TABLE_THRESHOLD = 50000000  # 50M rows - adjust based on your data characteristics

# Resource monitoring thresholds (when psutil is available)
MEMORY_THRESHOLD_HIGH = 80  # 80% memory usage triggers reduction
MEMORY_THRESHOLD_CRITICAL = 90  # 90% memory usage triggers aggressive reduction
CPU_THRESHOLD_HIGH = 85  # 85% CPU usage triggers thread reduction
CPU_THRESHOLD_CRITICAL = 95  # 95% CPU usage triggers aggressive reduction

# These queries are optimized for SQL Server and include Fivetran-specific fields.
# Modify these queries to match your specific database schema and requirements.

# Schema discovery queries
SCHEMA_TABLE_LIST_QUERY = """
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME IN ('table_1','table_2','table_3','table_4')
"""

SCHEMA_TABLE_LIST_QUERY_DEBUG = """
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME IN ('FLOWSHEET', 'ACTIONACTIVITY')
"""

SCHEMA_PK_QUERY = """
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
WHERE TABLE_NAME = '{table}' AND CONSTRAINT_NAME LIKE '%PRIMARY%'
"""

SCHEMA_COL_QUERY = """
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = 'dbo' 
ORDER BY ORDINAL_POSITION
"""

# Data extraction queries
SRC_UPSERT_RECORDS = """
SELECT TGT.*, CAST(0 as bit) as _FIVETRAN_DELETED, CURRENT_TIMESTAMP as _FIVETRAN_SYNCED 
FROM {tableName} TGT 
WHERE _LastUpdatedInstant >= '{endDate}' and _LastUpdatedInstant <= '{startDate}'
"""

SRC_DEL_RECORDS = """
SELECT {joincol} FROM {tableName} inc 
WHERE _LastUpdatedInstant >= '{endDate}' and _LastUpdatedInstant <= '{startDate}' and _IsDeleted = 1
"""

SRC_VAL_RECORD_COUNT = """
select count_big(*) from {tableName} where _IsDeleted = 0
"""

SRC_FL_RECORDS = """
select *,CAST(0 as bit) as _FIVETRAN_DELETED, CURRENT_TIMESTAMP as _FIVETRAN_SYNCED 
from {tableName} 
where {indexkey} between '{lowerbound}' and '{upperbound}' and _IsDeleted=0
"""

SRC_GEN_INDEX_COLUMN = """
SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = '{tableName}' and ordinal_position=1 and table_schema='dbo'
"""

# Complex partitioning query for large table optimization
SRC_GEN_INDEX_COLUMN_BOUNDS = """
WITH full_count AS (SELECT COUNT_BIG(*) AS rcnt FROM {tableName}), 
group_count AS (SELECT {indexkey}, COUNT_BIG(*) AS rcnt FROM {tableName} GROUP BY {indexkey}), 
parent_counts AS (SELECT group_count.{indexkey}, group_count.rcnt, (SELECT rcnt FROM full_count) AS tcnt FROM group_count), 
silky AS (SELECT parent_counts.{indexkey}, parent_counts.rcnt, parent_counts.tcnt, 
         CASE WHEN tcnt < 10000 THEN 1 ELSE {threads} END AS silkysheets FROM parent_counts), 
splitsizes AS (SELECT {indexkey}, rcnt, tcnt, silkysheets, CAST(tcnt / silkysheets AS NUMERIC) AS splitsize FROM silky), 
runningsplits AS (SELECT {indexkey}, splitsize, tcnt, silkysheets, 
                 SUM(rcnt) OVER (ORDER BY {indexkey} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS splitsum FROM splitsizes), 
threadgroups AS (SELECT {indexkey}, splitsize, tcnt, silkysheets, 
                CASE WHEN CAST(splitsum / splitsize AS NUMERIC) >= silkysheets - 1 THEN silkysheets - 1 
                     ELSE CAST(splitsum / splitsize AS NUMERIC) END AS threadgroup FROM runningsplits), 
minmax AS (SELECT threadgroup, splitsize, tcnt, silkysheets, 
          MIN({indexkey}) AS min_ord1pk, MAX({indexkey}) AS max_ord1pk FROM threadgroups GROUP BY threadgroup, splitsize, tcnt, silkysheets) 
SELECT threadgroup, min_ord1pk AS lowerbound, max_ord1pk AS upperbound, splitsize, tcnt, silkysheets 
FROM minmax ORDER BY threadgroup
"""

# ERROR HANDLING PATTERNS

# Deadlock detection patterns
DEADLOCK_PATTERNS = [
    'deadlock',
    'lock timeout',
    'lock wait timeout',
    'transaction deadlock',
    'lock request time out period exceeded',
    'lock escalation',
    'lock conflict',
    'blocked by another transaction'
]

# Connection timeout patterns
TIMEOUT_PATTERNS = [
    'connection timeout',
    'connection reset',
    'connection lost',
    'network timeout',
    'read timeout',
    'write timeout',
    'socket timeout',
    'timeout expired'
]

# CONNECTION MANAGEMENT

class ConnectionManager:
    """
    Manages database connections with timeout and deadlock detection.
    
    This class provides automatic connection management with:
    - Connection timeout handling
    - Deadlock detection and recovery
    - Automatic reconnection
    - Thread-safe operations
    """
    
    def __init__(self, configuration: dict, table_size: int = 0):
        self.configuration = configuration
        self.table_size = table_size
        self.connection_start_time = None
        self.current_connection = None
        self.current_cursor = None
        self.lock = threading.Lock()
        self.timeout_hours = get_adaptive_timeout(table_size)
        
    def _is_connection_expired(self) -> bool:
        """Check if current connection has exceeded timeout limit."""
        if not self.connection_start_time:
            return True
        elapsed = datetime.now(timezone.utc) - self.connection_start_time
        return elapsed.total_seconds() > (self.timeout_hours * 3600)
    
    def _is_deadlock_error(self, error: Exception) -> bool:
        """Detect if error is related to deadlock or lock timeout."""
        error_str = str(error).lower()
        return any(pattern in error_str for pattern in DEADLOCK_PATTERNS)
    
    def _is_timeout_error(self, error: Exception) -> bool:
        """Detect if error is related to connection timeout."""
        error_str = str(error).lower()
        return any(pattern in error_str for pattern in TIMEOUT_PATTERNS)
    
    def _create_connection(self):
        """Create a new database connection."""
        try:
            conn = connect_to_mssql(self.configuration)
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
                # Handle deadlock and timeout errors
                if self._is_deadlock_error(e):
                    log.warning(f"Deadlock detected: {e}")
                    self._close_connection()
                    raise DeadlockError(f"Database deadlock: {e}")
                elif self._is_timeout_error(e):
                    log.warning(f"Connection timeout detected: {e}")
                    self._close_connection()
                    raise TimeoutError(f"Database timeout: {e}")
                else:
                    log.severe(f"Database error: {e}")
                    raise

class DeadlockError(Exception):
    """Custom exception for deadlock errors."""
    pass

class TimeoutError(Exception):
    """Custom exception for timeout errors."""
    pass

# UTILITY FUNCTIONS

def safe_get_column_value(row, column_name: str, column_index: int = 0) -> Any:
    """
    Safely extract a column value from a cursor row, handling both dict and tuple formats.
    
    This function provides robust column value extraction that works with different
    cursor result formats, which is important for AI/ML data that may have varying schemas.
    """
    try:
        if isinstance(row, dict):
            return row.get(column_name)
        elif isinstance(row, (list, tuple)):
            return row[column_index] if column_index < len(row) else None
        else:
            return row
    except Exception as e:
        log.warning(f"Error extracting column value: {e}, row type: {type(row)}, column_name: {column_name}, column_index: {column_index}")
        return None

def safe_get_column_names(cursor) -> List[str]:
    """Safely extract column names from cursor description."""
    try:
        if hasattr(cursor, 'description') and cursor.description:
            column_names = [desc[0] for desc in cursor.description if desc and len(desc) > 0]
            if not column_names:
                log.warning("Cursor description exists but no column names found")
            return column_names
        else:
            log.warning("No cursor description available")
            return []
    except Exception as e:
        log.warning(f"Error extracting column names: {e}")
        return []

def safe_get_table_name(row, table_name_column: str = 'TABLE_NAME', table_name_index: int = 0) -> Optional[str]:
    """Safely extract table name from a cursor row."""
    try:
        value = safe_get_column_value(row, table_name_column, table_name_index)
        if value is None:
            log.warning(f"Could not extract table name from row: {row}")
            return None
        return str(value)
    except Exception as e:
        log.warning(f"Error extracting table name: {e}")
        return None

def safe_get_column_name(row, column_name_column: str = 'COLUMN_NAME', column_name_index: int = 0) -> Optional[str]:
    """Safely extract column name from a cursor row."""
    try:
        value = safe_get_column_value(row, column_name_column, column_name_index)
        if value is None:
            log.warning(f"Could not extract column name from row: {row}")
            return None
        return str(value)
    except Exception as e:
        log.warning(f"Error extracting column name: {e}")
        return None

def safe_get_row_count(row, row_count_column: str = 'ROW_COUNT', row_count_index: int = 1) -> int:
    """Safely extract row count from a cursor row."""
    try:
        value = safe_get_column_value(row, row_count_column, row_count_index)
        if value is None:
            return 0
        return int(value) if value is not None else 0
    except (ValueError, TypeError) as e:
        log.warning(f"Error converting row count to integer: {e}, value: {value}")
        return 0
    except Exception as e:
        log.warning(f"Error extracting row count: {e}")
        return 0

def flatten_dict(prefix: str, d: Any, result: Dict[str, Any]) -> None:
    """
    Flatten nested dictionary structures for database storage.
    
    This function is essential for AI/ML data that often contains nested structures
    like feature vectors, metadata, or complex JSON objects.
    """
    if isinstance(d, dict):
        if not d:
            result[prefix] = 'N/A'
        else:
            for k, v in d.items():
                new_key = f"{prefix}_{k}" if prefix else k
                flatten_dict(new_key, v, result)
    elif isinstance(d, list):
        result[prefix] = json.dumps(d) if d else 'N/A'
    else:
        result[prefix] = d if d is not None and d != "" else 'N/A'

def generate_cert_chain(server: str, port: int) -> str:
    """Generates a certificate chain file by fetching intermediate and root certificates."""
    proc = subprocess.run(
        ['openssl', 's_client', '-showcerts', '-connect', f'{server}:{port}'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    root_pem = requests.get(
        'https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem'
    ).text
    pem_blocks = re.findall(
        r'-----BEGIN CERTIFICATE-----.+?-----END CERTIFICATE-----',
        proc.stdout,
        re.DOTALL
    )
    intermediate = pem_blocks[1] if len(pem_blocks) > 1 else pem_blocks[0]
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem')
    tmp.write((intermediate + '\n' + root_pem).encode('utf-8'))
    tmp.flush()
    tmp.close()
    return tmp.name

def connect_to_mssql(configuration: dict):
    """
    Connects to MSSQL using TDS with SSL cert chain.
    
    This function handles the complex SSL certificate setup required for
    secure connections to SQL Server databases.
    """
    is_local = platform.system() == "Darwin"
    server_key = "MSSQL_SERVER_DIR" if is_local else "MSSQL_SERVER"
    cert_key = "MSSQL_CERT_SERVER_DIR" if is_local else "MSSQL_CERT_SERVER"
    port_key = "MSSQL_PORT_DIR" if is_local else "MSSQL_PORT"
    server = configuration.get(server_key)
    cert_server = configuration.get(cert_key)
    port = configuration.get(port_key)
    cafile_cfg = configuration.get("cert", "")

    if cafile_cfg:
        if cafile_cfg.lstrip().startswith("-----BEGIN"):
            import OpenSSL.SSL as SSL, OpenSSL.crypto as crypto, pytds.tls
            ctx = SSL.Context(SSL.TLS_METHOD)
            ctx.set_verify(SSL.VERIFY_PEER, lambda conn, cert, errnum, depth, ok: bool(ok))
            pem_blocks = re.findall(
                r'-----BEGIN CERTIFICATE-----.+?-----END CERTIFICATE-----',
                cafile_cfg, re.DOTALL
            )
            store = ctx.get_cert_store()
            if store is None:
                raise RuntimeError("Failed to retrieve certificate store from SSL context")
            for pem in pem_blocks:
                certificate = crypto.load_certificate(crypto.FILETYPE_PEM, pem)
                store.add_cert(certificate)
            pytds.tls.create_context = lambda cafile: ctx
            cafile = 'ignored'
        elif os.path.isfile(cafile_cfg):
            cafile = cafile_cfg
        else:
            if not cert_server or not port:
                raise ValueError("Cannot generate cert chain: server or port missing")
            cafile = generate_cert_chain(cert_server, int(port))
    else:
        if not cert_server or not port:
            raise ValueError("Cannot generate cert chain: server or port missing")
        cafile = generate_cert_chain(cert_server, int(port))
        
    conn = pytds.connect(
        server=server,
        database=configuration["MSSQL_DATABASE"],
        user=configuration["MSSQL_USER"],
        password=configuration["MSSQL_PASSWORD"],
        port=port,
        cafile=cafile,
        validate_host=False
    )
    return conn

# CONFIGURATION VALIDATION

def validate_configuration(configuration: dict) -> None:
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    
    This function ensures that all necessary configuration values are present
    before attempting to connect to the database.
    """
    required_configs = [
        "MSSQL_DATABASE", "MSSQL_USER", "MSSQL_PASSWORD",
        "MSSQL_SERVER", "MSSQL_PORT"
    ]
    
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

def ensure_string_configuration(configuration: dict) -> dict:
    """
    Ensure all configuration values are strings, except for specific boolean flags.
    
    This function converts configuration values to strings as required by the
    Fivetran Connector SDK, while preserving boolean values for debug flags.
    """
    string_config = {}
    
    # Boolean flags that should remain as booleans
    boolean_flags = {'debug'}
    
    for key, value in configuration.items():
        if key in boolean_flags:
            # Preserve boolean values for specific flags
            string_config[key] = value
        else:
            # Convert all other values to strings
            string_config[key] = str(value) if value is not None else ""
    
    return string_config

# ADAPTIVE PROCESSING FUNCTIONS

def get_adaptive_parameters_with_monitoring(table_size: int, base_threads: int, base_batch_size: int) -> Dict[str, Any]:
    """
    Get adaptive parameters considering both table size and current resource pressure.
    
    This function combines table size analysis with real-time resource monitoring
    to determine optimal processing parameters. It's particularly useful for
    AI/ML data pipelines where data volume and system resources can vary significantly.
    
    Args:
        table_size: Number of rows in the table
        base_threads: Base thread count from configuration
        base_batch_size: Base batch size from configuration
        
    Returns:
        Dictionary containing optimized processing parameters
    """
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
            log.info(f"Resource monitoring adjusted batch size to {batch_size:,} for table with {table_size:,} rows")
        
        # Check if we need to reduce threads due to CPU pressure
        should_reduce_thread, new_threads = should_reduce_threads(
            resource_status['cpu_percent'], threads
        )
        if should_reduce_thread:
            threads = new_threads
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

def get_adaptive_partition_size(table_size: int) -> int:
    """
    Get optimal partition size based on table size.
    
    THRESHOLD OPTIMIZATION:
    - Small tables (<1M rows): Use full partition size for maximum efficiency
    - Medium tables (1M-50M rows): Use half partition size to balance memory and speed
    - Large tables (50M+ rows): Use 1/10 partition size to prevent memory overflow
    
    For AI/ML data with many features, consider reducing these values by 25%.
    """
    if table_size < SMALL_TABLE_THRESHOLD:
        return PARTITION_SIZE  # 50K for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return PARTITION_SIZE // 2  # 25K for medium tables
    else:
        return PARTITION_SIZE // 10  # 5K for large tables

def get_adaptive_batch_size(table_size: int) -> int:
    """
    Get optimal batch size based on table size.
    
    THRESHOLD OPTIMIZATION:
    - Small tables (<1M rows): Use full batch size for maximum throughput
    - Medium tables (1M-50M rows): Use half batch size to balance memory usage
    - Large tables (50M+ rows): Use 1/5 batch size to prevent memory overflow
    
    For wide tables with many columns, consider reducing these values by 50%.
    """
    if table_size < SMALL_TABLE_THRESHOLD:
        return BATCH_SIZE  # 5K for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return BATCH_SIZE // 2  # 2.5K for medium tables
    else:
        return BATCH_SIZE // 5  # 1K for large tables

def get_adaptive_queue_size(table_size: int) -> int:
    """
    Get optimal queue size based on table size.
    
    THRESHOLD OPTIMIZATION:
    - Small tables (<1M rows): Larger queue for maximum parallelism
    - Medium tables (1M-50M rows): Medium queue to balance memory and speed
    - Large tables (50M+ rows): Smaller queue to prevent memory overflow
    """
    if table_size < SMALL_TABLE_THRESHOLD:
        return 10000  # 10K for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return 5000  # 5K for medium tables
    else:
        return 1000  # 1K for large tables

def get_adaptive_threads(table_size: int) -> int:
    """
    Get optimal thread count based on table size, capped at 4 threads.
    
    THRESHOLD OPTIMIZATION:
    - Small tables (<1M rows): Maximum threads for parallel processing
    - Medium tables (1M-50M rows): Moderate threads to balance resources
    - Large tables (50M+ rows): Single thread to avoid overwhelming the database
    
    For databases with connection limits, consider reducing these values.
    """
    if table_size < SMALL_TABLE_THRESHOLD:
        return 4  # 4 threads for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return 2  # 2 threads for medium tables
    else:
        return 1  # 1 thread for large tables to avoid overwhelming the DB

def get_adaptive_timeout(table_size: int) -> int:
    """
    Get adaptive timeout based on table size.
    
    THRESHOLD OPTIMIZATION:
    - Small tables (<1M rows): Standard timeout (3 hours)
    - Medium tables (1M-50M rows): Double timeout (6 hours)
    - Large tables (50M+ rows): Quadruple timeout (12 hours)
    
    Adjust these values based on your network latency and database performance.
    """
    if table_size < SMALL_TABLE_THRESHOLD:
        return CONNECTION_TIMEOUT_HOURS  # 3 hours for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return CONNECTION_TIMEOUT_HOURS * 2  # 6 hours for medium tables
    else:
        return CONNECTION_TIMEOUT_HOURS * 4  # 12 hours for large tables

def get_adaptive_checkpoint_interval(table_size: int) -> int:
    """
    Get adaptive checkpoint interval based on table size.
    
    THRESHOLD OPTIMIZATION:
    - Small tables (<1M rows): Standard checkpoint interval (1M records)
    - Medium tables (1M-50M rows): Half checkpoint interval (500K records)
    - Large tables (50M+ rows): Tenth checkpoint interval (100K records)
    
    More frequent checkpoints help with recovery but add overhead.
    """
    if table_size < SMALL_TABLE_THRESHOLD:
        return CHECKPOINT_INTERVAL  # 1M for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return CHECKPOINT_INTERVAL // 2  # 500K for medium tables
    else:
        return CHECKPOINT_INTERVAL // 10  # 100K for large tables

# RESOURCE MONITORING FUNCTIONS

def monitor_resources() -> Dict[str, Any]:
    """
    Monitor system resources and return current status.
    
    This function helps optimize processing by monitoring system resources and
    automatically adjusting processing parameters when resource pressure is detected.
    
    Returns:
        Dict containing resource status and usage metrics
    """
    if not PSUTIL_AVAILABLE:
        return {'status': 'disabled', 'reason': 'psutil not available'}
    
    try:
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
        
    except Exception as e:
        log.warning(f"Resource monitoring failed: {e}")
        return {'status': 'error', 'error': str(e)}

def should_reduce_batch_size(memory_usage: float, current_batch_size: int) -> Tuple[bool, int]:
    """
    Determine if batch size should be reduced based on memory pressure.
    
    This function helps prevent memory overflow by reducing batch sizes when
    system memory usage is high.
    
    Args:
        memory_usage: Current memory usage percentage
        current_batch_size: Current batch size being used
        
    Returns:
        Tuple of (should_reduce, new_batch_size)
    """
    if not PSUTIL_AVAILABLE:
        return False, current_batch_size
    
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
    """
    Determine if thread count should be reduced based on CPU pressure.
    
    This function helps prevent CPU overload by reducing thread counts when
    system CPU usage is high.
    
    Args:
        cpu_percent: Current CPU usage percentage
        current_threads: Current number of threads being used
        
    Returns:
        Tuple of (should_reduce, new_thread_count)
    """
    if not PSUTIL_AVAILABLE:
        return False, current_threads
    
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

# DATA PROCESSING FUNCTIONS

def get_table_sizes(configuration: dict, conn_manager, tables: List[str]) -> Dict[str, int]:
    """
    Get row counts for all tables efficiently.
    
    This function uses a single query to get all table sizes at once, which is
    much more efficient than individual queries for each table.
    """
    table_sizes = {}
    
    # Use a single query to get all table sizes at once
    size_query = """
    SELECT 
        t.TABLE_NAME,
        p.rows as ROW_COUNT
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.object_id = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    WHERE i.index_id <= 1
    AND t.TABLE_NAME IN ({})
    """.format(','.join([f"'{table}'" for table in tables]))
    
    try:
        with conn_manager.get_cursor() as cursor:
            cursor.execute(size_query)
            for row in cursor.fetchall():
                table_name = safe_get_table_name(row)
                row_count = safe_get_row_count(row)
                if table_name:
                    table_sizes[table_name] = row_count
    except Exception as e:
        log.warning(f"Failed to get table sizes efficiently: {e}. Falling back to individual queries.")
        # Fallback to individual queries
        for table in tables:
            try:
                with conn_manager.get_cursor() as cursor:
                    cursor.execute(SRC_VAL_RECORD_COUNT.format(tableName=table))
                    row = cursor.fetchone()
                    if row is not None:
                        table_sizes[table] = safe_get_row_count(row, None, 0)
                    else:
                        table_sizes[table] = 0
            except Exception as e2:
                log.warning(f"Failed to get size for table {table}: {e2}")
                table_sizes[table] = 0
    
    return table_sizes

def categorize_and_sort_tables(tables: List[str], table_sizes: Dict[str, int]) -> List[Tuple[str, str, int]]:
    """
    Categorize tables by size and sort for optimal processing order.
    
    This function categorizes tables into small, medium, and large based on
    the configured thresholds and sorts them for optimal processing order.
    
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

def display_processing_plan(categorized_tables: List[Tuple[str, str, int]]) -> None:
    """
    Display a detailed processing plan for the sync operation.
    
    This function provides a comprehensive overview of the processing strategy,
    helping users understand how their data will be processed and optimized.
    """
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
    
    # Estimated processing time (rough estimates)
    if small_tables:
        log.info(f"\nEstimated processing time:")
        log.info(f"  Small tables: ~{len(small_tables) * 2} minutes")
    if medium_tables:
        log.info(f"  Medium tables: ~{len(medium_tables) * 15} minutes")
    if large_tables:
        log.info(f"  Large tables: ~{sum(rows/1000000 for _, _, rows in large_tables)} hours")
    
    log.info("=" * 80)
    log.info("Starting sync process...")
    log.info("=" * 80)

# SCHEMA DISCOVERY AND PROCESSING

def schema(configuration: dict) -> List[Dict[str, Any]]:
    """
    Discover tables, columns, and primary keys.
    
    This function queries the database to discover the schema structure,
    including table names, primary keys, and column information. It's essential
    for AI/ML data pipelines where schema evolution is common.
    
    Args:
        configuration: Dictionary containing database connection details
        
    Returns:
        List of table schema definitions with primary keys and columns
    """
    # Ensure all configuration values are strings
    configuration = ensure_string_configuration(configuration)
    
    validate_configuration(configuration)
    
    raw_debug = configuration.get("debug", False)
    debug = (isinstance(raw_debug, str) and raw_debug.lower() == 'true')
    log.info(f"Debug mode: {debug}")
    
    conn_manager = ConnectionManager(configuration)
    
    with conn_manager.get_cursor() as cursor:
        query = SCHEMA_TABLE_LIST_QUERY_DEBUG if debug else SCHEMA_TABLE_LIST_QUERY
        cursor.execute(query)
        tables = [safe_get_table_name(r) for r in cursor.fetchall() if safe_get_table_name(r)]
    
    result = []
    for table in tables:
        try:
            with conn_manager.get_cursor() as cursor:
                cursor.execute(SCHEMA_PK_QUERY.format(table=table))
                pk_rows = cursor.fetchall()
                primary_keys = [safe_get_column_name(r) for r in pk_rows if safe_get_column_name(r)]
                cursor.execute(SCHEMA_COL_QUERY.format(table=table))
                col_rows = cursor.fetchall()
                columns = [safe_get_column_name(r) for r in col_rows if safe_get_column_name(r)]
            
            obj = {'table': table}
            if primary_keys:
                obj['primary_key'] = primary_keys
            if columns:
                obj['column'] = columns
            result.append(obj)
            
        except Exception as e:
            log.warning(f"Error processing schema for table {table}: {e}")
            continue
    
    return result

def process_incremental_sync(table: str, configuration: dict, state: dict, 
                           conn_manager: ConnectionManager, pk_map_full: Dict[str, List[str]]):
    """
    Process incremental sync for a table.
    
    This function handles incremental data synchronization by processing only
    records that have been updated since the last sync. It's optimized for
    AI/ML data pipelines where data changes frequently.
    
    Args:
        table: Name of the table to sync
        configuration: Database configuration
        state: Current sync state
        conn_manager: Database connection manager
        pk_map_full: Mapping of tables to their primary key columns
        
    Returns:
        Number of records processed
    """
    start_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    records_processed = 0
    
    # Get table size for adaptive parameters
    try:
        with conn_manager.get_cursor() as cursor:
            cursor.execute(SRC_VAL_RECORD_COUNT.format(tableName=table))
            row = cursor.fetchone()
            if row is not None:
                total_rows = safe_get_row_count(row, None, 0)
            else:
                total_rows = 0
            
            # Use resource-aware adaptive parameters
            adaptive_params = get_adaptive_parameters_with_monitoring(total_rows, 0, 0)
            batch_size = adaptive_params['batch_size']
            checkpoint_interval = adaptive_params['checkpoint_interval']
            
            if adaptive_params['resource_pressure']:
                log.info(f"Resource Monitor: Processing incremental sync for {table} with resource-aware parameters")
            
    except Exception as e:
        log.warning(f"Could not determine table size for {table}, using default batch size: {e}")
        batch_size = BATCH_SIZE
        checkpoint_interval = CHECKPOINT_INTERVAL
    
    with conn_manager.get_cursor() as cursor:
        query = SRC_UPSERT_RECORDS.format(
            tableName=table, endDate=state[table], startDate=start_date
        )
        log.info(f"Incremental sync for {table}: {query}")
        cursor.execute(query)
        
        cols = safe_get_column_names(cursor)
        if not cols:
            log.error(f"No column information available for table {table} - skipping incremental sync")
            return records_processed
            
        log.info(f"Processing {len(cols)} columns for table {table}: {cols[:5]}{'...' if len(cols) > 5 else ''}")
        
        while True:
            rows = cursor.fetchmany(batch_size)  # Use adaptive batch size
            if not rows:
                break
            for row in rows:
                try:
                    flat_data = {}
                    flatten_dict("", {cols[i]: row[i] for i in range(len(cols)) if i < len(row)}, flat_data)
                    op.upsert(table=table, data=flat_data)
                    records_processed += 1
                except Exception as e:
                    log.warning(f"Error processing row in table {table}: {e}")
                    continue
                
                # Checkpoint every adaptive interval
                if records_processed % checkpoint_interval == 0:
                    log.info(f"Checkpointing {table} after {records_processed} records")
                    op.checkpoint(state)
        
        # Process deletes
        pk_cols = pk_map_full.get(table, [])
        if pk_cols:
            try:
                del_query = SRC_DEL_RECORDS.format(
                    tableName=table, endDate=state[table], startDate=start_date,
                    joincol=", ".join(pk_cols)
                )
                log.info(f"Delete sync for {table}: {del_query}")
                cursor.execute(del_query)
                while True:
                    drows = cursor.fetchmany(batch_size)  # Use adaptive batch size
                    if not drows:
                        break
                    for drow in drows:
                        try:
                            keys = {}
                            for i, pk_col in enumerate(pk_cols):
                                if isinstance(drow, dict):
                                    keys[pk_col] = drow.get(pk_col)
                                elif isinstance(drow, (list, tuple)) and i < len(drow):
                                    keys[pk_col] = drow[i]
                            if keys:
                                op.delete(table=table, keys=keys)
                                records_processed += 1
                        except Exception as e:
                            log.warning(f"Error processing delete row in table {table}: {e}")
                            continue
            except Exception as e:
                log.warning(f"Error processing deletes for table {table}: {e}")
    
    return records_processed

def process_full_load(table: str, configuration: dict, conn_manager: ConnectionManager, 
                     pk_map: Dict[str, str], threads: int, max_queue_size: int, state: dict):
    """
    Process full load for a table using partitioned approach.
    
    This function handles full data synchronization by partitioning large tables
    and processing them in parallel. It's optimized for AI/ML data pipelines
    with large datasets that need efficient processing.
    
    Args:
        table: Name of the table to sync
        configuration: Database configuration
        conn_manager: Database connection manager
        pk_map: Mapping of tables to their primary key columns
        threads: Number of threads to use for processing
        max_queue_size: Maximum size of the processing queue
        state: Current sync state
        
    Returns:
        Number of records processed
    """
    records_processed = 0
    idx = pk_map.get(table)
    
    # Get total record count
    count_q = SRC_VAL_RECORD_COUNT.format(tableName=table)
    with conn_manager.get_cursor() as cursor:
        cursor.execute(count_q)
        row = cursor.fetchone()
        if row is not None:
            total_rows = safe_get_row_count(row, None, 0)
        else:
            total_rows = 0
        
        # Use adaptive parameters with resource monitoring
        adaptive_params = get_adaptive_parameters_with_monitoring(total_rows, threads, max_queue_size)
        
        partition_size = adaptive_params['partition_size']
        batch_size = adaptive_params['batch_size']
        adaptive_threads = adaptive_params['threads']
        adaptive_queue_size = adaptive_params['queue_size']
        checkpoint_interval = adaptive_params['checkpoint_interval']
        resource_pressure = adaptive_params['resource_pressure']
        
        # Override with configuration if provided, otherwise use adaptive values
        actual_threads = threads if threads > 0 else adaptive_threads
        actual_queue_size = max_queue_size if max_queue_size > 0 else adaptive_queue_size
        
        # Ensure actual_threads never exceeds 4
        if actual_threads > 4:
            log.warning(f"Table {table}: Adaptive threads ({actual_threads}) exceeds maximum allowed (4). Capping at 4 threads.")
            actual_threads = 4
        
        num_partitions = math.ceil(total_rows / partition_size)
        
        # Log table processing parameters with resource context
        log.info(f"Table {table}: {total_rows:,} rows, {num_partitions} partitions, "
                f"{actual_threads} threads, {actual_queue_size} queue size, "
                f"{partition_size:,} partition size, {batch_size:,} batch size, "
                f"{checkpoint_interval:,} checkpoint interval")
        
        if resource_pressure:
            log.info(f"Resource Monitor: Processing {table} with resource-aware parameters due to system pressure")
        if adaptive_params['resource_status']['status'] == 'active':
            log.info(f"Resource Monitor: Memory {adaptive_params['resource_status']['memory_usage']:.1f}%, "
                    f"CPU {adaptive_params['resource_status']['cpu_percent']:.1f}%")
    
    # Generate partition bounds
    bounds_q = SRC_GEN_INDEX_COLUMN_BOUNDS.format(
        tableName=table, indexkey=idx, threads=num_partitions
    )
    with conn_manager.get_cursor() as cursor:
        cursor.execute(bounds_q)
        parts = cursor.fetchall()

    def load_partition(partition_data, q, sentinel):
        """Load a partition into the shared queue."""
        for attempt in range(2):
            try:
                # Create a new connection manager for this partition with table size context
                partition_conn_manager = ConnectionManager(configuration, total_rows)
                with partition_conn_manager.get_cursor() as pc:
                    fl_q = SRC_FL_RECORDS.format(
                        tableName=table,
                        indexkey=idx,
                        lowerbound=partition_data.get('lowerbound', partition_data[1]) if isinstance(partition_data, dict) else partition_data[1],
                        upperbound=partition_data.get('upperbound', partition_data[2]) if isinstance(partition_data, dict) else partition_data[2]
                    )
                    log.info(f"Partition query: {fl_q}")
                    pc.execute(fl_q)
                    cols = safe_get_column_names(pc)
                    if not cols:
                        log.error(f"No column information available for partition in table {table} - skipping partition")
                        break
                    
                    log.debug(f"Processing partition with {len(cols)} columns: {cols[:3]}{'...' if len(cols) > 3 else ''}")
                    
                    while True:
                        rows = pc.fetchmany(batch_size)  # Use adaptive batch size
                        if not rows:
                            break
                        for row in rows:
                            try:
                                rec = {cols[i]: row[i] for i in range(len(cols)) if i < len(row)}
                                q.put(rec)
                            except Exception as e:
                                log.warning(f"Error processing partition row in table {table}: {e}")
                                continue
                break
            except Exception as e:
                log.warning(f"Partition load attempt {attempt+1} failed: {e}. Retrying connection")
                if attempt == 1:
                    raise
        q.put(sentinel)

    sentinel = object()
    q = queue.Queue(maxsize=actual_queue_size)  # Use adaptive queue size
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=actual_threads) as executor:  # Use adaptive threads
        for p in parts:
            executor.submit(load_partition, p, q, sentinel)
        
        finished = 0
        total = len(parts)
        while finished < total:
            item = q.get()
            if item is sentinel:
                finished += 1
            else:
                flat_data = {}
                flatten_dict("", item, flat_data)
                op.upsert(table=table, data=flat_data)
                records_processed += 1
                
                # Checkpoint every adaptive interval
                if records_processed % checkpoint_interval == 0:
                    log.info(f"Checkpointing {table} after {records_processed} records")
                    op.checkpoint(state)
    
    return records_processed

def update(configuration: dict, state: dict):
    """
    Main update function with enhanced error handling and state management.
    
    This function orchestrates the entire data synchronization process,
    including schema discovery, table categorization, and adaptive processing.
    It's designed for AI/ML data pipelines with varying data volumes and
    system resource constraints.
    
    Args:
        configuration: Dictionary containing database connection details
        state: Dictionary containing state from previous sync runs
    """
    # Ensure all configuration values are strings
    configuration = ensure_string_configuration(configuration)
    
    validate_configuration(configuration)
    
    # Configuration parameters
    threads = int(configuration.get("threads", 0))  # 0 means use adaptive
    max_queue_size = int(configuration.get("max_queue_size", 0))  # 0 means use adaptive
    max_retries = int(configuration.get("max_retries", MAX_RETRIES))
    sleep_seconds = float(configuration.get("retry_sleep_seconds", BASE_RETRY_DELAY))
    
    # Ensure threads never exceed 4
    if threads > 4:
        log.warning(f"Requested threads ({threads}) exceeds maximum allowed (4). Capping at 4 threads.")
        threads = 4
    
    # Debug mode
    raw_debug = configuration.get("debug", False)
    debug = (isinstance(raw_debug, str) and raw_debug.lower() == 'true')
    
    # Get schema and table information
    schema_list = schema(configuration)
    pk_map_full = {s['table']: s.get('primary_key', []) for s in schema_list}
    pk_map = {tbl: cols[0] for tbl, cols in pk_map_full.items() if cols}
    
    # Get table list
    # Create initial connection manager for schema discovery
    initial_conn_manager = ConnectionManager(configuration)
    with initial_conn_manager.get_cursor() as cursor:
        query = SCHEMA_TABLE_LIST_QUERY_DEBUG if debug else SCHEMA_TABLE_LIST_QUERY
        cursor.execute(query)
        tables = [safe_get_table_name(r) for r in cursor.fetchall() if safe_get_table_name(r)]
    
    if debug:
        log.info(f"Tables found: {tables}")
    
    # Get table sizes and categorize them
    log.info("Analyzing table sizes for optimal processing order...")
    table_sizes = get_table_sizes(configuration, initial_conn_manager, tables)
    categorized_tables = categorize_and_sort_tables(tables, table_sizes)
    
    # Log processing strategy
    small_count = sum(1 for _, cat, _ in categorized_tables if cat == 'small')
    medium_count = sum(1 for _, cat, _ in categorized_tables if cat == 'medium')
    large_count = sum(1 for _, cat, _ in categorized_tables if cat == 'large')
    
    log.info(f"Processing strategy: {small_count} small tables (<1M), "
            f"{medium_count} medium tables (1M-50M), {large_count} large tables (50M+)")
    
    # Display detailed processing plan
    display_processing_plan(categorized_tables)
    
    # Initialize resource monitoring
    if PSUTIL_AVAILABLE:
        log.info("Resource Monitor: System monitoring enabled - will automatically adjust parameters based on resource pressure")
        initial_status = monitor_resources()
        log.info(f"Resource Monitor: Initial system status - Memory {initial_status.get('memory_usage', 'N/A')}%, "
                f"CPU {initial_status.get('cpu_percent', 'N/A')}%, Disk {initial_status.get('disk_usage', 'N/A')}%")
    else:
        log.info("Resource Monitor: System monitoring disabled - psutil not available")
    
    # Initialize state for tables
    if "last_updated_at" in state:
        ts = state.pop("last_updated_at")
        for t in tables:
            state.setdefault(t, ts)
    
    # Process tables by category (small first, then medium, then large)
    total_tables = len(categorized_tables)
    processed_tables = 0
    
    for table, category, row_count in categorized_tables:
        processed_tables += 1
        start_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        records_processed = 0
        
        log.info(f"Processing table {processed_tables}/{total_tables}: {table} "
                f"({category}, {row_count:,} rows)")
        
        # Create connection manager with table size context for adaptive timeouts
        conn_manager = ConnectionManager(configuration, row_count)
        
        # Retry loop for deadlock and timeout handling
        for attempt in range(max_retries):
            try:
                if debug:
                    log.info(f"Processing table: {table}, start_date: {start_date}, "
                            f"state: {state}, attempt {attempt+1}/{max_retries}")
                
                if table in state:
                    # Incremental sync
                    records_processed = process_incremental_sync(table, configuration, state, conn_manager, pk_map_full)
                else:
                    # Full load
                    records_processed = process_full_load(table, configuration, conn_manager, pk_map, threads, max_queue_size, state)
                
                # Successful completion, exit retry loop
                log.info(f"Successfully processed table {table}: {records_processed} records")
                break
                
            except (DeadlockError, TimeoutError) as e:
                log.warning(f"{type(e).__name__} during sync for {table}, attempt {attempt+1}/{max_retries}: {e}")
                if attempt + 1 >= max_retries:
                    log.severe(f"Max retries exceeded for table {table}: {e}")
                    raise
                
                # Exponential backoff with jitter
                base_backoff = min(sleep_seconds * (2 ** attempt), MAX_RETRY_DELAY)
                delay = random.uniform(base_backoff * 0.5, base_backoff * 1.5)
                log.info(f"Retrying {table} after backoff {delay:.2f}s (attempt {attempt+1}/{max_retries})")
                time.sleep(delay)
                continue
                
            except Exception as e:
                log.severe(f"Unexpected error processing table {table}: {e}")
                raise
        
        # Update state and checkpoint after table completion
        state[table] = start_date
        op.checkpoint(state)
        
        # Record count validation
        try:
            with conn_manager.get_cursor() as cursor:
                cursor.execute(SRC_VAL_RECORD_COUNT.format(tableName=table))
                row = cursor.fetchone()
                if row is not None:
                    count = safe_get_row_count(row, None, 0)
                else:
                    count = 0
            
            op.upsert(table="VALIDATION", data={
                "datetime": datetime.now(timezone.utc).isoformat(),
                "tablename": table,
                "count": count,
                "records_processed": records_processed,
                "category": category,
                "processing_order": processed_tables
            })
            
        except Exception as e:
            log.warning(f"Failed to record validation for table {table}: {e}")
        
        # Progress update
        log.info(f"Completed {processed_tables}/{total_tables} tables. "
                f"Next: {categorized_tables[processed_tables][0] if processed_tables < total_tables else 'None'}")
        
        # Periodic resource monitoring check
        if PSUTIL_AVAILABLE and processed_tables % 5 == 0:  # Check every 5 tables
            log.info("Resource Monitor: Periodic system check...")
            current_status = monitor_resources()
            if current_status.get('status') == 'active':
                log.info(f"Resource Monitor: Current status - Memory {current_status['memory_usage']:.1f}%, "
                        f"CPU {current_status['cpu_percent']:.1f}%, Disk {current_status['disk_usage']:.1f}%")

# CONNECTOR INITIALIZATION

# Initialize the connector with the defined update and schema functions
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, "config.json")
    with open(config_path, "r") as f:
        cfg = json.load(f)
    connector.debug(configuration=cfg)
