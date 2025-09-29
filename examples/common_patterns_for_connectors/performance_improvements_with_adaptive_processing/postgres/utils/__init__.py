"""
Utility modules for PostgreSQL Connector with adaptive processing and resource monitoring.

This package provides modular components for building production-ready Fivetran connectors
with advanced features like resource monitoring, adaptive processing, and connection management.
"""

# Make key components easily importable
from .config import (
    BATCH_SIZE,
    PARTITION_SIZE,
    CHECKPOINT_INTERVAL,
    CONNECTION_TIMEOUT_HOURS,
    MAX_RETRIES,
    BASE_RETRY_DELAY,
    MAX_RETRY_DELAY,
    SMALL_TABLE_THRESHOLD,
    LARGE_TABLE_THRESHOLD,
    MEMORY_THRESHOLD_HIGH,
    MEMORY_THRESHOLD_CRITICAL,
    CPU_THRESHOLD_HIGH,
    CPU_THRESHOLD_CRITICAL,
    resource_state,
    validate_configuration
)

from .resource_monitor import (
    monitor_resources,
    should_reduce_batch_size,
    should_reduce_threads
)

from .adaptive_processing import (
    get_adaptive_partition_size,
    get_adaptive_batch_size,
    get_adaptive_queue_size,
    get_adaptive_threads,
    get_adaptive_checkpoint_interval,
    get_adaptive_parameters_with_monitoring
)

from .table_analysis import (
    get_table_sizes,
    categorize_and_sort_tables
)

from .connection_manager import ConnectionManager

from .schema_utils import (
    get_primary_keys,
    get_table_columns,
    get_table_comment,
    should_process_table,
    apply_transformations,
    get_filtered_query,
    build_schema
)

from .data_processor import (
    process_table_with_adaptive_parameters,
    display_processing_plan
)

__all__ = [
    # Config
    'BATCH_SIZE',
    'PARTITION_SIZE',
    'CHECKPOINT_INTERVAL',
    'CONNECTION_TIMEOUT_HOURS',
    'MAX_RETRIES',
    'BASE_RETRY_DELAY',
    'MAX_RETRY_DELAY',
    'SMALL_TABLE_THRESHOLD',
    'LARGE_TABLE_THRESHOLD',
    'MEMORY_THRESHOLD_HIGH',
    'MEMORY_THRESHOLD_CRITICAL',
    'CPU_THRESHOLD_HIGH',
    'CPU_THRESHOLD_CRITICAL',
    'resource_state',
    'validate_configuration',
    # Resource Monitor
    'monitor_resources',
    'should_reduce_batch_size',
    'should_reduce_threads',
    # Adaptive Processing
    'get_adaptive_partition_size',
    'get_adaptive_batch_size',
    'get_adaptive_queue_size',
    'get_adaptive_threads',
    'get_adaptive_checkpoint_interval',
    'get_adaptive_parameters_with_monitoring',
    # Table Analysis
    'get_table_sizes',
    'categorize_and_sort_tables',
    # Connection Manager
    'ConnectionManager',
    # Schema Utils
    'get_primary_keys',
    'get_table_columns',
    'get_table_comment',
    'should_process_table',
    'apply_transformations',
    'get_filtered_query',
    'build_schema',
    # Data Processor
    'process_table_with_adaptive_parameters',
    'display_processing_plan',
]
