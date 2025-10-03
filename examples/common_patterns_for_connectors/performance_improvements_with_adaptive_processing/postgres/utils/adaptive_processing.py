"""
Adaptive processing parameter calculation based on table size and resource monitoring.

This module provides functions to calculate optimal processing parameters
(batch size, partition size, threads, etc.) based on table size and current
system resource availability.
"""

from typing import Dict, Any
from fivetran_connector_sdk import Logging as log

from .config import (
    BATCH_SIZE,
    PARTITION_SIZE,
    CHECKPOINT_INTERVAL,
    SMALL_TABLE_THRESHOLD,
    LARGE_TABLE_THRESHOLD,
    resource_state
)
from .resource_monitor import (
    monitor_resources,
    should_reduce_batch_size,
    should_reduce_threads
)


def get_adaptive_partition_size(table_size: int) -> int:
    """
    Get optimal partition size based on table size.
    
    Partitioning helps manage memory usage for large tables by processing
    data in smaller chunks. Larger tables get smaller partitions.
    
    Args:
        table_size: Number of rows in the table
    
    Returns:
        Recommended partition size in rows
        - Small tables (<1M): 50K partition size
        - Medium tables (1M-50M): 25K partition size
        - Large tables (>50M): 5K partition size
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
    
    Batch size determines how many rows are fetched in each database query.
    Smaller batches for larger tables reduce memory pressure.
    
    Args:
        table_size: Number of rows in the table
    
    Returns:
        Recommended batch size in rows
        - Small tables (<1M): 5K batch size
        - Medium tables (1M-50M): 2.5K batch size
        - Large tables (>50M): 1K batch size
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
    
    Queue size determines how many records can be buffered in memory
    during processing. Smaller queues for larger tables.
    
    Args:
        table_size: Number of rows in the table
    
    Returns:
        Recommended queue size in records
        - Small tables (<1M): 10K queue size
        - Medium tables (1M-50M): 5K queue size
        - Large tables (>50M): 1K queue size
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
    
    Thread count determines parallelism in data processing.
    Larger tables get fewer threads to avoid overwhelming the database.
    
    Args:
        table_size: Number of rows in the table
    
    Returns:
        Recommended number of threads
        - Small tables (<1M): 4 threads
        - Medium tables (1M-50M): 2 threads
        - Large tables (>50M): 1 thread
    """
    if table_size < SMALL_TABLE_THRESHOLD:
        return 4  # 4 threads for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return 2  # 2 threads for medium tables
    else:
        return 1  # 1 thread for large tables to avoid overwhelming the DB


def get_adaptive_checkpoint_interval(table_size: int) -> int:
    """
    Get adaptive checkpoint interval based on table size.
    
    Checkpoint interval determines how often state is saved during sync.
    More frequent checkpoints for larger tables ensure better recovery.
    
    Args:
        table_size: Number of rows in the table
    
    Returns:
        Recommended checkpoint interval in records
        - Small tables (<1M): 1M records between checkpoints
        - Medium tables (1M-50M): 500K records between checkpoints
        - Large tables (>50M): 100K records between checkpoints
    """
    if table_size < SMALL_TABLE_THRESHOLD:
        return CHECKPOINT_INTERVAL  # 1M for small tables
    elif table_size < LARGE_TABLE_THRESHOLD:
        return CHECKPOINT_INTERVAL // 2  # 500K for medium tables
    else:
        return CHECKPOINT_INTERVAL // 10  # 100K for large tables


def get_adaptive_parameters_with_monitoring(
    table_size: int, 
    base_threads: int, 
    base_batch_size: int
) -> Dict[str, Any]:
    """
    Get adaptive parameters considering both table size and current resource pressure.
    
    This function combines table-size-based adaptive parameters with real-time
    resource monitoring to determine optimal processing configuration.
    
    Args:
        table_size: Number of rows in the table
        base_threads: Base thread count (not currently used, reserved for future)
        base_batch_size: Base batch size (not currently used, reserved for future)
    
    Returns:
        Dictionary containing:
            - partition_size: Optimal partition size
            - batch_size: Optimal batch size (adjusted for resource pressure)
            - threads: Optimal thread count (adjusted for CPU pressure)
            - queue_size: Optimal queue size
            - checkpoint_interval: Optimal checkpoint interval
            - resource_pressure: Boolean indicating if system is under pressure
            - resource_status: Full resource monitoring status
    
    Example:
        >>> params = get_adaptive_parameters_with_monitoring(5000000, 0, 0)
        >>> print(f"Using {params['threads']} threads, {params['batch_size']} batch size")
    """
    # Get base adaptive parameters based on table size
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
            log.info(
                f"Resource monitoring adjusted batch size to {batch_size:,} "
                f"for table with {table_size:,} rows"
            )
        
        # Check if we need to reduce threads due to CPU pressure
        should_reduce_thread, new_threads = should_reduce_threads(
            resource_status['cpu_percent'], threads
        )
        if should_reduce_thread:
            threads = new_threads
            resource_state['threads_reduced'] = True
            log.info(
                f"Resource monitoring adjusted threads to {threads} "
                f"for table with {table_size:,} rows"
            )
        
        # Log resource-aware parameter selection
        log.info(
            f"Resource-aware parameters for {table_size:,} row table: "
            f"{threads} threads, {batch_size:,} batch size, "
            f"{partition_size:,} partition size"
        )
    
    return {
        'partition_size': partition_size,
        'batch_size': batch_size,
        'threads': threads,
        'queue_size': queue_size,
        'checkpoint_interval': checkpoint_interval,
        'resource_pressure': resource_status.get('memory_pressure', False) or 
                           resource_status.get('cpu_pressure', False),
        'resource_status': resource_status
    }
