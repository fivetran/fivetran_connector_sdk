"""
Data processing functions with adaptive parameters and resource monitoring.

This module handles the core data processing logic including table processing,
progress tracking, and processing plan display.
"""

import time
from datetime import datetime, timezone
from typing import List, Tuple
from fivetran_connector_sdk import Logging as log, Operations as op

from .adaptive_processing import get_adaptive_parameters_with_monitoring
from .schema_utils import get_table_columns, get_filtered_query, apply_transformations
from .connection_manager import ConnectionManager


def process_table_with_adaptive_parameters(
    table_name: str, 
    schema_name: str, 
    configuration: dict, 
    conn_manager: ConnectionManager, 
    state: dict
):
    """
    Process a single table using adaptive parameters based on size and resource monitoring.
    
    This function handles the complete data extraction process for a single table,
    including adaptive batch sizing, resource monitoring, error handling, and checkpointing.
    Uses direct operation calls (no yield) following Fivetran SDK best practices.
    
    Args:
        table_name: Name of the table to process
        schema_name: Schema containing the table
        configuration: Connector configuration dictionary
        conn_manager: ConnectionManager instance for database access
        state: State dictionary for checkpointing
    
    Returns:
        Number of records processed
    
    Example:
        >>> conn_mgr = ConnectionManager(config, table_size=1000000)
        >>> state = {}
        >>> records = process_table_with_adaptive_parameters(
        ...     'orders', 'public', config, conn_mgr, state
        ... )
        >>> # Records are directly upserted without yield
    """
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
        
        log.info(
            f"Processing table {schema_name}.{table_name}: {total_rows:,} rows, "
            f"batch size: {batch_size:,}, checkpoint interval: {checkpoint_interval:,}"
        )
        
        if adaptive_params['resource_pressure']:
            log.info(
                f"Resource Monitor: Processing {table_name} with "
                f"resource-aware parameters"
            )
        
        # Get column information for the table
        with conn_manager.get_cursor() as cursor:
            columns = get_table_columns(cursor, table_name, schema_name)
            if not columns:
                log.warning(
                    f"No columns found for table {schema_name}.{table_name} - skipping"
                )
                return records_processed
                
            # Filter out None column names
            column_names = [col[0] for col in columns if col[0]]
            
            if not column_names:
                log.warning(
                    f"No valid column names found for table "
                    f"{schema_name}.{table_name} - skipping"
                )
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
                            row_dict = apply_transformations(
                                row_dict, table_name, configuration
                            )
                            
                            # Filter based on the transformed region value
                            # This is an example of business logic filtering
                            if 'region' in row_dict and row_dict['region'] == configuration.get('region'):
                                # Direct operation call without yield - easier to adopt
                                # The op.upsert method is called with two arguments:
                                # - table: The name of the destination table
                                # - data: A dictionary containing the data to be upserted
                                op.upsert(
                                    table=f"source_{schema_name}_{table_name}",
                                    data=row_dict
                                )
                                records_processed += 1
                            elif table_name.lower() != 'orders':
                                # For non-orders tables, upsert all records directly
                                op.upsert(
                                    table=f"source_{schema_name}_{table_name}",
                                    data=row_dict
                                )
                                records_processed += 1
                                
                        except Exception as row_error:
                            log.warning(
                                f"Error processing row in table "
                                f"{schema_name}.{table_name}: {row_error}"
                            )
                            continue
                    
                    # Checkpoint every adaptive interval for large tables
                    # Save progress to enable recovery in case of interruptions
                    if records_processed > 0 and records_processed % checkpoint_interval == 0:
                        log.info(
                            f"Checkpointing {table_name} after {records_processed} records"
                        )
                        # Direct checkpoint call - saves state for incremental syncs
                        op.checkpoint(state=state)
                            
            except Exception as query_error:
                log.severe(
                    f"Error executing query for table "
                    f"{schema_name}.{table_name}: {query_error}"
                )
                raise
        
        processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
        log.info(
            f"Completed processing {schema_name}.{table_name}: "
            f"{records_processed} records in {processing_time:.2f}s"
        )
        
    except Exception as e:
        log.severe(f"Error processing table {schema_name}.{table_name}: {e}")
        raise
    
    return records_processed


def display_processing_plan(categorized_tables: List[Tuple[str, str, int]]) -> None:
    """
    Display a detailed processing plan for the sync operation.
    
    This function provides visibility into the sync strategy by showing
    which tables will be processed in which order, grouped by size category.
    
    Args:
        categorized_tables: List of (table_name, category, row_count) tuples
                           from categorize_and_sort_tables()
    
    Example:
        >>> categorized = [
        ...     ('config', 'small', 100),
        ...     ('users', 'medium', 5000000),
        ...     ('events', 'large', 100000000)
        ... ]
        >>> display_processing_plan(categorized)
        >>> # Displays formatted plan showing processing order and statistics
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
    
    # Handle division by zero for empty datasets
    if total_rows > 0:
        log.info(
            f"Small tables: {len(small_tables)} tables, {small_rows:,} rows "
            f"({small_rows/total_rows*100:.1f}%)"
        )
        log.info(
            f"Medium tables: {len(medium_tables)} tables, {medium_rows:,} rows "
            f"({medium_rows/total_rows*100:.1f}%)"
        )
        log.info(
            f"Large tables: {len(large_tables)} tables, {large_rows:,} rows "
            f"({large_rows/total_rows*100:.1f}%)"
        )
    
    log.info("=" * 80)
    log.info("Starting sync process...")
    log.info("=" * 80)
