"""
Schema discovery and metadata utilities for PostgreSQL connector.

This module provides functions for discovering database schema,
including tables, columns, primary keys, and comments.
"""

from typing import List
import pyodbc
from fivetran_connector_sdk import Logging as log

from .config import validate_configuration
from .resource_monitor import monitor_resources


def get_primary_keys(cursor, table_name: str, schema_name: str) -> List[str]:
    """
    Get primary key columns for a table.
    
    This function queries the information_schema to find primary key constraints
    for a specific table. If no primary key is found, it defaults to 'id'.
    
    Args:
        cursor: Database cursor for executing queries
        table_name: Name of the table
        schema_name: Schema containing the table
    
    Returns:
        List of primary key column names, defaults to ['id'] if none found
    
    Example:
        >>> pks = get_primary_keys(cursor, 'orders', 'public')
        >>> # Returns ['order_id'] or ['id'] if no PK defined
    """
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


def get_table_columns(cursor, table_name: str, schema_name: str):
    """
    Get column names and types for a table.
    
    This function retrieves detailed column information from information_schema
    including data types, defaults, nullability, and max length.
    
    Args:
        cursor: Database cursor for executing queries
        table_name: Name of the table
        schema_name: Schema containing the table
    
    Returns:
        List of tuples containing column metadata:
        (column_name, data_type, column_default, is_nullable, character_maximum_length)
    
    Example:
        >>> cols = get_table_columns(cursor, 'users', 'public')
        >>> # Returns [('id', 'integer', None, 'NO', None), 
        >>> #          ('name', 'varchar', None, 'YES', 255), ...]
    """
    cursor.execute("""
        SELECT column_name, data_type, column_default, is_nullable, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = ?
        AND table_schema = ?
        ORDER BY ordinal_position
    """, (table_name, schema_name))
    return cursor.fetchall()


def get_table_comment(cursor, table_name: str, schema_name: str):
    """
    Fetch the comment for a given table in PostgreSQL.
    
    This function retrieves table-level comments that may contain
    important metadata or documentation about the table's purpose.
    
    Args:
        cursor: Database cursor for executing queries
        table_name: Name of the table
        schema_name: Schema containing the table
    
    Returns:
        Table comment string if exists, None otherwise
    
    Example:
        >>> comment = get_table_comment(cursor, 'orders', 'public')
        >>> # Returns "Customer order records" or None
    """
    try:
        # Use PostgreSQL system catalog to get table comment
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
        log.warning(
            f"Could not fetch comment for table {schema_name}.{table_name}: {e}"
        )
        return None


def should_process_table(table_name: str, schema_name: str, configuration: dict) -> bool:
    """
    Determine if table should be processed based on configuration.
    
    This function can be extended to implement filtering logic based on
    configuration settings (e.g., include/exclude patterns, schema filters).
    
    Args:
        table_name: Name of the table
        schema_name: Schema containing the table
        configuration: Connector configuration dictionary
    
    Returns:
        True if table should be processed, False otherwise
    
    Note:
        Current implementation processes all tables. Extend this function
        to add filtering logic based on your requirements.
    
    Example:
        >>> config = {"include_schemas": ["public"], "exclude_tables": ["tmp_*"]}
        >>> should_process = should_process_table("orders", "public", config)
    """
    # Always return True to process all tables
    # TODO: Implement filtering logic based on configuration
    # Example: Check if table matches include/exclude patterns
    # Example: Check if schema is in allowed list
    return True


def apply_transformations(row_dict: dict, table_name: str, configuration: dict) -> dict:
    """
    Apply business logic transformations to data.
    
    This function applies custom transformations to row data based on
    business rules. It's called for each row before upserting to destination.
    
    Args:
        row_dict: Dictionary of column names to values for the row
        table_name: Name of the source table
        configuration: Connector configuration dictionary
    
    Returns:
        Transformed row dictionary
    
    Example:
        >>> row = {'region': 'fdx55', 'amount': 100}
        >>> config = {'region': 'North America'}
        >>> transformed = apply_transformations(row, 'orders', config)
        >>> # Returns {'region': 'North America', 'amount': 100}
    
    Note:
        Customize this function to implement your specific business logic.
        Current implementation handles region code transformations for orders table.
    """
    # Example transformation: Replace region codes with values from config
    if table_name.lower() == 'orders':
        if row_dict.get('region') == 'fdx55':
            # Pull region value from configuration
            row_dict['region'] = configuration.get('region')
        elif row_dict.get('region') == 'weg45':
            # Hard-coded transformation example
            row_dict['region'] = 'Europe'
    
    return row_dict


def get_filtered_query(table_name: str, schema_name: str, column_names: List[str]) -> str:
    """
    Get the appropriate query based on table and columns.
    
    This function builds a SELECT query for fetching table data.
    It can be extended to add WHERE clauses, JOINs, or other SQL logic.
    
    Args:
        table_name: Name of the table
        schema_name: Schema containing the table
        column_names: List of column names to select
    
    Returns:
        SQL query string
    
    Example:
        >>> query = get_filtered_query('orders', 'public', ['id', 'amount'])
        >>> # Returns "SELECT id, amount FROM public.orders"
    
    Note:
        Extend this function to add filtering logic (WHERE clauses) based on
        configuration or state for incremental syncs.
    """
    base_query = f"SELECT {', '.join(column_names)} FROM {schema_name}.{table_name}"
    # TODO: Add WHERE clauses for incremental sync based on state
    # Example: base_query += " WHERE updated_at > ?"
    return base_query


def build_schema(configuration: dict):
    """
    Dynamically generate schema including metadata and source tables.
    
    This function discovers all tables in the database and generates
    the Fivetran schema definition including metadata tables and source tables.
    It also performs resource monitoring before schema discovery.
    
    Args:
        configuration: Database connection configuration dictionary
    
    Returns:
        List of schema entries, each containing:
            - table: Table name in destination
            - primary_key: List of primary key columns
            - columns: Optional dictionary of column definitions
    
    Raises:
        pyodbc.Error: If database connection or query fails
    
    Example:
        >>> config = {"host": "localhost", "port": "5432", ...}
        >>> schema_list = build_schema(config)
        >>> # Returns [
        >>> #   {"table": "postgres_table", "primary_key": ["table_name", "timestamp"]},
        >>> #   {"table": "source_public_orders", "primary_key": ["order_id"]},
        >>> #   ...
        >>> # ]
    """
    log.info("Starting schema discovery with resource monitoring...")
    
    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)
    
    # Monitor resources before schema discovery
    resource_status = monitor_resources()
    if resource_status['status'] == 'active':
        log.info(
            f"Resource Monitor: Schema discovery starting - "
            f"Memory {resource_status['memory_usage']:.1f}%, "
            f"CPU {resource_status['cpu_percent']:.1f}%"
        )
    
    # Build ODBC connection string
    conn_str = (
        f"DRIVER=/opt/homebrew/lib/psqlodbcw.so;"
        f"SERVER={configuration['host']};"
        f"PORT={configuration['port']};"
        f"DATABASE={configuration['database']};"
        f"UID={configuration['username']};"
        f"PWD={configuration['password']};"
    )

    # Define metadata tables for tracking PostgreSQL schema information
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

        # Get all user tables (excluding system schemas)
        cursor.execute("""
            SELECT table_name, table_schema
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            AND table_type = 'BASE TABLE'
        """)

        # Add each table to schema with its primary keys
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
