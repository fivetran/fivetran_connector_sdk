"""
Simple MSSQL Connector Example for Fivetran Connector SDK

This connector demonstrates how to fetch data from Microsoft SQL Server and upsert it into destination 
using the Fivetran Connector SDK. It includes hardcoded queries, minimal configuration, and follows 
best practices for easy adoption.

BUSINESS VALUE:
- Reduces data pipeline development time from weeks to hours
- Eliminates manual data extraction and transformation tasks
- Provides real-time data synchronization for business intelligence
- Scales from small datasets to enterprise-level data volumes
- Reduces data engineering maintenance overhead

FEATURES:
- Hardcoded SQL queries (no configuration complexity)
- Limited to 10 tables for demonstration
- Simple incremental sync with state management
- Proper error handling and logging
- Follows Fivetran best practices

UPGRADE PATH FOR THE ENTERPRISE:
This simple connector can be enhanced* with the following optimizations from {Git_Repo}/EHI_at_scale/connector.py:
1. Adaptive Processing: Automatically adjust batch sizes based on table size
2. Resource Monitoring: Monitor CPU/memory usage and adjust processing
3. Connection Management: Handle timeouts and deadlocks gracefully
4. Table Categorization: Process small/medium/large tables optimally
5. Multi-threading: Parallel processing for large datasets
6. Advanced Error Recovery: Retry logic with exponential backoff
7. Performance Metrics: Detailed monitoring and validation

"""

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Import source-specific modules
import json
import pytds
from datetime import datetime
from typing import Dict, List, Any

# Configuration constants
BATCH_SIZE = 1000  # Process 1000 records at a time
CHECKPOINT_INTERVAL = 5000  # Checkpoint every 5000 records
MAX_TABLES = 10  # Limit to 10 tables for demonstration

# UPGRADE TIP: For enterprise use, implement adaptive batch sizing:
# SMALL_TABLE_THRESHOLD = 1000000  # 1M rows
# LARGE_TABLE_THRESHOLD = 50000000  # 50M rows
# def get_adaptive_batch_size(table_size: int) -> int:
#     if table_size < SMALL_TABLE_THRESHOLD:
#         return 5000  # 5K for small tables
#     elif table_size < LARGE_TABLE_THRESHOLD:
#         return 2500  # 2.5K for medium tables
#     else:
#         return 1000  # 1K for large tables

def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    
    Args:
        configuration: A dictionary that holds the configuration settings for the connector.
    
    Raises:
        ValueError: If any required configuration parameter is missing.
    """
    required_configs = [
        "MSSQL_SERVER", 
        "MSSQL_DATABASE", 
        "MSSQL_USER", 
        "MSSQL_PASSWORD", 
        "MSSQL_PORT"
    ]
    
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

def connect_to_mssql(configuration: dict):
    """
    Connect to Microsoft SQL Server using pytds.
    
    BUSINESS VALUE: Reliable database connectivity ensures continuous data flow
    for business operations, reducing downtime and data pipeline failures.
    
    Args:
        configuration: Dictionary containing connection parameters.
    
    Returns:
        pytds connection object.
    
    UPGRADE TIP: For enterprise use, implement ConnectionManager class with:
    - Connection timeout handling (3+ hours for large tables)
    - Automatic reconnection on deadlocks
    - Connection pooling for better performance
    - SSL/TLS certificate validation
    """
    try:
        conn = pytds.connect(
            server=configuration["MSSQL_SERVER"],
            database=configuration["MSSQL_DATABASE"],
            user=configuration["MSSQL_USER"],
            password=configuration["MSSQL_PASSWORD"],
            port=int(configuration["MSSQL_PORT"]),
            validate_host=False
        )
        log.info(f"Successfully connected to MSSQL database: {configuration['MSSQL_DATABASE']}")
        return conn
    except Exception as e:
        log.severe(f"Failed to connect to MSSQL: {e}")
        raise

def get_table_list(connection) -> List[str]:
    """
    Get list of tables from the database, limited to 10 tables.
    
    BUSINESS VALUE: Automated table discovery eliminates manual configuration
    and reduces setup time from hours to minutes, ensuring no data sources are missed.
    
    Args:
        connection: Database connection object.
    
    Returns:
        List of table names.
    
    UPGRADE TIP: For enterprise use, implement table categorization:
    - Get table sizes to categorize as small/medium/large
    - Sort tables by size for optimal processing order
    - Filter tables based on business requirements
    - Support for custom table selection criteria
    """
    # Hardcoded query to get table list
    # You can change this to a WHERE clause to retrieve specific tables. Focus on a few tables to get familiar with this type of replication    
    query = """
    SELECT TOP 10 TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE = 'BASE TABLE' 
    AND TABLE_NAME NOT LIKE 'sys%'
    AND TABLE_NAME NOT LIKE 'INFORMATION_SCHEMA%'
    ORDER BY TABLE_NAME
    """

    # #WHERE clause example
    #query = """
    #SELECT TABLE_NAME 
    #FROM INFORMATION_SCHEMA.TABLES 
    #WHERE TABLE_TYPE = 'BASE TABLE' 
    #AND TABLE_NAME NOT LIKE 'sys%'
    #AND TABLE_NAME NOT LIKE 'INFORMATION_SCHEMA%'
    #AND TABLE_NAME IN ('my_table', 'my_other_table')
    #ORDER BY TABLE_NAME
    #"""

    
    
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        log.info(f"Found {len(tables)} tables: {tables}")
        return tables
    except Exception as e:
        log.severe(f"Failed to get table list: {e}")
        raise

def get_table_schema(connection, table_name: str) -> Dict[str, Any]:
    """
    Get schema information for a specific table.
    
    BUSINESS VALUE: Automatic schema discovery ensures data integrity and
    eliminates manual mapping errors, reducing data quality issues.
    
    Args:
        connection: Database connection object.
        table_name: Name of the table to get schema for.
    
    Returns:
        Dictionary containing table schema information.
    
    UPGRADE TIP: For enterprise use, enhance schema discovery with:
    - Data type mapping and validation
    - Column constraints and relationships
    - Index information for performance optimization
    - Schema change detection and handling
    """
    # Hardcoded query to get primary keys
    pk_query = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE TABLE_NAME = %s
    AND CONSTRAINT_NAME LIKE 'PK_%'
    ORDER BY ORDINAL_POSITION
    """
    
    # Hardcoded query to get all columns
    col_query = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = %s
    ORDER BY ORDINAL_POSITION
    """
    
    try:
        cursor = connection.cursor()
        
        # Get primary keys
        cursor.execute(pk_query, (table_name,))
        primary_keys = [row[0] for row in cursor.fetchall()]
        
        # Get all columns
        cursor.execute(col_query, (table_name,))
        columns = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        
        schema = {
            "table": table_name,
            "primary_key": primary_keys if primary_keys else None,
            "columns": columns
        }
        
        log.info(f"Schema for {table_name}: {len(columns)} columns, {len(primary_keys)} primary keys")
        return schema
        
    except Exception as e:
        log.warning(f"Failed to get schema for table {table_name}: {e}")
        return {"table": table_name, "primary_key": None, "columns": []}

def get_table_data(connection, table_name: str, last_sync_time: str = None) -> List[Dict[str, Any]]:
    """
    Get data from a table with optional incremental sync.
    
    BUSINESS VALUE: Incremental sync reduces processing time and
    minimizes database load, enabling real-time data updates for critical
    business decisions without impacting source system performance.
    
    Args:
        connection: Database connection object.
        table_name: Name of the table to fetch data from.
        last_sync_time: Last sync time for incremental sync.
    
    Returns:
        List of dictionaries containing table data.
    
    UPGRADE TIP: For enterprise use, implement advanced data processing:
    - Partitioned queries for large tables (50K+ records per partition)
    - Multi-threaded data fetching (up to 4 threads)
    - Memory-efficient streaming for very large datasets
    - Custom incremental sync strategies (timestamp, version, etc.)
    - Data transformation and enrichment during extraction
    """
    # Hardcoded query for incremental sync (if table has a timestamp column)
    incremental_query = """
    SELECT * FROM {table_name}
    WHERE ModifiedDate > %s
    ORDER BY ModifiedDate
    """
    
    # Hardcoded query for full load
    full_load_query = """
    SELECT * FROM {table_name}
    ORDER BY (SELECT NULL)
    """
    
    try:
        cursor = connection.cursor()
        
        # Try incremental sync first if we have a last sync time
        if last_sync_time:
            try:
                query = incremental_query.format(table_name=table_name)
                cursor.execute(query, (last_sync_time,))
                log.info(f"Incremental sync for {table_name} from {last_sync_time}")
            except Exception as e:
                log.warning(f"Incremental sync failed for {table_name}, falling back to full load: {e}")
                query = full_load_query.format(table_name=table_name)
                cursor.execute(query)
                log.info(f"Full load for {table_name}")
        else:
            query = full_load_query.format(table_name=table_name)
            cursor.execute(query)
            log.info(f"Full load for {table_name}")
        
        # UPGRADE TIP: For enterprise use, add performance monitoring:
        # - Track query execution time
        # - Monitor memory usage during data fetching
        # - Log performance metrics for optimization
        # - Implement query timeout handling
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Fetch data in batches
        data = []
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            
            for row in rows:
                # Convert row to dictionary
                row_dict = dict(zip(columns, row))
                data.append(row_dict)
        
        # UPGRADE TIP: For enterprise use, implement streaming processing:
        # - Process records one by one instead of loading all into memory
        # - Use generators to yield records as they're processed
        # - Implement memory monitoring and batch size adjustment
        # - Add data validation and transformation during processing
        
        cursor.close()
        log.info(f"Fetched {len(data)} records from {table_name}")
        return data
        
    except Exception as e:
        log.severe(f"Failed to get data from table {table_name}: {e}")
        raise

def schema(configuration: dict):
    """
    Define the schema function which configures the schema your connector delivers.
    
    BUSINESS VALUE: Automated schema discovery eliminates manual data modeling
    and reduces time-to-insight by providing immediate access to all available
    data sources without requiring technical expertise.
    
    Args:
        configuration: A dictionary that holds the configuration settings for the connector.
    
    Returns:
        List of dictionaries defining table schemas.
    
    UPGRADE TIP: For enterprise use, enhance schema function with:
    - Schema versioning and change tracking
    - Custom table filtering and selection
    - Schema validation and quality checks
    - Integration with data catalog systems
    """
    log.info("Starting schema discovery...")
    
    # Validate configuration
    validate_configuration(configuration)
    
    # Connect to database
    connection = connect_to_mssql(configuration)
    
    try:
        # Get list of tables
        tables = get_table_list(connection)
        
        # Get schema for each table
        schemas = []
        for table_name in tables:
            schema_info = get_table_schema(connection, table_name)
            if schema_info:
                schemas.append(schema_info)
        
        log.info(f"Schema discovery complete: {len(schemas)} tables found")
        return schemas
        
    finally:
        connection.close()

def update(configuration: dict, state: dict):
    """
    Define the update function, which is called by Fivetran during each sync.
    
    BUSINESS VALUE: Reliable data synchronization ensures business users always
    have access to the latest information, enabling data-driven decision making
    and reducing the risk of decisions based on stale data.
    
    Args:
        configuration: A dictionary containing connection details.
        state: A dictionary containing state information from previous runs.
    
    UPGRADE TIP: For enterprise use, implement advanced sync features:
    - Retry logic with exponential backoff for failed operations
    - Resource monitoring and adaptive processing
    - Detailed performance metrics and monitoring
    - Graceful error handling and recovery
    - Support for complex data transformations
    """
    log.info("Starting data sync...")
    
    # Validate configuration
    validate_configuration(configuration)
    
    # Connect to database
    connection = connect_to_mssql(configuration)
    
    try:
        # Get list of tables
        tables = get_table_list(connection)
        
        # Process each table
        total_records = 0
        
        for table_name in tables:
            log.info(f"Processing table: {table_name}")
            
            # Get last sync time for this table
            last_sync_time = state.get(table_name)
            
            # Get table data
            table_data = get_table_data(connection, table_name, last_sync_time)
            
            # Process records
            records_processed = 0
            for record in table_data:
                try:
                    # Direct operation call without yield - easier to adopt
                    # The op.upsert method is called with two arguments:
                    # - The first argument is the name of the table to upsert the data into.
                    # - The second argument is a dictionary containing the data to be upserted.
                    op.upsert(table=table_name, data=record)
                    records_processed += 1
                    total_records += 1
                    
                    # Checkpoint every CHECKPOINT_INTERVAL records
                    if records_processed % CHECKPOINT_INTERVAL == 0:
                        log.info(f"Checkpointing {table_name} after {records_processed} records")
                        op.checkpoint(state)
                        
                except Exception as e:
                    log.warning(f"Error processing record in table {table_name}: {e}")
                    continue
            
            # UPGRADE TIP: For enterprise use, add table-level optimizations:
            # - Get table size and adjust processing strategy
            # - Implement table categorization (small/medium/large)
            # - Add table-specific error handling and retry logic
            # - Monitor table processing performance metrics
            
            # Update state with current sync time
            current_time = datetime.utcnow().isoformat() + "Z"
            state[table_name] = current_time
            
            # Checkpoint after processing each table
            op.checkpoint(state)
            
            log.info(f"Completed table {table_name}: {records_processed} records processed")
        
        # Final validation record
        op.upsert(table="SYNC_VALIDATION", data={
            "sync_timestamp": datetime.utcnow().isoformat() + "Z",
            "total_tables": len(tables),
            "total_records": total_records,
            "tables_processed": ", ".join(tables)
        })
        
        log.info(f"Sync complete: {total_records} total records processed from {len(tables)} tables")
        
        # UPGRADE TIP: For enterprise use, enhance validation and monitoring:
        # - Record count validation against source database
        # - Performance metrics (processing time, throughput)
        # - Data quality checks and validation
        # - Integration with monitoring and alerting systems
        # - Detailed sync statistics for business reporting
        
    except Exception as e:
        log.severe(f"Failed to sync data: {e}")
        raise
    finally:
        connection.close()

# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)

# UPGRADE TIP: For enterprise deployment, consider:
# - Environment-specific configuration management
# - Secrets management for database credentials
# - Containerization with Docker for more control on optimization
# - CI/CD pipeline integration for automated testing
# - Monitoring and alerting integration
# - Performance benchmarking and optimization
