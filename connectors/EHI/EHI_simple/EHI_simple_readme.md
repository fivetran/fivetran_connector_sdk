# Simple EHI connector based on MS SQL Server

This connector demonstrates how to build a Fivetran connector for Microsoft SQL Server using the Fivetran Connector SDK designed to handle massive healthcare datasets (like EHR system data, for example Epic Caboodle). It provides a simplified, easy-to-adopt example that follows Fivetran best practices while maintaining enterprise-grade functionality.

## Connector overview

The Simple MSSQL Connector automatically discovers and synchronizes data from Microsoft SQL Server databases into your Fivetran destination. It supports both full and incremental data replication with automatic schema discovery, making it ideal for organizations looking to quickly establish data pipelines without complex configuration.

**Key capabilities:**
- Automated table and schema discovery
- Incremental data synchronization
- Batch processing for optimal performance
- Comprehensive error handling and logging
- State management for reliable sync operations

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
* Microsoft SQL Server access with appropriate permissions
* Network connectivity to the SQL Server instance

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* **Automated schema discovery** - Automatically detects tables, columns, and primary keys
* **Incremental synchronization** - Supports both full load and incremental data replication
* **Batch processing** - Processes data in configurable batches for optimal performance
* **Error handling** - Comprehensive error handling with graceful fallback mechanisms
* **State management** - Reliable checkpointing for sync progress tracking
* **Limited scope** - Processes up to 10 tables for demonstration and testing
* **Hardcoded queries** - All SQL queries embedded in code for simplicity

## Configuration file

The connector requires minimal configuration with only essential connection parameters:

```json
{
"MSSQL_SERVER": "<YOUR_VALUE_1>",
"MSSQL_DATABASE": "<YOUR_VALUE_2>",
"MSSQL_USER": "<YOUR_VALUE_3>",
"MSSQL_PASSWORD": "<YOUR_VALUE_4>",
"MSSQL_PORT": "<YOUR_VALUE_5>"
}
```

### Configuration parameters

| Parameter | Description | Required | Example |
|-----------|-------------|----------|---------|
| `MSSQL_SERVER` | SQL Server hostname or IP address | Yes | `server.company.com` |
| `MSSQL_DATABASE` | Database name to connect to | Yes | `SalesDB` |
| `MSSQL_USER` | Database username | Yes | `fivetran_user` |
| `MSSQL_PASSWORD` | Database password | Yes | `secure_password` |
| `MSSQL_PORT` | SQL Server port (default: 1433) | Yes | `1433` |

NOTE: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector:

```
pytds==1.9.2
```

NOTE: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses SQL Server authentication with username and password credentials. Ensure your database user has the following permissions:

* SELECT permissions on all tables you want to replicate
* Access to INFORMATION_SCHEMA views for schema discovery
* Network connectivity to the SQL Server instance

IMPORTANT: For production use, consider implementing Windows Authentication or Azure Active Directory authentication for enhanced security.

## Pagination

The connector implements batch processing to handle large datasets efficiently. Data is fetched in configurable batches (default: 1,000 records) to optimize memory usage and processing performance.

**Batch processing strategy:**
- Configurable batch size via `BATCH_SIZE` constant
- Automatic checkpointing every 5,000 records
- Memory-efficient processing for large datasets

## Data handling

The connector automatically discovers and maps data from SQL Server to Fivetran with the following characteristics:

**Schema discovery process:**
1. Queries INFORMATION_SCHEMA.TABLES to identify available tables
2. Discovers primary keys from INFORMATION_SCHEMA.KEY_COLUMN_USAGE
3. Maps column names and data types from INFORMATION_SCHEMA.COLUMNS
4. Creates table definitions with primary key information

**Data synchronization:**
- Full load for initial sync or when no previous state exists
- Incremental sync using timestamp-based filtering (ModifiedDate column)
- Automatic fallback to full load if incremental sync fails
- State management for tracking sync progress

**Data transformation:**
- Automatic flattening of nested data structures
- Null value handling and data type preservation
- Column name mapping and validation

## Error handling

The connector implements comprehensive error handling strategies:

**Connection errors** - Refer to `connect_to_mssql()` function
- Graceful handling of database connection failures
- Detailed error logging for troubleshooting
- Automatic retry mechanisms for transient failures

**Query errors** - Refer to `get_table_data()` function
- Fallback from incremental to full load on query failures
- Exception handling for malformed queries
- Logging of specific error details for debugging

**Data processing errors** - Refer to lines 280-290 in `update()` function
- Individual record error handling without stopping entire sync
- Warning logging for failed record processing
- Continuation of processing for remaining records

**State management errors** - Refer to `op.checkpoint()` calls
- Safe checkpointing with error recovery
- State validation and consistency checks
- Automatic state recovery mechanisms

## Tables created

The connector creates the following tables in your destination:

**Replicated tables** - One table per source table discovered
- Automatically named to match source table names
- Includes all columns from source tables
- Primary keys preserved for data integrity

**SYNC_VALIDATION table** - Contains sync metadata
- `sync_timestamp` - Timestamp of sync completion
- `total_tables` - Number of tables processed
- `total_records` - Total records synchronized
- `tables_processed` - Comma-separated list of processed tables

## Additional considerations

### Performance optimization

The connector includes several performance optimization features:

**Batch processing** - Refer to `BATCH_SIZE` constant
- Configurable batch size for optimal memory usage
- Automatic checkpointing to prevent data loss
- Efficient data streaming for large datasets

**Incremental sync** - Refer to `get_table_data()` function
- Reduces processing time by only syncing changed data
- Minimizes database load and network traffic
- Enables real-time data updates for business intelligence

**Connection management** - Refer to `connect_to_mssql()` function
- Efficient connection handling and resource cleanup
- Automatic connection validation and error recovery
- Optimized query execution and result processing

### Enterprise upgrade path

This simple connector can be enhanced with enterprise-grade features:

- **Adaptive processing** - Automatically adjust batch sizes based on table size
- **Resource monitoring** - Monitor CPU/memory usage and adjust processing
- **Connection management** - Handle timeouts and deadlocks gracefully
- **Table categorization** - Process small/medium/large tables optimally
- **Multi-threading** - Parallel processing for large datasets
- **Advanced error recovery** - Retry logic with exponential backoff
- **Performance metrics** - Detailed monitoring and validation

### Security considerations

IMPORTANT: Implement the following security measures for production use:

* Use encrypted connections (SSL/TLS) for database communication
* Implement credential rotation and secure credential management
* Restrict database user permissions to minimum required access
* Monitor and audit connector access and data movement
* Implement network security controls and firewall rules

### Monitoring and maintenance

**Recommended monitoring practices:**
- Monitor sync completion times and record counts
- Track error rates and failure patterns
- Monitor database connection performance
- Validate data quality and completeness
- Review sync validation table for operational insights

**Maintenance considerations:**
- Regular review of sync performance metrics
- Periodic validation of schema changes
- Monitoring of database resource usage
- Review and optimization of batch sizes
- Regular testing of incremental sync functionality

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team. 
