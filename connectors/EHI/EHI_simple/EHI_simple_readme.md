# Simple MSSQL Connector Example

## Connector overview

The Simple MSSQL Connector demonstrates how to fetch data from Microsoft SQL Server and upsert it into destination using the Fivetran Connector SDK. This connector provides a simplified, easy-to-adopt example that follows Fivetran best practices for healthcare datasets (like EHR system data, for example Epic Caboodle) while maintaining enterprise-grade functionality.

The connector automatically discovers tables and schemas from your Microsoft SQL Server database and synchronizes data using both full load and incremental sync methods. It includes hardcoded queries, minimal configuration complexity, and comprehensive error handling for reliable data pipeline operations.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- Microsoft SQL Server access with appropriate permissions
- Network connectivity to the SQL Server instance

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Automated table and schema discovery from INFORMATION_SCHEMA
- Incremental data synchronization with timestamp-based filtering
- Full load fallback when incremental sync fails
- Batch processing for optimal memory usage and performance
- Comprehensive error handling with graceful degradation
- State management with automatic checkpointing for sync reliability
- Limited to 10 tables for demonstration purposes
- Hardcoded SQL queries for simplicity and easy adoption

## Configuration file

The connector requires minimal configuration with only essential connection parameters:

```json
{
  "MSSQL_SERVER": "YOUR_SQL_SERVER_HOST_NAME",
  "MSSQL_DATABASE": "YOUR_SQL_SERVER_DATABASE",
  "MSSQL_USER": "YOUR_SQL_SERVER_DATABASE_USER",
  "MSSQL_PASSWORD": "YOUR_SQL_SERVER_USER_PASSWORD",
  "MSSQL_PORT": "YOUR_SQL_SERVER_PORT"
}
```

| Parameter | Description | Required | Example |
|-----------|-------------|----------|---------|
| `MSSQL_SERVER` | SQL Server hostname or IP address | Yes | `server.company.com` |
| `MSSQL_DATABASE` | Database name to connect to | Yes | `HealthcareDB` |
| `MSSQL_USER` | Database username | Yes | `fivetran_user` |
| `MSSQL_PASSWORD` | Database password | Yes | `secure_password` |
| `MSSQL_PORT` | SQL Server port (default: 1433) | Yes | `1433` |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector:

```
pytds==1.9.2
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses SQL Server authentication with username and password credentials. Ensure your database user has the following permissions:

- SELECT permissions on all tables you want to replicate
- Access to INFORMATION_SCHEMA views for automated schema discovery
- Network connectivity to the SQL Server instance

For production deployments, consider implementing Windows Authentication or Azure Active Directory authentication for enhanced security.

## Pagination

The connector implements batch processing to handle large datasets efficiently using the `fetchmany()` method. Data is processed in configurable batches to optimize memory usage and processing performance.

Batch processing strategy - Refer to `__BATCH_SIZE` constant:
- Default batch size: 1,000 records per batch
- Automatic checkpointing every 5,000 records
- Memory-efficient processing prevents system resource exhaustion
- Configurable batch sizes for performance tuning

## Data handling

The connector automatically discovers and maps data from SQL Server to Fivetran using the following process:

Schema discovery - Refer to `schema()` function:
1. Queries `INFORMATION_SCHEMA.TABLES` to identify available tables
2. Discovers primary keys from `INFORMATION_SCHEMA.KEY_COLUMN_USAGE`
3. Maps column names and data types from `INFORMATION_SCHEMA.COLUMNS`
4. Creates table definitions with primary key information

Data synchronization - Refer to `update()` function:
- Full load for initial sync or when no previous state exists
- Incremental sync using timestamp-based filtering (ModifiedDate column)
- Automatic fallback to full load if incremental sync fails
- State management for tracking sync progress and cursor values

Data transformation:
- Automatic conversion of cursor rows to dictionary format
- Null value handling and data type preservation
- Column name mapping and validation

## Error handling

The connector implements comprehensive error handling strategies:

Connection errors - Refer to `_connect_to_mssql()` function:
- Graceful handling of database connection failures
- Detailed error logging for troubleshooting connectivity issues
- Connection parameter validation before attempting connection

Query execution errors - Refer to `_get_table_data()` function:
- Automatic fallback from incremental to full load on query failures
- Exception handling for malformed queries or missing columns
- Logging of specific error details for debugging

Data processing errors - Refer to lines 426-443 in `update()` function:
- Individual record error handling without stopping entire sync operation
- Warning logging for failed record processing with detailed error information
- Continuation of processing for remaining records to maximize data recovery

State management errors - Refer to `op.checkpoint()` calls:
- Safe checkpointing with error recovery mechanisms
- State validation and consistency checks
- Automatic state recovery for interrupted sync operations

## Tables created

The connector creates the following tables in your destination:

**Replicated source tables**:
- Automatically named to match source table names
- Includes all columns from source tables with original data types
- Primary keys preserved for data integrity and upsert operations
- Limited to 10 tables for demonstration purposes

**SYNC_VALIDATION table** - Contains sync metadata and validation information:
- `sync_timestamp` - ISO timestamp of sync completion
- `total_tables` - Number of tables processed in the sync operation
- `total_records` - Total number of records synchronized across all tables
- `tables_processed` - Comma-separated list of successfully processed table names

## Additional considerations

### Enterprise upgrade path

This simple connector can be enhanced with advanced features from the at-scale version:
- Adaptive processing with automatic batch size adjustment based on table size
- Resource monitoring using psutil for CPU and memory optimization
- Connection management with timeout and deadlock handling
- Table categorization for optimal processing order (small/medium/large)
- Multi-threaded processing for parallel data extraction
- Advanced error recovery with exponential backoff retry logic
- Performance metrics and detailed monitoring capabilities

### Performance optimization

Batch processing - Refer to `__BATCH_SIZE` and `__CHECKPOINT_INTERVAL` constants:
- Configurable batch size for optimal memory usage
- Automatic checkpointing prevents data loss during long sync operations
- Efficient data streaming for large datasets

Incremental synchronization - Refer to `_get_table_data()` function:
- Reduces processing time by only syncing changed data
- Minimizes database load and network traffic
- Enables near real-time data updates for business intelligence

### Security considerations

For production deployments, implement the following security measures:
- Use encrypted connections (SSL/TLS) for database communication
- Implement credential rotation and secure credential management
- Restrict database user permissions to minimum required access
- Monitor and audit connector access and data movement
- Implement network security controls and firewall rules

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
