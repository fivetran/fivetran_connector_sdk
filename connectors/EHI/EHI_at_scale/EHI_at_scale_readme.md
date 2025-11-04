# EHI at Scale MSSQL Connector Example

## Connector overview

The EHI at Scale MSSQL Connector is an enterprise-grade solution specifically designed to handle massive healthcare datasets (like EHR system data, for example Epic Caboodle) with intelligent table size categorization and adaptive processing strategies. This connector can efficiently handle tables with 1+ billion rows without timeouts, prevents memory overflow through intelligent resource management, and provides comprehensive visibility into sync progress and performance.

The connector automatically categorizes tables by size (small/medium/large) and applies different processing strategies for optimal performance. It includes advanced features like deadlock detection, automatic timeout recovery, resource monitoring with psutil, and multi-threaded processing with intelligent queue management.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- Microsoft SQL Server access with appropriate permissions
- Network connectivity to the SQL Server instance
- SSL certificates for secure database connections
- Optional: psutil library for resource monitoring and automatic parameter adjustment

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- **Adaptive processing** - Automatically adjusts processing parameters based on table size and system resources
- **Table categorization** - Intelligent grouping of tables into small (<1M rows), medium (1M-50M rows), and large (50M+ rows) categories
- **Resource monitoring** - Real-time system resource monitoring with automatic parameter adjustment (requires psutil)
- **Multi-threaded processing** - Parallel processing with up to 4 threads, automatically scaled based on table size
- **Advanced error handling** - Deadlock detection, timeout recovery, and exponential backoff retry logic  
- **Connection management** - Robust connection handling with automatic reconnection and adaptive timeouts
- **SSL certificate support** - Automatic SSL certificate generation and validation for secure connections
- **Memory optimization** - Intelligent memory usage optimization to prevent system resource exhaustion
- **Progress visibility** - Detailed processing plans and real-time progress updates for large sync operations

## Configuration file

The connector supports both required and optional configuration parameters for maximum flexibility:

```json
{
  "mssql_server": "<YOUR_MSSQL_SERVER>",
  "mssql_cert_server": "<YOUR_MSSQL_CERT_SERVER>",
  "mssql_port": "<YOUR_MSSQL_PORT>",
  "mssql_database": "<YOUR_MSSQL_DATABASE>",
  "mssql_user": "<YOUR_MSSQL_USER>",
  "mssql_password": "<YOUR_MSSQL_PASSWORD>",
  "cert": "<YOUR_SSL_CERTIFICATE>",
  "threads": "<YOUR_THREAD_COUNT>",
  "max_queue_size": "<YOUR_MAX_QUEUE_SIZE>",
  "max_retries": "<YOUR_MAX_RETRIES>",
  "retry_sleep_seconds": "<YOUR_RETRY_SLEEP_SECONDS>",
  "debug": "<YOUR_DEBUG_FLAG>"
}
```

### Required parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `MSSQL_SERVER` | SQL Server hostname or IP address | `server.healthcare.com` |
| `MSSQL_CERT_SERVER` | Certificate server hostname for SSL | `cert-server.healthcare.com` |
| `MSSQL_PORT` | SQL Server port number | `1433` |
| `MSSQL_DATABASE` | Database name to connect to | `EpicCaboodle` |
| `MSSQL_USER` | Database username | `fivetran_enterprise` |
| `MSSQL_PASSWORD` | Database password | `secure_enterprise_password` |

### Optional parameters

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `cert` | SSL certificate content (PEM) or path | Auto-generated | `-----BEGIN CERTIFICATE-----...` |
| `threads` | Max threads for processing (capped at 4) | Adaptive | `4` |
| `max_queue_size` | Maximum processing queue size | Adaptive | `10000` |
| `max_retries` | Maximum retry attempts | `5` | `5` |
| `retry_sleep_seconds` | Base retry delay in seconds | `5` | `5` |
| `debug` | Enable debug logging | `false` | `true` |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector:

```
pytds==1.9.2
requests
psutil
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses SQL Server authentication with SSL certificate validation for secure connections. Authentication credentials are managed through the configuration file with support for automatic SSL certificate generation.

SSL certificate handling - Refer to `_connect_to_mssql()` and `_generate_cert_chain()` functions:
- Automatic SSL certificate chain generation from DigiCert
- Support for custom certificate content in PEM format
- Certificate file path support for existing certificate files
- Secure connection validation and error handling

Required database permissions:
- SELECT permissions on all tables to be replicated
- Access to INFORMATION_SCHEMA views for schema discovery
- Access to sys.tables, sys.indexes, and sys.partitions for table size analysis
- Network connectivity with SSL/TLS support

## Pagination

The connector implements intelligent pagination and partitioning strategies that adapt based on table size and system resources.

Adaptive batch processing - Refer to `_get_adaptive_batch_size()` function:
- Small tables (<1M rows): 5,000 records per batch for maximum throughput
- Medium tables (1M-50M rows): 2,500 records per batch for balanced performance
- Large tables (50M+ rows): 1,000 records per batch for memory conservation

Partitioning strategy - Refer to `_process_full_load()` and `__SRC_GEN_INDEX_COLUMN_BOUNDS` query:
- Complex SQL-based partitioning for parallel processing
- Dynamic partition sizing based on table characteristics
- Thread-safe queue management for partition distribution
- Intelligent partition bounds calculation for optimal load distribution

Resource-aware pagination - Refer to `_get_adaptive_parameters_with_monitoring()` function:
- Real-time memory usage monitoring with automatic batch size reduction
- CPU usage monitoring with automatic thread count adjustment  
- Dynamic parameter adjustment based on system resource pressure

## Data handling

The connector employs sophisticated data handling strategies optimized for large-scale healthcare datasets.

Schema discovery - Refer to `schema()` and `__get_table_schema()` functions:
- Comprehensive table and column discovery using INFORMATION_SCHEMA
- Primary key detection and validation for data integrity
- Column mapping with type preservation
- Error-tolerant schema discovery with graceful degradation

Data processing strategies - Refer to `_process_incremental_sync()` and `_process_full_load()` functions:
- **Incremental sync**: Timestamp-based change detection with upsert and delete operations
- **Full load**: Partitioned parallel processing with intelligent queue management
- **Hybrid approach**: Automatic selection between incremental and full load based on table state

Data transformation and flattening - Refer to `_flatten_dict()` function:
- Automatic flattening of nested dictionary structures for database storage
- Null value handling with N/A substitution for empty values
- JSON serialization for complex list structures
- Type-safe data conversion with error handling

Table categorization and processing order - Refer to `_categorize_and_sort_tables()` function:
- Automatic table size analysis using efficient SQL queries
- Processing order optimization: small → medium → large tables
- Real-time processing plan display with detailed breakdowns
- Progress tracking with completion percentages and estimates

## Error handling

The connector implements enterprise-grade error handling with multiple layers of protection and recovery.

Connection and timeout handling - Refer to `ConnectionManager` class:
- Automatic deadlock detection using pattern matching
- Connection timeout detection and recovery
- Exponential backoff retry logic with jitter
- Thread-safe connection management with context managers

Error classification and recovery - Refer to `__DEADLOCK_PATTERNS` and `__TIMEOUT_PATTERNS`:
- Deadlock error patterns: 'deadlock', 'lock timeout', 'transaction deadlock'
- Timeout error patterns: 'connection timeout', 'network timeout', 'socket timeout'
- Custom exception classes: `DeadlockError` and `TimeoutError` for specific handling
- Automatic retry with different strategies based on error type

Resource management errors - Refer to `_monitor_resources()` and resource threshold constants:
- Memory pressure detection with automatic batch size reduction
- CPU pressure detection with automatic thread count reduction
- System resource monitoring with psutil integration
- Graceful degradation when resource monitoring is unavailable

State persistence and recovery - Refer to checkpoint operations throughout `update()` function:
- Frequent checkpointing every 100K-1M records based on table size
- State validation and consistency checks
- Automatic recovery from interrupted sync operations
- Transaction-safe state updates

## Tables created

The connector creates comprehensive table structures in your destination:

**Replicated source tables**:
- Dynamically named to match source table names
- Complete column preservation with original data types
- Primary key constraints maintained for data integrity
- Support for complex nested data structures through flattening

**VALIDATION table** - Contains detailed sync metadata and performance metrics:
- `datetime` - ISO timestamp of validation record creation
- `tablename` - Name of the processed table
- `count` - Record count from source database validation
- `records_processed` - Number of records actually processed by connector
- `category` - Table size category (small/medium/large)
- `processing_order` - Sequential order of table processing

**System monitoring tables** (when resource monitoring is enabled):
- Real-time resource usage metrics
- Processing performance statistics
- Error frequency and recovery metrics
- Parameter adjustment history

## Additional files

The connector includes several additional files to modularize functionality:

- `config.json` – Local configuration file for testing and development
- `warehouse.db` – DuckDB database file for local testing and validation
- `requirements.txt` – Python dependencies specification
- SSL certificate files (auto-generated) – Temporary certificate files for secure connections

## Additional considerations

### Performance optimization for healthcare data

Table size thresholds - Refer to `__SMALL_TABLE_THRESHOLD` and `__LARGE_TABLE_THRESHOLD` constants:
- Default thresholds optimized for typical EHR database patterns
- Small tables (<1M rows): Patient demographics, lookup tables, configuration data
- Medium tables (1M-50M rows): Encounters, procedures, medications, lab results
- Large tables (50M+ rows): Flowsheet values, user activity logs, audit trails

AI/ML data pipeline optimizations:
- Wide tables with many features: Consider reducing small table threshold by 25%
- High cardinality data: Consider increasing large table threshold by 50%
- Sparse data patterns: Reduce both thresholds by 50% for optimal performance
- Time-series data: Default thresholds work well for temporal healthcare data

Resource monitoring and adaptive processing - Refer to resource threshold constants:
- Memory thresholds: 80% triggers reduction, 90% triggers aggressive reduction
- CPU thresholds: 85% triggers thread reduction, 95% triggers aggressive reduction
- Automatic parameter adjustment prevents system resource exhaustion
- Graceful degradation when psutil library is not available

### Enterprise deployment considerations

Security and compliance:
- SSL/TLS encryption for all database connections
- Certificate validation and secure credential management
- Audit logging for compliance with healthcare regulations (HIPAA, HITECH)
- Network security controls and firewall rule configuration

Monitoring and maintenance:
- Comprehensive logging for operational monitoring and troubleshooting
- Performance metrics tracking for optimization opportunities
- Regular validation of sync completeness and data quality
- Automated alerting for sync failures and performance degradation

Scalability and reliability:
- Horizontal scaling through multiple connector instances
- Database connection pooling for high-throughput scenarios
- Disaster recovery planning with state backup and restoration
- Integration with enterprise monitoring and alerting systems

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
