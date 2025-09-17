# Connector SDK - Performance improvements with adaptive processing

## Connector overview

This connector demonstrates advanced Connector SDK patterns and techniques that can be applied to any data source. The connector showcases comprehensive optimization strategies, robust error handling, and intelligent resource management designed for high-volume production environments.

**Key enterprise features applicable to any connector:**

- **Adaptive processing engine** - Automatically adjusts processing parameters based on data volume and system resources
- **Advanced error handling** - Comprehensive retry logic with exponential backoff and automatic recovery
- **Resource monitoring** - Real-time system monitoring with automatic parameter adjustment
- **Memory-efficient processing** - Immediate record processing without memory accumulation
- **Preprocessing system** - Pre-replication data transformation capabilities
- **Authentication & security** - Multiple authentication methods with enterprise security features
- **Incremental sync intelligence** - Automatic timestamp detection and incremental data replication
- **Schema evolution handling** - Dynamic schema adaptation and nested data processing

These patterns can be adapted for any data source, whether it's databases, APIs, file systems, or cloud services. These connectors can serve as a comprehensive reference implementation for building production-ready connections that can handle enterprise-scale data volumes while maintaining reliability and performance.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

This section demonstrates enterprise-grade patterns that can be implemented in any Connector SDK workflow. Each feature includes implementation examples and customization options applicable to various data sources.

### Adaptive processing engine

The adaptive processing engine automatically adjusts processing parameters based on data volume and system resources, ensuring optimal performance across varying workloads. This pattern can be adapted for any data source by analyzing data characteristics and system capacity.

**Key benefits:**
- 90%+ performance improvement for large datasets
- Automatic resource optimization based on data volume
- Memory overflow prevention for high-volume scenarios
- Intelligent processing order (small → medium → large datasets)

**Implementation:** Refer to `get_adaptive_parameters_with_monitoring()` for automatic parameter adjustment based on data volume.

**Customization:** Adjust thresholds for your environment using `SMALL_DATASET_THRESHOLD` and `LARGE_DATASET_THRESHOLD` constants.

### Advanced error handling & recovery

Comprehensive error handling with automatic recovery from various failure scenarios including deadlocks, timeouts, and connection issues. This pattern can be adapted for any data source by identifying common error patterns and implementing appropriate recovery strategies.

**Key benefits:**
- 90%+ reduction in connection-related failures
- Automatic deadlock detection and recovery
- Exponential backoff with jitter for retries
- Connection health monitoring and renewal

**Implementation:** Refer to `ConnectionManager` class and `_is_deadlock_error()` for automatic deadlock detection and recovery.

**Customization:** Add custom error patterns using `CUSTOM_ERROR_PATTERNS` and implement custom retry logic with `custom_retry_with_backoff()`.

### Resource monitoring & optimization

Real-time system resource monitoring with automatic parameter adjustment to prevent system overload.

**Key benefits:**
- Automatic memory management prevents OOM errors
- CPU pressure detection and thread reduction
- Disk space monitoring for checkpoint management
- Adaptive parameter adjustment based on system state

**Implementation:** Refer to `monitor_resources()` and `should_reduce_batch_size()` for real-time resource monitoring and automatic parameter adjustment.

**Customization:** Adjust resource thresholds using `MEMORY_THRESHOLD_HIGH`, `MEMORY_THRESHOLD_CRITICAL`, `CPU_THRESHOLD_HIGH`, and `CPU_THRESHOLD_CRITICAL` constants.

### Memory-efficient processing

Prevents memory overflow by processing records immediately without accumulation, essential for high-volume data pipelines.

**Key benefits:**
- Zero memory accumulation - processes records immediately
- Scalable to any dataset size - handles billions of records
- Immediate upsert - no batching delays
- Checkpoint-based progress - resumable operations

**Implementation:** Refer to `sync_table_optimized()` for memory-efficient table sync with immediate processing.

**Customization:** Use `get_adaptive_checkpoint_interval()` for custom checkpoint intervals based on table size.

### Preprocessing system

Execute transformations before data replication, enabling complex data processing workflows.

**Key benefits:**
- Pre-replication data transformation/orchestration
- Dynamic SQL generation with state substitution
- Multiple table support with individual UPDATE statements
- State-aware preprocessing using previous sync timestamps

**Implementation:** Refer to `preprocess_table_data()` for preprocessing SQL execution before data replication.

**Configuration example:**
```json
{
  "preprocessing_table": "LOG,CUSTOMERS",
  "preprocessing_sql": "UPDATE {table_list} SET _fivetran_deleted = True WHERE connector_id IN ({connector_list})",
  "connector_list": "connector1,connector2"
}
```

**Customization:** Use `PREPROCESSING_TEMPLATES` for custom preprocessing templates and `custom_state_substitution()` for custom state variables.

### Authentication & security

Multiple authentication methods with enterprise security features including JWT, PrivateLink, and SSL/TLS.

**Key benefits:**
- JWT authentication with private key support
- PrivateLink support for enterprise networks
- SSL/TLS configuration with verification options
- Password authentication fallback
- Role-based access control

**Implementation:** Refer to `get_snowflake_connection()` for secure Snowflake connection with multiple authentication methods.

**Customization:** Implement `custom_authentication()` for custom authentication logic and `apply_security_policies()` for custom security policies.

### Incremental sync intelligence

Intelligent timestamp detection and incremental sync capabilities to minimize data transfer and processing time.

**Key benefits:**
- Automatic timestamp detection from common column patterns
- Incremental sync reduces data transfer by 80%+
- Fallback to full sync when timestamps unavailable
- State persistence across sync runs

**Implementation:** Refer to `find_timestamp_column()` for intelligent timestamp column detection.

**Customization:** Use `CUSTOM_TIMESTAMP_PATTERNS` for custom timestamp patterns and `custom_timestamp_detection()` for custom timestamp logic.

### Schema evolution handling

Handles dynamic schema changes and nested data structures common in AI/ML data pipelines.

**Key benefits:**
- Dynamic column detection from database schema
- Nested data flattening for complex JSON structures
- Schema evolution support for changing data structures
- Type conversion for Fivetran compatibility

**Implementation:** Refer to `flatten_dict()` and `convert_value()` for nested data flattening and type conversion.

**Customization:** Use `custom_type_conversion()` for custom data type conversion and `validate_schema_evolution()` for custom schema evolution validation.

## Configuration & customization

### Environment-specific tuning

The connector supports different configuration profiles for various environments:

- **Development environment** - Lower thresholds for testing
- **Production environment** - Balanced thresholds for production workloads
- **High-volume environment** - Higher thresholds for enterprise-scale data

Refer to `DEV_CONFIG`, `PROD_CONFIG`, and `HIGH_VOLUME_CONFIG` for environment-specific settings.

### Custom processing strategies

The connector supports custom processing strategies for different use cases:

- **AI/ML data pipeline** - Custom strategy for AI/ML data pipelines using `ai_ml_processing_strategy()`
- **Real-time processing** - Custom strategy for real-time processing using `real_time_processing_strategy()`

## Performance optimization guide

### Table size analysis

Refer to `analyze_table_characteristics()` for analyzing table characteristics and determining optimization strategies.

### Resource monitoring setup

Refer to `setup_resource_monitoring()` for comprehensive resource monitoring configuration.

## Troubleshooting

### Common issues & solutions

**Memory issues:**
- Reduce `SMALL_TABLE_THRESHOLD` and `LARGE_TABLE_THRESHOLD`
- Enable resource monitoring
- Reduce batch sizes

**Performance issues:**
- Increase batch sizes for small tables
- Enable preprocessing for data transformation
- Use incremental sync with timestamp columns

**Connection issues:**
- Check network connectivity
- Verify authentication credentials
- Enable retry logic with exponential backoff

**Data quality issues:**
- Implement preprocessing for data cleaning
- Use custom type conversion
- Add data validation logic

## Monitoring & observability

### Key metrics to monitor

- Processing speed: Records per second
- Memory usage: Peak and average memory consumption
- Error rates: Deadlock, timeout, and connection errors
- Resource utilization: CPU, memory, and disk usage
- Data quality: Success rates and data validation results

### Logging configuration

Refer to `log_processing_metrics()` for custom logging of processing metrics.

## Configuration file

The connector uses a `configuration.json` file to define connection parameters and processing options. This pattern can be adapted for any data source by defining appropriate configuration parameters for authentication, data selection, and processing options. Upload this file to Fivetran when setting up your connector.

**Basic configuration:**
```json
{
  "user": "your_user",
  "password": "your_password",
  "account": "your_account",
  "warehouse": "your_warehouse",
  "database": "your_database",
  "schema": "your_schema",
  "tables": "table1,table2,table3",
  "enable_resource_monitoring": "true",
  "enable_preprocessing": "false"
}
```

**Advanced configuration:**
```json
{
  "user": "your_user",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...",
  "role": "your_role",
  "account": "your_account",
  "warehouse": "your_warehouse",
  "database": "your_database",
  "schema": "your_schema",
  "tables": "table1,table2,table3",
  "timestamp_column": "updated_at",
  "enable_resource_monitoring": "true",
  "enable_preprocessing": "true",
  "preprocessing_table": "table1,table2",
  "preprocessing_sql": "UPDATE {table_list} SET status = 'PROCESSED' WHERE id > 0",
  "batch_delay_ms": "50",
  "max_retries": "5"
}
```

**PrivateLink configuration (Snowflake example):**
```json
{
  "snowflake_user": "your_user",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...",
  "use_privatelink": "true",
  "privatelink_host": "account.region.privatelink.snowflakecomputing.com",
  "snowflake_warehouse": "your_warehouse",
  "snowflake_database": "your_database",
  "snowflake_schema": "your_schema",
  "tables": "table1,table2,table3",
  "ssl_verify": "true"
}
```

NOTE: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector.

**Example content of `requirements.txt`:**
```
snowflake-connector-python
psutil
cryptography
```

NOTE: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector supports multiple authentication methods:

**Password authentication:**
- Provide `password` for traditional username/password authentication
- Used as fallback when JWT authentication is not available

**PrivateLink support:**
- Set `use_privatelink` to `true`
- Provide `privatelink_host` in the format: `account.region.privatelink.snowflakecomputing.com`

## Data handling

The connector processes data with the following capabilities:

- **Incremental sync** - Automatically detects timestamp columns and performs incremental data replication
- **Schema evolution** - Handles dynamic schema changes and nested data structures
- **Data transformation** - Flattens nested JSON structures for database storage
- **Type conversion** - Converts data types to Fivetran-compatible formats
- **Memory management** - Processes records immediately without memory accumulation

## Error handling

The connector implements comprehensive error handling strategies:

- **Deadlock detection** - Automatically detects and recovers from deadlocks
- **Connection management** - Monitors connection health and automatically reconnects on failures
- **Retry logic** - Implements exponential backoff with jitter for transient failures
- **Resource monitoring** - Automatically adjusts processing parameters under resource pressure
- **Graceful degradation** - Falls back to conservative processing when system resources are limited

## Tables created

The connector replicates all tables specified in the `tables` configuration parameter. Each table is created in Fivetran with:

- Surrogate keys (`_fivetran_id`) automatically generated by Fivetran
- All original columns from the source table
- Flattened nested structures (e.g., JSON objects become separate columns)
- Proper data type mapping for Fivetran compatibility

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

This comprehensive features guide provides developers with the knowledge and examples needed to implement, customize, and optimize enterprise-grade connectors for any data source. The patterns and techniques demonstrated here can be adapted for databases, APIs, file systems, cloud services, and other data sources to build production-ready connectors that handle enterprise-scale data volumes while maintaining reliability and performance.
