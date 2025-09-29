# PostgreSQL Connector with Adaptive Processing and Resource Monitoring

This connector provides enterprise-ready PostgreSQL data ingestion with comprehensive optimization techniques, error handling, and resource management. It features a modular architecture designed for high-volume production environments with complex data processing requirements.

## Connector overview

The PostgreSQL connector with adaptive processing is a high-performance data integration solution that connects to PostgreSQL databases and replicates data to Fivetran destinations. It features intelligent resource management, automatic error recovery, and adaptive processing strategies that optimize performance based on table size and system resources.

The connector uses a modular architecture with dedicated utility modules for configuration management, resource monitoring, adaptive processing, table analysis, connection management, schema discovery, and data processing. This design improves maintainability, testability, and reusability while following Fivetran Connector SDK best practices.

This connector is specifically optimized for:
- High-volume data processing with memory management
- AI/ML data pipelines with complex data structures
- Enterprise environments requiring reliability and scalability
- Time-series data with intelligent timestamp detection
- Large datasets with adaptive batch processing
- Production deployments requiring modular, maintainable code

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
* PostgreSQL ODBC driver:
  * macOS: `brew install unixodbc psqlodbc`
  * Linux: `apt-get install unixodbc odbc-postgresql` or `yum install unixODBC postgresql-odbc`
  * Windows: [PostgreSQL ODBC driver installer](https://www.postgresql.org/ftp/odbc/versions/msi/)

## Getting started

Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

* **Modular architecture** - Organized into 8 specialized utility modules for improved maintainability and testability
* **Adaptive processing** - Automatically adjusts batch sizes, thread counts, and processing strategies based on table size and system resources (refer to `utils/adaptive_processing.py`)
* **Memory optimization** - Processes records immediately without accumulating them in memory to prevent overflow (refer to `utils/data_processor.py`)
* **Advanced error handling** - Automatic recovery from deadlocks, timeouts, and connection issues with exponential backoff (refer to `connector.py` update() function)
* **Resource monitoring** - Real-time CPU and memory monitoring with automatic parameter adjustment (refer to `utils/resource_monitor.py`)
* **Connection management** - Thread-safe database connections with automatic reconnection and timeout handling (refer to `utils/connection_manager.py`)
* **Business logic transformations** - Customizable data transformations before replication (refer to `utils/schema_utils.py` apply_transformations() function)
* **Incremental sync** - State management and checkpointing for efficient incremental data replication
* **Table categorization** - Intelligent sorting and processing order optimization (refer to `utils/table_analysis.py`)
* **Comprehensive logging** - Detailed logging at INFO, WARNING, and SEVERE levels for monitoring and debugging

## Configuration file

The connector uses a `configuration.json` file to define connection parameters and processing options. Here's an example configuration:

```json
{
  "host": "YOUR_POSTGRES_HOST",
  "port": "YOUR_POSTGRES_PORT",
  "database": "YOUR_POSTGRES_DATABASE",
  "username": "YOUR_POSTGRES_USERNAME",
  "password": "YOUR_POSTGRES_PASSWORD",
  "region": "YOUR_POSTGRES_TRANSFORMATION_VALUE",
  "max_retries": "YOUR_POSTGRES_MAX_RETRIES",
  "retry_sleep_seconds": "YOUR_POSTGRES_RETRY_SLEEP_IN_SECONDS"
}
```

### Configuration parameters

#### Required parameters
- `host` - PostgreSQL server hostname or IP address
- `port` - PostgreSQL server port (typically 5432)
- `database` - Database name to connect to
- `username` - PostgreSQL username for authentication
- `password` - PostgreSQL password for authentication

#### Optional parameters
- `region` - Region value for data filtering and transformations (default: none)
- `max_retries` - Maximum retry attempts for failed operations (default: 5)
- `retry_sleep_seconds` - Initial delay between retry attempts in seconds (default: 5)

NOTE: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector:

```
# PostgreSQL connector dependencies
pyodbc==5.0.1  # ODBC driver for PostgreSQL connectivity
psutil==5.9.8  # System resource monitoring (optional but recommended)
```

NOTE: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses standard PostgreSQL username/password authentication. Ensure your PostgreSQL user has the following permissions:

- `SELECT` privilege on all tables to be synced
- `USAGE` privilege on the schema
- Access to `information_schema` for schema discovery

Example grant statements:

```sql
GRANT USAGE ON SCHEMA public TO your_username;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO your_username;
```

## Modular architecture

The connector is organized into a modular architecture for improved maintainability and reusability:

### Core connector file
- **connector.py** - Main connector entry point with schema() and update() functions

### Utility modules (utils/)
- **config.py** - Configuration constants and validation logic
- **resource_monitor.py** - System resource monitoring (CPU, memory, disk)
- **adaptive_processing.py** - Adaptive parameter calculation based on table size and resources
- **table_analysis.py** - Table size estimation and categorization
- **connection_manager.py** - Thread-safe database connection management with auto-reconnect
- **schema_utils.py** - Schema discovery, metadata extraction, and data transformations
- **data_processor.py** - Data processing with adaptive batching and resource awareness
- **\_\_init\_\_.py** - Package exports for easy importing

For detailed documentation of each module, refer to `utils/README.md` and `REFACTORING_GUIDE.md`.

## Data handling

The connector processes data through several stages:

### Schema discovery
- Automatically detects table schemas and column types (refer to `utils/schema_utils.py` build_schema() function)
- Retrieves primary keys from `information_schema` (refer to get_primary_keys() function)
- Handles schema evolution and dynamic column changes
- Creates metadata tables for tracking PostgreSQL schema information

### Adaptive processing
The connector categorizes tables by size and applies appropriate processing strategies (refer to `utils/adaptive_processing.py`):

- **Small tables** (<1M rows): 5,000 batch size, 4 threads, 1M checkpoint interval
- **Medium tables** (1M-50M rows): 2,500 batch size, 2 threads, 500K checkpoint interval  
- **Large tables** (>50M rows): 1,000 batch size, 1 thread, 100K checkpoint interval

Processing order is optimized: small tables first for quick wins, then medium, then large tables.

### Data transformation
- Applies business logic transformations before replication (refer to `utils/schema_utils.py` apply_transformations() function)
- Filters data based on configuration parameters (e.g., region-based filtering)
- Adds sync timestamps for tracking
- Handles NULL values and type conversions

### Memory management
- Uses `cursor.fetchmany()` for batch processing (refer to `utils/data_processor.py` process_table_with_adaptive_parameters() function)
- Processes each record individually with `op.upsert()`
- No memory accumulation patterns
- Automatic resource monitoring and adjustment

## Error handling

The connector implements comprehensive error handling strategies:

### Connection management
- **Automatic Reconnection** - Refer to `utils/connection_manager.py` ConnectionManager class
- **Connection Timeout Handling** - Connections are renewed after 3 hours (refer to `_is_connection_expired()` method)
- **Thread-Safe Operations** - Refer to `get_cursor()` context manager
- **Exponential Backoff** - Retry logic with jitter in `connector.py` update() function

### Resource monitoring
- **Memory Pressure Detection** - Refer to `utils/resource_monitor.py` should_reduce_batch_size() function
- **CPU Pressure Detection** - Refer to should_reduce_threads() function
- **System Monitoring** - Refer to monitor_resources() function
- **Adaptive Response** - Automatic batch size and thread count reduction under pressure

### Recovery strategies
- Automatic retry with exponential backoff and jitter
- Connection renewal for transient issues
- Graceful degradation under resource pressure
- Checkpointing for progress recovery (refer to `op.checkpoint(state)` in update() function)

## Tables created

The connector creates the following tables in your destination:

### Metadata tables
- **postgres_table** - Metadata about PostgreSQL tables (table name, schema, type, timestamp, comment)
  - Primary key: `table_name`, `timestamp`
- **postgres_column** - Metadata about PostgreSQL columns (table name, column name, data type, defaults, nullability, max length, timestamp)
  - Primary key: `table_name`, `column_name`, `timestamp`

### Source tables
Each source table is replicated with the prefix `source_{schema}_{table_name}` and includes:
- `_fivetran_id` - Fivetran-generated surrogate key
- `_fivetran_synced` - Timestamp of last sync
- All original columns from your PostgreSQL tables
- `sync_timestamp` - Custom timestamp added by connector
- `region` - Region field for filtering (if applicable)

Example schema:
```
source_public_customers
├── _fivetran_id (STRING)
├── _fivetran_synced (TIMESTAMP)
├── customer_id (INTEGER)
├── customer_name (STRING)
├── email (STRING)
├── region (STRING)
├── sync_timestamp (FLOAT)
└── created_at (TIMESTAMP)
```

## Pagination

The connector uses server-side cursor pagination with adaptive batch sizing:

- Small tables use 5,000-record batches
- Medium tables use 2,500-record batches
- Large tables use 1,000-record batches

Batch sizes are automatically adjusted based on system resource pressure (refer to `utils/adaptive_processing.py` get_adaptive_parameters_with_monitoring() function).

## Additional files

The connector includes additional documentation files:

* **utils/README.md** – Comprehensive documentation of all utility modules with usage examples
* **REFACTORING_GUIDE.md** – Migration guide showing before/after architecture and customization examples
* **utils/ARCHITECTURE.md** – Visual diagrams showing data flow and module dependencies (if present)

## Performance optimization tips

- **Enable Resource Monitoring** - The connector automatically monitors system resources and adjusts parameters
- **Table Ordering** - The connector processes small tables first, then medium, then large for optimal performance
- **Batch Sizing** - Batch sizes are automatically adjusted based on table size and system resources
- **Memory Management** - The connector is optimized to prevent memory overflow in high-volume scenarios
- **Connection Pooling** - Connection manager reuses connections and reconnects only when expired

## Troubleshooting

### Connection issues
- Verify PostgreSQL server is accessible from your network
- Check that PostgreSQL is listening on the configured port
- Ensure authentication credentials are correct
- Verify ODBC driver is properly installed

### ODBC driver issues
On macOS, if you see `symbol not found` errors:
```bash
brew install unixodbc psqlodbc
pip uninstall pyodbc
LDFLAGS="-L/opt/homebrew/lib" CPPFLAGS="-I/opt/homebrew/include" pip install --no-binary :all: pyodbc
```

### Memory issues
- Enable resource monitoring (installed by default with psutil)
- The connector automatically reduces batch sizes under memory pressure
- Check logs for "CRITICAL MEMORY PRESSURE" or "HIGH MEMORY PRESSURE" warnings

### Performance issues
- Review table categorization in logs
- Check resource monitoring messages for CPU/memory pressure
- Verify network latency to PostgreSQL server
- Consider adjusting batch sizes in `utils/config.py` for your environment

### Import errors
If you see `protobuf` or `runtime_version` import errors:
```bash
pip install --upgrade --force-reinstall fivetran-connector-sdk
```

## Enterprise deployment

This connector is designed for enterprise environments and includes:

- Comprehensive error handling and recovery
- Resource monitoring and automatic adjustment
- Modular architecture for easy customization and maintenance
- Security features including credential protection
- Scalability features for high-volume data processing
- Detailed logging and monitoring capabilities
- Thread-safe operations for concurrent processing

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

### Customization

The modular architecture makes it easy to customize the connector:

- **Adjust resource thresholds** - Edit `utils/config.py` to modify memory/CPU thresholds
- **Change batch sizes** - Modify batch size constants in `utils/config.py`
- **Add transformations** - Extend `apply_transformations()` in `utils/schema_utils.py`
- **Implement filtering** - Customize `should_process_table()` in `utils/schema_utils.py`

For detailed customization examples, refer to `REFACTORING_GUIDE.md`.
