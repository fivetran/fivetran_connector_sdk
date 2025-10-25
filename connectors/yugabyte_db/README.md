# YugabyteDB Connector Example

## Connector overview
This connector fetches data from YugabyteDB database and syncs it to Fivetran destinations. YugabyteDB is a distributed SQL database that is PostgreSQL-compatible, making it possible to use standard PostgreSQL drivers for connectivity. The connector automatically discovers all tables in the specified schema, extracts their primary keys, and performs incremental syncs based on the `updated_at` column when available.

YugabyteDB is particularly popular for IoT sensor data, real-time analytics, time-series monitoring, and distributed OLTP applications. This example demonstrates syncing IoT sensor data from an industrial equipment monitoring system, which is a common use case for YugabyteDB deployments.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Automatic table discovery from specified schema
- Primary key detection for each table using PostgreSQL system catalogs
- Incremental sync support using `updated_at` column when available
- Server-side cursor streaming for memory-efficient data retrieval
- Batch processing with configurable batch size (100 records per batch)
- Periodic checkpointing every 1000 records for fault tolerance and resumability
- Per-table state tracking for independent sync progress
- Proper datetime to ISO string conversion for all timestamp fields
- Comprehensive error handling with specific exception types
- Detailed logging at key sync stages
- Configuration validation before sync execution
- PostgreSQL-compatible connection using psycopg2 driver

## Configuration file
The connector requires database connection credentials to access YugabyteDB.

```
{
  "host": "<YOUR_YUGABYTEDB_HOST>",
  "port": "<YOUR_YUGABYTEDB_PORT_DEFAULT_5433>",
  "database": "<YOUR_YUGABYTEDB_DATABASE_NAME>",
  "user": "<YOUR_YUGABYTEDB_USERNAME>",
  "password": "<YOUR_YUGABYTEDB_PASSWORD>",
  "schema": "<YOUR_SCHEMA_NAME_DEFAULT_PUBLIC>"
}
```

**Configuration Parameters:**
- `host` (required) - Hostname or IP address of your YugabyteDB server
- `port` (optional) - Port number for YugabyteDB connection (default: 5433)
- `database` (required) - Name of the database to connect to
- `user` (required) - Username for database authentication
- `password` (required) - Password for database authentication
- `schema` (optional) - Database schema to sync tables from (default: public)

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
The connector requires the `psycopg2-binary` package for PostgreSQL-compatible database connectivity with YugabyteDB.

```
psycopg2-binary
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
This connector uses username and password authentication to connect to YugabyteDB. The credentials are specified in the configuration file and passed to the `psycopg2.connect()` function (refer to the `create_connection()` function).

To set up authentication:

1. Create a YugabyteDB user with appropriate permissions to access the required tables.
2. Provide the username and password in the `configuration.json` file.
3. Ensure the user has `SELECT` permissions on the tables in the schema that need to be synced.

## Data handling
The connector processes data using an optimized streaming approach to manage memory efficiently:
- Tables are discovered dynamically from the database schema using `information_schema`
- Primary keys are automatically detected for each table using PostgreSQL system catalogs
- **Server-side cursors**: Uses named cursors with `itersize=100` to stream data from database without loading everything into memory
- **Batch processing**: Records are accumulated in batches of 100 before upserting to destination
- **Periodic checkpointing**: State is saved every 1000 records to enable resume on interruption
- **Type normalization**: Datetime objects are converted to ISO format strings via `normalize_record()` function
- **Incremental sync detection**: Automatically checks for `updated_at` column using `check_incremental_column()` function
- **Timestamp tracking**: Latest timestamp is tracked during iteration to maintain cursor position
- Each record is upserted individually based on the table's primary key
- Per-table state tracking maintains independent sync timestamps for each table

Refer to the `sync_table()` function for detailed batch processing and checkpointing logic, `normalize_record()` for type conversion, and `update()` function for overall state management.

## Error handling
The connector implements comprehensive error handling with specific exception types:
- **Configuration validation errors**: Caught as `ValueError` with clear messages about missing parameters
- **Connection errors**: `psycopg2.OperationalError` caught with guidance to check host, port, and credentials
- **Query errors**: `psycopg2.ProgrammingError` caught with guidance about table existence and permissions
- **General database errors**: Other `psycopg2.Error` exceptions caught and logged
- **Graceful cleanup**: Connection closure in finally block with error handling for cleanup failures
- **Detailed logging**: All errors logged using SDK logging framework (`log.severe()`, `log.warning()`) before raising
- **Descriptive error messages**: Runtime exceptions include context about what failed and how to fix it

Refer to the `create_connection()` function for connection error handling, `validate_configuration()` for config validation, and the `update()` function for comprehensive sync error handling.

## Tables created
The connector dynamically replicates all tables found in the specified schema. Each table retains its original name and structure from the source database.

Table schemas are automatically inferred by Fivetran, with primary keys explicitly defined during schema discovery. For tables with an `updated_at` column, incremental syncing is enabled. Tables without this column perform full syncs on each run.

**Example table structures for IoT use case:**

When using the provided `setup_test_data.sql`, the following tables are created:

1. **devices** - IoT sensor devices
  - Primary key: `device_id`
  - Incremental sync enabled via `updated_at`

2. **sensor_readings** - Time-series sensor measurements
  - Primary key: `reading_id`
  - Incremental sync enabled via `updated_at`
  - Contains temperature, pressure, humidity, vibration, and power consumption data

3. **alerts** - Equipment alerts and warnings
  - Primary key: `alert_id`
  - Incremental sync enabled via `updated_at`
  - Tracks alert severity, acknowledgment, and resolution status

4. **maintenance_log** - Maintenance history
  - Primary key: `maintenance_id`
  - Incremental sync enabled via `updated_at`
  - Records maintenance activities, duration, and costs

## Setting up YugabyteDB for testing

### Local setup using Docker
The easiest way to set up YugabyteDB locally is using Docker:

1. Pull and run the YugabyteDB Docker image:
```bash
docker run -d --name yugabyte \
  -p 7000:7000 -p 9000:9000 -p 5433:5433 -p 9042:9042 \
  yugabytedb/yugabyte:latest bin/yugabyted start \
  --background=false
```

2. Connect to the database using the YSQL shell:
```bash
docker exec -it yugabyte bin/ysqlsh -h localhost
```

3. Create test database and IoT sensor tables using the provided SQL script:
```bash
docker exec -it yugabyte bin/ysqlsh -h localhost -f /path/to/setup_test_data.sql
```

Or copy the contents of `setup_test_data.sql` and paste into the YSQL shell. This creates an IoT sensor monitoring database with four tables:
- `devices` - IoT sensor devices
- `sensor_readings` - Time-series sensor data
- `alerts` - Equipment alerts and warnings
- `maintenance_log` - Maintenance history

4. Update your `configuration.json`:
```json
{
  "host": "localhost",
  "port": "5433",
  "database": "iot_sensors",
  "user": "yugabyte",
  "password": "yugabyte",
  "schema": "public"
}
```

### Online setup using YugabyteDB Managed
Alternatively, you can use YugabyteDB Managed (cloud service):

1. Sign up for a free account at https://cloud.yugabyte.com/
2. Create a new cluster following the guided setup
3. Note the connection details provided (host, port, database, username, password)
4. Add your IP address to the IP allow list in the cluster settings
5. Use the provided credentials in your `configuration.json`

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
