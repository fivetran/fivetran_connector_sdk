# Epic Caboodle Connector Example

## Connector overview

The Simple Epic Caboodle Connector demonstrates how to fetch data from a Microsoft SQL Server database ( Epic Caboodle) and sync it to your Fivetran destination using the Fivetran Connector SDK. The connector automatically discovers tables and schemas from your Microsoft SQL Server database and synchronizes data using full load and incremental sync methods.


## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)


## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## Features

- Automated table and schema discovery from `INFORMATION_SCHEMA`
- Incremental data synchronization using a timestamp column (configurable via `__MODIFIED_COLUMN_NAME`)
- Batch processing for efficient data fetching
- Comprehensive error handling with per-record graceful degradation
- Limited to `__MAX_TABLES` tables (default: 10) for demonstration purposes


## Configuration file

The connector requires minimal configuration with only essential connection parameters:

```json
{
  "mssql_server": "<YOUR_CABOODLE_MSSQL_SERVER>",
  "mssql_database": "<YOUR_CABOODLE_MSSQL_DATABASE>",
  "mssql_user": "<YOUR_CABOODLE_MSSQL_USER>",
  "mssql_password": "<YOUR_CABOODLE_MSSQL_PASSWORD>",
  "mssql_port": "<YOUR_CABOODLE_MSSQL_PORT>"
}
```

- `mssql_server`(required): SQL Server hostname or IP address
- `mssql_database`(required): Database name to connect to
- `mssql_user`(required): Database username
- `mssql_password`(required): Database password
- `mssql_port`(required): SQL Server port

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
- Access to `INFORMATION_SCHEMA` views for automated schema discovery
- Network connectivity to the SQL Server instance

Note: The connector sets `validate_host=False` in the `pytds` connection, which disables TLS host-name verification. This is suitable for development environments or private networks. Enable host validation in production by removing this flag or setting it to `True`.


## Pagination

The connector implements batch processing to handle datasets efficiently using the `fetchmany()` method. Data is fetched in configurable batches and yielded one record at a time, keeping memory usage low regardless of table size. The default batch size is 1000 rows and checkpoint interval is 5000 records.

Refer to `__BATCH_SIZE` constant for configuring the batch size


## Data handling

The connector automatically discovers and maps data from SQL Server to Fivetran using the following process:

The schema discovery follows the following steps (Refer to `schema()` function):
1. Queries `INFORMATION_SCHEMA.TABLES` to identify available tables (up to `__MAX_TABLES`)
2. Discovers primary keys from `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` (looks for constraints prefixed with `PK_`)
3. Maps column names from `INFORMATION_SCHEMA.COLUMNS`
4. Creates table definitions with primary key information
5. Declares the `sync_history` table with `sync_timestamp` as its primary key

The Data synchronization is implemented using the following (refer to `update()` and `_get_table_data()`):

- Tables with `__MODIFIED_COLUMN_NAME`: The connector checks whether the table contains the `__MODIFIED_COLUMN_NAME` column. If present:
  - Historical sync: Uses the date `1970-01-01T00:00:00+00:00` as `last_sync_time`
  - Incremental syncs: Uses the actual `MODIFIED_COLUMN_NAME` value of the last successfully processed record as the cursor, fetched from state.
  - State (`state[table_name]`) is updated to the latest `__MODIFIED_COLUMN_NAME` seen after every record processed, ensuring the cursor at any checkpoint reflects the exact row last written to the destination.

- Tables without `__MODIFIED_COLUMN_NAME`: The connector performs a full resync everytime


## Error handling

The connector implements the following error handling mechanisms:
- Detailed error logging for troubleshooting connectivity issues
- Configuration parameter validation before attempting connection
- If a table query fails (e.g. network timeout, SQL error), the exception propagates and aborts the current sync run
- Individual record error handling without stopping the entire sync
- Warning logging for failed record processing with the exception detail


## Tables created

The connector replicates tables from the source after fetching the schema details from the source. These tables are named to match source table names and it includes all the columns from the source with exact primary keys. The number of tables replicated can be controlled using `__MAX_TABLES`.

Additionally, the connector creates the following tables:

### sync_history
This table contains one summary record per sync run, written at the end of each successful sync:
- `sync_timestamp` (primary key) — ISO timestamp of when the sync started
- `total_tables` — Number of tables processed in the sync
- `total_records` — Total number of records synchronized across all tables
- `tables_processed` — Comma-separated list of table names processed in the sync


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
