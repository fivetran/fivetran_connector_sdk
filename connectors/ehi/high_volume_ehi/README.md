# High-Volume EHI Connector Example

## Connector overview

This connector syncs healthcare data from Microsoft SQL Server databases such as Caboodle analytical data store and similar EHI (Electronic Health Information) schemas. It is designed to handle very high data volumes reliably and efficiently. The connector uses [pyodbc](https://pypi.org/project/pyodbc/) with Microsoft's ODBC Driver 18 for SQL Server. 


## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- Microsoft ODBC Driver 18 for SQL Server installed on the host — see the [Microsoft installation guide](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- A SQL Server login with `SELECT` permission on the target schema


## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

Note: Before running locally, install the Microsoft ODBC Driver 18 for SQL Server on your machine. On macOS, run `brew install msodbcsql18`. On Linux (Debian/Ubuntu), follow the [Microsoft apt installation guide](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server). Verify the installation with `odbcinst -q -d -n "ODBC Driver 18 for SQL Server"`.


## Features

- Automatic schema discovery — all tables in the target schema are discovered and synced without manual configuration
- Automatic replication key detection using known column name patterns (e.g. `UpdatedAt`, `_LastUpdatedInstant`, `ModifiedDate`) with fallback to the first datetime column or identity column
- Full load with keyset pagination when a replication key is available, or offset pagination otherwise
- Incremental sync after a completed full load — only rows where `repl_key > last_synced_value` are fetched
- Parallel table syncs via a configurable number of worker threads
- Configurable table include and exclude lists via constants in `constants.py`
- Binary and spatial columns (`varbinary`, `geography`, `geometry`, etc.) included as base64-encoded strings
- Transient error retry with SQLSTATE-based detection and exponential backoff


## Configuration file

```json
{
    "mssql_server": "<YOUR_SQL_SERVER_HOST>",
    "mssql_cert_server": "<YOUR_CERT_HOSTNAME_OR_EMPTY>",
    "mssql_port": "<YOUR_SQL_SERVER_PORT>",
    "mssql_database": "<YOUR_SQL_SERVER_DATABASE>",
    "mssql_user": "<YOUR_SQL_SERVER_USERNAME>",
    "mssql_password": "<YOUR_SQL_SERVER_PASSWORD>",
    "mssql_schema": "<YOUR_SQL_SERVER_SCHEMA>"
}
```

The configuration keys are:

- `mssql_server`: hostname or IP address of the SQL Server instance
- `mssql_cert_server` (optional): hostname to validate in the server's TLS certificate; leave empty to trust the server certificate without hostname verification, which is suitable for AWS RDS and other cloud-hosted SQL Servers with self-signed certificates
- `mssql_port`: TCP port for the SQL Server instance; defaults to `1433`
- `mssql_database`: name of the database to connect to
- `mssql_user`: SQL Server login username
- `mssql_password`: SQL Server login password
- `mssql_schema`: schema to discover and sync tables from; defaults to `dbo`

Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/fivetran_connector_sdk/tree/main/connectors) or enhancing an [example](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples) in the open-source [Connector SDK repository](https://github.com/fivetran/fivetran_connector_sdk/tree/main), ensure the `configuration.json` file has placeholder values.
When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file

The `requirements.txt` file specifies the Python library required by the connector beyond those pre-installed in the Fivetran environment.

```
pyodbc==5.2.0
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

Note: `pyodbc` requires Microsoft ODBC Driver 18 for SQL Server to be installed on the host. In the Fivetran connector runtime environment this driver is pre-installed.


## Authentication

The connector authenticates using SQL Server username and password credentials supplied in `configuration.json`. The connection always uses TLS encryption (`Encrypt=yes`).

When `mssql_cert_server` is set, the connector validates the server's TLS certificate against that hostname (`TrustServerCertificate=no`, `HostNameInCertificate=<value>`). This is the recommended setting for production environments where the SQL Server has a valid certificate issued to a known hostname.

When `mssql_cert_server` is empty, the connector sets `TrustServerCertificate=yes`, which bypasses hostname verification. This is suitable for cloud-hosted SQL Server instances (AWS RDS, Azure SQL) that use self-signed certificates.


## Pagination

The connector uses two pagination strategies depending on whether a replication key column can be detected for each table. Each page executes a fresh bounded query that seeks directly to `WHERE repl_col > last_value` using the table index. This is O(log n) per page and O(n) total, with no server-side cursor state. The last seen replication key value is stored in state after every checkpoint interval and used as the resume point if the sync is interrupted.

When many rows share the same replication key value (for example, batch-inserted rows with the same timestamp), enabling `DEFAULT_USE_PK_TIEBREAK` in `constants.py` switches to a composite keyset. Offset pagination is O(n²) in database cost and is used only as a fallback for tables where no replication key column can be detected. A warning is logged for each such table.


## Data handling

Schema detection queries `INFORMATION_SCHEMA.COLUMNS` and `COLUMNPROPERTY` for each table to determine column names, SQL Server data types, primary key membership, identity columns, and computed columns. Computed columns are excluded from `SELECT` lists because SQL Server rejects them in explicit column lists under certain schema configurations. Schema discovery runs in parallel using a thread pool.

The replication key for each table is selected using the following priority order:

1. `incremental_column` constant in `constants.py`
2. Column name matches a known Epic Caboodle pattern (e.g. `_LastUpdatedInstant`, `UpdatedAt`, `ModifiedDate`) — checked case-insensitively
3. First `datetime2`, `datetime`, or `datetimeoffset` column in ordinal position order
4. First `IDENTITY` integer column in ordinal position order
5. None — offset pagination only, no incremental mode available

Refer to `class SchemaDetector` in `models.py` and `def convert_value` in `readers.py`.


## Error handling

Transient SQL Server errors are detected using SQLSTATE codes rather than substring-matching error messages. SQLSTATE codes are standardised and locale-independent. SQL Server native error 1222 (lock request time out period exceeded) arrives with SQLSTATE `HY000` and is handled by checking the native error number embedded in the message string.

On a retryable error the connection is closed, the thread sleeps with exponential backoff and jitter (starting at 5 seconds, capped at 300 seconds), the connection is reopened, and the query is retried. Keyset queries are idempotent — re-executing with the same `last_seen_value` parameter returns the same page. After `MAX_RETRIES` exhausted attempts the exception is re-raised.

Non-retryable errors are logged at `SEVERE` level and re-raised immediately, causing that table's thread to exit. Other tables continue syncing. Failed table names are collected and logged as a warning at the end of the sync run.

Refer to `def _is_retryable_error` and `def execute_with_retry` in `client.py`.


## Tables created

Tables are discovered dynamically from the SQL Server schema specified in `mssql_schema`. No tables are hardcoded in the connector. The set of tables synced is the full contents of the schema, subject to `DEFAULT_TABLE_LIST` (include list) and `DEFAULT_TABLE_EXCLUSION_LIST` (exclude list) in `constants.py`.

Each table is created in the destination with:

- Column names and Fivetran-inferred types based on the mapped Python types
- Primary keys as detected from `INFORMATION_SCHEMA.KEY_COLUMN_USAGE`
- Binary and spatial columns stored as base64-encoded strings


## Additional files

- `client.py` – defines `MSSQLConnection` (single pyodbc connection with retry logic and `READ UNCOMMITTED` isolation) and `ConnectionPool` (fixed-size queue-based pool for multi-threaded access)
- `models.py` – defines `ColumnInfo` and `TableSchema` dataclasses and `SchemaDetector` (queries `INFORMATION_SCHEMA` to build per-table schemas and detect replication keys)
- `readers.py` – defines `KeysetReader`, `OffsetReader`, and `IncrementalReader` generators that stream table data in bounded batches, and `convert_value()` for type-safe row serialisation
- `constants.py` – tunable parameters: batch size, checkpoint interval, worker thread count, retry settings, replication key detection patterns, and operational defaults (table lists, force full resync, PK tiebreak)


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
