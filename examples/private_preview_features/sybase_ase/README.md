# Sybase ASE Connector Example

## Connector overview

This connector uses the Fivetran Connector SDK to replicate data from a Sybase ASE database to any Fivetran-supported destination. It dynamically discovers all user tables in the configured database, detects primary keys and column types, and syncs each table incrementally using a per-table watermark stored in state. A full sync is performed for tables that have no suitable incremental column.

The connector connects to Sybase ASE via FreeTDS and pyodbc, bundling the `libtdsodbc.so` driver directly so that no installation step is required in the Fivetran cloud environment.


## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- FreeTDS installed locally for development and testing:
  - macOS: `brew install freetds`
  - Linux: `bash drivers/installation.sh`


## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init <project-path> --template examples/private_preview_features/sybase_ase
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`.
If you do not specify a project path, Fivetran creates the project in your current directory.
For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/setup-guide#createyourcustomconnector).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.


## Features

- Dynamic table discovery — all user tables in the configured database are synced automatically, with no manual schema definitions required.
- Incremental sync — each table is synced using a per-table watermark (prefers `datetime` columns, falls back to the first primary key column).
- Full sync fallback — tables with no suitable incremental column are fully replicated on every sync.
- Bundled FreeTDS driver — `libtdsodbc.so` is shipped alongside the connector so no installation step is needed in the Fivetran cloud environment.
- Retry logic — `pyodbc.connect()` is retried up to 3 times with a 2-second back-off to handle transient TDS errors.
- Hard connect timeout — a background thread enforces a 15-second deadline on `pyodbc.connect()` so that a hung TDS handshake raises a clear Python exception before the Fivetran gRPC deadline.
- Optional table filtering — set the `tables` configuration key to a comma-separated list to sync only specific tables.


## Configuration file

```json
{
  "server": "<host>",
  "port": "<port>",
  "database": "<database>",
  "user_id": "<username>",
  "password": "<password>"
}
```

To sync only specific tables, add an optional `tables` field (comma-separated):

```json
{
  "server": "<host>",
  "port": "<port>",
  "database": "<database>",
  "user_id": "<username>",
  "password": "<password>",
  "tables": "sales,authors,titles"
}
```

If `tables` is omitted, all user tables in the database are synced automatically.

> Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/fivetran_connector_sdk/tree/main/connectors) or enhancing an [example](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples) in the open-source [Connector SDK repository](https://github.com/fivetran/fivetran_connector_sdk/tree/main), ensure the `configuration.json` file has placeholder values.
When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file

The connector requires `pyodbc` to connect to Sybase ASE via the bundled FreeTDS ODBC driver.

```
pyodbc==5.3.0
```

> Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Authentication

The connector authenticates with Sybase ASE using a username and password supplied via `configuration.json`. Credentials are passed directly to the ODBC connection string and are never logged.


## Data handling

Refer to `def map_sybase_to_fivetran_type(sybase_type, precision, scale)` for the full type mapping.

Sybase ASE column types are mapped to Fivetran types as follows:

| Sybase type | Fivetran type |
|-------------|---------------|
| `char`, `varchar`, `text`, `unichar`, `univarchar` | `STRING` |
| `int`, `integer` | `INT` |
| `smallint`, `tinyint` | `SHORT` |
| `bigint` | `LONG` |
| `float` | `DOUBLE` |
| `real` | `FLOAT` |
| `decimal`, `numeric`, `money`, `smallmoney` | `DECIMAL` (with precision and scale) |
| `datetime`, `smalldatetime`, `bigdatetime` | `NAIVE_DATETIME` |
| `date` | `NAIVE_DATE` |
| `time`, `bigtime` | `NAIVE_TIME` |
| `binary`, `varbinary`, `image` | `BINARY` |
| `bit` | `BOOLEAN` |

Any unmapped type defaults to `STRING`.

Python `Decimal` objects are converted to strings before being passed to Fivetran's protobuf layer. `datetime` and `date` objects are serialised to ISO 8601 strings.

Rows are fetched and upserted in batches of 1,000. State is checkpointed after each batch. Refer to `def fetch_and_upsert(cursor, query, table_name, state, ...)` for details.


## Error handling

Refer to `def create_sybase_connection(configuration)` for connection error handling.

- TCP pre-flight check — before attempting an ODBC connection, the connector probes the host and port with a raw TCP socket. A failure here raises an immediate, descriptive error.
- Connect timeout — `pyodbc.connect()` is run in a daemon thread and joined with a 15-second timeout. If the thread is still alive after the timeout, a `RuntimeError` is raised with a clear message before the Fivetran gRPC deadline expires.
- Connect retries — if `pyodbc.connect()` raises an exception, it is retried up to 3 times with a 2-second back-off. A hang (thread timeout) is not retried.
- Per-query timeout — `conn.timeout` is set to 15 seconds so that any hanging SQL statement raises a Python exception rather than blocking the sync indefinitely.


## Tables created

The connector replicates all user tables found in the configured Sybase ASE database. The exact set of tables depends on the database being synced. For the `pubs2` sample database, the tables replicated are:

- `au_pix`
- `authors`
- `blurbs`
- `discounts`
- `publishers`
- `roysched`
- `sales`
- `salesdetail`
- `stores`
- `titleauthor`
- `titles`


## Additional files

- `drivers/installation.sh` – Shell script that registers the bundled `libtdsodbc.so` with `odbcinst.ini`. Kept for local Linux development; not executed by the Fivetran cloud platform.
- `drivers/libtdsodbc.so` – FreeTDS ODBC driver (Linux x86_64, extracted from `tdsodbc_1.3.17+ds-2_amd64.deb`). Bundled directly so the connector works in the Fivetran cloud environment without any installation step.


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
