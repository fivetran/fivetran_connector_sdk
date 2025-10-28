# Apache Druid Connector Example

## Connector overview
This connector demonstrates how to use the Fivetran Connector SDK to sync data from Apache Druid, a distributed real-time analytics database, and load it into a Fivetran destination for analysis.

This connector queries Druid’s SQL API to retrieve metadata and datasource contents. It supports both incremental syncs (based on a timestamp column such as `__time` or `updated_at`) and full imports, with optional checksum-based soft delete detection.

The connector automatically discovers all datasources in the configured Druid schema and maps their columns to Fivetran-compatible data types.

The connector supports the following use cases:
- Syncing metrics and dimensions from Druid datasources to a data warehouse.
- Capturing schema metadata for analytical lineage tracking.
- Running incremental or full refresh syncs of large Druid datasets.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+ (arm64 or x86_64)
- Apache Druid 25.0.0 or later
- Access to the Druid Router or Coordinator (default port `8888`)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) for setup steps including dependency installation, configuration, and local testing with the `fivetran debug` command.

## Features
- Connects to Apache Druid via its SQL endpoint (`/druid/v2/sql`).
- Automatically discovers all tables (datasources) within the configured schema.
- Supports incremental syncs based on timestamp or configured incremental column.
- Performs full syncs when incremental columns are not available.
- Dynamically maps Druid data types to Fivetran data types.
- Supports soft deletes via checksum comparison.
- Includes built-in pagination and rate-limit retry handling.

## Configuration file
The connector reads credentials and connection settings from `configuration.json`.

```
{
  "druid_host": "<YOUR_DRUID_HOST>",
  "druid_port": "<YOUR_DRUID_PORT>",
  "schema_name": "<YOUR_SCHEMA_NAME>",
  "incremental_column": "<YOUR_INCREMENTAL_COLUMN>",
  "use_ssl": "<YOUR_SSL_OPTION>"
}
```

Configuration paramaters:

- `druid_host` (required) - Hostname or IP address of the Druid router or coordinator.
- `druid_port` (optional) - Port number for the Druid SQL service (default: 8888).
- `schema_name` (optional) - Optional schema name for filtering datasources.
- `incremental_column` (optional) - Name of the incremental timestamp column (default: `__time`)
- `use_ssl` (optional) - Set to `true` to use HTTPS; defaults to HTTP if not provided.

Note: Do not check `configuration.json` into version control to prevent credential exposure.

## Authentication
Authentication is handled by connecting anonymously. The `normalize_host_and_make_base_url(configuration)` function creates the base URL.

## Pagination
Pagination is implemented using SQL’s `LIMIT` and `OFFSET` clauses. Refer to `select_sql_for_table()` and `run_sql()` functions.

The connector queries data in chunks of 50,000 rows per batch until all data has been retrieved. Incremental syncs also filter results using a timestamp condition such as:

```
WHERE "__time" > TIMESTAMP '<last_sync_value>'
ORDER BY "__time"
LIMIT 50000 OFFSET {offset}
```

## Data handling
Refer to the `update(configuration, state)` and `schema(configuration)` functions.

1. `schema(configuration)` retrieves all datasource names from the `INFORMATION_SCHEMA.TABLES` view and fetches their columns and types.
2. `update(configuration, state)` performs data extraction:
    - Determines whether each datasource supports incremental sync.
    - Runs either a full sync (`sync_full_sql`) or incremental sync (`sync_incremental_sql`).
    - Each retrieved row is upserted into Fivetran using `op.upsert(table, row)`.
    - State is checkpointed after each batch to preserve progress between runs.
3. The connector normalizes column types via `map_druid_type_to_fivetran()` to ensure compatibility.

## Error handling
Refer to `run_sql()` and `get_datasources()` for request handling.

The connector includes error handling for network, API, and SQL execution failures:
- Connection or timeout errors → logged and raised.
- HTTP 401/403 → authentication failure; connector stops execution.
- HTTP 404 → datasource or endpoint missing; logged and skipped.
- Invalid JSON → logged with an error message.
- All other errors → raised as `RuntimeError` with response details.

Retries are handled manually for transient failures and rate limits.

## Tables created
The connector dynamically replicates all datasources in the configured schema. Each datasource becomes a table in the Fivetran destination.

### Example tables

| Table | Description |
|--------|-------------|
| `INFORMATION_SCHEMA.TABLES` | Metadata about all tables and datasources. |
| `INFORMATION_SCHEMA.COLUMNS` | Metadata about all columns and their data types. |
| `SALES_DATA` | Example Druid datasource table with metrics and dimensions. |
| `USER_EVENTS` | Example datasource table for event tracking data. |

### Data type mapping

| Druid Type | Fivetran Type |
|-------------|----------------|
| STRING, VARCHAR, CHAR | STRING |
| LONG, BIGINT, INTEGER | LONG |
| FLOAT, DOUBLE, DECIMAL | DOUBLE |
| BOOLEAN | BOOLEAN |
| TIME, TIMESTAMP, DATETIME | UTC_DATETIME |
| DATE | NAIVE_DATE |

## Additional considerations
- When running against production clusters, ensure that the Druid SQL service (`/druid/v2/sql`) is exposed and reachable.
- Incremental syncs require a timestamp column (`__time` or equivalent) in each datasource.
- Large datasources may require tuning batch size or checkpoint frequency.
- This connector is intended for demonstration and learning purposes with the Fivetran Connector SDK.

Note: While tested, this connector is not an official Fivetran-managed integration. Fivetran assumes no responsibility for issues resulting from modifications or external dependencies.

For questions or assistance, contact the Fivetran Support team.
