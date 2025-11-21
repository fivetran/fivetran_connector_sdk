# MotherDuck Connector Example

## Connector Overview

This connector demonstrates how to extract data from MotherDuck (DuckDB Cloud) databases and upsert it into your Fivetran destination using the Fivetran Connector SDK.

The connector connects to a MotherDuck workspace using an authentication token, discovers schemas and tables, and supports both incremental syncs (based on a timestamp column like `updated_at`) and full reimports for tables without incremental fields.

It automatically detects schema changes, computes checksums for delete detection, and checkpoints state for resumable syncs.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
* Operating system:
    * Windows: 10 or later (64-bit only)
    * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    * Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting Started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features

- Connects securely to MotherDuck using the DuckDB Python client and an authentication token.
- Automatically discovers schemas, tables, and columns within the connected database.
- Supports incremental syncs using a specified or detected column (default `updated_at`).
- Performs full table reimports for tables without incremental fields.
- Detects deletions using checksum-based soft delete logic.
- Periodically checkpoints to ensure resumable incremental syncs.
- Converts DuckDB data types to Fivetran-compatible schema types automatically.

## Configuration File

The connector uses a `configuration.json` file to store database connection settings.

```
{
  "motherduck_token": "<YOUR_MOTHERDUCK_TOKEN>",
  "database_name": "<YOUR_DATABASE_NAME>",
  "schema_name": "<YOUR_SCHEMA_NAME>",
  "incremental_column": "<YOUR_INCREMENTAL_COLUMN>"
}
```

| Field | Required | Description |
|--------|-----------|-------------|
| `motherduck_token` | Yes | Authentication token for connecting to MotherDuck. |
| `database_name` | No | Name of the MotherDuck database (defaults to your default DB). |
| `schema_name` | No | Schema to filter tables (optional). |
| `incremental_column` | No | Name of the incremental timestamp column (defaults to `updated_at`). |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

---

## Requirements File

The connector requires the `duckdb` package to connect to MotherDuck databases.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector authenticates to MotherDuck using an access token.  
The connection string is formatted as:

```
md:<YOUR_DATABASE_NAME>?motherduck_token=<YOUR_MOTHERDUCK_TOKEN>
```

Example:
```
md:analytics_db?motherduck_token=<YOUR_MOTHERDUCK_TOKEN>
```

Authentication is managed within the `connect(token, db)` function, which securely establishes a connection to the target MotherDuck workspace.

## Pagination

This connector implements batch-based pagination for incremental and full syncs.  
Refer to the following functions:
- `incremental_sync()` – retrieves new or updated rows in batches using a `LIMIT` clause.
- `reimport_sync()` – performs full table scans in chunks when incremental tracking is not supported.

Pagination is controlled by the `__BATCH_SIZE` constant (default: `10,000` rows per batch).

## Data Handling

Data extraction and loading follow this workflow:

1. The connector connects to the MotherDuck database using the provided token.
2. Discovers all schemas, tables, and columns via `information_schema`.
3. Detects data type mappings using `map_type()`.
4. For each table:
    - If an incremental column exists, runs `incremental_sync()` to pull new/updated records.
    - Otherwise, runs `reimport_sync()` to perform a full table refresh.
5. Each row is serialized, checksummed, and upserted into Fivetran using `op.upsert()`.
6. Rows missing in the source are deleted via `op.delete()`.
7. The connector checkpoints state after each sync using `op.checkpoint()` for reliable resumption.

All datetime, list, and JSON values are serialized in a Fivetran-compatible format using the `serialize()` function.

## Error Handling

The connector implements robust error handling throughout the sync process:

- Connection Errors: Captured and logged in `connect()`; raises exceptions if connection fails.
- Schema Discovery Errors: Skips problematic tables gracefully and logs warnings.
- Query Errors: Logs and continues when invalid SQL is encountered during column or table lookups.
- Serialization Errors: Handles non-JSON serializable data types (e.g., datetime, binary).
- Delete Detection: Uses checksums to detect and remove missing records safely.

All errors and warnings are logged via `fivetran_connector_sdk.Logging`.

## Tables Created

This connector dynamically replicates all tables from the configured database and schema.  
The resulting tables follow this naming convention:
```
<database_name>_<schema_name>_<table_name>
```

### Example

| Destination Table     | Description |
|-----------------------|-------------|
| `testdb_main_orders`  | Orders table from schema `main`. |
| `testdb_analytics_customers` | Customers table from schema `analytics`. |
| `testdb_sales_transactions`  | Transactions table synced incrementally using `updated_at`. |

## Additional Considerations

- Tables must include a timestamp column (like `updated_at`) to support incremental syncs.
- The connector auto-detects primary keys using simple naming heuristics (`id`, `*_id`, or `pk`).
- Batch size and incremental column can be customized in configuration.
- The connector is idempotent and safe for repeated runs — unchanged records will not be reloaded.

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
