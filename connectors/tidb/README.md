# TiDB Connector Example

## Connector overview
This example demonstrates a source connector that reads rows from a TiDB database and upserts them into the Fivetran destination using the Connector SDK. It supports incremental replication based on a `created_at` timestamp, vector column parsing (optional), and stores per-table progress in the connector `state`.
Use cases: Incremental sync of application tables, vector/embedding export for ML workflows, and incremental change capture for analytics.

## Accreditation
This example was contributed by [Nikhil Mankani](https://www.linkedin.com/in/nikhilmankani/).

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Incremental replication driven by `created_at` timestamp.
- Configurable table list and primary keys via `TABLES_PRIMARY_KEY_COLUMNS`.
- Optional support for vector columns: Parse serialized embeddings into proper JSON lists via `VECTOR_TABLES_DATA`.
- Robust error handling with per-table error markers stored in the connector `state`.

## Configuration file
- The connector expects a `configuration.json` file when running locally. Configuration keys consumed by this connector:

```

{
  "TIDB_HOST": "<YOUR_TIDB_HOST>",
  "TIDB_USER": "<YOUR_TIDB_USERNAME>",
  "TIDB_PASS": "<YOUR_TIDB_PASSWORD>",
  "TIDB_PORT": "<YOUR_TIDB_PORT>",
  "TIDB_DATABASE": "<YOUR_TIDB_DATABASE>",
  "TABLES_PRIMARY_KEY_COLUMNS": "<JSON_STRING_OF_TABLE_TO_PRIMARY_KEY_MAPPING>",
  "VECTOR_TABLES_DATA": "<JSON_STRING_OF_VECTOR_TABLE_CONFIGURATION>"
}

```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

### Configuration details

**TIDB_HOST**: Hostname or IP address of the TiDB server.

**TIDB_USER**: Username for TiDB connection.

**TIDB_PASS**: Password for the TiDB user.

**TIDB_PORT**: Port number for TiDB connection.

**TIDB_DATABASE**: Name of your TiDB database.

**TABLES_PRIMARY_KEY_COLUMNS**: A JSON object where keys are table names and values are the primary key column names.

Example:
```json
{
  "customers": "customer_id",
  "products": "product_id",
  "transactions": "transaction_id"
}
```

**VECTOR_TABLES_DATA** (Optional): A JSON object for tables containing vector/embedding columns. Each table requires:
- `primary_key_column`: The primary key column name
- `vector_column`: The column containing vector data

Example:
```json
{
  "product_embeddings": {
    "primary_key_column": "product_id",
    "vector_column": "embedding_vector"
  }
}
```

## Requirements file
- `requirements.txt` lists third-party Python packages required by this example. Example content:

```

pytidb>=0.0.11
certifi==2025.8.3

```

Note: The fivetran_connector_sdk:latest and requests:latest packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your requirements.txt.

## Authentication
This example uses direct database credentials (username/password) to connect to the TiDB cluster. Provide the host, user, password, port, and database in `configuration.json`.
Note: For production usage, use secure secret storage and avoid checking credentials into source control.

## Pagination
Not applicable. The connector issues simple `SELECT` queries against tables and uses `created_at` for incremental reads. If a table uses a different incremental column or requires cursor-based pagination at the source, modify `fetch_and_upsert_data` accordingly.

## Data handling
- Rows are fetched and passed to `process_row` for normalization.
- `process_row` ensures naive datetimes are made timezone-aware (UTC) and attempts to parse vector columns into Python lists when `VECTOR_TABLES_DATA` is configured.
- The declared schema (the `schema` function) depends on the `TABLES_PRIMARY_KEY_COLUMNS` and optional `VECTOR_TABLES_DATA` configuration for typed JSON columns.

## Error handling
- Query-level failures (for example, missing `created_at` column) are logged, added to `state` under `{table_name}_last_error`, and the connector checkpoints state so operators can inspect errors without losing progress on other tables. See `fetch_and_upsert_data`.
- Row-level failures are logged, and a sample of the row is stored in state under `{table_name}_last_row_error_sample` for debugging. See `fetch_and_upsert_data` and `process_row`.
- Connection-level failures are recorded in the state under `last_connection_error`, and the exception is raised to allow the runtime to retry according to its backoff policy. See `create_tidb_connection` and `update`.

## Tables created
Summary of tables replicated depends on `TABLES_PRIMARY_KEY_COLUMNS` in `configuration.json`. The connector does not create destination tables automatically; the connector SDK runtime handles writing rows into destination tables according to this schema declaration.
Example tables that could be synced: `caretakers`, `patients`, `patient_metadata`, `usual_spots`.

## Additional files
- `connector.py` – Main connector implementation including schema declaration, update loop, and helpers.
- `configuration.json` – Sample configuration for local debugging (do not commit secrets).
- `requirements.txt` – Third-party dependencies required by this example.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.