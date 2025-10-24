# TiDB connector example

## Connector overview
- This example demonstrates a source connector that reads rows from a TiDB database and upserts them into the Fivetran destination using the Connector SDK.
- It supports incremental replication based on a `created_at` timestamp, vector column parsing (optional), and stores per-table progress in the connector `state`.
- Use cases: incremental sync of application tables, vector/embedding export for ML workflows, and incremental change capture for analytics.

## Requirements
- Supported Python versions: see top-level Connector SDK README for supported versions.
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
- Refer to the Connector SDK Setup Guide to configure local development and packaging steps.
- Place this example in `connectors/tidb` (or your preferred connectors folder) and provide a local `configuration.json` for debug runs.

## Features
- incremental replication driven by `created_at` timestamp.
- configurable table list and primary keys via `TABLES_PRIMARY_KEY_COLUMNS`.
- optional support for vector columns: parse serialized embeddings into proper JSON lists via `VECTOR_TABLES_DATA`.
- robust error handling with per-table error markers stored into connector `state`.

## Configuration file
- The connector expects a `configuration.json` file when running locally. Configuration keys consumed by this connector:

```

{
"TIDB_HOST": "host.example.com",
"TIDB_USER": "tidb_user",
"TIDB_PASS": "tidb_password",
"TIDB_PORT": "4000",
"TIDB_DATABASE": "my_database",
"TABLES_PRIMARY_KEY_COLUMNS": "{\"table1\":\"id1\",\"table2\":\"id2\"}",
"VECTOR_TABLES_DATA": "{\"vector_table1\":{\"primary_key_column\":\"id3\",\"vector_column\":\"embedding\"}}"
}

```

Note: Ensure that the `configuration.json` file is not checked into version control to protect credentials.

## Requirements file
- `requirements.txt` lists third-party Python packages required by this example. Example content:

```

pytidb==0.0.11
certifi==2025.8.3

```

Note: The `fivetran_connector_sdk:latest` package is pre-installed in the Fivetran environment. Avoid re-declaring it in `requirements.txt` to prevent conflicts.

## Authentication
- This example uses direct database credentials (username/password) to connect to the TiDB cluster.
- Provide the host, user, password, port, and database in `configuration.json`.
- Note: For production usage, use secure secret storage and avoid checking credentials into source control.

## Pagination
- Not applicable. The connector issues simple `SELECT` queries against tables and uses `created_at` for incremental reads. If a table uses a different incremental column or requires cursor-based pagination at the source, modify `fetch_and_upsert_data` accordingly.

## Data handling
- Rows are fetched and passed to `process_row` for normalization.
- `process_row` ensures naive datetimes are made timezone-aware (UTC) and attempts to parse vector columns into Python lists when `VECTOR_TABLES_DATA` is configured.
- The declared schema (the `schema` function) depends on the `TABLES_PRIMARY_KEY_COLUMNS` and optional `VECTOR_TABLES_DATA` configuration for typed JSON columns.

## Error handling
- Query-level failures (for example, missing `created_at` column) are logged, added to `state` under `{table_name}_last_error`, and the connector checkpoints state so operators can inspect errors without losing progress on other tables. (Refer to `fetch_and_upsert_data`.)
- Row-level failures are logged and a sample of the row is stored in state under `{table_name}_last_row_error_sample` for debugging. (Refer to `fetch_and_upsert_data` and `process_row`.)
- Connection-level failures are recorded in state under `last_connection_error`, and the exception is raised to allow the runtime to retry according to its backoff policy. (Refer to `create_tidb_connection` and `update`.)

## Tables created
- Summary of tables replicated depends on `TABLES_PRIMARY_KEY_COLUMNS` in `configuration.json`. The connector does not create destination tables automatically; the Connector SDK runtime handles writing rows into destination tables according to this schema declaration.
- Example tables that could be synced: `caretakers`, `patients`, `patient_metadata`, `usual_spots`.

## Additional files
- `connector.py` – main connector implementation including schema declaration, update loop, and helpers.
- `configuration.json` – sample configuration for local debugging (do not commit secrets).
- `requirements.txt` – third-party dependencies required by this example.

## Additional considerations
- The example builds a safe, minimal retry for `op.upsert` to tolerate transient errors. For production workloads you may want to add exponential backoff and alerting integration.
- If your tables do not expose a `created_at` column, modify `fetch_and_upsert_data` to use another incremental marker (for example, an increasing integer primary key) or implement change-capture hooks.
- This example intentionally stores helpful error metadata into the connector `state` so operators can diagnose failures without reproducing the whole run locally.