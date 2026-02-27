# AWS Glue Connector Example – Using Boto3 and DuckDB

## Connector overview
The **AWS Glue Connector** demonstrates how to use the **Fivetran Connector SDK** to extract data from AWS Glue–managed datasets stored in **Amazon S3**.

This connector connects to the **AWS Glue Data Catalog** using the **boto3** SDK to discover databases, tables, and metadata. It then uses **DuckDB** to query data directly from S3 locations, handling both **incremental syncs** (based on `updated_at` or a configured timestamp column) and **full reimports** for tables without incremental fields.

This connector is ideal for:
- Extracting tabular datasets stored in S3 and registered in Glue.
- Syncing large S3 data lakes into a Fivetran destination.
- Running incremental updates and stateful syncs efficiently.

Note: You must provide valid AWS credentials with access to **AWS Glue** and **Amazon S3**.

---

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+ (arm64 or x86_64)
- AWS IAM credentials with the following permissions:
    - `glue:GetTables`
    - `glue:GetDatabase`
    - `s3:GetObject`
    - `s3:ListBucket`

---

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to set up your environment, install dependencies, and run your connector locally.

---

## Features

- Connects to **AWS Glue** via the **boto3** SDK.
- Retrieves table and column metadata from the Glue Data Catalog.
- Uses **DuckDB** with AWS S3 credentials for direct querying.
- Supports **incremental syncs** using a configurable timestamp column (default: `updated_at`).
- Performs **full reimport syncs** for non-incremental tables.
- Captures **soft deletes** using checksum comparison.
- Automatically maps **Glue data types** to **Fivetran types**.
- Persists state and checkpoints with `op.checkpoint()` for resumable syncs.

---

## Configuration file

The connector requires AWS credentials and Glue database details for access and configuration.

Example `configuration.json`:

```json
{
  "aws_access_key_id": "<YOUR_AWS_ACCESS_KEY_ID>",
  "aws_secret_access_key": "<YOUR_AWS_SECRET_ACCESS_KEY>",
  "aws_region": "<YOUR_AWS_REGION>",
  "glue_database": "<YOUR_GLUE_DATABASE>",
  "schema_name": "<YOUR_SCHEMA_PREFIX>",
  "incremental_column": "<INCREMENTAL_COLUMN_NAME>"
}
```

### Configuration parameters

| Key | Required | Description |
|-----|-----------|-------------|
| `aws_access_key_id` | Yes | AWS access key ID for authentication. |
| `aws_secret_access_key` | Yes | AWS secret access key for authentication. |
| `aws_region` | No | AWS region for Glue and S3 resources (default: `us-east-1`). |
| `glue_database` | Yes | The Glue Data Catalog database to query. |
| `schema_name` | No | Optional prefix filter for table names. |
| `incremental_column` | No | The column name for incremental sync (default: `updated_at`). |

Note: The `configuration.json` file contains sensitive credentials and must **not** be checked into version control.

---

## Requirements file

The connector uses the following Python dependencies for local execution:

```
boto3
duckdb
```

Note: The `fivetran_connector_sdk` package is pre-installed in Fivetran’s runtime environment. Include it only for local testing.

---

## Authentication

The connector authenticates with AWS using **boto3** and the provided credentials:

```python
boto3.client(
    'glue',
    aws_access_key_id=configuration['aws_access_key_id'],
    aws_secret_access_key=configuration['aws_secret_access_key'],
    region_name=configuration.get('aws_region', 'us-east-1')
)
```

- **AWS Glue** provides table and column metadata.
- **DuckDB** uses the same credentials via its `httpfs` extension to access S3 objects directly.
- Ensure that your IAM user or role has **read-only permissions** on Glue and S3.

---

## Pagination

Pagination is implemented at two levels:

1. **Glue Metadata Pagination**
    - Uses the Glue paginator API (`get_paginator('get_tables')`) to iterate through all tables in the Data Catalog.
    - Supports a `schema_name` prefix to limit results.

2. **Data Batch Pagination**
    - DuckDB queries are limited to a configurable batch size (`__BATCH_SIZE = 10000`).
    - Each batch is processed and checkpointed using `op.checkpoint()`.

---

## Data handling

The connector performs the following operations:

1. **Schema discovery**
    - Retrieves Glue table metadata using `get_glue_tables()`.
    - Builds Fivetran-compatible schemas through the `schema()` function.

2. **Incremental syncs**
    - Uses `find_incremental_column()` to detect timestamp columns.
    - Fetches only rows with values greater than the last checkpointed timestamp (`WHERE updated_at > last_sync`).
    - Processes results in batches and computes row checksums for change tracking.

3. **Full reimport**
    - If no incremental column is found, the connector performs a full sync.
    - Reads all data files using `read_parquet()`, `read_csv_auto()`, or `read_json_auto()` depending on Glue metadata.

4. **Soft delete detection**
    - Compares stored checksums against new data to identify deleted records.
    - Emits `op.delete()` events for missing records.

5. **Checkpointing**
    - Saves progress after every table or batch using `op.checkpoint(state)`.
    - Tracks `last_incremental_value`, `checksums`, and `last_sync` timestamps.

---

## Error handling

The connector implements robust error handling for AWS and DuckDB operations:

| Error Type | Description | Resolution |
|-------------|--------------|-------------|
| Missing credentials | One or more AWS parameters not provided. | Add credentials in `configuration.json`. |
| AWS connection errors | Glue or S3 connection failed. | Check IAM permissions and region configuration. |
| Unsupported file format | DuckDB cannot read file type (e.g., Avro). | Convert to Parquet, CSV, or JSON. |
| Query errors | Invalid or corrupted S3 data. | Inspect data file structure and metadata. |
| Serialization issues | Non-serializable field types (e.g., dicts). | Automatically converted to JSON strings. |

All errors are logged using Fivetran’s `Logging` API at appropriate levels (`info`, `warning`, `severe`).

---

## Tables created

The connector creates destination tables that mirror Glue Data Catalog tables.

| Table name | Primary key | Description |
|-------------|--------------|-------------|
| `glue_database.orders` | `order_id` | Incrementally synced order table. |
| `glue_database.customers` | `customer_id` | Customer dataset with soft delete detection. |
| `glue_database.transactions` | `transaction_id` | Full-refresh dataset stored in S3 Parquet format. |

Each table includes:
- `_row_id` – unique identifier generated from MD5 hash of all columns.
- `_fivetran_synced` – timestamp of the last sync.

---

## Additional considerations

- Supported file types: **Parquet**, **CSV**, and **JSON**.
- ORC and Avro files may not be supported by DuckDB.
- Incremental syncs require a timestamp-based column (`updated_at`, `event_time`, etc.).
- Large Glue catalogs may benefit from using the `schema_name` filter for faster syncs.
- Ensure S3 paths are accessible and not encrypted with unsupported KMS keys.

This connector is provided as an educational example for using **Fivetran Connector SDK** with **AWS Glue** and **S3**.  
It demonstrates key patterns such as schema inference, incremental syncs, delete detection, and checkpointing.

For questions or support, contact the **Fivetran Support** team.
