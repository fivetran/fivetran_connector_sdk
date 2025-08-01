# Snowflake Schema-Aware Connector Example

## Connector overview
This connector demonstrates how to dynamically extract the schema from a Snowflake database and sync data from multiple tables using the Fivetran Connector SDK. It uses Snowflake’s information schema to identify column names, types, and primary keys, and generates a compatible schema for Fivetran. The connector then syncs data from the `PRODUCTS` and `ORDERS` tables using different replication strategies.

This example shows how to:
- Use Snowflake introspection for flexible schema definition.
- Sync one table in full-refresh mode (products).
- Sync another table incrementally based on a timestamp (orders).
- Convert Snowflake data types to Fivetran-compatible types.


## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)


## Getting started
Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## Features
- Dynamically builds schema by querying Snowflake’s metadata.
- Supports column type mapping from Snowflake to Fivetran types.
- Automatically discovers primary keys using `SHOW PRIMARY KEYS`.
- Syncs `PRODUCTS` via full refresh.
- Syncs `ORDERS` incrementally using the `order_date` field.


## Configuration file
The following configuration keys are required:

```json
{
  "user":"<YOUR_SNOWFLAKE_USERNAME>",
  "password":"<YOUR_SNOWFLAKE_PASSWORD>",
  "account":"<YOUR_ACCOUNT_IDENTIFIER>",
  "database":"<YOUR_DATABASE_NAME>",
  "schema":"<YOUR_SCHEMA_NAME>",
  "tables": "<YOUR_TABLE_NAMES_SEPARATED_BY_COMMA>"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file
This connector requires the following Python dependencies:

```
snowflake_connector_python==3.14.0
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Authentication
The connector authenticates directly with Snowflake using the credentials from the configuration. It uses the native `snowflake.connector` Python library.


## Schema extraction
The connector queries Snowflake’s `INFORMATION_SCHEMA.COLUMNS` and `SHOW PRIMARY KEYS` to:
- Discover columns.
- Determine column types.
- Identify primary keys.
- Return a schema compatible with Fivetran SDK.

Unsupported or unknown data types default to `STRING`.


## Data handling
- The `PRODUCTS` table is loaded completely each time (full sync).
- The `ORDERS` table is synced incrementally using the `order_date` column:
  - It compares against state["lastOrder"].
  - Updates state after every row.


## Error handling
- Raises descriptive errors when configuration keys are missing.
- Uses `raise_for_status()` for failed SQL queries.
- Safely closes connections and cursors.

## Tables Created
The connector creates two tables:

`PRODUCTS`:
```json
{
  "table": "products",
  "primary_key": ["product_id"],
  "columns": {
    "product_id": "INT",
    "product_code": "STRING",
    "product_name": "STRING",
    "price": "DOUBLE",
    "in_stock": "BOOLEAN",
    "description": "STRING",
    "weight": "DOUBLE"
  }
}
```

`ORDERS`:
```json
{
  "table": "orders",
  "primary_key": ["order_id"],
  "columns": {
    "order_id": "STRING",
    "customer_id": "INT",
    "order_date": "NAIVE_DATE",
    "product_id": "INT",
    "quantity": "INT",
    "unit_price": "DOUBLE",
    "amount": "DOUBLE",
    "payment_method": "STRING",
    "status": "STRING",
    "street_address": "STRING",
    "city": "STRING",
    "state": "STRING",
    "zip": "STRING",
    "discount_applied": "DOUBLE"
  }
}
```


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.