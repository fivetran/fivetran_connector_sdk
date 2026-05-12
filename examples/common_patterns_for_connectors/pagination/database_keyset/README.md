# Database Keyset Pagination Connector Example

## Connector overview

This connector demonstrates how to implement keyset pagination for syncing data from a PostgreSQL database.

Keyset pagination filters rows using a `WHERE (updated_at, id) > (last_updated_at, last_id)` clause and orders by those same columns. After each page, the boundary advances to the last row of the page.

This example is intended for learning purposes. It is not meant for production use.


## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)


## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init --template examples/common_patterns_for_connectors/pagination/database_keyset
```
`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`.
For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/setup-guide#createyourcustomconnector).

Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.


## Features

- Demonstrates keyset pagination against a PostgreSQL database.
- Tracks sync progress using a `(updated_at, id)` boundary stored in state.
- Uses a tie-breaker `id` to handle rows that share the same `updated_at` timestamp.
- Implements `op.checkpoint()` for resumable syncs.
- Parses and upserts all paginated results into a `user` table.


## Configuration file

The connector requires the following configuration keys in the `configuration.json` file:

```json
{
  "hostname": "<YOUR_POSTGRESQL_HOSTNAME>",
  "port": "<YOUR_POSTGRESQL_PORT>",
  "database": "<YOUR_POSTGRESQL_DATABASE_NAME>",
  "username": "<YOUR_POSTGRESQL_USERNAME>",
  "password": "<YOUR_POSTGRESQL_PASSWORD>",
  "sslmode": "<YOUR_SSL_MODE>",
  "table_name": "<YOUR_POSTGRESQL_TABLE_NAME>"
}
```

Configuration parameters:

- `hostname` (required): PostgreSQL server hostname or IP address.
- `port` (required): PostgreSQL server port (typically 5432).
- `database` (required): Name of the database to connect to.
- `username` (required): Database username for authentication.
- `password` (required): Database password for authentication.
- `table_name` (required): Name of the source table to sync data from.
- `sslmode` (optional): SSL mode for connection security. Valid values: `disable`, `require`, or empty string. Defaults to `disable`.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file

The `requirements.txt` file specifies the Python libraries required by the connector:

```
psycopg2_binary==2.9.12
```

- `psycopg2_binary`: PostgreSQL database adapter for Python, used to establish connections and execute queries.

Note: The `fivetran_connector_sdk:latest` package is pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare it in your `requirements.txt`.


## Authentication

The connector uses standard PostgreSQL username/password authentication.

Ensure the database user has `SELECT` privilege on the source table and `CONNECT` privilege on the database.


## Pagination

Pagination is handled using a keyset boundary stored in state:
- On the first sync, the boundary starts before all records: `updated_at = '0001-01-01T00:00:00+00:00'`, `id = 0`.
- Each query fetches rows where `(updated_at, id) > (last_updated_at, last_id)`, ordered by `updated_at, id`.
- After each page, the boundary advances to the `updated_at` and `id` of the last row in the page.
- Both values are stored in state and checkpointed after each page.
- Pagination ends when the query returns no rows.

The `id` tie-breaker ensures that rows sharing the same `updated_at` timestamp are handled correctly and no rows are skipped or duplicated.

Note: this pattern requires an indexed monotonic column. For best performance in production, ensure the source table has an index on `(updated_at, id)`.


## Data handling

- Fetches rows in pages of 25 using the keyset query.
- Converts psycopg2 row tuples to dictionaries using `cursor.description` for column names.
- Syncs each row to Fivetran using `op.upsert(table="user", data=...)`.
- Checkpoints state after each page to support reliable resume.


## Error handling

- Validates all required configuration parameters before connecting.
- Validates `sslmode` against allowed values.
- Wraps connection and sync failures in `RuntimeError` with the original exception preserved.
- Database connection is always closed in a `finally` block in `update()`.
- Cursor is always closed in a `finally` block in `sync_items()`.
- Empty result sets halt pagination gracefully.


## Tables created

The connector creates the `user` table:

```
{
  "table": "user",
  "primary_key": ["id"],
  "columns": {
    "id": "INT",
    "name": "STRING",
    "email": "STRING",
    "updated_at": "UTC_DATETIME"
  }
}
```


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
