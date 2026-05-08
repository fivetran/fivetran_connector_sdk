# Database LIMIT/OFFSET Pagination Connector Example

## Connector overview
This connector demonstrates how to implement LIMIT/OFFSET pagination for syncing data from a relational database. The connector queries a local SQLite database that is automatically created and seeded on the first run — no external service is required.

LIMIT/OFFSET pagination adds `LIMIT <N> OFFSET <k>` to every query. It is simple to implement but has two important limitations compared to [keyset pagination](../database_keyset):
- Row-shift: if rows are inserted or deleted while paging, later offsets can shift, causing gaps or duplicate rows.
- Performance: the database must scan and skip `OFFSET` rows for every page, so queries become progressively slower as the offset grows.

For large or frequently updated tables, prefer the [database keyset pagination example](../database_keyset) instead.

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
fivetran init <project-path> --template examples/common_patterns_for_connectors/pagination/database_limit_offset
```
`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`.
If you do not specify a project path, Fivetran creates the project in your current directory.
For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/setup-guide#createyourcustomconnector).


## Features
- Demonstrates LIMIT/OFFSET pagination against a local SQLite database.
- Automatically creates and seeds the database with 200 sample rows on the first run.
- Tracks sync progress using a numeric `offset` stored in state.
- Implements `op.checkpoint()` for resumable syncs.
- Parses and upserts all paginated results into a `user` table.


## Configuration file
This example does not require a configuration file.

For production connectors, `configuration.json` would contain database connection details such as host, port, database name, username, and password.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file
This connector has no additional Python dependencies.

`sqlite3` is part of the Python standard library and requires no installation.

Note: The `fivetran_connector_sdk:latest` package is pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare it in your `requirements.txt`.

For production connectors that connect to PostgreSQL or MySQL, add the appropriate driver (e.g. `psycopg2-binary` or `PyMySQL`) to `requirements.txt`.


## Authentication
This example uses a local SQLite file and does not require authentication.

In real-world scenarios, replace the SQLite connection in `sync_items()` with a connection to your source database using the appropriate driver and credentials from `configuration`.


## Pagination
Pagination is handled using a numeric offset stored in state:
- On the first sync, `offset` starts at `0`.
- Each query fetches 25 rows starting at the current offset: `LIMIT 25 OFFSET <offset>`.
- After each page, the offset advances by the number of rows returned and is checkpointed in state.
- Pagination ends when the query returns no rows.
- A deterministic `ORDER BY updated_at, id` is used on every query to ensure a consistent row order across requests.

Important: if rows are inserted or deleted in the source table while paging is in progress, the offset can shift. Rows may be skipped or returned twice. If your table is stable during a sync window, LIMIT/OFFSET is safe to use. For tables that change frequently, use [keyset pagination](../database_keyset) instead.


## Data handling
- On the first run, creates and seeds a local `users.db` SQLite file with 200 rows.
- Subsequent runs reuse the existing database file.
- Fetches rows in pages of 25 using LIMIT/OFFSET.
- Syncs each row to Fivetran using `op.upsert(table="user", data=...)`.
- Checkpoints state after each page to support reliable resume.


## Error handling
- SQLite connections in `sync_items()` and `_seed_database_if_needed()` are closed in a `finally` block to prevent resource leaks.
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
