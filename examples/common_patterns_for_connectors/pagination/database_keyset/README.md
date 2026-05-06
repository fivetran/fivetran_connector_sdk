# Database Keyset Pagination Connector Example

## Connector overview
This connector demonstrates how to implement keyset pagination (also known as the seek method) for syncing data from a relational database. The connector queries a local SQLite database that is automatically created and seeded on the first run — no external service is required.

Keyset pagination filters rows using a `WHERE (updated_at, id) > (last_updated_at, last_id)` clause and orders by those same columns. After each page, the boundary advances to the last row of the page. This approach is stable under concurrent writes and performs consistently regardless of dataset size, because the database uses an index to find the starting row directly.

This differs from [LIMIT/OFFSET pagination](../database_limit_offset), which skips rows by counting from the beginning of the result set. Keyset is the recommended approach for large or frequently updated tables.

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
fivetran init <project-path> --template examples/common_patterns_for_connectors/pagination/database_keyset
```
`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`.
If you do not specify a project path, Fivetran creates the project in your current directory.
For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/setup-guide#createyourcustomconnector).


## Features
- Demonstrates keyset (seek method) pagination against a local SQLite database.
- Automatically creates and seeds the database with 200 sample rows on the first run.
- Tracks sync progress using a `(updated_at, id)` boundary stored in state.
- Uses a tie-breaker `id` to handle rows that share the same `updated_at` timestamp.
- Implements `op.checkpoint()` for resumable and incremental syncs.
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
Pagination is handled using a keyset boundary stored in state:
- On the first sync, the boundary starts before all records: `updated_at = '0001-01-01T00:00:00+00:00'`, `id = 0`.
- Each query fetches rows where `(updated_at, id) > (last_updated_at, last_id)`, ordered by `updated_at, id`.
- After each page, the boundary advances to the `updated_at` and `id` of the last row in the page.
- Both values are stored in state and checkpointed after each page.
- Pagination ends when the query returns no rows.

The `id` tie-breaker ensures that rows sharing the same `updated_at` timestamp are handled correctly and no rows are skipped or duplicated.

Note: this pattern requires an indexed monotonic column. For best performance in production, ensure the source table has an index on `(updated_at, id)`.


## Data handling
- On the first run, creates and seeds a local `users.db` SQLite file with 200 rows.
- Subsequent runs reuse the existing database file.
- Fetches rows in pages of 25 using the keyset query.
- Syncs each row to Fivetran using `op.upsert(table="user", data=...)`.
- Checkpoints state after each page to support reliable resume.


## Error handling
- SQLite connection is closed in a `finally` block inside `sync_items()` to prevent resource leaks.
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
