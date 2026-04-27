# History Mode Mimicry Using Composite Primary Key

## Connector overview

Fivetran's [History Mode](https://fivetran.com/docs/core-concepts/sync-modes/history-mode) is a sync mode that preserves all historical versions of a record in the destination. Instead of overwriting a row when a source record changes, History Mode inserts a new row for every observed state, keeping a full audit trail.

The Connector SDK does not support History Mode natively. However, you can mimic its behavior by including a timestamp column — such as `updatedAt` — as part of a composite primary key.

This connector demonstrates that pattern. It calls the Fivetran API Playground's `/incremental/timestamp` endpoint and uses `["id", "updatedAt"]` as the primary key. When a record's `updatedAt` value changes between syncs, Fivetran inserts a new row rather than overwriting the existing one, preserving the full history of changes just as History Mode would.


## How this mimics History Mode

### Standard upsert vs composite primary key

| Behavior | `primary_key: ["id"]` | `primary_key: ["id", "updatedAt"]` |
|---|---|---|
| Record updated in source | Old row overwritten | New row added, old row preserved |
| History retained? | No | Yes |

With a single-column primary key, each sync overwrites the previous state of a record. With a composite key that includes `updatedAt`, each distinct `(id, updatedAt)` pair is treated as a unique row — so an update in the source produces a new row in the destination instead of replacing the old one.

### Role of `updatedAt` when `createdAt` is absent

For sources that do not provide a `createdAt` field, `updatedAt` at the time of first sync equals the record's first-observed time — functionally equivalent to `createdAt`. So `updatedAt` alone is sufficient for both the composite primary key and the incremental cursor.

This connector excludes `createdAt` from its schema entirely. If your source does provide `createdAt`, simply omit it from the schema and strip it before upsert — it is not needed for this pattern.

### Limitations

Only the state of a record at the time of each sync is captured. If a record is updated multiple times between two syncs, only the most recent `updatedAt` value will appear in the destination — intermediate states are lost. For more detail, see the Fivetran documentation on [changes to data between syncs](https://fivetran.com/docs/core-concepts/sync-modes/history-mode#changestodatabetweensyncs).

Sync more frequently to reduce the window of missed intermediate updates.


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
fivetran init <project-path> --template examples/common_patterns_for_connectors/records_with_no_created_at_timestamp
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`.
If you do not specify a project path, Fivetran creates the project in your current directory.
For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/setup-guide#createyourcustomconnector).

This connector requires the [Fivetran API Playground](https://github.com/fivetran/fivetran_api_playground) running locally. Start it with:

```bash
playground start
```


## Features

- Mimics Fivetran's History Mode without requiring native History Mode support.
- Uses a composite primary key `["id", "updatedAt"]` to preserve historical row versions.
- Calls the Fivetran API Playground `/incremental/timestamp` endpoint with a cursor-based incremental sync.
- Excludes `createdAt` from the schema — `updatedAt` serves both the history key and the cursor roles.
- Checkpoints state after each page for safe, resumable syncs.


## Configuration file

This example does not require a configuration file.

In production, `configuration.json` might contain API tokens, base URLs, or initial cursors.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file

This connector requires the `requests` library for HTTP calls.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Authentication

This connector calls the Fivetran API Playground, which runs locally and requires no credentials.

If adapting this pattern for an external source, add authentication headers in your request logic and load credentials from the configuration.


## Pagination

The `/incremental/timestamp` endpoint returns all records updated since the `since` parameter in a single response. The connector loops until an empty response is returned.

In real-world connectors, combine this cursor pattern with page-based or keyset pagination for large datasets.


## Data handling

- Records are fetched from `/incremental/timestamp?since=<cursor>`, returning only records updated after the cursor.
- `createdAt` is stripped from each record before upsert — it is not part of the schema for this pattern.
- The composite primary key `["id", "updatedAt"]` means a record with a new `updatedAt` value produces a new row in the destination rather than overwriting the existing one.
- The cursor advances to the latest `updatedAt` seen in each batch and is checkpointed after every page.
- Records are returned sorted by `updatedAt` ascending by the playground, which is a prerequisite for correct cursor advancement.


## Error handling

- `response.raise_for_status()` propagates HTTP errors as exceptions, which the SDK logs and handles.
- Add retry logic and custom error handling as needed for production connectors.


## Tables created

The connector creates the `USER` table:

```
{
  "table": "user",
  "primary_key": ["id", "updatedAt"],
  "columns": {
    "id": "STRING",
    "name": "STRING",
    "email": "STRING",
    "address": "STRING",
    "company": "STRING",
    "job": "STRING",
    "updatedAt": "UTC_DATETIME"
  }
}
```


## Observing History Mode behavior

1. Start the playground: `playground start`
2. Run `fivetran debug` — first sync loads all records; cursor is saved to state.
3. Run `fivetran debug` again — incremental sync fetches records updated since the cursor; records with a changed `updatedAt` produce new rows.
4. Inspect `warehouse.db`:

```sql
SELECT id, updatedAt FROM user ORDER BY id, updatedAt;
```

Rows with the same `id` but different `updatedAt` values confirm that historical versions are being preserved — this is History Mode behavior.


## Additional considerations

- `updatedAt` must be reliable and monotonically increasing per record. If a source can assign the same `updatedAt` to two different updates of the same record, the old row will be overwritten rather than preserved.
- Do not use a low-granularity timestamp (e.g. date-only) as the composite key — two updates on the same day would collapse into one row.
- To adapt this pattern to a real connector, replace `__BASE_URL` with your actual API endpoint and filter by `updated_since=cursor` (or equivalent) in your request parameters.

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
