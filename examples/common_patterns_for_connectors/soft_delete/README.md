# Soft delete example

## Connector overview

This example demonstrates Fivetran's soft delete feature using the truncate-and-reload pattern. On each sync, `op.truncate()` marks all existing warehouse rows as soft-deleted (`_fivetran_deleted = True`), then the full current snapshot is re-upserted. Any row that reappears in the upsert has its `_fivetran_deleted` flag cleared automatically. Rows absent from the new snapshot remain marked as deleted — this is the soft-delete behavior.

Use this pattern when your source system provides a full snapshot of current state on each sync, with no native deleted flag or changelog. Rather than physically removing rows from the warehouse, soft delete preserves historical data while clearly marking which records are no longer active.


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
fivetran init <project-path> --template examples/common_patterns_for_connectors/soft_delete
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`.
If you do not specify a project path, Fivetran creates the project in your current directory.
For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/setup-guide#createyourcustomconnector).


## Features

- Demonstrates the truncate-and-reload soft delete pattern using `op.truncate()` and `op.upsert()`
- Skips truncate on the first sync when the destination table is empty, using state as the guard
- Tracks sync count in state to simulate two successive syncs with different data snapshots
- No external dependencies or configuration required — runs immediately with `fivetran debug`


## Data handling

The connector uses two hardcoded snapshots to illustrate the soft delete lifecycle. Refer to `fetch_snapshot(sync_count)`.

Sync 1 — full load:
- `fetch_snapshot(1)` returns all 5 employee records.
- State is empty, so `op.truncate()` is skipped.
- All 5 rows are upserted with `_fivetran_deleted = False`.

Sync 2 — rows 4 and 5 removed at the source:
- `fetch_snapshot(2)` returns 3 employee records (IDs 1, 2, 3).
- State is non-empty, so `op.truncate()` marks all 5 existing warehouse rows as `_fivetran_deleted = True`.
- The 3 surviving rows are re-upserted, clearing their `_fivetran_deleted` flag back to `False`.
- IDs 4 and 5 are absent from the upsert and remain soft-deleted.

In a real connector, replace the hardcoded snapshots in `fetch_snapshot()` with a call to your source system.


## Tables created

### `employees`

| Column | Type | Description |
|---|---|---|
| `id` | INT | Primary key |
| `name` | STRING | Employee name |
| `department` | STRING | Department the employee belongs to |
| `_fivetran_deleted` | BOOLEAN | `true` for rows absent from the latest snapshot, managed by Fivetran |

After sync 1 — all 5 employees active:

| id | name | department | _fivetran_deleted |
|---|---|---|---|
| 1 | Alice | Engineering | false |
| 2 | Bob | Marketing | false |
| 3 | Charlie | Engineering | false |
| 4 | Diana | HR | false |
| 5 | Edward | Finance | false |

After sync 2 — IDs 4 and 5 soft-deleted:

| id | name | department | _fivetran_deleted |
|---|---|---|---|
| 1 | Alice | Engineering | false |
| 2 | Bob | Marketing | false |
| 3 | Charlie | Engineering | false |
| 4 | Diana | HR | true |
| 5 | Edward | Finance | true |


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
