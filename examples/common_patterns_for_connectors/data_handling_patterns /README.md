# Data handling patterns connector example

## Connector overview

This example demonstrates three common techniques for handling nested data structures returned by an API source. Rather than connecting to a live API, it uses a mock API (`mock_api.py`) to simulate realistic source data shapes, making it easy to run and explore locally without any credentials or configuration.

Each of the three sync functions in `connector.py` targets a separate destination table and applies a different data handling pattern:

- Flatten into columns – when the source returns a single nested object alongside top-level fields, its fields are promoted to individual columns on the parent table.
- Break out into child tables – when the source returns a list of nested objects inside a parent record, each list item is written to a separate child table linked back to the parent by a foreign key.
- Write as a JSON blob – when the source returns a deeply nested or highly variable object, the entire structure is stored as a single JSON column.


## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)


## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## Features

- Demonstrates three distinct data handling patterns for nested API responses in a single connector.
- Flattens a nested address object into individual destination columns – refer to `sync_flattened()`.
- Splits a nested orders list into a parent table (`users`) and a child table (`orders`) linked by a foreign key – refer to `sync_parent_child()`.
- Stores a complex, variable-structure metadata object as a JSON blob column – refer to `sync_json_blob()`.
- Uses a mock API to generate synthetic source data, enabling local testing without credentials or configuration.
- Checkpoints state after every table sync and at a configurable interval (`CHECKPOINT_INTERVAL`) to support safe resumption after interruptions.


## Requirements file

The `requirements.txt` file lists the Python libraries required by the connector. This example uses the `faker` library in `mock_api.py` to generate synthetic source data for local testing.

```
faker==40.5.1
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Data handling

This connector demonstrates three patterns for transforming nested API data into flat destination tables. Refer to the corresponding sync functions in `connector.py` for implementation details.

### Flatten into columns

Refer to `sync_flattened()`.

When the source returns a single nested object alongside top-level fields, each field in the nested object can be promoted to a top-level column on the parent table. This avoids joins at query time and keeps related data in a single row.

Source shape:
```json
{
  "id": "1",
  "name": "Alice",
  "address": {"city": "New York", "zip": "10001"}
}
```

Destination — `users_flattened`:

| id | name  | address_city | address_zip |
|----|-------|--------------|-------------|
| 1  | Alice | New York     | 10001       |

### Break out into child tables

Refer to `sync_parent_child()`.

When the source returns a list of nested objects inside a parent record, the list is better modelled as a separate child table. Each item in the list becomes its own row in the child table, with the parent's primary key included as a foreign key. This avoids row duplication in the parent table and allows independent querying of child records.

Source shape:
```json
{
  "user_id": "1",
  "name": "Alice",
  "orders": [
    {"order_id": "A1", "amount": 20},
    {"order_id": "B2", "amount": 35}
  ]
}
```

Destination — `users` (parent):

| user_id | name  |
|---------|-------|
| 1       | Alice |

Destination — `orders` (child):

| order_id | user_id | amount |
|----------|---------|--------|
| A1       | 1       | 20.0   |
| B2       | 1       | 35.0   |

### Write as a JSON blob

Refer to `sync_json_blob()`.

When the source returns a deeply nested or highly variable object whose structure may differ between records, storing it as a single JSON column avoids frequent schema changes and preserves the full structure for downstream consumers. The Connector SDK accepts the Python dictionary directly; no manual serialization is required.

Source shape:
```json
{
  "id": "1",
  "name": "Alice",
  "metadata": {
    "preferences": {"theme": "dark", "notifications": true},
    "history": ["event_1", "event_2"]
  }
}
```

Destination — `users_metadata`:

| id | name  | metadata                                 |
|----|-------|------------------------------------------|
| 1  | Alice | {"preferences": {...}, "history": [...]} |


## Error handling

Refer to `validate_configuration()`.

- Configuration validation – at the start of every sync, `validate_configuration()` checks that all required configuration keys are present and raises a `ValueError` if any are missing. This prevents the sync from proceeding with incomplete credentials or settings.
- Checkpointing – the connector checkpoints state after every table sync completes and at every `CHECKPOINT_INTERVAL` upserts within a table. If a sync is interrupted, Fivetran resumes from the last checkpoint rather than replaying the entire sync from the beginning.

Note: This example requires no configuration, so `validate_configuration()` is included as a template placeholder. When building your own connector, add your required keys to the validation list inside that function.


## Tables created

This connector creates four destination tables across the three data handling patterns.

`users_flattened` — pattern 1, flatten into columns:

| Column | Type | Description |
|--------|------|-------------|
| id (PK) | STRING | Unique user identifier |
| name | STRING | User's full name |
| address_city | STRING | City, flattened from the nested address object |
| address_zip | STRING | ZIP code, flattened from the nested address object |

`users` — pattern 2, parent table:

| Column | Type | Description |
|--------|------|-------------|
| user_id (PK) | STRING | Unique user identifier |
| name | STRING | User's full name |

`orders` — pattern 2, child table:

| Column | Type | Description |
|--------|------|-------------|
| order_id (PK) | STRING | Unique order identifier |
| user_id | STRING | Foreign key referencing `users.user_id` |
| amount | DOUBLE | Order total amount |

`users_metadata` — pattern 3, JSON blob:

| Column | Type | Description |
|--------|------|-------------|
| id (PK) | STRING | Unique user identifier |
| name | STRING | User's full name |
| metadata | JSON | Full metadata object stored as a JSON blob |


## Additional files

- `mock_api.py` – A simulated API that generates synthetic user data in three different nested shapes, one for each data handling pattern demonstrated in `connector.py`.


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
