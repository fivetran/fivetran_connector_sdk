# ScyllaDB Connector Example

## Connector overview

This connector extracts data from a Cassandra or ScyllaDB cluster and streams it into your Fivetran destination using the Fivetran Connector SDK.

It performs incremental sync based on a timestamp column (`updated_at`) and supports checkpointing for reliable, resumable syncs.  
The connector discovers all tables in a keyspace automatically and supports upserts for every record.

---

## Features

- Connects securely to a Cassandra/ScyllaDB cluster using PlainTextAuthProvider
- Automatically discovers all tables in the keyspace 
- Supports incremental syncs using `updated_at` timestamps 
- Emits Upsert operations for each row 
- Periodically checkpoints for restartability 
- Dynamically maps Cassandra/Scylla data types to Fivetran-compatible types 
- Uses DCAwareRoundRobinPolicy and TokenAwarePolicy for balanced query routing

---

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
   - Windows: 10 or later (64-bit only)
   - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
   - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- Scylla DB credentials: (`host`, `port`, `username`, `password`, `keyspace`, `data_center_region`)
- Cassandra or ScyllaDB cluster (v3.x or later)
- Network access to port (eg `9042`) (CQL native transport)

Required fields

| Field | Description |
|--------|-------------|
| `host_1` | Primary Cassandra node hostname |
| `host_2` | Secondary node hostname (optional) |
| `host_3` | Tertiary node hostname (optional) |
| `port` | CQL port (default 9042) |
| `username` | Cassandra username |
| `password` | Cassandra password |
| `data_center_region` | Data center name for load balancing |
| `keyspace` | Cassandra keyspace to sync |

Example `configuration.json`:

```json
{
  "host_1": "<YOUR_SCYLLA_DB_HOST_1>",
  "host_2": "<YOUR_SCYLLA_DB_HOST_2>",
  "host_3": "<YOUR_SCYLLA_DB_HOST_3>",
  "port": "<YOUR_SCYLLA_DB_PORT>",
  "username": "<YOUR_SCYLLA_DB_USERNAME>",
  "password": "<YOUR_SCYLLA_DB_PASSWORD>",
  "data_center_region": "<YOUR_SCYLLA_DB_DATA_CENTER_REGION>",
  "keyspace": "<YOUR_SCYLLA_DB_KEYSPACE>"
}
```

> The connector validates all required fields before running.

---

## Requirements file
This connector requires the `cassandra-driver` library to connect to Scylla DB databases and tables.

```
cassandra_driver==3.29.2
```

---

## Authentication

The connector uses PlainTextAuthProvider to authenticate with Cassandra/ScyllaDB:

```python
auth_provider = PlainTextAuthProvider(
    username=configuration.get("username"),
    password=configuration.get("password")
)
```

---

## Data handling

1. Connects to the Cassandra cluster using `Cluster(contact_points=[...])`
2. Discovers all tables in the keyspace using:
   ```sql
   SELECT table_name FROM system_schema.tables WHERE keyspace_name='<keyspace>';
   ```
3. For each table:
    - Reads metadata (`columns`, `primary keys`) from `system_schema.columns`
    - Builds an incremental query using the `updated_at` column
    - Fetches rows in 6-hour batches (`__INTERVAL_HOURS`)
4. Emits:
    - `op.upsert(table, row_dict)` – tells the Fivetran platform to insert a new row into the destination table or update the existing row if the primary key already exists. This is how the connector keeps the destination in sync with the latest source data.
    - `op.checkpoint(state)` every 1000 rows – saves the current connector state (for example, the last processed updated_at per table) so that if a sync is interrupted, the next run can resume from the last checkpoint instead of re-reading all data.
5. Saves the last processed timestamp per table in the connector `state`.

Schema discovery

The connector dynamically builds the schema via Cassandra’s system tables:

- Partition and clustering keys are used as primary keys.
- Data types are mapped to Fivetran types (e.g., `text` → `STRING`, `timestamp` → `UTC_DATETIME`).

Data type mapping

| Cassandra/Scylla Type | Fivetran Type |
|------------------------|---------------|
| text, varchar, ascii | STRING |
| int, smallint, tinyint | INT |
| bigint, varint | LONG |
| float | FLOAT |
| double, decimal | DOUBLE |
| boolean | BOOLEAN |
| timestamp | UTC_DATETIME |
| date | NAIVE_DATE |
| uuid, timeuuid | STRING |
| blob | STRING |

---

## Error handling
- If required configuration fields are missing or empty, `validate_configuration()` raises a `ValueError` and the sync fails before connecting.
- Cluster connection issues in `get_cluster()` are logged with `log.severe()` and re-raised as runtime errors.
- Runtime query or processing errors in `update()` / `sync_table()` are caught at the top level, logged with `log.severe()`, and cause the sync to fail.
- Progress and state updates are logged using `log.info()` to help trace execution and checkpoints.


---

## Additional considerations

- Use UTC timestamps in your `updated_at` column for consistent incremental syncs.
- Ensure `updated_at` is indexed to avoid full-table scans.
- If some tables don’t have `updated_at`, they will sync fully each run.
- The connector supports both Cassandra and ScyllaDB seamlessly.
- For large tables, tune `__INTERVAL_HOURS` and `__CHECKPOINT_INTERVAL` for better performance.

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

---