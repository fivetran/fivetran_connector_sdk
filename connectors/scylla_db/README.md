# ScyllaDB Connector (Custom Fivetran SDK)

## Connector overview

This connector extracts data from a **Cassandra** or **ScyllaDB** cluster and streams it into your Fivetran destination using the **Fivetran Connector SDK**.

It performs **incremental extraction** based on a timestamp column (`updated_at`) and supports **checkpointing** for reliable, resumable syncs.  
The connector discovers all tables in a keyspace automatically and supports upserts for every record.

---

## Features

* Connects securely to a Cassandra/ScyllaDB cluster using **PlainTextAuthProvider**
* Automatically discovers all tables in the keyspace
* Supports **incremental syncs** using `updated_at` timestamps
* Emits **Upsert** operations for each row
* Periodically checkpoints for restartability
* Dynamically maps Cassandra/Scylla data types to Fivetran-compatible types
* Uses **DCAwareRoundRobinPolicy** and **TokenAwarePolicy** for balanced query routing

---

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
* Scylla DB credentials: (`host`, `port`, `username`, `password`, `keyspace`, `dc_region`)
* Cassandra or ScyllaDB cluster (v3.x or later)
* Network access to port (eg `9042`) (CQL native transport)

**Required fields**

| Field | Description |
|--------|-------------|
| `host_1` | Primary Cassandra node hostname |
| `host_2` | Secondary node hostname (optional) |
| `host_3` | Tertiary node hostname (optional) |
| `port` | CQL port (default 9042) |
| `username` | Cassandra username |
| `password` | Cassandra password |
| `dc_region` | Data center name for load balancing |
| `keyspace` | Cassandra keyspace to sync |

Example `configuration.json`:

```json
{
  "host_1": "<YOUR_SCYLLA_DB_HOST_1>",
  "host_2": "<YOUR_SCYLLA_DB_HOST_2>",
  "host_3": "<YOUR_SCYLLA_DB_HOST_3>",
  "port": "<YOUR_SCYLLA_DB_PORT>",
  "username": "<YOUR_SCYLLA_DB_USERNAME>",
  "password": "<_YOURSCYLLA_DB_YOUR_PASSWORD>",
  "dc_region": "<YOUR_SCYLLA_DB_DATA_CENTER_REGION>",
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

The connector uses **PlainTextAuthProvider** to authenticate with Cassandra/ScyllaDB:

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
    - `op.upsert(table, row_dict)` for each row
    - `op.checkpoint(state)` every 1000 rows
5. Saves the last processed timestamp per table in the connector `state`.

---

## Incremental sync logic

| Setting | Description |
|----------|--------------|
| `__UPDATED_AT_COLUMN` | Column used for incremental syncs (`updated_at`) |
| `__INITIAL_SYNC_START` | Default starting timestamp (`2025-10-19 00:00:00`) |
| `__INTERVAL_HOURS` | Batch window (6 hours) |
| `__CHECKPOINT_INTERVAL` | Checkpoint every 1000 records |


---

## Schema discovery

The connector dynamically builds the schema via Cassandra’s system tables:

- **Partition and clustering keys** are used as primary keys.
- **Data types** are mapped to Fivetran types (e.g., `text` → `STRING`, `timestamp` → `UTC_DATETIME`).

---

## Data type mapping

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

| Error | Description | Resolution |
|--------|-------------|------------|
| Missing config field | Configuration validation failed | Add missing keys in `configuration.json` |
| Connection error | Cluster unreachable | Check hostnames, ports, and auth credentials |
| Query error | Table or column not found | Verify keyspace/table exist and are accessible |
| Missing `updated_at` | Table has no incremental column | Add timestamp or perform full table sync |
| Schema drift | New column added | Detected automatically on next run |

All errors are logged using the Fivetran SDK logging system.

---

## Additional considerations

* Use **UTC timestamps** in your `updated_at` column for consistent incremental syncs.
* Ensure `updated_at` is indexed to avoid full-table scans.
* If some tables don’t have `updated_at`, they will sync fully each run.
* The connector supports both **Cassandra** and **ScyllaDB** seamlessly.
* For large tables, tune `__INTERVAL_HOURS` and `__CHECKPOINT_INTERVAL` for better performance.

---