# ScyllaDB Connector (Custom Fivetran SDK)

## Connector overview

This connector extracts data from a **Cassandra** or **ScyllaDB** cluster and streams it into your Fivetran destination using the **Fivetran Connector SDK**.

It performs **incremental extraction** based on a timestamp column (`updated_at`) and supports **checkpointing** for reliable, resumable syncs.  
The connector discovers all tables in a keyspace automatically and supports upserts for every record.

---

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
* Operating Systems:
    * Windows 10 or later
    * macOS 13 (Ventura) or later
    * Linux: Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+ (arm64 or x86_64)
* Cassandra or ScyllaDB cluster (v3.x or later)
* Network access to port `9042` (CQL native transport)

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

## Configuration file

Example `configuration.json`:

```json
{
  "host_1": "cassandra-node1.example.com",
  "host_2": "cassandra-node2.example.com",
  "host_3": "cassandra-node3.example.com",
  "port": 9042,
  "username": "fivetran_user",
  "password": "your_password",
  "dc_region": "AWS_US_EAST_1",
  "keyspace": "your_keyspace"
}
```

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

> ⚠️ The connector validates all required fields before running.

---

## Requirements file

Example `requirements.txt`:

```text
fivetran-connector-sdk
cassandra-driver
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

## Data handling and flow

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
    - `op.Upsert(table, row_dict)` for each row
    - `op.Checkpoint(state)` every 1000 rows
5. Saves the last processed timestamp per table in the connector `state`.

---

## Incremental sync logic

| Setting | Description |
|----------|--------------|
| `__UPDATED_AT_COLUMN` | Column used for incremental syncs (`updated_at`) |
| `__INITIAL_SYNC_START` | Default starting timestamp (`2025-10-19 00:00:00`) |
| `__INTERVAL_HOURS` | Batch window (6 hours) |
| `__CHECKPOINT_INTERVAL` | Checkpoint every 1000 records |

**Example CQL query**

```sql
SELECT * FROM orders
WHERE updated_at > %s AND updated_at <= %s
ALLOW FILTERING;
```

---

## Schema discovery

The connector dynamically builds the schema via Cassandra’s system tables:

```sql
SELECT column_name, kind, type
FROM system_schema.columns
WHERE keyspace_name='<keyspace>' AND table_name='<table>';
```

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

## Checkpointing

Checkpoints are created automatically to ensure the connector can resume from the last processed timestamp.

```python
if count % __CHECKPOINT_INTERVAL == 0:
    op.checkpoint()
```

State example:
```json
{
  "orders": {
    "last_updated_at": "2025-10-21 06:00:00"
  }
}
```

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

## Example log output

```
Configuration validation passed.
Connected to Cassandra cluster.
Querying orders from 2025-10-19 00:00:00 to 2025-10-19 06:00:00
Number of rows fetched: 112
Processed 1000 records for orders
Checkpoint created.
Cluster shutdown
```

---

## Local testing

To test locally:
```bash
fivetran debug --configuration configuration.json
```

### Sample debug summary
```
Operation       | Calls
----------------+------------
Upserts         | 112
Updates         | 0
Deletes         | 0
Truncates       | 0
SchemaChanges   | 3
Checkpoints     | 3
```

---

## Deployment

Deploy to your Fivetran environment:
```bash
fivetran deploy --destination <DESTINATION_NAME>                 --connection cassandra_connector                 --configuration configuration.json
```

---

## Additional considerations

* Use **UTC timestamps** in your `updated_at` column for consistent incremental syncs.
* Ensure `updated_at` is indexed to avoid full-table scans.
* If some tables don’t have `updated_at`, they will sync fully each run.
* The connector supports both **Cassandra** and **ScyllaDB** seamlessly.
* For large tables, tune `__INTERVAL_HOURS` and `__CHECKPOINT_INTERVAL` for better performance.

---