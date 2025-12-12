# SingleStoreDB Connector (Custom Fivetran SDK)

## Connector overview

This connector extracts data from a **SingleStoreDB** instance using **PyMySQL** and loads it into your Fivetran destination using the **Fivetran Connector SDK**.

It supports:
- **Automatic schema discovery** for all tables in a database
- **Incremental syncs** using the `updated_at` column
- **Checkpoints** for recovery and resumable syncs
- **Upserts** for every record fetched

This connector can be used for **initial full loads** and **ongoing incremental updates** across all tables.

---

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
* Operating Systems:
    * Windows 10 or later
    * macOS 13 (Ventura) or later
    * Linux: Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+ (arm64 or x86_64)
* A running **SingleStoreDB** instance (on-prem or in the cloud)
* Network access to port `3306` (or your custom MySQL-compatible port)

---

## Features

* Connects to SingleStoreDB securely using PyMySQL
* Discovers tables and primary keys dynamically
* Performs incremental syncs using `updated_at` (if present)
* Automatically handles schema inference and column typing
* Emits **Upsert** and **Checkpoint** operations
* Creates a synthetic `_fivetran_row_id` when no primary key exists
* Supports large datasets with batch checkpoints every 1,000 rows

---

## Configuration file

Example `configuration.json`:

```json
{
  "host": "your-singlestore-host",
  "port": 3306,
  "user": "fivetran_user",
  "password": "your_password",
  "database": "your_database"
}
```

**Required fields**
| Field | Description |
|--------|-------------|
| `host` | Hostname or IP address of your SingleStoreDB server |
| `port` | Port number (default: 3306) |
| `user` | Database username |
| `password` | Database password |
| `database` | The schema/database to sync |

> ❗ The connector validates that all required fields exist before running.

---

## Requirements file

Example `requirements.txt`:

```text
fivetran-connector-sdk
pymysql
```

---

## Authentication

The connector connects directly to your SingleStoreDB instance using **username and password authentication**.  
It uses SSL but disables strict certificate validation for testing:

```python
ssl={"cert_reqs": 0}
```

> ⚠️ For production, update this to use proper SSL verification.

---

## Data handling and flow

1. **Connect to SingleStoreDB** via `pymysql.connect()`
2. **Discover schema** dynamically using `INFORMATION_SCHEMA.TABLES` and `COLUMNS`
3. For each table:
    - Identify **primary key(s)**; fall back to `_fivetran_row_id` if none exist
    - If the table has an `updated_at` column → run incremental query
    - If not, perform a one-time full extract
4. Emit:
    - `op.Upsert(table, row)` for every record
    - `op.Checkpoint(state)` every 1,000 records
5. Save the last `updated_at` timestamp per table in the `state` for incremental continuation.

---

## Incremental sync logic

| Setting | Description |
|----------|--------------|
| `__UPDATED_AT_COLUMN` | Column used for incremental syncs (`updated_at`) |
| `__INITIAL_SYNC_START` | Default starting timestamp (`2025-10-01 00:00:00`) |
| `__INTERVAL_HOURS` | Batch window per query (6 hours) |
| `__CHECKPOINT_INTERVAL` | Checkpoint every 1,000 rows |

**Query example**
```sql
SELECT * FROM orders
WHERE updated_at > %s AND updated_at <= %s;
```

If no `updated_at` column exists, the connector extracts all data once and stops syncing that table incrementally.

---

## Schema discovery

The connector automatically inspects your database schema using:
```sql
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='<database>';
SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='<database>';
```

Primary keys are retrieved using:
```sql
SELECT COLUMN_NAME, COLUMN_KEY
FROM INFORMATION_SCHEMA.COLUMNS
WHERE COLUMN_KEY='PRI';
```

If no primary key exists, `_fivetran_row_id` is added automatically.

---

## Checkpointing

Checkpoints are sent periodically to ensure the connector can **resume from the last sync point** if interrupted.

```python
if count % __CHECKPOINT_INTERVAL == 0:
    op.checkpoint(state)
```

Each checkpoint updates:
```json
{
  "table_name": {
    "last_updated_at": "2025-10-21 13:45:00"
  }
}
```

---

## Error handling

| Error | Description | Action |
|--------|-------------|--------|
| Connection failure | Invalid host/port/user credentials | Retry or fix credentials |
| Missing column | Schema drift (new column added) | Automatically handled next sync |
| No `updated_at` column | Table performs full load only | Logged as info |
| SQL syntax error | Reserved keywords or missing quotes | Automatically quoted via `quote_identifier()` |

All errors are logged via `fivetran_connector_sdk.Logging`.

---

## Example Log Output

```
Connected to SingleStoreDB!
Querying orders from 2025-10-01 00:00:00 to 2025-10-01 06:00:00
Number of rows fetched: 5000
Processed 1000 records for orders
Checkpointed at 2025-10-01 06:00:00
Sync completed successfully.
```

---

## Local testing

Run the connector locally for debugging:
```bash
fivetran debug --configuration configuration.json
```

You’ll see metrics similar to:
```
Operation       | Calls
----------------+------------
Upserts         | 118,276
Updates         | 0
Deletes         | 0
Truncates       | 0
SchemaChanges   | 1
Checkpoints     | 121
```

---

## Deployment

Deploy to Fivetran with:
```bash
fivetran deploy --destination <DESTINATION_NAME>                 --connection singlestore_connector                 --configuration configuration.json
```

---

## Additional considerations

* Ensure `updated_at` exists on tables you want to sync incrementally
* Use UTC timestamps for all datetime comparisons
* Add indexes on `updated_at` for best performance
* Avoid using reserved words like `table`, `user`, or `group` as column names (or they’ll be auto-quoted)
* For large data volumes, increase `__INTERVAL_HOURS` or reduce `__CHECKPOINT_INTERVAL`

---
