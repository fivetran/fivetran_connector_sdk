# IBM Db2 Log-Based Replication Connector Example

## Connector overview

This connector demonstrates **log-based Change Data Capture (CDC)** for IBM Db2 using the Fivetran Connector SDK and IBM's SQL Replication ASN (Apply-Snapshot-Notify) framework.

Unlike timestamp-based or trigger-based polling, this connector reads changes that the **ASN Capture daemon** (`asncap`) extracted directly from the Db2 **transaction log** — the same low-level log file that records every INSERT, UPDATE, and DELETE committed to the database (analogous to MySQL's binlog).

```
Db2 transaction log
    └─► asncap daemon  (reads log via db2ReadLog C API)
            └─► ASN.IBMSNAP_EMPCD  (Change Data table)
                    └─► this connector  (reads CD table → Fivetran destination)
```

After the initial full load, the connector **never queries the source `EMPLOYEE` table again**. All incremental syncs are driven exclusively by what `asncap` wrote after reading the transaction log.

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
* [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
* Operating system:
  * Windows: 10 or later (64-bit only)
  * macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  * Linux: Ubuntu 20.04+, Debian 10+, or Amazon Linux 2+ (arm64 or x86_64)

## Getting started

### 1. Start the Db2 Docker container

```bash
docker-compose up -d
```

The first start takes **3–5 minutes** while Db2 initialises. Wait for the healthcheck to report `healthy`:

```bash
docker-compose ps   # STATUS column should show "(healthy)"
```

### 2. Run the CDC setup script

Copy the setup script into the container and execute it as the `db2inst1` user:

```bash
docker cp setup_cdc.sh db2_cdc:/tmp/setup_cdc.sh
docker exec -it db2_cdc su - db2inst1 -c "bash /tmp/setup_cdc.sh"
```

The script:
1. Creates the `EMPLOYEE` source table and enables `DATA CAPTURE CHANGES`
2. Creates the ASN control tables and the `IBMSNAP_EMPCD` Change Data table
3. Registers `EMPLOYEE` for capture
4. Starts the `asncap` daemon, which begins reading the transaction log

### 3. Configure the connector

Edit `configuration.json` — for the local Docker setup the defaults are:

```json
{
    "hostname": "localhost",
    "port": "50000",
    "database": "SAMPLE",
    "user_id": "db2inst1",
    "password": "password123",
    "schema_name": "DB2INST1"
}
```

### 4. Run the connector

```bash
fivetran debug
```

## How log-based replication works in Db2

### Archival logging (`ARCHIVE_LOGS=true`)

By default Db2 uses **circular logging** — log files are overwritten once they are no longer needed for crash recovery.
Setting `ARCHIVE_LOGS=true` in `docker-compose.yml` switches Db2 to **archival logging**, which preserves log files on disk. This is a prerequisite for any log-reading tool and is equivalent to enabling `binlog_format=ROW` in MySQL.

### `DATA CAPTURE CHANGES`

```sql
ALTER TABLE DB2INST1.EMPLOYEE DATA CAPTURE CHANGES;
```

This tells Db2 to write **full before/after row images** into the transaction log for every mutation to `EMPLOYEE`. Without this flag the log only records which columns changed, not their values — the `asncap` daemon cannot reconstruct the row.

### ASN Capture daemon (`asncap`)

`asncap` is the IBM SQL Replication capture agent shipped with Db2. Internally it calls the **`db2ReadLog` C API** to stream log records in LSN (Log Sequence Number) order. For every log record that belongs to a registered table it writes one row to the corresponding **Change Data (CD) table**, tagging it with:

| Column | Meaning |
|--------|---------|
| `IBMSNAP_OPERATION` | `'I'` insert, `'U'` update, `'D'` delete |
| `IBMSNAP_COMMITSEQ` | LSN of the committing transaction (ordering key) |
| `IBMSNAP_LOGMARKER` | Wall-clock timestamp copied from the log record |
| source columns | Row data at the time of the change |

### What the connector reads

The connector queries `ASN.IBMSNAP_EMPCD` (the CD table for `EMPLOYEE`) filtering by `IBMSNAP_LOGMARKER > last_log_marker`. It checkpoints the new high-water `IBMSNAP_LOGMARKER` after each sync so only new log events are processed next time.

## Configuration file

```json
{
    "hostname": "<YOUR_Db2_HOSTNAME>",
    "port": "<YOUR_Db2_PORT>",
    "database": "<YOUR_Db2_DATABASE_NAME>",
    "user_id": "<YOUR_Db2_USER_ID>",
    "password": "<YOUR_Db2_PASSWORD>",
    "schema_name": "<YOUR_Db2_SCHEMA>"
}
```

| Parameter | Description |
|-----------|-------------|
| `hostname` | Hostname or IP of the Db2 server (`localhost` for Docker) |
| `port` | TCP port — default `50000` |
| `database` | Db2 database name (`SAMPLE` in the Docker setup) |
| `user_id` | Username (`db2inst1` in the Docker setup) |
| `password` | Password |
| `schema_name` | Schema owning the `EMPLOYEE` table (`DB2INST1`) |

Do not check `configuration.json` into version control.

## Requirements file

```
ibm_db==3.2.6
```

Note: `fivetran_connector_sdk` and `requests` are pre-installed in the Fivetran environment; do not declare them in `requirements.txt`.

## Authentication

The connector authenticates with IBM Db2 using `user_id` and `password` supplied via `configuration.json`. SSL is not added to the local Docker connection string; add `SECURITY=SSL` to `create_connection_string()` for cloud-hosted Db2 instances that require it.

## Data handling

| Sync phase | Behaviour |
|------------|-----------|
| **First sync** | Full scan of `EMPLOYEE`; all rows are upserted. The `IBMSNAP_LOGMARKER` high-water mark is captured *before* the scan so concurrent writes are not missed. |
| **Subsequent syncs** | Reads `ASN.IBMSNAP_EMPCD WHERE IBMSNAP_LOGMARKER > last_log_marker`. `'I'`/`'U'` rows → `op.upsert()`; `'D'` rows → `op.delete()`. |
| **Checkpointing** | Every 500 CD rows and at the end of each sync. |

## Error handling

- **Missing configuration keys**: `validate_configuration()` raises `ValueError` before any connection is attempted.
- **Connection failures**: caught, logged with `log.severe()`, and re-raised.
- **Unknown ASN operations**: logged as a warning and skipped; the sync continues.
- **Resumable syncs**: the `IBMSNAP_LOGMARKER` cursor is checkpointed regularly so a mid-sync failure resumes from the last checkpoint.

## Tables created

### `employee`

```json
{
    "table": "employee",
    "primary_key": ["id"],
    "columns": {
        "id": "INT",
        "first_name": "STRING",
        "last_name": "STRING",
        "email": "STRING",
        "department": "STRING",
        "salary": "FLOAT"
    }
}
```

## Source objects created by setup_cdc.sh

| Object | Type | Purpose |
|--------|------|---------|
| `DB2INST1.EMPLOYEE` | Table | Source table (`DATA CAPTURE CHANGES` enabled) |
| `ASN.IBMSNAP_EMPCD` | Table | Change Data table written by `asncap` from the transaction log |
| `ASN.IBMSNAP_REGISTER` | Table | ASN control — maps source tables to their CD tables |
| `ASN.IBMSNAP_CAPPARMS` | Table | ASN Capture daemon configuration parameters |
| `ASN.IBMSNAP_PRUNCNTL` | Table | ASN pruning coordination |
| `ASN.IBMSNAP_CAPMON` | Table | ASN Capture monitoring/heartbeat |
| `asncap` (process) | Daemon | Reads Db2 transaction log → writes to `IBMSNAP_EMPCD` |

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
