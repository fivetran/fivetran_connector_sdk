# IBM Db2 Log-Based Replication Connector Example

## Connector overview

This connector demonstrates log-based Change Data Capture (CDC) for IBM Db2 using the Fivetran Connector SDK and IBM's SQL Replication ASN (Apply-Snapshot-Notify) framework.

The ASN Capture daemon (`asncap`) reads the Db2 transaction log and writes every INSERT, UPDATE, and DELETE to a Change Data (CD) table. This connector reads exclusively from that CD table after the initial full load — it never queries the source table again, making this genuine log-based replication.

```
Db2 transaction log
    └─► asncap daemon  (reads Db2 transaction log)
            └─► DB2INST1.CDEMPLOYEE  (Change Data table)
                    └─► this connector  (reads CD table → Fivetran destination)
```


## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)


## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## Features

- Log-based CDC using the ASN SQL Replication framework — no polling of the source table after the initial load
- Full initial load on first sync followed by incremental log-event processing on subsequent syncs
- Ordered change application using `IBMSNAP_COMMITSEQ` (the Db2 Log Sequence Number) as the cursor
- Handles inserts, updates, and deletes sourced from the Db2 transaction log
- Regular checkpointing every 500 rows for resumable syncs


## Configuration file

The connector uses `configuration.json` to define the connection parameters for the IBM Db2 database.

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

The configuration parameters are:
- `hostname` – hostname or IP of the Db2 server
- `port` – TCP port number; default Db2 port is `50000`
- `database` – Db2 database name
- `user_id` – username used to authenticate with Db2
- `password` – password used to authenticate with Db2
- `schema_name` – schema that owns the source `EMPLOYEE` table

Note: When submitting connector code as a [Community Connector](https://github.com/fivetran/fivetran_connector_sdk/tree/main/connectors) or enhancing an [example](https://github.com/fivetran/fivetran_connector_sdk/tree/main/examples) in the open-source [Connector SDK repository](https://github.com/fivetran/fivetran_connector_sdk/tree/main), ensure the `configuration.json` file has placeholder values.
When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file

The `requirements.txt` file specifies the Python library required by the connector:

```
ibm_db==3.2.6
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Authentication

The connector authenticates with IBM Db2 using `user_id` and `password` supplied via `configuration.json`. These credentials are passed to the `ibm_db.connect()` call inside `connect_to_database()`. Add `SECURITY=SSL` to `create_connection_string()` for Db2 instances that require encrypted connections.


## Data handling

The ASN Capture daemon (`asncap`) reads the Db2 transaction log and writes one row to the Change Data table (`DB2INST1.CDEMPLOYEE`) for every committed INSERT, UPDATE, or DELETE on the source table. Each CD row contains:

- `IBMSNAP_OPERATION` – `'I'` insert, `'U'` update, `'D'` delete
- `IBMSNAP_COMMITSEQ` – binary Log Sequence Number (LSN) of the commit, used as the ordering key and cursor
- `IBMSNAP_INTENTSEQ` – position within the transaction, used as secondary ordering key
- source columns – row data at the time of the change

The connector processes this data in two phases:

Initial sync:
- `perform_initial_load()` performs a full scan of the `EMPLOYEE` table and upserts every row to the destination.
- The `IBMSNAP_COMMITSEQ` high-water mark is captured before the scan starts, so any changes written to the CD table during the scan are not missed on the next sync.

Incremental sync:
- `process_cdc_changes()` reads only the rows in the Change Data table that are newer than the last processed commit sequence, ensuring changes are applied in the exact order they were committed to the database.
- `'I'` and `'U'` rows are applied as `op.upsert()`; `'D'` rows are applied as `op.delete()`.
- The new high-water `IBMSNAP_COMMITSEQ` hex value is saved to state after processing.

### Checkpointing
- State is checkpointed every 500 CD rows and once more at the end of each sync, so a mid-sync failure resumes from the last checkpoint rather than from the beginning.


## Error handling

Refer to `validate_configuration()` and `connect_to_database()` for implementation details.

- Missing or empty configuration keys – `validate_configuration()` raises a `ValueError` before any connection is attempted, identifying the missing key.
- Invalid port – `validate_configuration()` checks that `port` is an integer in the range 1–65535 and raises a `ValueError` with the actual value if not.
- Connection failures – these are caught in `connect_to_database()`, logged with `log.severe()`, and re-raised as a `RuntimeError`.
- Unknown ASN operations – these are logged as a warning by `process_cdc_changes()` and skipped; the sync continues.
- Resumable syncs – the `IBMSNAP_COMMITSEQ` cursor is checkpointed regularly so a mid-sync failure resumes from the last checkpoint.


## Tables created

The connector creates and syncs the `employee` table in the destination:

| Column | Type | Primary key |
|--------|------|-------------|
| `id` | INT | Yes |
| `first_name` | STRING | No |
| `last_name` | STRING | No |
| `email` | STRING | No |
| `department` | STRING | No |
| `salary` | FLOAT | No |


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
