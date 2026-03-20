# Sybase ASE Connector for Fivetran

A production-ready Fivetran Connector SDK implementation for Sybase ASE that dynamically discovers and syncs all tables in a database to any Fivetran-supported destination.

---

## Repository Contents

| File | Description |
|------|-------------|
| `connector.py` | Fivetran Connector SDK implementation |
| `sybase_ase_load_test.py` | Load testing script for validating database connectivity and performance |
| `configuration.json.example` | Example configuration file (copy to `configuration.json` and fill in your values) |
| `requirements.txt` | Python dependencies |
| `drivers/installation.sh` | FreeTDS driver installation script (Linux) |

---

## Prerequisites

### Python Dependencies

```bash
pip install pyodbc fivetran-connector-sdk
```

### FreeTDS Driver

FreeTDS is required for all connectivity to Sybase ASE.

```bash
# macOS
brew install freetds

# Linux (or use the included script)
bash drivers/installation.sh
```

### Configuration

Copy the example configuration file and fill in your values:

```bash
cp configuration.json.example configuration.json
```

```json
{
  "server": "<YOUR_SYBASE_ASE_SERVER>",
  "port": "<YOUR_SYBASE_ASE_PORT>",
  "database": "<YOUR_SYBASE_ASE_DATABASE>",
  "user_id": "<YOUR_SYBASE_ASE_USER_ID>",
  "password": "<YOUR_SYBASE_ASE_PASSWORD>"
}
```

To sync only specific tables, add an optional `tables` field (comma-separated):

```json
{
  "server": "<YOUR_SYBASE_ASE_SERVER>",
  "port": "<YOUR_SYBASE_ASE_PORT>",
  "database": "<YOUR_SYBASE_ASE_DATABASE>",
  "user_id": "<YOUR_SYBASE_ASE_USER_ID>",
  "password": "<YOUR_SYBASE_ASE_PASSWORD>",
  "tables": "sales,authors,titles"
}
```

If `tables` is omitted, all user tables in the database are synced automatically.

---

## Fivetran Connector (connector.py)

### How It Works

On each sync the connector:

1. Connects to Sybase ASE via FreeTDS and pyodbc
2. Queries `sysobjects` to discover all user tables in the configured database
3. For each table, detects primary keys via `sp_helpindex` and maps column types from `syscolumns`
4. Determines the best incremental column per table (prefers `datetime` columns, falls back to the first primary key)
5. Fetches rows incrementally using a per-table watermark stored in state, or performs a full sync if no incremental column exists
6. Converts Python types (e.g. `Decimal`, `datetime`) to Fivetran-compatible formats
7. Upserts rows to the destination in batches of 1,000 and checkpoints state after each batch

### Schema Discovery

The connector dynamically builds the schema at sync time — no manual table or column definitions required. It maps Sybase ASE types to Fivetran types as follows:

| Sybase Type | Fivetran Type |
|-------------|---------------|
| `char`, `varchar`, `text`, `unichar`, `univarchar` | `STRING` |
| `int`, `integer` | `INT` |
| `smallint`, `tinyint` | `SHORT` |
| `bigint` | `LONG` |
| `float` | `DOUBLE` |
| `real` | `FLOAT` |
| `decimal`, `numeric`, `money`, `smallmoney` | `DECIMAL` (with precision and scale) |
| `datetime`, `smalldatetime`, `bigdatetime` | `NAIVE_DATETIME` |
| `date` | `NAIVE_DATE` |
| `time`, `bigtime` | `NAIVE_TIME` |
| `binary`, `varbinary`, `image` | `BINARY` |
| `bit` | `BOOLEAN` |

Any unmapped type defaults to `STRING`.

### Key Functions

| Function | Description |
|----------|-------------|
| `validate_configuration(configuration)` | Checks all required fields are present |
| `schema(configuration)` | Dynamically discovers all tables and returns schema definitions |
| `get_sybase_tables(connection, database)` | Queries `sysobjects` for all user tables |
| `get_table_primary_keys(connection, table_name)` | Detects primary keys via `sp_helpindex` |
| `get_table_columns(connection, table_name)` | Reads column names and types from `syscolumns` |
| `get_table_incremental_column(connection, table_name)` | Finds the best column for incremental sync |
| `map_sybase_to_fivetran_type(sybase_type, precision, scale)` | Maps Sybase types to Fivetran types |
| `create_sybase_connection(configuration)` | Establishes a pyodbc connection via FreeTDS |
| `fetch_and_upsert(cursor, query, table_name, state, ...)` | Fetches rows in batches and emits upsert operations |
| `sync_table(connection, table_name, state)` | Orchestrates the sync for a single table |
| `update(configuration, state)` | Entry point called by Fivetran on each sync |

### Local Testing

```bash
python connector.py
```

This calls `connector.debug()` which initializes the full Fivetran SDK runtime, including logging, and runs a local test sync writing results to `files/warehouse.db`.

### Deploying to Fivetran

```bash
fivetran deploy \
  --api-key <YOUR_API_KEY> \
  --destination <YOUR_DESTINATION_NAME> \
  --connection <YOUR_CONNECTION_NAME> \
  --configuration configuration.json
```

---

## Load Test (sybase_ase_load_test.py)

A script for validating database connectivity and generating workload against the Sybase ASE database.

### Usage

```bash
python sybase_ase_load_test.py
```

Presents an interactive menu:

```
1. Run write load test (inserts, updates, deletes)
2. Run read-only load test (queries only)
3. Exit
```

The script reads connection details from `configuration.json` automatically.

### Write Load Test

Generates a realistic mixed workload:
- Inserts random `sales` records
- Inserts random `salesdetail` records
- Updates `titles` prices randomly
- Deletes old `sales` records (every 5 cycles)

### Read-Only Load Test

Runs SELECT queries only — safe for use against production databases or when the transaction log is constrained.

> **Note:** Write operations require available transaction log space. If you see `transaction log is full` errors, truncate the log before running write tests:
> ```sql
> USE <your_database>
> DUMP TRANSACTION <your_database> WITH TRUNCATE_ONLY
> ```

---

## Troubleshooting

### Connection Failures

```bash
# Verify basic connectivity
tsql -S <your_server> -p <your_port> -U <your_user> -P <your_password>

# Check FreeTDS ODBC registration
odbcinst -q -d

# Enable FreeTDS debug logging
export TDSDUMP=/tmp/freetds.log
python connector.py
cat /tmp/freetds.log
```

### Tables Not Appearing

Verify the user has `SELECT` access on `sysobjects`, `syscolumns`, and `systypes` in the target database, as these system tables are used for schema discovery.

### DECIMAL Type Errors

The connector converts Python `Decimal` objects to strings before passing them to Fivetran's protobuf layer. If you encounter type errors on numeric columns, ensure you are running the latest version of `connector.py`.

---

## Version

Version: 2.0
Last Updated: 2026-03-20
