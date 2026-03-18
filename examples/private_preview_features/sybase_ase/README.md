# Sybase ASE Connector and Load Testing Tools

This repository contains a Fivetran Connector SDK implementation for Sybase ASE and a suite of load testing tools for benchmarking and validating database performance against the pubs2 database.
It was based off the original Sybase ASE custom connector but with 2 changes:
1. Updated connection string with UTF-8 parameter - Kirk Van Arkel
2. Previously was hardcoded to use just the Sales tables in Pubs2 database, now you can select the tables in the schema you would like to sync - Jason Chletsos
## Repository Contents

| File | Description |
|------|-------------|
| `connector.py` | Fivetran Connector SDK implementation |
| `sybase_ase_hammerdb_workload.py` | HammerDB-style load test with mixed read/write transactions |
| `sybase_ase_hammerdb_workload_readonly.py` | Read-only load test, safe for production use |
| `configuration.json` | Database connection configuration |
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

All tools share the same `configuration.json`:

```json
{
  "server": "your_server_address",
  "port": "5000",
  "database": "pubs2",
  "user_id": "sa",
  "password": "your_password"
}
```

---

## Fivetran Connector (connector.py)

A production-ready connector built with the Fivetran Connector SDK that syncs data from Sybase ASE to any Fivetran-supported destination.

### How It Works

The connector connects to Sybase ASE via FreeTDS and pyodbc, queries the `sales` table incrementally using a `date` watermark stored in state, and upserts rows to the destination in batches of 1000.

On each sync:
1. Reads `last_created` from state (defaults to `1970-01-01T00:00:00` on first run)
2. Executes `SELECT * FROM sales WHERE date > '<last_created>' ORDER BY date`
3. Upserts each row to the destination `sales` table
4. Checkpoints state after each batch so syncs can resume safely if interrupted

### Schema

The connector delivers a single `sales` table:

| Column | Type | Notes |
|--------|------|-------|
| `stor_id` | STRING | Part of composite primary key |
| `ord_num` | STRING | Part of composite primary key |
| `date` | NAIVE_DATETIME | Used as the incremental watermark |

### Key Functions

| Function | Description |
|----------|-------------|
| `validate_configuration(configuration)` | Checks all required fields are present before connecting |
| `schema(configuration)` | Returns the table schema definition for Fivetran |
| `create_sybase_connection(configuration)` | Establishes a pyodbc connection via FreeTDS |
| `fetch_and_upsert(cursor, query, table_name, state, batch_size)` | Fetches rows and emits upsert operations |
| `update(configuration, state)` | Entry point called by Fivetran on each sync |
| `close_sybase_connection(connection, cursor)` | Closes cursor and connection cleanly |

### Local Testing

```bash
# Debug the connector locally using the Fivetran SDK debug runner
python connector.py
```

The connector reads `configuration.json` from `/configuration.json` when run directly. Update the path in the `__main__` block if needed.

### Extending the Connector

To sync additional tables, add entries to the list returned by `schema()` and add corresponding queries in `update()`. Follow the same incremental pattern using a date or sequence column as the watermark.

---

## HammerDB-Style Workload (sybase_ase_hammerdb_workload.py)

A TPC-C-inspired load testing tool that generates concurrent mixed read/write transactions against the pubs2 database. Designed to measure throughput, latency, and concurrency behaviour under realistic OLTP conditions.

### Transaction Mix

| Transaction | Weight | Type | Tables Touched |
|-------------|--------|------|----------------|
| New Sale | 45% | Write | sales, salesdetail, titles |
| Payment | 15% | Write | roysched |
| Order Status | 15% | Read | sales, stores, salesdetail, titles |
| Delivery | 10% | Write | salesdetail |
| Stock Level | 10% | Read | titles, salesdetail |
| Author Lookup | 3% | Read | authors, titleauthor, titles |
| Publisher Report | 2% | Read | publishers, titles |

### Usage

```bash
# Run with defaults (10 workers, 60 seconds)
python sybase_ase_hammerdb_workload.py --config configuration.json

# Custom workers and duration
python sybase_ase_hammerdb_workload.py --config configuration.json --workers 20 --duration 300

# Discover schema only, no load test
python sybase_ase_hammerdb_workload.py --config configuration.json --discover-only

# Verbose output for debugging
python sybase_ase_hammerdb_workload.py --config configuration.json --verbose
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--config` | required | Path to configuration.json |
| `--workers` | 10 | Number of concurrent worker threads |
| `--duration` | 60 | Test duration in seconds |
| `--discover-only` | false | Print schema and exit without running the test |
| `--verbose` | false | Print per-transaction errors and worker events |

### Sample Output

```
======================================================================
  HAMMERDB-STYLE WORKLOAD TEST
======================================================================
  Workers:  10
  Duration: 60s
======================================================================

======================================================================
  WORKLOAD RESULTS
======================================================================
  Total Time:     60.12s
  Total TX:       3,245
  Total Errors:   12
  Overall TPS:    53.98

----------------------------------------------------------------------
  Transaction Type     Count   Errors    Avg(ms)        TPS
----------------------------------------------------------------------
  new_sale             1,461        2     234.50      24.31
  payment                487        0     206.12       8.10
  order_status           486        0      37.45       8.08
  delivery               324        0      74.23       5.39
  stock_level            325        8     366.89       5.41
  author_lookup           97        0     147.67       1.61
  publisher_report        65        2     221.34       1.08
======================================================================
```

### Architecture

Each worker thread maintains its own database connection. Transactions are selected randomly according to the weighted mix. Results are recorded into a thread-safe `WorkloadStats` collector and printed as a summary at the end.

---

## Read-Only Workload (sybase_ase_hammerdb_workload_readonly.py)

An alternative load test that runs SELECT-only queries. Use this when write operations are not appropriate, such as testing against a production database or when the transaction log is constrained.

### Transaction Mix

| Transaction | Weight | Description |
|-------------|--------|-------------|
| Order Status | 25% | Multi-table join across sales, stores, salesdetail, titles |
| Stock Level | 20% | Inventory aggregation on titles and salesdetail |
| Author Lookup | 20% | Join across authors, titleauthor, titles |
| Publisher Report | 15% | GROUP BY aggregation on publishers and titles |
| Sales Analysis | 10% | Store-level sales summary |
| Title Search | 5% | Filter titles by type |
| Royalty Report | 5% | Estimated royalty calculation per author |

### Usage

```bash
# Run with defaults (10 workers, 60 seconds)
python sybase_ase_hammerdb_workload_readonly.py --config configuration.json

# Custom workers and duration
python sybase_ase_hammerdb_workload_readonly.py --config configuration.json --workers 20 --duration 300

# Verbose output
python sybase_ase_hammerdb_workload_readonly.py --config configuration.json --verbose
```

### Key Difference from Main Workload

Connections are opened with `autocommit=True`. No `BEGIN`/`COMMIT`/`ROLLBACK` is issued, so there is no transaction log growth and no risk of lock contention from writes.

---

## Performance Expectations

Results vary significantly based on network proximity to the Sybase server. The figures below assume a local or low-latency network connection.

### Mixed Workload

| Workers | Expected TPS | Expected Error Rate |
|---------|-------------|---------------------|
| 5 | 10-30 | < 5% |
| 10 | 30-60 | < 10% |
| 20 | 50-120 | < 15% |
| 50+ | 100-300 | < 20% |

### Read-Only Workload

| Workers | Expected TPS | Expected Error Rate |
|---------|-------------|---------------------|
| 10 | 50-100 | < 5% |
| 20 | 100-200 | < 5% |
| 50+ | 200-500 | < 10% |

---

## Troubleshooting

### Connection Failures

```bash
# Verify basic connectivity
tsql -S your_server -p 5000 -U sa -P your_password

# Enable FreeTDS debug logging
export TDSDUMP=/tmp/freetds.log
python sybase_ase_hammerdb_workload.py --config configuration.json
cat /tmp/freetds.log

# Check FreeTDS ODBC registration
odbcinst -q -d
```

### High Error Rate

Reduce the number of workers to lower lock contention:

```bash
python sybase_ase_hammerdb_workload.py --config configuration.json --workers 3
```

Switch to the read-only workload to confirm queries work in isolation:

```bash
python sybase_ase_hammerdb_workload_readonly.py --config configuration.json --workers 10
```

Check Sybase for blocking or errors:

```sql
sp_who
sp_lock
```

### Transaction Log Full

The mixed workload generates writes that consume transaction log space. If you see `Can't allocate space for object 'syslogs'`, truncate the log:

```sql
USE pubs2
DUMP TRANSACTION pubs2 WITH TRUNCATE_ONLY
```

Alternatively, switch to the read-only workload for the duration of testing.

### Low TPS

- Run the workload from the same network as the Sybase server to eliminate round-trip latency
- Check server resource usage with `sp_sysmon '00:01:00'`
- Verify indexes exist on frequently queried columns

---

## Additional Documentation

| File | Contents |
|------|----------|
| `QUICK_REFERENCE.md` | Command cheat sheet and common troubleshooting steps |
| `README_HAMMERDB_WORKLOAD.md` | Detailed workload documentation including architecture and tuning |
| `WORKLOAD_RESULTS.md` | Recorded test results and analysis from the pubs2 test environment |
| `INDEX.md` | Documentation navigation guide |

---

## Version

Version: 1.0
Last Updated: 2026-03-11
