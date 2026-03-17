"""
EHI Connector for Fivetran Connector SDK.
Syncs 100M+ row tables from Microsoft SQL Server (Caboodle and similar) using:
- pyodbc instead of pytds
- Keyset pagination (WHERE repl_key > ? FETCH NEXT ? ROWS ONLY)
- One thread per table — each thread owns its table's state key exclusively
- READ UNCOMMITTED isolation on all connections to avoid lock contention
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For JSON data serialization and configuration parsing
import json

# For exception handling and logging stack traces from worker threads
import traceback

# For concurrent execution of table syncs in multiple threads
from concurrent.futures import ThreadPoolExecutor, as_completed

# For timestamping sync start and completion times in ISO 8601 format
from datetime import datetime, timezone

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# constants specific to this connector
from constants import (
    BATCH_SIZE,
    CHECKPOINT_INTERVAL,
    MAX_WORKERS,
    DEFAULT_TABLE_LIST,
    DEFAULT_TABLE_EXCLUSION_LIST,
    DEFAULT_USE_PK_TIEBREAK,
    DEFAULT_FORCE_FULL_RESYNC,
)

# client and models for managing SQL Server connections and schema metadata
from client import ConnectionPool
from models import SchemaDetector, TableSchema
from readers import KeysetReader, OffsetReader, IncrementalReader

_CACHED_TABLE_SCHEMAS: dict = {}


def validate_configuration(configuration: dict) -> None:
    """
    Validate that all required connection parameters are present in the configuration.
    Args:
        configuration: A dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: If any required configuration parameter is missing.
    """
    required_fields = [
        "mssql_server",
        "mssql_port",
        "mssql_database",
        "mssql_user",
        "mssql_password",
        "mssql_schema",
    ]
    missing_fields = [
        field_name
        for field_name in required_fields
        if not configuration.get(field_name, "").strip()
    ]
    if missing_fields:
        raise ValueError(
            f"Missing required configuration fields: {missing_fields}. "
            "Check configuration.json."
        )


def _parse_table_list() -> tuple:
    """
    Parse DEFAULT_TABLE_LIST and DEFAULT_TABLE_EXCLUSION_LIST from constants.
    Returns:
        (included_names, excluded_names) where:
          included_names — list of table names to sync, or None (= sync all)
          excluded_names — frozenset of lowercase names to exclude (applied
                           both to explicit lists and to discovered tables)
    """
    excluded_names = (
        frozenset(
            name.strip().lower()
            for name in DEFAULT_TABLE_EXCLUSION_LIST.split(",")
            if name.strip()
        )
        if DEFAULT_TABLE_EXCLUSION_LIST
        else frozenset()
    )

    if DEFAULT_TABLE_LIST:
        included_names = [
            name.strip()
            for name in DEFAULT_TABLE_LIST.split(",")
            if name.strip() and name.strip().lower() not in excluded_names
        ]
        return (included_names or None), excluded_names

    # No explicit include list — discover all tables; exclusion applied later
    return None, excluded_names


def _get_table_sizes_batch(pool: ConnectionPool, schema_name: str, table_names: list) -> dict:
    """
    Return estimated row counts in a single SQL query using sys.partitions.
    Returns dict mapping table_name -> int row count (0 when not found).
    Args:
        pool: ConnectionPool to acquire a connection from
        schema_name: SQL Server schema name (e.g. 'dbo')
        table_names: list of table names to get sizes for
    """
    sql = """
        SELECT
            OBJECT_NAME(i.object_id) AS table_name,
            SUM(p.rows)              AS row_count
        FROM sys.indexes    i
        JOIN sys.partitions p
            ON  i.object_id = p.object_id
            AND i.index_id  = p.index_id
        WHERE i.index_id <= 1
          AND OBJECT_SCHEMA_NAME(i.object_id) = ?
        GROUP BY i.object_id
    """
    with pool.acquire() as conn:
        cursor = conn.execute_with_retry(sql, (schema_name,))
        rows = cursor.fetchall()
        cursor.close()

    size_map = {row[0]: int(row[1] or 0) for row in rows}
    return {name: size_map.get(name, 0) for name in table_names}


def _determine_mode(table_state: dict, table_schema: TableSchema) -> str:
    """
    Decide whether to run a full load or incremental load for a table.
    Decision tree:
      DEFAULT_FORCE_FULL_RESYNC=True → 'full' or 'full_offset'
      No prior state                 → 'full' or 'full_offset'
      mode='full', incomplete        → resume in-progress full load
      mode='full', completed         → 'incremental' (if replication key exists)
      mode='incremental'             → 'incremental'
    Args:
        table_state: dict from state[table_name] with keys like 'mode' and
                    'sync_completed_at' (ISO 8601 string or None)
        table_schema: TableSchema object with replication_key attribute
    Returns:
        one of: 'full', 'full_offset', 'incremental'
    """
    has_replication_key = table_schema.replication_key is not None

    def _full_mode():
        return "full" if has_replication_key else "full_offset"

    if DEFAULT_FORCE_FULL_RESYNC:
        return _full_mode()

    if not table_state:
        return _full_mode()

    prior_mode = table_state.get("mode", "")

    if prior_mode in ("full", "full_offset"):
        if table_state.get("sync_completed_at") is None:
            return prior_mode  # resume interrupted full load
        return "incremental" if has_replication_key else _full_mode()

    if prior_mode == "incremental":
        return "incremental"

    return _full_mode()


def _save_checkpoint(
    state: dict,
    table_name: str,
    table_state: dict,
    mode: str,
    has_repl_key: bool,
    marker,
    rows_synced: int,
    completed: bool = False,
) -> None:
    """
    Persist cursor position and call op.checkpoint().
    Uses a single state key ('last_seen_replication_value' for keyset tables,
    'last_offset' for offset tables) so the duplicated if/else in sync
    functions is replaced by a single call here.
    Args:
        state: global state dict to update
        table_name: name of the table being synced (state key)
        table_state: dict with existing state for this table, to be updated and saved
        mode: sync mode string to save in state for next run ('full', 'full_offset
                or 'incremental')
        has_repl_key: whether this table has a replication key (determines state key name)
        marker: the value of the replication key or offset to save as the cursor position
        rows_synced: total number of rows synced so far, to be saved in state
        completed: whether the sync is complete (used to set sync_completed_at timestamp)
    """
    if has_repl_key:
        table_state["last_seen_replication_value"] = marker
    else:
        table_state["last_offset"] = marker
    table_state["rows_synced"] = rows_synced
    table_state["mode"] = mode
    table_state["sync_completed_at"] = (
        datetime.now(timezone.utc).isoformat() if completed else None
    )
    state[table_name] = table_state
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state)


def _sync_full(
    table_schema: TableSchema,
    state: dict,
    table_state: dict,
    pool: ConnectionPool,
    batch_size: int,
    checkpoint_interval: int,
) -> None:
    """
    Full load using keyset (tables with replication key) or offset pagination.
    Checkpoints every checkpoint_interval rows so an interrupted sync can
    resume without re-scanning previously committed rows.
    Args:
        table_schema: TableSchema object with metadata for the table being synced
        state: global state dict to update with progress
        table_state: dict with existing state for this table, to be updated and saved
        pool: ConnectionPool to acquire SQL connections from
        batch_size: number of rows to fetch per batch from the database
        checkpoint_interval: number of rows to process between checkpoints
    """
    table_name = table_schema.table_name
    has_repl_key = table_schema.replication_key is not None
    mode = "full" if has_repl_key else "full_offset"

    last_marker = (
        table_state.get("last_seen_replication_value")
        if has_repl_key
        else int(table_state.get("last_offset", 0))
    )

    reader = (
        KeysetReader(
            pool,
            table_schema,
            last_marker,
            batch_size,
            use_pk_tiebreak=DEFAULT_USE_PK_TIEBREAK,
            pk_cols=table_schema.primary_keys,
        )
        if has_repl_key
        else OffsetReader(pool, table_schema, last_marker, batch_size)
    )

    rows_synced = int(table_state.get("rows_synced", 0))
    rows_since_checkpoint = 0

    log.info(
        f"{table_name}: starting {mode} load "
        f"(rows_so_far={rows_synced}, last_marker={last_marker})"
    )

    for batch, progress_marker in reader.read_batches():
        for row in batch:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table_name, row)

        batch_len = len(batch)
        rows_synced += batch_len
        rows_since_checkpoint += batch_len
        last_marker = progress_marker

        if rows_since_checkpoint >= checkpoint_interval:
            _save_checkpoint(
                state,
                table_name,
                table_state,
                mode,
                has_repl_key,
                progress_marker,
                rows_synced,
            )
            rows_since_checkpoint = 0
            log.info(f"{table_name}: checkpoint at {rows_synced:,} rows")

    # Final checkpoint — uses last_marker so the tail batch is not re-synced
    _save_checkpoint(
        state,
        table_name,
        table_state,
        mode,
        has_repl_key,
        last_marker,
        rows_synced,
        completed=True,
    )
    log.info(f"{table_name}: full load complete — {rows_synced:,} row(s) synced")


def _sync_incremental(
    table_schema: TableSchema,
    state: dict,
    table_state: dict,
    pool: ConnectionPool,
    batch_size: int,
    checkpoint_interval: int,
) -> None:
    """
    Incremental sync: fetch only rows where replication_key > last committed value.
    Falls back to a full load when last_seen_replication_value is absent from
    state rather than silently returning 0 rows.
    Args:
        table_schema: TableSchema object with metadata for the table being synced
        state: global state dict to update with progress
        table_state: dict with existing state for this table, to be updated and saved
        pool: ConnectionPool to acquire SQL connections from
        batch_size: number of rows to fetch per batch from the database
        checkpoint_interval: number of rows to process between checkpoints
    """
    table_name = table_schema.table_name
    last_replication_value = table_state.get("last_seen_replication_value")

    if last_replication_value is None:
        log.warning(
            f"{table_name}: incremental mode but no cursor in state — " "falling back to full load"
        )
        _sync_full(table_schema, state, table_state, pool, batch_size, checkpoint_interval)
        return

    reader = IncrementalReader(pool, table_schema, last_replication_value, batch_size)
    rows_synced = 0
    rows_since_checkpoint = 0
    current_last = last_replication_value

    log.info(f"{table_name}: starting incremental sync from {last_replication_value}")

    for batch, last_seen in reader.read_upsert_batches():
        for row in batch:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table_name, row)

        batch_len = len(batch)
        rows_synced += batch_len
        rows_since_checkpoint += batch_len
        current_last = last_seen

        if rows_since_checkpoint >= checkpoint_interval:
            _save_checkpoint(
                state,
                table_name,
                table_state,
                "incremental",
                True,
                current_last,
                rows_synced,
            )
            rows_since_checkpoint = 0
            log.info(f"{table_name}: incremental checkpoint at {rows_synced:,} rows")

    _save_checkpoint(
        state,
        table_name,
        table_state,
        "incremental",
        True,
        current_last,
        rows_synced,
        completed=True,
    )
    log.info(f"{table_name}: incremental sync complete — {rows_synced:,} upserted")


def _sync_table_thread(table_schema: TableSchema, state: dict, pool: ConnectionPool) -> None:
    """
    Entry point for each per-table worker thread.
    Each thread exclusively owns state[table_name] — no locking required
    because the GIL serialises dict writes and no two threads touch the same key.
    Exceptions are re-raised so the outer as_completed loop can track failures.
    Args:
        table_schema: TableSchema object with metadata for the table being synced
        state: global state dict to update with progress; each thread updates only its own table's
        pool: ConnectionPool to acquire SQL connections from
    """
    table_name = table_schema.table_name
    try:
        table_state = dict(state.get(table_name, {}))
        # Initialize metadata on first visit — single field encodes both
        # presence and name of the replication key (None when absent)
        if "sync_started_at" not in table_state:
            table_state["sync_started_at"] = datetime.now(timezone.utc).isoformat()
        if "replication_key_col" not in table_state:
            table_state["replication_key_col"] = (
                table_schema.replication_key.name if table_schema.replication_key else None
            )

        mode = _determine_mode(table_state, table_schema)
        log.info(f"{table_name}: mode={mode}")

        if mode in ("full", "full_offset"):
            _sync_full(table_schema, state, table_state, pool, BATCH_SIZE, CHECKPOINT_INTERVAL)
        elif mode == "incremental":
            _sync_incremental(
                table_schema, state, table_state, pool, BATCH_SIZE, CHECKPOINT_INTERVAL
            )
        else:
            log.warning(f"{table_name}: unknown mode '{mode}' — skipping")

    except Exception as exc:
        log.severe(f"{table_name}: sync failed: {exc}")
        log.severe(traceback.format_exc())
        raise


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    global _CACHED_TABLE_SCHEMAS

    validate_configuration(configuration)
    schema_name = configuration.get("mssql_schema", "dbo")
    table_include, table_exclude = _parse_table_list()

    pool = ConnectionPool(configuration=configuration, size=1)
    try:
        detector = SchemaDetector(pool)
        table_schemas = detector.detect_all_tables(
            schema_name, table_include, config=configuration, max_workers=1
        )
    finally:
        pool.close_all()

    # Apply exclusion to any tables discovered when table_include is None
    if table_exclude:
        table_schemas = {
            name: ts for name, ts in table_schemas.items() if name.lower() not in table_exclude
        }

    _CACHED_TABLE_SCHEMAS = table_schemas  # cache for update() reuse

    schema_list = []
    for table_name, table_schema_obj in sorted(table_schemas.items()):
        entry = {"table": table_name}
        primary_keys = table_schema_obj.primary_keys
        if primary_keys:
            entry["primary_key"] = primary_keys
        schema_list.append(entry)

    log.info(f"schema(): returning {len(schema_list)} table(s)")
    return schema_list


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: Connectors - EHI")
    schema_name = configuration.get("mssql_schema", "dbo")
    table_include, table_exclude = _parse_table_list()

    log.info(
        f"EHI Connector starting: schema={schema_name}, "
        f"max_workers={MAX_WORKERS}, "
        f"force_full_resync={DEFAULT_FORCE_FULL_RESYNC}"
    )

    pool_size = MAX_WORKERS + 2
    pool = ConnectionPool(configuration=configuration, size=pool_size)

    try:
        # Reuse schema cache when schema() was called earlier in this process
        if _CACHED_TABLE_SCHEMAS:
            table_schemas = _CACHED_TABLE_SCHEMAS
        else:
            detector = SchemaDetector(pool)
            table_schemas = detector.detect_all_tables(
                schema_name,
                table_include,
                config=configuration,
                max_workers=min(4, pool_size),
            )
            if table_exclude:
                table_schemas = {
                    name: ts
                    for name, ts in table_schemas.items()
                    if name.lower() not in table_exclude
                }

        if not table_schemas:
            log.warning("No tables discovered — nothing to sync")
            return

        discovered_tables = list(table_schemas.keys())
        log.info(f"Discovered {len(discovered_tables)} table(s): {discovered_tables}")

        size_map = _get_table_sizes_batch(pool, schema_name, discovered_tables)
        sorted_tables = sorted(discovered_tables, key=lambda tbl: size_map.get(tbl, 0))
        log.info(
            "Table order (small first): "
            + ", ".join(f"{tbl}({size_map.get(tbl, 0):,})" for tbl in sorted_tables)
        )

        state["_sync_start"] = datetime.now(timezone.utc).isoformat()

        failed_tables = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_table = {
                executor.submit(
                    _sync_table_thread,
                    table_schemas[table_name],
                    state,
                    pool,
                ): table_name
                for table_name in sorted_tables
            }
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    future.result()
                    log.info(f"{table_name}: thread finished successfully")
                except Exception as exc:
                    log.severe(f"{table_name}: thread raised unhandled exception: {exc}")
                    failed_tables.append(table_name)

        if failed_tables:
            log.warning(f"The following tables failed to sync: {failed_tables}")

        op.checkpoint(state)
        log.info("EHI Connector sync complete")

    finally:
        pool.close_all()


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging during development,
# such as using IDE debug tools (breakpoints, step-through debugging, etc.).
# Note: This method is not called by Fivetran when executing your connector in production.
# Always test using 'fivetran debug' prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
