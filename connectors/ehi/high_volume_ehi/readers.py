"""
Readers for full-table and incremental reads using keyset or offset pagination.

KeysetReader    — keyset (seek) pagination ordered by (repl_col[, pk_tiebreak]).
                  Pass last_seen_value=None for a fresh full load (scans from the
                  beginning). Pass a cursor value for an incremental sync or a
                  resumed full load (starts strictly after the cursor). O(n).
PKKeysetReader  — O(n) full load for tables with a single PK but no replication key.
OffsetReader    — last-resort fallback (no PK, no replication key); O(n²) database cost.
"""

# base64 is used for encoding binary data
import base64

# datetime types are converted to ISO8601 strings for JSON serialization
from datetime import datetime, date, time as _time

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# Constants for batch size and other tuning parameters
from constants import BATCH_SIZE

# ConnectionPool manages a pool of MSSQLConnection instances
from client import ConnectionPool

# TableSchema defines the structure of the table
from models import TableSchema

# Pre-built tuple used by isinstance() check in convert_value().
# Defined at module level so it is not recreated on every call.
_DATETIME_TYPES = (datetime, date, _time)

# SQL Server string types eligible as tiebreak PK columns.
# uniqueidentifier is excluded: its SQL Server internal sort order does not match
# lexicographic order, so > comparisons would produce an unreliable keyset cursor.
_TIEBREAK_ELIGIBLE_STR_TYPES = frozenset({"varchar", "nvarchar", "char", "nchar", "text", "ntext"})


def convert_value(value, python_type):
    """
    Convert a raw pyodbc value to a JSON-serialisable Python scalar.
    Ordering: str is checked first because it is the most common SQL Server
    type (varchar, nvarchar, datetime, etc.), reducing average branch evaluations.
    Args:
        value:       raw value from a pyodbc cursor row
        python_type: target type (int, float, str, bool, bytes) or None to skip
    Returns:
        Converted scalar, or None when value is NULL or type is None
    """
    if value is None:
        return None
    if python_type is str:
        if isinstance(value, _DATETIME_TYPES):
            return value.isoformat()
        return str(value)
    if python_type is int:
        return int(value)
    if python_type is float:
        return float(value)
    if python_type is bool:
        return bool(value)
    if python_type is bytes:
        # Binary, spatial, rowversion — encode as base64 ASCII string
        if isinstance(value, (bytes, bytearray, memoryview)):
            return base64.b64encode(bytes(value)).decode("ascii")
        # Unexpected runtime type — preserve as string rather than silently dropping.
        # Returning None here would cause the replication-key cursor to stall and
        # loop forever if this column is used as the replication key.
        return str(value)
    return None  # python_type is None — column explicitly excluded; skip


def _get_tiebreak_pk_col(schema: TableSchema, pk_cols: list, use_pk_tiebreak: bool):
    """
    Return the single PK column name to use for tiebreaking, or None if not applicable.

    Eligible PK types: integer columns and orderable string columns (varchar, nvarchar,
    char, nchar, text, ntext). uniqueidentifier is excluded because its SQL Server
    internal sort order does not match lexicographic order, so > comparisons would
    produce an unreliable keyset cursor.
    """
    if not use_pk_tiebreak:
        return None
    if len(pk_cols) != 1:
        if pk_cols:
            log.warning(
                f"{schema.table_name}: tiebreak requested but table has a "
                f"{len(pk_cols)}-column composite PK — tiebreak disabled"
            )
        return None
    pk_name = pk_cols[0]
    for col in schema.columns:
        if col.name != pk_name:
            continue
        if col.python_type is int:
            return pk_name
        if col.python_type is str and col.sql_type.lower() in _TIEBREAK_ELIGIBLE_STR_TYPES:
            return pk_name
        log.warning(
            f"{schema.table_name}: tiebreak requested but PK column '{pk_name}' "
            f"has SQL type '{col.sql_type}' which is not eligible for tiebreaking "
            f"(uniqueidentifier and other non-orderable types are excluded) — tiebreak disabled"
        )
        return None
    return None


class KeysetReader:
    """
    Reads a table using keyset (seek) pagination ordered by (repl_col[, pk_tiebreak]).
    O(n) cost regardless of table size — avoids the OFFSET O(n²) problem.

    Pass last_seen_value=None for a fresh full load (starts from the beginning of the
    table). Pass a cursor value for an incremental sync or a resumed full load (starts
    from rows strictly after the cursor). Both cases use the same SQL logic:
      - first page, no cursor:   WHERE [repl_col] IS NOT NULL
      - first page, with cursor: WHERE [repl_col] > ?
      - subsequent pages:        WHERE ([repl_col] > ?) OR ([repl_col] = ? AND [pk] > ?)

    When use_pk_tiebreak=True and the table has exactly one eligible primary key column,
    a composite (repl_col, pk_col) cursor prevents data loss when multiple rows share
    the same replication-key value across page boundaries.

    Yields (batch, last_repl_value, last_pk_value) 3-tuples.
    """

    def __init__(
        self,
        pool: ConnectionPool,
        schema: TableSchema,
        last_seen_value,
        batch_size: int = BATCH_SIZE,
        use_pk_tiebreak: bool = False,
        pk_cols: list = None,
        last_seen_pk=None,
    ) -> None:
        self._pool = pool
        self._schema = schema
        self._last_seen = last_seen_value
        self._batch_size = batch_size
        self._pk_cols = pk_cols or []
        self._last_seen_pk = last_seen_pk
        # Resolved once at construction — avoids repeating the eligibility check on every read.
        self._tiebreak_pk = _get_tiebreak_pk_col(schema, self._pk_cols, use_pk_tiebreak)

    def read_batches(self):
        """
        Generator yielding (batch, last_repl_value, last_pk_value) 3-tuples.

        SQL templates are built once before the loop. Column indices are precomputed
        so cursor tracking reads directly from the raw row array rather than the
        converted dict, avoiding per-row key lookups.
        """
        repl_col = self._schema.replication_key.name
        sel_cols = self._schema.selectable_columns
        col_sql = ", ".join(f"[{c.name}]" for c in sel_cols)
        tbl = f"[{self._schema.schema_name}].[{self._schema.table_name}]"
        tb_pk = self._tiebreak_pk
        fetch = f"OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY OPTION (FAST {self._batch_size})"

        # Fail fast if the replication key column is absent from selectable_columns (e.g. it is
        # a computed column excluded from SELECT). Defaulting to index 0 would silently corrupt
        # cursor tracking by advancing state from the wrong column.
        repl_idx = next((i for i, c in enumerate(sel_cols) if c.name == repl_col), None)
        if repl_idx is None:
            raise ValueError(
                f"{self._schema.table_name}: replication key column '{repl_col}' is not present "
                "in selectable_columns (it may be a computed column excluded from SELECT). "
                "Set incremental_column in constants.py to a non-computed column, "
                "or leave it unset to allow auto-detection to pick a selectable column."
            )
        pk_idx = (
            next((i for i, c in enumerate(sel_cols) if c.name == tb_pk), None) if tb_pk else None
        )

        # Pre-build SQL templates. Three variants are needed when tiebreaking:
        #   sql_start     — first page of a fresh full load: scans from the beginning.
        #   sql_from      — first page with a cursor (incremental or resumed full load).
        #   sql_composite — subsequent pages: composite (repl, pk) cursor prevents skipping
        #                   rows that share the same replication-key value at a page boundary.
        # WHERE [repl_col] > ? implicitly excludes NULL replication-key rows —
        # SQL NULL comparisons always evaluate to UNKNOWN/FALSE, so no explicit IS NOT NULL needed.
        if tb_pk:
            sql_start = (
                f"SELECT {col_sql} FROM {tbl} WITH (NOLOCK) "
                f"WHERE [{repl_col}] IS NOT NULL "
                f"ORDER BY [{repl_col}], [{tb_pk}] {fetch}"
            )
            sql_from = (
                f"SELECT {col_sql} FROM {tbl} WITH (NOLOCK) "
                f"WHERE [{repl_col}] > ? "
                f"ORDER BY [{repl_col}], [{tb_pk}] {fetch}"
            )
            sql_composite = (
                f"SELECT {col_sql} FROM {tbl} WITH (NOLOCK) "
                f"WHERE ([{repl_col}] > ?) OR ([{repl_col}] = ? AND [{tb_pk}] > ?) "
                f"ORDER BY [{repl_col}], [{tb_pk}] {fetch}"
            )
        else:
            sql_start = (
                f"SELECT {col_sql} FROM {tbl} WITH (NOLOCK) "
                f"WHERE [{repl_col}] IS NOT NULL "
                f"ORDER BY [{repl_col}] {fetch}"
            )
            sql_from = (
                f"SELECT {col_sql} FROM {tbl} WITH (NOLOCK) "
                f"WHERE [{repl_col}] > ? "
                f"ORDER BY [{repl_col}] {fetch}"
            )
            sql_composite = sql_from  # no tiebreak — single-cursor template every page

        current_last = self._last_seen
        current_last_pk = self._last_seen_pk
        page = 0

        while True:
            if current_last is None:
                sql, params = sql_start, (self._batch_size,)
            elif not tb_pk or current_last_pk is None:
                sql, params = sql_from, (current_last, self._batch_size)
            else:
                sql = sql_composite
                params = (current_last, current_last, current_last_pk, self._batch_size)

            with self._pool.acquire() as conn:
                cur = conn.execute_with_retry(sql, params)
                try:
                    rows = cur.fetchall()
                finally:
                    cur.close()

            if not rows:
                log.fine(f"{self._schema.table_name}: page {page} returned 0 rows — done")
                return

            # On the very first page of a fresh full load, warn if any NULL replication key
            # rows exist in the table — they are permanently excluded by the IS NOT NULL
            # filter and will never be synced.
            if page == 0 and current_last is None:
                null_count_sql = (
                    f"SELECT COUNT(*) FROM {tbl} WITH (NOLOCK) WHERE [{repl_col}] IS NULL"
                )
                with self._pool.acquire() as _conn:
                    _cur = _conn.execute_with_retry(null_count_sql, ())
                    try:
                        null_count = _cur.fetchone()[0] or 0
                    finally:
                        _cur.close()
                if null_count:
                    log.warning(
                        f"{self._schema.table_name}: {null_count} row(s) have a NULL "
                        f"replication key ('{repl_col}') and will never be synced. "
                        "Populate the column or use a different incremental strategy."
                    )

            # Warn on duplicate replication key values at page boundary (no-tiebreak path only).
            if not tb_pk and page > 0 and current_last is not None:
                first_rk = convert_value(rows[0][repl_idx], sel_cols[repl_idx].python_type)
                if first_rk == current_last:
                    log.warning(
                        f"{self._schema.table_name}: duplicate replication key values "
                        f"detected at page boundary (value={current_last}). "
                        "Rows may be skipped. Set use_pk_tiebreak=True to prevent data loss."
                    )

            last_rk = current_last
            last_pk = current_last_pk
            batch = []
            for row in rows:
                record = {
                    c.name: convert_value(raw, c.python_type) for c, raw in zip(sel_cols, row)
                }
                batch.append(record)
                # Advance replication-key cursor from the raw row (guaranteed non-NULL by WHERE).
                # str(raw) fallback ensures the cursor always moves forward.
                rk = convert_value(row[repl_idx], sel_cols[repl_idx].python_type)
                last_rk = rk if rk is not None else str(row[repl_idx])
                # Advance PK cursor when tiebreaking is active.
                if tb_pk and pk_idx is not None:
                    pk = convert_value(row[pk_idx], sel_cols[pk_idx].python_type)
                    if pk is not None:
                        last_pk = pk

            current_last = last_rk
            current_last_pk = last_pk
            page += 1
            log.fine(
                f"{self._schema.table_name}: page {page} — "
                f"{len(batch)} rows, last_value={current_last}"
            )
            yield batch, current_last, current_last_pk


class OffsetReader:
    """
    Reads a table using SQL Server OFFSET/FETCH pagination.

    Used as a fallback when no replication key can be detected.
    Guarantees a full scan but is O(n²) in database cost — use only for
    small tables or when no incremental option is available.
    """

    def __init__(
        self,
        pool: ConnectionPool,
        schema: TableSchema,
        last_offset: int,
        batch_size: int = BATCH_SIZE,
    ) -> None:
        self._pool = pool
        self._schema = schema
        self._last_offset = last_offset
        self._batch_size = batch_size
        pk_cols = schema.primary_keys
        if pk_cols:
            self._order_clause = "ORDER BY " + ", ".join(f"[{pk}]" for pk in pk_cols)
            self._has_pk_order = True
        else:
            self._order_clause = "ORDER BY (SELECT NULL)"
            self._has_pk_order = False

    def read_batches(self):
        """Generator yielding (batch: list[dict], next_offset) pairs."""
        sel_cols = self._schema.selectable_columns
        col_sql = ", ".join(f"[{col.name}]" for col in sel_cols)
        schema_table = f"[{self._schema.schema_name}].[{self._schema.table_name}]"

        if self._has_pk_order:
            log.warning(
                f"Table {self._schema.table_name} has no replication key — "
                "using OFFSET pagination with stable PK ordering (O(n²) cost). "
                "Consider adding an incremental_column to the configuration."
            )
        else:
            log.warning(
                f"Table {self._schema.table_name} has no replication key and no primary key — "
                "using OFFSET pagination with non-deterministic ordering. "
                "Row order is not guaranteed stable across pages; rows may be skipped or "
                "duplicated if the table is modified during sync. "
                "Consider adding a primary key or an incremental_column to the configuration."
            )

        # Pre-build SQL template
        sql = (
            f"SELECT {col_sql} FROM {schema_table} WITH (NOLOCK) "
            f"{self._order_clause} "
            f"OFFSET ? ROWS FETCH NEXT ? ROWS ONLY "
            f"OPTION (FAST {self._batch_size})"
        )

        offset = self._last_offset
        while True:
            with self._pool.acquire() as conn:
                cur = conn.execute_with_retry(sql, (offset, self._batch_size))
                try:
                    rows = cur.fetchall()
                finally:
                    cur.close()

            if not rows:
                return

            batch = [
                {col.name: convert_value(raw, col.python_type) for col, raw in zip(sel_cols, row)}
                for row in rows
            ]
            offset += len(batch)
            log.fine(f"{self._schema.table_name}: offset page, next_offset={offset}")
            yield batch, offset


class PKKeysetReader:
    """
    Reads a table in primary-key order using keyset (seek) pagination.

    Used as the preferred full-load strategy for tables that have a single-column
    primary key but no replication key. Advantages over OffsetReader:
      - O(n) database cost — no OFFSET penalty regardless of table size
      - Stable, resumable cursor: an interrupted sync restarts from the last
        committed PK value rather than a row-count offset
      - Deterministic ordering even under concurrent writes

    For tables with a composite primary key or no primary key at all, OffsetReader
    is used as a fallback (O(n²), non-resumable from exact position).
    """

    def __init__(
        self,
        pool: ConnectionPool,
        schema: TableSchema,
        last_seen_pk=None,
        batch_size: int = BATCH_SIZE,
    ) -> None:
        self._pool = pool
        self._schema = schema
        self._last_seen_pk = last_seen_pk
        self._batch_size = batch_size
        self._pk_col = schema.primary_keys[0]  # caller guarantees exactly one PK

    def read_batches(self):
        """Generator yielding (batch: list[dict], last_pk_value) pairs."""
        pk_col = self._pk_col
        sel_cols = self._schema.selectable_columns
        col_sql = ", ".join(f"[{col.name}]" for col in sel_cols)
        schema_table = f"[{self._schema.schema_name}].[{self._schema.table_name}]"
        fetch = f"OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY OPTION (FAST {self._batch_size})"

        sql_first = (
            f"SELECT {col_sql} FROM {schema_table} WITH (NOLOCK) " f"ORDER BY [{pk_col}] {fetch}"
        )
        sql_next = (
            f"SELECT {col_sql} FROM {schema_table} WITH (NOLOCK) "
            f"WHERE [{pk_col}] > ? "
            f"ORDER BY [{pk_col}] {fetch}"
        )

        pk_col_idx = next((i for i, col in enumerate(sel_cols) if col.name == pk_col), None)
        if pk_col_idx is None:
            raise ValueError(
                f"{self._schema.table_name}: PK column '{pk_col}' is not present in "
                "selectable_columns (it may be a computed column). "
                "This table cannot use PK-keyset pagination."
            )
        pk_python_type = sel_cols[pk_col_idx].python_type

        current_last = self._last_seen_pk

        while True:
            if current_last is None:
                sql, params = sql_first, (self._batch_size,)
            else:
                sql, params = sql_next, (current_last, self._batch_size)

            with self._pool.acquire() as conn:
                cur = conn.execute_with_retry(sql, params)
                try:
                    rows = cur.fetchall()
                finally:
                    cur.close()

            if not rows:
                log.fine(f"{self._schema.table_name}: PK-keyset page returned 0 rows — done")
                return

            last_pk_value = current_last
            batch = []
            for row in rows:
                record = {
                    col.name: convert_value(raw, col.python_type)
                    for col, raw in zip(sel_cols, row)
                }
                batch.append(record)
                pk_converted = convert_value(row[pk_col_idx], pk_python_type)
                if pk_converted is not None:
                    last_pk_value = pk_converted

            current_last = last_pk_value
            log.fine(
                f"{self._schema.table_name}: PK-keyset page — "
                f"{len(batch)} rows, last_pk={current_last}"
            )
            yield batch, current_last
