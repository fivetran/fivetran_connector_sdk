"""
Readers for full-table and incremental reads using keyset or offset pagination.

KeysetReader    — recommended default; O(n) cost, resumable, supports parallelism
PKKeysetReader  — O(n) full load for tables with a single PK but no replication key
OffsetReader    — last-resort fallback (no PK, no replication key); O(n²) database cost
IncrementalReader — reads only rows changed since the last committed cursor value
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


class KeysetReader:
    """
    Reads a table in order using keyset (seek) pagination.
    O(n) cost regardless of table size — avoids the OFFSET O(n²) problem.
    Each batch acquires its own connection from the pool so multiple tables
    can stream in parallel without connection starvation.
    When use_pk_tiebreak=True and the table has exactly one integer primary key,
    a composite keyset (repl_col, pk_col) prevents data loss when multiple rows
    share the same replication key value across page boundaries.
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
        self._use_pk_tiebreak = use_pk_tiebreak
        self._pk_cols = pk_cols or []
        self._last_seen_pk = last_seen_pk

    def _get_tiebreak_pk_col(self):
        """
        Return the single PK column name to use for tiebreaking, or None if not
        applicable. Eligible types: integer columns and string columns (varchar,
        nvarchar, char, nchar, text, ntext). uniqueidentifier is excluded because
        its SQL Server internal sort order does not match lexicographic order.
        """
        if not self._use_pk_tiebreak:
            return None
        if len(self._pk_cols) != 1:
            if self._pk_cols:
                log.warning(
                    f"{self._schema.table_name}: tiebreak requested but table has a "
                    f"{len(self._pk_cols)}-column composite PK — tiebreak disabled"
                )
            return None
        pk_name = self._pk_cols[0]
        for col in self._schema.columns:
            if col.name != pk_name:
                continue
            if col.python_type is int:
                return pk_name
            if col.python_type is str and col.sql_type.lower() in _TIEBREAK_ELIGIBLE_STR_TYPES:
                return pk_name
            log.warning(
                f"{self._schema.table_name}: tiebreak requested but PK column '{pk_name}' "
                f"has SQL type '{col.sql_type}' which is not eligible for tiebreaking "
                f"(uniqueidentifier and other non-orderable types are excluded) — tiebreak disabled"
            )
            return None
        return None

    def read_batches(self):
        """
        Generator yielding (batch, last_repl_value, last_pk_value) 3-tuples.

        SQL templates are built once before the loop to avoid repeated
        f-string formatting across thousands of pages.
        """
        repl_col = self._schema.replication_key.name
        sel_cols = self._schema.selectable_columns
        col_sql = ", ".join(f"[{col.name}]" for col in sel_cols)
        schema_table = f"[{self._schema.schema_name}].[{self._schema.table_name}]"
        tiebreak_pk = self._get_tiebreak_pk_col()
        fetch = f"OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY OPTION (FAST {self._batch_size})"

        # Pre-build first-page and subsequent-page SQL templates
        if tiebreak_pk:
            sql_first = (
                f"SELECT {col_sql} FROM {schema_table} WITH (NOLOCK) "
                f"WHERE [{repl_col}] IS NOT NULL "
                f"ORDER BY [{repl_col}], [{tiebreak_pk}] {fetch}"
            )
            sql_next = (
                f"SELECT {col_sql} FROM {schema_table} WITH (NOLOCK) "
                f"WHERE ([{repl_col}] > ?) OR ([{repl_col}] = ? AND [{tiebreak_pk}] > ?) "
                f"ORDER BY [{repl_col}], [{tiebreak_pk}] {fetch}"
            )
        else:
            sql_first = (
                f"SELECT {col_sql} FROM {schema_table} WITH (NOLOCK) "
                f"WHERE [{repl_col}] IS NOT NULL "
                f"ORDER BY [{repl_col}] {fetch}"
            )
            sql_next = (
                f"SELECT {col_sql} FROM {schema_table} WITH (NOLOCK) "
                f"WHERE [{repl_col}] > ? "
                f"ORDER BY [{repl_col}] {fetch}"
            )

        # Precompute repl_col index to avoid re-scanning sel_cols for the boundary warning.
        # Fail fast if the replication key column is absent from selectable_columns (e.g. it is
        # a computed column excluded from SELECT). Defaulting to index 0 would silently corrupt
        # cursor tracking by advancing state from the wrong column.
        repl_col_idx = next((i for i, col in enumerate(sel_cols) if col.name == repl_col), None)
        if repl_col_idx is None:
            raise ValueError(
                f"{self._schema.table_name}: replication key column '{repl_col}' is not present "
                "in selectable_columns (it may be a computed column excluded from SELECT). "
                "Set incremental_column in constants.py to a non-computed column, "
                "or leave it unset to allow auto-detection to pick a selectable column."
            )

        page = 0
        current_last = self._last_seen
        current_last_pk = self._last_seen_pk

        while True:
            if current_last is None:
                sql, params = sql_first, (self._batch_size,)
            elif tiebreak_pk:
                sql = sql_next
                params = (current_last, current_last, current_last_pk, self._batch_size)
            else:
                sql = sql_next
                params = (current_last, self._batch_size)

            with self._pool.acquire() as conn:
                cur = conn.execute_with_retry(sql, params)
                try:
                    rows = cur.fetchall()
                finally:
                    cur.close()

            if not rows:
                log.fine(f"{self._schema.table_name}: page {page} returned 0 rows — done")
                return

            # Warn on duplicate replication key values at page boundary
            if not tiebreak_pk and page > 0 and current_last is not None:
                first_rk = convert_value(rows[0][repl_col_idx], sel_cols[repl_col_idx].python_type)
                if first_rk == current_last:
                    log.warning(
                        f"{self._schema.table_name}: duplicate replication key values "
                        f"detected at page boundary (value={current_last}). "
                        "Rows may be skipped. Set use_pk_tiebreak=true to prevent data loss."
                    )

            last_value = current_last
            last_pk_value = current_last_pk
            batch = []
            for row in rows:
                record = {
                    col.name: convert_value(raw, col.python_type)
                    for col, raw in zip(sel_cols, row)
                }
                batch.append(record)
                # Advance cursor from the pre-computed row index rather than the record dict.
                # WHERE [repl_col] IS NOT NULL / [repl_col] > ? guarantees non-NULL here,
                # so convert_value will return a real value; str(raw) is a safety fallback.
                rk_converted = convert_value(row[repl_col_idx], sel_cols[repl_col_idx].python_type)
                last_value = rk_converted if rk_converted is not None else str(row[repl_col_idx])
                if tiebreak_pk:
                    pk_raw = record.get(tiebreak_pk)
                    if pk_raw is not None:
                        last_pk_value = pk_raw

            current_last = last_value
            current_last_pk = last_pk_value
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


class IncrementalReader:
    """
    Reads only rows changed since the last committed replication-key value.

    When use_pk_tiebreak=True and the table has exactly one integer primary key,
    a composite keyset (repl_col, pk_col) is used for within-run pagination so
    that rows sharing the same replication-key value are never skipped at page
    boundaries. Using >= alone would re-deliver the same page infinitely;
    the composite cursor (repl > X) OR (repl = X AND pk > Y) is the only
    correct solution.

    Yields (batch, last_repl_value, last_pk_value) 3-tuples. Both cursor values
    are persisted to state so that a resume after a mid-run checkpoint reconstructs
    the full composite cursor and avoids boundary data loss.
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
        self._use_pk_tiebreak = use_pk_tiebreak
        self._pk_cols = pk_cols or []
        self._last_seen_pk = last_seen_pk

    def _get_tiebreak_pk_col(self):
        """
        Return the single PK column name to use for tiebreaking, or None if not
        applicable. Eligible types: integer columns and string columns (varchar,
        nvarchar, char, nchar, text, ntext). uniqueidentifier is excluded because
        its SQL Server internal sort order does not match lexicographic order.
        """
        if not self._use_pk_tiebreak:
            return None
        if len(self._pk_cols) != 1:
            if self._pk_cols:
                log.warning(
                    f"{self._schema.table_name}: tiebreak requested but table has a "
                    f"{len(self._pk_cols)}-column composite PK — tiebreak disabled"
                )
            return None
        pk_name = self._pk_cols[0]
        for col in self._schema.columns:
            if col.name != pk_name:
                continue
            if col.python_type is int:
                return pk_name
            if col.python_type is str and col.sql_type.lower() in _TIEBREAK_ELIGIBLE_STR_TYPES:
                return pk_name
            log.warning(
                f"{self._schema.table_name}: tiebreak requested but PK column '{pk_name}' "
                f"has SQL type '{col.sql_type}' which is not eligible for tiebreaking "
                f"(uniqueidentifier and other non-orderable types are excluded) — tiebreak disabled"
            )
            return None
        return None

    def read_upsert_batches(self):
        """Generator yielding (batch, last_repl_value, last_pk_value) 3-tuples for changed rows."""
        repl_col = self._schema.replication_key.name
        sel_cols = self._schema.selectable_columns
        col_sql = ", ".join(f"[{col.name}]" for col in sel_cols)
        schema_table = f"[{self._schema.schema_name}].[{self._schema.table_name}]"
        tiebreak_pk = self._get_tiebreak_pk_col()
        fetch = f"OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY OPTION (FAST {self._batch_size})"

        # Pre-compute column indices for direct row access during cursor tracking.
        # Fail fast if the replication key column is absent from selectable_columns (e.g. it is
        # a computed column excluded from SELECT). Defaulting to index 0 would silently corrupt
        # cursor tracking by advancing state from the wrong column.
        repl_col_idx = next((i for i, col in enumerate(sel_cols) if col.name == repl_col), None)
        if repl_col_idx is None:
            raise ValueError(
                f"{self._schema.table_name}: replication key column '{repl_col}' is not present "
                "in selectable_columns (it may be a computed column excluded from SELECT). "
                "Set incremental_column in constants.py to a non-computed column, "
                "or leave it unset to allow auto-detection to pick a selectable column."
            )
        pk_col_idx = (
            next((i for i, col in enumerate(sel_cols) if col.name == tiebreak_pk), None)
            if tiebreak_pk
            else None
        )

        # Two SQL templates are needed when tiebreaking:
        #   sql_first — first page of each run: only the replication-key cursor applies.
        #   sql_next  — subsequent pages: composite (repl, pk) cursor prevents skipping rows
        #               that share the same replication-key value at a page boundary.
        # WHERE [repl_col] > ? implicitly excludes NULL replication-key rows —
        # SQL NULL comparisons always evaluate to UNKNOWN/FALSE, so no explicit IS NOT NULL needed.
        if tiebreak_pk:
            sql_first = (
                f"SELECT {col_sql} FROM {schema_table} WITH (NOLOCK) "
                f"WHERE [{repl_col}] > ? "
                f"ORDER BY [{repl_col}], [{tiebreak_pk}] {fetch}"
            )
            sql_next = (
                f"SELECT {col_sql} FROM {schema_table} WITH (NOLOCK) "
                f"WHERE ([{repl_col}] > ?) OR ([{repl_col}] = ? AND [{tiebreak_pk}] > ?) "
                f"ORDER BY [{repl_col}], [{tiebreak_pk}] {fetch}"
            )
        else:
            sql_first = (
                f"SELECT {col_sql} FROM {schema_table} WITH (NOLOCK) "
                f"WHERE [{repl_col}] > ? "
                f"ORDER BY [{repl_col}] {fetch}"
            )
            sql_next = sql_first  # no tiebreak — single cursor, same template every page

        current_last = self._last_seen
        current_last_pk = self._last_seen_pk

        while True:
            if not tiebreak_pk or current_last_pk is None:
                params = (current_last, self._batch_size)
                sql = sql_first
            else:
                params = (current_last, current_last, current_last_pk, self._batch_size)
                sql = sql_next

            with self._pool.acquire() as conn:
                cur = conn.execute_with_retry(sql, params)
                try:
                    rows = cur.fetchall()
                finally:
                    cur.close()

            if not rows:
                return
            last_value = current_last
            last_pk_value = current_last_pk
            batch = []
            for row in rows:
                record = {
                    col.name: convert_value(raw, col.python_type)
                    for col, raw in zip(sel_cols, row)
                }
                batch.append(record)
                # Advance replication-key cursor from the raw row (guaranteed non-NULL by WHERE).
                # str(raw) fallback ensures the cursor always moves forward.
                rk_converted = convert_value(row[repl_col_idx], sel_cols[repl_col_idx].python_type)
                last_value = rk_converted if rk_converted is not None else str(row[repl_col_idx])
                # Advance PK cursor when tiebreaking is active.
                if tiebreak_pk and pk_col_idx is not None:
                    pk_converted = convert_value(row[pk_col_idx], sel_cols[pk_col_idx].python_type)
                    if pk_converted is not None:
                        last_pk_value = pk_converted

            current_last = last_value
            current_last_pk = last_pk_value
            # Yield the replication-key value and the tiebreak PK cursor together so the
            # caller can persist both to state. This allows resume to reconstruct the full
            # composite (repl, pk) cursor and avoid boundary data loss.
            yield batch, current_last, current_last_pk


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
