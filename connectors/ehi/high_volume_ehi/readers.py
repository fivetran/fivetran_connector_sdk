"""
Readers for full-table and incremental reads using keyset or offset pagination.

KeysetReader  — recommended default; O(n) cost, resumable, supports parallelism
OffsetReader  — fallback for tables with no replication key; O(n²) database cost
IncrementalReader — reads only rows changed since the last committed cursor value
"""

import base64
from datetime import datetime, date, time as _time

from fivetran_connector_sdk import Logging as log

from constants import BATCH_SIZE
from client import ConnectionPool
from models import TableSchema


# Pre-built tuple used by isinstance() check in convert_value().
# Defined at module level so it is not recreated on every call.
_DATETIME_TYPES = (datetime, date, _time)


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
        return None
    return None  # unknown / None python_type — skip column


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
    ) -> None:
        self._pool = pool
        self._schema = schema
        self._last_seen = last_seen_value
        self._batch_size = batch_size
        self._use_pk_tiebreak = use_pk_tiebreak
        self._pk_cols = pk_cols or []

    def _get_tiebreak_pk_col(self):
        """
        Return the single integer PK column name to use for tiebreaking, or
        None if not applicable (not enabled, or multiple/non-integer PKs).
        """
        if not self._use_pk_tiebreak or len(self._pk_cols) != 1:
            return None
        pk_name = self._pk_cols[0]
        for col in self._schema.columns:
            if col.name == pk_name and col.python_type is int:
                return pk_name
        return None

    def read_batches(self):
        """
        Generator yielding (batch: list[dict], last_value) pairs.

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

        # Precompute repl_col index to avoid re-scanning sel_cols for the boundary warning
        repl_col_idx = next((i for i, col in enumerate(sel_cols) if col.name == repl_col), 0)

        page = 0
        current_last = self._last_seen
        current_last_pk = None

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
                rows = cur.fetchall()
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
                rk_raw = record.get(repl_col)
                if rk_raw is not None:
                    last_value = rk_raw
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
            yield batch, current_last


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

    def read_batches(self):
        """Generator yielding (batch: list[dict], next_offset) pairs."""
        sel_cols = self._schema.selectable_columns
        col_sql = ", ".join(f"[{col.name}]" for col in sel_cols)
        schema_table = f"[{self._schema.schema_name}].[{self._schema.table_name}]"

        log.warning(
            f"Table {self._schema.table_name} has no replication key — "
            "using OFFSET pagination (slow for large tables). "
            "Consider adding an incremental_column to the configuration."
        )

        # Pre-build SQL template
        sql = (
            f"SELECT {col_sql} FROM {schema_table} WITH (NOLOCK) "
            f"ORDER BY (SELECT NULL) "
            f"OFFSET ? ROWS FETCH NEXT ? ROWS ONLY "
            f"OPTION (FAST {self._batch_size})"
        )

        offset = self._last_offset
        while True:
            with self._pool.acquire() as conn:
                cur = conn.execute_with_retry(sql, (offset, self._batch_size))
                rows = cur.fetchall()
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
    """

    def __init__(
        self,
        pool: ConnectionPool,
        schema: TableSchema,
        last_seen_value,
        batch_size: int = BATCH_SIZE,
    ) -> None:
        self._pool = pool
        self._schema = schema
        self._last_seen = last_seen_value
        self._batch_size = batch_size

    def read_upsert_batches(self):
        """Generator yielding (batch: list[dict], last_value) pairs for changed rows."""
        repl_col = self._schema.replication_key.name
        sel_cols = self._schema.selectable_columns
        col_sql = ", ".join(f"[{col.name}]" for col in sel_cols)
        schema_table = f"[{self._schema.schema_name}].[{self._schema.table_name}]"

        # Pre-build SQL template
        sql = (
            f"SELECT {col_sql} FROM {schema_table} WITH (NOLOCK) "
            f"WHERE [{repl_col}] > ? "
            f"ORDER BY [{repl_col}] "
            f"OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY "
            f"OPTION (FAST {self._batch_size})"
        )

        current_last = self._last_seen
        while True:
            with self._pool.acquire() as conn:
                cur = conn.execute_with_retry(sql, (current_last, self._batch_size))
                rows = cur.fetchall()
                cur.close()

            if not rows:
                return

            last_value = current_last
            batch = []
            for row in rows:
                record = {
                    col.name: convert_value(raw, col.python_type)
                    for col, raw in zip(sel_cols, row)
                }
                batch.append(record)
                rk_raw = record.get(repl_col)
                if rk_raw is not None:
                    last_value = rk_raw

            current_last = last_value
            yield batch, current_last
