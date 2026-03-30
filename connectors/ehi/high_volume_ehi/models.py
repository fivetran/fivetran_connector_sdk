"""
Data models and schema detection logic for the SQL Server connector.
"""

# data classes for column and table metadata
from dataclasses import dataclass, field

# For parallel schema detection across multiple tables
from concurrent.futures import ThreadPoolExecutor, as_completed

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# Constants for schema detection and replication key inference
from constants import KNOWN_REPLICATION_KEY_PATTERNS

# ConnectionPool for acquiring database connections to query metadata
from client import ConnectionPool


@dataclass
class ColumnInfo:
    """
    Metadata for a single SQL Server column.

    python_type is the Python built-in type used by convert_value() to produce
    JSON-serialisable values from raw pyodbc data:
      int, float, str, bool  — direct scalar conversion
      bytes                  — base64-encoded string (binary/spatial columns)
      None                   — column should be excluded (e.g. unsupported computed)
    """

    name: str
    sql_type: str
    python_type: type  # int | float | str | bool | bytes | None
    is_primary_key: bool
    is_identity: bool
    is_computed: bool
    ordinal_position: int


@dataclass
class TableSchema:
    """
    Full schema description for a single SQL Server table including all
    column metadata and the auto-detected replication key (if any).
    """

    table_name: str
    schema_name: str
    columns: list = field(default_factory=list)
    replication_key: object = None  # ColumnInfo | None

    @property
    def primary_keys(self) -> list:
        """Return list of primary key column names."""
        return [col.name for col in self.columns if col.is_primary_key]

    @property
    def selectable_columns(self) -> list:
        """
        Return columns safe to SELECT: not computed and with a known python_type.
        Computed columns are excluded because SQL Server rejects them in explicit
        column lists under certain contexts.
        """
        return [col for col in self.columns if not col.is_computed and col.python_type is not None]


class SchemaDetector:
    """
    Detects table schemas by querying INFORMATION_SCHEMA and COLUMNPROPERTY
    metadata. Also auto-detects a replication key for each table.
    """

    _METADATA_SQL = """
        SELECT
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.ORDINAL_POSITION,
            COLUMNPROPERTY(
                OBJECT_ID('[' + c.TABLE_SCHEMA + '].[' + c.TABLE_NAME + ']'),
                c.COLUMN_NAME,
                'IsIdentity'
            ) AS is_identity,
            COLUMNPROPERTY(
                OBJECT_ID('[' + c.TABLE_SCHEMA + '].[' + c.TABLE_NAME + ']'),
                c.COLUMN_NAME,
                'IsComputed'
            ) AS is_computed,
            CASE WHEN kcu.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_pk
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            ON  tc.TABLE_SCHEMA = c.TABLE_SCHEMA
            AND tc.TABLE_NAME   = c.TABLE_NAME
            AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON  kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            AND kcu.TABLE_SCHEMA    = c.TABLE_SCHEMA
            AND kcu.TABLE_NAME      = c.TABLE_NAME
            AND kcu.COLUMN_NAME     = c.COLUMN_NAME
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
    """

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def detect_table(self, schema_name: str, table_name: str, config: dict = None) -> TableSchema:
        """
        Fetch column metadata for a single table and build a TableSchema.

        Args:
            schema_name: SQL Server schema (e.g. 'dbo')
            table_name: table name (unqualified)
            config: connector configuration dict; used to honour the
                    incremental_column override in detect_replication_key
        """
        with self._pool.acquire() as conn:
            cur = conn.execute_with_retry(self._METADATA_SQL, (schema_name, table_name))
            try:
                rows = cur.fetchall()
            finally:
                cur.close()

        columns = []
        for row in rows:
            col_name, data_type, ordinal, is_identity, is_computed, is_pk = row
            columns.append(
                ColumnInfo(
                    name=col_name,
                    sql_type=data_type,
                    python_type=SchemaDetector.map_sql_type_to_python(data_type),
                    is_primary_key=bool(is_pk),
                    is_identity=bool(is_identity),
                    is_computed=bool(is_computed),
                    ordinal_position=ordinal,
                )
            )

        if not columns:
            log.warning(f"No columns found for {schema_name}.{table_name}")

        ts = TableSchema(table_name=table_name, schema_name=schema_name, columns=columns)
        ts.replication_key = SchemaDetector.detect_replication_key(columns, config or {})
        return ts

    def detect_all_tables(
        self, schema_name: str, table_names: list = None, config: dict = None, max_workers: int = 4
    ) -> dict:
        """
        Detect schemas for all tables in a schema, optionally filtered by name.
        Uses ThreadPoolExecutor for parallel metadata queries.

        Returns dict mapping table_name -> TableSchema.
        """
        if table_names is None:
            table_names = self._list_tables(schema_name)

        results: dict = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_name = {
                executor.submit(self.detect_table, schema_name, table_name, config): table_name
                for table_name in table_names
            }
            for future in as_completed(future_to_name):
                table_name = future_to_name[future]
                try:
                    results[table_name] = future.result()
                except Exception as exc:
                    log.severe(f"Failed to detect schema for {schema_name}.{table_name}: {exc}")

        log.info(f"Schema detection complete: {len(results)} table(s) discovered")
        return results

    def _list_tables(self, schema_name: str) -> list:
        """Return all BASE TABLE names in the given schema."""
        sql = (
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' "
            "ORDER BY TABLE_NAME"
        )
        with self._pool.acquire() as conn:
            cur = conn.execute_with_retry(sql, (schema_name,))
            try:
                rows = cur.fetchall()
            finally:
                cur.close()
        return [row[0] for row in rows]

    @staticmethod
    def detect_replication_key(columns: list, config: dict):
        """
        Select the best replication key column using priority order:
        1. config["incremental_column"] user override
        2. Column name matches KNOWN_REPLICATION_KEY_PATTERNS (case-insensitive)
        3. First datetime2/datetime/datetimeoffset column
        4. First IDENTITY integer column

        Returns ColumnInfo or None.
        """
        # Priority 1: user override
        override = (config.get("incremental_column") or "").strip()
        if override:
            for col in columns:
                if col.name.lower() == override.lower():
                    log.fine(f"Using user-specified incremental column: {col.name}")
                    return col
            log.warning(
                f"incremental_column '{override}' not found in column list; "
                "falling back to auto-detection"
            )

        # Priority 2: known pattern match (case-insensitive)
        col_map = {col.name.lower(): col for col in columns}
        for pattern in KNOWN_REPLICATION_KEY_PATTERNS:
            if pattern.lower() in col_map:
                matched_col = col_map[pattern.lower()]
                log.fine(f"Replication key matched pattern '{pattern}': {matched_col.name}")
                return matched_col

        # Priority 3: first temporal column
        datetime_types = {"datetime2", "datetime", "datetimeoffset", "smalldatetime", "date"}
        for col in columns:
            if col.sql_type.lower() in datetime_types and col.python_type is not None:
                log.fine(f"Replication key inferred from datetime column: {col.name}")
                return col

        # Priority 4: first IDENTITY integer
        for col in columns:
            if col.is_identity and col.python_type is int:
                log.fine(f"Replication key inferred from IDENTITY column: {col.name}")
                return col

        return None

    @staticmethod
    def map_sql_type_to_python(sql_type: str):
        """
        Map a SQL Server DATA_TYPE string to a Python built-in type used by
        convert_value() in readers.py.

        Returns int, float, str, bool, or bytes.
        Unknown types are mapped to str so columns are not silently dropped.

        Binary and spatial types return bytes — convert_value() base64-encodes
        them so they are stored as ASCII strings in the destination.
        """
        normalized_type = sql_type.lower().strip()

        if normalized_type in {"int", "bigint", "smallint", "tinyint"}:
            return int
        if normalized_type in {"float", "real", "decimal", "numeric", "money", "smallmoney"}:
            return float
        if normalized_type in {
            "varchar",
            "nvarchar",
            "char",
            "nchar",
            "text",
            "ntext",
            "uniqueidentifier",
            "xml",
            "datetime",
            "datetime2",
            "date",
            "time",
            "smalldatetime",
            "datetimeoffset",
        }:
            return str
        if normalized_type == "bit":
            return bool
        # Binary, spatial, and rowversion — base64-encoded as strings at read time
        if normalized_type in {
            "varbinary",
            "binary",
            "image",
            "geography",
            "geometry",
            "hierarchyid",
            "timestamp",
            "rowversion",
        }:
            return bytes
        # Unknown type: treat as string so we don't silently drop columns
        log.fine(f"Unknown SQL type '{sql_type}' mapped to str")
        return str
