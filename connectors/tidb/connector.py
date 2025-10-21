# connector.py
"""
TiDB connector for Fivetran.

This file is intended as an example connector demonstrating:
- schema declaration via `schema(configuration)`
- incremental replication using `update(configuration, state)`
- defensive parsing and type normalization (timestamps, embeddings)
- basic error handling and checkpointing
"""

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For type hints
from typing import Dict, List, Any, Optional

# For reading configuration from a JSON file
import json

# CA bundle used for TLS connections (used by TiDB DSN)
import certifi

# TiDB client used to connect/query the TiDB cluster
from pytidb import TiDBClient

# for timestamp parsing/normalization
from datetime import datetime, timezone


# -----------------------------
# Config Validation helper
# -----------------------------

def validate_configuration(configuration: Dict[str, Any]):
    """
    Validate that the required keys are present in configuration.
    Raises ValueError if any required key is missing.
    """
    required = ["TIDB_HOST", "TIDB_USER", "TIDB_PASS", "TIDB_PORT", "TIDB_DATABASE", "TABLES_PRIMARY_KEY_COLUMNS"]
    missing = [k for k in required if not configuration.get(k)]
    if missing:
        raise ValueError(f"Missing required configuration keys: {', '.join(missing)}")


# -----------------------------
# Schema helper
# -----------------------------
def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Declare the destination schema expected by Fivetran.
    - Expects configuration["TABLES_PRIMARY_KEY_COLUMNS"] as a JSON string mapping table->primary_key_column.
    - Optionally supports `VECTOR_TABLES_DATA` mapping vector table -> {primary_key_column, vector_column}
    Returns a list of dicts, each with "table" and "primary_key" (and optional typed "columns").
    """
    if "TABLES_PRIMARY_KEY_COLUMNS" not in configuration:
        raise ValueError("Could not find 'TABLES_PRIMARY_KEY_COLUMNS' in configuration")

    try:
        tables_and_primary_key_columns = json.loads(configuration["TABLES_PRIMARY_KEY_COLUMNS"])
    except Exception as e:
        raise ValueError("Failed to parse TABLES_PRIMARY_KEY_COLUMNS JSON") from e

    schema_list = []
    for table_name, primary_key_column in tables_and_primary_key_columns.items():
        schema_list.append({"table": table_name, "primary_key": [primary_key_column]})

    # Optional: vector table metadata with a JSON typed vector column
    if configuration.get("VECTOR_TABLES_DATA"):
        try:
            vector_tables_data = json.loads(configuration["VECTOR_TABLES_DATA"])
        except Exception:
            log.info("Failed to parse VECTOR_TABLES_DATA; ignoring vector table configuration.")
            vector_tables_data = {}

        for table_name, table_data in vector_tables_data.items():
            # defensive guards - skip malformed entries
            pk = table_data.get("primary_key_column")
            vector_col = table_data.get("vector_column")
            if not pk or not vector_col:
                log.info("Skipping vector table '%s' due to missing keys", table_name)
                continue
            schema_list.append({
                "table": table_name,
                "primary_key": [pk],
                "columns": {vector_col: "JSON"}
            })

    return schema_list


# -----------------------------
# Utility: parse embedding string
# -----------------------------
def parse_embedding_string_to_list(s: Optional[str]) -> Optional[List[float]]:
    """
    Attempts to parse an embedding represented as a string into a list[float].
    - Handles JSON arrays first, and a simple bracketed CSV fallback like: "[0.1, 0.2]".
    - Returns None on failure which signals the caller to leave the raw value alone.
    """
    if s is None:
        return None

    # try JSON first
    try:
        parsed = json.loads(s)
        if isinstance(parsed, list):
            return [float(x) for x in parsed]
    except Exception:
        # fall through to fallback parser
        pass

    # fallback parse for strings like "[0.1, 0.2]" or "0.1,0.2"
    try:
        ss = s.strip()
        if ss.startswith("[") and ss.endswith("]"):
            inner = ss[1:-1].strip()
        else:
            inner = ss
        if inner == "":
            return []
        parts = [p.strip().strip('"').strip("'") for p in inner.split(",") if p.strip() != ""]
        out = []
        for p in parts:
            try:
                out.append(float(p))
            except Exception:
                log.info("Failed to parse embedding element '%s' in '%s'", p, s)
                return None
        return out
    except Exception:
        return None


# -----------------------------
# Utility: parse timestamp from state
# -----------------------------
def parse_state_timestamp(timestamp_str: Optional[str]) -> datetime:
    """
    Parse an ISO-like timestamp stored in state and return a timezone-aware datetime.
    If parsing fails or value missing, return a far-past sentinel datetime (UTC).
    """
    fallback = datetime(1990, 1, 1, tzinfo=timezone.utc)
    if not timestamp_str:
        return fallback
    try:
        # normalize trailing Z -> +00:00 for fromisoformat
        parsed = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        log.info("Failed to parse state timestamp '%s', using fallback.", timestamp_str)
        return fallback


# -----------------------------
# Row processing
# -----------------------------
def process_row(row_data: Dict[str, Any], table_name: str, configuration: Dict[str, Any], is_vector_table: bool) -> Dict[str, Any]:
    """
    Normalize row values before upsert:
    - Ensure naive datetimes returned by the driver get timezone set to UTC.
    - If vector table, attempt to parse embedding column to a Python list for JSON output.
    - Leave other fields untouched.
    Returns mutated row_data (same object for efficiency).
    """
    # Defensive: created_at/updated_at may be datetime objects or strings
    for ts_field in ("created_at", "updated_at"):
        if ts_field in row_data and row_data[ts_field] is not None:
            val = row_data[ts_field]
            # If it's a datetime-like object without tzinfo, set UTC
            if hasattr(val, "tzinfo"):
                if val.tzinfo is None:
                    row_data[ts_field] = val.replace(tzinfo=timezone.utc)
            else:
                # if it's a string, attempt parse from ISO
                try:
                    parsed = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    row_data[ts_field] = parsed
                except Exception:
                    # If parsing fails, leave original string but log once
                    log.fine("Could not parse %s value for table %s: %s", ts_field, table_name, val)

    # Vector table handling: parse embedding strings to lists so destination receives structured JSON
    if is_vector_table and configuration.get("VECTOR_TABLES_DATA"):
        try:
            vector_tables = json.loads(configuration["VECTOR_TABLES_DATA"])
            if table_name in vector_tables:
                embedding_column = vector_tables[table_name]["vector_column"]
                raw_embeddings = row_data.get(embedding_column)
                emb_list = parse_embedding_string_to_list(raw_embeddings)
                if emb_list is not None:
                    row_data[embedding_column] = emb_list
        except Exception:
            log.fine("Skipping vector parse for table %s due to malformed VECTOR_TABLES_DATA", table_name)

    return row_data


# -----------------------------
# Core ingestion: fetch and upsert
# -----------------------------
def fetch_and_upsert_data(cursor: TiDBClient, table_name: str, state: Dict[str, Any], configuration: Dict[str, Any], is_vector_table: bool = False):
    """
    Fetch rows newer than the timestamp tracked in state and upsert them.
    - Uses `created_at` for incremental tracking by default.
    - Writes progress back into state as ISO-8601 string.
    - Performs lightweight error handling so a single failing row doesn't break the whole table.
    - Referenced by `update`.
    """

    # read last processed timestamp for this table from state
    last_created = state.get(f"{table_name}_last_created", "1990-01-01T00:00:00Z")
    last_created_timestamp = parse_state_timestamp(last_created)

    # Build a conservative query: if created_at doesn't exist in the schema this will fail and be handled below.
    # Note: TiDB/MySQL datetime literals accept 'YYYY-MM-DD HH:MM:SS' with no timezone.
    try:
        # convert ISO to TiDB formatted timestamp: 'YYYY-MM-DD HH:MM:SS'
        # guard for either 'Z' or timezone suffix in last_created
        tidb_timestamp = last_created.replace("T", " ").replace("Z", "")
        tidb_query = f"SELECT * FROM {table_name} WHERE created_at > '{tidb_timestamp}' ORDER BY created_at"
    except Exception as e:
        log.severe("Failed to build query for table %s: %s", table_name, e)
        return

    try:
        query_result = cursor.query(tidb_query)
        # Some cursor implementations may return an object with to_list()
        if hasattr(query_result, "to_list"):
            rows = query_result.to_list()
        else:
            # If query_result is already iterable of dict-like rows
            rows = list(query_result)
    except Exception as e:
        # If the query failed (e.g. missing created_at column) log and skip this table.
        log.severe("Failed to execute query for table %s: %s", table_name, e)
        # write an error marker to state so operator can inspect, but don't crash the connector
        state[f"{table_name}_last_error"] = str(e)
        op.checkpoint(state)
        return

    # Iterate rows and upsert. We try to be resilient to individual row errors.
    max_seen_timestamp = last_created_timestamp

    for row in rows:
        try:
            row_data = process_row(row, table_name, configuration, is_vector_table)

            # Perform upsert with a small retry (3 attempts). Keep retries light to avoid long delays.
            upsert_attempts = 0
            success = False
            while upsert_attempts < 3 and not success:
                try:
                    op.upsert(table=table_name, data=row_data)
                    success = True
                except Exception as upp_err:
                    upsert_attempts += 1
                    log.info("Upsert failed for table %s row %s (attempt %d): %s", table_name, row.get("id", "<no-id>"), upsert_attempts, upp_err)
                    if upsert_attempts >= 3:
                        log.severe("Giving up on upsert for table %s row %s after %d attempts", table_name, row.get("id", "<no-id>"), upsert_attempts)

            # If upsert succeeded, update last seen created_at
            created_val = row_data.get("created_at")
            if created_val and hasattr(created_val, "tzinfo"):
                # If datetime object
                if created_val.tzinfo is None:
                    created_val = created_val.replace(tzinfo=timezone.utc)
                if created_val > max_seen_timestamp:
                    max_seen_timestamp = created_val
            else:
                # In some setups, created_at might be a string; attempt parse
                try:
                    parsed = parse_state_timestamp(str(created_val))
                    if parsed > max_seen_timestamp:
                        max_seen_timestamp = parsed
                except Exception:
                    # ignore - we won't advance timestamp if we can't parse
                    pass

        except Exception as row_err:
            # Log row-level errors and continue with other rows
            log.severe("Error processing row for table %s: %s", table_name, row_err)
            # Optionally capture sample of the row in state (trimmed) for operator debugging
            try:
                sample_key = f"{table_name}_last_row_error_sample"
                state[sample_key] = json.dumps({k: str(v) for k, v in (list(row.items())[:10])})
            except Exception:
                pass
            continue

    # Persist the last processed timestamp back to state as ISO string
    try:
        state[f"{table_name}_last_created"] = max_seen_timestamp.isoformat()
    except Exception:
        # Fallback: store as string representation if .isoformat() fails
        state[f"{table_name}_last_created"] = str(max_seen_timestamp)

    # Checkpoint to persist the state. This is safe to call even if no changes.
    try:
        op.checkpoint(state)
    except Exception as chk_err:
        log.severe("Failed to checkpoint state after processing table %s: %s", table_name, chk_err)


# -----------------------------
# TiDB connection helper
# -----------------------------
def create_tidb_connection(configuration: Dict[str, Any]) -> TiDBClient:
    """
    Create and return a TiDBClient connection based on configuration keys:
    TIDB_HOST, TIDB_USER, TIDB_PASS, TIDB_PORT, TIDB_DATABASE

    Raises a ValueError if required configuration keys are missing.
    """
    required_keys = ["TIDB_USER", "TIDB_PASS", "TIDB_HOST", "TIDB_PORT", "TIDB_DATABASE"]
    missing = [k for k in required_keys if not configuration.get(k)]
    if missing:
        raise ValueError(f"Missing required TiDB configuration keys: {', '.join(missing)}")

    user = configuration["TIDB_USER"]
    password = configuration["TIDB_PASS"]
    host = configuration["TIDB_HOST"]
    port = configuration["TIDB_PORT"]
    database = configuration["TIDB_DATABASE"]

    # Build DSN conservatively and include certifi CA bundle if TLS is desired
    try:
        TIDB_DATABASE_URL = (
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?ssl_ca={certifi.where()}"
        )
        connection = TiDBClient.connect(TIDB_DATABASE_URL)
        return connection
    except Exception as e:
        log.severe("Failed to create TiDB connection: %s", e)
        raise


# -----------------------------
# Update function called by the connector
# -----------------------------
def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    """
    Main update loop invoked by the Fivetran runtime.
    - Establish connection.
    - Iterate configured tables and call fetch_and_upsert_data.
    - Process optional vector tables separately.
    This function should be idempotent and robust to retries.
    """

    # Validate early so operator sees config problems immediately
    validate_configuration(configuration)

    try:
        connection = create_tidb_connection(configuration=configuration)
    except Exception as conn_err:
        log.severe("Could not connect to TiDB: %s", conn_err)
        # Surface the connection error in state for operator inspection and checkpoint
        state["last_connection_error"] = str(conn_err)
        try:
            op.checkpoint(state)
        except Exception:
            log.severe("Failed to checkpoint state after connection error.")
        # Re-raise to allow runtime to decide on retry/backoff
        raise

    # Parse list of tables from configuration
    try:
        tables = json.loads(configuration.get("TABLES_PRIMARY_KEY_COLUMNS", "{}")).keys()
    except Exception:
        log.severe("Failed to parse TABLES_PRIMARY_KEY_COLUMNS; nothing to do.")
        tables = []

    # Iterate non-vector tables
    for table_name in tables:
        try:
            fetch_and_upsert_data(cursor=connection, table_name=table_name, state=state, configuration=configuration)
        except Exception as t_err:
            # Catch table-level exceptions to allow other tables to be processed
            log.severe("Unhandled error processing table %s: %s", table_name, t_err)
            state[f"{table_name}_last_error"] = str(t_err)
            try:
                op.checkpoint(state)
            except Exception:
                log.severe("Failed to checkpoint state after table-level error for %s", table_name)

    # Process optional vector tables (may overlap with tables above if user included them there)
    if configuration.get("VECTOR_TABLES_DATA"):
        try:
            vector_tables = json.loads(configuration["VECTOR_TABLES_DATA"]).keys()
        except Exception:
            log.info("Failed to parse VECTOR_TABLES_DATA; skipping vector table processing.")
            vector_tables = []

        for table_name in vector_tables:
            try:
                fetch_and_upsert_data(cursor=connection, table_name=table_name, state=state, configuration=configuration, is_vector_table=True)
            except Exception as t_err:
                log.severe("Unhandled error processing vector table %s: %s", table_name, t_err)
                state[f"{table_name}_last_error"] = str(t_err)
                try:
                    op.checkpoint(state)
                except Exception:
                    log.severe("Failed to checkpoint state after vector-table error for %s", table_name)

    # Close the connection if the client exposes a close method
    try:
        if hasattr(connection, "close"):
            connection.close()
    except Exception:
        # Non-critical; log and proceed
        log.fine("Error while closing TiDB connection (non-fatal).")

# -----------------------------
# Connector bootstrap
# -----------------------------
connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    # Local debug helper - load local configuration.json and run connector.debug
    try:
        with open("configuration.json", "r") as f:
            configuration = json.load(f)
    except Exception as e:
        log.severe("Failed to load configuration.json: %s", e)
        raise

    # Run the connector in debug mode; let any exception bubble so local user can see stack trace.
    connector.debug(configuration=configuration)
