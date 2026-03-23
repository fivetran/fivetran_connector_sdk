# This is an example for how to work with the fivetran_connector_sdk module.
# It demonstrates log-based Change Data Capture (CDC) for IBM Db2 using the
# IBM SQL Replication ASN (Apply-Snapshot-Notify) framework.
#
# How the log pipeline works:
#   Db2 transaction log
#       └─► asncap daemon (reads log via db2ReadLog C API)
#               └─► ASN.IBMSNAP_EMPCD  (Change Data table — one row per INSERT/UPDATE/DELETE)
#                       └─► this connector (reads CD table, applies to destination)
#
# The connector never reads the source EMPLOYEE table after the initial full load.
# All incremental syncs are driven exclusively by what asncap wrote to the CD table
# after reading the transaction log — making this genuine log-based replication.
#
# Prerequisites (run setup_cdc.sh inside the Db2 Docker container):
#   1. Db2 Community Edition running locally via docker-compose.yml
#   2. ARCHIVE_LOGS=true set in docker-compose.yml (enables archival logging)
#   3. EMPLOYEE table with DATA CAPTURE CHANGES enabled
#   4. ASN control tables and CD table created by setup_cdc.sh
#   5. asncap daemon running (started by setup_cdc.sh)
#
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk.
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# ibm_db is used only to query the ASN Change Data table —
# it does not read the source EMPLOYEE table during incremental syncs.
import ibm_db
import json
from datetime import datetime, timezone
from decimal import Decimal

# ASN schema and CD table name as created by setup_cdc.sh
ASN_SCHEMA = "ASN"
CD_TABLE = "IBMSNAP_EMPCD"

# How many CD rows to process before writing an intermediate checkpoint
CHECKPOINT_INTERVAL = 500


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "employee",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "first_name": "STRING",
                "last_name": "STRING",
                "email": "STRING",
                "department": "STRING",
                "salary": "FLOAT",
            },
        }
    ]


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required fields.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    required_keys = ["hostname", "port", "database", "user_id", "password", "schema_name"]
    for key in required_keys:
        if key not in configuration:
            raise ValueError(f"Missing required configuration key: {key}")


def create_connection_string(configuration: dict) -> str:
    """
    Build an ibm_db connection string from the configuration dictionary.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        str: A formatted connection string for the IBM Db2 database.
    """
    return (
        f"DATABASE={configuration['database']};"
        f"HOSTNAME={configuration['hostname']};"
        f"PORT={configuration['port']};"
        f"PROTOCOL=TCPIP;"
        f"UID={configuration['user_id']};"
        f"PWD={configuration['password']};"
    )


def connect_to_db(configuration: dict):
    """
    Establish a connection to the IBM Db2 database.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        conn: A connection object if the connection is successful.
    """
    conn_str = create_connection_string(configuration)
    try:
        conn = ibm_db.connect(conn_str, "", "")
        log.info("Connected to IBM Db2 successfully.")
        return conn
    except Exception as e:
        log.severe(f"Connection failed: {e}")
        raise RuntimeError("Connection failed") from e


def standardize_row(row: dict) -> dict:
    """
    Lower-case all column keys and coerce values to Python-native types.
    Args:
        row: A dictionary representing a row fetched from the database.
    Returns:
        A new dictionary with lower-cased keys and coerced values.
    """
    result = {}
    for key, value in row.items():
        key = key.lower()
        if isinstance(value, int):
            result[key] = int(value)
        elif isinstance(value, (float, Decimal)):
            result[key] = float(value)
        elif isinstance(value, str):
            result[key] = value.strip()
        else:
            result[key] = str(value) if value is not None else None
    return result


def get_current_log_marker(conn) -> str:
    """
    Return the latest IBMSNAP_LOGMARKER in the CD table as an ISO timestamp string.
    This is used to anchor the initial-load cursor: changes written to the CD table
    during the full scan will have a LOGMARKER after this value, so they will be
    replayed on the first incremental sync without any rows being missed.
    Args:
        conn: A connection object to the IBM Db2 database.
    Returns:
        str: ISO timestamp of the maximum log marker, or epoch start if table is empty.
    """
    sql = f"SELECT COALESCE(MAX(IBMSNAP_LOGMARKER), TIMESTAMP('1970-01-01-00.00.00.000000')) FROM {ASN_SCHEMA}.{CD_TABLE}"
    stmt = ibm_db.exec_immediate(conn, sql)
    if not stmt:
        raise RuntimeError("Failed to query current log marker.")
    row = ibm_db.fetch_tuple(stmt)
    ts = row[0] if row else "1970-01-01 00:00:00"
    # ibm_db returns Db2 timestamps as strings like "2024-01-15 10:30:45.123456"
    return str(ts).replace("-", "-").strip()


def perform_initial_load(conn, schema_name: str) -> str:
    """
    Perform a full table scan of EMPLOYEE and upsert every row to the destination.
    The log marker high-water mark is captured *before* the scan so that any
    changes written to the CD table while the initial scan is running will be
    picked up by the first incremental sync.
    Args:
        conn: A connection object to the IBM Db2 database.
        schema_name: The Db2 schema that owns the EMPLOYEE table.
    Returns:
        str: The IBMSNAP_LOGMARKER high-water mark captured before the scan.
    """
    # Snapshot the log marker before we start scanning the source table.
    # ASN Capture may be writing to the CD table concurrently; those rows
    # will have LOGMARKER > high_water_mark and will be processed on the next run.
    high_water_mark = get_current_log_marker(conn)
    log.info(f"Initial load: log marker high-water mark before scan = {high_water_mark}")

    sql = (
        f"SELECT ID, FIRST_NAME, LAST_NAME, EMAIL, DEPARTMENT, CAST(SALARY AS DOUBLE) AS SALARY "
        f"FROM {schema_name}.EMPLOYEE ORDER BY ID"
    )
    stmt = ibm_db.exec_immediate(conn, sql)
    if not stmt:
        raise RuntimeError("Failed to execute initial load query.")

    row_count = 0
    while True:
        row = ibm_db.fetch_assoc(stmt)
        if not row:
            break
        op.upsert("employee", standardize_row(row))
        row_count += 1

        if row_count % CHECKPOINT_INTERVAL == 0:
            op.checkpoint({"last_log_marker": high_water_mark, "initial_load_complete": False})

    log.info(f"Initial load complete: {row_count} rows upserted.")
    return high_water_mark


def process_cdc_changes(conn, last_log_marker: str) -> str:
    """
    Read all rows from the ASN Change Data table with IBMSNAP_LOGMARKER greater
    than last_log_marker and apply them to the destination in chronological order.

    The IBMSNAP_LOGMARKER column contains the wall-clock timestamp that ASN Capture
    copied from the Db2 transaction log entry — it reflects when the original DML
    committed on the source database, not when we read it.

    IBMSNAP_OPERATION values written by asncap:
      'I' – row was inserted  → op.upsert()
      'U' – row was updated   → op.upsert()  (new values)
      'D' – row was deleted   → op.delete()

    Args:
        conn: A connection object to the IBM Db2 database.
        last_log_marker: ISO timestamp string from the previous checkpoint.
    Returns:
        str: The new IBMSNAP_LOGMARKER high-water mark after processing all changes.
    """
    sql = (
        f"SELECT IBMSNAP_OPERATION, IBMSNAP_LOGMARKER, "
        f"ID, FIRST_NAME, LAST_NAME, EMAIL, DEPARTMENT, CAST(SALARY AS DOUBLE) AS SALARY "
        f"FROM {ASN_SCHEMA}.{CD_TABLE} "
        f"WHERE IBMSNAP_LOGMARKER > TIMESTAMP('{last_log_marker}') "
        f"ORDER BY IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ"
    )
    stmt = ibm_db.exec_immediate(conn, sql)
    if not stmt:
        raise RuntimeError("Failed to query CD table.")

    current_marker = last_log_marker
    row_count = 0

    while True:
        row = ibm_db.fetch_assoc(stmt)
        if not row:
            break

        operation = row["IBMSNAP_OPERATION"].strip()
        log_marker = str(row["IBMSNAP_LOGMARKER"]).strip()

        # Build the source-table payload (strip ASN metadata columns)
        payload = standardize_row(
            {k: v for k, v in row.items() if k not in ("IBMSNAP_OPERATION", "IBMSNAP_LOGMARKER")}
        )

        if operation in ("I", "U"):
            # INSERT or UPDATE: upsert the latest version of the row into the destination.
            # ASN Capture stores new-image values for both operations.
            op.upsert("employee", payload)
        elif operation == "D":
            # DELETE: remove the row from the destination.
            # The CD row still contains the key column value so we know which row to drop.
            op.delete("employee", {"id": payload["id"]})
        else:
            log.warning(f"Unrecognised ASN operation '{operation}'; skipping row.")

        current_marker = log_marker
        row_count += 1

        if row_count % CHECKPOINT_INTERVAL == 0:
            op.checkpoint({"last_log_marker": current_marker, "initial_load_complete": True})

    log.info(
        f"CDC sync complete: {row_count} log event(s) applied. "
        f"New log marker: {current_marker}"
    )
    return current_marker


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    First sync:  full initial load from EMPLOYEE, cursor set to current log marker.
    All later syncs: read only from the ASN CD table (log events), never the source table.

    Args:
        configuration: A dictionary containing connection details.
        state: A dictionary containing state information from previous runs.
               The state dictionary is empty for the first sync or for any full re-sync.
    """
    log.warning("Example: Source Examples: IBM Db2 Log-Based Replication")

    validate_configuration(configuration)

    conn = connect_to_db(configuration)
    schema_name = configuration["schema_name"]

    try:
        last_log_marker = state.get("last_log_marker")

        if last_log_marker is None:
            # ── First sync: full initial load ──────────────────────────────────
            # Reads directly from the source EMPLOYEE table once to populate the
            # destination. After this, all changes come from the ASN CD table.
            log.info("No previous state found. Starting initial full load.")
            last_log_marker = perform_initial_load(conn, schema_name)
            new_state = {"last_log_marker": last_log_marker, "initial_load_complete": True}
        else:
            # ── Subsequent syncs: read from the ASN CD table ───────────────────
            # The CD table was populated by the asncap daemon reading the Db2
            # transaction log — this connector never queries EMPLOYEE again.
            log.info(f"Resuming incremental sync from log marker: {last_log_marker}")
            last_log_marker = process_cdc_changes(conn, last_log_marker)
            new_state = {"last_log_marker": last_log_marker, "initial_load_complete": True}

        # Save the final state so the next sync knows where to continue from.
        # Learn more about checkpointing:
        # https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation
        op.checkpoint(new_state)

    finally:
        ibm_db.close(conn)
        log.info("IBM Db2 connection closed.")


# This creates the connector object that will use the update and schema functions defined above.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug(configuration=configuration)
