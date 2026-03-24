"""This connector demonstrates log-based Change Data Capture (CDC) for IBM Db2 using the ASN SQL Replication framework.
The asncap daemon reads the Db2 transaction log and writes every INSERT, UPDATE, and DELETE to a Change Data table;
the connector reads exclusively from that table after the initial full load, making this genuine log-based replication.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# ibm_db is the IBM Db2 driver used to connect and query the database.
# It is used only to query the ASN Change Data table during incremental syncs
# and the source EMPLOYEE table during the initial full load.
import ibm_db

# For reading configuration from a JSON file
import json

# For coercing DECIMAL column values returned as Python Decimal objects
from decimal import Decimal

# Schema that owns the Change Data (CD) table created by ASNCLP for the EMPLOYEE table.
# ASNCLP places the CD table in the source owner's schema by default.
CD_SCHEMA = "DB2INST1"

# Change Data table written to by the asncap daemon.
# asncap reads the Db2 transaction log and inserts one row here per INSERT/UPDATE/DELETE.
# ASNCLP names this table CD<source_table>, so EMPLOYEE → CDEMPLOYEE.
CD_TABLE = "CDEMPLOYEE"

# Number of CD rows to process before writing an intermediate checkpoint.
# Checkpointing regularly prevents re-processing large batches on retry.
__CHECKPOINT_INTERVAL = 500


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "employee",  # Name of the table in the destination, required.
            "primary_key": ["id"],  # Primary key column(s) for the table.
            "columns": {  # Definition of columns and their types.
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
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector
    has all necessary configuration values before attempting a database connection.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configuration_keys = [
        "hostname",
        "port",
        "database",
        "user_id",
        "password",
        "schema_name",
    ]
    for key in required_configuration_keys:
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


def connect_to_database(configuration: dict):
    """
    Establish a connection to the IBM Db2 database using ibm_db.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        connection: A connection object if the connection is successful.
    Raises:
        RuntimeError: if the connection attempt fails.
    """
    connection_string = create_connection_string(configuration)
    try:
        connection = ibm_db.connect(connection_string, "", "")
        log.info("Connected to IBM Db2 successfully.")
        return connection
    except Exception as e:
        log.severe(f"Connection failed: {e}")
        raise RuntimeError("Connection failed") from e


def normalize_row(database_row: dict) -> dict:
    """
    Lower-case all column keys and coerce values to Python-native types.
    ibm_db returns column names in uppercase and may return DECIMAL columns as
    Python str — this function normalises both before passing records to the SDK.
    Args:
        database_row: A dictionary representing a row fetched from the database.
    Returns:
        A new dictionary with lower-cased keys and coerced primitive values.
    """
    normalized_record = {}
    for column_name, column_value in database_row.items():
        column_name = column_name.lower()
        if isinstance(column_value, int):
            normalized_record[column_name] = int(column_value)
        elif isinstance(column_value, (float, Decimal)):
            normalized_record[column_name] = float(column_value)
        elif isinstance(column_value, str):
            normalized_record[column_name] = column_value.strip()
        else:
            normalized_record[column_name] = (
                str(column_value) if column_value is not None else None
            )
    return normalized_record


def get_current_commit_sequence(connection) -> str:
    """
    Return the latest IBMSNAP_COMMITSEQ in the CD table as a hex string.
    This is used to anchor the initial-load cursor: changes written to the CD table
    during the full scan will have a COMMITSEQ after this value, so they will be
    replayed on the first incremental sync without any rows being missed.
    Args:
        connection: A connection object to the IBM Db2 database.
    Returns:
        str: Hex string of the maximum COMMITSEQ, or '0' if the table is empty.
    """
    query = f"SELECT HEX(MAX(IBMSNAP_COMMITSEQ)) FROM {CD_SCHEMA}.{CD_TABLE}"
    statement = ibm_db.exec_immediate(connection, query)
    if not statement:
        raise RuntimeError("Failed to query current commit sequence.")
    result_row = ibm_db.fetch_tuple(statement)
    return result_row[0] if (result_row and result_row[0]) else "0"


def perform_initial_load(connection, schema_name: str) -> str:
    """
    Perform a full table scan of EMPLOYEE and upsert every row to the destination.
    The COMMITSEQ high-water mark is captured *before* the scan so that any
    changes written to the CD table while the initial scan is running will be
    picked up by the first incremental sync.
    Args:
        connection: A connection object to the IBM Db2 database.
        schema_name: The Db2 schema that owns the EMPLOYEE table.
    Returns:
        str: The IBMSNAP_COMMITSEQ high-water mark (hex) captured before the scan.
    """
    # Snapshot the COMMITSEQ high-water mark before scanning the source table.
    # Any CD rows written concurrently will have COMMITSEQ > this value and
    # will be processed on the first incremental sync.
    commit_sequence_high_water_mark = get_current_commit_sequence(connection)
    log.info(
        f"Initial load: reading directly from source EMPLOYEE table. "
        f"COMMITSEQ high-water mark = {commit_sequence_high_water_mark}"
    )

    query = (
        f"SELECT ID, FIRST_NAME, LAST_NAME, EMAIL, DEPARTMENT, CAST(SALARY AS DOUBLE) AS SALARY "
        f"FROM {schema_name}.EMPLOYEE ORDER BY ID"
    )
    statement = ibm_db.exec_immediate(connection, query)
    if not statement:
        raise RuntimeError("Failed to execute initial load query.")

    row_count = 0
    while True:
        database_row = ibm_db.fetch_assoc(statement)
        if not database_row:
            break

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert("employee", normalize_row(database_row))
        row_count += 1

        if row_count % __CHECKPOINT_INTERVAL == 0:
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(
                {
                    "last_commit_sequence": commit_sequence_high_water_mark,
                    "initial_load_complete": False,
                }
            )

    log.info(f"Initial load complete: {row_count} rows upserted from EMPLOYEE table.")
    return commit_sequence_high_water_mark


def process_cdc_changes(connection, last_commit_sequence: str) -> str:
    """
    Read all rows from the ASN Change Data table (DB2INST1.CDEMPLOYEE) with
    IBMSNAP_COMMITSEQ greater than last_commit_sequence and apply them to the destination.

    asncap writes one row to this CD table for every INSERT/UPDATE/DELETE it reads
    from the Db2 transaction log — the connector never queries EMPLOYEE directly.

    IBMSNAP_OPERATION values written by asncap:
      'I' – row was inserted  → op.upsert()
      'U' – row was updated   → op.upsert()  (new-image values)
      'D' – row was deleted   → op.delete()

    Args:
        connection: A connection object to the IBM Db2 database.
        last_commit_sequence: Hex string of the last processed COMMITSEQ.
    Returns:
        str: The new IBMSNAP_COMMITSEQ high-water mark (hex) after processing.
    """
    # If no prior cursor exists, start from the very beginning of the CD table.
    if last_commit_sequence == "0":
        where_clause = "1=1"
    else:
        where_clause = f"HEX(IBMSNAP_COMMITSEQ) > '{last_commit_sequence}'"

    query = (
        f"SELECT IBMSNAP_OPERATION, HEX(IBMSNAP_COMMITSEQ) AS COMMITSEQ_HEX, "
        f"ID, FIRST_NAME, LAST_NAME, EMAIL, DEPARTMENT, CAST(SALARY AS DOUBLE) AS SALARY "
        f"FROM {CD_SCHEMA}.{CD_TABLE} "
        f"WHERE {where_clause} "
        f"ORDER BY IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ"
    )
    log.info(
        f"Incremental sync: reading from {CD_SCHEMA}.{CD_TABLE} "
        f"(populated by asncap from Db2 transaction log). "
        f"Cursor COMMITSEQ = {last_commit_sequence}"
    )
    statement = ibm_db.exec_immediate(connection, query)
    if not statement:
        raise RuntimeError("Failed to query CD table.")

    current_commit_sequence = last_commit_sequence
    row_count = 0

    while True:
        database_row = ibm_db.fetch_assoc(statement)
        if not database_row:
            break

        change_operation = database_row["IBMSNAP_OPERATION"].strip()
        current_commit_sequence = str(database_row["COMMITSEQ_HEX"]).strip()

        # Build the source-table record, stripping ASN metadata columns.
        record = normalize_row(
            {
                column_name: column_value
                for column_name, column_value in database_row.items()
                if column_name not in ("IBMSNAP_OPERATION", "COMMITSEQ_HEX")
            }
        )

        if change_operation in ("I", "U"):
            # INSERT or UPDATE: asncap wrote new-image values from the transaction log.
            log.info(
                f"  LOG EVENT [{change_operation}] id={record.get('id')} — sourced from Db2 transaction log via asncap"
            )
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert("employee", record)
        elif change_operation == "D":
            # DELETE: the CD row still carries the key so we know which row to remove.
            log.info(
                f"  LOG EVENT [D] id={record.get('id')} — sourced from Db2 transaction log via asncap"
            )
            # The 'delete' operation is used to delete data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be deleted.
            op.delete("employee", {"id": record["id"]})
        else:
            log.warning(f"Unrecognised ASN operation '{change_operation}'; skipping row.")

        row_count += 1

        if row_count % __CHECKPOINT_INTERVAL == 0:
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(
                {
                    "last_commit_sequence": current_commit_sequence,
                    "initial_load_complete": True,
                }
            )

    log.info(
        f"CDC sync complete: {row_count} log event(s) applied from {CD_SCHEMA}.{CD_TABLE}. "
        f"New COMMITSEQ cursor = {current_commit_sequence}"
    )
    return current_commit_sequence


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

    connection = connect_to_database(configuration)
    schema_name = configuration["schema_name"]

    try:
        last_commit_sequence = state.get("last_commit_sequence")

        if last_commit_sequence is None:
            # ── First sync: full initial load ──────────────────────────────────
            # Reads directly from the source EMPLOYEE table once to populate the
            # destination. After this, all changes come from the ASN CD table
            # (DB2INST1.CDEMPLOYEE), which is written by the asncap daemon from
            # the Db2 transaction log.
            log.info("No previous state found. Starting initial full load from EMPLOYEE table.")
            last_commit_sequence = perform_initial_load(connection, schema_name)
            new_state = {
                "last_commit_sequence": last_commit_sequence,
                "initial_load_complete": True,
            }
        else:
            # ── Subsequent syncs: read from DB2INST1.CDEMPLOYEE ────────────────
            # asncap wrote these rows by reading the Db2 transaction log.
            # This connector never queries EMPLOYEE again after the first sync.
            log.info(f"Resuming incremental sync from COMMITSEQ: {last_commit_sequence}")
            last_commit_sequence = process_cdc_changes(connection, last_commit_sequence)
            new_state = {
                "last_commit_sequence": last_commit_sequence,
                "initial_load_complete": True,
            }

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(new_state)

    finally:
        ibm_db.close(connection)
        log.info("IBM Db2 connection closed.")


# This creates the connector object that will use the update and schema functions defined above.
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
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug(configuration=configuration)
