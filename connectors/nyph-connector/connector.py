"""NYPH Connector v2 for Fivetran with Snowflake Integration.

This connector processes two file types with different sync strategies:
- IncrementalFullMix: Delete rows by UserID, then insert all from file
- RollingIncremental: Delete rows by TransactionDate threshold, then insert all from file

This version uses Snowflake as the destination with key-pair authentication and supports
large CSV files (up to ~10GB) with chunked processing and frequent checkpointing.

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting checkpoint() operation
from fivetran_connector_sdk import Operations as op

# Snowflake connector
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa

# Batch size for processing records
BATCH_SIZE = 100000

# Demo data for IncrementalFullMix mode (temporary - can be removed later)
DEMO_INCREMENTAL_FULL_MIX_DATA = [
    {
        "UserID": "11",
        "FirstName": "John",
        "LastName": "Doe",
        "Email": "john.doe@example.com",
        "Age": "32",
        "Gender": "Male",
        "City": "New York",
        "State": "NY",
        "Country": "USA",
        "PostalCode": "10001",
        "PhoneNumber": "555-0101",
    },
    {
        "UserID": "12",
        "FirstName": "Jane",
        "LastName": "Smith",
        "Email": "jane.smith@example.com",
        "Age": "28",
        "Gender": "Female",
        "City": "Los Angeles",
        "State": "CA",
        "Country": "USA",
        "PostalCode": "90001",
        "PhoneNumber": "555-0102",
    },
    {
        "UserID": "13",
        "FirstName": "Bob",
        "LastName": "Johnson",
        "Email": "bob.johnson@example.com",
        "Age": "45",
        "Gender": "Male",
        "City": "Chicago",
        "State": "IL",
        "Country": "USA",
        "PostalCode": "60601",
        "PhoneNumber": "555-0103",
    },
    {
        "UserID": "14",
        "FirstName": "Alice",
        "LastName": "Williams",
        "Email": "alice.williams@example.com",
        "Age": "35",
        "Gender": "Female",
        "City": "Houston",
        "State": "TX",
        "Country": "USA",
        "PostalCode": "77001",
        "PhoneNumber": "555-0104",
    },
    {
        "UserID": "15",
        "FirstName": "Charlie",
        "LastName": "Brown",
        "Email": "charlie.brown@example.com",
        "Age": "29",
        "Gender": "Male",
        "City": "Phoenix",
        "State": "AZ",
        "Country": "USA",
        "PostalCode": "85001",
        "PhoneNumber": "555-0105",
    },
    {
        "UserID": "6",
        "FirstName": "Diana_U",
        "LastName": "Martinez",
        "Email": "diana.martinez@example.com",
        "Age": "41",
        "Gender": "Female",
        "City": "Philadelphia",
        "State": "PA",
        "Country": "USA",
        "PostalCode": "19101",
        "PhoneNumber": "555-0106",
    },
    {
        "UserID": "7",
        "FirstName": "Edward_U",
        "LastName": "Davis",
        "Email": "edward.davis@example.com",
        "Age": "38",
        "Gender": "Male",
        "City": "San Antonio",
        "State": "TX",
        "Country": "USA",
        "PostalCode": "78201",
        "PhoneNumber": "555-0107",
    },
    {
        "UserID": "8",
        "FirstName": "Fiona_U",
        "LastName": "Garcia",
        "Email": "fiona.garcia@example.com",
        "Age": "27",
        "Gender": "Female",
        "City": "San Diego",
        "State": "CA",
        "Country": "USA",
        "PostalCode": "92101",
        "PhoneNumber": "555-0108",
    },
    {
        "UserID": "9",
        "FirstName": "George_U",
        "LastName": "Rodriguez",
        "Email": "george.rodriguez@example.com",
        "Age": "33",
        "Gender": "Male",
        "City": "Dallas",
        "State": "TX",
        "Country": "USA",
        "PostalCode": "75201",
        "PhoneNumber": "555-0109",
    },
    {
        "UserID": "10",
        "FirstName": "Hannah_U",
        "LastName": "Wilson",
        "Email": "hannah.wilson@example.com",
        "Age": "31",
        "Gender": "Female",
        "City": "San Jose",
        "State": "CA",
        "Country": "USA",
        "PostalCode": "95101",
        "PhoneNumber": "555-0110",
    },
]

# Demo data for RollingIncremental mode (temporary - can be removed later)
DEMO_ROLLING_INCREMENTAL_DATA = [
    {
        "UserID": "11",
        "TransactionDate": "2025-01-25",
        "FirstName": "John",
        "LastName": "Doe",
        "Email": "john.doe@example.com",
        "Age": "32",
        "Gender": "Male",
        "City": "New York",
        "State": "NY",
        "Country": "USA",
        "PostalCode": "10001",
        "PhoneNumber": "555-0101",
    },
    {
        "UserID": "22",
        "TransactionDate": "2025-01-26",
        "FirstName": "Jane",
        "LastName": "Smith",
        "Email": "jane.smith@example.com",
        "Age": "28",
        "Gender": "Female",
        "City": "Los Angeles",
        "State": "CA",
        "Country": "USA",
        "PostalCode": "90001",
        "PhoneNumber": "555-0102",
    },
    {
        "UserID": "13",
        "TransactionDate": "2025-01-27",
        "FirstName": "Bob",
        "LastName": "Johnson",
        "Email": "bob.johnson@example.com",
        "Age": "45",
        "Gender": "Male",
        "City": "Chicago",
        "State": "IL",
        "Country": "USA",
        "PostalCode": "60601",
        "PhoneNumber": "555-0103",
    },
    {
        "UserID": "14",
        "TransactionDate": "2025-01-28",
        "FirstName": "John",
        "LastName": "Doe",
        "Email": "john.doe@example.com",
        "Age": "32",
        "Gender": "Male",
        "City": "New York",
        "State": "NY",
        "Country": "USA",
        "PostalCode": "10001",
        "PhoneNumber": "555-0101",
    },
    {
        "UserID": "15",
        "TransactionDate": "2025-01-29",
        "FirstName": "Alice",
        "LastName": "Williams",
        "Email": "alice.williams@example.com",
        "Age": "35",
        "Gender": "Female",
        "City": "Houston",
        "State": "TX",
        "Country": "USA",
        "PostalCode": "77001",
        "PhoneNumber": "555-0104",
    },
    {
        "UserID": "6",
        "TransactionDate": "2025-01-20",
        "FirstName": "Charlie_U",
        "LastName": "Brown",
        "Email": "charlie.brown@example.com",
        "Age": "29",
        "Gender": "Male",
        "City": "Phoenix",
        "State": "AZ",
        "Country": "USA",
        "PostalCode": "85001",
        "PhoneNumber": "555-0105",
    },
    {
        "UserID": "7",
        "TransactionDate": "2025-01-21",
        "FirstName": "Jane_U",
        "LastName": "Smith",
        "Email": "jane.smith@example.com",
        "Age": "28",
        "Gender": "Female",
        "City": "Los Angeles",
        "State": "CA",
        "Country": "USA",
        "PostalCode": "90001",
        "PhoneNumber": "555-0102",
    },
    {
        "UserID": "8",
        "TransactionDate": "2025-01-22",
        "FirstName": "Diana_U",
        "LastName": "Martinez",
        "Email": "diana.martinez@example.com",
        "Age": "41",
        "Gender": "Female",
        "City": "Philadelphia",
        "State": "PA",
        "Country": "USA",
        "PostalCode": "19101",
        "PhoneNumber": "555-0106",
    },
    {
        "UserID": "9",
        "TransactionDate": "2025-01-23",
        "FirstName": "Edward_U",
        "LastName": "Davis",
        "Email": "edward.davis@example.com",
        "Age": "38",
        "Gender": "Male",
        "City": "San Antonio",
        "State": "TX",
        "Country": "USA",
        "PostalCode": "78201",
        "PhoneNumber": "555-0107",
    },
    {
        "UserID": "10",
        "TransactionDate": "2025-01-24",
        "FirstName": "Bob_U",
        "LastName": "Johnson",
        "Email": "bob.johnson@example.com",
        "Age": "45",
        "Gender": "Male",
        "City": "Chicago",
        "State": "IL",
        "Country": "USA",
        "PostalCode": "60601",
        "PhoneNumber": "555-0103",
    },
]


def validate_configuration(configuration: dict) -> None:
    """Validate the configuration dictionary to ensure it contains all required parameters.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    if "sync_mode" not in configuration:
        raise ValueError("Missing required configuration value: sync_mode")

    sync_mode = configuration["sync_mode"]
    if sync_mode not in ["incremental_full_mix", "rolling_incremental"]:
        raise ValueError(
            f"Invalid sync_mode: {sync_mode}. Must be 'incremental_full_mix' or 'rolling_incremental'"
        )

    # file_path defaults to "input-files" if not provided
    if "file_path" not in configuration:
        configuration["file_path"] = "input-files"

    # Validate Snowflake connection parameters
    required_snowflake_params = [
        "snowflake_account",
        "snowflake_user",
        "snowflake_warehouse",
        "snowflake_database",
        "snowflake_schema",
        "snowflake_private_key",
    ]
    for param in required_snowflake_params:
        if param not in configuration:
            raise ValueError(f"Missing required Snowflake configuration value: {param}")


def get_connector_directory() -> Path:
    """Get the directory where connector.py is located.

    Returns:
        Path object pointing to the connector directory
    """
    return Path(__file__).parent.absolute()


def is_initial_sync(state: dict) -> bool:
    """Check if this is an initial sync based on state.

    Args:
        state: State dictionary from previous runs

    Returns:
        True if this is an initial sync (no previous sync state), False otherwise
    """
    return not state or "last_sync_time" not in state or not state.get("last_sync_time")


def load_private_key(private_key_str: str, passphrase: Optional[str] = None) -> rsa.RSAPrivateKey:
    """Load and decrypt private key from PEM string.

    This function handles both encrypted and unencrypted private keys.
    If the key is encrypted, it will be decrypted using the provided passphrase.

    Args:
        private_key_str: Private key content as PEM format string
        passphrase: Passphrase for encrypted private key (required if key is encrypted)

    Returns:
        Decrypted RSAPrivateKey object for Snowflake authentication
    """
    try:
        # Check if placeholder text is still in the key
        if "YOUR_PRIVATE_KEY_CONTENT_HERE" in private_key_str:
            raise ValueError(
                "Private key contains placeholder text. Please replace with your actual private key."
            )

        # Parse the private key string
        private_key_bytes = private_key_str.encode("utf-8")

        # Check if key appears to be encrypted
        is_encrypted = b"ENCRYPTED" in private_key_bytes or "ENCRYPTED" in private_key_str.upper()

        # Prepare passphrase
        password = None
        if passphrase and passphrase.strip():
            password = passphrase.encode("utf-8")

        # If key is encrypted, require passphrase
        if is_encrypted:
            if not password:
                raise ValueError(
                    "Private key is encrypted but no passphrase provided. "
                    "Please provide 'snowflake_private_key_passphrase' in configuration."
                )

            # Decrypt the encrypted key
            try:
                private_key = serialization.load_pem_private_key(
                    private_key_bytes, password=password, backend=default_backend()
                )
                log.info("Successfully decrypted and loaded encrypted private key")
            except ValueError as e:
                raise ValueError(
                    f"Failed to decrypt private key with provided passphrase: {str(e)}. "
                    f"Please verify the passphrase is correct."
                )
        else:
            # Key is not encrypted, load without password
            if password:
                log.warning(
                    "Passphrase provided but key appears unencrypted. Ignoring passphrase."
                )

            try:
                private_key = serialization.load_pem_private_key(
                    private_key_bytes, password=None, backend=default_backend()
                )
                log.info("Successfully loaded unencrypted private key")
            except ValueError as e:
                # If loading without password fails, try with password (might be encrypted after all)
                if password:
                    try:
                        private_key = serialization.load_pem_private_key(
                            private_key_bytes, password=password, backend=default_backend()
                        )
                        log.info("Successfully decrypted private key (detected encryption)")
                    except Exception as e2:
                        raise ValueError(
                            f"Failed to load private key. "
                            f"Tried unencrypted: {str(e)}. "
                            f"Tried with passphrase: {str(e2)}"
                        )
                else:
                    raise ValueError(f"Failed to load unencrypted private key: {str(e)}")

        # Verify it's an RSA key
        if not isinstance(private_key, rsa.RSAPrivateKey):
            raise ValueError("Private key must be an RSA key")

        # The key is now decrypted and ready to use
        return private_key

    except ValueError as e:
        # Re-raise ValueError as-is (already has good message)
        raise
    except Exception as e:
        raise ValueError(
            f"Failed to load private key: {str(e)}. "
            f"Ensure the key is in PEM format (PKCS#8). "
            f"If encrypted, provide the correct passphrase."
        )


def create_snowflake_connection(configuration: dict) -> snowflake.connector.SnowflakeConnection:
    """Create a Snowflake connection using key-pair authentication.

    Args:
        configuration: Configuration dictionary with Snowflake parameters

    Returns:
        Snowflake connection object
    """
    private_key_pem = load_private_key(
        configuration["snowflake_private_key"],
        configuration.get("snowflake_private_key_passphrase"),
    )

    try:
        conn = snowflake.connector.connect(
            account=configuration["snowflake_account"],
            user=configuration["snowflake_user"],
            warehouse=configuration["snowflake_warehouse"],
            database=configuration["snowflake_database"],
            schema=configuration["snowflake_schema"],
            role=configuration.get("snowflake_role"),
            private_key=private_key_pem,
        )
        log.info("Successfully connected to Snowflake")
        return conn
    except Exception as e:
        raise RuntimeError(f"Failed to connect to Snowflake: {str(e)}")


def read_csv_columns(file_path: Path) -> List[str]:
    """Read column names from the first row of a CSV file.

    Args:
        file_path: Path to the CSV file

    Returns:
        List of column names with spaces stripped

    Raises:
        FileNotFoundError: if file doesn't exist
        ValueError: if file has no header row
    """
    if not file_path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            raw_column_names = reader.fieldnames if reader.fieldnames else []

            if not raw_column_names:
                raise ValueError(f"CSV file {file_path} has no header row")

            return [col.strip() for col in raw_column_names]
    except Exception as e:
        raise ValueError(f"Failed to read CSV file headers {file_path}: {str(e)}")


def get_table_name_for_schema(base_table_name: str) -> str:
    """Generate table name for Fivetran schema definition.

    Fivetran SDK handles schema separately, so we just return the table name.

    Args:
        base_table_name: Base table name (e.g., "availability", "transactions")

    Returns:
        Table name in uppercase format: TABLE
    """
    return base_table_name.upper()


def get_qualified_table_name(schema_name: str, base_table_name: str) -> str:
    """Generate fully qualified table name for SQL queries and operations.

    Args:
        schema_name: Schema name from configuration
        base_table_name: Base table name (e.g., "availability", "transactions")

    Returns:
        Fully qualified table name in format: SCHEMA.TABLE (uppercase)
    """
    schema_upper = schema_name.upper()
    table_upper = base_table_name.upper()
    return f"{schema_upper}.{table_upper}"


def escape_sql_identifier(identifier: str) -> str:
    """Escape SQL identifier for Snowflake.

    Handles both simple identifiers and qualified names (SCHEMA.TABLE).

    Args:
        identifier: SQL identifier to escape (can be simple or SCHEMA.TABLE format)

    Returns:
        Escaped identifier (e.g., "TABLE" or "SCHEMA"."TABLE")
    """
    # Snowflake uses double quotes for identifiers
    # If identifier contains a dot, treat as qualified name and escape each part
    if "." in identifier:
        parts = identifier.split(".", 1)  # Split only on first dot
        escaped_parts = [f'"{part.replace('"', '""')}"' for part in parts]
        return ".".join(escaped_parts)
    else:
        return f'"{identifier.replace('"', '""')}"'


def escape_sql_value(value: str) -> str:
    """Escape SQL string value for Snowflake.

    Args:
        value: String value to escape

    Returns:
        Escaped value
    """
    if value is None:
        return "NULL"
    # Escape single quotes and wrap in single quotes
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def process_incremental_full_mix(
    configuration: dict, state: dict, conn: snowflake.connector.SnowflakeConnection
) -> None:
    """Process IncrementalFullMix sync mode with Snowflake.

    Logic:
    1. Delete from [target] where userid in [file] (skipped on initial sync)
    2. Insert into [target] select * from [file] in batches with checkpointing

    Args:
        configuration: Configuration dictionary
        state: State dictionary to determine if this is an initial sync
        conn: Snowflake connection

    Raises:
        RuntimeError: if processing fails
    """
    log.info("Processing IncrementalFullMix sync mode")

    # Check if demo mode is enabled
    demo_mode = configuration.get("demo", "True")

    if demo_mode == "True":
        log.info("Demo mode enabled - using hardcoded demo data")
        # Use demo data
        all_records = DEMO_INCREMENTAL_FULL_MIX_DATA
        column_names = list(all_records[0].keys()) if all_records else []
        log.fine(f"Using demo data with columns: {column_names}")
    else:
        connector_dir = get_connector_directory()
        file_path = configuration.get("file_path", "input-files")
        input_dir = connector_dir / file_path / "IncrementalFullMix"

        csv_file = input_dir / "Availability.csv"
        if not csv_file.exists():
            raise RuntimeError(
                f"Availability.csv file not found in {input_dir}. Please ensure the file is present."
            )

        log.fine(f"Reading file: {csv_file}")

        # Read CSV file header
        column_names = read_csv_columns(csv_file)
        log.fine(f"Detected columns from file: {column_names}")

        if "UserID" not in column_names:
            raise RuntimeError("UserID column not found in Availability.csv")

        # Read all records from CSV
        all_records = []
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            reader.fieldnames = (
                [col.strip() for col in reader.fieldnames] if reader.fieldnames else []
            )
            for row in reader:
                if any(row.values()):
                    cleaned_row = {col.strip(): value for col, value in row.items()}
                    all_records.append(cleaned_row)

    # Stream read CSV twice: first to collect UserIDs, then to insert in batches
    is_initial = is_initial_sync(state)
    user_ids: Set[str] = set()

    try:
        # First pass: collect all UserIDs (memory efficient - only storing UserIDs)
        for record in all_records:
            user_id = record.get("UserID", "")
            if user_id:
                user_ids.add(user_id)

        # Get table names: qualified for SQL, simple for op.upsert()
        table_name_simple = get_table_name_for_schema("availability")
        table_name_qualified = get_qualified_table_name(
            configuration["snowflake_schema"], "availability"
        )

        # Delete all UserIDs from file (if not initial sync) - do this before inserts
        if not is_initial and user_ids:
            cursor = conn.cursor()
            try:
                # Delete in chunks to avoid SQL statement size limits
                delete_chunk_size = 10000
                user_ids_list = list(user_ids)
                total_deleted = 0

                for i in range(0, len(user_ids_list), delete_chunk_size):
                    chunk = user_ids_list[i : i + delete_chunk_size]
                    delete_user_ids_escaped = [escape_sql_value(uid) for uid in chunk]
                    # Use fully qualified name: SCHEMA.TABLE
                    delete_sql = (
                        f'DELETE FROM {escape_sql_identifier(table_name_qualified)} WHERE {escape_sql_identifier("USERID")} IN '
                        f"({', '.join(delete_user_ids_escaped)})"
                    )
                    cursor.execute(delete_sql)
                    total_deleted += len(chunk)

                conn.commit()
                log.info(f"Deleted records for {total_deleted} UserIDs")
            except Exception as e:
                conn.rollback()
                raise RuntimeError(f"Failed to delete records: {str(e)}")
            finally:
                cursor.close()

        # Second pass: insert records in batches with checkpointing
        batch: List[Dict] = []
        batch_num = 0
        total_records = 0
        last_user_id = None

        for record in all_records:
            user_id = record.get("UserID", "")

            if user_id:
                batch.append(record)
                last_user_id = user_id

                # Process batch when it reaches BATCH_SIZE
                if len(batch) >= BATCH_SIZE:
                    batch_num += 1
                    total_records += len(batch)

                    # Insert batch using Fivetran SDK op.upsert()
                    # op.upsert() expects just the table name (not fully qualified)
                    # Convert record keys to uppercase to match schema definition exactly
                    for record in batch:
                        record_uppercase = {k.upper(): v for k, v in record.items()}
                        op.upsert(table=table_name_simple, data=record_uppercase)
                    log.info(
                        f"Batch {batch_num}: Upserted {len(batch)} records into {table_name_simple}"
                    )

                    # Checkpoint after each batch
                    checkpoint_state = {
                        "last_sync_time": datetime.utcnow().isoformat() + "Z",
                        "sync_mode": "incremental_full_mix",
                        "records_processed": total_records,
                        "last_processed_user_id": last_user_id,
                    }
                    op.checkpoint(checkpoint_state)
                    log.info(
                        f"Checkpointed after batch {batch_num}: {total_records} records processed"
                    )

                    batch = []

        # Process remaining records in final batch
        if batch:
            batch_num += 1
            total_records += len(batch)

            # Insert final batch using Fivetran SDK op.upsert()
            # op.upsert() expects just the table name (not fully qualified)
            # Convert record keys to uppercase to match schema definition exactly
            for record in batch:
                record_uppercase = {k.upper(): v for k, v in record.items()}
                op.upsert(table=table_name_simple, data=record_uppercase)
            log.info(f"Final batch: Upserted {len(batch)} records into {table_name_simple}")

            # Final checkpoint
            checkpoint_state = {
                "last_sync_time": datetime.utcnow().isoformat() + "Z",
                "sync_mode": "incremental_full_mix",
                "records_processed": total_records,
                "last_processed_user_id": last_user_id,
            }
            op.checkpoint(checkpoint_state)
            log.info(f"Final checkpoint: {total_records} records processed")

        log.info(f"Successfully processed {total_records} records for IncrementalFullMix")

    except Exception as e:
        raise RuntimeError(f"Failed to process IncrementalFullMix: {str(e)}")


def process_rolling_incremental(
    configuration: dict, state: dict, conn: snowflake.connector.SnowflakeConnection
) -> None:
    """Process RollingIncremental sync mode with Snowflake.

    Logic:
    1. Select min_timestamp = min(TransactionDate) from file
    2. Delete from [target] where TransactionDate >= min_timestamp (skipped on initial sync)
    3. Insert into [target] select * from [file] in batches with checkpointing

    Args:
        configuration: Configuration dictionary
        state: State dictionary to determine if this is an initial sync
        conn: Snowflake connection

    Raises:
        RuntimeError: if processing fails
    """
    log.info("Processing RollingIncremental sync mode")

    # Check if demo mode is enabled
    demo_mode = configuration.get("demo", "True")

    if demo_mode == "True":
        log.info("Demo mode enabled - using hardcoded demo data")
        # Use demo data
        all_records = DEMO_ROLLING_INCREMENTAL_DATA
        column_names = list(all_records[0].keys()) if all_records else []
        log.fine(f"Using demo data with columns: {column_names}")
    else:
        connector_dir = get_connector_directory()
        file_path = configuration.get("file_path", "input-files")
        input_dir = connector_dir / file_path / "RollingIncremental"

        csv_file = input_dir / "RollingIncremental.csv"
        if not csv_file.exists():
            raise RuntimeError(
                f"RollingIncremental.csv file not found in {input_dir}. Please ensure the file is present."
            )

        log.fine(f"Reading file: {csv_file}")

        # Read CSV file header
        column_names = read_csv_columns(csv_file)
        log.fine(f"Detected columns from file: {column_names}")

        if "UserID" not in column_names:
            raise RuntimeError("UserID column not found in RollingIncremental.csv")
        if "TransactionDate" not in column_names:
            raise RuntimeError("TransactionDate column not found in RollingIncremental.csv")

        # Read all records from CSV
        all_records = []
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            reader.fieldnames = (
                [col.strip() for col in reader.fieldnames] if reader.fieldnames else []
            )
            for row in reader:
                if any(row.values()):
                    cleaned_row = {col.strip(): value for col, value in row.items()}
                    all_records.append(cleaned_row)

    # Stream read CSV to find min TransactionDate and process in batches
    is_initial = is_initial_sync(state)
    transaction_dates: List[str] = []
    batch: List[Dict] = []
    batch_num = 0
    total_records = 0
    last_transaction_date = None

    try:
        # First pass: find min TransactionDate
        for record in all_records:
            trans_date = record.get("TransactionDate", "")
            if trans_date:
                transaction_dates.append(trans_date)

        if not transaction_dates:
            raise RuntimeError("No valid TransactionDate values found in file")

        min_timestamp = min(transaction_dates)
        log.fine(f"Minimum TransactionDate from file: {min_timestamp}")

        # Get table names: qualified for SQL, simple for op.upsert()
        table_name_simple = get_table_name_for_schema("transactions")
        table_name_qualified = get_qualified_table_name(
            configuration["snowflake_schema"], "transactions"
        )

        # Delete records where TransactionDate >= min_timestamp (if not initial sync)
        if not is_initial:
            cursor = conn.cursor()
            try:
                min_timestamp_escaped = escape_sql_value(min_timestamp)
                # Use fully qualified name: SCHEMA.TABLE
                delete_sql = (
                    f"DELETE FROM {escape_sql_identifier(table_name_qualified)} "
                    f"WHERE {escape_sql_identifier('TRANSACTIONDATE')} >= {min_timestamp_escaped}"
                )
                cursor.execute(delete_sql)
                conn.commit()
                log.info(f"Deleted records with TransactionDate >= {min_timestamp}")
            except Exception as e:
                conn.rollback()
                raise RuntimeError(f"Failed to delete records: {str(e)}")
            finally:
                cursor.close()

        # Second pass: insert records in batches
        for record in all_records:
            trans_date = record.get("TransactionDate", "")
            batch.append(record)
            if trans_date:
                last_transaction_date = trans_date

            # Process batch when it reaches BATCH_SIZE
            if len(batch) >= BATCH_SIZE:
                batch_num += 1
                total_records += len(batch)

                # Insert batch using Fivetran SDK op.upsert()
                # op.upsert() expects just the table name (not fully qualified)
                for record in batch:
                    op.upsert(table=table_name_simple, data=record)
                log.info(
                    f"Batch {batch_num}: Upserted {len(batch)} records into {table_name_simple}"
                )

                # Checkpoint after each batch
                checkpoint_state = {
                    "last_sync_time": datetime.utcnow().isoformat() + "Z",
                    "sync_mode": "rolling_incremental",
                    "records_processed": total_records,
                    "last_transaction_date": last_transaction_date,
                }
                op.checkpoint(checkpoint_state)
                log.info(
                    f"Checkpointed after batch {batch_num}: {total_records} records processed"
                )

                batch = []

        # Process remaining records in final batch
        if batch:
            batch_num += 1
            total_records += len(batch)

            # Insert final batch using Fivetran SDK op.upsert()
            # op.upsert() expects just the table name (not fully qualified)
            # Convert record keys to uppercase to match schema definition exactly
            for record in batch:
                record_uppercase = {k.upper(): v for k, v in record.items()}
                op.upsert(table=table_name_simple, data=record_uppercase)
            log.info(f"Final batch: Upserted {len(batch)} records into {table_name_simple}")

            # Final checkpoint
            checkpoint_state = {
                "last_sync_time": datetime.utcnow().isoformat() + "Z",
                "sync_mode": "rolling_incremental",
                "records_processed": total_records,
                "last_transaction_date": last_transaction_date,
            }
            op.checkpoint(checkpoint_state)
            log.info(f"Final checkpoint: {total_records} records processed")

        log.info(f"Successfully processed {total_records} records for RollingIncremental")

    except Exception as e:
        raise RuntimeError(f"Failed to process RollingIncremental: {str(e)}")


def schema(configuration: dict) -> List[Dict]:
    """Define the schema function which configures the schema your connector delivers.

    This function reads the CSV files or demo data to extract column names and explicitly
    defines all columns in uppercase. Fivetran will not infer column names - all columns
    are explicitly defined from the input data.

    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Returns:
        List of table schema definitions with columns explicitly defined from input data
    """
    # Check if demo mode is enabled
    demo_mode = configuration.get("demo", "True")

    schema_definitions = []

    if demo_mode == "True":
        # Use demo data columns for schema - explicitly define all columns
        if DEMO_INCREMENTAL_FULL_MIX_DATA:
            # Get all column keys from the first record and convert to uppercase
            availability_columns = list(DEMO_INCREMENTAL_FULL_MIX_DATA[0].keys())
            table_name = get_table_name_for_schema("availability")
            # Explicitly define all columns in uppercase - Fivetran will not infer
            schema_definitions.append(
                {
                    "table": table_name,
                    "columns": {col.upper(): "STRING" for col in availability_columns}
                }
            )
        else:
            raise RuntimeError("DEMO_INCREMENTAL_FULL_MIX_DATA is empty - cannot define schema")

        if DEMO_ROLLING_INCREMENTAL_DATA:
            # Get all column keys from the first record and convert to uppercase
            transactions_columns = list(DEMO_ROLLING_INCREMENTAL_DATA[0].keys())
            table_name = get_table_name_for_schema("transactions")
            # Explicitly define all columns in uppercase - Fivetran will not infer
            schema_definitions.append(
                {
                    "table": table_name,
                    "columns": {col.upper(): "STRING" for col in transactions_columns}
                }
            )
        else:
            raise RuntimeError("DEMO_ROLLING_INCREMENTAL_DATA is empty - cannot define schema")
    else:
        connector_dir = get_connector_directory()
        file_path = configuration.get("file_path", "input-files")

        # Read Availability.csv to get columns for availability table
        availability_file = connector_dir / file_path / "IncrementalFullMix" / "Availability.csv"
        table_name_availability = get_table_name_for_schema("availability")

        if not availability_file.exists():
            raise RuntimeError(
                f"Availability.csv file not found at {availability_file}. "
                "Cannot define schema without input file."
            )

        try:
            # Read column names from CSV file header
            availability_columns = read_csv_columns(availability_file)
            if not availability_columns:
                raise RuntimeError("Availability.csv has no columns defined")

            # Explicitly define all columns in uppercase - Fivetran will not infer
            schema_definitions.append(
                {
                    "table": table_name_availability,
                    "columns": {col.upper(): "STRING" for col in availability_columns}
                }
            )
        except Exception as e:
            raise RuntimeError(f"Failed to read Availability.csv for schema definition: {str(e)}")

        # Read RollingIncremental.csv to get columns for transactions table
        transactions_file = (
            connector_dir / file_path / "RollingIncremental" / "RollingIncremental.csv"
        )
        table_name_transactions = get_table_name_for_schema("transactions")

        if not transactions_file.exists():
            raise RuntimeError(
                f"RollingIncremental.csv file not found at {transactions_file}. "
                "Cannot define schema without input file."
            )

        try:
            # Read column names from CSV file header
            transactions_columns = read_csv_columns(transactions_file)
            if not transactions_columns:
                raise RuntimeError("RollingIncremental.csv has no columns defined")

            # Explicitly define all columns in uppercase - Fivetran will not infer
            schema_definitions.append(
                {
                    "table": table_name_transactions,
                    "columns": {col.upper(): "STRING" for col in transactions_columns}
                }
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to read RollingIncremental.csv for schema definition: {str(e)}"
            )

    return schema_definitions


def update(configuration: dict, state: dict) -> None:
    """Define the update function, which is called by Fivetran during each sync.

    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
               The state dictionary is empty for the first sync or for any full re-sync
    """
    log.info("NYPH Connector v2: Starting sync")

    # Validate the configuration
    validate_configuration(configuration)

    sync_mode = configuration["sync_mode"]
    conn = None

    try:
        # Create Snowflake connection
        conn = create_snowflake_connection(configuration)

        # Process based on sync mode
        if sync_mode == "incremental_full_mix":
            process_incremental_full_mix(configuration, state, conn)
        elif sync_mode == "rolling_incremental":
            process_rolling_incremental(configuration, state, conn)
        else:
            raise ValueError(f"Unknown sync_mode: {sync_mode}")

        log.info(f"NYPH Connector v2: Successfully completed {sync_mode} sync")

    except Exception as e:
        error_msg = f"Failed to sync NYPH data: {str(e)}"
        log.warning(error_msg)
        raise RuntimeError(error_msg)
    finally:
        # Always close Snowflake connection
        if conn:
            try:
                conn.close()
                log.info("Closed Snowflake connection")
            except Exception as e:
                log.warning(f"Error closing Snowflake connection: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This allows your script to be run directly from the command line or IDE for debugging.
# This method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
