"""QuestDB connector for Fivetran - syncs high-performance time-series data from QuestDB.
This connector demonstrates how to fetch IoT sensor data, financial market data, and industrial telemetry from QuestDB and sync it to Fivetran using the Fivetran Connector SDK.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For making HTTP requests to QuestDB REST API
import requests

# For URL encoding SQL queries
from urllib.parse import quote

# For handling time operations and timestamps
from datetime import datetime, timezone

# Batch size for pagination
__BATCH_SIZE = 1000

# Checkpoint after processing every 5000 records
__CHECKPOINT_INTERVAL = 5000

# Maximum number of retry attempts for API requests
__MAX_RETRIES = 3

# Base delay in seconds for API request retries
__BASE_DELAY = 1

# Request timeout in seconds
__REQUEST_TIMEOUT_SEC = 60


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["host", "port", "tables"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    tables = configuration.get("tables", "").split(",")
    schemas = []

    for table in tables:
        table_name = table.strip()
        if table_name:
            schemas.append({"table": table_name, "primary_key": ["_fivetran_synced_key"]})

    return schemas


def build_questdb_url(configuration: dict, query: str) -> str:
    """
    Build QuestDB REST API URL with encoded query.
    Args:
        configuration: Configuration dictionary.
        query: SQL query to execute.
    Returns:
        Complete URL for QuestDB REST API request.
    """
    host = configuration.get("host", "localhost")
    port = configuration.get("port", "9000")
    encoded_query = quote(query)
    return f"http://{host}:{port}/exec?query={encoded_query}"


def build_auth_header(configuration: dict) -> dict:
    """
    Build authentication header if credentials are provided.
    Args:
        configuration: Configuration dictionary.
    Returns:
        Dictionary with authentication headers.
    """
    username = configuration.get("username")
    password = configuration.get("password")

    if username and password:
        import base64

        credentials = f"{username}:{password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return {"Authorization": f"Basic {encoded_credentials}"}

    return {}


def execute_questdb_query(configuration: dict, query: str) -> dict:
    """
    Execute a SQL query against QuestDB REST API with retry logic.
    Args:
        configuration: Configuration dictionary.
        query: SQL query to execute.
    Returns:
        JSON response from QuestDB.
    Raises:
        RuntimeError: If the API request fails after all retry attempts.
    """
    url = build_questdb_url(configuration, query)
    headers = build_auth_header(configuration)

    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=__REQUEST_TIMEOUT_SEC)

            if response.status_code == 200:
                return response.json()
            elif response.status_code in [429, 500, 502, 503, 504]:
                if attempt < __MAX_RETRIES - 1:
                    delay = __BASE_DELAY * (2**attempt)
                    log.warning(
                        f"Request failed with status {response.status_code}, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                    )
                    import time

                    time.sleep(delay)
                    continue
                else:
                    log.severe(
                        f"Failed to execute query after {__MAX_RETRIES} attempts. Last status: {response.status_code} - {response.text}"
                    )
                    raise RuntimeError(
                        f"API returned {response.status_code} after {__MAX_RETRIES} attempts: {response.text}"
                    )
            else:
                log.severe(
                    f"Query execution failed with status {response.status_code}: {response.text}"
                )
                raise RuntimeError(
                    f"Query execution failed: {response.status_code} - {response.text}"
                )

        except requests.Timeout:
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY * (2**attempt)
                log.warning(
                    f"Request timeout, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                )
                import time

                time.sleep(delay)
                continue
            else:
                log.severe(f"Request timeout after {__MAX_RETRIES} attempts")
                raise RuntimeError(f"Request timeout after {__MAX_RETRIES} attempts")

        except requests.RequestException as e:
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY * (2**attempt)
                log.warning(
                    f"Request exception: {str(e)}, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                )
                import time

                time.sleep(delay)
                continue
            else:
                log.severe(f"Request failed after {__MAX_RETRIES} attempts: {str(e)}")
                raise RuntimeError(f"Request failed: {str(e)}")


def get_table_columns(configuration: dict, table_name: str) -> list:
    """
    Get column information for a QuestDB table.
    Args:
        configuration: Configuration dictionary.
        table_name: Name of the table.
    Returns:
        List of column dictionaries with name and type.
    """
    query = f"SELECT * FROM {table_name} LIMIT 0"
    try:
        response = execute_questdb_query(configuration, query)
        return response.get("columns", [])
    except Exception as e:
        log.warning(f"Failed to get columns for table {table_name}: {str(e)}")
        return []


def sync_table_data(configuration: dict, table_name: str, state: dict) -> int:
    """
    Sync data from a QuestDB table with pagination support.
    Args:
        configuration: Configuration dictionary.
        table_name: Name of the table to sync.
        state: State dictionary for tracking sync progress.
    Returns:
        Number of records synced.
    """
    batch_size = int(configuration.get("batch_size", __BATCH_SIZE))
    offset = state.get(f"{table_name}_offset", 0)
    total_records = 0
    records_since_checkpoint = 0

    log.info(f"Starting sync for table {table_name}, offset: {offset}")

    while True:
        query = f"SELECT * FROM {table_name} LIMIT {offset},{batch_size}"

        try:
            response = execute_questdb_query(configuration, query)

            columns = response.get("columns", [])
            dataset = response.get("dataset", [])

            if not dataset:
                log.info(f"No more data to sync for table {table_name}")
                break

            column_names = [col["name"] for col in columns]

            for row in dataset:
                record = {}
                for idx, value in enumerate(row):
                    if idx < len(column_names):
                        record[column_names[idx]] = value

                record["_fivetran_synced_key"] = (
                    f"{table_name}_{offset + total_records % batch_size}"
                )

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table=table_name, data=record)

                total_records += 1
                records_since_checkpoint += 1

            offset += len(dataset)

            if records_since_checkpoint >= __CHECKPOINT_INTERVAL:
                new_state = state.copy()
                new_state[f"{table_name}_offset"] = offset
                new_state["last_sync_time"] = datetime.now(timezone.utc).isoformat()
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(new_state)
                records_since_checkpoint = 0
                log.info(f"Checkpointed at offset {offset} for table {table_name}")

            if len(dataset) < batch_size:
                log.info(f"Reached end of table {table_name}")
                break

        except Exception as e:
            log.severe(f"Error syncing table {table_name}: {str(e)}")
            raise RuntimeError(f"Failed to sync table {table_name}: {str(e)}")

    final_state = state.copy()
    final_state[f"{table_name}_offset"] = 0
    final_state["last_sync_time"] = datetime.now(timezone.utc).isoformat()
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(final_state)

    log.info(f"Completed sync for table {table_name}, total records: {total_records}")
    return total_records


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    log.warning("Example: SOURCE_EXAMPLES : QUESTDB_CONNECTOR")

    validate_configuration(configuration=configuration)

    tables = configuration.get("tables", "").split(",")

    for table in tables:
        table_name = table.strip()
        if not table_name:
            continue

        try:
            records_synced = sync_table_data(configuration, table_name, state)
            log.info(f"Successfully synced {records_synced} records from table {table_name}")
        except Exception as e:
            log.severe(f"Failed to sync table {table_name}: {str(e)}")
            raise RuntimeError(f"Failed to sync table {table_name}: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
