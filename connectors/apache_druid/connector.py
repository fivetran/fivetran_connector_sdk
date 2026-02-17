"""Apache Druid Connector for Fivetran Connector SDK.
This connector demonstrates how to fetch data from Apache Druid and sync it to destination.
Supports SQL queries, incremental sync, and proper error handling.

See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Import required libraries
import requests
import json
import time
from datetime import datetime, timezone

# Constants for API configuration
__REQUEST_TIMEOUT = 30  # Timeout for API requests in seconds
__RATE_LIMIT_DELAY = 2  # Base for exponential backoff calculation (2^attempt)
__MAX_RETRIES = 3  # Maximum number of retries for failed requests
__BATCH_SIZE = 1000  # Number of records to fetch per request


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: Dictionary containing connection details
    Raises:
        ValueError: If required configuration is missing or invalid
    """
    required_configs = ["host", "port", "datasources"]

    # Check for required fields
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration: {key}")

    # Validate host
    host = configuration.get("host")
    if not host or not isinstance(host, str) or not host.strip():
        raise ValueError("host must be a non-empty string")

    # Validate port
    port = configuration.get("port")
    if not isinstance(port, (int, str)):
        raise ValueError("port must be a number or string")
    try:
        port_num = int(port)
        if port_num < 1 or port_num > 65535:
            raise ValueError("port must be between 1 and 65535")
    except (ValueError, TypeError):
        raise ValueError("port must be a valid port number")

    # Validate datasources
    datasources = configuration.get("datasources")
    if not datasources or not isinstance(datasources, str) or not datasources.strip():
        raise ValueError("datasources must be a non-empty string (comma-separated list)")

    # Validate optional auth fields if provided
    if "username" in configuration:
        username = configuration.get("username")
        if username and (not isinstance(username, str) or not username.strip()):
            raise ValueError("username must be a non-empty string if provided")

    log.info("Configuration validated successfully")


def make_druid_request(url: str, headers: dict, data: dict = None):
    """
    Make a request to Druid API with proper error handling and retry logic.
    Args:
        url: The API endpoint URL
        headers: Request headers
        data: Request body (for POST requests)
    Returns:
        Response object from requests library
    Raises:
        RuntimeError: If the request fails after all retries
    """
    for attempt in range(__MAX_RETRIES):
        try:
            if data:
                response = requests.post(url, headers=headers, json=data, timeout=__REQUEST_TIMEOUT)
            else:
                response = requests.get(url, headers=headers, timeout=__REQUEST_TIMEOUT)

            # Handle HTTP errors
            if response.status_code >= 400:
                if response.status_code >= 500:
                    # Retry on 5xx server errors
                    if attempt == __MAX_RETRIES - 1:
                        raise RuntimeError(
                            f"Druid server error after {__MAX_RETRIES} attempts. "
                            f"Status: {response.status_code} - {response.text}"
                        )
                    log.warning(
                        f"Server error {response.status_code}, retrying in {2 ** (attempt + 1)} seconds"
                    )
                    time.sleep(__RATE_LIMIT_DELAY ** (attempt + 1))
                    continue
                else:
                    # 4xx errors - fail fast
                    raise RuntimeError(
                        f"Druid client error {response.status_code}: {response.text}"
                    )

            return response

        except requests.exceptions.Timeout:
            if attempt == __MAX_RETRIES - 1:
                raise RuntimeError(
                    f"Request timeout after {__MAX_RETRIES} attempts to {url}"
                )
            log.warning(f"Request timeout, retrying (attempt {attempt + 1}/{__MAX_RETRIES})")
            time.sleep(__RATE_LIMIT_DELAY ** (attempt + 1))

        except requests.exceptions.RequestException as e:
            if attempt == __MAX_RETRIES - 1:
                raise RuntimeError(
                    f"Failed to make request to url {url} after {__MAX_RETRIES} attempts: {str(e)}"
                )
            log.warning(f"Request attempt {attempt + 1} failed: {str(e)}")
            # Exponential backoff: 2^1=2s, 2^2=4s, 2^3=8s
            time.sleep(__RATE_LIMIT_DELAY ** (attempt + 1))

    raise RuntimeError(f"Failed to make request after {__MAX_RETRIES} attempts")


def execute_sql_query(base_url: str, headers: dict, query: str):
    """
    Execute a SQL query against Druid and return results.
    Args:
        base_url: Base URL for Druid API
        headers: Request headers including authentication
        query: SQL query string to execute
    Returns:
        List of result rows
    """
    url = f"{base_url}/druid/v2/sql"
    data = {"query": query}

    log.info(f"Executing SQL query: {query[:100]}...")
    response = make_druid_request(url, headers, data)

    if response and response.status_code == 200:
        results = response.json()
        log.info(f"Query returned {len(results)} rows")
        return results
    else:
        log.warning("Query returned empty result")
        return []


def get_datasource_columns(base_url: str, headers: dict, datasource: str):
    """
    Get column information for a datasource using INFORMATION_SCHEMA.
    Args:
        base_url: Base URL for Druid API
        headers: Request headers
        datasource: Name of the datasource
    Returns:
        List of column information dictionaries
    """
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{datasource}'
    ORDER BY ORDINAL_POSITION
    """

    columns = execute_sql_query(base_url, headers, query)
    log.info(f"Found {len(columns)} columns in datasource {datasource}")
    return columns


def fetch_datasource_data(
    base_url: str,
    headers: dict,
    datasource: str,
    timestamp_column: str = "__time",
    since: str = None,
):
    """
    Fetch data from a Druid datasource with optional incremental sync.
    Args:
        base_url: Base URL for Druid API
        headers: Request headers
        datasource: Name of the datasource to query
        timestamp_column: Name of the timestamp column for incremental sync
        since: ISO timestamp to fetch data after (for incremental sync)
    Yields:
        Dictionary records from the datasource
    """
    # Build SQL query with optional time filter
    if since:
        query = f"""
        SELECT *
        FROM "{datasource}"
        WHERE "{timestamp_column}" > TIMESTAMP '{since}'
        ORDER BY "{timestamp_column}"
        LIMIT {__BATCH_SIZE}
        """
        log.info(f"Fetching incremental data from {datasource} since {since}")
    else:
        query = f"""
        SELECT *
        FROM "{datasource}"
        ORDER BY "{timestamp_column}"
        LIMIT {__BATCH_SIZE}
        """
        log.info(f"Fetching full data from {datasource}")

    offset = 0
    total_records = 0

    while True:
        # Add pagination offset
        paginated_query = f"{query.rstrip()} OFFSET {offset}"

        results = execute_sql_query(base_url, headers, paginated_query)

        if not results or len(results) == 0:
            log.info(f"No more data to fetch from {datasource}")
            break

        for record in results:
            # Convert record to dict if needed
            if isinstance(record, list):
                # If results come as array, we need column names
                # For simplicity, assume dict format or handle accordingly
                yield record
            else:
                yield record

        total_records += len(results)
        log.info(f"Fetched {total_records} records so far from {datasource}")

        # Check if we got fewer results than batch size (last page)
        if len(results) < __BATCH_SIZE:
            break

        offset += __BATCH_SIZE
        # Small delay between batch requests to avoid overwhelming the server
        time.sleep(1)

    log.info(f"Completed fetching {total_records} total records from {datasource}")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: Dictionary that holds the configuration settings for the connector
    """
    # Validate configuration
    validate_configuration(configuration)

    # Get datasources from configuration
    datasources = configuration.get("datasources", "").split(",")

    # Define schema for each datasource as a separate table
    schema_list = []
    for datasource in datasources:
        datasource = datasource.strip()
        if datasource:
            schema_list.append(
                {
                    "table": datasource.replace("-", "_"),  # Sanitize table name
                    "primary_key": ["__time"],  # Druid typically uses __time as primary key
                }
            )

    return schema_list


def update(configuration: dict, state: dict):
    """
    Define the update function, which is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: Dictionary containing connection details
        state: Dictionary containing state information from previous runs
    """
    log.warning("Example: Connectors : Apache Druid")
    log.info("Starting Apache Druid Connector sync")

    # Validate configuration
    validate_configuration(configuration)

    # Extract configuration parameters
    host = configuration.get("host")
    port = configuration.get("port")
    datasources = configuration.get("datasources")
    username = configuration.get("username")
    password = configuration.get("password")
    use_https = configuration.get("use_https", "false").lower() == "true"

    # Build base URL
    protocol = "https" if use_https else "http"
    base_url = f"{protocol}://{host}:{port}"
    log.info(f"Connecting to Druid at {base_url}")

    # Prepare headers
    headers = {"Content-Type": "application/json"}

    # Add basic auth if credentials provided
    if username and password:
        import base64

        credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {credentials}"
        log.info("Using basic authentication")

    # Get last sync time from state
    last_sync_time = state.get("last_sync_time")

    try:
        # Track current sync time as fallback
        current_sync_time = datetime.now(timezone.utc).isoformat()
        record_count = 0

        # Track max __time per datasource for accurate incremental sync
        datasource_max_times = {}

        # Process each datasource
        for datasource in datasources.split(","):
            datasource = datasource.strip()
            if not datasource:
                continue

            log.info(f"Processing datasource: {datasource}")
            table_name = datasource.replace("-", "_")  # Sanitize table name

            # Get datasource-specific last sync time
            datasource_last_sync = state.get(f"last_sync_{datasource}", last_sync_time)

            # Track max __time from actual data processed
            max_time_processed = None
            datasource_record_count = 0

            # Fetch and upsert data
            for record in fetch_datasource_data(
                base_url, headers, datasource, "__time", datasource_last_sync
            ):
                # The 'upsert' operation is used to insert or update data in the destination table
                op.upsert(table=table_name, data=record)
                record_count += 1
                datasource_record_count += 1

                # Track the maximum __time from actual data
                record_time = record.get("__time")
                if record_time:
                    # Convert to ISO string if needed
                    if isinstance(record_time, str):
                        record_time_str = record_time
                    else:
                        record_time_str = record_time.isoformat() if hasattr(record_time, 'isoformat') else str(record_time)

                    if not max_time_processed or record_time_str > max_time_processed:
                        max_time_processed = record_time_str

                # Checkpoint periodically for large datasets
                if record_count % 1000 == 0:
                    log.info(f"Processed {record_count} records, checkpointing...")
                    # Use max processed time if available, otherwise current time
                    checkpoint_time = max_time_processed or current_sync_time
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
                    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                    op.checkpoint(
                        state={
                            "last_sync_time": checkpoint_time,
                            f"last_sync_{datasource}": checkpoint_time,
                        }
                    )

            # Store the max time for this datasource
            datasource_max_times[datasource] = max_time_processed or datasource_last_sync or current_sync_time

            log.info(
                f"Completed processing datasource: {datasource}. "
                f"Processed {datasource_record_count} records. "
                f"Max __time: {max_time_processed or 'N/A'}"
            )

        # Final state update - use actual max times from data
        final_state = {}

        # Set global last_sync_time to the latest of all datasources
        if datasource_max_times:
            final_state["last_sync_time"] = max(datasource_max_times.values())
        else:
            final_state["last_sync_time"] = current_sync_time

        # Add per-datasource sync times (actual max times from data)
        for datasource, max_time in datasource_max_times.items():
            final_state[f"last_sync_{datasource}"] = max_time

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(final_state)

        log.info(f"Sync completed successfully. Total records processed: {record_count}")

    except Exception as e:
        # In case of an exception, raise a runtime error
        raise RuntimeError(f"Failed to sync Apache Druid data: {str(e)}")


# Create connector instance
connector = Connector(update=update, schema=schema)

# Test the connector locally
if __name__ == "__main__":
    # Load configuration from configuration.json
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
