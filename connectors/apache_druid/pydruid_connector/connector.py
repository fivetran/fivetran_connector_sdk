"""
Apache Druid Connector for Fivetran Connector SDK using PyDruid.
This connector demonstrates how to fetch data from Apache Druid using the PyDruid library
and sync it to destination. Supports native Druid queries, incremental sync, and proper error handling.

See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Used for parsing JSON configuration
import json

# Used for replacing invalid characters in table names
import re

# Used for sleep delays between retries and batch requests
import time
import random

# Used for parsing timestamps for safe comparison
from datetime import datetime, timezone, timedelta

# PyDruid imports
try:
    from pydruid.client import PyDruid

    _PYDRUID_AVAILABLE = True
except ImportError:
    PyDruid = None
    _PYDRUID_AVAILABLE = False

# Constants
__BATCH_SIZE = 10000  # Number of records to fetch per request
__SCAN_BATCH_SIZE = 1000  # Number of records per batch for scan operations
__CHECKPOINT_INTERVAL = 1000  # Number of records between mid-sync checkpoints
__PAGINATION_DELAY_SECONDS = 1  # Delay in seconds between paginated batch requests
__MAX_RETRIES = 3  # Maximum number of retry attempts
__INITIAL_RETRY_DELAY = 1  # Initial retry delay in seconds
__MAX_RETRY_DELAY = 60  # Maximum retry delay in seconds


def retry_with_exponential_backoff(max_retries=__MAX_RETRIES):
    """
    Decorator that implements retry logic with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries + 1):  # +1 for initial attempt
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    if attempt == max_retries:
                        # Final attempt failed, re-raise the exception
                        raise

                    # Calculate exponential backoff delay with jitter
                    delay = min(__INITIAL_RETRY_DELAY * (2**attempt), __MAX_RETRY_DELAY)
                    # Add random jitter (±25% of delay)
                    jitter = delay * 0.25 * (2 * random.random() - 1)
                    actual_delay = max(0.1, delay + jitter)

                    log.warning(
                        f"Request failed (attempt {attempt + 1}/{max_retries + 1}), "
                        f"retrying in {actual_delay:.1f}s: {str(e)}"
                    )
                    time.sleep(actual_delay)

            # This should never be reached due to the raise above
            raise last_exception

        return wrapper

    return decorator


class DruidPyDruidClient:
    """
    Apache Druid client using pydruid package for native Druid queries.

    This client provides direct access to Druid's native query APIs including:
    - GroupBy queries for aggregation with grouping
    - Timeseries queries for time-based aggregation
    - TopN queries for finding top values
    - Scan queries for raw data extraction
    - Search queries for dimension value discovery
    """

    def __init__(self, configuration: dict):
        """
        Initialize the PyDruid client.

        Args:
            configuration: Dictionary containing connection parameters
                - host: Druid broker hostname
                - port: Druid broker port
                - protocol: http or https (optional, defaults to https)
                - username: Basic auth username (optional)
                - password: Basic auth password (optional)
        """
        if not _PYDRUID_AVAILABLE:
            raise ImportError(
                "pydruid package is required. Please install it with: pip install pydruid==0.6.5"
            )

        self.host = configuration.get("host")
        self.port = int(configuration.get("port", 8080))
        self.protocol = configuration.get(
            "protocol", "https"
        )  # Default to http for backward compatibility
        self.username = configuration.get("username")
        self.password = configuration.get("password")

        # Validate required parameters
        if not self.host:
            raise ValueError("host is required in configuration")

        # Build endpoint URL using specified or default protocol
        self.endpoint = f"{self.protocol}://{self.host}:{self.port}"

        # Initialize pydruid client
        self._initialize_client()

        log.info(f"Initialized PyDruid client for {self.endpoint}")

    def _initialize_client(self):
        """Initialize the pydruid Client instance."""
        try:
            # Create pydruid client
            self.client = PyDruid(self.endpoint, "druid/v2/")

            # Handle authentication if provided
            if self.username and self.password:
                # PyDruid supports basic authentication via set_basic_auth_credentials
                self.client.set_basic_auth_credentials(self.username, self.password)
                log.info(
                    f"PyDruid client initialized with basic authentication for user: {self.username}"
                )
            else:
                log.info("PyDruid client initialized without authentication")

        except Exception as e:
            raise RuntimeError(f"Failed to initialize PyDruid client: {str(e)}")

    def _execute_query_with_retry(self, query_func, *args, **kwargs):
        """Execute a PyDruid query with retry logic."""

        @retry_with_exponential_backoff()
        def execute():
            return query_func(*args, **kwargs)

        return execute()

    def scan_datasource(
        self,
        datasource: str,
        intervals: list[str],
        columns: list[str] | None = None,
        filters=None,
        limit: int = 10000,
        batch_size: int = 1000,  # __SCAN_BATCH_SIZE
    ):
        """
        Scan a datasource for raw data using Druid's scan query.

        Args:
            datasource: Name of the datasource to scan
            intervals: List of ISO intervals (e.g., ["2023-01-01/2023-02-01"])
            columns: Specific columns to retrieve (optional, gets all if None)
            filters: List of filter dictionaries (optional)
            limit: Maximum number of records to retrieve
            batch_size: Number of records per batch request

        Yields:
            Individual records as dictionaries
        """
        try:
            offset = 0
            total_fetched = 0

            while total_fetched < limit:
                current_batch_size = min(batch_size, limit - total_fetched)

                # Build scan query
                scan_params = {
                    "datasource": datasource,
                    "intervals": intervals,
                    "columns": columns or [],
                    "limit": current_batch_size,
                    "offset": offset,
                }

                # Add filter if provided (note: singular 'filter', not 'filters')
                if filters:
                    scan_params["filter"] = filters

                query = self._execute_query_with_retry(self.client.scan, **scan_params)

                if not hasattr(query, "result") or not query.result:
                    log.info(f"No more data to scan from {datasource} at offset {offset}")
                    break

                batch_count = 0
                batch_data = query.result

                # Handle PyDruid scan result structure: list of segments, each with events
                events = []
                if isinstance(batch_data, list):
                    for segment in batch_data:
                        if isinstance(segment, dict) and "events" in segment:
                            events.extend(segment["events"])
                        elif isinstance(segment, dict):
                            # Handle case where segment itself is an event
                            events.append(segment)
                else:
                    log.warning(f"Unexpected result format: {type(batch_data)}")
                    events = []

                for event in events:
                    if isinstance(event, dict):
                        yield event
                        batch_count += 1
                        total_fetched += 1

                        if total_fetched >= limit:
                            break
                    else:
                        log.warning(f"Unexpected event format: {type(event)} - {event}")

                log.info(
                    f"Scanned {batch_count} records from {datasource} (total: {total_fetched})"
                )

                # If we got fewer records than requested, we've reached the end
                if batch_count < current_batch_size:
                    break

                offset += batch_count

                # Small delay between batch requests
                time.sleep(1)  # __PAGINATION_DELAY_SECONDS

        except Exception as e:
            log.severe(f"Failed to scan datasource {datasource}: {str(e)}")
            raise RuntimeError(f"Failed to scan datasource {datasource}: {str(e)}")

    def incremental_scan(
        self,
        datasource: str,
        timestamp_column: str = "__time",
        since: str | None = None,
        batch_size: int = 10000,  # __BATCH_SIZE
    ):
        """
        Perform incremental scan of a datasource based on timestamp.

        Args:
            datasource: Name of the datasource
            timestamp_column: Name of the timestamp column
            since: ISO timestamp to start from (None for full scan)
            batch_size: Number of records per batch

        Yields:
            Individual records as dictionaries
        """
        try:
            # Determine the interval based on since parameter
            if since:
                # For incremental sync, we need to exclude records AT the since timestamp
                # Add 1 millisecond to ensure we only get records AFTER the last processed record
                try:
                    since_dt = _parse_iso_timestamp(since)
                    # Add 1 millisecond safely using timedelta
                    since_dt = since_dt + timedelta(milliseconds=1)
                    start_time = since_dt.isoformat().replace("+00:00", "Z")
                    log.info(f"Starting incremental scan from {start_time} (excluding {since})")
                except Exception as e:
                    log.warning(f"Could not parse since timestamp {since}, using as-is: {e}")
                    start_time = since
            else:
                start_time = "1970-01-01T00:00:00.000Z"
                log.info("Starting full scan from epoch")

            # Use a wide end time to capture all data
            end_time = "2100-01-01T00:00:00.000Z"
            intervals = [f"{start_time}/{end_time}"]

            # Add filter for timestamp if doing incremental sync
            filters = None
            if since:
                # PyDruid filter for timestamp comparison
                # Note: For simplicity, we'll use intervals to filter by time
                # and rely on the intervals parameter instead of complex filters
                pass

            # Use scan query to get raw data
            yield from self.scan_datasource(
                datasource=datasource, intervals=intervals, filters=filters, batch_size=batch_size
            )

        except Exception as e:
            log.severe(f"Failed to perform incremental scan: {str(e)}")
            raise RuntimeError(f"Failed to perform incremental scan: {str(e)}")

    def close(self):
        """Close the connection (cleanup if needed)."""
        # pydruid doesn't require explicit connection closing
        log.info("PyDruid client closed")


def _parse_iso_timestamp(ts: str) -> datetime:
    """
    Parse an ISO timestamp string, handling Z suffix properly.

    Args:
        ts: ISO timestamp string (may have Z suffix)

    Returns:
        datetime object with timezone info
    """
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def to_iso_timestamp(ts: str | datetime) -> str:
    """
    Normalize a timestamp to ISO 8601 format for use with Druid's TIME_PARSE() function.

    Accepts a datetime object or a string in either ISO 8601 format (e.g.
    '2015-09-12T23:59:59.200Z') or the legacy Druid SQL format ('yyyy-MM-dd HH:mm:ss')
    that may be present in older state values.
    Always returns a full ISO 8601 string with UTC offset (e.g. '2015-09-12T23:59:59.200000+00:00').
    """
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.isoformat()
    # Handle legacy 'yyyy-MM-dd HH:mm:ss' values that may exist in state from older syncs
    if "T" not in ts:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).isoformat()
    # Already ISO 8601 — normalise Z suffix for Python < 3.11 then round-trip to ensure consistency
    return _parse_iso_timestamp(ts).isoformat()


def sanitize_table_name(name: str) -> str:
    """
    Sanitize a Druid datasource name for use as a destination table name.

    Replaces any character that is not a letter, digit, or underscore with an underscore,
    ensuring the result is a valid identifier in all destination systems.

    Args:
        name: The raw datasource name from Druid

    Returns:
        A sanitized table name safe for use as a destination table identifier
    """
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


def _validate_datasource_name(datasource: str) -> None:
    """
    Validate a single datasource name to prevent injection issues.

    Args:
        datasource: The datasource name to validate

    Raises:
        ValueError: If the datasource name is invalid
    """
    if not datasource or not isinstance(datasource, str) or not datasource.strip():
        raise ValueError("Datasource name must be a non-empty string")

    # Check for dangerous characters that could cause injection issues
    dangerous_chars = ['"', "'", "\n", "\r", ";"]
    for char in dangerous_chars:
        if char in datasource:
            raise ValueError(f"Datasource name contains invalid character: {repr(char)}")


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

    # Validate port is a valid port number
    port = configuration.get("port")
    try:
        port_int = int(port)
    except (TypeError, ValueError):
        raise ValueError("port must be a valid port number between 1 and 65535") from None
    if port_int < 1 or port_int > 65535:
        raise ValueError("port must be between 1 and 65535")

    # Validate datasources
    datasources = configuration.get("datasources")
    if not datasources or not isinstance(datasources, str) or not datasources.strip():
        raise ValueError("datasources must be a non-empty string (comma-separated list)")

    # Validate each individual datasource name
    datasource_list = [ds.strip() for ds in datasources.split(",")]
    if not datasource_list:
        raise ValueError("datasources list cannot be empty")

    for datasource in datasource_list:
        try:
            _validate_datasource_name(datasource)
        except ValueError as e:
            raise ValueError(f"Invalid datasource in list: {str(e)}") from e

    # Check for duplicate datasource names
    if len(datasource_list) != len(set(datasource_list)):
        raise ValueError("Duplicate datasource names are not allowed")

    # Validate optional protocol field if provided
    if "protocol" in configuration:
        protocol = configuration.get("protocol")
        if protocol and protocol not in ["http", "https"]:
            raise ValueError("protocol must be either 'http' or 'https'")

    # Validate optional auth fields if provided
    if "username" in configuration:
        username = configuration.get("username")
        if username and (not isinstance(username, str) or not username.strip()):
            raise ValueError("username must be a non-empty string if provided")

    if "password" in configuration:
        password = configuration.get("password")
        if password and (not isinstance(password, str) or not password.strip()):
            raise ValueError("password must be a non-empty string if provided")

    log.info("Configuration validated successfully")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
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
            schema_list.append({"table": sanitize_table_name(datasource)})

    return schema_list


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: Connectors : Apache Druid")
    log.info("Starting Apache Druid Connector sync")

    # Validate configuration first
    validate_configuration(configuration)

    # Extract configuration parameters
    datasources = configuration.get("datasources")
    batch_size = __BATCH_SIZE

    log.info(f"Connecting to Druid using PyDruid client")

    # Initialize PyDruid client
    druid_client = DruidPyDruidClient(configuration)
    log.info(f"PyDruid client initialized successfully")

    try:
        record_count = 0

        # Start with a copy of existing state so all previous datasource keys are preserved
        current_state = dict(state)

        # Process each datasource
        for datasource in datasources.split(","):
            datasource = datasource.strip()
            if not datasource:
                continue

            log.info(f"Processing datasource: {datasource}")
            table_name = sanitize_table_name(datasource)

            # Get datasource-specific last sync time. None triggers a full fetch for new datasources.
            # Normalize to ISO 8601 to handle any legacy 'yyyy-MM-dd HH:mm:ss' values in state.
            raw_last_sync = current_state.get(f"last_sync_{datasource}")
            datasource_last_sync = to_iso_timestamp(raw_last_sync) if raw_last_sync else None

            # Track max __time from actual data processed as datetime for safe comparison
            max_time_processed: datetime | None = None
            datasource_record_count = 0

            # Fetch and upsert data using PyDruid client
            for record in druid_client.incremental_scan(
                datasource=datasource,
                timestamp_column="__time",
                since=datasource_last_sync,
                batch_size=batch_size,
            ):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table=table_name, data=record)
                record_count += 1
                datasource_record_count += 1

                # Track the maximum __time from actual data using datetime for safe comparison.
                # Handle various timestamp formats that PyDruid might return
                record_time = record.get("__time") or record.get("timestamp")
                if record_time:
                    try:
                        # Handle different timestamp formats
                        if isinstance(record_time, (int, float)):
                            # Unix timestamp in milliseconds
                            record_dt = datetime.fromtimestamp(record_time / 1000, tz=timezone.utc)
                        elif isinstance(record_time, str):
                            # ISO string format
                            record_dt = _parse_iso_timestamp(str(record_time))
                        else:
                            # Already a datetime object
                            record_dt = record_time

                        if record_dt.tzinfo is None:
                            record_dt = record_dt.replace(tzinfo=timezone.utc)
                        if max_time_processed is None or record_dt > max_time_processed:
                            max_time_processed = record_dt

                        # Debug log for first few records
                        if datasource_record_count <= 3:
                            log.info(
                                f"Record {datasource_record_count}: __time={record_time} -> {record_dt}"
                            )

                    except (ValueError, AttributeError, TypeError) as e:
                        log.warning(
                            f"Could not parse __time value '{record_time}' (type: {type(record_time)}): {e}"
                        )

                # Checkpoint periodically for large datasets
                if record_count % __CHECKPOINT_INTERVAL == 0:
                    log.info(f"Processed {record_count} records, checkpointing...")
                    # Save the progress by checkpointing the state. This is important for ensuring that
                    # the sync process can resume from the correct position in case of next sync or interruptions.
                    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran
                    # it is safe to write to destination.
                    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
                    current_state[f"last_sync_{datasource}"] = (
                        to_iso_timestamp(max_time_processed)
                        if max_time_processed
                        else datasource_last_sync
                    )
                    op.checkpoint(state=current_state)

            log.info(
                f"Completed processing datasource: {datasource}. "
                f"Processed {datasource_record_count} records. "
                f"Max __time: {max_time_processed or 'N/A'}"
            )
            current_state[f"last_sync_{datasource}"] = (
                to_iso_timestamp(max_time_processed)
                if max_time_processed
                else datasource_last_sync
            )

            # Save the progress by checkpointing the state. This is important for ensuring that
            # the sync process can resume from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran
            # it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state=current_state)

        log.info(f"Sync completed successfully. Total records processed: {record_count}")

    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Failed to sync Apache Druid data: {str(e)}")
    finally:
        # Clean up the PyDruid client connection
        druid_client.close()


# Create connector instance
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
    # Load configuration from configuration.json
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
