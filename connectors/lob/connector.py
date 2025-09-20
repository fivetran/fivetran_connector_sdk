"""
Lob Print & Mail API Connector for Fivetran Connector SDK.
This connector demonstrates how to fetch data from Lob's Print & Mail API and upsert it into destination using requests library.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import base64
import urllib.parse

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For making HTTP requests to the Lob API
import requests

"""
Constants for the Lob API connector.
These values control retry behavior, pagination, and API endpoints.
"""
__MAX_RETRIES = 3  # Maximum number of retry attempts for API requests
__BASE_DELAY = 1  # Base delay in seconds for API request retries
__DEFAULT_LIMIT = 100  # Default number of records to fetch per API request
__BASE_URL = "https://api.lob.com/v1"  # Lob API base URL

# API endpoints configuration
__ENDPOINTS = {
    "addresses": {"url": "/addresses", "table_name": "addresses", "primary_key": ["id"]},
    "postcards": {"url": "/postcards", "table_name": "postcards", "primary_key": ["id"]},
    "self_mailers": {"url": "/self_mailers", "table_name": "self_mailers", "primary_key": ["id"]},
    "letters": {"url": "/letters", "table_name": "letters", "primary_key": ["id"]},
    "checks": {"url": "/checks", "table_name": "checks", "primary_key": ["id"]},
    "snap_packs": {"url": "/snap_packs", "table_name": "snap_packs", "primary_key": ["id"]},
    "booklets": {"url": "/booklets", "table_name": "booklets", "primary_key": ["id"]},
    "bank_accounts": {
        "url": "/bank_accounts",
        "table_name": "bank_accounts",
        "primary_key": ["id"],
    },
    "templates": {"url": "/templates", "table_name": "templates", "primary_key": ["id"]},
}


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    # Validate required configuration parameters
    required_configs = ["api_key"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    # Validate API key format (should be a test or live key)
    api_key = configuration.get("api_key", "")
    if not (api_key.startswith("test_") or api_key.startswith("live_")):
        raise ValueError("API key must start with 'test_' or 'live_'")

    # Validate start_date format if provided
    start_date = configuration.get("start_date")
    if start_date:
        try:
            # Try to parse the date to ensure it's in valid ISO format
            datetime.fromisoformat(start_date.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(
                "start_date must be in ISO format (e.g., '1970-01-01' or '2023-01-01T00:00:00Z')"
            )


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
            "table": "addresses",  # Name of the table in the destination, required.
            "primary_key": ["id"],  # Primary key column(s) for the table, optional.
        },
        {
            "table": "postcards",  # Name of the table in the destination, required.
            "primary_key": ["id"],  # Primary key column(s) for the table, optional.
        },
        {
            "table": "self_mailers",  # Name of the table in the destination, required.
            "primary_key": ["id"],  # Primary key column(s) for the table, optional.
        },
        {
            "table": "letters",  # Name of the table in the destination, required.
            "primary_key": ["id"],  # Primary key column(s) for the table, optional.
        },
        {
            "table": "checks",  # Name of the table in the destination, required.
            "primary_key": ["id"],  # Primary key column(s) for the table, optional.
        },
        {
            "table": "snap_packs",  # Name of the table in the destination, required.
            "primary_key": ["id"],  # Primary key column(s) for the table, optional.
        },
        {
            "table": "booklets",  # Name of the table in the destination, required.
            "primary_key": ["id"],  # Primary key column(s) for the table, optional.
        },
        {
            "table": "bank_accounts",  # Name of the table in the destination, required.
            "primary_key": ["id"],  # Primary key column(s) for the table, optional.
        },
        {
            "table": "templates",  # Name of the table in the destination, required.
            "primary_key": ["id"],  # Primary key column(s) for the table, optional.
        },
    ]


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
    log.warning("Example: API Connector : Lob Print & Mail API")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # Extract configuration parameters as required
    api_key = configuration.get("api_key")
    # Use the default limit constant
    limit = __DEFAULT_LIMIT
    # Use start_date parameter, default to 1970 if not provided
    start_date = configuration.get("start_date", "1970-01-01T00:00:00Z")

    try:
        # Get current datetime for the 'lt' (less than) parameter
        current_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        # Sync each endpoint defined in the configuration
        for endpoint_key, endpoint_config in __ENDPOINTS.items():
            log.info(f"Starting sync for endpoint: {endpoint_key}")

            # Get the last sync time for this specific endpoint from state
            endpoint_state_key = f"{endpoint_key}_last_sync_time"
            last_sync_time = state.get(endpoint_state_key, start_date)

            # Build date range for this sync: from last_sync_time to current_time
            date_range = {"gt": last_sync_time, "lt": current_time}

            # Sync data for this endpoint
            new_sync_time = sync_endpoint_data(
                api_key=api_key,
                endpoint_config=endpoint_config,
                date_range=date_range,
                limit=limit,
            )

            # Update state with the current time as the new sync time
            # This ensures next sync will start from where this one ended
            state[endpoint_state_key] = current_time

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

        log.info("Sync completed successfully")

    except Exception as e:
        # In case of an exception, raise a runtime error
        log.severe(f"Failed to sync data: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


def sync_endpoint_data(
    api_key: str, endpoint_config: Dict[str, Any], date_range: Dict[str, str], limit: int
) -> Optional[str]:
    """
    Sync data for a specific endpoint with pagination and incremental loading support.
    Args:
        api_key: The API key for authentication
        endpoint_config: Configuration for the specific endpoint
        date_range: Dictionary with 'gt' and 'lt' dates for this sync
        limit: Number of records to fetch per request
    Returns:
        The latest date_created value encountered during sync
    """
    table_name = endpoint_config["table_name"]
    endpoint_url = endpoint_config["url"]
    latest_sync_time = None

    log.info(f"Syncing {table_name} from {endpoint_url}")
    log.info(f"Date range: {date_range['gt']} to {date_range['lt']}")

    # Track pagination
    next_url = None
    total_records = 0

    while True:
        # Fetch a page of data
        page_data, next_url = fetch_page_data(
            api_key=api_key,
            endpoint_url=endpoint_url,
            date_range=date_range,
            limit=limit,
            next_url=next_url,
        )

        if not page_data:
            log.info(f"No more data to fetch for {table_name}")
            break

        # Process each record in the page
        for record in page_data:
            # Simple flattening for all records
            flattened_record = flatten_record(record)
            op.upsert(table=table_name, data=flattened_record)

            # Track the latest date_created for logging purposes
            record_date = record.get("date_created")
            if record_date and (latest_sync_time is None or record_date > latest_sync_time):
                latest_sync_time = record_date

            total_records += 1

        log.info(f"Processed {len(page_data)} records for {table_name} (total: {total_records})")

        # Break if no next page
        if not next_url:
            break

    log.info(f"Completed sync for {table_name}: {total_records} total records")
    return latest_sync_time


def fetch_page_data(
    api_key: str,
    endpoint_url: str,
    date_range: Optional[Dict[str, str]],
    limit: int,
    next_url: Optional[str] = None,
) -> tuple:
    """
    Fetch a single page of data from the Lob API with retry logic.
    Args:
        api_key: The API key for authentication
        endpoint_url: The API endpoint to fetch data from
        date_range: Dictionary with 'gt' and 'lt' dates for filtering
        limit: Number of records to fetch per request
        next_url: URL for the next page if paginating
    Returns:
        Tuple of (data_list, next_url)
    """
    # Simple URL construction
    if next_url:
        url = next_url
    else:
        url = f"{__BASE_URL}{endpoint_url}?limit={limit}"

        # Add date filter if provided
        if date_range and date_range.get("gt") and date_range.get("lt"):
            date_filter = f'{{"gt": "{date_range["gt"]}", "lt": "{date_range["lt"]}"}}'
            url += f"&date_created={urllib.parse.quote(date_filter)}"

    # Simple retry logic
    for attempt in range(__MAX_RETRIES):
        try:
            response = make_api_request(api_key, url)

            if response.status_code == 200:
                response_data = response.json()
                data_list = response_data.get("data", [])
                next_page_url = response_data.get("next_url")
                return data_list, next_page_url

            elif response.status_code == 403:
                log.warning(f"Access forbidden to endpoint {endpoint_url}. Skipping...")
                return [], None
            else:
                log.warning(f"Request failed with status {response.status_code}")
                if attempt < __MAX_RETRIES - 1:
                    time.sleep(1)  # Simple 1 second delay
                    continue
                else:
                    raise RuntimeError(f"API returned {response.status_code}: {response.text}")

        except requests.RequestException as e:
            if attempt < __MAX_RETRIES - 1:
                time.sleep(1)
                continue
            else:
                raise RuntimeError(f"Request failed: {str(e)}")

    return [], None


def make_api_request(api_key: str, url: str) -> requests.Response:
    """
    Make an authenticated API request to the Lob API.
    Args:
        api_key: The API key for authentication
        url: The complete URL to make the request to
    Returns:
        Response object from the API call
    """
    # Lob uses HTTP Basic authentication with API key as username and empty password
    auth_header = base64.b64encode(f"{api_key}:".encode()).decode()

    headers = {"Authorization": f"Basic {auth_header}", "Content-Type": "application/json"}

    log.info(f"Making API request to: {url}")
    return requests.get(url, headers=headers, timeout=30)


def flatten_record(
    record: Dict[str, Any], parent_key: str = "", separator: str = "_"
) -> Dict[str, Any]:
    """
    Flatten a nested dictionary by converting nested keys to flat structure.
    This helps with data warehouse storage and querying.
    Args:
        record: The dictionary to flatten
        parent_key: The parent key for nested structures
        separator: The separator to use between nested keys
    Returns:
        Flattened dictionary
    """
    flattened = {}

    for key, value in record.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key

        if isinstance(value, dict):
            # Recursively flatten nested dictionaries
            flattened.update(flatten_record(value, new_key, separator))
        elif isinstance(value, list):
            # Simple array handling - just convert to JSON string
            flattened[new_key] = json.dumps(value) if value else None
        else:
            # Handle primitive values
            flattened[new_key] = value

    return flattened


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
