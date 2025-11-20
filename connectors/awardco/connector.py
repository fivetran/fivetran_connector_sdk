"""Awardco Connector Example
This connector fetches user data from the AwardCo API and upserts it into the destination using the Fivetran Connector SDK.
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

# For making HTTP requests to Award Co API
import requests

# For implementing delays in retry logic and rate limiting
import time

__PAGE_SIZE = 100
MAX_RETRIES = 3
MAX_RETRY_INTERVAL = 60  # seconds


def make_request_with_retry(url: str, headers: dict, params: dict) -> requests.Response:
    """
    Make an HTTP request with exponential backoff retry logic.
    Args:
        url: The URL to make the request to
        headers: Headers to include in the request
        params: Query parameters for the request
    Returns:
        Response: The successful response
    Raises:
        requests.exceptions.RequestException: If all retries fail
    """
    retry_count = 0
    last_exception = None

    while retry_count <= MAX_RETRIES:
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            retry_count += 1
            last_exception = e

            if retry_count > MAX_RETRIES:
                log.error(f"Max retries ({MAX_RETRIES}) exceeded. Last error: {str(e)}")
                raise last_exception

            # Calculate backoff time: 2^retry_count, but cap at max_interval
            backoff = min(2**retry_count, MAX_RETRY_INTERVAL)
            log.warning(
                f"Request failed: {str(e)}. Retrying in {backoff} seconds... (Attempt {retry_count} of {MAX_RETRIES})"
            )
            time.sleep(backoff)


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["api_key", "base_url"]
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
    return [
        {
            "table": "user",
            "primary_key": ["employeeId"],
        },
    ]


def fetch_users(base_url: str, api_key: str, page: int = 1, per_page: int = 100) -> list:
    """
    Fetch a page of users from the Awardco API with retry and exponential backoff.
    Args:
        base_url: The base URL for the API
        api_key: The API key for authentication
        page: The page number to fetch (default: 1)
        per_page: Number of items per page (default: 100)
    Returns:
        list: A list of user records from the API
    Raises:
        requests.exceptions.RequestException: If the API request fails after max retries
    """
    params = {"page": page, "per_page": per_page}
    headers = {"apiKey": api_key}
    url = f"{base_url}/api/users"

    response = make_request_with_retry(url, headers, params)
    return response.json().get("users", [])


def process_user_record(record: dict, current_sync_time: str) -> str:
    """
    Process a single user record and return the updated sync time.
    Args:
        record: The user record to process
        current_sync_time: The current sync time to compare against
    Returns:
        str: The updated sync time
    """
    # The 'upsert' operation is used to insert or update data in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the record to be upserted.
    # - The first argument is the name of the table to upsert the data into.
    # - The second argument is a dictionary containing the data to be upserted,
    op.upsert(table="user", data=record)
    record_time = record.get("updated_at")

    if current_sync_time is None or (record_time and record_time > current_sync_time):
        return record_time
    return current_sync_time


def process_user_page(users: list, current_sync_time: str) -> str:
    """
    Process a page of user records and return the latest sync time.
    Args:
        users: List of user records to process
        current_sync_time: The current sync time to compare against
    Returns:
        str: The updated sync time after processing all records
    """
    sync_time = current_sync_time
    for record in users:
        sync_time = process_user_record(record, sync_time)
    return sync_time


def checkpoint_sync_state(sync_time: str):
    """
    Save the sync state to resume from in the next sync.
    Args:
        sync_time: The sync time to checkpoint
    """
    new_state = {"last_sync_time": sync_time}
     # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
     # from the correct position in case of next sync or interruptions.
     # Learn more about how and where to checkpoint by reading our best practices documentation
     # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
     op.checkpoint(new_state)


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
    log.warning("Examples: Source Examples - Awardco")
    validate_configuration(configuration=configuration)

    api_key = configuration.get("api_key")
    base_url = configuration.get("base_url")
    sync_time = state.get("last_sync_time", "1990-01-01T00:00:00")

    try:
        page = 1
        per_page = __PAGE_SIZE

        while True:
            users = fetch_users(base_url, api_key, page, per_page)
            if not users:
                break

            sync_time = process_user_page(users, sync_time)
            checkpoint_sync_state(sync_time)
            if len(users) < per_page:
                break

            page += 1

    except Exception as e:
        raise RuntimeError(f"Failed to sync data: {str(e)}")


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
