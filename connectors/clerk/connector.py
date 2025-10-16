"""Clerk API Connector for Fivetran Connector SDK.
This connector fetches user data from Clerk API and syncs it to the destination with flattened nested structures.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For adding delay between retries
import time

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For making HTTP requests to the Clerk API
import requests

# Constants for API configuration
__API_BASE_URL = "https://api.clerk.com/v1"
__USERS_ENDPOINT = "/users"
__PAGE_LIMIT = 100  # Number of records to fetch per API request
__MAX_RETRIES = 3  # Maximum number of retry attempts for API requests
__CHECKPOINT_INTERVAL = 1000  # Checkpoint state after every 1000 records processed

# Child table configuration: maps array field names to their destination table names
__CHILD_TABLES = {
    "email_addresses": "user_email_address",
    "phone_numbers": "user_phone_number",
    "web3_wallets": "user_web3_wallet",
    "passkeys": "user_passkey",
    "external_accounts": "user_external_account",
    "saml_accounts": "user_saml_account",
}


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: If any required configuration parameter is missing.
    """

    # Validate required configuration parameters
    if "api_key" not in configuration:
        raise ValueError("Missing required configuration value: api_key")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema

    Args:
        configuration: A dictionary that holds the configuration settings for the connector.

    Returns:
        List of table schema definitions with table names and primary keys.
    """

    return [
        {
            "table": "user",
            "primary_key": ["id"],
        },
        {
            "table": "user_email_address",
            "primary_key": ["id"],
        },
        {
            "table": "user_phone_number",
            "primary_key": ["id"],
        },
        {
            "table": "user_web3_wallet",
            "primary_key": ["id"],
        },
        {
            "table": "user_passkey",
            "primary_key": ["id"],
        },
        {
            "table": "user_external_account",
            "primary_key": ["id"],
        },
        {
            "table": "user_saml_account",
            "primary_key": ["id"],
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Args:
        configuration: A dictionary containing connection details.
        state: A dictionary containing state information from previous runs.
               The state dictionary is empty for the first sync or for any full re-sync.

    Raises:
        RuntimeError: If API request fails or data sync encounters an error.
    """

    log.warning("Example: Source Examples - Clerk API")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # Extract configuration parameters as required
    api_key = configuration.get("api_key")

    # Get the state variable for the sync - last_created_at tracks incremental sync using created_at_after
    # Default to January 1, 1990 (631152000000 ms since epoch) if no previous state exists
    last_created_at = state.get("last_created_at", 631152000000)
    new_created_at = last_created_at

    # Counter for tracking records processed for checkpointing
    records_processed = 0

    try:
        # Fetch users using pagination with incremental sync support
        for user in fetch_users_paginated(api_key, last_created_at):

            # Process and upsert user and child table records
            process_user_record(user)

            # Track the latest created_at timestamp for incremental sync
            new_created_at = update_sync_cursor(user, new_created_at)

            records_processed += 1

            # Checkpoint state periodically to enable resumption on interruption
            if records_processed % __CHECKPOINT_INTERVAL == 0:
                checkpoint_sync_state(new_created_at, records_processed)

        # Final checkpoint with the latest state
        checkpoint_sync_state_final(new_created_at, records_processed)

    except requests.exceptions.RequestException as e:
        # Handle HTTP/network related errors
        raise RuntimeError(f"API request failed: {str(e)}")
    except Exception as e:
        # In case of an exception, raise a runtime error
        raise RuntimeError(f"Failed to sync data: {str(e)}")


def process_user_record(user):
    """
    Process and upsert a single user record along with its child table data.

    Args:
        user: The user dictionary from the API.
    """
    # Process and upsert the main user record
    user_record = flatten_user_record(user)

    # The 'upsert' operation is used to insert or update data in the destination table.
    # The op.upsert method is called with two arguments:
    # - The first argument is the name of the table to upsert the data into.
    # - The second argument is a dictionary containing the data to be upserted.
    op.upsert(table="user", data=user_record)

    # Process nested arrays and upsert into separate child tables
    process_child_tables(user)


def update_sync_cursor(user, current_cursor):
    """
    Update the sync cursor with the latest created_at timestamp.

    Args:
        user: The user dictionary from the API.
        current_cursor: The current sync cursor value.

    Returns:
        Updated cursor value.
    """
    user_created_at = user.get("created_at")
    if user_created_at and (current_cursor is None or user_created_at > current_cursor):
        return user_created_at
    return current_cursor


def checkpoint_sync_state(cursor_value, records_processed):
    """
    Checkpoint the current sync state.

    Args:
        cursor_value: The current cursor value to save.
        records_processed: Number of records processed so far.
    """
    checkpoint_state = {"last_created_at": cursor_value}
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(checkpoint_state)
    log.info(f"Checkpointed after processing {records_processed} records")


def checkpoint_sync_state_final(cursor_value, records_processed):
    """
    Checkpoint the final sync state and log completion.

    Args:
        cursor_value: The final cursor value to save.
        records_processed: Total number of records processed.
    """
    final_state = {"last_created_at": cursor_value}
    op.checkpoint(final_state)
    log.info(f"Sync completed. Total records processed: {records_processed}")


def fetch_users_paginated(api_key, last_created_at=None):
    """
    Fetch users from Clerk API using offset-based pagination with incremental sync support.
    This generator function yields user records one at a time to avoid loading all data into memory.
    Pagination is handled using offset and limit parameters as per Clerk API documentation.

    Args:
        api_key: The API key for authentication.
        last_created_at: Timestamp in milliseconds for incremental sync (fetches users created after this time).

    Yields:
        User dictionaries from the API response.
    """
    offset = 0
    has_more_data = True

    while has_more_data:
        # Build API URL with pagination parameters
        url = f"{__API_BASE_URL}{__USERS_ENDPOINT}"
        params = {"limit": __PAGE_LIMIT, "offset": offset, "order_by": "created_at"}

        # Add incremental sync parameter if last_created_at is provided
        if last_created_at:
            params["created_at_after"] = last_created_at

        # Make API request with retry logic
        users = make_api_request(url, api_key, params)

        # If no users returned, we've reached the end
        if not users:
            has_more_data = False
            break

        # Yield each user individually to avoid memory issues
        for user in users:
            yield user

        # If we received fewer records than the limit, we've reached the end
        if len(users) < __PAGE_LIMIT:
            has_more_data = False
        else:
            # Move to the next page
            offset += __PAGE_LIMIT


def make_api_request(url, api_key, params):
    """
    Make an API request to Clerk API with retry logic and exponential backoff.

    Args:
        url: The API endpoint URL.
        api_key: The API key for authentication.
        params: Query parameters for the request.

    Returns:
        JSON response from the API.

    Raises:
        requests.exceptions.RequestException: If all retry attempts fail.
    """
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    for attempt in range(__MAX_RETRIES):
        response = None
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError:
            if response and response.status_code == 429:
                # Exponential backoff for rate limiting
                wait_time = 2**attempt
                log.warning(f"Rate limited. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            elif response and 500 <= response.status_code < 600:
                # Retry on server errors
                if attempt < __MAX_RETRIES - 1:
                    wait_time = 2**attempt
                    log.warning(
                        f"Server error {response.status_code}. Retrying in {wait_time} seconds..."
                    )
                    time.sleep(wait_time)
                else:
                    raise
            else:
                # Don't retry on client errors (4xx except 429)
                raise
        except requests.exceptions.RequestException as e:
            # Retry on network errors
            if attempt < __MAX_RETRIES - 1:
                wait_time = 2**attempt
                log.warning(f"Request failed: {str(e)}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise

    # If all retries exhausted
    raise requests.exceptions.RequestException(f"Failed after {__MAX_RETRIES} attempts")


def flatten_record(record, excluded_fields=None):
    """
    Flatten a record by converting nested dictionaries into underscore-separated column names.

    Args:
        record: The record to flatten.
        excluded_fields: List of field names to exclude from flattening (e.g., array fields).

    Returns:
        Flattened dictionary.
    """
    flattened = {}
    excluded_fields = excluded_fields or []

    for key, value in record.items():
        if key in excluded_fields:
            continue
        elif isinstance(value, dict):
            # Flatten nested objects with underscore notation
            for nested_key, nested_value in value.items():
                if isinstance(nested_value, dict):
                    # Handle deeply nested objects
                    for deep_key, deep_value in nested_value.items():
                        flattened[f"{key}_{nested_key}_{deep_key}"] = deep_value
                else:
                    if isinstance(nested_value, list):
                        flattened[f"{key}_{nested_key}"] = json.dumps(nested_value) if nested_value else None
                    else:
                        flattened[f"{key}_{nested_key}"] = nested_value
        elif isinstance(value, list):
            # Convert arrays to JSON strings for storage
            flattened[key] = json.dumps(value) if value else None
        else:
            flattened[key] = value

    return flattened


def flatten_user_record(user):
    """
    Flatten the main user record by extracting nested JSON objects into the same table.
    Arrays are excluded and processed separately into child tables.

    Args:
        user: The user dictionary from the API.

    Returns:
        Flattened user dictionary with nested objects converted to column names.
    """
    # Exclude array fields that will be processed separately
    array_fields = list(__CHILD_TABLES.keys()) + ["enterprise_accounts"]
    return flatten_record(user, excluded_fields=array_fields)


def process_child_tables(user):
    """
    Process all child table arrays and upsert records into their respective tables.

    Args:
        user: The user dictionary containing array fields.
    """
    user_id = user.get("id")

    # Process each configured child table
    for array_field, table_name in __CHILD_TABLES.items():
        array_data = user.get(array_field, [])
        for item in array_data:
            # Add user_id as foreign key and flatten the record
            item_record = flatten_record(item)
            item_record["user_id"] = user_id

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted.
            op.upsert(table=table_name, data=item_record)


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
