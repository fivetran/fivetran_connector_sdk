"""ZeroHash API connector for syncing participants, accounts, and assets data.
This connector demonstrates how to fetch data from ZeroHash API and upsert it into destination using memory-efficient streaming patterns.
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

# For making HTTP requests to ZeroHash API
import requests

# For handling dates and timestamps
from datetime import datetime, timedelta, timezone

# For implementing delays in retry logic and rate limiting
import time

# For adding jitter to retry delays
import random

# For creating HMAC-SHA256 signatures for ZeroHash authentication
import hmac
import hashlib

# For base64 encoding of signatures
import base64

""" ADD YOUR SOURCE-SPECIFIC IMPORTS HERE
Example: import pandas, boto3, etc.
Add comment for each import to explain its purpose for users to follow.
"""
# Private constants (use __ prefix)
__API_ENDPOINT = "https://api.cert.zerohash.com"


def __get_config_int(configuration, key, default, min_val=None, max_val=None):
    """
    Extract and validate integer configuration parameters with range checking.
    This function safely extracts integer values from configuration and applies validation.

    Args:
        configuration: Configuration dictionary containing connector settings.
        key: The configuration key to extract.
        default: Default value to return if key is missing or invalid.
        min_val: Minimum allowed value (optional).
        max_val: Maximum allowed value (optional).

    Returns:
        int: The validated integer value or default if validation fails.
    """
    try:
        value = int(configuration.get(key, default))
        if min_val is not None and value < min_val:
            return default
        if max_val is not None and value > max_val:
            return default
        return value
    except (ValueError, TypeError):
        return default


def __get_config_str(configuration, key, default=""):
    """
    Extract string configuration parameters with type safety.
    This function safely extracts string values from configuration dictionary.

    Args:
        configuration: Configuration dictionary containing connector settings.
        key: The configuration key to extract.
        default: Default value to return if key is missing.

    Returns:
        str: The string value or default if key is missing.
    """
    return str(configuration.get(key, default))


def __get_config_bool(configuration, key, default=False):
    """
    Extract and parse boolean configuration parameters from strings or boolean values.
    This function handles string representations of boolean values commonly used in JSON configuration.

    Args:
        configuration: Configuration dictionary containing connector settings.
        key: The configuration key to extract.
        default: Default boolean value to return if key is missing.

    Returns:
        bool: The parsed boolean value or default if key is missing.
    """
    value = configuration.get(key, default)
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
    return bool(value)


def __generate_signature(timestamp, method, path, body, secret_key):
    """
    Generate HMAC-SHA256 signature for ZeroHash API authentication.
    This function creates the required signature using ZeroHash's authentication protocol.

    Args:
        timestamp: Unix timestamp as string.
        method: HTTP method (GET, POST, etc.).
        path: API endpoint path.
        body: Request body (empty string for GET requests).
        secret_key: Secret key for signing.

    Returns:
        str: Base64-encoded HMAC-SHA256 signature.
    """
    payload = f"{timestamp}{method}{path}{body}"
    signature = hmac.new(
        secret_key.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256
    ).digest()
    return base64.b64encode(signature).decode("utf-8")


def __calculate_wait_time(attempt, response_headers, base_delay=1, max_delay=60):
    """
    Calculate exponential backoff wait time with jitter for retry attempts.
    This function implements exponential backoff with random jitter to prevent thundering herd problems.

    Args:
        attempt: Current attempt number (0-based).
        response_headers: HTTP response headers dictionary that may contain Retry-After.
        base_delay: Base delay in seconds for exponential backoff.
        max_delay: Maximum delay cap in seconds.

    Returns:
        float: Wait time in seconds before next retry attempt.
    """
    if "Retry-After" in response_headers:
        return min(int(response_headers["Retry-After"]), max_delay)

    # Exponential backoff with jitter
    wait_time = min(base_delay * (2**attempt), max_delay)
    jitter = random.uniform(0.1, 0.3) * wait_time
    return wait_time + jitter


def __handle_rate_limit(attempt, response):
    """
    Handle HTTP 429 rate limiting responses with appropriate delays.
    This function logs the rate limit and waits before allowing retry attempts.

    Args:
        attempt: Current attempt number for logging purposes.
        response: HTTP response object containing rate limit headers.
    """
    wait_time = __calculate_wait_time(attempt, response.headers)
    log.warning(f"Rate limit hit, waiting {wait_time:.1f} seconds before retry {attempt + 1}")
    time.sleep(wait_time)


def __handle_request_error(attempt, retry_attempts, error, endpoint):
    """
    Handle request errors with exponential backoff retry logic.
    This function manages retry attempts for failed API requests with appropriate delays.

    Args:
        attempt: Current attempt number (0-based).
        retry_attempts: Total number of retry attempts allowed.
        error: The exception that occurred during the request.
        endpoint: API endpoint that failed for logging purposes.

    Raises:
        Exception: Re-raises the original error after all retry attempts are exhausted.
    """
    if attempt < retry_attempts - 1:
        wait_time = __calculate_wait_time(attempt, {})
        log.warning(
            f"Request failed for {endpoint}: {str(error)}. Retrying in {wait_time:.1f} seconds..."
        )
        time.sleep(wait_time)
    else:
        log.severe(f"All retry attempts failed for {endpoint}: {str(error)}")
        raise error


def execute_api_request(endpoint, api_key, secret_key, params=None, configuration=None):
    """
    Execute HTTP API requests with comprehensive error handling and retry logic.
    This function handles authentication, rate limiting, timeouts, and network errors.

    Args:
        endpoint: API endpoint path to request.
        api_key: Public API key for ZeroHash authentication.
        secret_key: Secret key for signing requests.
        params: Query parameters for the request (optional).
        configuration: Configuration dictionary for timeout and retry settings.

    Returns:
        dict: Parsed JSON response from the API.

    Raises:
        RuntimeError: If all retry attempts fail or unexpected errors occur.
        requests.exceptions.RequestException: For unrecoverable HTTP errors.
    """
    url = f"{__API_ENDPOINT}{endpoint}"

    # Generate timestamp and signature for authentication
    timestamp = str(int(time.time()))
    method = "GET"
    body = ""  # Empty for GET requests
    signature = __generate_signature(timestamp, method, endpoint, body, secret_key)

    headers = {"X-SCX-TIMESTAMP": timestamp, "X-SCX-SIGNED": signature, "x-pk": api_key}

    timeout = __get_config_int(configuration, "request_timeout_seconds", 30)
    retry_attempts = __get_config_int(configuration, "retry_attempts", 3)

    for attempt in range(retry_attempts):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)

            if response.status_code == 429:
                __handle_rate_limit(attempt, response)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            __handle_request_error(attempt, retry_attempts, e, endpoint)
            continue

    raise RuntimeError("Unexpected error in API request execution")


def __map_participant_data(record):
    """
    Transform API response record to participants table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "email": record.get("email", ""),
        "participant_code": record.get("participantCode", ""),
        "name": record.get("name", ""),
        "type": record.get("type", ""),
        "status": record.get("status", ""),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_account_data(record):
    """
    Transform API response record to accounts table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "account_id": record.get("accountId", ""),
        "participant_id": record.get("participantId", ""),
        "asset_symbol": record.get("assetSymbol", ""),
        "balance": record.get("balance", "0"),
        "available_balance": record.get("availableBalance", "0"),
        "status": record.get("status", ""),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_asset_data(record):
    """
    Transform API response record to assets table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "id": record.get("id", ""),
        "symbol": record.get("symbol", ""),
        "name": record.get("name", ""),
        "type": record.get("type", ""),
        "decimals": record.get("decimals", 0),
        "status": record.get("status", ""),
        "minimum_amount": record.get("minimumAmount", "0"),
        "maximum_amount": record.get("maximumAmount", "0"),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_time_range(last_sync_time=None, configuration=None):
    """
    Generate time range for incremental or initial data synchronization.
    This function creates start and end timestamps for API queries based on sync state.

    Args:
        last_sync_time: Timestamp of last successful sync (optional).
        configuration: Configuration dictionary containing sync settings.

    Returns:
        dict: Dictionary containing 'start' and 'end' timestamps in ISO format.
    """
    end_time = datetime.now(timezone.utc).isoformat()

    if last_sync_time:
        start_time = last_sync_time
    else:
        initial_sync_days = __get_config_int(configuration, "initial_sync_days", 90)
        start_time = (datetime.now(timezone.utc) - timedelta(days=initial_sync_days)).isoformat()

    return {"start": start_time, "end": end_time}


def get_participants(api_key, secret_key, last_sync_time=None, configuration=None):
    """
    Fetch participants data using memory-efficient streaming approach.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: Public API key for ZeroHash authentication.
        secret_key: Secret key for signing requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual participant records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/participants"
    response = execute_api_request(endpoint, api_key, secret_key, None, configuration)

    # Handle both list and object responses
    participants = response if isinstance(response, list) else response.get("participants", [])

    for participant in participants:
        yield __map_participant_data(participant)


def get_accounts(api_key, secret_key, last_sync_time=None, configuration=None):
    """
    Fetch accounts data using memory-efficient streaming approach.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: Public API key for ZeroHash authentication.
        secret_key: Secret key for signing requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual account records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/accounts"
    response = execute_api_request(endpoint, api_key, secret_key, None, configuration)

    # Handle both list and object responses
    accounts = response if isinstance(response, list) else response.get("accounts", [])

    for account in accounts:
        yield __map_account_data(account)


def get_assets(api_key, secret_key, last_sync_time=None, configuration=None):
    """
    Fetch assets data using memory-efficient streaming approach.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: Public API key for ZeroHash authentication.
        secret_key: Secret key for signing requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual asset records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/assets"
    response = execute_api_request(endpoint, api_key, secret_key, None, configuration)

    # Handle both list and object responses
    assets = response if isinstance(response, list) else response.get("assets", [])

    for asset in assets:
        yield __map_asset_data(asset)


def schema(configuration: dict):
    """
    Define database schema with table names and primary keys for the connector.
    This function specifies the destination tables and their primary keys for Fivetran to create.

    Args:
        configuration: Configuration dictionary (not used but required by SDK).

    Returns:
        list: List of table schema dictionaries with table names and primary keys.
    """
    return [
        {"table": "participants", "primary_key": ["id"]},
        {"table": "accounts", "primary_key": ["id"]},
        {"table": "assets", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Main synchronization function that fetches and processes data from the ZeroHash API.
    This function orchestrates the entire sync process using memory-efficient streaming patterns.

    Args:
        configuration: Configuration dictionary containing API credentials and settings.
        state: State dictionary containing sync cursors and checkpoints from previous runs.

    Raises:
        RuntimeError: If sync fails due to API errors or configuration issues.
    """
    log.info("Starting ZeroHash API connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    api_key = __get_config_str(configuration, "api_key")
    secret_key = __get_config_str(configuration, "secret_key")
    enable_incremental = __get_config_bool(configuration, "enable_incremental_sync", True)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time") if enable_incremental else None

    try:
        # Fetch participants data
        log.info("Fetching participants data...")
        participant_count = 0
        for participant in get_participants(api_key, secret_key, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="participants", data=participant)
            participant_count += 1

        log.info(f"Processed {participant_count} participants")

        # Fetch accounts data
        log.info("Fetching accounts data...")
        account_count = 0
        for account in get_accounts(api_key, secret_key, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="accounts", data=account)
            account_count += 1

        log.info(f"Processed {account_count} accounts")

        # Fetch assets data
        log.info("Fetching assets data...")
        asset_count = 0
        for asset in get_assets(api_key, secret_key, last_sync_time, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="assets", data=asset)
            asset_count += 1

        log.info(f"Processed {asset_count} assets")

        # Final checkpoint with completion status
        final_state = {"last_sync_time": datetime.now(timezone.utc).isoformat()}
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        total_records = participant_count + account_count + asset_count
        log.info(f"Sync completed successfully. Processed {total_records} total records.")

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync ZeroHash data: {str(e)}")


# Create the connector instance
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
