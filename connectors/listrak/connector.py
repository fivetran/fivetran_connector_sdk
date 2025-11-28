"""Listrak API connector for syncing email marketing data including contacts, campaigns, and events.
This connector demonstrates how to fetch data from Listrak API and upsert it into destination using memory-efficient streaming patterns.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For implementing delays in retry logic and rate limiting
import time

# For adding jitter to retry delays
import random

# For making HTTP requests to Listrak API
import requests

# For handling dates and timestamps
from datetime import datetime, timedelta, timezone

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Private constants (use __ prefix)
__API_ENDPOINT = "https://api.listrak.com/email/v1"  # Listrak API base URL


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


def execute_api_request(endpoint, api_key, params=None, configuration=None):
    """
    Execute HTTP API requests with comprehensive error handling and retry logic.
    This function handles authentication, rate limiting, timeouts, and network errors.

    Args:
        endpoint: API endpoint path to request.
        api_key: Authentication key for API access.
        params: Query parameters for the request (optional).
        configuration: Configuration dictionary for timeout and retry settings.

    Returns:
        dict: Parsed JSON response from the API.

    Raises:
        RuntimeError: If all retry attempts fail or unexpected errors occur.
        requests.exceptions.RequestException: For unrecoverable HTTP errors.
    """
    url = f"{__API_ENDPOINT}{endpoint}"
    headers = {"Authorization": f"Bearer {api_key}"}

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


def __map_contact_data(record):
    """
    Transform API response contact record to destination table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response contact record dictionary.

    Returns:
        dict: Transformed contact record ready for database insertion.
    """
    return {
        "contact_id": record.get("id", ""),
        "email_address": record.get("email", ""),
        "first_name": record.get("firstName", ""),
        "last_name": record.get("lastName", ""),
        "subscription_status": record.get("status", ""),
        "created_date": record.get("createdDate", ""),
        "updated_date": record.get("updatedDate", ""),
        "phone_number": record.get("phone", ""),
        "city": record.get("city", ""),
        "state": record.get("state", ""),
        "zip_code": record.get("zipCode", ""),
        "sync_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_campaign_data(record):
    """
    Transform API response campaign record to destination table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response campaign record dictionary.

    Returns:
        dict: Transformed campaign record ready for database insertion.
    """
    return {
        "campaign_id": record.get("id", ""),
        "campaign_name": record.get("name", ""),
        "subject_line": record.get("subject", ""),
        "send_date": record.get("sendDate", ""),
        "campaign_type": record.get("type", ""),
        "status": record.get("status", ""),
        "recipients_count": record.get("recipientsCount", 0),
        "opens_count": record.get("opensCount", 0),
        "clicks_count": record.get("clicksCount", 0),
        "bounces_count": record.get("bouncesCount", 0),
        "created_date": record.get("createdDate", ""),
        "updated_date": record.get("updatedDate", ""),
        "sync_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_event_data(record):
    """
    Transform API response event record to destination table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response event record dictionary.

    Returns:
        dict: Transformed event record ready for database insertion.
    """
    return {
        "event_id": record.get("id", ""),
        "contact_id": record.get("contactId", ""),
        "campaign_id": record.get("campaignId", ""),
        "event_type": record.get("eventType", ""),
        "event_timestamp": record.get("timestamp", ""),
        "email_address": record.get("email", ""),
        "user_agent": record.get("userAgent", ""),
        "ip_address": record.get("ipAddress", ""),
        "url": record.get("url", ""),
        "metadata": json.dumps(record.get("metadata", {})),
        "sync_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_contacts(api_key, last_sync_time=None, configuration=None):
    """
    Fetch contacts data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual contact records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/contacts"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    params = {
        "perPage": max_records,
        "page": 1,
        "updatedAfter": time_range["start"],
        "updatedBefore": time_range["end"],
    }

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        contacts = response.get("contacts", response.get("data", []))
        if not contacts:
            break

        # Yield individual records instead of accumulating
        for contact in contacts:
            yield __map_contact_data(contact)

        if len(contacts) < max_records:
            break
        page += 1


def get_campaigns(api_key, last_sync_time=None, configuration=None):
    """
    Fetch campaigns data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual campaign records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/campaigns"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    params = {
        "perPage": max_records,
        "page": 1,
        "sendDateAfter": time_range["start"],
        "sendDateBefore": time_range["end"],
    }

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        campaigns = response.get("campaigns", response.get("data", []))
        if not campaigns:
            break

        # Yield individual records instead of accumulating
        for campaign in campaigns:
            yield __map_campaign_data(campaign)

        if len(campaigns) < max_records:
            break
        page += 1


def get_events(api_key, last_sync_time=None, configuration=None):
    """
    Fetch events data using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        api_key: API authentication key for making requests.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual event records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/events"
    max_records = __get_config_int(configuration, "max_records_per_page", 100)

    params = {
        "perPage": max_records,
        "page": 1,
        "timestampAfter": time_range["start"],
        "timestampBefore": time_range["end"],
    }

    page = 1
    while True:
        params["page"] = page
        response = execute_api_request(endpoint, api_key, params, configuration)

        events = response.get("events", response.get("data", []))
        if not events:
            break

        # Yield individual records instead of accumulating
        for event in events:
            yield __map_event_data(event)

        if len(events) < max_records:
            break
        page += 1


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "contacts", "primary_key": ["contact_id"]},
        {"table": "campaigns", "primary_key": ["campaign_id"]},
        {"table": "events", "primary_key": ["event_id"]},
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
    log.info("Starting Listrak API connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    api_key = __get_config_str(configuration, "api_key")
    max_records_per_page = __get_config_int(configuration, "max_records_per_page", 100, 1, 1000)
    enable_incremental = __get_config_bool(configuration, "enable_incremental_sync", True)

    # Get state for incremental sync
    contacts_last_sync = state.get("contacts_last_sync_time")
    campaigns_last_sync = state.get("campaigns_last_sync_time")
    events_last_sync = state.get("events_last_sync_time")

    try:
        # Fetch contacts data using generator with incremental checkpointing
        log.info("Fetching contacts data...")
        contacts_count = 0
        latest_contact_timestamp = contacts_last_sync

        for contact in get_contacts(
            api_key, contacts_last_sync if enable_incremental else None, configuration
        ):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="contacts", data=contact)
            contacts_count += 1

            # Track latest timestamp for state update
            if contact.get("updated_date"):
                latest_contact_timestamp = contact["updated_date"]

            # Checkpoint every batch to save progress incrementally
            if contacts_count % max_records_per_page == 0:
                checkpoint_state = state.copy()
                checkpoint_state["contacts_last_sync_time"] = latest_contact_timestamp
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)

        log.info(f"Processed {contacts_count} contacts")

        # Fetch campaigns data using generator with incremental checkpointing
        log.info("Fetching campaigns data...")
        campaigns_count = 0
        latest_campaign_timestamp = campaigns_last_sync

        for campaign in get_campaigns(
            api_key, campaigns_last_sync if enable_incremental else None, configuration
        ):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="campaigns", data=campaign)
            campaigns_count += 1

            # Track latest timestamp for state update
            if campaign.get("send_date"):
                latest_campaign_timestamp = campaign["send_date"]

            # Checkpoint every batch to save progress incrementally
            if campaigns_count % max_records_per_page == 0:
                checkpoint_state = state.copy()
                checkpoint_state["campaigns_last_sync_time"] = latest_campaign_timestamp
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)

        log.info(f"Processed {campaigns_count} campaigns")

        # Fetch events data using generator with incremental checkpointing
        log.info("Fetching events data...")
        events_count = 0
        latest_event_timestamp = events_last_sync

        for event in get_events(
            api_key, events_last_sync if enable_incremental else None, configuration
        ):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="events", data=event)
            events_count += 1

            # Track latest timestamp for state update
            if event.get("event_timestamp"):
                latest_event_timestamp = event["event_timestamp"]

            # Checkpoint every batch to save progress incrementally
            if events_count % max_records_per_page == 0:
                checkpoint_state = state.copy()
                checkpoint_state["events_last_sync_time"] = latest_event_timestamp
                # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                # from the correct position in case of next sync or interruptions.
                # Learn more about how and where to checkpoint by reading our best practices documentation
                # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                op.checkpoint(checkpoint_state)

        log.info(f"Processed {events_count} events")

        # Final checkpoint with completion status
        default_timestamp = datetime.now(timezone.utc).isoformat()
        final_state = {
            "contacts_last_sync_time": (latest_contact_timestamp or default_timestamp),
            "campaigns_last_sync_time": (latest_campaign_timestamp or default_timestamp),
            "events_last_sync_time": (latest_event_timestamp or default_timestamp),
        }
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info(
            f"Sync completed successfully. Processed {contacts_count} contacts, {campaigns_count} campaigns, {events_count} events."
        )

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Create a Connector instance
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
