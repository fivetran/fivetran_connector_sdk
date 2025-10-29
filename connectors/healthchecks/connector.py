"""Healthchecks.io Connector for Fivetran Connector SDK.
This connector syncs health check monitoring data from Healthchecks.io API to your destination.
It supports incremental syncing of checks, pings, and integrations data.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For handling date and time operations
import time
from datetime import datetime, timezone

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For making HTTP API requests
import requests

# Maximum number of retry attempts for API requests
__MAX_RETRIES = 3

# Base delay in seconds for exponential backoff retry strategy
__BASE_DELAY_SECONDS = 1

# HTTP status codes that should trigger a retry
__RETRYABLE_STATUS_CODES = [429, 500, 502, 503, 504]

# Base URL for Healthchecks.io API v3
__BASE_API_URL = "https://healthchecks.io/api/v3"


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["api_key"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def get_headers(api_key: str):
    """
    Generate HTTP headers for API requests with authentication.
    Args:
        api_key: The Healthchecks.io API key for authentication.
    Returns:
        Dictionary containing HTTP headers with API key authentication.
    """
    return {"X-Api-Key": api_key, "Content-Type": "application/json"}


def make_api_request(url: str, api_key: str):
    """
    Make an API request with retry logic and exponential backoff.
    Implements retry logic for transient errors with exponential backoff strategy.
    Args:
        url: The full API endpoint URL to request.
        api_key: The API key for authentication.
    Returns:
        Dictionary containing the parsed JSON response.
    Raises:
        RuntimeError: If the API request fails after all retry attempts.
    """
    headers = get_headers(api_key)

    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code == 200:
                return response.json()
            elif response.status_code in __RETRYABLE_STATUS_CODES:
                if attempt < __MAX_RETRIES - 1:
                    delay = __BASE_DELAY_SECONDS * (2**attempt)
                    log.warning(
                        f"Request failed with status {response.status_code}, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                    )
                    time.sleep(delay)
                    continue
                else:
                    log.severe(
                        f"Failed to fetch data after {__MAX_RETRIES} attempts. Last status: {response.status_code} - {response.text}"
                    )
                    raise RuntimeError(
                        f"API returned {response.status_code} after {__MAX_RETRIES} attempts: {response.text}"
                    )
            else:
                log.severe(
                    f"API request failed with status {response.status_code}: {response.text}"
                )
                raise RuntimeError(f"API returned {response.status_code}: {response.text}")

        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(
                    f"Network error occurred, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES}): {str(e)}"
                )
                time.sleep(delay)
                continue
            else:
                log.severe(f"Network error after {__MAX_RETRIES} attempts: {str(e)}")
                raise RuntimeError(f"Network error after {__MAX_RETRIES} attempts: {str(e)}")


def fetch_checks(api_key: str):
    """
    Fetch all health checks from Healthchecks.io API.
    Retrieves the complete list of configured health checks with their current status.
    Note: The API does not support incremental filtering, so this performs a full refresh.
    Args:
        api_key: The API key for authentication.
    Returns:
        List of check dictionaries containing health check data.
    """
    url = f"{__BASE_API_URL}/checks/"
    log.info(f"Fetching all checks from {url} (full refresh)")

    response_data = make_api_request(url, api_key)
    checks = response_data.get("checks", [])

    log.info(f"Retrieved {len(checks)} checks")
    return checks


def fetch_check_pings(check_uuid: str, api_key: str):
    """
    Fetch ping history for a specific health check.
    Retrieves the list of pings (check-ins) for a given health check.
    Args:
        check_uuid: The unique identifier of the health check.
        api_key: The API key for authentication.
    Returns:
        List of ping dictionaries containing ping event data.
    """
    url = f"{__BASE_API_URL}/checks/{check_uuid}/pings/"
    log.fine(f"Fetching pings for check {check_uuid}")

    try:
        response_data = make_api_request(url, api_key)
        pings = response_data.get("pings", [])
        log.fine(f"Retrieved {len(pings)} pings for check {check_uuid}")
        return pings
    except RuntimeError as e:
        log.warning(f"Failed to fetch pings for check {check_uuid}: {str(e)}")
        return []


def fetch_check_flips(check_uuid: str, api_key: str):
    """
    Fetch status change history (flips) for a specific health check.
    Retrieves the list of status changes between up and down states.
    Args:
        check_uuid: The unique identifier of the health check.
        api_key: The API key for authentication.
    Returns:
        List of flip dictionaries containing status change event data.
    """
    url = f"{__BASE_API_URL}/checks/{check_uuid}/flips/"
    log.fine(f"Fetching flips for check {check_uuid}")

    try:
        response_data = make_api_request(url, api_key)
        flips = response_data.get("flips", [])
        log.fine(f"Retrieved {len(flips)} flips for check {check_uuid}")
        return flips
    except RuntimeError as e:
        log.warning(f"Failed to fetch flips for check {check_uuid}: {str(e)}")
        return []


def fetch_integrations(api_key: str):
    """
    Fetch all notification integrations (channels) from Healthchecks.io API.
    Retrieves the list of configured notification channels for alerts.
    Args:
        api_key: The API key for authentication.
    Returns:
        List of channel dictionaries containing integration data.
    """
    url = f"{__BASE_API_URL}/channels/"
    log.info(f"Fetching integrations from {url}")

    try:
        response_data = make_api_request(url, api_key)
        channels = response_data.get("channels", [])
        log.info(f"Retrieved {len(channels)} integrations")
        return channels
    except RuntimeError as e:
        log.warning(f"Failed to fetch integrations: {str(e)}")
        return []


def flatten_check_record(check: dict):
    """
    Flatten a check record for database insertion.
    Converts nested JSON structure to a flat dictionary suitable for upserting.
    Args:
        check: Dictionary containing raw check data from the API.
    Returns:
        Flattened dictionary with all fields at the top level.
    """
    return {
        "uuid": check.get("uuid"),
        "name": check.get("name"),
        "slug": check.get("slug"),
        "tags": check.get("tags"),
        "description": check.get("desc"),
        "grace": check.get("grace"),
        "n_pings": check.get("n_pings"),
        "status": check.get("status"),
        "started": check.get("started"),
        "last_ping": check.get("last_ping"),
        "next_ping": check.get("next_ping"),
        "manual_resume": check.get("manual_resume"),
        "methods": check.get("methods"),
        "subject": check.get("subject"),
        "subject_fail": check.get("subject_fail"),
        "start_kw": check.get("start_kw"),
        "success_kw": check.get("success_kw"),
        "failure_kw": check.get("failure_kw"),
        "filter_subject": check.get("filter_subject"),
        "filter_body": check.get("filter_body"),
        "badge_url": check.get("badge_url"),
        "ping_url": check.get("ping_url"),
        "update_url": check.get("update_url"),
        "pause_url": check.get("pause_url"),
        "resume_url": check.get("resume_url"),
        "channels": check.get("channels"),
        "timeout": check.get("timeout"),
    }


def flatten_ping_record(ping: dict, check_uuid: str):
    """
    Flatten a ping record for database insertion.
    Adds check_uuid as foreign key and flattens the structure.
    Args:
        ping: Dictionary containing raw ping data from the API.
        check_uuid: The UUID of the parent health check.
    Returns:
        Flattened dictionary with check_uuid as foreign key.
    """
    return {
        "ping_id": f"{check_uuid}_{ping.get('n')}",
        "check_uuid": check_uuid,
        "n": ping.get("n"),
        "type": ping.get("type"),
        "date": ping.get("date"),
        "scheme": ping.get("scheme"),
        "remote_addr": ping.get("remote_addr"),
        "method": ping.get("method"),
        "ua": ping.get("ua"),
        "duration": ping.get("duration"),
    }


def flatten_flip_record(flip: dict, check_uuid: str):
    """
    Flatten a flip (status change) record for database insertion.
    Adds check_uuid as foreign key and flattens the structure.
    Args:
        flip: Dictionary containing raw flip data from the API.
        check_uuid: The UUID of the parent health check.
    Returns:
        Flattened dictionary with check_uuid as foreign key.
    """
    return {
        "flip_id": f"{check_uuid}_{flip.get('timestamp')}",
        "check_uuid": check_uuid,
        "timestamp": flip.get("timestamp"),
        "up": flip.get("up"),
    }


def flatten_integration_record(channel: dict):
    """
    Flatten an integration (channel) record for database insertion.
    Converts nested JSON structure to a flat dictionary suitable for upserting.
    Args:
        channel: Dictionary containing raw channel data from the API.
    Returns:
        Flattened dictionary with all fields at the top level.
    """
    return {"id": channel.get("id"), "name": channel.get("name"), "kind": channel.get("kind")}


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "checks", "primary_key": ["uuid"]},
        {"table": "pings", "primary_key": ["ping_id"]},
        {"table": "flips", "primary_key": ["flip_id"]},
        {"table": "integrations", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Note: This connector performs a full refresh on each sync because the Healthchecks.io API
    does not support timestamp-based filtering or pagination. The state is maintained for
    checkpoint tracking but not used for incremental data filtering.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector (unused due to API limitations).
    """
    log.warning("Example: Source Connector : Healthchecks.io")

    validate_configuration(configuration=configuration)

    api_key = configuration.get("api_key")

    try:
        current_sync_timestamp = datetime.now(timezone.utc).isoformat()

        checks = fetch_checks(api_key)

        for check in checks:
            flattened_check = flatten_check_record(check)

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="checks", data=flattened_check)

            check_uuid = check.get("uuid")

            pings = fetch_check_pings(check_uuid, api_key)
            for ping in pings:
                flattened_ping = flatten_ping_record(ping, check_uuid)

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table="pings", data=flattened_ping)

            flips = fetch_check_flips(check_uuid, api_key)
            for flip in flips:
                flattened_flip = flatten_flip_record(flip, check_uuid)

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table="flips", data=flattened_flip)

        log.info(f"Completed syncing {len(checks)} checks")

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        new_state = {"last_updated_timestamp": current_sync_timestamp}
        op.checkpoint(new_state)

        integrations = fetch_integrations(api_key)
        for integration in integrations:
            flattened_integration = flatten_integration_record(integration)

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table="integrations", data=flattened_integration)

        log.info(f"Completed syncing {len(integrations)} integrations")

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)

    except Exception as e:
        log.severe(f"Failed to sync data: {str(e)}")
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
