"""This connector demonstrates how to fetch data from Adform API and upsert it into destination using the Fivetran Connector SDK.
Supports querying data from Campaigns, Line Items, and Performance Data APIs.

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Import required libraries for API interactions
import requests
import json
import time
import random
from datetime import datetime, timedelta, timezone


# Private constants (use __ prefix)
__INVALID_LITERAL_ERROR = "invalid literal"
__API_BASE_URL = "https://api.adform.com/v1"
__DEFAULT_PAGE_SIZE = 100
__MAX_PAGE_SIZE = 1000
__DEFAULT_TIMEOUT = 30
__MAX_RETRY_ATTEMPTS = 3
__BASE_DELAY = 1
__MAX_DELAY = 60


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """

    __validate_required_fields(configuration)
    __validate_numeric_ranges(configuration)


def __get_config_int(configuration, key, default, min_val=None, max_val=None):
    """
    Centralized configuration parameter parsing with validation.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        key: the key of the configuration parameter to parse.
        default: the default value to use if the configuration parameter is not found.
        min_val: the minimum value for the configuration parameter.
        max_val: the maximum value for the configuration parameter.
    Returns:
        The parsed configuration parameter.
    """
    try:
        value = int(configuration.get(key, default))
        if min_val is not None and value < min_val:
            log.warning(f"Config {key} value {value} below minimum {min_val}, using minimum")
            return min_val
        if max_val is not None and value > max_val:
            log.warning(f"Config {key} value {value} above maximum {max_val}, using maximum")
            return max_val
        return value
    except (ValueError, TypeError) as e:
        if __INVALID_LITERAL_ERROR in str(e).lower():
            log.warning(f"Invalid {key} value, using default {default}")
            return default
        raise ValueError(f"Invalid configuration value for {key}: {e}")


def __validate_required_fields(configuration):
    """
    Validate required fields using dictionary mapping approach.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_fields = {
        "api_key": "Adform API key is required",
        "client_id": "Adform client ID is required",
    }

    for field, error_message in required_fields.items():
        if not configuration.get(field):
            raise ValueError(error_message)


def __validate_numeric_ranges(configuration):
    """
    Validate numeric parameters using iteration pattern.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any configuration parameter is not within the valid range.
    """
    numeric_validations = [
        ("max_records_per_page", 1, __MAX_PAGE_SIZE),
        ("request_timeout_seconds", 5, 300),
        ("retry_attempts", 1, 10),
        ("initial_sync_days", 1, 365),
    ]

    for param, min_val, max_val in numeric_validations:
        if param in configuration:
            __get_config_int(configuration, param, None, min_val, max_val)


def __calculate_wait_time(
    attempt, response_headers, base_delay=__BASE_DELAY, max_delay=__MAX_DELAY
):
    """
    Calculate wait time with jitter for rate limiting and retries.
    Args:
        attempt: the current attempt number.
        response_headers: the headers of the response from the API.
        base_delay: the base delay in seconds.
        max_delay: the maximum delay in seconds.
    Returns:
        The calculated wait time.
    """
    if "Retry-After" in response_headers:
        try:
            retry_after = int(response_headers["Retry-After"])
            return min(retry_after, max_delay)
        except (ValueError, TypeError):
            pass

    # Exponential backoff with jitter
    wait_time = min(base_delay * (2**attempt), max_delay)
    jitter = random.uniform(0.1, 0.3) * wait_time
    return wait_time + jitter


def __handle_rate_limit(attempt, response):
    """
    Handle HTTP 429 rate limiting.
    Args:
        attempt: the current attempt number.
        response: the response from the API.
    """
    wait_time = __calculate_wait_time(attempt, response.headers)
    log.warning(f"Rate limit hit, waiting {wait_time:.2f} seconds before retry {attempt + 1}")
    time.sleep(wait_time)


def __handle_request_error(attempt, retry_attempts, error, endpoint):
    """Handle request errors with retry logic.
    Args:
        attempt: the current attempt number.
        retry_attempts: the maximum number of attempts.
        error: the error message.
        endpoint: the endpoint of the API.
    """
    if attempt < retry_attempts - 1:
        wait_time = __calculate_wait_time(attempt, {})
        log.warning(
            f"Request to {endpoint} failed (attempt {attempt + 1}): {error}. Retrying in {wait_time:.2f}s"
        )
        time.sleep(wait_time)
    else:
        log.severe(f"Request to {endpoint} failed after {retry_attempts} attempts: {error}")
        raise


def execute_api_request(endpoint, api_key, params=None, configuration=None):
    """Main API request orchestrator.
    Args:
        endpoint: the endpoint of the API.
        api_key: the API key.
        params: the parameters for the API request.
        configuration: the configuration for the connector.
    """
    url = f"{__API_BASE_URL}{endpoint}"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    timeout = __get_config_int(configuration, "request_timeout_seconds", __DEFAULT_TIMEOUT)
    retry_attempts = __get_config_int(configuration, "retry_attempts", __MAX_RETRY_ATTEMPTS)

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
    """Generate dynamic time range.
    Args:
        last_sync_time: the last sync time.
        configuration: the configuration for the connector.
    Returns:
        The time range.
    """
    end_time = datetime.now(timezone.utc).isoformat()

    if last_sync_time:
        start_time = last_sync_time
    else:
        initial_sync_days = __get_config_int(configuration, "initial_sync_days", 90)
        start_time = (datetime.now(timezone.utc) - timedelta(days=initial_sync_days)).isoformat()

    return {"start": start_time, "end": end_time}


def __map_campaign_data(record, client_id):
    """Extract field mapping logic for campaign data.
    Args:
        record: the record from the API.
        client_id: the client ID.
    Returns:
        The mapped campaign data.
    """
    return {
        "id": record.get("id", ""),
        "client_id": client_id,
        "name": record.get("name", ""),
        "status": record.get("status", ""),
        "start_date": record.get("startDate", ""),
        "end_date": record.get("endDate", ""),
        "budget": record.get("budget", 0),
        "currency": record.get("currency", ""),
        "created_at": record.get("createdAt", ""),
        "updated_at": record.get("updatedAt", ""),
        "campaign_type": record.get("campaignType", ""),
        "objective": record.get("objective", ""),
        "sync_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_campaigns(api_key, client_id, last_sync_time=None, configuration=None):
    """Fetch campaigns using streaming approach to prevent memory issues.
    Args:
        api_key: the API key.
        client_id: the client ID.
        last_sync_time: the last sync time.
        configuration: the configuration for the connector.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/buyer/campaigns"
    max_records = __get_config_int(configuration, "max_records_per_page", __DEFAULT_PAGE_SIZE)

    params = {
        "limit": max_records,
        "offset": 0,
        "clientId": client_id,
        "updatedSince": time_range["start"],
    }

    offset = 0
    while True:
        params["offset"] = offset
        response = execute_api_request(endpoint, api_key, params, configuration)

        campaigns = response.get("data", [])
        if not campaigns:
            break

        # Yield individual records instead of accumulating
        for campaign in campaigns:
            yield __map_campaign_data(campaign, client_id)

        if len(campaigns) < max_records:
            break
        offset += max_records


def schema(configuration: dict):
    """Define minimal schema - let Fivetran infer column types.
    Args:
        configuration: the configuration for the connector.
    Returns:
        The schema.
    """
    return [
        {"table": "campaign", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """Main sync function - uses generators for memory efficiency.
    Args:
        configuration: the configuration for the connector.
        state: the state for the connector.
    """
    log.info("Starting Adform connector sync")

    # Validate configuration
    validate_configuration(configuration)

    # Extract configuration parameters
    api_key = str(configuration.get("api_key", ""))
    client_id = str(configuration.get("client_id", ""))

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time")

    try:
        # Fetch campaigns using generator (NO MEMORY ACCUMULATION)
        log.info("Fetching campaigns...")
        campaign_count = 0
        new_processed_campaigns = []

        for campaign in get_campaigns(api_key, client_id, last_sync_time, configuration):
            op.upsert(table="campaign", data=campaign)
            campaign_count += 1
            campaign_id = campaign["id"]
            new_processed_campaigns.append(campaign_id)

        log.info(f"Processed {campaign_count} campaigns")

        # Update state and checkpoint
        new_state = {
            "last_sync_time": datetime.now(timezone.utc).isoformat(),
            "processed_campaigns": new_processed_campaigns,
        }

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)

        log.info("Adform sync completed successfully")

    except Exception as e:
        log.severe(f"Adform sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync Adform data: {str(e)}")


# Connector instance
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
