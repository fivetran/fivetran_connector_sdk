"""AdMob API connector for syncing advertising data including network reports, mediation reports, and account information.
This connector demonstrates how to fetch data from AdMob API and upsert it into destination using memory-efficient streaming patterns.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# For adding jitter to retry delays
import random

# For making HTTP requests to AdMob API
import requests

# For implementing delays in retry logic and rate limiting
import time

# For handling dates and timestamps
from datetime import datetime, timedelta, timezone

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Private constants (use __ prefix)
__INVALID_LITERAL_ERROR = "invalid literal"
__API_ENDPOINT = "https://admob.googleapis.com/v1"
__OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"


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


def __refresh_oauth_token(client_id, client_secret, refresh_token):
    """
    Refresh OAuth2 access token using refresh token.
    This function handles OAuth2 token refresh for AdMob API authentication.

    Args:
        client_id: OAuth2 client identifier.
        client_secret: OAuth2 client secret.
        refresh_token: OAuth2 refresh token for generating new access tokens.

    Returns:
        str: New access token for API authentication.

    Raises:
        RuntimeError: If token refresh fails or invalid credentials provided.
    """
    try:
        data = {
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        }

        response = requests.post(__OAUTH_TOKEN_URL, data=data, timeout=30)
        response.raise_for_status()

        token_data = response.json()
        return token_data["access_token"]
    except Exception as e:
        log.severe(f"OAuth token refresh failed: {str(e)}")
        raise RuntimeError(f"Failed to refresh OAuth token: {str(e)}")


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


def execute_api_request(endpoint, access_token, params=None, configuration=None):
    """
    Execute HTTP API requests with comprehensive error handling and retry logic.
    This function handles authentication, rate limiting, timeouts, and network errors.

    Args:
        endpoint: API endpoint path to request.
        access_token: OAuth2 access token for API authentication.
        params: Query parameters for the request (optional).
        configuration: Configuration dictionary for timeout and retry settings.

    Returns:
        dict: Parsed JSON response from the API.

    Raises:
        RuntimeError: If all retry attempts fail or unexpected errors occur.
        requests.exceptions.RequestException: For unrecoverable HTTP errors.
    """
    url = f"{__API_ENDPOINT}{endpoint}"
    headers = {"Authorization": f"Bearer {access_token}"}

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


def __map_network_report_data(record, publisher_id):
    """
    Transform API response record to NETWORK_REPORTS table schema format.
    This function maps raw API fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.
        publisher_id: Publisher ID for context.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    row = record.get("row", {})
    dimension_values = row.get("dimensionValues", {})
    metric_values = row.get("metricValues", {})

    return {
        "id": f"{publisher_id}_{dimension_values.get('DATE', {}).get('value', '')}_{dimension_values.get('AD_UNIT', {}).get('value', '')}",
        "publisher_id": publisher_id,
        "date": dimension_values.get("DATE", {}).get("value", ""),
        "ad_unit_id": dimension_values.get("AD_UNIT", {}).get("value", ""),
        "ad_unit_name": dimension_values.get("AD_UNIT", {}).get("displayLabel", ""),
        "app_id": dimension_values.get("APP", {}).get("value", ""),
        "app_name": dimension_values.get("APP", {}).get("displayLabel", ""),
        "country": dimension_values.get("COUNTRY", {}).get("value", ""),
        "platform": dimension_values.get("PLATFORM", {}).get("value", ""),
        "ad_type": dimension_values.get("AD_TYPE", {}).get("value", ""),
        "estimated_earnings": (
            float(metric_values.get("ESTIMATED_EARNINGS", {}).get("microsValue", 0)) / 1000000
        ),
        "ad_requests": int(metric_values.get("AD_REQUESTS", {}).get("integerValue", 0)),
        "matched_requests": int(metric_values.get("MATCHED_REQUESTS", {}).get("integerValue", 0)),
        "show_rate": float(metric_values.get("SHOW_RATE", {}).get("doubleValue", 0)),
        "impressions": int(metric_values.get("IMPRESSIONS", {}).get("integerValue", 0)),
        "clicks": int(metric_values.get("CLICKS", {}).get("integerValue", 0)),
        "sync_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def __map_account_data(record):
    """
    Transform API response record to ACCOUNTS table schema format.
    This function maps raw account fields to normalized database column names and types.

    Args:
        record: Raw API response record dictionary.

    Returns:
        dict: Transformed record ready for database insertion.
    """
    return {
        "publisher_id": record.get("publisherId", ""),
        "name": record.get("name", ""),
        "currency_code": record.get("currencyCode", ""),
        "reporting_time_zone": record.get("reportingTimeZone", ""),
        "sync_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def get_accounts(access_token, configuration=None):
    """
    Fetch publisher accounts using memory-efficient streaming approach.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        access_token: OAuth2 access token for making requests.
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual account records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = "/accounts"
    response = execute_api_request(endpoint, access_token, {}, configuration)

    accounts = response.get("account", [])
    for account in accounts:
        yield __map_account_data(account)


def get_network_reports(access_token, publisher_id, last_sync_time=None, configuration=None):
    """
    Fetch network reports using memory-efficient streaming approach with pagination.
    This generator function prevents memory accumulation by yielding individual records.

    Args:
        access_token: OAuth2 access token for making requests.
        publisher_id: Publisher account ID for the reports.
        last_sync_time: Timestamp for incremental sync (optional).
        configuration: Configuration dictionary containing connector settings.

    Yields:
        dict: Individual report records mapped to destination schema.

    Raises:
        RuntimeError: If API requests fail after all retry attempts.
    """
    endpoint = f"/accounts/{publisher_id}/networkReport:generate"

    # AdMob API report request format for future implementation
    # report_spec = {
    #     "dateRange": {
    #         "startDate": {
    #             "year": int(time_range["start"][:4]),
    #             "month": int(time_range["start"][5:7]),
    #             "day": int(time_range["start"][8:10])
    #         },
    #         "endDate": {
    #             "year": int(time_range["end"][:4]),
    #             "month": int(time_range["end"][5:7]),
    #             "day": int(time_range["end"][8:10])
    #         }
    #     },
    #     "dimensions": ["DATE", "AD_UNIT", "APP", "COUNTRY", "PLATFORM", "AD_TYPE"],
    #     "metrics": ["ESTIMATED_EARNINGS", "AD_REQUESTS", "MATCHED_REQUESTS", "SHOW_RATE", "IMPRESSIONS", "CLICKS"],
    #     "localizationSettings": {
    #         "currencyCode": "USD",
    #         "languageCode": "en-US"
    #     }
    # }

    response = execute_api_request(endpoint, access_token, {}, configuration)

    # Handle report generation response
    if "row" in response:
        # Single page response
        for row in response.get("row", []):
            yield __map_network_report_data({"row": row}, publisher_id)
    else:
        # Handle paginated or streaming response
        rows = response.get("rows", [])
        for row in rows:
            yield __map_network_report_data({"row": row}, publisher_id)


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
        {"table": "accounts", "primary_key": ["publisher_id"]},
        {"table": "network_reports", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Main synchronization function that fetches and processes data from the AdMob API.
    This function orchestrates the entire sync process using memory-efficient streaming patterns.

    Args:
        configuration: Configuration dictionary containing API credentials and settings.
        state: State dictionary containing sync cursors and checkpoints from previous runs.

    Raises:
        RuntimeError: If sync fails due to API errors or configuration issues.
    """
    log.info("Starting AdMob API connector sync")

    # Extract configuration parameters (SDK auto-validates required fields)
    client_id = __get_config_str(configuration, "client_id")
    client_secret = __get_config_str(configuration, "client_secret")
    refresh_token = __get_config_str(configuration, "refresh_token")
    max_records_per_page = __get_config_int(configuration, "max_records_per_page", 100, 1, 1000)

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time")

    try:
        # Refresh OAuth token
        log.info("Refreshing OAuth2 access token...")
        access_token = __refresh_oauth_token(client_id, client_secret, refresh_token)

        # Fetch accounts data
        log.info("Fetching AdMob accounts...")
        account_count = 0
        publisher_ids = []

        for account in get_accounts(access_token, configuration):
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="accounts", data=account)
            publisher_ids.append(account["publisher_id"])
            account_count += 1

        log.info(f"Processed {account_count} accounts")

        # Fetch network reports for each publisher
        report_count = 0
        for publisher_id in publisher_ids:
            log.info(f"Fetching network reports for publisher {publisher_id}...")

            for report in get_network_reports(
                access_token, publisher_id, last_sync_time, configuration
            ):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="network_reports", data=report)
                report_count += 1

                # Checkpoint every batch to save progress incrementally
                if report_count % max_records_per_page == 0:
                    checkpoint_state = {
                        "last_sync_time": report.get(
                            "date", datetime.now(timezone.utc).isoformat()
                        ),
                        "last_processed_reports": report_count,
                    }
                    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
                    # from the correct position in case of next sync or interruptions.
                    # Learn more about how and where to checkpoint by reading our best practices documentation
                    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
                    op.checkpoint(checkpoint_state)

        # Final checkpoint with completion status
        final_state = {"last_sync_time": datetime.now(timezone.utc).isoformat()}
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info(
            f"Sync completed successfully. Processed {account_count} accounts and {report_count} report records."
        )

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync AdMob data: {str(e)}")


# Creating Connector instance
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
