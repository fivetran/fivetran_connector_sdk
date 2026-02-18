# Snapchat Custom Conversions Connector
"""This connector fetches custom conversion data from Snapchat's Marketing API
and ad account statistics from Snapchat's Measurement API, then upserts them into destination
using the Fivetran Connector SDK.

Supports OAuth2 authentication with automatic token refresh.
Requires Python 3.14+.
"""
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Import required libraries for API interactions
import requests
import json
import time
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Tuple, Iterator

# Snapchat API endpoints
SNAPCHAT_API_BASE_URL = "https://adsapi.snapchat.com/v1"
SNAPCHAT_TOKEN_URL = "https://accounts.snapchat.com/login/oauth2/access_token"

# Token expiration buffer (refresh token 5 minutes before it expires)
TOKEN_EXPIRATION_BUFFER_SECONDS = 300

# Snapchat API datetime format: YYYY-MM-DDTHH:00:00 (start of hour, no milliseconds)
API_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Process stats records in batches to avoid accumulating all in memory
PROCESS_BATCH_SIZE = 100_000


def get_stats_fields(custom_conversions: List[Dict[str, Any]] | None = None) -> str:
    """
    Returns comma-separated string of field names for stats API requests.
    Dynamically generates conversion fields from custom conversion IDs by prefixing with "conversion_".

    Args:
        custom_conversions: List of custom conversion dictionaries containing 'custom_conversion_id'
    Returns:
        Comma-separated string of field names
    """
    if not custom_conversions:
        log.warning("No custom conversion fields found")
        return ""

    # Generate conversion fields
    conversion_fields = [
        f"conversion_{conv.get('custom_conversion_id', '')}"
        for conv in custom_conversions
        if conv.get("custom_conversion_id")
    ]

    if not conversion_fields:
        log.warning("No custom conversion fields found")
        return ""

    log.info(f"Generated {len(conversion_fields)} conversion fields from custom conversions")
    return ",".join(conversion_fields)


def _create_datetime_at_midnight(date_obj: date) -> datetime:
    """Helper function to create datetime object at exactly midnight (00:00:00)."""
    return datetime(
        date_obj.year,
        date_obj.month,
        date_obj.day,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )


def _format_datetime_for_api(dt: datetime) -> str:
    """Format datetime for API: YYYY-MM-DDTHH:00:00."""
    return dt.strftime(API_DATETIME_FORMAT)


def _extract_days_from_attribution_window(window_str: str) -> int:
    """
    Extract number of days from attribution window string.

    Args:
        window_str: Attribution window string (e.g., "28_DAY", "7_DAY", "1_DAY")
    Returns:
        Number of days as integer, or 0 if parsing fails or format is invalid
    """
    if "_DAY" in window_str:
        try:
            return int(window_str.split("_")[0])
        except ValueError:
            return 0
    return 0


def get_date_range_for_stats(state: dict, configuration: dict) -> Tuple[str, str]:
    """
    Determines start_time and end_time for stats API call using incremental date range.
    For initial sync: uses initial_sync_days from config.
    For incremental sync: uses incremental_lookback_start and incremental_lookback_end dates from config.

    The function limits the effective date range to 30 days per API call to avoid timeouts.
    For larger ranges, the connector will make multiple calls automatically.

    Args:
        state: State dictionary containing last_stats_sync_time
        configuration: Configuration dictionary containing:
            - initial_sync_days: For initial sync
            - incremental_lookback_start: Start date for incremental sync (YYYY-MM-DD format)
            - incremental_lookback_end: End date for incremental sync (YYYY-MM-DD format)
    Returns:
        Tuple of (start_time, end_time) in YYYY-MM-DDTHH:00:00 format (Snapchat API compatible)
    """
    # Get end_time: tomorrow at midnight (current date + 1 day)
    today = date.today()
    tomorrow_date = today + timedelta(days=1)
    end_time_dt = _create_datetime_at_midnight(tomorrow_date)
    end_time = _format_datetime_for_api(end_time_dt)

    # Get start_time based on whether this is initial or incremental sync
    last_stats_sync_time = state.get("last_stats_sync_time")

    if last_stats_sync_time:
        # Incremental sync: use incremental_lookback_start and incremental_lookback_end dates
        try:
            # Get dates from config
            incremental_lookback_start_str = configuration.get("incremental_lookback_start")
            incremental_lookback_end_str = configuration.get("incremental_lookback_end")

            # parse the dates, if they're invalid, raise an error
            lookback_start_date = date.fromisoformat(incremental_lookback_start_str)
            lookback_end_date = date.fromisoformat(incremental_lookback_end_str)

            # Use the lookback dates for incremental sync
            # Ensure start_date doesn't go before lookback_start
            start_date = lookback_start_date

            # Use lookback_end as the end_time (but ensure it's not in the future)
            if lookback_end_date > tomorrow_date:
                end_time_dt = _create_datetime_at_midnight(tomorrow_date)
                end_time = _format_datetime_for_api(end_time_dt)
                log.info(
                    f"incremental_lookback_end ({lookback_end_date}) is in the future, "
                    f"using tomorrow ({tomorrow_date}) as end_time"
                )
            else:
                # Use lookback_end + 1 day as end_time (API requires end_time to be exclusive)
                end_time_dt = _create_datetime_at_midnight(lookback_end_date + timedelta(days=1))
                end_time = _format_datetime_for_api(end_time_dt)

            # Snapchat API requires end_time > start_time. Ensure we never return start >= end.
            if start_date >= lookback_end_date:
                start_date = lookback_end_date - timedelta(days=1)
                log.warning(
                    f"Adjusted start_date to {start_date} (start >= end, ensuring valid range)"
                )

            start_time_dt = _create_datetime_at_midnight(start_date)
            start_time = _format_datetime_for_api(start_time_dt)

            log.info(
                f"Incremental stats sync: start_time={start_time}, end_time={end_time} "
                f"(lookback range: {lookback_start_date} to {lookback_end_date})"
            )
        except Exception as e:
            log.severe(f"Error processing incremental sync dates, exiting connector: {str(e)}")
            raise ValueError(
                f"Error processing incremental sync dates, exiting connector: {str(e)}"
            ) from e
    else:
        # Initial sync: use initial_sync_days
        initial_sync_days = int(configuration.get("initial_sync_days", "732"))
        # end_time is tomorrow; start_date is initial_sync_days before today
        start_date = tomorrow_date - timedelta(days=initial_sync_days)
        # Snapchat API requires end_time > start_time
        if start_date >= tomorrow_date:
            start_date = tomorrow_date - timedelta(days=1)
        start_time_dt = _create_datetime_at_midnight(start_date)
        start_time = _format_datetime_for_api(start_time_dt)

        log.info(
            f"Initial stats sync: start_time={start_time}, end_time={end_time} (last {initial_sync_days} days)"
        )

    return start_time, end_time


def split_date_range_if_needed(
    start_time: str, end_time: str, configuration: dict
) -> List[Tuple[str, str]]:
    """
    Splits a date range into smaller chunks if it exceeds the maximum safe range.

    This function calculates the effective lookback period (date range + attribution window)
    and adjusts chunk size accordingly.

    Args:
        start_time: Start time in ISO 8601 format
        end_time: End time in ISO 8601 format
        configuration: Configuration dictionary containing attribution window settings
    Returns:
        List of (start_time, end_time) tuples, each representing a chunk to fetch
    """
    # Parse dates
    start_dt = datetime.fromisoformat(start_time)
    end_dt = datetime.fromisoformat(end_time)

    start_date_only = start_dt.date()
    end_date_only = end_dt.date()

    start_dt = _create_datetime_at_midnight(start_date_only)
    end_dt = _create_datetime_at_midnight(end_date_only)

    # Calculate total days
    total_days = (end_dt - start_dt).days

    # Snapchat API requires end_time > start_time. Skip when range is empty or negative.
    if total_days <= 0:
        log.info(
            f"Skipping stats fetch: date range is empty or invalid (start={start_time}, end={end_time})"
        )
        return []

    # Get attribution windows from configuration
    swipe_up_attribution_window = configuration.get("swipe_up_attribution_window", "7_DAY")
    view_attribution_window = configuration.get("view_attribution_window", "1_DAY")

    # Extract days from attribution window strings (e.g., "28_DAY" -> 28)
    swipe_window_days = _extract_days_from_attribution_window(swipe_up_attribution_window)
    view_window_days = _extract_days_from_attribution_window(view_attribution_window)

    # Use the maximum attribution window to determine safe chunk size
    max_attribution_days = max(swipe_window_days, view_window_days)

    # Maximum effective lookback period we want to allow (date range + attribution window)
    # This is the total period the API needs to process
    max_effective_lookback_days = 30

    # Calculate maximum safe chunk size based on attribution window
    # The chunk size must ensure: chunk_size + attribution_window <= max_effective_lookback_days
    if max_attribution_days >= 28:
        # For 28-day attribution: use 1-day chunks (1 + 28 = 29 days effective lookback)
        max_days_per_call = 1
    elif max_attribution_days >= 7:
        # For 7-day attribution: use 7-day chunks (7 + 7 = 14 days effective lookback)
        max_days_per_call = 7
    elif max_attribution_days >= 1:
        # For 1-day attribution: use 7-day chunks (7 + 1 = 8 days effective lookback)
        max_days_per_call = 7
    else:
        # For no attribution window: use 7-day chunks as safe default
        max_days_per_call = 7

    # Ensure we don't exceed the effective lookback limit
    if max_days_per_call + max_attribution_days > max_effective_lookback_days:
        max_days_per_call = max_effective_lookback_days - max_attribution_days
        max_days_per_call = max(1, max_days_per_call)  # At least 1 day

    log.info(
        f"Attribution windows: swipe_up={swipe_up_attribution_window} ({swipe_window_days} days), "
        f"view={view_attribution_window} ({view_window_days} days)"
    )
    log.info(f"Total date range: {total_days} days (from {start_time} to {end_time})")
    log.info(
        f"Using chunk size of {max_days_per_call} days to keep effective lookback period "
        f"({max_days_per_call} + {max_attribution_days} = {max_days_per_call + max_attribution_days} days)"
    )

    if total_days <= max_days_per_call:
        # Range is small enough, return as-is
        log.info(
            f"Date range ({total_days} days) is within safe limit ({max_days_per_call} days), no chunking needed"
        )
        return [(start_time, end_time)]

    # Split into chunks
    chunks = []
    current_start = start_dt

    log.info(f"Splitting date range ({total_days} days) into chunks of {max_days_per_call} days")

    while current_start < end_dt:
        # Calculate chunk end (max_days_per_call days later, or end_time if smaller)
        chunk_end_calc = min(current_start + timedelta(days=max_days_per_call), end_dt)

        chunk_start_date = current_start.date()
        chunk_end_date = chunk_end_calc.date()

        chunk_start_dt = _create_datetime_at_midnight(chunk_start_date)
        chunk_end_dt = _create_datetime_at_midnight(chunk_end_date)

        chunk_start_str = _format_datetime_for_api(chunk_start_dt)
        chunk_end_str = _format_datetime_for_api(chunk_end_dt)

        chunks.append((chunk_start_str, chunk_end_str))
        log.info(f"Chunk: {chunk_start_str} to {chunk_end_str}")

        # Move to next chunk (start from chunk_end to ensure no gaps)
        # Since chunk_end is already end date + 1 (exclusive), we start the next chunk from chunk_end
        current_start = chunk_end_dt

    return chunks


def fetch_ad_accounts_from_organization(
    organization_id: str,
    configuration: dict,
    state: dict,
    timeout_seconds: int = 30,
    retry_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> Dict[str, Dict[str, str]]:
    """
    Fetches ad account IDs and names from a Snapchat organization using the Organizations API.

    Uses the /me/organizations endpoint with with_ad_accounts=true parameter to get
    all organizations the user has access to, along with their ad accounts.

    Args:
        organization_id: Snapchat Organization ID
        access_token: Valid access token
        configuration: Configuration dictionary
        state: State dictionary
        timeout_seconds: Request timeout in seconds
        retry_attempts: Number of retry attempts on failure
        retry_delay_seconds: Delay between retry attempts
    Returns:
        Dictionary mapping ad_account_id -> {"name": "..."}
    """
    # Use /me/organizations endpoint with with_ad_accounts=true parameter
    endpoint = f"{SNAPCHAT_API_BASE_URL}/me/organizations?with_ad_accounts=true"

    log.info(
        f"Fetching ad accounts from organizations API (filtering by organization_id: {organization_id})"
    )

    try:
        response_data = execute_authenticated_request(
            endpoint,
            configuration,
            state,
            timeout_seconds,
            retry_attempts,
            retry_delay_seconds,
        )

        ad_accounts_info = {}  # Dictionary: account_id -> {"name": "..."}

        # Parse the response structure: organizations -> organization -> ad_accounts
        if "organizations" in response_data:
            for org_item in response_data["organizations"]:
                organization = org_item.get("organization", {})
                org_id = organization.get("id", "")

                # Filter by organization_id if provided
                if organization_id and org_id != organization_id:
                    continue

                # Extract ad_accounts from this organization
                for ad_account in organization.get("ad_accounts", []):
                    ad_account_id = ad_account.get("id", "")
                    if not ad_account_id:
                        continue

                    account_info = {"name": ad_account.get("name", "") or ""}
                    ad_accounts_info[ad_account_id] = account_info
                    log.info(f"Found ad account: {ad_account_id} (name: {account_info['name']})")

        if not ad_accounts_info:
            if organization_id:
                log.warning(f"No ad accounts found for organization_id: {organization_id}")
            else:
                log.warning("No ad accounts found in any organization")

        log.info(f"Retrieved {len(ad_accounts_info)} ad account(s) from organization(s)")
        return ad_accounts_info

    except Exception as e:
        log.warning(f"Failed to fetch ad accounts from organization: {str(e)}")
        raise RuntimeError(f"Failed to fetch ad accounts and no fallback configured: {str(e)}")


def _validate_int_config(
    configuration: dict,
    key: str,
    default: str,
    min_value: int,
    max_value: int,
    error_message: str,
) -> int:
    """
    Validate and return an integer configuration value.

    Args:
        configuration: Configuration dictionary
        key: Configuration key name
        default: Default value as string
        min_value: Minimum allowed value (inclusive)
        max_value: Maximum allowed value (inclusive)
        error_message: Error message template
    Returns:
        Validated integer value
    Raises:
        ValueError: If value is invalid or out of range
    """
    try:
        value = int(configuration.get(key, default))
        if value < min_value or value > max_value:
            raise ValueError(error_message)
        return value
    except (ValueError, TypeError):
        raise ValueError(error_message)


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    required_configs = ["client_id", "client_secret", "pixel_id", "organization_id"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")
        if not str(configuration.get(key, "")).strip():
            raise ValueError(f"{key} cannot be empty")

    # Validate that we have either access_token or refresh_token
    has_access_token = (
        configuration.get("access_token") and str(configuration.get("access_token", "")).strip()
    )
    has_refresh_token = (
        configuration.get("refresh_token") and str(configuration.get("refresh_token", "")).strip()
    )

    if not has_access_token and not has_refresh_token:
        raise ValueError("Either access_token or refresh_token must be provided")

    # Validate optional configuration parameters (convert from strings)
    timeout_seconds = _validate_int_config(
        configuration,
        "request_timeout_seconds",
        "30",
        5,
        300,
        "request_timeout_seconds must be a valid integer between 5 and 300",
    )

    retry_attempts = _validate_int_config(
        configuration,
        "retry_attempts",
        "3",
        0,
        10,
        "retry_attempts must be a valid integer between 0 and 10",
    )

    retry_delay_seconds = _validate_int_config(
        configuration,
        "retry_delay_seconds",
        "5",
        1,
        60,
        "retry_delay_seconds must be a valid integer between 1 and 60",
    )

    # Validate optional stats configuration parameters
    initial_sync_days = _validate_int_config(
        configuration,
        "initial_sync_days",
        "90",
        1,
        732,
        "initial_sync_days must be a valid integer between 1 and 732",
    )

    # Validate stats_granularity
    valid_granularities = ["TOTAL", "DAY", "HOUR"]
    stats_granularity = configuration.get("stats_granularity", "DAY")
    if stats_granularity not in valid_granularities:
        raise ValueError(f"stats_granularity must be one of: {valid_granularities}")

    # Validate stats_breakdown
    valid_breakdowns = ["ad", "adsquad", "campaign"]
    stats_breakdown = configuration.get("stats_breakdown", "ad")
    if stats_breakdown not in valid_breakdowns:
        raise ValueError(f"stats_breakdown must be one of: {valid_breakdowns}")


def get_access_token(
    configuration: dict,
    state: dict,
    timeout_seconds: int = 30,
    retry_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> str:
    """
    Get a valid access token, refreshing if necessary.
    This function checks if the current access token is expired or about to expire,
    and refreshes it using the refresh token if needed.
    Args:
        configuration: Configuration dictionary containing client credentials
        state: State dictionary that may contain token information
        timeout_seconds: Request timeout in seconds
        retry_attempts: Number of retry attempts on failure
        retry_delay_seconds: Delay between retry attempts
    Returns:
        Valid access token string
    Raises:
        RuntimeError: If token refresh fails
    """
    client_id = str(configuration["client_id"])
    client_secret = str(configuration["client_secret"])

    # Check state first for stored tokens
    access_token = state.get("access_token") or configuration.get("access_token")
    refresh_token = state.get("refresh_token") or configuration.get("refresh_token")
    token_expires_at = state.get("token_expires_at")

    # If we have a token and expiration info, check if it's still valid
    if access_token and token_expires_at:
        try:
            # token_expires_at should be a Unix timestamp (float) stored from time.time() + expires_in
            # Convert to float in case it was stored as a string (e.g., from JSON state)
            expires_at_timestamp = float(token_expires_at)
            current_time = time.time()

            # Validate that expires_at_timestamp is a reasonable Unix timestamp
            # Unix timestamps are typically > 1000000000 (year 2001) and < 2147483647 (year 2038)
            if expires_at_timestamp < 1000000000 or expires_at_timestamp > 2147483647:
                raise ValueError(
                    f"token_expires_at value {expires_at_timestamp} doesn't look like a Unix timestamp"
                )

            # Check if token is still valid (with buffer)
            time_until_expiry = expires_at_timestamp - current_time
            if time_until_expiry > TOKEN_EXPIRATION_BUFFER_SECONDS:
                log.info(
                    f"Using existing valid access token (expires in {time_until_expiry:.0f} seconds, "
                    f"buffer: {TOKEN_EXPIRATION_BUFFER_SECONDS} seconds)"
                )
                return str(access_token)
            else:
                log.info(
                    f"Token expires soon (in {time_until_expiry:.0f} seconds), refreshing proactively"
                )
        except (ValueError, TypeError) as e:
            log.warning(
                f"Invalid token_expires_at value '{token_expires_at}' (type: {type(token_expires_at)}), "
                f"will refresh token: {str(e)}"
            )
            # Fall through to token refresh logic

    # If we have access_token but no expiration info, refresh to get expiration time
    if access_token and not token_expires_at and refresh_token:
        log.info("Access token found but no expiration info, refreshing to get expiration time")

    # Need to refresh the token
    if not refresh_token:
        raise ValueError(
            "No refresh token available and access token is expired or missing expiration info"
        )

    log.info("Access token expired or about to expire, refreshing...")

    # Refresh the token with retry logic
    refresh_data = {
        "grant_type": "refresh_token",
        "refresh_token": str(refresh_token),
        "client_id": client_id,
        "client_secret": client_secret,
    }

    last_exception = None
    for attempt in range(retry_attempts + 1):
        try:
            response = requests.post(
                SNAPCHAT_TOKEN_URL,
                data=refresh_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=timeout_seconds,
            )
            response.raise_for_status()

            token_response = response.json()
            new_access_token = token_response.get("access_token")
            new_refresh_token = token_response.get("refresh_token", refresh_token)
            expires_in = token_response.get("expires_in", 3600)

            if not new_access_token:
                raise RuntimeError("Token refresh response did not contain access_token")

            # Calculate expiration timestamp
            expires_at = time.time() + expires_in

            # Update state with new tokens
            state["access_token"] = new_access_token
            state["refresh_token"] = new_refresh_token
            state["token_expires_at"] = expires_at  # Store as float

            log.info(
                f"Successfully refreshed access token (expires in {expires_in} seconds, "
                f"expires_at={expires_at}, current_time={time.time()})"
            )

            # Verify the token was actually updated
            if state.get("access_token") != new_access_token:
                log.warning("⚠️  Token state update may have failed - access_token mismatch")

            return str(new_access_token)

        except requests.exceptions.RequestException as e:
            last_exception = e
            if attempt < retry_attempts:
                log.warning(
                    f"Token refresh failed (attempt {attempt + 1}/{retry_attempts + 1}): {str(e)}"
                )
                time.sleep(retry_delay_seconds * (2**attempt))  # Exponential backoff
            else:
                log.severe(
                    f"Failed to refresh access token after {retry_attempts + 1} attempts: {str(e)}"
                )
                if hasattr(e, "response") and e.response is not None:
                    log.severe(
                        f"Response status: {e.response.status_code}, body: {e.response.text}"
                    )
                raise RuntimeError(
                    f"Token refresh failed after {retry_attempts + 1} attempts: {str(e)}"
                )

    # This should never be reached
    raise RuntimeError(f"Token refresh failed: {str(last_exception)}")


def execute_authenticated_request(
    url: str,
    configuration: dict,
    state: dict,
    timeout_seconds: int = 30,
    retry_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> Dict[str, Any]:
    """
    Execute an authenticated API request against Snapchat API with retry logic and automatic token refresh.

    A fresh token is always fetched from state/config before each request to ensure it's valid and up-to-date.

    Args:
        url: The API endpoint URL to call
        configuration: Configuration dictionary
        state: State dictionary (for token refresh)
        timeout_seconds: Request timeout in seconds
        retry_attempts: Number of retry attempts on failure
        retry_delay_seconds: Delay between retry attempts
    Returns:
        The response data from the API
    """
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    last_exception = None

    # Get token once at the start of the request
    current_access_token = get_access_token(
        configuration,
        state,
        timeout_seconds,
        retry_attempts,
        retry_delay_seconds,
    )

    for attempt in range(retry_attempts + 1):
        try:
            headers["Authorization"] = f"Bearer {current_access_token}"
            response = requests.get(url, headers=headers, timeout=timeout_seconds)

            # Handle 401 Unauthorized - token may have expired
            if response.status_code == 401 and attempt < retry_attempts:
                log.warning("Received 401 Unauthorized, refreshing token and retrying...")
                # Force token refresh by clearing expiration time
                # This ensures get_access_token will refresh the token
                if "token_expires_at" in state:
                    del state["token_expires_at"]

                # Use separate retry parameters for token refresh to avoid exhausting main retries
                # Max 2 attempts for token refresh
                token_refresh_attempts = min(2, retry_attempts)
                current_access_token = get_access_token(
                    configuration,
                    state,
                    timeout_seconds,
                    token_refresh_attempts,
                    retry_delay_seconds,
                )
                # Persist the refreshed token state immediately
                op.checkpoint(state)
                log.info("Token refreshed and state checkpointed after 401 error")
                continue  # Retry with new token

            # Handle 400 Bad Request errors with detailed logging
            if response.status_code == 400:
                error_body = response.text
                log.warning(f"⚠️  400 Bad Request error for URL: {url[:300]}...")
                log.warning(f"Response status: 400, body: {error_body}")
                raise RuntimeError(f"400 Bad Request: {error_body[:500]}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            last_exception = e
            if attempt < retry_attempts:
                log.warning(
                    f"API request failed (attempt {attempt + 1}/{retry_attempts + 1}): {str(e)}"
                )
                # If it's a 401, try refreshing token
                if (
                    hasattr(e, "response")
                    and e.response is not None
                    and e.response.status_code == 401
                ):
                    log.warning("401 error detected in exception handler, refreshing token...")
                    # Force token refresh by clearing expiration time
                    if "token_expires_at" in state:
                        del state["token_expires_at"]

                    # Use separate retry parameters for token refresh to avoid exhausting main retries
                    # Max 2 attempts for token refresh
                    token_refresh_attempts = min(2, retry_attempts)
                    current_access_token = get_access_token(
                        configuration,
                        state,
                        timeout_seconds,
                        token_refresh_attempts,
                        retry_delay_seconds,
                    )
                    # Persist the refreshed token state immediately
                    op.checkpoint(state)
                    log.info("Token refreshed and state checkpointed after 401 exception")
                elif (
                    hasattr(e, "response")
                    and e.response is not None
                    and e.response.status_code == 400
                ):
                    # Don't retry on 400 errors
                    error_body = e.response.text
                    log.warning(f"⚠️  400 Bad Request error (not retrying): {error_body[:500]}")
                    raise RuntimeError(f"400 Bad Request: {error_body[:500]}")
                else:
                    time.sleep(retry_delay_seconds * (2**attempt))  # Exponential backoff
            else:
                log.severe(
                    f"Failed to execute API request after {retry_attempts + 1} attempts: {str(e)}"
                )
                if hasattr(e, "response") and e.response is not None:
                    log.severe(
                        f"Response status: {e.response.status_code}, body: {e.response.text}"
                    )
                raise RuntimeError(
                    f"API request failed after {retry_attempts + 1} attempts: {str(e)}"
                )

    # This should never be reached
    raise RuntimeError(f"API request failed: {str(last_exception)}")


def fetch_custom_conversions(
    pixel_id: str,
    configuration: dict,
    state: dict,
    timeout_seconds: int = 30,
    retry_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> List[Dict[str, Any]]:
    """
    Fetch custom conversions from Snapchat API.
    Args:
        pixel_id: Snapchat Pixel ID
        configuration: Configuration dictionary
        state: State dictionary
        timeout_seconds: Request timeout in seconds
        retry_attempts: Number of retry attempts on failure
        retry_delay_seconds: Delay between retry attempts
    Returns:
        List of custom conversion records
    """
    endpoint = f"{SNAPCHAT_API_BASE_URL}/pixels/{pixel_id}/custom_conversions"

    log.info(f"Fetching custom conversions for pixel_id: {pixel_id}")

    response_data = execute_authenticated_request(
        endpoint,
        configuration,
        state,
        timeout_seconds,
        retry_attempts,
        retry_delay_seconds,
    )

    custom_conversions = []

    # Parse the response structure
    for item in response_data.get("custom_conversions", []):
        custom_conversion_data = item.get("custom_conversion", {})
        if not custom_conversion_data:
            continue

        event_source = custom_conversion_data.get("event_source", {})
        custom_conversions.append(
            {
                "custom_conversion_id": custom_conversion_data.get("id", ""),
                "custom_conversion_name": custom_conversion_data.get("name", ""),
                "event_source_id": event_source.get("id", ""),
                "event_source_type": event_source.get("type", ""),
                "sub_request_status": item.get("sub_request_status", ""),
            }
        )

    log.info(f"Retrieved {len(custom_conversions)} custom conversions")
    return custom_conversions


def chunk_custom_conversions(
    custom_conversions: List[Dict[str, Any]], conversions_per_call: int = 500
) -> List[List[Dict[str, Any]]]:
    """
    Split custom conversions into chunks for API calls.

    Max limit is 607 fields, so 500 provides a safe margin.
    If we have fewer than conversions_per_call conversions, use all of them in a single call.

    Args:
        custom_conversions: List of custom conversion dictionaries
        conversions_per_call: Maximum number of conversions per API call (default: 500)
    Returns:
        List of conversion chunks, each chunk is a list of conversion dictionaries
    Raises:
        ValueError: If no custom conversions are provided
    """
    if not custom_conversions:
        log.warning("No custom conversions found, exiting the connector")
        raise ValueError("No custom conversions found, exiting the connector")

    num_conversions = len(custom_conversions)
    if num_conversions < conversions_per_call:
        # Use all conversions in a single call (no chunking needed)
        log.info(
            f"Using all {num_conversions} custom conversions in a single API call (below chunk size limit)"
        )
        return [custom_conversions]
    else:
        # Split into chunks
        conversion_chunks = []
        for i in range(0, num_conversions, conversions_per_call):
            chunk = custom_conversions[i : i + conversions_per_call]
            conversion_chunks.append(chunk)
        log.info(
            f"Split {num_conversions} custom conversions into {len(conversion_chunks)} chunk(s) "
            f"of up to {conversions_per_call} conversions each"
        )
        return conversion_chunks


def fetch_stats_for_ad_account(
    ad_account_id: str,
    account_name: str,
    date_chunks: List[Tuple[str, str]],
    conversion_chunk_fields: List[str],
    conversion_chunk_mappings: List[Dict[str, Dict[str, str]]],
    configuration: dict,
    state: dict,
    timeout_seconds: int = 30,
    retry_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> Iterator[List[Dict[str, Any]]]:
    """
    Fetch stats for a single ad_account_id from Snapchat Measurement API.

    Yields batches of up to PROCESS_BATCH_SIZE records to avoid accumulating all in memory.
    Date ranges are pre-chunked to avoid API timeouts with large ranges + attribution windows.

    Args:
        ad_account_id: Snapchat Ad Account ID
        account_name: Account name from organization API
        date_chunks: Pre-chunked list of (start_time, end_time) tuples for date ranges
        conversion_chunk_fields: Pre-generated fields strings for each conversion chunk
        conversion_chunk_mappings: Pre-generated conversion_field_to_info mappings for each conversion chunk
        configuration: Configuration dictionary
        state: State dictionary (for token management)
        timeout_seconds: Request timeout in seconds
        retry_attempts: Number of retry attempts on failure
        retry_delay_seconds: Delay between retry attempts
    Yields:
        Batches of up to PROCESS_BATCH_SIZE flattened stats records (avoids accumulating all in memory)
    """

    # Process each combination of date chunk and conversion chunk, yield batches to avoid memory accumulation
    buffer: List[Dict[str, Any]] = []
    total_calls = len(date_chunks) * len(conversion_chunk_fields)
    call_number = 0

    for date_idx, (chunk_start, chunk_end) in enumerate(date_chunks, 1):
        for conv_idx in range(len(conversion_chunk_fields)):
            call_number += 1
            log.info(
                f"Processing call {call_number}/{total_calls}: "
                f"date chunk {date_idx}/{len(date_chunks)}, "
                f"conversion chunk {conv_idx + 1}/{len(conversion_chunk_fields)}"
            )

            # Use pre-generated fields string and mapping for this conversion chunk
            fields = conversion_chunk_fields[conv_idx]
            conversion_field_to_info = conversion_chunk_mappings[conv_idx]

            chunk_records = _fetch_stats_for_date_range(
                ad_account_id,
                account_name,
                chunk_start,
                chunk_end,
                fields,
                conversion_field_to_info,
                configuration,
                state,
                timeout_seconds,
                retry_attempts,
                retry_delay_seconds,
            )
            log.info(
                f"Call {call_number}/{total_calls} completed: "
                f"fetched {len(chunk_records)} records"
            )
            buffer.extend(chunk_records)

            # Yield full batches to avoid accumulating in memory
            while len(buffer) >= PROCESS_BATCH_SIZE:
                batch = buffer[:PROCESS_BATCH_SIZE]
                buffer = buffer[PROCESS_BATCH_SIZE:]
                log.info(f"Yielding batch of {len(batch)} records for processing")
                yield batch

    # Yield any remaining records
    if buffer:
        log.info(f"Yielding final batch of {len(buffer)} records")
        yield buffer


def _fetch_stats_for_date_range(
    ad_account_id: str,
    account_name: str,
    start_time: str,
    end_time: str,
    fields: str,
    conversion_field_to_info: Dict[str, Dict[str, str]],
    configuration: dict,
    state: dict,
    timeout_seconds: int = 30,
    retry_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> List[Dict[str, Any]]:
    """
    Internal function to fetch stats for a specific date range (single chunk).

    Args:
        ad_account_id: Snapchat Ad Account ID
        account_name: Account name from organization API
        start_time: Start time in ISO 8601 format
        end_time: End time in ISO 8601 format
        fields: Pre-generated comma-separated fields string for the API request
        conversion_field_to_info: Pre-generated mapping from field names (e.g., "conversion_123") to conversion info
        configuration: Configuration dictionary
        state: State dictionary (for token management)
        timeout_seconds: Request timeout in seconds
        retry_attempts: Number of retry attempts on failure
        retry_delay_seconds: Delay between retry attempts
    Returns:
        List of flattened stats records
    """
    # Get configuration parameters
    granularity = configuration.get("stats_granularity", "DAY")
    breakdown = configuration.get("stats_breakdown", "ad")
    swipe_up_attribution_window = configuration.get("swipe_up_attribution_window", "7_DAY")
    view_attribution_window = configuration.get("view_attribution_window", "1_DAY")

    url = f"{SNAPCHAT_API_BASE_URL}/adaccounts/{ad_account_id}/stats?start_time={start_time}&end_time={end_time}&granularity={granularity}&breakdown={breakdown}&fields={fields}&swipe_up_attribution_window={swipe_up_attribution_window}&view_attribution_window={view_attribution_window}"

    log.info(
        f"Fetching stats for ad_account_id={ad_account_id}, granularity={granularity}, breakdown={breakdown}"
    )
    log.info(f"Date range: {start_time} to {end_time}")

    stats_records = []
    next_page_url = url

    # Handle pagination - fetch all pages of results
    while next_page_url:
        # Fetch stats data
        response_data = execute_authenticated_request(
            next_page_url,
            configuration,
            state,
            timeout_seconds,
            retry_attempts,
            retry_delay_seconds,
        )
        # Get next page link if available
        paging = response_data.get("paging", {})
        next_page_url = paging.get("next_link", "")
        if next_page_url:
            log.info(f"Found next page, continuing pagination...")

        # Parse response based on granularity
        # API returns nested structure: timeseries_stats[].timeseries_stat.breakdown_stats.ad[].timeseries
        if granularity == "DAY" and "timeseries_stats" in response_data:
            for ts_item in response_data.get("timeseries_stats", []):
                ts_stat = ts_item.get("timeseries_stat", ts_item)

                # With breakdown=ad, API nests ads under breakdown_stats.ad[]
                breakdown_ads = ts_stat.get("breakdown_stats", {}).get(breakdown, [])
                if breakdown_ads:
                    for ad_item in breakdown_ads:
                        ad_id = ad_item.get("id", "")
                        ad_timeseries = ad_item.get("timeseries", [])

                        for ad_stat in ad_timeseries:
                            data_start_time = ad_stat.get("start_time", "")
                            data_end_time = ad_stat.get("end_time", "")
                            ad_stat_stats = ad_stat.get("stats", {})

                            for metric, value in ad_stat_stats.items():
                                if not (metric.startswith("conversion_") and value is not None):
                                    continue

                                try:
                                    conversions_value = float(value)
                                    if conversions_value <= 0:
                                        continue

                                    # Get custom conversion info from mapping
                                    conv_info = conversion_field_to_info.get(metric, {})
                                    custom_conversion_id = conv_info.get(
                                        "custom_conversion_id", ""
                                    )
                                    custom_conversion_name = conv_info.get(
                                        "custom_conversion_name", ""
                                    )

                                    # Remove "conversion_" prefix from conversion_tag_id
                                    conversion_tag_id = custom_conversion_id or metric.replace(
                                        "conversion_", ""
                                    )

                                    stats_records.append(
                                        {
                                            "ad_id": ad_id,
                                            "start_time": data_start_time,
                                            "end_time": data_end_time,
                                            "campaign_advertiser_id": ad_account_id,
                                            "campaign_advertiser": account_name,
                                            "conversion_tag_id": conversion_tag_id,
                                            "conversion_tag_name": custom_conversion_name,
                                            "conversions": conversions_value,
                                        }
                                    )
                                except (ValueError, TypeError):
                                    pass
                else:
                    # No breakdown: timeseries_stat has timeseries directly
                    ad_id = ts_stat.get("id", "")
                    ad_timeseries = ts_stat.get("timeseries", [])

                    for ad_stat in ad_timeseries:
                        data_start_time = ad_stat.get("start_time", "")
                        data_end_time = ad_stat.get("end_time", "")
                        ad_stat_stats = ad_stat.get("stats", {})

                        for metric, value in ad_stat_stats.items():
                            if not (metric.startswith("conversion_") and value is not None):
                                continue

                            try:
                                conversions_value = float(value)
                                if conversions_value <= 0:
                                    continue

                                # Get custom conversion info from mapping
                                conv_info = conversion_field_to_info.get(metric, {})
                                custom_conversion_id = conv_info.get("custom_conversion_id", "")
                                custom_conversion_name = conv_info.get(
                                    "custom_conversion_name", ""
                                )

                                # Remove "conversion_" prefix from conversion_tag_id
                                conversion_tag_id = custom_conversion_id or metric.replace(
                                    "conversion_", ""
                                )

                                stats_records.append(
                                    {
                                        "ad_id": ad_id,
                                        "start_time": data_start_time,
                                        "end_time": data_end_time,
                                        "campaign_advertiser_id": ad_account_id,
                                        "campaign_advertiser": account_name,
                                        "conversion_tag_id": conversion_tag_id,
                                        "conversion_tag_name": custom_conversion_name,
                                        "conversions": conversions_value,
                                    }
                                )
                            except (ValueError, TypeError):
                                pass

    log.info(f"Retrieved {len(stats_records)} stats records for ad_account_id={ad_account_id}")
    return stats_records


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    # Build columns for ad_account_stats table
    stats_columns = {
        "ad_id": "STRING",
        "start_time": "STRING",
        "end_time": "STRING",
    }

    # Add conversion metric field as FLOAT
    stats_columns["conversions"] = "FLOAT"

    # Add additional fields
    stats_columns["campaign_advertiser_id"] = "STRING"
    stats_columns["campaign_advertiser"] = "STRING"
    stats_columns["conversion_tag_id"] = "STRING"
    stats_columns["conversion_tag_name"] = "STRING"

    return [
        {
            "table": "custom_conversions",
            "primary_key": ["custom_conversion_id"],
            "columns": {
                "custom_conversion_id": "STRING",
                "custom_conversion_name": "STRING",
                "event_source_id": "STRING",
                "event_source_type": "STRING",
                "sub_request_status": "STRING",
            },
        },
        {
            "table": "ad_account_stats",
            "primary_key": [
                "campaign_advertiser_id",
                "ad_id",
                "start_time",
                "end_time",
                "conversion_tag_id",
            ],
            "columns": stats_columns,
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is called by Fivetran during each sync.
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
    """
    log.info("Starting Snapchat Custom Conversions connector sync")

    # Validate the configuration
    validate_configuration(configuration=configuration)

    # Extract configuration parameters
    pixel_id = str(configuration["pixel_id"])

    # Extract optional configuration parameters with defaults and convert from strings
    timeout_seconds = int(configuration.get("request_timeout_seconds", "30"))
    retry_attempts = int(configuration.get("retry_attempts", "3"))
    retry_delay_seconds = int(configuration.get("retry_delay_seconds", "5"))

    log.info(
        f"Configuration: pixel_id={pixel_id}, timeout={timeout_seconds}s, retry_attempts={retry_attempts}"
    )

    try:
        # Get access token (may refresh if expired) to initialize token state
        # Store previous token_expires_at to detect if token was refreshed
        previous_token_expires_at = state.get("token_expires_at")

        get_access_token(
            configuration, state, timeout_seconds, retry_attempts, retry_delay_seconds
        )

        # Check if token was refreshed (expires_at changed)
        current_token_expires_at = state.get("token_expires_at")
        if previous_token_expires_at != current_token_expires_at:
            log.info("Token was refreshed during initialization, checkpointing state...")
            op.checkpoint(state)
        else:
            # Save token state early (in case it was refreshed or initialized)
            op.checkpoint(state)

        # Step 1: Fetch ad accounts from organization API first
        log.info("Fetching ad accounts from organization API...")
        # Dictionary to store {account_id: {"name": "..."}} mapping
        ad_accounts_info = {}

        organization_id = configuration.get("organization_id", "").strip()

        if organization_id:
            log.info(f"Fetching ad accounts from organization_id: {organization_id}")
            try:
                ad_accounts_info = fetch_ad_accounts_from_organization(
                    organization_id,
                    configuration,
                    state,
                    timeout_seconds,
                    retry_attempts,
                    retry_delay_seconds,
                )
            except Exception as e:
                log.error(f"Failed to fetch ad accounts from organization: {str(e)}")
                raise ValueError(
                    "Failed to fetch ad accounts from organization, exiting connector"
                )
        else:
            log.error("No organization_id found, skipping sync")
            raise ValueError("No organization_id found, exiting connector")

        # Optional: Filter accounts based on configuration parameter
        ad_account_ids_filter_str = configuration.get("ad_account_ids_filter", "").strip()
        if ad_account_ids_filter_str:
            # Parse comma-separated list of account IDs
            filter_account_ids = [
                account_id.strip()
                for account_id in ad_account_ids_filter_str.split(",")
                if account_id.strip()
            ]
            if filter_account_ids:
                # Filter the ad_accounts_info to only include the specified account IDs
                filtered_ad_accounts_info = {}
                for account_id in filter_account_ids:
                    if account_id in ad_accounts_info:
                        filtered_ad_accounts_info[account_id] = ad_accounts_info[account_id]
                    else:
                        log.warning(
                            f"Account ID '{account_id}' from ad_account_ids_filter not found in fetched accounts"
                        )
                ad_accounts_info = filtered_ad_accounts_info
                log.info(
                    f"Filtered to {len(filtered_ad_accounts_info)} account(s) based on ad_account_ids_filter: {filter_account_ids}"
                )
            else:
                log.info("ad_account_ids_filter is empty, processing all accounts")
        else:
            log.info(
                f"Processing all {len(ad_accounts_info)} account(s) (no ad_account_ids_filter specified)"
            )

        # Step 2: Fetch custom conversions once
        log.info("Fetching custom conversions...")
        custom_conversions = fetch_custom_conversions(
            pixel_id,
            configuration,
            state,
            timeout_seconds,
            retry_attempts,
            retry_delay_seconds,
        )

        if len(custom_conversions) == 0:
            log.error("No custom conversions found, exiting connector")
            raise ValueError("No custom conversions found, exiting connector")

        # Upsert custom conversion records once
        conversions_inserted = 0
        for record in custom_conversions:
            try:
                op.upsert(table="custom_conversions", data=record)
                conversions_inserted += 1
            except Exception as e:
                log.warning(f"Failed to upsert custom conversion record: {str(e)}")
                log.warning(f"Record data: {json.dumps(record, default=str)[:200]}...")
                # Continue with next record
                continue

        log.info(f"Synced {conversions_inserted} custom conversion records")

        # Checkpoint after custom conversions are synced
        checkpoint_state = {
            **state,  # Preserve token state
            "last_sync_time": datetime.now().isoformat(),
            "total_conversion_records": conversions_inserted,
            "total_stats_records": 0,
        }
        op.checkpoint(checkpoint_state)
        log.info("Checkpointed after custom conversions")

        # Step 3: Chunk custom conversions once (shared across all accounts)
        log.info("Chunking custom conversions for API calls...")
        conversion_chunks = chunk_custom_conversions(custom_conversions, conversions_per_call=500)
        log.info(f"Created {len(conversion_chunks)} conversion chunk(s) for processing")

        # Step 4: Pre-generate fields strings and mappings once (shared across all accounts)
        log.info(
            "Pre-generating fields strings and mappings for conversion chunks (shared across all accounts)..."
        )
        conversion_chunk_fields = []
        conversion_chunk_mappings = []
        for conversion_chunk in conversion_chunks:
            fields = get_stats_fields(conversion_chunk)
            conversion_chunk_fields.append(fields)

            # Create conversion_field_to_info mapping once per conversion chunk
            conversion_field_to_info = {
                f"conversion_{conv.get('custom_conversion_id', '')}": {
                    "custom_conversion_id": conv.get("custom_conversion_id", ""),
                    "custom_conversion_name": conv.get("custom_conversion_name", ""),
                }
                for conv in conversion_chunk
                if conv.get("custom_conversion_id")
            }
            conversion_chunk_mappings.append(conversion_field_to_info)

            log.info(
                f"Generated fields string and mapping for conversion chunk ({len(conversion_chunk)} conversions): "
                f"{len(fields.split(',')) if fields else 0} fields, {len(conversion_field_to_info)} mappings"
            )

        # Step 5: Calculate date ranges once (shared across all accounts)
        log.info("Calculating date ranges for stats sync...")
        start_time, end_time = get_date_range_for_stats(state, configuration)
        log.info(f"Date range: {start_time} to {end_time}")

        # Step 6: Split date range into chunks once (shared across all accounts)
        log.info("Splitting date range into chunks...")
        date_chunks = split_date_range_if_needed(start_time, end_time, configuration)
        log.info(f"Date range split into {len(date_chunks)} chunk(s)")
        for i, (chunk_start, chunk_end) in enumerate(date_chunks, 1):
            log.info(f"  Date chunk {i}/{len(date_chunks)}: {chunk_start} to {chunk_end}")

        # Step 7: For each account, fetch stats using the shared conversion chunks, fields, mappings, and date chunks
        total_stats_records = 0
        total_accounts = len(ad_accounts_info)
        account_counter = 0

        for ad_account_id, account_info in ad_accounts_info.items():
            account_counter += 1
            try:
                account_name = account_info.get("name", "")
                log.info(
                    f"Processing account {account_counter}/{total_accounts}: "
                    f"ad_account_id={ad_account_id} (name: {account_name})"
                )

                records_inserted = 0
                upsert_batch_size = PROCESS_BATCH_SIZE  # 100k
                process_batch_num = 0
                for process_batch in fetch_stats_for_ad_account(
                    ad_account_id,
                    account_name,
                    date_chunks,
                    conversion_chunk_fields,
                    conversion_chunk_mappings,
                    configuration,
                    state,
                    timeout_seconds,
                    retry_attempts,
                    retry_delay_seconds,
                ):
                    process_batch_num += 1
                    log.info(
                        f"Processing batch {process_batch_num} ({len(process_batch)} records) for "
                        f"account {account_counter}/{total_accounts} (ad_account_id={ad_account_id})"
                    )

                    for i in range(0, len(process_batch), upsert_batch_size):
                        batch = process_batch[i : i + upsert_batch_size]
                        for record in batch:
                            try:
                                op.upsert(table="ad_account_stats", data=record)
                                records_inserted += 1
                                total_stats_records += 1
                            except Exception as e:
                                log.warning(
                                    f"Failed to upsert record for ad_account_id={ad_account_id}: {str(e)}"
                                )
                                log.warning(
                                    f"Record data: {json.dumps(record, default=str)[:200]}..."
                                )

                    log.info(
                        f"Inserted process batch {process_batch_num} for account {account_counter}/{total_accounts} "
                        f"(ad_account_id={ad_account_id}, {records_inserted} total records)"
                    )

                    # Checkpoint after each process batch (~100k records)
                    checkpoint_state = {
                        **state,  # Preserve token state
                        "last_sync_time": datetime.now().isoformat(),
                        "total_conversion_records": conversions_inserted,
                        "total_stats_records": total_stats_records,
                        "last_account_processed": ad_account_id,
                        "last_account_records_inserted": records_inserted,
                    }
                    # Update last_stats_sync_time
                    if records_inserted > 0:
                        _, end_time_utc = get_date_range_for_stats(state, configuration)
                        checkpoint_state["last_stats_sync_time"] = end_time_utc

                    op.checkpoint(checkpoint_state)
                    log.info(f"Checkpointed after process batch {process_batch_num}")

                log.info(
                    f"Synced {records_inserted} stats records for account {account_counter}/{total_accounts} "
                    f"(ad_account_id={ad_account_id})"
                )

            except Exception as e:
                log.warning(
                    f"Failed to process account {account_counter}/{total_accounts} "
                    f"(ad_account_id={ad_account_id}): {str(e)}"
                )
                log.warning("Continuing with other accounts...")
                # Checkpoint even on error to save progress made so far
                checkpoint_state = {
                    **state,  # Preserve token state
                    "last_sync_time": datetime.now().isoformat(),
                    "total_conversion_records": conversions_inserted,
                    "total_stats_records": total_stats_records,
                    "last_account_processed": ad_account_id,
                    "last_error": str(e),
                }
                if total_stats_records > 0:
                    _, end_time_utc = get_date_range_for_stats(state, configuration)
                    checkpoint_state["last_stats_sync_time"] = end_time_utc
                op.checkpoint(checkpoint_state)
                log.info(
                    f"Checkpointed after error for account {account_counter}/{total_accounts} "
                    f"(ad_account_id={ad_account_id})"
                )
                continue

        # Update state with sync information
        new_state = {
            **state,  # Preserve token state
            "last_sync_time": datetime.now().isoformat(),
            "total_records_synced": conversions_inserted + total_stats_records,
        }

        # Update last_stats_sync_time
        if total_stats_records > 0:
            _, end_time = get_date_range_for_stats(state, configuration)
            new_state["last_stats_sync_time"] = end_time

        # Final checkpoint to save state
        op.checkpoint(new_state)

        log.info("Snapchat Custom Conversions connector sync completed successfully")
        log.info(f"Total custom conversion records synced: {conversions_inserted}")
        log.info(f"Total stats records synced: {total_stats_records}")
        log.info(f"Total records synced: {conversions_inserted + total_stats_records}")

    except Exception as e:
        log.severe(f"Failed to sync data: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
