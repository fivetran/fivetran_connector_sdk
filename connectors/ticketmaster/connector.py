"""Ticketmaster connector for syncing data including events, venues, attractions, and classifications.
This connector demonstrates how to fetch data from Ticketmaster Discovery API v2 and upsert it into destination using memory-efficient streaming patterns.
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

# For making HTTP requests to Ticketmaster Discovery API v2
import requests

# For handling dates and timestamps
from datetime import datetime, timedelta, timezone

# For type hints to improve code readability
from typing import Dict, List, Any, Optional

# For retry logic with exponential backoff
import time

# For random number generation
import random

""" ADD YOUR SOURCE-SPECIFIC IMPORTS HERE
Example: import pandas, boto3, etc.
Add comment for each import to explain its purpose for users to follow.
"""
# Private constants (use __ prefix)
__INVALID_LITERAL_ERROR = "invalid literal"
__API_BASE_URL = "https://app.ticketmaster.com/discovery/v2"
__DEFAULT_PAGE_SIZE = 100
__MAX_PAGE_SIZE = 200
__RATE_LIMIT_REQUESTS_PER_SECOND = 5
__RATE_LIMIT_DAILY_REQUESTS = 5000


def __get_config_int(
    configuration: Dict[str, Any],
    key: str,
    default: int,
    min_val: Optional[int] = None,
    max_val: Optional[int] = None,
) -> int:
    """
    Parse an integer configuration value with optional range validation.

    Args:
        configuration: Dictionary containing connector configuration values.
        key: Configuration key to read.
        default: Default value to use when the key is missing or invalid.
        min_val: Optional minimum allowed value (inclusive).
        max_val: Optional maximum allowed value (inclusive).

    Returns:
        Parsed integer value respecting provided bounds when applicable.

    Raises:
        ValueError: If bounds are provided and default violates them (should not occur in normal flow).
    """
    try:
        value = int(configuration.get(key, default))
        if min_val is not None and value < min_val:
            log.warning(f"Configuration {key}={value} below minimum {min_val}, using minimum")
            return min_val
        if max_val is not None and value > max_val:
            log.warning(f"Configuration {key}={value} above maximum {max_val}, using maximum")
            return max_val
        return value
    except (ValueError, TypeError):
        log.warning(f"Invalid {key} configuration, using default {default}")
        return default


def __get_config_bool(configuration: Dict[str, Any], key: str, default: bool) -> bool:
    """
    Parse a boolean configuration value from bool or string representations.

    Args:
        configuration: Dictionary containing connector configuration values.
        key: Configuration key to read.
        default: Default value to use when the key is missing or invalid.

    Returns:
        Boolean value parsed from configuration or the provided default.
    """
    value = configuration.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on", "enabled")
    return default


def __calculate_wait_time(
    attempt: int, response_headers: Dict[str, str], base_delay: int = 1, max_delay: int = 60
) -> int:
    """
    Calculate wait time with jitter for rate limiting and retries.

    Args:
        attempt: Zero-based retry attempt number.
        response_headers: Response headers that may contain retry hints (e.g., Retry-After).
        base_delay: Base delay in seconds for exponential backoff.
        max_delay: Maximum delay in seconds allowed.

    Returns:
        Computed wait time in seconds including jitter.
    """
    # Check for Retry-After header from rate limiting
    retry_after = response_headers.get("Retry-After")
    if retry_after:
        try:
            return min(int(retry_after), max_delay)
        except ValueError:
            pass

    # Exponential backoff with jitter
    delay = min(base_delay * (2**attempt), max_delay)
    jitter = random.uniform(0.1, 0.3) * delay
    return int(delay + jitter)


def __handle_rate_limit(attempt: int, response: requests.Response) -> None:
    """
    Handle HTTP 429 responses by waiting before retrying.

    Args:
        attempt: Zero-based retry attempt number.
        response: HTTP response object containing headers.
    """
    wait_time = __calculate_wait_time(attempt, response.headers)
    log.warning(f"Rate limit hit (attempt {attempt + 1}). Waiting {wait_time} seconds...")
    time.sleep(wait_time)


def __handle_request_error(
    attempt: int, retry_attempts: int, error: Exception, endpoint: str
) -> None:
    """
    Handle request errors with retry logic and logging.

    Args:
        attempt: Zero-based retry attempt number.
        retry_attempts: Total number of attempts allowed.
        error: Exception encountered during the request.
        endpoint: API endpoint being called.

    Raises:
        RuntimeError: When the final attempt has failed (propagated by caller after logging).
    """
    if attempt < retry_attempts - 1:
        wait_time = __calculate_wait_time(attempt, {})
        log.warning(
            f"Request to {endpoint} failed (attempt {attempt + 1}/{retry_attempts}): {str(error)}. Retrying in {wait_time}s..."
        )
        time.sleep(wait_time)
    else:
        log.severe(f"Request to {endpoint} failed after {retry_attempts} attempts: {str(error)}")


def execute_api_request(
    endpoint: str,
    api_key: str,
    params: Optional[Dict[str, Any]] = None,
    configuration: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Execute a GET request to the Ticketmaster API with retry and rate limit handling.

    Args:
        endpoint: API endpoint path beginning with '/'.
        api_key: Ticketmaster API key for authentication.
        params: Optional query parameters to include in the request.
        configuration: Optional configuration used for timeout and retry settings.

    Returns:
        Parsed JSON response as a dictionary.

    Raises:
        ValueError: If authentication fails due to an invalid API key.
        RuntimeError: If the request fails after exhausting retries.
    """
    url = f"{__API_BASE_URL}{endpoint}"

    # Prepare parameters with API key
    request_params = params.copy() if params else {}
    request_params["apikey"] = api_key

    timeout = __get_config_int(configuration or {}, "request_timeout_seconds", 30)
    retry_attempts = __get_config_int(configuration or {}, "retry_attempts", 3)

    for attempt in range(retry_attempts):
        try:
            response = requests.get(url, params=request_params, timeout=timeout)

            if response.status_code == 429:
                __handle_rate_limit(attempt, response)
                continue

            if response.status_code == 401:
                raise ValueError(
                    "Invalid API key. Please check your Ticketmaster API credentials."
                )

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            __handle_request_error(attempt, retry_attempts, e, endpoint)
            continue

    raise RuntimeError(f"API request to {endpoint} failed after {retry_attempts} attempts")


def get_time_range(
    last_sync_time: Optional[str] = None, configuration: Optional[Dict[str, Any]] = None
) -> Dict[str, str]:
    """
    Generate a UTC time range for incremental sync based on last sync time.

    Args:
        last_sync_time: ISO 8601 timestamp of the last successful sync.
        configuration: Optional configuration used to compute default initial window.

    Returns:
        Dictionary with 'start' and 'end' ISO 8601 timestamps in UTC.
    """
    end_time = datetime.now(timezone.utc)

    if last_sync_time:
        try:
            start_time = datetime.fromisoformat(last_sync_time.replace("Z", "+00:00"))
        except ValueError:
            log.warning(
                f"Invalid last_sync_time format: {last_sync_time}, using default initial sync period"
            )
            initial_sync_days = __get_config_int(configuration or {}, "initial_sync_days", 90)
            start_time = end_time - timedelta(days=initial_sync_days)
    else:
        initial_sync_days = __get_config_int(configuration or {}, "initial_sync_days", 90)
        start_time = end_time - timedelta(days=initial_sync_days)

    return {
        "start": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def __map_event_data(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a Ticketmaster event object into a flat record for upsert.

    Args:
        event: Raw event dictionary from the API response.

    Returns:
        Dictionary representing a normalized event record.
    """
    # Extract embedded venue and attractions data
    embedded = event.get("_embedded", {})
    venues = embedded.get("venues", [])
    attractions = embedded.get("attractions", [])

    # Get primary venue
    primary_venue = venues[0] if venues else {}

    # Get primary attraction
    primary_attraction = attractions[0] if attractions else {}

    # Extract date information
    dates = event.get("dates", {})
    start_date = dates.get("start", {})

    # Extract pricing information
    price_ranges = event.get("priceRanges", [])
    min_price = price_ranges[0].get("min") if price_ranges else None
    max_price = price_ranges[0].get("max") if price_ranges else None
    currency = price_ranges[0].get("currency") if price_ranges else None

    # Extract sales information
    sales = event.get("sales", {})
    public_sales = sales.get("public", {})

    return {
        "id": event.get("id", ""),
        "name": event.get("name", ""),
        "type": event.get("type", ""),
        "url": event.get("url", ""),
        "locale": event.get("locale", ""),
        "start_date_time": start_date.get("dateTime", ""),
        "start_local_date": start_date.get("localDate", ""),
        "start_local_time": start_date.get("localTime", ""),
        "timezone": start_date.get("timezone", ""),
        "status_code": dates.get("status", {}).get("code", ""),
        "venue_id": primary_venue.get("id", ""),
        "venue_name": primary_venue.get("name", ""),
        "venue_city": primary_venue.get("city", {}).get("name", ""),
        "venue_state": primary_venue.get("state", {}).get("name", ""),
        "venue_country": primary_venue.get("country", {}).get("name", ""),
        "attraction_id": primary_attraction.get("id", ""),
        "attraction_name": primary_attraction.get("name", ""),
        "min_price": min_price,
        "max_price": max_price,
        "currency": currency,
        "public_sales_start": public_sales.get("startDateTime", ""),
        "public_sales_end": public_sales.get("endDateTime", ""),
        "info": event.get("info", ""),
        "please_note": event.get("pleaseNote", ""),
        "images": json.dumps(event.get("images", [])),  # Store as JSON string
        "classifications": json.dumps(event.get("classifications", [])),  # Store as JSON string
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_venue_data(venue: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a Ticketmaster venue object into a flat record for upsert.

    Args:
        venue: Raw venue dictionary from the API response.

    Returns:
        Dictionary representing a normalized venue record.
    """
    # Extract location information
    location = venue.get("location", {})
    city = venue.get("city", {})
    state = venue.get("state", {})
    country = venue.get("country", {})
    address = venue.get("address", {})

    # Extract social media links
    social = venue.get("social", {})

    return {
        "id": venue.get("id", ""),
        "name": venue.get("name", ""),
        "type": venue.get("type", ""),
        "url": venue.get("url", ""),
        "locale": venue.get("locale", ""),
        "timezone": venue.get("timezone", ""),
        "city_name": city.get("name", ""),
        "state_name": state.get("name", ""),
        "state_code": state.get("stateCode", ""),
        "country_name": country.get("name", ""),
        "country_code": country.get("countryCode", ""),
        "address_line1": address.get("line1", ""),
        "address_line2": address.get("line2", ""),
        "postal_code": venue.get("postalCode", ""),
        "latitude": location.get("latitude", ""),
        "longitude": location.get("longitude", ""),
        "twitter": social.get("twitter", {}).get("handle", ""),
        "images": json.dumps(venue.get("images", [])),  # Store as JSON string
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_attraction_data(attraction: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a Ticketmaster attraction object into a flat record for upsert.

    Args:
        attraction: Raw attraction dictionary from the API response.

    Returns:
        Dictionary representing a normalized attraction record.
    """
    # Extract external links
    external_links = attraction.get("externalLinks", {})

    return {
        "id": attraction.get("id", ""),
        "name": attraction.get("name", ""),
        "type": attraction.get("type", ""),
        "url": attraction.get("url", ""),
        "locale": attraction.get("locale", ""),
        "external_links": json.dumps(external_links),  # Store as JSON string
        "images": json.dumps(attraction.get("images", [])),  # Store as JSON string
        "classifications": json.dumps(
            attraction.get("classifications", [])
        ),  # Store as JSON string
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def __map_classification_data(classification: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a Ticketmaster classification object into a flat record for upsert.

    Args:
        classification: Raw classification dictionary from the API response.

    Returns:
        Dictionary representing a normalized classification record.
    """
    # Extract segment, genre, and subGenre information
    segment = classification.get("segment", {})
    genre = classification.get("genre", {})
    sub_genre = classification.get("subGenre", {})

    return {
        "id": classification.get("id", ""),
        "name": classification.get("name", ""),
        "segment_id": segment.get("id", ""),
        "segment_name": segment.get("name", ""),
        "genre_id": genre.get("id", ""),
        "genre_name": genre.get("name", ""),
        "sub_genre_id": sub_genre.get("id", ""),
        "sub_genre_name": sub_genre.get("name", ""),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }


def get_events(
    api_key: str,
    last_sync_time: Optional[str] = None,
    configuration: Optional[Dict[str, Any]] = None,
):
    """
    Stream events from the API and yield normalized records.

    Args:
        api_key: Ticketmaster API key for authentication.
        last_sync_time: Optional ISO 8601 timestamp of the last successful sync.
        configuration: Optional configuration for pagination and time window.

    Yields:
        Normalized event records as dictionaries.

    Raises:
        RuntimeError: If repeated request failures occur during pagination.
    """
    time_range = get_time_range(last_sync_time, configuration)
    endpoint = "/events"
    max_records = __get_config_int(
        configuration or {}, "max_records_per_page", __DEFAULT_PAGE_SIZE
    )

    params = {
        "size": min(max_records, __MAX_PAGE_SIZE),
        "page": 0,
        "startDateTime": time_range["start"],
        "endDateTime": time_range["end"],
        "sort": "date,asc",
    }

    page = 0
    total_processed = 0

    while True:
        params["page"] = page
        try:
            response = execute_api_request(endpoint, api_key, params, configuration)

            # Extract events from embedded data
            embedded = response.get("_embedded", {})
            events = embedded.get("events", [])

            if not events:
                log.info(f"No more events found. Total processed: {total_processed}")
                break

            # Yield individual records instead of accumulating
            for event in events:
                yield __map_event_data(event)
                total_processed += 1

            # Check if we've reached the maximum items or last page
            page_info = response.get("page", {})
            total_pages = page_info.get("totalPages", 1)
            current_page = page_info.get("number", 0)

            if current_page >= total_pages - 1 or len(events) < params["size"]:
                log.info(f"Reached last page of events. Total processed: {total_processed}")
                break

            page += 1

            # Add small delay to respect rate limits
            time.sleep(1.0 / __RATE_LIMIT_REQUESTS_PER_SECOND)

        except Exception as e:
            log.severe(f"Error fetching events on page {page}: {str(e)}")
            break


def get_venues(api_key: str, configuration: Optional[Dict[str, Any]] = None):
    """
    Stream venues from the API and yield normalized records.

    Args:
        api_key: Ticketmaster API key for authentication.
        configuration: Optional configuration for pagination.

    Yields:
        Normalized venue records as dictionaries.

    Raises:
        RuntimeError: If repeated request failures occur during pagination.
    """
    endpoint = "/venues"
    max_records = __get_config_int(
        configuration or {}, "max_records_per_page", __DEFAULT_PAGE_SIZE
    )

    params = {"size": min(max_records, __MAX_PAGE_SIZE), "page": 0, "sort": "name,asc"}

    page = 0
    total_processed = 0

    while True:
        params["page"] = page
        try:
            response = execute_api_request(endpoint, api_key, params, configuration)

            # Extract venues from embedded data
            embedded = response.get("_embedded", {})
            venues = embedded.get("venues", [])

            if not venues:
                log.info(f"No more venues found. Total processed: {total_processed}")
                break

            # Yield individual records instead of accumulating
            for venue in venues:
                yield __map_venue_data(venue)
                total_processed += 1

            # Check if we've reached the maximum items or last page
            page_info = response.get("page", {})
            total_pages = page_info.get("totalPages", 1)
            current_page = page_info.get("number", 0)

            if current_page >= total_pages - 1 or len(venues) < params["size"]:
                log.info(f"Reached last page of venues. Total processed: {total_processed}")
                break

            page += 1

            # Add small delay to respect rate limits
            time.sleep(1.0 / __RATE_LIMIT_REQUESTS_PER_SECOND)

        except Exception as e:
            log.severe(f"Error fetching venues on page {page}: {str(e)}")
            break


def get_attractions(api_key: str, configuration: Optional[Dict[str, Any]] = None):
    """
    Stream attractions from the API and yield normalized records.

    Args:
        api_key: Ticketmaster API key for authentication.
        configuration: Optional configuration for pagination.

    Yields:
        Normalized attraction records as dictionaries.

    Raises:
        RuntimeError: If repeated request failures occur during pagination.
    """
    endpoint = "/attractions"
    max_records = __get_config_int(
        configuration or {}, "max_records_per_page", __DEFAULT_PAGE_SIZE
    )

    params = {"size": min(max_records, __MAX_PAGE_SIZE), "page": 0, "sort": "name,asc"}

    page = 0
    total_processed = 0

    while True:
        params["page"] = page
        try:
            response = execute_api_request(endpoint, api_key, params, configuration)

            # Extract attractions from embedded data
            embedded = response.get("_embedded", {})
            attractions = embedded.get("attractions", [])

            if not attractions:
                log.info(f"No more attractions found. Total processed: {total_processed}")
                break

            # Yield individual records instead of accumulating
            for attraction in attractions:
                yield __map_attraction_data(attraction)
                total_processed += 1

            # Check if we've reached the maximum items or last page
            page_info = response.get("page", {})
            total_pages = page_info.get("totalPages", 1)
            current_page = page_info.get("number", 0)

            if current_page >= total_pages - 1 or len(attractions) < params["size"]:
                log.info(f"Reached last page of attractions. Total processed: {total_processed}")
                break

            page += 1

            # Add small delay to respect rate limits
            time.sleep(1.0 / __RATE_LIMIT_REQUESTS_PER_SECOND)

        except Exception as e:
            log.severe(f"Error fetching attractions on page {page}: {str(e)}")
            break


def get_classifications(api_key: str, configuration: Optional[Dict[str, Any]] = None):
    """
    Stream classifications from the API and yield normalized records.

    Args:
        api_key: Ticketmaster API key for authentication.
        configuration: Optional configuration for pagination.

    Yields:
        Normalized classification records as dictionaries.

    Raises:
        RuntimeError: If repeated request failures occur during pagination.
    """
    endpoint = "/classifications"
    max_records = __get_config_int(
        configuration or {}, "max_records_per_page", __DEFAULT_PAGE_SIZE
    )

    params = {"size": min(max_records, __MAX_PAGE_SIZE), "page": 0, "sort": "name,asc"}

    page = 0
    total_processed = 0

    while True:
        params["page"] = page
        try:
            response = execute_api_request(endpoint, api_key, params, configuration)

            # Extract classifications from embedded data
            embedded = response.get("_embedded", {})
            classifications = embedded.get("classifications", [])

            if not classifications:
                log.info(f"No more classifications found. Total processed: {total_processed}")
                break

            # Yield individual records instead of accumulating
            for classification in classifications:
                yield __map_classification_data(classification)
                total_processed += 1

            # Check if we've reached the maximum items or last page
            page_info = response.get("page", {})
            total_pages = page_info.get("totalPages", 1)
            current_page = page_info.get("number", 0)

            if current_page >= total_pages - 1 or len(classifications) < params["size"]:
                log.info(
                    f"Reached last page of classifications. Total processed: {total_processed}"
                )
                break

            page += 1

            # Add small delay to respect rate limits
            time.sleep(1.0 / __RATE_LIMIT_REQUESTS_PER_SECOND)

        except Exception as e:
            log.severe(f"Error fetching classifications on page {page}: {str(e)}")
            break


def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Define the schema for destination tables and primary keys.

    Args:
        configuration: Dictionary containing connector configuration values.

    Returns:
        List of table schema dictionaries with table name and primary key fields.
    """
    return [
        {"table": "events", "primary_key": ["id"]},
        {"table": "venues", "primary_key": ["id"]},
        {"table": "attractions", "primary_key": ["id"]},
        {"table": "classifications", "primary_key": ["id"]},
    ]


def _sync_data_type(
    data_type: str, sync_function, api_key: str, sync_params: list, enable_debug_logging: bool
) -> int:
    """
    Generic helper function for syncing any data type with consistent logging and processing.

    Args:
        data_type: Name of the data type being synced (e.g., "events", "venues").
        sync_function: Generator function that yields records for the data type.
        api_key: Ticketmaster API key for authentication.
        sync_params: List of parameters to pass to the sync function.
        enable_debug_logging: Whether to log debug information during processing.

    Returns:
        int: Number of records processed for this data type.
    """
    processed = 0
    log.info(f"Fetching {data_type} data...")

    for record in sync_function(api_key, *sync_params):
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted,
        op.upsert(table=data_type, data=record)
        processed += 1

        if enable_debug_logging and processed % 100 == 0:
            log.info(f"Processed {processed} {data_type}")

    log.info(f"Completed {data_type} sync. Total {data_type} processed: {processed}")
    return processed


def update(configuration: Dict[str, Any], state: Dict[str, Any]) -> None:
    """
    Run a sync cycle, upserting records and checkpointing state.

    See the technical reference for details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Args:
        configuration: Connector configuration including credentials and options.
        state: State dictionary from previous runs used for incremental sync.

    Raises:
        RuntimeError: If the sync process fails.
    """
    log.info("Starting Ticketmaster connector sync")

    # Configuration is auto-validated by SDK when configuration.json exists

    # Extract configuration parameters
    api_key = str(configuration.get("api_key", "")).strip()
    enable_events = __get_config_bool(configuration, "enable_events", True)
    enable_venues = __get_config_bool(configuration, "enable_venues", True)
    enable_attractions = __get_config_bool(configuration, "enable_attractions", True)
    enable_classifications = __get_config_bool(configuration, "enable_classifications", True)
    enable_incremental_sync = __get_config_bool(configuration, "enable_incremental_sync", True)
    enable_debug_logging = __get_config_bool(configuration, "enable_debug_logging", False)

    if enable_debug_logging:
        log.info("Debug logging enabled")

    # Get state for incremental sync
    last_sync_time = state.get("last_sync_time") if enable_incremental_sync else None

    try:
        # Fetch data using helper function for each enabled data type (NO MEMORY ACCUMULATION)
        events_processed = 0
        if enable_events:
            events_processed = _sync_data_type(
                "events",
                get_events,
                api_key,
                [last_sync_time, configuration],
                enable_debug_logging,
            )

        venues_processed = 0
        if enable_venues:
            venues_processed = _sync_data_type(
                "venues", get_venues, api_key, [configuration], enable_debug_logging
            )

        attractions_processed = 0
        if enable_attractions:
            attractions_processed = _sync_data_type(
                "attractions", get_attractions, api_key, [configuration], enable_debug_logging
            )

        classifications_processed = 0
        if enable_classifications:
            classifications_processed = _sync_data_type(
                "classifications",
                get_classifications,
                api_key,
                [configuration],
                enable_debug_logging,
            )

        # Update state and checkpoint
        new_state = {
            "last_sync_time": datetime.now(timezone.utc).isoformat(),
            "events_processed": events_processed,
            "venues_processed": venues_processed,
            "attractions_processed": attractions_processed,
            "classifications_processed": classifications_processed,
        }

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)

        total_processed = (
            events_processed + venues_processed + attractions_processed + classifications_processed
        )
        log.info(f"Sync completed successfully. Total records processed: {total_processed}")

    except Exception as e:
        log.severe(f"Sync failed: {str(e)}")
        raise RuntimeError(f"Failed to sync Ticketmaster data: {str(e)}")


# Create connector instance
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
