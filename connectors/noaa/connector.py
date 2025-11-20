"""NOAA Weather API Connector for Fivetran Connector SDK.
This connector fetches weather observations and alerts from the National Weather Service API.
See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For handling HTTP requests to NOAA API
import requests

# For date/time operations and timestamp handling
from datetime import datetime, timezone

# For handling retry logic with exponential backoff
import time

# For adding jitter to retry delays to avoid thundering herd problem
import random

# For loading configuration from JSON file
import json

# For type hints to improve code clarity and maintainability
from typing import Optional, List, Dict

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Constants for API configuration
__NOAA_BASE_URL = "https://api.weather.gov"
__STATIONS_ENDPOINT = "/stations"
__OBSERVATIONS_ENDPOINT = "/stations/{station_id}/observations"
__ALERTS_ENDPOINT = "/alerts/active"
__MAX_RETRIES = 3  # Maximum number of retry attempts for API requests
__BASE_DELAY = 1  # Base delay in seconds for API request retries
__OBSERVATIONS_LIMIT = 500  # Maximum observations per request
__STATIONS_LIMIT = 500  # Maximum stations per request
__CHECKPOINT_INTERVAL = 100  # Checkpoint every N records


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["user_agent"]
    for config in required_configs:
        if config not in configuration or not configuration[config]:
            raise ValueError(f"Missing required configuration parameter: {config}")
    log.info("Configuration validation passed.")


def parse_user_date_to_iso(date_input: str) -> Optional[str]:
    """
    Parse user-provided date input in YYYY-MM-DD format and convert to ISO 8601 format.
    Args:
        date_input: Date string in YYYY-MM-DD format (e.g., "2023-01-01")
    Returns:
        ISO 8601 formatted string with UTC timezone (e.g., "2023-01-01T00:00:00Z")
    Raises:
        ValueError: if the date format is not YYYY-MM-DD or invalid
    """
    if not date_input:
        return None

    try:
        parsed_date = datetime.strptime(date_input, "%Y-%m-%d")
        return parsed_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError as e:
        raise ValueError(
            f"Invalid date format '{date_input}'. Please use YYYY-MM-DD format (e.g., '2023-01-01'). Error: {e}"
        )


def make_api_request(
    url: str, headers: Dict[str, str], params: Optional[Dict[str, str]] = None
) -> Dict:
    """
    Make an API request with retry logic and proper error handling.
    Args:
        url: The API endpoint URL to make the request to
        headers: HTTP headers for the request including User-Agent
        params: Optional query parameters for the request
    Returns:
        JSON response data from the API
    Raises:
        Exception: if the request fails after all retry attempts or for client errors
    """
    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                delay = __BASE_DELAY * (2**attempt) + random.uniform(0, 1)
                log.warning(f"Rate limited. Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            elif response.status_code in [400, 404]:
                raise ValueError(f"Client error {response.status_code}: {response.text[:200]}")
            elif response.status_code == 503:
                delay = __BASE_DELAY * (2**attempt) + random.uniform(0, 1)
                log.warning(f"Service unavailable. Retrying in {delay:.2f} seconds...")
                time.sleep(delay)
            else:
                error_detail = f"Status: {response.status_code}, Response: {response.text[:200]}"
                raise ValueError(f"API request failed: {error_detail}")

        except requests.Timeout as e:
            if attempt == __MAX_RETRIES - 1:
                raise ConnectionError(f"Request timeout after {__MAX_RETRIES} attempts: {e}")
            delay = __BASE_DELAY * (2**attempt) + random.uniform(0, 1)
            log.warning(f"Request timeout: {e}. Retrying in {delay:.2f} seconds...")
            time.sleep(delay)
        except requests.RequestException as e:
            if attempt == __MAX_RETRIES - 1:
                raise ConnectionError(f"Request failed after {__MAX_RETRIES} attempts: {e}")
            delay = __BASE_DELAY * (2**attempt) + random.uniform(0, 1)
            log.warning(f"Request failed: {e}. Retrying in {delay:.2f} seconds...")
            time.sleep(delay)

    raise ConnectionError("Max retries exceeded")


def fetch_stations_by_state(headers: Dict[str, str], state_code: Optional[str]) -> List[str]:
    """
    Fetch weather station IDs from NOAA API, optionally filtered by state.
    Args:
        headers: HTTP headers for the request including User-Agent
        state_code: Two-letter US state code to filter stations (e.g., "IL", "CA")
    Returns:
        List of station ID strings
    """
    log.info(f"Fetching stations for state: {state_code if state_code else 'all states'}")

    url = f"{__NOAA_BASE_URL}{__STATIONS_ENDPOINT}"
    params = {"limit": str(__STATIONS_LIMIT)}
    if state_code:
        params["state"] = state_code

    try:
        response_data = make_api_request(url, headers, params)
    except (ValueError, ConnectionError) as e:
        log.warning(f"Failed to fetch stations: {e}")
        return []

    features = response_data.get("features", [])
    station_ids = []

    for feature in features:
        properties = feature.get("properties", {})
        station_id = properties.get("stationIdentifier")
        if station_id:
            station_ids.append(station_id)

    log.info(f"Found {len(station_ids)} stations")
    return station_ids


def parse_station_ids(station_ids_input: str) -> List[str]:
    """
    Parse comma-separated station IDs from configuration.
    Args:
        station_ids_input: Comma-separated string of station IDs (e.g., "KORD,KMDW,KPWK")
    Returns:
        List of station ID strings with whitespace removed
    """
    if not station_ids_input:
        return []
    return [
        station_id.strip() for station_id in station_ids_input.split(",") if station_id.strip()
    ]


def fetch_observations_for_station(
    headers: Dict[str, str], station_id: str, start_time: Optional[str], state: Dict
) -> int:
    """
    Fetch weather observations for a single station.
    Args:
        headers: HTTP headers for the request including User-Agent
        station_id: The weather station identifier (e.g., "KORD")
        start_time: ISO timestamp to fetch observations from
        state: State dictionary to checkpoint progress
    Returns:
        Number of observations processed for this station
    """
    log.info(f"Fetching observations for station: {station_id}")

    url = f"{__NOAA_BASE_URL}{__OBSERVATIONS_ENDPOINT}".format(station_id=station_id)
    params = {"limit": str(__OBSERVATIONS_LIMIT)}
    if start_time:
        params["start"] = start_time

    try:
        response_data = make_api_request(url, headers, params)
    except ValueError as e:
        log.warning(f"Skipping station {station_id}: {e}")
        return 0

    features = response_data.get("features", [])
    observations_count = 0

    for feature in features:
        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})

        observation_record = {
            "id": properties.get("@id"),
            "station": properties.get("station"),
            "timestamp": properties.get("timestamp"),
            "raw_message": properties.get("rawMessage"),
            "text_description": properties.get("textDescription"),
            "temperature_c": properties.get("temperature", {}).get("value"),
            "dewpoint_c": properties.get("dewpoint", {}).get("value"),
            "wind_direction_degrees": properties.get("windDirection", {}).get("value"),
            "wind_speed_kmh": properties.get("windSpeed", {}).get("value"),
            "wind_gust_kmh": properties.get("windGust", {}).get("value"),
            "barometric_pressure_pa": properties.get("barometricPressure", {}).get("value"),
            "sea_level_pressure_pa": properties.get("seaLevelPressure", {}).get("value"),
            "visibility_m": properties.get("visibility", {}).get("value"),
            "max_temperature_last_24_hours_c": properties.get("maxTemperatureLast24Hours", {}).get(
                "value"
            ),
            "min_temperature_last_24_hours_c": properties.get("minTemperatureLast24Hours", {}).get(
                "value"
            ),
            "precipitation_last_hour_mm": properties.get("precipitationLastHour", {}).get("value"),
            "precipitation_last_3_hours_mm": properties.get("precipitationLast3Hours", {}).get(
                "value"
            ),
            "precipitation_last_6_hours_mm": properties.get("precipitationLast6Hours", {}).get(
                "value"
            ),
            "relative_humidity_percent": properties.get("relativeHumidity", {}).get("value"),
            "wind_chill_c": properties.get("windChill", {}).get("value"),
            "heat_index_c": properties.get("heatIndex", {}).get("value"),
            "cloud_layers": (
                str(properties.get("cloudLayers", [])) if properties.get("cloudLayers") else None
            ),
            "elevation_m": properties.get("elevation", {}).get("value"),
            "latitude": geometry.get("coordinates", [None, None])[1],
            "longitude": geometry.get("coordinates", [None, None])[0],
        }

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="observation", data=observation_record)
        observations_count += 1

        if observations_count % __CHECKPOINT_INTERVAL == 0:
            # Save the progress by checkpointing the state. This is important for ensuring
            # that the sync process can resume from the correct position in case of next sync
            # or interruptions. Learn more about how and where to checkpoint by reading our
            # best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices).
            op.checkpoint(state)

    log.info(f"Retrieved {observations_count} observations for station {station_id}")
    return observations_count


def fetch_active_alerts(headers: Dict[str, str], alert_area: Optional[str], state: Dict) -> int:
    """
    Fetch active weather alerts for specified area or all US alerts if area not specified.
    Args:
        headers: HTTP headers for the request including User-Agent
        alert_area: US state code for filtering alerts (e.g., "IL", "CA"), None for all US alerts
        state: State dictionary to checkpoint progress
    Returns:
        Number of alerts processed
    """
    if alert_area:
        log.info(f"Fetching active alerts for area: {alert_area}")
    else:
        log.info("Fetching all active alerts for the United States")

    url = f"{__NOAA_BASE_URL}{__ALERTS_ENDPOINT}"
    params = {}
    if alert_area:
        params["area"] = alert_area

    try:
        response_data = make_api_request(url, headers, params)
    except ValueError as e:
        log.warning(f"Failed to fetch alerts: {e}")
        return 0

    features = response_data.get("features", [])
    alerts_count = 0

    for feature in features:
        properties = feature.get("properties", {})
        geometry = feature.get("geometry")

        alert_record = {
            "id": properties.get("id"),
            "area_desc": properties.get("areaDesc"),
            "geocode_same": (
                str(properties.get("geocode", {}).get("SAME", []))
                if properties.get("geocode")
                else None
            ),
            "geocode_ugc": (
                str(properties.get("geocode", {}).get("UGC", []))
                if properties.get("geocode")
                else None
            ),
            "affected_zones": (
                str(properties.get("affectedZones", []))
                if properties.get("affectedZones")
                else None
            ),
            "sent": properties.get("sent"),
            "effective": properties.get("effective"),
            "onset": properties.get("onset"),
            "expires": properties.get("expires"),
            "ends": properties.get("ends"),
            "status": properties.get("status"),
            "message_type": properties.get("messageType"),
            "category": properties.get("category"),
            "severity": properties.get("severity"),
            "certainty": properties.get("certainty"),
            "urgency": properties.get("urgency"),
            "event": properties.get("event"),
            "sender": properties.get("sender"),
            "sender_name": properties.get("senderName"),
            "headline": properties.get("headline"),
            "description": properties.get("description"),
            "instruction": properties.get("instruction"),
            "response": properties.get("response"),
            "parameters": (
                str(properties.get("parameters", {})) if properties.get("parameters") else None
            ),
            "geometry_type": geometry.get("type") if geometry else None,
        }

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="alert", data=alert_record)
        alerts_count += 1

        if alerts_count % __CHECKPOINT_INTERVAL == 0:
            # Save the progress by checkpointing the state. This is important for ensuring
            # that the sync process can resume from the correct position in case of next sync
            # or interruptions. Learn more about how and where to checkpoint by reading our
            # best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices).
            op.checkpoint(state)

    log.info(f"Retrieved {alerts_count} active alerts")
    return alerts_count


def get_station_ids_for_sync(
    headers: Dict[str, str], station_ids_input: Optional[str], state_code: Optional[str]
) -> List[str]:
    """
    Determine which station IDs to sync based on configuration.
    Args:
        headers: HTTP headers for the request including User-Agent
        station_ids_input: Comma-separated string of station IDs from configuration
        state_code: Two-letter US state code to filter stations
    Returns:
        List of station ID strings to sync
    """
    if station_ids_input:
        station_ids = parse_station_ids(station_ids_input)
        log.info(f"Using manually specified stations: {station_ids}")
        return station_ids
    elif state_code:
        return fetch_stations_by_state(headers, state_code)
    else:
        log.info("No state_code or station_ids provided. Fetching stations from all states...")
        return fetch_stations_by_state(headers, None)


def sync_observations_for_stations(
    headers: Dict[str, str], station_ids: List[str], last_sync_time: Optional[str], state: Dict
) -> int:
    """
    Sync observations from multiple weather stations.
    Args:
        headers: HTTP headers for the request including User-Agent
        station_ids: List of weather station identifiers to sync
        last_sync_time: ISO timestamp to fetch observations from
        state: State dictionary to checkpoint progress
    Returns:
        Total number of observations processed
    """
    if not station_ids:
        log.info("No stations found for observation sync")
        return 0

    log.info(f"Syncing observations from {len(station_ids)} stations")
    total_observations = 0

    for station_id in station_ids:
        observations_count = fetch_observations_for_station(
            headers, station_id, last_sync_time, state
        )
        total_observations += observations_count

    return total_observations


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "observation", "primary_key": ["id"]},
        {"table": "alert", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """

    log.info("Example: Source Examples : NOAA Weather API")

    # Validate configuration before proceeding
    validate_configuration(configuration)

    user_agent = configuration.get("user_agent")
    station_ids_input = configuration.get("station_ids")
    state_code = configuration.get("state_code")
    alert_area = configuration.get("alert_area")
    start_date_input = configuration.get("start_date")

    start_time_iso = parse_user_date_to_iso(start_date_input) if start_date_input else None
    last_sync_time = state.get("last_sync_time", start_time_iso)

    log.info(f"Starting sync with last sync time: {last_sync_time}")

    headers = {"User-Agent": user_agent, "Accept": "application/geo+json"}

    try:
        station_ids = get_station_ids_for_sync(headers, station_ids_input, state_code)
        total_observations = sync_observations_for_stations(
            headers, station_ids, last_sync_time, state
        )
        total_alerts = fetch_active_alerts(headers, alert_area, state)

        current_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        new_state = {"last_sync_time": current_timestamp}

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)

        log.info(
            f"Sync completed. Total observations: {total_observations}, "
            f"Total alerts: {total_alerts}. Updated state timestamp: {current_timestamp}"
        )

    except ValueError as e:
        raise RuntimeError(f"Configuration or API error occurred: {str(e)}")
    except ConnectionError as e:
        raise RuntimeError(f"Connection error occurred: {str(e)}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error during sync: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
