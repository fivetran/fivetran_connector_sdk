"""
Sensource API Connector for Fivetran
This connector fetches traffic and occupancy data from Sensource API with OAuth2 authentication.
Sensource API documentation: https://vea.sensourceinc.com/api-docs/
"""

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()

from datetime import datetime, timedelta
from typing import Dict, List, Any
import requests
import json
import time

# API URLs
AUTH_URL = "https://auth.sensourceinc.com/oauth/token"
BASE_URL = "https://vea.sensourceinc.com"

# Retry configuration - Users can adjust these values based on their needs
MAX_RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 2

# Date processing configuration - Users can modify the chunk size for different data volumes
DAYS_PER_CHUNK = 29  # Creates 30-day ranges (29 + 1 day overlap)


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    # Validate required configuration parameters
    required_configs = ["client_id", "client_secret"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def get_access_token(client_id: str, client_secret: str) -> str:
    """
    Obtain OAuth2 access token from Sensource auth endpoint.
    This function handles the OAuth2 client credentials flow to authenticate with the Sensource API.
    
    Args:
        client_id: OAuth2 client ID from Sensource
        client_secret: OAuth2 client secret from Sensource
        
    Returns:
        Access token string for API requests
        
    Raises:
        Exception: If authentication fails due to invalid credentials or network issues
    """
    
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    try:
        response = requests.post(AUTH_URL, data=payload, headers=headers)
        
        if response.status_code != 200:
            log.severe(f"Authentication failed with status {response.status_code}: {response.text}")
            raise Exception(f"Authentication failed with status {response.status_code}")
        
        token_data = response.json()
        access_token = token_data.get("access_token")
        
        if not access_token:
            log.severe("No access token received from authentication endpoint")
            raise Exception("No access token received from authentication endpoint")
            
        log.fine("Successfully obtained access token")
        return access_token
        
    except requests.exceptions.RequestException as e:
        log.severe(f"Authentication request failed: {str(e)}")
        raise Exception(f"Failed to authenticate with Sensource API: {str(e)}")


def fetch_static_data(endpoint: str, access_token: str) -> List[Dict[str, Any]]:
    """
    Fetch static reference data from Sensource API endpoint.
    This function retrieves reference data that doesn't change frequently (locations, sites, zones, spaces, sensors).
    Static data is fetched without state tracking since it's reference information.
    
    Args:
        endpoint: API endpoint name (location, site, zone, space, sensor)
        access_token: OAuth2 access token for API authentication
        
    Returns:
        List of data records from the API endpoint
        
    Raises:
        Exception: If the API request fails
    """
    url = f"{BASE_URL}/api/{endpoint}"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    response = make_request_with_retry("get", url, headers=headers)
    data = response.json()
    results = data.get("results", []) if isinstance(data, dict) else data
    log.info(f"Successfully fetched {len(results)} records from {endpoint} endpoint")
    return results


def fetch_data(endpoint: str, access_token: str, start_date: str, end_date: str, metrics: str, entity_type: str = "zone") -> List[Dict[str, Any]]:
    """
    Fetch time-series data from Sensource API endpoint.
    This function retrieves traffic or occupancy data for a specific date range with hourly granularity.
    The data is processed in chunks to avoid memory overflow with large datasets.
    
    Args:
        endpoint: API endpoint name (traffic or occupancy)
        access_token: OAuth2 access token for API authentication
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        metrics: Comma-separated list of metrics to fetch
        entity_type: Entity type for data aggregation (zone for traffic, space for occupancy)
        
    Returns:
        List of data records from the API endpoint
        
    Raises:
        Exception: If the API request fails
    """
    url = f"{BASE_URL}/api/data/{endpoint}"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    params = {
        "startDate": start_date,
        "endDate": end_date,
        "dateGroupings": "hour",
        "entityType": entity_type,
        "metrics": metrics,
        "relativeDate": "custom"
    }
    
    response = make_request_with_retry("get", url, headers=headers, params=params)
    data = response.json()
    results = data.get("results", []) if isinstance(data, dict) else data
    log.info(f"Successfully fetched {len(results)} records from {endpoint} endpoint for {start_date} to {end_date}")
    return results


def generate_date_ranges(start_date: str = None, start_year: int = 2025) -> List[tuple]:
    """
    Generate date ranges for processing large historical datasets.
    This function creates 30-day chunks to process data efficiently and avoid memory issues.
    Users can modify the DAYS_PER_CHUNK constant to adjust the chunk size based on their data volume.
    
    Args:
        start_date: Starting date in YYYY-MM-DD format (takes precedence over start_year)
        start_year: Starting year for data collection (used if start_date not provided)
        
    Returns:
        List of (start_date, end_date) tuples for processing
    """
    ranges = []
    today = datetime.now().strftime("%Y-%m-%d")
    
    if start_date:
        # If start date is today or in the future, return empty list
        if start_date >= today:
            log.fine(f"Start date {start_date} is today or in the future, no date ranges to process")
            return ranges
            
        # Resume from the last sync date
        current_start = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        # Start from beginning of start_year
        current_start = datetime(start_year, 1, 1)
    
    end_date = datetime.now()
    
    while current_start < end_date:
        # Create chunks of DAYS_PER_CHUNK days to process data efficiently
        current_end = min(current_start + timedelta(days=DAYS_PER_CHUNK), end_date)
        
        ranges.append((
            current_start.strftime("%Y-%m-%d"),
            current_end.strftime("%Y-%m-%d")
        ))
        
        current_start = current_end + timedelta(days=1)
    
    return ranges


def schema(configuration: Dict[str, Any]):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    This function defines the table structure and primary keys for all tables created by the connector.
    Users can modify the primary keys or add additional columns as needed for their specific use case.
    
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "traffic",
            "primary_key": ["zone_id", "record_date_hour_1"]
        },
        {
            "table": "occupancy", 
            "primary_key": ["space_id", "record_date_hour_1"]
        },
        {
            "table": "location",
            "primary_key": ["location_id"]
        },
        {
            "table": "site",
            "primary_key": ["site_id"]
        },
        {
            "table": "zone",
            "primary_key": ["zone_id"]
        },
        {
            "table": "space",
            "primary_key": ["space_id"]
        },
        {
            "table": "sensor",
            "primary_key": ["sensor_id"]
        }
    ]


def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    """
     Define the update function, which is a required function, and is called by Fivetran during each sync.
    This function orchestrates the entire data sync process, including authentication, static data fetching,
    and incremental time-series data processing with proper state management.
    
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    
    Args:
        configuration: A dictionary containing connection details and sync parameters
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: Source Examples : Sensource API Connector")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # Extract configuration parameters as required
    client_id = configuration.get("client_id")
    client_secret = configuration.get("client_secret")
    traffic_metrics = configuration.get("traffic_metrics", "ins,outs")
    occupancy_metrics = configuration.get("occupancy_metrics", "occupancy(max),occupancy(min),occupancy(avg)")
    static_endpoints_str = configuration.get("static_endpoints", "location,site,zone,space")
    static_endpoints = [endpoint.strip() for endpoint in static_endpoints_str.split(",")]
    start_year = int(configuration.get("start_year", 2022))
    
    # Get the state variable for the sync, if needed
    last_sync_date = state.get("last_sync_date")

    try:
        log.fine("Starting Sensource data sync")
        
        # Get access token for API authentication
        access_token = get_access_token(client_id, client_secret)
        
        # Fetch static reference data (no state tracking needed)
        # Static data is reference information that doesn't change frequently
        for endpoint in static_endpoints:
            static_records = fetch_static_data(endpoint, access_token)
            for record in static_records:
                # The yield statement returns a generator object.
                # This generator will yield an upsert operation to the Fivetran connector.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted.
                # Upsert operations will insert new records or update existing ones based on the primary key.
                yield op.upsert(table=endpoint, data=record)
        
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Check if we're already synced to today to avoid unnecessary processing
        if last_sync_date == today:
            log.info(f"Already synced to today ({today}), skipping data fetch")
            return
        
        # Generate date ranges for incremental processing
        # This ensures we process data in manageable chunks to avoid memory issues
        if last_sync_date:
            log.fine(f"Resuming sync from {last_sync_date}")
            date_ranges = generate_date_ranges(start_date=last_sync_date)
        else:
            log.fine(f"Starting initial sync from {start_year}")
            date_ranges = generate_date_ranges(start_year=start_year)
        
        log.fine(f"Generated {len(date_ranges)} date ranges to process")
        
        # Process each date range to handle large datasets efficiently
        for start_date, end_date in date_ranges:
            # Skip if end date is before last sync date to avoid duplicate processing
            if last_sync_date and end_date < last_sync_date:
                log.fine(f"Skipping already processed range: {start_date} to {end_date}")
                continue
                
            log.fine(f"Processing date range: {start_date} to {end_date}")
            
            # Fetch traffic data (entity_type = zone)
            # Traffic data includes entry and exit counts by zone
            traffic_records = fetch_data("traffic", access_token, start_date, end_date, traffic_metrics, "zone")
            
            for record in traffic_records:
                # Upsert traffic data to ensure we have the latest metrics
                yield op.upsert(table="traffic", data=record)
            
            # Fetch occupancy data (entity_type = space)
            # Occupancy data includes maximum, minimum, and average occupancy by space
            occupancy_records = fetch_data("occupancy", access_token, start_date, end_date, occupancy_metrics, "space")
            
            for record in occupancy_records:
                # Upsert occupancy data to ensure we have the latest metrics
                yield op.upsert(table="occupancy", data=record)
            
            # Update state with the current sync time for the next run
            new_state = state.copy()
            new_state["last_sync_date"] = end_date
            
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Checkpointing after each date range ensures we don't lose progress if the sync is interrupted.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            yield op.checkpoint(state=new_state)
            
            log.fine(f"Completed processing for {start_date} to {end_date}")
        
        # Final checkpoint to ensure the complete state is saved
        yield op.checkpoint(state=new_state)
        log.fine("Sensource data sync completed")

    except Exception as e:
        # In case of an exception, raise a runtime error with descriptive message
        log.severe(f"Sync failed with error: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


def make_request_with_retry(method, url, **kwargs):
    """
    Make HTTP request with retry logic for improved reliability.
    This function implements exponential backoff for 5xx errors and network failures.
    Users can modify MAX_RETRY_ATTEMPTS and RETRY_BASE_DELAY constants to adjust retry behavior.
    
    Args:
        method: HTTP method (get, post, etc.)
        url: Request URL
        **kwargs: Additional arguments for requests library
        
    Returns:
        Response object from successful request
        
    Raises:
        Exception: If request fails after all retry attempts
    """
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            response = requests.request(method, url, **kwargs)
            
            if response.status_code == 200:
                return response
            elif 500 <= response.status_code < 600 and attempt < MAX_RETRY_ATTEMPTS - 1:
                # 5xx error, retry with exponential backoff
                delay = RETRY_BASE_DELAY ** attempt
                log.warning(f"5xx error (status {response.status_code}) on attempt {attempt + 1}/{MAX_RETRY_ATTEMPTS}. Retrying in {delay} seconds...")
                time.sleep(delay)
                continue
            else:
                log.severe(f"Request failed with status {response.status_code}: {response.text}")
                raise Exception(f"Request failed with status {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRY_ATTEMPTS - 1:
                delay = RETRY_BASE_DELAY ** attempt
                log.warning(f"Request failed on attempt {attempt + 1}/{MAX_RETRY_ATTEMPTS}. Retrying in {delay} seconds...")
                time.sleep(delay)
                continue
            else:
                log.severe(f"Request failed: {str(e)}")
                raise Exception(f"Request failed: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
