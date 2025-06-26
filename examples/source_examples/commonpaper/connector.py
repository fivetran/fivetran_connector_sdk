# This connector demonstrates how to fetch and sync data from the Common Paper API.
# This connector fetches agreement data from the Common Paper API and syncs it to the destination.
# It handles pagination and maintains sync state using checkpoints."""
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

# Import required libraries
import requests  # For making HTTP requests to the Common Paper API
import json      # For JSON data handling and serialization
import datetime  # For timestamp handling and UTC time operations
import time      # For implementing exponential backoff delays


# Base URL for the Common Paper API
__API_URL = "https://api.commonpaper.com/v1/agreements"

def get_headers(api_key):
    """
    Generate the headers required for Common Paper API authentication.
    
    Args:
        api_key (str): The API key for authentication
        
    Returns:
        dict: Headers dictionary containing Authorization and Accept headers
    """
    return {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json"
    }

def fetch_agreements(api_key, updated_at):
    """
    Fetch agreements from the Common Paper API with updated_at filter.
    Implements retry logic with exponential backoff for up to 3 attempts.
    
    Args:
        api_key (str): The API key for authentication
        updated_at (str): ISO format timestamp to filter agreements updated after this time
        
    Returns:
        dict: JSON response containing agreement data
        
    Raises:
        Exception: If the API request fails after 3 retry attempts
    """
    # Format the URL with the filter parameter
    url = f"{__API_URL}?filter[updated_at_gt]={updated_at}"
    log.fine(f"Fetching agreements from URL: {url}")

    max_retries = 3
    base_delay = 1  # Base delay in seconds
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=get_headers(api_key))
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code in [429, 500, 502, 503, 504]:  # Retryable status codes
                if attempt < max_retries - 1:  # Don't sleep on the last attempt
                    delay = base_delay * (2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                    log.warning(f"Request failed with status {response.status_code}, retrying in {delay} seconds (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    continue
                else:
                    log.severe(f"Failed to fetch agreements after {max_retries} attempts. Last status: {response.status_code} - {response.text}")
                    raise Exception(f"API returned {response.status_code} after {max_retries} attempts: {response.text}")
            else:
                # Non-retryable status codes (4xx errors except 429)
                log.severe(f"Failed to fetch agreements: {response.status_code} - {response.text}")
                raise Exception(f"API returned {response.status_code}: {response.text}")
                
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:  # Don't sleep on the last attempt
                delay = base_delay * (2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                log.warning(f"Network error occurred, retrying in {delay} seconds (attempt {attempt + 1}/{max_retries}): {str(e)}")
                time.sleep(delay)
                continue
            else:
                log.severe(f"Failed to fetch agreements after {max_retries} attempts due to network error: {str(e)}")
                raise Exception(f"Network error after {max_retries} attempts: {str(e)}")

def update(configuration, state):
    """
    Main update function that syncs agreements from Common Paper API.
    This function is called by Fivetran during each sync operation.
    
    Args:
        configuration (dict): Configuration containing API key and initial sync timestamp
        state (dict): State information from previous syncs
        
    Yields:
        Operations: Upsert operations for agreements and checkpoint operations
    """
    api_key = configuration["api_key"]
    # Use state to track the last updated_at value. Default to initial_sync_timestamp from config if not present.
    cursor = state.get("sync_cursor", configuration.get("initial_sync_timestamp"))
    log.info(f"Starting sync from updated_at: {cursor}")
    now = datetime.datetime.now(datetime.timezone.utc)
    next_cursor = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    data = fetch_agreements(api_key, cursor)
    agreements = data.get("data", [])
    
    for record in agreements:
        attributes = record.get("attributes", {})
        
        # Convert lists to strings for storage
        for field_name, field_value in attributes.items():
            if isinstance(field_value, list):
                attributes[field_name] = json.dumps(field_value)
            
        yield op.upsert("agreements", attributes)

    # Checkpoint the state to save progress and enable resumption in case of interruption
    yield op.checkpoint({"sync_cursor": next_cursor})

def schema(configuration: dict):
    """
    Define the schema for the connector.
    This function specifies the tables and their primary keys that will be created in the destination.
    
    Args:
        configuration (dict): Configuration dictionary (unused in this function)
        
    Returns:
        list: List of table definitions with their primary keys
    """
    return [
        {"table": "agreements", "primary_key": ["id"]}
    ]

# Initialize the connector with the defined update and schema functions
connector = Connector(update=update, schema=schema)

# Entry point for running the script directly (for debugging purposes)
if __name__ == "__main__":
    with open("/configuration.json", 'r') as f:
        configuration = json.load(f)  # Load configuration from JSON file
    connector.debug(configuration=configuration)  # Start debugging with the loaded configuration
