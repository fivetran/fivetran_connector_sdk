"""
Fivetran Connector for Dexory Inventory API
Fetches inventory scan data from a single site
"""

# For JSON serialization
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For type hints
from typing import Dict, List, Any

# For making HTTP requests
import requests

# For handling time delays
import time

# For handling date and time operations
from datetime import datetime, timezone


__MAX_RETRIES = 3  # Maximum number of retry attempts for API requests
__BASE_RETRY_DELAY_SECONDS = 2  # Base delay in seconds for exponential backoff retry logic
__DEFAULT_MAX_PAGES = 1000  # Default maximum number of pages to process
__DEFAULT_PAGE_SIZE = 1000  # Default page size for API requests
__DEFAULT_BASE_URL = "https://api.service.dexoryview.com"  # Default Dexory API base URL


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
    required_configs = ["site_name", "site_api_key", "base_url"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def convert_lists_to_strings(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert list values to JSON strings for Fivetran compatibility, excluding expected_inventory_objects"""
    result = {}
    for key, value in data.items():
        if key == "expected_inventory_objects":
            # Skip this field - it will be handled as a separate table
            continue
        elif isinstance(value, (list, dict)):
            # Combine list and dict handling since both get JSON serialized
            result[key] = json.dumps(value)
        else:
            result[key] = value
    return result


def _build_locations_url(
    site: str, api_key: str, page_size: int, base_url: str, next_url: str = None
) -> str:
    """Build the URL for fetching locations"""
    if next_url:
        return next_url
    return f"{base_url}/customer-api/v1/locations?api_key={api_key}&customer=gxo&site={site}&page[size]={page_size}"


def _should_retry_exception(exception: Exception, attempt: int, max_retries: int) -> bool:
    """Determine if an exception should trigger a retry"""
    if attempt >= max_retries:
        return False

    if isinstance(exception, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
        return True

    if isinstance(exception, requests.exceptions.HTTPError):
        # Retry on rate limiting or server errors
        return exception.response.status_code == 429 or exception.response.status_code >= 500

    if isinstance(exception, requests.exceptions.RequestException):
        return True

    return False


def _handle_retry_exception(
    exception: Exception, attempt: int, max_retries: int, base_retry_delay: int
) -> None:
    """Handle retry logic for different exception types"""
    if isinstance(exception, requests.exceptions.HTTPError):
        if exception.response.status_code == 429:
            log.warning(
                f"Rate limited, retrying in {base_retry_delay * (2**attempt)}s (attempt {attempt + 1}/{max_retries + 1})"
            )
        elif 400 <= exception.response.status_code < 500:
            log.severe(f"Client error {exception.response.status_code}: {exception}")
            raise
        else:
            log.warning(
                f"Server error {exception.response.status_code}, retrying in {base_retry_delay * (2**attempt)}s (attempt {attempt + 1}/{max_retries + 1})"
            )
    else:
        log.warning(
            f"{type(exception).__name__}, retrying in {base_retry_delay * (2**attempt)}s (attempt {attempt + 1}/{max_retries + 1})"
        )


def _make_api_request_with_retry(
    url: str, max_retries: int, base_retry_delay: int
) -> Dict[str, Any]:
    """Make API request with retry logic"""
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if _should_retry_exception(e, attempt, max_retries):
                _handle_retry_exception(e, attempt, max_retries, base_retry_delay)
                wait_time = base_retry_delay * (2**attempt)
                time.sleep(wait_time)
                continue
            else:
                log.severe(f"API request failed after {max_retries + 1} attempts: {e}")
                raise

    raise requests.exceptions.RequestException("Max retries exceeded")


def fetch_locations_with_retry(
    site: str,
    api_key: str,
    page_size: int,
    base_url: str,
    next_url: str = None,
    max_retries: int = __MAX_RETRIES,
    base_retry_delay: int = __BASE_RETRY_DELAY_SECONDS,
) -> Dict[str, Any]:
    """Fetch locations from Dexory API with pagination support and retry logic"""
    url = _build_locations_url(site, api_key, page_size, base_url, next_url)
    return _make_api_request_with_retry(url, max_retries, base_retry_delay)


def _process_location_data(location: Dict[str, Any]) -> None:
    """Process a single location and its expected inventory objects"""
    # Convert lists to strings
    processed_location = convert_lists_to_strings(location)

    # Get expected_objects once to avoid repeated dict lookups
    expected_objects = location.get("expected_inventory_objects", [])

    # Upsert the location
    op.upsert(table="location", data=processed_location)

    # Process expected inventory objects if they exist
    if expected_objects and isinstance(expected_objects, list):
        _process_expected_inventory_objects(location, expected_objects)


def _process_expected_inventory_objects(
    location: Dict[str, Any], expected_objects: List[Dict[str, Any]]
) -> None:
    """Process expected inventory objects for a location"""
    # Pre-calculate common FK values to avoid repeated lookups
    location_name = location.get("name")
    location_type = location.get("location_type")
    location_aisle = location.get("aisle")
    location_scan_date = location.get("scan_date")

    for obj in expected_objects:
        if isinstance(obj, dict):
            # Add foreign key fields to the existing object
            obj["name"] = location_name
            obj["location_type"] = location_type
            obj["aisle"] = location_aisle
            obj["scan_date"] = location_scan_date

            op.upsert(table="expected_inventory_object", data=obj)


def _process_locations_page(locations: List[Dict[str, Any]], page_count: int) -> int:
    """Process a page of locations and return the count of processed locations"""
    if not locations:
        log.fine(f"No locations found on page {page_count}")
        return 0

    location_count = 0
    for location in locations:
        _process_location_data(location)
        location_count += 1

    return location_count


def _should_continue_pagination(
    next_url: str, base_url: str, page_count: int, max_pages: int
) -> bool:
    """Determine if pagination should continue"""
    if page_count > max_pages:
        log.warning(f"Reached maximum page limit ({max_pages})")
        return False

    if not next_url or not next_url.startswith(base_url):
        return False

    return True


def _sync_locations_pages(
    site_name: str,
    api_key: str,
    page_size: int,
    base_url: str,
    max_pages: int,
    base_retry_delay: int,
) -> tuple[int, int]:
    """Sync all pages of locations and return page count and location count"""
    next_url = None
    page_count = 0
    location_count = 0

    while True:
        page_count += 1

        if not _should_continue_pagination(next_url, base_url, page_count, max_pages):
            break

        response_data = fetch_locations_with_retry(
            site_name, api_key, page_size, base_url, next_url, __MAX_RETRIES, base_retry_delay
        )

        # Process locations from current page
        locations = response_data.get("data", [])
        page_location_count = _process_locations_page(locations, page_count)
        location_count += page_location_count

        # Log progress every 10 pages
        if page_count % 10 == 0:
            log.info(f"Processed {page_count} pages")

        # Check for next page
        links = response_data.get("links", {})
        next_url = links.get("next")

    return page_count, location_count


def update(configuration: dict, state: dict) -> None:
    """
     Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: Dexory")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # Get configuration values
    site_name = configuration.get("site_name")
    api_key = configuration.get("site_api_key")
    base_url = configuration.get("base_url", __DEFAULT_BASE_URL)

    log.info(
        f"Max pages: {__DEFAULT_MAX_PAGES}, Page size: {__DEFAULT_PAGE_SIZE}, Base retry delay: {__BASE_RETRY_DELAY_SECONDS}s"
    )
    log.info(f"Syncing data for site: {site_name}")

    try:
        page_count, location_count = _sync_locations_pages(
            site_name,
            api_key,
            __DEFAULT_PAGE_SIZE,
            base_url,
            __DEFAULT_MAX_PAGES,
            __BASE_RETRY_DELAY_SECONDS,
        )

        log.info(f"Completed: {page_count} pages, {location_count} locations")

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        new_state = {"last_sync": datetime.now(timezone.utc).isoformat()}
        op.checkpoint(state=new_state)

    except Exception as e:
        log.severe(f"Sync failed: {e}")
        raise

    log.info("Dexory connector sync completed successfully")


def schema(configuration: dict) -> List[Dict[str, Any]]:
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "location", 
            "primary_key": ["name", "location_type", "aisle", "scan_date"]
        },
        {
            "table": "expected_inventory_object"
            # No primary key by design. FK fields will be added to each record,
            # but per dexoryscan "All fields are optional but at least one will be present."
            # Fivetran will infer a key based on all fields present.
        },
    ]


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
