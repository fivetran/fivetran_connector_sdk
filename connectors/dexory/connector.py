from fivetran_connector_sdk import Connector, Operations as op, Logging as log
from typing import Dict, List, Any, Generator
import requests
import json
import time
from datetime import datetime

"""
Fivetran Connector for Dexory Inventory API
Fetches inventory scan data from a single site
"""

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
    if next_url:
        url = next_url
    else:
        url = f"{base_url}/customer-api/v1/locations"
        url += f"?api_key={api_key}&customer=gxo&site={site}&page[size]={page_size}"

    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < max_retries:
                wait_time = base_retry_delay * (2**attempt)
                log.warning(
                    f"{type(e).__name__}, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries + 1})"
                )
                time.sleep(wait_time)
                continue
            log.severe(f"API request failed after {max_retries + 1} attempts: {e}")
            raise
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429 and attempt < max_retries:
                wait_time = base_retry_delay * (2**attempt)
                log.warning(
                    f"Rate limited, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries + 1})"
                )
                time.sleep(wait_time)
                continue
            elif 400 <= e.response.status_code < 500:
                log.severe(f"Client error {e.response.status_code}: {e}")
                raise
            elif attempt < max_retries:
                wait_time = base_retry_delay * (2**attempt)
                log.warning(
                    f"Server error {e.response.status_code}, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries + 1})"
                )
                time.sleep(wait_time)
                continue
            log.severe(f"API request failed after {max_retries + 1} attempts: {e}")
            raise
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                wait_time = base_retry_delay * (2**attempt)
                log.warning(
                    f"Request error, retrying in {wait_time}s (attempt {attempt + 1}/{max_retries + 1}): {e}"
                )
                time.sleep(wait_time)
                continue
            log.severe(f"API request failed after {max_retries + 1} attempts: {e}")
            raise

    raise requests.exceptions.RequestException("Max retries exceeded")


def update(configuration: dict, state: dict) -> Generator:
    """
    Main update function that fetches inventory data from a single Dexory site

    Args:
        configuration: Contains site configuration (site_name, api_key, base_url)
        state: Connector state for incremental syncs

    Yields:
        Operations for upserting location data
    """
    log.info("Starting Dexory connector sync")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # Get configuration values
    site_name = configuration.get("site_name")
    api_key = configuration.get("site_api_key")
    base_url = configuration.get("base_url", __DEFAULT_BASE_URL)

    # Get max pages limit from config (default to __DEFAULT_MAX_PAGES)
    max_pages = int(configuration.get("max_pages", __DEFAULT_MAX_PAGES))
    page_size = int(configuration.get("page_size", __DEFAULT_PAGE_SIZE))
    base_retry_delay = int(
        configuration.get("base_retry_delay_seconds", __BASE_RETRY_DELAY_SECONDS)
    )
    log.info(
        f"Max pages: {max_pages}, Page size: {page_size}, Base retry delay: {base_retry_delay}s"
    )
    log.info(f"Syncing data for site: {site_name}")

    try:
        # Fetch all pages of locations
        next_url = None
        page_count = 0
        location_count = 0

        while True:
            page_count += 1

            # Prevent infinite loops
            if page_count > max_pages:
                log.warning(f"Reached maximum page limit ({max_pages})")
                break

            response_data = fetch_locations_with_retry(
                site_name, api_key, page_size, base_url, next_url, __MAX_RETRIES, base_retry_delay
            )

            # Process locations from current page
            locations = response_data.get("data", [])

            # Check for empty data
            if not locations:
                log.fine(f"No locations found on page {page_count}")

            for location in locations:
                # Convert lists to strings
                processed_location = convert_lists_to_strings(location)

                # Get expected_objects once to avoid repeated dict lookups
                expected_objects = location.get("expected_inventory_objects", [])

                op.upsert(table="locations", data=processed_location)
                location_count += 1

                # Handle expected_inventory_objects as child table records
                if expected_objects and isinstance(expected_objects, list):
                    # Early exit if no objects to process
                    if not expected_objects:
                        continue

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

                            op.upsert(table="expected_inventory_objects", data=obj)

            # Log progress every 10 pages
            if page_count % 10 == 0:
                log.info(f"Processed {page_count} pages")

            # Check for next page
            links = response_data.get("links", {})
            next_url = links.get("next")

            # Validate next URL to prevent loops
            if not next_url or not next_url.startswith(base_url):
                break

        log.info(f"Completed: {page_count} pages, {location_count} locations")

        # Add checkpoint
        new_state = {"last_sync": datetime.utcnow().isoformat()}
        op.checkpoint(state=new_state)

    except Exception as e:
        log.severe(f"Sync failed: {e}")
        raise

    log.info("Dexory connector sync completed successfully")


def schema(configuration: dict) -> List[Dict[str, Any]]:
    """
    Define schema for the locations table and expected_inventory_objects child table

    Returns:
        List of table definitions with primary keys
    """
    return [
        {"table": "locations", "primary_key": ["name", "location_type", "aisle", "scan_date"]},
        {
            "table": "expected_inventory_objects"
            """
            No primary key by design. FK fields will be added to each record,
            but per dexoryscan "All fields are optional but at least one will be present."
            Fivetran will infer a key based on all fields present.
            """
        },
    ]


# Create connector instance
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Run in debug mode for local testing
    connector.debug()
