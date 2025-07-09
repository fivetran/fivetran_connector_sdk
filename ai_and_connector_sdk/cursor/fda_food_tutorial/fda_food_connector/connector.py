"""
FDA Food Enforcement API Connector

This connector fetches food enforcement data from the FDA's openFDA API and loads it into Fivetran.
It handles pagination, rate limiting, and flattens nested JSON structures into tabular format.
Can work with or without an API key (with different rate limits).
"""

from fivetran_connector_sdk import Connector, Logging as log, Operations as op
import json
import requests
import time
from datetime import datetime
import pytz
from dateutil import parser
from typing import Dict, List, Any, Generator, Optional

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 1
DEFAULT_BATCH_SIZE = 100
DEFAULT_RATE_LIMIT_PAUSE = 0.25  # Increased pause for no API key
NO_API_KEY_RATE_LIMIT_PAUSE = 0.5  # Longer pause when no API key is used
MAX_BATCHES = 10  # Change to None for full dataset


def validate_configuration(configuration: dict) -> None:
    """
    Validate the configuration dictionary to ensure it contains all required parameters.

    Args:
        configuration: Dictionary containing connector configuration settings

    Raises:
        ValueError: If any required configuration parameter is missing
    """
    required_configs = ["base_url"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def flatten_dict(d: Dict[str, Any], parent_key: str = "", sep: str = "_") -> Dict[str, Any]:
    """
    Flatten a nested dictionary structure.

    Args:
        d: Dictionary to flatten
        parent_key: Parent key for nested items
        sep: Separator to use between keys

    Returns:
        Flattened dictionary
    """
    items: List[tuple] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Convert lists to strings to maintain tabular structure
            items.append((new_key, json.dumps(v)))
        else:
            items.append((new_key, v))
    return dict(items)


def fetch_data(url: str, params: Dict[str, Any], api_key: Optional[str] = None) -> Dict[str, Any]:
    """
    Fetch data from the FDA API with retry logic and rate limiting.

    Args:
        url: API endpoint URL
        params: Query parameters
        api_key: Optional API key for authentication

    Returns:
        API response data

    Raises:
        RuntimeError: If API request fails after retries
    """
    headers = {}
    if api_key and api_key != "string":
        headers["Authorization"] = f"Basic {api_key}"
        params["api_key"] = api_key
        log.info("Using API key for authentication")
    else:
        log.warning(
            "No API key provided - using default rate limits (240 requests/min, 1,000 requests/day)"
        )

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise RuntimeError(f"Failed to fetch data after {MAX_RETRIES} attempts: {str(e)}")
            time.sleep(RETRY_DELAY * (attempt + 1))

    raise RuntimeError("Unexpected error in fetch_data")


def schema(configuration: dict) -> List[Dict[str, Any]]:
    """
    Define the schema for the FDA Food Enforcement data.
    Only primary keys are defined, letting Fivetran infer other column types.

    Args:
        configuration: Dictionary containing connector configuration settings

    Returns:
        List of table definitions
    """
    return [{"table": "fda_food_enforcement", "primary_key": ["recall_number", "event_id"]}]


def update(configuration: dict, state: dict) -> Generator[Any, None, None]:
    """
    Main update function that fetches and processes FDA Food Enforcement data.
    Processes only the first MAX_BATCHES batches (requests) for demonstration/testing.
    To process the full dataset, set MAX_BATCHES to None.

    Args:
        configuration: Dictionary containing connector configuration settings
        state: Dictionary containing state from previous sync

    Yields:
        Operations for Fivetran to process
    """
    log.info("Starting FDA Food Enforcement sync")

    # Validate configuration
    validate_configuration(configuration)

    # Extract configuration parameters
    api_key = configuration.get("api_key")
    base_url = configuration["base_url"]
    batch_size = int(configuration.get("batch_size", DEFAULT_BATCH_SIZE))

    # Adjust rate limit pause based on API key presence
    if api_key and api_key != "string":
        rate_limit_pause = float(configuration.get("rate_limit_pause", DEFAULT_RATE_LIMIT_PAUSE))
    else:
        rate_limit_pause = NO_API_KEY_RATE_LIMIT_PAUSE
        log.warning(f"Using longer rate limit pause ({rate_limit_pause}s) due to no API key")

    # Get last sync state
    last_sync_time = state.get("last_sync_time")
    skip = state.get("skip", 0)

    batch_count = 0  # Track how many batches have been processed
    try:
        while True:
            # Stop after MAX_BATCHES if set
            if MAX_BATCHES is not None and batch_count >= MAX_BATCHES:
                log.info(
                    f"Reached MAX_BATCHES ({MAX_BATCHES}). Exiting sync early for demonstration/testing."
                )
                break
            # Prepare query parameters
            params = {"limit": batch_size, "skip": skip}

            # Add date filter if we have a last sync time
            if last_sync_time:
                params["search"] = (
                    f"report_date:[{last_sync_time}+TO+{datetime.now().strftime('%Y%m%d')}]"
                )

            # Fetch data from API
            log.info(f"Fetching batch of {batch_size} records starting at offset {skip}")
            response_data = fetch_data(base_url, params, api_key)

            # Process results
            results = response_data.get("results", [])
            if not results:
                break

            # Process each record
            for record in results:
                # Flatten the record
                flattened_record = flatten_dict(record)

                # Convert date strings to ISO format
                for key, value in flattened_record.items():
                    if isinstance(value, str) and key.endswith(("_date", "_time")):
                        try:
                            dt = parser.parse(value)
                            if dt.tzinfo is None:
                                dt = pytz.UTC.localize(dt)
                            flattened_record[key] = dt.isoformat()
                        except (ValueError, TypeError):
                            pass

                # Yield upsert operation
                yield op.upsert("fda_food_enforcement", flattened_record)

            # Update skip for next batch
            skip += len(results)
            batch_count += 1  # Increment batch counter

            # Checkpoint progress
            new_state = {"skip": skip, "last_sync_time": datetime.now(pytz.UTC).strftime("%Y%m%d")}
            yield op.checkpoint(new_state)

            # Respect rate limits
            time.sleep(rate_limit_pause)

            # Check if we've processed all records
            if len(results) < batch_size:
                break

        log.info("FDA Food Enforcement sync completed successfully")

    except Exception as e:
        log.severe(f"Error during sync: {str(e)}")
        raise RuntimeError(f"Failed to sync FDA Food Enforcement data: {str(e)}")


# Initialize the connector
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("/configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
