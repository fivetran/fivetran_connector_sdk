"""
This example shows how to pull Organic Google Search results (top 6 results)
using SerpAPI and load it into a destination using the Fivetran Connector SDK.

This Fivetran Connector uses the SerpApi service to retrieve organic
Google Search results for a user-defined query. This connector demonstrates:
- Resilient API calls using exponential backoff and
retries (tenacity) to handle transient network errors.
- Flattening and unnesting of the structured JSON response
into a compatible tabular format.
- Data enrichment by merging search-level metadata
with individual organic results.
- Upserts to the destination table (organic_google_search_results)
using a composite primary key.

Refer to SerpAPI for more information (https://serpapi.com/search-api)
See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices)
for details.
"""

from typing import List, Dict, Any

# Import requests to make HTTP calls to API
import requests

# Import the json module to handle JSON data.
import json

# Imported for retries
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

# For supporting Data operations like Upsert(), Update(), Delete()
# and Checkpoint()
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete()
# and checkpoint()
from fivetran_connector_sdk import Operations as op


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required
    parameters.
    This function is called at the start of the update method to ensure that
    the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings
                       for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """

    # Define required configuration keys
    required_configs = ["api_key", "search_query"]

    # Check for missing keys or empty values
    for key in required_configs:
        value = configuration.get(key)
        if not value:
            raise ValueError(f"Missing required configuration value: {key}")


def schema(configuration: dict) -> List[Dict[str, Any]]:
    """
    Define the schema function which configures
    the tables delivered by the connector.
    """
    return [
        {
            "table": "organic_google_search_results",  # 'activity' table
            "primary_key": ["search_metadata_id", "position"],
        }
    ]


# Used for retry logic
@retry(
    # Start with 1 sec, double up to 60 sec
    wait=wait_exponential(min=1, max=60),
    stop=stop_after_attempt(5),  # Try up to 5 times
    # Retry only on specific network/server errors (5xx)
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    reraise=True,  # Re-raise the exception after the final failed attempt
)
def get_direct_google_search_results(query: str, api_key: str) -> dict:
    """
    Executes a Google Search via a direct HTTP request to the SerpApi endpoint
    with exponential backoff and retries on failure.

    If all retries fail, a requests.exceptions.RequestException will be raised.
    """
    log.info(f"Attempting API call for query: {query}")

    # 1. Define the API endpoint and parameters
    endpoint = "https://serpapi.com/search"

    params = {
        "engine": "google",
        "q": query,
        "hl": "en",
        "gl": "us",
        "google_domain": "google.com",
        "api_key": api_key,
    }

    # 2. Make the HTTP GET request
    response = requests.get(endpoint, params=params)

    # This will raise a requests.exceptions.RequestException
    # for 4xx or 5xx status codes.
    # The @retry decorator will catch this exception
    # and try again (up to 5 times).
    response.raise_for_status()

    results = response.json()

    # 3. Process and unnest the results
    unnested_data = {}

    # Extract metadata
    search_metadata = results.get("search_metadata", {})
    unnested_data["search_metadata_id"] = search_metadata.get("id")
    unnested_data["query_date"] = search_metadata.get("created_at")
    unnested_data["query_url"] = search_metadata.get("google_url")

    unnested_data["search_information_query_displayed"] = results.get(
        "search_information", {}
    ).get("query_displayed")

    # Extract search parameters
    search_params = results.get("search_parameters", {})
    for key in ["engine", "q", "google_domain", "hl", "gl", "device"]:
        unnested_data[f"search_parameters_{key}"] = search_params.get(key)

    # Extract organic results
    cleaned_organic_results = []
    for item in results.get("organic_results", []):
        cleaned_item = {
            "position": item.get("position"),
            "title": item.get("title"),
            "link": item.get("link"),
            "redirect_link": item.get("redirect_link"),
            "displayed_link": item.get("displayed_link"),
            "favicon": item.get("favicon"),
            "snippet": item.get("snippet"),
        }
        cleaned_organic_results.append(cleaned_item)

    unnested_data["organic_results"] = cleaned_organic_results

    return unnested_data


def sync_results(data: dict):
    """
    Takes the flattened search results dictionary, separates the organic
    results, enriches each record with the top-level metadata,
    and upserts them.

    Args:
        data (dict): The dictionary returned by
        get_direct_google_search_results.
        It contains metadata and a list of 'organic_results'.
    """
    # 1. Extract the list of organic results from the dictionary
    organic_results = data.get("organic_results", [])

    # 2. Extract the non-organic metadata fields for enrichment
    # We remove 'organic_results' from the main dict
    # so we're left with just metadata
    metadata = {k: v for k, v in data.items() if k != "organic_results"}

    # 3. Process and Upsert each record
    for record in organic_results:
        # Create a single, enriched record for the upsert
        # This combines the top-level metadata with the individual result data
        enriched_record = {
            **metadata,  # search_metadata
            **record,  # The individual position, title, link, etc.
        }

        # 4. Perform the upsert operation
        try:
            # The 'upsert' operation is used to insert or update data in the
            # destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data
            #   into.
            # - The second argument is a dictionary containing the data to be
            #   upserted
            op.upsert(
                table="organic_google_search_results",
                data=enriched_record,
            )
        except Exception as e:
            log.error(f"Error during upsert for record: {e}")

    log.info(f"Successfully processed and attempted to upsert " f"{len(organic_results)} records.")


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by
    Fivetran during each sync.
    See the technical reference documentation for more details on the update
    function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Args:
        configuration: A dictionary containing connection details.
        state: A dictionary containing state information from previous runs.
               The state dictionary is empty for the first sync or for any
               full re-sync.
    """
    validate_configuration(configuration)

    api_key = configuration["api_key"]
    search_query = configuration["search_query"]

    # The try/except block handles the case where all retries fail inside the
    # get_direct_google_search_results function.
    try:
        search_results = get_direct_google_search_results(search_query, api_key)

        # Check for the error key that was used in the previous non-retry logic
        if search_results and "error" in search_results:
            # If an error is returned (e.g., non-network/transient failure),
            # raise a final error.
            raise Exception(search_results["error"])

        sync_results(search_results)

    except requests.exceptions.RequestException as e:
        # This catches the error if the API call fails even after all 5 retries
        log.error(f"Fatal API error after all retries" " for query" f"'{search_query}': {e}")
        # Raising an exception here will fail the sync,
        # which is correct for a fatal error
        raise
    except Exception as e:
        log.error(f"An error occurred during sync: {e}")
        raise


# This creates the connector object
# that will use the update and schema functions.
connector = Connector(update=update, schema=schema)


# Standard Python entry point for local testing
# (not used by Fivetran in production)
# Please test using the Fivetran debug command prior
# to finalizing and deploying.
if __name__ == "__main__":

    try:
        with open("configuration.json", "r") as f:
            configuration = json.load(f)

        check_config = (
            not configuration.get("api_key") or configuration.get("api_key") == "YOUR_CLIENT_ID"
        )
        if check_config:
            log.warning("Please update configuration.json " "with actual api_key and api_secret.")

        connector.debug(configuration=configuration)

    except FileNotFoundError:
        log.error("Error: configuration.json not found. " "Please create it for local testing.")
    except Exception as e:
        log.error(f"An unexpected error occurred during debug execution: {e}")
