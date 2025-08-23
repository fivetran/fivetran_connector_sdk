# Customer Thermometer API Connector for Fivetran
"""
This connector demonstrates how to fetch data from Customer Thermometer API
and upsert it into destination using the Fivetran Connector SDK.
"""
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Import additional libraries for data processing and API requests
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

# Constants for API configuration
BASE_URL = "https://app.customerthermometer.com/api.php"
DEFAULT_CHECKPOINT_INTERVAL = 100  # Checkpoint every 100 records
API_TIMEOUT_SECONDS = 30  # API request timeout in seconds
REQUIRED_CONFIG_PARAMS = ["api_key"]  # Required configuration parameters
MIN_API_KEY_LENGTH = 10  # Minimum length for a valid API key


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector
    has all necessary configuration values.

    Args:
        configuration (dict): A dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: If any required configuration parameter is missing or invalid.
    """
    # Check for required configuration parameters
    for key in REQUIRED_CONFIG_PARAMS:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    # Validate API key format (basic check)
    api_key = configuration.get("api_key")
    if not api_key or len(api_key) < MIN_API_KEY_LENGTH:
        raise ValueError("Invalid API key format")


def make_api_request(
    endpoint: str, api_key: str, from_date: Optional[str] = None, to_date: Optional[str] = None
) -> requests.Response:
    """
    Make an authenticated request to the Customer Thermometer API.

    Args:
        endpoint (str): The API method to call (e.g., 'getComments', 'getThermometers')
        api_key (str): The API key for authentication
        from_date (str, optional): Optional start date filter (YYYY-MM-DD format)
        to_date (str, optional): Optional end date filter (YYYY-MM-DD format)

    Returns:
        requests.Response: The API response object

    Raises:
        requests.RequestException: If the API request fails
    """
    params = {"apiKey": api_key, "getMethod": endpoint}

    # Add date filters if provided
    if from_date:
        params["fromDate"] = from_date
    if to_date:
        params["endDate"] = to_date

    try:
        response = requests.get(BASE_URL, params=params, timeout=API_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        log.error(f"API request failed for endpoint {endpoint}: {str(e)}")
        raise


def parse_xml_response(
    xml_content: str, root_element: str, child_element: str
) -> List[Dict[str, Any]]:
    """
    Parse XML response from Customer Thermometer API into list of dictionaries.

    Args:
        xml_content (str): The XML response content as string
        root_element (str): The root XML element name to search within
        child_element (str): The child XML element name to extract data from

    Returns:
        List[Dict[str, Any]]: List of dictionaries with flattened XML data

    Raises:
        ET.ParseError: If the XML content cannot be parsed
    """
    try:
        root = ET.fromstring(xml_content)
        results = []

        for item in root.findall(child_element):
            record = {}
            for child in item:
                # Convert XML element to dictionary, handling empty values
                value = child.text if child.text is not None else ""
                record[child.tag] = value
            results.append(record)

        return results
    except ET.ParseError as e:
        log.error(f"Failed to parse XML response: {str(e)}")
        raise


def fetch_comments(
    api_key: str, from_date: Optional[str] = None, to_date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetch comments data from Customer Thermometer API.
    Refer to getComments endpoint in the API documentation.

    Args:
        api_key (str): The API key for authentication
        from_date (str, optional): Start date filter in YYYY-MM-DD format
        to_date (str, optional): End date filter in YYYY-MM-DD format

    Returns:
        List[Dict[str, Any]]: List of comment records with extracted fields

    Raises:
        requests.RequestException: If the API request fails
        ET.ParseError: If the XML response cannot be parsed
    """
    response = make_api_request("getComments", api_key, from_date, to_date)
    return parse_xml_response(response.text, "comments", "comment")


def fetch_blast_results(
    api_key: str, from_date: Optional[str] = None, to_date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetch blast results data from Customer Thermometer API.
    Refer to getBlastResults endpoint in the API documentation.

    Args:
        api_key (str): The API key for authentication
        from_date (str, optional): Start date filter in YYYY-MM-DD format
        to_date (str, optional): End date filter in YYYY-MM-DD format

    Returns:
        List[Dict[str, Any]]: List of blast result records with response details

    Raises:
        requests.RequestException: If the API request fails
        ET.ParseError: If the XML response cannot be parsed
    """
    response = make_api_request("getBlastResults", api_key, from_date, to_date)
    return parse_xml_response(
        response.text, "thermometer_blast_responses", "thermometer_blast_response"
    )


def fetch_recipient_lists(api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch recipient lists from Customer Thermometer API.
    Refer to getRecipientLists endpoint in the API documentation.

    Args:
        api_key (str): The API key for authentication

    Returns:
        List[Dict[str, Any]]: List of recipient list records with IDs and metadata

    Raises:
        requests.RequestException: If the API request fails
        ET.ParseError: If the XML response cannot be parsed
    """
    response = make_api_request("getRecipientLists", api_key)
    return parse_xml_response(response.text, "recipient_lists", "recipient_list")


def fetch_thermometers(api_key: str) -> List[Dict[str, Any]]:
    """
    Fetch thermometers from Customer Thermometer API.
    Refer to getThermometers endpoint in the API documentation.

    Args:
        api_key (str): The API key for authentication

    Returns:
        List[Dict[str, Any]]: List of thermometer records with IDs and configuration

    Raises:
        requests.RequestException: If the API request fails
        ET.ParseError: If the XML response cannot be parsed
    """
    response = make_api_request("getThermometers", api_key)
    return parse_xml_response(response.text, "thermometers", "thermometer")


def fetch_metric_value(
    api_key: str,
    metric_endpoint: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fetch single metric values from Customer Thermometer API.
    Used for endpoints that return single values like getNumResponsesValue, getResponseRateValue, etc.

    Args:
        api_key (str): The API key for authentication
        metric_endpoint (str): The specific metric endpoint to call
        from_date (str, optional): Start date filter in YYYY-MM-DD format
        to_date (str, optional): End date filter in YYYY-MM-DD format

    Returns:
        Dict[str, Any]: Dictionary containing the metric value and metadata

    Raises:
        requests.RequestException: If the API request fails
    """
    response = make_api_request(metric_endpoint, api_key, from_date, to_date)

    # Create a record with the metric value and current timestamp
    current_time = datetime.now(timezone.utc).isoformat()
    metric_name = metric_endpoint.replace("get", "").replace("Value", "").lower()

    return {
        "metric_name": metric_name,
        "metric_value": response.text.strip(),
        "recorded_at": current_time,
        "from_date": from_date,
        "to_date": to_date,
    }


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    This function specifies the tables and their primary keys for the destination.

    Args:
        configuration (dict): A dictionary that holds the configuration settings for the connector.

    Returns:
        list: A list of dictionaries defining tables and their primary keys.
    """
    return [
        {
            "table": "comments",
            "primary_key": ["response_id"],
        },
        {
            "table": "blast_results",
            "primary_key": ["blast_id", "response_id", "thermometer_id"],
        },
        {
            "table": "recipient_lists",
            "primary_key": ["id"],
        },
        {
            "table": "thermometers",
            "primary_key": ["id"],
        },
        {
            "table": "metrics",
            "primary_key": ["metric_name", "recorded_at"],
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    This function fetches data from the Customer Thermometer API and loads it into destination tables.

    Args:
        configuration (dict): A dictionary containing connection details and API credentials
        state (dict): A dictionary containing state information from previous runs
                     The state dictionary is empty for the first sync or for any full re-sync

    Raises:
        ValueError: If configuration validation fails
        RuntimeError: If any error occurs during the sync process
    """
    log.warning("Example: Customer Thermometer API Connector")

    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)

    # Extract configuration parameters
    api_key = configuration.get("api_key")
    from_date = configuration.get("from_date")
    to_date = configuration.get("to_date")

    # Get the state variable for the sync, if needed
    last_sync_time = state.get("last_sync_time")
    records_processed = 0

    try:
        # Sync Comments data - Refer to fetch_comments function
        log.info("Fetching comments data...")
        comments = fetch_comments(api_key, from_date, to_date)
        for comment in comments:
            op.upsert(table="comments", data=comment)
            records_processed += 1

            # Checkpoint progress every DEFAULT_CHECKPOINT_INTERVAL records
            if records_processed % DEFAULT_CHECKPOINT_INTERVAL == 0:
                new_state = {
                    "last_sync_time": datetime.now(timezone.utc).isoformat(),
                    "records_processed": records_processed,
                }
                op.checkpoint(new_state)

        log.info(f"Processed {len(comments)} comment records")

        # Sync Blast Results data - Refer to fetch_blast_results function
        log.info("Fetching blast results data...")
        blast_results = fetch_blast_results(api_key, from_date, to_date)
        for blast_result in blast_results:
            op.upsert(table="blast_results", data=blast_result)
            records_processed += 1

            if records_processed % DEFAULT_CHECKPOINT_INTERVAL == 0:
                new_state = {
                    "last_sync_time": datetime.now(timezone.utc).isoformat(),
                    "records_processed": records_processed,
                }
                op.checkpoint(new_state)

        log.info(f"Processed {len(blast_results)} blast result records")

        # Sync Recipient Lists data - Refer to fetch_recipient_lists function
        log.info("Fetching recipient lists data...")
        recipient_lists = fetch_recipient_lists(api_key)
        for recipient_list in recipient_lists:
            op.upsert(table="recipient_lists", data=recipient_list)
            records_processed += 1

        log.info(f"Processed {len(recipient_lists)} recipient list records")

        # Sync Thermometers data - Refer to fetch_thermometers function
        log.info("Fetching thermometers data...")
        thermometers = fetch_thermometers(api_key)
        for thermometer in thermometers:
            op.upsert(table="thermometers", data=thermometer)
            records_processed += 1

        log.info(f"Processed {len(thermometers)} thermometer records")

        # Sync Metrics data - Refer to fetch_metric_value function for each metric endpoint
        log.info("Fetching metrics data...")
        metric_endpoints = [
            "getNumResponsesValue",
            "getResponseRateValue",
            "getTempRatingValue",
            "getNPSValue",
            "getHappinessValue",
            "getSendQuota",
        ]

        for endpoint in metric_endpoints:
            try:
                metric_data = fetch_metric_value(api_key, endpoint, from_date, to_date)
                op.upsert(table="metrics", data=metric_data)
                records_processed += 1
                log.info(f"Processed metric: {endpoint}")
            except Exception as e:
                log.warning(f"Failed to fetch metric {endpoint}: {str(e)}")
                continue

        # Final state update with the current sync time for the next run
        final_state = {
            "last_sync_time": datetime.now(timezone.utc).isoformat(),
            "records_processed": records_processed,
        }

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info(f"Sync completed successfully. Total records processed: {records_processed}")

    except requests.RequestException as e:
        # Handle API request failures
        log.error(f"API request failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data due to API error: {str(e)}")
    except ET.ParseError as e:
        # Handle XML parsing errors
        log.error(f"XML parsing failed: {str(e)}")
        raise RuntimeError(f"Failed to parse API response: {str(e)}")
    except ValueError as e:
        # Handle validation and data errors
        log.error(f"Validation error: {str(e)}")
        raise RuntimeError(f"Failed due to validation error: {str(e)}")
    except Exception as e:
        # Catch-all for unexpected errors
        log.error(f"Sync failed with unexpected error: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


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

# Aug 23, 2025 11:29:41 PM: INFO Fivetran-Tester-Process: SYNC PROGRESS:
# Operation       | Calls
# ----------------+------------
# Upserts         | 19
# Updates         | 0
# Deletes         | 0
# Truncates       | 0
# SchemaChanges   | 5
# Checkpoints     | 1
#
# Aug 23, 2025 11:29:41 PM: INFO Fivetran-Tester-Process: Sync SUCCEEDED
