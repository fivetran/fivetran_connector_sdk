# This is an example for how to work with the fivetran_connector_sdk module.
# This connector demonstrates how to fetch data from Customer Thermometer API and upsert it into destination using the Fivetran Connector SDK.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk.
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import required libraries
import requests  # For making HTTP requests to the Common Paper API
import json  # For JSON data handling and serialization
import xml.etree.ElementTree as ET  # For parsing XML responses
from datetime import datetime, timezone  # For handling date and time
from typing import Dict, List, Any, Optional  # For type hinting

# Base URL for the Customer Thermometer API
__BASE_URL = "https://app.customerthermometer.com/api.php"
__DEFAULT_CHECKPOINT_INTERVAL = 100  # Checkpoint every 100 records
__API_TIMEOUT_SECONDS = 30  # API request timeout in seconds
__MIN_API_KEY_LENGTH = 10  # Minimum length for a valid API key

# Constants for metric endpoints
__METRIC_ENDPOINTS = [
    "getNumResponsesValue",
    "getResponseRateValue",
    "getTempRatingValue",
    "getNPSValue",
    "getHappinessValue",
    "getSendQuota",
]


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    # Check for required configuration parameters
    if "api_key" not in configuration:
        raise ValueError("Missing required configuration value: api_key")

    # Validate API key format (basic check)
    api_key = configuration.get("api_key")
    if len(api_key) < __MIN_API_KEY_LENGTH:
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
        response = requests.get(__BASE_URL, params=params, timeout=__API_TIMEOUT_SECONDS)
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
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
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


def process_comments(
    api_key: str, from_date: Optional[str] = None, to_date: Optional[str] = None
) -> None:
    """Process comments data from Customer Thermometer API."""
    log.info("Fetching comments data...")
    comments = fetch_comments(api_key, from_date, to_date)

    for comment in comments:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted
        op.upsert(table="comments", data=comment)

    log.info(f"Processed {len(comments)} comment records")


def process_blast_results(
    api_key: str, from_date: Optional[str] = None, to_date: Optional[str] = None
) -> None:
    """Process blast results data from Customer Thermometer API."""
    log.info("Fetching blast results data...")
    blast_results = fetch_blast_results(api_key, from_date, to_date)

    for blast_result in blast_results:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted
        op.upsert(table="blast_results", data=blast_result)

    log.info(f"Processed {len(blast_results)} blast result records")


def process_recipient_lists(api_key: str) -> None:
    """Process recipient lists data from Customer Thermometer API."""
    log.info("Fetching recipient lists data...")
    recipient_lists = fetch_recipient_lists(api_key)

    for recipient_list in recipient_lists:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted
        op.upsert(table="recipient_lists", data=recipient_list)

    log.info(f"Processed {len(recipient_lists)} recipient list records")


def process_thermometers(api_key: str) -> None:
    """Process thermometers data from Customer Thermometer API."""
    log.info("Fetching thermometers data...")
    thermometers = fetch_thermometers(api_key)

    for thermometer in thermometers:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted
        op.upsert(table="thermometers", data=thermometer)

    log.info(f"Processed {len(thermometers)} thermometer records")


def process_metrics(
    api_key: str, from_date: Optional[str] = None, to_date: Optional[str] = None
) -> None:
    """Process metrics data from Customer Thermometer API."""
    log.info("Fetching metrics data...")
    metrics_processed = 0

    for endpoint in __METRIC_ENDPOINTS:
        try:
            metric_data = fetch_metric_value(api_key, endpoint, from_date, to_date)
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert(table="metrics", data=metric_data)
            metrics_processed += 1
            log.info(f"Processed metric: {endpoint}")
        except Exception as e:
            log.warning(f"Failed to fetch metric {endpoint}: {str(e)}")
            continue


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: Source Examples : Customer Thermometer")

    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)

    # Extract configuration parameters (validated above, so safe to assume they exist)
    api_key: str = configuration["api_key"]
    from_date = configuration.get("from_date")
    to_date = configuration.get("to_date")

    try:
        # Process each endpoint
        process_comments(api_key, from_date, to_date)
        process_blast_results(api_key, from_date, to_date)
        process_recipient_lists(api_key)
        process_thermometers(api_key)
        process_metrics(api_key, from_date, to_date)

        log.info(f"Sync completed successfully. Processed all endpoints.")

    except requests.RequestException as e:
        # Handle API request failures
        log.severe(f"API request failed: {str(e)}")
        raise RuntimeError(f"Failed to sync data due to API error: {str(e)}")
    except ET.ParseError as e:
        # Handle XML parsing errors
        log.severe(f"XML parsing failed: {str(e)}")
        raise RuntimeError(f"Failed to parse API response: {str(e)}")
    except ValueError as e:
        # Handle validation and data errors
        log.severe(f"Validation error: {str(e)}")
        raise RuntimeError(f"Failed due to validation error: {str(e)}")
    except Exception as e:
        # Catch-all for unexpected errors
        log.severe(f"Sync failed with unexpected error: {str(e)}")
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
