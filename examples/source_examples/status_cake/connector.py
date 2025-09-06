"""This connector fetches uptime test data, history, periods, and alerts from StatusCake API and upsert it into destination using the Fivetran Connector SDK.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# Import required classes from fivetran_connector_sdk.
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

import json  # For reading configuration from a JSON file
import requests  # For making HTTP requests to StatusCake API
import time  # For implementing retry delays and handling rate limiting

__BASE_URL = "https://api.statuscake.com/v1"  # Base URL for StatusCake API
__MAX_RETRIES = 3  # Maximum number of retries for API requests
__RETRY_DELAY = 1  # Retry delay in seconds
__RATE_LIMIT_DELAY = 2  # Rate limit delay in seconds


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
    required_configs = ["api_key"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration parameter: {key}")


def make_api_request(url, headers, max_retries=__MAX_RETRIES):
    """
    Make an API request with retry logic for handling rate limiting and transient errors.
    Args:
        url: The API endpoint URL
        headers: Request headers including authentication
        max_retries: Maximum number of retry attempts
    Returns:
        Response data as JSON or None if all retries failed
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)

            # Handle rate limiting (429 status code)
            if response.status_code == 429:
                log.warning(
                    f"Rate limit exceeded, waiting {__RATE_LIMIT_DELAY} seconds before retry"
                )
                time.sleep(__RATE_LIMIT_DELAY)
                continue

            # Handle other HTTP errors
            if response.status_code != 200:
                log.warning(
                    f"API request failed with status {response.status_code}: {response.text}"
                )
                if attempt < max_retries - 1:
                    time.sleep(__RETRY_DELAY * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    raise Exception(f"API request failed after {max_retries} attempts")

            return response.json()

        except requests.exceptions.RequestException as e:
            log.warning(f"Request exception on attempt {attempt + 1}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(__RETRY_DELAY * (attempt + 1))  # Exponential backoff
            else:
                raise Exception(f"API request failed after {max_retries} attempts: {str(e)}")

    return None


def flatten_dictionary(data, parent_key="", separator="_"):
    """
    Flatten nested dictionary structures to create flat key-value pairs for table columns.
    Args:
        data: Dictionary to flatten
        parent_key: Parent key prefix
        separator: Key separator character
    Returns:
        Flattened dictionary
    """
    items = []
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{parent_key}{separator}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dictionary(v, new_key, separator=separator).items())
            elif isinstance(v, list):
                # Convert lists to comma-separated strings for easier handling
                items.append((new_key, ",".join(map(str, v)) if v else ""))
            else:
                items.append((new_key, v))
    return dict(items)


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
            "table": "uptime_tests",  # Table for main uptime test data
            "primary_key": ["id"],  # Primary key column(s) for the table
        },
        {
            "table": "uptime_test_history",  # Table for uptime test history records
            "primary_key": ["test_id", "created_at"],  # Composite primary key
        },
        {
            "table": "uptime_test_periods",  # Table for uptime test periods data
            "primary_key": ["test_id", "created_at"],  # Composite primary key
        },
        {
            "table": "uptime_test_alerts",  # Table for uptime test alerts data
            "primary_key": ["test_id", "id"],  # Composite primary key
        },
    ]


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

    log.warning("Example: Source Examples : StatusCake")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # Extract configuration parameters as required
    api_key = configuration.get("api_key")

    # Set up API headers for authentication
    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        # Fetch all uptime tests first - this is the primary data source
        uptime_tests = fetch_uptime_tests(headers)

        # Process each uptime test and its related data
        for test in uptime_tests:
            test_id = test.get("id")

            # Flatten the test data for upsert
            flattened_test = flatten_dictionary(test)

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # 1. The table name where data should be inserted/updated
            # 2. The data record as a dictionary with column names as keys
            op.upsert(table="uptime_tests", data=flattened_test)

            # Process related data for this uptime test
            process_uptime_test_history(test_id, headers)
            process_uptime_test_periods(test_id, headers)
            process_uptime_test_alerts(test_id, headers)

    except Exception as e:
        log.error(f"Error during sync: {str(e)}")
        raise


def fetch_uptime_tests(headers):
    """
    Fetch all uptime tests from the StatusCake API.
    Args:
        headers: HTTP headers including authentication
    Returns:
        List of uptime test dictionaries
    """
    url = f"{__BASE_URL}/uptime"
    response_data = make_api_request(url, headers)

    if response_data and "data" in response_data:
        log.info(f"Fetched {len(response_data['data'])} uptime tests")
        return response_data["data"]

    log.warning("No uptime tests data found")
    return []


def fetch_uptime_test_history(test_id, headers):
    """
    Fetch history data for a specific uptime test.
    Args:
        test_id: ID of the uptime test
        headers: HTTP headers including authentication
    Returns:
        List of history record dictionaries
    """
    url = f"{__BASE_URL}/uptime/{test_id}/history"
    response_data = make_api_request(url, headers)

    if response_data and "data" in response_data:
        log.info(f"Fetched {len(response_data['data'])} history records for test {test_id}")
        return response_data["data"]

    return []


def fetch_uptime_test_periods(test_id, headers):
    """
    Fetch periods data for a specific uptime test.
    Args:
        test_id: ID of the uptime test
        headers: HTTP headers including authentication
    Returns:
        List of period record dictionaries
    """
    url = f"{__BASE_URL}/uptime/{test_id}/periods"
    response_data = make_api_request(url, headers)

    if response_data and "data" in response_data:
        log.info(f"Fetched {len(response_data['data'])} period records for test {test_id}")
        return response_data["data"]

    return []


def fetch_uptime_test_alerts(test_id, headers):
    """
    Fetch alerts data for a specific uptime test.
    Args:
        test_id: ID of the uptime test
        headers: HTTP headers including authentication
    Returns:
        List of alert record dictionaries
    """
    url = f"{__BASE_URL}/uptime/{test_id}/alerts"
    response_data = make_api_request(url, headers)

    if response_data and "data" in response_data:
        log.info(f"Fetched {len(response_data['data'])} alert records for test {test_id}")
        return response_data["data"]

    return []


def process_uptime_test_history(test_id, headers):
    """
    Process and upsert history data for a specific uptime test.
    Args:
        test_id: ID of the uptime test
        headers: HTTP headers including authentication
    """
    history_data = fetch_uptime_test_history(test_id, headers)
    for history_record in history_data:
        history_record["test_id"] = test_id  # Add test_id reference
        flattened_history = flatten_dictionary(history_record)
        op.upsert(table="uptime_test_history", data=flattened_history)


def process_uptime_test_periods(test_id, headers):
    """
    Process and upsert periods data for a specific uptime test.
    Args:
        test_id: ID of the uptime test
        headers: HTTP headers including authentication
    """
    periods_data = fetch_uptime_test_periods(test_id, headers)
    for period_record in periods_data:
        period_record["test_id"] = test_id  # Add test_id reference
        flattened_period = flatten_dictionary(period_record)
        op.upsert(table="uptime_test_periods", data=flattened_period)


def process_uptime_test_alerts(test_id, headers):
    """
    Process and upsert alerts data for a specific uptime test.
    Args:
        test_id: ID of the uptime test
        headers: HTTP headers including authentication
    """
    alerts_data = fetch_uptime_test_alerts(test_id, headers)
    for alert_record in alerts_data:
        alert_record["test_id"] = test_id  # Add test_id reference
        flattened_alert = flatten_dictionary(alert_record)
        op.upsert(table="uptime_test_alerts", data=flattened_alert)


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
