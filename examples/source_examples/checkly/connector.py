# This is an example for how to work with the fivetran_connector_sdk module.
"""
Checkly Connector: This connector demonstrates how to fetch data from Checkly API and upsert it into destination using the Fivetran Connector SDK.
"""
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details


# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op


""" Add your source-specific imports here
requests - for making HTTP API calls to Checkly API
json - for handling JSON configuration and response data
time - for rate limiting between API requests"""
import json
import time


"""
GUIDELINES TO FOLLOW WHILE WRITING AN EXAMPLE CONNECTOR:
- Import only the necessary modules and libraries to keep the code clean and efficient.
- Use clear, consistent and descriptive names for your functions and variables.
- For constants and global variables, use uppercase letters with underscores (e.g. CHECKPOINT_INTERVAL, TABLE_NAME).
- Add comments to explain the purpose of each function in the docstring.
- Add comments to explain the purpose of complex logic within functions, where necessary.
- Add comments to highlight where users can make changes to the code to suit their specific use case.
- Split your code into smaller functions to improve readability and maintainability where required.
- Use logging to provide useful information about the connector's execution. Do not log excessively.
- Implement error handling to catch exceptions and log them appropriately. Catch specific exceptions where possible.
- Define the complete data model with primary key and data types in the schema function.
- Ensure that the connector does not load all data into memory at once. This can cause memory overflow errors. Use pagination or streaming where possible.
- Add comments to explain pagination or streaming logic to help users understand how to handle large datasets.
- Add comments for upsert, update and delete to explain the purpose of upsert, update and delete. This will help users understand the upsert, update and delete processes.
- Checkpoint your state at regular intervals to ensure that the connector can resume from the last successful sync in case of interruptions.
- Add comments for checkpointing to explain the purpose of checkpoint. This will help users understand the checkpointing process.
- Refer to the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices)
"""

# Constants
BASE_URL = "https://api.checklyhq.com/v1"
PAGE_SIZE = 100
RATE_LIMIT_DELAY_SECONDS = 0.1
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5

# Analytics metrics constants
NON_AGGREGATED_METRICS = [
    "responseTime",
    "TTFB",
    "FCP",
    "LCP",
    "CLS",
    "TBT",
    "consoleErrors",
    "networkErrors",
    "userScriptErrors",
    "documentErrors",
]

AGGREGATED_METRICS = [
    "availability",
    "retries",
    "responseTime_avg",
    "responseTime_max",
    "responseTime_median",
    "responseTime_min",
    "responseTime_p50",
    "responseTime_p90",
    "responseTime_p95",
    "responseTime_p99",
    "responseTime_stddev",
    "responseTime_sum",
    "TTFB_avg",
    "TTFB_max",
    "TTFB_median",
    "TTFB_min",
    "TTFB_p50",
    "TTFB_p90",
    "TTFB_p95",
    "TTFB_p99",
    "TTFB_stddev",
    "TTFB_sum",
    "FCP_avg",
    "FCP_max",
    "FCP_median",
    "FCP_min",
    "FCP_p50",
    "FCP_p90",
    "FCP_p95",
    "FCP_p99",
    "FCP_stddev",
    "FCP_sum",
    "LCP_avg",
    "LCP_max",
    "LCP_median",
    "LCP_min",
    "LCP_p50",
    "LCP_p90",
    "LCP_p95",
    "LCP_p99",
    "LCP_stddev",
    "LCP_sum",
    "CLS_avg",
    "CLS_max",
    "CLS_median",
    "CLS_min",
    "CLS_p50",
    "CLS_p90",
    "CLS_p95",
    "CLS_p99",
    "CLS_stddev",
    "CLS_sum",
    "TBT_avg",
    "TBT_max",
    "TBT_median",
    "TBT_min",
    "TBT_p50",
    "TBT_p90",
    "TBT_p95",
    "TBT_p99",
    "TBT_stddev",
    "TBT_sum",
    "consoleErrors_avg",
    "consoleErrors_max",
    "consoleErrors_median",
    "consoleErrors_min",
    "consoleErrors_p50",
    "consoleErrors_p90",
    "consoleErrors_p95",
    "consoleErrors_p99",
    "consoleErrors_stddev",
    "consoleErrors_sum",
    "networkErrors_avg",
    "networkErrors_max",
    "networkErrors_median",
    "networkErrors_min",
    "networkErrors_p50",
    "networkErrors_p90",
    "networkErrors_p95",
    "networkErrors_p99",
    "networkErrors_stddev",
    "networkErrors_sum",
    "userScriptErrors_avg",
    "userScriptErrors_max",
    "userScriptErrors_median",
    "userScriptErrors_min",
    "userScriptErrors_p50",
    "userScriptErrors_p90",
    "userScriptErrors_p95",
    "userScriptErrors_p99",
    "userScriptErrors_stddev",
    "userScriptErrors_sum",
    "documentErrors_avg",
    "documentErrors_max",
    "documentErrors_median",
    "documentErrors_min",
    "documentErrors_p50",
    "documentErrors_p90",
    "documentErrors_p95",
    "documentErrors_p99",
    "documentErrors_stddev",
    "documentErrors_sum",
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
    # Validate required configuration parameters
    required_configs = ["api_key", "account_id"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    # Validate optional analytics parameters with defaults
    quick_range = configuration.get("quick_range", "last24Hours")
    valid_quick_ranges = ["last24Hours", "last7Days", "last30Days", "last90Days"]
    if quick_range not in valid_quick_ranges:
        raise ValueError(f"quick_range must be one of: {', '.join(valid_quick_ranges)}")

    # Validate aggregation_interval if provided
    aggregation_interval = configuration.get("aggregation_interval", "60")
    try:
        interval = int(aggregation_interval)
        if interval < 1:
            raise ValueError("aggregation_interval must be a positive integer")
    except ValueError:
        raise ValueError("aggregation_interval must be a valid integer")


def flatten_nested_objects(data: dict, parent_key: str = "", separator: str = "_") -> dict:
    """
    Flatten nested dictionary objects into a single level dictionary.

    Args:
        data: Dictionary to flatten
        parent_key: Parent key for nested objects
        separator: Separator to use between parent and child keys

    Returns:
        Flattened dictionary
    """
    items = []

    for key, value in data.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key

        if isinstance(value, dict):
            # Recursively flatten nested dictionaries
            items.extend(flatten_nested_objects(value, new_key, separator).items())
        elif isinstance(value, list):
            # Convert arrays to comma-separated strings as mentioned in notes
            if value and all(isinstance(item, str) for item in value):
                items.append((new_key, ",".join(value)))
            else:
                # For complex arrays, convert to JSON string
                items.append((new_key, json.dumps(value)))
        else:
            items.append((new_key, value))

    return dict(items)


def make_api_request(url: str, headers: dict):
    """
    Make an API request to Checkly with proper error handling and rate limiting.

    Args:
        url: Complete API endpoint URL with parameters
        headers: Request headers including authentication

    Returns:
        JSON response data (dict or list)

    Raises:
        RuntimeError: If API request fails
    """
    # Import requests here to avoid global import as it's pre-installed
    import requests

    try:
        # Add rate limiting delay to avoid hitting API limits
        time.sleep(RATE_LIMIT_DELAY_SECONDS)

        # Set a timeout for the request to prevent hanging
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 429:
            log.warning(f"Rate limit hit, waiting {RETRY_DELAY_SECONDS} seconds before retry")
            time.sleep(RETRY_DELAY_SECONDS)
            response = requests.get(url, headers=headers, timeout=30)

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        log.error(f"API request failed for URL: {url}")
        raise RuntimeError(f"API request failed: {str(e)}")


def get_analytics_data(configuration: dict, check_id: str, check_type: str):
    """
    Fetch analytics data for browser checks from Checkly API.

    Args:
        configuration: Configuration dictionary with API credentials
        check_id: The check ID to fetch analytics for
        check_type: The type of check (only process if it's BROWSER)

    Process analytics records and upsert them to destination tables.
    """
    # Only fetch analytics for browser checks
    if check_type != "BROWSER":
        return

    api_key = configuration.get("api_key")
    account_id = configuration.get("account_id")

    # Get configuration parameters with defaults
    aggregation_interval = int(configuration.get("aggregation_interval", "60"))
    quick_range = configuration.get("quick_range", "last24Hours")

    headers = {
        "accept": "application/json",
        "x-checkly-account": account_id,
        "Authorization": f"Bearer {api_key}",
    }

    # Process both aggregated and non-aggregated metrics
    for metrics_type, metrics_list in [
        ("non_aggregated", NON_AGGREGATED_METRICS),
        ("aggregated", AGGREGATED_METRICS),
    ]:
        try:
            # Build metrics query parameters
            metrics_params = "&".join([f"metrics={metric}" for metric in metrics_list])

            # Construct URL with quickRange - single API call, no pagination
            url = f"{BASE_URL}/analytics/browser-checks/{check_id}?quickRange={quick_range}&aggregationInterval={aggregation_interval}&{metrics_params}"

            response_data = make_api_request(url, headers)

            if not response_data or not isinstance(response_data, dict):
                continue

            # Process each series item in the response
            series = response_data.get("series", [])
            for series_item in series:
                data_points = series_item.get("data", [])

                # For each data point, create a record
                for data_point in data_points:
                    analytics_record = {
                        "check_id": check_id,
                        "quick_range": quick_range,
                        "aggregation_interval": aggregation_interval,
                    }
                    analytics_record.update(data_point)

                    # Flatten the nested objects as required
                    flattened_record = flatten_nested_objects(analytics_record)

                    # Determine the target table based on metrics type
                    table_name = f"browser_checks_analytics_{metrics_type}"

                    # The 'upsert' operation is used to insert or update data in the destination table.
                    op.upsert(table=table_name, data=flattened_record)

        except Exception as e:
            log.warning(f"Error fetching {metrics_type} analytics for check {check_id}: {str(e)}")
            # Continue with next metrics type instead of failing completely
            continue


def get_checks_data(configuration: dict, state: dict):
    """
    Fetch checks data from Checkly API with pagination support.

    Args:
        configuration: Configuration dictionary with API credentials
        state: State dictionary for incremental sync support

    Process check records and upsert them to destination table.
    """
    api_key = configuration.get("api_key")
    account_id = configuration.get("account_id")

    headers = {
        "accept": "application/json",
        "x-checkly-account": account_id,
        "Authorization": f"Bearer {api_key}",
    }

    page = 1
    has_more_data = True
    total_records = 0
    browser_checks_found = 0

    # Pagination loop to fetch all checks data from Checkly API
    # Each page returns up to PAGE_SIZE records until all data is retrieved
    while has_more_data:
        # Construct URL with query parameters for pagination
        url = f"{BASE_URL}/checks?limit={PAGE_SIZE}&page={page}&applyGroupSettings=false"

        try:
            response_data = make_api_request(url, headers)

            if not response_data or len(response_data) == 0:
                has_more_data = False
                break

            for check_record in response_data:
                # Flatten the nested objects as required for destination table
                flattened_record = flatten_nested_objects(check_record)

                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table="checks", data=flattened_record)
                total_records += 1

                # If this is a browser check, fetch analytics data
                check_type = check_record.get("checkType")
                check_id = check_record.get("id")

                if check_type == "BROWSER" and check_id:
                    browser_checks_found += 1
                    try:
                        get_analytics_data(configuration, check_id, check_type)
                    except Exception as e:
                        log.error(f"Failed to process analytics for check {check_id}: {str(e)}")
                        # Continue with other checks for non-critical errors
                        continue

            # Check if we have more data to fetch
            if len(response_data) < PAGE_SIZE:
                has_more_data = False
            else:
                page += 1

        except Exception as e:
            log.error(f"Error fetching checks data on page {page}: {str(e)}")
            raise

    log.info(f"Successfully processed {total_records} check records")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    schema_definition = [
        {
            "table": "checks",  # Name of the table in the destination, required.
            "primary_key": ["id"],  # Primary key column(s) for the table, optional.
        },
        {
            "table": "browser_checks_analytics_aggregated",  # Name of the aggregated analytics table in the destination, required.
            "primary_key": ["check_id"],  # Primary key for aggregated analytics data, optional.
        },
        {
            "table": "browser_checks_analytics_non_aggregated",  # Name of the non-aggregated analytics table in the destination, required.
            "primary_key": [
                "check_id"
            ],  # Primary key for non-aggregated analytics data, optional.
        },
    ]

    return schema_definition


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
    log.warning("Example: API Connector : Checkly Checks")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # Get the state variable for the sync, if needed
    last_sync_time = state.get("last_sync_time")

    if last_sync_time:
        log.info(f"Incremental sync from last sync time: {last_sync_time}")
    else:
        log.info("Performing full sync - no previous state found")

    try:
        # Fetch and process checks data with pagination
        get_checks_data(configuration, state)

        log.info("Checkly sync completed successfully")

    except Exception as e:
        # In case of an exception, raise a runtime error
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    try:
        with open("configuration.json", "r") as f:
            configuration = json.load(f)
    except Exception as e:
        print(f"Failed to load configuration.json: {str(e)}")
        raise

    # Test the connector locally
    try:
        connector.debug(configuration=configuration)
        print("Connector debug execution completed successfully")
    except Exception as e:
        print(f"Connector debug execution failed: {str(e)}")
        raise
