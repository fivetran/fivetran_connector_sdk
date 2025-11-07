"""This connector demonstrates how to fetch data from Checkly API and upsert it into destination using the Fivetran Connector SDK.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import required libraries
import json  # For JSON data handling and serialization
import requests  # For making HTTP requests to the Checkly API
import time  # For handling time-related functions like sleep for rate limiting

BASE_URL = "https://api.checklyhq.com/v1"
PAGE_SIZE = 100
MAX_RETRIES = 3
__RETRY_DELAY_SECONDS = 60

# Analytics metrics constants
NON_AGGREGATED_METRICS = [
    # Performance metrics
    "responseTime",
    "TTFB",
    # Core Web Vitals
    "FCP",
    "LCP",
    "CLS",
    "TBT",
    # Error metrics
    "consoleErrors",
    "networkErrors",
    "userScriptErrors",
    "documentErrors",
]

AGGREGATED_METRICS = [
    # General availability metrics
    "availability",
    "retries",
    # Response Time metrics
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
    # Time To First Byte (TTFB) metrics
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
    # First Contentful Paint (FCP) metrics
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
    # Largest Contentful Paint (LCP) metrics
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
    # Cumulative Layout Shift (CLS) metrics
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
    # Total Blocking Time (TBT) metrics
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
    # Console Errors metrics
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
    # Network Errors metrics
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
    # User Script Errors metrics
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
    # Document Errors metrics
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


def validate_aggregation_interval(configuration: dict):
    """
    Validates the 'aggregation_interval' value in the configuration dictionary.
    Checks if the 'aggregation_interval' key in the provided configuration is set to a valid integer
    between 1 and 43200. If not present, defaults to '60'. Raises a ValueError if the value is invalid.
    Args:
        configuration (dict): The configuration dictionary containing the 'aggregation_interval' key.
    Raises:
        ValueError: If 'aggregation_interval' is not a valid integer between 1 and 43200.
    """
    aggregation_interval = configuration.get("aggregation_interval", "60")
    try:
        interval = int(aggregation_interval)
        if interval < 1 or interval > 43200:
            raise ValueError("aggregation_interval must be a positive integer between 1 and 43200")
    except ValueError:
        raise ValueError("aggregation_interval must be a valid integer between 1 and 43200")


def validate_quick_range(configuration: dict):
    """
    Checks if the 'quick_range' key in the provided configuration is set to a valid value.
    If not present, defaults to 'last24Hours'. Raises a ValueError if the value is not one of the allowed options.
    Args:
        configuration (dict): The configuration dictionary containing the 'quick_range' key.
    Raises:
        ValueError: If 'quick_range' is not one of the valid options.
    """

    quick_range = configuration.get("quick_range", "last24Hours")
    valid_quick_ranges = [
        "last24Hours",
        "last7Days",
        "last30Days",
        "thisWeek",
        "thisMonth",
        "lastWeek",
        "lastMonth",
    ]
    if quick_range not in valid_quick_ranges:
        raise ValueError(f"quick_range must be one of: {', '.join(valid_quick_ranges)}")


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
    Make an API request to Checkly with proper error handling and retry logic.
    Args:
        url: Complete API endpoint URL with parameters
        headers: Request headers including authentication
    Returns:
        JSON response data (dict or list)
    Raises:
        RuntimeError: If API request fails after all retries
    """

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES - 1:  # Last attempt
                log.severe(f"API request failed after {MAX_RETRIES} attempts: {str(e)}")
                raise RuntimeError(f"API request failed: {str(e)}")
    return None


def get_analytics_data(configuration: dict, check_id: str, check_type: str, state: dict):
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

    # Get configuration parameters with defaults
    aggregation_interval = int(configuration.get("aggregation_interval", "60"))
    quick_range = configuration.get("quick_range", "last24Hours")

    headers = build_api_headers(configuration)

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
                    op.upsert(table=table_name, data=flattened_record)

                op.checkpoint(state)

        except Exception as e:
            log.warning(f"Error fetching {metrics_type} analytics for check {check_id}: {str(e)}")
            continue


def build_api_headers(configuration: dict) -> dict:
    """
    Build API headers for Checkly requests.
    Args:
        configuration: Configuration dictionary with API credentials
    Returns:
        Headers dictionary for API requests
    """
    api_key = configuration.get("api_key")
    account_id = configuration.get("account_id")

    return {
        "accept": "application/json",
        "x-checkly-account": account_id,
        "Authorization": f"Bearer {api_key}",
    }


def process_single_check(check_record: dict, configuration: dict, state: dict) -> None:
    """
    Process a single check record and handle analytics data if it's a browser check.
    Args:
        check_record: Individual check record from API response
        configuration: Configuration dictionary with API credentials
    """
    # Flatten the nested objects as required for destination table
    flattened_record = flatten_nested_objects(check_record)
    op.upsert(table="checks", data=flattened_record)

    # If this is a browser check, fetch analytics data
    check_type = check_record.get("checkType")
    check_id = check_record.get("id")

    if check_type == "BROWSER" and check_id:
        try:
            get_analytics_data(configuration, check_id, check_type, state)
        except Exception as e:
            log.severe(f"Failed to process analytics for check {check_id}: {str(e)}")
            # Continue with other checks for non-critical errors


def fetch_checks_page(page: int, headers: dict) -> list:
    """
    Fetch a single page of checks data from Checkly API.
    Args:
        page: Page number to fetch
        headers: API request headers
    Returns:
        List of check records from the API response
    """
    # Construct URL with query parameters for pagination
    url = f"{BASE_URL}/checks?limit={PAGE_SIZE}&page={page}&applyGroupSettings=false"

    response_data = make_api_request(url, headers)

    # Return empty list if no data or invalid response
    if not response_data or not isinstance(response_data, list):
        return []

    return response_data


def get_checks_data(configuration: dict, state: dict):
    """
    Fetch checks data from Checkly API with pagination support.
    Args:
        configuration: Configuration dictionary with API credentials
    Process check records and upsert them to destination table.
    """
    headers = build_api_headers(configuration)

    page = 1
    total_records = 0

    # Pagination loop to fetch all checks data from Checkly API
    # Each page returns up to __PAGE_SIZE records until all data is retrieved
    while True:
        try:
            response_data = fetch_checks_page(page, headers)

            # Break if no more data
            if not response_data:
                break

            # Process each check record in the current page
            for check_record in response_data:
                process_single_check(check_record, configuration, state)
                total_records += 1

            op.checkpoint(state)

            # Check if we have more data to fetch
            if len(response_data) < PAGE_SIZE:
                break

            page += 1

        except Exception as e:
            log.severe(f"Error fetching checks data on page {page}: {str(e)}")
            raise

    log.info(f"Successfully processed {total_records} check records")


def schema(configuration: dict):
    """
    schema of connector
    """
    return [
        {
            "table": "checks",  # Name of the table in the destination, required.
            "primary_key": ["id"],  # Primary key column(s) for the table, optional.
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "checkType": "STRING",
                "frequency": "INTEGER",
                "activated": "BOOLEAN",
                "muted": "BOOLEAN",
                "createdAt": "TIMESTAMP",
                "updatedAt": "TIMESTAMP",
                "lastRunAt": "TIMESTAMP",
                "nextRunAt": "TIMESTAMP",
                "locations": "STRING",
                "tags": "STRING",
                "alertChannels": "STRING",
                "script": "STRING",
                "environmentVariables": "STRING",
                "groupSettings": "STRING",
                "sslCheck": "BOOLEAN",
                "retries": "INTEGER",
                "maxResponseTime": "INTEGER",
                "url": "STRING",
                "httpMethod": "STRING",
                "headers": "STRING",
                "body": "STRING",
                "assertions": "STRING",
                "basicAuth": "STRING",
                "cookies": "STRING",
                "followRedirects": "BOOLEAN",
                "clicks": "STRING",
                "navigationTimings": "STRING",
                "viewports": "STRING",
                "deviceEmulation": "STRING",
                "geolocation": "STRING",
            },
        },
        {
            "table": "browser_checks_analytics_aggregated",
            # Name of the aggregated analytics table in the destination, required.
        },
        {
            "table": "browser_checks_analytics_non_aggregated",
            # Name of the non-aggregated analytics table in the destination, required.
        },
    ]


def update(configuration: dict, state: dict):
    """
    update method of connector
    """
    log.info("Checkly example")

    try:
        # Fetch and process checks data with pagination
        get_checks_data(configuration, state)

        log.info("Checkly sync completed successfully")

    except Exception as e:
        # In case of an exception, raise a runtime error
        raise RuntimeError(f"Failed to sync data: {str(e)}")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
