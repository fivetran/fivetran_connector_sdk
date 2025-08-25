# New Relic Feature APIs Connector
"""This connector demonstrates how to fetch data from New Relic feature APIs and upsert it into destination using the Fivetran Connector SDK.
Supports querying data from APM, Infrastructure, Browser monitoring, Mobile monitoring, and Synthetic monitoring APIs.
"""
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Import required libraries for API interactions
import requests
import json
from datetime import datetime
from typing import Dict, List, Any, Optional


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["api_key", "account_id", "region"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    # Validate API key format
    if not configuration.get("api_key", "").startswith("NRAK-"):
        raise ValueError("API key should start with 'NRAK-'")

    # Validate region
    valid_regions = ["US", "EU"]
    if configuration.get("region") not in valid_regions:
        raise ValueError(f"Region must be one of: {valid_regions}")


def get_newrelic_endpoint(region: str) -> str:
    """
    Get the appropriate New Relic endpoint based on region.
    Args:
        region: The New Relic region (US or EU)
    Returns:
        The base URL for New Relic API
    """
    if region == "EU":
        return "https://api.eu.newrelic.com"
    return "https://api.newrelic.com"


def execute_nerdgraph_query(query: str, api_key: str, region: str) -> Dict[str, Any]:
    """
    Execute a NerdGraph query against New Relic API.
    Args:
        query: The GraphQL query to execute
        api_key: New Relic API key
        region: New Relic region
    Returns:
        The response data from the API
    """
    endpoint = get_newrelic_endpoint(region)
    url = f"{endpoint}/graphql"

    headers = {"API-Key": api_key, "Content-Type": "application/json"}

    payload = {"query": query}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        log.severe(f"Failed to execute NerdGraph query: {str(e)}")
        raise RuntimeError(f"API request failed: {str(e)}")


def get_time_range(last_sync_time: Optional[str] = None) -> str:
    """
    Generate dynamic time range for NRQL queries.
    Args:
        last_sync_time: Last sync timestamp for incremental sync
    Returns:
        Time range string for NRQL query
    """
    if last_sync_time:
        # Incremental sync: get data since last sync
        return f'SINCE "{last_sync_time}"'
    else:
        # Initial sync: get all available data (last 90 days as default)
        return "SINCE 90 days ago"


def get_apm_data(
    api_key: str, region: str, account_id: str, last_sync_time: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetch APM (Application Performance Monitoring) data from New Relic.
    Args:
        api_key: New Relic API key
        region: New Relic region
        account_id: New Relic account ID
        last_sync_time: Last sync timestamp for incremental sync
    Returns:
        List of APM application data
    """
    time_range = get_time_range(last_sync_time)
    query = f"""
    {{
      actor {{
        account(id: {account_id}) {{
          nrql(query: "SELECT * FROM Transaction {time_range} LIMIT 1000") {{
            results
          }}
        }}
      }}
    }}
    """

    response = execute_nerdgraph_query(query, api_key, region)

    apm_data = []
    if (
        response.get("data", {})
        .get("actor", {})
        .get("account", {})
        .get("nrql", {})
        .get("results")
    ):
        results = response["data"]["actor"]["account"]["nrql"]["results"]
        for result in results:
            apm_data.append(
                {
                    "account_id": account_id,
                    "timestamp": datetime.now().isoformat(),
                    "transaction_name": result.get("transactionName", ""),
                    "duration": result.get("duration", 0),
                    "error_rate": result.get("errorRate", 0),
                    "throughput": result.get("throughput", 0),
                    "apdex_score": result.get("apdexScore", 0),
                }
            )

    return apm_data


def get_infrastructure_data(
    api_key: str, region: str, account_id: str
) -> List[Dict[str, Any]]:
    """
    Fetch Infrastructure monitoring data from New Relic.
    Args:
        api_key: New Relic API key
        region: New Relic region
        account_id: New Relic account ID
    Returns:
        List of infrastructure host data
    """
    query = (
        """
    {
      actor {
        entitySearch(query: "domain = 'INFRA' AND type = 'HOST' AND accountId = %s") {
          count
          results {
            entities {
              name
              accountId
              domain
              type
              reporting
              tags {
                key
                values
              }
            }
          }
        }
      }
    }
    """
        % account_id
    )

    response = execute_nerdgraph_query(query, api_key, region)

    infra_data = []
    if (
        response.get("data", {})
        .get("actor", {})
        .get("entitySearch", {})
        .get("results", {})
        .get("entities")
    ):
        entities = response["data"]["actor"]["entitySearch"]["results"]["entities"]
        for entity in entities:
            infra_data.append(
                {
                    "account_id": account_id,
                    "timestamp": datetime.now().isoformat(),
                    "host_name": entity.get("name", ""),
                    "domain": entity.get("domain", ""),
                    "type": entity.get("type", ""),
                    "reporting": entity.get("reporting", False),
                    "tags": json.dumps(entity.get("tags", [])),
                }
            )

    return infra_data


def get_browser_data(
    api_key: str, region: str, account_id: str, last_sync_time: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetch Browser monitoring data from New Relic.
    Args:
        api_key: New Relic API key
        region: New Relic region
        account_id: New Relic account ID
        last_sync_time: Last sync timestamp for incremental sync
    Returns:
        List of browser monitoring data
    """
    time_range = get_time_range(last_sync_time)
    query = f"""
    {{
      actor {{
        account(id: {account_id}) {{
          nrql(query: "SELECT * FROM PageView {time_range} LIMIT 1000") {{
            results
          }}
        }}
      }}
    }}
    """

    response = execute_nerdgraph_query(query, api_key, region)

    browser_data = []
    if (
        response.get("data", {})
        .get("actor", {})
        .get("account", {})
        .get("nrql", {})
        .get("results")
    ):
        results = response["data"]["actor"]["account"]["nrql"]["results"]
        for result in results:
            browser_data.append(
                {
                    "account_id": account_id,
                    "timestamp": datetime.now().isoformat(),
                    "page_url": result.get("pageUrl", ""),
                    "browser_name": result.get("browserName", ""),
                    "browser_version": result.get("browserVersion", ""),
                    "device_type": result.get("deviceType", ""),
                    "load_time": result.get("loadTime", 0),
                    "dom_content_loaded": result.get("domContentLoaded", 0),
                }
            )

    return browser_data


def get_mobile_data(
    api_key: str, region: str, account_id: str, last_sync_time: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetch Mobile monitoring data from New Relic.
    Args:
        api_key: New Relic API key
        region: New Relic region
        account_id: New Relic account ID
        last_sync_time: Last sync timestamp for incremental sync
    Returns:
        List of mobile monitoring data
    """
    time_range = get_time_range(last_sync_time)
    query = f"""
    {{
      actor {{
        account(id: {account_id}) {{
          nrql(query: "SELECT * FROM Mobile WHERE appName IS NOT NULL {time_range} LIMIT 1000") {{
            results
          }}
        }}
      }}
    }}
    """

    response = execute_nerdgraph_query(query, api_key, region)

    mobile_data = []
    if (
        response.get("data", {})
        .get("actor", {})
        .get("account", {})
        .get("nrql", {})
        .get("results")
    ):
        results = response["data"]["actor"]["account"]["nrql"]["results"]
        for result in results:
            mobile_data.append(
                {
                    "account_id": account_id,
                    "timestamp": datetime.now().isoformat(),
                    "app_name": result.get("appName", ""),
                    "platform": result.get("platform", ""),
                    "version": result.get("version", ""),
                    "device_model": result.get("deviceModel", ""),
                    "os_version": result.get("osVersion", ""),
                    "crash_count": result.get("crashCount", 0),
                }
            )

    return mobile_data


def get_synthetic_data(
    api_key: str, region: str, account_id: str, last_sync_time: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetch Synthetic monitoring data from New Relic.
    Args:
        api_key: New Relic API key
        region: New Relic region
        account_id: New Relic account ID
        last_sync_time: Last sync timestamp for incremental sync
    Returns:
        List of synthetic monitoring data
    """
    time_range = get_time_range(last_sync_time)
    query = f"""
    {{
      actor {{
        account(id: {account_id}) {{
          nrql(query: "SELECT * FROM SyntheticCheck {time_range} LIMIT 1000") {{
            results
          }}
        }}
      }}
    }}
    """

    response = execute_nerdgraph_query(query, api_key, region)

    synthetic_data = []
    if (
        response.get("data", {})
        .get("actor", {})
        .get("account", {})
        .get("nrql", {})
        .get("results")
    ):
        results = response["data"]["actor"]["account"]["nrql"]["results"]
        for result in results:
            synthetic_data.append(
                {
                    "account_id": account_id,
                    "timestamp": datetime.now().isoformat(),
                    "monitor_name": result.get("monitorName", ""),
                    "monitor_type": result.get("monitorType", ""),
                    "status": result.get("status", ""),
                    "response_time": result.get("responseTime", 0),
                    "location": result.get("location", ""),
                    "error_message": result.get("errorMessage", ""),
                }
            )

    return synthetic_data


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "apm_data",
            "primary_key": ["account_id", "timestamp", "transaction_name"],
            "columns": {
                "account_id": "STRING",
                "timestamp": "STRING",
                "transaction_name": "STRING",
                "duration": "FLOAT",
                "error_rate": "FLOAT",
                "throughput": "FLOAT",
                "apdex_score": "FLOAT",
            },
        },
        {
            "table": "infrastructure_data",
            "primary_key": ["account_id", "timestamp", "host_name"],
            "columns": {
                "account_id": "STRING",
                "timestamp": "STRING",
                "host_name": "STRING",
                "domain": "STRING",
                "type": "STRING",
                "reporting": "BOOLEAN",
                "tags": "STRING",
            },
        },
        {
            "table": "browser_data",
            "primary_key": ["account_id", "timestamp", "page_url"],
            "columns": {
                "account_id": "STRING",
                "timestamp": "STRING",
                "page_url": "STRING",
                "browser_name": "STRING",
                "browser_version": "STRING",
                "device_type": "STRING",
                "load_time": "FLOAT",
                "dom_content_loaded": "FLOAT",
            },
        },
        {
            "table": "mobile_data",
            "primary_key": ["account_id", "timestamp", "app_name"],
            "columns": {
                "account_id": "STRING",
                "timestamp": "STRING",
                "app_name": "STRING",
                "platform": "STRING",
                "version": "STRING",
                "device_model": "STRING",
                "os_version": "STRING",
                "crash_count": "INT",
            },
        },
        {
            "table": "synthetic_data",
            "primary_key": ["account_id", "timestamp", "monitor_name"],
            "columns": {
                "account_id": "STRING",
                "timestamp": "STRING",
                "monitor_name": "STRING",
                "monitor_type": "STRING",
                "status": "STRING",
                "response_time": "FLOAT",
                "location": "STRING",
                "error_message": "STRING",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is called by Fivetran during each sync.
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
    """
    log.info("Starting New Relic Feature APIs connector sync")

    # Validate the configuration
    validate_configuration(configuration=configuration)

    # Extract configuration parameters
    api_key = configuration.get("api_key", "")
    account_id = configuration.get("account_id", "")
    region = configuration.get("region", "US")

    # Get the state variable for the sync
    last_sync_time = state.get("last_sync_time")

    # Log sync type
    if last_sync_time:
        log.info(f"Incremental sync: fetching data since {last_sync_time}")
    else:
        log.info("Initial sync: fetching all available data (last 90 days)")

    try:
        # Fetch APM data
        log.info("Fetching APM data...")
        apm_data = get_apm_data(api_key, region, account_id, last_sync_time)
        for record in apm_data:
            op.upsert(table="apm_data", data=record)

        # Fetch Infrastructure data
        log.info("Fetching Infrastructure data...")
        infra_data = get_infrastructure_data(api_key, region, account_id)
        for record in infra_data:
            op.upsert(table="infrastructure_data", data=record)

        # Fetch Browser monitoring data
        log.info("Fetching Browser monitoring data...")
        browser_data = get_browser_data(api_key, region, account_id, last_sync_time)
        for record in browser_data:
            op.upsert(table="browser_data", data=record)

        # Fetch Mobile monitoring data
        log.info("Fetching Mobile monitoring data...")
        mobile_data = get_mobile_data(api_key, region, account_id, last_sync_time)
        for record in mobile_data:
            op.upsert(table="mobile_data", data=record)

        # Fetch Synthetic monitoring data
        log.info("Fetching Synthetic monitoring data...")
        synthetic_data = get_synthetic_data(api_key, region, account_id, last_sync_time)
        for record in synthetic_data:
            op.upsert(table="synthetic_data", data=record)

        # Update state with the current sync time
        new_state = {"last_sync_time": datetime.now().isoformat()}

        # Checkpoint the state
        op.checkpoint(new_state)

        log.info("New Relic Feature APIs connector sync completed successfully")

    except Exception as e:
        log.severe(f"Failed to sync New Relic data: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
