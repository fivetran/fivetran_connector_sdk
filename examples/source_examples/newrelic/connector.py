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
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
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

    # Validate optional configuration parameters (convert from strings)
    try:
        sync_frequency = int(configuration.get("sync_frequency_minutes", "15"))
        if sync_frequency < 1 or sync_frequency > 1440:
            raise ValueError(
                "sync_frequency_minutes must be an integer between 1 and 1440"
            )
    except (ValueError, TypeError):
        raise ValueError(
            "sync_frequency_minutes must be a valid integer between 1 and 1440"
        )

    try:
        initial_sync_days = int(configuration.get("initial_sync_days", "90"))
        if initial_sync_days < 1 or initial_sync_days > 365:
            raise ValueError("initial_sync_days must be an integer between 1 and 365")
    except (ValueError, TypeError):
        raise ValueError("initial_sync_days must be a valid integer between 1 and 365")

    try:
        max_records = int(configuration.get("max_records_per_query", "1000"))
        if max_records < 1 or max_records > 10000:
            raise ValueError(
                "max_records_per_query must be an integer between 1 and 10000"
            )
    except (ValueError, TypeError):
        raise ValueError(
            "max_records_per_query must be a valid integer between 1 and 10000"
        )

    try:
        timeout = int(configuration.get("timeout_seconds", "30"))
        if timeout < 5 or timeout > 300:
            raise ValueError("timeout_seconds must be an integer between 5 and 300")
    except (ValueError, TypeError):
        raise ValueError("timeout_seconds must be a valid integer between 5 and 300")

    try:
        retry_attempts = int(configuration.get("retry_attempts", "3"))
        if retry_attempts < 0 or retry_attempts > 10:
            raise ValueError("retry_attempts must be an integer between 0 and 10")
    except (ValueError, TypeError):
        raise ValueError("retry_attempts must be a valid integer between 0 and 10")

    try:
        data_quality_threshold = float(
            configuration.get("data_quality_threshold", "0.95")
        )
        if data_quality_threshold < 0 or data_quality_threshold > 1:
            raise ValueError("data_quality_threshold must be a number between 0 and 1")
    except (ValueError, TypeError):
        raise ValueError(
            "data_quality_threshold must be a valid number between 0 and 1"
        )

    log_level = configuration.get("log_level", "INFO")
    valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "SEVERE"]
    if log_level not in valid_log_levels:
        raise ValueError(f"log_level must be one of: {valid_log_levels}")


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


def execute_nerdgraph_query(
    query: str,
    api_key: str,
    region: str,
    timeout_seconds: int = 30,
    retry_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> Dict[str, Any]:
    """
    Execute a NerdGraph query against New Relic API with retry logic.
    Args:
        query: The GraphQL query to execute
        api_key: New Relic API key
        region: New Relic region
        timeout_seconds: Request timeout in seconds
        retry_attempts: Number of retry attempts on failure
        retry_delay_seconds: Delay between retry attempts
    Returns:
        The response data from the API
    """
    endpoint = get_newrelic_endpoint(region)
    url = f"{endpoint}/graphql"

    headers = {"API-Key": api_key, "Content-Type": "application/json"}
    payload = {"query": query}

    last_exception = None

    for attempt in range(retry_attempts + 1):
        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=timeout_seconds
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            last_exception = e
            if attempt < retry_attempts:
                log.warning(
                    f"API request failed (attempt {attempt + 1}/{retry_attempts + 1}): {str(e)}"
                )
                import time

                time.sleep(retry_delay_seconds * (2**attempt))  # Exponential backoff
            else:
                log.severe(
                    f"Failed to execute NerdGraph query after {retry_attempts + 1} attempts: {str(e)}"
                )
                raise RuntimeError(
                    f"API request failed after {retry_attempts + 1} attempts: {str(e)}"
                )

    # This should never be reached, but just in case
    raise RuntimeError(f"API request failed: {str(last_exception)}")


def get_time_range(
    last_sync_time: Optional[str] = None, initial_sync_days: int = 90
) -> str:
    """
    Generate dynamic time range for NRQL queries.
    Args:
        last_sync_time: Last sync timestamp for incremental sync
        initial_sync_days: Number of days to fetch for initial sync
    Returns:
        Time range string for NRQL query
    """
    if last_sync_time:
        # Incremental sync: get data since last sync
        return f'SINCE "{last_sync_time}"'
    else:
        # Initial sync: get all available data for specified days
        return f"SINCE {initial_sync_days} days ago"


def get_timestamp_from_data(result: Dict[str, Any], fallback_timestamp: str) -> str:
    """
    Extract timestamp from New Relic data result, with fallback to provided timestamp.
    Args:
        result: The data result from New Relic API
        fallback_timestamp: Fallback timestamp if no timestamp found in data
    Returns:
        ISO format timestamp string
    """
    # Try to get timestamp from various possible fields
    timestamp_fields = [
        "timestamp",
        "eventTime",
        "occurredAt",
        "createdAt",
        "lastUpdated",
        "startTime",
        "endTime",
        "time",
    ]

    for field in timestamp_fields:
        if field in result and result[field]:
            # If it's already a string, return as is
            if isinstance(result[field], str):
                return result[field]
            # If it's a number (Unix timestamp), convert to ISO format
            elif isinstance(result[field], (int, float)):
                try:
                    dt = datetime.fromtimestamp(result[field], tz=timezone.utc)
                    return dt.isoformat()
                except (ValueError, OSError):
                    continue

    # If no timestamp found in data, use fallback
    return fallback_timestamp


def get_apm_data(
    api_key: str,
    region: str,
    account_id: str,
    last_sync_time: Optional[str] = None,
    initial_sync_days: int = 90,
    max_records: int = 1000,
    timeout_seconds: int = 30,
    retry_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> List[Dict[str, Any]]:
    """
    Fetch APM data from New Relic.
    Args:
        api_key: New Relic API key
        region: New Relic region
        account_id: New Relic account ID
        last_sync_time: Last sync timestamp for incremental sync
        initial_sync_days: Number of days to fetch for initial sync
        max_records: Maximum number of records to fetch per query
        timeout_seconds: Request timeout in seconds
        retry_attempts: Number of retry attempts on failure
        retry_delay_seconds: Delay between retry attempts
    Returns:
        List of APM data
    """
    time_range = get_time_range(last_sync_time, initial_sync_days)
    query = f"""
    {{
      actor {{
        account(id: {account_id}) {{
          nrql(query: "SELECT * FROM Transaction {time_range} LIMIT {max_records}") {{
            results
          }}
        }}
      }}
    }}
    """

    response = execute_nerdgraph_query(
        query, api_key, region, timeout_seconds, retry_attempts, retry_delay_seconds
    )

    apm_data = []
    if (
        response.get("data", {})
        .get("actor", {})
        .get("account", {})
        .get("nrql", {})
        .get("results")
    ):
        results = response["data"]["actor"]["account"]["nrql"]["results"]
        current_time = datetime.now(timezone.utc).isoformat()
        for result in results:
            apm_data.append(
                {
                    "account_id": account_id,
                    "timestamp": get_timestamp_from_data(result, current_time),
                    "transaction_name": result.get("transactionName", ""),
                    "duration": result.get("duration", 0),
                    "error_rate": result.get("errorRate", 0),
                    "throughput": result.get("throughput", 0),
                    "apdex_score": result.get("apdexScore", 0),
                }
            )

    return apm_data


def get_infrastructure_data(
    api_key: str,
    region: str,
    account_id: str,
    timeout_seconds: int = 30,
    retry_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> List[Dict[str, Any]]:
    """
    Fetch Infrastructure monitoring data from New Relic.
    Args:
        api_key: New Relic API key
        region: New Relic region
        account_id: New Relic account ID
        timeout_seconds: Request timeout in seconds
        retry_attempts: Number of retry attempts on failure
        retry_delay_seconds: Delay between retry attempts
    Returns:
        List of infrastructure host data
    """
    query = f"""
    {{
      actor {{
        entitySearch(query: "domain = 'INFRA' AND type = 'HOST' AND accountId = {account_id}") {{
          count
          results {{
            entities {{
              name
              accountId
              domain
              type
              reporting
              tags {{
                key
                values
              }}
            }}
          }}
        }}
      }}
    }}
    """

    response = execute_nerdgraph_query(
        query, api_key, region, timeout_seconds, retry_attempts, retry_delay_seconds
    )

    infra_data = []
    if (
        response.get("data", {})
        .get("actor", {})
        .get("entitySearch", {})
        .get("results", {})
        .get("entities")
    ):
        entities = response["data"]["actor"]["entitySearch"]["results"]["entities"]
        current_time = datetime.now(timezone.utc).isoformat()
        for entity in entities:
            infra_data.append(
                {
                    "account_id": account_id,
                    "timestamp": get_timestamp_from_data(entity, current_time),
                    "host_name": entity.get("name", ""),
                    "domain": entity.get("domain", ""),
                    "type": entity.get("type", ""),
                    "reporting": entity.get("reporting", False),
                    "tags": json.dumps(entity.get("tags", [])),
                }
            )

    return infra_data


def get_browser_data(
    api_key: str,
    region: str,
    account_id: str,
    last_sync_time: Optional[str] = None,
    initial_sync_days: int = 90,
    max_records: int = 1000,
    timeout_seconds: int = 30,
    retry_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> List[Dict[str, Any]]:
    """
    Fetch Browser monitoring data from New Relic.
    Args:
        api_key: New Relic API key
        region: New Relic region
        account_id: New Relic account ID
        last_sync_time: Last sync timestamp for incremental sync
        initial_sync_days: Number of days to fetch for initial sync
        max_records: Maximum number of records to fetch per query
        timeout_seconds: Request timeout in seconds
        retry_attempts: Number of retry attempts on failure
        retry_delay_seconds: Delay between retry attempts
    Returns:
        List of browser monitoring data
    """
    time_range = get_time_range(last_sync_time, initial_sync_days)
    query = f"""
    {{
      actor {{
        account(id: {account_id}) {{
          nrql(query: "SELECT * FROM PageView {time_range} LIMIT {max_records}") {{
            results
          }}
        }}
      }}
    }}
    """

    response = execute_nerdgraph_query(
        query, api_key, region, timeout_seconds, retry_attempts, retry_delay_seconds
    )

    browser_data = []
    if (
        response.get("data", {})
        .get("actor", {})
        .get("account", {})
        .get("nrql", {})
        .get("results")
    ):
        results = response["data"]["actor"]["account"]["nrql"]["results"]
        current_time = datetime.now(timezone.utc).isoformat()
        for result in results:
            browser_data.append(
                {
                    "account_id": account_id,
                    "timestamp": get_timestamp_from_data(result, current_time),
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
    api_key: str,
    region: str,
    account_id: str,
    last_sync_time: Optional[str] = None,
    initial_sync_days: int = 90,
    max_records: int = 1000,
    timeout_seconds: int = 30,
    retry_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> List[Dict[str, Any]]:
    """
    Fetch Mobile monitoring data from New Relic.
    Args:
        api_key: New Relic API key
        region: New Relic region
        account_id: New Relic account ID
        last_sync_time: Last sync timestamp for incremental sync
        initial_sync_days: Number of days to fetch for initial sync
        max_records: Maximum number of records to fetch per query
        timeout_seconds: Request timeout in seconds
        retry_attempts: Number of retry attempts on failure
        retry_delay_seconds: Delay between retry attempts
    Returns:
        List of mobile monitoring data
    """
    time_range = get_time_range(last_sync_time, initial_sync_days)
    query = f"""
    {{
      actor {{
        account(id: {account_id}) {{
          nrql(query: "SELECT * FROM Mobile WHERE appName IS NOT NULL {time_range} LIMIT {max_records}") {{
            results
          }}
        }}
      }}
    }}
    """

    response = execute_nerdgraph_query(
        query, api_key, region, timeout_seconds, retry_attempts, retry_delay_seconds
    )

    mobile_data = []
    if (
        response.get("data", {})
        .get("actor", {})
        .get("account", {})
        .get("nrql", {})
        .get("results")
    ):
        results = response["data"]["actor"]["account"]["nrql"]["results"]
        current_time = datetime.now(timezone.utc).isoformat()
        for result in results:
            mobile_data.append(
                {
                    "account_id": account_id,
                    "timestamp": get_timestamp_from_data(result, current_time),
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
    api_key: str,
    region: str,
    account_id: str,
    last_sync_time: Optional[str] = None,
    initial_sync_days: int = 90,
    max_records: int = 1000,
    timeout_seconds: int = 30,
    retry_attempts: int = 3,
    retry_delay_seconds: int = 5,
) -> List[Dict[str, Any]]:
    """
    Fetch Synthetic monitoring data from New Relic.
    Args:
        api_key: New Relic API key
        region: New Relic region
        account_id: New Relic account ID
        last_sync_time: Last sync timestamp for incremental sync
        initial_sync_days: Number of days to fetch for initial sync
        max_records: Maximum number of records to fetch per query
        timeout_seconds: Request timeout in seconds
        retry_attempts: Number of retry attempts on failure
        retry_delay_seconds: Delay between retry attempts
    Returns:
        List of synthetic monitoring data
    """
    time_range = get_time_range(last_sync_time, initial_sync_days)
    query = f"""
    {{
      actor {{
        account(id: {account_id}) {{
          nrql(query: "SELECT * FROM SyntheticCheck {time_range} LIMIT {max_records}") {{
            results
          }}
        }}
      }}
    }}
    """

    response = execute_nerdgraph_query(
        query, api_key, region, timeout_seconds, retry_attempts, retry_delay_seconds
    )

    synthetic_data = []
    if (
        response.get("data", {})
        .get("actor", {})
        .get("account", {})
        .get("nrql", {})
        .get("results")
    ):
        results = response["data"]["actor"]["account"]["nrql"]["results"]
        current_time = datetime.now(timezone.utc).isoformat()
        for result in results:
            synthetic_data.append(
                {
                    "account_id": account_id,
                    "timestamp": get_timestamp_from_data(result, current_time),
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

    # Extract optional configuration parameters with defaults and convert from strings
    initial_sync_days = int(configuration.get("initial_sync_days", "90"))
    max_records = int(configuration.get("max_records_per_query", "1000"))
    timeout_seconds = int(configuration.get("timeout_seconds", "30"))
    retry_attempts = int(configuration.get("retry_attempts", "3"))
    retry_delay_seconds = int(configuration.get("retry_delay_seconds", "5"))
    data_quality_threshold = float(configuration.get("data_quality_threshold", "0.95"))

    # Data source enablement flags (convert string "true"/"false" to boolean)
    enable_apm = configuration.get("enable_apm_data", "true").lower() == "true"
    enable_infrastructure = (
        configuration.get("enable_infrastructure_data", "true").lower() == "true"
    )
    enable_browser = configuration.get("enable_browser_data", "true").lower() == "true"
    enable_mobile = configuration.get("enable_mobile_data", "true").lower() == "true"
    enable_synthetic = (
        configuration.get("enable_synthetic_data", "true").lower() == "true"
    )

    # Get the state variable for the sync
    last_sync_time = state.get("last_sync_time")

    # Log sync type and configuration
    if last_sync_time:
        log.info(f"Incremental sync: fetching data since {last_sync_time}")
    else:
        log.info(
            f"Initial sync: fetching all available data (last {initial_sync_days} days)"
        )

    log.info(
        f"Configuration: max_records={max_records}, timeout={timeout_seconds}s, retry_attempts={retry_attempts}"
    )

    try:
        total_records = 0
        data_quality_scores = []

        # Fetch APM data
        if enable_apm:
            log.info("Fetching APM data...")
            apm_data = get_apm_data(
                api_key,
                region,
                account_id,
                last_sync_time,
                initial_sync_days,
                max_records,
                timeout_seconds,
                retry_attempts,
                retry_delay_seconds,
            )
            for record in apm_data:
                op.upsert(table="apm_data", data=record)
            total_records += len(apm_data)
            data_quality_scores.append(
                len(apm_data) / max(len(apm_data), 1)
            )  # Simple quality metric
            log.info(f"APM data: {len(apm_data)} records")

        # Fetch Infrastructure data
        if enable_infrastructure:
            log.info("Fetching Infrastructure data...")
            infra_data = get_infrastructure_data(
                api_key,
                region,
                account_id,
                timeout_seconds,
                retry_attempts,
                retry_delay_seconds,
            )
            for record in infra_data:
                op.upsert(table="infrastructure_data", data=record)
            total_records += len(infra_data)
            data_quality_scores.append(len(infra_data) / max(len(infra_data), 1))
            log.info(f"Infrastructure data: {len(infra_data)} records")

        # Fetch Browser monitoring data
        if enable_browser:
            log.info("Fetching Browser monitoring data...")
            browser_data = get_browser_data(
                api_key,
                region,
                account_id,
                last_sync_time,
                initial_sync_days,
                max_records,
                timeout_seconds,
                retry_attempts,
                retry_delay_seconds,
            )
            for record in browser_data:
                op.upsert(table="browser_data", data=record)
            total_records += len(browser_data)
            data_quality_scores.append(len(browser_data) / max(len(browser_data), 1))
            log.info(f"Browser data: {len(browser_data)} records")

        # Fetch Mobile monitoring data
        if enable_mobile:
            log.info("Fetching Mobile monitoring data...")
            mobile_data = get_mobile_data(
                api_key,
                region,
                account_id,
                last_sync_time,
                initial_sync_days,
                max_records,
                timeout_seconds,
                retry_attempts,
                retry_delay_seconds,
            )
            for record in mobile_data:
                op.upsert(table="mobile_data", data=record)
            total_records += len(mobile_data)
            data_quality_scores.append(len(mobile_data) / max(len(mobile_data), 1))
            log.info(f"Mobile data: {len(mobile_data)} records")

        # Fetch Synthetic monitoring data
        if enable_synthetic:
            log.info("Fetching Synthetic monitoring data...")
            synthetic_data = get_synthetic_data(
                api_key,
                region,
                account_id,
                last_sync_time,
                initial_sync_days,
                max_records,
                timeout_seconds,
                retry_attempts,
                retry_delay_seconds,
            )
            for record in synthetic_data:
                op.upsert(table="synthetic_data", data=record)
            total_records += len(synthetic_data)
            data_quality_scores.append(
                len(synthetic_data) / max(len(synthetic_data), 1)
            )
            log.info(f"Synthetic data: {len(synthetic_data)} records")

        # Calculate overall data quality score
        overall_quality = (
            sum(data_quality_scores) / len(data_quality_scores)
            if data_quality_scores
            else 1.0
        )

        # Check data quality threshold
        if overall_quality < data_quality_threshold:
            log.warning(
                f"Data quality score {overall_quality:.3f} below threshold {data_quality_threshold}"
            )
            if configuration.get("alert_on_errors", True):
                log.severe(
                    f"Data quality alert: score {overall_quality:.3f} below threshold {data_quality_threshold}"
                )

        # Update state with the current sync time
        new_state = {
            "last_sync_time": datetime.now(timezone.utc).isoformat(),
            "total_records_synced": total_records,
            "data_quality_score": overall_quality,
            "sync_timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Checkpoint the state
        op.checkpoint(new_state)

        log.info(f"New Relic Feature APIs connector sync completed successfully")
        log.info(
            f"Total records synced: {total_records}, Data quality: {overall_quality:.3f}"
        )

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
