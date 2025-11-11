# This connector demonstrates how to fetch data from New Relic feature APIs and upsert it into destination using the Fivetran Connector SDK.
# Supports querying data from APM, Infrastructure, Browser monitoring, Mobile monitoring, and Synthetic monitoring APIs.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import required libraries for API interactions
import requests
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional


def _validate_required_configs(configuration: dict):
    """Validate required configuration parameters."""
    required_configs = ["api_key", "account_id", "region"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def _validate_api_key(api_key: str):
    """Validate API key format."""
    if not api_key.startswith("NRAK-"):
        raise ValueError("API key should start with 'NRAK-'")


def _validate_region(region: str):
    """Validate region parameter."""
    valid_regions = ["US", "EU"]
    if region not in valid_regions:
        raise ValueError(f"Region must be one of: {valid_regions}")


def _validate_integer_range(
    configuration: dict, key: str, default: str, min_val: int, max_val: int, name: str
):
    """Validate integer configuration parameter within range."""
    try:
        int_val = int(configuration.get(key, default))
        if not (min_val <= int_val <= max_val):
            raise ValueError(f"{name} must be an integer between {min_val} and {max_val}")
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be a valid integer between {min_val} and {max_val}")


def _validate_float_range(
    configuration: dict,
    key: str,
    default: str,
    min_val: float,
    max_val: float,
    name: str,
):
    """Validate float configuration parameter within range."""
    try:
        float_val = float(configuration.get(key, default))
        if not (min_val <= float_val <= max_val):
            raise ValueError(f"{name} must be a number between {min_val} and {max_val}")
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be a valid number between {min_val} and {max_val}")


def _validate_log_level(log_level: str):
    """Validate log level parameter."""
    valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "SEVERE"]
    if log_level not in valid_log_levels:
        raise ValueError(f"log_level must be one of: {valid_log_levels}")


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    _validate_required_configs(configuration)
    _validate_api_key(configuration.get("api_key", ""))
    _validate_region(configuration.get("region"))

    # Validate numeric parameters
    _validate_integer_range(
        configuration, "sync_frequency_minutes", "15", 1, 1440, "sync_frequency_minutes"
    )
    _validate_integer_range(configuration, "initial_sync_days", "90", 1, 365, "initial_sync_days")
    _validate_integer_range(
        configuration,
        "max_records_per_query",
        "1000",
        1,
        10000,
        "max_records_per_query",
    )
    _validate_integer_range(configuration, "timeout_seconds", "30", 5, 300, "timeout_seconds")
    _validate_integer_range(configuration, "retry_attempts", "3", 0, 10, "retry_attempts")
    _validate_float_range(
        configuration, "data_quality_threshold", "0.95", 0, 1, "data_quality_threshold"
    )
    _validate_log_level(configuration.get("log_level", "INFO"))


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

    for attempt in range(retry_attempts + 1):
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=timeout_seconds)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < retry_attempts:
                log.warning(
                    f"API request failed (attempt {attempt + 1}/{retry_attempts + 1}): {str(e)}"
                )
                time.sleep(retry_delay_seconds * (2**attempt))  # Exponential backoff
            else:
                log.severe(
                    f"Failed to execute NerdGraph query after {retry_attempts + 1} attempts: {str(e)}"
                )
                raise RuntimeError(
                    f"API request failed after {retry_attempts + 1} attempts: {str(e)}"
                )


def get_time_range(last_sync_time: Optional[str] = None, initial_sync_days: int = 90) -> str:
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


def _extract_configuration_params(configuration: dict) -> dict:
    """
    Extract and convert configuration parameters.
    Args:
        configuration: Raw configuration dictionary
    Returns:
        Processed configuration parameters
    """
    return {
        "api_key": configuration.get("api_key", ""),
        "account_id": configuration.get("account_id", ""),
        "region": configuration.get("region", "US"),
        "initial_sync_days": int(configuration.get("initial_sync_days", "90")),
        "max_records": int(configuration.get("max_records_per_query", "1000")),
        "timeout_seconds": int(configuration.get("timeout_seconds", "30")),
        "retry_attempts": int(configuration.get("retry_attempts", "3")),
        "retry_delay_seconds": int(configuration.get("retry_delay_seconds", "5")),
        "data_quality_threshold": float(configuration.get("data_quality_threshold", "0.95")),
    }


def _get_data_source_flags(configuration: dict) -> dict:
    """
    Extract data source enablement flags.
    Args:
        configuration: Raw configuration dictionary
    Returns:
        Data source flags
    """
    return {
        "enable_apm": configuration.get("enable_apm_data", "true").lower() == "true",
        "enable_infrastructure": configuration.get("enable_infrastructure_data", "true").lower() == "true",
        "enable_browser": configuration.get("enable_browser_data", "true").lower() == "true",
        "enable_mobile": configuration.get("enable_mobile_data", "true").lower() == "true",
        "enable_synthetic": configuration.get("enable_synthetic_data", "true").lower() == "true",
    }


def _calculate_simple_data_quality(record_count: int) -> float:
    """
    Calculate a simple data quality score based on record count.
    For streaming data, we use a simplified quality metric.
    Args:
        record_count: Number of records processed
    Returns:
        Quality score between 0 and 1
    """
    # Simple quality metric: if we got records, quality is good
    return 1.0 if record_count > 0 else 0.0


def _handle_data_quality_check(overall_quality: float, threshold: float, alert_on_errors: bool):
    """
    Handle data quality threshold checking and alerting.
    Args:
        overall_quality: Calculated overall quality score
        threshold: Quality threshold to check against
        alert_on_errors: Whether to send severe alerts
    """
    if overall_quality < threshold:
        log.warning(f"Data quality score {overall_quality:.3f} below threshold {threshold}")
        if alert_on_errors:
            log.severe(
                f"Data quality alert: score {overall_quality:.3f} below threshold {threshold}"
            )


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
):
    """
    Fetch APM data from New Relic with pagination and insert records directly.
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
        Number of records processed
    """
    time_range = get_time_range(last_sync_time, initial_sync_days)
    total_record_count = 0
    offset = 0

    # Paginate through all results
    while True:
        query = f"""
        {{
          actor {{
            account(id: {account_id}) {{
              nrql(query: "SELECT * FROM Transaction {time_range} LIMIT {max_records} OFFSET {offset}") {{
                results
              }}
            }}
          }}
        }}
        """

        response = execute_nerdgraph_query(
            query, api_key, region, timeout_seconds, retry_attempts, retry_delay_seconds
        )

        # Check if we got results
        results = (
            response.get("data", {})
            .get("actor", {})
            .get("account", {})
            .get("nrql", {})
            .get("results", [])
        )

        if not results:
            # No more results, break pagination loop
            break

        current_time = datetime.now(timezone.utc).isoformat()
        page_count = 0

        for result in results:
            record = {
                "account_id": account_id,
                "timestamp": get_timestamp_from_data(result, current_time),
                "transaction_name": result.get("transactionName", ""),
                "duration": result.get("duration", 0),
                "error_rate": result.get("errorRate", 0),
                "throughput": result.get("throughput", 0),
                "apdex_score": result.get("apdexScore", 0),
            }
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="apm_data", data=record)
            page_count += 1
            total_record_count += 1

        log.info(f"APM data page: {page_count} records (offset: {offset})")

        # If we got fewer results than max_records, we've reached the end
        if len(results) < max_records:
            break

        # Move to next page
        offset += max_records

    return total_record_count


def get_infrastructure_data(
    api_key: str,
    region: str,
    account_id: str,
    timeout_seconds: int = 30,
    retry_attempts: int = 3,
    retry_delay_seconds: int = 5,
):
    """
    Fetch Infrastructure monitoring data from New Relic with cursor-based pagination and insert records directly.
    Args:
        api_key: New Relic API key
        region: New Relic region
        account_id: New Relic account ID
        timeout_seconds: Request timeout in seconds
        retry_attempts: Number of retry attempts on failure
        retry_delay_seconds: Delay between retry attempts
    Returns:
        Number of records processed
    """
    total_record_count = 0
    next_cursor = None

    # Paginate through all results using cursor-based pagination
    while True:
        cursor_param = f'cursor: "{next_cursor}"' if next_cursor else ""
        query = f"""
        {{
          actor {{
            entitySearch(query: "domain = 'INFRA' AND type = 'HOST' AND accountId = {account_id}" {cursor_param}) {{
              count
              results {{
                nextCursor
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

        # Check if we got results
        entity_search = response.get("data", {}).get("actor", {}).get("entitySearch", {})
        if not entity_search:
            break

        results = entity_search.get("results", {})
        entities = results.get("entities", [])

        if not entities:
            # No more entities, break pagination loop
            break

        current_time = datetime.now(timezone.utc).isoformat()
        page_count = 0

        for entity in entities:
            record = {
                "account_id": account_id,
                "timestamp": get_timestamp_from_data(entity, current_time),
                "host_name": entity.get("name", ""),
                "domain": entity.get("domain", ""),
                "type": entity.get("type", ""),
                "reporting": entity.get("reporting", False),
                "tags": json.dumps(entity.get("tags", [])),
            }
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="infrastructure_data", data=record)
            page_count += 1
            total_record_count += 1

        log.info(f"Infrastructure data page: {page_count} entities")

        # Check if there's a next cursor for pagination
        next_cursor = results.get("nextCursor")
        if not next_cursor:
            # No more pages, break pagination loop
            break

    return total_record_count


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
):
    """
    Fetch Browser monitoring data from New Relic with pagination and insert records directly.
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
        Number of records processed
    """
    time_range = get_time_range(last_sync_time, initial_sync_days)
    total_record_count = 0
    offset = 0

    # Paginate through all results
    while True:
        query = f"""
        {{
          actor {{
            account(id: {account_id}) {{
              nrql(query: "SELECT * FROM PageView {time_range} LIMIT {max_records} OFFSET {offset}") {{
                results
              }}
            }}
          }}
        }}
        """

        response = execute_nerdgraph_query(
            query, api_key, region, timeout_seconds, retry_attempts, retry_delay_seconds
        )

        # Check if we got results
        results = (
            response.get("data", {})
            .get("actor", {})
            .get("account", {})
            .get("nrql", {})
            .get("results", [])
        )

        if not results:
            # No more results, break pagination loop
            break

        current_time = datetime.now(timezone.utc).isoformat()
        page_count = 0

        for result in results:
            record = {
                "account_id": account_id,
                "timestamp": get_timestamp_from_data(result, current_time),
                "page_url": result.get("pageUrl", ""),
                "browser_name": result.get("browserName", ""),
                "browser_version": result.get("browserVersion", ""),
                "device_type": result.get("deviceType", ""),
                "load_time": result.get("loadTime", 0),
                "dom_content_loaded": result.get("domContentLoaded", 0),
            }
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="browser_data", data=record)
            page_count += 1
            total_record_count += 1

        log.info(f"Browser data page: {page_count} records (offset: {offset})")

        # If we got fewer results than max_records, we've reached the end
        if len(results) < max_records:
            break

        # Move to next page
        offset += max_records

    return total_record_count


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
):
    """
    Fetch Mobile monitoring data from New Relic with pagination and insert records directly.
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
        Number of records processed
    """
    time_range = get_time_range(last_sync_time, initial_sync_days)
    total_record_count = 0
    offset = 0

    # Paginate through all results
    while True:
        query = f"""
        {{
          actor {{
            account(id: {account_id}) {{
              nrql(query: "SELECT * FROM Mobile WHERE appName IS NOT NULL {time_range} LIMIT {max_records} OFFSET {offset}") {{
                results
              }}
            }}
          }}
        }}
        """

        response = execute_nerdgraph_query(
            query, api_key, region, timeout_seconds, retry_attempts, retry_delay_seconds
        )

        # Check if we got results
        results = (
            response.get("data", {})
            .get("actor", {})
            .get("account", {})
            .get("nrql", {})
            .get("results", [])
        )

        if not results:
            # No more results, break pagination loop
            break

        current_time = datetime.now(timezone.utc).isoformat()
        page_count = 0

        for result in results:
            record = {
                "account_id": account_id,
                "timestamp": get_timestamp_from_data(result, current_time),
                "app_name": result.get("appName", ""),
                "platform": result.get("platform", ""),
                "version": result.get("version", ""),
                "device_model": result.get("deviceModel", ""),
                "os_version": result.get("osVersion", ""),
                "crash_count": result.get("crashCount", 0),
            }
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="mobile_data", data=record)
            page_count += 1
            total_record_count += 1

        log.info(f"Mobile data page: {page_count} records (offset: {offset})")

        # If we got fewer results than max_records, we've reached the end
        if len(results) < max_records:
            break

        # Move to next page
        offset += max_records

    return total_record_count


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
):
    """
    Fetch Synthetic monitoring data from New Relic with pagination and insert records directly.
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
        Number of records processed
    """
    time_range = get_time_range(last_sync_time, initial_sync_days)
    total_record_count = 0
    offset = 0

    # Paginate through all results
    while True:
        query = f"""
        {{
          actor {{
            account(id: {account_id}) {{
              nrql(query: "SELECT * FROM SyntheticCheck {time_range} LIMIT {max_records} OFFSET {offset}") {{
                results
              }}
            }}
          }}
        }}
        """

        response = execute_nerdgraph_query(
            query, api_key, region, timeout_seconds, retry_attempts, retry_delay_seconds
        )

        # Check if we got results
        results = (
            response.get("data", {})
            .get("actor", {})
            .get("account", {})
            .get("nrql", {})
            .get("results", [])
        )

        if not results:
            # No more results, break pagination loop
            break

        current_time = datetime.now(timezone.utc).isoformat()
        page_count = 0

        for result in results:
            record = {
                "account_id": account_id,
                "timestamp": get_timestamp_from_data(result, current_time),
                "monitor_name": result.get("monitorName", ""),
                "monitor_type": result.get("monitorType", ""),
                "status": result.get("status", ""),
                "response_time": result.get("responseTime", 0),
                "location": result.get("location", ""),
                "error_message": result.get("errorMessage", ""),
            }
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="synthetic_data", data=record)
            page_count += 1
            total_record_count += 1

        log.info(f"Synthetic data page: {page_count} records (offset: {offset})")

        # If we got fewer results than max_records, we've reached the end
        if len(results) < max_records:
            break

        # Move to next page
        offset += max_records

    return total_record_count


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
            "table": "apm_data",
            "primary_key": ["account_id", "timestamp", "transaction_name"],
        },
        {
            "table": "infrastructure_data",
            "primary_key": ["account_id", "timestamp", "host_name"],
        },
        {
            "table": "browser_data",
            "primary_key": ["account_id", "timestamp", "page_url"],
        },
        {
            "table": "mobile_data",
            "primary_key": ["account_id", "timestamp", "app_name"],
        },
        {
            "table": "synthetic_data",
            "primary_key": ["account_id", "timestamp", "monitor_name"],
        },
    ]


def _process_data_sources(config_params: dict, data_source_flags: dict, last_sync_time: Optional[str]) -> tuple:
    """
    Process all enabled data sources and return metrics.
    Args:
        config_params: Configuration parameters
        data_source_flags: Data source enablement flags
        last_sync_time: Last sync timestamp for incremental sync
    Returns:
        Tuple of (total_records, data_quality_scores)
    """
    total_records = 0
    data_quality_scores = []

    # Define data sources with their fetch functions
    data_sources = [
        ("apm", data_source_flags["enable_apm"], get_apm_data, True),
        ("infrastructure", data_source_flags["enable_infrastructure"], get_infrastructure_data, False),
        ("browser", data_source_flags["enable_browser"], get_browser_data, True),
        ("mobile", data_source_flags["enable_mobile"], get_mobile_data, True),
        ("synthetic", data_source_flags["enable_synthetic"], get_synthetic_data, True),
    ]

    # Process each enabled data source
    for source_name, enabled, fetch_func, uses_time_range in data_sources:
        if enabled:
            log.info(f"Fetching {source_name.title()} data...")

            # Prepare arguments based on function signature
            if uses_time_range:
                data = fetch_func(
                    config_params["api_key"],
                    config_params["region"],
                    config_params["account_id"],
                    last_sync_time,
                    config_params["initial_sync_days"],
                    config_params["max_records"],
                    config_params["timeout_seconds"],
                    config_params["retry_attempts"],
                    config_params["retry_delay_seconds"],
                )
            else:
                data = fetch_func(
                    config_params["api_key"],
                    config_params["region"],
                    config_params["account_id"],
                    config_params["timeout_seconds"],
                    config_params["retry_attempts"],
                    config_params["retry_delay_seconds"],
                )

            # Get record count from fetch function (data is already synced)
            record_count = data
            quality_score = _calculate_simple_data_quality(record_count)
            total_records += record_count
            data_quality_scores.append(quality_score)
            log.info(f"{source_name.title()} data: {record_count} records")

    return total_records, data_quality_scores


def update(configuration: dict, state: dict):
    """
    Define the update function, which is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.info("Starting New Relic Feature APIs connector sync")

    # Validate the configuration
    validate_configuration(configuration=configuration)

    # Extract configuration parameters
    config_params = _extract_configuration_params(configuration)
    data_source_flags = _get_data_source_flags(configuration)
    last_sync_time = state.get("last_sync_time")

    # Log sync type and configuration
    if last_sync_time:
        log.info(f"Incremental sync: fetching data since {last_sync_time}")
    else:
        log.info(
            f"Initial sync: fetching all available data (last {config_params['initial_sync_days']} days)"
        )

    log.info(
        f"Configuration: max_records={config_params['max_records']}, timeout={config_params['timeout_seconds']}s, retry_attempts={config_params['retry_attempts']}"
    )

    try:
        # Process all data sources
        total_records, data_quality_scores = _process_data_sources(config_params, data_source_flags, last_sync_time)

        # Calculate overall data quality score
        overall_quality = (
            sum(data_quality_scores) / len(data_quality_scores) if data_quality_scores else 1.0
        )

        # Handle data quality checking
        alert_on_errors = configuration.get("alert_on_errors", "true").lower() == "true"
        _handle_data_quality_check(
            overall_quality, config_params["data_quality_threshold"], alert_on_errors
        )

        # Update state with the current sync time
        new_state = {
            "last_sync_time": datetime.now(timezone.utc).isoformat(),
            "total_records_synced": total_records,
            "data_quality_score": overall_quality,
            "sync_timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)

        log.info("New Relic Feature APIs connector sync completed successfully")
        log.info(f"Total records synced: {total_records}, Data quality: {overall_quality:.3f}")

    except Exception as e:
        log.severe(f"Failed to sync New Relic data: {str(e)}")
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
