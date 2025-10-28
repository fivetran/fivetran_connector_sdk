"""
Prefect Cloud Connector for Fivetran Connector SDK.
This connector fetches workflow orchestration data from Prefect Cloud API and syncs it to your data warehouse.
Supports incremental syncing of flows, deployments, flow_runs, task_runs, artifacts, and variables.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For making HTTP requests to Prefect API
import requests

# For handling datetime operations
from datetime import datetime

# For handling delays in retry logic
import time

# Define constants for the connector
__BASE_URL_TEMPLATE = (
    "https://api.prefect.cloud/api/accounts/{account_id}/workspaces/{workspace_id}"
)
__MAX_RETRIES = 3
__BASE_DELAY_SECONDS = 1
__BATCH_SIZE = 100
__CHECKPOINT_INTERVAL = 500


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["api_key", "account_id", "workspace_id"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "flows", "primary_key": ["id"]},
        {"table": "deployments", "primary_key": ["id"]},
        {"table": "flow_runs", "primary_key": ["id"]},
        {"table": "task_runs", "primary_key": ["id"]},
        {"table": "artifacts", "primary_key": ["id"]},
        {"table": "variables", "primary_key": ["id"]},
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
    log.warning("Example: Prefect Cloud API Connector")

    validate_configuration(configuration=configuration)

    api_key = configuration.get("api_key")
    account_id = configuration.get("account_id")
    workspace_id = configuration.get("workspace_id")

    base_url = __BASE_URL_TEMPLATE.format(account_id=account_id, workspace_id=workspace_id)
    headers = build_headers(api_key)

    try:
        sync_flows(base_url, headers, state)
        sync_deployments(base_url, headers, state)
        sync_flow_runs(base_url, headers, state)
        sync_task_runs(base_url, headers, state)
        sync_artifacts(base_url, headers, state)
        sync_variables(base_url, headers, state)

    except Exception as e:
        raise RuntimeError(f"Failed to sync Prefect data: {str(e)}")


def build_headers(api_key: str) -> dict:
    """
    Build HTTP headers for Prefect API requests.
    Args:
        api_key: The API key for authentication.
    Returns:
        A dictionary containing HTTP headers.
    """
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


def make_api_request(url: str, headers: dict, payload: dict) -> dict:
    """
    Make an API request with retry logic for transient failures.
    Implements exponential backoff for retries.
    Args:
        url: The API endpoint URL.
        headers: HTTP headers for the request.
        payload: Request body payload.
    Returns:
        Response JSON as a dictionary.
    Raises:
        RuntimeError: If the API request fails after all retry attempts.
    """
    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)

            if response.status_code == 200:
                return response.json()
            elif response.status_code in [429, 500, 502, 503, 504]:
                if attempt < __MAX_RETRIES - 1:
                    delay = __BASE_DELAY_SECONDS * (2**attempt)
                    log.warning(
                        f"Request failed with status {response.status_code}, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                    )
                    time.sleep(delay)
                    continue
                else:
                    log.severe(
                        f"Failed after {__MAX_RETRIES} attempts. Last status: {response.status_code}"
                    )
                    raise RuntimeError(
                        f"API returned {response.status_code} after {__MAX_RETRIES} attempts: {response.text}"
                    )
            else:
                log.severe(
                    f"API request failed with status {response.status_code}: {response.text}"
                )
                raise RuntimeError(f"API request failed: {response.status_code} - {response.text}")

        except requests.exceptions.Timeout as e:
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(
                    f"Request timeout, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                )
                time.sleep(delay)
                continue
            else:
                log.severe(f"Request timeout after {__MAX_RETRIES} attempts")
                raise RuntimeError(f"Request timeout after {__MAX_RETRIES} attempts: {str(e)}")

        except requests.exceptions.ConnectionError as e:
            if attempt < __MAX_RETRIES - 1:
                delay = __BASE_DELAY_SECONDS * (2**attempt)
                log.warning(
                    f"Connection error, retrying in {delay} seconds (attempt {attempt + 1}/{__MAX_RETRIES})"
                )
                time.sleep(delay)
                continue
            else:
                log.severe(f"Connection error after {__MAX_RETRIES} attempts")
                raise RuntimeError(f"Connection error after {__MAX_RETRIES} attempts: {str(e)}")


def fetch_paginated_data(
    base_url: str, endpoint: str, headers: dict, cursor_field: str, cursor_value: str = None
) -> list:
    """
    Fetch paginated data from Prefect API using filter endpoint with incremental cursor.
    Args:
        base_url: Base URL for the Prefect API.
        endpoint: API endpoint path.
        headers: HTTP headers for authentication.
        cursor_field: Field name to use for incremental filtering.
        cursor_value: Cursor value from last sync for incremental filtering.
    Returns:
        List of records from the API.
    """
    all_records = []
    offset = 0

    while True:
        payload = {"limit": __BATCH_SIZE, "offset": offset, "sort": f"{cursor_field.upper()}_ASC"}

        if cursor_value:
            payload[endpoint.split("/")[1]] = {cursor_field: {"after_": cursor_value}}

        url = f"{base_url}{endpoint}"
        response_data = make_api_request(url, headers, payload)

        if not response_data or len(response_data) == 0:
            break

        all_records.extend(response_data)
        log.info(
            f"Fetched {len(response_data)} records from {endpoint}, total: {len(all_records)}"
        )

        if len(response_data) < __BATCH_SIZE:
            break

        offset += __BATCH_SIZE

    return all_records


def flatten_record(record: dict, prefix: str = "") -> dict:
    """
    Flatten nested dictionary into a single-level dictionary.
    Args:
        record: The dictionary to flatten.
        prefix: Prefix for flattened keys.
    Returns:
        Flattened dictionary.
    """
    flattened = {}

    for key, value in record.items():
        new_key = f"{prefix}{key}" if prefix else key

        if isinstance(value, dict):
            flattened.update(flatten_record(value, f"{new_key}_"))
        elif isinstance(value, list):
            flattened[new_key] = json.dumps(value)
        else:
            flattened[new_key] = value

    return flattened


def sync_flows(base_url: str, headers: dict, state: dict):
    """
    Sync flows from Prefect API with incremental cursor based on updated timestamp.
    Args:
        base_url: Base URL for the Prefect API.
        headers: HTTP headers for authentication.
        state: State dictionary to track sync progress.
    """
    last_updated = state.get("flows_last_updated")
    records = fetch_paginated_data(base_url, "/flows/filter", headers, "updated", last_updated)

    log.info(f"Syncing {len(records)} flows")

    record_count = 0
    max_updated = last_updated

    for record in records:
        flattened_record = flatten_record(record)

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="flows", data=flattened_record)

        record_updated = record.get("updated")
        if record_updated and (max_updated is None or record_updated > max_updated):
            max_updated = record_updated

        record_count += 1

        if record_count % __CHECKPOINT_INTERVAL == 0:
            state["flows_last_updated"] = max_updated
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)
            log.info(f"Checkpointed flows at {record_count} records")

    if max_updated != last_updated:
        state["flows_last_updated"] = max_updated
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

    log.info(f"Completed syncing {record_count} flows")


def sync_deployments(base_url: str, headers: dict, state: dict):
    """
    Sync deployments from Prefect API with incremental cursor based on updated timestamp.
    Args:
        base_url: Base URL for the Prefect API.
        headers: HTTP headers for authentication.
        state: State dictionary to track sync progress.
    """
    last_updated = state.get("deployments_last_updated")
    records = fetch_paginated_data(
        base_url, "/deployments/filter", headers, "updated", last_updated
    )

    log.info(f"Syncing {len(records)} deployments")

    record_count = 0
    max_updated = last_updated

    for record in records:
        flattened_record = flatten_record(record)

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="deployments", data=flattened_record)

        record_updated = record.get("updated")
        if record_updated and (max_updated is None or record_updated > max_updated):
            max_updated = record_updated

        record_count += 1

        if record_count % __CHECKPOINT_INTERVAL == 0:
            state["deployments_last_updated"] = max_updated
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)
            log.info(f"Checkpointed deployments at {record_count} records")

    if max_updated != last_updated:
        state["deployments_last_updated"] = max_updated
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

    log.info(f"Completed syncing {record_count} deployments")


def sync_flow_runs(base_url: str, headers: dict, state: dict):
    """
    Sync flow runs from Prefect API with incremental cursor based on updated timestamp.
    Args:
        base_url: Base URL for the Prefect API.
        headers: HTTP headers for authentication.
        state: State dictionary to track sync progress.
    """
    last_updated = state.get("flow_runs_last_updated")
    records = fetch_paginated_data(base_url, "/flow_runs/filter", headers, "updated", last_updated)

    log.info(f"Syncing {len(records)} flow runs")

    record_count = 0
    max_updated = last_updated

    for record in records:
        flattened_record = flatten_record(record)

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="flow_runs", data=flattened_record)

        record_updated = record.get("updated")
        if record_updated and (max_updated is None or record_updated > max_updated):
            max_updated = record_updated

        record_count += 1

        if record_count % __CHECKPOINT_INTERVAL == 0:
            state["flow_runs_last_updated"] = max_updated
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)
            log.info(f"Checkpointed flow_runs at {record_count} records")

    if max_updated != last_updated:
        state["flow_runs_last_updated"] = max_updated
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

    log.info(f"Completed syncing {record_count} flow runs")


def sync_task_runs(base_url: str, headers: dict, state: dict):
    """
    Sync task runs from Prefect API with incremental cursor based on updated timestamp.
    Args:
        base_url: Base URL for the Prefect API.
        headers: HTTP headers for authentication.
        state: State dictionary to track sync progress.
    """
    last_updated = state.get("task_runs_last_updated")
    records = fetch_paginated_data(base_url, "/task_runs/filter", headers, "updated", last_updated)

    log.info(f"Syncing {len(records)} task runs")

    record_count = 0
    max_updated = last_updated

    for record in records:
        flattened_record = flatten_record(record)

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="task_runs", data=flattened_record)

        record_updated = record.get("updated")
        if record_updated and (max_updated is None or record_updated > max_updated):
            max_updated = record_updated

        record_count += 1

        if record_count % __CHECKPOINT_INTERVAL == 0:
            state["task_runs_last_updated"] = max_updated
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)
            log.info(f"Checkpointed task_runs at {record_count} records")

    if max_updated != last_updated:
        state["task_runs_last_updated"] = max_updated
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

    log.info(f"Completed syncing {record_count} task runs")


def sync_artifacts(base_url: str, headers: dict, state: dict):
    """
    Sync artifacts from Prefect API with incremental cursor based on updated timestamp.
    Args:
        base_url: Base URL for the Prefect API.
        headers: HTTP headers for authentication.
        state: State dictionary to track sync progress.
    """
    last_updated = state.get("artifacts_last_updated")
    records = fetch_paginated_data(base_url, "/artifacts/filter", headers, "updated", last_updated)

    log.info(f"Syncing {len(records)} artifacts")

    record_count = 0
    max_updated = last_updated

    for record in records:
        flattened_record = flatten_record(record)

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="artifacts", data=flattened_record)

        record_updated = record.get("updated")
        if record_updated and (max_updated is None or record_updated > max_updated):
            max_updated = record_updated

        record_count += 1

        if record_count % __CHECKPOINT_INTERVAL == 0:
            state["artifacts_last_updated"] = max_updated
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)
            log.info(f"Checkpointed artifacts at {record_count} records")

    if max_updated != last_updated:
        state["artifacts_last_updated"] = max_updated
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

    log.info(f"Completed syncing {record_count} artifacts")


def sync_variables(base_url: str, headers: dict, state: dict):
    """
    Sync variables from Prefect API with incremental cursor based on updated timestamp.
    Args:
        base_url: Base URL for the Prefect API.
        headers: HTTP headers for authentication.
        state: State dictionary to track sync progress.
    """
    last_updated = state.get("variables_last_updated")
    records = fetch_paginated_data(base_url, "/variables/filter", headers, "updated", last_updated)

    log.info(f"Syncing {len(records)} variables")

    record_count = 0
    max_updated = last_updated

    for record in records:
        flattened_record = flatten_record(record)

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="variables", data=flattened_record)

        record_updated = record.get("updated")
        if record_updated and (max_updated is None or record_updated > max_updated):
            max_updated = record_updated

        record_count += 1

        if record_count % __CHECKPOINT_INTERVAL == 0:
            state["variables_last_updated"] = max_updated
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)
            log.info(f"Checkpointed variables at {record_count} records")

    if max_updated != last_updated:
        state["variables_last_updated"] = max_updated
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

    log.info(f"Completed syncing {record_count} variables")


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
