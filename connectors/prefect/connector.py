"""
Prefect Cloud Connector for Fivetran Connector SDK.
This connector fetches workflow orchestration data from Prefect Cloud
API and syncs it to your data warehouse.
Supports incremental syncing of flow, deployment, work_pool,
work_queue, flow_run, task_run, artifact, and variable.
See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices)
for details.
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert and checkpoint
from fivetran_connector_sdk import Operations as op

# For making HTTP requests to Prefect API
import requests

# For handling delays in retry logic
import time

# Define constants for the connector
__BASE_URL_TEMPLATE = (
    "https://api.prefect.cloud/api/accounts/" "{account_id}/workspaces/{workspace_id}"
)
__MAX_RETRIES = 3
__BASE_DELAY_SECONDS = 1
__BATCH_SIZE = 100
__CHECKPOINT_INTERVAL = 500


def validate_configuration(configuration: dict):
    """
    Validate configuration dictionary for required parameters.
    This function is called at the start of the update method to
    ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration
        settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["api_key", "account_id", "workspace_id"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema
    your connector delivers.
    See the technical reference documentation for more details on
    the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration
        settings for the connector.
    """
    return [
        {"table": "flow", "primary_key": ["id"]},
        {"table": "deployment", "primary_key": ["id"]},
        {"table": "flow_run", "primary_key": ["id"]},
        {"table": "task_run", "primary_key": ["id"]},
        {"table": "artifact", "primary_key": ["id"]},
        {"table": "work_pool", "primary_key": ["id"]},
        {"table": "work_queue", "primary_key": ["id"]},
        {"table": "variable", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your
    connector fetches data.
    See the technical reference documentation for more details on
    the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration
        settings for the connector.
        state: a dictionary that holds the state of the connector.
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
        sync_work_pools(base_url, headers, state)
        sync_work_queues(base_url, headers, state)
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


def should_retry_request(attempt: int) -> bool:
    """
    Check if request should be retried based on attempt number.
    Args:
        attempt: Current attempt number (0-indexed).
    Returns:
        True if should retry, False otherwise.
    """
    return attempt < __MAX_RETRIES - 1


def calculate_retry_delay(attempt: int) -> int:
    """
    Calculate exponential backoff delay for retry attempt.
    Args:
        attempt: Current attempt number (0-indexed).
    Returns:
        Delay in seconds.
    """
    return __BASE_DELAY_SECONDS * (2**attempt)


def handle_retryable_error(attempt: int, error_message: str):
    """
    Handle retryable errors with logging and delay.
    Args:
        attempt: Current attempt number.
        error_message: Error description for logging.
    Raises:
        RuntimeError: If max retries exceeded.
    """
    if should_retry_request(attempt):
        delay = calculate_retry_delay(attempt)
        log.warning(
            f"{error_message}, retrying in {delay} seconds "
            f"(attempt {attempt + 1}/{__MAX_RETRIES})"
        )
        time.sleep(delay)
    else:
        log.severe(f"{error_message} after {__MAX_RETRIES} attempts")
        raise RuntimeError(f"{error_message} after {__MAX_RETRIES} attempts")


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
        RuntimeError: If the API request fails after all retry
        attempts.
    """
    for attempt in range(__MAX_RETRIES):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)

            if response.status_code == 200:
                return response.json()

            if response.status_code in [429, 500, 502, 503, 504]:
                error_msg = f"Request failed with status {response.status_code}"
                handle_retryable_error(attempt, error_msg)
                continue

            log.severe(
                f"API request failed with status " f"{response.status_code}: {response.text}"
            )
            raise RuntimeError(f"API request failed: {response.status_code} - " f"{response.text}")

        except requests.exceptions.Timeout:
            handle_retryable_error(attempt, "Request timeout")
            continue

        except requests.exceptions.ConnectionError:
            handle_retryable_error(attempt, "Connection error")
            continue


def fetch_paginated_data(
    base_url: str, endpoint: str, headers: dict, cursor_field: str, cursor_value: str = None
) -> list:
    """
    Fetch paginated data from Prefect API using filter endpoint.
    Performs client-side incremental filtering based on cursor value.
    Args:
        base_url: Base URL for the Prefect API.
        endpoint: API endpoint path.
        headers: HTTP headers for authentication.
        cursor_field: Field name to use for incremental filtering.
        cursor_value: Cursor value from last sync for client-side
        filtering.
    Returns:
        List of records filtered by cursor value.
    """
    all_records = []
    offset = 0

    while True:
        payload = {"limit": __BATCH_SIZE, "offset": offset}

        url = f"{base_url}{endpoint}"
        response_data = make_api_request(url, headers, payload)

        if not response_data or len(response_data) == 0:
            break

        # Client-side filtering for incremental sync
        if cursor_value:
            filtered_records = [
                record for record in response_data if record.get(cursor_field, "") > cursor_value
            ]
            all_records.extend(filtered_records)
            log.info(
                f"Fetched {len(response_data)} records from {endpoint}, "
                f"filtered to {len(filtered_records)} new records, "
                f"total: {len(all_records)}"
            )
        else:
            all_records.extend(response_data)
            log.info(
                f"Fetched {len(response_data)} records from {endpoint}, "
                f"total: {len(all_records)}"
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


def process_records(table_name: str, records: list, state: dict, state_key: str):
    """
    Process and upsert records with checkpointing.
    Args:
        table_name: Name of the destination table.
        records: List of records to process.
        state: State dictionary to track sync progress.
        state_key: Key in state dict to store cursor value.
    """
    log.info(f"Syncing {len(records)} {table_name}")

    record_count = 0
    max_updated = state.get(state_key)

    for record in records:
        flattened_record = flatten_record(record)

        # The 'upsert' operation is used to insert or update data
        # in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record
        # to be upserted.
        op.upsert(table=table_name, data=flattened_record)

        record_updated = record.get("updated")
        if record_updated and (max_updated is None or record_updated > max_updated):
            max_updated = record_updated

        record_count += 1

        if record_count % __CHECKPOINT_INTERVAL == 0:
            state[state_key] = max_updated
            # Save the progress by checkpointing the state. This is
            # important for ensuring that the sync process can resume
            # from the correct position in case of next sync or
            # interruptions.
            # Learn more about how and where to checkpoint by reading
            # our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)
            log.info(f"Checkpointed {table_name} at {record_count} records")

    if max_updated != state.get(state_key):
        state[state_key] = max_updated
        # Save the progress by checkpointing the state. This is
        # important for ensuring that the sync process can resume
        # from the correct position in case of next sync or
        # interruptions.
        # Learn more about how and where to checkpoint by reading
        # our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

    log.info(f"Completed syncing {record_count} {table_name}")


def sync_flows(base_url: str, headers: dict, state: dict):
    """
    Sync flows from Prefect API with incremental cursor based on
    updated timestamp.
    Args:
        base_url: Base URL for the Prefect API.
        headers: HTTP headers for authentication.
        state: State dictionary to track sync progress.
    """
    last_updated = state.get("flow_last_updated")
    records = fetch_paginated_data(base_url, "/flows/filter", headers, "updated", last_updated)
    process_records("flow", records, state, "flow_last_updated")


def sync_deployments(base_url: str, headers: dict, state: dict):
    """
    Sync deployments from Prefect API with incremental cursor based
    on updated timestamp.
    Args:
        base_url: Base URL for the Prefect API.
        headers: HTTP headers for authentication.
        state: State dictionary to track sync progress.
    """
    last_updated = state.get("deployment_last_updated")
    records = fetch_paginated_data(
        base_url, "/deployments/filter", headers, "updated", last_updated
    )
    process_records("deployment", records, state, "deployment_last_updated")


def sync_flow_runs(base_url: str, headers: dict, state: dict):
    """
    Sync flow runs from Prefect API with incremental cursor based on
    updated timestamp.
    Args:
        base_url: Base URL for the Prefect API.
        headers: HTTP headers for authentication.
        state: State dictionary to track sync progress.
    """
    last_updated = state.get("flow_run_last_updated")
    records = fetch_paginated_data(base_url, "/flow_runs/filter", headers, "updated", last_updated)
    process_records("flow_run", records, state, "flow_run_last_updated")


def sync_task_runs(base_url: str, headers: dict, state: dict):
    """
    Sync task runs from Prefect API with incremental cursor based on
    updated timestamp.
    Args:
        base_url: Base URL for the Prefect API.
        headers: HTTP headers for authentication.
        state: State dictionary to track sync progress.
    """
    last_updated = state.get("task_run_last_updated")
    records = fetch_paginated_data(base_url, "/task_runs/filter", headers, "updated", last_updated)
    process_records("task_run", records, state, "task_run_last_updated")


def sync_artifacts(base_url: str, headers: dict, state: dict):
    """
    Sync artifacts from Prefect API with incremental cursor based on
    updated timestamp.
    Args:
        base_url: Base URL for the Prefect API.
        headers: HTTP headers for authentication.
        state: State dictionary to track sync progress.
    """
    last_updated = state.get("artifact_last_updated")
    records = fetch_paginated_data(base_url, "/artifacts/filter", headers, "updated", last_updated)
    process_records("artifact", records, state, "artifact_last_updated")


def sync_work_pools(base_url: str, headers: dict, state: dict):
    """
    Sync work pools from Prefect API with incremental cursor based on
    updated timestamp.
    Args:
        base_url: Base URL for the Prefect API.
        headers: HTTP headers for authentication.
        state: State dictionary to track sync progress.
    """
    last_updated = state.get("work_pool_last_updated")
    records = fetch_paginated_data(
        base_url, "/work_pools/filter", headers, "updated", last_updated
    )
    process_records("work_pool", records, state, "work_pool_last_updated")


def sync_work_queues(base_url: str, headers: dict, state: dict):
    """
    Sync work queues from Prefect API with incremental cursor based on
    updated timestamp.
    Args:
        base_url: Base URL for the Prefect API.
        headers: HTTP headers for authentication.
        state: State dictionary to track sync progress.
    """
    last_updated = state.get("work_queue_last_updated")
    records = fetch_paginated_data(
        base_url, "/work_queues/filter", headers, "updated", last_updated
    )
    process_records("work_queue", records, state, "work_queue_last_updated")


def sync_variables(base_url: str, headers: dict, state: dict):
    """
    Sync variables from Prefect API with incremental cursor based on
    updated timestamp.
    Args:
        base_url: Base URL for the Prefect API.
        headers: HTTP headers for authentication.
        state: State dictionary to track sync progress.
    """
    last_updated = state.get("variable_last_updated")
    records = fetch_paginated_data(base_url, "/variables/filter", headers, "updated", last_updated)
    process_records("variable", records, state, "variable_last_updated")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be
# run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this
# method is not called by Fivetran when executing your connector in
# production.
# Please test using the Fivetran debug command prior to finalizing
# and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Test the connector locally
    connector.debug(configuration=configuration)
