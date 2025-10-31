import time       # Provides functions for measuring time, delays, and working with timestamps.
import datetime as dt  # Supplies classes for manipulating dates and times (dates, times, datetimes, timedeltas)
from typing import Optional  # Allows specifying that a type hint can be either a given type or None (improves readability and static type checking).

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

import requests  # For making HTTP requests to external APIs

__BASE_URL = "https://api.wandb.ai"
__REQUEST_TIMEOUT = 30
__MAX_RETRIES = 5
__BACKOFF_BASE = 1



def validate_configuration(configuration: dict):
    """Ensure required config keys are present."""
    required = ["api_key", "entity", "project"]
    for key in required:
        if key not in configuration or not configuration[key]:
            raise ValueError(f"Missing required configuration value: {key}")


def iso_now() -> str:
    """
    Get current UTC time in ISO 8601 format.
    :return:
    """
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()


def make_api_request(url: str, headers: dict, params: Optional[dict] = None) -> dict:
    """Generic GET with retries/backoff.
    Args: param url: API endpoint URL
          param headers: HTTP headers
          param params: Query parameters
    Returns: JSON response as dict
    Raises: requests.exceptions.RequestException on failure
    """
    params = params or {}
    for attempt in range(__MAX_RETRIES):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=__REQUEST_TIMEOUT)
            if r.status_code == 429 or r.status_code >= 500:
                if attempt < __MAX_RETRIES - 1:
                    delay = __BACKOFF_BASE * (2 ** attempt)
                    log.warning(f"Rate-limit/server error {r.status_code} for {url}. Retrying in {delay}s...")
                    time.sleep(delay)
                    continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            if attempt < __MAX_RETRIES - 1:
                delay = __BACKOFF_BASE * (2 ** attempt)
                log.warning(f"Request failed ({e}). Retrying in {delay}s...")
                time.sleep(delay)
                continue
            log.severe(f"HTTP error for {url}: {e}")
            raise

# Define the schema function, which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    """Define schema tables."""
    return [
        {"table": "runs", "primary_key": ["run_id"]},
        {"table": "metrics", "primary_key": ["run_id", "step", "metric_name"]},
    ]


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: a dictionary that contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary that contains whatever state you have chosen to checkpoint during the prior sync.
# The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: dict, state: dict):
    log.info("Starting Weights & Biases connector sync")

    validate_configuration(configuration)

    api_key = configuration["api_key"]
    entity = configuration["entity"]
    project = configuration["project"]
    page_size = 100

    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}

    last_sync = state.get("runs_hwm_utc")
    current_hwm = last_sync

    # -------------------------------------------------------------------
    # Sync runs
    # -------------------------------------------------------------------
    runs_url = f"{__BASE_URL}/projects/{entity}/{project}/runs"
    params = {"perPage": page_size}

    total_runs = 0
    while True:
        data = make_api_request(runs_url, headers, params)
        runs = data.get("runs", [])
        if not runs:
            break

        for run in runs:
            updated_at = run.get("updatedAt")
            if last_sync and updated_at and updated_at <= last_sync:
                continue

            run_record = {
                "run_id": run.get("id"),
                "name": run.get("displayName"),
                "state": run.get("state"),
                "user": (run.get("user") or {}).get("username"),
                "created_at": run.get("createdAt"),
                "updated_at": updated_at,
                "notes": run.get("notes"),
                "tags": json.dumps(run.get("tags", []), ensure_ascii=False),
                "config_json": json.dumps(run.get("config", {}), ensure_ascii=False),
                "summary_json": json.dumps(run.get("summaryMetrics", {}), ensure_ascii=False),
                "_fivetran_synced": iso_now(),
            }

            # The 'upsert' operation is used to insert or update data in a table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into, in this case, "hello".
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert("runs", run_record)
            total_runs += 1

            if updated_at and (not current_hwm or updated_at > current_hwm):
                current_hwm = updated_at

            # -------------------------------------------------------------------
            # Fetch metrics for each run
            # -------------------------------------------------------------------
            run_name = run.get("name") or run.get("id")
            if not run_name:
                continue
            metrics_url = f"{__BASE_URL}/runs/{entity}/{project}/{run_name}/history"
            m_params = {"perPage": 1000}
            m_data = make_api_request(metrics_url, headers, m_params)

            for row in m_data.get("history", []):
                step = row.get("_step")
                ts = row.get("_timestamp")
                timestamp = (
                    dt.datetime.utcfromtimestamp(ts).replace(tzinfo=dt.timezone.utc).isoformat()
                    if ts else None
                )
                for key, val in row.items():
                    if key.startswith("_"):
                        continue
                    metric = {
                        "run_id": run.get("id"),
                        "step": step,
                        "metric_name": key,
                        "metric_value": val,
                        "timestamp": timestamp,
                        "_fivetran_synced": iso_now(),
                    }

                    # The 'upsert' operation is used to insert or update data in a table.
                    # The op.upsert method is called with two arguments:
                    # - The first argument is the name of the table to upsert the data into, in this case, "hello".
                    # - The second argument is a dictionary containing the data to be upserted,
                    op.upsert("metrics", metric)

        next_url = data.get("nextUrl")
        if not next_url:
            break
        runs_url = next_url
        params = {}

    # -------------------------------------------------------------------
    # Checkpoint
    # -------------------------------------------------------------------
    if current_hwm and current_hwm != last_sync:
        new_state = {"runs_hwm_utc": current_hwm}

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(new_state)
    log.info(f"Synced {total_runs} runs from W&B")


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)
