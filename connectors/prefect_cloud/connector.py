"""
Fivetran Connector SDK for Prefect
----------------------------------
Syncs Prefect data (flows, flow_runs, deployments).
- Uses Prefect REST API (Cloud 2.x or self-hosted)
- Supports pagination via `limit` and `offset`
- Incremental sync on `updated` timestamp
Docs: https://docs.prefect.io/latest/api-ref/rest/
"""

import requests
import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from fivetran_connector_sdk import Connector, Logging as log, Operations as op


# Define the schema function, which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        {
            "table": "flows",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "tags": "JSON",
                "updated": "UTC_DATETIME",
                "_fivetran_synced": "UTC_DATETIME",
            },
        },
        {
            "table": "flow_runs",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "state_type": "STRING",
                "flow_id": "STRING",
                "deployment_id": "STRING",
                "start_time": "UTC_DATETIME",
                "end_time": "UTC_DATETIME",
                "updated": "UTC_DATETIME",
                "parameters": "JSON",
                "_fivetran_synced": "UTC_DATETIME",
            },
        },
        {
            "table": "deployments",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "flow_id": "STRING",
                "schedule": "JSON",
                "updated": "UTC_DATETIME",
                "_fivetran_synced": "UTC_DATETIME",
            },
        },
    ]


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: a dictionary that contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary that contains whatever state you have chosen to checkpoint during the prior sync.
# The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    if state is None:
        state = {}
    merged_state = dict(state)

    client = PrefectClient(
        base_url=configuration["api_url"],
        api_key=configuration["api_key"],
        batch=100,
    )

    # --- Flows (incremental)
    last_flow_update = merged_state.get("flows", {}).get("last_incremental_value")
    for row in client.paginated_get("/flows/filter", cursor_field="updated", last_value=last_flow_update):
        row["_fivetran_synced"] = utc_now()

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted
        yield op.upsert("flows", row)
        ts = row.get("updated")
        if ts and (not last_flow_update or ts > last_flow_update):
            last_flow_update = ts
    merged_state["flows"] = {"last_incremental_value": last_flow_update}

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(dict(merged_state))

    # --- Flow Runs (incremental)
    last_run_update = merged_state.get("flow_runs", {}).get("last_incremental_value")
    for row in client.paginated_get("/flow_runs/filter", cursor_field="updated", last_value=last_run_update):
        row["_fivetran_synced"] = utc_now()

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted
        yield op.upsert("flow_runs", row)
        ts = row.get("updated")
        if ts and (not last_run_update or ts > last_run_update):
            last_run_update = ts
    merged_state["flow_runs"] = {"last_incremental_value": last_run_update}

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(dict(merged_state))

    # --- Deployments (full refresh)
    for row in client.paginated_get("/deployments/filter"):
        row["_fivetran_synced"] = utc_now()

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted
        yield op.upsert("deployments", row)
    merged_state["deployments"] = {"last_sync": utc_now()}

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(dict(merged_state))


# ---------- CLIENT ----------
class PrefectClient:
    def __init__(self, base_url: str, api_key: str, batch: int = 100):
        self.base_url = base_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        self.batch = batch

    def paginated_get(self, endpoint: str, cursor_field: Optional[str] = None, last_value: Optional[str] = None):
        """POST /flows/filter or /flow_runs/filter expects JSON body with pagination and filters.
        Args: param endpoint: API endpoint (e.g., /flows/filter)
              param cursor_field: field for incremental sync (e.g., 'updated')
              param last_value: last synced value for incremental sync
        Yields: normalized records from the API
        """
        offset = 0
        while True:
            body = {"offset": offset, "limit": self.batch}
            if last_value and cursor_field:
                body["sort"] = [{"updated": "asc"}]
                body["updated_after"] = last_value

            url = f"{self.base_url}{endpoint}"
            resp = requests.post(url, headers=self.headers, json=body, timeout=30)

            if resp.status_code == 404:
                log.warning(f"{url} not found; skipping.")
                return
            if not resp.ok:
                raise Exception(f"Prefect API error {resp.status_code}: {resp.text}")

            data = resp.json()

            # FIX: handle both list and dict responses
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get("result") or data.get("results") or []
            else:
                items = []

            if not items:
                break

            for i in items:
                yield normalize(i)

            if len(items) < self.batch:
                break
            offset += self.batch


def normalize(row: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten nested structures and normalize datetimes.
    Args: param row: original record
    Returns: normalized record
    """
    out = {}
    for k, v in row.items():
        if isinstance(v, (list, dict)):
            out[k] = json.dumps(v)
        elif isinstance(v, str) and looks_datetime(v):
            out[k] = to_utc(v)
        else:
            out[k] = v
    return out


def looks_datetime(s: str) -> bool:
    """
    Check if a string looks like an ISO 8601 datetime.
    :param s:
    :return:
    """
    return "T" in s and ("Z" in s or "+" in s)


def to_utc(s: str) -> str:
    """
    Convert an ISO 8601 datetime string to UTC ISO format.
    Args: param s: original datetime string
    Returns: UTC ISO datetime string or original if parsing fails
    """
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return s


def utc_now() -> str:
    """
    Get the current UTC time in ISO format.
    :return:
    """
    return datetime.now(timezone.utc).isoformat()


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
