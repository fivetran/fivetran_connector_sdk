"""
Fivetran Connector SDK – Dagster (runsOrError + assetNodes; list-safe)
- Fixes: converts list values to supported types before upsert.
- Runs: paginates by 'id' (response has no 'cursor').
- Assets: uses assetNodes only (no legacy 'Assets' type).
"""

import json
import time
from typing import Any, Dict, Optional, List

import requests
from fivetran_connector_sdk import Connector, Operations as op, Logging as log

# -----------------------------
# GraphQL queries
# -----------------------------
RUNS_QUERY = """
query getRuns($cursor: String) {
  runsOrError(limit: 100, cursor: $cursor) {
    ... on Runs {
      results {
        id
        runId
        status
        jobName
        mode
        startTime
        endTime
        updateTime
        tags { key value }
      }
    }
  }
}
"""

ASSET_NODES_QUERY = """
query getAssetNodes {
  assetNodes {
    id
    assetKey { path }
    description
    assetMaterializations {
      runId
      timestamp
    }
  }
}
"""

SCHEDULES_QUERY = """
query getSchedules {
  schedulesOrError {
    ... on Schedules {
      results {
        id
        name
        cronSchedule
        pipelineName
        scheduleState { status runningCount }
      }
    }
  }
}
"""

SENSORS_QUERY = """
query getSensors {
  sensorsOrError {
    ... on Sensors {
      results {
        id
        name
        sensorType
        targets { pipelineName }
        sensorState { status runs { runId } }
      }
    }
  }
}
"""

# -----------------------------
# HTTP / GraphQL client
# -----------------------------
class DagsterClient:
    def __init__(self, base_url: str, api_token: Optional[str] = None, timeout: int = 30):
        self.base = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "fivetran-connector-sdk-dagster/1.0",
            }
        )
        if api_token:
            self.session.headers["Dagster-Cloud-Api-Token"] = api_token

    def query(self, graphql_query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute a GraphQL query against the Dagster endpoint.
        Args: param graphql_query: The GraphQL query string.
              param variables: Optional dictionary of variables for the query.
        Returns: The 'data' field from the GraphQL response.
        Raises: RuntimeError if the response contains errors or has a 400 status code.
        """

        url = f"{self.base}/graphql"
        payload = {"query": graphql_query, "variables": variables or {}}
        r = self.session.post(url, json=payload, timeout=self.timeout)

        if r.status_code == 400:
            try:
                body = r.json()
            except Exception:
                body = {"text": r.text}
            raise RuntimeError(f"GraphQL 400: {body}")

        r.raise_for_status()
        data = r.json()
        if "errors" in data:
            raise RuntimeError(f"GraphQL error: {data['errors']}")
        return data.get("data", {})

# -----------------------------
# Tables & schema
# -----------------------------
TABLES = {
    "dagster_runs": {
        "pk": ["run_id"],
        "updated": "update_time",
        "schema": {
            "run_id": "STRING",
            "pipeline_name": "STRING",   # jobName
            "status": "STRING",
            "mode": "STRING",
            "start_time": "UTC_DATETIME",
            "end_time": "UTC_DATETIME",
            "update_time": "UTC_DATETIME",
            "tags": "JSON",              # dict only (we'll convert list -> dict)
        },
    },
    "dagster_assets": {
        "pk": ["id"],
        "schema": {
            "id": "STRING",
            "key_path": "STRING",        # store dot-path (list -> string)
            "description": "STRING",
            "last_materialization_run_id": "STRING",
            "last_materialization_time": "UTC_DATETIME",
        },
    },
    "dagster_schedules": {
        "pk": ["id"],
        "schema": {
            "id": "STRING",
            "name": "STRING",
            "cron_schedule": "STRING",
            "pipeline_name": "STRING",
            "status": "STRING",
            "running_count": "INTEGER",
        },
    },
    "dagster_sensors": {
        "pk": ["id"],
        "schema": {
            "id": "STRING",
            "name": "STRING",
            "sensor_type": "STRING",
            "pipeline_name": "STRING",
            "status": "STRING",
            "active_run_ids": "STRING",  # store JSON string (list -> string)
        },
    },
}

# Define the schema function, which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: Dict[str, Any]):
    return [
        {"table": t, "primary_key": meta["pk"], "column": meta["schema"]}
        for t, meta in TABLES.items()
    ]

def ts_to_iso(ts) -> Optional[str]:
    """Convert epoch timestamp (float/int/str) to ISO 8601 UTC string.
    if ts is None: return None
    """
    if ts is None:
        return None
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(ts)))
    except Exception:
        return None

def tags_list_to_dict(tags: Any) -> Optional[Dict[str, Any]]:
    """
    Dagster returns tags as a list of {key, value}. SDK JSON expects dict (not list).
    Convert list to dict; if already dict, return as-is.
    Args: param tags: list of {key, value} dicts or dict
    Returns: dict of tags or None
    """
    if not tags:
        return None
    if isinstance(tags, list):
        out = {}
        for t in tags:
            if isinstance(t, dict) and "key" in t:
                out[t["key"]] = t.get("value")
        return out or None
    if isinstance(tags, dict):
        return tags
    return None

def path_list_to_dot(path: Any) -> Optional[str]:
    """
    path is either a list of path segments or a string.
    Convert list to dot-separated string; if already string, return as-is.
    :param path:
    Args: param path: list or string
    """
    if isinstance(path, list):
        return ".".join(str(p) for p in path)
    if isinstance(path, str):
        return path
    return None

def list_to_json_str(v: Any) -> Optional[str]:
    """
    SDK won't accept raw lists; store as JSON string.
    Args: param v: list or other
    Returns: JSON string or None
    """
    if v is None:
        return None
    if isinstance(v, list):
        return json.dumps(v, ensure_ascii=False)
    # if it's already a string/dict/other, stringify safely
    try:
        return json.dumps(v, ensure_ascii=False) if not isinstance(v, str) else v
    except Exception:
        return str(v)

def get_cursor(state: Dict[str, Any], table: str) -> Optional[str]:
    """
    Get the cursor for a specific table from the state.
    Args: param state: The current state dictionary.
          param table: The table name for which to get the cursor.
    Returns: The cursor value for the specified table, or None if not found.
    """
    return (state or {}).get("cursors", {}).get(table)

def set_cursor(state: Dict[str, Any], table: str, cursor: Optional[str]) -> Dict[str, Any]:
    """
    Set the cursor for a specific table in the state.
    Args: param state: The current state dictionary.
          param table: The table name for which to set the cursor.
          param cursor: The cursor value to set for the specified table.
    Returns: The updated state dictionary with the new cursor set for the specified table.
    """
    new_state = dict(state or {})
    cursors = dict(new_state.get("cursors", {}))
    if cursor:
        cursors[table] = cursor
    new_state["cursors"] = cursors
    return new_state


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: a dictionary that contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary that contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    base = configuration["base_url"]
    token = configuration.get("api_token")
    client = DagsterClient(base_url=base, api_token=token)

    # -------- Runs (incremental via "last id as cursor") --------
    cursor = get_cursor(state, "dagster_runs")
    max_cursor = cursor

    while True:
        data = client.query(RUNS_QUERY, {"cursor": cursor})
        runs_obj = data.get("runsOrError", {}) or {}
        results: List[Dict[str, Any]] = runs_obj.get("results", []) or []

        if not results:
            break

        for r in results:

            # The 'upsert' operation is used to insert or update data in a table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into, in this case, "hello".
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(
                "dagster_runs",
                {
                    "run_id": r["runId"],
                    "pipeline_name": r.get("jobName"),
                    "status": r.get("status"),
                    "mode": r.get("mode"),
                    "start_time": ts_to_iso(r.get("startTime")),
                    "end_time": ts_to_iso(r.get("endTime")),
                    "update_time": ts_to_iso(r.get("updateTime")),
                    "tags": tags_list_to_dict(r.get("tags")),  # LIST -> DICT
                },
            )
            max_cursor = r["id"]

        cursor = results[-1]["id"]

    state = set_cursor(state, "dagster_runs", max_cursor)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)

    # -------- Assets via assetNodes --------
    try:
        data = client.query(ASSET_NODES_QUERY)
        nodes = data.get("assetNodes")
        if isinstance(nodes, list):
            for n in nodes:
                lm = n.get("assetMaterializations")

                # The 'upsert' operation is used to insert or update data in a table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into, in this case, "hello".
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(
                    "dagster_assets",
                    {
                        "id": n["id"],
                        "key_path": path_list_to_dot((n.get("assetKey") or {}).get("path")),  # LIST -> STRING
                        "description": n.get("description"),
                        "last_materialization_run_id": (lm or {}).get("runId"),
                        "last_materialization_time": ts_to_iso((lm or {}).get("timestamp")),
                    },
                )
    except Exception as e:
        log.warning(f"Skipping assets due to schema mismatch: {e}")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)

    # -------- Schedules (best effort) --------
    try:
        schedules = client.query(SCHEDULES_QUERY).get("schedulesOrError", {}).get("results", []) or []
        for s in schedules:
            st = s.get("scheduleState") or {}

            # The 'upsert' operation is used to insert or update data in a table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into, in this case, "hello".
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(
                "dagster_schedules",
                {
                    "id": s["id"],
                    "name": s.get("name"),
                    "cron_schedule": s.get("cronSchedule"),
                    "pipeline_name": s.get("pipelineName"),
                    "status": st.get("status"),
                    "running_count": st.get("runningCount"),
                },
            )
    except Exception as e:
        log.warning(f"Skipping schedules due to schema mismatch: {e}")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)

    # -------- Sensors (best effort) --------
    try:
        sensors = client.query(SENSORS_QUERY).get("sensorsOrError", {}).get("results", []) or []
        for s in sensors:
            st = s.get("sensorState") or {}
            targets = s.get("targets") or [{}]
            active_ids = [r.get("runId") for r in (st.get("runs") or [])]

            # The 'upsert' operation is used to insert or update data in a table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into, in this case, "hello".
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(
                "dagster_sensors",
                {
                    "id": s["id"],
                    "name": s.get("name"),
                    "sensor_type": s.get("sensorType"),
                    "pipeline_name": targets[0].get("pipelineName"),
                    "status": st.get("status"),
                    "active_run_ids": list_to_json_str(active_ids),  # LIST -> STRING
                },
            )
    except Exception as e:
        log.warning(f"Skipping sensors due to schema mismatch: {e}")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)
    log.info("Dagster sync complete.")


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
