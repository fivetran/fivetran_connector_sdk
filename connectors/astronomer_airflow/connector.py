# Astronomer / Airflow Cloud API Connector for Fivetran
# Example connector built using fivetran_connector_sdk
# -----------------------------------------------------
# Fetches metadata from an Astronomer-hosted Airflow deployment via REST API
# and upserts DAGs, DAG Runs, and Task Instances incrementally.
#
# Docs:
#  - https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
#  - https://fivetran.com/docs/connectors/connector-sdk/best-practices
#  - https://www.astronomer.io/docs/astro/airflow-api/

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

import requests
from datetime import datetime, timedelta, timezone
import json
import time

# --- Table definitions ---
TABLE_DAGS = "airflow_dags"
TABLE_DAGRUNS = "airflow_dag_runs"
TABLE_TASKS = "airflow_task_instances"


# ---------- Utilities ----------

def iso_now():
    """
    return: Current UTC time in ISO 8601 format
    """
    return datetime.now(timezone.utc).isoformat()


def parse_iso(s: str):
    """
    Parse an ISO 8601 datetime string to a datetime object.
        Returns None if parsing fails.
    """
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def to_iso(dt: datetime):
    """
    Convert a datetime object to an ISO 8601 string in UTC.
    Args param dt: datetime object
    Returns ISO 8601 string or None
    """
    return dt.astimezone(timezone.utc).isoformat() if dt else None


class AirflowClient:
    def __init__(self, base_url: str, token: str, verify_ssl=True):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.token = token
        self.verify_ssl = verify_ssl

    def headers(self):
        """
        Returns the headers required for API requests.
        """
        return {"Authorization": f"Bearer {self.token}", "Accept": "application/json"}

    def request(self, method, path, params=None):
        """
        Makes an API request with retries for certain status codes.
        429, 502, 503, 504 are retried with exponential backoff.
        """
        url = f"{self.base_url}{path}"
        for attempt in range(5):
            resp = self.session.request(method, url, headers=self.headers(), params=params, timeout=60, verify=self.verify_ssl)
            if resp.status_code in (429, 502, 503, 504):
                delay = min(60, 2 ** attempt)
                log.warn(f"Retrying {url} after {resp.status_code}, sleeping {delay}s")
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f"API call failed: {url}")

    def list_dags(self):
        """
        Generator to list all DAGs with pagination.
        """
        offset = 0
        limit = 100
        while True:
            res = self.request("GET", "/api/v2/dags", params={"limit": limit, "offset": offset})
            dags = res.get("dags", [])
            for d in dags:
                yield d
            if len(dags) < limit:
                break
            offset += limit

    def list_dag_runs(self, dag_id: str, since: datetime = None):
        offset = 0
        limit = 100
        while True:
            res = self.request("GET", f"/api/v2/dags/{dag_id}/dagRuns", params={"limit": limit, "offset": offset})
            runs = res.get("dag_runs", [])
            for r in runs:
                ld = parse_iso(r.get("logical_date") or r.get("execution_date"))
                if not since or (ld and ld > since):
                    yield r
            if len(runs) < limit:
                break
            offset += limit

    def list_task_instances(self, dag_id: str, dag_run_id: str):
        offset = 0
        limit = 100
        while True:
            res = self.request("GET", f"/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances", params={"limit": limit, "offset": offset})
            tis = res.get("task_instances", [])
            for ti in tis:
                yield ti
            if len(tis) < limit:
                break
            offset += limit



def validate_configuration(cfg: dict):
    required = ["base_url", "api_token"]
    for k in required:
        if k not in cfg:
            raise ValueError(f"Missing configuration key: {k}")


# Define the schema function, which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": TABLE_DAGS,
            "primary_key": ["dag_id"],
            "columns": {
                "dag_id": "STRING",
                "is_paused": "BOOLEAN",
                "is_active": "BOOLEAN",
                "owners": "STRING",
                "tags": "STRING",
                "timezone": "STRING",
                "max_active_runs": "INT",
                "default_view": "STRING",
                "next_dagrun_create_after": "STRING",
                "_synced_at": "UTC_DATETIME",
            },
        },
        {
            "table": TABLE_DAGRUNS,
            "primary_key": ["dag_id", "dag_run_id"],
            "columns": {
                "dag_id": "STRING",
                "dag_run_id": "STRING",
                "run_type": "STRING",
                "state": "STRING",
                "logical_date": "UTC_DATETIME",
                "start_date": "UTC_DATETIME",
                "end_date": "UTC_DATETIME",
                "external_trigger": "BOOLEAN",
                "data_interval_start": "STRING",
                "data_interval_end": "STRING",
                "_synced_at": "UTC_DATETIME",
            },
        },
        {
            "table": TABLE_TASKS,
            "primary_key": ["dag_id", "dag_run_id", "task_id", "try_number"],
            "columns": {
                "dag_id": "STRING",
                "dag_run_id": "STRING",
                "task_id": "STRING",
                "try_number": "INT",
                "state": "STRING",
                "start_date": "UTC_DATETIME",
                "end_date": "UTC_DATETIME",
                "duration": "FLOAT",
                "operator": "STRING",
                "hostname": "STRING",
                "_synced_at": "UTC_DATETIME",
            },
        },
    ]

# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: a dictionary that contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary that contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration, state):
    """
    Fetches data from the Astronomer Airflow API and upserts it into destination tables.
    """
    log.info("Starting Astronomer Airflow sync")
    validate_configuration(configuration)

    client = AirflowClient(configuration["base_url"], configuration["api_token"], verify_ssl=bool(configuration.get("verify_ssl", True)))
    now = iso_now()
    state = state or {}

    # ---- 1. Fetch DAGs ----
    dags = []
    for d in client.list_dags():
        dags.append({
            "dag_id": d.get("dag_id"),
            "is_paused": d.get("is_paused"),
            "is_active": d.get("is_active"),
            "owners": ",".join(d.get("owners", [])),
            "tags": ",".join([t.get("name") for t in d.get("tags", [])]),
            "timezone": d.get("timezone"),
            "max_active_runs": d.get("max_active_tasks") or d.get("max_active_runs"),
            "default_view": d.get("default_view"),
            "next_dagrun_create_after": d.get("next_dagrun_create_after"),
            "_synced_at": now,
        })
    if dags:
        for dag in dags:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table=TABLE_DAGS, data=dag)

    # ---- 2. Fetch DAG Runs ----
    for d in dags:
        dag_id = d["dag_id"]
        cursor_key = f"{dag_id}_cursor"
        since_str = state.get(cursor_key)
        since_dt = parse_iso(since_str) if since_str else datetime.now(timezone.utc) - timedelta(days=7)
        newest = since_dt

        runs = []
        for r in client.list_dag_runs(dag_id, since_dt):
            ld = parse_iso(r.get("logical_date") or r.get("execution_date"))
            runs.append({
                "dag_id": dag_id,
                "dag_run_id": r.get("dag_run_id") or r.get("run_id"),
                "run_type": r.get("run_type"),
                "state": r.get("state"),
                "logical_date": to_iso(ld),
                "start_date": r.get("start_date"),
                "end_date": r.get("end_date"),
                "external_trigger": r.get("external_trigger"),
                "data_interval_start": r.get("data_interval_start"),
                "data_interval_end": r.get("data_interval_end"),
                "_synced_at": now,
            })
            if ld and ld > newest:
                newest = ld

        if runs:
            for run in runs:
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted,
                op.upsert(table=TABLE_DAGRUNS, data=run)
        if newest:
            state[cursor_key] = to_iso(newest)

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

    # ---- 3. Fetch Task Instances ----
    for d in dags:
        dag_id = d["dag_id"]
        cursor_key = f"{dag_id}_cursor"
        since_dt = parse_iso(state.get(cursor_key)) if state.get(cursor_key) else None

        # Get recent runs only (last 7 days)
        for r in client.list_dag_runs(dag_id, since_dt):
            run_id = r.get("dag_run_id") or r.get("run_id")
            ti_rows = []
            for ti in client.list_task_instances(dag_id, run_id):
                ti_rows.append({
                    "dag_id": dag_id,
                    "dag_run_id": run_id,
                    "task_id": ti.get("task_id"),
                    "try_number": ti.get("try_number", 0),
                    "state": ti.get("state"),
                    "start_date": ti.get("start_date"),
                    "end_date": ti.get("end_date"),
                    "duration": ti.get("duration"),
                    "operator": ti.get("operator"),
                    "hostname": ti.get("hostname"),
                    "_synced_at": now,
                })
            if ti_rows:
                for ti_row in ti_rows:
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The op.upsert method is called with two arguments:
                    # - The first argument is the name of the table to upsert the data into.
                    # - The second argument is a dictionary containing the data to be upserted,
                    op.upsert(table=TABLE_TASKS, data=ti_row)

    log.info("Sync completed successfully")
    return state


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
