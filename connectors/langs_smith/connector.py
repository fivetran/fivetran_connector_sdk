import json        # For encoding and decoding JSON data
import time        # For handling sleep/delay and measuring execution time
from datetime import datetime, timezone   # For working with timestamps and time zones
from typing import Any, Dict, Iterable, List, Optional  # For type hints that improve code clarity and tooling
import requests    # For making HTTP/HTTPS API requests

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op



# --------------- Client ----------------

class LangSmithClient:
    def __init__(self, api_key: str, tenant_id: str, org_id: Optional[str] = None, base="https://api.smith.langchain.com"):
        self.base = base.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "x-api-key": api_key,
            "x-tenant-id": tenant_id,
            "accept": "application/json",
            "user-agent": "fivetran-connector-sdk-langsmith/1.0"
        })
        if org_id:
            self.session.headers["x-organization-id"] = org_id
        self.timeout = 60

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Generic GET request with retries for rate limiting.
        Args: param path: API endpoint path.
              param params: Query parameters.
        Returns: Parsed JSON response.
        Raises: requests.HTTPError for non-404 errors.
        """

        url = f"{self.base}{path}"
        for _ in range(1, 4):
            r = self.session.get(url, params=params, timeout=self.timeout)
            if r.status_code == 429:
                sleep_for = int(r.headers.get("Retry-After", 5))
                log.warning(f"Rate limited 429. Sleeping {sleep_for}s")
                time.sleep(sleep_for)
                continue
            if r.status_code == 422:
                log.warning(f"422 for {url} - retrying without params.")
                return self.get(path, None)
            if r.status_code == 404:
                raise requests.HTTPError(f"404 {url}", response=r)
            r.raise_for_status()
            return r.json()
        return {}

    def list_datasets(self) -> Iterable[Dict[str, Any]]:
        """Fetch datasets (no params).
        Returns: Iterable of dataset dictionaries.
        """
        data = self.get("/datasets")
        if isinstance(data, list):
            return data
        for k in ("datasets", "items", "data", "results"):
            if isinstance(data.get(k), list):
                return data[k]
        return []

    def list_examples(self, dataset: dict, limit: int = 200):
        """
        Fetch examples for a given dataset.
        Args: param dataset: The dataset dictionary.
              param limit: Maximum number of examples to fetch.
        Returns: List of example dictionaries.
        """

        ds_id = dataset.get("id") or dataset.get("dataset_id")
        created_at = dataset.get("created_at") or dataset.get("modified_at")

        if not ds_id:
            log.warning(f"Dataset missing ID: {dataset.get('name')}")
            return []

        params = {"dataset_id": ds_id, "limit": limit}

        # LangSmith requires full offset form (not 'Z')
        if created_at:
            ts = norm_ts(created_at)
            if ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"
            params["as_of"] = ts

        try:
            data = self.get("/examples", params)
        except requests.HTTPError as e:
            log.severe(f"Failed to fetch examples for dataset {ds_id}: {e}")
            return []

        if isinstance(data, list):
            return data
        for k in ("examples", "items", "data", "results"):
            v = data.get(k)
            if isinstance(v, list):
                return v
        return []



    def list_runs(self, limit: int = 200) -> Iterable[Dict[str, Any]]:
        """
        Fetch runs with optional limit.
        Args: param limit: Maximum number of runs to fetch.
        Returns: Iterable of run dictionaries.
        """
        params = {"limit": limit}
        data = self.get("/runs", params)
        if isinstance(data, list):
            return data
        for k in ("runs", "items", "data", "results"):
            if isinstance(data.get(k), list):
                return data[k]
        return []


# --------------- Utilities ----------------

def now_utc_iso():
    """
    Get the current UTC time in ISO 8601 format with 'Z' suffix.
    """
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def as_bool(val: Optional[str], default=False) -> bool:
    """
    Convert a string to boolean.
    Recognizes "true", "1", "y", "yes" (case insensitive) as True.
    Args: param val: The string value to convert.
          param default: The default boolean value if val is None.
    Returns: Boolean representation of the string.
    """
    if val is None:
        return default
    return str(val).lower() in {"true", "1", "y", "yes"}


def norm_ts(x: Any) -> Optional[str]:
    """Normalize timestamps to RFC3339 with +00:00 offset.
    Args param x: The input timestamp (int, float, or str).
    Returns: Normalized timestamp string or None.
    """
    if x is None:
        return None
    try:
        v = int(x)
        if v > 10**11:
            dt = datetime.fromtimestamp(v / 1000, tz=timezone.utc)
        else:
            dt = datetime.fromtimestamp(v, tz=timezone.utc)
        return dt.replace(microsecond=0).isoformat()
    except Exception:
        s = str(x).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        elif len(s) == 19:
            s = s + "+00:00"
        return s

# Define the schema function, which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration):
    return [
        {
            "table": "langsmith_dataset",
            "primary_key": ["dataset_id"],
            "columns": {
                "dataset_id": "STRING",
                "name": "STRING",
                "description": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "raw": "JSON"
            }
        },
        {
            "table": "langsmith_example",
            "primary_key": ["example_id"],
            "columns": {
                "example_id": "STRING",
                "dataset_name": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "inputs": "JSON",
                "outputs": "JSON",
                "metadata": "JSON",
                "raw": "JSON"
            }
        },
        {
            "table": "langsmith_run",
            "primary_key": ["run_id"],
            "columns": {
                "run_id": "STRING",
                "project_name": "STRING",
                "run_type": "STRING",
                "start_time": "UTC_DATETIME",
                "end_time": "UTC_DATETIME",
                "name": "STRING",
                "status": "STRING",
                "inputs": "JSON",
                "outputs": "JSON",
                "error": "STRING",
                "tags": "JSON",
                "raw": "JSON"
            }
        }
    ]


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: a dictionary that contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary that contains whatever state you have chosen to checkpoint during the prior sync.
# The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    base = configuration.get("LANGSMITH_BASE_URL", "https://api.smith.langchain.com")
    api_key = configuration.get("LANGSMITH_API_KEY")
    tenant_id = configuration.get("LANGSMITH_TENANT_ID")
    org_id = configuration.get("LANGSMITH_ORG_ID")
    page_size = int(configuration.get("PAGE_SIZE", 200))

    if not api_key or not tenant_id:
        raise ValueError("LANGSMITH_API_KEY and LANGSMITH_TENANT_ID are required.")

    client = LangSmithClient(api_key=api_key, tenant_id=tenant_id, org_id=org_id, base=base)

    # --- Datasets ---
    log.info("Syncing LangSmith datasets...")
    for d in client.list_datasets():
        ds_id = str(d.get("id") or d.get("dataset_id") or d.get("uuid") or d.get("name"))
        row = {
            "dataset_id": ds_id,
            "name": d.get("name"),
            "description": d.get("description"),
            "created_at": norm_ts(d.get("created_at")),
            "updated_at": norm_ts(d.get("updated_at")),
            "raw": d
        }
        if ds_id:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert(table="langsmith_dataset", data=row)

    # --- Examples ---
    if as_bool(configuration.get("SYNC_EXAMPLES"), True):
        log.info("Syncing LangSmith examples...")
        for d in client.list_datasets():
            for ex in client.list_examples(d, limit=page_size):
                ex_id = str(ex.get("id") or ex.get("uuid") or ex.get("example_id"))
                row = {
                    "example_id": ex_id,
                    "dataset_name": d.get("name"),
                    "created_at": norm_ts(ex.get("created_at")),
                    "updated_at": norm_ts(ex.get("updated_at")),
                    "inputs": ex.get("inputs"),
                    "outputs": ex.get("outputs"),
                    "metadata": ex.get("metadata"),
                    "raw": ex,
                }
                if ex_id:
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The op.upsert method is called with two arguments:
                    # - The first argument is the name of the table to upsert the data into.
                    # - The second argument is a dictionary containing the data to be upserted
                    op.upsert(table="langsmith_example", data=row)

    # --- Runs ---
    if as_bool(configuration.get("SYNC_RUNS"), True):
        log.info("Syncing LangSmith runs...")
        last = state.get("runs_last_start_time")
        max_seen = last
        for r in client.list_runs(limit=page_size):
            run_id = str(r.get("id") or r.get("run_id") or r.get("uuid"))
            start = norm_ts(r.get("start_time"))
            if last and start and start <= last:
                continue
            row = {
                "run_id": run_id,
                "project_name": r.get("project_name"),
                "run_type": r.get("run_type"),
                "start_time": start,
                "end_time": norm_ts(r.get("end_time")),
                "name": r.get("name"),
                "status": r.get("status"),
                "inputs": r.get("inputs"),
                "outputs": r.get("outputs"),
                "error": json.dumps(r.get("error")) if isinstance(r.get("error"), (dict, list)) else r.get("error"),
                "tags": r.get("tags"),
                "raw": r
            }
            if run_id:
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted
                op.upsert(table="langsmith_run", data=row)
            if start and (not max_seen or start > max_seen):
                max_seen = start
        if max_seen:
            state["runs_last_start_time"] = max_seen

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state={"synced_at": now_utc_iso()})
    log.info("LangSmith sync complete.")


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
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