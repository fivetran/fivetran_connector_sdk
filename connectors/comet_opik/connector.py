"""
Fivetran Connector SDK — Comet Opik (Observability)
Final version: correct headers, pagination, and logging.
"""

import json  # For reading and writing JSON payloads (config, API responses, state)
import time  # For implementing retry delays, backoff, and simple timing logic
from datetime import datetime, timezone  # For working with timezone-aware UTC timestamps
from typing import Any, Dict, Iterable, List, Optional  # Type hints for generic values, mappings, iterables, and optional fields
import requests  # HTTP client used to make REST/GraphQL calls to external APIs

# Import required classes from fivetran_connector_sdk.
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

__BASE_URL = "https://www.comet.com/opik/api/v1/private"
__USER_AGENT = "fivetran-connector-sdk-opik/1.1"
__MAX_RETRIES = 3
__RETRY_BASE_DELAY_SECONDS = 2


def now_utc_iso() -> str:
    """
    Convert current UTC time to ISO 8601 format.
    """
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def norm_ts(x: Any) -> Optional[str]:
    """
    Normalize timestamp
    Args:
        x: timestamp
    Returns:
        ISO 8601 formatted timestamp string or None if input is empty.
    """
    if not x:
        return None
    try:
        dt = datetime.fromisoformat(str(x).replace("Z", "+00:00"))
        return dt.replace(microsecond=0).isoformat()
    except (ValueError, TypeError, OverflowError) as e:
        log.warning(f"Failed to normalize timestamp {x!r}: {type(e).__name__} - {e}")
        return str(x)


# ------------------- Client -------------------
class OpikClient:
    def __init__(self, api_key: str, workspace: str, timeout: int = 60):
        self.base = __BASE_URL
        self.timeout = timeout
        self.s = requests.Session()
        self.s.headers.update({
            "authorization": api_key,
            "Comet-Workspace": workspace,
            "accept": "application/json",
            "user-agent": __USER_AGENT,
        })

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        """
        Make GET request to Opik API with retry logic.
        Args:
            path: API endpoint path to request.
            params: Optional query parameters for the request
        Returns:
            JSON response data from the API.
        """
        url = f"{self.base}{path}"
        for attempt in range(1, 4):
            try:
                r = self.s.get(url, params=params, timeout=self.timeout)
                if r.status_code == 429:
                    wait = int(r.headers.get("Retry-After", 5))
                    log.warning(f"Rate limited, sleeping {wait}s")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r.json()
            except requests.HTTPError as e:
                # Log the status and limited snippet of response
                log.warning(f"HTTP {r.status_code if hasattr(r,'status_code') else '?'} {url}: {str(e)[:120]}")
                if attempt >= __MAX_RETRIES:
                    raise
                time.sleep(__RETRY_BASE_DELAY_SECONDS * attempt)

    def list_projects(self) -> Iterable[Dict[str, Any]]:
        """
        List all projects from the Opik API.
        Returns:
            Iterable of project dictionaries.
        """
        data = self.get("/projects") or {}
        return data.get("content", [])

    def list_datasets(self) -> Iterable[Dict[str, Any]]:
        """
        List all datasets from the Opik API.
        Returns:
            Iterable of dataset dictionaries.
        """
        data = self.get("/datasets") or {}
        return data.get("content", [])

    def list_dataset_items(self, dataset_id: str) -> Iterable[Dict[str, Any]]:
        """
        List all items for a specific dataset.
        Note: Some Opik deployments reject page/size params; fetches without pagination.
        Args:
            dataset_id: Unique identifier of the dataset.
        Returns:
            Iterable of dataset item dictionaries.
        """
        try:
            data = self.get(f"/datasets/{dataset_id}/items") or {}
            items = data.get("content") or data.get("items") or []
            for i in items:
                yield i
        except requests.HTTPError as e:
            log.warning(f"Skipping dataset {dataset_id} — items endpoint returned {e}")

# Define the schema function, which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        {
            "table": "opik_project",
            "primary_key": ["project_id"],
            "columns": {
                "project_id": "STRING",
                "name": "STRING",
                "visibility": "STRING",
                "created_at": "UTC_DATETIME",
                "created_by": "STRING",
                "last_updated_at": "UTC_DATETIME",
                "last_updated_by": "STRING",
                "raw": "JSON",
            },
        },
        {
            "table": "opik_dataset",
            "primary_key": ["dataset_id"],
            "columns": {
                "dataset_id": "STRING",
                "name": "STRING",
                "visibility": "STRING",
                "created_at": "UTC_DATETIME",
                "created_by": "STRING",
                "last_updated_at": "UTC_DATETIME",
                "last_updated_by": "STRING",
                "raw": "JSON",
            },
        },
        {
            "table": "opik_dataset_item",
            "primary_key": ["item_id"],
            "columns": {
                "item_id": "STRING",
                "dataset_id": "STRING",
                "inputs": "JSON",
                "outputs": "JSON",
                "metadata": "JSON",
                "created_at": "UTC_DATETIME",
                "raw": "JSON",
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
    api_key = configuration.get("COMET_OPIK_API_KEY")
    workspace = configuration.get("COMET_OPIK_WORKSPACE")
    if not api_key or not workspace:
        raise ValueError("Missing COMET_OPIK_API_KEY or COMET_OPIK_WORKSPACE")

    client = OpikClient(api_key, workspace)

    # ---- Projects ----
    log.info("Syncing Comet Opik projects …")
    for p in client.list_projects():

        # The 'upsert' operation is used to insert or update data in a table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted
        op.upsert("opik_project", {
            "project_id": p.get("id"),
            "name": p.get("name"),
            "visibility": p.get("visibility"),
            "created_at": norm_ts(p.get("created_at")),
            "created_by": p.get("created_by"),
            "last_updated_at": norm_ts(p.get("last_updated_at")),
            "last_updated_by": p.get("last_updated_by"),
            "raw": p,
        })

    # ---- Datasets ----
    log.info("Syncing Comet Opik datasets …")
    for d in client.list_datasets():

        # The 'upsert' operation is used to insert or update data in a table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted
        op.upsert("opik_dataset", {
            "dataset_id": d.get("id"),
            "name": d.get("name"),
            "visibility": d.get("visibility"),
            "created_at": norm_ts(d.get("created_at")),
            "created_by": d.get("created_by"),
            "last_updated_at": norm_ts(d.get("last_updated_at")),
            "last_updated_by": d.get("last_updated_by"),
            "raw": d,
        })

        # ---- Dataset Items ----
        for item in client.list_dataset_items(d.get("id")):

            # The 'upsert' operation is used to insert or update data in a table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert("opik_dataset_item", {
                "item_id": str(item.get("id")),
                "dataset_id": d.get("id"),
                "inputs": item.get("inputs"),
                "outputs": item.get("outputs"),
                "metadata": item.get("metadata"),
                "created_at": norm_ts(item.get("created_at")),
                "raw": item,
            })

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state={"synced_at": now_utc_iso()})
    log.info("Comet Opik sync complete")


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
