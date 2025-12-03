import json      # Provides tools to encode and decode JSON data (convert between Python objects and JSON strings)
import time      # Gives access to time-related functions like sleep() and timestamps
from datetime import datetime, timezone   # Handles date and time objects, including timezone-aware timestamps
from typing import Any, Dict, List, Optional   # Enables static type hints for variables, functions, and classes
import requests  # Simplifies making HTTP requests (GET, POST, etc.) to web APIs

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__BASE_URL = "https://api.dune.com/api/v1"
__UA = "fivetran-connector-sdk-dune/clean"
__TIMEOUT = 60


def now_utc_iso() -> str:
    """Get the current UTC time in ISO 8601 format without microseconds."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class DuneClient:
    def __init__(self, api_key: str):
        self.s = requests.Session()
        self.s.headers.update({
            "X-DUNE-API-KEY": api_key,
            "Accept": "application/json",
            "User-Agent": __UA,
        })

    def get(self, path: str) -> Dict[str, Any]:
        """Make a GET request to the Dune API.
        Args: param path: The API endpoint path.
        Returns: Dict[str, Any]: The JSON response from the API.
        Raises: requests.HTTPError: If the request fails.
        """
        url = f"{__BASE_URL}{path}"
        r = self.s.get(url, timeout=__TIMEOUT)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 5))
            log.warning(f"Rate limited. Sleeping {wait}s")
            time.sleep(wait)
            return self.get(path)
        r.raise_for_status()
        return r.json()

    def post(self, path: str) -> Dict[str, Any]:
        """
        Make a POST request to the Dune API.
        Args: param path: The API endpoint path.
        Returns: Dict[str, Any]: The JSON response from the API.
        Raises: requests.HTTPError: If the request fails.
        """
        url = f"{__BASE_URL}{path}"
        r = self.s.post(url, timeout=__TIMEOUT)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 5))
            log.warning(f"Rate limited. Sleeping {wait}s")
            time.sleep(wait)
            return self.post(path)
        r.raise_for_status()
        return r.json()

    def execute_query(self, query_id: int) -> str:
        """
        Execute a Dune query by its ID.
        Args: param query_id: The ID of the Dune query to execute.
        Returns: str: The execution ID of the started query.
        Raises: requests.HTTPError: If the request fails.
        """
        data = self.post(f"/query/{query_id}/execute")
        return data.get("execution_id")

    def get_status(self, execution_id: str) -> str:
        """
        Get the status of a Dune query execution.
        Args: param execution_id: The execution ID of the Dune query.
        Returns: str: The current state of the query execution.
        Raises: requests.HTTPError: If the request fails.
        """
        data = self.get(f"/execution/{execution_id}/status")
        return data.get("state")

    def get_results(self, execution_id: str) -> List[Dict[str, Any]]:
        """
        Get the results of a Dune query execution.
        Args param execution_id: The execution ID of the Dune query.
        Returns: List[Dict[str, Any]]: The rows of the query result.
        Raises: requests.HTTPError: If the request fails.
        """
        data = self.get(f"/execution/{execution_id}/results")
        return data.get("result", {}).get("rows", [])

# Define the schema function, which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration):
    return [
        {
            "table": "dune_query_result",
            "primary_key": ["query_id", "execution_id", "row_id"],
            "columns": {
                "query_id": "LONG",
                "execution_id": "STRING",
                "row_id": "LONG",
                "data": "JSON",
                "synced_at": "UTC_DATETIME",
            },
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
    api_key = configuration.get("DUNE_API_KEY")
    if not api_key:
        raise ValueError("Missing DUNE_API_KEY")

    dune = DuneClient(api_key)
    raw_ids = configuration.get("DUNE_QUERY_IDS", "")
    query_ids = [int(q.strip()) for q in raw_ids.split(",") if q.strip()]

    if not query_ids:
        log.warning("No DUNE_QUERY_IDS provided.")
        return

    for query_id in query_ids:
        log.info(f"Executing Dune query {query_id} …")
        try:
            execution_id = dune.execute_query(query_id)
        except requests.HTTPError as e:
            log.warning(f"Execution failed for query {query_id}: {e}")
            continue

        while True:
            state_name = dune.get_status(execution_id)
            log.info(f"Query {query_id} → {state_name}")
            if state_name in ("QUERY_STATE_COMPLETED", "COMPLETED"):
                break
            if state_name in ("QUERY_STATE_FAILED", "FAILED"):
                log.warning(f"Query {query_id} failed.")
                break
            time.sleep(10)

        rows = dune.get_results(execution_id)
        log.info(f"Fetched {len(rows)} rows for query {query_id}")
        for i, row in enumerate(rows, start=1):

            # The 'upsert' operation inserts the data into the destination.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into, in this case, "timestamps".
            # - The second argument is a dictionary containing the data to be upserted.
            op.upsert("dune_query_result", {
                "query_id": query_id,
                "execution_id": execution_id,
                "row_id": i,
                "data": row,
                "synced_at": now_utc_iso(),
            })

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state={"synced_at": now_utc_iso()})
    log.info("Dune sync completed")


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
