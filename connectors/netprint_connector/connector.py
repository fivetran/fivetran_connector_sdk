"""
This connector demonstrates how to fetch data from the NetPrint Web API and
upsert it into a Fivetran destination using the Connector SDK.

See Technical Reference:
https://fivetran.com/docs/connectors/connector-sdk/technical-reference
"""

# For reading configuration from a JSON file
import json

# For encoding authentication credentials (e.g., username%password%4)
import base64

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For making HTTP requests to Dgraph GraphQL API (provided by SDK runtime)
import requests

# For handling datetime parsing and formatting
from datetime import datetime, timezone

# For handling exponential backoff in retries
import time

from typing import Optional, Dict, Any, Iterable, Set  # for type hints

__DEFAULT_PAGE_SIZE = 200
__DEFAULT_BASE_URL = "https://api-s.printing.ne.jp/usr/webservice/api/"
__CHECKPOINT_INTERVAL = 1000


def parse_date(val: Optional[str]) -> datetime:
    """
    Parse ISO 8601 date string to datetime with UTC timezone.
    Args:
        - val: ISO 8601 date string
    Returns:
        - datetime object in UTC timezone, or datetime.min if parsing fails
    """
    if not val:
        return datetime.min.replace(tzinfo=timezone.utc)
    val = val.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(val)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError) as e:
        log.warning(f"Failed to parse date '{val}': {e}")
        return datetime.min.replace(tzinfo=timezone.utc)


class NetPrintAPI:
    """Minimal API wrapper for NetPrint WebService."""

    def __init__(self, username: str, password: str, base_url: str = __DEFAULT_BASE_URL):
        """
        Initialize the extractor with compiled regex patterns.
        This includes patterns for invoice ID, dates, amounts, contact fields, and financial totals.
        The patterns are designed to match common invoice formats and can be extended as needed.
        """
        self.base_url = base_url.rstrip("/") + "/"
        auth_string = f"{username}%{password}%4"
        encoded = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")
        self.headers = {
            "X-NPS-Authorization": encoded,
            "Content-Type": "application/json",
        }

    def request(self, endpoint: str, params: Dict = None, retry: int = 1) -> Dict[str, Any]:
        """
        Make a GET request to the NetPrint API.
        Handles rate limiting and common HTTP errors.
        Args:
            - endpoint: API endpoint to call
            - params: Query parameters for the request
            - retry: Number of retries for rate limiting
        Returns:
            - Parsed JSON response as a dictionary
        """
        try:
            res = requests.get(
                f"{self.base_url}{endpoint}",
                headers=self.headers,
                params=params,
                timeout=30,
            )
        except requests.RequestException as e:
            log.severe(f"Connection error calling {endpoint}: {e}")
            raise

        if res.status_code == 429 and retry:
            wait = int(res.headers.get("Retry-After", 60))
            log.info(f"429 received, retrying in {wait}s")
            time.sleep(wait)
            return self.request(endpoint, params, retry=0)

        if res.status_code == 200:
            try:
                return res.json()
            except ValueError:
                log.severe(f"Invalid JSON from {endpoint}")
                return {}

        if res.status_code in (401, 403):
            raise PermissionError(f"Auth failed for {endpoint} ({res.status_code})")

        if res.status_code == 404:
            log.warning(f"{endpoint} not found")
            return {}

        if 500 <= res.status_code < 600:
            raise requests.RequestException(f"Server error {res.status_code} at {endpoint}")

        raise RuntimeError(f"HTTP {res.status_code}: {res.text}")

    def get_information(self):
        """
        Fetch system information from the NetPrint API.
        Returns:
            - Dictionary containing system information
        """
        return self.request("core/information")

    def get_folder_size(self):
        """
        Fetch folder size information from the NetPrint API.
        Returns:
            - Dictionary containing folder size information
        """
        return self.request("core/folderSize")

    def iter_files(self, page_size: int = __DEFAULT_PAGE_SIZE) -> Iterable[Dict[str, Any]]:
        """
        Iterate over files in the NetPrint account using pagination.
        Args:
            - page_size: Number of files to fetch per request
        Returns:
            - Dictionary for each file record
        """
        all_files: List[Dict[str, Any]] = []
        from_count = 0

        while True:
            res = self.request(
            "core/file",
            params={"fromCount": from_count, "showCount": page_size},
            )
            files = res.get("fileList", []) or []
            if not files:
                break

            all_files.extend(files)

            # If we got less than a full page, we've hit the end
            if len(files) < page_size:
                break

            from_count += page_size

        return all_files


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required = ["username", "password"]
    for field in required:
        if field not in configuration:
            raise ValueError(f"Missing required config: {field}")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "system_info", "primary_key": []},
        {"table": "folder_usage", "primary_key": []},
        {"table": "files", "primary_key": ["accessKey"]},
    ]


def sync_static_tables(api: NetPrintAPI):
    """
    Sync static tables like system_info and folder_usage. These tables are expected to have a single record each.
    Args:
        - api: Instance of NetPrintAPI to fetch data from
    """
    for endpoint, table in [("information", "system_info"), ("folderSize", "folder_usage")]:
        data = getattr(api, f"get_{endpoint}")() or {}
        if data:
            data["_fivetran_deleted"] = False
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted.
            op.upsert(table, data)


def sync_files(api: NetPrintAPI, state: dict, page_size: int):
    """
    Sync the files table with incremental updates based on the last synced timestamp.
    Args:
        - api: Instance of NetPrintAPI to fetch data from
        - state: Dictionary to maintain state between syncs
        - page_size: Number of records to fetch per API call
    """
    file_state = state.get("files", {})
    bookmark = parse_date(file_state.get("last_synced_at"))
    if bookmark is None:
        # fallback if state is empty or invalid
        bookmark = parse_date("1970-01-01T00:00:00Z")

    # Keys we knew about from previous sync
    previous_keys: Set[str] = set(file_state.get("known_keys", []))

    # Keys we see in this sync (current "truth")
    new_known_keys: Set[str] = set()

    max_timestamp = bookmark
    record_count = 0

    for record in api.iter_files(page_size):
        access_key = record.get("accessKey")
        if not access_key:
            continue

        new_known_keys.add(access_key)
        # If this key was known previously, it's definitely not deleted now
        if access_key in previous_keys:
            previous_keys.discard(access_key)

        ts = parse_date(record.get("uploadDate") or record.get("registrationDate"))
        if ts and ts > max_timestamp:
            max_timestamp = ts

        record["_fivetran_deleted"] = False
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into.
        # - The second argument is a dictionary containing the data to be upserted.
        op.upsert("files", record)
        record_count += 1

        # Periodic checkpoint to avoid losing all progress on large datasets
        if record_count % __CHECKPOINT_INTERVAL == 0:
            state["files"] = {
                "last_synced_at": max_timestamp.astimezone(timezone.utc).isoformat(),
                "known_keys": sorted(new_known_keys),
            }

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)
            log.info(f"Checkpointed files after {record_count} records")

    # Any keys that were in previous_keys but not seen this run are now deleted
    for missing in previous_keys:
        op.upsert("files", {"accessKey": missing, "_fivetran_deleted": True})

    # Final state after full iteration
    state["files"] = {
        "last_synced_at": max_timestamp.astimezone(timezone.utc).isoformat(),
        "known_keys": sorted(new_known_keys),
    }

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)


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
    validate_configuration(configuration)
    username = configuration["username"]
    password = configuration["password"]
    page_size =  __DEFAULT_PAGE_SIZE
    base_url = configuration.get("BASE_URL", __DEFAULT_BASE_URL)

    api = NetPrintAPI(username, password, base_url)
    sync_static_tables(api)
    sync_files(api, state, page_size)


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
