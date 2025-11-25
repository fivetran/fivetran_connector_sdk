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

from typing import Optional, Dict, Any, Iterable, Set # for type hints

__DEFAULT_PAGE_SIZE = 200
__DEFAULT_BASE_URL = "https://api-s.printing.ne.jp/usr/webservice/api/"
_MAX_RETRIES = 3


def __parse_date(val: Optional[str]) -> datetime:
    if not val:
        return datetime.min.replace(tzinfo=timezone.utc)
    val = val.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(val)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


class NetPrintAPI:
    """Minimal API wrapper for NetPrint WebService."""

    def __init__(self, username: str, password: str, base_url: str = __DEFAULT_BASE_URL):
        self.base_url = base_url.rstrip("/") + "/"
        auth_string = f"{username}%{password}%4"
        encoded = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")
        self.headers = {
            "X-NPS-Authorization": encoded,
            "Content-Type": "application/json",
        }

    def _request(self, endpoint: str, params: Dict = None, retry: int = 1) -> Dict[str, Any]:
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
            return self._request(endpoint, params, retry=0)

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
        return self._request("core/information")

    def get_folder_size(self):
        return self._request("core/folderSize")

    def iter_files(self, page_size: int = __DEFAULT_PAGE_SIZE) -> Iterable[Dict[str, Any]]:
        from_count = 0
        while True:
            res = self._request(
                "core/file", params={"fromCount": from_count, "showCount": page_size}
            )
            files = res.get("fileList", []) or []
            if not files:
                break
            yield from files
            if len(files) < page_size:
                break
            from_count += page_size


def validate_configuration(configuration: dict):
    required = ["username", "password"]
    for field in required:
        if field not in configuration:
            raise ValueError(f"Missing required config: {field}")


def schema(_: dict):
    return [
        {"table": "system_info", "primary_key": []},
        {"table": "folder_usage", "primary_key": []},
        {"table": "files", "primary_key": ["accessKey"]},
    ]


def _sync_static_tables(api: NetPrintAPI):
    for endpoint, table in [("information", "system_info"), ("folderSize", "folder_usage")]:
        data = getattr(api, f"get_{endpoint}")() or {}
        if data:
            data["_fivetran_deleted"] = False
            op.upsert(table, data)


def _sync_files(api: NetPrintAPI, state: dict, page_size: int):
    file_state = state.get("files", {})
    bookmark = __parse_date(file_state.get("last_synced_at"))
    previous_keys = set(file_state.get("known_keys", []))

    seen_keys: Set[str] = set()
    max_timestamp = bookmark

    for record in api.iter_files(page_size):
        access_key = record.get("accessKey")
        if not access_key:
            continue
        seen_keys.add(access_key)

        ts = __parse_date(record.get("uploadDate") or record.get("registrationDate"))
        if ts > max_timestamp:
            max_timestamp = ts

        record["_fivetran_deleted"] = False
        op.upsert("files", record)

    for missing in previous_keys - seen_keys:
        op.upsert("files", {"accessKey": missing, "_fivetran_deleted": True})

    state["files"] = {
        "last_synced_at": max_timestamp.astimezone(timezone.utc).isoformat(),
        "known_keys": sorted(seen_keys),
    }
    op.checkpoint(state)


def update(configuration: dict, state: dict):
    validate_configuration(configuration)
    username = configuration["username"]
    password = configuration["password"]
    page_size = int(configuration.get("PAGE_SIZE", __DEFAULT_PAGE_SIZE))
    base_url = configuration.get("BASE_URL", __DEFAULT_BASE_URL)

    api = NetPrintAPI(username, password, base_url)
    _sync_static_tables(api)
    _sync_files(api, state, page_size)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        config = json.load(f)
    connector.debug(config)
