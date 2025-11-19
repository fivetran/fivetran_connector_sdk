"""
This connector demonstrates how to fetch data from the NetPrint Web API and
upsert it into a Fivetran destination using the Connector SDK.

See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

# For reading configuration from a JSON file
import json
# For Base64 encoding of authentication credentials
import base64
# For implementing retry delays and rate limit handling
import time
# For parsing and manipulating datetime values
from datetime import datetime, timezone
# For type hints to improve code clarity
from typing import Any, Dict, Iterable, Optional, Set

# For making HTTP requests to the NetPrint API (provided by Fivetran runtime)
import requests
# For connector initialization
from fivetran_connector_sdk import Connector
# For enabling logs in your connector code
from fivetran_connector_sdk import Logging as log
# For supporting data operations like upsert and checkpoint
from fivetran_connector_sdk import Operations as op

__MAX_RETRIES = 3  # Number of retries for transient errors


def _parse_dt(val: Optional[str]) -> datetime:
    """
    Best-effort ISO parser; defaults to epoch UTC if missing/unrecognized.
    """
    if not val:
        return datetime.min.replace(tzinfo=timezone.utc)
    val = val.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(val)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z"):
            try:
                return datetime.strptime(val, fmt)
            except (ValueError, TypeError):
                continue
        log.warning(f"Unrecognized datetime format: {val}; defaulting to epoch")
        return datetime.min.replace(tzinfo=timezone.utc)


class NetPrintAPI:
    """
    Minimal API client for the NetPrint webservice (read-only endpoints).

    Auth header:
        X-NPS-Authorization = base64(username%password%4)
    """

    def __init__(
        self,
        username: str,
        password: str,
        base_url: str = "https://api-s.printing.ne.jp/usr/webservice/api/",
    ):
        self.base_url = base_url.rstrip("/") + "/"
        auth_string = f"{username}%{password}%4"
        token = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")
        self.headers = {
            "X-NPS-Authorization": token,
            "Content-Type": "application/json",
        }

    def _request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET",
        data: Optional[Dict[str, Any]] = None,
        retry: int = 1,
    ) -> Dict[str, Any]:
        """
        Single HTTP request with basic 429 handling.
        Intended to be wrapped by higher-level retry logic.
        """
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.request(
                method,
                url,
                headers=self.headers,
                params=params,
                json=data,
                timeout=30,
            )
        except requests.RequestException as e:
            log.severe(f"Connection error calling {endpoint}: {e}")
            raise

        # Simple 429 handling: honor Retry-After once
        if response.status_code == 429 and retry > 0:
            wait_s = int(response.headers.get("Retry-After", "60"))
            log.info(f"429 from {endpoint}. Sleeping {wait_s}s and retrying once.")
            time.sleep(wait_s)
            return self._request(
                endpoint, params=params, method=method, data=data, retry=retry - 1
            )

        if response.status_code == 200:
            try:
                return response.json()
            except ValueError:
                log.severe(f"Invalid JSON from {endpoint}")
                return {}

        if response.status_code in (401, 403):
            raise PermissionError(f"Auth failed for {endpoint} ({response.status_code})")

        if response.status_code == 404:
            log.warning(f"Not found: {endpoint}")
            return {}

        if 500 <= response.status_code < 600:
            # Surface 5xx to outer retry loop
            raise requests.RequestException(
                f"Server error {response.status_code} at {endpoint}"
            )

        raise RuntimeError(
            f"HTTP {response.status_code} at {endpoint}: {response.text}"
        )

    def _request_with_retry(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET",
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Wrapper adding simple retry with backoff for transient network/server errors.
        """
        for attempt in range(1, __MAX_RETRIES + 1):
            try:
                return self._request(
                    endpoint,
                    params=params,
                    method=method,
                    data=data,
                    retry=1,
                )
            except requests.RequestException as exc:
                if attempt == __MAX_RETRIES:
                    log.severe(
                        f"Failed calling {endpoint} after {__MAX_RETRIES} attempts: {exc}"
                    )
                    raise
                sleep_time = min(60, 2**attempt)
                log.warning(
                    f"Transient error calling {endpoint}. "
                    f"Retry {attempt}/{__MAX_RETRIES} after {sleep_time}s."
                )
                time.sleep(sleep_time)

        # Should never reach here, but keep mypy/linters happy.
        return {}

    def get_information(self) -> Dict[str, Any]:
        """
        Fetch system/account information from core/information.
        """
        return self._request_with_retry("core/information")

    def get_folder_size(self) -> Dict[str, Any]:
        """
        Fetch folder usage from core/folderSize.
        """
        return self._request_with_retry("core/folderSize")

    def iter_files(self, show_count: int = 200) -> Iterable[Dict[str, Any]]:
        """
        Page through files using fromCount/showCount and yield file records
        from "fileList".
        """
        from_count = 0
        while True:
            payload = self._request_with_retry(
                "core/file",
                params={"fromCount": from_count, "showCount": show_count},
            )
            items = payload.get("fileList", []) or []
            if not items:
                break

            for file_record in items:
                yield file_record

            if len(items) < show_count:
                break
            from_count += show_count


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


def validate_configuration(configuration: dict) -> None:
    """
    Validate the configuration dictionary to ensure it contains all required parameters.

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.

    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["username", "password"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def _sync_small_tables(api: NetPrintAPI) -> None:
    """
    Sync small, full-refresh tables: system_info and folder_usage.
    """
    system_info = api.get_information() or {}
    if system_info:
        system_info["_fivetran_deleted"] = False
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="system_info", data=system_info)

    folder_usage = api.get_folder_size() or {}
    if folder_usage:
        folder_usage["_fivetran_deleted"] = False
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="folder_usage", data=folder_usage)


def _sync_files(
    api: NetPrintAPI,
    state: dict,
    page_size: int,
) -> None:
    """
    Full + incremental upserts with soft deletes for the `files` table.
    """
    files_state: Dict[str, Any] = state.get("files") or {}
    bookmark_str = files_state.get("last_synced_at", "")
    bookmark = (
        _parse_dt(bookmark_str)
        if bookmark_str
        else datetime.min.replace(tzinfo=timezone.utc)
    )
    previous_keys: Set[str] = set(files_state.get("known_keys") or [])

    max_seen_timestamp = bookmark
    current_keys: Set[str] = set()
    processed_count = 0

    for file_record in api.iter_files(show_count=page_size):
        access_key = file_record.get("accessKey")
        if not access_key:
            continue

        current_keys.add(access_key)

        timestamp_raw = file_record.get("uploadDate") or file_record.get(
            "registrationDate"
        ) or ""
        timestamp = _parse_dt(timestamp_raw)
        if timestamp > max_seen_timestamp:
            max_seen_timestamp = timestamp

        file_record["_fivetran_deleted"] = False
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="files", data=file_record)

        processed_count += 1
        # For large datasets, checkpoint periodically to avoid losing progress.
        if processed_count % 1000 == 0:
            state["files"] = {
                "last_synced_at": max_seen_timestamp.astimezone(
                    timezone.utc
                ).isoformat(),
                # Note: For very large NetPrint accounts, tracking all keys has
                # memory implications, but is kept for clarity in this example.
                "known_keys": sorted(current_keys),
            }
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)

    # Soft deletes for keys that disappeared since last run
    missing_keys = previous_keys - current_keys
    for access_key in missing_keys:
        deleted_row = {"accessKey": access_key, "_fivetran_deleted": True}
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="files", data=deleted_row)

    state["files"] = {
        "last_synced_at": max_seen_timestamp.astimezone(timezone.utc).isoformat(),
        "known_keys": sorted(current_keys),
    }
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    op.checkpoint(state)

    log.info(
        f"files synced: {len(current_keys)}, soft-deleted: {len(missing_keys)}"
    )


def update(configuration: dict, state: dict):
    """
    Define the update function which lets you configure how your connector fetches data.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: a dictionary that holds the state of the connector.
    """
    validate_configuration(configuration)
    log.warning("Example: Connector : NetPrint")

    # --- configuration (strings only per SDK) ---
    username = configuration["username"]
    password = configuration["password"]
    # Optional tuning knobs (strings in configuration.json)
    page_size = int(configuration.get("PAGE_SIZE", "200"))
    base_url = configuration.get(
        "BASE_URL", "https://api-s.printing.ne.jp/usr/webservice/api/"
    )

    api = NetPrintAPI(username, password, base_url=base_url)

    # Sync small, full-refresh tables
    _sync_small_tables(api)

    # Sync files with incremental logic and soft deletes
    _sync_files(api, state, page_size)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r", encoding="utf-8") as config_file:
        configuration = json.load(config_file)

    # Test the connector locally
    connector.debug()
