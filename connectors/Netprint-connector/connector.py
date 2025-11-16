"""
NetPrint → Fivetran Connector SDK (v2)
- schema(configuration): defines tables & PKs
- update(configuration, state): performs sync using Operations API
Features:
  * Full + incremental sync for `files`
  * Soft deletes via _fivetran_deleted
  * Paging and 429 backoff
"""

import base64
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Set

import requests
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


# ---------- helpers ----------

def _parse_dt(val: Optional[str]) -> datetime:
    """Best-effort ISO parser; defaults to epoch UTC if missing/unrecognized."""
    if not val:
        return datetime.min.replace(tzinfo=timezone.utc)
    val = val.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(val)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z"):
            try:
                return datetime.strptime(val, fmt)
            except Exception:
                continue
        log.warning(f"Unrecognized datetime format: {val}; defaulting to epoch")
        return datetime.min.replace(tzinfo=timezone.utc)


class NetPrintAPI:
    """
    Minimal API client for the NetPrint webservice (read-only endpoints).
    Auth: X-NPS-Authorization = base64(username%password%4)
    """
    def __init__(self, username: str, password: str, base_url: str = "https://api-s.printing.ne.jp/usr/webservice/api/"):
        self.base_url = base_url.rstrip("/") + "/"
        auth_string = f"{username}%{password}%4"
        token = base64.b64encode(auth_string.encode("utf-8")).decode("utf-8")
        self.headers = {"X-NPS-Authorization": token, "Content-Type": "application/json"}

    def _request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET",
        data: Optional[Dict[str, Any]] = None,
        retry: int = 1,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        try:
            resp = requests.request(method, url, headers=self.headers, params=params, json=data, timeout=30)
        except requests.RequestException as e:
            log.error(f"Connection error calling {endpoint}: {e}")
            raise

        # Simple 429 handling: honor Retry-After once
        if resp.status_code == 429 and retry > 0:
            wait_s = int(resp.headers.get("Retry-After", "60"))
            log.info(f"429 from {endpoint}. Sleeping {wait_s}s and retrying once.")
            time.sleep(wait_s)
            return self._request(endpoint, params=params, method=method, data=data, retry=retry - 1)

        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError:
                log.error(f"Invalid JSON from {endpoint}")
                return {}
        if resp.status_code in (401, 403):
            raise PermissionError(f"Auth failed for {endpoint} ({resp.status_code})")
        if resp.status_code == 404:
            log.warning(f"Not found: {endpoint}")
            return {}

        raise RuntimeError(f"HTTP {resp.status_code} at {endpoint}: {resp.text}")

    # ---- read endpoints ----
    def get_information(self) -> Dict[str, Any]:
        return self._request("core/information")

    def get_folder_size(self) -> Dict[str, Any]:
        return self._request("core/folderSize")

    def iter_files(self, show_count: int = 200) -> Iterable[Dict[str, Any]]:
        """
        Page through files using fromCount/showCount.
        Returns dicts from "fileList".
        """
        from_count = 0
        while True:
            payload = self._request("core/file", params={"fromCount": from_count, "showCount": show_count})
            items = payload.get("fileList", []) or []
            if not items:
                break
            for f in items:
                yield f
            if len(items) < show_count:
                break
            from_count += show_count


# ---------- SDK entry points ----------

def schema(configuration: dict):
    """
    Define destination tables & primary keys.
    (Columns are inferred unless you explicitly model them elsewhere.)
    """
    return [
        {"table": "system_info", "primary_key": []},
        {"table": "folder_usage", "primary_key": []},
        {"table": "files", "primary_key": ["accessKey"]},
    ]


def update(configuration: dict, state: dict):
    """
    Full + incremental sync with soft deletes for `files`.

    State layout:
      state["files"] = {
        "last_synced_at": ISO8601 string,
        "known_keys": ["AK1", "AK2", ...]
      }
    """
    # --- configuration (strings only per SDK) ---
    username = configuration["username"]
    password = configuration["password"]
    # Optional tuning knobs (strings in configuration.json)
    page_size = int(configuration.get("PAGE_SIZE", "200"))
    base_url = configuration.get("BASE_URL", "https://api-s.printing.ne.jp/usr/webservice/api/")

    api = NetPrintAPI(username, password, base_url=base_url)

    # --- small, full-refresh tables ---
    info = api.get_information() or {}
    if info:
        info["_fivetran_deleted"] = False
        op.upsert("system_info", info)

    usage = api.get_folder_size() or {}
    if usage:
        usage["_fivetran_deleted"] = False
        op.upsert("folder_usage", usage)

    # --- files: incremental upserts + soft deletes ---
    files_state: Dict[str, Any] = state.get("files") or {}
    bookmark_str = files_state.get("last_synced_at", "")
    bookmark = _parse_dt(bookmark_str) if bookmark_str else datetime.min.replace(tzinfo=timezone.utc)
    prev_keys: Set[str] = set(files_state.get("known_keys") or [])

    max_seen = bookmark
    current_keys: Set[str] = set()

    for f in api.iter_files(show_count=page_size):
        ak = f.get("accessKey")
        if not ak:
            continue
        current_keys.add(ak)

        ts_raw = f.get("uploadDate") or f.get("registrationDate") or ""
        ts = _parse_dt(ts_raw)
        if ts > max_seen:
            max_seen = ts

        # Upsert active row
        f["_fivetran_deleted"] = False
        op.upsert("files", f)

    # Soft deletes for keys that disappeared since last run
    missing = prev_keys - current_keys
    for ak in missing:
        op.upsert("files", {"accessKey": ak, "_fivetran_deleted": True})

    # Persist state (checkpoint)
    state["files"] = {
        "last_synced_at": max_seen.astimezone(timezone.utc).isoformat(),
        "known_keys": sorted(list(current_keys)),
    }
    op.checkpoint(state)

    log.info(f"files synced: {len(current_keys)}, soft-deleted: {len(missing)}")


# The object the SDK loader requires:
connector = Connector(update=update, schema=schema)
