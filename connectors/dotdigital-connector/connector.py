"""Example: Dotdigital Connector

This example demonstrates how to build a Fivetran connector using the
Connector SDK to extract and load data from the Dotdigital marketing
automation platform.

It syncs:
  • Contacts (v3 API, seek pagination)
  • Campaigns (v2 API, skip pagination)

The connector shows:
  • Region autodiscovery handling
  • Pagination with marker tokens
  • Basic rate limit backoff
  • Incremental sync with checkpoints
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
import base64  # For basic auth encoding
import time  # For rate-limit backoff handling
import requests  # For HTTP API requests
from fivetran_connector_sdk import Connector  # Core SDK object
from fivetran_connector_sdk import Logging as log  # Logging abstraction
from fivetran_connector_sdk import Operations as op  # DB write operations

# -----------------------------
# Constants
# -----------------------------
DEFAULT_REGION = "r1"
BASE_URL_FMT = "https://{region}-api.dotdigital.com"
USER_AGENT = "fivetran-connector-sdk-dotdigital/1.0"
DEFAULT_TIMEOUT = 120
DEFAULT_MAX_RETRIES = 5


# -----------------------------
# Helpers
# -----------------------------
def _as_bool(val: Optional[str], default: bool = False) -> bool:
    """Interpret common truthy strings as booleans."""
    if val is None:
        return default
    return str(val).strip().lower() in {"true", "1", "yes", "y"}


def _now_utc_iso() -> str:
    """Return current UTC timestamp as RFC3339 string with trailing 'Z'."""
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_utc(dt_str: Optional[str]) -> Optional[str]:
    """Normalize timestamps to RFC3339 with timezone."""
    if not dt_str:
        return None
    s = str(dt_str)
    if s.endswith("Z"):
        return s
    if len(s) > 19 and ("+" in s[19:] or "-" in s[19:]):
        return s
    return s + "Z"


# -----------------------------
# Errors
# -----------------------------
class RateLimitError(Exception):
    """Raised when the API responds with HTTP 429 and we should back off."""

    def __init__(self, reset_epoch: Optional[int] = None, message: str = "429 Too Many Requests"):
        super().__init__(message)
        self.reset_epoch = reset_epoch


# -----------------------------
# Dotdigital Client
# -----------------------------
class DotdigitalClient:
    """Lightweight HTTP client for Dotdigital APIs."""

    def __init__(self, api_user: str, api_password: str, region_id: Optional[str] = None) -> None:
        """Initialize a new Dotdigital client with authentication and region."""
        token = base64.b64encode(f"{api_user}:{api_password}".encode("utf-8")).decode("ascii")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Basic {token}",
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
            }
        )
        self.region_id = region_id or DEFAULT_REGION
        self.base_url = BASE_URL_FMT.format(region=self.region_id)

    def set_region(self, region_id: str) -> None:
        """Update client to use a new region base URL."""
        self.region_id = region_id
        self.base_url = BASE_URL_FMT.format(region=self.region_id)

    def autodiscover_region(self) -> str:
        """Discover the correct region via the r1 account-info endpoint."""
        url = "https://r1-api.dotdigital.com/v2/account-info"
        resp = self.session.get(url, timeout=DEFAULT_TIMEOUT)
        self._raise_for_status(resp)
        data = resp.json()
        endpoint = data.get("ApiEndpoint") or data.get("apiEndpoint")
        if not endpoint:
            log.warning(
                "Auto-discovery did not return ApiEndpoint; falling back to configured region"
            )
            return self.region_id
        region_id = endpoint.split("//")[-1].split("-")[0]
        self.set_region(region_id)
        log.info(f"Detected Dotdigital region: {region_id}")
        return region_id

    def _raise_for_status(self, resp: requests.Response) -> None:
        """Translate HTTP errors into exceptions with 429 special-casing."""
        if resp.status_code == 429:
            reset_header = resp.headers.get("X-RateLimit-Reset") or resp.headers.get(
                "x-ratelimit-reset"
            )
            reset_epoch = int(reset_header) if reset_header and reset_header.isdigit() else None
            raise RateLimitError(reset_epoch=reset_epoch)
        resp.raise_for_status()

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> requests.Response:
        """Perform an HTTP request with retry/backoff for 429 responses."""
        url = f"{self.base_url}{path}"
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = self.session.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    timeout=DEFAULT_TIMEOUT,
                )
                self._raise_for_status(resp)
                return resp
            except RateLimitError as exc:
                if exc.reset_epoch:
                    now_epoch = int(time.time())
                    sleep_for = max(0, exc.reset_epoch - now_epoch)
                else:
                    sleep_for = min(60, attempt * 5)
                log.warning(
                    f"Rate limited; sleeping {sleep_for}s (attempt {attempt}/{max_retries})"
                )
                time.sleep(sleep_for)
                if attempt >= max_retries:
                    raise

    def get_seek_paged(
        self, path: str, params: Optional[Dict[str, Any]] = None, limit: Optional[int] = None
    ) -> Iterable[Dict[str, Any]]:
        """Generator for v3 seek-pagination using marker links."""
        params = dict(params or {})
        if limit:
            params.setdefault("limit", limit)
            params.setdefault("select", limit)

        marker = params.get("marker")
        while True:
            if marker is not None:
                params["marker"] = marker
            elif "marker" in params:
                del params["marker"]

            resp = self._request("GET", path, params=params)
            payload: Dict[str, Any] = resp.json()
            items = payload.get("_items") or payload.get("items") or []
            for item in items:
                yield item

            links = payload.get("_links") or {}
            next_link = (links.get("next") or {}).get("marker")
            if not next_link:
                break
            marker = next_link

    def get_select_skip_paged(
        self, path: str, page_size: int = 1000, params: Optional[Dict[str, Any]] = None
    ) -> Iterable[Dict[str, Any]]:
        """Generator for v2 select/skip pagination."""
        params = dict(params or {})
        skip = 0
        while True:
            params.update({"select": page_size, "skip": skip})
            resp = self._request("GET", path, params=params)
            data = resp.json()
            items = data if isinstance(data, list) else data.get("items", [])
            if not items:
                break
            for item in items:
                yield item
            if len(items) < page_size:
                break
            skip += page_size


# -----------------------------
# Schema Definition
# -----------------------------
def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Define destination tables and their columns."""
    return [
        {
            "table": "dotdigital_contacts",
            "primary_key": ["contact_id"],
            "columns": {
                "contact_id": "LONG",
                "status": "STRING",
                "created": "UTC_DATETIME",
                "updated": "UTC_DATETIME",
                "email": "STRING",
                "mobile_number": "STRING",
                "identifiers": "JSON",
                "data_fields": "JSON",
                "channel_properties": "JSON",
                "lists": "JSON",
                "preferences": "JSON",
                "consent_records": "JSON",
            },
        },
        {
            "table": "dotdigital_campaigns",
            "primary_key": ["campaign_id"],
            "columns": {
                "campaign_id": "LONG",
                "name": "STRING",
                "subject": "STRING",
                "date_created": "UTC_DATETIME",
                "date_modified": "UTC_DATETIME",
                "status": "STRING",
                "raw": "JSON",
            },
        },
    ]


# -----------------------------
# Update (Sync Logic)
# -----------------------------
def update(configuration: Dict[str, Any], state: Dict[str, Any]) -> None:
    """Extract data from Dotdigital and upsert into destination tables."""
    api_user = configuration.get("DOTDIGITAL_API_USER")
    api_password = configuration.get("DOTDIGITAL_API_PASSWORD")
    region_id = configuration.get("DOTDIGITAL_REGION_ID") or DEFAULT_REGION
    autodetect = _as_bool(configuration.get("AUTO_DISCOVER_REGION"), False)

    if not api_user or not api_password:
        raise ValueError("Missing DOTDIGITAL_API_USER or DOTDIGITAL_API_PASSWORD in configuration")

    client = DotdigitalClient(api_user, api_password, region_id=region_id)
    if autodetect:
        try:
            client.autodiscover_region()
        except (requests.RequestException, ValueError) as exc:
            log.warning(
                f"Region autodiscovery failed: {exc}. Continuing with region {client.region_id}"
            )

    contacts_page_size = int(configuration.get("CONTACTS_PAGE_SIZE", 500))
    v2_page_size = int(configuration.get("V2_PAGE_SIZE", 1000))

    contacts_cursor = (
        state.get("contacts_cursor")
        or configuration.get("CONTACTS_START_TIMESTAMP")
        or "1970-01-01T00:00:00Z"
    )
    list_id_filter = configuration.get("LIST_ID_FILTER")

    # 1) Contacts (v3)
    log.info("Syncing contacts (v3)...")
    max_seen_updated = contacts_cursor

    params: Dict[str, Any] = {"sort": "asc"}
    if contacts_cursor:
        params["~modified"] = f"gte::{contacts_cursor}"
    if list_id_filter:
        params["~listId"] = str(list_id_filter)

    for item in client.get_seek_paged("/contacts/v3", params=params, limit=contacts_page_size):
        identifiers = item.get("identifiers", {})
        email = None
        mobile_number = None
        if isinstance(identifiers, dict):
            email = identifiers.get("email") or identifiers.get("Email")
            mobile_number = identifiers.get("mobileNumber") or identifiers.get("MobileNumber")

        created = _normalize_utc(item.get("created") or item.get("dateCreated"))
        updated = _normalize_utc(item.get("updated") or item.get("dateUpdated")) or created

        row = {
            "contact_id": item.get("contactId"),
            "status": item.get("status"),
            "created": created,
            "updated": updated,
            "email": email,
            "mobile_number": mobile_number,
            "identifiers": identifiers,
            "data_fields": item.get("dataFields"),
            "channel_properties": item.get("channelProperties"),
            "lists": item.get("lists"),
            "preferences": item.get("preferences"),
            "consent_records": item.get("consentRecords"),
        }
        op.upsert(table="dotdigital_contacts", data=row)

        if updated and (max_seen_updated is None or updated > max_seen_updated):
            max_seen_updated = updated

    op.checkpoint(state={"contacts_cursor": max_seen_updated or contacts_cursor})

    # 2) Campaigns (v2)
    if _as_bool(configuration.get("CAMPAIGNS_ENABLED"), True):
        log.info("Syncing campaigns (v2)...")
        for item in client.get_select_skip_paged("/v2/campaigns", page_size=v2_page_size):
            row = {
                "campaign_id": item.get("id"),
                "name": item.get("name"),
                "subject": item.get("subject"),
                "date_created": _normalize_utc(item.get("dateCreated")),
                "date_modified": _normalize_utc(item.get("dateModified")),
                "status": item.get("status"),
                "raw": item,
            }
            if row["campaign_id"] is not None:
                op.upsert(table="dotdigital_campaigns", data=row)
        op.checkpoint(state={"campaigns_synced_at": _now_utc_iso()})

    log.info("Dotdigital sync complete.")


# -----------------------------
# Connector Entry Point
# -----------------------------
connector = Connector(update=update, schema=schema)
