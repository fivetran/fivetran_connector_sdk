"""Dotdigital → Fivetran (Connector SDK)

A minimal, production-ready connector that syncs:
  • Contacts (v3 API)
  • Campaigns (v2 API)

Standards: PEP8, PEP257, black formatting, flake8-friendly.

Configuration keys (set via configuration.json or UI):
  DOTDIGITAL_API_USER         (str)  – required
  DOTDIGITAL_API_PASSWORD     (str)  – required
  DOTDIGITAL_REGION_ID        (str)  – optional if AUTO_DISCOVER_REGION=true, e.g. "r1"
  AUTO_DISCOVER_REGION        ("true"/"false")
  CONTACTS_PAGE_SIZE          (int)  – default 500
  V2_PAGE_SIZE                (int)  – default 1000
  CONTACTS_START_TIMESTAMP    (ISO 8601 UTC string) – default "1970-01-01T00:00:00Z"
  CAMPAIGNS_ENABLED           ("true"/"false") – default true
  LIST_ID_FILTER              (str/int) – optional, filter contacts by listId
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
import base64
import time

import requests
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

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
    """Interpret common truthy strings as booleans.

    Args:
        val: Raw string value from configuration.
        default: Value if val is None.

    Returns:
        bool: True if val in {"true","1","yes","y"} (case-insensitive), else False.
    """

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
    """Normalize timestamps to RFC3339 with timezone.

    Dotdigital sometimes returns timestamps without a timezone (e.g.,
    '2025-09-03T06:02:31.301'). The SDK requires a timezone. If no
    timezone is present, we assume UTC and append 'Z'.

    Args:
        dt_str: Timestamp from the API.

    Returns:
        A timestamp string with timezone, or None if input is falsy.
    """

    if not dt_str:
        return None
    s = str(dt_str)
    if s.endswith("Z"):
        return s
    # If there's a timezone offset (+/-) after the time part, keep as-is.
    if len(s) > 19 and ("+" in s[19:] or "-" in s[19:]):
        return s
    return s + "Z"


class RateLimitError(Exception):
    """Raised when the API responds with HTTP 429 and we should back off."""

    def __init__(self, reset_epoch: Optional[int] = None, message: str = "429 Too Many Requests"):
        super().__init__(message)
        self.reset_epoch = reset_epoch


class DotdigitalClient:
    """Lightweight HTTP client for Dotdigital APIs.

    Handles:
      • Basic auth header
      • Region autodiscovery
      • 429 backoff using X-RateLimit-Reset
      • Two pagination styles: seek (v3) and select/skip (v2)
    """

    def __init__(self, api_user: str, api_password: str, region_id: Optional[str] = None) -> None:
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
        """Discover the correct region via the r1 account-info endpoint.

        Returns:
            The detected region id (e.g., 'r2'). If discovery fails or the
            endpoint does not include an ApiEndpoint, the existing region is
            returned and a warning is logged.
        """

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
            # Some v3 endpoints accept "limit" or "select" as page size.
            params.setdefault("limit", limit)
            params.setdefault("select", limit)
        marker = params.get("marker")
        while True:
            if marker:
                params["marker"] = marker
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
# Schema
# -----------------------------

def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Define destination tables and their columns.

    Returns two tables:
      • dotdigital_contacts
      • dotdigital_campaigns
    """

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
# Update (sync)
# -----------------------------

def update(configuration: Dict[str, Any], state: Dict[str, Any]) -> None:
    """Extract from Dotdigital and upsert into destination tables.

    This function is called by the SDK for every sync. It reads configuration
    and state, pulls data from Dotdigital, writes rows via Operations, and
    checkpoints the incremental cursors.
    """

    api_user = configuration.get("DOTDIGITAL_API_USER")
    api_password = configuration.get("DOTDIGITAL_API_PASSWORD")
    region_id = configuration.get("DOTDIGITAL_REGION_ID") or DEFAULT_REGION
    autodetect = _as_bool(configuration.get("AUTO_DISCOVER_REGION"), False)

    if not api_user or not api_password:
        raise ValueError(
            "Missing DOTDIGITAL_API_USER or DOTDIGITAL_API_PASSWORD in configuration"
        )

    client = DotdigitalClient(api_user, api_password, region_id=region_id)
    if autodetect:
        try:
            client.autodiscover_region()
        except Exception as exc:  # noqa: BLE001 - keep broad to log and proceed
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
            mobile_number = identifiers.get("mobileNumber") or identifiers.get(
                "MobileNumber"
            )

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
        for item in client.get_select_skip_paged(
            "/v2/campaigns", page_size=v2_page_size
        ):
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


# Required Connector object for the SDK
connector = Connector(update=update, schema=schema)

# -----------------------------
# Notes
# -----------------------------
# • Keep requirements.txt empty – the SDK supplies fivetran_connector_sdk & requests.
# • To format/lint locally:
#     black connector.py
#     flake8 connector.py
