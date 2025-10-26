"""Example: Dotdigital Connector

This example demonstrates how to build a connector using the
Fivetran Connector SDK to extract and load data from the Dotdigital
marketing automation platform.

It syncs:
  • Contacts (v3 API, seek pagination)
  • Campaigns (v2 API, skip pagination)

The connector illustrates:
  • Region autodiscovery handling
  • Pagination with marker tokens
  • Rate-limit backoff using 429 headers
  • Incremental syncs with checkpoints
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
import base64  # For Basic Auth encoding
import time  # For rate-limit backoff
import requests  # For making HTTP requests
from fivetran_connector_sdk import Connector  # Core SDK interface
from fivetran_connector_sdk import Logging as log  # Logging abstraction
from fivetran_connector_sdk import Operations as op  # Destination operations

__DEFAULT_REGION = "r1"
__BASE_URL_FMT = "https://{region}-api.dotdigital.com"
__USER_AGENT = "fivetran-connector-sdk-dotdigital/1.0"
__DEFAULT_TIMEOUT = 120
__ACCOUNT_INFO_URL = "https://r1-api.dotdigital.com/v2/account-info"


def _as_bool(val: Optional[str], default: bool = False) -> bool:
    """Interpret common truthy strings as booleans."""
    if val is None:
        return default
    return str(val).strip().lower() in {"true", "1", "yes", "y"}


def _now_utc_iso() -> str:
    """Return current UTC timestamp as RFC3339 string with trailing 'Z'."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_utc(dt_str: Optional[str]) -> Optional[str]:
    """Normalize timestamps to RFC3339 with timezone suffix."""
    if not dt_str:
        return None
    s = str(dt_str)
    if s.endswith("Z"):
        return s
    if len(s) > 19 and ("+" in s[19:] or "-" in s[19:]):
        return s
    return s + "Z"


class RateLimitError(Exception):
    """Raised when API responds with HTTP 429 and requires backoff."""

    def __init__(self, reset_epoch: Optional[int] = None, message: str = "429 Too Many Requests"):
        super().__init__(message)
        self.reset_epoch = reset_epoch


class DotdigitalClient:
    """Lightweight HTTP client for Dotdigital APIs."""

    def __init__(self, api_user: str, api_password: str, region_id: Optional[str] = None) -> None:
        """Initialize the API client with Basic Auth and region."""
        token = base64.b64encode(f"{api_user}:{api_password}".encode("utf-8")).decode("ascii")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Basic {token}",
                "Accept": "application/json",
                "User-Agent": __USER_AGENT,
            }
        )
        self.region_id = region_id or __DEFAULT_REGION
        self.base_url = __BASE_URL_FMT.format(region=self.region_id)

    def set_region(self, region_id: str) -> None:
        """Update client to use a new region base URL."""
        self.region_id = region_id
        self.base_url = __BASE_URL_FMT.format(region=self.region_id)

    def autodiscover_region(self) -> str:
        """Discover the correct region via the r1 account-info endpoint."""
        resp = self.session.get(__ACCOUNT_INFO_URL, timeout=__DEFAULT_TIMEOUT)
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

    @staticmethod
    def _raise_for_status(resp: requests.Response) -> None:
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
        max_retries: int = 5,
    ) -> requests.Response:
        """Perform HTTP request with retry/backoff for 429 responses."""
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
                    timeout=__DEFAULT_TIMEOUT,
                )
                self._raise_for_status(resp)
                return resp
            except RateLimitError as exc:
                now_epoch = int(time.time())
                sleep_for = max(0, (exc.reset_epoch or now_epoch) - now_epoch)
                if not sleep_for:
                    sleep_for = min(60, attempt * 5)
                log.warning(
                    f"Rate limited; sleeping {sleep_for}s (attempt {attempt}/{max_retries})"
                )
                time.sleep(sleep_for)
                if attempt >= max_retries:
                    raise

    def get_seek_paged(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> Iterable[Dict[str, Any]]:
        """Generator for v3 seek pagination using marker tokens."""
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
        """Generator for v2 skip/limit pagination."""
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


def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Define destination tables and their columns."""
    return [
        {
            "table": "dotdigital_contact",
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
            "table": "dotdigital_campaign",
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


def update(configuration: Dict[str, Any], state: Dict[str, Any]) -> None:
    """Orchestrate contact and campaign syncs from Dotdigital."""
    api_user = configuration.get("DOTDIGITAL_API_USER")
    api_password = configuration.get("DOTDIGITAL_API_PASSWORD")
    region_id = configuration.get("DOTDIGITAL_REGION_ID") or __DEFAULT_REGION
    autodetect = _as_bool(configuration.get("AUTO_DISCOVER_REGION"), False)

    if not api_user or not api_password:
        raise ValueError("Missing DOTDIGITAL_API_USER or DOTDIGITAL_API_PASSWORD")

    client = DotdigitalClient(api_user, api_password, region_id=region_id)
    if autodetect:
        try:
            client.autodiscover_region()
        except (requests.RequestException, ValueError) as exc:
            log.warning(f"Region autodiscovery failed: {exc}")

    _sync_contacts(client, configuration, state)
    _sync_campaigns(client, configuration, state)
    log.info("Dotdigital sync complete.")


def _sync_contacts(
    client: DotdigitalClient, configuration: Dict[str, Any], state: Dict[str, Any]
) -> None:
    """Extract and upsert contact records (v3 API)."""
    contacts_page_size = int(configuration.get("CONTACTS_PAGE_SIZE", 500))
    contacts_cursor = (
        state.get("contacts_cursor")
        or configuration.get("CONTACTS_START_TIMESTAMP")
        or "1970-01-01T00:00:00Z"
    )
    list_id_filter = configuration.get("LIST_ID_FILTER")
    log.info("Syncing contacts (v3)...")
    max_seen_updated = contacts_cursor

    params: Dict[str, Any] = {"sort": "asc"}
    if contacts_cursor:
        params["~modified"] = f"gte::{contacts_cursor}"
    if list_id_filter:
        params["~listId"] = str(list_id_filter)

    for item in client.get_seek_paged("/contacts/v3", params=params, limit=contacts_page_size):
        identifiers = item.get("identifiers", {})
        email = (
            identifiers.get("email") or identifiers.get("Email")
            if isinstance(identifiers, dict)
            else None
        )
        mobile_number = (
            identifiers.get("mobileNumber") or identifiers.get("MobileNumber")
            if isinstance(identifiers, dict)
            else None
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
        op.upsert(table="dotdigital_contact", data=row)

        if updated and (max_seen_updated is None or updated > max_seen_updated):
            max_seen_updated = updated

    op.checkpoint(state={"contacts_cursor": max_seen_updated or contacts_cursor})


def _sync_campaigns(
    client: DotdigitalClient, configuration: Dict[str, Any], state: Dict[str, Any]
) -> None:
    """Extract and upsert campaign records (v2 API)."""
    if not _as_bool(configuration.get("CAMPAIGNS_ENABLED"), True):
        return
    log.info("Syncing campaigns (v2)...")
    v2_page_size = int(configuration.get("V2_PAGE_SIZE", 1000))
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
            op.upsert(table="dotdigital_campaign", data=row)
    op.checkpoint(state={"campaigns_synced_at": _now_utc_iso()})


connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":

    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug(configuration=configuration)
