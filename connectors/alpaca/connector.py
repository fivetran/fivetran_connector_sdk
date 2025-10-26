"""
Fivetran Connector SDK — Alpaca Trading (Paper or Live)
- Endpoints used:
  * /v2/account                  (singleton)
  * /v2/positions                (reimport)
  * /v2/assets                   (reimport)
  * /v2/orders                   (incremental via updated_at/submitted_at)
  * /v2/account/activities       (incremental via activity date/time)
- Handles pagination, rate limits (HTTP 429), and merged checkpoints.
"""

import json
import time
import hashlib
from datetime import datetime, timezone
from dateutil import parser as dtparse
from typing import Dict, List, Any, Optional, Iterable

import requests
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


# =========================
# Configuration form
# =========================
def configuration_form(current_configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        {"name": "api_key_id", "label": "API Key ID", "type": "string", "required": True},
        {"name": "secret_key", "label": "Secret Key", "type": "password", "required": True},
        {
            "name": "base_url",
            "label": "Base URL",
            "type": "string",
            "required": True,
            "default": "https://paper-api.alpaca.markets",  # paper by default
        },
        {
            "name": "start_date",
            "label": "Start Date (ISO8601)",
            "type": "string",
            "required": False,
            "description": "When no state exists, incremental sync will start after this date (e.g. 2024-01-01T00:00:00Z).",
            "default": "2024-01-01T00:00:00Z",
        },
        {"name": "batch_size", "label": "Page Size", "type": "integer", "required": False, "default": 200},
    ]


# =========================
# Schema (Fivetran format)
# =========================
def schema(configuration: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        {
            "table": "account",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "account_number": "STRING",
                "status": "STRING",
                "currency": "STRING",
                "cash": "FLOAT",
                "portfolio_value": "FLOAT",
                "buying_power": "FLOAT",
                "pattern_day_trader": "BOOLEAN",
                "created_at": "UTC_DATETIME",
                "trading_blocked": "BOOLEAN",
                "transfers_blocked": "BOOLEAN",
                "account_blocked": "BOOLEAN",
                "_fivetran_synced": "UTC_DATETIME",
            },
        },
        {
            "table": "positions",
            "primary_key": ["symbol"],
            "columns": {
                "symbol": "STRING",
                "qty": "FLOAT",
                "avg_entry_price": "FLOAT",
                "market_value": "FLOAT",
                "cost_basis": "FLOAT",
                "unrealized_pl": "FLOAT",
                "unrealized_plpc": "FLOAT",
                "unrealized_intraday_pl": "FLOAT",
                "unrealized_intraday_plpc": "FLOAT",
                "current_price": "FLOAT",
                "lastday_price": "FLOAT",
                "change_today": "FLOAT",
                "_fivetran_synced": "UTC_DATETIME",
            },
        },
        {
            "table": "assets",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "class": "STRING",
                "exchange": "STRING",
                "symbol": "STRING",
                "name": "STRING",
                "status": "STRING",
                "tradable": "BOOLEAN",
                "marginable": "BOOLEAN",
                "shortable": "BOOLEAN",
                "easy_to_borrow": "BOOLEAN",
                "_fivetran_synced": "UTC_DATETIME",
            },
        },
        {
            "table": "orders",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "client_order_id": "STRING",
                "symbol": "STRING",
                "qty": "FLOAT",
                "filled_qty": "FLOAT",
                "type": "STRING",
                "side": "STRING",
                "time_in_force": "STRING",
                "limit_price": "FLOAT",
                "stop_price": "FLOAT",
                "status": "STRING",
                "submitted_at": "UTC_DATETIME",
                "filled_at": "UTC_DATETIME",
                "expired_at": "UTC_DATETIME",
                "canceled_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",      # not always present; we compute fallback
                "_fivetran_synced": "UTC_DATETIME",
            },
        },
        {
            "table": "activities",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "activity_type": "STRING",
                "transaction_time": "UTC_DATETIME",
                "type": "STRING",
                "price": "FLOAT",
                "qty": "STRING",
                "side": "STRING",
                "symbol": "STRING",
                "leaves_qty": "FLOAT",
                "order_id": "STRING",
                "_fivetran_synced": "UTC_DATETIME",
            },
        },
    ]

# =========================
# Update (sync)
# =========================
def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    if state is None:
        state = {}
    merged_state = dict(state)

    client = AlpacaClient(
        base_url=configuration["base_url"].rstrip("/"),
        key=configuration["api_key_id"],
        secret=configuration["secret_key"],
        page_size=int(configuration.get("batch_size", 200)),
    )
    # start_fallback = configuration.get("start_date", "2024-01-01T00:00:00Z")

    # 1) ACCOUNT (singleton)
    acct = client.get("/v2/account")
    now_iso = _utcnow()
    acct_row = _normalize_times(acct)
    acct_row["_fivetran_synced"] = now_iso
    acct_row.setdefault("id", acct_row.get("account_number"))
    acct_row = _coerce_numbers(acct_row)
    acct_row = _serialize_supported_types(acct_row)
    yield op.upsert("account", acct_row)

    merged_state["accounts"] = {"last_sync": now_iso}
    yield op.checkpoint(dict(merged_state))

    # 2) POSITIONS (reimport)
    positions = client.get("/v2/positions") or []
    old_pos = (merged_state.get("positions") or {}).get("checksums", {})
    new_pos = {}
    for p in positions:
        row = _normalize_times(p)
        row["_fivetran_synced"] = now_iso
        rid = _rid(row)
        new_pos[rid] = _chk(row)
        if old_pos.get(rid) != new_pos[rid]:
            row = _coerce_numbers(row)
            row = _serialize_supported_types(row)
            yield op.upsert("positions", row)
    # deletions
    for rid in set(old_pos.keys()) - set(new_pos.keys()):
        yield op.delete("positions", {"_row_id": rid})
    merged_state["positions"] = {"checksums": new_pos, "last_sync": now_iso}
    yield op.checkpoint(dict(merged_state))

    # 3) ASSETS (reimport, tradable only to keep volume sane)
    assets = client.get("/v2/assets", params={"status": "active"}) or []
    old_assets = (merged_state.get("assets") or {}).get("checksums", {})
    new_assets = {}
    for a in assets:
        row = _normalize_times(a)
        row["_fivetran_synced"] = now_iso
        rid = _rid(row)
        new_assets[rid] = _chk(row)
        if old_assets.get(rid) != new_assets[rid]:
            # print("row: ", row)
            row = _coerce_numbers(row)
            row = _serialize_supported_types(row)
            # log.info(f"Normalized row: {json.dumps(row, indent=2)}")
            yield op.upsert("assets", row)
    for rid in set(old_assets.keys()) - set(new_assets.keys()):
        yield op.delete("assets", {"_row_id": rid})
    merged_state["assets"] = {"checksums": new_assets, "last_sync": now_iso}
    yield op.checkpoint(dict(merged_state))

    # 4) ORDERS (incremental)
    o_state = merged_state.get("orders") or {}
    last_order_ts = o_state.get("last_incremental_value") or start_fallback
    for row in _stream_orders_incremental(client, after_iso=last_order_ts):
        row["_fivetran_synced"] = _utcnow()
        row = _coerce_numbers(row)
        row = _serialize_supported_types(row)
        yield op.upsert("orders", row)
        # track max timestamp
        ts = row.get("updated_at") or row.get("submitted_at") or row.get("filled_at")
        if ts:
            if _is_after(ts, last_order_ts):
                last_order_ts = ts
    merged_state["orders"] = {"last_incremental_value": last_order_ts, "last_sync": _utcnow()}
    yield op.checkpoint(dict(merged_state))

    # 5) ACTIVITIES (incremental)
    a_state = merged_state.get("activities") or {}
    last_act_ts = a_state.get("last_incremental_value") or start_fallback
    for row in _stream_activities_incremental(client, after_iso=last_act_ts):
        row["_fivetran_synced"] = _utcnow()
        row = _coerce_numbers(row)
        row = _serialize_supported_types(row)
        yield op.upsert("activities", row)
        ts = row.get("transaction_time")
        if ts and _is_after(ts, last_act_ts):
            last_act_ts = ts
    merged_state["activities"] = {"last_incremental_value": last_act_ts, "last_sync": _utcnow()}

    # final checkpoint with all tables
    yield op.checkpoint(dict(merged_state))

def _coerce_numbers(row):
    out = {}
    for k, v in row.items():
        if isinstance(v, str):
            if v.replace('.', '', 1).isdigit():
                out[k] = float(v) if '.' in v else int(v)
            else:
                out[k] = v
        else:
            out[k] = v
    return out


def _serialize_supported_types(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert unsupported types (list/dict) into JSON strings for SDK compatibility."""
    out = {}
    for k, v in row.items():
        if isinstance(v, (list, dict)):
            out[k] = json.dumps(v)
        else:
            out[k] = v
    return out


# =========================
# Alpaca client
# =========================
class AlpacaClient:
    def __init__(self, base_url: str, key: str, secret: str, page_size: int = 200):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update(
            {
                "APCA-API-KEY-ID": key,
                "APCA-API-SECRET-KEY": secret,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        self.page_size = max(1, min(page_size, 1000))

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{path}"
        for attempt in range(6):
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                wait = 2 ** attempt
                log.warning(f"Rate limited (429). Retrying in {wait}s …")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            if resp.text.strip() == "":
                return None
            return resp.json()
        resp.raise_for_status()

    # Paginates endpoints that return {items, next_page_token} or list+header link
    def paginate(self, path: str, params: Optional[Dict[str, Any]] = None, token_param: str = "page_token") -> Iterable[Dict]:
        p = dict(params or {})
        p["limit"] = self.page_size
        token = None
        while True:
            if token:
                p[token_param] = token
            data = self.get(path, params=p)
            if isinstance(data, dict) and "orders" in data:
                items = data["orders"]
                token = data.get("next_page_token")
            elif isinstance(data, dict) and "activities" in data:
                items = data["activities"]
                token = data.get("next_page_token")
            elif isinstance(data, list):
                # some endpoints just return a list without tokens
                items = data
                token = None
            else:
                items = []
                token = None

            for it in items:
                yield it

            if not token:
                break


# =========================
# Incremental streams
# =========================
def _stream_orders_incremental(client: AlpacaClient, after_iso: str) -> Iterable[Dict[str, Any]]:
    """
    Orders v2 supports 'after' ISO8601 and pagination via next_page_token.
    We fetch status=all to capture updates/cancels as well.
    """
    params = {
        "status": "all",
        "after": after_iso,
        "direction": "asc",
        "limit": client.page_size,
    }
    # Newer API returns {"orders":[...], "next_page_token":...}
    url = "/v2/orders"
    while True:
        obj = client.get(url, params=params)
        orders = obj.get("orders", []) if isinstance(obj, dict) else []
        for o in orders:
            row = _normalize_times(o)
            # derive an 'updated_at' fallback
            row["updated_at"] = (
                    row.get("updated_at")
                    or row.get("filled_at")
                    or row.get("canceled_at")
                    or row.get("expired_at")
                    or row.get("submitted_at")
            )
            yield row
        token = obj.get("next_page_token") if isinstance(obj, dict) else None
        if not token:
            break
        params["page_token"] = token


def _stream_activities_incremental(client: AlpacaClient, after_iso: str) -> Iterable[Dict[str, Any]]:
    """
    Fetch all account activities for paper trading.
    Paper endpoint does not support date filters (after/until/direction),
    so we just call it once with page_size.
    """
    log.info("Fetching activities (paper endpoint, no date filters)")
    params = {"page_size": client.page_size}
    try:
        items = client.get("/v2/account/activities", params=params)
    except requests.exceptions.HTTPError as e:
        log.warning(f"Activities fetch failed: {e}")
        return

    if not items:
        return

    # Handle both list and dict formats
    if isinstance(items, list):
        for a in items:
            row = _normalize_times(a)
            row.setdefault("id", row.get("order_id") or _rid(row))
            yield row
    elif isinstance(items, dict) and "activities" in items:
        for a in items["activities"]:
            row = _normalize_times(a)
            row.setdefault("id", row.get("order_id") or _rid(row))
            yield row



# =========================
# Helpers
# =========================
def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

def _normalize_times(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, str):
            # Normalize common timestamp fields to ISO8601 Z
            if _looks_like_datetime(v):
                try:
                    ts = dtparse.isoparse(v)
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    out[k] = ts.astimezone(timezone.utc).isoformat()
                except Exception:
                    out[k] = v
            else:
                out[k] = v
        else:
            out[k] = v
    return out

def _looks_like_datetime(s: str) -> bool:
    # quick heuristic
    return "T" in s and (s.endswith("Z") or "+" in s or s.count("-") >= 2)

def _rid(rec: Dict[str, Any]) -> str:
    return hashlib.md5(json.dumps(rec, sort_keys=True, default=str).encode()).hexdigest()

def _chk(rec: Dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(rec, sort_keys=True, default=str).encode()).hexdigest()

def _is_after(a: str, b: str) -> bool:
    try:
        return dtparse.isoparse(a) > dtparse.isoparse(b)
    except Exception:
        return str(a) > str(b)


# =========================
# Connector entry
# =========================
connector = Connector(
    schema=schema,
    update=update,
)

if __name__ == "__main__":
    # Example local debug (Paper Trading defaults)
    cfg = {
        "api_key_id": "YOUR_KEY_ID",
        "secret_key": "YOUR_SECRET",
        "base_url": "https://paper-api.alpaca.markets",
        "start_date": "2024-01-01T00:00:00Z",
        "batch_size": 200,
    }
    connector.debug()
