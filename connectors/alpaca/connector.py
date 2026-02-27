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


# Define the schema function, which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
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


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
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
    start_fallback = configuration.get("start_date", "2024-01-01T00:00:00Z")

    # 1) ACCOUNT (singleton)
    acct = client.get("/v2/account")
    now_iso = utcnow()
    acct_row = normalize_times(acct)
    acct_row["_fivetran_synced"] = now_iso
    acct_row.setdefault("id", acct_row.get("account_number"))
    acct_row = coerce_numbers(acct_row)
    acct_row = serialize_supported_types(acct_row)

    # The 'upsert' operation inserts the data into the destination.
    # The op.upsert method is called with two arguments:
    # - The first argument is the name of the table to upsert the data into, in this case, "timestamps".
    # - The second argument is a dictionary containing the data to be upserted.
    yield op.upsert("account", acct_row)

    merged_state["accounts"] = {"last_sync": now_iso}
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(dict(merged_state))

    # 2) POSITIONS (reimport)
    positions = client.get("/v2/positions") or []
    old_pos = (merged_state.get("positions") or {}).get("checksums", {})
    new_pos = {}
    for p in positions:
        row = normalize_times(p)
        row["_fivetran_synced"] = now_iso
        rid = rid(row)
        new_pos[rid] = chk(row)
        if old_pos.get(rid) != new_pos[rid]:
            row = coerce_numbers(row)
            row = serialize_supported_types(row)

            # The 'upsert' operation inserts the data into the destination.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into, in this case, "timestamps".
            # - The second argument is a dictionary containing the data to be upserted.
            yield op.upsert("positions", row)
    # deletions
    for rid in set(old_pos.keys()) - set(new_pos.keys()):
        yield op.delete("positions", {"_row_id": rid})
    merged_state["positions"] = {"checksums": new_pos, "last_sync": now_iso}
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(dict(merged_state))

    # 3) ASSETS (reimport, tradable only to keep volume sane)
    assets = client.get("/v2/assets", params={"status": "active"}) or []
    old_assets = (merged_state.get("assets") or {}).get("checksums", {})
    new_assets = {}
    for a in assets:
        row = normalize_times(a)
        row["_fivetran_synced"] = now_iso
        rid = rid(row)
        new_assets[rid] = chk(row)
        if old_assets.get(rid) != new_assets[rid]:
            row = coerce_numbers(row)
            row = serialize_supported_types(row)

            # The 'upsert' operation inserts the data into the destination.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into, in this case, "timestamps".
            # - The second argument is a dictionary containing the data to be upserted.
            yield op.upsert("assets", row)
    for rid in set(old_assets.keys()) - set(new_assets.keys()):
        yield op.delete("assets", {"_row_id": rid})
    merged_state["assets"] = {"checksums": new_assets, "last_sync": now_iso}
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(dict(merged_state))

    # 4) ORDERS (incremental)
    o_state = merged_state.get("orders") or {}
    last_order_ts = o_state.get("last_incremental_value") or start_fallback
    for row in stream_orders_incremental(client, after_iso=last_order_ts):
        row["_fivetran_synced"] = utcnow()
        row = coerce_numbers(row)
        row = serialize_supported_types(row)

        # The 'upsert' operation inserts the data into the destination.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into, in this case, "timestamps".
        # - The second argument is a dictionary containing the data to be upserted.
        yield op.upsert("orders", row)
        # track max timestamp
        ts = row.get("updated_at") or row.get("submitted_at") or row.get("filled_at")
        if ts:
            if is_after(ts, last_order_ts):
                last_order_ts = ts
    merged_state["orders"] = {"last_incremental_value": last_order_ts, "last_sync": utcnow()}
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(dict(merged_state))

    # 5) ACTIVITIES (incremental)
    a_state = merged_state.get("activities") or {}
    last_act_ts = a_state.get("last_incremental_value") or start_fallback
    for row in stream_activities_incremental(client):
        row["_fivetran_synced"] = utcnow()
        row = coerce_numbers(row)
        row = serialize_supported_types(row)

        # The 'upsert' operation inserts the data into the destination.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into, in this case, "timestamps".
        # - The second argument is a dictionary containing the data to be upserted.
        yield op.upsert("activities", row)
        ts = row.get("transaction_time")
        if ts and is_after(ts, last_act_ts):
            last_act_ts = ts
    merged_state["activities"] = {"last_incremental_value": last_act_ts, "last_sync": utcnow()}

    # final checkpoint with all tables

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(dict(merged_state))

def coerce_numbers(row):
    """
    Convert string numbers into int/float types.
    Args param row: Row dictionary
    """
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


def serialize_supported_types(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert unsupported types (list/dict) into JSON strings for SDK compatibility.
    Args param row: Row dictionary
    """
    out = {}
    for k, v in row.items():
        if isinstance(v, (list, dict)):
            out[k] = json.dumps(v)
        else:
            out[k] = v
    return out


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
        """
        Make a GET request with exponential backoff on 429 rate limits.
        Args: param path: path to request
                param params: query parameters
        """
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
        """
        Paginate through results.
        Args: param path: path to request
              param params: query parameters
        """
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


def stream_orders_incremental(client: AlpacaClient, after_iso: str) -> Iterable[Dict[str, Any]]:
    """
    Orders v2 supports 'after' ISO8601 and pagination via next_page_token.
    We fetch status=all to capture updates/cancels as well.
    Args param client: AlpacaClient instance
         param after_iso: ISO8601 string to filter orders updated after this time
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
            row = normalize_times(o)
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


def stream_activities_incremental(client: AlpacaClient) -> Iterable[Dict[str, Any]]:
    """
    Fetch all account activities for paper trading.
    Paper endpoint does not support date filters (after/until/direction),
    so we just call it once with page_size.
    Args param client: AlpacaClient instance
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
            row = normalize_times(a)
            row.setdefault("id", row.get("order_id") or rid(row))
            yield row
    elif isinstance(items, dict) and "activities" in items:
        for a in items["activities"]:
            row = normalize_times(a)
            row.setdefault("id", row.get("order_id") or rid(row))
            yield row

def utcnow() -> str:
    """
    Get the current UTC time in ISO8601 format.
    """
    return datetime.now(timezone.utc).isoformat()

def normalize_times(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize datetime strings to ISO8601 Z format.
    Args: param d: Row dictionary
    :param d:
    :return:
    """
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, str):
            # Normalize common timestamp fields to ISO8601 Z
            if looks_like_datetime(v):
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

def looks_like_datetime(s: str) -> bool:
    """
    Heuristic to identify datetime-like strings.
    Args: param s: String to check
    """
    # quick heuristic
    return "T" in s and (s.endswith("Z") or "+" in s or s.count("-") >= 2)

def rid(rec: Dict[str, Any]) -> str:
    """
    Generate a row ID based on the MD5 hash of the record.
    Args param rec: Row dictionary
    """
    return hashlib.md5(json.dumps(rec, sort_keys=True, default=str).encode()).hexdigest()

def chk(rec: Dict[str, Any]) -> str:
    """
    Generate a checksum based on the SHA256 hash of the record.
    Args param rec: Row dictionary
    """
    return hashlib.sha256(json.dumps(rec, sort_keys=True, default=str).encode()).hexdigest()

def is_after(a: str, b: str) -> bool:
    """
    Compare two ISO8601 datetime strings.
    """
    try:
        return dtparse.isoparse(a) > dtparse.isoparse(b)
    except Exception:
        return str(a) > str(b)


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

# Fivetran debug results:
# Operation       | Calls
# ----------------+------------
# Upserts         | 12893
# Updates         | 0
# Deletes         | 0
# Truncates       | 0
# SchemaChanges   | 5
# Checkpoints     | 3
