import base64   # Provides functions for encoding and decoding data in Base64 format
import asyncio  # Enables writing and running asynchronous I/O and concurrent code
from typing import Any, Dict, List, Optional  # Provides type hints for generic, dictionary, list, and optional types

# For reading configuration from a JSON file
import json

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

import time  # For adding delays to avoid rate limits

# Temporal SDK (async)
from temporalio.client import Client
from temporalio.service import ServiceClient, ConnectConfig, TLSConfig


# ------------- Tables (SDK schema) -------------
TABLES: Dict[str, Dict[str, Any]] = {
    "temporal_workflows": {
        "pk": ["workflow_id", "run_id"],
        "updated": "close_time_unix",  # used for incremental cursor; falls back to start_time if open
        "schema": {
            # identity
            "namespace": "STRING",
            "workflow_id": "STRING",
            "run_id": "STRING",
            "workflow_type": "STRING",
            # timing
            "start_time": "UTC_DATETIME",
            "close_time": "UTC_DATETIME",
            "start_time_unix": "INTEGER",
            "close_time_unix": "INTEGER",
            # state
            "status": "STRING",          # RUNNING | COMPLETED | FAILED | CANCELED | TERMINATED | TIMED_OUT | CONTINUED_AS_NEW | UNDEFINED
            "task_queue": "STRING",
            "history_length": "INTEGER",
            # visibility payloads
            "search_attributes": "JSON", # flattened JSON map
            "memo": "JSON",
            # raw execution record (for auditing)
            "raw": "JSON",
        },
    },
    # (Optional) You can add derived tables later (signals, activities, etc.) by parsing history.
}

# Define the schema function, which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.def schema(configuration: Dict[str, Any]):
def schema(configuration):
    declared = []
    for t, meta in TABLES.items():
        declared.append(
            {
                "table": t,
                "primary_key": meta["pk"],
                "column": meta["schema"],
            }
        )
    return declared


# ------------- Cursor helpers -------------
# state = {"cursors": {"temporal_workflows": {"<namespace>": <last_unix_ts>}}}
def get_cursor(state: Dict[str, Any], ns: str) -> int:
    """
    Get the per-namespace cursor from state.
    Defaults to 0 if not found.
    Args: param state: state dict
          param ns: namespace string
    """
    try:
        return int((state or {}).get("cursors", {}).get("temporal_workflows", {}).get(ns, 0))
    except Exception:
        return 0

def set_cursor(state: Dict[str, Any], ns: str, value: int) -> Dict[str, Any]:
    """
    Set the per-namespace cursor in state.
    Args: param state: state dict
          param ns: namespace string
          param value: new cursor value
    """
    new_state = dict(state or {})
    cursors = dict(new_state.get("cursors", {}))
    wf = dict(cursors.get("temporal_workflows", {}))
    wf[ns] = int(value)
    cursors["temporal_workflows"] = wf
    new_state["cursors"] = cursors
    return new_state


# ------------- Temporal Connection -------------
def load_bytes(s: Optional[str]) -> Optional[bytes]:
    """
    Load bytes from either raw string or base64-encoded string.
    Used for TLS certs/keys.
    Args param s: input string
    Returns: bytes or None
    """
    if not s:
        return None
    # allow passing PEM directly or base64 of PEM
    try:
        # base64?
        return base64.b64decode(s)
    except Exception:
        # assume raw PEM text
        return s.encode("utf-8")

async def connect_temporal(configuration: Dict[str, Any]) -> Client:
    """
    Connect to Temporal Cloud using either API key or mTLS (both supported by Temporal Cloud).
    For API keys, pass:
      - endpoint: "<namespace_id>.<account_id>.tmprl.cloud:7233" (or regional endpoint)
      - namespace: "<namespace_id>.<account_id>"
      - api_key: "<YOUR_API_KEY>"
    For mTLS, pass:
      - endpoint, namespace, and TLS certs/keys (PEM or base64-encoded)
    """
    endpoint: str = configuration["endpoint"]
    namespace: str = configuration["namespace"]

    api_key: Optional[str] = configuration.get("api_key")
    tls_enable: bool = bool(configuration.get("tls", True))

    # Optional explicit TLS config (for mTLS)
    server_root_ca = load_bytes(configuration.get("server_root_ca"))
    client_cert = load_bytes(configuration.get("client_cert"))
    client_key = load_bytes(configuration.get("client_private_key"))

    tls_cfg = None
    if tls_enable:
        if server_root_ca or client_cert or client_key:
            tls_cfg = TLSConfig(
                server_root_ca_cert=server_root_ca,
                client_cert=client_cert,
                client_private_key=client_key,
            )
        else:
            tls_cfg = True  # default TLS

    # Connect (Temporal Python SDK is async)
    client = await Client.connect(
        target_host=endpoint,
        namespace=namespace,
        api_key=api_key,     # used for Cloud API key auth
        tls=tls_cfg,         # True or TLSConfig(...)
    )
    return client


# ------------- Data fetching -------------
async def list_workflows_since(client: Client, since_unix: int, page_size: int = 1000):
    """
    Iterate workflow executions using Visibility (Temporal search).
    Uses StartTime/CloseTime >= since_unix.
    Args: param client: Temporal Client
          param since_unix: cursor unix timestamp
        param page_size: page size for listing
    """
    # Temporal Visibility query syntax:
    # - StartTime, CloseTime, ExecutionStatus, WorkflowId, WorkflowType, etc.
    # Fetch both open and recently closed executions after our cursor.
    # NB: Temporal expects RFC3339 times in queries, but Python SDK exposes a helper
    # via Client.list_workflows(query=..., page_size=...).
    # We'll query by StartTime OR CloseTime being after our cursor to catch long-running.
    # Note: Python SDK returns an async iterator (paginates under the hood).
    since_sec = int(since_unix)
    query = f"StartTime >= {since_sec}s or CloseTime >= {since_sec}s"

    async for we in client.list_workflows(query=query, page_size=page_size):
        yield we


def dt_to_iso(dt) -> Optional[str]:
    """
    Convert tz-aware datetime to UTC ISO format string.
    Args: param dt: datetime
    """
    if not dt:
        return None
    # dt is aware; Temporal SDK returns tz-aware datetimes; convert to UTC ISO
    return dt.astimezone(tz=time.tzutc() if hasattr(time, "tzutc") else None).strftime("%Y-%m-%dT%H:%M:%SZ")


def dt_to_unix(dt) -> Optional[int]:
    """
    Convert tz-aware datetime to unix timestamp (int).
    Args: param dt: datetime
    """
    if not dt:
        return None
    return int(dt.timestamp())


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: a dictionary that contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary that contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: Dict[str, Any], state: Dict[str, Any]):
    """
    Main entrypoint called by Fivetran. Creates an event loop, connects to Temporal,
    streams executions since last cursor, and upserts rows.
    """
    namespace: str = configuration["namespace"]
    page_size: int = int(configuration.get("page_size", 1000))
    max_rows: Optional[int] = configuration.get("max_rows")  # optional guard in tests

    # Because temporalio is async, run an asyncio program inside update()
    async def _run():
        nonlocal state
        client = await connect_temporal(configuration)

        last = get_cursor(state, namespace)
        max_seen = last

        count = 0
        async for exec_info in list_workflows_since(client, last, page_size=page_size):
            # exec_info is temporalio.api.workflow.v1.WorkflowExecutionInfo-like wrapper
            # Collect fields with safe defaults
            wid = getattr(exec_info, "workflow_id", None)
            rid = getattr(exec_info, "run_id", None)
            wtype = getattr(getattr(exec_info, "type", None), "name", None)
            task_queue = getattr(getattr(exec_info, "task_queue", None), "name", None)
            start_dt = getattr(exec_info, "start_time", None)
            close_dt = getattr(exec_info, "close_time", None)
            history_length = getattr(exec_info, "history_length", None)
            status_enum = getattr(exec_info, "status", None)
            status = str(status_enum).split(".")[-1] if status_enum else None

            # search attributes + memo are structured; convert to plain dict
            sa = getattr(exec_info, "search_attributes", None)
            memo = getattr(exec_info, "memo", None)
            sa_obj = getattr(sa, "indexed_fields", None) if sa else None
            memo_obj = getattr(memo, "fields", None) if memo else None

            # Build row
            start_unix = dt_to_unix(start_dt)
            close_unix = dt_to_unix(close_dt)
            updated_cursor = close_unix or start_unix or 0

            row = {
                "namespace": namespace,
                "workflow_id": wid,
                "run_id": rid,
                "workflow_type": wtype,
                "task_queue": task_queue,
                "start_time": start_dt.isoformat().replace("+00:00", "Z") if start_dt else None,
                "close_time": close_dt.isoformat().replace("+00:00", "Z") if close_dt else None,
                "start_time_unix": start_unix,
                "close_time_unix": close_unix,
                "status": status,
                "history_length": history_length,
                "search_attributes": to_plain_dict(sa_obj),
                "memo": to_plain_dict(memo_obj),
                "raw": safe_asdict(exec_info),
            }

            # The 'upsert' operation inserts the data into the destination.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into
            # - The second argument is a dictionary containing the data to be upserted,
            op.upsert(table="temporal_workflows", data=row)

            if updated_cursor and updated_cursor > max_seen:
                max_seen = updated_cursor

            count += 1
            if max_rows and count >= int(max_rows):
                break

        # Save per-namespace cursor
        if max_seen != last:
            state = set_cursor(state, namespace, max_seen)

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)
        log.info(f"Temporal Cloud sync complete. Rows: {count}, cursor(ns={namespace})={max_seen}")

    # helpers used inside _run()
    def to_plain_dict(fields_obj) -> Optional[Dict[str, Any]]:
        """
        Convert Temporal payload maps (memo/search attributes) to plain JSONable dicts.
        Args: param fields_obj: mapping[str] -> Payload or Value
        """
        if not fields_obj:
            return None
        out = {}
        # fields_obj is a mapping[str] -> Payload or Value; try best-effort string/JSON decode
        for k, v in fields_obj.items():
            try:
                # Temporal Python SDK exposes .to_json() on payloads in many cases
                # fall back to str()
                if hasattr(v, "to_json"):
                    out[k] = v.to_json()
                elif hasattr(v, "data") and isinstance(v.data, (bytes, bytearray)):
                    out[k] = safe_b64(v.data)
                else:
                    out[k] = safe_asdict(v)
            except Exception:
                out[k] = safe_asdict(v)
        return out

    def safe_asdict(obj):
        """
        Safely convert an object to a dict for JSON serialization.
        Tries .to_dict(), dataclass asdict(), or json serialization.
        Fallbacks to str(obj) if all else fails.
        Args param obj: input object
        Returns: dict or str
        """
        try:
            # protobuf-like objects may have .to_dict()
            if hasattr(obj, "to_dict"):
                return obj.to_dict()  # type: ignore
            # dataclass?
            from dataclasses import asdict, is_dataclass
            if is_dataclass(obj):
                return asdict(obj)
        except Exception:
            pass
        try:
            return json.loads(json.dumps(obj, default=str))
        except Exception:
            return str(obj)

    def safe_b64(b: bytes) -> str:
        """
        Safely base64-encode bytes to ASCII string.
        Args: param b: input bytes
        Returns: base64 ASCII string
        """
        return base64.b64encode(b).decode("ascii")

    # run the async program
    asyncio.run(_run())


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
