# Example: Multiple Incremental Sync Strategies
# This connector demonstrates several ways to perform incremental syncs and save state.
# Strategies included: keyset pagination, offset-based pagination, timestamp-based sync,
# step-size sync, and replay sync.
#
# To run, you need the fivetran-connector-sdk and requests packages.

import requests as rq
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

def schema(configuration: dict):
    return [
        {
            "table": "user",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "email": "STRING",
                "address": "STRING",
                "company": "STRING",
                "job": "STRING",
                "updatedAt": "UTC_DATETIME",
                "createdAt": "UTC_DATETIME",
            },
        }
    ]

# --- Keyset Pagination Strategy ---
def update_keyset(configuration: dict, state: dict):
    log.info("Running keyset pagination incremental sync")
    base_url = configuration.get("base_url", "http://127.0.0.1:5001/pagination/keyset")
    cursor = state.get('last_updated_at', '0001-01-01T00:00:00Z')
    params = {"updated_since": cursor}
    yield from sync_items_keyset(base_url, params, state)

def sync_items_keyset(base_url, params, state):
    while True:
        response = rq.get(base_url, params=params)
        response.raise_for_status()
        data = response.json().get("data", [])
        if not data:
            break
        for user in data:
            yield op.upsert(table="user", data=user)
            state["last_updated_at"] = user["updatedAt"]
        yield op.checkpoint(state)
        scroll_param = response.json().get("scroll_param")
        if not scroll_param:
            break
        params = {"scroll_param": scroll_param}

# --- Offset-based Pagination Strategy ---
def update_offset(configuration: dict, state: dict):
    log.info("Running offset-based incremental sync")
    base_url = configuration.get("base_url", "http://127.0.0.1:5001/pagination/offset")
    offset = state.get('offset', 0)
    page_size = configuration.get('page_size', 100)
    while True:
        params = {"offset": offset, "limit": page_size}
        response = rq.get(base_url, params=params)
        response.raise_for_status()
        data = response.json().get("data", [])
        if not data:
            break
        for user in data:
            yield op.upsert(table="user", data=user)
        offset += len(data)
        state["offset"] = offset
        yield op.checkpoint(state)
        if len(data) < page_size:
            break

# --- Timestamp-based Incremental Sync ---
def update_timestamp(configuration: dict, state: dict):
    log.info("Running timestamp-based incremental sync")
    base_url = configuration.get("base_url", "http://127.0.0.1:5001/incremental/timestamp")
    last_ts = state.get('last_timestamp', '0001-01-01T00:00:00Z')
    params = {"since": last_ts}
    while True:
        response = rq.get(base_url, params=params)
        response.raise_for_status()
        data = response.json().get("data", [])
        if not data:
            break
        for user in data:
            yield op.upsert(table="user", data=user)
            state["last_timestamp"] = user["updatedAt"]
        yield op.checkpoint(state)
        # Assume API returns all new/updated records since last_timestamp in one call
        break

# --- Step-size Incremental Sync ---
def update_step_size(configuration: dict, state: dict):
    log.info("Running step-size incremental sync")
    base_url = configuration.get("base_url", "http://127.0.0.1:5001/incremental/step")
    current_id = state.get('current_id', configuration.get('initial_id', 1))
    step_size = configuration.get('step_size', 1000)
    max_id = configuration.get('max_id', 100000)  # Safety limit
    
    while current_id <= max_id:
        params = {"start_id": current_id, "end_id": current_id + step_size - 1}
        response = rq.get(base_url, params=params)
        response.raise_for_status()
        data = response.json().get("data", [])
        if not data:
            break
        for user in data:
            yield op.upsert(table="user", data=user)
        current_id += step_size
        state["current_id"] = current_id
        yield op.checkpoint(state)
        if len(data) < step_size:
            break

# --- Replay Incremental Sync (with buffer) ---
def update_replay(configuration: dict, state: dict):
    log.info("Running replay incremental sync with buffer")
    base_url = configuration.get("base_url", "http://127.0.0.1:5001/incremental/replay")
    buffer_hours = configuration.get('buffer_hours', 2)
    last_ts = state.get('last_timestamp', '0001-01-01T00:00:00Z')
    
    # Apply buffer by going back buffer_hours from the last timestamp
    # This is useful for read-replica scenarios where there might be replication lag
    from datetime import datetime, timedelta
    import pytz
    
    if last_ts != '0001-01-01T00:00:00Z':
        last_dt = datetime.fromisoformat(last_ts.replace('Z', '+00:00'))
        buffer_dt = last_dt - timedelta(hours=buffer_hours)
        buffer_ts = buffer_dt.isoformat().replace('+00:00', 'Z')
    else:
        buffer_ts = last_ts
    
    params = {"since": buffer_ts}
    while True:
        response = rq.get(base_url, params=params)
        response.raise_for_status()
        data = response.json().get("data", [])
        if not data:
            break
        for user in data:
            yield op.upsert(table="user", data=user)
            state["last_timestamp"] = user["updatedAt"]
        yield op.checkpoint(state)
        # Assume API returns all records since buffer_ts in one call
        break

# --- Main update function that dispatches based on configuration ---
def update(configuration: dict, state: dict):
    strategy = configuration.get("strategy", "keyset")
    if strategy == "keyset":
        yield from update_keyset(configuration, state)
    elif strategy == "offset":
        yield from update_offset(configuration, state)
    elif strategy == "timestamp":
        yield from update_timestamp(configuration, state)
    elif strategy == "step_size":
        yield from update_step_size(configuration, state)
    elif strategy == "replay":
        yield from update_replay(configuration, state)
    else:
        raise ValueError(f"Unknown incremental sync strategy: {strategy}")

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug() 