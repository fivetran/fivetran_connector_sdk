"""
Fivetran Connector SDK — Elasticsearch connector
Supports: Elasticsearch 7.12+, 8.x, 9.x, Elastic Cloud (stateful + serverless), OpenSearch 2.4+
Auth:      API key or Basic auth
Sync:      Full + incremental via _seq_no (indices) / @timestamp (data streams)
Pagination: PIT + search_after (replaces deprecated Scroll API)
"""

import base64
import json
import time

import requests as rq
from fivetran_connector_sdk import Connector

# Shared session for connection reuse (keep-alive) across all ES requests
_session = rq.Session()
_session.verify = False
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# ── Constants ─────────────────────────────────────────────────────────────────

PAGE_SIZE = 5_000
PIT_KEEP_ALIVE = "5m"
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30

# ── Elasticsearch type → Fivetran type ────────────────────────────────────────
# Based on the existing connector's ElasticsearchType.java mappings.
# Unsupported types (alias, geo_*, dense_vector, range types, etc.) are excluded
# from the schema — matching existing connector behaviour.

ES_TYPE_MAP = {
    "binary":           "BINARY",
    "boolean":          "BOOLEAN",
    "text":             "STRING",
    "keyword":          "STRING",
    "constant_keyword": "STRING",
    "wildcard":         "STRING",
    "integer":          "INT",
    "short":            "INT",
    "byte":             "INT",
    "double":           "DOUBLE",
    "float":            "DOUBLE",
    "half_float":       "DOUBLE",
    "scaled_float":     "DOUBLE",
    "long":             "LONG",
    "unsigned_long":    "LONG",
    "date":             "UTC_DATETIME",
    "date_nanos":       "UTC_DATETIME",
    "object":           "JSON",
    "nested":           "JSON",
    "flattened":        "JSON",
    "join":             "JSON",
}


# ── Auth & HTTP helpers ───────────────────────────────────────────────────────

def get_headers(configuration: dict) -> dict:
    auth_method = configuration.get("auth_method", "api_key").strip().lower()
    if auth_method == "api_key":
        return {
            "Authorization": f"ApiKey {configuration['api_key']}",
            "Content-Type": "application/json",
        }
    # basic_auth
    raw = f"{configuration['username']}:{configuration['password']}"
    encoded = base64.b64encode(raw.encode()).decode()
    return {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/json",
    }


def base_url(configuration: dict) -> str:
    return configuration["host"].rstrip("/")


def es_request(method: str, url: str, headers: dict, body: dict = None, params: dict = None) -> dict:
    """HTTP request with exponential-backoff retry on 429 / 5xx / transient errors."""
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = _session.request(
                method, url,
                headers=headers,
                json=body,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            # v9 change: timeouts return 429 instead of 5xx — both are retryable
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt < MAX_RETRIES:
                    wait = 2 ** attempt
                    log.warning(f"HTTP {resp.status_code} from {url} — retrying in {wait}s (attempt {attempt + 1}/{MAX_RETRIES})")
                    time.sleep(wait)
                    continue
            resp.raise_for_status()
            return resp.json()
        except (rq.exceptions.Timeout, rq.exceptions.ConnectionError) as e:
            if attempt < MAX_RETRIES:
                wait = 2 ** attempt
                log.warning(f"{type(e).__name__} — retrying in {wait}s (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(wait)
            else:
                raise


# ── Distribution detection ────────────────────────────────────────────────────

def detect_distribution(configuration: dict) -> str:
    """
    Detect whether the cluster is Elasticsearch or OpenSearch.
    Returns 'opensearch' or 'elasticsearch'.
    Defaults to 'elasticsearch' on any error.

    OpenSearch differences from Elasticsearch:
      PIT open:  POST /<index>/_search/point_in_time  (response key: 'pit_id')
      PIT close: DELETE /_search/point_in_time        (body key: 'pit_id')
      Sort tiebreaker: '_doc' (not '_shard_doc', which is ES-only)
    Elasticsearch:
      PIT open:  POST /<index>/_pit  (response key: 'id')
      PIT close: DELETE /_pit        (body key: 'id')
      Sort tiebreaker: '_shard_doc'
    """
    try:
        resp = es_request("GET", base_url(configuration), get_headers(configuration))
        distribution = resp.get("version", {}).get("distribution", "elasticsearch").lower()
        log.info(f"Detected cluster distribution: {distribution}")
        return distribution
    except Exception as e:
        log.warning(f"Could not detect cluster distribution, assuming elasticsearch: {e}")
        return "elasticsearch"


# ── Index / data-stream discovery ────────────────────────────────────────────

def discover_targets(configuration: dict) -> dict:
    """
    Return {name: target_type} for all syncable targets.
    target_type is either 'index' or 'data_stream'.

    System indices (names starting with '.') are excluded to match the
    behaviour of the existing connector (ESApiClient.filterSystemIndices).

    Uses GET /_mapping instead of GET /_cat/indices so that the connector
    works on Elastic Cloud Serverless (which blocks most _cat endpoints).
    """
    url = base_url(configuration)
    headers = get_headers(configuration)

    filter_cfg = configuration.get("indices", "all").strip()
    requested = None if filter_cfg.lower() == "all" else {i.strip() for i in filter_cfg.split(",")}

    targets = {}

    # 1. Data streams — very common in Elastic Cloud (APM, Beats, Fleet, SIEM)
    try:
        resp = es_request("GET", f"{url}/_data_stream", headers)
        for ds in resp.get("data_streams", []):
            name = ds["name"]
            if not name.startswith("."):
                targets[name] = "data_stream"
        log.info(f"Discovered {len(targets)} data streams")
    except Exception as e:
        log.warning(f"Could not list data streams (may not be supported on this cluster): {e}")

    # 2. Regular indices via _mapping — compatible with serverless + stateful
    try:
        resp = es_request("GET", f"{url}/*/_mapping", headers)
        index_count = 0
        for name in resp:
            if not name.startswith(".") and name not in targets:
                targets[name] = "index"
                index_count += 1
        log.info(f"Discovered {index_count} regular indices")
    except Exception as e:
        log.warning(f"Could not list indices: {e}")

    # Apply user filter
    if requested is not None:
        targets = {k: v for k, v in targets.items() if k in requested}
        log.info(f"Filtered to {len(targets)} targets based on 'indices' config")

    return targets


# ── Mapping helpers ───────────────────────────────────────────────────────────

def get_all_mappings(configuration: dict) -> dict:
    """Fetch all index mappings in a single request: {index_name: {properties}}."""
    try:
        resp = es_request("GET", f"{base_url(configuration)}/*/_mapping", get_headers(configuration))
        return {
            name: entry.get("mappings", {}).get("properties", {})
            for name, entry in resp.items()
        }
    except Exception as e:
        log.warning(f"Could not fetch mappings: {e}")
        return {}


def mapping_to_columns(properties: dict) -> dict:
    """
    Flatten ES mapping properties to {column_name: fivetran_type}.
    Unsupported ES types (alias, geo_*, dense_vector, range types, etc.)
    are silently excluded — matching existing connector behaviour.
    Object / nested / flattened types are synced as JSON blobs.
    """
    columns = {"_id": "STRING"}
    for field, defn in properties.items():
        es_type = defn.get("type")
        if es_type in ES_TYPE_MAP:
            columns[field] = ES_TYPE_MAP[es_type]
        elif es_type is None and "properties" in defn:
            # Nested object declared without an explicit type
            columns[field] = "JSON"
        # All other types (alias, geo_point, dense_vector, *_range, ip, etc.)
        # are excluded to match existing connector behaviour
    return columns


# ── PIT helpers ───────────────────────────────────────────────────────────────

def open_pit(configuration: dict, name: str, distribution: str = "elasticsearch") -> str:
    if distribution == "opensearch":
        url = f"{base_url(configuration)}/{name}/_search/point_in_time"
        resp = es_request("POST", url, get_headers(configuration), params={"keep_alive": PIT_KEEP_ALIVE})
        return resp["pit_id"]
    else:
        url = f"{base_url(configuration)}/{name}/_pit"
        resp = es_request("POST", url, get_headers(configuration), params={"keep_alive": PIT_KEEP_ALIVE})
        return resp["id"]


def close_pit(configuration: dict, pit_id: str, distribution: str = "elasticsearch") -> None:
    try:
        if distribution == "opensearch":
            url = f"{base_url(configuration)}/_search/point_in_time"
            body = {"pit_id": pit_id}
        else:
            url = f"{base_url(configuration)}/_pit"
            body = {"id": pit_id}
        es_request("DELETE", url, get_headers(configuration), body=body)
    except Exception as e:
        log.warning(f"Could not close PIT: {e}")


# ── Sequence-number helpers (regular indices) ─────────────────────────────────

def get_max_seq_no(configuration: dict, name: str) -> int:
    """Return the highest _seq_no currently in the index, or -1 if empty."""
    body = {
        "size": 1,
        "query": {"match_all": {}},
        "sort": [{"_seq_no": "desc"}],
        "seq_no_primary_term": True,
        "_source": False,
    }
    try:
        resp = es_request("POST", f"{base_url(configuration)}/{name}/_search",
                          get_headers(configuration), body=body)
        hits = resp.get("hits", {}).get("hits", [])
        if hits:
            return hits[0].get("_seq_no", -1)
    except Exception as e:
        log.warning(f"Could not get max seq_no for {name}: {e}")
    return -1


# ── Core PIT + search_after pagination ───────────────────────────────────────

def pit_page(configuration: dict, pit_id: str, sort_fields: list,
             search_after: list = None, query: dict = None) -> tuple:
    """
    Fetch one page using PIT + search_after.
    Returns (hits, updated_pit_id).

    ES may return a new PIT id on each response — always use the latest one
    per ES docs: "always use the most recently received ID for the next request."
    """
    body = {
        "size": PAGE_SIZE,
        "query": query or {"match_all": {}},
        "pit": {"id": pit_id, "keep_alive": PIT_KEEP_ALIVE},
        "sort": sort_fields,
        "seq_no_primary_term": True,
    }
    if search_after:
        body["search_after"] = search_after

    resp = es_request("POST", f"{base_url(configuration)}/_search",
                      get_headers(configuration), body=body)
    new_pit_id = resp.get("pit_id", pit_id)
    hits = resp.get("hits", {}).get("hits", [])
    return hits, new_pit_id


# ── Delete detection: full ID scan ────────────────────────────────────────────

def scan_all_ids(configuration: dict, name: str, distribution: str = "elasticsearch") -> set:
    """
    Scan every document ID in an index using PIT + search_after.
    Used for opt-in delete detection. _source is suppressed to minimise
    data transfer — only _id is fetched.
    """
    tiebreaker = "_doc" if distribution == "opensearch" else "_shard_doc"
    pit_id = open_pit(configuration, name, distribution)
    search_after = None
    ids = set()
    try:
        while True:
            body = {
                "size": PAGE_SIZE,
                "query": {"match_all": {}},
                "pit": {"id": pit_id, "keep_alive": PIT_KEEP_ALIVE},
                "sort": [{tiebreaker: "asc"}],
                "_source": False,
            }
            if search_after:
                body["search_after"] = search_after
            resp = es_request("POST", f"{base_url(configuration)}/_search",
                              get_headers(configuration), body=body)
            pit_id = resp.get("pit_id", pit_id)
            hits = resp.get("hits", {}).get("hits", [])
            if not hits:
                break
            for h in hits:
                ids.add(h["_id"])
            search_after = hits[-1]["sort"]
    finally:
        close_pit(configuration, pit_id, distribution)
    return ids


# ── Sync: regular index ───────────────────────────────────────────────────────

def sync_index(configuration: dict, name: str, index_state: dict,
               enable_delete_detection: bool, distribution: str = "elasticsearch") -> dict:
    """
    Sync a regular Elasticsearch index.

    Initial sync:      fetch all documents (seq_no 0 → current_max)
    Incremental sync:  fetch only documents with seq_no > last_max_seq_no

    Both use PIT + search_after (replaces the Scroll API used by the existing
    connector). The _seq_no incremental logic matches the existing connector's
    scrollDocsWithSeqNoGreaterThan approach.
    """
    last_max_seq_no = index_state.get("last_max_seq_no", -1)
    is_initial = last_max_seq_no == -1

    current_max_seq_no = get_max_seq_no(configuration, name)
    if current_max_seq_no == -1:
        log.info(f"{name}: index is empty, skipping")
        return index_state

    has_new_docs = is_initial or current_max_seq_no > last_max_seq_no

    # Upsert new/updated documents (skipped when only deletes have occurred)
    if has_new_docs:
        mode = "full" if is_initial else "incremental"
        log.info(f"{name}: starting {mode} sync (seq_no range: {last_max_seq_no} → {current_max_seq_no})")

        if is_initial:
            query = {"range": {"_seq_no": {"lte": current_max_seq_no}}}
        else:
            query = {"range": {"_seq_no": {"gt": last_max_seq_no, "lte": current_max_seq_no}}}

        tiebreaker = "_doc" if distribution == "opensearch" else "_shard_doc"
        pit_id = open_pit(configuration, name, distribution)
        search_after = None
        count = 0

        try:
            while True:
                hits, pit_id = pit_page(
                    configuration, pit_id,
                    sort_fields=[{tiebreaker: "asc"}],
                    search_after=search_after,
                    query=query,
                )
                if not hits:
                    break
                for hit in hits:
                    doc = {"_id": hit["_id"], **(hit.get("_source") or {})}
                    op.upsert(table=name, data=doc)
                    count += 1
                search_after = hits[-1]["sort"]
        finally:
            close_pit(configuration, pit_id, distribution)

        log.info(f"{name}: upserted {count} documents")
        index_state["last_max_seq_no"] = current_max_seq_no
    else:
        log.info(f"{name}: no new or updated documents since last sync")

    # Delete detection (opt-in)
    if enable_delete_detection:
        log.info(f"{name}: scanning all IDs for delete detection")
        curr_ids = scan_all_ids(configuration, name, distribution)
        prev_ids = set(index_state.get("all_ids", []))

        if not is_initial:
            deleted = prev_ids - curr_ids
            for did in deleted:
                op.delete(table=name, keys={"_id": did})
            if deleted:
                log.info(f"{name}: marked {len(deleted)} deleted documents")

        index_state["all_ids"] = list(curr_ids)

    return index_state


# ── Sync: data stream ─────────────────────────────────────────────────────────

def sync_data_stream(configuration: dict, name: str, index_state: dict, distribution: str = "elasticsearch") -> dict:
    """
    Sync an Elasticsearch data stream.

    Data streams are backed by multiple rolling indices; _seq_no resets on
    each rollover. We use @timestamp for incremental sync instead — every
    data stream document is required to have this field.

    Note: delete detection is intentionally not supported for data streams.
    Data streams are append-only by design (updates/deletes are uncommon
    and discouraged by Elasticsearch).
    """
    last_timestamp = index_state.get("last_timestamp")
    is_initial = last_timestamp is None
    mode = "full" if is_initial else "incremental"
    log.info(f"{name} (data stream): starting {mode} sync")

    if last_timestamp:
        query = {"range": {"@timestamp": {"gt": last_timestamp}}}
    else:
        query = {"match_all": {}}

    # Sort by @timestamp ASC so we can track the cursor, with tiebreaker
    tiebreaker = "_doc" if distribution == "opensearch" else "_shard_doc"
    sort_fields = [{"@timestamp": "asc"}, {tiebreaker: "asc"}]

    pit_id = open_pit(configuration, name, distribution)
    search_after = None
    count = 0
    latest_ts = last_timestamp

    try:
        while True:
            hits, pit_id = pit_page(
                configuration, pit_id,
                sort_fields=sort_fields,
                search_after=search_after,
                query=query,
            )
            if not hits:
                break
            for hit in hits:
                doc = {"_id": hit["_id"], **(hit.get("_source") or {})}
                op.upsert(table=name, data=doc)
                count += 1
                ts = (hit.get("_source") or {}).get("@timestamp")
                if ts and (latest_ts is None or ts > latest_ts):
                    latest_ts = ts
            search_after = hits[-1]["sort"]
    finally:
        close_pit(configuration, pit_id, distribution)

    if count:
        log.info(f"{name} (data stream): upserted {count} documents")
    else:
        log.info(f"{name} (data stream): no new documents since {last_timestamp}")

    if latest_ts:
        index_state["last_timestamp"] = latest_ts

    return index_state


# ── Schema ────────────────────────────────────────────────────────────────────

def schema(configuration: dict):
    """
    Discover all syncable indices and data streams and return their schemas.
    Column types are inferred from ES index mappings — matching the existing
    connector's ElasticsearchType → DataType mapping.
    Unsupported ES types are excluded (alias, geo_*, dense_vector, etc.).
    """
    targets = discover_targets(configuration)
    all_mappings = get_all_mappings(configuration)

    tables = []
    for name, target_type in targets.items():
        properties = all_mappings.get(name, {})
        columns = mapping_to_columns(properties)
        tables.append({
            "table": name,
            "primary_key": ["_id"],
            "columns": columns,
        })

    log.info(f"Schema: returning {len(tables)} tables")
    return tables


# ── Update ────────────────────────────────────────────────────────────────────

def update(configuration: dict, state: dict):
    enable_delete_detection = (
        configuration.get("enable_delete_detection", "false").strip().lower() == "true"
    )

    if enable_delete_detection:
        log.warning("Delete detection is enabled — full ID scans will run after each index sync")

    distribution = detect_distribution(configuration)

    targets = discover_targets(configuration)
    if not targets:
        log.warning("No indices or data streams found to sync. Check 'indices' config and permissions.")
        return

    log.info(f"Syncing {len(targets)} target(s): {list(targets.keys())}")

    for name, target_type in targets.items():
        index_state = state.get(name, {})
        try:
            if target_type == "data_stream":
                state[name] = sync_data_stream(configuration, name, index_state, distribution)
            else:
                state[name] = sync_index(configuration, name, index_state, enable_delete_detection, distribution)
        except Exception as e:
            log.severe(f"Failed to sync {name}: {e}")

        # Checkpoint after each index/stream so progress is preserved
        # even if a later index fails
        op.checkpoint(state)


# ── Entry point ───────────────────────────────────────────────────────────────

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
