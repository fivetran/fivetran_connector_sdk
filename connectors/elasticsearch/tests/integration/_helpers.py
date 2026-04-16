"""
Integration test helpers.

run_update(config, state) — calls connector.update(config, state) while
patching op.upsert / op.checkpoint / op.delete so we can inspect every
operation the connector emits without needing the SDK's gRPC harness.

seed_docs(host, index, docs) — bulk-insert documents into Elasticsearch /
OpenSearch using the _bulk API.

delete_doc(host, index, doc_id) — delete a single document by ID.
"""

import copy
import json
from unittest.mock import patch

import requests

# Import after sys.path is set by the root conftest
import connector
from fivetran_connector_sdk import Operations as op


# ── Operation capture ─────────────────────────────────────────────────────────

class SyncResult:
    """Collected output of one connector.update() call."""

    def __init__(self):
        self.upserts: list[dict] = []    # [{"table": str, "data": dict}]
        self.deletes: list[dict] = []    # [{"table": str, "keys": dict}]
        self.checkpoints: list[dict] = []  # [state_dict, ...]

    @property
    def last_state(self) -> dict:
        """Return the state from the last checkpoint, or {} if none."""
        return copy.deepcopy(self.checkpoints[-1]) if self.checkpoints else {}


def run_update(config: dict, state: dict) -> SyncResult:
    """
    Execute connector.update(config, state) and return a SyncResult with all
    emitted upserts, deletes and checkpoints captured.

    This bypasses the SDK's internal gRPC/queue machinery entirely by patching
    op.upsert, op.delete, and op.checkpoint at the module level before the
    call and restoring them afterwards.
    """
    result = SyncResult()

    def _capture_upsert(table: str, data: dict):
        result.upserts.append({"table": table, "data": copy.deepcopy(data)})

    def _capture_delete(table: str, keys: dict):
        result.deletes.append({"table": table, "keys": copy.deepcopy(keys)})

    def _capture_checkpoint(state_snap: dict):
        result.checkpoints.append(copy.deepcopy(state_snap))

    with (
        patch.object(op, "upsert", side_effect=_capture_upsert),
        patch.object(op, "delete", side_effect=_capture_delete),
        patch.object(op, "checkpoint", side_effect=_capture_checkpoint),
    ):
        connector.update(config, copy.deepcopy(state))

    return result


# ── Elasticsearch seeding / maintenance helpers ───────────────────────────────

def seed_docs(host: str, index: str, docs: list[dict]) -> None:
    """
    Bulk-insert *docs* into *index* on the cluster at *host*.

    Each element of *docs* should be a plain dict; if a ``_id`` key is present
    it is used as the document ID, otherwise ES assigns one automatically.

    Raises ``AssertionError`` if the bulk response reports any errors.
    """
    lines = []
    for doc in docs:
        doc_id = doc.get("_id")
        action = {"index": {"_index": index}}
        if doc_id is not None:
            action["index"]["_id"] = str(doc_id)
        # Exclude the synthetic _id from the _source
        source = {k: v for k, v in doc.items() if k != "_id"}
        lines.append(json.dumps(action))
        lines.append(json.dumps(source))

    body = "\n".join(lines) + "\n"
    resp = requests.post(
        f"{host}/_bulk",
        data=body,
        headers={"Content-Type": "application/x-ndjson"},
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json()
    assert not result.get("errors"), (
        f"Bulk index errors: {[i for i in result['items'] if 'error' in i.get('index', {})]}"
    )

    # Force a refresh so documents are immediately searchable
    requests.post(f"{host}/{index}/_refresh", timeout=10)


def delete_doc(host: str, index: str, doc_id: str) -> None:
    """Delete a single document by ID and refresh the index."""
    resp = requests.delete(f"{host}/{index}/_doc/{doc_id}", timeout=10)
    resp.raise_for_status()
    requests.post(f"{host}/{index}/_refresh", timeout=10)


def create_index(host: str, index: str) -> None:
    """Create an empty index (ignore 400 if it already exists)."""
    resp = requests.put(f"{host}/{index}", timeout=10)
    if resp.status_code not in (200, 400):
        resp.raise_for_status()
    requests.post(f"{host}/{index}/_refresh", timeout=10)
