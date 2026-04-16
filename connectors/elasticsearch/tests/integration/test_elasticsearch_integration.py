"""
Integration tests for the Elasticsearch connector.

These tests spin up a real Elasticsearch 8.17.0 container via testcontainers,
seed data, run connector.update(), and assert that the correct upserts and
checkpoints are emitted.

All tests are marked @pytest.mark.integration and are skipped automatically
when Docker is not available.
"""

import json
import time

import pytest
import requests

from tests.integration._helpers import run_update, seed_docs, delete_doc, create_index


pytestmark = pytest.mark.integration


# ─── helpers ──────────────────────────────────────────────────────────────────

def _make_docs(count: int, index: str = "test_idx") -> list:
    return [{"_id": str(i), "name": f"doc {i}", "value": i} for i in range(count)]


def _refresh(host: str, index: str) -> None:
    requests.post(f"{host}/{index}/_refresh", timeout=10)


def _delete_index(host: str, index: str) -> None:
    requests.delete(f"{host}/{index}", timeout=10)


# ─── Test: initial full sync ───────────────────────────────────────────────────

class TestInitialSync:
    """Full (first-run) sync retrieves all documents from an index."""

    def test_upserts_all_seeded_docs(self, es_config):
        index = "it_initial_sync"
        _delete_index(es_config["host"], index)
        docs = _make_docs(10, index)
        seed_docs(es_config["host"], index, docs)

        config = {**es_config, "indices": index}
        result = run_update(config, {})

        upserted_ids = {u["data"]["_id"] for u in result.upserts}
        expected_ids = {str(i) for i in range(10)}
        assert expected_ids == upserted_ids, (
            f"Expected IDs {expected_ids}, got {upserted_ids}"
        )

    def test_upsert_table_name_matches_index(self, es_config):
        index = "it_table_name"
        _delete_index(es_config["host"], index)
        seed_docs(es_config["host"], index, [{"_id": "1", "v": 1}])

        config = {**es_config, "indices": index}
        result = run_update(config, {})

        assert result.upserts, "Expected at least one upsert"
        for u in result.upserts:
            assert u["table"] == index

    def test_upsert_data_contains_source_fields(self, es_config):
        index = "it_source_fields"
        _delete_index(es_config["host"], index)
        seed_docs(es_config["host"], index, [{"_id": "1", "name": "Alice", "score": 42}])

        config = {**es_config, "indices": index}
        result = run_update(config, {})

        assert result.upserts, "Expected at least one upsert"
        data = result.upserts[0]["data"]
        assert data.get("name") == "Alice"
        assert data.get("score") == 42
        assert data.get("_id") == "1"

    def test_checkpoint_is_emitted_after_sync(self, es_config):
        index = "it_checkpoint"
        _delete_index(es_config["host"], index)
        seed_docs(es_config["host"], index, [{"_id": "x", "val": 1}])

        config = {**es_config, "indices": index}
        result = run_update(config, {})

        assert result.checkpoints, "Expected at least one checkpoint"

    def test_state_contains_index_entry_after_sync(self, es_config):
        index = "it_state_entry"
        _delete_index(es_config["host"], index)
        seed_docs(es_config["host"], index, [{"_id": "a"}])

        config = {**es_config, "indices": index}
        result = run_update(config, {})

        last_state = result.last_state
        assert index in last_state, f"Expected '{index}' key in state, got: {last_state}"

    def test_state_has_last_max_seq_no_after_initial_sync(self, es_config):
        index = "it_seq_no_state"
        _delete_index(es_config["host"], index)
        seed_docs(es_config["host"], index, [{"_id": "1"}, {"_id": "2"}])

        config = {**es_config, "indices": index}
        result = run_update(config, {})

        last_state = result.last_state
        index_state = last_state.get(index, {})
        assert "last_max_seq_no" in index_state, (
            f"Expected 'last_max_seq_no' in index state: {index_state}"
        )
        assert index_state["last_max_seq_no"] >= 0

    def test_empty_index_produces_no_upserts(self, es_config):
        index = "it_empty_index"
        _delete_index(es_config["host"], index)
        create_index(es_config["host"], index)

        config = {**es_config, "indices": index}
        result = run_update(config, {})

        assert result.upserts == [], "Empty index should produce no upserts"


# ─── Test: incremental sync ───────────────────────────────────────────────────

class TestIncrementalSync:
    """Second-run sync only picks up documents added after the last checkpoint."""

    def test_incremental_sync_picks_up_new_docs(self, es_config):
        index = "it_incremental"
        _delete_index(es_config["host"], index)
        seed_docs(es_config["host"], index, [{"_id": "0", "v": 0}])

        config = {**es_config, "indices": index}

        # Initial sync
        first_result = run_update(config, {})
        first_state = first_result.last_state
        assert first_result.upserts, "Initial sync produced no upserts"

        # Seed one more doc
        seed_docs(es_config["host"], index, [{"_id": "1", "v": 1}])

        # Incremental sync
        second_result = run_update(config, first_state)
        upserted_ids = {u["data"]["_id"] for u in second_result.upserts}

        assert "1" in upserted_ids, (
            f"Incremental sync should include new doc '1'; got: {upserted_ids}"
        )
        # The original doc should NOT be re-synced
        assert "0" not in upserted_ids, (
            f"Incremental sync should not re-sync doc '0'; got: {upserted_ids}"
        )

    def test_no_new_docs_means_no_upserts(self, es_config):
        index = "it_no_new_docs"
        _delete_index(es_config["host"], index)
        seed_docs(es_config["host"], index, [{"_id": "1", "v": 1}])

        config = {**es_config, "indices": index}

        # Initial sync
        first_result = run_update(config, {})
        first_state = first_result.last_state

        # Second sync with no new docs
        second_result = run_update(config, first_state)
        assert second_result.upserts == [], (
            "No new docs → should produce no upserts"
        )

    def test_incremental_state_advances(self, es_config):
        index = "it_state_advances"
        _delete_index(es_config["host"], index)
        seed_docs(es_config["host"], index, [{"_id": "a"}])

        config = {**es_config, "indices": index}

        first_result = run_update(config, {})
        first_seq_no = first_result.last_state[index]["last_max_seq_no"]

        seed_docs(es_config["host"], index, [{"_id": "b"}])

        second_result = run_update(config, first_result.last_state)
        second_seq_no = second_result.last_state[index]["last_max_seq_no"]

        assert second_seq_no > first_seq_no, (
            f"State seq_no should advance: {first_seq_no} → {second_seq_no}"
        )


# ─── Test: multi-index sync ───────────────────────────────────────────────────

class TestMultiIndexSync:
    """Connector syncs multiple indices in one pass."""

    def test_all_specified_indices_are_synced(self, es_config):
        idx_a = "it_multi_a"
        idx_b = "it_multi_b"
        for idx in (idx_a, idx_b):
            _delete_index(es_config["host"], idx)
            seed_docs(es_config["host"], idx, [{"_id": "1", "src": idx}])

        config = {**es_config, "indices": f"{idx_a},{idx_b}"}
        result = run_update(config, {})

        tables_synced = {u["table"] for u in result.upserts}
        assert idx_a in tables_synced
        assert idx_b in tables_synced

    def test_checkpoint_per_index(self, es_config):
        idx_a = "it_ckpt_a"
        idx_b = "it_ckpt_b"
        for idx in (idx_a, idx_b):
            _delete_index(es_config["host"], idx)
            seed_docs(es_config["host"], idx, [{"_id": "1"}])

        config = {**es_config, "indices": f"{idx_a},{idx_b}"}
        result = run_update(config, {})

        # Should have at least 2 checkpoints (one per index)
        assert len(result.checkpoints) >= 2

    def test_state_has_entry_for_each_index(self, es_config):
        idx_a = "it_state_a"
        idx_b = "it_state_b"
        for idx in (idx_a, idx_b):
            _delete_index(es_config["host"], idx)
            seed_docs(es_config["host"], idx, [{"_id": "1"}])

        config = {**es_config, "indices": f"{idx_a},{idx_b}"}
        result = run_update(config, {})

        state = result.last_state
        assert idx_a in state
        assert idx_b in state


# ─── Test: index filter (indices config) ─────────────────────────────────────

class TestIndexFilter:
    """The 'indices' config key controls which indices are synced."""

    def test_excluded_index_is_not_synced(self, es_config):
        idx_included = "it_filter_in"
        idx_excluded = "it_filter_out"
        for idx in (idx_included, idx_excluded):
            _delete_index(es_config["host"], idx)
            seed_docs(es_config["host"], idx, [{"_id": "1", "src": idx}])

        config = {**es_config, "indices": idx_included}
        result = run_update(config, {})

        tables_synced = {u["table"] for u in result.upserts}
        assert idx_included in tables_synced
        assert idx_excluded not in tables_synced


# ─── Test: delete detection ───────────────────────────────────────────────────

class TestDeleteDetection:
    """When enable_delete_detection=true, deletes are surfaced on incremental sync."""

    def test_deleted_doc_is_not_re_upserted(self, es_config):
        """After deletion, a subsequent sync should not upsert the deleted doc."""
        index = "it_delete_detect"
        _delete_index(es_config["host"], index)
        seed_docs(es_config["host"], index, [
            {"_id": "keep", "v": 1},
            {"_id": "remove", "v": 2},
        ])

        config = {**es_config, "indices": index, "enable_delete_detection": "true"}

        first_result = run_update(config, {})
        first_state = first_result.last_state

        delete_doc(es_config["host"], index, "remove")

        second_result = run_update(config, first_state)
        upserted_ids = {u["data"]["_id"] for u in second_result.upserts}

        # "remove" was deleted so it should not appear in upserts
        assert "remove" not in upserted_ids

    def test_deleted_doc_appears_in_deletes(self, es_config):
        """After deletion, the doc ID should appear in the deletes list."""
        index = "it_delete_op"
        _delete_index(es_config["host"], index)
        seed_docs(es_config["host"], index, [
            {"_id": "keep2", "v": 1},
            {"_id": "gone", "v": 2},
        ])

        config = {**es_config, "indices": index, "enable_delete_detection": "true"}

        first_result = run_update(config, {})
        first_state = first_result.last_state

        delete_doc(es_config["host"], index, "gone")

        second_result = run_update(config, first_state)
        deleted_ids = {d["keys"]["_id"] for d in second_result.deletes}

        assert "gone" in deleted_ids, (
            f"Expected 'gone' in deletes; got: {deleted_ids}"
        )
