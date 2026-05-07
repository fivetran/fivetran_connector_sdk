"""
Integration tests for the OpenSearch connector path.

These tests run against an OpenSearch 2.18.0 container via the opensearch_config
fixture. They verify that the distribution-specific PIT endpoints and sort
tiebreakers (_doc vs _shard_doc) work correctly end-to-end.

All tests are marked @pytest.mark.integration and are skipped automatically
when Docker is not available.
"""

import pytest
import requests

from tests.integration._helpers import run_update, seed_docs, delete_doc, create_index


pytestmark = pytest.mark.integration


# ─── helpers ──────────────────────────────────────────────────────────────────

def _make_docs(count: int) -> list:
    return [{"_id": str(i), "name": f"doc {i}", "value": i} for i in range(count)]


def _delete_index(host: str, index: str) -> None:
    requests.delete(f"{host}/{index}", timeout=10)


# ─── Test: initial full sync ───────────────────────────────────────────────────

class TestOpenSearchInitialSync:
    """Full sync against OpenSearch retrieves all documents."""

    def test_upserts_all_seeded_docs(self, opensearch_config):
        index = "os_initial_sync"
        _delete_index(opensearch_config["host"], index)
        seed_docs(opensearch_config["host"], index, _make_docs(5))

        config = {**opensearch_config, "indices": index}
        result = run_update(config, {})

        upserted_ids = {u["data"]["_id"] for u in result.upserts}
        assert upserted_ids == {str(i) for i in range(5)}

    def test_table_name_matches_index(self, opensearch_config):
        index = "os_table_name"
        _delete_index(opensearch_config["host"], index)
        seed_docs(opensearch_config["host"], index, [{"_id": "1", "v": 1}])

        config = {**opensearch_config, "indices": index}
        result = run_update(config, {})

        assert result.upserts
        for u in result.upserts:
            assert u["table"] == index

    def test_checkpoint_is_emitted(self, opensearch_config):
        index = "os_checkpoint"
        _delete_index(opensearch_config["host"], index)
        seed_docs(opensearch_config["host"], index, [{"_id": "1"}])

        config = {**opensearch_config, "indices": index}
        result = run_update(config, {})

        assert result.checkpoints

    def test_state_has_last_max_seq_no(self, opensearch_config):
        index = "os_seq_no_state"
        _delete_index(opensearch_config["host"], index)
        seed_docs(opensearch_config["host"], index, [{"_id": "1"}, {"_id": "2"}])

        config = {**opensearch_config, "indices": index}
        result = run_update(config, {})

        state = result.last_state
        assert index in state
        assert "last_max_seq_no" in state[index]
        assert state[index]["last_max_seq_no"] >= 0


# ─── Test: incremental sync ───────────────────────────────────────────────────

class TestOpenSearchIncrementalSync:
    """Second-run sync picks up only documents added after the last checkpoint."""

    def test_incremental_picks_up_new_docs(self, opensearch_config):
        index = "os_incremental"
        _delete_index(opensearch_config["host"], index)
        seed_docs(opensearch_config["host"], index, [{"_id": "0", "v": 0}])

        config = {**opensearch_config, "indices": index}
        first_result = run_update(config, {})
        first_state = first_result.last_state

        seed_docs(opensearch_config["host"], index, [{"_id": "1", "v": 1}])

        second_result = run_update(config, first_state)
        upserted_ids = {u["data"]["_id"] for u in second_result.upserts}

        assert "1" in upserted_ids
        assert "0" not in upserted_ids

    def test_no_new_docs_means_no_upserts(self, opensearch_config):
        index = "os_no_new_docs"
        _delete_index(opensearch_config["host"], index)
        seed_docs(opensearch_config["host"], index, [{"_id": "1", "v": 1}])

        config = {**opensearch_config, "indices": index}
        first_result = run_update(config, {})
        second_result = run_update(config, first_result.last_state)

        assert second_result.upserts == []


# ─── Test: delete detection ───────────────────────────────────────────────────

class TestOpenSearchDeleteDetection:
    """Delete detection emits op.delete for removed documents on OpenSearch."""

    def test_deleted_doc_appears_in_deletes(self, opensearch_config):
        index = "os_delete_detect"
        _delete_index(opensearch_config["host"], index)
        seed_docs(opensearch_config["host"], index, [
            {"_id": "keep", "v": 1},
            {"_id": "gone", "v": 2},
        ])

        config = {**opensearch_config, "indices": index, "enable_delete_detection": "true"}
        first_result = run_update(config, {})
        first_state = first_result.last_state

        delete_doc(opensearch_config["host"], index, "gone")

        second_result = run_update(config, first_state)
        deleted_ids = {d["keys"]["_id"] for d in second_result.deletes}

        assert "gone" in deleted_ids, (
            f"Expected 'gone' in deletes; got: {deleted_ids}"
        )


# ─── Test: multi-index sync ───────────────────────────────────────────────────

class TestOpenSearchMultiIndexSync:
    """Multiple indices sync in a single pass against OpenSearch."""

    def test_all_specified_indices_are_synced(self, opensearch_config):
        idx_a = "os_multi_a"
        idx_b = "os_multi_b"
        for idx in (idx_a, idx_b):
            _delete_index(opensearch_config["host"], idx)
            seed_docs(opensearch_config["host"], idx, [{"_id": "1", "src": idx}])

        config = {**opensearch_config, "indices": f"{idx_a},{idx_b}"}
        result = run_update(config, {})

        tables_synced = {u["table"] for u in result.upserts}
        assert idx_a in tables_synced
        assert idx_b in tables_synced


# ─── Test: system index exclusion ─────────────────────────────────────────────

class TestOpenSearchSystemIndexExclusion:
    """Dot-prefixed indices are excluded from sync on OpenSearch too."""

    def test_system_index_is_excluded(self, opensearch_config):
        host = opensearch_config["host"]
        _delete_index(host, "os_system_normal")
        seed_docs(host, "os_system_normal", [{"_id": "1", "v": 1}])

        # Attempt to create a dot-prefixed index; may be silently rejected
        create_index(host, ".os_system_test")

        config = {**opensearch_config, "indices": "all"}
        result = run_update(config, {})

        tables_synced = {u["table"] for u in result.upserts}
        assert "os_system_normal" in tables_synced
        dot_tables = {t for t in tables_synced if t.startswith(".")}
        assert not dot_tables, f"System indices should be excluded from sync: {dot_tables}"
