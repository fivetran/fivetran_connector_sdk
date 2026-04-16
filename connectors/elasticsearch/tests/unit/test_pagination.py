"""
Unit tests for PIT (Point-in-Time) helpers and the pit_page() pagination function.
Tests: open_pit(), close_pit(), pit_page()
"""
from unittest.mock import patch, call

import pytest
import connector


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _cfg():
    return {
        "host": "http://localhost:9200",
        "auth_method": "api_key",
        "api_key": "test-key",
    }


def _make_hit(doc_id: str, seq_no: int = 0, sort_val: int = 0, source: dict = None):
    return {
        "_id": doc_id,
        "_seq_no": seq_no,
        "_source": source or {"field": "value"},
        "sort": [sort_val],
    }


# ── open_pit ──────────────────────────────────────────────────────────────────

class TestOpenPit:
    """Tests for open_pit() — Elasticsearch vs OpenSearch branching."""

    @patch("connector.es_request")
    def test_elasticsearch_posts_to_pit_endpoint(self, mock_req):
        mock_req.return_value = {"id": "pit-abc-123"}
        result = connector.open_pit(_cfg(), "my_index", distribution="elasticsearch")
        assert result == "pit-abc-123"
        url_called = mock_req.call_args[0][1]
        assert url_called == "http://localhost:9200/my_index/_pit"
        assert mock_req.call_args[0][0] == "POST"

    @patch("connector.es_request")
    def test_opensearch_posts_to_point_in_time_endpoint(self, mock_req):
        mock_req.return_value = {"pit_id": "os-pit-xyz"}
        result = connector.open_pit(_cfg(), "my_index", distribution="opensearch")
        assert result == "os-pit-xyz"
        url_called = mock_req.call_args[0][1]
        assert url_called == "http://localhost:9200/my_index/_search/point_in_time"

    @patch("connector.es_request")
    def test_elasticsearch_keep_alive_param_is_set(self, mock_req):
        mock_req.return_value = {"id": "pit-id"}
        connector.open_pit(_cfg(), "idx", distribution="elasticsearch")
        params = mock_req.call_args[1].get("params") or mock_req.call_args[0][4] if len(mock_req.call_args[0]) > 4 else mock_req.call_args.kwargs.get("params")
        # Check keep_alive is passed
        call_kwargs = mock_req.call_args
        assert "keep_alive" in str(call_kwargs)

    @patch("connector.es_request")
    def test_default_distribution_is_elasticsearch(self, mock_req):
        mock_req.return_value = {"id": "pit-id"}
        connector.open_pit(_cfg(), "idx")  # no distribution arg
        url_called = mock_req.call_args[0][1]
        assert "_pit" in url_called
        assert "_search/point_in_time" not in url_called

    @patch("connector.es_request")
    def test_returns_string_id_not_full_response(self, mock_req):
        mock_req.return_value = {"id": "just-the-id", "extra": "ignored"}
        result = connector.open_pit(_cfg(), "idx", distribution="elasticsearch")
        assert result == "just-the-id"
        assert isinstance(result, str)


# ── close_pit ─────────────────────────────────────────────────────────────────

class TestClosePit:
    """Tests for close_pit() — URL and body differ between ES and OpenSearch."""

    @patch("connector.es_request")
    def test_elasticsearch_sends_delete_to_pit_url(self, mock_req):
        mock_req.return_value = {"succeeded": True}
        connector.close_pit(_cfg(), "my-pit-id", distribution="elasticsearch")
        assert mock_req.call_args[0][0] == "DELETE"
        assert mock_req.call_args[0][1] == "http://localhost:9200/_pit"

    @patch("connector.es_request")
    def test_elasticsearch_body_uses_id_key(self, mock_req):
        mock_req.return_value = {}
        connector.close_pit(_cfg(), "my-pit-id", distribution="elasticsearch")
        body = mock_req.call_args.kwargs.get("body") or mock_req.call_args[1].get("body")
        assert body == {"id": "my-pit-id"}

    @patch("connector.es_request")
    def test_opensearch_sends_delete_to_point_in_time_url(self, mock_req):
        mock_req.return_value = {}
        connector.close_pit(_cfg(), "os-pit-id", distribution="opensearch")
        assert mock_req.call_args[0][0] == "DELETE"
        assert "_search/point_in_time" in mock_req.call_args[0][1]

    @patch("connector.es_request")
    def test_opensearch_body_uses_pit_id_key(self, mock_req):
        mock_req.return_value = {}
        connector.close_pit(_cfg(), "os-pit-id", distribution="opensearch")
        body = mock_req.call_args.kwargs.get("body") or mock_req.call_args[1].get("body")
        assert body == {"pit_id": "os-pit-id"}

    @patch("connector.es_request")
    def test_close_pit_swallows_exceptions(self, mock_req):
        """close_pit must not raise — exceptions are logged and suppressed."""
        mock_req.side_effect = Exception("network error")
        # Should not raise
        connector.close_pit(_cfg(), "bad-pit", distribution="elasticsearch")

    @patch("connector.es_request")
    def test_default_distribution_is_elasticsearch(self, mock_req):
        mock_req.return_value = {}
        connector.close_pit(_cfg(), "pit-id")  # no distribution arg
        url_called = mock_req.call_args[0][1]
        assert url_called == "http://localhost:9200/_pit"


# ── pit_page ──────────────────────────────────────────────────────────────────

class TestPitPage:
    """Tests for pit_page() — builds the right request body and parses response."""

    @patch("connector.es_request")
    def test_returns_hits_and_pit_id(self, mock_req):
        hits = [_make_hit("doc1"), _make_hit("doc2")]
        mock_req.return_value = {
            "pit_id": "new-pit-id",
            "hits": {"hits": hits},
        }
        result_hits, result_pit = connector.pit_page(
            _cfg(), "old-pit-id", sort_fields=[{"_shard_doc": "asc"}]
        )
        assert result_hits == hits
        assert result_pit == "new-pit-id"

    @patch("connector.es_request")
    def test_uses_updated_pit_id_from_response(self, mock_req):
        """ES may rotate the PIT id; always use the latest."""
        mock_req.return_value = {
            "pit_id": "rotated-pit-id",
            "hits": {"hits": []},
        }
        _, returned_pit = connector.pit_page(
            _cfg(), "original-pit-id", sort_fields=[{"_shard_doc": "asc"}]
        )
        assert returned_pit == "rotated-pit-id"

    @patch("connector.es_request")
    def test_falls_back_to_input_pit_id_when_response_has_none(self, mock_req):
        mock_req.return_value = {"hits": {"hits": []}}  # no pit_id key
        _, returned_pit = connector.pit_page(
            _cfg(), "fallback-pit", sort_fields=[{"_shard_doc": "asc"}]
        )
        assert returned_pit == "fallback-pit"

    @patch("connector.es_request")
    def test_search_after_is_included_in_body_when_provided(self, mock_req):
        mock_req.return_value = {"pit_id": "p", "hits": {"hits": []}}
        connector.pit_page(
            _cfg(), "p", sort_fields=[{"_shard_doc": "asc"}], search_after=[42]
        )
        body_sent = mock_req.call_args[1].get("body") or mock_req.call_args[0][3]
        assert "search_after" in body_sent
        assert body_sent["search_after"] == [42]

    @patch("connector.es_request")
    def test_search_after_absent_when_none(self, mock_req):
        mock_req.return_value = {"pit_id": "p", "hits": {"hits": []}}
        connector.pit_page(
            _cfg(), "p", sort_fields=[{"_shard_doc": "asc"}], search_after=None
        )
        body_sent = mock_req.call_args[1].get("body") or mock_req.call_args[0][3]
        assert "search_after" not in body_sent

    @patch("connector.es_request")
    def test_default_query_is_match_all(self, mock_req):
        mock_req.return_value = {"pit_id": "p", "hits": {"hits": []}}
        connector.pit_page(_cfg(), "p", sort_fields=[{"_shard_doc": "asc"}])
        body_sent = mock_req.call_args[1].get("body") or mock_req.call_args[0][3]
        assert body_sent["query"] == {"match_all": {}}

    @patch("connector.es_request")
    def test_custom_query_overrides_match_all(self, mock_req):
        mock_req.return_value = {"pit_id": "p", "hits": {"hits": []}}
        custom_q = {"range": {"_seq_no": {"gt": 10}}}
        connector.pit_page(_cfg(), "p", sort_fields=[{"_shard_doc": "asc"}], query=custom_q)
        body_sent = mock_req.call_args[1].get("body") or mock_req.call_args[0][3]
        assert body_sent["query"] == custom_q

    @patch("connector.es_request")
    def test_request_sent_to_global_search_endpoint(self, mock_req):
        mock_req.return_value = {"pit_id": "p", "hits": {"hits": []}}
        connector.pit_page(_cfg(), "p", sort_fields=[{"_shard_doc": "asc"}])
        url_called = mock_req.call_args[0][1]
        assert url_called == "http://localhost:9200/_search"

    @patch("connector.es_request")
    def test_page_size_matches_constant(self, mock_req):
        mock_req.return_value = {"pit_id": "p", "hits": {"hits": []}}
        connector.pit_page(_cfg(), "p", sort_fields=[{"_shard_doc": "asc"}])
        body_sent = mock_req.call_args[1].get("body") or mock_req.call_args[0][3]
        assert body_sent["size"] == connector.PAGE_SIZE

    @patch("connector.es_request")
    def test_sort_fields_are_passed_through(self, mock_req):
        mock_req.return_value = {"pit_id": "p", "hits": {"hits": []}}
        sort = [{"@timestamp": "asc"}, {"_doc": "asc"}]
        connector.pit_page(_cfg(), "p", sort_fields=sort)
        body_sent = mock_req.call_args[1].get("body") or mock_req.call_args[0][3]
        assert body_sent["sort"] == sort

    @patch("connector.es_request")
    def test_seq_no_primary_term_is_requested(self, mock_req):
        mock_req.return_value = {"pit_id": "p", "hits": {"hits": []}}
        connector.pit_page(_cfg(), "p", sort_fields=[{"_shard_doc": "asc"}])
        body_sent = mock_req.call_args[1].get("body") or mock_req.call_args[0][3]
        assert body_sent.get("seq_no_primary_term") is True

    @patch("connector.es_request")
    def test_empty_hits_returns_empty_list(self, mock_req):
        mock_req.return_value = {"pit_id": "p", "hits": {"hits": []}}
        hits, _ = connector.pit_page(_cfg(), "p", sort_fields=[{"_shard_doc": "asc"}])
        assert hits == []

    @patch("connector.es_request")
    def test_multiple_hits_all_returned(self, mock_req):
        raw_hits = [_make_hit(f"doc{i}", sort_val=i) for i in range(5)]
        mock_req.return_value = {"pit_id": "p", "hits": {"hits": raw_hits}}
        hits, _ = connector.pit_page(_cfg(), "p", sort_fields=[{"_shard_doc": "asc"}])
        assert len(hits) == 5


# ── get_max_seq_no ────────────────────────────────────────────────────────────

class TestGetMaxSeqNo:
    """Tests for get_max_seq_no()."""

    @patch("connector.es_request")
    def test_returns_seq_no_from_top_hit(self, mock_req):
        mock_req.return_value = {
            "hits": {"hits": [{"_id": "1", "_seq_no": 42}]}
        }
        result = connector.get_max_seq_no(_cfg(), "my_index")
        assert result == 42

    @patch("connector.es_request")
    def test_returns_minus_one_for_empty_index(self, mock_req):
        mock_req.return_value = {"hits": {"hits": []}}
        result = connector.get_max_seq_no(_cfg(), "my_index")
        assert result == -1

    @patch("connector.es_request")
    def test_returns_minus_one_on_exception(self, mock_req):
        mock_req.side_effect = Exception("timeout")
        result = connector.get_max_seq_no(_cfg(), "my_index")
        assert result == -1

    @patch("connector.es_request")
    def test_posts_to_index_search_endpoint(self, mock_req):
        mock_req.return_value = {"hits": {"hits": []}}
        connector.get_max_seq_no(_cfg(), "target_index")
        url_called = mock_req.call_args[0][1]
        assert "target_index/_search" in url_called

    @patch("connector.es_request")
    def test_sorts_by_seq_no_descending(self, mock_req):
        mock_req.return_value = {"hits": {"hits": []}}
        connector.get_max_seq_no(_cfg(), "idx")
        body_sent = mock_req.call_args[1].get("body") or mock_req.call_args[0][3]
        assert {"_seq_no": "desc"} in body_sent["sort"]

    @patch("connector.es_request")
    def test_requests_size_one(self, mock_req):
        mock_req.return_value = {"hits": {"hits": []}}
        connector.get_max_seq_no(_cfg(), "idx")
        body_sent = mock_req.call_args[1].get("body") or mock_req.call_args[0][3]
        assert body_sent["size"] == 1
