"""Integration tests for error paths in HTTP-touching functions.

Mocks requests.Session to exercise the failure branches that
fivetran debug doesn't touch on the happy path.
"""

from unittest.mock import MagicMock

import pytest

import connector


@pytest.fixture
def mock_session():
    return MagicMock()


def _make_response(json_payload, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_payload
    resp.raise_for_status.return_value = None
    return resp


class TestCallAiQueryStatementIdGuard:
    """Fix #570-7 (class): when the initial response has state=PENDING/RUNNING
    but no statement_id, the connector must NOT poll a `.../None` URL."""

    def test_pending_with_missing_statement_id_returns_none_without_polling(
        self, base_config, mock_session, monkeypatch
    ):
        mock_session.post.return_value = _make_response({"status": {"state": "RUNNING"}})
        mock_session.get.return_value = _make_response(
            {
                "status": {"state": "SUCCEEDED"},
                "result": {"data_array": [["should-not-be-returned"]]},
            }
        )
        monkeypatch.setattr(connector.time, "sleep", lambda *_: None)

        result = connector.call_ai_query(mock_session, base_config, "test prompt")

        assert (
            result is None
        ), "Expected None when initial response is PENDING/RUNNING with no statement_id"
        assert mock_session.get.call_count == 0, (
            f"Expected zero GET requests (no polling allowed), "
            f"got {mock_session.get.call_count}"
        )

    def test_succeeded_with_missing_statement_id_still_returns_data(
        self, base_config, mock_session, monkeypatch
    ):
        mock_session.post.return_value = _make_response(
            {
                "status": {"state": "SUCCEEDED"},
                "result": {"data_array": [["happy-path-result"]]},
            }
        )
        monkeypatch.setattr(connector.time, "sleep", lambda *_: None)

        result = connector.call_ai_query(mock_session, base_config, "test prompt")

        assert result == "happy-path-result"
        assert mock_session.get.call_count == 0


class TestCallAiQueryWithStatementId:
    def test_pending_then_succeeded_polls_and_returns_data(
        self, base_config, mock_session, monkeypatch
    ):
        mock_session.post.return_value = _make_response(
            {"status": {"state": "PENDING"}, "statement_id": "stmt-abc-123"}
        )
        mock_session.get.return_value = _make_response(
            {
                "status": {"state": "SUCCEEDED"},
                "result": {"data_array": [["polled-result"]]},
            }
        )
        monkeypatch.setattr(connector.time, "sleep", lambda *_: None)

        result = connector.call_ai_query(mock_session, base_config, "test prompt")

        assert result == "polled-result"
        first_call_url = mock_session.get.call_args_list[0][0][0]
        assert "stmt-abc-123" in first_call_url
        assert "None" not in first_call_url


class TestNoEventsReturnedNoCrash:
    """When OpenFDA returns zero events (e.g., narrow filter, brand-new
    sync window), update() should checkpoint and return cleanly without
    invoking the debate phase."""

    def test_empty_events_short_circuits(self, base_config, captured_upserts, monkeypatch):
        ai_calls = []

        def fake_call_ai_query(session, configuration, prompt):
            ai_calls.append(prompt)
            return None

        def fake_fetch_adverse_events(session, search_query, limit, skip):
            return [], 0

        monkeypatch.setattr(connector, "call_ai_query", fake_call_ai_query)
        monkeypatch.setattr(connector, "fetch_adverse_events", fake_fetch_adverse_events)

        config = dict(base_config)
        config["enable_enrichment"] = "true"
        connector.update(config, {})

        assert len(ai_calls) == 0, "No events should mean no ai_query() calls"
        # Must still checkpoint (the SDK requires it before returning).
        assert len(captured_upserts["checkpoints"]) >= 1
