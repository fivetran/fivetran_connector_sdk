"""Integration tests for error paths in the Cortex streaming HTTP code.

Streaming responses must be closed in `finally` (PR #562 lesson).
Specific exception types only — no `except Exception:` (PR #562, PR #566).

# see snowflake-cortex-code-nvd-cve-threat-intelligence/tests/integration/test_error_paths.py
# for the canonical Cortex-side analogue.
"""

from unittest.mock import MagicMock

import pytest

import connector


def _make_streaming_response(sse_lines, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.iter_lines.return_value = iter(sse_lines)
    resp.raise_for_status.return_value = None
    return resp


class TestStreamingResponseClosedInFinally:
    """response.close() must run on every exit path (success, exception, empty)."""

    def test_close_called_on_happy_path(self, base_config, sample_optimist_response, monkeypatch):
        from tests.conftest import build_sse_lines

        sse = build_sse_lines(sample_optimist_response)
        resp = _make_streaming_response(sse)
        monkeypatch.setattr(connector.requests, "post", lambda *a, **kw: resp)

        result = connector.call_cortex_agent(base_config, "test prompt")
        assert result == sample_optimist_response
        assert resp.close.called

    def test_close_called_when_iter_lines_raises(self, base_config, monkeypatch):
        """When iter_lines raises mid-stream with a requests-compatible error,
        the inner finally must close() the response on each retry. After
        __MAX_RETRIES the outer handler returns None — but close() must have
        run on every response object the connector touched."""
        import requests as _requests

        responses_used = []

        def make_response():
            resp = MagicMock()
            resp.status_code = 200
            resp.raise_for_status.return_value = None

            def boom():
                yield b"data: partial"
                # Use the exception type the connector's outer except handles
                # (mid-stream connection drops surface as requests.exceptions.ConnectionError).
                raise _requests.exceptions.ConnectionError("simulated reset mid-stream")

            resp.iter_lines.return_value = boom()
            responses_used.append(resp)
            return resp

        monkeypatch.setattr(connector.requests, "post", lambda *a, **kw: make_response())

        result = connector.call_cortex_agent(base_config, "test prompt")
        assert result is None, "Connector should return None after retries exhausted"
        assert len(responses_used) >= 1
        for resp in responses_used:
            assert (
                resp.close.called
            ), "Streaming response must be closed even when iter_lines raises"

    def test_close_called_on_empty_response(self, base_config, monkeypatch):
        resp = _make_streaming_response([])
        monkeypatch.setattr(connector.requests, "post", lambda *a, **kw: resp)

        result = connector.call_cortex_agent(base_config, "test prompt")
        assert result is None
        assert resp.close.called


class TestExceptionSpecificity:
    """No `except Exception:` — programmer bugs must propagate."""

    def test_unexpected_exception_not_swallowed(self, base_config, monkeypatch):
        def explode(*a, **kw):
            raise ValueError("simulated programmer error")

        monkeypatch.setattr(connector.requests, "post", explode)
        with pytest.raises(ValueError, match="simulated programmer error"):
            connector.call_cortex_agent(base_config, "test prompt")

    def test_timeout_returns_none_after_retries(self, base_config, monkeypatch):
        import requests as _requests

        def timeout(*a, **kw):
            raise _requests.exceptions.Timeout("simulated timeout")

        monkeypatch.setattr(connector.requests, "post", timeout)
        # __MAX_RETRIES retries then None
        assert connector.call_cortex_agent(base_config, "test prompt") is None

    def test_connection_error_returns_none_after_retries(self, base_config, monkeypatch):
        import requests as _requests

        def conn_err(*a, **kw):
            raise _requests.exceptions.ConnectionError("simulated conn error")

        monkeypatch.setattr(connector.requests, "post", conn_err)
        assert connector.call_cortex_agent(base_config, "test prompt") is None

    def test_invalid_json_returns_none(self, base_config, monkeypatch):
        sse_lines = [b"data: this is not JSON at all", b"event: done"]
        resp = _make_streaming_response(sse_lines)
        monkeypatch.setattr(connector.requests, "post", lambda *a, **kw: resp)

        # The inner data-line JSON parse uses except json.JSONDecodeError: continue.
        # The OUTER agent_response will be empty after parsing; returns None.
        result = connector.call_cortex_agent(base_config, "test prompt")
        assert result is None


class TestEmptyResultsShortCircuit:
    """When ClinicalTrials.gov returns zero matching trials, update() must
    checkpoint and return cleanly without invoking Cortex phases."""

    def test_empty_trials_short_circuits(self, base_config, captured_upserts, monkeypatch):
        cortex_calls = []

        def fake_call_cortex(*args, **kwargs):
            cortex_calls.append(args)
            return None

        def fake_fetch_and_upsert(session, configuration, state):
            return {}

        monkeypatch.setattr(connector, "call_cortex_agent", fake_call_cortex)
        monkeypatch.setattr(connector, "fetch_and_upsert_seed_trials", fake_fetch_and_upsert)

        connector.update(base_config, {})
        assert len(cortex_calls) == 0
        assert len(captured_upserts["checkpoints"]) >= 1
