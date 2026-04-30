"""Integration tests for error paths in the Cortex Agent streaming flow.

Hazard #6 (api-profiling.md): every code path that reads from a streaming
response must close it in `finally`. Hazard verifies via:
- TestStreamingResponseClosedInFinally — close() called even when iter_lines()
  raises mid-stream.

Hazard #6 also requires specific exception types only — no `except Exception:`.
Verified via TestExceptionSpecificity.

Plus the standard happy-path / failure-path coverage for call_cortex_agent().
"""

import json
from unittest.mock import MagicMock

import pytest
import requests as _requests

import connector


def _make_streaming_response(content_chunks, status_code=200):
    """Build a mock requests.Response that yields SSE-formatted lines."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.raise_for_status.return_value = None

    def iter_lines():
        # Combine all chunks into a single SSE blob with one event:done at the end.
        full_text = "".join(content_chunks)
        # Emit it as one data: line for simplicity (the parser concatenates).
        sse = "data: " + json.dumps({"choices": [{"delta": {"content": full_text}}]})
        for line in sse.split("\n"):
            yield line.encode("utf-8")
        yield b"event: done"

    resp.iter_lines = iter_lines
    resp.close = MagicMock()
    return resp


class TestCallCortexAgentHappyPath:
    def test_returns_parsed_json(self, base_config, monkeypatch, sample_discovery_response):
        resp = _make_streaming_response([json.dumps(sample_discovery_response)])

        def fake_post(*args, **kwargs):
            return resp

        monkeypatch.setattr(connector.requests, "post", fake_post)
        result = connector.call_cortex_agent(base_config, "test prompt")
        assert result == sample_discovery_response

    def test_strips_markdown_fences(self, base_config, monkeypatch):
        fenced = "```json\n" + json.dumps({"key": "value"}) + "\n```"
        resp = _make_streaming_response([fenced])
        monkeypatch.setattr(connector.requests, "post", lambda *a, **kw: resp)
        result = connector.call_cortex_agent(base_config, "test prompt")
        assert result == {"key": "value"}

    def test_uses_pooled_session_when_provided(
        self, base_config, monkeypatch, sample_discovery_response
    ):
        """If a cortex_session is passed, the connector posts via session.post,
        not via the module-level requests.post — connection-pooling contract."""
        resp = _make_streaming_response([json.dumps(sample_discovery_response)])
        session = MagicMock()
        session.post.return_value = resp
        # If the connector forgets the session and falls through to requests.post,
        # this fake will raise to make it visible.
        monkeypatch.setattr(connector.requests, "post", _raises_if_called)

        result = connector.call_cortex_agent(base_config, "test prompt", cortex_session=session)
        assert result == sample_discovery_response
        assert session.post.called


def _raises_if_called(*args, **kwargs):
    raise AssertionError("requests.post called despite cortex_session being available")


class TestStreamingResponseClosedInFinally:
    """Hazard #6: stream=True responses must call response.close() in finally,
    even when iter_lines() raises mid-stream. Otherwise the underlying socket
    leaks and the worker eventually exhausts the connection pool."""

    def test_close_called_on_success(self, base_config, monkeypatch, sample_discovery_response):
        resp = _make_streaming_response([json.dumps(sample_discovery_response)])
        monkeypatch.setattr(connector.requests, "post", lambda *a, **kw: resp)
        connector.call_cortex_agent(base_config, "test prompt")
        assert resp.close.called, "close() must be called after successful iter_lines()"

    def test_close_called_when_iter_lines_raises(self, base_config, monkeypatch):
        """If iter_lines raises mid-stream, the finally block must still close."""
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status.return_value = None

        def boom_iter():
            yield b"data: partial"
            raise _requests.exceptions.ConnectionError("simulated reset")

        resp.iter_lines = boom_iter
        resp.close = MagicMock()
        monkeypatch.setattr(connector.requests, "post", lambda *a, **kw: resp)

        # The connector catches ConnectionError and returns None; either way,
        # close() must have been called.
        result = connector.call_cortex_agent(base_config, "test prompt")
        assert result is None
        assert resp.close.called, "close() must be called even when iter_lines raises"


class TestExceptionSpecificity:
    """Hazard #6 also requires specific exception types only. A programmer bug
    raised inside iter_lines (e.g., a bad attribute access in a refactor) must
    NOT be silently swallowed by a bare `except Exception:` — it has to surface
    so the outer update() logs it and Fivetran retries the sync.

    The connector has explicit catches for HTTPError, ConnectionError, Timeout,
    JSONDecodeError, and RequestException. ValueError must propagate."""

    def test_value_error_not_swallowed(self, base_config, monkeypatch):
        def fake_post(*args, **kwargs):
            raise ValueError("simulated programmer bug")

        monkeypatch.setattr(connector.requests, "post", fake_post)
        with pytest.raises(ValueError, match="simulated programmer bug"):
            connector.call_cortex_agent(base_config, "test prompt")


class TestCallCortexAgentFailures:
    def test_timeout_returns_none(self, base_config, monkeypatch):
        def fake_post(*args, **kwargs):
            raise _requests.exceptions.Timeout("simulated")

        monkeypatch.setattr(connector.requests, "post", fake_post)
        # The connector retries 3x then returns None
        assert connector.call_cortex_agent(base_config, "test prompt") is None

    def test_connection_error_returns_none(self, base_config, monkeypatch):
        def fake_post(*args, **kwargs):
            raise _requests.exceptions.ConnectionError("simulated")

        monkeypatch.setattr(connector.requests, "post", fake_post)
        assert connector.call_cortex_agent(base_config, "test prompt") is None

    def test_401_returns_none_no_retry(self, base_config, monkeypatch):
        post_count = [0]

        def fake_post(*args, **kwargs):
            post_count[0] += 1
            err_resp = MagicMock()
            err_resp.status_code = 401
            err_resp.text = "unauthorized"
            err = _requests.exceptions.HTTPError("401")
            err.response = err_resp
            raise err

        monkeypatch.setattr(connector.requests, "post", fake_post)
        assert connector.call_cortex_agent(base_config, "test prompt") is None
        # 401/403 are not retryable — must be one attempt only
        assert post_count[0] == 1

    def test_malformed_json_returns_none(self, base_config, monkeypatch):
        resp = _make_streaming_response(["not valid json at all"])
        monkeypatch.setattr(connector.requests, "post", lambda *a, **kw: resp)
        assert connector.call_cortex_agent(base_config, "test prompt") is None

    def test_empty_response_returns_none(self, base_config, monkeypatch):
        # iter_lines yields no SSE data lines
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        resp.iter_lines = lambda: iter([b"event: done"])
        resp.close = MagicMock()
        monkeypatch.setattr(connector.requests, "post", lambda *a, **kw: resp)
        assert connector.call_cortex_agent(base_config, "test prompt") is None


class TestRecommendedVehiclesTypeValidation:
    """Hazard: LLM output is untrusted. If `recommended_vehicles` is not a
    list (e.g., the model returned a string or a dict), the connector must
    not crash — it must skip the discovery fetches and continue."""

    def test_string_recommendation_does_not_crash(
        self, base_config, captured_upserts, monkeypatch, recall_batch, complaint_batch
    ):
        bad_response = {
            "top_components": [],
            "recommended_vehicles": "Toyota Sequoia",  # WRONG: string instead of list
            "analysis_summary": "x",
        }
        monkeypatch.setattr(connector, "fetch_recalls", lambda *a, **kw: recall_batch)
        monkeypatch.setattr(connector, "fetch_complaints", lambda *a, **kw: complaint_batch)
        monkeypatch.setattr(connector, "fetch_vehicle_specs", lambda *a, **kw: [])
        monkeypatch.setattr(connector, "call_cortex_agent", lambda *a, **kw: bad_response)
        # Should complete without raising
        connector.update(base_config, {})
        # Synthesis runs only if vehicles_investigated > 1; with bad rec we
        # only have the seed, so synthesis is skipped — that's OK.
