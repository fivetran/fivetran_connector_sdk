"""Connection-pooling tests for the Cortex Agent session.

Hazard #7 (api-profiling.md): when a connector makes multiple sequential
HTTP calls to the same host per record/sync, a single requests.Session must
be reused so TCP+TLS connections pool. PR #562 (NVD CVE) regressed on this
and the fix added _create_cortex_session() that's reused across discovery
and synthesis Cortex calls.

These tests verify:
- _create_cortex_session() returns a Session with the expected auth header
- The same session is reused across discovery + synthesis (not recreated per call)
- The session is closed via session.close() in update()'s flow
"""

from unittest.mock import MagicMock

import requests

import connector


class TestCreateCortexSession:
    def test_returns_requests_session(self, base_config):
        session = connector._create_cortex_session(base_config)
        assert isinstance(session, requests.Session)

    def test_sets_authorization_header(self, base_config):
        session = connector._create_cortex_session(base_config)
        auth = session.headers.get("Authorization", "")
        assert auth.startswith("Bearer "), f"Expected Bearer auth, got: {auth!r}"
        assert base_config["snowflake_pat_token"] in auth

    def test_sets_content_type_json(self, base_config):
        session = connector._create_cortex_session(base_config)
        assert session.headers.get("Content-Type") == "application/json"


class TestCortexSessionReusedAcrossPhases:
    """The same session must be passed to both discovery and synthesis
    Cortex calls. If two different sessions are seen, that's a regression to
    the pre-#562 pattern where every call created a fresh socket."""

    def test_session_object_id_is_constant_across_calls(
        self,
        base_config,
        captured_upserts,
        monkeypatch,
        sample_discovery_response,
        sample_synthesis_response,
        recall_batch,
        complaint_batch,
        spec_batch,
    ):
        seen_session_ids = set()

        def fake_call(configuration, prompt, cortex_session=None):
            if cortex_session is not None:
                seen_session_ids.add(id(cortex_session))
            if "AGGREGATE DATA" in prompt:
                return sample_synthesis_response
            return sample_discovery_response

        monkeypatch.setattr(connector, "call_cortex_agent", fake_call)
        monkeypatch.setattr(connector, "fetch_recalls", lambda *a, **kw: recall_batch)
        monkeypatch.setattr(connector, "fetch_complaints", lambda *a, **kw: complaint_batch)
        monkeypatch.setattr(connector, "fetch_vehicle_specs", lambda *a, **kw: spec_batch)

        connector.update(base_config, {})
        assert len(seen_session_ids) <= 1, (
            f"Cortex session must be reused across discovery + synthesis "
            f"(saw {len(seen_session_ids)} distinct session ids — regression risk)"
        )

    def test_session_close_is_called_after_phases(
        self,
        base_config,
        captured_upserts,
        monkeypatch,
        sample_discovery_response,
        sample_synthesis_response,
        recall_batch,
        complaint_batch,
        spec_batch,
    ):
        """After update() finishes the Cortex phases, the session must be
        explicitly closed to release pooled connections back to the OS."""
        # Replace _create_cortex_session with a tracked MagicMock
        tracked_session = MagicMock(spec=requests.Session)
        tracked_session.headers = {}
        monkeypatch.setattr(connector, "_create_cortex_session", lambda cfg: tracked_session)

        def fake_call(configuration, prompt, cortex_session=None):
            if "AGGREGATE DATA" in prompt:
                return sample_synthesis_response
            return sample_discovery_response

        monkeypatch.setattr(connector, "call_cortex_agent", fake_call)
        monkeypatch.setattr(connector, "fetch_recalls", lambda *a, **kw: recall_batch)
        monkeypatch.setattr(connector, "fetch_complaints", lambda *a, **kw: complaint_batch)
        monkeypatch.setattr(connector, "fetch_vehicle_specs", lambda *a, **kw: spec_batch)

        connector.update(base_config, {})
        assert (
            tracked_session.close.called
        ), "Cortex session must be explicitly closed after discovery+synthesis"
