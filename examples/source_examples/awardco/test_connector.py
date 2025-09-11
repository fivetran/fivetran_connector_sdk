"""Tests for awardco-users-connector/connector.py using generated mock data.

This test does not require network or the real Fivetran SDK. It stubs the
`fivetran_connector_sdk` module, monkeypatches `requests.get` to return the
contents of `files/mock_users.json`, and verifies that connector.update:

- Calls `op.upsert` once per user record
- Emits a checkpoint with the max updated_at as `last_sync_time`
"""

from __future__ import annotations

import json
import sys
import types
import unittest
import importlib.util
from pathlib import Path
from typing import Any, Dict, List


BASE_DIR = Path(__file__).parent
CONNECTOR_PATH = BASE_DIR / "connector.py"
MOCK_DATA_PATH = BASE_DIR / "files" / "mock_users.json"


class SDKStub(types.ModuleType):
    """Minimal stub for fivetran_connector_sdk used by connector.py import."""

    class Connector:  # noqa: D401
        def __init__(self, update, schema):
            self.update = update
            self.schema = schema

        def debug(self, configuration=None):  # pragma: no cover
            return None

    class Logging:  # noqa: D401
        @staticmethod
        def info(msg: str):  # pragma: no cover
            pass

        @staticmethod
        def warning(msg: str):  # pragma: no cover
            pass

        @staticmethod
        def severe(msg: str):  # pragma: no cover
            pass

    class Operations:  # Placeholder; replaced in test with Recorder
        @staticmethod
        def upsert(*args, **kwargs):  # pragma: no cover
            pass

        @staticmethod
        def checkpoint(*args, **kwargs):  # pragma: no cover
            pass


class Recorder:
    """Records Operations calls for assertions."""

    def __init__(self):
        self.upserts: List[Dict[str, Any]] = []
        self.checkpoints: List[Dict[str, Any]] = []

    # Accept both positional and keyword forms
    def upsert(self, table=None, data=None, **kwargs):
        if data is None and "data" in kwargs:
            data = kwargs["data"]
        if table is None and "table" in kwargs:
            table = kwargs["table"]
        self.upserts.append({"table": table, "data": data})

    def checkpoint(self, state=None, **kwargs):
        if state is None and "state" in kwargs:
            state = kwargs["state"]
        self.checkpoints.append(state)


def load_connector_module():
    """Load connector.py with an SDK stub injected into sys.modules."""
    sys.modules.setdefault("fivetran_connector_sdk", SDKStub("fivetran_connector_sdk"))
    spec = importlib.util.spec_from_file_location("awardco_users_connector", str(CONNECTOR_PATH))
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


class MockResponse:
    def __init__(self, payload: Dict[str, Any], status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def json(self) -> Dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if not (200 <= self.status_code < 300):
            raise RuntimeError(f"HTTP {self.status_code}")


class ConnectorTests(unittest.TestCase):
    def setUp(self):
        with MOCK_DATA_PATH.open("r", encoding="utf-8") as f:
            self.mock_payload = json.load(f)

        # Load connector with stubbed SDK
        self.connector = load_connector_module()

        # Monkeypatch requests.get in the connector module
        def _mock_get(url, headers=None):
            return MockResponse(self.mock_payload)

        self.connector.requests.get = _mock_get

        # Swap in our recorder for Operations
        self.recorder = Recorder()
        self.connector.op = self.recorder

    def test_update_processes_mock_users_and_emits_checkpoint(self):
        config = {"api_key": "x", "base_url": "https://example.com"}
        state: Dict[str, Any] = {}

        # Run the update; connector.update is not a generator in this implementation
        self.connector.update(configuration=config, state=state)

        users = self.mock_payload.get("users", [])
        self.assertEqual(len(self.recorder.upserts), len(users))

        # Ensure all upserts target the user table and include employeeId
        for call in self.recorder.upserts:
            self.assertEqual(call["table"], "user")
            self.assertIn("employeeId", call["data"])

        # Check that a checkpoint was emitted with the max updated_at
        self.assertGreaterEqual(len(self.recorder.checkpoints), 1)
        last_state = self.recorder.checkpoints[-1]
        expected_last = max(
            (u.get("updated_at") for u in users if u.get("updated_at")), default=None
        )
        self.assertIsNotNone(expected_last)
        self.assertEqual(last_state.get("last_sync_time"), expected_last)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
