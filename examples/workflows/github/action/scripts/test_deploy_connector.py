"""Run with: uv run --with pytest pytest action/scripts/test_deploy_connector.py

Mocks subprocess.Popen so these exercise the retry/decision logic in
deploy_connector.py without ever calling the real `fivetran` CLI.
"""

import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import deploy_connector as dc
import pytest


def test_derive_connection_name_sanitizes():
    assert dc.derive_connection_name(Path("csdk/meetup")) == "meetup"
    assert dc.derive_connection_name(Path("My Connector!")) == "my_connector_"
    assert dc.derive_connection_name(Path("123start")) == "_123start"


def fake_popen(output_lines: list[str], returncode: int):
    """Builds a MagicMock standing in for subprocess.Popen(...)."""
    process = MagicMock()
    process.stdout = io.StringIO("".join(output_lines))
    process.returncode = returncode
    process.wait = lambda: None
    return process


@pytest.fixture
def connector_dir(tmp_path):
    d = tmp_path / "connector"
    d.mkdir()
    (d / "configuration.example.json").write_text('{"api_key": "YOUR_KEY_HERE"}')
    return d


def set_env(monkeypatch, connector_dir, **overrides):
    values = {
        "CONNECTOR_DIR": str(connector_dir),
        "FIVETRAN_API_KEY": "dummy",
        "CONNECTION_NAME": "",
        "DESTINATION": "",
        "CONFIGURATION_JSON": "",
        "EMPTY_CONFIG_PATH": "configuration.example.json",
        "PYTHON_VERSION": "",
        "ACTIVATE_ON_CREATE": "",
    }
    values.update(overrides)
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def test_code_only_redeploy_of_existing_connection(monkeypatch, connector_dir):
    # A configuration.json left over from a previous run must not survive --
    # and must not be passed to the CLI -- on a code-only deploy.
    (connector_dir / "configuration.json").write_text('{"leftover": "true"}')
    set_env(monkeypatch, connector_dir)

    with patch("subprocess.Popen") as popen:
        popen.return_value = fake_popen(["connection updated\n"], returncode=0)
        assert dc.main() == 0

    cmd = popen.call_args.args[0]
    assert "--configuration" not in cmd
    assert not (connector_dir / "configuration.json").exists()


def test_first_deploy_retries_with_placeholder(monkeypatch, connector_dir):
    set_env(monkeypatch, connector_dir)
    calls = []

    def respond(cmd, **kwargs):
        if len(calls) == 0:
            calls.append(cmd)
            return fake_popen([f"{dc.FIRST_DEPLOY_ERROR}\n"], returncode=1)
        config_path = Path(cmd[cmd.index("--configuration") + 1])
        calls.append(config_path.read_text())
        return fake_popen(["connection created\n", "connection id: abc123\n"], returncode=0)

    with patch("subprocess.Popen", side_effect=respond) as popen:
        assert dc.main() == 0

    assert popen.call_count == 2
    assert "--configuration" not in calls[0]
    assert calls[1] == '{"api_key": "YOUR_KEY_HERE"}'  # copied from configuration.example.json


def test_first_deploy_placeholder_setup_tests_fail_is_a_warning_not_a_failure(
    monkeypatch, connector_dir
):
    set_env(monkeypatch, connector_dir)

    with patch("subprocess.Popen") as popen:
        popen.side_effect = [
            fake_popen([f"{dc.FIRST_DEPLOY_ERROR}\n"], returncode=1),
            fake_popen(
                [f"connection created but {dc.SETUP_TESTS_FAILED}\n", "connection id: abc123\n"],
                returncode=1,
            ),
        ]
        assert dc.main() == 0  # soft warning, not a hard CI failure


def test_activate_on_create_activates_a_newly_created_connection(monkeypatch, connector_dir):
    set_env(
        monkeypatch,
        connector_dir,
        CONFIGURATION_JSON='{"oauth_key_id": "real-value"}',
        ACTIVATE_ON_CREATE="true",
    )

    with patch("subprocess.Popen") as popen, patch("requests.patch") as patch_request:
        popen.return_value = fake_popen(
            ["connection created\n", "connection id: abc123\n"], returncode=0
        )
        patch_request.return_value = MagicMock(ok=True)
        assert dc.main() == 0

    patch_request.assert_called_once_with(
        f"{dc.FIVETRAN_API_BASE_URL}/connectors/abc123",
        headers={"Authorization": "Basic dummy"},
        json={"paused": False},
    )


def test_activate_on_create_does_not_touch_an_existing_connection(monkeypatch, connector_dir):
    # status "updated" -- this run didn't create the connection, so activation must be skipped.
    set_env(
        monkeypatch,
        connector_dir,
        CONFIGURATION_JSON='{"oauth_key_id": "real-value"}',
        ACTIVATE_ON_CREATE="true",
    )

    with patch("subprocess.Popen") as popen, patch("requests.patch") as patch_request:
        popen.return_value = fake_popen(["connection updated\n"], returncode=0)
        assert dc.main() == 0

    patch_request.assert_not_called()


def test_activate_on_create_skipped_for_placeholder_creation(monkeypatch, connector_dir):
    # Placeholder-config creation (setup tests fail as expected) must stay paused
    # even when activation was requested -- there are no real credentials yet.
    set_env(monkeypatch, connector_dir, ACTIVATE_ON_CREATE="true")

    with patch("subprocess.Popen") as popen, patch("requests.patch") as patch_request:
        popen.side_effect = [
            fake_popen([f"{dc.FIRST_DEPLOY_ERROR}\n"], returncode=1),
            fake_popen(
                [f"connection created but {dc.SETUP_TESTS_FAILED}\n", "connection id: abc123\n"],
                returncode=1,
            ),
        ]
        assert dc.main() == 0

    patch_request.assert_not_called()


def test_reports_active_state_on_redeploy_of_existing_connection(monkeypatch, connector_dir):
    # No activation requested, and this run didn't create the connection --
    # but the caller should still learn whether it's currently active.
    set_env(monkeypatch, connector_dir)

    with patch("subprocess.Popen") as popen, patch("requests.get") as get_request:
        popen.return_value = fake_popen(
            ["connection updated\n", "connection id: abc123\n"], returncode=0
        )
        get_request.return_value = MagicMock(ok=True, json=lambda: {"data": {"paused": False}})
        assert dc.main() == 0

    get_request.assert_called_once_with(
        f"{dc.FIVETRAN_API_BASE_URL}/connectors/abc123",
        headers={"Authorization": "Basic dummy"},
    )


def test_reports_paused_state_after_activation_fails(monkeypatch, connector_dir):
    set_env(
        monkeypatch,
        connector_dir,
        CONFIGURATION_JSON='{"oauth_key_id": "real-value"}',
        ACTIVATE_ON_CREATE="true",
    )

    with (
        patch("subprocess.Popen") as popen,
        patch("requests.patch") as patch_request,
        patch("requests.get") as get_request,
    ):
        popen.return_value = fake_popen(
            ["connection created\n", "connection id: abc123\n"], returncode=0
        )
        patch_request.return_value = MagicMock(ok=False, status_code=500, text="boom")
        get_request.return_value = MagicMock(ok=True, json=lambda: {"data": {"paused": True}})
        assert dc.main() == 0

    get_request.assert_called_once()


def test_configuration_json_is_pushed_in_one_call_when_provided(monkeypatch, connector_dir):
    set_env(
        monkeypatch,
        connector_dir,
        CONFIGURATION_JSON='{"oauth_key_id": "real-value"}',
    )

    written_content = []

    def capture_and_respond(cmd, **kwargs):
        config_path = Path(cmd[cmd.index("--configuration") + 1])
        written_content.append(config_path.read_text())
        return fake_popen(["connection updated\n"], returncode=0)

    with patch("subprocess.Popen", side_effect=capture_and_respond) as popen:
        assert dc.main() == 0

    assert popen.call_count == 1
    assert written_content == ['{"oauth_key_id": "real-value"}']
    # cleaned up afterwards, even on success
    assert not (connector_dir / "configuration.json").exists()
