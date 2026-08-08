import json
import os
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.config import settings
from backend.services.task_agent_isolation import (
    CLAUDE_MONITOR_BUILTIN_TOOLS,
    CLAUDE_SUB_AGENT_BUILTIN_TOOLS,
    CLAUDE_TASK_BUILTIN_TOOLS,
    TaskAgentIsolationError,
    generate_claude_aux_isolation_settings,
    generate_claude_task_isolation_settings,
    validate_claude_task_isolation_settings,
)
from backend.services.task_ssh_access import _protected_path_variants


def test_protected_path_variants_expand_environment_variables(
    tmp_path,
    monkeypatch,
):
    credential_root = tmp_path / "credentials"
    monkeypatch.setenv("CCM_TEST_CREDENTIAL_ROOT", str(credential_root))

    variants = _protected_path_variants("$CCM_TEST_CREDENTIAL_ROOT/ssh")

    assert str(credential_root / "ssh") in variants


@pytest.mark.parametrize(
    ("namespace", "identifier", "generation"),
    [("monitor", 8, 3), ("sub-agent", 9, None)],
)
def test_claude_aux_isolation_has_no_main_task_hooks_and_can_close_network(
    tmp_path,
    monkeypatch,
    namespace,
    identifier,
    generation,
):
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    path = generate_claude_aux_isolation_settings(
        namespace=namespace,
        identifier=identifier,
        protected_paths=["/Users/operator/.ssh"],
        turn_generation=generation,
        disable_direct_network=True,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert "hooks" not in payload
    assert payload["sandbox"]["network"]["allowedDomains"] == []
    assert "/Users/operator/.ssh" in payload["sandbox"]["filesystem"][
        "denyRead"
    ]
    if generation is not None:
        assert path.name == f"claude-security-{generation}.json"
    assert set(CLAUDE_MONITOR_BUILTIN_TOOLS) == {"Bash", "Glob", "Grep", "Read"}
    assert "Agent" not in CLAUDE_SUB_AGENT_BUILTIN_TOOLS


def test_claude_task_isolation_denies_credentials_and_direct_ssh_network(
    tmp_path,
    monkeypatch,
):
    runtime_root = tmp_path / "runtime"
    monkeypatch.setattr(settings, "task_runtime_secret_dir", str(runtime_root))
    monkeypatch.setattr(settings, "ask_user_enabled", True)

    path = generate_claude_task_isolation_settings(
        31,
        ["/Users/operator/.ssh", "/private/ccm/profile.pem"],
        ssh_capabilities={"read"},
    )
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert payload["permissions"]["defaultMode"] == "acceptEdits"
    assert payload["permissions"]["disableBypassPermissionsMode"] == "disable"
    assert "Read(//Users/operator/.ssh/**)" in payload["permissions"]["deny"]
    assert payload["sandbox"]["failIfUnavailable"] is True
    assert payload["sandbox"]["allowUnsandboxedCommands"] is False
    assert payload["sandbox"]["network"] == {
        "strictAllowlist": True,
        "allowedDomains": [],
        "deniedDomains": [],
        "allowAllUnixSockets": False,
        "allowLocalBinding": False,
    }
    assert "/Users/operator/.ssh" in payload["sandbox"]["filesystem"][
        "denyRead"
    ]
    assert str(runtime_root) in payload["sandbox"]["filesystem"]["denyRead"]
    commands = [
        hook["command"]
        for entry in payload["hooks"]["PreToolUse"]
        for hook in entry["hooks"]
    ]
    assert any("ask_user_hook.py" in command for command in commands)
    assert any("task_ssh_guard_hook.py" in command for command in commands)
    assert all("AUTH_TOKEN" not in command for command in commands)
    assert all("--auth-token" not in command for command in commands)


def test_claude_task_isolation_keeps_general_network_without_ssh_grant(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )
    path = generate_claude_task_isolation_settings(
        32,
        ["/Users/operator/.ssh"],
        ssh_capabilities=(),
    )

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["sandbox"]["network"]["allowedDomains"] == ["*"]
    assert not any(
        "task_ssh_guard_hook.py" in hook["command"]
        for entry in payload.get("hooks", {}).get("PreToolUse", [])
        for hook in entry["hooks"]
    )


def test_claude_task_isolation_rejects_root_as_protected_path(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        settings,
        "task_runtime_secret_dir",
        str(tmp_path / "runtime"),
    )

    with pytest.raises(TaskAgentIsolationError, match="filesystem root"):
        generate_claude_task_isolation_settings(33, ["/"])


def test_claude_isolation_preflight_scrubs_manager_tokens(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "settings.json"
    path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("AUTH_TOKEN", "deployment-secret")
    monkeypatch.setenv("CCM_INTERNAL_SERVICE_TOKEN", "internal-secret")
    monkeypatch.setenv("CCM_ASK_USER_TOKEN", "task-secret")
    captured = {}

    def fake_run(argv, **kwargs):
        captured["argv"] = argv
        captured["env"] = kwargs["env"]
        return SimpleNamespace(
            returncode=0,
            stdout="Claude Code doctor\nEverything looks healthy",
        )

    monkeypatch.setattr("subprocess.run", fake_run)

    validate_claude_task_isolation_settings(path, claude_binary="claude")

    assert captured["argv"] == [
        "claude",
        "--settings",
        str(path),
        "--setting-sources",
        "",
        "--strict-mcp-config",
        "doctor",
    ]
    assert "AUTH_TOKEN" not in captured["env"]
    assert "CCM_INTERNAL_SERVICE_TOKEN" not in captured["env"]
    assert "CCM_ASK_USER_TOKEN" not in captured["env"]


def test_task_claude_wrapper_is_private_and_uses_exact_cli_boundary():
    wrapper = Path(__file__).resolve().parents[1] / "services" / "task_claude_wrapper.sh"

    text = wrapper.read_text(encoding="utf-8")

    assert os.access(wrapper, os.X_OK)
    assert stat.S_IMODE(wrapper.stat().st_mode) & 0o022 == 0
    assert "--setting-sources \"\"" in text
    assert "--strict-mcp-config" in text
    assert "--permission-mode acceptEdits" in text
    assert "--dangerously-skip-permissions" not in text
    assert set(CLAUDE_TASK_BUILTIN_TOOLS) == {
        "AskUserQuestion",
        "Bash",
        "Edit",
        "Glob",
        "Grep",
        "MultiEdit",
        "NotebookEdit",
        "Read",
        "Write",
    }
