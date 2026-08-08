import json
import os
import subprocess
import sys
from pathlib import Path

from backend.services.ask_user_settings import (
    _SSH_GUARD_MARKER,
    ensure_ask_user_hook,
)
from backend.services.task_ssh_access import task_ssh_policy_context


_HOOK = Path(__file__).resolve().parents[1] / "hooks" / "task_ssh_guard_hook.py"


def _run_hook(
    *,
    tool_name: str,
    tool_input: dict,
    protected_path: Path,
    enabled: bool = True,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    if enabled:
        env["CCM_TASK_SSH_GUARD"] = "1"
    else:
        env.pop("CCM_TASK_SSH_GUARD", None)
    return subprocess.run(
        [
            sys.executable,
            str(_HOOK),
            "--protected-path",
            str(protected_path),
        ],
        input=json.dumps({
            "tool_name": tool_name,
            "tool_input": tool_input,
            "cwd": str(cwd) if cwd is not None else None,
        }),
        capture_output=True,
        text=True,
        timeout=10,
        env=env,
        check=False,
    )


def _decision(result: subprocess.CompletedProcess) -> str | None:
    if not result.stdout.strip():
        return None
    return json.loads(result.stdout)["hookSpecificOutput"][
        "permissionDecision"
    ]


def test_guard_denies_ambient_ssh_paths_and_direct_clients(tmp_path):
    protected = tmp_path / "managed" / "key.pem"

    for tool_name, tool_input in (
        ("Bash", {"command": "ls -la ~/.ssh"}),
        ("Bash", {"command": "/usr/bin/ssh ubuntu@example.test true"}),
        ("Bash", {"command": f"cat {protected}"}),
        ("Read", {"file_path": str(protected)}),
    ):
        result = _run_hook(
            tool_name=tool_name,
            tool_input=tool_input,
            protected_path=protected,
        )
        assert result.returncode == 0
        assert _decision(result) == "deny"
        assert "ccm_ssh.list_connections" in result.stdout

    relative = _run_hook(
        tool_name="Bash",
        tool_input={"command": "cat ../../managed/key.pem"},
        protected_path=protected,
        cwd=tmp_path / "workspace" / "nested",
    )
    assert _decision(relative) == "deny"


def test_guard_allows_unrelated_code_work_and_is_inactive_outside_ccm(tmp_path):
    protected = tmp_path / "managed" / "key.pem"

    ordinary = _run_hook(
        tool_name="Bash",
        tool_input={"command": "rg -n ssh backend"},
        protected_path=protected,
    )
    inactive = _run_hook(
        tool_name="Bash",
        tool_input={"command": "ssh example.test"},
        protected_path=protected,
        enabled=False,
    )

    assert ordinary.returncode == 0 and ordinary.stdout == ""
    assert inactive.returncode == 0 and inactive.stdout == ""


def test_claude_settings_injects_one_task_ssh_guard(tmp_path):
    protected = str(tmp_path / "key with spaces.pem")

    assert ensure_ask_user_hook(
        str(tmp_path),
        ssh_guard=True,
        ssh_protected_paths=(protected,),
    )
    assert ensure_ask_user_hook(
        str(tmp_path),
        ssh_guard=True,
        ssh_protected_paths=(protected,),
    )
    # A concurrent/non-SSH launch must not remove the dormant guard from the
    # shared account settings. It activates only through the Task env flag.
    assert ensure_ask_user_hook(str(tmp_path))

    data = json.loads((tmp_path / "settings.json").read_text())
    guards = [
        entry
        for entry in data["hooks"]["PreToolUse"]
        if any(
            _SSH_GUARD_MARKER in hook.get("command", "")
            for hook in entry.get("hooks", [])
        )
    ]
    assert len(guards) == 1
    assert "--protected-path" in guards[0]["hooks"][0]["command"]
    assert "key with spaces.pem" in guards[0]["hooks"][0]["command"]


def test_task_ssh_policy_makes_managed_list_authoritative():
    policy = task_ssh_policy_context(["read", "exec", "read"])

    assert "capabilities: exec, read" in policy
    assert "ccm_ssh.list_connections" in policy
    assert "known_hosts" in policy
    assert "complete authorized connection list" in policy
    assert task_ssh_policy_context([]) == ""
