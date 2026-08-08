"""Fail-closed filesystem/network policy for local model Task processes."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Iterable

from backend.services.mcp_config import (
    CCM_MONITOR_AGENT_TOOLS,
    CCM_SKILLS_TOOLS,
    CCM_SSH_TOOLS,
    CCM_SUB_AGENT_TOOLS,
)
from backend.services.task_runtime_secrets import (
    runtime_secret_root,
    write_private_json,
)


CLAUDE_TASK_BUILTIN_TOOLS = (
    "AskUserQuestion",
    "Bash",
    "Edit",
    "Glob",
    "Grep",
    "MultiEdit",
    "NotebookEdit",
    "Read",
    "Write",
)

CLAUDE_MONITOR_BUILTIN_TOOLS = (
    "Bash",
    "Glob",
    "Grep",
    "Read",
)

CLAUDE_SUB_AGENT_BUILTIN_TOOLS = (
    "Bash",
    "Edit",
    "Glob",
    "Grep",
    "MultiEdit",
    "NotebookEdit",
    "Read",
    "Write",
)


class TaskAgentIsolationError(RuntimeError):
    """The provider could not prove the required Task isolation boundary."""


def _canonical_protected_paths(values: Iterable[str]) -> tuple[str, ...]:
    paths: set[str] = set()
    for value in values:
        if not isinstance(value, str) or not value.strip():
            continue
        expanded = os.path.expandvars(os.path.expanduser(value))
        if not os.path.isabs(expanded):
            raise TaskAgentIsolationError(
                "Task credential protection requires absolute paths"
            )
        path = os.path.abspath(expanded)
        if path == os.path.sep:
            raise TaskAgentIsolationError(
                "Task credential protection cannot target filesystem root"
            )
        paths.add(path)
        try:
            paths.add(os.path.realpath(path))
        except OSError:
            pass
    paths.add(str(runtime_secret_root()))
    if os.name == "posix" and Path("/proc").is_dir():
        paths.add("/proc")
    return tuple(sorted(paths))


def _permission_path(path: str) -> str:
    return f"//{path.lstrip('/')}"


def _mcp_allow_rules() -> list[str]:
    servers = {
        "ccm_skills": CCM_SKILLS_TOOLS,
        "ccm_ssh": CCM_SSH_TOOLS,
        "ccm_monitor_agent": CCM_MONITOR_AGENT_TOOLS,
        "ccm_sub_agent": CCM_SUB_AGENT_TOOLS,
    }
    return [
        f"mcp__{server}__{tool}"
        for server, tools in servers.items()
        for tool in tools
    ]


def _generate_claude_isolation_settings(
    *,
    namespace: str,
    identifier: int,
    filename: str,
    protected_paths: Iterable[str],
    ssh_capabilities: Iterable[str] = (),
    include_task_hooks: bool,
) -> Path:
    from backend.config import settings
    from backend.services.ask_user_settings import (
        ask_user_hook_entry,
        task_ssh_guard_hook_entry,
    )

    paths = _canonical_protected_paths(protected_paths)
    capabilities = set(ssh_capabilities) & {"exec", "read", "write"}
    hooks = []
    if include_task_hooks and settings.ask_user_enabled:
        hooks.append(ask_user_hook_entry())
    if include_task_hooks and capabilities:
        hooks.append(task_ssh_guard_hook_entry(paths))

    permission_denies: list[str] = []
    for path in paths:
        rule_path = _permission_path(path)
        permission_denies.extend((
            f"Read({rule_path})",
            f"Read({rule_path}/**)",
            f"Edit({rule_path})",
            f"Edit({rule_path}/**)",
        ))

    payload: dict[str, object] = {
        "showThinkingSummaries": True,
        "disableAutoMode": "disable",
        "disableAgentView": True,
        "disableRemoteControl": True,
        "disableSkillShellExecution": True,
        "permissions": {
            "defaultMode": "acceptEdits",
            "disableBypassPermissionsMode": "disable",
            "allow": _mcp_allow_rules(),
            "deny": permission_denies,
        },
        "sandbox": {
            "enabled": True,
            "failIfUnavailable": True,
            "autoAllowBashIfSandboxed": True,
            "allowUnsandboxedCommands": False,
            "excludedCommands": [],
            "filesystem": {
                "denyRead": list(paths),
                "denyWrite": list(paths),
            },
            "credentials": {
                "files": [
                    {"path": path, "mode": "deny"}
                    for path in paths
                ],
            },
            "network": {
                "strictAllowlist": True,
                "allowedDomains": [] if capabilities else ["*"],
                "deniedDomains": [],
                "allowAllUnixSockets": False,
                "allowLocalBinding": False,
            },
        },
    }
    if hooks:
        payload["hooks"] = {"PreToolUse": hooks}
    return write_private_json(
        namespace,
        identifier,
        filename,
        payload,
    )


def generate_claude_task_isolation_settings(
    task_id: int,
    protected_paths: Iterable[str],
    *,
    ssh_capabilities: Iterable[str] = (),
) -> Path:
    """Write exact CLI settings for one direct Claude Task turn.

    Account/project/local settings are disabled separately on argv. These
    settings therefore cannot be weakened by a repository-controlled
    ``allowRead`` entry or an ambient hook/plugin/MCP server.
    """

    return _generate_claude_isolation_settings(
        namespace="task",
        identifier=task_id,
        filename="claude-security.json",
        protected_paths=protected_paths,
        ssh_capabilities=ssh_capabilities,
        include_task_hooks=True,
    )


def generate_claude_aux_isolation_settings(
    *,
    namespace: str,
    identifier: int,
    protected_paths: Iterable[str],
    turn_generation: int | None = None,
    disable_direct_network: bool = False,
) -> Path:
    """Write exact settings for a Monitor or Sub-Agent Claude child.

    Auxiliary children use their own scoped MCP callback and never inherit the
    main Task's AskUser/SSH hooks.  A parent Task with managed SSH grants also
    disables direct child networking, so the grant cannot be bypassed through
    an independently launched child.
    """

    if namespace not in {"monitor", "sub-agent"}:
        raise ValueError("Unsupported Claude auxiliary isolation namespace")
    filename = (
        f"claude-security-{turn_generation}.json"
        if turn_generation is not None
        else "claude-security.json"
    )
    return _generate_claude_isolation_settings(
        namespace=namespace,
        identifier=identifier,
        filename=filename,
        protected_paths=protected_paths,
        # Reuse the network fail-closed branch without installing the main
        # Task SSH hook into this independent callback process.
        ssh_capabilities=("exec",) if disable_direct_network else (),
        include_task_hooks=False,
    )


def validate_claude_task_isolation_settings(
    settings_path: Path,
    *,
    claude_binary: str,
    timeout_seconds: float = 15.0,
) -> None:
    """Make the installed CLI parse the security file before model input.

    Claude print mode otherwise ignores an invalid settings file. ``doctor``
    currently reports validation failures with exit code zero, so both output
    and process status are checked.
    """

    env = {
        key: value
        for key, value in os.environ.items()
        if key.upper() not in {
            "AUTH_TOKEN",
            "CCM_INTERNAL_SERVICE_TOKEN",
            "CCM_ASK_USER_TOKEN",
            "CLAUDECODE",
            "CLAUDE_CODE",
        }
    }
    try:
        result = subprocess.run(
            [
                claude_binary,
                "--settings",
                str(settings_path),
                "--setting-sources",
                "",
                "--strict-mcp-config",
                "doctor",
            ],
            cwd=os.path.sep,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise TaskAgentIsolationError(
            "Claude Task isolation settings could not be validated"
        ) from exc
    output = result.stdout or ""
    if result.returncode != 0 or "invalid settings" in output.lower():
        raise TaskAgentIsolationError(
            "Claude rejected the required Task isolation settings"
        )
    if "claude code doctor" not in output.lower():
        raise TaskAgentIsolationError(
            "Claude did not confirm the Task isolation settings preflight"
        )
