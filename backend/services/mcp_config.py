"""CCM MCP server specs and provider-specific config generation.

The server description is provider-neutral.  Existing callers still receive a
Claude-compatible JSON file, while pure Codex renderers expose the same specs
without changing either provider's runtime task-launch path.
"""

import json
import math
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Sequence, cast


_CCM_ROOT = str(Path(__file__).resolve().parent.parent.parent)
# MCP servers are children of the running backend and need its exact dependency
# environment. Reusing that interpreter is portable across Linux venvs,
# container system Python, and Windows ``Scripts/python.exe``; constructing a
# repository-relative ``.venv/bin/python3`` path breaks Windows-hosted bind
# mounts even though the backend itself has all required packages.
_VENV_PYTHON = sys.executable
_MCP_STARTUP_TIMEOUT_SEC = 10.0
_MCP_TOOL_TIMEOUT_SEC = 60.0
_TOML_BARE_KEY_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_CODEX_MCP_SERVER_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")

CCM_SKILLS_TOOLS = (
    "ccm_command_help",
    "ccm_read_skill",
    "ccm_read_user_skill",
    "ccm_create_skill",
    "ccm_distill",
    "ccm_enable_skill",
    "ccm_disable_skill",
    "create_monitor",
    "check_monitors",
    "stop_monitor",
    "create_sub_agent",
    "check_sub_agents",
    "stop_sub_agent",
)
CCM_MONITOR_AGENT_TOOLS = (
    "report_status",
    "mark_complete",
    "get_context",
)
CCM_SUB_AGENT_TOOLS = (
    "report_progress",
    "submit_result",
    "get_context",
)
CCM_SUB_AGENT_CONTROLLER_TOOLS = (
    "ccm_read_skill",
    "create_sub_agent",
    "check_sub_agents",
    "stop_sub_agent",
)
CCM_SSH_TOOLS = (
    "list_connections",
    "run_command",
    "list_directory",
    "read_file",
    "write_file",
)
CCM_SSH_CAPABILITY_TOOLS = {
    "exec": ("run_command",),
    "read": ("list_directory", "read_file"),
    "write": ("write_file",),
}


@dataclass(frozen=True, slots=True)
class McpServerSpec:
    """Provider-neutral description of one stdio MCP server."""

    name: str
    command: str
    args: tuple[str, ...] = ()
    cwd: str | None = None
    env: Mapping[str, str] = field(default_factory=dict)
    required: bool = False
    enabled_tools: tuple[str, ...] = ()
    default_tools_approval_mode: str | None = None
    startup_timeout_sec: float | None = None
    tool_timeout_sec: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "args", tuple(self.args))
        object.__setattr__(self, "env", MappingProxyType(dict(self.env)))
        object.__setattr__(self, "enabled_tools", tuple(self.enabled_tools))


def _api_base(api_base: str | None) -> str:
    from backend.services.internal_api_endpoint import resolve_internal_api_base

    return resolve_internal_api_base(api_base)


def _ccm_server_spec(
    *,
    name: str,
    module: str,
    context_args: Sequence[str],
    enabled_tools: tuple[str, ...],
    api_base: str | None,
    task_id: int | None = None,
    monitor_session_id: int | None = None,
    sub_agent_session_id: int | None = None,
    credential_owner_kind: str,
    credential_owner_id: str | int,
) -> McpServerSpec:
    from backend.services.internal_service_auth import (
        INTERNAL_TOKEN_ENV,
        issue_internal_service_token,
    )

    resolved_api_base = _api_base(api_base)
    args = [
        # Claude Code does not consistently honor ``cwd`` for stdio MCP
        # entries. Safe-path mode plus an explicit PYTHONPATH pins imports to
        # the running Manager checkout instead of the Task repository.
        "-P",
        "-m",
        module,
        *context_args,
        "--api-base",
        resolved_api_base,
    ]
    scoped_token = issue_internal_service_token(
        audience=name,
        task_id=task_id,
        monitor_session_id=monitor_session_id,
        sub_agent_session_id=sub_agent_session_id,
        owner_kind=credential_owner_kind,
        owner_id=credential_owner_id,
    )
    server_env = {"PYTHONPATH": _CCM_ROOT}
    if scoped_token:
        server_env[INTERNAL_TOKEN_ENV] = scoped_token

    return McpServerSpec(
        name=name,
        command=_VENV_PYTHON,
        args=tuple(args),
        cwd=_CCM_ROOT,
        env=server_env,
        required=True,
        enabled_tools=enabled_tools,
        # These are CCM-owned, task-scoped tools whose handlers enforce the
        # exact Task/session/generation again. Codex app-server otherwise
        # treats approvalPolicy="never" as a user rejection at call time.
        default_tools_approval_mode="approve",
        startup_timeout_sec=_MCP_STARTUP_TIMEOUT_SEC,
        tool_timeout_sec=_MCP_TOOL_TIMEOUT_SEC,
    )


def build_mcp_server_specs(
    task_id: int,
    enabled_skills: dict | None = None,
    api_base: str | None = None,
    *,
    provider: str = "claude",
    codex_monitor_enabled: bool = False,
) -> tuple[McpServerSpec, ...]:
    """Build the main task's CCM MCP server specs.

    ``enabled_skills`` remains part of the public API for compatibility.  The
    unified ``ccm_skills`` server is always present and decides tool behaviour
    from task state at call time.
    """

    enabled_tools = CCM_SKILLS_TOOLS
    if (
        (provider or "claude").lower() == "codex"
        and not codex_monitor_enabled
    ):
        from backend.services.skill_context import (
            CODEX_UNSUPPORTED_MAIN_TOOLS,
        )

        enabled_tools = tuple(
            tool
            for tool in CCM_SKILLS_TOOLS
            if tool not in CODEX_UNSUPPORTED_MAIN_TOOLS
        )

    return (
        _ccm_server_spec(
            name="ccm_skills",
            module="backend.mcp.ccm_skills_server",
            context_args=("--task-id", str(task_id)),
            enabled_tools=enabled_tools,
            api_base=api_base,
            task_id=task_id,
            credential_owner_kind="task-turn",
            credential_owner_id=task_id,
        ),
    )


def build_task_ssh_mcp_server_specs(
    task_id: int,
    api_base: str | None = None,
    *,
    capabilities: Sequence[str] = ("exec", "read", "write"),
) -> tuple[McpServerSpec, ...]:
    """Build the required, Task-scoped SSH capability server."""

    enabled_tools = ["list_connections"]
    selected = set(capabilities) & set(CCM_SSH_CAPABILITY_TOOLS)
    for capability in ("exec", "read", "write"):
        if capability in selected:
            enabled_tools.extend(CCM_SSH_CAPABILITY_TOOLS[capability])
    context_args = ["--task-id", str(task_id)]
    for capability in sorted(selected):
        context_args.extend(("--capability", capability))
    return (
        _ccm_server_spec(
            name="ccm_ssh",
            module="backend.mcp.ccm_ssh_server",
            context_args=tuple(context_args),
            enabled_tools=tuple(enabled_tools),
            api_base=api_base,
            task_id=task_id,
            credential_owner_kind="task-turn",
            credential_owner_id=task_id,
        ),
    )


def build_monitor_agent_mcp_server_specs(
    monitor_session_id: int,
    task_id: int,
    api_base: str | None = None,
    turn_generation: int | None = None,
) -> tuple[McpServerSpec, ...]:
    """Build MCP callback specs for one monitor agent."""

    context_args = [
        "--monitor-session-id",
        str(monitor_session_id),
        "--task-id",
        str(task_id),
    ]
    if turn_generation is not None:
        context_args.extend(
            ("--turn-generation", str(turn_generation))
        )
    return (
        _ccm_server_spec(
            name="ccm_monitor_agent",
            module="backend.mcp.ccm_monitor_agent_server",
            context_args=tuple(context_args),
            enabled_tools=CCM_MONITOR_AGENT_TOOLS,
            api_base=api_base,
            task_id=task_id,
            monitor_session_id=monitor_session_id,
            credential_owner_kind="monitor-turn",
            credential_owner_id=(
                f"{monitor_session_id}:{turn_generation}"
                if turn_generation is not None
                else f"{monitor_session_id}:legacy"
            ),
        ),
    )


def build_sub_agent_controller_mcp_server_specs(
    task_id: int,
    api_base: str | None = None,
) -> tuple[McpServerSpec, ...]:
    """Expose only the tools a Codex parent needs for the Sub-Agent skill."""

    return (
        _ccm_server_spec(
            name="ccm_skills",
            module="backend.mcp.ccm_skills_server",
            context_args=("--task-id", str(task_id)),
            enabled_tools=CCM_SUB_AGENT_CONTROLLER_TOOLS,
            api_base=api_base,
            task_id=task_id,
            credential_owner_kind="task-turn",
            credential_owner_id=task_id,
        ),
    )


def build_sub_agent_mcp_server_specs(
    session_id: int,
    task_id: int,
    api_base: str | None = None,
) -> tuple[McpServerSpec, ...]:
    """Build MCP callback specs for one sub-agent."""

    return (
        _ccm_server_spec(
            name="ccm_sub_agent",
            module="backend.mcp.ccm_sub_agent_server",
            context_args=(
                "--sub-agent-session-id",
                str(session_id),
                "--task-id",
                str(task_id),
            ),
            enabled_tools=CCM_SUB_AGENT_TOOLS,
            api_base=api_base,
            task_id=task_id,
            sub_agent_session_id=session_id,
            credential_owner_kind="sub-agent",
            credential_owner_id=session_id,
        ),
    )


def render_claude_mcp_config(specs: Sequence[McpServerSpec]) -> dict[str, object]:
    """Render specs in Claude Code's existing ``mcpServers`` JSON shape."""

    servers: dict[str, dict[str, object]] = {}
    for spec in specs:
        if spec.name in servers:
            raise ValueError(f"Duplicate MCP server name: {spec.name}")

        server: dict[str, object] = {
            "command": spec.command,
            "args": list(spec.args),
        }
        if spec.cwd is not None:
            server["cwd"] = spec.cwd
        if spec.env:
            server["env"] = dict(spec.env)
        servers[spec.name] = server

    return {"mcpServers": servers}


def render_codex_mcp_config(specs: Sequence[McpServerSpec]) -> dict[str, object]:
    """Render thread-level config for Codex app-server.

    The returned object is suitable for the ``config`` field of both
    ``thread/start`` and ``thread/resume``.  It is intentionally in-memory only;
    callers do not need to create or modify a Codex ``config.toml``.
    """

    servers: dict[str, dict[str, object]] = {}
    for spec in specs:
        if spec.name in servers:
            raise ValueError(f"Duplicate MCP server name: {spec.name}")
        if not _CODEX_MCP_SERVER_NAME_RE.fullmatch(spec.name):
            raise ValueError(
                f"Invalid Codex MCP server name {spec.name!r}: "
                "must match ^[A-Za-z0-9_-]+$"
            )

        for field_name, timeout in (
            ("startup_timeout_sec", spec.startup_timeout_sec),
            ("tool_timeout_sec", spec.tool_timeout_sec),
        ):
            if timeout is None:
                continue
            if (
                isinstance(timeout, bool)
                or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout)
                or timeout < 0
            ):
                raise ValueError(
                    f"Invalid Codex {field_name} for {spec.name!r}: "
                    "must be a finite non-negative number"
                )

        server: dict[str, object] = {
            "command": spec.command,
            "args": list(spec.args),
            "required": spec.required,
        }
        if spec.cwd is not None:
            server["cwd"] = spec.cwd
        if spec.env:
            server["env"] = dict(spec.env)
        if spec.enabled_tools:
            server["enabled_tools"] = list(spec.enabled_tools)
        if spec.default_tools_approval_mode is not None:
            if spec.default_tools_approval_mode not in {
                "auto",
                "prompt",
                "writes",
                "approve",
            }:
                raise ValueError(
                    "Invalid Codex default_tools_approval_mode for "
                    f"{spec.name!r}"
                )
            server["default_tools_approval_mode"] = (
                spec.default_tools_approval_mode
            )
        if spec.startup_timeout_sec is not None:
            server["startup_timeout_sec"] = spec.startup_timeout_sec
        if spec.tool_timeout_sec is not None:
            server["tool_timeout_sec"] = spec.tool_timeout_sec
        servers[spec.name] = server

    return {"mcp_servers": servers}


def _toml_key(key: str) -> str:
    """Return one TOML key segment without introducing dotted-key semantics."""

    if _TOML_BARE_KEY_RE.fullmatch(key):
        return key
    return json.dumps(key, ensure_ascii=False)


def _toml_literal(value: object) -> str:
    """Serialize the config value types used by ``McpServerSpec`` as TOML."""

    if isinstance(value, str):
        # JSON basic-string escapes are also valid TOML basic-string escapes.
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Codex config cannot contain a non-finite float")
        return repr(value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_toml_literal(item) for item in value) + "]"
    if isinstance(value, Mapping):
        items: list[str] = []
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("Codex config mapping keys must be strings")
            items.append(f"{_toml_key(key)} = {_toml_literal(item)}")
        if not items:
            return "{}"
        return "{ " + ", ".join(items) + " }"
    raise TypeError(f"Unsupported Codex config value: {type(value).__name__}")


def render_codex_exec_config_args(specs: Sequence[McpServerSpec]) -> list[str]:
    """Render ``codex exec -c`` argv tokens for the same config.

    The complete server table is one TOML override.  This avoids routing server
    names through Codex's dotted-path parser and still deep-merges with existing
    user MCP entries.  Returning argv tokens rather than a shell command
    preserves spaces, Unicode, quotes, and backslashes without applying shell
    quoting.
    """

    config = render_codex_mcp_config(specs)
    servers = cast(dict[str, dict[str, object]], config["mcp_servers"])
    if not servers:
        return []
    return ["-c", f"mcp_servers={_toml_literal(servers)}"]


def _write_claude_mcp_config(
    specs: Sequence[McpServerSpec],
    *,
    namespace: str,
    identifier: int,
    name: str,
) -> Path:
    from backend.services.task_runtime_secrets import write_private_json

    config = render_claude_mcp_config(specs)
    return write_private_json(
        namespace,
        identifier,
        name,
        cast(Mapping[str, object], config),
    )


def generate_mcp_config(
    task_id: int,
    enabled_skills: dict | None = None,
    api_base: str | None = None,
    *,
    task_ssh_capabilities: Sequence[str] = (),
) -> Path:
    """为指定 task 生成 MCP config JSON 文件。

    ccm_skills server 始终包含（提供 $help 等默认命令）。
    Returns: 临时文件路径，进程结束后由调用方清理。
    """
    specs = build_mcp_server_specs(task_id, enabled_skills, api_base)
    if task_ssh_capabilities:
        specs += build_task_ssh_mcp_server_specs(
            task_id,
            api_base,
            capabilities=task_ssh_capabilities,
        )
    return _write_claude_mcp_config(
        specs,
        namespace="task",
        identifier=task_id,
        name="mcp.json",
    )


def cleanup_mcp_config(task_id: int):
    """清理临时 MCP config 文件。"""
    from backend.services.task_runtime_secrets import remove_private_scope

    remove_private_scope("task", task_id)
    # Claude PTY and its MCP children persist across follow-up turns. Keep the
    # cached, route/task-scoped credentials alive until bounded expiry or an
    # explicit owner revocation; every endpoint still rechecks live state.


def generate_monitor_agent_mcp_config(
    monitor_session_id: int,
    task_id: int,
    api_base: str | None = None,
    turn_generation: int | None = None,
) -> Path:
    """为 monitor 子 agent 生成专用 MCP config。

    Returns:
        配置文件路径，调用方负责清理。
    """
    generation_name = (
        str(turn_generation) if turn_generation is not None else "legacy"
    )
    specs = build_monitor_agent_mcp_server_specs(
        monitor_session_id,
        task_id,
        api_base,
        turn_generation,
    )
    return _write_claude_mcp_config(
        specs,
        namespace="monitor",
        identifier=monitor_session_id,
        name=f"mcp-{generation_name}.json",
    )


def cleanup_monitor_agent_mcp_config(
    monitor_session_id: int,
    turn_generation: int | None = None,
):
    """清理 monitor 子 agent 的 MCP config 文件。"""
    from backend.services.internal_service_auth import (
        revoke_internal_service_owner,
        revoke_internal_service_owner_prefix,
    )
    from backend.services.task_runtime_secrets import (
        remove_private_file,
        remove_private_scope,
    )

    if turn_generation is None:
        remove_private_scope("monitor", monitor_session_id)
        revoke_internal_service_owner_prefix(
            "monitor-turn",
            f"{monitor_session_id}:",
        )
    else:
        remove_private_file(
            "monitor",
            monitor_session_id,
            f"mcp-{turn_generation}.json",
        )
        remove_private_file(
            "monitor",
            monitor_session_id,
            f"claude-security-{turn_generation}.json",
        )
        revoke_internal_service_owner(
            "monitor-turn",
            f"{monitor_session_id}:{turn_generation}",
        )


def generate_sub_agent_mcp_config(
    session_id: int, task_id: int, api_base: str | None = None
) -> Path:
    """为 sub-agent 子进程生成专用 MCP config。

    Returns:
        配置文件路径，调用方负责清理。
    """
    specs = build_sub_agent_mcp_server_specs(session_id, task_id, api_base)
    return _write_claude_mcp_config(
        specs,
        namespace="sub-agent",
        identifier=session_id,
        name="mcp.json",
    )


def cleanup_sub_agent_mcp_config(session_id: int):
    """清理 sub-agent 的 MCP config 文件。"""
    from backend.services.internal_service_auth import (
        revoke_internal_service_owner,
    )
    from backend.services.task_runtime_secrets import remove_private_scope

    remove_private_scope("sub-agent", session_id)
    revoke_internal_service_owner("sub-agent", session_id)
