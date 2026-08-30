"""CCM MCP server specs and provider-specific config generation.

The server description is provider-neutral.  Existing callers still receive a
Claude-compatible JSON file, while pure Codex renderers expose the same specs
without changing either provider's runtime task-launch path.
"""

import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Sequence, cast

from backend.services.trusted_runtime import (
    RUNNING_PYTHON,
    materialize_trusted_python_asset,
)


_CCM_ROOT = str(Path(__file__).resolve().parent.parent.parent)
# MCP servers are children of the running backend and need its exact dependency
# environment. Reusing that interpreter is portable across Linux venvs,
# container system Python, and Windows ``Scripts/python.exe``; constructing a
# repository-relative ``.venv/bin/python3`` path breaks Windows-hosted bind
# mounts even though the backend itself has all required packages.
_VENV_PYTHON = RUNNING_PYTHON
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
CCM_BROWSER_REVIEW_TOOLS = (
    "browser_open",
    "browser_observe",
    "browser_inspect",
    "browser_scroll",
    "browser_wait",
    "browser_move",
    "browser_click",
    "browser_double_click",
    "browser_type_text",
    "browser_keypress",
    "browser_drag",
    "finish_review",
)
CCM_FRONTEND_REVIEW_TOOLS = (
    "start_review",
    "check_review",
    "stop_review",
)
CCM_WORKSPACE_REVIEW_TOOLS = (
    "workspace_review_capabilities",
    "test_current_changes",
    "check_current_changes_review",
    "stop_current_changes_review",
    "test_git_target",
    "compare_test_runs",
)
CCM_SSH_TOOLS = (
    "list_connections",
    "new_effect_id",
    "run_command",
    "list_directory",
    "read_file",
    "write_file",
)
CCM_SSH_CAPABILITY_TOOLS = {
    "exec": ("new_effect_id", "run_command"),
    "read": ("list_directory", "read_file"),
    "write": ("new_effect_id", "write_file"),
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


def _review_scoped_auth_configured() -> bool:
    """Return whether review MCPs can derive a non-blank deployment secret."""

    from backend.config import settings

    token = getattr(settings, "auth_token", None)
    return isinstance(token, str) and bool(token.strip())


def _require_review_scoped_auth(name: str) -> None:
    if not _review_scoped_auth_configured():
        raise ValueError(
            f"{name} requires AUTH_TOKEN-backed scoped authentication"
        )


def _ccm_server_spec(
    *,
    name: str,
    module: str,
    context_args: Sequence[str],
    enabled_tools: tuple[str, ...],
    api_base: str | None,
    task_id: int | None = None,
    task_incarnation_id: str | None = None,
    task_retry_count: int | None = None,
    task_turn_generation: int | None = None,
    task_status: str | None = None,
    monitor_session_id: int | None = None,
    sub_agent_session_id: int | None = None,
    credential_owner_kind: str,
    credential_owner_id: str | int,
    trusted_runtime_asset: str | None = None,
    runtime_namespace: str | None = None,
    runtime_identifier: int | None = None,
    require_scoped_token: bool = False,
) -> McpServerSpec:
    from backend.services.internal_service_auth import (
        INTERNAL_TOKEN_ENV,
        issue_internal_service_token,
    )

    if task_id is not None and not re.fullmatch(
        r"[0-9a-f]{32}", task_incarnation_id or ""
    ):
        raise ValueError("Task-scoped MCP requires a durable Task incarnation")
    resolved_api_base = _api_base(api_base)
    scoped_token = issue_internal_service_token(
        audience=name,
        task_id=task_id,
        task_incarnation_id=task_incarnation_id,
        task_retry_count=task_retry_count,
        task_turn_generation=task_turn_generation,
        task_status=task_status,
        monitor_session_id=monitor_session_id,
        sub_agent_session_id=sub_agent_session_id,
        owner_kind=credential_owner_kind,
        owner_id=credential_owner_id,
    )
    if require_scoped_token and not scoped_token:
        raise ValueError(
            f"{name} requires AUTH_TOKEN-backed scoped authentication"
        )
    del module  # A Task MCP may never import a live backend module.
    if trusted_runtime_asset is None:
        raise ValueError("Task-scoped MCP requires a frozen trusted entrypoint")
    if runtime_namespace is None or runtime_identifier is None:
        raise ValueError(
            "Trusted MCP entrypoint requires a private runtime scope"
        )
    entrypoint = materialize_trusted_python_asset(
        trusted_runtime_asset,
        namespace=runtime_namespace,
        identifier=runtime_identifier,
    )
    args = [
        # Isolated mode ignores cwd/PYTHONPATH/user-site import injection while
        # preserving the running venv's installed FastMCP/httpx dependencies.
        "-I",
        str(entrypoint),
        *context_args,
        "--api-base",
        resolved_api_base,
    ]
    server_env = {
        "PYTHONPATH": "",
        "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    if scoped_token:
        server_env[INTERNAL_TOKEN_ENV] = scoped_token
    server_cwd = None

    return McpServerSpec(
        name=name,
        command=_VENV_PYTHON,
        args=tuple(args),
        cwd=server_cwd,
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
    task_incarnation_id: str,
    task_retry_count: int,
    task_turn_generation: int,
    task_status: str,
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

    browser_review_job_id = (enabled_skills or {}).get("browser-review")
    if isinstance(browser_review_job_id, str) and browser_review_job_id.strip():
        # A fixed Browser Review Task is a deliberately context-minimized
        # black-box agent. It receives only the bound browser evidence tools,
        # never the ordinary Task controller/context toolset.
        return build_browser_review_mcp_server_specs(
            browser_review_job_id.strip(),
            api_base=api_base,
            task_id=task_id,
            task_incarnation_id=task_incarnation_id,
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
            task_status=task_status,
        )

    specs = [
        _ccm_server_spec(
            name="ccm_skills",
            module="backend.mcp.ccm_skills_server",
            context_args=("--task-id", str(task_id)),
            enabled_tools=enabled_tools,
            api_base=api_base,
            task_id=task_id,
            task_incarnation_id=task_incarnation_id,
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
            task_status=task_status,
            credential_owner_kind="task-turn",
            credential_owner_id=task_id,
            trusted_runtime_asset="ccm_skills_http_server",
            runtime_namespace="task",
            runtime_identifier=task_id,
        ),
    ]
    from backend.config import settings

    if _review_scoped_auth_configured():
        specs.extend(
            build_frontend_review_mcp_server_specs(
                task_id,
                api_base=api_base,
                task_incarnation_id=task_incarnation_id,
                task_retry_count=task_retry_count,
                task_turn_generation=task_turn_generation,
                task_status=task_status,
            )
        )
        specs.extend(
            build_workspace_review_mcp_server_specs(
                task_id,
                api_base=api_base,
                task_incarnation_id=task_incarnation_id,
                task_retry_count=task_retry_count,
                task_turn_generation=task_turn_generation,
                task_status=task_status,
            )
        )
    return tuple(specs)


def build_task_ssh_mcp_server_specs(
    task_id: int,
    api_base: str | None = None,
    *,
    capabilities: Sequence[str] = ("exec", "read", "write"),
    task_incarnation_id: str,
    task_retry_count: int,
    task_turn_generation: int,
    task_status: str,
) -> tuple[McpServerSpec, ...]:
    """Build the required, Task-scoped SSH capability server."""

    if not re.fullmatch(r"[0-9a-f]{32}", task_incarnation_id):
        raise ValueError("Task SSH MCP requires a durable Task incarnation")
    if any(
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        for value in (task_retry_count, task_turn_generation)
    ):
        raise ValueError("Task SSH MCP requires an exact non-negative generation")
    if task_status not in {"in_progress", "executing"}:
        raise ValueError("Task SSH MCP requires an exact active Task status")
    enabled_tools = ["list_connections"]
    selected = set(capabilities) & set(CCM_SSH_CAPABILITY_TOOLS)
    for capability in ("exec", "read", "write"):
        if capability in selected:
            enabled_tools.extend(
                tool
                for tool in CCM_SSH_CAPABILITY_TOOLS[capability]
                if tool not in enabled_tools
            )
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
            task_incarnation_id=task_incarnation_id,
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
            task_status=task_status,
            credential_owner_kind="task-turn",
            credential_owner_id=task_id,
            trusted_runtime_asset="ccm_ssh_server",
            runtime_namespace="task",
            runtime_identifier=task_id,
        ),
    )


def _validate_review_task_generation(
    *,
    task_id: int,
    task_incarnation_id: str,
    task_retry_count: int,
    task_turn_generation: int,
    task_status: str,
) -> None:
    if isinstance(task_id, bool) or not isinstance(task_id, int) or task_id <= 0:
        raise ValueError("review MCP requires a positive Task id")
    if not re.fullmatch(r"[0-9a-f]{32}", task_incarnation_id):
        raise ValueError("review MCP requires a durable Task incarnation")
    if any(
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        for value in (task_retry_count, task_turn_generation)
    ):
        raise ValueError("review MCP requires an exact non-negative generation")
    if task_status not in {"in_progress", "executing"}:
        raise ValueError("review MCP requires an exact active Task status")


def build_frontend_review_mcp_server_specs(
    task_id: int,
    api_base: str | None = None,
    *,
    task_incarnation_id: str,
    task_retry_count: int,
    task_turn_generation: int,
    task_status: str,
) -> tuple[McpServerSpec, ...]:
    """Expose repeatable browser review tools inside an ordinary Task."""

    _require_review_scoped_auth("ccm_frontend_review")
    _validate_review_task_generation(
        task_id=task_id,
        task_incarnation_id=task_incarnation_id,
        task_retry_count=task_retry_count,
        task_turn_generation=task_turn_generation,
        task_status=task_status,
    )
    return (
        _ccm_server_spec(
            name="ccm_frontend_review",
            module="backend.mcp.ccm_browser_review_server",
            context_args=("--task-id", str(task_id)),
            enabled_tools=CCM_FRONTEND_REVIEW_TOOLS,
            api_base=api_base,
            task_id=task_id,
            task_incarnation_id=task_incarnation_id,
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
            task_status=task_status,
            credential_owner_kind="task-turn",
            credential_owner_id=task_id,
            trusted_runtime_asset="ccm_browser_review_server",
            runtime_namespace="task",
            runtime_identifier=task_id,
            require_scoped_token=True,
        ),
    )


def build_browser_review_mcp_server_specs(
    job_id: str,
    *,
    task_id: int,
    task_incarnation_id: str,
    task_retry_count: int,
    task_turn_generation: int,
    task_status: str,
    api_base: str | None = None,
) -> tuple[McpServerSpec, ...]:
    """Build the isolated, task-scoped browser tools for one review job."""

    normalized_job_id = job_id.strip()
    if not normalized_job_id:
        raise ValueError("browser review job id cannot be empty")
    _require_review_scoped_auth("ccm_browser_review")
    _validate_review_task_generation(
        task_id=task_id,
        task_incarnation_id=task_incarnation_id,
        task_retry_count=task_retry_count,
        task_turn_generation=task_turn_generation,
        task_status=task_status,
    )
    return (
        _ccm_server_spec(
            name="ccm_browser_review",
            module="backend.mcp.ccm_browser_review_server",
            context_args=("--job-id", normalized_job_id),
            enabled_tools=CCM_BROWSER_REVIEW_TOOLS,
            api_base=api_base,
            task_id=task_id,
            task_incarnation_id=task_incarnation_id,
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
            task_status=task_status,
            credential_owner_kind="browser-review-job",
            credential_owner_id=normalized_job_id,
            trusted_runtime_asset="ccm_browser_review_server",
            runtime_namespace="task",
            runtime_identifier=task_id,
            require_scoped_token=True,
        ),
    )


def build_workspace_review_mcp_server_specs(
    task_id: int,
    api_base: str | None = None,
    *,
    task_incarnation_id: str,
    task_retry_count: int,
    task_turn_generation: int,
    task_status: str,
) -> tuple[McpServerSpec, ...]:
    """Expose current-branch Preview + isolated Browser Agent orchestration."""

    _require_review_scoped_auth("ccm_workspace_review")
    _validate_review_task_generation(
        task_id=task_id,
        task_incarnation_id=task_incarnation_id,
        task_retry_count=task_retry_count,
        task_turn_generation=task_turn_generation,
        task_status=task_status,
    )
    return (
        _ccm_server_spec(
            name="ccm_workspace_review",
            module="backend.mcp.ccm_workspace_review_server",
            context_args=("--task-id", str(task_id)),
            enabled_tools=CCM_WORKSPACE_REVIEW_TOOLS,
            api_base=api_base,
            task_id=task_id,
            task_incarnation_id=task_incarnation_id,
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
            task_status=task_status,
            credential_owner_kind="task-turn",
            credential_owner_id=task_id,
            trusted_runtime_asset="ccm_workspace_review_server",
            runtime_namespace="task",
            runtime_identifier=task_id,
            require_scoped_token=True,
        ),
    )


def build_monitor_agent_mcp_server_specs(
    monitor_session_id: int,
    task_id: int,
    api_base: str | None = None,
    turn_generation: int | None = None,
    *,
    task_incarnation_id: str,
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
            task_incarnation_id=task_incarnation_id,
            monitor_session_id=monitor_session_id,
            credential_owner_kind="monitor-turn",
            credential_owner_id=(
                f"{monitor_session_id}:{turn_generation}"
                if turn_generation is not None
                else f"{monitor_session_id}:legacy"
            ),
            trusted_runtime_asset="ccm_monitor_agent_server",
            runtime_namespace="monitor",
            runtime_identifier=monitor_session_id,
        ),
    )


def build_sub_agent_controller_mcp_server_specs(
    task_id: int,
    api_base: str | None = None,
    *,
    task_incarnation_id: str,
    task_retry_count: int,
    task_turn_generation: int,
    task_status: str,
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
            task_incarnation_id=task_incarnation_id,
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
            task_status=task_status,
            credential_owner_kind="task-turn",
            credential_owner_id=task_id,
            trusted_runtime_asset="ccm_skills_http_server",
            runtime_namespace="task",
            runtime_identifier=task_id,
        ),
    )


def build_sub_agent_mcp_server_specs(
    session_id: int,
    task_id: int,
    api_base: str | None = None,
    *,
    task_incarnation_id: str,
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
            task_incarnation_id=task_incarnation_id,
            sub_agent_session_id=session_id,
            credential_owner_kind="sub-agent",
            credential_owner_id=session_id,
            trusted_runtime_asset="ccm_sub_agent_server",
            runtime_namespace="sub-agent",
            runtime_identifier=session_id,
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
    task_incarnation_id: str,
    task_retry_count: int,
    task_turn_generation: int,
    task_status: str,
) -> Path:
    """为指定 task 生成 MCP config JSON 文件。

    ccm_skills server 始终包含（提供 $help 等默认命令）。
    Returns: 临时文件路径，进程结束后由调用方清理。
    """
    specs = build_mcp_server_specs(
        task_id,
        enabled_skills,
        api_base,
        task_incarnation_id=task_incarnation_id,
        task_retry_count=task_retry_count,
        task_turn_generation=task_turn_generation,
        task_status=task_status,
    )
    if task_ssh_capabilities:
        specs += build_task_ssh_mcp_server_specs(
            task_id,
            api_base,
            capabilities=task_ssh_capabilities,
            task_incarnation_id=task_incarnation_id or "",
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
            task_status=task_status,
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
    *,
    task_incarnation_id: str,
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
        task_incarnation_id=task_incarnation_id,
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
    session_id: int,
    task_id: int,
    api_base: str | None = None,
    *,
    task_incarnation_id: str,
) -> Path:
    """为 sub-agent 子进程生成专用 MCP config。

    Returns:
        配置文件路径，调用方负责清理。
    """
    specs = build_sub_agent_mcp_server_specs(
        session_id,
        task_id,
        api_base,
        task_incarnation_id=task_incarnation_id,
    )
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
