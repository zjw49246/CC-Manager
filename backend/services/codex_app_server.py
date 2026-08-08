"""Persistent Codex app-server transport.

The regular ``codex exec resume`` integration starts a new CLI process for
every turn.  App-server keeps configuration, MCP clients, and active threads in
one process while exposing the same persisted Codex thread ids.  This module
adapts one app-server turn to the small subprocess surface InstanceManager
already consumes, so task status/retry/DB logic remains shared with exec mode.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Sequence

from backend.services.codex_tier_proxy import (
    CodexActualTierProxy,
    CodexTierProofError,
    CodexTierProxyRoute,
)
from backend.services.mcp_config import McpServerSpec, render_codex_mcp_config
from backend.services.process_safety import require_safe_process_group_id

logger = logging.getLogger(__name__)

_APP_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT = 5.0
_APP_SERVER_TERM_SHUTDOWN_TIMEOUT = 5.0
_APP_SERVER_KILL_SHUTDOWN_TIMEOUT = 5.0
_APP_SERVER_GROUP_POLL_INTERVAL = 0.05
_ACTIVE_TURN_MISMATCH_RE = re.compile(
    r"expected active turn id\s+[`'\"]?"
    r"(?P<expected>[^\s`'\"]+)[`'\"]?\s+but found\s+[`'\"]?"
    r"(?P<actual>[^\s`'\"]+)"
)
_GOALS_FEATURE_DISABLED_RE = re.compile(
    r"\bgoals feature is disabled\b",
    re.IGNORECASE,
)
_NO_ACTIVE_GOAL_RE = re.compile(
    r"\b(?:no active goal|goal is not active|goal not found)\b",
    re.IGNORECASE,
)
CODEX_SERVICE_TIER_DEFAULT = "default"
CODEX_SERVICE_TIER_PRIORITY = "priority"
_CODEX_SERVICE_TIERS = frozenset({
    CODEX_SERVICE_TIER_DEFAULT,
    CODEX_SERVICE_TIER_PRIORITY,
})
_MODEL_LIST_PAGE_LIMIT = 100
_MODEL_LIST_MAX_PAGES = 20
# A parent Codex turn may emit ``turn/completed`` while native child threads
# are still running.  Keep the adapter alive until every child is
# authoritatively quiescent.  Notifications are the fast path; the periodic
# read closes listener-attachment races without busy polling.
_DESCENDANT_RECONCILE_INTERVAL = 5.0
_DESCENDANT_RECONCILE_REQUEST_TIMEOUT = 5.0
_DESCENDANT_INTERRUPT_CONFIRM_TIMEOUT = 10.0
_DESCENDANT_INTERRUPT_POLL_INTERVAL = 0.1
# A transient local app-server RPC failure is not evidence that a Goal ended.
# Retry while retaining the exact CCM process generation; turn/Goal
# notifications remain the fast path and invalidate the stale guard.
_GOAL_RECONCILE_INTERVAL = 5.0
# Native sub-agents are otherwise allowed to run for hours.  Reaching this
# fence is an explicit failure, never permission to publish a false success.
_DESCENDANT_TERMINAL_TIMEOUT = 4 * 60 * 60.0


def _format_process_exit(returncode: int | None) -> str:
    """Describe a subprocess exit without mistaking stderr noise for cause."""

    if returncode is None:
        return "unknown exit status"
    if returncode < 0:
        signal_number = -returncode
        try:
            signal_name = signal.Signals(signal_number).name
        except ValueError:
            signal_name = f"signal {signal_number}"
        return f"killed by {signal_name} ({signal_number})"
    return f"exit code {returncode}"

# Codex has no public "core tool allow-list" in app-server 0.144.6.  An
# explicit empty environment removes every environment-backed tool, while a
# deny-all permission profile remains the execution boundary if a future
# version accidentally reintroduces one.  The response audit below proves that
# this exact profile, rather than an inherited :read-only profile, was selected
# before any model turn is admitted.
_TOOL_FREE_PERMISSION_PROFILE = "ccm_pr_review_no_access_v1"
_TASK_SSH_PERMISSION_PROFILE = "ccm_task_ssh_isolated_v1"
_TOOL_FREE_DISABLED_FEATURES = frozenset({
    "apps",
    "artifact",
    "auth_elicitation",
    "browser_use",
    "browser_use_external",
    "browser_use_full_cdp_access",
    "code_mode",
    "code_mode_host",
    "computer_use",
    "current_time_reminder",
    "default_mode_request_user_input",
    "deferred_executor",
    "enable_fanout",
    "enable_mcp_apps",
    "goals",
    "guardian_approval",
    "hooks",
    "image_generation",
    "in_app_browser",
    "memories",
    "multi_agent",
    "plugins",
    "plugin_sharing",
    "realtime_conversation",
    "remote_compaction_v2",
    "remote_plugin",
    "request_permissions_tool",
    "shell_snapshot",
    "shell_tool",
    "skill_mcp_dependency_install",
    "standalone_web_search",
    "token_budget",
    "tool_call_mcp_elicitation",
    "tool_suggest",
    "unified_exec",
    "workspace_dependencies",
})
_TOOL_FREE_PASSIVE_ITEM_TYPES = frozenset({
    "agentMessage",
    "contextCompaction",
    "reasoning",
    "userMessage",
})
_TOOL_FREE_PASSIVE_NOTIFICATION_METHODS = frozenset({
    "configWarning",
    "deprecationNotice",
    "error",
    "guardianWarning",
    "item/agentMessage/delta",
    "item/completed",
    "item/reasoning/summaryPartAdded",
    "item/reasoning/summaryTextDelta",
    "item/reasoning/textDelta",
    "item/started",
    "model/rerouted",
    "model/safetyBuffering/updated",
    "model/verification",
    "thread/tokenUsage/updated",
    "turn/completed",
    "turn/moderationMetadata",
    "turn/started",
    "warning",
})
_TOOL_FREE_FORBIDDEN_NOTIFICATION_PREFIXES = (
    "command/",
    "hook/",
    "item/autoApprovalReview/",
    "item/commandExecution/",
    "item/fileChange/",
    "item/mcpToolCall/",
    "item/plan/",
    "process/",
    "thread/goal/",
    "thread/realtime/",
    "turn/diff/",
    "turn/plan/",
)


async def _settle_registry_cleanup(awaitable):
    """Complete registry bookkeeping before propagating caller cancellation."""

    cleanup = asyncio.create_task(awaitable)
    delayed_cancellation: asyncio.CancelledError | None = None
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError as exc:
            if delayed_cancellation is None:
                delayed_cancellation = exc
        except BaseException:
            if cleanup.done():
                break
            raise
    result = cleanup.result()
    if delayed_cancellation is not None:
        raise delayed_cancellation
    return result


class CodexAppServerError(RuntimeError):
    """Raised when app-server rejects a request or loses its transport."""


class CodexAppServerRequestError(CodexAppServerError):
    """The server explicitly rejected one JSON-RPC request."""


class CodexAppServerBusyError(CodexAppServerError):
    """The requested account home still has an active Codex turn."""


class CodexSharedTransportBusyError(CodexAppServerBusyError):
    """A claimed turn cannot be stopped without disrupting a shared transport."""


class CodexThreadNotIdleError(CodexAppServerBusyError):
    """A native thread can still execute outside CCM's current turn adapter."""

    def __init__(
        self,
        thread_id: str,
        state: str,
        *,
        operation: str,
    ) -> None:
        super().__init__(
            f"Codex thread {thread_id} is not authoritatively idle before "
            f"{operation}: {state}"
        )
        self.thread_id = thread_id
        self.state = state
        self.operation = operation


class CodexThreadTerminalStateError(CodexAppServerError):
    """A native thread is terminal but cannot yet admit another turn safely."""

    def __init__(
        self,
        thread_id: str,
        state: str,
        *,
        operation: str,
        recovery_attempted: bool = False,
        detail: str | None = None,
    ) -> None:
        recovery = " after one runtime recycle" if recovery_attempted else ""
        suffix = f": {detail}" if detail else ""
        super().__init__(
            f"Codex thread {thread_id} remained in terminal state {state} "
            f"before {operation}{recovery}{suffix}"
        )
        self.thread_id = thread_id
        self.state = state
        self.operation = operation
        self.recovery_attempted = recovery_attempted


class CodexRequiredMcpError(CodexAppServerError):
    """A thread could not be created with its required MCP configuration."""


class CodexRequiredMcpPreTurnError(CodexRequiredMcpError):
    """Required task context failed before admission, so exec replay is safe."""


class CodexThreadHomeMismatchError(CodexAppServerError):
    """A thread was routed to a different account without an explicit rebind."""


class CodexThreadIdentityMismatchError(CodexThreadHomeMismatchError):
    """A resume response identified a different native thread than requested."""

    def __init__(
        self,
        expected_thread_id: str,
        actual_thread_id: str,
        *,
        operation: str,
    ) -> None:
        super().__init__(
            f"Codex {operation} for thread {expected_thread_id} returned "
            f"a different thread id: {actual_thread_id}"
        )
        self.expected_thread_id = expected_thread_id
        self.actual_thread_id = actual_thread_id
        self.operation = operation


class CodexServiceTierUnavailableError(CodexAppServerError):
    """The requested Codex service tier was not admitted before turn/start."""


class _UnconfirmedTurnCancellation(asyncio.CancelledError):
    """A cancelled turn/start whose interrupt was not acknowledged."""

    def __init__(self, process: "CodexTurnProcess", reason: str) -> None:
        super().__init__(reason)
        self.process = process
        self.reason = reason


class _UnconfirmedTurnStartFailure(CodexAppServerError):
    """A timed-out turn/start that may still be executing server-side."""

    def __init__(self, process: "CodexTurnProcess", reason: str) -> None:
        super().__init__(reason)
        self.process = process
        self.reason = reason


def _active_turn_id_from_error(error: BaseException | str) -> str | None:
    """Extract app-server's authoritative active turn from a mismatch error."""

    match = _ACTIVE_TURN_MISMATCH_RE.search(str(error))
    if match is None:
        return None
    actual = match.group("actual").rstrip("`'\".,:;)")
    expected = match.group("expected").rstrip("`'\".,:;)")
    return actual if actual and actual != expected else None


def normalize_codex_home(codex_home: str | os.PathLike[str] | None = None) -> str:
    """Return the canonical, absolute CODEX_HOME used as the process key."""

    configured = codex_home or os.environ.get("CODEX_HOME") or Path.home() / ".codex"
    return str(Path(configured).expanduser().resolve(strict=False))


def normalize_codex_service_tier(service_tier: str | None) -> str:
    """Return CCM's canonical Codex service tier or reject unsafe input."""

    value = str(service_tier or CODEX_SERVICE_TIER_DEFAULT).strip().lower()
    if value not in _CODEX_SERVICE_TIERS:
        raise ValueError(f"Unsupported Codex service tier: {service_tier!r}")
    return value


def _require_idle_thread_status(
    thread: Any,
    *,
    thread_id: str,
    operation: str,
) -> None:
    """Require the v2 protocol's explicit idle proof for one native thread."""

    status = thread.get("status") if isinstance(thread, dict) else None
    status_type = status.get("type") if isinstance(status, dict) else None
    if status_type in {"systemError", "notLoaded"}:
        raise CodexThreadTerminalStateError(
            thread_id,
            str(status_type),
            operation=operation,
        )
    if status_type != "idle":
        raise CodexThreadNotIdleError(
            thread_id,
            str(status_type or "unknown"),
            operation=operation,
        )


def _audit_tool_free_thread_response(response: Any) -> None:
    """Prove the deny-all runtime selected before sending model input."""

    if not isinstance(response, dict):
        raise ValueError("thread response is not an object")
    permission_profile = response.get("activePermissionProfile")
    if (
        not isinstance(permission_profile, dict)
        or permission_profile.get("id") != _TOOL_FREE_PERMISSION_PROFILE
        or permission_profile.get("extends") is not None
    ):
        raise ValueError("deny-all permission profile was not selected")
    if response.get("runtimeWorkspaceRoots") != []:
        raise ValueError("runtime workspace roots were not cleared")
    if response.get("instructionSources") != []:
        raise ValueError("ambient instruction sources were loaded")
    if response.get("sandbox") != {
        "type": "readOnly",
        "networkAccess": False,
    }:
        raise ValueError(
            "deny-all profile did not resolve to restricted sandbox"
        )


def _task_ssh_permission_config(
    *,
    cwd: str,
    protected_paths: Sequence[str],
    disable_network: bool,
    sandbox_mode: str,
) -> dict[str, Any]:
    """Build a request-local Codex profile that hides host credentials."""

    filesystem: dict[str, str] = {"/": "read"}
    if sandbox_mode == "workspace-write":
        for writable in {
            os.path.abspath(cwd),
            os.path.abspath(os.environ.get("TMPDIR") or "/tmp"),
            "/tmp",
        }:
            filesystem[writable] = "write"
    elif sandbox_mode != "read-only":
        raise ValueError("Task isolation requires a sandboxed Codex mode")
    for value in protected_paths:
        path = os.path.abspath(os.path.expanduser(str(value)))
        if path == "/":
            raise ValueError("Task SSH protected path cannot be filesystem root")
        filesystem[path] = "deny"
    return {
        "filesystem": filesystem,
        "network": {
            # Preserve Codex's read-only default (no network). Workspace
            # Tasks retain normal network unless an SSH grant requires the
            # broker-only network boundary.
            "enabled": sandbox_mode == "workspace-write" and not disable_network,
            "allow_local_binding": False,
        },
    }


def _audit_task_ssh_thread_response(
    response: Any,
    *,
    disable_network: bool,
    sandbox_mode: str,
) -> None:
    """Prove Codex admitted CCM's exact Task-isolation profile."""

    if not isinstance(response, dict):
        raise ValueError("thread response is not an object")
    permission_profile = response.get("activePermissionProfile")
    if (
        not isinstance(permission_profile, dict)
        or permission_profile.get("id") != _TASK_SSH_PERMISSION_PROFILE
        or permission_profile.get("extends") is not None
    ):
        raise ValueError("Task isolation profile was not selected")
    sandbox = response.get("sandbox")
    expected_type = {
        "workspace-write": "workspaceWrite",
        "read-only": "readOnly",
    }.get(sandbox_mode)
    expected_network = (
        sandbox_mode == "workspace-write" and not disable_network
    )
    if (
        not isinstance(sandbox, dict)
        or sandbox.get("type") != expected_type
        or sandbox.get("networkAccess") is not expected_network
    ):
        raise ValueError("Task isolation resolved an unexpected sandbox policy")


def _tool_free_disabled_skill_config(
    response: Any,
    *,
    cwd: str,
) -> list[dict[str, Any]]:
    """Turn a forced skills inventory into an exact path deny-list."""

    data = response.get("data") if isinstance(response, dict) else None
    if not isinstance(data, list) or len(data) != 1:
        raise ValueError("skills inventory has an unexpected shape")
    entry = data[0]
    if not isinstance(entry, dict):
        raise ValueError("skills inventory entry is malformed")
    entry_cwd = entry.get("cwd")
    if (
        not isinstance(entry_cwd, str)
        or _canonical_path(entry_cwd) != _canonical_path(cwd)
    ):
        raise ValueError("skills inventory cwd does not match")
    if entry.get("errors") != []:
        raise ValueError("skills inventory contains discovery errors")
    skills = entry.get("skills")
    if not isinstance(skills, list):
        raise ValueError("skills inventory list is malformed")
    disabled: list[dict[str, Any]] = []
    seen: set[str] = set()
    for skill in skills:
        if not isinstance(skill, dict):
            raise ValueError("skills inventory item is malformed")
        path = skill.get("path")
        enabled = skill.get("enabled")
        if (
            not isinstance(path, str)
            or not Path(path).is_absolute()
            or type(enabled) is not bool
        ):
            raise ValueError("skills inventory identity is malformed")
        canonical = str(_canonical_path(path))
        if canonical in seen:
            continue
        seen.add(canonical)
        disabled.append({"path": canonical, "enabled": False})
    return disabled


def _canonical_app_server_service_tier(value: Any) -> str | None:
    """Map app-server's explicit Standard value to its RPC request shape."""

    # Sending JSON null clears a sticky Fast setting, while Codex reports the
    # resulting effective setting as the literal string ``default``.
    if value is None or value == CODEX_SERVICE_TIER_DEFAULT:
        return None
    if value == CODEX_SERVICE_TIER_PRIORITY:
        return CODEX_SERVICE_TIER_PRIORITY
    return "__invalid__"


def _canonical_path(path: str | os.PathLike[str]) -> Path:
    """Best-effort equivalent of Codex's canonical project trust key path."""

    expanded = Path(path).expanduser()
    try:
        return expanded.resolve(strict=False)
    except (OSError, RuntimeError):
        return Path(os.path.abspath(os.fspath(expanded)))


def codex_project_trust_target(cwd: str | os.PathLike[str]) -> str:
    """Resolve the project key Codex 0.144.6 uses for trust decisions.

    A regular checkout trusts its nearest repository root.  A linked worktree
    trusts the main checkout root referenced by
    ``.git/worktrees/<worktree-name>``.  Codex falls back to the requested cwd
    when neither case can be proven, including malformed/non-worktree gitdir
    pointers.
    """

    requested_cwd = _canonical_path(cwd)
    base = requested_cwd if requested_cwd.is_dir() else requested_cwd.parent
    repository_root: Path | None = None
    dot_git: Path | None = None
    for candidate in (base, *base.parents):
        candidate_dot_git = candidate / ".git"
        try:
            candidate_dot_git.stat()
        except OSError:
            continue
        repository_root = candidate
        dot_git = candidate_dot_git
        break

    if repository_root is None or dot_git is None:
        return str(requested_cwd)

    try:
        if dot_git.is_dir():
            return str(_canonical_path(repository_root))
        pointer = dot_git.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        return str(requested_cwd)

    prefix = "gitdir:"
    if not pointer.startswith(prefix):
        return str(requested_cwd)
    git_dir_value = pointer[len(prefix):].strip()
    if not git_dir_value:
        return str(requested_cwd)

    git_dir = Path(git_dir_value).expanduser()
    if not git_dir.is_absolute():
        git_dir = repository_root / git_dir
    git_dir = _canonical_path(git_dir)
    worktrees_dir = git_dir.parent
    if worktrees_dir.name != "worktrees":
        return str(requested_cwd)
    common_dir = worktrees_dir.parent
    return str(_canonical_path(common_dir.parent))


def codex_untrusted_project_config(
    cwd: str | os.PathLike[str],
) -> dict[str, Any]:
    """Build a session-only config that disables project-local Codex config."""

    target = codex_project_trust_target(cwd)
    return {"projects": {target: {"trust_level": "untrusted"}}}


def codex_untrusted_project_override(cwd: str | os.PathLike[str]) -> str:
    """Build the equivalent safe whole-map TOML override for ``codex -c``."""

    target = json.dumps(codex_project_trust_target(cwd), ensure_ascii=False)
    return f'projects={{{target}={{trust_level="untrusted"}}}}'


def _deep_merge_config(
    target: dict[str, Any],
    override: dict[str, Any],
) -> None:
    """Merge a thread override without discarding unrelated nested config."""

    for key, value in override.items():
        current = target.get(key)
        if isinstance(current, dict) and isinstance(value, dict):
            _deep_merge_config(current, value)
        else:
            target[key] = value


class CodexTurnProcess:
    """Process-like view of one app-server turn.

    InstanceManager only needs stdout/stderr readers, ``wait()``, returncode,
    pid, and interrupt/kill methods.  Keeping that contract lets the existing
    output consumer own all final task/instance state transitions.
    """

    def __init__(
        self,
        pid: int,
        interrupt: Callable[[], Awaitable[None]],
        *,
        thread_id: str | None = None,
    ) -> None:
        self.pid = pid
        self.thread_id = thread_id
        self.unsubscribe_on_terminal = False
        self.returncode: int | None = None
        self.termination_kind: str | None = None
        self.stdout = asyncio.StreamReader(limit=10 * 1024 * 1024)
        self.stderr = asyncio.StreamReader(limit=1024 * 1024)
        self._interrupt = interrupt
        self._done = asyncio.get_running_loop().create_future()
        # Lightweight per-turn stream telemetry. Auxiliary callers such as
        # the Plan pipeline can persist this before deleting a disposable
        # thread, and can distinguish slow initial reasoning from a response
        # stream that stopped making progress after output began.
        self.last_delta_at: datetime | None = None
        self.last_delta_monotonic: float | None = None
        self.streamed_output_chars = 0
        self.last_event_type: str | None = None

    def feed(self, payload: dict[str, Any]) -> None:
        if self.returncode is not None:
            return
        event_type = payload.get("type")
        if isinstance(event_type, str) and event_type:
            self.last_event_type = event_type
        if event_type in {
            "item.agent_message.delta",
            "item.reasoning.delta",
        }:
            delta = payload.get("delta")
            if isinstance(delta, str):
                self.streamed_output_chars += len(delta)
            self.last_delta_at = datetime.utcnow()
            self.last_delta_monotonic = time.monotonic()
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        self.stdout.feed_data(line.encode("utf-8") + b"\n")

    def finish(
        self,
        returncode: int,
        stderr: str = "",
        *,
        termination_kind: str | None = None,
    ) -> None:
        if self.returncode is not None:
            return
        self.returncode = returncode
        # Dispatcher timeout is stamped before interrupting the native turn.
        # Do not let the subsequent app-server ``interrupted`` notification
        # relabel that forced abort as a successful user interrupt.
        if self.termination_kind != "timeout":
            self.termination_kind = termination_kind
        if stderr:
            self.stderr.feed_data(stderr.encode("utf-8", errors="replace"))
        self.stdout.feed_eof()
        self.stderr.feed_eof()
        if not self._done.done():
            self._done.set_result(returncode)

    async def wait(self) -> int:
        return await asyncio.shield(self._done)

    def send_signal(self, sig: int) -> None:
        if sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
            asyncio.create_task(self._interrupt_safely())

    def terminate(self) -> None:
        self.send_signal(signal.SIGTERM)

    def kill(self) -> None:
        self.send_signal(signal.SIGKILL)

    async def _interrupt_safely(self) -> None:
        try:
            await self._interrupt()
        except Exception:
            logger.exception("Codex app-server turn interrupt failed")
            # A failed RPC says nothing about the real server-side turn.  In
            # particular, marking this adapter terminal would let
            # InstanceManager release the Task/Instance claim while Codex may
            # still be executing.  Keep the process active so its caller
            # retries/escalates and retains exact generation evidence.
            return


@dataclass
class _TurnContext:
    thread_id: str
    process: CodexTurnProcess
    launch_started: float
    task_id: int | None
    turn_id: str | None = None
    admitted_turn_id: str | None = None
    observed_turn_id: str | None = None
    client_user_message_id: str | None = None
    provisional_started_turn_ids: set[str] = field(default_factory=set)
    usage: dict[str, int] | None = None
    first_input_seen: bool = False
    first_output_seen: bool = False
    turn_started_emitted: bool = False
    pending_terminal_notification: tuple[str, dict[str, Any]] | None = None
    admission_observed_future: asyncio.Future | None = None
    admission_confirmed: bool = False
    pending_admission_notifications: (
        list[tuple[str, dict[str, Any]]] | None
    ) = None
    descendant_thread_ids: set[str] = field(default_factory=set)
    active_descendant_thread_ids: set[str] = field(default_factory=set)
    descendant_state_changed: asyncio.Event | None = None
    descendant_interrupt_lock: asyncio.Lock | None = None
    descendant_guard_task: asyncio.Task | None = None
    deferred_terminal_notification: dict[str, Any] | None = None
    following_native_goal: bool = False
    pending_goal_terminal_notification: dict[str, Any] | None = None
    goal_terminal_generation: int = 0
    goal_guard_tasks: set[asyncio.Task] = field(default_factory=set)
    non_retry_error: dict[str, Any] | None = None
    tools_disabled: bool = False
    tool_policy_violation: str | None = None
    tool_policy_abort_task: asyncio.Task | None = None


@dataclass
class _ThreadRuntimeState:
    """Last authoritative app-server lifecycle facts for one native thread."""

    status_type: str | None = None
    active_turn_ids: set[str] = field(default_factory=set)
    goal_status: str | None = None


class CodexAppServer:
    """One lazily started app-server permanently bound to one CODEX_HOME."""

    def __init__(
        self,
        binary: str,
        request_timeout: float = 30.0,
        *,
        codex_home: str | os.PathLike[str] | None = None,
        env_remove: set[str] | None = None,
        actual_tier_proxy_route: CodexTierProxyRoute | None = None,
        require_actual_tier_proof: bool = False,
    ) -> None:
        self.binary = binary
        self.request_timeout = request_timeout
        self.codex_home = normalize_codex_home(codex_home)
        self._env_remove = {
            str(key).upper() for key in (env_remove or set())
        }
        self._actual_tier_proxy_route = actual_tier_proxy_route
        self._require_actual_tier_proof = bool(require_actual_tier_proof)
        self._actual_tier_proxy: CodexActualTierProxy | None = None
        self._process: asyncio.subprocess.Process | None = None
        # On POSIX, app-server is launched as its own session leader.  Keep
        # the exact process identity—not just its numeric PID—so a stale
        # shutdown can never signal a replacement generation after PID reuse.
        self._process_group_process: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task | None = None
        self._stderr_task: asyncio.Task | None = None
        self._pending: dict[int, asyncio.Future] = {}
        self._thread_settings_waiters: dict[str, asyncio.Future] = {}
        self._contexts_by_thread: dict[str, _TurnContext] = {}
        self._contexts_by_turn: dict[str, _TurnContext] = {}
        # Child threads are not CCM Tasks of their own, but app-server emits
        # their lifecycle on the same connection.  These maps keep each child
        # fenced to the exact parent adapter generation that observed it.
        self._contexts_by_descendant: dict[str, _TurnContext] = {}
        self._children_by_thread: dict[str, set[str]] = {}
        self._thread_runtime: dict[str, _ThreadRuntimeState] = {}
        self._stderr_lines: deque[str] = deque(maxlen=100)
        self._request_id = 0
        self._skills_revision = 0
        self._write_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._shutdown_requested = False
        # A shutdown intent is generation-bound.  ``_shutdown_requested`` is
        # published before the lifecycle lock to block new starts, but the
        # reader must only call an EOF planned after shutdown owns this exact
        # subprocess generation and is about to close its stdin.
        self._planned_shutdown: tuple[
            asyncio.subprocess.Process,
            CodexTurnProcess | None,
            str,
        ] | None = None
        self._observed_transport_exit: tuple[
            asyncio.subprocess.Process,
            tuple[
                asyncio.subprocess.Process,
                CodexTurnProcess | None,
                str,
            ] | None,
        ] | None = None
        self._finalized_transport_process: asyncio.subprocess.Process | None = None
        # App-server keeps completed threads loaded in memory.  The registry
        # uses this set to restart an idle target server before a migrated
        # rollout is resumed there again, avoiding stale in-memory history.
        self._known_threads: set[str] = set()

    @property
    def is_alive(self) -> bool:
        return self._process is not None and self._process.returncode is None

    @property
    def shutdown_requested(self) -> bool:
        return self._shutdown_requested

    @property
    def pid(self) -> int:
        return self._process.pid if self._process else 0

    @property
    def has_active_turns(self) -> bool:
        return any(
            context.process.returncode is None
            for context in self._contexts_by_thread.values()
        )

    def has_active_thread(self, thread_id: str) -> bool:
        context = self._contexts_by_thread.get(thread_id)
        return bool(context and context.process.returncode is None)

    def owns_live_turn_process(self, process: CodexTurnProcess) -> bool:
        """Return whether this server owns the exact live adapter generation."""

        return any(
            context.process is process and context.process.returncode is None
            for context in self._contexts_by_thread.values()
        )

    def has_other_live_turn_processes(self, process: CodexTurnProcess) -> bool:
        """Return whether another task still uses this account transport."""

        return any(
            context.process is not process
            and context.process.returncode is None
            for context in self._contexts_by_thread.values()
        )

    def knows_thread(self, thread_id: str) -> bool:
        return thread_id in self._known_threads

    def _detach_turn_context(self, context: _TurnContext) -> None:
        """Remove every identity mapping for one exact adapter generation."""

        admission_future = context.admission_observed_future
        if admission_future is not None and not admission_future.done():
            admission_future.cancel()
        guard_task = getattr(context, "descendant_guard_task", None)
        context.descendant_guard_task = None
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        if (
            guard_task is not None
            and guard_task is not current_task
            and not guard_task.done()
        ):
            guard_task.cancel()
        policy_task = getattr(context, "tool_policy_abort_task", None)
        context.tool_policy_abort_task = None
        if (
            policy_task is not None
            and policy_task is not current_task
            and not policy_task.done()
        ):
            policy_task.cancel()
        goal_tasks = set(getattr(context, "goal_guard_tasks", set()))
        if hasattr(context, "goal_guard_tasks"):
            context.goal_guard_tasks.clear()
        for goal_task in goal_tasks:
            if goal_task is not current_task and not goal_task.done():
                goal_task.cancel()
        state_changed = getattr(context, "descendant_state_changed", None)
        if state_changed is not None:
            state_changed.set()
        for thread_id in list(
            getattr(context, "descendant_thread_ids", set())
        ):
            if self._contexts_by_descendant.get(thread_id) is context:
                self._contexts_by_descendant.pop(thread_id, None)
        if hasattr(context, "descendant_thread_ids"):
            context.descendant_thread_ids.clear()
        if hasattr(context, "active_descendant_thread_ids"):
            context.active_descendant_thread_ids.clear()
        if self._contexts_by_thread.get(context.thread_id) is context:
            self._contexts_by_thread.pop(context.thread_id, None)
        for turn_id, candidate in list(self._contexts_by_turn.items()):
            if candidate is context:
                self._contexts_by_turn.pop(turn_id, None)

    def _bind_turn_context(
        self,
        context: _TurnContext,
        turn_id: str,
        *,
        observed: bool,
    ) -> bool:
        """Bind an adapter to one turn without leaving stale reverse mappings."""

        if not turn_id:
            return False
        existing = self._contexts_by_turn.get(turn_id)
        if existing is not None and existing is not context:
            logger.error(
                "Refusing to bind Codex turn %s for task %s over task %s",
                turn_id,
                context.task_id,
                existing.task_id,
            )
            return False
        old_turn_id = context.turn_id
        if (
            old_turn_id
            and old_turn_id != turn_id
            and self._contexts_by_turn.get(old_turn_id) is context
        ):
            self._contexts_by_turn.pop(old_turn_id, None)
        context.turn_id = turn_id
        if observed:
            context.observed_turn_id = turn_id
        self._contexts_by_turn[turn_id] = context
        return True

    def _alias_turn_context(
        self,
        context: _TurnContext,
        turn_id: str,
    ) -> bool:
        """Route a protocol alias without replacing the authoritative turn id.

        Codex can steer one ``turn/start`` submission into an already-active
        native goal turn. Notifications then legitimately alternate between
        the submission id and the active turn id. Both ids must reach the same
        adapter generation; treating the submission id as an unrelated turn
        drops assistant output and the terminal event, leaving the worker
        permanently ``running``.
        """

        if not turn_id:
            return False
        existing = self._contexts_by_turn.get(turn_id)
        if existing is not None and existing is not context:
            logger.error(
                "Refusing to alias Codex turn %s for task %s over task %s",
                turn_id,
                context.task_id,
                existing.task_id,
            )
            return False
        self._contexts_by_turn[turn_id] = context
        return True

    def _promote_correlated_turn_context(
        self,
        context: _TurnContext,
        turn_id: str,
    ) -> bool:
        """Promote one client-id-proven native turn while retaining aliases."""

        # The RPC submission id can continue to appear on item notifications
        # after Codex adopts the input into another native turn.  Keep it as an
        # alias; detach_turn_context removes every alias for this generation.
        if context.admitted_turn_id:
            if not self._alias_turn_context(context, context.admitted_turn_id):
                return False
        if not self._alias_turn_context(context, turn_id):
            return False
        if context.observed_turn_id in {
            None,
            context.admitted_turn_id,
            turn_id,
        }:
            context.turn_id = turn_id
            context.observed_turn_id = turn_id
        return True

    def _interrupt_context_is_current(self, context: _TurnContext) -> bool:
        """Return whether an exact turn context still owns live work."""

        if context.process.returncode is not None:
            return False
        if self._contexts_by_thread.get(context.thread_id) is not context:
            raise CodexAppServerError(
                f"Codex thread {context.thread_id} changed owner during interrupt"
            )
        return True

    @staticmethod
    def _notification_turn_id(
        method: str,
        params: dict[str, Any],
    ) -> str | None:
        """Return the real turn id from either v2 notification shape."""

        if method in {"turn/started", "turn/completed"}:
            turn = params.get("turn")
            if isinstance(turn, dict) and turn.get("id"):
                return str(turn["id"])
        turn_id = params.get("turnId")
        return str(turn_id) if turn_id else None

    @staticmethod
    def _notification_user_message_client_ids(
        method: str,
        params: dict[str, Any],
    ) -> set[str]:
        """Return schema-backed client ids proving which input owns a turn."""

        items: list[Any] = []
        if method in {"item/started", "item/completed"}:
            items.append(params.get("item"))
        elif method == "turn/completed":
            turn = params.get("turn")
            if isinstance(turn, dict) and isinstance(turn.get("items"), list):
                items.extend(turn["items"])
        result: set[str] = set()
        for item in items:
            if not isinstance(item, dict) or item.get("type") != "userMessage":
                continue
            client_id = item.get("clientId")
            if client_id:
                result.add(str(client_id))
        return result

    def _notification_matches_context_input(
        self,
        context: _TurnContext,
        method: str,
        params: dict[str, Any],
    ) -> bool:
        client_id = context.client_user_message_id
        return bool(
            client_id
            and client_id
            in self._notification_user_message_client_ids(method, params)
        )

    def _tool_free_context_for_params(
        self,
        params: Any,
    ) -> _TurnContext | None:
        """Resolve an exact tool-free generation from v1 or v2 identities."""

        if not isinstance(params, dict):
            return None
        turn_id = params.get("turnId")
        if turn_id:
            context = self._contexts_by_turn.get(str(turn_id))
            if context is not None and context.tools_disabled:
                return context
        thread_id = params.get("threadId") or params.get("conversationId")
        if thread_id:
            context = self._contexts_by_thread.get(str(thread_id))
            if context is not None and context.tools_disabled:
                return context
        return None

    async def _abort_tool_free_violation(
        self,
        context: _TurnContext,
        reason: str,
    ) -> None:
        """Interrupt one violating turn before publishing a hard failure."""

        try:
            await self._interrupt_turn_context(context)
        except BaseException:
            # Do not detach or claim failure while the native generation may
            # still execute. Its terminal notification (or transport shutdown)
            # remains responsible for closing the adapter.
            logger.exception(
                "Codex tool-free policy interrupt failed task=%s thread=%s",
                context.task_id,
                context.thread_id,
            )
            return
        if not self._context_is_current(context):
            return
        context.process.feed({
            "type": "turn.failed",
            "error": {
                "message": reason,
                "code": "ccm_tool_policy_violation",
            },
            "turn_id": context.turn_id,
        })
        self._detach_turn_context(context)
        context.process.finish(1, reason)

    def _schedule_tool_free_violation(
        self,
        context: _TurnContext,
        source: str,
    ) -> None:
        """Record the first unexpected capability use and fail it once."""

        if (
            not context.tools_disabled
            or context.process.returncode is not None
            or context.tool_policy_violation is not None
        ):
            return
        reason = (
            "Codex PR review attempted a forbidden tool or autonomous "
            f"capability: {source}"
        )
        context.tool_policy_violation = reason
        context.tool_policy_abort_task = asyncio.create_task(
            self._abort_tool_free_violation(context, reason),
        )

    @staticmethod
    def _thread_status_type(status: Any) -> str | None:
        if not isinstance(status, dict):
            return None
        status_type = status.get("type")
        return str(status_type) if status_type else None

    @staticmethod
    def _thread_status_is_terminal(status_type: str | None) -> bool:
        return status_type in {"idle", "systemError", "notLoaded"}

    def _runtime_state_for(self, thread_id: str) -> _ThreadRuntimeState:
        return self._thread_runtime.setdefault(
            thread_id,
            _ThreadRuntimeState(),
        )

    def _context_is_current(self, context: _TurnContext) -> bool:
        return (
            context.process.returncode is None
            and self._contexts_by_thread.get(context.thread_id) is context
        )

    def _lineage_context_for_thread(
        self,
        thread_id: str,
    ) -> _TurnContext | None:
        context = self._contexts_by_thread.get(thread_id)
        if context is None:
            context = self._contexts_by_descendant.get(thread_id)
        return context if context is not None and self._context_is_current(context) else None

    @staticmethod
    def _signal_descendant_state_change(context: _TurnContext) -> None:
        changed = context.descendant_state_changed
        if changed is not None:
            changed.set()

    def _mark_descendant_active(
        self,
        context: _TurnContext,
        thread_id: str,
    ) -> None:
        if not self._context_is_current(context):
            return
        context.active_descendant_thread_ids.add(thread_id)
        self._signal_descendant_state_change(context)

    def _mark_descendant_terminal(
        self,
        context: _TurnContext,
        thread_id: str,
    ) -> None:
        context.active_descendant_thread_ids.discard(thread_id)
        self._signal_descendant_state_change(context)

    def _contexts_tracking_descendant(
        self,
        thread_id: str,
    ) -> list[_TurnContext]:
        contexts: list[_TurnContext] = []
        mapped = self._contexts_by_descendant.get(thread_id)
        if mapped is not None and self._context_is_current(mapped):
            contexts.append(mapped)
        # A lineage conflict is fail-closed: the secondary context retains
        # the child in its own set and reconciles it by thread/read even though
        # the fast reverse lookup remains owned by the first generation.
        for candidate in self._contexts_by_thread.values():
            if (
                candidate is not mapped
                and thread_id
                in getattr(candidate, "descendant_thread_ids", set())
                and self._context_is_current(candidate)
            ):
                contexts.append(candidate)
        return contexts

    def _record_thread_status(
        self,
        thread_id: str,
        status: Any,
    ) -> None:
        status_type = self._thread_status_type(status)
        if status_type is None:
            return
        runtime = self._runtime_state_for(thread_id)
        runtime.status_type = status_type
        terminal = self._thread_status_is_terminal(status_type)
        if terminal:
            # App-server's status manager publishes a non-active status only
            # after it has cleared the native running-turn fact.
            runtime.active_turn_ids.clear()
        for context in self._contexts_tracking_descendant(thread_id):
            if status_type == "active":
                self._mark_descendant_active(context, thread_id)
            elif terminal:
                self._mark_descendant_terminal(context, thread_id)

    def _record_thread_turn_lifecycle(
        self,
        method: str,
        thread_id: str,
        turn_id: str | None,
    ) -> None:
        runtime = self._runtime_state_for(thread_id)
        if method == "turn/started":
            if turn_id:
                runtime.active_turn_ids.add(turn_id)
            runtime.status_type = "active"
            for context in self._contexts_tracking_descendant(thread_id):
                self._mark_descendant_active(context, thread_id)
            return
        if method != "turn/completed":
            return
        if turn_id:
            runtime.active_turn_ids.discard(turn_id)
        if not runtime.active_turn_ids:
            # ``turn/completed`` itself is also an authoritative no-running-
            # turn proof if a status notification was missed during listener
            # attachment.
            runtime.status_type = "idle"
            for context in self._contexts_tracking_descendant(thread_id):
                self._mark_descendant_terminal(context, thread_id)

    def _record_child_relation(
        self,
        parent_thread_id: str,
        child_thread_id: str,
    ) -> None:
        if not parent_thread_id or not child_thread_id:
            return
        if parent_thread_id == child_thread_id:
            logger.error(
                "Ignoring cyclic Codex child-thread relation %s",
                child_thread_id,
            )
            return
        self._children_by_thread.setdefault(parent_thread_id, set()).add(
            child_thread_id
        )

    def _attach_descendant(
        self,
        context: _TurnContext,
        thread_id: str,
        *,
        active: bool | None,
        _seen: set[str] | None = None,
    ) -> None:
        if not thread_id or thread_id == context.thread_id:
            return
        if not self._context_is_current(context):
            return
        seen = _seen if _seen is not None else set()
        if thread_id in seen:
            return
        seen.add(thread_id)
        context.descendant_thread_ids.add(thread_id)
        owner = self._contexts_by_descendant.get(thread_id)
        if owner is None or owner is context or not self._context_is_current(owner):
            self._contexts_by_descendant[thread_id] = context
        elif owner is not context:
            logger.error(
                "Codex descendant %s is already fenced to task %s; "
                "task %s will reconcile it without stealing ownership",
                thread_id,
                owner.task_id,
                context.task_id,
            )

        runtime = self._thread_runtime.get(thread_id)
        if active is True:
            self._mark_descendant_active(context, thread_id)
        elif active is False:
            self._mark_descendant_terminal(context, thread_id)
        elif runtime is None:
            # A discovered child with no status proof is a blocker, not an
            # implicit success.
            self._mark_descendant_active(context, thread_id)
        elif (
            runtime.status_type == "active"
            or bool(runtime.active_turn_ids)
        ):
            self._mark_descendant_active(context, thread_id)
        elif self._thread_status_is_terminal(runtime.status_type):
            self._mark_descendant_terminal(context, thread_id)
        else:
            self._mark_descendant_active(context, thread_id)

        for child_id in self._children_by_thread.get(thread_id, set()):
            self._attach_descendant(
                context,
                child_id,
                active=None,
                _seen=seen,
            )

    def _track_collaboration_item(
        self,
        context: _TurnContext,
        event_thread_id: str,
        item: Any,
    ) -> None:
        if not isinstance(item, dict):
            return
        item_type = item.get("type")
        if item_type in {"subAgentActivity", "sub_agent_activity"}:
            child_id = item.get("agentThreadId") or item.get("agent_thread_id")
            if not child_id:
                return
            child_id = str(child_id)
            self._record_child_relation(event_thread_id, child_id)
            kind = str(item.get("kind") or "")
            if kind == "interrupted":
                active: bool | None = False
            elif kind == "interacted":
                # MultiAgentV2 uses the same item for queue-only send_message
                # and turn-starting followup_task.  Do not guess: block first,
                # then thread/status or thread/read supplies the proof.
                active = True
            elif kind == "started":
                # This item is newer than any cached idle observation.  A
                # delayed child turn/status notification must not let stale
                # runtime state publish the parent as complete.
                active = True
            else:
                active = None
            self._attach_descendant(context, child_id, active=active)
            return

        if item_type not in {
            "collabAgentToolCall",
            "collab_agent_tool_call",
        }:
            return
        sender_id = (
            item.get("senderThreadId")
            or item.get("sender_thread_id")
            or event_thread_id
        )
        sender_id = str(sender_id)
        receiver_ids = (
            item.get("receiverThreadIds")
            if "receiverThreadIds" in item
            else item.get("receiver_thread_ids")
        )
        if not isinstance(receiver_ids, list):
            receiver_ids = []
        agent_states = (
            item.get("agentsStates")
            if "agentsStates" in item
            else item.get("agents_states")
        )
        if not isinstance(agent_states, dict):
            agent_states = {}
        tool = str(item.get("tool") or "")
        call_status = str(item.get("status") or "")
        active_agent_statuses = {"pendingInit", "pending_init", "running"}
        terminal_agent_statuses = {
            "interrupted",
            "completed",
            "errored",
            "shutdown",
            "notFound",
            "not_found",
        }
        for raw_child_id in receiver_ids:
            child_id = str(raw_child_id)
            self._record_child_relation(sender_id, child_id)
            state = agent_states.get(child_id)
            agent_status = (
                str(state.get("status") or "")
                if isinstance(state, dict)
                else ""
            )
            active: bool | None
            if agent_status in active_agent_statuses:
                active = True
            elif agent_status in terminal_agent_statuses:
                active = False
            elif tool == "closeAgent" and call_status == "completed":
                active = False
            elif (
                tool in {"spawnAgent", "sendInput", "resumeAgent"}
                and call_status != "failed"
            ):
                active = True
            else:
                active = None
            self._attach_descendant(context, child_id, active=active)

    async def _read_descendant_status(
        self,
        thread_id: str,
    ) -> tuple[str, str | None, set[str] | None]:
        try:
            response = await asyncio.wait_for(
                self._request(
                    "thread/read",
                    {"threadId": thread_id, "includeTurns": True},
                ),
                timeout=min(
                    max(0.01, self.request_timeout),
                    _DESCENDANT_RECONCILE_REQUEST_TIMEOUT,
                ),
            )
        except (asyncio.TimeoutError, CodexAppServerError):
            return thread_id, None, None
        except Exception:
            logger.exception(
                "Could not reconcile Codex descendant thread %s",
                thread_id,
            )
            return thread_id, None, None
        thread = response.get("thread") if isinstance(response, dict) else None
        if (
            not isinstance(thread, dict)
            or str(thread.get("id") or "") != thread_id
        ):
            return thread_id, None, None
        active_turn_ids: set[str] | None = None
        turns = thread.get("turns")
        if isinstance(turns, list):
            active_turn_ids = set()
            for turn in turns:
                if not isinstance(turn, dict) or not turn.get("id"):
                    continue
                status = str(turn.get("status") or "")
                normalized_status = status.replace("_", "").lower()
                if normalized_status in {
                    "active",
                    "inprogress",
                    "running",
                }:
                    active_turn_ids.add(str(turn["id"]))
        return (
            thread_id,
            self._thread_status_type(thread.get("status")),
            active_turn_ids,
        )

    def _apply_descendant_read_state(
        self,
        thread_id: str,
        status_type: str | None,
        active_turn_ids: set[str] | None,
    ) -> None:
        if status_type is None:
            return
        runtime = self._runtime_state_for(thread_id)
        if active_turn_ids is not None:
            runtime.active_turn_ids = set(active_turn_ids)
        self._record_thread_status(
            thread_id,
            {"type": status_type},
        )

    async def _reconcile_descendant_statuses(
        self,
        context: _TurnContext,
    ) -> None:
        blockers = tuple(context.active_descendant_thread_ids)
        if not blockers or not self._context_is_current(context):
            return
        results = await asyncio.gather(
            *(self._read_descendant_status(thread_id) for thread_id in blockers)
        )
        if not self._context_is_current(context):
            return
        for thread_id, status_type, active_turn_ids in results:
            self._apply_descendant_read_state(
                thread_id,
                status_type,
                active_turn_ids,
            )

    def _promote_descendant_terminal_failure(
        self,
        context: _TurnContext,
    ) -> None:
        if not self._context_is_current(context):
            return
        blockers = sorted(context.active_descendant_thread_ids)
        message = (
            "Codex parent turn completed, but CCM could not prove all native "
            "sub-agents terminal before the safety deadline"
        )
        if blockers:
            message += f": {', '.join(blockers)}"
        context.deferred_terminal_notification = {
            "threadId": context.thread_id,
            "turn": {
                "id": context.turn_id,
                "status": "failed",
                "error": {"message": message},
            },
        }
        logger.error(
            "%s; retaining the adapter until cleanup is authoritative",
            message,
        )

    async def _wait_descendant_terminal(
        self,
        context: _TurnContext,
        thread_id: str,
    ) -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _DESCENDANT_INTERRUPT_CONFIRM_TIMEOUT
        while (
            self._context_is_current(context)
            and thread_id in context.active_descendant_thread_ids
        ):
            (
                _,
                status_type,
                active_turn_ids,
            ) = await self._read_descendant_status(thread_id)
            self._apply_descendant_read_state(
                thread_id,
                status_type,
                active_turn_ids,
            )
            if thread_id not in context.active_descendant_thread_ids:
                return True
            remaining = deadline - loop.time()
            if remaining <= 0:
                return False
            changed = context.descendant_state_changed
            if changed is None:
                changed = asyncio.Event()
                context.descendant_state_changed = changed
            changed.clear()
            try:
                await asyncio.wait_for(
                    changed.wait(),
                    timeout=min(
                        _DESCENDANT_INTERRUPT_POLL_INTERVAL,
                        remaining,
                    ),
                )
            except asyncio.TimeoutError:
                pass
        return (
            not self._context_is_current(context)
            or thread_id not in context.active_descendant_thread_ids
        )

    async def _interrupt_one_descendant(
        self,
        context: _TurnContext,
        thread_id: str,
    ) -> bool:
        if thread_id not in context.active_descendant_thread_ids:
            return True
        runtime = self._thread_runtime.get(thread_id)
        active_turn_ids = (
            set(runtime.active_turn_ids)
            if runtime is not None
            else set()
        )
        if len(active_turn_ids) != 1:
            (
                _,
                status_type,
                read_active_turn_ids,
            ) = await self._read_descendant_status(thread_id)
            self._apply_descendant_read_state(
                thread_id,
                status_type,
                read_active_turn_ids,
            )
            if thread_id not in context.active_descendant_thread_ids:
                return True
            runtime = self._thread_runtime.get(thread_id)
            active_turn_ids = (
                set(runtime.active_turn_ids)
                if runtime is not None
                else set()
            )
        if len(active_turn_ids) != 1:
            # Native Goals (including collaboration agents waiting on their
            # own descendants) can keep a thread ``active`` without exposing
            # a running turn in ``thread/read``.  A turn interrupt is
            # impossible in that state, but the Goal protocol still provides
            # an exact, thread-scoped stop.  Pause it before giving up; this
            # avoids forcing callers to kill the account-wide shared
            # app-server just to release one task.
            try:
                goal_paused = await self._pause_active_goal(thread_id)
            except (asyncio.TimeoutError, CodexAppServerError):
                goal_paused = False
            if goal_paused:
                return await self._wait_descendant_terminal(
                    context,
                    thread_id,
                )
            return False

        turn_id = next(iter(active_turn_ids))
        for attempt in range(2):
            try:
                await self._request(
                    "turn/interrupt",
                    {"threadId": thread_id, "turnId": turn_id},
                )
                break
            except CodexAppServerError as exc:
                actual_turn_id = _active_turn_id_from_error(exc)
                if attempt or not actual_turn_id:
                    return await self._wait_descendant_terminal(
                        context,
                        thread_id,
                    )
                turn_id = actual_turn_id
                runtime = self._runtime_state_for(thread_id)
                runtime.active_turn_ids = {turn_id}
            except asyncio.TimeoutError:
                return await self._wait_descendant_terminal(
                    context,
                    thread_id,
                )
            except Exception:
                logger.exception(
                    "Could not interrupt Codex descendant turn "
                    "thread=%s turn=%s",
                    thread_id,
                    turn_id,
                )
                return False
        return await self._wait_descendant_terminal(context, thread_id)

    async def _interrupt_active_descendants(
        self,
        context: _TurnContext,
    ) -> bool:
        lock = context.descendant_interrupt_lock
        if lock is None:
            lock = asyncio.Lock()
            context.descendant_interrupt_lock = lock
        async with lock:
            if not self._context_is_current(context):
                return True
            blockers = tuple(context.active_descendant_thread_ids)
            if not blockers:
                return True
            results = await asyncio.gather(
                *(
                    self._interrupt_one_descendant(context, thread_id)
                    for thread_id in blockers
                )
            )
            return (
                all(results)
                and not context.active_descendant_thread_ids
            )

    async def _guard_deferred_terminal(
        self,
        context: _TurnContext,
    ) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _DESCENDANT_TERMINAL_TIMEOUT
        try:
            # Let already-buffered child and parent notifications drain before
            # evaluating the first terminal snapshot.
            await asyncio.sleep(0)
            while (
                self._context_is_current(context)
                and context.deferred_terminal_notification is not None
            ):
                changed = context.descendant_state_changed
                if changed is None:
                    changed = asyncio.Event()
                    context.descendant_state_changed = changed
                changed.clear()
                if not context.active_descendant_thread_ids:
                    # A second loop turn closes the status-idle /
                    # child-turn-completed ordering window and preserves any
                    # late parent output before EOF.
                    await asyncio.sleep(0)
                    if (
                        self._context_is_current(context)
                        and context.deferred_terminal_notification is not None
                        and not context.active_descendant_thread_ids
                    ):
                        params = context.deferred_terminal_notification
                        context.deferred_terminal_notification = None
                        self._finish_turn_context(context, params)
                    return

                remaining = deadline - loop.time()
                if remaining <= 0:
                    turn = (
                        context.deferred_terminal_notification.get("turn")
                        or {}
                    )
                    if turn.get("status") == "completed":
                        self._promote_descendant_terminal_failure(context)
                    remaining = _DESCENDANT_RECONCILE_INTERVAL

                turn = (
                    context.deferred_terminal_notification.get("turn")
                    or {}
                )
                if turn.get("status") != "completed":
                    await self._interrupt_active_descendants(context)
                    if not self._context_is_current(context):
                        return
                    if not context.active_descendant_thread_ids:
                        continue
                try:
                    await asyncio.wait_for(
                        changed.wait(),
                        timeout=min(
                            _DESCENDANT_RECONCILE_INTERVAL,
                            remaining,
                        ),
                    )
                except asyncio.TimeoutError:
                    await self._reconcile_descendant_statuses(context)
        except asyncio.CancelledError:
            return
        finally:
            if context.descendant_guard_task is asyncio.current_task():
                context.descendant_guard_task = None

    def _defer_terminal_turn_for_descendants(
        self,
        context: _TurnContext,
        params: dict[str, Any],
    ) -> None:
        if context.deferred_terminal_notification is None:
            context.deferred_terminal_notification = dict(params)
        else:
            logger.warning(
                "Ignoring duplicate deferred Codex terminal notification "
                "thread=%s turn=%s",
                context.thread_id,
                context.turn_id,
            )
            return
        turn = params.get("turn") or {}
        runtime = self._thread_runtime.get(context.thread_id)
        goal_may_continue = bool(
            context.following_native_goal
            or (
                runtime is not None
                and runtime.goal_status == "active"
            )
        )
        if turn.get("status", "completed") == "completed" and goal_may_continue:
            # Goal continuation is allowed as soon as the root thread is idle,
            # even while native descendants from the older turn are still
            # winding down. Release the completed root identity now so its
            # next turn can bind without dropping early output. The newer turn
            # invalidates this older deferred terminal below, while descendant
            # ownership remains attached to the shared context.
            self._reset_goal_turn_identity(context)
            self._mark_following_native_goal(context)
        guard = context.descendant_guard_task
        if guard is None or guard.done():
            context.descendant_guard_task = asyncio.create_task(
                self._guard_deferred_terminal(context),
            )

    def _reset_goal_turn_identity(self, context: _TurnContext) -> None:
        """Release one completed turn while retaining its thread owner."""

        for turn_id, candidate in list(self._contexts_by_turn.items()):
            if candidate is context:
                self._contexts_by_turn.pop(turn_id, None)
        context.turn_id = None
        context.admitted_turn_id = None
        context.observed_turn_id = None
        context.pending_terminal_notification = None

    def _mark_following_native_goal(self, context: _TurnContext) -> None:
        if context.following_native_goal:
            return
        context.following_native_goal = True
        context.process.feed({
            "type": "system_event",
            "content": "Codex 原生 Goal 仍在运行，CCM 将继续跟踪后续回合",
            "native_goal_status": "active",
            "thread_id": context.thread_id,
        })

    def _confirm_goal_continuation_started(
        self,
        context: _TurnContext,
    ) -> None:
        """Let a newer turn invalidate the older turn's Goal-state query."""

        if context.pending_goal_terminal_notification is not None:
            context.pending_goal_terminal_notification = None
            context.goal_terminal_generation += 1
        if context.deferred_terminal_notification is not None:
            context.deferred_terminal_notification = None
            guard = context.descendant_guard_task
            context.descendant_guard_task = None
            if guard is not None and not guard.done():
                guard.cancel()
            state_changed = context.descendant_state_changed
            if state_changed is not None:
                state_changed.set()
        self._mark_following_native_goal(context)
        context.usage = None
        context.first_input_seen = False
        context.first_output_seen = False
        context.non_retry_error = None

    async def _guard_native_goal_terminal(
        self,
        context: _TurnContext,
        params: dict[str, Any],
        generation: int,
    ) -> None:
        """Keep the CCM process alive while app-server reports an active Goal."""

        while True:
            try:
                goal = await self._read_thread_goal(context.thread_id)
            except asyncio.CancelledError:
                return
            except Exception:
                if (
                    not self._context_is_current(context)
                    or context.goal_terminal_generation != generation
                    or context.pending_goal_terminal_notification is None
                ):
                    return
                # This guard only exists because an authoritative Goal event or
                # an already-followed generation said work may continue. An
                # unavailable local RPC cannot safely downgrade that evidence
                # to success; retain ownership and reconcile again.
                logger.exception(
                    "Could not inspect native Goal after Codex turn completion; "
                    "retaining generation and retrying task=%s thread=%s",
                    context.task_id,
                    context.thread_id,
                )
                try:
                    await asyncio.sleep(_GOAL_RECONCILE_INTERVAL)
                except asyncio.CancelledError:
                    return
                continue

            if (
                not self._context_is_current(context)
                or context.goal_terminal_generation != generation
                or context.pending_goal_terminal_notification is None
            ):
                return

            runtime = self._thread_runtime.get(context.thread_id)
            newer_turn_active = bool(
                context.turn_id
                or (
                    runtime is not None
                    and (
                        runtime.status_type == "active"
                        or runtime.active_turn_ids
                    )
                )
            )
            if (
                isinstance(goal, dict)
                and goal.get("status") == "active"
            ) or newer_turn_active:
                # Keep the completed turn snapshot until either a newer turn
                # starts (and supersedes it) or a terminal Goal notification
                # proves that the retained between-turn generation can close.
                self._mark_following_native_goal(context)
                return

            context.pending_goal_terminal_notification = None
            self._publish_turn_context_terminal(context, params)
            return

    def _finish_retained_goal_if_terminal(
        self,
        context: _TurnContext,
        goal_status: str | None,
    ) -> None:
        """Close an idle retained Goal only after a terminal Goal event."""

        pending = context.pending_goal_terminal_notification
        if pending is None or goal_status == "active":
            return
        runtime = self._thread_runtime.get(context.thread_id)
        if (
            context.turn_id
            or (
                runtime is not None
                and (
                    runtime.status_type == "active"
                    or runtime.active_turn_ids
                )
            )
        ):
            # A terminal update emitted from inside a live turn is finalized by
            # that turn's own turn/completed notification, not the older one.
            return
        context.pending_goal_terminal_notification = None
        context.goal_terminal_generation += 1
        self._publish_turn_context_terminal(context, pending)

    def _defer_terminal_turn_for_native_goal(
        self,
        context: _TurnContext,
        params: dict[str, Any],
    ) -> None:
        """Query Goal state without opening a turn-boundary routing gap."""

        context.goal_terminal_generation += 1
        generation = context.goal_terminal_generation
        context.pending_goal_terminal_notification = dict(params)
        self._reset_goal_turn_identity(context)
        guard = asyncio.create_task(
            self._guard_native_goal_terminal(
                context,
                dict(params),
                generation,
            ),
        )
        context.goal_guard_tasks.add(guard)
        guard.add_done_callback(context.goal_guard_tasks.discard)

    def _finish_turn_context(
        self,
        context: _TurnContext,
        params: dict[str, Any],
    ) -> None:
        """Finish a regular turn or retain it for an active native Goal."""

        turn = params.get("turn") or {}
        runtime = self._thread_runtime.get(context.thread_id)
        goal_may_continue = bool(
            context.following_native_goal
            or (
                runtime is not None
                and runtime.goal_status == "active"
            )
        )
        if (
            turn.get("status", "completed") == "completed"
            and goal_may_continue
            and context.tool_policy_violation is None
            and context.non_retry_error is None
            and not context.tools_disabled
        ):
            self._defer_terminal_turn_for_native_goal(context, params)
            return
        self._publish_turn_context_terminal(context, params)

    def _publish_turn_context_terminal(
        self,
        context: _TurnContext,
        params: dict[str, Any],
    ) -> None:
        """Publish the final native terminal and close its CCM adapter."""

        if not self._context_is_current(context):
            return
        turn = params.get("turn") or {}
        terminal_turn_id = turn.get("id") or context.turn_id
        status = turn.get("status") or "completed"
        error = turn.get("error")
        if context.tool_policy_violation is not None:
            normalized_error = {
                "message": context.tool_policy_violation,
                "code": "ccm_tool_policy_violation",
            }
            context.process.feed(
                {"type": "turn.failed", "error": normalized_error}
            )
            status = "toolPolicyViolation"
            exit_code = 1
            stderr = context.tool_policy_violation
        elif status == "completed":
            context.process.feed(
                {
                    "type": "turn.completed",
                    "usage": context.usage or {},
                    "turn_id": terminal_turn_id,
                }
            )
            exit_code = 0
            stderr = ""
        elif status == "interrupted":
            context.process.feed(
                {
                    "type": "turn.completed",
                    "usage": context.usage or {},
                    "turn_id": terminal_turn_id,
                }
            )
            exit_code = 130
            stderr = ""
        else:
            normalized_error = self._normalize_turn_error(
                error,
                fallback=f"Codex turn ended with status {status}",
            )
            message = normalized_error["message"]
            context.process.feed(
                {"type": "turn.failed", "error": normalized_error}
            )
            exit_code = 1
            stderr = str(message)
        if context.non_retry_error is not None and exit_code != 1:
            # Match native `codex exec`: any ErrorNotification with
            # willRetry=false makes the turn fail even if a later terminal
            # notification reports completed/interrupted.
            exit_code = 1
            stderr = str(context.non_retry_error["message"])
        logger.info(
            "Codex latency task=%s thread=%s stage=completed elapsed_ms=%.1f status=%s",
            context.task_id,
            context.thread_id,
            (time.perf_counter() - context.launch_started) * 1000,
            status,
        )
        runtime = self._thread_runtime.get(context.thread_id)
        if runtime is not None:
            aliases = {
                turn_id
                for turn_id, candidate in self._contexts_by_turn.items()
                if candidate is context
            }
            runtime.active_turn_ids.difference_update(aliases)
            if not runtime.active_turn_ids:
                runtime.status_type = "idle"
        context.process.finish(
            exit_code,
            stderr,
            termination_kind=(
                "user_interrupt"
                if status == "interrupted" and exit_code in (-2, 130)
                else None
            ),
        )
        self._detach_turn_context(context)

    async def _pause_active_goal(self, thread_id: str) -> bool:
        """Pause an adopted native goal with one bounded protocol round trip."""

        try:
            await self._request(
                "thread/goal/set",
                {"threadId": thread_id, "status": "paused"},
            )
        except CodexAppServerError as exc:
            # Submission ids can be adopted by any steerable active turn, not
            # only a native goal turn. A build with Goals explicitly disabled
            # or a regular thread with no goal reports one of these errors.
            # There is then nothing to pause, so continue to interrupt the
            # authoritative active turn. Other failures remain fail-closed.
            if (
                _GOALS_FEATURE_DISABLED_RE.search(str(exc))
                or _NO_ACTIVE_GOAL_RE.search(str(exc))
            ):
                logger.debug(
                    "Codex thread %s has no pausable native goal; "
                    "continuing direct turn interrupt",
                    thread_id,
                )
                return False
            raise
        return True

    async def _steer_detached_native_goal(
        self,
        context: _TurnContext,
        steer_input: list[dict[str, Any]],
    ) -> str:
        """Adopt one active Goal turn through exact identity reconciliation."""

        probe_turn_id = "ccm-adopt-probe"
        expected_turn_id = probe_turn_id
        for attempt in range(2):
            try:
                response = await self._request(
                    "turn/steer",
                    {
                        "threadId": context.thread_id,
                        "expectedTurnId": expected_turn_id,
                        "input": steer_input,
                    },
                )
            except CodexAppServerError as exc:
                actual_turn_id = (
                    _active_turn_id_from_error(exc)
                    or context.turn_id
                )
                if attempt or not actual_turn_id:
                    raise CodexThreadNotIdleError(
                        context.thread_id,
                        "active-goal-turn-boundary",
                        operation="native Goal adoption",
                    ) from exc
                expected_turn_id = str(actual_turn_id)
                if not self._bind_turn_context(
                    context,
                    expected_turn_id,
                    observed=True,
                ):
                    raise CodexAppServerError(
                        "Could not bind authoritative Codex Goal turn "
                        f"{expected_turn_id}"
                    ) from exc
                continue

            response_turn_id = (
                response.get("turnId")
                if isinstance(response, dict)
                else None
            )
            if not response_turn_id:
                raise CodexAppServerError(
                    "turn/steer returned no turn id during native Goal adoption"
                )
            response_turn_id = str(response_turn_id)
            if (
                expected_turn_id != probe_turn_id
                and response_turn_id != expected_turn_id
            ):
                raise CodexAppServerError(
                    "turn/steer changed the authoritative native Goal turn "
                    f"from {expected_turn_id} to {response_turn_id}"
                )
            if not self._bind_turn_context(
                context,
                response_turn_id,
                observed=True,
            ):
                raise CodexAppServerError(
                    "Could not bind adopted Codex Goal turn "
                    f"{response_turn_id}"
                )
            return response_turn_id
        raise CodexAppServerError("Could not adopt active native Goal turn")

    async def _interrupt_turn_context(self, context: _TurnContext) -> None:
        """Interrupt the actual active turn, reconciling steer-style admission ids."""

        if not self._interrupt_context_is_current(context):
            return
        if context.pending_goal_terminal_notification is not None:
            # The stop request owns this turn boundary now. Prevent a
            # concurrent Goal-state guard from publishing ordinary completion
            # while pause/read is proving the interrupt outcome.
            context.goal_terminal_generation += 1
        # Once the native parent has already reported terminal, only its
        # descendants remain interruptible.  Re-sending a root interrupt would
        # fail with "no active turn" and skip the real cleanup target.
        if context.deferred_terminal_notification is None:
            goal_checked = False
            if context.following_native_goal:
                await self._pause_active_goal(context.thread_id)
                goal_checked = True
                if not self._interrupt_context_is_current(context):
                    return

            turn_id = context.turn_id
            if not turn_id:
                (
                    _,
                    status_type,
                    active_turn_ids,
                ) = await self._read_descendant_status(context.thread_id)
                if not self._interrupt_context_is_current(context):
                    return
                if status_type == "idle" and not active_turn_ids:
                    pending = context.pending_goal_terminal_notification or {}
                    pending_turn = pending.get("turn") or {}
                    context.pending_goal_terminal_notification = None
                    context.goal_terminal_generation += 1
                    self._publish_turn_context_terminal(context, {
                        "threadId": context.thread_id,
                        "turn": {
                            "id": pending_turn.get("id"),
                            "status": "interrupted",
                            "error": None,
                        },
                    })
                    return
                if active_turn_ids and len(active_turn_ids) == 1:
                    turn_id = next(iter(active_turn_ids))
                    if not self._bind_turn_context(
                        context,
                        turn_id,
                        observed=True,
                    ):
                        raise CodexAppServerError(
                            "Could not bind authoritative Codex Goal turn "
                            f"{turn_id} during interrupt"
                        )
                else:
                    raise CodexAppServerError(
                        f"Codex thread {context.thread_id} has no proven "
                        "interruptible Goal turn"
                    )

            if not turn_id:
                raise CodexAppServerError(
                    f"Codex thread {context.thread_id} has no interruptible turn id"
                )

            if (
                context.admitted_turn_id is not None
                and turn_id != context.admitted_turn_id
            ):
                await self._pause_active_goal(context.thread_id)
                goal_checked = True
                if not self._interrupt_context_is_current(context):
                    return

            for attempt in range(2):
                try:
                    await self._request(
                        "turn/interrupt",
                        {"threadId": context.thread_id, "turnId": turn_id},
                    )
                    break
                except CodexAppServerError as exc:
                    actual_turn_id = _active_turn_id_from_error(exc)
                    if attempt or not actual_turn_id:
                        raise
                    if not self._interrupt_context_is_current(context):
                        return
                    if not goal_checked:
                        await self._pause_active_goal(context.thread_id)
                        goal_checked = True
                        if not self._interrupt_context_is_current(context):
                            return
                    if not self._bind_turn_context(
                        context,
                        actual_turn_id,
                        observed=True,
                    ):
                        raise CodexAppServerError(
                            "Could not bind authoritative Codex active turn "
                            f"{actual_turn_id}"
                        ) from exc
                    turn_id = actual_turn_id

        if (
            self._interrupt_context_is_current(context)
            and not await self._interrupt_active_descendants(context)
        ):
            blockers = ", ".join(
                sorted(context.active_descendant_thread_ids)
            )
            raise CodexAppServerError(
                "Could not confirm Codex descendant cleanup"
                + (f": {blockers}" if blockers else "")
            )

    async def ensure_started(self) -> None:
        if self._shutdown_requested:
            raise CodexAppServerBusyError(
                f"Codex app-server is shutting down: {self.codex_home}"
            )
        if self.is_alive:
            return
        async with self._lifecycle_lock:
            if self._shutdown_requested:
                raise CodexAppServerBusyError(
                    f"Codex app-server is shutting down: {self.codex_home}"
                )
            if self.is_alive:
                return
            # A dead leader may still have descendants in the independent
            # app-server process group and keep stdout open.  Stop that exact
            # generation before waiting on its readers, or the reader wait can
            # hold this lifecycle lock forever and prevent shutdown itself.
            if self._process is not None:
                await self._shutdown_locked()
            elif any(
                task is not None and not task.done()
                for task in (self._reader_task, self._stderr_task)
            ):
                raise CodexAppServerError(
                    "Codex app-server reader remains without process evidence"
                )
            if self._shutdown_requested:
                raise CodexAppServerBusyError(
                    f"Codex app-server is shutting down: {self.codex_home}"
                )
            await self._start()

    async def read_rate_limits(self) -> dict[str, Any]:
        """Read the authenticated account's current rate-limit snapshot."""

        await self.ensure_started()
        # The v2 protocol declares this method with Option<()> params and the
        # official client omits the field. Sending ``params: {}`` is rejected
        # by Codex 0.144.6's serde decoder.
        return await self._request("account/rateLimits/read", None)

    async def _start(self) -> None:
        self._stderr_lines.clear()
        self._planned_shutdown = None
        self._observed_transport_exit = None
        self._finalized_transport_process = None
        self._skills_revision = 0
        # These facts belong to one app-server process generation.  Persisted
        # thread ownership is kept separately in ``_known_threads``.
        self._contexts_by_descendant.clear()
        self._children_by_thread.clear()
        self._thread_runtime.clear()
        codex_home = Path(self.codex_home)
        codex_home.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            os.chmod(codex_home, 0o700)
        except OSError:
            logger.warning("Could not enforce 0700 on CODEX_HOME %s", codex_home)
        env = {
            key: value
            for key, value in os.environ.items()
            if (
                key.upper() not in ("CLAUDECODE", "CLAUDE_CODE")
                and key.upper() not in self._env_remove
            )
        }
        env["CODEX_HOME"] = self.codex_home
        started = time.perf_counter()
        spawn_kwargs: dict[str, Any] = {
            "stdin": asyncio.subprocess.PIPE,
            "stdout": asyncio.subprocess.PIPE,
            "stderr": asyncio.subprocess.PIPE,
            "env": env,
            "limit": 10 * 1024 * 1024,
        }
        if os.name == "posix":
            spawn_kwargs["start_new_session"] = True
        app_server_args = [
            "app-server",
            "--enable",
            "fast_mode",
        ]
        if self._actual_tier_proxy_route is not None:
            proxy = CodexActualTierProxy(
                self._actual_tier_proxy_route,
                first_event_timeout=max(self.request_timeout, 60.0),
            )
            await proxy.start()
            self._actual_tier_proxy = proxy
            app_server_args.extend(proxy.codex_override_args())
        app_server_args.append("--stdio")
        try:
            self._process = await asyncio.create_subprocess_exec(
                self.binary,
                *app_server_args,
                **spawn_kwargs,
            )
        except BaseException:
            proxy = self._actual_tier_proxy
            self._actual_tier_proxy = None
            if proxy is not None:
                await proxy.close()
            raise
        process = self._process
        self._process_group_process = process if os.name == "posix" else None
        self._reader_task = asyncio.create_task(self._read_loop(process))
        self._stderr_task = asyncio.create_task(self._stderr_loop(process))
        try:
            await self._request(
                "initialize",
                {
                    "clientInfo": {
                        "name": "claude_code_manager",
                        "title": "Claude Code Manager",
                        "version": "0.1.0",
                    },
                    "capabilities": {"experimentalApi": True},
                },
            )
            await self._notify("initialized", {})
        except BaseException:
            # ``_start`` runs under the lifecycle lock, so use the locked
            # helper instead of re-entering public ``shutdown``.
            try:
                await self._shutdown_locked()
            except BaseException:
                # The transport did not initialize and its process generation
                # could not be proven gone.  Never let ``is_alive`` make a
                # later caller reuse this uninitialized server object.
                self._shutdown_requested = True
                raise
            raise
        logger.info(
            "Codex app-server ready pid=%s home=%s startup_ms=%.1f",
            self.pid,
            self.codex_home,
            (time.perf_counter() - started) * 1000,
        )

    async def start_turn(
        self,
        *,
        prompt: str,
        cwd: str,
        model: str | None,
        effort: str | None,
        resume_session_id: str | None,
        git_env: dict[str, str] | None,
        task_id: int | None,
        mcp_specs: Sequence[McpServerSpec] = (),
        disable_project_config: bool = False,
        disable_user_mcp: bool = False,
        skill_context: str = "",
        codex_service_tier: str = CODEX_SERVICE_TIER_DEFAULT,
        sandbox_mode: str = "danger-full-access",
        task_ssh_protected_paths: Sequence[str] = (),
        task_ssh_disable_network: bool = False,
        disable_autonomous_features: bool = False,
        output_schema: dict[str, Any] | None = None,
        tools_disabled: bool = False,
        on_thread_started: (
            Callable[[str], Awaitable[None]] | None
        ) = None,
        on_turn_prepared: (
            Callable[[CodexTurnProcess, str], Awaitable[None]] | None
        ) = None,
    ) -> tuple[CodexTurnProcess, str]:
        if sandbox_mode not in {
            "danger-full-access",
            "workspace-write",
            "read-only",
        }:
            raise ValueError(f"Unsupported Codex sandbox mode: {sandbox_mode!r}")
        if tools_disabled:
            if os.name != "posix":
                raise CodexRequiredMcpPreTurnError(
                    "Codex tool-free profile currently requires POSIX "
                    "deny-all filesystem permissions"
                )
            if sandbox_mode != "read-only":
                raise CodexRequiredMcpPreTurnError(
                    "Codex tool-free profile requires read-only admission"
                )
            if mcp_specs or skill_context.strip() or git_env:
                raise CodexRequiredMcpPreTurnError(
                    "Codex tool-free profile forbids MCP, injected skill "
                    "context, and per-project environment credentials"
                )
            if not disable_autonomous_features:
                raise CodexRequiredMcpPreTurnError(
                    "Codex tool-free profile requires autonomous features "
                    "to be disabled"
                )
            if resume_session_id:
                # Dynamic tools are persisted in rollout metadata.  Codex
                # 0.144.6 restores them when a resumed thread receives an
                # empty dynamic-tools list, and thread/resume has no field
                # that can authoritatively clear them.  A review prompt is a
                # complete immutable snapshot, so start a fresh provably empty
                # thread instead of trusting an older thread's capabilities.
                logger.info(
                    "Ignoring Codex PR-review resume thread %s; tool-free "
                    "admission requires a fresh native thread",
                    resume_session_id,
                )
                resume_session_id = None
        if task_ssh_protected_paths:
            if tools_disabled:
                raise CodexRequiredMcpPreTurnError(
                    "Task SSH isolation cannot be combined with tool-free mode"
                )
            if sandbox_mode not in {"workspace-write", "read-only"}:
                raise CodexRequiredMcpPreTurnError(
                    "Task isolation requires workspace-write or read-only admission"
                )
        elif task_ssh_disable_network:
            raise CodexRequiredMcpPreTurnError(
                "Task network isolation requires protected filesystem paths"
            )
        service_tier = normalize_codex_service_tier(codex_service_tier)
        if (
            service_tier == CODEX_SERVICE_TIER_PRIORITY
            and self._require_actual_tier_proof
            and self._actual_tier_proxy_route is None
        ):
            raise CodexServiceTierUnavailableError(
                "Codex execution cannot start because the account's actual "
                "service-tier upstream route could not be proven"
            )
        rpc_service_tier = (
            CODEX_SERVICE_TIER_PRIORITY
            if service_tier == CODEX_SERVICE_TIER_PRIORITY
            else None
        )
        required_mcp = any(spec.required for spec in mcp_specs)
        required_context = bool(skill_context.strip())
        tool_free_skills_revision: int | None = None
        try:
            thread_config: dict[str, Any] = (
                render_codex_mcp_config(mcp_specs) if mcp_specs else {}
            )
        except (TypeError, ValueError) as exc:
            if required_mcp:
                raise CodexRequiredMcpError(
                    f"Invalid required Codex MCP configuration: {exc}"
                ) from exc
            raise
        if disable_user_mcp:
            # This is a whole-map thread override. Plan/other read-only
            # auxiliary turns must not inherit user or project MCP servers
            # from the shared account process.
            thread_config["mcp_servers"] = {}
        if self._actual_tier_proxy_route is not None:
            # A thread-scoped ``features`` table can outrank the app-server's
            # process-level ``--disable enable_request_compression`` override
            # (observed with Codex 0.145.0).  The proof proxy must inspect the
            # exact Responses JSON, so reinforce the setting in the same
            # config layer used by thread/start and thread/resume.
            _deep_merge_config(
                thread_config,
                {
                    "features": {
                        "enable_request_compression": False,
                    },
                },
            )
        if disable_project_config:
            _deep_merge_config(
                thread_config,
                codex_untrusted_project_config(cwd),
            )
        if (
            service_tier == CODEX_SERVICE_TIER_PRIORITY
            or disable_autonomous_features
        ):
            # Hidden model work must not escape the proof boundary. Native
            # child requests are still lineage-checked by the proxy, but Fast
            # disables autonomous fanout/memory as a second fail-closed layer.
            _deep_merge_config(
                thread_config,
                {
                    "features": {
                        # Monitor/other isolated auxiliary turns may only use
                        # their explicitly injected CCM callback MCP. Do not
                        # inherit ChatGPT Apps through the shared native home.
                        "apps": False,
                        "enable_mcp_apps": False,
                        "multi_agent": False,
                        # 5.6 model-catalog defaults can materialize v2 as a
                        # feature config object.  Override that exact shape;
                        # a legacy scalar toggle alone can be displaced when
                        # the catalog layer is resolved.
                        "multi_agent_v2": {
                            "enabled": False,
                            "max_concurrent_threads_per_session": 1,
                            "hide_spawn_agent_metadata": True,
                        },
                        "enable_fanout": False,
                        "memories": False,
                        "realtime_conversation": False,
                        # Remote compaction v2 uses the ordinary streaming
                        # /responses path and inherits this thread's tier.
                        "remote_compaction_v2": (
                            False
                            if disable_autonomous_features
                            else True
                        ),
                    },
                    "agents": {
                        "max_threads": 1,
                        "max_depth": 1,
                    },
                    "memories": {
                        "generate_memories": False,
                        "use_memories": False,
                        "dedicated_tools": False,
                    },
                },
            )
        if tools_disabled:
            # PR-review turns receive a complete backend-snapshotted prompt.
            # ``environments=[]`` below removes environment-backed tool specs.
            # The named profile denies every filesystem path and all network,
            # so even an accidentally reintroduced tool cannot read local
            # credentials before the event-level audit interrupts the turn.
            _deep_merge_config(
                thread_config,
                {
                    "web_search": "disabled",
                    "features": {
                        feature: False
                        for feature in _TOOL_FREE_DISABLED_FEATURES
                    },
                    "tools": {
                        "experimental_request_user_input": {
                            "enabled": False,
                        },
                    },
                    "orchestrator": {
                        "skills": {"enabled": False},
                        "mcp": {"enabled": False},
                    },
                    "skills": {
                        "include_instructions": False,
                        "bundled": {"enabled": False},
                        "config": [],
                    },
                    "default_permissions": _TOOL_FREE_PERMISSION_PROFILE,
                    "permissions": {
                        _TOOL_FREE_PERMISSION_PROFILE: {
                            "filesystem": {
                                "/": "deny",
                            },
                            "network": {
                                "enabled": False,
                                "allow_local_binding": False,
                            },
                        },
                    },
                    "shell_environment_policy": {
                        "inherit": "none",
                        "set": {},
                    },
                    "project_doc_max_bytes": 0,
                    "project_doc_fallback_filenames": [],
                },
            )
        elif task_ssh_protected_paths:
            _deep_merge_config(
                thread_config,
                {
                    "default_permissions": _TASK_SSH_PERMISSION_PROFILE,
                    "permissions": {
                        _TASK_SSH_PERMISSION_PROFILE: (
                            _task_ssh_permission_config(
                                cwd=cwd,
                                protected_paths=task_ssh_protected_paths,
                                disable_network=task_ssh_disable_network,
                                sandbox_mode=sandbox_mode,
                            )
                        ),
                    },
                },
            )
        try:
            await self.ensure_started()
        except CodexAppServerBusyError:
            # Preserve maintenance/draining semantics.  InstanceManager already
            # treats this as unsafe to replay through exec.
            raise
        except Exception as exc:
            if required_mcp or required_context:
                error_type = (
                    CodexRequiredMcpError
                    if self.shutdown_requested
                    else CodexRequiredMcpPreTurnError
                )
                raise error_type(
                    (
                        "Codex app-server could not start required MCP "
                        "transport: "
                        if required_mcp
                        else "Codex app-server could not start required task "
                        "context: "
                    )
                    + str(exc)
                ) from exc
            raise
        if tools_disabled:
            try:
                effective = await self._request(
                    "config/read",
                    {
                        "cwd": os.path.abspath(cwd),
                        "includeLayers": False,
                    },
                )
                effective_config = (
                    effective.get("config")
                    if isinstance(effective, dict)
                    else None
                )
                if not isinstance(effective_config, dict):
                    raise ValueError("effective Codex configuration is malformed")
                for instruction_key in (
                    "developer_instructions",
                    "instructions",
                    "model_instructions_file",
                ):
                    if effective_config.get(instruction_key) not in {
                        None,
                        "",
                    }:
                        raise ValueError(
                            "ambient Codex instructions are configured"
                        )
                inherited_mcp = effective_config.get("mcp_servers", {})
                if not isinstance(inherited_mcp, dict) or any(
                    not isinstance(name, str) or not name
                    for name in inherited_mcp
                ):
                    raise ValueError(
                        "effective MCP server configuration is malformed"
                    )
                # An empty higher-level table does not erase lower config
                # layers in Codex. Disable every effective inherited server
                # explicitly; this was verified against CLI 0.144.6.
                thread_config["mcp_servers"] = {
                    name: {"enabled": False}
                    for name in inherited_mcp
                }
                skills_inventory = await self._request(
                    "skills/list",
                    {
                        "cwds": [os.path.abspath(cwd)],
                        "forceReload": True,
                    },
                )
                thread_config["skills"]["config"] = (
                    _tool_free_disabled_skill_config(
                        skills_inventory,
                        cwd=cwd,
                    )
                )
                tool_free_skills_revision = self._skills_revision
            except Exception as exc:
                raise CodexRequiredMcpPreTurnError(
                    "Codex tool-free profile could not audit ambient "
                    "instructions and inherited MCP servers"
                ) from exc
        actual_tier_proxy = self._actual_tier_proxy
        if (
            service_tier == CODEX_SERVICE_TIER_PRIORITY
            and self._require_actual_tier_proof
            and (
                actual_tier_proxy is None
                or not actual_tier_proxy.is_alive
            )
        ):
            raise CodexServiceTierUnavailableError(
                "Codex actual service-tier proxy is unavailable before "
                "thread admission"
            )
        if service_tier == CODEX_SERVICE_TIER_PRIORITY:
            await self._require_service_tier_support(
                model=model,
                service_tier=service_tier,
            )
        launch_started = time.perf_counter()
        if git_env or task_ssh_protected_paths:
            # Per-project git credentials must remain thread-scoped.  A global
            # app-server environment would leak one project's identity into
            # every other concurrently running task.
            shell_environment = dict(git_env or {})
            if task_ssh_protected_paths:
                shell_environment.update({
                    "SSH_AUTH_SOCK": "",
                    "SSH_AGENT_PID": "",
                    "SSH_ASKPASS": "",
                })
                if task_ssh_disable_network:
                    shell_environment["CCM_TASK_SSH_GUARD"] = "1"
            thread_config["shell_environment_policy"] = {
                "inherit": "all",
                "set": shell_environment,
            }

        common: dict[str, Any] = {
            "cwd": os.path.abspath(cwd),
            "approvalPolicy": "never",
            # Never allow a model-catalog/profile default to route approvals
            # through the guardian subagent, which would create hidden model
            # work outside the root turn.
            "approvalsReviewer": "user",
            # This field is intentionally present even for Standard.  JSON null
            # clears a sticky service tier inherited from a resumed thread.
            "serviceTier": rpc_service_tier,
        }
        if tools_disabled:
            # Clear config-level developer instructions. Project/user
            # instruction files are independently proven absent through
            # ``instructionSources`` in the thread response. Do not also set
            # the type-safe ``permissions`` selector here: Codex 0.144.6
            # resolves it before the request-local profile table. The config
            # layer above defines and selects the profile atomically.
            common["baseInstructions"] = ""
            common["developerInstructions"] = ""
        elif not task_ssh_protected_paths:
            # Task SSH selects its request-local named permission profile in
            # ``config.default_permissions`` below.  Sending the legacy
            # top-level sandbox selector at the same time wins precedence and
            # silently replaces that profile with ``:workspace``.
            common["sandbox"] = sandbox_mode
        if model and model != "default":
            common["model"] = model
        if thread_config:
            common["config"] = thread_config

        thread_method = "thread/resume" if resume_session_id else "thread/start"
        thread_params = (
            {"threadId": resume_session_id, **common}
            if resume_session_id
            else common
        )
        if tools_disabled and not resume_session_id:
            # These fields are gated by the experimentalApi capability that
            # CCM enables during initialization. Explicit empty values are
            # materially different from omission: omission selects the local
            # default environment and re-adds exec/apply_patch/view_image.
            thread_params.update({
                "environments": [],
                "runtimeWorkspaceRoots": [],
                "selectedCapabilityRoots": [],
                "dynamicTools": [],
            })
        try:
            response = await self._request(thread_method, thread_params)
        except Exception as exc:
            if required_mcp or required_context:
                raise CodexRequiredMcpPreTurnError(
                    (
                        f"{thread_method} could not initialize required MCP "
                        "configuration: "
                        if required_mcp
                        else f"{thread_method} could not initialize required "
                        "task context: "
                    )
                    + str(exc)
                ) from exc
            raise

        thread = response.get("thread") if isinstance(response, dict) else None
        thread_id = thread.get("id") if isinstance(thread, dict) else None
        if not thread_id:
            message = "thread start/resume returned no thread id"
            if required_mcp or required_context:
                raise CodexRequiredMcpPreTurnError(
                    (
                        "Required MCP configuration was not admitted: "
                        if required_mcp
                        else "Required task context was not admitted: "
                    )
                    + message
                )
            raise CodexAppServerError(message)
        thread_id = str(thread_id)
        if (
            resume_session_id
            and thread_id != str(resume_session_id)
        ):
            raise CodexThreadIdentityMismatchError(
                str(resume_session_id),
                thread_id,
                operation=thread_method,
            )
        terminal_recovery_attempted = False
        status = thread.get("status") if isinstance(thread, dict) else None
        status_type = self._thread_status_type(status)
        if resume_session_id and status_type == "systemError":
            # systemError is an authoritative no-running-turn state, but Codex
            # will not admit a follow-up until the loaded runtime is refreshed.
            # Recycle this exact thread once; never replay through exec and never
            # weaken the explicit-idle requirement on the second resume.
            terminal_recovery_attempted = True
            logger.warning(
                "Recycling terminal Codex thread before resume thread=%s state=%s",
                thread_id,
                status_type,
            )
            try:
                await self.recycle_thread_runtime(thread_id)
                response = await self._request(thread_method, thread_params)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise CodexThreadTerminalStateError(
                    thread_id,
                    status_type,
                    operation=f"{thread_method} turn admission",
                    recovery_attempted=True,
                    detail=f"runtime recycle failed: {exc}",
                ) from exc
            thread = response.get("thread") if isinstance(response, dict) else None
            recovered_thread_id = (
                thread.get("id") if isinstance(thread, dict) else None
            )
            if str(recovered_thread_id or "") != thread_id:
                raise CodexThreadTerminalStateError(
                    thread_id,
                    status_type,
                    operation=f"{thread_method} turn admission",
                    recovery_attempted=True,
                    detail="recovery resumed a different or missing thread",
                )
        if tools_disabled:
            try:
                _audit_tool_free_thread_response(response)
                # Drain notifications already queued behind thread/start. A
                # changed inventory invalidates the exact path deny-list and
                # must fail before turn/start sends model input.
                await asyncio.sleep(0)
                if (
                    tool_free_skills_revision is None
                    or self._skills_revision != tool_free_skills_revision
                ):
                    raise ValueError(
                        "skills inventory changed during admission"
                    )
            except (TypeError, ValueError) as exc:
                raise CodexRequiredMcpPreTurnError(
                    "Codex tool-free profile was not proven by the "
                    f"{thread_method} response"
                ) from exc
        elif task_ssh_protected_paths:
            try:
                _audit_task_ssh_thread_response(
                    response,
                    disable_network=task_ssh_disable_network,
                    sandbox_mode=sandbox_mode,
                )
            except (TypeError, ValueError) as exc:
                raise CodexRequiredMcpPreTurnError(
                    "Codex Task isolation profile was not proven by the "
                    f"{thread_method} response"
                ) from exc
        self._known_threads.add(thread_id)
        if on_thread_started is not None:
            # A caller that owns durable lifecycle state can bind the exact
            # native identity before any model turn is admitted. In
            # particular, Monitor uses this hook to survive a process crash
            # between thread/start and turn/start without guessing a rollout.
            await on_thread_started(thread_id)
        # A native Goal can continue after an older CCM version detached its
        # process adapter. Standard chat may recover that exact Goal by
        # preparing a new CCM owner and steering the pending user input into
        # its authoritative active turn. Fast, isolated/tool-free, and
        # non-Goal active work remain fail-closed.
        thread_status_type = self._thread_status_type(thread.get("status"))
        adopt_active_goal = False
        if thread_status_type != "idle":
            if (
                resume_session_id
                and thread_status_type == "active"
                and service_tier == CODEX_SERVICE_TIER_DEFAULT
                and not disable_autonomous_features
                and not tools_disabled
            ):
                try:
                    active_goal = await self._read_thread_goal(str(thread_id))
                except CodexAppServerError as exc:
                    raise CodexThreadNotIdleError(
                        str(thread_id),
                        "active-goal-unverified",
                        operation=f"{thread_method} turn admission",
                    ) from exc
                adopt_active_goal = bool(
                    isinstance(active_goal, dict)
                    and active_goal.get("status") == "active"
                )
            if not adopt_active_goal:
                try:
                    _require_idle_thread_status(
                        thread,
                        thread_id=str(thread_id),
                        operation=f"{thread_method} turn admission",
                    )
                except CodexThreadTerminalStateError as exc:
                    if (
                        terminal_recovery_attempted
                        and not exc.recovery_attempted
                    ):
                        raise CodexThreadTerminalStateError(
                            exc.thread_id,
                            exc.state,
                            operation=exc.operation,
                            recovery_attempted=True,
                        ) from exc
                    raise
        if (
            resume_session_id
            and service_tier == CODEX_SERVICE_TIER_PRIORITY
        ):
            # ``thread.status == idle`` only proves that no turn is executing
            # at this instant.  A native Goal can be active between autonomous
            # turns and start its next (old-tier) turn before our turn/start.
            # Fast therefore requires the stronger proof that no resumable
            # Goal exists before changing sticky thread settings.
            await self._require_no_resumable_thread_goal(
                str(thread_id),
                operation=f"{thread_method} Fast turn admission",
            )
        effective_service_tier = _canonical_app_server_service_tier(
            response.get("serviceTier")
            if isinstance(response, dict)
            else None
        )
        if effective_service_tier != rpc_service_tier:
            if resume_session_id:
                effective_service_tier = (
                    await self._update_loaded_thread_service_tier(
                        str(thread_id),
                        rpc_service_tier,
                    )
                )
            if effective_service_tier != rpc_service_tier:
                raise CodexServiceTierUnavailableError(
                    "Codex did not admit the requested service tier "
                    f"{service_tier!r} for model {model or 'default'!r}; "
                    f"effective tier was {effective_service_tier!r}"
                )
        if actual_tier_proxy is not None:
            # Keep this mapping for the lifetime of the native thread, not
            # merely the CCM adapter turn. Native Goals and hidden follow-up
            # requests can outlive ``turn/completed`` and must remain fenced.
            try:
                actual_tier_proxy.set_thread_tier(
                    str(thread_id),
                    service_tier,
                )
            except CodexTierProofError as exc:
                raise CodexServiceTierUnavailableError(
                    "Codex service tier cannot change while an older request "
                    f"in this native thread lineage is active: {exc}"
                ) from exc
        existing = self._contexts_by_thread.get(thread_id)
        if existing and existing.process.returncode is None:
            raise CodexAppServerBusyError(
                f"thread {thread_id} already has an active turn"
            )

        context: _TurnContext | None = None

        async def _interrupt() -> None:
            if context is not None:
                await self._interrupt_turn_context(context)

        turn_process = CodexTurnProcess(
            self.pid,
            _interrupt,
            thread_id=thread_id,
        )
        client_user_message_id = uuid.uuid4().hex
        context = _TurnContext(
            thread_id=thread_id,
            process=turn_process,
            launch_started=launch_started,
            task_id=task_id,
            client_user_message_id=client_user_message_id,
            tools_disabled=tools_disabled,
            descendant_state_changed=asyncio.Event(),
            descendant_interrupt_lock=asyncio.Lock(),
            admission_observed_future=(
                asyncio.get_running_loop().create_future()
                if service_tier == CODEX_SERVICE_TIER_PRIORITY
                else None
            ),
            pending_admission_notifications=(
                []
                if (
                    service_tier == CODEX_SERVICE_TIER_PRIORITY
                    or actual_tier_proxy is not None
                )
                else None
            ),
        )
        self._contexts_by_thread[thread_id] = context
        if adopt_active_goal:
            # The app-server keeps lineage/runtime observations even when an
            # older CCM adapter detached. Re-adopt every already-known child
            # before steering new input so root completion cannot release the
            # Task while one of those exact descendants is still active.
            for child_id in self._children_by_thread.get(thread_id, set()):
                self._attach_descendant(context, child_id, active=None)
        # Persist the native thread id through the same event path as exec.
        turn_process.feed({"type": "thread.started", "thread_id": thread_id})
        if on_turn_prepared is not None:
            try:
                # Publish the adapter before turn/start goes on the wire. This
                # closes the last shutdown/maintenance window in which model
                # work could exist without an exact in-memory owner.
                await on_turn_prepared(turn_process, thread_id)
            except BaseException:
                self._detach_turn_context(context)
                turn_process.finish(
                    1,
                    "Codex turn ownership preparation failed",
                )
                raise

        if (
            tools_disabled
            and (
                tool_free_skills_revision is None
                or self._skills_revision != tool_free_skills_revision
            )
        ):
            # The ownership hook above may await durable state. Recheck the
            # inventory generation at the final boundary before model input
            # goes on the wire.
            reason = (
                "Codex tool-free skills inventory changed before turn/start"
            )
            self._detach_turn_context(context)
            turn_process.finish(1, reason)
            raise CodexRequiredMcpPreTurnError(reason)

        model_prompt = prompt
        if required_context:
            from backend.services.skill_context import wrap_skill_context

            # Codex 0.144.6 silently drops unknown TurnStartParams fields.
            # Keep the canonical task context in the schema-backed text input
            # so the primary app-server route and exec fallback expose the
            # same bounded, model-visible prompt.
            model_prompt = wrap_skill_context(prompt, skill_context)

        turn_params: dict[str, Any] = {
            "threadId": thread_id,
            "input": [{"type": "text", "text": model_prompt}],
            "clientUserMessageId": client_user_message_id,
            "cwd": os.path.abspath(cwd),
            "approvalPolicy": "never",
            "approvalsReviewer": "user",
            "model": model if model and model != "default" else None,
            "effort": effort,
            # turn/start persists overrides for subsequent turns.  Repeat the
            # exact value so another caller cannot reintroduce a sticky tier
            # between thread admission and this turn.
            "serviceTier": rpc_service_tier,
        }
        if output_schema is not None:
            turn_params["outputSchema"] = output_schema
        if tools_disabled:
            # A turn-level cwd without an explicit empty environment causes
            # Codex to silently restore its default local environment. Repeat
            # both empty selections. Do not repeat the named permission
            # selector: Codex 0.144.6 rebuilds turn overrides without the
            # thread's request-local profile table and rejects that selector.
            # The audited thread profile remains active for this first turn.
            turn_params["environments"] = []
            turn_params["runtimeWorkspaceRoots"] = []
        elif sandbox_mode == "read-only":
            # Repeat the policy at turn/start.  This is a schema-backed field
            # and prevents a resumed thread's sticky/default settings from
            # widening a Monitor turn after thread admission.
            turn_params["sandboxPolicy"] = {
                "type": "readOnly",
                "networkAccess": False,
            }
        elif sandbox_mode == "workspace-write" and not task_ssh_protected_paths:
            turn_params["sandboxPolicy"] = {
                "type": "workspaceWrite",
                "writableRoots": [os.path.abspath(cwd)],
                "networkAccess": False,
                "excludeTmpdirEnvVar": False,
                "excludeSlashTmp": False,
            }
        if adopt_active_goal:
            self._mark_following_native_goal(context)
            adoption = asyncio.create_task(
                self._steer_detached_native_goal(
                    context,
                    list(turn_params["input"]),
                ),
            )
            adoption_cancelled = False
            while not adoption.done():
                try:
                    await asyncio.shield(adoption)
                except asyncio.CancelledError:
                    # User input may already have reached the older native
                    # turn. Resolve its exact identity, then pause/interrupt
                    # before propagating cancellation.
                    adoption_cancelled = True
                except BaseException:
                    break
            try:
                adopted_turn_id = adoption.result()
            except BaseException:
                self._detach_turn_context(context)
                turn_process.finish(
                    1,
                    "Codex active native Goal adoption failed",
                )
                raise

            context.admitted_turn_id = None
            if actual_tier_proxy is not None:
                context.admission_confirmed = True
                self._replay_pending_admission_notifications(context)
            turn_process.feed({
                "type": "turn.started",
                "turn_id": adopted_turn_id,
                "native_goal": True,
                "adopted": True,
            })
            if adoption_cancelled:
                cleanup = asyncio.create_task(self.abandon_turn(
                    turn_process,
                    "Codex native Goal adoption was cancelled",
                ))
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        continue
                if not cleanup.result():
                    raise _UnconfirmedTurnCancellation(
                        turn_process,
                        "Codex native Goal adoption was cancelled and its "
                        "interrupt could not be confirmed",
                    )
                raise asyncio.CancelledError
            logger.info(
                "Codex latency task=%s thread=%s stage=goal_adopted "
                "elapsed_ms=%.1f turn=%s",
                task_id,
                thread_id,
                (time.perf_counter() - launch_started) * 1000,
                adopted_turn_id,
            )
            return turn_process, thread_id

        turn_request = asyncio.create_task(self._request("turn/start", turn_params))
        turn_cancelled = False
        while not turn_request.done():
            try:
                await asyncio.shield(turn_request)
            except asyncio.CancelledError:
                # Once turn/start is on the wire, abandoning the response can
                # leave real model work with no process adapter/consumer. Wait
                # for the bounded RPC to resolve, then interrupt it explicitly.
                turn_cancelled = True
            except BaseException:
                # Retrieve and classify the completed request below. In
                # particular, a timeout is an indeterminate server-side turn,
                # not an ordinary rejected admission.
                break
        try:
            turn_response = turn_request.result()
        except asyncio.TimeoutError as exc:
            # The RPC was written but no response arrived. It is impossible to
            # distinguish a rejected request from real model work that started
            # just before the timeout, and no turn id exists for an interrupt.
            # Preserve the adapter/context so the registry can fail closed by
            # stopping this account transport.
            reason = "Codex app-server turn/start timed out with unknown server state"
            if turn_cancelled:
                raise _UnconfirmedTurnCancellation(turn_process, reason) from exc
            raise _UnconfirmedTurnStartFailure(turn_process, reason) from exc
        except BaseException as exc:
            self._detach_turn_context(context)
            turn_process.finish(1, "Codex app-server rejected turn/start")
            if (
                (required_mcp or required_context)
                and isinstance(exc, CodexAppServerRequestError)
            ):
                # An explicit JSON-RPC rejection proves that no turn was
                # admitted.  InstanceManager may therefore replay once
                # through exec with the same MCP config and canonical context.
                raise CodexRequiredMcpPreTurnError(
                    "turn/start rejected required CCM task context before "
                    f"admission: {exc}"
                ) from exc
            if (
                (required_mcp or required_context)
                and isinstance(exc, Exception)
            ):
                raise CodexRequiredMcpError(
                    "turn/start failed with unknown admission state while "
                    f"preserving required CCM task context: {exc}"
                ) from exc
            raise

        turn = turn_response.get("turn") if isinstance(turn_response, dict) else None
        turn_id = turn.get("id") if isinstance(turn, dict) else None
        if not turn_id:
            self._detach_turn_context(context)
            turn_process.finish(1, "Codex app-server turn/start returned no turn id")
            message = "turn/start returned no turn id"
            if required_mcp or required_context:
                raise CodexRequiredMcpError(
                    (
                        "Required MCP turn was not admitted: "
                        if required_mcp
                        else "Required task context turn was not admitted: "
                    )
                    + message
                )
            raise CodexAppServerError(message)
        context.admitted_turn_id = str(turn_id)
        # An already-running steerable turn can emit notifications before this
        # response arrives. In that case its notification turn id is the real
        # active generation and must not be overwritten by this submission id.
        # The submission id remains a valid notification alias, however.
        if context.observed_turn_id is None:
            self._bind_turn_context(
                context,
                str(turn_id),
                observed=False,
            )
        elif context.process.returncode is None:
            self._alias_turn_context(context, str(turn_id))
        if turn_cancelled:
            cleanup = asyncio.create_task(self.abandon_turn(
                turn_process,
                "Codex turn admission was cancelled",
            ))
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
            interrupt_confirmed = cleanup.result()
            if not interrupt_confirmed:
                raise _UnconfirmedTurnCancellation(
                    turn_process,
                    "Codex turn admission was cancelled and its interrupt "
                    "could not be confirmed",
                )
            raise asyncio.CancelledError
        actual_tier_proof = None
        if (
            actual_tier_proxy is not None
            and service_tier == CODEX_SERVICE_TIER_PRIORITY
        ):
            proof_wait = asyncio.create_task(
                actual_tier_proxy.wait_for_actual_tier(
                    str(thread_id),
                    str(turn_id),
                    service_tier,
                    timeout=max(self.request_timeout, 60.0),
                )
            )
            while not proof_wait.done():
                try:
                    await asyncio.shield(proof_wait)
                except asyncio.CancelledError:
                    # The native turn exists and its upstream request may
                    # already be in flight. Obtain or reject the exact proof,
                    # then interrupt before propagating cancellation.
                    turn_cancelled = True
                except BaseException:
                    break
            try:
                actual_tier_proof = proof_wait.result()
            except CodexTierProofError as exc:
                reason = (
                    "Codex upstream did not prove the requested actual "
                    f"service tier {service_tier!r}: {exc}"
                )
                interrupt_confirmed = await self.abandon_turn(
                    turn_process,
                    reason,
                )
                if not interrupt_confirmed:
                    raise _UnconfirmedTurnStartFailure(
                        turn_process,
                        f"{reason}, and its interrupt could not be confirmed",
                    ) from exc
                raise CodexServiceTierUnavailableError(reason) from exc
            if turn_cancelled:
                cleanup = asyncio.create_task(self.abandon_turn(
                    turn_process,
                    "Codex actual service-tier proof wait was cancelled",
                ))
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        continue
                interrupt_confirmed = cleanup.result()
                if not interrupt_confirmed:
                    raise _UnconfirmedTurnCancellation(
                        turn_process,
                        "Codex actual service-tier proof wait was cancelled "
                        "and its interrupt could not be confirmed",
                    )
                raise asyncio.CancelledError
        if service_tier == CODEX_SERVICE_TIER_PRIORITY:
            observed_future = context.admission_observed_future
            assert observed_future is not None
            observation_wait = asyncio.create_task(asyncio.wait_for(
                asyncio.shield(observed_future),
                timeout=self.request_timeout,
            ))
            while not observation_wait.done():
                try:
                    await asyncio.shield(observation_wait)
                except asyncio.CancelledError:
                    # The turn is already live.  Finish identity
                    # reconciliation, then interrupt the exact native
                    # generation before propagating cancellation.
                    turn_cancelled = True
                except BaseException:
                    break
            try:
                observation_wait.result()
            except asyncio.TimeoutError as exc:
                reason = (
                    "Codex Fast turn did not emit an authoritative native "
                    "turn identity before the admission deadline"
                )
                interrupt_confirmed = await self.abandon_turn(
                    turn_process,
                    reason,
                )
                if not interrupt_confirmed:
                    raise _UnconfirmedTurnStartFailure(
                        turn_process,
                        f"{reason}, and its interrupt could not be confirmed",
                    ) from exc
                raise CodexServiceTierUnavailableError(reason) from exc

            if turn_cancelled:
                cleanup = asyncio.create_task(self.abandon_turn(
                    turn_process,
                    "Codex Fast turn admission was cancelled",
                ))
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        continue
                interrupt_confirmed = cleanup.result()
                if not interrupt_confirmed:
                    raise _UnconfirmedTurnCancellation(
                        turn_process,
                        "Codex Fast turn admission was cancelled and its "
                        "interrupt could not be confirmed",
                    )
                raise asyncio.CancelledError
        if (
            service_tier == CODEX_SERVICE_TIER_PRIORITY
            and context.observed_turn_id is not None
            and context.observed_turn_id != context.admitted_turn_id
        ):
            # Codex can adopt a turn/start submission into an older native
            # Goal turn.  That older generation began before this request's
            # priority admission, so its actual tier is unknowable here.  Stop
            # it and fail instead of attaching a Fast proof to old work.
            reason = (
                "Codex Fast turn was adopted by an older active native turn; "
                "priority execution cannot be proven"
            )
            interrupt_confirmed = await self.abandon_turn(
                turn_process,
                reason,
            )
            if not interrupt_confirmed:
                raise _UnconfirmedTurnStartFailure(
                    turn_process,
                    f"{reason}, and its interrupt could not be confirmed",
                )
            raise CodexThreadNotIdleError(
                str(thread_id),
                (
                    "adopted-active-turn:"
                    f"{context.observed_turn_id}"
                ),
                operation="Fast turn admission",
            )
        if service_tier == CODEX_SERVICE_TIER_PRIORITY:
            # Only publish the proof after turn/start itself returned a real
            # turn id.  A thread-level confirmation followed by a rejected or
            # indeterminate turn must never appear as a successful Fast turn.
            admission_event = {
                "type": "system_event",
                "content": (
                    (
                        "Codex Fast 实际 priority 已由上游确认"
                        if actual_tier_proof is not None
                        else "Codex Fast priority 请求准入已确认"
                    )
                    + f" · 模型 {model or 'default'}"
                ),
                "requested_service_tier": CODEX_SERVICE_TIER_PRIORITY,
                "admitted_service_tier": (
                    actual_tier_proof.actual_tier
                    if actual_tier_proof is not None
                    else effective_service_tier
                ),
                "model": model or "default",
                "thread_id": thread_id,
                "turn_id": str(turn_id),
            }
            if actual_tier_proof is not None:
                admission_event.update({
                    "actual_service_tier_verified": True,
                    "upstream_response_id": actual_tier_proof.response_id,
                })
            turn_process.feed(admission_event)
            logger.info(
                "Codex service-tier request admitted task=%s thread=%s turn=%s "
                "requested=priority admitted=%s actual_verified=%s "
                "response=%s model=%s",
                task_id,
                thread_id,
                turn_id,
                (
                    actual_tier_proof.actual_tier
                    if actual_tier_proof is not None
                    else effective_service_tier
                ),
                actual_tier_proof is not None,
                (
                    actual_tier_proof.response_id
                    if actual_tier_proof is not None
                    else "-"
                ),
                model or "default",
            )
            context.admission_confirmed = True
            self._replay_pending_admission_notifications(context)
        else:
            logger.info(
                "Codex service-tier request admitted task=%s thread=%s turn=%s "
                "requested=default admitted=%s actual_verified=%s "
                "response=%s model=%s",
                task_id,
                thread_id,
                turn_id,
                effective_service_tier,
                actual_tier_proof is not None,
                (
                    actual_tier_proof.response_id
                    if actual_tier_proof is not None
                    else "-"
                ),
                model or "default",
            )
            if actual_tier_proxy is not None:
                context.admission_confirmed = True
                self._replay_pending_admission_notifications(context)
        logger.info(
            "Codex latency task=%s thread=%s stage=turn_started elapsed_ms=%.1f",
            task_id,
            thread_id,
            (time.perf_counter() - launch_started) * 1000,
        )
        self._replay_pending_terminal_notification(context)
        return turn_process, thread_id

    def _replay_pending_admission_notifications(
        self,
        context: _TurnContext,
    ) -> None:
        """Replay Fast events only after exact new-turn identity is proven."""

        pending = context.pending_admission_notifications
        context.pending_admission_notifications = None
        for method, params in pending or ():
            self._handle_notification(method, params)

    def _replay_pending_terminal_notification(
        self,
        context: _TurnContext,
    ) -> None:
        """Finish a turn only after its admission response has been handled."""

        pending = context.pending_terminal_notification
        if pending is None:
            return
        context.pending_terminal_notification = None
        method, params = pending
        self._handle_notification(method, params)

    async def _require_service_tier_support(
        self,
        *,
        model: str | None,
        service_tier: str,
    ) -> None:
        """Confirm the selected model advertises a tier before creating a thread."""

        requested_model = (
            str(model).strip()
            if model and str(model).strip().lower() != "default"
            else None
        )
        cursor: str | None = None
        seen_cursors: set[str] = set()
        for _page in range(_MODEL_LIST_MAX_PAGES):
            params: dict[str, Any] = {
                "includeHidden": True,
                "limit": _MODEL_LIST_PAGE_LIMIT,
            }
            if cursor is not None:
                params["cursor"] = cursor
            try:
                response = await self._request("model/list", params)
            except Exception as exc:
                raise CodexServiceTierUnavailableError(
                    "Codex model capabilities could not be verified before "
                    f"requesting service tier {service_tier!r}"
                ) from exc
            data = response.get("data") if isinstance(response, dict) else None
            if not isinstance(data, list):
                raise CodexServiceTierUnavailableError(
                    "Codex model/list returned an invalid capability catalog"
                )
            for item in data:
                if not isinstance(item, dict):
                    continue
                item_id = item.get("id")
                item_model = item.get("model")
                is_selected = (
                    (
                        requested_model is not None
                        and requested_model in {item_id, item_model}
                    )
                    or (
                        requested_model is None
                        and item.get("isDefault") is True
                    )
                )
                if not is_selected:
                    continue
                tiers = item.get("serviceTiers")
                supported = {
                    tier.get("id")
                    for tier in tiers
                    if isinstance(tier, dict) and isinstance(tier.get("id"), str)
                } if isinstance(tiers, list) else set()
                if service_tier in supported:
                    return
                raise CodexServiceTierUnavailableError(
                    f"Codex model {requested_model or item_id or item_model or 'default'!r} "
                    f"does not advertise service tier {service_tier!r}"
                )

            next_cursor = (
                response.get("nextCursor")
                if isinstance(response, dict)
                else None
            )
            if not isinstance(next_cursor, str) or not next_cursor:
                break
            if next_cursor in seen_cursors:
                raise CodexServiceTierUnavailableError(
                    "Codex model/list returned a repeated pagination cursor"
                )
            seen_cursors.add(next_cursor)
            cursor = next_cursor

        raise CodexServiceTierUnavailableError(
            f"Codex model {requested_model or 'default'!r} was not found in "
            "the authenticated account's capability catalog"
        )

    async def _update_loaded_thread_service_tier(
        self,
        thread_id: str,
        service_tier: str | None,
    ) -> str | None:
        """Update a hot thread and wait for its authoritative settings event."""

        if thread_id in self._thread_settings_waiters:
            raise CodexAppServerBusyError(
                f"thread {thread_id} already has a settings update in flight"
            )
        future = asyncio.get_running_loop().create_future()
        self._thread_settings_waiters[thread_id] = future
        try:
            await self._request(
                "thread/settings/update",
                {
                    "threadId": thread_id,
                    # null explicitly clears a prior Fast selection.
                    "serviceTier": service_tier,
                },
            )
            try:
                notification = await asyncio.wait_for(
                    asyncio.shield(future),
                    timeout=self.request_timeout,
                )
            except asyncio.TimeoutError as exc:
                raise CodexServiceTierUnavailableError(
                    "Codex accepted a thread service-tier update but did not "
                    "confirm its effective settings before turn/start"
                ) from exc
        except CodexServiceTierUnavailableError:
            raise
        except Exception as exc:
            raise CodexServiceTierUnavailableError(
                "Codex could not update the loaded thread's service tier "
                "before turn/start"
            ) from exc
        finally:
            if self._thread_settings_waiters.get(thread_id) is future:
                self._thread_settings_waiters.pop(thread_id, None)
            if not future.done():
                future.cancel()

        settings = (
            notification.get("threadSettings")
            if isinstance(notification, dict)
            else None
        )
        if not isinstance(settings, dict) or "serviceTier" not in settings:
            return "__invalid__"
        return _canonical_app_server_service_tier(
            settings.get("serviceTier"),
        )

    async def require_thread_routing_quiescence(
        self,
        thread_id: str,
    ) -> dict[str, Any]:
        """Prove a persisted thread cannot autonomously use its old route.

        Native Goals outlive CCM's per-turn adapter.  A completed Task and an
        empty ``_contexts_by_thread`` map are therefore insufficient evidence:
        app-server can continue an active/paused/blocked Goal later without a
        new CCM launch.  Routing changes are admitted only when there is no
        Goal that could be resumed and ``thread/read`` explicitly reports the
        loaded thread as idle.
        """

        if not thread_id:
            raise ValueError("thread_id is required")
        await self.ensure_started()

        goal = await self._require_no_resumable_thread_goal(
            thread_id,
            operation="routing configuration change",
        )

        response = await self._request(
            "thread/read",
            {
                "threadId": thread_id,
                "includeTurns": False,
            },
        )
        thread = response.get("thread") if isinstance(response, dict) else None
        if not isinstance(thread, dict) or thread.get("id") != thread_id:
            raise CodexAppServerError(
                f"thread/read returned an invalid thread for {thread_id}"
            )
        _require_idle_thread_status(
            thread,
            thread_id=thread_id,
            operation="routing configuration change",
        )
        return {
            "thread": thread,
            "goal": goal,
        }

    async def _read_thread_goal(
        self,
        thread_id: str,
    ) -> dict[str, Any] | None:
        """Return the native Goal exactly as reported by app-server."""

        try:
            goal_response = await self._request(
                "thread/goal/get",
                {"threadId": thread_id},
            )
        except CodexAppServerError as exc:
            if _GOALS_FEATURE_DISABLED_RE.search(str(exc)):
                return None
            raise
        if not isinstance(goal_response, dict) or "goal" not in goal_response:
            raise CodexAppServerError(
                f"thread/goal/get returned invalid data for {thread_id}"
            )
        goal = goal_response.get("goal")
        if goal is not None and not isinstance(goal, dict):
            raise CodexAppServerError(
                f"thread/goal/get returned an invalid goal for {thread_id}"
            )
        runtime = self._runtime_state_for(thread_id)
        runtime.goal_status = (
            str(goal.get("status"))
            if isinstance(goal, dict) and goal.get("status")
            else None
        )
        return goal

    async def _require_no_resumable_thread_goal(
        self,
        thread_id: str,
        *,
        operation: str,
    ) -> dict[str, Any] | None:
        """Return a terminal Goal, or reject any Goal that can run again."""

        goal = await self._read_thread_goal(thread_id)

        if goal is not None:
            goal_status = goal.get("status")
            # Only a completed Goal is immutable enough for a routing change.
            # Paused/blocked/limited Goals can be resumed by another surface,
            # so accepting them would reopen an old-tier autonomous turn after
            # the Task badge has changed.
            if goal_status != "complete":
                raise CodexThreadNotIdleError(
                    thread_id,
                    f"goal:{goal_status or 'unknown'}",
                    operation=operation,
                )
        return goal

    async def read_thread(self, thread_id: str) -> dict[str, Any]:
        """Load one thread and its persisted turns from this account home."""

        if not thread_id:
            raise ValueError("thread_id is required")
        await self.ensure_started()
        response = await self._request(
            "thread/read",
            {"threadId": thread_id, "includeTurns": True},
        )
        thread = response.get("thread")
        if not isinstance(thread, dict) or thread.get("id") != thread_id:
            raise CodexAppServerError(
                f"thread/read returned an invalid thread for {thread_id}"
            )
        return thread

    async def create_thread(
        self,
        *,
        cwd: str,
        model: str | None = None,
        disable_project_config: bool = False,
    ) -> dict[str, Any]:
        """Create an empty persisted thread without starting a turn."""

        if not cwd:
            raise ValueError("cwd is required")
        await self.ensure_started()
        params: dict[str, Any] = {
            "cwd": os.path.abspath(cwd),
            "approvalPolicy": "never",
            "sandbox": "danger-full-access",
        }
        if model and model != "default":
            params["model"] = model
        if disable_project_config:
            params["config"] = codex_untrusted_project_config(cwd)
        request = asyncio.create_task(self._request("thread/start", params))
        while not request.done():
            try:
                await asyncio.shield(request)
            except asyncio.CancelledError:
                continue
        response = request.result()
        thread = response.get("thread") if isinstance(response, dict) else None
        thread_id = thread.get("id") if isinstance(thread, dict) else None
        if not thread_id:
            raise CodexAppServerError("thread/start returned no thread id")
        self._known_threads.add(str(thread_id))
        return thread

    async def fork_thread(
        self,
        thread_id: str,
        *,
        last_turn_id: str,
    ) -> dict[str, Any]:
        """Fork a persisted thread through one completed turn, inclusive."""

        if not thread_id:
            raise ValueError("thread_id is required")
        if not last_turn_id:
            raise ValueError("last_turn_id is required")
        await self.ensure_started()
        # Once the mutating RPC is on the wire, cancellation cannot tell us
        # whether Codex created the fork. Settle the request so the caller
        # always receives the new id and can either commit or compensate it.
        request = asyncio.create_task(self._request(
            "thread/fork",
            {
                "threadId": thread_id,
                "lastTurnId": last_turn_id,
                "approvalPolicy": "never",
                "sandbox": "danger-full-access",
            },
        ))
        while not request.done():
            try:
                await asyncio.shield(request)
            except asyncio.CancelledError:
                continue
        response = request.result()
        thread = response.get("thread")
        fork_id = thread.get("id") if isinstance(thread, dict) else None
        if not fork_id or fork_id == thread_id:
            raise CodexAppServerError(
                f"thread/fork returned an invalid thread for {thread_id}"
            )
        self._known_threads.add(str(fork_id))
        return thread

    async def delete_thread(self, thread_id: str) -> None:
        """Delete one terminal thread and release its thread-scoped resources."""

        if not thread_id:
            raise ValueError("thread_id is required")
        # Terminal cleanup is durable and may be retried after a CCM restart.
        # Start this home's transport on demand instead of requiring an earlier
        # turn in the current process to have populated the registry.
        await self.ensure_started()
        if self.has_active_thread(thread_id):
            raise CodexAppServerBusyError(
                f"Codex thread {thread_id} still has an active turn"
            )
        await self._request("thread/delete", {"threadId": thread_id})
        self._known_threads.discard(thread_id)
        self._contexts_by_thread.pop(thread_id, None)
        for turn_id, context in list(self._contexts_by_turn.items()):
            if context.thread_id == thread_id:
                self._contexts_by_turn.pop(turn_id, None)

    async def unsubscribe_thread(self, thread_id: str) -> str:
        """Release this client's idle subscription while preserving history."""

        if not thread_id:
            raise ValueError("thread_id is required")
        if self.has_active_thread(thread_id):
            raise CodexAppServerBusyError(
                f"Codex thread {thread_id} still has an active turn"
            )
        response = await self._request(
            "thread/unsubscribe",
            {"threadId": thread_id},
        )
        status = response.get("status")
        if status not in {"unsubscribed", "notSubscribed", "notLoaded"}:
            raise CodexAppServerError(
                f"thread/unsubscribe returned invalid status for {thread_id}: "
                f"{status!r}"
            )
        if status == "notLoaded":
            self._known_threads.discard(thread_id)
        return status

    async def recycle_thread_runtime(self, thread_id: str) -> None:
        """Reload one idle thread without changing its persisted identity.

        Codex keeps a resumed thread's MCP clients alive, so a later
        ``thread/resume`` does not apply generation-specific MCP arguments.
        Archiving unloads that runtime; unarchiving restores the same persisted
        thread and history so the next resume creates fresh MCP clients.
        """

        if not thread_id:
            raise ValueError("thread_id is required")
        await self.ensure_started()
        if self.has_active_thread(thread_id):
            raise CodexAppServerBusyError(
                f"Codex thread {thread_id} still has an active turn"
            )

        async def _archive_then_unarchive() -> None:
            await self._request(
                "thread/archive",
                {"threadId": thread_id},
            )
            response = await self._request(
                "thread/unarchive",
                {"threadId": thread_id},
            )
            thread = (
                response.get("thread")
                if isinstance(response, dict)
                else None
            )
            if not isinstance(thread, dict) or thread.get("id") != thread_id:
                raise CodexAppServerError(
                    "thread/unarchive returned an invalid thread for "
                    f"{thread_id}"
                )

        # Cancellation after archive is on the wire must not leave a resumable
        # Monitor rollout stranded in the archive. Settle the whole pair before
        # propagating cancellation or any RPC failure.
        await _settle_registry_cleanup(_archive_then_unarchive())
        self._known_threads.add(thread_id)

    async def abandon_turn(
        self,
        process: CodexTurnProcess,
        reason: str,
    ) -> bool:
        """Interrupt and detach a turn that no caller can consume.

        Returns whether the interrupt RPC was confirmed.  The registry uses a
        false result to escalate to shutting down the account transport, which
        is the only safe way to rule out untracked model work.
        """

        context = next(
            (
                candidate
                for candidate in self._contexts_by_thread.values()
                if candidate.process is process
            ),
            None,
        )
        try:
            if context is None or not context.turn_id:
                return process.returncode is not None
            await self._interrupt_turn_context(context)
        except BaseException:
            logger.exception(
                "Failed to interrupt unclaimed Codex turn in %s",
                self.codex_home,
            )
            # Keep every mapping and the adapter open.  The registry sees the
            # false result and shuts down the whole account transport before
            # it releases any task/instance ownership.
            return False
        if process.returncode is None:
            if not self._context_is_current(context):
                return False
            self._detach_turn_context(context)
            process.finish(
                130,
                reason,
                termination_kind="internal_abort",
            )
        return True

    async def steer_turn(
        self,
        thread_id: str,
        content: str,
        *,
        input_items: list[dict[str, Any]] | None = None,
    ) -> bool:
        """Append user input to the currently active regular turn.

        ``expectedTurnId`` makes the request race-safe: if the turn finishes
        between the local context lookup and the RPC, app-server rejects the
        stale steer instead of attaching it to a later turn.
        """
        if not self.is_alive or not thread_id or (not content and not input_items):
            return False
        if input_items is None:
            steer_input: list[dict[str, Any]] = [
                {"type": "text", "text": content},
            ]
        else:
            steer_input = []
            for item in input_items:
                if not isinstance(item, dict):
                    raise ValueError("Invalid Codex steer input item")
                item_type = item.get("type")
                if (
                    item_type == "text"
                    and set(item) == {"type", "text"}
                    and isinstance(item.get("text"), str)
                    and item["text"]
                ):
                    steer_input.append({
                        "type": "text",
                        "text": item["text"],
                    })
                elif (
                    item_type == "localImage"
                    and set(item) == {"type", "path"}
                    and isinstance(item.get("path"), str)
                    and os.path.isabs(item["path"])
                ):
                    steer_input.append({
                        "type": "localImage",
                        "path": item["path"],
                    })
                elif (
                    item_type == "mention"
                    and set(item) == {"type", "name", "path"}
                    and isinstance(item.get("name"), str)
                    and item["name"]
                    and isinstance(item.get("path"), str)
                    and os.path.isabs(item["path"])
                ):
                    steer_input.append({
                        "type": "mention",
                        "name": item["name"],
                        "path": item["path"],
                    })
                else:
                    raise ValueError("Invalid Codex steer input item")
            if not steer_input:
                raise ValueError("Codex steer input cannot be empty")
        context = self._contexts_by_thread.get(thread_id)
        if (
            context is None
            or context.turn_id is None
            or context.process.returncode is not None
        ):
            return False

        expected_turn_id = context.turn_id
        try:
            response = await self._request(
                "turn/steer",
                {
                    "threadId": thread_id,
                    "expectedTurnId": expected_turn_id,
                    "input": steer_input,
                },
            )
        except Exception as exc:
            actual_turn_id = _active_turn_id_from_error(exc)
            if (
                actual_turn_id
                and self._contexts_by_thread.get(thread_id) is context
                and self._bind_turn_context(
                    context,
                    actual_turn_id,
                    observed=True,
                )
            ):
                try:
                    response = await self._request(
                        "turn/steer",
                        {
                            "threadId": thread_id,
                            "expectedTurnId": actual_turn_id,
                            "input": steer_input,
                        },
                    )
                except Exception as retry_exc:
                    exc = retry_exc
                else:
                    return response.get("turnId") == actual_turn_id
            # A normal turn-boundary race and non-steerable turns (review or
            # manual compact) are protocol rejections, not transport crashes.
            logger.info(
                "Codex steer rejected thread=%s turn=%s reason=%s",
                thread_id,
                expected_turn_id,
                exc,
            )
            return False
        return response.get("turnId") == expected_turn_id

    async def _request(
        self, method: str, params: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if not self.is_alive or not self._process or not self._process.stdin:
            raise CodexAppServerError("app-server is not running")
        self._request_id += 1
        request_id = self._request_id
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            message: dict[str, Any] = {"id": request_id, "method": method}
            if params is not None:
                message["params"] = params
            await self._write(message)
            response = await asyncio.wait_for(future, timeout=self.request_timeout)
        finally:
            # Cancellation can happen while waiting for the shared write lock
            # or draining stdin, before the response wait is entered. Never
            # leave that future permanently registered.
            self._pending.pop(request_id, None)
        if "error" in response:
            error = response.get("error") or {}
            raise CodexAppServerRequestError(
                f"{method} failed: {error.get('message') or error}"
            )
        result = response.get("result")
        return result if isinstance(result, dict) else {}

    async def _notify(self, method: str, params: dict[str, Any]) -> None:
        await self._write({"method": method, "params": params})

    async def _write(self, message: dict[str, Any]) -> None:
        if not self._process or not self._process.stdin:
            raise CodexAppServerError("app-server stdin is unavailable")
        payload = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        async with self._write_lock:
            self._process.stdin.write(payload.encode("utf-8") + b"\n")
            await self._process.stdin.drain()

    def _finalize_transport_exit(
        self,
        process: asyncio.subprocess.Process,
        returncode: int | None,
        planned_shutdown: tuple[
            asyncio.subprocess.Process,
            CodexTurnProcess | None,
            str,
        ] | None,
    ) -> None:
        """Settle every waiter and adapter for one exact transport generation."""

        if self._finalized_transport_process is process:
            return
        self._finalized_transport_process = process
        detail = "\n".join(self._stderr_lines)[-4000:]
        planned = bool(
            planned_shutdown is not None
            and planned_shutdown[0] is process
        )
        if planned:
            error = CodexAppServerError(
                "Codex app-server shut down by CCM "
                f"({_format_process_exit(returncode)})"
            )
            if detail:
                logger.debug(
                    "Codex app-server stderr before planned shutdown "
                    "home=%s pid=%s:\n%s",
                    self.codex_home,
                    getattr(process, "pid", None),
                    detail,
                )
        else:
            error = CodexAppServerError(
                "Codex app-server exited unexpectedly "
                f"({_format_process_exit(returncode)})"
            )
            logger.error(
                "Codex app-server transport exited unexpectedly "
                "home=%s pid=%s %s%s",
                self.codex_home,
                getattr(process, "pid", None),
                _format_process_exit(returncode),
                f"; stderr tail:\n{detail}" if detail else "",
            )
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(error)
        self._pending.clear()
        for future in list(self._thread_settings_waiters.values()):
            if not future.done():
                future.set_exception(error)
        self._thread_settings_waiters.clear()
        for context in list(self._contexts_by_thread.values()):
            future = context.admission_observed_future
            if future is not None and not future.done():
                future.set_exception(error)
            if (
                planned
                and planned_shutdown is not None
                and context.process is planned_shutdown[1]
            ):
                context.process.finish(
                    130,
                    planned_shutdown[2],
                    termination_kind="internal_abort",
                )
            else:
                # A planned account/server shutdown is not successful turn
                # completion.  Any non-target adapter that never received
                # turn/completed still fails, but receives only a fixed
                # transport-level reason rather than another task's shared
                # stderr history.
                context.process.finish(1, str(error))
            self._detach_turn_context(context)
        self._contexts_by_thread.clear()
        self._contexts_by_turn.clear()
        self._contexts_by_descendant.clear()
        self._children_by_thread.clear()
        self._thread_runtime.clear()
        if (
            self._planned_shutdown is not None
            and self._planned_shutdown[0] is process
        ):
            self._planned_shutdown = None
        if (
            self._observed_transport_exit is not None
            and self._observed_transport_exit[0] is process
        ):
            self._observed_transport_exit = None

    async def _read_loop(self, process: asyncio.subprocess.Process) -> None:
        assert process.stdout
        try:
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                try:
                    message = json.loads(line)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    logger.warning("Ignoring malformed Codex app-server output")
                    continue
                request_id = message.get("id")
                if request_id is not None and ("result" in message or "error" in message):
                    future = self._pending.pop(request_id, None)
                    if future and not future.done():
                        future.set_result(message)
                    continue
                if request_id is not None and message.get("method"):
                    await self._handle_server_request(message)
                    continue
                if message.get("method"):
                    self._handle_notification(message["method"], message.get("params") or {})
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Codex app-server reader failed")
        finally:
            # Freeze intent at the instant EOF/reader failure is observed.  A
            # later administrative shutdown must not relabel an already-broken
            # transport as a planned clean exit while process.wait() settles.
            planned_shutdown = self._planned_shutdown
            if (
                planned_shutdown is None
                or planned_shutdown[0] is not process
            ):
                planned_shutdown = None
            self._observed_transport_exit = (process, planned_shutdown)
            returncode = await process.wait()
            self._finalize_transport_exit(
                process,
                returncode,
                planned_shutdown,
            )

    async def _stderr_loop(self, process: asyncio.subprocess.Process) -> None:
        assert process.stderr
        try:
            while True:
                line = await process.stderr.readline()
                if not line:
                    break
                self._stderr_lines.append(
                    line.decode("utf-8", errors="replace").rstrip()
                )
        except asyncio.CancelledError:
            return

    async def _handle_server_request(self, message: dict[str, Any]) -> None:
        method = str(message.get("method") or "")
        request_id = message.get("id")
        params = message.get("params") or {}
        tool_free_context = self._tool_free_context_for_params(params)
        if (
            tool_free_context is not None
            and (
                method.startswith(("item/", "mcpServer/"))
                or method in {
                    "applyPatchApproval",
                    "currentTime/read",
                    "execCommandApproval",
                }
            )
        ):
            # Respond first so the app-server reader can continue processing
            # the interrupt RPC scheduled below. Awaiting that RPC from inside
            # this request handler would deadlock the shared stdio reader.
            if method in {
                "item/commandExecution/requestApproval",
                "item/fileChange/requestApproval",
            }:
                response = {"id": request_id, "result": {"decision": "decline"}}
            elif method in {"applyPatchApproval", "execCommandApproval"}:
                response = {"id": request_id, "result": {"decision": "denied"}}
            else:
                response = {
                    "id": request_id,
                    "error": {
                        "code": -32600,
                        "message": "Tool calls are disabled for PR review",
                    },
                }
            await self._write(response)
            self._schedule_tool_free_violation(
                tool_free_context,
                f"server request {method}",
            )
            return
        if method in {
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
        }:
            # Mirrors --dangerously-bypass-approvals-and-sandbox.  These should
            # not normally arrive because approvalPolicy is "never", but an
            # explicit response prevents a protocol deadlock if they do.
            await self._write({"id": request_id, "result": {"decision": "accept"}})
            return
        if method == "item/permissions/requestApproval":
            # This newer API has a different response schema: grant the exact
            # requested profile for this turn, matching danger-full-access.
            await self._write({
                "id": request_id,
                "result": {
                    "permissions": params.get("permissions") or {},
                    "scope": "turn",
                },
            })
            return
        if method in {"applyPatchApproval", "execCommandApproval"}:
            # Legacy v1 approval requests use ReviewDecision values.
            await self._write({
                "id": request_id,
                "result": {"decision": "approved"},
            })
            return
        if method == "currentTime/read":
            await self._write({
                "id": request_id,
                "result": {"currentTimeAt": int(time.time())},
            })
            return
        await self._write(
            {
                "id": request_id,
                "error": {"code": -32601, "message": f"Unsupported request: {method}"},
            }
        )

    def _handle_notification(self, method: str, params: dict[str, Any]) -> None:
        if method == "skills/changed":
            # Skill discovery is process-global. Any change invalidates the
            # exact path inventory captured for every active tool-free turn.
            self._skills_revision += 1
            for context in list(self._contexts_by_thread.values()):
                if context.tools_disabled:
                    self._schedule_tool_free_violation(
                        context,
                        "skills inventory changed",
                    )
            return
        if method == "thread/started":
            thread = params.get("thread")
            if isinstance(thread, dict):
                child_id = thread.get("id")
                parent_id = thread.get("parentThreadId")
                if isinstance(child_id, str):
                    self._record_thread_status(
                        child_id,
                        thread.get("status"),
                    )
                if (
                    isinstance(child_id, str)
                    and isinstance(parent_id, str)
                ):
                    self._record_child_relation(parent_id, child_id)
                    lineage_context = self._lineage_context_for_thread(parent_id)
                    if lineage_context is not None:
                        status_type = self._thread_status_type(
                            thread.get("status")
                        )
                        self._attach_descendant(
                            lineage_context,
                            child_id,
                            active=(
                                status_type == "active"
                                if status_type is not None
                                else None
                            ),
                        )
                        if lineage_context.tools_disabled:
                            self._schedule_tool_free_violation(
                                lineage_context,
                                f"native child thread {child_id}",
                            )
                proxy = self._actual_tier_proxy
                if (
                    proxy is not None
                    and isinstance(child_id, str)
                    and isinstance(parent_id, str)
                ):
                    try:
                        proxy.register_thread_parent(child_id, parent_id)
                    except CodexTierProofError:
                        # The request path independently requires the same
                        # lineage metadata and will reject it before upstream
                        # work. Keep notification handling synchronous and
                        # non-throwing so the shared app-server reader lives.
                        logger.exception(
                            "Rejected ambiguous Codex child-thread lineage "
                            "child=%s parent=%s",
                            child_id,
                            parent_id,
                        )
            return
        thread_id = params.get("threadId")
        thread_id_str = str(thread_id) if thread_id else None
        if method == "thread/goal/updated" and thread_id_str is not None:
            goal = params.get("goal")
            goal_status = (
                str(goal.get("status"))
                if isinstance(goal, dict) and goal.get("status")
                else None
            )
            self._runtime_state_for(thread_id_str).goal_status = goal_status
            context = self._contexts_by_thread.get(thread_id_str)
            if context is not None and not params.get("turnId"):
                self._finish_retained_goal_if_terminal(
                    context,
                    goal_status,
                )
        elif method == "thread/goal/cleared" and thread_id_str is not None:
            self._runtime_state_for(thread_id_str).goal_status = None
            context = self._contexts_by_thread.get(thread_id_str)
            if context is not None:
                self._finish_retained_goal_if_terminal(context, None)
        if method == "thread/status/changed":
            if thread_id_str is not None:
                self._record_thread_status(
                    thread_id_str,
                    params.get("status"),
                )
            return
        if method == "thread/closed":
            if thread_id_str is not None:
                self._record_thread_status(
                    thread_id_str,
                    {"type": "notLoaded"},
                )
            return
        if method == "thread/settings/updated":
            waiter = (
                self._thread_settings_waiters.get(thread_id_str)
                if thread_id_str
                else None
            )
            if waiter is not None and not waiter.done():
                waiter.set_result(params)
            return
        turn_id = self._notification_turn_id(method, params)
        context = self._contexts_by_turn.get(turn_id) if turn_id else None
        if (
            context is not None
            and thread_id_str
            and thread_id_str != context.thread_id
        ):
            logger.error(
                "Ignoring Codex notification with mismatched thread/turn "
                "thread=%s expected_thread=%s turn=%s method=%s",
                thread_id,
                context.thread_id,
                turn_id,
                method,
            )
            return
        notification_client_ids = self._notification_user_message_client_ids(
            method,
            params,
        )
        if (
            context is not None
            and context.client_user_message_id
            and notification_client_ids
            and context.client_user_message_id not in notification_client_ids
        ):
            # An exact turn-id mapping is not allowed to override explicit
            # schema-backed evidence that this user input belongs to another
            # request. Ignore the whole notification, including lifecycle
            # bookkeeping, so a contradictory terminal cannot close this
            # adapter or falsely mark its runtime idle.
            logger.error(
                "Ignoring Codex notification with mismatched client input "
                "thread=%s turn=%s expected_client=%s actual_clients=%s "
                "method=%s",
                thread_id,
                turn_id,
                context.client_user_message_id,
                sorted(notification_client_ids),
                method,
            )
            return
        if (
            thread_id_str is not None
            and method in {"turn/started", "turn/completed"}
        ):
            self._record_thread_turn_lifecycle(
                method,
                thread_id_str,
                turn_id,
            )
        if context is None and thread_id_str:
            candidate = self._contexts_by_thread.get(thread_id_str)
            if candidate is not None and turn_id:
                # turn/start can return a fresh submission id while its input
                # is actually steered into another native turn. turn/started is
                # only provisional: the response-first race can publish the
                # submission id before the native id. A schema-backed clientId
                # is the authoritative cross-id correlation proof.
                native_goal_continuation = bool(
                    method == "turn/started"
                    and candidate.following_native_goal
                    and candidate.turn_id is None
                )
                matches_input = self._notification_matches_context_input(
                    candidate,
                    method,
                    params,
                )
                if native_goal_continuation:
                    # The first user submission needs client-id proof because
                    # app-server may steer it into an older native turn.  Once
                    # CCM already owns an active native Goal, however, its next
                    # autonomous turn has no new userMessage/clientId.  The
                    # exact retained thread owner plus turn/started is the
                    # authoritative continuation boundary.
                    if not self._bind_turn_context(
                        candidate,
                        turn_id,
                        observed=True,
                    ):
                        return
                elif matches_input:
                    if not self._promote_correlated_turn_context(
                        candidate,
                        turn_id,
                    ):
                        return
                elif candidate.observed_turn_id is not None:
                    logger.debug(
                        "Ignoring Codex notification for unrelated turn "
                        "thread=%s expected=%s actual=%s method=%s",
                        thread_id,
                        candidate.observed_turn_id,
                        turn_id,
                        method,
                    )
                    return
                elif method == "turn/started":
                    candidate.provisional_started_turn_ids.add(turn_id)
                else:
                    logger.debug(
                        "Ignoring uncorrelated Codex notification for unknown "
                        "turn thread=%s candidates=%s actual=%s method=%s",
                        thread_id,
                        sorted(candidate.provisional_started_turn_ids),
                        turn_id,
                        method,
                    )
                    return
            context = candidate
        if context is None:
            # Child-thread output is intentionally not rendered as if it were
            # the root assistant's answer.  Its collaboration lifecycle still
            # extends the exact root generation and may discover grandchildren.
            lineage_context = (
                self._contexts_by_descendant.get(thread_id_str)
                if thread_id_str is not None
                else None
            )
            if (
                lineage_context is not None
                and self._context_is_current(lineage_context)
                and method in {"item/started", "item/completed"}
            ):
                self._track_collaboration_item(
                    lineage_context,
                    thread_id_str,
                    params.get("item"),
                )
            return
        if turn_id and method == "turn/started":
            context.provisional_started_turn_ids.add(turn_id)
        elif turn_id and context.observed_turn_id is None:
            matches_input = self._notification_matches_context_input(
                context,
                method,
                params,
            )
            if matches_input and context.turn_id != turn_id:
                if not self._promote_correlated_turn_context(context, turn_id):
                    return
            elif matches_input:
                if not self._bind_turn_context(context, turn_id, observed=True):
                    return
        if turn_id and context.observed_turn_id is not None:
            observed_future = context.admission_observed_future
            if observed_future is not None and not observed_future.done():
                observed_future.set_result(context.observed_turn_id)
        if method == "turn/started" and (
            context.following_native_goal
            or context.pending_goal_terminal_notification is not None
        ):
            self._confirm_goal_continuation_started(context)
        if (
            context.pending_admission_notifications is not None
            and not context.admission_confirmed
        ):
            context.pending_admission_notifications.append(
                (method, dict(params)),
            )
            return
        if context.tools_disabled:
            if method in {"item/started", "item/completed"}:
                item = params.get("item")
                item_type = (
                    item.get("type")
                    if isinstance(item, dict)
                    else None
                )
                if item_type not in _TOOL_FREE_PASSIVE_ITEM_TYPES:
                    self._schedule_tool_free_violation(
                        context,
                        f"{method} item type {item_type!r}",
                    )
                    return
            elif method.startswith(
                _TOOL_FREE_FORBIDDEN_NOTIFICATION_PREFIXES
            ):
                self._schedule_tool_free_violation(
                    context,
                    f"notification {method}",
                )
                return
            elif method not in _TOOL_FREE_PASSIVE_NOTIFICATION_METHODS:
                # Treat new protocol surface as executable until explicitly
                # audited and added to the passive allow-list.
                self._schedule_tool_free_violation(
                    context,
                    f"unexpected notification {method}",
                )
                return
        if (
            method == "turn/completed"
            and context.admitted_turn_id is None
            and not context.following_native_goal
        ):
            # Notifications may race ahead of the turn/start RPC response.
            # Keep the adapter open until the response supplies a real turn id
            # and Fast can publish its admission proof before terminal EOF.
            if context.pending_terminal_notification is None:
                context.pending_terminal_notification = (
                    method,
                    dict(params),
                )
            else:
                logger.warning(
                    "Ignoring duplicate pre-admission terminal notification "
                    "thread=%s turn=%s",
                    context.thread_id,
                    turn_id,
                )
            return

        if method == "turn/started":
            if not context.turn_started_emitted:
                context.turn_started_emitted = True
                context.process.feed({"type": "turn.started"})
            return

        if method == "item/started":
            item = params.get("item") or {}
            self._track_collaboration_item(
                context,
                context.thread_id,
                item,
            )
            if item.get("type") == "userMessage" and not context.first_input_seen:
                context.first_input_seen = True
                logger.info(
                    "Codex latency task=%s thread=%s stage=model_input elapsed_ms=%.1f",
                    context.task_id,
                    context.thread_id,
                    (time.perf_counter() - context.launch_started) * 1000,
                )
            normalized = self._normalize_item(item)
            if normalized and normalized.get("type") in {
                "command_execution",
                "file_change",
                "mcp_tool_call",
                "web_search",
                "collab_agent_tool_call",
            }:
                context.process.feed({
                    "type": "item.started",
                    "item": normalized,
                    "turn_id": context.turn_id,
                })
            return

        if method == "item/completed":
            item = params.get("item") or {}
            self._track_collaboration_item(
                context,
                context.thread_id,
                item,
            )
            normalized = self._normalize_item(item)
            if normalized and normalized.get("type") not in {
                "user_message",
                # These are lifecycle metadata, not user-facing completion
                # events. Passing them to the generic parser would render
                # misleading ``item.completed`` separators.
                "sub_agent_activity",
                "context_compaction",
            }:
                context.process.feed({
                    "type": "item.completed",
                    "item": normalized,
                    "turn_id": context.turn_id,
                })
            return

        if method == "item/agentMessage/delta":
            if not context.first_output_seen:
                context.first_output_seen = True
                logger.info(
                    "Codex latency task=%s thread=%s stage=first_delta elapsed_ms=%.1f",
                    context.task_id,
                    context.thread_id,
                    (time.perf_counter() - context.launch_started) * 1000,
                )
            context.process.feed(
                {
                    "type": "item.agent_message.delta",
                    "delta": params.get("delta") or "",
                    "item_id": params.get("itemId"),
                    "turn_id": context.turn_id,
                }
            )
            return

        if method in {
            "item/reasoning/summaryTextDelta",
            "item/reasoning/textDelta",
        }:
            context.process.feed(
                {
                    "type": "item.reasoning.delta",
                    "delta": params.get("delta") or "",
                    "item_id": params.get("itemId"),
                    "turn_id": context.turn_id,
                }
            )
            return

        if method == "thread/tokenUsage/updated":
            token_usage = params.get("tokenUsage") or {}
            last = token_usage.get("last") or token_usage.get("total") or {}
            context.usage = {
                "input_tokens": int(last.get("inputTokens") or 0),
                "cached_input_tokens": int(last.get("cachedInputTokens") or 0),
                "output_tokens": int(last.get("outputTokens") or 0),
                "reasoning_output_tokens": int(
                    last.get("reasoningOutputTokens") or 0
                ),
                "total_tokens": int(last.get("totalTokens") or 0),
                "context_window": int(
                    token_usage.get("modelContextWindow") or 0
                ),
            }
            return

        if method == "error":
            # App-server also publishes this notification while the Codex
            # client is retrying a live turn.  ``willRetry`` is authoritative:
            # retry notices are advisory, while non-retry errors must remain
            # fatal even though turn/completed closes the adapter later.
            error = params.get("error") or params
            normalized_error = self._normalize_turn_error(error)
            will_retry = bool(params.get("willRetry"))
            if not will_retry and context.non_retry_error is None:
                context.non_retry_error = normalized_error
            context.process.feed({
                "type": "turn.retrying" if will_retry else "turn.failed",
                "error": normalized_error,
                "turn_id": context.turn_id,
                "will_retry": will_retry,
                "terminal": not will_retry,
            })
            return

        if method == "turn/completed":
            turn = params.get("turn") or {}
            status = turn.get("status") or "completed"
            if (
                context.descendant_thread_ids
                and (
                    status == "completed"
                    or context.active_descendant_thread_ids
                )
            ):
                self._defer_terminal_turn_for_descendants(context, params)
                return
            self._finish_turn_context(context, params)

    @staticmethod
    def _normalize_turn_error(
        error: Any,
        *,
        fallback: str = "Codex turn failed",
    ) -> dict[str, Any]:
        """Keep app-server error classification fields alongside its message."""

        if isinstance(error, dict):
            normalized = dict(error)
            message = normalized.get("message") or fallback
        else:
            normalized = {}
            message = error or fallback
        normalized["message"] = str(message)
        return normalized

    @staticmethod
    def _normalize_item(item: dict[str, Any]) -> dict[str, Any] | None:
        item_type = item.get("type")
        type_map = {
            "userMessage": "user_message",
            "agentMessage": "agent_message",
            "commandExecution": "command_execution",
            "fileChange": "file_change",
            "mcpToolCall": "mcp_tool_call",
            "webSearch": "web_search",
            "todoList": "todo_list",
            "collabAgentToolCall": "collab_agent_tool_call",
            "subAgentActivity": "sub_agent_activity",
            "contextCompaction": "context_compaction",
        }
        normalized = dict(item)
        normalized["type"] = type_map.get(item_type, item_type)
        rename = {
            "aggregatedOutput": "aggregated_output",
            "exitCode": "exit_code",
            "senderThreadId": "sender_thread_id",
            "receiverThreadIds": "receiver_thread_ids",
            "reasoningEffort": "reasoning_effort",
            "agentsStates": "agents_states",
            "agentPath": "agent_path",
            "agentThreadId": "agent_thread_id",
        }
        for source, target in rename.items():
            if source in normalized:
                normalized[target] = normalized.pop(source)
        if normalized.get("type") == "reasoning":
            pieces = normalized.get("summary") or normalized.get("content") or []
            normalized["text"] = "\n".join(str(piece) for piece in pieces if piece)
        return normalized

    def _managed_process_group_id(
        self,
        process: asyncio.subprocess.Process,
    ) -> int | None:
        """Return the exact signal-safe PGID created for this generation."""

        if (
            os.name != "posix"
            or self._process_group_process is not process
        ):
            return None
        return require_safe_process_group_id(
            getattr(process, "pid", None),
            context=f"Codex app-server home {self.codex_home}",
        )

    def _process_group_alive(
        self,
        process: asyncio.subprocess.Process,
    ) -> bool:
        process_group_id = self._managed_process_group_id(process)
        if process_group_id is None:
            return False
        try:
            os.killpg(process_group_id, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            # Inability to signal is not proof that the group disappeared.
            return True

    def _signal_process_generation(
        self,
        process: asyncio.subprocess.Process,
        sig: signal.Signals,
    ) -> None:
        process_group_id = self._managed_process_group_id(process)
        try:
            if process_group_id is not None:
                os.killpg(process_group_id, sig)
            elif sig == signal.SIGTERM:
                process.terminate()
            elif sig == signal.SIGKILL:
                process.kill()
            else:
                process.send_signal(sig)
        except ProcessLookupError:
            # Exit may race the signal.  The bounded verification below is the
            # authority for whether the full generation is actually gone.
            return

    async def _wait_process_generation(
        self,
        process: asyncio.subprocess.Process,
        timeout: float,
    ) -> None:
        """Wait until both the app-server leader and its POSIX group are gone."""

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        if process.returncode is None:
            await asyncio.wait_for(
                asyncio.shield(process.wait()),
                timeout=max(0.01, timeout),
            )
        while self._process_group_alive(process):
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError
            await asyncio.sleep(
                min(_APP_SERVER_GROUP_POLL_INTERVAL, remaining)
            )

    async def _shutdown_locked(self) -> None:
        """Stop one generation while ``_lifecycle_lock`` is held."""

        process = self._process
        if not process:
            proxy = self._actual_tier_proxy
            self._actual_tier_proxy = None
            if proxy is not None:
                await proxy.close()
            return
        if process.stdin:
            try:
                process.stdin.close()
            except Exception:
                logger.exception(
                    "Failed to close Codex app-server stdin home=%s",
                    self.codex_home,
                )

        try:
            await self._wait_process_generation(
                process,
                _APP_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT,
            )
        except asyncio.TimeoutError:
            self._signal_process_generation(process, signal.SIGTERM)
            try:
                await self._wait_process_generation(
                    process,
                    _APP_SERVER_TERM_SHUTDOWN_TIMEOUT,
                )
            except asyncio.TimeoutError:
                self._signal_process_generation(process, signal.SIGKILL)
                try:
                    await self._wait_process_generation(
                        process,
                        _APP_SERVER_KILL_SHUTDOWN_TIMEOUT,
                    )
                except asyncio.TimeoutError as exc:
                    # Do not clear the process/tasks: callers and the
                    # registry need that evidence to remain fail-closed.
                    raise CodexAppServerError(
                        "Codex app-server process group survived SIGKILL "
                        f"for home {self.codex_home}"
                    ) from exc

        for task in (self._reader_task, self._stderr_task):
            if task and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (self._reader_task, self._stderr_task) if task),
            return_exceptions=True,
        )
        # A very fast clean exit can complete before the reader coroutine gets
        # its first scheduling turn; cancelling such an unstarted task does not
        # execute its ``finally`` block.  Finalize here as an idempotent fallback
        # so adapters and pending RPCs can never remain open after verified
        # transport shutdown.
        observed_exit = self._observed_transport_exit
        if observed_exit is not None and observed_exit[0] is process:
            planned_shutdown = observed_exit[1]
        else:
            planned_shutdown = self._planned_shutdown
            if (
                planned_shutdown is None
                or planned_shutdown[0] is not process
            ):
                planned_shutdown = None
        self._finalize_transport_exit(
            process,
            process.returncode,
            planned_shutdown,
        )
        if self._process is process:
            self._process = None
        if self._process_group_process is process:
            self._process_group_process = None
        self._reader_task = None
        self._stderr_task = None
        proxy = self._actual_tier_proxy
        self._actual_tier_proxy = None
        if proxy is not None:
            await proxy.close()

    async def shutdown(
        self,
        *,
        interrupted_process: CodexTurnProcess | None = None,
        reason: str = "CCM requested Codex app-server shutdown",
    ) -> None:
        """Permanently stop and verify this server object's process group."""

        # Publish the close intent before waiting for the lifecycle barrier.
        # A start already inside the barrier will be stopped after it exits;
        # one queued behind it will observe this flag and cannot spawn later.
        self._shutdown_requested = True
        async with self._lifecycle_lock:
            process = self._process
            planned_shutdown = self._planned_shutdown
            if (
                process is not None
                and process.returncode is None
                and (
                    planned_shutdown is None
                    or planned_shutdown[0] is not process
                )
            ):
                self._planned_shutdown = (
                    process,
                    interrupted_process,
                    reason,
                )
            await self._shutdown_locked()


class CodexAppServerRegistry:
    """Route Codex turns to one persistent app-server per account home.

    ``CODEX_HOME`` is process-scoped, so changing an environment variable per
    thread on one app-server can never provide account isolation.  This facade
    keeps the old InstanceManager-facing surface while enforcing a stable
    thread -> home owner and independent process lifecycle for every account.
    """

    def __init__(
        self,
        binary: str,
        request_timeout: float = 30.0,
        *,
        env_remove_resolver: Callable[[str], set[str]] | None = None,
        actual_tier_route_resolver: (
            Callable[[str], CodexTierProxyRoute | None] | None
        ) = None,
        require_actual_tier_proof: bool = False,
    ) -> None:
        self.binary = binary
        self.request_timeout = request_timeout
        self._env_remove_resolver = env_remove_resolver
        self._actual_tier_route_resolver = actual_tier_route_resolver
        self._require_actual_tier_proof = bool(require_actual_tier_proof)
        self._servers: dict[str, CodexAppServer] = {}
        self._thread_owners: dict[str, str] = {}
        self._draining: set[str] = set()
        # A thread rebind spans an optional target-server shutdown. Keep the
        # route reserved across that await so a manual resume or account
        # maintenance operation cannot reopen the source/target midway.
        self._rebindings: dict[str, tuple[str, str]] = {}
        # Number of home-bound RPC sequences (turn start/resume or account
        # reads) admitted but not yet returned to the caller. Maintenance
        # checks this under the same lock so relogin cannot race between
        # server lookup and an RPC using the old auth.json.
        self._starting: dict[str, int] = {}
        # A home-level count protects maintenance; this per-thread token also
        # prevents two concurrent resumes of the same native thread. Without
        # it, a failed first request could remove the successful second
        # request's owner because both routes contain the same home value.
        self._starting_threads: dict[str, object] = {}
        self._shutdown_requested = False
        self._lock = asyncio.Lock()
        # Claimed stops and truly-unclaimed admission cleanup can both need to
        # reason about every turn on one shared transport.  Serialize those
        # decisions per home so two cleanup attempts cannot independently
        # conclude that shutting down the same server generation is safe.
        self._abort_locks: dict[str, asyncio.Lock] = {}

    def _new_server(self, home: str) -> CodexAppServer:
        server_kwargs: dict[str, Any] = {}
        if self._env_remove_resolver is not None:
            server_kwargs["env_remove"] = self._env_remove_resolver(home)
        if self._actual_tier_route_resolver is not None:
            server_kwargs["actual_tier_proxy_route"] = (
                self._actual_tier_route_resolver(home)
            )
        if self._require_actual_tier_proof:
            server_kwargs["require_actual_tier_proof"] = True
        return CodexAppServer(
            self.binary,
            request_timeout=self.request_timeout,
            codex_home=home,
            **server_kwargs,
        )

    async def start_turn(
        self,
        *,
        codex_home: str | os.PathLike[str] | None = None,
        **kwargs: Any,
    ) -> tuple[CodexTurnProcess, str]:
        home = normalize_codex_home(codex_home)
        resume_session_id = kwargs.get("resume_session_id")
        reserved_owner = False
        start_token: object | None = None

        async with self._lock:
            if self._shutdown_requested:
                raise CodexAppServerBusyError(
                    "Codex app-server registry is shutting down"
                )
            if home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is draining: {home}"
                )
            if resume_session_id:
                if resume_session_id in self._starting_threads:
                    raise CodexAppServerBusyError(
                        f"Codex thread {resume_session_id} already has a resume request in flight"
                    )
                if resume_session_id in self._rebindings:
                    raise CodexAppServerBusyError(
                        f"Codex thread {resume_session_id} is being rebound"
                    )
                owner = self._thread_owners.get(resume_session_id)
                if owner is not None and owner != home:
                    raise CodexThreadHomeMismatchError(
                        f"Codex thread {resume_session_id} is bound to {owner}, not {home}; "
                        "migrate and rebind it before resume"
                    )
                if owner is None:
                    # Reserve the route while the RPC is in flight so two
                    # concurrent resumes cannot load one rollout in two homes.
                    self._thread_owners[resume_session_id] = home
                    reserved_owner = True
                start_token = object()
                self._starting_threads[resume_session_id] = start_token
            server = self._servers.get(home)
            if server is None:
                server = self._new_server(home)
                self._servers[home] = server
            self._starting[home] = self._starting.get(home, 0) + 1

        process: CodexTurnProcess | None = None
        thread_id: str | None = None
        admitted = False
        starting_released = False
        try:
            try:
                process, thread_id = await server.start_turn(**kwargs)
            except (
                _UnconfirmedTurnCancellation,
                _UnconfirmedTurnStartFailure,
            ) as exc:
                # The local adapter is terminal, but the real server-side turn
                # may still be executing. Preserve it for registry escalation.
                process = exc.process
                raise

            async with self._lock:
                assert process is not None and thread_id is not None
                owner = self._thread_owners.get(thread_id)
                if owner is not None and owner != home:
                    raise CodexThreadHomeMismatchError(
                        f"Codex thread {thread_id} is already owned by {owner}, not {home}"
                    )
                self._decrement_starting_locked(home)
                starting_released = True
                self._thread_owners[thread_id] = home
                admitted = True
            return process, thread_id
        except BaseException as exc:
            if isinstance(
                exc,
                (CodexThreadNotIdleError, CodexThreadTerminalStateError),
            ):
                # thread start/resume already resolved this exact native
                # thread in the selected home. Preserve the route even though
                # no new CCM process was admitted; dropping it would let a
                # later caller attempt the same rollout from another account.
                async with self._lock:
                    owner = self._thread_owners.get(exc.thread_id)
                    if owner is None:
                        self._thread_owners[exc.thread_id] = home
                    elif owner != home:
                        self._draining.add(home)
                        raise CodexThreadHomeMismatchError(
                            f"Active Codex thread {exc.thread_id} is bound to "
                            f"{owner}, not {home}"
                        ) from exc
                if resume_session_id == exc.thread_id:
                    reserved_owner = False
            if getattr(server, "shutdown_requested", False):
                async with self._lock:
                    if self._servers.get(home) is server:
                        self._draining.add(home)
            if process is not None and not admitted:
                async def _abort_cancelled_admission() -> None:
                    await self.abort_unclaimed_turn(
                        home,
                        process,
                        reason="Codex registry admission did not complete",
                    )

                cleanup = asyncio.create_task(_abort_cancelled_admission())
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        continue
                cleanup.result()
            raise
        finally:
            # Release counters/tokens even when abort or transport shutdown
            # fails. Such failures keep the home draining but must not poison
            # unrelated lifecycle accounting. Identity-check the thread token
            # so one request can never erase another generation's reservation.
            async def _release_start_reservations() -> None:
                async with self._lock:
                    if not starting_released:
                        self._decrement_starting_locked(home)
                    if (
                        resume_session_id
                        and self._starting_threads.get(resume_session_id) is start_token
                    ):
                        self._starting_threads.pop(resume_session_id, None)
                    if (
                        not admitted
                        and reserved_owner
                        and self._thread_owners.get(resume_session_id) == home
                    ):
                        self._thread_owners.pop(resume_session_id, None)

            cleanup = asyncio.create_task(_release_start_reservations())
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
            cleanup.result()

    def _decrement_starting_locked(self, home: str) -> None:
        """Release one start reservation while ``self._lock`` is held."""

        starting = self._starting.get(home, 0)
        if starting > 1:
            self._starting[home] = starting - 1
        else:
            self._starting.pop(home, None)

    async def read_thread(
        self,
        codex_home: str | os.PathLike[str] | None,
        thread_id: str,
    ) -> dict[str, Any]:
        """Read one idle native thread from its exact account home."""

        home = normalize_codex_home(codex_home)
        token = object()
        reserved_owner = False
        async with self._lock:
            if self._shutdown_requested or home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is unavailable: {home}"
                )
            owner = self._thread_owners.get(thread_id)
            if owner is not None and owner != home:
                raise CodexThreadHomeMismatchError(
                    f"Codex thread {thread_id} is bound to {owner}, not {home}"
                )
            if thread_id in self._starting_threads or thread_id in self._rebindings:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} already has an operation in flight"
                )
            server = self._servers.get(home)
            if server is None:
                server = self._new_server(home)
                self._servers[home] = server
            if server.has_active_thread(thread_id):
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} still has an active turn"
                )
            if owner is None:
                self._thread_owners[thread_id] = home
                reserved_owner = True
            self._starting_threads[thread_id] = token
            self._starting[home] = self._starting.get(home, 0) + 1

        succeeded = False
        try:
            result = await server.read_thread(thread_id)
            succeeded = True
            return result
        finally:
            async def _release_read_thread() -> None:
                async with self._lock:
                    self._decrement_starting_locked(home)
                    if self._starting_threads.get(thread_id) is token:
                        self._starting_threads.pop(thread_id, None)
                    if (
                        reserved_owner
                        and not succeeded
                        and self._thread_owners.get(thread_id) == home
                    ):
                        self._thread_owners.pop(thread_id, None)

            await _settle_registry_cleanup(_release_read_thread())

    @asynccontextmanager
    async def thread_routing_guard(
        self,
        codex_home: str | os.PathLike[str] | None,
        thread_id: str,
    ):
        """Hold an exact native-thread idle proof across a caller DB commit.

        The per-thread reservation uses the same registry fence as
        ``start_turn``.  Once the quiescence RPCs succeed, no CCM turn can
        resume this thread until the caller leaves the context (normally after
        committing the Task routing tuple or Worker stage marker).
        """

        if not thread_id:
            raise ValueError("thread_id is required")
        home = normalize_codex_home(codex_home)
        token = object()
        reserved_owner = False
        async with self._lock:
            if self._shutdown_requested or home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is unavailable: {home}"
                )
            owner = self._thread_owners.get(thread_id)
            if owner is not None and owner != home:
                raise CodexThreadHomeMismatchError(
                    f"Codex thread {thread_id} is bound to {owner}, not {home}"
                )
            if thread_id in self._starting_threads or thread_id in self._rebindings:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} already has an operation in flight"
                )
            server = self._servers.get(home)
            if server is None:
                server = self._new_server(home)
                self._servers[home] = server
            if server.has_active_thread(thread_id):
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} still has an active CCM turn"
                )
            if owner is None:
                self._thread_owners[thread_id] = home
                reserved_owner = True
            self._starting_threads[thread_id] = token
            self._starting[home] = self._starting.get(home, 0) + 1

        validated = False
        try:
            snapshot = await server.require_thread_routing_quiescence(thread_id)
            validated = True
            yield snapshot
        finally:
            async def _release_routing_guard() -> None:
                async with self._lock:
                    self._decrement_starting_locked(home)
                    if self._starting_threads.get(thread_id) is token:
                        self._starting_threads.pop(thread_id, None)
                    if (
                        reserved_owner
                        and not validated
                        and self._thread_owners.get(thread_id) == home
                    ):
                        self._thread_owners.pop(thread_id, None)

            await _settle_registry_cleanup(_release_routing_guard())

    async def create_thread(
        self,
        codex_home: str | os.PathLike[str] | None,
        *,
        cwd: str,
        model: str | None = None,
        disable_project_config: bool = False,
    ) -> dict[str, Any]:
        """Create an empty thread in one exact account home."""

        home = normalize_codex_home(codex_home)
        async with self._lock:
            if self._shutdown_requested or home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is unavailable: {home}"
                )
            server = self._servers.get(home)
            if server is None:
                server = self._new_server(home)
                self._servers[home] = server
            self._starting[home] = self._starting.get(home, 0) + 1

        thread_id: str | None = None
        try:
            result = await server.create_thread(
                cwd=cwd,
                model=model,
                disable_project_config=disable_project_config,
            )
            thread_id = str(result["id"])
            async with self._lock:
                existing = self._thread_owners.get(thread_id)
                if existing is not None and existing != home:
                    raise CodexThreadHomeMismatchError(
                        f"New Codex thread {thread_id} is already bound to {existing}"
                    )
                self._thread_owners[thread_id] = home
            return result
        finally:
            async def _release_create_thread() -> None:
                async with self._lock:
                    self._decrement_starting_locked(home)

            await _settle_registry_cleanup(_release_create_thread())

    async def fork_thread(
        self,
        codex_home: str | os.PathLike[str] | None,
        thread_id: str,
        *,
        last_turn_id: str,
    ) -> dict[str, Any]:
        """Fork an idle native thread and register the new thread owner."""

        home = normalize_codex_home(codex_home)
        token = object()
        reserved_owner = False
        async with self._lock:
            if self._shutdown_requested or home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is unavailable: {home}"
                )
            owner = self._thread_owners.get(thread_id)
            if owner is not None and owner != home:
                raise CodexThreadHomeMismatchError(
                    f"Codex thread {thread_id} is bound to {owner}, not {home}"
                )
            if thread_id in self._starting_threads or thread_id in self._rebindings:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} already has an operation in flight"
                )
            server = self._servers.get(home)
            if server is None:
                server = self._new_server(home)
                self._servers[home] = server
            if server.has_active_thread(thread_id):
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} still has an active turn"
                )
            if owner is None:
                self._thread_owners[thread_id] = home
                reserved_owner = True
            self._starting_threads[thread_id] = token
            self._starting[home] = self._starting.get(home, 0) + 1

        fork_id: str | None = None
        try:
            result = await server.fork_thread(
                thread_id,
                last_turn_id=last_turn_id,
            )
            fork_id = str(result["id"])
            async with self._lock:
                existing = self._thread_owners.get(fork_id)
                if existing is not None and existing != home:
                    raise CodexThreadHomeMismatchError(
                        f"Forked Codex thread {fork_id} is already bound to {existing}"
                    )
                self._thread_owners[fork_id] = home
            return result
        finally:
            async def _release_fork_thread() -> None:
                async with self._lock:
                    self._decrement_starting_locked(home)
                    if self._starting_threads.get(thread_id) is token:
                        self._starting_threads.pop(thread_id, None)
                    if (
                        reserved_owner
                        and fork_id is None
                        and self._thread_owners.get(thread_id) == home
                    ):
                        self._thread_owners.pop(thread_id, None)

            await _settle_registry_cleanup(_release_fork_thread())

    async def delete_thread(
        self,
        codex_home: str | os.PathLike[str] | None,
        thread_id: str,
    ) -> None:
        """Delete one terminal thread without disturbing its shared transport."""

        if not thread_id:
            raise ValueError("thread_id is required")
        home = normalize_codex_home(codex_home)
        token = object()

        async with self._lock:
            if self._shutdown_requested:
                raise CodexAppServerBusyError(
                    "Codex app-server registry is shutting down"
                )
            if home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is draining: {home}"
                )
            owner = self._thread_owners.get(thread_id)
            if owner is not None and owner != home:
                raise CodexThreadHomeMismatchError(
                    f"Codex thread {thread_id} is bound to {owner}, not {home}"
                )
            if thread_id in self._rebindings:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} is being rebound"
                )
            if thread_id in self._starting_threads:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} already has a request in flight"
                )
            server = self._servers.get(home)
            if server is None:
                server = self._new_server(home)
                self._servers[home] = server
            self._starting_threads[thread_id] = token
            self._starting[home] = self._starting.get(home, 0) + 1

        deleted = False
        try:
            await server.delete_thread(thread_id)
            deleted = True
        finally:
            async def _release_delete_thread() -> None:
                async with self._lock:
                    self._decrement_starting_locked(home)
                    if self._starting_threads.get(thread_id) is token:
                        self._starting_threads.pop(thread_id, None)
                    if deleted and self._thread_owners.get(thread_id) == home:
                        self._thread_owners.pop(thread_id, None)

            await _settle_registry_cleanup(_release_delete_thread())

    async def unsubscribe_thread(self, thread_id: str) -> str:
        """Release one idle subscription without dropping its resumable owner."""

        if not thread_id:
            raise ValueError("thread_id is required")
        token = object()

        async with self._lock:
            owner = self._thread_owners.get(thread_id)
            if owner is None:
                raise CodexAppServerError(
                    f"Codex thread {thread_id} has no registered owner"
                )
            if self._shutdown_requested:
                raise CodexAppServerBusyError(
                    "Codex app-server registry is shutting down"
                )
            if owner in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is draining: {owner}"
                )
            if thread_id in self._rebindings:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} is being rebound"
                )
            if thread_id in self._starting_threads:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} already has a request in flight"
                )
            server = self._servers.get(owner)
            if server is None:
                raise CodexAppServerError(
                    f"Codex app-server is unavailable for thread {thread_id}"
                )
            self._starting_threads[thread_id] = token
            self._starting[owner] = self._starting.get(owner, 0) + 1

        try:
            return await server.unsubscribe_thread(thread_id)
        finally:
            async def _release_unsubscribe_thread() -> None:
                async with self._lock:
                    self._decrement_starting_locked(owner)
                    if self._starting_threads.get(thread_id) is token:
                        self._starting_threads.pop(thread_id, None)

            await _settle_registry_cleanup(_release_unsubscribe_thread())

    async def recycle_thread_runtime(
        self,
        codex_home: str | os.PathLike[str] | None,
        thread_id: str,
    ) -> None:
        """Reload one exact idle thread while retaining its home ownership."""

        if not thread_id:
            raise ValueError("thread_id is required")
        home = normalize_codex_home(codex_home)
        token = object()

        async with self._lock:
            if self._shutdown_requested:
                raise CodexAppServerBusyError(
                    "Codex app-server registry is shutting down"
                )
            if home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is draining: {home}"
                )
            owner = self._thread_owners.get(thread_id)
            if owner is not None and owner != home:
                raise CodexThreadHomeMismatchError(
                    f"Codex thread {thread_id} is bound to {owner}, not {home}"
                )
            if thread_id in self._rebindings:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} is being rebound"
                )
            if thread_id in self._starting_threads:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} already has a request in flight"
                )
            server = self._servers.get(home)
            if server is None:
                server = self._new_server(home)
                self._servers[home] = server
            if server.has_active_thread(thread_id):
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} still has an active turn"
                )
            # Preserve this exact route even if archive succeeds but unarchive
            # fails; terminal cleanup still needs the authoritative home.
            if owner is None:
                self._thread_owners[thread_id] = home
            self._starting_threads[thread_id] = token
            self._starting[home] = self._starting.get(home, 0) + 1

        try:
            await server.recycle_thread_runtime(thread_id)
        finally:
            async def _release_recycle_thread() -> None:
                async with self._lock:
                    self._decrement_starting_locked(home)
                    if self._starting_threads.get(thread_id) is token:
                        self._starting_threads.pop(thread_id, None)

            await _settle_registry_cleanup(_release_recycle_thread())

    async def _abort_lock_for_home(self, home: str) -> asyncio.Lock:
        async with self._lock:
            lock = self._abort_locks.get(home)
            if lock is None:
                lock = asyncio.Lock()
                self._abort_locks[home] = lock
            return lock

    async def stop_claimed_turn(
        self,
        codex_home: str | os.PathLike[str],
        process: CodexTurnProcess,
        *,
        reason: str,
    ) -> bool:
        """Stop one durably-owned turn and recycle its account transport.

        Exact thread/turn interruption is always attempted first.  It is not,
        however, sufficient proof that task-scoped native helpers are gone:
        Codex keeps MCP servers and code-mode hosts below the persistent
        app-server even after a turn reports ``interrupted``.  Once admission
        is drained, therefore recycle the account transport for every explicit
        stop.  Non-target adapters fail and use the ordinary task retry path;
        returning success while target-owned native helpers remain alive is
        not an acceptable outcome.

        Returns whether transport shutdown was required.
        """

        home = normalize_codex_home(codex_home)
        abort_lock = await self._abort_lock_for_home(home)
        async with abort_lock:
            server: CodexAppServer | None = None
            drain_owned = False
            shutdown_attempted = False
            try:
                async with self._lock:
                    if self._shutdown_requested:
                        raise CodexSharedTransportBusyError(
                            "Codex app-server registry is shutting down"
                        )
                    if home in self._draining:
                        raise CodexSharedTransportBusyError(
                            "Cannot isolate a claimed turn while its shared "
                            f"Codex app-server transport is draining: {home}"
                        )
                    server = self._servers.get(home)
                    if (
                        server is None
                        or not server.owns_live_turn_process(process)
                    ):
                        raise CodexSharedTransportBusyError(
                            "Cannot isolate a claimed turn because its exact "
                            "Codex app-server transport generation is no "
                            f"longer registered: {home}"
                        )
                    starting = self._starting.get(home, 0)
                    if starting:
                        raise CodexSharedTransportBusyError(
                            "Cannot stop the claimed turn while "
                            f"{starting} admitted app-server request(s) are "
                            f"in flight on its shared transport: {home}"
                        )
                    # Close admission before the interrupt RPC.  A request
                    # admitted earlier remains visible in ``_starting`` and
                    # blocks transport-level escalation below.
                    self._draining.add(home)
                    drain_owned = True

                interrupt_confirmed = False
                try:
                    interrupt_confirmed = await server.abandon_turn(
                        process,
                        reason,
                    )
                except BaseException:
                    logger.exception(
                        "Failed to interrupt claimed Codex turn: %s",
                        home,
                    )
                async with self._lock:
                    server_is_current = self._servers.get(home) is server
                    target_is_current = server.owns_live_turn_process(process)
                    has_peer_turns = server.has_other_live_turn_processes(
                        process
                    )
                    starting = self._starting.get(home, 0)
                    registry_shutdown = self._shutdown_requested

                blockers: list[str] = []
                if not server_is_current:
                    blockers.append("the exact transport generation changed")
                if (
                    not interrupt_confirmed
                    and process.returncode is None
                    and not target_is_current
                ):
                    blockers.append("the exact target generation changed")
                if starting:
                    blockers.append(
                        f"{starting} admitted app-server request(s) are in flight"
                    )
                if registry_shutdown:
                    blockers.append("the registry is shutting down")
                if blockers:
                    raise CodexSharedTransportBusyError(
                        "Cannot stop the claimed turn without disrupting its "
                        "shared Codex app-server transport: "
                        + "; ".join(blockers)
                    )

                if has_peer_turns:
                    logger.warning(
                        "Recycling shared Codex app-server transport after "
                        "an explicit turn interrupt; peer turns "
                        "will fail and retry: %s",
                        home,
                    )

                # Admission is drained.  Transport recycle is the only
                # available lifecycle boundary that also closes task-scoped
                # MCP/code-mode helpers retained by Codex after a confirmed
                # turn interrupt.
                shutdown_attempted = True
                await server.shutdown(
                    interrupted_process=process,
                    reason=reason,
                )

                # Real servers finish all adapters in their reader. Preserve
                # the same guarantee for test doubles and an already-settled
                # reader cancellation race. Peers are failures, never false
                # successful completions.
                for context in list(server._contexts_by_thread.values()):
                    if context.process is process:
                        context.process.finish(
                            130,
                            reason,
                            termination_kind="internal_abort",
                        )
                    else:
                        context.process.finish(
                            1,
                            "Codex app-server recycled after another turn's "
                            "explicit interrupt could not be confirmed",
                        )
                    server._detach_turn_context(context)
                process.finish(
                    130,
                    reason,
                    termination_kind="internal_abort",
                )

                async with self._lock:
                    if self._servers.get(home) is server:
                        self._servers.pop(home, None)
                    for thread_id, owner in list(self._thread_owners.items()):
                        if owner == home:
                            self._thread_owners.pop(thread_id, None)
                    self._draining.discard(home)
                    drain_owned = False
                return True
            finally:
                if (
                    drain_owned
                    and not shutdown_attempted
                    and server is not None
                ):
                    # A shared-transport conflict leaves the original consumer
                    # authoritative, so the account may reopen after the stop
                    # fails.  A transport shutdown failure, by contrast, keeps
                    # the home draining because its process state is unknown.
                    async with self._lock:
                        if (
                            not self._shutdown_requested
                            and self._servers.get(home) is server
                        ):
                            self._draining.discard(home)

    async def abort_unclaimed_turn(
        self,
        codex_home: str | os.PathLike[str],
        process: CodexTurnProcess,
        *,
        reason: str,
    ) -> bool:
        """Ensure a successfully-started turn cannot outlive its cancelled caller."""

        home = normalize_codex_home(codex_home)
        abort_lock = await self._abort_lock_for_home(home)
        async with abort_lock:
            return await self._abort_unclaimed_turn_locked(
                home,
                process,
                reason=reason,
            )

    async def _abort_unclaimed_turn_locked(
        self,
        home: str,
        process: CodexTurnProcess,
        *,
        reason: str,
    ) -> bool:
        """Fail-closed cleanup while the per-home abort lock is held."""

        async with self._lock:
            server = self._servers.get(home)
            if server is not None:
                # No new turn may enter while the interrupt outcome is unknown.
                # Otherwise a required transport shutdown could strand or kill
                # a second, newly admitted turn.
                self._draining.add(home)
        if server is None:
            process.finish(
                130,
                reason,
                termination_kind="internal_abort",
            )
            return True

        abandon = getattr(server, "abandon_turn", None)
        interrupt_confirmed = False
        try:
            if abandon is not None:
                interrupt_confirmed = await abandon(process, reason)
            else:
                process.terminate()
                process.finish(
                    130,
                    reason,
                    termination_kind="internal_abort",
                )
        except BaseException:
            logger.exception(
                "Failed to interrupt unclaimed Codex turn before transport shutdown: %s",
                home,
            )
        if interrupt_confirmed:
            async def _reopen_interrupted_home() -> None:
                async with self._lock:
                    self._draining.discard(home)

            await _settle_registry_cleanup(_reopen_interrupted_home())
            return False

        # If the interrupt was not acknowledged, stopping this account's
        # transport is the only way to rule out real model work continuing
        # without an InstanceManager consumer. A shutdown failure deliberately
        # leaves the account draining (fail-closed).
        shutdown_completed = False
        try:
            await server.shutdown(
                interrupted_process=process,
                reason=reason,
            )
            shutdown_completed = True
        except BaseException:
            logger.exception(
                "Failed to shut down Codex transport after unclaimed turn: %s",
                home,
            )
            raise
        finally:
            if shutdown_completed:
                # Transport termination is authoritative even if a test
                # double, or a reader cancellation race, did not deliver its
                # normal EOF cleanup.  Only now is it safe to detach/finish
                # the abandoned adapter.
                for context in list(server._contexts_by_thread.values()):
                    if context.process is process:
                        server._detach_turn_context(context)
                process.finish(
                    130,
                    reason,
                    termination_kind="internal_abort",
                )

                async def _detach_shutdown_home() -> None:
                    async with self._lock:
                        if self._servers.get(home) is server:
                            self._servers.pop(home, None)
                        for owned_thread, owner in list(
                            self._thread_owners.items()
                        ):
                            if owner == home:
                                self._thread_owners.pop(owned_thread, None)
                        self._draining.discard(home)

                await _settle_registry_cleanup(_detach_shutdown_home())
        return True

    async def steer_turn(
        self,
        thread_id: str,
        content: str,
        *,
        input_items: list[dict[str, Any]] | None = None,
    ) -> bool:
        home: str | None = None
        async with self._lock:
            if self._shutdown_requested:
                raise CodexAppServerBusyError(
                    "Codex app-server registry is shutting down"
                )
            home = self._thread_owners.get(thread_id)
            if home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is draining: {home}"
                )
            if thread_id in self._rebindings:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} is being rebound"
                )
            server = self._servers.get(home) if home else None
            if server is not None and home is not None:
                # A steer can start more native work on the exact turn.  Make
                # it an admitted home operation so claimed stop/maintenance
                # cannot define a terminal boundary across an in-flight RPC.
                self._starting[home] = self._starting.get(home, 0) + 1
        if server is None:
            return False
        try:
            if input_items is None:
                return await server.steer_turn(thread_id, content)
            return await server.steer_turn(
                thread_id,
                content,
                input_items=input_items,
            )
        finally:
            assert home is not None

            async def _release_steer_reservation() -> None:
                async with self._lock:
                    self._decrement_starting_locked(home)

            await _settle_registry_cleanup(_release_steer_reservation())

    async def read_rate_limits(
        self, codex_home: str | os.PathLike[str] | None,
    ) -> dict[str, Any]:
        """Read one account's quota without crossing CODEX_HOME boundaries."""

        home = normalize_codex_home(codex_home)
        async with self._lock:
            if self._shutdown_requested:
                raise CodexAppServerBusyError(
                    "Codex app-server registry is shutting down"
                )
            if home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is draining: {home}"
                )
            server = self._servers.get(home)
            if server is None:
                server = self._new_server(home)
                self._servers[home] = server
            self._starting[home] = self._starting.get(home, 0) + 1

        try:
            return await server.read_rate_limits()
        except BaseException:
            if getattr(server, "shutdown_requested", False):
                async with self._lock:
                    if self._servers.get(home) is server:
                        self._draining.add(home)
            raise
        finally:
            # Cancellation must not leak the home reservation and make future
            # relogin/delete operations report a permanent busy state.
            async def _release_read_reservation() -> None:
                async with self._lock:
                    self._decrement_starting_locked(home)

            cleanup = asyncio.create_task(_release_read_reservation())
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
            cleanup.result()

    async def rebind_thread(
        self,
        thread_id: str,
        *,
        source_codex_home: str | os.PathLike[str] | None,
        target_codex_home: str | os.PathLike[str],
    ) -> None:
        """Move registry ownership after the rollout was safely copied.

        App-server keeps completed threads in memory.  If the target server has
        loaded this thread before (the B -> A leg of a round trip), an idle
        target process is restarted so its next ``thread/resume`` reads the
        newly copied rollout.  Active target turns are never killed; callers
        receive a retryable busy error instead.
        """

        source = normalize_codex_home(source_codex_home)
        target = normalize_codex_home(target_codex_home)
        if source == target:
            async with self._lock:
                self._thread_owners[thread_id] = target
            return

        restart_server: CodexAppServer | None = None
        async with self._lock:
            if thread_id in self._rebindings:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} is already being rebound"
                )
            owner = self._thread_owners.get(thread_id)
            if owner is not None and owner != source:
                raise CodexThreadHomeMismatchError(
                    f"Codex thread {thread_id} is owned by {owner}, expected {source}"
                )
            source_server = self._servers.get(source)
            if self._starting.get(source, 0) > 0:
                raise CodexAppServerBusyError(
                    f"Codex source account has a start/resume request in flight: {source}"
                )
            if source_server and source_server.has_active_thread(thread_id):
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} still has an active turn in {source}"
                )
            if target in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex target account app-server is draining: {target}"
                )
            target_server = self._servers.get(target)
            if target_server and target_server.knows_thread(thread_id):
                if self._starting.get(target, 0) > 0:
                    raise CodexAppServerBusyError(
                        f"Codex target account has a start/resume request in flight "
                        f"and a stale cached copy of thread {thread_id}: {target}"
                    )
                if target_server.has_active_turns:
                    raise CodexAppServerBusyError(
                        f"Codex target account has active turns and a stale cached "
                        f"copy of thread {thread_id}: {target}"
                    )
                self._draining.add(target)
                restart_server = target_server
            self._rebindings[thread_id] = (source, target)

        try:
            if restart_server is not None:
                await restart_server.shutdown()
                async def _detach_restarted_target() -> None:
                    async with self._lock:
                        if self._servers.get(target) is restart_server:
                            self._servers.pop(target, None)
                        self._draining.discard(target)

                await _settle_registry_cleanup(
                    _detach_restarted_target()
                )

            async with self._lock:
                owner = self._thread_owners.get(thread_id)
                if owner is not None and owner != source:
                    raise CodexThreadHomeMismatchError(
                        f"Codex thread {thread_id} changed owner to {owner} "
                        f"while rebinding from {source}"
                    )
                self._thread_owners[thread_id] = target
        finally:
            async def _release_rebinding() -> None:
                async with self._lock:
                    self._rebindings.pop(thread_id, None)

            await _settle_registry_cleanup(_release_rebinding())

    async def clear_thread_owner_for_recovery(
        self,
        thread_id: str,
        *,
        expected_codex_home: str | os.PathLike[str],
    ) -> bool:
        """Drop an idle owner reservation after a failed migration rollback.

        Completed turn contexts are already removed by ``turn/completed``;
        only the registry's thread-to-home reservation can remain split from
        the durable task binding. Clearing that one mapping is safer than
        shutting down an account server that may be serving unrelated turns.
        The next resume then cold-routes from the DB-authoritative account.
        """

        expected = normalize_codex_home(expected_codex_home)
        async with self._lock:
            if thread_id in self._rebindings:
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} is being rebound"
                )
            owner = self._thread_owners.get(thread_id)
            if owner is None:
                return True
            if owner != expected:
                raise CodexThreadHomeMismatchError(
                    f"Codex thread {thread_id} is owned by {owner}, expected {expected}"
                )
            server = self._servers.get(owner)
            if server and server.has_active_thread(thread_id):
                raise CodexAppServerBusyError(
                    f"Codex thread {thread_id} still has an active turn in {owner}"
                )
            self._thread_owners.pop(thread_id, None)
            return True

    async def begin_home_maintenance(
        self,
        codex_home: str | os.PathLike[str],
        *,
        require_idle: bool = True,
    ) -> bool:
        """Reserve and stop one home until ``end_home_maintenance``.

        This is the relogin/delete primitive: once it returns, new turns for
        the home are rejected even though its old process has already stopped.
        The caller must release the reservation in a ``finally`` block.
        """

        home = normalize_codex_home(codex_home)
        async with self._lock:
            if home in self._draining:
                raise CodexAppServerBusyError(
                    f"Codex account app-server is already draining: {home}"
                )
            if any(home in pair for pair in self._rebindings.values()):
                raise CodexAppServerBusyError(
                    f"Codex account has a thread rebind in flight: {home}"
                )
            server = self._servers.get(home)
            if require_idle and (
                self._starting.get(home, 0) > 0
                or (server is not None and server.has_active_turns)
            ):
                raise CodexAppServerBusyError(
                    f"Codex account still has an active or starting turn: {home}"
                )
            self._draining.add(home)

        shutdown_completed = server is None
        try:
            if server is not None:
                await server.shutdown()
                shutdown_completed = True
            # Cancellation can land after shutdown has completed but while the
            # registry lock below is contended.  Keep that await inside the
            # reservation guard so a half-finished begin never strands the
            # home in ``_draining`` forever.
            async with self._lock:
                if server is not None and self._servers.get(home) is server:
                    self._servers.pop(home, None)
                for thread_id, owner in list(self._thread_owners.items()):
                    if owner == home:
                        self._thread_owners.pop(thread_id, None)
        except asyncio.CancelledError:
            if shutdown_completed:
                # The caller never observes a successful reservation and thus
                # cannot call end_home_maintenance.  Finish detaching the now
                # dead server before reopening the home; merely clearing
                # _draining would make start_turn reuse a closed transport.
                async def _detach_and_reopen() -> None:
                    async with self._lock:
                        if server is not None and self._servers.get(home) is server:
                            self._servers.pop(home, None)
                        for thread_id, owner in list(self._thread_owners.items()):
                            if owner == home:
                                self._thread_owners.pop(thread_id, None)
                        self._draining.discard(home)

                cleanup = asyncio.create_task(_detach_and_reopen())
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        # Repeated caller cancellation must not reopen the home
                        # before the closed server has been removed.
                        continue
                cleanup.result()
            # If shutdown itself was cancelled, retain _draining fail-closed;
            # the server may be only partially terminated and must not be reused.
            raise
        except BaseException:
            # A shutdown failure has unknown process state.  Keep the home
            # reserved rather than reopening it onto a possibly half-dead or
            # still-running transport; restart recovery clears in-memory state.
            raise
        return server is not None

    async def end_home_maintenance(
        self, codex_home: str | os.PathLike[str],
    ) -> None:
        """Release a reservation created by ``begin_home_maintenance``."""

        home = normalize_codex_home(codex_home)

        async def _release_home() -> None:
            async with self._lock:
                self._draining.discard(home)

        await _settle_registry_cleanup(_release_home())

    async def shutdown_home(
        self,
        codex_home: str | os.PathLike[str],
        *,
        require_idle: bool = True,
    ) -> bool:
        """One-shot idle shutdown; unlike maintenance it immediately reopens."""

        maintenance_started = False
        try:
            stopped = await self.begin_home_maintenance(
                codex_home, require_idle=require_idle,
            )
            maintenance_started = True
            return stopped
        finally:
            if maintenance_started:
                await self.end_home_maintenance(codex_home)

    async def shutdown(self) -> None:
        async with self._lock:
            self._shutdown_requested = True
            servers = list(self._servers.items())
            self._draining.update(self._servers)
        results = await asyncio.gather(
            *(server.shutdown() for _, server in servers),
            return_exceptions=True,
        )
        failures: list[tuple[str, BaseException]] = []
        successful_homes: set[str] = set()
        for (home, _), result in zip(servers, results):
            if isinstance(result, BaseException):
                failures.append((home, result))
                logger.error(
                    "Failed to stop Codex app-server home=%s: %s",
                    home,
                    result,
                )
            else:
                successful_homes.add(home)

        async with self._lock:
            for home, server in servers:
                if home not in successful_homes:
                    # Preserve both route and draining evidence for a process
                    # generation whose termination could not be proven.
                    if self._servers.get(home) is server:
                        self._draining.add(home)
                    continue
                if self._servers.get(home) is server:
                    self._servers.pop(home, None)
                for thread_id, owner in list(self._thread_owners.items()):
                    if owner == home:
                        self._thread_owners.pop(thread_id, None)
                self._starting.pop(home, None)
                self._draining.discard(home)

            # In the normal quiescent shutdown path every server in the
            # registry was part of this snapshot.  Clear the remaining
            # coordination-only state.  If another server appeared
            # concurrently, retain its evidence instead of orphaning it.
            if not failures and not self._servers:
                self._thread_owners.clear()
                self._rebindings.clear()
                self._starting.clear()
                self._starting_threads.clear()
                self._draining.clear()

        if failures:
            homes = ", ".join(home for home, _ in failures)
            raise CodexAppServerError(
                "Failed to stop Codex app-server transport(s); "
                f"left draining for: {homes}"
            ) from failures[0][1]
