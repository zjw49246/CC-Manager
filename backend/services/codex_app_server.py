"""Persistent Codex app-server transport.

The regular ``codex exec resume`` integration starts a new CLI process for
every turn.  App-server keeps configuration, MCP clients, and active threads in
one process while exposing the same persisted Codex thread ids.  This module
adapts one app-server turn to the small subprocess surface InstanceManager
already consumes, so task status/retry/DB logic remains shared with exec mode.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import os
import re
import secrets
import shutil
import signal
import stat
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
from backend.services.task_runtime_secrets import PrivateTaskTempDir

logger = logging.getLogger(__name__)

_APP_SERVER_GRACEFUL_SHUTDOWN_TIMEOUT = 5.0
_APP_SERVER_TERM_SHUTDOWN_TIMEOUT = 5.0
_APP_SERVER_KILL_SHUTDOWN_TIMEOUT = 5.0
_APP_SERVER_GROUP_POLL_INTERVAL = 0.05
# App-server speaks newline-delimited JSON and tool results can legitimately
# make one protocol frame much larger than asyncio's 64 KiB default.  Keep a
# bounded limit, but leave enough room for large MCP/command output frames so
# the reader (and therefore live turn injection) is not torn down mid-turn.
_APP_SERVER_STREAM_LIMIT = 256 * 1024 * 1024
_CODEX_LOG_DB_MAX_BYTES = 1024 * 1024 * 1024
_CODEX_LOG_DB_NAMES = (
    "logs_2.sqlite",
    "logs_2.sqlite-wal",
    "logs_2.sqlite-shm",
)
_CODEX_LOG_QUARANTINE_PREFIX = ".ccm-log-quarantine-"
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


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validate_owned_regular_file(path: Path) -> os.stat_result:
    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_nlink != 1
    ):
        raise CodexAppServerError(
            f"Unsafe Codex app-server log database entry: {path}"
        )
    return info


def _validated_log_quarantine(path: Path) -> bool:
    try:
        info = path.lstat()
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_mode & 0o077
        ):
            return False
        entries = list(path.iterdir())
        if not entries or any(entry.name not in _CODEX_LOG_DB_NAMES for entry in entries):
            return False
        for entry in entries:
            _validate_owned_regular_file(entry)
        return True
    except (FileNotFoundError, OSError, CodexAppServerError):
        return False


def _prepare_codex_log_db_rotation(codex_home: Path) -> tuple[Path, ...]:
    """Atomically isolate an oversized Codex diagnostics database.

    Native rollout/session state is deliberately outside this exact filename
    allowlist.  Old quarantines are returned only when their complete shape is
    still CCM-owned, so a later successful initialize can finish cleanup after
    a Manager crash between rename and unlink.
    """

    stale = tuple(
        entry
        for entry in codex_home.iterdir()
        if entry.name.startswith(_CODEX_LOG_QUARANTINE_PREFIX)
        and _validated_log_quarantine(entry)
    )
    database = codex_home / _CODEX_LOG_DB_NAMES[0]
    try:
        database_info = _validate_owned_regular_file(database)
    except FileNotFoundError:
        return stale
    if database_info.st_size <= _CODEX_LOG_DB_MAX_BYTES:
        return stale

    sources: list[Path] = []
    for name in _CODEX_LOG_DB_NAMES:
        source = codex_home / name
        try:
            _validate_owned_regular_file(source)
        except FileNotFoundError:
            continue
        sources.append(source)

    quarantine = codex_home / (
        _CODEX_LOG_QUARANTINE_PREFIX + secrets.token_hex(8)
    )
    quarantine.mkdir(mode=0o700)
    moved: list[tuple[Path, Path]] = []
    try:
        for source in sources:
            target = quarantine / source.name
            os.replace(source, target)
            moved.append((source, target))
        _fsync_directory(quarantine)
        _fsync_directory(codex_home)
    except BaseException:
        for source, target in reversed(moved):
            try:
                os.replace(target, source)
            except OSError:
                logger.exception(
                    "Could not restore Codex log database after rotation failure"
                )
        try:
            quarantine.rmdir()
        except OSError:
            pass
        raise
    logger.warning(
        "Quarantined oversized Codex app-server log database home=%s bytes=%s",
        codex_home,
        database_info.st_size,
    )
    return (*stale, quarantine)


def _finalize_codex_log_db_rotation(
    codex_home: Path,
    quarantines: Sequence[Path],
) -> None:
    """Delete only validated quarantines after a new app-server initialized."""

    for quarantine in quarantines:
        if quarantine.parent != codex_home or not _validated_log_quarantine(quarantine):
            logger.error(
                "Refusing unsafe Codex log quarantine cleanup path=%s",
                quarantine,
            )
            continue
        for name in _CODEX_LOG_DB_NAMES:
            entry = quarantine / name
            try:
                _validate_owned_regular_file(entry)
            except FileNotFoundError:
                continue
            entry.unlink()
        quarantine.rmdir()
        logger.info("Removed recovered Codex log quarantine home=%s", codex_home)
    _fsync_directory(codex_home)
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
# Codex 0.147 exposes no RPC that proves background plugin discovery settled;
# calling ``plugin/list`` can itself schedule more background work. Isolated
# preflight therefore never calls it. Two consecutive forced, canonical skill
# snapshots capture the current inventory without triggering plugin refresh.
# This is not a plugin-settled proof: safety still comes from disabling plugin
# features plus the existing thread/ownership/turn fingerprint and revision
# fences. Bound both reads and wall time so a moving inventory fails closed.
_ISOLATED_SKILLS_SNAPSHOT_MAX_READS = 8
_ISOLATED_SKILLS_SNAPSHOT_TIMEOUT = 10.0


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
_TOOL_FREE_PERMISSION_PROFILE_PREFIX = "ccm_pr_review_no_access_v1_"
_TASK_SSH_PERMISSION_PROFILE_PREFIX = "ccm_task_ssh_isolated_v1_"
_TASK_MANAGED_NETWORK_PERMISSION_PROFILE_PREFIX = (
    "ccm_task_managed_network_v1_"
)
_CODEX_APP_SERVER_USER_AGENT_RE = re.compile(
    r"\Aclaude_code_manager/([0-9]+)\.([0-9]+)\.([0-9]+) "
    r"\("
)
_MANAGED_NETWORK_MIN_CODEX_VERSION = (0, 146, 0)
_TASK_SHELL_ENV_EXCLUDES = (
    "AUTH_TOKEN",
    "CCM_*",
    "GIT_*",
    "GH_*",
    "GITHUB_*",
    "SSH_*",
    "ANTHROPIC_*",
    "CLAUDE_*",
    "OPENAI_*",
    "CODEX_*",
    "AWS_*",
    "GOOGLE_*",
)
_NETWORK_ISOLATED_PERMISSION_PROFILE_PREFIX = "ccm_delivery_workspace_v1_"
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
    "mentions_v2",
    "multi_agent",
    "network_proxy",
    "plugins",
    "plugin_sharing",
    "realtime_conversation",
    "remote_compaction_v2",
    "remote_control",
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
# Delivery turns still need the local shell and patch tools, but every native
# route that can add remote capabilities, background work, or a second model
# lineage must remain off.  In particular ``shell_snapshot`` is security
# relevant here: Codex snapshots a login shell before applying the per-turn
# environment policy, so replaying one could otherwise resurrect GH_TOKEN or
# an SSH agent that the Delivery profile deliberately did not inherit.
_NETWORK_ISOLATED_DISABLED_FEATURES = frozenset({
    "apps",
    "artifact",
    "auth_elicitation",
    "browser_use",
    "browser_use_external",
    "browser_use_full_cdp_access",
    "computer_use",
    "code_mode",
    "code_mode_host",
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
    "mentions_v2",
    "multi_agent",
    "network_proxy",
    "plugins",
    "plugin_sharing",
    "realtime_conversation",
    "remote_compaction_v2",
    "remote_control",
    "remote_plugin",
    "request_permissions_tool",
    "shell_snapshot",
    "skill_mcp_dependency_install",
    "standalone_web_search",
    "tool_call_mcp_elicitation",
    "tool_suggest",
    "workspace_dependencies",
})
_ISOLATED_LOCAL_FEATURES = frozenset({
    "apply_patch_freeform",
    "shell_tool",
    "unified_exec",
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


class _CodexIsolatedSkillsDriftError(CodexRequiredMcpPreTurnError):
    """An isolated empty thread was admitted from a stale skills inventory."""


class CodexThreadRuntimeRecycleError(CodexRequiredMcpError):
    """A thread runtime recycle may have changed native external state."""

    def __init__(self, thread_id: str, detail: str) -> None:
        super().__init__(
            f"Codex thread {thread_id} runtime recycle outcome is uncertain: "
            f"{detail}"
        )
        self.thread_id = thread_id
        self.route_mutation_possible = True


class CodexThreadRuntimeRecycleCancelled(asyncio.CancelledError):
    """Cancellation observed after a runtime recycle mutation was attempted."""

    def __init__(self, thread_id: str) -> None:
        super().__init__(
            f"Codex thread {thread_id} runtime recycle was cancelled after "
            "native state mutation began"
        )
        self.thread_id = thread_id
        self.route_mutation_possible = True


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


def _parse_codex_app_server_version(value: Any) -> tuple[int, int, int] | None:
    """Parse the exact app-server generation's canonical initialize proof."""

    if not isinstance(value, str):
        return None
    match = _CODEX_APP_SERVER_USER_AGENT_RE.match(value)
    if match is None:
        return None
    return tuple(int(part) for part in match.groups())


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


def _audit_tool_free_thread_response(
    response: Any,
    *,
    permission_profile_id: str,
) -> None:
    """Prove the deny-all runtime selected before sending model input."""

    if not isinstance(response, dict):
        raise ValueError("thread response is not an object")
    permission_profile = response.get("activePermissionProfile")
    if (
        not isinstance(permission_profile, dict)
        or permission_profile.get("id") != permission_profile_id
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


def _tool_free_permission_config() -> dict[str, Any]:
    return {
        "filesystem": {"/": "deny"},
        "network": {
            "enabled": False,
            "allow_local_binding": False,
        },
    }


def _nearest_concrete_parent_permission(
    filesystem: dict[str, str],
    path: str,
) -> str | None:
    """Return the longest matching concrete parent permission for ``path``."""

    nearest_permission: str | None = None
    nearest_depth = -1
    for boundary, permission in filesystem.items():
        if not os.path.isabs(boundary) or boundary == path:
            continue
        try:
            if os.path.commonpath((path, boundary)) != boundary:
                continue
        except ValueError:
            # Different drives are never ancestors of one another.
            continue
        depth = len(Path(boundary).parts)
        if depth > nearest_depth:
            nearest_permission = permission
            nearest_depth = depth
    return nearest_permission


def _codex_runtime_read_paths(
    binary: str,
    *,
    projection_root: str | os.PathLike[str] | None = None,
) -> tuple[str, ...]:
    """Materialize the allow-listed Codex runtime outside protected homes."""

    expanded = os.path.expanduser(str(binary))
    has_separator = any(
        separator and separator in expanded
        for separator in (os.sep, os.altsep)
    )
    candidate = expanded if has_separator else shutil.which(expanded)
    if not candidate:
        return ()
    try:
        resolved = Path(candidate).resolve(strict=True)
        source_info = resolved.stat()
    except (OSError, RuntimeError):
        return ()
    if (
        not stat.S_ISREG(source_info.st_mode)
        or not os.access(resolved, os.X_OK)
    ):
        return ()
    effective_uid = (
        os.geteuid()
        if hasattr(os, "geteuid")
        else source_info.st_uid
    )

    requested_root = (
        Path(projection_root)
        if projection_root is not None
        else Path.home() / ".cache" / "claude-code-manager" / "codex-runtime"
    )
    root = Path(os.path.abspath(os.path.expanduser(os.fspath(requested_root))))
    executable_suffix = (
        ".exe" if resolved.name.lower().endswith(".exe") else ""
    )
    components: list[tuple[Path, os.stat_result, str]] = [
        (resolved, source_info, f"codex{executable_suffix}"),
    ]
    code_mode_host = resolved.with_name(
        f"codex-code-mode-host{executable_suffix}"
    )
    try:
        code_mode_host_info = code_mode_host.lstat()
    except FileNotFoundError:
        # Older Codex releases execute shell commands in-process and do not
        # ship the code-mode host. Preserve their single-file projection.
        pass
    except OSError:
        return ()
    else:
        if (
            stat.S_ISLNK(code_mode_host_info.st_mode)
            or not stat.S_ISREG(code_mode_host_info.st_mode)
            or code_mode_host_info.st_uid != source_info.st_uid
            or not os.access(code_mode_host, os.X_OK)
        ):
            # A present but untrusted companion must fail closed. Otherwise
            # the projected Codex binary could execute a substituted sibling.
            return ()
        components.append(
            (
                code_mode_host,
                code_mode_host_info,
                f"codex-code-mode-host{executable_suffix}",
            )
        )

    identity_material = "\0".join(
        "\0".join(
            (
                str(source),
                str(info.st_dev),
                str(info.st_ino),
                str(info.st_mode),
                str(info.st_size),
                str(info.st_mtime_ns),
            )
        )
        for source, info, _projection_name in components
    )
    identity = hashlib.sha256(
        identity_material.encode("utf-8")
    ).hexdigest()[:32]
    release_dir = root / identity
    projections = tuple(
        (source, info, release_dir / projection_name)
        for source, info, projection_name in components
    )
    try:
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        release_dir.mkdir(mode=0o700, exist_ok=True)
        if root.resolve(strict=True) != root:
            return ()
        for directory in (root, release_dir):
            directory_info = directory.lstat()
            if (
                stat.S_ISLNK(directory_info.st_mode)
                or not stat.S_ISDIR(directory_info.st_mode)
                or directory_info.st_uid != effective_uid
            ):
                return ()
            directory.chmod(0o700)
        projected_infos: list[os.stat_result] = []
        for source, _source_info, projection in projections:
            if not projection.exists():
                temporary = release_dir / (
                    f".{projection.name}-{secrets.token_hex(16)}"
                )
                try:
                    os.link(source, temporary)
                    os.replace(temporary, projection)
                finally:
                    temporary.unlink(missing_ok=True)
            projected_infos.append(projection.lstat())
    except OSError:
        return ()
    for (
        (source, _source_info, projection),
        projected_info,
    ) in zip(projections, projected_infos, strict=True):
        if (
            stat.S_ISLNK(projected_info.st_mode)
            or not stat.S_ISREG(projected_info.st_mode)
            or projected_info.st_uid != effective_uid
            or not os.access(projection, os.X_OK)
            or not os.path.samefile(source, projection)
        ):
            return ()
    return tuple(str(projection) for _, _, projection in projections)


def _task_ssh_permission_config(
    *,
    cwd: str,
    protected_paths: Sequence[str],
    allowed_read_paths: Sequence[str],
    git_read_paths: Sequence[str],
    private_tmpdir: str,
    disable_network: bool,
    managed_network_proxy: bool,
    sandbox_mode: str,
    runtime_read_paths: Sequence[str] = (),
) -> dict[str, Any]:
    """Build a request-local Codex profile that hides host credentials."""

    # ``network.enabled=true`` also enables AF_UNIX on Linux; Codex 0.144.6's
    # network unix-socket deny is not enforced there. A root-readable profile
    # would therefore let an ordinary networked Task rediscover Manager or
    # provider sockets. Default-deny the host filesystem, admit only Codex's
    # fixed executable/runtime roots, then grant the exact workspace and any
    # explicitly authorized Git credential files.
    filesystem: dict[str, str] = {
        ":root": "deny",
        ":minimal": "read",
    }
    workspace = os.path.abspath(cwd)
    if sandbox_mode == "workspace-write":
        filesystem[workspace] = "write"
    elif sandbox_mode == "read-only":
        filesystem[workspace] = "read"
    else:
        raise ValueError("Task isolation requires a sandboxed Codex mode")
    # A normal checkout keeps config, hooks, credentials, refs and objects in
    # this writable directory. Linked worktrees use a regular pointer file;
    # either way default-deny the entry and re-open only the discovery-proven
    # read paths below.
    filesystem[os.path.join(workspace, ".git")] = "deny"
    normalized_protected_paths = sorted(
        {
            os.path.abspath(os.path.expanduser(str(value)))
            for value in protected_paths
        },
        key=lambda path: (len(Path(path).parts), path),
    )
    for path in normalized_protected_paths:
        if path == "/":
            raise ValueError("Task SSH protected path cannot be filesystem root")
        # Codex materializes every concrete permission boundary as a sandbox
        # mount. A child deny below an already-denied parent is redundant and
        # can fail before exec when Bubblewrap tries to create its mountpoint
        # inside the read-only parent. Keep the child when a nearer read/write
        # boundary reopened the tree (for example, a secret inside workspace).
        if _nearest_concrete_parent_permission(filesystem, path) == "deny":
            continue
        filesystem[path] = "deny"
    for value in allowed_read_paths:
        path = os.path.abspath(os.path.expanduser(str(value)))
        if path == "/" or path in filesystem and filesystem[path] == "deny":
            raise ValueError(
                "Task Git credential read override conflicts with a deny path"
            )
        # Codex permission profiles use longest-path matching. Keeping the
        # denied parent alongside this exact file read entry prevents ambient
        # siblings or later-created credentials from becoming visible.
        filesystem[path] = "read"
    for value in runtime_read_paths:
        path = os.path.abspath(os.path.expanduser(str(value)))
        if (
            path == "/"
            or not os.path.isfile(path)
            or not os.access(path, os.X_OK)
        ):
            raise ValueError("Codex runtime projection is not an executable file")
        # Codex 0.147 re-execs its canonical standalone binary for shell
        # commands. The binary commonly lives below a protected CODEX_HOME,
        # so reopen only this exact file after installing the parent deny.
        filesystem[path] = "read"
    for value in git_read_paths:
        path = os.path.abspath(os.path.expanduser(str(value)))
        if path == "/":
            raise ValueError("Task linked Git read path cannot be filesystem root")
        filesystem[path] = "read"
    scratch = os.path.abspath(os.path.expanduser(str(private_tmpdir)))
    if scratch == "/" or scratch == workspace:
        raise ValueError("Task scratch write path must be an exact external leaf")
    filesystem[scratch] = "write"
    if disable_network == managed_network_proxy:
        raise ValueError(
            "Task isolation must select exactly one network boundary"
        )
    network: dict[str, Any]
    if managed_network_proxy:
        # Codex's managed proxy gives the sandbox public egress without
        # exposing host loopback, AF_UNIX, UDP, SOCKS, or deployment-level
        # upstream proxies. Keep every option explicit because omitted
        # NetworkToml fields inherit permissive runtime defaults.
        network = {
            "enabled": True,
            "enable_socks5": False,
            "enable_socks5_udp": False,
            "allow_upstream_proxy": False,
            "dangerously_allow_non_loopback_proxy": False,
            "dangerously_allow_all_unix_sockets": False,
            "mode": "full",
            "domains": {"*": "allow"},
            "unix_sockets": {},
            "allow_local_binding": False,
        }
    else:
        network = {
            "enabled": False,
            "allow_local_binding": False,
        }
    return {
        "filesystem": filesystem,
        "network": network,
    }


def _audit_task_isolation_permission_config(
    profile: Any,
    *,
    cwd: str,
    protected_paths: Sequence[str],
    allowed_read_paths: Sequence[str],
    git_read_paths: Sequence[str],
    private_tmpdir: str,
    disable_network: bool,
    managed_network_proxy: bool,
    sandbox_mode: str,
    runtime_read_paths: Sequence[str] = (),
) -> None:
    """Reject any widening of the request-local Task filesystem profile."""

    expected = _task_ssh_permission_config(
        cwd=cwd,
        protected_paths=protected_paths,
        allowed_read_paths=allowed_read_paths,
        git_read_paths=git_read_paths,
        private_tmpdir=private_tmpdir,
        disable_network=disable_network,
        managed_network_proxy=managed_network_proxy,
        sandbox_mode=sandbox_mode,
        runtime_read_paths=runtime_read_paths,
    )
    if profile != expected:
        raise ValueError("Task isolation permission profile changed before admission")
    filesystem = expected["filesystem"]
    scratch = os.path.abspath(private_tmpdir)
    workspace = os.path.abspath(cwd)
    write_paths = {
        path for path, mode in filesystem.items() if mode == "write"
    }
    expected_writes = {scratch}
    if sandbox_mode == "workspace-write":
        expected_writes.add(workspace)
    if write_paths != expected_writes:
        raise ValueError("Task isolation contains an unexpected writable root")
    if any(
        filesystem.get(os.path.abspath(path)) != "read"
        for path in git_read_paths
    ):
        raise ValueError("Task linked Git projection is not read-only")


def _network_isolated_permission_config(
    *,
    cwd: str,
    git_read_paths: Sequence[str],
    private_tmpdir: str,
    runtime_read_paths: Sequence[str] = (),
) -> dict[str, Any]:
    workspace = os.path.abspath(cwd)
    scratch = os.path.abspath(private_tmpdir)
    filesystem: dict[str, str] = {
        ":root": "deny",
        ":minimal": "read",
        workspace: "write",
        os.path.join(workspace, ".git"): "deny",
    }
    for value in git_read_paths:
        path = os.path.abspath(os.path.expanduser(str(value)))
        if path == "/":
            raise ValueError("Delivery linked Git read path cannot be root")
        filesystem[path] = "read"
    for value in runtime_read_paths:
        path = os.path.abspath(os.path.expanduser(str(value)))
        if (
            path == "/"
            or not os.path.isfile(path)
            or not os.access(path, os.X_OK)
        ):
            raise ValueError("Codex runtime projection is not an executable file")
        filesystem[path] = "read"
    if scratch in {"/", workspace}:
        raise ValueError("Delivery scratch path must be an external leaf")
    filesystem[scratch] = "write"
    return {
        "filesystem": filesystem,
        "network": {
            "enabled": False,
            "allow_local_binding": False,
        },
    }


def _audit_network_isolated_permission_config(
    profile: Any,
    *,
    cwd: str,
    git_read_paths: Sequence[str],
    private_tmpdir: str,
    runtime_read_paths: Sequence[str] = (),
) -> None:
    expected = _network_isolated_permission_config(
        cwd=cwd,
        git_read_paths=git_read_paths,
        private_tmpdir=private_tmpdir,
        runtime_read_paths=runtime_read_paths,
    )
    if profile != expected:
        raise ValueError("Delivery filesystem profile changed before admission")
    filesystem = expected["filesystem"]
    if {
        path for path, mode in filesystem.items() if mode == "write"
    } != {os.path.abspath(cwd), os.path.abspath(private_tmpdir)}:
        raise ValueError("Delivery contains an unexpected writable root")
    if any(
        filesystem.get(os.path.abspath(path)) != "read"
        for path in git_read_paths
    ):
        raise ValueError("Delivery linked Git projection is not read-only")


def _audit_isolated_request_config(
    config: Any,
    *,
    expected_config: dict[str, Any],
    permission_profile_id: str,
    expected_permission_profile: dict[str, Any],
    disabled_features: frozenset[str],
    network_proxy_enabled: bool,
) -> None:
    """Prove the complete request-local isolation layer before thread/start."""

    if not isinstance(config, dict) or config != expected_config:
        raise ValueError("isolated request configuration changed before admission")
    if config.get("web_search") != "disabled":
        raise ValueError("isolated request did not disable web search")
    if config.get("allow_login_shell") is not False:
        raise ValueError("isolated request did not disable login shells")
    features = config.get("features")
    if not isinstance(features, dict):
        raise ValueError("isolated feature configuration is malformed")
    for feature in disabled_features - {"network_proxy"}:
        if features.get(feature) is not False:
            raise ValueError(f"isolated feature {feature!r} was not disabled")
    if features.get("network_proxy") is not network_proxy_enabled:
        raise ValueError("isolated network proxy decision changed")
    if config.get("default_permissions") != permission_profile_id:
        raise ValueError("isolated permission selector changed")
    if config.get("permissions") != {
        permission_profile_id: expected_permission_profile,
    }:
        raise ValueError("isolated permission table changed")
    if config.get("tools") != {
        "experimental_request_user_input": {"enabled": False},
    }:
        raise ValueError("isolated native tool configuration changed")
    skills = config.get("skills")
    if (
        not isinstance(skills, dict)
        or skills.get("include_instructions") is not False
        or skills.get("bundled") != {"enabled": False}
        or not isinstance(skills.get("config"), list)
    ):
        raise ValueError("isolated skills configuration changed")
    agents = config.get("agents")
    if agents != {"max_threads": 1, "max_depth": 1}:
        raise ValueError("isolated agent fanout configuration changed")
    if config.get("memories") != {
        "generate_memories": False,
        "use_memories": False,
        "dedicated_tools": False,
    }:
        raise ValueError("isolated memories configuration changed")
    multi_agent_v2 = features.get("multi_agent_v2")
    if multi_agent_v2 != {
        "enabled": False,
        "max_concurrent_threads_per_session": 1,
        "hide_spawn_agent_metadata": True,
    }:
        raise ValueError("isolated multi-agent v2 configuration changed")
    if config.get("projects") != expected_config.get("projects"):
        raise ValueError("isolated project trust configuration changed")
    for key in ("mcp_servers", "orchestrator", "shell_environment_policy"):
        if config.get(key) != expected_config.get(key):
            raise ValueError(f"isolated {key} configuration changed")


def _require_ambient_keys_overridden(
    ambient: Any,
    override: Any,
    *,
    path: str,
) -> None:
    """Reject ambient nested keys that would survive Codex's deep merge."""

    if ambient is None or ambient == {} or ambient == []:
        return
    if not isinstance(ambient, dict) or not isinstance(override, dict):
        # A request scalar/list replaces the entire ambient value.
        if override is None:
            raise ValueError(f"ambient {path} is not explicitly overridden")
        return
    for key, value in ambient.items():
        if not isinstance(key, str) or key not in override:
            raise ValueError(f"ambient {path}.{key!s} would survive deep merge")
        _require_ambient_keys_overridden(
            value,
            override[key],
            path=f"{path}.{key}",
        )


def _harden_ambient_shell_environment_policy(
    effective_config: dict[str, Any],
    thread_config: dict[str, Any],
) -> None:
    """Use Codex's canonical filter table when the runtime exposes it.

    Codex 0.147 serializes ``shell_environment_policy.filters`` in
    ``config/read`` alongside resolved legacy aliases.  The canonical table
    cannot be combined with the legacy ``exclude`` / ``include_only`` arrays,
    so normalize both the audit snapshot and request-local policy before
    auditing the deep merge.  Ambient filter keys are explicitly overridden
    as excludes to keep isolated turns fail-closed.
    """

    ambient = effective_config.get("shell_environment_policy")
    if not isinstance(ambient, dict) or "filters" not in ambient:
        return
    request = thread_config.get("shell_environment_policy")
    if not isinstance(request, dict):
        raise ValueError("isolated shell environment policy is malformed")

    ambient_filters = ambient.get("filters")
    if ambient_filters is None:
        ambient_filters = {}
    if not isinstance(ambient_filters, dict) or any(
        not isinstance(pattern, str)
        or action not in {"include", "exclude"}
        for pattern, action in ambient_filters.items()
    ):
        raise ValueError("ambient shell environment filters are malformed")

    canonical_filters = {
        pattern: "exclude" for pattern in ambient_filters
    }
    ambient.pop("exclude", None)
    ambient.pop("include_only", None)
    for legacy_key, action in (
        ("exclude", "exclude"),
        ("include_only", "include"),
    ):
        patterns = request.pop(legacy_key, [])
        if not isinstance(patterns, list) or any(
            not isinstance(pattern, str) for pattern in patterns
        ):
            raise ValueError(
                f"isolated shell environment {legacy_key} is malformed"
            )
        canonical_filters.update({
            pattern: action for pattern in patterns
        })
    request["filters"] = canonical_filters


def _harden_ambient_feature_config(
    effective_config: dict[str, Any],
    thread_config: dict[str, Any],
    *,
    tools_disabled: bool,
) -> frozenset[str]:
    """Explicitly override every ambient feature key in the isolated layer."""

    ambient = effective_config.get("features")
    if ambient is None:
        return frozenset()
    if not isinstance(ambient, dict) or any(
        not isinstance(name, str) or not name for name in ambient
    ):
        raise ValueError("ambient feature configuration is malformed")
    request_features = thread_config.get("features")
    if not isinstance(request_features, dict):
        raise ValueError("isolated request feature configuration is malformed")
    disabled: set[str] = set()
    for name, value in ambient.items():
        if name in request_features:
            continue
        if not tools_disabled and name in _ISOLATED_LOCAL_FEATURES:
            if type(value) is not bool:
                raise ValueError("ambient local feature is not a strict boolean")
            request_features[name] = value
            continue
        # Unknown features are capabilities, not harmless metadata. A strict
        # false override is schema-safe for feature flags; if a future Codex
        # rejects it, thread/start fails closed before model input.
        request_features[name] = False
        disabled.add(name)
    return frozenset(disabled)


def _audit_ambient_isolation_merge_inputs(
    effective_config: dict[str, Any],
    thread_config: dict[str, Any],
    *,
    cwd: str,
    permission_profile_id: str,
    explicit_mcp_servers: frozenset[str],
) -> None:
    """Prove no ambient security key can widen the request via deep merge."""

    for table in (
        "features",
        "tools",
        "orchestrator",
        "skills",
        "agents",
        "memories",
        "hooks",
        "shell_environment_policy",
    ):
        _require_ambient_keys_overridden(
            effective_config.get(table),
            thread_config.get(table),
            path=table,
        )
    ambient_permissions = effective_config.get("permissions")
    if ambient_permissions is not None and not isinstance(
        ambient_permissions,
        dict,
    ):
        raise ValueError("ambient permission profiles are malformed")
    if isinstance(ambient_permissions, dict) and permission_profile_id in ambient_permissions:
        raise ValueError("ambient config collides with the random permission profile")
    if thread_config.get("default_permissions") != permission_profile_id:
        raise ValueError("isolated permission selector changed")

    ambient_mcp = _effective_mcp_inventory({"config": effective_config})
    request_mcp = thread_config.get("mcp_servers")
    if not isinstance(request_mcp, dict):
        raise ValueError("isolated MCP override is malformed")
    for name in ambient_mcp:
        request = request_mcp.get(name)
        if name in explicit_mcp_servers:
            if not isinstance(request, dict):
                raise ValueError("explicit CCM MCP override disappeared")
        elif request != {"enabled": False}:
            raise ValueError("ambient MCP server was not explicitly disabled")

    projects = effective_config.get("projects")
    if projects is not None and not isinstance(projects, dict):
        raise ValueError("ambient project configuration is malformed")
    trust_target = codex_project_trust_target(cwd)
    if isinstance(projects, dict) and trust_target in projects:
        request_projects = thread_config.get("projects")
        request_target = (
            request_projects.get(trust_target)
            if isinstance(request_projects, dict)
            else None
        )
        _require_ambient_keys_overridden(
            projects[trust_target],
            request_target,
            path=f"projects.{trust_target}",
        )


def _audit_task_ssh_thread_response(
    response: Any,
    *,
    permission_profile_id: str,
    disable_network: bool,
    managed_network_proxy: bool,
    sandbox_mode: str,
    cwd: str,
    private_tmpdir: str,
) -> None:
    """Prove Codex admitted CCM's exact Task-isolation profile."""

    if not isinstance(response, dict):
        raise ValueError("thread response is not an object")
    permission_profile = response.get("activePermissionProfile")
    if (
        not isinstance(permission_profile, dict)
        or permission_profile.get("id") != permission_profile_id
        or permission_profile.get("extends") is not None
    ):
        raise ValueError("Task isolation profile was not selected")
    sandbox = response.get("sandbox")
    if disable_network == managed_network_proxy:
        raise ValueError("Task response audit lost its network boundary")
    expected_network = managed_network_proxy
    if (
        not isinstance(sandbox, dict)
        # Every Task profile grants its generation-private TMP leaf write
        # access. Codex 0.147 therefore reports ``workspaceWrite`` even when
        # the repository itself was requested as read-only. The selected
        # request-unique permission profile proves the repository rule; for
        # read-only turns, also prove TMP is the only extra writable root.
        or sandbox.get("type") != "workspaceWrite"
        or sandbox.get("networkAccess") is not expected_network
    ):
        raise ValueError("Task isolation resolved an unexpected sandbox policy")
    if sandbox_mode == "read-only":
        writable_roots = sandbox.get("writableRoots")
        if not isinstance(writable_roots, list) or any(
            not isinstance(path, str) for path in writable_roots
        ):
            raise ValueError("read-only Task writable roots are malformed")
        expected_scratch = _canonical_path(private_tmpdir)
        if {
            _canonical_path(path) for path in writable_roots
        } != {expected_scratch}:
            raise ValueError("read-only Task admitted an unexpected writable root")
        if (
            sandbox.get("excludeTmpdirEnvVar") is not True
            or sandbox.get("excludeSlashTmp") is not True
        ):
            raise ValueError("read-only Task did not isolate ambient temp roots")
    elif sandbox_mode != "workspace-write":
        raise ValueError("Task isolation reported an unsupported sandbox mode")
    sources = response.get("instructionSources")
    if not isinstance(sources, list):
        raise ValueError("Task isolation did not report instruction sources")
    project_root = _canonical_path(cwd)
    allowed_names = {"AGENTS.md", "AGENTS.override.md", "CLAUDE.md"}
    for source in sources:
        source_path = (
            source.get("path") if isinstance(source, dict) else source
        )
        if not isinstance(source_path, str) or not source_path:
            raise ValueError("Task instruction source is malformed")
        lexical = Path(source_path).expanduser()
        if not lexical.is_absolute():
            lexical = project_root / lexical
        try:
            lexical.relative_to(project_root)
        except ValueError as exc:
            raise ValueError(
                "Task loaded an instruction source outside its project"
            ) from exc
        canonical = _canonical_path(lexical)
        try:
            canonical.relative_to(project_root)
        except ValueError as exc:
            raise ValueError(
                "Task instruction source resolves outside its project"
            ) from exc
        if lexical.name not in allowed_names or canonical.name not in allowed_names:
            raise ValueError("Task loaded an unexpected instruction source")


def _effective_mcp_inventory(response: Any) -> dict[str, dict[str, Any]]:
    """Return one exact, JSON-backed ambient MCP inventory or fail closed."""

    effective_config = (
        response.get("config") if isinstance(response, dict) else None
    )
    if not isinstance(effective_config, dict):
        raise ValueError("effective Codex configuration is malformed")
    inventory = effective_config.get("mcp_servers", {})
    if not isinstance(inventory, dict):
        raise ValueError("effective MCP server configuration is malformed")
    normalized: dict[str, dict[str, Any]] = {}
    for name, config in inventory.items():
        if not isinstance(name, str) or not name or not isinstance(config, dict):
            raise ValueError("effective MCP server configuration is malformed")
        normalized[name] = dict(config)
    # The app-server response is JSON, but round-tripping here also rejects a
    # test double or future protocol value that cannot be compared exactly.
    return json.loads(json.dumps(normalized, sort_keys=True))


def _mcp_inventory_fingerprint(inventory: dict[str, dict[str, Any]]) -> str:
    return json.dumps(
        inventory,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _canonical_json_fingerprint(value: Any, *, label: str) -> str:
    """Return a stable JSON fingerprint or reject non-protocol values."""

    try:
        normalized = json.loads(json.dumps(value, sort_keys=True))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} is not JSON-safe") from exc
    return json.dumps(
        normalized,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _audit_network_isolated_thread_response(
    response: Any,
    *,
    cwd: str,
    permission_profile_id: str,
) -> None:
    """Prove the Delivery thread kept its exact local sandbox boundary."""

    if not isinstance(response, dict):
        raise ValueError("thread response is not an object")
    response_cwd = response.get("cwd")
    if (
        not isinstance(response_cwd, str)
        or _canonical_path(response_cwd) != _canonical_path(cwd)
    ):
        raise ValueError("thread response changed the Delivery cwd")
    permission_profile = response.get("activePermissionProfile")
    if (
        not isinstance(permission_profile, dict)
        or permission_profile.get("id")
        != permission_profile_id
        or permission_profile.get("extends") is not None
    ):
        raise ValueError("Delivery permission profile was not selected")
    sandbox = response.get("sandbox")
    if (
        not isinstance(sandbox, dict)
        or sandbox.get("type") != "workspaceWrite"
        or sandbox.get("networkAccess") is not False
        or sandbox.get("excludeTmpdirEnvVar") is not True
        or sandbox.get("excludeSlashTmp") is not True
    ):
        raise ValueError(
            "Delivery workspace-write network isolation was not admitted"
        )
    writable_roots = sandbox.get("writableRoots")
    if not isinstance(writable_roots, list):
        raise ValueError("Delivery writable roots were not reported")
    workspace = _canonical_path(cwd)
    for value in writable_roots:
        if not isinstance(value, str):
            raise ValueError("Delivery writable root is malformed")
        candidate = _canonical_path(value)
        try:
            candidate.relative_to(workspace)
        except ValueError as exc:
            raise ValueError(
                "Delivery sandbox admitted a writable root outside the worktree"
            ) from exc


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
    return sorted(disabled, key=lambda item: item["path"])


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


def _instruction_sources_snapshot(
    response: Any,
    *,
    cwd: str,
) -> tuple[tuple[Any, ...], ...]:
    """Fingerprint safe project instruction files, including symlink identity."""

    sources = response.get("instructionSources") if isinstance(response, dict) else None
    if not isinstance(sources, list):
        raise ValueError("isolated thread did not report instruction sources")
    project_root = _canonical_path(cwd)
    allowed_names = {"AGENTS.md", "AGENTS.override.md", "CLAUDE.md"}
    identities: list[tuple[Any, ...]] = []
    for source in sources:
        source_path = source.get("path") if isinstance(source, dict) else source
        if not isinstance(source_path, str) or not source_path:
            raise ValueError("isolated instruction source is malformed")
        lexical = Path(source_path).expanduser()
        if not lexical.is_absolute():
            lexical = project_root / lexical
        lexical = Path(os.path.abspath(os.fspath(lexical)))
        try:
            lexical.relative_to(project_root)
        except ValueError as exc:
            raise ValueError(
                "isolated instruction source is outside its project"
            ) from exc
        canonical = _canonical_path(lexical)
        try:
            canonical.relative_to(project_root)
        except ValueError as exc:
            raise ValueError(
                "isolated instruction source resolves outside its project"
            ) from exc
        if lexical.name not in allowed_names or canonical.name not in allowed_names:
            raise ValueError("isolated thread loaded an unexpected instruction file")
        lexical_stat_before = os.lstat(lexical)
        if not (
            stat.S_ISREG(lexical_stat_before.st_mode)
            or stat.S_ISLNK(lexical_stat_before.st_mode)
        ):
            raise ValueError("isolated instruction source is not a file")
        open_flags = os.O_RDONLY
        if hasattr(os, "O_CLOEXEC"):
            open_flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            open_flags |= os.O_NOFOLLOW
        fd = os.open(canonical, open_flags)
        try:
            target_stat_before = os.fstat(fd)
            if not stat.S_ISREG(target_stat_before.st_mode):
                raise ValueError("isolated instruction target is not regular")
            digest = hashlib.sha256()
            while True:
                chunk = os.read(fd, 1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
            target_stat_after = os.fstat(fd)
        finally:
            os.close(fd)
        lexical_stat_after = os.lstat(lexical)
        if (
            lexical_stat_before.st_dev,
            lexical_stat_before.st_ino,
            lexical_stat_before.st_mode,
            lexical_stat_before.st_size,
            lexical_stat_before.st_mtime_ns,
        ) != (
            lexical_stat_after.st_dev,
            lexical_stat_after.st_ino,
            lexical_stat_after.st_mode,
            lexical_stat_after.st_size,
            lexical_stat_after.st_mtime_ns,
        ) or (
            target_stat_before.st_dev,
            target_stat_before.st_ino,
            target_stat_before.st_mode,
            target_stat_before.st_size,
            target_stat_before.st_mtime_ns,
        ) != (
            target_stat_after.st_dev,
            target_stat_after.st_ino,
            target_stat_after.st_mode,
            target_stat_after.st_size,
            target_stat_after.st_mtime_ns,
        ) or _canonical_path(lexical) != canonical:
            raise ValueError("isolated instruction source changed while hashing")
        identities.append((
            str(lexical),
            str(canonical),
            lexical_stat_after.st_dev,
            lexical_stat_after.st_ino,
            lexical_stat_after.st_mode,
            lexical_stat_after.st_size,
            lexical_stat_after.st_mtime_ns,
            target_stat_after.st_dev,
            target_stat_after.st_ino,
            target_stat_after.st_mode,
            target_stat_after.st_size,
            target_stat_after.st_mtime_ns,
            digest.hexdigest(),
        ))
    return tuple(identities)


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
        self.native_turn_id: str | None = None
        self.unsubscribe_on_terminal = False
        self.returncode: int | None = None
        self.termination_kind: str | None = None
        self.stdout = asyncio.StreamReader(limit=10 * 1024 * 1024)
        self.stderr = asyncio.StreamReader(limit=1024 * 1024)
        self._interrupt = interrupt
        self._done = asyncio.get_running_loop().create_future()
        self._runtime_cleanup: Callable[[], None] | None = None
        self._runtime_cleanup_task: asyncio.Future[None] | None = None
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
        self._schedule_runtime_cleanup()

    def set_runtime_cleanup(self, cleanup: Callable[[], None]) -> None:
        """Bind one exact generation-owned filesystem cleanup callback."""

        if self._runtime_cleanup is not None:
            raise RuntimeError("Codex turn runtime cleanup is already configured")
        if self.returncode is not None:
            raise RuntimeError("Cannot bind runtime cleanup to a terminal turn")
        self._runtime_cleanup = cleanup

    def _schedule_runtime_cleanup(self) -> None:
        cleanup = self._runtime_cleanup
        if cleanup is None or self._runtime_cleanup_task is not None:
            return
        # Submit synchronously. ``create_task(asyncio.to_thread(...))`` can be
        # cancelled before its first scheduling step when the event loop is
        # shutting down, leaving a normal terminal turn's scratch directory
        # behind. The default executor is drained by asyncio loop shutdown.
        task = asyncio.get_running_loop().run_in_executor(None, cleanup)
        self._runtime_cleanup_task = task

        def _log_cleanup_failure(done: asyncio.Future[None]) -> None:
            try:
                error = done.exception()
            except asyncio.CancelledError:
                error = RuntimeError("Codex Task runtime cleanup was cancelled")
            if error is not None:
                logger.error(
                    "Codex Task runtime cleanup failed",
                    exc_info=(type(error), error, error.__traceback__),
                )

        task.add_done_callback(_log_cleanup_failure)

    async def wait_runtime_cleanup(self) -> None:
        task = self._runtime_cleanup_task
        if task is None:
            return
        cancellation: asyncio.CancelledError | None = None
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError as exc:
                # Cleanup belongs to the exact native generation. Delay
                # caller cancellation until its executor job has settled.
                cancellation = exc
            except Exception:
                # The completion callback records the failure. A private
                # retained directory is safer than borrowing another turn's
                # cleanup or reclassifying native process termination.
                break
        if task.done() and not task.cancelled():
            try:
                task.result()
            except Exception:
                pass
        if cancellation is not None:
            raise cancellation

    async def wait(self) -> int:
        cancellation: asyncio.CancelledError | None = None
        if not self._done.done():
            try:
                returncode = await asyncio.shield(self._done)
            except asyncio.CancelledError as exc:
                # Preserve ordinary cancellation while the native turn is
                # still live. If terminal won the race, however, its scratch
                # cleanup is already part of this exact reap barrier.
                if not self._done.done():
                    raise
                cancellation = exc
                returncode = self._done.result()
        else:
            returncode = self._done.result()
        try:
            await self.wait_runtime_cleanup()
        except asyncio.CancelledError as exc:
            cancellation = exc
        if cancellation is not None:
            raise cancellation
        return returncode

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
    mcp_only: bool = False
    allowed_mcp_tools: frozenset[tuple[str, str]] = frozenset()
    active_mcp_item_ids: set[str] = field(default_factory=set)
    # Task-isolated turns may execute only the exact request-scoped MCP
    # servers admitted at thread/start.  Keep their names on the exact turn
    # generation so a later ambient/server-side route cannot masquerade as a
    # CCM tool after admission.
    allowed_mcp_servers: frozenset[str] | None = None
    tool_policy_violation: str | None = None
    tool_policy_abort_task: asyncio.Task | None = None
    terminal_protocol_violation: str | None = None
    malformed_terminal_guard_task: asyncio.Task | None = None


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
        # Permission profiles default-deny the host filesystem. Codex 0.147
        # re-execs its standalone binary and may spawn the adjacent code-mode
        # host for sandboxed shell calls. Launch through a hard-linked,
        # credential-free, allow-listed runtime projection so every required
        # executable resolves inside the admitted directory.
        self._runtime_read_paths = _codex_runtime_read_paths(binary)
        self.binary = (
            self._runtime_read_paths[0]
            if self._runtime_read_paths
            else binary
        )
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
        self._pending: dict[
            int,
            tuple[asyncio.subprocess.Process, asyncio.Future],
        ] = {}
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
        self._runtime_version: tuple[int, int, int] | None = None
        self._runtime_version_process: asyncio.subprocess.Process | None = None
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
        malformed_task = getattr(
            context,
            "malformed_terminal_guard_task",
            None,
        )
        context.malformed_terminal_guard_task = None
        if (
            malformed_task is not None
            and malformed_task is not current_task
            and not malformed_task.done()
        ):
            malformed_task.cancel()
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
        context.process.native_turn_id = turn_id
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
            if isinstance(turn, dict):
                turn_id = turn.get("id")
                if (
                    isinstance(turn_id, str)
                    and turn_id
                    and turn_id == turn_id.strip()
                ):
                    return turn_id
        turn_id = params.get("turnId")
        return str(turn_id) if turn_id else None

    def _malformed_turn_terminal_reason(
        self,
        context: _TurnContext,
        params: dict[str, Any],
    ) -> str | None:
        """Reject ambiguous native terminals before any deferral can retain them."""

        if "turn" not in params:
            return "turn/completed is missing its turn object"
        turn = params.get("turn")
        if not isinstance(turn, dict):
            return "turn/completed turn must be an object"

        turn_id = turn.get("id")
        if (
            not isinstance(turn_id, str)
            or not turn_id.strip()
            or turn_id != turn_id.strip()
        ):
            return (
                "turn/completed turn.id must be a non-empty, "
                "whitespace-normalized string"
            )
        if "turnId" in params:
            root_turn_id = params["turnId"]
            if (
                not isinstance(root_turn_id, str)
                or not root_turn_id
                or root_turn_id != root_turn_id.strip()
            ):
                return (
                    "turn/completed root turnId must be a non-empty, "
                    "whitespace-normalized string when present"
                )
            if root_turn_id != turn_id:
                return "turn/completed root turnId conflicts with turn.id"

        status = turn.get("status")
        if (
            not isinstance(status, str)
            or not status
            or status != status.strip()
        ):
            return (
                "turn/completed turn.status must be a non-empty, "
                "whitespace-normalized string"
            )
        if "status" in params:
            root_status = params["status"]
            if (
                not isinstance(root_status, str)
                or not root_status
                or root_status != root_status.strip()
            ):
                return (
                    "turn/completed root status must be a non-empty, "
                    "whitespace-normalized string when present"
                )
            if root_status != status:
                return "turn/completed root status conflicts with turn.status"

        success_values: list[Any] = []
        if "success" in turn:
            success_values.append(turn["success"])
        # ``success`` is not part of the current native Turn schema, but
        # reject a contradictory gateway spelling rather than silently
        # blessing it as completed.
        if "success" in params:
            success_values.append(params["success"])
        for success in success_values:
            if type(success) is not bool:
                return "turn/completed success must be a boolean when present"
            if success is not (status == "completed"):
                return (
                    "turn/completed success conflicts with "
                    f"turn.status {status!r}"
                )
        if len(success_values) > 1 and any(
            success is not success_values[0]
            for success in success_values[1:]
        ):
            return "turn/completed root success conflicts with turn.success"

        if "error" in turn and "error" in params and params["error"] != turn["error"]:
            return "turn/completed root error conflicts with turn.error"

        if status == "completed":
            error = turn.get("error")
            if error not in (None, "", {}, []):
                return (
                    "turn/completed reports completed with a non-empty "
                    "turn.error"
                )
            root_error = params.get("error")
            if root_error not in (None, "", {}, []):
                return (
                    "turn/completed reports completed with a non-empty "
                    "root error"
                )

        if self._turn_terminal_correlates_context(context, params):
            return None
        return (
            "turn/completed turn.id is not correlated to the active "
            f"adapter: {turn_id!r}"
        )

    def _turn_terminal_correlates_context(
        self,
        context: _TurnContext,
        params: dict[str, Any],
    ) -> bool:
        """Require native identity or client-input proof, never thread alone."""

        candidate_ids: list[Any] = []
        turn = params.get("turn")
        if isinstance(turn, dict) and "id" in turn:
            candidate_ids.append(turn["id"])
        if "turnId" in params:
            candidate_ids.append(params["turnId"])
        for turn_id in candidate_ids:
            if (
                isinstance(turn_id, str)
                and turn_id
                and turn_id == turn_id.strip()
                and self._contexts_by_turn.get(turn_id) is context
            ):
                return True
        return self._notification_matches_context_input(
            context,
            "turn/completed",
            params,
        )

    def _fail_malformed_turn_terminal(
        self,
        context: _TurnContext,
        params: dict[str, Any],
        reason: str,
        *,
        use_context_identity: bool = False,
    ) -> None:
        """Fail closed on a corrupt native terminal without reusing an old id."""

        if not self._context_is_current(context):
            return
        error = {
            "message": f"Malformed Codex turn/completed notification: {reason}",
            "code": "ccm_malformed_turn_terminal",
        }
        event: dict[str, Any] = {
            "type": "turn.failed",
            "status": "failed",
            "success": False,
            "terminal": True,
            "error": error,
        }
        if use_context_identity:
            if context.turn_id:
                event["turn_id"] = context.turn_id
        else:
            turn = params.get("turn")
            raw_turn_id = turn.get("id") if isinstance(turn, dict) else None
            if (
                isinstance(raw_turn_id, str)
                and raw_turn_id
                and raw_turn_id == raw_turn_id.strip()
            ):
                event["turn_id"] = raw_turn_id
        context.process.feed(event)

        # A malformed completion must not survive as a retained Goal or a
        # descendant-delayed success.  Detach also cancels any live guards.
        context.pending_terminal_notification = None
        context.pending_goal_terminal_notification = None
        context.deferred_terminal_notification = None
        context.goal_terminal_generation += 1

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

        logger.error(
            "Failing Codex adapter on malformed terminal thread=%s task=%s: %s",
            context.thread_id,
            context.task_id,
            reason,
        )
        context.process.finish(1, str(error["message"]))
        self._detach_turn_context(context)

    async def _abort_uncorrelated_turn_terminal(
        self,
        context: _TurnContext,
        params: dict[str, Any],
        reason: str,
    ) -> None:
        """Stop the real current turn before failing its local adapter.

        A terminal found only through ``threadId`` may belong to an older
        native turn.  Until the exact current turn is interrupted (or the
        whole transport is proven dead), its context and runtime identities
        must remain live so no replacement can overlap it.
        """

        try:
            try:
                await self._interrupt_turn_context(context)
            except asyncio.CancelledError:
                raise
            except BaseException:
                logger.exception(
                    "Could not confirm exact Codex interrupt after an "
                    "uncorrelated terminal thread=%s task=%s; shutting down "
                    "the transport",
                    context.thread_id,
                    context.task_id,
                )
                try:
                    # Do not mark the target as a user interrupt.  Protocol
                    # ambiguity is a failed turn, and every peer adapter on
                    # this now-untrusted transport must fail as well.
                    await self.shutdown(
                        reason=(
                            "Codex app-server emitted an uncorrelated terminal: "
                            f"{reason}"
                        ),
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException:
                    # shutdown() publishes its one-way intent before touching
                    # the process.  Retain the context/runtime and live adapter
                    # when termination cannot be proven; a future start will
                    # fail closed on that shutdown intent.
                    logger.exception(
                        "Could not prove Codex transport shutdown after an "
                        "uncorrelated terminal thread=%s task=%s",
                        context.thread_id,
                        context.task_id,
                    )
                    return
                if self._context_is_current(context):
                    # Test doubles and a reader-cancellation race may return
                    # from shutdown without running normal EOF finalization.
                    self._fail_malformed_turn_terminal(
                        context,
                        params,
                        reason,
                        use_context_identity=True,
                    )
                return

            if self._context_is_current(context):
                self._fail_malformed_turn_terminal(
                    context,
                    params,
                    reason,
                    use_context_identity=True,
                )
        finally:
            if context.malformed_terminal_guard_task is asyncio.current_task():
                context.malformed_terminal_guard_task = None

    def _schedule_uncorrelated_turn_terminal_abort(
        self,
        context: _TurnContext,
        params: dict[str, Any],
        reason: str,
    ) -> None:
        """Retain ownership while asynchronously isolating an unknown terminal."""

        if not self._context_is_current(context):
            return
        existing = context.malformed_terminal_guard_task
        if existing is not None and not existing.done():
            return
        context.terminal_protocol_violation = reason
        context.malformed_terminal_guard_task = asyncio.create_task(
            self._abort_uncorrelated_turn_terminal(
                context,
                dict(params),
                reason,
            )
        )

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
            if context is not None and (context.tools_disabled or context.mcp_only):
                return context
        thread_id = params.get("threadId") or params.get("conversationId")
        if thread_id:
            context = self._contexts_by_thread.get(str(thread_id))
            if context is not None and (context.tools_disabled or context.mcp_only):
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
            (
                not context.tools_disabled
                and not context.mcp_only
                and context.allowed_mcp_servers is None
            )
            or context.process.returncode is not None
            or context.tool_policy_violation is not None
        ):
            return
        if context.tools_disabled:
            reason = (
                "Codex PR review attempted a forbidden tool or autonomous "
                f"capability: {source}"
            )
        elif context.mcp_only:
            reason = (
                "Codex MCP-only turn attempted a forbidden tool or autonomous "
                f"capability: {source}"
            )
        else:
            reason = (
                "Codex Task-isolated turn attempted an unauthorized MCP "
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
            and context.terminal_protocol_violation is None
            and not (context.tools_disabled or context.mcp_only)
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
        if context.terminal_protocol_violation is not None:
            normalized_error = {
                "message": (
                    "Malformed Codex turn/completed notification: "
                    f"{context.terminal_protocol_violation}"
                ),
                "code": "ccm_malformed_turn_terminal",
            }
            context.process.feed(
                {
                    "type": "turn.failed",
                    "turn_id": terminal_turn_id,
                    "status": "failed",
                    "success": False,
                    "terminal": True,
                    "error": normalized_error,
                }
            )
            status = "terminalProtocolViolation"
            exit_code = 1
            stderr = str(normalized_error["message"])
        elif context.tool_policy_violation is not None:
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
        elif context.non_retry_error is not None:
            # A willRetry=false notification is authoritative even when Codex
            # later closes the native turn as completed/interrupted.  Publish
            # an explicit failed terminal so durable arbitration cannot infer
            # success from the otherwise ambiguous turn.completed type.
            normalized_error = dict(context.non_retry_error)
            context.process.feed(
                {
                    "type": "turn.completed",
                    "usage": context.usage or {},
                    "turn_id": terminal_turn_id,
                    "status": "failed",
                    "success": False,
                    "error": normalized_error,
                }
            )
            status = "failed"
            exit_code = 1
            stderr = str(normalized_error["message"])
        elif status == "completed":
            context.process.feed(
                {
                    "type": "turn.completed",
                    "usage": context.usage or {},
                    "turn_id": terminal_turn_id,
                    "status": "completed",
                    "success": True,
                    "error": None,
                }
            )
            exit_code = 0
            stderr = ""
        elif status == "interrupted":
            normalized_error = self._normalize_turn_error(
                error,
                fallback="Codex turn was interrupted",
            )
            context.process.feed(
                {
                    "type": "turn.completed",
                    "usage": context.usage or {},
                    "turn_id": terminal_turn_id,
                    "status": "interrupted",
                    "success": False,
                    "error": normalized_error,
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

    def _managed_network_runtime_is_safe(self) -> bool:
        """Require version proof from this exact live app-server generation."""

        return bool(
            self._process is not None
            and self._process.returncode is None
            and self._runtime_version_process is self._process
            and self._runtime_version is not None
            and self._runtime_version >= _MANAGED_NETWORK_MIN_CODEX_VERSION
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
        self._runtime_version = None
        self._runtime_version_process = None
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
        log_quarantines = _prepare_codex_log_db_rotation(codex_home)
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
            "limit": _APP_SERVER_STREAM_LIMIT,
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
            initialize_response = await self._request(
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
            self._runtime_version = _parse_codex_app_server_version(
                initialize_response.get("userAgent")
                if isinstance(initialize_response, dict)
                else None
            )
            self._runtime_version_process = process
            await self._notify("initialized", {})
            try:
                _finalize_codex_log_db_rotation(
                    codex_home,
                    log_quarantines,
                )
            except OSError:
                # Initialization already proved the replacement database is
                # healthy. Keep the exact quarantine for a later successful
                # startup instead of failing or touching session state.
                logger.exception(
                    "Could not remove recovered Codex log quarantine home=%s",
                    codex_home,
                )
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
        task_ssh_allowed_read_paths: Sequence[str] = (),
        task_git_read_paths: Sequence[str] = (),
        task_git_boundary_fingerprint: Sequence[tuple[object, ...]] = (),
        task_private_tmpdir: PrivateTaskTempDir | None = None,
        task_ssh_disable_network: bool = False,
        task_managed_network_proxy: bool = False,
        disable_autonomous_features: bool = False,
        network_isolated: bool = False,
        output_schema: dict[str, Any] | None = None,
        tools_disabled: bool = False,
        mcp_only: bool = False,
        on_thread_started: (
            Callable[[str], Awaitable[None]] | None
        ) = None,
        on_turn_prepared: (
            Callable[[CodexTurnProcess, str], Awaitable[None]] | None
        ) = None,
        _isolated_admission_retry_count: int = 0,
    ) -> tuple[CodexTurnProcess, str]:
        managed_runtime_prestarted = False
        managed_network_process: asyncio.subprocess.Process | None = None
        isolated_process: asyncio.subprocess.Process | None = None
        if task_managed_network_proxy:
            # ``initialize`` is the version proof for this exact transport
            # generation. Complete it before constructing the network profile;
            # no model input or thread RPC has happened at this boundary.
            try:
                await self.ensure_started()
            except CodexAppServerBusyError:
                raise
            except Exception as exc:
                raise CodexRequiredMcpPreTurnError(
                    "Codex managed-network runtime version could not be "
                    "proven before thread admission"
                ) from exc
            managed_runtime_prestarted = True
            if not self._managed_network_runtime_is_safe():
                logger.warning(
                    "Codex managed proxy disabled for unproven runtime "
                    "version binary=%s version=%s; using network-off Task "
                    "isolation",
                    self.binary,
                    self._runtime_version,
                )
                task_managed_network_proxy = False
                task_ssh_disable_network = True
            else:
                managed_network_process = self._process

        def require_managed_network_generation(stage: str) -> None:
            """Keep managed networking on its proven transport generation."""

            if not task_managed_network_proxy:
                return
            if (
                managed_network_process is None
                or self._process is not managed_network_process
                or not self._managed_network_runtime_is_safe()
            ):
                raise CodexRequiredMcpPreTurnError(
                    "Codex managed-network runtime generation changed " + stage
                )
        if sandbox_mode not in {
            "danger-full-access",
            "workspace-write",
            "read-only",
        }:
            raise ValueError(f"Unsupported Codex sandbox mode: {sandbox_mode!r}")
        if tools_disabled and mcp_only:
            raise CodexRequiredMcpPreTurnError(
                "Codex turn cannot be both tool-free and MCP-only"
            )
        restricted_tools = tools_disabled or mcp_only
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
        if mcp_only:
            if os.name != "posix":
                raise CodexRequiredMcpPreTurnError(
                    "Codex MCP-only profile currently requires POSIX"
                )
            if sandbox_mode != "read-only" or not mcp_specs:
                raise CodexRequiredMcpPreTurnError(
                    "Codex MCP-only profile requires read-only admission and required MCP"
                )
            if any(not spec.required for spec in mcp_specs):
                raise CodexRequiredMcpPreTurnError(
                    "Codex MCP-only profile requires every MCP server"
                )
            if any(not spec.enabled_tools for spec in mcp_specs):
                raise CodexRequiredMcpPreTurnError(
                    "Codex MCP-only profile requires an explicit tool allow-list"
                )
            if skill_context.strip() or git_env:
                raise CodexRequiredMcpPreTurnError(
                    "Codex MCP-only profile forbids skill context and Git credentials"
                )
            if not disable_autonomous_features:
                raise CodexRequiredMcpPreTurnError(
                    "Codex MCP-only profile requires autonomous features to be disabled"
                )
            if resume_session_id:
                logger.info(
                    "Ignoring Codex Browser Agent resume thread %s; MCP-only "
                    "admission requires a fresh native thread",
                    resume_session_id,
                )
                resume_session_id = None
        admitted_git_boundary = None
        if task_ssh_protected_paths:
            if tools_disabled:
                raise CodexRequiredMcpPreTurnError(
                    "Task SSH isolation cannot be combined with tool-free mode"
                )
            if not disable_project_config:
                raise CodexRequiredMcpPreTurnError(
                    "Task isolation requires project config to be disabled"
                )
            if not disable_user_mcp:
                raise CodexRequiredMcpPreTurnError(
                    "Task isolation requires ambient MCP to be disabled"
                )
            if not disable_autonomous_features:
                raise CodexRequiredMcpPreTurnError(
                    "Task isolation requires autonomous features to be disabled"
                )
            if sandbox_mode not in {"workspace-write", "read-only"}:
                raise CodexRequiredMcpPreTurnError(
                    "Task isolation requires workspace-write or read-only admission"
                )
            if task_managed_network_proxy:
                if sandbox_mode != "workspace-write":
                    raise CodexRequiredMcpPreTurnError(
                        "Codex managed Task network requires workspace-write"
                    )
                if task_ssh_disable_network:
                    raise CodexRequiredMcpPreTurnError(
                        "Codex Task cannot enable and disable network together"
                    )
            elif sandbox_mode == "workspace-write" and not task_ssh_disable_network:
                raise CodexRequiredMcpPreTurnError(
                    "Codex Task isolation requires either managed public "
                    "network or complete network denial"
                )
            protected = {
                os.path.abspath(os.path.expanduser(str(path)))
                for path in task_ssh_protected_paths
            }
            allowed = {
                os.path.abspath(os.path.expanduser(str(path)))
                for path in task_ssh_allowed_read_paths
            }
            if "/" in allowed or protected & allowed:
                raise CodexRequiredMcpPreTurnError(
                    "Task Git credential read overrides must be exact and "
                    "must not duplicate deny entries"
                )
            from backend.services.task_agent_isolation import (
                discover_linked_worktree_git_read_boundary,
            )

            git_boundary = discover_linked_worktree_git_read_boundary(cwd)
            admitted_git_boundary = git_boundary
            expected_git_paths = tuple(
                sorted(git_boundary.read_paths if git_boundary else ())
            )
            supplied_git_paths = tuple(sorted({
                os.path.abspath(os.path.expanduser(str(path)))
                for path in task_git_read_paths
            }))
            if supplied_git_paths != expected_git_paths:
                raise CodexRequiredMcpPreTurnError(
                    "Codex Task linked-worktree Git projection was not exact"
                )
            expected_fingerprint = (
                git_boundary.identity_fingerprint if git_boundary else ()
            )
            if tuple(task_git_boundary_fingerprint) != expected_fingerprint:
                raise CodexRequiredMcpPreTurnError(
                    "Codex Task linked-worktree identity snapshot was not exact"
                )
            if task_private_tmpdir is None:
                raise CodexRequiredMcpPreTurnError(
                    "Codex Task isolation requires a private generation TMPDIR"
                )
            task_private_tmpdir.assert_valid()
        elif task_ssh_disable_network or task_managed_network_proxy:
            raise CodexRequiredMcpPreTurnError(
                "Task network isolation requires protected filesystem paths"
            )
        elif (
            (task_git_read_paths or task_private_tmpdir is not None)
            and not network_isolated
        ):
            raise CodexRequiredMcpPreTurnError(
                "Task Git/TMP isolation requires protected filesystem paths"
            )
        if network_isolated:
            if sandbox_mode != "workspace-write":
                raise CodexRequiredMcpPreTurnError(
                    "Network-isolated Codex execution requires workspace-write"
                )
            if mcp_specs or git_env:
                raise CodexRequiredMcpPreTurnError(
                    "Network-isolated Codex execution forbids MCP and Git "
                    "credential environment injection"
                )
            if not disable_user_mcp or not disable_autonomous_features:
                raise CodexRequiredMcpPreTurnError(
                    "Network-isolated Codex execution requires user MCP and "
                    "autonomous features to be disabled"
                )
            if task_ssh_protected_paths:
                raise CodexRequiredMcpPreTurnError(
                    "Network-isolated execution cannot also use Task SSH"
                )
            from backend.services.task_agent_isolation import (
                discover_linked_worktree_git_read_boundary,
            )

            git_boundary = discover_linked_worktree_git_read_boundary(cwd)
            admitted_git_boundary = git_boundary
            expected_git_paths = tuple(
                sorted(git_boundary.read_paths if git_boundary else ())
            )
            supplied_git_paths = tuple(sorted({
                os.path.abspath(os.path.expanduser(str(path)))
                for path in task_git_read_paths
            }))
            if supplied_git_paths != expected_git_paths:
                raise CodexRequiredMcpPreTurnError(
                    "Codex Delivery linked-worktree Git projection was not exact"
                )
            expected_fingerprint = (
                git_boundary.identity_fingerprint if git_boundary else ()
            )
            if tuple(task_git_boundary_fingerprint) != expected_fingerprint:
                raise CodexRequiredMcpPreTurnError(
                    "Codex Delivery linked-worktree identity snapshot was not exact"
                )
            if task_private_tmpdir is None:
                raise CodexRequiredMcpPreTurnError(
                    "Codex Delivery requires a private generation TMPDIR"
                )
            task_private_tmpdir.assert_valid()
            network_permission_config = _network_isolated_permission_config(
                cwd=cwd,
                git_read_paths=task_git_read_paths,
                private_tmpdir=str(task_private_tmpdir.path),
                runtime_read_paths=self._runtime_read_paths,
            )
            _audit_network_isolated_permission_config(
                network_permission_config,
                cwd=cwd,
                git_read_paths=task_git_read_paths,
                private_tmpdir=str(task_private_tmpdir.path),
                runtime_read_paths=self._runtime_read_paths,
            )
        tool_free_permission_profile = (
            f"{_TOOL_FREE_PERMISSION_PROFILE_PREFIX}{uuid.uuid4().hex}"
            if restricted_tools
            else None
        )
        network_permission_profile = (
            f"{_NETWORK_ISOLATED_PERMISSION_PROFILE_PREFIX}{uuid.uuid4().hex}"
            if network_isolated
            else None
        )
        task_permission_profile = (
            f"{(
                _TASK_MANAGED_NETWORK_PERMISSION_PROFILE_PREFIX
                if task_managed_network_proxy
                else _TASK_SSH_PERMISSION_PROFILE_PREFIX
            )}{uuid.uuid4().hex}"
            if task_ssh_protected_paths
            else None
        )

        def require_stable_task_filesystem_boundary(stage: str) -> None:
            if not task_ssh_protected_paths and not network_isolated:
                return
            from backend.services.task_agent_isolation import (
                discover_linked_worktree_git_read_boundary,
            )

            current = discover_linked_worktree_git_read_boundary(cwd)
            current_paths = tuple(sorted(current.read_paths if current else ()))
            supplied_paths = tuple(sorted({
                os.path.abspath(os.path.expanduser(str(path)))
                for path in task_git_read_paths
            }))
            if current_paths != supplied_paths:
                raise CodexRequiredMcpPreTurnError(
                    "Codex Task linked-worktree boundary changed " + stage
                )
            if current != admitted_git_boundary:
                raise CodexRequiredMcpPreTurnError(
                    "Codex Task linked-worktree identity changed " + stage
                )
            if task_private_tmpdir is None:
                raise CodexRequiredMcpPreTurnError(
                    "Codex Task scratch boundary disappeared " + stage
                )
            try:
                task_private_tmpdir.assert_valid()
            except Exception as exc:
                raise CodexRequiredMcpPreTurnError(
                    "Codex Task scratch boundary changed " + stage
                ) from exc
            profile_id = (
                tool_free_permission_profile
                if restricted_tools
                else (
                    task_permission_profile
                    if task_ssh_protected_paths
                    else network_permission_profile
                )
            )
            profile = thread_config.get("permissions", {}).get(profile_id)
            try:
                if restricted_tools:
                    if profile != _tool_free_permission_config():
                        raise ValueError(
                            "tool-restricted permission profile changed"
                        )
                elif task_ssh_protected_paths:
                    _audit_task_isolation_permission_config(
                        profile,
                        cwd=cwd,
                        protected_paths=task_ssh_protected_paths,
                        allowed_read_paths=task_ssh_allowed_read_paths,
                        git_read_paths=task_git_read_paths,
                        private_tmpdir=str(task_private_tmpdir.path),
                        disable_network=task_ssh_disable_network,
                        managed_network_proxy=task_managed_network_proxy,
                        sandbox_mode=sandbox_mode,
                        runtime_read_paths=self._runtime_read_paths,
                    )
                else:
                    _audit_network_isolated_permission_config(
                        profile,
                        cwd=cwd,
                        git_read_paths=task_git_read_paths,
                        private_tmpdir=str(task_private_tmpdir.path),
                        runtime_read_paths=self._runtime_read_paths,
                    )
            except (TypeError, ValueError) as exc:
                raise CodexRequiredMcpPreTurnError(
                    "Codex Task permission profile changed " + stage
                ) from exc

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
        isolated_ambient_config_fingerprint: str | None = None
        isolated_ambient_config_sections: dict[str, str] | None = None
        isolated_skills_inventory_fingerprint: str | None = None
        isolated_ambient_effective_config: dict[str, Any] | None = None
        isolated_dynamic_disabled_features: frozenset[str] = frozenset()

        async def retry_isolated_admission_after_skills_drift(
            thread_id: str,
            error: _CodexIsolatedSkillsDriftError,
        ) -> tuple[CodexTurnProcess, str]:
            """Delete one empty thread and rebuild its exact skill deny-list."""

            if resume_session_id:
                # A resumed id owns durable history and must never be deleted
                # as compensation for a local admission race.
                raise error

            logger.warning(
                "Recycling empty Codex thread after isolated skills drift "
                "home=%s thread=%s retry=%s",
                self.codex_home,
                thread_id,
                _isolated_admission_retry_count,
            )
            delete_request = asyncio.create_task(self._request(
                "thread/delete",
                {"threadId": thread_id},
                expected_process=isolated_process,
            ))
            cancelled = False
            while not delete_request.done():
                try:
                    await asyncio.shield(delete_request)
                except asyncio.CancelledError:
                    cancelled = True
                    continue
            try:
                delete_request.result()
            except Exception as cleanup_error:
                raise CodexRequiredMcpPreTurnError(
                    "Codex could not release an empty isolated thread after "
                    "its skills inventory changed"
                ) from cleanup_error
            finally:
                self._known_threads.discard(thread_id)
                self._contexts_by_thread.pop(thread_id, None)

            if cancelled:
                raise asyncio.CancelledError
            if _isolated_admission_retry_count >= 1:
                raise error

            return await self.start_turn(
                prompt=prompt,
                cwd=cwd,
                model=model,
                effort=effort,
                resume_session_id=resume_session_id,
                git_env=git_env,
                task_id=task_id,
                mcp_specs=mcp_specs,
                disable_project_config=disable_project_config,
                disable_user_mcp=disable_user_mcp,
                skill_context=skill_context,
                codex_service_tier=codex_service_tier,
                sandbox_mode=sandbox_mode,
                task_ssh_protected_paths=task_ssh_protected_paths,
                task_ssh_allowed_read_paths=task_ssh_allowed_read_paths,
                task_git_read_paths=task_git_read_paths,
                task_git_boundary_fingerprint=task_git_boundary_fingerprint,
                task_private_tmpdir=task_private_tmpdir,
                task_ssh_disable_network=task_ssh_disable_network,
                task_managed_network_proxy=task_managed_network_proxy,
                disable_autonomous_features=disable_autonomous_features,
                network_isolated=network_isolated,
                output_schema=output_schema,
                tools_disabled=tools_disabled,
                mcp_only=mcp_only,
                on_thread_started=on_thread_started,
                on_turn_prepared=on_turn_prepared,
                _isolated_admission_retry_count=(
                    _isolated_admission_retry_count + 1
                ),
            )
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
        explicit_mcp_servers = dict(
            thread_config.get("mcp_servers", {})
        )
        if disable_user_mcp:
            # Keep the caller's exact CCM specs. Lower config layers are
            # inventoried and disabled after the account app-server starts.
            thread_config["mcp_servers"] = dict(explicit_mcp_servers)
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
        if disable_project_config or restricted_tools or network_isolated:
            _deep_merge_config(
                thread_config,
                codex_untrusted_project_config(cwd),
            )
        if network_isolated:
            # Built-in web search, Apps and ambient MCP run outside the local
            # shell sandbox. Disable those routes and every autonomous/remote
            # capability while retaining the local coding tools. ``core`` is
            # Codex's fixed PATH/HOME/etc allow-list; explicit excludes and a
            # disabled shell snapshot keep Git/GitHub credentials out even if
            # a future core list expands.
            _deep_merge_config(
                thread_config,
                {
                    "web_search": "disabled",
                    "allow_login_shell": False,
                    "features": {
                        feature: False
                        for feature in _NETWORK_ISOLATED_DISABLED_FEATURES
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
                    # WorkspaceWrite alone only narrows writes; its legacy
                    # profile can still read the entire host. This named
                    # request-local profile defaults the filesystem to deny,
                    # admits only Codex's minimal executable/runtime roots for
                    # reading, and grants the exact managed worktree read/write.
                    "default_permissions": (
                        network_permission_profile
                    ),
                    "permissions": {
                        network_permission_profile: network_permission_config,
                    },
                    "shell_environment_policy": {
                        "inherit": "core",
                        "ignore_default_excludes": False,
                        "exclude": [
                            "GIT_*",
                            "GH_*",
                            "GITHUB_*",
                            "SSH_*",
                        ],
                        "set": {
                            "GIT_TERMINAL_PROMPT": "0",
                            "GCM_INTERACTIVE": "never",
                            "GIT_CONFIG_GLOBAL": os.devnull,
                            "GIT_CONFIG_NOSYSTEM": "1",
                            "GH_PROMPT_DISABLED": "1",
                            "GIT_OPTIONAL_LOCKS": "0",
                            "TMPDIR": str(task_private_tmpdir.path),
                            "TMP": str(task_private_tmpdir.path),
                            "TEMP": str(task_private_tmpdir.path),
                        },
                    },
                },
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
        if restricted_tools:
            assert tool_free_permission_profile is not None
            # PR-review turns receive a complete backend-snapshotted prompt.
            # ``environments=[]`` below removes environment-backed tool specs.
            # The named profile denies every filesystem path and all network,
            # so even an accidentally reintroduced tool cannot read local
            # credentials before the event-level audit interrupts the turn.
            _deep_merge_config(
                thread_config,
                {
                    "web_search": "disabled",
                    "allow_login_shell": False,
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
                        "mcp": {"enabled": mcp_only},
                    },
                    "skills": {
                        "include_instructions": False,
                        "bundled": {"enabled": False},
                        "config": [],
                    },
                    "default_permissions": tool_free_permission_profile,
                    "permissions": {
                        tool_free_permission_profile: (
                            _tool_free_permission_config()
                        ),
                    },
                    "shell_environment_policy": {
                        "inherit": "none",
                        "ignore_default_excludes": False,
                        "exclude": [],
                        "include_only": [],
                        "experimental_use_profile": False,
                        "set": {},
                    },
                    "project_doc_max_bytes": 0,
                    "project_doc_fallback_filenames": [],
                },
            )
        elif task_ssh_protected_paths:
            assert task_private_tmpdir is not None
            assert task_permission_profile is not None
            task_permission_config = _task_ssh_permission_config(
                cwd=cwd,
                protected_paths=task_ssh_protected_paths,
                allowed_read_paths=task_ssh_allowed_read_paths,
                git_read_paths=task_git_read_paths,
                private_tmpdir=str(task_private_tmpdir.path),
                disable_network=task_ssh_disable_network,
                managed_network_proxy=task_managed_network_proxy,
                sandbox_mode=sandbox_mode,
                runtime_read_paths=self._runtime_read_paths,
            )
            _audit_task_isolation_permission_config(
                task_permission_config,
                cwd=cwd,
                protected_paths=task_ssh_protected_paths,
                allowed_read_paths=task_ssh_allowed_read_paths,
                git_read_paths=task_git_read_paths,
                private_tmpdir=str(task_private_tmpdir.path),
                disable_network=task_ssh_disable_network,
                managed_network_proxy=task_managed_network_proxy,
                sandbox_mode=sandbox_mode,
                runtime_read_paths=self._runtime_read_paths,
            )
            _deep_merge_config(
                thread_config,
                {
                    "web_search": "disabled",
                    "allow_login_shell": False,
                    "features": {
                        feature: False
                        for feature in _NETWORK_ISOLATED_DISABLED_FEATURES
                    } | {"network_proxy": task_managed_network_proxy},
                    "tools": {
                        "experimental_request_user_input": {
                            "enabled": False,
                        },
                    },
                    "orchestrator": {
                        "skills": {"enabled": False},
                    },
                    "skills": {
                        "include_instructions": False,
                        "bundled": {"enabled": False},
                        "config": [],
                    },
                    "default_permissions": task_permission_profile,
                    "permissions": {
                        task_permission_profile: task_permission_config,
                    },
                },
            )
        try:
            if not managed_runtime_prestarted:
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

        if restricted_tools or network_isolated or task_ssh_protected_paths:
            isolated_process = self._process
            if (
                isolated_process is None
                or isolated_process.returncode is not None
            ):
                raise CodexRequiredMcpPreTurnError(
                    "Codex isolated runtime generation was not captured"
                )
            if (
                task_managed_network_proxy
                and isolated_process is not managed_network_process
            ):
                raise CodexRequiredMcpPreTurnError(
                    "Codex managed-network runtime generation changed "
                    "during startup"
                )

        def require_isolated_runtime_generation(stage: str) -> None:
            if isolated_process is None:
                return
            if (
                self._process is not isolated_process
                or isolated_process.returncode is not None
            ):
                raise CodexRequiredMcpPreTurnError(
                    "Codex isolated runtime generation changed " + stage
                )

        async def read_effective_config() -> dict[str, Any]:
            response = await self._request(
                "config/read",
                {
                    "cwd": os.path.abspath(cwd),
                    "includeLayers": False,
                },
            )
            config = (
                response.get("config") if isinstance(response, dict) else None
            )
            if not isinstance(config, dict):
                raise ValueError("effective Codex configuration is malformed")
            return config

        async def read_ambient_mcp_inventory() -> dict[str, dict[str, Any]]:
            return _effective_mcp_inventory({
                "config": await read_effective_config(),
            })

        async def read_skills_inventory() -> Any:
            return await self._request(
                "skills/list",
                {
                    "cwds": [os.path.abspath(cwd)],
                    "forceReload": True,
                },
            )

        async def read_stable_skills_snapshot(
        ) -> tuple[list[dict[str, Any]], str, int] | None:
            """Capture two matching current Codex 0.147 skill inventories."""

            runtime_version = self._runtime_version
            if runtime_version is None or runtime_version < (0, 147, 0):
                return None
            reads = 0
            try:
                async with asyncio.timeout(
                    _ISOLATED_SKILLS_SNAPSHOT_TIMEOUT
                ):
                    previous_fingerprint: str | None = None
                    previous_revision: int | None = None
                    while reads < _ISOLATED_SKILLS_SNAPSHOT_MAX_READS:
                        reads += 1
                        revision_before = self._skills_revision
                        skills_inventory = await read_skills_inventory()
                        revision_after = self._skills_revision
                        disabled_skills = _tool_free_disabled_skill_config(
                            skills_inventory,
                            cwd=cwd,
                        )
                        fingerprint = _canonical_json_fingerprint(
                            disabled_skills,
                            label="Codex disabled skills inventory",
                        )
                        # Let notifications already queued behind the RPC
                        # response run before accepting this read.
                        await asyncio.sleep(0)
                        revision_drained = self._skills_revision
                        if (
                            revision_before != revision_after
                            or revision_after != revision_drained
                        ):
                            previous_fingerprint = None
                            previous_revision = None
                            logger.info(
                                "Resetting Codex isolated skills snapshot "
                                "after inventory refresh home=%s read=%s",
                                self.codex_home,
                                reads,
                            )
                            continue
                        if (
                            previous_fingerprint == fingerprint
                            and previous_revision == revision_before
                        ):
                            return (
                                disabled_skills,
                                fingerprint,
                                revision_drained,
                            )
                        previous_fingerprint = fingerprint
                        previous_revision = revision_drained
            except TimeoutError as exc:
                raise ValueError(
                    "Codex isolated skills inventory did not stabilize "
                    f"within {_ISOLATED_SKILLS_SNAPSHOT_TIMEOUT:g} seconds"
                ) from exc
            raise ValueError(
                "Codex isolated skills inventory did not stabilize after "
                f"{reads} forced skill reads"
            )

        def require_no_ambient_instruction_config(
            effective_config: dict[str, Any],
        ) -> None:
            for instruction_key in (
                "developer_instructions",
                "instructions",
                "model_instructions_file",
            ):
                value = effective_config.get(instruction_key)
                if value is not None and value != "":
                    raise ValueError("ambient Codex instructions are configured")

        async def require_stable_isolated_ambient_state(phase: str) -> None:
            if not (
                restricted_tools
                or network_isolated
                or task_ssh_protected_paths
            ):
                return
            if (
                isolated_ambient_config_fingerprint is None
                or isolated_ambient_config_sections is None
                or isolated_skills_inventory_fingerprint is None
                or tool_free_skills_revision is None
            ):
                raise CodexRequiredMcpPreTurnError(
                    "Codex isolated ambient state was not captured"
                )
            try:
                effective_config = await read_effective_config()
                require_no_ambient_instruction_config(effective_config)
                current_config_fingerprint = _canonical_json_fingerprint(
                    effective_config,
                    label="effective Codex configuration",
                )
                current_skills = await read_skills_inventory()
                current_disabled_skills = _tool_free_disabled_skill_config(
                    current_skills,
                    cwd=cwd,
                )
                current_skills_fingerprint = _canonical_json_fingerprint(
                    current_disabled_skills,
                    label="Codex disabled skills inventory",
                )
            except Exception as exc:
                raise CodexRequiredMcpPreTurnError(
                    "Codex isolated execution could not re-audit ambient "
                    "configuration and skills "
                    f"{phase}"
                ) from exc
            config_changed = (
                current_config_fingerprint
                != isolated_ambient_config_fingerprint
            )
            skills_changed = (
                current_skills_fingerprint
                != isolated_skills_inventory_fingerprint
            )
            revision_changed = (
                self._skills_revision != tool_free_skills_revision
            )
            if config_changed or skills_changed or revision_changed:
                current_sections = {
                    key: _canonical_json_fingerprint(
                        value,
                        label=f"effective Codex configuration section {key}",
                    )
                    for key, value in effective_config.items()
                }
                changed_sections = sorted(
                    key
                    for key in (
                        set(isolated_ambient_config_sections)
                        | set(current_sections)
                    )
                    if isolated_ambient_config_sections.get(key)
                    != current_sections.get(key)
                )
                error_type = (
                    _CodexIsolatedSkillsDriftError
                    if (
                        not config_changed
                        and (skills_changed or revision_changed)
                    )
                    else CodexRequiredMcpPreTurnError
                )
                raise error_type(
                    "Codex isolated ambient configuration or skills changed "
                    f"{phase} (config_sections={changed_sections}, "
                    f"skills={skills_changed}, revision={revision_changed})"
                )

        task_resume_runtime_recycled = False
        if resume_session_id and (task_ssh_protected_paths or network_isolated):
            # ``thread/resume`` applies request-local MCP, feature and skill
            # overrides only while loading an unloaded rollout. Codex keeps a
            # loaded thread's MCP clients and code-mode host alive and merely
            # logs that new overrides were ignored. Prove this exact native
            # thread is quiescent, then unload/reload it before taking any
            # ambient inventory snapshot or sending model input.
            #
            # The registry already holds the per-thread start reservation
            # across this method. Goal + thread/read is the stronger native
            # idle proof: an empty CCM context map alone cannot rule out an
            # autonomous Goal left by an older process generation.
            require_stable_task_filesystem_boundary(
                "before isolated resume runtime recycle"
            )
            require_isolated_runtime_generation(
                "before isolated resume runtime recycle"
            )
            require_managed_network_generation(
                "before isolated resume runtime recycle"
            )
            runtime_needs_recycle = True
            try:
                await self.require_thread_routing_quiescence(
                    str(resume_session_id),
                )
            except CodexThreadTerminalStateError as exc:
                if exc.state == "notLoaded":
                    # A fresh app-server has no live MCP client, code-mode
                    # host, or autonomous turn for an unloaded rollout. The
                    # upcoming exact thread/resume will load it with this
                    # request's newly audited isolation config, so mutating
                    # archive state here is both unnecessary and less safe.
                    runtime_needs_recycle = False
                    logger.info(
                        "Codex isolated resume runtime is already unloaded "
                        "task=%s thread=%s",
                        task_id,
                        resume_session_id,
                    )
                # systemError is authoritative evidence that no turn is
                # running. It is the one loaded terminal state for which the
                # existing archive/unarchive recovery is safe. Unknown and
                # all non-idle Goal states remain closed.
                elif exc.state != "systemError":
                    raise
            if runtime_needs_recycle:
                try:
                    await self.recycle_thread_runtime(str(resume_session_id))
                    task_resume_runtime_recycled = True
                except CodexThreadRuntimeRecycleCancelled:
                    raise
                except asyncio.CancelledError as exc:
                    raise CodexThreadRuntimeRecycleCancelled(
                        str(resume_session_id)
                    ) from exc
                except CodexThreadRuntimeRecycleError:
                    raise
                except Exception as exc:
                    # Archive is a native mutation boundary. Once recycle is
                    # attempted, an RPC error or lost acknowledgement cannot
                    # be classified as replay-safe merely because turn/start
                    # has not happened yet.
                    raise CodexThreadRuntimeRecycleError(
                        str(resume_session_id),
                        str(exc) or type(exc).__name__,
                    ) from exc
            require_stable_task_filesystem_boundary(
                "after isolated resume runtime recycle"
            )
            require_isolated_runtime_generation(
                "after isolated resume runtime recycle"
            )
            require_managed_network_generation(
                "after isolated resume runtime recycle"
            )
            if task_resume_runtime_recycled:
                logger.info(
                    "Recycled idle Codex Task runtime before exact resume "
                    "task=%s thread=%s",
                    task_id,
                    resume_session_id,
                )

        if restricted_tools or network_isolated or task_ssh_protected_paths:
            try:
                stable_skills = await read_stable_skills_snapshot()
                effective_config = await read_effective_config()
                require_no_ambient_instruction_config(effective_config)
                isolated_ambient_effective_config = copy.deepcopy(
                    effective_config
                )
                isolated_dynamic_disabled_features = (
                    _harden_ambient_feature_config(
                        effective_config,
                        thread_config,
                        tools_disabled=tools_disabled,
                    )
                )
                isolated_ambient_config_fingerprint = (
                    _canonical_json_fingerprint(
                        effective_config,
                        label="effective Codex configuration",
                    )
                )
                isolated_ambient_config_sections = {
                    key: _canonical_json_fingerprint(
                        value,
                        label=f"effective Codex configuration section {key}",
                    )
                    for key, value in effective_config.items()
                }
                inherited_mcp = _effective_mcp_inventory({
                    "config": effective_config,
                })
                if task_ssh_protected_paths or mcp_only:
                    collisions = sorted(
                        set(inherited_mcp) & set(explicit_mcp_servers)
                    )
                    if collisions:
                        raise ValueError(
                            "ambient MCP collides with an explicit CCM server"
                        )
                isolated_mcp = {
                    name: {"enabled": False}
                    for name in inherited_mcp
                }
                if task_ssh_protected_paths or mcp_only:
                    isolated_mcp.update(explicit_mcp_servers)
                thread_config["mcp_servers"] = isolated_mcp

                if stable_skills is None:
                    skills_inventory = await read_skills_inventory()
                    disabled_skills = _tool_free_disabled_skill_config(
                        skills_inventory,
                        cwd=cwd,
                    )
                    isolated_skills_inventory_fingerprint = (
                        _canonical_json_fingerprint(
                            disabled_skills,
                            label="Codex disabled skills inventory",
                        )
                    )
                    tool_free_skills_revision = self._skills_revision
                else:
                    (
                        disabled_skills,
                        isolated_skills_inventory_fingerprint,
                        tool_free_skills_revision,
                    ) = stable_skills
                    if self._skills_revision != tool_free_skills_revision:
                        raise ValueError(
                            "Codex skills changed after stable reconciliation"
                        )
                thread_config["skills"]["config"] = disabled_skills
            except Exception as exc:
                if task_ssh_protected_paths:
                    message = (
                        "Codex Task isolation could not audit and disable "
                        "ambient MCP/config/skills"
                    )
                elif network_isolated:
                    message = (
                        "Codex network-isolated profile could not audit "
                        "inherited MCP/config/skills"
                    )
                else:
                    message = (
                        "Codex tool-free profile could not audit ambient "
                        "instructions and inherited MCP/config/skills"
                    )
                raise CodexRequiredMcpPreTurnError(message) from exc
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
        if git_env or task_ssh_protected_paths or network_isolated:
            # Per-project git credentials must remain thread-scoped.  A global
            # app-server environment would leak one project's identity into
            # every other concurrently running task.
            from backend.services.task_agent_isolation import (
                task_model_tool_environment,
            )

            core_shell_environment = (
                task_model_tool_environment(os.environ)
                if task_ssh_protected_paths or network_isolated
                else {}
            )
            shell_environment = dict(core_shell_environment)
            shell_environment.update(dict(git_env or {}))
            if task_ssh_disable_network:
                shell_environment = {
                    key: value
                    for key, value in shell_environment.items()
                    if key in core_shell_environment
                    or key.upper() in {
                        "GIT_AUTHOR_NAME",
                        "GIT_AUTHOR_EMAIL",
                        "GIT_COMMITTER_NAME",
                        "GIT_COMMITTER_EMAIL",
                    }
                }
                shell_environment.update({
                    "GIT_TERMINAL_PROMPT": "0",
                    "GCM_INTERACTIVE": "never",
                    "GIT_CONFIG_GLOBAL": os.devnull,
                    "GIT_CONFIG_NOSYSTEM": "1",
                    "GH_PROMPT_DISABLED": "1",
                })
            if task_ssh_protected_paths:
                assert task_private_tmpdir is not None
                shell_environment.update({
                    "SSH_AUTH_SOCK": "",
                    "SSH_AGENT_PID": "",
                    "SSH_ASKPASS": "",
                    "TMPDIR": str(task_private_tmpdir.path),
                    "TMP": str(task_private_tmpdir.path),
                    "TEMP": str(task_private_tmpdir.path),
                    # Status/diff must not refresh the exact read-only index.
                    # Git write operations still fail at the filesystem
                    # boundary even if model code unsets this convenience.
                    "GIT_OPTIONAL_LOCKS": "0",
                })
                if task_ssh_disable_network:
                    shell_environment["CCM_TASK_SSH_GUARD"] = "1"
            elif network_isolated:
                assert task_private_tmpdir is not None
                shell_environment.update({
                    "GIT_TERMINAL_PROMPT": "0",
                    "GCM_INTERACTIVE": "never",
                    "GIT_CONFIG_GLOBAL": os.devnull,
                    "GIT_CONFIG_NOSYSTEM": "1",
                    "GH_PROMPT_DISABLED": "1",
                    "GIT_OPTIONAL_LOCKS": "0",
                    "SSH_AUTH_SOCK": "",
                    "SSH_AGENT_PID": "",
                    "SSH_ASKPASS": "",
                    "TMPDIR": str(task_private_tmpdir.path),
                    "TMP": str(task_private_tmpdir.path),
                    "TEMP": str(task_private_tmpdir.path),
                })
            # Every Task starts from Codex's minimal core environment. Exact
            # project Git variables are reintroduced through ``set``; Manager
            # Git/GitHub/SSH and provider credentials can never leak from the
            # long-lived app-server process into a model shell.
            thread_config["shell_environment_policy"] = {
                "inherit": "none",
                "ignore_default_excludes": False,
                "exclude": list(_TASK_SHELL_ENV_EXCLUDES),
                "include_only": [],
                "experimental_use_profile": False,
                "set": shell_environment,
            }

        isolated_expected_config: dict[str, Any] | None = None
        isolated_permission_profile_id: str | None = None
        isolated_expected_permission_profile: dict[str, Any] | None = None
        isolated_disabled_features: frozenset[str] | None = None
        if restricted_tools:
            isolated_permission_profile_id = tool_free_permission_profile
            isolated_expected_permission_profile = (
                _tool_free_permission_config()
            )
            isolated_disabled_features = _TOOL_FREE_DISABLED_FEATURES
        elif network_isolated:
            isolated_permission_profile_id = network_permission_profile
            isolated_expected_permission_profile = (
                _network_isolated_permission_config(
                    cwd=cwd,
                    git_read_paths=task_git_read_paths,
                    private_tmpdir=str(task_private_tmpdir.path),
                    runtime_read_paths=self._runtime_read_paths,
                )
            )
            isolated_disabled_features = _NETWORK_ISOLATED_DISABLED_FEATURES
        elif task_ssh_protected_paths:
            isolated_permission_profile_id = task_permission_profile
            isolated_expected_permission_profile = _task_ssh_permission_config(
                cwd=cwd,
                protected_paths=task_ssh_protected_paths,
                allowed_read_paths=task_ssh_allowed_read_paths,
                git_read_paths=task_git_read_paths,
                private_tmpdir=str(task_private_tmpdir.path),
                disable_network=task_ssh_disable_network,
                managed_network_proxy=task_managed_network_proxy,
                sandbox_mode=sandbox_mode,
                runtime_read_paths=self._runtime_read_paths,
            )
            isolated_disabled_features = _NETWORK_ISOLATED_DISABLED_FEATURES
        if isolated_permission_profile_id is not None:
            if isolated_ambient_effective_config is None:
                raise CodexRequiredMcpPreTurnError(
                    "Codex isolated ambient merge inputs were not captured"
                )
            try:
                _harden_ambient_shell_environment_policy(
                    isolated_ambient_effective_config,
                    thread_config,
                )
                _audit_ambient_isolation_merge_inputs(
                    isolated_ambient_effective_config,
                    thread_config,
                    cwd=cwd,
                    permission_profile_id=isolated_permission_profile_id,
                    explicit_mcp_servers=frozenset(explicit_mcp_servers),
                )
            except (TypeError, ValueError) as exc:
                raise CodexRequiredMcpPreTurnError(
                    "Codex isolated ambient configuration could not be "
                    "safely overridden"
                ) from exc
            isolated_disabled_features = (
                isolated_disabled_features
                | isolated_dynamic_disabled_features
            )
            isolated_expected_config = copy.deepcopy(thread_config)

        def audit_isolated_thread_config() -> None:
            if isolated_permission_profile_id is None:
                return
            assert isolated_expected_config is not None
            assert isolated_expected_permission_profile is not None
            assert isolated_disabled_features is not None
            try:
                _audit_isolated_request_config(
                    thread_config,
                    expected_config=isolated_expected_config,
                    permission_profile_id=isolated_permission_profile_id,
                    expected_permission_profile=(
                        isolated_expected_permission_profile
                    ),
                    disabled_features=isolated_disabled_features,
                    network_proxy_enabled=bool(
                        task_ssh_protected_paths
                        and task_managed_network_proxy
                    ),
                )
            except (TypeError, ValueError) as exc:
                raise CodexRequiredMcpPreTurnError(
                    "Codex isolated request configuration failed local audit"
                ) from exc

        audit_isolated_thread_config()

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
        if restricted_tools:
            # Clear config-level developer instructions. Project/user
            # instruction files are independently proven absent through
            # ``instructionSources`` in the thread response. Do not also set
            # the type-safe ``permissions`` selector here: Codex 0.144.6
            # resolves it before the request-local profile table. The config
            # layer above defines and selects the profile atomically.
            common["baseInstructions"] = ""
            common["developerInstructions"] = ""
        elif not task_ssh_protected_paths and not network_isolated:
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
        if restricted_tools and not resume_session_id:
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
        audit_isolated_thread_config()
        require_stable_task_filesystem_boundary("before thread admission")
        await require_stable_isolated_ambient_state("before thread admission")
        require_isolated_runtime_generation("before thread admission")
        require_managed_network_generation("before thread admission")
        try:
            response = await self._request(
                thread_method,
                thread_params,
                expected_process=isolated_process,
            )
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
            if task_resume_runtime_recycled:
                # The Task resume already consumed its single safe pre-turn
                # recycle. Repeating it would hide a persistent terminal
                # state instead of proving a fresh isolated runtime.
                raise CodexThreadTerminalStateError(
                    thread_id,
                    status_type,
                    operation=f"{thread_method} turn admission",
                    recovery_attempted=True,
                    detail="fresh isolated resume remained terminal",
                )
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
                require_managed_network_generation(
                    "during terminal thread recovery"
                )
                response = await self._request(
                    thread_method,
                    thread_params,
                    expected_process=isolated_process,
                )
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
        if restricted_tools:
            try:
                _audit_tool_free_thread_response(
                    response,
                    permission_profile_id=str(tool_free_permission_profile),
                )
                # Drain notifications already queued behind thread/start. A
                # changed inventory invalidates the exact path deny-list and
                # must fail before turn/start sends model input.
                await asyncio.sleep(0)
                if (
                    tool_free_skills_revision is None
                    or self._skills_revision != tool_free_skills_revision
                ):
                    raise _CodexIsolatedSkillsDriftError(
                        "skills inventory changed during admission"
                    )
            except _CodexIsolatedSkillsDriftError as exc:
                return await retry_isolated_admission_after_skills_drift(
                    thread_id,
                    exc,
                )
            except (TypeError, ValueError) as exc:
                raise CodexRequiredMcpPreTurnError(
                    "Codex tool-free profile was not proven by the "
                    f"{thread_method} response"
                ) from exc
        elif network_isolated:
            try:
                _audit_network_isolated_thread_response(
                    response,
                    cwd=cwd,
                    permission_profile_id=str(network_permission_profile),
                )
            except (TypeError, ValueError) as exc:
                raise CodexRequiredMcpPreTurnError(
                    "Codex network-isolated profile was not proven by the "
                    f"{thread_method} response"
                ) from exc
        elif task_ssh_protected_paths:
            try:
                _audit_task_ssh_thread_response(
                    response,
                    permission_profile_id=str(task_permission_profile),
                    disable_network=task_ssh_disable_network,
                    managed_network_proxy=task_managed_network_proxy,
                    sandbox_mode=sandbox_mode,
                    cwd=cwd,
                    private_tmpdir=str(task_private_tmpdir.path),
                )
                await asyncio.sleep(0)
                if (
                    tool_free_skills_revision is None
                    or self._skills_revision != tool_free_skills_revision
                ):
                    raise _CodexIsolatedSkillsDriftError(
                        "skills inventory changed during Task admission"
                    )
            except _CodexIsolatedSkillsDriftError as exc:
                return await retry_isolated_admission_after_skills_drift(
                    thread_id,
                    exc,
                )
            except (TypeError, ValueError) as exc:
                raise CodexRequiredMcpPreTurnError(
                    "Codex Task isolation profile was not proven by the "
                    f"{thread_method} response"
                ) from exc
        isolated_instruction_sources: tuple[tuple[Any, ...], ...] | None = None
        if restricted_tools or network_isolated or task_ssh_protected_paths:
            try:
                isolated_instruction_sources = _instruction_sources_snapshot(
                    response,
                    cwd=cwd,
                )
                if tools_disabled and isolated_instruction_sources:
                    raise ValueError(
                        "tool-free execution loaded project instructions"
                    )
            except (OSError, TypeError, ValueError) as exc:
                raise CodexRequiredMcpPreTurnError(
                    "Codex isolated instruction sources were not proven by "
                    f"the {thread_method} response"
                ) from exc

        def require_stable_instruction_sources(stage: str) -> None:
            if isolated_instruction_sources is None:
                return
            try:
                current = _instruction_sources_snapshot(response, cwd=cwd)
            except (OSError, TypeError, ValueError) as exc:
                raise CodexRequiredMcpPreTurnError(
                    "Codex isolated instruction sources could not be re-audited "
                    + stage
                ) from exc
            if current != isolated_instruction_sources:
                raise CodexRequiredMcpPreTurnError(
                    "Codex isolated instruction sources changed " + stage
                )
        require_stable_task_filesystem_boundary("after thread admission")
        require_isolated_runtime_generation("after thread admission")
        require_managed_network_generation("after thread admission")
        # Re-read before publishing the native id. Codex 0.147 may finish
        # background plugin discovery only after the first thread/start. If
        # that changes the exact skill deny-list, compensate the still-empty
        # thread and repeat admission once with a newly stabilized inventory.
        try:
            await require_stable_isolated_ambient_state(
                "before turn ownership"
            )
        except _CodexIsolatedSkillsDriftError as exc:
            return await retry_isolated_admission_after_skills_drift(
                thread_id,
                exc,
            )
        require_isolated_runtime_generation("before turn ownership")
        require_managed_network_generation("before turn ownership")
        require_stable_instruction_sources("before turn ownership")
        self._known_threads.add(thread_id)
        if on_thread_started is not None:
            # A caller that owns durable lifecycle state binds the exact native
            # identity after all pure admission checks and before model input.
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
                and not restricted_tools
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
        if (
            effective_service_tier is None
            and rpc_service_tier == CODEX_SERVICE_TIER_PRIORITY
            and actual_tier_proxy is not None
            and self._require_actual_tier_proof
        ):
            # Codex 0.147 custom-provider thread/start can accept the tier in
            # the request while omitting it from the response.  This value is
            # provisional only: the turn remains unpublished until the exact
            # proxy lineage proves the upstream response's actual tier below.
            effective_service_tier = rpc_service_tier
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
        if task_private_tmpdir is not None:
            task_private_tmpdir.bind_to_runtime()
            turn_process.set_runtime_cleanup(task_private_tmpdir.cleanup)

        async def finish_prepared_turn(
            returncode: int,
            stderr: str,
            *,
            termination_kind: str | None = None,
        ) -> None:
            """Settle this prepared adapter and its exact scratch barrier."""

            turn_process.finish(
                returncode,
                stderr,
                termination_kind=termination_kind,
            )
            await turn_process.wait_runtime_cleanup()

        client_user_message_id = uuid.uuid4().hex
        context = _TurnContext(
            thread_id=thread_id,
            process=turn_process,
            launch_started=launch_started,
            task_id=task_id,
            client_user_message_id=client_user_message_id,
            tools_disabled=tools_disabled,
            mcp_only=mcp_only,
            allowed_mcp_tools=frozenset(
                (spec.name, tool)
                for spec in mcp_specs
                for tool in spec.enabled_tools
            ) if mcp_only else frozenset(),
            allowed_mcp_servers=(
                frozenset({
                    *explicit_mcp_servers,
                    # Codex 0.147 exposes its bundled, runtime-projected
                    # code-mode host as the internal MCP server ``codex``.
                    # Task isolation has already disabled ambient user/project
                    # MCP and verified the effective inventory, so this name
                    # cannot be supplied by an untrusted config.  It is
                    # admitted only for the filesystem-isolated Task profile;
                    # MCP-only Browser and tool-free review profiles retain
                    # their exact external-server allowlists.
                    *(("codex",) if task_ssh_protected_paths and not mcp_only else ()),
                })
                if task_ssh_protected_paths or mcp_only
                else None
            ),
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
        if (
            (
                restricted_tools
                or network_isolated
                or bool(task_ssh_protected_paths)
            )
            and (
                tool_free_skills_revision is None
                or self._skills_revision != tool_free_skills_revision
            )
        ):
            # Recheck the inventory generation at the final pure-preflight
            # boundary before publishing durable launch ownership.
            reason = "Codex isolated skills inventory changed before turn/start"
            self._detach_turn_context(context)
            await finish_prepared_turn(1, reason)
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
        if restricted_tools:
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
        elif (
            sandbox_mode == "workspace-write"
            and not task_ssh_protected_paths
            and not network_isolated
        ):
            turn_params["sandboxPolicy"] = {
                "type": "workspaceWrite",
                "writableRoots": [os.path.abspath(cwd)],
                "networkAccess": False,
                "excludeTmpdirEnvVar": False,
                "excludeSlashTmp": False,
            }
        if on_turn_prepared is not None:
            try:
                # All fallible pure preflight is complete. Publish the exact
                # adapter immediately before either Goal steering or
                # turn/start can send model input.
                await on_turn_prepared(turn_process, thread_id)
            except BaseException:
                self._detach_turn_context(context)
                await finish_prepared_turn(
                    1,
                    "Codex turn ownership preparation failed",
                )
                raise
        if restricted_tools or network_isolated or task_ssh_protected_paths:
            try:
                # The ownership callback is awaited and may commit durable
                # launch state. Recheck once more before model input so an
                # inventory change during that await still fails closed.
                require_isolated_runtime_generation(
                    "while publishing launch ownership"
                )
                require_managed_network_generation(
                    "while publishing launch ownership"
                )
                await require_stable_isolated_ambient_state(
                    "while publishing launch ownership"
                )
            except BaseException:
                self._detach_turn_context(context)
                await finish_prepared_turn(
                    1,
                    "Codex isolated ambient state changed before turn/start",
                )
                raise
        if (
            (
                restricted_tools
                or network_isolated
                or bool(task_ssh_protected_paths)
            )
            and (
                tool_free_skills_revision is None
                or self._skills_revision != tool_free_skills_revision
            )
        ):
            # This is not retryable preflight: the ownership callback has
            # already crossed the durable ``launching`` boundary. It is a
            # final TOCTOU safety invariant so a skills change during that
            # awaited commit cannot widen a tool-free turn.
            reason = (
                "Codex isolated skills inventory changed while publishing "
                "launch ownership"
            )
            self._detach_turn_context(context)
            await finish_prepared_turn(1, reason)
            raise CodexRequiredMcpPreTurnError(reason)
        try:
            require_stable_task_filesystem_boundary(
                "while publishing launch ownership"
            )
            require_stable_instruction_sources(
                "while publishing launch ownership"
            )
        except BaseException:
            self._detach_turn_context(context)
            await finish_prepared_turn(
                1,
                "Codex Task Git/TMP boundary changed before turn/start",
            )
            raise
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
                await finish_prepared_turn(
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

        turn_request = asyncio.create_task(self._request(
            "turn/start",
            turn_params,
            expected_process=isolated_process,
        ))
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
            await finish_prepared_turn(
                1,
                "Codex app-server rejected turn/start",
            )
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
            await finish_prepared_turn(
                1,
                "Codex app-server turn/start returned no turn id",
            )
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
        if turn_process.returncode is not None:
            # A response-first malformed terminal can close the adapter while
            # the turn/start RPC response is still in flight. Do not resurrect
            # that failed context by binding the later submission id.
            self._detach_turn_context(context)
            if turn_cancelled:
                raise asyncio.CancelledError
            return turn_process, str(thread_id)
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
        self,
        method: str,
        params: dict[str, Any] | None,
        *,
        expected_process: asyncio.subprocess.Process | None = None,
    ) -> dict[str, Any]:
        process = self._process
        if expected_process is not None and process is not expected_process:
            raise CodexAppServerError(
                "app-server process generation changed before request"
            )
        if (
            process is None
            or process.returncode is not None
            or process.stdin is None
        ):
            raise CodexAppServerError("app-server is not running")
        self._request_id += 1
        request_id = self._request_id
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = (process, future)
        try:
            message: dict[str, Any] = {"id": request_id, "method": method}
            if params is not None:
                message["params"] = params
            await self._write(message, expected_process=process)
            response = await asyncio.wait_for(future, timeout=self.request_timeout)
        finally:
            # Cancellation can happen while waiting for the shared write lock
            # or draining stdin, before the response wait is entered. Never
            # leave that future permanently registered.
            pending = self._pending.get(request_id)
            if pending is not None and pending[1] is future:
                self._pending.pop(request_id, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                # A process finalizer can fail this future while the request
                # is still queued on the write lock. Retrieve that exact
                # exception before propagating the generation-write failure.
                future.exception()
        if "error" in response:
            error = response.get("error") or {}
            raise CodexAppServerRequestError(
                f"{method} failed: {error.get('message') or error}"
            )
        result = response.get("result")
        return result if isinstance(result, dict) else {}

    async def _notify(self, method: str, params: dict[str, Any]) -> None:
        await self._write({"method": method, "params": params})

    async def _write(
        self,
        message: dict[str, Any],
        *,
        expected_process: asyncio.subprocess.Process | None = None,
    ) -> None:
        payload = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        async with self._write_lock:
            process = self._process
            if (
                expected_process is not None
                and (
                    process is not expected_process
                    or expected_process.returncode is not None
                )
            ):
                raise CodexAppServerError(
                    "app-server process generation changed before request write"
                )
            if process is None or process.stdin is None:
                raise CodexAppServerError("app-server stdin is unavailable")
            process.stdin.write(payload.encode("utf-8") + b"\n")
            await process.stdin.drain()

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
        for request_id, (pending_process, future) in list(
            self._pending.items()
        ):
            if pending_process is not process:
                continue
            self._pending.pop(request_id, None)
            if not future.done():
                future.set_exception(error)
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
                    pending = self._pending.get(request_id)
                    if pending is None or pending[0] is not process:
                        continue
                    self._pending.pop(request_id, None)
                    future = pending[1]
                    if not future.done():
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
                if context.tools_disabled or context.mcp_only:
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
                        if lineage_context.tools_disabled or lineage_context.mcp_only:
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
        if method == "turn/completed":
            terminal_context = context
            if terminal_context is None and thread_id_str is not None:
                terminal_context = self._contexts_by_thread.get(thread_id_str)
                if (
                    terminal_context is not None
                    and terminal_context.client_user_message_id
                    and notification_client_ids
                    and terminal_context.client_user_message_id
                    not in notification_client_ids
                ):
                    # Preserve the same contradictory-clientId rule used for
                    # mapped aliases. A different input's terminal is not
                    # evidence that this adapter failed.
                    logger.error(
                        "Ignoring Codex terminal with mismatched client input "
                        "thread=%s turn=%s expected_client=%s "
                        "actual_clients=%s",
                        thread_id,
                        turn_id,
                        terminal_context.client_user_message_id,
                        sorted(notification_client_ids),
                    )
                    return
            if terminal_context is not None:
                terminal_is_correlated = self._turn_terminal_correlates_context(
                    terminal_context,
                    params,
                )
                malformed_reason = self._malformed_turn_terminal_reason(
                    terminal_context,
                    params,
                )
                if malformed_reason is not None:
                    if terminal_is_correlated:
                        self._fail_malformed_turn_terminal(
                            terminal_context,
                            params,
                            malformed_reason,
                        )
                    else:
                        self._schedule_uncorrelated_turn_terminal_abort(
                            terminal_context,
                            params,
                            malformed_reason,
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
        if context.allowed_mcp_servers is not None:
            mcp_item: Any = None
            if method in {"item/started", "item/completed"}:
                candidate = params.get("item")
                if (
                    isinstance(candidate, dict)
                    and candidate.get("type") == "mcpToolCall"
                ):
                    mcp_item = candidate
            elif method.startswith("item/mcpToolCall/"):
                candidate = params.get("item")
                mcp_item = candidate if isinstance(candidate, dict) else params
            if isinstance(mcp_item, dict):
                reported_servers = {
                    value
                    for key in ("server", "serverName", "server_name")
                    if isinstance((value := mcp_item.get(key)), str)
                    and value
                }
                if (
                    len(reported_servers) != 1
                    or not reported_servers.issubset(
                        context.allowed_mcp_servers
                    )
                ):
                    self._schedule_tool_free_violation(
                        context,
                        "MCP server "
                        + (
                            repr(sorted(reported_servers))
                            if reported_servers
                            else "with missing identity"
                        ),
                    )
                    return
        if context.tools_disabled or context.mcp_only:
            if method in {"item/started", "item/completed"}:
                item = params.get("item")
                item_type = (
                    item.get("type")
                    if isinstance(item, dict)
                    else None
                )
                allowed_item_types = _TOOL_FREE_PASSIVE_ITEM_TYPES
                if context.mcp_only:
                    allowed_item_types = allowed_item_types | {"mcpToolCall"}
                if item_type not in allowed_item_types:
                    self._schedule_tool_free_violation(
                        context,
                        f"{method} item type {item_type!r}",
                    )
                    return
                if context.mcp_only and item_type == "mcpToolCall":
                    assert isinstance(item, dict)
                    identity = (item.get("server"), item.get("tool"))
                    if identity not in context.allowed_mcp_tools:
                        self._schedule_tool_free_violation(
                            context,
                            f"{method} unbound MCP tool {identity!r}",
                        )
                        return
                    item_id = item.get("id")
                    if not isinstance(item_id, str) or not item_id:
                        self._schedule_tool_free_violation(
                            context,
                            f"{method} MCP item has no stable id",
                        )
                        return
                    if method == "item/started":
                        context.active_mcp_item_ids.add(item_id)
                    else:
                        context.active_mcp_item_ids.discard(item_id)
            elif method.startswith(
                _TOOL_FREE_FORBIDDEN_NOTIFICATION_PREFIXES
            ):
                if context.mcp_only and method.startswith("item/mcpToolCall/"):
                    item_id = params.get("itemId")
                    if (
                        not isinstance(item_id, str)
                        or item_id not in context.active_mcp_item_ids
                    ):
                        self._schedule_tool_free_violation(
                            context,
                            f"notification {method} for unknown MCP item",
                        )
                        return
                else:
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
                (
                    CodexThreadNotIdleError,
                    CodexThreadTerminalStateError,
                    CodexThreadRuntimeRecycleError,
                    CodexThreadRuntimeRecycleCancelled,
                ),
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
                    if server.has_other_live_turn_processes(process):
                        # Explicit stop currently requires recycling the whole
                        # account transport to prove task-scoped MCP helpers
                        # are gone.  Never interrupt first and discover peers
                        # afterwards: recycling here would turn an unrelated
                        # peer with emitted output/external effects into an
                        # unreplayable failure.
                        raise CodexSharedTransportBusyError(
                            "Cannot stop the claimed turn while another live "
                            "turn shares its Codex app-server transport: "
                            f"{home}"
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
                    raise CodexSharedTransportBusyError(
                        "Cannot recycle the claimed turn because another live "
                        "turn appeared on its Codex app-server transport: "
                        f"{home}"
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

        if server.has_other_live_turn_processes(process):
            # The abandoned admission is uncertain, but killing the account
            # transport would also kill an unrelated live Task.  Keep this
            # home draining so no new turn can enter and require explicit
            # reconciliation of the abandoned generation instead.
            raise CodexSharedTransportBusyError(
                "Cannot shut down an unclaimed Codex turn while another live "
                "turn shares its app-server transport: "
                f"{home}"
            )

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
