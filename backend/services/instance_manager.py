import asyncio
import concurrent.futures
import hashlib
import inspect
import json
import logging
import os
import stat
import re
import secrets
import signal
import threading
import time
import weakref
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Sequence

from sqlalchemy import exists, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.instance import Instance
from backend.models.task import Task
from backend.models.worker_task_termination import (
    WorkerTaskTerminationReceipt,
)

from backend.models.log_entry import LogEntry
from backend.services.context_compaction import (
    build_compacted_resume_prompt,
    read_codex_rollout_last_usage,
)
from backend.services.cancellation import (
    await_task_completion,
    consume_current_task_cancellation,
    finish_awaitable,
)
from backend.services.codex_models import clamp_codex_effort
from backend.services.process_identity import (
    capture_process_identity,
    persisted_process_is_definitively_dead,
)
from backend.services.process_safety import require_safe_process_group_id
from backend.services.stream_parser import (
    StreamParser,
    detect_assistant_protocol_anomaly,
)
from backend.services.task_queue import task_retry_not_superseded_predicate
from backend.services.trusted_runtime import prime_trusted_runtime
from backend.services.worker_routing_config import (
    has_pending_worker_routing,
)
from backend.services.worker_task_termination import (
    active_worker_task_termination_receipt,
    no_active_worker_task_termination_predicate,
    worker_task_runtime_persistence_predicate,
    worker_task_termination_authority_predicate,
    worker_task_termination_authority_matches,
)
from backend.services.ws_broadcaster import WebSocketBroadcaster

# Freeze standalone Task hook/MCP entrypoints while the Manager runtime itself
# is loading, before any writable Agent checkout can influence later launches.
prime_trusted_runtime()

if TYPE_CHECKING:
    from backend.services.mcp_config import McpServerSpec
    from backend.services.task_runtime_secrets import PrivateTaskTempDir

logger = logging.getLogger(__name__)

# Linux rejects a single argv entry around 128 KiB (MAX_ARG_STRLEN), before
# Claude Code can apply its own context-window guard. Keep ample room for
# encoding/platform variance and send only large direct-Claude prompts over
# stdin; ordinary launches retain their established argv shape.
_CLAUDE_PROMPT_STDIN_THRESHOLD_BYTES = 64 * 1024


async def _fence_worker_runtime_mutation(
    db: AsyncSession,
    *,
    producer: str,
) -> bool:
    """Serialize exact-generation persistence with the final runtime seal.

    PTY callbacks can outlive the foreground Task consumer and are not reached
    by the ordinary Task/Instance lifecycle locks. During phase-one drain this
    fence remains open only so callers can finish a *full* durable
    incarnation/retry/turn/runtime-generation CAS in the same transaction.
    Taking the node-control row first makes that write visible to backfill or
    rejects it after the exact phase-two seal. Manager databases retain the
    existing no-op behavior.
    """

    from backend.services.worker_node_control import (
        WorkerNodeDrainingConflict,
        fence_worker_node_runtime_persistence,
    )

    try:
        await fence_worker_node_runtime_persistence(db)
    except WorkerNodeDrainingConflict:
        await db.rollback()
        logger.info(
            "Dropping %s because the Worker runtime seal has committed",
            producer,
        )
        return False
    return True


async def _fence_worker_runtime_admission(
    db: AsyncSession,
    *,
    producer: str,
) -> bool:
    """Reject new PTY ownership as soon as phase-one drain commits."""

    from backend.services.worker_node_control import (
        WorkerNodeDrainingConflict,
        fence_worker_node_mutation,
    )

    try:
        await fence_worker_node_mutation(db)
    except WorkerNodeDrainingConflict:
        await db.rollback()
        logger.info(
            "Dropping %s because Worker node ownership admission is closed",
            producer,
        )
        return False
    return True


@dataclass(frozen=True, slots=True)
class _SshAgentSocketSnapshot:
    path: str | None
    device: int | None
    inode: int | None
    owner_uid: int | None

    @property
    def identity(
        self,
    ) -> tuple[str | None, int | None, int | None, int | None]:
        """Return the immutable identity retained across one logical turn."""

        return (self.path, self.device, self.inode, self.owner_uid)

    @classmethod
    def capture(cls, raw_path: object) -> "_SshAgentSocketSnapshot":
        if raw_path is None or raw_path == "":
            # Freeze the absence as well. A retry of this logical turn must
            # not pick up an agent that appeared after initial admission.
            return cls(None, None, None, None)
        if not isinstance(raw_path, str) or "\x00" in raw_path:
            raise LaunchSupersededError(
                "Host SSH agent socket path is invalid"
            )
        path = os.path.abspath(raw_path)
        if path != raw_path:
            raise LaunchSupersededError(
                "Host SSH agent socket path must be absolute"
            )
        try:
            info = os.lstat(path)
        except OSError as exc:
            raise LaunchSupersededError(
                "Host SSH agent socket is unavailable"
            ) from exc
        if (
            not stat.S_ISSOCK(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or info.st_uid != os.geteuid()
        ):
            raise LaunchSupersededError(
                "Host SSH agent socket is not a safe owned Unix socket"
            )
        return cls(path, info.st_dev, info.st_ino, info.st_uid)

    def assert_current(self) -> None:
        if self.path is None:
            return
        current = self.capture(self.path)
        if current != self:
            raise LaunchSupersededError(
                "Host SSH agent socket changed before provider admission"
            )


_EXPECTED_GENERATION_UNSET = object()
DEFAULT_TERMINAL_CONSUMER_TIMEOUT = 30.0
DEFAULT_CONSUMER_CANCEL_TIMEOUT = 5.0
TERMINAL_TASK_OPERATION_LOCK_POLL_SECONDS = 0.05
PTY_BACKGROUND_POLL_SECONDS = 5.0
PTY_BACKGROUND_MAX_SECONDS = 4 * 60 * 60
PTY_POST_EXIT_CHAT_GRACE_SECONDS = 30.0
# Keep the absolute chat-proof lease aligned with the maximum native
# background-work lifetime.  The watcher still retires a proof much sooner
# when the exact child/state has settled; this is only a final leak bound.
PTY_POST_EXIT_CHAT_HARD_TTL_SECONDS = PTY_BACKGROUND_MAX_SECONDS
_CLOUDROUTER_CLAUDE_AUTH_ENV_KEYS = (
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
)
_CLOUDROUTER_CODEX_AUTH_ENV_KEYS = (
    "OPENAI_API_KEY",
    "CODEX_API_KEY",
    "CLOUDROUTER_API_KEY",
    "APEX_CODEX_GATEWAY_KEY",
    "APEX_CODEX_API_KEY",
    "APEXROUTER_API_KEY",
    "APEXROUTER_CODEX_API_KEY",
)
_TASK_SSH_GIT_IDENTITY_ENV_KEYS = frozenset({
    "GIT_AUTHOR_NAME",
    "GIT_AUTHOR_EMAIL",
    "GIT_COMMITTER_NAME",
    "GIT_COMMITTER_EMAIL",
})
_TASK_SSH_SAFE_GIT_ENV = {
    "GIT_TERMINAL_PROMPT": "0",
    "GCM_INTERACTIVE": "never",
    "GIT_CONFIG_GLOBAL": os.devnull,
    "GIT_CONFIG_NOSYSTEM": "1",
    "GH_PROMPT_DISABLED": "1",
}
_DELIVERY_SAFE_GIT_ENV = {
    **_TASK_SSH_SAFE_GIT_ENV,
    # Delivery Developers may inspect the worktree but the Controller owns
    # every index/ref mutation and commit.
    "GIT_OPTIONAL_LOCKS": "0",
}
_DELIVERY_RUNTIME_TEMP_ENV_KEYS = frozenset({"TMPDIR", "TMP", "TEMP"})
_ACTUAL_TURN_PROVIDER_BY_TRANSPORT = {
    "claude_pty": "claude",
    "claude_exec": "claude",
    "codex_app_server": "codex",
    "codex_exec": "codex",
}
_ACTUAL_TURN_TRANSPORTS = frozenset(
    _ACTUAL_TURN_PROVIDER_BY_TRANSPORT
)
_SEQUENTIAL_TURN_TOKEN_TTL_SECONDS = 300.0
_TURN_FAILURE_EVENT_TYPE = "ccm.turn.failed"
_TURN_FAILURE_REASONS = frozenset(
    {"process_exit_before_response", "output_consumer_failure"}
)
_CLOUDROUTER_TRANSIENT_RE = re.compile(
    r"(?:API\s+Error|HTTP|status(?:\s+code)?|error|upstream)"
    r"[^\n]{0,120}(?:\b429\b|too many requests|rate[ _-]?limited)"
    r"|(?:\b429\b|too many requests|rate[ _-]?limited)"
    r"[^\n]{0,120}(?:API|HTTP|request|upstream|error)",
    re.IGNORECASE,
)


def _worker_termination_stop_predicates(
    worker_termination_operation_id: str | None,
    worker_termination_operation: str | None = None,
    worker_termination_execution_token: str | None = None,
    worker_termination_state_version: int | None = None,
    lease_valid_at: datetime | None = None,
) -> list:
    """Fence an Instance stop against durable Worker termination ownership.

    Ordinary lifecycle callers must see no active receipt.  The receipt
    executor may name its own operation, but ``allow_operation_id`` is only a
    negative exclusion: on its own it would also accept a stale or mistyped
    id after the real receipt disappeared.  Pair it with a positive exact
    Worker-side active-row proof in every stop transaction.
    """

    return [
        worker_task_termination_authority_predicate(
            operation_id=worker_termination_operation_id,
            operation=worker_termination_operation,
            execution_token=worker_termination_execution_token,
            state_version=worker_termination_state_version,
            lease_valid_at=lease_valid_at,
        )
    ]


def _worker_termination_instance_stop_predicate(
    worker_termination_operation_id: str | None,
    worker_termination_operation: str | None = None,
    worker_termination_execution_token: str | None = None,
    worker_termination_state_version: int | None = None,
    lease_valid_at: datetime | None = None,
):
    """Bind an Instance mutation to the receipt owning its current Task."""

    if worker_termination_operation_id is None:
        return ~exists(
            select(WorkerTaskTerminationReceipt.operation_id).where(
                WorkerTaskTerminationReceipt.active_task_id
                == Instance.current_task_id
            )
        )
    if lease_valid_at is None:
        return Instance.id != Instance.id
    return exists(
        select(WorkerTaskTerminationReceipt.operation_id).where(
            WorkerTaskTerminationReceipt.active_task_id
            == Instance.current_task_id,
            WorkerTaskTerminationReceipt.operation_id
            == worker_termination_operation_id,
            WorkerTaskTerminationReceipt.side == "worker",
            WorkerTaskTerminationReceipt.status == "executing",
            WorkerTaskTerminationReceipt.operation
            == worker_termination_operation,
            WorkerTaskTerminationReceipt.execution_token
            == worker_termination_execution_token,
            WorkerTaskTerminationReceipt.state_version
            == worker_termination_state_version,
            WorkerTaskTerminationReceipt.next_reconcile_at.is_not(None),
            WorkerTaskTerminationReceipt.next_reconcile_at
            > lease_valid_at,
        )
    )


async def _lock_worker_termination_stop_authority(
    db: AsyncSession,
    *,
    task_id: int,
    instance_id: int,
    task_predicates: Sequence[Any],
    instance_predicates: Sequence[Any],
    operation_id: str | None,
    operation: str | None,
    execution_token: str | None,
    state_version: int | None,
) -> datetime | None:
    """Lock Task -> receipt -> Instance, then sample and validate the lease."""

    task_lock = await db.execute(
        update(Task)
        .where(*task_predicates)
        .values(status=Task.status)
        .execution_options(synchronize_session=False)
    )
    if task_lock.rowcount != 1:
        await db.rollback()
        return None
    receipt = await active_worker_task_termination_receipt(
        db,
        task_id,
        for_update=True,
    )
    instance_lock = await db.execute(
        update(Instance)
        .where(*instance_predicates)
        .values(status=Instance.status)
        .execution_options(synchronize_session=False)
    )
    if instance_lock.rowcount != 1:
        await db.rollback()
        return None
    lease_valid_at = datetime.utcnow()
    if not worker_task_termination_authority_matches(
        receipt,
        operation_id=operation_id,
        operation=operation,
        execution_token=execution_token,
        state_version=state_version,
        lease_valid_at=lease_valid_at,
    ):
        await db.rollback()
        return None
    return lease_valid_at
_APEX_BUSY_TRANSIENT_RE = re.compile(
    r"(?:unexpected status\s+409|httpStatusCode[\"']?\s*:\s*409|"
    r"\bHTTP(?:/\d(?:\.\d)?)?\s+409\b|\b409\s+Conflict\b)"
    r"[^\n]{0,600}\b(?:all logged-in accounts are busy"
    r"|no eligible logged-in account is ready)\b"
    r"|\b(?:all logged-in accounts are busy"
    r"|no eligible logged-in account is ready)\b[^\n]{0,600}"
    r"(?:unexpected status\s+409|httpStatusCode[\"']?\s*:\s*409|"
    r"\bHTTP(?:/\d(?:\.\d)?)?\s+409\b|\b409\s+Conflict\b)",
    re.IGNORECASE,
)
_CLOUDROUTER_AUTH_RE = re.compile(
    r"\b401\b[^\n]{0,120}(?:unauthori[sz]ed|invalid|API[ _-]?key)"
    r"|\b403\b[^\n]{0,120}(?:forbidden|unauthori[sz]ed|API[ _-]?key)"
    r"|(?:invalid[ _-]?api[ _-]?key|API[ _-]?key[^\n]{0,80}invalid"
    r"|authentication_error|\bforbidden\b)",
    re.IGNORECASE,
)


async def _settle_instance_cleanup(awaitable):
    """Finish a lifecycle release before delivering caller cancellation."""

    return await finish_awaitable(awaitable)


class InstanceAlreadyRunningError(RuntimeError):
    """A second turn attempted to claim an occupied instance slot."""


class InstanceNotFoundError(InstanceAlreadyRunningError):
    """The reusable instance slot disappeared before launch was committed."""


class LaunchSupersededError(RuntimeError):
    """The task claim was cancelled or replaced while its agent was starting."""


class CodexLaunchCommitError(RuntimeError):
    """An app-server turn started but its CCM ownership commit did not finish."""


class ConsumerRecoveryUnsettledError(RuntimeError):
    """A crashed consumer could not durably settle its exact generation."""


class LiveAttachmentInjectionUnsupportedError(RuntimeError):
    """The active transport cannot safely access Manager upload paths."""


class ClaudeInjectionAdmissionUncertainError(RuntimeError):
    """A Claude live-injection write may have been accepted without a usable ACK."""


class SharedProjectAgentLaunchDisabledError(RuntimeError):
    """Agent execution is disabled for Projects visible to other users."""


async def _require_unshared_project_agent_launch(
    project_id: int | None,
    db_factory,
) -> None:
    """Fail closed while a Project has an active legacy cross-CCM share.

    Local TeamProjectShare ACLs do not alter the Task's execution principal or
    provider boundary and therefore are intentionally ignored here.
    """

    if project_id is None:
        return
    from backend.services.container_manager import is_shared_project

    try:
        shared = await is_shared_project(project_id, db_factory)
    except Exception as exc:
        raise SharedProjectAgentLaunchDisabledError(
            f"Could not verify sharing state for Project {project_id}; "
            "Agent launch is disabled"
        ) from exc
    if shared:
        raise SharedProjectAgentLaunchDisabledError(
            f"Agent launch is disabled while Project {project_id} is shared"
        )


def _terminal_failure_log_entry(
    *,
    instance_id: int,
    task_id: int,
    task_retry_count: int,
    task_turn_generation: int,
    provider: str,
    reason: str,
    exit_code: int | None,
    content: str,
) -> LogEntry:
    """Build one structured foreground veto for an exact failed turn."""

    normalized_provider = str(provider or "").strip().lower()
    if normalized_provider not in {"claude", "codex"}:
        raise ValueError(f"Unsupported terminal-failure provider: {provider!r}")
    if reason not in _TURN_FAILURE_REASONS:
        raise ValueError(f"Unsupported terminal-failure reason: {reason!r}")
    if exit_code is not None and type(exit_code) is not int:
        raise ValueError("Terminal-failure exit_code must be an integer or None")
    payload = {
        "type": _TURN_FAILURE_EVENT_TYPE,
        "version": 1,
        "provider": normalized_provider,
        "reason": reason,
        "exit_code": exit_code,
    }
    return LogEntry(
        instance_id=instance_id,
        task_id=task_id,
        task_retry_count=task_retry_count,
        task_turn_generation=task_turn_generation,
        turn_scope="foreground",
        event_type="system_event",
        role="system",
        content=content,
        raw_json=json.dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ),
        is_error=True,
    )


def _path_forms(value: str | None) -> tuple[str, ...]:
    """Return lexical and symlink-resolved absolute forms for a path."""

    if not isinstance(value, str) or not value.strip():
        return ()
    absolute = os.path.normcase(
        os.path.abspath(os.path.expanduser(value.strip()))
    )
    resolved = os.path.normcase(os.path.realpath(absolute))
    return (absolute,) if resolved == absolute else (absolute, resolved)


def _path_is_within(value: str | None, root: str | None) -> bool:
    for candidate in _path_forms(value):
        for boundary in _path_forms(root):
            try:
                if os.path.commonpath((candidate, boundary)) == boundary:
                    return True
            except ValueError:
                continue
    return False


def _is_conventional_delivery_workspace_path(value: str | None) -> bool:
    """Recognize the reserved ``.claude-manager/worktrees/delivery-*`` tree."""

    for candidate in _path_forms(value):
        parts = Path(candidate).parts
        for index in range(len(parts) - 2):
            if (
                parts[index] == ".claude-manager"
                and parts[index + 1] == "worktrees"
                and parts[index + 2].startswith("delivery-")
                and len(parts[index + 2]) > len("delivery-")
            ):
                return True
    return False


async def _task_has_protected_delivery_effect(
    db: AsyncSession,
    task_id: int,
) -> bool:
    """Return whether stopping this Task could mutate an owned workflow effect."""

    task = await db.get(Task, task_id)
    if task is None:
        # An Instance that names a missing Task has unresolved ownership.  A
        # default stop must not turn that uncertainty into an external-effect
        # cancellation.
        return True
    from backend.services.pr_review_runtime import is_pr_sandbox_task

    if (
        task.mode == "delivery_loop"
        or task.delivery_run_id is not None
        or is_pr_sandbox_task(task)
    ):
        return True

    # Durable reverse links remain authoritative even if an old client or a
    # partial migration stripped presentation tags/metadata from the Task.
    from backend.models.code_review import CodeReviewRun
    from backend.models.pr_monitor import (
        PRFindingAction,
        PRFindingRebuttal,
        PRReview,
        PRReviewerRun,
    )

    linked_queries = (
        select(CodeReviewRun.id).where(CodeReviewRun.reviewer_task_id == task_id),
        select(PRReview.id).where(PRReview.task_id == task_id),
        select(PRReviewerRun.id).where(PRReviewerRun.task_id == task_id),
        select(PRFindingAction.id).where(PRFindingAction.task_id == task_id),
        select(PRFindingRebuttal.id).where(PRFindingRebuttal.task_id == task_id),
    )
    for query in linked_queries:
        if (await db.execute(query.limit(1))).scalar_one_or_none() is not None:
            return True
    return False


async def _require_delivery_workspace_launch_boundary(
    db: AsyncSession,
    task: Task,
    *,
    cwd: str | None,
) -> bool:
    """Keep ordinary Tasks out of Controller-owned Delivery worktrees."""

    from backend.models.delivery import (
        DeliveryCycle,
        DELIVERY_TURN_ACTIVE_STATUSES,
        DeliveryRun,
        DeliveryTurn,
    )
    from backend.models.worktree import Worktree
    from backend.services.delivery_service import value_hash

    # ``worktrees`` is also the legacy registry for ordinary ``task-*``
    # isolation. Only rows with a durable Delivery owner reserve a path for
    # this boundary; treating every historical row as protected would block
    # normal auto Tasks from their own managed worktrees. The conventional
    # ``delivery-*`` path check below remains an independent fail-closed guard
    # for a missing or partially committed Delivery row.
    worktrees = list(
        (
            await db.execute(
                select(Worktree).where(
                    Worktree.delivery_run_id.is_not(None)
                )
            )
        )
        .scalars()
        .all()
    )
    candidates = (cwd, task.target_repo)
    registered_matches = {
        worktree.id
        for worktree in worktrees
        for candidate in candidates
        if _path_is_within(candidate, worktree.worktree_path)
    }
    protected = bool(registered_matches) or any(
        _is_conventional_delivery_workspace_path(candidate)
        for candidate in candidates
    )
    delivery_owned = (
        isinstance(task.mode, str)
        and task.mode == "delivery_loop"
    ) or type(task.delivery_run_id) is int
    if not protected:
        if delivery_owned:
            raise LaunchSupersededError(
                f"Delivery Task {task.id} is outside its managed worktree"
            )
        return False

    bound = next(
        (
            worktree
            for worktree in worktrees
            if worktree.task_id == task.id
            and worktree.delivery_run_id == task.delivery_run_id
        ),
        None,
    )
    run = (
        await db.get(DeliveryRun, task.delivery_run_id)
        if type(task.delivery_run_id) is int
        else None
    )
    cycle = (
        await db.get(DeliveryCycle, run.current_cycle_id)
        if run is not None and type(run.current_cycle_id) is int
        else None
    )
    active_turn = (
        (
            await db.execute(
                select(DeliveryTurn)
                .where(
                    DeliveryTurn.active_run_id == task.delivery_run_id,
                    DeliveryTurn.status.in_(DELIVERY_TURN_ACTIVE_STATUSES),
                )
                .limit(1)
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if type(task.delivery_run_id) is int
        else None
    )
    policy = run.policy_snapshot if run is not None else None
    exact_binding = bool(
        task.mode == "delivery_loop"
        and task.delivery_role == "developer"
        and bound is not None
        and bound.status == "active"
        and bound.cleanup_status == "retained"
        and run is not None
        and run.developer_task_id == task.id
        and run.worktree_id == bound.id
        and run.workspace_path == bound.worktree_path
        and run.phase == "coding"
        and run.activity == "running"
        and cycle is not None
        and cycle.run_id == run.id
        and cycle.active_run_id == run.id
        and cycle.status == "coding"
        and active_turn is not None
        and active_turn.run_id == run.id
        and active_turn.cycle_id == cycle.id
        and active_turn.generation == run.turn_count
        and active_turn.purpose == "code"
        and active_turn.task_id == task.id
        and active_turn.task_retry_count == task.retry_count
        and task.project_id == run.project_id
        and task.worker_id is None
        and task.shared_from_id is None
        and isinstance(policy, dict)
        and value_hash(policy) == run.policy_hash
        and policy.get("provider") in {"claude", "codex"}
        and task.provider == policy.get("provider")
        and task.model == policy.get("model")
        and task.codex_service_tier == policy.get("codex_service_tier")
        and task.effort_level == policy.get("effort_level")
        and not task.enable_workflows
        and not (task.enabled_skills or {})
        and _path_is_within(cwd, bound.worktree_path)
        and _path_is_within(task.last_cwd, bound.worktree_path)
        and all(
            not _is_conventional_delivery_workspace_path(candidate)
            or _path_is_within(candidate, bound.worktree_path)
            for candidate in candidates
        )
        and all(match_id == bound.id for match_id in registered_matches)
    )
    if not exact_binding:
        raise LaunchSupersededError(
            f"Task {task.id} is not the exact active owner of the requested "
            "Delivery worktree"
        )
    return True


@dataclass(frozen=True)
class _OutputConsumerRecord:
    """Identity of one output-bookkeeping generation for a reusable slot."""

    process: asyncio.subprocess.Process
    task: asyncio.Task
    chat_initiated: bool
    provider: str
    task_id: int | None = None
    task_retry_count: int | None = None
    task_turn_generation: int | None = None
    # Durable per-turn token. PTY hot reuse keeps the same native Session and
    # PID across many turns, so neither process identity nor PID alone can
    # distinguish a late exit callback from a newer turn on the same slot.
    instance_started_at: datetime | None = None
    # PTY is a persistent interactive process: an upstream API turn may abort
    # while the OS process remains healthy and therefore reports exit code 0.
    # Keep the semantic failure on this exact immutable turn generation.
    fatal_provider_error: str | None = None
    # PTY stop and consumer exit can meet while both need to settle the same
    # hot-session turn.  The first side to claim terminal ownership decides
    # the lock order: ``stop`` keeps the lifecycle lock and the consumer skips
    # DB finalization; ``consumer`` finalizes first and stop waits outside the
    # lock.  Compare/set is deliberately synchronous on the event-loop thread.
    pty_terminal_owner: str | None = None
    # Exact on_exit consumer is waiting for its Task/session background epoch.
    # A matching stop may take over this quiescent wait immediately instead of
    # burning the generic 30-second terminal-consumer timeout.
    pty_background_waiting: bool = False
    # Claude stream-json may emit the final assistant body once as an
    # ``assistant`` envelope and again in the terminal ``result`` envelope.
    # Keep terminal metadata but suppress only that exact repeated body.
    last_claude_assistant_text: str | None = None


@dataclass
class _PtyPostExitGeneration:
    """Exact PTY foreground generation retained across terminal handoff.

    ``FullMirrorCCMBackend.on_exit`` completes its proxy before dispatcher/Ralph
    commits the Task result, and a successful chat turn can release the same
    maps just before a late native child arms its detached background epoch.
    The ordinary instance-keyed process/consumer maps are intentionally
    released at that point, but an already-arriving callback or retained chat
    follow-up still needs immutable proof that it belongs to that exact
    Task/session/turn.  This record is never a reusable Instance-slot owner.
    """

    token: object
    instance_id: int
    task_id: int
    session_id: str
    session: Any
    process: Any
    record: _OutputConsumerRecord
    created_monotonic: float
    invalidated: bool = False
    watcher: asyncio.Task | None = None


@dataclass(frozen=True)
class _LaunchReservation:
    """Task identity held across the pre-owner subprocess launch window."""

    token: object
    task_id: int | None
    task_turn_generation: int | None
    previous_process: asyncio.subprocess.Process | None


@dataclass(frozen=True)
class _BrowserChildLaunchAdmission:
    """Immutable Browser-child authority captured by launch preflight.

    The durable binding is checked again at the final provider-effect
    boundary.  Retaining the exact preflight identity prevents a concurrent
    writer from replacing an otherwise self-consistent binding/Task profile
    between those two checks.
    """

    binding_id: str
    browser_review_job_id: str
    harness_run_id: str
    workspace_review_run_id: str | None
    launch_profile_version: int
    launch_config_digest: str
    owner_task_id: int
    owner_task_incarnation_id: str
    owner_task_retry_count: int
    owner_task_turn_generation: int
    owner_task_status: str
    child_task_id: int
    child_task_incarnation_id: str
    child_task_retry_count: int
    child_task_turn_generation: int
    claimed_instance_id: int


@dataclass(frozen=True)
class _SequentialTurnContinuation:
    """One-shot in-memory authority for the next turn of one live mode run."""

    token: object
    instance_id: int
    task_id: int
    task_retry_count: int
    task_turn_generation: int
    source_log_id: int
    actual_transport: str
    expires_at_monotonic: float


@dataclass(frozen=True)
class _ConsumerRecoveryEvidence:
    """Fail-closed evidence for one terminal consumer awaiting DB recovery."""

    error: BaseException
    tracked_generation: bool
    task_id: int | None
    task_retry_count: int | None
    task_turn_generation: int | None
    instance_pid: int | None
    instance_started_at: datetime | None
    consumer: asyncio.Task | None = None
    record: _OutputConsumerRecord | None = None


@dataclass(frozen=True)
class _ContextPreflightPermit:
    """Exact active Task snapshot allowed to compact one rejected Codex turn."""

    task_id: int
    status: str
    instance_id: int
    retry_count: int
    turn_generation: int
    turn_source_log_id: int
    session_id: str
    started_at: datetime | None
    completed_at: datetime | None


@dataclass
class _PtyBackgroundState:
    """One exact autonomous PTY epoch keyed by Task and native session."""

    task_id: int
    session_id: str
    generation: str
    task_retry_count: int
    task_turn_generation: int
    session: Any
    started_monotonic: float
    last_event_monotonic: float
    pending_tools: int = 0
    # User follow-ups may reuse the retained foreground Session while native
    # descendants are still draining.  The old background epoch must not
    # finalize (and tear down the shared consumer maps) until every such
    # follow-up has finished pumping its ordered provider events.
    pending_followups: int = 0
    terminal_seen: bool = False
    watcher: asyncio.Task | None = None
    done: asyncio.Event = field(default_factory=asyncio.Event)
    outcome: str | None = None
    accepting_events: bool = True
    watchdog_stopping: bool = False


@dataclass(frozen=True)
class _TaskLifecycleFence:
    """Duck-compatible immutable Task generation for routing side effects."""

    task_id: int
    worker_id: int | None
    shared_from_id: int | None
    retry_count: int
    turn_generation: int
    instance_id: int | None
    started_at: datetime | None
    completed_at: datetime | None
    # Routing from a queued chat freezes its pre-claim status. Dispatcher
    # lifecycles omit status because one generation advances in_progress →
    # executing without changing ownership.
    status: str | None = None


class InstanceManager:
    """Manages multiple Claude Code subprocess instances."""

    def __init__(
        self,
        db_factory,
        broadcaster: WebSocketBroadcaster,
        test_harness_service=None,
    ):
        self.db_factory = db_factory  # async_sessionmaker
        self.broadcaster = broadcaster
        # Resolved lazily to avoid importing the Harness service during module
        # initialization. Tests with an isolated database may inject their own
        # service; production reuses the process-global service.
        self.test_harness_service = test_harness_service
        # Wired by backend.main after Dispatcher construction. Keeping this
        # dependency explicit prevents background consumers (especially in
        # tests) from importing the process-global Dispatcher and enqueueing
        # work against an unrelated database.
        self.task_message_enqueuer = None
        # Dispatcher installs this optional terminal hook.  It is invoked only
        # after a completed Task's exact detached epoch is durably cleared, so
        # PR/share-style terminal consumers cannot observe partial output.
        self.pty_background_completion_handler = None
        # Injected by backend.main. Runtime lookup is path-only and never reads
        # or stores the API key in launch params, git env, or process argv.
        self.cloudrouter_store = None
        self.parser = StreamParser()
        self.processes: dict[int, asyncio.subprocess.Process] = {}
        self._tasks: dict[int, asyncio.Task] = {}  # instance_id -> consumer task
        # Keep process identity alongside the consumer: the instance id is a
        # reusable slot, so a late waiter for generation A must never consume
        # (or be poisoned by) generation B's bookkeeping failure.
        self._consumer_records: dict[int, _OutputConsumerRecord] = {}
        # A PTY foreground consumer may be waiting for an exact detached
        # background epoch. Follow-up prompts reuse that hot Session instead
        # of creating a second instance generation; keep their short-lived
        # event pumps separate from the terminal consumer so stop/reap can
        # still address the original process identity.
        self._pty_followup_tasks: dict[int, set[asyncio.Task]] = {}
        # The API may allocate a provisional follow-up id from the durable
        # retained-background marker before the Session route is inspected.
        # Keep the exact route selected under the lifecycle lock so a race
        # with a newly started foreground turn cannot make the API publish an
        # id for which no retained boundary will ever be emitted.
        self._pty_followup_operation_routes: weakref.WeakKeyDictionary[
            asyncio.Task[Any], dict[str, str | None]
        ] = weakref.WeakKeyDictionary()
        # Boundary receipts are idempotent by (Task, operation). Keep a small
        # in-process fence so retries cannot create duplicate history rows or
        # publish competing states.
        self._pty_followup_boundary_locks: dict[
            tuple[int, str], asyncio.Lock
        ] = {}
        # Holding the process object in the key also prevents Python object-id
        # reuse from ever mapping a very late failure onto a future process.
        self._consumer_errors: dict[
            tuple[int, asyncio.subprocess.Process], BaseException
        ] = {}
        # A terminal process is not a settled lifecycle when its recovery
        # transaction could not be confirmed.  Keep this exact generation
        # visible to admission, stop, and shutdown until a later lifecycle
        # call durably clears its Task/Instance ownership.
        self._consumer_recovery_pending: dict[
            tuple[int, asyncio.subprocess.Process],
            _ConsumerRecoveryEvidence,
        ] = {}
        # Reference-counted stop intents.  Multiple exact stop callers may
        # overlap while a terminal consumer finishes bookkeeping; one stale or
        # faster caller must not remove another caller's retry/relaunch fence.
        self._stopping: dict[int, int] = {}
        # Serialize process admission and stop cleanup for each reusable worker
        # slot.  API-level ``is_running`` checks are advisory; this lock is the
        # authoritative guard against two concurrent launches or stop→run map
        # replacement races on the same instance id.
        self._instance_lifecycle_locks: dict[int, asyncio.Lock] = {}
        # Monotonic admission generation per reusable slot. Two callers that
        # both observed the same settling consumer may race for the next turn;
        # exactly one may advance this token and the loser must return busy,
        # never wait through the winner and execute the prompt afterwards.
        self._instance_launch_generations: dict[int, int] = {}
        # A process can exist before Instance.current_task_id/PID is committed.
        # Keep the Task-visible reservation until launch either publishes its
        # durable reverse owner or proves the aborted generation fully reaped.
        self._launch_reservations: dict[int, _LaunchReservation] = {}
        # Claude Delivery uses one inode-fenced scratch leaf per exact Task
        # generation. Pending entries cover pre-spawn failures; active entries
        # stay bound to the exact process object until the process tree is
        # proven terminal and the output lifecycle removes the leaf.
        self._pending_private_runtime_tempdirs: dict[int, object] = {}
        self._active_private_runtime_tempdirs: dict[
            tuple[int, object], object
        ] = {}
        # Task-scoped Claude settings, frozen hook entrypoints, and MCP config
        # live in one private scope.  Direct turns own it until their exact
        # process generation is terminal; PTY turns transfer ownership to the
        # exact native Session, which can stay hot and run autonomous turns
        # after the visible foreground consumer exits.  Scope cleanup is
        # therefore owner-based rather than tied to a Dispatcher turn finally.
        self._task_runtime_scope_pending: set[int] = set()
        self._task_runtime_scope_direct_owners: dict[
            tuple[int, object], int
        ] = {}
        self._task_runtime_scope_pty_owners: dict[object, int] = {}
        # Goal/Loop intentionally contain multiple sequential provider turns
        # inside one Task generation.  An already-bound source may cross the
        # provider boundary again only with a one-shot token minted from the
        # immediately preceding exact successful process.  Tokens live solely
        # in this Manager object, so restart/lost-ACK recovery can never turn a
        # durable route string into replay authority.
        self._sequential_turn_continuations: dict[
            object,
            _SequentialTurnContinuation,
        ] = {}
        # instance_id -> provider credential home used for the current/recent
        # launch (CLAUDE_CONFIG_DIR for Claude, CODEX_HOME for Codex).  Retry
        # paths read this map to stay on the same account.
        self._config_dirs: dict[int, str] = {}
        self._container_tasks: dict[int, int] = {}  # instance_id -> project_id (if running in container)
        # Exact direct ``docker exec`` generation for each reusable slot.  A
        # host docker client can exit while its command remains alive in the
        # container, so process-group cleanup must prove both sides terminal.
        self._container_exec_processes: dict[
            int, asyncio.subprocess.Process
        ] = {}
        self._last_stderr: dict[int, str] = {}  # instance_id -> stderr from last run
        # Direct CLI can report a structured fatal provider result while the
        # OS process exits 0. Keep the semantic result by exact process
        # identity until the owning lifecycle reads it (bounded: one per slot).
        self._effective_exit_codes: dict[int, tuple[object, int]] = {}
        self._launch_params: dict[int, dict] = {}  # instance_id -> params for re-launch on rotation
        # instance_id -> consecutive transient-overload retry count. Survives
        # the in-place relaunch (launch() resets _launch_params, so this can't
        # live there); cleared on success / give-up / stop.
        self._transient_attempts: dict[int, int] = {}
        # instance_ids whose CURRENT turn emitted a transient server-side
        # 429/overload error event. Turn-scoped: reset at launch(), set in
        # _process_event. The reliable signal in PTY mode, where the aborted
        # turn still reports exit_code 0.
        self._transient_seen: set[int] = set()
        # PTY rate-limit detection: instance_ids whose current turn saw an
        # actionable rate_limit_event. Turn-scoped: reset at launch(), checked
        # after _wait_process in the chat path so dispatcher can rotate.
        self._pty_rate_limit_seen: set[int] = set()
        # Preserve the event payload (especially resetsAt) until the completed
        # PTY turn can migrate its session and quarantine the source account.
        # ``hard_limit`` distinguishes a plain-text exhausted banner from a
        # soft >=90% quota warning.
        self._pty_rate_limit_info: dict[int, dict] = {}
        # PTY 权限透传：request_id -> {session_id, task_id, tool_name, expires_at}
        # bridge HTTP 线程收到 CC 的权限请求后经 _loop 调度进事件循环
        self._pty_permissions: dict[str, dict] = {}
        # BridgeHub invokes the callback from an HTTP thread.  Track the exact
        # cross-thread Futures so an irreversible Worker drain can atomically
        # close admission, cancel queued callbacks, and await their retirement.
        self._pty_permission_callback_lock = threading.Lock()
        self._pty_permission_callback_futures: set[
            concurrent.futures.Future
        ] = set()
        self._pty_permission_callbacks_draining = False
        self._loop = None  # 主事件循环，lifespan 启动时注入
        # Codex persistent JSON-RPC backend.  Created lazily so Claude-only
        # deployments never start an extra process.
        self._codex_app_server = None
        # Relogin/delete reserves a canonical CODEX_HOME here as well as in
        # the app-server registry.  The manager-level gate also covers the
        # `codex exec` path when app-server is disabled or falls back.
        self._codex_home_maintenance: set[str] = set()
        self._codex_home_locks: dict[str, asyncio.Lock] = {}
        self._codex_exec_homes: dict[int, str] = {}
        # Non-task Codex subprocesses (goal evaluation and task distillation)
        # share the same credential-home and app-server admission barrier as
        # normal exec turns. Count by canonical home because they do not own a
        # reusable Instance slot and cannot safely be represented in
        # _codex_exec_homes.
        self._codex_ephemeral_home_users: dict[str, int] = {}
        # Direct CLI subprocesses start in their own POSIX session.  Remember
        # the exact process generation so stop can signal the whole process
        # group without ever targeting a later app-server/PTY generation that
        # reused the same Instance id.
        self._process_groups: dict[int, asyncio.subprocess.Process] = {}

        # PTY persistent-session backend (claude provider only).
        # Runtime-switchable: env USE_PTY_MODE is the boot default, the
        # /api/settings/runtime endpoint can flip it live (affects new
        # launches only; running sessions finish on their current path).
        self._pty_backend = None
        self._pty_enabled = False
        # ``claude_binary_override`` is currently injected by temporarily
        # wrapping the shared backend's build_config method. Every PTY launch
        # participates in this lock so an ordinary launch cannot observe
        # another instance's container-specific binary.
        self._pty_build_config_lock = asyncio.Lock()
        # FullMirrorCCMBackend.on_exit waits on this barrier before writing a
        # terminal state. PTY starts its consumer before InstanceManager can
        # commit `running`; without ordering, a very short turn can write idle
        # first and then be overwritten by the late running commit.
        self._pty_launch_barriers: dict[int, asyncio.Event] = {}
        # Claude PTY background work is Task/session scoped. Known foreground
        # native work keeps its exact Instance owner until the tail settles;
        # genuinely late turns on an already-completed Task are detached and
        # never touch a possibly reused Instance slot. The durable token plus
        # this registry prevents old sentinels from completing a newer turn.
        self._pty_background_states: dict[
            tuple[int, str], _PtyBackgroundState
        ] = {}
        self._pty_background_transition_locks: dict[
            tuple[int, str], asyncio.Lock
        ] = {}
        # Launch-time idle callbacks publish a synchronous handoff before
        # awaiting the transition lock. This lets on_exit observe an already
        # arrived autonomous event even when it currently owns that lock.
        self._pty_autonomous_activity_handoffs: dict[
            tuple[int, str], object
        ] = {}
        # Remember the immutable handoff observed by the exact coroutine that
        # is about to wait for ``pty_background_transition``.  Looking up only
        # by Task/session after the wait is insufficient: a successful stop
        # can clear the old token and a later callback can install a new one
        # under the same key (ABA), which must not revive the stopped Session.
        self._pty_autonomous_activity_handoff_owners: dict[
            tuple[asyncio.Task[Any], tuple[int, str]], object
        ] = {}
        self._pty_autonomous_activity_handoff_owner_callbacks: set[
            asyncio.Task[Any]
        ] = set()
        # A successful PTY consumer can release its process proxy before either
        # a lifecycle terminal commit or a late chat child handoff is visible.
        # Retain one immutable proof so only that exact Task/session/consumer
        # epoch can admit the callback or retained follow-up.
        self._pty_post_exit_generations: dict[
            tuple[int, str], _PtyPostExitGeneration
        ] = {}
        # Capture the exact proof visible when a callback synchronously notes
        # activity. Looking it up only after awaiting the transition lock would
        # let an old callback borrow a replacement proof under the same key.
        self._pty_autonomous_activity_post_exit_owners: dict[
            tuple[asyncio.Task[Any], tuple[int, str]],
            _PtyPostExitGeneration,
        ] = {}
        if settings.use_pty_mode:
            self.set_pty_mode(True)

    @property
    def pty_mode_enabled(self) -> bool:
        return self._pty_enabled and self._pty_backend is not None

    def is_pty_managed_turn(
        self,
        instance_id: int | None,
        process=None,
    ) -> bool:
        """Whether the exact current turn is owned by the PTY adapter."""

        if instance_id is None or self._pty_backend is None:
            return False
        current = process or self.processes.get(instance_id)
        if current is None:
            return False
        proxies = getattr(self._pty_backend, "_proxies", None)
        if isinstance(proxies, dict):
            return proxies.get(instance_id) is current
        # Compatibility for narrow test/older adapter doubles.
        return instance_id in getattr(self._pty_backend, "_sessions", {})

    def has_pty_session(self, session_id: str | None) -> bool:
        """Whether this native Claude session belongs to the PTY backend.

        Task.instance_id is a rotating worker claim and can be absent or stale
        when the injection endpoint runs, so injection routing must resolve by
        the native session id just like ``inject_pty_message`` itself.
        """

        if self._pty_backend is None or not session_id:
            return False
        if any(
            getattr(session, "session_id", None) == session_id
            for session in getattr(self._pty_backend, "_sessions", {}).values()
        ):
            return True
        # FullMirror releases the ordinary slot maps before a late native
        # child can publish its background marker.  Keep routing aware of the
        # exact retained Session during that handoff, but never treat a stale
        # or replaced proof as a live PTY owner.
        return any(
            proof.session_id == session_id
            and self._pty_post_exit_generation_is_current(proof)
            for proof in tuple(self._pty_post_exit_generations.values())
        )

    def _pty_post_exit_generation_is_current(
        self,
        proof: _PtyPostExitGeneration,
        *,
        instance_id: int | None = None,
        task_id: int | None = None,
        session_id: str | None = None,
        task_retry_count: int | None = None,
        task_turn_generation: int | None = None,
        allow_task_generation_drift: bool = False,
        background_generation: str | None = None,
        require_background_state: bool = False,
    ) -> bool:
        """Validate one immutable PTY post-exit proof without DB awaits.

        A proof may outlive the instance-keyed maps, but it may not survive a
        stop, launch reservation, same-slot replacement, native Session death,
        or an ABA identity mismatch.  Callers that handle background output
        additionally require the exact accepting background epoch.
        """

        key = (proof.task_id, proof.session_id)
        if self._pty_post_exit_generations.get(key) is not proof:
            return False
        if instance_id is not None and proof.instance_id != instance_id:
            return False
        if task_id is not None and proof.task_id != task_id:
            return False
        if session_id is not None and proof.session_id != session_id:
            return False
        if not allow_task_generation_drift:
            if (
                task_retry_count is not None
                and proof.record.task_retry_count != task_retry_count
            ):
                return False
            if (
                task_turn_generation is not None
                and proof.record.task_turn_generation != task_turn_generation
            ):
                return False
        if (
            proof.record.provider != "claude"
            or proof.record.task_id != proof.task_id
            or proof.record.task_retry_count is None
            or proof.record.task_turn_generation is None
            or proof.record.instance_started_at is None
            or proof.record.pty_terminal_owner != "consumer"
            or getattr(proof.session, "session_id", None) != proof.session_id
            or getattr(proof.session, "is_alive", True) is False
            or getattr(proof.process, "session", None) is not proof.session
            or proof.instance_id in self._stopping
            or proof.instance_id in self._launch_reservations
        ):
            return False

        # The maps may be absent after FullMirror cleanup, but if any of them
        # still has an entry it must be the exact old generation.  A new value
        # under the reusable key is an ABA replacement and rejects the proof.
        backend = self._pty_backend
        map_checks = (
            (self._consumer_records, proof.record),
            (self._tasks, proof.record.task),
            (self.processes, proof.process),
        )
        for mapping, expected in map_checks:
            current = mapping.get(proof.instance_id)
            if current is not None and current is not expected:
                return False
        if backend is not None:
            for mapping, expected in (
                (
                    getattr(backend, "_sessions", {}),
                    proof.session,
                ),
                (
                    getattr(backend, "_consumers", {}),
                    proof.record.task,
                ),
                (
                    getattr(backend, "_proxies", {}),
                    proof.process,
                ),
            ):
                current = mapping.get(proof.instance_id)
                if current is not None and current is not expected:
                    return False
        if any(
            pending_key[0] == proof.instance_id
            and pending_key[1] is proof.process
            for pending_key in self._consumer_recovery_pending
        ):
            return False

        if require_background_state:
            state = self._pty_background_states.get(key)
            if (
                state is None
                or not state.accepting_events
                or state.session is not proof.session
                or state.task_retry_count != proof.record.task_retry_count
                or state.task_turn_generation != proof.record.task_turn_generation
                or (
                    background_generation is not None
                    and state.generation != background_generation
                )
            ):
                return False
        return True

    def _finalize_pty_followup_pump(
        self,
        key: int,
        followup: asyncio.Task,
        background_state: _PtyBackgroundState,
    ) -> None:
        """Release one pump exactly once, including pre-start cancellation."""

        if getattr(followup, "_ccm_followup_finalized", False):
            return
        setattr(followup, "_ccm_followup_finalized", True)
        background_state.pending_followups = max(
            0,
            background_state.pending_followups - 1,
        )
        background_state.last_event_monotonic = time.monotonic()
        tasks = self._pty_followup_tasks.get(key)
        if tasks is not None:
            tasks.discard(followup)
            if not tasks:
                self._pty_followup_tasks.pop(key, None)

    def _record_pty_followup_operation_route(
        self,
        requested_operation_id: str | None,
        actual_operation_id: str | None,
    ) -> None:
        """Record the transport selected for one API provisional id.

        This is intentionally an in-memory handoff: the user-message audit is
        written immediately after the provider acknowledgement, while the
        retained boundary itself is persisted by the follow-up pump.  The
        caller consumes the entry synchronously, so no durable state depends
        on this map surviving a process restart.
        """

        if not requested_operation_id:
            return
        try:
            owner = asyncio.current_task()
        except RuntimeError:
            owner = None
        if owner is None:
            return
        routes = self._pty_followup_operation_routes.setdefault(owner, {})
        routes[requested_operation_id] = actual_operation_id

    def consume_pty_followup_operation_route(
        self,
        requested_operation_id: str | None,
    ) -> tuple[bool, str | None]:
        """Return and remove the exact route selected by a PTY injection.

        The boolean distinguishes a real foreground result of ``None`` from
        an older test double/manager that does not implement this handoff.
        """

        if not requested_operation_id:
            return False, None
        try:
            owner = asyncio.current_task()
        except RuntimeError:
            owner = None
        if owner is None:
            return False, None
        routes = self._pty_followup_operation_routes.get(owner)
        if routes is None or requested_operation_id not in routes:
            return False, None
        route = routes.pop(requested_operation_id)
        if not routes:
            self._pty_followup_operation_routes.pop(owner, None)
        return True, route

    async def persist_pty_followup_boundary(
        self,
        *,
        instance_id: int,
        task_id: int,
        task_retry_count: int,
        task_turn_generation: int,
        session_id: str,
        background_generation: str,
        followup_operation_id: str,
        state: str,
        post_exit_proof: _PtyPostExitGeneration | None = None,
    ) -> bool:
        """Persist one retained-PTY follow-up receipt independently.

        This is an audit of an already admitted provider side effect. It must
        survive a concurrent stop/retry that changes the mutable Task marker,
        so it deliberately keys the row by the admitted retry/turn identity
        instead of requiring the current ``pty_background_generation``. The
        operation id makes retries idempotent and the committed row lets a
        reconnect recover even when the live WebSocket publication was lost.
        """

        if state not in {"completed", "uncertain"} or not followup_operation_id:
            return False

        def proof_is_current() -> bool:
            if post_exit_proof is None:
                return True
            return bool(
                not post_exit_proof.invalidated
                and self._pty_post_exit_generation_is_current(
                    post_exit_proof,
                    instance_id=instance_id,
                    task_id=task_id,
                    session_id=session_id,
                    task_retry_count=task_retry_count,
                    task_turn_generation=task_turn_generation,
                    background_generation=background_generation,
                    require_background_state=True,
                )
            )

        def encoded_boundary(boundary_state: str) -> str:
            return json.dumps(
                {
                    "type": "pty.background_followup_boundary",
                    "version": 1,
                    "followup_operation_id": followup_operation_id,
                    "state": boundary_state,
                    "background_generation": background_generation,
                },
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )

        if state == "completed" and not proof_is_current():
            state = "uncertain"
        lock_key = (task_id, followup_operation_id)
        lock = self._pty_followup_boundary_locks.setdefault(
            lock_key,
            asyncio.Lock(),
        )
        async with lock:
            for attempt in range(2):
                try:
                    entry_id: int | None = None
                    entry_timestamp: datetime | None = None
                    effective_state = state
                    async with self.db_factory() as db:
                        if not await _fence_worker_runtime_mutation(
                            db,
                            producer="PTY follow-up boundary",
                        ):
                            return False
                        candidates = (
                            await db.execute(
                                select(LogEntry)
                                .where(
                                    LogEntry.task_id == task_id,
                                    LogEntry.task_retry_count
                                    == task_retry_count,
                                    LogEntry.task_turn_generation
                                    == task_turn_generation,
                                    LogEntry.event_type
                                    == "pty_background_followup_boundary",
                                    LogEntry.native_turn_id
                                    == background_generation,
                                )
                                .order_by(LogEntry.id.desc())
                            )
                        ).scalars().all()
                        existing = None
                        for candidate in candidates:
                            try:
                                candidate_payload = json.loads(
                                    candidate.raw_json or "{}"
                                )
                            except (TypeError, ValueError):
                                continue
                            if (
                                isinstance(candidate_payload, dict)
                                and candidate_payload.get(
                                    "followup_operation_id"
                                )
                                == followup_operation_id
                            ):
                                existing = candidate
                                break
                        if existing is None:
                            effective_state = state
                            if (
                                effective_state == "completed"
                                and not proof_is_current()
                            ):
                                effective_state = "uncertain"
                            now = datetime.utcnow()
                            entry = LogEntry(
                                instance_id=instance_id,
                                task_id=task_id,
                                task_retry_count=task_retry_count,
                                task_turn_generation=task_turn_generation,
                                native_turn_id=background_generation,
                                event_type=(
                                    "pty_background_followup_boundary"
                                ),
                                role="system",
                                content=None,
                                raw_json=encoded_boundary(effective_state),
                                is_error=effective_state == "uncertain",
                                timestamp=now,
                            )
                            db.add(entry)
                            await db.flush()
                            if (
                                effective_state == "completed"
                                and not proof_is_current()
                            ):
                                effective_state = "uncertain"
                                entry.raw_json = encoded_boundary(
                                    effective_state
                                )
                                entry.is_error = True
                            entry_id = entry.id
                            entry_timestamp = now
                            await db.commit()
                        else:
                            entry_id = existing.id
                            entry_timestamp = existing.timestamp
                            try:
                                existing_payload = json.loads(
                                    existing.raw_json or "{}"
                                )
                            except (TypeError, ValueError):
                                existing_payload = {}
                            if (
                                isinstance(existing_payload, dict)
                                and existing_payload.get("state")
                                == "completed"
                            ):
                                effective_state = "completed"

                    # Invalidation can land while the database commit yields.
                    # Repair that just-written row before any live publication;
                    # a replacement must never expose dropped output as complete.
                    if (
                        entry_id is not None
                        and effective_state == "completed"
                        and not proof_is_current()
                    ):
                        async with self.db_factory() as repair_db:
                            if not await _fence_worker_runtime_mutation(
                                repair_db,
                                producer="PTY follow-up boundary repair",
                            ):
                                return False
                            repaired = await repair_db.execute(
                                update(LogEntry)
                                .where(LogEntry.id == entry_id)
                                .values(
                                    raw_json=encoded_boundary("uncertain"),
                                    is_error=True,
                                )
                            )
                            if not repaired.rowcount:
                                await repair_db.rollback()
                                return False
                            await repair_db.commit()
                        effective_state = "uncertain"

                    if entry_id is None:
                        return False
                    broadcast = {
                        "event_type": (
                            "pty_background_followup_boundary"
                        ),
                        "role": "system",
                        "content": None,
                        "is_error": effective_state == "uncertain",
                        "followup_operation_id": followup_operation_id,
                        "pty_followup_state": effective_state,
                        "state": effective_state,
                        "pty_background_generation": background_generation,
                        "background_generation": background_generation,
                        "task_id": task_id,
                        "instance_id": instance_id,
                        "task_retry_count": task_retry_count,
                        "task_turn_generation": task_turn_generation,
                        "native_turn_id": background_generation,
                        "id": entry_id,
                        "timestamp": (
                            entry_timestamp or datetime.utcnow()
                        ).isoformat(),
                    }
                    try:
                        await self.broadcaster.broadcast(
                            f"task:{task_id}",
                            broadcast,
                        )
                    except Exception:
                        # The committed row is the recovery source; a
                        # transient WebSocket failure must not make the pump
                        # report that the receipt was not recorded.
                        logger.warning(
                            "PTY follow-up boundary broadcast failed for "
                            "task %s operation %s",
                            task_id,
                            followup_operation_id,
                            exc_info=True,
                        )
                    return True
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning(
                        "PTY follow-up boundary persistence attempt %s "
                        "failed for task %s operation %s",
                        attempt + 1,
                        task_id,
                        followup_operation_id,
                        exc_info=True,
                    )
                    if attempt == 0:
                        await asyncio.sleep(0.05)
            return False

    async def _publish_pty_followup_boundary(
        self,
        backend: Any,
        key: int,
        event: dict,
        launch_params: dict[str, Any],
        *,
        task_id: int,
        task_retry_count: int,
        task_turn_generation: int,
        session_id: str,
        background_generation: str,
        followup_operation_id: str,
        state: str,
    ) -> bool:
        """Publish through the backend, then repair a swallowed boundary.

        Older PTY adapters intentionally swallow ordinary callback failures.
        That compatibility behavior is useful for autonomous mirroring but
        cannot apply to a follow-up receipt. An explicit ``False`` result or
        exception falls back to the independent durable audit writer; ``None``
        remains a compatibility success for older adapters and test doubles.
        """

        try:
            result = await backend.on_event(
                key,
                event,
                **launch_params,
            )
            if result is True:
                return True
            # Compatibility test doubles and older adapters return ``None``
            # after accepting the callback. Only an explicit ``False`` means
            # the strict writer declined the receipt and needs repair.
            if result is not False:
                return True
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "PTY follow-up boundary callback failed for task %s",
                task_id,
            )
        return await self.persist_pty_followup_boundary(
            instance_id=key,
            task_id=task_id,
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
            session_id=session_id,
            background_generation=background_generation,
            followup_operation_id=followup_operation_id,
            state=state,
        )

    def _pty_followup_proof_is_current(
        self,
        key: int,
        session: Any,
        record: _OutputConsumerRecord,
        background_state: _PtyBackgroundState,
        post_exit_proof: _PtyPostExitGeneration | None,
    ) -> bool:
        """Whether a retained follow-up still owns its exact output epoch."""

        task_id = record.task_id
        session_id = background_state.session_id
        if task_id is None:
            return False
        state_key = (task_id, session_id)
        if (
            self._pty_background_states.get(state_key) is not background_state
            or not getattr(background_state, "accepting_events", True)
            or getattr(session, "session_id", None) != session_id
            or getattr(session, "is_alive", True) is False
            or key in self._stopping
            or key in self._launch_reservations
        ):
            return False
        if post_exit_proof is None:
            return bool(
                self._consumer_records.get(key) is record
                and self._tasks.get(key) is record.task
                and self.processes.get(key) is record.process
                and getattr(record.process, "session", None) is session
            )
        return bool(
            not post_exit_proof.invalidated
            and post_exit_proof.instance_id == key
            and post_exit_proof.session is session
            and post_exit_proof.record is record
            and self._pty_post_exit_generation_is_current(
                post_exit_proof,
                instance_id=key,
                task_id=task_id,
                session_id=session_id,
                task_retry_count=background_state.task_retry_count,
                task_turn_generation=background_state.task_turn_generation,
                background_generation=background_state.generation,
                require_background_state=True,
            )
        )

    async def _settle_pty_followup_boundary(
        self,
        key: int,
        backend: Any,
        session: Any,
        record: _OutputConsumerRecord,
        background_state: _PtyBackgroundState,
        post_exit_proof: _PtyPostExitGeneration | None,
        launch_params: dict[str, Any],
        followup_operation_id: str,
        *,
        stream_completed: bool,
        ownership_lost: bool,
        cancelled: bool,
    ) -> tuple[bool, dict[str, Any]]:
        """Publish one receipt with the captured proof as a downgrade fence."""

        ownership_current = self._pty_followup_proof_is_current(
            key,
            session,
            record,
            background_state,
            post_exit_proof,
        )
        boundary_state = (
            "completed"
            if (
                stream_completed
                and not ownership_lost
                and not cancelled
                and ownership_current
            )
            else "uncertain"
        )
        boundary_payload = {
            "type": "pty.background_followup_boundary",
            "version": 1,
            "followup_operation_id": followup_operation_id,
            "state": boundary_state,
            "background_generation": background_state.generation,
        }
        boundary_event = {
            "event_type": "pty_background_followup_boundary",
            "role": "system",
            "content": None,
            "raw_json": json.dumps(
                boundary_payload,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ),
            "is_error": boundary_state == "uncertain",
            "followup_operation_id": followup_operation_id,
            "pty_followup_state": boundary_state,
            "state": boundary_state,
            "pty_background_generation": background_state.generation,
            "task_id": record.task_id,
            "task_retry_count": background_state.task_retry_count,
            "task_turn_generation": background_state.task_turn_generation,
            # FullMirrorCCMBackend handles this synthetic event with the
            # strict receipt writer; ordinary output callbacks retain their
            # historical error-swallowing behavior.
            "pty_followup_boundary": True,
        }
        if post_exit_proof is not None:
            boundary_ok = await self.persist_pty_followup_boundary(
                instance_id=key,
                task_id=record.task_id,
                task_retry_count=background_state.task_retry_count,
                task_turn_generation=background_state.task_turn_generation,
                session_id=background_state.session_id,
                background_generation=background_state.generation,
                followup_operation_id=followup_operation_id,
                state=boundary_state,
                post_exit_proof=post_exit_proof,
            )
        else:
            boundary_ok = await self._publish_pty_followup_boundary(
                backend,
                key,
                boundary_event,
                launch_params,
                task_id=record.task_id,
                task_retry_count=background_state.task_retry_count,
                task_turn_generation=(
                    background_state.task_turn_generation
                ),
                session_id=background_state.session_id,
                background_generation=background_state.generation,
                followup_operation_id=followup_operation_id,
                state=boundary_state,
            )
        return boundary_ok, boundary_event

    async def _run_pty_followup_prompt(
        self,
        key: int,
        backend: Any,
        session: Any,
        content: str,
        record: _OutputConsumerRecord,
        background_state: _PtyBackgroundState,
        post_exit_proof: _PtyPostExitGeneration | None,
        admission: asyncio.Future[bool],
        followup_operation_id: str,
    ) -> None:
        """Pump one follow-up prompt through an already retained PTY Session."""

        current = asyncio.current_task()
        if current is not None:
            setattr(current, "_ccm_output_consumer_record", record)
            setattr(current, "_ccm_followup_started", True)
        stream = None
        first_event: asyncio.Task | None = None
        delayed_cancellation: asyncio.CancelledError | None = None
        admitted_locally = False
        stream_completed = False
        followup_terminal_seen = False
        ownership_lost = False

        async def publish_event(event: Any) -> dict[str, Any]:
            """Route pre-prompt child output back through its lifecycle owner."""

            event_dict = event.to_dict()
            if event_dict.get("orphan"):
                callback = getattr(session, "on_autonomous_event", None)
                callback_matches = bool(
                    callable(callback)
                    and getattr(callback, "__name__", "")
                    == "_full_autonomous_mirror"
                    and getattr(callback, "_ccm_task_id", None)
                    == record.task_id
                    and getattr(callback, "_ccm_session_id", None)
                    == background_state.session_id
                    and self._pty_background_states.get(
                        (record.task_id, background_state.session_id)
                    )
                    is background_state
                    and getattr(background_state, "accepting_events", True)
                )
                if callback_matches:
                    # Session.send_prompt marks every record before its own
                    # prompt echo as orphan. During a retained follow-up those
                    # records can be the still-running child's autonomous
                    # notification/result/sentinel. The idle watcher yields to
                    # send_prompt, so this pump must hand them to the exact
                    # autonomous callback instead of persisting them as
                    # follow-up output and losing the child terminal edge.
                    await callback(event)
                    return event_dict
            await backend.on_event(
                key,
                event_dict,
                **launch_params,
            )
            return event_dict

        try:
            if backend is None:
                admission.set_result(False)
                return
            launch_params = dict(
                getattr(backend, "_launch_params", {}).get(key) or {}
            )
            launch_params["task_id"] = record.task_id
            launch_params.setdefault("loop_iteration", None)
            launch_params.update(
                background_followup=True,
                expected_session_id=background_state.session_id,
                expected_background_generation=background_state.generation,
                expected_task_retry_count=(
                    background_state.task_retry_count
                ),
                expected_task_turn_generation=(
                    background_state.task_turn_generation
                ),
            )
            if not self._pty_followup_proof_is_current(
                key,
                session,
                record,
                background_state,
                post_exit_proof,
            ):
                admission.set_result(False)
                return
            stream = session.send_prompt(content)
            first_event = asyncio.create_task(
                anext(stream),
                name=f"pty-followup-first-event-{key}",
            )
            # Let the async generator execute once. A synchronous pre-delivery
            # error/empty stream is still a rejection; otherwise the exact
            # pump now owns the serialized send_prompt operation. Provider
            # delivery and the JSONL prompt echo may take arbitrarily longer
            # and are deliberately not part of this local admission receipt.
            await asyncio.sleep(0)
            if first_event.done():
                try:
                    event = first_event.result()
                except StopAsyncIteration:
                    admission.set_result(False)
                    return
            else:
                admitted_locally = True
                admission.set_result(True)
                event = await first_event
            if not admission.done():
                admitted_locally = True
                admission.set_result(True)
            if not self._pty_followup_proof_is_current(
                key,
                session,
                record,
                background_state,
                post_exit_proof,
            ):
                ownership_lost = True
            if not ownership_lost:
                event_dict = await publish_event(event)
                if (
                    not event_dict.get("orphan")
                    and self._is_pty_autonomous_terminal(event_dict)
                ):
                    followup_terminal_seen = True
                if not self._pty_followup_proof_is_current(
                    key,
                    session,
                    record,
                    background_state,
                    post_exit_proof,
                ):
                    ownership_lost = True
            if not ownership_lost:
                async for event in stream:
                    if not self._pty_followup_proof_is_current(
                        key,
                        session,
                        record,
                        background_state,
                        post_exit_proof,
                    ):
                        ownership_lost = True
                        break
                    event_dict = await publish_event(event)
                    if (
                        not event_dict.get("orphan")
                        and self._is_pty_autonomous_terminal(event_dict)
                    ):
                        followup_terminal_seen = True
                    if not self._pty_followup_proof_is_current(
                        key,
                        session,
                        record,
                        background_state,
                        post_exit_proof,
                    ):
                        ownership_lost = True
                        break
                stream_completed = bool(
                    not ownership_lost and followup_terminal_seen
                )
        except asyncio.CancelledError as exc:
            delayed_cancellation = exc
            if not admission.done():
                admission.set_result(False)
        except Exception:
            if not admission.done():
                admission.set_result(False)
            logger.exception(
                "PTY follow-up prompt failed for instance %s task %s",
                key,
                record.task_id,
            )
        finally:
            if not admission.done():
                admission.set_result(False)
            if first_event is not None and not first_event.done():
                first_event.cancel()
                await asyncio.gather(first_event, return_exceptions=True)
            try:
                close_stream = getattr(stream, "aclose", None)
                if callable(close_stream):
                    close = asyncio.create_task(close_stream())
                    close_cancellation = await await_task_completion(close)
                    try:
                        close.result()
                    except asyncio.CancelledError as exc:
                        if delayed_cancellation is None:
                            delayed_cancellation = exc
                    except Exception:
                        logger.exception(
                            "Failed to close PTY follow-up stream for instance %s",
                            key,
                        )
                    if delayed_cancellation is None:
                        delayed_cancellation = close_cancellation
            except asyncio.CancelledError as exc:
                if delayed_cancellation is None:
                    delayed_cancellation = exc
            except Exception:
                logger.exception(
                    "Could not create PTY follow-up stream cleanup for "
                    "instance %s",
                    key,
                )
        try:
            # Once the provider accepted the prompt, always leave a durable
            # receipt. A cancellation after admission is explicitly uncertain
            # rather than an invisible side effect that can strand the UI.
            if admitted_locally:
                boundary_task = asyncio.create_task(
                    self._settle_pty_followup_boundary(
                        key,
                        backend,
                        session,
                        record,
                        background_state,
                        post_exit_proof,
                        launch_params,
                        followup_operation_id,
                        stream_completed=stream_completed,
                        ownership_lost=ownership_lost,
                        cancelled=delayed_cancellation is not None,
                    ),
                    name=f"pty-followup-boundary-{key}",
                )
                # The strict boundary callback is part of this same retained
                # output generation. Preserve the consumer record on the
                # helper task so backend adapters can apply the same exact
                # generation fence as the ordinary output pump.
                setattr(
                    boundary_task,
                    "_ccm_output_consumer_record",
                    record,
                )
                boundary_cancellation = await await_task_completion(
                    boundary_task
                )
                boundary_ok = False
                boundary_event: dict[str, Any] = {}
                try:
                    boundary_ok, boundary_event = boundary_task.result()
                except asyncio.CancelledError as exc:
                    if delayed_cancellation is None:
                        delayed_cancellation = exc
                except Exception:
                    logger.exception(
                        "PTY follow-up boundary task failed for instance %s",
                        key,
                    )
                if (
                    boundary_cancellation is not None
                    and delayed_cancellation is None
                ):
                    delayed_cancellation = boundary_cancellation
                if not boundary_ok:
                    # There is no durable recovery path left after repeated
                    # DB failure, but an uncertain live signal still prevents
                    # a mounted client from waiting forever. History remains
                    # conservative: the queue will require confirmation.
                    volatile_boundary = {
                        k: v for k, v in boundary_event.items()
                        if k not in {"raw_json", "pty_followup_boundary"}
                    }
                    volatile_boundary["pty_followup_state"] = "uncertain"
                    volatile_boundary["state"] = "uncertain"
                    volatile_boundary["is_error"] = True
                    fallback_task = asyncio.create_task(
                        self.broadcaster.broadcast(
                            f"task:{record.task_id}",
                            volatile_boundary,
                        ),
                        name=f"pty-followup-boundary-fallback-{key}",
                    )
                    fallback_cancellation = await await_task_completion(
                        fallback_task
                    )
                    try:
                        fallback_task.result()
                    except Exception:
                        logger.exception(
                            "PTY follow-up volatile boundary failed for "
                            "instance %s",
                            key,
                        )
                    if (
                        fallback_cancellation is not None
                        and delayed_cancellation is None
                    ):
                        delayed_cancellation = fallback_cancellation
        finally:
            if current is not None:
                self._finalize_pty_followup_pump(
                    key,
                    current,
                    background_state,
                )
        # A proof invalidation is an internal replacement/stop cancellation;
        # its uncertainty has already been recorded and must not surface as a
        # cancelled pump to the teardown waiter.  Preserve propagation of a
        # cancellation delivered by the API/dispatcher caller itself.
        proof_was_invalidated = bool(
            post_exit_proof is not None and post_exit_proof.invalidated
        )
        if delayed_cancellation is not None and not proof_was_invalidated:
            raise delayed_cancellation

    async def _cancel_pty_followup_tasks(
        self,
        instance_ids: set[int] | None = None,
    ) -> None:
        """Cancel and settle retained-Session event pumps before teardown."""

        keys = (
            set(self._pty_followup_tasks)
            if instance_ids is None
            else set(instance_ids)
        )
        pending: list[asyncio.Task] = []
        for key in keys:
            for followup in tuple(self._pty_followup_tasks.pop(key, set())):
                if not followup.done():
                    followup.cancel()
                pending.append(followup)
        if pending:
            settlement = asyncio.gather(*pending, return_exceptions=True)
            delayed_cancellation = await await_task_completion(settlement)
            settlement.result()
            if delayed_cancellation is not None:
                raise delayed_cancellation

    async def inject_pty_message(
        self,
        session_id: str,
        content: str,
        *,
        task_id: int | None = None,
        task_retry_count: int | None = None,
        task_turn_generation: int | None = None,
        expected_instance_id: int | None = None,
        require_host_file_access: bool = False,
        followup_operation_id: str | None = None,
    ) -> bool:
        """Inject into one exact live Claude PTY foreground turn.

        Active-turn user input uses Claude-PTY's fenced
        ``steer_active_turn`` API.  The separate channel ``inject`` API is a
        notification-only transport: it may be accepted by the local MCP
        server without becoming a user message in the current turn and is only
        guaranteed to appear at a later tool boundary.  Treating its HTTP 200
        as a successful steer can report a message that the model never
        consumed.

        ``steer_active_turn`` proves the complete stdin write and matching
        queue acknowledgement.  If the write completed but the acknowledgement
        is unavailable, the pinned PTY dependency raises its uncertain-
        delivery error; this method maps it to
        ``ClaudeInjectionAdmissionUncertainError`` so callers preserve the
        process and refuse an automatic duplicate.

        ``Task.instance_id`` can be absent for chat-launched turns, so the
        native session id resolves the candidate slot.  The immutable
        Task/consumer/process identity is then revalidated under that slot's
        lifecycle lock before any input is written.  Returns False for an
        idle, terminal, replaced, ambiguous, or otherwise stale generation.
        """
        if self._pty_backend is None or not content or not session_id:
            return False

        sessions = getattr(self._pty_backend, "_sessions", {})
        ordinary_candidates = [
            (candidate_key, candidate)
            for candidate_key, candidate in sessions.items()
            if (
                (expected_instance_id is None
                 or candidate_key == expected_instance_id)
                and getattr(candidate, "session_id", None) == session_id
            )
        ]
        # A retained proof is the only valid route once FullMirror has
        # released the ordinary PTY maps.  Resolve it by immutable generation,
        # never by choosing an arbitrary same-session dictionary entry.
        proof_candidates: list[_PtyPostExitGeneration] = []
        if (
            task_id is not None
            and task_retry_count is not None
            and task_turn_generation is not None
        ):
            for proof in tuple(self._pty_post_exit_generations.values()):
                if (
                    proof.session_id != session_id
                    or (
                        expected_instance_id is not None
                        and proof.instance_id != expected_instance_id
                    )
                    or not self._pty_post_exit_generation_is_current(
                        proof,
                        task_id=task_id,
                        session_id=session_id,
                        task_retry_count=task_retry_count,
                        task_turn_generation=task_turn_generation,
                    )
                ):
                    continue
                proof_candidates.append(proof)

        candidate_keys = {
            candidate_key for candidate_key, _candidate in ordinary_candidates
        }
        candidate_keys.update(proof.instance_id for proof in proof_candidates)
        # A duplicated native session registration or proof is an ABA
        # ambiguity, not a reason to pick whichever entry appears first.
        if len(candidate_keys) != 1:
            return False
        key = next(iter(candidate_keys))
        candidate = next(
            (
                ordinary_candidate
                for candidate_key, ordinary_candidate in ordinary_candidates
                if candidate_key == key
            ),
            None,
        )
        proof_candidate = next(
            (proof for proof in proof_candidates if proof.instance_id == key),
            None,
        )
        if (
            candidate is not None
            and proof_candidate is not None
            and candidate is not proof_candidate.session
        ):
            return False
        if candidate is None and proof_candidate is None:
            return False
        if candidate is None:
            candidate = proof_candidate.session
        lifecycle_lock = self._instance_lifecycle_lock(key)
        async with lifecycle_lock:
            attached_session = getattr(self._pty_backend, "_sessions", {}).get(
                key
            )
            proof_candidate = (
                self._pty_post_exit_generations.get((task_id, session_id))
                if task_id is not None
                else None
            )
            if attached_session is not None and attached_session is not candidate:
                return False
            if attached_session is None and (
                proof_candidate is None or proof_candidate.session is not candidate
            ):
                return False
            session = attached_session or candidate
            if (
                getattr(session, "session_id", None) != session_id
                or not getattr(session, "is_alive", False)
                or key in self._stopping
                or key in self._launch_reservations
            ):
                return False

            if require_host_file_access and key in self._container_tasks:
                # Shared-project PTY sessions run inside a container whose
                # mount set does not include Manager's upload directory.
                raise LiveAttachmentInjectionUnsupportedError(
                    "The active Claude PTY container cannot access uploaded files"
                )

            consumer = getattr(self._pty_backend, "_consumers", {}).get(key)
            record = self._consumer_records.get(key)
            process = self.processes.get(key)
            proxy = getattr(self._pty_backend, "_proxies", {}).get(key)
            background_generation = (
                self.pty_background_generation_for(task_id, session_id)
                if task_id is not None
                else None
            )
            retained_proof = None
            if proof_candidate is not None and self._pty_post_exit_generation_is_current(
                proof_candidate,
                instance_id=key,
                task_id=task_id,
                session_id=session_id,
                task_retry_count=task_retry_count,
                task_turn_generation=task_turn_generation,
                background_generation=background_generation,
                require_background_state=True,
            ):
                retained_proof = proof_candidate
            cancelling = getattr(consumer, "cancelling", None)
            exact_turn = bool(
                consumer is not None
                and not consumer.done()
                and (not callable(cancelling) or cancelling() == 0)
                and record is not None
                and record.task is consumer
                and record.process is process
                and record.provider == "claude"
                and record.pty_terminal_owner is None
                and self._tasks.get(key) is consumer
                and proxy is process
                and getattr(proxy, "session", None) is session
                and getattr(process, "returncode", None) is None
                and not any(
                    pending_key[0] == key
                    and pending_key[1] is process
                    for pending_key in self._consumer_recovery_pending
                )
            )
            if task_id is not None:
                exact_turn = bool(
                    exact_turn
                    and record.task_id == task_id
                    and record.task_retry_count == task_retry_count
                    and record.task_turn_generation == task_turn_generation
                )
            if not exact_turn and retained_proof is None:
                logger.info(
                    "PTY steer rejected for session %s: stale foreground turn",
                    session_id,
                )
                return False

            native_process = getattr(session, "active_turn_process", None)
            steer = getattr(session, "steer_active_turn", None)
            if native_process is not None:
                if retained_proof is not None and not exact_turn:
                    logger.info(
                        "PTY steer rejected for session %s: retained proof "
                        "has no idle follow-up boundary",
                        session_id,
                    )
                    return False
                # A retained follow-up pump owns the Session's current native
                # process after ``send_prompt`` starts.  Do not mistake that
                # process for an independent foreground turn: steering it
                # would bypass the serialized follow-up slot and leave the
                # second API operation waiting for a boundary it cannot own.
                followups = self._pty_followup_tasks.get(key, ())
                if any(not followup.done() for followup in followups):
                    logger.info(
                        "PTY steer rejected for session %s: a retained "
                        "follow-up prompt is already being consumed",
                        session_id,
                    )
                    return False
            if native_process is None:
                # FullMirrorCCMBackend keeps the original consumer waiting on
                # the exact background marker after the visible root turn
                # ends. The provider Session is still alive and serializes a
                # new prompt with its internal send lock; use a small event
                # pump so the response follows the same durable event path.
                send_prompt = getattr(session, "send_prompt", None)
                followup_record = (
                    retained_proof.record
                    if retained_proof is not None
                    else record
                )
                if (
                    task_id is None
                    or followup_record is None
                    or not callable(send_prompt)
                ):
                    logger.info(
                        "PTY steer rejected for session %s: turn is not "
                        "steerable and no retained background Session is free",
                        session_id,
                    )
                    return False
                async with self.pty_background_transition(
                    task_id,
                    session_id,
                ):
                    background_generation = (
                        self.pty_background_generation_for(
                            task_id,
                            session_id,
                        )
                    )
                    background_state = self.pty_background_state_for(
                        task_id,
                        session_id,
                        background_generation,
                    )
                    if background_state is None:
                        logger.info(
                            "PTY steer rejected for session %s: turn is not "
                            "steerable and no retained background Session is free",
                            session_id,
                        )
                        return False
                    if retained_proof is not None and not self._pty_post_exit_generation_is_current(
                        retained_proof,
                        instance_id=key,
                        task_id=task_id,
                        session_id=session_id,
                        task_retry_count=task_retry_count,
                        task_turn_generation=task_turn_generation,
                        background_generation=background_generation,
                        require_background_state=True,
                    ):
                        logger.info(
                            "PTY follow-up rejected for session %s: retained "
                            "proof was replaced or stopped",
                            session_id,
                        )
                        return False
                    followups = self._pty_followup_tasks.setdefault(key, set())
                    if any(not task.done() for task in followups):
                        logger.info(
                            "PTY steer rejected for session %s: a follow-up "
                            "prompt is already being consumed",
                            session_id,
                        )
                        return False
                    background_state.pending_followups += 1
                    resolved_followup_operation_id = (
                        followup_operation_id or secrets.token_hex(16)
                    )
                    admission = asyncio.get_running_loop().create_future()
                    try:
                        followup = asyncio.create_task(
                            self._run_pty_followup_prompt(
                                key,
                                self._pty_backend,
                                session,
                                content,
                                followup_record,
                                background_state,
                                retained_proof,
                                admission,
                                resolved_followup_operation_id,
                            ),
                            name=f"pty-followup-{key}",
                        )
                    except Exception:
                        background_state.pending_followups = max(
                            0,
                            background_state.pending_followups - 1,
                        )
                        logger.exception(
                            "Could not create PTY follow-up pump for session %s",
                            session_id,
                        )
                        return False
                    setattr(followup, "_ccm_followup_finalized", False)
                    setattr(
                        followup,
                        "_ccm_pty_post_exit_proof",
                        retained_proof,
                    )

                    def settle_followup(done: asyncio.Task) -> None:
                        # ``Task.cancel()`` can win before the coroutine's
                        # first bytecode executes. Resolve the local receipt
                        # and accounting from the done callback as the exact
                        # fallback, otherwise inject would wait forever on a
                        # pump that never reached its ``finally`` block.
                        if not admission.done():
                            admission.set_result(False)
                        self._finalize_pty_followup_pump(
                            key,
                            done,
                            background_state,
                        )
                        try:
                            error = done.exception()
                        except asyncio.CancelledError:
                            return
                        if error is not None:
                            logger.error(
                                "PTY follow-up pump terminated unexpectedly "
                                "for session %s",
                                session_id,
                                exc_info=(
                                    type(error),
                                    error,
                                    error.__traceback__,
                                ),
                            )

                    followup.add_done_callback(settle_followup)
                    followups.add(followup)
                # The pump publishes a local receipt after one generator
                # scheduling step. This catches synchronous creation failures
                # without coupling HTTP success to Claude's potentially slow
                # prompt echo or ``active_turn_process`` visibility. As with
                # live steering, caller cancellation is delayed so the API can
                # persist its audit record for every admitted side effect.
                caller_cancellation = await await_task_completion(admission)
                admitted = bool(admission.result())
                if not admitted:
                    later_cancellation = await await_task_completion(followup)
                    try:
                        followup.result()
                    except BaseException:
                        pass
                    if caller_cancellation is None:
                        caller_cancellation = later_cancellation
                if caller_cancellation is not None:
                    logger.info(
                        "Finished PTY follow-up admission for session %s "
                        "after caller cancellation",
                        session_id,
                    )
                if admitted:
                    self._record_pty_followup_operation_route(
                        followup_operation_id,
                        resolved_followup_operation_id,
                    )
                return admitted
            if not callable(steer):
                logger.info(
                    "PTY steer rejected for session %s: turn is not steerable",
                    session_id,
                )
                return False
            # Active-turn input intentionally bypasses the notification-only
            # channel transport and uses the fenced stdin steer below.
            try:
                # Once the exact provider side effect is admitted, finish its
                # acknowledgement even if the HTTP caller disconnects.  Do
                # not let cancellation release the lifecycle lock while a PTY
                # thread can still write, or skip the API's audit LogEntry
                # after Claude already accepted the update.
                steering = asyncio.create_task(
                    steer(content, expected_process=native_process)
                )
                caller_cancellation = await await_task_completion(steering)
                result = bool(steering.result())
                if caller_cancellation is not None:
                    logger.info(
                        "Finished PTY steer acknowledgement for session %s "
                        "after caller cancellation",
                        session_id,
                    )
                if result:
                    # The durable marker may still be present while a new
                    # foreground native turn has already started.  This
                    # transport has no retained boundary, so explicitly map
                    # the provisional API id to ``None``.
                    self._record_pty_followup_operation_route(
                        followup_operation_id,
                        None,
                    )
                return result
            except Exception as exc:
                # A complete legacy stdin steer has an at-most-once side
                # effect even when Claude's optional queue receipt is absent.
                # The pinned PTY dependency exposes that case explicitly so
                # callers can preserve the process and refuse an automatic
                # duplicate rather than misclassifying it as an unavailable
                # turn.
                try:
                    from claude_pty import SteerDeliveryUncertainError
                except ImportError:  # pragma: no cover - PTY mode is absent
                    steer_uncertain_types: tuple[type[BaseException], ...] = ()
                else:
                    steer_uncertain_types = (SteerDeliveryUncertainError,)
                if isinstance(exc, steer_uncertain_types):
                    raise ClaudeInjectionAdmissionUncertainError(
                        "Claude stdin steering completed without an "
                        "authoritative delivery acknowledgement"
                    ) from exc
                logger.exception("PTY steer failed for session %s", session_id)
                return False

    async def inject_codex_message(
        self,
        thread_id: str,
        content: str,
        *,
        input_items: list[dict] | None = None,
    ) -> bool:
        """Steer a live Codex app-server turn without starting a new turn.

        Codex ``exec`` subprocesses do not expose same-turn steering, so a
        missing app-server/context deliberately returns False.
        """
        if (
            self._codex_app_server is None
            or not thread_id
            or (not content and not input_items)
        ):
            return False
        from backend.services.codex_app_server import (
            CodexTurnAdmissionUncertainError,
        )

        try:
            if input_items is None:
                return await self._codex_app_server.steer_turn(
                    thread_id,
                    content,
                )
            return await self._codex_app_server.steer_turn(
                thread_id,
                content,
                input_items=input_items,
            )
        except CodexTurnAdmissionUncertainError:
            logger.exception(
                "Codex inject admission is uncertain for thread %s",
                thread_id,
            )
            raise
        except Exception:
            logger.exception("Codex inject failed for thread %s", thread_id)
            return False

    async def release_pty_session(self, session_id: str) -> bool:
        """Return a PTY session to nothing — stop it and remove from the pool.
        Used when a workload (e.g. a loop task) is finished with its session.
        No-op when PTY mode is not in use."""
        if self._pty_backend is None or not session_id:
            return False
        pool = self._pty_backend._pool
        session = await pool.get(session_id)
        followup_instance_ids = (
            {
                key
                for key, candidate in getattr(
                    self._pty_backend,
                    "_sessions",
                    {},
                ).items()
                if candidate is session
            }
            if session is not None
            else set()
        )

        async def cancel_followups_and_remove() -> None:
            await self._cancel_pty_followup_tasks(followup_instance_ids)
            await pool.remove(session_id)

        try:
            # SessionPool.remove() unpublishes the exact Session before it
            # awaits stop.  Keep that cleanup alive across caller
            # cancellation so neither an unpublished native process nor its
            # CCM-owned follow-up pump can survive the release boundary.
            await _settle_instance_cleanup(cancel_followups_and_remove())
        except Exception:
            logger.exception("Failed to release PTY session %s", session_id)
            # If the pinned pool already popped the Session before stop
            # failed, retain our captured identity and make one direct,
            # cancellation-safe recovery attempt.  Cold resume remains
            # forbidden unless exact native death is observable afterwards.
            if session is None or await pool.get(session_id) is session:
                return False
            try:
                await _settle_instance_cleanup(session.stop())
            except Exception:
                logger.exception(
                    "Failed to recover removed PTY session %s", session_id
                )
                return False
        if session is None:
            return True
        pool_session = await pool.get(session_id)
        released = bool(
            pool_session is not session
            and getattr(session, "is_alive", True) is False
        )
        if released:
            self._release_task_runtime_scope_pty_owner(session)
        return released

    def _task_pty_runtime_session_candidates(
        self,
        task_id: int,
        session_id: str,
        *,
        instance_id: int | None = None,
    ) -> list[tuple[int | None, Any]]:
        """Find PTY Sessions still owned by one exact Task/session pair.

        The post-exit proof is intentionally short-lived after a hot follow-up
        settles, while the task runtime-scope owner remains until the native
        Session is actually stopped. Deletion must consult both records so a
        terminal Task cannot leave a live Session behind after its proof watcher
        has retired.
        """

        candidates: list[tuple[int | None, Any]] = []

        def add_candidate(candidate_instance_id: int | None, session: Any) -> None:
            if session is None or getattr(session, "session_id", None) != session_id:
                return
            if (
                instance_id is not None
                and candidate_instance_id is not None
                and candidate_instance_id != instance_id
            ):
                return
            for index, (existing_instance_id, existing_session) in enumerate(
                candidates
            ):
                if existing_session is not session:
                    continue
                if existing_instance_id is None and candidate_instance_id is not None:
                    candidates[index] = (candidate_instance_id, session)
                return
            candidates.append((candidate_instance_id, session))

        for session, owner_task_id in tuple(
            self._task_runtime_scope_pty_owners.items()
        ):
            if owner_task_id == task_id:
                add_candidate(None, session)

        state = self._pty_background_states.get((task_id, session_id))
        if state is not None:
            add_candidate(getattr(state, "instance_id", None), state.session)

        backend_sessions = getattr(self._pty_backend, "_sessions", {})
        for candidate_instance_id, session in tuple(backend_sessions.items()):
            if getattr(session, "session_id", None) != session_id:
                continue
            if instance_id is not None and candidate_instance_id != instance_id:
                continue
            record = self._consumer_records.get(candidate_instance_id)
            if record is not None and record.task_id != task_id:
                # A reusable slot now owned by another Task is never a
                # destructive-stop target for this delete.
                continue
            add_candidate(candidate_instance_id, session)

        # Resolve owner-only candidates to their attached slot when there is
        # exactly one. Multiple attachments remain represented as separate
        # candidates and are rejected by the caller as an ABA ambiguity.
        for candidate_instance_id, session in tuple(candidates):
            if candidate_instance_id is not None:
                continue
            attached_ids = [
                key
                for key, attached in tuple(backend_sessions.items())
                if attached is session
                and (instance_id is None or key == instance_id)
            ]
            if len(attached_ids) == 1:
                index = candidates.index((candidate_instance_id, session))
                candidates[index] = (attached_ids[0], session)
            elif len(attached_ids) > 1:
                for attached_id in attached_ids[1:]:
                    candidates.append((attached_id, session))

        return candidates

    def has_live_task_pty_post_exit(
        self,
        task_id: int,
        *,
        session_id: str | None = None,
        instance_id: int | None = None,
        task_retry_count: int | None = None,
        task_turn_generation: int | None = None,
    ) -> bool:
        """Return whether a retained PTY proof still owns this Task.

        ``FullMirrorCCMBackend`` deliberately removes the ordinary
        instance-keyed process maps before retaining a hot Session for a
        follow-up. Callers that only inspect ``is_running()`` therefore miss
        that native process. Keep this check generation/session-aware so a
        stale proof from a deleted/reused Task id cannot be mistaken for the
        current Task's Session.
        """

        for proof in tuple(self._pty_post_exit_generations.values()):
            if proof.task_id != task_id:
                continue
            if session_id is not None and proof.session_id != session_id:
                continue
            if instance_id is not None and proof.instance_id != instance_id:
                continue
            if not self._pty_post_exit_generation_is_current(
                proof,
                task_id=task_id,
                session_id=session_id,
                allow_task_generation_drift=True,
                task_retry_count=task_retry_count,
                task_turn_generation=task_turn_generation,
            ):
                continue
            if getattr(proof.session, "is_alive", True) is not False:
                return True
        return any(
            getattr(session, "is_alive", True) is not False
            for _candidate_instance_id, session in (
                self._task_pty_runtime_session_candidates(
                    task_id,
                    session_id,
                    instance_id=instance_id,
                )
                if session_id
                else ()
            )
        )

    async def cleanup_task_pty_for_delete(
        self,
        task_id: int,
        *,
        session_id: str | None,
        instance_id: int | None = None,
        task_retry_count: int | None = None,
        task_turn_generation: int | None = None,
    ) -> bool:
        """Stop the exact retained PTY Session before deleting its Task.

        A terminal Claude chat intentionally retains its native Session for a
        short follow-up window. Task deletion is a stronger lifecycle edge:
        it must invalidate that handoff and prove the native process stopped
        before the durable Task row disappears. This method acquires the
        reusable-slot lock itself, so callers may invoke it before taking the
        normal deletion lock set.
        """

        if self._pty_backend is None:
            return True
        if not session_id:
            # Without the frozen native session id there is no safe target for
            # a destructive stop. Let the caller's durable delete guard fail
            # closed instead of guessing from a reusable instance slot.
            return not self.has_live_task_pty_post_exit(
                task_id,
                instance_id=instance_id,
                task_retry_count=task_retry_count,
                task_turn_generation=task_turn_generation,
            )

        proof_candidates = [
            proof
            for proof in tuple(self._pty_post_exit_generations.values())
            if (
                proof.task_id == task_id
                and proof.session_id == session_id
                and (instance_id is None or proof.instance_id == instance_id)
                and self._pty_post_exit_generation_is_current(
                    proof,
                    task_id=task_id,
                    session_id=session_id,
                    allow_task_generation_drift=True,
                    task_retry_count=task_retry_count,
                    task_turn_generation=task_turn_generation,
                )
            )
        ]
        runtime_candidates = self._task_pty_runtime_session_candidates(
            task_id,
            session_id,
            instance_id=instance_id,
        )
        session_candidates: list[tuple[int | None, Any]] = list(
            runtime_candidates
        )
        for proof in proof_candidates:
            if not any(session is proof.session for _slot, session in session_candidates):
                session_candidates.append((proof.instance_id, proof.session))
        distinct_sessions = []
        for candidate_instance_id, session in session_candidates:
            if not any(existing is session for _slot, existing in distinct_sessions):
                distinct_sessions.append((candidate_instance_id, session))
            elif candidate_instance_id is not None:
                for index, (existing_slot, existing) in enumerate(distinct_sessions):
                    if existing is session and existing_slot is None:
                        distinct_sessions[index] = (candidate_instance_id, existing)
        # There should be one exact Session per (Task, native session id).
        # Multiple Session objects or attached reusable slots are an ABA
        # ambiguity; refusing deletion preserves the runtime evidence.
        if len(distinct_sessions) != 1:
            if not distinct_sessions:
                return True
            return False

        candidate_instance_id, session = distinct_sessions[0]
        if candidate_instance_id is not None:
            lifecycle_lock = self._instance_lifecycle_lock(candidate_instance_id)
        else:
            lifecycle_lock = asyncio.Lock()
        async with lifecycle_lock:
            if candidate_instance_id is not None:
                attached = getattr(self._pty_backend, "_sessions", {}).get(
                    candidate_instance_id
                )
                if attached is not None and attached is not session:
                    return False
                record = self._consumer_records.get(candidate_instance_id)
                if record is not None and record.task_id != task_id:
                    return False

            if candidate_instance_id is not None:
                self._begin_stopping(candidate_instance_id)
            try:
                # Invalidate queued follow-ups before stopping the native
                # process. Their cancellation receipts must not race a Task
                # DELETE that is holding the task operation fence.
                key = (task_id, session_id)
                self._pty_autonomous_activity_handoffs.pop(key, None)
                backend_session = (
                    getattr(self._pty_backend, "_sessions", {}).get(
                        candidate_instance_id
                    )
                    if candidate_instance_id is not None
                    else None
                )
                if backend_session is session:
                    # The proof normally reaches this path after FullMirror
                    # released the slot maps, but handle an attached session
                    # conservatively by checking the pool identity first.
                    pool = getattr(self._pty_backend, "_pool", None)
                    pool_session = await pool.get(session_id) if pool is not None else None
                    if pool_session is not session:
                        return False
                    released = await self.release_pty_session(session_id)
                else:
                    released = await self._stop_exact_unattached_pty_session(
                        session,
                        session_id,
                        task_id,
                    )
                if not released:
                    return False
                for proof in tuple(proof_candidates):
                    self._discard_pty_post_exit_generation(
                        (proof.task_id, proof.session_id),
                        proof,
                    )
                self._pty_autonomous_activity_handoffs.pop(key, None)
                state = self._pty_background_states.get(key)
                if state is not None:
                    self._discard_pty_background_state(key, state.generation)
                return getattr(session, "is_alive", True) is False
            finally:
                if candidate_instance_id is not None:
                    self._end_stopping(candidate_instance_id)

    async def drain_idle_pty_sessions(self) -> int:
        """Stop idle PTY sessions (called after PTY mode is switched off).
        In-flight turns are untouched and finish on the PTY path."""
        if self._pty_backend is None:
            return 0
        before = dict(getattr(self._pty_backend._pool, "_sessions", {}))
        drained = await self._pty_backend.drain_idle_sessions()
        after = getattr(self._pty_backend._pool, "_sessions", {})
        for session_id, session in before.items():
            if (
                after.get(session_id) is not session
                and getattr(session, "is_alive", True) is False
            ):
                self._release_task_runtime_scope_pty_owner(session)
        self._reap_dead_task_runtime_scope_pty_owners()
        return drained

    async def shutdown_pty_backend(self) -> None:
        """Stop PTY transports, then release only proven-dead scope owners."""

        backend = self._pty_backend
        if backend is None:
            return
        sessions = set(
            getattr(getattr(backend, "_pool", None), "_sessions", {}).values()
        )

        async def cancel_followups_and_shutdown() -> None:
            await self._cancel_pty_followup_tasks()
            await backend.shutdown()

        await _settle_instance_cleanup(cancel_followups_and_shutdown())
        for session in sessions:
            if getattr(session, "is_alive", True) is False:
                self._release_task_runtime_scope_pty_owner(session)
        self._reap_dead_task_runtime_scope_pty_owners()

    def set_pty_mode(self, enabled: bool) -> bool:
        """Enable/disable PTY mode at runtime. Returns the effective state.

        The backend is created lazily on first enable and kept on disable
        (it may still manage sessions that started in PTY mode).
        """
        if enabled:
            if self._pty_backend is None:
                try:
                    # FullMirrorCCMBackend = CCMBackend + idle-time autonomous
                    # turn 全量镜像（后台监视器回报进聊天，task 27 事故）
                    from backend.services.pty_full_mirror import (
                        FullMirrorCCMBackend,
                    )
                    self._pty_backend = FullMirrorCCMBackend(self)
                    # 权限透传：CC 的权限请求经 BridgeHub 转给前端卡片，
                    # 不注册的话 channel server 120s 超时默认 deny
                    self._pty_backend._bridge.on_permission_request(
                        self._on_pty_permission_request
                    )
                    logger.info("PTY mode enabled (claude_pty persistent sessions)")
                except ImportError:
                    logger.warning(
                        "PTY mode requested but claude_pty is not installed; "
                        "staying on `claude -p` mode"
                    )
                    self._pty_enabled = False
                    return False
            self._pty_enabled = True
        else:
            if self._pty_enabled:
                logger.info("PTY mode disabled; new launches use `claude -p`")
            # NOTE: idle-session drain on toggle-off is the API layer's job
            # (PUT /api/settings/runtime awaits drain_idle_pty_sessions) —
            # this sync method must stay loop-free.
            self._pty_enabled = False
        return self._pty_enabled

    def _instance_lifecycle_lock(self, instance_id: int) -> asyncio.Lock:
        lock = self._instance_lifecycle_locks.get(instance_id)
        if lock is None:
            lock = asyncio.Lock()
            self._instance_lifecycle_locks[instance_id] = lock
        return lock

    async def wait_for_task_launch_barrier(
        self,
        instance_id: int,
        task_id: int,
        *,
        timeout: float = 30.0,
    ) -> bool:
        """Wait until a Task's pre-owner launch window is proven settled.

        Cancellation first terminally CASes the Task.  A launch that already
        spawned but has not committed ``Instance.current_task_id`` must then
        observe that CAS, abort, and reap under this same lifecycle lock.
        Returning ``True`` proves there is no retained hidden reservation for
        the Task; ``False`` is fail-closed evidence for an API 409.
        """

        lock = self._instance_lifecycle_lock(instance_id)
        try:
            await asyncio.wait_for(lock.acquire(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        try:
            reservation = self._launch_reservations.get(instance_id)
            if reservation is not None and reservation.task_id == task_id:
                return False
            return True
        finally:
            lock.release()

    def _begin_stopping(self, instance_id: int) -> None:
        """Publish one owned stop-intent token for a reusable slot."""

        self._revoke_sequential_turn_continuations_for_instance(instance_id)
        self._stopping[instance_id] = self._stopping.get(instance_id, 0) + 1

    def _end_stopping(self, instance_id: int) -> None:
        """Release only this caller's stop-intent token."""

        remaining = self._stopping.get(instance_id, 0) - 1
        if remaining > 0:
            self._stopping[instance_id] = remaining
        else:
            self._stopping.pop(instance_id, None)

    async def _acquire_terminal_task_operation_lock(
        self,
        task_id: int,
        instance_id: int,
    ) -> asyncio.Lock | None:
        """Order natural consumer settlement against Task termination.

        Worker receipt execution holds the process-wide Task operation lock
        while it calls ``stop()``. A terminal consumer cannot wait for that
        lock unconditionally: ``stop()`` may in turn be waiting for this exact
        consumer to finish. Poll the lock with bounded waits and yield as soon
        as either the durable receipt or Instance stop intent proves another
        owner will perform terminal cleanup.

        Returning the acquired lock transfers release responsibility to the
        caller. ``None`` means the consumer must leave Task/Instance state and
        terminal publication to the stop/receipt owner.
        """

        from backend.services.worker_proxy import get_task_operation_lock

        operation_lock = get_task_operation_lock(task_id)
        while True:
            if instance_id in self._stopping:
                return None
            try:
                await asyncio.wait_for(
                    operation_lock.acquire(),
                    timeout=TERMINAL_TASK_OPERATION_LOCK_POLL_SECONDS,
                )
            except asyncio.TimeoutError:
                if instance_id in self._stopping:
                    return None
                # Receipt acceptance commits before its executor can stop the
                # process. Use a fresh read while contending for the operation
                # lock so a receipt-owned stop never deadlocks on its own
                # terminal consumer.
                async with self.db_factory() as db:
                    receipt = await active_worker_task_termination_receipt(
                        db,
                        task_id,
                    )
                    await db.rollback()
                if receipt is not None:
                    return None
                continue

            if instance_id in self._stopping:
                operation_lock.release()
                return None
            return operation_lock

    @staticmethod
    def _claim_pty_terminal_owner(
        record: _OutputConsumerRecord,
        owner: str,
        *,
        take_over_completed_consumer: bool = False,
        take_over_background_waiter: bool = False,
    ) -> str:
        """Atomically claim one PTY turn's terminal bookkeeping owner.

        This helper must stay synchronous: stop and ``FullMirror.on_exit`` run
        on the same asyncio event-loop thread, so the no-await compare/set is
        the handoff that prevents either side from waiting on the other while
        holding the per-instance lifecycle lock.  A consumer that has already
        terminated can no longer touch bookkeeping, so an exact stop may
        safely take over its abandoned claim.
        """

        if owner not in {"stop", "consumer"}:
            raise ValueError(f"Unsupported PTY terminal owner: {owner}")
        current = record.pty_terminal_owner
        if current is None:
            object.__setattr__(record, "pty_terminal_owner", owner)
            return owner
        if (
            current == "consumer"
            and owner == "stop"
            and (
                (
                    take_over_completed_consumer
                    and record.task.done()
                )
                or (
                    take_over_background_waiter
                    and record.pty_background_waiting
                )
            )
        ):
            object.__setattr__(record, "pty_terminal_owner", owner)
            return owner
        if current not in {"stop", "consumer"}:
            raise RuntimeError(
                f"Invalid PTY terminal owner recorded: {current}"
            )
        return current

    async def launch(
        self,
        instance_id: int,
        prompt: str,
        task_id: int | None = None,
        task_turn_generation: int | None = None,
        cwd: str | None = None,
        model: str | None = None,
        resume_session_id: str | None = None,
        loop_iteration: int | None = None,
        git_env: dict | None = None,
        thinking_budget: int | None = None,
        effort_level: str | None = None,
        chat_initiated: bool = False,
        config_dir: str | None = None,
        provider: str = "claude",
        enable_workflows: bool = False,
        enabled_skills: dict | None = None,
        system_prompt_mode: str | None = None,
        source_log_id: int | None = None,
        current_message: str | None = None,
        queue_timestamp: float | None = None,
        codex_service_tier: str = "default",
        on_launch_admitted: Callable[[], Awaitable[None]] | None = None,
        sequential_turn_token: object | None = None,
        initiating_user_id: int | None = None,
        initiating_user_role: str = "member",
        execution_mode: str = "sandbox",
        execution_principal_kind: str = "system",
        attachment_paths: tuple[str, ...] = (),
        ssh_agent_socket_snapshot: _SshAgentSocketSnapshot | None = None,
        context_retry_permit: object | None = None,
        context_retry_claimed_source_log_id: int | None = None,
    ) -> int:
        """Admit one turn and spend continuation authority on every attempt.

        A continuation token is handed to one public launch attempt, not merely
        to the later provider-boundary callback.  Cancellation while waiting
        for the lifecycle lock and lock-local preflight rejections must revoke
        it too; otherwise the same authority could float into a later step.
        Successful durable admission consumes the token inside the transport
        fence.  The unconditional outer cleanup is then a harmless no-op, and
        also closes legacy/test paths that return without crossing that fence.
        """

        try:
            return await self._launch_impl(
                instance_id=instance_id,
                prompt=prompt,
                task_id=task_id,
                task_turn_generation=task_turn_generation,
                cwd=cwd,
                model=model,
                resume_session_id=resume_session_id,
                loop_iteration=loop_iteration,
                git_env=git_env,
                thinking_budget=thinking_budget,
                effort_level=effort_level,
                chat_initiated=chat_initiated,
                config_dir=config_dir,
                provider=provider,
                enable_workflows=enable_workflows,
                enabled_skills=enabled_skills,
                system_prompt_mode=system_prompt_mode,
                source_log_id=source_log_id,
                current_message=current_message,
                queue_timestamp=queue_timestamp,
                codex_service_tier=codex_service_tier,
                on_launch_admitted=on_launch_admitted,
                sequential_turn_token=sequential_turn_token,
                initiating_user_id=initiating_user_id,
                initiating_user_role=initiating_user_role,
                execution_mode=execution_mode,
                execution_principal_kind=execution_principal_kind,
                attachment_paths=attachment_paths,
                ssh_agent_socket_snapshot=ssh_agent_socket_snapshot,
                context_retry_permit=context_retry_permit,
                context_retry_claimed_source_log_id=(
                    context_retry_claimed_source_log_id
                ),
            )
        finally:
            self.revoke_sequential_turn_continuation(
                sequential_turn_token
            )

    async def _launch_impl(
        self,
        instance_id: int,
        prompt: str,
        task_id: int | None = None,
        task_turn_generation: int | None = None,
        cwd: str | None = None,
        model: str | None = None,
        resume_session_id: str | None = None,
        loop_iteration: int | None = None,
        git_env: dict | None = None,
        thinking_budget: int | None = None,
        effort_level: str | None = None,
        chat_initiated: bool = False,
        config_dir: str | None = None,
        provider: str = "claude",
        enable_workflows: bool = False,
        enabled_skills: dict | None = None,
        system_prompt_mode: str | None = None,
        source_log_id: int | None = None,
        initiating_user_id: int | None = None,
        initiating_user_role: str = "member",
        execution_mode: str = "sandbox",
        execution_principal_kind: str = "system",
        attachment_paths: tuple[str, ...] = (),
        ssh_agent_socket_snapshot: _SshAgentSocketSnapshot | None = None,
        current_message: str | None = None,
        queue_timestamp: float | None = None,
        codex_service_tier: str = "default",
        on_launch_admitted: Callable[[], Awaitable[None]] | None = None,
        sequential_turn_token: object | None = None,
        context_retry_permit: object | None = None,
        context_retry_claimed_source_log_id: int | None = None,
    ) -> int:
        """Atomically admit one turn into a reusable instance slot."""

        provider = (provider or "claude").lower()
        if (
            provider == "codex"
            and str(codex_service_tier or "default").strip().lower()
            == "priority"
            and (not model or str(model).strip().lower() == "default")
        ):
            # Task.model may be NULL on historical rows.  Fast validation and
            # pool selection use CCM's configured default, so the actual turn
            # must name that same model instead of asking an account-specific
            # app-server default which could advertise a different tier.
            model = settings.default_codex_model
        lifecycle_lock = self._instance_lifecycle_lock(instance_id)
        current = asyncio.current_task()
        observed_generation: int | None = None
        async def settle_launch_admitted_callback() -> None:
            assert on_launch_admitted is not None
            await _settle_instance_cleanup(on_launch_admitted())

        settled_on_launch_admitted = (
            settle_launch_admitted_callback
            if on_launch_admitted is not None
            else None
        )

        while True:
            async with lifecycle_lock:
                # This check is inside the same admission lock used by stop().
                # It closes the long retry window where a terminal consumer
                # passed its early `_stopping` check, then slept/migrated and
                # attempted an in-place replacement after stop intent began.
                if instance_id in self._stopping:
                    raise InstanceAlreadyRunningError(
                        f"Instance {instance_id} is being stopped"
                    )
                generation = self._instance_launch_generations.get(instance_id, 0)
                if observed_generation is None:
                    observed_generation = generation
                elif generation != observed_generation:
                    raise InstanceAlreadyRunningError(
                        f"Instance {instance_id} was claimed by another launch"
                    )
                process = (
                    self.processes.get(instance_id)
                    or self._process_groups.get(instance_id)
                    or self._container_exec_processes.get(instance_id)
                )
                record = self._consumer_records.get(instance_id)
                consumer = record.task if record is not None else self._tasks.get(instance_id)
                if (
                    process is not None
                    and not self._generation_reap_confirmed(
                        instance_id, process
                    )
                    and consumer is not current
                ):
                    raise InstanceAlreadyRunningError(
                        f"Instance {instance_id} is already running"
                    )
                if consumer is None or consumer is current:
                    self._instance_launch_generations[instance_id] = generation + 1
                    reservation = _LaunchReservation(
                        object(), task_id, task_turn_generation, process
                    )
                    self._launch_reservations[instance_id] = reservation
                    try:
                        async with self._cloudrouter_runtime_admission(
                            provider,
                            config_dir,
                            model,
                            service_tier=codex_service_tier,
                        ):
                            result = await self._launch_locked(
                                instance_id=instance_id,
                                prompt=prompt,
                                task_id=task_id,
                                task_turn_generation=task_turn_generation,
                                cwd=cwd,
                                model=model,
                                resume_session_id=resume_session_id,
                                loop_iteration=loop_iteration,
                                git_env=git_env,
                                thinking_budget=thinking_budget,
                                effort_level=effort_level,
                                chat_initiated=chat_initiated,
                                config_dir=config_dir,
                                provider=provider,
                                enable_workflows=enable_workflows,
                                enabled_skills=enabled_skills,
                                system_prompt_mode=system_prompt_mode,
                                source_log_id=source_log_id,
                                current_message=current_message,
                                queue_timestamp=queue_timestamp,
                                codex_service_tier=codex_service_tier,
                                on_launch_admitted=settled_on_launch_admitted,
                                sequential_turn_token=sequential_turn_token,
                                initiating_user_id=initiating_user_id,
                                initiating_user_role=initiating_user_role,
                                execution_mode=execution_mode,
                                execution_principal_kind=(
                                    execution_principal_kind
                                ),
                                attachment_paths=attachment_paths,
                                ssh_agent_socket_snapshot=(
                                    ssh_agent_socket_snapshot
                                ),
                                context_retry_permit=(
                                    context_retry_permit
                                ),
                                context_retry_claimed_source_log_id=(
                                    context_retry_claimed_source_log_id
                                ),
                            )
                    except BaseException:
                        # A token which did not reach the durable provider
                        # boundary must not float into a later, unrelated mode
                        # step.  Preflight retry requires a newly proven
                        # predecessor/authority rather than reusing this one.
                        self.revoke_sequential_turn_continuation(
                            sequential_turn_token
                        )
                        try:
                            await self._cleanup_unbound_private_runtime_tempdir(
                                instance_id
                            )
                        except Exception:
                            # Retaining an inode-fenced 0700 leaf is safer than
                            # masking the launch failure or guessing cleanup
                            # ownership after an indeterminate provider start.
                            logger.exception(
                                "Could not clean pending private runtime "
                                "directory for instance %s",
                                instance_id,
                            )
                        current_process = (
                            self.processes.get(instance_id)
                            or self._process_groups.get(instance_id)
                            or self._container_exec_processes.get(instance_id)
                        )
                        current_record = self._consumer_records.get(instance_id)
                        unresolved_generation = bool(
                            (
                                current_process is not None
                                and current_process is not process
                                and not self._generation_reap_confirmed(
                                    instance_id, current_process
                                )
                            )
                            or (
                                current_record is not None
                                and current_record.task_id == task_id
                                and not self._generation_reap_confirmed(
                                    instance_id, current_record.process
                                )
                            )
                        )
                        if (
                            task_id is not None
                            and not unresolved_generation
                            and task_id in self._task_runtime_scope_pending
                        ):
                            self._discard_task_runtime_scope_reservation(
                                task_id
                            )
                        if (
                            not unresolved_generation
                            and self._launch_reservations.get(instance_id)
                            is reservation
                        ):
                            self._launch_reservations.pop(instance_id, None)
                        raise
                    else:
                        if (
                            self._launch_reservations.get(instance_id)
                            is reservation
                        ):
                            self._launch_reservations.pop(instance_id, None)
                        try:
                            unused_continuation = (
                                self._sequential_turn_continuations.get(
                                    sequential_turn_token
                                )
                            )
                        except TypeError:
                            unused_continuation = None
                        if unused_continuation is not None:
                            self.revoke_sequential_turn_continuation(
                                sequential_turn_token
                            )
                            raise LaunchSupersededError(
                                "Sequential turn launch returned without "
                                "consuming its provider-boundary authority"
                            )
                        return result

            # Never hold admission while waiting: a terminal consumer may
            # legitimately self-launch a transient/account retry, which needs
            # this same lock.  Re-enter and re-check all maps afterwards so an
            # external contender cannot slip a second process into the slot.
            if consumer is not None:
                try:
                    await self.wait_for_output_consumer(
                        instance_id,
                        provider=provider,
                        timeout=None,
                        # A bare legacy/test task has no process-generation
                        # identity.  Passing the process here makes the waiter
                        # deliberately ignore that task and spin forever.
                        expected_process=(
                            record.process if record is not None else None
                        ),
                        preserve_error=True,
                    )
                except BaseException:
                    self.revoke_sequential_turn_continuation(
                        sequential_turn_token
                    )
                    raise

    def _prune_sequential_turn_continuations(self) -> None:
        now = time.monotonic()
        for token, continuation in list(
            self._sequential_turn_continuations.items()
        ):
            if continuation.expires_at_monotonic <= now:
                self._sequential_turn_continuations.pop(token, None)

    def revoke_sequential_turn_continuation(
        self,
        token: object | None,
    ) -> None:
        """Drop unused continuation authority without touching provider state."""

        if token is None:
            return
        try:
            self._sequential_turn_continuations.pop(token, None)
        except TypeError:
            return

    def _revoke_sequential_turn_continuations_for_instance(
        self,
        instance_id: int,
    ) -> None:
        for token, continuation in list(
            self._sequential_turn_continuations.items()
        ):
            if continuation.instance_id == instance_id:
                self._sequential_turn_continuations.pop(token, None)

    async def mint_sequential_turn_continuation(
        self,
        *,
        instance_id: int,
        task_id: int,
        task_turn_generation: int,
        source_log_id: int,
        previous_process: object,
    ) -> object:
        """Mint one next-turn authority from an exact successful mode turn.

        Dispatcher additionally proves the terminal log through exact-turn
        arbitration before calling this method.  Here we bind that proof to
        the live Manager/process generation and the immutable durable source.
        A fresh Manager has no token, and one predecessor can mint only once.
        """

        if (
            type(instance_id) is not int
            or type(task_id) is not int
            or type(task_turn_generation) is not int
            or type(source_log_id) is not int
            or instance_id <= 0
            or task_id <= 0
            or task_turn_generation < 0
            or source_log_id <= 0
            or previous_process is None
        ):
            raise LaunchSupersededError(
                "Sequential turn continuation requires exact positive identity"
            )
        lifecycle_lock = self._instance_lifecycle_lock(instance_id)

        def assert_predecessor_is_mintable() -> None:
            # Run this only while holding ``lifecycle_lock``.  In particular,
            # the one-shot marker and the live process maps must be observed
            # in the same serialization domain used by launch and stop.
            if instance_id in self._stopping:
                raise LaunchSupersededError(
                    "Sequential turn instance is being stopped"
                )
            if self.effective_exit_code(instance_id, previous_process) != 0:
                raise LaunchSupersededError(
                    "Sequential turn continuation requires a successful predecessor"
                )
            if getattr(
                previous_process,
                "_ccm_sequential_continuation_minted",
                False,
            ) is True:
                raise LaunchSupersededError(
                    "Sequential turn predecessor already minted a continuation"
                )
            current_record = self._consumer_records.get(instance_id)
            current_process = (
                self.processes.get(instance_id)
                or self._process_groups.get(instance_id)
                or self._container_exec_processes.get(instance_id)
                or (
                    current_record.process
                    if current_record is not None
                    else None
                )
            )
            if (
                current_process is not None
                and current_process is not previous_process
                and not self._generation_reap_confirmed(
                    instance_id, current_process
                )
            ):
                raise LaunchSupersededError(
                    "A replacement process already owns the sequential turn slot"
                )

        # The sole production caller reaches this method after the preceding
        # output consumer has settled and does not hold the lifecycle lock.
        # Keep the public lock boundary here so two concurrent mint callers
        # cannot both pass the predecessor marker before either publishes it.
        async with lifecycle_lock:
            assert_predecessor_is_mintable()

            async with self.db_factory() as db:
                task = (
                    await db.execute(
                        select(Task).where(
                            Task.id == task_id,
                            Task.instance_id == instance_id,
                            Task.status.in_(["in_progress", "executing"]),
                            Task.turn_generation == task_turn_generation,
                            Task.turn_source_log_id == source_log_id,
                            task_retry_not_superseded_predicate(),
                        )
                    )
                ).scalar_one_or_none()
                if task is None:
                    raise LaunchSupersededError(
                        "Sequential turn Task generation is no longer active"
                    )
                source = await db.get(LogEntry, source_log_id)
                from backend.services.terminal_arbitration import (
                    source_alias_original_log_id,
                    source_shape_is_canonical,
                )

                original_source = None
                if source is not None:
                    original_id = source_alias_original_log_id(source)
                    if original_id is not None:
                        original_source = await db.get(LogEntry, original_id)
                if (
                    source is None
                    or source.task_id != task_id
                    or source.task_retry_count != task.retry_count
                    or source.task_turn_generation != task_turn_generation
                    or source.turn_scope != "source"
                    or source.instance_id != instance_id
                    or source.actual_transport not in _ACTUAL_TURN_TRANSPORTS
                    or not source_shape_is_canonical(source, original_source)
                ):
                    raise LaunchSupersededError(
                        "Sequential turn source evidence is stale or malformed"
                    )
                task_retry_count = task.retry_count
                actual_transport = source.actual_transport

            # The durable proof above contains awaits.  Consumer cleanup can
            # still refine process maps, and cancellation/stop state may have
            # been published before this lock was acquired.  Revalidate every
            # predecessor condition immediately before publishing authority;
            # no one-shot state is burned on a stale or failed proof.
            assert_predecessor_is_mintable()
            self._prune_sequential_turn_continuations()
            # At most one next-step authority can exist for a reusable slot. A
            # newer exact successful predecessor supersedes any abandoned
            # token from the same live lifecycle. Stage the replacement before
            # removing older authority: if this predecessor cannot carry its
            # one-shot marker, publication fails without burning a still-valid
            # token minted by an earlier predecessor.
            superseded_tokens = [
                existing_token
                for existing_token, existing in self._sequential_turn_continuations.items()
                if existing.instance_id == instance_id
            ]
            token = object()
            continuation = _SequentialTurnContinuation(
                token=token,
                instance_id=instance_id,
                task_id=task_id,
                task_retry_count=task_retry_count,
                task_turn_generation=task_turn_generation,
                source_log_id=source_log_id,
                actual_transport=actual_transport,
                expires_at_monotonic=(
                    time.monotonic() + _SEQUENTIAL_TURN_TOKEN_TTL_SECONDS
                ),
            )
            self._sequential_turn_continuations[token] = continuation
            try:
                setattr(
                    previous_process,
                    "_ccm_sequential_continuation_minted",
                    True,
                )
            except BaseException:
                self._sequential_turn_continuations.pop(token, None)
                raise LaunchSupersededError(
                    "Sequential turn predecessor cannot carry one-shot state"
                )
            for superseded_token in superseded_tokens:
                self._sequential_turn_continuations.pop(
                    superseded_token, None
                )
            return token

    async def _persist_actual_turn_transport(
        self,
        *,
        instance_id: int,
        task_id: int | None,
        task_retry_count: int | None,
        task_turn_generation: int | None,
        source_log_id: int | None,
        actual_transport: str,
        sequential_turn_token: object | None = None,
        browser_child_admission: _BrowserChildLaunchAdmission | None = None,
        initiating_user_id: int | None = None,
        initiating_user_role: str = "member",
        execution_mode: str = "sandbox",
        execution_principal_kind: str = "system",
    ) -> bool:
        """Persist the final runtime route before its first provider effect.

        The Task-side instance claim plus the lifecycle-lock reservation are
        the pre-spawn owner. ``Instance.current_task_id`` is normally written
        only after spawn, so it may be NULL here but must never name a peer.
        The bound source row is write-once.  Any later ``launch()`` call sees
        an already-crossed provider boundary and must fail closed, even when it
        proposes the same route: the durable value cannot distinguish a lost
        admission acknowledgement from a turn that already performed tools or
        other external effects.  Idempotence inside one live ``launch()``
        closure is handled by ``admit_external_launch`` before this method.

        Legacy/internal launches without a durable source remain launchable but
        deliberately produce no proof, making later terminal interpretation
        fail closed. Once either side claims a source, a missing or mismatched
        launch identity is an error rather than a silent downgrade.
        """

        if actual_transport not in _ACTUAL_TURN_TRANSPORTS:
            raise ValueError(
                f"Unsupported actual turn transport: {actual_transport}"
            )
        actual_provider = _ACTUAL_TURN_PROVIDER_BY_TRANSPORT[
            actual_transport
        ]
        if task_id is None:
            return False
        if (
            type(task_id) is not int
            or type(task_retry_count) is not int
            or type(task_turn_generation) is not int
            or task_id <= 0
            or task_retry_count < 0
            or task_turn_generation < 0
        ):
            raise LaunchSupersededError(
                "Actual transport requires an exact Task generation"
            )

        reservation = self._launch_reservations.get(instance_id)
        if (
            reservation is not None
            and reservation.task_id == task_id
            and reservation.task_turn_generation is None
        ):
            # ``launch()`` accepts an omitted generation for legacy callers,
            # then resolves the exact durable generation under the lifecycle
            # lock in _launch_locked. Refine the same reservation object once;
            # the outer cleanup retains object identity across this update.
            object.__setattr__(
                reservation,
                "task_turn_generation",
                task_turn_generation,
            )
        if (
            reservation is None
            or reservation.task_id != task_id
            or reservation.task_turn_generation != task_turn_generation
        ):
            raise LaunchSupersededError(
                f"Task {task_id} lost its pre-spawn instance reservation"
            )

        async with self.db_factory() as db:
            from backend.services.worker_node_control import (
                WorkerNodeDrainingConflict,
                fence_worker_node_mutation,
            )

            try:
                # This is the final common provider boundary for Claude/Codex
                # and Browser children. Keep the node-control lock through the
                # exact Task/source/Instance commit below so an already begun
                # drain can never be followed by a provider process launch.
                await fence_worker_node_mutation(db)
            except WorkerNodeDrainingConflict as exc:
                raise LaunchSupersededError(str(exc.detail)) from exc
            browser_binding = None
            browser_owner = None
            if browser_child_admission is not None:
                admission = browser_child_admission
                if (
                    admission.child_task_id != task_id
                    or admission.child_task_retry_count != task_retry_count
                    or admission.child_task_turn_generation
                    != task_turn_generation
                    or admission.claimed_instance_id != instance_id
                    or admission.owner_task_id >= admission.child_task_id
                ):
                    raise LaunchSupersededError(
                        "Browser Agent lost its exact preflight launch identity"
                    )
                from backend.models.test_harness import (
                    TestHarnessChildBinding,
                    TestHarnessRun,
                )
                from backend.models.workspace_review import WorkspaceReviewRun
                from backend.services.test_harness_children import (
                    CHILD_RUNNING,
                    browser_binding_owner_identity,
                    browser_child_owner_error,
                    require_browser_child_binding,
                )
                from backend.services.test_harness_contracts import (
                    HARNESS_TERMINAL_STATUSES,
                )
                from backend.services.test_harness_owner_fence import (
                    TestHarnessOwnerIdentity,
                    lock_test_harness_owner,
                )

                expected_owner_identity = TestHarnessOwnerIdentity(
                    task_id=admission.owner_task_id,
                    incarnation_id=admission.owner_task_incarnation_id,
                    retry_count=admission.owner_task_retry_count,
                    turn_generation=admission.owner_task_turn_generation,
                    status=admission.owner_task_status,
                )
                try:
                    # Keep the same global order as Browser callbacks:
                    # owner Task -> Run/Workspace -> binding -> child Task ->
                    # Instance.  The owner no-op UPDATE is also SQLite's first
                    # writer reservation, so a callback cannot deadlock by
                    # holding the owner while this admission holds a binding.
                    browser_owner = await lock_test_harness_owner(
                        db,
                        expected_owner_identity,
                    )
                except RuntimeError as exc:
                    raise LaunchSupersededError(str(exc)) from exc

                harness_run = (
                    await db.execute(
                        select(TestHarnessRun)
                        .where(TestHarnessRun.id == admission.harness_run_id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                if (
                    harness_run is None
                    or harness_run.task_id != admission.owner_task_id
                    or harness_run.browser_review_job_id
                    != admission.browser_review_job_id
                    or harness_run.workspace_review_run_id
                    != admission.workspace_review_run_id
                    or harness_run.status in HARNESS_TERMINAL_STATUSES
                    or harness_run.status == "cancelling"
                    or harness_run.owner_task_incarnation_id
                    != admission.owner_task_incarnation_id
                    or harness_run.owner_task_retry_count
                    != admission.owner_task_retry_count
                    or harness_run.owner_task_turn_generation
                    != admission.owner_task_turn_generation
                    or harness_run.owner_task_status
                    != admission.owner_task_status
                ):
                    raise LaunchSupersededError(
                        "Browser Agent Harness run stopped or changed before "
                        "provider admission"
                    )

                workspace_run = None
                if admission.workspace_review_run_id is not None:
                    workspace_run = (
                        await db.execute(
                            select(WorkspaceReviewRun)
                            .where(
                                WorkspaceReviewRun.id
                                == admission.workspace_review_run_id
                            )
                            .with_for_update()
                            .execution_options(populate_existing=True)
                        )
                    ).scalar_one_or_none()
                    if (
                        workspace_run is None
                        or workspace_run.task_id != admission.owner_task_id
                        or workspace_run.harness_run_id
                        != admission.harness_run_id
                        or workspace_run.browser_review_job_id
                        != admission.browser_review_job_id
                        or workspace_run.status
                        in {"completed", "failed", "cancelled", "cancelling"}
                        or workspace_run.owner_task_incarnation_id
                        != admission.owner_task_incarnation_id
                        or workspace_run.owner_task_retry_count
                        != admission.owner_task_retry_count
                        or workspace_run.owner_task_turn_generation
                        != admission.owner_task_turn_generation
                        or workspace_run.owner_task_status
                        != admission.owner_task_status
                    ):
                        raise LaunchSupersededError(
                            "Browser Agent Workspace run stopped or changed "
                            "before provider admission"
                        )

                # stop_binding publishes ``stopping`` here before terminating
                # the child Task.  Its committed stop intent therefore makes
                # this exact CAS miss.  If admission locked the row first, the
                # provider boundary is the earlier winner and stop waits.
                binding_fence = await db.execute(
                    update(TestHarnessChildBinding)
                    .where(
                        TestHarnessChildBinding.id == admission.binding_id,
                        TestHarnessChildBinding.harness_run_id
                        == admission.harness_run_id,
                        TestHarnessChildBinding.workspace_review_run_id
                        == admission.workspace_review_run_id,
                        TestHarnessChildBinding.browser_review_job_id
                        == admission.browser_review_job_id,
                        TestHarnessChildBinding.launch_profile_version
                        == admission.launch_profile_version,
                        TestHarnessChildBinding.launch_config_digest
                        == admission.launch_config_digest,
                        TestHarnessChildBinding.owner_task_id
                        == admission.owner_task_id,
                        TestHarnessChildBinding.owner_task_incarnation_id
                        == admission.owner_task_incarnation_id,
                        TestHarnessChildBinding.owner_task_retry_count
                        == admission.owner_task_retry_count,
                        TestHarnessChildBinding.owner_task_turn_generation
                        == admission.owner_task_turn_generation,
                        TestHarnessChildBinding.owner_task_status
                        == admission.owner_task_status,
                        TestHarnessChildBinding.child_task_id
                        == admission.child_task_id,
                        TestHarnessChildBinding.child_task_incarnation_id
                        == admission.child_task_incarnation_id,
                        TestHarnessChildBinding.state == CHILD_RUNNING,
                        TestHarnessChildBinding.claimed_retry_count
                        == admission.child_task_retry_count,
                        TestHarnessChildBinding.claimed_instance_id
                        == admission.claimed_instance_id,
                    )
                    .values(state=CHILD_RUNNING)
                )
                if binding_fence.rowcount != 1:
                    raise LaunchSupersededError(
                        "Browser Agent binding stopped or changed before "
                        "provider admission"
                    )
                browser_binding = (
                    await db.execute(
                        select(TestHarnessChildBinding)
                        .where(
                            TestHarnessChildBinding.id
                            == admission.binding_id
                        )
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                if browser_binding is None:
                    raise LaunchSupersededError(
                        "Browser Agent binding disappeared before provider "
                        "admission"
                    )
                try:
                    current_owner_identity = browser_binding_owner_identity(
                        browser_binding
                    )
                except RuntimeError as exc:
                    raise LaunchSupersededError(str(exc)) from exc
                if current_owner_identity != expected_owner_identity:
                    raise LaunchSupersededError(
                        "Browser Agent owner identity changed before provider "
                        "admission"
                    )

            task_identity_predicates = (
                Task.id == task_id,
                Task.instance_id == instance_id,
                Task.retry_count == task_retry_count,
                Task.turn_generation == task_turn_generation,
                Task.status.in_(["in_progress", "executing"]),
                task_retry_not_superseded_predicate(),
                no_active_worker_task_termination_predicate(),
            )
            # SELECT .. FOR UPDATE is an exact row lock on PostgreSQL/MySQL but
            # is ignored by SQLite.  Make the transaction's first Task access
            # a conditional write fence so cancellation either commits first
            # and makes this CAS miss, or waits until transport admission is
            # durable.  SQLAlchemy enables matched-row semantics for MySQL, so
            # this no-op assignment has a stable rowcount on every supported
            # dialect.
            task_fence = await db.execute(
                update(Task)
                .where(*task_identity_predicates)
                .values(turn_source_log_id=Task.turn_source_log_id)
            )
            if task_fence.rowcount != 1:
                raise LaunchSupersededError(
                    f"Task {task_id} lost its exact launch generation"
                )
            task = (
                await db.execute(
                    select(Task)
                    .where(*task_identity_predicates)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if task is None:
                raise LaunchSupersededError(
                    f"Task {task_id} lost its exact launch generation"
                )

            frozen_principal = (
                initiating_user_id,
                initiating_user_role,
                execution_mode,
                execution_principal_kind,
            )
            if frozen_principal != (
                task.execution_user_id,
                task.execution_user_role,
                task.execution_mode,
                task.execution_principal_kind,
            ):
                raise LaunchSupersededError(
                    "Task execution principal changed at the provider boundary"
                )

            # A local user's active state and role are mutable authority, not
            # facts that a queued/retried turn may cache indefinitely.  Keep
            # this conditional writer in the same transaction as the exact
            # Task fence and actual_transport CAS.  A concurrent demotion or
            # deactivation therefore either commits first and makes this gate
            # miss, or waits until this already-admitted provider boundary is
            # durable.  Delegated users belong to the authoritative Manager
            # database and are instead covered by the generation-bound launch
            # permit obtained immediately before this transaction.
            if execution_principal_kind == "user":
                if (
                    isinstance(initiating_user_id, bool)
                    or not isinstance(initiating_user_id, int)
                    or initiating_user_id <= 0
                    or initiating_user_role
                    not in {"member", "admin", "super_admin"}
                    or execution_mode
                    != (
                        "unrestricted"
                        if initiating_user_role in {"admin", "super_admin"}
                        else "sandbox"
                    )
                ):
                    raise LaunchSupersededError(
                        "Task turn initiator identity is invalid at the "
                        "provider boundary"
                    )
                from backend.models.user import User

                principal_fence = await db.execute(
                    update(User)
                    .where(
                        User.id == initiating_user_id,
                        User.is_active.is_(True),
                        User.role == initiating_user_role,
                    )
                    .values(role=User.role)
                    .execution_options(synchronize_session=False)
                )
                if principal_fence.rowcount != 1:
                    raise LaunchSupersededError(
                        "Task turn initiator role or active state changed at "
                        "the provider boundary"
                    )

            if browser_binding is not None:
                try:
                    require_browser_child_binding(browser_binding, task)
                except RuntimeError as exc:
                    raise LaunchSupersededError(str(exc)) from exc
                owner_error = browser_child_owner_error(
                    browser_binding,
                    browser_owner,
                )
                if owner_error is not None:
                    raise LaunchSupersededError(owner_error)
                if (
                    browser_binding.state != CHILD_RUNNING
                    or browser_binding.claimed_retry_count
                    != task_retry_count
                    or browser_binding.claimed_instance_id != instance_id
                    or browser_binding.browser_review_job_id
                    != browser_child_admission.browser_review_job_id
                    or browser_binding.launch_config_digest
                    != browser_child_admission.launch_config_digest
                ):
                    raise LaunchSupersededError(
                        "Browser Agent exact launch authority changed before "
                        "provider admission"
                    )

            instance = (
                await db.execute(
                    select(Instance)
                    .where(Instance.id == instance_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if instance is None:
                raise InstanceNotFoundError(
                    f"Instance {instance_id} disappeared before transport admission"
                )
            if (
                instance.current_task_id not in (None, task_id)
                or instance.current_plan_run_id is not None
            ):
                raise LaunchSupersededError(
                    f"Instance {instance_id} is owned by another launch"
                )
            # Persist the provider selected by the final runtime route in the
            # same Task -> Instance -> source transaction, before any model
            # process or native turn can start.  The later PID commit both
            # rechecks and rewrites this value for defense in depth.
            instance.provider = actual_provider

            bound_source_id = task.turn_source_log_id
            if bound_source_id is None:
                if source_log_id is not None:
                    raise LaunchSupersededError(
                        f"Task {task_id} has no bound source for this launch"
                    )
                # Historical callers may not yet have terminal-arbitration
                # identity. Never synthesize proof from their planned route.
                await db.commit()
                return False
            if type(bound_source_id) is not int or bound_source_id <= 0:
                raise LaunchSupersededError(
                    f"Task {task_id} has an invalid bound source pointer"
                )
            if type(source_log_id) is not int or source_log_id <= 0:
                raise LaunchSupersededError(
                    f"Task {task_id} launch omitted its exact source identity"
                )

            source = (
                await db.execute(
                    select(LogEntry)
                    .where(LogEntry.id == bound_source_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            from backend.services.terminal_arbitration import (
                source_alias_original_log_id,
                source_shape_is_canonical,
            )

            alias_original_id = (
                source_alias_original_log_id(source)
                if source is not None
                else None
            )
            original_source = None
            if alias_original_id is not None:
                original_source = (
                    await db.execute(
                        select(LogEntry)
                        .where(LogEntry.id == alias_original_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
            if (
                source is None
                or source.task_id != task_id
                or source.task_retry_count != task_retry_count
                or source.task_turn_generation != task_turn_generation
                or source.turn_scope != "source"
                or source.instance_id != instance_id
                or not source_shape_is_canonical(source, original_source)
            ):
                raise LaunchSupersededError(
                    f"Task {task_id} bound source is stale or malformed"
                )

            source_argument_matches = source.id == source_log_id or (
                alias_original_id is not None
                and alias_original_id == source_log_id
            )
            if not source_argument_matches:
                raise LaunchSupersededError(
                    f"Task {task_id} launch source does not match its bound source"
                )

            if source.actual_transport is not None:
                self._prune_sequential_turn_continuations()
                try:
                    continuation = self._sequential_turn_continuations.get(
                        sequential_turn_token
                    )
                except TypeError:
                    continuation = None
                continuation_matches = bool(
                    continuation is not None
                    and continuation.token is sequential_turn_token
                    and continuation.instance_id == instance_id
                    and continuation.task_id == task_id
                    and continuation.task_retry_count == task_retry_count
                    and continuation.task_turn_generation
                    == task_turn_generation
                    and continuation.source_log_id == bound_source_id
                    and continuation.actual_transport == source.actual_transport
                    and continuation.actual_transport == actual_transport
                    and continuation.expires_at_monotonic > time.monotonic()
                )
                if not continuation_matches:
                    raise LaunchSupersededError(
                        f"Task {task_id} already crossed its provider boundary "
                        f"through {source.actual_transport}; fresh launch was blocked"
                    )
                # Consume before the commit/provider effect.  If commit loses
                # its acknowledgement or the caller is cancelled afterwards,
                # the token remains spent and the uncertain turn cannot replay.
                consumed = self._sequential_turn_continuations.pop(
                    sequential_turn_token,
                    None,
                )
                if consumed is not continuation:
                    raise LaunchSupersededError(
                        "Sequential turn continuation was already consumed"
                    )
            else:
                if sequential_turn_token is not None:
                    raise LaunchSupersededError(
                        "Sequential turn token cannot authorize an initial launch"
                    )
                transport_update = await db.execute(
                    update(LogEntry)
                    .where(
                        LogEntry.id == bound_source_id,
                        LogEntry.task_id == task_id,
                        LogEntry.task_retry_count == task_retry_count,
                        LogEntry.task_turn_generation == task_turn_generation,
                        LogEntry.turn_scope == "source",
                        LogEntry.actual_transport.is_(None),
                    )
                    .values(actual_transport=actual_transport)
                )
                if transport_update.rowcount != 1:
                    raise LaunchSupersededError(
                        f"Task {task_id} actual transport CAS was superseded"
                    )
            await db.commit()

        if self._launch_reservations.get(instance_id) is not reservation:
            raise LaunchSupersededError(
                f"Task {task_id} lost its transport launch reservation"
            )
        return True

    async def _launch_locked(
        self,
        instance_id: int,
        prompt: str,
        task_id: int | None = None,
        task_turn_generation: int | None = None,
        cwd: str | None = None,
        model: str | None = None,
        resume_session_id: str | None = None,
        loop_iteration: int | None = None,
        git_env: dict | None = None,
        thinking_budget: int | None = None,
        effort_level: str | None = None,
        chat_initiated: bool = False,
        config_dir: str | None = None,
        provider: str = "claude",
        enable_workflows: bool = False,
        enabled_skills: dict | None = None,
        system_prompt_mode: str | None = None,
        source_log_id: int | None = None,
        current_message: str | None = None,
        queue_timestamp: float | None = None,
        codex_service_tier: str = "default",
        on_launch_admitted: Callable[[], Awaitable[None]] | None = None,
        sequential_turn_token: object | None = None,
        initiating_user_id: int | None = None,
        initiating_user_role: str = "member",
        execution_mode: str = "sandbox",
        execution_principal_kind: str = "system",
        attachment_paths: tuple[str, ...] = (),
        ssh_agent_socket_snapshot: _SshAgentSocketSnapshot | None = None,
        context_retry_permit: object | None = None,
        context_retry_claimed_source_log_id: int | None = None,
    ) -> int:
        """Launch a Claude Code subprocess for the given instance.

        If resume_session_id is provided, uses --resume to continue the conversation.
        loop_iteration is recorded on every LogEntry produced by this invocation so
        that loop-task chat history can be grouped by iteration in the frontend.
        """
        provider = (provider or "claude").lower()
        launch_boundary_attempted = False
        launch_boundary_completed = False

        selected_actual_transport: str | None = None

        def context_retry_wire_authority() -> dict[str, object] | None:
            """Render only the narrow fields understood by Manager admission."""

            if context_retry_permit is None:
                return None
            authority_id = getattr(
                context_retry_permit, "authority_id", None
            )
            permit_task_id = getattr(context_retry_permit, "task_id", None)
            permit_retry_count = getattr(
                context_retry_permit, "retry_count", None
            )
            from_generation = getattr(
                context_retry_permit, "turn_generation", None
            )
            source_log_id = getattr(
                context_retry_permit, "turn_source_log_id", None
            )
            if (
                not isinstance(authority_id, str)
                or len(authority_id) != 32
                or any(
                    char not in "0123456789abcdef"
                    for char in authority_id
                )
                or permit_task_id != task_id
                or type(permit_retry_count) is not int
                or type(from_generation) is not int
                or type(source_log_id) is not int
                or source_log_id <= 0
                or type(context_retry_claimed_source_log_id) is not int
                or context_retry_claimed_source_log_id <= 0
                or permit_retry_count != task_retry_count
                or task_turn_generation != from_generation + 1
            ):
                raise LaunchSupersededError(
                    "Context retry launch authority is malformed or stale"
                )
            return {
                "authority_id": authority_id,
                "retry_count": permit_retry_count,
                "from_generation": from_generation,
                "source_log_id": source_log_id,
                "claimed_source_log_id": (
                    context_retry_claimed_source_log_id
                ),
            }

        async def require_current_initiating_principal(
            actual_transport: str,
        ) -> None:
            """Revalidate a frozen user principal at the provider boundary.

            The Dispatcher performs the same check when it first consumes a
            queued message.  Automatic transient retries and account rotation
            can wait and relaunch without returning through that queue, so the
            final common launch boundary must not trust their cached role.
            System turns intentionally have no local user id. Worker-managed
            mirrors still require an exact-generation Manager permit; only
            genuinely Worker-local derived tasks retain local system authority.
            """

            if execution_principal_kind not in {
                "user",
                "deployment_token",
                "system",
                "delegated_user",
                "delegated_deployment_token",
            }:
                raise LaunchSupersededError(
                    "Task turn principal kind is invalid"
                )
            if initiating_user_id is None:
                if execution_principal_kind in {
                    "deployment_token",
                    "delegated_deployment_token",
                }:
                    if (
                        initiating_user_role != "super_admin"
                        or execution_mode != "unrestricted"
                        or task_id is None
                    ):
                        raise LaunchSupersededError(
                            "Deployment-token Task principal is invalid"
                        )
                    if execution_principal_kind == "delegated_deployment_token":
                        from backend.services.worker_launch_admission import (
                            WorkerLaunchAdmissionError,
                            request_worker_launch_admission,
                        )

                        try:
                            await request_worker_launch_admission(
                                broadcaster=self.broadcaster,
                                task_id=task_id,
                                incarnation_id=task_incarnation_id or "",
                                retry_count=task_retry_count,
                                turn_generation=task_turn_generation,
                                actual_transport=actual_transport,
                                execution_principal={
                                    "execution_user_id": initiating_user_id,
                                    "execution_user_role": initiating_user_role,
                                    "execution_mode": execution_mode,
                                    "execution_principal_kind": (
                                        execution_principal_kind
                                    ),
                                },
                                context_retry=(
                                    context_retry_wire_authority()
                                ),
                            )
                        except WorkerLaunchAdmissionError as exc:
                            raise LaunchSupersededError(str(exc)) from exc
                    return
                if (
                    execution_principal_kind != "system"
                    or initiating_user_role != "member"
                    or execution_mode != "sandbox"
                ):
                    raise LaunchSupersededError(
                        "Source-less unrestricted Task principal is invalid"
                    )
                if settings.ccm_node_role == "worker" and worker_managed_task:
                    from backend.services.worker_launch_admission import (
                        WorkerLaunchAdmissionError,
                        request_worker_launch_admission,
                    )

                    try:
                        await request_worker_launch_admission(
                            broadcaster=self.broadcaster,
                            task_id=task_id,
                            incarnation_id=task_incarnation_id or "",
                            retry_count=task_retry_count,
                            turn_generation=task_turn_generation,
                            actual_transport=actual_transport,
                            execution_principal={
                                "execution_user_id": initiating_user_id,
                                "execution_user_role": initiating_user_role,
                                "execution_mode": execution_mode,
                                "execution_principal_kind": (
                                    execution_principal_kind
                                ),
                            },
                            context_retry=(
                                context_retry_wire_authority()
                            ),
                        )
                    except WorkerLaunchAdmissionError as exc:
                        raise LaunchSupersededError(str(exc)) from exc
                return
            if execution_principal_kind not in {
                "user",
                "delegated_user",
            }:
                raise LaunchSupersededError(
                    "Only a user Task principal may carry a user id"
                )
            if (
                isinstance(initiating_user_id, bool)
                or not isinstance(initiating_user_id, int)
                or initiating_user_id <= 0
            ):
                raise LaunchSupersededError(
                    "Task turn initiator identity is invalid"
                )

            # A delegated user belongs to the authoritative Manager database,
            # not the Worker. Its shape and immutable envelope were verified
            # at the authenticated control-plane boundary. The Worker cannot
            # query that foreign User row; fresh messages are re-authorized by
            # the Manager before a new envelope is issued.
            if execution_principal_kind == "delegated_user":
                expected_execution_mode = (
                    "unrestricted"
                    if initiating_user_role in {"admin", "super_admin"}
                    else "sandbox"
                )
                if expected_execution_mode != execution_mode:
                    raise LaunchSupersededError(
                        "Delegated Task principal role/mode mismatch"
                    )
                from backend.services.worker_launch_admission import (
                    WorkerLaunchAdmissionError,
                    request_worker_launch_admission,
                )

                try:
                    await request_worker_launch_admission(
                        broadcaster=self.broadcaster,
                        task_id=task_id,
                        incarnation_id=task_incarnation_id or "",
                        retry_count=task_retry_count,
                        turn_generation=task_turn_generation,
                        actual_transport=actual_transport,
                        execution_principal={
                            "execution_user_id": initiating_user_id,
                            "execution_user_role": initiating_user_role,
                            "execution_mode": execution_mode,
                            "execution_principal_kind": execution_principal_kind,
                        },
                        context_retry=context_retry_wire_authority(),
                    )
                except WorkerLaunchAdmissionError as exc:
                    raise LaunchSupersededError(str(exc)) from exc
                return

            from backend.models.user import User

            async with self.db_factory() as principal_db:
                principal = await principal_db.get(User, initiating_user_id)
                if principal is None or not principal.is_active:
                    raise LaunchSupersededError(
                        "Task turn initiator is no longer active"
                    )
                principal_role = principal.role

            expected_execution_mode = (
                "unrestricted"
                if principal_role in {"admin", "super_admin"}
                else "sandbox"
            )
            if (
                principal_role != initiating_user_role
                or expected_execution_mode != execution_mode
            ):
                raise LaunchSupersededError(
                    "Task turn initiator role changed after admission"
                )

        async def admit_external_launch(actual_transport: str) -> None:
            """Persist the actual route and cross the provider boundary once."""

            nonlocal launch_boundary_attempted, launch_boundary_completed
            nonlocal selected_actual_transport
            if launch_boundary_completed:
                if selected_actual_transport != actual_transport:
                    raise RuntimeError(
                        "Launch admission attempted to change actual transport"
                    )
                return
            if launch_boundary_attempted:
                raise RuntimeError("Launch admission callback is already running")
            launch_boundary_attempted = True
            selected_actual_transport = actual_transport
            if (
                provider == "claude"
                and delivery_task
            ):
                from backend.services.task_agent_isolation import (
                    discover_linked_worktree_git_read_boundary,
                )

                current_git_boundary = (
                    discover_linked_worktree_git_read_boundary(
                        cwd or os.getcwd()
                    )
                )
                if task_private_tmpdir is None:
                    raise LaunchSupersededError(
                        "Claude Delivery private runtime boundary disappeared"
                    )
                task_private_tmpdir.assert_valid()
                if current_git_boundary != claude_delivery_git_boundary:
                    raise LaunchSupersededError(
                        "Claude Delivery Git isolation boundary changed at "
                        "provider launch"
                    )
            # Retry/backoff and account migration may have taken long enough
            # for an administrator to be disabled or demoted.  Revalidate as
            # late as possible before any provider process/turn is admitted.
            await require_current_initiating_principal(actual_transport)
            if ssh_agent_socket_snapshot is not None:
                ssh_agent_socket_snapshot.assert_current()
            # Sharing may be enabled after the initial Task snapshot. Recheck
            # at the last common boundary for Claude PTY/direct and Codex
            # app-server/direct routes; an unavailable check is also a veto.
            await _require_unshared_project_agent_launch(
                task_project_id,
                self.db_factory,
            )
            await self._persist_actual_turn_transport(
                instance_id=instance_id,
                task_id=task_id,
                task_retry_count=task_retry_count,
                task_turn_generation=task_turn_generation,
                source_log_id=source_log_id,
                actual_transport=actual_transport,
                sequential_turn_token=sequential_turn_token,
                browser_child_admission=browser_child_admission,
                initiating_user_id=initiating_user_id,
                initiating_user_role=initiating_user_role,
                execution_mode=execution_mode,
                execution_principal_kind=execution_principal_kind,
            )
            if on_launch_admitted is not None:
                await on_launch_admitted()
            launch_boundary_completed = True

        async def admit_codex_app_server_transport() -> None:
            await admit_external_launch("codex_app_server")

        async def admit_claude_pty_transport() -> None:
            await admit_external_launch("claude_pty")

        cloudrouter_account = self._cloudrouter_account_for_runtime_home(
            provider, config_dir
        )
        # The API and Dispatcher serialize deletion/reservation with this
        # lifecycle lock.  Verify the reusable slot before creating config
        # files, containers, or a real agent process; a post-spawn rowcount
        # check remains below as defense against cross-process DB mutation.
        task_retry_count: int | None = None
        task_status: str | None = None
        task_incarnation_id: str | None = None
        task_project_id: int | None = None
        task_skill_context = ""
        codex_monitor_enabled = False
        pr_review_task = False
        browser_review_task = False
        browser_child_admission: _BrowserChildLaunchAdmission | None = None
        worker_managed_task = False
        task_ssh_capabilities: set[str] = set()
        task_ssh_broker_only = False
        task_ssh_protected_path_values: tuple[str, ...] = ()
        task_git_credential_read_path_values: tuple[str, ...] = ()
        task_attachment_read_path_values: tuple[str, ...] = ()
        task_git_metadata_read_path_values: tuple[str, ...] = ()
        task_git_metadata_identity_fingerprint: tuple[
            tuple[object, ...], ...
        ] = ()
        task_private_tmpdir = None
        delivery_task = False
        if execution_mode not in {"sandbox", "unrestricted"}:
            raise LaunchSupersededError("Invalid Task execution mode")
        unrestricted_admin_turn = bool(
            execution_mode == "unrestricted"
            and initiating_user_role in {"admin", "super_admin"}
        )
        # An unrestricted administrator turn may inherit the service node's
        # exact ssh-agent socket. For delegated principals this is the Worker
        # node's own operator socket (never a path transported from Manager).
        # The inode/owner snapshot is captured locally and revalidated at the
        # final provider boundary. Member/system and special workflows are
        # vetoed below.
        host_ssh_agent_allowed = bool(
            unrestricted_admin_turn
            and (
                (
                    execution_principal_kind in {"user", "delegated_user"}
                    and initiating_user_id is not None
                )
                or execution_principal_kind in {
                    "deployment_token",
                    "delegated_deployment_token",
                }
            )
        )
        validated_attachment_paths: tuple[str, ...] = ()
        if attachment_paths:
            from backend.api.uploads import (
                UploadAttachmentValidationError,
                validate_upload_attachments,
            )

            try:
                validated = validate_upload_attachments(
                    file_paths=list(attachment_paths),
                )
            except UploadAttachmentValidationError as exc:
                raise LaunchSupersededError(
                    "Task attachment is no longer a managed upload"
                ) from exc
            validated_attachment_paths = tuple(
                upload.path for upload in validated
            )
        if (
            execution_mode == "unrestricted"
            and initiating_user_role not in {"admin", "super_admin"}
        ):
            raise LaunchSupersededError(
                "Only an administrator may launch an unrestricted Task turn"
            )
        async with self.db_factory() as db:
            if await db.get(Instance, instance_id) is None:
                raise InstanceNotFoundError(
                    f"Instance {instance_id} no longer exists"
                )
            if task_id is not None:
                generation_predicates = [
                    Task.id == task_id,
                    Task.instance_id == instance_id,
                    Task.status.in_(["in_progress", "executing"]),
                ]
                if task_turn_generation is not None:
                    generation_predicates.append(
                        Task.turn_generation == task_turn_generation
                    )
                generation_row = (
                    await db.execute(
                        select(
                            Task.retry_count,
                            Task.turn_generation,
                            Task.status,
                        ).where(*generation_predicates)
                    )
                ).first()
                if generation_row is None:
                    raise LaunchSupersededError(
                        f"Task {task_id} no longer owns instance {instance_id}"
                    )
                task_retry_count = generation_row[0]
                task_turn_generation = generation_row[1]
                task_status = generation_row[2]
                task = await db.get(Task, task_id)
                if task is None:
                    raise LaunchSupersededError(
                        f"Task {task_id} disappeared before launch"
                    )
                task_incarnation_id = task.incarnation_id
                task_project_id = task.project_id
                task_principal_snapshot = (
                    task.execution_user_id,
                    task.execution_user_role,
                    task.execution_mode,
                    task.execution_principal_kind,
                )
                if task_principal_snapshot != (
                    initiating_user_id,
                    initiating_user_role,
                    execution_mode,
                    execution_principal_kind,
                ):
                    raise LaunchSupersededError(
                        "Task execution principal changed before launch"
                    )
                from backend.services.skill_context import (
                    is_worker_managed_task_metadata,
                )

                worker_managed_task = is_worker_managed_task_metadata(
                    task.metadata_
                )
                if worker_managed_task:
                    if execution_principal_kind not in {
                        "delegated_user",
                        "delegated_deployment_token",
                        "system",
                    }:
                        raise LaunchSupersededError(
                            "Worker-managed Task has a non-delegated principal"
                        )
                elif execution_principal_kind in {
                    "delegated_user",
                    "delegated_deployment_token",
                }:
                    raise LaunchSupersededError(
                        "Delegated principal is valid only for a Worker-managed "
                        "Task"
                    )
                await _require_unshared_project_agent_launch(
                    task_project_id,
                    self.db_factory,
                )
                from backend.services.task_agent_isolation import (
                    prepare_task_working_directory,
                )

                cwd = prepare_task_working_directory(
                    task_id,
                    task_incarnation_id or "",
                    cwd,
                    has_explicit_workspace=bool(
                        task.project_id is not None or task.target_repo
                    ),
                )
                delivery_task = await _require_delivery_workspace_launch_boundary(
                    db,
                    task,
                    cwd=cwd,
                )
                # Role-based unrestricted execution applies to ordinary local
                # Tasks and to Worker-local mirrors carrying a Manager-
                # delegated administrator principal.  A Manager mirror with a
                # non-null ``worker_id`` never launches locally; legacy
                # cross-CCM shares and purpose-built workflows retain their
                # independent isolation contracts.
                if task.worker_id is not None or task.shared_from_id is not None:
                    unrestricted_admin_turn = False
                    host_ssh_agent_allowed = False
                if delivery_task:
                    if (
                        provider not in {"claude", "codex"}
                        or provider != task.provider
                        or model != task.model
                        or codex_service_tier != task.codex_service_tier
                        or effort_level != task.effort_level
                        or enable_workflows
                        or bool(enabled_skills)
                        or bool(task.system_prompt_mode)
                        or bool(system_prompt_mode)
                    ):
                        raise LaunchSupersededError(
                            "Delivery Developer launch no longer matches its "
                            "frozen execution policy"
                        )
                    # Dispatcher normally builds project Git credentials before
                    # it knows which pending Task won the queue.  The Delivery
                    # boundary deliberately drops that ambient authority here;
                    # only the Controller publisher may receive push credentials.
                    # Codex's network-isolated app-server installs the same
                    # safe Git environment internally and forbids any caller
                    # environment. Claude direct/PTY needs it supplied here.
                    git_env = (
                        dict(_DELIVERY_SAFE_GIT_ENV)
                        if provider == "claude"
                        else None
                    )
                from backend.services.pr_review_runtime import (
                    is_pr_sandbox_task,
                )

                pr_review_task = is_pr_sandbox_task(task)
                if pr_review_task or delivery_task:
                    unrestricted_admin_turn = False
                    host_ssh_agent_allowed = False
                from backend.models.test_harness import TestHarnessChildBinding
                from backend.services.test_harness_children import (
                    CHILD_RUNNING,
                    browser_binding_owner_identity,
                    browser_child_owner_error,
                    require_browser_child_binding,
                )
                from backend.services.test_harness_owner_fence import (
                    test_harness_owner_terminal_gate_matches,
                )

                browser_binding = await db.scalar(
                    select(TestHarnessChildBinding).where(
                        TestHarnessChildBinding.child_task_id == task.id
                    )
                )
                isolated_browser_marker = bool(
                    (task.metadata_ or {}).get("isolated_browser_agent") is True
                )
                if browser_binding is not None:
                    try:
                        require_browser_child_binding(browser_binding, task)
                    except RuntimeError as exc:
                        raise LaunchSupersededError(str(exc)) from exc
                    browser_owner = await db.get(
                        Task,
                        browser_binding.owner_task_id,
                    )
                    owner_error = browser_child_owner_error(
                        browser_binding,
                        browser_owner,
                    )
                    if owner_error is not None:
                        raise LaunchSupersededError(owner_error)
                    owner_identity = browser_binding_owner_identity(
                        browser_binding
                    )
                    if test_harness_owner_terminal_gate_matches(
                        browser_owner,
                        owner_identity,
                    ):
                        raise LaunchSupersededError(
                            "Browser Agent owner is already terminalizing"
                        )
                    if (
                        browser_binding.state != CHILD_RUNNING
                        or browser_binding.claimed_retry_count
                        != task.retry_count
                        or browser_binding.claimed_instance_id != instance_id
                    ):
                        raise LaunchSupersededError(
                            "Browser Agent launch does not own its exact queue claim"
                        )
                    task_browser_job_id = browser_binding.browser_review_job_id
                else:
                    task_browser_job_id = None
                supplied_browser_job_id = (
                    enabled_skills.get("browser-review")
                    if isinstance(enabled_skills, dict)
                    else None
                )
                browser_review_task = bool(browser_binding is not None)
                if browser_review_task:
                    unrestricted_admin_turn = False
                    host_ssh_agent_allowed = False
                if isolated_browser_marker and browser_binding is None:
                    raise LaunchSupersededError(
                        "Browser Agent launch has no durable isolation binding"
                    )
                if supplied_browser_job_id is not None and not browser_review_task:
                    raise LaunchSupersededError(
                        "Browser Agent MCP cannot launch without a durable binding"
                    )
                if browser_review_task and (
                    provider != browser_binding.provider
                    or model != browser_binding.model
                    or effort_level != browser_binding.reasoning_effort
                    or codex_service_tier
                    != browser_binding.codex_service_tier
                    or enable_workflows != task.enable_workflows
                    or enabled_skills
                    != {"browser-review": task_browser_job_id}
                ):
                    raise LaunchSupersededError(
                        "Browser Agent launch no longer matches its immutable "
                        "execution profile"
                    )
                if browser_review_task and resume_session_id is not None:
                    raise LaunchSupersededError(
                        "Browser Agent Tasks must launch one fresh provider "
                        "session and can never resume"
                    )
                if (
                    browser_review_task
                    and supplied_browser_job_id != task_browser_job_id
                ):
                    raise LaunchSupersededError(
                        "Browser Agent launch lost its exact bound MCP job"
                    )
                if browser_review_task and (
                    not isinstance(settings.auth_token, str)
                    or not settings.auth_token.strip()
                    ):
                    raise LaunchSupersededError(
                        "Browser Agent launch requires AUTH_TOKEN-backed "
                        "scoped authentication"
                    )
                if execution_mode == "unrestricted" and not unrestricted_admin_turn:
                    raise LaunchSupersededError(
                        "This Task execution channel cannot run unrestricted"
                    )
                if browser_review_task:
                    if (
                        not isinstance(browser_binding.id, str)
                        or not isinstance(
                            browser_binding.browser_review_job_id,
                            str,
                        )
                        or not isinstance(
                            browser_binding.harness_run_id,
                            str,
                        )
                        or (
                            browser_binding.workspace_review_run_id is not None
                            and not isinstance(
                                browser_binding.workspace_review_run_id,
                                str,
                            )
                        )
                        or type(browser_binding.launch_profile_version) is not int
                        or not isinstance(
                            browser_binding.launch_config_digest,
                            str,
                        )
                        or not isinstance(
                            browser_binding.child_task_incarnation_id,
                            str,
                        )
                        or not isinstance(task.incarnation_id, str)
                    ):
                        raise LaunchSupersededError(
                            "Browser Agent binding has incomplete launch identity"
                        )
                    browser_child_admission = _BrowserChildLaunchAdmission(
                        binding_id=browser_binding.id,
                        browser_review_job_id=(
                            browser_binding.browser_review_job_id
                        ),
                        harness_run_id=browser_binding.harness_run_id,
                        workspace_review_run_id=(
                            browser_binding.workspace_review_run_id
                        ),
                        launch_profile_version=(
                            browser_binding.launch_profile_version
                        ),
                        launch_config_digest=(
                            browser_binding.launch_config_digest
                        ),
                        owner_task_id=owner_identity.task_id,
                        owner_task_incarnation_id=owner_identity.incarnation_id,
                        owner_task_retry_count=owner_identity.retry_count,
                        owner_task_turn_generation=owner_identity.turn_generation,
                        owner_task_status=owner_identity.status,
                        child_task_id=task.id,
                        child_task_incarnation_id=task.incarnation_id,
                        child_task_retry_count=task.retry_count,
                        child_task_turn_generation=task.turn_generation,
                        claimed_instance_id=instance_id,
                    )
                if not pr_review_task:
                    from backend.services.task_agent_isolation import (
                        explicit_git_credential_paths,
                    )
                    from backend.services.task_ssh_access import (
                        task_git_non_overridable_paths,
                        task_ssh_policy_context,
                        task_ssh_protected_paths,
                        task_ssh_runtime_policy,
                    )

                    task_ssh_runtime = await task_ssh_runtime_policy(
                        db,
                        task,
                    )
                    if delivery_task and task_ssh_runtime.broker_only:
                        # Delivery Developer turns have a frozen networkless
                        # policy.  A durable SSH grant is conflicting ambient
                        # authority even when its Profile is stale/disabled;
                        # never make it disappear merely because Delivery
                        # intentionally omits the ccm_ssh MCP server.
                        raise LaunchSupersededError(
                            "Delivery Developer Task has a durable SSH grant"
                        )
                    task_ssh_capabilities = set(
                        task_ssh_runtime.capabilities
                    )
                    task_ssh_broker_only = bool(
                        task_ssh_runtime.broker_only
                        and not unrestricted_admin_turn
                    )
                    if task_ssh_broker_only:
                        # Member/system turns with a durable grant use only
                        # the audited broker route.  For an unrestricted
                        # administrator turn the same grant is an additional
                        # managed capability; it must not silently revoke the
                        # execution node's ordinary SSH agent authority.
                        host_ssh_agent_allowed = False
                    explicit_git_paths = (
                        ()
                        if task_ssh_broker_only or browser_review_task
                        else explicit_git_credential_paths(git_env)
                    )
                    # Every local Task must be unable to inspect Manager SSH,
                    # provider-account, and scoped runtime credentials. A Task
                    # with grants additionally loses direct network access and
                    # reaches SSH only through the broker MCP.
                    if not unrestricted_admin_turn:
                        task_ssh_protected_path_values = (
                            await task_ssh_protected_paths(
                                db,
                                task=task,
                                working_directory=cwd,
                                include_direct_git_credentials=True,
                                allowed_credential_paths=explicit_git_paths,
                            )
                        )
                    if not task_ssh_broker_only:
                        from backend.services.task_agent_isolation import (
                            require_git_credentials_outside_protected_paths,
                        )

                        task_git_credential_read_path_values = (
                            require_git_credentials_outside_protected_paths(
                                git_env,
                                task_ssh_protected_path_values,
                                allowed_read_paths=explicit_git_paths,
                                non_overridable_paths=task_git_non_overridable_paths(
                                    *((config_dir,) if config_dir else ()),
                                ),
                            )
                        )
                    task_attachment_read_path_values = (
                        validated_attachment_paths
                    )
                if browser_review_task:
                    if task_ssh_broker_only or task_ssh_capabilities:
                        raise LaunchSupersededError(
                            "Browser Agent Tasks cannot inherit Task SSH grants"
                        )
                    # A fixed Browser child has no repository or credential
                    # authority. Its only network path is the exact required
                    # Browser MCP server admitted below.
                    git_env = None
                from backend.services.skill_context import (
                    codex_monitor_supported_for_scope,
                )

                codex_monitor_enabled = codex_monitor_supported_for_scope(
                    provider=provider,
                    worker_id=task.worker_id,
                    shared_from_id=task.shared_from_id,
                    metadata=task.metadata_,
                    codex_main_mcp_enabled=settings.codex_main_mcp_enabled,
                )
                if (
                    provider == "claude"
                    or settings.codex_main_mcp_enabled
                ):
                    from backend.services.skill_context import (
                        build_task_skill_context,
                    )

                    task_skill_context = await build_task_skill_context(
                        db,
                        task_id=task_id,
                        provider=provider,
                        project_dir=cwd,
                        enabled_skills=enabled_skills,
                    )
                if task_ssh_broker_only:
                    # A managed-SSH Task has no direct network authority. Keep
                    # only non-secret commit identity from Dispatcher git_env;
                    # project/global SSH keys and HTTPS askpass helpers remain
                    # Manager-side and are never exposed to the model process.
                    git_env = {
                        key: value
                        for key, value in (git_env or {}).items()
                        if key.upper() in _TASK_SSH_GIT_IDENTITY_ENV_KEYS
                    }
                    git_env.update(_TASK_SSH_SAFE_GIT_ENV)
                    if task_ssh_capabilities:
                        ssh_policy = task_ssh_policy_context(
                            task_ssh_capabilities
                        )
                        task_skill_context = "\n\n".join(
                            value
                            for value in (
                                task_skill_context.strip(),
                                ssh_policy,
                            )
                            if value
                        )
                if pr_review_task or browser_review_task or delivery_task:
                    # PR input is already snapshotted into the fixed prompt.
                    # No ambient skills or monitor capability may reintroduce
                    # filesystem/network tools.
                    task_skill_context = ""
                    codex_monitor_enabled = False
                    if pr_review_task or browser_review_task:
                        # Dispatcher may have prepared project Git identity or
                        # credentials before it learned that this is a fixed,
                        # tool-free review.  Neither review route may inherit
                        # that process environment.
                        git_env = None
        if provider == "codex":
            # A Codex turn is not reusable when its process adapter reaches a
            # terminal returncode: the output consumer may still be migrating
            # the rollout and persisting its new account binding.  Keep this
            # guard in launch itself so API/manual callers cannot bypass the
            # lifecycle-specific waits.  A consumer-driven retry is allowed to
            # replace itself and therefore skips waiting on its own task.
            await self.wait_for_output_consumer(instance_id, provider=provider)
        if provider == "claude" and not config_dir:
            # Instance ids are reused across tasks. A default-account launch
            # must not inherit the explicit home recorded for an earlier task,
            # otherwise lifecycle callers can report or reuse a stale account.
            self._config_dirs.pop(instance_id, None)

        # CODEX_HOME is process-scoped.  Resolve it once and pass the exact
        # same canonical value through app-server, retries, and exec fallback;
        # otherwise one failed app-server request can silently resume a thread
        # with another account inherited from the service environment.
        if provider == "codex":
            from backend.services.codex_app_server import (
                CodexAppServerBusyError,
                CodexRequiredMcpError,
                CodexRequiredMcpPreTurnError,
                CodexServiceTierUnavailableError,
                CodexThreadHomeMismatchError,
                CodexThreadTerminalStateError,
                normalize_codex_service_tier,
                normalize_codex_home,
            )

            codex_service_tier = normalize_codex_service_tier(
                codex_service_tier
            )
            config_dir = normalize_codex_home(config_dir)
            codex_home_path = Path(config_dir)
            codex_home_path.mkdir(parents=True, exist_ok=True, mode=0o700)
            try:
                os.chmod(codex_home_path, 0o700)
            except OSError:
                logger.warning("Could not enforce 0700 on CODEX_HOME %s", config_dir)
            self._config_dirs[instance_id] = config_dir

        if (
            task_id is not None
            and config_dir
            and not unrestricted_admin_turn
        ):
            from backend.services.task_ssh_access import (
                _protected_path_variants,
            )

            task_ssh_protected_path_values = tuple(sorted({
                *task_ssh_protected_path_values,
                *_protected_path_variants(config_dir),
            }))

        # New turn → clear per-turn flags.
        self._transient_seen.discard(instance_id)
        self._pty_rate_limit_seen.discard(instance_id)
        self._pty_rate_limit_info.pop(instance_id, None)
        self._effective_exit_codes.pop(instance_id, None)

        mcp_config_path = None
        claude_isolation_settings_path = None
        claude_isolation_tools: tuple[str, ...] | None = None
        claude_isolation_allowed_rules: tuple[str, ...] | None = None
        claude_unrestricted_settings_path = None
        claude_unrestricted_tools: tuple[str, ...] | None = None
        claude_unrestricted_allowed_rules: tuple[str, ...] | None = None
        claude_delivery_git_boundary = None
        claude_task_runtime_scope_reserved = False
        if (
            provider == "claude"
            and task_id
            and not pr_review_task
        ):
            self._reserve_task_runtime_scope(task_id)
            claude_task_runtime_scope_reserved = True
        if (
            provider == "claude"
            and task_id
            and not pr_review_task
            and not delivery_task
        ):
            from backend.services.mcp_config import generate_mcp_config

            mcp_config_path = generate_mcp_config(
                task_id,
                enabled_skills or {},
                task_ssh_capabilities=tuple(sorted(task_ssh_capabilities)),
                task_incarnation_id=task_incarnation_id,
                task_retry_count=task_retry_count,
                task_turn_generation=task_turn_generation,
                task_status=task_status,
            )
        if (
            provider == "claude"
            and task_id
            and not pr_review_task
            and not delivery_task
            and unrestricted_admin_turn
        ):
            from backend.services.task_agent_isolation import (
                CLAUDE_UNRESTRICTED_BUILTIN_TOOLS,
                CLAUDE_UNRESTRICTED_PERMISSION_TOOLS,
                claude_permission_allow_rules,
                generate_claude_unrestricted_task_settings,
                validate_claude_unrestricted_task_settings,
            )

            if task_turn_generation is None:
                raise LaunchSupersededError(
                    "Claude unrestricted Task lost its exact turn generation"
                )
            claude_unrestricted_tools = CLAUDE_UNRESTRICTED_BUILTIN_TOOLS
            claude_unrestricted_allowed_rules = (
                claude_permission_allow_rules(
                    CLAUDE_UNRESTRICTED_PERMISSION_TOOLS
                )
            )
            claude_unrestricted_settings_path = (
                generate_claude_unrestricted_task_settings(
                    task_id,
                    turn_generation=task_turn_generation,
                    builtin_tools=CLAUDE_UNRESTRICTED_PERMISSION_TOOLS,
                )
            )
            await asyncio.to_thread(
                validate_claude_unrestricted_task_settings,
                claude_unrestricted_settings_path,
                builtin_tools=CLAUDE_UNRESTRICTED_PERMISSION_TOOLS,
            )
        if (
            provider == "claude"
            and task_id
            and not pr_review_task
            and not unrestricted_admin_turn
        ):
            from backend.services.task_agent_isolation import (
                CLAUDE_TASK_BUILTIN_TOOLS,
                generate_claude_task_isolation_settings,
                validate_claude_task_isolation_settings,
            )

            if delivery_task:
                from backend.services.task_agent_isolation import (
                    CLAUDE_DELIVERY_BUILTIN_TOOLS,
                    generate_claude_delivery_isolation_settings,
                    validate_claude_delivery_isolation_settings,
                )
                from backend.services.task_runtime_secrets import (
                    create_private_task_temp_dir,
                )

                if (
                    task_incarnation_id is None
                    or task_retry_count is None
                    or task_turn_generation is None
                ):
                    raise LaunchSupersededError(
                        "Claude Delivery isolation lost its exact generation"
                    )
                task_private_tmpdir = create_private_task_temp_dir(
                    task_id=task_id,
                    task_incarnation_id=task_incarnation_id,
                    retry_count=task_retry_count,
                    turn_generation=task_turn_generation,
                )
                self._reserve_private_runtime_tempdir(
                    instance_id,
                    task_private_tmpdir,
                )
                git_env = dict(git_env or _DELIVERY_SAFE_GIT_ENV)
                git_env.update({
                    key: str(task_private_tmpdir.path)
                    for key in _DELIVERY_RUNTIME_TEMP_ENV_KEYS
                })

                claude_isolation_tools = CLAUDE_DELIVERY_BUILTIN_TOOLS
                from backend.services.task_agent_isolation import (
                    claude_permission_allow_rules,
                )

                claude_isolation_allowed_rules = (
                    claude_permission_allow_rules(
                        CLAUDE_DELIVERY_BUILTIN_TOOLS,
                        include_mcp_tools=False,
                    )
                )
                (
                    claude_isolation_settings_path,
                    claude_delivery_git_boundary,
                ) = generate_claude_delivery_isolation_settings(
                    task_id,
                    task_ssh_protected_path_values,
                    working_directory=cwd or os.getcwd(),
                    private_tmpdir=task_private_tmpdir.path,
                )
                await asyncio.to_thread(
                    validate_claude_delivery_isolation_settings,
                    claude_isolation_settings_path,
                    claude_binary=settings.claude_binary,
                    working_directory=cwd or os.getcwd(),
                    private_tmpdir=task_private_tmpdir.path,
                    expected_git_boundary=claude_delivery_git_boundary,
                )
            else:
                claude_isolation_tools = CLAUDE_TASK_BUILTIN_TOOLS
                from backend.services.task_agent_isolation import (
                    claude_permission_allow_rules,
                )

                claude_isolation_allowed_rules = (
                    claude_permission_allow_rules(
                        CLAUDE_TASK_BUILTIN_TOOLS,
                    )
                )
                claude_isolation_settings_path = (
                    generate_claude_task_isolation_settings(
                        task_id,
                        task_ssh_protected_path_values,
                        allowed_read_paths=task_git_credential_read_path_values,
                        read_only_allow_paths=(
                            task_attachment_read_path_values
                        ),
                        ssh_capabilities=task_ssh_capabilities,
                        disable_direct_network=task_ssh_broker_only,
                    )
                )
                await asyncio.to_thread(
                    validate_claude_task_isolation_settings,
                    claude_isolation_settings_path,
                    claude_binary=settings.claude_binary,
                    tools=CLAUDE_TASK_BUILTIN_TOOLS,
                )

        # Prompt-only Claude launches have no Task-scoped exact settings file;
        # retain the legacy account-level AskUser compatibility hook for them.
        if provider == "claude" and not pr_review_task and task_id is None:
            from backend.services.ask_user_settings import ensure_ask_user_hook
            hooks_ready = ensure_ask_user_hook(
                config_dir or os.path.expanduser("~/.claude"),
                ssh_guard=bool(task_ssh_capabilities),
                ssh_protected_paths=task_ssh_protected_path_values,
            )
            if task_ssh_capabilities and not hooks_ready:
                raise RuntimeError(
                    "Task SSH guard could not be installed for Claude"
                )

        if (
            provider == "claude"
            and task_id is not None
            and not pr_review_task
            and not browser_review_task
            and not delivery_task
            and settings.ask_user_enabled
        ):
            from backend.services.internal_service_auth import (
                ASK_USER_TOKEN_ENV,
                issue_internal_service_token,
            )

            ask_user_token = issue_internal_service_token(
                audience="ccm_ask_user",
                task_id=task_id,
                task_incarnation_id=task_incarnation_id,
                task_retry_count=task_retry_count,
                task_turn_generation=task_turn_generation,
                task_status=task_status,
                owner_kind="task-turn",
                owner_id=task_id,
            )
            if ask_user_token:
                git_env = dict(git_env or {})
                git_env[ASK_USER_TOKEN_ENV] = ask_user_token

        if (
            task_id is not None
            and not pr_review_task
            and not browser_review_task
            and not delivery_task
        ):
            # An ordinary local administrator turn receives this node's exact
            # ssh-agent coordinates.  That includes a Worker-local mirror with
            # a Manager-delegated administrator principal, but never a socket
            # path transported by the Manager.  Project config and caller
            # ``git_env`` remain mutable inputs; every other Task explicitly
            # shadows ambient SSH state. A Browser child is MCP-only and
            # receives no shell environment at all, including otherwise-safe
            # empty SSH variables.
            git_env = dict(git_env or {})
            if host_ssh_agent_allowed:
                if ssh_agent_socket_snapshot is None:
                    ssh_agent_socket_snapshot = (
                        _SshAgentSocketSnapshot.capture(
                            os.environ.get("SSH_AUTH_SOCK")
                        )
                    )
                ssh_agent_socket_snapshot.assert_current()
                git_env["SSH_AUTH_SOCK"] = (
                    ssh_agent_socket_snapshot.path or ""
                )
            else:
                ssh_agent_socket_snapshot = None
                git_env["SSH_AUTH_SOCK"] = ""
            # PID is unused by ssh clients and ASKPASS could execute arbitrary
            # Manager-side code. They are never inherited by any Task role.
            git_env["SSH_AGENT_PID"] = ""
            git_env["SSH_ASKPASS"] = ""
            if task_ssh_broker_only:
                git_env["CCM_TASK_SSH_GUARD"] = "1"

        codex_main_mcp_required = bool(
            provider == "codex"
            and task_id is not None
            and settings.codex_main_mcp_enabled
            and not pr_review_task
            and not delivery_task
        )
        browser_review_job_id = (
            enabled_skills.get("browser-review")
            if isinstance(enabled_skills, dict)
            else None
        )
        codex_browser_mcp_required = bool(
            provider == "codex"
            and task_id is not None
            and browser_review_task
            and isinstance(browser_review_job_id, str)
            and browser_review_job_id.strip()
        )
        if browser_review_task and not codex_browser_mcp_required and provider == "codex":
            raise CodexRequiredMcpError(
                "Codex Browser Agent lost its required bound Browser MCP"
            )
        codex_frontend_review_mcp_required = bool(
            provider == "codex"
            and task_id is not None
            and isinstance(settings.auth_token, str)
            and bool(settings.auth_token.strip())
            and not pr_review_task
            and not codex_browser_mcp_required
            and not delivery_task
        )
        codex_sub_agent_mcp_required = bool(
            provider == "codex"
            and task_id is not None
            and enabled_skills
            and enabled_skills.get("sub-agent")
            and not pr_review_task
            and not delivery_task
        )
        codex_ssh_mcp_required = bool(
            provider == "codex"
            and task_id is not None
            and bool(task_ssh_capabilities)
            and not pr_review_task
            and not browser_review_task
            and not delivery_task
        )
        codex_task_isolation_required = bool(
            provider == "codex"
            and task_id is not None
            and not pr_review_task
            and not delivery_task
            and not unrestricted_admin_turn
            and task_ssh_protected_path_values
        )
        codex_mcp_required = (
            codex_main_mcp_required
            or codex_browser_mcp_required
            or codex_frontend_review_mcp_required
            or codex_sub_agent_mcp_required
            or codex_ssh_mcp_required
        )
        if (
            provider == "codex"
            and browser_review_task
            and not settings.codex_app_server_enabled
        ):
            raise CodexRequiredMcpError(
                "Codex isolated review requires the app-server read-only "
                "sandbox; exec fallback is disabled"
            )
        codex_mcp_specs: tuple["McpServerSpec", ...] = ()
        codex_exec_route = "direct-exec"
        if codex_main_mcp_required:
            from backend.services.mcp_config import build_mcp_server_specs

            codex_mcp_specs = build_mcp_server_specs(
                task_id,
                enabled_skills or {},
                provider=provider,
                codex_monitor_enabled=codex_monitor_enabled,
                task_incarnation_id=task_incarnation_id,
                task_retry_count=task_retry_count,
                task_turn_generation=task_turn_generation,
                task_status=task_status,
            )
        else:
            direct_specs: list["McpServerSpec"] = []
            if codex_browser_mcp_required:
                from backend.services.mcp_config import (
                    build_browser_review_mcp_server_specs,
                )

                direct_specs.extend(
                    build_browser_review_mcp_server_specs(
                        browser_review_job_id,
                        task_id=task_id,
                        task_incarnation_id=task_incarnation_id or "",
                        task_retry_count=task_retry_count,
                        task_turn_generation=task_turn_generation,
                        task_status=task_status,
                    )
                )
            elif codex_frontend_review_mcp_required:
                from backend.services.mcp_config import (
                    build_frontend_review_mcp_server_specs,
                    build_workspace_review_mcp_server_specs,
                )

                direct_specs.extend(
                    build_frontend_review_mcp_server_specs(
                        task_id,
                        task_incarnation_id=task_incarnation_id or "",
                        task_retry_count=task_retry_count,
                        task_turn_generation=task_turn_generation,
                        task_status=task_status,
                    )
                )
                direct_specs.extend(
                    build_workspace_review_mcp_server_specs(
                        task_id,
                        task_incarnation_id=task_incarnation_id or "",
                        task_retry_count=task_retry_count,
                        task_turn_generation=task_turn_generation,
                        task_status=task_status,
                    )
                )
            if codex_sub_agent_mcp_required:
                from backend.services.mcp_config import (
                    build_sub_agent_controller_mcp_server_specs,
                )

                direct_specs.extend(
                    build_sub_agent_controller_mcp_server_specs(
                        task_id,
                        task_incarnation_id=task_incarnation_id or "",
                        task_retry_count=task_retry_count,
                        task_turn_generation=task_turn_generation,
                        task_status=task_status,
                    )
                )
            codex_mcp_specs = tuple(direct_specs)
        if codex_ssh_mcp_required:
            from backend.services.mcp_config import (
                build_task_ssh_mcp_server_specs,
            )

            codex_mcp_specs += build_task_ssh_mcp_server_specs(
                task_id,
                capabilities=tuple(sorted(task_ssh_capabilities)),
                task_incarnation_id=task_incarnation_id or "",
                task_retry_count=task_retry_count,
                task_turn_generation=task_turn_generation,
                task_status=task_status,
            )

        if (
            provider == "codex"
            and codex_sub_agent_mcp_required
            and not settings.codex_app_server_enabled
        ):
            logger.error(
                "Codex transport fail-closed route=direct-exec "
                "reason=sub-agent-requires-app-server task_id=%s "
                "instance_id=%s home=%s",
                task_id,
                instance_id,
                config_dir,
            )
            raise CodexRequiredMcpError(
                "Codex Sub-Agent MCP requires the app-server transport; "
                "exec fallback does not provide live thread control"
            )

        if (
            provider == "codex"
            and codex_service_tier == "priority"
            and not settings.codex_app_server_enabled
        ):
            raise CodexServiceTierUnavailableError(
                "Codex Fast requires app-server admission confirmation; "
                "the app-server transport is disabled"
            )

        if (
            provider == "codex"
            and (pr_review_task or browser_review_task or delivery_task)
            and not settings.codex_app_server_enabled
        ):
            if delivery_task:
                raise CodexRequiredMcpError(
                    "Codex Delivery isolation requires the app-server sandbox; "
                    "exec fallback is disabled"
                )
            raise CodexRequiredMcpError(
                "Codex isolated review requires the app-server read-only "
                "sandbox; exec fallback is disabled"
            )

        if (
            provider == "codex"
            and codex_task_isolation_required
            and not settings.codex_app_server_enabled
        ):
            raise CodexRequiredMcpError(
                "Codex Task credential protection requires the app-server "
                "isolated permission "
                "profile; exec fallback is disabled"
            )

        codex_filesystem_boundary_required = bool(
            provider == "codex"
            and task_id is not None
            and (codex_task_isolation_required or delivery_task)
        )
        if codex_filesystem_boundary_required:
            from backend.services.task_agent_isolation import (
                discover_linked_worktree_git_read_boundary,
            )
            from backend.services.task_runtime_secrets import (
                create_private_task_temp_dir,
            )

            git_boundary = discover_linked_worktree_git_read_boundary(
                cwd or os.getcwd()
            )
            task_git_metadata_read_path_values = (
                git_boundary.read_paths if git_boundary is not None else ()
            )
            task_git_metadata_identity_fingerprint = (
                git_boundary.identity_fingerprint
                if git_boundary is not None
                else ()
            )
            if (
                task_incarnation_id is None
                or task_retry_count is None
                or task_turn_generation is None
            ):
                raise LaunchSupersededError(
                    "Codex Task filesystem isolation lost its exact generation"
                )
            task_private_tmpdir = create_private_task_temp_dir(
                task_id=task_id,
                task_incarnation_id=task_incarnation_id,
                retry_count=task_retry_count,
                turn_generation=task_turn_generation,
            )

        if provider == "codex" and settings.codex_app_server_enabled:
            async with self.codex_home_app_server_guard(config_dir):
                try:
                    pid = await self._launch_codex_app_server(
                        instance_id=instance_id,
                        prompt=prompt,
                        task_id=task_id,
                        cwd=cwd,
                        model=model,
                        resume_session_id=resume_session_id,
                        loop_iteration=loop_iteration,
                        git_env=git_env,
                        effort_level=effort_level,
                        chat_initiated=chat_initiated,
                        config_dir=config_dir,
                        enable_workflows=enable_workflows,
                        enabled_skills=enabled_skills,
                        task_retry_count=task_retry_count,
                        task_turn_generation=task_turn_generation,
                        mcp_specs=codex_mcp_specs,
                        skill_context=task_skill_context,
                        source_log_id=source_log_id,
                        current_message=current_message,
                        queue_timestamp=queue_timestamp,
                        initiating_user_id=initiating_user_id,
                        initiating_user_role=initiating_user_role,
                        execution_mode=execution_mode,
                        execution_principal_kind=execution_principal_kind,
                        attachment_paths=attachment_paths,
                        ssh_agent_socket_snapshot=ssh_agent_socket_snapshot,
                        disable_project_config=(
                            cloudrouter_account is not None
                            or pr_review_task
                            or browser_review_task
                            or delivery_task
                            or codex_task_isolation_required
                        ),
                        codex_service_tier=codex_service_tier,
                        sandbox_mode=(
                            "read-only"
                            if pr_review_task or browser_review_task
                            else "workspace-write"
                            if delivery_task or codex_task_isolation_required
                            else "danger-full-access"
                        ),
                        task_ssh_protected_paths=(
                            task_ssh_protected_path_values
                            if codex_task_isolation_required
                            else ()
                        ),
                        task_ssh_allowed_read_paths=(
                            tuple(dict.fromkeys((
                                *task_git_credential_read_path_values,
                                *task_attachment_read_path_values,
                            )))
                            if codex_task_isolation_required
                            else ()
                        ),
                        task_git_read_paths=(
                            task_git_metadata_read_path_values
                            if codex_filesystem_boundary_required
                            else ()
                        ),
                        task_git_boundary_fingerprint=(
                            task_git_metadata_identity_fingerprint
                            if codex_filesystem_boundary_required
                            else ()
                        ),
                        task_private_tmpdir=task_private_tmpdir,
                        # Member/system durable SSH grants remain
                        # broker-only/network-off.
                        # Ordinary local Tasks get public egress only through
                        # Codex's managed proxy, whose thread response is
                        # audited before model input.
                        task_ssh_disable_network=(
                            browser_review_task or task_ssh_broker_only
                        ),
                        task_managed_network_proxy=(
                            codex_task_isolation_required
                            and not browser_review_task
                            and not task_ssh_broker_only
                        ),
                        disable_user_mcp=(
                            pr_review_task
                            or browser_review_task
                            or delivery_task
                            or codex_task_isolation_required
                        ),
                        disable_autonomous_features=(
                            pr_review_task
                            or browser_review_task
                            or delivery_task
                            or codex_task_isolation_required
                        ),
                        network_isolated=delivery_task,
                        tools_disabled=pr_review_task,
                        mcp_only=browser_review_task,
                        on_launch_admitted=admit_codex_app_server_transport,
                    )
                    logger.info(
                        "Codex transport selected route=app-server task_id=%s "
                        "instance_id=%s home=%s required_mcp=%s",
                        task_id,
                        instance_id,
                        config_dir,
                        codex_mcp_required,
                    )
                    return pid
                except CodexRequiredMcpPreTurnError as exc:
                    if launch_boundary_attempted:
                        raise
                    logger.warning(
                        "Codex app-server pre-turn admission failed "
                        "task_id=%s instance_id=%s browser_review=%s "
                        "pr_review=%s delivery=%s reason=%s",
                        task_id,
                        instance_id,
                        browser_review_task,
                        pr_review_task,
                        delivery_task,
                        exc,
                    )
                    if delivery_task:
                        raise CodexRequiredMcpError(
                            "Codex Delivery workspace/network isolation could "
                            "not be confirmed before turn/start"
                        ) from exc
                    if pr_review_task or browser_review_task:
                        raise CodexRequiredMcpError(
                            "Codex isolated review sandbox could not be "
                            "confirmed before turn/start"
                        ) from exc
                    if codex_service_tier == "priority":
                        raise CodexServiceTierUnavailableError(
                            "Codex Fast could not be confirmed before "
                            "turn/start; exec fallback is disabled for Fast"
                        ) from exc
                    if codex_task_isolation_required:
                        raise CodexRequiredMcpError(
                            "Codex Task credential isolation could not be confirmed "
                            "before turn/start"
                        ) from exc
                    if (
                        (
                            codex_main_mcp_required
                            or codex_frontend_review_mcp_required
                        )
                        and not codex_sub_agent_mcp_required
                    ):
                        codex_exec_route = "safe-fallback"
                        logger.warning(
                            "Codex transport fallback route=safe-fallback "
                            "reason=required-mcp-pre-turn task_id=%s "
                            "instance_id=%s home=%s; retrying once through "
                            "MCP-equivalent exec",
                            task_id,
                            instance_id,
                            config_dir,
                        )
                    else:
                        logger.exception(
                            "Codex transport fail-closed route=app-server "
                            "reason=non-fallback-required-mcp task_id=%s "
                            "instance_id=%s home=%s",
                            task_id,
                            instance_id,
                            config_dir,
                        )
                        raise
                except (
                    asyncio.TimeoutError,
                    CodexAppServerBusyError,
                    CodexRequiredMcpError,
                    CodexServiceTierUnavailableError,
                    CodexThreadHomeMismatchError,
                    CodexThreadTerminalStateError,
                    CodexLaunchCommitError,
                    InstanceNotFoundError,
                    LaunchSupersededError,
                ):
                    # These failures are not safe to replay through `codex exec`:
                    # a timed-out turn/start may already be running, while busy or
                    # owner-mismatch means the requested account route is invalid.
                    # Falling back would duplicate work or mix auth/thread state.
                    logger.exception(
                        "Codex transport fail-closed route=app-server "
                        "reason=unsafe-replay task_id=%s instance_id=%s home=%s",
                        task_id,
                        instance_id,
                        config_dir,
                    )
                    raise
                except Exception as exc:
                    if launch_boundary_attempted:
                        # The durable owner has already entered ``launching``.
                        # Replaying through exec could duplicate a turn even
                        # when app-server failed before returning its adapter.
                        raise
                    if delivery_task:
                        logger.exception(
                            "Codex Delivery app-server failed; refusing exec "
                            "fallback task_id=%s",
                            task_id,
                        )
                        raise CodexRequiredMcpError(
                            "Codex Delivery isolation could not be guaranteed"
                        ) from exc
                    if pr_review_task or browser_review_task:
                        logger.exception(
                            "Codex isolated review app-server failed; refusing "
                            "unsandboxed exec fallback task_id=%s",
                            task_id,
                        )
                        raise CodexRequiredMcpError(
                            "Codex isolated review boundary could not be guaranteed"
                        ) from exc
                    if codex_service_tier == "priority":
                        # exec --json does not expose an accepted/effective
                        # service tier before it executes the prompt.  A
                        # catalog plus argv can prove only that Fast was
                        # requested, so fail closed instead of showing a
                        # misleading Fast badge or silently using Standard.
                        raise CodexServiceTierUnavailableError(
                            "Codex Fast could not be confirmed before "
                            "turn/start; refusing unverified exec fallback"
                        ) from exc
                    if codex_mcp_required or codex_task_isolation_required:
                        # Once required ccm_skills was selected, every unknown
                        # app-server failure must fail closed instead of
                        # silently replaying without tools.
                        logger.exception(
                            "Codex transport fail-closed route=app-server "
                            "reason=required-mcp-not-guaranteed task_id=%s "
                            "instance_id=%s home=%s",
                            task_id,
                            instance_id,
                            config_dir,
                        )
                        required_server = (
                            "ccm_skills"
                            if (
                                codex_main_mcp_required
                                or codex_sub_agent_mcp_required
                            )
                            else "ccm_ssh"
                        )
                        raise CodexRequiredMcpError(
                            "Codex app-server failed before required "
                            f"{required_server} could be guaranteed"
                        ) from exc
                    # App-server is an experimental Codex surface.  A CLI upgrade
                    # must not take all Codex tasks down; retain the proven exec
                    # path as an automatic compatibility fallback.
                    codex_exec_route = "safe-fallback"
                    logger.exception(
                        "Codex transport fallback route=safe-fallback "
                        "reason=app-server-compatibility task_id=%s "
                        "instance_id=%s home=%s; falling back to codex exec",
                        task_id,
                        instance_id,
                        config_dir,
                    )
                finally:
                    if task_private_tmpdir is not None:
                        await asyncio.to_thread(
                            task_private_tmpdir.cleanup_if_unbound
                        )

        if (
            provider == "claude"
            and delivery_task
        ):
            from backend.services.task_agent_isolation import (
                discover_linked_worktree_git_read_boundary,
            )

            refreshed_boundary = discover_linked_worktree_git_read_boundary(
                cwd or os.getcwd()
            )
            if (
                claude_delivery_git_boundary is None
                or refreshed_boundary is None
                or refreshed_boundary != claude_delivery_git_boundary
            ):
                raise LaunchSupersededError(
                    "Claude Delivery Git isolation boundary changed before launch"
                )

        if (
            provider == "claude"
            and self.pty_mode_enabled
            and not pr_review_task
            and not browser_review_task
        ):
            return await self._launch_pty(
                instance_id=instance_id,
                prompt=prompt,
                task_id=task_id,
                cwd=cwd,
                model=model,
                resume_session_id=resume_session_id,
                loop_iteration=loop_iteration,
                git_env=git_env,
                thinking_budget=thinking_budget,
                effort_level=effort_level,
                chat_initiated=chat_initiated,
                config_dir=config_dir,
                enable_workflows=enable_workflows,
                enabled_skills=enabled_skills,
                mcp_config_path=str(mcp_config_path) if mcp_config_path else None,
                # claude-pty's default config resolves ambient ``claude``.
                # Carry CCM's configured binary through the wrapper so PTY
                # and direct launches use the same pinned CLI.
                claude_binary_override=settings.claude_binary,
                container_exec_spec=None,
                task_retry_count=task_retry_count,
                task_turn_generation=task_turn_generation,
                skill_context=task_skill_context,
                cloudrouter_api=cloudrouter_account is not None,
                source_log_id=source_log_id,
                current_message=current_message,
                queue_timestamp=queue_timestamp,
                initiating_user_id=initiating_user_id,
                initiating_user_role=initiating_user_role,
                execution_mode=execution_mode,
                execution_principal_kind=execution_principal_kind,
                attachment_paths=attachment_paths,
                ssh_agent_socket_snapshot=ssh_agent_socket_snapshot,
                claude_isolation_settings_path=(
                    claude_isolation_settings_path
                ),
                claude_isolation_tools=claude_isolation_tools,
                claude_isolation_allowed_rules=(
                    claude_isolation_allowed_rules
                ),
                claude_unrestricted_settings_path=(
                    claude_unrestricted_settings_path
                ),
                claude_unrestricted_tools=claude_unrestricted_tools,
                claude_unrestricted_allowed_rules=(
                    claude_unrestricted_allowed_rules
                ),
                claude_task_runtime_scope_reserved=(
                    claude_task_runtime_scope_reserved
                ),
                private_runtime_tempdir=task_private_tmpdir,
                on_launch_admitted=admit_claude_pty_transport,
            )

        if provider == "codex" and (
            pr_review_task or browser_review_task or delivery_task
        ):
            raise CodexRequiredMcpError(
                "Codex isolated workflow execution forbids exec fallback"
            )

        # PTY launches return above with the complete prompt. Only the direct
        # exec transport reaches this argv-size fallback and owns stdin.
        claude_prompt_via_stdin = bool(
            provider == "claude"
            and len(prompt.encode("utf-8"))
            >= _CLAUDE_PROMPT_STDIN_THRESHOLD_BYTES
        )
        cmd = self._build_command(
            provider=provider,
            prompt=prompt,
            claude_prompt_via_stdin=claude_prompt_via_stdin,
            model=model,
            resume_session_id=resume_session_id,
            effort_level=effort_level,
            enable_workflows=enable_workflows,
            mcp_config_path=str(mcp_config_path) if mcp_config_path else None,
            enabled_skills=enabled_skills,
            system_prompt_mode=system_prompt_mode,
            cwd=cwd,
            task_id=task_id,
            skill_context=task_skill_context,
            codex_mcp_specs=(
                codex_mcp_specs if codex_mcp_required else ()
            ),
            codex_api_account=cloudrouter_account is not None,
            codex_service_tier=codex_service_tier,
            tools_disabled=(
                pr_review_task
                or (provider == "claude" and browser_review_task)
            ),
            claude_isolation_settings_path=claude_isolation_settings_path,
            claude_isolation_tools=claude_isolation_tools,
            claude_isolation_allowed_rules=claude_isolation_allowed_rules,
            claude_unrestricted_settings_path=(
                claude_unrestricted_settings_path
            ),
            claude_unrestricted_tools=claude_unrestricted_tools,
            claude_unrestricted_allowed_rules=(
                claude_unrestricted_allowed_rules
            ),
        )
        if provider == "codex":
            logger.info(
                "Codex transport selected route=%s task_id=%s instance_id=%s "
                "home=%s required_mcp=%s",
                codex_exec_route,
                task_id,
                instance_id,
                config_dir,
                codex_mcp_required,
            )

        # Task-launched model processes must not inherit the deployment bearer
        # credential. Their CCM MCP children receive separate, route-scoped
        # credentials through each server spec instead.
        task_secret_env = {
            "AUTH_TOKEN",
            "CCM_INTERNAL_SERVICE_TOKEN",
        }
        from backend.services.task_agent_isolation import (
            scrub_task_model_environment,
        )

        # Every Task starts without Manager ambient Git/GitHub/SSH authority.
        # The exact project-scoped git_env is applied below; managed-SSH Tasks
        # have already reduced it to non-secret identity plus fail-closed flags.
        env = scrub_task_model_environment(
            os.environ,
            provider=provider,
        )

        # Inject per-project git identity and credentials as environment variables.
        # These take precedence over any global ~/.gitconfig or system credential helper.
        if git_env:
            env.update(git_env)
        for key in task_secret_env:
            env.pop(key, None)

        if cloudrouter_account is not None:
            auth_keys = (
                _CLOUDROUTER_CLAUDE_AUTH_ENV_KEYS
                if provider == "claude"
                else _CLOUDROUTER_CODEX_AUTH_ENV_KEYS
            )
            for key in auth_keys:
                env.pop(key, None)

            if provider == "claude":
                from backend.services.claude_auth_projection import (
                    inject_cloudrouter_claude_direct_auth,
                )

                if not inject_cloudrouter_claude_direct_auth(
                    env,
                    self.cloudrouter_store,
                    config_dir,
                ):
                    raise RuntimeError(
                        "Selected Claude API account could not be projected "
                        "into the model process"
                    )

        # Provider account home.  The Codex assignment is especially important
        # for app-server fallback: the rollout and auth.json must stay together.
        if config_dir and provider == "claude":
            env["CLAUDE_CONFIG_DIR"] = config_dir
            self._config_dirs[instance_id] = config_dir
        elif config_dir and provider == "codex":
            env["CODEX_HOME"] = config_dir
            self._config_dirs[instance_id] = config_dir

        # Disable CC's auto-compact — CCM manages context/compaction itself
        env["DISABLE_AUTO_COMPACT"] = "true"

        # Forward Extended Thinking budget (Claude-specific env var)
        if thinking_budget and thinking_budget > 0 and provider == "claude":
            env["MAX_THINKING_TOKENS"] = str(thinking_budget)

        if provider == "codex":
            # Hold the per-home gate through process creation and tracking.
            # Maintenance can then either see this active exec or reserve
            # the home first; it can never edit auth.json in the gap.
            home_lock = self._codex_home_lock(config_dir)
            async with home_lock:
                # The dispatcher snapshot is only a routing hint.  This
                # lock-local predicate is the authoritative barrier for
                # two fresh tasks that selected the same home, or for an
                # ephemeral exec that won admission after selection.
                self._assert_codex_app_server_home_available(
                    config_dir,
                    replacing_exec_instance_id=instance_id,
                )
                # app-server keeps threads and MCP clients resident in
                # memory.  Before an exec generation enters the same home,
                # stop an idle transport or reject an active one.  Holding
                # the home lock through spawn + ownership registration
                # closes the reverse race with a concurrent app-server
                # launch.
                registry = self._codex_app_server
                if registry is not None:
                    await registry.shutdown_home(
                        config_dir,
                        require_idle=True,
                    )
                spawn_kwargs = {
                    "stdout": asyncio.subprocess.PIPE,
                    "stderr": asyncio.subprocess.PIPE,
                    "cwd": cwd or os.getcwd(),
                    "env": env,
                    "limit": 10 * 1024 * 1024,
                }
                if os.name == "posix":
                    spawn_kwargs["start_new_session"] = True
                await admit_external_launch("codex_exec")
                process = await self._spawn_managed_direct_process(
                    instance_id,
                    task_id,
                    cmd,
                    spawn_kwargs,
                    codex_home=config_dir,
                )
        else:
            spawn_kwargs = {
                "stdout": asyncio.subprocess.PIPE,
                "stderr": asyncio.subprocess.PIPE,
                "cwd": cwd or os.getcwd(),
                "env": env,
                "limit": 10 * 1024 * 1024,
            }
            if claude_prompt_via_stdin:
                spawn_kwargs["stdin"] = asyncio.subprocess.PIPE
            if os.name == "posix":
                spawn_kwargs["start_new_session"] = True
            await admit_external_launch("claude_exec")
            if task_private_tmpdir is not None:
                self._bind_private_runtime_tempdir(
                    instance_id,
                    task_private_tmpdir,
                )
            previous_direct_process = self.processes.get(instance_id)
            try:
                process = await self._spawn_managed_direct_process(
                    instance_id,
                    task_id,
                    cmd,
                    spawn_kwargs,
                    task_runtime_scope_task_id=(
                        task_id
                        if claude_task_runtime_scope_reserved
                        else None
                    ),
                )
                if claude_prompt_via_stdin:
                    await self._write_direct_claude_prompt(
                        instance_id,
                        process,
                        prompt,
                    )
            except BaseException:
                if task_private_tmpdir is not None:
                    exact_process = self.processes.get(instance_id)
                    if (
                        exact_process is not None
                        and exact_process is not previous_direct_process
                        and not self._generation_reap_confirmed(
                            instance_id,
                            exact_process,
                        )
                    ):
                        # _spawn_managed_direct_process publishes the exact
                        # child before delivering delayed cancellation.  A
                        # failed reap must retain both that generation and its
                        # bound scratch leaf; deleting TMPDIR while the child
                        # may still be running would race model filesystem
                        # effects and destroy recovery evidence.
                        self._adopt_private_runtime_tempdir(
                            instance_id,
                            exact_process,
                            task_private_tmpdir,
                        )
                    else:
                        await self._abort_private_runtime_tempdir(
                            instance_id,
                            task_private_tmpdir,
                            process=(
                                exact_process
                                if exact_process is not previous_direct_process
                                else None
                            ),
                        )
                raise
            if task_private_tmpdir is not None:
                self._adopt_private_runtime_tempdir(
                    instance_id,
                    process,
                    task_private_tmpdir,
                )

        if provider != "codex":
            self.processes[instance_id] = process

        # Store launch params for potential pool rotation re-launch
        if chat_initiated:
            self._launch_params[instance_id] = {
                "prompt": prompt,
                "task_id": task_id,
                "task_turn_generation": task_turn_generation,
                "cwd": cwd,
                "model": model,
                "git_env": git_env,
                "thinking_budget": thinking_budget,
                "effort_level": effort_level,
                "enable_workflows": enable_workflows,
                "enabled_skills": enabled_skills,
                "provider": provider,
                "config_dir": config_dir,
                "source_log_id": source_log_id,
                "current_message": current_message or prompt,
                "queue_timestamp": queue_timestamp,
                "codex_service_tier": codex_service_tier,
                "initiating_user_id": initiating_user_id,
                "initiating_user_role": initiating_user_role,
                "execution_mode": execution_mode,
                "execution_principal_kind": execution_principal_kind,
                "attachment_paths": tuple(attachment_paths),
                "ssh_agent_socket_snapshot": ssh_agent_socket_snapshot,
            }

        return await self._persist_and_track_launch(
            instance_id=instance_id,
            task_id=task_id,
            process=process,
            actual_cwd=cwd or os.getcwd(),
            loop_iteration=loop_iteration,
            chat_initiated=chat_initiated,
            provider=provider,
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
        )

    def _ensure_codex_app_server_registry(self):
        """Return the lazy per-CODEX_HOME app-server registry."""

        from backend.services.codex_app_server import CodexAppServerRegistry

        if self._codex_app_server is None:
            self._codex_app_server = CodexAppServerRegistry(
                self._resolve_codex_binary(),
                request_timeout=settings.codex_app_server_request_timeout,
                env_remove_resolver=self._codex_env_remove_for_home,
                actual_tier_route_resolver=(
                    self._codex_actual_tier_route_for_home
                ),
                require_actual_tier_proof=True,
            )
        return self._codex_app_server

    def active_codex_task_ids(self) -> frozenset[int]:
        """Return Tasks durably represented by live app-server turns."""

        registry = self._codex_app_server
        if registry is None:
            return frozenset()
        return registry.live_task_ids()

    def active_codex_transport_pids(self) -> frozenset[int]:
        """Return shared Codex transport PIDs owned by this manager."""

        registry = self._codex_app_server
        if registry is None:
            return frozenset()
        return registry.live_transport_pids()

    def _codex_actual_tier_route_for_home(self, codex_home: str):
        """Resolve a non-secret upstream route for the per-home proof proxy."""

        from backend.services.cloudrouter_accounts import (
            API_PROVIDER_APEX,
            API_PROVIDER_SPECS,
            LEGACY_APEX_CODEX_PROVIDER,
        )
        from backend.services.codex_tier_proxy import (
            CodexTierProxyError,
            CodexTierProxyRoute,
            resolve_native_codex_tier_route,
        )

        account = self._cloudrouter_account_for_runtime_home(
            "codex",
            codex_home,
        )
        try:
            if account is not None:
                spec = API_PROVIDER_SPECS[account.api_provider]
                return CodexTierProxyRoute(
                    upstream_base_url=spec.codex_base_url,
                    provider_id=spec.codex_provider,
                    provider_aliases=(
                        (LEGACY_APEX_CODEX_PROVIDER,)
                        if account.api_provider == API_PROVIDER_APEX
                        else ()
                    ),
                    built_in_openai=False,
                    label=spec.label,
                )
            return resolve_native_codex_tier_route(codex_home)
        except (CodexTierProxyError, OSError):
            # Quota/configuration RPCs may still use an unproxied app-server,
            # but a Fast start_turn requires a route and fails before prompt
            # work. Standard remains compatible with an explicitly configured
            # custom provider and still clears sticky Fast state through RPC.
            # Never log auth content or the configured upstream URL.
            logger.warning(
                "Codex tier request route is unavailable home=%s reason=%s",
                codex_home,
                "unsupported-or-unverifiable-account",
            )
            return None

    def _cloudrouter_account_for_runtime_home(
        self,
        provider: str,
        config_dir: str | None,
    ):
        """Resolve an API account projection by its exact runtime home."""

        if self.cloudrouter_store is None or not config_dir:
            return None
        try:
            if provider == "codex":
                return self.cloudrouter_store.account_for_codex_home(config_dir)
            return self.cloudrouter_store.account_for_claude_config_dir(config_dir)
        except Exception:
            logger.exception(
                "Could not resolve CloudRouter runtime home for %s", config_dir
            )
            return None

    def _provider_process_label(
        self,
        instance_id: int,
        provider: str | None,
    ) -> str:
        """Describe the exact CLI/API process used by one instance turn."""

        normalized = (provider or "claude").lower()
        label = "Codex" if normalized == "codex" else "Claude"
        config_dir = self._config_dirs.get(instance_id)
        if (
            config_dir
            and self._cloudrouter_account_for_runtime_home(
                normalized, config_dir
            )
            is not None
        ):
            label += " API"
        return label

    @asynccontextmanager
    async def _cloudrouter_runtime_admission(
        self,
        provider: str,
        config_dir: str | None,
        model: str | None,
        *,
        service_tier: str = "default",
    ):
        """Revalidate an API route atomically with process admission."""

        account = self._cloudrouter_account_for_runtime_home(
            provider, config_dir,
        )
        if account is None:
            yield None
            return
        guard = getattr(self.cloudrouter_store, "runtime_admission", None)
        if not callable(guard):
            raise RuntimeError(
                "CloudRouter account store cannot fence runtime admission"
            )
        async with guard(
            provider,
            config_dir,
            model,
            **(
                {"service_tier": service_tier}
                if provider == "codex"
                and str(service_tier or "default").lower() != "default"
                else {}
            ),
        ) as current:
            yield current

    @asynccontextmanager
    async def _cloudrouter_configuration_admission(
        self,
        provider: str,
        config_dir: str | None,
    ):
        """Validate an API route before a non-model app-server operation."""

        account = self._cloudrouter_account_for_runtime_home(
            provider, config_dir,
        )
        if account is None:
            yield None
            return
        guard = getattr(
            self.cloudrouter_store,
            "configuration_admission",
            None,
        )
        if not callable(guard):
            raise RuntimeError(
                "CloudRouter account store cannot validate runtime storage"
            )
        async with guard(provider, config_dir) as current:
            yield current

    def _codex_env_remove_for_home(self, codex_home: str) -> set[str]:
        removed = {
            "AUTH_TOKEN",
            "CCM_INTERNAL_SERVICE_TOKEN",
        }
        if self._cloudrouter_account_for_runtime_home("codex", codex_home):
            removed.update(_CLOUDROUTER_CODEX_AUTH_ENV_KEYS)
        return removed

    def is_cloudrouter_transient(
        self,
        instance_id: int,
        provider: str,
        text: str,
    ) -> bool:
        """Classify retryable gateway capacity only for a proven API account."""

        config_dir = self._config_dirs.get(instance_id)
        account = self._cloudrouter_account_for_runtime_home(
            provider, config_dir,
        )
        if account is None:
            return False
        value = text or ""
        if _CLOUDROUTER_TRANSIENT_RE.search(value):
            return True
        return bool(
            provider == "codex"
            and getattr(account, "api_provider", None) == "apex"
            and _APEX_BUSY_TRANSIENT_RE.search(value)
        )

    def is_cloudrouter_auth_failure(
        self,
        instance_id: int,
        provider: str,
        text: str,
    ) -> bool:
        """Classify gateway key rejection only for a proven API account."""

        config_dir = self._config_dirs.get(instance_id)
        if self._cloudrouter_account_for_runtime_home(provider, config_dir) is None:
            return False
        return bool(_CLOUDROUTER_AUTH_RE.search(text or ""))

    async def read_codex_rate_limits(self, codex_home: str) -> dict:
        """Read live quota from the app-server bound to ``codex_home``."""

        async with self._cloudrouter_configuration_admission(
            "codex", codex_home,
        ):
            async with self.codex_home_app_server_guard(codex_home) as home:
                registry = self._ensure_codex_app_server_registry()
                return await registry.read_rate_limits(home)

    @asynccontextmanager
    async def codex_home_app_server_guard(self, codex_home: str | None):
        """Serialize any app-server admission against exec and maintenance."""

        from backend.services.codex_app_server import normalize_codex_home

        home = normalize_codex_home(codex_home)
        home_lock = self._codex_home_lock(home)
        async with home_lock:
            self._assert_codex_app_server_home_available(home)
            yield home

    def _assert_codex_app_server_home_available(
        self,
        codex_home: str,
        *,
        replacing_exec_instance_id: int | None = None,
    ) -> None:
        """Reject exclusive transport admission while a runtime owns a home.

        The caller must hold the canonical home's admission lock so the check
        and the subsequent app-server/direct-exec operation form one atomic
        admission.
        """

        from backend.services.codex_app_server import CodexAppServerBusyError

        if codex_home in self._codex_home_maintenance:
            raise CodexAppServerBusyError(
                f"Codex account is under maintenance: {codex_home}"
            )
        for exec_instance_id, exec_home in self._codex_exec_homes.items():
            if exec_home == codex_home:
                previous = self.processes.get(exec_instance_id)
                if (
                    exec_instance_id == replacing_exec_instance_id
                    and previous is not None
                    and self._generation_reap_confirmed(
                        exec_instance_id,
                        previous,
                    )
                ):
                    continue
                raise CodexAppServerBusyError(
                    "Codex account still has an exec generation "
                    f"owned by instance {exec_instance_id}: {codex_home}"
                )
        if self._codex_ephemeral_home_users.get(codex_home, 0):
            raise CodexAppServerBusyError(
                f"Codex account has an active ephemeral exec: {codex_home}"
            )
        if codex_home in self._codex_retained_ephemeral_homes():
            raise CodexAppServerBusyError(
                f"Codex account has a retained ephemeral exec: {codex_home}"
            )

    @staticmethod
    def _codex_retained_ephemeral_homes() -> set[str]:
        """Snapshot exact external runtimes that outlived their guard.

        GoalEvaluator and TaskDistill retain structured process/home evidence
        until terminal cleanup is proven.  Read those registries directly;
        their human-facing account-retirement blocker strings are not an
        admission protocol.
        """

        from backend.services.goal_evaluator import (
            codex_goal_evaluator_runtime_homes,
        )
        from backend.services.skill_distill import (
            codex_task_distill_runtime_homes,
        )

        return (
            codex_goal_evaluator_runtime_homes()
            | codex_task_distill_runtime_homes()
        )

    def busy_codex_homes(self) -> set[str]:
        """Snapshot homes held by maintenance or non-app-server execs.

        App-server turns are intentionally not included: one account transport
        can serve independent threads concurrently. This is only a scheduling
        hint; the per-home launch guard closes the select-to-launch race.
        """

        from backend.services.codex_app_server import normalize_codex_home

        homes = set(self._codex_home_maintenance)
        homes.update(self._codex_exec_homes.values())
        homes.update(
            home
            for home, users in self._codex_ephemeral_home_users.items()
            if users
        )
        homes.update(self._codex_retained_ephemeral_homes())
        return {normalize_codex_home(home) for home in homes}

    async def read_codex_thread(
        self, codex_home: str, thread_id: str,
    ) -> dict:
        """Read one idle Codex thread with turns from its bound account."""

        async with self._cloudrouter_configuration_admission(
            "codex", codex_home,
        ):
            async with self.codex_home_app_server_guard(codex_home) as home:
                registry = self._ensure_codex_app_server_registry()
                return await registry.read_thread(home, thread_id)

    @asynccontextmanager
    async def codex_thread_routing_guard(
        self,
        codex_home: str,
        thread_id: str,
    ):
        """Hold native-thread quiescence across a Task routing DB commit."""

        async with self._cloudrouter_configuration_admission(
            "codex", codex_home,
        ):
            async with self.codex_home_app_server_guard(codex_home) as home:
                registry = self._ensure_codex_app_server_registry()
                async with registry.thread_routing_guard(
                    home,
                    thread_id,
                ) as snapshot:
                    yield snapshot

    async def create_codex_thread(
        self,
        codex_home: str,
        *,
        cwd: str,
        model: str | None = None,
    ) -> dict:
        """Create and register an empty native Codex thread."""

        from backend.services.codex_app_server import normalize_codex_home

        home = normalize_codex_home(codex_home)
        # Match ordinary launch lock ordering: API-store admission precedes
        # the per-home app-server gate, never the reverse.
        async with self._cloudrouter_runtime_admission(
            "codex", home, model,
        ) as api_account:
            async with self.codex_home_app_server_guard(home) as admitted_home:
                registry = self._ensure_codex_app_server_registry()
                return await registry.create_thread(
                    admitted_home,
                    cwd=cwd,
                    model=model,
                    disable_project_config=api_account is not None,
                )

    async def fork_codex_thread(
        self,
        codex_home: str,
        thread_id: str,
        *,
        last_turn_id: str,
    ) -> dict:
        """Create and register a native Codex thread fork."""

        async with self._cloudrouter_configuration_admission(
            "codex", codex_home,
        ):
            async with self.codex_home_app_server_guard(codex_home) as home:
                registry = self._ensure_codex_app_server_registry()
                return await registry.fork_thread(
                    home,
                    thread_id,
                    last_turn_id=last_turn_id,
                )

    async def delete_codex_thread(
        self, codex_home: str, thread_id: str,
    ) -> None:
        """Compensate a failed fork by deleting its unclaimed thread."""

        async with self._cloudrouter_configuration_admission(
            "codex", codex_home,
        ):
            async with self.codex_home_app_server_guard(codex_home) as home:
                registry = self._ensure_codex_app_server_registry()
                await registry.delete_thread(home, thread_id)

    def _codex_home_lock(self, codex_home: str) -> asyncio.Lock:
        """Return the admission/maintenance lock for a canonical home."""

        lock = self._codex_home_locks.get(codex_home)
        if lock is None:
            lock = asyncio.Lock()
            self._codex_home_locks[codex_home] = lock
        return lock

    @asynccontextmanager
    async def codex_home_exec_guard(self, codex_home: str | None):
        """Reserve one CODEX_HOME for an external ephemeral Codex process.

        Admission and maintenance use the same per-home lock. Maintenance
        therefore either reserves the home first and rejects this process, or
        observes the active user and fails busy before touching credentials.
        An idle app-server transport is stopped before admission; an active
        transport rejects the exec instead. The synchronous finalizer cannot
        be interrupted by task cancellation.
        """

        from backend.services.codex_app_server import (
            CodexAppServerBusyError,
            normalize_codex_home,
        )

        home = normalize_codex_home(codex_home)
        home_lock = self._codex_home_lock(home)
        async with home_lock:
            self._assert_codex_app_server_home_available(home)
            registry = self._codex_app_server
            if registry is not None:
                await registry.shutdown_home(home, require_idle=True)
            self._codex_ephemeral_home_users[home] = (
                self._codex_ephemeral_home_users.get(home, 0) + 1
            )
        try:
            yield home
        finally:
            remaining = self._codex_ephemeral_home_users.get(home, 0) - 1
            if remaining > 0:
                self._codex_ephemeral_home_users[home] = remaining
            else:
                self._codex_ephemeral_home_users.pop(home, None)

    async def _launch_codex_app_server(
        self,
        *,
        instance_id: int,
        prompt: str,
        task_id: int | None,
        cwd: str | None,
        model: str | None,
        resume_session_id: str | None,
        loop_iteration: int | None,
        git_env: dict | None,
        effort_level: str | None,
        chat_initiated: bool,
        config_dir: str | None,
        enable_workflows: bool,
        enabled_skills: dict | None,
        task_retry_count: int | None = None,
        task_turn_generation: int | None = None,
        mcp_specs: Sequence["McpServerSpec"] = (),
        skill_context: str = "",
        source_log_id: int | None = None,
        current_message: str | None = None,
        queue_timestamp: float | None = None,
        initiating_user_id: int | None = None,
        initiating_user_role: str = "member",
        execution_mode: str = "sandbox",
        execution_principal_kind: str = "system",
        attachment_paths: Sequence[str] = (),
        ssh_agent_socket_snapshot: _SshAgentSocketSnapshot | None = None,
        disable_project_config: bool = False,
        disable_user_mcp: bool = False,
        codex_service_tier: str = "default",
        sandbox_mode: str = "danger-full-access",
        task_ssh_protected_paths: Sequence[str] = (),
        task_ssh_allowed_read_paths: Sequence[str] = (),
        task_git_read_paths: Sequence[str] = (),
        task_git_boundary_fingerprint: Sequence[tuple[object, ...]] = (),
        task_private_tmpdir: "PrivateTaskTempDir | None" = None,
        task_ssh_disable_network: bool = False,
        task_managed_network_proxy: bool = False,
        disable_autonomous_features: bool = False,
        network_isolated: bool = False,
        tools_disabled: bool = False,
        mcp_only: bool = False,
        on_launch_admitted: Callable[[], Awaitable[None]] | None = None,
    ) -> int:
        """Launch one turn on the persistent app-server for its CODEX_HOME."""
        registry = self._ensure_codex_app_server_registry()

        actual_cwd = cwd or os.getcwd()
        codex_effort = clamp_codex_effort(model, effort_level)

        async def publish_launch_admission(_process, _thread_id) -> None:
            if on_launch_admitted is not None:
                await on_launch_admitted()

        process, _thread_id = await registry.start_turn(
            codex_home=config_dir,
            prompt=prompt,
            cwd=actual_cwd,
            model=model,
            effort=codex_effort,
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
            tools_disabled=tools_disabled,
            mcp_only=mcp_only,
            on_turn_prepared=(
                publish_launch_admission
                if on_launch_admitted is not None
                else None
            ),
        )
        # Keep thread-scoped cleanup ownership on the exact native turn. Fresh
        # dispatcher launches do not populate ``_launch_params`` (that cache is
        # reserved for chat retry), so consulting it at terminal time would
        # leak the initial thread's MCP helper/subscription.
        process.unsubscribe_on_terminal = bool(mcp_specs)
        if config_dir:
            self._config_dirs[instance_id] = config_dir
        self.processes[instance_id] = process
        # This instance may previously have used the exec fallback.  It is now
        # owned by the registry, whose active-turn check is authoritative.
        self._codex_exec_homes.pop(instance_id, None)

        if chat_initiated:
            self._launch_params[instance_id] = {
                "prompt": prompt,
                "task_id": task_id,
                "task_turn_generation": task_turn_generation,
                "cwd": cwd,
                "model": model,
                "git_env": git_env,
                "thinking_budget": None,
                "effort_level": effort_level,
                "enable_workflows": enable_workflows,
                "enabled_skills": enabled_skills,
                "provider": "codex",
                "config_dir": config_dir,
                "source_log_id": source_log_id,
                "current_message": current_message or prompt,
                "queue_timestamp": queue_timestamp,
                "codex_service_tier": codex_service_tier,
                "initiating_user_id": initiating_user_id,
                "initiating_user_role": initiating_user_role,
                "execution_mode": execution_mode,
                "execution_principal_kind": execution_principal_kind,
                "attachment_paths": tuple(attachment_paths),
                "ssh_agent_socket_snapshot": ssh_agent_socket_snapshot,
            }

        try:
            return await self._persist_and_track_launch(
                instance_id=instance_id,
                task_id=task_id,
                process=process,
                actual_cwd=actual_cwd,
                loop_iteration=loop_iteration,
                chat_initiated=chat_initiated,
                provider="codex",
                task_retry_count=task_retry_count,
                task_turn_generation=task_turn_generation,
            )
        except (InstanceNotFoundError, LaunchSupersededError):
            raise
        except Exception as exc:
            # start_turn already returned a real native turn.  Even when its
            # cleanup appears successful, replaying via `codex exec` is not a
            # protocol compatibility fallback—it can duplicate model work.
            raise CodexLaunchCommitError(
                f"Codex turn ownership commit failed for instance {instance_id}"
            ) from exc

    async def _persist_and_track_launch(
        self,
        *,
        instance_id: int,
        task_id: int | None,
        process: asyncio.subprocess.Process,
        actual_cwd: str,
        loop_iteration: int | None,
        chat_initiated: bool,
        provider: str,
        task_retry_count: int | None = None,
        task_turn_generation: int | None = None,
    ) -> int:
        """Commit launch metadata and install the consumer as one guarded step."""

        consumer: asyncio.Task | None = None
        launch_started_at = datetime.utcnow()
        persisted_started_at: datetime | None = None
        try:
            async with self.db_factory() as db:
                if task_id:
                    task_update = await db.execute(
                        update(Task)
                        .where(
                            Task.id == task_id,
                            Task.instance_id == instance_id,
                            Task.status.in_(["in_progress", "executing"]),
                            task_retry_not_superseded_predicate(),
                            (
                                Task.id == task_id
                                if task_retry_count is None
                                else Task.retry_count == task_retry_count
                            ),
                            (
                                Task.id == task_id
                                if task_turn_generation is None
                                else Task.turn_generation
                                == task_turn_generation
                            ),
                        )
                        .values(last_cwd=actual_cwd)
                    )
                    if task_update.rowcount == 0:
                        raise LaunchSupersededError(
                            f"Task {task_id} no longer owns instance {instance_id}"
                        )
                instance_identity = [Instance.id == instance_id]
                if task_id is not None:
                    instance_identity.append(Instance.provider == provider)
                instance_update = await db.execute(
                    update(Instance)
                    .where(*instance_identity)
                    .values(
                        pid=process.pid,
                        process_identity=capture_process_identity(process.pid),
                        status="running",
                        current_task_id=task_id,
                        provider=provider,
                        started_at=launch_started_at,
                        last_heartbeat=datetime.utcnow(),
                    )
                )
                if instance_update.rowcount == 0:
                    raise InstanceNotFoundError(
                        f"Instance {instance_id} no longer exists"
                    )
                # MySQL's generic DATETIME drops Python microseconds.  Keep
                # the consumer generation fence aligned with the value that
                # was actually persisted, otherwise its own terminal CAS
                # rejects every direct turn on MySQL.
                persisted_started_at = await db.scalar(
                    select(Instance.started_at)
                    .where(Instance.id == instance_id)
                    .with_for_update()
                )
                await db.commit()

            # No await is allowed between task creation and map registration.
            # Once this point succeeds every live process has a stdout owner.
            consumer = asyncio.create_task(
                self._consume_output(
                    instance_id,
                    task_id,
                    process,
                    loop_iteration,
                    chat_initiated,
                    provider,
                )
            )
            self._track_output_consumer(
                instance_id,
                process,
                consumer,
                chat_initiated=chat_initiated,
                provider=provider,
                task_id=task_id,
                task_retry_count=task_retry_count,
                task_turn_generation=task_turn_generation,
                instance_started_at=persisted_started_at,
            )
            return process.pid
        except BaseException:
            async def _cleanup_failed_launch() -> None:
                reap_confirmed = True
                if consumer is not None and not consumer.done():
                    consumer.cancel()
                    await asyncio.gather(consumer, return_exceptions=True)
                try:
                    container_alive = await self._container_exec_alive(
                        instance_id, process
                    )
                except Exception:
                    # Losing Docker control-plane visibility is not proof that
                    # the inner process vanished.
                    container_alive = True
                    logger.exception(
                        "Could not inspect aborted container launch for "
                        "instance %s",
                        instance_id,
                    )
                if (
                    process.returncode is None
                    or self._process_group_alive(instance_id, process)
                    or container_alive
                ):
                    from backend.services.codex_app_server import CodexTurnProcess
                    if (
                        isinstance(process, CodexTurnProcess)
                        and self._codex_app_server is not None
                    ):
                        try:
                            await self._codex_app_server.abort_unclaimed_turn(
                                self._config_dirs.get(instance_id),
                                process,
                                reason="CCM launch metadata commit failed",
                            )
                            await self._wait_process_tree(
                                instance_id, process, 5.0
                            )
                        except Exception:
                            reap_confirmed = False
                            logger.exception(
                                "Could not abort unclaimed Codex turn for instance %s",
                                instance_id,
                            )
                    else:
                        try:
                            await self._signal_managed_process_tree(
                                instance_id, process, signal.SIGKILL
                            )
                            await self._wait_process_tree(
                                instance_id, process, 5.0
                            )
                        except Exception:
                            reap_confirmed = False
                            logger.exception(
                                "Aborted launch process group survived for instance %s",
                                instance_id,
                            )
                if reap_confirmed:
                    if self.processes.get(instance_id) is process:
                        self.processes.pop(instance_id, None)
                        self._codex_exec_homes.pop(instance_id, None)
                        self._launch_params.pop(instance_id, None)
                    if self._process_groups.get(instance_id) is process:
                        self._process_groups.pop(instance_id, None)
                    self._forget_container_exec(instance_id, process)
                    if consumer is not None and self._tasks.get(instance_id) is consumer:
                        self._tasks.pop(instance_id, None)
                    record = self._consumer_records.get(instance_id)
                    if record is not None and record.task is consumer:
                        self._consumer_records.pop(instance_id, None)
                    await self._cleanup_active_private_runtime_tempdir(
                        instance_id,
                        process,
                    )
                    self._release_task_runtime_scope_direct_owner(
                        instance_id,
                        process,
                    )
                try:
                    async with self.db_factory() as db:
                        if reap_confirmed:
                            await db.execute(
                                update(Instance)
                                .where(
                                    Instance.id == instance_id,
                                    Instance.pid == getattr(process, "pid", None),
                                )
                                .values(
                                    status="idle",
                                    pid=None,
                                    process_identity=None,
                                    current_task_id=None,
                                )
                            )
                        else:
                            await db.execute(
                                update(Instance)
                                .where(Instance.id == instance_id)
                                .values(
                                    status="error",
                                    pid=getattr(process, "pid", None),
                                    process_identity=capture_process_identity(
                                        getattr(process, "pid", None)
                                    ),
                                    current_task_id=task_id,
                                )
                            )
                        await db.commit()
                except Exception:
                    logger.exception(
                        "Failed to rollback metadata for aborted launch %s",
                        instance_id,
                    )

            cleanup = asyncio.create_task(_cleanup_failed_launch())
            await await_task_completion(cleanup)
            cleanup.result()
            raise

    def _track_output_consumer(
        self,
        instance_id: int,
        process: asyncio.subprocess.Process,
        consumer: asyncio.Task,
        *,
        chat_initiated: bool = False,
        provider: str = "claude",
        task_id: int | None = None,
        task_retry_count: int | None = None,
        task_turn_generation: int | None = None,
        instance_started_at: datetime | None = None,
    ) -> _OutputConsumerRecord:
        """Register a consumer with identity-safe terminal cleanup.

        Most cleanup happens inside ``_consume_output`` because it also owns
        database and broadcast work.  This callback is the last-resort guard:
        an unexpected exception in that bookkeeping must not leave a finished
        task in ``_tasks`` that every future Codex launch re-awaits forever.
        """

        # A new output generation on this reusable slot supersedes every
        # post-exit handoff proof retained by an older process. Do this before
        # publishing the replacement record so an old autonomous callback can
        # never borrow the new turn's instance-keyed identity.
        self.discard_pty_post_exit_generations(
            instance_id=instance_id,
            invalidate_handoffs=True,
        )
        record = _OutputConsumerRecord(
            process=process,
            task=consumer,
            chat_initiated=chat_initiated,
            provider=(provider or "claude").lower(),
            task_id=task_id,
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
            instance_started_at=instance_started_at,
        )
        self._tasks[instance_id] = consumer
        self._consumer_records[instance_id] = record
        # The instance-keyed registry can already point at a replacement when
        # an old PTY consumer reaches on_exit. Keep its immutable generation
        # record on the task itself so that callback can still clean up exactly
        # its own proxy without borrowing the replacement's identity.
        setattr(consumer, "_ccm_output_consumer_record", record)

        def _consumer_done(done: asyncio.Task) -> None:
            try:
                error = done.exception()
            except asyncio.CancelledError:
                error = None
            if error is not None:
                logger.error(
                    "Output consumer crashed for instance %s",
                    instance_id,
                    exc_info=(type(error), error, error.__traceback__),
                )

            recovery_key = (instance_id, process)
            pending_recovery = self._consumer_recovery_pending.get(
                recovery_key
            )
            if self._consumer_records.get(instance_id) is record:
                if pending_recovery is not None:
                    # The OS process is gone, but the durable Task/Instance
                    # owner was not settled.  Keep every exact in-memory handle
                    # so stop/admission can expose and retry that recovery.
                    self._consumer_errors[recovery_key] = (
                        pending_recovery.error
                    )
                    return
                if error is not None and not record.chat_initiated:
                    self._consumer_errors[(instance_id, process)] = error
                if not self._generation_reap_confirmed(
                    instance_id, process
                ):
                    # A terminal parent is not terminal generation evidence.
                    # Keep the record/task/process maps so is_running, stop and
                    # shutdown can still find and reap surviving descendants.
                    return
                self._consumer_records.pop(instance_id, None)
                if self._tasks.get(instance_id) is done:
                    self._tasks.pop(instance_id, None)
                if (
                    self.processes.get(instance_id) is process
                    and self._generation_reap_confirmed(instance_id, process)
                ):
                    self.processes.pop(instance_id, None)
                    self._codex_exec_homes.pop(instance_id, None)
                    self._launch_params.pop(instance_id, None)
                    if self._process_groups.get(instance_id) is process:
                        self._process_groups.pop(instance_id, None)
                self._release_task_runtime_scope_direct_owner(
                    instance_id,
                    process,
                )
            elif (
                pending_recovery is None
                and self._generation_reap_confirmed(instance_id, process)
            ):
                # A chat retry can publish a replacement record on the same
                # reusable slot before this old consumer's done callback runs.
                # The instance-keyed maps belong to the replacement, but the
                # exact terminal process must still surrender its Task scope.
                self._release_task_runtime_scope_direct_owner(
                    instance_id,
                    process,
                )

        consumer.add_done_callback(_consumer_done)
        return record

    def _mark_consumer_recovery_pending(
        self,
        instance_id: int,
        process: asyncio.subprocess.Process,
        *,
        error: BaseException,
        tracked_generation: bool,
        task_id: int | None,
        task_retry_count: int | None,
        task_turn_generation: int | None,
        instance_pid: int | None,
        instance_started_at: datetime | None,
        consumer: asyncio.Task | None = None,
        record: _OutputConsumerRecord | None = None,
    ) -> _ConsumerRecoveryEvidence:
        """Retain one exact terminal generation whose DB recovery is unknown."""

        evidence = _ConsumerRecoveryEvidence(
            error=error,
            tracked_generation=tracked_generation,
            task_id=task_id,
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
            instance_pid=instance_pid,
            instance_started_at=instance_started_at,
            consumer=consumer,
            record=record,
        )
        key = (instance_id, process)
        self._consumer_recovery_pending[key] = evidence
        self._consumer_errors[key] = error
        return evidence

    def _clear_consumer_recovery_pending(
        self,
        instance_id: int,
        process: asyncio.subprocess.Process | None,
    ) -> None:
        """Forget recovery evidence only after a confirmed durable settlement."""

        if process is None:
            return
        key = (instance_id, process)
        self._consumer_recovery_pending.pop(key, None)
        self._consumer_errors.pop(key, None)

    def _terminal_stop_recovery_match(
        self,
        instance_id: int,
        *,
        process: Any | None = None,
        expected_task_id: int | None | object = _EXPECTED_GENERATION_UNSET,
        expected_task_retry_count: int | object = _EXPECTED_GENERATION_UNSET,
        expected_task_turn_generation: int | object = (
            _EXPECTED_GENERATION_UNSET
        ),
        expected_pid: int | None | object = _EXPECTED_GENERATION_UNSET,
        expected_started_at: datetime | None | object = (
            _EXPECTED_GENERATION_UNSET
        ),
    ) -> tuple[Any, _ConsumerRecoveryEvidence] | None:
        """Return one exact reaped generation awaiting only durable settlement."""

        matches: list[tuple[Any, _ConsumerRecoveryEvidence]] = []
        mapped_process = self.processes.get(instance_id)
        mapped_process_group = self._process_groups.get(instance_id)
        mapped_container_process = self._container_exec_processes.get(
            instance_id
        )
        mapped_task = self._tasks.get(instance_id)
        mapped_record = self._consumer_records.get(instance_id)
        for (
            recovery_instance_id,
            recovery_process,
        ), evidence in self._consumer_recovery_pending.items():
            if (
                recovery_instance_id != instance_id
                or not evidence.tracked_generation
                or (process is not None and recovery_process is not process)
                or (
                    expected_task_id is not _EXPECTED_GENERATION_UNSET
                    and evidence.task_id != expected_task_id
                )
                or (
                    expected_task_retry_count
                    is not _EXPECTED_GENERATION_UNSET
                    and evidence.task_retry_count
                    != expected_task_retry_count
                )
                or (
                    expected_task_turn_generation
                    is not _EXPECTED_GENERATION_UNSET
                    and evidence.task_turn_generation
                    != expected_task_turn_generation
                )
                or (
                    expected_pid is not _EXPECTED_GENERATION_UNSET
                    and evidence.instance_pid != expected_pid
                )
                or (
                    expected_started_at is not _EXPECTED_GENERATION_UNSET
                    and evidence.instance_started_at != expected_started_at
                )
                or getattr(recovery_process, "pid", None)
                != evidence.instance_pid
                or not self._generation_reap_confirmed(
                    instance_id,
                    recovery_process,
                )
                or evidence.consumer is None
                or evidence.record is None
                or evidence.record.process is not recovery_process
                or evidence.record.task is not evidence.consumer
                or not evidence.consumer.done()
                or (
                    mapped_process is not None
                    and mapped_process is not recovery_process
                )
                or (
                    mapped_process_group is not None
                    and mapped_process_group is not recovery_process
                )
                or (
                    mapped_container_process is not None
                    and mapped_container_process is not recovery_process
                )
            ):
                continue
            if mapped_record is not None and mapped_record is not evidence.record:
                continue
            if mapped_task is not None and mapped_task is not evidence.consumer:
                continue
            matches.append((recovery_process, evidence))
            if len(matches) > 1:
                return None
        return matches[0] if matches else None

    async def shutdown_codex_app_server(self) -> None:
        """Stop every persistent Codex account transport at app shutdown."""
        registry = self._codex_app_server
        if registry is None:
            return
        await registry.shutdown()
        if self._codex_app_server is registry:
            self._codex_app_server = None

    async def shutdown_codex_app_server_home(
        self, codex_home: str, *, require_idle: bool = True,
    ) -> bool:
        """One-shot drain of an account transport before removal."""

        started = False
        try:
            stopped = await self.begin_codex_home_maintenance(
                codex_home, require_idle=require_idle,
            )
            started = True
            return stopped
        finally:
            if started:
                await self.end_codex_home_maintenance(codex_home)

    async def api_account_runtime_users(self, account) -> list[str]:
        """Return proven local runtime/DB users of one managed API account.

        Retirement has already durably disabled Store admission before this
        method runs. Therefore a launch which entered earlier has finished
        registering its process, while a later launch cannot spawn.
        """

        target_claude = os.path.realpath(os.path.abspath(
            account.claude_config_dir
        ))
        blockers: list[str] = []
        for instance_id, config_dir in list(self._config_dirs.items()):
            if os.path.realpath(os.path.abspath(config_dir)) != target_claude:
                continue
            if self.is_running(instance_id):
                blockers.append(f"instance {instance_id}")

        # An idle PTY process intentionally survives between visible turns and
        # can start an autonomous monitor turn without a new dispatcher spawn.
        # Do not remove credentials beneath it. The operator can disable PTY
        # mode (which drains idle sessions) or stop the owning task, then retry.
        backend = self._pty_backend
        if backend is not None:
            for instance_id, session in list(
                getattr(backend, "_sessions", {}).items()
            ):
                session_config = getattr(session, "config", None)
                config_dir = (
                    getattr(session_config, "config_dir", None)
                    or self._config_dirs.get(instance_id)
                )
                if (
                    config_dir
                    and os.path.realpath(os.path.abspath(config_dir))
                    == target_claude
                    and bool(getattr(session, "is_alive", False))
                ):
                    label = f"PTY session on instance {instance_id}"
                    if label not in blockers:
                        blockers.append(label)
            # FullMirror removes its per-turn backend._sessions entry on exit,
            # while the hot native Session remains in SessionPool and can wake
            # autonomously. Its own immutable config_dir is authoritative.
            pool = getattr(backend, "_pool", None)
            for session_id, session in list(
                getattr(pool, "_sessions", {}).items()
            ):
                session_config = getattr(session, "config", None)
                config_dir = getattr(session_config, "config_dir", None)
                if (
                    config_dir
                    and os.path.realpath(os.path.abspath(config_dir))
                    == target_claude
                    and bool(getattr(session, "is_alive", False))
                ):
                    blockers.append(f"hot PTY session {session_id}")

        # These subprocesses are not Instance generations, but they can retain
        # the same credential home after a cancelled/failed cleanup. Their
        # module registries keep exact handles until terminal proof.
        from backend.services.goal_evaluator import (
            goal_evaluator_runtime_users,
        )
        from backend.services.plan_agent_runner import plan_agent_runtime_users
        from backend.services.skill_distill import task_distill_runtime_users

        blockers.extend(goal_evaluator_runtime_users(
            "claude", account.claude_config_dir,
        ))
        blockers.extend(goal_evaluator_runtime_users(
            "codex", account.codex_home,
        ))
        blockers.extend(task_distill_runtime_users(
            account.claude_config_dir,
        ))
        blockers.extend(task_distill_runtime_users(account.codex_home))
        blockers.extend(plan_agent_runtime_users(
            account.claude_config_dir,
        ))
        blockers.extend(plan_agent_runtime_users(account.codex_home))

        # After restart, an unknown/live generation may exist only as durable
        # Task/Instance recovery evidence. Missing/mismatched ownership cannot
        # prove that the surviving process uses another credential home.
        try:
            async with self.db_factory() as db:
                tasks = (
                    await db.execute(
                        select(Task).where(
                            Task.worker_id.is_(None),
                            Task.status.in_(
                                ("in_progress", "executing", "migrating")
                            ),
                        )
                    )
                ).scalars().all()
                by_id = {task.id: task for task in tasks}
                instances = (
                    await db.execute(
                        select(Instance).where(
                            or_(
                                Instance.status == "running",
                                Instance.pid.is_not(None),
                                Instance.current_task_id.is_not(None),
                            )
                        )
                    )
                ).scalars().all()
                relevant_providers = (
                    {"codex"}
                    if getattr(account, "api_provider", None) == "apex"
                    else {"claude", "codex"}
                )

                claimed_task_ids = {
                    instance.current_task_id
                    for instance in instances
                    if instance.current_task_id is not None
                }

                # A pre-spawn active Task explicitly bound to this id blocks
                # even before an Instance generation can publish its exact
                # source transport. Claimed generations are handled below
                # from their stronger source evidence instead.
                for task in tasks:
                    if task.id in claimed_task_ids:
                        continue
                    metadata = task.metadata_ or {}
                    task_provider = str(
                        getattr(task, "provider", None) or ""
                    ).strip().lower()
                    if task_provider in {"claude", "codex"}:
                        bound_account_id = metadata.get(
                            f"{task_provider}_account_id"
                        )
                    else:
                        bound_account_id = None
                    if account.id == bound_account_id:
                        blockers.append(f"task {task.id} ({task.status})")

                for instance in instances:
                    task_id = instance.current_task_id
                    if task_id not in by_id and task_id is not None:
                        task = await db.get(Task, task_id)
                        if task is not None:
                            by_id[task_id] = task
                    else:
                        task = by_id.get(task_id)
                    if task is None:
                        blockers.append(
                            f"instance {instance.id} has unverifiable "
                            f"task claim {task_id}"
                        )
                        continue

                    source_id = task.turn_source_log_id
                    source = (
                        await db.get(LogEntry, source_id)
                        if type(source_id) is int and source_id > 0
                        else None
                    )
                    source_provider = None
                    if (
                        source is not None
                        and source.id == source_id
                        and source.task_id == task.id
                        and source.task_retry_count == task.retry_count
                        and source.task_turn_generation
                        == task.turn_generation
                        and source.turn_scope == "source"
                        and source.instance_id == instance.id
                    ):
                        from backend.services.terminal_arbitration import (
                            source_alias_original_log_id,
                            source_shape_is_canonical,
                        )

                        original_source_id = source_alias_original_log_id(
                            source
                        )
                        original_source = (
                            await db.get(LogEntry, original_source_id)
                            if original_source_id is not None
                            else None
                        )
                        if source_shape_is_canonical(
                            source,
                            original_source,
                        ):
                            source_provider = (
                                _ACTUAL_TURN_PROVIDER_BY_TRANSPORT.get(
                                    source.actual_transport
                                )
                            )

                    metadata = task.metadata_ or {}
                    if source_provider is None:
                        # A legacy, missing, malformed, or not-yet-admitted
                        # source cannot provide a negative account-ownership
                        # proof. Instance.provider may be stale on a reused
                        # slot, so use metadata only to retain positive target
                        # blockers and otherwise fail closed.
                        if account.id in {
                            metadata.get("claude_account_id"),
                            metadata.get("codex_account_id"),
                        }:
                            blockers.append(
                                f"instance {instance.id} task {task.id}"
                            )
                        else:
                            blockers.append(
                                f"instance {instance.id} has unverifiable "
                                "provider ownership"
                            )
                        continue

                    effective_provider = source_provider
                    if effective_provider not in relevant_providers:
                        continue
                    binding = metadata.get(
                        f"{effective_provider}_account_id"
                    )
                    if account.id == binding:
                        blockers.append(
                            f"instance {instance.id} task {task.id}"
                        )
                        continue
                    if isinstance(binding, str) and binding.strip():
                        # A durable exact binding to another account is the
                        # only safe negative proof for a persisted generation.
                        continue
                    blockers.append(
                        f"instance {instance.id} has unverifiable "
                        "provider account ownership"
                    )
        except Exception as exc:
            raise RuntimeError(
                "Could not verify durable task account ownership",
            ) from exc
        return sorted(set(blockers))

    async def detach_api_account_containers(self, account) -> int:
        """Remove idle CCM containers retaining a read-only key bind mount."""

        from backend.services.container_manager import ContainerManager

        manager = getattr(self, "_container_mgr", None)
        if manager is None:
            manager = ContainerManager()
        return await manager.retire_api_account_mounts(account.root)

    async def begin_codex_home_maintenance(
        self, codex_home: str, *, require_idle: bool = True,
    ) -> bool:
        """Reserve one CODEX_HOME while auth files are replaced or removed.

        A registry is created even when Codex has not run yet so a concurrent
        first turn cannot slip through the relogin/delete window.  Callers must
        pair this with ``end_codex_home_maintenance`` in ``finally``.
        """

        from backend.services.codex_app_server import (
            CodexAppServerBusyError,
            normalize_codex_home,
        )

        home = normalize_codex_home(codex_home)
        home_lock = self._codex_home_lock(home)
        async with home_lock:
            if home in self._codex_home_maintenance:
                raise CodexAppServerBusyError(
                    f"Codex account is already under maintenance: {home}"
                )
            if self._codex_ephemeral_home_users.get(home, 0):
                raise CodexAppServerBusyError(
                    f"Codex account still has an active ephemeral exec: {home}"
                )
            if home in self._codex_retained_ephemeral_homes():
                raise CodexAppServerBusyError(
                    f"Codex account still has a retained ephemeral exec: {home}"
                )
            if require_idle:
                for instance_id, exec_home in self._codex_exec_homes.items():
                    process = self.processes.get(instance_id)
                    if (
                        exec_home == home
                        and process is not None
                        and process.returncode is None
                    ):
                        raise CodexAppServerBusyError(
                            f"Codex account still has an active exec turn: {home}"
                        )

            registry = self._ensure_codex_app_server_registry()
            stopped = await registry.begin_home_maintenance(
                home, require_idle=require_idle,
            )
            self._codex_home_maintenance.add(home)
            return stopped

    async def end_codex_home_maintenance(
        self, codex_home: str,
    ) -> None:
        """Release a CODEX_HOME maintenance reservation."""

        from backend.services.codex_app_server import normalize_codex_home

        home = normalize_codex_home(codex_home)
        home_lock = self._codex_home_lock(home)

        async def _release_home() -> None:
            async with home_lock:
                if self._codex_app_server is not None:
                    await self._codex_app_server.end_home_maintenance(home)
                self._codex_home_maintenance.discard(home)

        await _settle_instance_cleanup(_release_home())

    async def begin_codex_app_server_home_maintenance(
        self, codex_home: str, *, require_idle: bool = True,
    ) -> bool:
        """Compatibility alias for account API callers."""

        return await self.begin_codex_home_maintenance(
            codex_home, require_idle=require_idle,
        )

    async def end_codex_app_server_home_maintenance(
        self, codex_home: str,
    ) -> None:
        """Compatibility alias for account API callers."""

        await self.end_codex_home_maintenance(codex_home)

    async def rebind_codex_app_server_thread(
        self,
        thread_id: str,
        *,
        source_codex_home: str | None,
        target_codex_home: str,
    ) -> None:
        """Update the live registry after a rollout was migrated."""

        if self._codex_app_server is None:
            # No process has loaded the thread in this backend lifetime; the
            # next resume can establish ownership directly in the target home.
            return
        await self._codex_app_server.rebind_thread(
            thread_id,
            source_codex_home=source_codex_home,
            target_codex_home=target_codex_home,
        )

    async def rebind_codex_thread(
        self,
        thread_id: str,
        *,
        source_codex_home: str | None,
        target_codex_home: str,
    ) -> None:
        """Dispatcher-facing alias for a migrated rollout rebind."""

        await self.rebind_codex_app_server_thread(
            thread_id,
            source_codex_home=source_codex_home,
            target_codex_home=target_codex_home,
        )

    async def clear_codex_thread_owner_for_recovery(
        self,
        thread_id: str,
        *,
        expected_codex_home: str,
    ) -> bool:
        """Forget an idle in-memory route so durable DB affinity wins again."""

        if self._codex_app_server is None:
            return True
        return await self._codex_app_server.clear_thread_owner_for_recovery(
            thread_id,
            expected_codex_home=expected_codex_home,
        )

    @staticmethod
    def _claude_task_runtime_fingerprint(
        settings_path: Path,
        *,
        mcp_config_path: str | Path | None,
        git_env: dict | None,
        ssh_agent_socket_identity: tuple[object, ...] | None = None,
    ) -> str:
        """Fingerprint every launch input retained by a hot Claude process."""

        digest = hashlib.sha256()

        def add_component(label: str, value: bytes) -> None:
            encoded_label = label.encode("utf-8")
            digest.update(len(encoded_label).to_bytes(4, "big"))
            digest.update(encoded_label)
            digest.update(len(value).to_bytes(8, "big"))
            digest.update(value)

        add_component("settings", settings_path.read_bytes())
        from backend.services.trusted_runtime import (
            trusted_hook_components_from_settings,
        )

        for asset_name, asset_bytes in trusted_hook_components_from_settings(
            settings_path
        ):
            add_component(f"trusted-hook:{asset_name}", asset_bytes)
        if mcp_config_path is not None:
            add_component("mcp", Path(mcp_config_path).read_bytes())
        add_component(
            "ssh-agent-socket-identity",
            json.dumps(
                ssh_agent_socket_identity,
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode("utf-8"),
        )
        environment_items = sorted(
            (
                str(key),
                str(value),
            )
            for key, value in (git_env or {}).items()
        )
        add_component(
            "git-env-count",
            len(environment_items).to_bytes(8, "big"),
        )
        for key, value in environment_items:
            # Only the terminal SHA-256 digest is retained on the PTY config;
            # credential names and values never leave this local hash state.
            add_component("git-env-key", key.encode("utf-8"))
            add_component("git-env-value", value.encode("utf-8"))
        return digest.hexdigest()

    def _reserve_task_runtime_scope(self, task_id: int) -> None:
        """Fence a Task scope while launch files are being materialized."""

        self._task_runtime_scope_pending.add(task_id)

    def _discard_task_runtime_scope_reservation(self, task_id: int) -> bool:
        """Release a pre-spawn fence after proving no generation escaped."""

        self._task_runtime_scope_pending.discard(task_id)
        return self._cleanup_task_runtime_scope_if_unused(task_id)

    def _adopt_task_runtime_scope_direct(
        self,
        task_id: int,
        instance_id: int,
        process: object,
    ) -> None:
        key = (instance_id, process)
        existing = self._task_runtime_scope_direct_owners.get(key)
        if existing is not None and existing != task_id:
            raise RuntimeError("Direct process already owns another Task scope")
        self._task_runtime_scope_direct_owners[key] = task_id
        self._task_runtime_scope_pending.discard(task_id)

    def _adopt_task_runtime_scope_pty(
        self,
        task_id: int,
        session: object,
    ) -> None:
        existing = self._task_runtime_scope_pty_owners.get(session)
        if existing is not None and existing != task_id:
            raise RuntimeError("PTY Session already owns another Task scope")
        self._task_runtime_scope_pty_owners[session] = task_id
        self._task_runtime_scope_pending.discard(task_id)
        self._install_task_runtime_scope_pty_stop_callback(session)
        self._reap_dead_task_runtime_scope_pty_owners()

    def _install_task_runtime_scope_pty_stop_callback(
        self,
        session: object,
    ) -> None:
        """Release a Session owner after its exact normal stop succeeds.

        The pinned pool's periodic reaper has no host callback, but every
        remove/evict/reap/shutdown path awaits ``Session.stop``.  Wrapping the
        resident object covers those paths while deliberately retaining scope
        across a native-process death that Session may auto-resume.
        """

        stop = getattr(session, "stop", None)
        if not callable(stop):
            return
        marker = "_ccm_task_runtime_scope_stop_callback"
        if getattr(session, marker, False):
            return

        async def stop_and_release(*args, **kwargs):
            result = await _settle_instance_cleanup(stop(*args, **kwargs))
            backend = self._pty_backend
            registrations = (
                getattr(getattr(backend, "_pool", None), "_sessions", None),
                getattr(backend, "_sessions", None),
            )
            remains_registered = any(
                candidate is session
                for sessions in registrations
                if isinstance(sessions, dict)
                for candidate in sessions.values()
            )
            if (
                not remains_registered
                and getattr(session, "is_alive", True) is False
            ):
                self._release_task_runtime_scope_pty_owner(session)
            return result

        setattr(session, "stop", stop_and_release)
        setattr(session, marker, True)

    def _reap_dead_task_runtime_scope_pty_owners(self) -> None:
        """Release Sessions already removed by claude-pty's own pool reaper.

        Overflow eviction and the periodic idle reaper live inside the pinned
        ``claude_pty`` dependency and do not call back into InstanceManager.
        Absence from that pool plus exact native death is the conservative
        proof that their Task assets are no longer in use.
        """

        registered_sessions: set[object] = set()
        backend = self._pty_backend
        if backend is not None:
            for sessions in (
                getattr(getattr(backend, "_pool", None), "_sessions", None),
                getattr(backend, "_sessions", None),
            ):
                if isinstance(sessions, dict):
                    registered_sessions.update(sessions.values())
        for session in tuple(self._task_runtime_scope_pty_owners):
            if (
                session not in registered_sessions
                and getattr(session, "is_alive", True) is False
            ):
                self._release_task_runtime_scope_pty_owner(session)

    def _task_runtime_scope_has_owner(self, task_id: int) -> bool:
        return bool(
            task_id in self._task_runtime_scope_pending
            or task_id in self._task_runtime_scope_direct_owners.values()
            or task_id in self._task_runtime_scope_pty_owners.values()
        )

    def _cleanup_task_runtime_scope_if_unused(self, task_id: int) -> bool:
        """Remove a scope only when no exact provider generation owns it."""

        if self._task_runtime_scope_has_owner(task_id):
            return False
        from backend.services.mcp_config import cleanup_mcp_config

        cleanup_mcp_config(task_id)
        return True

    def _release_task_runtime_scope_direct_owner(
        self,
        instance_id: int,
        process: object,
    ) -> None:
        task_id = self._task_runtime_scope_direct_owners.pop(
            (instance_id, process), None
        )
        if task_id is not None:
            self._cleanup_task_runtime_scope_if_unused(task_id)

    def _release_task_runtime_scope_pty_owner(self, session: object) -> None:
        task_id = self._task_runtime_scope_pty_owners.pop(session, None)
        if task_id is not None:
            self._cleanup_task_runtime_scope_if_unused(task_id)

    def cleanup_task_runtime_scope_after_turn(self, task_id: int) -> bool:
        """Dispatcher terminal hook; hot PTY owners deliberately retain it."""

        had_owner = self._task_runtime_scope_has_owner(task_id)
        self._reap_dead_task_runtime_scope_pty_owners()
        if had_owner and not self._task_runtime_scope_has_owner(task_id):
            # Reaping the final dead PTY owner already performed the cleanup.
            return True
        return self._cleanup_task_runtime_scope_if_unused(task_id)

    def _reserve_private_runtime_tempdir(
        self,
        instance_id: int,
        runtime_tempdir,
    ) -> None:
        """Reserve one pre-spawn scratch generation for an Instance launch."""

        current = self._pending_private_runtime_tempdirs.get(instance_id)
        if current is not None and current is not runtime_tempdir:
            raise RuntimeError(
                "Instance already owns a pending private runtime directory"
            )
        runtime_tempdir.assert_valid()
        self._pending_private_runtime_tempdirs[instance_id] = runtime_tempdir

    def _bind_private_runtime_tempdir(
        self,
        instance_id: int,
        runtime_tempdir,
    ) -> None:
        """Transfer a pending scratch leaf to the imminent provider turn."""

        if self._pending_private_runtime_tempdirs.get(instance_id) is not runtime_tempdir:
            raise RuntimeError(
                "Private runtime directory lost its launch reservation"
            )
        runtime_tempdir.assert_valid()
        if not runtime_tempdir.bound:
            runtime_tempdir.bind_to_runtime()

    def _adopt_private_runtime_tempdir(
        self,
        instance_id: int,
        process: object,
        runtime_tempdir,
    ) -> None:
        """Bind cleanup ownership to an exact process object."""

        if self._pending_private_runtime_tempdirs.get(instance_id) is not runtime_tempdir:
            raise RuntimeError(
                "Private runtime directory lost its pending owner"
            )
        if not runtime_tempdir.bound:
            raise RuntimeError(
                "Private runtime directory was not bound before provider launch"
            )
        key = (instance_id, process)
        existing = self._active_private_runtime_tempdirs.get(key)
        if existing is not None and existing is not runtime_tempdir:
            raise RuntimeError(
                "Process already owns another private runtime directory"
            )
        self._active_private_runtime_tempdirs[key] = runtime_tempdir
        self._pending_private_runtime_tempdirs.pop(instance_id, None)

    async def _cleanup_unbound_private_runtime_tempdir(
        self,
        instance_id: int,
    ) -> None:
        runtime_tempdir = self._pending_private_runtime_tempdirs.get(instance_id)
        if runtime_tempdir is None:
            return
        cleaned = await _settle_instance_cleanup(
            asyncio.to_thread(runtime_tempdir.cleanup_if_unbound)
        )
        if cleaned or runtime_tempdir.cleaned:
            if self._pending_private_runtime_tempdirs.get(instance_id) is runtime_tempdir:
                self._pending_private_runtime_tempdirs.pop(instance_id, None)

    async def _cleanup_active_private_runtime_tempdir(
        self,
        instance_id: int,
        process: object,
    ) -> None:
        key = (instance_id, process)
        runtime_tempdir = self._active_private_runtime_tempdirs.get(key)
        if runtime_tempdir is None:
            return
        await _settle_instance_cleanup(
            asyncio.to_thread(runtime_tempdir.cleanup)
        )
        if self._active_private_runtime_tempdirs.get(key) is runtime_tempdir:
            self._active_private_runtime_tempdirs.pop(key, None)

    async def _abort_private_runtime_tempdir(
        self,
        instance_id: int,
        runtime_tempdir,
        *,
        process: object | None,
    ) -> None:
        """Clean an aborted generation after its provider is proven absent."""

        if process is not None:
            key = (instance_id, process)
            if self._active_private_runtime_tempdirs.get(key) is runtime_tempdir:
                await self._cleanup_active_private_runtime_tempdir(
                    instance_id,
                    process,
                )
                return
        await _settle_instance_cleanup(asyncio.to_thread(runtime_tempdir.cleanup))
        if self._pending_private_runtime_tempdirs.get(instance_id) is runtime_tempdir:
            self._pending_private_runtime_tempdirs.pop(instance_id, None)

    async def _launch_pty(
        self,
        instance_id: int,
        prompt: str,
        task_id: int | None,
        cwd: str | None,
        model: str | None,
        resume_session_id: str | None,
        loop_iteration: int | None,
        git_env: dict | None,
        thinking_budget: int | None,
        effort_level: str | None,
        chat_initiated: bool,
        config_dir: str | None,
        enable_workflows: bool,
        enabled_skills: dict | None,
        mcp_config_path: str | None,
        claude_binary_override: str | None = None,
        container_exec_spec=None,
        task_retry_count: int | None = None,
        task_turn_generation: int | None = None,
        skill_context: str = "",
        cloudrouter_api: bool = False,
        source_log_id: int | None = None,
        current_message: str | None = None,
        queue_timestamp: float | None = None,
        initiating_user_id: int | None = None,
        initiating_user_role: str = "member",
        execution_mode: str = "sandbox",
        execution_principal_kind: str = "system",
        attachment_paths: Sequence[str] = (),
        ssh_agent_socket_snapshot: _SshAgentSocketSnapshot | None = None,
        claude_isolation_settings_path: Path | None = None,
        claude_isolation_tools: Sequence[str] | None = None,
        claude_isolation_allowed_rules: Sequence[str] | None = None,
        claude_unrestricted_settings_path: Path | None = None,
        claude_unrestricted_tools: Sequence[str] | None = None,
        claude_unrestricted_allowed_rules: Sequence[str] | None = None,
        claude_task_runtime_scope_reserved: bool = False,
        private_runtime_tempdir=None,
        on_launch_admitted: Callable[[], Awaitable[None]] | None = None,
    ) -> int:
        """PTY-mode launch: delegate to claude_pty, mirror -p bookkeeping.

        The backend installs a process proxy into self.processes and a
        consumer into self._tasks; events flow back through _process_event,
        so everything downstream (DB, WebSocket, dispatcher wait) is
        unchanged.
        """
        isolation_fingerprint = None
        runtime_settings_path = (
            claude_unrestricted_settings_path
            or claude_isolation_settings_path
        )
        if runtime_settings_path is not None:
            isolation_fingerprint = (
                self._claude_task_runtime_fingerprint(
                    runtime_settings_path,
                    mcp_config_path=mcp_config_path,
                    git_env=git_env,
                    ssh_agent_socket_identity=(
                        ssh_agent_socket_snapshot.identity
                        if ssh_agent_socket_snapshot is not None
                        else None
                    ),
                )
            )
        elif claude_unrestricted_tools is not None:
            # Legacy unrestricted Delivery turns intentionally have no Task
            # settings/MCP profile. Their fixed built-in inventory remains the
            # complete hot-process boundary.
            isolation_fingerprint = (
                "agent-unrestricted-v1",
                tuple(claude_unrestricted_tools),
            )
        if isolation_fingerprint is not None and resume_session_id:
            existing_session = (
                self._pty_backend._pool._sessions.get(
                    resume_session_id
                )
            )
            existing_fingerprint = getattr(
                getattr(existing_session, "config", None),
                "_ccm_task_isolation_fingerprint",
                None,
            )
            if (
                existing_session is not None
                and existing_fingerprint != isolation_fingerprint
            ):
                # A hot process cannot absorb changed CLI settings. Stop
                # it while idle and cold-resume the same native session.
                released = await self.release_pty_session(resume_session_id)
                if not released:
                    raise RuntimeError(
                        "Changed Claude Task runtime could not stop its exact "
                        "hot PTY Session"
                    )

        is_cold_start = (
            resume_session_id
            and resume_session_id not in self._pty_backend._pool._sessions
        )
        if is_cold_start and task_id:
            await self.broadcaster.broadcast(f"task:{task_id}", {
                "event_type": "system_event",
                "content": "正在恢复 PTY 会话，请稍候...",
                "pty_cold_start": True,
            })

        metadata_barrier = asyncio.Event()
        self._pty_launch_barriers[instance_id] = metadata_barrier
        previous_process = self.processes.get(instance_id)
        previous_consumer = self._tasks.get(instance_id)
        process = None
        consumer = None
        session_id = None
        pty_launch_params = None
        # The PTY backend starts its output consumer inside launch_for_ccm().
        # Bind the account route and exact retry parameters before that call so
        # a fast API error cannot outrun CloudRouter classification.
        if config_dir:
            self._config_dirs[instance_id] = config_dir
        if chat_initiated:
            pty_launch_params = {
                "prompt": prompt,
                "task_id": task_id,
                "task_turn_generation": task_turn_generation,
                "cwd": cwd,
                "model": model,
                "git_env": git_env,
                "thinking_budget": thinking_budget,
                "effort_level": effort_level,
                "enable_workflows": enable_workflows,
                "enabled_skills": enabled_skills,
                "provider": "claude",
                "config_dir": config_dir,
                "source_log_id": source_log_id,
                "current_message": current_message or prompt,
                "queue_timestamp": queue_timestamp,
                "initiating_user_id": initiating_user_id,
                "initiating_user_role": initiating_user_role,
                "execution_mode": execution_mode,
                "execution_principal_kind": execution_principal_kind,
                "attachment_paths": tuple(attachment_paths),
                "ssh_agent_socket_snapshot": ssh_agent_socket_snapshot,
            }
            self._launch_params[instance_id] = pty_launch_params
        try:
            # build_config is shared by every PTY instance. Hold one global
            # admission lock across patch -> config construction -> restore so
            # a container wrapper can never leak into another launch.
            async with self._pty_build_config_lock:
                original_build_config = getattr(
                    self._pty_backend,
                    "build_config",
                    None,
                )
                wrapper = claude_binary_override
                if original_build_config is None and (
                    claude_isolation_settings_path is not None
                    or claude_unrestricted_settings_path is not None
                    or claude_unrestricted_tools is not None
                    or cloudrouter_api
                    or wrapper is not None
                ):
                    raise RuntimeError(
                        "PTY backend cannot apply the required launch boundary"
                    )

                def _patched_build_config(**kw):
                    if original_build_config is None:
                        raise RuntimeError(
                            "PTY backend does not expose secure config construction"
                        )
                    cfg = original_build_config(**kw)
                    cfg.response_timeout = float(
                        settings.claude_pty_response_idle_timeout_seconds
                    )
                    original_overrides = dict(
                        getattr(cfg, "env_overrides", None) or {}
                    )
                    from backend.services.task_agent_isolation import (
                        CLAUDE_SUBPROCESS_ENV_SCRUB,
                        scrub_task_model_environment,
                    )

                    overrides = scrub_task_model_environment(
                        original_overrides,
                        provider="claude",
                    )
                    safe_parent = scrub_task_model_environment(
                        os.environ,
                        provider="claude",
                    )
                    # claude-pty begins with os.environ and only then applies
                    # env_overrides. Shadow every credential that CCM's direct
                    # path removes; simply omitting a key would reveal the
                    # Manager value again. Explicit project GIT_* variables are
                    # restored below from this exact launch's git_env.
                    for key in {
                        *original_overrides,
                        *os.environ,
                    }:
                        if (
                            (
                                key in original_overrides
                                and key not in overrides
                            )
                            or (
                                key in os.environ
                                and key not in safe_parent
                            )
                        ):
                            overrides[key] = ""
                    for key, value in (git_env or {}).items():
                        upper_key = key.upper()
                        if (
                            upper_key.startswith("GIT_")
                            or upper_key in _DELIVERY_SAFE_GIT_ENV
                            or upper_key in _DELIVERY_RUNTIME_TEMP_ENV_KEYS
                            or upper_key
                            in {
                                "CCM_ASK_USER_TOKEN",
                                "CCM_TASK_SSH_GUARD",
                                "SSH_AUTH_SOCK",
                            }
                        ):
                            overrides[key] = value
                    # Claude strips CLAUDE_* from the PTY parent environment,
                    # so this security switch must be an explicit override.
                    overrides[CLAUDE_SUBPROCESS_ENV_SCRUB] = "1"
                    overrides["AUTH_TOKEN"] = ""
                    overrides["CCM_INTERNAL_SERVICE_TOKEN"] = ""
                    final_binary = wrapper or cfg.claude_binary
                    if cloudrouter_api:
                        from backend.services.claude_auth_projection import (
                            inject_cloudrouter_claude_direct_auth,
                        )

                        if not inject_cloudrouter_claude_direct_auth(
                            overrides,
                            self.cloudrouter_store,
                            config_dir,
                        ):
                            raise RuntimeError(
                                "Selected Claude API account could not be "
                                "projected into the PTY model process"
                            )
                    if claude_isolation_settings_path is not None:
                        from backend.services.task_agent_isolation import (
                            CLAUDE_TASK_BUILTIN_TOOLS,
                            claude_permission_allow_rules,
                        )

                        selected_claude_tools = tuple(
                            claude_isolation_tools
                            or CLAUDE_TASK_BUILTIN_TOOLS
                        )
                        selected_allowed_rules = tuple(
                            claude_isolation_allowed_rules
                            or claude_permission_allow_rules(
                                selected_claude_tools,
                            )
                        )

                        task_wrapper = Path(__file__).with_name(
                            "task_claude_wrapper.sh"
                        )
                        if not (
                            task_wrapper.is_file()
                            and os.access(task_wrapper, os.X_OK)
                        ):
                            raise RuntimeError(
                                "Task Claude isolation wrapper is unavailable"
                            )
                        overrides.update({
                            "CCM_TASK_CLAUDE_SETTINGS": str(
                                claude_isolation_settings_path
                            ),
                            "CCM_TASK_CLAUDE_BINARY": str(final_binary),
                            "CCM_TASK_CLAUDE_TOOLS": ",".join(
                                selected_claude_tools
                            ),
                            "CCM_TASK_CLAUDE_ALLOWED_RULES": ",".join(
                                selected_allowed_rules
                            ),
                        })
                        cfg.claude_binary = str(task_wrapper)
                        cfg.dangerously_skip_permissions = False
                        setattr(
                            cfg,
                            "_ccm_task_isolation_fingerprint",
                            isolation_fingerprint,
                        )
                    elif claude_unrestricted_tools is not None:
                        from backend.services.task_agent_isolation import (
                            claude_permission_allow_rules,
                        )

                        selected_claude_tools = tuple(
                            claude_unrestricted_tools
                        )
                        selected_allowed_rules = tuple(
                            claude_unrestricted_allowed_rules
                            or claude_permission_allow_rules(
                                selected_claude_tools,
                                include_mcp_tools=(
                                    claude_unrestricted_settings_path
                                    is not None
                                ),
                            )
                        )
                        task_wrapper = Path(__file__).with_name(
                            "task_claude_wrapper.sh"
                        )
                        if not (
                            task_wrapper.is_file()
                            and os.access(task_wrapper, os.X_OK)
                        ):
                            raise RuntimeError(
                                "Task Claude permission wrapper is unavailable"
                            )
                        overrides.update({
                            "CCM_TASK_CLAUDE_PROFILE": "unrestricted",
                            "CCM_TASK_CLAUDE_BINARY": str(final_binary),
                            "CCM_TASK_CLAUDE_TOOLS": ",".join(
                                selected_claude_tools
                            ),
                            "CCM_TASK_CLAUDE_ALLOWED_RULES": ",".join(
                                selected_allowed_rules
                            ),
                        })
                        if claude_unrestricted_settings_path is not None:
                            overrides["CCM_TASK_CLAUDE_SETTINGS"] = str(
                                claude_unrestricted_settings_path
                            )
                        cfg.claude_binary = str(task_wrapper)
                        cfg.dangerously_skip_permissions = True
                        setattr(
                            cfg,
                            "_ccm_task_isolation_fingerprint",
                            isolation_fingerprint,
                        )
                    else:
                        cfg.claude_binary = str(final_binary)
                    cfg.env_overrides = overrides
                    return cfg

                setattr(
                    self._pty_backend,
                    "build_config",
                    _patched_build_config,
                )
                try:
                    from backend.services.skill_context import (
                        wrap_skill_context,
                    )

                    wrapped_prompt = wrap_skill_context(prompt, skill_context)
                    if on_launch_admitted is not None:
                        await on_launch_admitted()
                    if private_runtime_tempdir is not None:
                        self._bind_private_runtime_tempdir(
                            instance_id,
                            private_runtime_tempdir,
                        )
                    session_id = await self._pty_backend.launch_for_ccm(
                        instance_id=instance_id,
                        prompt=wrapped_prompt,
                        task_id=task_id,
                        cwd=cwd,
                        model=model if model and model != "default" else None,
                        resume_session_id=resume_session_id,
                        loop_iteration=loop_iteration,
                        git_env=git_env,
                        thinking_budget=thinking_budget,
                        effort_level=effort_level,
                        chat_initiated=chat_initiated,
                        config_dir=config_dir,
                        enable_workflows=enable_workflows,
                        enabled_skills=enabled_skills,
                        mcp_config_path=mcp_config_path,
                    )
                finally:
                    if original_build_config is None:
                        delattr(self._pty_backend, "build_config")
                    else:
                        self._pty_backend.build_config = original_build_config

            process = self.processes.get(instance_id)
            consumer = self._tasks.get(instance_id)
            if process is None:
                raise RuntimeError(
                    "PTY backend did not register a process during startup"
                )
            if private_runtime_tempdir is not None:
                self._adopt_private_runtime_tempdir(
                    instance_id,
                    process,
                    private_runtime_tempdir,
                )
            native_session = getattr(process, "session", None)
            if (
                native_session is not None
                and getattr(native_session, "is_alive", None) is False
            ):
                native_process = getattr(native_session, "_process", None)
                exit_code = getattr(native_process, "exit_code", None)
                # BasePTYBackend creates the output consumer before returning.
                # Prevent that exact task from observing the dead process and
                # entering Session._auto_resume while failure cleanup starts.
                if consumer is not None and not consumer.done():
                    consumer.cancel()
                raise RuntimeError(
                    "PTY process exited during startup "
                    f"(exit_code={exit_code})"
                )
            if (
                task_id is not None
                and claude_task_runtime_scope_reserved
                and native_session is not None
            ):
                self._adopt_task_runtime_scope_pty(task_id, native_session)
            turn_started_at = datetime.utcnow()
            if container_exec_spec is not None and process is not None:
                self._container_mgr.register_exec(
                    process, container_exec_spec
                )
                self._container_exec_processes[instance_id] = process
            consumer_record = None
            if consumer is not None and process is not None:
                consumer_record = self._track_output_consumer(
                    instance_id,
                    process,
                    consumer,
                    chat_initiated=chat_initiated,
                    provider="claude",
                    task_id=task_id,
                    task_retry_count=task_retry_count,
                    task_turn_generation=task_turn_generation,
                    instance_started_at=turn_started_at,
                )
            pid = getattr(process, "pid", 0) or 0

            # Session affinity and instance ownership become visible in one
            # commit. A failure/cancellation below tears down the exact PTY
            # generation before the per-instance lifecycle lock is released.
            async with self.db_factory() as db:
                if task_id:
                    task_values = {"last_cwd": cwd or os.getcwd()}
                    if session_id:
                        task_values["session_id"] = session_id
                    task_update = await db.execute(
                        update(Task)
                        .where(
                            Task.id == task_id,
                            Task.instance_id == instance_id,
                            Task.status.in_(["in_progress", "executing"]),
                            Task.pty_background_generation.is_(None),
                            task_retry_not_superseded_predicate(),
                            (
                                Task.id == task_id
                                if task_retry_count is None
                                else Task.retry_count == task_retry_count
                            ),
                            (
                                Task.id == task_id
                                if task_turn_generation is None
                                else Task.turn_generation
                                == task_turn_generation
                            ),
                        )
                        .values(**task_values)
                    )
                    if task_update.rowcount == 0:
                        raise LaunchSupersededError(
                            f"Task {task_id} no longer owns instance {instance_id}"
                        )
                instance_identity = [Instance.id == instance_id]
                if task_id is not None:
                    instance_identity.append(Instance.provider == "claude")
                instance_update = await db.execute(
                    update(Instance)
                    .where(*instance_identity)
                    .values(
                        pid=pid,
                        process_identity=capture_process_identity(pid),
                        status="running",
                        current_task_id=task_id,
                        provider="claude",
                        started_at=turn_started_at,
                        last_heartbeat=datetime.utcnow(),
                    )
                )
                if instance_update.rowcount == 0:
                    raise InstanceNotFoundError(
                        f"Instance {instance_id} no longer exists"
                    )
                persisted_turn_started_at = (
                    await db.execute(
                        select(Instance.started_at)
                        .where(Instance.id == instance_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if consumer_record is not None:
                    # The PTY consumer is registered before this commit so its
                    # on-exit callback cannot outrun launch metadata. Update
                    # that same immutable identity object with the
                    # database-normalized timestamp before opening the
                    # metadata barrier.
                    object.__setattr__(
                        consumer_record,
                        "instance_started_at",
                        persisted_turn_started_at,
                    )
                await db.commit()
            metadata_barrier.set()
            if self._pty_launch_barriers.get(instance_id) is metadata_barrier:
                self._pty_launch_barriers.pop(instance_id, None)
            return pid
        except BaseException:
            process = self.processes.get(instance_id)
            consumer = self._tasks.get(instance_id)
            if (
                private_runtime_tempdir is not None
                and process is not None
                and private_runtime_tempdir.bound
                and self._pending_private_runtime_tempdirs.get(instance_id)
                is private_runtime_tempdir
            ):
                self._adopt_private_runtime_tempdir(
                    instance_id,
                    process,
                    private_runtime_tempdir,
                )
            consumer_record = self._consumer_records.get(instance_id)
            if (
                consumer_record is not None
                and consumer_record.process is process
                and consumer_record.task is consumer
            ):
                # launch() still owns the lifecycle lock here.  Win terminal
                # ownership before opening the metadata barrier; otherwise
                # backend.stop would await a consumer that re-enters this lock.
                self._claim_pty_terminal_owner(consumer_record, "stop")
            # Unblock an on_exit that may already be waiting.  The owner claim
            # above makes the explicit rollback the sole terminal DB writer.
            metadata_barrier.set()
            if self._pty_launch_barriers.get(instance_id) is metadata_barrier:
                self._pty_launch_barriers.pop(instance_id, None)
            if (
                container_exec_spec is not None
                and process is not None
                and not self._container_mgr.owns_exec(process)
            ):
                self._container_mgr.register_exec(
                    process, container_exec_spec
                )
                self._container_exec_processes[instance_id] = process
            owns_new_process = (
                process is not None and process is not previous_process
            )
            owns_new_consumer = (
                consumer is not None and consumer is not previous_consumer
            )

            async def _cleanup_failed_pty_launch() -> None:
                reap_confirmed = not (owns_new_process or owns_new_consumer)
                if (
                    owns_new_process
                    or owns_new_consumer
                    or (
                        private_runtime_tempdir is not None
                        and private_runtime_tempdir.bound
                    )
                ):
                    backend_stopped = True
                    stop_pty = getattr(self._pty_backend, "stop", None)
                    if stop_pty is not None:
                        try:
                            await stop_pty(instance_id)
                        except Exception:
                            backend_stopped = False
                            logger.exception(
                                "Failed to stop aborted PTY launch for instance %s",
                                instance_id,
                            )
                    if consumer is not None and not consumer.done():
                        consumer.cancel()
                        await asyncio.gather(consumer, return_exceptions=True)
                    container_alive = False
                    if process is not None:
                        try:
                            container_alive = await self._container_exec_alive(
                                instance_id, process
                            )
                        except Exception:
                            container_alive = True
                    if process is not None and (
                        process.returncode is None or container_alive
                    ):
                        try:
                            await self._signal_managed_process_tree(
                                instance_id, process, signal.SIGKILL
                            )
                            await self._wait_process_tree(
                                instance_id, process, 10.0
                            )
                        except Exception:
                            backend_stopped = False
                            logger.exception(
                                "Aborted PTY/container process did not "
                                "terminate for instance %s",
                                instance_id,
                            )

                    reap_confirmed = backend_stopped and (
                        process is None
                        or self._generation_reap_confirmed(
                            instance_id,
                            process,
                        )
                    )
                    if reap_confirmed and process is not None:
                        try:
                            reap_confirmed = not await self._container_exec_alive(
                                instance_id, process
                            )
                        except Exception:
                            reap_confirmed = False
                    if reap_confirmed:
                        if process is not None:
                            self._forget_container_exec(instance_id, process)
                        if self.processes.get(instance_id) is process:
                            self.processes.pop(instance_id, None)
                        if self._tasks.get(instance_id) is consumer:
                            self._tasks.pop(instance_id, None)
                        record = self._consumer_records.get(instance_id)
                        if record is not None and record.task is consumer:
                            self._consumer_records.pop(instance_id, None)
                if reap_confirmed and private_runtime_tempdir is not None:
                    await self._abort_private_runtime_tempdir(
                        instance_id,
                        private_runtime_tempdir,
                        process=process,
                    )
                if reap_confirmed:
                    native_session = (
                        getattr(process, "session", None)
                        if process is not None
                        else None
                    )
                    if (
                        native_session is not None
                        and getattr(native_session, "is_alive", True) is False
                    ):
                        self._release_task_runtime_scope_pty_owner(
                            native_session
                        )
                    if (
                        task_id is not None
                        and claude_task_runtime_scope_reserved
                    ):
                        self._discard_task_runtime_scope_reservation(task_id)
                    if (
                        pty_launch_params is not None
                        and self._launch_params.get(instance_id)
                        is pty_launch_params
                    ):
                        self._launch_params.pop(instance_id, None)
                    if (
                        config_dir
                        and self._config_dirs.get(instance_id) == config_dir
                    ):
                        self._config_dirs.pop(instance_id, None)
                if (
                    reap_confirmed
                    and process is None
                    and container_exec_spec is not None
                ):
                    self._container_mgr.discard_spec(container_exec_spec)
                    self._container_tasks.pop(instance_id, None)

                # Commit outcome is indeterminate under cancellation.  Reopen
                # the slot only after both PTY backend and proxy are confirmed
                # stopped; otherwise retain the generation evidence fail-closed.
                try:
                    async with self.db_factory() as db:
                        if reap_confirmed:
                            await db.execute(
                                update(Instance)
                                .where(Instance.id == instance_id)
                                .values(
                                    status="idle",
                                    pid=None,
                                    process_identity=None,
                                    current_task_id=None,
                                )
                            )
                        else:
                            await db.execute(
                                update(Instance)
                                .where(Instance.id == instance_id)
                                .values(
                                    status="error",
                                    pid=(getattr(process, "pid", None) or None),
                                    process_identity=capture_process_identity(
                                        getattr(process, "pid", None) or None
                                    ),
                                    current_task_id=task_id,
                                )
                            )
                        await db.commit()
                except Exception:
                    logger.exception(
                        "Failed to rollback aborted PTY launch metadata for instance %s",
                        instance_id,
                    )

            cleanup = asyncio.create_task(_cleanup_failed_pty_launch())
            await await_task_completion(cleanup)
            cleanup.result()
            raise

    async def wait_for_pty_launch_metadata(self, instance_id: int) -> None:
        """Order PTY terminal cleanup after the initial running commit."""

        barrier = self._pty_launch_barriers.get(instance_id)
        if barrier is not None:
            await barrier.wait()

    @staticmethod
    def _is_pty_autonomous_terminal(event: dict) -> bool:
        """Whether an idle-watcher event is Claude's exact turn sentinel."""

        return (
            event.get("event_type") == "system_event"
            and event.get("content") == "turn_duration"
        )

    @classmethod
    def _is_pty_autonomous_activity(cls, event: dict) -> bool:
        """Ignore stale channel echoes and a standalone trailing sentinel."""

        if cls._is_pty_autonomous_terminal(event):
            return False
        if event.get("role") != "user":
            return True
        return "<task-notification>" in str(event.get("content") or "")

    def active_pty_background_task_ids(self) -> set[int]:
        """Return live PTY background Tasks for same-process reconciliation."""

        return {state.task_id for state in self._pty_background_states.values()}

    def project_share_runtime_block_reason(
        self,
        *,
        project_id: int,
        task_ids: set[int],
        instance_ids: set[int],
    ) -> str | None:
        """Synchronously snapshot runtime evidence that vetoes Project share.

        The caller already holds the durable Project -> Tasks -> Instances
        writer fence. This method has no await point, so an in-process launch
        reservation cannot appear halfway through the snapshot. A launch that
        starts immediately afterwards must cross the Project writer fence and
        will observe the newly committed share before its provider effect.
        """

        if (
            type(project_id) is not int
            or project_id <= 0
            or any(type(value) is not int or value <= 0 for value in task_ids)
            or any(
                type(value) is not int or value <= 0
                for value in instance_ids
            )
        ):
            return "Could not verify local Agent runtime; Project sharing is disabled"

        def related(instance_id: int, task_id: int | None = None) -> bool:
            return bool(
                instance_id in instance_ids
                or task_id in task_ids
                or self._container_tasks.get(instance_id) == project_id
            )

        for instance_id, reservation in self._launch_reservations.items():
            if related(instance_id, reservation.task_id):
                return (
                    "A local Agent launch is in progress for this Project; "
                    "wait for it to settle before sharing"
                )

        live_instance_ids = set(self.processes)
        live_instance_ids.update(self._process_groups)
        live_instance_ids.update(self._container_exec_processes)
        live_instance_ids.update(self._tasks)
        live_instance_ids.update(self._consumer_records)
        live_instance_ids.update(self._pty_launch_barriers)
        live_instance_ids.update(self._codex_exec_homes)
        live_instance_ids.update(self._stopping)
        for instance_id in live_instance_ids:
            record = self._consumer_records.get(instance_id)
            record_task_id = record.task_id if record is not None else None
            params = self._launch_params.get(instance_id) or {}
            params_task_id = params.get("task_id")
            if related(instance_id, record_task_id) or related(
                instance_id,
                params_task_id if type(params_task_id) is int else None,
            ):
                return (
                    "A local Agent runtime is still attached to this Project; "
                    "stop it before sharing"
                )

        for (instance_id, _process), evidence in (
            self._consumer_recovery_pending.items()
        ):
            if related(instance_id, evidence.task_id):
                return (
                    "A local Agent recovery is unresolved for this Project; "
                    "wait for recovery before sharing"
                )

        for state in self._pty_background_states.values():
            if state.task_id in task_ids:
                return (
                    "A local background Agent is still running for this Project; "
                    "wait for it to settle before sharing"
                )
        for proof in self._pty_post_exit_generations.values():
            if related(proof.instance_id, proof.task_id):
                return (
                    "A local Agent terminal handoff is unresolved for this "
                    "Project; wait before sharing"
                )
        for continuation in self._sequential_turn_continuations.values():
            if related(continuation.instance_id, continuation.task_id):
                return (
                    "A local Agent continuation remains admitted for this "
                    "Project; wait before sharing"
                )
        for task_id, _session_id in self._pty_autonomous_activity_handoffs:
            if task_id in task_ids:
                return (
                    "A local autonomous Agent handoff is unresolved for this "
                    "Project; wait before sharing"
                )
        for permission in self._pty_permissions.values():
            if permission.get("task_id") in task_ids:
                return (
                    "A local Agent permission request is still active for this "
                    "Project; resolve it before sharing"
                )
        return None

    def pty_background_generation_for(
        self,
        task_id: int,
        session_id: str,
    ) -> str | None:
        state = self._pty_background_states.get((task_id, session_id))
        return state.generation if state is not None else None

    def pty_background_state_for(
        self,
        task_id: int,
        session_id: str,
        generation: str,
    ) -> _PtyBackgroundState | None:
        """Return the exact in-memory epoch, never a same-key replacement."""

        state = self._pty_background_states.get((task_id, session_id))
        if state is None or state.generation != generation:
            return None
        return state

    def _pty_background_state_for_task(
        self,
        task_id: int,
    ) -> _PtyBackgroundState | None:
        """Resolve a unique retained state for exact-stop recovery only."""

        matches = [
            state
            for state in self._pty_background_states.values()
            if state.task_id == task_id
        ]
        return matches[0] if len(matches) == 1 else None

    def _has_reapable_pty_background_state(
        self,
        task_id: int | None,
    ) -> bool:
        if task_id is None:
            return False
        state = self._pty_background_state_for_task(task_id)
        return bool(
            state is not None
            and not state.accepting_events
            and getattr(state.session, "is_alive", True) is False
        )

    def retain_pty_post_exit_generation(
        self,
        instance_id: int,
        task_id: int,
        session_id: str,
        session: Any,
        record: _OutputConsumerRecord,
    ) -> _PtyPostExitGeneration | None:
        """Retain one exact PTY turn across proxy→Task handoff.

        Registration is deliberately synchronous and succeeds only while all
        ordinary instance-keyed maps still identify ``record``.  The retained
        proof is therefore immutable evidence of the generation that is about
        to disappear from those maps, not a late lookup of a reusable slot.
        """

        process = record.process
        if (
            record.provider != "claude"
            or record.task_id != task_id
            or record.task_retry_count is None
            or record.task_turn_generation is None
            or record.instance_started_at is None
            or record.pty_terminal_owner != "consumer"
            or record.task.done()
            or instance_id in self._stopping
            or getattr(session, "session_id", None) != session_id
            or getattr(session, "is_alive", True) is False
            or getattr(process, "session", None) is not session
            or self._consumer_records.get(instance_id) is not record
            or self._tasks.get(instance_id) is not record.task
            or self.processes.get(instance_id) is not process
        ):
            return None

        key = (task_id, session_id)
        current = self._pty_post_exit_generations.get(key)
        if (
            current is not None
            and current.instance_id == instance_id
            and current.session is session
            and current.process is process
            and current.record is record
        ):
            return current
        if current is not None:
            self._discard_pty_post_exit_generation(key, current)

        proof = _PtyPostExitGeneration(
            token=object(),
            instance_id=instance_id,
            task_id=task_id,
            session_id=session_id,
            session=session,
            process=process,
            record=record,
            created_monotonic=time.monotonic(),
        )
        self._pty_post_exit_generations[key] = proof
        proof.watcher = asyncio.create_task(
            self._watch_pty_post_exit_generation(proof)
        )
        return proof

    def _discard_pty_post_exit_generation(
        self,
        key: tuple[int, str],
        proof: _PtyPostExitGeneration,
    ) -> bool:
        """Discard only the exact retained proof, never a same-key replacement."""

        if self._pty_post_exit_generations.get(key) is not proof:
            return False
        proof.invalidated = True
        self._pty_post_exit_generations.pop(key, None)
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        for followup in tuple(self._pty_followup_tasks.get(proof.instance_id, ())):
            if (
                followup is not current_task
                and not followup.done()
                and getattr(followup, "_ccm_pty_post_exit_proof", None)
                is proof
            ):
                followup.cancel()
        watcher = proof.watcher
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        if watcher is not None and watcher is not current and not watcher.done():
            watcher.cancel()
        return True

    def discard_pty_post_exit_generations(
        self,
        *,
        task_id: int | None = None,
        session_id: str | None = None,
        instance_id: int | None = None,
        process: Any = None,
        record: _OutputConsumerRecord | None = None,
        invalidate_handoffs: bool = False,
    ) -> int:
        """Discard retained proofs matching an explicit immutable generation."""

        removed = 0
        for key, proof in list(self._pty_post_exit_generations.items()):
            if task_id is not None and proof.task_id != task_id:
                continue
            if session_id is not None and proof.session_id != session_id:
                continue
            if instance_id is not None and proof.instance_id != instance_id:
                continue
            if process is not None and proof.process is not process:
                continue
            if record is not None and proof.record is not record:
                continue
            discarded = self._discard_pty_post_exit_generation(key, proof)
            removed += int(discarded)
            if discarded and invalidate_handoffs:
                # Stop/replacement is an explicit invalidation, unlike natural
                # Task terminal commit. Make every already-waiting callback's
                # immutable handoff mismatch so it cannot fall through to the
                # completed-only detached admission path.
                self._pty_autonomous_activity_handoffs.pop(key, None)
        return removed

    def _pty_post_exit_generation_for_instance(
        self,
        instance_id: int,
        task_id: int | None = None,
    ) -> _PtyPostExitGeneration | None:
        matches = [
            proof
            for proof in self._pty_post_exit_generations.values()
            if (
                proof.instance_id == instance_id
                and (task_id is None or proof.task_id == task_id)
            )
        ]
        return matches[0] if len(matches) == 1 else None

    async def _watch_pty_post_exit_generation(
        self,
        proof: _PtyPostExitGeneration,
    ) -> None:
        """Remove a handoff proof as soon as its durable owner is no longer active."""

        key = (proof.task_id, proof.session_id)
        poll_delay = 0.05
        try:
            while self._pty_post_exit_generations.get(key) is proof:
                # Let the dispatcher/Ralph waiter that was awakened by
                # process.complete() reach its result CAS before polling.
                await asyncio.sleep(poll_delay)
                # The chat proof is a bounded handoff lease even while a
                # background-state row or an exact-stop operation remains
                # visible. A wedged stop/state watcher must not turn that
                # lease into an unbounded owner of a reusable PTY slot.
                if (
                    proof.record.chat_initiated
                    and time.monotonic() - proof.created_monotonic
                    >= PTY_POST_EXIT_CHAT_HARD_TTL_SECONDS
                ):
                    self._discard_pty_post_exit_generation(key, proof)
                    return
                if proof.instance_id in self._stopping:
                    # A successful exact stop invalidates both proof and
                    # handoff after its Task+Instance CAS. Do not let this
                    # observer retire only the proof in the middle of that
                    # operation and leave a waiting callback reusable.
                    continue
                state = self._pty_background_states.get(key)
                if state is not None:
                    # Non-chat proofs only bridge the autonomous callback
                    # race.  A chat proof is also the immutable record needed
                    # by a follow-up pump after the ordinary maps disappear,
                    # so keep it for the exact accepting epoch.  The epoch's
                    # terminal cleanup retires this proof.
                    if (
                        proof.record.chat_initiated
                        and self._pty_post_exit_generation_is_current(
                            proof,
                            require_background_state=True,
                            background_generation=state.generation,
                        )
                    ):
                        poll_delay = min(1.0, poll_delay * 2)
                        continue
                    self._discard_pty_post_exit_generation(key, proof)
                    return
                if getattr(proof.session, "is_alive", True) is False:
                    self._discard_pty_post_exit_generation(key, proof)
                    return
                if not self._pty_post_exit_generation_is_current(proof):
                    self._discard_pty_post_exit_generation(key, proof)
                    return
                try:
                    async with self.db_factory() as db:
                        task = await db.get(Task, proof.task_id)
                        owner = await db.get(Instance, proof.instance_id)
                        still_exact = bool(
                            task is not None
                            and owner is not None
                            and task.status in ("in_progress", "executing")
                            and task.worker_id is None
                            and task.shared_from_id is None
                            and task.instance_id == proof.instance_id
                            and task.session_id == proof.session_id
                            and task.retry_count
                            == proof.record.task_retry_count
                            and task.turn_generation
                            == proof.record.task_turn_generation
                            and owner.current_task_id == proof.task_id
                            and owner.pid
                            == getattr(proof.process, "pid", None)
                            and owner.started_at
                            == proof.record.instance_started_at
                        )
                        terminal_exact = bool(
                            proof.record.chat_initiated
                            and task is not None
                            and owner is not None
                            and task.status == "completed"
                            and task.completed_at is not None
                            and task.worker_id is None
                            and task.shared_from_id is None
                            and task.instance_id == proof.instance_id
                            and task.session_id == proof.session_id
                            and task.retry_count
                            == proof.record.task_retry_count
                            and task.turn_generation
                            == proof.record.task_turn_generation
                            and task.pty_background_generation is None
                            and owner.status == "idle"
                            and owner.current_task_id is None
                            and owner.pid is None
                            and owner.started_at
                            == proof.record.instance_started_at
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # A transient database failure is not evidence that the
                    # generation changed. Keep the proof fail-closed and retry.
                    logger.exception(
                        "Failed to verify PTY post-exit proof for task %s",
                        proof.task_id,
                    )
                    poll_delay = min(1.0, poll_delay * 2)
                    continue
                if not still_exact and terminal_exact:
                    # A late native child may be registered just after the
                    # foreground terminal commit.  Keep the chat proof during
                    # a short handoff grace, and longer while durable/native
                    # background work is demonstrably still running.  The
                    # native/durable pending probe is not an unbounded lease:
                    # if it wedges, retire the ownerless proof at the hard TTL.
                    proof_age = time.monotonic() - proof.created_monotonic
                    if proof_age < PTY_POST_EXIT_CHAT_HARD_TTL_SECONDS:
                        pending_activity = (
                            proof_age < PTY_POST_EXIT_CHAT_GRACE_SECONDS
                        )
                        if not pending_activity:
                            pending_activity = (
                                await self.pty_background_activity_pending(
                                    proof.task_id,
                                    proof.session,
                                )
                            )
                        if pending_activity:
                            poll_delay = min(1.0, poll_delay * 2)
                            continue
                if not still_exact:
                    if any(
                        owner_proof is proof
                        for owner_proof in (
                            self
                            ._pty_autonomous_activity_post_exit_owners
                            .values()
                        )
                    ):
                        # A callback captured this proof before the durable
                        # terminal CAS and is still waiting to enter the
                        # transition lock. Keep the immutable bridge until that
                        # exact coroutine either admits via completed-only state
                        # or its done callback releases ownership.
                        continue
                    self._discard_pty_post_exit_generation(key, proof)
                    return
                poll_delay = min(1.0, poll_delay * 2)
        except asyncio.CancelledError:
            return

    def note_pty_autonomous_activity(
        self,
        task_id: int,
        session_id: str,
    ) -> object:
        key = (task_id, session_id)
        token = self._pty_autonomous_activity_handoffs.get(key)
        if token is None:
            token = object()
            self._pty_autonomous_activity_handoffs[key] = token
        try:
            owner = asyncio.current_task()
        except RuntimeError:
            owner = None
        if owner is not None:
            owner_key = (owner, key)
            self._pty_autonomous_activity_handoff_owners[owner_key] = token
            proof = self._pty_post_exit_generations.get(key)
            if proof is not None:
                self._pty_autonomous_activity_post_exit_owners[
                    owner_key
                ] = proof
            else:
                self._pty_autonomous_activity_post_exit_owners.pop(
                    owner_key, None
                )
            if (
                owner
                not in self._pty_autonomous_activity_handoff_owner_callbacks
            ):
                self._pty_autonomous_activity_handoff_owner_callbacks.add(
                    owner
                )
                owner.add_done_callback(
                    self._forget_pty_autonomous_activity_handoff_owner
                )
        return token

    def _forget_pty_autonomous_activity_handoff_owner(
        self,
        owner: asyncio.Task[Any],
    ) -> None:
        self._pty_autonomous_activity_handoff_owner_callbacks.discard(owner)
        for owner_key in [
            owner_key
            for owner_key in self._pty_autonomous_activity_handoff_owners
            if owner_key[0] is owner
        ]:
            self._pty_autonomous_activity_handoff_owners.pop(
                owner_key, None
            )
            self._pty_autonomous_activity_post_exit_owners.pop(
                owner_key, None
            )

    def _owned_pty_autonomous_activity_handoff(
        self,
        key: tuple[int, str],
    ) -> object | None:
        try:
            owner = asyncio.current_task()
        except RuntimeError:
            return None
        if owner is None:
            return None
        return self._pty_autonomous_activity_handoff_owners.get(
            (owner, key)
        )

    def _owned_pty_post_exit_generation(
        self,
        key: tuple[int, str],
    ) -> _PtyPostExitGeneration | None:
        try:
            owner = asyncio.current_task()
        except RuntimeError:
            return None
        if owner is None:
            return None
        return self._pty_autonomous_activity_post_exit_owners.get(
            (owner, key)
        )

    def has_pty_autonomous_activity_handoff(
        self,
        task_id: int,
        session_id: str,
    ) -> bool:
        return (task_id, session_id) in (
            self._pty_autonomous_activity_handoffs
        )

    def clear_pty_autonomous_activity_handoff(
        self,
        task_id: int,
        session_id: str,
        token: object | None,
    ) -> None:
        if token is None:
            return
        key = (task_id, session_id)
        try:
            owner = asyncio.current_task()
        except RuntimeError:
            owner = None
        if owner is not None:
            owner_key = (owner, key)
            if (
                self._pty_autonomous_activity_handoff_owners.get(owner_key)
                is token
            ):
                self._pty_autonomous_activity_handoff_owners.pop(
                    owner_key, None
                )
                self._pty_autonomous_activity_post_exit_owners.pop(
                    owner_key, None
                )
        if self._pty_autonomous_activity_handoffs.get(key) is token:
            self._pty_autonomous_activity_handoffs.pop(key, None)

    def reset_pty_autonomous_activity_handoff(
        self,
        task_id: int,
        session_id: str,
    ) -> None:
        key = (task_id, session_id)
        self._pty_autonomous_activity_handoffs.pop(key, None)
        proof = self._pty_post_exit_generations.get(key)
        if proof is not None:
            self._discard_pty_post_exit_generation(key, proof)

    def _restore_pty_background_after_failed_stop(
        self,
        state: _PtyBackgroundState,
        handoff: object | None,
        session: Any,
        *,
        instance_id: int | None = None,
        process: Any = None,
    ) -> bool:
        """Re-open only the exact still-live epoch after an unproven stop.

        A backend exception does not prove that the native Session survived.
        Restoration is therefore deliberately identity- and liveness-fenced:
        a stopped Session, a replacement state/handoff, or a reaped active
        process remains frozen so stale output cannot be admitted.
        """

        key = (state.task_id, state.session_id)
        if (
            self._pty_background_states.get(key) is not state
            or state.session is not session
            or getattr(session, "session_id", None) != state.session_id
            or self._pty_autonomous_activity_handoffs.get(key) is not handoff
            or getattr(session, "is_alive", True) is False
            or (
                instance_id is not None
                and process is not None
                and self._generation_reap_confirmed(instance_id, process)
            )
        ):
            return False

        state.outcome = None
        state.accepting_events = True
        state.done.clear()
        watcher = state.watcher
        if (
            watcher is None
            or watcher.done()
            or (
                hasattr(watcher, "cancelling")
                and watcher.cancelling()
            )
        ):
            state.watcher = asyncio.create_task(
                self._watch_pty_background_generation(state)
            )
        return True

    @asynccontextmanager
    async def pty_background_transition(
        self,
        task_id: int,
        session_id: str,
    ):
        """Serialize foreground on_exit with idle-watcher pre-arm."""

        key = (task_id, session_id)
        lock = self._pty_background_transition_locks.setdefault(
            key, asyncio.Lock()
        )
        async with lock:
            yield

    async def pty_background_activity_pending(
        self,
        task_id: int,
        session: Any,
    ) -> bool:
        """Check live native/Bash work and its durable native-agent mirror."""

        # A retained Session can accept one user follow-up while the original
        # root consumer waits for its native descendants.  That follow-up is
        # not represented by the autonomous tracker, but its event pump still
        # owns the exact Task/session generation and must finish before the
        # background marker may be cleared.
        for instance_id, followups in self._pty_followup_tasks.items():
            if not any(not task.done() for task in followups):
                continue
            record = self._consumer_records.get(instance_id)
            process = getattr(record, "process", None)
            if (
                record is not None
                and record.task_id == task_id
                and getattr(process, "session", None) is session
            ):
                return True

        ccm_tracker = getattr(
            session, "_ccm_background_work_tracker", None
        )
        if (
            getattr(
                ccm_tracker,
                "has_pending_background_commands",
                False,
            )
            is True
        ):
            return True
        if getattr(session, "has_pending_subagents", False) is True:
            return True
        from backend.models.sub_agent import SubAgentSession

        async with self.db_factory() as db:
            result = await db.execute(
                select(SubAgentSession.id)
                .where(
                    SubAgentSession.task_id == task_id,
                    SubAgentSession.source == "native",
                    SubAgentSession.status == "running",
                )
                .limit(1)
            )
            return result.scalar_one_or_none() is not None

    async def arm_pty_background_generation(
        self,
        instance_id: int,
        task_id: int,
        session_id: str,
        generation: str,
        record: _OutputConsumerRecord,
        *,
        post_exit_proof: _PtyPostExitGeneration | None = None,
    ) -> bool:
        """Persist background activity for a dispatcher-owned foreground turn.

        The foreground proxy stays pending until this marker clears, so no
        chat/initial/goal/loop terminal consumer can observe partial output.
        """

        if (
            record.task_id != task_id
            or record.task_retry_count is None
            or record.task_turn_generation is None
            or record.instance_started_at is None
        ):
            return False
        if post_exit_proof is not None:
            key = (task_id, session_id)
            if (
                self._pty_post_exit_generations.get(key)
                is not post_exit_proof
                or post_exit_proof.instance_id != instance_id
                or post_exit_proof.task_id != task_id
                or post_exit_proof.session_id != session_id
                or post_exit_proof.record is not record
                or post_exit_proof.process is not record.process
                or getattr(post_exit_proof.session, "session_id", None)
                != session_id
                or getattr(post_exit_proof.session, "is_alive", True)
                is False
                or instance_id in self._stopping
            ):
                return False
        state = self._pty_background_states.get((task_id, session_id))
        marker_predicate = (
            Task.pty_background_generation == generation
            if state is not None and state.generation == generation
            else Task.pty_background_generation.is_(None)
        )
        async with self.db_factory() as db:
            if not await _fence_worker_runtime_admission(
                db,
                producer="PTY background-generation admission",
            ):
                return False
            armed = await db.execute(
                update(Task)
                .where(
                    Task.id == task_id,
                    Task.instance_id == instance_id,
                    Task.session_id == session_id,
                    Task.retry_count == record.task_retry_count,
                    Task.turn_generation == record.task_turn_generation,
                    Task.status.in_(("in_progress", "executing")),
                    marker_predicate,
                    task_retry_not_superseded_predicate(),
                    no_active_worker_task_termination_predicate(),
                )
                .values(pty_background_generation=generation)
            )
            if not armed.rowcount:
                await db.rollback()
                return False
            if post_exit_proof is not None:
                # The Task row is locked first, preserving the global
                # Task→Instance lock order. This no-op owner CAS proves the
                # retained turn still owns the same reusable slot even though
                # its ordinary in-memory maps have already been released.
                owner_guard = await db.execute(
                    update(Instance)
                    .where(
                        Instance.id == instance_id,
                        Instance.current_task_id == task_id,
                        Instance.pid
                        == getattr(post_exit_proof.process, "pid", None),
                        Instance.started_at
                        == record.instance_started_at,
                    )
                    .values(status=Instance.status)
                )
                if (
                    not owner_guard.rowcount
                    or self._pty_post_exit_generations.get(
                        (task_id, session_id)
                    )
                    is not post_exit_proof
                    or instance_id in self._stopping
                ):
                    await db.rollback()
                    return False
            await db.commit()
        payload = {
            "event": "background_activity",
            "event_type": "background_activity",
            "task_id": task_id,
            "task_retry_count": record.task_retry_count,
            "task_turn_generation": record.task_turn_generation,
            "background_active": True,
        }
        await self.broadcaster.broadcast("tasks", payload)
        await self.broadcaster.broadcast(f"task:{task_id}", payload)
        return True

    def register_pty_background_generation(
        self,
        task_id: int,
        session_id: str,
        generation: str,
        session: Any,
        *,
        task_retry_count: int,
        task_turn_generation: int,
    ) -> _PtyBackgroundState:
        """Track an already-persisted foreground→background transition."""

        key = (task_id, session_id)
        current = self._pty_background_states.get(key)
        if current is not None and current.generation == generation:
            if (
                current.task_retry_count != task_retry_count
                or current.task_turn_generation != task_turn_generation
            ):
                raise RuntimeError(
                    "PTY background generation turn identity changed"
                )
            current.session = session
            current.last_event_monotonic = time.monotonic()
            return current
        if current is not None:
            current.outcome = "superseded"
            self._discard_pty_background_state(key, current.generation)
        now = time.monotonic()
        state = _PtyBackgroundState(
            task_id=task_id,
            session_id=session_id,
            generation=generation,
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
            session=session,
            started_monotonic=now,
            last_event_monotonic=now,
        )
        self._pty_background_states[key] = state
        state.watcher = asyncio.create_task(
            self._watch_pty_background_generation(state)
        )
        return state

    def _discard_pty_background_state(
        self,
        key: tuple[int, str],
        generation: str,
    ) -> None:
        state = self._pty_background_states.get(key)
        if state is None or state.generation != generation:
            return
        self._pty_background_states.pop(key, None)
        state.accepting_events = False
        state.done.set()
        # A chat post-exit proof is the event-consumer identity for this
        # detached epoch.  Retire only the proof that belongs to this exact
        # Session/retry/turn; a same-key replacement must remain untouched.
        proof = self._pty_post_exit_generations.get(key)
        if (
            proof is not None
            and proof.session is state.session
            and proof.record.task_retry_count == state.task_retry_count
            and proof.record.task_turn_generation
            == state.task_turn_generation
        ):
            self._discard_pty_post_exit_generation(key, proof)
        watcher = state.watcher
        if (
            watcher is not None
            and watcher is not asyncio.current_task()
            and not watcher.done()
            and not state.watchdog_stopping
        ):
            watcher.cancel()

    async def wait_pty_background_generation(
        self,
        task_id: int,
        session_id: str,
        generation: str,
    ) -> str | None:
        """Wait for one exact dispatcher-owned PTY epoch to settle.

        Chat, initial, goal, and loop turns must not wake their lifecycle
        consumer while a native Agent/Monitor is still producing the result.
        """

        state = self.pty_background_state_for(
            task_id, session_id, generation
        )
        if state is None:
            return None
        return await self.wait_pty_background_outcome(state)

    @staticmethod
    async def wait_pty_background_outcome(
        state: _PtyBackgroundState,
    ) -> str:
        """Wait through dictionary removal using an exact state reference."""

        await state.done.wait()
        return state.outcome or "superseded"

    def abandon_pty_background_generation(
        self,
        task_id: int,
        session_id: str,
        generation: str,
        *,
        outcome: str = "abandoned",
    ) -> None:
        """Wake an owner being stopped while retaining its exact Session.

        PTY backend stop may fail after the foreground consumer is unblocked.
        The state therefore stays indexed until the exact process stop and
        Task/Instance transaction both succeed, allowing a later stop retry to
        address the same Session instead of stranding a durable marker.
        """

        state = self._pty_background_states.get((task_id, session_id))
        if state is None or state.generation != generation:
            return
        state.outcome = outcome
        state.accepting_events = False
        state.done.set()
        watcher = state.watcher
        if (
            watcher is not None
            and watcher is not asyncio.current_task()
            and not watcher.done()
            and not state.watchdog_stopping
        ):
            watcher.cancel()

    async def _stop_exact_unattached_pty_session(
        self,
        session: Any,
        session_id: str,
        task_id: int,
    ) -> bool:
        """Stop one Session object without ever addressing a reusable key."""

        if getattr(session, "session_id", None) != session_id:
            return False
        attached = getattr(self._pty_backend, "_sessions", {})
        if isinstance(attached, dict) and any(
            candidate is session for candidate in attached.values()
        ):
            return False

        try:
            pool = getattr(self._pty_backend, "_pool", None)
            pool_sessions = getattr(pool, "_sessions", None)
            pool_lock = getattr(pool, "_lock", None)
            if isinstance(pool_sessions, dict) and pool_lock is not None:
                # Keep replacement of this session-id serialized with the
                # exact stop. Pop only by object identity: an ABA replacement
                # must remain alive.
                async with pool_lock:
                    await session.stop()
                    if pool_sessions.get(session_id) is session:
                        pool_sessions.pop(session_id, None)
                        access_order = getattr(pool, "_access_order", None)
                        if isinstance(access_order, dict):
                            access_order.pop(session_id, None)
            else:
                await session.stop()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Could not stop exact detached PTY session %s for task %s",
                session_id,
                task_id,
            )
            return False
        # Absence/unknown is not proof that an exact native Session stopped.
        stopped = getattr(session, "is_alive", True) is False
        if stopped:
            self._release_task_runtime_scope_pty_owner(session)
        return stopped

    async def _stop_exact_detached_pty_session(
        self,
        state: _PtyBackgroundState,
    ) -> bool:
        return await self._stop_exact_unattached_pty_session(
            state.session,
            state.session_id,
            state.task_id,
        )

    async def _stop_exact_post_exit_pty_session(
        self,
        proof: _PtyPostExitGeneration,
    ) -> bool:
        key = (proof.task_id, proof.session_id)
        if self._pty_post_exit_generations.get(key) is not proof:
            return False
        return await self._stop_exact_unattached_pty_session(
            proof.session,
            proof.session_id,
            proof.task_id,
        )

    async def stop_detached_pty_background_generation(
        self,
        task_id: int,
        session_id: str,
        generation: str,
        *,
        expected_status: str,
        expected_retry_count: int,
        expected_turn_generation: int,
        expected_instance_id: int | None,
        expected_started_at: datetime | None,
        expected_completed_at: datetime | None,
        terminal_status: str | None = None,
        error_message: str | None = None,
        yield_to_worker_task_termination: bool = True,
        worker_termination_operation_id: str | None = None,
        worker_termination_operation: str | None = None,
        worker_termination_execution_token: str | None = None,
        worker_termination_state_version: int | None = None,
    ) -> bool:
        """Stop one ownerless PTY tail without addressing a reusable slot.

        A genuinely late autonomous turn can begin after the foreground
        Instance has already become idle (and may already belong to another
        Task).  The durable marker is Task/session scoped, so cleanup must stop
        the exact ``Session`` object retained by ``_PtyBackgroundState``.  It
        must never call the adapter's instance-keyed ``stop()``.

        The marker is cleared only after that exact session is proven stopped.
        Any missing state, surviving process, pool race, or DB generation
        mismatch therefore remains fail-closed and can be retried.
        """

        key = (task_id, session_id)
        async with self.pty_background_transition(task_id, session_id):
            state = self._pty_background_states.get(key)

            identity_predicates = [
                Task.id == task_id,
                Task.worker_id.is_(None),
                Task.shared_from_id.is_(None),
                Task.status == expected_status,
                Task.retry_count == expected_retry_count,
                Task.turn_generation == expected_turn_generation,
                Task.session_id == session_id,
                (
                    Task.instance_id.is_(None)
                    if expected_instance_id is None
                    else Task.instance_id == expected_instance_id
                ),
                (
                    Task.started_at.is_(None)
                    if expected_started_at is None
                    else Task.started_at == expected_started_at
                ),
            ]
            receipt_identity = (
                worker_termination_operation_id,
                worker_termination_operation,
                worker_termination_execution_token,
                worker_termination_state_version,
            )
            if (
                yield_to_worker_task_termination
                != (worker_termination_operation_id is None)
                or (
                    worker_termination_operation_id is None
                    and any(value is not None for value in receipt_identity[1:])
                )
                or (
                    worker_termination_operation_id is not None
                    and (
                        worker_termination_operation
                        not in {"cancel", "stop_session", "supersede"}
                        or worker_termination_execution_token is None
                        or worker_termination_state_version is None
                    )
                )
            ):
                # A caller may not disable durable arbitration anonymously.
                # Receipt-owned cleanup must name the exact active Worker
                # operation that authorizes the bypass.
                return False
            async def lock_detached_authority(
                db: AsyncSession,
                *extra_task_predicates: Any,
            ) -> datetime | None:
                task_lock = await db.execute(
                    update(Task)
                    .where(
                        *identity_predicates,
                        *extra_task_predicates,
                    )
                    .values(status=Task.status)
                    .execution_options(synchronize_session=False)
                )
                if task_lock.rowcount != 1:
                    await db.rollback()
                    return None
                receipt = await active_worker_task_termination_receipt(
                    db,
                    task_id,
                    for_update=True,
                )
                owner = await db.scalar(
                    select(Instance.id)
                    .where(Instance.current_task_id == task_id)
                    .with_for_update()
                )
                lease_valid_at = datetime.utcnow()
                if owner is not None or not (
                    worker_task_termination_authority_matches(
                        receipt,
                        operation_id=worker_termination_operation_id,
                        operation=worker_termination_operation,
                        execution_token=worker_termination_execution_token,
                        state_version=worker_termination_state_version,
                        lease_valid_at=lease_valid_at,
                    )
                ):
                    await db.rollback()
                    return None
                return lease_valid_at

            # Natural background completion can win just before this lock.
            # A cleared marker on the same Task/session/retry is already the
            # desired result even if it refreshed completed_at.
            async with self.db_factory() as db:
                lease_valid_at = await lock_detached_authority(db)
                if lease_valid_at is None:
                    return False
                durable_background = (
                    await db.execute(
                        select(
                            Task.pty_background_generation,
                            Task.completed_at,
                        ).where(Task.id == task_id)
                    )
                ).one()
                if durable_background.pty_background_generation is None:
                    await db.rollback()
                    return True
                if (
                    durable_background.pty_background_generation != generation
                    or durable_background.completed_at != expected_completed_at
                ):
                    await db.rollback()
                    return False
                guarded = await db.execute(
                    update(Task)
                    .where(
                        *identity_predicates,
                        (
                            Task.completed_at.is_(None)
                            if expected_completed_at is None
                            else Task.completed_at == expected_completed_at
                        ),
                        Task.pty_background_generation == generation,
                        *_worker_termination_stop_predicates(
                            worker_termination_operation_id,
                            worker_termination_operation,
                            worker_termination_execution_token,
                            worker_termination_state_version,
                            lease_valid_at,
                        ),
                    )
                    .values(status=expected_status)
                )
                if not guarded.rowcount:
                    await db.rollback()
                    return False
                await db.commit()

            if (
                state is None
                or state.generation != generation
                or state.task_retry_count != expected_retry_count
                or state.task_turn_generation != expected_turn_generation
            ):
                return False
            handoff = self._pty_autonomous_activity_handoffs.get(key)
            exact_session = state.session
            async with self.db_factory() as effect_db:
                effect_lease_valid_at = await lock_detached_authority(
                    effect_db,
                    (
                        Task.completed_at.is_(None)
                        if expected_completed_at is None
                        else Task.completed_at == expected_completed_at
                    ),
                    Task.pty_background_generation == generation,
                )
                await effect_db.rollback()
            if effect_lease_valid_at is None:
                return False
            self.abandon_pty_background_generation(
                task_id, session_id, generation
            )
            try:
                session_stopped = (
                    await self._stop_exact_detached_pty_session(state)
                )
            except BaseException:
                self._restore_pty_background_after_failed_stop(
                    state,
                    handoff,
                    exact_session,
                )
                raise
            if not session_stopped:
                self._restore_pty_background_after_failed_stop(
                    state,
                    handoff,
                    exact_session,
                )
                return False

            completed_at = datetime.utcnow()
            async with self.db_factory() as db:
                lease_valid_at = await lock_detached_authority(
                    db,
                    (
                        Task.completed_at.is_(None)
                        if expected_completed_at is None
                        else Task.completed_at == expected_completed_at
                    ),
                    Task.pty_background_generation == generation,
                )
                if lease_valid_at is None:
                    return False
                task_values: dict[str, Any] = {
                    "pty_background_generation": None
                }
                if terminal_status is not None:
                    task_values.update(
                        status=terminal_status,
                        completed_at=completed_at,
                        error_message=(
                            (error_message or "")[:2000] or None
                        ),
                    )
                cleared = await db.execute(
                    update(Task)
                    .where(
                        *identity_predicates,
                        (
                            Task.completed_at.is_(None)
                            if expected_completed_at is None
                            else Task.completed_at == expected_completed_at
                        ),
                        Task.pty_background_generation == generation,
                        *_worker_termination_stop_predicates(
                            worker_termination_operation_id,
                            worker_termination_operation,
                            worker_termination_execution_token,
                            worker_termination_state_version,
                            lease_valid_at,
                        ),
                    )
                    .values(**task_values)
                )
                if not cleared.rowcount:
                    await db.rollback()
                    return False
                from backend.models.sub_agent import SubAgentSession

                await db.execute(
                    update(SubAgentSession)
                    .where(
                        SubAgentSession.task_id == task_id,
                        SubAgentSession.source == "native",
                        SubAgentSession.status == "running",
                    )
                    .values(
                        status=(
                            "failed"
                            if terminal_status == "failed"
                            else "cancelled"
                        ),
                        completed_at=completed_at,
                    )
                )
                commit_valid_at = datetime.utcnow()
                commit_guard = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task_id,
                        *_worker_termination_stop_predicates(
                            worker_termination_operation_id,
                            worker_termination_operation,
                            worker_termination_execution_token,
                            worker_termination_state_version,
                            commit_valid_at,
                        ),
                    )
                    .values(status=Task.status)
                    .execution_options(synchronize_session=False)
                )
                if commit_guard.rowcount != 1:
                    await db.rollback()
                    return False
                await db.commit()

            state.outcome = (
                "failed" if terminal_status == "failed" else "abandoned"
            )
            self.clear_pty_autonomous_activity_handoff(
                task_id,
                session_id,
                self._pty_autonomous_activity_handoffs.get(key),
            )
            self._discard_pty_background_state(key, generation)
            return True

    async def begin_pty_autonomous_activity(
        self,
        task_id: int,
        session_id: str,
        session: Any,
        event: dict,
        *,
        instance_id: int | None = None,
    ) -> str | None:
        async with self.pty_background_transition(task_id, session_id):
            return await self._begin_pty_autonomous_activity_locked(
                task_id,
                session_id,
                session,
                event,
                instance_id=instance_id,
            )

    async def _begin_pty_autonomous_activity_locked(
        self,
        task_id: int,
        session_id: str,
        session: Any,
        event: dict,
        *,
        instance_id: int | None = None,
    ) -> str | None:
        """Move a completed Task into a detached, token-fenced background epoch.

        This method never reads or writes an Instance row.  The instance key
        supplied by claude-pty is a reusable adapter slot and may already host
        an unrelated Task by the time an idle-watcher callback arrives.
        """

        key = (task_id, session_id)
        state = self._pty_background_states.get(key)
        resolved_task_retry_count = (
            state.task_retry_count if state is not None else None
        )
        resolved_task_turn_generation = (
            state.task_turn_generation if state is not None else None
        )
        owned_handoff = self._owned_pty_autonomous_activity_handoff(key)
        owned_post_exit_proof = self._owned_pty_post_exit_generation(key)
        if (
            owned_handoff is not None
            and self._pty_autonomous_activity_handoffs.get(key)
            is not owned_handoff
        ):
            # This exact callback announced activity before it blocked on the
            # transition lock.  A successful exact stop invalidated that
            # immutable announcement while we were waiting; never let a new
            # same-key handoff (or the absence of one) resurrect it.
            return None
        if (
            owned_handoff is not None
            and (
                getattr(session, "session_id", None) != session_id
                or getattr(session, "is_alive", True) is False
            )
        ):
            return None
        if (
            owned_post_exit_proof is not None
            and self._pty_post_exit_generations.get(key)
            is not owned_post_exit_proof
        ):
            # Natural terminal reconciliation may retire the short-lived proof
            # before this already-announced callback enters the lock. It may
            # still use the completed-only Task/session CAS below. Explicit
            # stop/replacement also invalidates the handoff token itself and was
            # rejected above, so no stale callback can borrow that fallback.
            owned_post_exit_proof = None
        if (
            owned_post_exit_proof is not None
            and owned_post_exit_proof.instance_id in self._stopping
        ):
            return None
        if state is not None and (
            state.session is not session or not state.accepting_events
        ):
            return None
        if self._is_pty_autonomous_terminal(event):
            return (
                state.generation
                if state is not None and state.accepting_events
                else None
            )
        if not self._is_pty_autonomous_activity(event):
            return None

        transitioned = False
        generation: str | None = None
        # The idle watcher can win the small window between send_prompt
        # releasing its reader lock and on_exit arming the durable marker.
        # A callback installed at launch may pre-arm only while the exact
        # foreground consumer record still proves this Task/session owner.
        if state is None and instance_id is not None:
            record = self._consumer_records.get(instance_id)
            process = getattr(record, "process", None)
            if (
                record is not None
                and record.task_id == task_id
                and getattr(process, "session", None) is session
                and self.processes.get(instance_id) is process
                and instance_id not in self._stopping
                and record.pty_terminal_owner != "stop"
            ):
                candidate = secrets.token_urlsafe(24)
                if await self.arm_pty_background_generation(
                    instance_id,
                    task_id,
                    session_id,
                    candidate,
                    record,
                ):
                    self.register_pty_background_generation(
                        task_id,
                        session_id,
                        candidate,
                        session,
                        task_retry_count=record.task_retry_count,
                        task_turn_generation=record.task_turn_generation,
                    )
                    state = self._pty_background_states.get(key)
                    generation = candidate

        # ``process.complete()`` wakes dispatcher/Ralph before they commit the
        # Task result. Their exact output maps are already gone by then, so the
        # ordinary pre-arm branch above cannot prove ownership. A callback that
        # synchronously captured this immutable retained record may bridge only
        # that exact gap; no arbitrary executing Task is admitted.
        if (
            state is None
            and owned_post_exit_proof is not None
            and instance_id == owned_post_exit_proof.instance_id
            and owned_post_exit_proof.session is session
            and owned_post_exit_proof.record.task_id == task_id
            and owned_post_exit_proof.record.task_retry_count is not None
            and owned_post_exit_proof.record.task_turn_generation is not None
            and getattr(owned_post_exit_proof.process, "session", None)
            is session
            and instance_id not in self._stopping
        ):
            candidate = secrets.token_urlsafe(24)
            if await self.arm_pty_background_generation(
                instance_id,
                task_id,
                session_id,
                candidate,
                owned_post_exit_proof.record,
                post_exit_proof=owned_post_exit_proof,
            ):
                self.register_pty_background_generation(
                    task_id,
                    session_id,
                    candidate,
                    session,
                    task_retry_count=(
                        owned_post_exit_proof.record.task_retry_count
                    ),
                    task_turn_generation=(
                        owned_post_exit_proof.record.task_turn_generation
                    ),
                )
                state = self._pty_background_states.get(key)
                generation = candidate
                if not owned_post_exit_proof.record.chat_initiated:
                    self._discard_pty_post_exit_generation(
                        key, owned_post_exit_proof
                    )

        async with self.db_factory() as db:
            fence = (
                _fence_worker_runtime_mutation
                if state is not None
                else _fence_worker_runtime_admission
            )
            if not await fence(db, producer="PTY autonomous-activity admission"):
                return None
            # Dispatcher-owned initial/goal/loop turns pre-arm the exact epoch
            # while their Task is still executing.  Reuse that durable token
            # before applying the completed-only late-autonomous admission rule.
            if state is not None:
                existing_guard = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task_id,
                        Task.session_id == session_id,
                        Task.status.in_(
                            ("in_progress", "executing", "completed")
                        ),
                        Task.retry_count == state.task_retry_count,
                        Task.turn_generation == state.task_turn_generation,
                        Task.pty_background_generation == state.generation,
                        task_retry_not_superseded_predicate(),
                        no_active_worker_task_termination_predicate(),
                    )
                    .values(status=Task.status)
                )
                if existing_guard.rowcount:
                    task = await db.get(
                        Task, task_id, populate_existing=True
                    )
                    if task is not None and not has_pending_worker_routing(task):
                        generation = state.generation
                        resolved_task_retry_count = task.retry_count
                        resolved_task_turn_generation = task.turn_generation
                        await db.commit()
                    else:
                        await db.rollback()
                        return None
                else:
                    await db.rollback()
                    return None
            else:
                task_guard = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task_id,
                        Task.session_id == session_id,
                        Task.status == "completed",
                        task_retry_not_superseded_predicate(),
                        no_active_worker_task_termination_predicate(),
                    )
                    .values(status=Task.status)
                )
                if not task_guard.rowcount:
                    await db.rollback()
                    return None
                task = await db.get(Task, task_id, populate_existing=True)
                if task is None or has_pending_worker_routing(task):
                    await db.rollback()
                    return None
                resolved_task_turn_generation = task.turn_generation
                resolved_task_retry_count = task.retry_count
                generation = task.pty_background_generation
                if not generation:
                    generation = secrets.token_urlsafe(24)
                    task.pty_background_generation = generation
                    transitioned = True
                await db.commit()

        if generation is None:
            return None
        if (
            resolved_task_retry_count is None
            or resolved_task_turn_generation is None
        ):
            return None
        if (
            owned_post_exit_proof is not None
            and not owned_post_exit_proof.record.chat_initiated
        ):
            self._discard_pty_post_exit_generation(
                key, owned_post_exit_proof
            )
        self.register_pty_background_generation(
            task_id,
            session_id,
            generation,
            session,
            task_retry_count=resolved_task_retry_count,
            task_turn_generation=resolved_task_turn_generation,
        )
        state = self._pty_background_states.get(key)
        if state is not None and state.generation == generation:
            state.last_event_monotonic = time.monotonic()
            # A generation may span several harness autonomous turns while
            # sibling native agents remain alive.  The first activity after a
            # prior sentinel starts a new turn; never let that old sentinel
            # authorize completion of the new turn.
            state.terminal_seen = False
            event_type = str(event.get("event_type") or "")
            if event_type == "tool_use":
                state.pending_tools += 1
            elif event_type == "tool_result" and state.pending_tools:
                state.pending_tools -= 1
        if transitioned:
            payload = {
                "event": "background_activity",
                "event_type": "background_activity",
                "task_id": task_id,
                "task_retry_count": state.task_retry_count,
                "task_turn_generation": state.task_turn_generation,
                "background_active": True,
            }
            await self.broadcaster.broadcast("tasks", payload)
            await self.broadcaster.broadcast(f"task:{task_id}", payload)
        return generation

    async def finish_pty_autonomous_activity(
        self,
        task_id: int,
        session_id: str,
        generation: str | None,
        event: dict,
    ) -> None:
        """Complete a background epoch only at its exact turn sentinel."""

        completion_state = None
        async with self.pty_background_transition(task_id, session_id):
            completion_state = await self._finish_pty_autonomous_activity_locked(
                task_id,
                session_id,
                generation,
                event,
            )
        if completion_state is not None:
            await self._try_complete_pty_background_generation(
                completion_state
            )

    async def _finish_pty_autonomous_activity_locked(
        self,
        task_id: int,
        session_id: str,
        generation: str | None,
        event: dict,
    ) -> _PtyBackgroundState | None:
        if generation is None:
            return None
        state = self._pty_background_states.get((task_id, session_id))
        if state is None or state.generation != generation:
            return None
        state.last_event_monotonic = time.monotonic()
        if not self._is_pty_autonomous_terminal(event):
            return None
        state.terminal_seen = True
        state.pending_tools = 0
        return state

    async def _try_complete_pty_background_generation(
        self,
        state: _PtyBackgroundState,
    ) -> bool:
        key = (state.task_id, state.session_id)
        if (
            self._pty_background_states.get(key) is not state
            or state.pending_tools
            or not state.terminal_seen
            or await self.pty_background_activity_pending(
                state.task_id,
                state.session,
            )
        ):
            return False
        # This preflight is deliberately before Harness cleanup.  On a
        # Worker, NodeControl must be the first writer and the exact Task CAS
        # keeps a drain that starts afterwards from overlooking this live
        # marker.  If the irreversible drain already won, retain both the
        # durable marker and the in-memory Session for its termination receipt.
        async with self.db_factory() as db:
            if not await _fence_worker_runtime_mutation(
                db,
                producer="PTY background terminal admission",
            ):
                return False
            admitted = await db.execute(
                update(Task)
                .where(
                    Task.id == state.task_id,
                    Task.session_id == state.session_id,
                    Task.status.in_(
                        ("in_progress", "executing", "completed")
                    ),
                    Task.retry_count == state.task_retry_count,
                    Task.turn_generation == state.task_turn_generation,
                    Task.pty_background_generation == state.generation,
                    task_retry_not_superseded_predicate(),
                    no_active_worker_task_termination_predicate(),
                )
                .values(status=Task.status)
                .execution_options(synchronize_session=False)
            )
            if admitted.rowcount != 1:
                await db.rollback()
                return False
            await db.commit()
        async with self._test_harness_owner_terminal_context(
            state.task_id,
            reason="PTY background activity reached terminal completion",
            expected_retry_count=state.task_retry_count,
            expected_turn_generation=state.task_turn_generation,
            expected_session_id=state.session_id,
            expected_background_generation=state.generation,
        ) as terminal_fenced:
            if not terminal_fenced:
                return False
            async with self.pty_background_transition(
                state.task_id, state.session_id
            ):
                return await self._try_complete_pty_background_generation_locked(
                    state
                )

    @staticmethod
    def _pty_terminal_publication_matches_task(task: Task, publication) -> bool:
        """Match the committed post-marker Task snapshot exactly."""

        return bool(
            task.id == publication.task_id
            and task.incarnation_id == publication.incarnation_id
            and task.retry_count == publication.retry_count
            and task.turn_generation == publication.turn_generation
            and task.session_id == publication.session_id
            and task.status == publication.status
            and task.instance_id == publication.instance_id
            and task.started_at == publication.started_at
            and task.completed_at == publication.completed_at
            and task.pty_background_generation is None
        )

    @staticmethod
    def _pty_terminal_publication_is_superseded(task: Task, publication) -> bool:
        """Prove that an old batch must be discarded rather than emitted.

        A same-generation shape mismatch is deliberately not enough.  It may
        be corruption or an ABA transition whose ordering is unknown.  A new
        incarnation, a strictly newer retry/turn, or a different committed
        terminal status is durable supersession evidence.
        """

        if task.incarnation_id != publication.incarnation_id:
            return True
        current_generation = (task.retry_count, task.turn_generation)
        published_generation = (
            publication.retry_count,
            publication.turn_generation,
        )
        if current_generation > published_generation:
            return True
        return bool(
            current_generation == published_generation
            and task.status in {"completed", "failed", "cancelled", "conflict"}
            and task.status != publication.status
        )

    async def _publish_pty_terminal_publication(self, outbox_id: int) -> bool:
        """Publish one committed PTY terminal batch and delete its marker last.

        The first read is only routing information.  Publication authority is
        reacquired in the canonical NodeControl -> Task -> LogEntry order, and
        the raw envelope is parsed and compared again under those locks.
        """

        from backend.services.task_events import (
            PTY_TERMINAL_PUBLICATION_EVENT_TYPE,
            parse_pty_terminal_publication_payload,
        )
        from backend.services.worker_node_control import (
            fence_worker_node_receipt_resolution,
        )

        try:
            async with self.db_factory() as snapshot_db:
                snapshot = (
                    await snapshot_db.execute(
                        select(
                            LogEntry.id,
                            LogEntry.task_id,
                            LogEntry.task_retry_count,
                            LogEntry.task_turn_generation,
                            LogEntry.native_turn_id,
                            LogEntry.role,
                            LogEntry.content,
                            LogEntry.raw_json,
                        ).where(
                            LogEntry.id == outbox_id,
                            LogEntry.event_type
                            == PTY_TERMINAL_PUBLICATION_EVENT_TYPE,
                        )
                    )
                ).one_or_none()
            if snapshot is None:
                return True
            publication = parse_pty_terminal_publication_payload(
                snapshot.raw_json
            )
            if (
                snapshot.task_id != publication.task_id
                or snapshot.task_retry_count != publication.retry_count
                or snapshot.task_turn_generation
                != publication.turn_generation
                or snapshot.native_turn_id
                != publication.source_background_generation
                or snapshot.role is not None
                or snapshot.content is not None
            ):
                raise ValueError(
                    "PTY terminal publication LogEntry identity is invalid"
                )

            async with self.db_factory() as db:
                # A drain may resolve ownership admitted before its irreversible
                # claim, but it may not create a new publication row.  This
                # writer is therefore intentionally the receipt-resolution
                # fence rather than the ordinary mutation fence.
                await fence_worker_node_receipt_resolution(db)
                task_guard = await db.execute(
                    update(Task)
                    .where(Task.id == publication.task_id)
                    .values(status=Task.status)
                    .execution_options(synchronize_session=False)
                )
                task = None
                if task_guard.rowcount:
                    task = await db.get(
                        Task,
                        publication.task_id,
                        populate_existing=True,
                    )

                # Task is locked first.  Deletion/retry writers either committed
                # before this point (and are observed below) or must wait until
                # the exact old batch is fully emitted and ACK-deleted.
                entry = (
                    await db.execute(
                        select(LogEntry)
                        .where(
                            LogEntry.id == outbox_id,
                            LogEntry.event_type
                            == PTY_TERMINAL_PUBLICATION_EVENT_TYPE,
                        )
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                if entry is None:
                    await db.rollback()
                    return True
                locked_publication = parse_pty_terminal_publication_payload(
                    entry.raw_json
                )
                if (
                    locked_publication != publication
                    or entry.task_id != publication.task_id
                    or entry.task_retry_count != publication.retry_count
                    or entry.task_turn_generation
                    != publication.turn_generation
                    or entry.native_turn_id
                    != publication.source_background_generation
                    or entry.role is not None
                    or entry.content is not None
                ):
                    await db.rollback()
                    logger.error(
                        "Refusing malformed PTY terminal publication %s",
                        outbox_id,
                    )
                    return False

                if task is None:
                    # Task deletion is a committed, irreversible supersession.
                    # There is no channel owner left to receive the old batch.
                    await db.delete(entry)
                    await db.commit()
                    return True
                if not self._pty_terminal_publication_matches_task(
                    task, publication
                ):
                    if self._pty_terminal_publication_is_superseded(
                        task, publication
                    ):
                        await db.delete(entry)
                        await db.commit()
                        logger.info(
                            "Discarded superseded PTY terminal publication %s "
                            "for task %s",
                            outbox_id,
                            publication.task_id,
                        )
                        return True
                    await db.rollback()
                    logger.error(
                        "Retaining ambiguous PTY terminal publication %s for "
                        "task %s after an exact-generation mismatch",
                        outbox_id,
                        publication.task_id,
                    )
                    return False

                for channel, data in publication.events:
                    await self.broadcaster.broadcast(channel, data)
                # Delete-on-ACK is the marker-last step.  A broadcaster error
                # or cancellation rolls this transaction back, leaving the
                # exact immutable batch available for startup/drain recovery.
                await db.delete(entry)
                await db.commit()
                return True
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "PTY terminal publication %s failed; durable outbox retained",
                outbox_id,
            )
            return False

    async def recover_pty_terminal_publications(self) -> tuple[int, int]:
        """Replay every crash-left terminal batch in stable row order."""

        from backend.services.task_events import (
            PTY_TERMINAL_PUBLICATION_EVENT_TYPE,
        )

        async with self.db_factory() as db:
            outbox_ids = list(
                (
                    await db.execute(
                        select(LogEntry.id)
                        .where(
                            LogEntry.event_type
                            == PTY_TERMINAL_PUBLICATION_EVENT_TYPE
                        )
                        .order_by(LogEntry.id)
                    )
                ).scalars()
            )
        resolved = 0
        for outbox_id in outbox_ids:
            if await self._publish_pty_terminal_publication(outbox_id):
                resolved += 1
        return resolved, len(outbox_ids) - resolved

    async def _try_complete_pty_background_generation_locked(
        self,
        state: _PtyBackgroundState,
    ) -> bool:
        key = (state.task_id, state.session_id)
        if self._pty_background_states.get(key) is not state:
            return False
        if state.pending_followups:
            return False
        if await self.pty_background_activity_pending(
            state.task_id, state.session
        ):
            return False
        if state.pending_tools:
            return False
        if not state.terminal_seen:
            return False

        completed_task = False
        original_status: str | None = None
        original_completed_at: datetime | None = None
        async with self.db_factory() as db:
            guarded = await db.execute(
                select(Task.status, Task.completed_at).where(
                    Task.id == state.task_id,
                    Task.session_id == state.session_id,
                    Task.status.in_(
                        ("in_progress", "executing", "completed")
                    ),
                    Task.retry_count == state.task_retry_count,
                    Task.turn_generation == state.task_turn_generation,
                    Task.pty_background_generation == state.generation,
                    task_retry_not_superseded_predicate(),
                    no_active_worker_task_termination_predicate(),
                )
            )
            row = guarded.one_or_none()
            if row is None:
                active_termination = (
                    await active_worker_task_termination_receipt(
                        db,
                        state.task_id,
                    )
                )
                await db.rollback()
                if active_termination is not None:
                    # The receipt now owns terminal cleanup.  Keep the exact
                    # Session/marker state indexed so its executor can stop it;
                    # treating this as an ordinary supersede would strand the
                    # live PTY tail outside durable ownership.
                    return False
                state.outcome = "superseded"
                self._discard_pty_background_state(
                    key, state.generation
                )
                return False
            original_status = row.status
            original_completed_at = row.completed_at
            completed_task = original_status == "completed"
            await db.rollback()

        # PR terminal consumers may persist their own exact-generation
        # outcome.  Keep the durable PTY marker throughout that work: a Worker
        # drain proof must continue to see this Task as live until every
        # terminal consumer has returned.
        if completed_task:
            handler = self.pty_background_completion_handler
            if handler is not None:
                try:
                    result = handler(state.task_id, state.generation)
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    logger.exception(
                        "PTY background terminal handler failed for task %s",
                        state.task_id,
                    )

        # Stage a delete-on-ACK LogEntry together with the post-terminal Task
        # state.  The Task marker is now allowed to clear before publication:
        # the committed LogEntry becomes the Worker drain blocker until every
        # event has returned and the publisher deletes it last.
        outbox_id: int | None = None
        async with self.db_factory() as db:
            if not await _fence_worker_runtime_mutation(
                db,
                producer="PTY background terminal staging",
            ):
                return False
            publish_predicates = [
                Task.id == state.task_id,
                Task.session_id == state.session_id,
                Task.status == original_status,
                Task.retry_count == state.task_retry_count,
                Task.turn_generation == state.task_turn_generation,
                Task.pty_background_generation == state.generation,
                task_retry_not_superseded_predicate(),
                no_active_worker_task_termination_predicate(),
            ]
            if completed_task:
                publish_predicates.append(
                    Task.completed_at.is_(None)
                    if original_completed_at is None
                    else Task.completed_at == original_completed_at
                )
            publication_guard = await db.execute(
                update(Task)
                .where(*publish_predicates)
                .values(status=Task.status)
                .execution_options(synchronize_session=False)
            )
            if publication_guard.rowcount != 1:
                active_termination = (
                    await active_worker_task_termination_receipt(
                        db,
                        state.task_id,
                    )
                )
                await db.rollback()
                if active_termination is not None:
                    return False
                state.outcome = "superseded"
                self._discard_pty_background_state(key, state.generation)
                return False
            locked_task = await db.get(
                Task,
                state.task_id,
                populate_existing=True,
            )
            if locked_task is None:
                await db.rollback()
                return False
            completed_at = (
                datetime.utcnow().replace(microsecond=0)
                if completed_task
                else original_completed_at
            )
            from backend.services.task_events import (
                PTY_TERMINAL_PUBLICATION_EVENT_TYPE,
                build_pty_terminal_publication_payload,
            )

            outbox = LogEntry(
                instance_id=None,
                task_id=state.task_id,
                task_retry_count=state.task_retry_count,
                task_turn_generation=state.task_turn_generation,
                native_turn_id=state.generation,
                turn_scope=None,
                event_type=PTY_TERMINAL_PUBLICATION_EVENT_TYPE,
                role=None,
                content=None,
                raw_json=build_pty_terminal_publication_payload(
                    task_id=state.task_id,
                    incarnation_id=locked_task.incarnation_id,
                    retry_count=state.task_retry_count,
                    turn_generation=state.task_turn_generation,
                    session_id=state.session_id,
                    source_background_generation=state.generation,
                    status=original_status,
                    instance_id=locked_task.instance_id,
                    started_at=locked_task.started_at,
                    completed_at=completed_at,
                ),
                is_error=False,
            )
            db.add(outbox)
            await db.flush()
            # AsyncSession expires ORM attributes on commit in production.
            # Capture the generated key while it is still synchronously loaded
            # so publication never performs an implicit post-commit refresh.
            outbox_id = outbox.id
            values: dict[str, Any] = {
                "error_message": None,
                "pty_background_generation": None,
            }
            if completed_task:
                values["completed_at"] = completed_at
            cleared = await db.execute(
                update(Task)
                .where(*publish_predicates)
                .values(**values)
                .execution_options(synchronize_session=False)
            )
            if cleared.rowcount != 1:
                await db.rollback()
                return False
            await db.commit()

        if outbox_id is None:
            return False
        published = await self._publish_pty_terminal_publication(outbox_id)
        if not published:
            logger.warning(
                "PTY terminal state for task %s committed before publication; "
                "durable outbox %s will be recovered",
                state.task_id,
                outbox_id,
            )
        state.outcome = "completed"
        self._discard_pty_background_state(key, state.generation)
        return True

    async def _fail_pty_background_generation(
        self,
        state: _PtyBackgroundState,
    ) -> bool:
        """Stop the exact PTY Session before publishing a hard-bound failure."""

        key = (state.task_id, state.session_id)
        error = (
            "Claude PTY background activity did not reach a terminal turn "
            f"within {PTY_BACKGROUND_MAX_SECONDS:.0f} seconds"
        )

        async with self.pty_background_transition(
            state.task_id, state.session_id
        ):
            if self._pty_background_states.get(key) is not state:
                return True
            # This is an absolute lifetime bound. A stuck tool or stale
            # durable "running" row is evidence that cleanup is needed, not
            # permission to renew another four-hour lease indefinitely.
            if (
                time.monotonic() - state.started_monotonic
                < PTY_BACKGROUND_MAX_SECONDS
            ):
                return False
            if state.watchdog_stopping:
                return False
            state.watchdog_stopping = True
            # Freeze new events now, but do not wake an attached foreground
            # waiter until InstanceManager.stop has claimed terminal ownership.
            # Waking here would let on_exit remove the adapter mapping before
            # the watchdog can stop that exact Session.
            state.accepting_events = False

            async with self.db_factory() as db:
                if not await _fence_worker_runtime_mutation(
                    db,
                    producer="PTY background watchdog admission",
                ):
                    state.watchdog_stopping = False
                    return False
                task_lock = await db.execute(
                    update(Task)
                    .where(
                        Task.id == state.task_id,
                        Task.session_id == state.session_id,
                        Task.status.in_(
                            ("in_progress", "executing", "completed")
                        ),
                        Task.retry_count == state.task_retry_count,
                        Task.turn_generation
                        == state.task_turn_generation,
                        Task.pty_background_generation
                        == state.generation,
                        task_retry_not_superseded_predicate(),
                        no_active_worker_task_termination_predicate(),
                    )
                    .values(status=Task.status)
                    .execution_options(synchronize_session=False)
                )
                if task_lock.rowcount != 1:
                    active_termination = (
                        await active_worker_task_termination_receipt(
                            db,
                            state.task_id,
                        )
                    )
                    await db.rollback()
                    if active_termination is not None:
                        # Freeze this natural watchdog owner but retain its
                        # exact state for the receipt executor's stop path.
                        state.watchdog_stopping = False
                        return False
                    state.outcome = "superseded"
                    self._discard_pty_background_state(
                        key, state.generation
                    )
                    return True
                task_row = (
                    await db.execute(
                        select(
                            Task.status,
                            Task.retry_count,
                            Task.turn_generation,
                            Task.instance_id,
                            Task.started_at,
                            Task.completed_at,
                        ).where(
                            Task.id == state.task_id,
                            Task.session_id == state.session_id,
                            Task.status.in_(
                                ("in_progress", "executing", "completed")
                            ),
                            Task.retry_count == state.task_retry_count,
                            Task.turn_generation == state.task_turn_generation,
                            Task.pty_background_generation
                            == state.generation,
                            task_retry_not_superseded_predicate(),
                            no_active_worker_task_termination_predicate(),
                        )
                    )
                ).one_or_none()
                if task_row is None:
                    await db.rollback()
                    state.outcome = "superseded"
                    self._discard_pty_background_state(
                        key, state.generation
                    )
                    return True
                owner_rows = (
                    await db.execute(
                        select(Instance).where(
                            Instance.current_task_id == state.task_id
                        )
                    )
                ).scalars().all()
                if len(owner_rows) > 1:
                    state.watchdog_stopping = False
                    await db.rollback()
                    return False
                owner = owner_rows[0] if owner_rows else None
                await db.commit()

        detached = owner is None

        async def stop_exact_background_owner() -> bool:
            if owner is not None:
                attached = getattr(self._pty_backend, "_sessions", {})
                attached_keys = (
                    [
                        candidate_key
                        for candidate_key, candidate in attached.items()
                        if candidate is state.session
                    ]
                    if isinstance(attached, dict)
                    else []
                )
                if attached_keys and attached_keys != [owner.id]:
                    return False
                if not attached_keys:
                    # Backend map cleanup may have won after a prior failed
                    # stop. Retain Task/Instance evidence, stop the exact
                    # Session object, then let the settled-cleanup path clear
                    # those durable owners.
                    if not await self._stop_exact_detached_pty_session(state):
                        return False
                return await self.stop(
                    owner.id,
                    expected_task_id=state.task_id,
                    expected_task_turn_generation=state.task_turn_generation,
                    expected_pid=owner.pid,
                    expected_started_at=owner.started_at,
                    task_status="failed",
                    task_error_message=error,
                    allow_delivery_effect_stop=True,
                    yield_to_worker_task_termination=True,
                )
            return await self.stop_detached_pty_background_generation(
                state.task_id,
                state.session_id,
                state.generation,
                expected_status=task_row.status,
                expected_retry_count=task_row.retry_count,
                expected_turn_generation=task_row.turn_generation,
                expected_instance_id=task_row.instance_id,
                expected_started_at=task_row.started_at,
                expected_completed_at=task_row.completed_at,
                terminal_status="failed",
                error_message=error,
                yield_to_worker_task_termination=True,
            )

        succeeded = False
        try:
            async with self._test_harness_owner_terminal_context(
                state.task_id,
                reason="PTY background watchdog reached its hard timeout",
                expected_retry_count=state.task_retry_count,
                expected_turn_generation=state.task_turn_generation,
                expected_instance_id=task_row.instance_id,
                expected_session_id=state.session_id,
                expected_background_generation=state.generation,
            ) as terminal_fenced:
                if not terminal_fenced:
                    return False
                succeeded = await stop_exact_background_owner()
        finally:
            if self._pty_background_states.get(key) is state:
                state.watchdog_stopping = False

        if not succeeded:
            return False
        if detached:
            # The detached stop committed before returning. Reacquire the
            # exact failed generation and keep its Task row locked through
            # every event so a manual retry/G+1 cannot make these G events
            # arrive after the replacement generation. Receipt admission
            # follows the same Task -> receipt order and therefore either
            # wins before this guard (suppressing publication) or waits until
            # all exact events have been emitted.
            async with self.db_factory() as publication_db:
                if not await _fence_worker_runtime_mutation(
                    publication_db,
                    producer="PTY background watchdog publication",
                ):
                    return True
                publication_guard = await publication_db.execute(
                    update(Task)
                    .where(
                        Task.id == state.task_id,
                        Task.worker_id.is_(None),
                        Task.shared_from_id.is_(None),
                        Task.session_id == state.session_id,
                        Task.status == "failed",
                        Task.retry_count == state.task_retry_count,
                        Task.turn_generation
                        == state.task_turn_generation,
                        (
                            Task.instance_id.is_(None)
                            if task_row.instance_id is None
                            else Task.instance_id == task_row.instance_id
                        ),
                        (
                            Task.started_at.is_(None)
                            if task_row.started_at is None
                            else Task.started_at == task_row.started_at
                        ),
                        Task.completed_at.is_not(None),
                        Task.pty_background_generation.is_(None),
                        task_retry_not_superseded_predicate(),
                    )
                    .values(status="failed")
                    .execution_options(synchronize_session=False)
                )
                if publication_guard.rowcount != 1:
                    await publication_db.rollback()
                    return True
                publication_receipt = (
                    await active_worker_task_termination_receipt(
                        publication_db,
                        state.task_id,
                        for_update=True,
                    )
                )
                if publication_receipt is not None:
                    await publication_db.rollback()
                    return True
                await self.broadcaster.broadcast(
                    "tasks",
                    {
                        "event": "status_change",
                        "task_id": state.task_id,
                        "task_retry_count": state.task_retry_count,
                        "task_turn_generation": state.task_turn_generation,
                        "new_status": "failed",
                        "background_active": False,
                    },
                )
                payload = {
                    "event": "background_activity",
                    "event_type": "background_activity",
                    "task_id": state.task_id,
                    "task_retry_count": state.task_retry_count,
                    "task_turn_generation": state.task_turn_generation,
                    "background_active": False,
                }
                await self.broadcaster.broadcast("tasks", payload)
                await self.broadcaster.broadcast(
                    f"task:{state.task_id}", payload
                )
                await self.broadcaster.broadcast(
                    f"task:{state.task_id}",
                    {
                        "event_type": "process_exit",
                        "task_retry_count": state.task_retry_count,
                        "task_turn_generation": state.task_turn_generation,
                        "exit_code": 1,
                        "stderr": error,
                        "background": True,
                    },
                )
                await publication_db.commit()
        return True

    async def _watch_pty_background_generation(
        self,
        state: _PtyBackgroundState,
    ) -> None:
        """Fail closed at a hard bound when the exact sentinel never arrives."""

        while True:
            try:
                await asyncio.sleep(PTY_BACKGROUND_POLL_SECONDS)
                key = (state.task_id, state.session_id)
                if self._pty_background_states.get(key) is not state:
                    return
                if (
                    state.terminal_seen
                    and await self._try_complete_pty_background_generation(
                        state
                    )
                ):
                    return
                now = time.monotonic()
                if (
                    now - state.started_monotonic
                    >= PTY_BACKGROUND_MAX_SECONDS
                ):
                    if await self._fail_pty_background_generation(state):
                        return
            except asyncio.CancelledError:
                raise
            except Exception:
                # A transient database failure must not kill the only live
                # finalizer and strand a durable marker forever.
                logger.exception(
                    "PTY background watcher iteration failed for task %s "
                    "session %s; retrying",
                    state.task_id,
                    state.session_id,
                )

    @asynccontextmanager
    async def _test_harness_owner_terminal_context(
        self,
        task_id: int,
        *,
        reason: str,
        expected_retry_count: int,
        expected_turn_generation: int,
        expected_instance_id: int | None | object = _EXPECTED_GENERATION_UNSET,
        expected_session_id: str | None | object = _EXPECTED_GENERATION_UNSET,
        expected_background_generation: str | None | object = (
            _EXPECTED_GENERATION_UNSET
        ),
    ):
        """Fence and drain one exact Task owner graph before a status write.

        The Harness identity includes status.  Consequently a terminal writer
        or a new-turn claim must keep this context open until its Task status /
        generation change commits; otherwise a Run can materialize under the
        old identity in the cleanup-to-commit gap.
        """

        from backend.services.test_harness import (
            TestHarnessService,
            test_harness_service as global_test_harness_service,
        )
        from backend.services.test_harness_owner_fence import (
            test_harness_owner_identity,
        )

        predicates = [
            Task.id == task_id,
            Task.retry_count == expected_retry_count,
            Task.turn_generation == expected_turn_generation,
            task_retry_not_superseded_predicate(),
            no_active_worker_task_termination_predicate(),
        ]
        if expected_instance_id is not _EXPECTED_GENERATION_UNSET:
            predicates.append(
                Task.instance_id.is_(None)
                if expected_instance_id is None
                else Task.instance_id == expected_instance_id
            )
        if expected_session_id is not _EXPECTED_GENERATION_UNSET:
            predicates.append(
                Task.session_id.is_(None)
                if expected_session_id is None
                else Task.session_id == expected_session_id
            )
        if expected_background_generation is not _EXPECTED_GENERATION_UNSET:
            predicates.append(
                Task.pty_background_generation.is_(None)
                if expected_background_generation is None
                else Task.pty_background_generation
                == expected_background_generation
            )
        async with self.db_factory() as db:
            task = (
                await db.execute(select(Task).where(*predicates))
            ).scalar_one_or_none()
            if task is None:
                yield False
                return
            identity = test_harness_owner_identity(task)

        service = self.test_harness_service
        if service is None:
            service = (
                global_test_harness_service
                if global_test_harness_service.db_factory is self.db_factory
                else TestHarnessService(db_factory=self.db_factory)
            )
            self.test_harness_service = service
        async with service.owner_stop_fence(
            task_id,
            reason=reason,
            expected_identity=identity,
        ):
            yield True

    @asynccontextmanager
    async def _chat_terminal_locks(
        self,
        task_id: int,
        instance_id: int,
        *,
        expected_retry_count: int,
        expected_turn_generation: int,
        reason: str,
    ):
        """Acquire Harness -> capability -> Instance terminal lock order."""

        from backend.services.capability_service import capability_task_lock

        async with self._test_harness_owner_terminal_context(
            task_id,
            reason=reason,
            expected_retry_count=expected_retry_count,
            expected_turn_generation=expected_turn_generation,
            expected_instance_id=instance_id,
        ) as fenced:
            if not fenced:
                yield False
                return
            async with capability_task_lock(task_id):
                async with self._instance_lifecycle_lock(instance_id):
                    yield True

    @asynccontextmanager
    async def _chat_terminal_db(
        self,
        task_id: int,
        instance_id: int,
        *,
        expected_retry_count: int,
        expected_turn_generation: int,
        reason: str,
    ):
        """Open one Task terminal transaction under the global lock order."""

        async with self._chat_terminal_locks(
            task_id,
            instance_id,
            expected_retry_count=expected_retry_count,
            expected_turn_generation=expected_turn_generation,
            reason=reason,
        ) as fenced:
            if not fenced:
                yield None
                return
            async with self.db_factory() as db:
                yield db

    async def _apply_chat_terminal_to_locked_task(
        self,
        db: AsyncSession,
        task: Task,
        *,
        instance_id: int,
        successful_terminal: bool,
        admit_agent_action: bool,
        failure_sets_completed_at: bool,
        failure_message: str,
        settle_previous_resume: bool = True,
    ) -> Any | None:
        """Settle one exact chat turn inside a caller-owned transaction.

        The caller owns ``capability_task_lock(task.id)``, the Instance
        lifecycle lock, and the Task row lock, in that order.  The previous
        resume outbox must be settled before interpreting this turn so a G+1
        terminal action can atomically acquire the just-released capability
        slot for G+2.  The returned admission is intentionally published only
        after the caller also releases the exact Instance in the same commit.
        """

        if task.status not in {"executing", "in_progress"}:
            raise RuntimeError(
                "Capability resume settlement requires an active Task turn"
            )

        if settle_previous_resume:
            from backend.services.capability_resume import (
                settle_previous_resume_in_terminal_tx,
            )

            await settle_previous_resume_in_terminal_tx(db, task)
        admission = None
        if successful_terminal:
            if (
                admit_agent_action
                and task.mode == "auto"
                and task.capability_policy is not None
            ):
                from backend.services.agent_capability_admission import (
                    AgentTerminalExpectation,
                    admit_agent_terminal_action_locked,
                )

                if (
                    not isinstance(task.incarnation_id, str)
                    or len(task.incarnation_id) != 32
                    or type(task.retry_count) is not int
                    or task.retry_count < 0
                    or type(task.turn_generation) is not int
                    or task.turn_generation < 0
                    or type(task.turn_source_log_id) is not int
                    or task.turn_source_log_id <= 0
                    or task.instance_id != instance_id
                ):
                    task.status = "failed"
                    task.completed_at = datetime.utcnow()
                    task.error_message = (
                        "Auto capability terminal admission lost its exact "
                        "Task/source identity"
                    )
                    task.pty_background_generation = None
                else:
                    admission = await admit_agent_terminal_action_locked(
                        db,
                        task,
                        expected=AgentTerminalExpectation(
                            task_id=task.id,
                            task_incarnation_id=task.incarnation_id,
                            retry_count=task.retry_count,
                            turn_generation=task.turn_generation,
                            instance_id=instance_id,
                            source_log_id=task.turn_source_log_id,
                        ),
                    )
                    if admission.outcome == "stale":
                        return admission
                    if admission.outcome == "ordinary_completion":
                        task.status = "completed"
                        task.completed_at = datetime.utcnow()
                        task.error_message = None
                        task.pty_background_generation = None
            else:
                task.status = "completed"
                task.completed_at = datetime.utcnow()
                task.error_message = None
                task.pty_background_generation = None
        else:
            task.status = "failed"
            if failure_sets_completed_at:
                task.completed_at = datetime.utcnow()
            task.error_message = failure_message[:2000]
            task.pty_background_generation = None
        await db.flush()
        return admission

    async def _publish_agent_terminal_admission(self, admission: Any | None) -> None:
        """Publish creation only while its exact waiting generation survives."""

        if (
            admission is None
            or not getattr(admission, "created", False)
            or getattr(admission, "invocation_id", None) is None
        ):
            return
        from backend.services.agent_capability_admission import (
            publish_agent_terminal_admission_locked,
        )
        from backend.services.capability_service import capability_task_lock

        try:
            async with capability_task_lock(admission.task_id):
                async with self.db_factory() as db:
                    if not await publish_agent_terminal_admission_locked(
                        db,
                        admission,
                    ):
                        await db.rollback()
                        return
                    await db.commit()
        except Exception:
            # Admission and Instance release are already durable.  A transient
            # invalidation failure must not turn that successful terminal
            # transaction into output-consumer recovery or a Task failure.
            logger.exception(
                "Capability creation event publication failed for task %s "
                "invocation %s",
                admission.task_id,
                admission.invocation_id,
            )

    async def finalize_pty_chat_generation(
        self,
        instance_id: int,
        task_id: int,
        exit_code: int | None,
        record: _OutputConsumerRecord,
        *,
        background_generation: str | None = None,
        preserve_background_failure: bool = False,
        background_session_id: str | None = None,
    ) -> str | None:
        """Finalize one exact PTY chat turn, or discard a stale exit callback.

        A PTY ``Session`` and its OS PID are deliberately reused across turns.
        The upstream adapter therefore cannot safely finalize by
        ``instance_id``/``task_id`` alone: an old callback can otherwise clear
        a newer owner on the same slot (including a same-task ABA).  The
        consumer record carries the Task retry generation plus the exact
        ``Instance.started_at`` written for this turn.  Finalization is ordered
        Task -> Instance and both updates live in one transaction; a failure of
        either CAS rolls the other back.
        """

        consumer = asyncio.current_task()
        process = record.process
        expected_started_at = record.instance_started_at
        expected_retry_count = record.task_retry_count
        expected_turn_generation = record.task_turn_generation
        if (
            record.task is not consumer
            or record.task_id != task_id
            or expected_started_at is None
            or expected_retry_count is None
            or expected_turn_generation is None
        ):
            return None

        def owns_generation() -> bool:
            return (
                self._consumer_records.get(instance_id) is record
                and self._tasks.get(instance_id) is consumer
                and self.processes.get(instance_id) is process
            )

        ec = exit_code if exit_code is not None else 0
        interrupted = ec in (-2, 130)
        successful_terminal = ec == 0 or interrupted
        final_status = "completed" if successful_terminal else "failed"
        completed_at = datetime.utcnow()
        provider_error = (record.fatal_provider_error or "").strip()
        failure_notice_data = None

        def background_handoff_pending() -> bool:
            return bool(
                successful_terminal
                and background_generation is None
                and background_session_id
                and self.has_pty_autonomous_activity_handoff(
                    task_id, background_session_id
                )
            )

        async def restore_background_handoff(db) -> bool:
            """Reverse an unpublished terminal commit into one armed epoch."""

            if not background_handoff_pending():
                return False
            generation = secrets.token_urlsafe(24)
            task_restored = await db.execute(
                update(Task)
                .where(
                    Task.id == task_id,
                    Task.status == "completed",
                    Task.instance_id == instance_id,
                    Task.retry_count == expected_retry_count,
                    Task.turn_generation == expected_turn_generation,
                    Task.completed_at == completed_at,
                    Task.pty_background_generation.is_(None),
                    task_retry_not_superseded_predicate(),
                    no_active_worker_task_termination_predicate(),
                )
                .values(
                    status="executing",
                    completed_at=None,
                    error_message=None,
                    pty_background_generation=generation,
                )
            )
            instance_restored = await db.execute(
                update(Instance)
                .where(
                    Instance.id == instance_id,
                    Instance.status == "idle",
                    Instance.pid.is_(None),
                    Instance.current_task_id.is_(None),
                    Instance.started_at == expected_started_at,
                    Instance.provider == "claude",
                )
                .values(
                    status="running",
                    pid=(getattr(process, "pid", 0) or 0),
                    process_identity=capture_process_identity(
                        getattr(process, "pid", 0) or 0
                    ),
                    current_task_id=task_id,
                    provider="claude",
                )
            )
            if (
                not task_restored.rowcount
                or not instance_restored.rowcount
                or not owns_generation()
            ):
                await db.rollback()
                return False
            await db.commit()
            state = self.register_pty_background_generation(
                task_id,
                background_session_id,
                generation,
                getattr(process, "session", None),
                task_retry_count=expected_retry_count,
                task_turn_generation=expected_turn_generation,
            )
            state.last_event_monotonic = time.monotonic()
            payload = {
                "event": "background_activity",
                "event_type": "background_activity",
                "task_id": task_id,
                "task_retry_count": expected_retry_count,
                "task_turn_generation": expected_turn_generation,
                "background_active": True,
            }
            await self.broadcaster.broadcast("tasks", payload)
            await self.broadcaster.broadcast(f"task:{task_id}", payload)
            return True

        async def arm_original_background_handoff(db) -> bool:
            """Arm after rolling an uncommitted terminal admission back."""

            if not background_handoff_pending():
                return False
            generation = secrets.token_urlsafe(24)
            task_armed = await db.execute(
                update(Task)
                .where(
                    Task.id == task_id,
                    Task.status.in_(("executing", "in_progress")),
                    Task.instance_id == instance_id,
                    Task.retry_count == expected_retry_count,
                    Task.turn_generation == expected_turn_generation,
                    Task.completed_at.is_(None),
                    Task.pty_background_generation.is_(None),
                    task_retry_not_superseded_predicate(),
                    no_active_worker_task_termination_predicate(),
                )
                .values(
                    status="executing",
                    completed_at=None,
                    error_message=None,
                    pty_background_generation=generation,
                )
            )
            instance_armed = await db.execute(
                update(Instance)
                .where(
                    Instance.id == instance_id,
                    Instance.status == "running",
                    Instance.pid == (getattr(process, "pid", 0) or 0),
                    Instance.current_task_id == task_id,
                    Instance.started_at == expected_started_at,
                    Instance.provider == "claude",
                )
                .values(status="running", provider="claude")
            )
            if (
                not task_armed.rowcount
                or not instance_armed.rowcount
                or not owns_generation()
            ):
                await db.rollback()
                return False
            await db.commit()
            state = self.register_pty_background_generation(
                task_id,
                background_session_id,
                generation,
                getattr(process, "session", None),
                task_retry_count=expected_retry_count,
                task_turn_generation=expected_turn_generation,
            )
            state.last_event_monotonic = time.monotonic()
            payload = {
                "event": "background_activity",
                "event_type": "background_activity",
                "task_id": task_id,
                "task_retry_count": expected_retry_count,
                "task_turn_generation": expected_turn_generation,
                "background_active": True,
            }
            await self.broadcaster.broadcast("tasks", payload)
            await self.broadcaster.broadcast(f"task:{task_id}", payload)
            return True

        async def withdraw_admission_for_background_handoff(db) -> bool:
            """Withdraw only a pristine, unpublished PTY admission.

            ``finalize_pty_chat_generation`` still owns both the capability
            Task lock and Instance lifecycle lock here.  A same-process
            coordinator therefore cannot start the new Invocation.  The
            durable checks below additionally fail closed if any other actor
            advanced a row during the commit acknowledgement window.
            """

            nonlocal admission
            if not background_handoff_pending() or final_status == "completed":
                return False
            generation = secrets.token_urlsafe(24)
            task = (
                await db.execute(
                    select(Task)
                    .where(
                        Task.id == task_id,
                        Task.status == final_status,
                        Task.instance_id == instance_id,
                        Task.retry_count == expected_retry_count,
                        Task.turn_generation == expected_turn_generation,
                        Task.pty_background_generation.is_(None),
                        task_retry_not_superseded_predicate(),
                        no_active_worker_task_termination_predicate(),
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if task is None:
                await db.rollback()
                return False

            if admission is not None and getattr(admission, "created", False):
                from backend.models.capability import (
                    CapabilityExecution,
                    CapabilityInvocation,
                    CapabilityResumeOutbox,
                )

                invocation = (
                    await db.execute(
                        select(CapabilityInvocation)
                        .where(
                            CapabilityInvocation.id
                            == admission.invocation_id,
                            CapabilityInvocation.task_id == task_id,
                        )
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                executions = list(
                    (
                        await db.scalars(
                            select(CapabilityExecution)
                            .where(
                                CapabilityExecution.invocation_id
                                == admission.invocation_id
                            )
                            .with_for_update()
                            .execution_options(populate_existing=True)
                        )
                    ).all()
                )
                outbox = (
                    await db.execute(
                        select(CapabilityResumeOutbox)
                        .where(
                            CapabilityResumeOutbox.id == admission.outbox_id,
                            CapabilityResumeOutbox.invocation_id
                            == admission.invocation_id,
                            CapabilityResumeOutbox.task_id == task_id,
                        )
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                pristine = bool(
                    invocation is not None
                    and invocation.source == "agent_request"
                    and invocation.status == "queued"
                    and invocation.state_version == 1
                    and invocation.active_task_id == task_id
                    and len(executions) == 1
                    and executions[0].status == "queued"
                    and executions[0].state_version == 1
                    and executions[0].active_invocation_id == invocation.id
                    and executions[0].handle_kind is None
                    and executions[0].handle_id is None
                    and outbox is not None
                    and outbox.status == "pending"
                    and outbox.state_version == 1
                    and outbox.attempt_count == 0
                    and outbox.active_task_id == task_id
                    and outbox.active_invocation_id == invocation.id
                    and outbox.resume_payload is None
                )
                if not pristine:
                    await db.rollback()
                    return False
                await db.delete(outbox)
                await db.delete(executions[0])
                await db.delete(invocation)

            instance = (
                await db.execute(
                    select(Instance)
                    .where(
                        Instance.id == instance_id,
                        Instance.status == "idle",
                        Instance.pid.is_(None),
                        Instance.current_task_id.is_(None),
                        Instance.started_at == expected_started_at,
                        Instance.provider == "claude",
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if instance is None or not owns_generation():
                await db.rollback()
                return False
            task.status = "executing"
            task.completed_at = None
            task.error_message = None
            task.pty_background_generation = generation

            pid = getattr(process, "pid", 0) or 0
            instance.status = "running"
            instance.pid = pid
            instance.process_identity = capture_process_identity(pid)
            instance.current_task_id = task_id
            instance.provider = "claude"
            await db.flush()
            await db.commit()
            admission = None
            state = self.register_pty_background_generation(
                task_id,
                background_session_id,
                generation,
                getattr(process, "session", None),
                task_retry_count=expected_retry_count,
                task_turn_generation=expected_turn_generation,
            )
            state.last_event_monotonic = time.monotonic()
            payload = {
                "event": "background_activity",
                "event_type": "background_activity",
                "task_id": task_id,
                "task_retry_count": expected_retry_count,
                "task_turn_generation": expected_turn_generation,
                "background_active": True,
            }
            await self.broadcaster.broadcast("tasks", payload)
            await self.broadcaster.broadcast(f"task:{task_id}", payload)
            return True

        admission = None
        async with self._chat_terminal_locks(
            task_id,
            instance_id,
            expected_retry_count=expected_retry_count,
            expected_turn_generation=expected_turn_generation,
            reason="PTY chat turn reached terminal bookkeeping",
        ) as terminal_fenced:
            if not terminal_fenced:
                return None
            if instance_id in self._stopping or not owns_generation():
                return None

            async with self.db_factory() as db:
                # Lock/update the Task first.  Cancellation and retry use the
                # same global Task -> Instance order.
                if preserve_background_failure:
                    if successful_terminal or (
                        background_generation is not None
                    ):
                        return None
                    task_result = await db.execute(
                        update(Task)
                        .where(
                            Task.id == task_id,
                            Task.status == "failed",
                            Task.instance_id == instance_id,
                            Task.retry_count == expected_retry_count,
                            Task.turn_generation == expected_turn_generation,
                            Task.pty_background_generation.is_(None),
                            no_active_worker_task_termination_predicate(),
                        )
                        .values(status=Task.status)
                    )
                else:
                    task_result = await db.execute(
                        update(Task)
                        .where(
                            Task.id == task_id,
                            Task.status.in_(
                                [
                                    "executing",
                                    "in_progress",
                                    "failed",
                                    "pending",
                                ]
                            ),
                            Task.instance_id == instance_id,
                            Task.retry_count == expected_retry_count,
                            Task.turn_generation == expected_turn_generation,
                            no_active_worker_task_termination_predicate(),
                        )
                        .values(status=Task.status)
                    )
                if not task_result.rowcount:
                    await db.rollback()
                    return None

                task = (
                    await db.execute(
                        select(Task)
                        .where(Task.id == task_id)
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one()
                if not preserve_background_failure and task.status in {
                    "executing",
                    "in_progress",
                }:
                    admission = await self._apply_chat_terminal_to_locked_task(
                        db,
                        task,
                        instance_id=instance_id,
                        successful_terminal=successful_terminal,
                        admit_agent_action=(ec == 0),
                        failure_sets_completed_at=True,
                        failure_message=(
                            provider_error[:2000]
                            if provider_error
                            else f"Process exited with code {ec}"
                        ),
                    )
                    if (
                        admission is not None
                        and admission.outcome == "stale"
                    ):
                        await db.rollback()
                        return None
                    final_status = task.status
                elif not preserve_background_failure:
                    final_status = (
                        "completed" if successful_terminal else "failed"
                    )
                    task.status = final_status
                    task.pty_background_generation = None
                    task.completed_at = datetime.utcnow()
                    task.error_message = (
                        None
                        if successful_terminal
                        else (
                            provider_error[:2000]
                            if provider_error
                            else f"Process exited with code {ec}"
                        )
                    )
                    await db.flush()
                if (
                    not preserve_background_failure
                    and not successful_terminal
                    and provider_error.startswith("Response timed out")
                ):
                    # A silent Claude PTY timeout means the persisted native
                    # turn may contain an unmatched tool_use. Resuming that
                    # session deterministically reproduced the same dead turn
                    # in production (task 315). Fence it off so the next user
                    # turn starts a fresh native session instead of replaying
                    # corrupted executor state.
                    task.session_id = None
                    task.context_window_usage = None
                    await db.flush()
                if not preserve_background_failure:
                    if (
                        final_status == "completed"
                        and background_generation is not None
                    ):
                        task.pty_background_generation = background_generation
                        await db.flush()

                instance_result = await db.execute(
                    update(Instance)
                    .where(
                        Instance.id == instance_id,
                        Instance.status == "running",
                        Instance.pid == (getattr(process, "pid", 0) or 0),
                        Instance.current_task_id == task_id,
                        Instance.started_at == expected_started_at,
                    )
                    .values(
                        status=(
                            "idle"
                            if successful_terminal
                            else "error"
                        ),
                        pid=None,
                        process_identity=None,
                        current_task_id=None,
                    )
                )
                if not instance_result.rowcount or not owns_generation():
                    await db.rollback()
                    return None
                # API-error events have already been persisted verbatim by
                # _process_event. A process that dies before producing any
                # event still needs a visible chat entry; process_exit alone
                # only stops the frontend spinner.
                if (
                    not successful_terminal
                    and not provider_error
                    and not preserve_background_failure
                ):
                    failure_notice = _terminal_failure_log_entry(
                        instance_id=instance_id,
                        task_id=task_id,
                        task_retry_count=expected_retry_count,
                        task_turn_generation=expected_turn_generation,
                        provider="claude",
                        reason="process_exit_before_response",
                        exit_code=ec,
                        content=(
                            "Claude 进程在返回回复前异常退出"
                            f"（exit code {ec}）。"
                        ),
                    )
                    db.add(failure_notice)
                    await db.flush()
                    failure_notice_data = {
                        "id": failure_notice.id,
                        "instance_id": instance_id,
                        "task_id": task_id,
                        "task_retry_count": expected_retry_count,
                        "task_turn_generation": expected_turn_generation,
                        "turn_scope": failure_notice.turn_scope,
                        "event_type": "system_event",
                        "role": "system",
                        "content": failure_notice.content,
                        "raw_json": failure_notice.raw_json,
                        "is_error": True,
                        "timestamp": (
                            failure_notice.timestamp or datetime.utcnow()
                        ).isoformat(),
                    }
                # MySQL DATETIME without fractional precision normalizes away
                # Python microseconds.  Re-read the locked row before commit
                # and use that database value for the publication fence.
                if not preserve_background_failure:
                    final_status, completed_at = (
                        await db.execute(
                            select(Task.status, Task.completed_at).where(
                                Task.id == task_id
                            )
                        )
                    ).one()
                    if background_handoff_pending():
                        if final_status == "completed":
                            if await restore_background_handoff(db):
                                return "background_armed"
                        else:
                            # The admission is still uncommitted here. Roll it
                            # back rather than charging budget for a foreground
                            # boundary invalidated by autonomous PTY activity.
                            await db.rollback()
                            admission = None
                            if await arm_original_background_handoff(db):
                                return "background_armed"
                            return None
                await db.commit()

            if preserve_background_failure:
                # The watchdog already committed and broadcast the precise
                # timeout failure.  This path owns only the exact Instance
                # release; overwriting the Task would lose the root cause and
                # publish a duplicate generic process error.
                return final_status

            # Publish only while an exact no-op Task update holds this terminal
            # generation.  A retry/replacement must take the same row lock and
            # cannot be followed by this old "completed"/"failed" event.
            async with self.db_factory() as db:
                publish_guard = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task_id,
                        Task.status == final_status,
                        Task.instance_id == instance_id,
                        Task.retry_count == expected_retry_count,
                        Task.turn_generation == expected_turn_generation,
                        Task.completed_at == completed_at,
                        (
                            Task.pty_background_generation
                            == background_generation
                            if background_generation is not None
                            else Task.pty_background_generation.is_(None)
                        ),
                        no_active_worker_task_termination_predicate(),
                    )
                    .values(status=final_status)
                )
                if publish_guard.rowcount and background_handoff_pending():
                    if final_status == "completed":
                        if await restore_background_handoff(db):
                            return "background_armed"
                    elif await withdraw_admission_for_background_handoff(db):
                        return "background_armed"
                if publish_guard.rowcount:
                    if failure_notice_data is not None:
                        await self.broadcaster.broadcast(
                            f"task:{task_id}",
                            failure_notice_data,
                        )
                    if (
                        final_status == "completed"
                        and background_generation is not None
                    ):
                        background_payload = {
                            "event": "background_activity",
                            "event_type": "background_activity",
                            "task_id": task_id,
                            "task_retry_count": expected_retry_count,
                            "task_turn_generation": expected_turn_generation,
                            "background_active": True,
                        }
                        await self.broadcaster.broadcast(
                            "tasks", background_payload
                        )
                        await self.broadcaster.broadcast(
                            f"task:{task_id}", background_payload
                        )
                    await self.broadcaster.broadcast(
                        "tasks",
                        {
                            "event": "status_change",
                            "task_id": task_id,
                            "task_retry_count": expected_retry_count,
                            "task_turn_generation": expected_turn_generation,
                            "new_status": final_status,
                            "instance_id": instance_id,
                            "background_active": bool(
                                background_generation
                                and final_status == "completed"
                            ),
                        },
                    )
                    await self.broadcaster.broadcast(
                        f"task:{task_id}",
                        {
                            "event_type": "process_exit",
                            "task_id": task_id,
                            "task_retry_count": expected_retry_count,
                            "task_turn_generation": expected_turn_generation,
                            "exit_code": ec,
                            "stderr": None,
                            "background_active": bool(
                                background_generation
                                and final_status == "completed"
                            ),
                        },
                    )
                await db.commit()
        await self._publish_agent_terminal_admission(admission)
        return final_status

    def _build_command(
        self,
        provider: str,
        prompt: str,
        model: str | None,
        resume_session_id: str | None,
        effort_level: str | None,
        enable_workflows: bool = False,
        mcp_config_path: str | None = None,
        enabled_skills: dict | None = None,
        system_prompt_mode: str | None = None,
        cwd: str | None = None,
        task_id: int | None = None,
        skill_context: str = "",
        codex_mcp_specs: Sequence["McpServerSpec"] = (),
        codex_api_account: bool = False,
        codex_service_tier: str = "default",
        tools_disabled: bool = False,
        claude_isolation_settings_path: Path | None = None,
        claude_isolation_tools: Sequence[str] | None = None,
        claude_isolation_allowed_rules: Sequence[str] | None = None,
        claude_unrestricted_settings_path: Path | None = None,
        claude_unrestricted_tools: Sequence[str] | None = None,
        claude_unrestricted_allowed_rules: Sequence[str] | None = None,
        claude_prompt_via_stdin: bool = False,
    ) -> list[str]:
        """Build the subprocess command for a supported coding-agent CLI."""
        if provider == "claude":
            cmd = [
                settings.claude_binary,
                "-p",
                "--output-format", "stream-json",
                "--verbose",
            ]
            if not claude_prompt_via_stdin:
                cmd.insert(2, prompt)
            if claude_isolation_settings_path is not None:
                from backend.services.task_agent_isolation import (
                    CLAUDE_TASK_BUILTIN_TOOLS,
                    claude_permission_allow_rules,
                )

                selected_claude_tools = tuple(
                    claude_isolation_tools or CLAUDE_TASK_BUILTIN_TOOLS
                )
                selected_allowed_rules = tuple(
                    claude_isolation_allowed_rules
                    or claude_permission_allow_rules(selected_claude_tools)
                )

                cmd.extend([
                    "--permission-mode",
                    "acceptEdits",
                    "--settings",
                    str(claude_isolation_settings_path),
                    "--setting-sources",
                    "",
                    "--strict-mcp-config",
                    "--disable-slash-commands",
                    "--no-chrome",
                ])
                if not tools_disabled:
                    cmd.extend([
                        "--tools",
                        ",".join(selected_claude_tools),
                        "--allowedTools",
                        ",".join(selected_allowed_rules),
                    ])
            else:
                cmd.append("--dangerously-skip-permissions")
                if claude_unrestricted_tools is not None and not tools_disabled:
                    from backend.services.task_agent_isolation import (
                        claude_permission_allow_rules,
                    )

                    selected_claude_tools = tuple(claude_unrestricted_tools)
                    selected_allowed_rules = tuple(
                        claude_unrestricted_allowed_rules
                        or claude_permission_allow_rules(
                            selected_claude_tools,
                            include_mcp_tools=(
                                claude_unrestricted_settings_path is not None
                            ),
                        )
                    )
                    if claude_unrestricted_settings_path is not None:
                        cmd.extend([
                            "--settings",
                            str(claude_unrestricted_settings_path),
                        ])
                    cmd.extend([
                        "--setting-sources",
                        "",
                        "--strict-mcp-config",
                        "--disable-slash-commands",
                        "--no-chrome",
                        "--tools",
                        ",".join(selected_claude_tools),
                        "--allowedTools",
                        ",".join(selected_allowed_rules),
                    ])
            if resume_session_id:
                cmd.extend(["--resume", resume_session_id])
            if model:
                cmd.extend(["--model", model])
            if effort_level:
                cmd.extend(["--effort", effort_level])
            if tools_disabled:
                # PR review prompts contain the complete backend-snapshotted
                # input. An empty allow-list is the actual credential/tool
                # boundary; the prompt's "do not write" sentence is only
                # defense in depth.
                cmd.extend([
                    "--tools",
                    "",
                    # Ignore user/project/local settings (including hooks and
                    # CLAUDE.md discovery) while retaining the account's normal
                    # OAuth/keychain authentication path; ``--bare`` cannot be
                    # used because it intentionally disables that auth path.
                    "--setting-sources",
                    "",
                    "--strict-mcp-config",
                    "--disable-slash-commands",
                    "--exclude-dynamic-system-prompt-sections",
                ])
                if mcp_config_path and Path(mcp_config_path).exists():
                    cmd.extend(["--mcp-config", mcp_config_path])
                return cmd
            from backend.services.skill_loader import (
                discover_skills,
                get_skill_disallowed_tools,
            )
            skills = discover_skills(project_dir=cwd)
            disallowed = []
            if not enable_workflows:
                disallowed.append("Workflow")
            disallowed.extend(get_skill_disallowed_tools(skills, enabled_skills))
            # Sub-Agent skill: force-disable native Agent/Task tools
            if enabled_skills and enabled_skills.get("sub-agent"):
                disallowed.extend(["Agent", "Task"])
            if disallowed:
                cmd.extend(["--disallowedTools", ",".join(sorted(set(disallowed)))])
            if mcp_config_path and Path(mcp_config_path).exists():
                cmd.extend(["--mcp-config", mcp_config_path])
            # Skill prompt injection (plugins + user skills) is built once by
            # the provider-neutral task context builder.
            from backend.services.skill_context import write_skill_context_file

            skill_prompt_path = write_skill_context_file(
                skill_context,
                task_id,
            )
            if skill_prompt_path:
                cmd.extend(["--append-system-prompt-file", skill_prompt_path])
            if system_prompt_mode and settings.append_system_prompt_file:
                sp_path = Path(settings.append_system_prompt_file)
                if not sp_path.is_absolute():
                    sp_path = Path(settings.worker_deploy_source_dir) / sp_path
                if sp_path.exists():
                    flag = "--system-prompt-file" if system_prompt_mode == "replace" else "--append-system-prompt-file"
                    cmd.extend([flag, str(sp_path)])
            return cmd

        if provider == "codex":
            from backend.services.skill_context import wrap_skill_context

            prompt = wrap_skill_context(prompt, skill_context)
            from backend.services.codex_app_server import (
                CODEX_SERVICE_TIER_PRIORITY,
                normalize_codex_service_tier,
            )

            service_tier = normalize_codex_service_tier(codex_service_tier)
            codex_binary = self._resolve_codex_binary()
            if resume_session_id:
                cmd = [codex_binary, "exec", "resume"]
            else:
                cmd = [codex_binary, "exec"]
            cmd.extend([
                "--json",
                "--skip-git-repo-check",
                "--dangerously-bypass-approvals-and-sandbox",
            ])
            if service_tier == CODEX_SERVICE_TIER_PRIORITY:
                cmd.extend([
                    "--enable",
                    "fast_mode",
                ])
            if model and model != "default":
                cmd.extend(["--model", model])
            codex_effort = clamp_codex_effort(model, effort_level)
            if codex_effort:
                cmd.extend(["-c", f'model_reasoning_effort="{codex_effort}"'])
            if codex_api_account:
                from backend.services.codex_app_server import (
                    codex_untrusted_project_override,
                )

                # Codex otherwise persists this danger-full-access workspace
                # as trusted in the API account's managed user config.  Keep
                # project-local config disabled for API-key isolation while
                # leaving ordinary Codex accounts unchanged.
                cmd.extend([
                    "-c",
                    codex_untrusted_project_override(cwd or os.getcwd()),
                ])
            if codex_mcp_specs:
                from backend.services.codex_app_server import CodexRequiredMcpError
                from backend.services.mcp_config import (
                    render_codex_exec_config_args,
                )

                try:
                    cmd.extend(render_codex_exec_config_args(codex_mcp_specs))
                except (TypeError, ValueError) as exc:
                    if any(spec.required for spec in codex_mcp_specs):
                        logger.exception(
                            "Codex transport fail-closed route=direct-exec "
                            "reason=invalid-required-mcp-config task_id=%s",
                            task_id,
                        )
                        raise CodexRequiredMcpError(
                            "Invalid required Codex exec MCP configuration: "
                            f"{exc}"
                        ) from exc
                    raise
            if service_tier == CODEX_SERVICE_TIER_PRIORITY:
                cmd.extend(["-c", 'service_tier="fast"'])
            else:
                # Explicitly override a user-level Fast preference.  Omission
                # would make a CCM Standard task inherit hidden priority usage.
                cmd.extend(["-c", 'service_tier="default"'])
            if resume_session_id:
                cmd.append(resume_session_id)
            cmd.append(prompt)
            return cmd

        raise ValueError(f"Unsupported CLI provider: {provider}")

    def _resolve_codex_binary(self) -> str:
        """Resolve Codex CLI without relying on the WindowsApps execution alias."""
        configured = settings.codex_binary
        if configured and configured.lower() != "codex":
            return configured

        local_appdata = os.environ.get("LOCALAPPDATA")
        if local_appdata:
            bin_root = Path(local_appdata) / "OpenAI" / "Codex" / "bin"
            candidates = list(bin_root.glob("*/codex.exe"))
            if candidates:
                newest = max(candidates, key=lambda p: p.stat().st_mtime)
                return str(newest)

        return configured or "codex"

    async def _consume_output(
        self,
        instance_id: int,
        task_id: int | None,
        process: asyncio.subprocess.Process,
        loop_iteration: int | None = None,
        chat_initiated: bool = False,
        provider: str = "claude",
    ) -> None:
        """Run the consumer with a terminal, identity-safe recovery boundary."""

        try:
            await self._consume_output_impl(
                instance_id,
                task_id,
                process,
                loop_iteration,
                chat_initiated,
                provider,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(
                "Output consumer bookkeeping failed for instance %s task %s",
                instance_id,
                task_id,
            )
            consumer = asyncio.current_task()
            record = self._consumer_records.get(instance_id)
            tracked_generation = bool(
                record is not None
                and record.process is process
                and record.task is consumer
            )
            expected_retry_count = (
                record.task_retry_count
                if tracked_generation
                else None
            )
            expected_turn_generation = (
                record.task_turn_generation
                if tracked_generation
                else None
            )
            expected_started_at = (
                record.instance_started_at
                if tracked_generation
                else None
            )
            mapped_process = self.processes.get(instance_id)
            mapped_consumer = self._tasks.get(instance_id)
            if (
                mapped_process is not process
                or (
                    mapped_consumer is not None
                    and mapped_consumer is not consumer
                )
            ):
                # A replacement already owns the reusable key.  This stale
                # callback must not retain or mutate the replacement.
                return
            if mapped_consumer is None:
                # Legacy/direct integrations may have registered only the
                # process. Preserve this crashed consumer as fail-closed
                # evidence; it still lacks the durable token required below.
                self._tasks[instance_id] = consumer
            try:
                container_alive = await self._container_exec_alive(
                    instance_id, process
                )
            except Exception:
                container_alive = True
                logger.exception(
                    "Could not inspect container exec after consumer failure "
                    "for instance %s",
                    instance_id,
                )
            reap_confirmed = (
                process.returncode is not None
                and not self._process_group_alive(instance_id, process)
                and not container_alive
            )
            if not reap_confirmed:
                try:
                    await self._signal_managed_process_tree(
                        instance_id, process, signal.SIGKILL
                    )
                    await self._wait_process_tree(instance_id, process, 5.0)
                    reap_confirmed = True
                except Exception:
                    logger.exception(
                        "Could not terminate crashed consumer process for instance %s",
                        instance_id,
                    )
            if reap_confirmed:
                self._forget_container_exec(instance_id, process)
                try:
                    await self._cleanup_active_private_runtime_tempdir(
                        instance_id,
                        process,
                    )
                except Exception:
                    logger.exception(
                        "Private runtime cleanup failed during consumer "
                        "recovery for instance %s",
                        instance_id,
                    )
            if not tracked_generation:
                # In-memory identity alone cannot prove which durable
                # Task/Instance generation is still stored.  Never degrade an
                # emergency recovery into id-only writes: retain the exact
                # process/task handles and require an explicitly fenced
                # lifecycle operation to reconcile them.
                unsettled = ConsumerRecoveryUnsettledError(
                    "Output consumer recovery lacks an exact generation "
                    f"record for instance {instance_id}"
                )
                self._mark_consumer_recovery_pending(
                    instance_id,
                    process,
                    error=unsettled,
                    tracked_generation=False,
                    task_id=task_id,
                    task_retry_count=None,
                    task_turn_generation=None,
                    instance_pid=None,
                    instance_started_at=None,
                    consumer=consumer,
                    record=record,
                )
                raise unsettled from exc
            # A failed post-process hook must not leave the DB advertising a
            # running worker after the process is terminal.  Conditions on the
            # task status preserve a result already committed before a later
            # broadcast/cleanup failure.
            task_publication_generation: dict | None = None
            recovery_failure: ConsumerRecoveryUnsettledError | None = None
            try:
                recovery_db_context = (
                    self._chat_terminal_db(
                        task_id,
                        instance_id,
                        expected_retry_count=expected_retry_count,
                        expected_turn_generation=expected_turn_generation,
                        reason=(
                            "Chat output bookkeeping failed before terminal "
                            "settlement"
                        ),
                    )
                    if task_id and chat_initiated
                    else self.db_factory()
                )
                async with recovery_db_context as db:
                    if db is None:
                        raise RuntimeError(
                            "Output consumer recovery lost its exact Harness "
                            "owner generation"
                        )
                    task_recovery = None
                    if task_id and chat_initiated:
                        # Recovery participates in the same global
                        # Task -> Instance lock order as every other terminal
                        # lifecycle path. If the reverse Instance CAS below
                        # fails, this Task write is rolled back with it.
                        task_recovery = await db.execute(
                            update(Task)
                            .where(
                                Task.id == task_id,
                                Task.status.in_(
                                    ["executing", "in_progress"]
                                ),
                                (
                                    Task.instance_id == instance_id
                                    if tracked_generation
                                    else Task.id == task_id
                                ),
                                (
                                    Task.id == task_id
                                    if expected_retry_count is None
                                    else Task.retry_count
                                    == expected_retry_count
                                ),
                                (
                                    Task.id == task_id
                                    if expected_turn_generation is None
                                    else Task.turn_generation
                                    == expected_turn_generation
                                ),
                                no_active_worker_task_termination_predicate(),
                            )
                            .values(
                                status="failed",
                                completed_at=datetime.utcnow(),
                                error_message=(
                                    f"Output bookkeeping failed: {exc}"
                                )[:500],
                            )
                        )
                    elif (
                        task_id
                        and type(expected_retry_count) is int
                        and expected_retry_count >= 0
                        and type(expected_turn_generation) is int
                        and expected_turn_generation >= 0
                    ):
                        # Dispatcher-owned turns retain their Task status, but
                        # the consumer still owns exact output evidence. Take
                        # a conditional writer fence before the Instance CAS so
                        # a same-transaction failure marker cannot attach to a
                        # cancelled, retried, or otherwise superseded turn.
                        task_recovery = await db.execute(
                            update(Task)
                            .where(
                                Task.id == task_id,
                                Task.status.in_(
                                    ["executing", "in_progress"]
                                ),
                                Task.instance_id == instance_id,
                                Task.retry_count == expected_retry_count,
                                Task.turn_generation
                                == expected_turn_generation,
                                no_active_worker_task_termination_predicate(),
                            )
                            .values(
                                turn_source_log_id=Task.turn_source_log_id
                            )
                        )
                    if (
                        task_recovery is not None
                        and not task_recovery.rowcount
                        and task_id is not None
                        and await active_worker_task_termination_receipt(
                            db,
                            task_id,
                        )
                        is not None
                    ):
                        await db.rollback()
                        raise RuntimeError(
                            "Output consumer recovery yielded to an active "
                            "Worker termination receipt"
                        )
                    if reap_confirmed:
                        recovery_status = (
                            "idle"
                            if process.returncode in (0, -2, 130)
                            else "error"
                        )
                        instance_recovery = await db.execute(
                            update(Instance)
                            .where(
                                Instance.id == instance_id,
                                Instance.pid
                                == getattr(process, "pid", None),
                                (
                                    Instance.current_task_id.is_(None)
                                    if task_id is None
                                    else Instance.current_task_id == task_id
                                ),
                                (
                                    Instance.started_at.is_(None)
                                    if expected_started_at is None
                                    else Instance.started_at
                                    == expected_started_at
                                ),
                            )
                            .values(
                                status=recovery_status,
                                pid=None,
                                process_identity=None,
                                current_task_id=None,
                            )
                        )
                    else:
                        instance_recovery = await db.execute(
                            update(Instance)
                            .where(
                                Instance.id == instance_id,
                                Instance.pid
                                == getattr(process, "pid", None),
                                (
                                    Instance.current_task_id.is_(None)
                                    if task_id is None
                                    else Instance.current_task_id == task_id
                                ),
                                (
                                    Instance.started_at.is_(None)
                                    if expected_started_at is None
                                    else Instance.started_at
                                    == expected_started_at
                                ),
                            )
                            .values(
                                status="error",
                                pid=getattr(process, "pid", None),
                                process_identity=capture_process_identity(
                                    getattr(process, "pid", None)
                                ),
                                current_task_id=task_id,
                            )
                        )
                    if not instance_recovery.rowcount:
                        await db.rollback()
                        durable_instance_generation = (
                            await db.execute(
                                select(
                                    Instance.pid,
                                    Instance.current_task_id,
                                    Instance.started_at,
                                ).where(Instance.id == instance_id)
                            )
                        ).one_or_none()
                        # A missing row or a different durable per-turn token
                        # proves this terminal callback was superseded.  Any
                        # other CAS miss is ambiguous (including same-second
                        # PID reuse on MySQL) and must retain fail-closed
                        # recovery evidence.
                        recovery_superseded = (
                            durable_instance_generation is None
                            or durable_instance_generation.started_at
                            != expected_started_at
                        )
                        if not recovery_superseded:
                            raise RuntimeError(
                                "Exact Instance recovery CAS did not match "
                                f"generation {instance_id}/"
                                f"{getattr(process, 'pid', None)}/"
                                f"{expected_started_at}"
                            )
                    else:
                        if task_recovery is not None and task_recovery.rowcount:
                            # MySQL DATETIME may discard Python microseconds.
                            # Capture the exact persisted values while the Task
                            # row is still locked for the publication fence.
                            resulting_task_generation = (
                                await db.execute(
                                    select(
                                        Task.status,
                                        Task.retry_count,
                                        Task.turn_generation,
                                        Task.instance_id,
                                        Task.started_at,
                                        Task.completed_at,
                                        Task.turn_source_log_id,
                                    ).where(Task.id == task_id)
                                )
                            ).one()
                            source_id = (
                                resulting_task_generation.turn_source_log_id
                            )
                            if (
                                type(expected_retry_count) is int
                                and expected_retry_count >= 0
                                and type(expected_turn_generation) is int
                                and expected_turn_generation >= 0
                                and type(source_id) is int
                                and source_id > 0
                            ):
                                from backend.services.terminal_arbitration import (
                                    source_alias_original_log_id,
                                    source_shape_is_canonical,
                                )

                                recovery_source = (
                                    await db.execute(
                                        select(LogEntry)
                                        .where(LogEntry.id == source_id)
                                        .with_for_update()
                                    )
                                ).scalar_one_or_none()
                                recovery_original = None
                                if recovery_source is not None:
                                    recovery_original_id = (
                                        source_alias_original_log_id(
                                            recovery_source
                                        )
                                    )
                                    if recovery_original_id is not None:
                                        recovery_original = (
                                            await db.execute(
                                                select(LogEntry)
                                                .where(
                                                    LogEntry.id
                                                    == recovery_original_id
                                                )
                                                .with_for_update()
                                            )
                                        ).scalar_one_or_none()
                                if (
                                    recovery_source is not None
                                    and recovery_source.task_id == task_id
                                    and recovery_source.task_retry_count
                                    == expected_retry_count
                                    and recovery_source.task_turn_generation
                                    == expected_turn_generation
                                    and recovery_source.turn_scope == "source"
                                    and source_shape_is_canonical(
                                        recovery_source,
                                        recovery_original,
                                    )
                                ):
                                    process_label = self._provider_process_label(
                                        instance_id,
                                        provider,
                                    )
                                    db.add(
                                        _terminal_failure_log_entry(
                                            instance_id=instance_id,
                                            task_id=task_id,
                                            task_retry_count=(
                                                expected_retry_count
                                            ),
                                            task_turn_generation=(
                                                expected_turn_generation
                                            ),
                                            provider=(
                                                "codex"
                                                if str(provider or "")
                                                .strip()
                                                .lower()
                                                == "codex"
                                                else "claude"
                                            ),
                                            reason="output_consumer_failure",
                                            exit_code=(
                                                process.returncode
                                                if type(process.returncode)
                                                is int
                                                else None
                                            ),
                                            content=(
                                                f"{process_label} 输出消费器在"
                                                "终态记账时失败。"
                                            ),
                                        )
                                    )
                                    await db.flush()
                            if chat_initiated:
                                task_publication_generation = {
                                    "status": (
                                        resulting_task_generation.status
                                    ),
                                    "retry_count": (
                                        resulting_task_generation.retry_count
                                    ),
                                    "turn_generation": (
                                        resulting_task_generation.turn_generation
                                    ),
                                    "instance_id": (
                                        resulting_task_generation.instance_id
                                    ),
                                    "started_at": (
                                        resulting_task_generation.started_at
                                    ),
                                    "completed_at": (
                                        resulting_task_generation.completed_at
                                    ),
                                }
                        await db.commit()
            except Exception as recovery_exc:
                logger.exception(
                    "Failed to persist consumer recovery for instance %s",
                    instance_id,
                )
                recovery_failure = ConsumerRecoveryUnsettledError(
                    "Could not confirm output consumer recovery for "
                    f"instance {instance_id}: {recovery_exc}"
                )
                self._mark_consumer_recovery_pending(
                    instance_id,
                    process,
                    error=recovery_failure,
                    tracked_generation=True,
                    task_id=task_id,
                    task_retry_count=expected_retry_count,
                    task_turn_generation=expected_turn_generation,
                    instance_pid=getattr(process, "pid", None),
                    instance_started_at=expected_started_at,
                    consumer=consumer,
                    record=record,
                )
            if task_publication_generation is not None:
                try:
                    async with self.db_factory() as db:
                        publish_guard = await db.execute(
                            update(Task)
                            .where(
                                Task.id == task_id,
                                Task.status
                                == task_publication_generation["status"],
                                Task.retry_count
                                == task_publication_generation["retry_count"],
                                Task.turn_generation
                                == task_publication_generation[
                                    "turn_generation"
                                ],
                                (
                                    Task.instance_id.is_(None)
                                    if task_publication_generation[
                                        "instance_id"
                                    ]
                                    is None
                                    else Task.instance_id
                                    == task_publication_generation[
                                        "instance_id"
                                    ]
                                ),
                                (
                                    Task.started_at.is_(None)
                                    if task_publication_generation["started_at"]
                                    is None
                                    else Task.started_at
                                    == task_publication_generation["started_at"]
                                ),
                                (
                                    Task.completed_at.is_(None)
                                    if task_publication_generation[
                                        "completed_at"
                                    ]
                                    is None
                                    else Task.completed_at
                                    == task_publication_generation[
                                        "completed_at"
                                    ]
                                ),
                            )
                            .values(
                                status=task_publication_generation["status"]
                            )
                        )
                        if publish_guard.rowcount:
                            await self.broadcaster.broadcast(
                                "tasks",
                                {
                                    "event": "status_change",
                                    "task_id": task_id,
                                    "task_retry_count": (
                                        task_publication_generation[
                                            "retry_count"
                                        ]
                                    ),
                                    "task_turn_generation": (
                                        task_publication_generation[
                                            "turn_generation"
                                        ]
                                    ),
                                    "new_status": "failed",
                                    "instance_id": instance_id,
                                },
                            )
                        await db.commit()
                except Exception:
                    logger.exception(
                        "Failed to publish consumer recovery for instance %s",
                        instance_id,
                    )
            if recovery_failure is not None:
                raise recovery_failure from exc
            raise

    @staticmethod
    async def _drain_stderr(stream, *, retain_bytes: int = 2 * 1024 * 1024) -> bytes:
        """Continuously drain a child stderr pipe while retaining a bounded tail."""

        if stream is None:
            return b""
        retained = bytearray()
        while True:
            try:
                chunk = await stream.read(64 * 1024)
            except TypeError:
                # A few process/test adapters expose ``read()`` without the
                # optional size argument. They are one-shot readers.
                chunk = await stream.read()
                if isinstance(chunk, str):
                    chunk = chunk.encode()
                return bytes(chunk or b"")[-retain_bytes:]
            if not chunk:
                break
            if isinstance(chunk, str):
                chunk = chunk.encode()
            retained.extend(chunk)
            if len(retained) > retain_bytes:
                del retained[:-retain_bytes]
        return bytes(retained)

    @staticmethod
    async def _wait_for_parent_exit(process: asyncio.subprocess.Process) -> None:
        """Wait for the OS parent without requiring inherited pipe EOF.

        On POSIX asyncio may keep ``Process.wait()`` pending until pipe
        transports close, even though the child watcher has already populated
        ``returncode``.  Run wait concurrently, but stop awaiting its adapter
        as soon as either source proves the parent exited.
        """

        if process.returncode is not None:
            wait_runtime_cleanup = getattr(
                process,
                "wait_runtime_cleanup",
                None,
            )
            if callable(wait_runtime_cleanup):
                await wait_runtime_cleanup()
            return
        waiter = asyncio.create_task(process.wait())
        try:
            while process.returncode is None and not waiter.done():
                await asyncio.sleep(0.02)
            if waiter.done():
                await waiter
        finally:
            if not waiter.done():
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)

    @staticmethod
    async def _readline_until_parent_exit(
        process: asyncio.subprocess.Process,
    ) -> bytes:
        """Read one stdout line without letting an orphaned fd block forever."""

        reader = asyncio.create_task(process.stdout.readline())
        try:
            while not reader.done():
                await asyncio.wait({reader}, timeout=0.05)
                if process.returncode is not None and not reader.done():
                    # The OS parent is terminal and no buffered complete line
                    # is available.  A descendant owns the remaining fd; let
                    # finally terminate the exact process group.
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
                    return b""
            return await reader
        except BaseException:
            if not reader.done():
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
            raise

    async def _consume_output_impl(self, instance_id: int, task_id: int | None, process: asyncio.subprocess.Process, loop_iteration: int | None = None, chat_initiated: bool = False, provider: str = "claude"):
        """Read NDJSON lines from stdout, parse, store, and broadcast.

        This method MUST keep running until the process closes stdout (EOF).
        Any exception other than CancelledError is caught and logged so that
        a single bad line or transient DB error never kills the whole consumer.
        """
        consumer_task = asyncio.current_task()
        record = self._consumer_records.get(instance_id)
        tracked_generation = bool(
            record is not None
            and record.process is process
            and record.task is consumer_task
        )
        expected_retry_count = (
            record.task_retry_count
            if tracked_generation
            else None
        )
        expected_turn_generation = (
            record.task_turn_generation
            if tracked_generation
            else None
        )
        expected_started_at = (
            record.instance_started_at
            if tracked_generation
            else None
        )
        # Drain stderr while stdout is being consumed. Reading stderr only
        # after process.wait() can deadlock once the OS pipe buffer fills: the
        # child blocks writing stderr and can neither close stdout nor exit.
        stderr_reader = asyncio.create_task(
            self._drain_stderr(process.stderr),
            name=f"instance-{instance_id}-stderr",
        )

        def owns_instance_turn() -> bool:
            """Whether this consumer still owns the instance bookkeeping.

            Direct unit callers do not register the process/consumer maps, so
            an absent entry remains compatible.  A different entry, however,
            proves a replacement turn was installed and the old consumer must
            not reset its DB state or erase the replacement's maps.
            """

            mapped_process = self.processes.get(instance_id)
            mapped_consumer = self._tasks.get(instance_id)
            return (
                (mapped_process is None or mapped_process is process)
                and (mapped_consumer is None or mapped_consumer is consumer_task)
            )

        _assistant_texts: list[str] = []
        _failure_details: list[object] = []
        _saw_rate_limit = False
        _rate_limit_info: dict | None = None
        _saw_error = False
        _fatal_provider_error: str | None = None
        try:
            while True:
                try:
                    line = await self._readline_until_parent_exit(process)
                    if not line:
                        break
                    text = line.decode("utf-8", errors="replace").strip()
                    if not text:
                        continue

                    if provider == "claude":
                        events = self.parser.parse_line(text)
                    else:
                        parsed = self._parse_codex_line(text)
                        events = [parsed] if parsed else []
                    if not events:
                        continue

                    for event in events:
                        try:
                            fatal_provider_error = (
                                self._fatal_provider_error_for_event(event)
                            )
                            if (
                                fatal_provider_error
                                and _fatal_provider_error is None
                            ):
                                _fatal_provider_error = (
                                    fatal_provider_error[:2000]
                                )
                            await self._process_event(
                                instance_id,
                                task_id,
                                event,
                                loop_iteration,
                                consumer_record=(
                                    record if tracked_generation else None
                                ),
                            )
                            if event.get("event_type") == "rate_limit_event":
                                # Only a genuine near-limit/blocked event should
                                # evaluate a switch. The CLI emits an
                                # "allowed" ping almost every turn; treating those
                                # as rate limits benches healthy accounts and
                                # starves the pool (prod #734/#740).
                                from backend.services.claude_pool import rate_limit_event_is_actionable
                                info = event.get("rate_limit_info")
                                if info is None:
                                    raw = event.get("raw_json")
                                    if raw:
                                        try:
                                            info = json.loads(raw).get("rate_limit_info")
                                        except (ValueError, TypeError):
                                            info = None
                                if rate_limit_event_is_actionable(info):
                                    _saw_rate_limit = True
                                    _rate_limit_info = info
                            if event.get("is_error"):
                                _saw_error = True
                                _failure_details.extend((
                                    event.get("content"),
                                    event.get("error_code"),
                                    event.get("error_details"),
                                ))
                            if event.get("event_type") in ("message", "result") and event.get("role") == "assistant":
                                c = event.get("content") or ""
                                if c:
                                    _assistant_texts.append(c)
                        except Exception:
                            logger.exception("Failed to process event for instance %s task %s: %s", instance_id, task_id, event.get("event_type"))
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Unexpected error in consume loop for instance %s, continuing", instance_id)

        except asyncio.CancelledError:
            # Consumer cancellation is a shutdown/stop signal, but the exact
            # process generation still has to be reaped and its durable owner
            # settled before the task may finish.  Continue into the terminal
            # cleanup below instead of returning from a ``finally`` block.
            pass

        # The CLI parent can exit while a tool process remains in its
        # session and keeps inherited stdout/stderr fds open.  Parent
        # returncode alone is therefore never enough to release this
        # reusable slot: kill and prove the exact direct/container
        # generation gone before writing idle or dropping group evidence.
        reap_error: Exception | None = None
        try:
            await self._wait_for_parent_exit(process)
            if (
                self._process_group_alive(instance_id, process)
                or await self._container_exec_alive(instance_id, process)
            ):
                await self._signal_managed_process_tree(
                    instance_id, process, signal.SIGKILL
                )
                await self._wait_process_tree(instance_id, process, 5.0)
            else:
                self._forget_container_exec(instance_id, process)
            if not self._generation_reap_confirmed(instance_id, process):
                raise RuntimeError(
                    f"Process generation for instance {instance_id} "
                    "could not be proven terminal"
                )
            await self._cleanup_active_private_runtime_tempdir(
                instance_id,
                process,
            )
        except Exception as exc:
            # Drain/cancel stderr below before surfacing the failure.  The
            # outer recovery boundary will retry the exact kill and retain
            # DB ownership plus generation maps if proof still fails.
            reap_error = exc
        exit_code = process.returncode
        if exit_code == 0 and _fatal_provider_error:
            # The CLI can report a structurally failed provider turn while
            # exiting cleanly. Use the semantic result for retries and durable
            # task status; process health must not turn an API error into a
            # completed chat.
            exit_code = 1
        self._effective_exit_codes[instance_id] = (process, exit_code)

        # stderr has been drained concurrently since the turn started.
        # A tool/descendant can outlive the CLI parent while retaining the
        # inherited pipe fd.  Never let that orphan hold the reusable
        # Instance lifecycle forever waiting for EOF.
        try:
            stderr_data = await asyncio.wait_for(
                asyncio.shield(stderr_reader), timeout=2.0
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Timed out draining inherited stderr for instance %s",
                instance_id,
            )
            stderr_reader.cancel()
            await asyncio.gather(stderr_reader, return_exceptions=True)
            stderr_data = b""
        stderr_text = stderr_data.decode("utf-8", errors="replace").strip() if stderr_data else ""
        if stderr_text:
            lines = stderr_text.splitlines()
            lines = [l for l in lines if not re.sub(r'\x1b\[[0-9;]*m', '', l).strip().startswith("[auto]")]
            stderr_text = "\n".join(lines).strip()
        self._last_stderr[instance_id] = stderr_text
        failure_text = _fatal_provider_error or stderr_text

        if reap_error is not None:
            raise RuntimeError(
                f"Could not reap process generation for instance {instance_id}"
            ) from reap_error

        # If stop() was called, it handles instance + task cleanup — skip here
        if instance_id in self._stopping:
            return

        # Empty-reply retry: if chat turn produced only "No response requested."
        # or similar non-response, re-enqueue the original prompt once.
        _NO_RESPONSE_PATTERNS = {"no response requested.", "no response requested", "no response needed."}
        if (
            task_id
            and chat_initiated
            and exit_code == 0
            and not _saw_error
            and instance_id in self._launch_params
            and not self._launch_params[instance_id].get("_retried")
        ):
            combined = " ".join(_assistant_texts).strip().lower().rstrip(".")
            if not _assistant_texts or combined in _NO_RESPONSE_PATTERNS:
                params = self._launch_params[instance_id]
                enqueuer = self.task_message_enqueuer
                from backend.main import dispatcher

                retry_fence = await self._chat_automatic_relaunch_fence(
                    task_id,
                    params,
                    dispatcher=dispatcher,
                )
                if retry_fence is None:
                    # Empty text is not evidence that the provider avoided
                    # tools or other external effects.  Modern chat turns bind
                    # their exact source and actual transport before the first
                    # provider call, so replaying the same source here could
                    # duplicate already-completed work.
                    logger.error(
                        "Task %d got empty/non-response (%r) after provider "
                        "admission; automatic replay was blocked",
                        task_id,
                        combined[:80],
                    )
                elif enqueuer is None:
                    logger.warning(
                        "Task %d got empty/non-response (%r), but no task "
                        "message enqueuer is configured",
                        task_id,
                        combined[:80],
                    )
                else:
                    from backend.services.dispatcher import PRIORITY_USER
                    retry_kwargs = dict(
                        task_id=task_id,
                        prompt=params.get("current_message") or params["prompt"],
                        priority=PRIORITY_USER,
                        source="retry",
                        current_message=(
                            params.get("current_message") or params["prompt"]
                        ),
                        queue_admission_fence=retry_fence,
                    )
                    if isinstance(params.get("enabled_skills"), dict):
                        retry_kwargs["command_skills"] = dict(
                            params["enabled_skills"]
                        )
                    if isinstance(params.get("model"), str):
                        retry_kwargs["model_override"] = params["model"]
                    if params.get("source_log_id") is not None:
                        retry_kwargs["source_log_id"] = params["source_log_id"]
                    if params.get("queue_timestamp") is not None:
                        retry_kwargs["queue_timestamp"] = params[
                            "queue_timestamp"
                        ]
                    retry_kwargs.update({
                        "initiating_user_id": params.get(
                            "initiating_user_id"
                        ),
                        "initiating_user_role": params.get(
                            "initiating_user_role", "member"
                        ),
                        "execution_mode": params.get(
                            "execution_mode", "sandbox"
                        ),
                        "execution_principal_kind": params.get(
                            "execution_principal_kind", "system"
                        ),
                        "attachment_paths": tuple(
                            params.get("attachment_paths") or ()
                        ),
                        "ssh_agent_socket_snapshot": params.get(
                            "ssh_agent_socket_snapshot"
                        ),
                    })
                    admitted = await enqueuer(**retry_kwargs)
                    if admitted is False:
                        logger.info(
                            "Discarded stale empty-reply retry for task %d "
                            "after a queue clear",
                            task_id,
                        )
                    else:
                        params["_retried"] = True
                        logger.warning(
                            "Task %d got empty/non-response (%r), "
                            "re-enqueued prompt",
                            task_id,
                            combined[:80],
                        )
                # Still clean up instance below so it's available for the retry
                # fall through to normal cleanup

        # Quota-aware proactive switch after a successful turn. Claude is
        # event-driven; Codex refreshes its rollout quota on every completed
        # turn because its exec/app-server stream has no equivalent event.
        if task_id and exit_code == 0 and (
            provider == "codex" or _saw_rate_limit
        ):
            await self._try_proactive_pool_switch(
                instance_id,
                task_id,
                rate_limit_info=_rate_limit_info,
                consumer_record=(
                    record if tracked_generation else None
                ),
            )

        structured_preflight_rejection = False
        if task_id and chat_initiated and exit_code not in (0, -2, 130):
            # Human-readable overflow text is not replay evidence: a failed
            # turn may already have emitted assistant output or performed a
            # tool side effect.  Compact/requeue only when the exact durable
            # Codex source and foreground tail prove a structured preflight
            # rejection before any agent activity.
            params = self._launch_params.get(instance_id, {})
            context_preflight_permit = (
                await self._chat_structured_context_preflight_rejection(
                    task_id,
                    params,
                    instance_id=instance_id,
                    expected_retry_count=expected_retry_count,
                    expected_turn_generation=expected_turn_generation,
                    expected_started_at=expected_started_at,
                )
            )
            if context_preflight_permit is not None:
                structured_preflight_rejection = True
                try:
                    from backend.main import dispatcher
                    from backend.services.dispatcher import PRIORITY_USER

                    async with self.db_factory() as db:
                        permit = context_preflight_permit
                        exact_generation = (
                            self._context_preflight_permit_predicates(permit)
                        )
                        still_exact = (
                            await db.execute(
                                select(Task.id).where(*exact_generation)
                            )
                        ).scalar_one_or_none()
                        summary = None
                        if still_exact is None:
                            await db.rollback()
                            logger.info(
                                "Discarding stale prompt-too-long proof for task "
                                "%s before summary collection",
                                task_id,
                            )
                        else:
                            logger.warning(
                                "Task %d exceeded its context window, "
                                "compacting session",
                                task_id,
                            )
                            compact_kwargs = {}
                            if params.get("source_log_id") is not None:
                                compact_kwargs["exclude_log_entry_id"] = (
                                    params["source_log_id"]
                                )
                                compact_kwargs[
                                    "post_source_injects_are_current"
                                ] = True
                            summary = await dispatcher._compact_session(
                                task_id,
                                permit.session_id,
                                db,
                                **compact_kwargs,
                            )
                        if summary:
                            compacted = await db.execute(
                                update(Task)
                                .where(*exact_generation)
                                .values(
                                    session_id=None,
                                    context_window_usage=None,
                                )
                            )
                            if not compacted.rowcount:
                                await db.rollback()
                                logger.info(
                                    "Discarding stale prompt-too-long compaction "
                                    "for task %s",
                                    task_id,
                                )
                            else:
                                await db.commit()
                                current_message = (
                                    params.get("current_message")
                                    or params.get("prompt")
                                    or "continue"
                                )
                                context_retry_permit = (
                                    dispatcher.issue_context_retry_permit(
                                        task_id=permit.task_id,
                                        instance_id=permit.instance_id,
                                        retry_count=permit.retry_count,
                                        turn_generation=permit.turn_generation,
                                        turn_source_log_id=(
                                            permit.turn_source_log_id
                                        ),
                                        session_id=None,
                                        started_at=permit.started_at,
                                        completed_at=permit.completed_at,
                                    )
                                )
                                retry_kwargs = dict(
                                    task_id=task_id,
                                    prompt=build_compacted_resume_prompt(
                                        summary,
                                        current_message,
                                        interrupted=True,
                                    ),
                                    priority=PRIORITY_USER,
                                    source="compact_retry",
                                    current_message=current_message,
                                    context_retry_permit=(
                                        context_retry_permit
                                    ),
                                )
                                if isinstance(
                                    params.get("enabled_skills"),
                                    dict,
                                ):
                                    retry_kwargs["command_skills"] = dict(
                                        params["enabled_skills"]
                                    )
                                if isinstance(params.get("model"), str):
                                    retry_kwargs["model_override"] = params[
                                        "model"
                                    ]
                                if params.get("source_log_id") is not None:
                                    retry_kwargs["source_log_id"] = (
                                        params["source_log_id"]
                                    )
                                if params.get("queue_timestamp") is not None:
                                    retry_kwargs["queue_timestamp"] = (
                                        params["queue_timestamp"]
                                    )
                                retry_kwargs.update({
                                    "initiating_user_id": params.get(
                                        "initiating_user_id"
                                    ),
                                    "initiating_user_role": params.get(
                                        "initiating_user_role", "member"
                                    ),
                                    "execution_mode": params.get(
                                        "execution_mode", "sandbox"
                                    ),
                                    "execution_principal_kind": params.get(
                                        "execution_principal_kind", "system"
                                    ),
                                    "attachment_paths": tuple(
                                        params.get("attachment_paths") or ()
                                    ),
                                    "ssh_agent_socket_snapshot": params.get(
                                        "ssh_agent_socket_snapshot"
                                    ),
                                })
                                try:
                                    admitted = await dispatcher.enqueue_message(
                                        **retry_kwargs
                                    )
                                except BaseException:
                                    dispatcher.revoke_context_retry_permit(
                                        context_retry_permit
                                    )
                                    raise
                                if admitted is False:
                                    dispatcher.revoke_context_retry_permit(
                                        context_retry_permit
                                    )
                except Exception:
                    logger.exception(
                        "Context-window compaction failed for task %d",
                        task_id,
                    )
            # Transient server-side 429/overload: wait + retry same account
            elif await self._try_chat_transient_retry(instance_id, task_id, exit_code, failure_text):
                return
            # Pool rotation for chat-initiated rate limit failures
            elif await self._try_chat_pool_rotation(instance_id, task_id, exit_code, failure_text):
                return
        elif task_id and chat_initiated:
            # Clean turn — drop any transient-retry tally for this instance.
            self._transient_attempts.pop(instance_id, None)

        if not owns_instance_turn():
            logger.info(
                "Skipping stale consumer cleanup for instance %s task %s; "
                "a replacement turn now owns the instance",
                instance_id,
                task_id,
            )
            return

        if task_id and chat_initiated:
            terminal_operation_lock = (
                await self._acquire_terminal_task_operation_lock(
                    task_id,
                    instance_id,
                )
            )
            if terminal_operation_lock is None:
                logger.info(
                    "Chat consumer for instance %s task %s yielded terminal "
                    "settlement to an active stop/termination receipt",
                    instance_id,
                    task_id,
                )
                return
            # Do not retain the in-process lock while writing or publishing.
            # Receipt execution holds it across InstanceManager.stop(), which
            # may await this consumer.  The fresh Task writer transaction and
            # receipt predicates below provide the durable ordering after this
            # cooperative admission handoff is released.
            terminal_operation_lock.release()

        # Commit terminal bookkeeping in the global capability -> lifecycle
        # -> Task -> Instance lock order. Cancellation/retry/delete use the
        # same order; taking either database row first can deadlock those paths
        # on PostgreSQL or MySQL.
        successful_terminal = self._chat_terminal_succeeded(
            process,
            exit_code,
        )
        new_status = "idle" if successful_terminal else "error"
        final_status = None
        task_publication_generation: dict | None = None
        failure_notice_data = None
        admission = None
        terminal_db_context = (
            self._chat_terminal_db(
                task_id,
                instance_id,
                expected_retry_count=expected_retry_count,
                expected_turn_generation=expected_turn_generation,
                reason="Chat turn reached terminal bookkeeping",
            )
            if task_id and chat_initiated
            else self.db_factory()
        )
        async with terminal_db_context as db:
            if task_id and chat_initiated:
                if db is None:
                    logger.info(
                        "Discarding stale chat consumer for instance %s task %s "
                        "before Harness terminal cleanup",
                        instance_id,
                        task_id,
                    )
                    return
                if instance_id in self._stopping or not owns_instance_turn():
                    await db.rollback()
                    return
                # Lock this exact Task generation even when cancellation has
                # already made it terminal. The terminal Task must still allow
                # its exact reverse Instance owner to be released.
                task_generation_predicates = [Task.id == task_id]
                if tracked_generation:
                    task_generation_predicates.append(
                        Task.instance_id == instance_id
                    )
                    if expected_retry_count is not None:
                        task_generation_predicates.append(
                            Task.retry_count == expected_retry_count
                        )
                    if expected_turn_generation is not None:
                        task_generation_predicates.append(
                            Task.turn_generation == expected_turn_generation
                        )
                task_generation_predicates.append(
                    no_active_worker_task_termination_predicate()
                )
                task_lock = await db.execute(
                    update(Task)
                    .where(*task_generation_predicates)
                    .values(status=Task.status)
                )
                if not task_lock.rowcount:
                    await db.rollback()
                    logger.info(
                        "Discarding stale chat consumer for instance %s task %s "
                        "because its Task generation changed",
                        instance_id,
                        task_id,
                    )
                    return

                current_task_generation = (
                    await db.execute(
                        select(Task)
                        .where(Task.id == task_id)
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one()
                chat_active_statuses = {
                    "executing",
                    "in_progress",
                    "failed",
                    "pending",
                }
                if current_task_generation.status in chat_active_statuses:
                    if current_task_generation.status in {
                        "executing",
                        "in_progress",
                    }:
                        admission = (
                            await self._apply_chat_terminal_to_locked_task(
                                db,
                                current_task_generation,
                                instance_id=instance_id,
                                successful_terminal=successful_terminal,
                                admit_agent_action=(exit_code == 0),
                                failure_sets_completed_at=False,
                                failure_message=(
                                    failure_text[:2000]
                                    if failure_text
                                    else f"Process exited with code {exit_code}"
                                ),
                                settle_previous_resume=(
                                    not structured_preflight_rejection
                                ),
                            )
                        )
                        if (
                            admission is not None
                            and admission.outcome == "stale"
                        ):
                            await db.rollback()
                            return
                    else:
                        current_task_generation.status = (
                            "completed"
                            if successful_terminal
                            else "failed"
                        )
                        if successful_terminal:
                            current_task_generation.completed_at = (
                                datetime.utcnow()
                            )
                            current_task_generation.error_message = None
                        else:
                            current_task_generation.error_message = (
                                failure_text[:2000]
                                if failure_text
                                else f"Process exited with code {exit_code}"
                            )
                        await db.flush()
                    final_status = current_task_generation.status
                    if not successful_terminal and not _fatal_provider_error:
                        process_label = self._provider_process_label(
                            instance_id, provider
                        )
                        failure_notice = _terminal_failure_log_entry(
                            instance_id=instance_id,
                            task_id=task_id,
                            task_retry_count=(
                                current_task_generation.retry_count
                            ),
                            task_turn_generation=(
                                current_task_generation.turn_generation
                            ),
                            provider=(
                                "codex"
                                if str(provider or "").strip().lower()
                                == "codex"
                                else "claude"
                            ),
                            reason="process_exit_before_response",
                            exit_code=exit_code,
                            content=(
                                f"{process_label} 进程在返回回复前异常退出"
                                f"（exit code {exit_code}）。"
                            ),
                        )
                        db.add(failure_notice)
                        await db.flush()
                        failure_notice_data = {
                            "id": failure_notice.id,
                            "instance_id": instance_id,
                            "task_id": task_id,
                            "task_retry_count": (
                                current_task_generation.retry_count
                            ),
                            "task_turn_generation": (
                                current_task_generation.turn_generation
                            ),
                            "turn_scope": failure_notice.turn_scope,
                            "event_type": "system_event",
                            "role": "system",
                            "content": failure_notice.content,
                            "raw_json": failure_notice.raw_json,
                            "is_error": True,
                            "timestamp": (
                                failure_notice.timestamp or datetime.utcnow()
                            ).isoformat(),
                        }

                # MySQL DATETIME may discard Python microseconds. Re-read the
                # exact values under the Task lock for the publication fence.
                resulting_task_generation = (
                    await db.execute(
                        select(
                            Task.status,
                            Task.retry_count,
                            Task.turn_generation,
                            Task.instance_id,
                            Task.started_at,
                            Task.completed_at,
                        ).where(Task.id == task_id)
                    )
                ).one()
                task_publication_generation = {
                    "status": resulting_task_generation.status,
                    "retry_count": resulting_task_generation.retry_count,
                    "turn_generation": (
                        resulting_task_generation.turn_generation
                    ),
                    "instance_id": resulting_task_generation.instance_id,
                    "started_at": resulting_task_generation.started_at,
                    "completed_at": resulting_task_generation.completed_at,
                }

            instance_generation_predicates = [Instance.id == instance_id]
            if tracked_generation:
                instance_generation_predicates.extend(
                    [
                        Instance.pid == getattr(process, "pid", None),
                        (
                            Instance.current_task_id.is_(None)
                            if task_id is None
                            else Instance.current_task_id == task_id
                        ),
                        (
                            Instance.started_at.is_(None)
                            if record is None
                            or record.instance_started_at is None
                            else Instance.started_at
                            == record.instance_started_at
                        ),
                    ]
                )
            instance_cleanup = await db.execute(
                update(Instance)
                .where(*instance_generation_predicates)
                .values(
                    status=new_status,
                    pid=None,
                    process_identity=None,
                    current_task_id=None,
                )
            )
            if not instance_cleanup.rowcount:
                await db.rollback()
                logger.info(
                    "Discarding stale consumer cleanup for instance %s "
                    "because its durable generation changed",
                    instance_id,
                )
                return
            if not owns_instance_turn():
                await db.rollback()
                logger.info(
                    "Discarding stale consumer DB cleanup for instance %s task %s",
                    instance_id,
                    task_id,
                )
                return
            await db.commit()

        await self._publish_agent_terminal_admission(admission)

        # Publish only while no-op writes hold the exact terminal generation.
        # A retry/reclaim must take the same locks and therefore cannot be
        # followed by this old status or process-exit event.
        async with self.db_factory() as db:
            publish_allowed = True
            if task_publication_generation is not None:
                task_publish_guard = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task_id,
                        Task.status
                        == task_publication_generation["status"],
                        Task.retry_count
                        == task_publication_generation["retry_count"],
                        Task.turn_generation
                        == task_publication_generation["turn_generation"],
                        (
                            Task.instance_id.is_(None)
                            if task_publication_generation["instance_id"]
                            is None
                            else Task.instance_id
                            == task_publication_generation["instance_id"]
                        ),
                        (
                            Task.started_at.is_(None)
                            if task_publication_generation["started_at"]
                            is None
                            else Task.started_at
                            == task_publication_generation["started_at"]
                        ),
                        (
                            Task.completed_at.is_(None)
                            if task_publication_generation["completed_at"]
                            is None
                            else Task.completed_at
                            == task_publication_generation["completed_at"]
                        ),
                        no_active_worker_task_termination_predicate(),
                    )
                    .values(
                        status=task_publication_generation["status"]
                    )
                )
                publish_allowed = bool(task_publish_guard.rowcount)

            if publish_allowed:
                instance_publish_predicates = [
                    Instance.id == instance_id,
                    Instance.status == new_status,
                    Instance.pid.is_(None),
                    Instance.current_task_id.is_(None),
                ]
                if tracked_generation:
                    instance_publish_predicates.append(
                        Instance.started_at.is_(None)
                        if record is None
                        or record.instance_started_at is None
                        else Instance.started_at
                        == record.instance_started_at
                    )
                instance_publish_guard = await db.execute(
                    update(Instance)
                    .where(*instance_publish_predicates)
                    .values(status=new_status)
                )
                publish_allowed = bool(instance_publish_guard.rowcount)

            if publish_allowed:
                if failure_notice_data is not None:
                    await self.broadcaster.broadcast(
                        f"task:{task_id}",
                        failure_notice_data,
                    )
                if final_status:
                    await self.broadcaster.broadcast(
                        "tasks",
                        {
                            "event": "status_change",
                            "task_id": task_id,
                            "task_retry_count": (
                                task_publication_generation["retry_count"]
                            ),
                            "task_turn_generation": (
                                task_publication_generation[
                                    "turn_generation"
                                ]
                            ),
                            "new_status": final_status,
                            "instance_id": instance_id,
                        },
                    )
                exit_event = {
                    "event_type": "process_exit",
                    "task_id": task_id,
                    "task_retry_count": (
                        task_publication_generation["retry_count"]
                        if task_publication_generation is not None
                        else expected_retry_count
                    ),
                    "task_turn_generation": (
                        task_publication_generation["turn_generation"]
                        if task_publication_generation is not None
                        else expected_turn_generation
                    ),
                    "exit_code": exit_code,
                    "stderr": (
                        stderr_text[:2000] if stderr_text else None
                    ),
                }
                await self.broadcaster.broadcast(
                    f"instance:{instance_id}",
                    exit_event,
                )
                if task_id:
                    await self.broadcaster.broadcast(
                        f"task:{task_id}",
                        exit_event,
                    )
                await self.broadcaster.broadcast(
                    "system",
                    {
                        "event": "instance_status",
                        "instance_id": instance_id,
                        "status": new_status,
                        "exit_code": exit_code,
                    },
                )
            await db.commit()

        # 原生子 agent（native-monitor 等）生命周期跟 session 走。Claude
        # transcript children preserve the historical completed-on-exit
        # behavior; a Codex child should already have emitted an authoritative
        # terminal edge, so any row still running at adapter exit is failed
        # closed（显式中断则 cancelled），不能伪报 completed。
        # CCM 自己的 monitor 子 agent（source="ccm"）有独立进程，不跟主
        # session 走，必须排除，否则 chat turn 结束就误杀 monitor。
        # 但如果有 native-monitor 在 running，说明 monitor 被进程退出打断，
        # 需要 auto-resume 让主 agent 处理积压的 <task-notification>。
        if task_id:
            from backend.models.sub_agent import SubAgentSession

            native_retry_count = (
                task_publication_generation["retry_count"]
                if task_publication_generation is not None
                else expected_retry_count
            )
            native_turn_generation = (
                task_publication_generation["turn_generation"]
                if task_publication_generation is not None
                else expected_turn_generation
            )
            has_stale_native_candidate = False
            has_pending_native_candidate = False
            # Snapshot queue admission before terminalizing a Claude native
            # monitor, but treat this first read only as a hint.  The exact
            # Task generation is locked and every row is re-read below before
            # the durable mutation.
            async with self.db_factory() as db:
                candidates = await db.execute(
                    select(
                        SubAgentSession.provider,
                        SubAgentSession.agent_type,
                    ).where(
                        SubAgentSession.task_id == task_id,
                        SubAgentSession.status == "running",
                        SubAgentSession.source != "ccm",
                    )
                )
                for child_provider, agent_type in candidates.all():
                    has_stale_native_candidate = True
                    if (
                        child_provider != "codex"
                        and agent_type
                        in ("native-monitor", "monitor", "native-agent")
                    ):
                        has_pending_native_candidate = True

            dispatcher = None
            queue_admission_fence = None
            if (
                has_pending_native_candidate
                and exit_code == 0
                and chat_initiated
            ):
                from backend.main import dispatcher
                from backend.services.dispatcher import TaskStartPausedError

                try:
                    queue_admission_fence = (
                        await dispatcher.snapshot_queue_admission(task_id)
                    )
                except (TaskStartPausedError, RuntimeError):
                    logger.info(
                        "Skipping native sub-agent exit wake for task %s "
                        "because queue admission is closed",
                        task_id,
                    )

            has_pending_native = False
            stale_native_by_status: dict[str, list[int]] = {
                "completed": [],
                "failed": [],
                "cancelled": [],
            }
            stale_native_snapshots: dict[int, dict[str, Any]] = {}
            active_sub_agent_count: int | None = None
            if (
                has_stale_native_candidate
                and type(native_retry_count) is int
                and type(native_turn_generation) is int
            ):
                async with self.db_factory() as db:
                    generation_guard = await db.execute(
                        update(Task)
                        .where(
                            Task.id == task_id,
                            Task.retry_count == native_retry_count,
                            Task.turn_generation == native_turn_generation,
                            task_retry_not_superseded_predicate(),
                            no_active_worker_task_termination_predicate(),
                        )
                        .values(status=Task.status)
                    )
                    if not generation_guard.rowcount:
                        await db.rollback()
                    else:
                        stale = await db.execute(
                            select(SubAgentSession)
                            .where(
                                SubAgentSession.task_id == task_id,
                                SubAgentSession.status == "running",
                                SubAgentSession.source != "ccm",
                            )
                            .with_for_update()
                        )
                        for native_session in stale.scalars():
                            session_id = native_session.id
                            agent_type = native_session.agent_type
                            child_provider = (
                                native_session.provider or "claude"
                            )
                            child_status = "completed"
                            if child_provider == "codex":
                                child_status = (
                                    "cancelled"
                                    if exit_code in (-2, 130)
                                    else "failed"
                                )
                            native_sequence = None
                            if child_provider == "codex":
                                try:
                                    child_meta = json.loads(
                                        native_session.meta or "{}"
                                    )
                                except (TypeError, ValueError):
                                    child_meta = None
                                last_sequence = (
                                    child_meta.get("last_sequence")
                                    if isinstance(child_meta, dict)
                                    else None
                                )
                                if type(last_sequence) is int:
                                    native_sequence = last_sequence + 1
                                    child_meta["last_sequence"] = (
                                        native_sequence
                                    )
                                    native_session.meta = json.dumps(
                                        child_meta,
                                        ensure_ascii=False,
                                        sort_keys=True,
                                    )
                                else:
                                    logger.error(
                                        "Codex native child %s lacks a valid "
                                        "lifecycle sequence at process exit",
                                        session_id,
                                    )
                            stale_native_by_status[child_status].append(
                                session_id
                            )
                            stale_native_snapshots[session_id] = {
                                "agent_type": agent_type,
                                "source": "native",
                                "native_mirror_version": 1,
                                "provider": child_provider,
                                "description": native_session.description,
                                "model": native_session.model,
                                "reasoning_effort": (
                                    native_session.codex_effort_level
                                ),
                                "checks_done": native_session.checks_done,
                                "last_summary": native_session.last_summary,
                                "codex_thread_id": (
                                    native_session.codex_thread_id
                                ),
                                "native_sequence": native_sequence,
                            }
                            if (
                                child_provider != "codex"
                                and agent_type
                                in (
                                    "native-monitor",
                                    "monitor",
                                    "native-agent",
                                )
                            ):
                                has_pending_native = True

                        for child_status, session_ids in (
                            stale_native_by_status.items()
                        ):
                            if not session_ids:
                                continue
                            await db.execute(
                                update(SubAgentSession)
                                .where(
                                    SubAgentSession.id.in_(session_ids),
                                    SubAgentSession.task_id == task_id,
                                    SubAgentSession.status == "running",
                                    SubAgentSession.source != "ccm",
                                )
                                .values(
                                    status=child_status,
                                    completed_at=datetime.utcnow(),
                                )
                            )
                        active_sub_agent_count = int((
                            await db.execute(
                                select(func.count(SubAgentSession.id)).where(
                                    SubAgentSession.task_id == task_id,
                                    SubAgentSession.status == "running",
                                )
                            )
                        ).scalar_one())
                        await db.commit()

            stale_native_ids = [
                session_id
                for ids in stale_native_by_status.values()
                for session_id in ids
            ]
            if stale_native_ids:
                for child_status, session_ids in stale_native_by_status.items():
                    for session_id in session_ids:
                        await self.broadcaster.broadcast(
                            f"task:{task_id}",
                            {
                                "event_type": "sub_agent_session_status",
                                "sub_agent_session_id": session_id,
                                "status": child_status,
                                **stale_native_snapshots[session_id],
                                "task_retry_count": native_retry_count,
                                "task_turn_generation": native_turn_generation,
                            },
                        )
                await self.broadcaster.broadcast("tasks", {
                    "event": "sub_agent_count",
                    "event_type": "sub_agent_count",
                    "task_id": task_id,
                    "active_sub_agents": active_sub_agent_count,
                    "task_retry_count": native_retry_count,
                    "task_turn_generation": native_turn_generation,
                })

            # Auto-resume: native sub-agents (monitor/agent) 随进程退出，
            # resume 让主 agent 处理积压的结果并回复用户
            if (
                has_pending_native
                and exit_code == 0
                and chat_initiated
                and queue_admission_fence is not None
            ):
                try:
                    from backend.services.dispatcher import PRIORITY_MONITOR_COMPLETE

                    admitted = await dispatcher.enqueue_message(
                        task_id=task_id,
                        prompt=(
                            "[Monitor 通知] 你之前启动的 Monitor 已有结果。"
                            "请检查 monitor 的 task-notification 并根据结果决定下一步操作。"
                        ),
                        priority=PRIORITY_MONITOR_COMPLETE,
                        source="monitor:native-exit-resume",
                        user_message_text="[Monitor] 后台监控已产生通知，自动恢复会话",
                        queue_admission_fence=queue_admission_fence,
                    )
                    if admitted:
                        logger.info(
                            "Task %d had pending native monitors on exit, "
                            "enqueued auto-resume",
                            task_id,
                        )
                    else:
                        logger.info(
                            "Discarded stale native sub-agent exit wake for "
                            "task %d after a queue clear",
                            task_id,
                        )
                except Exception:
                    logger.exception(
                        "Failed to enqueue monitor auto-resume for task %s", task_id,
                    )

        # A Codex thread with the CCM Sub-Agent controller owns a dedicated
        # stdio MCP helper. Preserve the resumable native thread, but release
        # this app-server connection's idle subscription so Codex can unload
        # the thread-scoped MCP stack after its idle grace period.
        if (
            provider == "codex"
            and getattr(process, "unsubscribe_on_terminal", False)
            and self._codex_app_server is not None
        ):
            thread_id = getattr(process, "thread_id", None)
            if thread_id:
                try:
                    await self._codex_app_server.unsubscribe_thread(thread_id)
                except Exception:
                    # A queued follow-up may already be resuming this thread.
                    # In that case its later terminal consumer will retry the
                    # unsubscribe; never fail completed task bookkeeping.
                    logger.info(
                        "Codex controller thread unsubscribe deferred: "
                        "task=%s thread=%s",
                        task_id,
                        thread_id,
                        exc_info=True,
                    )

        # Never let an old consumer erase a replacement process/consumer.
        # Conditional identity checks also make cleanup safe if a caller
        # bypasses ``launch`` and installs a turn directly in the maps.
        if self.processes.get(instance_id) is process:
            self.processes.pop(instance_id, None)
            if self._process_groups.get(instance_id) is process:
                self._process_groups.pop(instance_id, None)
        if self._tasks.get(instance_id) is consumer_task:
            self._tasks.pop(instance_id, None)
        if owns_instance_turn():
            self._launch_params.pop(instance_id, None)
            self._codex_exec_homes.pop(instance_id, None)

    @staticmethod
    def _context_preflight_permit_predicates(
        permit: _ContextPreflightPermit,
    ) -> list:
        """Fence every Task identity proven by direct-chat preflight logs."""

        return [
            Task.id == permit.task_id,
            Task.status == permit.status,
            Task.instance_id == permit.instance_id,
            Task.retry_count == permit.retry_count,
            Task.turn_generation == permit.turn_generation,
            Task.turn_source_log_id == permit.turn_source_log_id,
            Task.session_id == permit.session_id,
            (
                Task.started_at.is_(None)
                if permit.started_at is None
                else Task.started_at == permit.started_at
            ),
            (
                Task.completed_at.is_(None)
                if permit.completed_at is None
                else Task.completed_at == permit.completed_at
            ),
        ]

    async def _chat_structured_context_preflight_rejection(
        self,
        task_id: int,
        params: dict,
        *,
        instance_id: int,
        expected_retry_count: int | None = None,
        expected_turn_generation: int | None = None,
        expected_started_at: datetime | None = None,
    ) -> _ContextPreflightPermit | None:
        """Prove a direct chat overflow happened before agent activity.

        This is deliberately independent of stderr and rendered message text.
        Only the current exact source, its committed runtime transport, and a
        strict durable provider envelope can authorize compaction and replay.
        Missing, stale, malformed, or mixed-turn evidence fails closed.
        """

        provider = (
            str(params.get("provider") or "").strip().lower()
            if isinstance(params, dict)
            else ""
        )
        if (
            type(task_id) is not int
            or task_id <= 0
            or not isinstance(params, dict)
            or provider not in {"codex", "claude"}
        ):
            return None
        requested_source_id = params.get("source_log_id")
        requested_turn_generation = params.get("task_turn_generation")
        if (
            type(requested_source_id) is not int
            or requested_source_id <= 0
            or type(requested_turn_generation) is not int
            or requested_turn_generation < 0
            or type(instance_id) is not int
            or instance_id <= 0
            or type(expected_retry_count) is not int
            or expected_retry_count < 0
            or type(expected_turn_generation) is not int
            or expected_turn_generation < 0
            or expected_turn_generation != requested_turn_generation
            or not isinstance(expected_started_at, datetime)
        ):
            return None

        from backend.services.terminal_arbitration import (
            source_alias_original_log_id,
            source_shape_is_canonical,
        )

        async with self.db_factory() as db:
            task = await db.get(Task, task_id)
            instance = await db.get(Instance, instance_id)
            if (
                task is None
                or instance is None
                or (task.provider or "claude").lower() != provider
                or task.status not in {"executing", "in_progress"}
                or task.instance_id != instance_id
                or task.retry_count != expected_retry_count
                or task.turn_generation != requested_turn_generation
                or not isinstance(task.session_id, str)
                or not task.session_id
                or type(task.turn_source_log_id) is not int
                or task.turn_source_log_id <= 0
                or instance.current_task_id != task_id
                or instance.started_at != expected_started_at
            ):
                return None

            source = await db.get(LogEntry, task.turn_source_log_id)
            original_id = (
                source_alias_original_log_id(source)
                if source is not None
                else None
            )
            original = (
                await db.get(LogEntry, original_id)
                if original_id is not None
                else None
            )
            if (
                source is None
                or source.task_id != task.id
                or source.task_retry_count != task.retry_count
                or source.task_turn_generation != task.turn_generation
                or source.turn_scope != "source"
                or source.actual_transport
                not in (
                    {"codex_app_server", "codex_exec"}
                    if provider == "codex"
                    else {"claude_pty", "claude_exec"}
                )
                or not source_shape_is_canonical(source, original)
                or requested_source_id not in {source.id, original_id}
            ):
                return None

            rows = list(
                (
                    await db.execute(
                        select(LogEntry)
                        .where(
                            LogEntry.task_id == task.id,
                            LogEntry.task_retry_count == task.retry_count,
                            LogEntry.task_turn_generation
                            == task.turn_generation,
                            LogEntry.turn_scope == "foreground",
                            LogEntry.id > source.id,
                        )
                        .order_by(LogEntry.id)
                    )
                )
                .scalars()
                .all()
            )
            if not rows:
                return None

            parsed_rows: list[dict | None] = []
            for row in rows:
                raw = row.raw_json
                if isinstance(raw, dict):
                    parsed = raw
                elif isinstance(raw, str) and raw:
                    try:
                        value = json.loads(raw)
                    except (TypeError, ValueError, RecursionError):
                        value = None
                    parsed = value if isinstance(value, dict) else None
                else:
                    parsed = None
                parsed_rows.append(parsed)

            terminal = rows[-1]
            terminal_raw = parsed_rows[-1]
            if provider == "claude":
                terminal_result = (
                    terminal_raw if isinstance(terminal_raw, dict) else {}
                )
                terminal_message = terminal_result.get("result")
                usage = terminal_result.get("usage")
                required_usage_keys = (
                    "input_tokens",
                    "output_tokens",
                )
                optional_usage_keys = (
                    "cache_creation_input_tokens",
                    "cache_read_input_tokens",
                )
                usage_is_canonical_zero = (
                    isinstance(usage, dict)
                    and set(usage).issubset(
                        {*required_usage_keys, *optional_usage_keys}
                    )
                    and all(
                        key in usage
                        and type(usage[key]) is int
                        and usage[key] == 0
                        for key in required_usage_keys
                    )
                    and all(
                        type(usage[key]) is int and usage[key] == 0
                        for key in optional_usage_keys
                        if key in usage
                    )
                )
                if not (
                    terminal.event_type == "result"
                    and terminal.is_error is True
                    and terminal_result.get("type") == "result"
                    and terminal_result.get("is_error") is True
                    and terminal_result.get("terminal_reason") == "blocking_limit"
                    and isinstance(terminal_message, str)
                    and "prompt is too long" in terminal_message.lower()
                    and type(terminal_result.get("duration_api_ms")) is int
                    and terminal_result.get("duration_api_ms") == 0
                    and usage_is_canonical_zero
                ):
                    return None
                for row, raw in zip(rows[:-1], parsed_rows[:-1], strict=True):
                    if row.event_type == "message":
                        if not (
                            row.role == "assistant"
                            and row.is_error is True
                            and isinstance(raw, dict)
                            and raw.get("type") == "assistant"
                            and raw.get("isApiErrorMessage") is True
                            and raw.get("error") == "invalid_request"
                            and str(row.content or "").strip().lower()
                            == "prompt is too long"
                        ):
                            return None
                    elif row.event_type in {"system_init", "rate_limit_event"}:
                        continue
                    else:
                        return None
                return _ContextPreflightPermit(
                    task_id=task.id,
                    status=task.status,
                    instance_id=instance_id,
                    retry_count=task.retry_count,
                    turn_generation=task.turn_generation,
                    turn_source_log_id=task.turn_source_log_id,
                    session_id=task.session_id,
                    started_at=task.started_at,
                    completed_at=task.completed_at,
                )
            error = (
                terminal_raw.get("error")
                if isinstance(terminal_raw, dict)
                else None
            )
            error_code = (
                error.get("codexErrorInfo")
                if isinstance(error, dict)
                else None
            )
            error_message = (
                error.get("message") if isinstance(error, dict) else None
            )
            if not (
                terminal.event_type == "system_event"
                and terminal.role is None
                and terminal.is_error is True
                and isinstance(terminal_raw, dict)
                and terminal_raw.get("type") == "turn.failed"
                and isinstance(error_message, str)
                and terminal.content == error_message
                and isinstance(error_code, str)
                and error_code.strip().lower() == "contextwindowexceeded"
            ):
                return None

            seen_start_types: set[str] = set()
            for row, raw in zip(rows[:-1], parsed_rows[:-1], strict=True):
                raw_type = raw.get("type") if isinstance(raw, dict) else None
                if not (
                    row.event_type == "system_event"
                    and row.is_error is False
                    and isinstance(raw, dict)
                    and raw_type in {"thread.started", "turn.started"}
                    and raw_type not in seen_start_types
                ):
                    return None
                seen_start_types.add(raw_type)
            return _ContextPreflightPermit(
                task_id=task.id,
                status=task.status,
                instance_id=instance_id,
                retry_count=task.retry_count,
                turn_generation=task.turn_generation,
                turn_source_log_id=task.turn_source_log_id,
                session_id=task.session_id,
                started_at=task.started_at,
                completed_at=task.completed_at,
            )

    async def _chat_automatic_relaunch_fence(
        self,
        task_id: int,
        params: dict,
        *,
        dispatcher=None,
    ):
        """Freeze queue admission before proving an automatic replay safe.

        The ordering is intentional: cancellation advances the queue epoch or
        generation after this snapshot.  The exact-source DB guard then proves
        the Task was still active before enqueue, and Dispatcher performs the
        final generation comparison while publishing the message.  This
        closes both snapshot-to-guard and guard-to-enqueue cancellation races.
        """

        if dispatcher is None:
            from backend.main import dispatcher as active_dispatcher

            dispatcher = active_dispatcher
        if dispatcher is None:
            return None
        from backend.services.dispatcher import TaskStartPausedError

        try:
            fence = await dispatcher.snapshot_queue_admission(task_id)
        except (TaskStartPausedError, RuntimeError):
            logger.info(
                "Automatic chat replay admission is closed for task %d",
                task_id,
            )
            return None
        if await self._chat_automatic_relaunch_is_blocked(task_id, params):
            return None
        return fence

    async def _chat_automatic_relaunch_is_blocked(
        self,
        task_id: int,
        params: dict,
    ) -> bool:
        """Reject a second chat launch once its exact provider call began.

        Chat retries run inside the output consumer rather than Dispatcher, so
        they need the same durable source/transport fence.  A supplied source
        that is stale, malformed, or no longer owns the Task also fails closed;
        legacy launch params without a source fail closed because they cannot
        prove either the exact logical turn or that no provider effect began.
        """

        requested_source_id = params.get("source_log_id")
        if requested_source_id is None:
            return True
        expected_turn_generation = params.get("task_turn_generation")
        if (
            type(requested_source_id) is not int
            or requested_source_id <= 0
            or type(expected_turn_generation) is not int
            or expected_turn_generation < 0
        ):
            return True

        from backend.services.terminal_arbitration import (
            source_alias_original_log_id,
            source_shape_is_canonical,
        )

        async with self.db_factory() as db:
            task = await db.get(Task, task_id)
            if (
                task is None
                or task.status not in {"executing", "in_progress"}
                or task.turn_generation != expected_turn_generation
                or type(task.turn_source_log_id) is not int
                or task.turn_source_log_id <= 0
            ):
                return True
            source = await db.get(LogEntry, task.turn_source_log_id)
            alias_original_id = (
                source_alias_original_log_id(source)
                if source is not None
                else None
            )
            original = (
                await db.get(LogEntry, alias_original_id)
                if alias_original_id is not None
                else None
            )
            if (
                source is None
                or source.task_id != task.id
                or source.task_retry_count != task.retry_count
                or source.task_turn_generation != task.turn_generation
                or source.turn_scope != "source"
                or not source_shape_is_canonical(source, original)
                or requested_source_id
                not in {source.id, alias_original_id}
            ):
                return True
            return source.actual_transport is not None

    async def _try_chat_transient_retry(
        self, instance_id: int, task_id: int, exit_code: int, stderr_text: str,
    ) -> bool:
        """Wait out a transient server-side 429/overload for a chat turn and
        relaunch the SAME account (no rotation — Anthropic infra throttling, not
        this account's usage limit). Returns True if a retry was launched.

        The attempt tally lives in self._transient_attempts (not _launch_params,
        which launch() overwrites) so it survives the relaunch; it is cleared on
        a non-transient failure, on exhaustion, and on a clean turn.
        """
        params: dict = {}
        provider = "claude"
        try:
            from backend.config import settings as _settings
            if not getattr(_settings, "transient_retry_enabled", True):
                return False

            from backend.services.claude_pool import (
                is_transient_for, transient_retry_delay,
                collect_process_output_for_detection,
            )

            params = self._launch_params.get(instance_id) or {}
            provider = (params.get("provider") or "claude").lower()
            if await self._chat_automatic_relaunch_is_blocked(task_id, params):
                logger.error(
                    "Chat task %d crossed its provider boundary; transient "
                    "relaunch was blocked",
                    task_id,
                )
                self._transient_attempts.pop(instance_id, None)
                return False
            log_contents = await self.get_recent_log_contents(task_id, limit=10)
            combined = collect_process_output_for_detection(stderr_text, log_contents)
            if not (
                is_transient_for(provider, combined)
                or self.is_cloudrouter_transient(
                    instance_id, provider, combined
                )
            ):
                # Non-transient failure — reset tally so the next genuine
                # overload chain starts fresh.
                self._transient_attempts.pop(instance_id, None)
                return False

            attempt = self._transient_attempts.get(instance_id, 0) + 1
            if attempt > _settings.transient_retry_max:
                logger.warning(
                    "Chat task %d transient retries exhausted (%d) — failing turn",
                    task_id, _settings.transient_retry_max,
                )
                self._transient_attempts.pop(instance_id, None)
                return False

            if not params:
                return False

            async with self.db_factory() as db:
                task = await db.get(Task, task_id)
                if not task or not task.session_id:
                    return False
                session_id = task.session_id
                cwd = task.last_cwd or task.target_repo

            config_dir = self._config_dirs.get(instance_id)
            delay = transient_retry_delay(
                attempt,
                _settings.transient_retry_base_delay,
                _settings.transient_retry_max_delay,
            )
            self._transient_attempts[instance_id] = attempt

            logger.info(
                "Chat task %d transient 429/overload — waiting %.0fs before retry #%d/%d",
                task_id, delay, attempt, _settings.transient_retry_max,
            )
            await self.broadcaster.broadcast(f"task:{task_id}", {
                "event_type": "transient_retry",
                "task_id": task_id,
                "attempt": attempt,
                "max_attempts": _settings.transient_retry_max,
                "delay": round(delay, 1),
            })
            await asyncio.sleep(delay)

            await self.launch(
                instance_id=instance_id,
                prompt=params.get("prompt", "请继续之前的工作。"),
                task_id=task_id,
                task_turn_generation=params.get("task_turn_generation"),
                cwd=cwd,
                model=params.get("model"),
                resume_session_id=session_id,
                git_env=params.get("git_env"),
                thinking_budget=params.get("thinking_budget"),
                effort_level=params.get("effort_level"),
                chat_initiated=True,
                config_dir=config_dir,
                provider=provider,
                enable_workflows=params.get("enable_workflows", False),
                enabled_skills=params.get("enabled_skills"),
                source_log_id=params.get("source_log_id"),
                current_message=params.get("current_message"),
                queue_timestamp=params.get("queue_timestamp"),
                codex_service_tier=params.get(
                    "codex_service_tier", "default"
                ),
                initiating_user_id=params.get("initiating_user_id"),
                initiating_user_role=params.get(
                    "initiating_user_role", "member"
                ),
                execution_mode=params.get("execution_mode", "sandbox"),
                execution_principal_kind=params.get(
                    "execution_principal_kind", "system"
                ),
                attachment_paths=tuple(
                    params.get("attachment_paths") or ()
                ),
                ssh_agent_socket_snapshot=params.get(
                    "ssh_agent_socket_snapshot"
                ),
            )
            return True

        except Exception as exc:
            from backend.services.codex_app_server import (
                CodexAppServerBusyError,
                CodexThreadHomeMismatchError,
            )
            from backend.services.dispatcher import CodexAccountRoutingError

            if provider == "codex" and isinstance(
                exc,
                (
                    CodexAccountRoutingError,
                    CodexAppServerBusyError,
                    CodexThreadHomeMismatchError,
                ),
            ):
                await self._requeue_codex_chat_prompt(
                    task_id, params, exc, phase="transient retry",
                )
                self._transient_attempts.pop(instance_id, None)
                return False
            logger.exception("Chat transient retry failed for task %d", task_id)
            self._transient_attempts.pop(instance_id, None)
            return False
    async def _requeue_codex_chat_prompt(
        self,
        task_id: int,
        params: dict,
        exc: Exception,
        *,
        phase: str,
    ) -> bool:
        """Preserve a Codex chat prompt when replacement routing is busy."""

        return await self._requeue_chat_prompt(
            task_id,
            params,
            exc,
            phase=phase,
            provider="Codex",
        )

    async def _requeue_chat_prompt(
        self,
        task_id: int,
        params: dict,
        exc: Exception,
        *,
        phase: str,
        provider: str,
    ) -> bool:
        """Preserve an exact chat prompt after a retryable routing failure."""

        prompt = params.get("prompt")
        if not isinstance(prompt, str) or not prompt:
            return False
        try:
            from backend.main import dispatcher
            from backend.services.dispatcher import PRIORITY_USER

            # Callers normally check before attempting migration/rebind work,
            # but those awaits can race cancellation or terminal settlement.
            # Snapshot queue admission first, then revalidate the exact source
            # and Task status.  Dispatcher compares the frozen fence while
            # publishing, so cancellation in either gap rejects the enqueue.
            retry_fence = await self._chat_automatic_relaunch_fence(
                task_id,
                params,
                dispatcher=dispatcher,
            )
            if retry_fence is None:
                logger.error(
                    "%s chat task %d %s retry lacked a safe exact source; "
                    "queue replay was blocked",
                    provider,
                    task_id,
                    phase,
                )
                return False
            requeue_kwargs = {
                "task_id": task_id,
                "prompt": prompt,
                "priority": PRIORITY_USER,
                "source": "routing_retry",
                "command_skills": (
                    dict(params["enabled_skills"])
                    if isinstance(params.get("enabled_skills"), dict)
                    else None
                ),
                "model_override": (
                    params["model"]
                    if isinstance(params.get("model"), str)
                    else None
                ),
                "queue_admission_fence": retry_fence,
            }
            if params.get("source_log_id") is not None:
                requeue_kwargs["source_log_id"] = params["source_log_id"]
            if params.get("current_message") is not None:
                requeue_kwargs["current_message"] = params["current_message"]
            if params.get("queue_timestamp") is not None:
                requeue_kwargs["queue_timestamp"] = params[
                    "queue_timestamp"
                ]
            requeue_kwargs.update({
                "initiating_user_id": params.get("initiating_user_id"),
                "initiating_user_role": params.get(
                    "initiating_user_role", "member"
                ),
                "execution_mode": params.get("execution_mode", "sandbox"),
                "execution_principal_kind": params.get(
                    "execution_principal_kind", "system"
                ),
                "attachment_paths": tuple(
                    params.get("attachment_paths") or ()
                ),
                "ssh_agent_socket_snapshot": params.get(
                    "ssh_agent_socket_snapshot"
                ),
            })
            admitted = await dispatcher.enqueue_message(**requeue_kwargs)
            if admitted is False:
                logger.info(
                    "Discarded stale %s chat retry for task %d after a "
                    "queue clear",
                    provider,
                    task_id,
                )
                return False
            logger.warning(
                "%s chat task %d %s routing failed; requeued original "
                "prompt for safe retry: %s",
                provider,
                task_id,
                phase,
                exc,
            )
            return True
        except Exception:
            logger.exception(
                "Failed to requeue %s chat prompt for task %d after %s",
                provider,
                task_id,
                phase,
            )
            return False

    async def _try_chat_pool_rotation(
        self, instance_id: int, task_id: int, exit_code: int, stderr_text: str,
    ) -> bool:
        """Attempt pool rotation for a chat-initiated process that hit rate limit.

        Returns True only if rotation succeeded and a replacement process was
        launched. A safely requeued prompt still returns False so the caller
        completes cleanup of the failed process generation first.
        """
        params: dict = {}
        provider = "claude"
        try:
            from backend.main import dispatcher
            if not dispatcher:
                return False

            params = self._launch_params.get(instance_id, {})
            provider = (params.get("provider") or "claude").lower()
            if await self._chat_automatic_relaunch_is_blocked(task_id, params):
                logger.error(
                    "Chat task %d crossed its provider boundary; pool rotation "
                    "relaunch was blocked",
                    task_id,
                )
                return False

            from backend.services.claude_pool import (
                is_pool_rotatable, is_rate_limited, is_auth_failure,
                collect_process_output_for_detection, migrate_session_async,
            )

            log_contents = await self.get_recent_log_contents(task_id, limit=10)
            combined = collect_process_output_for_detection(stderr_text, log_contents)

            if provider == "codex":
                # Dispatcher owns Codex account cooldown, rollout migration,
                # task binding, and registry rebind.  Reuse that single path
                # instead of duplicating subtly different pool semantics here.
                from backend.services.dispatcher import (
                    CodexAccountRoutingError,
                )

                try:
                    rotation = await dispatcher._check_rate_limit_and_rotate(
                        instance_id, task_id, exit_code, combined=combined,
                    )
                except CodexAccountRoutingError as exc:
                    # The failed turn is still cleaned up by _consume_output.
                    # Preserve its exact prompt in the task queue so a rollout
                    # migration/rebind race does not silently drop the user's
                    # message; the queue's routing guard will retry it safely.
                    await self._requeue_codex_chat_prompt(
                        task_id, params, exc, phase="pool rotation",
                    )
                    return False
                if not rotation or not rotation.get("config_dir"):
                    return False

                async with self.db_factory() as db:
                    task = await db.get(Task, task_id)
                    if not task:
                        return False
                    session_id = rotation.get("session_id") or task.session_id
                    cwd = task.last_cwd or task.target_repo

                await self.launch(
                    instance_id=instance_id,
                    prompt=params.get("prompt", "continue"),
                    task_id=task_id,
                    task_turn_generation=params.get("task_turn_generation"),
                    cwd=cwd,
                    model=params.get("model"),
                    resume_session_id=session_id,
                    git_env=params.get("git_env"),
                    thinking_budget=params.get("thinking_budget"),
                    effort_level=params.get("effort_level"),
                    chat_initiated=True,
                    config_dir=rotation["config_dir"],
                    provider="codex",
                    enable_workflows=params.get("enable_workflows", False),
                    enabled_skills=params.get("enabled_skills"),
                    source_log_id=params.get("source_log_id"),
                    current_message=params.get("current_message"),
                    queue_timestamp=params.get("queue_timestamp"),
                    codex_service_tier=params.get(
                        "codex_service_tier", "default"
                    ),
                    initiating_user_id=params.get("initiating_user_id"),
                    initiating_user_role=params.get(
                        "initiating_user_role", "member"
                    ),
                    execution_mode=params.get("execution_mode", "sandbox"),
                    execution_principal_kind=params.get(
                        "execution_principal_kind", "system"
                    ),
                    attachment_paths=tuple(
                        params.get("attachment_paths") or ()
                    ),
                    ssh_agent_socket_snapshot=params.get(
                        "ssh_agent_socket_snapshot"
                    ),
                )
                return True

            if provider != "claude":
                return False
            if not dispatcher.pool or not dispatcher.pool.enabled:
                return False

            cloudrouter_auth_failed = self.is_cloudrouter_auth_failure(
                instance_id, provider, combined
            )
            if not (is_pool_rotatable(combined) or cloudrouter_auth_failed):
                return False

            old_config_dir = self._config_dirs.get(instance_id)
            if not old_config_dir:
                # Default-account launch — still rotatable (see dispatcher)
                old_config_dir = os.path.expanduser("~/.claude")

            if is_auth_failure(combined) or cloudrouter_auth_failed:
                dispatcher.pool.mark_auth_failure(old_config_dir)
                logger.warning("Chat pool rotation: account %s auth failure", old_config_dir)
            elif is_rate_limited(combined):
                dispatcher.pool.mark_rate_limited(old_config_dir)
                logger.info("Chat pool rotation: account %s rate-limited", old_config_dir)

            old_account_id = dispatcher.pool.account_id_from_config_dir(old_config_dir)
            excluded = {old_account_id} if old_account_id else set()
            new_config_dir = dispatcher.pool.select(
                exclude=excluded,
                model=params.get("model"),
            )

            if not new_config_dir:
                logger.warning("Chat pool rotation: no alternative account for task %d", task_id)
                return False

            async with self.db_factory() as db:
                task = await db.get(Task, task_id)
                if not task or not task.session_id:
                    return False
                session_id = task.session_id
                cwd = task.last_cwd or task.target_repo

            # The session may have been created under a different account dir
            # than the one this instance launched with — locate it
            source_dir = (
                dispatcher.pool.locate_session_config_dir(
                    session_id,
                    resident_config_dir=old_config_dir,
                )
                or old_config_dir
            )
            migrated = await migrate_session_async(
                old_config_dir=source_dir,
                new_config_dir=new_config_dir,
                session_id=session_id,
            )
            if not migrated:
                # ``False`` means the target cannot safely resume this native
                # session (missing source JSONL, conflicting target, hardlink
                # failure, ...).  Do not announce or launch a switch that did
                # not happen.  Returning False lets the terminal consumer
                # release the failed generation; the queued exact prompt then
                # follows the normal failed-session recovery path.
                await self._requeue_chat_prompt(
                    task_id,
                    params,
                    RuntimeError(
                        f"session {session_id} could not be migrated "
                        f"from {source_dir} to {new_config_dir}"
                    ),
                    phase="session migration",
                    provider="Claude",
                )
                return False

            new_account_id = dispatcher.pool.account_id_from_config_dir(new_config_dir)
            try:
                binding_persisted = (
                    await dispatcher._persist_claude_binding_for_route(
                        task_id=task_id,
                        config_dir=new_config_dir,
                        expected_generation=None,
                    )
                )
                if not binding_persisted:
                    raise RuntimeError(
                        f"Claude account binding for task {task_id} "
                        "was not persisted"
                    )
            except Exception as exc:
                await self._requeue_chat_prompt(
                    task_id,
                    params,
                    exc,
                    phase="account binding",
                    provider="Claude",
                )
                return False
            logger.info("Chat pool rotation: task %d switching %s -> %s",
                        task_id, old_account_id, new_account_id)

            await self.broadcaster.broadcast(f"task:{task_id}", {
                "event_type": "pool_rotation",
                "old_account": old_account_id,
                "new_account": new_account_id,
                "reason": "rate_limit" if is_rate_limited(combined) else "auth_failure",
            })
            await self.broadcaster.broadcast("system", {
                "event": "pool_rotation",
                "task_id": task_id,
                "instance_id": instance_id,
                "old_account": old_account_id,
                "new_account": new_account_id,
            })

            await self.launch(
                instance_id=instance_id,
                prompt=params.get("prompt", "continue"),
                task_id=task_id,
                task_turn_generation=params.get("task_turn_generation"),
                cwd=cwd,
                model=params.get("model"),
                resume_session_id=session_id,
                git_env=params.get("git_env"),
                thinking_budget=params.get("thinking_budget"),
                effort_level=params.get("effort_level"),
                chat_initiated=True,
                config_dir=new_config_dir,
                enable_workflows=params.get("enable_workflows", False),
                enabled_skills=params.get("enabled_skills"),
                source_log_id=params.get("source_log_id"),
                current_message=params.get("current_message"),
                queue_timestamp=params.get("queue_timestamp"),
                codex_service_tier=params.get(
                    "codex_service_tier", "default"
                ),
                initiating_user_id=params.get("initiating_user_id"),
                initiating_user_role=params.get(
                    "initiating_user_role", "member"
                ),
                execution_mode=params.get("execution_mode", "sandbox"),
                execution_principal_kind=params.get(
                    "execution_principal_kind", "system"
                ),
                attachment_paths=tuple(
                    params.get("attachment_paths") or ()
                ),
                ssh_agent_socket_snapshot=params.get(
                    "ssh_agent_socket_snapshot"
                ),
            )
            return True

        except Exception as exc:
            if provider == "codex":
                from backend.services.codex_app_server import (
                    CodexAppServerBusyError,
                    CodexThreadHomeMismatchError,
                )
                from backend.services.dispatcher import CodexAccountRoutingError

                if isinstance(
                    exc,
                    (
                        CodexAccountRoutingError,
                        CodexAppServerBusyError,
                        CodexThreadHomeMismatchError,
                    ),
                ):
                    await self._requeue_codex_chat_prompt(
                        task_id, params, exc, phase="replacement launch",
                    )
                    return False
            elif provider == "claude":
                from backend.services.dispatcher import (
                    ClaudeAccountRoutingError,
                )

                if isinstance(exc, ClaudeAccountRoutingError):
                    await self._requeue_chat_prompt(
                        task_id,
                        params,
                        exc,
                        phase="replacement launch",
                        provider="Claude",
                    )
                    return False
            logger.exception("Chat pool rotation failed for task %d", task_id)
            return False

    async def _try_proactive_pool_switch(
        self,
        instance_id: int,
        task_id: int,
        *,
        rate_limit_info: dict | None = None,
        expected_generation=None,
        consumer_record: _OutputConsumerRecord | None = None,
    ) -> bool:
        """Move a completed session when its active quota reaches 90%.

        This never relaunches the just-completed turn. A soft quota warning only
        changes account state after migration/rebind succeeds; if every other
        account is unavailable or also known-high, the current account remains
        usable. Plain-text/rejected hard limits retain the existing cooldown
        behavior and are not handled as soft quota thresholds.
        """
        try:
            from backend.main import dispatcher
            if not dispatcher:
                return False

            def generation_predicates(generation) -> list:
                predicates = [
                    Task.id == generation.task_id,
                    Task.retry_count == generation.retry_count,
                    Task.turn_generation == generation.turn_generation,
                    (
                        Task.worker_id.is_(None)
                        if generation.worker_id is None
                        else Task.worker_id == generation.worker_id
                    ),
                    (
                        Task.shared_from_id.is_(None)
                        if generation.shared_from_id is None
                        else Task.shared_from_id
                        == generation.shared_from_id
                    ),
                    (
                        Task.instance_id.is_(None)
                        if generation.instance_id is None
                        else Task.instance_id == generation.instance_id
                    ),
                    (
                        Task.started_at.is_(None)
                        if generation.started_at is None
                        else Task.started_at == generation.started_at
                    ),
                    (
                        Task.completed_at.is_(None)
                        if generation.completed_at is None
                        else Task.completed_at == generation.completed_at
                    ),
                    task_retry_not_superseded_predicate(),
                ]
                frozen_status = getattr(generation, "status", None)
                predicates.append(
                    Task.status == frozen_status
                    if frozen_status is not None
                    else Task.status.in_(("in_progress", "executing"))
                )
                return predicates

            async def generation_is_current(generation) -> bool:
                if consumer_record is not None:
                    if not (
                        self._consumer_records.get(instance_id)
                        is consumer_record
                        and self._tasks.get(instance_id)
                        is consumer_record.task
                        and self.processes.get(instance_id)
                        is consumer_record.process
                    ):
                        return False
                async with self.db_factory() as generation_db:
                    current = await generation_db.scalar(
                        select(Task.id).where(
                            *generation_predicates(generation)
                        )
                    )
                return current is not None

            async with self.db_factory() as db:
                task_stmt = select(Task).where(Task.id == task_id)
                if expected_generation is not None:
                    task_stmt = task_stmt.where(
                        *generation_predicates(expected_generation)
                    )
                task = (
                    await db.execute(task_stmt)
                ).scalar_one_or_none()
                if not task:
                    return False
                generation = _TaskLifecycleFence(
                    task_id=task.id,
                    worker_id=task.worker_id,
                    shared_from_id=task.shared_from_id,
                    retry_count=task.retry_count,
                    turn_generation=task.turn_generation,
                    instance_id=task.instance_id,
                    started_at=task.started_at,
                    completed_at=task.completed_at,
                    status=(
                        getattr(expected_generation, "status", None)
                        if expected_generation is not None
                        else task.status
                    ),
                )
                provider = (task.provider or "claude").lower()
                session_id = task.session_id
                bound_codex_id = (task.metadata_ or {}).get("codex_account_id")
                task_model = task.model
                task_service_tier = (
                    task.codex_service_tier
                    if (
                        isinstance(task.codex_service_tier, str)
                        and task.codex_service_tier in {"default", "priority"}
                    )
                    else "default"
                )

            if not await generation_is_current(generation):
                return False

            if provider == "codex":
                if not session_id:
                    return False
                pool = dispatcher.codex_pool
                if not (pool and pool.enabled):
                    return False
                # An explicit UI selection is a routing lock, not merely a
                # hint for fresh tasks.  In particular, do not let the
                # completed-turn quota balancer undo "switch to this
                # account" by moving the same thread back to an API account.
                # The normal next-turn resolver owns migration to the pinned
                # account when the selection was made while a turn was busy.
                if pool.preferred_account_id is not None:
                    logger.info(
                        "Codex quota switch skipped for task %d: account %s "
                        "is explicitly preferred",
                        task_id,
                        pool.preferred_account_id,
                    )
                    return False
                old_home = self._config_dirs.get(instance_id)
                if not old_home and isinstance(bound_codex_id, str):
                    old_home = pool.home_for_account(bound_codex_id)
                if not old_home:
                    return False
                old_home = pool.canonical_home(old_home)
                new_home = await pool.select_quota_alternative(
                    old_home,
                    model=task_model,
                    service_tier=task_service_tier,
                )
                if not await generation_is_current(generation):
                    return False
                # set_preferred() can race the asynchronous quota lookup.  A
                # late pin must still win before any rollout/owner mutation.
                if pool.preferred_account_id is not None:
                    logger.info(
                        "Codex quota switch abandoned for task %d: account %s "
                        "was explicitly preferred during quota selection",
                        task_id,
                        pool.preferred_account_id,
                    )
                    return False
                if not new_home:
                    logger.info(
                        "Codex quota switch: current account below 90%% or no "
                        "usable alternative for task %d",
                        task_id,
                    )
                    return False
                new_home = pool.canonical_home(new_home)
                old_quota = pool.cached_quota_for_home(old_home)

                from backend.services.codex_session_migration import (
                    migrate_codex_rollout_session,
                )
                from backend.services.codex_pool import quota_cooldown_seconds

                old_account_id = pool.account_id_for_home(old_home)
                new_account_id = pool.account_id_for_home(new_home)
                if old_account_id is None:
                    logger.warning(
                        "Codex quota switch refused for task %d: source home "
                        "%s is not a registered account",
                        task_id,
                        old_home,
                    )
                    return False
                try:
                    await dispatcher._persist_codex_binding_for_route(
                        task_id=task_id,
                        account_id=old_account_id,
                        expected_generation=generation,
                        record_route=False,
                    )
                except Exception:
                    logger.exception(
                        "Codex quota switch could not anchor source account "
                        "binding for task %d",
                        task_id,
                    )
                    return False

                async def rollback_codex_owner() -> bool:
                    try:
                        await self.rebind_codex_thread(
                            session_id,
                            source_codex_home=new_home,
                            target_codex_home=old_home,
                        )
                        rollback_succeeded = True
                    except Exception:
                        rollback_succeeded = False
                        logger.exception(
                            "Codex quota switch rollback failed for task %d "
                            "(%s -> %s)",
                            task_id,
                            new_home,
                            old_home,
                        )
                        try:
                            await self.clear_codex_thread_owner_for_recovery(
                                session_id,
                                expected_codex_home=new_home,
                            )
                        except Exception:
                            logger.exception(
                                "Codex quota switch could not clear stale owner "
                                "for task %d thread %s",
                                task_id,
                                session_id,
                            )
                    return rollback_succeeded

                async def finish_codex_switch() -> bool:
                    """Settle owner + durable affinity as one cancellation unit."""

                    owner_rebound = False
                    binding_committed = False

                    def commit_in_memory_route() -> None:
                        nonlocal binding_committed
                        self._config_dirs[instance_id] = new_home
                        binding_committed = True

                    try:
                        await asyncio.to_thread(
                            migrate_codex_rollout_session,
                            session_id,
                            old_home,
                            new_home,
                        )
                        if not await generation_is_current(generation):
                            raise RuntimeError(
                                "task generation changed after rollout copy"
                            )
                        if pool.preferred_account_id is not None:
                            raise RuntimeError(
                                "explicit Codex account preference changed "
                                "during quota switch"
                            )
                        await self.rebind_codex_thread(
                            session_id,
                            source_codex_home=old_home,
                            target_codex_home=new_home,
                        )
                        owner_rebound = True
                        if not await generation_is_current(generation):
                            raise RuntimeError(
                                "task generation changed after owner rebind"
                            )
                        if pool.preferred_account_id is not None:
                            raise RuntimeError(
                                "explicit Codex account preference changed "
                                "after quota owner rebind"
                            )
                        binding_changed = await (
                            dispatcher._persist_codex_binding_for_route(
                                task_id=task_id,
                                account_id=new_account_id,
                                expected_generation=generation,
                                on_route_committed=commit_in_memory_route,
                            )
                        )
                        if not binding_changed:
                            raise RuntimeError(
                                "task generation changed before binding commit"
                            )
                    except BaseException as exc:
                        # Once the live owner moved, never expose DB=old /
                        # owner=new.  The enclosing task is shielded from its
                        # caller; this branch also compensates direct internal
                        # cancellation before propagating it.
                        rollback_succeeded = True
                        if owner_rebound and not binding_committed:
                            rollback_succeeded = await rollback_codex_owner()
                        if isinstance(exc, asyncio.CancelledError):
                            raise
                        logger.warning(
                            "Codex quota switch failed for task "
                            "%d; old binding %s retained (owner rollback "
                            "succeeded=%s): %s",
                            task_id,
                            old_account_id,
                            rollback_succeeded,
                            exc,
                        )
                        return False

                    pool.mark_rate_limited(
                        old_home,
                        duration=quota_cooldown_seconds(
                            old_quota,
                            fallback=pool._cooldown_seconds,
                        ),
                    )
                    await self.broadcaster.broadcast(f"task:{task_id}", {
                        "event_type": "pool_rotation",
                        "provider": "codex",
                        "old_account": old_account_id,
                        "new_account": new_account_id,
                        "reason": "quota_threshold",
                    })
                    logger.info(
                        "Codex quota switch: task %d migrated %s -> %s",
                        task_id,
                        old_account_id,
                        new_account_id,
                    )
                    return True

                # Parent cancellation (request disconnect / backend shutdown)
                # must not land between the live owner move and the durable
                # Task binding.  Delay it until the exact operation commits or
                # compensates; repeated cancellations are deliberately ignored
                # while the shielded child is still settling.
                switch_operation = asyncio.create_task(finish_codex_switch())
                delayed_cancellation = await await_task_completion(
                    switch_operation
                )
                switched = switch_operation.result()
                if delayed_cancellation is not None:
                    raise delayed_cancellation
                return switched

            if provider != "claude" or not (
                dispatcher.pool and dispatcher.pool.enabled
            ):
                return False

            from backend.services.claude_pool import (
                migrate_session_async,
                quota_cooldown_seconds,
                rate_limit_event_is_actionable,
            )

            old_config_dir = self._config_dirs.get(instance_id)
            if not old_config_dir:
                old_config_dir = os.path.expanduser("~/.claude")

            old_account_id = dispatcher.pool.account_id_from_config_dir(old_config_dir)
            info = rate_limit_info or self._pty_rate_limit_info.get(instance_id)
            status = str((info or {}).get("status") or "").lower()
            hard_limit = bool((info or {}).get("hard_limit")) or (
                bool(status) and status not in {"allowed", "allowed_warning"}
            )

            if hard_limit:
                # Existing hard-limit semantics: quarantine immediately and try
                # any other enabled account. Reactive non-PTY failures continue
                # to use _check_rate_limit_and_rotate unchanged.
                dispatcher.pool.mark_rate_limited(old_config_dir)
                excluded = {old_account_id} if old_account_id else set()
                new_config_dir = dispatcher.pool.select(
                    exclude=excluded,
                    model=task_model,
                )
                reason = "proactive_rate_limit"
            else:
                if not session_id:
                    return False
                if not rate_limit_event_is_actionable(info):
                    return False
                new_config_dir = await dispatcher.pool.select_quota_alternative(
                    old_config_dir,
                    model=task_model,
                )
                if not await generation_is_current(generation):
                    return False
                reason = "quota_threshold"

            if not new_config_dir:
                logger.info(
                    "Proactive pool switch: no usable alternative for task %d; "
                    "continuing current account",
                    task_id,
                )
                return False

            if not session_id:
                # Preserve the legacy hard-limit order: the exhausted account
                # is already cooled even when a fresh turn has no resumable ID.
                return False

            source_dir = (
                dispatcher.pool.locate_session_config_dir(
                    session_id,
                    resident_config_dir=old_config_dir,
                )
                or old_config_dir
            )
            if not await generation_is_current(generation):
                return False
            try:
                source_anchored = (
                    await dispatcher._persist_claude_binding_for_route(
                        task_id=task_id,
                        config_dir=source_dir,
                        expected_generation=generation,
                        record_route=False,
                    )
                )
            except Exception:
                logger.exception(
                    "Proactive Claude pool switch could not anchor source "
                    "account binding for task %d",
                    task_id,
                )
                return False
            if not source_anchored:
                logger.warning(
                    "Proactive Claude pool switch refused unregistered source "
                    "%s for task %d",
                    source_dir,
                    task_id,
                )
                return False
            migrated = await migrate_session_async(
                old_config_dir=source_dir,
                new_config_dir=new_config_dir,
                session_id=session_id,
            )
            if not migrated:
                logger.warning(
                    "Proactive pool switch: session migration failed for task %d",
                    task_id,
                )
                return False

            if not await generation_is_current(generation):
                return False

            new_account_id = dispatcher.pool.account_id_from_config_dir(new_config_dir)
            if not await generation_is_current(generation):
                return False

            def commit_in_memory_route() -> None:
                self._config_dirs[instance_id] = new_config_dir

            try:
                binding_changed = (
                    await dispatcher._persist_claude_binding_for_route(
                        task_id=task_id,
                        config_dir=new_config_dir,
                        expected_generation=generation,
                        on_route_committed=commit_in_memory_route,
                    )
                )
            except Exception:
                logger.exception(
                    "Proactive Claude pool switch could not persist account "
                    "binding for task %d",
                    task_id,
                )
                return False
            if not binding_changed:
                logger.info(
                    "Proactive Claude pool switch for task %d lost its exact "
                    "Task generation before account binding",
                    task_id,
                )
                return False
            if not hard_limit:
                # Soft >=90% isolation begins only after context and the
                # durable target binding have both committed. Use the event's
                # reset boundary (capped; malformed values use pool default).
                dispatcher.pool.mark_rate_limited(
                    old_config_dir,
                    duration=quota_cooldown_seconds(
                        info,
                        fallback=dispatcher.pool._cooldown_seconds,
                    ),
                )
            logger.info(
                "Proactive pool switch: task %d migrated %s -> %s (%s)",
                task_id,
                old_account_id,
                new_account_id,
                reason,
            )

            await self.broadcaster.broadcast(f"task:{task_id}", {
                "event_type": "pool_rotation",
                "old_account": old_account_id,
                "new_account": new_account_id,
                "reason": reason,
            })
            return True

        except Exception:
            logger.exception("Proactive pool switch failed for task %d", task_id)
            return False

    def _parse_codex_line(self, line: str) -> dict | None:
        """Normalize Codex CLI JSONL events into the same shape as Claude logs."""
        now = datetime.utcnow().isoformat()
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            return {
                "event_type": "message",
                "role": "assistant",
                "content": line,
                "tool_name": None,
                "tool_input": None,
                "tool_output": None,
                "raw_json": None,
                "is_error": False,
                "timestamp": now,
            }

        codex_type = data.get("type") or data.get("event") or data.get("event_type") or "codex_event"
        item = data.get("item") if isinstance(data.get("item"), dict) else {}
        item_type = item.get("type")

        event = self._base_codex_event(line, now)
        item_id = data.get("item_id") or data.get("itemId") or item.get("id")
        if item_id:
            # Keep the native item identity on the durable completion event as
            # well as its deltas. The frontend uses it to replace a partial
            # live bubble after a task-channel resubscription.
            event["item_id"] = str(item_id)

        if codex_type == "native.subagent.lifecycle":
            lifecycle_event = data.get("lifecycle_event")
            native_agent_id = data.get("native_agent_id")
            status = data.get("status")
            sequence = data.get("sequence")
            expected_statuses = {
                "spawn": {"running"},
                "progress": {"running"},
                "done": {"completed", "failed", "cancelled"},
            }
            if (
                data.get("provider") != "codex"
                or lifecycle_event not in expected_statuses
                or status not in expected_statuses[lifecycle_event]
                or not isinstance(native_agent_id, str)
                or not native_agent_id
                or native_agent_id != native_agent_id.strip()
                or len(native_agent_id) > 255
                or type(sequence) is not int
                or sequence <= 0
            ):
                return None

            bounded_strings = {
                "root_thread_id": 255,
                "parent_native_agent_id": 255,
                "description": 500,
                "agent_path": 500,
                "model": 100,
                "reasoning_effort": 20,
                "summary": 2000,
            }
            info: dict[str, Any] = {
                "tool_use_id": f"codex:{native_agent_id}",
                "native_agent_id": native_agent_id,
                "provider": "codex",
                "kind": "native-agent",
                "status": status,
                "sequence": sequence,
            }
            for key, limit in bounded_strings.items():
                value = data.get(key)
                if value is None:
                    continue
                if (
                    not isinstance(value, str)
                    or value != value.strip()
                    or not value
                    or len(value) > limit
                ):
                    return None
                info[key] = value
            event.update({
                "event_type": f"subagent_{lifecycle_event}",
                "role": "system",
                "content": None,
                "subagent": info,
            })
        elif codex_type == "item.agent_message.delta":
            event.update({
                "event_type": "message_delta",
                "role": "assistant",
                "content": data.get("delta") or "",
                "item_id": data.get("item_id"),
            })
        elif codex_type == "item.reasoning.delta":
            event.update({
                "event_type": "thinking_delta",
                "role": "assistant",
                "content": data.get("delta") or "",
                "item_id": data.get("item_id"),
            })
        elif codex_type == "item.completed" and item_type == "agent_message":
            event.update({
                "event_type": "message",
                "role": "assistant",
                "content": item.get("text") or "",
            })
        elif codex_type == "item.started" and item_type == "command_execution":
            command = item.get("command") or ""
            event.update({
                "event_type": "tool_use",
                "role": "assistant",
                "content": None,
                "tool_name": "Shell",
                "tool_input": json.dumps({"command": command}, ensure_ascii=False),
            })
        elif codex_type == "item.completed" and item_type == "command_execution":
            command = item.get("command") or ""
            output = item.get("aggregated_output") or ""
            exit_code = item.get("exit_code")
            status = item.get("status") or "completed"
            summary = f"Command {status}"
            if exit_code is not None:
                summary += f" with exit code {exit_code}"
            if output:
                summary += f"\n{output}"
            event.update({
                "event_type": "tool_result",
                "role": "tool",
                "content": None,
                "tool_name": "Shell",
                "tool_input": json.dumps({"command": command}, ensure_ascii=False),
                "tool_output": output or summary,
                "is_error": bool(exit_code not in (None, 0)),
            })
        elif codex_type == "item.completed" and item_type == "reasoning":
            # Codex 的 reasoning summary → 与 claude 同形的 thinking 事件，
            # 前端复用现成的 thinking 折叠渲染
            text = item.get("text") or ""
            if not text:
                return None
            event.update({
                "event_type": "thinking",
                "role": "assistant",
                "content": text,
            })
        elif item_type in {"collab_agent_tool_call", "collabAgentToolCall"}:
            # app-server uses an item-local ``status=completed`` for Codex's
            # multi-agent tools.  It means this one wait/spawn/send call
            # finished, not that the parent turn or CCM Task completed.
            tool = str(item.get("tool") or "unknown")
            tool_key = re.sub(r"(?<!^)(?=[A-Z])", "_", tool).lower()
            tool_name = f"Agent.{tool_key}"
            receiver_thread_ids = (
                item.get("receiver_thread_ids")
                if "receiver_thread_ids" in item
                else item.get("receiverThreadIds")
            )
            sender_thread_id = (
                item.get("sender_thread_id")
                if "sender_thread_id" in item
                else item.get("senderThreadId")
            )
            reasoning_effort = (
                item.get("reasoning_effort")
                if "reasoning_effort" in item
                else item.get("reasoningEffort")
            )
            agents_states = (
                item.get("agents_states")
                if "agents_states" in item
                else item.get("agentsStates")
            )
            tool_args = {
                key: value
                for key, value in {
                    "sender_thread_id": sender_thread_id,
                    "receiver_thread_ids": receiver_thread_ids,
                    "prompt": item.get("prompt"),
                    "model": item.get("model"),
                    "reasoning_effort": reasoning_effort,
                }.items()
                if value not in (None, "", [], {})
            }
            tool_input = (
                json.dumps(tool_args, ensure_ascii=False)
                if tool_args else None
            )
            if codex_type == "item.started":
                event.update({
                    "event_type": "tool_use",
                    "role": "assistant",
                    "tool_name": tool_name,
                    "tool_input": tool_input,
                })
            else:
                status = str(item.get("status") or "completed")
                tool_result = {"status": status}
                if agents_states:
                    tool_result["agents_states"] = agents_states
                error = item.get("error")
                if error:
                    tool_result["error"] = error
                event.update({
                    "event_type": "tool_result",
                    "role": "tool",
                    "tool_name": tool_name,
                    "tool_input": tool_input,
                    "tool_output": json.dumps(
                        tool_result,
                        ensure_ascii=False,
                    ),
                    "is_error": (
                        status.lower() in {"failed", "error"}
                        or bool(error)
                    ),
                })
        elif item_type in {
            "sub_agent_activity",
            "subAgentActivity",
            "context_compaction",
            "contextCompaction",
        }:
            # Metadata-only item lifecycles have no chat payload. In
            # particular, their item/completed notification must not be
            # confused with the parent turn's terminal event.
            return None
        elif item_type == "file_change":
            # 实测（CLI 0.144.6 真实事件流）file_change 有 started + completed
            # 两态——源码注释声称 completed-only 不可信
            changes = item.get("changes") or []
            status = item.get("status") or "completed"
            tool_input = json.dumps({"changes": changes}, ensure_ascii=False)
            if codex_type == "item.started":
                event.update({
                    "event_type": "tool_use",
                    "role": "assistant",
                    "tool_name": "FileChange",
                    "tool_input": tool_input,
                })
            else:
                lines = [
                    f"{c.get('kind', 'update')} {c.get('path', '')}".strip()
                    for c in changes if isinstance(c, dict)
                ]
                summary = f"Patch {status}"
                if lines:
                    summary += "\n" + "\n".join(lines)
                event.update({
                    "event_type": "tool_result",
                    "role": "tool",
                    "tool_name": "FileChange",
                    "tool_input": tool_input,
                    "tool_output": summary,
                    "is_error": status == "failed",
                })
        elif item_type == "mcp_tool_call":
            server = item.get("server") or ""
            tool = item.get("tool") or ""
            name = f"{server}.{tool}".strip(".") or "mcp_tool_call"
            arguments = item.get("arguments")
            tool_input = (
                json.dumps(arguments, ensure_ascii=False)
                if isinstance(arguments, (dict, list)) else arguments
            )
            if codex_type == "item.started":
                event.update({
                    "event_type": "tool_use",
                    "role": "assistant",
                    "tool_name": name,
                    "tool_input": tool_input,
                })
            else:
                status = item.get("status") or "completed"
                result = item.get("result")
                error = item.get("error")
                if isinstance(result, (dict, list)):
                    result = json.dumps(result, ensure_ascii=False)
                if isinstance(error, (dict, list)):
                    error = json.dumps(error, ensure_ascii=False)
                event.update({
                    "event_type": "tool_result",
                    "role": "tool",
                    "tool_name": name,
                    "tool_input": tool_input,
                    "tool_output": error or result or f"MCP call {status}",
                    "is_error": status == "failed" or bool(error),
                })
        elif item_type == "web_search":
            query = item.get("query") or ""
            tool_input = json.dumps({"query": query}, ensure_ascii=False)
            if codex_type == "item.started":
                event.update({
                    "event_type": "tool_use",
                    "role": "assistant",
                    "tool_name": "WebSearch",
                    "tool_input": tool_input,
                })
            else:
                event.update({
                    "event_type": "tool_result",
                    "role": "tool",
                    "tool_name": "WebSearch",
                    "tool_input": tool_input,
                    "tool_output": f"Search completed: {query}",
                })
        elif item_type == "todo_list":
            items = item.get("items") or []
            lines = [
                f"{'✓' if it.get('completed') else '○'} {it.get('text', '')}"
                for it in items if isinstance(it, dict)
            ]
            event.update({
                "event_type": "system_event",
                "role": "assistant",
                "content": "Todo:\n" + "\n".join(lines) if lines else "Todo list updated",
            })
        elif item_type == "error":
            event.update({
                "event_type": "system_event",
                "content": str(item.get("message") or "codex error item"),
                "is_error": True,
            })
        elif codex_type == "turn.completed":
            usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
            event.update({
                "event_type": "system_event",
                "content": "turn.completed",
                "context_usage": self._codex_context_usage(usage) if usage else None,
            })
        elif codex_type == "background.lifecycle":
            event.update({
                "event_type": "background_lifecycle",
                "content": None,
                "background_state": data.get("state"),
                "background_reason": data.get("reason"),
                "background_active_count": data.get("active_count"),
                "background_active_thread_ids": data.get(
                    "active_thread_ids"
                ),
                "background_started_at": data.get("started_at"),
                "background_last_activity_at": data.get(
                    "last_activity_at"
                ),
            })
        elif "error" in codex_type.lower() or data.get("error"):
            # turn.failed 形如 {"type":"turn.failed","error":{"message":...}}（实测）
            message = data.get("message")
            err = data.get("error")
            if message is None and isinstance(err, dict):
                message = err.get("message") or err
            if message is None:
                message = err or codex_type
            if isinstance(message, (dict, list)):
                message = json.dumps(message, ensure_ascii=False)
            error_code = None
            error_details = None
            if isinstance(err, dict):
                error_code = (
                    err.get("codexErrorInfo")
                    or err.get("codex_error_info")
                    or err.get("code")
                )
                error_details = (
                    err.get("additionalDetails")
                    or err.get("additional_details")
                )
            event.update({
                "event_type": "system_event",
                "content": str(message),
                "is_error": True,
                "error_code": error_code,
                "error_details": error_details,
            })
        elif codex_type in {"item.started", "item.completed"} and item:
            # An item-local lifecycle status is never a parent-turn status.
            # Unsupported app-server items (for example dynamicToolCall or
            # imageGeneration) must be explicitly normalized before they can
            # become user-facing tool events.  Falling through and displaying
            # item.status used to turn their ``completed`` value into a false
            # chat separator that looked like the whole Task had completed.
            return None
        else:
            content = data.get("content") or data.get("message") or data.get("text")
            if content is None and item:
                content = item.get("text") or item.get("command")
            if isinstance(content, (dict, list)):
                content = json.dumps(content, ensure_ascii=False)
            # Skip events with no extractable content (heartbeats, metadata),
            # but keep events that carry a session_id
            session_id_present = bool(self._extract_codex_session_id(data))
            if not content and not session_id_present and codex_type not in ("item.started", "item.completed"):
                return None
            tool_input = data.get("tool_input") or data.get("input")
            tool_output = data.get("tool_output") or data.get("output")
            event.update({
                "event_type": "system_event",
                "role": data.get("role") or ("assistant" if "message" in codex_type else None),
                "content": content or codex_type,
                "tool_name": data.get("tool_name") or data.get("name"),
                "tool_input": json.dumps(tool_input, ensure_ascii=False) if isinstance(tool_input, (dict, list)) else tool_input,
                "tool_output": json.dumps(tool_output, ensure_ascii=False) if isinstance(tool_output, (dict, list)) else tool_output,
                "is_error": bool(data.get("is_error") or data.get("error") or "error" in codex_type.lower()),
            })

        session_id = self._extract_codex_session_id(data)
        if session_id:
            event["session_id"] = session_id
        if item.get("id"):
            event["item_id"] = item["id"]
        return event

    def _base_codex_event(self, line: str, timestamp: str) -> dict:
        return {
            "event_type": "system_event",
            "role": None,
            "content": None,
            "tool_name": None,
            "tool_input": None,
            "tool_output": None,
            "raw_json": line,
            "is_error": False,
            "timestamp": timestamp,
        }

    def _extract_codex_session_id(self, data: dict) -> str | None:
        session_id = (
            data.get("session_id")
            or data.get("sessionId")
            or data.get("conversation_id")
            or data.get("thread_id")
        )
        if not session_id and isinstance(data.get("session"), dict):
            session_id = data["session"].get("id")
        if not session_id and isinstance(data.get("thread"), dict):
            session_id = data["thread"].get("id")
        return session_id

    def _codex_context_usage(self, usage: dict) -> dict:
        input_tokens = int(usage.get("input_tokens") or 0)
        cached_tokens = int(usage.get("cached_input_tokens") or 0)
        output_tokens = int(usage.get("output_tokens") or 0)
        reasoning_tokens = int(usage.get("reasoning_output_tokens") or 0)
        total_tokens = int(
            usage.get("total_tokens") or (input_tokens + output_tokens)
        )
        result = {
            "input_tokens": max(input_tokens - cached_tokens, 0),
            "cache_read_input_tokens": cached_tokens,
            "cache_creation_input_tokens": 0,
            "output_tokens": output_tokens,
            "reasoning_output_tokens": reasoning_tokens,
            "total_input_tokens": input_tokens,
            "total_tokens": total_tokens,
            "context_tokens": max(total_tokens - reasoning_tokens, 0),
        }
        context_window = int(usage.get("context_window") or 0)
        if context_window:
            result["context_window"] = context_window
        return result

    @staticmethod
    def _fatal_provider_error_for_event(event: dict) -> str | None:
        """Return a turn-fatal provider error without matching tool failures."""

        if (
            not event.get("is_error")
            or event.get("orphan")
            or event.get("autonomous")
        ):
            return None

        event_type = str(event.get("event_type") or "")
        content = str(event.get("content") or "").strip()
        raw = event.get("raw_json")
        parsed = None
        if raw:
            try:
                parsed = json.loads(raw) if isinstance(raw, str) else raw
            except (TypeError, ValueError):
                parsed = None
        if (
            isinstance(parsed, dict)
            and parsed.get("isApiErrorMessage")
            and event_type in {"message", "result"}
        ):
            return content or "Claude API request failed"
        if (
            isinstance(parsed, dict)
            and parsed.get("type") == "turn.failed"
            and event_type == "system_event"
        ):
            # Both ``codex exec --json`` and the app-server adapter emit this
            # exact terminal shape. Preserve its real provider message and
            # prevent consumer cleanup from appending a misleading generic
            # process-exit notice.
            return content or "Codex turn failed"
        if event_type in {"result", "session_crashed"}:
            return content or "Claude turn failed"
        if event_type in {"message", "result"}:
            from backend.services.claude_pool import is_auth_failure

            if is_auth_failure(content):
                return content or "Claude authentication failed"
        if event_type == "rate_limit_event":
            return content or "Claude API rate limit"
        if event_type == "system_event" and (
            content.startswith("api_error:")
            or content.startswith("Response timed out")
        ):
            return content
        return None

    @staticmethod
    def _suppress_duplicate_claude_result(
        event: dict,
        record: _OutputConsumerRecord | None,
        provider: str,
    ) -> dict:
        """Remove only an exact successful Claude assistant/result repeat."""

        if (
            provider != "claude"
            or record is None
            or event.get("role") != "assistant"
            or event.get("event_type") not in {"message", "result"}
            or event.get("is_error")
            or event.get("orphan")
            or event.get("autonomous")
        ):
            return event
        content = event.get("content")
        if not isinstance(content, str) or not content.strip():
            return event
        normalized = content.replace("\r\n", "\n").strip()
        if event["event_type"] == "message":
            object.__setattr__(record, "last_claude_assistant_text", normalized)
            return event
        if normalized != record.last_claude_assistant_text:
            return event
        suppressed = dict(event)
        suppressed["content"] = None
        suppressed["duplicate_of_assistant"] = True
        return suppressed

    async def _process_event(
        self,
        instance_id: int,
        task_id: int | None,
        event: dict,
        loop_iteration: int | None = None,
        *,
        consumer_record: _OutputConsumerRecord | None = None,
        detached_autonomous: bool = False,
        background_followup: bool = False,
        expected_session_id: str | None = None,
        expected_background_generation: str | None = None,
        expected_task_retry_count: int | None = None,
        expected_task_turn_generation: int | None = None,
    ):
        """Process a single parsed event: save to DB and broadcast."""
        provider = str(
            self._launch_params.get(instance_id, {}).get("provider") or "claude"
        ).lower()
        # Subprocess consumers pass their immutable record explicitly. PTY
        # callbacks run through the currently registered record instead. This
        # distinction lets an old subprocess that was replaced mid-callback be
        # rejected rather than borrowing the replacement turn's identity.
        explicit_consumer_record = consumer_record is not None
        event_record = (
            consumer_record
            if explicit_consumer_record
            else (
                None
                if detached_autonomous
                else self._consumer_records.get(instance_id)
            )
        )
        if event_record is not None:
            provider = str(event_record.provider or provider).lower()
        background_scoped = detached_autonomous or background_followup

        def owns_event_generation() -> bool:
            if event_record is None:
                return False
            ordinary_owner = (
                event_record.task_id == task_id
                and (
                    task_id is None
                    or (
                        event_record.task_retry_count is not None
                        and event_record.task_turn_generation is not None
                    )
                )
                and event_record.instance_started_at is not None
                and self._consumer_records.get(instance_id) is event_record
                and self._tasks.get(instance_id) is event_record.task
                and self.processes.get(instance_id) is event_record.process
            )
            if ordinary_owner:
                return True
            if not background_followup:
                return False
            if (
                task_id is None
                or expected_session_id is None
                or expected_background_generation is None
                or expected_task_retry_count is None
                or expected_task_turn_generation is None
            ):
                return False
            proof = self._pty_post_exit_generations.get(
                (task_id, expected_session_id)
            )
            if proof is None or proof.record is not event_record:
                return False
            return self._pty_post_exit_generation_is_current(
                proof,
                instance_id=instance_id,
                task_id=task_id,
                session_id=expected_session_id,
                task_retry_count=expected_task_retry_count,
                task_turn_generation=expected_task_turn_generation,
                background_generation=expected_background_generation,
                require_background_state=True,
            )

        def task_event_predicates() -> list:
            predicates = [
                Task.id == task_id,
                task_retry_not_superseded_predicate(),
            ]
            if background_scoped:
                predicates.extend(
                    [
                        Task.session_id == expected_session_id,
                        Task.retry_count == expected_task_retry_count,
                        Task.turn_generation
                        == expected_task_turn_generation,
                        Task.pty_background_generation
                        == expected_background_generation,
                        worker_task_runtime_persistence_predicate(
                            task_retry_count=expected_task_retry_count,
                            task_turn_generation=(
                                expected_task_turn_generation
                            ),
                            session_id=expected_session_id,
                            pty_background_generation=(
                                expected_background_generation
                            ),
                        ),
                    ]
                )
            elif event_record is not None:
                predicates.extend(
                    [
                        Task.instance_id == instance_id,
                        Task.retry_count == event_record.task_retry_count,
                        Task.turn_generation
                        == event_record.task_turn_generation,
                        worker_task_runtime_persistence_predicate(
                            task_retry_count=event_record.task_retry_count,
                            task_turn_generation=(
                                event_record.task_turn_generation
                            ),
                            instance_id=instance_id,
                        ),
                    ]
                )
            else:
                predicates.append(
                    no_active_worker_task_termination_predicate()
                )
            return predicates

        async def guard_managed_event_generation(db) -> bool:
            """Lock the exact durable Task→Instance event generation."""

            if background_scoped:
                if (
                    task_id is None
                    or expected_session_id is None
                    or expected_background_generation is None
                    or expected_task_retry_count is None
                    or expected_task_turn_generation is None
                ):
                    return False
                if background_followup and not owns_event_generation():
                    return False
                task_guard = await db.execute(
                    update(Task)
                    .where(
                        *task_event_predicates(),
                        Task.status.in_(
                            ("in_progress", "executing", "completed")
                        ),
                    )
                    .values(status=Task.status)
                )
                return bool(
                    task_guard.rowcount
                    and (
                        not background_followup
                        or owns_event_generation()
                    )
                )
            if event_record is None:
                return True
            if not owns_event_generation():
                return False
            if task_id is not None:
                task_guard = await db.execute(
                    update(Task)
                    .where(
                        *task_event_predicates(),
                        Task.status.in_(
                            ("in_progress", "executing", "completed")
                        ),
                    )
                    .values(status=Task.status)
                )
                if not task_guard.rowcount:
                    return False
            instance_guard = await db.execute(
                update(Instance)
                .where(
                    Instance.id == instance_id,
                    Instance.status == "running",
                    Instance.pid
                    == (getattr(event_record.process, "pid", 0) or 0),
                    (
                        Instance.current_task_id.is_(None)
                        if task_id is None
                        else Instance.current_task_id == task_id
                    ),
                    Instance.started_at
                    == event_record.instance_started_at,
                )
                .values(status="running")
            )
            return bool(
                instance_guard.rowcount and owns_event_generation()
            )

        if explicit_consumer_record and not owns_event_generation():
            logger.info(
                "Dropping stale event for instance %s task %s because its "
                "consumer generation was replaced",
                instance_id,
                task_id,
            )
            return
        fatal_provider_error = self._fatal_provider_error_for_event(event)
        if (
            fatal_provider_error
            and event_record is not None
            and not background_followup
        ):
            # Keep the first (usually detailed upstream) message. A later
            # synthetic "api_error: turn aborted" marker must not replace it.
            if event_record.fatal_provider_error is None:
                object.__setattr__(
                    event_record,
                    "fatal_provider_error",
                    fatal_provider_error[:2000],
                )
        # Extract session_id, cost, and context usage from event
        session_id = event.pop("session_id", None)
        cost_usd = event.pop("cost_usd", None)
        context_usage = event.pop("context_usage", None)

        # Autonomous-turn user records are the harness's own wake-up inputs,
        # not fresh user messages. Mirroring them verbatim is the historical
        # "stale prompt replay" that once forced on_exit to mute the autonomous
        # callback entirely (claude_pty 412d911)。<task-notification> 压成一行
        # system_event 说明会话为何自己动了；channel 回显等其余 user 记录在
        # 发送时已入库过，直接丢弃。
        if event.get("autonomous") and event.get("role") == "user":
            content = event.get("content") or ""
            if "<task-notification>" not in content:
                return
            m_tid = re.search(r"<task-id>([^<]*)</task-id>", content)
            m_status = re.search(r"<status>([^<]*)</status>", content)
            label = m_tid.group(1) if m_tid else "?"
            status = f"（{m_status.group(1)}）" if m_status else ""
            event = {
                "event_type": "system_event",
                "role": "system",
                "content": f"⏰ 后台任务 {label} 回报{status}，会话自主处理中",
                "autonomous": True,
            }

        # App-server deltas are intentionally live-only: persisting every token
        # would recreate the raw-json/DB amplification that this path is meant
        # to avoid.  The final item/completed event is still stored normally.
        if event.get("event_type") in ("message_delta", "thinking_delta"):
            # A live-only delta still needs the same durable generation fence
            # as a persisted final item.  In-memory consumer identity alone is
            # insufficient: a retry/new turn may already have committed while
            # the old callback is still unwinding.  Keep the no-op Task→Instance
            # row locks through publication so that a generation transition
            # cannot commit between the check and the external WS side effect.
            # Missing exact foreground identity is deliberately fail-closed.
            if not detached_autonomous and event_record is None:
                logger.info(
                    "Dropping unscoped foreground delta for instance %s task %s",
                    instance_id,
                    task_id,
                )
                return
            broadcast_data = {k: v for k, v in event.items() if k != "raw_json"}
            if background_scoped:
                broadcast_data["task_retry_count"] = (
                    expected_task_retry_count
                )
                broadcast_data["task_turn_generation"] = (
                    expected_task_turn_generation
                )
            elif event_record is not None:
                if not owns_event_generation():
                    return
                broadcast_data["task_retry_count"] = (
                    event_record.task_retry_count
                )
                broadcast_data["task_turn_generation"] = (
                    event_record.task_turn_generation
                )
                native_turn_id = getattr(
                    event_record.process,
                    "native_turn_id",
                    None,
                )
                if native_turn_id:
                    broadcast_data["native_turn_id"] = str(native_turn_id)
            if loop_iteration is not None:
                broadcast_data["loop_iteration"] = loop_iteration
            async with self.db_factory() as db:
                if not await _fence_worker_runtime_mutation(
                    db,
                    producer="PTY streaming event",
                ):
                    return
                if not await guard_managed_event_generation(db):
                    await db.rollback()
                    logger.info(
                        "Dropping stale %s delta for task %s on instance %s",
                        "background" if background_scoped else "foreground",
                        task_id,
                        instance_id,
                    )
                    return
                if not background_scoped:
                    await self.broadcaster.broadcast(
                        f"instance:{instance_id}", broadcast_data
                    )
                if task_id:
                    await self.broadcaster.broadcast(
                        f"task:{task_id}", broadcast_data
                    )
                await db.commit()
            return

        # A foreground turn can still produce output after another callback
        # prematurely marked it completed. Reactivate only while the exact
        # durable Task/Instance/process generation is still live. A plain
        # ``Task.id + completed`` write here used to let a late event revive a
        # retry or a PR-review Task that synchronize had permanently
        # superseded.
        if (
            task_id
            and event.get("role") == "assistant"
            and event["event_type"] in ("message", "tool_use")
            and not event.get("orphan")
            and not event.get("autonomous")
            and not background_followup
            and owns_event_generation()
        ):
            reactivated_completed_at: datetime | None = None
            async with self.db_factory() as db:
                if not await _fence_worker_runtime_mutation(
                    db,
                    producer="PTY Task reactivation",
                ):
                    return
                # First acquire the exact Task write barrier without changing
                # status.  A routing stage that wins before this point leaves
                # a durable marker which late output must never cross.
                task_reactivated = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task_id,
                        Task.status == "completed",
                        Task.instance_id == instance_id,
                        Task.retry_count == event_record.task_retry_count,
                        Task.turn_generation
                        == event_record.task_turn_generation,
                        task_retry_not_superseded_predicate(),
                        worker_task_runtime_persistence_predicate(
                            task_retry_count=event_record.task_retry_count,
                            task_turn_generation=(
                                event_record.task_turn_generation
                            ),
                            instance_id=instance_id,
                        ),
                    )
                    .values(status=Task.status)
                )
                if task_reactivated.rowcount:
                    current_task = await db.get(
                        Task,
                        task_id,
                        populate_existing=True,
                    )
                    if (
                        current_task is None
                        or has_pending_worker_routing(current_task)
                    ):
                        await db.rollback()
                        task_reactivated = None
                    else:
                        instance_guard = await db.execute(
                            update(Instance)
                            .where(
                                Instance.id == instance_id,
                                Instance.status == "running",
                                Instance.pid
                                == (
                                    getattr(
                                        event_record.process,
                                        "pid",
                                        0,
                                    )
                                    or 0
                                ),
                                Instance.current_task_id == task_id,
                                Instance.started_at
                                == event_record.instance_started_at,
                            )
                            .values(status="running")
                        )
                        if (
                            not instance_guard.rowcount
                            or not owns_event_generation()
                        ):
                            await db.rollback()
                            task_reactivated = None
                        else:
                            reactivated_completed_at = (
                                current_task.completed_at
                            )
                            current_task.status = "executing"
                            await db.commit()
                else:
                    await db.rollback()

            if task_reactivated is not None and task_reactivated.rowcount:
                # Fence publication as well: retry/supersede must acquire the
                # same Task row lock, so an old executing event cannot cross a
                # newer generation.
                async with self.db_factory() as db:
                    if not await _fence_worker_runtime_mutation(
                        db,
                        producer="PTY Task reactivation publication",
                    ):
                        return
                    publish_task_guard = await db.execute(
                        update(Task)
                        .where(
                            Task.id == task_id,
                            Task.status == "executing",
                            Task.instance_id == instance_id,
                            Task.retry_count == event_record.task_retry_count,
                            Task.turn_generation
                            == event_record.task_turn_generation,
                            (
                                Task.completed_at.is_(None)
                                if reactivated_completed_at is None
                                else Task.completed_at
                                == reactivated_completed_at
                            ),
                            task_retry_not_superseded_predicate(),
                            worker_task_runtime_persistence_predicate(
                                task_retry_count=(
                                    event_record.task_retry_count
                                ),
                                task_turn_generation=(
                                    event_record.task_turn_generation
                                ),
                                instance_id=instance_id,
                            ),
                        )
                        .values(status="executing")
                    )
                    publish_instance_guard = None
                    if publish_task_guard.rowcount:
                        publish_instance_guard = await db.execute(
                            update(Instance)
                            .where(
                                Instance.id == instance_id,
                                Instance.status == "running",
                                Instance.pid
                                == (
                                    getattr(event_record.process, "pid", 0)
                                    or 0
                                ),
                                Instance.current_task_id == task_id,
                                Instance.started_at
                                == event_record.instance_started_at,
                            )
                            .values(status="running")
                        )
                    if (
                        publish_task_guard.rowcount
                        and publish_instance_guard is not None
                        and publish_instance_guard.rowcount
                        and owns_event_generation()
                    ):
                        await self.broadcaster.broadcast("tasks", {
                            "event": "status_change",
                            "task_id": task_id,
                            "task_retry_count": event_record.task_retry_count,
                            "task_turn_generation": (
                                event_record.task_turn_generation
                            ),
                            "new_status": "executing",
                        })
                    await db.commit()

        # Skip streaming text fragments (e.g. "court" from Opus 4.8 encrypted
        # thinking). These are tiny text chunks emitted before a tool_use block
        # with no stop_reason — not real assistant replies.
        if (
            event["event_type"] == "message"
            and event.get("role") == "assistant"
            and event.get("content")
            and len(event["content"]) < 5
        ):
            raw = event.get("raw_json")
            if raw:
                import json as _json
                try:
                    parsed = _json.loads(raw) if isinstance(raw, str) else raw
                    # This filter is specific to Claude stream-json envelopes.
                    # Codex item.completed events have no top-level ``message``
                    # object, and short answers such as "OK" are complete replies.
                    message = parsed.get("message")
                    if isinstance(message, dict) and not message.get("stop_reason"):
                        logger.debug("Dropping streaming fragment: %r", event["content"])
                        return
                except (ValueError, TypeError):
                    pass

        event = self._suppress_duplicate_claude_result(
            event,
            event_record,
            provider,
        )
        protocol_anomaly = detect_assistant_protocol_anomaly(
            event.get("event_type"),
            event.get("role"),
            event.get("content"),
            provider=provider,
        )
        if protocol_anomaly:
            event["protocol_anomaly"] = protocol_anomaly
            logger.warning(
                "Assistant protocol anomaly %s on instance %s task %s "
                "(provider=%s); preserving it as inert text",
                protocol_anomaly,
                instance_id,
                task_id,
                provider,
            )
        else:
            # Adapter or relay metadata cannot assert a protocol anomaly that
            # is supported only by a now-suppressed or non-canonical payload.
            event.pop("protocol_anomaly", None)
        context_preflight_relay_proof: dict[str, object] | None = None

        # Store the event and related heartbeat/session/unread updates in one
        # transaction.  The old path committed 2-4 times per Codex event,
        # serializing the stream behind SQLite fsyncs before WebSocket delivery.
        # When both ownership rows are touched, preserve the global
        # Task -> Instance lock order used by lifecycle transactions.
        async with self.db_factory() as db:
            if not await _fence_worker_runtime_mutation(
                db,
                producer="PTY durable event",
            ):
                return
            if not await guard_managed_event_generation(db):
                await db.rollback()
                logger.info(
                    "Dropping stale durable event for instance %s task %s",
                    instance_id,
                    task_id,
                )
                return
            persisted_task_retry_count = None
            persisted_task_turn_generation = None
            if task_id is not None:
                if event_record is not None:
                    persisted_task_retry_count = (
                        event_record.task_retry_count
                    )
                    persisted_task_turn_generation = (
                        event_record.task_turn_generation
                    )
                elif detached_autonomous:
                    persisted_task_retry_count = (
                        await db.execute(
                            select(Task.retry_count).where(
                                *task_event_predicates()
                            )
                        )
                    ).scalar_one_or_none()
                    if persisted_task_retry_count is None:
                        await db.rollback()
                        logger.info(
                            "Dropping autonomous event without an exact retry "
                            "generation for task %s",
                            task_id,
                        )
                        return
                    persisted_task_turn_generation = (
                        expected_task_turn_generation
                    )
                    if persisted_task_turn_generation is None:
                        await db.rollback()
                        logger.info(
                            "Dropping autonomous event without an exact turn "
                            "generation for task %s",
                            task_id,
                        )
                        return
            raw_payload = event.get("raw_json")
            parsed_raw = None
            if isinstance(raw_payload, dict):
                parsed_raw = raw_payload
            elif isinstance(raw_payload, str) and raw_payload:
                try:
                    parsed_raw = json.loads(raw_payload)
                except (TypeError, ValueError):
                    parsed_raw = None
            native_turn_id = event.get("turn_id") or event.get("turnId")
            if not native_turn_id and isinstance(parsed_raw, dict):
                raw_turn = parsed_raw.get("turn")
                native_turn_id = (
                    parsed_raw.get("turn_id")
                    or parsed_raw.get("turnId")
                    or (
                        raw_turn.get("id")
                        if isinstance(raw_turn, dict)
                        else None
                    )
                )
            if not native_turn_id and event_record is not None:
                native_turn_id = getattr(
                    event_record.process,
                    "native_turn_id",
                    None,
                )
            # Scope is durable arbitration evidence, not a presentation hint.
            # Persist it at the one event-ingest boundary after autonomous user
            # sanitization, while keeping live-only deltas out of the database.
            # ``orphan`` wins over ``autonomous`` when an upstream replay marks
            # both, so stale backlog can never become terminal evidence.
            from backend.services.terminal_arbitration import (
                classify_turn_scope,
            )

            turn_scope = (
                classify_turn_scope(
                    event,
                    detached_autonomous=detached_autonomous,
                )
                if task_id is not None
                else None
            )
            if (
                settings.ccm_node_role == "worker"
                and
                str(provider or "").strip().lower() == "codex"
                and task_id is not None
                and turn_scope == "foreground"
                and type(persisted_task_retry_count) is int
                and type(persisted_task_turn_generation) is int
            ):
                source_identity = (
                    await db.execute(
                        select(
                            Task.turn_source_log_id,
                            LogEntry.actual_transport,
                            Task.metadata_,
                        )
                        .join(
                            LogEntry,
                            LogEntry.id == Task.turn_source_log_id,
                        )
                        .where(*task_event_predicates())
                    )
                ).one_or_none()
                if source_identity is not None:
                    from backend.services.skill_context import (
                        is_worker_managed_task_metadata,
                    )
                    from backend.services.worker_launch_admission import (
                        build_codex_context_preflight_relay_proof,
                    )

                    if is_worker_managed_task_metadata(
                        source_identity[2]
                    ):
                        context_preflight_relay_proof = (
                            build_codex_context_preflight_relay_proof(
                                raw_payload,
                                event,
                                retry_count=persisted_task_retry_count,
                                turn_generation=(
                                    persisted_task_turn_generation
                                ),
                                source_log_id=source_identity[0],
                                actual_transport=source_identity[1],
                            )
                        )
            entry = LogEntry(
                instance_id=instance_id,
                task_id=task_id,
                task_retry_count=persisted_task_retry_count,
                task_turn_generation=persisted_task_turn_generation,
                native_turn_id=(
                    str(native_turn_id) if native_turn_id else None
                ),
                turn_scope=turn_scope,
                event_type=event["event_type"],
                role=event.get("role"),
                content=event.get("content"),
                tool_name=event.get("tool_name"),
                tool_input=event.get("tool_input"),
                tool_output=event.get("tool_output"),
                raw_json=event.get("raw_json"),
                is_error=event.get("is_error", False),
                loop_iteration=loop_iteration,
            )
            db.add(entry)
            if task_id:
                task_values = {}
                if session_id and not detached_autonomous:
                    task_values["session_id"] = session_id
                if (
                    event.get("role") == "assistant"
                    and event["event_type"] in ("message", "result")
                    and event.get("content")
                ):
                    task_values["has_unread"] = True
                if task_values:
                    await db.execute(
                        update(Task)
                        .where(*task_event_predicates())
                        .values(**task_values)
                    )
            if not background_scoped:
                instance_values = {"last_heartbeat": datetime.utcnow()}
                if cost_usd is not None:
                    instance_values["total_cost_usd"] = cost_usd
                await db.execute(
                    update(Instance)
                    .where(Instance.id == instance_id)
                    .values(**instance_values)
                )
            if event_record is not None and not owns_event_generation():
                await db.rollback()
                logger.info(
                    "Dropping event for replaced consumer generation on "
                    "instance %s task %s",
                    instance_id,
                    task_id,
                )
                return
            await db.commit()

        # Native sub-agent lifecycle (model-spawned Agent/Monitor, observed by
        # the active provider adapter) — register only after the exact parent event
        # generation committed, so a late old callback cannot create lifecycle
        # state under a replacement turn.
        if (
            task_id
            and event.get("subagent")
            and event["event_type"].startswith("subagent_")
        ):
            try:
                await self._upsert_native_sub_agent(
                    task_id,
                    event["event_type"],
                    event["subagent"],
                    task_retry_count=entry.task_retry_count,
                    task_turn_generation=entry.task_turn_generation,
                )
            except Exception:
                logger.exception(
                    "Failed to upsert native sub-agent for task %s", task_id
                )

        # Per-turn transient-overload detection: a server-side 429/overload
        # surfaces as an is_error message ("Server is temporarily limiting
        # requests (not your usage limit)" / overloaded). Flag it so the host
        # can wait + retry even in PTY mode (where the aborted turn still
        # reports exit_code 0).
        #
        # Only the CURRENT foreground turn's own events count. `orphan` events
        # are stale backlog from a previous turn — on resume PTY re-reads the
        # JSONL and replays the very api_error that triggered THIS retry — and
        # `autonomous` events belong to background sub-agent turns. Flagging
        # either keeps transient_error_seen() True across a clean resume, so the
        # host "retries" a turn that already succeeded and finally marks the
        # task failed (the recover-then-failed bug). See PROGRESS.md.
        if (
            event.get("is_error")
            and not event.get("orphan")
            and not event.get("autonomous")
            and not background_followup
        ):
            from backend.services.claude_pool import is_transient_for
            event_content = event.get("content") or ""
            if (
                is_transient_for(provider, event_content)
                or self.is_cloudrouter_transient(
                    instance_id, provider, event_content
                )
            ):
                self._transient_seen.add(instance_id)

        # PTY rate-limit detection: actionable rate_limit_event during this turn
        if (
            provider == "claude"
            and event.get("event_type") == "rate_limit_event"
            and not event.get("orphan")
            and not event.get("autonomous")
            and not background_followup
        ):
            from backend.services.claude_pool import rate_limit_event_is_actionable
            info = event.get("rate_limit_info")
            if info is None:
                raw = event.get("raw_json")
                if raw:
                    import json as _json
                    try:
                        info = (_json.loads(raw) if isinstance(raw, str) else raw).get("rate_limit_info")
                    except (ValueError, TypeError):
                        info = None
            if rate_limit_event_is_actionable(info):
                self._pty_rate_limit_seen.add(instance_id)
                if isinstance(info, dict):
                    self._pty_rate_limit_info[instance_id] = info

        # PTY rate-limit detection from assistant text: CC outputs messages like
        # "You've hit your session limit" as plain assistant text, not as a
        # rate_limit_event. In PTY mode the process stays alive so
        # _check_rate_limit_and_rotate (which needs exit_code != 0) never fires.
        if (
            provider == "claude"
            and event.get("role") == "assistant"
            and event.get("event_type") in ("message", "result")
            and not event.get("orphan")
            and not event.get("autonomous")
            and not background_followup
        ):
            content = event.get("content") or ""
            if content:
                from backend.services.claude_pool import is_rate_limited
                if is_rate_limited(content):
                    self._pty_rate_limit_seen.add(instance_id)
                    self._pty_rate_limit_info[instance_id] = {"hard_limit": True}
                    logger.info("PTY rate limit detected from assistant text (instance %s): %s",
                                instance_id, content[:120])

        # Track last tool_use name for evolution (tool_result may not carry tool_name)
        if event["event_type"] == "tool_use" and event.get("tool_name"):
            self._last_tool_name = event["tool_name"]

        # Skill evolution: learn from tool failures
        if (
            task_id
            and event["event_type"] == "tool_result"
            and event.get("is_error")
        ):
            failed_tool = event.get("tool_name") or getattr(self, "_last_tool_name", None)
            if failed_tool:
                try:
                    from backend.services.skill_evolution import evolve_on_failure
                    async with self.db_factory() as db:
                        if not await _fence_worker_runtime_admission(
                            db,
                            producer="PTY skill-evolution event",
                        ):
                            return
                        await evolve_on_failure(
                            tool_name=failed_tool,
                            error=str(event.get("content") or event.get("tool_output", ""))[:500],
                            context=str(event.get("tool_input", ""))[:300],
                            db=db,
                        )
                except Exception:
                    logger.debug("skill evolution failed", exc_info=True)

        # Broadcast via WebSocket
        broadcast_data = {k: v for k, v in event.items() if k != "raw_json"}
        broadcast_data.update(
            id=entry.id,
            instance_id=instance_id,
            task_id=task_id,
            timestamp=(entry.timestamp or datetime.utcnow()).isoformat(),
            # Relay consumers must receive the committed arbitration evidence,
            # never same-named fields supplied by an upstream provider event.
            turn_scope=entry.turn_scope,
            actual_transport=entry.actual_transport,
        )
        if entry.task_retry_count is not None:
            broadcast_data["task_retry_count"] = entry.task_retry_count
        if entry.task_turn_generation is not None:
            broadcast_data["task_turn_generation"] = (
                entry.task_turn_generation
            )
        if entry.native_turn_id is not None:
            broadcast_data["native_turn_id"] = entry.native_turn_id
        if loop_iteration is not None:
            broadcast_data["loop_iteration"] = loop_iteration
        if not background_scoped:
            await self.broadcaster.broadcast(
                f"instance:{instance_id}", broadcast_data
            )
        if task_id:
            if context_preflight_relay_proof is not None:
                from backend.services.worker_launch_admission import (
                    WORKER_CONTEXT_PREFLIGHT_PROOF_KEY,
                )

                broadcast_data[WORKER_CONTEXT_PREFLIGHT_PROOF_KEY] = (
                    context_preflight_relay_proof
                )
            await self.broadcaster.broadcast(f"task:{task_id}", broadcast_data)

        # Persist and broadcast context usage
        if (
            event_record is not None
            and not owns_event_generation()
        ):
            context_usage = None
        if detached_autonomous:
            # The reusable Instance key may already belong to another turn.
            # Without the old session id in every upstream usage envelope,
            # persisting this value could overwrite the replacement's context.
            context_usage = None
        elif context_usage and "total_input_tokens" not in context_usage:
            # Window-only refinement (result events carry just the
            # authoritative contextWindow — their usage numbers are cumulative
            # and unusable). Merge into the stored per-request usage.
            window = context_usage.get("context_window")
            context_usage = None
            if window and task_id:
                async with self.db_factory() as db:
                    t = (
                        await db.execute(
                            select(Task).where(*task_event_predicates())
                        )
                    ).scalar_one_or_none()
                    stored = dict(t.context_window_usage) if (t and t.context_window_usage) else None
                    model_name = (t.model or "") if t else ""
                # modelUsage may underreport large-context models as 200K.
                from backend.services.claude_models import claude_context_window

                window = max(window, claude_context_window(model_name))
                if stored and stored.get("context_window") != window:
                    stored["context_window"] = window
                    context_usage = stored
        elif context_usage and not context_usage.get("context_window"):
            # Per-request usage without a window (PTY interactive mode and -p
            # assistant events; codex turn.completed usage). Fill from the
            # Fill from the provider-specific model capability table.
            model_name = ""
            task_provider = "claude"
            task_session_id = None
            if task_id:
                async with self.db_factory() as db:
                    t = (
                        await db.execute(
                            select(Task).where(*task_event_predicates())
                        )
                    ).scalar_one_or_none()
                    model_name = (t.model or "") if t else ""
                    task_provider = ((t.provider if t else None) or "claude").lower()
                    task_session_id = t.session_id if t else None
            if task_provider == "codex":
                # ``codex exec --json`` exposes cumulative turn usage, which
                # can exceed the model window many times during a tool-heavy
                # turn. Its rollout contains the authoritative latest request
                # usage used by Codex's own context meter.
                rollout_usage = None
                if task_session_id:
                    rollout_path = None
                    codex_home = self._config_dirs.get(instance_id)
                    if codex_home:
                        from backend.services.codex_session_migration import (
                            CodexSessionMigrationError,
                            find_codex_rollout_session,
                        )

                        try:
                            rollout_path = find_codex_rollout_session(
                                task_session_id,
                                codex_home,
                            )
                        except CodexSessionMigrationError:
                            logger.debug(
                                "Could not resolve current-home rollout for "
                                "Codex context usage",
                                exc_info=True,
                            )
                    if rollout_path is None:
                        from backend.api.tasks import _find_session_jsonl

                        rollout_path = _find_session_jsonl(
                            task_session_id,
                            provider="codex",
                        )
                    if rollout_path:
                        rollout_usage = await asyncio.to_thread(
                            read_codex_rollout_last_usage,
                            rollout_path,
                        )
                if rollout_usage:
                    context_usage = self._codex_context_usage(rollout_usage)
                else:
                    from backend.services.codex_models import codex_context_window
                    context_usage["context_window"] = codex_context_window(
                        model_name
                    )
            else:
                from backend.services.claude_models import claude_context_window

                context_usage["context_window"] = claude_context_window(model_name)
        if context_usage and task_id:
            async with self.db_factory() as db:
                if not await _fence_worker_runtime_mutation(
                    db,
                    producer="PTY context-usage update",
                ):
                    return
                context_updated = await db.execute(
                    update(Task)
                    .where(*task_event_predicates())
                    .values(context_window_usage=context_usage)
                )
                if (
                    event_record is not None
                    and not owns_event_generation()
                ):
                    await db.rollback()
                    context_updated = None
                else:
                    await db.commit()
            if context_updated is not None and context_updated.rowcount:
                await self.broadcaster.broadcast(
                    f"task:{task_id}",
                    {
                        "event_type": "context_usage",
                        "task_retry_count": entry.task_retry_count,
                        "task_turn_generation": (
                            entry.task_turn_generation
                        ),
                        **context_usage,
                    },
                )

    async def _upsert_native_sub_agent(
        self,
        task_id: int,
        event_type: str,
        info: dict,
        *,
        task_retry_count: int | None,
        task_turn_generation: int | None,
    ) -> None:
        """Mirror a native sub-agent lifecycle event into sub_agent_sessions.

        Claude PTY events retain their historical ``tool_use_id`` identity.
        Codex app-server events use the native child thread plus the exact
        Task retry/turn generation, with a monotonic provider sequence stored
        in meta.  This prevents replay, stale-turn aliasing, and thread reuse
        from corrupting the generic SubAgentSession projection.
        """
        import json as _json
        from sqlalchemy import func as _func, select as _select
        from backend.models.sub_agent import SubAgentReport, SubAgentSession

        if (
            type(task_retry_count) is not int
            or type(task_turn_generation) is not int
        ):
            return

        tool_use_id = info.get("tool_use_id")
        if not isinstance(tool_use_id, str) or not tool_use_id:
            return

        provider = info.get("provider") or "claude"
        is_codex = provider == "codex"
        native_agent_id = info.get("native_agent_id")
        sequence = info.get("sequence")
        requested_status = info.get("status")
        if is_codex and (
            not isinstance(native_agent_id, str)
            or not native_agent_id
            or native_agent_id != native_agent_id.strip()
            or len(native_agent_id) > 255
            or type(sequence) is not int
            or sequence <= 0
        ):
            return
        if is_codex and event_type == "subagent_done":
            if requested_status not in {"completed", "failed", "cancelled"}:
                return
        elif is_codex and requested_status != "running":
            return

        terminal_status = (
            requested_status
            if is_codex and event_type == "subagent_done"
            else "completed"
        )
        broadcasts: list[dict[str, Any]] = []
        active_count: int | None = None

        async with self.db_factory() as db:
            node_fence = (
                _fence_worker_runtime_admission
                if event_type == "subagent_spawn"
                else _fence_worker_runtime_mutation
            )
            if not await node_fence(db, producer="native sub-agent event"):
                return
            termination_fence = (
                no_active_worker_task_termination_predicate()
                if event_type == "subagent_spawn"
                else worker_task_runtime_persistence_predicate(
                    task_retry_count=task_retry_count,
                    task_turn_generation=task_turn_generation,
                )
            )
            generation_guard = await db.execute(
                update(Task)
                .where(
                    Task.id == task_id,
                    Task.retry_count == task_retry_count,
                    Task.turn_generation == task_turn_generation,
                    task_retry_not_superseded_predicate(),
                    termination_fence,
                )
                .values(status=Task.status)
            )
            if not generation_guard.rowcount:
                await db.rollback()
                return
            existing: SubAgentSession | None = None
            stored_meta: dict[str, Any] = {}
            if is_codex:
                candidates = list(
                    (
                        await db.execute(
                            _select(SubAgentSession)
                            .where(
                                SubAgentSession.task_id == task_id,
                                SubAgentSession.source == "native",
                                SubAgentSession.provider == "codex",
                                SubAgentSession.codex_thread_id
                                == native_agent_id,
                            )
                            .with_for_update()
                        )
                    ).scalars()
                )
                exact: list[tuple[SubAgentSession, dict[str, Any]]] = []
                for candidate in candidates:
                    try:
                        candidate_meta = _json.loads(candidate.meta or "{}")
                    except (TypeError, ValueError):
                        await db.rollback()
                        return
                    if (
                        not isinstance(candidate_meta, dict)
                        or type(candidate_meta.get("owner_retry_count"))
                        is not int
                        or type(candidate_meta.get("owner_turn_generation"))
                        is not int
                    ):
                        await db.rollback()
                        return
                    if (
                        candidate_meta.get("owner_retry_count")
                        == task_retry_count
                        and candidate_meta.get("owner_turn_generation")
                        == task_turn_generation
                    ):
                        exact.append((candidate, candidate_meta))
                if len(exact) > 1:
                    await db.rollback()
                    return
                if exact:
                    existing, stored_meta = exact[0]
                    last_sequence = stored_meta.get("last_sequence")
                    if type(last_sequence) is not int or sequence <= last_sequence:
                        await db.rollback()
                        return
            else:
                existing = (
                    await db.execute(
                        _select(SubAgentSession)
                        .where(
                            SubAgentSession.task_id == task_id,
                            SubAgentSession.source == "native",
                            SubAgentSession.meta.like(f'%"{tool_use_id}"%'),
                        )
                        .with_for_update()
                    )
                ).scalars().first()

            created = False
            previous_status = existing.status if existing is not None else None
            if existing is None:
                if not is_codex and event_type != "subagent_spawn":
                    await db.rollback()
                    return
                initial_status = (
                    terminal_status
                    if event_type == "subagent_done"
                    else "running"
                )
                description = str(info.get("description") or "")[:500]
                if not description and is_codex:
                    description = f"Codex agent {str(native_agent_id)[:12]}"
                existing = SubAgentSession(
                    task_id=task_id,
                    agent_type=info.get("kind") or "native-agent",
                    source="native",
                    description=description,
                    provider="codex" if is_codex else "claude",
                    model=(str(info["model"])[:100] if info.get("model") else None),
                    status=initial_status,
                    codex_thread_id=native_agent_id if is_codex else None,
                    codex_effort_level=(
                        str(info["reasoning_effort"])[:20]
                        if is_codex and info.get("reasoning_effort")
                        else None
                    ),
                    completed_at=(
                        datetime.utcnow()
                        if initial_status != "running"
                        else None
                    ),
                )
                db.add(existing)
                await db.flush()
                created = True

            if is_codex:
                stored_meta.update(info)
                stored_meta.update({
                    "owner_retry_count": task_retry_count,
                    "owner_turn_generation": task_turn_generation,
                    "last_sequence": sequence,
                })
                existing.meta = _json.dumps(
                    stored_meta,
                    ensure_ascii=False,
                    sort_keys=True,
                )
                if info.get("description"):
                    existing.description = str(info["description"])[:500]
                if info.get("model"):
                    existing.model = str(info["model"])[:100]
                if info.get("reasoning_effort"):
                    existing.codex_effort_level = str(
                        info["reasoning_effort"]
                    )[:20]
            elif created:
                existing.meta = _json.dumps(info, ensure_ascii=False)

            if event_type == "subagent_spawn":
                if not created and not is_codex:
                    await db.rollback()
                    return
                if not created and existing.status != "running":
                    existing.status = "running"
                    existing.completed_at = None
                event_name = (
                    "sub_agent_session_created"
                    if created
                    else "sub_agent_session_status"
                )
                broadcasts.append({"event_type": event_name})

            elif event_type == "subagent_progress":
                if existing.status != "running":
                    await db.rollback()
                    return
                existing.checks_done = (existing.checks_done or 0) + 1
                if info.get("summary"):
                    existing.last_summary = str(info["summary"])[:2000]
                    db.add(SubAgentReport(
                        session_id=existing.id,
                        check_number=existing.checks_done,
                        status="running",
                        summary=existing.last_summary,
                    ))
                # Write progress as system_event in chat (like monitor checks)
                summary_text = (existing.last_summary or "working...")[:300]
                log_content = f"[Agent #{existing.id}] {existing.description}: {summary_text}"
                db.add(LogEntry(
                    instance_id=None,
                    task_id=task_id,
                    task_retry_count=task_retry_count,
                    task_turn_generation=task_turn_generation,
                    event_type="system_event",
                    content=log_content,
                    is_error=False,
                ))
                broadcasts.extend([
                    {
                        "event_type": "sub_agent_session_created",
                    } if created else {},
                    {
                        "event_type": "sub_agent_report",
                        "check_number": existing.checks_done,
                        "summary": existing.last_summary,
                    },
                    {
                        "event_type": "system_event",
                        "content": log_content,
                    },
                ])
            elif event_type == "subagent_done":
                if (
                    not created
                    and existing.status in {"completed", "failed", "cancelled"}
                ):
                    await db.rollback()
                    return
                existing.status = terminal_status
                existing.completed_at = datetime.utcnow()
                if info.get("summary"):
                    existing.last_summary = str(info["summary"])[:2000]
                    existing.checks_done = (existing.checks_done or 0) + 1
                    db.add(SubAgentReport(
                        session_id=existing.id,
                        check_number=existing.checks_done,
                        status=terminal_status,
                        summary=existing.last_summary,
                    ))
                if info.get("timed_out"):
                    existing.last_summary = (
                        (existing.last_summary or "") + " [timed out]"
                    ).strip()
                broadcasts.append({
                    "event_type": "sub_agent_session_status",
                })
                # 绝不在这里 enqueue auto-resume：Claude PTY 的 harness 会用
                # task-notification 唤醒 session；Codex app-server 则把 child
                # 生命周期保留在同一个 root adapter 内。这里再投递一条 prompt
                # 会和 provider 的原生通知/turn 赛跑，输了会被 CLI 当
                # mid-turn steering 吸收（queue-op remove、无独立回显）→
                # send_prompt 的回显锁定永不成立 → consumer 永挂 → 队列冻结 →
                # 7200s 超时杀掉仍在干活的进程（2026-07-15 task 32/33 事故）。
                # -p 模式的退出补唤醒走 _consume_output 的
                # monitor:native-exit-resume，不受影响。
            else:
                await db.rollback()
                return

            # The lifecycle row, optional report, and chat audit entry are one
            # fenced durable mutation.  Compute the public count in the same
            # transaction so the global badge update cannot lead the DB.
            active_count = int((
                await db.execute(
                    _select(_func.count(SubAgentSession.id)).where(
                        SubAgentSession.task_id == task_id,
                        SubAgentSession.status == "running",
                    )
                )
            ).scalar_one())
            await db.commit()

            common = {
                "sub_agent_session_id": existing.id,
                "agent_type": existing.agent_type,
                "source": "native",
                "native_mirror_version": 1,
                "provider": existing.provider,
                "description": existing.description,
                "model": existing.model,
                "reasoning_effort": existing.codex_effort_level,
                "status": existing.status,
                "checks_done": existing.checks_done,
                "last_summary": existing.last_summary,
                "codex_thread_id": existing.codex_thread_id,
                "native_sequence": (
                    stored_meta.get("last_sequence") if is_codex else None
                ),
                "task_retry_count": task_retry_count,
                "task_turn_generation": task_turn_generation,
            }

        for payload in broadcasts:
            if not payload:
                continue
            await self.broadcaster.broadcast(
                f"task:{task_id}",
                {**common, **payload},
            )
        if active_count is not None and previous_status != common["status"]:
            await self.broadcaster.broadcast("tasks", {
                "event": "sub_agent_count",
                "event_type": "sub_agent_count",
                "task_id": task_id,
                "active_sub_agents": active_count,
                "task_retry_count": task_retry_count,
                "task_turn_generation": task_turn_generation,
            })

    # ---------------------------------------------------- PTY 权限透传

    _PTY_PERMISSION_TIMEOUT = 120  # channel server 阻塞上限（秒），超时 deny

    def _on_pty_permission_request(self, session_id: str, request: dict) -> None:
        """BridgeHub thread callback with drainable cross-thread ownership."""
        loop = self._loop
        if loop is None or loop.is_closed():
            logger.warning(
                "PTY permission request dropped (no event loop): %s", request
            )
            return
        with self._pty_permission_callback_lock:
            if self._pty_permission_callbacks_draining:
                logger.info(
                    "PTY permission request dropped because Worker runtime "
                    "callbacks are draining"
                )
                return
            callback = self._handle_pty_permission_request(session_id, request)
            try:
                future = asyncio.run_coroutine_threadsafe(callback, loop)
            except RuntimeError:
                callback.close()
                logger.info(
                    "PTY permission request dropped because the event loop "
                    "closed during scheduling"
                )
                return
            self._pty_permission_callback_futures.add(future)
        # ``add_done_callback`` invokes synchronously when the Future already
        # finished; attach it outside the non-reentrant registry lock.
        future.add_done_callback(self._pty_permission_callback_finished)

    def _pty_permission_callback_finished(
        self,
        future: concurrent.futures.Future,
    ) -> None:
        with self._pty_permission_callback_lock:
            self._pty_permission_callback_futures.discard(future)
        try:
            future.result()
        except concurrent.futures.CancelledError:
            return
        except Exception:
            logger.exception("PTY permission request callback failed")

    async def drain_pty_permission_callbacks(self) -> int:
        """Irreversibly close and reap permission callbacks for node drain."""

        with self._pty_permission_callback_lock:
            self._pty_permission_callbacks_draining = True
            pending = tuple(self._pty_permission_callback_futures)
        # Do not cancel the concurrent Future: run_coroutine_threadsafe marks
        # that facade cancelled before the loop Task has necessarily unwound.
        # Admission is already closed and the durable node claim makes each
        # callback fail closed, so natural Future completion is the exact
        # settlement acknowledgement required here.
        if pending:
            await asyncio.gather(
                *(asyncio.wrap_future(future) for future in pending),
                return_exceptions=True,
            )
        with self._pty_permission_callback_lock:
            for future in pending:
                self._pty_permission_callback_futures.discard(future)
        return len(pending)

    async def _handle_pty_permission_request(
        self, session_id: str, request: dict
    ) -> None:
        """Persist one exact PTY permission request before publishing it."""
        import json as _json
        import time as _time

        request_id = request.get("request_id")
        if not request_id:
            return

        task_id = None
        task_incarnation_id = None
        task_retry_count = None
        task_turn_generation = None
        instance_id = None
        async with self.db_factory() as db:
            # A permission request can authorize a new tool effect, so it is
            # admission rather than persistence by an already-admitted runtime.
            # Lock order is node-control -> Task -> termination receipt.  The
            # no-op Task writer keeps a new stop receipt from appearing between
            # the generation check and the permission log commit.
            if not await _fence_worker_runtime_admission(
                db,
                producer="PTY permission request",
            ):
                return
            row = (
                await db.execute(
                    select(Task)
                    .where(Task.session_id == session_id)
                    .order_by(Task.id.desc())
                    .limit(1)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row:
                guarded_task_id = row.id
                guarded = await db.execute(
                    update(Task)
                    .where(
                        Task.id == row.id,
                        Task.session_id == session_id,
                        Task.retry_count == row.retry_count,
                        Task.turn_generation == row.turn_generation,
                        task_retry_not_superseded_predicate(),
                        no_active_worker_task_termination_predicate(),
                    )
                    .values(status=Task.status)
                )
                if guarded.rowcount != 1:
                    await db.rollback()
                    logger.info(
                        "Dropping PTY permission request %s because Task %s "
                        "is terminating or changed generation",
                        request_id,
                        guarded_task_id,
                    )
                    return
                task_id = row.id
                task_incarnation_id = row.incarnation_id
                task_retry_count = row.retry_count
                task_turn_generation = row.turn_generation
                instance_id = row.instance_id or 1
                db.add(LogEntry(
                    instance_id=instance_id,
                    task_id=task_id,
                    task_retry_count=task_retry_count,
                    task_turn_generation=task_turn_generation,
                    event_type="permission_request",
                    role="system",
                    content=request.get("description")
                    or f"权限请求: {request.get('tool_name')}",
                    tool_name=request.get("tool_name"),
                    tool_input=request.get("input_preview"),
                    raw_json=_json.dumps(
                        {"request_id": request_id, "session_id": session_id},
                        ensure_ascii=False,
                    ),
                ))
            await db.commit()

        self._pty_permissions[request_id] = {
            "session_id": session_id,
            "task_id": task_id,
            "task_incarnation_id": task_incarnation_id,
            "task_retry_count": task_retry_count,
            "task_turn_generation": task_turn_generation,
            "instance_id": instance_id,
            "tool_name": request.get("tool_name"),
            "expires_at": _time.monotonic() + self._PTY_PERMISSION_TIMEOUT,
        }

        payload = {
            "event_type": "permission_request",
            "request_id": request_id,
            "tool_name": request.get("tool_name"),
            "description": request.get("description"),
            "input_preview": request.get("input_preview"),
            "timeout_seconds": self._PTY_PERMISSION_TIMEOUT,
        }

        if task_id:
            await self.broadcaster.broadcast(f"task:{task_id}", payload)
        else:
            logger.warning(
                "PTY permission request for unknown session %s (tool=%s)",
                session_id, request.get("tool_name"),
            )

    async def resolve_pty_permission(
        self,
        request_id: str,
        behavior: str,
        *,
        fenced_db: AsyncSession | None = None,
        authorized_task_id: int | None = None,
    ) -> bool:
        """Resolve a Bridge permission under one node/ACL/Task transaction.

        The HTTP effect path supplies ``fenced_db`` after acquiring the Worker
        node, Project ACL, and Task ACL writer fences in that order.  Internal
        callers may omit it and this method acquires the node/Task fences in a
        fresh transaction.  Returns False when the request is stale or its
        exact Task generation no longer owns the effect.
        """
        import json as _json
        import time as _time

        pending = self._pty_permissions.pop(request_id, None)
        # 顺手清理其他过期项
        now = _time.monotonic()
        for rid in [r for r, p in self._pty_permissions.items()
                    if p["expires_at"] < now]:
            self._pty_permissions.pop(rid, None)

        if not pending or pending["expires_at"] < now:
            if fenced_db is not None:
                await fenced_db.rollback()
            return False
        if self._pty_backend is None:
            if fenced_db is not None:
                await fenced_db.rollback()
            return False

        task_id = pending.get("task_id")
        if fenced_db is not None and (
            authorized_task_id is None or task_id != authorized_task_id
        ):
            await fenced_db.rollback()
            return False

        async def settle(db: AsyncSession, *, node_fence_held: bool) -> bool:
            # Keep the node and exact Task generation locked across the Bridge
            # effect and its audit row.  Resolving a pending prompt can start a
            # new tool effect, so phase-one drain and any active Task stop both
            # reject it; it is not late persistence by the old runtime.
            if not node_fence_held:
                if not await _fence_worker_runtime_admission(
                    db,
                    producer="PTY permission resolution",
                ):
                    return False
            if task_id:
                incarnation_id = pending.get("task_incarnation_id")
                incarnation_predicate = (
                    Task.incarnation_id.is_(None)
                    if incarnation_id is None
                    else Task.incarnation_id == incarnation_id
                )
                guarded = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task_id,
                        Task.session_id == pending.get("session_id"),
                        incarnation_predicate,
                        Task.retry_count == pending.get("task_retry_count"),
                        Task.turn_generation
                        == pending.get("task_turn_generation"),
                        task_retry_not_superseded_predicate(),
                        no_active_worker_task_termination_predicate(),
                    )
                    .values(status=Task.status)
                )
                if guarded.rowcount != 1:
                    await db.rollback()
                    return False

            loop = asyncio.get_running_loop()
            ok = await loop.run_in_executor(
                None,
                self._pty_backend._bridge.resolve_permission,
                pending["session_id"],
                request_id,
                behavior,
            )

            # 只有真正送达 CC（channel server 还挂着这个请求）才记录/广播，
            # 否则其他在线客户端会把过期请求误标成"已允许/拒绝"
            if ok and task_id:
                db.add(LogEntry(
                    instance_id=pending.get("instance_id") or 1,
                    task_id=task_id,
                    task_retry_count=pending.get("task_retry_count"),
                    task_turn_generation=pending.get(
                        "task_turn_generation"
                    ),
                    event_type="system_event",
                    role="system",
                    content=f"permission_{behavior}: {pending.get('tool_name')}",
                    raw_json=_json.dumps({"request_id": request_id}),
                ))
            if ok:
                await db.commit()
            else:
                await db.rollback()
            return bool(ok)

        if fenced_db is not None:
            ok = await _settle_instance_cleanup(
                settle(fenced_db, node_fence_held=True)
            )
        else:
            async with self.db_factory() as db:
                ok = await _settle_instance_cleanup(
                    settle(db, node_fence_held=False)
                )
        if ok and task_id:
            await self.broadcaster.broadcast(f"task:{task_id}", {
                "event_type": "permission_resolved",
                "request_id": request_id,
                "behavior": behavior,
            })
        return bool(ok)

    async def _write_direct_claude_prompt(
        self,
        instance_id: int,
        process: asyncio.subprocess.Process,
        prompt: str,
    ) -> None:
        """Deliver a large Claude prompt without exposing it through argv."""

        writer = process.stdin
        if writer is None:
            raise RuntimeError("Claude stdin prompt transport was not created")
        try:
            writer.write(prompt.encode("utf-8"))
            await writer.drain()
            writer.close()
            wait_closed = getattr(writer, "wait_closed", None)
            if wait_closed is not None:
                await wait_closed()
        except BaseException:
            # The process was already registered as the exact generation.
            # Reap it before surfacing input-delivery failure so a half-fed
            # Claude process cannot remain attached to a reusable Instance.
            if (
                process.returncode is None
                or self._process_group_alive(instance_id, process)
            ):
                self._signal_process_tree(instance_id, process, signal.SIGKILL)
            await self._wait_process_tree(instance_id, process, 5.0)
            raise

    async def _spawn_managed_direct_process(
        self,
        instance_id: int,
        task_id: int | None,
        cmd: list[str],
        spawn_kwargs: dict,
        *,
        codex_home: str | None = None,
        task_runtime_scope_task_id: int | None = None,
    ) -> asyncio.subprocess.Process:
        """Spawn and register a direct process without a cancellation gap.

        ``create_subprocess_exec`` can create the OS child and then be
        cancelled before returning its Process adapter.  Shield the spawn,
        collect its outcome, and synchronously install generation evidence.
        If the caller was cancelled, cleanup is itself shielded and the
        original cancellation is delayed until the exact group is proven gone.
        """

        spawn = asyncio.create_task(
            asyncio.create_subprocess_exec(*cmd, **spawn_kwargs)
        )
        cancellation: asyncio.CancelledError | None = None
        # Keep one explicit checkpoint: a spawn can finish between task
        # creation and the first done check, while cancellation of this caller
        # is already scheduled.  Afterwards the shared helper consumes each
        # delivered request so Python 3.14 cannot immediately cancel every new
        # shield and starve the exact process registration below.
        try:
            await asyncio.shield(spawn)
        except asyncio.CancelledError as exc:
            if consume_current_task_cancellation():
                cancellation = exc
            elif not spawn.done():
                raise
        except BaseException:
            if not spawn.done():
                raise
        if not spawn.done():
            later_cancellation = await await_task_completion(spawn)
            if cancellation is None:
                cancellation = later_cancellation

        process = spawn.result()
        self.processes[instance_id] = process
        if os.name == "posix":
            self._process_groups[instance_id] = process
        if codex_home is not None:
            # The Codex caller holds this canonical home's admission lock.
            # Publish the exact process/home pair before delivering a delayed
            # spawn cancellation so failed cleanup cannot release the home
            # while the child generation may still be alive.
            self._codex_exec_homes[instance_id] = codex_home
        if task_runtime_scope_task_id is not None:
            self._adopt_task_runtime_scope_direct(
                task_runtime_scope_task_id,
                instance_id,
                process,
            )

        if cancellation is None:
            return process

        async def cleanup_cancelled_spawn() -> None:
            if (
                process.returncode is None
                or self._process_group_alive(instance_id, process)
            ):
                self._signal_process_tree(
                    instance_id, process, signal.SIGKILL
                )
            await self._wait_process_tree(instance_id, process, 5.0)
            if not self._generation_reap_confirmed(instance_id, process):
                raise RuntimeError(
                    f"Cancelled spawn for instance {instance_id} "
                    "could not be proven terminal"
                )
            if self.processes.get(instance_id) is process:
                self.processes.pop(instance_id, None)
                if (
                    codex_home is not None
                    and self._codex_exec_homes.get(instance_id) == codex_home
                ):
                    self._codex_exec_homes.pop(instance_id, None)
            if self._process_groups.get(instance_id) is process:
                self._process_groups.pop(instance_id, None)
            self._release_task_runtime_scope_direct_owner(
                instance_id,
                process,
            )

        cleanup = asyncio.create_task(cleanup_cancelled_spawn())
        cleanup_error: BaseException | None = None
        # Preserve the first cancellation while refusing to abandon the OS
        # child during repeated application-shutdown cancels.
        await await_task_completion(cleanup)
        try:
            cleanup.result()
        except BaseException as exc:
            cleanup_error = exc
            logger.exception(
                "Could not reap cancelled direct spawn for instance %s",
                instance_id,
                exc_info=(type(exc), exc, exc.__traceback__),
            )

        if cleanup_error is not None:
            try:
                async with self.db_factory() as db:
                    await db.execute(
                        update(Instance)
                        .where(Instance.id == instance_id)
                        .values(
                            status="error",
                            pid=getattr(process, "pid", None),
                            process_identity=capture_process_identity(
                                getattr(process, "pid", None)
                            ),
                            current_task_id=task_id,
                        )
                    )
                    await db.commit()
            except Exception:
                logger.exception(
                    "Failed to persist cancelled spawn evidence for "
                    "instance %s",
                    instance_id,
                )
        raise cancellation

    def _uses_managed_process_group(
        self,
        instance_id: int,
        process: asyncio.subprocess.Process,
    ) -> bool:
        return self._managed_process_group_id(instance_id, process) is not None

    def _managed_process_group_id(
        self,
        instance_id: int,
        process: asyncio.subprocess.Process,
    ) -> int | None:
        """Return a signal-safe PGID for one registered direct generation.

        ``os.killpg(1, sig)`` is translated to ``kill(-1, sig)`` on POSIX,
        which broadcasts to every process the service user may signal.  Treat
        missing, synthetic, or corrupted group identities as unresolved
        generation evidence instead of ever falling back to that broadcast.
        """

        if (
            os.name != "posix"
            or self._process_groups.get(instance_id) is not process
        ):
            return None
        return require_safe_process_group_id(
            getattr(process, "pid", None),
            context=f"instance {instance_id}",
        )

    def _process_group_alive(
        self,
        instance_id: int,
        process: asyncio.subprocess.Process,
    ) -> bool:
        process_group_id = self._managed_process_group_id(
            instance_id, process
        )
        if process_group_id is None:
            return False
        try:
            os.killpg(process_group_id, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    def _generation_reap_confirmed(
        self,
        instance_id: int,
        process: asyncio.subprocess.Process,
    ) -> bool:
        """Whether every known part of one exact process generation is gone.

        This deliberately uses only synchronous evidence so task done
        callbacks can decide whether map cleanup is safe.  A retained
        container-exec mapping is treated as live/unknown until the async
        control-plane check has proved it gone and ``_forget_container_exec``
        removes that evidence.
        """

        if process.returncode is None:
            return False
        try:
            if self._process_group_alive(instance_id, process):
                return False
        except Exception:
            return False
        return self._container_exec_processes.get(instance_id) is not process

    def _signal_process_tree(
        self,
        instance_id: int,
        process: asyncio.subprocess.Process,
        sig: signal.Signals,
    ) -> None:
        """Signal a direct CLI process group, or one adapter process."""

        process_group_id = self._managed_process_group_id(
            instance_id, process
        )
        if process_group_id is not None:
            try:
                os.killpg(process_group_id, sig)
                return
            except ProcessLookupError:
                return
        try:
            if sig == signal.SIGTERM:
                process.terminate()
            elif sig == signal.SIGKILL:
                process.kill()
            else:
                process.send_signal(sig)
        except ProcessLookupError:
            return

    def _is_managed_container_exec(
        self,
        instance_id: int,
        process: asyncio.subprocess.Process,
    ) -> bool:
        manager = getattr(self, "_container_mgr", None)
        return (
            manager is not None
            and self._container_exec_processes.get(instance_id) is process
            and manager.owns_exec(process)
        )

    async def _container_exec_alive(
        self,
        instance_id: int,
        process: asyncio.subprocess.Process,
    ) -> bool:
        if not self._is_managed_container_exec(instance_id, process):
            return False
        return await self._container_mgr.exec_is_alive(process)

    def _forget_container_exec(
        self,
        instance_id: int,
        process: asyncio.subprocess.Process,
    ) -> None:
        if self._container_exec_processes.get(instance_id) is not process:
            return
        manager = getattr(self, "_container_mgr", None)
        if manager is not None:
            manager.forget_exec(process)
        self._container_exec_processes.pop(instance_id, None)
        self._container_tasks.pop(instance_id, None)

    async def finalize_pty_container_exec(
        self,
        instance_id: int,
        *,
        expected_process: asyncio.subprocess.Process | None = None,
    ) -> None:
        """Prove a PTY container generation gone before on_exit advertises idle."""

        process = self._container_exec_processes.get(instance_id)
        if expected_process is not None and process is not expected_process:
            # A late PTY callback must never signal a replacement container
            # generation that already reused this instance key.
            return
        if process is None or not self._is_managed_container_exec(
            instance_id, process
        ):
            return
        if await self._container_exec_alive(instance_id, process):
            await self._container_mgr.signal_exec(
                process, signal.SIGKILL
            )
            deadline = asyncio.get_running_loop().time() + 5.0
            while await self._container_exec_alive(instance_id, process):
                if asyncio.get_running_loop().time() >= deadline:
                    raise RuntimeError(
                        f"Container PTY for instance {instance_id} "
                        "survived SIGKILL"
                    )
                await asyncio.sleep(0.05)
        self._forget_container_exec(instance_id, process)

    async def _signal_managed_process_tree(
        self,
        instance_id: int,
        process: asyncio.subprocess.Process,
        sig: signal.Signals,
    ) -> None:
        """Signal an inner container group and its exact host process group."""

        container_error: Exception | None = None
        if self._is_managed_container_exec(instance_id, process):
            try:
                await self._container_mgr.signal_exec(process, sig)
            except Exception as exc:
                # Still stop the host-side docker client, but fail closed: the
                # caller must retain PID/owner evidence because inner cleanup
                # could not be proven.
                container_error = exc
        self._signal_process_tree(instance_id, process, sig)
        if container_error is not None:
            raise container_error

    async def _wait_process_tree(
        self,
        instance_id: int,
        process: asyncio.subprocess.Process,
        timeout: float,
    ) -> None:
        """Wait for the CLI parent and every child in its managed group."""

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        if process.returncode is None:
            await asyncio.wait_for(
                asyncio.shield(process.wait()), timeout=max(0.01, timeout)
            )
        while (
            self._process_group_alive(instance_id, process)
            or await self._container_exec_alive(instance_id, process)
        ):
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError
            await asyncio.sleep(min(0.05, remaining))
        self._forget_container_exec(instance_id, process)

    async def kill_process_generation(
        self,
        instance_id: int,
        expected_process: asyncio.subprocess.Process,
        *,
        timeout: float = 5.0,
    ) -> bool:
        """SIGKILL one exact direct/adapted generation without changing DB state.

        Dispatcher timeout handling still owns the Task retry/fail decision,
        so it must not call the higher-level ``stop()`` (which releases the
        claim).  This narrow operation shares the launch/stop lifecycle lock,
        signals the managed POSIX process group, and delays caller cancellation
        until the reap attempt has settled.
        """

        async def kill_exact() -> bool:
            lifecycle_lock = self._instance_lifecycle_lock(instance_id)
            async with lifecycle_lock:
                record = self._consumer_records.get(instance_id)
                exact_generation_known = (
                    self.processes.get(instance_id) is expected_process
                    or self._process_groups.get(instance_id) is expected_process
                    or self._container_exec_processes.get(instance_id)
                    is expected_process
                    or (
                        record is not None
                        and record.process is expected_process
                    )
                )
                if not exact_generation_known:
                    return False
                if (
                    expected_process.returncode is None
                    or self._process_group_alive(instance_id, expected_process)
                ):
                    await self._signal_managed_process_tree(
                        instance_id, expected_process, signal.SIGKILL
                    )
                try:
                    await self._wait_process_tree(
                        instance_id, expected_process, timeout
                    )
                except asyncio.TimeoutError as exc:
                    raise RuntimeError(
                        f"Process group for instance {instance_id} survived SIGKILL"
                    ) from exc
                return True

        return await finish_awaitable(kill_exact())

    async def stop(
        self,
        instance_id: int,
        *,
        expected_task_id: int | None = None,
        expected_task_turn_generation: int | object = (
            _EXPECTED_GENERATION_UNSET
        ),
        expected_pid: int | None | object = _EXPECTED_GENERATION_UNSET,
        expected_started_at: datetime | None | object = _EXPECTED_GENERATION_UNSET,
        task_status: str = "pending",
        task_error_message: str | None = None,
        terminal_consumer_timeout: float | None = (
            DEFAULT_TERMINAL_CONSUMER_TIMEOUT
        ),
        consumer_cancel_timeout: float | None = DEFAULT_CONSUMER_CANCEL_TIMEOUT,
        allow_delivery_effect_stop: bool = False,
        yield_to_worker_task_termination: bool = True,
        worker_termination_operation_id: str | None = None,
        worker_termination_operation: str | None = None,
        worker_termination_execution_token: str | None = None,
        worker_termination_state_version: int | None = None,
    ) -> bool:
        """Cancellation-safe stop of one reusable worker slot.

        ``expected_task_id`` turns a historical instance reference into an
        owner-checked operation. ``expected_task_turn_generation`` fences hot
        PTY reuse where Task, Instance, PID, and start time remain unchanged.
        ``expected_pid`` and
        ``expected_started_at`` additionally fence the exact process
        generation (explicit ``None`` is a real expected value; omission
        disables that one fence). All are verified under the launch lock and
        again in the terminal DB CAS, so a recycled slot cannot stop a newer
        generation even when it belongs to the same task.

        Workflow-owned Delivery/PR effect Tasks are fail-closed by default.
        Only an internal controller shutdown or exact recovery path may opt in
        with ``allow_delivery_effect_stop=True``.

        Ordinary lifecycle stops always yield to a durable Worker termination
        receipt.  The receipt executor is the sole exception: it must both
        disable yielding and name the exact active Worker-side operation.  The
        identity is re-proven in SQL before process effects, in the terminal
        Task/Instance transaction, and again before publication.
        """

        if task_status not in {
            "pending",
            "completed",
            "cancelled",
            "failed",
        }:
            raise ValueError(f"Unsupported terminal task status: {task_status}")
        if (
            expected_task_turn_generation is not _EXPECTED_GENERATION_UNSET
            and expected_task_id is None
        ):
            raise ValueError(
                "expected_task_turn_generation requires expected_task_id"
            )
        receipt_identity = (
            worker_termination_operation_id,
            worker_termination_operation,
            worker_termination_execution_token,
            worker_termination_state_version,
        )
        if (
            yield_to_worker_task_termination
            != (worker_termination_operation_id is None)
            or (
                worker_termination_operation_id is None
                and any(value is not None for value in receipt_identity[1:])
            )
            or (
                worker_termination_operation_id is not None
                and (
                    worker_termination_operation
                    not in {"cancel", "stop_session", "supersede"}
                    or worker_termination_execution_token is None
                    or worker_termination_state_version is None
                )
            )
        ):
            # Valid modes are deliberately unambiguous: ordinary callers use
            # the default yielding gate with no operation id; the receipt
            # executor uses a non-yielding stop carrying its exact id.  Never
            # accept the historical anonymous ``False`` escape hatch.
            return False

        return await finish_awaitable(
            self._stop_serialized(
                instance_id,
                expected_task_id=expected_task_id,
                expected_task_turn_generation=(
                    expected_task_turn_generation
                ),
                expected_pid=expected_pid,
                expected_started_at=expected_started_at,
                task_status=task_status,
                task_error_message=task_error_message,
                terminal_consumer_timeout=terminal_consumer_timeout,
                consumer_cancel_timeout=consumer_cancel_timeout,
                allow_delivery_effect_stop=allow_delivery_effect_stop,
                yield_to_worker_task_termination=(
                    yield_to_worker_task_termination
                ),
                worker_termination_operation_id=(
                    worker_termination_operation_id
                ),
                worker_termination_operation=worker_termination_operation,
                worker_termination_execution_token=(
                    worker_termination_execution_token
                ),
                worker_termination_state_version=(
                    worker_termination_state_version
                ),
            )
        )

    @staticmethod
    def _pid_is_definitely_gone(
        pid: int | None,
        persisted_identity: str | None,
    ) -> bool:
        """Return True only when the exact persisted process is gone."""

        return persisted_process_is_definitively_dead(
            pid,
            persisted_identity,
        )

    async def reconcile_dead_reverse_task_owner(
        self,
        instance_id: int,
        *,
        expected_task_id: int,
        expected_pid: int | None,
        expected_started_at: datetime | None,
        worker_termination_operation_id: str | None = None,
        worker_termination_operation: str | None = None,
        worker_termination_execution_token: str | None = None,
        worker_termination_state_version: int | None = None,
    ) -> bool:
        """Release one exact dead reverse owner superseded by another slot.

        A retry can leave ``Instance.current_task_id`` pointing at a Task whose
        authoritative ``Task.instance_id`` has already moved on.  There is no
        process left for ``stop()`` to signal in that state.  Reconcile only
        when the old PID is provably absent, all same-process runtime evidence
        is gone, the Task points elsewhere, and the durable Instance still
        matches the caller's exact PID/start fences.
        """

        receipt_identity = (
            worker_termination_operation_id,
            worker_termination_operation,
            worker_termination_execution_token,
            worker_termination_state_version,
        )
        if (
            (
                worker_termination_operation_id is None
                and any(value is not None for value in receipt_identity[1:])
            )
            or (
                worker_termination_operation_id is not None
                and (
                    worker_termination_operation
                    not in {"cancel", "stop_session", "supersede"}
                    or worker_termination_execution_token is None
                    or worker_termination_state_version is None
                )
            )
        ):
            return False

        lifecycle_lock = self._instance_lifecycle_lock(instance_id)
        async with lifecycle_lock:
            if (
                instance_id in self._stopping
                or instance_id in self._launch_reservations
                or self.processes.get(instance_id) is not None
                or self._process_groups.get(instance_id) is not None
                or self._container_exec_processes.get(instance_id) is not None
                or self._tasks.get(instance_id) is not None
                or self._consumer_records.get(instance_id) is not None
                or any(
                    recovery_instance_id == instance_id
                    for recovery_instance_id, _process in (
                        self._consumer_recovery_pending
                    )
                )
                or self._pty_post_exit_generation_for_instance(
                    instance_id,
                    expected_task_id,
                ) is not None
            ):
                return False
            # Snapshot the identity from the same exact reverse-owner row. The
            # later writer predicate repeats it, so a concurrent generation
            # cannot lend its boot/start proof to this cleanup decision.
            async with self.db_factory() as identity_db:
                identity_row = (
                    await identity_db.execute(
                        select(Instance.process_identity).where(
                            Instance.id == instance_id,
                            Instance.current_task_id == expected_task_id,
                            (
                                Instance.pid.is_(None)
                                if expected_pid is None
                                else Instance.pid == expected_pid
                            ),
                            (
                                Instance.started_at.is_(None)
                                if expected_started_at is None
                                else Instance.started_at == expected_started_at
                            ),
                        )
                    )
                ).one_or_none()
                await identity_db.rollback()
            if identity_row is None:
                return False
            expected_process_identity = identity_row.process_identity
            if not self._pid_is_definitely_gone(
                expected_pid,
                expected_process_identity,
            ):
                return False

            async with self.db_factory() as db:
                # Preserve the global Task -> Instance lock order. The Task
                # no-op locks its authoritative owner before the exact stale
                # reverse owner is cleared.
                instance_generation_predicates = [
                    Instance.id == instance_id,
                    Instance.current_task_id == expected_task_id,
                    (
                        Instance.pid.is_(None)
                        if expected_pid is None
                        else Instance.pid == expected_pid
                    ),
                    (
                        Instance.started_at.is_(None)
                        if expected_started_at is None
                        else Instance.started_at == expected_started_at
                    ),
                    (
                        Instance.process_identity.is_(None)
                        if expected_process_identity is None
                        else Instance.process_identity
                        == expected_process_identity
                    ),
                ]
                lease_valid_at = (
                    await _lock_worker_termination_stop_authority(
                        db,
                        task_id=expected_task_id,
                        instance_id=instance_id,
                        task_predicates=(Task.id == expected_task_id,),
                        instance_predicates=instance_generation_predicates,
                        operation_id=worker_termination_operation_id,
                        operation=worker_termination_operation,
                        execution_token=(
                            worker_termination_execution_token
                        ),
                        state_version=worker_termination_state_version,
                    )
                )
                if lease_valid_at is None:
                    return False
                current_instance_id = await db.scalar(
                    select(Task.instance_id).where(
                        Task.id == expected_task_id
                    )
                )
                if current_instance_id == instance_id:
                    await db.rollback()
                    return False

                instance_cleanup = await db.execute(
                    update(Instance)
                    .where(
                        Instance.id == instance_id,
                        Instance.current_task_id == expected_task_id,
                        _worker_termination_instance_stop_predicate(
                            worker_termination_operation_id,
                            worker_termination_operation,
                            worker_termination_execution_token,
                            worker_termination_state_version,
                            lease_valid_at,
                        ),
                        (
                            Instance.pid.is_(None)
                            if expected_pid is None
                            else Instance.pid == expected_pid
                        ),
                        (
                            Instance.started_at.is_(None)
                            if expected_started_at is None
                            else Instance.started_at
                            == expected_started_at
                        ),
                        (
                            Instance.process_identity.is_(None)
                            if expected_process_identity is None
                            else Instance.process_identity
                            == expected_process_identity
                        ),
                    )
                    .values(
                        status="idle",
                        pid=None,
                        process_identity=None,
                        current_task_id=None,
                    )
                )
                if not instance_cleanup.rowcount:
                    await db.rollback()
                    return False
                await db.commit()

            async with self.db_factory() as publication_db:
                publication_lease = (
                    await _lock_worker_termination_stop_authority(
                        publication_db,
                        task_id=expected_task_id,
                        instance_id=instance_id,
                        task_predicates=(Task.id == expected_task_id,),
                        instance_predicates=(
                            Instance.id == instance_id,
                            Instance.current_task_id.is_(None),
                            Instance.pid.is_(None),
                            Instance.status == "idle",
                        ),
                        operation_id=worker_termination_operation_id,
                        operation=worker_termination_operation,
                        execution_token=(
                            worker_termination_execution_token
                        ),
                        state_version=worker_termination_state_version,
                    )
                )
                if publication_lease is not None:
                    try:
                        await self.broadcaster.broadcast(
                            "system",
                            {
                                "event": "instance_status",
                                "instance_id": instance_id,
                                "status": "idle",
                                "exit_code": None,
                            },
                        )
                    except Exception:
                        # The exact DB cleanup is already durable. A transient
                        # WebSocket failure must not turn it back into an
                        # unresolved process owner.
                        logger.exception(
                            "Failed to publish reconciled instance %s",
                            instance_id,
                        )
                    await publication_db.commit()
            logger.warning(
                "Reconciled dead reverse owner: instance %s / task %s / pid %s",
                instance_id,
                expected_task_id,
                expected_pid,
            )
            return True

    async def _stop_serialized(
        self,
        instance_id: int,
        *,
        expected_task_id: int | None,
        expected_task_turn_generation: int | object,
        expected_pid: int | None | object,
        expected_started_at: datetime | None | object,
        task_status: str,
        task_error_message: str | None,
        terminal_consumer_timeout: float | None,
        consumer_cancel_timeout: float | None,
        allow_delivery_effect_stop: bool,
        yield_to_worker_task_termination: bool,
        worker_termination_operation_id: str | None,
        worker_termination_operation: str | None,
        worker_termination_execution_token: str | None,
        worker_termination_state_version: int | None,
    ) -> bool:
        """Serialize stop against launch without cancelling terminal bookkeeping."""

        lifecycle_lock = self._instance_lifecycle_lock(instance_id)
        settled_terminal_consumer = False
        force_cancel_consumer = False
        expected_owner_verified = False
        stop_fence_registered = False
        # A terminal consumer can remove every instance-keyed map from its
        # done callback while this stop is awaiting it.  Keep the exact
        # handles captured before the await so a later DB failure can still
        # leave a generation-bound recovery receipt.  These are never put
        # back into the live maps.
        terminal_process: Any | None = None
        terminal_record: _OutputConsumerRecord | None = None
        terminal_task: asyncio.Task | None = None
        terminal_launch_params: dict | None = None
        runtime_mismatch = object()

        def snapshot_runtime():
            mapped_process = (
                self.processes.get(instance_id)
                or self._process_groups.get(instance_id)
                or self._container_exec_processes.get(instance_id)
            )
            mapped_record = self._consumer_records.get(instance_id)
            mapped_task = self._tasks.get(instance_id)
            if mapped_process is None and mapped_record is not None:
                mapped_process = mapped_record.process
            if terminal_process is None:
                process = mapped_process
                record = mapped_record
                task = (
                    record.task
                    if record is not None
                    else mapped_task
                )
                if (
                    record is not None
                    and process is not None
                    and record.process is not process
                ):
                    return runtime_mismatch
                return process, record, task

            # Once a terminal generation has been captured, a non-empty map
            # pointing elsewhere is an ABA/replacement race.  Do not let the
            # old stop touch the new generation.
            for mapped in (
                mapped_process,
                self.processes.get(instance_id),
                self._process_groups.get(instance_id),
                self._container_exec_processes.get(instance_id),
            ):
                if mapped is not None and mapped is not terminal_process:
                    return runtime_mismatch
            if (
                mapped_record is not None
                and terminal_record is not None
                and mapped_record is not terminal_record
            ):
                return runtime_mismatch
            if (
                mapped_task is not None
                and terminal_task is not None
                and mapped_task is not terminal_task
            ):
                return runtime_mismatch
            record = terminal_record or mapped_record
            task = terminal_task or (
                record.task if record is not None else mapped_task
            )
            if (
                record is not None
                and record.process is not terminal_process
            ):
                return runtime_mismatch
            if record is not None and task is not None and record.task is not task:
                return runtime_mismatch
            return terminal_process, record, task

        try:
            while True:
                async with lifecycle_lock:
                    initial_runtime = snapshot_runtime()
                    if initial_runtime is runtime_mismatch:
                        return False
                    initial_process, initial_record, initial_task = (
                        initial_runtime
                    )
                    if (
                        terminal_process is None
                        and initial_process is not None
                        and initial_record is not None
                        and initial_task is not None
                        and self._generation_reap_confirmed(
                            instance_id, initial_process
                        )
                    ):
                        # Capture before the first owner/guard query.  The
                        # consumer callback can finish during that await and
                        # remove all instance-keyed maps.
                        terminal_process = initial_process
                        terminal_record = initial_record
                        terminal_task = initial_task
                        terminal_launch_params = self._launch_params.get(
                            instance_id
                        )
                    has_expected_owner = (
                        expected_task_id is not None
                        or expected_task_turn_generation
                        is not _EXPECTED_GENERATION_UNSET
                        or expected_pid is not _EXPECTED_GENERATION_UNSET
                        or expected_started_at is not _EXPECTED_GENERATION_UNSET
                    )
                    owner: Instance | None | object = (
                        _EXPECTED_GENERATION_UNSET
                    )
                    if has_expected_owner:
                        async with self.db_factory() as db:
                            task_turn_generation = (
                                await db.scalar(
                                    select(Task.turn_generation).where(
                                        Task.id == expected_task_id
                                    )
                                )
                                if expected_task_turn_generation
                                is not _EXPECTED_GENERATION_UNSET
                                else None
                            )
                            owner = await db.get(Instance, instance_id)
                        if (
                            owner is None
                            or (
                                expected_task_turn_generation
                                is not _EXPECTED_GENERATION_UNSET
                                and task_turn_generation
                                != expected_task_turn_generation
                            )
                            or (
                                expected_task_id is not None
                                and owner.current_task_id != expected_task_id
                            )
                            or (
                                expected_pid is not _EXPECTED_GENERATION_UNSET
                                and owner.pid != expected_pid
                            )
                            or (
                                expected_started_at
                                is not _EXPECTED_GENERATION_UNSET
                                and owner.started_at != expected_started_at
                            )
                        ):
                            if (
                                expected_pid is not _EXPECTED_GENERATION_UNSET
                                or expected_started_at
                                is not _EXPECTED_GENERATION_UNSET
                            ):
                                # An exact generation fence is never satisfied
                                # by merely settling an older consumer: the
                                # same Task/Instance may already own a new PID.
                                return False
                            return bool(
                                settled_terminal_consumer
                                and expected_owner_verified
                            )
                        expected_owner_verified = True
                    if not stop_fence_registered:
                        # Publish the stop intent after exact-owner validation,
                        # but before any slower workflow ownership lookup.  It
                        # is only an admission fence; no consumer or process is
                        # touched until the guard below succeeds.
                        self._begin_stopping(instance_id)
                        stop_fence_registered = True
                    runtime = snapshot_runtime()
                    if runtime is runtime_mismatch:
                        return False
                    pre_guard_process, pre_guard_record, _pre_guard_task = runtime
                    protected_task_id = expected_task_id
                    if protected_task_id is None:
                        guard_owner = (
                            owner
                            if isinstance(owner, Instance)
                            else None
                        )
                        if guard_owner is None:
                            async with self.db_factory() as db:
                                guard_owner = await db.get(Instance, instance_id)
                        if guard_owner is not None:
                            protected_task_id = guard_owner.current_task_id
                    if protected_task_id is None:
                        guard_record = self._consumer_records.get(instance_id)
                        if guard_record is not None:
                            protected_task_id = guard_record.task_id
                    if (
                        protected_task_id is not None
                        and not allow_delivery_effect_stop
                    ):
                        async with self.db_factory() as db:
                            if await _task_has_protected_delivery_effect(
                                db,
                                protected_task_id,
                            ):
                                logger.warning(
                                    "Refused generic stop of workflow-owned "
                                    "Task %s on instance %s",
                                    protected_task_id,
                                    instance_id,
                                )
                                return False
                    if (
                        pre_guard_record is not None
                        and pre_guard_record.task.done()
                        and pre_guard_process is not None
                        and self._generation_reap_confirmed(
                            instance_id,
                            pre_guard_process,
                        )
                        and self._consumer_records.get(instance_id)
                        is not pre_guard_record
                    ):
                        # The ownership query deliberately awaits before any
                        # signal.  A terminal consumer may finish and remove
                        # its exact maps during that await; preserve the same
                        # settled-cleanup proof the pre-guard loop observed.
                        settled_terminal_consumer = True
                        if terminal_process is None:
                            terminal_process = pre_guard_process
                            terminal_record = pre_guard_record
                            terminal_task = pre_guard_record.task
                            terminal_launch_params = self._launch_params.get(
                                instance_id
                            )
                    runtime = snapshot_runtime()
                    if runtime is runtime_mismatch:
                        return False
                    process, record, task = runtime
                    record_process = record.process if record is not None else process
                    recovery_match = self._terminal_stop_recovery_match(
                        instance_id,
                        process=process,
                        expected_task_id=(
                            expected_task_id
                            if expected_task_id is not None
                            else _EXPECTED_GENERATION_UNSET
                        ),
                        expected_task_turn_generation=(
                            expected_task_turn_generation
                        ),
                        expected_pid=expected_pid,
                        expected_started_at=expected_started_at,
                    )
                    recovery_pending = (
                        recovery_match[1]
                        if recovery_match is not None
                        else None
                    )
                    if (
                        recovery_pending is not None
                        and recovery_pending.tracked_generation
                        and owner is _EXPECTED_GENERATION_UNSET
                    ):
                        # Recovery evidence derives exact pid/started_at
                        # fences inside _stop_locked. Resolve their durable
                        # owner before terminal ownership can be claimed.
                        async with self.db_factory() as db:
                            owner = await db.get(Instance, instance_id)
                    process_live = (
                        process is not None
                        and not self._generation_reap_confirmed(
                            instance_id, process
                        )
                    )
                    consumer_live = task is not None and not task.done()
                    pty_managed = bool(
                        process_live
                        and self._pty_backend is not None
                        and instance_id
                        in getattr(self._pty_backend, "_sessions", {})
                    )
                    pty_consumer_owns_terminal = False
                    if (
                        pty_managed
                        and consumer_live
                        and record is not None
                        and record.process is process
                        and record.task is task
                    ):
                        terminal_owner = record.pty_terminal_owner
                        if terminal_owner not in {
                            None,
                            "stop",
                            "consumer",
                        }:
                            raise RuntimeError(
                                "Invalid PTY terminal owner recorded: "
                                f"{terminal_owner}"
                            )
                        pty_consumer_owns_terminal = (
                            terminal_owner == "consumer"
                            and not record.pty_background_waiting
                        )
                    terminal_consumer = (
                        consumer_live
                        and (
                            pty_consumer_owns_terminal
                            or (
                                not process_live
                                and (
                                    record_process is None
                                    or record_process.returncode is not None
                                )
                            )
                        )
                    )
                    if terminal_consumer and not force_cancel_consumer:
                        # The exact owner fence above has succeeded.  Publish
                        # stop intent before releasing the lifecycle lock so
                        # this consumer cannot launch a retry/replacement while
                        # we await its terminal bookkeeping.
                        expected_process = record_process
                        provider = record.provider if record is not None else "claude"
                        terminal_process = record_process
                        terminal_record = record
                        terminal_task = task
                        terminal_launch_params = self._launch_params.get(
                            instance_id
                        )
                    else:
                        stopped = await self._stop_locked(
                            instance_id,
                            expected_task_id=expected_task_id,
                            expected_task_turn_generation=(
                                expected_task_turn_generation
                            ),
                            expected_pid=expected_pid,
                            expected_started_at=expected_started_at,
                            task_status=task_status,
                            task_error_message=task_error_message,
                            consumer_cancel_timeout=consumer_cancel_timeout,
                            allow_settled_cleanup=(
                                settled_terminal_consumer
                                or recovery_pending is not None
                                or (
                                    worker_termination_operation_id
                                    is not None
                                    and pre_guard_process is not None
                                    and self._generation_reap_confirmed(
                                        instance_id,
                                        pre_guard_process,
                                    )
                                )
                                or (
                                    worker_termination_operation_id
                                    is not None
                                    and isinstance(owner, Instance)
                                    and expected_pid
                                    is not _EXPECTED_GENERATION_UNSET
                                    and owner.pid == expected_pid
                                    and self._pid_is_definitely_gone(
                                        expected_pid,
                                        owner.process_identity,
                                    )
                                )
                                or self._has_reapable_pty_background_state(
                                    expected_task_id
                                )
                                or self._pty_post_exit_generation_for_instance(
                                    instance_id,
                                    expected_task_id,
                                )
                                is not None
                            ),
                            verified_owner=owner,
                            yield_to_worker_task_termination=(
                                yield_to_worker_task_termination
                            ),
                            worker_termination_operation_id=(
                                worker_termination_operation_id
                            ),
                            worker_termination_operation=(
                                worker_termination_operation
                            ),
                            worker_termination_execution_token=(
                                worker_termination_execution_token
                            ),
                            worker_termination_state_version=(
                                worker_termination_state_version
                            ),
                            exact_process=(
                                terminal_process
                                if terminal_process is not None
                                else _EXPECTED_GENERATION_UNSET
                            ),
                            exact_record=(
                                terminal_record
                                if terminal_process is not None
                                else _EXPECTED_GENERATION_UNSET
                            ),
                            exact_task=(
                                terminal_task
                                if terminal_process is not None
                                else _EXPECTED_GENERATION_UNSET
                            ),
                            exact_launch_params=(
                                terminal_launch_params
                                if terminal_process is not None
                                else _EXPECTED_GENERATION_UNSET
                            ),
                        )
                        return stopped or (
                            settled_terminal_consumer
                            and recovery_pending is None
                        )

                # The model process has already ended. In particular, a Codex
                # consumer may now be migrating its rollout, rebinding the
                # app-server owner, and persisting task affinity. Await it
                # outside the lifecycle lock so terminal bookkeeping can finish.
                # A consumer-driven retry may acquire the lock, but launch()
                # observes this stop token there and rejects the replacement.
                try:
                    terminal_wait = self.wait_for_output_consumer(
                        instance_id,
                        provider=provider,
                        timeout=None if provider == "codex" else 30,
                        expected_process=expected_process,
                        preserve_error=True,
                    )
                    if terminal_consumer_timeout is None:
                        await terminal_wait
                    else:
                        await asyncio.wait_for(
                            terminal_wait,
                            timeout=terminal_consumer_timeout,
                        )
                except asyncio.TimeoutError:
                    force_cancel_consumer = True
                    # The consumer owns terminal bookkeeping, so it cannot be
                    # pre-empted while live: it may already be inside the DB
                    # finalizer.  A Worker execution lease can expire while
                    # this wait is shielded, so re-prove the exact durable
                    # authority before cancellation becomes another runtime
                    # effect.  Cancel and reap that exact task outside the
                    # lifecycle lock first; the next locked iteration can
                    # then safely take over its abandoned owner claim.
                    if task is not None and not task.done():
                        cancel_task_id = expected_task_id
                        if cancel_task_id is None and record is not None:
                            cancel_task_id = record.task_id
                        if (
                            cancel_task_id is None
                            and isinstance(owner, Instance)
                        ):
                            cancel_task_id = owner.current_task_id
                        if cancel_task_id is None:
                            if worker_termination_operation_id is not None:
                                return False
                        else:
                            cancel_task_predicates: list[Any] = [
                                Task.id == cancel_task_id,
                            ]
                            if record is not None:
                                if record.task_retry_count is not None:
                                    cancel_task_predicates.append(
                                        Task.retry_count
                                        == record.task_retry_count
                                    )
                                if record.task_turn_generation is not None:
                                    cancel_task_predicates.append(
                                        Task.turn_generation
                                        == record.task_turn_generation
                                    )
                            cancel_instance_predicates: list[Any] = [
                                Instance.id == instance_id,
                            ]
                            if expected_pid is not _EXPECTED_GENERATION_UNSET:
                                cancel_instance_predicates.append(
                                    Instance.pid == expected_pid
                                )
                            if (
                                expected_started_at
                                is not _EXPECTED_GENERATION_UNSET
                            ):
                                cancel_instance_predicates.append(
                                    Instance.started_at.is_(None)
                                    if expected_started_at is None
                                    else Instance.started_at
                                    == expected_started_at
                                )
                            async with self.db_factory() as authority_db:
                                cancel_lease_valid_at = await (
                                    _lock_worker_termination_stop_authority(
                                        authority_db,
                                        task_id=cancel_task_id,
                                        instance_id=instance_id,
                                        task_predicates=(
                                            cancel_task_predicates
                                        ),
                                        instance_predicates=(
                                            cancel_instance_predicates
                                        ),
                                        operation_id=(
                                            worker_termination_operation_id
                                        ),
                                        operation=(
                                            worker_termination_operation
                                        ),
                                        execution_token=(
                                            worker_termination_execution_token
                                        ),
                                        state_version=(
                                            worker_termination_state_version
                                        ),
                                    )
                                )
                            if cancel_lease_valid_at is None:
                                return False
                        task.cancel()
                        if consumer_cancel_timeout is None:
                            await asyncio.gather(
                                task, return_exceptions=True
                            )
                        else:
                            done, pending = await asyncio.wait(
                                {task},
                                timeout=consumer_cancel_timeout,
                            )
                            if pending:
                                raise RuntimeError(
                                    "Terminal output consumer for instance "
                                    f"{instance_id} ignored cancellation"
                                )
                            await asyncio.gather(
                                *done, return_exceptions=True
                            )
                except Exception:
                    logger.exception(
                        "Terminal output consumer failed while stopping instance %s",
                        instance_id,
                    )
                settled_terminal_consumer = True
        finally:
            if stop_fence_registered:
                self._end_stopping(instance_id)

    async def _stop_locked(
        self,
        instance_id: int,
        *,
        expected_task_id: int | None,
        expected_task_turn_generation: int | object = (
            _EXPECTED_GENERATION_UNSET
        ),
        task_status: str,
        task_error_message: str | None = None,
        expected_pid: int | None | object = _EXPECTED_GENERATION_UNSET,
        expected_started_at: datetime | None | object = _EXPECTED_GENERATION_UNSET,
        consumer_cancel_timeout: float | None = None,
        allow_settled_cleanup: bool = False,
        verified_owner: Instance | None | object = (
            _EXPECTED_GENERATION_UNSET
        ),
        yield_to_worker_task_termination: bool = True,
        worker_termination_operation_id: str | None = None,
        worker_termination_operation: str | None = None,
        worker_termination_execution_token: str | None = None,
        worker_termination_state_version: int | None = None,
        exact_process: Any | object = _EXPECTED_GENERATION_UNSET,
        exact_record: _OutputConsumerRecord | object = (
            _EXPECTED_GENERATION_UNSET
        ),
        exact_task: asyncio.Task | object = _EXPECTED_GENERATION_UNSET,
        exact_launch_params: dict | object = _EXPECTED_GENERATION_UNSET,
    ) -> bool:
        """Serialize an active PTY background epoch against its exact stop."""

        mapped_process = (
            self.processes.get(instance_id)
            or self._process_groups.get(instance_id)
            or self._container_exec_processes.get(instance_id)
        )
        mapped_record = self._consumer_records.get(instance_id)
        mapped_task = self._tasks.get(instance_id)
        if exact_process is _EXPECTED_GENERATION_UNSET:
            process = mapped_process
        else:
            process = exact_process
            if (
                mapped_process is not None
                and mapped_process is not process
            ):
                return False
            for mapped in (
                self.processes.get(instance_id),
                self._process_groups.get(instance_id),
                self._container_exec_processes.get(instance_id),
            ):
                if mapped is not None and mapped is not process:
                    return False
        if exact_record is _EXPECTED_GENERATION_UNSET:
            record = mapped_record
        else:
            record = exact_record
            if (
                mapped_record is not None
                and mapped_record is not record
            ):
                return False
        if record is not None and process is None:
            process = record.process
        if (
            record is not None
            and process is not None
            and record.process is not process
        ):
            return False
        if exact_task is _EXPECTED_GENERATION_UNSET:
            task = (
                record.task
                if record is not None
                else mapped_task
            )
        else:
            task = exact_task
            if mapped_task is not None and mapped_task is not task:
                return False
        if record is not None and task is not None and record.task is not task:
            return False
        session = getattr(process, "session", None)
        session_id = getattr(session, "session_id", None)
        state = (
            self.pty_background_state_for(
                record.task_id,
                session_id,
                self.pty_background_generation_for(
                    record.task_id, session_id
                )
                or "",
            )
            if (
                record is not None
                and record.task_id is not None
                and session_id
            )
            else None
        )
        if state is None and expected_task_id is not None:
            state = self._pty_background_state_for_task(expected_task_id)
        post_exit_proof = self._pty_post_exit_generation_for_instance(
            instance_id,
            expected_task_id,
        )

        async def run_inner() -> bool:
            return await self._stop_locked_inner(
                instance_id,
                expected_task_id=expected_task_id,
                expected_task_turn_generation=(
                    expected_task_turn_generation
                ),
                task_status=task_status,
                task_error_message=task_error_message,
                expected_pid=expected_pid,
                expected_started_at=expected_started_at,
                consumer_cancel_timeout=consumer_cancel_timeout,
                allow_settled_cleanup=allow_settled_cleanup,
                verified_owner=verified_owner,
                yield_to_worker_task_termination=(
                    yield_to_worker_task_termination
                ),
                worker_termination_operation_id=(
                    worker_termination_operation_id
                ),
                worker_termination_operation=worker_termination_operation,
                worker_termination_execution_token=(
                    worker_termination_execution_token
                ),
                worker_termination_state_version=(
                    worker_termination_state_version
                ),
                exact_process=process,
                exact_record=record,
                exact_task=task,
                exact_launch_params=exact_launch_params,
            )

        if state is None and post_exit_proof is None:
            return await run_inner()
        if state is None and post_exit_proof is not None:
            async with self.pty_background_transition(
                post_exit_proof.task_id,
                post_exit_proof.session_id,
            ):
                if (
                    self._pty_post_exit_generations.get(
                        (
                            post_exit_proof.task_id,
                            post_exit_proof.session_id,
                        )
                    )
                    is not post_exit_proof
                ):
                    return False
                return await run_inner()
        async with self.pty_background_transition(
            state.task_id, state.session_id
        ):
            key = (state.task_id, state.session_id)
            handoff = self._pty_autonomous_activity_handoffs.get(key)
            exact_session = state.session
            try:
                stopped = await run_inner()
            except BaseException:
                self._restore_pty_background_after_failed_stop(
                    state,
                    handoff,
                    exact_session,
                    instance_id=instance_id,
                    process=process,
                )
                raise
            if not stopped:
                self._restore_pty_background_after_failed_stop(
                    state,
                    handoff,
                    exact_session,
                    instance_id=instance_id,
                    process=process,
                )
            return stopped

    async def require_stop_session_preflight(self, instance_id: int) -> None:
        """Reject an in-flight target Codex operation before queue mutation."""

        process = (
            self.processes.get(instance_id)
            or self._process_groups.get(instance_id)
            or self._container_exec_processes.get(instance_id)
        )
        from backend.services.codex_app_server import (
            CodexSharedTransportBusyError,
            CodexTurnProcess,
        )

        if not isinstance(process, CodexTurnProcess):
            return
        if (
            self._terminal_stop_recovery_match(
                instance_id,
                process=process,
            )
            is not None
        ):
            # The registry has already detached this terminal adapter. Its
            # exact recovery receipt authorizes DB-only settlement below.
            return
        registry = self._codex_app_server
        codex_home = self._config_dirs.get(instance_id)
        if registry is None or not codex_home:
            raise CodexSharedTransportBusyError(
                "Codex app-server turn has no registered account owner for "
                f"instance {instance_id}"
            )
        await registry.require_claimed_turn_stop_isolated(
            codex_home,
            process,
        )

    def _retain_terminal_stop_recovery(
        self,
        instance_id: int,
        *,
        process: Any | None,
        task: asyncio.Task | None,
        record: _OutputConsumerRecord | None,
        task_id: int | None,
        expected_task_turn_generation: int | object,
        expected_pid: int | None | object,
        expected_started_at: datetime | None | object,
        error: BaseException,
    ) -> None:
        """Keep exact reaped runtime evidence after final stop persistence fails."""

        if (
            process is None
            or task is None
            or record is None
            or record.process is not process
            or record.task is not task
            or record.task_id != task_id
            or record.task_retry_count is None
            or record.task_turn_generation is None
            or not task.done()
            or not self._generation_reap_confirmed(instance_id, process)
            or (
                expected_task_turn_generation is not _EXPECTED_GENERATION_UNSET
                and record.task_turn_generation
                != expected_task_turn_generation
            )
            or (
                expected_pid is not _EXPECTED_GENERATION_UNSET
                and getattr(process, "pid", None) != expected_pid
            )
            or (
                expected_started_at is not _EXPECTED_GENERATION_UNSET
                and record.instance_started_at != expected_started_at
            )
        ):
            return

        recovery_key = (instance_id, process)
        if recovery_key not in self._consumer_recovery_pending:
            unsettled = ConsumerRecoveryUnsettledError(
                "Could not confirm stopped generation settlement for instance "
                f"{instance_id}: {error}"
            )
            self._mark_consumer_recovery_pending(
                instance_id,
                process,
                error=unsettled,
                tracked_generation=True,
                task_id=task_id,
                task_retry_count=record.task_retry_count,
                task_turn_generation=record.task_turn_generation,
                instance_pid=getattr(process, "pid", None),
                instance_started_at=record.instance_started_at,
                consumer=task,
                record=record,
            )

    def _discard_terminal_stop_runtime(
        self,
        instance_id: int,
        *,
        process: Any | None,
        task: asyncio.Task | None,
        record: _OutputConsumerRecord | None,
        launch_params: dict | None = None,
    ) -> None:
        """Drop only the exact runtime whose durable stop is confirmed."""

        if process is not None and self.processes.get(instance_id) is process:
            self.processes.pop(instance_id, None)
            self._codex_exec_homes.pop(instance_id, None)
        if (
            process is not None
            and self._process_groups.get(instance_id) is process
        ):
            self._process_groups.pop(instance_id, None)
        if task is not None and self._tasks.get(instance_id) is task:
            self._tasks.pop(instance_id, None)
        current_record = self._consumer_records.get(instance_id)
        if record is not None and current_record is record:
            self._consumer_records.pop(instance_id, None)
        if (
            launch_params is not None
            and self._launch_params.get(instance_id) is launch_params
        ):
            self._launch_params.pop(instance_id, None)
        if process is not None:
            native_session = getattr(process, "session", None)
            if (
                native_session is not None
                and getattr(native_session, "is_alive", True) is False
            ):
                self._release_task_runtime_scope_pty_owner(native_session)
            if self._generation_reap_confirmed(instance_id, process):
                self._release_task_runtime_scope_direct_owner(
                    instance_id,
                    process,
                )
        self._clear_consumer_recovery_pending(instance_id, process)
        self._transient_attempts.pop(instance_id, None)
        self._pty_rate_limit_seen.discard(instance_id)
        self._pty_rate_limit_info.pop(instance_id, None)

    async def _stop_finalization_is_durable(
        self,
        instance_id: int,
        *,
        process: Any | None,
        task_id: int | None,
        expected_task_retry_count: int | None,
        expected_task_turn_generation: int | object,
        expected_pid: int | None | object,
        expected_started_at: datetime | None | object,
        task_status: str,
    ) -> bool:
        """Resolve an ambiguous commit ACK from exact durable end state."""

        if process is None and (
            (
                expected_pid is not _EXPECTED_GENERATION_UNSET
                and expected_pid is not None
            )
            or (
                expected_started_at is not _EXPECTED_GENERATION_UNSET
                and expected_started_at is not None
            )
        ):
            # A durable terminal row is not proof that an unknown runtime
            # generation was reaped.  Require the exact process handle when
            # the caller supplied generation fences.
            return False
        if process is not None and not self._generation_reap_confirmed(
            instance_id, process
        ):
            return False
        if (
            expected_pid is not _EXPECTED_GENERATION_UNSET
            and (
                (
                    process is None
                    and expected_pid is not None
                )
                or (
                    process is not None
                    and getattr(process, "pid", None) != expected_pid
                )
            )
        ):
            return False
        if (
            task_id is not None
            and (
                expected_task_retry_count is None
                or expected_task_turn_generation
                is _EXPECTED_GENERATION_UNSET
            )
        ):
            return False
        try:
            async with self.db_factory() as db:
                instance_row = (
                    await db.execute(
                        select(
                            Instance.status,
                            Instance.pid,
                            Instance.process_identity,
                            Instance.started_at,
                            Instance.current_task_id,
                        ).where(Instance.id == instance_id)
                    )
                ).one_or_none()
                task_row = (
                    (
                        await db.execute(
                            select(
                                Task.status,
                                Task.retry_count,
                                Task.turn_generation,
                                Task.started_at,
                                Task.instance_id,
                                Task.pty_background_generation,
                            ).where(Task.id == task_id)
                        )
                    ).one_or_none()
                    if task_id is not None
                    else None
                )
                await db.rollback()
        except Exception:
            return False

        if (
            instance_row is None
            or instance_row.current_task_id is not None
            or instance_row.pid is not None
            or instance_row.process_identity is not None
            or instance_row.status
            != ("error" if task_status == "failed" else "idle")
            or (
                expected_started_at is not _EXPECTED_GENERATION_UNSET
                and instance_row.started_at != expected_started_at
            )
        ):
            return False
        if task_id is None:
            return True
        assert task_row is not None or task_id is not None
        if task_row is None:
            return False
        task_status_is_settled = (
            task_row.status == task_status
            or task_row.status not in {"executing", "in_progress", "merging"}
        )
        expected_task_instance_id = (
            None if task_row.status == "pending" else instance_id
        )
        expected_task_started_at = (
            None
            if task_status == "pending"
            else expected_started_at
        )
        return bool(
            task_status_is_settled
            and task_row.retry_count == expected_task_retry_count
            and task_row.turn_generation == expected_task_turn_generation
            and (
                expected_task_started_at is _EXPECTED_GENERATION_UNSET
                or task_row.started_at == expected_task_started_at
            )
            and task_row.instance_id == expected_task_instance_id
            and task_row.pty_background_generation is None
        )

    @asynccontextmanager
    async def _stop_finalization_db(
        self,
        instance_id: int,
        *,
        process: Any | None,
        task: asyncio.Task | None,
        record: _OutputConsumerRecord | None,
        task_id: int | None,
        expected_task_retry_count: int | None,
        expected_task_turn_generation: int | object,
        expected_pid: int | None | object,
        expected_started_at: datetime | None | object,
        task_status: str,
        launch_params: dict | None = None,
        settlement_state: dict[str, bool] | None = None,
    ):
        """Retain exact retry evidence if the final stop transaction fails."""

        try:
            async with self.db_factory() as db:
                yield db
        except BaseException as exc:
            if not isinstance(exc, asyncio.CancelledError) and await (
                self._stop_finalization_is_durable(
                    instance_id,
                    process=process,
                    task_id=task_id,
                    expected_task_retry_count=expected_task_retry_count,
                    expected_task_turn_generation=(
                        expected_task_turn_generation
                    ),
                    expected_pid=expected_pid,
                    expected_started_at=expected_started_at,
                    task_status=task_status,
                )
            ):
                if settlement_state is not None:
                    settlement_state["durable"] = True
                self._discard_terminal_stop_runtime(
                    instance_id,
                    process=process,
                    task=task,
                    record=record,
                    launch_params=launch_params,
                )
                return
            self._retain_terminal_stop_recovery(
                instance_id,
                process=process,
                task=task,
                record=record,
                task_id=task_id,
                expected_task_turn_generation=(
                    expected_task_turn_generation
                ),
                expected_pid=expected_pid,
                expected_started_at=expected_started_at,
                error=exc,
            )
            raise

    async def _stop_locked_inner(
        self,
        instance_id: int,
        *,
        expected_task_id: int | None,
        expected_task_turn_generation: int | object = (
            _EXPECTED_GENERATION_UNSET
        ),
        task_status: str,
        task_error_message: str | None = None,
        expected_pid: int | None | object = _EXPECTED_GENERATION_UNSET,
        expected_started_at: datetime | None | object = _EXPECTED_GENERATION_UNSET,
        consumer_cancel_timeout: float | None = None,
        allow_settled_cleanup: bool = False,
        verified_owner: Instance | None | object = (
            _EXPECTED_GENERATION_UNSET
        ),
        yield_to_worker_task_termination: bool = True,
        worker_termination_operation_id: str | None = None,
        worker_termination_operation: str | None = None,
        worker_termination_execution_token: str | None = None,
        worker_termination_state_version: int | None = None,
        exact_process: Any | object = _EXPECTED_GENERATION_UNSET,
        exact_record: _OutputConsumerRecord | object = (
            _EXPECTED_GENERATION_UNSET
        ),
        exact_task: asyncio.Task | object = _EXPECTED_GENERATION_UNSET,
        exact_launch_params: dict | object = _EXPECTED_GENERATION_UNSET,
    ) -> bool:
        """Stop a running Claude Code instance via SIGINT (interrupt).

        Sends SIGINT first so Claude can gracefully save session state,
        then falls back to SIGTERM and SIGKILL if needed.
        """
        if (
            yield_to_worker_task_termination
            != (worker_termination_operation_id is None)
        ):
            return False
        mapped_process = (
            self.processes.get(instance_id)
            or self._process_groups.get(instance_id)
            or self._container_exec_processes.get(instance_id)
        )
        mapped_record = self._consumer_records.get(instance_id)
        mapped_task = self._tasks.get(instance_id)
        if exact_process is _EXPECTED_GENERATION_UNSET:
            process = mapped_process
        else:
            process = exact_process
            for mapped in (
                mapped_process,
                self.processes.get(instance_id),
                self._process_groups.get(instance_id),
                self._container_exec_processes.get(instance_id),
            ):
                if mapped is not None and mapped is not process:
                    return False
        if exact_record is _EXPECTED_GENERATION_UNSET:
            record = mapped_record
        else:
            record = exact_record
            if mapped_record is not None and mapped_record is not record:
                return False
        if process is None and record is not None:
            process = record.process
        if (
            record is not None
            and process is not None
            and record.process is not process
        ):
            return False
        if exact_task is _EXPECTED_GENERATION_UNSET:
            task = (
                record.task
                if record is not None
                else mapped_task
            )
        else:
            task = exact_task
            if mapped_task is not None and mapped_task is not task:
                return False
        if record is not None and task is not None and record.task is not task:
            return False
        launch_params = (
            self._launch_params.get(instance_id)
            if exact_launch_params is _EXPECTED_GENERATION_UNSET
            else exact_launch_params
        )
        post_exit_proof = self._pty_post_exit_generation_for_instance(
            instance_id,
            expected_task_id,
        )
        if (
            exact_process is _EXPECTED_GENERATION_UNSET
            and process is None
            and post_exit_proof is not None
        ):
            process = post_exit_proof.process
            record = post_exit_proof.record
            if exact_task is _EXPECTED_GENERATION_UNSET:
                task = record.task
        recovery_match = self._terminal_stop_recovery_match(
            instance_id,
            process=process,
            expected_task_id=(
                expected_task_id
                if expected_task_id is not None
                else _EXPECTED_GENERATION_UNSET
            ),
            expected_task_turn_generation=expected_task_turn_generation,
            expected_pid=expected_pid,
            expected_started_at=expected_started_at,
        )
        if recovery_match is None and any(
            recovery_instance_id == instance_id
            and evidence.tracked_generation
            for (recovery_instance_id, _), evidence
            in self._consumer_recovery_pending.items()
        ):
            # A retained terminal receipt owns an older exact generation.  A
            # partial replacement (record/task installed before its process
            # map, or a rapidly reaped replacement) must never be mistaken
            # for that receipt and cancelled with the old Task id.
            return False
        if process is None and recovery_match is not None:
            process = recovery_match[0]
        if recovery_match is not None:
            # The consumer done callback may have already removed the
            # instance-keyed maps.  Recovery evidence owns the exact
            # generation handles in that case; never reconstruct a terminal
            # Codex adapter in the live registry/maps.
            recovery_record = recovery_match[1].record
            recovery_task = recovery_match[1].consumer
            if record is None:
                record = recovery_record
            elif recovery_record is not None and record is not recovery_record:
                return False
            if recovery_task is not None:
                if task is not None and task is not recovery_task:
                    return False
                task = recovery_task
        direct_recovery_evidence = (
            self._consumer_recovery_pending.get((instance_id, process))
            if process is not None
            else None
        )
        recovery_evidence = (
            recovery_match[1]
            if recovery_match is not None
            and recovery_match[0] is process
            else (
                direct_recovery_evidence
                if direct_recovery_evidence is not None
                and not direct_recovery_evidence.tracked_generation
                else None
            )
        )
        # A tracked recovery record supplies the durable per-turn token even
        # when the caller is a generic lifecycle cleanup.  An untracked record
        # may only be reconciled when the caller independently supplies both
        # exact Instance fences.
        if recovery_evidence is not None:
            if recovery_evidence.tracked_generation:
                effective_expected_pid = recovery_evidence.instance_pid
                effective_expected_started_at = (
                    recovery_evidence.instance_started_at
                )
            else:
                if (
                    expected_pid is _EXPECTED_GENERATION_UNSET
                    or expected_started_at is _EXPECTED_GENERATION_UNSET
                ):
                    return False
                effective_expected_pid = expected_pid
                effective_expected_started_at = expected_started_at
        else:
            effective_expected_pid = expected_pid
            effective_expected_started_at = expected_started_at

        if (
            expected_task_turn_generation is not _EXPECTED_GENERATION_UNSET
        ):
            async with self.db_factory() as db:
                current_task_turn_generation = await db.scalar(
                    select(Task.turn_generation).where(
                        Task.id == expected_task_id
                    )
                )
            if (
                current_task_turn_generation
                != expected_task_turn_generation
            ):
                return False

        if (
            expected_task_id is not None
            or effective_expected_pid is not _EXPECTED_GENERATION_UNSET
            or effective_expected_started_at is not _EXPECTED_GENERATION_UNSET
        ):
            if verified_owner is _EXPECTED_GENERATION_UNSET:
                async with self.db_factory() as db:
                    owner = await db.get(Instance, instance_id)
            else:
                owner = verified_owner
            if (
                owner is None
                or (
                    expected_task_id is not None
                    and owner.current_task_id != expected_task_id
                )
                or (
                    effective_expected_pid
                    is not _EXPECTED_GENERATION_UNSET
                    and owner.pid != effective_expected_pid
                )
                or (
                    effective_expected_started_at
                    is not _EXPECTED_GENERATION_UNSET
                    and owner.started_at
                    != effective_expected_started_at
                )
            ):
                return False

        process_live = (
            process is not None
            and not self._generation_reap_confirmed(instance_id, process)
        )
        if (
            process_live
            and expected_task_turn_generation
            is not _EXPECTED_GENERATION_UNSET
            and not (
                record is not None
                and record.task_id == expected_task_id
                and record.task_turn_generation
                == expected_task_turn_generation
            )
        ):
            # PID/start time do not distinguish hot PTY turns. Never signal a
            # live process unless its in-memory consumer proves the same turn.
            return False
        consumer_live = task is not None and not task.done()
        stopping_background_state = (
            self._pty_background_state_for_task(expected_task_id)
            if expected_task_id is not None
            else None
        )
        if not process_live and not consumer_live and not allow_settled_cleanup:
            return False
        if (
            not process_live
            and not consumer_live
            and stopping_background_state is not None
            and getattr(
                stopping_background_state.session, "is_alive", True
            )
            is not False
        ):
            return False

        # Use a fresh write transaction immediately before any native-session
        # stop or POSIX signal.  This catches receipts already durable before
        # the physical side effect and also refreshes the exact reverse owner
        # after the earlier lifecycle snapshot.  We intentionally release the
        # DB locks before waiting on a process: a receipt admitted in that
        # unavoidable post-check window becomes the sole durable terminal
        # writer because the final Task/Instance CAS below repeats this gate.
        stop_task_id = expected_task_id
        if stop_task_id is None and recovery_evidence is not None:
            stop_task_id = recovery_evidence.task_id
        if stop_task_id is None and record is not None:
            stop_task_id = record.task_id
        async with self.db_factory() as db:
            if stop_task_id is None:
                discovered_task_id = await db.scalar(
                    select(Instance.current_task_id).where(
                        Instance.id == instance_id
                    )
                )
                stop_task_id = (
                    discovered_task_id
                    if type(discovered_task_id) is int
                    and discovered_task_id > 0
                    else None
                )

        def stop_task_identity_predicates() -> list[Any]:
            assert stop_task_id is not None
            return [
                Task.id == stop_task_id,
                Task.instance_id == instance_id,
                (
                    Task.id == stop_task_id
                    if expected_task_turn_generation
                    is _EXPECTED_GENERATION_UNSET
                    else Task.turn_generation
                    == expected_task_turn_generation
                ),
                (
                    Task.id == stop_task_id
                    if recovery_evidence is None
                    or recovery_evidence.task_retry_count is None
                    else Task.retry_count
                    == recovery_evidence.task_retry_count
                ),
                (
                    Task.id == stop_task_id
                    if recovery_evidence is None
                    or recovery_evidence.task_turn_generation is None
                    else Task.turn_generation
                    == recovery_evidence.task_turn_generation
                ),
            ]

        def stop_instance_identity_predicates() -> list[Any]:
            assert stop_task_id is not None
            predicates: list[Any] = [
                Instance.id == instance_id,
                Instance.current_task_id == stop_task_id,
            ]
            if effective_expected_pid is not _EXPECTED_GENERATION_UNSET:
                predicates.append(Instance.pid == effective_expected_pid)
            if effective_expected_started_at is not _EXPECTED_GENERATION_UNSET:
                predicates.append(
                    Instance.started_at.is_(None)
                    if effective_expected_started_at is None
                    else Instance.started_at == effective_expected_started_at
                )
            return predicates

        async def fresh_stop_effect_authority() -> bool:
            if stop_task_id is None:
                return worker_termination_operation_id is None
            async with self.db_factory() as authority_db:
                lease_valid_at = (
                    await _lock_worker_termination_stop_authority(
                        authority_db,
                        task_id=stop_task_id,
                        instance_id=instance_id,
                        task_predicates=stop_task_identity_predicates(),
                        instance_predicates=(
                            stop_instance_identity_predicates()
                        ),
                        operation_id=worker_termination_operation_id,
                        operation=worker_termination_operation,
                        execution_token=(
                            worker_termination_execution_token
                        ),
                        state_version=worker_termination_state_version,
                    )
                )
                return lease_valid_at is not None

        if not await fresh_stop_effect_authority():
            return False

        post_exit_proof_is_attached = bool(
            post_exit_proof is not None
            and self._pty_backend is not None
            and getattr(self._pty_backend, "_sessions", {}).get(instance_id)
            is post_exit_proof.session
        )
        if post_exit_proof is not None and not post_exit_proof_is_attached:
            if not await fresh_stop_effect_authority():
                return False
            if not await self._stop_exact_post_exit_pty_session(
                post_exit_proof
            ):
                return False

        pty_managed = (
            process_live
            and self._pty_backend is not None
            and instance_id in getattr(self._pty_backend, "_sessions", {})
        )
        from backend.services.codex_app_server import (
            CodexSharedTransportBusyError,
            CodexTurnProcess,
        )

        codex_app_server_managed = (
            process_live and isinstance(process, CodexTurnProcess)
        )
        if codex_app_server_managed:
            # A CodexTurnProcess is an adapter for one native turn; its PID is
            # the persistent, account-scoped app-server transport rather than
            # a task-owned POSIX process.  Generic SIGINT/TERM/KILL escalation
            # only repeats the turn RPC and then waits forever for the shared
            # transport PID to exit.  This is especially visible after the
            # parent turn reaches terminal while a descendant tool remains
            # active: descendant cleanup can fail, yet killing the adapter
            # cannot reap that native work.
            #
            # This is a durably claimed turn.  The registry unloads only the
            # target lineage when a peer shares the account process, and may
            # use whole-transport shutdown only for an isolated Task.  If
            # exact cleanup cannot be proven, retain every runtime and durable
            # owner so the API can return a retryable conflict.
            registry = self._codex_app_server
            codex_home = self._config_dirs.get(instance_id)
            if registry is None or not codex_home:
                raise RuntimeError(
                    "Codex app-server turn has no registered account owner "
                    f"for instance {instance_id}"
                )
            if not await fresh_stop_effect_authority():
                return False
            try:
                await registry.stop_claimed_turn(
                    codex_home,
                    process,
                    reason="CCM task session interrupted",
                )
            except CodexSharedTransportBusyError as exc:
                logger.warning(
                    "Keeping claimed Codex turn active because its shared "
                    "transport cannot be isolated for instance %s: %s",
                    instance_id,
                    exc,
                )
                return False
            try:
                await asyncio.wait_for(
                    asyncio.shield(process.wait()),
                    timeout=10.0,
                )
            except asyncio.TimeoutError as exc:
                raise RuntimeError(
                    f"Codex app-server turn for instance {instance_id} "
                    "survived account transport cleanup"
                ) from exc
        elif pty_managed:
            # Esc-interrupt the turn, then tear the session down; the proxy's
            # wait() is unblocked by the backend's on_exit.  Claim terminal
            # ownership before awaiting the consumer: FullMirror.on_exit then
            # skips its lifecycle-locking DB finalizer and leaves the exact
            # Task/Instance transaction to this stop call.  If the consumer
            # already claimed first, _stop_serialized must have routed through
            # its lock-free terminal-consumer wait instead.
            if not await fresh_stop_effect_authority():
                return False
            if record is not None and record.process is process:
                terminal_owner = self._claim_pty_terminal_owner(
                    record,
                    "stop",
                    take_over_completed_consumer=True,
                    take_over_background_waiter=True,
                )
                if terminal_owner != "stop":
                    raise RuntimeError(
                        "PTY consumer already owns terminal finalization; "
                        "stop must wait outside the lifecycle lock"
                    )
                if record.task_id is not None:
                    session = getattr(process, "session", None)
                    session_id = getattr(session, "session_id", None)
                    if session_id:
                        generation = self.pty_background_generation_for(
                            record.task_id, session_id
                        )
                        if generation:
                            stopping_background_state = (
                                self.pty_background_state_for(
                                    record.task_id,
                                    session_id,
                                    generation,
                                )
                            )
                            self.abandon_pty_background_generation(
                                record.task_id,
                                session_id,
                                generation,
                                outcome=(
                                    "failed"
                                    if task_status == "failed"
                                    else "abandoned"
                                ),
                            )
            container_signal_error: Exception | None = None
            if self._is_managed_container_exec(instance_id, process):
                if not await fresh_stop_effect_authority():
                    if record is not None and record.pty_terminal_owner == "stop":
                        record.pty_terminal_owner = None
                    return False
                try:
                    await self._container_mgr.signal_exec(
                        process, signal.SIGINT
                    )
                except Exception as exc:
                    container_signal_error = exc
                    logger.exception(
                        "Could not interrupt container PTY for instance %s",
                        instance_id,
                    )
            if not await fresh_stop_effect_authority():
                if record is not None and record.pty_terminal_owner == "stop":
                    record.pty_terminal_owner = None
                return False
            # Follow-up prompts reuse the retained Session but are separate
            # event pumps. Once this exact stop owns the PTY generation, cancel
            # those pumps before tearing down the native process so no late
            # follow-up event can mutate the stopped Task.
            await self._cancel_pty_followup_tasks({instance_id})
            await self._pty_backend.stop(instance_id)
            # claude-pty normally completes its asyncio-compatible proxy from
            # the consumer's on_exit callback. A forced Interrupt may cancel
            # that consumer after the native Session has already reaped the
            # Claude process, leaving proxy.wait() blocked forever even though
            # there is no live child left. Bridge only from exact native death
            # evidence; a still-live or unknown Session remains fail-closed.
            if process.returncode is None:
                native_session = getattr(process, "session", None)
                native_dead = (
                    native_session is not None
                    and getattr(native_session, "is_alive", None) is False
                )
                complete_proxy = getattr(process, "complete", None)
                if native_dead and callable(complete_proxy):
                    native_process = getattr(native_session, "_process", None)
                    native_exit_code = getattr(
                        native_process, "exit_code", None
                    )
                    complete_proxy(
                        native_exit_code
                        if isinstance(native_exit_code, int)
                        else 130
                    )
            try:
                await self._wait_process_tree(instance_id, process, 10.0)
            except asyncio.TimeoutError:
                if not await fresh_stop_effect_authority():
                    if (
                        record is not None
                        and record.pty_terminal_owner == "stop"
                    ):
                        record.pty_terminal_owner = None
                    return False
                try:
                    await self._signal_managed_process_tree(
                        instance_id, process, signal.SIGKILL
                    )
                    await self._wait_process_tree(
                        instance_id, process, 5.0
                    )
                except (asyncio.TimeoutError, RuntimeError) as exc:
                    raise RuntimeError(
                        f"PTY process for instance {instance_id} survived SIGKILL"
                    ) from exc
            if container_signal_error is not None:
                raise RuntimeError(
                    f"Container PTY state for instance {instance_id} "
                    "could not be controlled"
                ) from container_signal_error
        elif process_live:
            if not await fresh_stop_effect_authority():
                return False
            await self._signal_managed_process_tree(
                instance_id, process, signal.SIGINT
            )
            try:
                await self._wait_process_tree(instance_id, process, 10.0)
            except asyncio.TimeoutError:
                if not await fresh_stop_effect_authority():
                    return False
                await self._signal_managed_process_tree(
                    instance_id, process, signal.SIGTERM
                )
                try:
                    await self._wait_process_tree(instance_id, process, 5.0)
                except asyncio.TimeoutError:
                    if not await fresh_stop_effect_authority():
                        return False
                    await self._signal_managed_process_tree(
                        instance_id, process, signal.SIGKILL
                    )
                    try:
                        await self._wait_process_tree(instance_id, process, 5.0)
                    except asyncio.TimeoutError:
                        logger.error(
                            "Process group for instance %s survived SIGKILL",
                            instance_id,
                        )
                        raise RuntimeError(
                            f"Process group for instance {instance_id} survived SIGKILL"
                        )

        # Cancel consumer task
        if task and not task.done():
            if not await fresh_stop_effect_authority():
                if record is not None and record.pty_terminal_owner == "stop":
                    record.pty_terminal_owner = None
                return False
            task.cancel()
        if task:
            # The consumer's stopping branch still drains process/stderr state.
            # Reap that exact task before the lifecycle lock can admit a new
            # process under the same instance id.
            if consumer_cancel_timeout is None:
                await asyncio.gather(task, return_exceptions=True)
            else:
                done, pending = await asyncio.wait(
                    {task},
                    timeout=consumer_cancel_timeout,
                )
                if pending:
                    raise RuntimeError(
                        "Output consumer for instance "
                        f"{instance_id} ignored cancellation"
                    )
                await asyncio.gather(*done, return_exceptions=True)

        if expected_task_id is not None:
            task_id = expected_task_id
        elif recovery_evidence is not None:
            task_id = recovery_evidence.task_id
        elif record is not None:
            task_id = record.task_id
        else:
            task_id = None
        if task_id is None:
            # Discovery is a plain snapshot outside the terminal transaction;
            # the exact Instance CAS below validates it.  Never lock Instance
            # and then Task in one transaction.
            async with self.db_factory() as db:
                task_id = (
                    await db.execute(
                        select(Instance.current_task_id).where(
                            Instance.id == instance_id
                        )
                    )
                ).scalar_one_or_none()

        changed_task_status = False
        cleared_background = False
        cleared_background_generation: str | None = None
        published_generation: dict | None = None
        finalization_state: dict[str, bool] = {}
        async with self._stop_finalization_db(
            instance_id,
            process=process,
            task=task,
            record=record,
            task_id=task_id,
            expected_task_retry_count=(
                recovery_evidence.task_retry_count
                if recovery_evidence is not None
                else (
                    record.task_retry_count
                    if record is not None
                    else None
                )
            ),
            expected_task_turn_generation=expected_task_turn_generation,
            expected_pid=effective_expected_pid,
            expected_started_at=effective_expected_started_at,
            task_status=task_status,
            launch_params=launch_params,
            settlement_state=finalization_state,
        ) as db:
            final_lease_valid_at: datetime | None = None
            if task_id is not None:
                # Global ownership lock order is Task -> Instance.  A no-op
                # UPDATE is portable across SQLite/PostgreSQL/MySQL and also
                # locks an already-terminal Task that cancellation published
                # before asking us to clear its reverse Instance owner.
                task_lock_predicates = [
                    Task.id == task_id,
                    Task.instance_id == instance_id,
                    (
                        Task.id == task_id
                        if expected_task_turn_generation
                        is _EXPECTED_GENERATION_UNSET
                        else Task.turn_generation
                        == expected_task_turn_generation
                    ),
                    (
                        Task.id == task_id
                        if recovery_evidence is None
                        or recovery_evidence.task_retry_count is None
                        else Task.retry_count
                        == recovery_evidence.task_retry_count
                    ),
                    (
                        Task.id == task_id
                        if recovery_evidence is None
                        or recovery_evidence.task_turn_generation is None
                        else Task.turn_generation
                        == recovery_evidence.task_turn_generation
                    ),
                ]
                final_instance_lock_predicates = [
                    Instance.id == instance_id,
                    Instance.current_task_id == task_id,
                ]
                if effective_expected_pid is not _EXPECTED_GENERATION_UNSET:
                    final_instance_lock_predicates.append(
                        Instance.pid == effective_expected_pid
                    )
                if (
                    effective_expected_started_at
                    is not _EXPECTED_GENERATION_UNSET
                ):
                    final_instance_lock_predicates.append(
                        Instance.started_at.is_(None)
                        if effective_expected_started_at is None
                        else Instance.started_at
                        == effective_expected_started_at
                    )
                final_lease_valid_at = (
                    await _lock_worker_termination_stop_authority(
                        db,
                        task_id=task_id,
                        instance_id=instance_id,
                        task_predicates=task_lock_predicates,
                        instance_predicates=final_instance_lock_predicates,
                        operation_id=worker_termination_operation_id,
                        operation=worker_termination_operation,
                        execution_token=(
                            worker_termination_execution_token
                        ),
                        state_version=worker_termination_state_version,
                    )
                )
                if final_lease_valid_at is None:
                    return False

                current_task_generation = (
                    await db.execute(
                        select(
                            Task.status,
                            Task.retry_count,
                            Task.turn_generation,
                            Task.instance_id,
                            Task.started_at,
                            Task.completed_at,
                            Task.pty_background_generation,
                        ).where(Task.id == task_id)
                    )
                ).one()
                if current_task_generation.status in {
                    "executing",
                    "in_progress",
                    "merging",
                }:
                    task_values: dict = {
                        "status": task_status,
                        "error_message": (
                            (task_error_message or "")[:2000] or None
                            if task_status == "failed"
                            else None
                        ),
                        "pty_background_generation": None,
                    }
                    if task_status == "pending":
                        task_values.update(
                            instance_id=None,
                            started_at=None,
                            completed_at=None,
                        )
                    else:
                        task_values["completed_at"] = datetime.utcnow()
                    task_update = await db.execute(
                        update(Task)
                        .where(
                            Task.id == task_id,
                            Task.status == current_task_generation.status,
                            Task.retry_count
                            == current_task_generation.retry_count,
                            Task.turn_generation
                            == current_task_generation.turn_generation,
                            Task.instance_id == instance_id,
                            *_worker_termination_stop_predicates(
                                worker_termination_operation_id,
                                worker_termination_operation,
                                worker_termination_execution_token,
                                worker_termination_state_version,
                                final_lease_valid_at,
                            ),
                        )
                        .values(**task_values)
                    )
                    changed_task_status = bool(task_update.rowcount)
                    cleared_background = bool(
                        task_update.rowcount
                        and current_task_generation
                        .pty_background_generation is not None
                    )
                    if cleared_background:
                        cleared_background_generation = (
                            current_task_generation
                            .pty_background_generation
                        )
                elif (
                    current_task_generation.pty_background_generation
                    is not None
                ):
                    background_clear = await db.execute(
                        update(Task)
                        .where(
                            Task.id == task_id,
                            Task.status
                            == current_task_generation.status,
                            Task.retry_count
                            == current_task_generation.retry_count,
                            Task.turn_generation
                            == current_task_generation.turn_generation,
                            Task.instance_id == instance_id,
                            Task.pty_background_generation
                            == current_task_generation
                            .pty_background_generation,
                            *_worker_termination_stop_predicates(
                                worker_termination_operation_id,
                                worker_termination_operation,
                                worker_termination_execution_token,
                                worker_termination_state_version,
                                final_lease_valid_at,
                            ),
                        )
                        .values(pty_background_generation=None)
                    )
                    cleared_background = bool(
                        background_clear.rowcount
                    )
                    if cleared_background:
                        cleared_background_generation = (
                            current_task_generation
                            .pty_background_generation
                        )

                if changed_task_status or cleared_background:
                    from backend.models.sub_agent import SubAgentSession

                    await db.execute(
                        update(SubAgentSession)
                        .where(
                            SubAgentSession.task_id == task_id,
                            SubAgentSession.source == "native",
                            SubAgentSession.status == "running",
                        )
                        .values(
                            status=(
                                "failed"
                                if task_status == "failed"
                                else "cancelled"
                            ),
                            completed_at=datetime.utcnow(),
                        )
                    )

                resulting_task_generation = (
                    await db.execute(
                        select(
                            Task.status,
                            Task.retry_count,
                            Task.turn_generation,
                            Task.instance_id,
                            Task.started_at,
                            Task.completed_at,
                            Task.pty_background_generation,
                        ).where(Task.id == task_id)
                    )
                ).one()
                published_generation = {
                    "status": resulting_task_generation.status,
                    "retry_count": resulting_task_generation.retry_count,
                    "turn_generation": (
                        resulting_task_generation.turn_generation
                    ),
                    "instance_id": resulting_task_generation.instance_id,
                    "started_at": resulting_task_generation.started_at,
                    "completed_at": resulting_task_generation.completed_at,
                    "pty_background_generation": (
                        resulting_task_generation
                        .pty_background_generation
                    ),
                }

            # Sub-agent cleanup above can itself wait on locks. Sample again
            # immediately before the reverse-owner CAS so an execution lease
            # that expired during that wait cannot clear the Instance row.
            instance_lease_valid_at = (
                datetime.utcnow() if task_id is not None else None
            )
            instance_predicates = [
                Instance.id == instance_id,
                _worker_termination_instance_stop_predicate(
                    worker_termination_operation_id,
                    worker_termination_operation,
                    worker_termination_execution_token,
                    worker_termination_state_version,
                    instance_lease_valid_at,
                ),
            ]
            if task_id is not None:
                instance_predicates.append(
                    Instance.current_task_id == task_id
                )
            if effective_expected_pid is not _EXPECTED_GENERATION_UNSET:
                instance_predicates.append(
                    Instance.pid == effective_expected_pid
                )
            if (
                effective_expected_started_at
                is not _EXPECTED_GENERATION_UNSET
            ):
                instance_predicates.append(
                    (
                        Instance.started_at.is_(None)
                        if effective_expected_started_at is None
                        else Instance.started_at
                        == effective_expected_started_at
                    )
                )
            instance_update = await db.execute(
                update(Instance)
                .where(*instance_predicates)
                .values(
                    status=(
                        "error" if task_status == "failed" else "idle"
                    ),
                    pid=None,
                    process_identity=None,
                    current_task_id=None,
                )
            )
            if instance_update.rowcount == 0:
                await db.rollback()
                return False
            await db.commit()

        if task_id is not None:
            # The Task+Instance exact stop CAS above is now durable. Any
            # pre-terminal post-exit bridge for that owner is obsolete; remove
            # it before terminal publication so a queued old callback cannot
            # revive the stopped generation.
            self.discard_pty_post_exit_generations(
                task_id=task_id,
                instance_id=instance_id,
                invalidate_handoffs=True,
            )

        if (
            stopping_background_state is not None
            and cleared_background
            and stopping_background_state.generation
            == cleared_background_generation
        ):
            stopping_background_state.outcome = (
                "failed" if task_status == "failed" else "abandoned"
            )
            self.clear_pty_autonomous_activity_handoff(
                stopping_background_state.task_id,
                stopping_background_state.session_id,
                self._pty_autonomous_activity_handoffs.get(
                    (
                        stopping_background_state.task_id,
                        stopping_background_state.session_id,
                    )
                ),
            )
            self._discard_pty_background_state(
                (
                    stopping_background_state.task_id,
                    stopping_background_state.session_id,
                ),
                stopping_background_state.generation,
            )

        if (
            task_id is not None
            and published_generation is not None
            and not finalization_state.get("durable")
        ):
            # Keep a row lock across publication. A rapid retry/replacement
            # must change one of these exact fields first and therefore
            # suppresses the old generation's status/process-exit events.
            @asynccontextmanager
            async def terminal_publication_guard():
                try:
                    yield
                except asyncio.CancelledError:
                    # The terminal CAS is already durable. Do not let a
                    # cancellation during this best-effort phase reach
                    # _stop_locked, whose failure path would restore a PTY
                    # background generation that has already been settled.
                    logger.warning(
                        "Terminal stop publication cancelled after durable "
                        "settlement for instance %s/task %s",
                        instance_id,
                        task_id,
                    )
                except Exception:
                    # Task/Instance settlement committed before this
                    # best-effort publication phase. A lock or broadcaster
                    # failure must not turn a durable stop into a retryable
                    # runtime owner; the DB context rolls back its pending
                    # publication work on exit and the exact runtime is
                    # discarded below.
                    logger.exception(
                        "Terminal stop publication failed after durable "
                        "settlement for instance %s/task %s",
                        instance_id,
                        task_id,
                    )

            async with terminal_publication_guard(), self.db_factory() as db:
                generation_predicates = [
                    Task.id == task_id,
                    Task.status == published_generation["status"],
                    Task.retry_count == published_generation["retry_count"],
                    Task.turn_generation
                    == published_generation["turn_generation"],
                    (
                        Task.instance_id.is_(None)
                        if published_generation["instance_id"] is None
                        else Task.instance_id
                        == published_generation["instance_id"]
                    ),
                    (
                        Task.started_at.is_(None)
                        if published_generation["started_at"] is None
                        else Task.started_at
                        == published_generation["started_at"]
                    ),
                    (
                        Task.completed_at.is_(None)
                        if published_generation["completed_at"] is None
                        else Task.completed_at
                        == published_generation["completed_at"]
                    ),
                    (
                        Task.pty_background_generation.is_(None)
                        if published_generation[
                            "pty_background_generation"
                        ] is None
                        else Task.pty_background_generation
                        == published_generation[
                            "pty_background_generation"
                        ]
                    ),
                ]
                publication_lease_valid_at = (
                    await _lock_worker_termination_stop_authority(
                        db,
                        task_id=task_id,
                        instance_id=instance_id,
                        task_predicates=generation_predicates,
                        instance_predicates=(
                            Instance.id == instance_id,
                            Instance.current_task_id.is_(None),
                            Instance.pid.is_(None),
                            Instance.status
                            == (
                                "error"
                                if task_status == "failed"
                                else "idle"
                            ),
                        ),
                        operation_id=worker_termination_operation_id,
                        operation=worker_termination_operation,
                        execution_token=(
                            worker_termination_execution_token
                        ),
                        state_version=worker_termination_state_version,
                    )
                )
                if publication_lease_valid_at is not None:
                    generation_predicates.extend(
                        _worker_termination_stop_predicates(
                            worker_termination_operation_id,
                            worker_termination_operation,
                            worker_termination_execution_token,
                            worker_termination_state_version,
                            publication_lease_valid_at,
                        )
                    )
                    publish_guard = await db.execute(
                        update(Task)
                        .where(*generation_predicates)
                        .values(status=published_generation["status"])
                    )
                else:
                    publish_guard = None
                if publish_guard is not None and publish_guard.rowcount:
                    async def publication_authority_is_live() -> bool:
                        """Re-sample the locked receipt before each effect."""

                        receipt = (
                            await active_worker_task_termination_receipt(
                                db,
                                task_id,
                                for_update=True,
                            )
                        )
                        lease_valid_at = datetime.utcnow()
                        if worker_task_termination_authority_matches(
                            receipt,
                            operation_id=(
                                worker_termination_operation_id
                            ),
                            operation=worker_termination_operation,
                            execution_token=(
                                worker_termination_execution_token
                            ),
                            state_version=(
                                worker_termination_state_version
                            ),
                            lease_valid_at=lease_valid_at,
                        ):
                            return True
                        # The durable Task/Instance stop was committed above.
                        # Release publication locks and suppress every
                        # remaining best-effort event for this expired owner.
                        await db.rollback()
                        return False

                    publication_live = True
                    if changed_task_status:
                        # InstanceManager already owns the exact broadcaster
                        # for this runtime.  Importing ``backend.main`` through
                        # task_events here creates an application-entrypoint
                        # cycle and can run startup recovery while an exact
                        # stop still holds its transition lock.
                        publication_live = (
                            await publication_authority_is_live()
                        )
                        if publication_live:
                            await self.broadcaster.broadcast(
                                "tasks",
                                {
                                    "event": "status_change",
                                    "task_id": task_id,
                                    "task_retry_count": (
                                        published_generation["retry_count"]
                                    ),
                                    "task_turn_generation": (
                                        published_generation[
                                            "turn_generation"
                                        ]
                                    ),
                                    "new_status": task_status,
                                    "instance_id": instance_id,
                                    "background_active": False,
                                },
                            )
                    elif cleared_background:
                        background_payload = {
                            "event": "background_activity",
                            "event_type": "background_activity",
                            "task_id": task_id,
                            "task_retry_count": published_generation[
                                "retry_count"
                            ],
                            "task_turn_generation": published_generation[
                                "turn_generation"
                            ],
                            "background_active": False,
                        }
                        publication_live = (
                            await publication_authority_is_live()
                        )
                        if publication_live:
                            await self.broadcaster.broadcast(
                                "tasks", background_payload
                            )
                        if publication_live:
                            publication_live = (
                                await publication_authority_is_live()
                            )
                        if publication_live:
                            await self.broadcaster.broadcast(
                                f"task:{task_id}", background_payload
                            )
                    if publication_live:
                        publication_live = (
                            await publication_authority_is_live()
                        )
                    if publication_live:
                        await self.broadcaster.broadcast(
                            f"task:{task_id}",
                            {
                                "event_type": "process_exit",
                                "task_id": task_id,
                                "task_retry_count": published_generation[
                                    "retry_count"
                                ],
                                "task_turn_generation": (
                                    published_generation[
                                        "turn_generation"
                                    ]
                                ),
                                "exit_code": (
                                    process.returncode
                                    if process is not None
                                    else None
                                ),
                                "stderr": (
                                    task_error_message
                                    if task_status == "failed"
                                    else None
                                ),
                            },
                        )
                    if publication_live:
                        await db.commit()

        if finalization_state.get("durable"):
            # The primary transaction committed but its acknowledgement was
            # lost.  Do not emit a second, potentially stale publication pass.
            return True

        self._discard_terminal_stop_runtime(
            instance_id,
            process=process,
            task=task,
            record=record,
            launch_params=launch_params,
        )
        return True

    async def wait_for_output_consumer(
        self,
        instance_id: int,
        *,
        provider: str = "claude",
        timeout: float | None = 30,
        expected_process: asyncio.subprocess.Process | None = None,
        preserve_error: bool = False,
    ) -> None:
        """Wait until an instance's output bookkeeping is fully settled.

        Codex consumers own post-turn rollout migration and account binding, so
        an arbitrary timeout would expose a half-finished native thread to the
        next launch.  They are therefore awaited without a timeout and followed
        across consumer-driven retry replacement.  Claude keeps the historical
        bounded wait, but shields the consumer so a timeout does not cancel its
        remaining output processing.
        """

        provider = (provider or "claude").lower()
        current = asyncio.current_task()
        deadline = None
        if provider != "codex" and timeout is not None:
            deadline = asyncio.get_running_loop().time() + timeout

        while True:
            expected_key = (
                (instance_id, expected_process)
                if expected_process is not None
                else None
            )
            if expected_key is not None:
                recovery_pending = (
                    expected_key in self._consumer_recovery_pending
                )
                stored_error = (
                    self._consumer_errors.get(expected_key)
                    if preserve_error or recovery_pending
                    else self._consumer_errors.pop(expected_key, None)
                )
                if stored_error is not None:
                    raise RuntimeError(
                        f"Output consumer failed for instance {instance_id}"
                    ) from stored_error

            record = self._consumer_records.get(instance_id)
            if record is not None:
                if (
                    expected_process is not None
                    and record.process is not expected_process
                ):
                    # The expected generation has already settled and a newer
                    # turn owns this reusable instance slot.  Never await or
                    # consume errors from that newer generation.
                    return
                consumer = record.task
                process = record.process
            else:
                # Compatibility for tests/legacy integrations that install a
                # bare task directly. Exact-generation waiting deliberately
                # does not attach such a task to an unrelated process.
                if expected_process is not None:
                    return
                consumer = self._tasks.get(instance_id)
                process = self.processes.get(instance_id)
            if consumer is None or consumer is current:
                return
            try:
                if consumer.done():
                    # Retrieve any exception instead of leaving a finished task
                    # unobserved, while preserving its normal propagation.
                    await asyncio.shield(consumer)
                elif provider == "codex":
                    await asyncio.shield(consumer)
                else:
                    remaining = None
                    if deadline is not None:
                        remaining = deadline - asyncio.get_running_loop().time()
                        if remaining <= 0:
                            raise asyncio.TimeoutError
                    await asyncio.wait_for(
                        asyncio.shield(consumer), timeout=remaining
                    )
            except Exception as exc:
                if isinstance(exc, asyncio.TimeoutError) and not consumer.done():
                    # wait_for timed out; the shielded consumer still owns its
                    # generation and must continue draining output.
                    raise

                # If the waiter beats the done callback, clear the exact stale
                # generation here. A generic admission waiter must preserve a
                # managed-turn failure for its lifecycle owner; chat turns have
                # no separate owner and already persist their own failed state.
                failure_key = (
                    (instance_id, process) if process is not None else None
                )
                recovery_pending = bool(
                    failure_key is not None
                    and failure_key in self._consumer_recovery_pending
                )
                if (
                    failure_key is not None
                    and record is not None
                    and (
                        not record.chat_initiated
                        or recovery_pending
                    )
                    and (expected_process is None or preserve_error)
                ):
                    self._consumer_errors.setdefault(failure_key, exc)
                elif failure_key is not None and not recovery_pending:
                    self._consumer_errors.pop(failure_key, None)
                reap_confirmed = (
                    process is None
                    or self._generation_reap_confirmed(instance_id, process)
                )
                if reap_confirmed and not recovery_pending:
                    if self._tasks.get(instance_id) is consumer:
                        self._tasks.pop(instance_id, None)
                    if (
                        record is not None
                        and self._consumer_records.get(instance_id) is record
                    ):
                        self._consumer_records.pop(instance_id, None)
                    if (
                        process is not None
                        and self.processes.get(instance_id) is process
                    ):
                        self.processes.pop(instance_id, None)
                        self._codex_exec_homes.pop(instance_id, None)
                        self._launch_params.pop(instance_id, None)
                        if self._process_groups.get(instance_id) is process:
                            self._process_groups.pop(instance_id, None)
                raise

            # A chat consumer may launch its own replacement on a transient or
            # account-limit retry.  Codex callers must wait for that replacement
            # too before considering the instance reusable.
            if (
                record is not None
                and self._consumer_records.get(instance_id) is record
                and (instance_id, record.process)
                not in self._consumer_recovery_pending
                and (
                    process is None
                    or self._generation_reap_confirmed(instance_id, process)
                )
            ):
                self._consumer_records.pop(instance_id, None)
                if self._tasks.get(instance_id) is consumer:
                    self._tasks.pop(instance_id, None)
                if (
                    process is not None
                    and self.processes.get(instance_id) is process
                    and self._generation_reap_confirmed(instance_id, process)
                ):
                    self.processes.pop(instance_id, None)
                    self._codex_exec_homes.pop(instance_id, None)
                    self._launch_params.pop(instance_id, None)
                    if self._process_groups.get(instance_id) is process:
                        self._process_groups.pop(instance_id, None)

            replacement = self._consumer_records.get(instance_id)
            if (
                expected_process is not None
                or provider != "codex"
                or replacement is None
                or replacement.task is consumer
            ):
                return

    def is_running(self, instance_id: int) -> bool:
        process = (
            self.processes.get(instance_id)
            or self._process_groups.get(instance_id)
            or self._container_exec_processes.get(instance_id)
        )
        record = self._consumer_records.get(instance_id)
        consumer = record.task if record is not None else self._tasks.get(instance_id)
        return (
            any(
                key[0] == instance_id
                for key in self._consumer_recovery_pending
            )
            or (
                process is not None
                and not self._generation_reap_confirmed(instance_id, process)
            )
            or (consumer is not None and not consumer.done())
        )

    def get_last_stderr(self, instance_id: int) -> str:
        return self._last_stderr.pop(instance_id, "")

    def effective_exit_code(
        self,
        instance_id: int,
        process,
    ) -> int:
        """Return the provider-semantic exit code for this exact process."""

        effective = self._effective_exit_codes.get(instance_id)
        if effective is not None and effective[0] is process:
            return effective[1]
        returncode = getattr(process, "returncode", None)
        return returncode if isinstance(returncode, int) else -1

    @staticmethod
    def _chat_terminal_succeeded(process, exit_code: int) -> bool:
        """Separate an acknowledged user interrupt from internal abort cleanup."""

        if exit_code == 0:
            return True
        if exit_code not in (-2, 130):
            return False
        return (
            getattr(process, "termination_kind", None)
            not in {"internal_abort", "timeout"}
        )

    def get_config_dir(self, instance_id: int) -> str | None:
        return self._config_dirs.get(instance_id)

    def transient_error_seen(self, instance_id: int) -> bool:
        """True if the instance's most recent turn emitted a transient
        server-side 429/overload error (turn-scoped; reset at next launch)."""
        return instance_id in self._transient_seen

    def pty_rate_limit_seen(self, instance_id: int) -> bool:
        """True if the instance's most recent PTY turn saw an actionable
        rate_limit_event (turn-scoped; reset at next launch)."""
        return instance_id in self._pty_rate_limit_seen

    def pty_rate_limit_info(self, instance_id: int) -> dict | None:
        """Return the latest actionable PTY quota event for this turn."""

        return self._pty_rate_limit_info.get(instance_id)

    def clear_pty_rate_limit(self, instance_id: int) -> None:
        """Clear the completed turn's PTY quota signal and reset metadata."""

        self._pty_rate_limit_seen.discard(instance_id)
        self._pty_rate_limit_info.pop(instance_id, None)

    async def get_recent_log_contents(self, task_id: int, limit: int = 10) -> list[str]:
        """Fetch recent task output used by terminal failure classifiers.

        Structured provider codes such as Codex ``contextWindowExceeded`` live
        in the raw error envelope even when the human message is generic. Keep
        that small error envelope alongside its content; non-error event JSON
        (which can contain large prompts/tool payloads) remains excluded.
        """
        from backend.models.log_entry import LogEntry
        from sqlalchemy import select as sa_select
        async with self.db_factory() as db:
            result = await db.execute(
                sa_select(
                    LogEntry.content,
                    LogEntry.raw_json,
                    LogEntry.is_error,
                    LogEntry.event_type,
                )
                .where(LogEntry.task_id == task_id)
                .order_by(LogEntry.id.desc())
                .limit(limit)
            )
            contents = []
            for content, raw_json, is_error, event_type in result.all():
                pieces = [content] if content else []
                if is_error and event_type == "system_event" and raw_json:
                    pieces.append(raw_json[:4000])
                if pieces:
                    contents.append("\n".join(pieces))
            return contents
