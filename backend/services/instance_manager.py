import asyncio
import json
import logging
import os
import re
import signal
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Sequence

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.instance import Instance
from backend.models.task import Task

from backend.models.log_entry import LogEntry
from backend.services.context_compaction import (
    build_compacted_resume_prompt,
    is_context_window_exceeded,
    read_codex_rollout_last_usage,
)
from backend.services.codex_models import clamp_codex_effort
from backend.services.process_safety import require_safe_process_group_id
from backend.services.stream_parser import StreamParser
from backend.services.task_queue import task_retry_not_superseded_predicate
from backend.services.ws_broadcaster import WebSocketBroadcaster

if TYPE_CHECKING:
    from backend.services.mcp_config import McpServerSpec

logger = logging.getLogger(__name__)
_EXPECTED_GENERATION_UNSET = object()
DEFAULT_TERMINAL_CONSUMER_TIMEOUT = 30.0
DEFAULT_CONSUMER_CANCEL_TIMEOUT = 5.0
_CLOUDROUTER_CLAUDE_AUTH_ENV_KEYS = (
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
)
_CLOUDROUTER_CLAUDE_BINARY_ENV = "CCM_CLOUDROUTER_CLAUDE_BINARY"
_CLOUDROUTER_CODEX_AUTH_ENV_KEYS = (
    "OPENAI_API_KEY",
    "CODEX_API_KEY",
    "CLOUDROUTER_API_KEY",
)
_CLOUDROUTER_TRANSIENT_RE = re.compile(
    r"(?:API\s+Error|HTTP|status(?:\s+code)?|error|upstream)"
    r"[^\n]{0,120}(?:\b429\b|too many requests|rate[ _-]?limited)"
    r"|(?:\b429\b|too many requests|rate[ _-]?limited)"
    r"[^\n]{0,120}(?:API|HTTP|request|upstream|error)",
    re.IGNORECASE,
)
_CLOUDROUTER_AUTH_RE = re.compile(
    r"\b401\b[^\n]{0,120}(?:unauthori[sz]ed|invalid|API[ _-]?key)"
    r"|\b403\b[^\n]{0,120}(?:forbidden|unauthori[sz]ed|API[ _-]?key)"
    r"|(?:invalid[ _-]?api[ _-]?key|API[ _-]?key[^\n]{0,80}invalid"
    r"|authentication_error|\bforbidden\b)",
    re.IGNORECASE,
)


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


@dataclass(frozen=True)
class _OutputConsumerRecord:
    """Identity of one output-bookkeeping generation for a reusable slot."""

    process: asyncio.subprocess.Process
    task: asyncio.Task
    chat_initiated: bool
    provider: str
    task_id: int | None = None
    task_retry_count: int | None = None
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


@dataclass(frozen=True)
class _LaunchReservation:
    """Task identity held across the pre-owner subprocess launch window."""

    token: object
    task_id: int | None
    previous_process: asyncio.subprocess.Process | None


@dataclass(frozen=True)
class _ConsumerRecoveryEvidence:
    """Fail-closed evidence for one terminal consumer awaiting DB recovery."""

    error: BaseException
    tracked_generation: bool
    task_id: int | None
    task_retry_count: int | None
    instance_started_at: datetime | None


@dataclass(frozen=True)
class _TaskLifecycleFence:
    """Duck-compatible immutable Task generation for routing side effects."""

    task_id: int
    worker_id: int | None
    shared_from_id: int | None
    retry_count: int
    instance_id: int | None
    started_at: datetime | None
    completed_at: datetime | None
    # Routing from a queued chat freezes its pre-claim status. Dispatcher
    # lifecycles omit status because one generation advances in_progress →
    # executing without changing ownership.
    status: str | None = None


class InstanceManager:
    """Manages multiple Claude Code subprocess instances."""

    def __init__(self, db_factory, broadcaster: WebSocketBroadcaster):
        self.db_factory = db_factory  # async_sessionmaker
        self.broadcaster = broadcaster
        # Wired by backend.main after Dispatcher construction. Keeping this
        # dependency explicit prevents background consumers (especially in
        # tests) from importing the process-global Dispatcher and enqueueing
        # work against an unrelated database.
        self.task_message_enqueuer = None
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
        # Non-task Codex subprocesses (currently task distillation) share the
        # same credential-home and app-server admission barrier as normal exec
        # turns. Count by canonical home because they do not own a reusable
        # Instance slot and cannot safely be represented in _codex_exec_homes.
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
        return any(
            getattr(session, "session_id", None) == session_id
            for session in getattr(self._pty_backend, "_sessions", {}).values()
        )

    async def inject_pty_message(self, session_id: str, content: str) -> bool:
        """Inject text into a live PTY session (PTY-only).

        Looked up by Claude session_id — the chat path picks a different
        instance per message, so task.instance_id is NOT a reliable key.
        Delivered as a channel notification; CC consumes it at the next
        tool-call boundary (mid-turn) or at the start of the next turn.
        Returns False when PTY mode is off, no live session exists, or
        injection fails.
        """
        if self._pty_backend is None or not content or not session_id:
            return False
        session = None
        key = None
        for k, sess in self._pty_backend._sessions.items():
            if sess.session_id == session_id and sess.is_alive:
                session, key = sess, k
                break
        if session is None:
            return False
        # 仅允许注入到【正在运行的 turn】：turn 结束后 consumer 退出，
        # 此时注入的 channel 消息会唤醒一个无人采集的"孤儿 turn"——
        # 回复只进 JSONL，CCM 永远看不到（生产 task 51 实录）。
        consumer = self._pty_backend._consumers.get(key)
        if consumer is None or consumer.done():
            logger.info(
                "PTY inject rejected for session %s: no running turn", session_id
            )
            return False
        try:
            return await session.inject(content)
        except Exception:
            logger.exception("PTY inject failed for session %s", session_id)
            return False

    async def inject_codex_message(self, thread_id: str, content: str) -> bool:
        """Steer a live Codex app-server turn without starting a new turn.

        Codex ``exec`` subprocesses do not expose same-turn steering, so a
        missing app-server/context deliberately returns False.
        """
        if self._codex_app_server is None or not thread_id or not content:
            return False
        try:
            return await self._codex_app_server.steer_turn(thread_id, content)
        except Exception:
            logger.exception("Codex inject failed for thread %s", thread_id)
            return False

    async def release_pty_session(self, session_id: str) -> None:
        """Return a PTY session to nothing — stop it and remove from the pool.
        Used when a workload (e.g. a loop task) is finished with its session.
        No-op when PTY mode is not in use."""
        if self._pty_backend is None or not session_id:
            return
        try:
            await self._pty_backend._pool.remove(session_id)
        except Exception:
            logger.exception("Failed to release PTY session %s", session_id)

    async def drain_idle_pty_sessions(self) -> int:
        """Stop idle PTY sessions (called after PTY mode is switched off).
        In-flight turns are untouched and finish on the PTY path."""
        if self._pty_backend is None:
            return 0
        return await self._pty_backend.drain_idle_sessions()

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

        self._stopping[instance_id] = self._stopping.get(instance_id, 0) + 1

    def _end_stopping(self, instance_id: int) -> None:
        """Release only this caller's stop-intent token."""

        remaining = self._stopping.get(instance_id, 0) - 1
        if remaining > 0:
            self._stopping[instance_id] = remaining
        else:
            self._stopping.pop(instance_id, None)

    @staticmethod
    def _claim_pty_terminal_owner(
        record: _OutputConsumerRecord,
        owner: str,
        *,
        take_over_completed_consumer: bool = False,
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
            and take_over_completed_consumer
            and record.task.done()
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
    ) -> int:
        """Atomically admit one turn into a reusable instance slot."""

        provider = (provider or "claude").lower()
        lifecycle_lock = self._instance_lifecycle_lock(instance_id)
        current = asyncio.current_task()
        observed_generation: int | None = None
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
                        object(), task_id, process
                    )
                    self._launch_reservations[instance_id] = reservation
                    try:
                        async with self._cloudrouter_runtime_admission(
                            provider, config_dir, model,
                        ):
                            result = await self._launch_locked(
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
                                provider=provider,
                                enable_workflows=enable_workflows,
                                enabled_skills=enabled_skills,
                                system_prompt_mode=system_prompt_mode,
                                source_log_id=source_log_id,
                                current_message=current_message,
                                queue_timestamp=queue_timestamp,
                            )
                    except BaseException:
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
                        return result

            # Never hold admission while waiting: a terminal consumer may
            # legitimately self-launch a transient/account retry, which needs
            # this same lock.  Re-enter and re-check all maps afterwards so an
            # external contender cannot slip a second process into the slot.
            if consumer is not None:
                await self.wait_for_output_consumer(
                    instance_id,
                    provider=provider,
                    timeout=None,
                    # A bare legacy/test task has no process-generation
                    # identity.  Passing the process here makes the waiter
                    # deliberately ignore that task and spin forever.
                    expected_process=(record.process if record is not None else None),
                    preserve_error=True,
                )

    async def _launch_locked(
        self,
        instance_id: int,
        prompt: str,
        task_id: int | None = None,
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
    ) -> int:
        """Launch a Claude Code subprocess for the given instance.

        If resume_session_id is provided, uses --resume to continue the conversation.
        loop_iteration is recorded on every LogEntry produced by this invocation so
        that loop-task chat history can be grouped by iteration in the frontend.
        """
        provider = (provider or "claude").lower()
        cloudrouter_account = self._cloudrouter_account_for_runtime_home(
            provider, config_dir
        )
        # The API and Dispatcher serialize deletion/reservation with this
        # lifecycle lock.  Verify the reusable slot before creating config
        # files, containers, or a real agent process; a post-spawn rowcount
        # check remains below as defense against cross-process DB mutation.
        task_retry_count: int | None = None
        async with self.db_factory() as db:
            if await db.get(Instance, instance_id) is None:
                raise InstanceNotFoundError(
                    f"Instance {instance_id} no longer exists"
                )
            if task_id is not None:
                generation_row = (
                    await db.execute(
                        select(Task.retry_count).where(
                            Task.id == task_id,
                            Task.instance_id == instance_id,
                            Task.status.in_(["in_progress", "executing"]),
                        )
                    )
                ).first()
                if generation_row is None:
                    raise LaunchSupersededError(
                        f"Task {task_id} no longer owns instance {instance_id}"
                    )
                task_retry_count = generation_row[0]
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
                CodexThreadHomeMismatchError,
                normalize_codex_home,
            )

            config_dir = normalize_codex_home(config_dir)
            codex_home_path = Path(config_dir)
            codex_home_path.mkdir(parents=True, exist_ok=True, mode=0o700)
            try:
                os.chmod(codex_home_path, 0o700)
            except OSError:
                logger.warning("Could not enforce 0700 on CODEX_HOME %s", config_dir)
            self._config_dirs[instance_id] = config_dir

        # New turn → clear per-turn flags.
        self._transient_seen.discard(instance_id)
        self._pty_rate_limit_seen.discard(instance_id)
        self._pty_rate_limit_info.pop(instance_id, None)
        self._effective_exit_codes.pop(instance_id, None)

        mcp_config_path = None
        if provider == "claude" and task_id:
            from backend.services.mcp_config import generate_mcp_config
            mcp_config_path = generate_mcp_config(task_id, enabled_skills or {})

        # ask_user：把 AskUserQuestion 拦截 hook 注入本次使用的 config_dir（-p 与 PTY 统一）。
        # config_dir 为空时落到默认 ~/.claude。失败不阻断 launch。
        if provider == "claude":
            from backend.services.ask_user_settings import ensure_ask_user_hook
            ensure_ask_user_hook(config_dir or os.path.expanduser("~/.claude"))

        # Check if shared project → prepare Docker container wrapper for PTY
        _container_project_id = None
        _container_wrapper = None
        _container_exec_spec = None
        if provider == "claude" and task_id:
            try:
                from backend.services.container_manager import is_shared_project, ContainerManager
                async with self.db_factory() as _db:
                    from backend.models.task import Task as _Task
                    _t = await _db.get(_Task, task_id)
                    if _t and _t.project_id:
                        if await is_shared_project(_t.project_id, self.db_factory) and ContainerManager.is_docker_available():
                            _container_project_id = _t.project_id
                            if not hasattr(self, '_container_mgr'):
                                self._container_mgr = ContainerManager()
                            project_path = cwd or os.getcwd()
                            # Get project git credentials for container isolation
                            from backend.models.project import Project as _Project
                            _proj = await _db.get(_Project, _t.project_id)
                            container_name = await self._container_mgr.ensure_container(
                                _container_project_id, project_path, config_dir,
                                api_account_root=(
                                    str(cloudrouter_account.root)
                                    if cloudrouter_account is not None
                                    else None
                                ),
                                git_credential_type=_proj.git_credential_type if _proj else None,
                                git_ssh_key_path=_proj.git_ssh_key_path if _proj else None,
                                git_https_username=_proj.git_https_username if _proj else None,
                                git_https_token=_proj.git_https_token if _proj else None,
                            )
                            (
                                _container_wrapper,
                                _container_exec_spec,
                            ) = self._container_mgr.create_pty_wrapper(
                                _container_project_id,
                                instance_id,
                            )
                            self._container_tasks[instance_id] = _container_project_id
            except Exception as exc:
                # Shared Projects must never escape their isolation boundary
                # because the container's private /tmp is pressured, busy, or
                # unverifiable.  The container supervisor independently
                # repeats this gate immediately before child creation.
                from backend.services.container_manager import (
                    ContainerTmpPressureError,
                )

                if isinstance(exc, ContainerTmpPressureError):
                    raise
                logger.debug("Container setup failed, falling back to bare process")

        codex_main_mcp_required = bool(
            provider == "codex"
            and task_id is not None
            and settings.codex_main_mcp_enabled
        )
        codex_sub_agent_mcp_required = bool(
            provider == "codex"
            and task_id is not None
            and enabled_skills
            and enabled_skills.get("sub-agent")
        )
        codex_mcp_required = (
            codex_main_mcp_required or codex_sub_agent_mcp_required
        )
        codex_mcp_specs: tuple["McpServerSpec", ...] = ()
        codex_exec_route = "direct-exec"
        if codex_main_mcp_required:
            from backend.services.mcp_config import build_mcp_server_specs

            codex_mcp_specs = build_mcp_server_specs(
                task_id,
                enabled_skills or {},
            )
        elif codex_sub_agent_mcp_required:
            from backend.services.mcp_config import (
                build_sub_agent_controller_mcp_server_specs,
            )

            codex_mcp_specs = build_sub_agent_controller_mcp_server_specs(task_id)

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
                        mcp_specs=codex_mcp_specs,
                        source_log_id=source_log_id,
                        current_message=current_message,
                        queue_timestamp=queue_timestamp,
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
                except CodexRequiredMcpPreTurnError:
                    if (
                        codex_main_mcp_required
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
                    CodexThreadHomeMismatchError,
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
                    if codex_mcp_required:
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
                        raise CodexRequiredMcpError(
                            "Codex app-server failed before required "
                            "ccm_skills could be guaranteed"
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

        if provider == "claude" and self.pty_mode_enabled:
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
                claude_binary_override=_container_wrapper,
                container_exec_spec=_container_exec_spec,
                task_retry_count=task_retry_count,
                cloudrouter_api=cloudrouter_account is not None,
                source_log_id=source_log_id,
                current_message=current_message,
                queue_timestamp=queue_timestamp,
            )

        cmd = self._build_command(
            provider=provider,
            prompt=prompt,
            model=model,
            resume_session_id=resume_session_id,
            effort_level=effort_level,
            enable_workflows=enable_workflows,
            mcp_config_path=str(mcp_config_path) if mcp_config_path else None,
            enabled_skills=enabled_skills,
            system_prompt_mode=system_prompt_mode,
            cwd=cwd,
            task_id=task_id,
            codex_mcp_specs=(
                codex_mcp_specs if codex_main_mcp_required else ()
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
                codex_main_mcp_required,
            )

        # Must unset CLAUDE_CODE env var to avoid nested session detection
        env = {k: v for k, v in os.environ.items() if k.upper() not in ("CLAUDECODE", "CLAUDE_CODE")}

        # Inject per-project git identity and credentials as environment variables.
        # These take precedence over any global ~/.gitconfig or system credential helper.
        if git_env:
            env.update(git_env)

        if cloudrouter_account is not None:
            auth_keys = (
                _CLOUDROUTER_CLAUDE_AUTH_ENV_KEYS
                if provider == "claude"
                else _CLOUDROUTER_CODEX_AUTH_ENV_KEYS
            )
            for key in auth_keys:
                env.pop(key, None)

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

        # Check if this task's project is shared → run in Docker container
        use_container = False
        container_project_id = None
        if task_id and provider == "claude":
            try:
                from backend.services.container_manager import is_shared_project, ContainerManager
                async with self.db_factory() as _db:
                    from backend.models.task import Task as _Task
                    _task = await _db.get(_Task, task_id)
                    if _task and _task.project_id:
                        _shared = await is_shared_project(_task.project_id, self.db_factory)
                        if _shared and ContainerManager.is_docker_available():
                            use_container = True
                            container_project_id = _task.project_id
            except Exception:
                logger.debug("Container check failed, falling back to bare process")

        if use_container and container_project_id:
            from backend.services.container_manager import (
                ContainerExecSpawnCleanupError,
                ContainerManager,
            )
            if not hasattr(self, '_container_mgr'):
                self._container_mgr = ContainerManager()
            project_path = cwd or os.getcwd()
            # Get project git credentials
            _git_creds = {}
            try:
                async with self.db_factory() as _db2:
                    from backend.models.project import Project as _Proj
                    _p = await _db2.get(_Proj, container_project_id)
                    if _p:
                        _git_creds = {
                            "git_credential_type": _p.git_credential_type,
                            "git_ssh_key_path": _p.git_ssh_key_path,
                            "git_https_username": _p.git_https_username,
                            "git_https_token": _p.git_https_token,
                        }
            except Exception:
                pass
            await self._container_mgr.ensure_container(
                container_project_id,
                project_path,
                config_dir,
                api_account_root=(
                    str(cloudrouter_account.root)
                    if cloudrouter_account is not None
                    else None
                ),
                **_git_creds,
            )
            try:
                container_env = env
                if provider == "claude" and config_dir:
                    container_env = dict(env)
                    container_env["CLAUDE_CONFIG_DIR"] = "/home/sandbox/.claude"
                process = await self._container_mgr.exec_command(
                    container_project_id,
                    cmd,
                    env=container_env,
                    cwd="/workspace",
                )
            except ContainerExecSpawnCleanupError as exc:
                # exec_command was cancelled after docker(1) may have asked
                # the daemon to create the inner command, and exact cleanup
                # could not be proven.  Install the hidden spawn outcome under
                # this Instance before surfacing the failure so stop/shutdown
                # can retry it; never release the slot as idle.
                process = exc.process
                self.processes[instance_id] = process
                self._container_tasks[instance_id] = container_project_id
                self._container_exec_processes[instance_id] = process
                if os.name == "posix":
                    self._process_groups[instance_id] = process
                try:
                    async with self.db_factory() as db:
                        await db.execute(
                            update(Instance)
                            .where(Instance.id == instance_id)
                            .values(
                                status="error",
                                pid=getattr(process, "pid", None),
                                current_task_id=task_id,
                            )
                        )
                        await db.commit()
                except Exception:
                    logger.exception(
                        "Failed to persist unresolved container spawn "
                        "for instance %s",
                        instance_id,
                    )
                raise
            self._container_tasks[instance_id] = container_project_id
            self._container_exec_processes[instance_id] = process
            if os.name == "posix":
                self._process_groups[instance_id] = process
        else:
            if provider == "codex":
                # Hold the per-home gate through process creation and tracking.
                # Maintenance can then either see this active exec or reserve
                # the home first; it can never edit auth.json in the gap.
                home_lock = self._codex_home_lock(config_dir)
                async with home_lock:
                    if config_dir in self._codex_home_maintenance:
                        raise CodexAppServerBusyError(
                            f"Codex account is under maintenance: {config_dir}"
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
                    process = await self._spawn_managed_direct_process(
                        instance_id,
                        task_id,
                        cmd,
                        spawn_kwargs,
                    )
                    self._codex_exec_homes[instance_id] = config_dir
            else:
                spawn_kwargs = {
                    "stdout": asyncio.subprocess.PIPE,
                    "stderr": asyncio.subprocess.PIPE,
                    "cwd": cwd or os.getcwd(),
                    "env": env,
                    "limit": 10 * 1024 * 1024,
                }
                if os.name == "posix":
                    spawn_kwargs["start_new_session"] = True
                process = await self._spawn_managed_direct_process(
                    instance_id,
                    task_id,
                    cmd,
                    spawn_kwargs,
                )

        if provider != "codex":
            self.processes[instance_id] = process

        # Store launch params for potential pool rotation re-launch
        if chat_initiated:
            self._launch_params[instance_id] = {
                "prompt": prompt,
                "task_id": task_id,
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
        )

    def _ensure_codex_app_server_registry(self):
        """Return the lazy per-CODEX_HOME app-server registry."""

        from backend.services.codex_app_server import CodexAppServerRegistry

        if self._codex_app_server is None:
            self._codex_app_server = CodexAppServerRegistry(
                self._resolve_codex_binary(),
                request_timeout=settings.codex_app_server_request_timeout,
                env_remove_resolver=self._codex_env_remove_for_home,
            )
        return self._codex_app_server

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
        async with guard(provider, config_dir, model) as current:
            yield current

    def _codex_env_remove_for_home(self, codex_home: str) -> set[str]:
        if self._cloudrouter_account_for_runtime_home("codex", codex_home):
            return set(_CLOUDROUTER_CODEX_AUTH_ENV_KEYS)
        return set()

    def is_cloudrouter_transient(
        self,
        instance_id: int,
        provider: str,
        text: str,
    ) -> bool:
        """Classify gateway 429s only for a proven API-account launch."""

        config_dir = self._config_dirs.get(instance_id)
        if self._cloudrouter_account_for_runtime_home(provider, config_dir) is None:
            return False
        return bool(_CLOUDROUTER_TRANSIENT_RE.search(text or ""))

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

    def _assert_codex_app_server_home_available(self, codex_home: str) -> None:
        """Reject app-server admission while another transport owns a home.

        The caller must hold the canonical home's admission lock so the check
        and the subsequent app-server operation form one atomic admission.
        """

        from backend.services.codex_app_server import CodexAppServerBusyError

        if codex_home in self._codex_home_maintenance:
            raise CodexAppServerBusyError(
                f"Codex account is under maintenance: {codex_home}"
            )
        for exec_instance_id, exec_home in self._codex_exec_homes.items():
            if exec_home == codex_home:
                raise CodexAppServerBusyError(
                    "Codex account still has an exec generation "
                    f"owned by instance {exec_instance_id}: {codex_home}"
                )
        if self._codex_ephemeral_home_users.get(codex_home, 0):
            raise CodexAppServerBusyError(
                f"Codex account has an active ephemeral exec: {codex_home}"
            )

    async def read_codex_thread(
        self, codex_home: str, thread_id: str,
    ) -> dict:
        """Read one idle Codex thread with turns from its bound account."""

        async with self.codex_home_app_server_guard(codex_home) as home:
            registry = self._ensure_codex_app_server_registry()
            return await registry.read_thread(home, thread_id)

    async def create_codex_thread(
        self,
        codex_home: str,
        *,
        cwd: str,
        model: str | None = None,
    ) -> dict:
        """Create and register an empty native Codex thread."""

        async with self.codex_home_app_server_guard(codex_home) as home:
            registry = self._ensure_codex_app_server_registry()
            return await registry.create_thread(
                home,
                cwd=cwd,
                model=model,
            )

    async def fork_codex_thread(
        self,
        codex_home: str,
        thread_id: str,
        *,
        last_turn_id: str,
    ) -> dict:
        """Create and register a native Codex thread fork."""

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
            if home in self._codex_home_maintenance:
                raise CodexAppServerBusyError(
                    f"Codex account is under maintenance: {home}"
                )
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
        mcp_specs: Sequence["McpServerSpec"] = (),
        source_log_id: int | None = None,
        current_message: str | None = None,
        queue_timestamp: float | None = None,
    ) -> int:
        """Launch one turn on the persistent app-server for its CODEX_HOME."""
        registry = self._ensure_codex_app_server_registry()

        actual_cwd = cwd or os.getcwd()
        codex_effort = clamp_codex_effort(model, effort_level)
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
                        )
                        .values(last_cwd=actual_cwd)
                    )
                    if task_update.rowcount == 0:
                        raise LaunchSupersededError(
                            f"Task {task_id} no longer owns instance {instance_id}"
                        )
                instance_update = await db.execute(
                    update(Instance)
                    .where(Instance.id == instance_id)
                    .values(
                        pid=process.pid,
                        status="running",
                        current_task_id=task_id,
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
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
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
        instance_started_at: datetime | None = None,
    ) -> _OutputConsumerRecord:
        """Register a consumer with identity-safe terminal cleanup.

        Most cleanup happens inside ``_consume_output`` because it also owns
        database and broadcast work.  This callback is the last-resort guard:
        an unexpected exception in that bookkeeping must not leave a finished
        task in ``_tasks`` that every future Codex launch re-awaits forever.
        """

        record = _OutputConsumerRecord(
            process,
            consumer,
            chat_initiated,
            (provider or "claude").lower(),
            task_id,
            task_retry_count,
            instance_started_at,
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

            if self._consumer_records.get(instance_id) is record:
                recovery_key = (instance_id, process)
                pending_recovery = self._consumer_recovery_pending.get(
                    recovery_key
                )
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
        instance_started_at: datetime | None,
    ) -> _ConsumerRecoveryEvidence:
        """Retain one exact terminal generation whose DB recovery is unknown."""

        evidence = _ConsumerRecoveryEvidence(
            error=error,
            tracked_generation=tracked_generation,
            task_id=task_id,
            task_retry_count=task_retry_count,
            instance_started_at=instance_started_at,
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
        async with home_lock:
            try:
                if self._codex_app_server is not None:
                    await self._codex_app_server.end_home_maintenance(home)
            finally:
                self._codex_home_maintenance.discard(home)

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
        cloudrouter_api: bool = False,
        source_log_id: int | None = None,
        current_message: str | None = None,
        queue_timestamp: float | None = None,
    ) -> int:
        """PTY-mode launch: delegate to claude_pty, mirror -p bookkeeping.

        The backend installs a process proxy into self.processes and a
        consumer into self._tasks; events flow back through _process_event,
        so everything downstream (DB, WebSocket, dispatcher wait) is
        unchanged.
        """
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
            }
            self._launch_params[instance_id] = pty_launch_params
        try:
            # build_config is shared by every PTY instance. Hold one global
            # admission lock across patch -> config construction -> restore so
            # a container wrapper can never leak into another launch.
            async with self._pty_build_config_lock:
                original_build_config = None
                if claude_binary_override or cloudrouter_api:
                    original_build_config = self._pty_backend.build_config
                    wrapper = claude_binary_override

                    def _patched_build_config(**kw):
                        cfg = original_build_config(**kw)
                        if cloudrouter_api:
                            final_binary = wrapper or cfg.claude_binary
                            cloudrouter_wrapper = Path(__file__).with_name(
                                "cloudrouter_claude_wrapper.sh"
                            )
                            if not (
                                cloudrouter_wrapper.is_file()
                                and os.access(cloudrouter_wrapper, os.X_OK)
                            ):
                                raise RuntimeError(
                                    "CloudRouter Claude wrapper is unavailable"
                                )
                            overrides = dict(cfg.env_overrides or {})
                            for key in _CLOUDROUTER_CLAUDE_AUTH_ENV_KEYS:
                                overrides.pop(key, None)
                            overrides[_CLOUDROUTER_CLAUDE_BINARY_ENV] = str(
                                final_binary
                            )
                            cfg.env_overrides = overrides
                            cfg.claude_binary = str(cloudrouter_wrapper)
                        elif wrapper:
                            cfg.claude_binary = wrapper
                        return cfg

                    self._pty_backend.build_config = _patched_build_config
                try:
                    session_id = await self._pty_backend.launch_for_ccm(
                        instance_id=instance_id,
                        prompt=prompt,
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
                    if original_build_config is not None:
                        self._pty_backend.build_config = original_build_config

            process = self.processes.get(instance_id)
            consumer = self._tasks.get(instance_id)
            if process is None:
                raise RuntimeError(
                    "PTY backend did not register a process during startup"
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
                            task_retry_not_superseded_predicate(),
                            (
                                Task.id == task_id
                                if task_retry_count is None
                                else Task.retry_count == task_retry_count
                            ),
                        )
                        .values(**task_values)
                    )
                    if task_update.rowcount == 0:
                        raise LaunchSupersededError(
                            f"Task {task_id} no longer owns instance {instance_id}"
                        )
                instance_update = await db.execute(
                    update(Instance)
                    .where(Instance.id == instance_id)
                    .values(
                        pid=pid,
                        status="running",
                        current_task_id=task_id,
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
                if owns_new_process or owns_new_consumer:
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
                        process is None or process.returncode is not None
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
                if reap_confirmed:
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
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
            cleanup.result()
            raise

    async def wait_for_pty_launch_metadata(self, instance_id: int) -> None:
        """Order PTY terminal cleanup after the initial running commit."""

        barrier = self._pty_launch_barriers.get(instance_id)
        if barrier is not None:
            await barrier.wait()

    async def finalize_pty_chat_generation(
        self,
        instance_id: int,
        task_id: int,
        exit_code: int | None,
        record: _OutputConsumerRecord,
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
        if (
            record.task is not consumer
            or record.task_id != task_id
            or expected_started_at is None
            or expected_retry_count is None
        ):
            return None

        def owns_generation() -> bool:
            return (
                self._consumer_records.get(instance_id) is record
                and self._tasks.get(instance_id) is consumer
                and self.processes.get(instance_id) is process
            )

        lifecycle_lock = self._instance_lifecycle_lock(instance_id)
        ec = exit_code if exit_code is not None else 0
        interrupted = ec in (-2, 130)
        final_status = "completed" if ec == 0 or interrupted else "failed"
        completed_at = datetime.utcnow()
        provider_error = (record.fatal_provider_error or "").strip()
        failure_notice_data = None

        async with lifecycle_lock:
            if instance_id in self._stopping or not owns_generation():
                return None

            async with self.db_factory() as db:
                task_values: dict = {"status": final_status}
                if final_status == "completed":
                    task_values.update(
                        completed_at=completed_at,
                        error_message=None,
                    )
                else:
                    task_values.update(
                        completed_at=completed_at,
                        error_message=(
                            provider_error[:2000]
                            if provider_error
                            else f"Process exited with code {ec}"
                        ),
                    )
                # Lock/update the Task first.  Cancellation and retry use the
                # same global Task -> Instance order.
                task_result = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task_id,
                        Task.status.in_(
                            ["executing", "in_progress", "failed", "pending"]
                        ),
                        Task.instance_id == instance_id,
                        Task.retry_count == expected_retry_count,
                    )
                    .values(**task_values)
                )
                if not task_result.rowcount:
                    await db.rollback()
                    return None

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
                            if final_status == "completed" or interrupted
                            else "error"
                        ),
                        pid=None,
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
                if final_status == "failed" and not provider_error:
                    failure_notice = LogEntry(
                        instance_id=instance_id,
                        task_id=task_id,
                        event_type="system_event",
                        role="system",
                        content=(
                            "Claude 进程在返回回复前异常退出"
                            f"（exit code {ec}）。"
                        ),
                        is_error=True,
                    )
                    db.add(failure_notice)
                    await db.flush()
                    failure_notice_data = {
                        "id": failure_notice.id,
                        "instance_id": instance_id,
                        "task_id": task_id,
                        "event_type": "system_event",
                        "role": "system",
                        "content": failure_notice.content,
                        "is_error": True,
                        "timestamp": (
                            failure_notice.timestamp or datetime.utcnow()
                        ).isoformat(),
                    }
                # MySQL DATETIME without fractional precision normalizes away
                # Python microseconds.  Re-read the locked row before commit
                # and use that database value for the publication fence.
                completed_at = (
                    await db.execute(
                        select(Task.completed_at).where(Task.id == task_id)
                    )
                ).scalar_one()
                await db.commit()

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
                        Task.completed_at == completed_at,
                    )
                    .values(status=final_status)
                )
                if publish_guard.rowcount:
                    if failure_notice_data is not None:
                        await self.broadcaster.broadcast(
                            f"task:{task_id}",
                            failure_notice_data,
                        )
                    await self.broadcaster.broadcast(
                        "tasks",
                        {
                            "event": "status_change",
                            "task_id": task_id,
                            "new_status": final_status,
                            "instance_id": instance_id,
                        },
                    )
                    await self.broadcaster.broadcast(
                        f"task:{task_id}",
                        {
                            "event_type": "process_exit",
                            "exit_code": ec,
                            "stderr": None,
                        },
                    )
                await db.commit()
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
        codex_mcp_specs: Sequence["McpServerSpec"] = (),
    ) -> list[str]:
        """Build the subprocess command for a supported coding-agent CLI."""
        if provider == "claude":
            cmd = [
                settings.claude_binary,
                "-p", prompt,
                "--dangerously-skip-permissions",
                "--output-format", "stream-json",
                "--verbose",
            ]
            if resume_session_id:
                cmd.extend(["--resume", resume_session_id])
            if model:
                cmd.extend(["--model", model])
            if effort_level:
                cmd.extend(["--effort", effort_level])
            from backend.services.skill_loader import discover_skills, build_skill_prompt_file, get_skill_disallowed_tools
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
            # Skill prompt injection (plugins + user skills)
            skill_prompt_path = build_skill_prompt_file(skills, enabled_skills, task_id)
            if skill_prompt_path:
                cmd.extend(["--append-system-prompt-file", skill_prompt_path])
            # User skill injection (L0 directory in prompt)
            if task_id:
                from backend.services.user_skill_injector import build_user_skill_prompt_sync
                user_skill_path = build_user_skill_prompt_sync(task_id)
                if user_skill_path:
                    cmd.extend(["--append-system-prompt-file", user_skill_path])
            if system_prompt_mode and settings.append_system_prompt_file:
                sp_path = Path(settings.append_system_prompt_file)
                if not sp_path.is_absolute():
                    sp_path = Path(settings.worker_deploy_source_dir) / sp_path
                if sp_path.exists():
                    flag = "--system-prompt-file" if system_prompt_mode == "replace" else "--append-system-prompt-file"
                    cmd.extend([flag, str(sp_path)])
            return cmd

        if provider == "codex":
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
            if model and model != "default":
                cmd.extend(["--model", model])
            codex_effort = clamp_codex_effort(model, effort_level)
            if codex_effort:
                cmd.extend(["-c", f'model_reasoning_effort="{codex_effort}"'])
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
                    instance_started_at=None,
                )
                raise unsettled from exc
            # A failed post-process hook must not leave the DB advertising a
            # running worker after the process is terminal.  Conditions on the
            # task status preserve a result already committed before a later
            # broadcast/cleanup failure.
            task_publication_generation: dict | None = None
            recovery_failure: ConsumerRecoveryUnsettledError | None = None
            try:
                async with self.db_factory() as db:
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
                            )
                            .values(
                                status="failed",
                                completed_at=datetime.utcnow(),
                                error_message=(
                                    f"Output bookkeeping failed: {exc}"
                                )[:500],
                            )
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
                                        Task.instance_id,
                                        Task.started_at,
                                        Task.completed_at,
                                    ).where(Task.id == task_id)
                                )
                            ).one()
                            task_publication_generation = {
                                "status": resulting_task_generation.status,
                                "retry_count": (
                                    resulting_task_generation.retry_count
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
                    instance_started_at=expected_started_at,
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
                if enqueuer is None:
                    logger.warning(
                        "Task %d got empty/non-response (%r), but no task "
                        "message enqueuer is configured",
                        task_id,
                        combined[:80],
                    )
                else:
                    params["_retried"] = True
                    logger.warning(
                        "Task %d got empty/non-response (%r), re-enqueueing prompt",
                        task_id, combined[:80],
                    )
                    from backend.services.dispatcher import PRIORITY_USER
                    retry_kwargs = dict(
                        task_id=task_id,
                        prompt=params.get("current_message") or params["prompt"],
                        priority=PRIORITY_USER,
                        source="retry",
                        current_message=(
                            params.get("current_message") or params["prompt"]
                        ),
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
                    await enqueuer(**retry_kwargs)
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

        if task_id and chat_initiated and exit_code not in (0, -2, 130):
            # Context overflow can be a plain CLI message or the app-server's
            # structured contextWindowExceeded code. Compact and continue.
            _context_window_exceeded = is_context_window_exceeded(
                provider,
                _failure_details,
                _assistant_texts,
                stderr_text,
            )
            if _context_window_exceeded:
                try:
                    from backend.main import dispatcher
                    async with self.db_factory() as db:
                        from backend.models.task import Task as _Task
                        task = await db.get(_Task, task_id)
                        if task and task.session_id:
                            logger.warning(
                                "Task %d exceeded its context window, "
                                "compacting session",
                                task_id,
                            )
                            compacted_session_id = task.session_id
                            compacted_status = task.status
                            params = self._launch_params.get(instance_id, {})
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
                                compacted_session_id,
                                db,
                                **compact_kwargs,
                            )
                            if summary:
                                compact_generation_predicates = [
                                    Task.id == task_id,
                                    Task.status == compacted_status,
                                    Task.session_id == compacted_session_id,
                                ]
                                if tracked_generation:
                                    compact_generation_predicates.append(
                                        Task.instance_id == instance_id
                                    )
                                    if expected_retry_count is not None:
                                        compact_generation_predicates.append(
                                            Task.retry_count
                                            == expected_retry_count
                                        )
                                compacted = await db.execute(
                                    update(Task)
                                    .where(*compact_generation_predicates)
                                    .values(
                                        session_id=None,
                                        context_window_usage=None,
                                    )
                                )
                                if not compacted.rowcount:
                                    await db.rollback()
                                    logger.info(
                                        "Discarding stale prompt-too-long "
                                        "compaction for task %s",
                                        task_id,
                                    )
                                else:
                                    await db.commit()
                                    from backend.services.dispatcher import PRIORITY_USER
                                    current_message = (
                                        params.get("current_message")
                                        or params.get("prompt")
                                        or "continue"
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
                                    await dispatcher.enqueue_message(
                                        **retry_kwargs
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

        # Commit terminal bookkeeping in the global Task -> Instance lock
        # order. Cancellation/retry/delete use the same order; taking the
        # Instance write first here can deadlock those paths on PostgreSQL or
        # MySQL.
        successful_terminal = self._chat_terminal_succeeded(
            process,
            exit_code,
        )
        new_status = "idle" if successful_terminal else "error"
        final_status = None
        task_publication_generation: dict | None = None
        failure_notice_data = None
        async with self.db_factory() as db:
            if task_id and chat_initiated:
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
                        select(
                            Task.status,
                            Task.retry_count,
                            Task.instance_id,
                            Task.started_at,
                            Task.completed_at,
                        ).where(Task.id == task_id)
                    )
                ).one()
                chat_active_statuses = {
                    "executing",
                    "in_progress",
                    "failed",
                    "pending",
                }
                if current_task_generation.status in chat_active_statuses:
                    final_status = (
                        "completed"
                        if successful_terminal
                        else "failed"
                    )
                    task_values: dict = {"status": final_status}
                    if final_status == "completed":
                        task_values.update(
                            completed_at=datetime.utcnow(),
                            error_message=None,
                        )
                    else:
                        task_values["error_message"] = (
                            failure_text[:2000]
                            if failure_text
                            else f"Process exited with code {exit_code}"
                        )
                    task_update = await db.execute(
                        update(Task)
                        .where(
                            *task_generation_predicates,
                            Task.status == current_task_generation.status,
                        )
                        .values(**task_values)
                    )
                    if not task_update.rowcount:
                        await db.rollback()
                        return
                    if final_status == "failed" and not _fatal_provider_error:
                        process_label = self._provider_process_label(
                            instance_id, provider
                        )
                        failure_notice = LogEntry(
                            instance_id=instance_id,
                            task_id=task_id,
                            event_type="system_event",
                            role="system",
                            content=(
                                f"{process_label} 进程在返回回复前异常退出"
                                f"（exit code {exit_code}）。"
                            ),
                            is_error=True,
                        )
                        db.add(failure_notice)
                        await db.flush()
                        failure_notice_data = {
                            "id": failure_notice.id,
                            "instance_id": instance_id,
                            "task_id": task_id,
                            "event_type": "system_event",
                            "role": "system",
                            "content": failure_notice.content,
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
                            Task.instance_id,
                            Task.started_at,
                            Task.completed_at,
                        ).where(Task.id == task_id)
                    )
                ).one()
                task_publication_generation = {
                    "status": resulting_task_generation.status,
                    "retry_count": resulting_task_generation.retry_count,
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
                            "new_status": final_status,
                            "instance_id": instance_id,
                        },
                    )
                exit_event = {
                    "event_type": "process_exit",
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

        # 原生子 agent（native-monitor 等）生命周期跟 session 走——
        # session 退出/重建时一律标 completed，否则 UI 上永远显示 running。
        # CCM 自己的 monitor 子 agent（source="ccm"）有独立进程，不跟主
        # session 走，必须排除，否则 chat turn 结束就误杀 monitor。
        # 但如果有 native-monitor 在 running，说明 monitor 被进程退出打断，
        # 需要 auto-resume 让主 agent 处理积压的 <task-notification>。
        if task_id:
            from backend.models.sub_agent import SubAgentSession
            has_pending_native = False
            async with self.db_factory() as db:
                stale = await db.execute(
                    select(SubAgentSession).where(
                        SubAgentSession.task_id == task_id,
                        SubAgentSession.status == "running",
                        SubAgentSession.source != "ccm",
                    )
                )
                for sa in stale.scalars().all():
                    if sa.agent_type in ("native-monitor", "monitor", "native-agent"):
                        has_pending_native = True
                    sa.status = "completed"
                    sa.completed_at = datetime.utcnow()
                await db.commit()

            # Auto-resume: native sub-agents (monitor/agent) 随进程退出，
            # resume 让主 agent 处理积压的结果并回复用户
            if has_pending_native and exit_code == 0 and chat_initiated:
                try:
                    from backend.main import dispatcher
                    from backend.services.dispatcher import PRIORITY_MONITOR_COMPLETE
                    await dispatcher.enqueue_message(
                        task_id=task_id,
                        prompt=(
                            "[Monitor 通知] 你之前启动的 Monitor 已有结果。"
                            "请检查 monitor 的 task-notification 并根据结果决定下一步操作。"
                        ),
                        priority=PRIORITY_MONITOR_COMPLETE,
                        source="monitor:native-exit-resume",
                        user_message_text="[Monitor] 后台监控已产生通知，自动恢复会话",
                    )
                    logger.info(
                        "Task %d had pending native monitors on exit, enqueued auto-resume",
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

            if not dispatcher:
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
            }
            if params.get("source_log_id") is not None:
                requeue_kwargs["source_log_id"] = params["source_log_id"]
            if params.get("current_message") is not None:
                requeue_kwargs["current_message"] = params["current_message"]
            if params.get("queue_timestamp") is not None:
                requeue_kwargs["queue_timestamp"] = params[
                    "queue_timestamp"
                ]
            await dispatcher.enqueue_message(**requeue_kwargs)
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

            if not await generation_is_current(generation):
                return False

            if provider == "codex":
                if not session_id:
                    return False
                pool = dispatcher.codex_pool
                if not (pool and pool.enabled):
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
                )
                if not await generation_is_current(generation):
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
                delayed_cancellation: asyncio.CancelledError | None = None
                while not switch_operation.done():
                    try:
                        await asyncio.shield(switch_operation)
                    except asyncio.CancelledError as exc:
                        delayed_cancellation = (
                            delayed_cancellation or exc
                        )
                    except BaseException:
                        break
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

        if codex_type == "item.agent_message.delta":
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
        else:
            content = data.get("content") or data.get("message") or data.get("text")
            if content is None and item:
                content = item.get("text") or item.get("command") or item.get("status")
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
        if event_type == "rate_limit_event":
            return content or "Claude API rate limit"
        if event_type == "system_event" and (
            content.startswith("api_error:")
            or content.startswith("Response timed out")
        ):
            return content
        return None

    async def _process_event(
        self,
        instance_id: int,
        task_id: int | None,
        event: dict,
        loop_iteration: int | None = None,
        *,
        consumer_record: _OutputConsumerRecord | None = None,
        detached_autonomous: bool = False,
        expected_session_id: str | None = None,
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

        def owns_event_generation() -> bool:
            if event_record is None:
                return False
            return (
                event_record.task_id == task_id
                and (
                    task_id is None
                    or event_record.task_retry_count is not None
                )
                and event_record.instance_started_at is not None
                and self._consumer_records.get(instance_id) is event_record
                and self._tasks.get(instance_id) is event_record.task
                and self.processes.get(instance_id) is event_record.process
            )

        def task_event_predicates() -> list:
            predicates = [
                Task.id == task_id,
                task_retry_not_superseded_predicate(),
            ]
            if detached_autonomous and expected_session_id is not None:
                predicates.append(Task.session_id == expected_session_id)
            elif event_record is not None:
                predicates.extend(
                    [
                        Task.instance_id == instance_id,
                        Task.retry_count == event_record.task_retry_count,
                    ]
                )
            return predicates

        async def guard_managed_event_generation(db) -> bool:
            """Lock the exact durable Task→Instance event generation."""

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
        if fatal_provider_error and event_record is not None:
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
            broadcast_data = {k: v for k, v in event.items() if k != "raw_json"}
            if loop_iteration is not None:
                broadcast_data["loop_iteration"] = loop_iteration
            if not detached_autonomous:
                await self.broadcaster.broadcast(
                    f"instance:{instance_id}", broadcast_data
                )
            if task_id:
                await self.broadcaster.broadcast(f"task:{task_id}", broadcast_data)
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
            and owns_event_generation()
        ):
            reactivated_completed_at: datetime | None = None
            async with self.db_factory() as db:
                task_reactivated = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task_id,
                        Task.status == "completed",
                        Task.instance_id == instance_id,
                        Task.retry_count == event_record.task_retry_count,
                        task_retry_not_superseded_predicate(),
                    )
                    .values(status="executing")
                )
                if task_reactivated.rowcount:
                    instance_guard = await db.execute(
                        update(Instance)
                        .where(
                            Instance.id == instance_id,
                            Instance.status == "running",
                            Instance.pid
                            == (getattr(event_record.process, "pid", 0) or 0),
                            Instance.current_task_id == task_id,
                            Instance.started_at
                            == event_record.instance_started_at,
                        )
                        .values(status="running")
                    )
                    if not instance_guard.rowcount or not owns_event_generation():
                        await db.rollback()
                        task_reactivated = None
                    else:
                        reactivated_completed_at = await db.scalar(
                            select(Task.completed_at).where(Task.id == task_id)
                        )
                        await db.commit()
                else:
                    await db.rollback()

            if task_reactivated is not None and task_reactivated.rowcount:
                # Fence publication as well: retry/supersede must acquire the
                # same Task row lock, so an old executing event cannot cross a
                # newer generation.
                async with self.db_factory() as db:
                    publish_task_guard = await db.execute(
                        update(Task)
                        .where(
                            Task.id == task_id,
                            Task.status == "executing",
                            Task.instance_id == instance_id,
                            Task.retry_count == event_record.task_retry_count,
                            (
                                Task.completed_at.is_(None)
                                if reactivated_completed_at is None
                                else Task.completed_at
                                == reactivated_completed_at
                            ),
                            task_retry_not_superseded_predicate(),
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

        # Store the event and related heartbeat/session/unread updates in one
        # transaction.  The old path committed 2-4 times per Codex event,
        # serializing the stream behind SQLite fsyncs before WebSocket delivery.
        # When both ownership rows are touched, preserve the global
        # Task -> Instance lock order used by lifecycle transactions.
        async with self.db_factory() as db:
            if not await guard_managed_event_generation(db):
                await db.rollback()
                logger.info(
                    "Dropping stale durable event for instance %s task %s",
                    instance_id,
                    task_id,
                )
                return
            entry = LogEntry(
                instance_id=instance_id,
                task_id=task_id,
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
                ):
                    task_values["has_unread"] = True
                if task_values:
                    await db.execute(
                        update(Task)
                        .where(*task_event_predicates())
                        .values(**task_values)
                    )
            if not detached_autonomous:
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
        # the PTY layer) — register only after the exact parent event
        # generation committed, so a late old callback cannot create lifecycle
        # state under a replacement turn.
        if (
            task_id
            and event.get("subagent")
            and event["event_type"].startswith("subagent_")
        ):
            try:
                await self._upsert_native_sub_agent(
                    task_id, event["event_type"], event["subagent"]
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
            and
            event.get("event_type") == "rate_limit_event"
            and not event.get("orphan")
            and not event.get("autonomous")
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
            and
            event.get("role") == "assistant"
            and event.get("event_type") in ("message", "result")
            and not event.get("orphan")
            and not event.get("autonomous")
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
        )
        if loop_iteration is not None:
            broadcast_data["loop_iteration"] = loop_iteration
        if not detached_autonomous:
            await self.broadcaster.broadcast(
                f"instance:{instance_id}", broadcast_data
            )
        if task_id:
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
                await self.broadcaster.broadcast(f"task:{task_id}", {
                    "event_type": "context_usage",
                    **context_usage,
                })

    async def _upsert_native_sub_agent(
        self, task_id: int, event_type: str, info: dict
    ) -> None:
        """Mirror a native sub-agent lifecycle event into sub_agent_sessions.

        Keyed by tool_use_id (stored in meta JSON): spawn inserts a running
        record, progress bumps checks_done/last_summary, done completes it.
        Broadcasts sub_agent_* WebSocket events for the frontend panel/badge.
        """
        import json as _json
        from sqlalchemy import select as _select
        from backend.models.sub_agent import SubAgentSession

        tool_use_id = info.get("tool_use_id")
        if not tool_use_id:
            return

        async with self.db_factory() as db:
            existing = (
                await db.execute(
                    _select(SubAgentSession).where(
                        SubAgentSession.task_id == task_id,
                        SubAgentSession.source == "native",
                        SubAgentSession.meta.like(f'%"{tool_use_id}"%'),
                    )
                )
            ).scalars().first()

            if event_type == "subagent_spawn":
                if existing:
                    return  # replay safety
                sa = SubAgentSession(
                    task_id=task_id,
                    agent_type=info.get("kind") or "native-agent",
                    source="native",
                    description=(info.get("description") or "")[:500],
                    status="running",
                    meta=_json.dumps(info, ensure_ascii=False),
                )
                db.add(sa)
                await db.commit()
                await db.refresh(sa)
                await self.broadcaster.broadcast(f"task:{task_id}", {
                    "event_type": "sub_agent_session_created",
                    "sub_agent_session_id": sa.id,
                    "agent_type": sa.agent_type,
                    "source": "native",
                    "description": sa.description,
                })
                return

            if not existing:
                return

            if event_type == "subagent_progress":
                existing.checks_done = (existing.checks_done or 0) + 1
                if info.get("summary"):
                    existing.last_summary = info["summary"][:2000]
                await db.commit()
                await self.broadcaster.broadcast(f"task:{task_id}", {
                    "event_type": "sub_agent_report",
                    "sub_agent_session_id": existing.id,
                    "agent_type": existing.agent_type,
                    "check_number": existing.checks_done,
                    "summary": existing.last_summary,
                })
                # Write progress as system_event in chat (like monitor checks)
                summary_text = (existing.last_summary or "working...")[:300]
                log_content = f"[Agent #{existing.id}] {existing.description}: {summary_text}"
                db.add(LogEntry(
                    instance_id=None,
                    task_id=task_id,
                    event_type="system_event",
                    content=log_content,
                    is_error=False,
                ))
                await db.commit()
                await self.broadcaster.broadcast(f"task:{task_id}", {
                    "event_type": "system_event",
                    "content": log_content,
                })
            elif event_type == "subagent_done":
                existing.status = "completed"
                existing.completed_at = datetime.utcnow()
                if info.get("timed_out"):
                    existing.last_summary = (
                        (existing.last_summary or "") + " [timed out]"
                    ).strip()
                await db.commit()
                await self.broadcaster.broadcast(f"task:{task_id}", {
                    "event_type": "sub_agent_session_status",
                    "sub_agent_session_id": existing.id,
                    "agent_type": existing.agent_type,
                    "status": "completed",
                })
                # 绝不在这里 enqueue auto-resume：subagent_done 只来自 PTY 观测，
                # 而 PTY 模式下 harness 自己的 task-notification 已在同一瞬间唤醒
                # session（唤醒后的产出由 FullMirrorCCMBackend 镜像进聊天）。这里
                # 再投递一条 prompt 必然和该通知 turn 赛跑，输了会被 CLI 当
                # mid-turn steering 吸收（queue-op remove、无独立回显）→
                # send_prompt 的回显锁定永不成立 → consumer 永挂 → 队列冻结 →
                # 7200s 超时杀掉仍在干活的进程（2026-07-15 task 32/33 事故）。
                # -p 模式的退出补唤醒走 _consume_output 的
                # monitor:native-exit-resume，不受影响。

    # ---------------------------------------------------- PTY 权限透传

    _PTY_PERMISSION_TIMEOUT = 120  # channel server 阻塞上限（秒），超时 deny

    def _on_pty_permission_request(self, session_id: str, request: dict) -> None:
        """BridgeHub HTTP 线程回调——只做线程切换，业务在事件循环里处理。"""
        loop = self._loop
        if loop is None or loop.is_closed():
            logger.warning(
                "PTY permission request dropped (no event loop): %s", request
            )
            return
        asyncio.run_coroutine_threadsafe(
            self._handle_pty_permission_request(session_id, request), loop
        )

    async def _handle_pty_permission_request(
        self, session_id: str, request: dict
    ) -> None:
        """把 CC 的权限请求落库并广播成前端聊天卡片。"""
        import json as _json
        import time as _time

        request_id = request.get("request_id")
        if not request_id:
            return

        task_id = None
        async with self.db_factory() as db:
            row = (
                await db.execute(
                    select(Task)
                    .where(Task.session_id == session_id)
                    .order_by(Task.id.desc())
                )
            ).scalars().first()
            if row:
                task_id = row.id

        self._pty_permissions[request_id] = {
            "session_id": session_id,
            "task_id": task_id,
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
            instance_id = row.instance_id or 1
            async with self.db_factory() as db:
                db.add(LogEntry(
                    instance_id=instance_id,
                    task_id=task_id,
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
            await self.broadcaster.broadcast(f"task:{task_id}", payload)
        else:
            logger.warning(
                "PTY permission request for unknown session %s (tool=%s)",
                session_id, request.get("tool_name"),
            )

    async def resolve_pty_permission(self, request_id: str, behavior: str) -> bool:
        """前端按钮回包 → BridgeHub → channel server 解除阻塞。

        Returns False when the request is unknown/expired（channel server
        已超时默认 deny）。
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
            return False
        if self._pty_backend is None:
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
        task_id = pending.get("task_id")
        if ok and task_id:
            async with self.db_factory() as db:
                db.add(LogEntry(
                    instance_id=1,
                    task_id=task_id,
                    event_type="system_event",
                    role="system",
                    content=f"permission_{behavior}: {pending.get('tool_name')}",
                    raw_json=_json.dumps({"request_id": request_id}),
                ))
                await db.commit()
            await self.broadcaster.broadcast(f"task:{task_id}", {
                "event_type": "permission_resolved",
                "request_id": request_id,
                "behavior": behavior,
            })
        return bool(ok)

    async def _spawn_managed_direct_process(
        self,
        instance_id: int,
        task_id: int | None,
        cmd: list[str],
        spawn_kwargs: dict,
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
        first_wait = True
        while first_wait or not spawn.done():
            first_wait = False
            try:
                await asyncio.shield(spawn)
            except asyncio.CancelledError as exc:
                if cancellation is None:
                    cancellation = exc

        process = spawn.result()
        self.processes[instance_id] = process
        if os.name == "posix":
            self._process_groups[instance_id] = process

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
            if self._process_groups.get(instance_id) is process:
                self._process_groups.pop(instance_id, None)

        cleanup = asyncio.create_task(cleanup_cancelled_spawn())
        cleanup_error: BaseException | None = None
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                # Preserve the first cancellation while refusing to abandon
                # the OS child during repeated application-shutdown cancels.
                continue
            except BaseException:
                # Retrieve and handle the completed cleanup failure below,
                # where generation evidence is persisted before cancellation
                # is propagated.
                if cleanup.done():
                    break
                raise
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

        operation = asyncio.create_task(kill_exact())
        cancellation: asyncio.CancelledError | None = None
        while not operation.done():
            try:
                await asyncio.shield(operation)
            except asyncio.CancelledError as exc:
                cancellation = exc
        result = operation.result()
        if cancellation is not None:
            raise cancellation
        return result

    async def stop(
        self,
        instance_id: int,
        *,
        expected_task_id: int | None = None,
        expected_pid: int | None | object = _EXPECTED_GENERATION_UNSET,
        expected_started_at: datetime | None | object = _EXPECTED_GENERATION_UNSET,
        task_status: str = "pending",
        terminal_consumer_timeout: float | None = (
            DEFAULT_TERMINAL_CONSUMER_TIMEOUT
        ),
        consumer_cancel_timeout: float | None = DEFAULT_CONSUMER_CANCEL_TIMEOUT,
    ) -> bool:
        """Cancellation-safe stop of one reusable worker slot.

        ``expected_task_id`` turns a historical instance reference into an
        owner-checked operation. ``expected_pid`` and
        ``expected_started_at`` additionally fence the exact process
        generation (explicit ``None`` is a real expected value; omission
        disables that one fence). All are verified under the launch lock and
        again in the terminal DB CAS, so a recycled slot cannot stop a newer
        generation even when it belongs to the same task.
        """

        if task_status not in {"pending", "completed", "cancelled"}:
            raise ValueError(f"Unsupported terminal task status: {task_status}")

        operation = asyncio.create_task(
            self._stop_serialized(
                instance_id,
                expected_task_id=expected_task_id,
                expected_pid=expected_pid,
                expected_started_at=expected_started_at,
                task_status=task_status,
                terminal_consumer_timeout=terminal_consumer_timeout,
                consumer_cancel_timeout=consumer_cancel_timeout,
            )
        )
        cancellation: asyncio.CancelledError | None = None
        while not operation.done():
            try:
                await asyncio.shield(operation)
            except asyncio.CancelledError as exc:
                # A disconnected API caller or shutdown cancellation must not
                # abandon a signalled process while it is still mapped. Delay
                # propagation until exact process/consumer cleanup completes.
                cancellation = exc
        result = operation.result()
        if cancellation is not None:
            raise cancellation
        return result

    async def _stop_serialized(
        self,
        instance_id: int,
        *,
        expected_task_id: int | None,
        expected_pid: int | None | object,
        expected_started_at: datetime | None | object,
        task_status: str,
        terminal_consumer_timeout: float | None,
        consumer_cancel_timeout: float | None,
    ) -> bool:
        """Serialize stop against launch without cancelling terminal bookkeeping."""

        lifecycle_lock = self._instance_lifecycle_lock(instance_id)
        settled_terminal_consumer = False
        force_cancel_consumer = False
        expected_owner_verified = False
        stop_fence_registered = False
        try:
            while True:
                async with lifecycle_lock:
                    has_expected_owner = (
                        expected_task_id is not None
                        or expected_pid is not _EXPECTED_GENERATION_UNSET
                        or expected_started_at is not _EXPECTED_GENERATION_UNSET
                    )
                    owner: Instance | None | object = (
                        _EXPECTED_GENERATION_UNSET
                    )
                    if has_expected_owner:
                        async with self.db_factory() as db:
                            owner = await db.get(Instance, instance_id)
                        if (
                            owner is None
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
                        # Register exactly one token owned by this stop call.
                        # Do this only after any requested generation fence has
                        # succeeded, so a stale/mismatched stop cannot tear down
                        # or contribute a fence for the current owner.
                        self._begin_stopping(instance_id)
                        stop_fence_registered = True
                    process = (
                        self.processes.get(instance_id)
                        or self._process_groups.get(instance_id)
                        or self._container_exec_processes.get(instance_id)
                    )
                    record = self._consumer_records.get(instance_id)
                    task = (
                        record.task
                        if record is not None
                        else self._tasks.get(instance_id)
                    )
                    record_process = record.process if record is not None else process
                    recovery_pending = (
                        self._consumer_recovery_pending.get(
                            (instance_id, record_process)
                        )
                        if record_process is not None
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
                    else:
                        stopped = await self._stop_locked(
                            instance_id,
                            expected_task_id=expected_task_id,
                            expected_pid=expected_pid,
                            expected_started_at=expected_started_at,
                            task_status=task_status,
                            consumer_cancel_timeout=consumer_cancel_timeout,
                            allow_settled_cleanup=(
                                settled_terminal_consumer
                                or recovery_pending is not None
                            ),
                            verified_owner=owner,
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
                    # finalizer.  Cancel and reap that exact task outside the
                    # lifecycle lock first; the next locked iteration can
                    # then safely take over its abandoned owner claim.
                    if task is not None and not task.done():
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
        task_status: str,
        expected_pid: int | None | object = _EXPECTED_GENERATION_UNSET,
        expected_started_at: datetime | None | object = _EXPECTED_GENERATION_UNSET,
        consumer_cancel_timeout: float | None = None,
        allow_settled_cleanup: bool = False,
        verified_owner: Instance | None | object = (
            _EXPECTED_GENERATION_UNSET
        ),
    ) -> bool:
        """Stop a running Claude Code instance via SIGINT (interrupt).

        Sends SIGINT first so Claude can gracefully save session state,
        then falls back to SIGTERM and SIGKILL if needed.
        """
        process = (
            self.processes.get(instance_id)
            or self._process_groups.get(instance_id)
            or self._container_exec_processes.get(instance_id)
        )
        record = self._consumer_records.get(instance_id)
        task = record.task if record is not None else self._tasks.get(instance_id)
        recovery_evidence = (
            self._consumer_recovery_pending.get((instance_id, process))
            if process is not None
            else None
        )
        # A tracked recovery record supplies the durable per-turn token even
        # when the caller is a generic lifecycle cleanup.  An untracked record
        # may only be reconciled when the caller independently supplies both
        # exact Instance fences.
        if recovery_evidence is not None:
            if recovery_evidence.tracked_generation:
                effective_expected_pid = (
                    getattr(process, "pid", None)
                    if expected_pid is _EXPECTED_GENERATION_UNSET
                    else expected_pid
                )
                effective_expected_started_at = (
                    recovery_evidence.instance_started_at
                    if expected_started_at is _EXPECTED_GENERATION_UNSET
                    else expected_started_at
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
        consumer_live = task is not None and not task.done()
        if not process_live and not consumer_live and not allow_settled_cleanup:
            return False

        pty_managed = (
            process_live
            and self._pty_backend is not None
            and instance_id in getattr(self._pty_backend, "_sessions", {})
        )
        if pty_managed:
            # Esc-interrupt the turn, then tear the session down; the proxy's
            # wait() is unblocked by the backend's on_exit.  Claim terminal
            # ownership before awaiting the consumer: FullMirror.on_exit then
            # skips its lifecycle-locking DB finalizer and leaves the exact
            # Task/Instance transaction to this stop call.  If the consumer
            # already claimed first, _stop_serialized must have routed through
            # its lock-free terminal-consumer wait instead.
            if record is not None and record.process is process:
                terminal_owner = self._claim_pty_terminal_owner(
                    record,
                    "stop",
                    take_over_completed_consumer=True,
                )
                if terminal_owner != "stop":
                    raise RuntimeError(
                        "PTY consumer already owns terminal finalization; "
                        "stop must wait outside the lifecycle lock"
                    )
            container_signal_error: Exception | None = None
            if self._is_managed_container_exec(instance_id, process):
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
            await self._pty_backend.stop(instance_id)
            try:
                await self._wait_process_tree(instance_id, process, 10.0)
            except asyncio.TimeoutError:
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
            await self._signal_managed_process_tree(
                instance_id, process, signal.SIGINT
            )
            try:
                await self._wait_process_tree(instance_id, process, 10.0)
            except asyncio.TimeoutError:
                await self._signal_managed_process_tree(
                    instance_id, process, signal.SIGTERM
                )
                try:
                    await self._wait_process_tree(instance_id, process, 5.0)
                except asyncio.TimeoutError:
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
        published_generation: dict | None = None
        async with self.db_factory() as db:
            if task_id is not None:
                # Global ownership lock order is Task -> Instance.  A no-op
                # UPDATE is portable across SQLite/PostgreSQL/MySQL and also
                # locks an already-terminal Task that cancellation published
                # before asking us to clear its reverse Instance owner.
                task_lock = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task_id,
                        Task.instance_id == instance_id,
                        (
                            Task.id == task_id
                            if recovery_evidence is None
                            or recovery_evidence.task_retry_count is None
                            else Task.retry_count
                            == recovery_evidence.task_retry_count
                        ),
                    )
                    .values(status=Task.status)
                )
                if not task_lock.rowcount:
                    await db.rollback()
                    return False

                current_task_generation = (
                    await db.execute(
                        select(
                            Task.status,
                            Task.retry_count,
                            Task.instance_id,
                            Task.started_at,
                            Task.completed_at,
                        ).where(Task.id == task_id)
                    )
                ).one()
                if current_task_generation.status in {
                    "executing",
                    "in_progress",
                }:
                    task_values: dict = {
                        "status": task_status,
                        "error_message": None,
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
                            Task.instance_id == instance_id,
                        )
                        .values(**task_values)
                    )
                    changed_task_status = bool(task_update.rowcount)

                resulting_task_generation = (
                    await db.execute(
                        select(
                            Task.status,
                            Task.retry_count,
                            Task.instance_id,
                            Task.started_at,
                            Task.completed_at,
                        ).where(Task.id == task_id)
                    )
                ).one()
                published_generation = {
                    "status": resulting_task_generation.status,
                    "retry_count": resulting_task_generation.retry_count,
                    "instance_id": resulting_task_generation.instance_id,
                    "started_at": resulting_task_generation.started_at,
                    "completed_at": resulting_task_generation.completed_at,
                }

            instance_predicates = [Instance.id == instance_id]
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
                .values(status="idle", pid=None, current_task_id=None)
            )
            if instance_update.rowcount == 0:
                await db.rollback()
                return False
            await db.commit()

        if task_id is not None and published_generation is not None:
            # Keep a row lock across publication. A rapid retry/replacement
            # must change one of these exact fields first and therefore
            # suppresses the old generation's status/process-exit events.
            async with self.db_factory() as db:
                generation_predicates = [
                    Task.id == task_id,
                    Task.status == published_generation["status"],
                    Task.retry_count == published_generation["retry_count"],
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
                ]
                publish_guard = await db.execute(
                    update(Task)
                    .where(*generation_predicates)
                    .values(status=published_generation["status"])
                )
                if publish_guard.rowcount:
                    if changed_task_status:
                        from backend.services.task_events import (
                            broadcast_status_change,
                        )

                        await broadcast_status_change(
                            task_id, task_status, instance_id
                        )
                    await self.broadcaster.broadcast(
                        f"task:{task_id}",
                        {
                            "event_type": "process_exit",
                            "exit_code": (
                                process.returncode
                                if process is not None
                                else None
                            ),
                            "stderr": None,
                        },
                    )
                await db.commit()

        if process is not None and self.processes.get(instance_id) is process:
            self.processes.pop(instance_id, None)
            self._codex_exec_homes.pop(instance_id, None)
        if process is not None and self._process_groups.get(instance_id) is process:
            self._process_groups.pop(instance_id, None)
        if task is not None and self._tasks.get(instance_id) is task:
            self._tasks.pop(instance_id, None)
        record = self._consumer_records.get(instance_id)
        if record is not None and record.task is task:
            self._consumer_records.pop(instance_id, None)
        self._clear_consumer_recovery_pending(instance_id, process)
        self._transient_attempts.pop(instance_id, None)
        self._pty_rate_limit_seen.discard(instance_id)
        self._pty_rate_limit_info.pop(instance_id, None)
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
            != "internal_abort"
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
