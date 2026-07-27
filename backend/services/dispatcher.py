import asyncio
import glob
import json
import logging
import os
import re
import signal
import shutil
import tempfile
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from sqlalchemy import select, update, func, or_

from sqlalchemy import select as sa_select

from backend.config import settings
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.models.project import Project
from backend.models.global_settings import GlobalSettings
from backend.models.secret import Secret
from backend.services.git_config import merge_git_config, settings_to_dict
from backend.services.context_compaction import (
    build_compacted_resume_prompt,
    build_compacted_task_retry_prompt,
    context_tokens_used,
    is_context_window_exceeded,
)
from backend.services.instance_capacity import (
    active_capacity_predicate,
    instance_capacity_lock,
    instance_is_reusable_idle,
    instance_occupies_slot,
    reusable_idle_predicate,
)
from backend.services.instance_manager import (
    InstanceAlreadyRunningError,
    InstanceManager,
)
from backend.services.process_safety import require_safe_process_group_id
from backend.services.deployment_start_guard import (
    DeploymentTaskStartBlocked,
)
from backend.services.task_queue import (
    TaskQueue,
    task_is_pr_review_superseded,
    task_retry_not_superseded_predicate,
)
from backend.services.ws_broadcaster import WebSocketBroadcaster

logger = logging.getLogger(__name__)


class QueuedMessagePrelaunchError(RuntimeError):
    """A queued message launch failed before any managed turn could start."""


class TaskQueueAbortTimeoutError(RuntimeError):
    """A dequeued message worker did not settle after cancellation."""


class TaskLifecycleSupersededError(RuntimeError):
    """An external routing side effect lost its immutable Task generation."""


async def _settle_despite_cancellation(awaitable):
    """Finish one critical awaitable and report any outer cancellation.

    ``asyncio.shield`` alone still raises into the caller immediately.  Looping
    on the same operation task makes repeated cancellation harmless until the
    binding/rollback/reset outcome is known; the caller then re-raises the
    original ``CancelledError`` after restoring invariants.
    """

    operation = asyncio.ensure_future(awaitable)
    cancellation: asyncio.CancelledError | None = None
    while not operation.done():
        try:
            await asyncio.shield(operation)
        except asyncio.CancelledError as exc:
            if cancellation is None:
                cancellation = exc
        except BaseException:
            # The operation itself failed and is now inspectable via result().
            break
    return operation, cancellation


def _cleanup_skill_prompt_files(task_id: int):
    """Clean up temporary skill prompt files created during task launch."""
    tmpdir = tempfile.gettempdir()
    for pattern in [f"ccm-skills-{task_id}-*", f"ccm-user-skills-{task_id}-*"]:
        for f in glob.glob(os.path.join(tmpdir, pattern)):
            try:
                os.unlink(f)
            except OSError:
                pass


def _default_provider() -> str:
    provider = getattr(settings, "default_provider", "claude")
    return provider if isinstance(provider, str) and provider else "claude"


def _agent_doc_name(provider: str | None) -> str:
    """Instruction file for the given CLI provider (Codex reads AGENTS.md)."""
    return "AGENTS.md" if (provider or "claude").lower() == "codex" else "CLAUDE.md"


# CLAUDE.md/AGENTS.md 同步纪律：靠 agent 编码时自觉执行、不做程序化同步。
# 经 prompt 前导下发是唯一覆盖所有被开发项目的注入点（老项目的文档里没有这条规则）。
_DOC_SYNC_NOTE = (
    "注意：如需修改 CLAUDE.md 或 AGENTS.md，两个文件的关键内容必须保持同步——"
    "往其中一个写入新内容时，把相同的意思也写进另一个（不要求逐字一致；"
    "若两者是 symlink 关系则改一处即可，无需额外操作）。"
)


def _agent_doc_preamble(provider: str | None) -> str:
    """First-line prompt preamble pointing the agent at the project doc.

    Codex automatically loads AGENTS.md.  Explicitly telling it to read the
    same file again makes even a trivial task perform redundant shell/file
    operations.  Keep only the cross-document synchronization rule for Codex;
    Claude still needs the explicit CLAUDE.md workflow reminder.
    """
    if (provider or "claude").lower() == "codex":
        return _DOC_SYNC_NOTE
    read_line = "请阅读项目根目录的 CLAUDE.md 了解项目规范和任务完成后的 git 流程。"
    return f"{read_line}\n{_DOC_SYNC_NOTE}"


# Priority levels for the per-task message queue
PRIORITY_USER = 0
PRIORITY_MONITOR_COMPLETE = 1
PRIORITY_MONITOR_IMPORTANT = 2

# Per-task queue consumer tuning (module-level so tests can patch them).
# QUEUE_CONSUMER_IDLE_TIMEOUT: stop a consumer after this many idle seconds.
# QUEUE_HEARTBEAT_INTERVAL: how often the consumer marks itself alive.
# QUEUE_STUCK_THRESHOLD: _ensure_queue_worker treats a consumer whose heartbeat
#   is older than this as wedged and respawns it. It MUST be comfortably larger
#   than the heartbeat interval — a heartbeat now runs for the consumer's whole
#   lifetime (incl. a multi-minute turn or an idle wait), so a fresh heartbeat
#   means "alive" and only a truly wedged event loop trips the watchdog. See
#   prod task #728: a 14-min turn used to look "stuck", got respawned, and
#   produced concurrent `claude --resume` on one session.
QUEUE_CONSUMER_IDLE_TIMEOUT = 300
QUEUE_HEARTBEAT_INTERVAL = 30
QUEUE_STUCK_THRESHOLD = 120
CODEX_ROUTING_RETRY_DELAY = 5
SHUTDOWN_TERMINAL_CONSUMER_TIMEOUT = 10
SHUTDOWN_CONSUMER_CANCEL_TIMEOUT = 5
TASK_QUEUE_ABORT_TIMEOUT = 15.0
AUX_LIFECYCLE_CANCEL_TIMEOUT = 10.0
DISPATCHER_BACKGROUND_STOP_TIMEOUT = 10.0
SHUTDOWN_LIFECYCLE_CANCEL_TIMEOUT = 15.0
@dataclass(frozen=True)
class _TaskStatusGeneration:
    """Exact durable Task generation used to fence status publication."""

    task_id: int
    worker_id: int | None
    shared_from_id: int | None
    status: str
    retry_count: int
    instance_id: int | None
    started_at: datetime | None
    completed_at: datetime | None


@dataclass(frozen=True)
class _TaskLifecycleGeneration:
    """Immutable owner generation for one dispatcher lifecycle coroutine.

    ``status`` is deliberately excluded because the same lifecycle advances
    from ``in_progress`` to ``executing``.  Every other mutable ownership field
    is frozen from the DB-normalized Step 2 row and must still match before an
    old coroutine may launch, refresh, retry, complete, fail, or clean up.
    """

    task_id: int
    worker_id: int | None
    shared_from_id: int | None
    retry_count: int
    instance_id: int | None
    started_at: datetime | None
    completed_at: datetime | None


_TaskRoutingGeneration = (
    _TaskLifecycleGeneration | _TaskStatusGeneration
)


class CodexAccountRoutingError(RuntimeError):
    """A Codex turn cannot be safely assigned to an account right now."""

    def __init__(
        self,
        message: str,
        *,
        retry_after: float | None = None,
        permanent: bool = False,
    ):
        super().__init__(message)
        self.retry_after = retry_after
        self.permanent = permanent


class ClaudeAccountRoutingError(QueuedMessagePrelaunchError):
    """A Claude turn cannot be safely assigned without risking its context."""

    def __init__(
        self,
        message: str,
        *,
        retry_after: float | None = None,
        permanent: bool = False,
    ):
        super().__init__(message)
        self.retry_after = retry_after
        self.permanent = permanent


class TaskStartPausedError(RuntimeError):
    """A new task turn reached the admission gate during maintenance."""


@dataclass(order=True)
class QueuedMessage:
    priority: int
    timestamp: float = field(compare=True)
    prompt: str = field(compare=False)
    # Queue clears advance a per-task generation. A consumer that has already
    # dequeued this object but has not registered it as in-flight can then
    # recognize that stop-session cancelled the handoff.
    queue_generation: int = field(compare=False, default=0)
    source: str = field(compare=False, default="user")
    user_message_text: str | None = field(compare=False, default=None)
    command_skills: dict | None = field(compare=False, default=None)
    # One-shot model override for this message only (not persisted to task)
    model_override: str | None = field(compare=False, default=None)
    # Source monitor/sub-agent session ID for dedup (frontend uses this to
    # render [Monitor] / [Sub-Agent] badges on injected user_message bubbles)
    monitor_session_id: int | None = field(compare=False, default=None)
    # The API persists a visible user row before queue admission.  Keep its
    # exact id so compaction can exclude the current request from history.
    source_log_id: int | None = field(compare=False, default=None)
    # Immutable model-facing request. ``prompt`` may later be wrapped with a
    # compacted history; retries must not treat that wrapper as a new request.
    current_message: str | None = field(compare=False, default=None)
    # A routing retry reuses this same object. Monitor/sub-agent source bubbles
    # are persisted/broadcast once, not once per account-maintenance retry.
    source_logged: bool = field(compare=False, default=False)
    # Transient in-process reservation held between idle-instance selection and
    # launch().  The queue consumer releases it in its outer finally as a
    # fail-safe for any pre-launch exception.
    instance_claim: tuple[int, object] | None = field(
        compare=False, default=None, repr=False
    )
    # Recovery/context compaction can intentionally clear Task.session_id and
    # start a new native session.  Preserve that admission fact on the exact
    # queued object if routing/slot contention requires another queue attempt.
    allow_new_session: bool = field(compare=False, default=False)


def _binary_available(binary: str) -> bool:
    if not isinstance(binary, str) or not binary:
        return False
    path = Path(binary).expanduser()
    if path.is_absolute() or any(sep in binary for sep in (os.sep, os.altsep) if sep):
        return path.exists()
    return shutil.which(binary) is not None


def _codex_binary_available() -> bool:
    configured = settings.codex_binary
    if configured and configured.lower() != "codex":
        return _binary_available(configured)
    if _binary_available(configured or "codex"):
        return True

    local_appdata = os.environ.get("LOCALAPPDATA")
    if not local_appdata:
        return False
    bin_root = Path(local_appdata) / "OpenAI" / "Codex" / "bin"
    return any(bin_root.glob("*/codex.exe"))


def _provider_available(provider: str) -> bool:
    provider = (provider or "claude").lower()
    if provider == "claude":
        return _binary_available(settings.claude_binary)
    if provider == "codex":
        return _codex_binary_available()
    return False


def _default_worker_provider() -> str:
    provider = _default_provider()
    if _provider_available(provider):
        return provider
    if provider == "claude" and _provider_available("codex"):
        return "codex"
    return provider


def _default_worker_model(provider: str) -> str:
    return settings.default_codex_model if provider == "codex" else settings.default_model


def _initial_task_command(task: Task):
    """Parse an explicit leading $command from a newly-created Task."""

    from backend.services.command_registry import parse_command

    return parse_command(task.description or "")


def _initial_task_launch_skills(task: Task) -> tuple[dict, bool]:
    """Return launch-visible skills and whether they are temporary."""

    original = dict(task.enabled_skills or {})
    command, _args = _initial_task_command(task)
    required = dict(command.required_skills or {}) if command else {}
    if not required:
        return original, False
    effective = dict(original)
    effective.update(required)
    return effective, effective != original


def _build_git_env(merged_config: dict) -> dict:
    """Build git-related environment variables from a merged git config dict.

    GIT_AUTHOR_* / GIT_COMMITTER_* override user.name/email for every git commit
    executed inside the Claude Code subprocess, regardless of any ~/.gitconfig.
    GIT_SSH_COMMAND overrides the SSH key used for push/pull over SSH.
    GIT_ASKPASS overrides credentials for push/pull over HTTPS.

    Both SSH and HTTPS credentials are injected simultaneously when available,
    because the remote URL protocol determines which one git actually uses.
    This way, users don't need to worry about matching credential type to URL.

    Priority: project-level > global settings > instance-level (settings.git_ssh_key_path).
    """
    env: dict = {}
    if merged_config.get("git_author_name"):
        env["GIT_AUTHOR_NAME"] = merged_config["git_author_name"]
        env["GIT_COMMITTER_NAME"] = merged_config["git_author_name"]
    if merged_config.get("git_author_email"):
        env["GIT_AUTHOR_EMAIL"] = merged_config["git_author_email"]
        env["GIT_COMMITTER_EMAIL"] = merged_config["git_author_email"]

    # Inject SSH credentials if available
    if merged_config.get("git_ssh_key_path"):
        env["GIT_SSH_COMMAND"] = f"ssh -i {merged_config['git_ssh_key_path']} -o IdentitiesOnly=yes -o StrictHostKeyChecking=no"

    # Inject HTTPS credentials if available
    if merged_config.get("git_https_token"):
        askpass_script = _get_or_create_askpass_script(
            merged_config.get("git_https_username") or "",
            merged_config["git_https_token"],
        )
        env["GIT_ASKPASS"] = askpass_script
        env["GIT_TERMINAL_PROMPT"] = "0"
        # Bypass global/system git config entirely so that macOS osxkeychain
        # (or any other system credential helper) never intercepts our credentials.
        # GIT_CONFIG_COUNT approach doesn't work: empty credential.helper via env
        # is treated as an additive entry, not a chain reset.
        env["GIT_CONFIG_GLOBAL"] = "/dev/null"
        env["GIT_CONFIG_NOSYSTEM"] = "1"

    # Fallback to instance-level SSH key (set via GIT_SSH_KEY_PATH env var)
    if "GIT_SSH_COMMAND" not in env and settings.git_ssh_key_path:
        env["GIT_SSH_COMMAND"] = f"ssh -i {settings.git_ssh_key_path} -o IdentitiesOnly=yes -o StrictHostKeyChecking=no"
    return env


def _get_or_create_askpass_script(username: str, token: str) -> str:
    """Create a temporary GIT_ASKPASS script that provides HTTPS credentials.

    The script echoes the username when git asks for "Username" and the token
    when git asks for "Password". This avoids storing credentials in the URL
    or relying on any system credential helper.
    """
    import hashlib
    import stat
    import tempfile
    from pathlib import Path

    # Use a stable path based on credential hash so we don't create unlimited files
    cred_hash = hashlib.sha256(f"{username}:{token}".encode()).hexdigest()[:12]
    askpass_dir = Path(tempfile.gettempdir()) / "claude-manager-askpass"
    askpass_dir.mkdir(exist_ok=True)
    askpass_path = askpass_dir / f"askpass_{cred_hash}.sh"

    if not askpass_path.exists():
        # The script receives a prompt like "Username for ..." or "Password for ..."
        script_content = f"""#!/bin/sh
case "$1" in
    Username*) echo "{username}" ;;
    *) echo "{token}" ;;
esac
"""
        askpass_path.write_text(script_content)
        askpass_path.chmod(stat.S_IRWXU)  # 0o700

    return str(askpass_path)


async def _build_secrets_block(db_factory, secret_ids: list[int]) -> str:
    """Load secrets by IDs and format them as a prompt block."""
    if not secret_ids:
        return ""
    async with db_factory() as db:
        result = await db.execute(
            sa_select(Secret).where(Secret.id.in_(secret_ids))
        )
        secrets = list(result.scalars().all())
    if not secrets:
        return ""
    lines = ["以下是用户提供的私密信息，请在需要时使用（不要在输出中泄露）："]
    for s in secrets:
        lines.append(f"- {s.name}: {s.content}")
    return "\n".join(lines)


class GlobalDispatcher:
    """Single global dispatcher that manages all instances and task lifecycle.

    Claude Code is fully autonomous — it handles worktree creation, commit,
    fetch, merge, push, conflict resolution, and cleanup itself via CLAUDE.md.
    The dispatcher only manages:
    - Task assignment (dequeue)
    - Starting/waiting on Claude Code processes
    - Marking tasks completed/failed
    - Pool rotation on rate limit (when pool is enabled)
    """

    def __init__(
        self,
        db_factory,
        instance_manager: InstanceManager,
        broadcaster: WebSocketBroadcaster,
    ):
        self.db_factory = db_factory
        self.instance_manager = instance_manager
        self.broadcaster = broadcaster
        self._dispatch_task: asyncio.Task | None = None
        # New tasks wake the loop immediately.  The 2s timeout remains as a
        # safety poll for tasks inserted by legacy/direct DB paths.
        self._dispatch_wakeup = asyncio.Event()
        # Self-update maintenance gate: pause new claims without cancelling any
        # lifecycle that is already running. Every path that can turn idle work
        # into active Task work must cross this lock and persist its active
        # status before releasing it.
        self._dispatch_claim_lock = asyncio.Lock()
        self._dispatch_paused = False
        self._maintenance_shutdown_committed = False
        self._dispatch_resumed = asyncio.Event()
        self._dispatch_resumed.set()
        # backend.main injects a repo-scoped, cross-process deployment fence.
        # Tests and embedders may leave it unset.
        self.deployment_task_start_fence = None
        # Local lifecycle tasks use integer Instance IDs.  Worker forwarding
        # tasks use string keys (``worker-<task_id>``) and must never leak into
        # SQL predicates against the integer ``instances.id`` column.
        self._running_tasks: dict[int | str, asyncio.Task] = {}
        # Instances mid-launch by the queued-message path. Their DB status is
        # still "idle" until launch() flips it to "running", so without an
        # in-memory claim the dispatch loop (and other queued-message launches)
        # could grab the same instance and clobber the half-started PTY session
        # (prod task #676).
        self._launching_instances: set[int] = set()
        # Selection and reservation must be one atomic in-process operation.
        # Otherwise two per-task queue consumers can both SELECT the same idle
        # row before either reaches `_launching_instances.add()`.
        self._instance_claim_lock = asyncio.Lock()
        self._instance_claim_owners: dict[
            int, tuple[object, asyncio.Task | None]
        ] = {}
        # Startup reconciliation and queued-chat Phase 1 share this gate.
        # A queued turn may do slow account/session preparation after reserving
        # an idle slot; start() must either observe that spawned generation or
        # finish its stale-state snapshot before the turn can spawn.
        self._chat_launch_admission_lock = asyncio.Lock()
        self._running = False
        self._shutting_down = False
        self._monitor_tasks: dict[int, asyncio.Task] = {}           # monitor_session_id -> asyncio task
        self._monitor_processes: dict[int, asyncio.subprocess.Process] = {}  # monitor_session_id -> subprocess
        self._monitor_log_fhs: dict[int, object] = {}  # monitor_session_id -> log file handle

        # Sub-agent (one-shot tasks) lifecycle — parallel to monitor
        self._sub_agent_tasks: dict[int, asyncio.Task] = {}      # session_id -> asyncio task
        self._sub_agent_processes: dict[int, asyncio.subprocess.Process] = {}
        self._sub_agent_log_fhs: dict[int, object] = {}
        # Codex turns share an account-level app-server process. Keep them
        # separate from OS subprocesses so auxiliary cleanup never sends a
        # process-group signal to the shared transport.
        self._sub_agent_codex_processes: dict[int, object] = {}
        self._sub_agent_codex_homes: dict[int, str | None] = {}
        self._sub_agent_codex_threads: dict[int, str] = {}

        # Per-task message queue for serialized chat/monitor messages
        self._task_queues: dict[int, asyncio.PriorityQueue] = {}
        self._task_queue_workers: dict[int, asyncio.Task] = {}
        self._task_queue_activity: dict[int, float] = {}
        # A queued or currently-consumed resume is task work even before its DB
        # status becomes executing. Keeping it as a maintenance blocker avoids
        # restarting after accepting a chat/monitor message but before launch.
        self._pending_task_starts: set[int] = set()
        # A queue item stops contributing to qsize() as soon as a consumer
        # dequeues it. Track consumers separately so clearing the remaining
        # queue cannot erase the blocker for work already in preparation.
        self._task_queue_inflight: dict[int, int] = {}
        # Cancellation generation for the dequeue -> in-flight handoff window.
        # Entries intentionally outlive empty queues: deleting one could make a
        # stale dequeued message's old generation look current again.
        self._task_queue_generations: dict[int, int] = {}
        # Fresh lifecycle tasks waiting for a provider-account cooldown or
        # maintenance window. TaskQueue excludes them without consuming retry
        # budget, while unrelated pending tasks can still use idle instances.
        self._account_routing_not_before: dict[int, float] = {}

        # Pool: initialized lazily on start() if pool_enabled
        self.pool: "ClaudePool | None" = None
        # Injected by backend.main. A configured CloudRouter store can provide
        # Claude/Codex account projections even when the native OAuth pool file
        # does not exist.
        self.cloudrouter_store = None
        # CodexPool is created by backend.main and injected after construction.
        # Provider account ownership lives on Task.metadata_
        # ("claude_account_id"/"codex_account_id") because instances are
        # generic workers that rotate between unrelated tasks.
        self.codex_pool: "CodexPool | None" = None

    @property
    def is_running(self) -> bool:
        return self._running

    async def start(self):
        if self._shutting_down:
            raise RuntimeError("GlobalDispatcher is shutting down")
        if self._running:
            return
        self._running = True
        try:
            # Initialize pool if enabled
            has_cloudrouter_claude = bool(
                self.cloudrouter_store is not None
                and any(
                    account.enabled
                    and not account.retired
                    and account.supports_model("claude", None)
                    for account in self.cloudrouter_store.all_accounts(
                        include_retired=True
                    )
                )
            )
            if settings.pool_enabled or has_cloudrouter_claude:
                from backend.services.claude_pool import ClaudePool
                self.pool = ClaudePool(
                    config_path=settings.pool_config_path,
                    cooldown_seconds=settings.pool_cooldown_seconds,
                    cloudrouter_store=self.cloudrouter_store,
                    bootstrap_default=settings.pool_enabled,
                    include_native=settings.pool_enabled,
                )
                logger.info(
                    "Claude pool enabled with %d accounts",
                    len(self.pool._accounts),
                )

            # A paused queue consumer is still allowed to answer chat.  Fence
            # its pre-spawn phase so no child can appear after reconciliation's
            # manager-owned snapshot without being represented in that snapshot.
            async with self._chat_launch_admission_lock:
                await self._cleanup_stale_state()

            # Ensure we have worker instances up to max_concurrent_instances
            await self._ensure_instances()

            self._dispatch_task = asyncio.create_task(self._dispatch_loop())
            self._curator_task = asyncio.create_task(self._curator_loop())
        except BaseException:
            # start() is retryable.  In particular, a transient DB failure in
            # stale-state cleanup must not leave the public state claiming the
            # dispatcher is running while no dispatch loop exists.
            self._running = False
            logger.exception("GlobalDispatcher failed to start")
            raise
        logger.info("GlobalDispatcher started")

    def wake(self) -> None:
        """Wake the task dispatcher after a pending task is committed."""
        self._dispatch_wakeup.set()

    async def _reserve_idle_instance(
        self,
        db,
        *,
        instance_id: int | None = None,
    ) -> tuple[Instance | None, object | None]:
        """Atomically select and reserve one DB-idle local instance.

        The DB status remains ``idle`` until ``InstanceManager.launch`` has
        created the process/turn, so the reservation lives in memory.  All
        dispatcher paths use the same lock and publish the reservation before
        releasing it; this closes the SELECT -> claim race between concurrent
        per-task consumers and the fresh-task dispatch loop.
        """
        async with self._instance_claim_lock:
            busy_iids = {
                iid
                for iid, task in self._running_tasks.items()
                if type(iid) is int and not task.done()
            } | self._launching_instances
            cap = settings.max_concurrent_instances
            if cap > 0:
                occupied_iids = set(
                    (
                        await db.execute(
                            select(Instance.id).where(
                                active_capacity_predicate()
                            )
                        )
                    ).scalars()
                ) | busy_iids
                # Lowering the cap never interrupts active work; it only
                # closes admission until occupancy falls below the new cap.
                if len(occupied_iids) >= cap:
                    return None, None

            stmt = select(Instance).where(reusable_idle_predicate())
            if instance_id is not None:
                stmt = stmt.where(Instance.id == instance_id)
            if busy_iids:
                stmt = stmt.where(Instance.id.notin_(busy_iids))
            result = await db.execute(stmt.order_by(Instance.id).limit(1))
            instance = result.scalar_one_or_none()
            if instance is None:
                return None, None

            token = object()
            self._launching_instances.add(instance.id)
            self._instance_claim_owners[instance.id] = (
                token,
                asyncio.current_task(),
            )
            return instance, token

    async def _release_instance_reservation(
        self,
        instance_id: int,
        token: object,
    ) -> None:
        """Release a reservation only when ``token`` still owns it."""
        async with self._instance_claim_lock:
            claim = self._instance_claim_owners.get(instance_id)
            if claim is None or claim[0] is not token:
                return
            self._instance_claim_owners.pop(instance_id, None)
            self._launching_instances.discard(instance_id)

    async def _release_owned_instance_reservations(
        self,
        owner: asyncio.Task | None,
    ) -> None:
        """Fail-safe cleanup when the dispatch loop errors or is cancelled."""
        async with self._instance_claim_lock:
            owned = [
                instance_id
                for instance_id, (_token, claim_owner)
                in self._instance_claim_owners.items()
                if claim_owner is owner
            ]
            for instance_id in owned:
                self._instance_claim_owners.pop(instance_id, None)
                self._launching_instances.discard(instance_id)

    async def pause_dispatching(self) -> None:
        """Close task-start admission while allowing active tasks to finish.

        Taking the same lock as every start path waits for any in-flight claim
        to persist ``in_progress``/``executing`` before this method returns.
        """
        async with self._dispatch_claim_lock:
            self._dispatch_paused = True
            self._maintenance_shutdown_committed = False
            self._dispatch_resumed.clear()
        self._dispatch_wakeup.set()

    def resume_dispatching(self) -> None:
        """Resume task claims after a cancelled or completed maintenance run."""
        self._dispatch_paused = False
        self._maintenance_shutdown_committed = False
        self._dispatch_resumed.set()
        self._dispatch_wakeup.set()

    @asynccontextmanager
    async def task_start_guard(self):
        """Admit one new Task start and serialize it with maintenance.

        The caller must commit the Task's active status before leaving the
        context. A paused caller retries after ``wait_until_resumed`` instead
        of launching work in the shutdown window.
        """
        async with self._dispatch_claim_lock:
            if self._dispatch_paused or self._shutting_down:
                raise TaskStartPausedError("task starts are paused for maintenance")
            fence = self.deployment_task_start_fence
            if fence is None:
                yield
                return
            try:
                with fence():
                    yield
            except DeploymentTaskStartBlocked as exc:
                raise TaskStartPausedError(str(exc)) from exc

    async def wait_until_resumed(self) -> None:
        await self._dispatch_resumed.wait()

    async def pending_task_start_ids(self) -> set[int]:
        """Return queued/in-flight resume task IDs under the admission lock."""
        async with self._dispatch_claim_lock:
            return set(self._pending_task_starts)

    @asynccontextmanager
    async def maintenance_shutdown_guard(self):
        """Hold task admission closed across the final check and stop spawn."""
        async with self._dispatch_claim_lock:
            if not self._dispatch_paused:
                raise RuntimeError("maintenance shutdown requires paused dispatching")
            yield set(self._pending_task_starts)

    def commit_maintenance_shutdown(self) -> None:
        """Seal admission after the final check while the guard is held."""
        if not self._dispatch_paused or not self._dispatch_claim_lock.locked():
            raise RuntimeError("shutdown commit must hold paused task admission")
        self._maintenance_shutdown_committed = True

    async def reconcile_stale_state_for_maintenance(self) -> None:
        """Reconcile orphaned runtime claims while task admission is closed.

        UpdateService uses this explicit entry point instead of interpreting
        persisted PIDs itself.  The chat-launch lock closes the remaining
        pre-spawn window; manager-owned process/consumer generations are still
        preserved by ``_cleanup_stale_state``.
        """

        if not self._dispatch_paused:
            raise RuntimeError(
                "stale-state reconciliation requires paused task admission"
            )
        async with self._chat_launch_admission_lock:
            # This is an in-process operator action, not process startup.
            # Auxiliary/native sub-agents have separate ownership registries;
            # the startup-only stale sweep would incorrectly fail live rows.
            await self._cleanup_stale_state(reconcile_auxiliary=False)

    async def _cleanup_stale_state(
        self,
        *,
        reconcile_auxiliary: bool = True,
    ):
        """Reconcile persisted claims with generations owned by this process.

        An OS PID is not attachable state and may have been reused.  After a
        real manager restart, a ``running`` row without an in-memory process or
        output consumer is quarantined as terminal ``error``.  Dead/no PID
        claims return to pending only when Task/Instance ownership is unique
        and bidirectionally consistent; corrupt ownership or a PID that may
        still be alive fails the task closed so CCM cannot start a duplicate
        writer. Conversely, Pause -> Start preserves manager-owned generations
        exactly as they are.
        """

        import os

        manager_owned_instance_ids: set[int] = set()
        for instance_id in (
            set(self.instance_manager.processes)
            | set(getattr(self.instance_manager, "_tasks", {}))
            | set(getattr(self.instance_manager, "_consumer_records", {}))
            | set(getattr(self.instance_manager, "_process_groups", {}))
            | set(
                getattr(
                    self.instance_manager,
                    "_container_exec_processes",
                    {},
                )
            )
        ):
            if not isinstance(instance_id, int):
                continue
            records = getattr(self.instance_manager, "_consumer_records", {})
            record = (
                records.get(instance_id)
                if isinstance(records, dict)
                else None
            )
            process = (
                self.instance_manager.processes.get(instance_id)
                or getattr(self.instance_manager, "_process_groups", {}).get(
                    instance_id
                )
                or getattr(
                    self.instance_manager,
                    "_container_exec_processes",
                    {},
                ).get(instance_id)
                or getattr(record, "process", None)
            )
            consumer = (
                getattr(record, "task", None)
                or getattr(self.instance_manager, "_tasks", {}).get(instance_id)
            )
            running_result = self.instance_manager.is_running(instance_id)
            manager_reports_running = (
                running_result if isinstance(running_result, bool) else False
            )
            if manager_reports_running or (
                (process is not None and process.returncode is None)
                or (consumer is not None and not consumer.done())
            ):
                manager_owned_instance_ids.add(instance_id)
        # A fresh lifecycle can be in account/project preparation before the
        # subprocess map exists.  It is still an in-process owned generation
        # and must survive an immediate Pause -> Start.
        manager_owned_instance_ids |= self._active_local_instance_ids()
        # The fresh-task path keeps its exact Instance reservation from the
        # pending -> in_progress claim through project/config preparation, but
        # does not publish `_running_tasks` until that preparation completes.
        # Maintenance can pause immediately after the durable claim, so take a
        # lock-consistent reservation snapshot as additional ownership proof.
        # Otherwise reconciliation could fail a legitimate admitted Task in
        # this narrow pre-lifecycle window.
        async with self._instance_claim_lock:
            manager_owned_instance_ids.update(self._launching_instances)

        async with self.db_factory() as db:
            result = await db.execute(
                select(Instance).where(
                    or_(
                        Instance.status == "running",
                        Instance.pid.isnot(None),
                        Instance.current_task_id.isnot(None),
                    )
                )
            )
            persisted_instances = list(result.scalars().all())
            live_task_ids: set[int] = set()
            unmanaged_live_pids: dict[int, int] = {}
            unmanaged_live_instance_pids: dict[int, int] = {}
            unmanaged_live_owners: dict[int, tuple[int, int]] = {}
            reverse_owner_ids: dict[int, set[int]] = {}
            reconciliation_race_instance_ids: set[int] = set()
            stale_instances: list[
                tuple[Instance, bool]
            ] = []
            for inst in persisted_instances:
                if inst.current_task_id is not None:
                    reverse_owner_ids.setdefault(
                        inst.current_task_id, set()
                    ).add(inst.id)
                if inst.id in manager_owned_instance_ids:
                    if inst.current_task_id is not None:
                        live_task_ids.add(inst.current_task_id)
                    continue
                pid_may_be_alive = False
                if inst.pid is not None:
                    try:
                        os.kill(inst.pid, 0)
                        pid_may_be_alive = True
                    except ProcessLookupError:
                        pass
                    except OSError:
                        # Anything other than a definitive ESRCH is uncertain
                        # and therefore fail-closed against duplicate writes.
                        pid_may_be_alive = True
                logger.warning(
                    "Quarantining unowned instance %s (%s), persisted PID %s%s",
                    inst.id,
                    inst.name,
                    inst.pid,
                    " may still be alive" if pid_may_be_alive else "",
                )
                stale_instances.append((inst, pid_may_be_alive))

            if manager_owned_instance_ids:
                owned_task_ids = await db.execute(
                    select(Task.id).where(
                        Task.instance_id.in_(manager_owned_instance_ids),
                        Task.status.in_(["executing", "in_progress"]),
                        Task.worker_id.is_(None),
                        Task.shared_from_id.is_(None),
                    )
                )
                live_task_ids.update(owned_task_ids.scalars().all())

            active_result = await db.execute(
                select(Task).where(
                    Task.status.in_(["executing", "in_progress"]),
                    Task.worker_id.is_(None),
                    # Shared tasks are remote-authoritative mirror rows and
                    # never execute on this dispatcher. Reconciliation must
                    # not rewrite their synced lifecycle state.
                    Task.shared_from_id.is_(None),
                )
            )
            active_tasks = list(active_result.scalars().all())

            # Establish the global lifecycle lock order before touching any
            # Instance row.  Startup cleanup may later update active tasks and
            # pending reverse owners, so lock every possible Task first.  The
            # exact no-op UPDATE is also a CAS on SQLite/MySQL configurations
            # where SELECT FOR UPDATE alone is insufficient.
            task_ids_to_lock = {task.id for task in active_tasks}
            task_ids_to_lock.update(
                inst.current_task_id
                for inst, _ in stale_instances
                if inst.current_task_id is not None
            )
            if task_ids_to_lock:
                locked_tasks = list(
                    (
                        await db.execute(
                            select(Task)
                            .where(Task.id.in_(task_ids_to_lock))
                            .order_by(Task.id)
                            .with_for_update()
                            .execution_options(populate_existing=True)
                        )
                    )
                    .scalars()
                    .all()
                )
                for locked_task in locked_tasks:
                    locked_generation = self._task_status_generation(
                        locked_task
                    )
                    task_guard = await db.execute(
                        update(Task)
                        .where(
                            *self._task_status_generation_predicates(
                                locked_generation
                            )
                        )
                        .values(status=locked_generation.status)
                    )
                    if not task_guard.rowcount:
                        await db.rollback()
                        logger.warning(
                            "Aborted stale-state cleanup because task %s "
                            "changed while acquiring Task->Instance locks",
                            locked_task.id,
                        )
                        return

            # Task locks are now held.  Instance quarantine may safely follow;
            # all later Task transitions reuse the already locked rows.
            for inst, pid_may_be_alive in stale_instances:
                quarantine_values = {"status": "error"}
                if not pid_may_be_alive:
                    # A definitively dead/no-PID generation is safe to detach.
                    # For an uncertain live PID, retain both links as evidence
                    # so retry/cleanup can continue to block duplicate work.
                    quarantine_values.update(current_task_id=None, pid=None)
                quarantined = await db.execute(
                    update(Instance)
                    .where(
                        Instance.id == inst.id,
                        # Match the complete persisted generation observed by
                        # the SELECT.  In particular, an ``idle`` row carrying
                        # a live PID is dirty orphan evidence, not an available
                        # slot.  A concurrent owner/PID/status change must win
                        # this CAS and be left untouched for the next pass.
                        Instance.status == inst.status,
                        Instance.current_task_id == inst.current_task_id,
                        Instance.pid == inst.pid,
                        (
                            Instance.started_at.is_(None)
                            if inst.started_at is None
                            else Instance.started_at == inst.started_at
                        ),
                    )
                    .values(**quarantine_values)
                )
                if not quarantined.rowcount:
                    reconciliation_race_instance_ids.add(inst.id)
                    logger.warning(
                        "Skipped stale-state quarantine for instance %s "
                        "because its persisted generation changed concurrently",
                        inst.id,
                    )
                    continue
                if pid_may_be_alive and inst.current_task_id is not None:
                    unmanaged_live_pids[inst.current_task_id] = inst.pid
                    unmanaged_live_owners[inst.current_task_id] = (
                        inst.id,
                        inst.pid,
                    )
                if pid_may_be_alive:
                    unmanaged_live_instance_pids[inst.id] = inst.pid

            reset_tasks: list[_TaskStatusGeneration] = []
            for t in active_tasks:
                if t.id in live_task_ids:
                    continue
                if t.instance_id in reconciliation_race_instance_ids:
                    # The instance changed after our ownership snapshot.  Do
                    # not apply a stale task decision to its newer generation.
                    continue
                task_reverse_owners = reverse_owner_ids.get(t.id, set())
                if task_reverse_owners & reconciliation_race_instance_ids:
                    # A duplicate/corrupt reverse owner also participates in
                    # the generation proof. If any one of them changed, leave
                    # the Task untouched and retry reconciliation later.
                    continue
                unmanaged_pid = unmanaged_live_pids.get(t.id)
                if unmanaged_pid is None and t.instance_id is not None:
                    unmanaged_pid = unmanaged_live_instance_pids.get(t.instance_id)
                if unmanaged_pid is not None:
                    new_status = "failed"
                    values = {
                        "status": "failed",
                        "completed_at": datetime.utcnow(),
                        "error_message": (
                            f"Unmanaged process PID {unmanaged_pid} may still "
                            "be running after manager restart; automatic retry "
                            "was blocked to prevent duplicate execution"
                        ),
                    }
                    logger.error(
                        "Fail-closing task %s because unmanaged PID %s may be alive",
                        t.id,
                        unmanaged_pid,
                    )
                elif (
                    len(task_reverse_owners) > 1
                    or (
                        task_reverse_owners
                        and t.instance_id not in task_reverse_owners
                    )
                    or (
                        not task_reverse_owners
                        and t.instance_id is not None
                    )
                ):
                    # Only a unique, bidirectionally consistent dead claim is
                    # safe to retry automatically. Multiple reverse owners (or
                    # a mismatched Task owner) mean the generation history is
                    # corrupt; replay could duplicate non-idempotent work.
                    new_status = "failed"
                    values = {
                        "status": "failed",
                        "instance_id": None,
                        "completed_at": datetime.utcnow(),
                        "error_message": (
                            "Recovered inconsistent Task/Instance ownership "
                            f"(reverse owners: {sorted(task_reverse_owners)}); "
                            "automatic replay was blocked"
                        ),
                    }
                    logger.error(
                        "Failing unowned task %s because ownership evidence is "
                        "inconsistent: task owner %s, reverse owners %s",
                        t.id,
                        t.instance_id,
                        sorted(task_reverse_owners),
                    )
                else:
                    new_status = "pending"
                    values = {
                        "status": "pending",
                        "instance_id": None,
                        "started_at": None,
                        "completed_at": None,
                        "error_message": "Recovered unowned execution claim",
                    }
                    logger.warning(
                        "Releasing unowned task %s from %s back to pending",
                        t.id, t.status,
                    )
                release_predicates = [
                    Task.id == t.id,
                    Task.status == t.status,
                    Task.retry_count == t.retry_count,
                    (
                        Task.instance_id.is_(None)
                        if t.instance_id is None
                        else Task.instance_id == t.instance_id
                    ),
                    (
                        Task.started_at.is_(None)
                        if t.started_at is None
                        else Task.started_at == t.started_at
                    ),
                    (
                        Task.completed_at.is_(None)
                        if t.completed_at is None
                        else Task.completed_at == t.completed_at
                    ),
                    (
                        Task.session_id.is_(None)
                        if t.session_id is None
                        else Task.session_id == t.session_id
                    ),
                    Task.worker_id.is_(None),
                    Task.shared_from_id.is_(None),
                ]
                if new_status == "pending":
                    release_predicates.append(
                        task_retry_not_superseded_predicate()
                    )
                released = await db.execute(
                    update(Task)
                    .where(*release_predicates)
                    .values(**values)
                )
                if released.rowcount:
                    resulting_generation = (
                        await self._read_task_status_generation(db, t.id)
                    )
                    if resulting_generation is not None:
                        reset_tasks.append(resulting_generation)

            # Older shutdown/retry paths could clear a Task back to pending
            # before proving its orphan process dead.  Quarantine that dirty
            # state regardless of the task's current queue status so startup
            # cannot dispatch a second writer.
            for task_id, (instance_id, unmanaged_pid) in (
                unmanaged_live_owners.items()
            ):
                pending_owner = await db.get(
                    Task, task_id, populate_existing=True
                )
                if (
                    pending_owner is None
                    or pending_owner.instance_id not in (None, instance_id)
                ):
                    # A concurrent retry may already have claimed a different
                    # slot.  Never use that refreshed owner as permission to
                    # overwrite it with this stale reverse Instance link.
                    continue
                quarantined = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task_id,
                        Task.status == "pending",
                        Task.retry_count == pending_owner.retry_count,
                        (
                            Task.instance_id.is_(None)
                            if pending_owner.instance_id is None
                            else Task.instance_id == pending_owner.instance_id
                        ),
                        (
                            Task.started_at.is_(None)
                            if pending_owner.started_at is None
                            else Task.started_at == pending_owner.started_at
                        ),
                        (
                            Task.completed_at.is_(None)
                            if pending_owner.completed_at is None
                            else Task.completed_at == pending_owner.completed_at
                        ),
                        (
                            Task.session_id.is_(None)
                            if pending_owner.session_id is None
                            else Task.session_id == pending_owner.session_id
                        ),
                        Task.worker_id.is_(None),
                        Task.shared_from_id.is_(None),
                    )
                    .values(
                        status="failed",
                        instance_id=instance_id,
                        completed_at=datetime.utcnow(),
                        error_message=(
                            f"Unmanaged process PID {unmanaged_pid} may still "
                            "be running after manager restart; automatic retry "
                            "was blocked to prevent duplicate execution"
                        ),
                    )
                )
                if quarantined.rowcount:
                    resulting_generation = (
                        await self._read_task_status_generation(db, task_id)
                    )
                    if resulting_generation is not None:
                        reset_tasks.append(resulting_generation)

            if reconcile_auxiliary:
                from backend.models.monitor_session import MonitorSession
                (
                    active_monitor_ids,
                    active_sub_agent_ids,
                ) = self._active_auxiliary_session_ids()
                result = await db.execute(
                    select(MonitorSession).where(
                        MonitorSession.status == "running"
                    )
                )
                for ms in result.scalars().all():
                    # Remote mirror rows are owned by their source CCM.
                    if ms.remote_id is not None:
                        continue
                    if ms.source == "native":
                        # Historical native monitor rows may use the generic
                        # ``monitor`` agent_type. Source is authoritative.
                        manager_owned = ms.task_id in live_task_ids
                    elif ms.agent_type == "monitor":
                        manager_owned = ms.id in active_monitor_ids
                    elif ms.agent_type == "sub_agent":
                        manager_owned = ms.id in active_sub_agent_ids
                    elif ms.agent_type in {
                        "native-agent",
                        "native-monitor",
                    }:
                        # Native children live inside the parent CLI
                        # generation, represented by exact InstanceManager
                        # evidence in ``live_task_ids``.
                        manager_owned = ms.task_id in live_task_ids
                    else:
                        # Unknown future CCM auxiliary types can use either
                        # lifecycle registry; fail closed while exact evidence
                        # remains.
                        manager_owned = (
                            ms.id in active_monitor_ids
                            or ms.id in active_sub_agent_ids
                        )
                    if manager_owned:
                        continue
                    logger.warning(
                        "Cleaning up stale auxiliary session %s", ms.id
                    )
                    ms.status = "failed"
                    ms.completed_at = datetime.utcnow()

            await db.commit()

        # 防御性广播：lifespan 启动路径此刻还没有 WS 订阅者（重连前端靠重连后
        # 轮询自愈），但 dispatcher 也可能经 API 端点手动 start——那时有观众
        for resulting_generation in reset_tasks:
            await self._broadcast_task_status_generation(
                resulting_generation
            )

    async def stop(
        self,
        *,
        timeout: float = DISPATCHER_BACKGROUND_STOP_TIMEOUT,
    ):
        """Pause new admission without interrupting turns already in flight.

        Process termination is an InstanceManager/application-shutdown concern.
        Keeping lifecycle and per-task queue consumers alive makes the runtime
        toggle a true pause: current work finishes normally while no fresh
        TaskQueue claims are made.
        """

        self._running = False
        failures: list[str] = []
        if self._dispatch_task and not self._dispatch_task.done():
            self._dispatch_task.cancel()
            _, pending = await asyncio.wait(
                {self._dispatch_task}, timeout=timeout
            )
            if pending:
                failures.append("dispatch loop ignored cancellation")
            else:
                await asyncio.gather(
                    self._dispatch_task, return_exceptions=True
                )
        curator = getattr(self, "_curator_task", None)
        if curator and not curator.done():
            curator.cancel()
            _, pending = await asyncio.wait({curator}, timeout=timeout)
            if pending:
                failures.append("curator loop ignored cancellation")
            else:
                await asyncio.gather(curator, return_exceptions=True)
        if failures:
            # Keep the exact task attributes intact so shutdown/admin retry can
            # observe them.  A runtime pause must not report success while an
            # admission producer is still live.
            raise RuntimeError(
                "GlobalDispatcher background stop incomplete: "
                + "; ".join(failures)
            )
        logger.info("GlobalDispatcher paused (in-flight work preserved)")

    async def shutdown(self) -> None:
        """Quiesce every producer, then reap all InstanceManager generations.

        This is intentionally distinct from the UI/runtime ``stop`` pause.
        Admission is closed first, including future chat enqueue, so the final
        manager snapshot cannot miss a launch created after it was taken.
        """

        self._shutting_down = True
        shutdown_failures: list[str] = []
        try:
            await self.stop()
        except Exception as exc:
            shutdown_failures.append(
                f"dispatcher producer stop failed: {exc!r}"
            )
            logger.exception(
                "Dispatcher producer stop failed; continuing exact reapers"
            )

        # Queue workers are independent from the fresh-task dispatch loop and
        # may already hold a message removed by q.get().  Abort and await them
        # before taking the final InstanceManager generation snapshot.
        for task_id in list(self._task_queue_workers):
            try:
                await self.abort_task_queue(task_id)
            except Exception as exc:
                shutdown_failures.append(
                    f"task {task_id} queue cleanup failed: {exc!r}"
                )
                logger.error(
                    "Task queue cleanup failed during shutdown for task %s",
                    task_id,
                    exc_info=True,
                )

        # CCM monitor/sub-agent lifecycles are not InstanceManager generations,
        # but they still own real process groups.  Stop and await them before
        # the manager snapshot so shutdown cannot leave invisible children.
        aux_stops = [
            *(self.stop_monitor_session_process(session_id)
              for session_id in set(self._monitor_tasks) | set(self._monitor_processes)),
            *(self.stop_sub_agent_session_process(session_id)
              for session_id in (
                  set(self._sub_agent_tasks)
                  | set(self._sub_agent_processes)
                  | set(self._sub_agent_codex_processes)
                  | set(self._sub_agent_codex_threads)
              )),
        ]
        if aux_stops:
            results = await asyncio.gather(*aux_stops, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    shutdown_failures.append(
                        f"auxiliary process cleanup failed: {result!r}"
                    )
                    logger.error(
                        "Failed to reap auxiliary process during shutdown: %r",
                        result,
                    )

        fresh_instance_ids = {
            instance_id
            for instance_id, task in self._running_tasks.items()
            if isinstance(instance_id, int) and not task.done()
        }
        lifecycle_tasks = [
            task for task in self._running_tasks.values() if not task.done()
        ]
        for task in lifecycle_tasks:
            task.cancel()
        pending_lifecycle_tasks: set[asyncio.Task] = set()
        if lifecycle_tasks:
            done, pending_lifecycle_tasks = await asyncio.wait(
                set(lifecycle_tasks),
                timeout=SHUTDOWN_LIFECYCLE_CANCEL_TIMEOUT,
            )
            if done:
                await asyncio.gather(*done, return_exceptions=True)
            if pending_lifecycle_tasks:
                pending_keys = [
                    str(key)
                    for key, task in self._running_tasks.items()
                    if task in pending_lifecycle_tasks
                ]
                shutdown_failures.append(
                    "lifecycle tasks ignored cancellation: "
                    + ", ".join(pending_keys)
                )
                logger.error(
                    "Lifecycle task(s) ignored shutdown cancellation: %s",
                    ", ".join(pending_keys),
                )
        unsettled_lifecycle_instance_ids = {
            instance_id
            for instance_id, lifecycle in self._running_tasks.items()
            if (
                isinstance(instance_id, int)
                and lifecycle in pending_lifecycle_tasks
            )
        }
        try:
            from backend.services.goal_evaluator import (
                reap_unreaped_goal_evaluators,
            )

            await reap_unreaped_goal_evaluators()
        except Exception as exc:
            shutdown_failures.append(
                f"goal evaluator cleanup failed: {exc!r}"
            )
            logger.exception(
                "Failed to reap retained goal evaluator during shutdown"
            )

        managed_instance_ids = {
            instance_id
            for instance_id in (
                set(self.instance_manager.processes)
                | set(getattr(self.instance_manager, "_tasks", {}))
                | set(getattr(self.instance_manager, "_consumer_records", {}))
                | set(getattr(self.instance_manager, "_process_groups", {}))
                | set(
                    getattr(
                        self.instance_manager,
                        "_container_exec_processes",
                        {},
                    )
                )
            )
            if isinstance(instance_id, int)
        }
        # Snapshot exact in-memory generations before consulting persisted
        # Instance rows.  A missing/corrupt/raced DB row must not prevent
        # shutdown from killing a child process that this manager can identify.
        managed_processes: dict[int, asyncio.subprocess.Process] = {}
        for instance_id in managed_instance_ids:
            records = getattr(self.instance_manager, "_consumer_records", {})
            record = records.get(instance_id) if isinstance(records, dict) else None
            candidates = (
                self.instance_manager.processes.get(instance_id),
                getattr(self.instance_manager, "_process_groups", {}).get(
                    instance_id
                ),
                getattr(
                    self.instance_manager,
                    "_container_exec_processes",
                    {},
                ).get(instance_id),
                getattr(record, "process", None),
            )
            exact_process = next(
                (candidate for candidate in candidates if candidate is not None),
                None,
            )
            if exact_process is not None:
                managed_processes[instance_id] = exact_process
        failed_reaps: set[int] = set()
        for instance_id in managed_instance_ids:
            records = getattr(self.instance_manager, "_consumer_records", {})
            record = records.get(instance_id) if isinstance(records, dict) else None
            task_status = (
                "completed"
                if record is not None
                and getattr(record, "chat_initiated", False)
                else "pending"
            )
            stop_failed = False
            try:
                async with self.db_factory() as db:
                    instance = await db.get(Instance, instance_id)
                    expected_task_id = (
                        instance.current_task_id if instance is not None else None
                    )
                    expected_pid = instance.pid if instance is not None else None
                    expected_started_at = (
                        instance.started_at if instance is not None else None
                    )
                stopped = await self.instance_manager.stop(
                    instance_id,
                    expected_task_id=expected_task_id,
                    expected_pid=expected_pid,
                    expected_started_at=expected_started_at,
                    task_status=task_status,
                    terminal_consumer_timeout=(
                        SHUTDOWN_TERMINAL_CONSUMER_TIMEOUT
                    ),
                    consumer_cancel_timeout=SHUTDOWN_CONSUMER_CANCEL_TIMEOUT,
                )
                if not stopped and self.instance_manager.is_running(instance_id):
                    stop_failed = True
            except Exception:
                stop_failed = True
                logger.exception(
                    "Failed to reap instance %s during dispatcher shutdown",
                    instance_id,
                )

            fallback_stop_token = False
            if stop_failed:
                # The DB-fenced stop has released its own token.  Keep launch
                # admission closed while exact-handle fallback kills/cancels the
                # old generation, otherwise its terminal consumer could race an
                # in-place retry during this window.
                self.instance_manager._begin_stopping(instance_id)
                fallback_stop_token = True

            exact_process = managed_processes.get(instance_id)
            exact_reaped = (
                exact_process is None
                or self.instance_manager._generation_reap_confirmed(
                    instance_id, exact_process
                )
            )
            if (
                exact_process is not None
                and not exact_reaped
                and not fallback_stop_token
            ):
                self.instance_manager._begin_stopping(instance_id)
                fallback_stop_token = True
            if exact_process is not None and not exact_reaped:
                # The high-level stop is DB-owner fenced and can correctly lose
                # to a concurrent persisted generation.  Shutdown still owns
                # this exact Process object: kill only that generation without
                # mutating the newer DB owner.
                try:
                    killed = await self.instance_manager.kill_process_generation(
                        instance_id,
                        exact_process,
                        timeout=SHUTDOWN_CONSUMER_CANCEL_TIMEOUT,
                    )
                    exact_reaped = bool(killed) and (
                        self.instance_manager._generation_reap_confirmed(
                            instance_id, exact_process
                        )
                    )
                except Exception:
                    logger.exception(
                        "Exact process fallback failed for instance %s during "
                        "dispatcher shutdown",
                        instance_id,
                    )
                    exact_reaped = False

            # A DB-fenced stop may fail after the exact child is dead while its
            # output consumer is still unwinding.  Bound that task too; never
            # let application shutdown silently abandon an exact consumer.
            records = getattr(self.instance_manager, "_consumer_records", {})
            current_record = (
                records.get(instance_id) if isinstance(records, dict) else None
            )
            exact_consumer = (
                getattr(current_record, "task", None)
                if current_record is not None
                and getattr(current_record, "process", None) is exact_process
                else None
            )
            if (
                exact_consumer is not None
                and not exact_consumer.done()
                and exact_reaped
            ):
                exact_consumer.cancel()
                done, _ = await asyncio.wait(
                    {exact_consumer},
                    timeout=SHUTDOWN_CONSUMER_CANCEL_TIMEOUT,
                )
                if not done:
                    exact_reaped = False
                    logger.error(
                        "Exact output consumer for instance %s ignored shutdown "
                        "cancellation",
                        instance_id,
                    )

            if stop_failed:
                # Preserve any durable owner from the failed high-level stop;
                # startup reconciliation will release it only after PID death
                # is definitive.
                failed_reaps.add(instance_id)
            if not exact_reaped:
                shutdown_failures.append(
                    f"instance {instance_id} exact process generation survived"
                )
            if fallback_stop_token:
                self.instance_manager._end_stopping(instance_id)

        # A fresh lifecycle cancelled before spawning has no manager generation
        # for stop() to release.  Return only those proven process-free claims;
        # a failed reap keeps its active Task owner fail-closed.
        for instance_id in (
            fresh_instance_ids
            - failed_reaps
            - unsettled_lifecycle_instance_ids
        ):
            if self.instance_manager.is_running(instance_id):
                continue
            async with self.db_factory() as db:
                task_id = await db.scalar(
                    select(Task.id).where(
                        Task.instance_id == instance_id,
                        Task.status.in_(["in_progress", "executing"]),
                        Task.worker_id.is_(None),
                    )
                )
                if task_id is not None:
                    await TaskQueue(db).defer(
                        task_id,
                        "dispatcher shutdown before process launch",
                        instance_id=instance_id,
                    )

        # Forget only settled lifecycle registrations.  Pending tasks and
        # launch reservations are exact evidence and must survive a failed
        # shutdown so an operator/retry can still find them.
        for key, lifecycle in list(self._running_tasks.items()):
            if lifecycle.done():
                self._running_tasks.pop(key, None)
        if not shutdown_failures:
            async with self._instance_claim_lock:
                self._launching_instances.clear()
                self._instance_claim_owners.clear()
        if shutdown_failures:
            # Auxiliary processes are independent POSIX sessions.  Returning
            # success here would discard the only in-memory process evidence as
            # the application exits.  Keep their maps intact and make the
            # incomplete shutdown explicit to the lifespan owner.
            raise RuntimeError(
                "GlobalDispatcher shutdown could not prove all process groups "
                f"terminal: {'; '.join(shutdown_failures)}"
            )
        logger.info("GlobalDispatcher shutdown complete")

    def status(self) -> dict:
        return {
            "running": self._running,
            "paused": self._dispatch_paused,
            "active_tasks": {
                iid: not t.done() for iid, t in self._running_tasks.items()
            },
        }

    def _active_local_instance_ids(self) -> set[int]:
        """Return only live *local* Instance keys used for admission.

        ``_running_tasks`` also holds distributed Worker forwarding tasks under
        string keys.  Passing those strings to ``Instance.id.notin_(...)`` is
        tolerated by SQLite but fails PostgreSQL's integer binder.
        """

        return {
            instance_id
            for instance_id, task in self._running_tasks.items()
            if type(instance_id) is int and not task.done()
        }

    def _active_auxiliary_session_ids(self) -> tuple[set[int], set[int]]:
        """Snapshot exact in-process CCM auxiliary lifecycle evidence.

        A retained process-map entry remains blocking even if its parent task
        has completed: the entry is removed only after exact process-group
        reaping proves that generation terminal.
        """

        monitor_ids = {
            session_id
            for session_id, task in self._monitor_tasks.items()
            if type(session_id) is int and not task.done()
        }
        monitor_ids.update(
            session_id
            for session_id in self._monitor_processes
            if type(session_id) is int
        )
        sub_agent_ids = {
            session_id
            for session_id, task in self._sub_agent_tasks.items()
            if type(session_id) is int and not task.done()
        }
        sub_agent_ids.update(
            session_id
            for session_id in self._sub_agent_processes
            if type(session_id) is int
        )
        sub_agent_ids.update(
            session_id
            for session_id in self._sub_agent_codex_processes
            if type(session_id) is int
        )
        sub_agent_ids.update(
            session_id
            for session_id in self._sub_agent_codex_threads
            if type(session_id) is int
        )
        return monitor_ids, sub_agent_ids

    def active_auxiliary_blockers(self) -> list[dict[str, object]]:
        """Return live CCM-owned auxiliary generations that a restart kills."""

        monitor_ids, sub_agent_ids = self._active_auxiliary_session_ids()
        return [
            *(
                {
                    "id": session_id,
                    "title": f"监控子 Agent #{session_id}",
                    "status": "running_auxiliary",
                    "kind": "monitor",
                }
                for session_id in sorted(monitor_ids)
            ),
            *(
                {
                    "id": session_id,
                    "title": f"子 Agent #{session_id}",
                    "status": "running_auxiliary",
                    "kind": "sub_agent",
                }
                for session_id in sorted(sub_agent_ids)
            ),
        ]

    def _remove_running_task_if_same(
        self,
        key: int | str,
        finished: asyncio.Task,
    ) -> None:
        """Do not let an old done callback erase a replacement generation."""

        if self._running_tasks.get(key) is finished:
            self._running_tasks.pop(key, None)

    async def _ensure_instances(self):
        """Create workers until live capacity reaches max_concurrent_instances.

        Error/stopped rows are terminal history and do not own a process, so
        they must not consume the concurrency cap.  They remain available for
        inspection until the instance cleanup endpoint removes them.
        """
        async with instance_capacity_lock:
            async with self.db_factory() as db:
                result = await db.execute(select(Instance))
                existing = list(result.scalars().all())
                live_count = sum(
                    1
                    for instance in existing
                    if instance_occupies_slot(instance)
                )
                needed = settings.max_concurrent_instances - live_count
                if needed <= 0:
                    return
                base = 0
                for inst in existing:
                    match = re.match(r"worker-(\d+)$", inst.name or "")
                    if match:
                        base = max(base, int(match.group(1)))
                for i in range(needed):
                    name = f"worker-{base + i + 1}"
                    instance = Instance(name=name)
                    db.add(instance)
                await db.commit()
        logger.info(f"Created {needed} worker instances")

    async def _ensure_min_idle_instances(self):
        """Auto top-up: keep at least min_idle_instances idle workers available.

        Named worker-<N> continuing from the highest existing numeric suffix,
        so deletions never produce duplicate names.
        """
        if settings.min_idle_instances <= 0:
            return
        async with instance_capacity_lock:
            async with self.db_factory() as db:
                # Re-read under the shared API/dispatcher capacity lock.  A
                # count made before acquiring it can over-create after a
                # concurrent POST /instances commits.
                result = await db.execute(select(Instance))
                existing = list(result.scalars().all())
                live_count = sum(
                    1 for instance in existing
                    if instance_occupies_slot(instance)
                )
                idle_count = sum(
                    1 for instance in existing
                    if instance_is_reusable_idle(instance)
                )
                needed = settings.min_idle_instances - idle_count
                if needed <= 0:
                    return
                # Terminal error/stopped rows hold no process and must not consume
                # the live concurrency cap.
                cap = settings.max_concurrent_instances
                if cap > 0 and live_count + needed > cap:
                    needed = max(0, cap - live_count)
                if needed <= 0:
                    return
                base = 0
                for inst in existing:
                    m = re.match(r"worker-(\d+)$", inst.name or "")
                    if m:
                        base = max(base, int(m.group(1)))
                for i in range(needed):
                    db.add(Instance(name=f"worker-{base + i + 1}"))
                await db.commit()
        logger.info(
            f"Auto-added {needed} worker instances "
            f"(idle was {idle_count}, min_idle_instances={settings.min_idle_instances})"
        )

    def _resolve_timeout(self, task) -> float | None:
        """任务有效超时（秒）。None = 不限时。

        task.timeout_hours: NULL = 全局默认（settings.task_timeout_seconds），
        0 = 不限时，>0 = 指定小时数。
        """
        th = getattr(task, "timeout_hours", None)
        if th is not None:
            return th * 3600 if th > 0 else None
        return settings.task_timeout_seconds

    async def _wait_process(
        self,
        process,
        task,
        label: str,
        *,
        instance_id: int,
    ) -> None:
        """Wait for one exact managed generation with the task timeout."""
        timeout = self._resolve_timeout(task)
        if timeout:
            try:
                await asyncio.wait_for(process.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning(
                    "%s (task %s) timed out after %.0fs, killing exact process group",
                    label,
                    task.id,
                    timeout,
                )
                killed = await self.instance_manager.kill_process_generation(
                    instance_id,
                    process,
                )
                if not killed:
                    raise RuntimeError(
                        f"Timed-out process generation changed for instance {instance_id}"
                    )
        else:
            await process.wait()

    async def _wait_output_consumer(
        self, instance_id: int, task: Task, label: str, process=None
    ) -> None:
        """Wait for post-process output/account bookkeeping.

        ``InstanceManager`` deliberately gives Codex an unbounded wait because
        its consumer may still be migrating and rebinding the native rollout.
        Claude retains the historical 30-second bound.
        """

        try:
            await self.instance_manager.wait_for_output_consumer(
                instance_id,
                provider=task.provider,
                timeout=30,
                expected_process=process,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Output consumer did not finish after %s for task %s",
                label,
                task.id,
            )

    def _effective_process_exit_code(self, instance_id: int, process) -> int:
        """Return the exact turn's provider-semantic exit code.

        A direct CLI can exit zero while its structured result reports an API
        failure.  InstanceManager records that distinction after consuming all
        output; lifecycle callers must use it instead of the OS return code.
        """

        if process is None:
            return -1
        resolver = getattr(self.instance_manager, "effective_exit_code", None)
        if callable(resolver):
            value = resolver(instance_id, process)
            if isinstance(value, int):
                return value
        returncode = getattr(process, "returncode", None)
        return returncode if isinstance(returncode, int) else -1

    async def _curator_loop(self):
        """Background curator: periodic skill lifecycle management.

        Checks every hour; only runs when:
          - >= 7 days since last run
          - No executing tasks (system is idle)
          - Project has enough history
        Reference: Hermes Curator scheduling + MiMo project age check.
        """
        _last_curator_run: datetime | None = None
        while self._running:
            try:
                await asyncio.sleep(3600)  # Check every hour
                if not self._running:
                    break

                now = datetime.utcnow()

                # First run: seed timestamp, defer by one full interval (Hermes pattern)
                if _last_curator_run is None:
                    _last_curator_run = now
                    continue

                # Check interval
                hours_since = (now - _last_curator_run).total_seconds() / 3600
                if hours_since < 168:  # 7 days
                    continue

                # Check if system is idle (no executing tasks)
                async with self.db_factory() as db:
                    from backend.models.task import Task
                    executing = (await db.execute(
                        select(func.count()).select_from(Task)
                        .where(Task.status.in_(["executing", "in_progress"]))
                    )).scalar() or 0
                if executing > 0:
                    continue

                # Run curator (every 7 days)
                logger.info("curator: starting periodic run")
                async with self.db_factory() as db:
                    from backend.services.skill_curator import run_curator
                    summary = await run_curator(db)
                    logger.info("curator: checked %d skills, %d stale",
                                summary["checked"], len(summary["stale"]))

                # Run distill (every 30 days — check if 30 days since last distill)
                if hours_since >= 720:  # 30 days
                    try:
                        logger.info("distill: starting periodic analysis")
                        async with self.db_factory() as db:
                            from backend.services.skill_distill import analyze_patterns
                            result = await analyze_patterns(db)
                            logger.info("distill: %s", result.get("summary", ""))
                    except Exception:
                        logger.exception("distill: periodic analysis failed")

                _last_curator_run = now

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("curator: error in curator loop")

    async def _dispatch_loop(self):
        """Dispatch pending tasks, event-driven with a low-frequency poll fallback."""
        while self._running:
            try:
                self._dispatch_wakeup.clear()
                if self._dispatch_paused:
                    try:
                        await asyncio.wait_for(self._dispatch_wakeup.wait(), timeout=2)
                    except asyncio.TimeoutError:
                        pass
                    continue
                # Top up idle workers before looking for capacity
                await self._ensure_min_idle_instances()

                # 路径 1：分布式 Worker task —— 不消耗本地 instance，直接转发
                try:
                    async with self.task_start_guard():
                        await self._dispatch_worker_tasks()
                except TaskStartPausedError:
                    pass

                # Fill available local slots.  Reservation and task claim are
                # deliberately coupled: a task is stamped with its active
                # instance_id by the same CAS that moves it out of pending.
                while self._running:
                    instance = None
                    claim_token = None
                    task = None
                    lifecycle_registered = False
                    try:
                        # The durable pending -> in_progress transition is the
                        # maintenance admission commit point.
                        async with self.task_start_guard():
                            async with self.db_factory() as db:
                                instance, claim_token = (
                                    await self._reserve_idle_instance(db)
                                )
                                if instance is None or claim_token is None:
                                    break
                                queue = TaskQueue(db)
                                now = time.monotonic()
                                self._account_routing_not_before = {
                                    task_id: deadline
                                    for task_id, deadline
                                    in self._account_routing_not_before.items()
                                    if deadline > now
                                }
                                task = await queue.dequeue(
                                    exclude_ids=set(
                                        self._account_routing_not_before
                                    ),
                                    instance_id=instance.id,
                                )

                        if task is None:
                            break

                        # Resolve project -> target_repo + git config
                        merged: dict = {}
                        if task.project_id:
                            async with self.db_factory() as db:
                                project = await db.get(Project, task.project_id)
                                global_cfg = await db.get(GlobalSettings, 1)
                                if project:
                                    if project.local_path and not task.target_repo:
                                        await db.execute(
                                            update(Task)
                                            .where(
                                                Task.id == task.id,
                                                Task.status == "in_progress",
                                                Task.instance_id == instance.id,
                                            )
                                            .values(target_repo=project.local_path)
                                        )
                                        await db.commit()
                                        task.target_repo = project.local_path
                                    merged = merge_git_config(
                                        settings_to_dict(project),
                                        settings_to_dict(global_cfg),
                                    )
                        git_env = _build_git_env(merged)

                        logger.info(
                            "Dispatching task %s (%s) to instance %s (%s)",
                            task.id, task.title, instance.id, instance.name,
                        )
                        lifecycle = asyncio.create_task(
                            self._run_task_lifecycle(instance.id, task, git_env)
                        )
                        self._running_tasks[instance.id] = lifecycle
                        lifecycle_registered = True
                    except TaskStartPausedError:
                        break
                    except asyncio.CancelledError:
                        if task is not None and instance is not None:
                            async with self.db_factory() as db:
                                await TaskQueue(db).defer(
                                    task.id,
                                    "dispatcher stopped before launch",
                                    instance_id=instance.id,
                                )
                        raise
                    except Exception as exc:
                        logger.exception("Failed to prepare a claimed task for launch")
                        if task is not None and instance is not None:
                            async with self.db_factory() as db:
                                deferred = await TaskQueue(db).defer(
                                    task.id,
                                    f"launch preparation failed: {exc}"[:500],
                                    instance_id=instance.id,
                                )
                            if deferred:
                                from backend.services.task_events import (
                                    broadcast_status_change,
                                )
                                await broadcast_status_change(
                                    task.id, "pending", instance.id
                                )
                    finally:
                        # Once registered, _running_tasks is the admission
                        # guard.  Otherwise this releases a failed/no-task
                        # reservation so another caller can use the slot.
                        if instance is not None and claim_token is not None:
                            await self._release_instance_reservation(
                                instance.id, claim_token
                            )

                    if not lifecycle_registered:
                        # Preparation errors are usually systemic; let the
                        # outer wake/poll cadence retry instead of spinning.
                        break

                try:
                    await asyncio.wait_for(self._dispatch_wakeup.wait(), timeout=2)
                except asyncio.TimeoutError:
                    pass

            except asyncio.CancelledError:
                await self._release_owned_instance_reservations(
                    asyncio.current_task()
                )
                break
            except Exception as e:
                await self._release_owned_instance_reservations(
                    asyncio.current_task()
                )
                logger.error(f"Dispatch loop error: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def _dispatch_worker_tasks(self):
        """转发 pending 的 worker task（elastic-worker 设计 §5.3）。

        取出后立即标 in_progress，防止 2 秒后重复转发；转发失败回 failed。
        """
        from backend.main import worker_proxy
        if worker_proxy is None:
            return
        from backend.models.worker import Worker as WorkerModel

        async with self.db_factory() as db:
            result = await db.execute(
                select(Task).where(
                    Task.status == "pending",
                    Task.worker_id.isnot(None),
                    Task.shared_from_id.is_(None),
                    task_retry_not_superseded_predicate(),
                )
            )
            worker_tasks = list(result.scalars().all())

        for task in worker_tasks:
            pending_generation = self._task_status_generation(task)
            if (
                pending_generation.worker_id is None
                or pending_generation.shared_from_id is not None
            ):
                continue
            async with self.db_factory() as db:
                worker = await db.get(
                    WorkerModel,
                    pending_generation.worker_id,
                )
            if not worker or worker.status != "ready":
                continue  # worker 没就绪，留在 pending 等下轮
            # Check worker concurrency limit
            async with self.db_factory() as db:
                running_on_worker = (await db.execute(
                    select(func.count(Task.id)).where(
                        Task.worker_id == worker.id,
                        Task.status.in_(["in_progress", "executing"]),
                    )
                )).scalar() or 0
            if running_on_worker >= worker.max_tasks:
                continue  # worker 已满，留在 pending 等下轮
            # 与本地路径一致：把 project.local_path 写进 target_repo——
            # 否则迁回本机后 chat 解析不出 cwd（实测 task 58 教训）
            if task.project_id and not task.target_repo:
                async with self.db_factory() as db:
                    project = await db.get(Project, task.project_id)
                    if project and project.local_path:
                        target_updated = await db.execute(
                            update(Task)
                            .where(
                                *self._task_status_generation_predicates(
                                    pending_generation
                                ),
                                Task.project_id == task.project_id,
                                (
                                    Task.target_repo.is_(None)
                                    if task.target_repo is None
                                    else Task.target_repo == task.target_repo
                                ),
                            )
                            .values(target_repo=project.local_path)
                        )
                        if not target_updated.rowcount:
                            await db.rollback()
                            continue
                        await db.commit()
                        task.target_repo = project.local_path
            async with self.db_factory() as db:
                claimed = await db.execute(
                    update(Task)
                    .where(
                        *self._task_status_generation_predicates(
                            pending_generation
                        ),
                        Task.worker_id == worker.id,
                        Task.shared_from_id.is_(None),
                        task_retry_not_superseded_predicate(),
                    )
                    .values(status="in_progress", started_at=datetime.utcnow())
                )
                claimed_generation = None
                claimed_task = None
                if claimed.rowcount:
                    claimed_task = await db.get(
                        Task,
                        task.id,
                        populate_existing=True,
                    )
                    if claimed_task is not None:
                        claimed_generation = self._task_status_generation(
                            claimed_task
                        )
                await db.commit()
            if (
                not claimed.rowcount
                or claimed_generation is None
                or claimed_task is None
            ):
                continue
            # 与本地 task 一致地广播，前端立即看到状态（不等 relay 回传）。
            # Publication is fenced to the exact Worker assignment/generation:
            # a migration or retry that wins after the claim must not be
            # followed by this stale ``in_progress`` event.
            await self._broadcast_task_status_generation(
                claimed_generation,
                extra={"old_status": "pending"},
            )
            t = asyncio.create_task(
                self._safe_forward_to_worker(
                    claimed_task,
                    claimed_generation,
                )
            )
            key = f"worker-{task.id}"
            self._running_tasks[key] = t  # 强引用防 GC
            t.add_done_callback(
                lambda finished, k=key: self._remove_running_task_if_same(
                    k,
                    finished,
                )
            )

    async def _safe_forward_to_worker(
        self,
        task: Task,
        claimed_generation: _TaskStatusGeneration,
    ):
        from backend.main import worker_proxy
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await worker_proxy.forward_task_to_worker(task)
                return
            except Exception as e:
                if attempt < max_retries - 1:
                    delay = 2 ** attempt
                    logger.warning("forward task %s to worker failed (attempt %d/%d), retry in %ds: %s",
                                   task.id, attempt + 1, max_retries, delay, e)
                    await asyncio.sleep(delay)
                else:
                    logger.error("forward task %s to worker failed after %d attempts: %s",
                                 task.id, max_retries, e)
                    resulting_generation = None
                    async with self.db_factory() as db:
                        failed = await db.execute(
                            update(Task)
                            .where(
                                *self._task_status_generation_predicates(
                                    claimed_generation
                                ),
                                Task.worker_id == task.worker_id,
                            )
                            .values(
                                status="failed",
                                completed_at=datetime.utcnow(),
                                error_message=(
                                    "转发到 Worker 失败 "
                                    f"({max_retries} 次重试): {e}"
                                ),
                            )
                        )
                        if failed.rowcount:
                            resulting_generation = (
                                await self._read_task_status_generation(
                                    db, task.id
                                )
                            )
                        await db.commit()
                    if resulting_generation is not None:
                        await self._broadcast_task_status_generation(
                            resulting_generation,
                            extra={"old_status": "in_progress"},
                        )

    async def _pool_select(
        self,
        exclude: set[str] | None = None,
        *,
        model: str | None = None,
    ) -> str | None:
        """Select a pool account config_dir, or None if pool is off / exhausted."""
        if not self.pool:
            return None
        # validate=True probes accounts with a blocking subprocess (up to 30s
        # each) — must run off the event loop
        selected = await self.pool.select_async(
            exclude=exclude,
            validate=True,
            model=model,
        )
        if selected is None:
            detail = (
                "no enabled account supports the requested model"
                if not self.pool.has_compatible_enabled_account(model)
                else "all compatible pool accounts are currently unavailable"
            )
            raise RuntimeError(
                f"Claude pool has {detail} {model!r}; refusing to run an "
                "auxiliary process with inherited service credentials"
            )
        return selected

    def _sanitize_cloudrouter_claude_env(
        self, env: dict[str, str], config_dir: str | None
    ) -> None:
        """Remove inherited credentials that override API-key helper auth."""

        if self.cloudrouter_store is None or not config_dir:
            return
        try:
            account = self.cloudrouter_store.account_for_claude_config_dir(
                config_dir
            )
        except Exception:
            logger.exception(
                "Could not resolve auxiliary Claude account home %s",
                config_dir,
            )
            return
        if account is None:
            return
        for key in (
            "ANTHROPIC_AUTH_TOKEN",
            "ANTHROPIC_API_KEY",
            "CLAUDE_CODE_OAUTH_TOKEN",
        ):
            env.pop(key, None)

    async def _resolve_resume_config_dir(
        self,
        session_id: str | None,
        provider: str | None = "claude",
        *,
        task_id: int | None = None,
        expected_generation: _TaskRoutingGeneration | None = None,
        model: str | None = None,
    ) -> str | None:
        """Resolve the provider account home for a (possibly resuming) launch.

        Claude returns ``CLAUDE_CONFIG_DIR`` and Codex returns ``CODEX_HOME``.
        Both persist the selected account on the Task so retained hardlink/
        rollout copies do not make future resumes ambiguous.

        Explicit preferred selection has the highest priority and safely
        migrates an existing session on its next turn. Without an explicit
        choice, a healthy resident session remains sticky so its native context
        and hot transport are preserved; fresh launches prefer a compatible,
        available CloudRouter API projection and then fall back to the native
        pool policy.

        The critical case is when the pool can hand out **no** healthy account
        (every account rate-limited): we must NOT let the launch fall through to
        an arbitrary inherited ``CLAUDE_CONFIG_DIR`` — that account won't hold
        the session JSONL and ``claude --resume`` dies with "No conversation
        found with session ID", which hard-fails the task and loses the session
        (prod tasks #734/#740). Instead anchor the resume to whichever account
        dir actually holds the session; if that account is rate-limited it
        surfaces as a recoverable rate-limit/transient event the existing retry
        paths handle, rather than a fatal lookup miss.

        Returns a config_dir, or None when there is no pool (default account)
        or no session to anchor a fallback to.
        """
        await self._require_task_lifecycle_active(expected_generation)
        if (provider or "claude").lower() == "codex":
            return await self._resolve_codex_home(
                session_id,
                task_id=task_id,
                expected_generation=expected_generation,
                model=model,
            )
        if not (self.pool and self.pool.enabled):
            return None

        bound_id = await self._claude_task_binding(task_id)
        bound_account = self.pool.account(bound_id) if bound_id else None
        bound_config_dir = (
            os.path.expanduser(bound_account.config_dir)
            if bound_account is not None
            else None
        )
        preferred_owner_config_dir: str | None = None
        preferred_config_dir: str | None = None
        preferred_id = self.pool.preferred_account_id
        if preferred_id:
            preferred_account = self.pool.account(preferred_id)
            if preferred_account is not None:
                preferred_owner_config_dir = os.path.expanduser(
                    preferred_account.config_dir
                )
            if (
                preferred_owner_config_dir is not None
                and self.pool.is_config_dir_available(
                    preferred_owner_config_dir
                )
                and self.pool.supports_model_for_config_dir(
                    preferred_owner_config_dir, model
                )
            ):
                preferred_config_dir = preferred_owner_config_dir

        # --- Resume happy path: anchor to the session's resident account ---
        # The expensive part used to be ``select_async(validate=True)``, which
        # spawned a ``claude -p`` probe (a full API round-trip, up to 30s) on
        # EVERY message before resume even started. That probe is redundant on
        # the resume path: rate-limited / auth-failed accounts are already
        # excluded by the in-memory cooldown map, and a limit that slips through
        # surfaces as a recoverable event the reactive rotation path handles.
        # Worse, validate-driven round-robin drifted the config_dir off the
        # session's resident account every turn, forcing the PTY pool to drop a
        # hot session and pay an 8s cold restart. So: if the session lives on a
        # healthy (not cooled-down) account, reuse it directly — no probe, no
        # migration, no config_dir drift, PTY hot-session preserved.
        if session_id:
            matches = self.pool.locate_session_config_dirs(session_id)
            resident: str | None = None
            if bound_config_dir and (
                not matches or bound_config_dir in matches
            ):
                # A just-started/hot PTY session may not have flushed its JSONL
                # yet. The durable Task owner is still safer than drifting to a
                # newly selected account where --resume cannot possibly find
                # the native context.
                resident = bound_config_dir
            elif len(matches) == 1:
                resident = matches[0]
                if bound_config_dir and resident != bound_config_dir:
                    logger.warning(
                        "Repairing stale Claude account binding for task %s "
                        "session %s: %s -> %s",
                        task_id,
                        session_id,
                        bound_config_dir,
                        resident,
                    )
            elif len(matches) > 1:
                if preferred_owner_config_dir in matches:
                    # An explicit user choice is authoritative enough to
                    # disambiguate legacy divergent copies. Persist it before
                    # launch so clearing the global preference later cannot
                    # jump this chat back to a different history.
                    resident = preferred_owner_config_dir
                    logger.warning(
                        "Using explicitly preferred Claude account %s to "
                        "disambiguate session %s for task %s",
                        resident,
                        session_id,
                        task_id,
                    )
                else:
                    resident = self.pool.authoritative_session_config_dir(
                        session_id,
                        matches,
                    )
                if resident is None:
                    raise ClaudeAccountRoutingError(
                        f"Claude session {session_id} has multiple copies "
                        "without one provable complete owner and no "
                        "authoritative Task binding; refusing to guess which "
                        "context is current",
                        permanent=True,
                    )
                if preferred_owner_config_dir not in matches:
                    logger.warning(
                        "Bootstrapping Claude account binding for task %s "
                        "session %s from its provable complete copy: %s",
                        task_id,
                        session_id,
                        resident,
                    )
            resident_available = bool(
                resident
                and self.pool.is_known_account(resident)
                and self.pool.is_config_dir_available(resident)
                and self.pool.supports_model_for_config_dir(resident, model)
            )
            if resident_available and (
                preferred_config_dir is None
                or preferred_config_dir == resident
            ):
                await self._persist_claude_binding_for_route(
                    task_id=task_id,
                    config_dir=resident,
                    expected_generation=expected_generation,
                )
                return resident
            # Resident account is missing, rate-limited, or disabled → pick a
            # healthy enabled account cheaply (cooldown/enabled-aware, no
            # subprocess) and migrate the session in. The disabled case makes
            # ``enabled=false`` a hard guarantee: an in-flight session sitting on
            # a retired account is moved off it on its next resume instead of
            # being reused. An explicit preferred account reaches the same
            # migration path even when the resident is healthy, so the next
            # turn really honors "切换到此账号".
            config_dir = preferred_config_dir or self.pool.select(
                validate=False, model=model
            )
            if config_dir:
                if resident and resident != config_dir:
                    await self._require_task_lifecycle_active(
                        expected_generation
                    )
                    from backend.services.claude_pool import (
                        migrate_session_async,
                    )
                    migrated = await migrate_session_async(
                        old_config_dir=resident,
                        new_config_dir=config_dir,
                        session_id=session_id,
                    )
                    await self._require_task_lifecycle_active(
                        expected_generation
                    )
                    if not migrated:
                        if resident_available:
                            logger.error(
                                "Preferred Claude account switch for task %s "
                                "session %s could not migrate %s -> %s; "
                                "continuing on the intact resident account",
                                task_id,
                                session_id,
                                resident,
                                config_dir,
                            )
                            await self._persist_claude_binding_for_route(
                                task_id=task_id,
                                config_dir=resident,
                                expected_generation=expected_generation,
                            )
                            return resident
                        raise ClaudeAccountRoutingError(
                            f"Claude session {session_id} could not be migrated "
                            "to an available account; preserving the turn "
                            "instead of launching without its native context",
                            retry_after=CODEX_ROUTING_RETRY_DELAY,
                        )
                await self._persist_claude_binding_for_route(
                    task_id=task_id,
                    config_dir=config_dir,
                    expected_generation=expected_generation,
                )
                return config_dir
            # Pool exhausted: anchor to where the session actually lives so
            # --resume finds the conversation instead of hard-failing on a wrong
            # (inherited) account dir.
            if resident:
                compatible_account_exists = (
                    self.pool.has_compatible_enabled_account(model)
                )
                retryable_account_exists = (
                    compatible_account_exists
                    and self.pool.has_retryable_compatible_account(model)
                )
                routing_error_kwargs = {
                    "retry_after": (
                        CODEX_ROUTING_RETRY_DELAY
                        if retryable_account_exists
                        else None
                    ),
                    "permanent": not retryable_account_exists,
                }
                if (
                    self.pool.is_known_account(resident)
                    and not self.pool.supports_model_for_config_dir(
                        resident, model
                    )
                ):
                    raise ClaudeAccountRoutingError(
                        f"Claude session is resident on a CloudRouter account "
                        f"that does not support model {model!r}, and no "
                        "compatible replacement is currently available",
                        **routing_error_kwargs,
                    )
                if (
                    self.pool.is_known_account(resident)
                    and self.pool.is_disabled(resident)
                ):
                    raise ClaudeAccountRoutingError(
                        "Claude session is resident on a disabled account and "
                        "no enabled replacement account is currently available",
                        **routing_error_kwargs,
                    )
                if self.pool.is_cloudrouter_account(resident):
                    raise ClaudeAccountRoutingError(
                        f"Claude CloudRouter account for model {model!r} is "
                        "currently unavailable and no compatible replacement "
                        "account can safely resume the session",
                        **routing_error_kwargs,
                    )
                logger.warning(
                    "Pool exhausted; resuming session %s on its resident account "
                    "dir %s (account may be rate-limited, but --resume can still "
                    "find the conversation)",
                    session_id, resident,
                )
                await self._persist_claude_binding_for_route(
                    task_id=task_id,
                    config_dir=resident,
                    expected_generation=expected_generation,
                )
            return resident

        # Fresh launch (no session to anchor): just pick a healthy account.
        config_dir = self.pool.select(validate=False, model=model)
        if config_dir is None and self.pool._accounts:
            compatible_account_exists = (
                self.pool.has_compatible_enabled_account(model)
            )
            retryable_account_exists = (
                compatible_account_exists
                and self.pool.has_retryable_compatible_account(model)
            )
            detail = (
                "no enabled account supports the model"
                if not compatible_account_exists
                else (
                    "all compatible accounts require quota or credential intervention"
                    if not retryable_account_exists
                    else "all compatible pool accounts are currently unavailable"
                )
            )
            raise ClaudeAccountRoutingError(
                f"Claude pool has {detail} {model!r}; refusing to fall back "
                "to the service default account",
                retry_after=(
                    CODEX_ROUTING_RETRY_DELAY
                    if retryable_account_exists
                    else None
                ),
                permanent=not retryable_account_exists,
            )
        if config_dir:
            await self._persist_claude_binding_for_route(
                task_id=task_id,
                config_dir=config_dir,
                expected_generation=expected_generation,
            )
        return config_dir

    async def _task_account_binding(
        self,
        task_id: int | None,
        metadata_key: str,
    ) -> str | None:
        if task_id is None:
            return None
        async with self.db_factory() as db:
            task = await db.get(Task, task_id)
            if not task:
                return None
            value = (task.metadata_ or {}).get(metadata_key)
            return value if isinstance(value, str) and value else None

    async def _set_task_account_binding(
        self,
        task_id: int | None,
        account_id: str | None,
        metadata_key: str,
        *,
        expected_generation: _TaskRoutingGeneration | None = None,
    ) -> bool:
        if task_id is None or not account_id:
            return False
        async with self.db_factory() as db:
            # Merge only after locking the current row.  Account rotation can
            # overlap PR synchronize, which atomically adds the durable
            # ``pr_review_superseded`` marker.  A pre-lock ORM snapshot followed
            # by a whole-JSON UPDATE would otherwise erase that marker.
            statement = select(Task).where(Task.id == task_id)
            if expected_generation is not None:
                if getattr(expected_generation, "status", None) is None:
                    expected_predicates = (
                        self._task_lifecycle_generation_predicates(
                            expected_generation
                        )
                    )
                else:
                    expected_predicates = [
                        *self._task_status_generation_predicates(
                            expected_generation
                        ),
                        task_retry_not_superseded_predicate(),
                    ]
                statement = statement.where(
                    *expected_predicates
                )
            task = (
                await db.execute(statement.with_for_update())
            ).scalar_one_or_none()
            if not task:
                return False
            metadata = dict(task.metadata_ or {})
            if metadata.get(metadata_key) == account_id:
                return True
            metadata[metadata_key] = account_id
            # SQLAlchemy JSON columns do not reliably detect in-place changes.
            task.metadata_ = metadata
            await db.commit()
            return True

    async def _claude_task_binding(self, task_id: int | None) -> str | None:
        return await self._task_account_binding(
            task_id,
            "claude_account_id",
        )

    async def _set_claude_task_binding(
        self,
        task_id: int | None,
        account_id: str | None,
        *,
        expected_generation: _TaskRoutingGeneration | None = None,
    ) -> bool:
        return await self._set_task_account_binding(
            task_id,
            account_id,
            "claude_account_id",
            expected_generation=expected_generation,
        )

    async def _codex_task_binding(self, task_id: int | None) -> str | None:
        return await self._task_account_binding(
            task_id,
            "codex_account_id",
        )

    async def _set_codex_task_binding(
        self,
        task_id: int | None,
        account_id: str | None,
        *,
        expected_generation: _TaskRoutingGeneration | None = None,
    ) -> bool:
        return await self._set_task_account_binding(
            task_id,
            account_id,
            "codex_account_id",
            expected_generation=expected_generation,
        )

    async def _persist_claude_binding_for_route(
        self,
        *,
        task_id: int | None,
        config_dir: str | None,
        expected_generation: _TaskRoutingGeneration | None,
        record_route: bool = True,
        on_route_committed: Callable[[], None] | None = None,
    ) -> bool:
        """Durably bind a Task to the resolved Claude session owner.

        Claude session migration intentionally leaves a hardlinked source copy.
        Without a durable owner, a later process restart would rediscover the
        first copy in pool order and silently move the chat back to an older
        account. Binding is therefore settled before launch; cancellation or a
        transient database failure must never allow an unbound launch.
        """

        if not config_dir or self.pool is None:
            return False
        account_id = self.pool.account_id_from_config_dir(config_dir)
        if account_id is None:
            return False
        if task_id is None:
            if on_route_committed is not None:
                on_route_committed()
            if record_route:
                self.pool.record_routed_account(config_dir)
            return False

        binding, cancellation = await _settle_despite_cancellation(
            self._set_claude_task_binding(
                task_id,
                account_id,
                expected_generation=expected_generation,
            )
        )
        try:
            bound = binding.result()
        except BaseException as exc:
            if cancellation is not None:
                raise cancellation from exc
            raise ClaudeAccountRoutingError(
                f"Claude account binding for task {task_id} could not be "
                "persisted; preserving the turn for retry",
                retry_after=CODEX_ROUTING_RETRY_DELAY,
            ) from exc
        if not bound:
            if expected_generation is not None:
                raise TaskLifecycleSupersededError(
                    f"Task {task_id} lost its lifecycle before Claude binding"
                )
            raise ClaudeAccountRoutingError(
                f"Claude account binding for task {task_id} was not "
                "persisted; preserving the turn for retry",
                retry_after=CODEX_ROUTING_RETRY_DELAY,
            )
        # The binding is now the durable route. Publish the marker before
        # delivering a delayed caller cancellation, otherwise DB=target could
        # coexist with a stale "recently used" account indefinitely.
        if on_route_committed is not None:
            on_route_committed()
        if record_route:
            self.pool.record_routed_account(config_dir)
        if cancellation is not None:
            raise cancellation
        return bound

    async def _rollback_codex_rebind_for_recovery(
        self,
        *,
        task_id: int | None,
        session_id: str,
        source_home: str,
        target_home: str,
    ) -> tuple[bool, asyncio.CancelledError | None]:
        """Restore app-server routing after the durable binding CAS loses."""

        cancellation: asyncio.CancelledError | None = None
        try:
            rollback, cancellation = await _settle_despite_cancellation(
                self.instance_manager.rebind_codex_thread(
                    session_id,
                    source_codex_home=target_home,
                    target_codex_home=source_home,
                )
            )
            rollback.result()
            return True, cancellation
        except BaseException:
            logger.exception(
                "Codex routing rollback failed for task %s thread %s "
                "(%s -> %s)",
                task_id,
                session_id,
                target_home,
                source_home,
            )
            try:
                clear_owner, clear_cancellation = (
                    await _settle_despite_cancellation(
                        self.instance_manager
                        .clear_codex_thread_owner_for_recovery(
                            session_id,
                            expected_codex_home=target_home,
                        )
                    )
                )
                if cancellation is None:
                    cancellation = clear_cancellation
                clear_owner.result()
            except BaseException:
                logger.exception(
                    "Codex routing could not clear stale owner for task %s "
                    "thread %s",
                    task_id,
                    session_id,
                )
            return False, cancellation

    async def _persist_codex_binding_for_route(
        self,
        *,
        task_id: int | None,
        account_id: str | None,
        expected_generation: _TaskRoutingGeneration | None,
        session_id: str | None = None,
        source_home: str | None = None,
        target_home: str | None = None,
        record_route: bool = True,
        on_route_committed: Callable[[], None] | None = None,
    ) -> bool:
        """Settle the binding commit, compensating a prior thread rebind."""

        binding, cancellation = await _settle_despite_cancellation(
            self._set_codex_task_binding(
                task_id,
                account_id,
                expected_generation=expected_generation,
            )
        )
        binding_error: BaseException | None = None
        try:
            bound = binding.result()
        except BaseException as exc:
            bound = False
            binding_error = exc

        lost_binding = (
            task_id is not None
            and account_id is not None
            and not bound
        )
        if (
            (binding_error is not None or lost_binding)
            and session_id
            and source_home
            and target_home
        ):
            _, rollback_cancellation = (
                await self._rollback_codex_rebind_for_recovery(
                    task_id=task_id,
                    session_id=session_id,
                    source_home=source_home,
                    target_home=target_home,
                )
            )
            if cancellation is None:
                cancellation = rollback_cancellation

        if binding_error is not None:
            if cancellation is not None:
                raise cancellation from binding_error
            raise CodexAccountRoutingError(
                f"Codex account binding for task {task_id} could not be "
                "persisted; preserving the turn for retry",
                retry_after=CODEX_ROUTING_RETRY_DELAY,
            ) from binding_error
        if lost_binding:
            if cancellation is not None:
                raise cancellation
            if expected_generation is not None:
                raise TaskLifecycleSupersededError(
                    f"Task {task_id} lost its lifecycle before Codex binding"
                )
            raise CodexAccountRoutingError(
                f"Codex account binding for task {task_id} was not persisted; "
                "preserving the turn for retry",
                retry_after=CODEX_ROUTING_RETRY_DELAY,
            )
        if bound and on_route_committed is not None:
            on_route_committed()
        if (
            record_route
            and account_id
            and self.codex_pool
            and (bound or task_id is None)
        ):
            routed_home = self.codex_pool.home_for_account(account_id)
            if routed_home:
                self.codex_pool.record_routed_account(routed_home)
        if cancellation is not None:
            raise cancellation
        return bound

    async def _rebind_and_persist_codex_route(
        self,
        *,
        task_id: int | None,
        session_id: str,
        source_home: str,
        target_home: str,
        account_id: str | None,
        expected_generation: _TaskRoutingGeneration | None,
    ) -> bool:
        """Settle forward rebind + binding + compensation as one unit."""

        async def transition() -> bool:
            await self.instance_manager.rebind_codex_thread(
                session_id,
                source_codex_home=source_home,
                target_codex_home=target_home,
            )
            return await self._persist_codex_binding_for_route(
                task_id=task_id,
                account_id=account_id,
                expected_generation=expected_generation,
                session_id=session_id,
                source_home=source_home,
                target_home=target_home,
            )

        operation, cancellation = await _settle_despite_cancellation(
            transition()
        )
        try:
            result = operation.result()
        except BaseException as exc:
            if cancellation is not None:
                raise cancellation from exc
            raise
        if cancellation is not None:
            raise cancellation
        return result

    async def _migrate_rebind_and_persist_codex_route(
        self,
        *,
        task_id: int | None,
        session_id: str,
        source_home: str,
        target_home: str,
        account_id: str | None,
        expected_generation: _TaskRoutingGeneration | None,
    ) -> bool:
        """Settle rollout copy, live owner move and binding as one unit.

        ``asyncio.to_thread`` cannot stop an in-progress filesystem copy when
        its caller is cancelled.  Letting cancellation escape at that point
        could leave a second rollout copy without a durable owner.  Delay the
        cancellation until the copy is followed by either a committed binding
        or the existing rebind compensation path.
        """

        from backend.services.codex_session_migration import (
            migrate_codex_rollout_session,
        )

        source_account_id = (
            self.codex_pool.account_id_for_home(source_home)
            if self.codex_pool is not None
            else None
        )
        if task_id is None or source_account_id is None:
            raise CodexAccountRoutingError(
                f"Codex session {session_id} cannot be migrated from "
                f"unregistered account home {source_home} without a durable "
                "source binding",
                permanent=True,
            )

        # Anchor the source before creating another physical rollout copy.  A
        # concurrent retry can supersede the Task generation while the file
        # copy is running; the pre-existing source binding then remains
        # authoritative instead of leaving two unbound legacy copies.
        await self._persist_codex_binding_for_route(
            task_id=task_id,
            account_id=source_account_id,
            expected_generation=expected_generation,
            record_route=False,
        )

        async def transition() -> bool:
            await self._require_task_lifecycle_active(expected_generation)
            await asyncio.to_thread(
                migrate_codex_rollout_session,
                session_id,
                source_home,
                target_home,
            )
            await self._require_task_lifecycle_active(expected_generation)
            return await self._rebind_and_persist_codex_route(
                task_id=task_id,
                session_id=session_id,
                source_home=source_home,
                target_home=target_home,
                account_id=account_id,
                expected_generation=expected_generation,
            )

        operation, cancellation = await _settle_despite_cancellation(
            transition()
        )
        try:
            result = operation.result()
        except BaseException as exc:
            if cancellation is not None:
                raise cancellation from exc
            raise
        if cancellation is not None:
            raise cancellation
        return result

    def _codex_pool_retry_after(self) -> float | None:
        pool = self.codex_pool
        if not pool:
            return None
        remaining = [
            float(account.get("cooldown_remaining") or 0)
            for account in pool.list_accounts()
            if account.get("enabled") and account.get("cooldown_remaining")
        ]
        return max(1.0, min(remaining)) if remaining else None

    async def _resolve_codex_home(
        self,
        session_id: str | None,
        *,
        task_id: int | None,
        expected_generation: _TaskRoutingGeneration | None = None,
        model: str | None = None,
    ) -> str | None:
        """Select/reuse a Codex account without losing the native thread.

        A migrated rollout deliberately remains in the source account as a
        recovery copy, so filesystem discovery alone cannot pick an owner after
        the first switch. ``Task.metadata_.codex_account_id`` is authoritative;
        a single discovered home is only used to bootstrap older tasks.
        """
        pool = self.codex_pool
        if not (pool and pool.enabled):
            return None
        await self._require_task_lifecycle_active(expected_generation)

        bound_id = await self._codex_task_binding(task_id)
        bound_home = pool.home_for_account(bound_id) if bound_id else None
        matches: list[str] = []
        if session_id:
            matches = pool.locate_session_homes(session_id)

        preferred_owner_home: str | None = None
        preferred_home: str | None = None
        preferred_id = pool.preferred_account_id
        if preferred_id:
            candidate_home = pool.home_for_account(preferred_id)
            if candidate_home:
                preferred_owner_home = pool.canonical_home(candidate_home)
            if (
                preferred_owner_home
                and pool.is_home_available(preferred_owner_home)
                and pool.supports_model_for_home(
                    preferred_owner_home,
                    model,
                )
            ):
                preferred_home = preferred_owner_home

        resident: str | None = None
        if bound_home:
            canonical_bound = pool.canonical_home(bound_home)
            if not session_id or not matches or canonical_bound in matches:
                # No rollout yet is valid for a just-started app-server thread.
                resident = canonical_bound
            elif len(matches) == 1:
                # Repair stale metadata created by legacy/manual launch paths:
                # the only physical rollout is more authoritative than a
                # binding that points at a home where resume cannot work.
                resident = matches[0]
                logger.warning(
                    "Repairing stale Codex account binding for task %s session %s: "
                    "%s -> %s",
                    task_id, session_id, canonical_bound, resident,
                )
            elif preferred_owner_home in matches:
                resident = preferred_owner_home
                logger.warning(
                    "Explicit Codex preference overrides stale binding for "
                    "task %s session %s: %s -> %s",
                    task_id,
                    session_id,
                    canonical_bound,
                    resident,
                )
            else:
                raise CodexAccountRoutingError(
                    f"Codex session {session_id} has multiple rollout copies, "
                    f"none in its bound account home {canonical_bound}",
                    permanent=True,
                )
        elif len(matches) == 1:
            resident = matches[0]
        elif len(matches) > 1:
            if preferred_owner_home in matches:
                resident = preferred_owner_home
                logger.warning(
                    "Using explicitly preferred Codex account %s to "
                    "disambiguate session %s for task %s",
                    resident,
                    session_id,
                    task_id,
                )
            else:
                raise CodexAccountRoutingError(
                    f"Codex session {session_id} exists in multiple account "
                    "homes but the task has no codex_account_id binding",
                    permanent=True,
                )

        resident_available = bool(
            resident
            and pool.is_home_available(resident)
            and pool.supports_model_for_home(resident, model)
        )

        if resident_available and (
            preferred_home is None or preferred_home == resident
        ):
            account_id = pool.account_id_for_home(resident)
            await self._persist_codex_binding_for_route(
                task_id=task_id,
                account_id=account_id,
                expected_generation=expected_generation,
            )
            return resident

        excluded: set[str] = set()
        resident_id = pool.account_id_for_home(resident) if resident else None
        if resident_id:
            excluded.add(resident_id)
        target = preferred_home or pool.select(exclude=excluded, model=model)

        if not target:
            if not pool.has_compatible_enabled_account(model):
                raise CodexAccountRoutingError(
                    f"Codex pool has no enabled account supporting model "
                    f"{model!r}; refusing to route or downgrade the task",
                    permanent=True,
                )
            if not pool.has_retryable_compatible_account(model):
                raise CodexAccountRoutingError(
                    f"All compatible Codex accounts for model {model!r} "
                    "require quota or credential intervention before retrying",
                    permanent=True,
                )
            if resident and pool.is_known_account(resident) and pool.is_home_enabled(resident):
                retry_after = self._codex_pool_retry_after()
                raise CodexAccountRoutingError(
                    f"Codex pool is cooling down; task {task_id} session "
                    f"{session_id} remains safely stored in {resident}",
                    retry_after=retry_after,
                )
            if resident:
                raise CodexAccountRoutingError(
                    f"Codex task {task_id} is bound to disabled/removed account "
                    f"home {resident} and no enabled account is available for migration",
                    permanent=True,
                )
            retry_after = self._codex_pool_retry_after()
            raise CodexAccountRoutingError(
                "Codex pool has no available account; refusing to fall back to "
                "the service's default CODEX_HOME",
                retry_after=retry_after,
            )

        target = pool.canonical_home(target)
        account_id = pool.account_id_for_home(target)
        binding_persisted = False
        if session_id and resident and resident != target:
            from backend.services.codex_session_migration import (
                CodexSessionMigrationError,
            )

            try:
                await self._migrate_rebind_and_persist_codex_route(
                    task_id=task_id,
                    session_id=session_id,
                    source_home=resident,
                    target_home=target,
                    account_id=account_id,
                    expected_generation=expected_generation,
                )
                binding_persisted = True
            except CodexSessionMigrationError:
                logger.exception(
                    "Refusing to switch Codex task %s session %s from %s to %s "
                    "because its rollout could not be migrated safely",
                    task_id, session_id, resident, target,
                )
                if pool.is_home_available(resident):
                    await self._persist_codex_binding_for_route(
                        task_id=task_id,
                        account_id=pool.account_id_for_home(resident),
                        expected_generation=expected_generation,
                    )
                    return resident
                raise CodexAccountRoutingError(
                    f"Codex session {session_id} could not be migrated from "
                    f"unavailable account home {resident}"
                )

        if not binding_persisted:
            await self._persist_codex_binding_for_route(
                task_id=task_id,
                account_id=account_id,
                expected_generation=expected_generation,
            )
        return target

    async def _collect_failure_output(self, instance_id: int, task_id: int) -> str:
        """Gather stderr + recent log text once for failure classification.

        ``get_last_stderr()`` is destructive (it pops), so a caller that needs
        to test for BOTH transient overload and account rotation must collect
        once and pass the combined text into both detectors.
        """
        from backend.services.claude_pool import collect_process_output_for_detection
        stderr = self.instance_manager.get_last_stderr(instance_id)
        log_contents = await self.instance_manager.get_recent_log_contents(task_id, limit=10)
        return collect_process_output_for_detection(stderr, log_contents)

    async def _check_rate_limit_and_rotate(
        self,
        instance_id: int,
        task_id: int,
        exit_code: int,
        combined: str | None = None,
        *,
        expected_generation: _TaskLifecycleGeneration | None = None,
    ) -> dict | None:
        """After a failed process, check if it was a rate limit and attempt rotation.

        Returns a dict with {config_dir, session_id, excluded} if rotation is
        possible, or None if this is not a pool-rotatable failure. ``combined``
        may be pre-collected by the caller (see _collect_failure_output) to
        avoid double-popping stderr.
        """
        if exit_code == 0 or exit_code in (-2, 130):
            return None
        await self._require_task_lifecycle_active(expected_generation)

        async with self.db_factory() as db:
            t = (
                await self._read_owned_lifecycle_task(
                    db,
                    expected_generation,
                )
                if expected_generation is not None
                else await db.get(Task, task_id)
            )
            provider = (t.provider or "claude").lower() if t else "claude"
            task_model = t.model if t else None

        # Codex has an independent account pool and native rollout format. It
        # must never enter the Claude pool/migrate_session path below.
        if provider == "codex":
            return await self._check_codex_rate_limit_and_rotate(
                instance_id,
                task_id,
                combined=combined,
                expected_generation=expected_generation,
            )
        if not self.pool:
            return None

        from backend.services.claude_pool import is_pool_rotatable, is_auth_failure, is_rate_limited
        from backend.services.claude_pool import (
            collect_process_output_for_detection,
            migrate_session_async,
        )

        if combined is None:
            stderr = self.instance_manager.get_last_stderr(instance_id)
            log_contents = await self.instance_manager.get_recent_log_contents(task_id, limit=10)
            combined = collect_process_output_for_detection(stderr, log_contents)
        await self._require_task_lifecycle_active(expected_generation)

        cloudrouter_auth_failed = (
            self.instance_manager.is_cloudrouter_auth_failure(
                instance_id, "claude", combined
            )
            is True
        )
        if not (is_pool_rotatable(combined) or cloudrouter_auth_failed):
            return None

        old_config_dir = self.instance_manager.get_config_dir(instance_id)
        if not old_config_dir:
            # Launched on the default account (no explicit config_dir) —
            # rotation must still work; the default dir is a pool member.
            import os as _os
            old_config_dir = _os.path.expanduser("~/.claude")

        # Mark the old account
        if is_auth_failure(combined) or cloudrouter_auth_failed:
            self.pool.mark_auth_failure(old_config_dir)
            logger.warning("Pool account %s auth failure, marked indefinite cooldown", old_config_dir)
        elif is_rate_limited(combined):
            self.pool.mark_rate_limited(old_config_dir)
            logger.info("Pool account %s rate-limited, marked cooldown", old_config_dir)

        # Build exclusion set
        old_account_id = self.pool.account_id_from_config_dir(old_config_dir)
        excluded = {old_account_id} if old_account_id else set()

        new_config_dir = await self._pool_select(
            exclude=excluded,
            model=task_model,
        )
        await self._require_task_lifecycle_active(expected_generation)
        if not new_config_dir:
            logger.warning("Pool exhausted — no alternative account for task %d", task_id)
            return None

        # Get session_id for --resume
        async with self.db_factory() as db:
            t = (
                await self._read_owned_lifecycle_task(
                    db,
                    expected_generation,
                )
                if expected_generation is not None
                else await db.get(Task, task_id)
            )
            session_id = t.session_id if t else None

        if session_id:
            source_dir = (
                self.pool.locate_session_config_dir(
                    session_id,
                    resident_config_dir=old_config_dir,
                )
                or old_config_dir
            )
            await self._require_task_lifecycle_active(expected_generation)
            migrated = await migrate_session_async(
                old_config_dir=source_dir,
                new_config_dir=new_config_dir,
                session_id=session_id,
            )
            await self._require_task_lifecycle_active(expected_generation)
            if not migrated:
                await self._persist_claude_binding_for_route(
                    task_id=task_id,
                    config_dir=source_dir,
                    expected_generation=expected_generation,
                )
                logger.error(
                    "Pool rotation for task %s could not migrate Claude "
                    "session %s from %s to %s; refusing context-less resume",
                    task_id,
                    session_id,
                    source_dir,
                    new_config_dir,
                )
                return None

        await self._persist_claude_binding_for_route(
            task_id=task_id,
            config_dir=new_config_dir,
            expected_generation=expected_generation,
        )

        # Broadcast pool rotation event
        await self._require_task_lifecycle_active(expected_generation)
        await self.broadcaster.broadcast(f"task:{task_id}", {
            "event_type": "pool_rotation",
            "old_account": old_account_id,
            "new_account": self.pool.account_id_from_config_dir(new_config_dir),
            "reason": (
                "rate_limit"
                if is_rate_limited(combined) and not cloudrouter_auth_failed
                else "auth_failure"
            ),
        })
        await self.broadcaster.broadcast("system", {
            "event": "pool_rotation",
            "task_id": task_id,
            "instance_id": instance_id,
            "old_account": old_account_id,
            "new_account": self.pool.account_id_from_config_dir(new_config_dir),
        })

        return {
            "config_dir": new_config_dir,
            "session_id": session_id,
            "excluded": excluded,
        }

    async def _check_codex_rate_limit_and_rotate(
        self,
        instance_id: int,
        task_id: int,
        *,
        combined: str | None,
        expected_generation: _TaskLifecycleGeneration | None = None,
    ) -> dict | None:
        pool = self.codex_pool
        if not (pool and pool.enabled):
            return None
        await self._require_task_lifecycle_active(expected_generation)

        from backend.services.codex_pool import (
            is_auth_failure,
            is_pool_rotatable,
            is_rate_limited,
        )

        if combined is None:
            combined = await self._collect_failure_output(instance_id, task_id)
        await self._require_task_lifecycle_active(expected_generation)
        cloudrouter_auth_failed = (
            self.instance_manager.is_cloudrouter_auth_failure(
                instance_id, "codex", combined
            )
            is True
        )
        if not (is_pool_rotatable(combined) or cloudrouter_auth_failed):
            return None

        async with self.db_factory() as db:
            task = (
                await self._read_owned_lifecycle_task(
                    db,
                    expected_generation,
                )
                if expected_generation is not None
                else await db.get(Task, task_id)
            )
            session_id = task.session_id if task else None
            bound_id = (
                (task.metadata_ or {}).get("codex_account_id") if task else None
            )
            task_model = task.model if task else None

        old_home = self.instance_manager.get_config_dir(instance_id)
        if not old_home and isinstance(bound_id, str):
            old_home = pool.home_for_account(bound_id)
        old_home = pool.canonical_home(
            old_home or os.environ.get("CODEX_HOME") or str(Path.home() / ".codex")
        )

        auth_failed = is_auth_failure(combined) or cloudrouter_auth_failed
        await self._require_task_lifecycle_active(expected_generation)
        if auth_failed:
            pool.mark_auth_failure(old_home)
            logger.warning(
                "Codex pool account %s auth failed; cooling it indefinitely",
                old_home,
            )
        elif is_rate_limited(combined):
            pool.mark_rate_limited(old_home)
            logger.info("Codex pool account %s hit its usage limit", old_home)

        old_account_id = pool.account_id_for_home(old_home)
        excluded = {old_account_id} if old_account_id else set()
        new_home = pool.select(exclude=excluded, model=task_model)
        if not new_home:
            logger.warning(
                "Codex pool exhausted — no alternative account for task %d",
                task_id,
            )
            raise CodexAccountRoutingError(
                f"Codex pool has no alternative account for task {task_id}; "
                "the current native thread remains in its original CODEX_HOME",
                retry_after=self._codex_pool_retry_after(),
            )
        new_home = pool.canonical_home(new_home)

        new_account_id = pool.account_id_for_home(new_home)
        binding_persisted = False
        source_home = old_home
        if session_id and old_home != new_home:
            from backend.services.codex_app_server import (
                CodexAppServerBusyError,
                CodexThreadHomeMismatchError,
            )
            from backend.services.codex_session_migration import (
                CodexSessionMigrationError,
            )

            matches = pool.locate_session_homes(session_id)
            if source_home not in matches:
                # Older tasks may not have an account binding. Accept one
                # unambiguous pool copy, but never guess among migrated copies.
                if len(matches) != 1:
                    logger.error(
                        "Cannot identify a unique Codex rollout for task %s "
                        "session %s",
                        task_id,
                        session_id,
                    )
                    raise CodexAccountRoutingError(
                        f"Cannot identify a unique Codex rollout for task "
                        f"{task_id} session {session_id}",
                        retry_after=CODEX_ROUTING_RETRY_DELAY,
                    )
                source_home = matches[0]

            try:
                await self._migrate_rebind_and_persist_codex_route(
                    task_id=task_id,
                    session_id=session_id,
                    source_home=source_home,
                    target_home=new_home,
                    account_id=new_account_id,
                    expected_generation=expected_generation,
                )
                binding_persisted = True
            except CodexSessionMigrationError:
                logger.exception(
                    "Codex rollout migration failed for task %s session %s",
                    task_id, session_id,
                )
                raise CodexAccountRoutingError(
                    f"Codex rollout migration failed for task {task_id} "
                    f"session {session_id}",
                    retry_after=CODEX_ROUTING_RETRY_DELAY,
                )
            except (CodexAppServerBusyError, CodexThreadHomeMismatchError):
                logger.exception(
                    "Codex app-server refused account rebind for task %s session %s",
                    task_id, session_id,
                )
                raise CodexAccountRoutingError(
                    f"Codex app-server could not rebind task {task_id} session "
                    f"{session_id}",
                    retry_after=CODEX_ROUTING_RETRY_DELAY,
                )
            except CodexAccountRoutingError:
                raise

        if not binding_persisted:
            await self._persist_codex_binding_for_route(
                task_id=task_id,
                account_id=new_account_id,
                expected_generation=expected_generation,
            )
        await self._require_task_lifecycle_active(expected_generation)
        reason = "auth_failure" if auth_failed else "rate_limit"
        await self.broadcaster.broadcast(f"task:{task_id}", {
            "event_type": "pool_rotation",
            "provider": "codex",
            "old_account": old_account_id,
            "new_account": new_account_id,
            "reason": reason,
        })
        await self.broadcaster.broadcast("system", {
            "event": "pool_rotation",
            "provider": "codex",
            "task_id": task_id,
            "instance_id": instance_id,
            "old_account": old_account_id,
            "new_account": new_account_id,
        })
        return {
            "config_dir": new_home,
            "session_id": session_id,
            "excluded": excluded,
        }

    async def _build_task_prompt(self, task: Task) -> str:
        """Reconstruct a task's initial prompt (CLAUDE.md preamble + secrets +
        images + description). Enabled skills are advertised separately through
        the launch-time skill directory, so merely enabling one never claims
        that the user invoked its $command. Shared by the first launch and
        every fresh re-launch (rotation / transient retry)."""
        metadata = task.metadata_ or {}
        image_paths = metadata.get("image_paths") or []
        secret_ids = metadata.get("secret_ids") or []
        secrets_block = await _build_secrets_block(self.db_factory, secret_ids)
        parts = [_agent_doc_preamble(task.provider)]
        if secrets_block:
            parts.append(secrets_block)
        if image_paths:
            image_list = "\n".join(f"- {p}" for p in image_paths)
            parts.append(f"用户提供了以下参考图片，请先用 Read 工具查看：\n{image_list}")
        command, command_args = _initial_task_command(task)
        task_description = task.description
        if command:
            if command_args:
                task_description = command_args
            parts.append(command.prompt_template)
        parts.append(f"任务:\n{task_description}")
        return "\n\n".join(parts)

    async def _relaunch_and_wait(
        self,
        instance_id: int,
        task: Task,
        generation: _TaskLifecycleGeneration,
        cwd: str,
        git_env: dict | None,
        config_dir: str | None,
        session_id: str | None,
        *,
        thinking_budget: int | None,
        effort_level: str | None,
        label: str,
    ) -> int:
        """Resume (or fresh-launch) a task on a specific account, wait for the
        process + output consumer, and return its exit code."""
        if not await self._task_claim_is_active(generation):
            logger.info(
                "Skipping stale relaunch for task %s on instance %s",
                task.id,
                instance_id,
            )
            return -2
        if session_id:
            await self.instance_manager.launch(
                instance_id=instance_id,
                prompt="请继续之前的工作。",
                task_id=task.id,
                cwd=cwd,
                model=task.model,
                resume_session_id=session_id,
                git_env=git_env or {},
                thinking_budget=thinking_budget,
                effort_level=effort_level,
                provider=task.provider,
                config_dir=config_dir,
                enable_workflows=task.enable_workflows,
                enabled_skills=task.enabled_skills,
            )
        else:
            full_prompt = await self._build_task_prompt(task)
            await self.instance_manager.launch(
                instance_id=instance_id,
                prompt=full_prompt,
                task_id=task.id,
                cwd=cwd,
                model=task.model,
                git_env=git_env or {},
                thinking_budget=thinking_budget,
                effort_level=effort_level,
                provider=task.provider,
                config_dir=config_dir,
                enable_workflows=task.enable_workflows,
                enabled_skills=task.enabled_skills,
            )

        process = self.instance_manager.processes.get(instance_id)
        if process:
            await self._wait_process(
                process, task, label, instance_id=instance_id
            )
        await self._wait_output_consumer(instance_id, task, label, process)
        return self._effective_process_exit_code(instance_id, process)

    async def _launch_mode_turn_with_rotation(
        self,
        instance_id: int,
        task: Task,
        generation: _TaskLifecycleGeneration,
        cwd: str,
        git_env: dict | None,
        *,
        prompt: str,
        config_dir: str | None,
        resume_session_id: str | None,
        loop_iteration: int | None,
        effort_level: str | None,
        label: str,
        max_rotations: int = 5,
    ) -> tuple[int, str | None]:
        """Run one plan/loop/goal turn and rotate provider accounts on limit.

        These lifecycle modes return before the normal Step 5 classifier, so
        without a local classifier Codex usage/auth failures would never reach
        its pool.  Replaying the same mode prompt on the migrated native thread
        preserves that mode's contract while avoiding the generic pool retry,
        which would incorrectly mark the entire task completed after one turn.
        """
        current_home = config_dir
        current_session = resume_session_id

        for rotation_attempt in range(max_rotations + 1):
            if not await self._task_claim_is_active(generation):
                logger.info(
                    "Skipping stale %s launch for task %s on instance %s",
                    label,
                    task.id,
                    instance_id,
                )
                return -2, current_home
            await self.instance_manager.launch(
                instance_id=instance_id,
                prompt=prompt,
                task_id=task.id,
                cwd=cwd,
                model=task.model,
                resume_session_id=current_session,
                loop_iteration=loop_iteration,
                git_env=git_env or {},
                thinking_budget=task.thinking_budget,
                effort_level=task.effort_level or effort_level,
                provider=task.provider,
                config_dir=current_home,
                enable_workflows=task.enable_workflows,
                enabled_skills=task.enabled_skills,
            )

            process = self.instance_manager.processes.get(instance_id)
            if process:
                await self._wait_process(
                    process, task, label, instance_id=instance_id
                )
            await self._wait_output_consumer(instance_id, task, label, process)
            exit_code = self._effective_process_exit_code(instance_id, process)
            if not await self._task_claim_is_active(generation):
                return -2, current_home
            if exit_code in (0, -2, 130):
                # _consume_output may have completed a proactive quota switch
                # after this successful turn.  Keep lifecycle/evaluator
                # routing aligned with the newly persisted task binding rather
                # than returning the home used at launch.
                if exit_code == 0:
                    active_home = self.instance_manager.get_config_dir(instance_id)
                    if isinstance(active_home, str) and active_home:
                        current_home = active_home
                return exit_code, current_home
            if rotation_attempt >= max_rotations:
                return exit_code, current_home

            combined = await self._collect_failure_output(instance_id, task.id)
            rotation = await self._check_rate_limit_and_rotate(
                instance_id,
                task.id,
                exit_code,
                combined=combined,
                expected_generation=generation,
            )
            if not rotation:
                return exit_code, current_home
            current_home = rotation["config_dir"]
            current_session = rotation.get("session_id") or current_session
            logger.info(
                "%s for task %s rotating account and retrying native session %s",
                label, task.id, current_session,
            )

        return -1, current_home

    async def _task_claim_is_active(
        self,
        generation: _TaskLifecycleGeneration,
    ) -> bool:
        """Return whether this lifecycle still owns an executable Task row.

        Account selection and retry backoff may await for long enough that a
        concurrent cancel/stop-session wins.  Re-checking the persisted claim
        immediately before every launch prevents a stale coroutine from
        starting a new process after cancellation.  The immutable lifecycle
        generation is captured from the DB-normalized Step 2 row; a refreshed
        ORM object must never replace it because a rapid retry may reuse both
        task id and instance id (ABA).
        """

        async with self.db_factory() as db:
            result = await db.execute(
                select(Task.id).where(
                    *self._task_lifecycle_generation_predicates(generation)
                )
            )
            return result.scalar_one_or_none() is not None

    async def _require_task_lifecycle_active(
        self,
        generation: _TaskRoutingGeneration | None,
    ) -> None:
        """Fail closed before/after account migration or thread rebind.

        Fresh/mode lifecycles allow their own in_progress -> executing status
        transition.  Queued chat freezes the exact pre-claim status generation
        because it may legitimately start from completed/failed.
        """

        if generation is None:
            return
        # InstanceManager uses a duck-typed lifecycle fence with status=None.
        # Queued chat carries an exact non-null status generation.
        if getattr(generation, "status", None) is None:
            active = await self._task_claim_is_active(generation)
        else:
            async with self.db_factory() as db:
                active = (
                    await db.execute(
                        select(Task.id).where(
                            *self._task_status_generation_predicates(
                                generation
                            ),
                            task_retry_not_superseded_predicate(),
                        )
                    )
                ).scalar_one_or_none() is not None
        if not active:
            raise TaskLifecycleSupersededError(
                f"Task {generation.task_id} lifecycle generation was superseded"
            )

    @staticmethod
    def _task_lifecycle_generation(
        source: _TaskStatusGeneration | Task,
    ) -> _TaskLifecycleGeneration:
        """Freeze ownership fields while allowing the status to advance."""

        return _TaskLifecycleGeneration(
            task_id=(
                source.task_id
                if isinstance(source, _TaskStatusGeneration)
                else source.id
            ),
            worker_id=source.worker_id,
            shared_from_id=source.shared_from_id,
            retry_count=source.retry_count,
            instance_id=source.instance_id,
            started_at=source.started_at,
            completed_at=source.completed_at,
        )

    @staticmethod
    def _task_lifecycle_generation_predicates(
        generation: _TaskLifecycleGeneration,
        *,
        statuses: tuple[str, ...] = ("in_progress", "executing"),
    ) -> list:
        """Build the durable active-owner fence for one lifecycle coroutine."""

        return [
            *GlobalDispatcher._task_lifecycle_stable_predicates(generation),
            Task.status.in_(statuses),
            (
                Task.completed_at.is_(None)
                if generation.completed_at is None
                else Task.completed_at == generation.completed_at
            ),
        ]

    @staticmethod
    def _task_lifecycle_stable_predicates(
        generation: _TaskLifecycleGeneration,
    ) -> list:
        """Fence fields unchanged by this lifecycle's own terminal transition."""

        return [
            Task.id == generation.task_id,
            (
                Task.worker_id.is_(None)
                if generation.worker_id is None
                else Task.worker_id == generation.worker_id
            ),
            (
                Task.shared_from_id.is_(None)
                if generation.shared_from_id is None
                else Task.shared_from_id == generation.shared_from_id
            ),
            Task.retry_count == generation.retry_count,
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
            task_retry_not_superseded_predicate(),
        ]

    @staticmethod
    def _task_lifecycle_queue_fence(
        generation: _TaskLifecycleGeneration,
    ) -> tuple[int, int | None, datetime | None, datetime | None]:
        """Adapt the stronger lifecycle fence to TaskQueue's CAS fields."""

        return (
            generation.retry_count,
            generation.instance_id,
            generation.started_at,
            generation.completed_at,
        )

    async def _read_owned_lifecycle_task(
        self,
        db,
        generation: _TaskLifecycleGeneration,
        *,
        for_update: bool = False,
    ) -> Task | None:
        """Refresh mutable settings without ever adopting a replacement ABA."""

        statement = select(Task).where(
            *self._task_lifecycle_generation_predicates(generation)
        )
        if for_update:
            statement = statement.with_for_update()
        return (await db.execute(statement)).scalar_one_or_none()

    async def _read_same_lifecycle_task(
        self,
        db,
        generation: _TaskLifecycleGeneration,
        *,
        for_update: bool = False,
    ) -> Task | None:
        """Read the same lifecycle after its own status/completion transition."""

        statement = select(Task).where(
            *self._task_lifecycle_stable_predicates(generation)
        )
        if for_update:
            statement = statement.with_for_update()
        return (await db.execute(statement)).scalar_one_or_none()

    @staticmethod
    def _task_status_generation_predicates(
        generation: _TaskStatusGeneration,
    ) -> list:
        """Build the exact SQL fence for one durable Task generation."""

        return [
            Task.id == generation.task_id,
            (
                Task.worker_id.is_(None)
                if generation.worker_id is None
                else Task.worker_id == generation.worker_id
            ),
            (
                Task.shared_from_id.is_(None)
                if generation.shared_from_id is None
                else Task.shared_from_id == generation.shared_from_id
            ),
            Task.status == generation.status,
            Task.retry_count == generation.retry_count,
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
        ]

    @staticmethod
    def _task_status_generation(
        task: Task,
    ) -> _TaskStatusGeneration:
        return _TaskStatusGeneration(
            task_id=task.id,
            worker_id=task.worker_id,
            shared_from_id=task.shared_from_id,
            status=task.status,
            retry_count=task.retry_count,
            instance_id=task.instance_id,
            started_at=task.started_at,
            completed_at=task.completed_at,
        )

    @staticmethod
    async def _read_task_status_generation(
        db,
        task_id: int,
    ) -> _TaskStatusGeneration | None:
        """Read DB-normalized fields after a transition and before commit."""

        row = (
            await db.execute(
                select(
                    Task.id,
                    Task.worker_id,
                    Task.shared_from_id,
                    Task.status,
                    Task.retry_count,
                    Task.instance_id,
                    Task.started_at,
                    Task.completed_at,
                ).where(Task.id == task_id)
            )
        ).one_or_none()
        if row is None:
            return None
        return _TaskStatusGeneration(
            task_id=row.id,
            worker_id=row.worker_id,
            shared_from_id=row.shared_from_id,
            status=row.status,
            retry_count=row.retry_count,
            instance_id=row.instance_id,
            started_at=row.started_at,
            completed_at=row.completed_at,
        )

    async def _publish_task_generation_events(
        self,
        generation: _TaskStatusGeneration,
        events: list[tuple[str, dict]],
        *,
        db=None,
    ) -> bool:
        """Publish events while holding a write lock on the exact result row.

        The lifecycle transition commits before WebSocket publication.  This
        second exact no-op UPDATE is the publication fence: a retry/reclaim
        must acquire the same Task row lock, so an old status event cannot
        cross a newer generation.
        """

        async def publish_with_session(session) -> bool:
            guarded = await session.execute(
                update(Task)
                .where(
                    *self._task_status_generation_predicates(generation)
                )
                .values(status=generation.status)
            )
            if not guarded.rowcount:
                await session.rollback()
                return False

            for channel, payload in events:
                try:
                    await self.broadcaster.broadcast(channel, payload)
                except Exception:
                    # Publication is best-effort.  Keeping the exact row lock
                    # until every await finishes is the correctness property;
                    # polling will repair a failed WebSocket delivery.
                    logger.exception(
                        "Failed to publish generation event for task %s",
                        generation.task_id,
                    )
            await session.commit()
            return True

        if db is not None:
            return await publish_with_session(db)
        async with self.db_factory() as publish_db:
            return await publish_with_session(publish_db)

    async def _broadcast_task_status_generation(
        self,
        generation: _TaskStatusGeneration,
        *,
        instance_id: int | None = None,
        extra: dict | None = None,
        db=None,
    ) -> bool:
        payload = {
            "event": "status_change",
            "task_id": generation.task_id,
            "new_status": generation.status,
        }
        if instance_id is not None:
            payload["instance_id"] = instance_id
        if extra:
            payload.update(extra)
        return await self._publish_task_generation_events(
            generation,
            [("tasks", payload)],
            db=db,
        )

    async def _ensure_owned_executing(
        self,
        generation: _TaskLifecycleGeneration,
    ) -> bool:
        """Confirm this mode coroutine still owns the active Task generation.

        Mode handlers are entered only after the dispatcher's durable dequeue
        claim.  Re-acquiring ``pending`` here would let an old coroutine revive
        itself after a concurrent cancel → retry cleared its ownership.
        """

        async with self.db_factory() as db:
            claimed = await db.execute(
                update(Task)
                .where(
                    *self._task_lifecycle_generation_predicates(generation)
                )
                .values(status="executing")
            )
            await db.commit()
        return bool(claimed.rowcount)

    async def _retry_or_fail_mode_task(
        self,
        generation: _TaskLifecycleGeneration,
        reason: str,
    ) -> str | None:
        async with self.db_factory() as db:
            task = await self._read_owned_lifecycle_task(
                db,
                generation,
                for_update=True,
            )
            if task is None:
                logger.info(
                    "Skipping stale retry/fail for task %s on instance %s",
                    generation.task_id,
                    generation.instance_id,
                )
                return None

            observed_generation = self._task_status_generation(task)
            if task.retry_count < task.max_retries:
                changed = await db.execute(
                    update(Task)
                    .where(
                        *self._task_status_generation_predicates(
                            observed_generation
                        ),
                        task_retry_not_superseded_predicate(),
                    )
                    .values(
                        status="pending",
                        retry_count=Task.retry_count + 1,
                        instance_id=None,
                        error_message=None,
                        started_at=None,
                        completed_at=None,
                    )
                )
                status = "pending"
            else:
                changed = await db.execute(
                    update(Task)
                    .where(
                        *self._task_status_generation_predicates(
                            observed_generation
                        ),
                        task_retry_not_superseded_predicate(),
                    )
                    .values(
                        status="failed",
                        error_message=reason,
                        completed_at=datetime.utcnow(),
                    )
                )
                status = "failed"

            if not changed.rowcount:
                await db.rollback()
                return None
            resulting_generation = await self._read_task_status_generation(
                db, generation.task_id
            )
            if resulting_generation is None:
                await db.rollback()
                return None
            await db.commit()

        await self._broadcast_task_status_generation(
            resulting_generation,
            instance_id=generation.instance_id,
        )
        return status

    async def _complete_owned_task(
        self,
        generation: _TaskLifecycleGeneration,
        *,
        count_completion: bool = False,
    ) -> bool:
        """Complete and broadcast only if this active Instance still owns it."""

        async with self.db_factory() as db:
            task = await self._read_owned_lifecycle_task(
                db,
                generation,
                for_update=True,
            )
            if task is None:
                return False
            observed_generation = self._task_status_generation(task)
            changed = await db.execute(
                update(Task)
                .where(
                    *self._task_status_generation_predicates(
                        observed_generation
                    ),
                    task_retry_not_superseded_predicate(),
                )
                .values(
                    status="completed",
                    completed_at=datetime.utcnow(),
                    error_message=None,
                )
            )
            if not changed.rowcount:
                await db.rollback()
                return False
            if count_completion:
                # Global lifecycle order is Task -> Instance.  The Task row is
                # already locked above before this accounting write.
                await db.execute(
                    update(Instance)
                    .where(Instance.id == generation.instance_id)
                    .values(
                        total_tasks_completed=Instance.total_tasks_completed + 1
                    )
                )
            resulting_generation = await self._read_task_status_generation(
                db, generation.task_id
            )
            if resulting_generation is None:
                await db.rollback()
                return False
            await db.commit()

        await self._broadcast_task_status_generation(
            resulting_generation,
            instance_id=generation.instance_id,
        )
        return True

    async def _fail_owned_task(
        self,
        generation: _TaskLifecycleGeneration,
        reason: str,
    ) -> bool:
        """Fail and broadcast only the still-active task generation."""

        async with self.db_factory() as db:
            task = await self._read_owned_lifecycle_task(
                db,
                generation,
                for_update=True,
            )
            if task is None:
                return False
            observed_generation = self._task_status_generation(task)
            changed = await db.execute(
                update(Task)
                .where(
                    *self._task_status_generation_predicates(
                        observed_generation
                    ),
                    task_retry_not_superseded_predicate(),
                )
                .values(
                    status="failed",
                    error_message=reason,
                    completed_at=datetime.utcnow(),
                )
            )
            if not changed.rowcount:
                await db.rollback()
                return False
            resulting_generation = await self._read_task_status_generation(
                db, generation.task_id
            )
            if resulting_generation is None:
                await db.rollback()
                return False
            await db.commit()

        await self._broadcast_task_status_generation(
            resulting_generation,
            instance_id=generation.instance_id,
        )
        return True

    async def _defer_account_routing_task(
        self,
        generation: _TaskLifecycleGeneration,
        reason: str,
        *,
        retry_after: float | None = None,
    ) -> None:
        task_id = generation.task_id
        instance_id = generation.instance_id
        delay = max(1.0, min(float(retry_after or CODEX_ROUTING_RETRY_DELAY), 300.0))
        # Install the exclusion before committing ``pending``.  Otherwise the
        # dispatch loop can observe the pending row during the context-manager
        # exit yield and immediately claim it again before this coroutine stores
        # the deadline.
        self._account_routing_not_before[task_id] = time.monotonic() + delay
        try:
            async with self.db_factory() as db:
                queue = TaskQueue(db)
                deferred = await queue.defer(
                    task_id,
                    reason[:500],
                    instance_id=instance_id,
                    generation_fence=self._task_lifecycle_queue_fence(
                        generation
                    ),
                )
        except BaseException:
            self._account_routing_not_before.pop(task_id, None)
            raise
        if not deferred:
            # Cancellation/deletion may race the launch failure.  Never revive a
            # terminal task merely because account routing also failed.
            self._account_routing_not_before.pop(task_id, None)
            logger.info(
                "Skipped account routing deferral for inactive task %s", task_id,
            )
            return

        await self.broadcaster.broadcast("tasks", {
            "event": "status_change",
            "task_id": task_id,
            "new_status": "pending",
            "instance_id": instance_id,
            "reason": "codex_account_wait",
            "retry_after": round(delay, 1),
        })
        logger.warning(
            "Deferred task %s for %.1fs while account routing recovers: %s",
            task_id, delay, reason,
        )

        asyncio.get_running_loop().call_later(delay, self.wake)

    async def _run_transient_retry(
        self,
        instance_id: int,
        task: Task,
        generation: _TaskLifecycleGeneration,
        cwd: str,
        git_env: dict | None,
        *,
        thinking_budget: int | None = None,
        effort_level: str | None = None,
        attempt: int = 1,
    ):
        """Wait out a transient server-side 429/overload and retry the SAME account.

        Anthropic infra throttling/overload ("Server is temporarily limiting
        requests (not your usage limit)" / overloaded) is NOT an account usage
        limit — rotating accounts wouldn't help — so we back off and --resume
        the same session, up to settings.transient_retry_max times. Once the
        failure is no longer transient (or the budget is exhausted) we hand off
        to account rotation, then to the normal retry/fail path.
        """
        from backend.services.claude_pool import is_transient_for, transient_retry_delay

        delay = transient_retry_delay(
            attempt,
            settings.transient_retry_base_delay,
            settings.transient_retry_max_delay,
        )
        logger.info(
            "Task %d transient 429/overload — waiting %.0fs before retry #%d/%d",
            task.id, delay, attempt, settings.transient_retry_max,
        )
        await self.broadcaster.broadcast(f"task:{task.id}", {
            "event_type": "transient_retry",
            "task_id": task.id,
            "attempt": attempt,
            "max_attempts": settings.transient_retry_max,
            "delay": round(delay, 1),
        })
        await asyncio.sleep(delay)

        if not await self._task_claim_is_active(generation):
            logger.info(
                "Transient retry for task %s was superseded during backoff",
                task.id,
            )
            return

        config_dir = self.instance_manager.get_config_dir(instance_id)
        async with self.db_factory() as db:
            current = await self._read_owned_lifecycle_task(db, generation)
            if current is None:
                return
            session_id = current.session_id or task.session_id

        exit_code = await self._relaunch_and_wait(
            instance_id, task, generation, cwd, git_env, config_dir, session_id,
            thinking_budget=thinking_budget, effort_level=effort_level,
            label=f"Transient retry #{attempt}",
        )
        if not await self._task_claim_is_active(generation):
            return

        # PTY mode: another transient overload also aborts with exit_code 0, so
        # the flag — not the exit code — tells us whether it recovered.
        still_transient = (
            settings.transient_retry_enabled
            and self.instance_manager.transient_error_seen(instance_id)
        )

        if exit_code in (0, -2, 130) and not still_transient:
            changed = await self._complete_owned_task(
                generation,
                count_completion=exit_code == 0,
            )
            if not changed:
                return
            logger.info("Task %d recovered after %d transient retry(ies)", task.id, attempt)
            return

        # Still failing — keep backing off while it's transient and budget
        # remains (flag covers PTY's exit_code-0 repeat; text covers stderr).
        combined = await self._collect_failure_output(instance_id, task.id)
        if (
            settings.transient_retry_enabled
            and attempt < settings.transient_retry_max
            and (
                still_transient
                or is_transient_for(task.provider, combined)
                or self.instance_manager.is_cloudrouter_transient(
                    instance_id,
                    (task.provider or "claude").lower(),
                    combined,
                ) is True
            )
        ):
            await self._run_transient_retry(
                instance_id, task, generation, cwd, git_env,
                thinking_budget=thinking_budget,
                effort_level=effort_level,
                attempt=attempt + 1,
            )
            return

        # No longer transient, or budget exhausted → account rotation, then
        # normal retry/fail. (Rotation never re-enters the transient path, so
        # there is no ping-pong between the two.)
        rotation = await self._check_rate_limit_and_rotate(
            instance_id,
            task.id,
            exit_code,
            combined=combined,
            expected_generation=generation,
        )
        if rotation:
            await self._run_pool_retry(
                instance_id, task, generation, cwd, git_env,
                rotation["config_dir"], rotation["session_id"], rotation["excluded"],
                thinking_budget=thinking_budget, effort_level=effort_level,
            )
            return

        if still_transient:
            reason = f"Transient server overload persisted after {attempt} retries"
        else:
            reason = f"Exit code: {exit_code} after {attempt} transient retry(ies)"
        await self._retry_or_fail_mode_task(generation, reason)

    async def _run_task_lifecycle(self, instance_id: int, task: Task, git_env: dict | None = None):
        """Execute the task lifecycle: assign → Claude Code → judge result.

        Claude Code handles worktree creation, git operations, and cleanup
        autonomously based on the project's CLAUDE.md instructions.
        """
        lifecycle_task = asyncio.current_task()
        process = None
        lifecycle_cancelled = False
        claim_validated = False
        # ``dequeue`` refreshes this ORM row after its claim commit.  Freeze it
        # immediately so cancellation/errors before Step 2 are fenced too;
        # Step 2 replaces it with the DB-normalized executing row.
        lifecycle_generation: _TaskLifecycleGeneration | None = (
            self._task_lifecycle_generation(task)
        )
        original_task_skills = dict(task.enabled_skills or {})
        launch_skills, has_temporary_initial_skills = (
            _initial_task_launch_skills(task)
        )
        try:
            # === Step 1: Mark in_progress ===
            await self._broadcast_task_status_generation(
                self._task_status_generation(task),
                instance_id=instance_id,
                extra={"old_status": "pending"},
            )

            # === Step 2: Determine cwd and update task ===
            # 必须是绝对路径：PTY 模式按 cwd 推导 JSONL 轮询路径，"." 会落空
            cwd = task.last_cwd or task.target_repo or os.getcwd()

            # 存量项目统一补 AGENTS.md（Codex 指令文件）：有 CLAUDE.md 而无
            # AGENTS.md 时注入 symlink，任何项目下次跑任务时自动补齐。
            # 不 commit（由 agent 的正常 git 流程带入），幂等且绝不阻断任务。
            from backend.services.agent_docs import ensure_agents_md
            ensure_agents_md(task.target_repo or cwd)
            thinking_budget = task.thinking_budget
            effort_level = task.effort_level or settings.default_effort
            async with self.db_factory() as db:
                claim_values: dict = {
                    "status": "executing",
                    "instance_id": instance_id,
                }
                if has_temporary_initial_skills:
                    # The API checks Task.enabled_skills when the model calls
                    # create_sub_agent. Publish the temporary command skill in
                    # the same transaction as execution ownership.
                    claim_values["enabled_skills"] = launch_skills
                claimed = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task.id,
                        Task.status == "in_progress",
                        Task.retry_count == task.retry_count,
                        Task.instance_id == instance_id,
                        (
                            Task.started_at.is_(None)
                            if task.started_at is None
                            else Task.started_at == task.started_at
                        ),
                        (
                            Task.completed_at.is_(None)
                            if task.completed_at is None
                            else Task.completed_at == task.completed_at
                        ),
                        Task.worker_id.is_(None),
                        Task.shared_from_id.is_(None),
                        task_retry_not_superseded_predicate(),
                    )
                    .values(**claim_values)
                )
                executing_generation = None
                if claimed.rowcount:
                    executing_generation = (
                        await self._read_task_status_generation(db, task.id)
                    )
                await db.commit()
            if not claimed.rowcount or executing_generation is None:
                logger.info(
                    "Task %s launch claim on instance %s was superseded",
                    task.id, instance_id,
                )
                return
            lifecycle_generation = self._task_lifecycle_generation(
                executing_generation
            )
            if has_temporary_initial_skills:
                task.enabled_skills = launch_skills
            claim_validated = True
            await self._broadcast_task_status_generation(
                executing_generation,
                instance_id=instance_id,
            )

            # === Step 3: Plan mode check ===
            if task.mode == "plan" and not task.plan_approved:
                await self._run_plan_phase(
                    instance_id,
                    task,
                    lifecycle_generation,
                    cwd,
                    git_env,
                    effort_level=effort_level,
                )
                return

            # === Step 3b: Loop mode ===
            if task.mode == "loop":
                await self._run_loop_lifecycle(
                    instance_id,
                    task,
                    lifecycle_generation,
                    cwd,
                    git_env,
                    effort_level=effort_level,
                )
                return

            # === Step 3c: Goal mode ===
            if task.mode == "goal":
                await self._run_goal_lifecycle(
                    instance_id,
                    task,
                    lifecycle_generation,
                    cwd,
                    git_env,
                    effort_level=effort_level,
                )
                return

            # === Step 4: Launch Claude Code ===
            full_prompt = await self._build_task_prompt(task)

            # Pool: select an account for this launch. For a resume (retry of a
            # task that already has a session) this also anchors to the session's
            # resident dir when the pool is exhausted, so --resume doesn't miss
            # the JSONL and hard-fail with "No conversation found" (prod #734/#740).
            pool_config_dir = await self._resolve_resume_config_dir(
                task.session_id,
                task.provider,
                task_id=task.id,
                expected_generation=lifecycle_generation,
                **({"model": task.model} if task.model else {}),
            )

            if not await self._task_claim_is_active(lifecycle_generation):
                logger.info(
                    "Task %s launch was superseded during account resolution",
                    task.id,
                )
                return

            await self.instance_manager.launch(
                instance_id=instance_id,
                prompt=full_prompt,
                task_id=task.id,
                cwd=cwd,
                model=task.model,
                resume_session_id=task.session_id,
                git_env=git_env or {},
                thinking_budget=thinking_budget,
                effort_level=effort_level,
                provider=task.provider,
                config_dir=pool_config_dir,
                enable_workflows=task.enable_workflows,
                enabled_skills=task.enabled_skills,
                system_prompt_mode=task.system_prompt_mode,
            )

            # Wait for process to finish (with timeout)
            process = self.instance_manager.processes.get(instance_id)
            pty_managed_turn = self.instance_manager.is_pty_managed_turn(
                instance_id, process
            )
            if process:
                await self._wait_process(
                    process, task, "Task run", instance_id=instance_id
                )

            # Wait for output consumer to finish processing all remaining
            # buffered output before judging the result. Without this the
            # task can be marked completed while the last chunk of Claude's
            # reply is still being parsed/broadcast.
            await self._wait_output_consumer(
                instance_id, task, "Task run", process
            )

            exit_code = self._effective_process_exit_code(instance_id, process)

            # === Step 5: Judge result ===
            if not await self._task_claim_is_active(lifecycle_generation):
                logger.info(
                    "Task %s lifecycle was superseded before result "
                    "classification",
                    task.id,
                )
                return

            # SIGINT (exit code -2 or 130) means user interrupted — not a failure.
            # Keep session alive so user can resume via chat.
            interrupted = exit_code in (-2, 130)
            if interrupted:
                logger.info(f"Task {task.id} was interrupted by user (exit_code={exit_code})")
                await self._complete_owned_task(lifecycle_generation)
                return

            # PTY mode aborts a transient-429/overload turn but keeps the
            # persistent session alive, so it reports exit_code 0. The per-turn
            # flag (set in _process_event) is the reliable cross-mode signal →
            # wait + retry the same account before judging success/failure.
            if settings.transient_retry_enabled and self.instance_manager.transient_error_seen(instance_id):
                await self._run_transient_retry(
                    instance_id, task, lifecycle_generation, cwd, git_env,
                    thinking_budget=thinking_budget,
                    effort_level=effort_level,
                )
                return

            # PTY proactive pool switch: turn finished OK but an actionable
            # rate_limit_event was observed → migrate session to a healthy
            # account before judging success (next retry/turn uses fresh quota).
            if (
                pty_managed_turn
                and self.instance_manager.pty_rate_limit_seen(instance_id)
            ):
                await self.instance_manager._try_proactive_pool_switch(
                    instance_id,
                    task.id,
                    rate_limit_info=self.instance_manager.pty_rate_limit_info(
                        instance_id
                    ),
                    expected_generation=lifecycle_generation,
                )
                self.instance_manager.clear_pty_rate_limit(instance_id)

            if exit_code != 0:
                from backend.services.claude_pool import is_transient_for
                combined = await self._collect_failure_output(instance_id, task.id)

                # Transient overload that only surfaced on stderr (subprocess
                # mode) — the flag above may miss it, so re-check the text.
                if settings.transient_retry_enabled and (
                    is_transient_for(task.provider, combined)
                    or self.instance_manager.is_cloudrouter_transient(
                        instance_id,
                        (task.provider or "claude").lower(),
                        combined,
                    ) is True
                ):
                    await self._run_transient_retry(
                        instance_id, task, lifecycle_generation, cwd, git_env,
                        thinking_budget=thinking_budget,
                        effort_level=effort_level,
                    )
                    return

                # Account usage-limit / auth-failure → rotate account and resume
                rotation = await self._check_rate_limit_and_rotate(
                    instance_id,
                    task.id,
                    exit_code,
                    combined=combined,
                    expected_generation=lifecycle_generation,
                )
                if rotation:
                    await self._run_pool_retry(
                        instance_id, task, lifecycle_generation, cwd, git_env,
                        rotation["config_dir"], rotation["session_id"],
                        rotation["excluded"],
                        thinking_budget=thinking_budget,
                        effort_level=effort_level,
                    )
                    return

                # Context overflow — compact and retry instead of failing.
                # Codex app-server uses a structured contextWindowExceeded
                # code while exec/Claude may only expose human-readable text.
                if is_context_window_exceeded(task.provider, combined):
                    try:
                        async with self.db_factory() as db:
                            t = await self._read_owned_lifecycle_task(
                                db,
                                lifecycle_generation,
                            )
                            if t and t.session_id:
                                logger.warning(
                                    "Task %d exceeded its context window, "
                                    "compacting session",
                                    task.id,
                                )
                                summary = await self._compact_session(task.id, t.session_id, db)
                                if summary:
                                    compacted = await db.execute(
                                        update(Task)
                                        .where(
                                            *self._task_lifecycle_generation_predicates(
                                                lifecycle_generation,
                                                statuses=("executing",),
                                            )
                                        )
                                        .values(
                                            session_id=None,
                                            context_window_usage=None,
                                            status="pending",
                                            instance_id=None,
                                            description=build_compacted_task_retry_prompt(
                                                summary
                                            ),
                                        )
                                    )
                                    await db.commit()
                                    if compacted.rowcount:
                                        await self.broadcaster.broadcast("tasks", {
                                            "event": "status_change",
                                            "task_id": task.id,
                                            "new_status": "pending",
                                            "instance_id": instance_id,
                                        })
                                    return
                    except Exception:
                        logger.exception(
                            "Context-window compaction failed for task %d",
                            task.id,
                        )

                await self._retry_or_fail_mode_task(
                    lifecycle_generation,
                    f"Exit code: {exit_code}",
                )
                return

            # === Claude Code completed successfully ===
            completed = await self._complete_owned_task(
                lifecycle_generation,
                count_completion=True,
            )
            if not completed:
                return

            logger.info(f"Task {task.id} ({task.title}) completed successfully on instance {instance_id}")

            await self._handle_pr_review_completion(task)

        except asyncio.CancelledError:
            lifecycle_cancelled = True
            logger.info(f"Lifecycle cancelled for task {task.id} on instance {instance_id}")
            if not self._shutting_down:
                async with self.db_factory() as db:
                    deferred = await TaskQueue(db).defer(
                        task.id,
                        "dispatcher stopped",
                        instance_id=instance_id,
                        generation_fence=(
                            self._task_lifecycle_queue_fence(
                                lifecycle_generation
                            )
                            if lifecycle_generation is not None
                            else None
                        ),
                    )
                if deferred:
                    from backend.services.task_events import broadcast_status_change
                    await broadcast_status_change(task.id, "pending", instance_id)
            raise
        except TaskLifecycleSupersededError:
            logger.info(
                "Lifecycle routing side effect for task %s on instance %s "
                "lost its immutable generation",
                task.id,
                instance_id,
            )
            return
        except ClaudeAccountRoutingError as e:
            if e.permanent:
                logger.error(
                    "Permanent Claude account routing error for task %s: %s",
                    task.id,
                    e,
                )
                if lifecycle_generation is not None:
                    await self._fail_owned_task(
                        lifecycle_generation,
                        str(e)[:500],
                    )
            elif lifecycle_generation is not None:
                await self._defer_account_routing_task(
                    lifecycle_generation,
                    str(e),
                    retry_after=e.retry_after,
                )
        except CodexAccountRoutingError as e:
            if e.permanent:
                logger.error(
                    "Permanent Codex account routing error for task %s: %s",
                    task.id, e,
                )
                if lifecycle_generation is not None:
                    await self._fail_owned_task(
                        lifecycle_generation,
                        str(e)[:500],
                    )
            else:
                if lifecycle_generation is not None:
                    await self._defer_account_routing_task(
                        lifecycle_generation,
                        str(e),
                        retry_after=e.retry_after,
                    )
        except Exception as e:
            from backend.services.codex_app_server import (
                CodexAppServerBusyError,
                CodexThreadHomeMismatchError,
            )

            if isinstance(e, (CodexAppServerBusyError, CodexThreadHomeMismatchError)):
                if lifecycle_generation is not None:
                    await self._defer_account_routing_task(
                        lifecycle_generation,
                        str(e),
                    )
                return
            if isinstance(e, InstanceAlreadyRunningError):
                async with self.db_factory() as db:
                    deferred = await TaskQueue(db).defer(
                        task.id,
                        f"instance admission race: {e}"[:500],
                        instance_id=instance_id,
                        generation_fence=(
                            self._task_lifecycle_queue_fence(
                                lifecycle_generation
                            )
                            if lifecycle_generation is not None
                            else None
                        ),
                    )
                if deferred:
                    from backend.services.task_events import broadcast_status_change
                    await broadcast_status_change(task.id, "pending", instance_id)
                return
            logger.error(f"Lifecycle error for task {task.id}: {e}", exc_info=True)
            if lifecycle_generation is not None:
                failed = await self._fail_owned_task(
                    lifecycle_generation,
                    str(e)[:500],
                )
                if failed:
                    await self._handle_pr_review_failure(task, str(e))
        finally:
            from backend.services.mcp_config import cleanup_mcp_config
            cleanup_mcp_config(task.id)
            _cleanup_skill_prompt_files(task.id)
            if has_temporary_initial_skills:
                try:
                    async with self.db_factory() as db:
                        current = await db.get(Task, task.id)
                        if (
                            current is not None
                            and dict(current.enabled_skills or {}) == launch_skills
                        ):
                            current.enabled_skills = original_task_skills
                            await db.commit()
                except Exception:
                    logger.exception(
                        "Failed to restore initial command skills for task %s",
                        task.id,
                    )
            try:
                # Keep the lifecycle registered through exact stale cleanup.
                # The queued-chat admission path treats this registration as a
                # live generation; popping first would let completed ->
                # executing recreate the same Task tuple before old cleanup
                # reaches its generation fence (same-task/same-slot ABA).
                if (
                    claim_validated
                    and lifecycle_generation is not None
                    and not (lifecycle_cancelled and self._shutting_down)
                ):
                    reset, reset_cancellation = (
                        await _settle_despite_cancellation(
                            self._reset_instance_if_stale(
                                instance_id,
                                lifecycle_generation,
                            )
                        )
                    )
                    reset.result()
                    if reset_cancellation is not None:
                        raise reset_cancellation
            finally:
                if self._running_tasks.get(instance_id) is lifecycle_task:
                    self._running_tasks.pop(instance_id, None)

    async def _handle_pr_review_completion(self, task: Task):
        meta = task.metadata_ or {}
        pr_review_id = meta.get("pr_review_id")
        if not pr_review_id:
            return
        try:
            from backend.models.pr_monitor import MonitoredRepo, PRReview
            from backend.services.pr_review_service import check_and_update_review
            async with self.db_factory() as db:
                review = await db.get(PRReview, pr_review_id)
                if not review:
                    return
                repo = await db.get(MonitoredRepo, review.repo_id)
                if not repo:
                    return
                await check_and_update_review(db, pr_review_id, repo.repo_full_name)
        except Exception as e:
            logger.error(f"PR review completion handler error: {e}", exc_info=True)

    async def _handle_pr_review_failure(self, task: Task, error: str):
        meta = task.metadata_ or {}
        pr_review_id = meta.get("pr_review_id")
        if not pr_review_id:
            return
        try:
            from backend.models.pr_monitor import PRReview
            from datetime import datetime
            async with self.db_factory() as db:
                failed = await db.execute(
                    update(PRReview)
                    .where(
                        PRReview.id == pr_review_id,
                        PRReview.task_id == task.id,
                        PRReview.status.in_(("pending", "reviewing")),
                    )
                    .values(
                        status="error",
                        action_taken="error",
                        review_summary=f"Task failed: {error[:500]}",
                        completed_at=datetime.utcnow(),
                    )
                )
                await db.commit()
                if failed.rowcount:
                    await self.broadcaster.broadcast("pr-monitor", {
                        "type": "review_updated",
                        "review_id": pr_review_id,
                        "status": "error",
                    })
        except Exception as e:
            logger.error(f"PR review failure handler error: {e}", exc_info=True)

    async def _reset_instance_if_stale(
        self,
        instance_id: int,
        generation: _TaskLifecycleGeneration,
    ):
        """Safety-reset only the exact inactive owner generation.

        A reusable Instance may already have a new owner by the time an older
        lifecycle reaches ``finally``.  Ownership predicates prevent that old
        cleanup from erasing the new PID/current_task_id.  The manager lock
        closes the smaller race where a new launch starts between checking the
        in-memory process and committing the CAS updates.  The transaction
        locks and writes Task before Instance, matching every other lifecycle
        path and preventing a cross-path deadlock.
        """
        try:
            if generation.instance_id != instance_id:
                return
            lifecycle_lock = self.instance_manager._instance_lifecycle_lock(
                instance_id
            )
            async with lifecycle_lock:
                # ``is_running`` covers more exact-generation evidence than the
                # parent process map alone: a terminal parent may still have a
                # live output consumer, descendant process group, container
                # exec, or recovery-pending record.
                running_result = self.instance_manager.is_running(instance_id)
                if isinstance(running_result, bool) and running_result:
                    return
                current_process = self.instance_manager.processes.get(instance_id)
                if (
                    current_process is not None
                    and current_process.returncode is None
                ):
                    return

                async with self.db_factory() as db:
                    # Global database lock order is Task -> Instance.  Do not
                    # use db.get(Instance) before acquiring the Task row.
                    task_owner = await self._read_same_lifecycle_task(
                        db,
                        generation,
                        for_update=True,
                    )
                    if task_owner is None:
                        return

                    owner = (
                        await db.execute(
                            select(Instance)
                            .where(Instance.id == instance_id)
                            .with_for_update()
                        )
                    ).scalar_one_or_none()
                    if (
                        owner is not None
                        and owner.current_task_id
                        not in (None, generation.task_id)
                    ):
                        return

                    resulting_generation = None
                    task_reset = None
                    if (
                        task_owner is not None
                        and task_owner.status in ("executing", "in_progress")
                    ):
                        observed_task = self._task_status_generation(task_owner)
                        task_reset = await db.execute(
                            update(Task)
                            .where(
                                *self._task_status_generation_predicates(
                                    observed_task
                                ),
                                task_retry_not_superseded_predicate(),
                            )
                            .values(
                                status="completed",
                                completed_at=datetime.utcnow(),
                                error_message=None,
                            )
                        )
                        if not task_reset.rowcount:
                            await db.rollback()
                            return

                    if owner is None:
                        await db.rollback()
                        return
                    instance_predicates = [
                        Instance.id == instance_id,
                        Instance.status == "running",
                        (
                            Instance.current_task_id.is_(None)
                            if owner.current_task_id is None
                            else Instance.current_task_id
                            == owner.current_task_id
                        ),
                        (
                            Instance.pid.is_(None)
                            if owner.pid is None
                            else Instance.pid == owner.pid
                        ),
                        (
                            Instance.started_at.is_(None)
                            if owner.started_at is None
                            else Instance.started_at == owner.started_at
                        ),
                    ]
                    instance_reset = await db.execute(
                        update(Instance)
                        .where(*instance_predicates)
                        .values(status="idle", current_task_id=None, pid=None)
                    )
                    if not instance_reset.rowcount:
                        # The Task transition above belongs to the same
                        # transaction and is rolled back with a newer Instance.
                        await db.rollback()
                        return
                    if task_reset is not None:
                        resulting_generation = (
                            await self._read_task_status_generation(
                                db,
                                generation.task_id,
                            )
                        )
                        if resulting_generation is None:
                            await db.rollback()
                            return
                    await db.commit()
                if instance_reset.rowcount:
                    logger.warning(
                        "Safety reset inactive owner: instance %s / task %s",
                        instance_id, generation.task_id,
                    )
                if resulting_generation is not None:
                    await self._broadcast_task_status_generation(
                        resulting_generation,
                        instance_id=instance_id,
                    )
        except Exception:
            logger.exception(
                "Failed to safety-reset instance %s / task %s",
                instance_id,
                generation.task_id,
            )

    async def _run_pool_retry(
        self,
        instance_id: int,
        task: Task,
        generation: _TaskLifecycleGeneration,
        cwd: str,
        git_env: dict | None,
        config_dir: str,
        session_id: str | None,
        excluded: set[str],
        *,
        thinking_budget: int | None = None,
        effort_level: str | None = None,
        max_rotations: int = 5,
        _rotation_count: int = 1,
    ):
        """Resume a task on a different pool account after rate limit.

        If the new account also hits a rate limit, recurse with the accumulated
        exclusion set until max_rotations is reached or accounts are exhausted.
        """
        logger.info(
            "Pool retry #%d for task %d: switching to %s (session=%s)",
            _rotation_count, task.id, config_dir, session_id,
        )

        if not await self._task_claim_is_active(generation):
            logger.info(
                "Pool retry for task %s was superseded before relaunch",
                task.id,
            )
            return

        exit_code = await self._relaunch_and_wait(
            instance_id, task, generation, cwd, git_env, config_dir, session_id,
            thinking_budget=thinking_budget, effort_level=effort_level,
            label="Pool retry run",
        )
        if not await self._task_claim_is_active(generation):
            return

        if exit_code in (0, -2, 130):
            # Success or user interrupt
            changed = await self._complete_owned_task(
                generation,
                count_completion=exit_code == 0,
            )
            if changed and exit_code == 0:
                logger.info("Task %d completed after %d pool rotation(s)", task.id, _rotation_count)
            return

        # Failed again — try another rotation if budget remains
        if _rotation_count < max_rotations:
            rotation = await self._check_rate_limit_and_rotate(
                instance_id,
                task.id,
                exit_code,
                expected_generation=generation,
            )
            if rotation:
                merged_excluded = excluded | rotation["excluded"]
                await self._run_pool_retry(
                    instance_id, task, generation, cwd, git_env,
                    rotation["config_dir"], rotation["session_id"],
                    merged_excluded,
                    thinking_budget=thinking_budget,
                    effort_level=effort_level,
                    max_rotations=max_rotations,
                    _rotation_count=_rotation_count + 1,
                )
                return

        # Non-rotatable failure or exhausted rotations — normal retry/fail
        await self._retry_or_fail_mode_task(
            generation,
            f"Exit code: {exit_code} after {_rotation_count} pool rotation(s)",
        )

    async def _run_loop_lifecycle(
        self,
        instance_id: int,
        task: Task,
        generation: _TaskLifecycleGeneration,
        cwd: str,
        git_env: dict | None = None,
        effort_level: str | None = None,
    ):
        """Loop entry: run iterations, then always release the PTY session.

        In PTY mode the whole loop shares one hot session (one iteration ==
        one turn); releasing it afterwards keeps the pool free of one-shot
        leftovers. No-op in -p mode.
        """
        if not await self._ensure_owned_executing(generation):
            return
        try:
            await self._run_loop_iterations(
                instance_id,
                task,
                generation,
                cwd,
                git_env,
                effort_level=effort_level,
            )
        finally:
            try:
                async with self.db_factory() as db:
                    t = await self._read_same_lifecycle_task(
                        db,
                        generation,
                    )
                    sid = t.session_id if t else None
                if sid:
                    release = getattr(self.instance_manager, "release_pty_session", None)
                    if release is not None:
                        result = release(sid)
                        import inspect as _inspect
                        if _inspect.isawaitable(result):
                            await result
            except Exception:
                logger.exception("Failed to release loop PTY session for task %d", task.id)

    async def _run_loop_iterations(
        self,
        instance_id: int,
        task: Task,
        generation: _TaskLifecycleGeneration,
        cwd: str,
        git_env: dict | None = None,
        effort_level: str | None = None,
    ):
        """Loop: repeatedly invoke Claude Code until it signals done or abort.

        Each iteration starts a fresh Claude Code subprocess. Claude reads the todo
        file itself, executes the next item, marks it done, then writes a signal file
        telling us whether to continue, stop (done), or give up (abort).
        The backend never parses the todo file — Claude owns that logic entirely.
        """
        import json
        from pathlib import Path

        signal_path = Path(cwd) / ".claude-manager" / f"loop_signal_{task.id}.json"
        signal_path.parent.mkdir(parents=True, exist_ok=True)

        iteration = 0
        history: list[dict] = []
        anchored_total: int | None = None
        plan: str | None = None

        max_iterations = task.max_iterations or 50

        while True:
            # Check if task was cancelled or deleted externally between iterations
            async with self.db_factory() as db:
                t = await self._read_owned_lifecycle_task(db, generation)
                if not t:
                    logger.info(
                        "Loop task %s generation was superseded, stopping",
                        task.id,
                    )
                    return
                # 每轮刷新可变设置（用户可能在 Config 面板中修改了
                # model/effort/thinking/timeout，下一轮立即生效）
                task = t

            # Enforce max iterations limit
            if iteration >= max_iterations:
                if task.must_complete:
                    last_progress = history[-1]["progress"] if history else "unknown"
                    fail_msg = f"未能在 {max_iterations} 轮内完成所有任务项（当前进度: {last_progress}）"
                else:
                    fail_msg = f"超出最大迭代次数限制 ({max_iterations})"
                await self._fail_owned_task(generation, fail_msg)
                logger.warning(f"Loop task {task.id} exceeded max iterations ({max_iterations}), aborting")
                return

            # Clear signal file so we can detect if Claude fails to write one
            signal_path.unlink(missing_ok=True)

            prompt = self._build_loop_prompt(
                task, iteration, str(signal_path), history, anchored_total, plan,
            )

            # PTY mode: iterations after the first reuse the same hot session
            # (one iteration == one turn) — no cold start, continuous context.
            # -p mode keeps its stateless-per-iteration semantics (no resume).
            resume_sid = None
            if iteration > 0 and (
                (task.provider or "claude").lower() == "codex"
                or getattr(self.instance_manager, "pty_mode_enabled", False)
            ):
                resume_sid = task.session_id

            # Pool: pick the account for this iteration (mirrors the non-loop
            # Step 4 path). Without this, loop launches passed config_dir=None
            # and silently inherited the hardcoded systemd CLAUDE_CONFIG_DIR —
            # the pool was never consulted, cooled-down accounts were never
            # avoided, and a PTY resume on iteration>0 could hit the wrong
            # account and die with "No conversation found". For a resume it
            # anchors to the session's resident account (no config_dir drift →
            # PTY hot session preserved); fresh iterations get a healthy pick.
            config_dir = await self._resolve_resume_config_dir(
                resume_sid,
                task.provider,
                task_id=task.id,
                expected_generation=generation,
                **({"model": task.model} if task.model else {}),
            )

            iteration_exit_code, config_dir = await self._launch_mode_turn_with_rotation(
                instance_id,
                task,
                generation,
                cwd,
                git_env,
                prompt=prompt,
                config_dir=config_dir,
                resume_session_id=resume_sid,
                loop_iteration=iteration,
                effort_level=effort_level,
                label="Loop iteration",
            )

            # P1: Check if task was cancelled/deleted while the iteration was running
            async with self.db_factory() as db:
                t = await self._read_owned_lifecycle_task(db, generation)
                if not t:
                    logger.info(
                        "Loop task %s generation was superseded during "
                        "iteration %s, stopping",
                        task.id,
                        iteration,
                    )
                    return
                task = t

            if iteration_exit_code not in (0, -2, 130):
                signal = {
                    "action": "abort",
                    "reason": f"Loop iteration process failed (exit code {iteration_exit_code})",
                }
            else:
                signal = self._read_loop_signal(signal_path)

            # P0: If signal is missing, attempt one resume to ask Claude to write it
            if (
                iteration_exit_code == 0
                and signal.get("reason") == "Signal file missing or invalid JSON"
            ):
                signal = await self._resume_fix_signal(
                    instance_id,
                    task,
                    generation,
                    cwd,
                    signal_path,
                    iteration,
                    git_env or {},
                    effort_level=effort_level,
                )

            # Update loop_progress from signal (Claude's self-reported progress string)
            if signal.get("progress"):
                async with self.db_factory() as db:
                    progress_updated = await db.execute(
                        update(Task)
                        .where(
                            *self._task_lifecycle_generation_predicates(
                                generation
                            )
                        )
                        .values(loop_progress=signal["progress"])
                    )
                    await db.commit()
                if not progress_updated.rowcount:
                    logger.info(
                        "Loop task %s generation was superseded before "
                        "progress publication",
                        task.id,
                    )
                    return

            # Anchor total from the first progress report so subsequent iterations stay consistent
            progress_str = signal.get("progress", "")
            if progress_str and anchored_total is None:
                try:
                    anchored_total = int(progress_str.split("/")[1])
                except (IndexError, ValueError):
                    pass

            # Capture plan from signal (latest plan overwrites previous)
            if signal.get("plan"):
                plan = signal["plan"]

            # Collect iteration history for subsequent prompts
            history.append({
                "iteration": iteration + 1,
                "progress": progress_str,
                "summary": signal.get("summary", ""),
            })

            # Broadcast iteration result so frontend can update the panel header
            await self.broadcaster.broadcast(f"task:{task.id}", {
                "event": "loop_iteration_end",
                "iteration": iteration,
                "action": signal.get("action", "abort"),
                "reason": signal.get("reason", ""),
                "progress": signal.get("progress"),
            })

            action = signal.get("action")

            # must_complete: reject "done" if progress shows incomplete
            if action == "done" and task.must_complete and anchored_total is not None:
                try:
                    numerator = int(progress_str.split("/")[0])
                except (IndexError, ValueError):
                    numerator = None
                if numerator is not None and numerator < anchored_total:
                    logger.info(
                        f"Loop task {task.id} rejected premature done "
                        f"(progress {progress_str}, need {anchored_total}), forcing continue"
                    )
                    iteration += 1
                    continue

            if action == "continue":
                iteration += 1
                continue

            elif action == "done":
                await self._complete_owned_task(
                    generation,
                    count_completion=True,
                )
                logger.info(f"Loop task {task.id} completed after {iteration + 1} iteration(s)")
                break

            else:
                # "abort" or missing/malformed signal — P1: retry if attempts remain
                reason = signal.get("reason") or "Claude did not write a valid loop signal"
                status = await self._retry_or_fail_mode_task(
                    generation,
                    reason,
                )
                logger.warning(
                    "Loop task %s aborted at iteration %s -> %s: %s",
                    task.id, iteration, status or "superseded", reason,
                )
                break

        signal_path.unlink(missing_ok=True)

    # ------------------------------------------------------------------ #
    #                       Goal mode lifecycle                           #
    # ------------------------------------------------------------------ #

    async def _evaluate_goal_with_rotation(
        self,
        evaluator,
        task: Task,
        generation: _TaskLifecycleGeneration,
        instance_id: int,
        conversation_summary: str,
        codex_home: str | None,
    ):
        """Evaluate once, retrying a Codex usage/auth failure on a new account.

        Evaluation is part of the current goal turn, not a new agent turn.  A
        pool rotation therefore retries only the ephemeral evaluator and does
        not advance ``goal_turns_used`` or rerun the goal prompt.
        """
        from backend.services.goal_evaluator import GoalEvaluationError

        provider = (task.provider or "claude").lower()
        evaluator_model = task.goal_evaluator_model or (
            settings.default_codex_goal_evaluator_model
            if provider == "codex"
            else settings.default_goal_evaluator_model
        )
        turn_home = codex_home
        evaluation_home = turn_home

        if provider == "codex" and self.codex_pool:
            pool = self.codex_pool
            current_usable = bool(
                turn_home
                and (
                    not pool.is_known_account(turn_home)
                    or (
                        pool.is_home_available(turn_home)
                        and pool.supports_model_for_home(
                            turn_home, evaluator_model
                        )
                    )
                )
            )
            if not current_usable:
                evaluation_home = pool.select(model=evaluator_model)
                if evaluation_home is None:
                    detail = (
                        "no enabled account supports"
                        if not pool.has_compatible_enabled_account(
                            evaluator_model
                        )
                        else "no compatible pool account is currently available for"
                    )
                    raise GoalEvaluationError(
                        f"Codex pool has {detail} goal evaluator model "
                        f"{evaluator_model!r}",
                        provider=provider,
                    )
        elif provider == "claude" and self.pool:
            pool = self.pool
            current_usable = bool(
                turn_home
                and (
                    not pool.is_known_account(turn_home)
                    or (
                        pool.is_config_dir_available(turn_home)
                        and pool.supports_model_for_config_dir(
                            turn_home, evaluator_model
                        )
                    )
                )
            )
            if not current_usable:
                evaluation_home = pool.select(
                    validate=False,
                    model=evaluator_model,
                )
                if evaluation_home is None:
                    detail = (
                        "no enabled account supports"
                        if not pool.has_compatible_enabled_account(
                            evaluator_model
                        )
                        else "no compatible pool account is currently available for"
                    )
                    raise GoalEvaluationError(
                        f"Claude pool has {detail} goal evaluator model "
                        f"{evaluator_model!r}",
                        provider=provider,
                    )

        def record_evaluator_route() -> None:
            if not evaluation_home:
                return
            if provider == "codex" and self.codex_pool:
                self.codex_pool.record_routed_account(evaluation_home)
            elif provider == "claude" and self.pool:
                self.pool.record_routed_account(evaluation_home)

        for rotation_attempt in range(2):
            if not await self._task_claim_is_active(generation):
                raise GoalEvaluationError(
                    "Goal task generation was superseded before evaluation",
                    provider=provider,
                )
            try:
                result = await evaluator.evaluate(
                    condition=task.goal_condition,
                    conversation_summary=conversation_summary,
                    model=task.goal_evaluator_model,
                    provider=provider,
                    codex_home=evaluation_home,
                    task_id=task.id,
                    config_dir=evaluation_home,
                    cloudrouter_store=self.cloudrouter_store,
                )
                record_evaluator_route()
                return result, turn_home
            except GoalEvaluationError as exc:
                # A concrete return code proves the evaluator process spawned;
                # failed provider turns still count as real recent usage.
                if exc.returncode is not None:
                    record_evaluator_route()
                if (
                    provider != "codex"
                    or rotation_attempt > 0
                ):
                    raise
                if not await self._task_claim_is_active(generation):
                    raise
                # An evaluator may deliberately use a different API account
                # because the task's resident account does not support the
                # evaluator model.  Never run the main-session migration path
                # against that ephemeral evaluator home.
                if evaluation_home != turn_home:
                    raise

                classifier_exit_code = (
                    exc.returncode
                    if isinstance(exc.returncode, int) and exc.returncode not in (0, -2, 130)
                    else 1
                )
                rotation = await self._check_rate_limit_and_rotate(
                    instance_id,
                    task.id,
                    classifier_exit_code,
                    combined=exc.combined_output,
                    expected_generation=generation,
                )
                if not rotation:
                    raise

                evaluation_home = rotation.get("config_dir")
                if not evaluation_home:
                    raise
                turn_home = evaluation_home
                logger.info(
                    "Goal evaluator for task %s rotating Codex account and retrying",
                    task.id,
                )

        raise RuntimeError("unreachable goal evaluator retry state")

    async def _run_goal_lifecycle(
        self,
        instance_id: int,
        task: Task,
        generation: _TaskLifecycleGeneration,
        cwd: str,
        git_env: dict | None = None,
        effort_level: str | None = None,
    ):
        """Goal mode: repeatedly invoke Claude Code until an evaluator confirms
        the goal condition is met.

        Uses --resume to keep the same session across turns, preserving full
        context. After each turn, a lightweight evaluator model judges the
        conversation transcript against the goal condition.
        """
        if not await self._ensure_owned_executing(generation):
            return
        from backend.services.goal_evaluator import GoalEvaluationError, GoalEvaluator

        evaluator = GoalEvaluator()
        turn = max(0, int(task.goal_turns_used or 0))
        max_turns = task.goal_max_turns or 30
        session_id: str | None = task.session_id
        last_reason = task.goal_last_reason or ""

        while True:
            # Check if task was cancelled or deleted externally between turns
            async with self.db_factory() as db:
                t = await self._read_owned_lifecycle_task(db, generation)
                if not t:
                    logger.info(
                        "Goal task %s generation was superseded, stopping",
                        task.id,
                    )
                    return
                # 每轮刷新可变设置（model/effort/thinking/timeout 下一轮生效）
                task = t
                turn = max(turn, int(t.goal_turns_used or 0))
                session_id = t.session_id or session_id
                last_reason = t.goal_last_reason or last_reason
                max_turns = t.goal_max_turns or 30

            if turn >= max_turns:
                break

            if turn == 0 and not session_id:
                turn_prompt = self._build_goal_initial_prompt(task)
                turn_resume_session = None
                # Pool: pick a healthy account for the fresh session (mirrors the
                # non-goal Step 4 path). Without this, goal launches passed
                # config_dir=None and silently inherited the hardcoded systemd
                # CLAUDE_CONFIG_DIR — the pool was never consulted. See loop fix
                # (#770); goal had the identical gap.
                config_dir = await self._resolve_resume_config_dir(
                    None,
                    task.provider,
                    task_id=task.id,
                    expected_generation=generation,
                    **({"model": task.model} if task.model else {}),
                )
            else:
                resume_reason = last_reason or "上一轮未能完成评估，请检查当前进度并继续完成目标。"
                turn_prompt = self._build_goal_followup_prompt(resume_reason, turn, max_turns)
                turn_resume_session = session_id
                # Resume on the session's resident account (no config_dir drift →
                # PTY hot session preserved); migrate / fall back if cooled down.
                config_dir = await self._resolve_resume_config_dir(
                    session_id,
                    task.provider,
                    task_id=task.id,
                    expected_generation=generation,
                    **({"model": task.model} if task.model else {}),
                )

            turn_exit_code, config_dir = await self._launch_mode_turn_with_rotation(
                instance_id,
                task,
                generation,
                cwd,
                git_env,
                prompt=turn_prompt,
                config_dir=config_dir,
                resume_session_id=turn_resume_session,
                loop_iteration=turn,
                effort_level=effort_level,
                label="Goal turn",
            )

            # Check if cancelled/deleted during execution
            async with self.db_factory() as db:
                t = await self._read_owned_lifecycle_task(db, generation)
                if not t:
                    logger.info(
                        "Goal task %s generation was superseded during turn "
                        "%s, stopping",
                        task.id,
                        turn,
                    )
                    return
                task = t
                if t.session_id:
                    session_id = t.session_id

            if turn_exit_code not in (0, -2, 130):
                await self._retry_or_fail_mode_task(
                    generation,
                    f"Goal turn failed (exit code {turn_exit_code})",
                )
                return
            if turn_exit_code in (-2, 130):
                return

            # Collect conversation summary for evaluator
            conversation_summary = await self._collect_goal_conversation(task.id, turn)

            # Evaluate goal condition. Operational failures are distinct from
            # an actual "not achieved" verdict, so they never consume a goal
            # turn. Codex usage/auth failures rotate and retry the evaluator on
            # another account without rerunning the agent turn.
            try:
                eval_result, config_dir = await self._evaluate_goal_with_rotation(
                    evaluator,
                    task,
                    generation,
                    instance_id,
                    conversation_summary,
                    config_dir,
                )
            except GoalEvaluationError as exc:
                await self._retry_or_fail_mode_task(
                    generation,
                    f"Goal evaluation failed: {exc}",
                )
                return

            turn += 1
            last_reason = eval_result.reason

            # Update progress in DB
            async with self.db_factory() as db:
                progress_updated = await db.execute(
                    update(Task)
                    .where(
                        *self._task_lifecycle_generation_predicates(
                            generation
                        )
                    )
                    .values(
                        goal_turns_used=turn,
                        goal_last_reason=eval_result.reason,
                    )
                )
                await db.commit()
            if not progress_updated.rowcount:
                logger.info(
                    "Goal task %s generation was superseded before progress "
                    "publication",
                    task.id,
                )
                return

            # Broadcast evaluation result
            await self.broadcaster.broadcast(f"task:{task.id}", {
                "event_type": "goal_evaluation",
                "turn": turn,
                "max_turns": max_turns,
                "achieved": eval_result.achieved,
                "reason": eval_result.reason,
            })
            await self.broadcaster.broadcast("tasks", {
                "event": "goal_evaluation",
                "task_id": task.id,
                "turn": turn,
                "achieved": eval_result.achieved,
            })

            if eval_result.achieved:
                await self._complete_owned_task(
                    generation,
                    count_completion=True,
                )
                logger.info(f"Goal task {task.id} achieved after {turn} turn(s)")
                return

        # Exceeded max turns
        fail_msg = f"未在 {max_turns} 轮内达成目标条件"
        await self._fail_owned_task(generation, fail_msg)
        logger.warning(f"Goal task {task.id} exceeded max turns ({max_turns})")

    def _build_goal_initial_prompt(self, task: Task) -> str:
        """Build the first-turn prompt for a goal task."""
        parts = [_agent_doc_preamble(task.provider)]

        metadata = task.metadata_ or {}
        image_paths = metadata.get("image_paths") or []
        if image_paths:
            image_list = "\n".join(f"- {p}" for p in image_paths)
            parts.append(f"用户提供了以下参考图片，请先用 Read 工具查看：\n{image_list}")

        parts.append(f"任务:\n{task.description}")
        parts.append(
            f"\n目标完成条件:\n{task.goal_condition}\n\n"
            f"请持续工作直到满足以上目标条件。每轮结束后，"
            f"一个独立的评估器会检查你的工作是否已达成目标。"
            f"你有最多 {task.goal_max_turns or 30} 轮来完成。"
            f"请在每轮结束时简要说明本轮完成了什么、当前状态如何。"
        )
        return "\n\n".join(parts)

    def _build_goal_followup_prompt(self, last_reason: str, turn: int, max_turns: int) -> str:
        """Build follow-up prompt for subsequent goal turns."""
        remaining = max_turns - turn
        return (
            f"评估器判断目标尚未达成。\n\n"
            f"评估器反馈: {last_reason}\n\n"
            f"请继续工作以满足目标条件。你还有 {remaining} 轮机会。\n"
            f"本轮结束时请简要说明完成了什么、当前状态如何。"
        )

    async def _collect_goal_conversation(self, task_id: int, current_turn: int) -> str:
        """Collect recent conversation log entries for the evaluator.

        Reads the last N assistant messages from log_entries to build a
        summary that the evaluator can judge against the goal condition.
        Only sends recent turns to keep the evaluator prompt concise.
        """
        from backend.models.log_entry import LogEntry

        async with self.db_factory() as db:
            result = await db.execute(
                select(LogEntry.content, LogEntry.loop_iteration)
                .where(
                    LogEntry.task_id == task_id,
                    LogEntry.event_type == "message",
                    LogEntry.role == "assistant",
                )
                .order_by(LogEntry.id.desc())
                .limit(30)
            )
            rows = list(result.all())

        if not rows:
            return "(No conversation output recorded)"

        rows.reverse()
        parts = []
        for content, iteration in rows:
            if content:
                turn_label = f"[Turn {(iteration or 0) + 1}] " if iteration is not None else ""
                parts.append(f"{turn_label}{content}")

        summary = "\n\n".join(parts)
        if len(summary) > 15000:
            summary = summary[-15000:]
        return summary

    def _build_loop_prompt(self, task: Task, iteration: int, signal_path: str,
                           history: list[dict] | None = None, anchored_total: int | None = None,
                           plan: str | None = None) -> str:
        """Build the per-iteration prompt for a loop task.

        Only describes todo-related responsibilities. Git/commit/worktree lifecycle
        is already covered by CLAUDE.md — no need to repeat it here.
        """
        parts = []
        doc = _agent_doc_name(task.provider)
        max_iterations = task.max_iterations or 50
        remaining = max_iterations - iteration

        if task.description:
            parts.append(f"背景说明：{task.description}\n")

        # Include previous iterations' summaries so Claude has context
        if history:
            parts.append("=== 前几轮完成情况 ===")
            for h in history:
                line = f"第 {h['iteration']} 轮"
                if h.get("progress"):
                    line += f" | 进度: {h['progress']}"
                if h.get("summary"):
                    line += f" | {h['summary']}"
                parts.append(line)
            parts.append("=== 前几轮完成情况结束 ===\n")

        # Include plan from previous iterations
        if plan:
            parts.append("=== 整体计划 ===")
            parts.append(plan)
            parts.append("=== 整体计划结束 ===\n")

        # Progress format: anchor total from first iteration so denominator stays consistent
        if anchored_total is not None:
            progress_hint = f"已完成数/{anchored_total}"
        else:
            progress_hint = "已完成数/总数"

        # Signal template: plan field for must_complete, without for normal
        plan_field = ', "plan": "后续每轮计划（简洁，如需调整则更新，无变化则留空）"' if task.must_complete else ""

        if task.must_complete and iteration == 0:
            # First iteration of must_complete: require planning
            parts.append(f"""\
请遵循 {doc} 中的所有要求和项目约定。

这是一个必须全部完成的循环任务，你总共有 {max_iterations} 轮来完成所有任务项。

你的职责：
1. 打开 {task.todo_file_path}，理解其结构，统计所有待完成的任务项
2. 制定整体执行计划：规划每一轮大致完成哪些项，确保在 {max_iterations} 轮内全部完成
3. 执行本轮计划的任务项，在 todo 文件中标记为已完成

完成后，将以下 JSON 写入 {signal_path}：

还有待完成项，请继续下一轮：
{{"action": "continue", "reason": "...", "progress": "{progress_hint}", "summary": "本轮做了什么（一句话）", "plan": "后续每轮计划（简洁）"}}

全部完成：
{{"action": "done", "reason": "所有 todo 项已完成", "progress": "{progress_hint}", "summary": "本轮做了什么（一句话）"}}

注意：所有任务项必须全部完成，任务才算成功。请合理分配每轮工作量，确保在 {max_iterations} 轮内完成。
""")
        elif task.must_complete:
            # Subsequent iterations of must_complete
            # Calculate remaining items
            remaining_items = ""
            if anchored_total is not None and history:
                last_progress = ""
                for h in reversed(history):
                    if h.get("progress"):
                        last_progress = h["progress"]
                        break
                if last_progress:
                    try:
                        done_count = int(last_progress.split("/")[0])
                        remaining_items = f"，还剩 {anchored_total - done_count} 项未完成"
                    except (IndexError, ValueError):
                        pass

            parts.append(f"""\
请遵循 {doc} 中的所有要求和项目约定。

这是一个必须全部完成的循环任务的第 {iteration + 1} 轮，还剩 {remaining} 轮。

你的职责：
1. 打开 {task.todo_file_path}，按照计划执行本轮应完成的任务项
2. 在 todo 文件中将完成的项标记为已完成

完成后，将以下 JSON 写入 {signal_path}：

还有待完成项，请继续下一轮：
{{"action": "continue", "reason": "...", "progress": "{progress_hint}", "summary": "本轮做了什么（一句话）"{plan_field}}}

全部完成：
{{"action": "done", "reason": "所有 todo 项已完成", "progress": "{progress_hint}", "summary": "本轮做了什么（一句话）"}}

无法继续（遇到阻塞或明确问题）：
{{"action": "abort", "reason": "具体原因", "progress": "{progress_hint}", "summary": "本轮做了什么（一句话）"}}

注意：{"任务总数已确定为 " + str(anchored_total) + remaining_items + "，" if anchored_total is not None else ""}你还有 {remaining} 轮机会。
所有任务项必须全部完成。如需调整计划，在 plan 字段中更新。
""")
        else:
            # Normal (non-must_complete) loop
            total_note = ""
            if anchored_total is not None:
                total_note = f"\n注意：任务总数已确定为 {anchored_total}，progress 分母必须始终为 {anchored_total}，不要重新计数。"

            parts.append(f"""\
请遵循 {doc} 中的所有要求和项目约定。

这是一个持续循环任务的第 {iteration + 1} 轮。

你的职责：
1. 打开 {task.todo_file_path}，理解其结构，找到下一个待完成的任务项
2. 根据 {doc} 的要求执行该任务项
3. 在 todo 文件中将该项标记为已完成

完成后，将以下 JSON 写入 {signal_path}：

还有待完成项，请继续下一轮：
{{"action": "continue", "reason": "...", "progress": "{progress_hint}", "summary": "本轮做了什么（一句话）"}}

全部完成：
{{"action": "done", "reason": "所有 todo 项已完成", "progress": "{progress_hint}", "summary": "本轮做了什么（一句话）"}}

无法继续（遇到阻塞或明确问题）：
{{"action": "abort", "reason": "具体原因", "progress": "{progress_hint}", "summary": "本轮做了什么（一句话）"}}
{total_note}
""")
        return "\n".join(parts)

    def _read_loop_signal(self, signal_path) -> dict:
        """Read and parse the signal file Claude writes at the end of each iteration.

        Returns abort with a reason if the file is missing or malformed, so the
        while loop always terminates cleanly instead of spinning indefinitely.
        """
        import json
        from pathlib import Path
        try:
            return json.loads(Path(signal_path).read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"Failed to read loop signal from {signal_path}: {e}")
            return {"action": "abort", "reason": "Signal file missing or invalid JSON"}

    async def _resume_fix_signal(
        self,
        instance_id: int,
        task: Task,
        generation: _TaskLifecycleGeneration,
        cwd: str,
        signal_path,
        iteration: int,
        git_env: dict,
        effort_level: str | None = None,
    ) -> dict:
        """Resume the last session to ask Claude to write the missing signal file.

        Called at most once per iteration when Claude completes work but forgets
        to write the signal JSON.  Returns the signal dict (may still be abort if
        Claude fails to write it on the second attempt).
        """
        async with self.db_factory() as db:
            t = await self._read_owned_lifecycle_task(db, generation)
            resume_sid = t.session_id if t else None

        if not resume_sid:
            logger.warning(f"Loop task {task.id} iter {iteration}: signal missing and no session_id to resume")
            return {"action": "abort", "reason": "Signal file missing and no session to resume"}

        fix_prompt = (
            f"你刚才完成了工作但忘记写信号文件。请检查 {task.todo_file_path} 的当前状态，"
            f"然后立即将以下其中一个 JSON 写入 {signal_path}：\n\n"
            f'还有待完成项：{{"action": "continue", "reason": "...", "progress": "已完成数/总数"}}\n'
            f'全部完成：{{"action": "done", "reason": "所有 todo 项已完成"}}\n'
            f'无法继续：{{"action": "abort", "reason": "具体原因"}}'
        )

        # Same pool anchoring as the main loop launch: resume on the session's
        # resident account, not the inherited systemd default (else --resume
        # misses the JSONL on the wrong account).
        config_dir = await self._resolve_resume_config_dir(
            resume_sid,
            task.provider,
            task_id=task.id,
            expected_generation=generation,
            **({"model": task.model} if task.model else {}),
        )

        logger.info(f"Loop task {task.id} iter {iteration}: resuming session {resume_sid} to fix missing signal")
        exit_code, _ = await self._launch_mode_turn_with_rotation(
            instance_id,
            task,
            generation,
            cwd,
            git_env,
            prompt=fix_prompt,
            config_dir=config_dir,
            resume_session_id=resume_sid,
            loop_iteration=iteration,
            effort_level=effort_level,
            label="Loop signal repair",
        )
        if exit_code not in (0, -2, 130):
            return {
                "action": "abort",
                "reason": f"Signal repair failed (exit code {exit_code})",
            }

        return self._read_loop_signal(signal_path)

    async def _run_plan_phase(
        self,
        instance_id: int,
        task: Task,
        generation: _TaskLifecycleGeneration,
        cwd: str,
        git_env: dict | None = None,
        effort_level: str | None = None,
    ):
        """Run plan phase for plan-mode tasks."""
        if not await self._ensure_owned_executing(generation):
            return
        plan_prompt = (
            f"Please analyze the following task and create a detailed plan. "
            f"Do NOT execute any changes, only describe what you would do:\n\n{task.description}"
        )
        config_dir = await self._resolve_resume_config_dir(
            task.session_id,
            task.provider,
            task_id=task.id,
            expected_generation=generation,
            **({"model": task.model} if task.model else {}),
        )
        exit_code, config_dir = await self._launch_mode_turn_with_rotation(
            instance_id,
            task,
            generation,
            cwd,
            git_env,
            prompt=plan_prompt,
            config_dir=config_dir,
            resume_session_id=task.session_id,
            loop_iteration=None,
            effort_level=effort_level,
            label="Plan phase",
        )
        if exit_code not in (0, -2, 130):
            await self._retry_or_fail_mode_task(
                generation,
                f"Plan phase failed (exit code {exit_code})",
            )
            return
        if exit_code in (-2, 130):
            return

        # Collect plan content from logs
        async with self.db_factory() as db:
            from sqlalchemy import select as sa_select
            from backend.models.log_entry import LogEntry
            result = await db.execute(
                sa_select(LogEntry.content)
                .where(
                    LogEntry.task_id == task.id,
                    LogEntry.event_type == "message",
                    LogEntry.role == "assistant",
                )
                .order_by(LogEntry.id)
            )
            plan_texts = [r[0] for r in result.all() if r[0]]
            plan_content = "\n".join(plan_texts)

            plan_ready = await db.execute(
                update(Task)
                .where(
                    *self._task_lifecycle_generation_predicates(
                        generation,
                        statuses=("executing",),
                    )
                )
                .values(plan_content=plan_content, status="plan_review")
            )
            await db.commit()

        if plan_ready.rowcount:
            await self.broadcaster.broadcast("tasks", {
                "event": "plan_ready",
                "task_id": task.id,
                "instance_id": instance_id,
            })

    # -----------------------------------------------------------------------
    # Monitor Session lifecycle
    # -----------------------------------------------------------------------

    @staticmethod
    def _aux_process_group_id(
        process: asyncio.subprocess.Process,
    ) -> int | None:
        if os.name != "posix":
            return None
        return require_safe_process_group_id(
            getattr(process, "pid", None),
            context="monitor/sub-agent",
        )

    @staticmethod
    def _aux_process_group_alive(process: asyncio.subprocess.Process) -> bool:
        process_group_id = GlobalDispatcher._aux_process_group_id(process)
        if process_group_id is None:
            return process.returncode is None
        try:
            os.killpg(process_group_id, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    @classmethod
    def _aux_process_reaped(cls, process: asyncio.subprocess.Process) -> bool:
        """Return true only when both the parent and its exact group are gone."""

        return (
            process.returncode is not None
            and not cls._aux_process_group_alive(process)
        )

    @staticmethod
    async def _settle_aux_process_spawn(
        *cmd: str,
        **spawn_kwargs,
    ) -> tuple[asyncio.subprocess.Process, asyncio.CancelledError | None]:
        """Settle a subprocess spawn even when its caller is cancelled.

        ``create_subprocess_exec`` can create the OS child before its awaitable
        delivers the ``Process`` object.  Cancelling that await directly loses
        the only exact PID/process-group handle.  Shielding a dedicated task
        lets the caller register and reap that handle before cancellation is
        delivered.
        """

        spawn_task = asyncio.create_task(
            asyncio.create_subprocess_exec(*cmd, **spawn_kwargs)
        )
        delayed_cancellation: asyncio.CancelledError | None = None
        while not spawn_task.done():
            try:
                await asyncio.shield(spawn_task)
            except asyncio.CancelledError as exc:
                if spawn_task.done():
                    break
                delayed_cancellation = exc
            except Exception:
                break

        try:
            process = spawn_task.result()
        except BaseException:
            if delayed_cancellation is not None:
                raise delayed_cancellation
            raise
        return process, delayed_cancellation

    @classmethod
    async def _terminate_aux_process(
        cls,
        process: asyncio.subprocess.Process | None,
        *,
        timeout: float = 5.0,
    ) -> None:
        """Kill and reap one monitor/sub-agent process group.

        These subprocesses are spawned in their own POSIX sessions.  Waiting
        only for the CLI parent is insufficient because a tool child may keep
        working after the parent exits.  Cleanup is shielded so application
        cancellation cannot abandon the group halfway through reaping it.
        """

        if process is None:
            return

        async def terminate() -> None:
            process_group_id = cls._aux_process_group_id(process)
            if process.returncode is None or cls._aux_process_group_alive(process):
                try:
                    if process_group_id is not None:
                        os.killpg(process_group_id, signal.SIGKILL)
                    else:
                        process.kill()
                except ProcessLookupError:
                    pass
                except PermissionError as exc:
                    raise RuntimeError(
                        f"Cannot signal auxiliary process group {process.pid}"
                    ) from exc

            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout
            if process.returncode is None:
                await asyncio.wait_for(
                    asyncio.shield(process.wait()), timeout=max(0.01, timeout)
                )
            while cls._aux_process_group_alive(process):
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise RuntimeError(
                        f"Auxiliary process group {process.pid} survived SIGKILL"
                    )
                await asyncio.sleep(min(0.05, remaining))

        operation = asyncio.create_task(terminate())
        cancellation: asyncio.CancelledError | None = None
        while not operation.done():
            try:
                await asyncio.shield(operation)
            except asyncio.CancelledError as exc:
                cancellation = exc
        operation.result()
        if cancellation is not None:
            raise cancellation

    async def _stop_aux_session(
        self,
        session_id: int,
        task_map: dict[int, asyncio.Task],
        process_map: dict[int, asyncio.subprocess.Process],
        *,
        lifecycle_timeout: float = AUX_LIFECYCLE_CANCEL_TIMEOUT,
    ) -> None:
        lifecycle = task_map.get(session_id)
        lifecycle_timed_out = False
        if (
            lifecycle is not None
            and lifecycle is not asyncio.current_task()
            and not lifecycle.done()
        ):
            lifecycle.cancel()
            _, pending = await asyncio.wait(
                {lifecycle}, timeout=lifecycle_timeout
            )
            lifecycle_timed_out = bool(pending)
            if not lifecycle_timed_out:
                await asyncio.gather(lifecycle, return_exceptions=True)
        # Cancellation may have landed while `_settle_aux_process_spawn` was
        # shielded.  That lifecycle registers its exact Process only after the
        # spawn settles, so the pre-cancel snapshot can legitimately be None.
        # Refresh after awaiting the lifecycle or shutdown can miss the child.
        process = process_map.get(session_id)
        if process is not None and (
            process.returncode is None or self._aux_process_group_alive(process)
        ):
            await self._terminate_aux_process(process)
        if (
            process is not None
            and self._aux_process_reaped(process)
            and process_map.get(session_id) is process
        ):
            process_map.pop(session_id, None)
        elif process is not None:
            # Preserve the exact handle and make failure visible to shutdown;
            # logging-and-returning would let a dedicated child session outlive
            # CCM after this in-memory evidence disappears.
            process_map.setdefault(session_id, process)
            raise RuntimeError(
                f"Auxiliary process group {process.pid} could not be proven terminal"
            )
        if lifecycle_timed_out:
            # The task registry intentionally remains intact.  In particular,
            # a spawn awaitable may still be settling and can publish an exact
            # Process after this point; forgetting the lifecycle would make
            # that child invisible to the next stop/shutdown attempt.
            raise RuntimeError(
                f"Auxiliary session {session_id} lifecycle did not stop within "
                f"{lifecycle_timeout:.1f}s"
            )

    async def _launch_registered_aux_process(
        self,
        *,
        cmd: list[str],
        cwd: str,
        env: dict[str, str],
        log_path: Path,
        session_id: int,
        process_map: dict[int, asyncio.subprocess.Process],
        log_map: dict[int, object],
    ) -> asyncio.subprocess.Process:
        """Spawn, register, and cancellation-safely reap an auxiliary CLI."""

        log_fh = open(log_path, "wb")
        try:
            process, delayed_cancellation = (
                await self._settle_aux_process_spawn(
                    *cmd,
                    stdout=log_fh,
                    stderr=log_fh,
                    cwd=cwd,
                    env=env,
                    start_new_session=True,
                )
            )
        except BaseException:
            log_fh.close()
            raise

        # Registration is synchronous with receiving the exact Process handle,
        # so cancellation can no longer create an invisible child.
        log_map[session_id] = log_fh
        process_map[session_id] = process

        if delayed_cancellation is not None:
            cancellation = delayed_cancellation
            try:
                await self._terminate_aux_process(process)
            except asyncio.CancelledError as exc:
                # _terminate_aux_process delivers cancellation only after its
                # shielded cleanup operation has settled.
                cancellation = exc
            except Exception:
                logger.exception(
                    "Failed to reap auxiliary process %s after spawn cancellation",
                    process.pid,
                )

            if self._aux_process_reaped(process):
                if process_map.get(session_id) is process:
                    process_map.pop(session_id, None)
                if log_map.get(session_id) is log_fh:
                    log_map.pop(session_id, None)
                log_fh.close()
            else:
                # Keep the exact process handle visible so shutdown/admin stop
                # can retry; closing our duplicate file descriptor does not
                # invalidate the child's inherited log descriptor.
                if log_map.get(session_id) is log_fh:
                    log_map.pop(session_id, None)
                log_fh.close()
                logger.critical(
                    "Retaining unreaped auxiliary process evidence: "
                    "session=%s pid=%s",
                    session_id,
                    process.pid,
                )
            raise cancellation

        return process

    async def _finalize_aux_lifecycle_process(
        self,
        *,
        session_id: int,
        process: asyncio.subprocess.Process | None,
        process_map: dict[int, asyncio.subprocess.Process],
    ) -> asyncio.CancelledError | None:
        """Reap one lifecycle generation and forget only proven-dead evidence."""

        candidate = process or process_map.get(session_id)
        if candidate is None:
            return None

        delayed_cancellation: asyncio.CancelledError | None = None
        try:
            # This is intentionally also called after a normal parent wait:
            # descendants can close/inherit no stdio and outlive that parent.
            await self._terminate_aux_process(candidate)
        except asyncio.CancelledError as exc:
            delayed_cancellation = exc
        except Exception:
            logger.exception(
                "Failed to prove auxiliary process group reaped: "
                "session=%s pid=%s",
                session_id,
                candidate.pid,
            )

        if self._aux_process_reaped(candidate):
            if process_map.get(session_id) is candidate:
                process_map.pop(session_id, None)
        else:
            # If launch was mocked or registration was interrupted, recover
            # the exact handle here.  Never turn an uncertain group into an
            # apparently free session slot by dropping its only evidence.
            process_map.setdefault(session_id, candidate)
            logger.critical(
                "Retaining unreaped auxiliary process evidence: "
                "session=%s pid=%s",
                session_id,
                candidate.pid,
            )
        return delayed_cancellation

    async def stop_monitor_session_process(self, session_id: int) -> None:
        await self._stop_aux_session(
            session_id, self._monitor_tasks, self._monitor_processes
        )

    async def stop_sub_agent_session_process(self, session_id: int) -> None:
        if (
            session_id in self._sub_agent_codex_processes
            or session_id in self._sub_agent_codex_homes
            or session_id in self._sub_agent_codex_threads
        ):
            task = self._sub_agent_tasks.get(session_id)
            if task and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await self._finalize_codex_sub_agent_turn(
                session_id,
                self._sub_agent_codex_processes.get(session_id),
                reason="CCM sub-agent session stopped",
            )
            return
        await self._stop_aux_session(
            session_id, self._sub_agent_tasks, self._sub_agent_processes
        )

    async def _finalize_codex_sub_agent_turn(
        self,
        session_id: int,
        process: object | None,
        *,
        reason: str,
    ) -> None:
        """Interrupt one Codex child turn without killing its shared transport."""

        candidate = process or self._sub_agent_codex_processes.get(session_id)
        home = self._sub_agent_codex_homes.get(session_id)
        thread_id = self._sub_agent_codex_threads.get(session_id)
        if candidate is None and thread_id is None:
            self._sub_agent_codex_homes.pop(session_id, None)
            return

        registry = self.instance_manager._ensure_codex_app_server_registry()
        if candidate is not None and getattr(candidate, "returncode", None) is None:
            await registry.abort_unclaimed_turn(
                home,
                candidate,
                reason=reason,
            )

        if thread_id is not None:
            await registry.delete_thread(home, thread_id)

        if candidate is None or getattr(candidate, "returncode", None) is not None:
            if self._sub_agent_codex_processes.get(session_id) is candidate:
                self._sub_agent_codex_processes.pop(session_id, None)
            self._sub_agent_codex_homes.pop(session_id, None)
            self._sub_agent_codex_threads.pop(session_id, None)

    def start_monitor_session(self, monitor_session):
        if getattr(self, "_shutting_down", False):
            raise RuntimeError(
                "GlobalDispatcher is shutting down; monitor admission is closed"
            )
        task = asyncio.create_task(
            self._monitor_session_lifecycle(monitor_session.id)
        )
        self._monitor_tasks[monitor_session.id] = task

    async def _monitor_session_lifecycle(self, monitor_session_id: int):
        """Run a persistent monitor sub-agent process.

        New flow: read DB → build prompt → generate MCP config → launch
        persistent Claude subprocess → wait for process exit (up to 4h) →
        check session status → cleanup.

        The sub-agent communicates via its own MCP tools (report_status,
        mark_complete, get_context) which call back into the CCM API.
        """
        from backend.models.monitor_session import MonitorSession
        from backend.services.mcp_config import (
            generate_monitor_agent_mcp_config,
            cleanup_monitor_agent_mcp_config,
        )

        task_id: int | None = None
        proc: asyncio.subprocess.Process | None = None

        try:
            async with self.db_factory() as db:
                ms = await db.get(MonitorSession, monitor_session_id)
                if not ms:
                    return
                task = await db.get(Task, ms.task_id)
                if not task:
                    return
                task_id = ms.task_id
                ms_description = ms.description
                ms_context = ms.monitor_context
                ms_interval = ms.interval
                ms_max_checks = ms.max_checks
                model = ms.model
                task_cwd = task.last_cwd or task.target_repo or os.getcwd()

            # Dynamic timeout: expected duration + 50% buffer, minimum 30 min
            expected_seconds = ms_interval * ms_max_checks
            timeout_seconds = max(expected_seconds * 1.5, 1800)

            prompt = self._build_monitor_agent_prompt(
                description=ms_description,
                context=ms_context,
                interval=ms_interval,
            )

            mcp_config_path = generate_monitor_agent_mcp_config(
                monitor_session_id=monitor_session_id,
                task_id=task_id,
            )

            proc = await self._launch_monitor_agent(
                prompt=prompt,
                cwd=task_cwd,
                model=model,
                monitor_session_id=monitor_session_id,
                mcp_config_path=mcp_config_path,
                interval_seconds=ms_interval,
            )

            try:
                await asyncio.wait_for(
                    proc.wait(),
                    timeout=timeout_seconds,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"Monitor session {monitor_session_id} timed out after {timeout_seconds:.0f}s, killing"
                )
                await self._terminate_aux_process(proc)
            # A successful wait proves only that the CLI parent exited.  Its
            # dedicated group may still contain tool descendants.
            await self._terminate_aux_process(proc)

            # If session is still running after process exit, the sub-agent
            # exited abnormally without calling mark_complete → mark failed
            async with self.db_factory() as db:
                ms = await db.get(MonitorSession, monitor_session_id)
                if ms and ms.status == "running":
                    ms.status = "failed"
                    ms.completed_at = datetime.utcnow()
                    await db.commit()
                    await self.broadcaster.broadcast(
                        f"task:{task_id}",
                        {
                            "event": "monitor_session_status",
                            "monitor_session_id": monitor_session_id,
                            "status": "failed",
                        },
                    )
                    logger.warning(
                        f"Monitor session {monitor_session_id} process exited "
                        f"(rc={proc.returncode}) without calling mark_complete, marked failed"
                    )

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(f"Monitor session {monitor_session_id} failed unexpectedly")
            try:
                async with self.db_factory() as db:
                    ms = await db.get(MonitorSession, monitor_session_id)
                    if ms and ms.status == "running":
                        ms.status = "failed"
                        ms.completed_at = datetime.utcnow()
                        await db.commit()
                        await self.broadcaster.broadcast(
                            f"task:{ms.task_id}",
                            {"event": "monitor_session_status", "monitor_session_id": ms.id, "status": "failed"},
                        )
            except Exception:
                pass
        finally:
            delayed_cancellation = await self._finalize_aux_lifecycle_process(
                session_id=monitor_session_id,
                process=proc,
                process_map=self._monitor_processes,
            )
            cleanup_monitor_agent_mcp_config(monitor_session_id)
            log_fh = self._monitor_log_fhs.pop(monitor_session_id, None)
            if log_fh:
                try:
                    log_fh.close()
                except Exception:
                    pass
            if self._monitor_tasks.get(monitor_session_id) is asyncio.current_task():
                self._monitor_tasks.pop(monitor_session_id, None)
            if delayed_cancellation is not None:
                raise delayed_cancellation

    async def _launch_monitor_agent(
        self,
        prompt: str,
        cwd: str,
        model: str | None,
        monitor_session_id: int,
        mcp_config_path: Path,
        interval_seconds: int = 300,
    ) -> asyncio.subprocess.Process:
        """Launch a persistent Claude subprocess for a monitor sub-agent.

        Stdout is written to a log file (not PIPE) to prevent buffer blocking.
        The process runs in its own session (start_new_session=True) so it can
        be killed independently without affecting the parent process group.
        """
        cmd = [
            settings.claude_binary,
            "-p", prompt,
            "--output-format", "stream-json",
            "--verbose",
            "--dangerously-skip-permissions",
            "--disallowedTools", "Edit,Write,NotebookEdit,Workflow,Agent,Monitor",
            "--mcp-config", str(mcp_config_path),
        ]
        if model:
            cmd.extend(["--model", model])
        elif settings.default_model:
            cmd.extend(["--model", settings.default_model])

        env = {k: v for k, v in os.environ.items()
               if k.upper() not in ("CLAUDECODE", "CLAUDE_CODE")}
        config_dir: str | None = None

        # Bash 单调用超时上限默认 600s：interval 更长时，子 agent 的一次
        # time.sleep(interval) 会被 CLI 转后台（不阻塞）→ 它转投 ScheduleWakeup
        # 并结束回合 → -p 进程退出 → monitor 被误判 failed（2026-07-16 task 35
        # #192/#193/#194 三连死）。按 interval 抬高上限并留检查余量；只抬不降，
        # 环境里已有更大值时保留。
        want_ms = (max(interval_seconds, 0) + 600) * 1000
        try:
            have_ms = int(env.get("BASH_MAX_TIMEOUT_MS", "0"))
        except ValueError:
            have_ms = 0
        env["BASH_MAX_TIMEOUT_MS"] = str(max(want_ms, have_ms, 600_000))

        # Monitor sub-agent needs a logged-in account. Pick one from the pool
        # (or fall back to default ~/.claude).
        if self.pool:
            config_dir = await self._pool_select(
                model=model or settings.default_model
            )
            if config_dir:
                env["CLAUDE_CONFIG_DIR"] = config_dir
                self._sanitize_cloudrouter_claude_env(env, config_dir)

        log_path = Path(f"/tmp/ccm_monitor_{monitor_session_id}.log")
        process = await self._launch_registered_aux_process(
            cmd=cmd,
            cwd=cwd,
            env=env,
            log_path=log_path,
            session_id=monitor_session_id,
            process_map=self._monitor_processes,
            log_map=self._monitor_log_fhs,
        )
        if config_dir and self.pool:
            self.pool.record_routed_account(config_dir)
        logger.info(
            f"Monitor agent launched: session={monitor_session_id} pid={process.pid} "
            f"log={log_path}"
        )
        return process

    def _build_monitor_agent_prompt(
        self, description: str, context: str | None, interval: int = 300
    ) -> str:
        """Build the system prompt for a monitor sub-agent."""
        parts = [
            "你是一个自主监控 Agent，持续监控目标并在有变化时主动汇报。",
            "",
            "## 监控目标",
            description,
        ]
        if context:
            parts.append("")
            parts.append("## 上下文")
            parts.append(context)
        parts.append("")
        # 等待指引必须按 interval 生成：默认 Bash timeout 只有 120s，单次
        # sleep 又不能超过 CLI 的单调用上限（launch 时已按 interval 抬高
        # BASH_MAX_TIMEOUT_MS），两头都要在 prompt 里写死数字，子 agent
        # 才不会自己发明等待方式（ScheduleWakeup / 转后台 = 进程退出即死）。
        sleep_timeout_ms = (interval + 120) * 1000
        parts.append(f"""\
## 你的 MCP 工具
- report_status(summary, is_important): 报告状态。重要变化设 is_important=True
- mark_complete(reason): 监控目标完成时调用，然后立即停止所有活动
- get_context(): 获取最新监控配置

## 行为准则
1. 用 Bash 执行 ps、tail、cat 等命令检查状态
2. 每次检查后调用 report_status 汇报
3. 你的检查间隔是 {interval} 秒。等待下一轮检查时，用 python 延时命令一次睡满整个间隔，
   并且必须像下面这样显式传大 timeout（默认只有 120 秒，会截断长等待）：
   `Bash(command="python3 -c 'import time; time.sleep({interval})'", timeout={sleep_timeout_ms})`
   【重要】不要使用 bash 的 sleep 命令（会被系统拦截），必须用 python3 的 time.sleep。
   【兜底】如果这个 sleep 没有阻塞等待、而是立即返回（提示转入后台/超出时限），改用
   连续多次 `time.sleep(300)`（每次一个独立 Bash 调用，timeout=360000）累计等够
   {interval} 秒，绝不因此改用其他等待方式。
4. 【关键】你必须严格按以下循环执行，绝不中断：
   检查 → report_status → python sleep → 检查 → report_status → python sleep → ...
   每一步都是一个独立的工具调用。你的进程必须持续运行直到目标完成。
5. 任务完成/失败/异常 → mark_complete 并说明原因，然后停止
6. 你是只读观察者，不要修改任何文件
7. 【禁止】不要使用内置的 Agent 工具
8. 【禁止】不要使用 Monitor 工具、ScheduleWakeup 工具或 run_in_background —— 你是
   一次性 claude -p 进程，回合一结束进程就退出，"到点自动唤醒"的承诺对你不成立，
   用了必死
9. 【禁止】不要在调用 mark_complete 之前结束你的回合（end_turn）

先做一次初始状态检查并 report_status，然后用 python sleep 等待，然后继续下一轮。""")
        return "\n".join(parts)

    # -----------------------------------------------------------------------
    # Sub-Agent Session lifecycle (one-shot tasks)
    # -----------------------------------------------------------------------

    def start_sub_agent_session(self, session):
        if getattr(self, "_shutting_down", False):
            raise RuntimeError(
                "GlobalDispatcher is shutting down; sub-agent admission is closed"
            )
        task = asyncio.create_task(
            self._sub_agent_session_lifecycle(session.id)
        )
        self._sub_agent_tasks[session.id] = task

    async def _sub_agent_session_lifecycle(self, session_id: int):
        """Run a one-shot sub-agent subprocess.

        Simpler than monitor: no interval loop, just launch → wait → handle exit.
        """
        from backend.models.sub_agent import SubAgentSession
        from backend.services.mcp_config import (
            build_sub_agent_mcp_server_specs,
            generate_sub_agent_mcp_config,
            cleanup_sub_agent_mcp_config,
        )

        task_id: int | None = None
        provider = "claude"
        proc: object | None = None
        mcp_config_path: Path | None = None
        SUB_AGENT_TIMEOUT = 7200  # 2 hours

        try:
            async with self.db_factory() as db:
                sa = await db.get(SubAgentSession, session_id)
                if not sa:
                    return
                task = await db.get(Task, sa.task_id)
                if not task:
                    return
                task_id = sa.task_id
                sa_description = sa.description
                sa_context = sa.monitor_context
                sa_prompt_text = sa.last_summary  # stored prompt
                model = sa.model
                task_cwd = task.last_cwd or task.target_repo or os.getcwd()
                provider = (task.provider or "claude").lower()
                task_model = task.model
                task_effort = task.effort_level
                task_metadata = dict(task.metadata_ or {})

            prompt = self._build_sub_agent_prompt(
                description=sa_prompt_text or sa_description,
                context=sa_context,
            )

            if provider == "codex":
                proc = await self._launch_codex_sub_agent(
                    prompt=prompt,
                    cwd=task_cwd,
                    model=model or task_model or settings.default_codex_model,
                    effort_level=task_effort or settings.default_effort,
                    session_id=session_id,
                    task_id=task_id,
                    task_metadata=task_metadata,
                    mcp_specs=build_sub_agent_mcp_server_specs(
                        session_id,
                        task_id,
                    ),
                )
            else:
                mcp_config_path = generate_sub_agent_mcp_config(
                    session_id=session_id,
                    task_id=task_id,
                )
                proc = await self._launch_sub_agent(
                    prompt=prompt,
                    cwd=task_cwd,
                    model=model,
                    session_id=session_id,
                    mcp_config_path=mcp_config_path,
                )

            try:
                await asyncio.wait_for(proc.wait(), timeout=SUB_AGENT_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning(
                    f"Sub-agent session {session_id} timed out after {SUB_AGENT_TIMEOUT}s, killing"
                )
                if provider == "codex":
                    await self._finalize_codex_sub_agent_turn(
                        session_id,
                        proc,
                        reason="CCM sub-agent session timed out",
                    )
                else:
                    await self._terminate_aux_process(proc)
            if provider == "claude":
                await self._terminate_aux_process(proc)

            # If session still running after exit, sub-agent didn't call submit_result
            async with self.db_factory() as db:
                sa = await db.get(SubAgentSession, session_id)
                if sa and sa.status == "running":
                    sa.status = "failed"
                    sa.completed_at = datetime.utcnow()
                    sa.last_summary = f"进程退出 (rc={proc.returncode}) 未提交结果"
                    await db.commit()
                    await self.broadcaster.broadcast(
                        f"task:{task_id}",
                        {
                            "event": "sub_agent_session_status",
                            "sub_agent_session_id": session_id,
                            "status": "failed",
                        },
                    )
                    # Notify main agent of failure
                    await self.enqueue_message(
                        task_id=task_id,
                        prompt=f"[Sub-Agent: {sa_description}] 执行失败: 进程退出 (exit_code={proc.returncode})",
                        priority=PRIORITY_MONITOR_COMPLETE,
                        source="sub-agent:result",
                        user_message_text=f"[Sub-Agent: {sa_description}] 执行失败 (exit_code={proc.returncode})",
                        monitor_session_id=session_id,
                    )
                    logger.warning(
                        f"Sub-agent session {session_id} process exited "
                        f"(rc={proc.returncode}) without submitting result, marked failed"
                    )

        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(f"Sub-agent session {session_id} failed unexpectedly")
            try:
                async with self.db_factory() as db:
                    sa = await db.get(SubAgentSession, session_id)
                    if sa and sa.status == "running":
                        sa.status = "failed"
                        sa.completed_at = datetime.utcnow()
                        await db.commit()
                        await self.broadcaster.broadcast(
                            f"task:{sa.task_id}",
                            {"event": "sub_agent_session_status", "sub_agent_session_id": sa.id, "status": "failed"},
                        )
            except Exception:
                pass
        finally:
            delayed_cancellation = None
            if provider == "codex":
                await self._finalize_codex_sub_agent_turn(
                    session_id,
                    proc,
                    reason="CCM sub-agent lifecycle ended",
                )
            else:
                delayed_cancellation = await self._finalize_aux_lifecycle_process(
                    session_id=session_id,
                    process=proc,
                    process_map=self._sub_agent_processes,
                )
            if mcp_config_path is not None:
                cleanup_sub_agent_mcp_config(session_id)
            log_fh = self._sub_agent_log_fhs.pop(session_id, None)
            if log_fh:
                try:
                    log_fh.close()
                except Exception:
                    pass
            if self._sub_agent_tasks.get(session_id) is asyncio.current_task():
                self._sub_agent_tasks.pop(session_id, None)
            if delayed_cancellation is not None:
                raise delayed_cancellation

    async def _launch_sub_agent(
        self,
        prompt: str,
        cwd: str,
        model: str | None,
        session_id: int,
        mcp_config_path: Path,
    ) -> asyncio.subprocess.Process:
        """Launch a Claude subprocess for a one-shot sub-agent task."""
        cmd = [
            settings.claude_binary,
            "-p", prompt,
            "--output-format", "stream-json",
            "--verbose",
            "--dangerously-skip-permissions",
            "--disallowedTools", "Agent,Task,Monitor",
            "--mcp-config", str(mcp_config_path),
        ]
        if model:
            cmd.extend(["--model", model])
        elif settings.default_model:
            cmd.extend(["--model", settings.default_model])

        env = {k: v for k, v in os.environ.items()
               if k.upper() not in ("CLAUDECODE", "CLAUDE_CODE")}
        config_dir: str | None = None

        if self.pool:
            config_dir = await self._pool_select(
                model=model or settings.default_model
            )
            if config_dir:
                env["CLAUDE_CONFIG_DIR"] = config_dir
                self._sanitize_cloudrouter_claude_env(env, config_dir)

        log_path = Path(f"/tmp/ccm_sub_agent_{session_id}.log")
        process = await self._launch_registered_aux_process(
            cmd=cmd,
            cwd=cwd,
            env=env,
            log_path=log_path,
            session_id=session_id,
            process_map=self._sub_agent_processes,
            log_map=self._sub_agent_log_fhs,
        )
        if config_dir and self.pool:
            self.pool.record_routed_account(config_dir)
        logger.info(
            f"Sub-agent launched: session={session_id} pid={process.pid} log={log_path}"
        )
        return process

    async def _launch_codex_sub_agent(
        self,
        *,
        prompt: str,
        cwd: str,
        model: str,
        effort_level: str | None,
        session_id: int,
        task_id: int,
        task_metadata: dict,
        mcp_specs: tuple,
    ):
        """Launch an independent Codex thread with required callback tools."""

        from backend.services.codex_models import clamp_codex_effort

        codex_home: str | None = None
        pool = self.codex_pool
        if pool and pool.enabled:
            bound_id = task_metadata.get("codex_account_id")
            bound_home = pool.home_for_account(bound_id) if bound_id else None
            if (
                bound_home
                and pool.is_home_available(bound_home)
                and pool.supports_model_for_home(bound_home, model)
            ):
                codex_home = pool.canonical_home(bound_home)
            else:
                codex_home = pool.select(model=model)
            if not codex_home:
                raise RuntimeError(
                    f"No available Codex account supports sub-agent model {model!r}"
                )

        async with self.instance_manager.codex_home_app_server_guard(
            codex_home
        ) as admitted_home:
            registry = self.instance_manager._ensure_codex_app_server_registry()
            process, thread_id = await registry.start_turn(
                codex_home=admitted_home,
                prompt=prompt,
                cwd=cwd,
                model=model,
                effort=clamp_codex_effort(model, effort_level),
                resume_session_id=None,
                git_env=None,
                task_id=task_id,
                mcp_specs=mcp_specs,
            )
        self._sub_agent_codex_homes[session_id] = admitted_home
        self._sub_agent_codex_processes[session_id] = process
        self._sub_agent_codex_threads[session_id] = thread_id
        if codex_home and pool:
            pool.record_routed_account(codex_home)
        logger.info(
            "Codex sub-agent launched: session=%s thread=%s home=%s",
            session_id,
            thread_id,
            admitted_home,
        )
        return process

    def _build_sub_agent_prompt(self, description: str, context: str | None) -> str:
        """Build the system prompt for a one-shot sub-agent."""
        parts = [
            "你是一个自主执行任务的 Sub-Agent。完成任务后用 submit_result 提交结果。",
            "",
            "## 任务",
            description,
        ]
        if context:
            parts.append("")
            parts.append("## 上下文")
            parts.append(context)
        parts.append("")
        parts.append("""\
## 你的 MCP 工具
- report_progress(summary): 报告当前进度，让主 session 实时看到
- submit_result(result, success): 提交最终结果并结束。result 用 Markdown 格式
- get_context(): 获取任务上下文（项目信息、task 描述等）

## 行为准则
1. 先用 get_context() 了解项目背景
2. 执行过程中定期用 report_progress() 汇报进度
3. 完成后用 submit_result() 提交最终结果，然后停止所有活动
4. 如果任务失败，调用 submit_result(result="失败原因", success=False)
5. 【禁止】不要使用内置的 Agent 工具
6. 【禁止】不要使用 Monitor 工具
7. 【关键】必须在合理时间内完成任务并调用 submit_result""")
        return "\n".join(parts)

    # -----------------------------------------------------------------------
    # Per-task message queue (chat + monitor reports)
    # -----------------------------------------------------------------------

    def _get_task_queue(self, task_id: int) -> asyncio.PriorityQueue:
        if task_id not in self._task_queues:
            self._task_queues[task_id] = asyncio.PriorityQueue()
        return self._task_queues[task_id]

    def _ensure_queue_worker(self, task_id: int):
        existing = self._task_queue_workers.get(task_id)
        if existing and not existing.done():
            # A watchdog replacement first waits for the old consumer's
            # cancellation cleanup.  Treat that handoff as the one registered
            # worker; nesting another replacement would recreate the same
            # concurrent-resume race the handoff prevents.
            if getattr(existing, "_ccm_queue_worker_handoff", False):
                return
            last_activity = self._task_queue_activity.get(task_id, 0)
            if last_activity and time.monotonic() - last_activity > QUEUE_STUCK_THRESHOLD:
                logger.warning(
                    f"Task {task_id} queue consumer stuck for >{QUEUE_STUCK_THRESHOLD}s, "
                    "cancelling before replacement"
                )
                self._task_queue_activity[task_id] = time.monotonic()
                handoff = asyncio.create_task(
                    self._replace_stuck_queue_worker(task_id, existing)
                )
                setattr(handoff, "_ccm_queue_worker_handoff", True)
                self._task_queue_workers[task_id] = handoff
                return
            else:
                return
        self._task_queue_activity[task_id] = time.monotonic()
        worker = asyncio.create_task(self._task_queue_consumer(task_id))
        self._task_queue_workers[task_id] = worker

    async def _replace_stuck_queue_worker(
        self,
        task_id: int,
        old_worker: asyncio.Task,
    ) -> None:
        """Serialize watchdog replacement after exact old-worker cleanup."""

        current = asyncio.current_task()
        old_worker.cancel()
        delayed_cancellation: asyncio.CancelledError | None = None
        while not old_worker.done():
            try:
                await asyncio.shield(old_worker)
            except asyncio.CancelledError as exc:
                if old_worker.done():
                    # The shield is surfacing the old worker's expected
                    # cancellation, not cancellation of this handoff.
                    break
                # abort_task_queue/shutdown may cancel this handoff too.  Keep
                # waiting for the old consumer's reservation/process cleanup,
                # then deliver cancellation without spawning a replacement.
                delayed_cancellation = exc
            except BaseException:
                break
        await asyncio.gather(old_worker, return_exceptions=True)

        cancellation_requested = bool(
            delayed_cancellation is not None
            or (current is not None and current.cancelling())
        )
        if cancellation_requested or self._shutting_down:
            if self._task_queue_workers.get(task_id) is current:
                self._task_queue_workers.pop(task_id, None)
            if delayed_cancellation is not None:
                raise delayed_cancellation
            return

        # Another explicit abort/replacement may have won while the old worker
        # was settling.  Only the registered handoff may install its successor.
        if self._task_queue_workers.get(task_id) is not current:
            return
        self._task_queue_activity[task_id] = time.monotonic()
        replacement = asyncio.create_task(
            self._task_queue_consumer(task_id)
        )
        self._task_queue_workers[task_id] = replacement

    async def enqueue_message(
        self,
        task_id: int,
        prompt: str,
        priority: int = PRIORITY_USER,
        source: str = "user",
        user_message_text: str | None = None,
        command_skills: dict | None = None,
        model_override: str | None = None,
        monitor_session_id: int | None = None,
        source_log_id: int | None = None,
        current_message: str | None = None,
        queue_timestamp: float | None = None,
    ):
        """Enqueue a message for the main agent of a task.

        Messages are processed serially by a per-task consumer. Registration
        shares the task-start gate with self-update: if maintenance has already
        paused launches, the message becomes a blocker and is retained until
        the update cancels its restart and resumes dispatching.
        """
        if self._shutting_down:
            raise RuntimeError(
                "Dispatcher is shutting down; message admission is closed"
            )
        msg = QueuedMessage(
            priority=priority,
            timestamp=(
                time.monotonic()
                if queue_timestamp is None
                else queue_timestamp
            ),
            prompt=prompt,
            source=source,
            user_message_text=user_message_text,
            command_skills=command_skills,
            model_override=model_override,
            monitor_session_id=monitor_session_id,
            source_log_id=source_log_id,
            current_message=prompt if current_message is None else current_message,
        )
        async with self._dispatch_claim_lock:
            if self._maintenance_shutdown_committed:
                raise TaskStartPausedError("service shutdown has already been committed")
            msg.queue_generation = self._task_queue_generations.get(task_id, 0)
            q = self._get_task_queue(task_id)
            await q.put(msg)
            self._pending_task_starts.add(task_id)
            self._ensure_queue_worker(task_id)
        logger.info(
            f"Enqueued message for task {task_id}: source={source} priority={priority} "
            f"queue_depth={q.qsize()}"
        )

    async def clear_task_queue(self, task_id: int) -> int:
        """Drop all pending queued messages for a task (used on interrupt).

        Returns the number of messages discarded. The message currently being
        processed (if any) is not affected — callers stop the process separately.
        """
        async with self._dispatch_claim_lock:
            self._task_queue_generations[task_id] = (
                self._task_queue_generations.get(task_id, 0) + 1
            )
            q = self._task_queues.get(task_id)
            if q is None:
                self._pending_task_starts.discard(task_id)
                return 0
            # q.get() removes an item before the consumer can acquire the
            # admission lock to register it as in-flight. In that state the
            # queue is empty but pending still records the accepted message.
            # Advancing the generation above cancels that handoff; count it so
            # stop-session reports a successful clear instead of a false 400.
            cancelled_handoff = (
                q.empty()
                and task_id in self._pending_task_starts
                and not self._task_queue_inflight.get(task_id, 0)
            )
            cleared = 0
            while True:
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    break
                q.task_done()
                cleared += 1
            if q.empty() and not self._task_queue_inflight.get(task_id, 0):
                self._pending_task_starts.discard(task_id)
            if cancelled_handoff:
                cleared += 1
        if cleared:
            logger.info(f"Cleared {cleared} pending queued message(s) for task {task_id} on interrupt")
        return cleared

    async def _claim_dequeued_message(
        self,
        task_id: int,
        msg: QueuedMessage,
    ) -> bool:
        """Register a dequeued message unless a queue clear invalidated it."""
        async with self._dispatch_claim_lock:
            if msg.queue_generation != self._task_queue_generations.get(task_id, 0):
                return False
            self._task_queue_inflight[task_id] = (
                self._task_queue_inflight.get(task_id, 0) + 1
            )
            self._pending_task_starts.add(task_id)
            return True

    async def abort_task_queue(
        self,
        task_id: int,
        *,
        timeout: float = TASK_QUEUE_ABORT_TIMEOUT,
    ) -> int:
        """Discard pending messages and cancel the already-dequeued message.

        Draining ``asyncio.Queue`` alone cannot see the item currently held by
        ``_task_queue_consumer``.  That item may be waiting for a prior turn to
        become idle and would otherwise launch *after* stop-session returned.
        Waiting for consumer cancellation also guarantees its slot reservation
        and temporary skill state have completed their ``finally`` cleanup.
        """

        cleared = await self.clear_task_queue(task_id)
        worker = self._task_queue_workers.get(task_id)
        cancelled_worker = False
        if (
            worker is not None
            and worker is not asyncio.current_task()
            and not worker.done()
        ):
            cancelled_worker = True
            worker.cancel()
            done, pending = await asyncio.wait({worker}, timeout=timeout)
            if pending:
                # Keep _task_queue_workers as exact evidence.  Returning would
                # let cancel/stop claim success while the already-dequeued
                # message can still own a hidden launch reservation or child.
                raise TaskQueueAbortTimeoutError(
                    f"Task {task_id} queue worker did not stop within "
                    f"{timeout:.1f}s"
                )
            await asyncio.gather(*done, return_exceptions=True)
        # A retryable error can requeue between the first drain and consumer
        # cancellation.  Drain once more after the worker is definitively gone.
        if cancelled_worker:
            cleared += await self.clear_task_queue(task_id)
        return cleared

    async def _queue_heartbeat(self, task_id: int):
        """Continuously mark a task's queue consumer as alive.

        Runs for the consumer's entire lifetime — including a long turn blocked
        in `_wait_process` and an idle wait on `q.get()` — so neither looks
        "stuck" to `_ensure_queue_worker`. Previously the activity timestamp was
        only bumped around each `_process_queued_message` call, so any turn
        longer than QUEUE_STUCK_THRESHOLD froze the heartbeat, the watchdog
        respawned the consumer, and the orphaned `claude` subprocess kept
        running — yielding concurrent `--resume` on one session (prod task #728).
        """
        try:
            while True:
                self._task_queue_activity[task_id] = time.monotonic()
                await asyncio.sleep(QUEUE_HEARTBEAT_INTERVAL)
        except asyncio.CancelledError:
            pass

    async def _publish_permanent_account_routing_failure(
        self,
        task_id: int,
        exc: Exception,
    ) -> None:
        """Make a non-retryable queued-message routing refusal visible."""

        provider = (
            "Claude"
            if isinstance(exc, ClaudeAccountRoutingError)
            else "Codex"
        )
        notice = (
            f"{provider} 账号路由无法安全确定，本条消息未执行。"
            "请检查账号启用状态与模型支持；若是旧会话多副本，"
            "请先手动指定正确账号后重新发送。"
        )
        async with self.db_factory() as db:
            entry = LogEntry(
                instance_id=None,
                task_id=task_id,
                event_type="system_event",
                role="system",
                content=notice,
                is_error=True,
            )
            db.add(entry)
            await db.commit()
        await self.broadcaster.broadcast(
            f"task:{task_id}",
            {
                "event_type": "system_event",
                "role": "system",
                "content": notice,
                "is_error": True,
            },
        )

    async def _task_queue_consumer(self, task_id: int):
        """Serial consumer: process queued messages one at a time for a task."""
        q = self._get_task_queue(task_id)
        # Lifetime heartbeat: keeps the watchdog from respawning us during a
        # long-running turn or an idle wait (see _queue_heartbeat / task #728).
        hb_task = asyncio.create_task(self._queue_heartbeat(task_id))

        try:
            while True:
                try:
                    msg: QueuedMessage = await asyncio.wait_for(
                        q.get(), timeout=QUEUE_CONSUMER_IDLE_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    logger.info(
                        f"Task {task_id} queue consumer idle for "
                        f"{QUEUE_CONSUMER_IDLE_TIMEOUT}s, stopping"
                    )
                    break

                try:
                    claimed = await self._claim_dequeued_message(task_id, msg)
                except BaseException:
                    q.task_done()
                    raise
                if not claimed:
                    q.task_done()
                    logger.info(
                        "Discarded cancelled queued-message handoff for task %s",
                        task_id,
                    )
                    continue
                try:
                    while True:
                        await self.wait_until_resumed()
                        try:
                            await self._process_queued_message(task_id, msg)
                            break
                        except TaskStartPausedError:
                            # Maintenance won the late admission race after the
                            # consumer had already prepared this turn. Keep the
                            # exact message in hand and retry only after the
                            # updater cancels its restart and reopens the gate.
                            continue
                except Exception as exc:
                    from backend.services.codex_app_server import (
                        CodexAppServerBusyError,
                        CodexThreadHomeMismatchError,
                    )
                    from backend.services.instance_manager import (
                        InstanceAlreadyRunningError,
                    )

                    if isinstance(
                        exc,
                        (
                            QueuedMessagePrelaunchError,
                            CodexAccountRoutingError,
                            CodexAppServerBusyError,
                            CodexThreadHomeMismatchError,
                            InstanceAlreadyRunningError,
                        ),
                    ):
                        permanent_routing_error = (
                            isinstance(
                                exc,
                                (
                                    ClaudeAccountRoutingError,
                                    CodexAccountRoutingError,
                                ),
                            )
                            and exc.permanent
                        )
                        if permanent_routing_error:
                            logger.error(
                                "Queued message for task %s cannot be routed: %s",
                                task_id,
                                exc,
                            )
                            try:
                                await (
                                    self
                                    ._publish_permanent_account_routing_failure(
                                        task_id,
                                        exc,
                                    )
                                )
                            except Exception:
                                # If the durable/user-visible refusal itself
                                # cannot be committed, preserve the exact
                                # message rather than acknowledging it unseen.
                                logger.exception(
                                    "Could not publish permanent routing "
                                    "failure for task %s; requeueing message",
                                    task_id,
                                )
                                await q.put(msg)
                                await asyncio.sleep(
                                    CODEX_ROUTING_RETRY_DELAY
                                )
                        else:
                            # Routing/rebind/instance-contention conflicts are
                            # temporary. Preserve the exact user message instead
                            # of acknowledging q.task_done() and dropping it.
                            logger.warning(
                                "Deferring queued message for task %s until a "
                                "launch slot is available: %s",
                                task_id, exc,
                            )
                            await q.put(msg)
                            retry_after = getattr(exc, "retry_after", None)
                            await asyncio.sleep(
                                max(
                                    0.0,
                                    min(
                                        float(
                                            retry_after
                                            or CODEX_ROUTING_RETRY_DELAY
                                        ),
                                        300.0,
                                    ),
                                )
                            )
                    else:
                        logger.exception(
                            f"Error processing queued message for task {task_id}"
                        )
                finally:
                    # `_process_queued_message` normally releases immediately
                    # after launch. This outer guard also covers exceptions in
                    # config resolution, compaction, logging, or DB commits
                    # after the atomic reservation was acquired.
                    if msg.instance_claim is not None:
                        claimed_id, claim_token = msg.instance_claim
                        await self._release_instance_reservation(
                            claimed_id, claim_token
                        )
                        msg.instance_claim = None
                    q.task_done()
                    async with self._dispatch_claim_lock:
                        inflight = self._task_queue_inflight.get(task_id, 0) - 1
                        if inflight > 0:
                            self._task_queue_inflight[task_id] = inflight
                        else:
                            self._task_queue_inflight.pop(task_id, None)
                        if q.empty() and inflight <= 0:
                            self._pending_task_starts.discard(task_id)
        finally:
            hb_task.cancel()
            await asyncio.gather(hb_task, return_exceptions=True)
            # Only deregister if THIS task is still the registered worker. The
            # watchdog (_ensure_queue_worker) may have already cancelled us and
            # registered a fresh consumer; popping unconditionally would erase
            # the live worker's registration, so the next enqueue would spawn a
            # *second* live consumer → two concurrent `--resume` (task #728).
            if self._task_queue_workers.get(task_id) is asyncio.current_task():
                self._task_queue_workers.pop(task_id, None)
            async with self._dispatch_claim_lock:
                if q.empty() and not self._task_queue_inflight.get(task_id, 0):
                    self._pending_task_starts.discard(task_id)
                    self._task_queues.pop(task_id, None)

    async def _queued_task_has_live_generation(self, db, task_id: int) -> bool:
        """Check the task's exact durable owner against all local generations."""

        task = await db.get(Task, task_id, populate_existing=True)
        if task is None or task.instance_id is None:
            return False
        instance_id = task.instance_id

        lifecycle = self._running_tasks.get(instance_id)
        if lifecycle is not None and not lifecycle.done():
            # Fresh lifecycle preparation precedes Instance.current_task_id/PID
            # persistence, so Task.instance_id is its exact durable owner link.
            return True

        records = getattr(self.instance_manager, "_consumer_records", {})
        record = (
            records.get(instance_id) if isinstance(records, dict) else None
        )
        record_task_id = getattr(record, "task_id", None)
        launch_params = getattr(self.instance_manager, "_launch_params", {})
        params = (
            launch_params.get(instance_id)
            if isinstance(launch_params, dict)
            else None
        )
        params_task_id = (
            params.get("task_id") if isinstance(params, dict) else None
        )
        manager_running = bool(self.instance_manager.is_running(instance_id))
        if manager_running and (
            record_task_id == task_id or params_task_id == task_id
        ):
            # Instance.current_task_id can be cleared near the end of output
            # persistence while the exact Codex consumer still owns rollout
            # migration/account binding.  The generation record is the
            # stronger identity during that terminal window.
            return True

        instance = await db.get(Instance, instance_id, populate_existing=True)
        if instance is None or instance.current_task_id != task_id:
            return False

        # is_running includes the exact process group/container generation and
        # its output-consumer record.  A terminal parent with a live consumer
        # remains busy until rollout/account bookkeeping has settled.
        return manager_running

    async def _process_queued_message(self, task_id: int, msg: QueuedMessage):
        """Process one message and never leak its pre-launch slot lease."""

        launch_admission = {"held": False}
        cleanup_state = {
            "has_temp_skills": False,
            "original_skills": {},
        }
        try:
            return await self._process_queued_message_inner(
                task_id, msg, launch_admission, cleanup_state
            )
        finally:
            if launch_admission["held"]:
                launch_admission["held"] = False
                self._chat_launch_admission_lock.release()
            # Covers failures during account resolution/compaction/DB writes,
            # before the narrower launch try/finally is reached.
            if msg.instance_claim is not None:
                instance_id, claim_token = msg.instance_claim
                await self._release_instance_reservation(
                    instance_id, claim_token
                )
                msg.instance_claim = None
            if cleanup_state["has_temp_skills"]:
                await self._restore_queued_message_skills(
                    task_id,
                    msg,
                    cleanup_state["original_skills"],
                )
                cleanup_state["has_temp_skills"] = False

    async def _restore_queued_message_skills(
        self,
        task_id: int,
        msg: QueuedMessage,
        original_skills: dict,
    ) -> None:
        """Restore one-message skill overrides before queue cancellation settles."""

        try:
            async with self.db_factory() as db:
                task = await db.get(Task, task_id)
                if task is None:
                    return
                current = dict(task.enabled_skills or {})
                for key in msg.command_skills or {}:
                    if key in original_skills:
                        current[key] = original_skills[key]
                    else:
                        current.pop(key, None)
                task.enabled_skills = current
                await db.commit()
        except Exception:
            logger.exception(
                "Failed to restore enabled_skills for task %s",
                task_id,
            )

    async def _process_queued_message_inner(
        self,
        task_id: int,
        msg: QueuedMessage,
        launch_admission: dict[str, bool],
        cleanup_state: dict,
    ):
        """Resume main agent session with a queued message."""
        if msg.current_message is None:
            # Tests and a few internal callers construct QueuedMessage
            # directly instead of going through enqueue_message().
            msg.current_message = msg.prompt
        # Phase 1: read task state, find idle instance, launch process
        inst_id: int | None = None
        original_skills: dict = {}
        queued_turn_generation: _TaskStatusGeneration | None = None
        async with self.db_factory() as db:
            task = await db.get(Task, task_id)
            if not task:
                logger.warning(f"Task {task_id} not found, skipping queued message")
                return
            if task_is_pr_review_superseded(task):
                logger.info(
                    "Discarding queued message for superseded PR review task %s",
                    task_id,
                )
                return
            if task.worker_id is not None or task.shared_from_id is not None:
                # A message can be dequeued just before Task migration commits.
                # It cannot be safely replayed locally (that would create a
                # second writer) or reconstructed as an exact remote API upload.
                # Make the non-delivery visible and require a resend through the
                # now-authoritative Worker/shared route.
                notice = (
                    "此消息未执行：任务在排队期间已迁移到远程 Worker/共享节点，"
                    "请在任务迁移完成后重新发送。"
                )
                db.add(
                    LogEntry(
                        instance_id=None,
                        task_id=task_id,
                        event_type="system_event",
                        role="system",
                        content=notice,
                        is_error=True,
                    )
                )
                await db.commit()
                await self.broadcaster.broadcast(
                    f"task:{task_id}",
                    {
                        "event_type": "system_event",
                        "role": "system",
                        "content": notice,
                        "is_error": True,
                    },
                )
                logger.warning(
                    "Refused local queued launch for migrated task %s",
                    task_id,
                )
                return
            # compact_retry starts a fresh session with the compacted summary,
            # so it doesn't need an existing session_id to resume.
            if (
                not task.session_id
                and msg.source != "compact_retry"
                and not msg.allow_new_session
            ):
                logger.warning(f"Task {task_id} no session, skipping queued message")
                return

            # Recover before resuming when the session can't be resumed:
            #   - task=failed after an abnormal exit (session may still be on disk), OR
            #   - the session JSONL is gone (resume would die with "No conversation
            #     found", which non-0 exits and hard-fails the task).
            # Without the on-disk check the FIRST message after a session vanishes
            # is always sacrificed to flip the task to "failed"; only the SECOND
            # message reaches this branch and recovers (prod task #725).
            from backend.api.tasks import _clone_session, _find_session_jsonl
            provider = (task.provider or "claude").lower()
            session_gone = bool(task.session_id) and _find_session_jsonl(
                task.session_id, provider=provider
            ) is None
            if task.session_id and (task.status == "failed" or session_gone):
                # Snapshot the complete resume generation before any clone /
                # compaction awaits.  A concurrent cancel, retry, or owner/session
                # change must win and keep this exact QueuedMessage unconsumed.
                recovery_status = task.status
                recovery_retry_count = task.retry_count
                recovery_instance_id = task.instance_id
                recovery_session_id = task.session_id
                recovery_started_at = task.started_at
                recovery_completed_at = task.completed_at
                if task.status == "failed":
                    logger.info("Task %d session crashed, recovering session...", task_id)
                else:
                    logger.warning(
                        "Task %d session %s not on disk, recovering before resume "
                        "(would otherwise hard-fail with 'No conversation found')",
                        task_id, task.session_id,
                    )
                # A present Codex rollout remains resumable after a failed
                # turn.  Unlike Claude's flat JSONL, it cannot be made into a
                # new thread by merely copying/renaming the file because the
                # thread id is embedded in its metadata.
                keep_codex_session = provider == "codex" and not session_gone
                cloned = None if keep_codex_session else await _clone_session(task_id, db)
                recovered_session_id = recovery_session_id
                recovered_context_usage = task.context_window_usage
                recovered_prompt = msg.prompt
                if cloned:
                    recovered_session_id = cloned["session_id"]
                    logger.info(
                        "Task %d cloned session -> %s",
                        task_id,
                        recovered_session_id,
                    )
                elif not keep_codex_session:
                    # JSONL file missing, fall back to compact summary
                    logger.warning("Task %d JSONL not found, falling back to compact summary", task_id)
                    summary = await self._compact_session(
                        task_id,
                        recovery_session_id,
                        db,
                        exclude_log_entry_id=msg.source_log_id,
                    )
                    recovered_session_id = None
                    recovered_context_usage = None
                    if summary:
                        recovered_prompt = build_compacted_resume_prompt(
                            summary,
                            msg.current_message,
                            interrupted=True,
                        )
                else:
                    logger.info(
                        "Task %d reusing existing Codex session %s after failed turn",
                        task_id, task.session_id,
                    )
                # 关键：不能设成 "pending"——否则主调度循环 (dequeue) 会把它当作
                # 新任务抢走一个空闲 instance 从头执行 task 描述，导致同一 task 出现
                # 两个 Claude session（一个回应聊天、一个重跑任务）。设成 "in_progress"
                # 表示"已被 queue consumer 认领、待 resume"，dispatch loop 不会重复分配。
                # 详见 PROGRESS.md task #707 双 session 竞争条件。
                recovery_claim = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task_id,
                        Task.status == recovery_status,
                        Task.retry_count == recovery_retry_count,
                        (
                            Task.instance_id.is_(None)
                            if recovery_instance_id is None
                            else Task.instance_id == recovery_instance_id
                        ),
                        (
                            Task.session_id.is_(None)
                            if recovery_session_id is None
                            else Task.session_id == recovery_session_id
                        ),
                        (
                            Task.started_at.is_(None)
                            if recovery_started_at is None
                            else Task.started_at == recovery_started_at
                        ),
                        (
                            Task.completed_at.is_(None)
                            if recovery_completed_at is None
                            else Task.completed_at == recovery_completed_at
                        ),
                        Task.worker_id.is_(None),
                        Task.shared_from_id.is_(None),
                        task_retry_not_superseded_predicate(),
                    )
                    .values(
                        status="in_progress",
                        session_id=recovered_session_id,
                        context_window_usage=recovered_context_usage,
                        completed_at=None,
                        error_message=None,
                    )
                )
                if not recovery_claim.rowcount:
                    await db.rollback()
                    current = await db.get(
                        Task,
                        task_id,
                        populate_existing=True,
                    )
                    if task_is_pr_review_superseded(current):
                        logger.info(
                            "Discarding queued recovery for superseded PR "
                            "review task %s",
                            task_id,
                        )
                        return
                    logger.info(
                        "Task %s recovery was superseded by a concurrent "
                        "status/retry/owner/session generation",
                        task_id,
                    )
                    raise QueuedMessagePrelaunchError(
                        "Queued task recovery generation changed; preserving "
                        "the exact message for retry"
                    )
                await db.commit()
                msg.prompt = recovered_prompt
                if recovered_session_id is None:
                    msg.allow_new_session = True
                task = await db.get(Task, task_id, populate_existing=True)
                if task is None:
                    raise QueuedMessagePrelaunchError(
                        "Queued task disappeared after recovery commit"
                    )
                # 广播认领态（此分支是 failed/session 丢失的恢复路径）：不广播
                # 的话前端要等 executing 广播才知道任务被认领（轮询窗口内分叉）
                from backend.services.task_events import broadcast_status_change
                await broadcast_status_change(task_id, "in_progress")

            # Wait for the task's exact local generation to become idle.  The
            # parent process alone is insufficient: terminal Codex consumers
            # still own rollout migration/rebind work, while a fresh lifecycle
            # can own Task.instance_id before it has registered any process.
            for attempt in range(60):
                if not await self._queued_task_has_live_generation(db, task_id):
                    break
                await asyncio.sleep(2)
            else:
                logger.warning(f"Task {task_id} still busy after 120s, re-queueing message: {msg.source}")
                q = self._get_task_queue(task_id)
                await q.put(msg)
                await asyncio.sleep(5)
                return

            # Fence startup reconciliation from this point through successful
            # spawn.  Refresh after acquiring because start() or another owner
            # may have changed the Task while we waited at the gate.
            await self._chat_launch_admission_lock.acquire()
            launch_admission["held"] = True
            task = await db.get(Task, task_id, populate_existing=True)
            if task is None:
                logger.warning(
                    "Task %s disappeared before queued launch admission",
                    task_id,
                )
                return
            if task_is_pr_review_superseded(task):
                logger.info(
                    "Discarding queued message after PR review task %s was "
                    "superseded while waiting for launch admission",
                    task_id,
                )
                return
            if await self._queued_task_has_live_generation(db, task_id):
                logger.info(
                    "Task %s acquired a live generation while queued launch "
                    "waited for startup reconciliation; preserving message",
                    task_id,
                )
                await self._get_task_queue(task_id).put(msg)
                return
            task = await db.get(Task, task_id, populate_existing=True)
            if task is None:
                return
            if task_is_pr_review_superseded(task):
                logger.info(
                    "Discarding queued message for superseded PR review task %s",
                    task_id,
                )
                return
            if task.worker_id is not None or task.shared_from_id is not None:
                raise QueuedMessagePrelaunchError(
                    "Task migrated while queued launch waited for admission"
                )
            # Atomically bridge DB idle selection to the launch window.  This
            # also filters distributed Worker string keys before constructing
            # the integer Instance predicate (PostgreSQL is strict here).
            inst, claim_token = await self._reserve_idle_instance(db)
            if inst is None or claim_token is None:
                logger.warning(f"No idle instance for task {task_id}, re-queueing message")
                q = self._get_task_queue(task_id)
                await q.put(msg)
                await asyncio.sleep(5)
                return
            msg.instance_claim = (inst.id, claim_token)

            # Build git env
            merged: dict = {}
            if task.project_id:
                project = await db.get(Project, task.project_id)
                global_cfg = await db.get(GlobalSettings, 1)
                if project:
                    merged = merge_git_config(settings_to_dict(project), settings_to_dict(global_cfg))
            git_env = _build_git_env(merged)

            effort_level = task.effort_level or settings.default_effort

            # Pool: pick the account for this resume. Resolves a fresh validated
            # account (migrating the session into it) when one is available, and
            # — crucially — when the pool is exhausted still anchors --resume to
            # the session's resident dir instead of letting it fall through to an
            # inherited CLAUDE_CONFIG_DIR that lacks the JSONL (prod #734/#740).
            queued_routing_generation = self._task_status_generation(task)
            effective_model = msg.model_override or task.model
            config_dir = await self._resolve_resume_config_dir(
                task.session_id,
                task.provider,
                task_id=task.id,
                expected_generation=queued_routing_generation,
                **({"model": effective_model} if effective_model else {}),
            )

            logger.info(
                f"Processing queued message for task {task_id}: source={msg.source} "
                f"on instance {inst.id}"
            )

            # Merge command_skills with task's enabled_skills for this launch
            original_skills = dict(task.enabled_skills or {})
            cleanup_state["original_skills"] = original_skills
            effective_skills = dict(original_skills)
            if msg.command_skills:
                effective_skills.update(msg.command_skills)

            # 上下文超阈值时自动摘要 + 新 session（无限续聊）
            if task.session_id and task.context_window_usage:
                usage = task.context_window_usage
                used_tokens = context_tokens_used(task.provider, usage)
                # context_window 可能被 CC 低报（1M 模型报 200K），用模型名兜底；
                # codex 无该字段时查 codex 窗口表（272K/128K，非 claude 的 200K）
                if (task.provider or "claude").lower() == "codex":
                    from backend.services.codex_models import codex_context_window
                    window = usage.get("context_window") or codex_context_window(
                        effective_model
                    )
                else:
                    from backend.services.claude_models import claude_context_window

                    window = max(
                        usage.get("context_window") or 0,
                        claude_context_window(effective_model),
                    )
                utilization = used_tokens / window if window else 0
                # 阈值：GlobalSettings 覆盖 > env 默认（前端运行时设置可改）
                gs = await db.get(GlobalSettings, 1)
                compact_threshold = (
                    gs.context_compact_threshold
                    if gs and gs.context_compact_threshold is not None
                    else settings.context_compact_threshold
                )
                if utilization >= compact_threshold:
                    logger.info(
                        "Task %d context at %.0f%% (%d/%d), compacting session...",
                        task_id, utilization * 100, used_tokens, window,
                    )
                    # 收集最近对话摘要
                    summary = await self._compact_session(
                        task_id,
                        task.session_id,
                        db,
                        exclude_log_entry_id=msg.source_log_id,
                    )
                    if summary:
                        # 清空 session_id → 下次 launch 开新 session，prompt 带摘要
                        task.session_id = None
                        task.context_window_usage = None
                        # 在聊天里给用户留一条可见的压缩提示（落库 + 实时广播）
                        notice = (
                            f"⚡ 上下文已达 {utilization * 100:.0f}%"
                            f"（{used_tokens:,}/{window:,} tokens，阈值 {compact_threshold * 100:.0f}%），"
                            f"已自动压缩摘要并开启新会话延续上下文"
                        )
                        db.add(LogEntry(
                            instance_id=inst.id,
                            task_id=task_id,
                            event_type="system_event",
                            role="system",
                            content=notice,
                            is_error=False,
                        ))
                        await db.commit()
                        await self.broadcaster.broadcast(f"task:{task_id}", {
                            "event_type": "system_event",
                            "role": "system",
                            "content": notice,
                        })
                        msg.prompt = build_compacted_resume_prompt(
                            summary,
                            msg.current_message,
                        )
                        msg.allow_new_session = True
                        logger.info("Task %d compacted, new session will start with summary", task_id)

            # Capture launch params before closing DB session
            launch_kwargs = dict(
                instance_id=inst.id,
                prompt=msg.prompt,
                task_id=task_id,
                cwd=task.last_cwd or task.target_repo or os.getcwd(),
                model=effective_model,
                resume_session_id=task.session_id,
                git_env=git_env,
                thinking_budget=task.thinking_budget,
                effort_level=effort_level,
                chat_initiated=True,
                config_dir=config_dir,
                provider=task.provider,
                enable_workflows=task.enable_workflows,
                enabled_skills=effective_skills,
                system_prompt_mode=task.system_prompt_mode,
                source_log_id=msg.source_log_id,
                current_message=msg.current_message,
                queue_timestamp=msg.timestamp,
            )
            inst_id = inst.id
            task_provider = (task.provider or "claude").lower()

            # Build source metadata now, but persist/broadcast only after the
            # exact Task claim succeeds.  A lost CAS is retried with the same
            # QueuedMessage and must not create duplicate/phantom bubbles.
            source_log_pending = (
                not msg.source_logged
                and msg.source
                and (
                    msg.source.startswith("monitor:")
                    or msg.source.startswith("sub-agent:")
                )
            )
            monitor_log = None
            broadcast_data = None
            if source_log_pending:
                import json as _json
                src_label = "monitor" if msg.source.startswith("monitor:") else "sub-agent"
                log_raw: dict = {"source": src_label}
                if msg.monitor_session_id:
                    log_raw["monitor_session_id"] = msg.monitor_session_id
                monitor_log = LogEntry(
                    instance_id=inst.id,
                    task_id=task_id,
                    event_type="user_message",
                    role="user",
                    content=msg.user_message_text or msg.prompt,
                    raw_json=_json.dumps(log_raw),
                    is_error=False,
                )
                broadcast_data = {
                    "event_type": "user_message",
                    "role": "user",
                    "content": msg.user_message_text or msg.prompt,
                    "source": src_label,
                }
                if msg.monitor_session_id:
                    broadcast_data["monitor_session_id"] = msg.monitor_session_id

            status_before_launch = task.status
            retry_count_before_launch = task.retry_count
            completed_at_before_launch = task.completed_at
            instance_id_before_launch = task.instance_id
            session_id_before_launch = task.session_id
            started_at_before_launch = task.started_at
            claim_values: dict = {
                "status": "executing",
                "instance_id": inst.id,
                "completed_at": None,
            }
            if msg.command_skills:
                # The skill view becomes visible atomically with launch
                # ownership, before the process can make its first MCP call.
                claim_values["enabled_skills"] = effective_skills
            # The exact status/owner transition is the maintenance admission
            # commit point. A paused updater either observes this executing
            # generation or prevents the launch before the CAS is committed.
            async with self.task_start_guard():
                status_claim = await db.execute(
                    update(Task)
                    .where(
                        Task.id == task_id,
                        Task.status == status_before_launch,
                        Task.retry_count == retry_count_before_launch,
                        (
                            Task.instance_id.is_(None)
                            if instance_id_before_launch is None
                            else Task.instance_id == instance_id_before_launch
                        ),
                        (
                            Task.session_id.is_(None)
                            if session_id_before_launch is None
                            else Task.session_id == session_id_before_launch
                        ),
                        (
                            Task.started_at.is_(None)
                            if started_at_before_launch is None
                            else Task.started_at == started_at_before_launch
                        ),
                        (
                            Task.completed_at.is_(None)
                            if completed_at_before_launch is None
                            else Task.completed_at == completed_at_before_launch
                        ),
                        Task.worker_id.is_(None),
                        Task.shared_from_id.is_(None),
                        task_retry_not_superseded_predicate(),
                    )
                    .values(**claim_values)
                )
                if not status_claim.rowcount:
                    await db.rollback()
                    current = await db.get(
                        Task,
                        task_id,
                        populate_existing=True,
                    )
                    if task_is_pr_review_superseded(current):
                        logger.info(
                            "Discarding queued message after PR review task %s "
                            "was superseded before final launch claim",
                            task_id,
                        )
                        return
                    logger.info(
                        "Queued message admission for task %s was superseded by "
                        "a concurrent status/owner generation change",
                        task_id,
                    )
                    raise QueuedMessagePrelaunchError(
                        "Queued task ownership changed before launch; preserving "
                        "the exact message for retry"
                    )
                queued_turn_generation = (
                    await self._read_task_status_generation(db, task_id)
                )
                if queued_turn_generation is None:
                    await db.rollback()
                    raise QueuedMessagePrelaunchError(
                        "Queued task disappeared after launch claim"
                    )
                cleanup_state["has_temp_skills"] = bool(
                    msg.command_skills
                )
                if monitor_log is not None:
                    db.add(monitor_log)
                await db.commit()
                if monitor_log is not None:
                    msg.source_log_id = monitor_log.id
                    launch_kwargs["source_log_id"] = monitor_log.id
            if broadcast_data is not None:
                await self.broadcaster.broadcast(
                    f"task:{task_id}", broadcast_data
                )
                msg.source_logged = True

            # Claim the instance across the launch window: launch() only flips
            # its DB status to "running" once the PTY session is fully spawned,
            # so until then both the dispatch loop and other queued-message
            # launches must treat it as taken (prod task #676). Released in
            # finally so a failed launch can't leak the claim and wedge the
            # instance out of the dispatch pool forever.
            try:
                await self.instance_manager.launch(**launch_kwargs)
            except asyncio.CancelledError:
                # Stop/cancel owns the exact executing generation. Do not race
                # its termination CAS by publishing a synthetic terminal or
                # rolling the Task back to an earlier status.
                await db.rollback()
                raise
            except Exception as exc:
                from backend.services.codex_app_server import (
                    CodexAppServerBusyError,
                    CodexThreadHomeMismatchError,
                )
                from backend.services.cloudrouter_accounts import (
                    CloudRouterAccountError,
                    CloudRouterUnsafePathError,
                )

                # Known routing/admission errors cannot have started this turn.
                # For arbitrary spawn failures, the absence of this exact
                # InstanceManager generation is the proof required before the
                # message may be retried.  If a generation remains tracked,
                # fail closed instead of replaying a potentially-started turn.
                known_prelaunch = isinstance(
                    exc,
                    (
                        CodexAccountRoutingError,
                        CodexAppServerBusyError,
                        CodexThreadHomeMismatchError,
                        InstanceAlreadyRunningError,
                    ),
                )
                permanent_prelaunch = isinstance(
                    exc, CloudRouterAccountError
                )
                safe_to_retry = (
                    known_prelaunch
                    or self.instance_manager.processes.get(inst_id) is None
                )
                permanent_notice_data = None
                rollback_values = {
                    "status": (
                        status_before_launch if safe_to_retry else "failed"
                    ),
                    "instance_id": instance_id_before_launch,
                    "completed_at": completed_at_before_launch,
                }
                if not safe_to_retry:
                    rollback_values.update(
                        instance_id=inst_id,
                        completed_at=datetime.utcnow(),
                        error_message=(
                            "Launch failed after a process generation may have "
                            f"started: {exc}"
                        )[:2000],
                    )
                if cleanup_state["has_temp_skills"]:
                    rollback_values["enabled_skills"] = dict(original_skills)
                    cleanup_state["has_temp_skills"] = False
                restored = await db.execute(
                    update(Task)
                    .where(
                        *self._task_status_generation_predicates(
                            queued_turn_generation
                        ),
                        (
                            Task.session_id.is_(None)
                            if session_id_before_launch is None
                            else Task.session_id == session_id_before_launch
                        ),
                        Task.worker_id.is_(None),
                        Task.shared_from_id.is_(None),
                    )
                    .values(**rollback_values)
                )
                failed_generation = None
                if not safe_to_retry and restored.rowcount:
                    failed_generation = (
                        await self._read_task_status_generation(db, task_id)
                    )
                if permanent_prelaunch and restored.rowcount:
                    unsafe_cloudrouter_config = isinstance(
                        exc, CloudRouterUnsafePathError
                    )
                    permanent_notice = LogEntry(
                        instance_id=None,
                        task_id=task_id,
                        event_type="system_event",
                        role="system",
                        content=(
                            (
                                "API 账号配置安全校验失败，本条消息未执行。"
                                "请检查或重新保存该 API 账号后再发送。"
                            )
                            if unsafe_cloudrouter_config
                            else (
                                "API 账号当前不可用，本条消息未执行。"
                                "请刷新账号，并检查启用状态、模型支持或额度后"
                                "重新发送。"
                            )
                        ),
                        is_error=True,
                    )
                    db.add(permanent_notice)
                    await db.flush()
                    permanent_notice_data = {
                        "id": permanent_notice.id,
                        "instance_id": None,
                        "task_id": task_id,
                        "event_type": "system_event",
                        "role": "system",
                        "content": permanent_notice.content,
                        "is_error": True,
                        "timestamp": (
                            permanent_notice.timestamp or datetime.utcnow()
                        ).isoformat(),
                    }
                await db.commit()
                if permanent_notice_data is not None:
                    await self.broadcaster.broadcast(
                        f"task:{task_id}",
                        permanent_notice_data,
                    )
                if failed_generation is not None:
                    await self._broadcast_task_status_generation(
                        failed_generation,
                        instance_id=inst_id,
                        db=db,
                    )
                if (
                    safe_to_retry
                    and not known_prelaunch
                    and not permanent_prelaunch
                ):
                    raise QueuedMessagePrelaunchError(
                        f"Queued message launch failed before process creation: {exc}"
                    ) from exc
                raise
            finally:
                await self._release_instance_reservation(
                    inst_id, claim_token
                )
                msg.instance_claim = None

            await self.broadcaster.broadcast("tasks", {
                "event": "status_change",
                "task_id": task_id,
                "new_status": "executing",
                "instance_id": inst_id,
            })
        # DB session closed — process runs independently
        if launch_admission["held"]:
            launch_admission["held"] = False
            self._chat_launch_admission_lock.release()

        # Phase 2: wait for process to finish (no DB held)
        try:
            process = self.instance_manager.processes.get(inst_id)
            consumer = self.instance_manager._tasks.get(inst_id)
            pty_managed_turn = bool(
                task_provider == "claude"
                and self.instance_manager.is_pty_managed_turn(
                    inst_id, process
                )
            )
            if not pty_managed_turn:
                # A subprocess/app-server output consumer may react to a
                # transient or account limit by launching a replacement turn
                # before it exits. Follow that chain to completion so this
                # per-task queue cannot start the next message concurrently on
                # the same native session.
                while process is not None:
                    await self._wait_process(
                        process, task, "Chat run", instance_id=inst_id
                    )
                    if consumer is not None and consumer is not asyncio.current_task():
                        try:
                            await asyncio.shield(consumer)
                        except Exception:
                            logger.exception(
                                "Output consumer failed while serializing task %d",
                                task_id,
                            )
                    next_process = self.instance_manager.processes.get(inst_id)
                    next_consumer = self.instance_manager._tasks.get(inst_id)
                    if next_process is None or (
                        next_process is process and next_consumer is consumer
                    ):
                        break
                    process, consumer = next_process, next_consumer
            elif process:
                # Claude PTY represents one turn through a persistent session;
                # its retry/switch handling remains in the PTY branch below.
                await self._wait_process(
                    process, task, "Chat run", instance_id=inst_id
                )
            # Status management is handled by _consume_output (chat_initiated=True)
            #
            # FullMirrorCCMBackend.on_exit is the sole owner of PTY transient
            # retries. It chains every replacement proxy back to ``process``,
            # so the wait above already covers the whole retry sequence.
            # Retrying again here would reset an exhausted attempt tally and
            # could keep a failed API turn spinning indefinitely.

            # PTY proactive pool switch: if this turn saw an actionable
            # rate_limit_event, migrate the session to a healthy account so the
            # next message uses fresh quota. In -p subprocess mode this is
            # handled by _try_proactive_pool_switch in _consume_output; PTY
            # mode needs it here because the process stays alive (exit_code 0).
            if (
                inst_id is not None
                and pty_managed_turn
                and self.instance_manager.pty_rate_limit_seen(inst_id)
            ):
                await self.instance_manager._try_proactive_pool_switch(
                    inst_id,
                    task_id,
                    rate_limit_info=self.instance_manager.pty_rate_limit_info(
                        inst_id
                    ),
                )
                self.instance_manager.clear_pty_rate_limit(inst_id)
        finally:
            # FullMirrorCCMBackend.on_exit is the sole authoritative PTY
            # Task→Instance finalizer. A queue cancellation or wait failure
            # must never manufacture a successful ``completed`` generation.
            pass

    async def _compact_session(
        self,
        task_id: int,
        session_id: str,
        db,
        *,
        exclude_log_entry_id: int | None = None,
        post_source_injects_are_current: bool = False,
    ) -> str | None:
        """Collect recent logged history for a replacement native session.

        Log rows do not currently carry a native session id, so ``session_id``
        identifies the caller's generation rather than filtering the query.
        The current user row is bounded by its exact id, so the same request
        cannot appear in both the historical summary and the new-message
        section. Later ordinary queued requests are excluded, while live
        injections made into the still-active turn are retained.
        """

        del session_id
        try:
            from backend.models.log_entry import LogEntry
            from backend.models.task import Task

            task = await db.get(Task, task_id)

            user_conditions = [
                LogEntry.task_id == task_id,
                LogEntry.event_type == "user_message",
            ]
            if exclude_log_entry_id is not None:
                user_conditions.append(LogEntry.id < exclude_log_entry_id)
            user_result = await db.execute(
                select(LogEntry)
                .where(*user_conditions)
                .order_by(LogEntry.id.desc())
                .limit(8)
            )
            users = list(reversed(user_result.scalars().all()))

            def _user_text(entry: LogEntry) -> tuple[str, str]:
                content = entry.content or ""
                source = ""
                if entry.raw_json:
                    try:
                        raw = json.loads(entry.raw_json)
                        if isinstance(raw, dict):
                            if isinstance(raw.get("raw_content"), str):
                                content = raw["raw_content"]
                            if isinstance(raw.get("source"), str):
                                source = raw["source"]
                    except (json.JSONDecodeError, TypeError):
                        pass
                return content[:600], source

            async def _last_assistant_between(
                lower_id: int,
                upper_id: int | None,
            ) -> LogEntry | None:
                conditions = [
                    LogEntry.task_id == task_id,
                    LogEntry.id > lower_id,
                    or_(
                        LogEntry.event_type == "result",
                        (
                            (LogEntry.event_type == "message")
                            & (LogEntry.role == "assistant")
                        ),
                    ),
                    LogEntry.content.is_not(None),
                    LogEntry.content != "",
                    LogEntry.is_error.is_(False),
                ]
                if upper_id is not None:
                    conditions.append(LogEntry.id < upper_id)
                result = await db.execute(
                    select(LogEntry)
                    .where(*conditions)
                    .order_by(LogEntry.id.desc())
                    .limit(1)
                )
                return result.scalar_one_or_none()

            history_blocks: list[str] = []
            for index, user_entry in enumerate(users):
                next_user_id = (
                    users[index + 1].id
                    if index + 1 < len(users)
                    else exclude_log_entry_id
                )
                user_content, source = _user_text(user_entry)
                label = (
                    "用户当时的执行中补充/纠正"
                    if source == "inject"
                    else "用户当时的消息"
                )
                block = f"[{label}]\n{user_content}"
                assistant_entry = await _last_assistant_between(
                    user_entry.id,
                    next_user_id,
                )
                if assistant_entry is not None:
                    block += (
                        "\n\n[该阶段助手的最后输出]\n"
                        f"{(assistant_entry.content or '')[:1200]}"
                    )
                history_blocks.append(block)

            # A later ordinary user row is only queued while this task's single
            # consumer is still processing the current turn.  It therefore
            # must not hide a still-later live injection into the active turn.
            # Keep all explicit injections but never import ordinary queued
            # requests. queue_timestamp preserves their execution order when
            # this turn is retried after compaction.
            if exclude_log_entry_id is not None:
                later_users_result = await db.execute(
                    select(LogEntry)
                    .where(
                        LogEntry.task_id == task_id,
                        LogEntry.event_type == "user_message",
                        LogEntry.id > exclude_log_entry_id,
                    )
                    .order_by(LogEntry.id.asc())
                )
                queued_during_previous_turn: list[LogEntry] = []
                for later_user in later_users_result.scalars().all():
                    _, source = _user_text(later_user)
                    if source == "inject":
                        queued_during_previous_turn.append(later_user)
                post_current_blocks: list[tuple[int, str]] = []
                for injected_user in queued_during_previous_turn:
                    injected_content, _ = _user_text(injected_user)
                    if post_source_injects_are_current:
                        inject_label = (
                            "当前消息执行期间的后续补充/纠正"
                            "（比基础当前消息更新，冲突时以此为准）"
                        )
                    else:
                        inject_label = (
                            "当前消息排队期间对上一阶段的补充/纠正"
                        )
                    post_current_blocks.append(
                        (
                            injected_user.id,
                            f"[{inject_label}]\n"
                            f"{injected_content}",
                        )
                    )
                trailing_assistant = await _last_assistant_between(
                    exclude_log_entry_id,
                    None,
                )
                if trailing_assistant is not None:
                    trailing_label = (
                        "当前消息执行期间的最后输出（最新状态）"
                        if post_source_injects_are_current
                        else "当前消息排队期间完成的上一阶段最后输出"
                    )
                    post_current_blocks.append(
                        (
                            trailing_assistant.id,
                            f"[{trailing_label}]\n"
                            f"{(trailing_assistant.content or '')[:1200]}",
                        )
                    )
                history_blocks.extend(
                    block
                    for _, block in sorted(post_current_blocks)
                )

            # Prefer complete recent stages over a wide but shallow history.
            # Walk backwards under a fixed budget, then restore chronological
            # order.  Old stages are the first thing discarded.
            recent_blocks: list[str] = []
            remaining = 6500
            for block in reversed(history_blocks):
                if len(block) > remaining:
                    if recent_blocks:
                        continue
                    block = block[:remaining]
                recent_blocks.append(block)
                remaining -= len(block) + 2
                if remaining <= 0:
                    break
            recent_blocks.reverse()

            parts: list[str] = []
            if recent_blocks:
                parts.append(
                    "## 近期对话（按真实发生顺序，越靠后越新）\n"
                    + "\n\n".join(recent_blocks)
                )

            # Put the original description last and label it explicitly.  It
            # is provenance, not an instruction that should outrank months of
            # follow-up conversation.
            def _truncate_background(
                text: str,
                limit: int = 2000,
            ) -> str:
                if len(text) <= limit:
                    return text
                omission = "\n...[中间省略]...\n"
                head_length = (limit - len(omission)) // 2
                tail_length = limit - len(omission) - head_length
                return (
                    text[:head_length]
                    + omission
                    + text[-tail_length:]
                )

            original_background = task.description if task else None
            if (
                original_background
                and original_background.lstrip().startswith(
                    "[Context compacted]"
                )
            ):
                # Lifecycle retries replace Task.description with a compacted
                # wrapper. On a later overflow, retain only the stable original
                # background section instead of recursively nesting the whole
                # previous summary and its recovery instructions.
                marker_index = original_background.rfind(
                    "## 原始任务背景（"
                )
                if marker_index >= 0:
                    marker_end = original_background.find(
                        "\n",
                        marker_index,
                    )
                    original_background = (
                        original_background[marker_end + 1 :].strip()
                        if marker_end >= 0
                        else None
                    )
                else:
                    # Compatibility with the legacy lifecycle wrapper:
                    # [Context compacted]\n{summary}\n\n---\n\n{original}
                    legacy_separator = "\n\n---\n\n"
                    if legacy_separator in original_background:
                        original_background = original_background.rsplit(
                            legacy_separator,
                            1,
                        )[1].strip()
                    else:
                        original_background = None
            if original_background:
                parts.append(
                    "## 原始任务背景（最低优先级，可能已被近期信息取代）\n"
                    f"{_truncate_background(original_background)}"
                )

            summary = "\n\n".join(parts)
            return summary if summary.strip() else None
        except Exception as e:
            logger.exception("compact session failed for task %d: %s", task_id, e)
            return None
