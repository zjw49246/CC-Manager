"""TaskMigrator — 统一迁移机制（elastic-worker 设计 §10）。

三个场景同一本质："把 task 的执行态从机器 A 搬到机器 B"：
1. 实时切换执行位置（PUT /api/tasks/{id} 改 worker_id）
2. Worker 销毁 = 对其全部 task migrate 回本机
3. 跨机克隆（只搬 session 的子集操作）

搬运原则：先复制后切指针——源机文件不删，任一步失败状态复原可重试。
前提：所有机器 WORKSPACE_DIR 一致（bootstrap 保证），cwd 编码出的 session
路径两边天然对得上，迁过去 --resume 直接续聊。
"""

from __future__ import annotations

import asyncio
import copy
import glob
import logging
import os
import re
import secrets
import shlex
import shutil
import tempfile
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import PurePosixPath

import httpx
from sqlalchemy import JSON, and_, func, or_, select, update
from sqlalchemy.exc import IntegrityError

from backend.config import settings
from backend.models.capability import (
    ACTIVE_RESUME_OUTBOX_STATUSES,
    CapabilityResumeOutbox,
)
from backend.models.project import Project
from backend.models.plan import Plan
from backend.models.sub_agent import SubAgentSession
from backend.models.task import Task
from backend.models.task_migration import (
    MANAGER_ACTIVE_TASK_MIGRATION_PHASES,
    TaskMigrationOperation,
)
from backend.models.worker import Worker
from backend.services.ssh_executor import SSHExecutor, worker_known_hosts_path
from backend.services.cancellation import settle_awaitable
from backend.services.pr_review_runtime import is_pr_sandbox_task
from backend.services.task_queue import (
    PR_REVIEW_SUPERSEDED_METADATA_KEY,
    task_retry_not_superseded_predicate,
)
from backend.services.task_id_namespace import (
    TaskIdNamespaceError,
    validate_manager_allocated_task_id,
)
from backend.services.test_harness_owner_fence import (
    has_active_test_harness_owner_graph,
    no_active_test_harness_owner_graph_predicate,
)
from backend.services.worker_proxy import get_task_operation_lock
from backend.services.worker_relay import (
    WORKER_REMOTE_MATERIALIZED_METADATA_KEY,
    has_worker_execution_quarantine,
    worker_child_mirror_identity,
)
from backend.services.worker_routing_config import (
    InvalidWorkerRoutingMarker,
    WORKER_ROUTING_SAFE_STATUSES,
    WorkerMigrationImportReservation,
    read_worker_migration_import_reservation,
)
from backend.services.worker_task_termination import (
    active_worker_task_termination_receipt,
    no_active_worker_task_termination_predicate,
)

logger = logging.getLogger(__name__)
_CODEX_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_COPY_BUFFER_SIZE = 1024 * 1024
_SHARED_WORKSPACE_SYNC_CACHE: ContextVar[set[str] | None] = ContextVar(
    "ccm_shared_workspace_sync_cache",
    default=None,
)
_COORDINATED_TASK_UPDATE_FIELDS = frozenset({
    "title",
    "model",
    "codex_service_tier",
    "effort_level",
    "thinking_budget",
    "system_prompt_mode",
    "timeout_hours",
    "sort_order",
    "description",
    "priority",
    "project_id",
    "target_repo",
    "target_branch",
    "max_retries",
    "max_iterations",
    "must_complete",
    "mode",
    "goal_condition",
    "goal_max_turns",
    "goal_evaluator_model",
    "enable_workflows",
    "enabled_skills",
    "selected_user_skills",
    "provider",
    "starred",
    "tags",
    "attention_tag",
    # Internal transport for validated User Skill snapshots.
    "metadata_",
})


@asynccontextmanager
async def shared_workspace_sync_cache():
    """Deduplicate workspace copies inside one explicit migration batch.

    A ContextVar keeps concurrent Task migrations isolated while allowing a
    Worker destroy coordinator to pre-copy every Project and then migrate all
    of its Tasks without rsyncing the same repository once per Task.
    """

    token = _SHARED_WORKSPACE_SYNC_CACHE.set(set())
    try:
        yield
    finally:
        _SHARED_WORKSPACE_SYNC_CACHE.reset(token)


class MigrationError(Exception):
    pass


async def _assert_no_active_ccm_child_mirrors(db, task: Task) -> None:
    """Fence migration against Worker-owned Monitor/Sub-Agent processes.

    ``remote_id`` is only unique inside one Worker.  A Task can retain valid
    historical mirrors after moving from Worker A to Worker B, so the current
    owner is the full ``(worker_id, incarnation_id, remote_id)`` tuple stored
    in strict mirror metadata.  Historical tuples do not block a later move;
    an unparseable/duplicate tuple is ambiguous and therefore fails closed.
    """

    rows = list(
        (
            await db.execute(
                select(SubAgentSession).where(
                    SubAgentSession.task_id == task.id,
                    SubAgentSession.agent_type.in_(("monitor", "sub_agent")),
                    SubAgentSession.source == "ccm",
                )
            )
        ).scalars()
    )
    current_worker_id = task.worker_id
    incarnation_id = task.incarnation_id
    if current_worker_id is not None and (
        type(current_worker_id) is not int
        or current_worker_id <= 0
        or not isinstance(incarnation_id, str)
        or re.fullmatch(r"[0-9a-f]{32}", incarnation_id) is None
    ):
        raise MigrationError(
            "Task current Worker/incarnation identity is malformed; reconcile "
            "it before migration"
        )

    seen_remote_identities: set[tuple[int, str, int]] = set()
    for row in rows:
        remote_id = row.remote_id
        if remote_id is None:
            # A terminal row with no remote identity is legitimate history
            # from a period when the Task ran on the Manager.  A running row
            # would be orphaned by any location change.  Non-empty metadata
            # without a remote id is neither a valid local row nor a Worker
            # mirror and cannot be classified safely.
            if row.meta is not None:
                raise MigrationError(
                    "Task has an ambiguous or malformed CCM Worker child "
                    "mirror; reconcile it before migration"
                )
            if row.status == "running":
                raise MigrationError(
                    "Task has an active CCM Monitor/Sub-Agent; stop it before "
                    "migration"
                )
            continue

        identity = worker_child_mirror_identity(row.meta)
        if identity is None or remote_id != identity[2]:
            raise MigrationError(
                "Task has an ambiguous or malformed CCM Worker child mirror; "
                "reconcile it before migration"
            )
        if identity in seen_remote_identities:
            raise MigrationError(
                "Task has duplicate CCM Worker child mirror identity; "
                "reconcile it before migration"
            )
        seen_remote_identities.add(identity)

        if (
            current_worker_id is not None
            and identity[0] == current_worker_id
            and identity[1] == incarnation_id
            and row.status == "running"
        ):
            raise MigrationError(
                "Task has an active current-Worker CCM Monitor/Sub-Agent; "
                "stop it before migration"
            )


def _no_active_capability_resume_outbox_predicate():
    """Fence migration against a durable capability G -> G+1 handoff."""

    return ~(
        select(CapabilityResumeOutbox.id)
        .where(
            CapabilityResumeOutbox.task_id == Task.id,
            CapabilityResumeOutbox.status.in_(ACTIVE_RESUME_OUTBOX_STATUSES),
        )
        .correlate(Task)
        .exists()
    )


@dataclass(frozen=True)
class MigrationTaskGeneration:
    """Exact Manager-side Task generation owned by one migration attempt."""

    task_id: int
    incarnation_id: str | None
    worker_id: int | None
    status: str
    retry_count: int
    turn_generation: int
    instance_id: int | None
    started_at: datetime | None
    completed_at: datetime | None
    migration_operation_id: str | None = None
    migration_operation_sequence: int | None = None
    migration_target_worker_id: int | None = None
    migration_source_status: str | None = None


def migration_task_generation(task: Task) -> MigrationTaskGeneration:
    return MigrationTaskGeneration(
        task_id=task.id,
        incarnation_id=task.incarnation_id,
        worker_id=task.worker_id,
        status=task.status,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        instance_id=task.instance_id,
        started_at=task.started_at,
        completed_at=task.completed_at,
        migration_source_status=task.status,
    )


def _nullable_eq(column, value):
    return column.is_(None) if value is None else column == value


def migration_generation_predicates(
    generation: MigrationTaskGeneration,
) -> tuple:
    return (
        Task.id == generation.task_id,
        _nullable_eq(Task.incarnation_id, generation.incarnation_id),
        (
            Task.worker_id.is_(None)
            if generation.worker_id is None
            else Task.worker_id == generation.worker_id
        ),
        Task.shared_from_id.is_(None),
        Task.status == generation.status,
        Task.retry_count == generation.retry_count,
        Task.turn_generation == generation.turn_generation,
        _nullable_eq(Task.instance_id, generation.instance_id),
        _nullable_eq(Task.started_at, generation.started_at),
        _nullable_eq(Task.completed_at, generation.completed_at),
    )


def _migration_operation_predicates(
    claimed: MigrationTaskGeneration,
    *,
    phases: tuple[str, ...] = MANAGER_ACTIVE_TASK_MIGRATION_PHASES,
) -> tuple:
    operation_id = claimed.migration_operation_id
    operation_sequence = claimed.migration_operation_sequence
    source_status = claimed.migration_source_status
    if (
        operation_id is None
        or operation_sequence is None
        or source_status is None
    ):
        raise MigrationError("Task migration has no durable operation identity")
    return (
        TaskMigrationOperation.operation_id == operation_id,
        TaskMigrationOperation.operation_sequence == operation_sequence,
        TaskMigrationOperation.side == "manager",
        TaskMigrationOperation.active_task_id == claimed.task_id,
        TaskMigrationOperation.task_id == claimed.task_id,
        TaskMigrationOperation.task_incarnation_id == claimed.incarnation_id,
        TaskMigrationOperation.retry_count == claimed.retry_count,
        TaskMigrationOperation.turn_generation == claimed.turn_generation,
        _nullable_eq(
            TaskMigrationOperation.source_worker_id,
            claimed.worker_id,
        ),
        _nullable_eq(
            TaskMigrationOperation.target_worker_id,
            claimed.migration_target_worker_id,
        ),
        TaskMigrationOperation.source_status == source_status,
        _nullable_eq(
            TaskMigrationOperation.instance_id,
            claimed.instance_id,
        ),
        _nullable_eq(
            TaskMigrationOperation.started_at,
            claimed.started_at,
        ),
        _nullable_eq(
            TaskMigrationOperation.completed_at,
            claimed.completed_at,
        ),
        TaskMigrationOperation.phase.in_(phases),
    )


def _task_value_predicates(values: dict) -> tuple:
    """SQL predicates for portable scalar coordinated values.

    PostgreSQL ``JSON`` (unlike ``JSONB``) has no equality operator, and
    SQLite JSON text equality depends on key order.  JSON values are compared
    in Python only after a scalar Task writer has locked the transaction.
    """

    predicates = []
    for field, value in values.items():
        column = getattr(Task, field)
        if isinstance(column.type, JSON):
            continue
        predicates.append(
            column.is_(None) if value is None else column == value
        )
    return tuple(predicates)


def _task_json_values_match(task: Task, values: dict) -> bool:
    """Compare coordinated JSON snapshots on an already locked Task row."""

    for field, expected in values.items():
        column = getattr(Task, field)
        if isinstance(column.type, JSON) and getattr(task, field) != expected:
            return False
    return True


def _coordinated_task_updates(task_updates: dict | None) -> dict:
    updates = copy.deepcopy(task_updates or {})
    invalid = set(updates).difference(_COORDINATED_TASK_UPDATE_FIELDS)
    if invalid:
        raise MigrationError(
            "Unsupported coordinated migration fields: "
            + ", ".join(sorted(invalid))
        )
    return updates


async def _settle_despite_cancellation(awaitable):
    """Finish a finite migration barrier before delivering cancellation."""

    return await settle_awaitable(awaitable)


class TaskMigrator:
    def __init__(self, db_factory, relay, broadcaster=None):
        self.db_factory = db_factory
        self.relay = relay
        self.broadcaster = broadcaster
        self._locks: dict[int, asyncio.Lock] = {}
        # API-account retirement must not remove a credential/config while a
        # cross-Worker migration is reading or rebinding task execution state.
        # This short bookkeeping lock preserves parallel migrations; the
        # active counter, rather than the lock itself, spans each workflow.
        self._api_account_fence_lock = asyncio.Lock()
        self._active_migrations = 0
        self._api_account_retirement_reserved = False
        self._recovery_task: asyncio.Task | None = None
        self._recovery_stop = asyncio.Event()

    @staticmethod
    def _generation_from_operation(
        operation: TaskMigrationOperation,
    ) -> MigrationTaskGeneration:
        if (
            operation.side != "manager"
            or operation.active_task_id != operation.task_id
            or operation.phase not in MANAGER_ACTIVE_TASK_MIGRATION_PHASES
        ):
            raise MigrationError(
                "Task migration recovery found a non-active Manager operation"
            )
        return MigrationTaskGeneration(
            task_id=operation.task_id,
            incarnation_id=operation.task_incarnation_id,
            worker_id=operation.source_worker_id,
            status="migrating",
            retry_count=operation.retry_count,
            turn_generation=operation.turn_generation,
            instance_id=operation.instance_id,
            started_at=operation.started_at,
            completed_at=operation.completed_at,
            migration_operation_id=operation.operation_id,
            migration_operation_sequence=operation.operation_sequence,
            migration_target_worker_id=operation.target_worker_id,
            migration_source_status=operation.source_status,
        )

    @staticmethod
    def _reservation_from_operation(
        operation: TaskMigrationOperation,
    ) -> WorkerMigrationImportReservation:
        return WorkerMigrationImportReservation(
            operation_id=operation.operation_id,
            operation_sequence=operation.operation_sequence,
            incarnation_id=operation.task_incarnation_id,
            retry_count=operation.retry_count,
            turn_generation=operation.turn_generation,
            source_status=operation.source_status,
        )

    async def _recover_operation(
        self,
        *,
        operation_id: str,
        task_id: int,
    ) -> bool:
        lock = self._locks.setdefault(task_id, asyncio.Lock())
        async with lock:
            async with self._migration_account_guard():
                async with get_task_operation_lock(task_id):
                    async with self.db_factory() as db:
                        operation = await db.get(
                            TaskMigrationOperation,
                            operation_id,
                            populate_existing=True,
                        )
                        if operation is None or operation.active_task_id is None:
                            return False
                        claimed = self._generation_from_operation(operation)
                        reservation = self._reservation_from_operation(operation)
                        phase = operation.phase

                    target_worker_id = claimed.migration_target_worker_id
                    destination = (
                        await self._get_worker(target_worker_id)
                        if target_worker_id is not None
                        else None
                    )
                    if target_worker_id is not None and destination is None:
                        raise MigrationError(
                            f"Migration destination Worker {target_worker_id} disappeared"
                        )

                    if phase == "committed_pending_ack":
                        if destination is None:
                            raise MigrationError(
                                "Committed migration has no destination Worker"
                            )
                        await self._commit_worker_task_import(
                            destination,
                            task_id=task_id,
                            reservation=reservation,
                        )
                        await self._settle_committed_migration(
                            claimed,
                            restored_status=operation.source_status,
                        )
                    else:
                        if phase in {"claimed", "remote_prepared"}:
                            restored = await self._restore_migration_claim(
                                claimed,
                                operation.source_status,
                            )
                            if not restored:
                                raise MigrationError(
                                    "Migration recovery lost its source claim"
                                )
                        elif phase == "rollback_pending":
                            await self._read_claimed_task(claimed)
                        else:
                            raise MigrationError(
                                f"Unsupported migration recovery phase {phase!r}"
                            )
                        if destination is not None:
                            await self._rollback_worker_task_import(
                                destination,
                                task_id=task_id,
                                reservation=reservation,
                            )
                        await self._settle_rolled_back_migration(
                            claimed,
                            restored_status=operation.source_status,
                        )

                    await self._broadcast_status(
                        task_id,
                        "migrating",
                        operation.source_status,
                    )
                    return True

    async def recover_once(self, *, limit: int = 64) -> int:
        """Reconcile crash-left Manager operations without reopening Tasks."""

        async with self.db_factory() as db:
            rows = list(
                (
                    await db.execute(
                        select(
                            TaskMigrationOperation.operation_id,
                            TaskMigrationOperation.task_id,
                        )
                        .where(
                            TaskMigrationOperation.side == "manager",
                            TaskMigrationOperation.active_task_id.isnot(None),
                            TaskMigrationOperation.phase.in_(
                                MANAGER_ACTIVE_TASK_MIGRATION_PHASES
                            ),
                        )
                        .order_by(
                            TaskMigrationOperation.task_id,
                            TaskMigrationOperation.operation_sequence,
                        )
                        .limit(limit)
                    )
                ).all()
            )

        recovered = 0
        for operation_id, task_id in rows:
            try:
                if await self._recover_operation(
                    operation_id=operation_id,
                    task_id=task_id,
                ):
                    recovered += 1
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Task %s migration operation %s recovery remains pending",
                    task_id,
                    operation_id,
                )
        return recovered

    async def _recovery_loop(self) -> None:
        while not self._recovery_stop.is_set():
            try:
                await asyncio.wait_for(self._recovery_stop.wait(), timeout=15.0)
                continue
            except TimeoutError:
                pass
            try:
                await self.recover_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Task migration recovery pass failed")

    async def start(self) -> None:
        if self._recovery_task is not None and not self._recovery_task.done():
            return
        self._recovery_stop.clear()
        self._recovery_task = asyncio.create_task(
            self._recovery_loop(),
            name="task-migration-recovery",
        )

    async def shutdown(self) -> None:
        self._recovery_stop.set()
        task = self._recovery_task
        self._recovery_task = None
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _release_migration_admission(self) -> None:
        async with self._api_account_fence_lock:
            self._active_migrations = max(0, self._active_migrations - 1)

    @asynccontextmanager
    async def _migration_account_guard(self):
        async with self._api_account_fence_lock:
            if self._api_account_retirement_reserved:
                raise MigrationError(
                    "API account deletion is in progress; retry task migration",
                )
            self._active_migrations += 1
        try:
            yield
        finally:
            release, cancellation = await _settle_despite_cancellation(
                self._release_migration_admission()
            )
            release.result()
            if cancellation is not None:
                raise cancellation

    async def _release_api_account_retirement(self) -> None:
        async with self._api_account_fence_lock:
            self._api_account_retirement_reserved = False

    @asynccontextmanager
    async def api_account_retirement_guard(self):
        """Reject retirement during migration and new migration during cleanup."""

        async with self._api_account_fence_lock:
            if (
                self._api_account_retirement_reserved
                or self._active_migrations > 0
            ):
                raise MigrationError(
                    "A task migration is using account runtime state",
                )
            self._api_account_retirement_reserved = True
        try:
            yield
        finally:
            release, cancellation = await _settle_despite_cancellation(
                self._release_api_account_retirement()
            )
            release.result()
            if cancellation is not None:
                raise cancellation

    # ------------------------------------------------------------------
    # 入口
    # ------------------------------------------------------------------

    async def migrate(
        self,
        task_id: int,
        target_worker_id: int | None,
        *,
        task_updates: dict | None = None,
    ):
        """把 task 迁到 target（worker_id 或 None=本机）。"""
        coordinated_updates = _coordinated_task_updates(task_updates)
        lock = self._locks.setdefault(task_id, asyncio.Lock())
        if lock.locked():
            raise MigrationError("该 task 正在迁移中")
        async with lock:
            async with self._migration_account_guard():
                # Migration keeps its fast duplicate-request guard above, but
                # the full workflow also shares WorkerProxy's mutation lock.
                # Chat, retry and plan operations therefore cannot mutate the
                # source Worker while files/session state are being copied.
                async with get_task_operation_lock(task_id):
                    await self._migrate_locked(
                        task_id,
                        target_worker_id,
                        coordinated_updates=coordinated_updates,
                    )

    async def _migrate_locked(
        self,
        task_id: int,
        target: int | None,
        *,
        coordinated_updates: dict,
    ):
        try:
            validate_manager_allocated_task_id(task_id)
        except TaskIdNamespaceError as exc:
            raise MigrationError(str(exc)) from exc
        async with self.db_factory() as db:
            task = await db.get(Task, task_id)
            if not task:
                raise MigrationError("task 不存在")
            unfinished_migration = await db.scalar(
                select(TaskMigrationOperation.operation_id)
                .where(TaskMigrationOperation.active_task_id == task_id)
                .limit(1)
            )
            if unfinished_migration is not None:
                raise MigrationError(
                    "Task has an unfinished durable migration operation "
                    f"{unfinished_migration}; reconcile it before retrying"
                )
            if task.status == "waiting_capability":
                raise MigrationError(
                    "Task is waiting for its requested capability; durable "
                    "resume must finish before migration"
                )
            active_resume_outbox_id = await db.scalar(
                select(CapabilityResumeOutbox.id)
                .where(
                    CapabilityResumeOutbox.task_id == task_id,
                    CapabilityResumeOutbox.status.in_(
                        ACTIVE_RESUME_OUTBOX_STATUSES
                    ),
                )
                .order_by(CapabilityResumeOutbox.id)
                .limit(1)
            )
            if active_resume_outbox_id is not None:
                raise MigrationError(
                    "Task has an active capability resume outbox; durable "
                    "resume must finish before migration"
                )
            if await active_worker_task_termination_receipt(db, task_id):
                raise MigrationError(
                    "Worker task termination is still active; reconcile the "
                    "durable receipt before migrating"
                )
            if task.plan_target_task_id is not None:
                raise MigrationError(
                    "关联 Plan 不能脱离目标 Task 单独迁移"
                )
            if task.mode == "plan":
                raise MigrationError(
                    "Plan Tasks cannot be migrated through the generic Task "
                    "protocol; keep the legacy carrier on its original node"
                )
            if task.worker_id == target:
                if coordinated_updates:
                    raise MigrationError(
                        "Coordinated updates require a Worker location change"
                    )
                return  # 已在目标位置
            if task.worker_id is None and task.status in (
                "in_progress",
                "executing",
                "merging",
                "migrating",
            ):
                raise MigrationError(f"task 状态 {task.status}，先停止再切换")
            if task.worker_id is None and task.status == "pending":
                # A pending local Task has not crossed its provider boundary.
                # Importing it through the inert migration protocol would
                # create a cancelled Worker row, then restore a Manager-side
                # pending row carrying the materialized marker. Dispatcher
                # correctly refuses to re-create that row, so the Task would
                # be stranded and its initiating principal would be lost.
                # Initial Worker placement must use the normal claimed
                # forwarding protocol; generic migration is terminal/inert.
                raise MigrationError(
                    "pending Task cannot use the inert migration protocol; "
                    "let its current queue claim settle, then migrate an "
                    "inert terminal Task"
                )
            if task.status not in WORKER_ROUTING_SAFE_STATUSES:
                raise MigrationError(
                    f"Task source status {task.status} is not an inert "
                    "migration state"
                )
            if task.worker_id is not None:
                if has_worker_execution_quarantine(task.metadata_):
                    raise MigrationError(
                        "Worker task execution is quarantined; reconcile the "
                        "exact remote generation before migrating"
                    )
            if task.mode == "delivery_loop" or task.delivery_run_id is not None:
                raise MigrationError(
                    "Delivery Loop V1 is local-only; pause and finish the Run "
                    "on its owning Manager instead of migrating its Developer Task"
                )
            # NULL is the only disabled state.  Even a malformed/legacy empty
            # object must remain local and fail closed instead of bypassing
            # capability execution locality through truthiness.
            if task.capability_policy is not None:
                raise MigrationError(
                    "Auto capability policy is immutable and local-only; "
                    "create a new Task without it before migrating"
                )
            if task.worker_turn_handoff_id is not None:
                raise MigrationError(
                    "Worker follow-up turn handoff is still pending; wait for "
                    "the exact remote turn before migrating"
                )
            if is_pr_sandbox_task(task):
                raise MigrationError(
                    "Automated PR workflow Tasks are bound to their isolated "
                    "review runtime and cannot be migrated"
                )
            if (
                (task.metadata_ or {}).get(
                    PR_REVIEW_SUPERSEDED_METADATA_KEY
                )
                is True
            ):
                raise MigrationError("已被新 push 取代的 PR review task 不可迁移")
            if task.pty_background_generation is not None:
                raise MigrationError(
                    "Claude PTY 后台活动仍在输出，结束后再迁移"
                )
            if task.plan_target_task_id is not None:
                raise MigrationError(
                    "关联 Plan 不能脱离目标 Task 单独迁移"
                )
            from backend.services.plan_tasks import ACTIVE_PLAN_STATUSES

            blocking_versioned_plan_id = await db.scalar(
                select(Plan.id)
                .where(
                    Plan.target_task_id == task_id,
                    Plan.archived_at.is_(None),
                    Plan.active_run_id.isnot(None),
                )
                .limit(1)
            )
            if blocking_versioned_plan_id is not None:
                raise MigrationError(
                    f"关联 Plan #{blocking_versioned_plan_id} 仍有 active Run，"
                    "完成或取消后再迁移目标 Task"
                )

            blocking_plan_id = await db.scalar(
                select(Task.id)
                .where(
                    Task.plan_target_task_id == task_id,
                    Task.mode == "plan",
                    or_(
                        Task.status.in_(
                            (*ACTIVE_PLAN_STATUSES, "plan_review")
                        ),
                        and_(
                            Task.status == "completed",
                            Task.plan_approved.is_(True),
                            Task.plan_applied_at.is_(None),
                        ),
                    ),
                )
                .limit(1)
            )
            if blocking_plan_id is not None:
                raise MigrationError(
                    f"关联 Plan #{blocking_plan_id} 仍在运行、待审批或待应用，"
                    "完成处置后再迁移"
                )
            await _assert_no_active_ccm_child_mirrors(db, task)
            observed = migration_task_generation(task)
            prev_status = observed.status
            src_worker_id = observed.worker_id
            original_provider = (task.provider or "claude").lower()
            observed_update_values = {
                field: copy.deepcopy(getattr(task, field))
                for field in coordinated_updates
            }

        # Check the destination against the same validated final tuple that
        # will be imported and committed. Keep these values detached/in-memory
        # until the final pointer CAS so failure leaves Manager config intact.
        for field, value in coordinated_updates.items():
            setattr(task, field, copy.deepcopy(value))

        src = await self._get_worker(src_worker_id) if src_worker_id else None
        dst = await self._get_worker(target) if target else None
        if target and (
            not dst
            or dst.status != "ready"
            or dst.bootstrap_step is not None
        ):
            raise MigrationError(f"目标 Worker {dst.name if dst else target} 不可用")
        if src_worker_id and (not src or src.status not in ("ready", "destroying")):
            raise MigrationError(
                f"源 Worker {src.name if src else src_worker_id} 不可用（{src.status if src else '不存在'}）——"
                "无法取回执行态。可先启动该 Worker 再切换"
            )

        # Worker validation contains awaits, so the snapshot above is not a
        # claim.  Atomically transition the exact original state to migrating;
        # a dispatcher/user update which wins the race makes this CAS fail.
        # A request cancellation can arrive after COMMIT but before the
        # coroutine returns.  Settle the claim so we always know whether there
        # is an exact ``migrating`` generation that must be restored.
        migration_operation_id = secrets.token_hex(16)
        claim_awaitable = (
            self._claim_migration(
                observed,
                target_worker_id=target,
                operation_id=migration_operation_id,
                expected_values=observed_update_values,
            )
            if observed_update_values
            else self._claim_migration(
                observed,
                target_worker_id=target,
                operation_id=migration_operation_id,
            )
        )
        claim_operation, claim_cancellation = await _settle_despite_cancellation(
            claim_awaitable
        )
        try:
            claimed = claim_operation.result()
        except BaseException as claim_error:
            if claim_cancellation is not None:
                raise claim_cancellation from claim_error
            raise

        local_codex_target_home: str | None = None
        src_unsubscribed = False
        dst_subscribed = False
        claim_active = True
        destination_import: WorkerMigrationImportReservation | None = None
        try:
            if claim_cancellation is not None:
                raise claim_cancellation
            if src_worker_id is not None:
                # The status/retry/turn CAS above owns the source generation,
                # but the uncertainty marker is Manager-local JSON and is not
                # part of that portable SQL tuple.  Re-read it after the claim
                # and before any workspace/session copy so a marker persisted
                # during destination validation cannot be carried away or
                # silently discarded by migration.
                claimed_task = await self._read_claimed_task(
                    claimed,
                    expected_values=observed_update_values,
                )
                if has_worker_execution_quarantine(
                    claimed_task.metadata_
                ):
                    raise MigrationError(
                        "Worker task execution became quarantined before "
                        "migration; reconcile the exact remote generation"
                    )
            # The Plan admission path fences on this exact Task row before it
            # commits an active Run. Recheck after our claim so either commit
            # ordering is safe: migration-first rejects the Run; Run-first
            # makes this migration restore its claim before external effects.
            blocking_versioned_plan_id = await self._active_versioned_plan_id(
                task_id
            )
            if blocking_versioned_plan_id is not None:
                raise MigrationError(
                    f"关联 Plan #{blocking_versioned_plan_id} 仍有 active Run，"
                    "完成或取消后再迁移目标 Task"
                )
            if dst is not None:
                # Migration import is a mutating cross-host boundary.
                # Negotiate every mixed-version contract after all exact
                # Manager-side claim gates, but before status publication,
                # workspace copy, subscription changes, or remote creation.
                from backend.main import worker_proxy

                if worker_proxy is None:
                    raise MigrationError("Worker 代理未初始化")
                try:
                    await worker_proxy.require_worker_delegated_principal_support(
                        dst
                    )
                    await worker_proxy.require_worker_task_incarnation_support(
                        dst
                    )
                    await worker_proxy.require_worker_migration_import_support(
                        dst
                    )
                    if (
                        (task.provider or "claude").lower() == "codex"
                        and (task.codex_service_tier or "default") == "priority"
                    ):
                        await worker_proxy.require_worker_fast_support(dst, task)
                except Exception as exc:
                    raise MigrationError(str(exc)) from exc
            await self._broadcast_status(task_id, prev_status, "migrating")

            # 1. 源是 worker：先把 relay 收不到的字段同步回来（session_id/last_cwd）
            if src is not None:
                await self._sync_task_fields_from_worker(
                    src,
                    claimed,
                    expected_remote_status=prev_status,
                    protected_fields=frozenset(coordinated_updates),
                    expected_values=observed_update_values,
                )

            task = await self._read_claimed_task(
                claimed,
                expected_values=observed_update_values,
            )
            for field, value in coordinated_updates.items():
                setattr(task, field, copy.deepcopy(value))
            session_id = task.session_id
            project_id = task.project_id
            provider = (task.provider or "claude").lower()
            if provider != original_provider and session_id is not None:
                raise MigrationError(
                    "Task provider cannot change while an existing native "
                    "session may still emit output; start a new Task instead"
                )

            # 2. 工作目录搬运（含 .git + 未提交改动，无过滤全量 rsync）
            local_path = None
            if project_id:
                async with self.db_factory() as db:
                    project = await db.get(Project, project_id)
                local_path = project.local_path if project else None
            # Administrators may create a Worker Task without a Project.  Its
            # explicit repository is still durable workspace ownership and
            # must be copied before the Worker pointer moves.
            if not local_path:
                local_path = task.target_repo
            if local_path:
                await self.sync_workspace_once(src, dst, local_path)

            # 3. session 文件搬运（claude 落目标机 ~/.claude；codex 落 ~/.codex/sessions）
            if session_id:
                if provider == "codex":
                    moved_codex_home = await self._move_codex_session(
                        src, dst, session_id
                    )
                    if dst is None:
                        local_codex_target_home = moved_codex_home
                else:
                    await self._move_session(src, dst, session_id)

            # 4. 目标是 worker：确保项目记录 + 用同 ID 重建 task
            if dst is not None:
                from backend.main import worker_proxy
                worker_project_id = await worker_proxy.ensure_worker_project(dst, task)
                source_incarnation_id = task.incarnation_id
                if (
                    not isinstance(source_incarnation_id, str)
                    or re.fullmatch(
                        r"[0-9a-f]{32}",
                        source_incarnation_id,
                    )
                    is None
                ):
                    raise MigrationError(
                        "源 Task 缺少可验证的 immutable incarnation identity"
                    )
                if (
                    claimed.migration_operation_id is None
                    or claimed.migration_operation_sequence is None
                ):
                    raise MigrationError(
                        "Task migration lost its durable operation identity"
                    )
                destination_import = WorkerMigrationImportReservation(
                    operation_id=claimed.migration_operation_id,
                    operation_sequence=claimed.migration_operation_sequence,
                    incarnation_id=source_incarnation_id,
                    retry_count=task.retry_count,
                    turn_generation=task.turn_generation,
                    source_status=(
                        prev_status
                        if prev_status in WORKER_ROUTING_SAFE_STATUSES
                        else "cancelled"
                    ),
                )
                await self._ensure_worker_task(
                    dst,
                    task,
                    worker_project_id,
                    source_status=prev_status,
                    import_reservation=destination_import,
                )
                await self._mark_migration_remote_prepared(
                    claimed,
                    expected_values=observed_update_values,
                )

            # 5. relay 订阅切换
            if src is not None:
                self.relay.unsubscribe_task(src.id, task_id)
                src_unsubscribed = True
            if dst is not None:
                subscribe_operation, subscribe_cancellation = (
                    await _settle_despite_cancellation(
                        self.relay.subscribe_task(dst, task_id)
                    )
                )
                subscribe_operation.result()
                dst_subscribed = True
                if subscribe_cancellation is not None:
                    raise subscribe_cancellation

            # 6. 切指针 + 状态复原。仍以 migrating + 原 worker_id 为 CAS
            # 条件；并发取消/认领不能被迁移完成阶段覆盖。
            finish_operation, finish_cancellation = (
                await _settle_despite_cancellation(
                    self._finish_migration(
                        claimed=claimed,
                        target_worker_id=target,
                        restored_status=prev_status,
                        provider=provider,
                        local_codex_target_home=local_codex_target_home,
                        task_updates=coordinated_updates,
                        expected_values=observed_update_values,
                    )
                )
            )
            finish_operation.result()
            claim_active = False
            deferred_cancellation = finish_cancellation
            if dst is not None:
                if destination_import is None:
                    raise MigrationError(
                        "Destination migration lost its import reservation"
                    )
                commit_operation, commit_cancellation = (
                    await _settle_despite_cancellation(
                        self._commit_destination_migration(
                            dst,
                            claimed=claimed,
                            restored_status=prev_status,
                            reservation=destination_import,
                        )
                    )
                )
                commit_operation.result()
                if deferred_cancellation is None:
                    deferred_cancellation = commit_cancellation
            await self._broadcast_status(task_id, "migrating", prev_status)
            if deferred_cancellation is not None:
                raise deferred_cancellation
            logger.info("task %s migrated: %s -> %s", task_id, src_worker_id, target)
        except BaseException as migration_error:
            # ``CancelledError`` is a BaseException on supported Python
            # versions.  Rollback must finish despite repeated cancellation;
            # otherwise the durable claim can remain ``migrating`` forever.
            rollback_operation, rollback_cancellation = (
                await _settle_despite_cancellation(
                    self._rollback_failed_migration(
                        task_id=task_id,
                        claimed=claimed,
                        restored_status=prev_status,
                        src=src,
                        dst=dst,
                        src_unsubscribed=src_unsubscribed,
                        dst_subscribed=dst_subscribed,
                        claim_active=claim_active,
                        destination_import=destination_import,
                    )
                )
            )
            try:
                rollback_operation.result()
            except BaseException as rollback_error:
                if isinstance(migration_error, asyncio.CancelledError):
                    raise migration_error from rollback_error
                if rollback_cancellation is not None:
                    raise rollback_cancellation from rollback_error
                raise rollback_error from migration_error
            if isinstance(migration_error, asyncio.CancelledError):
                raise migration_error
            if rollback_cancellation is not None:
                raise rollback_cancellation from migration_error
            raise migration_error

    # ------------------------------------------------------------------
    # 子操作
    # ------------------------------------------------------------------

    async def _get_worker(self, worker_id: int) -> Worker | None:
        async with self.db_factory() as db:
            return await db.get(Worker, worker_id)

    async def _active_versioned_plan_id(self, task_id: int) -> int | None:
        async with self.db_factory() as db:
            return await db.scalar(
                select(Plan.id)
                .where(
                    Plan.target_task_id == task_id,
                    Plan.archived_at.is_(None),
                    Plan.active_run_id.isnot(None),
                )
                .limit(1)
            )

    async def _lock_migration_operation(
        self,
        db,
        claimed: MigrationTaskGeneration,
        *,
        phases: tuple[str, ...] = MANAGER_ACTIVE_TASK_MIGRATION_PHASES,
    ) -> TaskMigrationOperation:
        operation = (
            await db.execute(
                select(TaskMigrationOperation)
                .where(
                    *_migration_operation_predicates(
                        claimed,
                        phases=phases,
                    )
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if operation is None:
            raise MigrationError(
                "Task migration durable operation changed or disappeared"
            )
        return operation

    async def _read_claimed_task(
        self,
        claimed: MigrationTaskGeneration,
        *,
        expected_values: dict | None = None,
    ) -> Task:
        async with self.db_factory() as db:
            task = (
                await db.execute(
                    select(Task).where(
                        *migration_generation_predicates(claimed),
                        *_task_value_predicates(expected_values or {}),
                        no_active_worker_task_termination_predicate(),
                    )
                )
            ).scalar_one_or_none()
            if task is None or not _task_json_values_match(
                task,
                expected_values or {},
            ):
                raise MigrationError(
                    "task 迁移 generation 已被并发修改，拒绝继续使用旧状态"
                )
            await self._lock_migration_operation(db, claimed)
            return task

    async def _claim_migration(
        self,
        observed: MigrationTaskGeneration,
        *,
        target_worker_id: int | None,
        operation_id: str,
        expected_values: dict | None = None,
    ) -> MigrationTaskGeneration:
        if (
            not isinstance(observed.incarnation_id, str)
            or re.fullmatch(r"[0-9a-f]{32}", observed.incarnation_id) is None
        ):
            raise MigrationError(
                "Task migration requires an immutable incarnation identity"
            )
        if re.fullmatch(r"[0-9a-f]{32}", operation_id) is None:
            raise MigrationError("Task migration operation identity is invalid")
        async with self.db_factory() as db:
            result = await db.execute(
                update(Task)
                .where(
                    *migration_generation_predicates(observed),
                    *_task_value_predicates(expected_values or {}),
                    Task.pty_background_generation.is_(None),
                    Task.worker_turn_handoff_id.is_(None),
                    task_retry_not_superseded_predicate(),
                    _no_active_capability_resume_outbox_predicate(),
                    no_active_worker_task_termination_predicate(),
                    no_active_test_harness_owner_graph_predicate(),
                )
                .values(status="migrating")
            )
            if result.rowcount != 1:
                await db.rollback()
                if await has_active_test_harness_owner_graph(
                    db,
                    observed.task_id,
                ):
                    await db.rollback()
                    raise MigrationError(
                        "Task owns an active Test Harness, Workspace Review, "
                        "or Browser Agent graph; wait for it to finish before "
                        "migrating the Task"
                    )
                current = await db.get(Task, observed.task_id)
                if current is None:
                    raise MigrationError("task 不存在")
                if current.status == "waiting_capability":
                    raise MigrationError(
                        "Task is waiting for its requested capability; durable "
                        "resume must finish before migration"
                    )
                active_resume_outbox_id = await db.scalar(
                    select(CapabilityResumeOutbox.id)
                    .where(
                        CapabilityResumeOutbox.task_id == observed.task_id,
                        CapabilityResumeOutbox.status.in_(
                            ACTIVE_RESUME_OUTBOX_STATUSES
                        ),
                    )
                    .order_by(CapabilityResumeOutbox.id)
                    .limit(1)
                )
                if active_resume_outbox_id is not None:
                    raise MigrationError(
                        "Task has an active capability resume outbox; durable "
                        "resume must finish before migration"
                    )
                raise MigrationError(
                    "task 在迁移认领前已被并发修改"
                    f"（status={current.status}, worker_id={current.worker_id}）"
                )
            db.expire_all()
            claimed_task = await db.get(
                Task,
                observed.task_id,
                with_for_update=True,
                populate_existing=True,
            )
            if claimed_task is None or not _task_json_values_match(
                claimed_task,
                expected_values or {},
            ):
                await db.rollback()
                raise MigrationError(
                    "task JSON 配置在迁移认领前已被并发修改"
                )
            try:
                # The Task UPDATE above is the cross-process writer fence.
                # Recheck child mirrors while it remains locked so a relay or
                # API admission that committed after the optimistic preflight
                # cannot be carried past the migration claim.
                await _assert_no_active_ccm_child_mirrors(db, claimed_task)
            except MigrationError:
                await db.rollback()
                raise
            previous_sequence = await db.scalar(
                select(func.max(TaskMigrationOperation.operation_sequence))
                .where(TaskMigrationOperation.task_id == observed.task_id)
            )
            operation_sequence = int(previous_sequence or 0) + 1
            if operation_sequence > 9_223_372_036_854_775_807:
                await db.rollback()
                raise MigrationError("Task migration operation sequence exhausted")
            claimed = replace(
                observed,
                status="migrating",
                migration_operation_id=operation_id,
                migration_operation_sequence=operation_sequence,
                migration_target_worker_id=target_worker_id,
                migration_source_status=observed.status,
            )
            db.add(TaskMigrationOperation(
                operation_id=operation_id,
                operation_sequence=operation_sequence,
                side="manager",
                active_task_id=observed.task_id,
                task_id=observed.task_id,
                task_incarnation_id=observed.incarnation_id,
                retry_count=observed.retry_count,
                turn_generation=observed.turn_generation,
                source_worker_id=observed.worker_id,
                target_worker_id=target_worker_id,
                source_status=observed.status,
                phase="claimed",
                instance_id=observed.instance_id,
                started_at=observed.started_at,
                completed_at=observed.completed_at,
            ))
            try:
                await db.commit()
            except IntegrityError as exc:
                await db.rollback()
                raise MigrationError(
                    "Task already owns an unfinished durable migration"
                ) from exc
        return claimed

    async def _restore_migration_claim(
        self,
        claimed: MigrationTaskGeneration,
        restored_status: str,
    ) -> bool:
        """Durably choose rollback while retaining the Task admission gate."""

        if restored_status != claimed.migration_source_status:
            raise MigrationError("Task migration restore status changed")
        async with self.db_factory() as db:
            result = await db.execute(
                update(Task)
                .where(
                    *migration_generation_predicates(claimed),
                    no_active_worker_task_termination_predicate(),
                )
                .values(status=Task.status)
                .execution_options(synchronize_session=False)
            )
            if result.rowcount != 1:
                await db.rollback()
                return False
            try:
                operation = await self._lock_migration_operation(
                    db,
                    claimed,
                    phases=("claimed", "remote_prepared"),
                )
            except MigrationError:
                await db.rollback()
                return False
            operation.phase = "rollback_pending"
            operation.updated_at = datetime.utcnow()
            await db.commit()
            return True

    async def _mark_migration_remote_prepared(
        self,
        claimed: MigrationTaskGeneration,
        *,
        expected_values: dict | None = None,
    ) -> None:
        """Persist destination prepare before any Manager pointer cut."""

        async with self.db_factory() as db:
            fenced = await db.execute(
                update(Task)
                .where(
                    *migration_generation_predicates(claimed),
                    *_task_value_predicates(expected_values or {}),
                    no_active_worker_task_termination_predicate(),
                )
                .values(status=Task.status)
                .execution_options(synchronize_session=False)
            )
            if fenced.rowcount != 1:
                await db.rollback()
                raise MigrationError(
                    "Task changed before destination prepare was recorded"
                )
            db.expire_all()
            task = await db.get(Task, claimed.task_id, populate_existing=True)
            if task is None or not _task_json_values_match(
                task,
                expected_values or {},
            ):
                await db.rollback()
                raise MigrationError(
                    "Task JSON changed before destination prepare was recorded"
                )
            operation = await self._lock_migration_operation(
                db,
                claimed,
                phases=("claimed",),
            )
            operation.phase = "remote_prepared"
            operation.updated_at = datetime.utcnow()
            await db.commit()

    async def _settle_rolled_back_migration(
        self,
        claimed: MigrationTaskGeneration,
        *,
        restored_status: str,
    ) -> None:
        """Release source execution only after remote deletion is acknowledged."""

        async with self.db_factory() as db:
            fenced = await db.execute(
                update(Task)
                .where(
                    *migration_generation_predicates(claimed),
                    no_active_worker_task_termination_predicate(),
                )
                .values(status=Task.status)
                .execution_options(synchronize_session=False)
            )
            if fenced.rowcount != 1:
                await db.rollback()
                raise MigrationError(
                    "Task changed before migration rollback settlement"
                )
            operation = await self._lock_migration_operation(
                db,
                claimed,
                phases=("rollback_pending",),
            )
            completed = await db.execute(
                update(Task)
                .where(
                    *migration_generation_predicates(claimed),
                    no_active_worker_task_termination_predicate(),
                )
                .values(status=restored_status)
                .execution_options(synchronize_session=False)
            )
            if completed.rowcount != 1:
                await db.rollback()
                raise MigrationError(
                    "Task changed during migration rollback settlement"
                )
            operation.phase = "rolled_back"
            operation.active_task_id = None
            operation.updated_at = datetime.utcnow()
            await db.commit()

    async def _settle_committed_migration(
        self,
        claimed: MigrationTaskGeneration,
        *,
        restored_status: str,
    ) -> None:
        """Release destination execution after its commit receipt is durable."""

        target_worker_id = claimed.migration_target_worker_id
        if target_worker_id is None:
            raise MigrationError("Remote migration has no destination Worker")
        destination_generation = replace(
            claimed,
            worker_id=target_worker_id,
        )
        async with self.db_factory() as db:
            fenced = await db.execute(
                update(Task)
                .where(
                    *migration_generation_predicates(destination_generation),
                    no_active_worker_task_termination_predicate(),
                )
                .values(status=Task.status)
                .execution_options(synchronize_session=False)
            )
            if fenced.rowcount != 1:
                await db.rollback()
                raise MigrationError(
                    "Task changed before destination commit settlement"
                )
            operation = await self._lock_migration_operation(
                db,
                claimed,
                phases=("committed_pending_ack",),
            )
            completed = await db.execute(
                update(Task)
                .where(
                    *migration_generation_predicates(destination_generation),
                    no_active_worker_task_termination_predicate(),
                )
                .values(status=restored_status)
                .execution_options(synchronize_session=False)
            )
            if completed.rowcount != 1:
                await db.rollback()
                raise MigrationError(
                    "Task changed during destination commit settlement"
                )
            operation.phase = "committed"
            operation.active_task_id = None
            operation.updated_at = datetime.utcnow()
            await db.commit()

    async def _commit_destination_migration(
        self,
        dst: Worker,
        *,
        claimed: MigrationTaskGeneration,
        restored_status: str,
        reservation: WorkerMigrationImportReservation,
    ) -> None:
        """Settle the Worker receipt before reopening Manager admission."""

        await self._commit_worker_task_import(
            dst,
            task_id=claimed.task_id,
            reservation=reservation,
        )
        await self._settle_committed_migration(
            claimed,
            restored_status=restored_status,
        )

    async def _rollback_failed_migration(
        self,
        *,
        task_id: int,
        claimed: MigrationTaskGeneration,
        restored_status: str,
        src: Worker | None,
        dst: Worker | None,
        src_unsubscribed: bool,
        dst_subscribed: bool,
        claim_active: bool,
        destination_import: WorkerMigrationImportReservation | None,
    ) -> None:
        """Restore an owned claim and relay route as one settled barrier."""

        if not claim_active:
            # The destination pointer was already committed.  A late
            # cancellation (for example during the final broadcast) must not
            # route the relay back to the old Worker.
            return

        errors: list[BaseException] = []
        restored = False
        try:
            restored = await self._restore_migration_claim(
                claimed,
                restored_status,
            )
            if not restored:
                logger.warning(
                    "task %s migration rollback skipped: claim no longer owned",
                    task_id,
                )
        except BaseException as exc:
            errors.append(exc)
            logger.exception("task %s migration claim rollback failed", task_id)

        # The source claim must be durably restored before touching the remote
        # mirror.  A failed CAS can mean the destination pointer already won;
        # deleting in that state would erase the authoritative Task.  The
        # destination cleanup itself is nonce + generation fenced and remains
        # legal after that node has entered its irreversible drain.
        if restored and dst is not None and destination_import is not None:
            try:
                await self._rollback_worker_task_import(
                    dst,
                    task_id=task_id,
                    reservation=destination_import,
                )
            except BaseException as exc:
                errors.append(exc)
                logger.exception(
                    "task %s destination import rollback failed",
                    task_id,
                )

        if restored and not errors:
            try:
                await self._settle_rolled_back_migration(
                    claimed,
                    restored_status=restored_status,
                )
                await self._broadcast_status(
                    task_id,
                    "migrating",
                    restored_status,
                )
            except BaseException as exc:
                errors.append(exc)
                logger.exception(
                    "task %s migration rollback settlement failed",
                    task_id,
                )

        # Keep relay routing aligned with the unchanged source pointer when a
        # failure happens after subscription switching.  A COMMIT can succeed
        # even when its acknowledgement is lost; in that case the restore CAS
        # returns false while the destination pointer is already authoritative.
        # Re-read that durable pointer instead of blindly routing back to src.
        try:
            route_to_source = restored
            route_to_destination = False
            if not restored:
                async with self.db_factory() as db:
                    pointer = (
                        await db.execute(
                            select(Task.id, Task.worker_id).where(
                                Task.id == task_id
                            )
                        )
                    ).one_or_none()
                if pointer is None:
                    raise MigrationError(
                        "Task disappeared while reconciling migration relay"
                    )
                current_worker_id = pointer.worker_id
                source_worker_id = src.id if src is not None else None
                destination_worker_id = dst.id if dst is not None else None
                route_to_source = current_worker_id == source_worker_id
                route_to_destination = (
                    current_worker_id == destination_worker_id
                    and destination_worker_id != source_worker_id
                )
                if not route_to_source and not route_to_destination:
                    raise MigrationError(
                        "Task migration rollback cannot reconcile the current "
                        f"Worker pointer ({current_worker_id})"
                    )

            if route_to_source:
                if dst_subscribed and dst is not None:
                    self.relay.unsubscribe_task(dst.id, task_id)
                if src_unsubscribed and src is not None:
                    await self.relay.subscribe_task(src, task_id)
            elif route_to_destination:
                if src is not None and not src_unsubscribed:
                    self.relay.unsubscribe_task(src.id, task_id)
                if not dst_subscribed and dst is not None:
                    await self.relay.subscribe_task(dst, task_id)
        except BaseException as exc:
            errors.append(exc)
            logger.exception("task %s relay rollback failed", task_id)

        if errors:
            raise errors[0]

    async def _finish_migration(
        self,
        *,
        claimed: MigrationTaskGeneration,
        target_worker_id: int | None,
        restored_status: str,
        provider: str,
        local_codex_target_home: str | None,
        task_updates: dict | None = None,
        expected_values: dict | None = None,
    ) -> None:
        async with self.db_factory() as db:
            task_fence = await db.execute(
                update(Task)
                .where(
                    *migration_generation_predicates(claimed),
                    *_task_value_predicates(expected_values or {}),
                    no_active_worker_task_termination_predicate(),
                )
                .values(status=Task.status)
                .execution_options(synchronize_session=False)
            )
            if task_fence.rowcount != 1:
                await db.rollback()
                raise MigrationError(
                    "task 迁移状态或 generation 已被并发修改，拒绝覆盖"
                )

            db.expire_all()
            operation = await self._lock_migration_operation(
                db,
                claimed,
                phases=(
                    ("remote_prepared",)
                    if target_worker_id is not None
                    else ("claimed",)
                ),
            )
            task = (
                await db.execute(
                    select(Task)
                    .where(
                        *migration_generation_predicates(claimed),
                        *_task_value_predicates(expected_values or {}),
                        no_active_worker_task_termination_predicate(),
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if task is None or not _task_json_values_match(
                task,
                expected_values or {},
            ):
                await db.rollback()
                raise MigrationError(
                    "task 迁移状态或 generation 已被并发修改，拒绝覆盖"
                )

            if target_worker_id is not None:
                # Global lifecycle lock order is Task -> Worker.  Destination
                # validation above precedes workspace/session copy and is only
                # advisory; a concurrent destroy may claim the Worker while
                # this Task still points at its source and is therefore absent
                # from destroy's Task snapshot.  Take a portable no-op write
                # barrier here so either this final pointer cut wins while the
                # Worker is still ready, or destroy wins and migration restores
                # its exact source claim.  Retaining ``updated_at`` suppresses
                # its Python onupdate hook and keeps opaque lifecycle claims
                # stable for a genuinely unchanged ready Worker.
                target_ready = await db.execute(
                    update(Worker)
                    .where(
                        Worker.id == target_worker_id,
                        Worker.status == "ready",
                        Worker.bootstrap_step.is_(None),
                    )
                    .values(
                        status=Worker.status,
                        updated_at=Worker.updated_at,
                    )
                )
                if target_ready.rowcount != 1:
                    await db.rollback()
                    raise MigrationError(
                        f"目标 Worker {target_worker_id} 在迁移完成前不再 ready"
                    )

            values: dict = copy.deepcopy(task_updates or {})
            values.update({
                "worker_id": target_worker_id,
                # A remote pointer is not executable until the Worker has
                # durably converted its rollback reservation into a commit
                # receipt.  Keep Manager admission closed in the interim.
                "status": (
                    "migrating"
                    if target_worker_id is not None
                    else restored_status
                ),
            })
            # ``_ensure_worker_task`` has already committed an inert mirror on
            # the destination Worker.  Record that fact on the Manager in the
            # same pointer-cut transaction; otherwise the next retry mistakes
            # this for an unmaterialized initial forward and attempts to create
            # the same global Task id a second time.
            metadata = dict(values.get("metadata_", task.metadata_) or {})
            if target_worker_id is not None:
                from backend.services.task_creation import (
                    system_task_execution_principal_values,
                )

                metadata[WORKER_REMOTE_MATERIALIZED_METADATA_KEY] = True
                # Migration imports are deliberately inert and use the system
                # principal on the Worker.  Keep the Manager mirror on that
                # exact native source principal until the next authenticated
                # chat/retry envelope performs the audited transition to its
                # delegated caller.  A stale pre-migration user principal here
                # would make the first remote retry irreconcilable.
                values.update(system_task_execution_principal_values())
            else:
                metadata.pop(WORKER_REMOTE_MATERIALIZED_METADATA_KEY, None)
            values["metadata_"] = metadata or None
            if provider == "codex" and target_worker_id is None and local_codex_target_home:
                values["metadata_"] = self._local_codex_account_metadata(
                    values.get("metadata_", task.metadata_),
                    local_codex_target_home,
                )

            # last_cwd 防护：失败启动会把 os.getcwd() 写进 last_cwd（污染），
            # 且它优先于 target_repo——切回本机时不存在/不在项目内的一律清掉，
            # 让 cwd 解析回落到 target_repo。
            if target_worker_id is None and task.last_cwd:
                effective_target_repo = values.get(
                    "target_repo",
                    task.target_repo,
                )
                valid = os.path.isdir(task.last_cwd) and (
                    not effective_target_repo
                    or task.last_cwd.startswith(effective_target_repo)
                )
                if not valid:
                    values["last_cwd"] = None

            result = await db.execute(
                update(Task)
                .where(
                    *migration_generation_predicates(claimed),
                    *_task_value_predicates(expected_values or {}),
                    no_active_worker_task_termination_predicate(),
                )
                .values(**values)
            )
            if result.rowcount != 1:
                await db.rollback()
                raise MigrationError("task 迁移状态已被并发修改，拒绝覆盖")
            if target_worker_id is not None:
                operation.phase = "committed_pending_ack"
                operation.updated_at = datetime.utcnow()
            else:
                operation.phase = "committed"
                operation.active_task_id = None
                operation.updated_at = datetime.utcnow()
            await db.commit()

    def _ssh(self, worker: Worker) -> SSHExecutor:
        return SSHExecutor(
            host=worker.private_ip,
            user=worker.ssh_user,
            key_path=worker.ssh_key_path or settings.worker_ssh_key_path,
            known_hosts_path=(
                worker_known_hosts_path(worker.cloud_instance_id)
                if worker.cloud_instance_id else None
            ),
        )

    async def _broadcast_status(self, task_id: int, old: str, new: str):
        if self.broadcaster:
            try:
                await self.broadcaster.broadcast("tasks", {
                    "event": "status_change", "task_id": task_id,
                    "old_status": old, "new_status": new,
                })
            except Exception:
                logger.exception("task %s status broadcast failed", task_id)

    async def _sync_task_fields_from_worker(
        self,
        worker: Worker,
        claimed: MigrationTaskGeneration,
        *,
        expected_remote_status: str,
        protected_fields: frozenset[str] = frozenset(),
        expected_values: dict | None = None,
    ):
        """worker 广播会 pop session_id、last_cwd 只写 worker DB——迁移前必须拉全。"""
        task_id = claimed.task_id
        incarnation_id = claimed.incarnation_id
        if (
            not isinstance(incarnation_id, str)
            or re.fullmatch(r"[0-9a-f]{32}", incarnation_id) is None
        ):
            raise MigrationError(
                "源 Worker task 缺少稳定 incarnation，拒绝迁移旧状态"
            )
        from backend.main import worker_proxy

        if worker_proxy is None:
            raise MigrationError("Worker 代理未初始化")
        try:
            await worker_proxy.require_worker_task_incarnation_support(worker)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise MigrationError(str(exc)) from exc
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(
                f"http://{worker.private_ip}:{worker.ccm_port}/api/tasks/{task_id}",
                headers={
                    "Authorization": f"Bearer {worker.auth_token}",
                    "X-CCM-Task-Incarnation": incarnation_id,
                },
            )
            if r.status_code != 200:
                raise MigrationError(f"从 worker 拉取 task 详情失败: HTTP {r.status_code}")
            wt = r.json()
        if (
            not isinstance(wt, dict)
            or wt.get("id") != task_id
            or wt.get("incarnation_id") != incarnation_id
            # Destination imports are deliberately inert ("cancelled") while
            # the Manager mirror restores the pre-migration status.  A later
            # move back must accept that intentional mismatch, but no other
            # unexpected source status may be borrowed.
            or wt.get("status") not in {
                expected_remote_status,
                "cancelled",
            }
            or wt.get("retry_count") != claimed.retry_count
            or type(wt.get("turn_generation")) is not int
            or wt["turn_generation"] != claimed.turn_generation
        ):
            raise MigrationError(
                "源 Worker task generation 已变化，拒绝迁移旧状态"
            )
        async with self.db_factory() as db:
            task_fence = await db.execute(
                update(Task)
                .where(
                    *migration_generation_predicates(claimed),
                    *_task_value_predicates(expected_values or {}),
                    no_active_worker_task_termination_predicate(),
                )
                .values(status=Task.status)
                .execution_options(synchronize_session=False)
            )
            if task_fence.rowcount != 1:
                await db.rollback()
                raise MigrationError(
                    "task 在 Worker 状态同步期间已被并发修改"
                )
            db.expire_all()
            task = (
                await db.execute(
                    select(Task)
                    .where(
                        *migration_generation_predicates(claimed),
                        *_task_value_predicates(expected_values or {}),
                        no_active_worker_task_termination_predicate(),
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if task is None or not _task_json_values_match(
                task,
                expected_values or {},
            ):
                await db.rollback()
                raise MigrationError(
                    "task 在 Worker 状态同步期间已被并发修改"
                )
            await self._lock_migration_operation(
                db,
                claimed,
                phases=("claimed",),
            )
            remote_metadata = wt.get("metadata_") or {}
            if (
                isinstance(remote_metadata, dict)
                and remote_metadata.get(
                    PR_REVIEW_SUPERSEDED_METADATA_KEY
                )
                is True
            ):
                # A lost hidden-termination response can leave the source
                # Worker gated while the Manager mirror is still stale. Mirror
                # that durable proof before aborting migration; otherwise the
                # destination TaskCreate payload would drop the gate and make
                # the obsolete review retryable again.
                metadata = dict(task.metadata_ or {})
                metadata[PR_REVIEW_SUPERSEDED_METADATA_KEY] = True
                changed = await db.execute(
                    update(Task)
                    .where(
                        *migration_generation_predicates(claimed),
                        *_task_value_predicates(expected_values or {}),
                        no_active_worker_task_termination_predicate(),
                    )
                    .values(metadata_=metadata)
                )
                if changed.rowcount != 1:
                    await db.rollback()
                    raise MigrationError(
                        "task 在 Worker 状态同步期间已被并发修改"
                    )
                await db.commit()
                raise MigrationError(
                    "源 Worker task 已被新 push 取代，拒绝迁移"
                )
            values = {
                field: wt[field]
                for field in (
                    "session_id",
                    "last_cwd",
                    "target_repo",
                    "error_message",
                )
                if field in wt and field not in protected_fields
            }
            # Even an empty response must prove that the claimed generation is
            # still current after the network await.
            if not values:
                values["status"] = claimed.status
            changed = await db.execute(
                update(Task)
                .where(
                    *migration_generation_predicates(claimed),
                    *_task_value_predicates(expected_values or {}),
                    no_active_worker_task_termination_predicate(),
                )
                .values(**values)
            )
            if changed.rowcount != 1:
                await db.rollback()
                raise MigrationError(
                    "task 在 Worker 状态同步期间已被并发修改"
                )
            await db.commit()

    async def _sync_workspace(self, src: Worker | None, dst: Worker | None, local_path: str):
        """项目目录在机器间搬运。worker→worker 经 Manager 两跳。"""
        path = os.path.expanduser(local_path).rstrip("/")
        if src is None and dst is not None:
            if not os.path.isdir(path):
                return  # 本机没有工作目录可推
            await self._ssh(dst).rsync_to(path + "/", path + "/", excludes=[], timeout=1200)
        elif src is not None and dst is None:
            await self._ssh(src).rsync_from(path + "/", path + "/", timeout=1200)
        elif src is not None and dst is not None:
            tmp = tempfile.mkdtemp(prefix="ccm-migrate-")
            try:
                hop = os.path.join(tmp, "ws")
                await self._ssh(src).rsync_from(path + "/", hop + "/", timeout=1200)
                await self._ssh(dst).rsync_to(hop + "/", path + "/", excludes=[], timeout=1200)
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

    async def sync_workspace_once(
        self,
        src: Worker | None,
        dst: Worker | None,
        local_path: str,
    ) -> None:
        """Copy one workspace, deduplicating only inside an explicit batch."""

        normalized = os.path.abspath(os.path.expanduser(local_path)).rstrip("/")
        cache = _SHARED_WORKSPACE_SYNC_CACHE.get()
        if cache is not None and normalized in cache:
            return
        await self._sync_workspace(src, dst, normalized)
        if cache is not None:
            cache.add(normalized)

    # -- session 搬运 ---------------------------------------------------

    @staticmethod
    def _local_session_glob(session_id: str) -> list[str]:
        home = os.path.expanduser("~")
        pats = [
            f"{home}/.claude/projects/*/{session_id}.jsonl",
            f"{home}/.claude-*/projects/*/{session_id}.jsonl",
        ]
        out: list[str] = []
        for p in pats:
            out.extend(glob.glob(p))
        return out

    async def _move_session(self, src: Worker | None, dst: Worker | None, session_id: str):
        """Move a complete Claude session JSONL plus its sibling sidecar tree."""

        temporary_dir: str | None = None
        try:
            if src is None:
                matches = self._local_session_glob(session_id)
                if not matches:
                    logger.warning(
                        "session %s 本机未找到，跳过 session 搬运",
                        session_id,
                    )
                    return
                src_file = matches[0]
                encoded = os.path.basename(os.path.dirname(src_file))
                src_sidecar = os.path.join(
                    os.path.dirname(src_file),
                    session_id,
                )
            else:
                ssh = self._ssh(src)
                _code, out = await ssh.run(
                    f"ls ~/.claude/projects/*/{session_id}.jsonl "
                    f"~/.claude-*/projects/*/{session_id}.jsonl "
                    "2>/dev/null | head -1"
                )
                remote_file = (
                    out.strip().splitlines()[0].strip()
                    if out.strip()
                    else ""
                )
                if not remote_file:
                    logger.warning(
                        "session %s 在 worker %s 未找到，跳过",
                        session_id,
                        src.id,
                    )
                    return
                encoded = os.path.basename(os.path.dirname(remote_file))
                remote_sidecar = remote_file.removesuffix(".jsonl")
                temporary_dir = tempfile.mkdtemp(prefix="ccm-sess-")
                src_file = os.path.join(
                    temporary_dir,
                    f"{session_id}.jsonl",
                )
                src_sidecar = os.path.join(temporary_dir, session_id)
                await ssh.rsync_from(
                    remote_file,
                    src_file,
                    delete=False,
                )
                sidecar_code, _sidecar_out = await ssh.run(
                    f"test -d {shlex.quote(remote_sidecar)}"
                )
                if sidecar_code == 0:
                    await ssh.rsync_from(
                        remote_sidecar + "/",
                        src_sidecar + "/",
                        delete=False,
                    )

            sidecar_exists = os.path.isdir(src_sidecar)
            if dst is None:
                config_dir = (
                    os.environ.get("CLAUDE_CONFIG_DIR")
                    or os.path.expanduser("~/.claude")
                )
                target = os.path.join(
                    config_dir,
                    f"projects/{encoded}/{session_id}.jsonl",
                )
                target_sidecar = os.path.join(
                    os.path.dirname(target),
                    session_id,
                )
                await asyncio.to_thread(
                    os.makedirs,
                    os.path.dirname(target),
                    exist_ok=True,
                )
                if os.path.abspath(src_file) != os.path.abspath(target):
                    await asyncio.to_thread(shutil.copy2, src_file, target)
                if (
                    sidecar_exists
                    and os.path.abspath(src_sidecar)
                    != os.path.abspath(target_sidecar)
                ):
                    await asyncio.to_thread(
                        shutil.copytree,
                        src_sidecar,
                        target_sidecar,
                        dirs_exist_ok=True,
                    )
            else:
                target = (
                    f"/home/{dst.ssh_user}/.claude/projects/"
                    f"{encoded}/{session_id}.jsonl"
                )
                target_sidecar = target.removesuffix(".jsonl")
                destination_ssh = self._ssh(dst)
                await destination_ssh.copy_file(src_file, target)
                if sidecar_exists:
                    await destination_ssh.rsync_to(
                        src_sidecar + "/",
                        target_sidecar + "/",
                        excludes=[],
                        timeout=1200,
                    )
        finally:
            if temporary_dir is not None:
                await asyncio.to_thread(
                    shutil.rmtree,
                    temporary_dir,
                    ignore_errors=True,
                )

    # -- codex session 搬运 ---------------------------------------------
    # codex 的 session 是 rollout 文件：~/.codex/sessions/YYYY/MM/DD/
    # rollout-<timestamp>-<session_id>.jsonl（本机 CLI 0.144.6 实证）。
    # `codex exec resume <id>` 按 id 扫描 sessions 树，故目标机保持源机的
    # 相对路径（含日期目录）落盘即可。

    @staticmethod
    def _local_codex_session_glob(session_id: str) -> list[str]:
        if not _CODEX_SESSION_ID_RE.fullmatch(session_id):
            raise MigrationError("无效 Codex session id")
        home = os.path.expanduser("~")
        escaped_session_id = glob.escape(session_id)
        return sorted(
            glob.glob(
                f"{home}/.codex*/sessions/*/*/*/rollout-*-{escaped_session_id}.jsonl"
            )
        )

    @staticmethod
    def _codex_sessions_root_and_relative(rollout_file: str) -> tuple[str, str]:
        """Return the matched account's sessions root and safe date/file path."""
        rollout = PurePosixPath(rollout_file)
        try:
            sessions_root = rollout.parents[3]
            relative = rollout.relative_to(sessions_root)
        except (IndexError, ValueError) as exc:
            raise MigrationError(f"无效 Codex rollout 路径: {rollout_file}") from exc

        if (
            sessions_root.name != "sessions"
            or len(relative.parts) != 4
            or any(part in ("", ".", "..") for part in relative.parts)
            or not relative.name.startswith("rollout-")
            or not relative.name.endswith(".jsonl")
        ):
            raise MigrationError(f"无效 Codex rollout 路径: {rollout_file}")
        return str(sessions_root), relative.as_posix()

    @staticmethod
    def _file_is_prefix(prefix_file: str, full_file: str) -> bool:
        """Whether one rollout is a byte-prefix of another rollout."""

        if os.path.getsize(prefix_file) > os.path.getsize(full_file):
            return False
        with open(prefix_file, "rb") as prefix_stream, open(full_file, "rb") as full_stream:
            while True:
                chunk = prefix_stream.read(_COPY_BUFFER_SIZE)
                if not chunk:
                    return True
                if full_stream.read(len(chunk)) != chunk:
                    return False

    @classmethod
    def _select_authoritative_codex_rollout(cls, candidates: list[str]) -> str:
        """Choose the longest rollout only when every other copy is its prefix.

        Account rotation intentionally keeps recovery copies.  Picking the
        lexicographically first home loses later turns, while picking by mtime
        can choose a touched stale file.  Prefix validation proves that the
        selected file contains all known history; divergent histories fail
        closed and require manual reconciliation.
        """

        if not candidates:
            raise MigrationError("未找到 Codex rollout")
        ordered = sorted(candidates, key=lambda path: (-os.path.getsize(path), path))
        selected = ordered[0]
        for candidate in ordered[1:]:
            if not cls._file_is_prefix(candidate, selected):
                raise MigrationError(
                    "Codex session 存在分叉 rollout，拒绝猜测并迁移可能过期的上下文"
                )
        return selected

    @staticmethod
    def _local_codex_account_metadata(
        current_metadata: dict | None,
        target_home: str,
    ) -> dict:
        """Return metadata aligned with the local account receiving rollout.

        Worker and manager account IDs are machine-local.  Keeping the worker's
        old ID (or an earlier local ID) after copying into ``~/.codex`` makes
        the resolver trust a stale recovery copy.  Persist the actual local
        account when it is registered; otherwise clear the foreign binding so
        it is never treated as authoritative.
        """

        account_id: str | None = None
        try:
            from backend.main import codex_pool

            if codex_pool is not None:
                resolved = codex_pool.account_id_for_home(target_home)
                if isinstance(resolved, str) and resolved:
                    account_id = resolved
        except Exception:
            logger.exception(
                "Failed to map migrated CODEX_HOME %s to a local account",
                target_home,
            )

        metadata = dict(current_metadata or {})
        if account_id:
            metadata["codex_account_id"] = account_id
        else:
            metadata.pop("codex_account_id", None)
        return metadata

    @classmethod
    def _sync_local_codex_account_binding(cls, task: Task, target_home: str) -> None:
        """Compatibility wrapper for callers mutating an ORM task directly."""
        task.metadata_ = cls._local_codex_account_metadata(
            task.metadata_, target_home
        )

    async def _move_codex_session(
        self,
        src: Worker | None,
        dst: Worker | None,
        session_id: str,
    ) -> str | None:
        codex_home = os.path.expanduser("~/.codex")
        codex_root = os.path.join(codex_home, "sessions")
        if not _CODEX_SESSION_ID_RE.fullmatch(session_id):
            raise MigrationError("无效 Codex session id")
        temporary_dir: str | None = None
        target_home: str | None = None
        try:
            if src is None:
                matches = self._local_codex_session_glob(session_id)
                if not matches:
                    logger.warning("codex session %s 本机未找到，跳过 session 搬运", session_id)
                    return
                src_file = self._select_authoritative_codex_rollout(matches)
                _, rel = self._codex_sessions_root_and_relative(src_file)
            else:
                ssh = self._ssh(src)
                quoted_name = shlex.quote(f"rollout-*-{session_id}.jsonl")
                _code, out = await ssh.run(
                    "find ~/.codex*/sessions -mindepth 4 -maxdepth 4 -type f "
                    f"-name {quoted_name} -print 2>/dev/null"
                )
                remote_files = [line.strip() for line in out.splitlines() if line.strip()]
                if not remote_files:
                    logger.warning("codex session %s 在 worker %s 未找到，跳过", session_id, src.id)
                    return
                temporary_dir = tempfile.mkdtemp(prefix="ccm-codex-sess-")
                local_to_remote: dict[str, str] = {}
                for index, remote_file in enumerate(remote_files):
                    # Prefix the basename because migrated account copies
                    # usually have the exact same rollout filename.
                    local_file = os.path.join(
                        temporary_dir,
                        f"{index:04d}-{os.path.basename(remote_file)}",
                    )
                    await ssh.rsync_from(remote_file, local_file, delete=False)
                    local_to_remote[local_file] = remote_file
                src_file = self._select_authoritative_codex_rollout(
                    list(local_to_remote)
                )
                _, rel = self._codex_sessions_root_and_relative(
                    local_to_remote[src_file]
                )

            if dst is None:
                target = os.path.join(codex_root, rel)
                os.makedirs(os.path.dirname(target), exist_ok=True)
                if os.path.abspath(src_file) != os.path.abspath(target):
                    shutil.copy2(src_file, target)
                target_home = codex_home
            else:
                target = f"/home/{dst.ssh_user}/.codex/sessions/{rel}"
                await self._ssh(dst).copy_file(src_file, target)
                target_home = f"/home/{dst.ssh_user}/.codex"
            return target_home
        finally:
            if temporary_dir is not None:
                shutil.rmtree(temporary_dir, ignore_errors=True)

    # -- 目标 worker 上重建 task ----------------------------------------

    async def _rollback_worker_task_import(
        self,
        dst: Worker,
        *,
        task_id: int,
        reservation: WorkerMigrationImportReservation,
    ) -> None:
        """Remove only this failed attempt's exact inert destination mirror."""

        headers = {"Authorization": f"Bearer {dst.auth_token}"}
        url = (
            f"http://{dst.private_ip}:{dst.ccm_port}"
            "/api/tasks/migration-import/rollback"
        )
        payload = {
            "task_id": task_id,
            **reservation.as_dict(),
        }
        # The wire schema calls this field ``operation_id`` already.  Keep the
        # reservation serializer as the single canonical identity shape.
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                url,
                headers=headers,
                json=payload,
            )
            if response.status_code == 409:
                try:
                    detail = response.json().get("detail", response.text)
                except ValueError:
                    detail = response.text
                raise MigrationError(
                    f"目标 Worker 拒绝回滚 imported task: {detail}"
                )
            response.raise_for_status()
            result = response.json()
            if (
                not isinstance(result, dict)
                or result.get("ok") is not True
                or result.get("task_id") != task_id
                or result.get("operation_id") != reservation.operation_id
                or result.get("operation_sequence")
                != reservation.operation_sequence
            ):
                raise MigrationError(
                    "目标 Worker 未确认 exact migration import rollback"
                )

    async def _commit_worker_task_import(
        self,
        dst: Worker,
        *,
        task_id: int,
        reservation: WorkerMigrationImportReservation,
    ) -> None:
        """Make one exact destination mirror permanently authoritative."""

        headers = {"Authorization": f"Bearer {dst.auth_token}"}
        url = (
            f"http://{dst.private_ip}:{dst.ccm_port}"
            "/api/tasks/migration-import/commit"
        )
        payload = {
            "task_id": task_id,
            **reservation.as_dict(),
        }
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                url,
                headers=headers,
                json=payload,
            )
            if response.status_code == 409:
                try:
                    detail = response.json().get("detail", response.text)
                except ValueError:
                    detail = response.text
                raise MigrationError(
                    f"目标 Worker 拒绝提交 imported task: {detail}"
                )
            response.raise_for_status()
            result = response.json()
            if (
                not isinstance(result, dict)
                or result.get("ok") is not True
                or result.get("committed") is not True
                or result.get("task_id") != task_id
                or result.get("operation_id") != reservation.operation_id
                or result.get("operation_sequence")
                != reservation.operation_sequence
            ):
                raise MigrationError(
                    "目标 Worker 未确认 exact migration import commit"
                )

    async def _ensure_worker_task(
        self,
        dst: Worker,
        task: Task,
        worker_project_id: int,
        *,
        source_status: str | None = None,
        import_reservation: WorkerMigrationImportReservation | None = None,
    ):
        """Atomically import an inert same-ID task on the destination Worker."""
        headers = {"Authorization": f"Bearer {dst.auth_token}"}
        base = f"http://{dst.private_ip}:{dst.ccm_port}/api/tasks"
        from backend.services.skill_context import (
            build_user_skill_snapshot_payload,
            normalize_user_skill_ids,
        )
        from backend.services.worker_routing_config import (
            WORKER_ROUTING_SAFE_STATUSES,
        )
        from backend.services.task_creation import (
            system_task_execution_principal_values,
        )

        user_skill_snapshots = []
        if normalize_user_skill_ids(task.selected_user_skills):
            async with self.db_factory() as db:
                user_skill_snapshots = (
                    await build_user_skill_snapshot_payload(
                        db,
                        task.selected_user_skills,
                        metadata=task.metadata_,
                    )
                )
        requested_source_status = source_status or task.status
        source_status = (
            requested_source_status
            if requested_source_status in WORKER_ROUTING_SAFE_STATUSES
            else "cancelled"
        )
        source_incarnation_id = task.incarnation_id
        if (
            not isinstance(source_incarnation_id, str)
            or re.fullmatch(r"[0-9a-f]{32}", source_incarnation_id) is None
        ):
            raise MigrationError(
                "源 Task 缺少可验证的 immutable incarnation identity"
            )
        if import_reservation is None:
            raise MigrationError(
                "Destination import requires a durable Manager operation"
            )
        if (
            re.fullmatch(r"[0-9a-f]{32}", import_reservation.operation_id)
            is None
            or type(import_reservation.operation_sequence) is not int
            or import_reservation.operation_sequence <= 0
        ):
            raise MigrationError(
                "Destination import reservation identity is invalid"
            )
        if import_reservation != WorkerMigrationImportReservation(
            operation_id=import_reservation.operation_id,
            operation_sequence=import_reservation.operation_sequence,
            incarnation_id=source_incarnation_id,
            retry_count=task.retry_count,
            turn_generation=task.turn_generation,
            source_status=source_status,
        ):
            raise MigrationError(
                "Destination import reservation does not match the exact Task generation"
            )
        payload = {
            "id": task.id,
            "source_incarnation_id": source_incarnation_id,
            "migration_operation_id": import_reservation.operation_id,
            "migration_operation_sequence": (
                import_reservation.operation_sequence
            ),
            "worker_id": None,
            "source_status": source_status,
            "title": task.title,
            "description": task.description or task.title or "migrated task",
            "project_id": worker_project_id,
            "target_repo": task.target_repo,
            "target_branch": task.target_branch or "main",
            "priority": task.priority,
            "retry_count": task.retry_count,
            "turn_generation": task.turn_generation,
            "max_retries": task.max_retries,
            "mode": task.mode,
            "todo_file_path": task.todo_file_path,
            "max_iterations": task.max_iterations,
            "must_complete": task.must_complete,
            "goal_condition": task.goal_condition,
            "goal_max_turns": task.goal_max_turns,
            "goal_evaluator_model": task.goal_evaluator_model,
            "provider": task.provider,
            "model": task.model,
            "codex_service_tier": task.codex_service_tier,
            "effort_level": task.effort_level,
            "thinking_budget": task.thinking_budget,
            "system_prompt_mode": task.system_prompt_mode,
            "timeout_hours": task.timeout_hours,
            "sort_order": task.sort_order,
            "enable_workflows": task.enable_workflows,
            "enabled_skills": task.enabled_skills,
            "selected_user_skills": task.selected_user_skills,
            "user_skill_snapshots": user_skill_snapshots,
            "tags": task.tags,
            "attention_tag": task.attention_tag,
            "starred": task.starred,
            "session_id": task.session_id,
            "last_cwd": task.last_cwd,
            **system_task_execution_principal_values(),
        }
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                f"{base}/migration-import",
                headers=headers,
                json=payload,
            )
            if r.status_code == 409:
                try:
                    detail = r.json().get("detail", r.text)
                except ValueError:
                    detail = r.text
                raise MigrationError(f"目标 Worker 导入 task 冲突: {detail}")
            r.raise_for_status()
            created = r.json()
            if not isinstance(created, dict):
                raise MigrationError(
                    "目标 Worker 导入 task 未返回有效对象"
                )
            if created.get("id") != task.id:
                raise MigrationError(
                    "目标 Worker 未确认导入 task 的 exact global identity"
                )
            if created.get("incarnation_id") != source_incarnation_id:
                raise MigrationError(
                    "目标 Worker 未确认导入 task 的 exact incarnation identity"
                )
            expected_principal = system_task_execution_principal_values()
            if {
                field: created.get(field)
                for field in expected_principal
            } != expected_principal:
                raise MigrationError(
                    "目标 Worker 未确认导入 task 的 fail-closed principal"
                )
            if created.get("status") != source_status:
                raise MigrationError("目标 Worker 导入 task 未保持不可调度状态")
            if (
                type(created.get("retry_count")) is not int
                or created["retry_count"] != task.retry_count
            ):
                raise MigrationError(
                    "目标 Worker 未确认导入 task 的 exact retry generation"
                )
            if (
                type(created.get("turn_generation")) is not int
                or created["turn_generation"] != task.turn_generation
            ):
                raise MigrationError(
                    "目标 Worker 未确认导入 task 的 exact turn generation"
                )
            if (
                (task.codex_service_tier or "default") == "priority"
                and created.get("codex_service_tier") != "priority"
            ):
                raise MigrationError("目标 Worker 未确认 Codex Fast 任务配置")
            try:
                confirmed_reservation = (
                    read_worker_migration_import_reservation(
                        created.get("metadata_")
                    )
                )
            except InvalidWorkerRoutingMarker as exc:
                raise MigrationError(
                    "目标 Worker 返回了无效的 migration import reservation"
                ) from exc
            if confirmed_reservation != import_reservation:
                raise MigrationError(
                    "目标 Worker 未确认 exact migration import reservation"
                )
