"""Recoverable controller for the autonomous Delivery Loop.

The controller is deliberately a small durable orchestrator.  Agent work is
delegated to Capability Core and the normal Dispatcher; Git/GitHub work is
delegated to injectable gateways.  A ``DeliveryRun`` lease serializes one
reconciliation decision across CCM processes, while ``DeliveryAction`` is the
idempotent outbox for the only remote effect owned here (publishing a PR).

No Git, GitHub, Capability executor, or wake callback is invoked while this
module holds a database row lock.  Every external result is accepted only
after re-locking and re-validating the run lease and exact subject.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
import inspect
import logging
from pathlib import Path
import re
import secrets
from typing import Any, Protocol

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.capability import CapabilityInvocation
from backend.models.code_review import CodeReviewResult
from backend.models.delivery import (
    DELIVERY_CYCLE_ACTIVE_STATUSES,
    DELIVERY_TURN_ACTIVE_STATUSES,
    DeliveryAction,
    DeliveryCycle,
    DeliveryRun,
    DeliveryTurn,
)
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.plan import PlanVersion
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRMonitorRun,
    PRRepairWake,
    PRReview,
)
from backend.models.project import Project
from backend.models.task import Task
from backend.models.test_harness import (
    TestHarnessAttempt,
    TestHarnessEvidence,
    TestHarnessFinding,
    TestHarnessRun,
)
from backend.models.worktree import Worktree
from backend.services.capability_service import (
    CapabilityDisabledError,
    CapabilityConflictError,
    CapabilityUnavailableError,
    CapabilityUnsupportedScopeError,
    CapabilityValidationError,
    consume_ready_invocation,
    create_controller_invocation,
)
from backend.services.cancellation import await_task_completion
from backend.services.delivery_reducer import DeliveryReducerEvent
from backend.services.delivery_service import (
    DeliveryConflictError,
    apply_run_event,
    canonical_json,
    complete_cycle,
    lock_current_cycle,
    lock_run,
    start_next_cycle,
    value_hash,
)
from backend.services.delivery_workspace import (
    DeliveryWorkspaceError,
    DeliveryWorkspaceManager,
    DeliveryWorkspaceSnapshot,
)
from backend.services.test_harness_owner_fence import (
    TestHarnessOwnerIdentity,
    no_active_test_harness_owner_graph_predicate,
    test_harness_owner_identity,
)


logger = logging.getLogger(__name__)

_TASK_ACTIVE_STATUSES = frozenset({"in_progress", "executing"})
_TASK_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "cancelled", "stopped", "conflict"}
)
_TASK_REUSABLE_STATUSES = _TASK_TERMINAL_STATUSES | {"delivery_waiting"}
_REPORT_COMPLETE_MARKER = "DELIVERY_RESULT: REPORT_COMPLETE"
_CAPABILITY_PENDING_STATUSES = frozenset(
    {"queued", "running", "waiting_user", "cancelling"}
)
_CAPABILITY_TERMINAL_ERRORS = frozenset({"failed", "cancelled", "stale"})
_MONITOR_WAITING_STATUSES = frozenset(
    {
        "observing",
        "waiting_ci",
        "reviewing",
        "adjudicating",
        "resolving_fixed_threads",
    }
)
_MONITOR_BLOCKED_STATUSES = frozenset({"repair_pending", "waiting_for_fix"})
_LEGACY_WAKE_ACTIVE = frozenset({"delivering", "accepted", "awaiting_push"})
_HEX_64_RE = re.compile(r"[0-9a-f]{64}\Z")
_LEGACY_BOUND_STALE_HEAD_REASON = (
    "Delivery pull-request history is ambiguous: "
    "Pull request does not match the exact Delivery subject"
)


class DeliveryControllerError(RuntimeError):
    """Stable controller error base class."""


class DeliveryPublisherPermanentError(DeliveryControllerError):
    """Retrying blindly is unsafe, but a remote effect may already exist."""


class DeliveryPublisherNoEffectPreflightError(DeliveryPublisherPermanentError):
    """A deterministic preflight rejection proved no remote effect began."""


class DeliveryPublisherUnavailable(DeliveryPublisherNoEffectPreflightError):
    """Publishing is not configured, so no remote effect can begin."""


class _DeliveryTerminalReceiptError(DeliveryPublisherPermanentError):
    """A validated durable receipt proves publication cannot continue."""


class DeliverySubjectChanged(DeliveryControllerError):
    """An exact Task, workspace, or PR subject changed unexpectedly."""


@dataclass(frozen=True, slots=True)
class CapabilitySnapshot:
    invocation_id: int
    status: str
    state_version: int
    result_kind: str | None = None
    result_id: int | None = None
    result_hash: str | None = None
    verdict: str | None = None
    summary: str | None = None
    findings: tuple[dict, ...] = ()
    subject_ref: dict | None = None
    error_code: str | None = None
    error_message: str | None = None


class DeliveryCapabilityGateway(Protocol):
    async def create(
        self,
        *,
        task_id: int,
        capability_key: str,
        request_payload: dict,
        idempotency_key: str,
    ) -> CapabilitySnapshot: ...

    async def observe(self, invocation_id: int) -> CapabilitySnapshot: ...

    async def consume(self, snapshot: CapabilitySnapshot) -> CapabilitySnapshot: ...


class CoreDeliveryCapabilityGateway:
    """Narrow adapter over Capability Core's controller-only entry point."""

    def __init__(self, db_factory: Callable[[], Any]) -> None:
        self._db_factory = db_factory

    async def create(
        self,
        *,
        task_id: int,
        capability_key: str,
        request_payload: dict,
        idempotency_key: str,
    ) -> CapabilitySnapshot:
        async with self._db_factory() as db:
            invocation, _created = await create_controller_invocation(
                db,
                task_id=task_id,
                capability_key=capability_key,
                request_payload=request_payload,
                idempotency_key=idempotency_key,
                purpose="required_gate",
            )
            invocation_id = invocation.id
        return await self.observe(invocation_id)

    async def observe(self, invocation_id: int) -> CapabilitySnapshot:
        async with self._db_factory() as db:
            invocation = await db.get(
                CapabilityInvocation,
                invocation_id,
                populate_existing=True,
            )
            if invocation is None:
                raise DeliverySubjectChanged(
                    f"Capability invocation {invocation_id} disappeared"
                )
            verdict = None
            summary = None
            findings: tuple[dict, ...] = ()
            subject_ref = None
            if (
                invocation.capability_key == "code_review"
                and invocation.result_id is not None
            ):
                result = await db.get(
                    CodeReviewResult,
                    invocation.result_id,
                    populate_existing=True,
                )
                if result is not None:
                    verdict = result.verdict
                    summary = result.summary
                    findings = tuple(result.findings or ())
                    subject_ref = dict(result.subject_ref or {})
            return CapabilitySnapshot(
                invocation_id=invocation.id,
                status=invocation.status,
                state_version=invocation.state_version,
                result_kind=invocation.result_kind,
                result_id=invocation.result_id,
                result_hash=invocation.result_hash,
                verdict=verdict,
                summary=summary,
                findings=findings,
                subject_ref=subject_ref,
                error_code=invocation.error_code,
                error_message=invocation.error_message,
            )

    async def consume(self, snapshot: CapabilitySnapshot) -> CapabilitySnapshot:
        if snapshot.status == "completed":
            return snapshot
        if snapshot.status != "ready":
            raise DeliverySubjectChanged(
                f"Capability {snapshot.invocation_id} is not ready to consume"
            )
        try:
            async with self._db_factory() as db:
                await consume_ready_invocation(
                    db,
                    invocation_id=snapshot.invocation_id,
                    expected_state_version=snapshot.state_version,
                    allow_workflow_owned=True,
                )
        except CapabilityConflictError:
            current = await self.observe(snapshot.invocation_id)
            if current.status != "completed":
                raise
            return current
        return await self.observe(snapshot.invocation_id)


@dataclass(frozen=True, slots=True)
class PublishedPullRequest:
    """Exact PR snapshot returned by an idempotent publishing gateway."""

    repo_id: int
    pr_number: int
    url: str
    base_sha: str
    head_sha: str
    head_branch: str
    head_repo_full_name: str | None = None


@dataclass(frozen=True, slots=True)
class DeliveryEffectFence:
    """Exact DB lease/action generation authorizing one remote publication."""

    run_id: int
    controller_owner: str
    controller_generation: int
    action_id: int
    action_token: str
    expected_base_sha: str
    expected_head_sha: str


class DeliveryPublisher(Protocol):
    async def ensure_pull_request(
        self,
        *,
        run_id: int,
        idempotency_key: str,
        fence: DeliveryEffectFence,
    ) -> PublishedPullRequest: ...

    async def ensure_monitor(
        self,
        *,
        run_id: int,
        pull_request: PublishedPullRequest,
        idempotency_key: str,
        fence: DeliveryEffectFence,
    ) -> int: ...

    async def verify_ready_to_merge(
        self,
        *,
        run_id: int,
        pull_request: PublishedPullRequest,
        monitor_run_id: int,
        expected_monitor_state_version: int,
    ) -> PublishedPullRequest: ...

    async def verify_merged(
        self,
        *,
        run_id: int,
        pull_request: PublishedPullRequest,
        monitor_run_id: int,
        expected_monitor_state_version: int,
    ) -> PublishedPullRequest: ...


class UnavailableDeliveryPublisher:
    async def ensure_pull_request(
        self,
        *,
        run_id: int,
        idempotency_key: str,
        fence: DeliveryEffectFence,
    ) -> PublishedPullRequest:
        del run_id, idempotency_key, fence
        raise DeliveryPublisherUnavailable(
            "Delivery publishing gateway is not configured"
        )

    async def ensure_monitor(
        self,
        *,
        run_id: int,
        pull_request: PublishedPullRequest,
        idempotency_key: str,
        fence: DeliveryEffectFence,
    ) -> int:
        del run_id, pull_request, idempotency_key, fence
        raise DeliveryPublisherUnavailable(
            "Delivery PR Monitor gateway is not configured"
        )

    async def verify_ready_to_merge(
        self,
        *,
        run_id: int,
        pull_request: PublishedPullRequest,
        monitor_run_id: int,
        expected_monitor_state_version: int,
    ) -> PublishedPullRequest:
        del (
            run_id,
            pull_request,
            monitor_run_id,
            expected_monitor_state_version,
        )
        raise DeliveryPublisherUnavailable(
            "Delivery ready-to-merge verifier is not configured"
        )

    async def verify_merged(
        self,
        *,
        run_id: int,
        pull_request: PublishedPullRequest,
        monitor_run_id: int,
        expected_monitor_state_version: int,
    ) -> PublishedPullRequest:
        del (
            run_id,
            pull_request,
            monitor_run_id,
            expected_monitor_state_version,
        )
        raise DeliveryPublisherUnavailable("Delivery merged verifier is not configured")


@dataclass(frozen=True, slots=True)
class _Lease:
    run_id: int
    generation: int


@dataclass(frozen=True, slots=True)
class _RunContext:
    run_id: int
    project_id: int
    developer_task_id: int
    monitored_repo_id: int
    cycle_id: int
    cycle_number: int
    phase: str
    activity: str
    workspace_path: str | None
    repo_path: str
    repo_full_name: str
    delivery_branch: str
    base_branch: str
    base_sha: str | None
    head_sha: str | None
    head_tree_sha: str | None
    title: str
    requirements: str
    auto_merge: bool
    terminal: str
    frontend_review_mode: str
    frontend_review_profile: str
    frontend_review_allow_actions: bool


def _utcnow() -> datetime:
    return datetime.utcnow()


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


async def _maybe_call(callback: Callable[[], Any] | None) -> None:
    if callback is None:
        return
    result = callback()
    if inspect.isawaitable(result):
        await result


async def _await_task_settled(task: asyncio.Task[Any]) -> Any:
    """Wait for one exact child despite repeated caller cancellation.

    ``asyncio.shield`` protects the child from one cancellation, but awaiting
    it can still be interrupted by a later ``Task.cancel()``.  Controller
    cleanup must prove its drive/Git subprocess is terminal before the durable
    Run lease is released for takeover.
    """

    cancellation = await await_task_completion(task)
    result = task.result()
    if cancellation is not None:
        raise cancellation
    return result


class DeliveryController:
    """Poll, lease, and advance durable Delivery Runs one decision at a time."""

    def __init__(
        self,
        *,
        db_factory: Callable[[], Any],
        capability_gateway: DeliveryCapabilityGateway | None = None,
        capability_coordinator: Any | None = None,
        dispatcher: Any | None = None,
        dispatcher_wake: Callable[[], Any] | None = None,
        workspace_manager: DeliveryWorkspaceManager | None = None,
        publisher: DeliveryPublisher | None = None,
        test_harness_service: Any | None = None,
        enabled: bool | None = None,
        poll_interval_seconds: float | None = None,
        reconcile_interval_seconds: float | None = None,
        lease_seconds: float | None = None,
        action_lease_seconds: float | None = None,
        max_concurrency: int = 4,
        scan_limit: int | None = None,
        owner_id: str | None = None,
    ) -> None:
        poll_interval_seconds = (
            settings.delivery_controller_poll_interval_seconds
            if poll_interval_seconds is None
            else poll_interval_seconds
        )
        reconcile_interval_seconds = (
            settings.delivery_controller_reconcile_interval_seconds
            if reconcile_interval_seconds is None
            else reconcile_interval_seconds
        )
        lease_seconds = (
            float(settings.delivery_controller_lease_seconds)
            if lease_seconds is None
            else lease_seconds
        )
        action_lease_seconds = (
            max(float(lease_seconds) * 4.0, 120.0)
            if action_lease_seconds is None
            else action_lease_seconds
        )
        scan_limit = (
            settings.delivery_controller_scan_limit
            if scan_limit is None
            else scan_limit
        )
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if reconcile_interval_seconds <= 0:
            raise ValueError("reconcile_interval_seconds must be positive")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if action_lease_seconds <= 0:
            raise ValueError("action_lease_seconds must be positive")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least one")
        if scan_limit < max_concurrency:
            raise ValueError("scan_limit must be at least max_concurrency")

        self.db_factory = db_factory
        self.capabilities = capability_gateway or CoreDeliveryCapabilityGateway(
            db_factory
        )
        self.capability_coordinator = capability_coordinator
        self.dispatcher = dispatcher
        self.instance_manager = (
            getattr(dispatcher, "instance_manager", None)
            if dispatcher is not None
            else None
        )
        self.dispatcher_wake = dispatcher_wake or (
            getattr(dispatcher, "wake", None) if dispatcher is not None else None
        )
        self.workspace = workspace_manager or DeliveryWorkspaceManager()
        self.publisher = publisher or UnavailableDeliveryPublisher()
        self.test_harness_service = test_harness_service
        # The rollout flag gates new Run admission at the API boundary.  The
        # controller itself stays on so a restart/config rollback cannot
        # strand work that was already admitted durably.  ``enabled=False`` is
        # retained only as an explicit embedding/test override.
        self.enabled = True if enabled is None else enabled
        self.poll_interval_seconds = float(poll_interval_seconds)
        self.reconcile_interval_seconds = float(reconcile_interval_seconds)
        self.lease_seconds = float(lease_seconds)
        self.action_lease_seconds = float(action_lease_seconds)
        self.max_concurrency = max_concurrency
        self.scan_limit = scan_limit
        self.owner_id = owner_id or secrets.token_hex(24)

        self._wake_event = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._lifecycle_lock = asyncio.Lock()
        self._run_locks: dict[int, asyncio.Lock] = {}
        self._runner: asyncio.Task[None] | None = None
        self._active_action_fences: dict[int, tuple[int, str]] = {}

    async def _developer_task_settled_locked(
        self,
        db: AsyncSession,
        task: Task,
    ) -> bool:
        """Prove a terminal Developer generation released every local owner.

        ``Task.instance_id`` is historical execution metadata and deliberately
        survives Dispatcher completion.  The caller must already hold the
        Task row lock; this method then follows the global Task -> Instance
        lock order and accepts that historical id only after no Instance
        reverse-owns the Task and no exact in-process lifecycle remains.
        """

        if task.pty_background_generation is not None:
            return False

        historical_id = task.instance_id
        predicates = [Instance.current_task_id == task.id]
        if historical_id is not None:
            predicates.append(Instance.id == historical_id)
        instances = list(
            (
                await db.execute(
                    select(Instance)
                    .where(or_(*predicates))
                    .order_by(Instance.id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalars()
        )
        if any(instance.current_task_id == task.id for instance in instances):
            return False

        dispatcher = self.dispatcher
        if dispatcher is not None:
            running_tasks = getattr(dispatcher, "_running_tasks", {})
            if isinstance(running_tasks, dict):
                for lifecycle in tuple(running_tasks.values()):
                    if (
                        getattr(lifecycle, "_ccm_task_id", None) == task.id
                        and not lifecycle.done()
                    ):
                        return False

        if historical_id is None:
            return True

        historical = next(
            (instance for instance in instances if instance.id == historical_id),
            None,
        )
        # A reused slot's process belongs to its new reverse owner, not to this
        # historical Task generation.
        if historical is not None and (
            historical.current_task_id is not None
            or historical.current_plan_run_id is not None
        ):
            return True

        manager = self.instance_manager
        if manager is not None:
            try:
                running = manager.is_running(historical_id)
            except Exception:
                return False
            if not isinstance(running, bool) or running:
                return False
            process = getattr(manager, "processes", {}).get(historical_id)
            if process is not None and getattr(process, "returncode", None) is None:
                return False
            return True

        # Tests and deliberately narrow embeddings may omit InstanceManager;
        # in that case only an intact, fully released durable slot is proof.
        return bool(
            historical is not None
            and historical.current_task_id is None
            and historical.pid is None
            and historical.status != "running"
        )

    @staticmethod
    async def _fence_developer_task_graph_locked(
        db: AsyncSession,
        task: Task,
    ) -> bool:
        """Win the Task writer race only when its Harness graph is idle.

        Delivery reuses one developer Task across cycles.  A terminal Task is
        also a valid owner for a user-started Test Harness run, so changing its
        status without this CAS would strand the Run/Workspace/Browser graph
        on an owner identity that can no longer be cancelled or reconciled.

        Harness admission writes the exact owner Task before inserting its
        graph.  This no-op UPDATE is therefore the portable first-writer gate:
        PostgreSQL/MySQL take the row lock and SQLite WAL takes the writer
        reservation.  The correlated graph predicate then gives either the
        Harness admission or this Delivery transition one deterministic
        winner without performing cross-session cleanup while Delivery locks
        are held.
        """

        predicates = [
            Task.id == task.id,
            (
                Task.incarnation_id.is_(None)
                if task.incarnation_id is None
                else Task.incarnation_id == task.incarnation_id
            ),
            Task.status == task.status,
            Task.retry_count == task.retry_count,
            Task.turn_generation == task.turn_generation,
            (
                Task.instance_id.is_(None)
                if task.instance_id is None
                else Task.instance_id == task.instance_id
            ),
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
            (
                Task.pty_background_generation.is_(None)
                if task.pty_background_generation is None
                else Task.pty_background_generation == task.pty_background_generation
            ),
            Task.delivery_run_id == task.delivery_run_id,
            Task.delivery_role == task.delivery_role,
            Task.mode == task.mode,
            Task.worker_id.is_(None),
            Task.shared_from_id.is_(None),
            no_active_test_harness_owner_graph_predicate(),
        ]
        fenced = await db.execute(
            update(Task)
            .where(*predicates)
            .values(status=task.status)
            .execution_options(synchronize_session=False)
        )
        return fenced.rowcount == 1

    async def _terminal_task_generation_settled(
        self,
        *,
        task_id: int,
        snapshot: dict[str, Any],
    ) -> bool:
        """Re-lock an exact terminal generation before inspecting its Git."""

        async with self.db_factory() as db:
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == task_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if (
                task is None
                or task.delivery_run_id != snapshot["delivery_run_id"]
                or task.status != snapshot["status"]
                or task.retry_count != snapshot["retry_count"]
                or task.instance_id != snapshot["instance_id"]
                or task.started_at != snapshot["started_at"]
                or task.completed_at != snapshot["completed_at"]
                or task.session_id != snapshot["session_id"]
                or task.pty_background_generation
                != snapshot["pty_background_generation"]
            ):
                raise DeliverySubjectChanged(
                    "Developer terminal generation changed during settlement"
                )
            settled = await self._developer_task_settled_locked(db, task)
            await db.rollback()
            return settled

    @property
    def is_running(self) -> bool:
        return self._runner is not None and not self._runner.done()

    def wake(self) -> None:
        self._wake_event.set()

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self.is_running:
                return
            if not self.enabled:
                logger.info("Delivery Controller remains dark (feature disabled)")
                return
            self._stop_event.clear()
            self._wake_event.clear()
            # A synchronous recovery scan settles durable leases/actions before
            # startup reports success.
            await self.run_once(scan_limit=None, recovery=True)
            if self._stop_event.is_set():
                return
            self._runner = asyncio.create_task(
                self._run_loop(),
                name=f"delivery-controller-{self.owner_id[:8]}",
            )

    async def stop(self) -> None:
        async with self._lifecycle_lock:
            self._stop_event.set()
            self._wake_event.set()
            runner = self._runner
        if runner is not None:
            try:
                await _await_task_settled(runner)
            finally:
                async with self._lifecycle_lock:
                    if self._runner is runner:
                        self._runner = None

    shutdown = stop

    async def _run_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(
                        self._wake_event.wait(),
                        timeout=self.poll_interval_seconds,
                    )
                except asyncio.TimeoutError:
                    pass
                self._wake_event.clear()
                if self._stop_event.is_set():
                    break
                try:
                    await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Delivery Controller scan failed")
        finally:
            self._stop_event.set()

    async def run_once(
        self,
        *,
        scan_limit: int | None | object = ...,
        recovery: bool = False,
    ) -> int:
        if scan_limit is ...:
            scan_limit = self.scan_limit
        now = _utcnow()
        statement = select(DeliveryRun.id).where(
            DeliveryRun.activity.not_in(("paused", "terminal")),
        )
        if not recovery:
            statement = statement.where(
                or_(
                    DeliveryRun.next_reconcile_at.is_(None),
                    DeliveryRun.next_reconcile_at <= now,
                )
            )
        statement = statement.order_by(
            DeliveryRun.next_reconcile_at,
            DeliveryRun.id,
        )
        if scan_limit is not None:
            statement = statement.limit(scan_limit)
        async with self.db_factory() as db:
            run_ids = list((await db.scalars(statement)).all())
        semaphore = asyncio.Semaphore(self.max_concurrency)

        async def reconcile(run_id: int) -> bool:
            async with semaphore:
                return await self.reconcile_run(run_id)

        results = await asyncio.gather(
            *(reconcile(run_id) for run_id in run_ids),
            return_exceptions=True,
        )
        processed = 0
        for run_id, result in zip(run_ids, results, strict=True):
            if isinstance(result, BaseException):
                logger.error(
                    "Delivery Run %s reconciliation crashed",
                    run_id,
                    exc_info=(type(result), result, result.__traceback__),
                )
            elif result:
                processed += 1
        return processed

    async def reconcile_run(self, run_id: int) -> bool:
        lock = self._run_locks.setdefault(run_id, asyncio.Lock())
        async with lock:
            lease = await self._claim_run(run_id)
            if lease is None:
                return False
            delay = self.poll_interval_seconds
            immediate = False
            try:
                immediate = await self._drive_with_heartbeat(lease)
                delay = 0.0 if immediate else self.reconcile_interval_seconds
            except asyncio.CancelledError:
                raise
            except DeliverySubjectChanged as exc:
                await self._pause_run(
                    lease,
                    reason=str(exc),
                    code="delivery_subject_changed",
                )
            except CapabilityConflictError as exc:
                await self._pause_run(
                    lease,
                    reason=str(exc),
                    code="delivery_capability_conflict",
                )
            except (
                CapabilityDisabledError,
                CapabilityUnavailableError,
                CapabilityUnsupportedScopeError,
                CapabilityValidationError,
            ) as exc:
                await self._fail_run(
                    lease,
                    code="delivery_capability_unavailable",
                    message=str(exc),
                )
            except (
                DeliveryPublisherUnavailable,
                DeliveryPublisherPermanentError,
            ) as exc:
                await self._fail_run(
                    lease,
                    code="delivery_publisher_unavailable",
                    message=str(exc),
                )
            except Exception:
                # Unknown transport/DB faults are not proof that an idempotent
                # external effect did not occur.  Preserve durable state and
                # retry instead of guessing a terminal outcome.
                logger.exception("Delivery Run %s reconcile failed", run_id)
            finally:
                release = asyncio.create_task(
                    self._release_run(lease, delay=delay),
                    name=f"delivery-release-{lease.run_id}-{lease.generation}",
                )
                await _await_task_settled(release)
                try:
                    from backend.services.delivery_events import (
                        broadcast_delivery_event,
                    )

                    await broadcast_delivery_event(
                        "delivery_progress_changed",
                        run_id=run_id,
                    )
                except Exception:
                    logger.exception(
                        "Delivery Run %s progress broadcast failed",
                        run_id,
                    )
            if immediate:
                self.wake()
            return True

    async def _renew_run_lease(self, lease: _Lease) -> bool:
        """Extend the exact Run lease and any in-flight publication action."""

        now = _utcnow()
        async with self.db_factory() as db:
            renewed = await db.execute(
                update(DeliveryRun)
                .where(
                    DeliveryRun.id == lease.run_id,
                    DeliveryRun.lease_owner == self.owner_id,
                    DeliveryRun.controller_generation == lease.generation,
                    DeliveryRun.lease_expires_at.is_not(None),
                    DeliveryRun.lease_expires_at > now,
                )
                .values(
                    lease_expires_at=now + timedelta(seconds=self.lease_seconds),
                    updated_at=now,
                )
            )
            if renewed.rowcount != 1:
                await db.rollback()
                return False
            action_fence = self._active_action_fences.get(lease.run_id)
            if action_fence is not None:
                action_id, action_token = action_fence
                action_renewed = await db.execute(
                    update(DeliveryAction)
                    .where(
                        DeliveryAction.id == action_id,
                        DeliveryAction.run_id == lease.run_id,
                        DeliveryAction.active_run_id == lease.run_id,
                        DeliveryAction.status == "leased",
                        DeliveryAction.lease_owner == action_token,
                        DeliveryAction.lease_expires_at.is_not(None),
                        DeliveryAction.lease_expires_at > now,
                    )
                    .values(
                        lease_expires_at=now
                        + timedelta(seconds=self.action_lease_seconds)
                    )
                )
                if action_renewed.rowcount != 1:
                    await db.rollback()
                    return False
            await db.commit()
            return True

    async def _drive_with_heartbeat(self, lease: _Lease) -> bool:
        """Cancel local work immediately if its durable lease cannot renew."""

        interval = max(
            0.001,
            min(self.lease_seconds, self.action_lease_seconds) / 3.0,
        )
        drive = asyncio.create_task(
            self._drive(lease),
            name=f"delivery-drive-{lease.run_id}-{lease.generation}",
        )

        async def heartbeat() -> bool:
            while True:
                await asyncio.sleep(interval)
                if drive.done():
                    return True
                try:
                    if not await self._renew_run_lease(lease):
                        return False
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Delivery Run %s lease heartbeat failed",
                        lease.run_id,
                    )
                    return False

        beat = asyncio.create_task(
            heartbeat(),
            name=f"delivery-heartbeat-{lease.run_id}-{lease.generation}",
        )
        try:
            done, _pending = await asyncio.wait(
                {drive, beat},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if drive in done:
                return await drive
            lease_retained = await beat
            if lease_retained and drive.done():
                return await drive
            drive.cancel()
            try:
                await drive
            except asyncio.CancelledError:
                pass
            raise DeliveryConflictError(
                "Delivery Controller lost its exact lease during an external effect"
            )
        finally:
            if not drive.done():
                drive.cancel()
            if not beat.done():
                beat.cancel()

            async def settle_children() -> None:
                await asyncio.gather(drive, beat, return_exceptions=True)

            settlement = asyncio.create_task(
                settle_children(),
                name=f"delivery-settle-{lease.run_id}-{lease.generation}",
            )
            await _await_task_settled(settlement)

    async def _claim_run(self, run_id: int) -> _Lease | None:
        now = _utcnow()
        expires = now + timedelta(seconds=self.lease_seconds)
        async with self.db_factory() as db:
            claimed = await db.execute(
                update(DeliveryRun)
                .where(
                    DeliveryRun.id == run_id,
                    DeliveryRun.activity.not_in(("paused", "terminal")),
                    or_(
                        DeliveryRun.lease_owner.is_(None),
                        DeliveryRun.lease_expires_at.is_(None),
                        DeliveryRun.lease_expires_at <= now,
                        DeliveryRun.lease_owner == self.owner_id,
                    ),
                )
                .values(
                    lease_owner=self.owner_id,
                    lease_expires_at=expires,
                    controller_generation=DeliveryRun.controller_generation + 1,
                    updated_at=now,
                )
            )
            if claimed.rowcount != 1:
                await db.rollback()
                return None
            await db.commit()
            run = await db.get(DeliveryRun, run_id, populate_existing=True)
            if run is None or run.lease_owner != self.owner_id:
                return None
            return _Lease(run_id=run.id, generation=run.controller_generation)

    async def _release_run(self, lease: _Lease, *, delay: float) -> None:
        now = _utcnow()
        async with self.db_factory() as db:
            await db.execute(
                update(DeliveryRun)
                .where(
                    DeliveryRun.id == lease.run_id,
                    DeliveryRun.lease_owner == self.owner_id,
                    DeliveryRun.controller_generation == lease.generation,
                    DeliveryRun.lease_expires_at.is_not(None),
                    DeliveryRun.lease_expires_at > now,
                )
                .values(
                    lease_owner=None,
                    lease_expires_at=None,
                    next_reconcile_at=now + timedelta(seconds=max(delay, 0.0)),
                    updated_at=now,
                )
            )
            await db.commit()

    def _assert_lease(self, run: DeliveryRun, lease: _Lease) -> None:
        if (
            run.lease_owner != self.owner_id
            or run.controller_generation != lease.generation
        ):
            raise DeliveryConflictError("Delivery Run controller lease changed")
        if run.lease_expires_at is None or run.lease_expires_at <= _utcnow():
            raise DeliveryConflictError("Delivery Run controller lease expired")

    def _assert_action_lease(
        self,
        action: DeliveryAction | None,
        *,
        run: DeliveryRun,
        action_id: int,
        token: str,
    ) -> DeliveryAction:
        if (
            action is None
            or action.id != action_id
            or action.run_id != run.id
            or action.cycle_id != run.current_cycle_id
            or action.active_run_id != run.id
            or action.status != "leased"
            or action.lease_owner != token
        ):
            raise DeliveryConflictError("Delivery publication action lease changed")
        if action.lease_expires_at is None or action.lease_expires_at <= _utcnow():
            raise DeliveryConflictError("Delivery publication action lease expired")
        return action

    async def _context(self, lease: _Lease) -> _RunContext:
        async with self.db_factory() as db:
            run = await db.get(DeliveryRun, lease.run_id, populate_existing=True)
            if run is None:
                raise DeliverySubjectChanged("Delivery Run disappeared")
            self._assert_lease(run, lease)
            if (
                run.developer_task_id is None
                or run.monitored_repo_id is None
                or run.current_cycle_id is None
            ):
                raise DeliverySubjectChanged("Delivery Run ownership is incomplete")
            project = await db.get(Project, run.project_id, populate_existing=True)
            repo = await db.get(
                MonitoredRepo,
                run.monitored_repo_id,
                populate_existing=True,
            )
            cycle = await db.get(
                DeliveryCycle,
                run.current_cycle_id,
                populate_existing=True,
            )
            monitor_policy = (
                run.policy_snapshot.get("pr_monitor")
                if isinstance(run.policy_snapshot, dict)
                else None
            )
            policy = run.policy_snapshot
            auto_merge = policy.get("auto_merge") if isinstance(policy, dict) else None
            terminal = policy.get("terminal") if isinstance(policy, dict) else None
            frontend_review = policy.get("frontend_review")
            if not isinstance(frontend_review, dict):
                frontend_review = {
                    "mode": "off",
                    "profile": "standard",
                    "allow_actions": True,
                }
            frontend_review_mode = frontend_review.get("mode")
            frontend_review_profile = frontend_review.get("profile")
            frontend_review_allow_actions = frontend_review.get("allow_actions")
            frozen_repo_full_name = (
                monitor_policy.get("repo_full_name")
                if isinstance(monitor_policy, dict)
                else None
            )
            if (
                project is None
                or not project.local_path
                or repo is None
                or not isinstance(policy, dict)
                or value_hash(policy) != run.policy_hash
                or type(auto_merge) is not bool
                or terminal != ("merged" if auto_merge else "ready_to_merge")
                or frontend_review_mode not in {"auto", "required", "off"}
                or frontend_review_profile not in {"standard", "exhaustive"}
                or type(frontend_review_allow_actions) is not bool
                or repo.project_id != project.id
                or not isinstance(frozen_repo_full_name, str)
                or not frozen_repo_full_name
                or repo.repo_full_name.lower() != frozen_repo_full_name.lower()
                or cycle is None
            ):
                raise DeliverySubjectChanged("Delivery project or cycle disappeared")
            return _RunContext(
                run_id=run.id,
                project_id=run.project_id,
                developer_task_id=run.developer_task_id,
                monitored_repo_id=run.monitored_repo_id,
                cycle_id=cycle.id,
                cycle_number=cycle.cycle_number,
                phase=run.phase,
                activity=run.activity,
                workspace_path=run.workspace_path,
                repo_path=project.local_path,
                repo_full_name=frozen_repo_full_name,
                delivery_branch=run.delivery_branch,
                base_branch=run.base_branch,
                base_sha=run.base_sha,
                head_sha=run.head_sha,
                head_tree_sha=run.head_tree_sha,
                title=run.title,
                requirements=run.requirements,
                auto_merge=auto_merge,
                terminal=terminal,
                frontend_review_mode=frontend_review_mode,
                frontend_review_profile=frontend_review_profile,
                frontend_review_allow_actions=frontend_review_allow_actions,
            )

    async def _drive(self, lease: _Lease) -> bool:
        context = await self._context(lease)
        if context.phase == "planning":
            return await self._drive_planning(lease, context)
        if context.phase == "coding":
            return await self._drive_coding(lease, context)
        if context.phase == "pre_review":
            return await self._drive_review(lease, context)
        if context.phase == "frontend_review":
            return await self._drive_frontend_review(lease, context)
        if context.phase == "publishing":
            return await self._drive_publishing(lease, context)
        if context.phase == "monitoring":
            return await self._drive_monitoring(lease, context)
        return False

    async def _ensure_workspace(
        self,
        lease: _Lease,
        context: _RunContext,
    ) -> DeliveryWorkspaceSnapshot:
        # Git calls happen with no AsyncSession/row lock alive.
        try:
            if context.workspace_path:
                snapshot = await self.workspace.inspect(
                    repo_path=context.repo_path,
                    worktree_path=context.workspace_path,
                    branch=context.delivery_branch,
                    base_branch=context.base_branch,
                    expected_repo_full_name=context.repo_full_name,
                )
            else:
                snapshot = await self.workspace.prepare(
                    repo_path=context.repo_path,
                    run_id=context.run_id,
                    branch=context.delivery_branch,
                    base_branch=context.base_branch,
                    expected_repo_full_name=context.repo_full_name,
                )
        except DeliveryWorkspaceError as exc:
            raise DeliverySubjectChanged(
                f"Delivery workspace validation failed: {exc}"
            ) from exc
        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == run.developer_task_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if task is None or task.delivery_run_id != run.id:
                raise DeliverySubjectChanged("Delivery Developer Task changed owner")
            if run.base_sha is not None and run.base_sha != snapshot.base_sha:
                raise DeliverySubjectChanged(
                    "Delivery workspace base changed outside the controller"
                )
            if run.head_sha is not None and run.head_sha != snapshot.head_sha:
                raise DeliverySubjectChanged(
                    "Delivery workspace head changed outside an admitted code turn"
                )
            worktree = (
                await db.execute(
                    select(Worktree)
                    .where(Worktree.delivery_run_id == run.id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if worktree is None:
                worktree = Worktree(
                    repo_path=snapshot.repo_path,
                    worktree_path=snapshot.worktree_path,
                    branch_name=snapshot.branch,
                    base_branch=snapshot.base_branch,
                    task_id=task.id,
                    delivery_run_id=run.id,
                    last_verified_head=snapshot.head_sha,
                    cleanup_status="retained",
                    status="active",
                )
                db.add(worktree)
                await db.flush()
            elif (
                worktree.repo_path != snapshot.repo_path
                or worktree.worktree_path != snapshot.worktree_path
                or worktree.branch_name != snapshot.branch
            ):
                raise DeliverySubjectChanged("Delivery Worktree binding changed")
            else:
                worktree.last_verified_head = snapshot.head_sha
            run.worktree_id = worktree.id
            run.workspace_path = snapshot.worktree_path
            run.base_sha = snapshot.base_sha
            run.head_sha = snapshot.head_sha
            run.head_tree_sha = snapshot.head_tree_sha
            task.target_repo = snapshot.repo_path
            task.last_cwd = snapshot.worktree_path
            task.target_branch = snapshot.base_branch
            task.result_branch = snapshot.branch
            await db.commit()
        return snapshot

    async def _drive_planning(
        self,
        lease: _Lease,
        context: _RunContext,
    ) -> bool:
        if context.activity == "ready":
            await self._ensure_workspace(lease, context)
            context = await self._context(lease)
            if context.phase != "planning" or context.activity != "ready":
                return True
            async with self.db_factory() as db:
                cycle = await db.get(
                    DeliveryCycle,
                    context.cycle_id,
                    populate_existing=True,
                )
                if cycle is None:
                    raise DeliverySubjectChanged("Delivery cycle disappeared")
                invocation_id = cycle.plan_invocation_id
                trigger_payload = dict(cycle.trigger_payload or {})
            if invocation_id is None:
                snapshot = await self.capabilities.create(
                    task_id=context.developer_task_id,
                    capability_key="plan",
                    request_payload={
                        "title": f"Delivery cycle {context.cycle_number}: {context.title}",
                        "prompt": self._plan_prompt(context, trigger_payload),
                    },
                    idempotency_key=(
                        f"delivery:{context.run_id}:cycle:{context.cycle_number}:plan"
                    ),
                )
                invocation_id = snapshot.invocation_id
            async with self.db_factory() as db:
                run = await lock_run(db, lease.run_id)
                self._assert_lease(run, lease)
                cycle = await lock_current_cycle(db, run)
                if (
                    run.phase != "planning"
                    or run.activity != "ready"
                    or cycle.id != context.cycle_id
                ):
                    await db.rollback()
                    return True
                if cycle.plan_invocation_id not in (None, invocation_id):
                    raise DeliverySubjectChanged(
                        "Delivery cycle acquired a different Plan invocation"
                    )
                cycle.plan_invocation_id = invocation_id
                cycle.state_version += 1
                cycle.updated_at = _utcnow()
                await apply_run_event(
                    db,
                    run=run,
                    event=DeliveryReducerEvent("plan_requested"),
                    actor_kind="controller",
                    actor_id=self.owner_id,
                    metadata={"capability_invocation_id": invocation_id},
                )
                await db.commit()
            if self.capability_coordinator is not None:
                await _maybe_call(getattr(self.capability_coordinator, "wake", None))
            return True

        if context.activity != "waiting":
            raise DeliverySubjectChanged("Planning phase has an invalid activity")
        async with self.db_factory() as db:
            cycle = await db.get(
                DeliveryCycle,
                context.cycle_id,
                populate_existing=True,
            )
            invocation_id = cycle.plan_invocation_id if cycle is not None else None
        if invocation_id is None:
            raise DeliverySubjectChanged("Planning wait lost its Capability handle")
        snapshot = await self.capabilities.observe(invocation_id)
        if snapshot.status in _CAPABILITY_PENDING_STATUSES:
            return False
        if snapshot.status in _CAPABILITY_TERMINAL_ERRORS:
            await self._fail_run(
                lease,
                code=snapshot.error_code or "plan_capability_failed",
                message=snapshot.error_message or f"Plan capability {snapshot.status}",
            )
            return False
        if (
            snapshot.status not in {"ready", "completed"}
            or snapshot.result_kind != "plan_version"
            or snapshot.result_id is None
        ):
            raise DeliverySubjectChanged("Plan capability returned an invalid result")
        if snapshot.status == "ready":
            await self.capabilities.consume(snapshot)
        async with self.db_factory() as db:
            plan_version = await db.get(
                PlanVersion,
                snapshot.result_id,
                populate_existing=True,
            )
            if plan_version is None:
                raise DeliverySubjectChanged("Plan capability result disappeared")
            await db.rollback()
        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            cycle = await lock_current_cycle(db, run)
            if (
                run.phase != "planning"
                or run.activity != "waiting"
                or cycle.plan_invocation_id != invocation_id
            ):
                await db.rollback()
                return True
            cycle.plan_version_id = snapshot.result_id
            cycle.status = "coding"
            cycle.state_version += 1
            cycle.updated_at = _utcnow()
            await apply_run_event(
                db,
                run=run,
                event=DeliveryReducerEvent("plan_ready"),
                actor_kind="capability",
                actor_id=str(invocation_id),
                metadata={
                    "plan_version_id": snapshot.result_id,
                    "result_hash": snapshot.result_hash,
                },
            )
            await db.commit()
        return True

    @staticmethod
    def _plan_prompt(context: _RunContext, trigger_payload: dict) -> str:
        trigger = canonical_json(trigger_payload)
        return (
            "Create a concrete implementation plan for this Delivery Loop cycle. "
            "The plan must address every item in the trigger evidence and remain "
            "scoped to the requested repository. Include tests when the Requirements "
            "permit repository or runtime writes. When the Requirements explicitly "
            "prohibit writes or request inspection only, produce a report-only plan: "
            "use reproducible evidence from the fixed Git revision, avoid commands "
            "that can refresh the index or create caches, use GIT_OPTIONAL_LOCKS=0 for "
            "Git worktree inspection when unavoidable, rely on the Delivery Controller "
            "for the final repository-state audit, and satisfy repository instructions "
            "through read-only fixed-revision sources. Do not invent implementation or "
            "test work that contradicts the Requirements.\n\n"
            "The managed worktree and Delivery branch already exist. The Developer "
            "must leave changes uncommitted; the Delivery Controller alone commits, "
            "pushes, and creates or updates the pull request. Do not put branch "
            "creation, git commit, push, or pull-request operations in the plan. "
            "Do not require the Developer to stop merely because ignored or "
            "controller-managed files exist outside the intended diff.\n\n"
            f"Requirements:\n{context.requirements}\n\n"
            f"Cycle trigger (JSON):\n{trigger}"
        )

    async def _drive_coding(
        self,
        lease: _Lease,
        context: _RunContext,
    ) -> bool:
        if context.activity == "ready":
            return await self._dispatch_code_turn(lease, context)
        if context.activity != "running":
            raise DeliverySubjectChanged("Coding phase has an invalid activity")
        return await self._observe_code_turn(lease, context)

    async def _dispatch_code_turn(
        self,
        lease: _Lease,
        context: _RunContext,
    ) -> bool:
        async with self.db_factory() as db:
            cycle = await db.get(
                DeliveryCycle,
                context.cycle_id,
                populate_existing=True,
            )
            plan = (
                await db.get(PlanVersion, cycle.plan_version_id, populate_existing=True)
                if cycle is not None and cycle.plan_version_id is not None
                else None
            )
            if cycle is None or plan is None:
                raise DeliverySubjectChanged("Coding phase lost its approved Plan")
            prompt = self._code_prompt(context, cycle, plan)
        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            cycle = await lock_current_cycle(db, run)
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == run.developer_task_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            policy = run.policy_snapshot
            frozen_route_matches = bool(
                isinstance(policy, dict)
                and value_hash(policy) == run.policy_hash
                and task is not None
                and task.provider == policy.get("provider")
                and task.model == policy.get("model")
                and task.codex_service_tier == policy.get("codex_service_tier")
                and task.effort_level == policy.get("effort_level")
            )
            if (
                run.phase != "coding"
                or run.activity != "ready"
                or cycle.id != context.cycle_id
            ):
                await db.rollback()
                return True
            if not frozen_route_matches:
                raise DeliverySubjectChanged(
                    "Developer Task routing no longer matches the frozen policy"
                )
            active_turn = await db.scalar(
                select(DeliveryTurn.id)
                .where(DeliveryTurn.active_run_id == run.id)
                .limit(1)
            )
            if active_turn is not None:
                existing = await db.get(
                    DeliveryTurn,
                    active_turn,
                    populate_existing=True,
                )
                if existing is None or existing.cycle_id != cycle.id:
                    raise DeliverySubjectChanged(
                        f"Delivery Run has a foreign active turn {active_turn}"
                    )
                # A crash or legacy state may expose an admitted Turn while the
                # Run is ready. Re-enter observation of that same durable Turn
                # instead of dispatching a duplicate.
                await apply_run_event(
                    db,
                    run=run,
                    event=DeliveryReducerEvent("code_started"),
                    actor_kind="controller",
                    actor_id=self.owner_id,
                    metadata={
                        "turn_generation": existing.generation,
                        "resumed_existing": True,
                    },
                )
                await db.commit()
                return True
            if (
                task is None
                or task.delivery_run_id != run.id
                or task.delivery_role != "developer"
                or task.mode != "delivery_loop"
                or task.worker_id is not None
                or task.shared_from_id is not None
                or task.status not in _TASK_REUSABLE_STATUSES
            ):
                raise DeliverySubjectChanged(
                    "Developer Task is not an idle controller-owned generation"
                )
            if not await self._developer_task_settled_locked(db, task):
                await db.rollback()
                return False
            if not await self._fence_developer_task_graph_locked(db, task):
                await db.rollback()
                return False
            generation = run.turn_count + 1
            checkpoint = {
                "previous_status": task.status,
                "previous_retry_count": task.retry_count,
                "previous_started_at": _iso(task.started_at),
                "previous_completed_at": _iso(task.completed_at),
                "previous_session_id": task.session_id,
            }
            prompt_payload = {
                "schema_version": 1,
                "cycle_number": cycle.cycle_number,
                "plan_version_id": cycle.plan_version_id,
                "trigger_kind": cycle.trigger_kind,
                "trigger_hash": cycle.trigger_hash,
                "requirements_hash": run.requirements_hash,
            }
            turn = DeliveryTurn(
                run_id=run.id,
                cycle_id=cycle.id,
                generation=generation,
                correlation_id=f"delivery:{run.id}:turn:{generation}",
                active_run_id=run.id,
                purpose="code",
                trigger_kind="plan_ready",
                trigger_payload=dict(cycle.trigger_payload or {}),
                prompt_payload=prompt_payload,
                prompt_hash=value_hash({"payload": prompt_payload, "prompt": prompt}),
                status="queued",
                task_id=task.id,
                task_retry_count=task.retry_count,
                task_session_id=task.session_id,
                checkpoint=checkpoint,
                checkpoint_status="admitted",
                progress_signature_before=run.head_sha,
                attempts=1,
            )
            db.add(turn)
            task.description = prompt
            task.status = "pending"
            task.instance_id = None
            task.started_at = None
            task.completed_at = None
            task.error_message = None
            task.target_repo = run.workspace_path or task.target_repo
            task.last_cwd = run.workspace_path
            task.target_branch = run.base_branch
            # Prevent PR Monitor's legacy branch auto-binding while this Task
            # is active.  The durable DeliveryRun remains the branch authority.
            task.result_branch = None
            # Delivery Controller owns retry policy and exact turn accounting.
            task.max_retries = task.retry_count
            run.turn_count = generation
            cycle.status = "coding"
            cycle.state_version += 1
            cycle.updated_at = _utcnow()
            await apply_run_event(
                db,
                run=run,
                event=DeliveryReducerEvent("code_started"),
                actor_kind="controller",
                actor_id=self.owner_id,
                metadata={"turn_generation": generation},
            )
            await db.commit()
        await _maybe_call(self.dispatcher_wake)
        return True

    @staticmethod
    def _code_prompt(
        context: _RunContext,
        cycle: DeliveryCycle,
        plan: PlanVersion,
    ) -> str:
        trigger = canonical_json(cycle.trigger_payload or {})
        return (
            f"You are the Developer Agent for Delivery Loop cycle {cycle.cycle_number}.\n"
            "Implement the approved plan in the current managed worktree. Run "
            "sufficient tests and review your own diff. Leave all intended changes "
            "uncommitted: the Delivery Controller exclusively creates the fenced "
            "commit, pushes it, and creates or updates the pull request. Do not run "
            "git commit, push, create, merge, or modify a pull request.\n\n"
            "These Delivery Controller boundaries override any conflicting step in "
            "the approved plan. Do not stop merely because the plan mentions a "
            "prohibited Git or pull-request operation; skip that operation and "
            "implement, test, and review the repository change. Ignore pre-existing "
            "ignored or controller-managed files unless they overlap the intended "
            "diff.\n\n"
            "If the approved plan is intentionally report-only and the Requirements "
            "are fully satisfied without repository changes, finish your final response "
            f"with the exact standalone line `{_REPORT_COMPLETE_MARKER}`. Do not emit "
            "that marker when implementation, fixes, tests, or repository changes remain.\n\n"
            f"Requirements:\n{context.requirements}\n\n"
            f"Approved plan:\n{plan.content}\n\n"
            f"Cycle trigger evidence (JSON):\n{trigger}"
        )

    async def _active_turn(self, db: AsyncSession, run_id: int) -> DeliveryTurn | None:
        return (
            await db.execute(
                select(DeliveryTurn)
                .where(
                    DeliveryTurn.run_id == run_id,
                    DeliveryTurn.status.in_(DELIVERY_TURN_ACTIVE_STATUSES),
                )
                .order_by(DeliveryTurn.generation.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

    async def _observe_code_turn(
        self,
        lease: _Lease,
        context: _RunContext,
    ) -> bool:
        async with self.db_factory() as db:
            run = await db.get(DeliveryRun, lease.run_id, populate_existing=True)
            turn = await self._active_turn(db, lease.run_id)
            task = (
                await db.get(Task, run.developer_task_id, populate_existing=True)
                if run is not None and run.developer_task_id is not None
                else None
            )
            if run is None or turn is None or task is None:
                raise DeliverySubjectChanged("Active Developer turn disappeared")
            self._assert_lease(run, lease)
            checkpoint = dict(turn.checkpoint or {})
            task_snapshot = {
                "id": task.id,
                "status": task.status,
                "retry_count": task.retry_count,
                "instance_id": task.instance_id,
                "started_at": task.started_at,
                "completed_at": task.completed_at,
                "session_id": task.session_id,
                "pty_background_generation": task.pty_background_generation,
                "delivery_run_id": task.delivery_run_id,
                "provider": task.provider,
                "model": task.model,
                "codex_service_tier": task.codex_service_tier,
                "effort_level": task.effort_level,
            }
            turn_snapshot = {
                "id": turn.id,
                "generation": turn.generation,
                "status": turn.status,
                "retry_count": turn.task_retry_count,
                "started_at": turn.task_started_at,
            }
        if task_snapshot["delivery_run_id"] != lease.run_id:
            raise DeliverySubjectChanged("Developer Task changed Delivery owner")
        policy = run.policy_snapshot
        if (
            not isinstance(policy, dict)
            or value_hash(policy) != run.policy_hash
            or task_snapshot["provider"] != policy.get("provider")
            or task_snapshot["model"] != policy.get("model")
            or task_snapshot["codex_service_tier"] != policy.get("codex_service_tier")
            or task_snapshot["effort_level"] != policy.get("effort_level")
        ):
            raise DeliverySubjectChanged(
                "Developer Task routing changed after turn admission"
            )
        if task_snapshot["retry_count"] != turn_snapshot["retry_count"]:
            raise DeliverySubjectChanged("Developer Task retry generation changed")

        status = str(task_snapshot["status"])
        if status == "pending":
            if turn_snapshot["status"] not in {"queued", "dispatching"}:
                raise DeliverySubjectChanged(
                    "Running Developer turn returned to pending"
                )
            return False
        if status in _TASK_ACTIVE_STATUSES:
            started_at = task_snapshot["started_at"]
            if started_at is None:
                raise DeliverySubjectChanged("Active Developer Task has no started_at")
            if (
                turn_snapshot["started_at"] is not None
                and turn_snapshot["started_at"] != started_at
            ):
                raise DeliverySubjectChanged("Developer Task active generation changed")
            if turn_snapshot["status"] != "running":
                async with self.db_factory() as db:
                    run = await lock_run(db, lease.run_id)
                    self._assert_lease(run, lease)
                    turn = (
                        await db.execute(
                            select(DeliveryTurn)
                            .where(DeliveryTurn.id == turn_snapshot["id"])
                            .with_for_update()
                            .execution_options(populate_existing=True)
                        )
                    ).scalar_one_or_none()
                    task = (
                        await db.execute(
                            select(Task)
                            .where(Task.id == run.developer_task_id)
                            .with_for_update()
                            .execution_options(populate_existing=True)
                        )
                    ).scalar_one_or_none()
                    if (
                        turn is None
                        or task is None
                        or turn.status not in {"queued", "dispatching"}
                        or task.status not in _TASK_ACTIVE_STATUSES
                        or task.retry_count != turn.task_retry_count
                        or task.started_at != started_at
                    ):
                        await db.rollback()
                        return True
                    turn.status = "running"
                    turn.task_instance_id = task.instance_id
                    turn.task_started_at = task.started_at
                    turn.task_session_id = task.session_id
                    turn.started_at = task.started_at
                    turn.checkpoint_status = "running"
                    await db.commit()
                return True
            return False

        if status not in _TASK_TERMINAL_STATUSES:
            raise DeliverySubjectChanged(f"Unexpected Developer Task status {status!r}")
        if not await self._terminal_task_generation_settled(
            task_id=int(task_snapshot["id"]),
            snapshot=task_snapshot,
        ):
            return False
        started_at = task_snapshot["started_at"]
        completed_at = task_snapshot["completed_at"]
        if started_at is None or completed_at is None:
            raise DeliverySubjectChanged(
                "Developer terminal lacks generation timestamps"
            )
        if turn_snapshot["started_at"] is not None:
            if turn_snapshot["started_at"] != started_at:
                raise DeliverySubjectChanged(
                    "Developer terminal belongs to another turn"
                )
        else:
            if _iso(started_at) == checkpoint.get("previous_started_at"):
                raise DeliverySubjectChanged(
                    "Developer terminal did not start a new turn"
                )
        if _iso(completed_at) == checkpoint.get("previous_completed_at"):
            raise DeliverySubjectChanged("Developer terminal predates turn admission")
        if status != "completed":
            return await self._finalize_failed_turn(
                lease,
                turn_id=int(turn_snapshot["id"]),
                started_at=started_at,
                completed_at=completed_at,
                task_status=status,
            )

        # The network-disabled Developer cannot write the linked worktree's Git
        # metadata.  Commit its reviewed working tree through the Controller's
        # hardened, idempotent Git boundary only after the exact Task generation
        # and process owners have settled.
        try:
            snapshot = await self.workspace.commit_changes(
                repo_path=context.repo_path,
                worktree_path=context.workspace_path or "",
                branch=context.delivery_branch,
                base_branch=context.base_branch,
                expected_head_sha=context.head_sha or "",
                run_id=context.run_id,
                turn_generation=int(turn_snapshot["generation"]),
                title=context.title,
                expected_repo_full_name=context.repo_full_name,
            )
        except DeliveryWorkspaceError as exc:
            raise DeliverySubjectChanged(
                f"Delivery workspace validation failed: {exc}"
            ) from exc
        return await self._finalize_completed_turn(
            lease,
            context=context,
            turn_id=int(turn_snapshot["id"]),
            started_at=started_at,
            completed_at=completed_at,
            snapshot=snapshot,
        )

    async def _finalize_failed_turn(
        self,
        lease: _Lease,
        *,
        turn_id: int,
        started_at: datetime,
        completed_at: datetime,
        task_status: str,
    ) -> bool:
        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            cycle = await lock_current_cycle(db, run)
            turn = (
                await db.execute(
                    select(DeliveryTurn)
                    .where(DeliveryTurn.id == turn_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == run.developer_task_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if (
                run.phase != "coding"
                or run.activity != "running"
                or turn is None
                or turn.cycle_id != cycle.id
                or turn.status not in DELIVERY_TURN_ACTIVE_STATUSES
                or task is None
                or task.status != task_status
                or task.started_at != started_at
                or task.completed_at != completed_at
                or task.retry_count != turn.task_retry_count
            ):
                raise DeliverySubjectChanged("Developer failure generation changed")
            if not await self._developer_task_settled_locked(db, task):
                await db.rollback()
                return False
            turn.status = "failed"
            turn.active_run_id = None
            turn.completed_at = completed_at
            turn.last_error = task.error_message or f"developer_turn_{task_status}"
            complete_cycle(cycle, status="failed")
            await apply_run_event(
                db,
                run=run,
                event=DeliveryReducerEvent(
                    "fail",
                    {
                        "error_code": f"developer_turn_{task_status}",
                        "error_message": turn.last_error,
                    },
                ),
                actor_kind="developer",
                actor_id=str(task.id),
                metadata={"turn_id": turn.id},
            )
            await db.commit()
            return True

    async def _finalize_completed_turn(
        self,
        lease: _Lease,
        *,
        context: _RunContext,
        turn_id: int,
        started_at: datetime,
        completed_at: datetime,
        snapshot: DeliveryWorkspaceSnapshot,
    ) -> bool:
        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            cycle = await lock_current_cycle(db, run)
            turn = (
                await db.execute(
                    select(DeliveryTurn)
                    .where(DeliveryTurn.id == turn_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == run.developer_task_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if (
                run.phase != "coding"
                or run.activity != "running"
                or cycle.id != context.cycle_id
                or turn is None
                or turn.status not in DELIVERY_TURN_ACTIVE_STATUSES
                or task is None
                or task.status != "completed"
                or task.retry_count != turn.task_retry_count
                or task.started_at != started_at
                or task.completed_at != completed_at
            ):
                raise DeliverySubjectChanged("Developer completion generation changed")
            if not await self._developer_task_settled_locked(db, task):
                await db.rollback()
                return False
            if not await self._fence_developer_task_graph_locked(db, task):
                await db.rollback()
                return False
            if snapshot.worktree_path != run.workspace_path:
                raise DeliverySubjectChanged("Developer completed in another workspace")
            if run.base_sha != snapshot.base_sha:
                raise DeliverySubjectChanged(
                    "Delivery base changed during Developer turn"
                )
            if run.head_sha != turn.progress_signature_before:
                raise DeliverySubjectChanged(
                    "Delivery head changed before turn completion"
                )

            progressed = snapshot.head_sha != turn.progress_signature_before
            report_completed = False
            if not progressed:
                final_message = await db.scalar(
                    select(LogEntry.content)
                    .where(
                        LogEntry.task_id == task.id,
                        LogEntry.task_retry_count == task.retry_count,
                        LogEntry.task_turn_generation == turn.generation,
                        LogEntry.event_type == "message",
                        LogEntry.role == "assistant",
                        LogEntry.is_error.is_(False),
                    )
                    .order_by(LogEntry.id.desc())
                    .limit(1)
                )
                report_completed = bool(
                    final_message
                    and re.search(
                        rf"(?m)^{re.escape(_REPORT_COMPLETE_MARKER)}\s*$",
                        final_message,
                    )
                )
            run.no_progress_count = (
                0 if progressed or report_completed else run.no_progress_count + 1
            )
            turn.status = "completed"
            turn.active_run_id = None
            turn.task_instance_id = task.instance_id
            turn.task_started_at = started_at
            turn.task_session_id = task.session_id
            turn.completed_at = completed_at
            turn.checkpoint_status = "completed"
            turn.progress_signature_after = snapshot.head_sha
            cycle.result_head_sha = snapshot.head_sha
            cycle.result_head_tree_sha = snapshot.head_tree_sha
            run.head_sha = snapshot.head_sha
            run.head_tree_sha = snapshot.head_tree_sha
            run.head_generation += 1 if progressed else 0
            run.last_progress_signature = snapshot.head_sha
            task.status = "delivery_waiting"
            task.result_branch = run.delivery_branch
            task.error_message = None

            failure_code = None
            failure_message = None
            if not progressed and run.no_progress_count >= run.max_no_progress:
                failure_code = "delivery_no_progress"
                failure_message = (
                    "Developer completed without a new commit for "
                    f"{run.no_progress_count} consecutive cycles"
                )
            elif not progressed and run.cycle_count >= run.max_cycles:
                failure_code = "delivery_max_cycles"
                failure_message = (
                    "Developer completed without a new commit after the "
                    "Delivery cycle budget was exhausted"
                )

            if report_completed:
                complete_cycle(cycle)
                await apply_run_event(
                    db,
                    run=run,
                    event=DeliveryReducerEvent("report_completed"),
                    actor_kind="developer",
                    actor_id=str(task.id),
                    metadata={
                        "turn_id": turn.id,
                        "head_sha": snapshot.head_sha,
                        "head_tree_sha": snapshot.head_tree_sha,
                        "progressed": False,
                    },
                )
                task.status = "completed"
                task.error_message = None
            elif failure_code is not None and failure_message is not None:
                complete_cycle(cycle, status="failed")
                await apply_run_event(
                    db,
                    run=run,
                    event=DeliveryReducerEvent(
                        "fail",
                        {
                            "error_code": failure_code,
                            "error_message": failure_message,
                        },
                    ),
                    actor_kind="controller",
                    actor_id=self.owner_id,
                    metadata={"turn_id": turn.id},
                )
                task.status = "failed"
                task.completed_at = _utcnow()
                task.error_message = failure_message
            elif not progressed:
                approved_plan_version_id = cycle.plan_version_id
                complete_cycle(cycle)
                await apply_run_event(
                    db,
                    run=run,
                    event=DeliveryReducerEvent("developer_no_progress"),
                    actor_kind="developer",
                    actor_id=str(task.id),
                    metadata={
                        "turn_id": turn.id,
                        "head_sha": snapshot.head_sha,
                        "head_tree_sha": snapshot.head_tree_sha,
                        "no_progress_count": run.no_progress_count,
                    },
                )
                next_cycle = await start_next_cycle(
                    db,
                    run=run,
                    trigger_kind="developer_no_progress",
                    trigger_payload={
                        "turn_id": turn.id,
                        "head_sha": snapshot.head_sha,
                        "head_tree_sha": snapshot.head_tree_sha,
                        "no_progress_count": run.no_progress_count,
                    },
                )
                next_cycle.status = "coding"
                next_cycle.plan_version_id = approved_plan_version_id
            else:
                cycle.status = "pre_review"
                cycle.state_version += 1
                cycle.updated_at = _utcnow()
                await apply_run_event(
                    db,
                    run=run,
                    event=DeliveryReducerEvent("code_completed"),
                    actor_kind="developer",
                    actor_id=str(task.id),
                    metadata={
                        "turn_id": turn.id,
                        "head_sha": snapshot.head_sha,
                        "head_tree_sha": snapshot.head_tree_sha,
                        "progressed": progressed,
                    },
                )
            await db.commit()
            return True

    async def _drive_review(
        self,
        lease: _Lease,
        context: _RunContext,
    ) -> bool:
        if context.activity == "ready":
            if not context.base_sha or not context.head_sha:
                raise DeliverySubjectChanged("Code Review subject is incomplete")
            async with self.db_factory() as db:
                cycle = await db.get(
                    DeliveryCycle,
                    context.cycle_id,
                    populate_existing=True,
                )
                invocation_id = cycle.review_invocation_id if cycle else None
            if invocation_id is None:
                snapshot = await self.capabilities.create(
                    task_id=context.developer_task_id,
                    capability_key="code_review",
                    request_payload={
                        "base_sha": context.base_sha,
                        "head_sha": context.head_sha,
                    },
                    idempotency_key=(
                        f"delivery:{context.run_id}:cycle:{context.cycle_number}:review"
                    ),
                )
                invocation_id = snapshot.invocation_id
            async with self.db_factory() as db:
                run = await lock_run(db, lease.run_id)
                self._assert_lease(run, lease)
                cycle = await lock_current_cycle(db, run)
                if run.phase != "pre_review" or run.activity != "ready":
                    await db.rollback()
                    return True
                if cycle.review_invocation_id not in (None, invocation_id):
                    raise DeliverySubjectChanged(
                        "Delivery cycle acquired a different Review invocation"
                    )
                cycle.review_invocation_id = invocation_id
                cycle.state_version += 1
                cycle.updated_at = _utcnow()
                await apply_run_event(
                    db,
                    run=run,
                    event=DeliveryReducerEvent("review_requested"),
                    actor_kind="controller",
                    actor_id=self.owner_id,
                    metadata={"capability_invocation_id": invocation_id},
                )
                await db.commit()
            if self.capability_coordinator is not None:
                await _maybe_call(getattr(self.capability_coordinator, "wake", None))
            return True

        if context.activity != "waiting":
            raise DeliverySubjectChanged("Code Review phase has an invalid activity")
        async with self.db_factory() as db:
            cycle = await db.get(
                DeliveryCycle,
                context.cycle_id,
                populate_existing=True,
            )
            invocation_id = cycle.review_invocation_id if cycle else None
        if invocation_id is None:
            raise DeliverySubjectChanged("Code Review wait lost its Capability handle")
        snapshot = await self.capabilities.observe(invocation_id)
        if snapshot.status in _CAPABILITY_PENDING_STATUSES:
            return False
        if snapshot.status in _CAPABILITY_TERMINAL_ERRORS:
            await self._fail_run(
                lease,
                code=snapshot.error_code or "code_review_capability_failed",
                message=(
                    snapshot.error_message
                    or f"Code Review capability {snapshot.status}"
                ),
            )
            return False
        if (
            snapshot.status not in {"ready", "completed"}
            or snapshot.result_kind != "code_review_result"
            or snapshot.result_id is None
            or snapshot.verdict not in {"approved", "changes_requested"}
        ):
            raise DeliverySubjectChanged("Code Review returned an invalid result")
        subject = snapshot.subject_ref or {}
        expected_subject = {
            "base_sha": context.base_sha,
            "head_sha": context.head_sha,
            "head_tree_sha": context.head_tree_sha,
        }
        for field, expected in expected_subject.items():
            if expected is not None and subject.get(field) != expected:
                raise DeliverySubjectChanged(
                    f"Code Review result {field} does not match the Delivery subject"
                )
        patch_sha = subject.get("patch_sha256")
        if not isinstance(patch_sha, str) or _HEX_64_RE.fullmatch(patch_sha) is None:
            raise DeliverySubjectChanged("Code Review result lacks patch_sha256")
        if snapshot.status == "ready":
            await self.capabilities.consume(snapshot)

        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            cycle = await lock_current_cycle(db, run)
            if (
                run.phase != "pre_review"
                or run.activity != "waiting"
                or cycle.review_invocation_id != invocation_id
                or run.base_sha != context.base_sha
                or run.head_sha != context.head_sha
                or run.head_tree_sha != context.head_tree_sha
            ):
                await db.rollback()
                return True
            cycle.review_result_id = snapshot.result_id
            cycle.review_verdict = snapshot.verdict
            cycle.review_summary = snapshot.summary
            cycle.result_patch_sha256 = patch_sha
            run.patch_sha256 = patch_sha
            if snapshot.verdict == "approved":
                cycle.status = "frontend_review"
                cycle.state_version += 1
                cycle.updated_at = _utcnow()
                await apply_run_event(
                    db,
                    run=run,
                    event=DeliveryReducerEvent("review_approved"),
                    actor_kind="capability",
                    actor_id=str(invocation_id),
                    metadata={"review_result_id": snapshot.result_id},
                )
                # Direct service callers and legacy policies intentionally
                # keep the original Code Review -> Publish flow. Record the
                # omitted Browser gate explicitly instead of making it
                # invisible in the progress timeline.
                if context.frontend_review_mode == "off":
                    skip_reason = "Frontend review is disabled by Delivery policy"
                    cycle.frontend_review_skip_reason = skip_reason
                    cycle.status = "publishing"
                    cycle.state_version += 1
                    cycle.updated_at = _utcnow()
                    await apply_run_event(
                        db,
                        run=run,
                        event=DeliveryReducerEvent("frontend_review_skipped"),
                        actor_kind="controller",
                        actor_id=self.owner_id,
                        metadata={"skip_reason": skip_reason},
                    )
            else:
                if run.cycle_count >= run.max_cycles:
                    failure_message = (
                        "Code Review requested changes after the Delivery "
                        "cycle budget was exhausted"
                    )
                    task = (
                        await db.execute(
                            select(Task)
                            .where(Task.id == run.developer_task_id)
                            .with_for_update()
                            .execution_options(populate_existing=True)
                        )
                    ).scalar_one_or_none()
                    if task is None or task.status not in _TASK_REUSABLE_STATUSES:
                        raise DeliverySubjectChanged(
                            "Developer Task changed before Review budget failure"
                        )
                    if not await self._developer_task_settled_locked(db, task):
                        await db.rollback()
                        return False
                    if not await self._fence_developer_task_graph_locked(
                        db,
                        task,
                    ):
                        await db.rollback()
                        return False
                    complete_cycle(cycle, status="failed")
                    await apply_run_event(
                        db,
                        run=run,
                        event=DeliveryReducerEvent(
                            "fail",
                            {
                                "error_code": "delivery_max_cycles",
                                "error_message": failure_message,
                            },
                        ),
                        actor_kind="controller",
                        actor_id=self.owner_id,
                    )
                    task.status = "failed"
                    task.completed_at = _utcnow()
                    task.error_message = failure_message
                else:
                    complete_cycle(cycle)
                    await apply_run_event(
                        db,
                        run=run,
                        event=DeliveryReducerEvent("review_changes_requested"),
                        actor_kind="capability",
                        actor_id=str(invocation_id),
                        metadata={"review_result_id": snapshot.result_id},
                    )
                    await start_next_cycle(
                        db,
                        run=run,
                        trigger_kind="pre_review_changes_requested",
                        trigger_payload={
                            "review_result_id": snapshot.result_id,
                            "summary": snapshot.summary,
                            "findings": list(snapshot.findings),
                            "subject": subject,
                        },
                    )
            await db.commit()
        return True

    def _frontend_harness_service(self):
        if self.test_harness_service is not None:
            return self.test_harness_service
        from backend.services.test_harness import test_harness_service

        return test_harness_service

    async def _skip_frontend_review(
        self,
        lease: _Lease,
        context: _RunContext,
        *,
        reason: str,
    ) -> bool:
        """Persist one deterministic auto-policy skip with exact Task fences."""

        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            cycle = await lock_current_cycle(db, run)
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == run.developer_task_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if (
                run.phase != "frontend_review"
                or run.activity != "ready"
                or cycle.id != context.cycle_id
                or cycle.status != "frontend_review"
                or cycle.frontend_review_run_id is not None
                or run.head_sha != context.head_sha
                or run.head_tree_sha != context.head_tree_sha
                or task is None
                or task.status != "delivery_waiting"
            ):
                await db.rollback()
                return True
            if not await self._developer_task_settled_locked(db, task):
                await db.rollback()
                return False
            if not await self._fence_developer_task_graph_locked(db, task):
                await db.rollback()
                return False
            skip_reason = reason.strip()[:2000]
            cycle.frontend_review_skip_reason = skip_reason
            cycle.status = "publishing"
            cycle.state_version += 1
            cycle.updated_at = _utcnow()
            await apply_run_event(
                db,
                run=run,
                event=DeliveryReducerEvent("frontend_review_skipped"),
                actor_kind="controller",
                actor_id=self.owner_id,
                metadata={"skip_reason": skip_reason},
            )
            await db.commit()
        return True

    async def _drive_frontend_review(
        self,
        lease: _Lease,
        context: _RunContext,
    ) -> bool:
        """Run Browser/Test Harness as a read-only gate over the exact head."""

        if context.frontend_review_mode == "off":
            return await self._skip_frontend_review(
                lease,
                context,
                reason="Frontend review is disabled by Delivery policy",
            )
        if not context.head_sha or not context.head_tree_sha:
            raise DeliverySubjectChanged("Frontend review subject is incomplete")
        if not context.base_sha or not context.workspace_path:
            raise DeliverySubjectChanged("Frontend review workspace is incomplete")
        if context.activity == "ready":
            legacy_key = (
                f"delivery:{context.run_id}:cycle:{context.cycle_id}:"
                f"frontend:{context.head_sha}"
            )
            async with self.db_factory() as db:
                cycle = await db.get(DeliveryCycle, context.cycle_id)
                task = await db.get(Task, context.developer_task_id)
                legacy_harness = await db.scalar(
                    select(TestHarnessRun).where(
                        TestHarnessRun.task_id == context.developer_task_id,
                        TestHarnessRun.idempotency_scope
                        == f"task:{context.developer_task_id}",
                        TestHarnessRun.idempotency_key == legacy_key,
                    )
                )
            if (
                legacy_harness is not None
                and cycle is not None
                and task is not None
                and cycle.frontend_review_run_id is None
                and cycle.frontend_review_config_snapshot is None
            ):
                owner_identity = test_harness_owner_identity(task)
                if (
                    legacy_harness.target_kind != "current_workspace"
                    or legacy_harness.owner_task_incarnation_id
                    != owner_identity.incarnation_id
                    or legacy_harness.owner_task_retry_count
                    != owner_identity.retry_count
                    or legacy_harness.owner_task_turn_generation
                    != owner_identity.turn_generation
                    or legacy_harness.owner_task_status != owner_identity.status
                ):
                    raise DeliverySubjectChanged(
                        "Legacy frontend review recovery identity changed"
                    )
                from backend.services.test_harness_execution_context import (
                    execution_context_from_runtime,
                )

                try:
                    execution_context = execution_context_from_runtime(
                        legacy_harness.runtime_config,
                        target_kind="current_workspace",
                    )
                    preview = execution_context["preview_config"]
                    frozen_config = {
                        "version": 2,
                        "default_profile": "default",
                        "profiles": [
                            {
                                **preview,
                                "id": "default",
                                "match_paths": ["**"],
                                "enabled": True,
                            }
                        ],
                    }
                except Exception:
                    # Pre-execution-context Harness rows can still be bound by
                    # their exact owner/head/idempotency evidence. They never
                    # start another profile, so no mutable Preview config is
                    # consulted during this recovery path.
                    frozen_config = None
                async with self.db_factory() as db:
                    run = await lock_run(db, lease.run_id)
                    self._assert_lease(run, lease)
                    cycle = await lock_current_cycle(db, run)
                    if (
                        run.phase != "frontend_review"
                        or run.activity != "ready"
                        or cycle.id != context.cycle_id
                        or cycle.frontend_review_run_id is not None
                    ):
                        raise DeliverySubjectChanged(
                            "Legacy frontend review changed before recovery"
                        )
                    cycle.frontend_review_config_snapshot = frozen_config
                    cycle.frontend_review_profile_ids = ["default"]
                    cycle.frontend_review_profile_index = 0
                    cycle.frontend_review_results = []
                    cycle.frontend_review_run_id = legacy_harness.id
                    cycle.state_version += 1
                    cycle.updated_at = _utcnow()
                    await apply_run_event(
                        db,
                        run=run,
                        event=DeliveryReducerEvent("frontend_review_requested"),
                        actor_kind="test_harness",
                        actor_id=legacy_harness.id,
                        metadata={
                            "test_harness_run_id": legacy_harness.id,
                            "preview_profile_id": "default",
                            "legacy_recovery": True,
                        },
                    )
                    await db.commit()
                return True
        if context.activity == "ready" and (
            not isinstance(settings.auth_token, str) or not settings.auth_token.strip()
        ):
            message = "Frontend review unavailable: AUTH_TOKEN is not configured"
            if context.frontend_review_mode == "auto":
                return await self._skip_frontend_review(
                    lease,
                    context,
                    reason=message,
                )
            await self._fail_run(
                lease,
                code="frontend_review_unavailable",
                message=message,
            )
            return False

        if context.activity == "ready":
            async with self.db_factory() as db:
                cycle = await db.get(
                    DeliveryCycle,
                    context.cycle_id,
                    populate_existing=True,
                )
                project = await db.get(Project, context.project_id)
                task = await db.get(Task, context.developer_task_id)
                if cycle is None or project is None:
                    raise DeliverySubjectChanged(
                        "Frontend review Project or cycle disappeared"
                    )
                frozen_config = cycle.frontend_review_config_snapshot
                selected_profile_ids = list(cycle.frontend_review_profile_ids or [])
                selected_index = cycle.frontend_review_profile_index

            if frozen_config is None:
                changed_paths = await self.workspace.list_changed_paths(
                    worktree_path=context.workspace_path,
                    base_sha=context.base_sha,
                    head_sha=context.head_sha,
                )
                from backend.services.workspace_review import (
                    resolve_preview_config,
                    validate_preview_profiles,
                    workspace_review_capability,
                )

                try:
                    capability = workspace_review_capability(task, project)
                    if not capability.get("available"):
                        raise ValueError(
                            capability.get("reason") or "trusted Preview is unavailable"
                        )
                    if project.preview_config is None:
                        # Compatibility for injected/legacy capability adapters;
                        # the production capability cannot report available in
                        # this state.
                        collection = None
                        selected_profiles = [{"id": "default"}]
                    else:
                        collection = validate_preview_profiles(
                            project.preview_config,
                            Path(context.workspace_path),
                        )
                        selected_profiles = resolve_preview_config(
                            collection,
                            Path(context.workspace_path),
                            changed_paths=changed_paths,
                        )
                except Exception as exc:
                    message = f"Frontend review unavailable: {exc}"
                    if context.frontend_review_mode == "auto":
                        return await self._skip_frontend_review(
                            lease,
                            context,
                            reason=message,
                        )
                    await self._fail_run(
                        lease,
                        code="frontend_review_unavailable",
                        message=message,
                    )
                    return False
                selected_profile_ids = [profile["id"] for profile in selected_profiles]
                if not selected_profile_ids:
                    message = (
                        "Frontend review found no trusted Preview profile matching "
                        "the final changed paths"
                    )
                    if context.frontend_review_mode == "auto":
                        return await self._skip_frontend_review(
                            lease,
                            context,
                            reason=message,
                        )
                    await self._fail_run(
                        lease,
                        code="frontend_review_unavailable",
                        message=message,
                    )
                    return False
                frozen_config = (
                    {
                        "version": 2,
                        "default_profile": collection["default_profile"],
                        "profiles": collection["profiles"],
                    }
                    if collection is not None
                    else None
                )
                async with self.db_factory() as db:
                    run = await lock_run(db, lease.run_id)
                    self._assert_lease(run, lease)
                    cycle = await lock_current_cycle(db, run)
                    if (
                        run.phase != "frontend_review"
                        or run.activity != "ready"
                        or cycle.id != context.cycle_id
                        or run.head_sha != context.head_sha
                    ):
                        raise DeliverySubjectChanged(
                            "Frontend review subject changed before profile selection"
                        )
                    if cycle.frontend_review_config_snapshot is None:
                        cycle.frontend_review_config_snapshot = frozen_config
                        cycle.frontend_review_profile_ids = selected_profile_ids
                        cycle.frontend_review_profile_index = 0
                        cycle.frontend_review_results = []
                        cycle.state_version += 1
                        cycle.updated_at = _utcnow()
                        await db.commit()
                    else:
                        await db.rollback()
                        frozen_config = cycle.frontend_review_config_snapshot
                        selected_profile_ids = list(
                            cycle.frontend_review_profile_ids or []
                        )
                        selected_index = cycle.frontend_review_profile_index
            if selected_index < 0 or selected_index >= len(selected_profile_ids):
                raise DeliverySubjectChanged(
                    "Frontend review profile cursor is invalid"
                )
            current_preview_profile_id = selected_profile_ids[selected_index]

        if context.activity == "ready":
            harness_idempotency_key = (
                f"delivery:{context.run_id}:cycle:{context.cycle_id}:"
                f"frontend:{context.head_sha}:{current_preview_profile_id}"
            )
            async with self.db_factory() as db:
                run = await db.get(DeliveryRun, lease.run_id, populate_existing=True)
                self._assert_lease(run, lease)
                cycle = await db.get(
                    DeliveryCycle,
                    context.cycle_id,
                    populate_existing=True,
                )
                task = await db.get(
                    Task,
                    context.developer_task_id,
                    populate_existing=True,
                )
                project = await db.get(Project, context.project_id)
                if (
                    run.phase != "frontend_review"
                    or run.activity != "ready"
                    or cycle is None
                    or cycle.id != run.current_cycle_id
                    or cycle.status != "frontend_review"
                    or task is None
                    or task.status != "delivery_waiting"
                    or task.delivery_run_id != run.id
                    or project is None
                ):
                    raise DeliverySubjectChanged(
                        "Frontend review owner changed before admission"
                    )
                if not await self._developer_task_settled_locked(db, task):
                    await db.rollback()
                    return False
                owner_identity = test_harness_owner_identity(task)
                owner_user_id = run.created_by
                if cycle.frontend_review_run_id is not None:
                    raise DeliverySubjectChanged(
                        "Frontend review handle exists before its waiting transition"
                    )
                recoverable_harness = await db.scalar(
                    select(TestHarnessRun).where(
                        TestHarnessRun.task_id == task.id,
                        TestHarnessRun.idempotency_scope == f"task:{task.id}",
                        TestHarnessRun.idempotency_key == harness_idempotency_key,
                    )
                )
                recovery_harness_id = None
                if recoverable_harness is not None:
                    if (
                        recoverable_harness.target_kind != "current_workspace"
                        or (
                            recoverable_harness.target_spec.get("preview_profile_id")
                            not in (
                                {None, "default"}
                                if current_preview_profile_id == "default"
                                else {current_preview_profile_id}
                            )
                        )
                        or recoverable_harness.project_id != task.project_id
                        or recoverable_harness.owner_task_incarnation_id
                        != owner_identity.incarnation_id
                        or recoverable_harness.owner_task_retry_count
                        != owner_identity.retry_count
                        or recoverable_harness.owner_task_turn_generation
                        != owner_identity.turn_generation
                        or recoverable_harness.owner_task_status
                        != owner_identity.status
                        or (
                            recoverable_harness.source_git_head is not None
                            and recoverable_harness.source_git_head != context.head_sha
                        )
                    ):
                        raise DeliverySubjectChanged(
                            "Recovered frontend review does not match the exact owner"
                        )
                    recovery_harness_id = recoverable_harness.id
                elif not await self._fence_developer_task_graph_locked(db, task):
                    await db.rollback()
                    return False
                capability = (
                    None
                    if recovery_harness_id is not None
                    else {"available": True, "reason": None}
                )
                await db.rollback()

            harness_run_id = recovery_harness_id
            if harness_run_id is None:
                unavailable_reason = None
                if (
                    not isinstance(settings.auth_token, str)
                    or not settings.auth_token.strip()
                ):
                    unavailable_reason = "AUTH_TOKEN is not configured"
                elif not capability or not capability.get("available"):
                    unavailable_reason = str(
                        (capability or {}).get("reason")
                        or "trusted Preview is unavailable"
                    )
                if unavailable_reason is not None:
                    message = f"Frontend review unavailable: {unavailable_reason}"
                    if context.frontend_review_mode == "auto":
                        return await self._skip_frontend_review(
                            lease,
                            context,
                            reason=message,
                        )
                    await self._fail_run(
                        lease,
                        code="frontend_review_unavailable",
                        message=message,
                    )
                    return False

                from backend.services.test_harness_contracts import TestHarnessSpec

                goal = (
                    f"Validate Preview profile '{current_preview_profile_id}' for "
                    "the current Delivery implementation as a black-box "
                    "frontend. Verify the requested user-visible behavior, runtime "
                    "health, error feedback, and regressions. Report issues only; "
                    "do not edit source code.\n\nDelivery requirements:\n"
                    + context.requirements
                )[:20_000]
                service = self._frontend_harness_service()
                try:
                    start_kwargs = {
                        "task_id": context.developer_task_id,
                        "spec": TestHarnessSpec(
                            target_kind="current_workspace",
                            target={
                                "preview_profile_id": current_preview_profile_id,
                            },
                            goal=goal,
                            profile=context.frontend_review_profile,
                            allow_actions=context.frontend_review_allow_actions,
                            idempotency_key=harness_idempotency_key,
                        ),
                        "owner_user_id": owner_user_id,
                        "owner_identity": owner_identity,
                    }
                    if frozen_config is not None:
                        start_kwargs["preview_config_override"] = frozen_config
                    harness_run = await service.start_task_run(
                        **start_kwargs,
                    )
                    harness_run_id = harness_run.id
                except Exception as exc:
                    from backend.services.test_harness import TestHarnessBusyError

                    if isinstance(exc, TestHarnessBusyError):
                        return False
                    # Starting the Harness can cross Browser/Preview boundaries.
                    # Its idempotency key makes retry safe, but an unclassified
                    # error must remain visible instead of silently publishing.
                    raise DeliverySubjectChanged(
                        f"Frontend review could not start safely: {exc}"
                    ) from exc

            async with self.db_factory() as db:
                run = await lock_run(db, lease.run_id)
                self._assert_lease(run, lease)
                cycle = await lock_current_cycle(db, run)
                task = (
                    await db.execute(
                        select(Task)
                        .where(Task.id == run.developer_task_id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                exact_harness = await db.get(
                    TestHarnessRun,
                    harness_run_id,
                    populate_existing=True,
                )
                if (
                    run.phase != "frontend_review"
                    or run.activity != "ready"
                    or cycle.id != context.cycle_id
                    or cycle.status != "frontend_review"
                    or cycle.frontend_review_run_id not in {None, harness_run_id}
                    or run.head_sha != context.head_sha
                    or task is None
                    or task.status != owner_identity.status
                    or task.incarnation_id != owner_identity.incarnation_id
                    or task.retry_count != owner_identity.retry_count
                    or task.turn_generation != owner_identity.turn_generation
                    or exact_harness is None
                    or exact_harness.task_id != task.id
                    or exact_harness.target_kind != "current_workspace"
                    or (
                        exact_harness.target_spec.get("preview_profile_id")
                        not in (
                            {None, "default"}
                            if current_preview_profile_id == "default"
                            else {current_preview_profile_id}
                        )
                    )
                    or exact_harness.owner_task_incarnation_id
                    != owner_identity.incarnation_id
                    or exact_harness.owner_task_retry_count
                    != owner_identity.retry_count
                    or exact_harness.owner_task_turn_generation
                    != owner_identity.turn_generation
                    or exact_harness.owner_task_status != owner_identity.status
                    or (
                        exact_harness.source_git_head is not None
                        and exact_harness.source_git_head != context.head_sha
                    )
                ):
                    raise DeliverySubjectChanged(
                        "Frontend review owner changed while binding its handle"
                    )
                cycle.frontend_review_run_id = harness_run_id
                cycle.state_version += 1
                cycle.updated_at = _utcnow()
                await apply_run_event(
                    db,
                    run=run,
                    event=DeliveryReducerEvent("frontend_review_requested"),
                    actor_kind="test_harness",
                    actor_id=harness_run_id,
                    metadata={"test_harness_run_id": harness_run_id},
                )
                await db.commit()
            return True

        if context.activity != "waiting":
            raise DeliverySubjectChanged(
                "Frontend review phase has an invalid activity"
            )

        async with self.db_factory() as db:
            cycle = await db.get(
                DeliveryCycle,
                context.cycle_id,
                populate_existing=True,
            )
            harness_run_id = cycle.frontend_review_run_id if cycle else None
            profile_ids = list(cycle.frontend_review_profile_ids or []) if cycle else []
            profile_index = cycle.frontend_review_profile_index if cycle else 0
            if profile_index < 0 or profile_index >= len(profile_ids):
                raise DeliverySubjectChanged(
                    "Frontend review waiting profile cursor is invalid"
                )
            current_preview_profile_id = profile_ids[profile_index]
            harness = (
                await db.get(TestHarnessRun, harness_run_id)
                if harness_run_id is not None
                else None
            )
            task = await db.get(Task, context.developer_task_id)
            attempt = None
            findings: list[TestHarnessFinding] = []
            evidence: list[TestHarnessEvidence] = []
            if harness is not None:
                attempt = (
                    await db.execute(
                        select(TestHarnessAttempt)
                        .where(TestHarnessAttempt.run_id == harness.id)
                        .order_by(TestHarnessAttempt.ordinal.desc())
                        .limit(1)
                    )
                ).scalar_one_or_none()
                findings = list(
                    (
                        await db.execute(
                            select(TestHarnessFinding)
                            .where(TestHarnessFinding.run_id == harness.id)
                            .order_by(TestHarnessFinding.ordinal)
                        )
                    ).scalars()
                )
                if attempt is not None:
                    evidence = list(
                        (
                            await db.execute(
                                select(TestHarnessEvidence).where(
                                    TestHarnessEvidence.run_id == harness.id,
                                    TestHarnessEvidence.attempt_id == attempt.id,
                                )
                            )
                        ).scalars()
                    )
            if harness is None or task is None:
                raise DeliverySubjectChanged(
                    "Frontend review wait lost its Harness owner"
                )
            owner_identity = TestHarnessOwnerIdentity(
                task_id=task.id,
                incarnation_id=harness.owner_task_incarnation_id or "",
                retry_count=harness.owner_task_retry_count or 0,
                turn_generation=harness.owner_task_turn_generation or 0,
                status=harness.owner_task_status or "",
            )
            harness_snapshot = {
                "id": harness.id,
                "task_id": harness.task_id,
                "owner_incarnation": harness.owner_task_incarnation_id,
                "owner_retry": harness.owner_task_retry_count,
                "owner_turn": harness.owner_task_turn_generation,
                "owner_status": harness.owner_task_status,
                "status": harness.status,
                "stage": harness.stage,
                "verdict": harness.verdict,
                "source_git_head": harness.source_git_head,
                "stale": harness.stale,
                "report": harness.report,
                "error": harness.error,
                "cleanup_status": harness.cleanup_status,
                "attempt_id": attempt.id if attempt else None,
                "archive_state": attempt.archive_state if attempt else None,
                "finding_ids": [item.id for item in findings],
                "evidence_ids": [item.id for item in evidence],
                "preview_profile_id": current_preview_profile_id,
            }

        if harness_snapshot["status"] not in {
            "completed",
            "failed",
            "cancelled",
            "stale",
        }:
            return False
        if harness_snapshot["cleanup_status"] != "completed":
            try:
                await self._frontend_harness_service().cancel(
                    harness_run_id,
                    expected_identity=owner_identity,
                )
            except Exception as exc:
                raise DeliverySubjectChanged(
                    f"Frontend review cleanup could not be proven: {exc}"
                ) from exc
            return False
        if (
            harness_snapshot["task_id"] != context.developer_task_id
            or harness_snapshot["owner_incarnation"] != task.incarnation_id
            or harness_snapshot["owner_retry"] != task.retry_count
            or harness_snapshot["owner_turn"] != task.turn_generation
            or harness_snapshot["owner_status"] != task.status
            or harness_snapshot["source_git_head"] != context.head_sha
            or harness_snapshot["stale"] is True
        ):
            raise DeliverySubjectChanged(
                "Frontend review result does not match the exact Delivery head"
            )

        verdict = harness_snapshot["verdict"]
        if harness_snapshot["status"] != "completed" or verdict not in {
            "passed",
            "failed",
        }:
            await self._fail_run(
                lease,
                code="frontend_review_error",
                message=(
                    harness_snapshot["error"]
                    or harness_snapshot["report"]
                    or f"Frontend review ended with {harness_snapshot['status']}/{verdict}"
                ),
            )
            return False

        evidence_kinds = {item.kind for item in evidence}
        if (
            harness_snapshot["archive_state"] != "complete"
            or not harness_snapshot["report"]
            or "screenshot" not in evidence_kinds
            or "report" not in evidence_kinds
        ):
            await self._fail_run(
                lease,
                code="frontend_review_evidence_incomplete",
                message=(
                    "Frontend review ended without a complete archived "
                    "report and screenshot evidence set"
                ),
            )
            return False

        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            cycle = await lock_current_cycle(db, run)
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == run.developer_task_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            exact_harness = await db.get(
                TestHarnessRun,
                harness_run_id,
                populate_existing=True,
            )
            exact_attempt = (
                await db.get(
                    TestHarnessAttempt,
                    harness_snapshot["attempt_id"],
                    populate_existing=True,
                )
                if harness_snapshot["attempt_id"] is not None
                else None
            )
            exact_evidence: list[TestHarnessEvidence] = []
            if exact_attempt is not None:
                exact_evidence = list(
                    (
                        await db.execute(
                            select(TestHarnessEvidence)
                            .where(
                                TestHarnessEvidence.run_id == harness_run_id,
                                TestHarnessEvidence.attempt_id == exact_attempt.id,
                            )
                            .with_for_update()
                        )
                    ).scalars()
                )
            exact_findings = list(
                (
                    await db.execute(
                        select(TestHarnessFinding)
                        .where(TestHarnessFinding.run_id == harness_run_id)
                        .order_by(TestHarnessFinding.ordinal)
                        .with_for_update()
                    )
                ).scalars()
            )
            if (
                run.phase != "frontend_review"
                or run.activity != "waiting"
                or cycle.id != context.cycle_id
                or cycle.frontend_review_run_id != harness_run_id
                or run.head_sha != context.head_sha
                or task is None
                or task.status != "delivery_waiting"
                or exact_harness is None
                or exact_harness.task_id != task.id
                or exact_harness.target_kind != "current_workspace"
                or (
                    exact_harness.target_spec.get("preview_profile_id")
                    not in (
                        {None, "default"}
                        if harness_snapshot["preview_profile_id"] == "default"
                        else {harness_snapshot["preview_profile_id"]}
                    )
                )
                or exact_harness.owner_task_incarnation_id != task.incarnation_id
                or exact_harness.owner_task_retry_count != task.retry_count
                or exact_harness.owner_task_turn_generation != task.turn_generation
                or exact_harness.owner_task_status != task.status
                or exact_harness.status != harness_snapshot["status"]
                or exact_harness.verdict != verdict
                or exact_harness.report != harness_snapshot["report"]
                or exact_harness.cleanup_status != "completed"
                or exact_harness.source_git_head != context.head_sha
                or exact_harness.stale
                or exact_attempt is None
                or exact_attempt.run_id != harness_run_id
                or exact_attempt.archive_state != harness_snapshot["archive_state"]
                or {item.id for item in exact_evidence}
                != set(harness_snapshot["evidence_ids"])
                or not {"report", "screenshot"}.issubset(
                    {item.kind for item in exact_evidence}
                )
                or {item.id for item in exact_findings}
                != set(harness_snapshot["finding_ids"])
            ):
                raise DeliverySubjectChanged(
                    "Frontend review result changed before acceptance"
                )
            if not await self._developer_task_settled_locked(db, task):
                await db.rollback()
                return False
            if not await self._fence_developer_task_graph_locked(db, task):
                await db.rollback()
                return False

            cycle.frontend_review_verdict = verdict
            cycle.frontend_review_summary = (
                harness_snapshot["report"] or harness_snapshot["error"]
            )[:20_000]
            results = list(cycle.frontend_review_results or [])
            results.append(
                {
                    "profile_id": harness_snapshot["preview_profile_id"],
                    "run_id": harness_run_id,
                    "verdict": verdict,
                    "report": cycle.frontend_review_summary,
                    "evidence_count": len(exact_evidence),
                    "finding_count": len(exact_findings),
                }
            )
            cycle.frontend_review_results = results
            cycle.state_version += 1
            cycle.updated_at = _utcnow()
            if verdict == "passed":
                next_profile_index = cycle.frontend_review_profile_index + 1
                if next_profile_index < len(cycle.frontend_review_profile_ids):
                    cycle.frontend_review_profile_index = next_profile_index
                    cycle.frontend_review_run_id = None
                    await apply_run_event(
                        db,
                        run=run,
                        event=DeliveryReducerEvent("frontend_review_profile_passed"),
                        actor_kind="test_harness",
                        actor_id=harness_run_id,
                        metadata={
                            "test_harness_run_id": harness_run_id,
                            "preview_profile_id": harness_snapshot[
                                "preview_profile_id"
                            ],
                            "next_preview_profile_id": (
                                cycle.frontend_review_profile_ids[next_profile_index]
                            ),
                            "evidence_count": len(evidence),
                        },
                    )
                else:
                    cycle.status = "publishing"
                    await apply_run_event(
                        db,
                        run=run,
                        event=DeliveryReducerEvent("frontend_review_passed"),
                        actor_kind="test_harness",
                        actor_id=harness_run_id,
                        metadata={
                            "test_harness_run_id": harness_run_id,
                            "preview_profile_ids": list(
                                cycle.frontend_review_profile_ids
                            ),
                            "evidence_count": sum(
                                int(item.get("evidence_count", 0))
                                for item in results
                                if isinstance(item, dict)
                            ),
                        },
                    )
            elif run.cycle_count >= run.max_cycles:
                failure_message = (
                    "Frontend review found issues after the Delivery cycle "
                    "budget was exhausted"
                )
                complete_cycle(cycle, status="failed")
                await apply_run_event(
                    db,
                    run=run,
                    event=DeliveryReducerEvent(
                        "fail",
                        {
                            "error_code": "delivery_max_cycles",
                            "error_message": failure_message,
                        },
                    ),
                    actor_kind="controller",
                    actor_id=self.owner_id,
                    metadata={"test_harness_run_id": harness_run_id},
                )
                task.status = "failed"
                task.completed_at = _utcnow()
                task.error_message = failure_message
            else:
                finding_payload = [
                    {
                        "fingerprint": item.fingerprint,
                        "scenario_id": item.scenario_id,
                        "severity": item.severity,
                        "category": item.category,
                        "title": item.title,
                        "route": item.route,
                        "locator": item.locator,
                        "expected": item.expected,
                        "actual": item.actual,
                        "reproduction": item.reproduction,
                        "evidence": item.evidence_names,
                    }
                    for item in exact_findings
                ]
                complete_cycle(cycle)
                await apply_run_event(
                    db,
                    run=run,
                    event=DeliveryReducerEvent("frontend_review_changes_requested"),
                    actor_kind="test_harness",
                    actor_id=harness_run_id,
                    metadata={
                        "test_harness_run_id": harness_run_id,
                        "summary": cycle.frontend_review_summary,
                    },
                )
                await start_next_cycle(
                    db,
                    run=run,
                    trigger_kind="frontend_review_changes_requested",
                    trigger_payload={
                        "test_harness_run_id": harness_run_id,
                        "source_git_head": context.head_sha,
                        "report": cycle.frontend_review_summary,
                        "findings": finding_payload,
                    },
                )
            await db.commit()
        return True

    async def _drive_publishing(
        self,
        lease: _Lease,
        context: _RunContext,
    ) -> bool:
        if not all((context.base_sha, context.head_sha, context.head_tree_sha)):
            raise DeliverySubjectChanged("Publishing subject is incomplete")
        try:
            snapshot = await self.workspace.inspect(
                repo_path=context.repo_path,
                worktree_path=context.workspace_path or "",
                branch=context.delivery_branch,
                base_branch=context.base_branch,
                expected_repo_full_name=context.repo_full_name,
            )
        except DeliveryWorkspaceError as exc:
            raise DeliverySubjectChanged(
                f"Delivery workspace validation failed: {exc}"
            ) from exc
        if (
            snapshot.base_sha != context.base_sha
            or snapshot.head_sha != context.head_sha
            or snapshot.head_tree_sha != context.head_tree_sha
        ):
            raise DeliverySubjectChanged("Workspace changed after pre-PR Review")

        action = await self._stage_or_claim_publish_action(lease, context)
        if action is None:
            return False
        token, action_id, idempotency_key, pull_request = action
        fence = DeliveryEffectFence(
            run_id=context.run_id,
            controller_owner=self.owner_id,
            controller_generation=lease.generation,
            action_id=action_id,
            action_token=token,
            expected_base_sha=context.base_sha,
            expected_head_sha=context.head_sha,
        )
        active_fence = (action_id, token)
        existing_fence = self._active_action_fences.get(context.run_id)
        if existing_fence not in (None, active_fence):
            raise DeliveryConflictError(
                "Delivery Run already has another local publication fence"
            )
        self._active_action_fences[context.run_id] = active_fence
        try:
            await self._assert_publish_authority(
                lease,
                context=context,
                action_id=action_id,
                token=token,
            )
            if pull_request is None:
                try:
                    pull_request = await self.publisher.ensure_pull_request(
                        run_id=context.run_id,
                        idempotency_key=idempotency_key,
                        fence=fence,
                    )
                except DeliveryPublisherNoEffectPreflightError as exc:
                    await self._mark_action_failed(
                        lease,
                        action_id,
                        token,
                        exc,
                    )
                    raise
                except DeliveryPublisherPermanentError as exc:
                    # ``permanent`` means a blind retry is unsafe, not that the
                    # remote write did not happen.  Preserve the outbox receipt
                    # slot so the next owner can reconcile branch/PR truth.
                    await self._mark_action_unknown(
                        lease,
                        action_id,
                        token,
                        exc,
                    )
                    return False
                await self._record_pull_request_receipt(
                    lease,
                    context=context,
                    action_id=action_id,
                    token=token,
                    pull_request=pull_request,
                )
            await self._assert_publish_authority(
                lease,
                context=context,
                action_id=action_id,
                token=token,
            )
            try:
                monitor_id = await self.publisher.ensure_monitor(
                    run_id=context.run_id,
                    pull_request=pull_request,
                    idempotency_key=f"{idempotency_key}:monitor",
                    fence=fence,
                )
            except DeliveryPublisherNoEffectPreflightError as exc:
                # The PR receipt is already durable.  A proven no-effect
                # Monitor preflight may therefore fail deterministically
                # without losing the remote PR evidence.
                await self._mark_action_failed(
                    lease,
                    action_id,
                    token,
                    exc,
                )
                raise
            except DeliveryPublisherPermanentError as exc:
                # The remote PR effect is already a fenced durable receipt.
                # A Monitor bind failure must leave that receipt recoverable;
                # the next claim resumes at ensure_monitor and never creates
                # the PR again.
                await self._mark_action_unknown(lease, action_id, token, exc)
                return False
            await self._finalize_publish_action(
                lease,
                context=context,
                action_id=action_id,
                token=token,
                pull_request=pull_request,
                monitor_id=monitor_id,
            )
        except DeliveryPublisherNoEffectPreflightError:
            raise
        except DeliveryPublisherPermanentError as exc:
            await self._mark_action_unknown(lease, action_id, token, exc)
            return False
        except DeliverySubjectChanged as exc:
            await self._mark_action_unknown(lease, action_id, token, exc)
            raise
        except Exception as exc:
            await self._mark_action_unknown(lease, action_id, token, exc)
            return False
        finally:
            if self._active_action_fences.get(context.run_id) == active_fence:
                self._active_action_fences.pop(context.run_id, None)
        return True

    @staticmethod
    def _validate_published_pull_request(
        context: _RunContext,
        pull_request: PublishedPullRequest,
    ) -> None:
        if (
            pull_request.repo_id != context.monitored_repo_id
            or pull_request.base_sha != context.base_sha
            or pull_request.head_sha != context.head_sha
            or pull_request.head_branch != context.delivery_branch
            or not isinstance(pull_request.head_repo_full_name, str)
            or pull_request.head_repo_full_name.lower()
            != context.repo_full_name.lower()
            or not pull_request.url
            or pull_request.pr_number <= 0
        ):
            raise DeliverySubjectChanged("Publisher returned a different PR subject")

    def _pull_request_from_receipt(
        self,
        *,
        action: DeliveryAction,
        run: DeliveryRun,
        context: _RunContext,
    ) -> PublishedPullRequest | None:
        if (
            action.remote_id is None
            and action.remote_url is None
            and action.result is None
        ):
            return None
        result = action.result
        expected_v2_subject = {
            "run_id": run.id,
            "repo_id": run.monitored_repo_id,
            "repo_full_name": context.repo_full_name,
            "base_branch": context.base_branch,
            "delivery_branch": context.delivery_branch,
            "base_sha": context.base_sha,
            "head_sha": context.head_sha,
            "head_tree_sha": context.head_tree_sha,
            "patch_sha256": run.patch_sha256,
        }
        if isinstance(result, dict) and result.get("schema_version") == 2:
            kind = result.get("kind")
            reason = result.get("reason")
            if kind == "pull_request_create_intent":
                if (
                    set(result) != {"schema_version", "kind", "subject"}
                    or result.get("subject") != expected_v2_subject
                    or action.remote_id is not None
                    or action.remote_url is not None
                    or run.pr_number is not None
                    or run.pr_url is not None
                ):
                    raise DeliverySubjectChanged(
                        "Publish action has a malformed PR creation intent"
                    )
                # The external request may already have crossed GitHub.  This is
                # deliberately not an open-PR receipt: the publisher may only
                # reconcile state=all and must never issue create again.
                return None
            if kind not in {
                "pull_request_history_conflict",
                "pull_request_history_ambiguous",
                "pull_request_create_unresolved",
            } or (
                set(result)
                != (
                    {"schema_version", "kind", "subject", "reason", "remote"}
                    if kind == "pull_request_history_conflict"
                    else {"schema_version", "kind", "subject", "reason"}
                )
                or result.get("subject") != expected_v2_subject
                or not isinstance(reason, str)
                or not reason
            ):
                raise DeliverySubjectChanged(
                    "Publish action has a malformed terminal PR conflict receipt"
                )
            if kind in {
                "pull_request_history_ambiguous",
                "pull_request_create_unresolved",
            }:
                if action.remote_id is not None or action.remote_url is not None:
                    raise DeliverySubjectChanged(
                        "Unresolved PR evidence has unexpected remote identity"
                    )
                if kind == "pull_request_history_ambiguous" and (
                    action.status in {"unknown", "leased"}
                    and reason == _LEGACY_BOUND_STALE_HEAD_REASON
                    and not isinstance(run.pr_number, bool)
                    and isinstance(run.pr_number, int)
                    and run.pr_number > 0
                    and isinstance(run.pr_url, str)
                    and bool(run.pr_url)
                ):
                    # Older publishers compared the next repair head against
                    # the still-open PR before advancing its branch.  A bound
                    # Run cannot create a replacement PR, so retaining this
                    # receipt while re-entering publisher reconciliation is
                    # safe.  Success atomically upgrades it to schema v1;
                    # another identity mismatch remains fail-closed.
                    return None
                if run.pr_number is not None or run.pr_url is not None:
                    raise DeliverySubjectChanged(
                        "Unresolved PR evidence has incomplete remote identity"
                    )
            else:
                remote = result.get("remote")
                if (
                    not isinstance(remote, dict)
                    or set(remote)
                    != {
                        "state",
                        "repo_id",
                        "pr_number",
                        "url",
                        "base_sha",
                        "head_sha",
                        "head_branch",
                        "head_repo_full_name",
                    }
                    or remote.get("state") not in {"closed", "merged"}
                    or remote.get("repo_id") != context.monitored_repo_id
                    or isinstance(remote.get("pr_number"), bool)
                    or not isinstance(remote.get("pr_number"), int)
                    or remote.get("pr_number", 0) <= 0
                    or not isinstance(remote.get("url"), str)
                    or not remote.get("url")
                    or remote.get("base_sha") != context.base_sha
                    or remote.get("head_sha") != context.head_sha
                    or remote.get("head_branch") != context.delivery_branch
                    or not isinstance(remote.get("head_repo_full_name"), str)
                    or remote.get("head_repo_full_name", "").lower()
                    != context.repo_full_name.lower()
                    or action.remote_id != str(remote.get("pr_number"))
                    or action.remote_url != remote.get("url")
                    or run.pr_number != remote.get("pr_number")
                    or run.pr_url != remote.get("url")
                ):
                    raise DeliverySubjectChanged(
                        "Historical PR conflict receipt does not match the Run"
                    )
            raise _DeliveryTerminalReceiptError(reason)
        if (
            not isinstance(result, dict)
            or result.get("schema_version") != 1
            or isinstance(result.get("repo_id"), bool)
            or not isinstance(result.get("repo_id"), int)
            or isinstance(result.get("pr_number"), bool)
            or not isinstance(result.get("pr_number"), int)
            or not isinstance(result.get("url"), str)
            or not result.get("url")
            or not isinstance(result.get("base_sha"), str)
            or not isinstance(result.get("head_sha"), str)
            or not isinstance(result.get("head_branch"), str)
            or not isinstance(result.get("head_repo_full_name"), str)
            or result.get("head_repo_full_name", "").lower()
            != context.repo_full_name.lower()
        ):
            raise DeliverySubjectChanged(
                "Publish action has a malformed remote PR receipt"
            )
        pull_request = PublishedPullRequest(
            repo_id=result["repo_id"],
            pr_number=result["pr_number"],
            url=result["url"],
            base_sha=result["base_sha"],
            head_sha=result["head_sha"],
            head_branch=result["head_branch"],
            head_repo_full_name=result.get("head_repo_full_name"),
        )
        self._validate_published_pull_request(context, pull_request)
        if (
            action.remote_id != str(pull_request.pr_number)
            or action.remote_url != pull_request.url
            or run.pr_number != pull_request.pr_number
            or run.pr_url != pull_request.url
        ):
            raise DeliverySubjectChanged(
                "Publish action remote PR receipt no longer matches the Run"
            )
        return pull_request

    async def _assert_publish_authority(
        self,
        lease: _Lease,
        *,
        context: _RunContext,
        action_id: int,
        token: str,
    ) -> None:
        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            action = (
                await db.execute(
                    select(DeliveryAction)
                    .where(DeliveryAction.id == action_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            action = self._assert_action_lease(
                action,
                run=run,
                action_id=action_id,
                token=token,
            )
            expected_key = f"delivery:{run.id}:publish:{run.base_sha}:{run.head_sha}"
            if (
                run.phase != "publishing"
                or run.activity != "running"
                or run.current_cycle_id != context.cycle_id
                or run.monitored_repo_id != context.monitored_repo_id
                or run.base_sha != context.base_sha
                or run.head_sha != context.head_sha
                or run.head_tree_sha != context.head_tree_sha
                or run.delivery_branch != context.delivery_branch
                or action.action_type != "ensure_pull_request"
                or action.idempotency_key != expected_key
                or action.expected_base_sha != run.base_sha
                or action.expected_head_sha != run.head_sha
                or not isinstance(action.payload, dict)
                or action.payload_hash != value_hash(action.payload)
                or action.payload.get("run_id") != run.id
                or action.payload.get("cycle_id") != run.current_cycle_id
                or action.payload.get("repo_id") != run.monitored_repo_id
                or action.payload.get("base_sha") != run.base_sha
                or action.payload.get("head_sha") != run.head_sha
                or action.payload.get("head_tree_sha") != run.head_tree_sha
                or action.payload.get("patch_sha256") != run.patch_sha256
                or action.payload.get("base_branch") != run.base_branch
                or action.payload.get("delivery_branch") != run.delivery_branch
            ):
                raise DeliverySubjectChanged(
                    "Publish action no longer matches its exact subject"
                )
            await db.rollback()

    async def _record_pull_request_receipt(
        self,
        lease: _Lease,
        *,
        context: _RunContext,
        action_id: int,
        token: str,
        pull_request: PublishedPullRequest,
    ) -> None:
        self._validate_published_pull_request(context, pull_request)
        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            action = (
                await db.execute(
                    select(DeliveryAction)
                    .where(DeliveryAction.id == action_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            action = self._assert_action_lease(
                action,
                run=run,
                action_id=action_id,
                token=token,
            )
            if (
                run.phase != "publishing"
                or run.activity != "running"
                or run.current_cycle_id != context.cycle_id
                or run.monitored_repo_id != pull_request.repo_id
                or run.base_sha != pull_request.base_sha
                or run.head_sha != pull_request.head_sha
                or run.head_tree_sha != context.head_tree_sha
                or run.base_branch != context.base_branch
                or run.delivery_branch != pull_request.head_branch
                or action.action_type != "ensure_pull_request"
                or action.idempotency_key
                != f"delivery:{run.id}:publish:{run.base_sha}:{run.head_sha}"
                or action.expected_base_sha != run.base_sha
                or action.expected_head_sha != run.head_sha
                or not isinstance(action.payload, dict)
                or action.payload_hash != value_hash(action.payload)
                or action.payload.get("run_id") != run.id
                or action.payload.get("cycle_id") != run.current_cycle_id
                or action.payload.get("repo_id") != run.monitored_repo_id
                or action.payload.get("base_sha") != run.base_sha
                or action.payload.get("head_sha") != run.head_sha
                or action.payload.get("head_tree_sha") != run.head_tree_sha
                or action.payload.get("patch_sha256") != run.patch_sha256
                or action.payload.get("base_branch") != run.base_branch
                or action.payload.get("delivery_branch") != run.delivery_branch
            ):
                raise DeliverySubjectChanged(
                    "Published PR no longer matches its durable action"
                )
            existing = self._pull_request_from_receipt(
                action=action,
                run=run,
                context=context,
            )
            if existing is not None and existing != pull_request:
                raise DeliverySubjectChanged(
                    "Publish action is already bound to another remote PR"
                )
            if (run.pr_number, run.pr_url) not in (
                (None, None),
                (pull_request.pr_number, pull_request.url),
            ):
                raise DeliverySubjectChanged(
                    "Delivery Run is already bound to another remote PR"
                )
            action.remote_id = str(pull_request.pr_number)
            action.remote_url = pull_request.url
            action.result = {
                "schema_version": 1,
                "repo_id": pull_request.repo_id,
                "pr_number": pull_request.pr_number,
                "url": pull_request.url,
                "base_sha": pull_request.base_sha,
                "head_sha": pull_request.head_sha,
                "head_branch": pull_request.head_branch,
                "head_repo_full_name": pull_request.head_repo_full_name,
            }
            run.pr_number = pull_request.pr_number
            run.pr_url = pull_request.url
            run.updated_at = _utcnow()
            await db.commit()

    async def _stage_or_claim_publish_action(
        self,
        lease: _Lease,
        context: _RunContext,
    ) -> tuple[str, int, str, PublishedPullRequest | None] | None:
        now = _utcnow()
        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            cycle = await lock_current_cycle(db, run)
            if run.phase != "publishing" or cycle.id != context.cycle_id:
                await db.rollback()
                return None
            action = (
                await db.execute(
                    select(DeliveryAction)
                    .where(
                        DeliveryAction.run_id == run.id,
                        DeliveryAction.action_type == "ensure_pull_request",
                        DeliveryAction.expected_base_sha == run.base_sha,
                        DeliveryAction.expected_head_sha == run.head_sha,
                    )
                    .order_by(DeliveryAction.id.desc())
                    .limit(1)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            idempotency_key = f"delivery:{run.id}:publish:{run.base_sha}:{run.head_sha}"
            if action is None:
                payload = {
                    "schema_version": 1,
                    "run_id": run.id,
                    "cycle_id": cycle.id,
                    "repo_id": run.monitored_repo_id,
                    "base_sha": run.base_sha,
                    "head_sha": run.head_sha,
                    "head_tree_sha": run.head_tree_sha,
                    "patch_sha256": run.patch_sha256,
                    "base_branch": run.base_branch,
                    "delivery_branch": run.delivery_branch,
                }
                action = DeliveryAction(
                    run_id=run.id,
                    cycle_id=cycle.id,
                    active_run_id=run.id,
                    action_type="ensure_pull_request",
                    idempotency_key=idempotency_key,
                    desired_version=run.state_version,
                    expected_head_sha=run.head_sha,
                    expected_base_sha=run.base_sha,
                    payload=payload,
                    payload_hash=value_hash(payload),
                    status="pending",
                )
                db.add(action)
                await db.flush()
            if (
                action.cycle_id != cycle.id
                or action.idempotency_key != idempotency_key
                or action.expected_base_sha != run.base_sha
                or action.expected_head_sha != run.head_sha
                or not isinstance(action.payload, dict)
                or action.payload_hash != value_hash(action.payload)
                or action.payload.get("run_id") != run.id
                or action.payload.get("cycle_id") != cycle.id
                or action.payload.get("repo_id") != run.monitored_repo_id
                or action.payload.get("base_sha") != run.base_sha
                or action.payload.get("head_sha") != run.head_sha
                or action.payload.get("head_tree_sha") != run.head_tree_sha
                or action.payload.get("patch_sha256") != run.patch_sha256
                or action.payload.get("base_branch") != run.base_branch
                or action.payload.get("delivery_branch") != run.delivery_branch
            ):
                raise DeliverySubjectChanged(
                    "Publish action no longer matches its exact subject"
                )
            try:
                receipt = self._pull_request_from_receipt(
                    action=action,
                    run=run,
                    context=context,
                )
            except _DeliveryTerminalReceiptError as exc:
                # The publisher committed an exact terminal receipt before it
                # could safely report success.  Consume that receipt under the
                # Run/Action row locks so a terminal Run never strands an
                # active outbox slot or an expired owner token.
                action.status = "failed"
                action.active_run_id = None
                action.lease_owner = None
                action.lease_expires_at = None
                action.next_attempt_at = None
                action.completed_at = now
                action.last_error = str(exc)[:2000]
                await db.commit()
                raise DeliveryPublisherPermanentError(str(exc)) from exc
            if action.status == "failed":
                raise DeliveryPublisherPermanentError(
                    action.last_error or "Delivery publish action failed"
                )
            if action.status == "succeeded":
                raise DeliverySubjectChanged(
                    "Succeeded publish action was not atomically bound to its PR"
                )
            if action.next_attempt_at is not None and action.next_attempt_at > now:
                await db.rollback()
                return None
            if (
                action.status == "leased"
                and action.lease_expires_at is not None
                and action.lease_expires_at > now
            ):
                await db.rollback()
                return None
            token = secrets.token_hex(32)
            action.status = "leased"
            action.lease_owner = token
            action.lease_expires_at = now + timedelta(seconds=self.action_lease_seconds)
            action.attempts += 1
            action.last_error = None
            if run.activity == "ready":
                await apply_run_event(
                    db,
                    run=run,
                    event=DeliveryReducerEvent("publish_started"),
                    actor_kind="controller",
                    actor_id=self.owner_id,
                    metadata={"action_id": action.id},
                )
            elif run.activity != "running":
                raise DeliverySubjectChanged("Publishing action has invalid activity")
            await db.commit()
            return token, action.id, idempotency_key, receipt

    async def _mark_action_unknown(
        self,
        lease: _Lease,
        action_id: int,
        token: str,
        exc: Exception,
    ) -> None:
        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            action = (
                await db.execute(
                    select(DeliveryAction)
                    .where(DeliveryAction.id == action_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            action = self._assert_action_lease(
                action,
                run=run,
                action_id=action_id,
                token=token,
            )
            action.status = "unknown"
            action.lease_owner = None
            action.lease_expires_at = None
            action.last_error = f"{type(exc).__name__}: {exc}"[:2000]
            action.next_attempt_at = _utcnow() + timedelta(
                seconds=self.poll_interval_seconds
            )
            await db.commit()

    async def _mark_action_failed(
        self,
        lease: _Lease,
        action_id: int,
        token: str,
        exc: Exception | None = None,
    ) -> None:
        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            action = (
                await db.execute(
                    select(DeliveryAction)
                    .where(DeliveryAction.id == action_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            action = self._assert_action_lease(
                action,
                run=run,
                action_id=action_id,
                token=token,
            )
            action.status = "failed"
            action.active_run_id = None
            action.lease_owner = None
            action.lease_expires_at = None
            action.completed_at = _utcnow()
            action.last_error = (
                f"{type(exc).__name__}: {exc}"[:2000]
                if exc is not None
                else "Delivery publisher rejected the action"
            )
            await db.commit()

    async def _finalize_publish_action(
        self,
        lease: _Lease,
        *,
        context: _RunContext,
        action_id: int,
        token: str,
        pull_request: PublishedPullRequest,
        monitor_id: int,
    ) -> None:
        self._validate_published_pull_request(context, pull_request)
        if isinstance(monitor_id, bool) or monitor_id <= 0:
            raise DeliverySubjectChanged("Publisher returned a different PR subject")
        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            cycle = await lock_current_cycle(db, run)
            action = (
                await db.execute(
                    select(DeliveryAction)
                    .where(DeliveryAction.id == action_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            action = self._assert_action_lease(
                action,
                run=run,
                action_id=action_id,
                token=token,
            )
            receipt = self._pull_request_from_receipt(
                action=action,
                run=run,
                context=context,
            )
            monitor = (
                await db.execute(
                    select(PRMonitorRun)
                    .where(PRMonitorRun.id == monitor_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if (
                run.phase != "publishing"
                or run.activity != "running"
                or cycle.id != context.cycle_id
                or receipt != pull_request
                or action.expected_base_sha != run.base_sha
                or action.expected_head_sha != run.head_sha
                or monitor is None
                or monitor.repo_id != run.monitored_repo_id
                or monitor.pr_number != pull_request.pr_number
                or monitor.current_base_sha != run.base_sha
                or monitor.current_head_sha != run.head_sha
                or monitor.head_repo_full_name is None
                or pull_request.head_repo_full_name is None
                or monitor.head_repo_full_name.lower()
                != pull_request.head_repo_full_name.lower()
                or monitor.head_branch != pull_request.head_branch
            ):
                raise DeliverySubjectChanged(
                    "Publish result no longer matches its durable action"
                )
            await self._neutralize_legacy_repair_locked(
                db,
                run=run,
                monitor=monitor,
            )
            action.status = "succeeded"
            action.active_run_id = None
            action.lease_owner = None
            action.lease_expires_at = None
            action.remote_id = str(pull_request.pr_number)
            action.remote_url = pull_request.url
            action.result = {
                "schema_version": 1,
                "repo_id": pull_request.repo_id,
                "pr_number": pull_request.pr_number,
                "url": pull_request.url,
                "base_sha": pull_request.base_sha,
                "head_sha": pull_request.head_sha,
                "head_branch": pull_request.head_branch,
                "head_repo_full_name": pull_request.head_repo_full_name,
                "monitor_run_id": monitor.id,
            }
            action.completed_at = _utcnow()
            run.pr_number = pull_request.pr_number
            run.pr_url = pull_request.url
            run.pr_monitor_run_id = monitor.id
            complete_cycle(cycle)
            await apply_run_event(
                db,
                run=run,
                event=DeliveryReducerEvent("pr_bound"),
                actor_kind="controller",
                actor_id=self.owner_id,
                metadata={
                    "action_id": action.id,
                    "monitor_run_id": monitor.id,
                    "pr_number": pull_request.pr_number,
                },
            )
            await db.commit()

    async def _neutralize_legacy_repair_locked(
        self,
        db: AsyncSession,
        *,
        run: DeliveryRun,
        monitor: PRMonitorRun,
    ) -> None:
        wakes = list(
            (
                await db.execute(
                    select(PRRepairWake)
                    .where(
                        PRRepairWake.monitor_run_id == monitor.id,
                        PRRepairWake.trigger_head_sha == monitor.current_head_sha,
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalars()
        )
        active = [wake for wake in wakes if wake.status in _LEGACY_WAKE_ACTIVE]
        if active:
            raise DeliverySubjectChanged(
                "PR Monitor legacy repair already owns the Developer Task"
            )
        changed = False
        for wake in wakes:
            if wake.status == "pending":
                wake.status = "shadow"
                wake.last_error = "delivery_controller_owned"
                wake.developer_task_id = None
                changed = True
        if monitor.developer_task_id is not None:
            if monitor.developer_task_id != run.developer_task_id:
                raise DeliverySubjectChanged(
                    "PR Monitor is bound to another Developer Task"
                )
            monitor.developer_task_id = None
            monitor.binding_verified_at = None
            changed = True
        if monitor.status == "repair_pending":
            monitor.status = "waiting_for_fix"
            monitor.pause_reason = None
            changed = True
        if changed:
            monitor.state_version += 1

    async def _drive_monitoring(
        self,
        lease: _Lease,
        context: _RunContext,
    ) -> bool:
        if context.activity != "waiting":
            raise DeliverySubjectChanged("Monitoring phase has an invalid activity")
        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            if run.pr_monitor_run_id is None:
                raise DeliverySubjectChanged("Delivery Run lost its PR Monitor binding")
            monitor = (
                await db.execute(
                    select(PRMonitorRun)
                    .where(PRMonitorRun.id == run.pr_monitor_run_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if monitor is None:
                raise DeliverySubjectChanged("Bound PR Monitor Run disappeared")
            await self._neutralize_legacy_repair_locked(
                db,
                run=run,
                monitor=monitor,
            )
            await db.commit()

        async with self.db_factory() as db:
            run = await db.get(DeliveryRun, lease.run_id, populate_existing=True)
            monitor = (
                await db.get(
                    PRMonitorRun,
                    run.pr_monitor_run_id,
                    populate_existing=True,
                )
                if run is not None and run.pr_monitor_run_id is not None
                else None
            )
            repo = (
                await db.get(
                    MonitoredRepo, run.monitored_repo_id, populate_existing=True
                )
                if run is not None and run.monitored_repo_id is not None
                else None
            )
            if run is None or monitor is None or repo is None:
                raise DeliverySubjectChanged("Monitoring subject disappeared")
            self._assert_lease(run, lease)
            review = (
                await db.get(
                    PRReview, monitor.current_review_id, populate_existing=True
                )
                if monitor.current_review_id is not None
                else None
            )
            wake = (
                await db.execute(
                    select(PRRepairWake)
                    .where(
                        PRRepairWake.monitor_run_id == monitor.id,
                        PRRepairWake.trigger_base_sha == monitor.current_base_sha,
                        PRRepairWake.trigger_head_sha == monitor.current_head_sha,
                    )
                    .order_by(PRRepairWake.id.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
            monitor_snapshot = {
                "id": monitor.id,
                "repo_id": monitor.repo_id,
                "pr_number": monitor.pr_number,
                "status": monitor.status,
                "base_sha": monitor.current_base_sha,
                "head_sha": monitor.current_head_sha,
                "review_id": monitor.current_review_id,
                "head_branch": monitor.head_branch,
                "head_repo_full_name": monitor.head_repo_full_name,
                "pause_reason": monitor.pause_reason,
                "state_version": monitor.state_version,
            }
            run_binding = {
                "pr_number": run.pr_number,
                "pr_url": run.pr_url,
                "monitor_run_id": run.pr_monitor_run_id,
                "base_sha": run.base_sha,
                "head_sha": run.head_sha,
            }
            repo_full_name = repo.repo_full_name
            evidence = dict(wake.evidence or {}) if wake is not None else {}
            wake_id = wake.id if wake is not None else None
            review_id = review.id if review is not None else None
            review_snapshot = (
                {
                    "id": review.id,
                    "monitor_run_id": review.monitor_run_id,
                    "repo_id": review.repo_id,
                    "pr_number": review.pr_number,
                    "status": review.status,
                    "action_taken": review.action_taken,
                    "base_ref": review.base_ref,
                    "base_sha": review.base_sha,
                    "head_sha": review.head_sha,
                    "delivery_id": review.delivery_id,
                    "pr_url": review.pr_url,
                    "task_id": review.task_id,
                    "action_nonce": review.action_nonce,
                    "publishing_actor": review.publishing_actor,
                    "publishing_started_at": review.publishing_started_at,
                    "merge_method": review.merge_method,
                    "completed_at": review.completed_at,
                    "summary": review.review_summary,
                }
                if review is not None
                else None
            )

        if (
            monitor_snapshot["repo_id"] != context.monitored_repo_id
            or monitor_snapshot["pr_number"] != run_binding["pr_number"]
        ):
            raise DeliverySubjectChanged("PR Monitor binding changed")
        if (
            monitor_snapshot["base_sha"] != context.base_sha
            or monitor_snapshot["head_sha"] != context.head_sha
        ):
            raise DeliverySubjectChanged(
                "PR Monitor advanced to an unowned base/head subject"
            )
        if monitor_snapshot["head_branch"] != context.delivery_branch:
            raise DeliverySubjectChanged("PR head branch changed")
        if (
            not isinstance(monitor_snapshot["head_repo_full_name"], str)
            or monitor_snapshot["head_repo_full_name"].lower() != repo_full_name.lower()
        ):
            raise DeliverySubjectChanged("PR head repository changed")

        status = str(monitor_snapshot["status"])
        if (
            review_snapshot is not None
            and review_snapshot["status"] == "error"
            and review_snapshot["action_taken"] == "error"
        ):
            await self._fail_current_review_error(
                lease,
                monitor_snapshot=monitor_snapshot,
                review_snapshot=review_snapshot,
            )
            return False
        if status == context.terminal:
            if (
                not isinstance(run_binding["pr_number"], int)
                or run_binding["pr_number"] <= 0
                or not isinstance(run_binding["pr_url"], str)
                or not run_binding["pr_url"]
                or run_binding["monitor_run_id"] != monitor_snapshot["id"]
                or not isinstance(context.base_sha, str)
                or not isinstance(context.head_sha, str)
            ):
                raise DeliverySubjectChanged(
                    f"{context.terminal} Delivery PR binding is incomplete"
                )
            if (
                review_snapshot is None
                or review_snapshot["monitor_run_id"] != monitor_snapshot["id"]
                or review_snapshot["repo_id"] != context.monitored_repo_id
                or review_snapshot["pr_number"] != run_binding["pr_number"]
                or review_snapshot["base_ref"] != context.base_branch
                or review_snapshot["base_sha"] != context.base_sha
                or review_snapshot["head_sha"] != context.head_sha
                or review_snapshot["delivery_id"]
                != f"delivery:{context.run_id}:{context.head_sha}"
                or not isinstance(review_snapshot["pr_url"], str)
                or review_snapshot["pr_url"].rstrip("/")
                != run_binding["pr_url"].rstrip("/")
                or review_snapshot["completed_at"] is None
                or (
                    context.auto_merge
                    and (
                        review_snapshot["status"] != "merged"
                        or review_snapshot["action_taken"] != "approved_merged"
                        or not isinstance(review_snapshot["publishing_actor"], str)
                        or not review_snapshot["publishing_actor"]
                        or not isinstance(
                            review_snapshot["publishing_started_at"], datetime
                        )
                        or review_snapshot["merge_method"]
                        not in {"merge", "squash", "fast-forward"}
                    )
                )
                or (
                    not context.auto_merge
                    and (
                        review_snapshot["status"],
                        review_snapshot["action_taken"],
                    )
                    not in {
                        ("approved", "lgtm_comment"),
                        ("commented", "review_comments"),
                    }
                )
            ):
                raise DeliverySubjectChanged(
                    f"{context.terminal} Delivery terminal lacks its exact "
                    "Review evidence"
                )
            expected_pull_request = PublishedPullRequest(
                repo_id=context.monitored_repo_id,
                pr_number=run_binding["pr_number"],
                url=run_binding["pr_url"],
                base_sha=context.base_sha,
                head_sha=context.head_sha,
                head_branch=context.delivery_branch,
                head_repo_full_name=repo_full_name,
            )
            verifier = (
                self.publisher.verify_merged
                if context.auto_merge
                else self.publisher.verify_ready_to_merge
            )
            verified_pull_request = await verifier(
                run_id=context.run_id,
                pull_request=expected_pull_request,
                monitor_run_id=int(monitor_snapshot["id"]),
                expected_monitor_state_version=int(monitor_snapshot["state_version"]),
            )
            if verified_pull_request != expected_pull_request:
                raise DeliverySubjectChanged(
                    "Terminal verifier returned a different PR subject"
                )
            async with self.db_factory() as db:
                run = await lock_run(db, lease.run_id)
                self._assert_lease(run, lease)
                task = (
                    await db.execute(
                        select(Task)
                        .where(Task.id == run.developer_task_id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                monitor = (
                    await db.execute(
                        select(PRMonitorRun)
                        .where(PRMonitorRun.id == run.pr_monitor_run_id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                terminal_review = (
                    (
                        await db.execute(
                            select(PRReview)
                            .where(PRReview.id == monitor.current_review_id)
                            .with_for_update()
                            .execution_options(populate_existing=True)
                        )
                    ).scalar_one_or_none()
                    if monitor is not None and monitor.current_review_id is not None
                    else None
                )
                if (
                    run.phase != "monitoring"
                    or run.activity != "waiting"
                    or not isinstance(run.policy_snapshot, dict)
                    or value_hash(run.policy_snapshot) != run.policy_hash
                    or run.policy_snapshot.get("auto_merge") is not context.auto_merge
                    or run.policy_snapshot.get("terminal") != context.terminal
                    or run.project_id != context.project_id
                    or run.developer_task_id != context.developer_task_id
                    or run.monitored_repo_id != context.monitored_repo_id
                    or run.current_cycle_id != context.cycle_id
                    or run.delivery_branch != context.delivery_branch
                    or run.base_branch != context.base_branch
                    or run.pr_monitor_run_id != monitor_snapshot["id"]
                    or run.pr_number != verified_pull_request.pr_number
                    or run.pr_url != verified_pull_request.url
                    or run.base_sha != verified_pull_request.base_sha
                    or run.head_sha != verified_pull_request.head_sha
                    or monitor is None
                    or monitor.id != monitor_snapshot["id"]
                    or monitor.state_version != monitor_snapshot["state_version"]
                    or monitor.status != context.terminal
                    or monitor.repo_id != verified_pull_request.repo_id
                    or monitor.pr_number != verified_pull_request.pr_number
                    or monitor.current_base_sha != verified_pull_request.base_sha
                    or monitor.current_head_sha != verified_pull_request.head_sha
                    or monitor.head_branch != verified_pull_request.head_branch
                    or monitor.head_repo_full_name is None
                    or verified_pull_request.head_repo_full_name is None
                    or monitor.head_repo_full_name.lower()
                    != verified_pull_request.head_repo_full_name.lower()
                    or task is None
                    or task.status not in _TASK_REUSABLE_STATUSES
                    or (
                        terminal_review is None
                        or review_snapshot is None
                        or terminal_review.id != review_snapshot["id"]
                        or terminal_review.monitor_run_id != monitor.id
                        or terminal_review.repo_id != verified_pull_request.repo_id
                        or terminal_review.pr_number != verified_pull_request.pr_number
                        or terminal_review.base_ref != context.base_branch
                        or terminal_review.base_ref != review_snapshot["base_ref"]
                        or terminal_review.base_sha != verified_pull_request.base_sha
                        or terminal_review.base_sha != review_snapshot["base_sha"]
                        or terminal_review.head_sha != verified_pull_request.head_sha
                        or terminal_review.head_sha != review_snapshot["head_sha"]
                        or terminal_review.delivery_id
                        != f"delivery:{run.id}:{run.head_sha}"
                        or terminal_review.delivery_id != review_snapshot["delivery_id"]
                        or terminal_review.pr_url.rstrip("/")
                        != verified_pull_request.url.rstrip("/")
                        or terminal_review.pr_url != review_snapshot["pr_url"]
                        or terminal_review.status != review_snapshot["status"]
                        or terminal_review.action_taken
                        != review_snapshot["action_taken"]
                        or terminal_review.task_id != review_snapshot["task_id"]
                        or terminal_review.action_nonce
                        != review_snapshot["action_nonce"]
                        or terminal_review.publishing_actor
                        != review_snapshot["publishing_actor"]
                        or terminal_review.publishing_started_at
                        != review_snapshot["publishing_started_at"]
                        or terminal_review.merge_method
                        != review_snapshot["merge_method"]
                        or terminal_review.completed_at
                        != review_snapshot["completed_at"]
                    )
                ):
                    raise DeliverySubjectChanged(
                        f"{context.terminal} subject changed before completion"
                    )
                if not await self._developer_task_settled_locked(db, task):
                    await db.rollback()
                    return False
                if not await self._fence_developer_task_graph_locked(db, task):
                    await db.rollback()
                    return False
                await apply_run_event(
                    db,
                    run=run,
                    event=DeliveryReducerEvent("monitor_ready"),
                    actor_kind="pr_monitor",
                    actor_id=str(monitor.id),
                    metadata={
                        "base_sha": monitor.current_base_sha,
                        "head_sha": monitor.current_head_sha,
                        "monitor_status": monitor.status,
                        "auto_merge": context.auto_merge,
                    },
                )
                task.status = "completed"
                task.completed_at = _utcnow()
                task.error_message = None
                await db.commit()
            return False

        if status in _MONITOR_WAITING_STATUSES:
            return False
        if status in _MONITOR_BLOCKED_STATUSES:
            async with self.db_factory() as db:
                run = await lock_run(db, lease.run_id)
                self._assert_lease(run, lease)
                monitor = (
                    await db.execute(
                        select(PRMonitorRun)
                        .where(PRMonitorRun.id == run.pr_monitor_run_id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                if (
                    monitor is None
                    or monitor.status not in _MONITOR_BLOCKED_STATUSES
                    or monitor.current_base_sha != run.base_sha
                    or monitor.current_head_sha != run.head_sha
                    or monitor.state_version != monitor_snapshot["state_version"]
                ):
                    await db.rollback()
                    return True
                if run.cycle_count >= run.max_cycles:
                    failure_message = (
                        "PR Monitor requested another repair after the "
                        "Delivery cycle budget was exhausted"
                    )
                    task = (
                        await db.execute(
                            select(Task)
                            .where(Task.id == run.developer_task_id)
                            .with_for_update()
                            .execution_options(populate_existing=True)
                        )
                    ).scalar_one_or_none()
                    if task is None or task.status not in _TASK_REUSABLE_STATUSES:
                        raise DeliverySubjectChanged(
                            "Developer Task changed before Monitor budget failure"
                        )
                    if not await self._developer_task_settled_locked(db, task):
                        await db.rollback()
                        return False
                    if not await self._fence_developer_task_graph_locked(
                        db,
                        task,
                    ):
                        await db.rollback()
                        return False
                    await apply_run_event(
                        db,
                        run=run,
                        event=DeliveryReducerEvent(
                            "fail",
                            {
                                "error_code": "delivery_max_cycles",
                                "error_message": failure_message,
                            },
                        ),
                        actor_kind="controller",
                        actor_id=self.owner_id,
                    )
                    task.status = "failed"
                    task.completed_at = _utcnow()
                    task.error_message = failure_message
                else:
                    await apply_run_event(
                        db,
                        run=run,
                        event=DeliveryReducerEvent("monitor_blocked"),
                        actor_kind="pr_monitor",
                        actor_id=str(monitor.id),
                        metadata={"review_id": review_id, "wake_id": wake_id},
                    )
                    await start_next_cycle(
                        db,
                        run=run,
                        trigger_kind="pr_monitor_blocked",
                        trigger_payload={
                            "monitor_run_id": monitor.id,
                            "monitor_status": monitor.status,
                            "review_id": review_id,
                            "repair_wake_id": wake_id,
                            "evidence": evidence,
                            "base_sha": monitor.current_base_sha,
                            "head_sha": monitor.current_head_sha,
                        },
                        trigger_pr_review_id=review_id,
                        trigger_pr_repair_wake_id=wake_id,
                    )
                await db.commit()
            return True
        if status == "paused":
            await self._pause_run(
                lease,
                reason=(
                    str(monitor_snapshot["pause_reason"])
                    if monitor_snapshot["pause_reason"]
                    else "PR Monitor paused"
                ),
                code="pr_monitor_paused",
            )
            return False
        if status in {"repairing", "repair_migrating"}:
            raise DeliverySubjectChanged(
                "Legacy PR repair became active for a Delivery-owned PR"
            )
        if status in {"merged", "closed"}:
            await self._fail_run(
                lease,
                code="pr_terminal_before_ready",
                message=f"PR became {status} before exact ready_to_merge",
            )
            return False
        raise DeliverySubjectChanged(f"Unknown PR Monitor status {status!r}")

    async def _fail_current_review_error(
        self,
        lease: _Lease,
        *,
        monitor_snapshot: dict[str, Any],
        review_snapshot: dict[str, Any],
    ) -> None:
        """Terminalize only the exact Delivery/Monitor/Review error subject."""

        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            monitor = (
                await db.execute(
                    select(PRMonitorRun)
                    .where(PRMonitorRun.id == monitor_snapshot["id"])
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            review = (
                await db.execute(
                    select(PRReview)
                    .where(PRReview.id == review_snapshot["id"])
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if (
                run.phase != "monitoring"
                or run.activity != "waiting"
                or run.pr_monitor_run_id != monitor_snapshot["id"]
                or run.base_sha != monitor_snapshot["base_sha"]
                or run.head_sha != monitor_snapshot["head_sha"]
                or review is None
                or review.id != monitor_snapshot["review_id"]
                or review.monitor_run_id != monitor_snapshot["id"]
                or review.status != "error"
                or review.action_taken != "error"
                or review.base_ref != run.base_branch
                or review.base_sha != monitor_snapshot["base_sha"]
                or review.head_sha != monitor_snapshot["head_sha"]
                or review.review_summary != review_snapshot["summary"]
                or monitor is None
                or monitor.id != run.pr_monitor_run_id
                or monitor.state_version != monitor_snapshot["state_version"]
                or monitor.status != monitor_snapshot["status"]
                or monitor.current_review_id != review.id
                or monitor.current_base_sha != run.base_sha
                or monitor.current_head_sha != run.head_sha
            ):
                await db.rollback()
                return
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == run.developer_task_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if task is None or task.status not in _TASK_REUSABLE_STATUSES:
                await db.rollback()
                return
            if not await self._developer_task_settled_locked(db, task):
                await db.rollback()
                return
            if not await self._fence_developer_task_graph_locked(db, task):
                await db.rollback()
                return
            message = (review.review_summary or "PR reviewer failed without a summary")[
                :2000
            ]
            if run.current_cycle_id is not None:
                cycle = await db.get(
                    DeliveryCycle,
                    run.current_cycle_id,
                    populate_existing=True,
                )
                if cycle is not None and cycle.status in DELIVERY_CYCLE_ACTIVE_STATUSES:
                    complete_cycle(cycle, status="failed")
            await apply_run_event(
                db,
                run=run,
                event=DeliveryReducerEvent(
                    "fail",
                    {
                        "error_code": "pr_review_error",
                        "error_message": message,
                    },
                ),
                actor_kind="pr_monitor",
                actor_id=str(monitor.id),
                metadata={"review_id": review.id},
            )
            task.status = "failed"
            task.completed_at = _utcnow()
            task.error_message = message
            await db.commit()

    async def _pause_run(
        self,
        lease: _Lease,
        *,
        reason: str,
        code: str,
    ) -> None:
        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            if run.activity in {"paused", "terminal"}:
                await db.rollback()
                return
            await apply_run_event(
                db,
                run=run,
                event=DeliveryReducerEvent("pause", {"reason": reason[:2000]}),
                actor_kind="controller",
                actor_id=self.owner_id,
                metadata={"error_code": code},
            )
            await db.commit()

    async def _fail_run(
        self,
        lease: _Lease,
        *,
        code: str,
        message: str,
    ) -> bool:
        async with self.db_factory() as db:
            run = await lock_run(db, lease.run_id)
            self._assert_lease(run, lease)
            if run.activity == "terminal":
                await db.rollback()
                return True
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == run.developer_task_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()
            if task is None or task.status not in _TASK_REUSABLE_STATUSES:
                await db.rollback()
                return False
            if not await self._developer_task_settled_locked(db, task):
                await db.rollback()
                return False
            if not await self._fence_developer_task_graph_locked(db, task):
                await db.rollback()
                return False
            if run.current_cycle_id is not None:
                cycle = await db.get(
                    DeliveryCycle,
                    run.current_cycle_id,
                    populate_existing=True,
                )
                if cycle is not None and cycle.status in DELIVERY_CYCLE_ACTIVE_STATUSES:
                    complete_cycle(cycle, status="failed")
            await apply_run_event(
                db,
                run=run,
                event=DeliveryReducerEvent(
                    "fail",
                    {
                        "error_code": (code or "delivery_failed")[:64],
                        "error_message": message[:2000],
                    },
                ),
                actor_kind="controller",
                actor_id=self.owner_id,
            )
            task.status = "failed"
            task.completed_at = _utcnow()
            task.error_message = message[:2000]
            await db.commit()
            return True


__all__ = [
    "CapabilitySnapshot",
    "CoreDeliveryCapabilityGateway",
    "DeliveryCapabilityGateway",
    "DeliveryController",
    "DeliveryControllerError",
    "DeliveryPublisher",
    "DeliveryPublisherNoEffectPreflightError",
    "DeliveryPublisherPermanentError",
    "DeliveryPublisherUnavailable",
    "DeliverySubjectChanged",
    "PublishedPullRequest",
    "UnavailableDeliveryPublisher",
]
