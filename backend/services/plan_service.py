"""Transactional aggregate operations for first-class versioned Plans."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
import json
from typing import Iterable

from fastapi import HTTPException
from sqlalchemy import and_, delete, exists, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.plan import (
    Plan,
    PlanApplication,
    PlanApplicationAttempt,
    PlanApplicationReceipt,
    PlanInputRequest,
    PlanLegacyTaskLink,
    PlanVersion,
)
from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentStep,
    PlanAgentWorkerDispatchReceipt,
)
from backend.models.delivery import DeliveryCycle
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.models.worker import Worker
from backend.services.cancellation import settle_awaitable
from backend.services.task_creation import stage_task_record
from backend.services.plan_tasks import MAX_ACTIVE_PLANS_PER_TASK
from backend.services.worker_task_termination import (
    active_worker_task_termination_receipt,
    no_active_worker_task_termination_predicate,
)
from backend.services.worker_node_control import fence_worker_node_mutation
from backend.schemas.plan_resource import (
    PlanApplicationAttemptResource,
    PlanApplicationResource,
    PlanInputAnswer,
    PlanInputRequestResponse,
    PlanQuestion,
    PlanResource,
    PlanRunResource,
    PlanStepResource,
    PlanVersionResource,
)


ACTIVE_RUN_STATUSES = frozenset({"queued", "running", "waiting_user"})
TERMINAL_RUN_STATUSES = frozenset({"completed", "failed", "cancelled"})
LOCAL_CANCELLATION_FENCE_ATTEMPTS = 3
ACTIVE_PLAN_DELIVERY_STATUSES = frozenset(
    {"pending", "queued", "launching", "uncertain"}
)
TERMINAL_PLAN_DELIVERY_STATUSES = frozenset(
    {"launched", "failed", "cancelled"}
)
# CapabilityExecution.handle_generation identifies the immutable staged Plan
# handle. Provider/dispatcher turn generations live on PlanAgentRun instead.
PLAN_RUN_HANDLE_GENERATION = 0
_plan_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
_target_plan_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
_held_target_plan_locks: ContextVar[tuple[object | None, frozenset[int]]] = ContextVar(
    "held_target_plan_locks",
    default=(None, frozenset()),
)
LockedPlanAuthorizationCallback = Callable[[AsyncSession, Plan], Awaitable[None]]
LockedPlanCreationAuthorizationCallback = Callable[
    [AsyncSession],
    Awaitable[None],
]
PlanEffectBoundaryAuthorizationCallback = Callable[
    [AsyncSession],
    Awaitable[None],
]


CAPABILITY_OWNED_PLAN_MUTATION_DETAIL = {
    "code": "capability_owned_plan_read_only",
    "message": "Capability-owned Plans can only be mutated by Capability Core",
}


def _capability_plan_owner_exists():
    """Return the durable aggregate-ownership predicate shared by writers."""

    capability_marker = or_(
        PlanAgentRun.run_type == "capability",
        PlanAgentRun.capability_execution_id.is_not(None),
    )
    return exists(
        select(PlanAgentRun.id).where(
            capability_marker,
            or_(
                PlanAgentRun.plan_id == Plan.id,
                and_(
                    Plan.active_run_id.is_not(None),
                    PlanAgentRun.id == Plan.active_run_id,
                ),
            ),
        )
    )


async def reject_capability_owned_plan_mutation(
    db: AsyncSession,
    *,
    plan_ids: Iterable[int],
) -> None:
    """Keep generic Plan writers away from Capability-owned aggregates.

    Ownership is durable once *any* Run carries a Capability marker.  Querying
    it with one correlated ``EXISTS`` keeps historical Runs authoritative and
    cannot be bypassed by list pagination.  The active-Run branch is a
    fail-closed integrity fence for a malformed reverse association whose Run
    no longer points back at the Plan.
    """

    normalized_ids = tuple(sorted({int(plan_id) for plan_id in plan_ids}))
    if not normalized_ids:
        return
    owned_plan_id = await db.scalar(
        select(Plan.id)
        .where(Plan.id.in_(normalized_ids), _capability_plan_owner_exists())
        .limit(1)
    )
    if owned_plan_id is not None:
        raise HTTPException(
            409,
            dict(CAPABILITY_OWNED_PLAN_MUTATION_DETAIL),
        )


@dataclass(frozen=True)
class PlanExecutionTaskResult:
    """Exact, idempotent result of applying a Plan Version as a new Task."""

    plan: Plan
    version: PlanVersion
    application: PlanApplication
    task: Task
    created: bool


def plan_operation_lock(plan_id: int) -> asyncio.Lock:
    return _plan_locks[plan_id]


async def _remove_receipt_applications(
    db: AsyncSession,
    receipt: PlanApplicationReceipt,
    *,
    delivery_status: str,
    error: str,
) -> list[int]:
    applications = list(
        (
            await db.execute(
                select(PlanApplication).where(
                    PlanApplication.application_receipt_key == receipt.receipt_key
                )
            )
        ).scalars()
    )
    plan_ids = list(dict.fromkeys(item.plan_id for item in applications))
    existing_attempt_versions = set(
        (
            await db.execute(
                select(PlanApplicationAttempt.plan_version_id).where(
                    PlanApplicationAttempt.application_receipt_key
                    == receipt.receipt_key
                )
            )
        ).scalars()
    )
    released_at = datetime.utcnow()
    for application in applications:
        if application.plan_version_id in existing_attempt_versions:
            continue
        db.add(
            PlanApplicationAttempt(
                plan_id=application.plan_id,
                plan_version_id=application.plan_version_id,
                application_receipt_key=receipt.receipt_key,
                application_type=application.application_type,
                target_task_id=application.target_task_id,
                target_session_id=application.target_session_id,
                user_log_id=application.user_log_id,
                execution_task_id=application.execution_task_id,
                applied_by=application.applied_by,
                application_created_at=application.created_at,
                released_at=released_at,
            )
        )
    # Persist the immutable attempt before deleting the active application.
    # The receipt row lock serializes resolution; the unique key remains the
    # final cross-process idempotency fence.
    await db.flush()
    await db.execute(
        delete(PlanApplication).where(
            PlanApplication.application_receipt_key == receipt.receipt_key
        )
    )
    log = await db.get(LogEntry, receipt.manager_user_log_id)
    if log is not None:
        try:
            metadata = json.loads(log.raw_json or "{}")
        except (TypeError, json.JSONDecodeError):
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        metadata.pop("applied_plans", None)
        metadata["plan_delivery"] = {
            "status": delivery_status,
            "error": error[:2000],
        }
        log.raw_json = json.dumps(metadata)
    return plan_ids


async def release_unstarted_plan_application(
    db: AsyncSession,
    *,
    receipt_key: str,
    delivery_status: str,
    error: str,
    expected_worker_id: int | None = None,
) -> tuple[list[int], int | None] | None:
    """Release a Version application only after prelaunch is proven.

    The caller owns that proof. ``launching``/``uncertain`` receipts must never
    enter here because an external turn may already exist.
    """

    query = (
        select(PlanApplicationReceipt)
        .where(
            PlanApplicationReceipt.receipt_key == receipt_key,
            PlanApplicationReceipt.delivery_status.in_(["pending", "queued"]),
        )
        .with_for_update()
    )
    if expected_worker_id is not None:
        query = query.where(PlanApplicationReceipt.worker_id == expected_worker_id)
    receipt = (await db.execute(query)).scalar_one_or_none()
    if receipt is None:
        return None

    plan_ids = await _remove_receipt_applications(
        db,
        receipt,
        delivery_status=delivery_status,
        error=error,
    )
    receipt.delivery_status = delivery_status
    receipt.delivery_error = error[:2000]
    receipt.updated_at = datetime.utcnow()
    return plan_ids, receipt.target_task_id


async def release_unstarted_plan_applications_for_task(
    db: AsyncSession,
    *,
    target_task_id: int,
    delivery_status: str,
    error: str,
) -> list[tuple[str, list[int], int | None]]:
    """Cancel every durable local outbox row not yet past queue admission."""

    keys = list(
        (
            await db.execute(
                select(PlanApplicationReceipt.receipt_key)
                .where(
                    PlanApplicationReceipt.target_task_id == target_task_id,
                    PlanApplicationReceipt.outbox_payload.isnot(None),
                    PlanApplicationReceipt.delivery_status.in_(["pending", "queued"]),
                )
                .with_for_update()
            )
        ).scalars()
    )
    released: list[tuple[str, list[int], int | None]] = []
    for receipt_key in keys:
        result = await release_unstarted_plan_application(
            db,
            receipt_key=receipt_key,
            delivery_status=delivery_status,
            error=error,
        )
        if result is not None:
            plan_ids, task_id = result
            released.append((receipt_key, plan_ids, task_id))
    return released


async def preserve_uncertain_plan_application(
    db: AsyncSession,
    *,
    receipt: PlanApplicationReceipt,
    error: str,
    launch_evidence: dict | None,
    response: dict | None = None,
    applied_by: int | None = None,
) -> list[int]:
    """Conservatively consume every Version while a Worker launch is ambiguous."""

    version_ids = list(dict.fromkeys(receipt.plan_version_ids or []))
    versions = list(
        (
            await db.execute(select(PlanVersion).where(PlanVersion.id.in_(version_ids)))
        ).scalars()
    )
    versions_by_id = {version.id: version for version in versions}
    if set(versions_by_id) != set(version_ids):
        raise HTTPException(
            409,
            "Plan delivery receipt references a missing Version",
        )
    plans = list(
        (
            await db.execute(
                select(Plan).where(
                    Plan.id.in_({version.plan_id for version in versions})
                )
            )
        ).scalars()
    )
    plans_by_id = {plan.id: plan for plan in plans}
    target = await db.get(Task, receipt.target_task_id)
    existing = {
        application.plan_version_id: application
        for application in (
            await db.execute(
                select(PlanApplication).where(
                    PlanApplication.plan_version_id.in_(version_ids)
                )
            )
        ).scalars()
    }
    approved: list[tuple[Plan, PlanVersion]] = []
    for version_id in version_ids:
        version = versions_by_id[version_id]
        plan = plans_by_id.get(version.plan_id)
        if plan is None:
            raise HTTPException(409, "Plan delivery receipt lost its Plan")
        approved.append((plan, version))
        application = existing.get(version_id)
        if application is not None:
            if application.application_receipt_key != receipt.receipt_key:
                raise HTTPException(
                    409,
                    "Plan Version has a different application receipt",
                )
            continue
        db.add(
            PlanApplication(
                plan_id=plan.id,
                plan_version_id=version.id,
                application_type="chat_message",
                target_task_id=receipt.target_task_id,
                target_session_id=target.session_id if target is not None else None,
                user_log_id=receipt.manager_user_log_id,
                applied_by=applied_by,
                application_receipt_key=receipt.receipt_key,
            )
        )

    log = await db.get(LogEntry, receipt.manager_user_log_id)
    if log is not None:
        try:
            metadata = json.loads(log.raw_json or "{}")
        except (TypeError, json.JSONDecodeError):
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        metadata["applied_plans"] = versioned_plan_snapshots(approved)
        metadata["plan_delivery"] = {
            "status": "uncertain",
            "error": error[:2000],
        }
        log.raw_json = json.dumps(metadata)

    receipt.status = "committed" if response is not None else receipt.status
    if response is not None:
        receipt.response = response
    receipt.delivery_status = "uncertain"
    receipt.delivery_error = error[:2000]
    if isinstance(launch_evidence, dict):
        receipt.launch_evidence = launch_evidence
    receipt.updated_at = datetime.utcnow()
    await db.flush()
    return list(dict.fromkeys(plan.id for plan, _version in approved))


async def resolve_uncertain_plan_application(
    db: AsyncSession,
    *,
    receipt_key: str,
    action: str,
    note: str,
    actor_id: int | None,
) -> tuple[list[int], int | None]:
    """Resolve one ambiguous launch after an administrator checks evidence."""

    if action not in {"confirm_launched", "release_for_retry"}:
        raise HTTPException(422, "Unknown Plan delivery resolution action")
    receipt = (
        await db.execute(
            select(PlanApplicationReceipt)
            .where(PlanApplicationReceipt.receipt_key == receipt_key)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if receipt is None:
        raise HTTPException(404, "Plan application receipt not found")
    receipt_plan_ids = list(
        dict.fromkeys(
            (
                await db.execute(
                    select(PlanVersion.plan_id).where(
                        or_(
                            PlanVersion.id.in_(receipt.plan_version_ids or []),
                            PlanVersion.id.in_(
                                select(PlanApplication.plan_version_id).where(
                                    PlanApplication.application_receipt_key
                                    == receipt.receipt_key
                                )
                            ),
                        )
                    )
                )
            ).scalars()
        )
    )
    await reject_capability_owned_plan_mutation(
        db,
        plan_ids=receipt_plan_ids,
    )
    prior_resolution = receipt.delivery_resolution
    if isinstance(prior_resolution, dict) and prior_resolution.get("action") == action:
        if actor_id is not None and prior_resolution.get("resolved_by") is None:
            enriched_resolution = dict(prior_resolution)
            enriched_resolution["resolved_by"] = actor_id
            enriched_resolution["note"] = note[:2000]
            enriched_resolution["manager_confirmed_at"] = datetime.utcnow().isoformat()
            receipt.delivery_resolution = enriched_resolution
            receipt.updated_at = datetime.utcnow()
        return receipt_plan_ids, receipt.target_task_id
    if receipt.delivery_status != "uncertain":
        raise HTTPException(
            409,
            f"Plan delivery is {receipt.delivery_status}, not uncertain",
        )

    now = datetime.utcnow()
    resolution = {
        "action": action,
        "note": note[:2000],
        "resolved_by": actor_id,
        "resolved_at": now.isoformat(),
        "previous_status": "uncertain",
    }
    if action == "confirm_launched":
        plan_ids = receipt_plan_ids
        receipt.delivery_status = "launched"
        receipt.delivery_error = None
    else:
        plan_ids = await _remove_receipt_applications(
            db,
            receipt,
            delivery_status="cancelled",
            error=(
                "Administrator confirmed that the ambiguous delivery did not launch: "
                f"{note}"
            ),
        )
        if not plan_ids:
            plan_ids = receipt_plan_ids
        receipt.delivery_status = "cancelled"
        receipt.delivery_error = (
            "Administrator confirmed that the ambiguous delivery did not launch"
        )
    receipt.delivery_resolution = resolution
    receipt.updated_at = now
    return plan_ids, receipt.target_task_id


@asynccontextmanager
async def related_plan_creation_lock(target_task_id: int | None):
    """Keep one target's Plan admission serialized, with reentrant nesting."""

    if target_task_id is None:
        yield
        return
    current_task = asyncio.current_task()
    owner_task, held = _held_target_plan_locks.get()
    if owner_task is current_task and target_task_id in held:
        yield
        return
    async with _target_plan_locks[target_task_id]:
        nested = held if owner_task is current_task else frozenset()
        token = _held_target_plan_locks.set(
            (current_task, nested | {target_task_id})
        )
        try:
            yield
        finally:
            _held_target_plan_locks.reset(token)


def serialize_related_plan_creation(function):
    """Keep target reads, COUNT, INSERT, and commit serialized in-process."""

    @wraps(function)
    async def wrapped(*args, **kwargs):
        target_task_id = kwargs.get("target_task_id")
        body = kwargs.get("body")
        if target_task_id is None and body is not None:
            target_task_id = getattr(body, "target_task_id", None)
        async with related_plan_creation_lock(target_task_id):
            return await function(*args, **kwargs)

    return wrapped


async def _end_plan_routing_read(db: AsyncSession) -> None:
    """Start the Task admission fence from a fresh writer transaction.

    Plan APIs perform authorization and context reads before entering this
    service.  Upgrading that SQLite WAL snapshot after another connection
    admits a termination receipt raises ``BUSY_SNAPSHOT`` instead of letting
    the Task predicate choose a winner.  A clean commit preserves the loaded
    ORM inputs (sessions use ``expire_on_commit=False``) while ending the old
    read snapshot.  Staged caller mutations are rejected rather than being
    published early by this routing boundary.
    """

    if db.new or db.dirty or db.deleted:
        raise HTTPException(
            409,
            "Plan admission requires a clean database transaction",
        )
    await db.commit()


async def fence_plan_target_task(
    db: AsyncSession,
    *,
    target_task_id: int | None,
    expected_worker_id: int | None,
) -> None:
    """Serialize a new active Run against an exact Task migration claim."""

    if target_task_id is None:
        return
    worker_predicate = (
        Task.worker_id.is_(None)
        if expected_worker_id is None
        else Task.worker_id == expected_worker_id
    )
    fenced = await db.execute(
        update(Task)
        .where(
            Task.id == target_task_id,
            Task.status != "migrating",
            worker_predicate,
            no_active_worker_task_termination_predicate(),
        )
        # A matched-row UPDATE takes the same database write lock used by the
        # migration claim without changing user-visible Task state.
        .values(status=Task.status)
    )
    if fenced.rowcount != 1:
        receipt = await active_worker_task_termination_receipt(
            db,
            target_task_id,
        )
        await db.rollback()
        if receipt is not None:
            raise HTTPException(
                409,
                "Plan target has an active Worker termination receipt",
            )
        raise HTTPException(409, "Plan target is changing execution location")
    from backend.models.test_harness import TestHarnessChildBinding
    from backend.services.test_harness_children import (
        browser_child_public_mutation_error,
    )

    browser_parent = await db.scalar(
        select(TestHarnessChildBinding.id).where(
            TestHarnessChildBinding.child_task_id == target_task_id
        )
    )
    target = await db.get(Task, target_task_id, populate_existing=True)
    browser_error = (
        browser_child_public_mutation_error(
            target,
            has_binding=browser_parent is not None,
        )
        if target is not None
        else None
    )
    if browser_error is not None:
        await db.rollback()
        raise HTTPException(
            409,
            "Isolated Browser Agent Tasks cannot own first-class Plans",
        )


async def fence_worker_plan_application_receipt(
    db: AsyncSession,
    *,
    receipt_key: str,
    target_task_id: int,
    expected_worker_id: int,
) -> PlanApplicationReceipt | None:
    """Lock one Worker delivery receipt in the Task deletion lock order.

    Worker delivery events can arrive in a different Manager process long
    after the originating HTTP request committed its prepared receipt.  End
    any earlier read snapshot, then acquire the target Task writer fence before
    touching application audit rows.  Task deletion owns the same fence and
    locks active applications/attempts before receipts, so preserving or
    resolving an ambiguous delivery cannot recreate a dangling audit or form a
    Receipt -> Application deadlock.

    ``None`` means the exact receipt disappeared or no longer belongs to the
    fenced Task/Worker generation.  A missing, migrating, terminating, or
    reassigned Task raises the same fail-closed ``409`` as Plan admission.
    """

    await _end_plan_routing_read(db)
    await fence_plan_target_task(
        db,
        target_task_id=target_task_id,
        expected_worker_id=expected_worker_id,
    )
    application_scope = or_(
        PlanApplication.target_task_id == target_task_id,
        PlanApplication.application_receipt_key == receipt_key,
    )
    await db.execute(
        select(PlanApplication)
        .where(application_scope)
        .order_by(PlanApplication.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    attempt_scope = or_(
        PlanApplicationAttempt.target_task_id == target_task_id,
        PlanApplicationAttempt.application_receipt_key == receipt_key,
    )
    await db.execute(
        select(PlanApplicationAttempt)
        .where(attempt_scope)
        .order_by(PlanApplicationAttempt.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return (
        await db.execute(
            select(PlanApplicationReceipt)
            .where(
                PlanApplicationReceipt.receipt_key == receipt_key,
                PlanApplicationReceipt.target_task_id == target_task_id,
                PlanApplicationReceipt.worker_id == expected_worker_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def fence_plan_worker(
    db: AsyncSession,
    *,
    worker_id: int | None,
) -> None:
    """Serialize a Plan/Run assignment against Worker destruction.

    Worker destroy takes Task locks before the Worker row.  Plan creation uses
    the same Task -> Worker order and keeps the Worker write fence through its
    Plan/Run commit.  Once destroy commits ``destroying``, no later writer can
    leave a fresh durable pointer to the machine being terminated.
    """

    from backend.services.worker_assignment import (
        WorkerAssignmentConflict,
        fence_ready_worker_assignment,
    )

    try:
        await fence_ready_worker_assignment(db, worker_id)
    except WorkerAssignmentConflict as exc:
        await db.rollback()
        raise HTTPException(
            409,
            "Plan Worker is unavailable or is changing lifecycle state: "
            f"{exc.detail}",
        ) from exc


def _public_attachments(items: list[dict] | None) -> list[dict] | None:
    if not items:
        return None
    return [
        {key: item[key] for key in ("url", "name", "is_image") if key in item}
        for item in items
        if isinstance(item, dict)
    ] or None


def input_request_resource(
    input_request: PlanInputRequest,
) -> PlanInputRequestResponse:
    return PlanInputRequestResponse.model_validate(input_request).model_copy(
        update={"attachments": _public_attachments(input_request.attachments)}
    )


async def stage_plan_with_run(
    db: AsyncSession,
    *,
    title: str,
    initial_request: str,
    attachments: list[dict] | None,
    target_task_id: int | None,
    project_id: int | None,
    target_repo: str | None,
    target_branch: str | None,
    worker_id: int | None,
    priority: int,
    timeout_hours: float | None,
    created_by: int | None,
    pipeline_config: dict,
    context_session_id: str | None,
    context_log_id: int | None,
    context_snapshot: str | None,
    repo_revision: dict | None,
    forked_from_version_id: int | None = None,
    base_version_id: int | None = None,
    run_type: str = "initial",
    capability_execution_id: int | None = None,
    authorize_effect_boundary: (
        PlanEffectBoundaryAuthorizationCallback | None
    ) = None,
    authorize_locked_creation: (
        LockedPlanCreationAuthorizationCallback | None
    ) = None,
) -> tuple[Plan, PlanAgentRun]:
    """Stage a new Plan and its first Run without ending the transaction.

    This is the composable counterpart to :func:`create_plan_with_run`.  A
    caller may add its own durable ownership record after this function
    returns and commit all three records atomically.  The caller owns the
    eventual commit or rollback; this helper only flushes so the generated
    Plan and Run ids are available.
    """

    if authorize_effect_boundary is not None:
        # Public adapters establish Worker-node -> Project -> Task -> Worker ->
        # group -> User authority here.  This service re-enters the already
        # locked Task/Worker rows below; the callback keeps every authority row
        # locked through the Plan/Run commit.
        try:
            await authorize_effect_boundary(db)
        except BaseException:
            operation, _ = await settle_awaitable(db.rollback())
            operation.result()
            raise
    else:
        # On a Worker the node-control row is the outermost producer fence.
        # Keep it locked through the Plan/Run flush and the caller's eventual
        # commit so a destroy proof can neither miss an in-flight Run nor be
        # followed by a newly committed one. Manager databases deliberately
        # treat this as a no-op.
        await fence_worker_node_mutation(db)
    now = datetime.utcnow()
    await fence_plan_target_task(
        db,
        target_task_id=target_task_id,
        expected_worker_id=worker_id,
    )
    await fence_plan_worker(db, worker_id=worker_id)
    if authorize_locked_creation is not None:
        try:
            # Authorization performed by the HTTP adapter before context
            # capture belongs to the read transaction which
            # ``_end_plan_routing_read`` deliberately ended.  Re-run it only
            # after the Task/Worker writer fences are held, so a revoked ACL
            # can never publish a Plan/Run pair from stale routing data.
            await authorize_locked_creation(db)
        except BaseException:
            operation, _ = await settle_awaitable(db.rollback())
            operation.result()
            raise
    if target_task_id is not None:
        # The target Task write fence above serializes this COUNT -> INSERT
        # boundary across processes and all supported databases. Both ordinary
        # creation and Fork enter through this service boundary.
        active_count = int(
            await db.scalar(
                select(func.count(Plan.id)).where(
                    Plan.target_task_id == target_task_id,
                    Plan.archived_at.is_(None),
                    Plan.active_run_id.isnot(None),
                )
            )
            or 0
        )
        if active_count >= MAX_ACTIVE_PLANS_PER_TASK:
            await db.rollback()
            raise HTTPException(
                429,
                f"Task already has {MAX_ACTIVE_PLANS_PER_TASK} active Plans",
            )
    plan = Plan(
        title=title[:200],
        initial_request=initial_request,
        initial_attachments=attachments or None,
        target_task_id=target_task_id,
        project_id=project_id,
        target_repo=target_repo,
        target_branch=target_branch,
        worker_id=worker_id,
        priority=priority,
        timeout_hours=timeout_hours,
        created_by=created_by,
        pipeline_config=pipeline_config,
        forked_from_version_id=forked_from_version_id,
        created_at=now,
        updated_at=now,
    )
    db.add(plan)
    await db.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        plan_task_id=None,
        capability_execution_id=capability_execution_id,
        run_type=run_type,
        status="queued",
        current_stage="planner",
        base_version_id=base_version_id,
        request_text=initial_request,
        attachments=attachments or None,
        context_session_id=context_session_id,
        context_log_id=context_log_id,
        context_snapshot=context_snapshot,
        repo_revision=repo_revision,
        worker_id=worker_id,
        pipeline_config=pipeline_config,
        round=1,
        generation=0,
        max_interactions=pipeline_config.get("max_interactions", 3),
        updated_at=now,
    )
    db.add(run)
    await db.flush()
    plan.active_run_id = run.id
    await db.flush()
    return plan, run


@serialize_related_plan_creation
async def create_plan_with_run(
    db: AsyncSession,
    *,
    title: str,
    initial_request: str,
    attachments: list[dict] | None,
    target_task_id: int | None,
    project_id: int | None,
    target_repo: str | None,
    target_branch: str | None,
    worker_id: int | None,
    priority: int,
    timeout_hours: float | None,
    created_by: int | None,
    pipeline_config: dict,
    context_session_id: str | None,
    context_log_id: int | None,
    context_snapshot: str | None,
    repo_revision: dict | None,
    forked_from_version_id: int | None = None,
    base_version_id: int | None = None,
    run_type: str = "initial",
    authorize_effect_boundary: (
        PlanEffectBoundaryAuthorizationCallback | None
    ) = None,
    authorize_locked_creation: (
        LockedPlanCreationAuthorizationCallback | None
    ) = None,
) -> tuple[Plan, PlanAgentRun]:
    """Create and commit a Plan/Run pair for existing API callers."""

    await _end_plan_routing_read(db)
    plan, run = await stage_plan_with_run(
        db,
        title=title,
        initial_request=initial_request,
        attachments=attachments,
        target_task_id=target_task_id,
        project_id=project_id,
        target_repo=target_repo,
        target_branch=target_branch,
        worker_id=worker_id,
        priority=priority,
        timeout_hours=timeout_hours,
        created_by=created_by,
        pipeline_config=pipeline_config,
        context_session_id=context_session_id,
        context_log_id=context_log_id,
        context_snapshot=context_snapshot,
        repo_revision=repo_revision,
        forked_from_version_id=forked_from_version_id,
        base_version_id=base_version_id,
        run_type=run_type,
        authorize_effect_boundary=authorize_effect_boundary,
        authorize_locked_creation=authorize_locked_creation,
    )
    await db.commit()
    await db.refresh(plan)
    await db.refresh(run)
    return plan, run


async def create_plan_run(
    db: AsyncSession,
    *,
    plan: Plan,
    run_type: str,
    request_text: str,
    attachments: list[dict] | None,
    base_version_id: int | None,
    expected_current_version_id: int | None,
    context_session_id: str | None,
    context_log_id: int | None,
    context_snapshot: str | None,
    repo_revision: dict | None,
    project_id: int | None,
    target_repo: str | None,
    target_branch: str | None,
    worker_id: int | None,
    source_run_id: int | None = None,
    authorize_effect_boundary: (
        PlanEffectBoundaryAuthorizationCallback | None
    ) = None,
    authorize_locked_plan: LockedPlanAuthorizationCallback | None = None,
) -> PlanAgentRun:
    """Create one Run under the Plan's durable active-run fence."""

    if plan.archived_at is not None:
        raise HTTPException(409, "Archived Plan cannot start a Run")
    if plan.active_run_id is not None:
        raise HTTPException(409, f"Plan already has active Run #{plan.active_run_id}")
    if expected_current_version_id != plan.current_version_id:
        raise HTTPException(409, "Plan current Version changed")
    if base_version_id is not None:
        base = await db.get(PlanVersion, base_version_id)
        if base is None or base.plan_id != plan.id:
            raise HTTPException(400, "Base Version does not belong to this Plan")

    expected_plan_id = plan.id
    expected_plan_lock_version = plan.lock_version
    expected_target_task_id = plan.target_task_id

    await _end_plan_routing_read(db)
    if authorize_effect_boundary is not None:
        # Public adapters establish Worker-node -> Project -> Task -> Worker ->
        # group -> User authority here.  The service re-enters Task/Worker and
        # then appends Plan; authorizing from the later Plan callback would
        # append Project after Task and deadlock with share revocation.
        try:
            await authorize_effect_boundary(db)
        except BaseException:
            operation, _ = await settle_awaitable(db.rollback())
            operation.result()
            raise
    else:
        await fence_worker_node_mutation(db)
    await fence_plan_target_task(
        db,
        target_task_id=expected_target_task_id,
        expected_worker_id=worker_id,
    )
    await fence_plan_worker(db, worker_id=worker_id)

    # A portable no-op CAS is the first Plan write in the fresh transaction.
    # ``SELECT ... FOR UPDATE`` alone is not a writer fence on SQLite, and a
    # local standalone Plan has no Task/Worker row to fence.  Keep this row
    # locked through authorization, Run insertion, and the active_run claim.
    fenced = await db.execute(
        update(Plan)
        .where(
            Plan.id == expected_plan_id,
            Plan.lock_version == expected_plan_lock_version,
            Plan.active_run_id.is_(None),
            Plan.archived_at.is_(None),
            Plan.target_task_id == expected_target_task_id,
            (
                Plan.current_version_id.is_(None)
                if expected_current_version_id is None
                else Plan.current_version_id == expected_current_version_id
            ),
        )
        .values(updated_at=Plan.updated_at)
    )
    if fenced.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Plan changed while creating the Run")

    try:
        plan = await db.get(
            Plan,
            expected_plan_id,
            with_for_update=True,
            populate_existing=True,
        )
        if plan is None:
            raise HTTPException(409, "Plan changed while creating the Run")
        if authorize_locked_plan is not None:
            await authorize_locked_plan(db, plan)
        await reject_capability_owned_plan_mutation(db, plan_ids=(plan.id,))
        if (
            plan.archived_at is not None
            or plan.active_run_id is not None
            or plan.current_version_id != expected_current_version_id
            or plan.target_task_id != expected_target_task_id
            or (
                expected_target_task_id is None
                and (
                    plan.project_id != project_id
                    or plan.target_repo != target_repo
                    or plan.target_branch != target_branch
                    or plan.worker_id != worker_id
                )
            )
        ):
            raise HTTPException(409, "Plan changed while creating the Run")

        if base_version_id is not None:
            base = await db.get(
                PlanVersion,
                base_version_id,
                with_for_update=True,
                populate_existing=True,
            )
            if base is None or base.plan_id != plan.id:
                raise HTTPException(400, "Base Version does not belong to this Plan")

        if run_type == "retry":
            if source_run_id is None:
                raise HTTPException(422, "retry requires source_run_id")
            source_run = await db.get(
                PlanAgentRun,
                source_run_id,
                with_for_update=True,
                populate_existing=True,
            )
            if (
                source_run is None
                or source_run.plan_id != plan.id
                or source_run.status != "failed"
                or source_run.finished_at is None
            ):
                raise HTTPException(409, "Retry source must be a terminal failed Run")
        elif source_run_id is not None:
            raise HTTPException(422, "source_run_id is only valid for retry")
    except BaseException:
        operation, _ = await settle_awaitable(db.rollback())
        operation.result()
        raise

    now = datetime.utcnow()
    run = PlanAgentRun(
        plan_id=plan.id,
        plan_task_id=None,
        run_type=run_type,
        source_run_id=source_run_id,
        status="queued",
        current_stage="planner",
        base_version_id=base_version_id,
        request_text=request_text,
        attachments=attachments or None,
        context_session_id=context_session_id,
        context_log_id=context_log_id,
        context_snapshot=context_snapshot,
        repo_revision=repo_revision,
        worker_id=worker_id,
        pipeline_config=plan.pipeline_config,
        round=1,
        generation=0,
        max_interactions=dict(plan.pipeline_config).get("max_interactions", 3),
        updated_at=now,
    )
    db.add(run)
    await db.flush()
    claimed = await db.execute(
        update(Plan)
        .where(
            Plan.id == plan.id,
            Plan.active_run_id.is_(None),
            Plan.archived_at.is_(None),
            (
                Plan.current_version_id.is_(None)
                if expected_current_version_id is None
                else Plan.current_version_id == expected_current_version_id
            ),
            Plan.lock_version == expected_plan_lock_version,
        )
        .values(
            active_run_id=run.id,
            project_id=project_id,
            target_repo=target_repo,
            target_branch=target_branch,
            worker_id=worker_id,
            lock_version=Plan.lock_version + 1,
            updated_at=now,
        )
    )
    if claimed.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Plan changed while creating the Run")
    await db.commit()
    await db.refresh(run)
    return run


async def complete_plan_run_with_version(
    db: AsyncSession,
    *,
    plan: Plan,
    run: PlanAgentRun,
    planner_step: PlanAgentStep,
    content: str,
    repo_revision: dict | None,
    reviewer_step_id: int | None,
    verdict: str,
    feedback: str,
    exhausted: bool,
    reviewer_repo_revision: dict | None,
    completed_at: datetime,
) -> PlanVersion:
    """Atomically publish one completed pipeline candidate as a Version."""

    existing = (
        await db.execute(
            select(PlanVersion).where(
                PlanVersion.produced_by_step_id == planner_step.id
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        if (
            existing.plan_id != plan.id
            or existing.produced_by_run_id != run.id
            or existing.content != content
        ):
            raise RuntimeError("Planner Step Version identity changed")
        version = existing
    else:
        next_number = (
            int(
                await db.scalar(
                    select(
                        func.coalesce(func.max(PlanVersion.version_number), 0)
                    ).where(PlanVersion.plan_id == plan.id)
                )
                or 0
            )
            + 1
        )
        version = PlanVersion(
            plan_id=plan.id,
            version_number=next_number,
            parent_version_id=plan.current_version_id,
            produced_by_run_id=run.id,
            produced_by_step_id=planner_step.id,
            content=content,
            context_session_id=run.context_session_id,
            context_log_id=run.context_log_id,
            context_snapshot=run.context_snapshot,
            repo_revision=repo_revision,
            human_decision="pending",
        )
        db.add(version)
        await db.flush()
    previous_id = version.parent_version_id
    if previous_id is not None and previous_id != version.id:
        await db.execute(
            update(PlanVersion)
            .where(
                PlanVersion.id == previous_id,
                PlanVersion.plan_id == plan.id,
                PlanVersion.superseded_by_version_id.is_(None),
            )
            .values(superseded_by_version_id=version.id)
        )

    version.review_verdict = "exhausted" if exhausted else verdict
    version.review_feedback = feedback
    version.reviewed_by_step_id = reviewer_step_id
    version.review_exhausted = exhausted
    version.reviewed_at = completed_at
    version.reviewer_repo_revision = reviewer_repo_revision
    planner_step.plan_version_id = version.id
    run.result_version_id = version.id
    run.status = "completed"
    run.current_stage = "complete"
    run.review_verdict = verdict
    run.review_feedback = feedback
    run.review_exhausted = exhausted
    run.finished_at = completed_at
    run.updated_at = completed_at
    plan.current_version_id = version.id
    plan.active_run_id = None
    plan.lock_version += 1
    plan.updated_at = completed_at
    await db.commit()
    await db.refresh(version)
    return version


def _answer_map(answers: Iterable[PlanInputAnswer | dict]) -> dict[str, object]:
    result: dict[str, object] = {}
    for answer in answers:
        item = answer.model_dump() if isinstance(answer, PlanInputAnswer) else answer
        question_id = item.get("question_id")
        if not isinstance(question_id, str) or question_id in result:
            raise HTTPException(422, "Answers must use unique valid question_id values")
        result[question_id] = item.get("value")
    return result


def validate_input_answers(
    questions: list[dict], answers: Iterable[PlanInputAnswer | dict]
) -> list[dict]:
    """Validate all questions without imposing a question-count limit."""

    parsed = [PlanQuestion.model_validate(question) for question in questions]
    by_id = {question.id: question for question in parsed}
    values = _answer_map(answers)
    unknown = set(values) - set(by_id)
    if unknown:
        raise HTTPException(422, f"Unknown question ids: {sorted(unknown)}")
    normalized: list[dict] = []
    for question in parsed:
        value = values.get(question.id)
        if question.required and (value is None or value == "" or value == []):
            raise HTTPException(422, f"Question {question.id!r} requires an answer")
        if value is None:
            normalized.append({"question_id": question.id, "value": None})
            continue
        if question.response_type == "text":
            if not isinstance(value, str) or len(value) > 50_000:
                raise HTTPException(422, f"Question {question.id!r} requires text")
        elif question.response_type == "single_choice":
            allowed = {option.value for option in question.options}
            if not isinstance(value, str) or value not in allowed:
                raise HTTPException(
                    422, f"Question {question.id!r} has an invalid choice"
                )
        else:
            allowed = {option.value for option in question.options}
            if (
                not isinstance(value, list)
                or any(
                    not isinstance(item, str) or item not in allowed for item in value
                )
                or len(value) != len(set(value))
            ):
                raise HTTPException(
                    422, f"Question {question.id!r} has invalid choices"
                )
        normalized.append({"question_id": question.id, "value": value})
    return normalized


async def answer_input_request(
    db: AsyncSession,
    *,
    plan: Plan,
    run: PlanAgentRun,
    input_request: PlanInputRequest,
    expected_generation: int,
    idempotency_key: str,
    answers: Iterable[PlanInputAnswer | dict],
    response_text: str | None,
    attachments: list[dict] | None,
    answered_by: int | None,
) -> PlanInputRequest:
    capability_owned = (
        await db.scalar(
            select(Plan.id)
            .where(
                Plan.id == plan.id,
                _capability_plan_owner_exists(),
            )
            .limit(1)
        )
        is not None
    )
    if capability_owned and (
        run.run_type != "capability" or run.capability_execution_id is None
    ):
        # Ownership is aggregate-wide and permanent.  A malformed or stale
        # ordinary active Run must not turn the generic answer endpoint into a
        # write backdoor after an earlier Capability Run established ownership.
        raise HTTPException(
            409,
            dict(CAPABILITY_OWNED_PLAN_MUTATION_DETAIL),
        )

    normalized = validate_input_answers(input_request.questions, answers)
    normalized_attachments = attachments or None
    replay = input_request.status == "answered"
    if replay:
        source_step = await db.get(
            PlanAgentStep,
            input_request.source_step_id,
            populate_existing=True,
        )
        if (
            input_request.answer_idempotency_key != idempotency_key
            or source_step is None
            or source_step.id != input_request.source_step_id
            or source_step.run_id != run.id
            or source_step.plan_id != plan.id
            or source_step.generation != expected_generation
            or input_request.answers != normalized
            or input_request.response_text != response_text
            or input_request.attachments != normalized_attachments
        ):
            raise HTTPException(
                409,
                "Answered Plan input can only replay its exact idempotent payload",
            )

    capability_invocation_id: int | None = None
    capability_invocation_version: int | None = None
    capability_execution_version: int | None = None
    if capability_owned:
        # Freeze the immutable Core link before ending the routing snapshot.
        # The fresh transaction below takes writer fences in the same global
        # order as Core/deletion: Invocation -> Execution -> Run -> Input.
        from backend.models.capability import (
            CapabilityExecution,
            CapabilityInvocation,
        )

        execution_ref = await db.get(
            CapabilityExecution,
            run.capability_execution_id,
            populate_existing=True,
        )
        if execution_ref is None:
            raise HTTPException(
                409,
                "Plan Capability execution disappeared before input answer",
            )
        invocation = await db.get(
            CapabilityInvocation,
            execution_ref.invocation_id,
            populate_existing=True,
        )
        execution = await db.get(
            CapabilityExecution,
            run.capability_execution_id,
            populate_existing=True,
        )
        immutable_link_valid = (
            invocation is not None
            and execution is not None
            and execution.invocation_id == invocation.id
            and invocation.task_id == plan.target_task_id
            and invocation.capability_key == "plan"
            and invocation.executor_kind == "plan_agent"
            and execution.executor_kind == "plan_agent"
            and run.run_type == "capability"
            and execution.handle_kind == "plan_agent_run"
            and execution.handle_id == str(run.id)
            and execution.handle_generation == PLAN_RUN_HANDLE_GENERATION
        )
        if not immutable_link_valid:
            raise HTTPException(
                409,
                "Plan Capability execution identity is invalid",
            )
        if replay:
            return input_request
        if (
            execution.active_invocation_id != invocation.id
            or invocation.active_task_id != invocation.task_id
            or invocation.status not in {"running", "waiting_user"}
            or execution.status not in {"running", "waiting_user"}
        ):
            raise HTTPException(
                409,
                "Plan Capability is no longer accepting input",
            )
        capability_invocation_id = invocation.id
        capability_invocation_version = invocation.state_version
        capability_execution_version = execution.state_version
    if replay:
        return input_request
    if plan.active_run_id != run.id or run.status != "waiting_user":
        raise HTTPException(409, "Plan Run is no longer waiting for input")
    if run.generation != expected_generation:
        raise HTTPException(409, "Plan Run generation changed")
    if run.open_input_request_id != input_request.id or input_request.status != "open":
        raise HTTPException(409, "Input request is no longer open")
    from backend.services.plan_input_safety import contains_high_confidence_secret

    if contains_high_confidence_secret(
        [response_text, *[item.get("value") for item in normalized]]
    ):
        raise HTTPException(
            422,
            "Plan answers cannot store API keys or access tokens. "
            "Save the credential in Settings → Secrets and answer with its name/reference.",
        )

    plan_id = plan.id
    run_id = run.id
    input_request_id = input_request.id
    target_task_id = plan.target_task_id
    capability_execution_id = run.capability_execution_id

    # End all read snapshots before the first writer fence. SQLite WAL cannot
    # upgrade a snapshot after a concurrent cancellation/claim commits. The
    # conditional writer sequence below also gives PostgreSQL/MySQL one global
    # row-lock order, so answer and every cancellation path cannot deadlock.
    await db.rollback()
    # Answering an open request reactivates the Run.  Serialize that producer
    # transition before Invocation/Execution/Run locks so a Worker drain that
    # has already begun cannot turn a resting ``waiting_user`` Run back into
    # executable work after the node-wide snapshot.
    await fence_worker_node_mutation(db)
    now = datetime.utcnow()

    if capability_owned:
        assert capability_invocation_id is not None
        assert capability_invocation_version is not None
        assert capability_execution_id is not None
        assert capability_execution_version is not None
        invocation_fenced = await db.execute(
            update(CapabilityInvocation)
            .where(
                CapabilityInvocation.id == capability_invocation_id,
                CapabilityInvocation.task_id == target_task_id,
                CapabilityInvocation.active_task_id == target_task_id,
                CapabilityInvocation.capability_key == "plan",
                CapabilityInvocation.executor_kind == "plan_agent",
                CapabilityInvocation.status.in_(["running", "waiting_user"]),
                CapabilityInvocation.state_version
                == capability_invocation_version,
            )
            # A self-assignment is an intentional portable writer/row-lock
            # fence; the Core version remains owned by Core transitions.
            .values(state_version=CapabilityInvocation.state_version)
        )
        if invocation_fenced.rowcount != 1:
            await db.rollback()
            raise HTTPException(409, "Plan Capability is no longer accepting input")
        execution_fenced = await db.execute(
            update(CapabilityExecution)
            .where(
                CapabilityExecution.id == capability_execution_id,
                CapabilityExecution.invocation_id == capability_invocation_id,
                CapabilityExecution.active_invocation_id
                == capability_invocation_id,
                CapabilityExecution.executor_kind == "plan_agent",
                CapabilityExecution.handle_kind == "plan_agent_run",
                CapabilityExecution.handle_id == str(run_id),
                CapabilityExecution.handle_generation
                == PLAN_RUN_HANDLE_GENERATION,
                CapabilityExecution.status.in_(["running", "waiting_user"]),
                CapabilityExecution.state_version == capability_execution_version,
            )
            .values(state_version=CapabilityExecution.state_version)
        )
        if execution_fenced.rowcount != 1:
            await db.rollback()
            raise HTTPException(409, "Plan Capability is no longer accepting input")

    resumed = await db.execute(
        update(PlanAgentRun)
        .where(
            PlanAgentRun.id == run_id,
            PlanAgentRun.plan_id == plan_id,
            PlanAgentRun.status == "waiting_user",
            PlanAgentRun.generation == expected_generation,
            PlanAgentRun.open_input_request_id == input_request_id,
            (
                PlanAgentRun.capability_execution_id == capability_execution_id
                if capability_owned
                else PlanAgentRun.capability_execution_id.is_(None)
            ),
        )
        .values(
            status="queued",
            current_stage="planner",
            open_input_request_id=None,
            generation=PlanAgentRun.generation + 1,
            updated_at=now,
        )
    )
    if resumed.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Input request was answered concurrently")
    updated = await db.execute(
        update(PlanInputRequest)
        .where(
            PlanInputRequest.id == input_request_id,
            PlanInputRequest.plan_id == plan_id,
            PlanInputRequest.run_id == run_id,
            PlanInputRequest.status == "open",
        )
        .values(
            status="answered",
            answers=normalized,
            response_text=response_text,
            attachments=normalized_attachments,
            answered_by=answered_by,
            answered_at=now,
            answer_idempotency_key=idempotency_key,
        )
    )
    if updated.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Input request was answered concurrently")
    await db.commit()
    # Rehydrate identities expired by the fresh-writer rollback. Callers use
    # Plan/Run fields for the response event after this service returns.
    await db.get(Plan, plan_id, populate_existing=True)
    await db.get(PlanAgentRun, run_id, populate_existing=True)
    answered = await db.get(
        PlanInputRequest,
        input_request_id,
        populate_existing=True,
    )
    if answered is None:
        raise HTTPException(409, "Input request disappeared after answer commit")
    return answered


async def decide_version(
    db: AsyncSession,
    *,
    plan: Plan,
    version: PlanVersion,
    decision: str,
    decided_by: int | None,
    expected_current_version_id: int,
) -> PlanVersion:
    if (
        plan.current_version_id != expected_current_version_id
        or version.id != expected_current_version_id
    ):
        raise HTTPException(409, "Plan current Version changed")
    if plan.active_run_id is not None:
        raise HTTPException(409, "Plan has an active Run")
    if (
        version.review_verdict not in {"approve", "disabled", "exhausted"}
        and not version.review_exhausted
    ):
        raise HTTPException(409, "Version is not ready for a human decision")
    if version.human_decision != "pending":
        if version.human_decision == decision:
            return version
        raise HTTPException(409, f"Version was already {version.human_decision}")
    # This service can also run inside a headless Worker.  The node singleton
    # is the outermost durable producer fence there and a no-op on the
    # Manager.  API adapters may already hold it together with Project/Task
    # authority; taking the same row again in the same transaction is
    # idempotent and keeps direct/internal callers fail-closed.
    await fence_worker_node_mutation(db)
    plan_fenced = await db.execute(
        update(Plan)
        .where(
            Plan.id == plan.id,
            Plan.lock_version == plan.lock_version,
            Plan.current_version_id == expected_current_version_id,
            Plan.active_run_id.is_(None),
            Plan.archived_at.is_(None),
            Plan.target_task_id == plan.target_task_id,
            Plan.project_id == plan.project_id,
            Plan.created_by == plan.created_by,
        )
        # Portable Plan writer fence: SELECT FOR UPDATE is ignored by SQLite.
        .values(updated_at=Plan.updated_at)
    )
    if plan_fenced.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Plan changed while deciding the Version")
    changed = await db.execute(
        update(PlanVersion)
        .where(
            PlanVersion.id == version.id,
            PlanVersion.plan_id == plan.id,
            PlanVersion.human_decision == "pending",
            PlanVersion.superseded_by_version_id.is_(None),
        )
        .values(
            human_decision=decision,
            decided_at=datetime.utcnow(),
            decided_by=decided_by,
        )
    )
    if changed.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Version decision changed concurrently")
    await db.commit()
    await db.refresh(version)
    return version


async def materialize_execution_task(
    db: AsyncSession,
    *,
    plan_id: int,
    version_id: int,
    expected_current_version_id: int,
    confirm_stale: bool,
    approve_if_pending: bool,
    actor_id: int | None,
    execution_principal: dict[str, object] | None = None,
    execution_metadata: dict | None = None,
    authorize_effect_boundary: (
        PlanEffectBoundaryAuthorizationCallback | None
    ) = None,
    authorize_locked_plan: LockedPlanAuthorizationCallback | None = None,
) -> PlanExecutionTaskResult:
    """Idempotently apply one standalone Plan Version as an execution Task.

    This is the canonical in-process boundary for UI/API callers and future
    orchestrators.  The exact Plan Version is the idempotency key: replaying
    the operation returns its existing Task and never creates a second one.
    Authorization and post-commit wake/broadcast behavior remain adapter
    concerns and must be handled by the caller.
    """

    from backend.services.task_creation import (
        TASK_EXECUTION_MODES,
        TASK_EXECUTION_PRINCIPAL_KINDS,
        TASK_EXECUTION_ROLES,
        task_execution_principal_values,
        system_task_execution_principal_values,
    )

    principal_fields = dict(
        execution_principal or system_task_execution_principal_values()
    )
    if set(principal_fields) != {
        "execution_user_id",
        "execution_user_role",
        "execution_mode",
        "execution_principal_kind",
    }:
        raise ValueError("invalid execution Task principal shape")
    role = principal_fields["execution_user_role"]
    mode = principal_fields["execution_mode"]
    kind = principal_fields["execution_principal_kind"]
    if (
        role not in TASK_EXECUTION_ROLES
        or mode not in TASK_EXECUTION_MODES
        or kind not in TASK_EXECUTION_PRINCIPAL_KINDS
    ):
        raise ValueError("invalid execution Task principal")
    expected_principal = task_execution_principal_values(
        user_id=principal_fields["execution_user_id"],
        role=role,
        principal_kind=kind,
    )
    if principal_fields != expected_principal:
        raise ValueError("execution Task principal role/mode mismatch")

    async def existing_execution_task_result() -> PlanExecutionTaskResult | None:
        """Return the immutable winner without revalidating mutable Plan state."""

        existing_plan = await db.get(
            Plan,
            plan_id,
            populate_existing=True,
        )
        existing_version = await db.get(
            PlanVersion,
            version_id,
            populate_existing=True,
        )
        existing_application = (
            await db.execute(
                select(PlanApplication).where(
                    PlanApplication.plan_version_id == version_id
                )
            )
        ).scalar_one_or_none()
        if existing_application is None:
            return None
        if (
            existing_plan is None
            or existing_version is None
            or existing_version.plan_id != existing_plan.id
            or existing_application.plan_id != existing_plan.id
            or existing_application.application_type != "execution_task"
            or existing_application.execution_task_id is None
            or existing_version.human_decision != "approved"
        ):
            raise HTTPException(409, "Plan Version was already applied")
        existing_task = await db.get(
            Task,
            existing_application.execution_task_id,
            populate_existing=True,
        )
        if existing_task is None:
            raise HTTPException(
                409,
                "Plan Version execution Task is missing",
            )
        return PlanExecutionTaskResult(
            plan=existing_plan,
            version=existing_version,
            application=existing_application,
            task=existing_task,
            created=False,
        )

    async with plan_operation_lock(plan_id):
        plan = await db.get(Plan, plan_id, populate_existing=True)
        version = await db.get(PlanVersion, version_id, populate_existing=True)
        if plan is None:
            raise HTTPException(404, "Plan not found")
        if version is None or version.plan_id != plan.id:
            raise HTTPException(404, "Plan Version not found")
        await reject_capability_owned_plan_mutation(
            db,
            plan_ids=(plan.id,),
        )
        # A committed Application is the immutable result for this exact
        # Version.  Archive/Refresh/new-Version state may legitimately change
        # afterwards and must not turn an HTTP retry or orchestrator replay
        # into a false conflict.
        existing_result = await existing_execution_task_result()
        if existing_result is not None:
            if authorize_locked_plan is not None:
                await authorize_locked_plan(db, existing_result.plan)
            return existing_result
        if plan.archived_at is not None:
            raise HTTPException(
                409,
                "Archived Plan cannot create an execution Task",
            )
        if plan.target_task_id is not None:
            raise HTTPException(400, "Only standalone Plans create execution Tasks")
        if (
            plan.current_version_id != expected_current_version_id
            or version.id != expected_current_version_id
        ):
            raise HTTPException(
                409,
                {
                    "code": "plan_version_changed",
                    "message": "Plan current Version changed",
                    "plan_id": plan.id,
                    "current_version_id": plan.current_version_id,
                    "active_run_id": plan.active_run_id,
                },
            )
        if plan.active_run_id is not None:
            raise HTTPException(
                409,
                {
                    "code": "plan_active_run",
                    "message": "Plan has an active Run",
                    "plan_id": plan.id,
                    "current_version_id": plan.current_version_id,
                    "active_run_id": plan.active_run_id,
                },
            )
        if version.superseded_by_version_id is not None:
            raise HTTPException(409, "Superseded Plan Version cannot be executed")
        expected_plan_lock_version = plan.lock_version
        expected_worker_id = plan.worker_id
        expected_project_id = plan.project_id
        expected_target_repo = plan.target_repo
        expected_target_branch = plan.target_branch
        expected_priority = plan.priority
        expected_timeout_hours = plan.timeout_hours

        from backend.services.plan_staleness import version_staleness

        stale = await version_staleness(db, plan, version)
        if stale["hard_conflict"]:
            raise HTTPException(
                409,
                {
                    "code": "plan_hard_conflict",
                    "message": "Execution target is unavailable",
                    **stale,
                },
            )
        if stale["stale"] and not confirm_stale:
            raise HTTPException(
                409,
                {
                    "code": "plan_stale",
                    "message": "Plan Version context is stale",
                    **stale,
                },
            )
        # Staleness inspection can await Git/Worker I/O and therefore leave a
        # long-lived SQLite WAL read snapshot.  End it before taking the
        # portable writer fences: otherwise a concurrent writer can turn the
        # updates below into SQLITE_BUSY_SNAPSHOT instead of a deterministic
        # CAS miss.
        await db.rollback()

        # Public execution admission establishes Worker-node -> Project ->
        # Worker -> group -> User before the Plan aggregate.  The Worker fence
        # below re-enters that already-held row in the same transaction.
        if authorize_effect_boundary is not None:
            try:
                await authorize_effect_boundary(db)
            except BaseException:
                operation, _ = await settle_awaitable(db.rollback())
                operation.result()
                raise

        # All Plan admission paths keep Worker before Plan.  Taking the Worker
        # lifecycle fence here prevents a standalone create-Run from
        # deadlocking with execution materialization on PostgreSQL/MySQL.
        await fence_plan_worker(db, worker_id=expected_worker_id)

        # This conditional write is the cross-process writer fence.  The
        # process-local plan_operation_lock only serializes one Manager
        # process; lock_version detects an archive/new-Run/current-Version
        # change which won while staleness was being evaluated, and the row
        # write keeps later Plan writers behind this transaction until the
        # approval, Task and Application commit atomically.
        now = datetime.utcnow()
        fenced = await db.execute(
            update(Plan)
            .where(
                Plan.id == plan_id,
                Plan.lock_version == expected_plan_lock_version,
                Plan.archived_at.is_(None),
                Plan.target_task_id.is_(None),
                Plan.active_run_id.is_(None),
                Plan.current_version_id == expected_current_version_id,
                Plan.worker_id == expected_worker_id,
                Plan.project_id == expected_project_id,
                Plan.target_repo == expected_target_repo,
                Plan.target_branch == expected_target_branch,
                Plan.priority == expected_priority,
                Plan.timeout_hours == expected_timeout_hours,
            )
            .values(
                lock_version=Plan.lock_version + 1,
                updated_at=now,
            )
        )
        if fenced.rowcount != 1:
            await db.rollback()
            # A simultaneous identical materialization may have won the Plan
            # fence and committed while this UPDATE waited.  Preserve exact
            # Version idempotency even if a later aggregate writer has already
            # archived the Plan or started its next Run.
            existing_result = await existing_execution_task_result()
            if existing_result is not None:
                # The old authorization snapshot ended before the Plan CAS.
                # A concurrent exact materialization is safe to replay, but
                # its newly-created Task is still protected data: re-check
                # current access before returning that winner.
                if authorize_locked_plan is not None:
                    try:
                        await authorize_locked_plan(db, existing_result.plan)
                    except BaseException:
                        operation, _ = await settle_awaitable(db.rollback())
                        operation.result()
                        raise
                return existing_result
            concurrent_plan = await db.get(
                Plan,
                plan_id,
                populate_existing=True,
            )
            raise HTTPException(
                409,
                {
                    "code": "plan_changed_during_execution_materialization",
                    "message": "Plan changed while creating the execution Task",
                    "plan_id": plan_id,
                    "current_version_id": (
                        concurrent_plan.current_version_id
                        if concurrent_plan is not None
                        else None
                    ),
                    "active_run_id": (
                        concurrent_plan.active_run_id
                        if concurrent_plan is not None
                        else None
                    ),
                },
            )

        try:
            # The Plan UPDATE above already owns the aggregate writer fence.
            # Refresh both exact rows inside that transaction before applying
            # the approval so no state inspected before the WAL reset is used
            # to create the Task.
            plan = await db.get(
                Plan,
                plan_id,
                with_for_update=True,
                populate_existing=True,
            )
            version = await db.get(
                PlanVersion,
                version_id,
                with_for_update=True,
                populate_existing=True,
            )
            if plan is None or version is None or version.plan_id != plan.id:
                raise HTTPException(
                    409,
                    "Plan or Version changed while creating the execution Task",
                )
            if authorize_locked_plan is not None:
                await authorize_locked_plan(db, plan)
            await reject_capability_owned_plan_mutation(
                db,
                plan_ids=(plan.id,),
            )
            if (
                plan.archived_at is not None
                or plan.target_task_id is not None
                or plan.active_run_id is not None
                or plan.current_version_id != expected_current_version_id
                or version.id != expected_current_version_id
                or version.superseded_by_version_id is not None
                or plan.worker_id != expected_worker_id
                or plan.project_id != expected_project_id
                or plan.target_repo != expected_target_repo
                or plan.target_branch != expected_target_branch
                or plan.priority != expected_priority
                or plan.timeout_hours != expected_timeout_hours
            ):
                raise HTTPException(
                    409,
                    "Plan or Version changed while creating the execution Task",
                )
            if version.human_decision == "pending" and approve_if_pending:
                if (
                    version.review_verdict
                    not in {"approve", "disabled", "exhausted"}
                    and not version.review_exhausted
                ):
                    raise HTTPException(
                        409,
                        "Version is not ready for a human decision",
                    )
                changed = await db.execute(
                    update(PlanVersion)
                    .where(
                        PlanVersion.id == version.id,
                        PlanVersion.plan_id == plan.id,
                        PlanVersion.human_decision == "pending",
                        PlanVersion.superseded_by_version_id.is_(None),
                    )
                    .values(
                        human_decision="approved",
                        decided_at=now,
                        decided_by=actor_id,
                    )
                )
                if changed.rowcount != 1:
                    raise HTTPException(
                        409,
                        "Version decision changed concurrently",
                    )
                await db.refresh(version)
            if version.human_decision != "approved":
                raise HTTPException(409, "Plan Version must be approved")

            if plan.project_id is not None:
                from backend.models.project import Project
                from backend.services.project_readiness import (
                    ProjectNotDispatchableError,
                    require_project_dispatchable,
                )

                try:
                    require_project_dispatchable(
                        await db.get(Project, plan.project_id)
                    )
                except ProjectNotDispatchableError as exc:
                    raise HTTPException(422, exc.detail) from exc

            metadata = dict(execution_metadata or {})
            # These audit keys are authoritative and cannot be overridden by an
            # embedding orchestrator's optional correlation metadata.
            metadata.update(
                {
                    "created_from_plan_id": plan.id,
                    "created_from_plan_version_id": version.id,
                }
            )
            task = await stage_task_record(
                db,
                title=f"Execute {plan.title} · v{version.version_number}"[:200],
                description=(
                    "[Approved implementation plan]\n"
                    "Implement the exact approved Plan Version below.\n\n"
                    f'<plan id="{plan.id}" version="{version.version_number}">\n'
                    f"{version.content}\n</plan>\n\n"
                    f"[Original planning request]\n{plan.initial_request}"
                ),
                status="pending",
                priority=plan.priority,
                timeout_hours=plan.timeout_hours,
                project_id=plan.project_id,
                target_repo=plan.target_repo,
                target_branch=plan.target_branch,
                merge_status="pending",
                worker_id=plan.worker_id,
                created_by=actor_id,
                mode="auto",
                metadata_=metadata,
                # Worker placement is not an authorization downgrade.  This
                # implementation Task retains the native Manager principal;
                # the Worker transport converts it to a delegated envelope.
                **principal_fields,
            )
            application = PlanApplication(
                plan_id=plan.id,
                plan_version_id=version.id,
                application_type="execution_task",
                execution_task_id=task.id,
                applied_by=actor_id,
            )
            db.add(application)
            await db.commit()
        except IntegrityError:
            # The database uniqueness fence is authoritative across API
            # processes.  A concurrent winner may have committed after our
            # pre-check; discard this transaction's Task and return that exact
            # application instead of exposing a false failure to a retrying
            # orchestrator.
            await db.rollback()
            existing_result = await existing_execution_task_result()
            if existing_result is None:
                raise
            if authorize_locked_plan is not None:
                try:
                    await authorize_locked_plan(db, existing_result.plan)
                except BaseException:
                    operation, _ = await settle_awaitable(db.rollback())
                    operation.result()
                    raise
            return existing_result
        except Exception:
            await db.rollback()
            raise
        await db.refresh(application)
        await db.refresh(task)
        return PlanExecutionTaskResult(
            plan=plan,
            version=version,
            application=application,
            task=task,
            created=True,
        )


async def cancel_run(
    db: AsyncSession, *, plan: Plan, run: PlanAgentRun
) -> PlanAgentRun:
    """Fence an ordinary local Run before its provider runtime is stopped.

    The public endpoint finishes the cancellation only after the exact runtime
    generation has a durable cleanup receipt.  Keeping both aggregate owners
    here makes a failed stop or Manager crash safely retryable.
    """

    for attempt in range(LOCAL_CANCELLATION_FENCE_ATTEMPTS):
        if run.status == "cancelled":
            await db.commit()
            await db.refresh(run)
            return run
        if (
            run.run_type == "capability"
            or run.capability_execution_id is not None
            or await db.scalar(
                select(Plan.id)
                .where(Plan.id == plan.id, _capability_plan_owner_exists())
                .limit(1)
            )
            is not None
        ):
            raise HTTPException(409, dict(CAPABILITY_OWNED_PLAN_MUTATION_DETAIL))
        if run.worker_id is not None:
            raise HTTPException(
                409,
                "Worker Plan Run requires a remote cancellation receipt",
            )
        if run.status == "cancelling":
            if (
                plan.active_run_id != run.id
                or run.cancellation_target_generation is None
                or run.generation != run.cancellation_target_generation + 1
            ):
                raise HTTPException(
                    409,
                    "Plan Run cancellation fence is inconsistent",
                )
            await db.commit()
            await db.refresh(run)
            return run
        if plan.active_run_id != run.id or run.status not in ACTIVE_RUN_STATUSES:
            raise HTTPException(409, "Plan Run is no longer active")

        run_id = run.id
        plan_id = plan.id
        expected_plan_lock_version = plan.lock_version
        expected_input_request_id = run.open_input_request_id

        # End every authorization/read snapshot before the Run UPDATE becomes
        # the first statement in a fresh writer transaction.  This is required
        # on SQLite WAL: SELECT -> concurrent claim commit -> UPDATE otherwise
        # fails with SQLITE_BUSY_SNAPSHOT instead of choosing a durable winner.
        await db.rollback()
        now = datetime.utcnow()
        # Run is the global Plan-aggregate writer fence.  It must be the first
        # write after the WAL snapshot rollback and precede Plan/children/Input
        # on every supported database.  The exact open-input pointer makes an
        # answer and cancellation choose one winner without opposite row locks.
        changed = await db.execute(
            update(PlanAgentRun)
            .where(
                PlanAgentRun.id == run_id,
                PlanAgentRun.plan_id == plan_id,
                PlanAgentRun.worker_id.is_(None),
                PlanAgentRun.run_type != "capability",
                PlanAgentRun.capability_execution_id.is_(None),
                PlanAgentRun.status.in_(ACTIVE_RUN_STATUSES),
                (
                    PlanAgentRun.open_input_request_id.is_(None)
                    if expected_input_request_id is None
                    else PlanAgentRun.open_input_request_id
                    == expected_input_request_id
                ),
            )
            # Capture the database generation on the same atomic write that
            # fences it. MySQL evaluates ordered SET values left-to-right.
            .ordered_values(
                (
                    PlanAgentRun.cancellation_target_generation,
                    PlanAgentRun.generation,
                ),
                (PlanAgentRun.generation, PlanAgentRun.generation + 1),
                (PlanAgentRun.status, "cancelling"),
                (PlanAgentRun.open_input_request_id, None),
                (PlanAgentRun.error, "Cancellation requested"),
                (PlanAgentRun.updated_at, now),
            )
        )
        plan_changed = None
        if changed is not None and changed.rowcount == 1:
            plan_changed = await db.execute(
                update(Plan)
                .where(
                    Plan.id == plan_id,
                    Plan.active_run_id == run_id,
                    Plan.lock_version == expected_plan_lock_version,
                )
                .values(
                    lock_version=Plan.lock_version + 1,
                    updated_at=now,
                )
            )
        input_changed = None
        if (
            changed.rowcount == 1
            and plan_changed is not None
            and plan_changed.rowcount == 1
            and expected_input_request_id is not None
        ):
            input_changed = await db.execute(
                update(PlanInputRequest)
                .where(
                    PlanInputRequest.id == expected_input_request_id,
                    PlanInputRequest.run_id == run_id,
                    PlanInputRequest.status.in_(["prepared", "open"]),
                )
                .values(status="cancelled", cancelled_at=now)
            )
        input_won = expected_input_request_id is None or (
            input_changed is not None and input_changed.rowcount == 1
        )
        if (
            changed.rowcount == 1
            and plan_changed is not None
            and plan_changed.rowcount == 1
            and input_won
        ):
            await db.commit()
            fenced = await db.get(
                PlanAgentRun,
                run_id,
                populate_existing=True,
            )
            if fenced is None:
                raise HTTPException(
                    409,
                    "Plan Run disappeared after fencing cancellation",
                )
            return fenced

        await db.rollback()
        if attempt + 1 >= LOCAL_CANCELLATION_FENCE_ATTEMPTS:
            break
        run = await db.get(
            PlanAgentRun,
            run_id,
            populate_existing=True,
        )
        plan = await db.get(
            Plan,
            plan_id,
            populate_existing=True,
        )
        if run is None or plan is None or run.plan_id != plan.id:
            await db.rollback()
            raise HTTPException(409, "Plan Run changed while fencing cancellation")

    await db.rollback()
    raise HTTPException(409, "Plan Run changed while fencing cancellation")


async def release_run_owner_after_cleanup(
    db: AsyncSession,
    *,
    plan: Plan,
    run: PlanAgentRun,
) -> PlanAgentRun:
    """Release an ordinary local Run owner only after exact G is clean."""

    capability_owned = (
        await db.scalar(
            select(Plan.id)
            .where(Plan.id == plan.id, _capability_plan_owner_exists())
            .limit(1)
        )
        is not None
    )
    if (
        capability_owned
        or run.run_type == "capability"
        or run.capability_execution_id is not None
        or run.worker_id is not None
        or run.status != "cancelling"
        or run.cancellation_target_generation is None
        or run.generation != run.cancellation_target_generation + 1
        or run.plan_id != plan.id
        or plan.active_run_id != run.id
    ):
        raise HTTPException(409, "Plan Run cancellation owner is inconsistent")

    from backend.services.plan_runtime_receipt import runtime_generation_is_clean

    if not await runtime_generation_is_clean(
        db,
        run_id=run.id,
        generation=run.cancellation_target_generation,
    ):
        raise HTTPException(
            409,
            "Plan Run runtime cleanup is not durably confirmed",
        )

    reverse_owners = list(
        (
            await db.execute(
                select(Instance)
                .where(Instance.current_plan_run_id == run.id)
                .order_by(Instance.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    if len(reverse_owners) > 1:
        raise HTTPException(409, "Plan Run has duplicate Instance owners")
    reverse_owner = reverse_owners[0] if reverse_owners else None
    if reverse_owner is not None:
        if reverse_owner.current_task_id is not None or reverse_owner.pid is not None:
            raise HTTPException(
                409,
                f"Plan Run #{run.id} still owns live Instance #{reverse_owner.id}",
            )
        if (
            (run.instance_id is not None and run.instance_id != reverse_owner.id)
            or reverse_owner.status not in {"running", "idle"}
        ):
            raise HTTPException(409, "Plan Run Instance owner is not safe to release")
        reverse_owner.current_plan_run_id = None
        reverse_owner.status = "idle"
    elif run.instance_id is not None:
        # A previous transaction may already have released the reverse side.
        # Lock the named row but never mutate a newer owner.
        await db.get(Instance, run.instance_id, with_for_update=True)

    now = datetime.utcnow()
    if run.last_execution_started_at is not None:
        run.execution_seconds = float(run.execution_seconds or 0) + max(
            0.0,
            (now - run.last_execution_started_at).total_seconds(),
        )
        run.last_execution_started_at = None
    run.instance_id = None
    for step in list(
        (
            await db.execute(
                select(PlanAgentStep).where(
                    PlanAgentStep.run_id == run.id,
                    PlanAgentStep.generation == run.cancellation_target_generation,
                    PlanAgentStep.status == "running",
                )
            )
        ).scalars()
    ):
        step.status = "cancelled"
        step.error = "Cancelled by user"
        step.finished_at = now
    run.updated_at = now
    await db.commit()
    await db.refresh(run)
    return run


async def finalize_run_cancellation(
    db: AsyncSession,
    *,
    plan: Plan,
    run: PlanAgentRun,
) -> PlanAgentRun:
    """Publish an ordinary local cancellation after owner release."""

    if run.status == "cancelled":
        await db.commit()
        await db.refresh(run)
        return run
    capability_owned = (
        await db.scalar(
            select(Plan.id)
            .where(Plan.id == plan.id, _capability_plan_owner_exists())
            .limit(1)
        )
        is not None
    )
    if (
        capability_owned
        or run.run_type == "capability"
        or run.capability_execution_id is not None
        or run.worker_id is not None
        or run.status != "cancelling"
        or run.cancellation_target_generation is None
        or run.generation != run.cancellation_target_generation + 1
        or run.plan_id != plan.id
        or plan.active_run_id != run.id
    ):
        raise HTTPException(409, "Plan Run cancellation is not pending")

    from backend.services.plan_runtime_receipt import runtime_generation_is_clean

    reverse_owner = await db.scalar(
        select(Instance.id)
        .where(Instance.current_plan_run_id == run.id)
        .limit(1)
    )
    if (
        run.instance_id is not None
        or run.last_execution_started_at is not None
        or reverse_owner is not None
        or not await runtime_generation_is_clean(
            db,
            run_id=run.id,
            generation=run.cancellation_target_generation,
        )
    ):
        raise HTTPException(
            409,
            "Plan Run runtime cleanup is not durably confirmed",
        )

    now = datetime.utcnow()
    changed = await db.execute(
        update(PlanAgentRun)
        .where(
            PlanAgentRun.id == run.id,
            PlanAgentRun.plan_id == plan.id,
            PlanAgentRun.status == "cancelling",
            PlanAgentRun.cancellation_target_generation
            == run.cancellation_target_generation,
        )
        .values(
            status="cancelled",
            cancellation_target_generation=None,
            error="Cancelled by user",
            updated_at=now,
            finished_at=now,
        )
    )
    released = await db.execute(
        update(Plan)
        .where(Plan.id == plan.id, Plan.active_run_id == run.id)
        .values(
            active_run_id=None,
            lock_version=Plan.lock_version + 1,
            updated_at=now,
        )
    )
    if changed.rowcount != 1 or released.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Plan Run changed while finalizing cancellation")
    await db.commit()
    await db.refresh(run)
    return run


async def cancel_worker_mirror_run_after_ack(
    db: AsyncSession,
    *,
    plan: Plan,
    run: PlanAgentRun,
    receipt_settlement_reason: str = "remote_cancelled",
    receipt_remote_status: str | None = "cancelled",
) -> PlanAgentRun:
    """Terminalize a Manager mirror after exact remote-absence evidence."""

    expected_run_id = run.id
    expected_plan_id = plan.id
    run = await db.get(
        PlanAgentRun,
        expected_run_id,
        with_for_update=True,
        populate_existing=True,
    )
    if run is None or run.plan_id != expected_plan_id:
        raise HTTPException(409, "Worker Plan Run mirror changed before cancellation")
    plan = await db.get(
        Plan,
        run.plan_id,
        with_for_update=True,
        populate_existing=True,
    )
    if plan is None:
        raise HTTPException(409, "Worker Plan Run mirror changed before cancellation")
    if run.status == "cancelled":
        await db.commit()
        await db.refresh(run)
        return run
    if (
        run.worker_id is None
        or plan.worker_id != run.worker_id
        or run.run_type == "capability"
        or run.capability_execution_id is not None
        or plan.active_run_id != run.id
        or run.status not in ACTIVE_RUN_STATUSES
        or run.instance_id is not None
        or await db.scalar(
            select(Instance.id)
            .where(Instance.current_plan_run_id == run.id)
            .limit(1)
        )
        is not None
    ):
        raise HTTPException(409, "Worker Plan Run mirror is not safe to cancel")
    observed_worker_id = run.worker_id
    observed_generation = run.generation
    observed_status = run.status
    expected_plan_lock_version = plan.lock_version
    observed_input_request_id = run.open_input_request_id

    # The Worker ACK proves only the exact generation observed above. End the
    # SELECT/FOR UPDATE snapshot before taking the portable writer fence:
    # SQLite WAL ignores FOR UPDATE and cannot upgrade a stale read snapshot
    # after another connection commits. Unlike local cancellation, a newer
    # Worker mirror generation must never be absorbed; the caller must rebuild
    # its remote proof from scratch.
    await db.rollback()
    now = datetime.utcnow()
    run_fenced = await db.execute(
        update(PlanAgentRun)
        .where(
            PlanAgentRun.id == expected_run_id,
            PlanAgentRun.plan_id == expected_plan_id,
            PlanAgentRun.worker_id == observed_worker_id,
            PlanAgentRun.run_type != "capability",
            PlanAgentRun.capability_execution_id.is_(None),
            PlanAgentRun.instance_id.is_(None),
            PlanAgentRun.status == observed_status,
            PlanAgentRun.generation == observed_generation,
            (
                PlanAgentRun.open_input_request_id.is_(None)
                if observed_input_request_id is None
                else PlanAgentRun.open_input_request_id
                == observed_input_request_id
            ),
        )
        .values(updated_at=now)
    )
    if run_fenced.rowcount != 1:
        await db.rollback()
        raise HTTPException(
            409,
            "Worker Plan Run generation changed after cancellation proof",
        )
    plan_fenced = await db.execute(
        update(Plan)
        .where(
            Plan.id == expected_plan_id,
            Plan.active_run_id == expected_run_id,
            Plan.worker_id == observed_worker_id,
            Plan.lock_version == expected_plan_lock_version,
        )
        .values(
            active_run_id=None,
            lock_version=Plan.lock_version + 1,
            updated_at=now,
        )
    )
    if plan_fenced.rowcount != 1:
        await db.rollback()
        raise HTTPException(
            409,
            "Worker Plan changed after cancellation proof",
        )

    # Canonical deletion/dispatch ordering is Run -> Plan -> boundary receipt.
    # The two writes above now hold that boundary through receipt settlement.
    run = await db.get(
        PlanAgentRun,
        expected_run_id,
        populate_existing=True,
    )
    plan = await db.get(
        Plan,
        expected_plan_id,
        populate_existing=True,
    )
    if (
        run is None
        or plan is None
        or run.generation != observed_generation
        or run.status != observed_status
        or run.worker_id != observed_worker_id
        or plan.active_run_id is not None
        or await db.scalar(
            select(Instance.id)
            .where(Instance.current_plan_run_id == expected_run_id)
            .limit(1)
        )
        is not None
    ):
        await db.rollback()
        raise HTTPException(409, "Worker Plan Run changed while cancelling")

    dispatch_receipt = (
        await db.execute(
            select(PlanAgentWorkerDispatchReceipt)
            .where(
                PlanAgentWorkerDispatchReceipt.run_id == expected_run_id,
                PlanAgentWorkerDispatchReceipt.run_generation
                == observed_generation,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if dispatch_receipt is not None:
        from backend.services.worker_plan_dispatch import (
            settle_worker_dispatch_receipt,
        )

        settle_worker_dispatch_receipt(
            receipt=dispatch_receipt,
            plan=plan,
            run=run,
            generation=observed_generation,
            reason=receipt_settlement_reason,
            remote_status=receipt_remote_status,
        )

    # Input is always the final aggregate lock tier. An answer first fences the
    # Run, so exactly one of answer/cancel can reach this row.
    if observed_input_request_id is not None:
        input_fenced = await db.execute(
            update(PlanInputRequest)
            .where(
                PlanInputRequest.id == observed_input_request_id,
                PlanInputRequest.plan_id == expected_plan_id,
                PlanInputRequest.run_id == expected_run_id,
                PlanInputRequest.status.in_(["prepared", "open"]),
            )
            .values(status="cancelled", cancelled_at=now)
        )
        if input_fenced.rowcount != 1:
            await db.rollback()
            raise HTTPException(
                409,
                "Worker Plan input changed after cancellation proof",
            )

    execution_seconds = float(run.execution_seconds or 0)
    if run.last_execution_started_at is not None:
        execution_seconds += max(
            0.0,
            (now - run.last_execution_started_at).total_seconds(),
        )
    changed = await db.execute(
        update(PlanAgentRun)
        .where(
            PlanAgentRun.id == expected_run_id,
            PlanAgentRun.plan_id == expected_plan_id,
            PlanAgentRun.worker_id == observed_worker_id,
            PlanAgentRun.status == observed_status,
            PlanAgentRun.generation == observed_generation,
        )
        .values(
            status="cancelled",
            open_input_request_id=None,
            execution_seconds=execution_seconds,
            last_execution_started_at=None,
            cancellation_target_generation=observed_generation,
            generation=observed_generation + 1,
            error="Cancelled by user",
            updated_at=now,
            finished_at=now,
        )
    )
    if changed.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Worker Plan Run changed while cancelling")
    await db.commit()
    cancelled = await db.get(
        PlanAgentRun,
        expected_run_id,
        populate_existing=True,
    )
    if cancelled is None:
        raise HTTPException(409, "Worker Plan Run disappeared after cancellation")
    return cancelled


async def fence_capability_run_cancellation(
    db: AsyncSession,
    *,
    plan: Plan,
    run: PlanAgentRun,
) -> PlanAgentRun:
    """Durably stop a Capability Plan Run from accepting new work.

    The intermediate ``cancelling`` state intentionally keeps both the Plan's
    active-run slot and the last Instance id.  A failed process stop can then
    be retried without losing ownership evidence, while the Plan dispatcher
    no longer sees a claimable queued/running/waiting Run.
    """

    run_id = run.id
    plan_id = plan.id
    capability_execution_id = run.capability_execution_id
    for attempt in range(LOCAL_CANCELLATION_FENCE_ATTEMPTS):
        if (
            run.run_type != "capability"
            or capability_execution_id is None
            or run.capability_execution_id != capability_execution_id
            or run.plan_id != plan.id
        ):
            raise HTTPException(409, "Capability Plan Run identity changed")
        if run.status == "cancelling":
            if (
                plan.active_run_id != run.id
                or run.cancellation_target_generation is None
                or run.generation != run.cancellation_target_generation + 1
            ):
                raise HTTPException(
                    409,
                    "Plan Run cancellation fence is inconsistent",
                )
            await db.commit()
            await db.refresh(run)
            return run
        if run.status not in ACTIVE_RUN_STATUSES:
            if run.status in TERMINAL_RUN_STATUSES:
                await db.commit()
                await db.refresh(run)
                return run
            raise HTTPException(409, "Plan Run cannot enter cancellation")
        if plan.active_run_id != run.id:
            raise HTTPException(409, "Plan Run is no longer active")

        expected_plan_lock_version = plan.lock_version
        expected_input_request_id = run.open_input_request_id
        # See cancel_run: Run must be the first writer in the fresh transaction
        # and every Plan mutation uses Run -> Plan -> children/Input order.
        await db.rollback()
        now = datetime.utcnow()
        changed = await db.execute(
            update(PlanAgentRun)
            .where(
                PlanAgentRun.id == run_id,
                PlanAgentRun.plan_id == plan_id,
                PlanAgentRun.worker_id.is_(None),
                PlanAgentRun.run_type == "capability",
                PlanAgentRun.capability_execution_id == capability_execution_id,
                PlanAgentRun.status.in_(ACTIVE_RUN_STATUSES),
                (
                    PlanAgentRun.open_input_request_id.is_(None)
                    if expected_input_request_id is None
                    else PlanAgentRun.open_input_request_id
                    == expected_input_request_id
                ),
            )
            .ordered_values(
                (
                    PlanAgentRun.cancellation_target_generation,
                    PlanAgentRun.generation,
                ),
                (PlanAgentRun.generation, PlanAgentRun.generation + 1),
                (PlanAgentRun.status, "cancelling"),
                (PlanAgentRun.open_input_request_id, None),
                (PlanAgentRun.error, "Cancellation requested"),
                (PlanAgentRun.updated_at, now),
            )
        )
        plan_changed = None
        if changed is not None and changed.rowcount == 1:
            plan_changed = await db.execute(
                update(Plan)
                .where(
                    Plan.id == plan_id,
                    Plan.active_run_id == run_id,
                    Plan.lock_version == expected_plan_lock_version,
                )
                .values(
                    lock_version=Plan.lock_version + 1,
                    updated_at=now,
                )
            )
        input_changed = None
        if (
            changed.rowcount == 1
            and plan_changed is not None
            and plan_changed.rowcount == 1
            and expected_input_request_id is not None
        ):
            input_changed = await db.execute(
                update(PlanInputRequest)
                .where(
                    PlanInputRequest.id == expected_input_request_id,
                    PlanInputRequest.run_id == run_id,
                    PlanInputRequest.status.in_(["prepared", "open"]),
                )
                .values(status="cancelled", cancelled_at=now)
            )
        input_won = expected_input_request_id is None or (
            input_changed is not None and input_changed.rowcount == 1
        )
        if (
            changed.rowcount == 1
            and plan_changed is not None
            and plan_changed.rowcount == 1
            and input_won
        ):
            await db.commit()
            fenced = await db.get(
                PlanAgentRun,
                run_id,
                populate_existing=True,
            )
            if fenced is None:
                raise HTTPException(
                    409,
                    "Plan Run disappeared after fencing cancellation",
                )
            return fenced

        await db.rollback()
        if attempt + 1 >= LOCAL_CANCELLATION_FENCE_ATTEMPTS:
            break
        run = await db.get(
            PlanAgentRun,
            run_id,
            populate_existing=True,
        )
        plan = await db.get(
            Plan,
            plan_id,
            populate_existing=True,
        )
        if run is None or plan is None or run.plan_id != plan.id:
            await db.rollback()
            raise HTTPException(409, "Plan Run changed while fencing cancellation")

    await db.rollback()
    raise HTTPException(409, "Plan Run changed while fencing cancellation")


async def release_capability_run_owner_after_cleanup(
    db: AsyncSession,
    *,
    plan: Plan,
    run: PlanAgentRun,
) -> PlanAgentRun:
    """Converge only a fenced Capability owner with clean runtime receipts.

    A crash may occur after either side of an older owner-release path was
    committed.  Exact generation receipts let us repair one-way ownership, but
    duplicate/mismatched reverse owners remain fail-closed audit evidence.
    """

    cancellation_pending = run.status == "cancelling"
    cancellation_published = run.status == "cancelled"
    if (
        not (cancellation_pending or cancellation_published)
        or run.run_type != "capability"
        or run.capability_execution_id is None
        or run.cancellation_target_generation is None
        or run.generation != run.cancellation_target_generation + 1
        or run.plan_id != plan.id
        or (
            cancellation_pending
            and plan.active_run_id != run.id
        )
        or (
            cancellation_published
            and plan.active_run_id is not None
        )
    ):
        raise HTTPException(409, "Plan Run cancellation owner is inconsistent")

    from backend.services.plan_runtime_receipt import runtime_generation_is_clean

    if not await runtime_generation_is_clean(
        db,
        run_id=run.id,
        generation=run.cancellation_target_generation,
    ):
        raise HTTPException(
            409,
            "Plan Run runtime cleanup is not durably confirmed",
        )

    reverse_owners = list(
        (
            await db.execute(
                select(Instance)
                .where(Instance.current_plan_run_id == run.id)
                .order_by(Instance.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )
    if len(reverse_owners) > 1:
        raise HTTPException(409, "Plan Run has duplicate Instance owners")
    reverse_owner = reverse_owners[0] if reverse_owners else None
    if cancellation_published and (
        reverse_owner is not None
        or run.instance_id is not None
        or run.last_execution_started_at is not None
    ):
        # The current state machine publishes ``cancelled`` only after owner
        # release.  A terminal row retaining either direction is therefore
        # corruption, not an interrupted release that may be repaired.
        raise HTTPException(409, "Cancelled Plan Run retained a runtime owner")
    if reverse_owner is not None:
        if reverse_owner.current_task_id is not None or reverse_owner.pid is not None:
            raise HTTPException(
                409,
                f"Plan Run #{run.id} still owns live Instance #{reverse_owner.id}",
            )
        if (
            (run.instance_id is not None and run.instance_id != reverse_owner.id)
            or reverse_owner.status not in {"running", "idle"}
        ):
            raise HTTPException(409, "Plan Run Instance owner is not safe to release")
        reverse_owner.current_plan_run_id = None
        reverse_owner.status = "idle"
    elif run.instance_id is not None:
        # Lock the named row if it still exists.  It may already have been
        # released/reused after an older transaction committed the reverse
        # side first; clean receipts make clearing only the stale Run pointer
        # safe without mutating the row's newer owner.
        await db.get(Instance, run.instance_id, with_for_update=True)

    if cancellation_published:
        await db.commit()
        await db.refresh(run)
        return run

    now = datetime.utcnow()
    if run.last_execution_started_at is not None:
        run.execution_seconds = float(run.execution_seconds or 0) + max(
            0.0,
            (now - run.last_execution_started_at).total_seconds(),
        )
        run.last_execution_started_at = None
    run.instance_id = None
    running_steps = list(
        (
            await db.execute(
                select(PlanAgentStep).where(
                    PlanAgentStep.run_id == run.id,
                    PlanAgentStep.generation == run.cancellation_target_generation,
                    PlanAgentStep.status == "running",
                )
            )
        ).scalars()
    )
    for step in running_steps:
        step.status = "cancelled"
        step.error = "Cancelled by user"
        step.finished_at = now
    run.updated_at = now
    await db.commit()
    await db.refresh(run)
    return run


async def finalize_capability_run_cancellation(
    db: AsyncSession,
    *,
    plan: Plan,
    run: PlanAgentRun,
) -> PlanAgentRun:
    """Publish cancelled only after the adapter proves runtime cleanup."""

    if run.status == "cancelled":
        if (
            run.run_type != "capability"
            or run.capability_execution_id is None
            or run.cancellation_target_generation is None
            or run.generation != run.cancellation_target_generation + 1
            or plan.active_run_id is not None
        ):
            raise HTTPException(409, "Published Plan cancellation is inconsistent")
        from backend.services.plan_runtime_receipt import runtime_generation_is_clean

        reverse_owner = await db.scalar(
            select(Instance.id)
            .where(Instance.current_plan_run_id == run.id)
            .limit(1)
        )
        if (
            run.instance_id is not None
            or run.last_execution_started_at is not None
            or reverse_owner is not None
            or not await runtime_generation_is_clean(
                db,
                run_id=run.id,
                generation=run.cancellation_target_generation,
            )
        ):
            raise HTTPException(
                409,
                "Published Plan cancellation lost its runtime cleanup proof",
            )
        await db.commit()
        await db.refresh(run)
        return run
    if run.status != "cancelling" or plan.active_run_id != run.id:
        if run.status in {"completed", "failed"}:
            await db.commit()
            await db.refresh(run)
            return run
        raise HTTPException(409, "Plan Run cancellation is not pending")
    if run.cancellation_target_generation is None:
        raise HTTPException(409, "Plan Run cancellation runtime generation is missing")
    from backend.services.plan_runtime_receipt import runtime_generation_is_clean

    reverse_owner = await db.scalar(
        select(Instance.id)
        .where(Instance.current_plan_run_id == run.id)
        .limit(1)
    )
    if (
        run.instance_id is not None
        or run.last_execution_started_at is not None
        or reverse_owner is not None
        or not await runtime_generation_is_clean(
            db,
            run_id=run.id,
            generation=run.cancellation_target_generation,
        )
    ):
        raise HTTPException(
            409,
            "Plan Run runtime cleanup is not durably confirmed",
        )
    now = datetime.utcnow()
    execution_seconds = float(run.execution_seconds or 0)
    if run.last_execution_started_at is not None:
        execution_seconds += max(
            0.0,
            (now - run.last_execution_started_at).total_seconds(),
        )
    changed = await db.execute(
        update(PlanAgentRun)
        .where(
            PlanAgentRun.id == run.id,
            PlanAgentRun.plan_id == plan.id,
            PlanAgentRun.status == "cancelling",
        )
        .values(
            status="cancelled",
            instance_id=None,
            execution_seconds=execution_seconds,
            last_execution_started_at=None,
            # Preserve exact G after terminal publication.  If CCM crashes
            # before CapabilityExecution is marked cancelled, the adapter can
            # prove the already-published Run terminal from G's cleaned
            # receipt instead of guessing or replaying provider work.
            cancellation_target_generation=run.cancellation_target_generation,
            error="Cancelled by user",
            updated_at=now,
            finished_at=now,
        )
    )
    released = await db.execute(
        update(Plan)
        .where(Plan.id == plan.id, Plan.active_run_id == run.id)
        .values(
            active_run_id=None,
            lock_version=Plan.lock_version + 1,
            updated_at=now,
        )
    )
    if changed.rowcount != 1 or released.rowcount != 1:
        await db.rollback()
        raise HTTPException(409, "Plan Run changed while finalizing cancellation")
    await db.commit()
    await db.refresh(run)
    return run


async def resolve_legacy_task(
    db: AsyncSession, task_id: int
) -> PlanLegacyTaskLink | None:
    return await db.get(PlanLegacyTaskLink, task_id)


async def approved_versions_for_message(
    db: AsyncSession,
    *,
    target,
    version_ids: list[int] | None,
    confirmed_stale_version_ids: list[int] | None = None,
) -> list[tuple[Plan, PlanVersion]]:
    """Resolve exact approved Versions in caller order for one chat turn."""

    raw_ids = version_ids or []
    ids: list[int] = []
    for value in raw_ids:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError("plan_version_ids must contain positive integers")
        if value in ids:
            raise ValueError("plan_version_ids must not contain duplicates")
        ids.append(value)
    if not ids:
        return []
    versions = {
        row.id: row
        for row in (
            await db.execute(select(PlanVersion).where(PlanVersion.id.in_(ids)))
        ).scalars()
    }
    plan_ids = {row.plan_id for row in versions.values()}
    plans = {
        row.id: row
        for row in (
            await db.execute(select(Plan).where(Plan.id.in_(plan_ids)))
        ).scalars()
    }
    confirmed = set(confirmed_stale_version_ids or [])
    from backend.services.plan_staleness import version_staleness

    result: list[tuple[Plan, PlanVersion]] = []
    for version_id in ids:
        version = versions.get(version_id)
        plan = plans.get(version.plan_id) if version is not None else None
        if version is None or plan is None:
            raise ValueError(f"Plan Version #{version_id} was not found")
        if plan.archived_at is not None:
            raise ValueError(
                f"Plan Version #{version_id} belongs to an archived Plan"
            )
        if plan.target_task_id != target.id:
            raise ValueError(
                f"Plan Version #{version_id} is not associated with Task #{target.id}"
            )
        if version.human_decision != "approved" or not version.content:
            raise ValueError(f"Plan Version #{version_id} is not approved and ready")
        applied = await db.scalar(
            select(PlanApplication.id)
            .where(PlanApplication.plan_version_id == version.id)
            .limit(1)
        )
        if applied is not None:
            raise ValueError(f"Plan Version #{version_id} has already been applied")
        staleness = await version_staleness(db, plan, version)
        if staleness["hard_conflict"]:
            error = ValueError(
                f"Plan Version #{version.id} has a non-bypassable target conflict"
            )
            setattr(error, "staleness", staleness)
            setattr(error, "plan_version_id", version.id)
            raise error
        if staleness["stale"] and version.id not in confirmed:
            error = ValueError(
                f"Plan Version #{version.id} context changed; confirm stale application"
            )
            setattr(error, "staleness", staleness)
            setattr(error, "plan_version_id", version.id)
            raise error
        result.append((plan, version))
    return result


def versioned_plan_snapshots(
    approved: list[tuple[Plan, PlanVersion]],
) -> list[dict[str, object]]:
    return [
        {
            # Legacy display readers require id/title/content. ``id`` remains
            # the stable Plan id while the new fields preserve exact identity.
            "id": plan.id,
            "plan_id": plan.id,
            "version_id": version.id,
            "version_number": version.version_number,
            "title": plan.title or f"Plan #{plan.id}",
            "content": version.content,
        }
        for plan, version in approved
    ]


def build_versioned_plan_prompt(
    approved: list[tuple[Plan, PlanVersion]], user_prompt: str
) -> str:
    if not approved:
        return user_prompt
    parts = [
        "[Approved Plan Versions explicitly selected by the user for this turn]",
        (
            "The Versions below are immutable context for the current instruction. "
            "Approval alone grants no permission beyond that instruction."
        ),
    ]
    for plan, version in approved:
        parts.append(
            f'<approved_plan plan_id="{plan.id}" version_id="{version.id}" '
            f'version="{version.version_number}">\n{version.content}\n</approved_plan>'
        )
    parts.extend(["[User instruction for this turn]", user_prompt])
    return "\n\n".join(parts)


async def _version_resource(
    db: AsyncSession, version: PlanVersion | None
) -> PlanVersionResource | None:
    if version is None:
        return None
    applied = (
        await db.scalar(
            select(PlanApplication.id)
            .where(PlanApplication.plan_version_id == version.id)
            .limit(1)
        )
        is not None
    )
    if applied:
        display_state = "applied"
    elif version.human_decision == "rejected":
        display_state = "rejected"
    elif version.human_decision == "approved":
        display_state = "approved"
    elif version.superseded_by_version_id is not None:
        display_state = "superseded"
    elif (
        version.review_verdict in {"approve", "disabled", "exhausted"}
        or version.review_exhausted
    ):
        display_state = "awaiting_review"
    else:
        display_state = "draft"
    return PlanVersionResource.model_validate(version).model_copy(
        update={"applied": applied, "display_state": display_state}
    )


async def _application_resources(
    db: AsyncSession,
    applications: list[PlanApplication],
) -> list[PlanApplicationResource]:
    execution_task_ids = {
        item.execution_task_id
        for item in applications
        if item.application_type == "execution_task"
        and item.execution_task_id is not None
    }
    available_execution_task_ids = (
        set(
            (
                await db.execute(select(Task.id).where(Task.id.in_(execution_task_ids)))
            ).scalars()
        )
        if execution_task_ids
        else set()
    )
    receipt_keys = {
        item.application_receipt_key
        for item in applications
        if item.application_receipt_key is not None
    }
    receipts = (
        {
            row.receipt_key: row
            for row in (
                await db.execute(
                    select(PlanApplicationReceipt).where(
                        PlanApplicationReceipt.receipt_key.in_(receipt_keys)
                    )
                )
            ).scalars()
        }
        if receipt_keys
        else {}
    )
    return [
        PlanApplicationResource.model_validate(item).model_copy(
            update={
                "execution_task_available": (
                    item.execution_task_id in available_execution_task_ids
                    if item.application_type == "execution_task"
                    and item.execution_task_id is not None
                    else None
                ),
                "delivery_status": (
                    receipts[item.application_receipt_key].delivery_status
                    if item.application_receipt_key in receipts
                    else None
                ),
                "delivery_error": (
                    receipts[item.application_receipt_key].delivery_error
                    if item.application_receipt_key in receipts
                    else None
                ),
                "launch_evidence": (
                    receipts[item.application_receipt_key].launch_evidence
                    if item.application_receipt_key in receipts
                    else None
                ),
                "delivery_resolution": (
                    receipts[item.application_receipt_key].delivery_resolution
                    if item.application_receipt_key in receipts
                    else None
                ),
            }
        )
        for item in applications
    ]


async def _application_attempt_resources(
    db: AsyncSession,
    attempts: list[PlanApplicationAttempt],
) -> list[PlanApplicationAttemptResource]:
    receipt_keys = {item.application_receipt_key for item in attempts}
    receipts = {
        row.receipt_key: row
        for row in (
            await db.execute(
                select(PlanApplicationReceipt).where(
                    PlanApplicationReceipt.receipt_key.in_(receipt_keys)
                )
            )
        ).scalars()
    }
    return [
        PlanApplicationAttemptResource.model_validate(item).model_copy(
            update={
                "delivery_status": (
                    receipts[item.application_receipt_key].delivery_status
                    if item.application_receipt_key in receipts
                    else "missing"
                ),
                "delivery_error": (
                    receipts[item.application_receipt_key].delivery_error
                    if item.application_receipt_key in receipts
                    else "Plan application receipt is missing"
                ),
                "launch_evidence": (
                    receipts[item.application_receipt_key].launch_evidence
                    if item.application_receipt_key in receipts
                    else None
                ),
                "delivery_resolution": (
                    receipts[item.application_receipt_key].delivery_resolution
                    if item.application_receipt_key in receipts
                    else None
                ),
            }
        )
        for item in attempts
    ]


async def _run_resource(
    db: AsyncSession, run: PlanAgentRun | None, *, include_audit: bool = False
) -> PlanRunResource | None:
    if run is None:
        return None
    steps: list[PlanStepResource] = []
    inputs: list[PlanInputRequestResponse] = []
    if include_audit:
        steps = [
            PlanStepResource.model_validate(row)
            for row in (
                await db.execute(
                    select(PlanAgentStep)
                    .where(PlanAgentStep.run_id == run.id)
                    .order_by(PlanAgentStep.id)
                )
            ).scalars()
        ]
        inputs = [
            input_request_resource(row)
            for row in (
                await db.execute(
                    select(PlanInputRequest)
                    .where(PlanInputRequest.run_id == run.id)
                    .order_by(PlanInputRequest.id)
                )
            ).scalars()
        ]
    return PlanRunResource.model_validate(run).model_copy(
        update={"steps": steps, "input_requests": inputs}
    )


async def plan_resource(
    db: AsyncSession, plan: Plan, *, include_audit: bool = False
) -> PlanResource:
    resources = await plan_resources(db, [plan], include_audit=include_audit)
    return resources[0]


async def plan_resources(
    db: AsyncSession,
    plans: list[Plan],
    *,
    include_audit: bool = False,
) -> list[PlanResource]:
    """Build a Plan list with a bounded set of aggregate preload queries."""

    if not plans:
        return []
    plan_ids = [plan.id for plan in plans]
    delivery_rows = list(
        (
            await db.execute(
                select(DeliveryCycle.run_id, PlanVersion.plan_id)
                .join(PlanVersion, PlanVersion.id == DeliveryCycle.plan_version_id)
                .where(PlanVersion.plan_id.in_(plan_ids))
                .order_by(DeliveryCycle.id.desc())
            )
        ).all()
    )
    delivery_run_by_plan: dict[int, int] = {}
    for delivery_run_id, plan_id in delivery_rows:
        delivery_run_by_plan.setdefault(plan_id, delivery_run_id)
    target_task_ids = {
        plan.target_task_id for plan in plans if plan.target_task_id is not None
    }
    if target_task_ids:
        task_delivery_rows = list(
            (
                await db.execute(
                    select(Task.id, Task.delivery_run_id).where(
                        Task.id.in_(target_task_ids),
                        Task.delivery_run_id.is_not(None),
                    )
                )
            ).all()
        )
        delivery_run_by_task = {
            task_id: delivery_run_id
            for task_id, delivery_run_id in task_delivery_rows
            if delivery_run_id is not None
        }
        for plan in plans:
            if plan.target_task_id is not None:
                delivery_run_id = delivery_run_by_task.get(plan.target_task_id)
                if delivery_run_id is not None:
                    delivery_run_by_plan.setdefault(plan.id, delivery_run_id)
    version_ids = {
        plan.current_version_id for plan in plans if plan.current_version_id is not None
    }
    active_run_ids = {
        plan.active_run_id for plan in plans if plan.active_run_id is not None
    }
    versions = (
        {
            row.id: row
            for row in (
                await db.execute(
                    select(PlanVersion).where(PlanVersion.id.in_(version_ids))
                )
            ).scalars()
        }
        if version_ids
        else {}
    )
    active_runs = (
        {
            row.id: row
            for row in (
                await db.execute(
                    select(PlanAgentRun).where(PlanAgentRun.id.in_(active_run_ids))
                )
            ).scalars()
        }
        if active_run_ids
        else {}
    )
    latest_run_ids = (
        select(func.max(PlanAgentRun.id))
        .where(PlanAgentRun.plan_id.in_(plan_ids))
        .group_by(PlanAgentRun.plan_id)
    )
    latest_runs = {
        row.plan_id: row
        for row in (
            await db.execute(
                select(PlanAgentRun).where(PlanAgentRun.id.in_(latest_run_ids))
            )
        ).scalars()
    }
    capability_owned_plan_ids = set(
        (
            await db.execute(
                select(PlanAgentRun.plan_id).where(
                    PlanAgentRun.plan_id.in_(plan_ids),
                    or_(
                        PlanAgentRun.run_type == "capability",
                        PlanAgentRun.capability_execution_id.is_not(None),
                    ),
                )
            )
        ).scalars()
    )
    open_input_ids = {
        run.open_input_request_id
        for run in active_runs.values()
        if run.open_input_request_id is not None
    }
    open_inputs = (
        {
            row.id: row
            for row in (
                await db.execute(
                    select(PlanInputRequest).where(
                        PlanInputRequest.id.in_(open_input_ids)
                    )
                )
            ).scalars()
        }
        if open_input_ids
        else {}
    )
    applications = list(
        (
            await db.execute(
                select(PlanApplication)
                .where(PlanApplication.plan_id.in_(plan_ids))
                .order_by(
                    PlanApplication.plan_id,
                    PlanApplication.created_at,
                    PlanApplication.id,
                )
            )
        ).scalars()
    )
    applications_by_plan: defaultdict[int, list[PlanApplication]] = defaultdict(list)
    for application in applications:
        applications_by_plan[application.plan_id].append(application)
    application_resources = await _application_resources(db, applications)
    application_resource_by_id = {item.id: item for item in application_resources}
    application_attempts = (
        list(
            (
                await db.execute(
                    select(PlanApplicationAttempt)
                    .where(PlanApplicationAttempt.plan_id.in_(plan_ids))
                    .order_by(
                        PlanApplicationAttempt.plan_id,
                        PlanApplicationAttempt.released_at,
                        PlanApplicationAttempt.id,
                    )
                )
            ).scalars()
        )
        if include_audit
        else []
    )
    application_attempts_by_plan: defaultdict[int, list[PlanApplicationAttempt]] = (
        defaultdict(list)
    )
    for attempt in application_attempts:
        application_attempts_by_plan[attempt.plan_id].append(attempt)
    application_attempt_resources = (
        await _application_attempt_resources(
            db,
            application_attempts,
        )
        if application_attempts
        else []
    )
    application_attempt_resource_by_id = {
        item.id: item for item in application_attempt_resources
    }
    applied_version_ids = {item.plan_version_id for item in applications}
    legacy_plan_ids = set(
        (
            await db.execute(
                select(PlanLegacyTaskLink.plan_id).where(
                    PlanLegacyTaskLink.plan_id.in_(plan_ids)
                )
            )
        ).scalars()
    )

    steps_by_run: defaultdict[int, list[PlanStepResource]] = defaultdict(list)
    inputs_by_run: defaultdict[int, list[PlanInputRequestResponse]] = defaultdict(list)
    if include_audit and active_run_ids:
        for row in (
            await db.execute(
                select(PlanAgentStep)
                .where(PlanAgentStep.run_id.in_(active_run_ids))
                .order_by(PlanAgentStep.run_id, PlanAgentStep.id)
            )
        ).scalars():
            steps_by_run[row.run_id].append(PlanStepResource.model_validate(row))
        for row in (
            await db.execute(
                select(PlanInputRequest)
                .where(PlanInputRequest.run_id.in_(active_run_ids))
                .order_by(PlanInputRequest.run_id, PlanInputRequest.id)
            )
        ).scalars():
            inputs_by_run[row.run_id].append(input_request_resource(row))

    result: list[PlanResource] = []
    for plan in plans:
        current = versions.get(plan.current_version_id)
        active = active_runs.get(plan.active_run_id)
        latest = latest_runs.get(plan.id)
        capability_owned = plan.id in capability_owned_plan_ids or bool(
            active is not None
            and (
                active.run_type == "capability"
                or active.capability_execution_id is not None
            )
        )
        plan_applications = applications_by_plan[plan.id]
        current_application = next(
            (
                item
                for item in plan_applications
                if current is not None and item.plan_version_id == current.id
            ),
            None,
        )
        if plan.archived_at is not None:
            display_state = "archived"
        elif active is not None and active.status == "waiting_user":
            display_state = "waiting_user"
        elif active is not None and active.status == "cancelling":
            display_state = "cancelling"
        elif active is not None and active.status in {"queued", "running"}:
            display_state = active.current_stage or "running"
        elif current_application is not None:
            display_state = "applied"
        elif current is not None and current.human_decision == "approved":
            display_state = "approved"
        elif current is not None and current.human_decision == "rejected":
            display_state = "rejected"
        elif current is not None and (
            current.review_verdict in {"approve", "disabled", "exhausted"}
            or current.review_exhausted
        ):
            display_state = "awaiting_review"
        elif latest is not None and latest.status in {"failed", "cancelled"}:
            display_state = latest.status
        else:
            display_state = "draft"
        payload = {
            column: getattr(plan, column)
            for column in (
                "id",
                "title",
                "initial_request",
                "initial_attachments",
                "target_task_id",
                "project_id",
                "target_repo",
                "target_branch",
                "worker_id",
                "priority",
                "timeout_hours",
                "created_by",
                "pipeline_config",
                "current_version_id",
                "active_run_id",
                "forked_from_version_id",
                "archived_at",
                "closed_at",
                "lock_version",
                "created_at",
                "updated_at",
            )
        }
        payload["initial_attachments"] = _public_attachments(plan.initial_attachments)
        current_resource = None
        if current is not None:
            if current.id in applied_version_ids:
                version_state = "applied"
            elif current.human_decision == "rejected":
                version_state = "rejected"
            elif current.human_decision == "approved":
                version_state = "approved"
            elif current.superseded_by_version_id is not None:
                version_state = "superseded"
            elif (
                current.review_verdict in {"approve", "disabled", "exhausted"}
                or current.review_exhausted
            ):
                version_state = "awaiting_review"
            else:
                version_state = "draft"
            current_resource = PlanVersionResource.model_validate(current).model_copy(
                update={
                    "applied": current.id in applied_version_ids,
                    "display_state": version_state,
                }
            )
        active_resource = None
        if active is not None:
            active_resource = PlanRunResource.model_validate(active).model_copy(
                update={
                    "steps": steps_by_run[active.id] if include_audit else [],
                    "input_requests": (
                        inputs_by_run[active.id] if include_audit else []
                    ),
                }
            )
        result.append(
            PlanResource(
                **payload,
                display_state=display_state,
                legacy=plan.id in legacy_plan_ids,
                ownership="capability" if capability_owned else "standard",
                read_only=capability_owned,
                delivery_run_id=delivery_run_by_plan.get(plan.id),
                latest_run_status=latest.status if latest else None,
                latest_run_error=(
                    latest.error
                    if latest is not None and latest.status == "failed"
                    else None
                ),
                application=(
                    application_resource_by_id.get(current_application.id)
                    if current_application is not None
                    else None
                ),
                applications=[
                    application_resource_by_id[item.id] for item in plan_applications
                ],
                application_attempts=[
                    application_attempt_resource_by_id[item.id]
                    for item in application_attempts_by_plan[plan.id]
                ],
                current_version=current_resource,
                active_run=active_resource,
                open_input_request=(
                    input_request_resource(open_inputs[active.open_input_request_id])
                    if active is not None
                    and active.open_input_request_id in open_inputs
                    else None
                ),
            )
        )
    return result


async def run_resource(
    db: AsyncSession, run: PlanAgentRun, *, include_audit: bool = True
) -> PlanRunResource:
    resource = await _run_resource(db, run, include_audit=include_audit)
    assert resource is not None
    return resource


async def version_resource(
    db: AsyncSession, version: PlanVersion
) -> PlanVersionResource:
    resource = await _version_resource(db, version)
    assert resource is not None
    return resource


async def apply_worker_plan_outcome(
    db: AsyncSession,
    *,
    plan: Plan,
    run: PlanAgentRun,
    worker_id: int,
    expected_generation: int,
    payload: dict,
    worker_dispatch_receipt_id: int | None = None,
    allow_cancelling_successor: bool = False,
) -> PlanAgentRun:
    """Import one exact Worker pause while keeping Manager ids authoritative."""

    if payload.get("protocol") != 3:
        raise RuntimeError("Worker Plan outcome protocol mismatch")
    base_worker_version_id = payload.get("base_worker_version_id")
    if isinstance(base_worker_version_id, bool) or (
        base_worker_version_id is not None
        and not isinstance(base_worker_version_id, int)
    ):
        raise RuntimeError("Worker Plan outcome has invalid base Version identity")
    raw_remote = payload.get("run")
    if isinstance(raw_remote, dict) and raw_remote.get("status") in {
        "completed",
        "failed",
        "cancelled",
    }:
        from backend.services.worker_plan_dispatch import (
            WorkerPlanDispatchConflict,
            validate_worker_terminal_outcome_graph,
        )

        try:
            validate_worker_terminal_outcome_graph(
                payload,
                plan_id=plan.id,
                run_id=run.id,
            )
        except WorkerPlanDispatchConflict as exc:
            raise RuntimeError(
                "Worker Plan terminal outcome graph is invalid"
            ) from exc
    manager_base = (
        await db.get(PlanVersion, run.base_version_id)
        if run.base_version_id is not None
        else None
    )
    is_fork = run.run_type == "fork"
    if run.base_version_id is not None and manager_base is None:
        raise RuntimeError(
            "Worker Plan outcome base Version does not match the Manager Run"
        )
    if (
        is_fork
        and (manager_base is None or base_worker_version_id is not None)
    ) or (
        not is_fork
        and ((manager_base is None) != (base_worker_version_id is None))
    ):
        raise RuntimeError(
            "Worker Plan outcome base Version does not match the Manager Run"
        )
    if (
        manager_base is not None
        and manager_base.plan_id != plan.id
        and run.run_type != "fork"
    ):
        raise RuntimeError("Plan Run base Version belongs to another Plan")
    remote = PlanRunResource.model_validate(payload.get("run"))
    remote_versions = [
        PlanVersionResource.model_validate(item) for item in payload.get("versions", [])
    ]
    if remote.status == "completed":
        remote_result = next(
            (
                item
                for item in remote_versions
                if item.id == remote.result_version_id
            ),
            None,
        )
        expected_version_number = (
            manager_base.version_number + 1
            if manager_base is not None and not is_fork
            else 1
        )
        if (
            remote_result is None
            or remote_result.version_number != expected_version_number
        ):
            raise RuntimeError(
                "Worker Plan result Version number does not extend its exact base"
            )
    ordinary_owner = run.status == "running" and run.generation == expected_generation
    cancelling_target_observation = bool(
        allow_cancelling_successor
        and remote.status == "cancelled"
        and run.status == "cancelling"
        and run.cancellation_target_generation == expected_generation
        and run.generation == expected_generation + 1
    )
    cancelling_successor_observation = bool(
        allow_cancelling_successor
        and remote.status == "cancelled"
        and run.status == "cancelling"
        and run.cancellation_target_generation == expected_generation - 1
        and run.generation == expected_generation
    )
    cancelling_successor = bool(
        cancelling_target_observation or cancelling_successor_observation
    )
    if (
        plan.worker_id != worker_id
        or run.worker_id != worker_id
        or plan.active_run_id != run.id
        or not (ordinary_owner or cancelling_successor)
        or remote.id != run.id
        or remote.plan_id != plan.id
        or remote.status not in {"waiting_user", "completed", "failed", "cancelled"}
    ):
        raise RuntimeError("Worker Plan outcome no longer owns this Run generation")
    dispatch_receipt = None
    if worker_dispatch_receipt_id is not None:
        dispatch_receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            worker_dispatch_receipt_id,
            with_for_update=True,
            populate_existing=True,
        )
        if dispatch_receipt is None:
            raise RuntimeError("Worker Plan dispatch receipt disappeared")

    step_by_remote: dict[int, PlanAgentStep] = {}
    for item in remote.steps:
        if item.run_id != remote.id or item.plan_id != plan.id:
            raise RuntimeError("Worker Plan Step belongs to another Run or Plan")
        step = (
            await db.execute(
                select(PlanAgentStep).where(
                    PlanAgentStep.worker_id == worker_id,
                    PlanAgentStep.worker_step_id == item.id,
                )
            )
        ).scalar_one_or_none()
        if step is None:
            step = PlanAgentStep(
                run_id=run.id,
                plan_id=plan.id,
                worker_id=worker_id,
                worker_step_id=item.id,
                generation=item.generation,
                step_type=item.step_type,
                round=item.round,
                provider=item.provider,
                model=item.model,
                effort=item.effort,
                route_slot=item.route_slot,
                status=item.status,
                output=item.output,
                error=item.error,
                last_delta_at=item.last_delta_at,
                streamed_output_chars=item.streamed_output_chars,
                last_event_type=item.last_event_type,
                started_at=item.started_at,
                finished_at=item.finished_at,
            )
            db.add(step)
            await db.flush()
        elif (
            step.run_id != run.id
            or step.plan_id != plan.id
            or step.step_type != item.step_type
            or step.round != item.round
            or step.generation != item.generation
            or step.provider != item.provider
            or step.model != item.model
            or step.effort != item.effort
            or step.route_slot != item.route_slot
            or step.status != item.status
            or step.output != item.output
            or step.error != item.error
            or step.last_delta_at != item.last_delta_at
            or step.streamed_output_chars != item.streamed_output_chars
            or step.last_event_type != item.last_event_type
        ):
            raise RuntimeError("Worker Plan Step mapping collides with another Run")
        step_by_remote[item.id] = step

    version_by_remote: dict[int, PlanVersion] = {}
    for item in sorted(remote_versions, key=lambda version: version.version_number):
        if item.plan_id != plan.id:
            raise RuntimeError("Worker Plan Version belongs to another Plan")
        version = (
            await db.execute(
                select(PlanVersion).where(
                    PlanVersion.worker_id == worker_id,
                    PlanVersion.worker_version_id == item.id,
                )
            )
        ).scalar_one_or_none()
        parent = (
            manager_base
            if item.parent_version_id is not None
            and item.parent_version_id == base_worker_version_id
            else version_by_remote.get(item.parent_version_id)
        )
        if item.parent_version_id is not None and parent is None:
            raise RuntimeError("Worker Plan Version parent was not imported")
        produced = step_by_remote.get(item.produced_by_step_id)
        reviewed = step_by_remote.get(item.reviewed_by_step_id)
        if version is None:
            version = PlanVersion(
                plan_id=plan.id,
                worker_id=worker_id,
                worker_version_id=item.id,
                version_number=item.version_number,
                parent_version_id=parent.id if parent is not None else None,
                produced_by_run_id=run.id,
                produced_by_step_id=produced.id if produced is not None else None,
                content=item.content,
                # Manager log/session ids are the authoritative staleness
                # coordinate; Worker-local ids are not comparable here.
                context_session_id=run.context_session_id,
                context_log_id=run.context_log_id,
                # Context snapshots remain Manager-owned and are deliberately
                # not exposed by the public Version resource protocol.
                context_snapshot=run.context_snapshot,
                repo_revision=item.repo_revision,
                reviewer_repo_revision=item.reviewer_repo_revision,
                human_decision="pending",
                created_at=item.created_at,
            )
            db.add(version)
            await db.flush()
            if (
                parent is manager_base
                and manager_base is not None
                and manager_base.plan_id == plan.id
                and manager_base.superseded_by_version_id is None
            ):
                manager_base.superseded_by_version_id = version.id
        elif (
            version.plan_id != plan.id
            or version.version_number != item.version_number
            or version.content != item.content
        ):
            raise RuntimeError("Worker Plan Version mapping changed immutable content")
        version.review_verdict = item.review_verdict
        version.review_feedback = item.review_feedback
        version.reviewed_by_step_id = reviewed.id if reviewed is not None else None
        version.review_exhausted = item.review_exhausted
        version.reviewed_at = item.reviewed_at
        version.reviewer_repo_revision = item.reviewer_repo_revision
        version_by_remote[item.id] = version
        if produced is not None:
            produced.plan_version_id = version.id

    for item in remote_versions:
        version = version_by_remote[item.id]
        successor = version_by_remote.get(item.superseded_by_version_id)
        if item.superseded_by_version_id is not None and successor is None:
            raise RuntimeError("Worker Plan Version successor was not imported")
        if successor is not None:
            version.superseded_by_version_id = successor.id

    input_by_remote: dict[int, PlanInputRequest] = {}
    for item in remote.input_requests:
        input_request = (
            await db.execute(
                select(PlanInputRequest).where(
                    PlanInputRequest.worker_id == worker_id,
                    PlanInputRequest.worker_input_request_id == item.id,
                )
            )
        ).scalar_one_or_none()
        source = step_by_remote.get(item.source_step_id)
        if source is None:
            raise RuntimeError("Worker InputRequest has no imported source Step")
        normalized_questions = [
            question.model_dump(mode="json") for question in item.questions
        ]
        if input_request is None:
            input_request = PlanInputRequest(
                plan_id=plan.id,
                run_id=run.id,
                worker_id=worker_id,
                worker_input_request_id=item.id,
                source_step_id=source.id,
                requested_by=item.requested_by,
                reason=item.reason,
                questions=normalized_questions,
                status=item.status,
                answers=item.answers,
                response_text=item.response_text,
                attachments=item.attachments,
                answered_by=item.answered_by,
                idempotency_key=f"worker:{worker_id}:input:{item.id}",
                opened_at=item.opened_at,
                answered_at=item.answered_at,
                created_at=item.created_at,
            )
            db.add(input_request)
            await db.flush()
        elif (
            input_request.run_id != run.id
            or input_request.plan_id != plan.id
            or input_request.source_step_id != source.id
            or input_request.requested_by != item.requested_by
            or input_request.reason != item.reason
            or input_request.questions != normalized_questions
            or input_request.opened_at != item.opened_at
            or input_request.created_at != item.created_at
        ):
            # Answer status/timestamps and attachment paths deliberately remain
            # Manager-owned: an answer can be durable locally before its exact
            # replay reaches the Worker, and Worker upload paths use another
            # namespace. The request identity and question graph are immutable
            # in both domains and must match on every readback.
            raise RuntimeError("Worker InputRequest mapping changed immutable content")
        input_by_remote[item.id] = input_request
        source.input_request_id = input_request.id

    latest = max(
        version_by_remote.values(),
        key=lambda version: version.version_number,
        default=None,
    )
    result_version = version_by_remote.get(remote.result_version_id)
    run.current_stage = remote.current_stage
    run.round = remote.round
    # Keep the Manager claim generation authoritative on the Manager. The
    # Worker generation only fences Worker-local execution/input operations.
    run.execution_seconds = remote.execution_seconds
    run.last_execution_started_at = None
    run.result_version_id = result_version.id if result_version is not None else None
    run.draft_content = remote.draft_content
    draft_step = step_by_remote.get(remote.draft_step_id)
    if remote.draft_step_id is not None and draft_step is None:
        raise RuntimeError("Worker Plan draft has no imported Planner Step")
    run.draft_step_id = draft_step.id if draft_step is not None else None
    run.draft_repo_revision = remote.draft_repo_revision
    run.interaction_count = remote.interaction_count
    run.review_verdict = remote.review_verdict
    run.review_feedback = remote.review_feedback
    run.review_exhausted = remote.review_exhausted
    run.error = remote.error
    run.updated_at = datetime.utcnow()
    if latest is not None:
        plan.current_version_id = latest.id

    if remote.status == "waiting_user":
        open_input = input_by_remote.get(remote.open_input_request_id)
        if open_input is None or open_input.status != "open":
            raise RuntimeError("Worker waiting Run has no exact open InputRequest")
        run.status = "waiting_user"
        run.open_input_request_id = open_input.id
    else:
        if remote.status == "completed" and result_version is None:
            raise RuntimeError("Worker completed Run has no exact result Version")
        run.status = remote.status
        run.open_input_request_id = None
        run.finished_at = remote.finished_at or datetime.utcnow()
        plan.active_run_id = None
    plan.lock_version += 1
    plan.updated_at = datetime.utcnow()
    if dispatch_receipt is not None:
        from backend.services.worker_plan_dispatch import (
            settle_worker_dispatch_receipt,
        )

        settle_worker_dispatch_receipt(
            receipt=dispatch_receipt,
            plan=plan,
            run=run,
            generation=expected_generation,
            reason="remote_pause",
            remote_status=remote.status,
            allow_cancelling_successor=cancelling_successor,
        )
    await db.commit()
    await db.refresh(run)
    return run
