"""ACL-scoped API for the autonomous Delivery Loop mode."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import get_current_user_id, require_project_access
from backend.config import settings
from backend.database import get_db
from backend.models.capability import CapabilityInvocation
from backend.models.delivery import (
    DELIVERY_ACTION_ACTIVE_STATUSES,
    DELIVERY_CYCLE_ACTIVE_STATUSES,
    DeliveryAction,
    DeliveryCycle,
    DeliveryRun,
    DeliveryTransition,
    DeliveryTurn,
)
from backend.models.task import Task
from backend.schemas.delivery import (
    DeliveryCommand,
    DeliveryResumeCommand,
    DeliveryRunCreate,
    DeliveryRunDetail,
    DeliveryRunResponse,
)
from backend.services.delivery_reducer import DeliveryReducerEvent
from backend.services.delivery_service import (
    DeliveryConflictError,
    DeliveryCreateSpec,
    DeliveryError,
    DeliveryNotFoundError,
    DeliveryUnsupportedScopeError,
    DeliveryUnavailableError,
    DeliveryValidationError,
    apply_run_event,
    complete_cycle,
    create_delivery_run,
    get_delivery_run,
    list_delivery_runs,
    lock_run,
)


router = APIRouter(prefix="/api/delivery-runs", tags=["delivery-runs"])


def _allowed_actions(
    run: DeliveryRun,
    *,
    has_active_controller_capability: bool = False,
    has_active_delivery_action: bool = False,
) -> list[str]:
    if run.activity == "terminal":
        return []
    # Controller reconciliation may be between its durable admission and the
    # matching state transition.  Commands must not cross that lease window:
    # doing so could terminalize a Run while its Capability/Git effect is
    # already committed but not yet bound.  The owner clears this field on
    # release; an expired crash lease is recovered by the always-on controller.
    if run.lease_owner is not None:
        return []
    if run.activity == "paused":
        # Cancellation never has a proof that a previously-started effect was
        # fenced.  Resume is the only safe path back through reconciliation.
        return ["resume"]
    if (
        run.phase in {"publishing", "monitoring"}
        or run.pr_number is not None
        or run.pr_monitor_run_id is not None
        or has_active_delivery_action
    ):
        return []
    if has_active_controller_capability:
        # A Capability may already be committed while the controller is still
        # between admission and binding its id to the current Cycle.  Keep the
        # UI from offering a command that the locked command fence must reject.
        return []
    if run.activity == "ready":
        return ["pause", "cancel"]
    # An active Developer turn, Git action, or other effect needs the
    # controller's exact-generation stop fence; V1 does not pretend that a
    # synchronous API response stopped it.
    return []


async def _has_active_controller_capability(
    db: AsyncSession,
    run: DeliveryRun,
) -> bool:
    if run.developer_task_id is None:
        return False
    invocation_id = await db.scalar(
        select(CapabilityInvocation.id)
        .where(
            CapabilityInvocation.active_task_id == run.developer_task_id,
            CapabilityInvocation.source == "delivery_controller",
        )
        .limit(1)
    )
    return invocation_id is not None


async def _has_active_delivery_action(
    db: AsyncSession,
    run: DeliveryRun,
) -> bool:
    action_id = await db.scalar(
        select(DeliveryAction.id)
        .where(
            DeliveryAction.run_id == run.id,
            DeliveryAction.status.in_(DELIVERY_ACTION_ACTIVE_STATUSES),
        )
        .limit(1)
    )
    return action_id is not None


async def _require_command_safe_state(
    db: AsyncSession,
    run: DeliveryRun,
    *,
    event_kind: str,
) -> None:
    """Fence commands that would race an active controller-owned effect.

    This check must run on the freshly locked row.  Checking the ACL snapshot
    before ``lock_run`` is insufficient: the controller may move a Run from
    ready to running between those reads, and a stale pause/cancel request
    would then bypass the exact-generation stop fence.
    """

    if run.lease_owner is not None:
        raise DeliveryConflictError(
            "Delivery Controller reconciliation is active; retry the command "
            "after its exact lease is released"
        )
    if event_kind in {"pause", "cancel"} and (
        run.phase in {"publishing", "monitoring"}
        or run.pr_number is not None
        or run.pr_monitor_run_id is not None
    ):
        raise DeliveryConflictError(
            "Published or monitored Delivery work cannot be paused or "
            "cancelled without an exact-generation PR/Monitor side-effect fence"
        )
    if run.activity == "running" or (
        run.activity == "waiting" and run.phase != "monitoring"
    ):
        raise DeliveryConflictError(
            "An active Delivery effect must finish or be stopped through its "
            "exact-generation controller fence"
        )
    if event_kind == "cancel" and run.activity == "paused":
        raise DeliveryConflictError(
            "A paused Delivery Run can only be resumed; cancellation requires "
            "controller reconciliation through its exact-generation fence"
        )
    if event_kind in {"pause", "cancel"} and await _has_active_controller_capability(
        db,
        run,
    ):
        raise DeliveryConflictError(
            "An active Delivery Capability must be bound and reconciled through "
            "its exact-generation controller fence"
        )
    if event_kind in {"pause", "cancel"} and await _has_active_delivery_action(
        db,
        run,
    ):
        raise DeliveryConflictError(
            "An active Delivery publication action must be reconciled through "
            "its exact-generation controller fence"
        )


def _response(
    run: DeliveryRun,
    *,
    has_active_controller_capability: bool = False,
    has_active_delivery_action: bool = False,
) -> DeliveryRunResponse:
    payload = DeliveryRunResponse.model_validate(run)
    policy = run.policy_snapshot if isinstance(run.policy_snapshot, dict) else {}
    terminal = policy.get("terminal")
    if terminal not in {"ready_to_merge", "merged"}:
        terminal = None
    return payload.model_copy(
        update={
            "terminal": terminal,
            "allowed_actions": _allowed_actions(
                run,
                has_active_controller_capability=has_active_controller_capability,
                has_active_delivery_action=has_active_delivery_action,
            )
        }
    )


def _map_error(exc: DeliveryError) -> HTTPException:
    if isinstance(exc, DeliveryNotFoundError):
        return HTTPException(404, str(exc))
    if isinstance(exc, (DeliveryValidationError, DeliveryUnsupportedScopeError)):
        return HTTPException(400, str(exc))
    if isinstance(exc, DeliveryUnavailableError):
        return HTTPException(503, str(exc))
    if isinstance(exc, DeliveryConflictError):
        return HTTPException(409, str(exc))
    return HTTPException(500, "Delivery operation failed")


def _wake_controller() -> None:
    try:
        from backend.main import delivery_controller

        if delivery_controller is not None:
            delivery_controller.wake()
    except (ImportError, AttributeError):
        # The committed database state is authoritative; the controller's
        # periodic recovery scan remains the fallback.
        return


async def _accessible_run(
    request: Request,
    db: AsyncSession,
    run_id: int,
) -> DeliveryRun:
    try:
        run = await get_delivery_run(db, run_id)
    except DeliveryError as exc:
        raise _map_error(exc) from exc
    await require_project_access(request, run.project_id, db)
    return run


async def _response_with_effect_fence(
    db: AsyncSession,
    run: DeliveryRun,
) -> DeliveryRunResponse:
    has_capability = await _has_active_controller_capability(db, run)
    has_action = await _has_active_delivery_action(db, run)
    return _response(
        run,
        has_active_controller_capability=has_capability,
        has_active_delivery_action=has_action,
    )


@router.post("", response_model=DeliveryRunResponse, status_code=201)
async def create_run(
    body: DeliveryRunCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    admission_disabled_reason = None
    if not settings.delivery_loop_enabled:
        admission_disabled_reason = "Delivery Loop mode is disabled"
    elif not settings.capability_core_enabled:
        admission_disabled_reason = (
            "Delivery Loop requires Capability Core for Plan and Code Review"
        )
    await require_project_access(request, body.project_id, db)
    try:
        run = await create_delivery_run(
            db,
            DeliveryCreateSpec(
                idempotency_key=body.idempotency_key,
                project_id=body.project_id,
                monitored_repo_id=body.monitored_repo_id,
                title=body.title,
                requirements=body.requirements,
                created_by=get_current_user_id(request),
                source_todo_id=body.source_todo_id,
                base_branch=body.base_branch,
                provider=body.provider,
                model=body.model,
                codex_service_tier=body.codex_service_tier,
                effort_level=body.effort_level,
                timeout_hours=body.timeout_hours,
                max_cycles=body.max_cycles,
                max_no_progress=body.max_no_progress,
                strict_branch_protection=body.strict_branch_protection,
            ),
            admission_disabled_reason=admission_disabled_reason,
        )
    except DeliveryError as exc:
        # Validation may fail after the Run, Developer Task, or Todo claim was
        # staged.  Roll back here instead of relying on dependency teardown so
        # the HTTP error itself is the atomic admission boundary.
        await db.rollback()
        raise _map_error(exc) from exc
    _wake_controller()
    return await _response_with_effect_fence(db, run)


@router.get("", response_model=list[DeliveryRunResponse])
async def list_runs(
    request: Request,
    project_id: int | None = None,
    limit: int = 100,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    if project_id is not None:
        await require_project_access(request, project_id, db)
        runs = await list_delivery_runs(
            db,
            project_id=project_id,
            limit=limit,
            offset=offset,
        )
    else:
        # Reuse the canonical ACL for each row.  This avoids duplicating the
        # evolving Team/Group visibility query in a second subsystem.
        bounded_limit = max(1, min(limit, 200))
        bounded_offset = max(offset, 0)
        runs = []
        visible_seen = 0
        scan_offset = 0
        while len(runs) < bounded_limit:
            candidates = await list_delivery_runs(
                db,
                limit=200,
                offset=scan_offset,
            )
            if not candidates:
                break
            scan_offset += len(candidates)
            for run in candidates:
                try:
                    await require_project_access(request, run.project_id, db)
                except HTTPException as exc:
                    if exc.status_code == 403:
                        continue
                    raise
                if visible_seen < bounded_offset:
                    visible_seen += 1
                    continue
                runs.append(run)
                if len(runs) >= bounded_limit:
                    break
            if len(candidates) < 200:
                break
    return [await _response_with_effect_fence(db, run) for run in runs]


@router.get("/{run_id}", response_model=DeliveryRunDetail)
async def read_run(
    run_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    run = await _accessible_run(request, db, run_id)
    cycles = list(
        (
            await db.execute(
                select(DeliveryCycle)
                .where(DeliveryCycle.run_id == run.id)
                .order_by(DeliveryCycle.cycle_number)
            )
        ).scalars()
    )
    turns = list(
        (
            await db.execute(
                select(DeliveryTurn)
                .where(DeliveryTurn.run_id == run.id)
                .order_by(DeliveryTurn.generation)
            )
        ).scalars()
    )
    transitions = list(
        (
            await db.execute(
                select(DeliveryTransition)
                .where(DeliveryTransition.run_id == run.id)
                .order_by(DeliveryTransition.state_version)
            )
        ).scalars()
    )
    base = (await _response_with_effect_fence(db, run)).model_dump()
    return DeliveryRunDetail.model_validate(
        {
            **base,
            "policy_snapshot": run.policy_snapshot,
            "cycles": cycles,
            "turns": turns,
            "transitions": transitions,
        }
    )


async def _command(
    *,
    request: Request,
    db: AsyncSession,
    run_id: int,
    event: DeliveryReducerEvent,
) -> DeliveryRunResponse:
    accessible = await _accessible_run(request, db, run_id)
    try:
        run = await lock_run(db, accessible.id)
        await _require_command_safe_state(db, run, event_kind=event.kind)
        if event.kind == "cancel":
            if run.current_cycle_id is not None:
                cycle = (
                    await db.execute(
                        select(DeliveryCycle)
                        .where(
                            DeliveryCycle.id == run.current_cycle_id,
                            DeliveryCycle.run_id == run.id,
                        )
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                if (
                    cycle is not None
                    and cycle.status in DELIVERY_CYCLE_ACTIVE_STATUSES
                ):
                    complete_cycle(cycle, status="cancelled")
            if run.developer_task_id is not None:
                task = (
                    await db.execute(
                        select(Task)
                        .where(Task.id == run.developer_task_id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).scalar_one_or_none()
                if task is not None and task.status == "delivery_waiting":
                    task.status = "cancelled"
                    task.completed_at = datetime.utcnow()
                    task.error_message = (
                        event.payload.get("reason")
                        if isinstance(event.payload.get("reason"), str)
                        else "Delivery Run cancelled"
                    )
        await apply_run_event(
            db,
            run=run,
            event=event,
            actor_kind="user",
            actor_id=(
                str(get_current_user_id(request))
                if get_current_user_id(request) is not None
                else None
            ),
            metadata=(
                {"reason": event.payload["reason"]}
                if isinstance(event.payload.get("reason"), str)
                else None
            ),
        )
        await db.commit()
        await db.refresh(run)
    except DeliveryError as exc:
        await db.rollback()
        raise _map_error(exc) from exc
    except ValueError as exc:
        await db.rollback()
        raise HTTPException(409, str(exc)) from exc
    _wake_controller()
    return _response(run)


@router.post("/{run_id}/pause", response_model=DeliveryRunResponse)
async def pause_run(
    run_id: int,
    body: DeliveryCommand,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    return await _command(
        request=request,
        db=db,
        run_id=run_id,
        event=DeliveryReducerEvent("pause", {"reason": body.reason}),
    )


@router.post("/{run_id}/resume", response_model=DeliveryRunResponse)
async def resume_run(
    run_id: int,
    body: DeliveryResumeCommand,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    return await _command(
        request=request,
        db=db,
        run_id=run_id,
        event=DeliveryReducerEvent(
            "resume",
            {"reason": body.reason} if body.reason is not None else {},
        ),
    )


@router.post("/{run_id}/cancel", response_model=DeliveryRunResponse)
async def cancel_run(
    run_id: int,
    body: DeliveryCommand,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    return await _command(
        request=request,
        db=db,
        run_id=run_id,
        event=DeliveryReducerEvent("cancel", {"reason": body.reason}),
    )
