"""ACL-scoped API for the autonomous Delivery Loop mode."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import (
    get_current_user_id,
    is_admin,
    lock_project_effect_access,
    lock_request_user_authority,
    require_project_access,
)
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
from backend.models.instance import Instance
from backend.models.pr_monitor import MonitoredRepo
from backend.models.task import Task
from backend.schemas.delivery import (
    DeliveryAttentionCount,
    DeliveryCommand,
    DeliveryCycleResponse,
    DeliveryProgressResponse,
    DeliveryQuickStartCreate,
    DeliveryRetryCommand,
    DeliveryResumeCommand,
    DeliveryRunCreate,
    DeliveryRunDetail,
    DeliveryRunResponse,
    DeliveryTransitionResponse,
    DeliveryTurnResponse,
)
from backend.services.delivery_events import broadcast_delivery_event
from backend.services.delivery_progress import (
    build_delivery_progress,
    delivery_run_attention_required,
)
from backend.services.pr_review_actions import (
    FindingActionConflict,
    lock_pr_repo_action_boundary,
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
    lock_current_cycle,
    lock_run,
    start_next_cycle,
)
from backend.services.delivery_setup import (
    DeliverySetupConflictError,
    DeliverySetupError,
    DeliverySetupNotFoundError,
    DeliverySetupPermissionError,
    DeliverySetupUnavailableError,
    DeliverySetupValidationError,
    ensure_default_delivery_monitor,
)
from backend.services.test_harness_owner_fence import (
    has_active_test_harness_owner_graph,
)


router = APIRouter(prefix="/api/delivery-runs", tags=["delivery-runs"])


def _retry_available(
    run: DeliveryRun,
    *,
    has_active_controller_capability: bool = False,
    has_active_delivery_action: bool = False,
) -> bool:
    """Expose stage retry only when no external effect remains active."""

    return bool(
        settings.delivery_loop_enabled
        and settings.capability_core_enabled
        and run.activity == "terminal"
        and run.outcome == "failed"
        and run.lease_owner is None
        and run.current_cycle_id is not None
        and run.developer_task_id is not None
        and run.cycle_count < run.max_cycles
        and run.error_code != "delivery_max_cycles"
        and not has_active_controller_capability
        and not has_active_delivery_action
    )


_RETRY_PHASES = {
    "planning": ("planning", "ready"),
    "coding": ("coding", "ready"),
    "pre_review": ("pre_review", "ready"),
    "frontend_review": ("frontend_review", "ready"),
    "publishing": ("publishing", "ready"),
    "monitoring": ("monitoring", "waiting"),
}


def _retry_target(transition: DeliveryTransition) -> tuple[str, str]:
    before = (
        transition.before_state if isinstance(transition.before_state, dict) else {}
    )
    phase = before.get("phase")
    target = _RETRY_PHASES.get(phase)
    if target is None:
        raise DeliveryConflictError("Delivery retry cannot prove which stage failed")
    return target


def _copy_retry_evidence(
    failed_cycle: DeliveryCycle,
    next_cycle: DeliveryCycle,
    *,
    phase: str,
    run: DeliveryRun,
) -> None:
    if phase != "planning" and failed_cycle.plan_version_id is None:
        raise DeliveryConflictError(
            f"Cannot retry {phase}: the approved Plan evidence is missing"
        )
    if phase in {"pre_review", "frontend_review", "publishing", "monitoring"}:
        if failed_cycle.result_head_sha is None or run.head_sha is None:
            raise DeliveryConflictError(
                f"Cannot retry {phase}: the Development result is missing"
            )
    if phase in {"frontend_review", "publishing", "monitoring"}:
        if (
            failed_cycle.review_result_id is None
            or failed_cycle.review_verdict != "approved"
        ):
            raise DeliveryConflictError(
                f"Cannot retry {phase}: the approved Code Review evidence is missing"
            )
    if phase in {"publishing", "monitoring"}:
        frontend_passed = failed_cycle.frontend_review_verdict == "passed"
        frontend_skipped = bool(failed_cycle.frontend_review_skip_reason)
        if not frontend_passed and not frontend_skipped:
            raise DeliveryConflictError(
                f"Cannot retry {phase}: the Frontend Review result is missing"
            )
    if phase == "monitoring" and (
        run.pr_number is None or run.pr_url is None or run.pr_monitor_run_id is None
    ):
        raise DeliveryConflictError(
            "Cannot resume monitoring: the exact PR Monitor binding is missing"
        )

    for field in (
        "plan_version_id",
        "result_head_sha",
        "result_head_tree_sha",
        "result_patch_sha256",
        "review_result_id",
        "review_verdict",
        "review_summary",
        "frontend_review_config_snapshot",
        "frontend_review_profile_ids",
        "frontend_review_profile_index",
        "frontend_review_results",
        "frontend_review_verdict",
        "frontend_review_summary",
        "frontend_review_skip_reason",
    ):
        setattr(next_cycle, field, getattr(failed_cycle, field))
    next_cycle.status = "publishing" if phase == "monitoring" else phase


def _allowed_actions(
    run: DeliveryRun,
    *,
    has_active_controller_capability: bool = False,
    has_active_delivery_action: bool = False,
) -> list[str]:
    if run.activity == "terminal":
        return (
            ["retry"]
            if _retry_available(
                run,
                has_active_controller_capability=has_active_controller_capability,
                has_active_delivery_action=has_active_delivery_action,
            )
            else []
        )
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
            "wait_reason": _public_delivery_text(run.wait_reason, run),
            "pause_reason": _public_delivery_text(run.pause_reason, run),
            "error_message": _public_delivery_text(run.error_message, run),
            "allowed_actions": _allowed_actions(
                run,
                has_active_controller_capability=has_active_controller_capability,
                has_active_delivery_action=has_active_delivery_action,
            ),
        }
    )


def _delivery_path_prefixes(run: DeliveryRun) -> tuple[str, ...]:
    """Return exact host prefixes which must never cross the human API."""

    workspace = run.workspace_path
    if not isinstance(workspace, str) or not workspace.startswith("/"):
        return ()
    values = {workspace.rstrip("/")}
    marker = "/.claude-manager/worktrees/"
    if marker in workspace:
        project_root = workspace.split(marker, 1)[0].rstrip("/")
        if project_root:
            values.add(project_root)
    return tuple(sorted(values, key=len, reverse=True))


def _public_delivery_text(value: object, run: DeliveryRun) -> str | None:
    if not isinstance(value, str):
        return None
    result = value
    for prefix in _delivery_path_prefixes(run):
        result = result.replace(prefix, "[delivery-workspace]")
    return result


_PUBLIC_FINDING_KEYS = frozenset(
    {
        "severity",
        "category",
        "title",
        "summary",
        "message",
        "file",
        "path",
        "line",
        "column",
        "route",
        "locator",
        "expected",
        "actual",
        "reproduction",
        "evidence",
    }
)


def _public_finding(value: object, run: DeliveryRun) -> dict | None:
    if not isinstance(value, dict):
        return None
    result = {}
    for key in _PUBLIC_FINDING_KEYS:
        if key not in value:
            continue
        item = value.get(key)
        if isinstance(item, str):
            result[key] = _public_delivery_text(item, run)
        elif type(item) in {int, float, bool} or item is None:
            result[key] = item
        elif isinstance(item, list) and key == "evidence":
            result[key] = [
                text
                for raw in item[:100]
                if (text := _public_delivery_text(raw, run)) is not None
            ]
    return result or None


def _public_cycle_trigger_payload(
    cycle: DeliveryCycle,
    run: DeliveryRun,
) -> dict:
    """Keep user-facing retry/finding context, not controller receipts."""

    raw = cycle.trigger_payload if isinstance(cycle.trigger_payload, dict) else {}
    result = {}
    for key in ("reason", "previous_error_message", "summary", "report"):
        text = _public_delivery_text(raw.get(key), run)
        if text is not None:
            result["summary" if key == "report" else key] = text
    monitor_status = raw.get("monitor_status")
    if isinstance(monitor_status, str):
        result["monitor_status"] = monitor_status
    no_progress_count = raw.get("no_progress_count")
    if type(no_progress_count) is int:
        result["no_progress_count"] = no_progress_count

    findings = raw.get("findings")
    if isinstance(findings, list):
        result["findings"] = [
            finding
            for item in findings[:200]
            if (finding := _public_finding(item, run)) is not None
        ]
    evidence = raw.get("evidence")
    if isinstance(evidence, dict):
        public_evidence = {}
        evidence_summary = _public_delivery_text(evidence.get("summary"), run)
        if evidence_summary is not None:
            public_evidence["summary"] = evidence_summary
        evidence_findings = evidence.get("findings")
        if isinstance(evidence_findings, list):
            public_evidence["findings"] = [
                finding
                for item in evidence_findings[:200]
                if (finding := _public_finding(item, run)) is not None
            ]
        if public_evidence:
            result["evidence"] = public_evidence
    return result


def _public_cycle_response(
    cycle: DeliveryCycle,
    run: DeliveryRun,
) -> DeliveryCycleResponse:
    payload = DeliveryCycleResponse.model_validate(cycle)
    return payload.model_copy(
        update={
            "trigger_payload": _public_cycle_trigger_payload(cycle, run),
            "review_summary": _public_delivery_text(cycle.review_summary, run),
            "frontend_review_summary": _public_delivery_text(
                cycle.frontend_review_summary,
                run,
            ),
            "frontend_review_skip_reason": _public_delivery_text(
                cycle.frontend_review_skip_reason,
                run,
            ),
            "error_message": _public_delivery_text(cycle.error_message, run),
        }
    )


def _public_turn_response(
    turn: DeliveryTurn,
    run: DeliveryRun,
) -> DeliveryTurnResponse:
    return DeliveryTurnResponse.model_validate(turn).model_copy(
        update={"last_error": _public_delivery_text(turn.last_error, run)}
    )


def _public_transition_response(
    transition: DeliveryTransition,
) -> DeliveryTransitionResponse:
    return DeliveryTransitionResponse.model_validate(transition)


def _public_progress_response(
    progress: DeliveryProgressResponse,
    run: DeliveryRun,
) -> DeliveryProgressResponse:
    active_agent = progress.active_agent
    if active_agent is not None:
        active_agent = active_agent.model_copy(
            update={"detail": _public_delivery_text(active_agent.detail, run)}
        )
    frontend = progress.frontend_review.model_copy(
        update={
            "report": _public_delivery_text(progress.frontend_review.report, run),
            "error": _public_delivery_text(progress.frontend_review.error, run),
            "skip_reason": _public_delivery_text(
                progress.frontend_review.skip_reason,
                run,
            ),
        }
    )
    events = [
        event.model_copy(
            update={
                # Stable list position is sufficient for the UI; raw DB ids
                # and cross-domain receipt handles stay server-side.
                "id": f"event:{index}",
                "title": _public_delivery_text(event.title, run) or "Delivery event",
                "detail": _public_delivery_text(event.detail, run),
            }
        )
        for index, event in enumerate(progress.events, start=1)
    ]
    stages = [
        stage.model_copy(
            update={
                "summary": _public_delivery_text(stage.summary, run) or "Delivery stage"
            }
        )
        for stage in progress.stages
    ]
    return progress.model_copy(
        update={
            "detail": _public_delivery_text(progress.detail, run),
            "active_agent": active_agent,
            "frontend_review": frontend,
            "events": events,
            "stages": stages,
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


def _quick_start_title(requirements: str) -> str:
    first_line = next(
        (line.strip() for line in requirements.splitlines() if line.strip()),
        "Delivery task",
    )
    normalized = " ".join(first_line.split())
    return normalized[:200].rstrip() or "Delivery task"


def _map_setup_error(exc: DeliverySetupError) -> HTTPException:
    if isinstance(exc, DeliverySetupNotFoundError):
        return HTTPException(404, str(exc))
    if isinstance(exc, DeliverySetupPermissionError):
        return HTTPException(403, str(exc))
    if isinstance(exc, DeliverySetupValidationError):
        return HTTPException(400, str(exc))
    if isinstance(exc, DeliverySetupConflictError):
        return HTTPException(409, str(exc))
    if isinstance(exc, DeliverySetupUnavailableError):
        detail = {
            "code": exc.code,
            "message": str(exc),
            "repo_full_name": exc.repo_full_name,
            "candidate_checks": exc.candidates,
        }
        return HTTPException(503, detail)
    return HTTPException(500, "Delivery PR Monitor setup failed")


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


async def _lock_delivery_admission_topology(
    request: Request,
    db: AsyncSession,
    *,
    project_id: int,
    monitored_repo_id: int,
) -> None:
    """Authorize and lock one Delivery scope in global topology order.

    The optimistic read prevents an unauthorized caller from using the
    repository writer fence as a cross-Project lock oracle. The authoritative
    transaction then takes MonitoredRepo before Project, matching every PR
    Monitor mutation that reauthorizes through Project-derived ACLs.
    """

    await require_project_access(request, project_id, db)
    repo = await db.get(MonitoredRepo, monitored_repo_id)
    if repo is None:
        raise HTTPException(404, "Monitored repository not found")
    if repo.project_id != project_id:
        raise HTTPException(
            400,
            "Monitored repository must be bound to the selected Project",
        )

    await db.rollback()
    try:
        locked_repo = await lock_pr_repo_action_boundary(
            db,
            monitored_repo_id,
        )
    except FindingActionConflict as exc:
        raise HTTPException(404, "Monitored repository not found") from exc
    if locked_repo.project_id != project_id:
        raise HTTPException(
            409,
            "PR Monitor Project changed before Delivery admission",
        )
    await lock_project_effect_access(request, project_id, db)


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
    await _lock_delivery_admission_topology(
        request,
        db,
        project_id=body.project_id,
        monitored_repo_id=body.monitored_repo_id,
    )
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
                auto_merge=body.auto_merge,
                strict_branch_protection=body.strict_branch_protection,
                frontend_review=body.frontend_review,
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
    await broadcast_delivery_event(
        "delivery_created",
        run_id=run.id,
        project_id=run.project_id,
        state_version=run.state_version,
    )
    return await _response_with_effect_fence(db, run)


@router.post(
    "/quick-start",
    response_model=DeliveryRunResponse,
    status_code=201,
)
async def quick_start_run(
    body: DeliveryQuickStartCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Start Delivery from one requirement, bootstrapping PR Monitor once."""

    admission_disabled_reason = None
    if not settings.delivery_loop_enabled:
        admission_disabled_reason = "Delivery Loop mode is disabled"
    elif not settings.capability_core_enabled:
        admission_disabled_reason = (
            "Delivery Loop requires Capability Core for Plan and Code Review"
        )
    await require_project_access(request, body.project_id, db)

    async def authorize_monitor_create(effect_db: AsyncSession) -> None:
        await lock_request_user_authority(request, effect_db)

    try:
        setup = await ensure_default_delivery_monitor(
            db,
            body.project_id,
            allow_create=(admission_disabled_reason is None and is_admin(request)),
            strict_branch_protection=body.strict_branch_protection,
            create_authorizer=authorize_monitor_create,
        )
    except DeliverySetupPermissionError as exc:
        await db.rollback()
        if admission_disabled_reason is not None:
            raise HTTPException(503, admission_disabled_reason) from exc
        raise _map_setup_error(exc) from exc
    except DeliverySetupError as exc:
        await db.rollback()
        raise _map_setup_error(exc) from exc

    title = body.title or _quick_start_title(body.requirements)
    setup_repo_id = setup.repo.id
    setup_default_branch = setup.repo.default_branch
    setup_provider = setup.repo.provider
    try:
        # Monitor discovery/existing-row reconciliation may commit and release
        # the optimistic ACL snapshot.  Reacquire the canonical Project and
        # group-membership writer fence in the transaction that materializes
        # the Run, Developer Task, and first Cycle.
        await db.rollback()
        await _lock_delivery_admission_topology(
            request,
            db,
            project_id=body.project_id,
            monitored_repo_id=setup_repo_id,
        )
        run = await create_delivery_run(
            db,
            DeliveryCreateSpec(
                idempotency_key=body.idempotency_key,
                project_id=body.project_id,
                monitored_repo_id=setup_repo_id,
                title=title,
                requirements=body.requirements,
                created_by=get_current_user_id(request),
                base_branch=setup_default_branch,
                provider=setup_provider,
                timeout_hours=body.timeout_hours,
                max_cycles=body.max_cycles,
                max_no_progress=body.max_no_progress,
                auto_merge=body.auto_merge,
                strict_branch_protection=body.strict_branch_protection,
                frontend_review=body.frontend_review,
            ),
            admission_disabled_reason=admission_disabled_reason,
        )
    except DeliveryError as exc:
        await db.rollback()
        raise _map_error(exc) from exc
    _wake_controller()
    await broadcast_delivery_event(
        "delivery_created",
        run_id=run.id,
        project_id=run.project_id,
        state_version=run.state_version,
    )
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


@router.get("/attention-count", response_model=DeliveryAttentionCount)
async def count_attention_runs(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    candidates = list(
        (
            await db.execute(select(DeliveryRun).order_by(DeliveryRun.id.desc()))
        ).scalars()
    )
    total = 0
    for run in candidates:
        try:
            await require_project_access(request, run.project_id, db)
        except HTTPException as exc:
            if exc.status_code == 403:
                continue
            raise
        if await delivery_run_attention_required(db, run):
            total += 1
    return DeliveryAttentionCount(total=total)


@router.get("/{run_id}/progress", response_model=DeliveryProgressResponse)
async def read_run_progress(
    run_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    run = await _accessible_run(request, db, run_id)
    progress = await build_delivery_progress(db, run)
    return _public_progress_response(progress, run)


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
            "cycles": [_public_cycle_response(cycle, run) for cycle in cycles],
            "turns": [_public_turn_response(turn, run) for turn in turns],
            "transitions": [
                _public_transition_response(transition) for transition in transitions
            ],
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
    accessible_run_id = accessible.id
    accessible_project_id = accessible.project_id
    try:
        await db.rollback()
        await lock_project_effect_access(request, accessible_project_id, db)
        run = await lock_run(db, accessible_run_id)
        if run.project_id != accessible_project_id:
            raise DeliveryConflictError(
                "Delivery Run Project changed before command admission"
            )
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
                if cycle is not None and cycle.status in DELIVERY_CYCLE_ACTIVE_STATUSES:
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
    await broadcast_delivery_event(
        "delivery_command_applied",
        run_id=run.id,
        project_id=run.project_id,
        state_version=run.state_version,
    )
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


@router.post("/{run_id}/retry", response_model=DeliveryRunResponse)
async def retry_failed_run(
    run_id: int,
    body: DeliveryRetryCommand,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Retry only the failed Delivery stage while preserving prior evidence."""

    accessible = await _accessible_run(request, db, run_id)
    accessible_run_id = accessible.id
    accessible_project_id = accessible.project_id
    try:
        await db.rollback()
        await lock_project_effect_access(request, accessible_project_id, db)
        run = await lock_run(db, accessible_run_id)
        if run.project_id != accessible_project_id:
            raise DeliveryConflictError(
                "Delivery Run Project changed before retry admission"
            )
        if run.state_version != body.expected_state_version:
            raise DeliveryConflictError(
                "Delivery Run changed before retry; refresh and retry again"
            )
        has_capability = await _has_active_controller_capability(db, run)
        has_action = await _has_active_delivery_action(db, run)
        if not _retry_available(
            run,
            has_active_controller_capability=has_capability,
            has_active_delivery_action=has_action,
        ):
            raise DeliveryConflictError(
                "This Delivery failure cannot be retried: it must have remaining "
                "cycle budget and own no active effect"
            )

        failed_cycle = await lock_current_cycle(db, run)
        if failed_cycle.status != "failed" or failed_cycle.active_run_id is not None:
            raise DeliveryConflictError(
                "Delivery retry requires an exact failed terminal cycle"
            )
        active_turn_id = await db.scalar(
            select(DeliveryTurn.id).where(DeliveryTurn.active_run_id == run.id).limit(1)
        )
        if active_turn_id is not None:
            raise DeliveryConflictError(
                "Delivery retry is blocked by an active Developer turn"
            )

        task = (
            await db.execute(
                select(Task)
                .where(Task.id == run.developer_task_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        reusable_statuses = {
            "delivery_waiting",
            "completed",
            "failed",
            "cancelled",
            "stopped",
            "conflict",
        }
        if (
            task is None
            or task.delivery_run_id != run.id
            or task.delivery_role != "developer"
            or task.mode != "delivery_loop"
            or task.worker_id is not None
            or task.shared_from_id is not None
            or task.status not in reusable_statuses
            or task.pty_background_generation is not None
        ):
            raise DeliveryConflictError(
                "Delivery Developer Task is not in a reusable terminal generation"
            )
        active_instance_id = await db.scalar(
            select(Instance.id)
            .where(Instance.current_task_id == task.id)
            .with_for_update()
            .limit(1)
        )
        if active_instance_id is not None:
            raise DeliveryConflictError(
                "Delivery retry is blocked by an active Developer instance"
            )
        if await has_active_test_harness_owner_graph(db, task.id):
            raise DeliveryConflictError(
                "Delivery retry is blocked until the current frontend test "
                "graph is terminal and cleanup is proven"
            )

        failure_transition = await db.scalar(
            select(DeliveryTransition)
            .where(
                DeliveryTransition.run_id == run.id,
                DeliveryTransition.cause == "fail",
                DeliveryTransition.state_version == run.state_version,
            )
            .limit(1)
        )
        if failure_transition is None:
            raise DeliveryConflictError(
                "Delivery retry cannot prove the exact failed stage"
            )
        retry_phase, retry_activity = _retry_target(failure_transition)

        previous_error_code = run.error_code
        previous_error_message = run.error_message
        requested_by = get_current_user_id(request)
        reason = body.reason or f"Operator requested retry of {retry_phase}"
        next_cycle = await start_next_cycle(
            db,
            run=run,
            trigger_kind="operator_retry",
            trigger_payload={
                "previous_cycle_id": failed_cycle.id,
                "previous_cycle_number": failed_cycle.cycle_number,
                "previous_error_code": previous_error_code,
                "previous_error_message": previous_error_message,
                "reason": reason,
                "requested_by": requested_by,
                "resume_phase": retry_phase,
                "resume_activity": retry_activity,
            },
        )
        _copy_retry_evidence(
            failed_cycle,
            next_cycle,
            phase=retry_phase,
            run=run,
        )
        await apply_run_event(
            db,
            run=run,
            event=DeliveryReducerEvent(
                "retry",
                {"phase": retry_phase, "activity": retry_activity},
            ),
            actor_kind="user",
            actor_id=(str(requested_by) if requested_by is not None else None),
            metadata={
                "reason": reason,
                "previous_cycle_id": failed_cycle.id,
                "next_cycle_id": next_cycle.id,
                "previous_error_code": previous_error_code,
                "resume_phase": retry_phase,
            },
        )
        task.status = "delivery_waiting"
        task.completed_at = None
        task.error_message = None
        task.result_branch = run.delivery_branch
        run.next_reconcile_at = datetime.utcnow()
        await db.commit()
        await db.refresh(run)
    except DeliveryError as exc:
        await db.rollback()
        raise _map_error(exc) from exc
    except ValueError as exc:
        await db.rollback()
        raise HTTPException(409, str(exc)) from exc

    _wake_controller()
    await broadcast_delivery_event(
        "delivery_retry_started",
        run_id=run.id,
        project_id=run.project_id,
        state_version=run.state_version,
        cycle_id=run.current_cycle_id,
    )
    return _response(run)
