"""Canonical first-class Plan, Version, Run, and InputRequest APIs."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import os
from types import SimpleNamespace

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import and_, case, exists, false, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from backend.api.deps import (
    get_current_user_id,
    has_project_access,
    is_admin,
    lock_request_user_authority,
    lock_task_effect_access,
    require_admin,
    require_project_access,
    require_internal_service,
    require_task_control,
    require_worker_target_access,
    task_execution_principal_from_request,
)
from backend.api.uploads import (
    UploadAttachmentValidationError,
    validate_upload_attachments,
)
from backend.config import settings
from backend.database import get_db
from backend.models.plan import (
    Plan,
    PlanApplication,
    PlanApplicationReceipt,
    PlanInputRequest,
    PlanVersion,
)
from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentWorkerDispatchReceipt,
    PlanAgentWorkerImportReceipt,
)
from backend.models.delivery import DeliveryCycle
from backend.models.instance import Instance
from backend.models.task import Task
from backend.models.project import Project
from backend.models.team_share import TeamProjectShare
from backend.models.user_group import UserGroupMember
from backend.models.worker import Worker
from backend.schemas.plan import resolve_plan_pipeline_config
from backend.schemas.plan_resource import (
    PlanCreateRequest,
    PlanDecisionRequest,
    PlanExecutionCreateRequest,
    PlanExecutionResource,
    PlanForkRequest,
    PlanInputAnswerRequest,
    PlanInputRequestResponse,
    PlanPatchRequest,
    PlanResource,
    PlanRunCreateRequest,
    PlanRunResource,
    PlanVersionResource,
    WorkerPlanRunImportRequest,
    WorkerPlanRunCancelRequest,
    WorkerPlanVersionImportRequest,
    WorkerPlanVersionSeed,
)
from backend.services.plan_pipeline_settings import effective_plan_pipeline_config
from backend.services.plan_service import (
    ACTIVE_RUN_STATUSES,
    answer_input_request,
    cancel_worker_mirror_run_after_ack,
    cancel_run,
    create_plan_run,
    create_plan_with_run,
    decide_version,
    fence_plan_target_task,
    fence_plan_worker,
    finalize_run_cancellation,
    input_request_resource,
    materialize_execution_task,
    plan_operation_lock,
    plan_resource,
    plan_resources,
    release_run_owner_after_cleanup,
    reject_capability_owned_plan_mutation,
    resolve_legacy_task,
    run_resource,
    serialize_related_plan_creation,
    version_resource,
)
from backend.services.plan_tasks import (
    capture_repo_revision,
    capture_task_context,
    latest_task_log_id,
)
from backend.services.plan_staleness import version_staleness
from backend.services.plan_events import broadcast_plan_event
from backend.services.cancellation import await_task_completion
from backend.services.plan_input_safety import contains_high_confidence_secret
from backend.services.worker_node_control import fence_worker_node_mutation


router = APIRouter(tags=["plan-resources"])


class _WorkerRepoRevisionRequest(BaseModel):
    project_id: int | None = None
    target_task_id: int | None = None


class _PlanDeliveryResolutionRequest(BaseModel):
    action: str = Field(pattern=r"^(confirm_launched|release_for_retry)$")
    note: str = Field(min_length=1, max_length=2000)


async def _settle_plan_cancel_task(
    operation: asyncio.Task,
) -> asyncio.CancelledError | None:
    """Delay repeated caller cancellation until one finite barrier settles."""

    return await await_task_completion(operation)


async def _finish_plan_cancel_mutation(
    awaitable,
    *,
    lifecycle_stop: dict[str, object | None],
    dispatcher,
):
    """Prove a durable mutation, arm cleanup, then deliver cancellation."""

    operation = asyncio.create_task(awaitable)
    delayed_cancellation = await _settle_plan_cancel_task(operation)
    result = operation.result()
    # No await may separate proof of the committed mutation from arming its
    # lifecycle cleanup.  The guard itself exits after plan_operation_lock.
    lifecycle_stop["dispatcher"] = dispatcher
    if delayed_cancellation is not None:
        raise delayed_cancellation
    return result


@asynccontextmanager
async def _stop_plan_run_lifecycle_after_fence(run_id: int):
    """Reap a Worker dispatch only after its Plan aggregate lock is released.

    The caller arms this guard only after committing a durable cancellation
    fence (or a terminal pre-import cancellation).  Entering it before
    ``plan_operation_lock`` makes its exit run afterwards even when the exact
    Worker RPC fails or the request is cancelled.  This avoids both a lock
    re-entry deadlock and a live dispatch lifecycle permanently suppressing
    cold cancellation recovery.
    """

    state: dict[str, object | None] = {"dispatcher": None}
    try:
        yield state
    finally:
        dispatcher = state["dispatcher"]
        stop_lifecycle = getattr(
            dispatcher,
            "stop_plan_run_lifecycle",
            None,
        )
        if callable(stop_lifecycle):
            cleanup = asyncio.create_task(stop_lifecycle(run_id, None))
            delayed_cancellation = await _settle_plan_cancel_task(cleanup)
            try:
                cleanup.result()
            except (asyncio.CancelledError, Exception):
                # The durable Run/receipt fence prevents stale publication;
                # cold recovery will retry the reap before its exact RPC.
                pass
            if delayed_cancellation is not None:
                raise delayed_cancellation


def _reject_durable_plan_secrets(*values: object) -> None:
    if contains_high_confidence_secret(values):
        raise HTTPException(
            422,
            "Plan text cannot store API keys or access tokens. "
            "Save the credential in Settings → Secrets and refer to it by name.",
        )


async def _wake_dispatcher() -> None:
    try:
        from backend.main import dispatcher

        if dispatcher:
            dispatcher.wake()
    except Exception:
        pass


async def _materialize_worker_version(
    db: AsyncSession,
    *,
    plan: Plan,
    seed: WorkerPlanVersionSeed,
) -> PlanVersion:
    """Idempotently restore one immutable Manager Version on this Worker."""

    version = (
        await db.execute(
            select(PlanVersion).where(
                PlanVersion.plan_id == plan.id,
                PlanVersion.version_number == seed.version_number,
            )
        )
    ).scalar_one_or_none()
    if version is None:
        version = PlanVersion(
            plan_id=plan.id,
            version_number=seed.version_number,
            content=seed.content,
            context_session_id=seed.context_session_id,
            context_log_id=seed.context_log_id,
            context_snapshot=seed.context_snapshot,
            repo_revision=seed.repo_revision,
            reviewer_repo_revision=seed.reviewer_repo_revision,
            review_verdict=seed.review_verdict,
            review_feedback=seed.review_feedback,
            review_exhausted=seed.review_exhausted,
            reviewed_at=seed.reviewed_at,
            human_decision=seed.human_decision,
        )
        db.add(version)
        await db.flush()
    elif version.content != seed.content:
        raise HTTPException(
            409,
            "Worker Plan Version number collides with different immutable content",
        )
    elif (
        version.human_decision not in {"pending", seed.human_decision}
        and seed.human_decision != "pending"
    ):
        raise HTTPException(409, "Worker Plan Version decision conflicts with Manager")
    if seed.human_decision != "pending":
        version.human_decision = seed.human_decision
    version.review_verdict = seed.review_verdict
    version.review_feedback = seed.review_feedback
    version.review_exhausted = seed.review_exhausted
    version.reviewed_at = seed.reviewed_at
    version.reviewer_repo_revision = seed.reviewer_repo_revision
    plan.current_version_id = version.id
    plan.updated_at = datetime.utcnow()
    return version


async def _fence_worker_mirror_target(
    db: AsyncSession,
    target_task_id: int | None,
) -> Task | None:
    """Keep a Worker mirror insert inside the target Task delete barrier."""

    if target_task_id is None:
        return None
    target = await db.get(Task, target_task_id)
    if target is None:
        raise HTTPException(409, "Worker Plan target Task is missing")
    # End the routing/read snapshot before taking the exact Task writer fence.
    # Task deletion takes the same row first and therefore either sees this
    # complete Plan mirror or commits before this import can proceed.
    await db.rollback()
    await fence_plan_target_task(
        db,
        target_task_id=target_task_id,
        expected_worker_id=None,
    )
    target = await db.get(Task, target_task_id, populate_existing=True)
    if target is None:  # Defensive: the write fence prevents this normally.
        raise HTTPException(409, "Worker Plan target Task disappeared")
    return target


def _validated_uploads(body) -> list[dict] | None:
    try:
        uploads = validate_upload_attachments(
            file_paths=body.file_paths,
            image_paths=body.image_paths,
            attachments=body.attachments,
        )
    except UploadAttachmentValidationError as exc:
        raise HTTPException(400, str(exc)) from exc
    return [
        {**item.public_dict(), "path": item.path}
        for item in uploads
    ] or None


def _validate_attachment_manifest(
    uploads: list[dict] | None,
    manifest: list[dict] | None,
) -> list[dict]:
    expected = manifest or []
    paths = [item["path"] for item in (uploads or [])]
    if len(expected) != len(paths):
        raise HTTPException(409, "Plan attachment manifest count does not match uploads")
    receipt: list[dict] = []
    for index, path in enumerate(paths):
        item = expected[index]
        if not isinstance(item, dict) or os.path.abspath(path) != item.get("path"):
            raise HTTPException(409, "Plan attachment manifest path/order mismatch")
        digest = hashlib.sha256()
        size = 0
        try:
            with open(path, "rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    size += len(chunk)
                    digest.update(chunk)
        except OSError as exc:
            raise HTTPException(409, "Plan attachment is unavailable on Worker") from exc
        row = {"path": os.path.abspath(path), "size": size, "sha256": digest.hexdigest()}
        if row != item:
            raise HTTPException(409, "Plan attachment digest/size mismatch")
        receipt.append(row)
    return receipt


def _worker_run_import_digest(
    body: WorkerPlanRunImportRequest,
    attachment_receipt: list[dict],
) -> str:
    """Hash immutable import identity, excluding the Manager retry fence."""

    payload = body.model_dump(
        mode="json",
        exclude={"manager_claim_generation", "attachment_manifest"},
    )
    payload["attachment_receipt"] = attachment_receipt
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()


def _require_worker_import_receipt_identity(
    receipt: PlanAgentWorkerImportReceipt,
    *,
    plan_id: int,
    run_id: int,
    payload_digest: str,
) -> None:
    """Fail closed unless a permanent Worker receipt matches byte-for-byte."""

    if (
        receipt.run_id != run_id
        or receipt.plan_id != plan_id
        or receipt.protocol != 1
        or receipt.relay_origin != "manager_v1"
        or receipt.payload_digest != payload_digest
        or receipt.outcome not in {"imported", "cancelled_before_import"}
    ):
        raise HTTPException(
            409,
            "Worker Plan Run id belongs to another immutable import identity",
        )


async def _load_worker_import_receipt(
    db: AsyncSession,
    *,
    run_id: int,
    for_update: bool = False,
) -> PlanAgentWorkerImportReceipt | None:
    query = select(PlanAgentWorkerImportReceipt).where(
        PlanAgentWorkerImportReceipt.run_id == run_id
    )
    if for_update:
        query = query.with_for_update()
    return (await db.execute(query)).scalar_one_or_none()


async def _refresh_plan_acl_dependencies(
    db: AsyncSession,
    *,
    worker_id: int | None,
    project_id: int | None,
) -> None:
    """Refresh rows which task/Plan ACL helpers may read by identity.

    The surrounding service already owns the primary Task/Worker/Plan writer
    fences for the mutation.  These reads exist only to evict values retained
    by ``expire_on_commit=False`` after the API's routing transaction ended.
    Taking additional row locks here is both insufficient to serialize share
    grants and unsafe: after a Task migration its current Worker and its
    Project's Worker may differ, so two cross-migrations could otherwise lock
    those Workers in opposite order.
    """

    if worker_id is not None:
        await db.get(
            Worker,
            worker_id,
            populate_existing=True,
        )
    if project_id is not None:
        project = await db.get(
            Project,
            project_id,
            populate_existing=True,
        )
        if (
            project is not None
            and project.worker_id is not None
            and project.worker_id != worker_id
        ):
            await db.get(
                Worker,
                project.worker_id,
                populate_existing=True,
            )


async def _has_plan_access(
    request: Request, plan: Plan, db: AsyncSession, *, control: bool
) -> bool:
    if not settings.auth_token or is_admin(request):
        return True
    if plan.target_task_id is not None:
        target = await db.get(Task, plan.target_task_id)
        if target is None:
            return False
        try:
            # The rich first-class Plan resource contains raw attachment
            # metadata, repository revisions, native session/instance ids and
            # application receipts.  A TeamTaskShare(permission="chat") may
            # converse with the target Task, but it is not authorization to
            # read this control-plane/audit model.  Chat clients use the
            # narrow request-aware related-Task projection instead.
            await require_task_control(request, target, db)
            return True
        except HTTPException:
            return False
    user_id = get_current_user_id(request)
    if user_id is not None and plan.created_by == user_id:
        return True
    if plan.project_id is not None and await has_project_access(request, plan.project_id, db):
        return True
    return False


async def _lock_standalone_plan_effect_access(
    request: Request,
    db: AsyncSession,
    *,
    project_id: int | None,
    plan_created_by: int | None,
    worker_id: int | None = None,
    fence_worker_assignment: bool = False,
) -> Project | None:
    """Fence a standalone Plan effect without narrowing Plan ownership.

    A standalone Plan creator remains an independent controller even when it
    is not currently a Project-share recipient.  Group-derived callers use
    the canonical Project ACL helper; creator/admin/no-auth callers still take
    the same Project row boundary so deletion and share writers retain one
    global Worker-node -> Project -> Worker -> membership -> User order when
    the effect creates new durable Worker ownership.
    """

    await fence_worker_node_mutation(db)
    if project_id is None:
        if fence_worker_assignment:
            from backend.services.worker_assignment import (
                fence_ready_worker_assignment,
            )

            await fence_ready_worker_assignment(db, worker_id)
        await lock_request_user_authority(request, db)
        return None
    from backend.services.project_share_admission import (
        ProjectShareAdmissionError,
        lock_project_share_authority,
    )

    try:
        project = await lock_project_share_authority(db, project_id)
    except ProjectShareAdmissionError as exc:
        await db.rollback()
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        await db.rollback()
        raise HTTPException(409, "Plan Project is no longer available") from exc
    if fence_worker_assignment:
        from backend.services.worker_assignment import (
            fence_ready_worker_assignment,
        )

        await fence_ready_worker_assignment(db, project.worker_id)
    creator_or_admin = (
        not settings.auth_token
        or is_admin(request)
        or (
            get_current_user_id(request) is not None
            and get_current_user_id(request) == plan_created_by
        )
    )
    if not creator_or_admin and not await has_project_access(
        request,
        project_id,
        db,
        effect_fence=True,
    ):
        raise HTTPException(403, "No access to this project")
    await lock_request_user_authority(request, db)
    return project


def _plan_list_acl(request: Request):
    """SQL predicate matching the read-access rules used by detail APIs."""

    if not settings.auth_token or is_admin(request):
        return None
    user_id = get_current_user_id(request)
    if user_id is None:
        return false()
    group_ids = select(UserGroupMember.group_id).where(
        UserGroupMember.user_id == user_id
    )
    project_share = exists(
        select(TeamProjectShare.id).where(
            TeamProjectShare.project_id == Project.id,
            or_(
                and_(
                    TeamProjectShare.target_type == "user",
                    TeamProjectShare.target_id == user_id,
                ),
                and_(
                    TeamProjectShare.target_type == "group",
                    TeamProjectShare.target_id.in_(group_ids),
                ),
            ),
        )
    )
    project_ids = select(Project.id).where(project_share)
    task_ids = select(Task.id).where(
        or_(
            Task.created_by == user_id,
            Task.project_id.in_(project_ids),
        )
    )
    return or_(
        and_(
            Plan.target_task_id.isnot(None),
            Plan.target_task_id.in_(task_ids),
        ),
        and_(
            Plan.target_task_id.is_(None),
            or_(
                Plan.created_by == user_id,
                Plan.project_id.in_(project_ids),
            ),
        ),
    )


def _plan_display_state_expression():
    active_status = (
        select(PlanAgentRun.status)
        .where(PlanAgentRun.id == Plan.active_run_id)
        .correlate(Plan)
        .scalar_subquery()
    )
    active_stage = (
        select(PlanAgentRun.current_stage)
        .where(PlanAgentRun.id == Plan.active_run_id)
        .correlate(Plan)
        .scalar_subquery()
    )
    latest_status = (
        select(PlanAgentRun.status)
        .where(PlanAgentRun.plan_id == Plan.id)
        .order_by(PlanAgentRun.id.desc())
        .limit(1)
        .correlate(Plan)
        .scalar_subquery()
    )
    human_decision = (
        select(PlanVersion.human_decision)
        .where(PlanVersion.id == Plan.current_version_id)
        .correlate(Plan)
        .scalar_subquery()
    )
    review_verdict = (
        select(PlanVersion.review_verdict)
        .where(PlanVersion.id == Plan.current_version_id)
        .correlate(Plan)
        .scalar_subquery()
    )
    review_exhausted = (
        select(PlanVersion.review_exhausted)
        .where(PlanVersion.id == Plan.current_version_id)
        .correlate(Plan)
        .scalar_subquery()
    )
    applied = exists(
        select(PlanApplication.id).where(
            PlanApplication.plan_version_id == Plan.current_version_id
        )
    )
    return case(
        (Plan.archived_at.isnot(None), "archived"),
        (active_status == "waiting_user", "waiting_user"),
        (active_status == "cancelling", "cancelling"),
        (
            active_status.in_(["queued", "running"]),
            func.coalesce(active_stage, "running"),
        ),
        (and_(Plan.current_version_id.isnot(None), applied), "applied"),
        (human_decision == "approved", "approved"),
        (human_decision == "rejected", "rejected"),
        (
            or_(
                review_verdict.in_(["approve", "disabled", "exhausted"]),
                review_exhausted.is_(True),
            ),
            "awaiting_review",
        ),
        (latest_status.in_(["failed", "cancelled"]), latest_status),
        else_="draft",
    )


def _plan_collection_query(
    request: Request,
    *,
    target_task_id: int | None,
    kind: str | None,
    project_id: int | None,
    include_archived: bool,
    archived_only: bool,
    q: str | None,
    display_states: set[str],
):
    delivery_target_task = (
        select(Task.id)
        .where(
            Task.id == Plan.target_task_id,
            or_(
                Task.mode == "delivery_loop",
                Task.delivery_run_id.isnot(None),
            ),
        )
        .correlate(Plan)
        .exists()
    )
    delivery_version = (
        select(DeliveryCycle.id)
        .join(PlanVersion, PlanVersion.id == DeliveryCycle.plan_version_id)
        .where(PlanVersion.plan_id == Plan.id)
        .correlate(Plan)
        .exists()
    )
    query = select(Plan).where(~or_(delivery_target_task, delivery_version))
    if target_task_id is not None:
        query = query.where(Plan.target_task_id == target_task_id)
    if project_id is not None:
        query = query.where(Plan.project_id == project_id)
    if kind == "standalone":
        query = query.where(Plan.target_task_id.is_(None))
    elif kind == "related":
        query = query.where(Plan.target_task_id.isnot(None))
    if archived_only:
        query = query.where(Plan.archived_at.isnot(None))
    elif not include_archived:
        query = query.where(Plan.archived_at.is_(None))
    if q and q.strip():
        escaped = (
            q.strip().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        )
        pattern = f"%{escaped}%"
        query = query.where(
            or_(
                Plan.title.ilike(pattern, escape="\\"),
                Plan.initial_request.ilike(pattern, escape="\\"),
            )
        )
    acl = _plan_list_acl(request)
    if acl is not None:
        query = query.where(acl)
    if display_states:
        query = query.where(_plan_display_state_expression().in_(display_states))
    return query


async def _require_plan(
    request: Request,
    db: AsyncSession,
    plan_id: int,
    *,
    control: bool = False,
) -> Plan:
    plan = await db.get(Plan, plan_id)
    if plan is None:
        raise HTTPException(404, "Plan not found")
    if not await _has_plan_access(request, plan, db, control=control):
        raise HTTPException(403, "No permission to control this Plan" if control else "No access to this Plan")
    return plan


async def _require_version(
    request: Request,
    db: AsyncSession,
    version_id: int,
    *,
    control: bool = False,
) -> tuple[Plan, PlanVersion]:
    version = await db.get(PlanVersion, version_id)
    if version is None:
        raise HTTPException(404, "Plan Version not found")
    plan = await _require_plan(request, db, version.plan_id, control=control)
    return plan, version


async def _capture_context_for_plan(
    db: AsyncSession,
    *,
    target: Task | None,
    target_repo: str | None,
    worker_id: int | None,
) -> tuple[str | None, int | None, str | None, dict | None]:
    session_id = target.session_id if target is not None else None
    log_id = await latest_task_log_id(db, target.id) if target is not None else None
    snapshot = (
        await capture_task_context(
            db,
            target.id,
            through_log_id=log_id,
            max_chars=settings.plan_transcript_max_chars,
        )
        if target is not None
        else None
    )
    repo_revision = (
        None
        if worker_id is not None
        else await capture_repo_revision(
            (target.last_cwd or target.target_repo) if target is not None else target_repo
        )
    )
    return session_id, log_id, snapshot, repo_revision


async def _version_staleness(
    db: AsyncSession, plan: Plan, version: PlanVersion
) -> dict:
    return await version_staleness(db, plan, version)


@router.post("/api/plans/worker-repo-revision")
async def worker_repo_revision(
    body: _WorkerRepoRevisionRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Return a Worker-local fingerprint without exposing repository content."""

    require_internal_service(request)
    target = await db.get(Task, body.target_task_id) if body.target_task_id else None
    if body.target_task_id is not None and target is None:
        raise HTTPException(409, "Worker target Task is missing")
    project = await db.get(Project, body.project_id) if body.project_id else None
    if body.project_id is not None and project is None:
        raise HTTPException(409, "Worker Project is missing")
    path = (
        target.last_cwd or target.target_repo
        if target is not None
        else project.local_path if project is not None else None
    )
    return {"repo_revision": await capture_repo_revision(path)}


@router.get("/api/plans/worker-application-receipts/{receipt_key}")
async def worker_application_receipt(
    receipt_key: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_internal_service(request)
    receipt = (
        await db.execute(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.receipt_key == receipt_key
            )
        )
    ).scalar_one_or_none()
    if receipt is None:
        raise HTTPException(404, "Plan application receipt not found")
    return {
        "receipt_key": receipt.receipt_key,
        "target_task_id": receipt.target_task_id,
        "plan_version_ids": receipt.plan_version_ids,
        "status": receipt.status,
        "delivery_status": receipt.delivery_status,
        "delivery_error": receipt.delivery_error,
        "launch_evidence": receipt.launch_evidence,
        "delivery_resolution": receipt.delivery_resolution,
        "response": receipt.response,
    }


async def _broadcast_delivery_resolution(
    *,
    receipt_key: str,
    action: str,
    note: str,
    plan_ids: list[int],
    target_task_id: int | None,
) -> None:
    for plan_id in plan_ids:
        await broadcast_plan_event(
            event="plan_application_delivery_resolved",
            plan_id=plan_id,
            target_task_id=target_task_id,
            receipt_key=receipt_key,
            action=action,
        )
    if target_task_id is not None:
        from backend.main import broadcaster

        await broadcaster.broadcast(
            f"task:{target_task_id}",
            {
                "event_type": "plan_application_delivery_resolved",
                "task_id": target_task_id,
                "receipt_key": receipt_key,
                "action": action,
                "note": note,
                "delivery_status": (
                    "launched" if action == "confirm_launched" else "cancelled"
                ),
            },
        )


@router.post("/api/plans/worker-application-receipts/{receipt_key}/resolve")
async def resolve_worker_application_receipt(
    receipt_key: str,
    body: _PlanDeliveryResolutionRequest,
    request: Request,
):
    require_internal_service(request)
    note = body.note.strip()
    if not note:
        raise HTTPException(422, "Resolution note cannot be blank")
    from backend.main import dispatcher

    if dispatcher is None:
        raise HTTPException(503, "Dispatcher is unavailable")
    try:
        plan_ids, target_task_id = await dispatcher.resolve_uncertain_plan_delivery(
            receipt_key=receipt_key,
            action=body.action,
            note=note,
            actor_id=None,
        )
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    await _broadcast_delivery_resolution(
        receipt_key=receipt_key,
        action=body.action,
        note=note,
        plan_ids=plan_ids,
        target_task_id=target_task_id,
    )
    return {
        "receipt_key": receipt_key,
        "action": body.action,
        "plan_ids": plan_ids,
        "target_task_id": target_task_id,
    }


@router.post("/api/plans", response_model=PlanResource, status_code=201)
@serialize_related_plan_creation
async def create_plan(
    body: PlanCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _reject_durable_plan_secrets(body.input, body.title)
    target = None
    if body.target_task_id is not None:
        target = await db.get(Task, body.target_task_id)
        if target is None:
            raise HTTPException(404, "Target Task not found")
        await require_task_control(request, target, db)
        if not target.session_id:
            raise HTTPException(400, "Run the target Task before creating a session Plan")
        if target.shared_from_id is not None:
            raise HTTPException(409, "Shared shadow tasks cannot own Plans")
        if target.status == "migrating":
            raise HTTPException(409, "Plan target is changing execution location")
        project_id = target.project_id
        # A resumed Task may have moved into a worktree/subdirectory after its
        # original target was created. Freeze the same exact checkout used by
        # context/revision capture so the Plan runner cannot inspect one tree
        # and execute in another.
        target_repo = target.last_cwd or target.target_repo
        target_branch = target.target_branch
        worker_id = target.worker_id
        priority = target.priority
        timeout_hours = target.timeout_hours
    else:
        project_id = body.project_id
        target_repo = body.target_repo
        target_branch = body.target_branch
        worker_id = body.worker_id
        priority = body.priority
        timeout_hours = body.timeout_hours
        if project_id is not None:
            from backend.models.project import Project

            project = await db.get(Project, project_id)
            if project is None:
                raise HTTPException(404, "Project not found")
            if body.worker_id is not None and body.worker_id != project.worker_id:
                raise HTTPException(
                    400,
                    "Plan Worker must match the selected Project location",
                )
            # A Project is the authorization boundary for its checkout. Never
            # let a member pair shared Project access with an arbitrary path.
            target_repo = project.local_path
            worker_id = project.worker_id
        if settings.auth_token:
            if project_id is not None:
                await require_project_access(request, project_id, db)
            else:
                # Worker ownership is node-management authority, not a data
                # scope. Projectless standalone Plans are administrator-only.
                require_admin(request)
                await require_worker_target_access(request, worker_id, db)

    uploads = _validated_uploads(body)
    pipeline = resolve_plan_pipeline_config(
        None,
        base_config=await effective_plan_pipeline_config(db),
    )
    context = await _capture_context_for_plan(
        db, target=target, target_repo=target_repo, worker_id=worker_id
    )
    title = (
        body.title.strip()
        if body.title and body.title.strip()
        else (
            f"Plan for #{target.id}: {target.title}" if target is not None else body.input.strip().splitlines()[0]
        )
    )[:200]

    target_task_id = target.id if target is not None else None
    target_incarnation_id = target.incarnation_id if target is not None else None
    target_session_id = target.session_id if target is not None else None
    target_effect_probe = (
        SimpleNamespace(id=target_task_id, project_id=project_id)
        if target_task_id is not None
        else None
    )

    def target_snapshot_changed(locked_target: Task | None) -> bool:
        return bool(
            locked_target is None
            or locked_target.incarnation_id != target_incarnation_id
            or locked_target.session_id != target_session_id
            or not locked_target.session_id
            or locked_target.shared_from_id is not None
            or locked_target.status == "migrating"
            or locked_target.project_id != project_id
            or (locked_target.last_cwd or locked_target.target_repo)
            != target_repo
            or locked_target.target_branch != target_branch
            or locked_target.worker_id != worker_id
            or locked_target.priority != priority
            or locked_target.timeout_hours != timeout_hours
        )

    async def authorize_effect_boundary(locked_db: AsyncSession) -> None:
        """Fence the final ACL and routing snapshot in commit lock order."""

        if target_effect_probe is not None:
            await fence_worker_node_mutation(locked_db)
            locked_target = await lock_task_effect_access(
                request,
                target_effect_probe,
                locked_db,
                allow_chat_share=False,
                fence_worker_node=True,
                worker_node_fence_held=True,
                fence_worker_assignment=True,
            )
            if target_snapshot_changed(locked_target):
                raise HTTPException(
                    409,
                    "Plan target changed while creating the Plan",
                )
            return

        if project_id is not None:
            # This Plan does not exist yet, so its future creator is not an
            # independent ACL. Members must still hold the selected Project;
            # administrators bypass that ACL but lock the same Project row.
            locked_project = await _lock_standalone_plan_effect_access(
                request,
                locked_db,
                project_id=project_id,
                plan_created_by=None,
                worker_id=worker_id,
                fence_worker_assignment=True,
            )
            if (
                locked_project is None
                or locked_project.worker_id != worker_id
                or locked_project.local_path != target_repo
            ):
                raise HTTPException(
                    409,
                    "Plan Project changed while creating the Plan",
                )
            return

        # Projectless Plans are an administrator-only data scope. A JWT admin
        # must retain its exact active role through the commit; deployment
        # tokens have no mutable User row and retain legacy compatibility.
        if settings.auth_token:
            require_admin(request)
        await _lock_standalone_plan_effect_access(
            request,
            locked_db,
            project_id=None,
            plan_created_by=None,
            worker_id=worker_id,
            fence_worker_assignment=True,
        )
        if settings.auth_token:
            await require_worker_target_access(request, worker_id, locked_db)

    async def authorize_locked_creation(locked_db: AsyncSession) -> None:
        """Re-read the already-fenced target after Worker admission."""

        if target_task_id is None:
            return
        locked_target = await locked_db.get(
            Task,
            target_task_id,
            with_for_update=True,
            populate_existing=True,
        )
        if target_snapshot_changed(locked_target):
            raise HTTPException(
                409,
                "Plan target changed while creating the Plan",
            )

    plan, _run = await create_plan_with_run(
        db,
        title=title,
        initial_request=body.input.strip(),
        attachments=uploads,
        target_task_id=target_task_id,
        project_id=project_id,
        target_repo=target_repo,
        target_branch=target_branch,
        worker_id=worker_id,
        priority=priority,
        timeout_hours=timeout_hours,
        created_by=get_current_user_id(request),
        pipeline_config=pipeline.model_dump(mode="json"),
        context_session_id=context[0],
        context_log_id=context[1],
        context_snapshot=context[2],
        repo_revision=context[3],
        authorize_effect_boundary=authorize_effect_boundary,
        authorize_locked_creation=authorize_locked_creation,
    )
    await _wake_dispatcher()
    await broadcast_plan_event(
        event="plan_created", plan_id=plan.id, target_task_id=plan.target_task_id
    )
    return await plan_resource(db, plan, include_audit=True)


@router.post("/api/plans/worker-import")
async def import_worker_plan_run(
    body: WorkerPlanRunImportRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Create one inert Manager-owned mirror, then let this Worker dispatch it."""

    require_internal_service(request)
    uploads = _validated_uploads(body)
    attachment_receipt = _validate_attachment_manifest(
        uploads, body.attachment_manifest
    )
    import_digest = _worker_run_import_digest(body, attachment_receipt)

    # Import and exact cancellation mutate the same immutable Plan identity.
    # Serialize them within one API process; the permanent receipt primary key
    # remains the cross-process winner fence.
    async with plan_operation_lock(body.plan_id):
        return await _import_worker_plan_run_locked(
            body=body,
            db=db,
            uploads=uploads,
            attachment_receipt=attachment_receipt,
            import_digest=import_digest,
        )


async def _import_worker_plan_run_locked(
    *,
    body: WorkerPlanRunImportRequest,
    db: AsyncSession,
    uploads,
    attachment_receipt,
    import_digest: str,
):
    """Import one Worker mirror while holding its Plan operation lock."""

    # The imported Run is immediately dispatchable on this headless node.
    # Serialize the entire mirror transaction with the irreversible node drain
    # before taking any Task/Plan database writer locks.
    await fence_worker_node_mutation(db)

    target = await _fence_worker_mirror_target(db, body.target_task_id)
    project = (
        await db.get(Project, body.project_id) if body.project_id is not None else None
    )
    if body.project_id is not None and project is None:
        raise HTTPException(409, "Worker Plan project is missing")

    # The permanent receipt is the cross-request arbitration point between a
    # mutating import and an exact cancellation.  It is inserted in the same
    # transaction as the Run, so a cancellation that wins first leaves a
    # tombstone which a delayed import can never cross.
    import_receipt = await _load_worker_import_receipt(
        db,
        run_id=body.run_id,
        for_update=True,
    )
    claimed_new_identity = import_receipt is None
    if import_receipt is None:
        import_receipt = PlanAgentWorkerImportReceipt(
            run_id=body.run_id,
            plan_id=body.plan_id,
            protocol=1,
            relay_origin="manager_v1",
            payload_digest=import_digest,
            outcome="imported",
        )
        db.add(import_receipt)
        try:
            await db.flush()
        except IntegrityError:
            # Another server transaction won the run_id uniqueness fence.
            # Roll back our target/read snapshot and authenticate that exact
            # durable winner before considering an idempotent replay.
            await db.rollback()
            claimed_new_identity = False
            import_receipt = await _load_worker_import_receipt(
                db,
                run_id=body.run_id,
                for_update=True,
            )
            if import_receipt is None:
                raise HTTPException(
                    409,
                    "Worker Plan import identity changed concurrently",
                )
    _require_worker_import_receipt_identity(
        import_receipt,
        plan_id=body.plan_id,
        run_id=body.run_id,
        payload_digest=import_digest,
    )
    if import_receipt.outcome == "cancelled_before_import":
        raise HTTPException(
            409,
            "Worker Plan import was cancelled before admission",
        )
    if not claimed_new_identity:
        admitted_run = await db.get(
            PlanAgentRun,
            body.run_id,
            populate_existing=True,
        )
        if admitted_run is None:
            # Receipts intentionally survive graph deletion.  Recreating the
            # same Run from a late/replayed POST would resurrect old work.
            raise HTTPException(
                409,
                "Worker Plan import identity is historical and cannot be recreated",
            )
        admitted_plan = (
            await db.get(
                Plan,
                admitted_run.plan_id,
                populate_existing=True,
            )
            if admitted_run.plan_id is not None
            else None
        )
        if (
            admitted_plan is None
            or admitted_plan.id != body.plan_id
            or admitted_plan.relay_origin != "manager_v1"
            or admitted_run.plan_id != body.plan_id
            or admitted_run.relay_origin != "manager_v1"
            or admitted_run.import_receipt_protocol != 1
            or admitted_run.import_payload_digest != import_digest
        ):
            raise HTTPException(
                409,
                "Worker Plan Run id belongs to another immutable import identity",
            )
        return {
            "protocol": 3,
            "base_worker_version_id": admitted_run.base_version_id,
            "attachment_receipt": admitted_run.import_attachment_receipt or [],
            "import_payload_digest": admitted_run.import_payload_digest,
            "run": (await run_resource(db, admitted_run)).model_dump(mode="json"),
        }
    target_repo = (
        (target.last_cwd or target.target_repo)
        if target is not None
        else (project.local_path if project is not None else None)
    )

    plan = await db.get(Plan, body.plan_id)
    if plan is None:
        plan = Plan(
            id=body.plan_id,
            title=body.title,
            initial_request=body.initial_request,
            initial_attachments=uploads,
            target_task_id=body.target_task_id,
            project_id=body.project_id,
            target_repo=target_repo,
            target_branch=body.target_branch,
            worker_id=None,
            relay_origin="manager_v1",
            priority=body.priority,
            timeout_hours=body.timeout_hours,
            created_by=None,
            pipeline_config=body.pipeline_config.model_dump(mode="json"),
        )
        db.add(plan)
        try:
            await db.flush()
        except Exception as exc:
            await db.rollback()
            raise HTTPException(409, "Worker Plan id collides with local data") from exc
    else:
        await reject_capability_owned_plan_mutation(db, plan_ids=(plan.id,))
        if (
            plan.relay_origin != "manager_v1"
            or plan.initial_request != body.initial_request
            or plan.target_task_id != body.target_task_id
            or plan.project_id != body.project_id
            or plan.target_branch != body.target_branch
            or plan.priority != body.priority
            or plan.timeout_hours != body.timeout_hours
            or plan.pipeline_config != body.pipeline_config.model_dump(mode="json")
        ):
            raise HTTPException(409, "Worker Plan mirror identity changed")
    plan.title = body.title

    existing = await db.get(PlanAgentRun, body.run_id)
    if existing is not None:
        if (
            existing.plan_id != plan.id
            or existing.relay_origin != "manager_v1"
            or existing.import_receipt_protocol != 1
            or existing.import_payload_digest != import_digest
        ):
            raise HTTPException(409, "Worker Plan Run id collides with local data")
        await db.commit()
        return {
            "protocol": 3,
            "base_worker_version_id": existing.base_version_id,
            "attachment_receipt": existing.import_attachment_receipt or [],
            "import_payload_digest": existing.import_payload_digest,
            "run": (await run_resource(db, existing)).model_dump(mode="json"),
        }
    if plan.active_run_id is not None:
        raise HTTPException(409, f"Worker Plan already has active Run #{plan.active_run_id}")

    base_version = None
    if body.base_version is not None:
        base_version = await _materialize_worker_version(
            db,
            plan=plan,
            seed=body.base_version,
        )

    run = PlanAgentRun(
        id=body.run_id,
        plan_id=plan.id,
        plan_task_id=None,
        run_type=body.run_type,
        source_run_id=body.source_run_id,
        base_version_id=base_version.id if base_version is not None else None,
        request_text=body.request_text,
        attachments=uploads,
        context_session_id=body.context_session_id,
        context_log_id=body.context_log_id,
        context_snapshot=body.context_snapshot,
        repo_revision=body.repo_revision,
        current_stage="planner",
        generation=0,
        worker_id=None,
        relay_origin="manager_v1",
        import_payload_digest=import_digest,
        import_receipt_protocol=1,
        import_attachment_receipt=attachment_receipt,
        max_interactions=body.max_interactions,
        pipeline_config=body.pipeline_config.model_dump(mode="json"),
        status="queued",
        round=1,
    )
    db.add(run)
    try:
        await db.flush()
    except Exception as exc:
        await db.rollback()
        raise HTTPException(409, "Worker Plan Run id collides with local data") from exc
    plan.active_run_id = run.id
    plan.updated_at = datetime.utcnow()
    await db.commit()
    await _wake_dispatcher()
    return {
        "protocol": 3,
        "base_worker_version_id": run.base_version_id,
        "attachment_receipt": attachment_receipt,
        "import_payload_digest": import_digest,
        "run": (await run_resource(db, run)).model_dump(mode="json"),
    }


@router.get("/api/plan-runs/{run_id}/worker-import-audit")
async def audit_worker_plan_run_import(
    run_id: int,
    request: Request,
    plan_id: int = Query(ge=1),
    payload_digest: str = Query(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
    ),
    db: AsyncSession = Depends(get_db),
):
    """Read-only proof for an uncertain Manager -> Worker Plan import."""

    require_internal_service(request)
    # Receipt is the commit marker for an admitted/tombstoned identity. Read
    # it before the mutable Run so READ COMMITTED can never observe an
    # ``imported`` marker followed by a pre-commit missing Run.
    import_receipt = await _load_worker_import_receipt(
        db,
        run_id=run_id,
    )
    run = await db.get(PlanAgentRun, run_id, populate_existing=True)
    if import_receipt is None and run is not None:
        # Import may have committed between the two statements. Re-read its
        # atomic marker before classifying the visible Run as a collision.
        import_receipt = await _load_worker_import_receipt(
            db,
            run_id=run_id,
        )
    if run is None:
        if import_receipt is not None:
            _require_worker_import_receipt_identity(
                import_receipt,
                plan_id=plan_id,
                run_id=run_id,
                payload_digest=payload_digest,
            )
            if import_receipt.outcome == "imported":
                raise HTTPException(
                    409,
                    "Worker Plan import identity is historical and no longer exists",
                )
            return {
                "protocol": 1,
                "state": "cancelled",
                "plan_id": plan_id,
                "run_id": run_id,
                "payload_digest": payload_digest,
                "base_worker_version_id": None,
                "run": None,
                "versions": [],
            }
        return {
            "protocol": 1,
            "state": "absent",
            "plan_id": plan_id,
            "run_id": run_id,
            "payload_digest": payload_digest,
            "base_worker_version_id": None,
            "run": None,
            "versions": [],
        }
    plan = (
        await db.get(Plan, run.plan_id, populate_existing=True)
        if run.plan_id is not None
        else None
    )
    if (
        plan is None
        or import_receipt is None
        or run.plan_id != plan_id
        or plan.id != plan_id
        or plan.relay_origin != "manager_v1"
        or run.relay_origin != "manager_v1"
        or run.import_receipt_protocol != 1
        or run.import_payload_digest != payload_digest
    ):
        raise HTTPException(
            409,
            "Worker Plan Run id belongs to another immutable import identity",
        )
    _require_worker_import_receipt_identity(
        import_receipt,
        plan_id=plan_id,
        run_id=run_id,
        payload_digest=payload_digest,
    )
    if import_receipt.outcome != "imported":
        raise HTTPException(
            409,
            "Worker Plan Run contradicts its cancellation tombstone",
        )
    versions = list(
        (
            await db.execute(
                select(PlanVersion)
                .where(PlanVersion.produced_by_run_id == run.id)
                .order_by(PlanVersion.version_number, PlanVersion.id)
            )
        ).scalars()
    )
    return {
        "protocol": 1,
        "state": "matched",
        "plan_id": plan.id,
        "run_id": run.id,
        "payload_digest": payload_digest,
        "base_worker_version_id": run.base_version_id,
        "run": (await run_resource(db, run)).model_dump(mode="json"),
        "versions": [
            (await version_resource(db, version)).model_dump(mode="json")
            for version in versions
        ],
    }


@router.post(
    "/api/plans/worker-materialize-version",
    response_model=PlanVersionResource,
)
async def materialize_worker_plan_version(
    body: WorkerPlanVersionImportRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Materialize exact approved content before a Worker chat application."""

    require_internal_service(request)
    await fence_worker_node_mutation(db)
    target = await _fence_worker_mirror_target(db, body.target_task_id)
    project = (
        await db.get(Project, body.project_id)
        if body.project_id is not None
        else None
    )
    if body.project_id is not None and project is None:
        raise HTTPException(409, "Worker Plan project is missing")
    target_repo = (
        (target.last_cwd or target.target_repo)
        if target is not None
        else (project.local_path if project is not None else None)
    )
    plan = await db.get(Plan, body.plan_id)
    pipeline = body.pipeline_config.model_dump(mode="json")
    if plan is None:
        plan = Plan(
            id=body.plan_id,
            title=body.title,
            initial_request=body.initial_request,
            target_task_id=body.target_task_id,
            project_id=body.project_id,
            target_repo=target_repo,
            target_branch=body.target_branch,
            worker_id=None,
            relay_origin="manager_v1",
            priority=body.priority,
            timeout_hours=body.timeout_hours,
            created_by=None,
            pipeline_config=pipeline,
        )
        db.add(plan)
        try:
            await db.flush()
        except Exception as exc:
            await db.rollback()
            raise HTTPException(409, "Worker Plan id collides with local data") from exc
    else:
        await reject_capability_owned_plan_mutation(db, plan_ids=(plan.id,))
        if (
            plan.relay_origin != "manager_v1"
            or plan.initial_request != body.initial_request
            or plan.target_task_id != body.target_task_id
            or plan.project_id != body.project_id
            or plan.target_branch != body.target_branch
            or plan.pipeline_config != pipeline
        ):
            raise HTTPException(409, "Worker Plan mirror identity changed")
    plan.title = body.title
    if plan.active_run_id is not None:
        raise HTTPException(409, "Worker Plan has an active Run")
    version = await _materialize_worker_version(
        db,
        plan=plan,
        seed=body.version,
    )
    await db.commit()
    await db.refresh(version)
    return await version_resource(db, version)


@router.get("/api/plans", response_model=list[PlanResource])
async def list_plans(
    request: Request,
    target_task_id: int | None = None,
    kind: str | None = Query(default=None, pattern="^(standalone|related)$"),
    display_state: str | None = None,
    project_id: int | None = None,
    include_archived: bool = False,
    archived_only: bool = False,
    q: str | None = Query(default=None, max_length=200),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    display_states = {
        state.strip() for state in (display_state or "").split(",") if state.strip()
    }
    query = _plan_collection_query(
        request,
        target_task_id=target_task_id,
        kind=kind,
        project_id=project_id,
        include_archived=include_archived,
        archived_only=archived_only,
        q=q,
        display_states=display_states,
    )
    rows = list(
        (
            await db.execute(
                query.order_by(Plan.updated_at.desc(), Plan.id.desc())
                .limit(limit)
                .offset(offset)
            )
        ).scalars()
    )
    return await plan_resources(db, rows)


@router.get("/api/plans/count")
async def count_plans(
    request: Request,
    target_task_id: int | None = None,
    kind: str | None = Query(default=None, pattern="^(standalone|related)$"),
    display_state: str | None = None,
    project_id: int | None = None,
    include_archived: bool = False,
    archived_only: bool = False,
    q: str | None = Query(default=None, max_length=200),
    db: AsyncSession = Depends(get_db),
):
    """Count the same ACL-filtered projection exposed by ``list_plans``."""

    display_states = {
        state.strip() for state in (display_state or "").split(",") if state.strip()
    }
    query = _plan_collection_query(
        request,
        target_task_id=target_task_id,
        kind=kind,
        project_id=project_id,
        include_archived=include_archived,
        archived_only=archived_only,
        q=q,
        display_states=display_states,
    )
    total = int(
        await db.scalar(select(func.count()).select_from(query.subquery())) or 0
    )
    return {"total": total}


@router.get("/api/plans/resolve-legacy-task/{task_id}", response_model=PlanResource)
async def resolve_legacy_plan_task(
    task_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    link = await resolve_legacy_task(db, task_id)
    if link is None:
        raise HTTPException(404, "Legacy Plan Task link not found")
    plan = await _require_plan(request, db, link.plan_id)
    return await plan_resource(db, plan, include_audit=True)


@router.get("/api/plans/{plan_id}", response_model=PlanResource)
async def get_plan_resource(
    plan_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    plan = await _require_plan(request, db, plan_id)
    return await plan_resource(db, plan, include_audit=True)


@router.post("/api/plans/{plan_id}/application-deliveries/{receipt_key}/resolve")
async def resolve_plan_application_delivery(
    plan_id: int,
    receipt_key: str,
    body: _PlanDeliveryResolutionRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    require_admin(request)
    note = body.note.strip()
    if not note:
        raise HTTPException(422, "Resolution note cannot be blank")
    async with plan_operation_lock(plan_id):
        plan = await _require_plan(request, db, plan_id, control=True)
        await reject_capability_owned_plan_mutation(db, plan_ids=(plan.id,))
        receipt = (
            await db.execute(
                select(PlanApplicationReceipt).where(
                    PlanApplicationReceipt.receipt_key == receipt_key
                )
            )
        ).scalar_one_or_none()
        if receipt is None:
            raise HTTPException(404, "Plan application receipt not found")
        belongs_to_plan = await db.scalar(
            select(func.count(PlanVersion.id)).where(
                PlanVersion.plan_id == plan_id,
                PlanVersion.id.in_(receipt.plan_version_ids or []),
            )
        )
        if not belongs_to_plan:
            raise HTTPException(409, "Receipt does not apply this Plan")
        if receipt.delivery_status != "uncertain" and not (
            isinstance(receipt.delivery_resolution, dict)
            and receipt.delivery_resolution.get("action") == body.action
        ):
            raise HTTPException(
                409,
                f"Plan delivery is {receipt.delivery_status}, not uncertain",
            )
        if receipt.worker_id is not None:
            from backend.main import worker_proxy

            if worker_proxy is None:
                raise HTTPException(503, "Worker proxy is unavailable")
            worker = await worker_proxy.require_ready_worker(receipt.worker_id)
            await worker_proxy.resolve_plan_application_receipt(
                worker,
                receipt_key,
                action=body.action,
                note=note,
            )
        await db.rollback()
        from backend.main import dispatcher

        if dispatcher is None:
            raise HTTPException(503, "Dispatcher is unavailable")
        try:
            plan_ids, target_task_id = (
                await dispatcher.resolve_uncertain_plan_delivery(
                    receipt_key=receipt_key,
                    action=body.action,
                    note=note,
                    actor_id=get_current_user_id(request),
                )
            )
        except RuntimeError as exc:
            raise HTTPException(409, str(exc)) from exc
    await _broadcast_delivery_resolution(
        receipt_key=receipt_key,
        action=body.action,
        note=note,
        plan_ids=plan_ids,
        target_task_id=target_task_id,
    )
    return {
        "receipt_key": receipt_key,
        "action": body.action,
        "plan_ids": plan_ids,
        "target_task_id": target_task_id,
    }


@router.patch("/api/plans/{plan_id}", response_model=PlanResource)
async def patch_plan(
    plan_id: int,
    body: PlanPatchRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _reject_durable_plan_secrets(body.title)
    async with plan_operation_lock(plan_id):
        plan = await _require_plan(request, db, plan_id, control=True)
        await reject_capability_owned_plan_mutation(db, plan_ids=(plan.id,))
        if body.archived is True and plan.active_run_id is not None:
            raise HTTPException(409, "Cancel the active Plan Run before archiving")
        if body.archived is False and plan.archived_at is not None:
            await fence_plan_worker(db, worker_id=plan.worker_id)
        values: dict = {
            "lock_version": Plan.lock_version + 1,
            "updated_at": datetime.utcnow(),
        }
        if body.title is not None:
            values["title"] = body.title.strip()
        if body.archived is not None:
            values["archived_at"] = datetime.utcnow() if body.archived else None
        changed = await db.execute(
            update(Plan)
            .where(Plan.id == plan.id, Plan.lock_version == body.expected_lock_version)
            .values(**values)
        )
        if changed.rowcount != 1:
            await db.rollback()
            raise HTTPException(409, "Plan changed concurrently")
        await db.commit()
        plan = await db.get(Plan, plan_id)
        resource = await plan_resource(db, plan, include_audit=True)
    await broadcast_plan_event(
        event="plan_archived" if plan.archived_at else "plan_restored",
        plan_id=plan.id,
        target_task_id=plan.target_task_id,
        archived=plan.archived_at is not None,
    )
    return resource


@router.post("/api/plans/{plan_id}/runs", response_model=PlanRunResource, status_code=201)
async def create_run(
    plan_id: int,
    body: PlanRunCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _reject_durable_plan_secrets(body.request)
    uploads = _validated_uploads(body)
    async with plan_operation_lock(plan_id):
        plan = await _require_plan(request, db, plan_id, control=True)
        await reject_capability_owned_plan_mutation(db, plan_ids=(plan.id,))
        target = await db.get(Task, plan.target_task_id) if plan.target_task_id is not None else None
        if plan.target_task_id is not None and target is None:
            raise HTTPException(409, "Plan target no longer exists")
        if target is not None and target.status == "migrating":
            raise HTTPException(409, "Plan target is changing execution location")
        if target is not None:
            # Inactive Plan history is Manager-owned. A new Run follows the
            # target's current checkout/Worker and rehydrates its exact base
            # Version. The service persists this routing change atomically
            # with its active-Run claim after ending the API's old snapshot.
            run_worker_id = target.worker_id
            run_project_id = target.project_id
            run_target_repo = target.last_cwd or target.target_repo
            run_target_branch = target.target_branch
        else:
            run_worker_id = plan.worker_id
            run_project_id = plan.project_id
            run_target_repo = plan.target_repo
            run_target_branch = plan.target_branch
        source_run = None
        if body.run_type == "retry":
            if not is_admin(request):
                raise HTTPException(403, "Only administrators can retry failed Plan Runs")
            if body.source_run_id is None:
                raise HTTPException(422, "retry requires source_run_id")
            source_run = await db.get(PlanAgentRun, body.source_run_id)
            if (
                source_run is None
                or source_run.plan_id != plan.id
                or source_run.status != "failed"
                or source_run.finished_at is None
            ):
                raise HTTPException(409, "Retry source must be a terminal failed Run")
        elif body.source_run_id is not None:
            raise HTTPException(422, "source_run_id is only valid for retry")
        context = await _capture_context_for_plan(
            db,
            target=target,
            target_repo=run_target_repo,
            worker_id=run_worker_id,
        )

        target_incarnation_id = (
            target.incarnation_id if target is not None else None
        )
        target_session_id = target.session_id if target is not None else None
        target_effect_probe = (
            SimpleNamespace(id=target.id, project_id=target.project_id)
            if target is not None
            else None
        )
        plan_created_by = plan.created_by

        async def authorize_effect_boundary(
            locked_db: AsyncSession,
        ) -> None:
            if target is None:
                await _lock_standalone_plan_effect_access(
                    request,
                    locked_db,
                    project_id=run_project_id,
                    plan_created_by=plan_created_by,
                    worker_id=run_worker_id,
                    fence_worker_assignment=True,
                )
                return
            assert target_effect_probe is not None
            admitted_target = await lock_task_effect_access(
                request,
                target_effect_probe,
                locked_db,
                allow_chat_share=False,
                fence_worker_node=True,
                fence_worker_assignment=True,
            )
            if (
                admitted_target.incarnation_id != target_incarnation_id
                or admitted_target.session_id != target_session_id
                or admitted_target.status == "migrating"
                or admitted_target.project_id != run_project_id
                or (admitted_target.last_cwd or admitted_target.target_repo)
                != run_target_repo
                or admitted_target.target_branch != run_target_branch
                or admitted_target.worker_id != run_worker_id
            ):
                raise HTTPException(
                    409,
                    "Plan target changed before Run effect admission",
                )

        async def authorize_locked_plan(
            locked_db: AsyncSession,
            locked_plan: Plan,
        ) -> None:
            if locked_plan.target_task_id is not None:
                locked_target = await locked_db.get(
                    Task,
                    locked_plan.target_task_id,
                    with_for_update=True,
                    populate_existing=True,
                )
                if (
                    locked_target is None
                    or locked_target.incarnation_id != target_incarnation_id
                    or locked_target.session_id != target_session_id
                    or locked_target.status == "migrating"
                    or locked_target.project_id != run_project_id
                    or (locked_target.last_cwd or locked_target.target_repo)
                    != run_target_repo
                    or locked_target.target_branch != run_target_branch
                    or locked_target.worker_id != run_worker_id
                ):
                    raise HTTPException(
                        409,
                        "Plan target changed while creating the Run",
                    )
                await _refresh_plan_acl_dependencies(
                    locked_db,
                    worker_id=locked_target.worker_id,
                    project_id=locked_target.project_id,
                )
            else:
                await _refresh_plan_acl_dependencies(
                    locked_db,
                    worker_id=locked_plan.worker_id,
                    project_id=locked_plan.project_id,
                )
            if not await _has_plan_access(
                request,
                locked_plan,
                locked_db,
                control=True,
            ):
                raise HTTPException(403, "No permission to control this Plan")

        run = await create_plan_run(
            db,
            plan=plan,
            run_type=body.run_type,
            request_text=body.request.strip(),
            attachments=uploads,
            base_version_id=body.base_version_id,
            expected_current_version_id=body.expected_current_version_id,
            context_session_id=context[0],
            context_log_id=context[1],
            context_snapshot=context[2],
            repo_revision=context[3],
            project_id=run_project_id,
            target_repo=run_target_repo,
            target_branch=run_target_branch,
            worker_id=run_worker_id,
            source_run_id=source_run.id if source_run is not None else None,
            authorize_effect_boundary=authorize_effect_boundary,
            authorize_locked_plan=authorize_locked_plan,
        )
    await _wake_dispatcher()
    await broadcast_plan_event(
        event="plan_run_created",
        plan_id=plan_id,
        target_task_id=plan.target_task_id,
        run_id=run.id,
        status=run.status,
    )
    return await run_resource(db, run)


@router.post("/api/plans/{plan_id}/fork", response_model=PlanResource, status_code=201)
async def fork_plan(
    plan_id: int,
    body: PlanForkRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    _reject_durable_plan_secrets(body.title, body.request)
    async with plan_operation_lock(plan_id):
        source = await _require_plan(request, db, plan_id, control=True)
        await reject_capability_owned_plan_mutation(db, plan_ids=(source.id,))
        if source.archived_at is not None:
            raise HTTPException(409, "Archived Plan cannot be forked")
        version = await db.get(PlanVersion, body.base_version_id)
        if version is None or version.plan_id != source.id:
            raise HTTPException(400, "Fork Version does not belong to this Plan")
        target = (
            await db.get(Task, source.target_task_id)
            if source.target_task_id is not None
            else None
        )
        if target is not None and target.status == "migrating":
            raise HTTPException(409, "Plan target is changing execution location")
        if target is not None:
            fork_worker_id = target.worker_id
            fork_project_id = target.project_id
            fork_target_repo = target.last_cwd or target.target_repo
            fork_target_branch = target.target_branch
            fork_target_incarnation_id = target.incarnation_id
            fork_target_session_id = target.session_id
        else:
            fork_worker_id = source.worker_id
            fork_project_id = source.project_id
            fork_target_repo = source.target_repo
            fork_target_branch = source.target_branch
            fork_target_incarnation_id = None
            fork_target_session_id = None
        context = await _capture_context_for_plan(
            db,
            target=target,
            target_repo=fork_target_repo,
            worker_id=fork_worker_id,
        )
        request_text = (
            body.request.strip()
            if body.request
            else (
                f"Fork this planning direction from v{version.version_number}."
                f"\n\n{version.content}"
            )
        )

        source_lock_version = source.lock_version
        source_target_task_id = source.target_task_id
        source_project_id = source.project_id
        source_target_repo = source.target_repo
        source_target_branch = source.target_branch
        source_worker_id = source.worker_id
        source_priority = source.priority
        source_timeout_hours = source.timeout_hours
        source_created_by = source.created_by
        source_version_number = version.version_number
        source_version_content = version.content
        fork_target_probe = (
            SimpleNamespace(id=target.id, project_id=target.project_id)
            if target is not None
            else None
        )

        async def authorize_fork_effect_boundary(
            locked_db: AsyncSession,
        ) -> None:
            """Fence Task/Worker before mutable ACL authority for the fork."""

            if fork_target_probe is not None:
                admitted_target = await lock_task_effect_access(
                    request,
                    fork_target_probe,
                    locked_db,
                    allow_chat_share=False,
                    fence_worker_node=True,
                    fence_worker_assignment=True,
                )
                if (
                    admitted_target.incarnation_id
                    != fork_target_incarnation_id
                    or admitted_target.session_id != fork_target_session_id
                    or admitted_target.status == "migrating"
                    or admitted_target.project_id != fork_project_id
                    or (
                        admitted_target.last_cwd
                        or admitted_target.target_repo
                    )
                    != fork_target_repo
                    or admitted_target.target_branch != fork_target_branch
                    or admitted_target.worker_id != fork_worker_id
                ):
                    raise HTTPException(
                        409,
                        "Source Plan target changed while creating the fork",
                    )
                return
            await _lock_standalone_plan_effect_access(
                request,
                locked_db,
                project_id=fork_project_id,
                plan_created_by=source_created_by,
                worker_id=fork_worker_id,
                fence_worker_assignment=True,
            )

        async def authorize_locked_fork(locked_db: AsyncSession) -> None:
            # Lock the source aggregate itself.  The new fork's Task/Worker
            # fences do not protect a standalone source Plan from an archive,
            # owner change, or concurrent Version publication.
            fenced_source = await locked_db.execute(
                update(Plan)
                .where(
                    Plan.id == source.id,
                    Plan.lock_version == source_lock_version,
                    Plan.archived_at.is_(None),
                )
                .values(updated_at=Plan.updated_at)
            )
            if fenced_source.rowcount != 1:
                raise HTTPException(
                    409,
                    "Source Plan changed while creating the fork",
                )
            locked_source = await locked_db.get(
                Plan,
                source.id,
                with_for_update=True,
                populate_existing=True,
            )
            if locked_source is not None:
                if locked_source.target_task_id is not None:
                    locked_target = await locked_db.get(
                        Task,
                        locked_source.target_task_id,
                        with_for_update=True,
                        populate_existing=True,
                    )
                    if locked_target is None:
                        raise HTTPException(
                            409,
                            "Source Plan target changed while creating the fork",
                        )
                    if (
                        locked_target.incarnation_id
                        != fork_target_incarnation_id
                        or locked_target.session_id != fork_target_session_id
                        or locked_target.status == "migrating"
                        or locked_target.project_id != fork_project_id
                        or (locked_target.last_cwd or locked_target.target_repo)
                        != fork_target_repo
                        or locked_target.target_branch != fork_target_branch
                        or locked_target.worker_id != fork_worker_id
                    ):
                        raise HTTPException(
                            409,
                            "Source Plan target changed while creating the fork",
                        )
                    await _refresh_plan_acl_dependencies(
                        locked_db,
                        worker_id=locked_target.worker_id,
                        project_id=locked_target.project_id,
                    )
                else:
                    await _refresh_plan_acl_dependencies(
                        locked_db,
                        worker_id=locked_source.worker_id,
                        project_id=locked_source.project_id,
                    )
            if locked_source is None or not await _has_plan_access(
                request,
                locked_source,
                locked_db,
                control=True,
            ):
                raise HTTPException(403, "No permission to control this Plan")
            await reject_capability_owned_plan_mutation(
                locked_db,
                plan_ids=(locked_source.id,),
            )
            locked_version = await locked_db.get(
                PlanVersion,
                version.id,
                with_for_update=True,
                populate_existing=True,
            )
            if (
                locked_source.archived_at is not None
                or locked_source.lock_version != source_lock_version
                or locked_source.target_task_id != source_target_task_id
                or locked_source.project_id != source_project_id
                or locked_source.target_repo != source_target_repo
                or locked_source.target_branch != source_target_branch
                or locked_source.worker_id != source_worker_id
                or locked_source.priority != source_priority
                or locked_source.timeout_hours != source_timeout_hours
                or locked_version is None
                or locked_version.plan_id != locked_source.id
                or locked_version.version_number != source_version_number
                or locked_version.content != source_version_content
            ):
                raise HTTPException(
                    409,
                    "Source Plan changed while creating the fork",
                )

        fork, _run = await create_plan_with_run(
            db,
            title=(
                body.title.strip() if body.title else f"Fork of {source.title}"
            )[:200],
            initial_request=request_text,
            attachments=deepcopy(source.initial_attachments),
            target_task_id=source.target_task_id,
            project_id=fork_project_id,
            target_repo=fork_target_repo,
            target_branch=fork_target_branch,
            worker_id=fork_worker_id,
            priority=source.priority,
            timeout_hours=source.timeout_hours,
            created_by=get_current_user_id(request),
            pipeline_config=deepcopy(source.pipeline_config),
            context_session_id=context[0],
            context_log_id=context[1],
            context_snapshot=context[2],
            repo_revision=context[3],
            forked_from_version_id=version.id,
            base_version_id=version.id,
            run_type="fork",
            authorize_effect_boundary=authorize_fork_effect_boundary,
            authorize_locked_creation=authorize_locked_fork,
        )
        resource = await plan_resource(db, fork, include_audit=True)
    await _wake_dispatcher()
    await broadcast_plan_event(
        event="plan_created",
        plan_id=fork.id,
        target_task_id=fork.target_task_id,
        forked_from_plan_id=source.id,
    )
    return resource


@router.get("/api/plans/{plan_id}/versions", response_model=list[PlanVersionResource])
async def list_versions(
    plan_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    await _require_plan(request, db, plan_id)
    rows = (
        await db.execute(
            select(PlanVersion).where(PlanVersion.plan_id == plan_id).order_by(PlanVersion.version_number.desc())
        )
    ).scalars()
    return [await version_resource(db, row) for row in rows]


@router.get("/api/plan-versions/{version_id}", response_model=PlanVersionResource)
async def get_version(
    version_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    _plan, version = await _require_version(request, db, version_id)
    return await version_resource(db, version)


@router.get("/api/plan-versions/{version_id}/staleness")
async def get_version_staleness(
    version_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    plan, version = await _require_version(request, db, version_id)
    return await _version_staleness(db, plan, version)


async def _decide(
    *, version_id: int, body: PlanDecisionRequest, request: Request,
    db: AsyncSession, decision: str,
) -> PlanVersionResource:
    plan, _ = await _require_version(request, db, version_id, control=True)
    async with plan_operation_lock(plan.id):
        plan, version = await _require_version(
            request,
            db,
            version_id,
            control=True,
        )
        routing_identity = {
            "plan_id": plan.id,
            "target_task_id": plan.target_task_id,
            "project_id": plan.project_id,
            "created_by": plan.created_by,
        }
        if plan.target_task_id is not None:
            target = await db.get(Task, plan.target_task_id)
            if target is None:
                # A deleted target is a repository/context hard conflict, not
                # an authorization-shaped error.  Preserve the structured
                # staleness contract so callers can distinguish a missing
                # execution target from a revoked ACL without attempting a
                # decision write.
                stale = await _version_staleness(db, plan, version)
                if stale["hard_conflict"]:
                    raise HTTPException(
                        409,
                        {
                            "code": "plan_hard_conflict",
                            "message": "Plan Version cannot be decided",
                            **stale,
                        },
                    )
                raise HTTPException(409, "Plan target Task is unavailable")
            await lock_task_effect_access(
                request,
                target,
                db,
                allow_chat_share=False,
                fence_worker_node=True,
            )
        else:
            # End the preliminary ACL/staleness snapshot before taking the
            # portable Worker-node -> Project -> User writer sequence.
            await db.rollback()
            await _lock_standalone_plan_effect_access(
                request,
                db,
                project_id=routing_identity["project_id"],
                plan_created_by=routing_identity["created_by"],
            )
        plan, version = await _require_version(
            request,
            db,
            version_id,
            control=True,
        )
        if routing_identity != {
            "plan_id": plan.id,
            "target_task_id": plan.target_task_id,
            "project_id": plan.project_id,
            "created_by": plan.created_by,
        }:
            await db.rollback()
            raise HTTPException(
                409,
                "Plan routing changed while authorizing the decision",
            )
        await reject_capability_owned_plan_mutation(db, plan_ids=(plan.id,))
        stale = await _version_staleness(db, plan, version)
        if stale["hard_conflict"]:
            raise HTTPException(
                409,
                {"code": "plan_hard_conflict", "message": "Plan Version cannot be decided", **stale},
            )
        if decision == "approved" and stale["stale"] and not body.confirm_stale:
            raise HTTPException(409, {"message": "Plan Version context is stale", **stale})
        version = await decide_version(
            db, plan=plan, version=version, decision=decision,
            decided_by=get_current_user_id(request),
            expected_current_version_id=body.expected_current_version_id,
        )
    await broadcast_plan_event(
        event="plan_version_decided",
        plan_id=plan.id,
        target_task_id=plan.target_task_id,
        version_id=version.id,
        decision=decision,
    )
    return await version_resource(db, version)


@router.post("/api/plan-versions/{version_id}/approve", response_model=PlanVersionResource)
async def approve_version(
    version_id: int, body: PlanDecisionRequest, request: Request,
    db: AsyncSession = Depends(get_db),
):
    return await _decide(
        version_id=version_id, body=body, request=request, db=db, decision="approved"
    )


@router.post("/api/plan-versions/{version_id}/reject", response_model=PlanVersionResource)
async def reject_version(
    version_id: int, body: PlanDecisionRequest, request: Request,
    db: AsyncSession = Depends(get_db),
):
    return await _decide(
        version_id=version_id, body=body, request=request, db=db, decision="rejected"
    )


@router.get("/api/plans/{plan_id}/runs", response_model=list[PlanRunResource])
async def list_runs(
    plan_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    await _require_plan(request, db, plan_id)
    rows = (
        await db.execute(
            select(PlanAgentRun).where(PlanAgentRun.plan_id == plan_id).order_by(PlanAgentRun.id.desc())
        )
    ).scalars()
    return [await run_resource(db, row) for row in rows]


@router.get("/api/plan-runs/{run_id}", response_model=PlanRunResource)
async def get_run(
    run_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    run = await db.get(PlanAgentRun, run_id)
    if run is None or run.plan_id is None:
        raise HTTPException(404, "Plan Run not found")
    await _require_plan(request, db, run.plan_id)
    return await run_resource(db, run)


async def _finish_local_plan_run_cancellation(
    db: AsyncSession,
    *,
    run_id: int,
    plan_id: int,
    owned_instance_id: int | None,
    target_generation: int | None,
) -> tuple[Plan, PlanAgentRun]:
    """Reap and terminalize one already-fenced local Plan runtime."""

    try:
        from backend.main import dispatcher, instance_manager

        stopped = (
            await dispatcher.stop_plan_run_lifecycle(run_id, owned_instance_id)
            if dispatcher is not None
            else False
        )
        if not stopped:
            from backend.services.plan_agent_runner import cancel_plan_run_runtime

            await cancel_plan_run_runtime(run_id)
        if target_generation is None:
            raise RuntimeError("Plan Run cancellation generation disappeared")
        from backend.services.plan_runtime_receipt import (
            reconcile_runtime_generation,
        )

        runtime_db_factory = async_sessionmaker(
            bind=db.bind,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        if not await reconcile_runtime_generation(
            runtime_db_factory,
            instance_manager,
            run_id=run_id,
            generation=target_generation,
            allow_transport_kill=False,
        ):
            raise RuntimeError(
                f"Plan Run #{run_id} runtime cleanup is not durable"
            )
        async with plan_operation_lock(plan_id):
            run = await db.get(
                PlanAgentRun,
                run_id,
                with_for_update=True,
                populate_existing=True,
            )
            if run is None or run.plan_id != plan_id:
                raise RuntimeError("Plan Run disappeared during cancellation")
            plan = await db.get(
                Plan,
                run.plan_id,
                with_for_update=True,
                populate_existing=True,
            )
            if plan is None:
                raise RuntimeError("Plan Run disappeared during cancellation")
            run = await release_run_owner_after_cleanup(
                db,
                plan=plan,
                run=run,
            )
            # The owner-release helper commits; reacquire Run -> Plan before
            # publishing the terminal state.
            run = await db.get(
                PlanAgentRun,
                run_id,
                with_for_update=True,
                populate_existing=True,
            )
            if run is None or run.plan_id != plan_id:
                raise RuntimeError("Plan Run disappeared during cancellation")
            plan = await db.get(
                Plan,
                run.plan_id,
                with_for_update=True,
                populate_existing=True,
            )
            if plan is None:
                raise RuntimeError("Plan Run disappeared during cancellation")
            run = await finalize_run_cancellation(
                db,
                plan=plan,
                run=run,
            )
            return plan, run
    except HTTPException:
        raise
    except Exception as exc:
        # The durable ``cancelling`` fence remains visible for retry/recovery.
        raise HTTPException(
            409,
            f"Plan Run cancellation is fenced, but runtime cleanup is not "
            f"confirmed: {exc}",
        ) from exc


@router.post("/api/plan-runs/{run_id}/worker-import-cancel")
async def cancel_worker_imported_plan_run(
    run_id: int,
    body: WorkerPlanRunCancelRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Cancel only the exact immutable Manager import, never a run_id peer."""

    require_internal_service(request)
    payload_digest = body.payload_digest
    async with plan_operation_lock(body.plan_id):
        # Exact cancellation is itself a Worker-local graph mutation: it may
        # create a permanent pre-import tombstone or publish a terminal
        # Run/Step/Input graph.  Serialize it with the irreversible node drain
        # before taking the import-receipt or Plan writers so a completed drain
        # proof cannot be invalidated by a late Manager cancellation RPC.
        await fence_worker_node_mutation(db)
        import_receipt = await _load_worker_import_receipt(
            db,
            run_id=run_id,
            for_update=True,
        )
        if import_receipt is None:
            # A local/foreign Run collision must never be touched or hidden by
            # a tombstone for a different Manager identity.
            colliding_run = await db.get(
                PlanAgentRun,
                run_id,
                populate_existing=True,
            )
            if colliding_run is not None:
                raise HTTPException(
                    409,
                    "Worker Plan Run id belongs to another immutable import identity",
                )
            import_receipt = PlanAgentWorkerImportReceipt(
                run_id=run_id,
                plan_id=body.plan_id,
                protocol=1,
                relay_origin="manager_v1",
                payload_digest=payload_digest,
                outcome="cancelled_before_import",
            )
            db.add(import_receipt)
            try:
                await db.commit()
            except IntegrityError:
                # Import and cancellation may race in different API server
                # processes.  The run_id primary key chooses one winner.
                await db.rollback()
                import_receipt = await _load_worker_import_receipt(
                    db,
                    run_id=run_id,
                    for_update=True,
                )
                if import_receipt is None:
                    raise HTTPException(
                        409,
                        "Worker Plan import identity changed concurrently",
                    )
            else:
                return {
                    "protocol": 1,
                    "state": "absent",
                    "plan_id": body.plan_id,
                    "run_id": run_id,
                    "payload_digest": payload_digest,
                    "base_worker_version_id": None,
                    "run": None,
                    "versions": [],
                }

        _require_worker_import_receipt_identity(
            import_receipt,
            plan_id=body.plan_id,
            run_id=run_id,
            payload_digest=payload_digest,
        )
        if import_receipt.outcome == "cancelled_before_import":
            await db.commit()
            return {
                "protocol": 1,
                "state": "absent",
                "plan_id": body.plan_id,
                "run_id": run_id,
                "payload_digest": payload_digest,
                "base_worker_version_id": None,
                "run": None,
                "versions": [],
            }

        run = await db.get(
            PlanAgentRun,
            run_id,
            with_for_update=True,
            populate_existing=True,
        )
        if run is None:
            # Imported receipts survive exact graph deletion.  The missing Run
            # is therefore authenticated historical absence, not permission
            # to recreate or cancel another object.
            await db.commit()
            return {
                "protocol": 1,
                "state": "absent",
                "plan_id": body.plan_id,
                "run_id": run_id,
                "payload_digest": payload_digest,
                "base_worker_version_id": None,
                "run": None,
                "versions": [],
            }
        plan = await db.get(
            Plan,
            body.plan_id,
            with_for_update=True,
            populate_existing=True,
        )
        if (
            plan is None
            or plan.id != body.plan_id
            or plan.relay_origin != "manager_v1"
            or run.plan_id != plan.id
            or run.relay_origin != "manager_v1"
            or run.import_receipt_protocol != 1
            or run.import_payload_digest != payload_digest
        ):
            raise HTTPException(
                409,
                "Worker Plan Run id belongs to another immutable import identity",
            )
        if run.status == "cancelled":
            versions = list(
                (
                    await db.execute(
                        select(PlanVersion)
                        .where(PlanVersion.produced_by_run_id == run.id)
                        .order_by(PlanVersion.version_number, PlanVersion.id)
                    )
                ).scalars()
            )
            await db.commit()
            return {
                "protocol": 1,
                "state": "terminal",
                "plan_id": plan.id,
                "run_id": run.id,
                "payload_digest": payload_digest,
                "base_worker_version_id": run.base_version_id,
                "run": (await run_resource(db, run)).model_dump(mode="json"),
                "versions": [
                    (await version_resource(db, version)).model_dump(mode="json")
                    for version in versions
                ],
            }
        if run.status in {"completed", "failed"}:
            versions = list(
                (
                    await db.execute(
                        select(PlanVersion)
                        .where(PlanVersion.produced_by_run_id == run.id)
                        .order_by(PlanVersion.version_number, PlanVersion.id)
                    )
                ).scalars()
            )
            await db.commit()
            return {
                "protocol": 1,
                "state": "terminal",
                "plan_id": plan.id,
                "run_id": run.id,
                "payload_digest": payload_digest,
                "base_worker_version_id": run.base_version_id,
                "run": (await run_resource(db, run)).model_dump(mode="json"),
                "versions": [
                    (await version_resource(db, version)).model_dump(mode="json")
                    for version in versions
                ],
            }
        run = await cancel_run(db, plan=plan, run=run)
        owned_instance_id = run.instance_id
        target_generation = run.cancellation_target_generation

    plan, run = await _finish_local_plan_run_cancellation(
        db,
        run_id=run_id,
        plan_id=body.plan_id,
        owned_instance_id=owned_instance_id,
        target_generation=target_generation,
    )
    await broadcast_plan_event(
        event="plan_run_status_changed",
        plan_id=plan.id,
        target_task_id=plan.target_task_id,
        run_id=run.id,
        status=run.status,
    )
    return {
        "protocol": 1,
        "state": "terminal",
        "plan_id": plan.id,
        "run_id": run.id,
        "payload_digest": payload_digest,
        "base_worker_version_id": run.base_version_id,
        "run": (await run_resource(db, run)).model_dump(mode="json"),
        "versions": [
            (await version_resource(db, version)).model_dump(mode="json")
            for version in (
                await db.execute(
                    select(PlanVersion)
                    .where(PlanVersion.produced_by_run_id == run.id)
                    .order_by(PlanVersion.version_number, PlanVersion.id)
                )
            ).scalars()
        ],
    }


@router.post("/api/plan-runs/{run_id}/cancel", response_model=PlanRunResource)
async def cancel_plan_run(
    run_id: int, request: Request, db: AsyncSession = Depends(get_db)
):
    run = await db.get(PlanAgentRun, run_id)
    if run is None or run.plan_id is None:
        raise HTTPException(404, "Plan Run not found")
    frozen_plan_id = run.plan_id
    async with (
        _stop_plan_run_lifecycle_after_fence(run_id) as lifecycle_stop,
        plan_operation_lock(frozen_plan_id),
    ):
        plan = await _require_plan(request, db, frozen_plan_id, control=True)
        await reject_capability_owned_plan_mutation(db, plan_ids=(plan.id,))
        run = await db.get(PlanAgentRun, run_id, populate_existing=True)
        if run is None:
            raise HTTPException(
                409,
                "Worker Plan Run mirror changed before cancellation",
            )
        if run.status == "cancelled":
            return await run_resource(db, run)
        worker_id = run.worker_id
        if worker_id is not None:
            if run.status not in ACTIVE_RUN_STATUSES and run.status != "cancelling":
                raise HTTPException(409, "Worker Plan Run is no longer active")
            from backend.main import dispatcher, worker_proxy
            from backend.services.worker_plan_dispatch import (
                WorkerPlanDispatchConflict,
                apply_worker_terminal_after_cancellation_race,
                fence_worker_mirror_cancellation,
                finalize_worker_mirror_cancellation,
                snapshot_worker_dispatch_receipt,
            )

            if worker_proxy is None:
                raise HTTPException(503, "Worker Plan runtime is unavailable")

            # Freeze the Manager aggregate and its complete dispatch history.
            # A current ``prepared`` row proves this generation has not crossed
            # the remote boundary; any historical digest proves that the same
            # immutable Worker Run may already exist and therefore requires an
            # exact identity-bound RPC even before the next claim is launched.
            run = await db.get(
                PlanAgentRun,
                run_id,
                with_for_update=True,
                populate_existing=True,
            )
            plan = await db.get(
                Plan,
                plan.id,
                with_for_update=True,
                populate_existing=True,
            )
            if run is None or plan is None:
                raise HTTPException(
                    409,
                    "Worker Plan Run mirror changed before cancellation",
                )
            dispatch_receipts = list(
                (
                    await db.execute(
                        select(PlanAgentWorkerDispatchReceipt)
                        .where(PlanAgentWorkerDispatchReceipt.run_id == run_id)
                        .order_by(
                            PlanAgentWorkerDispatchReceipt.run_generation,
                            PlanAgentWorkerDispatchReceipt.id,
                        )
                        .with_for_update()
                    )
                ).scalars()
            )
            try:
                snapshots = [
                    snapshot_worker_dispatch_receipt(receipt)
                    for receipt in dispatch_receipts
                ]
            except WorkerPlanDispatchConflict as exc:
                raise HTTPException(409, str(exc)) from exc
            target_generation = (
                run.cancellation_target_generation
                if run.status == "cancelling"
                else run.generation
            )
            if (
                run.plan_id != plan.id
                or run.worker_id != worker_id
                or plan.worker_id != worker_id
                or plan.active_run_id != run.id
                or run.status not in {*ACTIVE_RUN_STATUSES, "cancelling"}
                or target_generation is None
                or (
                    run.status == "cancelling"
                    and run.generation != target_generation + 1
                )
                or any(
                    snapshot.plan_id != plan.id
                    or snapshot.run_id != run.id
                    or snapshot.target_task_id != plan.target_task_id
                    or snapshot.worker_id != worker_id
                    or snapshot.run_generation > target_generation
                    for snapshot in snapshots
                )
            ):
                raise HTTPException(
                    409,
                    "Worker Plan Run mirror changed before cancellation",
                )
            payload_digests = {
                snapshot.payload_digest
                for snapshot in snapshots
                if snapshot.payload_digest is not None
            }
            if len(payload_digests) > 1:
                raise HTTPException(
                    409,
                    "Worker Plan dispatch history changed immutable identity",
                )
            payload_digest = next(iter(payload_digests), None)
            current_receipt = next(
                (
                    receipt
                    for receipt in dispatch_receipts
                    if receipt.run_generation == target_generation
                ),
                None,
            )

            if payload_digest is None:
                if run.status == "cancelling":
                    raise HTTPException(
                        409,
                        "Worker Plan cancellation lost its exact remote identity",
                    )
                if current_receipt is None and run.status != "queued":
                    raise HTTPException(
                        409,
                        "Worker Plan Run has no pre-import cancellation proof",
                    )
                if current_receipt is not None:
                    if current_receipt.status != "prepared":
                        raise HTTPException(
                            409,
                            "Worker Plan Run has no exact remote identity",
                        )
                # This transaction owns Run -> Plan -> receipt while publishing
                # the terminal mirror, so a concurrent boundary callback sees
                # the settled fence and cannot issue its import POST.
                try:
                    run = await _finish_plan_cancel_mutation(
                        cancel_worker_mirror_run_after_ack(
                            db,
                            plan=plan,
                            run=run,
                            receipt_settlement_reason="not_launched",
                            receipt_remote_status=None,
                        ),
                        lifecycle_stop=lifecycle_stop,
                        dispatcher=dispatcher,
                    )
                except WorkerPlanDispatchConflict as exc:
                    # ``cancel_worker_mirror_run_after_ack`` deliberately ends
                    # the stale WAL snapshot before its Run-first writer fence.
                    # If on_remote_possible commits in that window, its receipt
                    # is authoritative and this pre-import proof is stale.
                    raise HTTPException(409, str(exc)) from exc
            else:
                expected_generation = target_generation
                expected_plan_id = plan.id
                if run.status != "cancelling":
                    try:
                        run = await _finish_plan_cancel_mutation(
                            fence_worker_mirror_cancellation(
                                db,
                                plan_id=expected_plan_id,
                                run_id=run_id,
                                worker_id=worker_id,
                                generation=expected_generation,
                                payload_digest=payload_digest,
                            ),
                            lifecycle_stop=lifecycle_stop,
                            dispatcher=dispatcher,
                        )
                    except WorkerPlanDispatchConflict as exc:
                        raise HTTPException(409, str(exc)) from exc
                else:
                    await _finish_plan_cancel_mutation(
                        db.commit(),
                        lifecycle_stop=lifecycle_stop,
                        dispatcher=dispatcher,
                    )
                if dispatcher is not None:
                    # Every retry of a durable intent must keep cold recovery
                    # armed. A concurrent exact-recovery lifecycle is not a
                    # stale dispatch and is deliberately excluded from the
                    # generic lifecycle reap below.
                    request_recovery = getattr(
                        dispatcher,
                        "_request_plan_runtime_recovery",
                        None,
                    )
                    if callable(request_recovery):
                        request_recovery()
                try:
                    remote = await worker_proxy.cancel_versioned_plan_run(
                        worker_id,
                        run_id,
                        plan_id=expected_plan_id,
                        payload_digest=payload_digest,
                    )
                    if (
                        remote.get("protocol") != 1
                        or remote.get("state")
                        not in {"absent", "terminal"}
                        or type(remote.get("plan_id")) is not int
                        or remote.get("plan_id") != expected_plan_id
                        or type(remote.get("run_id")) is not int
                        or remote.get("run_id") != run_id
                        or remote.get("payload_digest") != payload_digest
                        or (
                            remote.get("state") == "absent"
                            and (
                                remote.get("run") is not None
                                or "base_worker_version_id" not in remote
                                or remote.get("base_worker_version_id") is not None
                                or remote.get("versions") != []
                            )
                        )
                        or (
                            remote.get("state") == "terminal"
                            and (
                                not isinstance(remote.get("run"), dict)
                                or type(remote["run"].get("id")) is not int
                                or remote["run"].get("id") != run_id
                                or type(remote["run"].get("plan_id")) is not int
                                or remote["run"].get("plan_id")
                                != expected_plan_id
                                or remote["run"].get("status")
                                not in {"completed", "failed", "cancelled"}
                                or not isinstance(remote.get("versions"), list)
                                or isinstance(
                                    remote.get("base_worker_version_id"),
                                    bool,
                                )
                                or (
                                    remote.get("base_worker_version_id")
                                    is not None
                                    and not isinstance(
                                        remote.get("base_worker_version_id"),
                                        int,
                                    )
                                )
                            )
                        )
                    ):
                        raise RuntimeError(
                            "Worker returned an invalid exact Plan cancellation receipt"
                        )
                except HTTPException:
                    raise
                except Exception as exc:
                    raise HTTPException(
                        503,
                        f"Worker Plan Run could not be cancelled safely: {exc}",
                    ) from exc

                try:
                    if remote["state"] == "terminal":
                        run = await apply_worker_terminal_after_cancellation_race(
                            db,
                            plan_id=expected_plan_id,
                            run_id=run_id,
                            worker_id=worker_id,
                            target_generation=expected_generation,
                            payload_digest=payload_digest,
                            payload={
                                "protocol": 3,
                                "base_worker_version_id": remote.get(
                                    "base_worker_version_id"
                                ),
                                "run": remote.get("run"),
                                "versions": remote.get("versions"),
                            },
                        )
                    else:
                        run = await finalize_worker_mirror_cancellation(
                            db,
                            plan_id=expected_plan_id,
                            run_id=run_id,
                            worker_id=worker_id,
                            target_generation=expected_generation,
                            payload_digest=payload_digest,
                            remote_state=remote["state"],
                        )
                except WorkerPlanDispatchConflict as exc:
                    raise HTTPException(409, str(exc)) from exc
                plan = await db.get(Plan, expected_plan_id, populate_existing=True)

        else:
            run = await cancel_run(db, plan=plan, run=run)
            owned_instance_id = run.instance_id
            target_generation = run.cancellation_target_generation

    if worker_id is None:
        plan, run = await _finish_local_plan_run_cancellation(
            db,
            run_id=run_id,
            plan_id=frozen_plan_id,
            owned_instance_id=owned_instance_id,
            target_generation=target_generation,
        )
    await broadcast_plan_event(
        event="plan_run_status_changed",
        plan_id=plan.id,
        target_task_id=plan.target_task_id,
        run_id=run.id,
        status=run.status,
    )
    return await run_resource(db, run)


@router.post(
    "/api/plan-runs/{run_id}/input-requests/{request_id}/answer",
    response_model=PlanInputRequestResponse,
)
async def answer_plan_input(
    run_id: int,
    request_id: int,
    body: PlanInputAnswerRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    run = await db.get(PlanAgentRun, run_id)
    if run is None or run.plan_id is None:
        raise HTTPException(404, "Plan Run not found")
    uploads = _validated_uploads(body)
    if body.attachment_manifest is not None:
        _validate_attachment_manifest(uploads, body.attachment_manifest)
    async with plan_operation_lock(run.plan_id):
        plan = await _require_plan(request, db, run.plan_id, control=True)
        run = await db.get(PlanAgentRun, run_id)
        input_request = await db.get(PlanInputRequest, request_id)
        if input_request is None or input_request.run_id != run.id or input_request.plan_id != plan.id:
            raise HTTPException(404, "Plan InputRequest not found")
        answered = await answer_input_request(
            db,
            plan=plan,
            run=run,
            input_request=input_request,
            expected_generation=body.expected_run_generation,
            idempotency_key=body.idempotency_key,
            answers=body.answers,
            response_text=body.response_text,
            attachments=uploads,
            answered_by=get_current_user_id(request),
        )
    await _wake_dispatcher()
    await broadcast_plan_event(
        event="plan_input_answered",
        plan_id=plan.id,
        target_task_id=plan.target_task_id,
        run_id=run.id,
        input_request_id=answered.id,
    )
    return input_request_resource(answered)


@router.post(
    "/api/plan-versions/{version_id}/create-execution-task",
    response_model=PlanExecutionResource,
    status_code=201,
)
async def create_execution_task(
    version_id: int,
    body: PlanExecutionCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    plan, _ = await _require_version(request, db, version_id, control=True)
    plan_project_id = plan.project_id
    plan_created_by = plan.created_by
    plan_worker_id = plan.worker_id

    async def authorize_effect_boundary(locked_db: AsyncSession) -> None:
        await _lock_standalone_plan_effect_access(
            request,
            locked_db,
            project_id=plan_project_id,
            plan_created_by=plan_created_by,
            worker_id=plan_worker_id,
            fence_worker_assignment=True,
        )

    async def authorize_locked_plan(locked_db: AsyncSession, locked_plan: Plan):
        await _refresh_plan_acl_dependencies(
            locked_db,
            worker_id=locked_plan.worker_id,
            project_id=locked_plan.project_id,
        )
        if not await _has_plan_access(
            request,
            locked_plan,
            locked_db,
            control=True,
        ):
            raise HTTPException(403, "No permission to control this Plan")

    result = await materialize_execution_task(
        db,
        plan_id=plan.id,
        version_id=version_id,
        expected_current_version_id=body.expected_current_version_id,
        confirm_stale=body.confirm_stale,
        approve_if_pending=body.approve_if_pending,
        actor_id=get_current_user_id(request),
        # Applying an approved Plan creates an ordinary implementation Task.
        # Preserve the caller's Manager principal even when the repository is
        # hosted on a Worker; WorkerProxy delegates it at the node boundary.
        execution_principal=task_execution_principal_from_request(request),
        authorize_effect_boundary=authorize_effect_boundary,
        authorize_locked_plan=authorize_locked_plan,
    )
    execution_id = result.task.id
    version = result.version
    await _wake_dispatcher()
    refreshed_plan = await db.get(Plan, plan.id)
    refreshed_version = await db.get(PlanVersion, version.id)
    await broadcast_plan_event(
        event="plan_version_applied",
        plan_id=plan.id,
        target_task_id=plan.target_task_id,
        version_id=version.id,
        execution_task_id=execution_id,
    )
    return PlanExecutionResource(
        plan=await plan_resource(db, refreshed_plan, include_audit=True),
        version=await version_resource(db, refreshed_version),
        execution_task_id=execution_id,
    )
