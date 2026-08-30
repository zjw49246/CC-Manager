"""Independent Plan Task creation, history, revision, and execution APIs."""

from copy import deepcopy
from contextlib import AsyncExitStack
from datetime import datetime
from types import SimpleNamespace

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import (
    get_current_user_id,
    lock_task_effect_access,
    lock_task_effect_accesses,
    require_task_access,
    require_task_control,
    task_execution_principal_from_request,
)
from backend.api.uploads import (
    UploadAttachmentValidationError,
    validate_upload_attachments,
)
from backend.api.task_projection import (
    task_list_response,
    task_response,
    task_response_model,
)
from backend.config import settings
from backend.database import get_db
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.task import Task
from backend.schemas.plan import (
    PlanPipelineConfig,
    resolve_plan_pipeline_config,
)
from backend.schemas.task import TaskResponse
from backend.services.plan_tasks import (
    ACTIVE_PLAN_STATUSES,
    MAX_ACTIVE_PLANS_PER_TASK,
    capture_task_context,
    capture_repo_revision,
    latest_task_log_id,
    mark_plan_superseded,
    PlanTerminalQuiescenceError,
    plan_staleness,
    run_plan_terminal_transition,
)
from backend.services.plan_pipeline_settings import effective_plan_pipeline_config
from backend.services.task_creation import (
    stage_task_record,
    validate_task_service_tier_configuration,
)
from backend.services.task_queue import TaskQueue
from backend.services.worker_proxy import get_task_operation_lock
from backend.services.worker_task_termination import (
    active_worker_task_termination_receipt,
    no_active_worker_task_termination_predicate,
)


router = APIRouter(prefix="/api/tasks", tags=["plans"])


class RelatedPlanCreate(BaseModel):
    input: str = Field(min_length=1, max_length=200_000)
    title: str | None = Field(default=None, max_length=200)
    file_paths: list[str] | None = None
    image_paths: list[str] | None = None
    attachments: list[dict] | None = None
    provider: str | None = None
    model: str | None = None
    effort_level: str | None = None
    pipeline_config: PlanPipelineConfig | None = None
    supersedes_plan_task_id: int | None = None


class PlanRevisionRequest(BaseModel):
    feedback: str = Field(min_length=1, max_length=50_000)
    title: str | None = Field(default=None, max_length=200)
    pipeline_config: PlanPipelineConfig | None = None


class PlanExecutionResponse(BaseModel):
    plan_task: TaskResponse
    execution_task: TaskResponse


class PlanAgentStepResponse(BaseModel):
    id: int
    step_type: str
    round: int
    provider: str
    model: str | None
    effort: str | None
    route_slot: str | None
    status: str
    output: str | None
    error: str | None
    last_delta_at: datetime | None = None
    streamed_output_chars: int = 0
    last_event_type: str | None = None
    started_at: datetime
    finished_at: datetime | None

    model_config = {"from_attributes": True}


class PlanAgentRunResponse(BaseModel):
    id: int
    plan_task_id: int
    status: str
    combo_used: str | None
    planner_provider: str | None
    planner_model: str | None
    planner_effort: str | None
    reviewer_provider: str | None
    reviewer_model: str | None
    reviewer_effort: str | None
    pipeline_config: dict | None
    round: int
    review_verdict: str | None
    review_feedback: str | None
    review_exhausted: bool
    error: str | None
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None
    steps: list[PlanAgentStepResponse]


def _queue(db: AsyncSession) -> TaskQueue:
    return TaskQueue(db)


async def _wake_dispatcher() -> None:
    try:
        from backend.main import dispatcher

        if dispatcher:
            dispatcher.wake()
    except Exception:
        pass


def _require_revisable_plan(plan: Task) -> None:
    if plan.mode != "plan":
        raise HTTPException(400, "Task is not a Plan")
    if plan.status == "superseded":
        successor_id = (plan.metadata_ or {}).get(
            "plan_superseded_by_task_id"
        )
        detail = "Plan has been superseded"
        if isinstance(successor_id, int):
            detail += f" by Plan #{successor_id}"
        raise HTTPException(409, detail)
    if plan.status != "plan_review":
        raise HTTPException(400, "Task is not in plan review state")


async def _fence_plan_task_admission(
    db: AsyncSession,
    task_id: int,
) -> None:
    """Order Plan creation against durable Task termination ownership."""

    fenced = await db.execute(
        update(Task)
        .where(
            Task.id == task_id,
            no_active_worker_task_termination_predicate(),
        )
        .values(status=Task.status)
    )
    if fenced.rowcount == 1:
        return
    if await active_worker_task_termination_receipt(db, task_id):
        raise HTTPException(
            409,
            "Task has an active Worker termination receipt",
        )
    raise HTTPException(409, "Task changed while Plan admission was starting")


def _plan_upload_fields(
    task: Task,
) -> tuple[list[str] | None, list[str] | None, list[dict] | None]:
    """Normalize both current and legacy Task attachment metadata."""

    metadata = task.metadata_ or {}
    file_paths = metadata.get("file_paths") or metadata.get("image_paths")
    attachments = metadata.get("attachments")
    if not file_paths:
        return None, None, attachments
    if metadata.get("file_paths") is not None:
        return file_paths, metadata.get("image_paths") or [], attachments
    if isinstance(attachments, list) and len(attachments) == len(file_paths):
        image_paths = [
            path
            for path, attachment in zip(file_paths, attachments, strict=True)
            if isinstance(attachment, dict)
            and attachment.get("is_image") is True
        ]
        return file_paths, image_paths, attachments
    return file_paths, file_paths, attachments


async def _create_related_plan(
    *,
    db: AsyncSession,
    request: Request,
    target: Task,
    body: RelatedPlanCreate,
) -> Task:
    active_count = await db.scalar(
        select(func.count(Task.id)).where(
            Task.plan_target_task_id == target.id,
            Task.mode == "plan",
            Task.status.in_(ACTIVE_PLAN_STATUSES),
        )
    )
    if int(active_count or 0) >= MAX_ACTIVE_PLANS_PER_TASK:
        raise HTTPException(
            429,
            f"Task already has {MAX_ACTIVE_PLANS_PER_TASK} active Plans",
        )

    supersedes = None
    if body.supersedes_plan_task_id is not None:
        supersedes = await db.get(Task, body.supersedes_plan_task_id)
        if (
            supersedes is None
            or supersedes.mode != "plan"
            or supersedes.plan_target_task_id != target.id
        ):
            raise HTTPException(400, "Superseded Plan does not belong to this Task")
        await require_task_control(request, supersedes, db)
        _require_revisable_plan(supersedes)
    superseded_id = supersedes.id if supersedes is not None else None

    try:
        uploads = validate_upload_attachments(
            file_paths=body.file_paths,
            image_paths=body.image_paths,
            attachments=body.attachments,
        )
    except UploadAttachmentValidationError as exc:
        raise HTTPException(400, str(exc)) from exc

    pipeline = resolve_plan_pipeline_config(
        body.pipeline_config,
        base_config=await effective_plan_pipeline_config(db),
        legacy_provider=body.provider,
        legacy_model=body.model,
        legacy_effort=body.effort_level,
    )
    provider = pipeline.planner.primary.provider
    model = pipeline.planner.primary.model
    effort = pipeline.planner.primary.effort
    # Plan Agents use isolated read-only turns. Fast requires the app-server
    # proof chain and must never be silently downgraded.
    codex_service_tier = "default"
    try:
        routes = (
            pipeline.planner.primary,
            pipeline.planner.fallback,
            pipeline.reviewer.primary,
            pipeline.reviewer.fallback,
        )
        for route in routes:
            validate_task_service_tier_configuration(
                provider=route.provider,
                model=route.model,
                codex_service_tier=codex_service_tier,
                mode="plan",
                goal_evaluator_model=None,
            )
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    context_log_id = await latest_task_log_id(db, target.id)
    context_snapshot = await capture_task_context(
        db,
        target.id,
        through_log_id=context_log_id,
        max_chars=settings.plan_transcript_max_chars,
    )
    repo_revision = (
        None
        if target.worker_id is not None
        else await capture_repo_revision(target.last_cwd or target.target_repo)
    )
    target_id = target.id
    target_snapshot = (
        target.incarnation_id,
        target.session_id,
        target.shared_from_id,
        target.status,
        target.title,
        target.priority,
        target.project_id,
        target.target_repo,
        target.last_cwd,
        target.target_branch,
        target.worker_id,
        target.max_retries,
        target.timeout_hours,
    )
    target_probe = SimpleNamespace(
        id=target.id,
        project_id=target.project_id,
    )

    def target_changed(current: Task) -> bool:
        return target_snapshot != (
            current.incarnation_id,
            current.session_id,
            current.shared_from_id,
            current.status,
            current.title,
            current.priority,
            current.project_id,
            current.target_repo,
            current.last_cwd,
            current.target_branch,
            current.worker_id,
            current.max_retries,
            current.timeout_hours,
        )

    async def stage_plan(
        current_target: Task,
        current_supersedes: Task | None,
    ) -> Task:
        return await stage_task_record(
            db,
            title=(
                body.title.strip()
                if body.title and body.title.strip()
                else (
                    f"Plan for #{current_target.id}: {current_target.title.strip()}"
                    if current_target.title and current_target.title.strip()
                    else f"Plan for #{current_target.id}"
                )
            )[:200],
            description=body.input.strip(),
            status="pending",
            priority=current_target.priority,
            project_id=current_target.project_id,
            target_repo=current_target.target_repo,
            target_branch=current_target.target_branch,
            merge_status="pending",
            worker_id=current_target.worker_id,
            created_by=get_current_user_id(request),
            **task_execution_principal_from_request(
                request,
                force_sandbox=True,
            ),
            max_retries=current_target.max_retries,
            mode="plan",
            provider=provider,
            model=model,
            codex_service_tier=codex_service_tier,
            effort_level=effort,
            thinking_budget=None,
            timeout_hours=current_target.timeout_hours,
            enable_workflows=False,
            enabled_skills={},
            selected_user_skills=[],
            metadata_={
                "created_from_plan_target_task_id": current_target.id,
                **(
                    {
                        "file_paths": [upload.path for upload in uploads],
                        "image_paths": [
                            upload.path for upload in uploads if upload.is_image
                        ],
                        "attachments": [
                            upload.public_dict() for upload in uploads
                        ],
                    }
                    if uploads
                    else {}
                ),
                **(
                    {"revised_from_plan_task_id": current_supersedes.id}
                    if current_supersedes is not None
                    else {}
                ),
            },
            plan_target_task_id=current_target.id,
            plan_context_session_id=current_target.session_id,
            plan_context_log_id=context_log_id,
            plan_context_snapshot=context_snapshot,
            plan_repo_revision=repo_revision,
            supersedes_plan_task_id=(
                current_supersedes.id
                if current_supersedes is not None
                else None
            ),
            plan_pipeline_config=pipeline.model_dump(mode="json"),
        )

    if supersedes is None:
        current_target = await lock_task_effect_access(
            request,
            target_probe,
            db,
            allow_chat_share=False,
            fence_worker_node=True,
            fence_worker_assignment=True,
        )
        if target_changed(current_target):
            raise HTTPException(
                409,
                "Plan target changed while creating the Plan",
            )
        await _fence_plan_task_admission(db, current_target.id)
        exact_active_count = await db.scalar(
            select(func.count(Task.id)).where(
                Task.plan_target_task_id == current_target.id,
                Task.mode == "plan",
                Task.status.in_(ACTIVE_PLAN_STATUSES),
            )
        )
        if int(exact_active_count or 0) >= MAX_ACTIVE_PLANS_PER_TASK:
            raise HTTPException(
                429,
                f"Task already has {MAX_ACTIVE_PLANS_PER_TASK} active Plans",
            )
        plan = await stage_plan(current_target, None)
        await db.commit()
    else:
        supersedes_snapshot = (
            supersedes.incarnation_id,
            supersedes.mode,
            supersedes.status,
            supersedes.plan_target_task_id,
            supersedes.project_id,
            supersedes.worker_id,
        )
        supersedes_probe = SimpleNamespace(
            id=supersedes.id,
            project_id=supersedes.project_id,
        )
        authorized_tasks: dict[int, Task] = {}

        async def authorize_supersede_effect() -> None:
            locked = await lock_task_effect_accesses(
                request,
                [target_probe, supersedes_probe],
                db,
                allow_chat_share=False,
                fence_worker_node=True,
                fence_worker_assignment=True,
            )
            authorized_tasks.update({task.id: task for task in locked})
            current_target = authorized_tasks.get(target_id)
            current_supersedes = authorized_tasks.get(superseded_id)
            if current_target is None or target_changed(current_target):
                raise HTTPException(
                    409,
                    "Plan target changed while creating the revision",
                )
            if current_supersedes is None or supersedes_snapshot != (
                current_supersedes.incarnation_id,
                current_supersedes.mode,
                current_supersedes.status,
                current_supersedes.plan_target_task_id,
                current_supersedes.project_id,
                current_supersedes.worker_id,
            ):
                raise HTTPException(
                    409,
                    "Superseded Plan changed while creating the revision",
                )

        async def commit_supersede() -> Task:
            db.expire_all()
            current_target = await db.get(
                Task,
                target_id,
                populate_existing=True,
            )
            current_supersedes = await db.get(
                Task,
                superseded_id,
                populate_existing=True,
            )
            if current_target is None or current_supersedes is None:
                raise HTTPException(
                    409,
                    "Plan or target disappeared during revision",
                )
            if target_changed(current_target):
                raise HTTPException(
                    409,
                    "Plan target changed while creating the revision",
                )
            if (
                current_supersedes.mode != "plan"
                or current_supersedes.plan_target_task_id != current_target.id
            ):
                raise HTTPException(
                    400,
                    "Superseded Plan does not belong to this Task",
                )
            _require_revisable_plan(current_supersedes)
            await _fence_plan_task_admission(db, current_target.id)
            exact_active_count = await db.scalar(
                select(func.count(Task.id)).where(
                    Task.plan_target_task_id == current_target.id,
                    Task.mode == "plan",
                    Task.status.in_(ACTIVE_PLAN_STATUSES),
                )
            )
            if int(exact_active_count or 0) >= MAX_ACTIVE_PLANS_PER_TASK:
                raise HTTPException(
                    429,
                    f"Task already has {MAX_ACTIVE_PLANS_PER_TASK} active Plans",
                )
            staged = await stage_plan(current_target, current_supersedes)
            if not await mark_plan_superseded(
                db,
                current_supersedes,
                successor_id=staged.id,
            ):
                raise HTTPException(
                    409,
                    "Plan changed while its revision was being created",
                )
            return staged

        try:
            plan = await run_plan_terminal_transition(
                db,
                superseded_id,
                "superseded",
                commit_supersede,
                authorize_effect_boundary=authorize_supersede_effect,
            )
        except PlanTerminalQuiescenceError as exc:
            raise HTTPException(409, str(exc)) from exc
    await db.refresh(plan)
    await _wake_dispatcher()
    return plan


@router.post(
    "/{target_task_id}/plans",
    response_model=TaskResponse,
    status_code=201,
)
async def create_related_plan(
    target_task_id: int,
    body: RelatedPlanCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    if {
        "pipeline_config",
        "provider",
        "model",
        "effort_level",
    } & body.model_fields_set:
        raise HTTPException(
            422,
            "Planner and Reviewer routes are configured globally",
        )
    target = await db.get(Task, target_task_id)
    if target is None:
        raise HTTPException(404, "Task not found")
    await require_task_control(request, target, db)
    from backend.api.tasks import _require_not_isolated_browser_child

    await _require_not_isolated_browser_child(
        db,
        target,
        action="used as a Plan owner",
    )
    if not target.session_id:
        raise HTTPException(400, "Run the target Task before creating a session Plan")
    if target.shared_from_id is not None:
        raise HTTPException(409, "Shared shadow tasks cannot own Plan Tasks")
    lock_ids = {target_task_id}
    if body.supersedes_plan_task_id is not None:
        lock_ids.add(body.supersedes_plan_task_id)
    async with AsyncExitStack() as stack:
        for task_id in sorted(lock_ids):
            await stack.enter_async_context(get_task_operation_lock(task_id))
        db.expire_all()
        target = await db.get(Task, target_task_id)
        if target is None:
            raise HTTPException(404, "Task not found")
        await require_task_control(request, target, db)
        await _require_not_isolated_browser_child(
            db,
            target,
            action="used as a Plan owner",
        )
        plan = await _create_related_plan(
            db=db,
            request=request,
            target=target,
            body=body,
        )
        return await task_response(
            request,
            plan,
            db,
            status_code=201,
        )


@router.get("/{target_task_id}/plans", response_model=list[TaskResponse])
async def list_related_plans(
    target_task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    target = await db.get(Task, target_task_id)
    if target is None:
        raise HTTPException(404, "Task not found")
    await require_task_access(request, target, db)
    rows = await db.execute(
        select(Task)
        .where(
            Task.plan_target_task_id == target_task_id,
            Task.mode == "plan",
        )
        .order_by(Task.created_at.desc(), Task.id.desc())
    )
    plans = list(rows.scalars().all())
    # A related Plan inherits the target's visibility, but do not accidentally
    # expose a row whose ownership/routing was corrupted independently. A
    # TeamTaskShare belongs to the target Task and is intentionally not copied
    # into every Plan row, so re-running require_task_access(plan) here would
    # turn inherited chat visibility into an unconditional 403.
    for plan in plans:
        if (
            plan.plan_target_task_id != target.id
            or plan.project_id != target.project_id
            or plan.worker_id != target.worker_id
            or plan.shared_from_id != target.shared_from_id
        ):
            raise HTTPException(
                409,
                "Related Plan routing does not match its target Task",
            )
    return await task_list_response(request, plans, db)


@router.get("/{plan_task_id}/plan/staleness")
async def get_plan_staleness(
    plan_task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    plan = await db.get(Task, plan_task_id)
    if plan is None:
        raise HTTPException(404, "Plan Task not found")
    await require_task_access(request, plan, db)
    if plan.mode != "plan":
        raise HTTPException(400, "Task is not a Plan")
    if plan.plan_target_task_id is not None:
        target = await db.get(Task, plan.plan_target_task_id)
        if target is None:
            raise HTTPException(409, "Plan target no longer exists")
        await require_task_access(request, target, db)
    if plan.worker_id is not None:
        from backend.api.tasks import _proxy

        result = await _proxy(
            plan,
            "GET",
            f"/api/tasks/{plan_task_id}/plan/staleness",
        )
        if not isinstance(result, dict) or "stale" not in result:
            raise HTTPException(502, "Worker returned invalid Plan staleness")
        return result
    return await plan_staleness(db, plan)


@router.get(
    "/{plan_task_id}/plan/runs",
    response_model=list[PlanAgentRunResponse],
)
async def list_plan_runs(
    plan_task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    plan = await db.get(Task, plan_task_id)
    if plan is None:
        raise HTTPException(404, "Plan Task not found")
    await require_task_access(request, plan, db)
    if plan.mode != "plan":
        raise HTTPException(400, "Task is not a Plan")
    if plan.worker_id is not None:
        from backend.api.tasks import _proxy

        result = await _proxy(
            plan,
            "GET",
            f"/api/tasks/{plan_task_id}/plan/runs",
        )
        if not isinstance(result, list):
            raise HTTPException(502, "Worker returned invalid Plan run history")
        return result

    runs = list(
        (
            await db.execute(
                select(PlanAgentRun)
                .where(PlanAgentRun.plan_task_id == plan_task_id)
                .order_by(PlanAgentRun.id.desc())
            )
        ).scalars().all()
    )
    if not runs:
        return []
    step_rows = list(
        (
            await db.execute(
                select(PlanAgentStep)
                .where(PlanAgentStep.run_id.in_([run.id for run in runs]))
                .order_by(PlanAgentStep.id)
            )
        ).scalars().all()
    )
    by_run: dict[int, list[PlanAgentStep]] = {}
    for step in step_rows:
        by_run.setdefault(step.run_id, []).append(step)
    return [
        PlanAgentRunResponse(
            **{
                column: getattr(run, column)
                for column in (
                    "id",
                    "plan_task_id",
                    "status",
                    "combo_used",
                    "planner_provider",
                    "planner_model",
                    "planner_effort",
                    "reviewer_provider",
                    "reviewer_model",
                    "reviewer_effort",
                    "pipeline_config",
                    "round",
                    "review_verdict",
                    "review_feedback",
                    "review_exhausted",
                    "error",
                    "created_at",
                    "updated_at",
                    "finished_at",
                )
            },
            steps=by_run.get(run.id, []),
        )
        for run in runs
    ]


@router.post(
    "/{plan_task_id}/plan/revise",
    response_model=TaskResponse,
    status_code=201,
)
async def revise_plan(
    plan_task_id: int,
    body: PlanRevisionRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    source = await db.get(Task, plan_task_id)
    if source is None:
        raise HTTPException(404, "Plan Task not found")
    await require_task_control(request, source, db)
    _require_revisable_plan(source)
    target_id = source.plan_target_task_id or source.id
    async with AsyncExitStack() as stack:
        for task_id in sorted({source.id, target_id}):
            await stack.enter_async_context(get_task_operation_lock(task_id))
        db.expire_all()
        current_source = await db.get(Task, plan_task_id)
        current_target = (
            await db.get(Task, current_source.plan_target_task_id)
            if current_source is not None
            and current_source.plan_target_task_id is not None
            else current_source
        )
        if current_source is None or current_target is None:
            raise HTTPException(409, "Plan or target disappeared during revision")
        await require_task_control(request, current_target, db)
        await require_task_control(request, current_source, db)
        _require_revisable_plan(current_source)

        prompt = (
            f"{current_source.description or ''}\n\n"
            "Previous Plan:\n"
            f"{current_source.plan_content or '(no completed plan)'}\n\n"
            "User revision feedback:\n"
            f"{body.feedback.strip()}"
        )
        revision_pipeline = resolve_plan_pipeline_config(
            body.pipeline_config or current_source.plan_pipeline_config,
            legacy_provider=current_source.provider,
            legacy_model=current_source.model,
            legacy_effort=current_source.effort_level,
        )
        if current_source.plan_target_task_id is None:
            source_snapshot = (
                current_source.incarnation_id,
                current_source.mode,
                current_source.status,
                current_source.plan_target_task_id,
                current_source.project_id,
                current_source.worker_id,
                current_source.target_repo,
                current_source.last_cwd,
                current_source.target_branch,
                current_source.priority,
                current_source.max_retries,
                current_source.timeout_hours,
            )
            source_probe = SimpleNamespace(
                id=current_source.id,
                project_id=current_source.project_id,
            )

            def source_changed(candidate: Task) -> bool:
                return source_snapshot != (
                    candidate.incarnation_id,
                    candidate.mode,
                    candidate.status,
                    candidate.plan_target_task_id,
                    candidate.project_id,
                    candidate.worker_id,
                    candidate.target_repo,
                    candidate.last_cwd,
                    candidate.target_branch,
                    candidate.priority,
                    candidate.max_retries,
                    candidate.timeout_hours,
                )

            repo_revision = await capture_repo_revision(
                current_source.last_cwd or current_source.target_repo
            )

            async def authorize_standalone_revision() -> None:
                admitted = await lock_task_effect_access(
                    request,
                    source_probe,
                    db,
                    allow_chat_share=False,
                    fence_worker_node=True,
                    fence_worker_assignment=True,
                )
                if source_changed(admitted):
                    raise HTTPException(
                        409,
                        "Plan changed while its revision was being created",
                    )

            async def commit_standalone_supersede() -> Task:
                db.expire_all()
                exact_source = await db.get(
                    Task,
                    plan_task_id,
                    populate_existing=True,
                )
                if exact_source is None:
                    raise HTTPException(409, "Plan disappeared during revision")
                if source_changed(exact_source):
                    raise HTTPException(
                        409,
                        "Plan changed while its revision was being created",
                    )
                _require_revisable_plan(exact_source)
                if exact_source.plan_target_task_id is not None:
                    raise HTTPException(
                        409,
                        "Plan target changed while its revision was being created",
                    )
                exact_prompt = (
                    f"{exact_source.description or ''}\n\n"
                    "Previous Plan:\n"
                    f"{exact_source.plan_content or '(no completed plan)'}\n\n"
                    "User revision feedback:\n"
                    f"{body.feedback.strip()}"
                )
                staged = await stage_task_record(
                    db,
                    title=(
                        body.title.strip()
                        if body.title and body.title.strip()
                        else f"Revision of Plan #{exact_source.id}"
                    )[:200],
                    description=exact_prompt,
                    status="pending",
                    priority=exact_source.priority,
                    project_id=exact_source.project_id,
                    target_repo=exact_source.target_repo,
                    target_branch=exact_source.target_branch,
                    merge_status="pending",
                    worker_id=exact_source.worker_id,
                    created_by=get_current_user_id(request),
                    **task_execution_principal_from_request(
                        request,
                        force_sandbox=True,
                    ),
                    max_retries=exact_source.max_retries,
                    mode="plan",
                    provider=revision_pipeline.planner.primary.provider,
                    model=revision_pipeline.planner.primary.model,
                    codex_service_tier=exact_source.codex_service_tier,
                    effort_level=revision_pipeline.planner.primary.effort,
                    plan_pipeline_config=(
                        revision_pipeline.model_dump(mode="json")
                    ),
                    timeout_hours=exact_source.timeout_hours,
                    enable_workflows=False,
                    enabled_skills={},
                    selected_user_skills=[],
                    metadata_={
                        "revised_from_plan_task_id": exact_source.id
                    },
                    plan_context_session_id=None,
                    plan_context_log_id=None,
                    plan_repo_revision=repo_revision,
                    supersedes_plan_task_id=exact_source.id,
                )
                if not await mark_plan_superseded(
                    db,
                    exact_source,
                    successor_id=staged.id,
                ):
                    raise HTTPException(
                        409,
                        "Plan changed while its revision was being created",
                    )
                return staged

            try:
                revision = await run_plan_terminal_transition(
                    db,
                    plan_task_id,
                    "superseded",
                    commit_standalone_supersede,
                    authorize_effect_boundary=authorize_standalone_revision,
                )
            except PlanTerminalQuiescenceError as exc:
                raise HTTPException(409, str(exc)) from exc
            await db.refresh(revision)
            await _wake_dispatcher()
            return await task_response(
                request,
                revision,
                db,
                status_code=201,
            )

        revision_files, revision_images, revision_attachments = (
            _plan_upload_fields(current_source)
        )
        revision = await _create_related_plan(
            db=db,
            request=request,
            target=current_target,
            body=RelatedPlanCreate(
                input=prompt,
                title=body.title or f"Revision of Plan #{current_source.id}",
                file_paths=revision_files,
                image_paths=revision_images,
                attachments=revision_attachments,
                provider=current_source.provider,
                model=current_source.model,
                effort_level=current_source.effort_level,
                pipeline_config=revision_pipeline,
                supersedes_plan_task_id=current_source.id,
            ),
        )
        return await task_response(
            request,
            revision,
            db,
            status_code=201,
        )


@router.post(
    "/{plan_task_id}/plan/create-execution-task",
    response_model=PlanExecutionResponse,
    status_code=201,
)
async def create_plan_execution_task(
    plan_task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    async with get_task_operation_lock(plan_task_id):
        plan = await db.get(Task, plan_task_id)
        if plan is None:
            raise HTTPException(404, "Plan Task not found")
        await require_task_control(request, plan, db)
        if plan.canonical_plan_id is not None:
            raise HTTPException(
                409,
                "Migrated Plan carriers already contain their exact execution "
                "application; use the canonical Plan instead",
            )
        if plan.mode != "plan" or plan.plan_target_task_id is not None:
            raise HTTPException(400, "Only standalone Plans create execution Tasks")
        if plan.plan_approved is not True or not plan.plan_content:
            raise HTTPException(400, "Plan must be approved before execution")
        if plan.plan_execution_task_id is not None:
            existing = await db.get(Task, plan.plan_execution_task_id)
            if existing is None:
                raise HTTPException(409, "Recorded execution Task no longer exists")
            projected_plan = await task_response_model(request, plan, db)
            projected_execution = await task_response_model(
                request,
                existing,
                db,
            )
            return JSONResponse(
                status_code=201,
                content={
                    "plan_task": projected_plan.model_dump(mode="json"),
                    "execution_task": projected_execution.model_dump(
                        mode="json"
                    ),
                },
            )

        plan_snapshot = (
            plan.incarnation_id,
            plan.mode,
            plan.status,
            plan.plan_target_task_id,
            plan.canonical_plan_id,
            plan.plan_approved,
            plan.plan_content,
            plan.plan_execution_task_id,
            plan.project_id,
            plan.worker_id,
            plan.target_repo,
            plan.target_branch,
        )
        plan = await lock_task_effect_access(
            request,
            SimpleNamespace(id=plan.id, project_id=plan.project_id),
            db,
            allow_chat_share=False,
            fence_worker_node=True,
            fence_worker_assignment=True,
        )
        if plan_snapshot != (
            plan.incarnation_id,
            plan.mode,
            plan.status,
            plan.plan_target_task_id,
            plan.canonical_plan_id,
            plan.plan_approved,
            plan.plan_content,
            plan.plan_execution_task_id,
            plan.project_id,
            plan.worker_id,
            plan.target_repo,
            plan.target_branch,
        ):
            raise HTTPException(
                409,
                "Plan changed while creating its execution Task",
            )
        await _fence_plan_task_admission(db, plan.id)

        metadata = deepcopy(plan.metadata_ or {})
        metadata["created_from_plan_task_id"] = plan.id
        execution = await stage_task_record(
            db,
            title=f"Execute Plan #{plan.id}: {plan.title}"[:200],
            description=(
                "[Approved implementation plan]\n"
                "The user explicitly created this execution Task from the "
                "approved Plan below. Implement it now, adapting only when the "
                "repository requires it.\n\n"
                f"<plan>\n{plan.plan_content}\n</plan>\n\n"
                "[Original planning request]\n"
                f"{plan.description or ''}"
            ),
            status="pending",
            priority=plan.priority,
            project_id=plan.project_id,
            target_repo=plan.target_repo,
            target_branch=plan.target_branch,
            merge_status="pending",
            worker_id=plan.worker_id,
            created_by=get_current_user_id(request),
            # This is the ordinary implementation Task selected by the user.
            # Worker is only its execution location; forwarding converts this
            # native Manager principal to the authenticated delegated form.
            **task_execution_principal_from_request(request),
            max_retries=plan.max_retries,
            mode="auto",
            provider=plan.provider,
            model=plan.model,
            codex_service_tier=plan.codex_service_tier,
            effort_level=plan.effort_level,
            thinking_budget=plan.thinking_budget,
            system_prompt_mode=plan.system_prompt_mode,
            timeout_hours=plan.timeout_hours,
            enable_workflows=plan.enable_workflows,
            enabled_skills=deepcopy(plan.enabled_skills),
            selected_user_skills=deepcopy(plan.selected_user_skills),
            tags=deepcopy(plan.tags),
            metadata_=metadata,
        )
        linked = await db.execute(
            update(Task)
            .where(
                Task.id == plan.id,
                Task.plan_execution_task_id.is_(None),
                Task.plan_approved.is_(True),
                no_active_worker_task_termination_predicate(),
            )
            .values(plan_execution_task_id=execution.id)
        )
        if linked.rowcount != 1:
            await db.rollback()
            raise HTTPException(409, "Plan execution Task was created concurrently")
        await db.commit()
        await db.refresh(plan)
        await db.refresh(execution)
    await _wake_dispatcher()
    projected_plan = await task_response_model(request, plan, db)
    projected_execution = await task_response_model(request, execution, db)
    return JSONResponse(
        status_code=201,
        content={
            "plan_task": projected_plan.model_dump(mode="json"),
            "execution_task": projected_execution.model_dump(mode="json"),
        },
    )
