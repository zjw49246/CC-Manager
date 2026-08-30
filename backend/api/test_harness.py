"""Task-scoped API for the durable frontend test harness."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.background import BackgroundTask

from backend.api.deps import (
    get_current_user_id,
    lock_task_effect_access,
    require_internal_service,
    require_internal_task_incarnation,
    require_task_control,
)
from backend.config import settings
from backend.database import get_db
from backend.models.project import Project
from backend.models.task import Task
from backend.models.test_harness import TestHarnessRun
from backend.services.test_harness import (
    TestHarnessBusyError,
    TestHarnessError,
    TestHarnessIdempotencyError,
    test_harness_service,
)
from backend.services.test_harness_contracts import (
    DEFAULT_BROWSER_CHANNEL,
    TestHarnessContractError,
    TestHarnessSpec,
)
from backend.services.test_harness_runtime import (
    HARNESS_RUNTIME_METADATA_KEY,
    build_saved_harness_runtime,
    harness_runtime_config_payload,
)
from backend.services.workspace_review import (
    WorkspaceReviewError,
    public_workspace_review_capability,
    workspace_review_capability,
)
from backend.services.test_harness_targets import untrusted_git_target_capability
from backend.services.test_harness_owner_fence import (
    TestHarnessOwnerIdentity,
    test_harness_owner_identity,
    test_harness_owner_locality_error,
)


router = APIRouter(prefix="/api/tasks", tags=["test-harness"])


class TestHarnessRunStart(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_kind: Literal[
        "current_workspace", "fixed_url", "pull_request", "git_ref"
    ] = "current_workspace"
    target: dict[str, Any] = Field(default_factory=dict)
    goal: str = Field(min_length=1, max_length=20_000)
    profile: Literal["quick", "standard", "exhaustive"] = "standard"
    allow_actions: bool = True
    browser_channel: Literal["chrome", "chromium"] = DEFAULT_BROWSER_CHANNEL
    viewport_width: int = Field(default=1440, ge=320, le=3840)
    viewport_height: int = Field(default=900, ge=320, le=2160)
    max_steps: int | None = Field(default=None, ge=1, le=50)
    max_actions: int | None = Field(default=None, ge=0, le=200)
    provider: Literal["claude", "codex"] | None = None
    model: str | None = Field(default=None, min_length=1, max_length=100)
    reasoning_effort: str | None = Field(default=None, min_length=1, max_length=20)
    codex_service_tier: Literal["default", "priority"] | None = None
    test_plan: dict[str, Any] | None = None
    parent_run_id: str | None = Field(default=None, min_length=32, max_length=32)
    idempotency_key: str | None = Field(default=None, max_length=200)


class TestHarnessRuntimeConfigUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    inherit_task: bool = True
    provider: Literal["claude", "codex"] | None = None
    model: str | None = Field(default=None, min_length=1, max_length=100)
    reasoning_effort: str | None = Field(default=None, min_length=1, max_length=20)
    codex_service_tier: Literal["default", "priority"] | None = None


async def _task_or_404(task_id: int, db: AsyncSession) -> Task:
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


def _spec(body: TestHarnessRunStart) -> TestHarnessSpec:
    return TestHarnessSpec(
        target_kind=body.target_kind,
        target=body.target,
        goal=body.goal,
        profile=body.profile,
        allow_actions=body.allow_actions,
        browser_channel=body.browser_channel,
        viewport_width=body.viewport_width,
        viewport_height=body.viewport_height,
        max_steps=body.max_steps,
        max_actions=body.max_actions,
        provider=body.provider,
        model=body.model,
        reasoning_effort=body.reasoning_effort,
        codex_service_tier=body.codex_service_tier,
        test_plan=body.test_plan,
        parent_run_id=body.parent_run_id,
        idempotency_key=body.idempotency_key,
    )


async def _start(
    *,
    task: Task,
    body: TestHarnessRunStart,
    owner_user_id: int | None,
    inline: bool,
    owner_identity: TestHarnessOwnerIdentity | None = None,
) -> dict[str, Any]:
    try:
        run = await test_harness_service.start_task_run(
            task_id=task.id,
            spec=_spec(body),
            owner_user_id=owner_user_id,
            owner_identity=owner_identity,
        )
        if body.target_kind == "fixed_url" and run.browser_review_job_id is None:
            await test_harness_service.start_fixed_url_browser(
                run_id=run.id,
                inline=inline,
            )
        payload = await test_harness_service.get_run(run.id)
        assert payload is not None
        return payload
    except TestHarnessBusyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except TestHarnessIdempotencyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (TestHarnessContractError, TestHarnessError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/{task_id}/test-runs/capabilities")
async def get_test_harness_capabilities(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    task = await _task_or_404(task_id, db)
    await require_task_control(request, task, db)
    project = await db.get(Project, task.project_id) if task.project_id else None
    workspace = public_workspace_review_capability(
        workspace_review_capability(task, project),
        include_suggestion=False,
    )
    try:
        runtime = harness_runtime_config_payload(task)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    provider = runtime["provider"]
    git_targets = await untrusted_git_target_capability(project=project)
    locality_error = test_harness_owner_locality_error(task)
    return {
        "contract_version": 1,
        "available": locality_error is None and workspace["available"],
        "reason": locality_error or workspace["reason"],
        "provider": provider,
        "task_provider": task.provider,
        "provider_browser_capability": provider in {"claude", "codex"},
        "runtime_configurable": True,
        "runtime": runtime,
        "context_policy": "isolated_black_box_v1",
        "targets": {
            "current_workspace": (
                locality_error is None and workspace["available"]
            ),
            "fixed_url": locality_error is None,
            "pull_request": (
                locality_error is None and git_targets.available
            ),
            "git_ref": locality_error is None and git_targets.available,
        },
        "target_reasons": {
            "current_workspace": locality_error or workspace["reason"],
            "fixed_url": locality_error,
            "pull_request": locality_error or git_targets.reason,
            "git_ref": locality_error or git_targets.reason,
        },
        "sandbox": git_targets.sandbox.as_dict(),
        "preview": workspace,
        "supports_repeat": True,
        "supports_compare": True,
    }


@router.get("/{task_id}/test-runs/config")
async def get_test_harness_runtime_config(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    task = await _task_or_404(task_id, db)
    await require_task_control(request, task, db)
    try:
        return harness_runtime_config_payload(task)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.put("/{task_id}/test-runs/config")
async def update_test_harness_runtime_config(
    task_id: int,
    body: TestHarnessRuntimeConfigUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    task = await _task_or_404(task_id, db)
    await require_task_control(request, task, db)
    if task.status not in {"completed", "failed", "cancelled", "conflict"}:
        raise HTTPException(
            status_code=409,
            detail="等待当前 Task 回合结束后再修改 Browser Agent 配置",
        )
    try:
        saved = build_saved_harness_runtime(
            inherit_task=body.inherit_task,
            provider=body.provider,
            model=body.model,
            reasoning_effort=body.reasoning_effort,
            codex_service_tier=body.codex_service_tier,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Authorization and validation above intentionally happen against the
    # optimistic read.  Before persisting, take a portable exact-generation
    # writer barrier and then merge into a fresh metadata snapshot.  A
    # terminalizer may have installed its durable Harness gate after the
    # optimistic read; writing the old dict here would otherwise erase it.
    identity = test_harness_owner_identity(task)
    await db.rollback()
    db.expire_all()
    fenced = await db.execute(
        update(Task)
        .where(
            Task.id == identity.task_id,
            Task.incarnation_id == identity.incarnation_id,
            Task.retry_count == identity.retry_count,
            Task.turn_generation == identity.turn_generation,
            Task.status == identity.status,
        )
        .values(status=Task.status)
    )
    if fenced.rowcount != 1:
        raise HTTPException(
            status_code=409,
            detail="Task generation changed while Browser Agent config was saved",
        )
    current = await db.get(Task, task_id, populate_existing=True)
    if current is None:
        raise HTTPException(status_code=404, detail="Task not found")
    if current.status not in {"completed", "failed", "cancelled", "conflict"}:
        raise HTTPException(
            status_code=409,
            detail="等待当前 Task 回合结束后再修改 Browser Agent 配置",
        )
    current.metadata_ = {
        **(current.metadata_ or {}),
        HARNESS_RUNTIME_METADATA_KEY: saved,
    }
    await db.commit()
    try:
        return harness_runtime_config_payload(current)
    except ValueError as exc:  # pragma: no cover - just-validated assignment
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/{task_id}/test-runs", status_code=status.HTTP_202_ACCEPTED)
async def start_test_harness_run(
    task_id: int,
    body: TestHarnessRunStart,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    task = await _task_or_404(task_id, db)
    task = await lock_task_effect_access(
        request,
        task,
        db,
        allow_chat_share=False,
        fence_worker_node=True,
    )
    if task.status not in {"completed", "failed", "cancelled", "conflict"}:
        raise HTTPException(
            status_code=409,
            detail="等待当前 Task 回合结束后再从界面启动测试；执行中的 Agent 可直接调用测试工具",
        )
    locality_error = test_harness_owner_locality_error(task)
    if locality_error is not None:
        raise HTTPException(status_code=409, detail=locality_error)
    owner_identity = test_harness_owner_identity(task)
    # The Harness service materializes the durable Run in its own database
    # session under Node-control -> owner Task locks.  Commit this
    # Node-control -> Project -> Task -> membership -> User authorization
    # transaction first: it is the public effect's linearization point and
    # avoids holding the same owner rows across two independent sessions.
    await db.commit()
    return await _start(
        task=task,
        body=body,
        owner_user_id=get_current_user_id(request),
        inline=False,
        owner_identity=owner_identity,
    )


@router.post(
    "/{task_id}/test-runs/internal/start",
    status_code=status.HTTP_202_ACCEPTED,
)
async def start_test_harness_run_internal(
    task_id: int,
    body: TestHarnessRunStart,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    require_internal_service(request)
    if not isinstance(settings.auth_token, str) or not settings.auth_token.strip():
        raise HTTPException(
            status_code=503,
            detail="Test Harness requires AUTH_TOKEN to be configured",
        )
    task = await require_internal_task_incarnation(
        request,
        task_id,
        db,
        write_fence=True,
    )
    if task is None:
        raise HTTPException(
            status_code=403,
            detail="Scoped Test Harness credential required",
        )
    if task.status not in {"in_progress", "executing"}:
        raise HTTPException(
            status_code=409,
            detail="Test Harness tool requires its parent Task to be running",
        )
    # The ordinary Task only orchestrates and polls this run. Browser actions
    # belong to a separately routed black-box child Task so its model/effort
    # can differ from the parent Task without recording a false runtime.
    owner_identity = test_harness_owner_identity(task)
    owner_user_id = task.created_by
    # The service performs the materialization writer CAS in its own session.
    # Release this route fence, but carry the claims-derived exact identity so
    # the service cannot silently rebind the old credential to a newer turn.
    await db.commit()
    return await _start(
        task=task,
        body=body,
        owner_user_id=owner_user_id,
        inline=False,
        owner_identity=owner_identity,
    )


@router.get("/{task_id}/test-runs")
async def list_test_harness_runs(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> list[dict[str, Any]]:
    task = await _task_or_404(task_id, db)
    await require_task_control(request, task, db)
    # ACL evaluation has checked out the request-scoped connection.  Freshness
    # probing uses independent service sessions and can spend seconds in Git;
    # return this connection before entering that work so list polling cannot
    # deadlock a small pool or starve Task control endpoints.
    await db.rollback()
    try:
        await test_harness_service.refresh_task_staleness(task_id)
    except (TestHarnessError, WorkspaceReviewError, OSError, ValueError):
        # Historical evidence remains readable if the checkout is temporarily
        # unavailable; it simply cannot be freshly proven in this poll.
        pass
    return await test_harness_service.list_for_task(task_id)


@router.get("/{task_id}/test-runs/{run_id}")
async def get_test_harness_run(
    task_id: int,
    run_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    task = await _task_or_404(task_id, db)
    await require_task_control(request, task, db)
    run = await test_harness_service.get_run(run_id)
    if run is None or run["task_id"] != task_id:
        raise HTTPException(status_code=404, detail="Test run not found")
    return run


@router.get("/{task_id}/test-runs/{run_id}/internal/status")
async def get_test_harness_run_internal(
    task_id: int,
    run_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    require_internal_service(request)
    task = await require_internal_task_incarnation(
        request,
        task_id,
        db,
        write_fence=True,
    )
    if task is None:
        raise HTTPException(403, "Scoped Test Harness credential required")
    identity = test_harness_owner_identity(task)
    exact_run = await db.scalar(
        select(TestHarnessRun)
        .where(
            TestHarnessRun.id == run_id,
            TestHarnessRun.task_id == task_id,
            TestHarnessRun.owner_task_incarnation_id == identity.incarnation_id,
            TestHarnessRun.owner_task_retry_count == identity.retry_count,
            TestHarnessRun.owner_task_turn_generation == identity.turn_generation,
            TestHarnessRun.owner_task_status == identity.status,
        )
        .with_for_update()
    )
    if exact_run is None:
        raise HTTPException(status_code=404, detail="Test run not found")
    run = await test_harness_service.get_run(run_id)
    if run is None or run["task_id"] != task_id:
        raise HTTPException(status_code=404, detail="Test run not found")
    await db.rollback()
    return run


@router.post("/{task_id}/test-runs/{run_id}/cancel")
async def cancel_test_harness_run(
    task_id: int,
    run_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    task = await _task_or_404(task_id, db)
    await require_task_control(request, task, db)
    run = await test_harness_service.get_run(run_id)
    if run is None or run["task_id"] != task_id:
        raise HTTPException(status_code=404, detail="Test run not found")

    async def stop_agent(agent_task_id: int) -> None:
        from backend.api.tasks import cancel_task

        await cancel_task(agent_task_id, request, db)

    cancelled = await test_harness_service.cancel(
        run_id,
        stop_agent_task=stop_agent,
    )
    assert cancelled is not None
    payload = await test_harness_service.get_run(cancelled.id)
    assert payload is not None
    return payload


@router.post("/{task_id}/test-runs/{run_id}/internal/stop")
async def cancel_test_harness_run_internal(
    task_id: int,
    run_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    require_internal_service(request)
    task = await require_internal_task_incarnation(
        request,
        task_id,
        db,
        write_fence=True,
    )
    if task is None:
        raise HTTPException(403, "Scoped Test Harness credential required")
    identity = test_harness_owner_identity(task)
    exact_run = await db.scalar(
        select(TestHarnessRun)
        .where(
            TestHarnessRun.id == run_id,
            TestHarnessRun.task_id == task_id,
            TestHarnessRun.owner_task_incarnation_id == identity.incarnation_id,
            TestHarnessRun.owner_task_retry_count == identity.retry_count,
            TestHarnessRun.owner_task_turn_generation == identity.turn_generation,
            TestHarnessRun.owner_task_status == identity.status,
        )
        .with_for_update()
    )
    if exact_run is None:
        raise HTTPException(status_code=404, detail="Test run not found")
    await db.rollback()
    try:
        cancelled = await test_harness_service.cancel(
            run_id,
            expected_identity=identity,
        )
    except TestHarnessError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    assert cancelled is not None
    payload = await test_harness_service.get_run(cancelled.id)
    assert payload is not None
    return payload


@router.post("/{task_id}/test-runs/{run_id}/repeat", status_code=202)
async def repeat_test_harness_run(
    task_id: int,
    run_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    task = await _task_or_404(task_id, db)
    task = await lock_task_effect_access(
        request,
        task,
        db,
        allow_chat_share=False,
        fence_worker_node=True,
    )
    if task.status not in {"completed", "failed", "cancelled", "conflict"}:
        raise HTTPException(
            status_code=409,
            detail="等待当前 Task 回合结束后再从界面重复测试",
        )
    locality_error = test_harness_owner_locality_error(task)
    if locality_error is not None:
        raise HTTPException(status_code=409, detail=locality_error)
    owner_identity = test_harness_owner_identity(task)
    # See start_test_harness_run(): authorize and release the route session
    # before the service takes its own Node/owner materialization fence.
    await db.commit()
    source = await test_harness_service.get_run(run_id)
    if source is None or source["task_id"] != task_id:
        raise HTTPException(status_code=404, detail="Test run not found")
    try:
        repeated = await test_harness_service.repeat(
            run_id,
            owner_user_id=get_current_user_id(request),
            owner_identity=owner_identity,
        )
        if repeated.target_kind == "fixed_url" and repeated.browser_review_job_id is None:
            await test_harness_service.start_fixed_url_browser(
                run_id=repeated.id,
                inline=False,
            )
    except TestHarnessError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    payload = await test_harness_service.get_run(repeated.id)
    assert payload is not None
    return payload


@router.get("/{task_id}/test-runs/{run_id}/compare/{candidate_run_id}")
async def compare_test_harness_runs(
    task_id: int,
    run_id: str,
    candidate_run_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    task = await _task_or_404(task_id, db)
    await require_task_control(request, task, db)
    base = await test_harness_service.get_run(run_id)
    candidate = await test_harness_service.get_run(candidate_run_id)
    if (
        base is None
        or candidate is None
        or base["task_id"] != task_id
        or candidate["task_id"] != task_id
    ):
        raise HTTPException(status_code=404, detail="Test run not found")
    try:
        result = await test_harness_service.compare(run_id, candidate_run_id)
    except TestHarnessError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return result


@router.get("/{task_id}/test-runs/{run_id}/evidence/{name}")
async def get_test_harness_evidence(
    task_id: int,
    run_id: str,
    name: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> StreamingResponse:
    task = await _task_or_404(task_id, db)
    await require_task_control(request, task, db)
    run = await test_harness_service.get_run(run_id)
    if run is None or run["task_id"] != task_id:
        raise HTTPException(status_code=404, detail="Test run not found")
    opened = await test_harness_service.open_evidence(run_id, name)
    if opened is None:
        raise HTTPException(status_code=404, detail="Test evidence not found")
    suffix = Path(opened.name).suffix
    return StreamingResponse(
        opened.chunks(),
        media_type={
            ".png": "image/png",
            ".md": "text/markdown; charset=utf-8",
            ".json": "application/json",
            ".jsonl": "application/x-ndjson",
        }.get(suffix, "application/octet-stream"),
        headers={
            "Content-Length": str(opened.byte_size),
            "Content-Disposition": f'inline; filename="{opened.name}"',
        },
        background=BackgroundTask(opened.close),
    )
