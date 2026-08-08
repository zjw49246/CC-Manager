from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.deps import (
    require_admin,
    require_internal_service,
    require_task_access,
    require_task_control,
)
from backend.database import get_db
from backend.models.project import Project
from backend.models.task import Task
from backend.models.workspace_review import WorkspaceReviewRun
from backend.services.workspace_review import (
    PreviewConfigurationError,
    WorkspaceReviewBusyError,
    WorkspaceReviewError,
    refresh_workspace_review_staleness,
    validate_preview_config,
    workspace_review_capability,
    workspace_review_manager,
    workspace_review_run_dict,
)
from backend.services.test_harness import (
    TestHarnessBusyError,
    TestHarnessError,
    test_harness_service,
)
from backend.services.test_harness_contracts import (
    DEFAULT_BROWSER_CHANNEL,
    TestHarnessContractError,
    TestHarnessSpec,
)


router = APIRouter(prefix="/api/tasks", tags=["workspace-reviews"])


class WorkspacePreviewConfigApproval(BaseModel):
    config: dict[str, Any]


class WorkspaceReviewStart(BaseModel):
    goal: str = Field(min_length=1, max_length=20_000)
    mode: Literal["review_only", "fix_loop"] = "review_only"
    profile: Literal["quick", "standard", "exhaustive"] = "standard"
    allow_actions: bool = True
    browser_channel: Literal["chrome", "chromium"] = DEFAULT_BROWSER_CHANNEL
    viewport_width: int = Field(default=1440, ge=320, le=3840)
    viewport_height: int = Field(default=900, ge=320, le=2160)
    provider: Literal["claude", "codex"] | None = None
    model: str | None = Field(default=None, min_length=1, max_length=100)
    reasoning_effort: str | None = Field(default=None, min_length=1, max_length=20)
    codex_service_tier: Literal["default", "priority"] | None = None


async def _task_or_404(task_id: int, db: AsyncSession) -> Task:
    task = await db.get(Task, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


async def _start_review(task_id: int, body: WorkspaceReviewStart) -> dict[str, Any]:
    if body.mode != "review_only":
        raise HTTPException(
            status_code=422,
            detail="循环修改由 Task Goal/Loop Controller 编排；Test Harness 每次只执行一次黑盒测试",
        )
    try:
        harness_run = await test_harness_service.start_task_run(
            task_id=task_id,
            spec=TestHarnessSpec(
                target_kind="current_workspace",
                target={},
                goal=body.goal,
                profile=body.profile,
                allow_actions=body.allow_actions,
                browser_channel=body.browser_channel,
                viewport_width=body.viewport_width,
                viewport_height=body.viewport_height,
                provider=body.provider,
                model=body.model,
                reasoning_effort=body.reasoning_effort,
                codex_service_tier=body.codex_service_tier,
            ),
        )
        payload = await test_harness_service.get_run(harness_run.id)
        if payload is None or payload["workspace_review"] is None:
            raise TestHarnessError("Workspace Review adapter did not create a run")
        return payload["workspace_review"]
    except (WorkspaceReviewBusyError, TestHarnessBusyError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except PreviewConfigurationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (
        WorkspaceReviewError,
        TestHarnessError,
        TestHarnessContractError,
        ValueError,
    ) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/{task_id}/workspace-reviews/capabilities")
async def get_workspace_review_capabilities(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    task = await _task_or_404(task_id, db)
    await require_task_access(request, task, db)
    project = await db.get(Project, task.project_id) if task.project_id else None
    return workspace_review_capability(task, project)


@router.put("/{task_id}/workspace-reviews/preview-config")
async def approve_workspace_preview_config(
    task_id: int,
    body: WorkspacePreviewConfigApproval,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    require_admin(request)
    task = await _task_or_404(task_id, db)
    await require_task_control(request, task, db)
    if task.project_id is None:
        raise HTTPException(status_code=409, detail="Task is not bound to a Project")
    project = await db.get(Project, task.project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    capability = workspace_review_capability(task, project)
    workspace_path = capability.get("repo_path")
    if not isinstance(workspace_path, str):
        raise HTTPException(
            status_code=409,
            detail=capability.get("reason") or "Task workspace is unavailable",
        )
    try:
        from pathlib import Path

        normalized = validate_preview_config(body.config, Path(workspace_path))
    except (PreviewConfigurationError, OSError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    project.preview_config = normalized
    await db.commit()
    return workspace_review_capability(task, project)


@router.post(
    "/{task_id}/workspace-reviews",
    status_code=status.HTTP_202_ACCEPTED,
)
async def start_workspace_review(
    task_id: int,
    body: WorkspaceReviewStart,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    task = await _task_or_404(task_id, db)
    await require_task_control(request, task, db)
    if task.status not in {"completed", "failed", "cancelled", "conflict"}:
        raise HTTPException(
            status_code=409,
            detail="等待当前 Task 回合结束后再从界面启动测试；执行中的 Agent 可直接调用测试工具",
        )
    return await _start_review(task_id, body)


@router.post(
    "/{task_id}/workspace-reviews/internal/start",
    status_code=status.HTTP_202_ACCEPTED,
)
async def start_workspace_review_internal(
    task_id: int,
    body: WorkspaceReviewStart,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    require_internal_service(request)
    task = await _task_or_404(task_id, db)
    if task.status not in {"in_progress", "executing"}:
        raise HTTPException(
            status_code=409,
            detail="Workspace Review tool requires its parent Task to be running",
        )
    return await _start_review(task_id, body)


@router.get("/{task_id}/workspace-reviews")
async def list_workspace_reviews(
    task_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> list[dict[str, Any]]:
    task = await _task_or_404(task_id, db)
    await require_task_access(request, task, db)
    try:
        await refresh_workspace_review_staleness(task_id)
    except WorkspaceReviewError:
        # Historical results remain readable when the checkout is temporarily
        # unavailable; they simply cannot be re-proven as current.
        pass
    db.expire_all()
    runs = list(
        (
            await db.execute(
                select(WorkspaceReviewRun)
                .where(WorkspaceReviewRun.task_id == task_id)
                .order_by(WorkspaceReviewRun.created_at.desc())
                .limit(50)
            )
        ).scalars()
    )
    return [workspace_review_run_dict(run) for run in runs]


@router.get("/{task_id}/workspace-reviews/{run_id}")
async def get_workspace_review(
    task_id: int,
    run_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    task = await _task_or_404(task_id, db)
    await require_task_access(request, task, db)
    run = await db.get(WorkspaceReviewRun, run_id)
    if run is None or run.task_id != task_id:
        raise HTTPException(status_code=404, detail="Workspace Review not found")
    return workspace_review_run_dict(run)


@router.get("/{task_id}/workspace-reviews/{run_id}/internal/status")
async def get_workspace_review_internal(
    task_id: int,
    run_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    require_internal_service(request)
    run = await db.get(WorkspaceReviewRun, run_id)
    if run is None or run.task_id != task_id:
        raise HTTPException(status_code=404, detail="Workspace Review not found")
    return workspace_review_run_dict(run)


@router.post("/{task_id}/workspace-reviews/{run_id}/cancel")
async def cancel_workspace_review(
    task_id: int,
    run_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    task = await _task_or_404(task_id, db)
    await require_task_control(request, task, db)
    run = await db.get(WorkspaceReviewRun, run_id)
    if run is None or run.task_id != task_id:
        raise HTTPException(status_code=404, detail="Workspace Review not found")
    cancelled = await workspace_review_manager.cancel(run_id)
    if cancelled is None:
        raise HTTPException(status_code=404, detail="Workspace Review not found")
    return workspace_review_run_dict(cancelled)


@router.post("/{task_id}/workspace-reviews/{run_id}/internal/stop")
async def cancel_workspace_review_internal(
    task_id: int,
    run_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    require_internal_service(request)
    run = await db.get(WorkspaceReviewRun, run_id)
    if run is None or run.task_id != task_id:
        raise HTTPException(status_code=404, detail="Workspace Review not found")
    cancelled = await workspace_review_manager.cancel(run_id)
    assert cancelled is not None
    return workspace_review_run_dict(cancelled)
