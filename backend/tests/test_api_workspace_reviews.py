from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import pytest_asyncio
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient

from backend.api import workspace_reviews
from backend.database import get_db
from backend.models.project import Project
from backend.models.task import Task
from backend.models.workspace_review import WorkspaceReviewRun
from backend.services.workspace_review import workspace_review_run_dict


@pytest_asyncio.fixture
async def workspace_client(db_factory):
    app = FastAPI()

    @app.middleware("http")
    async def _admin_identity(request: Request, call_next):
        request.state.user_role = "admin"
        request.state.auth_type = "token"
        return await call_next(request)

    app.include_router(workspace_reviews.router)

    async def _get_db():
        async with db_factory() as db:
            yield db

    app.dependency_overrides[get_db] = _get_db
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client


def _make_vite_repo(tmp_path: Path) -> Path:
    workspace = tmp_path / "vite-project"
    workspace.mkdir()
    subprocess.run(["git", "init"], cwd=workspace, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "api-test@example.invalid"],
        cwd=workspace,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "API Test"],
        cwd=workspace,
        check=True,
    )
    (workspace / "package.json").write_text(
        '{"scripts":{"dev":"vite"}}',
        encoding="utf-8",
    )
    subprocess.run(["git", "add", "package.json"], cwd=workspace, check=True)
    subprocess.run(
        ["git", "commit", "-m", "initial"],
        cwd=workspace,
        check=True,
        capture_output=True,
    )
    return workspace


@pytest.mark.asyncio
async def test_workspace_review_api_confirms_preview_then_starts_one_shot(
    monkeypatch,
    tmp_path,
    workspace_client,
    db_factory,
):
    workspace = _make_vite_repo(tmp_path)
    async with db_factory() as db:
        project = Project(
            name="workspace-api-project",
            local_path=str(workspace),
            status="ready",
        )
        db.add(project)
        await db.flush()
        task = Task(
            title="Develop the Vite page",
            status="completed",
            project_id=project.id,
            target_repo=str(workspace),
            last_cwd=str(workspace),
            session_id="workspace-api-session",
            provider="codex",
            model="gpt-5.6-sol",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    capability = await workspace_client.get(
        f"/api/tasks/{task_id}/workspace-reviews/capabilities"
    )
    assert capability.status_code == 200, capability.text
    assert capability.json()["available"] is False
    suggestion = capability.json()["suggested_config"]
    assert suggestion["name"] == "Vite development preview"

    approved = await workspace_client.put(
        f"/api/tasks/{task_id}/workspace-reviews/preview-config",
        json={"config": suggestion},
    )
    assert approved.status_code == 200, approved.text
    assert approved.json()["available"] is True
    assert approved.json()["configured"] is True

    captured: dict = {}

    class FakeHarnessService:
        async def start_task_run(self, *, task_id, spec, **_kwargs):
            captured.update({"task_id": task_id, "spec": spec})
            now = datetime.utcnow()
            self.workspace = WorkspaceReviewRun(
                id="api-workspace-review",
                task_id=task_id,
                project_id=1,
                mode="review_only",
                profile=spec.profile,
                goal=spec.goal,
                status="queued",
                stage="queued",
                workspace_path=str(workspace),
                git_head="a" * 40,
                workspace_fingerprint="b" * 64,
                preview_config=suggestion,
                stale=False,
                cleanup_status="pending",
                created_at=now,
            )
            return SimpleNamespace(id="harness-workspace-review")

        async def get_run(self, _run_id):
            return {"workspace_review": workspace_review_run_dict(self.workspace)}

    monkeypatch.setattr(
        workspace_reviews,
        "test_harness_service",
        FakeHarnessService(),
    )
    started = await workspace_client.post(
        f"/api/tasks/{task_id}/workspace-reviews",
        json={
            "goal": "Verify the settings flow without modifying code",
            "mode": "review_only",
            "profile": "standard",
        },
    )
    assert started.status_code == 202, started.text
    assert started.json()["id"] == "api-workspace-review"
    assert started.json()["mode"] == "review_only"
    assert captured["task_id"] == task_id
    assert captured["spec"].goal == "Verify the settings flow without modifying code"
    assert captured["spec"].browser_channel == "chromium"


@pytest.mark.asyncio
async def test_workspace_review_public_start_waits_for_idle_task(
    tmp_path,
    workspace_client,
    db_factory,
):
    workspace = _make_vite_repo(tmp_path)
    async with db_factory() as db:
        project = Project(
            name="workspace-api-running-project",
            local_path=str(workspace),
            status="ready",
        )
        db.add(project)
        await db.flush()
        task = Task(
            title="Still running",
            status="executing",
            project_id=project.id,
            target_repo=str(workspace),
            last_cwd=str(workspace),
            session_id="running-session",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    response = await workspace_client.post(
        f"/api/tasks/{task_id}/workspace-reviews",
        json={"goal": "Do not race the active coding turn"},
    )
    assert response.status_code == 409
    assert "执行中的 Agent 可直接调用测试工具" in response.json()["detail"]
