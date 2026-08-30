from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from fastapi import FastAPI, HTTPException, Request
from httpx import ASGITransport, AsyncClient

from backend.api import workspace_reviews
from backend.config import settings
from backend.database import get_db
from backend.models.project import Project
from backend.models.task import Task
from backend.models.user import User
from backend.models.workspace_review import WorkspaceReviewRun
from backend.services.workspace_review import workspace_review_run_dict
from backend.services.internal_service_auth import InternalServiceClaims


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
    assert capability.json()["repo_path"] is None
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
    assert started.json()["workspace_path"] is None
    assert started.json()["preview_url"] is None
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


@pytest.mark.asyncio
async def test_workspace_review_start_rejects_cached_jwt_role_change(
    monkeypatch,
    db_factory,
):
    async with db_factory() as db:
        user = User(
            email="workspace-role-race@example.invalid",
            name="Workspace role race",
            password_hash="not-used",
            role="member",
            is_active=True,
        )
        db.add(user)
        await db.flush()
        task = Task(
            title="Workspace cached role admission",
            status="completed",
            created_by=user.id,
        )
        db.add(task)
        await db.commit()
        task_id = task.id
        user_id = user.id

    start_task_run = AsyncMock()
    monkeypatch.setattr(
        workspace_reviews,
        "test_harness_service",
        SimpleNamespace(
            start_task_run=start_task_run,
            get_run=AsyncMock(),
        ),
    )
    request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="jwt",
            user_role="admin",
            user_id=user_id,
        ),
        headers={},
    )
    async with db_factory() as db:
        with pytest.raises(HTTPException) as caught:
            await workspace_reviews.start_workspace_review(
                task_id,
                workspace_reviews.WorkspaceReviewStart(
                    goal="Do not use stale admin authority",
                ),
                request,
                db,
            )

    assert caught.value.status_code == 409
    assert "changed role" in caught.value.detail
    start_task_run.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("owner_fields", "message"),
    [
        ({"worker_id": 81}, "Worker-authoritative"),
        ({"shared_from_id": 82}, "Shared shadow"),
    ],
)
async def test_workspace_review_start_never_materializes_manager_mirror(
    monkeypatch,
    db_factory,
    owner_fields,
    message,
):
    async with db_factory() as db:
        task = Task(
            title="Remote Workspace mirror",
            status="completed",
            **owner_fields,
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    start_task_run = AsyncMock()
    monkeypatch.setattr(
        workspace_reviews,
        "test_harness_service",
        SimpleNamespace(
            start_task_run=start_task_run,
            get_run=AsyncMock(),
        ),
    )
    request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="token",
            user_role="super_admin",
            user_id=None,
        ),
        headers={},
    )
    async with db_factory() as db:
        with pytest.raises(HTTPException) as caught:
            await workspace_reviews.start_workspace_review(
                task_id,
                workspace_reviews.WorkspaceReviewStart(
                    goal="Do not materialize on the Manager mirror",
                ),
                request,
                db,
            )

    assert caught.value.status_code == 409
    assert message in caught.value.detail
    start_task_run.assert_not_awaited()


@pytest.mark.asyncio
async def test_internal_workspace_routes_revalidate_exact_parent_generation(
    monkeypatch,
    db_factory,
):
    monkeypatch.setattr(settings, "auth_token", "workspace-internal-secret")
    async with db_factory() as db:
        task = Task(
            title="Internal Workspace owner",
            status="executing",
            provider="codex",
            model="gpt-5.6-sol",
        )
        db.add(task)
        await db.flush()
        run = WorkspaceReviewRun(
            id="f" * 32,
            task_id=task.id,
            owner_task_incarnation_id=task.incarnation_id,
            owner_task_retry_count=task.retry_count,
            owner_task_turn_generation=task.turn_generation,
            owner_task_status=task.status,
            mode="review_only",
            profile="standard",
            goal="Review exact generation",
            status="queued",
            stage="queued",
            workspace_path="/workspace/project",
            git_head="a" * 40,
            workspace_fingerprint="b" * 64,
            preview_config={"commands": []},
            cleanup_status="pending",
        )
        db.add(run)
        await db.commit()
        task_id = task.id
        run_id = run.id
        claims = InternalServiceClaims(
            audience="ccm_workspace_review",
            token_id="workspace-internal-token",
            expires_at=4_000_000_000,
            task_id=task.id,
            task_incarnation_id=task.incarnation_id,
            task_retry_count=task.retry_count,
            task_turn_generation=task.turn_generation,
            task_status=task.status,
            owner_kind="task",
            owner_id=task.id,
        )
    request = SimpleNamespace(
        state=SimpleNamespace(
            auth_type="internal_service",
            internal_service_claims=claims,
        )
    )
    captured: dict[str, object] = {}

    class FakeHarnessService:
        async def start_task_run(self, *, task_id, owner_identity, **_kwargs):
            captured["start_identity"] = owner_identity
            return SimpleNamespace(id="h" * 32)

        async def get_run(self, _run_id):
            async with db_factory() as db:
                current = await db.get(WorkspaceReviewRun, run_id)
                return {"workspace_review": workspace_review_run_dict(current)}

    class FakeWorkspaceManager:
        async def cancel(self, requested_run_id, *, expected_identity):
            assert requested_run_id == run_id
            captured["stop_identity"] = expected_identity
            async with db_factory() as db:
                current = await db.get(WorkspaceReviewRun, run_id)
                current.status = "cancelled"
                current.stage = "cancelled"
                await db.commit()
                return current

    monkeypatch.setattr(
        workspace_reviews,
        "test_harness_service",
        FakeHarnessService(),
    )
    monkeypatch.setattr(
        workspace_reviews,
        "workspace_review_manager",
        FakeWorkspaceManager(),
    )
    body = workspace_reviews.WorkspaceReviewStart(
        goal="Review exact generation",
    )
    async with db_factory() as db:
        started = await workspace_reviews.start_workspace_review_internal(
            task_id,
            body,
            request,
            db,
        )
    assert started["id"] == run_id
    assert captured["start_identity"].incarnation_id == claims.task_incarnation_id

    async with db_factory() as db:
        status_payload = await workspace_reviews.get_workspace_review_internal(
            task_id,
            run_id,
            request,
            db,
        )
    assert status_payload["id"] == run_id

    async with db_factory() as db:
        stopped = await workspace_reviews.cancel_workspace_review_internal(
            task_id,
            run_id,
            request,
            db,
        )
    assert stopped["status"] == "cancelled"
    assert captured["stop_identity"].turn_generation == claims.task_turn_generation

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.retry_count += 1
        await db.commit()
    async with db_factory() as db:
        with pytest.raises(HTTPException) as caught:
            await workspace_reviews.get_workspace_review_internal(
                task_id,
                run_id,
                request,
                db,
            )
    assert caught.value.status_code == 403
