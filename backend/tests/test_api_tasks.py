"""Tests for Task API endpoints."""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.tests.group_acl_test_helpers import (
    grant_group_project_access,
    revoke_group_membership_at_effect_fence,
)
from backend.tests.test_auth_ws_security import (
    _create_user,
    secured_client as secured_client,
)


_SYSTEM_EXECUTION_PRINCIPAL = {
    "execution_user_id": None,
    "execution_user_role": "member",
    "execution_mode": "sandbox",
    "execution_principal_kind": "system",
}


async def _seed_group_project_control_task(
    session_factory,
    *,
    member_id: int,
    worker: bool = False,
    **task_values,
):
    """Create one Task controlled only through a group Project grant."""

    from backend.models.project import Project
    from backend.models.task import Task
    from backend.models.worker import Worker

    async with session_factory() as db:
        project = Project(
            name=f"task-control-effect-{member_id}-{worker}",
            status="ready",
        )
        db.add(project)
        worker_row = None
        if worker:
            worker_row = Worker(
                name=f"task-control-worker-{member_id}",
                status="ready",
                private_ip="10.0.0.81",
                auth_token="task-control-worker-token",
            )
            db.add(worker_row)
        await db.flush()
        values = {
            "title": "Task control effect fence",
            "description": "group Project authority is revoked",
            "project_id": project.id,
            "created_by": 999,
            "status": "completed",
            "worker_id": worker_row.id if worker_row is not None else None,
        }
        values.update(task_values)
        task = Task(**values)
        db.add(task)
        await db.commit()
        return project.id, task.id


@pytest.mark.asyncio
async def test_projectless_task_create_rejects_concurrent_wal_admin_demotion(
    tmp_path,
    monkeypatch,
):
    """Task commit re-locks the exact JWT role cached by authentication."""

    from types import SimpleNamespace

    from fastapi import HTTPException
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from backend.api import tasks as tasks_api
    from backend.config import settings
    from backend.database import Base
    from backend.models.task import Task
    from backend.models.user import User
    from backend.schemas.task import TaskCreate

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'task-create-admin-role.db'}",
        connect_args={"timeout": 2},
    )
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.exec_driver_sql(
                "PRAGMA journal_mode=WAL"
            )
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        async with sessions() as setup:
            admin = User(
                email="task-create-demoted-admin@example.com",
                name="task-create-admin",
                password_hash="not-used",
                role="admin",
                is_active=True,
            )
            setup.add(admin)
            await setup.commit()
            admin_id = admin.id

        request = SimpleNamespace(
            state=SimpleNamespace(
                user_id=admin_id,
                user_role="admin",
                auth_type="jwt",
            ),
            headers={},
        )
        validation_entered = asyncio.Event()
        release_validation = asyncio.Event()

        async def blocked_skill_validation(*_args, **_kwargs):
            validation_entered.set()
            await release_validation.wait()
            return None

        monkeypatch.setattr(settings, "auth_token", "task-wal-auth-token")
        monkeypatch.setattr(settings, "ccm_node_role", "manager")
        monkeypatch.setattr(
            tasks_api,
            "_validate_skill_configuration",
            blocked_skill_validation,
        )

        async def create_projectless_task():
            async with sessions() as creator:
                return await tasks_api.create_task(
                    request=request,
                    body=TaskCreate(
                        title="Must not retain stale admin authority",
                        description="Concurrent WAL role demotion",
                    ),
                    queue=MagicMock(),
                    db=creator,
                )

        pending = asyncio.create_task(create_projectless_task())
        await asyncio.wait_for(validation_entered.wait(), timeout=2)
        async with sessions() as demoter:
            changed = await demoter.execute(
                update(User)
                .where(User.id == admin_id, User.role == "admin")
                .values(role="member")
            )
            assert changed.rowcount == 1
            await demoter.commit()
        release_validation.set()

        with pytest.raises(HTTPException) as rejected:
            await pending
        assert rejected.value.status_code == 409
        assert "changed role" in rejected.value.detail
        async with sessions() as verify:
            assert await verify.scalar(select(func.count(Task.id))) == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_project_task_create_locks_node_before_final_project_authority(
    client,
    session_factory,
    monkeypatch,
):
    """The Worker node-control row remains outer to Project/User authority."""

    from backend.api import tasks as tasks_api
    from backend.models.project import Project

    async with session_factory() as db:
        project = Project(
            name="task-create-node-order",
            local_path="/tmp",
            status="ready",
        )
        db.add(project)
        await db.commit()
        project_id = project.id

    order = []
    original_node_fence = tasks_api.fence_worker_node_mutation
    original_project_fence = tasks_api.lock_project_worker_effect_access

    async def record_node_fence(db):
        order.append("node")
        return await original_node_fence(db)

    async def record_project_fence(request, observed_project_id, db):
        order.append("project")
        return await original_project_fence(request, observed_project_id, db)

    monkeypatch.setattr(
        tasks_api,
        "fence_worker_node_mutation",
        record_node_fence,
    )
    monkeypatch.setattr(
        tasks_api,
        "lock_project_worker_effect_access",
        record_project_fence,
    )

    created = await client.post(
        "/api/tasks",
        json={
            "title": "Node fence precedes Project authority",
            "description": "lock-order regression",
            "project_id": project_id,
        },
    )

    assert created.status_code == 201, created.text
    assert order[:2] == ["node", "project"]


@pytest.mark.asyncio
async def test_task_create_and_update_stop_when_acl_is_revoked_at_effect_fence(
    client,
    session_factory,
):
    from fastapi import HTTPException

    import backend.api.tasks as tasks_api
    from backend.models.project import Project
    from backend.models.task import Task

    async with session_factory() as db:
        project = Project(name="task-effect-acl-race", status="ready")
        db.add(project)
        await db.flush()
        task = Task(
            title="unchanged",
            description="unchanged",
            project_id=project.id,
            created_by=1,
        )
        db.add(task)
        await db.commit()
        project_id, task_id = project.id, task.id

    revoked = HTTPException(403, "access revoked while waiting for writer fence")
    with patch.object(
        tasks_api,
        "lock_project_worker_effect_access",
        AsyncMock(side_effect=revoked),
    ):
        created = await client.post("/api/tasks", json={
            "title": "must not exist",
            "description": "denied",
            "project_id": project_id,
        })
    with patch.object(
        tasks_api,
        "lock_task_effect_access",
        AsyncMock(side_effect=revoked),
    ):
        updated = await client.put(
            f"/api/tasks/{task_id}",
            json={"title": "must not change"},
        )

    assert created.status_code == 403
    assert updated.status_code == 403
    async with session_factory() as db:
        current = await db.get(Task, task_id)
        assert current.title == "unchanged"
        assert await db.scalar(
            select(func.count(Task.id)).where(Task.title == "must not exist")
        ) == 0


async def _post_migration_import(client, payload):
    """Send the explicit fail-closed principal required by Worker import."""

    from backend.config import settings

    previous_role = settings.ccm_node_role
    previous_token = settings.auth_token
    settings.ccm_node_role = "worker"
    settings.auth_token = "worker-migration-test-token"
    try:
        task_id = int(payload["id"])
        return await client.post(
            "/api/tasks/migration-import",
            json={
                "migration_operation_id": f"{task_id:032x}",
                "migration_operation_sequence": 1,
                **payload,
                **_SYSTEM_EXECUTION_PRINCIPAL,
            },
            headers={
                "Authorization": "Bearer worker-migration-test-token"
            },
        )
    finally:
        settings.ccm_node_role = previous_role
        settings.auth_token = previous_token


async def _post_migration_import_rollback(client, payload):
    """Call the drain-safe exact destination rollback as the Manager."""

    from backend.config import settings

    previous_role = settings.ccm_node_role
    previous_token = settings.auth_token
    settings.ccm_node_role = "worker"
    settings.auth_token = "worker-migration-test-token"
    try:
        return await client.post(
            "/api/tasks/migration-import/rollback",
            json={"operation_sequence": 1, **payload},
            headers={
                "Authorization": "Bearer worker-migration-test-token"
            },
        )
    finally:
        settings.ccm_node_role = previous_role
        settings.auth_token = previous_token


async def _post_migration_import_commit(client, payload):
    """Commit one exact destination reservation as the Manager."""

    from backend.config import settings

    previous_role = settings.ccm_node_role
    previous_token = settings.auth_token
    settings.ccm_node_role = "worker"
    settings.auth_token = "worker-migration-test-token"
    try:
        return await client.post(
            "/api/tasks/migration-import/commit",
            json={"operation_sequence": 1, **payload},
            headers={
                "Authorization": "Bearer worker-migration-test-token"
            },
        )
    finally:
        settings.ccm_node_role = previous_role
        settings.auth_token = previous_token


# === Existing tests ===


@pytest.mark.asyncio
async def test_create_task(client):
    resp = await client.post("/api/tasks", json={
        "title": "Test",
        "description": "Do something",
        "target_repo": "/tmp/repo",
        "priority": 1,
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["title"] == "Test"
    assert data["status"] == "pending"
    assert data["priority"] == 1


@pytest.mark.asyncio
async def test_create_task_with_explicit_id_uses_internal_service_gate(
    client,
    monkeypatch,
):
    from fastapi import HTTPException
    from backend.config import settings

    with patch(
        "backend.api.tasks.require_internal_service",
        side_effect=HTTPException(
            403,
            "Internal service authentication required",
        ),
    ) as require_internal:
        rejected = await client.post("/api/tasks", json={
            "id": 7001,
            "title": "caller-chosen identity",
            "description": "must be internal",
        })

    assert rejected.status_code == 403
    require_internal.assert_called_once()

    forwarded = {
        "id": 7001,
        "source_incarnation_id": "7" * 32,
        "source_retry_count": 0,
        "source_turn_generation": 1,
        "title": "manager-forwarded identity",
        "description": "accepted without configured auth",
        "execution_user_id": 41,
        "execution_user_role": "admin",
        "execution_mode": "unrestricted",
        "execution_principal_kind": "delegated_user",
    }
    wrong_node = await client.post("/api/tasks", json=forwarded)
    assert wrong_node.status_code == 409
    assert "CCM_NODE_ROLE=worker" in wrong_node.text

    # Explicit mirrored ids are valid only on an authenticated Worker.  A
    # Worker never inherits the Manager's legacy auth-disabled semantics.
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    blocked = await client.post("/api/tasks", json=forwarded)
    assert blocked.status_code == 503
    monkeypatch.setattr(settings, "auth_token", "worker-forward-test-token")
    client.headers["Authorization"] = "Bearer worker-forward-test-token"
    allowed = await client.post("/api/tasks", json=forwarded)
    assert allowed.status_code == 201, allowed.text
    assert allowed.json()["id"] == 7001

    # A Worker deployment token is control-plane authentication, not a public
    # Task-creation identity. Worker-local derived Tasks use the internal
    # allocator/service boundary; this HTTP route accepts only an explicit
    # Manager mirror.
    generated = await client.post("/api/tasks", json={
        "title": "locally generated identity",
        "description": "must not collide with the Worker mirror",
    })
    assert generated.status_code == 403, generated.text
    assert "explicit Manager-mirrored Task identity" in generated.text


@pytest.mark.asyncio
@pytest.mark.parametrize("source_mode", ["plan", "canonical_link"])
async def test_create_task_cannot_clone_a_legacy_plan_carrier(
    client,
    session_factory,
    source_mode,
):
    from backend.models.plan import Plan, PlanLegacyTaskLink
    from backend.models.task import Task

    async with session_factory() as db:
        source = Task(
            title="legacy Plan source",
            description="approved planning history",
            status="completed",
            mode="plan" if source_mode == "plan" else "auto",
            session_id="legacy-plan-session",
            last_cwd="/repo",
        )
        db.add(source)
        await db.flush()
        if source_mode == "canonical_link":
            plan = Plan(
                title="Canonical Plan",
                initial_request="plan this",
                pipeline_config={},
            )
            db.add(plan)
            await db.flush()
            db.add(
                PlanLegacyTaskLink(
                    legacy_task_id=source.id,
                    plan_id=plan.id,
                    plan_version_id=None,
                )
            )
        await db.commit()
        source_id = source.id

    response = await client.post("/api/tasks", json={
        "title": "bypass canonical execution",
        "description": "must not inherit the Plan session",
        "clone_from_task_id": source_id,
    })

    assert response.status_code == 409, response.text
    assert "Plan Tasks cannot be used as clone sources" in response.text


@pytest.mark.asyncio
async def test_create_task_rejects_unknown_mode_before_write(
    client,
    session_factory,
):
    from backend.models.task import Task

    response = await client.post("/api/tasks", json={
        "title": "Unknown mode",
        "description": "must not silently become Auto",
        "mode": "delivery",
    })

    assert response.status_code == 422
    error = response.json()["detail"][0]
    assert error["loc"] == ["body", "mode"]
    assert error["type"] == "literal_error"
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_mode", ["delivery", "AUTO", " auto ", None])
async def test_update_task_rejects_invalid_mode_without_mutation(
    client,
    session_factory,
    invalid_mode,
):
    from backend.models.task import Task

    created = await client.post("/api/tasks", json={
        "title": "Keep Auto mode",
        "description": "d",
    })
    assert created.status_code == 201, created.text
    task_id = created.json()["id"]

    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"mode": invalid_mode},
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "mode"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.mode == "auto"


@pytest.mark.asyncio
async def test_update_task_rejects_valid_mode_change_without_mutation(
    client,
    session_factory,
):
    from backend.models.task import Task

    created = await client.post("/api/tasks", json={
        "title": "Immutable lifecycle mode",
        "description": "must remain Auto",
    })
    assert created.status_code == 201, created.text
    task_id = created.json()["id"]

    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"mode": "loop"},
    )

    assert response.status_code == 422
    error = response.json()["detail"][0]
    assert error["loc"] == ["body", "mode"]
    assert "immutable" in error["msg"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.mode == "auto"


@pytest.mark.asyncio
async def test_update_task_rejects_immutable_browser_child_binding(
    client,
    session_factory,
):
    from backend.models.task import Task
    from backend.models.test_harness import TestHarnessChildBinding

    async with session_factory() as db:
        owner = Task(
            title="Browser owner",
            description="Own the isolated child",
            status="completed",
        )
        db.add(owner)
        await db.flush()
        child = Task(
            title="Immutable Browser child",
            description="Frozen launch profile",
            status="pending_activation",
            provider="codex",
            model="gpt-5.6-sol",
            effort_level="high",
            enabled_skills={"browser-review": "immutable-job"},
            metadata_={"isolated_browser_agent": True},
            archived=True,
        )
        db.add(child)
        await db.flush()
        db.add(
            TestHarnessChildBinding(
                id="e" * 32,
                harness_run_id="f" * 32,
                owner_task_id=owner.id,
                owner_task_incarnation_id=owner.incarnation_id,
                owner_task_retry_count=owner.retry_count,
                owner_task_turn_generation=owner.turn_generation,
                owner_task_status=owner.status,
                child_task_id=child.id,
                child_task_incarnation_id=child.incarnation_id,
                browser_review_job_id="immutable-job",
                state="reserved",
            )
        )
        await db.commit()
        child_id = child.id

    response = await client.put(
        f"/api/tasks/{child_id}",
        json={"title": "Mutated Browser child"},
    )

    assert response.status_code == 409, response.text
    assert "Harness owner" in response.text
    async with session_factory() as db:
        child = await db.get(Task, child_id)
        assert child.title == "Immutable Browser child"


@pytest.mark.asyncio
@pytest.mark.parametrize("graph_kind", ["harness", "workspace", "browser_child"])
async def test_update_task_rejects_active_browser_owner_graph(
    client,
    session_factory,
    graph_kind,
):
    from backend.models.task import Task
    from backend.models.test_harness import (
        TestHarnessChildBinding,
        TestHarnessRun,
    )
    from backend.models.workspace_review import WorkspaceReviewRun

    async with session_factory() as db:
        owner = Task(
            title="Frozen Browser graph owner",
            description="active graph freezes public Task edits",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
        )
        db.add(owner)
        await db.flush()
        identity = {
            "owner_task_incarnation_id": owner.incarnation_id,
            "owner_task_retry_count": owner.retry_count,
            "owner_task_turn_generation": owner.turn_generation,
            "owner_task_status": owner.status,
        }
        if graph_kind == "harness":
            run_id = "1" * 32
            db.add(
                TestHarnessRun(
                    id=run_id,
                    task_id=owner.id,
                    **identity,
                    target_kind="fixed_url",
                    target_spec={"url": "https://example.com"},
                    test_plan={"objective": "freeze owner"},
                    runtime_config={"provider": "codex"},
                    request_fingerprint="a" * 64,
                    root_run_id=run_id,
                    status="running",
                    stage="waiting_for_agent",
                )
            )
        elif graph_kind == "workspace":
            db.add(
                WorkspaceReviewRun(
                    id="2" * 32,
                    task_id=owner.id,
                    **identity,
                    mode="review_only",
                    profile="standard",
                    goal="freeze owner",
                    status="reviewing",
                    stage="browser_agent_queued",
                    workspace_path="/repo",
                    git_head="b" * 40,
                    workspace_fingerprint="c" * 64,
                    preview_config={"kind": "http"},
                    cleanup_status="pending",
                )
            )
        else:
            child = Task(
                title="Frozen Browser child",
                description="child",
                status="pending_activation",
                provider="codex",
                model="gpt-5.6-sol",
                archived=True,
                metadata_={"isolated_browser_agent": True},
            )
            db.add(child)
            await db.flush()
            db.add(
                TestHarnessChildBinding(
                    id="3" * 32,
                    harness_run_id="4" * 32,
                    owner_task_id=owner.id,
                    **identity,
                    child_task_id=child.id,
                    child_task_incarnation_id=child.incarnation_id,
                    browser_review_job_id="5" * 32,
                    state="ready",
                )
            )
        await db.commit()
        owner_id = owner.id

    response = await client.put(
        f"/api/tasks/{owner_id}",
        json={"title": "must not change while Browser graph is active"},
    )

    assert response.status_code == 409, response.text
    assert "active Test Harness" in response.text
    async with session_factory() as db:
        owner = await db.get(Task, owner_id)
        assert owner.title == "Frozen Browser graph owner"


@pytest.mark.asyncio
async def test_update_task_final_writer_detects_late_harness_materialization(
    client,
    session_factory,
    monkeypatch,
):
    """The final Task CAS, not only the API preflight, owns the graph fence."""

    from backend.models.task import Task
    from backend.models.test_harness import TestHarnessRun
    from backend.services.task_queue import TaskQueue

    created = await client.post(
        "/api/tasks",
        json={"title": "Owner before materialization", "description": "owner"},
    )
    assert created.status_code == 201, created.text
    owner_id = created.json()["id"]
    original_update = TaskQueue.update_task
    inserted = False

    async def materialize_before_final_writer(self, task_id, **kwargs):
        nonlocal inserted
        if task_id == owner_id and not inserted:
            inserted = True
            async with session_factory() as db:
                owner = await db.get(Task, owner_id)
                run_id = "6" * 32
                db.add(
                    TestHarnessRun(
                        id=run_id,
                        task_id=owner.id,
                        owner_task_incarnation_id=owner.incarnation_id,
                        owner_task_retry_count=owner.retry_count,
                        owner_task_turn_generation=owner.turn_generation,
                        owner_task_status=owner.status,
                        target_kind="fixed_url",
                        target_spec={"url": "https://example.com"},
                        test_plan={"objective": "late materialization"},
                        runtime_config={"provider": "codex"},
                        request_fingerprint="d" * 64,
                        root_run_id=run_id,
                        status="running",
                        stage="waiting_for_agent",
                    )
                )
                await db.commit()
        return await original_update(self, task_id, **kwargs)

    monkeypatch.setattr(TaskQueue, "update_task", materialize_before_final_writer)
    response = await client.put(
        f"/api/tasks/{owner_id}",
        json={"title": "late stale edit"},
    )

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        owner = await db.get(Task, owner_id)
        assert owner.title == "Owner before materialization"


@pytest.mark.asyncio
async def test_update_task_allows_edit_after_harness_graph_is_terminal(
    client,
    session_factory,
):
    from backend.models.task import Task
    from backend.models.test_harness import TestHarnessRun

    async with session_factory() as db:
        owner = Task(
            title="Terminal graph owner",
            description="owner",
            status="completed",
        )
        db.add(owner)
        await db.flush()
        run_id = "7" * 32
        db.add(
            TestHarnessRun(
                id=run_id,
                task_id=owner.id,
                owner_task_incarnation_id=owner.incarnation_id,
                owner_task_retry_count=owner.retry_count,
                owner_task_turn_generation=owner.turn_generation,
                owner_task_status=owner.status,
                target_kind="fixed_url",
                target_spec={"url": "https://example.com"},
                test_plan={"objective": "historical evidence"},
                runtime_config={"provider": "codex"},
                request_fingerprint="e" * 64,
                    root_run_id=run_id,
                    status="completed",
                    stage="completed",
                    cleanup_status="completed",
                )
        )
        await db.commit()
        owner_id = owner.id

    response = await client.put(
        f"/api/tasks/{owner_id}",
        json={"title": "Editable after graph terminal"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["title"] == "Editable after graph terminal"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "suffix", "payload"),
    [
        ("DELETE", "", None),
        ("POST", "/cancel", None),
        ("POST", "/retry", None),
        ("POST", "/stop-session", None),
        ("POST", "/archive", None),
        ("POST", "/chat", {"message": "escape owner lifecycle"}),
        ("GET", "/fork-anchors", None),
        ("POST", "/fork", {"anchor": {"type": "latest"}}),
        ("GET", "/inject-capabilities", None),
        ("POST", "/inject", {"message": "steer child"}),
    ],
)
async def test_public_task_controls_reject_isolated_browser_marker(
    client,
    session_factory,
    method,
    suffix,
    payload,
):
    from backend.models.task import Task

    async with session_factory() as db:
        child = Task(
            title="Internal Browser child",
            description="not a public Task lifecycle",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
            session_id="browser-session-is-not-resumable",
            last_cwd="/tmp/browser-child",
            metadata_={"isolated_browser_agent": True},
            archived=True,
        )
        db.add(child)
        await db.commit()
        child_id = child.id

    response = await client.request(
        method,
        f"/api/tasks/{child_id}{suffix}",
        json=payload,
    )

    assert response.status_code == 409, response.text
    assert "Harness owner" in response.text
    async with session_factory() as db:
        assert await db.get(Task, child_id) is not None


@pytest.mark.asyncio
async def test_browser_child_cannot_be_shared_or_own_plan(client, session_factory):
    from backend.models.task import Task
    from backend.services import task_sharing

    async with session_factory() as db:
        child = Task(
            title="Internal Browser child",
            description="not shareable or plannable",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
            session_id="browser-session-is-not-resumable",
            metadata_={"isolated_browser_agent": True},
            archived=True,
        )
        db.add(child)
        await db.commit()
        child_id = child.id

    async with session_factory() as db:
        with pytest.raises(ValueError, match="Browser Agent"):
            await task_sharing.share_task(db, child_id, [])
    related_plan = await client.post(
        f"/api/tasks/{child_id}/plans",
        json={"title": "forbidden", "input": "forbidden"},
    )

    assert related_plan.status_code == 409, related_plan.text
    assert "Harness owner" in related_plan.text


@pytest.mark.asyncio
async def test_create_task_wakes_dispatcher_after_commit(client):
    """New work should not wait for the dispatcher's 2-second safety poll."""
    from backend.main import dispatcher

    with patch.object(dispatcher, "wake") as wake:
        resp = await client.post("/api/tasks", json={
            "title": "Wake now",
            "description": "Dispatch immediately",
        })

    assert resp.status_code == 201
    wake.assert_called_once_with()


@pytest.mark.asyncio
async def test_update_task_can_clear_nullable_runtime_configuration(
    client,
    session_factory,
):
    from backend.models.task import Task

    response = await client.post("/api/tasks", json={
        "title": "Clear nullable config",
        "description": "d",
        "thinking_budget": 123,
        "timeout_hours": 4,
        "system_prompt_mode": "append",
    })
    task_id = response.json()["id"]

    response = await client.put(f"/api/tasks/{task_id}", json={
        "thinking_budget": None,
        "timeout_hours": None,
        "system_prompt_mode": "off",
    })

    assert response.status_code == 200
    assert response.json()["thinking_budget"] is None
    assert response.json()["timeout_hours"] is None
    assert response.json()["system_prompt_mode"] is None
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.thinking_budget is None
        assert task.timeout_hours is None
        assert task.system_prompt_mode is None


@pytest.mark.asyncio
async def test_explicit_skill_save_clears_temporary_generation_marker(
    client,
    session_factory,
):
    from backend.models.task import Task
    from backend.services.task_skill_overrides import (
        TEMP_SKILLS_GENERATION_KEY,
    )

    response = await client.post("/api/tasks", json={
        "title": "Save temporary-looking skills",
        "description": "d",
        "provider": "claude",
        "enabled_skills": {"monitor": True},
    })
    task_id = response.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.metadata_ = {
            TEMP_SKILLS_GENERATION_KEY: "temporary-generation",
            "keep": "metadata",
        }
        await db.commit()

    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"enabled_skills": {"monitor": True}},
    )

    assert response.status_code == 200
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.enabled_skills == {"monitor": True}
        assert task.metadata_ == {"keep": "metadata"}


@pytest.mark.asyncio
async def test_internal_skill_update_endpoint_accepts_only_enabled_skills(client):
    created = await client.post("/api/tasks", json={
        "title": "Scoped skill update",
        "description": "d",
        "provider": "claude",
    })
    task_id = created.json()["id"]

    updated = await client.put(
        f"/api/tasks/{task_id}/internal/enabled-skills",
        json={"enabled_skills": {"monitor": True}},
    )
    rejected = await client.put(
        f"/api/tasks/{task_id}/internal/enabled-skills",
        json={
            "enabled_skills": {},
            "title": "must not be writable through the MCP credential",
        },
    )

    assert updated.status_code == 200
    assert updated.json()["enabled_skills"] == {"monitor": True}
    assert rejected.status_code == 422


@pytest.mark.asyncio
async def test_local_codex_accepts_monitor_when_main_mcp_is_enabled(
    client,
    monkeypatch,
):
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    response = await client.post("/api/tasks", json={
        "title": "Local Codex Monitor",
        "description": "d",
        "provider": "codex",
        "enabled_skills": {"monitor": True},
    })

    assert response.status_code == 201, response.text
    assert response.json()["enabled_skills"]["monitor"] is True


@pytest.mark.asyncio
async def test_local_codex_accepts_monitor_command_without_persisting_skill(
    client,
    monkeypatch,
):
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    response = await client.post("/api/tasks", json={
        "title": "Local Codex Monitor command",
        "description": "$monitor watch the build",
        "provider": "codex",
        "enabled_skills": {},
    })

    assert response.status_code == 201, response.text
    assert response.json()["enabled_skills"].get("monitor") is not True


@pytest.mark.asyncio
async def test_local_codex_accepts_monitor_command_added_by_task_update(
    client,
    monkeypatch,
):
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    created = await client.post("/api/tasks", json={
        "title": "Updated local Codex Monitor command",
        "description": "ordinary task",
        "provider": "codex",
    })

    response = await client.put(
        f"/api/tasks/{created.json()['id']}",
        json={"description": "$monitor watch the build"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["description"] == "$monitor watch the build"


@pytest.mark.asyncio
async def test_codex_worker_create_rejects_monitor_before_task_write(
    client,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.models.task import Task
    from backend.models.worker import Worker

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    async with session_factory() as db:
        db.add(Worker(
            id=40,
            name="monitor-closed-worker",
            status="ready",
            private_ip="10.0.0.40",
            auth_token="token",
        ))
        await db.commit()

    response = await client.post("/api/tasks", json={
        "title": "No Worker Codex Monitor",
        "description": "d",
        "provider": "codex",
        "worker_id": 40,
        "enabled_skills": {"monitor": True},
    })

    assert response.status_code == 400
    assert "does not support Skills: monitor" in response.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_codex_main_mcp_kill_switch_keeps_only_sub_agent(client):
    rejected = await client.post("/api/tasks", json={
        "title": "No ordinary skills",
        "description": "d",
        "provider": "codex",
        "enabled_skills": {"code-review": True},
    })
    allowed = await client.post("/api/tasks", json={
        "title": "Sub-Agent remains independent",
        "description": "d",
        "provider": "codex",
        "enabled_skills": {"sub-agent": True},
    })

    assert rejected.status_code == 400
    assert "main-task MCP is disabled" in rejected.text
    assert allowed.status_code == 201


@pytest.mark.asyncio
async def test_selected_user_skills_are_validated_and_deduplicated(
    client,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.models.user_skill import UserSkill

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    async with session_factory() as db:
        skill = UserSkill(
            name="API selected skill",
            description="selected",
            content="body",
        )
        db.add(skill)
        await db.commit()
        await db.refresh(skill)

    created = await client.post("/api/tasks", json={
        "title": "Selected User Skill",
        "description": "d",
        "provider": "codex",
        "selected_user_skills": [skill.id, skill.id],
    })
    missing = await client.post("/api/tasks", json={
        "title": "Missing User Skill",
        "description": "d",
        "provider": "codex",
        "selected_user_skills": [skill.id + 1000],
    })

    assert created.status_code == 201, created.text
    assert created.json()["selected_user_skills"] == [skill.id]
    assert missing.status_code == 400
    assert "do not exist" in missing.text


@pytest.mark.asyncio
async def test_worker_snapshot_ids_never_fall_back_to_local_user_skills(
    client,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.models.user_skill import UserSkill

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    async with session_factory() as db:
        local_skill = UserSkill(
            name="Worker-local collision",
            description="must not authorize a Manager selection",
            content="local body",
        )
        db.add(local_skill)
        await db.commit()
        await db.refresh(local_skill)

    response = await client.post("/api/tasks", json={
        "title": "Authoritative empty snapshot",
        "description": "d",
        "provider": "codex",
        "selected_user_skills": [local_skill.id],
        "user_skill_snapshots": [],
    })

    assert response.status_code == 400
    assert "do not exist" in response.text


@pytest.mark.asyncio
async def test_local_task_skill_snapshot_is_not_marked_worker_managed(
    client,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.models.task import Task

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    response = await client.post("/api/tasks", json={
        "title": "Local snapshot",
        "description": "keep native principal",
        "provider": "codex",
        "selected_user_skills": [8123],
        "user_skill_snapshots": [{
            "id": 8123,
            "name": "Manager supplied skill",
            "description": "snapshot",
            "content": "body",
        }],
    })

    assert response.status_code == 201, response.text
    async with session_factory() as db:
        task = await db.get(Task, response.json()["id"])
    assert task.metadata_["ccm_user_skill_snapshots"][0]["id"] == 8123
    assert "ccm_worker_managed_task" not in task.metadata_
    assert task.execution_principal_kind == "deployment_token"
    assert task.execution_mode == "unrestricted"


@pytest.mark.asyncio
async def test_unrelated_update_allows_legacy_codex_monitor_configuration(
    client,
    session_factory,
):
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            title="Legacy Codex Monitor",
            description="d",
            provider="codex",
            model="gpt-5.6-sol",
            enabled_skills={"monitor": True},
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    response = await client.put(
        f"/api/tasks/{task.id}",
        json={"model": "gpt-5.6-terra"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["model"] == "gpt-5.6-terra"


@pytest.mark.asyncio
async def test_local_provider_switch_accepts_inherited_monitor(
    client,
    monkeypatch,
):
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    created = await client.post("/api/tasks", json={
        "title": "Claude Monitor",
        "description": "d",
        "provider": "claude",
        "enabled_skills": {"monitor": True},
    })

    response = await client.put(
        f"/api/tasks/{created.json()['id']}",
        json={"provider": "codex"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["provider"] == "codex"
    assert response.json()["enabled_skills"]["monitor"] is True


@pytest.mark.asyncio
async def test_invalid_skill_update_is_rejected_before_worker_migration(
    client,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.models.task import Task
    from backend.models.worker import Worker

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)

    async with session_factory() as db:
        worker = Worker(
            id=41,
            name="monitor-destination",
            status="ready",
            private_ip="10.0.0.41",
            auth_token="token",
        )
        task = Task(
            title="Do not migrate invalid configuration",
            description="d",
            provider="codex",
            worker_id=None,
            enabled_skills={"monitor": True},
        )
        db.add_all([worker, task])
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    migrator = MagicMock()
    migrator.migrate = AsyncMock()
    with patch("backend.main.task_migrator", migrator):
        response = await client.put(f"/api/tasks/{task_id}", json={
            "worker_id": 41,
        })

    assert response.status_code == 400
    assert "does not support Skills: monitor" in response.text
    migrator.migrate.assert_not_awaited()
    async with session_factory() as db:
        persisted = await db.get(Task, task_id)
    assert persisted.worker_id is None
    assert persisted.provider == "codex"
    assert persisted.enabled_skills == {"monitor": True}


@pytest.mark.asyncio
async def test_valid_skill_update_is_coordinated_with_worker_migration(
    client,
    session_factory,
):
    from backend.models.task import Task
    from backend.models.worker import Worker

    async with session_factory() as db:
        worker = Worker(
            id=42,
            name="skill-destination",
            status="ready",
            private_ip="10.0.0.42",
            auth_token="token",
        )
        task = Task(
            title="Migrate final configuration",
            description="d",
            provider="claude",
            enabled_skills={},
        )
        db.add_all([worker, task])
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    captured = {}

    async def migrate(task_id, target, *, task_updates):
        captured.update(
            task_id=task_id,
            target=target,
            task_updates=task_updates,
        )
        async with session_factory() as db:
            persisted = await db.get(Task, task_id)
            for field, value in task_updates.items():
                setattr(persisted, field, value)
            persisted.worker_id = target
            await db.commit()

    migrator = MagicMock()
    migrator.migrate = AsyncMock(side_effect=migrate)
    with patch("backend.main.task_migrator", migrator):
        response = await client.put(f"/api/tasks/{task_id}", json={
            "worker_id": 42,
            "provider": "codex",
            "enabled_skills": {"sub-agent": True},
        })

    assert response.status_code == 200, response.text
    assert response.json()["worker_id"] == 42
    assert response.json()["provider"] == "codex"
    assert response.json()["enabled_skills"]["sub-agent"] is True
    assert captured == {
        "task_id": task_id,
        "target": 42,
        "task_updates": {
            "provider": "codex",
            "enabled_skills": {"sub-agent": True},
            "metadata_": {},
        },
    }


@pytest.mark.asyncio
async def test_migration_import_preserves_inert_status_without_waking_dispatcher(
    client, session_factory,
):
    """Worker imports preserve plan review without a pending dispatch window."""
    from backend.main import dispatcher
    from backend.models.task import Task

    with patch.object(dispatcher, "wake") as wake:
        resp = await _post_migration_import(client, {
            "id": 7001,
            "title": "Migrated",
            "description": "Resume an existing session",
            "provider": "claude",
            "session_id": "session-1",
            "last_cwd": "/workspace/repo",
            "retry_count": 2,
            "turn_generation": 7,
            "source_status": "plan_review",
            "source_incarnation_id": "7" * 32,
            "mode": "plan",
            "selected_user_skills": [81],
            "user_skill_snapshots": [{
                "id": 81,
                "name": "Migrated skill",
                "description": "Manager copy",
                "content": "full body",
            }],
        })

    assert resp.status_code == 201, resp.text
    assert resp.json()["status"] == "plan_review"
    assert resp.json()["turn_generation"] == 7
    wake.assert_not_called()
    async with session_factory() as db:
        task = await db.get(Task, 7001)
    assert task.status == "plan_review"
    assert task.session_id == "session-1"
    assert task.retry_count == 2
    assert task.turn_generation == 7
    assert task.execution_user_id is None
    assert task.execution_user_role == "member"
    assert task.execution_mode == "sandbox"
    assert task.execution_principal_kind == "system"
    assert task.selected_user_skills == [81]
    assert task.metadata_["ccm_user_skill_snapshots"] == [{
        "id": 81,
        "name": "Migrated skill",
        "description": "Manager copy",
        "content": "full body",
    }]
    assert task.metadata_["ccm_worker_managed_task"] is True


@pytest.mark.asyncio
async def test_migration_import_maps_source_incarnation_on_existing_copy(
    client,
    session_factory,
):
    """Refreshing a Worker mirror preserves the Manager's exact identity."""
    from backend.models.ssh_profile import SSHProfile
    from backend.models.task import Task
    from backend.models.task_share import TaskShare
    from backend.models.task_ssh_grant import TaskSSHGrant
    from backend.models.team_share import TeamTaskShare
    from backend.services.task_creation import (
        SOURCE_TASK_INCARNATION_METADATA_KEY,
    )

    async with session_factory() as db:
        task = Task(
            id=7004,
            title="stale Worker mirror",
            description="d",
            status="cancelled",
            incarnation_id="a" * 32,
        )
        profile = SSHProfile(
            name="stale-migration-grant",
            host="ssh.invalid",
            username="worker",
            key_path="/run/ccm/managed-key",
            public_key_fingerprint="SHA256:public",
            host_key_type="ssh-ed25519",
            host_key_value="AAAA",
            host_key_fingerprint="SHA256:host",
        )
        db.add_all([task, profile])
        await db.flush()
        db.add_all([
            TaskShare(
                task_id=task.id,
                shared_to_open_id="legacy-reader",
                shared_to_ccm_url="https://peer.invalid",
                share_token="legacy-migration-share",
            ),
            TeamTaskShare(
                task_id=task.id,
                target_type="user",
                target_id=41,
                permission="chat",
                shared_by=7,
            ),
            TaskSSHGrant(
                task_id=task.id,
                ssh_profile_id=profile.id,
                profile_revision=1,
                capabilities=["read"],
            ),
        ])
        await db.commit()

    response = await _post_migration_import(client, {
        "id": 7004,
        "title": "current Manager mirror",
        "description": "d",
        "source_status": "completed",
        "source_incarnation_id": "b" * 32,
    })

    assert response.status_code == 201, response.text
    async with session_factory() as db:
        current = await db.get(Task, 7004)
    assert current is not None
    assert current.incarnation_id == "b" * 32
    assert (
        current.metadata_[SOURCE_TASK_INCARNATION_METADATA_KEY]
        == "b" * 32
    )
    async with session_factory() as db:
        assert await db.scalar(
            select(TaskShare.id).where(TaskShare.task_id == 7004)
        ) is None
        assert await db.scalar(
            select(TeamTaskShare.id).where(TeamTaskShare.task_id == 7004)
        ) is None
        assert await db.scalar(
            select(TaskSSHGrant.id).where(TaskSSHGrant.task_id == 7004)
        ) is None

    exact_refresh = await _post_migration_import(client, {
        "id": 7004,
        "title": "bound Manager refresh",
        "description": "same logical Task",
        "source_status": "completed",
        "source_incarnation_id": "b" * 32,
    })
    assert exact_refresh.status_code == 201, exact_refresh.text

    mismatched = await _post_migration_import(client, {
        "id": 7004,
        "title": "stale different Manager",
        "description": "must not rebind",
        "source_status": "completed",
        "source_incarnation_id": "e" * 32,
    })
    assert mismatched.status_code == 409, mismatched.text
    omitted = await _post_migration_import(client, {
        "id": 7004,
        "title": "legacy identity-less Manager",
        "description": "must not overwrite a bound mirror",
        "source_status": "completed",
    })
    assert omitted.status_code == 422, omitted.text


@pytest.mark.asyncio
async def test_migration_import_maps_source_incarnation_on_new_copy(
    client,
    session_factory,
):
    """A new Worker mirror shares the Manager's exact immutable identity."""
    from backend.models.task import Task
    from backend.services.task_creation import (
        SOURCE_TASK_INCARNATION_METADATA_KEY,
    )

    source_incarnation = "d" * 32
    response = await _post_migration_import(client, {
        "id": 7006,
        "title": "new Manager mirror",
        "description": "d",
        "source_status": "completed",
        "source_incarnation_id": source_incarnation,
    })

    assert response.status_code == 201, response.text
    async with session_factory() as db:
        current = await db.get(Task, 7006)
    assert current is not None
    assert current.incarnation_id == source_incarnation
    assert (
        current.metadata_[SOURCE_TASK_INCARNATION_METADATA_KEY]
        == source_incarnation
    )


@pytest.mark.asyncio
async def test_migration_import_exact_rollback_survives_destination_drain(
    client,
    session_factory,
):
    """Destroy may claim the destination after import but before pointer cut."""

    from sqlalchemy import delete

    from backend.models.log_entry import LogEntry
    from backend.models.task import Task
    from backend.services.worker_node_control import begin_worker_node_drain

    task_id = 7010
    incarnation_id = "c" * 32
    operation_id = "d" * 32
    imported = await _post_migration_import(client, {
        "id": task_id,
        "title": "uncommitted destination mirror",
        "description": "d",
        "source_status": "completed",
        "source_incarnation_id": incarnation_id,
        "migration_operation_id": operation_id,
        "retry_count": 2,
        "turn_generation": 7,
    })
    assert imported.status_code == 201, imported.text

    async with session_factory() as db:
        await begin_worker_node_drain(db, claim="e" * 64)
        await db.commit()

    exact = {
        "task_id": task_id,
        "operation_id": operation_id,
        "incarnation_id": incarnation_id,
        "retry_count": 2,
        "turn_generation": 7,
        "source_status": "completed",
    }
    stale = await _post_migration_import_rollback(
        client,
        {**exact, "operation_id": "f" * 32},
    )
    assert stale.status_code == 409, stale.text
    wrong_generation = await _post_migration_import_rollback(
        client,
        {**exact, "turn_generation": 8},
    )
    assert wrong_generation.status_code == 409, wrong_generation.text
    async with session_factory() as db:
        assert await db.get(Task, task_id) is not None

        db.add(LogEntry(
            instance_id=1,
            task_id=task_id,
            event_type="assistant",
            content="unimported destination evidence must survive",
        ))
        await db.commit()

    evidence_blocked = await _post_migration_import_rollback(client, exact)
    assert evidence_blocked.status_code == 409, evidence_blocked.text
    async with session_factory() as db:
        assert await db.get(Task, task_id) is not None
        assert await db.scalar(
            select(LogEntry.id).where(LogEntry.task_id == task_id)
        ) is not None
        await db.execute(delete(LogEntry).where(LogEntry.task_id == task_id))
        await db.commit()

    rolled_back = await _post_migration_import_rollback(client, exact)
    assert rolled_back.status_code == 200, rolled_back.text
    assert rolled_back.json() == {
        "ok": True,
        "removed": True,
        "task_id": task_id,
        "operation_id": operation_id,
        "operation_sequence": 1,
    }
    async with session_factory() as db:
        assert await db.get(Task, task_id) is None


@pytest.mark.asyncio
async def test_migration_rollback_before_import_blocks_delayed_prepare(
    client,
    session_factory,
):
    """A lost prepare response cannot resurrect a mirror after rollback ACK."""

    from backend.models.task import Task
    from backend.models.task_migration import TaskMigrationOperation

    task_id = 7013
    incarnation_id = "2" * 32
    operation_id = "3" * 32
    exact = {
        "task_id": task_id,
        "operation_id": operation_id,
        "operation_sequence": 1,
        "incarnation_id": incarnation_id,
        "retry_count": 4,
        "turn_generation": 11,
        "source_status": "completed",
    }

    rolled_back = await _post_migration_import_rollback(client, exact)
    assert rolled_back.status_code == 200, rolled_back.text
    assert rolled_back.json() == {
        "ok": True,
        "removed": False,
        "task_id": task_id,
        "operation_id": operation_id,
        "operation_sequence": 1,
    }
    async with session_factory() as db:
        operation = await db.get(TaskMigrationOperation, operation_id)
        assert operation is not None
        assert operation.phase == "rolled_back"
        assert operation.active_task_id is None
        assert await db.get(Task, task_id) is None

    delayed_payload = {
        "id": task_id,
        "title": "must never materialize",
        "description": "d",
        "source_status": exact["source_status"],
        "source_incarnation_id": incarnation_id,
        "migration_operation_id": operation_id,
        "migration_operation_sequence": 1,
        "retry_count": exact["retry_count"],
        "turn_generation": exact["turn_generation"],
    }
    delayed = await _post_migration_import(client, delayed_payload)
    assert delayed.status_code == 409, delayed.text
    assert "already rolled_back" in delayed.text

    next_operation_id = "4" * 32
    next_prepare = await _post_migration_import(client, {
        **delayed_payload,
        "title": "new migration generation",
        "migration_operation_id": next_operation_id,
        "migration_operation_sequence": 2,
    })
    assert next_prepare.status_code == 201, next_prepare.text

    delayed_again = await _post_migration_import(client, delayed_payload)
    assert delayed_again.status_code == 409, delayed_again.text
    assert "stale" in delayed_again.text
    async with session_factory() as db:
        operations = (
            await db.execute(
                select(TaskMigrationOperation)
                .where(TaskMigrationOperation.task_id == task_id)
                .order_by(TaskMigrationOperation.operation_sequence)
            )
        ).scalars().all()
        assert [operation.phase for operation in operations] == [
            "rolled_back",
            "prepared",
        ]
        assert [operation.active_task_id for operation in operations] == [
            None,
            task_id,
        ]


@pytest.mark.asyncio
async def test_migration_rollback_waits_for_owner_before_node_receipt_fence(
    client,
    monkeypatch,
):
    """Rollback keeps operation -> owner -> node process/database lock order."""

    import backend.api.tasks as tasks_api
    from backend.services.test_harness_owner_fence import (
        test_harness_owner_fence,
    )

    task_id = 7024
    operation_id = "8" * 32
    exact = {
        "task_id": task_id,
        "operation_id": operation_id,
        "operation_sequence": 1,
        "incarnation_id": "9" * 32,
        "retry_count": 3,
        "turn_generation": 12,
        "source_status": "completed",
    }
    operation_entered = asyncio.Event()
    node_fence_called = asyncio.Event()
    observed_order: list[str] = []
    original_operation_lock = tasks_api.get_task_operation_lock
    original_node_fence = tasks_api.fence_worker_node_receipt_resolution

    @asynccontextmanager
    async def observed_operation_lock(locked_task_id):
        assert locked_task_id == task_id
        async with original_operation_lock(locked_task_id):
            observed_order.append("operation")
            operation_entered.set()
            yield

    async def observed_node_fence(db):
        observed_order.append("node")
        node_fence_called.set()
        return await original_node_fence(db)

    monkeypatch.setattr(
        tasks_api,
        "get_task_operation_lock",
        observed_operation_lock,
    )
    monkeypatch.setattr(
        tasks_api,
        "fence_worker_node_receipt_resolution",
        observed_node_fence,
    )

    request_task = None
    try:
        async with test_harness_owner_fence(task_id):
            request_task = asyncio.create_task(
                _post_migration_import_rollback(client, exact)
            )
            await asyncio.wait_for(operation_entered.wait(), timeout=1)
            # Event.set() does not pre-empt the request coroutine. By the time
            # this waiter resumes, the request has advanced to its next await:
            # the held owner fence in the correct implementation, or the node
            # writer in the old operation -> node -> owner implementation.
            await asyncio.sleep(0)
            assert observed_order == ["operation"]
            assert node_fence_called.is_set() is False

        response = await asyncio.wait_for(request_task, timeout=2)
    finally:
        if request_task is not None and not request_task.done():
            await asyncio.gather(request_task, return_exceptions=True)

    assert response.status_code == 200, response.text
    assert response.json() == {
        "ok": True,
        "removed": False,
        "task_id": task_id,
        "operation_id": operation_id,
        "operation_sequence": 1,
    }
    assert node_fence_called.is_set() is True
    assert observed_order == ["operation", "node"]


@pytest.mark.asyncio
async def test_migration_rollback_before_import_uses_drain_as_durable_barrier(
    client,
    session_factory,
):
    """A drain claim may replace the tombstone because it rejects all prepares."""

    from backend.models.task import Task
    from backend.models.task_migration import TaskMigrationOperation
    from backend.services.worker_node_control import begin_worker_node_drain

    task_id = 7014
    operation_id = "5" * 32
    incarnation_id = "6" * 32
    async with session_factory() as db:
        await begin_worker_node_drain(db, claim="7" * 64)
        await db.commit()

    exact = {
        "task_id": task_id,
        "operation_id": operation_id,
        "operation_sequence": 9,
        "incarnation_id": incarnation_id,
        "retry_count": 0,
        "turn_generation": 2,
        "source_status": "failed",
    }
    rolled_back = await _post_migration_import_rollback(client, exact)
    assert rolled_back.status_code == 200, rolled_back.text
    assert rolled_back.json()["removed"] is False

    delayed = await _post_migration_import(client, {
        "id": task_id,
        "title": "drained destination",
        "description": "d",
        "source_status": exact["source_status"],
        "source_incarnation_id": incarnation_id,
        "migration_operation_id": operation_id,
        "migration_operation_sequence": exact["operation_sequence"],
        "retry_count": exact["retry_count"],
        "turn_generation": exact["turn_generation"],
    })
    assert delayed.status_code == 409, delayed.text
    async with session_factory() as db:
        assert await db.get(Task, task_id) is None
        assert await db.scalar(
            select(TaskMigrationOperation.operation_id)
            .where(TaskMigrationOperation.task_id == task_id)
        ) is None


@pytest.mark.asyncio
async def test_committed_import_history_allows_only_a_newer_prepare(
    client,
    session_factory,
):
    from backend.models.task import Task
    from backend.models.task_migration import TaskMigrationOperation

    task_id = 7015
    incarnation_id = "8" * 32
    first_operation_id = "9" * 32
    first = {
        "task_id": task_id,
        "operation_id": first_operation_id,
        "operation_sequence": 1,
        "incarnation_id": incarnation_id,
        "retry_count": 1,
        "turn_generation": 3,
        "source_status": "completed",
    }
    imported = await _post_migration_import(client, {
        "id": task_id,
        "title": "first committed migration",
        "description": "d",
        "source_status": first["source_status"],
        "source_incarnation_id": incarnation_id,
        "migration_operation_id": first_operation_id,
        "migration_operation_sequence": 1,
        "retry_count": first["retry_count"],
        "turn_generation": first["turn_generation"],
    })
    assert imported.status_code == 201, imported.text
    committed = await _post_migration_import_commit(client, first)
    assert committed.status_code == 200, committed.text
    rejected_rollback = await _post_migration_import_rollback(client, first)
    assert rejected_rollback.status_code == 409, rejected_rollback.text

    second_operation_id = "a" * 32
    second = await _post_migration_import(client, {
        "id": task_id,
        "title": "second migration generation",
        "description": "d",
        "source_status": first["source_status"],
        "source_incarnation_id": incarnation_id,
        "migration_operation_id": second_operation_id,
        "migration_operation_sequence": 2,
        "retry_count": first["retry_count"],
        "turn_generation": first["turn_generation"],
    })
    assert second.status_code == 201, second.text

    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task is not None
        assert (
            task.metadata_["worker_migration_import_reservation"]
            ["operation_sequence"]
            == 2
        )
        operations = (
            await db.execute(
                select(TaskMigrationOperation)
                .where(TaskMigrationOperation.task_id == task_id)
                .order_by(TaskMigrationOperation.operation_sequence)
            )
        ).scalars().all()
        assert [operation.phase for operation in operations] == [
            "committed",
            "prepared",
        ]


@pytest.mark.asyncio
async def test_destination_drain_rejects_existing_migration_import_refresh(
    client,
    session_factory,
):
    """An existing inert mirror cannot mutate after node drain admission."""

    from backend.models.task import Task
    from backend.services.worker_node_control import begin_worker_node_drain

    task_id = 7011
    incarnation_id = "a" * 32
    initial_operation_id = "b" * 32
    imported = await _post_migration_import(client, {
        "id": task_id,
        "title": "original inert mirror",
        "description": "d",
        "source_status": "completed",
        "source_incarnation_id": incarnation_id,
        "migration_operation_id": initial_operation_id,
        "retry_count": 2,
        "turn_generation": 7,
    })
    assert imported.status_code == 201, imported.text

    async with session_factory() as db:
        await begin_worker_node_drain(db, claim="c" * 64)
        await db.commit()

    refreshed = await _post_migration_import(client, {
        "id": task_id,
        "title": "must not cross drain",
        "description": "d",
        "source_status": "completed",
        "source_incarnation_id": incarnation_id,
        "migration_operation_id": "d" * 32,
        "retry_count": 2,
        "turn_generation": 7,
    })
    assert refreshed.status_code == 409, refreshed.text

    async with session_factory() as db:
        current = await db.get(Task, task_id)
    assert current is not None
    assert current.title == "original inert mirror"
    assert (
        current.metadata_["worker_migration_import_reservation"]
        ["operation_id"]
        == initial_operation_id
    )


@pytest.mark.asyncio
async def test_committed_destination_import_can_never_be_rolled_back(
    client,
    session_factory,
):
    """Pointer-cut acknowledgement is durable, drain-safe, and idempotent."""

    from backend.models.task import Task
    from backend.services.worker_node_control import begin_worker_node_drain

    exact = {
        "task_id": 7012,
        "operation_id": "e" * 32,
        "incarnation_id": "f" * 32,
        "retry_count": 3,
        "turn_generation": 9,
        "source_status": "completed",
    }
    imported = await _post_migration_import(client, {
        "id": exact["task_id"],
        "title": "authoritative destination",
        "description": "d",
        "source_status": exact["source_status"],
        "source_incarnation_id": exact["incarnation_id"],
        "migration_operation_id": exact["operation_id"],
        "retry_count": exact["retry_count"],
        "turn_generation": exact["turn_generation"],
    })
    assert imported.status_code == 201, imported.text

    async with session_factory() as db:
        await begin_worker_node_drain(db, claim="1" * 64)
        await db.commit()

    committed = await _post_migration_import_commit(client, exact)
    assert committed.status_code == 200, committed.text
    committed_again = await _post_migration_import_commit(client, exact)
    assert committed_again.status_code == 200, committed_again.text

    stale_rollback = await _post_migration_import_rollback(client, exact)
    assert stale_rollback.status_code == 409, stale_rollback.text
    async with session_factory() as db:
        current = await db.get(Task, exact["task_id"])
    assert current is not None
    assert (
        current.metadata_["worker_migration_import_commit_receipt"]
        ["operation_id"]
        == exact["operation_id"]
    )


@pytest.mark.asyncio
async def test_migration_import_without_source_preserves_existing_incarnation(
    client,
    session_factory,
):
    """Legacy imports cannot silently replace a destination identity fence."""
    from backend.models.task import Task

    existing_incarnation = "c" * 32
    async with session_factory() as db:
        task = Task(
            id=7005,
            title="legacy Worker mirror",
            description="d",
            status="cancelled",
            incarnation_id=existing_incarnation,
        )
        db.add(task)
        await db.commit()

    response = await _post_migration_import(client, {
        "id": 7005,
        "title": "legacy Manager refresh",
        "description": "d",
        "source_status": "completed",
    })

    assert response.status_code == 422, response.text
    async with session_factory() as db:
        current = await db.get(Task, 7005)
    assert current is not None
    assert current.incarnation_id == existing_incarnation


@pytest.mark.asyncio
async def test_migration_import_cannot_repurpose_existing_task_mode(
    client,
    session_factory,
):
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            id=7010,
            title="existing Auto task",
            description="keep its authority",
            status="cancelled",
            mode="auto",
        )
        db.add(task)
        await db.commit()

    response = await _post_migration_import(client, {
        "id": 7010,
        "title": "forged Goal replacement",
        "description": "must not replace",
        "mode": "goal",
        "goal_condition": "done",
        "source_status": "cancelled",
        "source_incarnation_id": task.incarnation_id,
    })

    assert response.status_code == 409, response.text
    assert "cannot change an existing Task mode" in response.text
    async with session_factory() as db:
        current = await db.get(Task, 7010)
    assert current.mode == "auto"
    assert current.title == "existing Auto task"


@pytest.mark.asyncio
async def test_migration_import_cannot_refresh_existing_plan_carrier(
    client,
    session_factory,
):
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            id=7011,
            title="existing Plan carrier",
            description="immutable approval history",
            status="cancelled",
            mode="plan",
        )
        db.add(task)
        await db.commit()

    response = await _post_migration_import(client, {
        "id": 7011,
        "title": "replace Plan carrier",
        "description": "must not replace",
        "mode": "plan",
        "source_status": "cancelled",
        "source_incarnation_id": task.incarnation_id,
    })

    assert response.status_code == 409, response.text
    assert "Plan carriers are immutable" in response.text
    async with session_factory() as db:
        current = await db.get(Task, 7011)
    assert current.title == "existing Plan carrier"


@pytest.mark.asyncio
async def test_migration_import_rejects_codex_monitor_worker_copy(
    client,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.models.task import Task

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    response = await _post_migration_import(client, {
        "id": 7008,
        "title": "Worker Monitor remains closed",
        "description": "d",
        "provider": "codex",
        "enabled_skills": {"monitor": True},
        "source_status": "completed",
        "source_incarnation_id": "8" * 32,
        "user_skill_snapshots": [],
    })

    assert response.status_code == 400
    assert "does not support Skills: monitor" in response.text
    async with session_factory() as db:
        assert await db.get(Task, 7008) is None


@pytest.mark.asyncio
async def test_migration_import_refuses_active_existing_task(client, session_factory):
    """An import must never cancel a same-ID task which is already running."""
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            id=7002,
            title="Already running",
            description="d",
            status="in_progress",
        )
        db.add(task)
        await db.commit()

    resp = await _post_migration_import(client, {
        "id": 7002,
        "title": "Migrated",
        "description": "d",
        "source_incarnation_id": task.incarnation_id,
    })

    assert resp.status_code == 409
    async with session_factory() as db:
        task = await db.get(Task, 7002)
    assert task.status == "in_progress"
    assert task.title == "Already running"


@pytest.mark.asyncio
async def test_migration_import_existing_row_uses_full_generation_cas(
    client,
    session_factory,
    monkeypatch,
):
    """A same-status retry ABA cannot be overwritten by an old import."""

    import backend.api.tasks as task_api
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            id=7003,
            title="Current generation",
            description="d",
            status="cancelled",
            retry_count=4,
        )
        db.add(task)
        await db.commit()

    @asynccontextmanager
    async def replace_generation_after_snapshot(task_id):
        # Model a different Worker process committing after this request froze
        # its exact scalar predicates and ended its read transaction.  The
        # fresh-writer CAS must miss without SQLite BUSY_SNAPSHOT.
        async with session_factory() as competing_db:
            await competing_db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(retry_count=5)
            )
            await competing_db.commit()
        yield

    monkeypatch.setattr(
        task_api,
        "get_task_operation_lock",
        replace_generation_after_snapshot,
    )

    response = await _post_migration_import(client, {
        "id": 7003,
        "title": "Stale imported copy",
        "description": "d",
        "retry_count": 4,
        "source_incarnation_id": task.incarnation_id,
    })

    assert response.status_code == 409
    async with session_factory() as db:
        current = await db.get(Task, 7003)
    assert current.title == "Current generation"
    assert current.status == "cancelled"
    assert current.retry_count == 5


@pytest.mark.asyncio
async def test_migration_import_existing_row_cas_binds_observed_incarnation(
    client,
    session_factory,
    monkeypatch,
):
    """A stale legacy adoption cannot overwrite a concurrently bound mirror."""

    import backend.api.tasks as task_api
    from backend.models.task import Task
    from backend.services.skill_context import WORKER_MANAGED_TASK_METADATA_KEY
    from backend.services.task_creation import (
        SOURCE_TASK_INCARNATION_METADATA_KEY,
    )

    async with session_factory() as db:
        task = Task(
            id=7015,
            title="unbound legacy mirror",
            description="d",
            status="cancelled",
            incarnation_id="a" * 32,
        )
        db.add(task)
        await db.commit()

    winning_incarnation = "a" * 32

    @asynccontextmanager
    async def bind_after_snapshot(task_id):
        async with session_factory() as competing_db:
            await competing_db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(metadata_={
                    WORKER_MANAGED_TASK_METADATA_KEY: True,
                    SOURCE_TASK_INCARNATION_METADATA_KEY: winning_incarnation,
                })
            )
            await competing_db.commit()
        yield

    monkeypatch.setattr(
        task_api,
        "get_task_operation_lock",
        bind_after_snapshot,
    )

    response = await _post_migration_import(client, {
        "id": 7015,
        "title": "stale adoption",
        "description": "must lose",
        "source_status": "completed",
        "source_incarnation_id": "c" * 32,
    })

    assert response.status_code == 409, response.text
    async with session_factory() as db:
        current = await db.get(Task, 7015)
    assert current is not None
    assert current.title == "unbound legacy mirror"
    assert current.incarnation_id == winning_incarnation
    assert (
        current.metadata_[SOURCE_TASK_INCARNATION_METADATA_KEY]
        == winning_incarnation
    )


@pytest.mark.asyncio
async def test_migration_import_revalidates_capability_authority_after_fence(
    client,
    session_factory,
    monkeypatch,
):
    """A late local capability policy cannot be overwritten by an import."""

    import backend.api.tasks as task_api
    from backend.models.task import Task

    policy = {
        "version": 1,
        "max_invocations": 1,
        "capabilities": {"plan": 1},
    }
    async with session_factory() as db:
        task = Task(
            id=7016,
            title="local Auto task",
            description="d",
            status="cancelled",
            mode="auto",
        )
        db.add(task)
        await db.commit()

    @asynccontextmanager
    async def add_capability_after_snapshot(task_id):
        async with session_factory() as competing_db:
            await competing_db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(capability_policy=policy)
            )
            await competing_db.commit()
        yield

    monkeypatch.setattr(
        task_api,
        "get_task_operation_lock",
        add_capability_after_snapshot,
    )

    response = await _post_migration_import(client, {
        "id": 7016,
        "title": "stale Worker import",
        "description": "must not replace local authority",
        "source_status": "completed",
        "source_incarnation_id": "d" * 32,
    })

    assert response.status_code == 409, response.text
    assert "capability policy" in response.text
    async with session_factory() as db:
        current = await db.get(Task, 7016)
    assert current is not None
    assert current.title == "local Auto task"
    assert current.capability_policy == policy


@pytest.mark.asyncio
async def test_migration_import_rejects_turn_generation_only_aba(
    client,
    session_factory,
    monkeypatch,
):
    """An import snapshot cannot overwrite a newer logical turn."""

    import backend.api.tasks as task_api
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            id=7009,
            title="Current logical turn",
            description="d",
            status="cancelled",
            retry_count=4,
            turn_generation=9,
        )
        db.add(task)
        await db.commit()

    @asynccontextmanager
    async def replace_turn_after_snapshot(task_id):
        async with session_factory() as competing_db:
            await competing_db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(turn_generation=10)
            )
            await competing_db.commit()
        yield

    monkeypatch.setattr(
        task_api,
        "get_task_operation_lock",
        replace_turn_after_snapshot,
    )

    response = await _post_migration_import(client, {
        "id": 7009,
        "title": "Stale imported turn",
        "description": "d",
        "retry_count": 4,
        "turn_generation": 9,
        "source_incarnation_id": task.incarnation_id,
    })

    assert response.status_code == 409
    async with session_factory() as db:
        current = await db.get(Task, 7009)
    assert current.title == "Current logical turn"
    assert current.status == "cancelled"
    assert current.retry_count == 4
    assert current.turn_generation == 10


@pytest.mark.asyncio
async def test_migration_import_yields_to_receipt_after_snapshot(
    client,
    session_factory,
    monkeypatch,
):
    """A Worker receipt committed after validation owns the inert copy."""

    import backend.api.tasks as task_api
    from backend.models.task import Task
    from backend.tests.worker_termination_helpers import (
        persist_active_worker_receipt,
    )

    async with session_factory() as db:
        task = Task(
            id=7012,
            title="receipt-owned Worker copy",
            description="d",
            status="cancelled",
        )
        db.add(task)
        await db.commit()

    @asynccontextmanager
    async def receipt_wins_after_snapshot(task_id):
        await persist_active_worker_receipt(session_factory, task_id)
        yield

    monkeypatch.setattr(
        task_api,
        "get_task_operation_lock",
        receipt_wins_after_snapshot,
    )

    response = await _post_migration_import(client, {
        "id": 7012,
        "title": "stale Manager mirror",
        "description": "must not overwrite the receipt generation",
        "source_status": "completed",
        "source_incarnation_id": task.incarnation_id,
    })

    assert response.status_code == 409, response.text
    assert "termination receipt" in response.text
    async with session_factory() as db:
        current = await db.get(Task, 7012)
    assert current is not None
    assert current.title == "receipt-owned Worker copy"
    assert current.status == "cancelled"


@pytest.mark.asyncio
async def test_migration_import_commits_before_status_publication(
    client,
    session_factory,
):
    """A publication failure cannot roll back an already imported mirror."""

    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            id=7013,
            title="old Worker mirror",
            description="d",
            status="cancelled",
        )
        db.add(task)
        await db.commit()

    async def fail_after_verifying_commit(task_id, status):
        assert task_id == 7013
        assert status == "completed"
        async with session_factory() as verify_db:
            committed = await verify_db.get(Task, task_id)
            assert committed is not None
            assert committed.title == "durable imported mirror"
            assert committed.status == "completed"
        raise RuntimeError("publication failed after durable import")

    with (
        patch(
            "backend.services.task_events.broadcast_status_change",
            new=AsyncMock(side_effect=fail_after_verifying_commit),
        ),
        pytest.raises(RuntimeError, match="publication failed"),
    ):
        await _post_migration_import(client, {
            "id": 7013,
            "title": "durable imported mirror",
            "description": "d",
            "source_status": "completed",
            "source_incarnation_id": task.incarnation_id,
        })

    async with session_factory() as db:
        current = await db.get(Task, 7013)
    assert current is not None
    assert current.title == "durable imported mirror"
    assert current.status == "completed"


@pytest.mark.asyncio
async def test_migration_import_publication_yields_to_post_commit_receipt(
    client,
    session_factory,
    monkeypatch,
):
    """A receipt admitted after import commit suppresses its old status event."""

    from backend.models.task import Task
    from backend.tests.worker_termination_helpers import (
        persist_active_worker_receipt,
    )

    async with session_factory() as db:
        task = Task(
            id=7014,
            title="old Worker mirror",
            description="d",
            status="cancelled",
        )
        db.add(task)
        await db.commit()

    publication_waiting = asyncio.Event()
    release_publication = asyncio.Event()
    original_execute = AsyncSession.execute
    publication_paused = False

    async def pause_publication_guard(self, statement, *args, **kwargs):
        nonlocal publication_paused
        values = getattr(statement, "_values", None)
        value_keys = {
            getattr(column, "key", None)
            for column in values or ()
        }
        table = getattr(statement, "table", None)
        if (
            not publication_paused
            and getattr(table, "name", None) == Task.__tablename__
            and value_keys == {"status"}
        ):
            publication_paused = True
            publication_waiting.set()
            await release_publication.wait()
        return await original_execute(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", pause_publication_guard)
    with patch(
        "backend.services.task_events.broadcast_status_change",
        new_callable=AsyncMock,
    ) as publish:
        request_task = asyncio.create_task(
            _post_migration_import(client, {
                "id": 7014,
                "title": "durable imported mirror",
                "description": "d",
                "source_status": "completed",
                "source_incarnation_id": task.incarnation_id,
            })
        )
        await asyncio.wait_for(publication_waiting.wait(), timeout=2)
        async with session_factory() as db:
            committed = await db.get(Task, 7014)
            assert committed is not None
            assert committed.title == "durable imported mirror"
            assert committed.status == "completed"

        await persist_active_worker_receipt(session_factory, 7014)
        release_publication.set()
        response = await asyncio.wait_for(request_task, timeout=2)

    assert response.status_code == 201, response.text
    assert response.json()["title"] == "durable imported mirror"
    assert response.json()["status"] == "completed"
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_task_with_project_id(client, session_factory):
    from backend.models.project import Project

    async with session_factory() as db:
        project = Project(
            name="task-project",
            local_path="/tmp/task-project",
            status="ready",
        )
        db.add(project)
        await db.commit()
        await db.refresh(project)
        project_id = project.id

    resp = await client.post("/api/tasks", json={
        "title": "Test",
        "description": "Do something",
        "project_id": project_id,
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["project_id"] == project_id


@pytest.mark.asyncio
async def test_list_tasks(client):
    await client.post("/api/tasks", json={
        "title": "A", "description": "d", "target_repo": "/tmp",
    })
    await client.post("/api/tasks", json={
        "title": "B", "description": "d", "target_repo": "/tmp",
    })
    resp = await client.get("/api/tasks")
    assert resp.status_code == 200
    assert len(resp.json()) == 2


@pytest.mark.asyncio
async def test_get_task(client):
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    resp = await client.get(f"/api/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["title"] == "T"


@pytest.mark.asyncio
async def test_get_task_not_found(client):
    resp = await client.get("/api/tasks/9999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_worker_termination_receipt_returns_exact_task_not_found(
    client,
):
    operation_id = "a" * 32
    task_id = 987654

    response = await client.get(
        f"/api/tasks/{task_id}/termination-receipts/{operation_id}"
    )

    assert response.status_code == 200, response.text
    assert response.json() == {
        "version": 2,
        "task_id": task_id,
        "operation_id": operation_id,
        "status": "task_not_found",
    }


@pytest.mark.asyncio
async def test_get_worker_termination_receipt_fails_closed_on_corrupt_storage(
    client,
    session_factory,
):
    from backend.models.task import Task
    from backend.services.worker_task_termination import (
        WorkerTaskTerminationReceipt,
        canonical_json_digest,
        stage_worker_receipt,
    )

    operation_id = "b" * 32
    async with session_factory() as db:
        task = Task(title="corrupt Worker receipt", status="pending")
        db.add(task)
        await db.commit()
        task_id = task.id
        payload = {
            "version": 2,
            "operation_id": operation_id,
            "task_id": task_id,
            "operation": "cancel",
            "manager_worker_id": 17,
            "expected_remote": {
                "status": "pending",
                "retry_count": 0,
                "turn_generation": 0,
            },
            "manager_handoff": None,
        }
        await stage_worker_receipt(
            db,
            task_id=task_id,
            operation_id=operation_id,
            operation="cancel",
            request_payload=payload,
            request_digest=canonical_json_digest(payload),
        )
        receipt = await db.get(WorkerTaskTerminationReceipt, operation_id)
        receipt.request_payload = {**payload, "unexpected": True}
        await db.commit()

    response = await client.get(
        f"/api/tasks/{task_id}/termination-receipts/{operation_id}"
    )

    assert response.status_code == 409, response.text
    assert "receipt" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_delete_task(client):
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    resp = await client.delete(f"/api/tasks/{task_id}")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_cancel_task(client):
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    resp = await client.post(f"/api/tasks/{task_id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"


@pytest.mark.asyncio
async def test_cancel_owner_cascades_to_durable_browser_child(
    client,
    session_factory,
):
    from backend.models.task import Task
    from backend.models.test_harness import (
        TestHarnessChildBinding,
        TestHarnessRun,
    )

    create_resp = await client.post(
        "/api/tasks",
        json={"title": "Owner with browser run", "description": "d"},
    )
    task_id = create_resp.json()["id"]
    run_id = "a" * 32
    async with session_factory() as db:
        owner = await db.get(Task, task_id)
        assert owner is not None
        child = Task(
            title="Isolated Browser Agent",
            description="black-box review",
            status="pending_activation",
            provider="codex",
            model="gpt-5.6-sol",
            codex_service_tier="default",
            effort_level="high",
            archived=True,
            metadata_={
                "isolated_browser_agent": True,
                "test_harness_run_id": run_id,
                "test_harness_parent_task_id": task_id,
                "browser_review_job_id": "browser-cascade-job",
            },
        )
        db.add(child)
        await db.flush()
        db.add(
            TestHarnessRun(
                id=run_id,
                task_id=task_id,
                owner_task_incarnation_id=owner.incarnation_id,
                owner_task_retry_count=owner.retry_count,
                owner_task_turn_generation=owner.turn_generation,
                owner_task_status=owner.status,
                agent_task_id=child.id,
                browser_review_job_id="browser-cascade-job",
                target_kind="fixed_url",
                target_spec={"url": "https://example.com"},
                test_plan={"objective": "Review the page"},
                runtime_config={"provider": "codex"},
                request_fingerprint="b" * 64,
                root_run_id=run_id,
                status="running",
                stage="waiting_for_agent",
            )
        )
        db.add(
            TestHarnessChildBinding(
                id="c" * 32,
                harness_run_id=run_id,
                owner_task_id=task_id,
                owner_task_incarnation_id=owner.incarnation_id,
                owner_task_retry_count=owner.retry_count,
                owner_task_turn_generation=owner.turn_generation,
                owner_task_status=owner.status,
                child_task_id=child.id,
                child_task_incarnation_id=child.incarnation_id,
                browser_review_job_id="browser-cascade-job",
                state="reserved",
            )
        )
        await db.commit()
        child_id = child.id

    response = await client.post(f"/api/tasks/{task_id}/cancel")

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"
    async with session_factory() as db:
        owner = await db.get(Task, task_id)
        child = await db.get(Task, child_id)
        run = await db.get(TestHarnessRun, run_id)
        binding = await db.get(TestHarnessChildBinding, "c" * 32)
        assert owner.status == "cancelled"
        assert child.status == "cancelled"
        assert run.status == "cancelled"
        assert binding.state == "stopped"


@pytest.mark.asyncio
async def test_retry_task(client):
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    # Manual retry only accepts terminal generations.
    cancelled = await client.post(f"/api/tasks/{task_id}/cancel")
    assert cancelled.status_code == 200
    resp = await client.post(f"/api/tasks/{task_id}/retry")
    assert resp.status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize("remote", [False, True], ids=["local", "worker"])
async def test_retry_rejects_group_revoked_at_final_effect_fence(
    secured_client,
    monkeypatch,
    remote,
):
    """Retry never publishes G+1 or a Worker outbox from stale group access."""

    from backend.models.project import Project
    from backend.models.task import Task
    from backend.models.worker import Worker
    from backend.services.test_harness import test_harness_service
    import backend.main as main_module

    client, session_factory = secured_client
    monkeypatch.setattr(test_harness_service, "db_factory", session_factory)
    monkeypatch.setattr(
        test_harness_service.child_service,
        "db_factory",
        session_factory,
    )
    member_id, member_token = await _create_user(
        session_factory,
        email=f"retry-effect-{remote}@example.com",
        role="member",
    )
    async with session_factory() as db:
        project = Project(
            name=f"retry-effect-{remote}-project",
            status="ready",
        )
        db.add(project)
        worker = None
        if remote:
            worker = Worker(
                name="retry-effect-worker",
                status="ready",
                private_ip="10.0.0.77",
                auth_token="worker-token",
            )
            db.add(worker)
        await db.flush()
        task = Task(
            title="retry effect fence",
            description="membership disappears at final retry admission",
            project_id=project.id,
            created_by=999,
            status="failed",
            provider="codex",
            model="gpt-5.6-sol",
            codex_service_tier="default",
            worker_id=worker.id if worker is not None else None,
            metadata_=(
                {"ccm_worker_remote_materialized_v1": True}
                if remote
                else None
            ),
        )
        db.add(task)
        await db.commit()
        project_id = project.id
        task_id = task.id
    await grant_group_project_access(
        session_factory,
        project_id=project_id,
        user_id=member_id,
    )
    # The public operation-lock check is call one. The boundary immediately
    # before local G+1 or the remote prepared marker is call two.
    fence = revoke_group_membership_at_effect_fence(
        monkeypatch,
        on_call=2,
    )
    proxy = MagicMock()
    proxy.require_ready_worker = AsyncMock(return_value=worker)
    proxy.require_worker_delegated_principal_support = AsyncMock()
    proxy.require_worker_manual_retry_support = AsyncMock()
    proxy.sync_task_skill_selection = AsyncMock()
    proxy.proxy_to_worker = AsyncMock(
        return_value={
            "id": task_id,
            "status": "failed",
            "worker_id": None,
            "shared_from_id": None,
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "default",
            "pending": None,
        }
    )

    with patch.object(main_module, "worker_proxy", proxy):
        response = await client.post(
            f"/api/tasks/{task_id}/retry",
            headers={"Authorization": f"Bearer {member_token}"},
        )

    assert response.status_code == 403, response.text
    assert fence == {"calls": 2, "revoked": True}
    if remote:
        assert proxy.proxy_to_worker.await_count == 1
        assert proxy.proxy_to_worker.await_args.args[1] == "GET"
        proxy.sync_task_skill_selection.assert_awaited_once()
    else:
        proxy.proxy_to_worker.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "failed"
        assert current.retry_count == 0
        assert (
            "ccm_worker_manual_retry_receipt_v1" not in (current.metadata_ or {})
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "initial_values"),
    (
        pytest.param("star", {"starred": False}, id="star"),
        pytest.param("read", {"has_unread": True}, id="read"),
        pytest.param("unread", {"has_unread": False}, id="unread"),
        pytest.param("archive", {"archived": False}, id="archive"),
    ),
)
async def test_task_flag_controls_reject_group_revoked_at_final_effect_fence(
    secured_client,
    monkeypatch,
    action,
    initial_values,
):
    """Cosmetic Task writes cannot cross a winning Project ACL revoke."""

    from backend.models.task import Task

    client, session_factory = secured_client
    member_id, member_token = await _create_user(
        session_factory,
        email=f"task-flag-effect-{action}@example.com",
        role="member",
    )
    project_id, task_id = await _seed_group_project_control_task(
        session_factory,
        member_id=member_id,
        **initial_values,
    )
    await grant_group_project_access(
        session_factory,
        project_id=project_id,
        user_id=member_id,
    )
    async with session_factory() as db:
        before = await db.get(Task, task_id)
        before_values = (
            before.starred,
            before.has_unread,
            before.archived,
            before.sort_order,
        )

    fence = revoke_group_membership_at_effect_fence(monkeypatch)
    response = await client.post(
        f"/api/tasks/{task_id}/{action}",
        headers={"Authorization": f"Bearer {member_token}"},
    )

    assert response.status_code == 403, response.text
    assert fence == {"calls": 1, "revoked": True}
    async with session_factory() as db:
        current = await db.get(Task, task_id)
        assert current is not None
        assert (
            current.starred,
            current.has_unread,
            current.archived,
            current.sort_order,
        ) == before_values


@pytest.mark.asyncio
async def test_star_rejects_admin_demoted_before_final_user_fence(
    secured_client,
    monkeypatch,
):
    """A stale JWT admin role cannot cross the final User writer fence."""

    from backend.api import tasks as tasks_api
    from backend.models.task import Task
    from backend.models.user import User

    client, session_factory = secured_client
    admin_id, admin_token = await _create_user(
        session_factory,
        email="task-star-demoted-admin@example.com",
        role="admin",
    )
    async with session_factory() as db:
        task = Task(
            title="Demoted admin cannot star",
            description="d",
            created_by=999,
            status="completed",
            starred=False,
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    original_fence = tasks_api.lock_task_effect_access
    demoted = False

    async def demote_before_fence(*args, **kwargs):
        nonlocal demoted
        if not demoted:
            async with session_factory() as demoter:
                changed = await demoter.execute(
                    update(User)
                    .where(User.id == admin_id, User.role == "admin")
                    .values(role="member")
                )
                assert changed.rowcount == 1
                await demoter.commit()
            demoted = True
        return await original_fence(*args, **kwargs)

    monkeypatch.setattr(
        tasks_api,
        "lock_task_effect_access",
        demote_before_fence,
    )

    response = await client.post(
        f"/api/tasks/{task_id}/star",
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert response.status_code == 409, response.text
    assert demoted is True
    async with session_factory() as db:
        current = await db.get(Task, task_id)
        actor = await db.get(User, admin_id)
        assert current.starred is False
        assert actor.role == "member"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "action",
    (
        pytest.param("stop-session", id="stop-session"),
        pytest.param("cancel", id="cancel"),
        pytest.param("delete", id="delete"),
    ),
)
async def test_local_task_controls_reject_group_revoked_before_effect_gate(
    secured_client,
    monkeypatch,
    action,
):
    """Local terminal/delete effects require fresh group authority."""

    from backend.models.task import Task
    from backend.services.test_harness_owner_fence import (
        TEST_HARNESS_TERMINAL_GATE_KEY,
    )

    client, session_factory = secured_client
    member_id, member_token = await _create_user(
        session_factory,
        email=f"local-control-effect-{action}@example.com",
        role="member",
    )
    project_id, task_id = await _seed_group_project_control_task(
        session_factory,
        member_id=member_id,
    )
    await grant_group_project_access(
        session_factory,
        project_id=project_id,
        user_id=member_id,
    )
    fence = revoke_group_membership_at_effect_fence(monkeypatch)

    if action == "delete":
        response = await client.delete(
            f"/api/tasks/{task_id}",
            headers={"Authorization": f"Bearer {member_token}"},
        )
    else:
        response = await client.post(
            f"/api/tasks/{task_id}/{action}",
            headers={"Authorization": f"Bearer {member_token}"},
        )

    assert response.status_code == 403, response.text
    assert fence == {"calls": 1, "revoked": True}
    async with session_factory() as db:
        current = await db.get(Task, task_id)
        assert current is not None
        assert current.status == "completed"
        assert TEST_HARNESS_TERMINAL_GATE_KEY not in (current.metadata_ or {})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "action",
    (
        pytest.param("stop-session", id="stop-session"),
        pytest.param("cancel", id="cancel"),
        pytest.param("delete", id="delete"),
    ),
)
async def test_worker_task_controls_reject_group_revoked_before_receipt(
    secured_client,
    monkeypatch,
    action,
):
    """No Worker request or durable receipt may follow a winning revoke."""

    import backend.main as main_module
    from backend.models.task import Task
    from backend.models.worker_task_termination import (
        WorkerTaskTerminationReceipt,
    )

    client, session_factory = secured_client
    member_id, member_token = await _create_user(
        session_factory,
        email=f"worker-control-effect-{action}@example.com",
        role="member",
    )
    project_id, task_id = await _seed_group_project_control_task(
        session_factory,
        member_id=member_id,
        worker=True,
    )
    await grant_group_project_access(
        session_factory,
        project_id=project_id,
        user_id=member_id,
    )
    fence = revoke_group_membership_at_effect_fence(monkeypatch)
    proxy = MagicMock()
    proxy.task_operation_lock = MagicMock(return_value=asyncio.Lock())
    proxy.proxy_to_worker = AsyncMock()
    proxy.require_task_plan_delete_protocol = AsyncMock()
    proxy.relay = MagicMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(main_module, "task_migrator", None)

    if action == "delete":
        response = await client.delete(
            f"/api/tasks/{task_id}",
            headers={"Authorization": f"Bearer {member_token}"},
        )
    else:
        response = await client.post(
            f"/api/tasks/{task_id}/{action}",
            headers={"Authorization": f"Bearer {member_token}"},
        )

    assert response.status_code == 403, response.text
    assert fence == {"calls": 1, "revoked": True}
    proxy.proxy_to_worker.assert_not_awaited()
    proxy.require_task_plan_delete_protocol.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task_id)
        assert current is not None
        assert current.status == "completed"
        assert await db.scalar(
            select(func.count(WorkerTaskTerminationReceipt.operation_id)).where(
                WorkerTaskTerminationReceipt.task_id == task_id
            )
        ) == 0


@pytest.mark.asyncio
async def test_completed_stop_400_settles_gate_before_later_delete(
    client,
    session_factory,
):
    """A proven no-process stop must not strand the terminal Task forever."""

    from backend.models.task import Task
    from backend.services.test_harness_owner_fence import (
        TEST_HARNESS_TERMINAL_GATE_KEY,
    )

    async with session_factory() as db:
        task = Task(
            title="No-process stop may be followed by delete",
            description="d",
            status="completed",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    stopped = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert stopped.status_code == 400, stopped.text
    assert stopped.json()["detail"] == "No running session found for this task"
    async with session_factory() as db:
        current = await db.get(Task, task_id)
        gate = (current.metadata_ or {}).get(TEST_HARNESS_TERMINAL_GATE_KEY)
        assert gate["task_control_effect"] == "stop_session"
        assert gate["task_control_effect_state"] == "settled"

    deleted = await client.delete(f"/api/tasks/{task_id}")

    assert deleted.status_code == 200, deleted.text
    async with session_factory() as db:
        assert await db.get(Task, task_id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("initial_status", "expected_cancel_status"),
    (("completed", 400), ("cancelled", 200)),
)
async def test_terminal_cancel_settles_gate_before_later_delete(
    client,
    session_factory,
    initial_status,
    expected_cancel_status,
):
    """Known no-op and idempotent cancellation cannot strand the Task."""

    from backend.models.task import Task
    from backend.services.test_harness_owner_fence import (
        TEST_HARNESS_TERMINAL_GATE_KEY,
    )

    async with session_factory() as db:
        task = Task(
            title=f"{initial_status} cancel may be followed by delete",
            description="d",
            status=initial_status,
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    cancelled = await client.post(f"/api/tasks/{task_id}/cancel")

    assert cancelled.status_code == expected_cancel_status, cancelled.text
    async with session_factory() as db:
        current = await db.get(Task, task_id)
        gate = (current.metadata_ or {}).get(TEST_HARNESS_TERMINAL_GATE_KEY)
        assert current.status == initial_status
        assert gate["task_control_effect"] == "cancel"
        assert gate["task_control_effect_state"] == "settled"

    deleted = await client.delete(f"/api/tasks/{task_id}")

    assert deleted.status_code == 200, deleted.text
    async with session_factory() as db:
        assert await db.get(Task, task_id) is None


@pytest.mark.asyncio
async def test_star_rejects_real_wal_group_revoke_before_final_effect_fence(
    tmp_path,
    monkeypatch,
):
    """A separately committed WAL revocation wins before the final writer."""

    from types import SimpleNamespace

    from fastapi import HTTPException
    from sqlalchemy import delete
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from backend.api import tasks as tasks_api
    from backend.database import Base
    from backend.models.project import Project
    from backend.models.task import Task
    from backend.models.team_share import TeamProjectShare
    from backend.models.user import User
    from backend.models.user_group import UserGroup, UserGroupMember
    from backend.services.task_queue import TaskQueue

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'task-star-acl-race.db'}",
        connect_args={"timeout": 2},
    )
    release_fence = asyncio.Event()
    request_task = None
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.exec_driver_sql(
                "PRAGMA journal_mode=WAL"
            )
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        async with sessions() as setup:
            member = User(
                email="task-star-wal-member@example.com",
                name="task-star-wal-member",
                password_hash="not-used",
                role="member",
                is_active=True,
            )
            group = UserGroup(name="task-star-wal-group", created_by=999)
            project = Project(name="task-star-wal-project", status="ready")
            setup.add_all([member, group, project])
            await setup.flush()
            membership = UserGroupMember(
                group_id=group.id,
                user_id=member.id,
            )
            task = Task(
                title="Task star WAL authority",
                description="revocation commits before final fence",
                project_id=project.id,
                created_by=999,
                status="completed",
                starred=False,
            )
            setup.add_all(
                [
                    membership,
                    task,
                    TeamProjectShare(
                        project_id=project.id,
                        target_type="group",
                        target_id=group.id,
                        shared_by=999,
                    ),
                ]
            )
            await setup.commit()
            member_id = member.id
            membership_id = membership.id
            task_id = task.id

        request = SimpleNamespace(
            state=SimpleNamespace(
                user_id=member_id,
                user_role="member",
                auth_type="jwt",
            ),
            headers={},
        )
        fence_entered = asyncio.Event()
        original_fence = tasks_api.lock_task_effect_access

        async def blocked_fence(*args, **kwargs):
            fence_entered.set()
            await release_fence.wait()
            return await original_fence(*args, **kwargs)

        monkeypatch.setattr(
            tasks_api,
            "lock_task_effect_access",
            blocked_fence,
        )

        async def star_task():
            async with sessions() as effect_db:
                return await tasks_api.star_task(
                    task_id,
                    request,
                    TaskQueue(effect_db),
                    effect_db,
                )

        request_task = asyncio.create_task(star_task())
        await asyncio.wait_for(fence_entered.wait(), timeout=2)
        async with sessions() as revoker:
            revoked = await revoker.execute(
                delete(UserGroupMember).where(
                    UserGroupMember.id == membership_id
                )
            )
            assert revoked.rowcount == 1
            await revoker.commit()
        release_fence.set()

        with pytest.raises(HTTPException) as rejected:
            await asyncio.wait_for(request_task, timeout=2)
        assert rejected.value.status_code == 403
        async with sessions() as verify:
            current = await verify.get(Task, task_id)
            assert current is not None
            assert current.starred is False
            assert await verify.get(UserGroupMember, membership_id) is None
    finally:
        release_fence.set()
        if request_task is not None and not request_task.done():
            request_task.cancel()
            await asyncio.gather(request_task, return_exceptions=True)
        await engine.dispose()


@pytest.mark.asyncio
async def test_manual_retry_freezes_the_authenticated_retry_principal(
    client,
    session_factory,
):
    """Retry is a new admission and must not inherit stale Task authority."""

    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            title="Retry principal",
            description="d",
            status="failed",
            execution_user_id=None,
            execution_user_role="member",
            execution_mode="sandbox",
            execution_principal_kind="system",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    response = await client.post(f"/api/tasks/{task_id}/retry")

    assert response.status_code == 200, response.text
    async with session_factory() as db:
        retried = await db.get(Task, task_id)
    assert retried.status == "pending"
    assert retried.retry_count == 1
    assert (
        retried.execution_user_id,
        retried.execution_user_role,
        retried.execution_mode,
        retried.execution_principal_kind,
    ) == (None, "super_admin", "unrestricted", "deployment_token")


@pytest.mark.asyncio
async def test_retry_fences_and_cancels_concurrent_harness_owner(
    client,
    session_factory,
    monkeypatch,
):
    """A retry cannot strand a Run or child on the previous generation."""

    from backend.models.task import Task
    from backend.models.test_harness import TestHarnessRun
    from backend.config import settings
    from backend.services.test_harness import test_harness_service
    from backend.services.test_harness_contracts import TestHarnessSpec

    monkeypatch.setattr(settings, "auth_token", "harness-race-secret")
    client.headers["Authorization"] = "Bearer harness-race-secret"

    async with session_factory() as db:
        owner = Task(
            title="Retry Harness owner",
            description="race retry against run materialization",
            status="failed",
            provider="codex",
            model="gpt-5.6-sol",
        )
        db.add(owner)
        await db.commit()
        task_id = owner.id

    entered_create = asyncio.Event()
    release_create = asyncio.Event()
    original_create = test_harness_service._create_run

    async def delayed_create(**kwargs):
        entered_create.set()
        await release_create.wait()
        return await original_create(**kwargs)

    monkeypatch.setattr(test_harness_service, "_create_run", delayed_create)
    start_request = asyncio.create_task(
        test_harness_service.start_task_run(
            task_id=task_id,
            spec=TestHarnessSpec(
                target_kind="fixed_url",
                target={"url": "https://example.com"},
                goal="Exercise retry owner fencing",
            ),
        )
    )
    await asyncio.wait_for(entered_create.wait(), timeout=1)

    retry_request = asyncio.create_task(client.post(f"/api/tasks/{task_id}/retry"))
    await asyncio.sleep(0)
    assert not retry_request.done()

    release_create.set()
    run = await asyncio.wait_for(start_request, timeout=1)
    response = await asyncio.wait_for(retry_request, timeout=2)

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "pending"
    assert response.json()["retry_count"] == 1
    async with session_factory() as db:
        owner = await db.get(Task, task_id)
        persisted_run = await db.get(TestHarnessRun, run.id)
        assert owner is not None
        assert owner.status == "pending"
        assert owner.retry_count == 1
        assert persisted_run is not None
        assert persisted_run.status == "cancelled"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "plan_approved"),
    [("completed", True), ("cancelled", False)],
)
async def test_manual_retry_rejects_terminal_plan_tasks(
    client,
    session_factory,
    status,
    plan_approved,
):
    """Plan decisions are immutable; revision owns another planning run."""

    from backend.models.task import Task

    async with session_factory() as db:
        plan = Task(
            title="Terminal Plan",
            description="Plan this",
            status=status,
            mode="plan",
            plan_approved=plan_approved,
            plan_content="A reviewed proposal",
        )
        db.add(plan)
        await db.commit()
        task_id = plan.id

    response = await client.post(f"/api/tasks/{task_id}/retry")

    assert response.status_code == 409
    assert "Plan Tasks cannot be retried" in response.json()["detail"]
    async with session_factory() as db:
        plan = await db.get(Task, task_id)
    assert plan.status == status
    assert plan.retry_count == 0
    assert plan.plan_approved is plan_approved


@pytest.mark.asyncio
async def test_retry_rejects_stale_fast_view_before_mutating_generation(
    client,
    session_factory,
):
    """A stale Fast retry must not enqueue the Task as Standard."""
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Stale retry",
        "description": "d",
        "target_repo": "/tmp",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "default",
    })
    task_id = create_resp.json()["id"]
    cancelled = await client.post(f"/api/tasks/{task_id}/cancel")
    assert cancelled.status_code == 200

    response = await client.post(
        f"/api/tasks/{task_id}/retry",
        json={
            "expected_routing": {
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "codex_service_tier": "priority",
            },
        },
    )

    assert response.status_code == 409
    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "cancelled"
    assert task.retry_count == 0


@pytest.mark.parametrize("status", ["pending", "in_progress", "executing", "migrating"])
@pytest.mark.asyncio
async def test_manual_retry_rejects_non_terminal_status(
    client,
    session_factory,
    status,
):
    """Manual retry cannot steal active, queued, or migrating work."""

    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Not retryable",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status=status)
        )
        await db.commit()

    response = await client.post(f"/api/tasks/{task_id}/retry")

    assert response.status_code == 409
    assert status in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == status
    assert task.retry_count == 0


@pytest.mark.asyncio
async def test_retry_rejects_orphan_pid_that_may_be_alive(
    client, session_factory,
):
    """Manual retry must not erase an unknown live process owner."""
    from backend.models.instance import Instance
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            title="orphan-live",
            description="d",
            status="failed",
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="orphan-live-slot",
            status="error",
            pid=43210,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id, instance_id = task.id, instance.id

    with patch("backend.services.process_identity.os.kill", return_value=None):
        response = await client.post(f"/api/tasks/{task_id}/retry")

    assert response.status_code == 409
    assert "still alive" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "failed"
        assert task.instance_id == instance_id
        assert instance.pid == 43210
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_retry_allows_task_whose_pid_is_from_a_previous_boot(
    client, session_factory,
):
    """The reported dead end: retry must not be blocked by a reused PID.

    Reproduces screenshot #1004. The recorded boot id differs from the current
    one, which proves the owning generation died with the previous boot even
    though an unrelated process now answers to that PID number.
    """
    import os as _os

    from backend.models.instance import Instance
    from backend.models.task import Task
    from backend.services.process_identity import (
        ProcessIdentity,
        encode_process_identity,
    )

    live_pid = _os.getpid()
    async with session_factory() as db:
        task = Task(
            title="orphan-previous-boot",
            description="d",
            status="failed",
            error_message=(
                f"Unmanaged process PID {live_pid} may still be running after "
                "manager restart; automatic retry was blocked to prevent "
                "duplicate execution"
            ),
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="orphan-previous-boot-slot",
            status="error",
            pid=live_pid,
            process_identity=encode_process_identity(
                ProcessIdentity(
                    pid=live_pid,
                    start_ticks=999,
                    boot_id="22222222-2222-4222-8222-222222222222",
                )
            ),
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id, instance_id = task.id, instance.id

    response = await client.post(f"/api/tasks/{task_id}/retry")

    assert response.status_code == 200, response.text
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "pending"
        assert task.instance_id is None
        # The freed slot must stop consuming instance capacity.
        assert instance.pid is None
        assert instance.process_identity is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_retry_rejects_orphan_with_exactly_matching_live_identity(
    client, session_factory,
):
    """The safety property: a provably live generation still blocks retry."""
    import os as _os

    from backend.models.instance import Instance
    from backend.models.task import Task
    from backend.services import process_identity as pi
    from backend.services.process_identity import encode_process_identity

    live_pid = _os.getpid()
    async with session_factory() as db:
        task = Task(title="orphan-live-identity", description="d", status="failed")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="orphan-live-identity-slot",
            status="error",
            pid=live_pid,
            process_identity=encode_process_identity(
                pi.read_process_identity(live_pid)
            ),
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id, instance_id = task.id, instance.id

    response = await client.post(f"/api/tasks/{task_id}/retry")

    assert response.status_code == 409
    assert "still alive" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "failed"
        assert (await db.get(Instance, instance_id)).pid == live_pid


@pytest.mark.asyncio
async def test_retry_reconciles_dead_orphan_before_releasing_task(
    client, session_factory,
):
    """A definitively dead PID is detached before the task becomes pending."""
    from backend.models.instance import Instance
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            title="orphan-dead",
            description="d",
            status="failed",
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="orphan-dead-slot",
            status="error",
            pid=54321,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id, instance_id = task.id, instance.id

    with patch(
        "backend.services.process_identity.os.kill",
        side_effect=ProcessLookupError,
    ):
        response = await client.post(f"/api/tasks/{task_id}/retry")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "pending"
    # Process/slot linkage is internal Worker/runtime state and is deliberately
    # absent from the human Task projection.  Verify the detach in the
    # authoritative database instead of reopening that field publicly.
    assert "instance_id" not in response.json()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.instance_id is None
        instance = await db.get(Instance, instance_id)
        assert instance.status == "error"
        assert instance.pid is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_retry_does_not_clear_owner_that_changes_while_waiting(
    client, session_factory,
):
    """A retry waiting on an old slot lock cannot erase a newer orphan link."""
    from backend.models.instance import Instance
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(title="retry-owner-race", description="d", status="failed")
        old_instance = Instance(name="old-owner", status="error")
        new_instance = Instance(name="new-owner", status="error", pid=65432)
        db.add_all([task, old_instance, new_instance])
        await db.flush()
        task.instance_id = old_instance.id
        old_instance.current_task_id = task.id
        await db.commit()
        task_id = task.id
        old_instance_id = old_instance.id
        new_instance_id = new_instance.id

    old_lock = asyncio.Lock()
    reached_lock = asyncio.Event()
    await old_lock.acquire()
    manager = MagicMock()
    manager.processes = {}

    def lifecycle_lock(instance_id):
        assert instance_id == old_instance_id
        reached_lock.set()
        return old_lock

    manager._instance_lifecycle_lock.side_effect = lifecycle_lock
    try:
        with patch("backend.main.instance_manager", manager):
            request = asyncio.create_task(
                client.post(f"/api/tasks/{task_id}/retry")
            )
            await asyncio.wait_for(reached_lock.wait(), timeout=5)
            async with session_factory() as db:
                task = await db.get(Task, task_id)
                old_instance = await db.get(Instance, old_instance_id)
                new_instance = await db.get(Instance, new_instance_id)
                old_instance.current_task_id = None
                task.instance_id = new_instance_id
                new_instance.current_task_id = task_id
                await db.commit()
            old_lock.release()
            response = await asyncio.wait_for(request, timeout=5)
    finally:
        if old_lock.locked():
            old_lock.release()

    assert response.status_code == 409
    assert "ownership changed" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        new_instance = await db.get(Instance, new_instance_id)
        assert task.status == "failed"
        assert task.instance_id == new_instance_id
        assert new_instance.pid == 65432
        assert new_instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_retry_checks_reverse_owner_when_task_side_owner_is_null(
    client,
    session_factory,
):
    """A one-sided reverse owner is still process evidence and blocks retry."""

    from backend.models.instance import Instance
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            title="reverse-only-live-owner",
            description="d",
            status="failed",
            instance_id=None,
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="reverse-only-slot",
            status="error",
            pid=76543,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.commit()
        task_id = task.id
        instance_id = instance.id

    with patch("backend.services.process_identity.os.kill", return_value=None):
        response = await client.post(f"/api/tasks/{task_id}/retry")

    assert response.status_code == 409
    assert "still alive" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "failed"
        assert task.instance_id is None
        assert instance.pid == 76543
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_retry_uses_full_manager_generation_evidence(
    client,
    session_factory,
):
    """A reaped parent is insufficient while descendants/consumer remain."""

    from backend.models.instance import Instance
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            title="managed-descendant-owner",
            description="d",
            status="failed",
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="managed-descendant-slot",
            status="error",
            pid=None,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id = task.id
        instance_id = instance.id

    manager = MagicMock()
    manager._instance_lifecycle_lock.return_value = asyncio.Lock()
    manager.is_running.return_value = True
    # The old parent object looks terminal; is_running additionally covers its
    # process group, container supervisor and output consumer generation.
    manager.processes = {
        instance_id: MagicMock(returncode=0),
    }
    with patch("backend.main.instance_manager", manager):
        response = await client.post(f"/api/tasks/{task_id}/retry")

    assert response.status_code == 409
    assert "live managed generation" in response.json()["detail"]
    manager.is_running.assert_called_with(instance_id)
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "failed"
        assert task.instance_id == instance_id
        assert instance.current_task_id == task_id


# === New tests (Phase 2 gaps) ===


@pytest.mark.asyncio
async def test_update_task(client):
    """PUT /api/tasks/{id} updates task fields."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Original", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    resp = await client.put(f"/api/tasks/{task_id}", json={"title": "Updated"})
    assert resp.status_code == 200
    assert resp.json()["title"] == "Updated"


@pytest.mark.asyncio
async def test_admin_project_move_binds_task_to_project_workspace(
    client,
    session_factory,
    tmp_path,
):
    """Even an administrator cannot attach an arbitrary host path to a Project."""

    from backend.models.project import Project
    from backend.models.task import Task

    source_root = tmp_path / "source-project"
    target_root = tmp_path / "target-project"
    source_root.mkdir()
    target_root.mkdir()
    async with session_factory() as db:
        source = Project(
            name="source-project-workspace",
            status="ready",
            local_path=str(source_root),
        )
        target = Project(
            name="target-project-workspace",
            status="ready",
            local_path=str(target_root),
        )
        db.add_all([source, target])
        await db.flush()
        task = Task(
            title="project workspace authority",
            description="d",
            project_id=source.id,
            target_repo=str(source_root),
        )
        db.add(task)
        await db.commit()
        task_id, target_project_id = task.id, target.id

    forged = await client.put(
        f"/api/tasks/{task_id}",
        json={"target_repo": str(tmp_path / "forged")},
    )
    moved = await client.put(
        f"/api/tasks/{task_id}",
        json={"project_id": target_project_id},
    )

    assert forged.status_code == 400
    assert moved.status_code == 200, moved.text
    assert moved.json()["project_id"] == target_project_id
    assert moved.json()["target_repo"] == str(target_root)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "suffix", "payload"),
    [
        ("PUT", "", {"title": "must not change"}),
        ("POST", "/retry", None),
        ("DELETE", "", None),
    ],
)
async def test_waiting_capability_rejects_ordinary_task_management(
    client,
    session_factory,
    method,
    suffix,
    payload,
):
    """Only the durable resume protocol may advance a waiting Task."""

    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            title="durable capability wait",
            description="d",
            status="waiting_capability",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    response = await client.request(
        method,
        f"/api/tasks/{task_id}{suffix}",
        json=payload,
    )

    assert response.status_code == 409, response.text
    assert "waiting" in response.json()["detail"].lower()
    assert "capability" in response.json()["detail"].lower()
    async with session_factory() as db:
        current = await db.get(Task, task_id)
        assert current is not None
        assert current.status == "waiting_capability"
        assert current.title == "durable capability wait"
        assert current.retry_count == 0


@pytest.mark.asyncio
async def test_waiting_capability_keeps_independent_task_preferences_available(
    client,
    session_factory,
):
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            title="waiting preferences",
            description="d",
            status="waiting_capability",
            has_unread=True,
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    for suffix in ("star", "read", "unread", "archive"):
        response = await client.post(f"/api/tasks/{task_id}/{suffix}")
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "waiting_capability"

    async with session_factory() as db:
        current = await db.get(Task, task_id)
        assert current is not None
        assert current.status == "waiting_capability"
        assert current.starred is True
        assert current.has_unread is True
        assert current.archived is True


async def _seed_waiting_capability_for_terminal_api(
    db,
    *,
    capability_key: str = "plan",
    executor_kind: str = "plan_agent",
    invocation_status: str = "queued",
    queued_runtime_evidence: bool = False,
):
    """Create one exact agent-request aggregate for Task terminal tests."""

    from backend.models.capability import (
        CapabilityExecution,
        CapabilityInvocation,
        CapabilityResumeOutbox,
    )
    from backend.models.task import Task

    digest = "d" * 64
    task = Task(
        title=f"waiting {capability_key}",
        description="terminalize the exact capability wait",
        status="waiting_capability",
        mode="auto",
        provider="claude",
        retry_count=2,
        turn_generation=7,
        session_id="waiting-capability-session",
    )
    db.add(task)
    await db.flush()
    source_log_id = task.id * 10 + 1
    output_log_id = task.id * 10 + 2
    terminal_log_id = task.id * 10 + 3
    result_ready = invocation_status in {"ready", "resuming"}
    invocation = CapabilityInvocation(
        task_id=task.id,
        capability_key=capability_key,
        source="agent_request",
        purpose="advisory",
        status=invocation_status,
        state_version=1,
        idempotency_key=f"waiting-api-{task.id}",
        input_payload={"focus": "safe cancellation"},
        input_hash=digest,
        subject_kind="task_generation",
        subject_ref={
            "task_id": task.id,
            "incarnation_id": task.incarnation_id,
            "retry_count": task.retry_count,
            "turn_generation": task.turn_generation,
        },
        subject_hash=digest,
        executor_kind=executor_kind,
        executor_config={},
        executor_config_hash=digest,
        policy_snapshot={"enabled": True},
        policy_hash=digest,
        resume_policy="resume_task",
        max_attempts=1,
        active_task_id=task.id,
        request_task_incarnation_id=task.incarnation_id,
        request_task_retry_count=task.retry_count,
        request_task_instance_id=None,
        request_task_started_at=None,
        request_task_session_id=task.session_id,
        request_task_turn_generation=task.turn_generation,
        request_source_log_id=source_log_id,
        request_output_log_id=output_log_id,
        request_terminal_log_id=terminal_log_id,
        request_reason="Need capability guidance",
        request_protocol_version=1,
        request_output_hash=digest,
        result_kind=("plan_version" if result_ready else None),
        result_id=(task.id if result_ready else None),
        result_hash=(digest if result_ready else None),
        ready_at=(datetime.utcnow() if result_ready else None),
    )
    db.add(invocation)
    await db.flush()
    running = invocation_status in {"running", "waiting_user", "cancelling"}
    execution = CapabilityExecution(
        invocation_id=invocation.id,
        attempt=1,
        status=("completed" if result_ready else invocation_status),
        state_version=1,
        active_invocation_id=(None if result_ready else invocation.id),
        idempotency_key=f"waiting-api-execution-{task.id}",
        executor_kind=executor_kind,
        input_hash=digest,
        handle_kind=(
            f"{capability_key}_runtime"
            if running or queued_runtime_evidence
            else None
        ),
        handle_id=(
            str(task.id) if running or queued_runtime_evidence else None
        ),
        handle_generation=(1 if running or queued_runtime_evidence else None),
        lease_token=(digest if running else None),
        heartbeat_at=(datetime.utcnow() if running else None),
        started_at=(datetime.utcnow() if running else None),
        output_kind=("plan_version" if result_ready else None),
        output_id=(task.id if result_ready else None),
        output_hash=(digest if result_ready else None),
        completed_at=(datetime.utcnow() if result_ready else None),
    )
    db.add(execution)
    await db.flush()
    outbox = CapabilityResumeOutbox(
        task_id=task.id,
        invocation_id=invocation.id,
        active_task_id=task.id,
        active_invocation_id=invocation.id,
        status="pending",
        state_version=1,
        request_task_incarnation_id=task.incarnation_id,
        request_task_retry_count=task.retry_count,
        from_turn_generation=task.turn_generation,
        request_task_session_id=task.session_id,
        request_source_log_id=source_log_id,
        request_output_log_id=output_log_id,
        request_terminal_log_id=terminal_log_id,
    )
    db.add(outbox)
    await db.commit()
    return task.id, invocation.id, execution.id, outbox.id


async def _promote_waiting_capability_to_released_claimed_g_plus_one(
    db,
    *,
    task_id: int,
    invocation_id: int,
    execution_id: int,
    outbox_id: int,
) -> None:
    """Model the canonical claimed G+1 state after pre-provider release."""

    from backend.models.capability import (
        CapabilityExecution,
        CapabilityInvocation,
        CapabilityResumeOutbox,
    )
    from backend.models.task import Task
    from backend.services.terminal_arbitration import bind_turn_source

    digest = "d" * 64
    task = await db.get(Task, task_id, populate_existing=True)
    invocation = await db.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    execution = await db.get(
        CapabilityExecution,
        execution_id,
        populate_existing=True,
    )
    outbox = await db.get(
        CapabilityResumeOutbox,
        outbox_id,
        populate_existing=True,
    )
    task.turn_generation = outbox.from_turn_generation + 1
    task.instance_id = 77
    source = await bind_turn_source(
        db,
        task,
        None,
        instance_id=77,
        transport=None,
    )
    task.status = "waiting_capability"
    task.instance_id = None

    now = datetime.utcnow()
    invocation.status = "resuming"
    invocation.state_version += 1
    invocation.result_kind = "plan_version"
    invocation.result_id = task.id
    invocation.result_hash = digest
    invocation.ready_at = now
    execution.status = "completed"
    execution.state_version += 1
    execution.active_invocation_id = None
    execution.output_kind = "plan_version"
    execution.output_id = task.id
    execution.output_hash = digest
    execution.completed_at = now
    outbox.status = "claimed"
    outbox.state_version += 1
    outbox.invocation_terminal_status = "completed"
    outbox.invocation_result_kind = "plan_version"
    outbox.invocation_result_id = task.id
    outbox.invocation_result_hash = digest
    outbox.resume_payload = {
        "schema_version": 1,
        "status": "completed",
        "result": {"kind": "plan_version", "id": task.id},
    }
    outbox.resume_payload_hash = digest
    outbox.resume_source_log_id = source.id
    outbox.claimed_turn_generation = task.turn_generation
    outbox.attempt_count = 1
    outbox.ready_at = now
    outbox.claimed_at = now
    outbox.lease_token = None
    outbox.lease_expires_at = None
    await db.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "terminal_status"),
    (
        pytest.param("stop-session", "completed", id="stop-session"),
        pytest.param("cancel", "cancelled", id="cancel"),
    ),
)
async def test_waiting_capability_queued_terminal_request_is_atomic(
    client,
    session_factory,
    endpoint,
    terminal_status,
):
    """No-runtime queued work, its outbox, and Task settle in this order."""

    from backend.models.capability import (
        CapabilityExecution,
        CapabilityInvocation,
        CapabilityResumeOutbox,
    )
    from backend.models.task import Task

    async with session_factory() as db:
        task_id, invocation_id, execution_id, outbox_id = (
            await _seed_waiting_capability_for_terminal_api(db)
        )

    response = await client.post(f"/api/tasks/{task_id}/{endpoint}")

    assert response.status_code == 200, response.text
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        invocation = await db.get(CapabilityInvocation, invocation_id)
        execution = await db.get(CapabilityExecution, execution_id)
        outbox = await db.get(CapabilityResumeOutbox, outbox_id)
        assert task.status == terminal_status
        assert invocation.status == "cancelled"
        assert invocation.active_task_id is None
        assert execution.status == "cancelled"
        assert execution.active_invocation_id is None
        assert outbox.status == "cancelled"
        assert outbox.active_task_id is None
        assert outbox.active_invocation_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "terminal_status"),
    (
        pytest.param("stop-session", "completed", id="stop-session"),
        pytest.param("cancel", "cancelled", id="cancel"),
    ),
)
async def test_waiting_capability_released_claimed_g_plus_one_can_stop(
    client,
    session_factory,
    endpoint,
    terminal_status,
):
    """A released pre-provider G+1 is replayable, but also stoppable."""

    from backend.models.capability import (
        CapabilityInvocation,
        CapabilityResumeOutbox,
    )
    from backend.models.task import Task

    async with session_factory() as db:
        task_id, invocation_id, execution_id, outbox_id = (
            await _seed_waiting_capability_for_terminal_api(
                db,
                invocation_status="resuming",
            )
        )
    async with session_factory() as db:
        await _promote_waiting_capability_to_released_claimed_g_plus_one(
            db,
            task_id=task_id,
            invocation_id=invocation_id,
            execution_id=execution_id,
            outbox_id=outbox_id,
        )

    response = await client.post(f"/api/tasks/{task_id}/{endpoint}")

    assert response.status_code == 200, response.text
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        invocation = await db.get(CapabilityInvocation, invocation_id)
        outbox = await db.get(CapabilityResumeOutbox, outbox_id)
        assert task.status == terminal_status
        assert task.turn_generation == 8
        assert invocation.status == "cancelled"
        assert outbox.status == "cancelled"
        assert outbox.claimed_turn_generation == 8
        assert outbox.resume_source_log_id == task.turn_source_log_id


@pytest.mark.asyncio
async def test_waiting_capability_quiesces_inflight_claim_before_cancelling(
    client,
    session_factory,
):
    """A consumer claiming G+1 on cancellation cannot race the Invocation."""

    from backend.models.capability import (
        CapabilityInvocation,
        CapabilityResumeOutbox,
    )
    from backend.models.task import Task
    import backend.main

    async with session_factory() as db:
        task_id, invocation_id, execution_id, outbox_id = (
            await _seed_waiting_capability_for_terminal_api(
                db,
                invocation_status="resuming",
            )
        )

    worker_started = asyncio.Event()
    never = asyncio.Event()

    async def claim_while_unwinding():
        worker_started.set()
        try:
            await never.wait()
        except asyncio.CancelledError:
            async with session_factory() as db:
                await _promote_waiting_capability_to_released_claimed_g_plus_one(
                    db,
                    task_id=task_id,
                    invocation_id=invocation_id,
                    execution_id=execution_id,
                    outbox_id=outbox_id,
                )

    worker = asyncio.create_task(claim_while_unwinding())
    backend.main.dispatcher._task_queue_workers[task_id] = worker
    await worker_started.wait()
    try:
        response = await client.post(f"/api/tasks/{task_id}/cancel")
    finally:
        backend.main.dispatcher._task_queue_workers.pop(task_id, None)
        if not worker.done():
            worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)

    assert response.status_code == 200, response.text
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        invocation = await db.get(CapabilityInvocation, invocation_id)
        outbox = await db.get(CapabilityResumeOutbox, outbox_id)
        assert task.status == "cancelled"
        assert task.turn_generation == 8
        assert invocation.status == "cancelled"
        assert outbox.status == "cancelled"
        assert outbox.resume_actual_transport is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "terminal_status"),
    (
        pytest.param("stop-session", "completed", id="stop-session"),
        pytest.param("cancel", "cancelled", id="cancel"),
    ),
)
async def test_claimed_capability_resume_live_worker_stops_before_outbox_cancel(
    client,
    session_factory,
    monkeypatch,
    endpoint,
    terminal_status,
):
    """Stop/cancel joins real claimed G+1 before settling its outbox."""

    import backend.api.tasks as tasks_module
    import backend.main
    from backend.models.capability import (
        CapabilityExecution,
        CapabilityInvocation,
        CapabilityResumeOutbox,
    )
    from backend.models.instance import Instance
    from backend.models.log_entry import LogEntry
    from backend.models.task import Task
    from backend.services.capability_resume import materialize_resume_outbox
    from backend.tests.test_capability_resume import (
        _install_fake_result,
        _seed_resume,
    )
    from backend.tests.test_service_dispatcher import _make_dispatcher

    monkeypatch.setattr(
        tasks_module,
        "_find_session_jsonl",
        lambda _session_id, provider="claude": "/tmp/fake.jsonl",
    )
    dispatcher = _make_dispatcher(session_factory)
    dispatcher._resolve_resume_config_dir = AsyncMock(return_value=None)

    async with session_factory() as db:
        seed = await _seed_resume(db, invocation_status="ready")
        instance = Instance(name="claimed-resume-stop", status="idle")
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
        instance_id = instance.id
    _install_fake_result(monkeypatch, seed.execution_id)
    async with session_factory() as db:
        ready = await materialize_resume_outbox(db, seed.outbox_id)
    assert ready is not None and ready.status == "ready"

    launch_entered = asyncio.Event()
    async def block_before_provider_boundary(**kwargs):
        assert kwargs["task_id"] == seed.task_id
        assert kwargs["instance_id"] == instance_id
        assert kwargs["task_turn_generation"] == 8
        assert callable(kwargs["on_launch_admitted"])
        launch_entered.set()
        # Never invoke the provider-boundary callback. Cancellation must join
        # this exact pre-provider worker before changing the durable outbox.
        await asyncio.Event().wait()

    dispatcher.instance_manager.launch = AsyncMock(
        side_effect=block_before_provider_boundary
    )

    with (
        patch.object(backend.main, "dispatcher", dispatcher),
        patch.object(
            backend.main,
            "instance_manager",
            dispatcher.instance_manager,
        ),
    ):
        assert await dispatcher.enqueue_capability_resume(seed.outbox_id)
        await asyncio.wait_for(launch_entered.wait(), timeout=2)

        async with session_factory() as db:
            claimed_task = await db.get(Task, seed.task_id)
            claimed_outbox = await db.get(
                CapabilityResumeOutbox,
                seed.outbox_id,
            )
            claimed_source = await db.get(
                LogEntry,
                claimed_outbox.resume_source_log_id,
            )
            claimed_lease = claimed_outbox.lease_token
            claimed_source_id = claimed_source.id
            assert claimed_task.status == "executing"
            assert claimed_task.turn_generation == 8
            assert claimed_task.instance_id == instance_id
            assert claimed_task.turn_source_log_id == claimed_source.id
            assert claimed_outbox.status == "claimed"
            assert claimed_outbox.claimed_turn_generation == 8
            assert isinstance(claimed_lease, str) and len(claimed_lease) == 64
            assert claimed_source.actual_transport is None
            assert claimed_source.instance_id == instance_id

        response = await client.post(
            f"/api/tasks/{seed.task_id}/{endpoint}"
        )

    assert response.status_code == 200, response.text
    async with session_factory() as db:
        task = await db.get(Task, seed.task_id)
        invocation = await db.get(
            CapabilityInvocation,
            seed.invocation_id,
        )
        execution = await db.get(
            CapabilityExecution,
            seed.execution_id,
        )
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        source = await db.get(LogEntry, claimed_source_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == terminal_status
        assert task.turn_generation == 8
        assert task.instance_id is None
        assert task.turn_source_log_id == claimed_source_id
        assert invocation.status == "cancelled"
        assert invocation.active_task_id is None
        assert execution.status == "completed"
        assert execution.active_invocation_id is None
        assert outbox.status == "cancelled"
        assert outbox.claimed_turn_generation == 8
        assert outbox.resume_source_log_id == claimed_source_id
        assert outbox.lease_token is None
        assert outbox.resume_actual_transport is None
        assert outbox.launched_at is None
        assert source.actual_transport is None
        assert source.instance_id == instance_id
        assert instance.status == "idle"
        assert instance.current_task_id is None

    assert seed.task_id not in dispatcher._task_queue_workers
    assert seed.task_id not in dispatcher._task_queue_active_messages
    assert seed.task_id not in dispatcher._task_queue_inflight
    assert seed.outbox_id not in dispatcher._queued_capability_resume_ids
    assert instance_id not in dispatcher._instance_claim_owners


@pytest.mark.asyncio
async def test_waiting_capability_launched_outbox_fails_closed(
    client,
    session_factory,
):
    """Provider-boundary evidence must survive an inconsistent waiting Task."""

    from backend.models.capability import (
        CapabilityInvocation,
        CapabilityResumeOutbox,
    )
    from backend.models.log_entry import LogEntry
    from backend.models.task import Task
    import backend.main

    async with session_factory() as db:
        task_id, invocation_id, execution_id, outbox_id = (
            await _seed_waiting_capability_for_terminal_api(
                db,
                invocation_status="resuming",
            )
        )
    async with session_factory() as db:
        await _promote_waiting_capability_to_released_claimed_g_plus_one(
            db,
            task_id=task_id,
            invocation_id=invocation_id,
            execution_id=execution_id,
            outbox_id=outbox_id,
        )
        outbox = await db.get(CapabilityResumeOutbox, outbox_id)
        source = await db.get(LogEntry, outbox.resume_source_log_id)
        now = datetime.utcnow()
        source.actual_transport = "claude_exec"
        outbox.status = "launched"
        outbox.state_version += 1
        outbox.active_task_id = None
        outbox.active_invocation_id = None
        outbox.resume_actual_transport = "claude_exec"
        outbox.launched_at = now
        await db.commit()

    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
    ) as abort:
        response = await client.post(f"/api/tasks/{task_id}/cancel")

    assert response.status_code == 409, response.text
    abort.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        invocation = await db.get(CapabilityInvocation, invocation_id)
        outbox = await db.get(CapabilityResumeOutbox, outbox_id)
        assert task.status == "waiting_capability"
        assert invocation.status == "resuming"
        assert outbox.status == "launched"
        assert outbox.resume_actual_transport == "claude_exec"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("capability_key", "executor_kind", "endpoint", "terminal_status"),
    (
        pytest.param(
            "plan",
            "plan_agent",
            "stop-session",
            "completed",
            id="running-plan-stop",
        ),
        pytest.param(
            "code_review",
            "code_review_task",
            "cancel",
            "cancelled",
            id="running-review-cancel",
        ),
    ),
)
async def test_waiting_capability_running_executor_stops_before_outbox(
    client,
    session_factory,
    capability_key,
    executor_kind,
    endpoint,
    terminal_status,
):
    from backend.models.capability import (
        CapabilityExecution,
        CapabilityInvocation,
        CapabilityResumeOutbox,
    )
    from backend.models.task import Task
    import backend.main

    async with session_factory() as db:
        task_id, invocation_id, execution_id, outbox_id = (
            await _seed_waiting_capability_for_terminal_api(
                db,
                capability_key=capability_key,
                executor_kind=executor_kind,
                invocation_status="running",
            )
        )

    order: list[str] = []

    async def cancel_executor(callback_db, *, invocation_id: int):
        order.append("executor")
        invocation = await callback_db.get(
            CapabilityInvocation,
            invocation_id,
            populate_existing=True,
        )
        execution = await callback_db.get(
            CapabilityExecution,
            execution_id,
            populate_existing=True,
        )
        now = datetime.utcnow()
        execution.status = "cancelled"
        execution.state_version += 1
        execution.active_invocation_id = None
        execution.completed_at = now
        invocation.status = "cancelled"
        invocation.state_version += 1
        invocation.active_task_id = None
        invocation.completed_at = now
        await callback_db.commit()

    executor = MagicMock()
    executor.cancel = AsyncMock(side_effect=cancel_executor)
    definition = MagicMock(
        executor_kind=executor_kind,
        executor=executor,
    )
    original_abort = backend.main.dispatcher.abort_task_queue

    async def observed_abort(*args, **kwargs):
        assert order == ["executor"]
        order.append("outbox")
        return await original_abort(*args, **kwargs)

    with (
        patch(
            "backend.services.capability_registry.resolve_capability",
            return_value=definition,
        ),
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            side_effect=observed_abort,
        ),
    ):
        response = await client.post(f"/api/tasks/{task_id}/{endpoint}")

    assert response.status_code == 200, response.text
    assert order == ["executor", "outbox"]
    executor.cancel.assert_awaited_once()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        outbox = await db.get(CapabilityResumeOutbox, outbox_id)
        assert task.status == terminal_status
        assert outbox.status == "cancelled"


@pytest.mark.asyncio
async def test_waiting_capability_executor_failure_preserves_task_and_outbox(
    client,
    session_factory,
):
    from backend.models.capability import CapabilityResumeOutbox
    from backend.models.task import Task
    import backend.main

    async with session_factory() as db:
        task_id, _invocation_id, _execution_id, outbox_id = (
            await _seed_waiting_capability_for_terminal_api(
                db,
                invocation_status="running",
            )
        )
    executor = MagicMock()
    executor.cancel = AsyncMock(side_effect=RuntimeError("stop failed"))
    definition = MagicMock(executor_kind="plan_agent", executor=executor)

    with (
        patch(
            "backend.services.capability_registry.resolve_capability",
            return_value=definition,
        ),
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
        ) as abort,
    ):
        response = await client.post(f"/api/tasks/{task_id}/cancel")

    assert response.status_code == 409, response.text
    abort.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        outbox = await db.get(CapabilityResumeOutbox, outbox_id)
        assert task.status == "waiting_capability"
        assert outbox.status == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize("invocation_status", ("queued", "running"))
async def test_waiting_capability_noop_cancellation_fails_closed(
    client,
    session_factory,
    invocation_status,
):
    from backend.models.capability import (
        CapabilityExecution,
        CapabilityInvocation,
        CapabilityResumeOutbox,
    )
    from backend.models.task import Task
    import backend.main

    async with session_factory() as db:
        task_id, invocation_id, execution_id, outbox_id = (
            await _seed_waiting_capability_for_terminal_api(
                db,
                invocation_status=invocation_status,
            )
        )

    noop = AsyncMock(return_value=None)
    registry_patch = (
        patch(
            "backend.services.capability_registry.resolve_capability",
            return_value=MagicMock(
                executor_kind="plan_agent",
                executor=MagicMock(cancel=noop),
            ),
        )
        if invocation_status == "running"
        else patch(
            "backend.services.capability_service.cancel_invocation",
            new=noop,
        )
    )
    with (
        registry_patch,
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
        ) as abort,
    ):
        response = await client.post(f"/api/tasks/{task_id}/cancel")

    assert response.status_code == 409, response.text
    assert "did not reach one durable terminal state" in response.json()["detail"]
    noop.assert_awaited_once()
    abort.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        invocation = await db.get(CapabilityInvocation, invocation_id)
        execution = await db.get(CapabilityExecution, execution_id)
        outbox = await db.get(CapabilityResumeOutbox, outbox_id)
        assert task.status == "waiting_capability"
        assert invocation.status == invocation_status
        assert execution.status == invocation_status
        assert outbox.status == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize("registry_case", ["missing", "mismatched"])
async def test_waiting_capability_active_executor_registry_fails_closed(
    client,
    session_factory,
    registry_case,
):
    from backend.models.capability import CapabilityResumeOutbox
    from backend.models.task import Task
    import backend.main

    async with session_factory() as db:
        task_id, _invocation_id, _execution_id, outbox_id = (
            await _seed_waiting_capability_for_terminal_api(
                db,
                invocation_status="running",
            )
        )
    definition = None
    if registry_case == "mismatched":
        definition = MagicMock(
            executor_kind="wrong_executor_kind",
            executor=MagicMock(cancel=AsyncMock()),
        )

    with (
        patch(
            "backend.services.capability_registry.resolve_capability",
            return_value=definition,
        ),
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
        ) as abort,
    ):
        response = await client.post(f"/api/tasks/{task_id}/cancel")

    assert response.status_code == 409, response.text
    assert "unavailable or mismatched" in response.json()["detail"]
    abort.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        outbox = await db.get(CapabilityResumeOutbox, outbox_id)
        assert task.status == "waiting_capability"
        assert outbox.status == "pending"


@pytest.mark.asyncio
async def test_waiting_capability_executor_ack_loss_uses_durable_readback(
    client,
    session_factory,
):
    from backend.models.capability import (
        CapabilityExecution,
        CapabilityInvocation,
        CapabilityResumeOutbox,
    )
    from backend.models.task import Task

    async with session_factory() as db:
        task_id, invocation_id, execution_id, outbox_id = (
            await _seed_waiting_capability_for_terminal_api(
                db,
                invocation_status="running",
            )
        )

    async def cancel_then_lose_ack(callback_db, *, invocation_id: int):
        invocation = await callback_db.get(
            CapabilityInvocation,
            invocation_id,
            populate_existing=True,
        )
        execution = await callback_db.get(
            CapabilityExecution,
            execution_id,
            populate_existing=True,
        )
        now = datetime.utcnow()
        invocation.status = "cancelled"
        invocation.state_version += 1
        invocation.active_task_id = None
        invocation.completed_at = now
        execution.status = "cancelled"
        execution.state_version += 1
        execution.active_invocation_id = None
        execution.completed_at = now
        await callback_db.commit()
        raise ConnectionError("response acknowledgement lost")

    executor = MagicMock()
    executor.cancel = AsyncMock(side_effect=cancel_then_lose_ack)
    definition = MagicMock(executor_kind="plan_agent", executor=executor)
    with patch(
        "backend.services.capability_registry.resolve_capability",
        return_value=definition,
    ):
        response = await client.post(f"/api/tasks/{task_id}/cancel")

    assert response.status_code == 200, response.text
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        invocation = await db.get(CapabilityInvocation, invocation_id)
        execution = await db.get(CapabilityExecution, execution_id)
        outbox = await db.get(CapabilityResumeOutbox, outbox_id)
        assert task.status == "cancelled"
        assert invocation.status == "cancelled"
        assert execution.status == "cancelled"
        assert outbox.status == "cancelled"


@pytest.mark.asyncio
async def test_waiting_capability_client_cancel_cannot_break_cleanup_barrier(
    client,
    session_factory,
):
    from backend.models.capability import (
        CapabilityExecution,
        CapabilityInvocation,
        CapabilityResumeOutbox,
    )
    from backend.models.task import Task

    async with session_factory() as db:
        task_id, invocation_id, execution_id, outbox_id = (
            await _seed_waiting_capability_for_terminal_api(
                db,
                invocation_status="running",
            )
        )
    entered = asyncio.Event()
    allow_finish = asyncio.Event()

    async def delayed_cancel(callback_db, *, invocation_id: int):
        entered.set()
        await allow_finish.wait()
        invocation = await callback_db.get(
            CapabilityInvocation,
            invocation_id,
            populate_existing=True,
        )
        execution = await callback_db.get(
            CapabilityExecution,
            execution_id,
            populate_existing=True,
        )
        now = datetime.utcnow()
        invocation.status = "cancelled"
        invocation.state_version += 1
        invocation.active_task_id = None
        invocation.completed_at = now
        execution.status = "cancelled"
        execution.state_version += 1
        execution.active_invocation_id = None
        execution.completed_at = now
        await callback_db.commit()

    executor = MagicMock()
    executor.cancel = AsyncMock(side_effect=delayed_cancel)
    definition = MagicMock(executor_kind="plan_agent", executor=executor)
    with patch(
        "backend.services.capability_registry.resolve_capability",
        return_value=definition,
    ):
        request = asyncio.create_task(
            client.post(f"/api/tasks/{task_id}/cancel")
        )
        await entered.wait()
        request.cancel()
        await asyncio.sleep(0)
        assert not request.done()
        allow_finish.set()
        with pytest.raises(asyncio.CancelledError):
            await request

    async with session_factory() as db:
        task = await db.get(Task, task_id)
        invocation = await db.get(CapabilityInvocation, invocation_id)
        execution = await db.get(CapabilityExecution, execution_id)
        outbox = await db.get(CapabilityResumeOutbox, outbox_id)
        assert task.status == "cancelled"
        assert invocation.status == "cancelled"
        assert execution.status == "cancelled"
        assert outbox.status == "cancelled"


@pytest.mark.asyncio
async def test_waiting_capability_queued_runtime_evidence_fails_closed(
    client,
    session_factory,
):
    from backend.models.capability import CapabilityResumeOutbox
    from backend.models.task import Task
    import backend.main

    async with session_factory() as db:
        task_id, _invocation_id, _execution_id, outbox_id = (
            await _seed_waiting_capability_for_terminal_api(
                db,
                queued_runtime_evidence=True,
            )
        )

    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
    ) as abort:
        response = await client.post(f"/api/tasks/{task_id}/cancel")

    assert response.status_code == 409, response.text
    assert "no-runtime proof" in response.json()["detail"]
    abort.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        outbox = await db.get(CapabilityResumeOutbox, outbox_id)
        assert task.status == "waiting_capability"
        assert outbox.status == "pending"


@pytest.mark.asyncio
async def test_update_task_yields_to_receipt_after_api_precheck(
    client,
    session_factory,
    monkeypatch,
):
    """The service-level CAS defeats receipt admission after authorization."""

    import backend.services.worker_proxy as worker_proxy_module
    from backend.models.task import Task
    from backend.tests.worker_termination_helpers import (
        persist_active_worker_receipt,
    )

    async with session_factory() as db:
        task = Task(
            title="receipt-owned config",
            description="d",
            status="completed",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    @asynccontextmanager
    async def receipt_wins_before_service_cas(locked_task_id):
        assert locked_task_id == task_id
        await persist_active_worker_receipt(session_factory, task_id)
        yield

    monkeypatch.setattr(
        worker_proxy_module,
        "get_task_operation_lock",
        receipt_wins_before_service_cas,
    )

    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"title": "must not be saved"},
    )

    assert response.status_code == 409, response.text
    assert "termination receipt" in response.text
    async with session_factory() as db:
        current = await db.get(Task, task_id)
    assert current is not None
    assert current.title == "receipt-owned config"


async def _create_worker_task_for_handoff_edit_test(
    session_factory,
    *,
    reserve_handoff: bool,
) -> int:
    """Create a Worker Task, optionally with a valid durable G -> G+1 marker."""

    from backend.models.log_entry import LogEntry
    from backend.models.task import Task
    from backend.models.worker import Worker
    from backend.services.worker_relay import (
        _handoff_payload_digest,
        reserve_worker_turn_handoff,
        worker_task_generation,
    )

    async with session_factory() as db:
        worker = Worker(
            name="handoff-edit-worker",
            status="ready",
            private_ip="10.0.0.77",
            auth_token="worker-token",
        )
        db.add(worker)
        await db.flush()
        task = Task(
            title="Worker handoff edit fence",
            description="original description",
            status="completed",
            worker_id=worker.id,
            provider="codex",
            model="gpt-5.6-sol",
            codex_service_tier="default",
            effort_level="medium",
            system_prompt_mode=None,
            enabled_skills={},
        )
        db.add(task)
        await db.flush()
        await db.refresh(task)

        if reserve_handoff:
            source = LogEntry(
                task_id=task.id,
                event_type="user_message",
                role="user",
                content="reserved follow-up",
            )
            db.add(source)
            await db.flush()
            observed = worker_task_generation(
                task,
                expected_worker_id=worker.id,
            )
            assert observed is not None
            request_payload = {
                "message": "reserved follow-up",
                "worker_turn_handoff_id": "a" * 32,
                "worker_turn_handoff_retry_count": task.retry_count,
                "worker_turn_handoff_from_generation": task.turn_generation,
            }
            reserved = await reserve_worker_turn_handoff(
                db,
                observed,
                handoff_id=request_payload["worker_turn_handoff_id"],
                source_log_id=source.id,
                request_payload=request_payload,
                request_digest=_handoff_payload_digest(request_payload),
            )
            assert reserved is not None

        await db.commit()
        return task.id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"model": "gpt-5.6-terra"}, id="routing-model"),
        pytest.param({"codex_service_tier": "priority"}, id="routing-tier"),
        pytest.param(
            {"enabled_skills": {"sub-agent": True}},
            id="skills",
        ),
        pytest.param({"effort_level": "high"}, id="generic-effort"),
        pytest.param(
            {"system_prompt_mode": "append"},
            id="generic-system-prompt",
        ),
        pytest.param(
            {"description": "changed description"},
            id="generic-description",
        ),
    ],
)
async def test_pending_worker_turn_handoff_blocks_task_configuration_edits(
    client,
    session_factory,
    payload,
):
    """No execution-affecting PUT may cross an exact Worker turn handoff."""

    import backend.api.tasks as task_api
    from backend.models.task import Task

    task_id = await _create_worker_task_for_handoff_edit_test(
        session_factory,
        reserve_handoff=True,
    )
    with patch.object(task_api, "_proxy", new_callable=AsyncMock) as proxy:
        response = await client.put(f"/api/tasks/{task_id}", json=payload)

    assert response.status_code == 409
    assert "Worker follow-up" in response.json()["detail"]
    proxy.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.model == "gpt-5.6-sol"
        assert task.codex_service_tier == "default"
        assert task.enabled_skills == {}
        assert task.effort_level == "medium"
        assert task.system_prompt_mode is None
        assert task.description == "original description"
        assert task.worker_turn_handoff_id == "a" * 32
        assert task.worker_turn_handoff_acknowledged is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "field", "expected"),
    [
        pytest.param(
            {"enabled_skills": {"sub-agent": True}},
            "enabled_skills",
            {"sub-agent": True},
            id="skills",
        ),
        pytest.param(
            {"effort_level": "high"},
            "effort_level",
            "high",
            id="generic-effort",
        ),
        pytest.param(
            {"system_prompt_mode": "append"},
            "system_prompt_mode",
            "append",
            id="generic-system-prompt",
        ),
        pytest.param(
            {"description": "changed description"},
            "description",
            "changed description",
            id="generic-description",
        ),
    ],
)
async def test_worker_task_configuration_edits_still_work_without_handoff(
    client,
    session_factory,
    payload,
    field,
    expected,
):
    """The handoff fence must not freeze an ordinary quiescent Worker Task."""

    from backend.models.task import Task

    task_id = await _create_worker_task_for_handoff_edit_test(
        session_factory,
        reserve_handoff=False,
    )

    response = await client.put(f"/api/tasks/{task_id}", json=payload)

    assert response.status_code == 200, response.text
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert getattr(task, field) == expected
        assert task.worker_turn_handoff_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "expected_model", "expected_tier"),
    [
        pytest.param(
            {"model": "gpt-5.6-terra"},
            "gpt-5.6-terra",
            "default",
            id="model",
        ),
        pytest.param(
            {"codex_service_tier": "priority"},
            "gpt-5.6-sol",
            "priority",
            id="tier",
        ),
    ],
)
async def test_worker_routing_edits_still_sync_without_handoff(
    client,
    session_factory,
    monkeypatch,
    payload,
    expected_model,
    expected_tier,
):
    """A marker-free routing PUT retains the existing Worker sync protocol."""

    import backend.api.tasks as task_api
    from backend.models.task import Task

    task_id = await _create_worker_task_for_handoff_edit_test(
        session_factory,
        reserve_handoff=False,
    )
    calls = []

    async def proxy(_task, method, path, body=None, **_kwargs):
        calls.append((method, path))
        base = {
            "id": task_id,
            "status": "completed",
            "worker_id": None,
            "shared_from_id": None,
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "default",
            "pending": None,
        }
        if path.endswith("/routing-config/stage"):
            return {**base, "pending": body}
        if path.endswith("/routing-config/ack"):
            return {
                **base,
                "model": expected_model,
                "codex_service_tier": expected_tier,
            }
        return base

    monkeypatch.setattr(task_api, "_proxy", proxy)

    response = await client.put(f"/api/tasks/{task_id}", json=payload)

    assert response.status_code == 200, response.text
    assert [method for method, _path in calls] == ["GET", "POST", "POST"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.model == expected_model
        assert task.codex_service_tier == expected_tier
        assert task.worker_turn_handoff_id is None


@pytest.mark.asyncio
async def test_attention_tag_create_update_and_clear_preserves_system_tags(client):
    created = await client.post("/api/tasks", json={
        "title": "Tagged session",
        "description": "d",
        "target_repo": "/tmp",
        "attention_tag": "  等它结束后再看  ",
        "tags": ["existing-system-marker"],
    })

    assert created.status_code == 201
    task_id = created.json()["id"]
    assert created.json()["attention_tag"] == "等它结束后再看"
    assert created.json()["tags"] == ["existing-system-marker"]

    updated = await client.put(
        f"/api/tasks/{task_id}",
        json={"attention_tag": "  今晚继续  "},
    )

    assert updated.status_code == 200
    assert updated.json()["attention_tag"] == "今晚继续"
    assert updated.json()["tags"] == ["existing-system-marker"]

    cleared = await client.put(
        f"/api/tasks/{task_id}",
        json={"attention_tag": "   "},
    )

    assert cleared.status_code == 200
    assert cleared.json()["attention_tag"] is None
    assert cleared.json()["tags"] == ["existing-system-marker"]


@pytest.mark.asyncio
async def test_attention_tag_rejects_values_longer_than_80_characters(client):
    created = await client.post("/api/tasks", json={
        "title": "Tagged session",
        "description": "d",
        "target_repo": "/tmp",
        "attention_tag": "x" * 81,
    })

    assert created.status_code == 422


@pytest.mark.asyncio
async def test_cloned_task_inherits_attention_tag_unless_overridden(client):
    source = await client.post("/api/tasks", json={
        "title": "Source",
        "description": "d",
        "target_repo": "/tmp",
        "attention_tag": "等源任务结束",
    })
    source_id = source.json()["id"]

    inherited = await client.post("/api/tasks", json={
        "title": "Inherited",
        "description": "d",
        "target_repo": "/tmp",
        "clone_from_task_id": source_id,
    })
    overridden = await client.post("/api/tasks", json={
        "title": "Overridden",
        "description": "d",
        "target_repo": "/tmp",
        "clone_from_task_id": source_id,
        "attention_tag": "单独关注",
    })
    cleared = await client.post("/api/tasks", json={
        "title": "Cleared",
        "description": "d",
        "target_repo": "/tmp",
        "clone_from_task_id": source_id,
        "attention_tag": None,
    })

    assert inherited.status_code == 201
    assert inherited.json()["attention_tag"] == "等源任务结束"
    assert overridden.status_code == 201
    assert overridden.json()["attention_tag"] == "单独关注"
    assert cleared.status_code == 201
    assert cleared.json()["attention_tag"] is None


@pytest.mark.asyncio
async def test_update_task_not_found(client):
    resp = await client.put("/api/tasks/9999", json={"title": "X"})
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_tasks_filter_status(client):
    """GET /api/tasks?status=pending returns only matching tasks."""
    await client.post("/api/tasks", json={
        "title": "A", "description": "d", "target_repo": "/tmp",
    })
    create2 = await client.post("/api/tasks", json={
        "title": "B", "description": "d", "target_repo": "/tmp",
    })
    # Cancel B so it's not pending
    await client.post(f"/api/tasks/{create2.json()['id']}/cancel")

    resp = await client.get("/api/tasks?status=pending")
    assert resp.status_code == 200
    tasks = resp.json()
    assert all(t["status"] == "pending" for t in tasks)


@pytest.mark.asyncio
async def test_list_and_count_tasks_filter_by_task_kind(
    client,
    session_factory,
):
    from backend.models.task import Task

    async with session_factory() as db:
        main = Task(title="Main", description="d", mode="auto")
        standalone = Task(
            title="Standalone Plan",
            description="d",
            mode="plan",
        )
        db.add_all([main, standalone])
        await db.flush()
        related = Task(
            title="Related Plan",
            description="d",
            mode="plan",
            plan_target_task_id=main.id,
        )
        db.add(related)
        await db.commit()

    expected = {
        "main": "Main",
        "standalone_plan": "Standalone Plan",
        "related_plan": "Related Plan",
    }
    for task_kind, title in expected.items():
        response = await client.get(
            f"/api/tasks?task_kind={task_kind}"
        )
        assert response.status_code == 200
        assert [task["title"] for task in response.json()] == [title]

        count = await client.get(
            f"/api/tasks/count?task_kind={task_kind}"
        )
        assert count.status_code == 200
        assert count.json() == {"total": 1}

    invalid = await client.get("/api/tasks?task_kind=unknown")
    assert invalid.status_code == 422


@pytest.mark.asyncio
async def test_list_tasks_pagination(client):
    """GET /api/tasks?limit=1&offset=1 returns second task."""
    await client.post("/api/tasks", json={
        "title": "First", "description": "d", "target_repo": "/tmp",
    })
    await client.post("/api/tasks", json={
        "title": "Second", "description": "d", "target_repo": "/tmp",
    })
    resp = await client.get("/api/tasks?limit=1&offset=1")
    assert resp.status_code == 200
    tasks = resp.json()
    assert len(tasks) == 1


@pytest.mark.asyncio
async def test_queue_next(client):
    """GET /api/tasks/queue/next returns pending tasks."""
    await client.post("/api/tasks", json={
        "title": "Pending", "description": "d", "target_repo": "/tmp",
    })
    resp = await client.get("/api/tasks/queue/next")
    assert resp.status_code == 200
    tasks = resp.json()
    assert len(tasks) >= 1
    assert all(t["status"] == "pending" for t in tasks)


@pytest.mark.asyncio
async def test_delete_in_progress_rejected(client, session_factory):
    """Cannot delete a task in in_progress state."""
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    # Set to in_progress directly in DB
    async with session_factory() as db:
        await db.execute(
            update(Task).where(Task.id == task_id).values(status="in_progress")
        )
        await db.commit()

    resp = await client.delete(f"/api/tasks/{task_id}")
    assert resp.status_code == 400


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["plan_review", "superseded"])
async def test_delete_stopped_plan_cleans_pipeline_history(
    client,
    session_factory,
    status,
):
    from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
    from backend.models.task import Task
    from backend.services.plan_runtime_receipt import (
        new_prepared_runtime_receipt,
    )

    async with session_factory() as db:
        finished_at = datetime.utcnow()
        plan = Task(
            title="Disposable Plan",
            description="Plan this",
            status=status,
            mode="plan",
            plan_content="A completed proposal",
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_task_id=plan.id,
            status="completed",
            finished_at=finished_at,
        )
        db.add(run)
        await db.flush()
        step = PlanAgentStep(
            run_id=run.id,
            step_type="planner",
            provider="claude",
            status="completed",
            finished_at=finished_at,
        )
        db.add(step)
        await db.flush()
        receipt = new_prepared_runtime_receipt(step, attempt_index=1)
        receipt.status = "cleaned"
        receipt.cleaned_at = finished_at
        db.add(receipt)
        await db.commit()
        plan_id = plan.id
        run_id = run.id

    response = await client.delete(f"/api/tasks/{plan_id}")

    assert response.status_code == 200
    async with session_factory() as db:
        assert await db.get(Task, plan_id) is None
        assert await db.get(PlanAgentRun, run_id) is None
        steps = (
            await db.execute(
                select(PlanAgentStep).where(PlanAgentStep.run_id == run_id)
            )
        ).scalars().all()
        assert steps == []


@pytest.mark.asyncio
async def test_delete_non_plan_in_plan_review_state_is_rejected(
    client,
    session_factory,
):
    from backend.models.task import Task

    async with session_factory() as db:
        task = Task(
            title="Not a Plan",
            description="work",
            status="plan_review",
            mode="auto",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    response = await client.delete(f"/api/tasks/{task_id}")

    assert response.status_code == 400


# === image_paths tests ===


@pytest.mark.asyncio
async def test_create_task_with_image_paths(
    client,
    session_factory,
    tmp_path,
    monkeypatch,
):
    """image_paths are stored in task.metadata_['image_paths']."""
    from backend.models.task import Task

    monkeypatch.setattr("backend.api.uploads.UPLOAD_DIR", tmp_path)
    image_paths = [
        tmp_path / "11111111-1111-4111-8111-111111111111.png",
        tmp_path / "22222222-2222-4222-8222-222222222222.jpg",
    ]
    for path in image_paths:
        path.write_bytes(b"image")
    resp = await client.post("/api/tasks", json={
        "title": "Img Task",
        "description": "look at this image",
        "target_repo": "/tmp",
        "image_paths": [str(path) for path in image_paths],
    })
    assert resp.status_code == 201
    task_id = resp.json()["id"]

    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.metadata_ is not None
    expected_paths = [str(path) for path in image_paths]
    assert task.metadata_["file_paths"] == expected_paths
    assert task.metadata_["image_paths"] == expected_paths


@pytest.mark.asyncio
async def test_create_task_without_image_paths(client, session_factory):
    """Task created without image_paths has no image_paths in metadata_."""
    from backend.models.task import Task

    resp = await client.post("/api/tasks", json={
        "title": "No Img", "description": "plain task", "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    task_id = resp.json()["id"]

    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert (task.metadata_ or {}).get("image_paths") is None


@pytest.mark.asyncio
async def test_create_task_image_paths_not_in_response(
    client,
    tmp_path,
    monkeypatch,
):
    """image_paths field is not leaked in the TaskResponse (stored in metadata_)."""
    monkeypatch.setattr("backend.api.uploads.UPLOAD_DIR", tmp_path)
    image_path = tmp_path / "33333333-3333-4333-8333-333333333333.png"
    image_path.write_bytes(b"image")
    resp = await client.post("/api/tasks", json={
        "title": "Img Task",
        "description": "check response",
        "target_repo": "/tmp",
        "image_paths": [str(image_path)],
    })
    assert resp.status_code == 201
    data = resp.json()
    # image_paths should not appear as a top-level key in the response schema
    assert "image_paths" not in data
    assert "image_paths" not in (data.get("metadata_") or {})


@pytest.mark.asyncio
async def test_create_task_rejects_unmanaged_attachment_path(
    client,
    tmp_path,
    monkeypatch,
):
    upload_dir = tmp_path / "uploads"
    upload_dir.mkdir()
    monkeypatch.setattr("backend.api.uploads.UPLOAD_DIR", upload_dir)
    outside = tmp_path / "44444444-4444-4444-8444-444444444444.txt"
    outside.write_text("host data", encoding="utf-8")

    response = await client.post("/api/tasks", json={
        "title": "Forged attachment",
        "description": "must fail",
        "target_repo": "/tmp",
        "file_paths": [str(outside)],
    })

    assert response.status_code == 422
    assert "upload directory" in response.text


# === max_iterations tests ===


@pytest.mark.asyncio
async def test_create_loop_task_default_max_iterations(client):
    """Loop task created without max_iterations gets default value of 50."""
    resp = await client.post("/api/tasks", json={
        "title": "Loop Default",
        "mode": "loop",
        "todo_file_path": "TODO.md",
        "target_repo": "/tmp/repo",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["max_iterations"] == 50


@pytest.mark.asyncio
async def test_create_loop_task_custom_max_iterations(client):
    """Loop task created with custom max_iterations stores it correctly."""
    resp = await client.post("/api/tasks", json={
        "title": "Loop Custom",
        "mode": "loop",
        "todo_file_path": "TODO.md",
        "target_repo": "/tmp/repo",
        "max_iterations": 10,
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["max_iterations"] == 10


@pytest.mark.asyncio
async def test_create_auto_task_max_iterations_in_response(client):
    """Non-loop task also exposes max_iterations in response (always 50 by default)."""
    resp = await client.post("/api/tasks", json={
        "title": "Auto Task",
        "description": "do something",
        "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert "max_iterations" in data
    assert data["max_iterations"] == 50


@pytest.mark.asyncio
async def test_update_task_max_iterations(client):
    """PUT /api/tasks/{id} can update max_iterations."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Loop Task",
        "mode": "loop",
        "todo_file_path": "TODO.md",
        "target_repo": "/tmp/repo",
        "max_iterations": 20,
    })
    task_id = create_resp.json()["id"]

    resp = await client.put(f"/api/tasks/{task_id}", json={"max_iterations": 5})
    assert resp.status_code == 200
    assert resp.json()["max_iterations"] == 5


@pytest.mark.asyncio
async def test_create_loop_task_requires_todo_file_path(client):
    """Loop task without todo_file_path returns 422."""
    resp = await client.post("/api/tasks", json={
        "title": "Missing Todo",
        "mode": "loop",
        "target_repo": "/tmp/repo",
    })
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_loop_task_max_iterations_persisted(client, session_factory):
    """max_iterations value is actually persisted to the database."""
    from backend.models.task import Task

    resp = await client.post("/api/tasks", json={
        "title": "Persisted",
        "mode": "loop",
        "todo_file_path": "TODO.md",
        "target_repo": "/tmp/repo",
        "max_iterations": 7,
    })
    task_id = resp.json()["id"]

    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.max_iterations == 7


# === has_unread tests ===


@pytest.mark.asyncio
async def test_create_task_has_unread_defaults_false(client):
    """New task has has_unread=False by default."""
    resp = await client.post("/api/tasks", json={
        "title": "Unread test", "description": "d", "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    assert resp.json()["has_unread"] is False


@pytest.mark.asyncio
async def test_mark_task_read_clears_unread(client, session_factory):
    """POST /api/tasks/{id}/read sets has_unread=False."""
    from backend.models.task import Task
    from sqlalchemy import update

    create_resp = await client.post("/api/tasks", json={
        "title": "Unread", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    # Set has_unread=True directly in DB
    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == task_id).values(has_unread=True))
        await db.commit()

    resp = await client.post(f"/api/tasks/{task_id}/read")
    assert resp.status_code == 200
    assert resp.json()["has_unread"] is False


@pytest.mark.asyncio
async def test_mark_task_read_not_found(client):
    """POST /api/tasks/9999/read returns 404 for missing task."""
    resp = await client.post("/api/tasks/9999/read")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_has_unread_persisted_in_db(client, session_factory):
    """has_unread=True set in DB is returned in task response."""
    from backend.models.task import Task
    from sqlalchemy import update

    create_resp = await client.post("/api/tasks", json={
        "title": "Persist unread", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == task_id).values(has_unread=True))
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["has_unread"] is True


# === Model field tests ===


@pytest.mark.asyncio
async def test_create_task_with_model(client):
    """Task created with model field stores and returns the model."""
    resp = await client.post("/api/tasks", json={
        "title": "Opus task", "description": "d", "target_repo": "/tmp", "model": "opus",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["model"] == "opus"


@pytest.mark.asyncio
async def test_create_task_without_model_fills_default(client):
    """设置归 Task：不指定 model 时创建即填入全局默认值。"""
    from backend.config import settings
    with (
        patch.object(settings, "default_provider", "codex"),
        patch.object(settings, "default_codex_model", "gpt-5.6-sol"),
    ):
        resp = await client.post("/api/tasks", json={
            "title": "No model", "description": "d", "target_repo": "/tmp",
        })
    assert resp.status_code == 201
    data = resp.json()
    assert data["provider"] == "codex"
    assert data["model"] == "gpt-5.6-sol"


@pytest.mark.asyncio
async def test_create_task_model_persisted_in_get(client):
    """Model value survives a round-trip through GET."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp", "model": "sonnet",
    })
    task_id = create_resp.json()["id"]

    resp = await client.get(f"/api/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["model"] == "sonnet"


@pytest.mark.asyncio
async def test_create_task_model_in_list(client):
    """model field is included when listing tasks."""
    await client.post("/api/tasks", json={
        "title": "A", "description": "d", "target_repo": "/tmp", "model": "haiku",
    })
    resp = await client.get("/api/tasks")
    assert resp.status_code == 200
    tasks = resp.json()
    assert tasks[0]["model"] == "haiku"


# === Codex service tier tests ===


@pytest.mark.asyncio
async def test_create_task_defaults_to_standard_service_tier(client):
    resp = await client.post("/api/tasks", json={
        "title": "Standard task",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.6-sol",
    })

    assert resp.status_code == 201, resp.text
    task_id = resp.json()["id"]
    assert resp.json()["codex_service_tier"] == "default"

    get_resp = await client.get(f"/api/tasks/{task_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["codex_service_tier"] == "default"


@pytest.mark.asyncio
async def test_create_fast_codex_task_persists_priority(client):
    resp = await client.post("/api/tasks", json={
        "title": "Fast task",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    })

    assert resp.status_code == 201, resp.text
    assert resp.json()["codex_service_tier"] == "priority"


@pytest.mark.asyncio
async def test_create_fast_goal_inherits_task_model_for_evaluator(client):
    resp = await client.post("/api/tasks", json={
        "title": "Fast Goal",
        "description": "d",
        "mode": "goal",
        "goal_condition": "tests pass",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    })

    assert resp.status_code == 201, resp.text
    assert resp.json()["goal_evaluator_model"] is None
    assert resp.json()["codex_service_tier"] == "priority"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("model", "goal_evaluator_model"),
    [
        (None, "configured-default"),
        ("default", "configured-default"),
        ("configured-default", "default"),
    ],
)
async def test_create_fast_goal_normalizes_default_model_aliases(
    client,
    model,
    goal_evaluator_model,
):
    from backend.config import settings

    default_model = settings.default_codex_model
    payload = {
        "title": "Fast Goal default aliases",
        "description": "d",
        "mode": "goal",
        "goal_condition": "tests pass",
        "provider": "codex",
        "model": default_model if model == "configured-default" else model,
        "goal_evaluator_model": (
            default_model
            if goal_evaluator_model == "configured-default"
            else goal_evaluator_model
        ),
        "codex_service_tier": "priority",
    }

    resp = await client.post("/api/tasks", json=payload)

    assert resp.status_code == 201, resp.text
    assert resp.json()["codex_service_tier"] == "priority"


@pytest.mark.asyncio
async def test_create_fast_goal_rejects_distinct_evaluator_model(client):
    resp = await client.post("/api/tasks", json={
        "title": "Split Fast Goal",
        "description": "d",
        "mode": "goal",
        "goal_condition": "tests pass",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "goal_evaluator_model": "gpt-5.6-terra",
        "codex_service_tier": "priority",
    })

    assert resp.status_code == 422
    assert "must use the Task model" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_update_fast_goal_rejects_distinct_evaluator_model(
    client,
    session_factory,
):
    from backend.models.task import Task

    created = await client.post("/api/tasks", json={
        "title": "Fast Goal update",
        "description": "d",
        "mode": "goal",
        "goal_condition": "tests pass",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    })
    task_id = created.json()["id"]
    cancelled = await client.post(f"/api/tasks/{task_id}/cancel")
    assert cancelled.status_code == 200

    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"goal_evaluator_model": "gpt-5.6-terra"},
    )

    assert response.status_code == 422
    assert "must use the Task model" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.goal_evaluator_model is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "model", "detail"),
    [
        ("claude", "claude-opus-4-6", "only available for Codex"),
        ("codex", "gpt-5.4-mini", "not supported by model"),
        ("codex", "gpt-5.3-codex-spark", "not supported by model"),
    ],
)
async def test_create_fast_task_rejects_incompatible_configuration(
    client,
    provider,
    model,
    detail,
):
    resp = await client.post("/api/tasks", json={
        "title": "Invalid Fast task",
        "description": "d",
        "provider": provider,
        "model": model,
        "codex_service_tier": "priority",
    })

    assert resp.status_code == 422
    assert detail in resp.json()["detail"]


@pytest.mark.asyncio
async def test_update_validates_merged_provider_model_and_service_tier(client):
    create_resp = await client.post("/api/tasks", json={
        "title": "Fast task",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    })
    assert create_resp.status_code == 201
    task_id = create_resp.json()["id"]

    claude_resp = await client.put(
        f"/api/tasks/{task_id}",
        json={"provider": "claude"},
    )
    assert claude_resp.status_code == 422
    assert "only available for Codex" in claude_resp.json()["detail"]

    mini_resp = await client.put(
        f"/api/tasks/{task_id}",
        json={"model": "gpt-5.4-mini"},
    )
    assert mini_resp.status_code == 422
    assert "not supported by model" in mini_resp.json()["detail"]

    disable_resp = await client.put(
        f"/api/tasks/{task_id}",
        json={
            "provider": "claude",
            "model": "claude-opus-4-6",
            "codex_service_tier": "default",
        },
    )
    assert disable_resp.status_code == 200, disable_resp.text
    assert disable_resp.json()["provider"] == "claude"
    assert disable_resp.json()["codex_service_tier"] == "default"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "mode"),
    (("in_progress", "loop"), ("executing", "goal")),
)
async def test_local_fast_update_rejects_active_mode_generation(
    client,
    session_factory,
    status,
    mode,
):
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Active Standard task",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.6-sol",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status=status, mode=mode)
        )
        await db.commit()

    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 409
    assert "execution claim became active" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.codex_service_tier == "default"


@pytest.mark.asyncio
async def test_local_fast_update_rejects_running_ccm_sub_agent(
    client,
    session_factory,
):
    from backend.models.monitor_session import MonitorSession
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Parent with old Standard child",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.6-sol",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        db.add(
            MonitorSession(
                task_id=task_id,
                agent_type="sub_agent",
                source="ccm",
                description="still running",
                status="running",
            )
        )
        await db.commit()

    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 409
    assert "sub-agent is running" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.codex_service_tier == "default"


@pytest.mark.asyncio
async def test_local_fast_update_holds_codex_thread_guard_through_commit(
    client,
    session_factory,
    monkeypatch,
):
    import backend.main as main_module
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Idle native thread",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.6-sol",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        task.session_id = "idle-native-thread"
        await db.commit()

    guard_exited = False

    @asynccontextmanager
    async def routing_guard(_home, thread_id):
        nonlocal guard_exited
        assert thread_id == "idle-native-thread"
        async with session_factory() as db:
            before = await db.get(Task, task_id)
            assert before.codex_service_tier == "default"
        yield {"thread": {"status": {"type": "idle"}}, "goal": None}
        async with session_factory() as db:
            committed = await db.get(Task, task_id)
            assert committed.codex_service_tier == "priority"
        guard_exited = True

    monkeypatch.setattr(
        main_module.instance_manager,
        "codex_thread_routing_guard",
        routing_guard,
    )
    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["codex_service_tier"] == "priority"
    assert guard_exited


@pytest.mark.asyncio
async def test_local_fast_update_fails_before_commit_when_thread_not_idle(
    client,
    session_factory,
    monkeypatch,
):
    import backend.main as main_module
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Active native Goal",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.6-sol",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        task.session_id = "active-goal-thread"
        await db.commit()

    @asynccontextmanager
    async def routing_guard(_home, _thread_id):
        raise RuntimeError("goal:active")
        yield  # pragma: no cover

    monkeypatch.setattr(
        main_module.instance_manager,
        "codex_thread_routing_guard",
        routing_guard,
    )
    response = await client.put(
        f"/api/tasks/{task_id}",
        json={"codex_service_tier": "priority"},
    )

    assert response.status_code == 409
    assert "could not be proven idle" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.codex_service_tier == "default"


@pytest.mark.asyncio
async def test_update_rejects_null_or_unknown_service_tier(client):
    create_resp = await client.post("/api/tasks", json={
        "title": "Tier validation",
        "description": "d",
    })
    task_id = create_resp.json()["id"]

    null_resp = await client.put(
        f"/api/tasks/{task_id}",
        json={"codex_service_tier": None},
    )
    assert null_resp.status_code == 422

    unknown_resp = await client.put(
        f"/api/tasks/{task_id}",
        json={"codex_service_tier": "turbo"},
    )
    assert unknown_resp.status_code == 422


@pytest.mark.asyncio
async def test_migration_import_preserves_fast_service_tier(client):
    resp = await _post_migration_import(client, {
        "id": 7091,
        "title": "Migrated Fast task",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.5",
        "codex_service_tier": "priority",
        "source_incarnation_id": "9" * 32,
    })

    assert resp.status_code == 201, resp.text
    assert resp.json()["status"] == "cancelled"
    assert resp.json()["codex_service_tier"] == "priority"


@pytest.mark.asyncio
async def test_migration_import_rejects_incompatible_fast_service_tier(client):
    resp = await _post_migration_import(client, {
        "id": 7092,
        "title": "Invalid migrated Fast task",
        "description": "d",
        "provider": "codex",
        "model": "gpt-5.4-mini",
        "codex_service_tier": "priority",
        "source_incarnation_id": "a" * 32,
    })

    assert resp.status_code == 422
    assert "not supported by model" in resp.json()["detail"]


# === Title update tests ===


@pytest.mark.asyncio
async def test_update_task_title_only(client):
    """PUT /api/tasks/{id} with only title preserves other fields."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Original Title", "description": "Keep this", "target_repo": "/tmp", "priority": 2,
    })
    task_id = create_resp.json()["id"]

    resp = await client.put(f"/api/tasks/{task_id}", json={"title": "New Title"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["title"] == "New Title"
    assert data["description"] == "Keep this"
    assert data["priority"] == 2


@pytest.mark.asyncio
async def test_update_task_title_empty_string(client):
    """PUT /api/tasks/{id} can set title to empty string."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Has Title", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    resp = await client.put(f"/api/tasks/{task_id}", json={"title": ""})
    assert resp.status_code == 200
    assert resp.json()["title"] == ""


@pytest.mark.asyncio
async def test_update_task_title_persisted_in_get(client):
    """Updated title is returned on subsequent GET."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Old", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    await client.put(f"/api/tasks/{task_id}", json={"title": "Renamed"})
    resp = await client.get(f"/api/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["title"] == "Renamed"


# === Effort level tests ===


@pytest.mark.asyncio
async def test_create_task_with_effort_level(client):
    """Task created with effort_level field stores and returns it."""
    resp = await client.post("/api/tasks", json={
        "title": "Effort task", "description": "d", "target_repo": "/tmp", "effort_level": "high",
    })
    assert resp.status_code == 201
    assert resp.json()["effort_level"] == "high"


@pytest.mark.asyncio
async def test_create_task_without_effort_level_fills_default(client):
    """设置归 Task：不指定 effort 时创建即填入全局默认值。"""
    from backend.config import settings
    resp = await client.post("/api/tasks", json={
        "title": "No effort", "description": "d", "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    assert resp.json()["effort_level"] == settings.default_effort


@pytest.mark.asyncio
async def test_create_task_effort_level_persisted_in_get(client):
    """effort_level survives a round-trip through GET."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp", "effort_level": "max",
    })
    task_id = create_resp.json()["id"]

    resp = await client.get(f"/api/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["effort_level"] == "max"


@pytest.mark.asyncio
async def test_create_task_effort_level_in_list(client):
    """effort_level is included when listing tasks."""
    await client.post("/api/tasks", json={
        "title": "A", "description": "d", "target_repo": "/tmp", "effort_level": "low",
    })
    resp = await client.get("/api/tasks")
    assert resp.status_code == 200
    tasks = resp.json()
    assert tasks[0]["effort_level"] == "low"


# === Goal mode tests ===


@pytest.mark.asyncio
async def test_create_goal_task(client):
    """Goal task with condition is created successfully."""
    resp = await client.post("/api/tasks", json={
        "title": "Goal Task",
        "description": "implement feature",
        "mode": "goal",
        "goal_condition": "all tests pass",
        "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["mode"] == "goal"
    assert data["goal_condition"] == "all tests pass"
    assert data["goal_max_turns"] == 30
    assert data["goal_turns_used"] == 0
    assert data["goal_last_reason"] is None


@pytest.mark.asyncio
async def test_create_frontend_review_goal_builds_internal_condition(client):
    resp = await client.post("/api/tasks", json={
        "title": "Frontend Review Goal",
        "description": "审查 http://127.0.0.1:5173，修复后重新验证",
        "target_repo": "/tmp",
        "frontend_review": {
            "mode": "goal",
            "profile": "standard",
            "max_iterations": 5,
        },
    })

    assert resp.status_code == 201
    data = resp.json()
    assert data["mode"] == "goal"
    assert data["goal_max_turns"] == 5
    assert "Browser Review" in data["goal_condition"]
    assert data["metadata_"]["frontend_review"] == {
        "mode": "goal",
        "profile": "standard",
        "max_iterations": 5,
    }


@pytest.mark.asyncio
async def test_create_frontend_review_goal_rejects_excessive_iterations(client):
    resp = await client.post("/api/tasks", json={
        "title": "Unbounded Frontend Review Goal",
        "description": "review it",
        "target_repo": "/tmp",
        "frontend_review": {
            "mode": "goal",
            "profile": "standard",
            "max_iterations": 11,
        },
    })

    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_goal_task_custom_max_turns(client):
    """Goal task with custom max_turns stores it correctly."""
    resp = await client.post("/api/tasks", json={
        "title": "Goal Custom",
        "description": "do it",
        "mode": "goal",
        "goal_condition": "lint clean",
        "goal_max_turns": 15,
        "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    assert resp.json()["goal_max_turns"] == 15


@pytest.mark.asyncio
async def test_create_goal_task_requires_condition(client):
    """Goal task without goal_condition returns 422."""
    resp = await client.post("/api/tasks", json={
        "title": "No Condition",
        "description": "do it",
        "mode": "goal",
        "target_repo": "/tmp",
    })
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_create_goal_task_with_evaluator_model(client):
    """Goal task with custom evaluator model stores it."""
    resp = await client.post("/api/tasks", json={
        "title": "Goal Eval",
        "description": "do it",
        "mode": "goal",
        "goal_condition": "condition",
        "goal_evaluator_model": "claude-sonnet-4-6",
        "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    assert resp.json()["goal_evaluator_model"] == "claude-sonnet-4-6"


@pytest.mark.asyncio
async def test_goal_fields_persisted_in_db(client, session_factory):
    """Goal fields are actually persisted to the database."""
    from backend.models.task import Task

    resp = await client.post("/api/tasks", json={
        "title": "Persist Goal",
        "description": "do it",
        "mode": "goal",
        "goal_condition": "all green",
        "goal_max_turns": 20,
        "target_repo": "/tmp",
    })
    task_id = resp.json()["id"]

    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.goal_condition == "all green"
    assert task.goal_max_turns == 20
    assert task.goal_turns_used == 0


@pytest.mark.asyncio
async def test_goal_fields_in_get_response(client):
    """Goal fields are returned in GET /api/tasks/{id}."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T",
        "description": "d",
        "mode": "goal",
        "goal_condition": "tests pass",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    resp = await client.get(f"/api/tasks/{task_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["goal_condition"] == "tests pass"
    assert data["goal_max_turns"] == 30
    assert data["goal_turns_used"] == 0


@pytest.mark.asyncio
async def test_update_goal_task_fields(client):
    """PUT /api/tasks/{id} can update goal-specific fields."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Goal Update",
        "description": "d",
        "mode": "goal",
        "goal_condition": "old condition",
        "goal_max_turns": 10,
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    resp = await client.put(f"/api/tasks/{task_id}", json={
        "goal_condition": "new condition",
        "goal_max_turns": 50,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["goal_condition"] == "new condition"
    assert data["goal_max_turns"] == 50


@pytest.mark.asyncio
async def test_non_goal_task_has_null_goal_fields(client):
    """Auto-mode task has null goal fields."""
    resp = await client.post("/api/tasks", json={
        "title": "Auto", "description": "d", "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["goal_condition"] is None
    assert data["goal_max_turns"] == 30
    assert data["goal_turns_used"] == 0
    assert data["goal_last_reason"] is None


# === Cancel task kills process tests ===


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "terminal_status"),
    (
        pytest.param("stop-session", "completed", id="stop-session"),
        pytest.param("cancel", "cancelled", id="cancel"),
    ),
)
async def test_tracked_pty_background_terminal_request_clears_marker(
    client,
    session_factory,
    endpoint,
    terminal_status,
):
    """An exact live owner lets terminal CAS retire its PTY marker."""

    import backend.main
    from backend.models.instance import Instance
    from backend.models.task import Task

    created = await client.post("/api/tasks", json={
        "title": f"Tracked background {endpoint}",
        "description": "d",
    })
    task_id = created.json()["id"]
    started_at = datetime(2026, 7, 28, 1, 2, 3)
    async with session_factory() as db:
        instance = Instance(
            name=f"tracked-background-{endpoint}",
            status="running",
            pid=41001,
            started_at=started_at,
            current_task_id=task_id,
        )
        db.add(instance)
        await db.flush()
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="executing",
                instance_id=instance.id,
                started_at=started_at,
                session_id=f"tracked-session-{endpoint}",
                pty_background_generation=f"tracked-generation-{endpoint}",
            )
        )
        await db.commit()
        instance_id = instance.id

    async def stop_exact(
        stopped_task_id,
        _db,
        *,
        expected_generations,
        expected_task_turn_generation,
        task_status,
        worker_termination_operation_id,
    ):
        assert stopped_task_id == task_id
        assert expected_task_turn_generation == 0
        assert task_status == terminal_status
        assert worker_termination_operation_id is None
        assert expected_generations == [
            (instance_id, 41001, started_at)
        ]
        async with session_factory() as db:
            instance = await db.get(Instance, instance_id)
            instance.status = "idle"
            instance.pid = None
            instance.current_task_id = None
            task = await db.get(Task, task_id)
            task.status = terminal_status
            task.completed_at = datetime.utcnow()
            task.pty_background_generation = None
            await db.commit()
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(
            task_id,
            terminal_status,
            background_active=False,
        )
        return True

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch.object(
            backend.main.instance_manager,
            "wait_for_task_launch_barrier",
            new_callable=AsyncMock,
            return_value=True,
        ),
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            side_effect=stop_exact,
        ),
        patch(
            "backend.services.task_events.broadcast_status_change",
            new_callable=AsyncMock,
        ) as publish,
    ):
        response = await client.post(f"/api/tasks/{task_id}/{endpoint}")

    assert response.status_code == 200, response.text
    publish.assert_awaited_once_with(
        task_id,
        terminal_status,
        background_active=False,
    )
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == terminal_status
        assert task.pty_background_generation is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "terminal_status"),
    (
        pytest.param("stop-session", "completed", id="stop-session"),
        pytest.param("cancel", "cancelled", id="cancel"),
    ),
)
async def test_owner_stop_preserves_new_background_generation(
    client,
    session_factory,
    endpoint,
    terminal_status,
):
    """A post-stop same-Task epoch must never be mistaken for the old tail."""

    import backend.main
    from backend.models.instance import Instance
    from backend.models.task import Task

    created = await client.post("/api/tasks", json={
        "title": f"Owner ABA {endpoint}",
        "description": "d",
    })
    task_id = created.json()["id"]
    started_at = datetime(2026, 7, 28, 4, 5, 6)
    new_generation = f"new-owner-background-{endpoint}"
    async with session_factory() as db:
        instance = Instance(
            name=f"owner-aba-{endpoint}",
            status="running",
            pid=42001,
            started_at=started_at,
            current_task_id=task_id,
        )
        db.add(instance)
        await db.flush()
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="executing",
                instance_id=instance.id,
                started_at=started_at,
                session_id=f"owner-aba-session-{endpoint}",
            )
        )
        await db.commit()
        instance_id = instance.id

    async def stop_then_new_epoch(
        _task_id,
        _db,
        *,
        expected_generations,
        expected_task_turn_generation,
        task_status,
        worker_termination_operation_id,
    ):
        assert _task_id == task_id
        assert expected_task_turn_generation == 0
        assert task_status == terminal_status
        assert worker_termination_operation_id is None
        assert expected_generations == [
            (instance_id, 42001, started_at)
        ]
        async with session_factory() as db:
            instance = await db.get(Instance, instance_id)
            instance.status = "idle"
            instance.pid = None
            instance.current_task_id = None
            task = await db.get(Task, task_id)
            task.status = terminal_status
            task.completed_at = datetime.utcnow()
            # Models a newer same-task turn completing and arming after the
            # old Instance stop fence is released.
            task.pty_background_generation = new_generation
            await db.commit()
        return True

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            side_effect=stop_then_new_epoch,
        ),
        patch(
            "backend.services.task_events.broadcast_status_change",
            new_callable=AsyncMock,
        ) as publish,
    ):
        response = await client.post(f"/api/tasks/{task_id}/{endpoint}")

    assert response.status_code == 409
    assert "newer PTY background generation" in response.json()["detail"]
    publish.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == terminal_status
        assert task.pty_background_generation == new_generation


@pytest.mark.asyncio
async def test_stop_session_stops_ownerless_pty_background_generation(
    client,
    session_factory,
):
    """Late output is stopped by Task/session token, not historical Instance."""

    import backend.main
    from backend.models.task import Task

    created = await client.post("/api/tasks", json={
        "title": "Detached background stop",
        "description": "d",
    })
    task_id = created.json()["id"]
    session_id = "detached-api-session"
    generation = "detached-api-generation"
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="completed",
                session_id=session_id,
                pty_background_generation=generation,
            )
        )
        await db.commit()

    async def settle_exact(
        settled_task_id,
        settled_session_id,
        settled_generation,
        **expected,
    ):
        assert (
            settled_task_id,
            settled_session_id,
            settled_generation,
        ) == (task_id, session_id, generation)
        assert expected["expected_status"] == "completed"
        assert expected["expected_turn_generation"] == 0
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(
                    Task.id == task_id,
                    Task.pty_background_generation == generation,
                )
                .values(pty_background_generation=None)
            )
            await db.commit()
        return True

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            return_value=False,
        ),
        patch.object(
            backend.main.instance_manager,
            "stop_detached_pty_background_generation",
            new_callable=AsyncMock,
            side_effect=settle_exact,
        ) as stop_detached,
        patch(
            "backend.services.task_events.broadcast_status_change",
            new_callable=AsyncMock,
        ) as publish,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/stop-session"
        )

    assert response.status_code == 200, response.text
    assert response.json()["stopped"] is True
    stop_detached.assert_awaited_once()
    publish.assert_awaited_once_with(
        task_id,
        "completed",
        background_active=False,
    )
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.pty_background_generation is None


@pytest.mark.asyncio
async def test_cancel_ownerless_pty_background_requires_stop_session(
    client,
    session_factory,
):
    """Cancel cannot silently retire a detached Session it does not stop."""

    import backend.main
    from backend.models.task import Task

    created = await client.post("/api/tasks", json={
        "title": "Detached background cancel failure",
        "description": "d",
    })
    task_id = created.json()["id"]
    generation = "detached-cancel-failure"
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="completed",
                session_id="detached-cancel-session",
                pty_background_generation=generation,
            )
        )
        await db.commit()

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            return_value=False,
        ),
        patch.object(
            backend.main.instance_manager,
            "stop_detached_pty_background_generation",
            new_callable=AsyncMock,
            return_value=False,
        ) as stop_detached,
    ):
        response = await client.post(f"/api/tasks/{task_id}/cancel")

    assert response.status_code == 400
    stop_detached.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "completed"
        assert task.pty_background_generation == generation


@pytest.mark.asyncio
async def test_ownerless_pending_cancel_does_not_infer_process(
    client, session_factory
):
    """A pending Task without an exact owner needs no process stop."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Cancel Me", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    with patch("backend.api.tasks._stop_task_process", new_callable=AsyncMock, return_value=False) as mock_stop:
        resp = await client.post(f"/api/tasks/{task_id}/cancel")

    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"
    mock_stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_task_still_works_if_no_process(client):
    """Cancel works even when no process is running (stop returns False)."""
    create_resp = await client.post("/api/tasks", json={
        "title": "No Process", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    with patch("backend.api.tasks._stop_task_process", new_callable=AsyncMock, return_value=False):
        resp = await client.post(f"/api/tasks/{task_id}/cancel")

    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"


@pytest.mark.asyncio
async def test_cancel_returns_409_when_queue_worker_does_not_settle(
    client, session_factory
):
    from backend.models.task import Task
    from backend.services.dispatcher import TaskQueueAbortTimeoutError
    import backend.main

    create_resp = await client.post("/api/tasks", json={
        "title": "Stubborn queue", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
        side_effect=TaskQueueAbortTimeoutError("still active"),
    ):
        resp = await client.post(f"/api/tasks/{task_id}/cancel")

    assert resp.status_code == 409
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "initial_status"),
    (("cancel", "pending"), ("stop-session", "executing")),
)
async def test_terminal_generation_uses_database_normalized_completed_at(
    client,
    session_factory,
    endpoint,
    initial_status,
):
    """MySQL DATETIME may truncate Python microseconds before postcheck."""

    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Timestamp fence",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    if initial_status != "pending":
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(status=initial_status)
            )
            await db.commit()

    normalized = datetime(2026, 7, 23, 12, 34, 56)
    observed_postcheck: dict = {}

    async def capture_postcheck(locked_task_id, db, **kwargs):
        assert locked_task_id == task_id
        observed_postcheck.update(kwargs)
        db.expire_all()
        return await db.get(Task, task_id)

    with (
        patch(
            "backend.api.tasks._read_persisted_task_completed_at",
            new_callable=AsyncMock,
            return_value=normalized,
        ) as read_persisted,
        patch(
            "backend.api.tasks._lock_task_generation",
            side_effect=capture_postcheck,
        ),
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            return_value=False,
        ),
    ):
        response = await client.post(f"/api/tasks/{task_id}/{endpoint}")

    assert response.status_code == 200, response.text
    read_persisted.assert_awaited_once()
    assert observed_postcheck["expected_completed_at"] == normalized


@pytest.mark.asyncio
async def test_ownerless_stop_drops_stale_completed_publication_after_retry(
    client,
    session_factory,
):
    """A retry committed after stop cannot receive the old completed event."""

    import backend.main
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Stop publication retry race",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="executing")
        )
        await db.commit()

    async def retry_before_publication(*_args, **_kwargs):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(
                    status="pending",
                    retry_count=Task.retry_count + 1,
                    completed_at=None,
                )
            )
            await db.commit()
        return None

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch(
            "backend.api.tasks._lock_task_generation",
            new_callable=AsyncMock,
            side_effect=retry_before_publication,
        ),
        patch(
            "backend.services.task_events.broadcast_status_change",
            new_callable=AsyncMock,
        ) as publish,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/stop-session"
        )

    assert response.status_code == 409
    publish.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "pending"
        assert task.retry_count == 1


@pytest.mark.asyncio
async def test_detached_stop_drops_false_event_if_new_background_rearms(
    client,
    session_factory,
):
    """A new exact PTY epoch suppresses the old background=false event."""

    import backend.main
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Detached publication rearm race",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    old_generation = "old-detached-generation"
    new_generation = "new-detached-generation"
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="completed",
                completed_at=datetime(2026, 7, 28, 2, 3, 4),
                session_id="detached-publication-session",
                pty_background_generation=old_generation,
            )
        )
        await db.commit()

    async def stop_old_generation(*_args, **_kwargs):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(
                    Task.id == task_id,
                    Task.pty_background_generation == old_generation,
                )
                .values(pty_background_generation=None)
            )
            await db.commit()
        return True

    async def rearm_before_publication(*_args, **_kwargs):
        async with session_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(pty_background_generation=new_generation)
            )
            await db.commit()
            return await db.get(Task, task_id)

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch.object(
            backend.main.instance_manager,
            "stop_detached_pty_background_generation",
            new_callable=AsyncMock,
            side_effect=stop_old_generation,
        ),
        patch(
            "backend.api.tasks._lock_task_generation",
            new_callable=AsyncMock,
            side_effect=rearm_before_publication,
        ),
        patch(
            "backend.services.task_events.broadcast_status_change",
            new_callable=AsyncMock,
        ) as publish,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/stop-session"
        )

    assert response.status_code == 409
    publish.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "completed"
        assert task.pty_background_generation == new_generation


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "terminal_status"),
    (
        pytest.param("stop-session", "completed", id="stop-session"),
        pytest.param("cancel", "cancelled", id="cancel"),
    ),
)
async def test_terminal_request_cancellation_before_first_commit_still_reaps(
    client,
    session_factory,
    endpoint,
    terminal_status,
):
    """A disconnected caller cannot strand a terminal Task with a live owner."""

    import backend.main
    from backend.models.instance import Instance
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": f"Cancel-safe {endpoint}",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    started_at = datetime(2026, 7, 23, 13, 14, 15)
    async with session_factory() as db:
        instance = Instance(
            name=f"cancel-safe-{endpoint}",
            status="running",
            pid=54101,
            current_task_id=task_id,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="executing",
                instance_id=instance.id,
                started_at=started_at,
            )
        )
        await db.commit()
        instance_id = instance.id

    stop_entered = asyncio.Event()
    allow_stop = asyncio.Event()

    async def stop_exact(
        stopped_task_id,
        _db,
        *,
        expected_generations,
        expected_task_turn_generation,
        task_status,
        worker_termination_operation_id,
    ):
        assert stopped_task_id == task_id
        assert expected_task_turn_generation == 0
        assert task_status == terminal_status
        assert worker_termination_operation_id is None
        assert [
            (owner_id, pid, owner_started_at)
            for owner_id, pid, owner_started_at in expected_generations
        ] == [(instance_id, 54101, started_at)]
        stop_entered.set()
        await allow_stop.wait()
        async with session_factory() as db:
            owner = await db.get(Instance, instance_id)
            owner.status = "idle"
            owner.pid = None
            owner.current_task_id = None
            task = await db.get(Task, task_id)
            task.status = terminal_status
            task.completed_at = datetime.utcnow()
            task.pty_background_generation = None
            await db.commit()
        from backend.services.task_events import broadcast_status_change

        await broadcast_status_change(
            task_id,
            terminal_status,
            background_active=False,
        )
        return True

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch.object(
            backend.main.instance_manager,
            "wait_for_task_launch_barrier",
            new_callable=AsyncMock,
            return_value=True,
        ),
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            side_effect=stop_exact,
        ) as stop,
        patch(
            "backend.services.task_events.broadcast_status_change",
            new_callable=AsyncMock,
        ) as publish,
    ):
        request = asyncio.create_task(
            client.post(f"/api/tasks/{task_id}/{endpoint}")
        )
        await stop_entered.wait()
        request.cancel()
        await asyncio.sleep(0)
        assert not request.done()
        allow_stop.set()
        with pytest.raises(asyncio.CancelledError):
            await request

    stop.assert_awaited_once()
    publish.assert_awaited_once_with(
        task_id,
        terminal_status,
        background_active=False,
    )
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == terminal_status
        assert instance.status == "idle"
        assert instance.pid is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_pending_stop_session_does_not_infer_process(
    client, session_factory
):
    """A pending Task with no exact owner has no running session to stop."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Stop Me", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    with patch(
        "backend.api.tasks._stop_task_process",
        new_callable=AsyncMock,
        return_value=True,
    ) as mock_stop:
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 400
    mock_stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_session_409_when_hidden_launch_cannot_be_proven_reaped(
    client, session_factory
):
    from backend.models.instance import Instance
    from backend.models.task import Task
    import backend.main

    create_resp = await client.post("/api/tasks", json={
        "title": "Hidden launch", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        instance = Instance(name="hidden-launch", status="idle")
        db.add(instance)
        await db.flush()
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="executing", instance_id=instance.id)
        )
        await db.commit()
        instance_id = instance.id

    with (
        patch.object(
            backend.main.instance_manager,
            "wait_for_task_launch_barrier",
            new_callable=AsyncMock,
            return_value=False,
        ) as barrier,
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            return_value=False,
        ),
    ):
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 409
    barrier.assert_awaited_once_with(instance_id, task_id)
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "executing"


@pytest.mark.asyncio
async def test_stop_session_no_process_returns_400(client):
    """POST /api/tasks/{id}/stop-session returns 400 when no process found."""
    create_resp = await client.post("/api/tasks", json={
        "title": "No Session", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    with patch("backend.api.tasks._stop_task_process", new_callable=AsyncMock, return_value=False):
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_stop_session_reports_unresolved_exact_owner(
    client,
    session_factory,
):
    """Terminal status must not masquerade as successful process cleanup."""

    from backend.models.instance import Instance
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Unresolved stop",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = Instance(
            name="unresolved-stop-slot",
            status="error",
            pid=45678,
            current_task_id=task_id,
        )
        db.add(instance)
        await db.flush()
        task.status = "executing"
        task.instance_id = instance.id
        await db.commit()
        instance_id = instance.id

    with patch(
        "backend.api.tasks._stop_task_process",
        new_callable=AsyncMock,
        return_value=False,
    ):
        response = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert response.status_code == 409
    assert "cleanup could not be confirmed" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "executing"
        assert instance.pid == 45678
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_stop_session_shared_codex_preflight_preserves_queue(
    client,
    session_factory,
):
    """A known shared-transport conflict has no queue-side effects."""

    import backend.main
    from backend.models.instance import Instance
    from backend.models.task import Task
    from backend.services.codex_app_server import CodexSharedTransportBusyError

    create_resp = await client.post("/api/tasks", json={
        "title": "Shared Codex stop",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = Instance(
            name="shared-codex-slot",
            status="running",
            pid=45679,
            current_task_id=task_id,
        )
        db.add(instance)
        await db.flush()
        task.status = "executing"
        task.instance_id = instance.id
        await db.commit()

    with patch.object(
        backend.main.instance_manager,
        "require_stop_session_preflight",
        new_callable=AsyncMock,
        side_effect=CodexSharedTransportBusyError("live peer"),
    ), patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
    ) as abort_queue:
        response = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert response.status_code == 409
    assert "no queued messages" in response.json()["detail"]
    abort_queue.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "executing"


@pytest.mark.asyncio
async def test_stop_session_codex_retries_final_db_failure_without_stopping_peer(
    client,
    session_factory,
):
    """A DB failure after native stop retries without touching a shared peer."""

    import backend.main
    import sqlite3

    from backend.models.delivery import DeliveryRun
    from backend.models.instance import Instance
    from backend.models.project import Project
    from backend.models.task import Task
    from backend.services import instance_manager as instance_manager_module
    from backend.services.codex_app_server import (
        CodexSharedTransportBusyError,
        CodexTurnProcess,
    )
    from sqlalchemy.exc import OperationalError

    shared_pid = 54_321
    target_started_at = datetime(2026, 8, 17, 10, 0, 0)
    peer_started_at = datetime(2026, 8, 17, 10, 0, 1)
    async with session_factory() as db:
        project = Project(
            name="api-stop-delivery-peer",
            local_path="/tmp/api-stop-delivery-peer",
            status="ready",
        )
        db.add(project)
        await db.flush()
        delivery_run = DeliveryRun(
            admission_scope="test:api-stop",
            idempotency_key="delivery-peer",
            request_hash="a" * 64,
            project_id=project.id,
            title="API stop Delivery peer",
            requirements="Remain untouched",
            requirements_hash="b" * 64,
            policy_snapshot={"provider": "codex"},
            policy_hash="c" * 64,
            base_branch="main",
            delivery_branch="ccm/delivery/api-stop-peer",
            workspace_path="/tmp/api-stop-delivery-peer",
            phase="coding",
            activity="running",
            turn_count=1,
            max_cycles=4,
            max_no_progress=2,
        )
        target_instance = Instance(
            name="api-stop-target",
            status="running",
            provider="codex",
            pid=shared_pid,
            started_at=target_started_at,
        )
        peer_instance = Instance(
            name="api-stop-delivery-peer",
            status="running",
            provider="codex",
            pid=shared_pid,
            started_at=peer_started_at,
        )
        db.add_all([delivery_run, target_instance, peer_instance])
        await db.flush()
        target_task = Task(
            title="API scoped stop target",
            status="executing",
            provider="codex",
            instance_id=target_instance.id,
        )
        peer_task = Task(
            title="API shared Delivery peer",
            status="executing",
            provider="codex",
            instance_id=peer_instance.id,
            project_id=project.id,
            target_repo="/tmp/api-stop-delivery-peer",
            mode="delivery_loop",
            delivery_run_id=delivery_run.id,
            delivery_role="developer",
        )
        db.add_all([target_task, peer_task])
        await db.flush()
        target_instance.current_task_id = target_task.id
        peer_instance.current_task_id = peer_task.id
        delivery_run.developer_task_id = peer_task.id
        await db.commit()
        target_instance_id = target_instance.id
        peer_instance_id = peer_instance.id
        target_task_id = target_task.id
        peer_task_id = peer_task.id
        delivery_run_id = delivery_run.id

    async def interrupt():
        raise AssertionError("registry owns the exact Codex interrupt")

    target_process = CodexTurnProcess(
        shared_pid,
        interrupt,
        thread_id="thread-api-stop-target",
    )
    peer_process = CodexTurnProcess(
        shared_pid,
        interrupt,
        thread_id="thread-api-stop-delivery-peer",
    )
    target_release = asyncio.Event()
    peer_release = asyncio.Event()
    target_consumer = asyncio.create_task(target_release.wait())
    peer_consumer = asyncio.create_task(peer_release.wait())
    registry = MagicMock()

    async def require_claimed_turn_stop_isolated(_home, exact_process):
        if registry.require_claimed_turn_stop_isolated.await_count > 1:
            raise CodexSharedTransportBusyError(
                "retry must use the retained terminal recovery receipt"
            )
        assert exact_process is target_process

    registry.require_claimed_turn_stop_isolated = AsyncMock(
        side_effect=require_claimed_turn_stop_isolated,
    )

    async def stop_target(_home, exact_process, *, reason):
        assert exact_process is target_process
        target_process.finish(
            130,
            reason,
            termination_kind="internal_abort",
        )
        return False

    registry.stop_claimed_turn = AsyncMock(side_effect=stop_target)
    manager = backend.main.instance_manager
    previous_registry = manager._codex_app_server
    previous_db_factory = manager.db_factory
    manager.db_factory = session_factory
    manager._codex_app_server = registry
    manager._config_dirs[target_instance_id] = "/tmp/api-stop-shared-home"
    manager._config_dirs[peer_instance_id] = "/tmp/api-stop-shared-home"
    manager.processes[target_instance_id] = target_process
    manager.processes[peer_instance_id] = peer_process
    manager._track_output_consumer(
        target_instance_id,
        target_process,
        target_consumer,
        provider="codex",
        task_id=target_task_id,
        task_retry_count=0,
        task_turn_generation=0,
        instance_started_at=target_started_at,
    )
    manager._track_output_consumer(
        peer_instance_id,
        peer_process,
        peer_consumer,
        provider="codex",
        task_id=peer_task_id,
        task_retry_count=0,
        task_turn_generation=0,
        instance_started_at=peer_started_at,
    )

    real_lock_stop_authority = (
        instance_manager_module._lock_worker_termination_stop_authority
    )
    failed_final_stop = False

    async def fail_first_final_stop(*args, **kwargs):
        nonlocal failed_final_stop
        if (
            not failed_final_stop
            and kwargs.get("task_id") == target_task_id
            and kwargs.get("instance_id") == target_instance_id
            and target_process.returncode is not None
            and target_consumer.done()
        ):
            failed_final_stop = True
            raise OperationalError(
                "UPDATE tasks SET status=tasks.status",
                (target_task_id, target_instance_id, 0),
                sqlite3.OperationalError("database is locked"),
            )
        return await real_lock_stop_authority(*args, **kwargs)

    try:
        with patch.object(
            instance_manager_module,
            "_lock_worker_termination_stop_authority",
            side_effect=fail_first_final_stop,
        ):
            with pytest.raises(OperationalError, match="database is locked"):
                await client.post(
                    f"/api/tasks/{target_task_id}/stop-session"
                )

            recovery_key = (target_instance_id, target_process)
            assert failed_final_stop is True
            assert recovery_key in manager._consumer_recovery_pending
            evidence = manager._consumer_recovery_pending[recovery_key]
            assert evidence.consumer is target_consumer
            assert evidence.record is not None
            assert evidence.record.process is target_process
            assert evidence.record.task is target_consumer
            assert target_instance_id not in manager.processes
            assert target_instance_id not in manager._tasks
            assert target_instance_id not in manager._consumer_records
            assert target_process.returncode == 130
            assert peer_process.returncode is None
            assert not peer_consumer.done()
            assert manager.processes[peer_instance_id] is peer_process

            async with session_factory() as db:
                unsettled_target = await db.get(Task, target_task_id)
                unsettled_instance = await db.get(
                    Instance,
                    target_instance_id,
                )
                assert unsettled_target.status == "executing"
                assert unsettled_target.instance_id == target_instance_id
                assert unsettled_instance.status == "running"
                assert unsettled_instance.pid == shared_pid
                assert unsettled_instance.current_task_id == target_task_id

            response = await client.post(
                f"/api/tasks/{target_task_id}/stop-session"
            )
        assert response.status_code == 200, response.text
        assert registry.require_claimed_turn_stop_isolated.await_count == 1
        assert (
            registry.require_claimed_turn_stop_isolated.await_args_list[0].args
            == ("/tmp/api-stop-shared-home", target_process)
        )
        registry.stop_claimed_turn.assert_awaited_once_with(
            "/tmp/api-stop-shared-home",
            target_process,
            reason="CCM task session interrupted",
        )
        assert target_process.returncode == 130
        assert peer_process.returncode is None
        assert not peer_consumer.done()
        assert manager.processes[peer_instance_id] is peer_process
        assert recovery_key not in manager._consumer_recovery_pending
        assert recovery_key not in manager._consumer_errors
        assert target_instance_id not in manager.processes
        assert target_instance_id not in manager._tasks
        assert target_instance_id not in manager._consumer_records

        async with session_factory() as db:
            durable_target = await db.get(Task, target_task_id)
            durable_target_instance = await db.get(
                Instance,
                target_instance_id,
            )
            durable_peer = await db.get(Task, peer_task_id)
            durable_peer_instance = await db.get(Instance, peer_instance_id)
            durable_run = await db.get(DeliveryRun, delivery_run_id)
            assert durable_target.status == "completed"
            assert durable_target.instance_id == target_instance_id
            assert durable_target_instance.status == "idle"
            assert durable_target_instance.pid is None
            assert durable_target_instance.current_task_id is None
            assert durable_peer.status == "executing"
            assert durable_peer.instance_id == peer_instance_id
            assert durable_peer_instance.status == "running"
            assert durable_peer_instance.pid == shared_pid
            assert durable_peer_instance.current_task_id == peer_task_id
            assert durable_run.phase == "coding"
            assert durable_run.activity == "running"
            assert durable_run.developer_task_id == peer_task_id
    finally:
        target_release.set()
        peer_release.set()
        await asyncio.gather(
            target_consumer,
            peer_consumer,
            return_exceptions=True,
        )
        manager._codex_app_server = previous_registry
        manager.db_factory = previous_db_factory
        for instance_id in (target_instance_id, peer_instance_id):
            manager.processes.pop(instance_id, None)
            manager._tasks.pop(instance_id, None)
            manager._consumer_records.pop(instance_id, None)
            manager._config_dirs.pop(instance_id, None)
        manager._consumer_recovery_pending.pop(
            (target_instance_id, target_process),
            None,
        )
        manager._consumer_errors.pop(
            (target_instance_id, target_process),
            None,
        )


@pytest.mark.asyncio
async def test_cancel_reports_unresolved_exact_owner(
    client,
    session_factory,
):
    """Cancellation remains fail-closed while its exact process owner exists."""

    from backend.models.instance import Instance
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Unresolved cancel",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = Instance(
            name="unresolved-cancel-slot",
            status="error",
            pid=45679,
            current_task_id=task_id,
        )
        db.add(instance)
        await db.flush()
        task.status = "executing"
        task.instance_id = instance.id
        await db.commit()
        instance_id = instance.id

    with patch(
        "backend.api.tasks._stop_task_process",
        new_callable=AsyncMock,
        return_value=False,
    ):
        response = await client.post(f"/api/tasks/{task_id}/cancel")

    assert response.status_code == 409
    assert "cleanup could not be confirmed" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "executing"
        assert instance.pid == 45679
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_cancel_retries_cancelled_auxiliary_cleanup(
    client,
    session_factory,
):
    """A failed auxiliary reap remains reachable through a repeated cancel."""

    import backend.main
    from backend.models.monitor_session import MonitorSession
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Retry auxiliary cancel",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        monitor = MonitorSession(
            task_id=task_id,
            agent_type="monitor",
            source="ccm",
            description="retained process",
            status="running",
        )
        db.add(monitor)
        await db.commit()
        monitor_id = monitor.id

    attempts = 0
    terminal_values = []

    async def fail_once(session_id, *, terminal=False):
        nonlocal attempts
        assert session_id == monitor_id
        terminal_values.append(terminal)
        attempts += 1
        if attempts == 1:
            raise RuntimeError("process group still alive")

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch.object(
            backend.main.dispatcher,
            "stop_monitor_session_process",
            new_callable=AsyncMock,
            side_effect=fail_once,
        ),
        patch(
            "backend.api.tasks._stop_task_process",
            new_callable=AsyncMock,
            return_value=False,
        ),
    ):
        first = await client.post(f"/api/tasks/{task_id}/cancel")
        assert first.status_code == 409

        async with session_factory() as db:
            task = await db.get(Task, task_id)
            monitor = await db.get(MonitorSession, monitor_id)
            assert task.status == "cancelled"
            assert monitor.status == "cancelled"

        second = await client.post(f"/api/tasks/{task_id}/cancel")

    assert second.status_code == 200, second.text
    assert second.json()["status"] == "cancelled"
    assert attempts == 2
    assert terminal_values == [True, True]


@pytest.mark.asyncio
async def test_stop_helper_never_uses_historical_recycled_instance(
    session_factory,
):
    """Stopping old Task A must not stop slot now owned by Task B."""
    from backend.api.tasks import _stop_task_process
    from backend.models.instance import Instance
    from backend.models.task import Task
    import backend.main

    async with session_factory() as db:
        old_task = Task(title="old", description="d", status="completed")
        new_task = Task(title="new", description="d", status="executing")
        db.add_all([old_task, new_task])
        await db.flush()
        inst = Instance(
            name="reused",
            status="running",
            current_task_id=new_task.id,
        )
        db.add(inst)
        await db.flush()
        old_task.instance_id = inst.id
        new_task.instance_id = inst.id
        await db.commit()

        with patch.object(
            backend.main.instance_manager,
            "stop",
            new_callable=AsyncMock,
            return_value=True,
        ) as stop:
            assert await _stop_task_process(
                old_task.id,
                db,
                expected_generations=[],
                expected_task_turn_generation=old_task.turn_generation,
            ) is False
            stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_helper_rechecks_live_owner_inside_manager_lock(
    session_factory,
):
    from backend.api.tasks import _stop_task_process
    from backend.models.instance import Instance
    from backend.models.task import Task
    import backend.main

    async with session_factory() as db:
        task = Task(title="live", description="d", status="executing")
        db.add(task)
        await db.flush()
        inst = Instance(
            name="owned",
            status="running",
            current_task_id=task.id,
        )
        db.add(inst)
        await db.commit()
        task_id = task.id
        task_turn_generation = task.turn_generation
        instance_id = inst.id

        with patch.object(
            backend.main.instance_manager,
            "stop",
            new_callable=AsyncMock,
            return_value=True,
        ) as stop:
            assert await _stop_task_process(
                task_id,
                db,
                expected_generations=[(instance_id, None, None)],
                expected_task_turn_generation=task_turn_generation,
            ) is True
            stop.assert_awaited_once_with(
                instance_id,
                expected_task_id=task_id,
                expected_task_turn_generation=task_turn_generation,
                expected_pid=None,
                expected_started_at=None,
                task_status="completed",
                terminal_consumer_timeout=30.0,
                consumer_cancel_timeout=10.0,
                yield_to_worker_task_termination=True,
            )


@pytest.mark.asyncio
async def test_stop_helper_reconciles_an_exact_dead_reverse_owner(
    session_factory,
):
    from backend.api.tasks import _stop_task_process
    from backend.models.instance import Instance
    from backend.models.task import Task
    import backend.main

    started_at = datetime(2026, 8, 2, 7, 36, 23)
    manager = backend.main.instance_manager
    async with session_factory() as db:
        task = Task(title="orphan owner", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="dead reverse owner",
            status="running",
            pid=145_0775,
            current_task_id=task.id,
            started_at=started_at,
        )
        db.add(instance)
        await db.commit()
        task_id, instance_id = task.id, instance.id
        task_turn_generation = task.turn_generation

        with (
            patch.object(
                manager,
                "stop",
                new_callable=AsyncMock,
                return_value=False,
            ) as stop,
            patch.object(
                manager,
                "reconcile_dead_reverse_task_owner",
                new_callable=AsyncMock,
                return_value=True,
            ) as reconcile,
        ):
            assert await _stop_task_process(
                task_id,
                db,
                expected_generations=[(
                    instance_id,
                    145_0775,
                    started_at,
                )],
                expected_task_turn_generation=task_turn_generation,
            ) is True

    stop.assert_awaited_once_with(
        instance_id,
        expected_task_id=task_id,
        expected_task_turn_generation=task_turn_generation,
        expected_pid=145_0775,
        expected_started_at=started_at,
        task_status="completed",
        terminal_consumer_timeout=30.0,
        consumer_cancel_timeout=10.0,
        yield_to_worker_task_termination=True,
    )
    reconcile.assert_awaited_once_with(
        instance_id,
        expected_task_id=task_id,
        expected_pid=145_0775,
        expected_started_at=started_at,
    )


@pytest.mark.asyncio
async def test_stop_helper_passes_exact_generation_for_same_task_aba(
    session_factory,
):
    """Task id equality alone cannot authorize stopping a rapid retry."""

    from datetime import datetime
    from backend.api.tasks import _stop_task_process
    from backend.models.instance import Instance
    from backend.models.task import Task
    import backend.main

    old_started_at = datetime(2026, 3, 4, 5, 6, 7)
    new_started_at = datetime(2026, 3, 4, 5, 6, 8)
    async with session_factory() as db:
        task = Task(
            title="same task ABA",
            description="d",
            status="executing",
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="same-task-reused-slot",
            status="running",
            pid=1111,
            started_at=old_started_at,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    async def reject_old_generation(
        stopped_instance_id,
        *,
        expected_task_id,
        expected_task_turn_generation,
        expected_pid,
        expected_started_at,
        task_status,
        terminal_consumer_timeout,
        consumer_cancel_timeout,
        yield_to_worker_task_termination,
    ):
        assert stopped_instance_id == instance_id
        assert expected_task_id == task_id
        assert expected_task_turn_generation == 0
        assert expected_pid == 1111
        assert expected_started_at == old_started_at
        assert task_status == "completed"
        assert terminal_consumer_timeout == 30.0
        assert consumer_cancel_timeout == 10.0
        assert yield_to_worker_task_termination is True
        async with session_factory() as db:
            instance = await db.get(Instance, instance_id)
            instance.pid = 2222
            instance.started_at = new_started_at
            await db.commit()
        # Models the manager's lock-internal exact-generation rejection.
        return False

    async with session_factory() as db:
        with (
            patch.object(
                backend.main.instance_manager,
                "stop",
                side_effect=reject_old_generation,
            ),
            patch.object(
                backend.main.instance_manager,
                "reconcile_dead_reverse_task_owner",
                new_callable=AsyncMock,
                return_value=False,
            ) as reconcile,
        ):
            assert await _stop_task_process(
                task_id,
                db,
                expected_generations=[
                    (instance_id, 1111, old_started_at)
                ],
                expected_task_turn_generation=0,
            ) is False
            reconcile.assert_not_awaited()

    async with session_factory() as db:
        instance = await db.get(Instance, instance_id)
        assert instance.current_task_id == task_id
        assert instance.pid == 2222
        assert instance.started_at == new_started_at


@pytest.mark.asyncio
async def test_cancel_stops_exact_owner_before_publishing_status(
    client, session_factory
):
    """The live owner is reaped while its Task generation is still active."""
    from backend.models.instance import Instance
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "Race Test", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        instance = Instance(
            name="cancel-order-owner",
            status="running",
            pid=9911,
            current_task_id=task_id,
        )
        db.add(instance)
        await db.flush()
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="executing", instance_id=instance.id)
        )
        await db.commit()
        instance_id = instance.id

    async def tracking_stop(
        tid,
        db,
        *,
        expected_generations,
        expected_task_turn_generation,
        task_status,
        worker_termination_operation_id,
    ):
        assert tid == task_id
        assert expected_task_turn_generation == 0
        assert task_status == "cancelled"
        assert worker_termination_operation_id is None
        assert expected_generations == [(instance_id, 9911, None)]
        async with session_factory() as verify_db:
            task = await verify_db.get(Task, task_id)
            assert task.status == "executing"
            owner = await verify_db.get(Instance, instance_id)
            owner.status = "idle"
            owner.pid = None
            owner.current_task_id = None
            task.status = "cancelled"
            task.completed_at = datetime.utcnow()
            await verify_db.commit()
        return True

    with patch("backend.api.tasks._stop_task_process", side_effect=tracking_stop):
        resp = await client.post(f"/api/tasks/{task_id}/cancel")

    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"


# === Mark unread tests ===


@pytest.mark.asyncio
async def test_mark_task_unread(client, session_factory):
    """POST /api/tasks/{id}/unread sets has_unread=True."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Read task", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    assert create_resp.json()["has_unread"] is False

    resp = await client.post(f"/api/tasks/{task_id}/unread")
    assert resp.status_code == 200
    assert resp.json()["has_unread"] is True


@pytest.mark.asyncio
async def test_mark_task_unread_not_found(client):
    """POST /api/tasks/9999/unread returns 404 for missing task."""
    resp = await client.post("/api/tasks/9999/unread")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_toggle_read_unread_roundtrip(client, session_factory):
    """Can toggle between read and unread states."""
    from backend.models.task import Task
    from sqlalchemy import update

    create_resp = await client.post("/api/tasks", json={
        "title": "Toggle", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    # Mark unread
    resp = await client.post(f"/api/tasks/{task_id}/unread")
    assert resp.json()["has_unread"] is True

    # Mark read
    resp = await client.post(f"/api/tasks/{task_id}/read")
    assert resp.json()["has_unread"] is False

    # Mark unread again
    resp = await client.post(f"/api/tasks/{task_id}/unread")
    assert resp.json()["has_unread"] is True


@pytest.mark.asyncio
async def test_mark_already_unread_task_unread(client, session_factory):
    """Marking an already-unread task as unread is idempotent."""
    from backend.models.task import Task
    from sqlalchemy import update

    create_resp = await client.post("/api/tasks", json={
        "title": "Already unread", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    # Set unread via DB
    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == task_id).values(has_unread=True))
        await db.commit()

    resp = await client.post(f"/api/tasks/{task_id}/unread")
    assert resp.status_code == 200
    assert resp.json()["has_unread"] is True


# === Starred on create tests ===


@pytest.mark.asyncio
async def test_create_task_starred_default_false(client):
    """Task created without starred flag has starred=False."""
    resp = await client.post("/api/tasks", json={
        "title": "No star", "description": "d", "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    assert resp.json()["starred"] is False


@pytest.mark.asyncio
async def test_create_task_with_starred_true(client):
    """Task created with starred=True is starred immediately."""
    resp = await client.post("/api/tasks", json={
        "title": "Starred", "description": "d", "target_repo": "/tmp",
        "starred": True,
    })
    assert resp.status_code == 201
    assert resp.json()["starred"] is True


@pytest.mark.asyncio
async def test_create_task_starred_persisted_in_db(client, session_factory):
    """starred=True at creation is persisted to the database."""
    from backend.models.task import Task

    resp = await client.post("/api/tasks", json={
        "title": "Starred persist", "description": "d", "target_repo": "/tmp",
        "starred": True,
    })
    task_id = resp.json()["id"]

    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.starred is True


@pytest.mark.asyncio
async def test_create_task_starred_in_list(client):
    """Starred task appears with starred=True in list endpoint."""
    await client.post("/api/tasks", json={
        "title": "Starred list", "description": "d", "target_repo": "/tmp",
        "starred": True,
    })
    resp = await client.get("/api/tasks")
    assert resp.status_code == 200
    tasks = resp.json()
    assert any(t["starred"] is True for t in tasks)


@pytest.mark.asyncio
async def test_create_task_starred_filter(client):
    """Starred filter returns only starred tasks including those starred at creation."""
    await client.post("/api/tasks", json={
        "title": "Not starred", "description": "d", "target_repo": "/tmp",
    })
    await client.post("/api/tasks", json={
        "title": "Starred", "description": "d", "target_repo": "/tmp",
        "starred": True,
    })
    resp = await client.get("/api/tasks?starred=true")
    assert resp.status_code == 200
    tasks = resp.json()
    assert len(tasks) == 1
    assert tasks[0]["starred"] is True


# === has_unread filter tests ===


@pytest.mark.asyncio
async def test_filter_unread_tasks(client, session_factory):
    """has_unread=true filter returns only unread tasks."""
    from backend.models.task import Task
    from sqlalchemy import update

    r1 = await client.post("/api/tasks", json={
        "title": "Read task", "description": "d", "target_repo": "/tmp",
    })
    r2 = await client.post("/api/tasks", json={
        "title": "Unread task", "description": "d", "target_repo": "/tmp",
    })
    unread_id = r2.json()["id"]

    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == unread_id).values(has_unread=True))
        await db.commit()

    resp = await client.get("/api/tasks?has_unread=true")
    assert resp.status_code == 200
    tasks = resp.json()
    assert len(tasks) == 1
    assert tasks[0]["id"] == unread_id
    assert tasks[0]["has_unread"] is True


@pytest.mark.asyncio
async def test_filter_read_tasks(client, session_factory):
    """has_unread=false filter returns only read tasks."""
    from backend.models.task import Task
    from sqlalchemy import update

    r1 = await client.post("/api/tasks", json={
        "title": "Read task", "description": "d", "target_repo": "/tmp",
    })
    r2 = await client.post("/api/tasks", json={
        "title": "Unread task", "description": "d", "target_repo": "/tmp",
    })
    unread_id = r2.json()["id"]
    read_id = r1.json()["id"]

    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == unread_id).values(has_unread=True))
        await db.commit()

    resp = await client.get("/api/tasks?has_unread=false")
    assert resp.status_code == 200
    tasks = resp.json()
    assert len(tasks) == 1
    assert tasks[0]["id"] == read_id
    assert tasks[0]["has_unread"] is False


@pytest.mark.asyncio
async def test_count_unread_tasks(client, session_factory):
    """has_unread filter works with count endpoint."""
    from backend.models.task import Task
    from sqlalchemy import update

    r1 = await client.post("/api/tasks", json={
        "title": "Task A", "description": "d", "target_repo": "/tmp",
    })
    r2 = await client.post("/api/tasks", json={
        "title": "Task B", "description": "d", "target_repo": "/tmp",
    })
    r3 = await client.post("/api/tasks", json={
        "title": "Task C", "description": "d", "target_repo": "/tmp",
    })

    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == r1.json()["id"]).values(has_unread=True))
        await db.execute(update(Task).where(Task.id == r2.json()["id"]).values(has_unread=True))
        await db.commit()

    resp = await client.get("/api/tasks/count?has_unread=true")
    assert resp.status_code == 200
    assert resp.json()["total"] == 2

    resp = await client.get("/api/tasks/count?has_unread=false")
    assert resp.status_code == 200
    assert resp.json()["total"] == 1


@pytest.mark.asyncio
async def test_filter_unread_combined_with_status(client, session_factory):
    """has_unread filter works combined with status filter."""
    from backend.models.task import Task
    from sqlalchemy import update

    r1 = await client.post("/api/tasks", json={
        "title": "Pending unread", "description": "d", "target_repo": "/tmp",
    })
    r2 = await client.post("/api/tasks", json={
        "title": "Pending read", "description": "d", "target_repo": "/tmp",
    })

    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == r1.json()["id"]).values(has_unread=True))
        await db.commit()

    resp = await client.get("/api/tasks?has_unread=true&status=pending")
    assert resp.status_code == 200
    tasks = resp.json()
    assert len(tasks) == 1
    assert tasks[0]["id"] == r1.json()["id"]


@pytest.mark.asyncio
async def test_filter_unread_with_no_results(client):
    """has_unread=true returns empty list when no unread tasks exist."""
    await client.post("/api/tasks", json={
        "title": "All read", "description": "d", "target_repo": "/tmp",
    })

    resp = await client.get("/api/tasks?has_unread=true")
    assert resp.status_code == 200
    assert resp.json() == []

    resp = await client.get("/api/tasks/count?has_unread=true")
    assert resp.status_code == 200
    assert resp.json()["total"] == 0


@pytest.mark.asyncio
async def test_no_unread_filter_returns_all(client, session_factory):
    """Without has_unread filter, both read and unread tasks are returned."""
    from backend.models.task import Task
    from sqlalchemy import update

    r1 = await client.post("/api/tasks", json={
        "title": "Read", "description": "d", "target_repo": "/tmp",
    })
    r2 = await client.post("/api/tasks", json={
        "title": "Unread", "description": "d", "target_repo": "/tmp",
    })

    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == r2.json()["id"]).values(has_unread=True))
        await db.commit()

    resp = await client.get("/api/tasks")
    assert resp.status_code == 200
    assert len(resp.json()) == 2


# === enable_workflows tests ===


@pytest.mark.asyncio
async def test_create_task_enable_workflows_default(client):
    """Task created without enable_workflows defaults to False."""
    resp = await client.post("/api/tasks", json={
        "title": "Default WF",
        "description": "test default",
        "target_repo": "/tmp",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["enable_workflows"] is False


@pytest.mark.asyncio
async def test_create_task_enable_workflows_true(client):
    """Task created with enable_workflows=True stores it correctly."""
    resp = await client.post("/api/tasks", json={
        "title": "WF Enabled",
        "description": "workflows enabled",
        "target_repo": "/tmp",
        "enable_workflows": True,
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["enable_workflows"] is True


@pytest.mark.asyncio
async def test_create_task_enable_workflows_false(client):
    """Task created with enable_workflows=False stores it correctly."""
    resp = await client.post("/api/tasks", json={
        "title": "WF Disabled",
        "description": "workflows disabled",
        "target_repo": "/tmp",
        "enable_workflows": False,
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["enable_workflows"] is False


@pytest.mark.asyncio
async def test_update_task_enable_workflows(client):
    """PUT /api/tasks/{id} can update enable_workflows."""
    create_resp = await client.post("/api/tasks", json={
        "title": "WF Toggle",
        "description": "toggle test",
        "target_repo": "/tmp",
        "enable_workflows": False,
    })
    task_id = create_resp.json()["id"]
    assert create_resp.json()["enable_workflows"] is False

    update_resp = await client.put(f"/api/tasks/{task_id}", json={
        "enable_workflows": True,
    })
    assert update_resp.status_code == 200
    assert update_resp.json()["enable_workflows"] is True


@pytest.mark.asyncio
async def test_create_task_enable_workflows_persisted_in_db(client, session_factory):
    """enable_workflows value is persisted in the database."""
    from backend.models.task import Task

    resp = await client.post("/api/tasks", json={
        "title": "DB Check",
        "description": "check db",
        "target_repo": "/tmp",
        "enable_workflows": True,
    })
    assert resp.status_code == 201
    task_id = resp.json()["id"]

    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.enable_workflows is True


@pytest.mark.asyncio
async def test_stop_session_clears_pending_queue(client):
    """A pending Task may clear its queue without inventing a live process."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Stop Queue", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    import backend.main
    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
        return_value=2,
    ) as mock_clear, patch(
        "backend.api.tasks._stop_task_process",
        new_callable=AsyncMock,
        return_value=True,
    ) as mock_stop:
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["stopped"] is False
    assert body["cleared_messages"] == 2
    mock_clear.assert_awaited_once()
    assert mock_clear.await_args.args == (task_id,)
    assert mock_clear.await_args.kwargs["durable_db"] is not None
    mock_stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_session_closes_auxiliary_producers_before_final_queue_drain(
    client,
    session_factory,
):
    """A monitor cannot refill the queue after Interrupt drains it."""

    import backend.main
    from backend.models.monitor_session import MonitorSession

    create_resp = await client.post("/api/tasks", json={
        "title": "Stop monitor producer",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        monitor = MonitorSession(
            task_id=task_id,
            agent_type="monitor",
            source="ccm",
            description="keeps reporting",
            status="running",
        )
        db.add(monitor)
        await db.commit()
        monitor_id = monitor.id

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            side_effect=[46, 1],
        ) as abort_queue,
        patch.object(
            backend.main.dispatcher,
            "stop_monitor_session_process",
            new_callable=AsyncMock,
        ) as stop_monitor,
    ):
        response = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert response.status_code == 200
    assert response.json()["cleared_messages"] == 47
    assert abort_queue.await_count == 2
    stop_monitor.assert_awaited_once_with(monitor_id, terminal=True)
    async with session_factory() as db:
        monitor = await db.get(MonitorSession, monitor_id)
        assert monitor.status == "cancelled"
        assert monitor.next_check_at is None
        assert monitor.active_turn_generation is None


@pytest.mark.asyncio
async def test_stop_session_no_process_reports_not_stopped(client, session_factory):
    """When no process is found but task is executing, response says stopped=False."""
    from backend.models.task import Task
    from sqlalchemy import update

    create_resp = await client.post("/api/tasks", json={
        "title": "No Proc", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == task_id).values(status="executing"))
        await db.commit()

    import backend.main
    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
        return_value=0,
    ), patch(
        "backend.api.tasks._stop_task_process",
        new_callable=AsyncMock,
        return_value=False,
    ) as mock_stop:
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 200
    body = resp.json()
    assert body["stopped"] is False
    assert "note" in body
    mock_stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_session_terminal_task_without_process_returns_error(
    client,
    session_factory,
):
    """A terminal Task with no queue/process preserves the public 400 contract."""
    from backend.models.task import Task
    from sqlalchemy import update

    create_resp = await client.post("/api/tasks", json={
        "title": "Already done", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="completed")
        )
        await db.commit()

    resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 400
    assert resp.json()["detail"] == "No running session found for this task"


@pytest.mark.asyncio
async def test_stop_session_ownerless_generation_does_not_infer_process(
    client,
    session_factory,
):
    """An ownerless active Task is terminalized without addressing a slot."""

    from backend.models.task import Task
    from sqlalchemy import update

    create_resp = await client.post("/api/tasks", json={
        "title": "Stop launch race",
        "description": "d",
        "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="executing")
        )
        await db.commit()

    import backend.main
    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
        return_value=0,
    ), patch(
        "backend.api.tasks._stop_task_process",
        new_callable=AsyncMock,
        return_value=True,
    ) as mock_stop:
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 200
    assert resp.json()["stopped"] is False
    mock_stop.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "completed"
        assert task.instance_id is None


@pytest.mark.asyncio
async def test_stop_session_cleared_only_returns_ok(client):
    """No process and task not executing, but messages were cleared -> 200 not 400."""
    create_resp = await client.post("/api/tasks", json={
        "title": "Cleared Only", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    import backend.main
    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
        return_value=1,
    ), patch(
        "backend.api.tasks._stop_task_process",
        new_callable=AsyncMock,
        return_value=False,
    ) as mock_stop:
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 200
    assert resp.json()["stopped"] is False
    mock_stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_list_order_starred_then_access_then_manual(client, session_factory):
    """排序：标星置顶 → 手动 sort_order / 最近访问时间（越新越靠前）。"""
    from datetime import datetime, timedelta
    from backend.models.task import Task

    ids = []
    for i in range(4):
        resp = await client.post("/api/tasks", json={
            "title": f"T{i}", "description": "d", "target_repo": "/tmp",
        })
        ids.append(resp.json()["id"])

    now = datetime.utcnow()
    async with session_factory() as db:
        a, b, c, d = [await db.get(Task, i) for i in ids]
        a.last_accessed_at = now - timedelta(hours=3)
        b.last_accessed_at = now - timedelta(hours=1)   # 最近访问
        c.last_accessed_at = now - timedelta(hours=2)
        c.starred = True                                 # 标星 → 置顶
        d.last_accessed_at = now - timedelta(hours=4)
        d.sort_order = now.timestamp() + 999             # 手动拖到最前（非星组）
        await db.commit()

    resp = await client.get("/api/tasks?limit=50")
    order = [t["id"] for t in resp.json() if t["id"] in ids]
    # c 标星置顶；非星组按位置键：d 手动键最大 → b（访问较近）→ a
    assert order == [ids[2], ids[3], ids[1], ids[0]]


@pytest.mark.asyncio
async def test_chat_history_touches_last_accessed(client, session_factory):
    """打开 chat（拉历史，touch=true）应更新 last_accessed_at；
    不带 touch 的拉取（分页/后台轮询/旧版客户端）不得更新——
    生产实录：旧版前端残留标签页轮询导致任务在列表里来回跳。"""
    from backend.models.task import Task

    resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = resp.json()["id"]
    async with session_factory() as db:
        t = await db.get(Task, task_id)
        assert t.last_accessed_at is None

    # 不带 touch：不更新（回归：每次 history 拉取都 touch 会被轮询滥用）
    await client.get(f"/api/tasks/{task_id}/chat/history")
    async with session_factory() as db:
        t = await db.get(Task, task_id)
        assert t.last_accessed_at is None

    await client.get(f"/api/tasks/{task_id}/chat/history?touch=true")
    async with session_factory() as db:
        t = await db.get(Task, task_id)
        assert t.last_accessed_at is not None


@pytest.mark.asyncio
async def test_open_chat_moves_task_to_front_of_group(client, session_factory):
    """Touch updates last_accessed_at; for tasks without sort_order,
    the query sorts by last_accessed_at (auto_sort_on_access=True default),
    so the most recently accessed task appears first."""
    from datetime import datetime, timedelta
    from backend.models.task import Task

    now = datetime.utcnow()
    ids = []
    for i in range(3):
        resp = await client.post("/api/tasks", json={
            "title": f"T{i}", "description": "d", "target_repo": "/tmp",
        })
        ids.append(resp.json()["id"])

    # Set distinct created_at timestamps with 10s gaps to avoid strftime("%s") collisions
    async with session_factory() as db:
        for i, tid in enumerate(ids):
            t = await db.get(Task, tid)
            t.created_at = now - timedelta(seconds=30 - i * 10)  # t0 oldest, t2 newest
            t.sort_order = None
            t.last_accessed_at = None
        await db.commit()

    # Before touch: t2 (newest created_at) should be first
    resp = await client.get("/api/tasks?limit=50")
    order = [t["id"] for t in resp.json() if t["id"] in ids]
    assert order[0] == ids[2]

    # Touch t0 → t0's last_accessed_at becomes now → should sort first
    await client.get(f"/api/tasks/{ids[0]}/chat/history?touch=true")
    resp = await client.get("/api/tasks?limit=50")
    order = [t["id"] for t in resp.json() if t["id"] in ids]
    assert order[0] == ids[0]


@pytest.mark.asyncio
async def test_update_sort_order_via_api_moves_task(client):
    """回归：sort_order 曾只加在 TaskCreate 上，PUT 被 pydantic 丢弃 →
    前端拖拽永远不生效。必须走 API 全链路验证。"""
    ids = []
    for i in range(3):
        resp = await client.post("/api/tasks", json={
            "title": f"T{i}", "description": "d", "target_repo": "/tmp",
        })
        ids.append(resp.json()["id"])

    # 默认按创建时间倒序：[t2, t1, t0]；把 t0 拖到第一
    resp = await client.get("/api/tasks?limit=10")
    order = [t["id"] for t in resp.json() if t["id"] in ids]
    assert order == [ids[2], ids[1], ids[0]]

    import time
    resp = await client.put(f"/api/tasks/{ids[0]}", json={"sort_order": time.time() + 9999})
    assert resp.status_code == 200
    assert resp.json()["sort_order"] is not None

    resp = await client.get("/api/tasks?limit=10")
    order = [t["id"] for t in resp.json() if t["id"] in ids]
    assert order == [ids[0], ids[2], ids[1]]


# === status_change 广播收口（2026-07 状态显示大排查）===
# API 侧改 Task.status 的路径必须广播 status_change，否则 ChatView（WS 驱动）
# 与任务列表（轮询驱动）状态分叉。


@pytest.mark.asyncio
async def test_cancel_task_broadcasts_status_change(client):
    create = await client.post("/api/tasks", json={"description": "to cancel"})
    task_id = create.json()["id"]

    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()
    with patch("backend.main.broadcaster", mock_broadcaster):
        resp = await client.post(f"/api/tasks/{task_id}/cancel")
    assert resp.status_code == 200

    payloads = [
        c.args[1] for c in mock_broadcaster.broadcast.await_args_list
        if c.args[1].get("event") == "status_change"
    ]
    assert any(p["task_id"] == task_id and p["new_status"] == "cancelled" for p in payloads)
