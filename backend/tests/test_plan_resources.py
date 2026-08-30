from datetime import datetime
import asyncio
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import delete, event, func, select, update
from sqlalchemy.exc import IntegrityError

from backend.config import settings
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.global_settings import GlobalSettings
from backend.models.project import Project
from backend.models.plan import (
    Plan,
    PlanApplication,
    PlanApplicationAttempt,
    PlanApplicationReceipt,
    PlanInputRequest,
    PlanVersion,
)
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.task import Task
from backend.models.worker import Worker, WorkerNodeControl
from backend.schemas.plan import default_plan_pipeline_config
from backend.services.plan_agent_runner import PlanAgentRunner
from backend.services.plan_runtime_receipt import new_prepared_runtime_receipt
from backend.services.plan_service import (
    apply_worker_plan_outcome,
    decide_version,
    materialize_execution_task,
    stage_plan_with_run,
)
from backend.tests.group_acl_test_helpers import (
    grant_group_project_access,
    revoke_group_membership_at_effect_fence,
)
from backend.tests.test_auth_ws_security import (
    _create_user,
    secured_client as secured_client,
)


@pytest.mark.asyncio
async def test_plan_authorization_cancel_finishes_transaction_rollback_under_anyio():
    from anyio import CancelScope

    scope_holder: dict[str, CancelScope] = {}
    rollback_started = asyncio.Event()
    release_rollback = asyncio.Event()
    rollback_finished = asyncio.Event()

    class FakeSession:
        async def rollback(self):
            rollback_started.set()
            await release_rollback.wait()
            rollback_finished.set()

    async def cancel_authorization(_db):
        scope_holder["scope"].cancel()
        await asyncio.sleep(0)

    async def release():
        await rollback_started.wait()
        await asyncio.sleep(0)
        release_rollback.set()

    releaser = asyncio.create_task(release())
    try:
        with CancelScope() as scope:
            scope_holder["scope"] = scope
            with pytest.raises(asyncio.CancelledError):
                await stage_plan_with_run(
                    FakeSession(),
                    title="cancelled Plan authorization",
                    initial_request="rollback before materialization",
                    attachments=None,
                    target_task_id=None,
                    project_id=None,
                    target_repo=None,
                    target_branch=None,
                    worker_id=None,
                    priority=0,
                    timeout_hours=None,
                    created_by=None,
                    pipeline_config={},
                    context_session_id=None,
                    context_log_id=None,
                    context_snapshot=None,
                    repo_revision=None,
                    authorize_effect_boundary=cancel_authorization,
                )
        await releaser
    finally:
        release_rollback.set()
        if not releaser.done():
            releaser.cancel()
        await asyncio.gather(releaser, return_exceptions=True)

    assert rollback_finished.is_set()


async def _target(client, session_factory) -> Task:
    response = await client.post(
        "/api/tasks",
        json={
            "title": "Versioned Plan target",
            "description": "Initial task request",
            "target_repo": "/tmp",
        },
    )
    assert response.status_code == 201, response.text
    task_id = response.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.session_id = "session-plan-v2"
        task.status = "completed"
        db.add(
            LogEntry(
                instance_id=1,
                task_id=task.id,
                event_type="user_message",
                role="user",
                content="Existing context",
            )
        )
        await db.commit()
        await db.refresh(task)
        db.expunge(task)
        return task


@pytest.mark.asyncio
async def test_standalone_plan_effect_rejects_cached_jwt_role_change(
    secured_client,
):
    """Projectless Plan effects still fence their mutable User authority."""

    from backend.api.plan_resources import _lock_standalone_plan_effect_access

    _client, session_factory = secured_client
    user_id, _ = await _create_user(
        session_factory,
        email="standalone-plan-stale-admin@example.com",
        role="member",
    )
    request = SimpleNamespace(
        state=SimpleNamespace(
            user_id=user_id,
            user_role="admin",
            auth_type="jwt",
        ),
        headers={},
    )
    async with session_factory() as db:
        with pytest.raises(HTTPException) as caught:
            await _lock_standalone_plan_effect_access(
                request,
                db,
                project_id=None,
                plan_created_by=user_id,
            )

    assert caught.value.status_code == 409
    assert "changed role" in caught.value.detail


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["target_task", "project"])
async def test_initial_plan_creation_rejects_concurrent_wal_group_revocation(
    tmp_path,
    monkeypatch,
    scope,
):
    """The POST /api/plans commit cannot cross a revoked group ACL."""

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from backend.api import plan_resources as api
    from backend.database import Base
    from backend.models.team_share import TeamProjectShare
    from backend.models.user import User
    from backend.models.user_group import UserGroup, UserGroupMember
    from backend.schemas.plan_resource import PlanCreateRequest

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / f'initial-plan-{scope}-acl.db'}",
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
            user = User(
                email=f"initial-plan-{scope}-member@example.com",
                name="initial-plan-member",
                password_hash="not-used",
                role="member",
                is_active=True,
            )
            group = UserGroup(name=f"initial-plan-{scope}-group")
            project = Project(
                name=f"initial-plan-{scope}-project",
                local_path="/tmp",
                status="ready",
            )
            setup.add_all([user, group, project])
            await setup.flush()
            membership = UserGroupMember(
                group_id=group.id,
                user_id=user.id,
            )
            setup.add_all(
                [
                    membership,
                    TeamProjectShare(
                        project_id=project.id,
                        target_type="group",
                        target_id=group.id,
                        shared_by=999,
                    ),
                ]
            )
            target = None
            if scope == "target_task":
                target = Task(
                    title="Initial Plan WAL ACL target",
                    description="protected by Project group membership",
                    status="completed",
                    session_id="initial-plan-wal-session",
                    project_id=project.id,
                    target_repo="/tmp",
                    created_by=999,
                )
                setup.add(target)
            await setup.commit()
            user_id = user.id
            membership_id = membership.id
            project_id = project.id
            target_task_id = target.id if target is not None else None

        request = SimpleNamespace(
            state=SimpleNamespace(
                user_id=user_id,
                user_role="member",
                auth_type="jwt",
            ),
            headers={},
        )
        capture_entered = asyncio.Event()
        release_capture = asyncio.Event()

        async def blocked_capture(*_args, **_kwargs):
            capture_entered.set()
            await release_capture.wait()
            return (None, None, None, {"available": False, "reason": "test"})

        monkeypatch.setattr(settings, "auth_token", "plan-wal-auth-token")
        monkeypatch.setattr(settings, "ccm_node_role", "manager")
        monkeypatch.setattr(api, "_capture_context_for_plan", blocked_capture)
        body = PlanCreateRequest(
            input="Do not publish after access is revoked",
            target_task_id=target_task_id,
            project_id=project_id if target_task_id is None else None,
        )

        async def create_initial_plan():
            async with sessions() as creator:
                return await api.create_plan(
                    body=body,
                    request=request,
                    db=creator,
                )

        pending = asyncio.create_task(create_initial_plan())
        await asyncio.wait_for(capture_entered.wait(), timeout=2)
        async with sessions() as revoker:
            revoked = await revoker.execute(
                delete(UserGroupMember).where(
                    UserGroupMember.id == membership_id
                )
            )
            assert revoked.rowcount == 1
            await revoker.commit()
        release_capture.set()

        with pytest.raises(HTTPException) as rejected:
            await pending
        assert rejected.value.status_code == 403
        async with sessions() as verify:
            assert await verify.scalar(select(func.count(Plan.id))) == 0
            assert await verify.scalar(select(func.count(PlanAgentRun.id))) == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_projectless_initial_plan_rejects_concurrent_wal_admin_demotion(
    tmp_path,
    monkeypatch,
):
    """A cached admin role is fenced again in the Plan commit transaction."""

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from backend.api import plan_resources as api
    from backend.database import Base
    from backend.models.user import User
    from backend.schemas.plan_resource import PlanCreateRequest

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'initial-plan-admin-role.db'}",
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
                email="initial-plan-demoted-admin@example.com",
                name="initial-plan-admin",
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
        capture_entered = asyncio.Event()
        release_capture = asyncio.Event()

        async def blocked_capture(*_args, **_kwargs):
            capture_entered.set()
            await release_capture.wait()
            return (None, None, None, {"available": False, "reason": "test"})

        monkeypatch.setattr(settings, "auth_token", "plan-wal-auth-token")
        monkeypatch.setattr(settings, "ccm_node_role", "manager")
        monkeypatch.setattr(api, "_capture_context_for_plan", blocked_capture)

        async def create_initial_plan():
            async with sessions() as creator:
                return await api.create_plan(
                    body=PlanCreateRequest(
                        input="Projectless admin Plan must retain authority",
                    ),
                    request=request,
                    db=creator,
                )

        pending = asyncio.create_task(create_initial_plan())
        await asyncio.wait_for(capture_entered.wait(), timeout=2)
        async with sessions() as demoter:
            changed = await demoter.execute(
                update(User)
                .where(User.id == admin_id, User.role == "admin")
                .values(role="member")
            )
            assert changed.rowcount == 1
            await demoter.commit()
        release_capture.set()

        with pytest.raises(HTTPException) as rejected:
            await pending
        assert rejected.value.status_code == 409
        assert "changed role" in rejected.value.detail
        async with sessions() as verify:
            assert await verify.scalar(select(func.count(Plan.id))) == 0
            assert await verify.scalar(select(func.count(PlanAgentRun.id))) == 0
    finally:
        await engine.dispose()


async def _finish_current_run_with_version(
    session_factory,
    *,
    plan_id: int,
    content: str = "# Ready Plan",
) -> int:
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, plan.active_run_id)
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            produced_by_run_id=run.id,
            content=content,
            context_session_id=run.context_session_id,
            context_log_id=run.context_log_id,
            repo_revision=run.repo_revision,
            reviewer_repo_revision=run.repo_revision,
            review_verdict="approve",
            reviewed_at=datetime.utcnow(),
        )
        db.add(version)
        await db.flush()
        plan.current_version_id = version.id
        plan.active_run_id = None
        run.status = "completed"
        run.current_stage = "complete"
        run.result_version_id = version.id
        run.finished_at = datetime.utcnow()
        await db.commit()
        return version.id


@pytest.mark.asyncio
async def test_attached_plan_freezes_target_last_cwd(client, session_factory):
    target = await _target(client, session_factory)
    async with session_factory() as db:
        current = await db.get(Task, target.id)
        current.target_repo = "/workspace/project"
        current.last_cwd = "/workspace/project/.worktrees/exact-turn"
        await db.commit()

    created = await client.post(
        "/api/plans",
        json={"input": "Plan against the active checkout", "target_task_id": target.id},
    )

    assert created.status_code == 201, created.text
    assert created.json()["target_repo"] == "/workspace/project/.worktrees/exact-turn"
    async with session_factory() as db:
        plan = await db.get(Plan, created.json()["id"])
        assert plan.target_repo == "/workspace/project/.worktrees/exact-turn"


@pytest.mark.asyncio
async def test_related_plan_revision_rebinds_to_current_target_checkout(
    client,
    session_factory,
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Refresh the exact checkout", "target_task_id": target.id},
    )
    assert created.status_code == 201, created.text
    plan_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
    )
    next_checkout = "/workspace/project/.worktrees/revision-turn"
    async with session_factory() as db:
        current = await db.get(Task, target.id)
        current.last_cwd = next_checkout
        current.target_branch = "revision-branch"
        await db.commit()

    revised = await client.post(
        f"/api/plans/{plan_id}/runs",
        json={
            "run_type": "user_revision",
            "request": "Use the Task's current checkout",
            "base_version_id": version_id,
            "expected_current_version_id": version_id,
        },
    )

    assert revised.status_code == 201, revised.text
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        assert plan.target_repo == next_checkout
        assert plan.target_branch == "revision-branch"


@pytest.mark.asyncio
async def test_related_plan_fork_binds_to_current_target_checkout(
    client,
    session_factory,
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Fork the current checkout", "target_task_id": target.id},
    )
    assert created.status_code == 201, created.text
    source_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=source_id,
    )
    next_checkout = "/workspace/project/.worktrees/fork-turn"
    async with session_factory() as db:
        current = await db.get(Task, target.id)
        current.last_cwd = next_checkout
        current.target_branch = "fork-branch"
        await db.commit()

    forked = await client.post(
        f"/api/plans/{source_id}/fork",
        json={"base_version_id": version_id},
    )

    assert forked.status_code == 201, forked.text
    assert forked.json()["target_repo"] == next_checkout
    assert forked.json()["target_branch"] == "fork-branch"


@pytest.mark.asyncio
async def test_create_plan_rechecks_task_control_in_final_transaction(
    client,
    session_factory,
    monkeypatch,
):
    from backend.api import plan_resources as api

    target = await _target(client, session_factory)
    revoked = AsyncMock(
        side_effect=HTTPException(403, "control was revoked")
    )
    monkeypatch.setattr(api, "lock_task_effect_access", revoked)

    response = await client.post(
        "/api/plans",
        json={
            "input": "Do not create from a revoked Task snapshot",
            "target_task_id": target.id,
        },
    )

    assert response.status_code == 403
    assert revoked.await_count == 1
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Plan.id))) == 0
        assert await db.scalar(select(func.count(PlanAgentRun.id))) == 0


@pytest.mark.asyncio
async def test_create_project_plan_rechecks_access_in_final_transaction(
    client,
    session_factory,
    monkeypatch,
):
    from backend.api import plan_resources as api

    async with session_factory() as db:
        project = Project(
            name="revoked-plan-project",
            local_path="/tmp/revoked-plan-project",
            status="ready",
        )
        db.add(project)
        await db.commit()
        project_id = project.id

    monkeypatch.setattr(settings, "auth_token", "plan-acl-test-token")
    revoked = AsyncMock(
        side_effect=HTTPException(403, "project access was revoked")
    )
    monkeypatch.setattr(api, "_lock_standalone_plan_effect_access", revoked)
    response = await client.post(
        "/api/plans",
        headers={"Authorization": "Bearer plan-acl-test-token"},
        json={
            "input": "Do not create from revoked Project access",
            "project_id": project_id,
        },
    )

    assert response.status_code == 403
    assert revoked.await_count == 1
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Plan.id))) == 0
        assert await db.scalar(select(func.count(PlanAgentRun.id))) == 0


@pytest.mark.asyncio
async def test_create_worker_plan_rechecks_access_in_final_transaction(
    client,
    session_factory,
    monkeypatch,
):
    from backend.api import plan_resources as api

    async with session_factory() as db:
        worker = Worker(name="revoked-plan-worker", status="ready")
        db.add(worker)
        await db.commit()
        worker_id = worker.id

    monkeypatch.setattr(settings, "auth_token", "plan-acl-test-token")
    revoked = AsyncMock(
        side_effect=[None, HTTPException(403, "Worker access was revoked")]
    )
    monkeypatch.setattr(api, "require_worker_target_access", revoked)
    response = await client.post(
        "/api/plans",
        headers={"Authorization": "Bearer plan-acl-test-token"},
        json={
            "input": "Do not create from revoked Worker access",
            "target_repo": "/workspace/revoked-plan-worker",
            "worker_id": worker_id,
        },
    )

    assert response.status_code == 403
    assert revoked.await_count == 2
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Plan.id))) == 0
        assert await db.scalar(select(func.count(PlanAgentRun.id))) == 0


@pytest.mark.asyncio
async def test_acl_dependency_refresh_never_adds_cross_worker_writer_locks():
    """Refreshing cached ACL inputs must not create an A/B Worker lock cycle."""

    from backend.api.plan_resources import _refresh_plan_acl_dependencies

    db = MagicMock()
    db.get = AsyncMock(
        side_effect=[
            Worker(id=22, name="current-worker", status="ready"),
            Project(
                id=33,
                name="migrated-project",
                local_path="/workspace/project",
                worker_id=11,
            ),
            Worker(id=11, name="project-worker", status="ready"),
        ]
    )
    db.execute = AsyncMock()

    await _refresh_plan_acl_dependencies(
        db,
        worker_id=22,
        project_id=33,
    )

    db.execute.assert_not_awaited()
    assert db.get.await_args_list == [
        ((Worker, 22), {"populate_existing": True}),
        ((Project, 33), {"populate_existing": True}),
        ((Worker, 11), {"populate_existing": True}),
    ]


@pytest.mark.asyncio
async def test_create_run_rechecks_plan_control_in_final_transaction(
    client,
    session_factory,
    monkeypatch,
):
    from backend.api import plan_resources as api

    created = await client.post(
        "/api/plans",
        json={"input": "Create a protected revision", "target_repo": "/tmp"},
    )
    assert created.status_code == 201, created.text
    plan_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
    )
    async with session_factory() as db:
        before_plan = await db.get(Plan, plan_id)
        before_lock_version = before_plan.lock_version
        before_run_count = await db.scalar(
            select(func.count(PlanAgentRun.id)).where(
                PlanAgentRun.plan_id == plan_id
            )
        )

    access = AsyncMock(side_effect=[True, False])
    monkeypatch.setattr(api, "_has_plan_access", access)
    response = await client.post(
        f"/api/plans/{plan_id}/runs",
        json={
            "run_type": "user_revision",
            "request": "This write must lose authorization",
            "base_version_id": version_id,
            "expected_current_version_id": version_id,
        },
    )

    assert response.status_code == 403
    assert access.await_count == 2
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        assert plan.active_run_id is None
        assert plan.lock_version == before_lock_version
        assert (
            await db.scalar(
                select(func.count(PlanAgentRun.id)).where(
                    PlanAgentRun.plan_id == plan_id
                )
            )
            == before_run_count
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("effect", ["run", "decision", "execution"])
async def test_plan_effect_rejects_group_revoked_at_final_project_fence(
    secured_client,
    monkeypatch,
    effect,
):
    """No Plan effect can use stale group-derived Project access."""

    client, session_factory = secured_client
    member_id, member_token = await _create_user(
        session_factory,
        email=f"plan-{effect}-effect@example.com",
        role="member",
    )
    async with session_factory() as db:
        project = Project(
            name=f"plan-{effect}-effect-project",
            local_path="/tmp",
            status="ready",
        )
        db.add(project)
        await db.commit()
        project_id = project.id
    created = await client.post(
        "/api/plans",
        headers={"Authorization": "Bearer security-service-token"},
        json={
            "input": f"Seed the {effect} effect fence",
            "project_id": project_id,
        },
    )
    assert created.status_code == 201, created.text
    plan_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
    )
    await grant_group_project_access(
        session_factory,
        project_id=project_id,
        user_id=member_id,
    )
    async with session_factory() as db:
        initial_run_count = await db.scalar(
            select(func.count(PlanAgentRun.id)).where(
                PlanAgentRun.plan_id == plan_id
            )
        )
    fence = revoke_group_membership_at_effect_fence(monkeypatch)
    headers = {"Authorization": f"Bearer {member_token}"}

    if effect == "run":
        response = await client.post(
            f"/api/plans/{plan_id}/runs",
            headers=headers,
            json={
                "run_type": "user_revision",
                "request": "must not create another Run",
                "base_version_id": version_id,
                "expected_current_version_id": version_id,
            },
        )
    elif effect == "decision":
        response = await client.post(
            f"/api/plan-versions/{version_id}/approve",
            headers=headers,
            json={"expected_current_version_id": version_id},
        )
    else:
        response = await client.post(
            f"/api/plan-versions/{version_id}/create-execution-task",
            headers=headers,
            json={
                "expected_current_version_id": version_id,
                "approve_if_pending": True,
            },
        )

    assert response.status_code == 403, response.text
    assert fence == {"calls": 1, "revoked": True}
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        version = await db.get(PlanVersion, version_id)
        assert plan.active_run_id is None
        assert version.human_decision == "pending"
        assert await db.scalar(
            select(func.count(PlanAgentRun.id)).where(
                PlanAgentRun.plan_id == plan_id
            )
        ) == initial_run_count
        assert await db.scalar(
            select(func.count(PlanApplication.id)).where(
                PlanApplication.plan_id == plan_id
            )
        ) == 0
        assert await db.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_fork_rechecks_source_plan_control_in_final_transaction(
    client,
    session_factory,
    monkeypatch,
):
    from backend.api import plan_resources as api

    created = await client.post(
        "/api/plans",
        json={"input": "Create a protected fork", "target_repo": "/tmp"},
    )
    assert created.status_code == 201, created.text
    source_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=source_id,
    )
    async with session_factory() as db:
        before_plan_count = await db.scalar(select(func.count(Plan.id)))
        before_run_count = await db.scalar(select(func.count(PlanAgentRun.id)))

    access = AsyncMock(side_effect=[True, False])
    monkeypatch.setattr(api, "_has_plan_access", access)
    response = await client.post(
        f"/api/plans/{source_id}/fork",
        json={"base_version_id": version_id},
    )

    assert response.status_code == 403
    assert access.await_count == 2
    async with session_factory() as db:
        source = await db.get(Plan, source_id)
        assert source.active_run_id is None
        assert await db.scalar(select(func.count(Plan.id))) == before_plan_count
        assert (
            await db.scalar(select(func.count(PlanAgentRun.id)))
            == before_run_count
        )


@pytest.mark.asyncio
async def test_worker_lifecycle_fences_new_plan_admission(client, session_factory):
    async with session_factory() as db:
        worker = Worker(
            name="destroying-plan-worker",
            status="destroying",
            cloud_instance_id="i-destroying-plan",
        )
        db.add(worker)
        await db.commit()
        worker_id = worker.id

    created = await client.post(
        "/api/plans",
        json={
            "input": "Must not outlive the Worker",
            "target_repo": "/workspace/remote",
            "worker_id": worker_id,
        },
    )

    assert created.status_code == 409
    assert "not ready for assignment" in created.json()["detail"]
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Plan.id))) == 0
        assert await db.scalar(select(func.count(PlanAgentRun.id))) == 0


@pytest.mark.asyncio
async def test_worker_node_drain_claim_blocks_plan_version_decision(
    client,
    session_factory,
    monkeypatch,
):
    """A Worker drain that wins first leaves the pending Version untouched."""

    created = await client.post(
        "/api/plans",
        json={"input": "Do not decide after Worker drain", "target_repo": "/tmp"},
    )
    assert created.status_code == 201, created.text
    plan_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
    )
    async with session_factory() as db:
        control = await db.get(WorkerNodeControl, 1)
        assert control is not None
        control.drain_claim = "d" * 64
        control.drain_started_at = datetime.utcnow()
        await db.commit()

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        version = await db.get(PlanVersion, version_id)
        assert plan is not None and version is not None
        with pytest.raises(HTTPException) as blocked:
            await decide_version(
                db,
                plan=plan,
                version=version,
                decision="approved",
                decided_by=7,
                expected_current_version_id=version_id,
            )
    assert blocked.value.status_code == 409
    assert "destruction has begun" in str(blocked.value.detail)

    async with session_factory() as db:
        version = await db.get(PlanVersion, version_id)
        assert version is not None
        assert version.human_decision == "pending"
        assert version.decided_at is None
        assert version.decided_by is None


@pytest.mark.asyncio
async def test_plan_version_decision_holds_worker_node_fence_through_commit(
    tmp_path,
    monkeypatch,
):
    """If a decision wins first, node drain waits and then observes it."""

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from backend.database import Base
    from backend.services import plan_service
    from backend.services.worker_node_control import begin_worker_node_drain

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'plan-decision-drain-order.db'}",
        connect_args={"timeout": 5},
    )
    decision_task = None
    drain_task = None
    release_decision = asyncio.Event()
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
        pipeline = default_plan_pipeline_config().model_dump(mode="json")
        async with sessions() as setup:
            plan = Plan(
                title="Decision wins Worker drain",
                initial_request="Persist the exact human decision",
                target_repo="/tmp",
                pipeline_config=pipeline,
            )
            setup.add(plan)
            await setup.flush()
            run = PlanAgentRun(
                plan_id=plan.id,
                run_type="initial",
                status="completed",
                current_stage="complete",
                pipeline_config=pipeline,
                finished_at=datetime.utcnow(),
            )
            setup.add(run)
            await setup.flush()
            version = PlanVersion(
                plan_id=plan.id,
                version_number=1,
                produced_by_run_id=run.id,
                content="# Exact decision",
                review_verdict="approve",
                reviewed_at=datetime.utcnow(),
            )
            setup.add(version)
            await setup.flush()
            plan.current_version_id = version.id
            run.result_version_id = version.id
            await setup.commit()
            plan_id = plan.id
            version_id = version.id

        monkeypatch.setattr(settings, "ccm_node_role", "worker")
        original_fence = plan_service.fence_worker_node_mutation
        decision_fence_held = asyncio.Event()

        async def hold_decision_fence(db):
            await original_fence(db)
            decision_fence_held.set()
            await release_decision.wait()

        monkeypatch.setattr(
            plan_service,
            "fence_worker_node_mutation",
            hold_decision_fence,
        )

        async def decide():
            async with sessions() as db:
                plan = await db.get(Plan, plan_id)
                version = await db.get(PlanVersion, version_id)
                assert plan is not None and version is not None
                return await plan_service.decide_version(
                    db,
                    plan=plan,
                    version=version,
                    decision="approved",
                    decided_by=7,
                    expected_current_version_id=version_id,
                )

        async def begin_drain():
            async with sessions() as db:
                await begin_worker_node_drain(db, claim="e" * 64)
                await db.commit()

        decision_task = asyncio.create_task(decide())
        await asyncio.wait_for(decision_fence_held.wait(), timeout=2)
        drain_task = asyncio.create_task(begin_drain())
        await asyncio.sleep(0.05)
        assert not drain_task.done()

        release_decision.set()
        decided = await asyncio.wait_for(decision_task, timeout=5)
        await asyncio.wait_for(drain_task, timeout=5)
        assert decided.human_decision == "approved"

        async with sessions() as verify:
            version = await verify.get(PlanVersion, version_id)
            control = await verify.get(WorkerNodeControl, 1)
            assert version is not None and version.human_decision == "approved"
            assert version.decided_by == 7
            assert control is not None and control.drain_claim == "e" * 64
    finally:
        release_decision.set()
        for pending in (decision_task, drain_task):
            if pending is not None and not pending.done():
                pending.cancel()
        await asyncio.gather(
            *(
                pending
                for pending in (decision_task, drain_task)
                if pending is not None
            ),
            return_exceptions=True,
        )
        await engine.dispose()


@pytest.mark.asyncio
async def test_worker_lifecycle_fences_new_run_and_restore(
    client,
    session_factory,
):
    async with session_factory() as db:
        worker = Worker(name="plan-worker", status="ready")
        db.add(worker)
        await db.flush()
        plan = Plan(
            title="Archived Worker Plan",
            initial_request="Keep historical output",
            target_repo="/workspace/remote",
            worker_id=worker.id,
            priority=0,
            archived_at=datetime.utcnow(),
            pipeline_config=default_plan_pipeline_config().model_dump(mode="json"),
        )
        db.add(plan)
        runnable_plan = Plan(
            title="Runnable Worker Plan",
            initial_request="Attempt another generation",
            target_repo="/workspace/remote",
            worker_id=worker.id,
            priority=0,
            pipeline_config=default_plan_pipeline_config().model_dump(mode="json"),
        )
        db.add(runnable_plan)
        await db.commit()
        plan_id = plan.id
        runnable_plan_id = runnable_plan.id
        worker.status = "destroying"
        await db.commit()

    restored = await client.patch(
        f"/api/plans/{plan_id}",
        json={"archived": False, "expected_lock_version": 0},
    )
    forked = await client.post(
        f"/api/plans/{plan_id}/fork",
        json={"base_version_id": 1},
    )
    new_run = await client.post(
        f"/api/plans/{runnable_plan_id}/runs",
        json={
            "run_type": "user_revision",
            "request": "Do not cross Worker destruction",
        },
    )

    assert restored.status_code == 409
    assert "lifecycle" in restored.json()["detail"]
    # The archive fence is checked before a fork can turn historical Worker
    # data into another executable Plan generation.
    assert forked.status_code == 409
    assert "Archived Plan" in forked.json()["detail"]
    assert new_run.status_code == 409
    assert "not ready for assignment" in new_run.json()["detail"]
    async with session_factory() as db:
        assert (
            await db.scalar(
                select(func.count(PlanAgentRun.id)).where(
                    PlanAgentRun.plan_id == runnable_plan_id
                )
            )
            == 0
        )


@pytest.mark.asyncio
async def test_public_plan_routes_are_global_only_and_frozen(client, session_factory):
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    pipeline["max_interactions"] = 5
    pipeline["planner"]["primary"] = {
        "provider": "codex",
        "model": "gpt-5.6-terra",
        "effort": "ultra",
    }
    async with session_factory() as db:
        db.add(GlobalSettings(id=1, plan_pipeline_config=pipeline))
        await db.commit()

    overridden = await client.post(
        "/api/plans",
        json={
            "input": "Attempt a per-Plan route",
            "target_repo": "/tmp",
            "pipeline_config": default_plan_pipeline_config().model_dump(mode="json"),
        },
    )
    assert overridden.status_code == 422
    assert "extra_forbidden" in overridden.text

    created = await client.post(
        "/api/plans",
        json={"input": "Use the global route", "target_repo": "/tmp"},
    )
    assert created.status_code == 201, created.text
    payload = created.json()
    assert payload["pipeline_config"] == pipeline
    assert payload["active_run"]["max_interactions"] == 5

    async with session_factory() as db:
        settings_row = await db.get(GlobalSettings, 1)
        changed = default_plan_pipeline_config().model_dump(mode="json")
        changed["max_interactions"] = 0
        settings_row.plan_pipeline_config = changed
        await db.commit()

    frozen = await client.get(f"/api/plans/{payload['id']}")
    assert frozen.json()["pipeline_config"] == pipeline


@pytest.mark.asyncio
async def test_plan_and_run_requests_reject_blank_text(client, session_factory):
    blank_plan = await client.post("/api/plans", json={"input": "   \n  "})
    assert blank_plan.status_code == 422

    created = await client.post("/api/plans", json={"input": "Valid request"})
    assert created.status_code == 201, created.text
    plan_id = created.json()["id"]
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, plan.active_run_id)
        run.status = "failed"
        run.current_stage = "failed"
        run.finished_at = datetime.utcnow()
        plan.active_run_id = None
        await db.commit()

    blank_run = await client.post(
        f"/api/plans/{plan_id}/runs",
        json={"run_type": "user_revision", "request": "   "},
    )
    assert blank_run.status_code == 422


@pytest.mark.asyncio
async def test_plan_catalog_search_and_archived_only_match_list_and_count(client):
    archived = await client.post(
        "/api/plans",
        json={"input": "Needle migration details", "title": "Archived artifact"},
    )
    active = await client.post(
        "/api/plans",
        json={"input": "Unrelated active request", "title": "Active Plan"},
    )
    assert archived.status_code == 201, archived.text
    assert active.status_code == 201, active.text
    archived_payload = archived.json()

    cancelled = await client.post(
        f"/api/plan-runs/{archived_payload['active_run']['id']}/cancel"
    )
    assert cancelled.status_code == 200, cancelled.text
    current = await client.get(f"/api/plans/{archived_payload['id']}")
    assert current.status_code == 200, current.text
    assert current.json()["display_state"] == "cancelled"
    assert current.json()["latest_run_status"] == "cancelled"
    assert current.json()["latest_run_error"] is None
    archived_result = await client.patch(
        f"/api/plans/{archived_payload['id']}",
        json={
            "archived": True,
            "expected_lock_version": current.json()["lock_version"],
        },
    )
    assert archived_result.status_code == 200, archived_result.text

    default_rows = await client.get("/api/plans", params={"q": "needle migration"})
    assert default_rows.status_code == 200
    assert default_rows.json() == []

    rows = await client.get(
        "/api/plans", params={"archived_only": True, "q": "needle migration"}
    )
    count = await client.get(
        "/api/plans/count",
        params={"archived_only": True, "q": "needle migration"},
    )
    assert [item["id"] for item in rows.json()] == [archived_payload["id"]]
    assert count.json() == {"total": 1}

    running_rows = await client.get(
        "/api/plans", params={"display_state": "planner,reviewer"}
    )
    running_count = await client.get(
        "/api/plans/count", params={"display_state": "planner,reviewer"}
    )
    assert [item["id"] for item in running_rows.json()] == [active.json()["id"]]
    assert running_count.json() == {"total": 1}


@pytest.mark.asyncio
async def test_retry_requires_exact_terminal_failed_source(client, session_factory):
    created = await client.post(
        "/api/plans",
        json={"input": "Retry safely", "target_repo": "/tmp"},
    )
    plan_id = created.json()["id"]
    source_run_id = created.json()["active_run"]["id"]
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        source = await db.get(PlanAgentRun, source_run_id)
        source.status = "failed"
        source.current_stage = "failed"
        source.error = "transient worker failure"
        source.finished_at = datetime.utcnow()
        plan.active_run_id = None
        await db.commit()

    missing = await client.post(
        f"/api/plans/{plan_id}/runs",
        json={"run_type": "retry", "request": "Retry"},
    )
    assert missing.status_code == 422
    wrong_type = await client.post(
        f"/api/plans/{plan_id}/runs",
        json={
            "run_type": "refresh_context",
            "request": "Refresh",
            "source_run_id": source_run_id,
        },
    )
    assert wrong_type.status_code == 422
    retry = await client.post(
        f"/api/plans/{plan_id}/runs",
        json={
            "run_type": "retry",
            "request": "Retry",
            "source_run_id": source_run_id,
        },
    )
    assert retry.status_code == 201, retry.text
    assert retry.json()["source_run_id"] == source_run_id


@pytest.mark.asyncio
async def test_plan_input_rejects_high_confidence_credentials(client, session_factory):
    rejected_create = await client.post(
        "/api/plans",
        json={"input": ("Use ghp_abcdefghijklmnopqrstuvwxyz1234567890ABCD directly")},
    )
    assert rejected_create.status_code == 422
    assert "Settings" in rejected_create.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Plan.id))) == 0

    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Need a safe reference", "target_task_id": target.id},
    )
    plan_id = created.json()["id"]
    run_id = created.json()["active_run"]["id"]
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        run.status = "waiting_user"
        step = PlanAgentStep(
            run_id=run.id,
            plan_id=plan_id,
            step_type="planner",
            round=1,
            generation=run.generation,
            provider="claude",
            status="completed",
        )
        db.add(step)
        await db.flush()
        input_request = PlanInputRequest(
            plan_id=plan_id,
            run_id=run.id,
            source_step_id=step.id,
            requested_by="planner",
            questions=[
                {
                    "id": "credential_reference",
                    "header": "Credential",
                    "question": "Name the configured credential reference",
                    "response_type": "text",
                    "options": [],
                    "required": True,
                }
            ],
            status="open",
            idempotency_key=f"secret-guard:{run.id}",
            opened_at=datetime.utcnow(),
        )
        db.add(input_request)
        await db.flush()
        run.open_input_request_id = input_request.id
        await db.commit()
        request_id = input_request.id
        generation = run.generation

    rejected = await client.post(
        f"/api/plan-runs/{run_id}/input-requests/{request_id}/answer",
        json={
            "expected_run_generation": generation,
            "idempotency_key": "credential-answer",
            "answers": [
                {
                    "question_id": "credential_reference",
                    "value": "ghp_abcdefghijklmnopqrstuvwxyz1234567890ABCD",
                }
            ],
        },
    )
    assert rejected.status_code == 422
    assert "Settings" in rejected.text
    async with session_factory() as db:
        request_row = await db.get(PlanInputRequest, request_id)
        run = await db.get(PlanAgentRun, run_id)
        assert request_row.status == "open"
        assert request_row.answers is None
        assert run.status == "waiting_user"

    async with session_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        plan = await db.get(Plan, plan_id)
        run.status = "failed"
        run.current_stage = "failed"
        run.finished_at = datetime.utcnow()
        plan.active_run_id = None
        await db.commit()

    rejected_revision = await client.post(
        f"/api/plans/{plan_id}/runs",
        json={
            "run_type": "user_revision",
            "request": ("Use ghp_abcdefghijklmnopqrstuvwxyz1234567890ABCD directly"),
        },
    )
    assert rejected_revision.status_code == 422
    async with session_factory() as db:
        assert (
            await db.scalar(
                select(func.count(PlanAgentRun.id)).where(
                    PlanAgentRun.plan_id == plan_id
                )
            )
            == 1
        )


@pytest.mark.asyncio
async def test_stale_confirmation_and_missing_target_hard_conflict(
    client, session_factory
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Approve exact context", "target_task_id": target.id},
    )
    plan_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
    )
    async with session_factory() as db:
        db.add(
            LogEntry(
                instance_id=1,
                task_id=target.id,
                event_type="user_message",
                role="user",
                content="Context changed after planning",
            )
        )
        await db.commit()

    stale = await client.post(
        f"/api/plan-versions/{version_id}/approve",
        json={"expected_current_version_id": version_id},
    )
    assert stale.status_code == 409
    assert stale.json()["detail"]["can_confirm"] is True
    approved = await client.post(
        f"/api/plan-versions/{version_id}/approve",
        json={
            "expected_current_version_id": version_id,
            "confirm_stale": True,
        },
    )
    assert approved.status_code == 200, approved.text

    second = await client.post(
        "/api/plans",
        json={"input": "Do not approve a missing target", "target_task_id": target.id},
    )
    second_plan_id = second.json()["id"]
    second_version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=second_plan_id,
    )
    async with session_factory() as db:
        target_row = await db.get(Task, target.id)
        await db.delete(target_row)
        await db.commit()
    hard = await client.post(
        f"/api/plan-versions/{second_version_id}/approve",
        json={
            "expected_current_version_id": second_version_id,
            "confirm_stale": True,
        },
    )
    assert hard.status_code == 409
    assert hard.json()["detail"]["hard_conflict"] is True
    assert "target_task_missing" in hard.json()["detail"]["hard_conflicts"]


@pytest.mark.asyncio
async def test_missing_legacy_repository_snapshot_is_confirmable_not_blocking(
    client,
    session_factory,
):
    async def create_legacy_version(title: str) -> tuple[int, int]:
        created = await client.post(
            "/api/plans",
            json={"input": title, "target_repo": "/tmp"},
        )
        assert created.status_code == 201, created.text
        plan_id = created.json()["id"]
        version_id = await _finish_current_run_with_version(
            session_factory,
            plan_id=plan_id,
        )
        async with session_factory() as db:
            version = await db.get(PlanVersion, version_id)
            version.repo_revision = None
            version.reviewer_repo_revision = None
            await db.commit()
        return plan_id, version_id

    with patch(
        "backend.services.plan_staleness.capture_repo_revision",
        new=AsyncMock(
            return_value={
                "available": True,
                "head": "current-head",
                "dirty_sha256": "clean",
            }
        ),
    ):
        _reject_plan_id, reject_version_id = await create_legacy_version(
            "Reject migrated Version",
        )
        stale = await client.get(f"/api/plan-versions/{reject_version_id}/staleness")
        assert stale.status_code == 200, stale.text
        assert stale.json()["stale"] is True
        assert stale.json()["hard_conflict"] is False
        assert stale.json()["can_confirm"] is True
        assert stale.json()["reasons"] == ["captured_repository_state_missing"]

        rejected = await client.post(
            f"/api/plan-versions/{reject_version_id}/reject",
            json={"expected_current_version_id": reject_version_id},
        )
        assert rejected.status_code == 200, rejected.text
        assert rejected.json()["human_decision"] == "rejected"

        _approve_plan_id, approve_version_id = await create_legacy_version(
            "Approve migrated Version",
        )
        unconfirmed = await client.post(
            f"/api/plan-versions/{approve_version_id}/approve",
            json={"expected_current_version_id": approve_version_id},
        )
        assert unconfirmed.status_code == 409
        assert unconfirmed.json()["detail"]["can_confirm"] is True

        approved = await client.post(
            f"/api/plan-versions/{approve_version_id}/approve",
            json={
                "expected_current_version_id": approve_version_id,
                "confirm_stale": True,
            },
        )
        assert approved.status_code == 200, approved.text
        assert approved.json()["human_decision"] == "approved"

        _execution_plan_id, execution_version_id = await create_legacy_version(
            "Execute migrated Version",
        )
        blocked_execution = await client.post(
            f"/api/plan-versions/{execution_version_id}/create-execution-task",
            json={
                "expected_current_version_id": execution_version_id,
                "approve_if_pending": True,
            },
        )
        assert blocked_execution.status_code == 409
        assert blocked_execution.json()["detail"]["can_confirm"] is True

        confirmed_execution = await client.post(
            f"/api/plan-versions/{execution_version_id}/create-execution-task",
            json={
                "expected_current_version_id": execution_version_id,
                "approve_if_pending": True,
                "confirm_stale": True,
            },
        )
        assert confirmed_execution.status_code == 201, confirmed_execution.text


@pytest.mark.asyncio
async def test_approve_and_create_execution_is_atomic_and_history_stays_linked(
    client, session_factory
):
    created = await client.post(
        "/api/plans",
        json={"input": "Create an execution task", "target_repo": "/tmp"},
    )
    plan_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
    )
    executed = await client.post(
        f"/api/plan-versions/{version_id}/create-execution-task",
        json={
            "expected_current_version_id": version_id,
            "approve_if_pending": True,
        },
    )
    assert executed.status_code == 201, executed.text
    execution_task_id = executed.json()["execution_task_id"]
    assert executed.json()["version"]["human_decision"] == "approved"

    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        version2 = PlanVersion(
            plan_id=plan.id,
            version_number=2,
            parent_version_id=version_id,
            content="# New current version",
            repo_revision={"available": False, "reason": "not_git"},
            review_verdict="approve",
        )
        db.add(version2)
        await db.flush()
        plan.current_version_id = version2.id
        await db.commit()

    resource = await client.get(f"/api/plans/{plan_id}")
    assert resource.status_code == 200, resource.text
    payload = resource.json()
    assert payload["application"] is None
    assert payload["applications"][0]["plan_version_id"] == version_id
    assert payload["applications"][0]["execution_task_id"] == execution_task_id
    assert payload["applications"][0]["execution_task_available"] is True

    versions = await client.get(f"/api/plans/{plan_id}/versions")
    assert versions.status_code == 200, versions.text
    version_states = {
        item["version_number"]: item["display_state"] for item in versions.json()
    }
    assert version_states == {1: "applied", 2: "awaiting_review"}

    deleted_execution = await client.delete(f"/api/tasks/{execution_task_id}")
    assert deleted_execution.status_code == 200, deleted_execution.text

    async with session_factory() as db:
        assert await db.get(Task, execution_task_id) is None
        assert await db.scalar(
            select(PlanApplication.id).where(
                PlanApplication.plan_version_id == version_id,
                PlanApplication.execution_task_id == execution_task_id,
            )
        ) is not None

    missing_target = await client.get(f"/api/plans/{plan_id}")
    assert missing_target.status_code == 200, missing_target.text
    missing_application = missing_target.json()["applications"][0]
    assert missing_application["execution_task_available"] is False


@pytest.mark.asyncio
async def test_execution_materialization_rechecks_control_after_snapshot_reset(
    client,
    session_factory,
    monkeypatch,
):
    from backend.api import plan_resources as api

    created = await client.post(
        "/api/plans",
        json={"input": "Recheck authorization before apply", "target_repo": "/tmp"},
    )
    assert created.status_code == 201, created.text
    plan_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
    )
    access = AsyncMock(side_effect=[True, False])
    monkeypatch.setattr(api, "_has_plan_access", access)

    response = await client.post(
        f"/api/plan-versions/{version_id}/create-execution-task",
        json={
            "expected_current_version_id": version_id,
            "approve_if_pending": True,
        },
    )

    assert response.status_code == 403
    assert access.await_count == 2
    async with session_factory() as db:
        version = await db.get(PlanVersion, version_id)
        assert version is not None and version.human_decision == "pending"
        assert await db.scalar(select(func.count(PlanApplication.id))) == 0
        assert await db.scalar(select(func.count(Task.id))) == 0


@pytest.mark.asyncio
async def test_execution_materialization_rolls_back_inline_approval_on_failure(
    client,
    session_factory,
):
    created = await client.post(
        "/api/plans",
        json={"input": "Keep approval atomic with Task creation", "target_repo": "/tmp"},
    )
    plan_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
    )
    async with session_factory() as db:
        initial_plan = await db.get(Plan, plan_id)
        assert initial_plan is not None
        initial_lock_version = initial_plan.lock_version
        initial_task_count = int(await db.scalar(select(func.count(Task.id))) or 0)

    with patch(
        "backend.services.plan_service.stage_task_record",
        new=AsyncMock(side_effect=RuntimeError("injected Task staging failure")),
    ):
        async with session_factory() as db:
            with pytest.raises(RuntimeError, match="injected Task staging failure"):
                await materialize_execution_task(
                    db,
                    plan_id=plan_id,
                    version_id=version_id,
                    expected_current_version_id=version_id,
                    confirm_stale=False,
                    approve_if_pending=True,
                    actor_id=42,
                )

    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        version = await db.get(PlanVersion, version_id)
        assert plan is not None and version is not None
        assert plan.lock_version == initial_lock_version
        assert version.human_decision == "pending"
        assert version.decided_at is None
        assert await db.scalar(
            select(func.count(PlanApplication.id)).where(
                PlanApplication.plan_version_id == version_id
            )
        ) == 0
        assert int(await db.scalar(select(func.count(Task.id))) or 0) == initial_task_count


@pytest.mark.asyncio
async def test_execution_materialization_rejects_active_refresh_run(
    client,
    session_factory,
):
    created = await client.post(
        "/api/plans",
        json={"input": "Do not execute while refreshing", "target_repo": "/tmp"},
    )
    plan_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
    )
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        assert plan is not None
        refresh = PlanAgentRun(
            plan_id=plan.id,
            run_type="refresh",
            base_version_id=version_id,
            request_text="Refresh current context",
            status="queued",
            current_stage="planner",
            pipeline_config=plan.pipeline_config,
        )
        db.add(refresh)
        await db.flush()
        plan.active_run_id = refresh.id
        plan.lock_version += 1
        await db.commit()

    async with session_factory() as db:
        with pytest.raises(HTTPException) as exc_info:
            await materialize_execution_task(
                db,
                plan_id=plan_id,
                version_id=version_id,
                expected_current_version_id=version_id,
                confirm_stale=False,
                approve_if_pending=True,
                actor_id=42,
            )
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["code"] == "plan_active_run"

    async with session_factory() as db:
        version = await db.get(PlanVersion, version_id)
        assert version is not None and version.human_decision == "pending"
        assert await db.scalar(select(func.count(PlanApplication.id))) == 0


@pytest.mark.asyncio
async def test_execution_materialization_loses_cleanly_to_concurrent_plan_writer(
    tmp_path,
):
    """A WAL snapshot race is a deterministic Plan CAS conflict, never BUSY."""

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from backend.database import Base
    from backend.services.plan_staleness import version_staleness as real_staleness

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'execution-materialization.db'}",
        connect_args={"timeout": 1},
    )
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        pipeline = default_plan_pipeline_config().model_dump(mode="json")
        async with sessions() as setup:
            plan = Plan(
                title="Execution materialization WAL fence",
                initial_request="Create one exact Task",
                target_repo="/tmp",
                target_branch="main",
                pipeline_config=pipeline,
            )
            setup.add(plan)
            await setup.flush()
            run = PlanAgentRun(
                plan_id=plan.id,
                run_type="initial",
                status="completed",
                current_stage="complete",
                pipeline_config=pipeline,
                finished_at=datetime.utcnow(),
            )
            setup.add(run)
            await setup.flush()
            version = PlanVersion(
                plan_id=plan.id,
                version_number=1,
                produced_by_run_id=run.id,
                content="# Exact candidate",
                repo_revision={"available": False, "reason": "not_git"},
                reviewer_repo_revision={"available": False, "reason": "not_git"},
                review_verdict="approve",
                reviewed_at=datetime.utcnow(),
            )
            setup.add(version)
            await setup.flush()
            plan.current_version_id = version.id
            run.result_version_id = version.id
            await setup.commit()
            plan_id = plan.id
            version_id = version.id

        staleness_entered = asyncio.Event()
        release_staleness = asyncio.Event()

        async def blocked_staleness(db, current_plan, current_version):
            result = await real_staleness(db, current_plan, current_version)
            staleness_entered.set()
            await release_staleness.wait()
            return result

        async def materialize():
            async with sessions() as materializer:
                with patch(
                    "backend.services.plan_staleness.version_staleness",
                    new=blocked_staleness,
                ):
                    return await materialize_execution_task(
                        materializer,
                        plan_id=plan_id,
                        version_id=version_id,
                        expected_current_version_id=version_id,
                        confirm_stale=True,
                        approve_if_pending=True,
                        actor_id=42,
                    )

        pending = asyncio.create_task(materialize())
        await asyncio.wait_for(staleness_entered.wait(), timeout=2)
        async with sessions() as writer:
            changed = await writer.execute(
                update(Plan)
                .where(Plan.id == plan_id)
                .values(
                    archived_at=datetime.utcnow(),
                    lock_version=Plan.lock_version + 1,
                )
            )
            assert changed.rowcount == 1
            await writer.commit()
        release_staleness.set()

        with pytest.raises(HTTPException) as exc_info:
            await pending
        assert exc_info.value.status_code == 409
        assert (
            exc_info.value.detail["code"]
            == "plan_changed_during_execution_materialization"
        )

        async with sessions() as verify:
            persisted_version = await verify.get(PlanVersion, version_id)
            assert persisted_version is not None
            assert persisted_version.human_decision == "pending"
            assert await verify.scalar(select(func.count(PlanApplication.id))) == 0
            assert await verify.scalar(select(func.count(Task.id))) == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_execution_materialization_reauthorizes_concurrent_exact_winner(
    tmp_path,
):
    """A CAS-losing replay must not disclose a winner after ACL revocation."""

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from backend.database import Base
    from backend.services.plan_staleness import version_staleness as real_staleness

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'execution-winner-acl.db'}",
        connect_args={"timeout": 1},
    )
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        pipeline = default_plan_pipeline_config().model_dump(mode="json")
        async with sessions() as setup:
            plan = Plan(
                title="Concurrent exact winner ACL",
                initial_request="Create one protected Task",
                target_repo="/tmp",
                target_branch="main",
                pipeline_config=pipeline,
            )
            setup.add(plan)
            await setup.flush()
            run = PlanAgentRun(
                plan_id=plan.id,
                run_type="initial",
                status="completed",
                current_stage="complete",
                pipeline_config=pipeline,
                finished_at=datetime.utcnow(),
            )
            setup.add(run)
            await setup.flush()
            version = PlanVersion(
                plan_id=plan.id,
                version_number=1,
                produced_by_run_id=run.id,
                content="# Protected exact winner",
                repo_revision={"available": False, "reason": "not_git"},
                reviewer_repo_revision={"available": False, "reason": "not_git"},
                review_verdict="approve",
                reviewed_at=datetime.utcnow(),
            )
            setup.add(version)
            await setup.flush()
            plan.current_version_id = version.id
            run.result_version_id = version.id
            await setup.commit()
            plan_id = plan.id
            version_id = version.id

        staleness_entered = asyncio.Event()
        release_staleness = asyncio.Event()
        revoked = AsyncMock(side_effect=HTTPException(403, "Plan access was revoked"))

        async def blocked_staleness(db, current_plan, current_version):
            result = await real_staleness(db, current_plan, current_version)
            staleness_entered.set()
            await release_staleness.wait()
            return result

        async def losing_replay():
            async with sessions() as materializer:
                with patch(
                    "backend.services.plan_staleness.version_staleness",
                    new=blocked_staleness,
                ):
                    return await materialize_execution_task(
                        materializer,
                        plan_id=plan_id,
                        version_id=version_id,
                        expected_current_version_id=version_id,
                        confirm_stale=True,
                        approve_if_pending=True,
                        actor_id=42,
                        authorize_locked_plan=revoked,
                    )

        pending = asyncio.create_task(losing_replay())
        await asyncio.wait_for(staleness_entered.wait(), timeout=2)
        async with sessions() as writer:
            execution = Task(
                title="Protected execution winner",
                description="Implement the exact Plan Version",
                status="pending",
                target_repo="/tmp",
                target_branch="main",
            )
            writer.add(execution)
            await writer.flush()
            winner_version = await writer.get(PlanVersion, version_id)
            winner_version.human_decision = "approved"
            winner_version.decided_at = datetime.utcnow()
            winner_version.decided_by = 7
            writer.add(
                PlanApplication(
                    plan_id=plan_id,
                    plan_version_id=version_id,
                    application_type="execution_task",
                    execution_task_id=execution.id,
                    applied_by=7,
                )
            )
            changed = await writer.execute(
                update(Plan)
                .where(Plan.id == plan_id)
                .values(lock_version=Plan.lock_version + 1)
            )
            assert changed.rowcount == 1
            await writer.commit()

        release_staleness.set()
        with pytest.raises(HTTPException) as exc_info:
            await pending
        assert exc_info.value.status_code == 403
        revoked.assert_awaited_once()

        async with sessions() as verify:
            assert await verify.scalar(select(func.count(PlanApplication.id))) == 1
            assert await verify.scalar(select(func.count(Task.id))) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_undecided_superseded_version_has_derived_historical_state(
    client,
    session_factory,
):
    created = await client.post(
        "/api/plans",
        json={"input": "Revise an undecided Version", "target_repo": "/tmp"},
    )
    plan_id = created.json()["id"]
    version1_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
        content="# Undecided v1",
    )
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        version1 = await db.get(PlanVersion, version1_id)
        version2 = PlanVersion(
            plan_id=plan.id,
            version_number=2,
            parent_version_id=version1.id,
            content="# Current v2",
            review_verdict="approve",
        )
        db.add(version2)
        await db.flush()
        version1.superseded_by_version_id = version2.id
        plan.current_version_id = version2.id
        await db.commit()

    versions = await client.get(f"/api/plans/{plan_id}/versions")
    assert versions.status_code == 200, versions.text
    by_number = {item["version_number"]: item for item in versions.json()}
    assert by_number[1]["human_decision"] == "pending"
    assert by_number[1]["display_state"] == "superseded"
    assert by_number[2]["display_state"] == "awaiting_review"


@pytest.mark.asyncio
async def test_execution_task_materializer_is_directly_callable_and_idempotent(
    client, session_factory, monkeypatch
):
    created = await client.post(
        "/api/plans",
        json={
            "input": "Expose a stable execution seam",
            "target_repo": "/tmp",
            "timeout_hours": 3.5,
        },
    )
    plan_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
    )

    async with session_factory() as db:
        # Worker is only an execution location.  Applying the approved Plan
        # must retain this native Manager principal so WorkerProxy can delegate
        # it, rather than silently replacing it with system/member/sandbox.
        worker = Worker(
            name="execution-principal-worker",
            status="ready",
        )
        db.add(worker)
        await db.flush()
        worker_id = worker.id
        plan = await db.get(Plan, plan_id)
        plan.worker_id = worker_id
        await db.commit()
        from backend.services import plan_staleness

        monkeypatch.setattr(
            plan_staleness,
            "version_staleness",
            AsyncMock(return_value={
                "stale": False,
                "reasons": [],
                "hard_conflict": False,
                "hard_conflicts": [],
                "can_confirm": False,
            }),
        )
        principal = {
            "execution_user_id": None,
            "execution_user_role": "super_admin",
            "execution_mode": "unrestricted",
            "execution_principal_kind": "deployment_token",
        }
        first = await materialize_execution_task(
            db,
            plan_id=plan_id,
            version_id=version_id,
            expected_current_version_id=version_id,
            confirm_stale=False,
            approve_if_pending=True,
            actor_id=42,
            execution_principal=principal,
            execution_metadata={
                "auto_run_id": "auto-7",
                "created_from_plan_id": -1,
            },
        )
        replay = await materialize_execution_task(
            db,
            plan_id=plan_id,
            version_id=version_id,
            expected_current_version_id=version_id,
            confirm_stale=False,
            approve_if_pending=False,
            actor_id=42,
            execution_principal=principal,
            execution_metadata={"auto_run_id": "ignored-on-replay"},
        )

        assert first.created is True
        assert replay.created is False
        assert replay.task.id == first.task.id
        assert replay.application.id == first.application.id
        assert first.task.metadata_["auto_run_id"] == "auto-7"
        assert first.task.metadata_["created_from_plan_id"] == plan_id
        assert first.task.metadata_["created_from_plan_version_id"] == version_id
        assert first.task.provider == settings.default_provider
        assert first.task.model == (
            settings.default_codex_model
            if settings.default_provider == "codex"
            else settings.default_model
        )
        assert first.task.effort_level == settings.default_effort
        assert first.task.codex_service_tier == "default"
        assert first.task.timeout_hours == 3.5
        assert first.task.worker_id == worker_id
        assert first.task.execution_user_id is None
        assert first.task.execution_user_role == "super_admin"
        assert first.task.execution_mode == "unrestricted"
        assert first.task.execution_principal_kind == "deployment_token"
        assert (
            await db.scalar(
                select(func.count(PlanApplication.id)).where(
                    PlanApplication.plan_version_id == version_id
                )
            )
            == 1
        )


@pytest.mark.asyncio
async def test_execution_task_replay_survives_later_plan_state_changes(
    client,
    session_factory,
):
    """Exact-Version idempotency outlives archive, Refresh, and supersession."""

    created = await client.post(
        "/api/plans",
        json={"input": "Keep the immutable execution result", "target_repo": "/tmp"},
    )
    plan_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
    )

    async with session_factory() as db:
        first = await materialize_execution_task(
            db,
            plan_id=plan_id,
            version_id=version_id,
            expected_current_version_id=version_id,
            confirm_stale=False,
            approve_if_pending=True,
            actor_id=42,
        )
        plan = await db.get(Plan, plan_id, populate_existing=True)
        version = await db.get(PlanVersion, version_id, populate_existing=True)
        assert plan is not None and version is not None
        refresh = PlanAgentRun(
            plan_id=plan.id,
            run_type="refresh",
            base_version_id=version.id,
            request_text="Refresh after the execution Task was created",
            status="queued",
            current_stage="planner",
            pipeline_config=plan.pipeline_config,
        )
        db.add(refresh)
        await db.flush()
        successor = PlanVersion(
            plan_id=plan.id,
            version_number=version.version_number + 1,
            parent_version_id=version.id,
            content="# Later historical Version",
            review_verdict="approve",
        )
        db.add(successor)
        await db.flush()
        version.superseded_by_version_id = successor.id
        plan.current_version_id = successor.id
        plan.active_run_id = refresh.id
        plan.archived_at = datetime.utcnow()
        plan.lock_version += 1
        await db.commit()

        replay = await materialize_execution_task(
            db,
            plan_id=plan_id,
            version_id=version_id,
            expected_current_version_id=version_id,
            confirm_stale=False,
            approve_if_pending=False,
            actor_id=42,
        )

        assert replay.created is False
        assert replay.task.id == first.task.id
        assert replay.application.id == first.application.id
        assert (
            await db.scalar(
                select(func.count(PlanApplication.id)).where(
                    PlanApplication.plan_version_id == version_id
                )
            )
            == 1
        )


@pytest.mark.asyncio
async def test_worker_import_creates_idempotent_inert_mirror(client, session_factory):
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    body = {
        "protocol": 3,
        "plan_id": 5101,
        "run_id": 5201,
        "manager_claim_generation": 4,
        "title": "Relayed Plan",
        "initial_request": "Design on the Worker",
        "priority": 2,
        "pipeline_config": pipeline,
        "run_type": "initial",
        "request_text": "Design on the Worker",
        "max_interactions": 3,
    }
    created = await client.post("/api/plans/worker-import", json=body)
    assert created.status_code == 200, created.text
    assert created.json()["run"]["status"] == "queued"

    replay = await client.post("/api/plans/worker-import", json=body)
    assert replay.status_code == 200, replay.text
    # A later Manager claim may have a higher generation after any number of
    # restarts; it must map to the same Worker-local Run.
    body["manager_claim_generation"] = 99
    replay_after_restarts = await client.post("/api/plans/worker-import", json=body)
    assert replay_after_restarts.status_code == 200, replay_after_restarts.text
    changed = {**body, "request_text": "Different imported request"}
    rejected = await client.post("/api/plans/worker-import", json=changed)
    assert rejected.status_code == 409
    async with session_factory() as db:
        plan = await db.get(Plan, 5101)
        run = await db.get(PlanAgentRun, 5201)
        assert plan.relay_origin == "manager_v1"
        assert plan.worker_id is None
        assert plan.active_run_id == run.id
        assert run.relay_origin == "manager_v1"
        assert run.generation == 0
        assert await db.scalar(select(func.count(Plan.id))) == 1
        assert await db.scalar(select(func.count(PlanAgentRun.id))) == 1


@pytest.mark.asyncio
async def test_related_plan_capacity_is_atomic_for_concurrent_creates(
    client, session_factory
):
    target = await _target(client, session_factory)
    responses = await asyncio.gather(
        *(
            client.post(
                "/api/plans",
                json={"input": f"Concurrent Plan {index}", "target_task_id": target.id},
            )
            for index in range(4)
        )
    )
    assert sorted(response.status_code for response in responses) == [
        201,
        201,
        201,
        429,
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ("create", "run"))
async def test_plan_admission_loses_cleanly_to_concurrent_wal_receipt(
    tmp_path,
    transition,
):
    """First-class Plan writers end API reads before the Task receipt CAS."""

    from fastapi import HTTPException
    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from backend.database import Base
    from backend.services.plan_service import (
        create_plan_run,
        create_plan_with_run,
    )
    from backend.tests.worker_termination_helpers import (
        persist_active_worker_receipt,
    )

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / f'plan-{transition}.db'}",
        connect_args={"timeout": 1},
    )
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        pipeline = default_plan_pipeline_config().model_dump(mode="json")
        async with sessions() as setup:
            target = Task(
                title="WAL receipt target",
                description="d",
                status="completed",
                session_id="wal-plan-session",
                target_repo="/tmp",
            )
            setup.add(target)
            await setup.flush()
            target_id = target.id
            plan_id = None
            if transition == "run":
                plan = Plan(
                    title="Inactive WAL Plan",
                    initial_request="revise safely",
                    target_task_id=target_id,
                    target_repo="/tmp",
                    target_branch="main",
                    worker_id=None,
                    pipeline_config=pipeline,
                    active_run_id=None,
                )
                setup.add(plan)
                await setup.flush()
                plan_id = plan.id
            await setup.commit()

        async with sessions() as creator:
            # Freeze an old WAL read snapshot, then let a different connection
            # durably admit the receipt before the Plan's first Task write.
            observed_target = await creator.get(Task, target_id)
            assert observed_target is not None
            observed_plan = (
                await creator.get(Plan, plan_id)
                if plan_id is not None
                else None
            )
            assert creator.in_transaction()
            await persist_active_worker_receipt(sessions, target_id)

            with pytest.raises(HTTPException) as rejected:
                if transition == "create":
                    await create_plan_with_run(
                        creator,
                        title="Must not be created",
                        initial_request="receipt owns admission",
                        attachments=None,
                        target_task_id=target_id,
                        project_id=None,
                        target_repo="/tmp",
                        target_branch="main",
                        worker_id=None,
                        priority=0,
                        timeout_hours=None,
                        created_by=None,
                        pipeline_config=pipeline,
                        context_session_id=observed_target.session_id,
                        context_log_id=None,
                        context_snapshot=None,
                        repo_revision=None,
                    )
                else:
                    assert observed_plan is not None
                    await create_plan_run(
                        creator,
                        plan=observed_plan,
                        run_type="user_revision",
                        request_text="receipt owns admission",
                        attachments=None,
                        base_version_id=None,
                        expected_current_version_id=None,
                        context_session_id=observed_target.session_id,
                        context_log_id=None,
                        context_snapshot=None,
                        repo_revision=None,
                        project_id=observed_plan.project_id,
                        target_repo=observed_plan.target_repo,
                        target_branch=observed_plan.target_branch,
                        worker_id=None,
                    )

            assert rejected.value.status_code == 409
            assert "termination receipt" in rejected.value.detail

        async with sessions() as verify:
            assert await verify.scalar(select(func.count(PlanAgentRun.id))) == 0
            expected_plans = 0 if transition == "create" else 1
            assert await verify.scalar(select(func.count(Plan.id))) == expected_plans
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_related_plan_capacity_applies_to_forks(client, session_factory):
    target = await _target(client, session_factory)
    source = await client.post(
        "/api/plans",
        json={"input": "Fork source", "target_task_id": target.id},
    )
    assert source.status_code == 201, source.text
    source_id = source.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory, plan_id=source_id
    )
    responses = []
    for index in range(4):
        responses.append(
            await client.post(
                f"/api/plans/{source_id}/fork",
                json={
                    "base_version_id": version_id,
                    "title": f"Fork {index}",
                },
            )
        )
    assert [response.status_code for response in responses] == [201, 201, 201, 429]


@pytest.mark.asyncio
async def test_plan_catalog_paginates_before_bounded_batch_projection(
    client, session_factory
):
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        for index in range(25):
            plan = Plan(
                title=f"Bulk {index:02d}",
                initial_request="Measure catalog queries",
                pipeline_config=pipeline,
                priority=0,
            )
            db.add(plan)
            await db.flush()
            run = PlanAgentRun(
                plan_id=plan.id,
                run_type="initial",
                request_text=plan.initial_request,
                pipeline_config=pipeline,
                status="queued",
                current_stage="planner",
            )
            db.add(run)
            await db.flush()
            plan.active_run_id = run.id
        await db.commit()

    engine = session_factory.kw["bind"].sync_engine
    statements = 0

    def count_statement(*_args):
        nonlocal statements
        statements += 1

    event.listen(engine, "before_cursor_execute", count_statement)
    try:
        response = await client.get("/api/plans?limit=5&offset=10")
    finally:
        event.remove(engine, "before_cursor_execute", count_statement)
    assert response.status_code == 200, response.text
    assert len(response.json()) == 5
    # The projection performs a fixed set of bulk queries, not one set per Plan.
    assert statements <= 10

    statements = 0
    event.listen(engine, "before_cursor_execute", count_statement)
    try:
        counted = await client.get("/api/plans/count")
    finally:
        event.remove(engine, "before_cursor_execute", count_statement)
    assert counted.status_code == 200, counted.text
    assert counted.json()["total"] >= 25
    assert statements <= 2


@pytest.mark.asyncio
async def test_worker_import_requires_exact_attachment_digest(client):
    uploaded = await client.post(
        "/api/uploads",
        files={"files": ("requirements.txt", b"exact bytes", "text/plain")},
    )
    assert uploaded.status_code == 200, uploaded.text
    item = uploaded.json()[0]
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    body = {
        "protocol": 3,
        "plan_id": 5151,
        "run_id": 5251,
        "manager_claim_generation": 0,
        "title": "Attachment Plan",
        "initial_request": "Use the attachment",
        "priority": 0,
        "pipeline_config": pipeline,
        "run_type": "initial",
        "request_text": "Use the attachment",
        "max_interactions": 3,
        "file_paths": [item["path"]],
        "image_paths": [],
        "attachments": [
            {
                "url": item["url"],
                "name": item["filename"],
                "is_image": False,
            }
        ],
        "attachment_manifest": [
            {
                "path": item["path"],
                "size": len(b"exact bytes"),
                "sha256": "0" * 64,
            }
        ],
    }
    rejected = await client.post("/api/plans/worker-import", json=body)
    assert rejected.status_code == 409
    assert "digest/size" in rejected.text

    body["attachment_manifest"][0]["sha256"] = hashlib.sha256(
        b"exact bytes"
    ).hexdigest()
    accepted = await client.post("/api/plans/worker-import", json=body)
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["attachment_receipt"] == body["attachment_manifest"]


@pytest.mark.asyncio
async def test_worker_materializes_exact_version_idempotently(client, session_factory):
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    body = {
        "protocol": 3,
        "plan_id": 5301,
        "title": "Migrated Plan",
        "initial_request": "Plan before migration",
        "priority": 0,
        "pipeline_config": pipeline,
        "version": {
            "source_version_id": 5401,
            "version_number": 3,
            "content": "# Immutable v3",
            "context_session_id": "session-before-migration",
            "context_log_id": 88,
            "context_snapshot": "private relay context",
            "review_verdict": "approve",
            "review_exhausted": False,
            "human_decision": "approved",
        },
    }
    created = await client.post(
        "/api/plans/worker-materialize-version",
        json=body,
    )
    assert created.status_code == 200, created.text
    remote_version_id = created.json()["id"]
    replay = await client.post(
        "/api/plans/worker-materialize-version",
        json=body,
    )
    assert replay.status_code == 200, replay.text
    assert replay.json()["id"] == remote_version_id

    async with session_factory() as db:
        plan = await db.get(Plan, 5301)
        version = await db.get(PlanVersion, remote_version_id)
        assert plan.current_version_id == version.id
        assert version.version_number == 3
        assert version.content == "# Immutable v3"
        assert version.human_decision == "approved"
        assert (
            await db.scalar(
                select(func.count(PlanVersion.id)).where(PlanVersion.plan_id == plan.id)
            )
            == 1
        )


@pytest.mark.asyncio
async def test_worker_outcome_maps_exact_audit_and_preserves_manager_context(
    session_factory,
):
    now = datetime.utcnow()
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        plan = Plan(
            title="Manager authority",
            initial_request="Plan this",
            worker_id=7,
            pipeline_config=pipeline,
            priority=0,
        )
        db.add(plan)
        await db.flush()
        base = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            content="# Manager base",
            context_session_id="manager-session",
            context_log_id=70,
            human_decision="approved",
        )
        db.add(base)
        await db.flush()
        plan.current_version_id = base.id
        run = PlanAgentRun(
            plan_id=plan.id,
            worker_id=7,
            run_type="initial",
            base_version_id=base.id,
            request_text="Plan this",
            context_session_id="manager-session",
            context_log_id=91,
            context_snapshot="manager-only context",
            pipeline_config=pipeline,
            status="running",
            current_stage="planner",
            generation=2,
            max_interactions=3,
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        await db.commit()
        plan_id = plan.id
        run_id = run.id
        base_version_id = base.id
        initial_lock_version = plan.lock_version

    payload = {
        "protocol": 3,
        "base_worker_version_id": 800,
        "run": {
            "id": run_id,
            "plan_id": plan_id,
            "run_type": "initial",
            "status": "waiting_user",
            "current_stage": "reviewer",
            "base_version_id": None,
            "result_version_id": None,
            "draft_content": "# Worker candidate",
            "draft_step_id": 701,
            "draft_repo_revision": {"commit": "abc"},
            "request_text": "Plan this",
            "round": 1,
            "generation": 1,
            "instance_id": None,
            "worker_id": None,
            "open_input_request_id": 901,
            "interaction_count": 1,
            "max_interactions": 3,
            "execution_seconds": 12.5,
            "last_execution_started_at": None,
            "review_verdict": None,
            "review_feedback": None,
            "review_exhausted": False,
            "error": None,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "finished_at": None,
            "steps": [
                {
                    "id": 701,
                    "run_id": run_id,
                    "plan_id": plan_id,
                    "plan_version_id": None,
                    "input_request_id": None,
                    "step_type": "planner",
                    "round": 1,
                    "generation": 1,
                    "provider": "codex",
                    "model": "gpt-test",
                    "effort": "high",
                    "route_slot": "primary",
                    "status": "completed",
                    "output": "planner output",
                    "error": None,
                    "last_delta_at": now.isoformat(),
                    "streamed_output_chars": 42,
                    "last_event_type": "turn.completed",
                    "started_at": now.isoformat(),
                    "finished_at": now.isoformat(),
                },
                {
                    "id": 702,
                    "run_id": run_id,
                    "plan_id": plan_id,
                    "plan_version_id": None,
                    "input_request_id": 901,
                    "step_type": "reviewer",
                    "round": 1,
                    "generation": 1,
                    "provider": "claude",
                    "model": "claude-test",
                    "effort": "medium",
                    "route_slot": "fallback",
                    "status": "completed",
                    "output": "need input",
                    "error": None,
                    "started_at": now.isoformat(),
                    "finished_at": now.isoformat(),
                },
            ],
            "input_requests": [
                {
                    "id": 901,
                    "plan_id": plan_id,
                    "run_id": run_id,
                    "source_step_id": 702,
                    "requested_by": "reviewer",
                    "reason": "Need deployment target",
                    "questions": [
                        {
                            "id": "target",
                            "header": "Target",
                            "question": "Where should this run?",
                            "response_type": "text",
                            "options": [],
                            "required": True,
                        }
                    ],
                    "status": "open",
                    "answers": None,
                    "response_text": None,
                    "attachments": None,
                    "answered_by": None,
                    "opened_at": now.isoformat(),
                    "answered_at": None,
                    "created_at": now.isoformat(),
                }
            ],
        },
        "versions": [],
    }
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, run_id)
        await apply_worker_plan_outcome(
            db,
            plan=plan,
            run=run,
            worker_id=7,
            expected_generation=2,
            payload=payload,
        )

    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, run_id)
        version = await db.get(PlanVersion, plan.current_version_id)
        input_request = await db.get(PlanInputRequest, run.open_input_request_id)
        assert run.status == "waiting_user"
        # Manager and Worker generations are independent protocol-v3 fences.
        assert run.generation == 2
        assert plan.lock_version == initial_lock_version + 1
        assert plan.current_version_id == base_version_id
        assert version.id == base_version_id
        assert run.result_version_id is None
        assert run.draft_content == "# Worker candidate"
        assert run.draft_repo_revision == {"commit": "abc"}
        draft_step = await db.get(PlanAgentStep, run.draft_step_id)
        assert draft_step.worker_step_id == 701
        assert draft_step.last_delta_at == now
        assert draft_step.streamed_output_chars == 42
        assert draft_step.last_event_type == "turn.completed"
        assert input_request.worker_input_request_id == 901
        assert input_request.status == "open"
        base = await db.get(PlanVersion, base_version_id)
        assert base.superseded_by_version_id is None


@pytest.mark.asyncio
async def test_canonical_create_and_revision_keep_stable_plan_identity(
    client, session_factory
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Design the change", "target_task_id": target.id},
    )
    assert created.status_code == 201, created.text
    payload = created.json()
    plan_id = payload["id"]
    first_run_id = payload["active_run"]["id"]
    assert payload["target_task_id"] == target.id
    assert payload["display_state"] == "planner"

    async with session_factory() as db:
        assert (
            await db.scalar(select(func.count(Task.id)).where(Task.mode == "plan")) == 0
        )
        plan = await db.get(Plan, plan_id)
        first_run = await db.get(PlanAgentRun, first_run_id)
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            produced_by_run_id=first_run.id,
            content="# v1",
            context_session_id=first_run.context_session_id,
            context_log_id=first_run.context_log_id,
            repo_revision=first_run.repo_revision,
            review_verdict="approve",
            reviewed_at=datetime.utcnow(),
        )
        db.add(version)
        await db.flush()
        plan.current_version_id = version.id
        plan.active_run_id = None
        first_run.status = "completed"
        first_run.current_stage = "complete"
        first_run.result_version_id = version.id
        first_run.finished_at = datetime.utcnow()
        await db.commit()
        version_id = version.id

    revised = await client.post(
        f"/api/plans/{plan_id}/runs",
        json={
            "run_type": "user_revision",
            "request": "Add rollback details",
            "base_version_id": version_id,
            "expected_current_version_id": version_id,
        },
    )
    assert revised.status_code == 201, revised.text
    revised_payload = revised.json()
    assert revised_payload["plan_id"] == plan_id
    assert revised_payload["id"] != first_run_id
    assert revised_payload["base_version_id"] == version_id

    async with session_factory() as db:
        assert await db.scalar(select(func.count(Plan.id))) == 1
        assert await db.scalar(select(func.count(PlanAgentRun.id))) == 2
        assert (
            await db.scalar(select(func.count(Task.id)).where(Task.mode == "plan")) == 0
        )


@pytest.mark.asyncio
async def test_revision_runner_restores_original_scope_base_and_review_feedback(
    client, session_factory
):
    created = await client.post(
        "/api/plans",
        json={"input": "Implement authentication, caching, and audit logs"},
    )
    assert created.status_code == 201, created.text
    plan_id = created.json()["id"]
    base_version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
        content="# Base\nAuthentication\nCaching\nAudit logs",
    )
    async with session_factory() as db:
        base = await db.get(PlanVersion, base_version_id)
        base.review_verdict = "exhausted"
        base.review_exhausted = True
        base.review_feedback = "Retain an explicit rollback procedure"
        await db.commit()

    revised = await client.post(
        f"/api/plans/{plan_id}/runs",
        json={
            "run_type": "user_revision",
            "request": "Change only the cache invalidation strategy",
            "base_version_id": base_version_id,
            "expected_current_version_id": base_version_id,
        },
    )
    assert revised.status_code == 201, revised.text
    run_id = revised.json()["id"]
    outputs = [
        {
            "action": "propose",
            "plan": "# Candidate 1\nAuthentication\nNew caching\nAudit logs",
        },
        {
            "action": "revise",
            "feedback": "Specify cache rollback behavior",
        },
        {
            "action": "propose",
            "plan": "# Candidate 2\nAuthentication\nNew caching with rollback\nAudit logs",
        },
        {"action": "approve", "feedback": "All findings are resolved"},
    ]
    prompts: list[str] = []

    async def fake_stage(**kwargs):
        prompts.append(kwargs["prompt"])
        output = outputs.pop(0)
        async with session_factory() as db:
            db.add(
                PlanAgentStep(
                    run_id=kwargs["run_id"],
                    plan_id=kwargs["plan_id"],
                    step_type=kwargs["step_type"],
                    round=kwargs["round_number"],
                    generation=kwargs["generation"],
                    provider="claude",
                    model="test-model",
                    route_slot="primary",
                    status="completed",
                    output=json.dumps(output),
                    finished_at=datetime.utcnow(),
                )
            )
            await db.commit()
        return output, json.dumps(output), object(), "primary", "test-account"

    async def claim_run():
        async with session_factory() as db:
            run = await db.get(PlanAgentRun, run_id)
            assert run.status == "queued"
            run.status = "running"
            run.generation += 1
            run.last_execution_started_at = datetime.utcnow()
            await db.commit()

    runner = PlanAgentRunner(
        db_factory=session_factory,
        instance_manager=AsyncMock(),
    )
    runner._run_stage = fake_stage

    for expected in ("queued", "queued", "queued", "completed"):
        await claim_run()
        assert await runner.advance_versioned(run_id, cwd="/tmp") == expected

    assert len(prompts) == 4
    for prompt in prompts:
        assert "Implement authentication, caching, and audit logs" in prompt
        assert "user_revision" in prompt
        assert "incremental revision" in prompt
        assert "Change only the cache invalidation strategy" in prompt
    assert "# Base" in prompts[0]
    assert "Retain an explicit rollback procedure" in prompts[0]
    assert "# Base" in prompts[1]
    assert "Specify cache rollback behavior" in prompts[2]
    assert "Specify cache rollback behavior" in prompts[3]


@pytest.mark.asyncio
async def test_related_plan_creation_rejects_migrating_target(client, session_factory):
    target = await _target(client, session_factory)
    async with session_factory() as db:
        current = await db.get(Task, target.id)
        current.status = "migrating"
        await db.commit()

    response = await client.post(
        "/api/plans",
        json={"input": "Do not race migration", "target_task_id": target.id},
    )

    assert response.status_code == 409
    assert "changing execution location" in response.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Plan.id))) == 0


@pytest.mark.asyncio
async def test_input_request_accepts_many_questions_and_resumes_same_run(
    client, session_factory
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Need user choices", "target_task_id": target.id},
    )
    assert created.status_code == 201, created.text
    plan_id = created.json()["id"]
    run_id = created.json()["active_run"]["id"]

    questions = [
        {
            "id": f"question_{index}",
            "header": f"Q{index}",
            "question": f"Provide required value {index}",
            "response_type": "text",
            "options": [],
            "required": True,
        }
        for index in range(8)
    ]
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        run.status = "waiting_user"
        run.current_stage = "planner"
        run.generation = 7
        run.interaction_count = 1
        step = PlanAgentStep(
            run_id=run.id,
            plan_id=plan_id,
            step_type="planner",
            round=1,
            generation=7,
            provider="claude",
            model="test",
            status="completed",
        )
        db.add(step)
        await db.flush()
        input_request = PlanInputRequest(
            plan_id=plan_id,
            run_id=run.id,
            source_step_id=step.id,
            requested_by="planner",
            reason="All eight values are necessary",
            questions=questions,
            status="open",
            idempotency_key=f"run:{run.id}:step:{step.id}",
            opened_at=datetime.utcnow(),
        )
        db.add(input_request)
        await db.flush()
        run.open_input_request_id = input_request.id
        await db.commit()
        request_id = input_request.id

    body = {
        "expected_run_generation": 7,
        "idempotency_key": "answer-many-questions",
        "answers": [
            {"question_id": item["id"], "value": f"answer-{index}"}
            for index, item in enumerate(questions)
        ],
    }
    answered = await client.post(
        f"/api/plan-runs/{run_id}/input-requests/{request_id}/answer",
        json=body,
    )
    assert answered.status_code == 200, answered.text
    assert len(answered.json()["answers"]) == 8

    replay = await client.post(
        f"/api/plan-runs/{run_id}/input-requests/{request_id}/answer",
        json=body,
    )
    assert replay.status_code == 200, replay.text
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        assert run.plan_id == plan_id
        assert run.status == "queued"
        assert run.generation == 8
        assert run.open_input_request_id is None
        assert await db.scalar(select(func.count(PlanAgentRun.id))) == 1


@pytest.mark.asyncio
async def test_required_choice_accepts_free_form_alternative(
    client, session_factory
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Choose a safe rollout", "target_task_id": target.id},
    )
    assert created.status_code == 201, created.text
    plan_id = created.json()["id"]
    run_id = created.json()["active_run"]["id"]

    async with session_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        run.status = "waiting_user"
        run.current_stage = "planner"
        run.generation = 3
        step = PlanAgentStep(
            run_id=run.id,
            plan_id=plan_id,
            step_type="planner",
            round=1,
            generation=3,
            provider="claude",
            status="completed",
        )
        db.add(step)
        await db.flush()
        input_request = PlanInputRequest(
            plan_id=plan_id,
            run_id=run.id,
            source_step_id=step.id,
            requested_by="planner",
            reason="Select the rollout strategy",
            questions=[{
                "id": "rollout",
                "header": "Rollout",
                "question": "Which rollout strategy should be used?",
                "response_type": "single_choice",
                "options": [
                    {"value": "blue_green", "label": "Blue-green"},
                    {"value": "rolling", "label": "Rolling"},
                ],
                "required": True,
            }],
            status="open",
            idempotency_key=f"free-form:{run.id}:{step.id}",
            opened_at=datetime.utcnow(),
        )
        db.add(input_request)
        await db.flush()
        run.open_input_request_id = input_request.id
        await db.commit()
        request_id = input_request.id

    missing = await client.post(
        f"/api/plan-runs/{run_id}/input-requests/{request_id}/answer",
        json={
            "expected_run_generation": 3,
            "idempotency_key": "missing-choice",
            "answers": [{"question_id": "rollout", "value": None}],
        },
    )
    assert missing.status_code == 422
    assert "additional response" in missing.text

    answered = await client.post(
        f"/api/plan-runs/{run_id}/input-requests/{request_id}/answer",
        json={
            "expected_run_generation": 3,
            "idempotency_key": "free-form-choice",
            "answers": [{"question_id": "rollout", "value": None}],
            "response_text": (
                "Neither option fits. Use a canary rollout with a manual gate."
            ),
        },
    )
    assert answered.status_code == 200, answered.text
    assert answered.json()["answers"] == [
        {"question_id": "rollout", "value": None}
    ]
    assert "canary rollout" in answered.json()["response_text"]

    async with session_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        input_request = await db.get(PlanInputRequest, request_id)
        assert run.status == "queued"
        assert input_request.status == "answered"


@pytest.mark.asyncio
async def test_exact_approved_version_is_applied_to_real_user_message(
    client, session_factory
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Plan exact application", "target_task_id": target.id},
    )
    plan_id = created.json()["id"]
    run_id = created.json()["active_run"]["id"]
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, run_id)
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            produced_by_run_id=run.id,
            content="# Exact immutable content",
            context_session_id=run.context_session_id,
            context_log_id=run.context_log_id,
            repo_revision=run.repo_revision,
            review_verdict="approve",
            reviewed_at=datetime.utcnow(),
        )
        db.add(version)
        await db.flush()
        plan.current_version_id = version.id
        plan.active_run_id = None
        run.status = "completed"
        run.current_stage = "complete"
        run.result_version_id = version.id
        run.finished_at = datetime.utcnow()
        await db.commit()
        version_id = version.id

    approved = await client.post(
        f"/api/plan-versions/{version_id}/approve",
        json={"expected_current_version_id": version_id, "confirm_stale": False},
    )
    assert approved.status_code == 200, approved.text

    enqueue = AsyncMock(side_effect=RuntimeError("shutdown after durable commit"))
    with patch(
        "backend.main.dispatcher.enqueue_plan_application_receipt",
        new=enqueue,
    ):
        sent = await client.post(
            f"/api/tasks/{target.id}/chat",
            json={"message": "Implement it", "plan_version_ids": [version_id]},
        )
    assert sent.status_code == 200, sent.text
    assert sent.json()["applied_plan_version_ids"] == [version_id]
    response_receipt_key = sent.json()["plan_application_receipt_key"]

    async with session_factory() as db:
        application = (
            await db.execute(
                select(PlanApplication).where(
                    PlanApplication.plan_version_id == version_id
                )
            )
        ).scalar_one()
        log = await db.get(LogEntry, application.user_log_id)
        snapshot = json.loads(log.raw_json)["applied_plans"][0]
        assert snapshot["plan_id"] == plan_id
        assert snapshot["version_id"] == version_id
        assert snapshot["version_number"] == 1
        assert snapshot["content"] == "# Exact immutable content"
        receipt = (
            await db.execute(
                select(PlanApplicationReceipt).where(
                    PlanApplicationReceipt.receipt_key
                    == application.application_receipt_key
                )
            )
        ).scalar_one()
        assert receipt.status == "committed"
        assert receipt.delivery_status == "pending"
        assert receipt.outbox_payload["source_log_id"] == log.id
        assert receipt.outbox_payload["user_message_text"] == "Implement it"
        assert "# Exact immutable content" in receipt.outbox_payload["current_message"]
        receipt_key = receipt.receipt_key
        assert receipt_key == response_receipt_key

    from backend.services.dispatcher import GlobalDispatcher

    recovered_dispatcher = GlobalDispatcher(
        session_factory,
        MagicMock(),
        AsyncMock(),
    )
    recovered_dispatcher._ensure_queue_worker = MagicMock()
    assert await recovered_dispatcher.enqueue_plan_application_receipt(receipt_key)
    recovered = await recovered_dispatcher._get_task_queue(target.id).get()
    recovered_dispatcher._get_task_queue(target.id).task_done()
    assert recovered.delivery_key == receipt_key
    assert "# Exact immutable content" in recovered.current_message
    async with session_factory() as db:
        receipt = (
            await db.execute(
                select(PlanApplicationReceipt).where(
                    PlanApplicationReceipt.receipt_key == receipt_key
                )
            )
        ).scalar_one()
        assert receipt.delivery_status == "queued"

    duplicate = await client.post(
        f"/api/tasks/{target.id}/chat",
        json={"message": "Again", "plan_version_ids": [version_id]},
    )
    assert duplicate.status_code == 400


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_endpoint", ["stop-session", "cancel"])
async def test_terminal_operation_rejects_plan_admission_after_queue_abort(
    client,
    session_factory,
    terminal_endpoint,
):
    """The cancellation lease must outlive abort and the launch barrier."""

    import backend.main

    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Plan terminal race", "target_task_id": target.id},
    )
    plan_id = created.json()["id"]
    version_id = await _finish_current_run_with_version(
        session_factory,
        plan_id=plan_id,
        content="# Must not enter the stopped queue",
    )
    approved = await client.post(
        f"/api/plan-versions/{version_id}/approve",
        json={
            "expected_current_version_id": version_id,
            "confirm_stale": False,
        },
    )
    assert approved.status_code == 200, approved.text
    async with session_factory() as db:
        task = await db.get(Task, target.id)
        task.status = "executing"
        # Force the terminal path through the async launch barrier without a
        # reverse process owner, reproducing the post-abort/pre-CAS window.
        task.instance_id = 991
        await db.commit()

    abort_finished = asyncio.Event()
    barrier_entered = asyncio.Event()
    finish_terminal = asyncio.Event()

    async def abort_queue(*_args, **_kwargs):
        abort_finished.set()
        return 0

    async def wait_for_launch_barrier(*_args, **_kwargs):
        assert abort_finished.is_set()
        barrier_entered.set()
        await finish_terminal.wait()
        return True

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new=AsyncMock(side_effect=abort_queue),
        ) as abort,
        patch.object(
            backend.main.instance_manager,
            "wait_for_task_launch_barrier",
            new=AsyncMock(side_effect=wait_for_launch_barrier),
        ),
    ):
        terminal_request = asyncio.create_task(
            client.post(f"/api/tasks/{target.id}/{terminal_endpoint}")
        )
        await asyncio.wait_for(barrier_entered.wait(), timeout=2)
        rejected = await client.post(
            f"/api/tasks/{target.id}/chat",
            json={
                "message": "This races with terminal publication",
                "plan_version_ids": [version_id],
            },
        )
        finish_terminal.set()
        terminal_response = await asyncio.wait_for(terminal_request, timeout=2)

    assert rejected.status_code == 409, rejected.text
    assert "Plan Version was not applied" in rejected.json()["detail"]
    assert terminal_response.status_code == 200, terminal_response.text
    assert abort.await_args.kwargs["cancel_durable"] is False
    async with session_factory() as db:
        assert (
            await db.scalar(
                select(PlanApplication.id).where(
                    PlanApplication.plan_version_id == version_id
                )
            )
            is None
        )
        assert (
            await db.scalar(
                select(PlanApplicationReceipt.id).where(
                    PlanApplicationReceipt.plan_version_ids == [version_id]
                )
            )
            is None
        )


@pytest.mark.asyncio
async def test_uncertain_plan_delivery_is_visible_and_admin_can_release_it(
    client,
    session_factory,
    monkeypatch,
):
    from backend import main as main_module
    from backend.services.dispatcher import GlobalDispatcher

    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        task = Task(
            title="delivery target",
            description="delivery target",
            status="completed",
            session_id="delivery-session",
        )
        plan = Plan(
            title="uncertain delivery",
            initial_request="Plan this",
            pipeline_config=pipeline,
        )
        db.add_all([task, plan])
        await db.flush()
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            content="# Exact plan",
            human_decision="approved",
        )
        log = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="Implement it",
            raw_json=json.dumps({"applied_plans": [{"plan_id": plan.id}]}),
        )
        db.add_all([version, log])
        await db.flush()
        plan.current_version_id = version.id
        receipt = PlanApplicationReceipt(
            receipt_key="uncertain-visible-receipt",
            target_task_id=task.id,
            manager_user_log_id=log.id,
            plan_version_ids=[version.id],
            status="committed",
            delivery_status="uncertain",
            delivery_error="Automatic replay blocked",
            launch_evidence={
                "task_id": task.id,
                "instance_id": 7,
                "retry_count": 3,
            },
        )
        db.add(receipt)
        await db.flush()
        db.add(
            PlanApplication(
                plan_id=plan.id,
                plan_version_id=version.id,
                application_type="chat_message",
                target_task_id=task.id,
                user_log_id=log.id,
                application_receipt_key=receipt.receipt_key,
            )
        )
        await db.commit()
        plan_id = plan.id
        version_id = version.id

    resource = await client.get(f"/api/plans/{plan_id}")
    assert resource.status_code == 200, resource.text
    application = resource.json()["applications"][0]
    assert application["application_receipt_key"] == "uncertain-visible-receipt"
    assert application["delivery_status"] == "uncertain"
    assert application["launch_evidence"]["retry_count"] == 3

    dispatcher = GlobalDispatcher(session_factory, MagicMock(), AsyncMock())
    monkeypatch.setattr(main_module, "dispatcher", dispatcher)
    monkeypatch.setattr(main_module, "broadcaster", AsyncMock())
    resolved = await client.post(
        f"/api/plans/{plan_id}/application-deliveries/"
        "uncertain-visible-receipt/resolve",
        json={
            "action": "release_for_retry",
            "note": "No exact native turn or process generation exists",
        },
    )
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["plan_ids"] == [plan_id]
    replayed = await client.post(
        f"/api/plans/{plan_id}/application-deliveries/"
        "uncertain-visible-receipt/resolve",
        json={
            "action": "release_for_retry",
            "note": "Idempotent replay of the same audited decision",
        },
    )
    assert replayed.status_code == 200, replayed.text

    refreshed = await client.get(f"/api/plans/{plan_id}")
    assert refreshed.status_code == 200, refreshed.text
    assert refreshed.json()["applications"] == []
    attempts = refreshed.json()["application_attempts"]
    assert len(attempts) == 1
    assert attempts[0]["application_receipt_key"] == "uncertain-visible-receipt"
    assert attempts[0]["delivery_status"] == "cancelled"
    assert attempts[0]["delivery_resolution"]["action"] == "release_for_retry"
    assert "native turn" in attempts[0]["delivery_resolution"]["note"]
    assert attempts[0]["launch_evidence"]["retry_count"] == 3

    async with session_factory() as db:
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.receipt_key == "uncertain-visible-receipt"
            )
        )
        assert receipt.delivery_status == "cancelled"
        assert receipt.delivery_resolution["action"] == "release_for_retry"
        assert "native turn" in receipt.delivery_resolution["note"]
        assert (
            await db.scalar(
                select(PlanApplication.id).where(
                    PlanApplication.plan_version_id == version_id
                )
            )
            is None
        )
        attempt = await db.scalar(
            select(PlanApplicationAttempt).where(
                PlanApplicationAttempt.plan_version_id == version_id
            )
        )
        assert attempt is not None
        assert attempt.application_receipt_key == "uncertain-visible-receipt"


@pytest.mark.asyncio
async def test_instance_capacity_owner_is_task_xor_plan_run(db_session):
    instance = Instance(name="slot", status="running", current_plan_run_id=4)
    db_session.add(instance)
    await db_session.commit()
    assert instance.current_task_id is None
    assert instance.current_plan_run_id == 4
    db_session.add(
        Instance(
            name="invalid-slot",
            status="running",
            current_task_id=3,
            current_plan_run_id=4,
        )
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()
    await db_session.rollback()


@pytest.mark.asyncio
async def test_plan_application_target_shape_is_database_enforced(db_session):
    db_session.add(
        PlanApplication(
            plan_id=1,
            plan_version_id=1,
            application_type="chat_message",
            execution_task_id=99,
        )
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()
    await db_session.rollback()


@pytest.mark.asyncio
async def test_plan_resources_never_expose_internal_attachment_paths(
    client, session_factory
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Inspect attached requirements", "target_task_id": target.id},
    )
    plan_id = created.json()["id"]
    run_id = created.json()["active_run"]["id"]
    internal = {
        "url": "/api/uploads/example.txt",
        "name": "example.txt",
        "is_image": False,
        "path": "/private/uploads/example.txt",
    }
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, run_id)
        plan.initial_attachments = [internal]
        run.status = "waiting_user"
        step = PlanAgentStep(
            run_id=run_id,
            plan_id=plan_id,
            step_type="planner",
            round=1,
            generation=run.generation,
            provider="claude",
            status="completed",
        )
        db.add(step)
        await db.flush()
        input_request = PlanInputRequest(
            plan_id=plan_id,
            run_id=run_id,
            source_step_id=step.id,
            requested_by="planner",
            reason="Need confirmation",
            questions=[
                {
                    "id": "confirm",
                    "header": "Confirm",
                    "question": "Confirm the requirement",
                    "response_type": "text",
                    "options": [],
                    "required": True,
                }
            ],
            status="open",
            attachments=[internal],
            idempotency_key=f"test-path:{run_id}",
        )
        db.add(input_request)
        await db.flush()
        run.open_input_request_id = input_request.id
        await db.commit()

    resource = await client.get(f"/api/plans/{plan_id}")
    assert resource.status_code == 200, resource.text
    payload = resource.json()
    assert payload["initial_attachments"] == [
        {
            "url": "/api/uploads/example.txt",
            "name": "example.txt",
            "is_image": False,
        }
    ]
    assert "path" not in payload["open_input_request"]["attachments"][0]
    run_resource = await client.get(f"/api/plan-runs/{run_id}")
    assert "path" not in run_resource.json()["input_requests"][0]["attachments"][0]


@pytest.mark.asyncio
async def test_interaction_round_limit_fails_without_limiting_question_count(
    client, session_factory
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Need one more round", "target_task_id": target.id},
    )
    plan_id = created.json()["id"]
    run_id = created.json()["active_run"]["id"]
    async with session_factory() as db:
        owner = Instance(name="limited-plan-slot", status="running")
        db.add(owner)
        await db.flush()
        run = await db.get(PlanAgentRun, run_id)
        run.status = "running"
        run.generation = 4
        run.instance_id = owner.id
        run.interaction_count = 3
        run.max_interactions = 3
        run.last_execution_started_at = datetime.utcnow()
        owner.current_plan_run_id = run_id
        step = PlanAgentStep(
            run_id=run_id,
            plan_id=plan_id,
            step_type="planner",
            round=1,
            generation=4,
            provider="claude",
            status="completed",
        )
        db.add(step)
        await db.flush()
        receipt = new_prepared_runtime_receipt(step, attempt_index=1)
        receipt.status = "cleaned"
        receipt.cleaned_at = datetime.utcnow()
        db.add(receipt)
        await db.commit()
        await db.refresh(step)
        step_id = step.id
        owner_id = owner.id

    runner = PlanAgentRunner(
        db_factory=session_factory,
        instance_manager=AsyncMock(),
    )
    outcome = await runner._open_input_request(
        run_id=run_id,
        generation=4,
        source_step=PlanAgentStep(id=step_id),
        requested_by="planner",
        reason="One more interaction is necessary",
        questions=[
            {
                "id": f"q{index}",
                "header": f"Q{index}",
                "question": f"Decision {index}",
                "response_type": "text",
                "options": [],
                "required": True,
            }
            for index in range(20)
        ],
        max_interactions=3,
    )
    assert outcome == "failed"
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, run_id)
        owner = await db.get(Instance, owner_id)
        assert run.status == "failed"
        assert "3 user-interaction round limit" in run.error
        assert plan.active_run_id is None
        assert owner.status == "idle"
        assert owner.current_plan_run_id is None
        assert (
            await db.scalar(
                select(func.count(PlanInputRequest.id)).where(
                    PlanInputRequest.run_id == run_id
                )
            )
            == 0
        )


@pytest.mark.asyncio
async def test_versioned_run_pauses_twice_and_resumes_same_pipeline(
    client, session_factory
):
    target = await _target(client, session_factory)
    created = await client.post(
        "/api/plans",
        json={"input": "Design an interactive rollout", "target_task_id": target.id},
    )
    assert created.status_code == 201, created.text
    plan_id = created.json()["id"]
    run_id = created.json()["active_run"]["id"]
    instance = Instance(name="plan-slot", status="idle")
    async with session_factory() as db:
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
        instance_id = instance.id

    planner_questions = [
        {
            "id": f"decision_{index}",
            "header": f"Q{index}",
            "question": f"Required decision {index}",
            "response_type": "text",
            "options": [],
            "required": True,
        }
        for index in range(8)
    ]
    outputs = [
        {
            "action": "request_input",
            "reason": "These decisions affect the architecture",
            "questions": planner_questions,
        },
        {"action": "propose", "plan": "# Version 1\nInitial decisions included."},
        {
            "action": "request_input",
            "reason": "Reviewer found one unresolved deployment constraint",
            "questions": [
                {
                    "id": "maintenance_window",
                    "header": "Rollout",
                    "question": "Which maintenance window should the Plan use?",
                    "response_type": "text",
                    "options": [],
                    "required": True,
                }
            ],
        },
        {
            "action": "propose",
            "plan": "# Version 2\nIncludes every decision and the Sunday window.",
        },
        {"action": "approve", "feedback": "Self-contained and testable"},
    ]
    prompts: list[str] = []

    async def fake_stage(**kwargs):
        prompts.append(kwargs["prompt"])
        output = outputs.pop(0)
        async with session_factory() as db:
            step = PlanAgentStep(
                run_id=kwargs["run_id"],
                plan_id=kwargs["plan_id"],
                step_type=kwargs["step_type"],
                round=kwargs["round_number"],
                generation=kwargs["generation"],
                provider="claude",
                model="test-model",
                route_slot="primary",
                status="completed",
                output=json.dumps(output),
                finished_at=datetime.utcnow(),
            )
            db.add(step)
            await db.flush()
            receipt = new_prepared_runtime_receipt(step, attempt_index=1)
            receipt.status = "cleaned"
            receipt.cleaned_at = datetime.utcnow()
            db.add(receipt)
            await db.commit()
        return output, json.dumps(output), object(), "primary", "test-account"

    async def claim_current_run():
        async with session_factory() as db:
            run = await db.get(PlanAgentRun, run_id)
            owner = await db.get(Instance, instance_id)
            assert run.status == "queued"
            assert owner.status == "idle"
            run.status = "running"
            run.generation += 1
            run.instance_id = instance_id
            run.last_execution_started_at = datetime.utcnow()
            owner.status = "running"
            owner.current_plan_run_id = run_id
            await db.commit()

    runner = PlanAgentRunner(
        db_factory=session_factory,
        instance_manager=AsyncMock(),
    )
    runner._run_stage = fake_stage

    await claim_current_run()
    assert await runner.advance_versioned(run_id, cwd="/tmp") == "waiting_user"
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        owner = await db.get(Instance, instance_id)
        first_request = await db.get(PlanInputRequest, run.open_input_request_id)
        assert run.status == "waiting_user"
        assert run.instance_id is None
        assert len(first_request.questions) == 8
        assert owner.status == "idle"
        assert owner.current_plan_run_id is None
        first_generation = run.generation

    answered = await client.post(
        f"/api/plan-runs/{run_id}/input-requests/{first_request.id}/answer",
        json={
            "expected_run_generation": first_generation,
            "idempotency_key": "first-answer",
            "answers": [
                {"question_id": question["id"], "value": f"value-{index}"}
                for index, question in enumerate(planner_questions)
            ],
        },
    )
    assert answered.status_code == 200, answered.text

    await claim_current_run()
    assert await runner.advance_versioned(run_id, cwd="/tmp") == "queued"
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, run_id)
        assert plan.current_version_id is None
        assert run.result_version_id is None
        assert run.draft_content == "# Version 1\nInitial decisions included."
        assert (
            await db.scalar(
                select(func.count(PlanVersion.id)).where(PlanVersion.plan_id == plan_id)
            )
            == 0
        )
    await claim_current_run()
    assert await runner.advance_versioned(run_id, cwd="/tmp") == "waiting_user"
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        second_request = await db.get(PlanInputRequest, run.open_input_request_id)
        second_generation = run.generation
        assert second_request.requested_by == "reviewer"

    answered = await client.post(
        f"/api/plan-runs/{run_id}/input-requests/{second_request.id}/answer",
        json={
            "expected_run_generation": second_generation,
            "idempotency_key": "reviewer-answer",
            "answers": [
                {
                    "question_id": "maintenance_window",
                    "value": "Sunday 02:00 UTC",
                }
            ],
        },
    )
    assert answered.status_code == 200, answered.text

    await claim_current_run()
    assert await runner.advance_versioned(run_id, cwd="/tmp") == "queued"
    assert "Sunday 02:00 UTC" in prompts[-1]
    await claim_current_run()
    assert await runner.advance_versioned(run_id, cwd="/tmp") == "completed"

    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, run_id)
        versions = list(
            (
                await db.execute(
                    select(PlanVersion)
                    .where(PlanVersion.plan_id == plan_id)
                    .order_by(PlanVersion.version_number)
                )
            ).scalars()
        )
        requests = list(
            (
                await db.execute(
                    select(PlanInputRequest)
                    .where(PlanInputRequest.run_id == run_id)
                    .order_by(PlanInputRequest.id)
                )
            ).scalars()
        )
        assert plan.active_run_id is None
        assert plan.current_version_id == versions[0].id
        assert run.status == "completed"
        assert run.result_version_id == versions[0].id
        assert (
            run.draft_content
            == "# Version 2\nIncludes every decision and the Sunday window."
        )
        assert run.interaction_count == 2
        assert [item.status for item in requests] == ["answered", "answered"]
        assert [item.version_number for item in versions] == [1]
        assert versions[0].content == run.draft_content
        assert versions[0].superseded_by_version_id is None
        assert versions[0].review_verdict == "approve"
        assert versions[0].human_decision == "pending"
