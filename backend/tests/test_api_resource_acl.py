"""Cross-resource HTTP ACL regressions.

These tests use the real authentication middleware.  They intentionally keep
the identities, Workers, and Projects distinct so an "owns any resource"
check cannot accidentally satisfy an exact-target authorization decision.
"""

import asyncio
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from backend.models.discussion import Discussion, DiscussionAgent, DiscussionEvent
from backend.config import settings
from backend.models.log_entry import LogEntry
from backend.models.monitor_session import MonitorSession
from backend.models.plan import Plan
from backend.models.pr_monitor import MonitoredRepo, PRReview
from backend.models.project import Project
from backend.models.secret import Secret
from backend.models.sub_agent import SubAgentSession
from backend.models.tag import Tag
from backend.models.task import Task
from backend.models.team_share import TeamProjectShare, TeamTaskShare
from backend.models.user_group import UserGroup, UserGroupMember
from backend.models.worker import Worker
from backend.schemas.plan import default_plan_pipeline_config
from backend.tests.test_auth_ws_security import _create_user, secured_client


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def _add_worker(db, *, name: str, owner_user_id: int) -> Worker:
    worker = Worker(
        name=name,
        status="ready",
        owner_user_id=owner_user_id,
        auth_token=f"{name}-token",
    )
    db.add(worker)
    await db.flush()
    return worker


@pytest.mark.asyncio
async def test_local_retry_revalidates_user_role_in_final_retry_transaction(
    secured_client,
    monkeypatch,
):
    """A demoted admin cannot persist stale unrestricted retry authority."""

    from backend.models.user import User
    from backend.services.test_harness import test_harness_service

    client, session_factory = secured_client
    admin_id, admin_token = await _create_user(
        session_factory,
        email="retry-role-admin@example.com",
        role="admin",
    )
    async with session_factory() as db:
        task = Task(
            title="retry principal fence",
            description="demote after HTTP authentication",
            status="failed",
            created_by=admin_id,
            execution_user_id=admin_id,
            execution_user_role="admin",
            execution_mode="unrestricted",
            execution_principal_kind="user",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    @asynccontextmanager
    async def demote_before_retry_cas(*_args, **_kwargs):
        async with session_factory() as db:
            principal = await db.get(User, admin_id)
            principal.role = "member"
            await db.commit()
        yield

    monkeypatch.setattr(
        test_harness_service,
        "owner_stop_fence",
        demote_before_retry_cas,
    )

    response = await client.post(
        f"/api/tasks/{task_id}/retry",
        headers=_headers(admin_token),
    )

    assert response.status_code == 409, response.text
    assert "changed role" in response.json()["detail"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "failed"
    assert task.retry_count == 0
    assert task.execution_user_role == "admin"
    assert task.execution_mode == "unrestricted"


@pytest.mark.asyncio
async def test_org_registry_mutations_are_not_unsigned_public_writes(
    secured_client,
    monkeypatch,
):
    client, session_factory = secured_client
    _, member_token = await _create_user(
        session_factory,
        email="org-member@example.com",
        role="member",
    )
    monkeypatch.setattr(settings, "org_registry_enabled", True)
    registration = {
        "open_id": "ou_attacker",
        "name": "Forged member",
        "ccm_url": "http://127.0.0.1:9",
    }

    unsigned = await client.post("/api/org/register", json=registration)
    member_register = await client.post(
        "/api/org/register",
        headers=_headers(member_token),
        json=registration,
    )
    member_import = await client.post(
        "/api/org/import",
        headers=_headers(member_token),
        json={"members": [], "teams": [], "team_members": []},
    )
    member_registry_change = await client.post(
        "/api/org/registry-changed",
        headers=_headers(member_token),
        json={"new_registry_url": "http://127.0.0.1:9"},
    )

    assert unsigned.status_code == 401
    assert member_register.status_code == 403
    assert member_import.status_code == 403
    assert member_registry_change.status_code == 403


@pytest.mark.asyncio
async def test_only_internal_service_can_choose_task_id(
    secured_client,
    monkeypatch,
):
    from backend.config import settings

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    client, session_factory = secured_client
    _, member_token = await _create_user(
        session_factory,
        email="task-id-member@example.com",
        role="member",
    )
    payload = {
        "id": 91001,
        "source_incarnation_id": "1" * 32,
        "source_retry_count": 0,
        "source_turn_generation": 1,
        "execution_user_id": None,
        "execution_user_role": "super_admin",
        "execution_mode": "unrestricted",
        "execution_principal_kind": "delegated_deployment_token",
        "title": "manager-owned identity",
        "description": "only an internal forward may choose this id",
    }

    rejected = await client.post(
        "/api/tasks",
        headers=_headers(member_token),
        json=payload,
    )
    accepted = await client.post(
        "/api/tasks",
        headers={"Authorization": "Bearer security-service-token"},
        json=payload,
    )

    # Worker nodes do not own the Manager's JWT/user control plane at all.
    assert rejected.status_code == 401
    assert "deployment" in rejected.json()["detail"]
    assert accepted.status_code == 201, accepted.text
    assert accepted.json()["id"] == payload["id"]


@pytest.mark.asyncio
async def test_task_and_project_targets_require_exact_resource_access(
    secured_client,
):
    client, session_factory = secured_client
    alice_id, alice_token = await _create_user(
        session_factory,
        email="target-alice@example.com",
        role="member",
    )
    bob_id, _ = await _create_user(
        session_factory,
        email="target-bob@example.com",
        role="member",
    )

    async with session_factory() as db:
        alice_worker = await _add_worker(
            db,
            name="target-alice-worker",
            owner_user_id=alice_id,
        )
        bob_worker = await _add_worker(
            db,
            name="target-bob-worker",
            owner_user_id=bob_id,
        )
        shared_project = Project(
            name="target-shared-project",
            worker_id=bob_worker.id,
            local_path="/tmp/target-shared-project",
            status="ready",
        )
        victim_project = Project(
            name="target-victim-project",
            worker_id=bob_worker.id,
            local_path="/tmp/target-victim-project",
            status="ready",
        )
        db.add_all([shared_project, victim_project])
        await db.flush()
        db.add(
            TeamProjectShare(
                project_id=shared_project.id,
                target_type="user",
                target_id=alice_id,
                shared_by=bob_id,
            )
        )
        clone_source = Task(
            title="private clone source",
            description="private",
            worker_id=bob_worker.id,
            created_by=bob_id,
        )
        alice_task = Task(
            title="alice task",
            description="owned",
            worker_id=alice_worker.id,
            created_by=alice_id,
        )
        shared_task_by_bob = Task(
            title="shared project task",
            description="shared",
            worker_id=bob_worker.id,
            project_id=shared_project.id,
            created_by=bob_id,
        )
        db.add_all([clone_source, alice_task, shared_task_by_bob])
        await db.commit()
        ids = {
            "alice_worker": alice_worker.id,
            "bob_worker": bob_worker.id,
            "shared_project": shared_project.id,
            "victim_project": victim_project.id,
            "clone_source": clone_source.id,
            "alice_task": alice_task.id,
            "shared_task_by_bob": shared_task_by_bob.id,
        }

    headers = _headers(alice_token)

    local_task = await client.post(
        "/api/tasks",
        headers=headers,
        json={"title": "local", "description": "local"},
    )
    other_worker_task = await client.post(
        "/api/tasks",
        headers=headers,
        json={
            "title": "other worker",
            "description": "other worker",
            "worker_id": ids["bob_worker"],
        },
    )
    own_worker_task = await client.post(
        "/api/tasks",
        headers=headers,
        json={
            "title": "own worker",
            "description": "own worker",
            "worker_id": ids["alice_worker"],
        },
    )
    assert local_task.status_code == 403
    assert other_worker_task.status_code == 403
    # Worker ownership is node-management authority only. A member still
    # needs a Project ACL before creating ordinary work on that node.
    assert own_worker_task.status_code == 403

    # A Project share grants work on that exact Project, including inheriting
    # its Worker.  It does not grant the Worker as a free-standing target.
    shared_project_task = await client.post(
        "/api/tasks",
        headers=headers,
        json={
            "title": "shared project",
            "description": "shared project",
            "project_id": ids["shared_project"],
        },
    )
    mismatched_project_task = await client.post(
        "/api/tasks",
        headers=headers,
        json={
            "title": "mismatched project",
            "description": "mismatched project",
            "project_id": ids["shared_project"],
            "worker_id": ids["alice_worker"],
        },
    )
    assert shared_project_task.status_code == 201, shared_project_task.text
    assert shared_project_task.json()["worker_id"] == ids["bob_worker"]
    assert mismatched_project_task.status_code == 400
    async with session_factory() as db:
        created = await db.get(Task, shared_project_task.json()["id"])
        assert created is not None
        assert created.created_by == alice_id
        assert created.execution_user_id == alice_id
        assert created.execution_user_role == "member"
        assert created.execution_mode == "sandbox"
        assert created.execution_principal_kind == "user"

    inaccessible_clone = await client.post(
        "/api/tasks",
        headers=headers,
        json={
            "title": "clone",
            "description": "clone",
            "worker_id": ids["alice_worker"],
            "clone_from_task_id": ids["clone_source"],
        },
    )
    assert inaccessible_clone.status_code == 403

    inaccessible_project_update = await client.put(
        f"/api/tasks/{ids['alice_task']}",
        headers=headers,
        json={"project_id": ids["victim_project"]},
    )
    inaccessible_worker_update = await client.put(
        f"/api/tasks/{ids['alice_task']}",
        headers=headers,
        json={"worker_id": ids["bob_worker"]},
    )
    assert inaccessible_project_update.status_code == 403
    assert inaccessible_worker_update.status_code == 403

    shared_project_update = await client.put(
        f"/api/tasks/{ids['shared_task_by_bob']}",
        headers=headers,
        json={"title": "updated by project collaborator"},
    )
    assert shared_project_update.status_code == 200
    assert shared_project_update.json()["title"] == (
        "updated by project collaborator"
    )

    # Project ACL is Task authority inside that Project, not authority to
    # manufacture a projectless Task on a separately owned compute node.
    detach_project = await client.put(
        f"/api/tasks/{ids['shared_task_by_bob']}",
        headers=headers,
        json={
            "project_id": None,
            "worker_id": ids["alice_worker"],
        },
    )
    forge_project_workspace = await client.put(
        f"/api/tasks/{ids['shared_task_by_bob']}",
        headers=headers,
        json={"target_repo": "/tmp/member-controlled-host-path"},
    )
    assert detach_project.status_code == 403
    assert forge_project_workspace.status_code == 400

    own_project = await client.post(
        "/api/projects",
        headers=headers,
        json={"name": "alice-project", "worker_id": ids["alice_worker"]},
    )
    other_project = await client.post(
        "/api/projects",
        headers=headers,
        json={"name": "bob-project-from-alice", "worker_id": ids["bob_worker"]},
    )
    local_project = await client.post(
        "/api/projects",
        headers=headers,
        json={"name": "local-project-from-alice"},
    )
    assert own_project.status_code == 403
    assert other_project.status_code == 403
    assert local_project.status_code == 403


@pytest.mark.asyncio
async def test_plan_acl_uses_project_or_task_not_worker_ownership(
    secured_client,
):
    client, session_factory = secured_client
    alice_id, alice_token = await _create_user(
        session_factory,
        email="plan-worker-owner@example.com",
        role="member",
    )
    bob_id, _ = await _create_user(
        session_factory,
        email="plan-data-owner@example.com",
        role="member",
    )
    async with session_factory() as db:
        alice_worker = await _add_worker(
            db,
            name="plan-owner-compute-only",
            owner_user_id=alice_id,
        )
        private_project = Project(
            name="plan-private-worker-project",
            worker_id=alice_worker.id,
            local_path="/tmp/plan-private-worker-project",
            status="ready",
        )
        shared_project = Project(
            name="plan-shared-local-project",
            worker_id=None,
            local_path="/tmp",
            status="ready",
        )
        db.add_all([private_project, shared_project])
        await db.flush()
        db.add(TeamProjectShare(
            project_id=shared_project.id,
            target_type="user",
            target_id=alice_id,
            shared_by=bob_id,
        ))
        pipeline = default_plan_pipeline_config().model_dump(mode="json")
        private_project_plan = Plan(
            title="Worker location is not an ACL",
            initial_request="private project plan",
            project_id=private_project.id,
            worker_id=alice_worker.id,
            created_by=bob_id,
            pipeline_config=pipeline,
        )
        projectless_plan = Plan(
            title="Projectless worker Plan",
            initial_request="projectless private plan",
            project_id=None,
            worker_id=alice_worker.id,
            created_by=bob_id,
            pipeline_config=pipeline,
        )
        shared_project_plan = Plan(
            title="Shared Project Plan",
            initial_request="shared project plan",
            project_id=shared_project.id,
            worker_id=None,
            created_by=bob_id,
            pipeline_config=pipeline,
        )
        db.add_all([
            private_project_plan,
            projectless_plan,
            shared_project_plan,
        ])
        await db.commit()
        ids = {
            "worker": alice_worker.id,
            "private": private_project_plan.id,
            "projectless": projectless_plan.id,
            "shared": shared_project_plan.id,
            "shared_project": shared_project.id,
        }

    headers = _headers(alice_token)
    listed = await client.get("/api/plans", headers=headers)
    private_detail = await client.get(
        f"/api/plans/{ids['private']}",
        headers=headers,
    )
    projectless_detail = await client.get(
        f"/api/plans/{ids['projectless']}",
        headers=headers,
    )
    shared_detail = await client.get(
        f"/api/plans/{ids['shared']}",
        headers=headers,
    )
    create_projectless = await client.post(
        "/api/plans",
        headers=headers,
        json={
            "input": "worker ownership must not create a data scope",
            "worker_id": ids["worker"],
        },
    )
    create_shared_project = await client.post(
        "/api/plans",
        headers=headers,
        json={
            "input": "Project membership permits this Plan",
            "project_id": ids["shared_project"],
        },
    )

    assert listed.status_code == 200
    assert {row["id"] for row in listed.json()} == {ids["shared"]}
    assert private_detail.status_code == 403
    assert projectless_detail.status_code == 403
    assert shared_detail.status_code == 200
    assert create_projectless.status_code == 403
    assert create_shared_project.status_code == 201, create_shared_project.text


@pytest.mark.asyncio
async def test_chat_share_is_read_and_chat_only(secured_client):
    client, session_factory = secured_client
    owner_id, _owner_token = await _create_user(
        session_factory,
        email="chat-owner@example.com",
        role="member",
    )
    recipient_id, recipient_token = await _create_user(
        session_factory,
        email="chat-recipient@example.com",
        role="member",
    )
    async with session_factory() as db:
        worker = await _add_worker(
            db,
            name="chat-owner-worker",
            owner_user_id=owner_id,
        )
        task = Task(
            title="chat shared",
            description="shared",
            worker_id=worker.id,
            created_by=owner_id,
        )
        db.add(task)
        await db.flush()
        db.add(
            TeamTaskShare(
                task_id=task.id,
                target_type="user",
                target_id=recipient_id,
                permission="chat",
                shared_by=owner_id,
            )
        )
        monitor = MonitorSession(
            task_id=task.id,
            agent_type="monitor",
            source="ccm",
            description="private monitor",
            status="running",
        )
        plan = Plan(
            title="chat share must not expose rich Plan audit",
            initial_request="private planning context",
            target_task_id=task.id,
            worker_id=worker.id,
            created_by=owner_id,
            pipeline_config=default_plan_pipeline_config().model_dump(mode="json"),
        )
        db.add_all([monitor, plan])
        await db.commit()
        task_id = task.id
        plan_id = plan.id

    headers = _headers(recipient_token)
    detail = await client.get(f"/api/tasks/{task_id}", headers=headers)
    history = await client.get(
        f"/api/tasks/{task_id}/chat/history?touch=true",
        headers=headers,
    )
    ssh_grants = await client.get(
        f"/api/tasks/{task_id}/ssh-grants",
        headers=headers,
    )
    capabilities = await client.get(
        f"/api/tasks/{task_id}/capability-invocations",
        headers=headers,
    )
    harness_capabilities = await client.get(
        f"/api/tasks/{task_id}/test-runs/capabilities",
        headers=headers,
    )
    harness_runs = await client.get(
        f"/api/tasks/{task_id}/test-runs",
        headers=headers,
    )
    workspace_capabilities = await client.get(
        f"/api/tasks/{task_id}/workspace-reviews/capabilities",
        headers=headers,
    )
    workspace_runs = await client.get(
        f"/api/tasks/{task_id}/workspace-reviews",
        headers=headers,
    )
    browser_reviews = await client.get(
        f"/api/tasks/{task_id}/browser-reviews",
        headers=headers,
    )
    rich_plans = await client.get(
        f"/api/plans?target_task_id={task_id}",
        headers=headers,
    )
    rich_plan_detail = await client.get(
        f"/api/plans/{plan_id}",
        headers=headers,
    )
    inject_capabilities = await client.get(
        f"/api/tasks/{task_id}/inject-capabilities",
        headers=headers,
    )
    inject = await client.post(
        f"/api/tasks/{task_id}/inject",
        headers=headers,
        json={"message": "chat shares cannot steer an active turn"},
    )
    delete = await client.delete(f"/api/tasks/{task_id}", headers=headers)
    archive = await client.post(f"/api/tasks/{task_id}/archive", headers=headers)
    create_monitor = await client.post(
        f"/api/tasks/{task_id}/monitor-sessions",
        headers=headers,
        json={"description": "not allowed"},
    )
    assert detail.status_code == 200
    assert history.status_code == 200
    assert ssh_grants.status_code == 403
    assert capabilities.status_code == 403
    assert harness_capabilities.status_code == 403
    assert harness_runs.status_code == 403
    assert workspace_capabilities.status_code == 403
    assert workspace_runs.status_code == 403
    assert browser_reviews.status_code == 403
    assert rich_plans.status_code == 200
    assert rich_plans.json() == []
    assert rich_plan_detail.status_code == 403
    assert inject_capabilities.status_code == 403
    assert inject.status_code == 403
    assert delete.status_code == 403
    assert archive.status_code == 403
    assert create_monitor.status_code == 403

    async with session_factory() as db:
        current = await db.get(Task, task_id)
        assert current is not None
        assert current.last_accessed_at is None


@pytest.mark.asyncio
async def test_task_creator_and_admin_can_share_but_other_member_cannot(
    secured_client,
):
    client, session_factory = secured_client
    owner_id, owner_token = await _create_user(
        session_factory,
        email="task-share-owner@example.com",
        role="member",
    )
    _other_id, other_token = await _create_user(
        session_factory,
        email="task-share-other@example.com",
        role="member",
    )
    target_id, _ = await _create_user(
        session_factory,
        email="task-share-target@example.com",
        role="member",
    )
    _admin_id, admin_token = await _create_user(
        session_factory,
        email="task-share-admin@example.com",
        role="admin",
    )
    async with session_factory() as db:
        owner_task = Task(
            title="creator may share",
            description="creator authority",
            created_by=owner_id,
        )
        admin_task = Task(
            title="admin may share",
            description="administrator authority",
            created_by=owner_id,
        )
        db.add_all([owner_task, admin_task])
        await db.commit()
        owner_task_id, admin_task_id = owner_task.id, admin_task.id

    body = {"target_type": "user", "target_id": target_id}
    owner_share = await client.post(
        f"/api/team/tasks/{owner_task_id}/share",
        headers=_headers(owner_token),
        json=body,
    )
    unrelated_share = await client.post(
        f"/api/team/tasks/{admin_task_id}/share",
        headers=_headers(other_token),
        json=body,
    )
    admin_share = await client.post(
        f"/api/team/tasks/{admin_task_id}/share",
        headers=_headers(admin_token),
        json=body,
    )

    assert owner_share.status_code == 200, owner_share.text
    assert unrelated_share.status_code == 403
    assert admin_share.status_code == 200, admin_share.text


def _member_request(user_id: int):
    return SimpleNamespace(
        state=SimpleNamespace(
            user_id=user_id,
            user_role="member",
            auth_type="jwt",
        ),
        headers={},
    )


def _request_without_user_id(*, role: str):
    return SimpleNamespace(
        state=SimpleNamespace(
            user_id=None,
            user_role=role,
            auth_type="jwt" if role == "member" else "deployment_token",
        ),
        headers={},
    )


@pytest.mark.asyncio
async def test_task_collections_fail_closed_for_member_without_user_id(
    secured_client,
):
    """A missing member identity must never turn into Queue's no-filter mode."""

    from backend.api.tasks import count_tasks, get_queue, list_tasks
    from backend.services.task_queue import TaskQueue

    _client, session_factory = secured_client
    async with session_factory() as db:
        db.add(Task(
            title="must not leak through absent identity",
            description="private",
            status="pending",
            created_by=999,
        ))
        await db.commit()
        queue = TaskQueue(db)

        member_request = _request_without_user_id(role="member")
        assert await count_tasks(member_request, queue=queue) == {"total": 0}
        assert await list_tasks(member_request, queue=queue) == []
        assert await get_queue(member_request, queue=queue) == []

        # Deployment-token/super-admin requests deliberately retain the
        # Queue's user_id=None meaning: infrastructure administrators can
        # inspect all Tasks even though they are not represented by a User.
        admin_request = _request_without_user_id(role="super_admin")
        assert await count_tasks(admin_request, queue=queue) == {"total": 1}
        listed = await list_tasks(admin_request, queue=queue, db=db)
        queued = await get_queue(admin_request, queue=queue, db=db)
        assert len(json.loads(listed.body)) == 1
        assert len(json.loads(queued.body)) == 1


@pytest.mark.asyncio
async def test_group_member_requires_active_user_and_is_idempotent(
    secured_client,
):
    """Group ACLs cannot be pre-seeded for future or disabled identities."""

    client, session_factory = secured_client
    _admin_id, admin_token = await _create_user(
        session_factory,
        email="group-membership-admin@example.com",
        role="admin",
    )
    active_id, _ = await _create_user(
        session_factory,
        email="group-membership-active@example.com",
        role="member",
    )
    inactive_id, _ = await _create_user(
        session_factory,
        email="group-membership-inactive@example.com",
        role="member",
        active=False,
    )
    async with session_factory() as db:
        group = UserGroup(name="group-membership-validation")
        db.add(group)
        await db.commit()
        group_id = group.id

    headers = _headers(admin_token)
    first = await client.post(
        f"/api/team/groups/{group_id}/members",
        headers=headers,
        json={"user_id": active_id},
    )
    duplicate = await client.post(
        f"/api/team/groups/{group_id}/members",
        headers=headers,
        json={"user_id": active_id},
    )
    inactive = await client.post(
        f"/api/team/groups/{group_id}/members",
        headers=headers,
        json={"user_id": inactive_id},
    )
    absent = await client.post(
        f"/api/team/groups/{group_id}/members",
        headers=headers,
        json={"user_id": 2_000_000_000},
    )

    assert first.status_code == 200, first.text
    assert first.json() == {"ok": True}
    assert duplicate.status_code == 200, duplicate.text
    assert duplicate.json() == {"ok": True, "message": "Already a member"}
    assert inactive.status_code == 404
    assert inactive.json()["detail"] == "Active user not found"
    assert absent.status_code == 404
    assert absent.json()["detail"] == "Active user not found"
    async with session_factory() as db:
        count = await db.scalar(
            select(func.count(UserGroupMember.id)).where(
                UserGroupMember.group_id == group_id,
                UserGroupMember.user_id == active_id,
            )
        )
        assert count == 1


@pytest.mark.asyncio
async def test_concurrent_group_member_adds_are_idempotent(tmp_path):
    """Two independent requests converge on one durable membership row."""

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from backend.api.team_sharing import GroupMemberAdd, add_group_member
    from backend.database import Base
    from backend.models.user import User

    # The suite's shared in-memory SQLite connection cannot model two real
    # transactions. A file database gives each request an independent
    # connection and exercises the same cross-process writer/constraint race.
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'group-membership-race.db'}"
    )
    sessions = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        async with sessions() as db:
            user = User(
                email="group-membership-race-member@example.com",
                name="race-member",
                password_hash="not-used",
                role="member",
                is_active=True,
            )
            group = UserGroup(name="group-membership-race")
            db.add_all([user, group])
            await db.commit()
            member_id, group_id = user.id, group.id

        async def add_once():
            async with sessions() as db:
                return await add_group_member(
                    group_id,
                    GroupMemberAdd(user_id=member_id),
                    _request_without_user_id(role="admin"),
                    db,
                )

        first, second = await asyncio.gather(add_once(), add_once())

        assert first["ok"] is True
        assert second["ok"] is True
        async with sessions() as db:
            rows = (
                await db.execute(
                    select(UserGroupMember).where(
                        UserGroupMember.group_id == group_id,
                        UserGroupMember.user_id == member_id,
                    )
                )
            ).scalars().all()
            assert len(rows) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_group_membership_database_constraint_rejects_duplicates(
    secured_client,
):
    """The unique constraint protects imports and cross-process races."""

    _client, session_factory = secured_client
    user_id, _ = await _create_user(
        session_factory,
        email="group-membership-constraint@example.com",
        role="member",
    )
    async with session_factory() as db:
        group = UserGroup(name="group-membership-constraint")
        db.add(group)
        await db.flush()
        db.add(UserGroupMember(group_id=group.id, user_id=user_id))
        await db.commit()

        db.add(UserGroupMember(group_id=group.id, user_id=user_id))
        with pytest.raises(IntegrityError):
            await db.commit()
        await db.rollback()


@pytest.mark.asyncio
async def test_task_effect_fence_accepts_chat_share_without_project_share(
    secured_client,
):
    """A Task share is independent authority, not a Project-share alias."""

    from backend.api.deps import lock_task_effect_access

    _client, session_factory = secured_client
    recipient_id, _ = await _create_user(
        session_factory,
        email="task-only-chat-recipient@example.com",
        role="member",
    )
    async with session_factory() as db:
        project = Project(name="task-only-chat-project", status="ready")
        db.add(project)
        await db.flush()
        task = Task(
            title="task-only chat authority",
            description="no Project ACL",
            project_id=project.id,
            created_by=999,
        )
        db.add(task)
        await db.flush()
        db.add(TeamTaskShare(
            task_id=task.id,
            target_type="user",
            target_id=recipient_id,
            permission="chat",
            shared_by=999,
        ))
        await db.commit()
        task_id, project_id = task.id, project.id

        admitted = await lock_task_effect_access(
            _member_request(recipient_id),
            task,
            db,
            allow_chat_share=True,
        )

        assert admitted.id == task_id
        assert admitted.project_id == project_id
        await db.rollback()
        current = await db.get(Task, task_id)
        assert current is not None

        with pytest.raises(HTTPException) as control_error:
            await lock_task_effect_access(
                _member_request(recipient_id),
                current,
                db,
                allow_chat_share=False,
            )
        assert control_error.value.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("database_role", "request_role"),
    [
        ("member", "admin"),
        ("admin", "member"),
    ],
)
async def test_task_effect_fence_rejects_cached_jwt_role_change(
    secured_client,
    database_role,
    request_role,
):
    """Role promotion and demotion both invalidate an in-flight snapshot."""

    from backend.api.deps import lock_task_effect_access

    _client, session_factory = secured_client
    user_id, _ = await _create_user(
        session_factory,
        email=f"effect-role-{database_role}-{request_role}@example.com",
        role=database_role,
    )
    async with session_factory() as db:
        task = Task(
            title="cached role effect authority",
            created_by=user_id,
        )
        db.add(task)
        await db.commit()
        request = SimpleNamespace(
            state=SimpleNamespace(
                user_id=user_id,
                user_role=request_role,
                auth_type="jwt",
            ),
            headers={},
        )

        with pytest.raises(HTTPException) as caught:
            await lock_task_effect_access(
                request,
                task,
                db,
                allow_chat_share=False,
            )

        assert caught.value.status_code == 409
        assert "changed role" in caught.value.detail


@pytest.mark.asyncio
async def test_task_effect_fence_keeps_manager_deployment_token_compatible(
    secured_client,
):
    """Manager AUTH_TOKEN authority has no mutable User row to re-lock."""

    from backend.api.deps import lock_task_effect_access

    _client, session_factory = secured_client
    async with session_factory() as db:
        task = Task(
            title="deployment-token effect authority",
            created_by=999_999,
        )
        db.add(task)
        await db.commit()

        admitted = await lock_task_effect_access(
            SimpleNamespace(
                state=SimpleNamespace(
                    user_id=None,
                    user_role="super_admin",
                    auth_type="token",
                ),
                headers={},
            ),
            task,
            db,
            allow_chat_share=False,
        )

        assert admitted.id == task.id
        await db.rollback()


@pytest.mark.asyncio
async def test_project_worker_effect_fence_uses_global_creation_lock_order(
    monkeypatch,
):
    """Project work admission locks node, Project, Worker, ACL, then User."""

    import backend.api.deps as deps
    import backend.services.worker_assignment as worker_assignment
    import backend.services.worker_node_control as worker_node_control

    order: list[str] = []
    project = SimpleNamespace(id=17, worker_id=23)

    async def lock_project(project_id, db):
        assert project_id == project.id
        order.append("project")
        return project

    async def lock_node(db):
        order.append("node")

    async def lock_worker(db, worker_id):
        assert worker_id == project.worker_id
        order.append("worker")

    async def check_acl(request, project_id, db, *, effect_fence=False):
        assert project_id == project.id
        assert effect_fence is True
        order.append("membership_acl")
        return True

    async def lock_user(request, db):
        order.append("user")

    monkeypatch.setattr(deps, "_lock_project_effect_fence", lock_project)
    monkeypatch.setattr(
        worker_node_control,
        "fence_worker_node_mutation",
        lock_node,
    )
    monkeypatch.setattr(
        worker_assignment,
        "fence_ready_worker_assignment",
        lock_worker,
    )
    monkeypatch.setattr(deps, "has_project_access", check_acl)
    monkeypatch.setattr(deps, "lock_request_user_authority", lock_user)

    admitted = await deps.lock_project_worker_effect_access(
        _member_request(31),
        project.id,
        object(),
    )

    assert admitted is project
    assert order == ["node", "project", "worker", "membership_acl", "user"]


@pytest.mark.asyncio
async def test_projectless_worker_effect_fence_locks_node_before_worker_user(
    monkeypatch,
):
    """Projectless work uses node -> Worker -> access -> actor User."""

    import backend.api.deps as deps
    import backend.services.worker_assignment as worker_assignment
    import backend.services.worker_node_control as worker_node_control

    order: list[str] = []

    async def lock_node(db):
        order.append("node")

    async def lock_worker(db, worker_id):
        assert worker_id == 37
        order.append("worker")

    async def check_worker(request, worker_id, db):
        assert worker_id == 37
        order.append("worker_access")

    async def lock_user(request, db):
        order.append("user")

    monkeypatch.setattr(
        worker_node_control,
        "fence_worker_node_mutation",
        lock_node,
    )
    monkeypatch.setattr(
        worker_assignment,
        "fence_ready_worker_assignment",
        lock_worker,
    )
    monkeypatch.setattr(deps, "require_worker_target_access", check_worker)
    monkeypatch.setattr(deps, "lock_request_user_authority", lock_user)

    await deps.lock_worker_effect_access(
        _member_request(41),
        37,
        object(),
    )

    assert order == ["node", "worker", "worker_access", "user"]


@pytest.mark.asyncio
async def test_task_worker_effect_fence_uses_global_creation_lock_order(
    monkeypatch,
):
    """Task work admission locks Project, Task, Worker, ACL, then User."""

    import backend.api.deps as deps
    import backend.services.task_sharing as task_sharing
    import backend.services.worker_assignment as worker_assignment

    order: list[str] = []
    task = SimpleNamespace(id=41, project_id=43, worker_id=47)

    class FakeDB:
        async def rollback(self):
            return None

        async def get(self, model, task_id, **kwargs):
            assert task_id == task.id
            return task

    async def lock_project(project_id, db):
        assert project_id == task.project_id
        order.append("project")

    async def lock_task(db, current):
        assert current is task
        order.append("task")
        return True

    async def lock_worker(db, worker_id):
        assert worker_id == task.worker_id
        order.append("worker")

    async def check_acl(request, current, db, **kwargs):
        assert current is task
        assert kwargs == {"allow_chat_share": False, "effect_fence": True}
        order.append("membership_acl")
        return True

    async def lock_user(request, db):
        order.append("user")

    monkeypatch.setattr(deps, "_lock_project_effect_fence", lock_project)
    monkeypatch.setattr(task_sharing, "lock_task_share_authority", lock_task)
    monkeypatch.setattr(
        worker_assignment,
        "fence_ready_worker_assignment",
        lock_worker,
    )
    monkeypatch.setattr(deps, "_task_access_allowed", check_acl)
    monkeypatch.setattr(deps, "lock_request_user_authority", lock_user)

    admitted = await deps.lock_task_effect_access(
        _member_request(53),
        task,
        FakeDB(),
        allow_chat_share=False,
        fence_worker_assignment=True,
    )

    assert admitted is task
    assert order == ["project", "task", "worker", "membership_acl", "user"]


@pytest.mark.asyncio
async def test_multi_task_worker_effect_fence_sorts_all_resource_rows(
    monkeypatch,
):
    """Multi-Task admission sorts Projects, Tasks, and distinct Workers."""

    import backend.api.deps as deps
    import backend.services.task_sharing as task_sharing
    import backend.services.worker_assignment as worker_assignment
    import backend.services.worker_node_control as worker_node_control

    tasks = {
        61: SimpleNamespace(id=61, project_id=71, worker_id=83),
        59: SimpleNamespace(id=59, project_id=67, worker_id=79),
        63: SimpleNamespace(id=63, project_id=71, worker_id=79),
    }
    order: list[tuple[str, int] | tuple[str, None]] = []

    class FakeDB:
        async def rollback(self):
            return None

        async def get(self, model, task_id, **kwargs):
            order.append(("task_get", task_id))
            return tasks[task_id]

    async def lock_project(project_id, db):
        order.append(("project", project_id))

    async def lock_node(db):
        order.append(("node", None))

    async def lock_task(db, current):
        order.append(("task_lock", current.id))
        return True

    async def lock_worker(db, worker_id):
        order.append(("worker", worker_id))

    async def check_acl(request, current, db, **kwargs):
        order.append(("acl", current.id))
        return True

    async def lock_user(request, db):
        order.append(("user", None))

    monkeypatch.setattr(deps, "_lock_project_effect_fence", lock_project)
    monkeypatch.setattr(
        worker_node_control,
        "fence_worker_node_mutation",
        lock_node,
    )
    monkeypatch.setattr(task_sharing, "lock_task_share_authority", lock_task)
    monkeypatch.setattr(
        worker_assignment,
        "fence_ready_worker_assignment",
        lock_worker,
    )
    monkeypatch.setattr(deps, "_task_access_allowed", check_acl)
    monkeypatch.setattr(deps, "lock_request_user_authority", lock_user)

    admitted = await deps.lock_task_effect_accesses(
        _member_request(89),
        [tasks[61], tasks[59], tasks[63]],
        FakeDB(),
        allow_chat_share=False,
        fence_worker_node=True,
        fence_worker_assignment=True,
    )

    assert [task.id for task in admitted] == [61, 59, 63]
    assert order == [
        ("node", None),
        ("project", 67),
        ("project", 71),
        ("task_get", 59),
        ("task_lock", 59),
        ("task_get", 61),
        ("task_lock", 61),
        ("task_get", 63),
        ("task_lock", 63),
        ("worker", 79),
        ("worker", 83),
        ("acl", 61),
        ("acl", 59),
        ("acl", 63),
        ("user", None),
    ]


@pytest.mark.asyncio
async def test_project_backed_task_share_can_chat_but_cannot_control(
    secured_client,
    monkeypatch,
):
    """Exercise the public endpoint, including its final durable ACL fence."""

    from backend.main import broadcaster, dispatcher

    client, session_factory = secured_client
    owner_id, _ = await _create_user(
        session_factory,
        email="project-task-chat-owner@example.com",
        role="member",
    )
    recipient_id, recipient_token = await _create_user(
        session_factory,
        email="project-task-chat-recipient@example.com",
        role="member",
    )
    async with session_factory() as db:
        project = Project(name="project-task-chat-only", status="ready")
        db.add(project)
        await db.flush()
        task = Task(
            title="project task shared directly",
            description="recipient has no Project membership",
            project_id=project.id,
            target_repo="/tmp",
            status="completed",
            session_id="project-task-chat-session",
            created_by=owner_id,
        )
        db.add(task)
        await db.flush()
        db.add(TeamTaskShare(
            task_id=task.id,
            target_type="user",
            target_id=recipient_id,
            permission="chat",
            shared_by=owner_id,
        ))
        await db.commit()
        task_id = task.id

    enqueue_message = AsyncMock()
    broadcast = AsyncMock()
    monkeypatch.setattr(dispatcher, "enqueue_message", enqueue_message)
    monkeypatch.setattr(broadcaster, "broadcast", broadcast)
    headers = _headers(recipient_token)

    chatted = await client.post(
        f"/api/tasks/{task_id}/chat",
        headers=headers,
        json={"message": "Task share should be sufficient"},
    )
    controlled = await client.put(
        f"/api/tasks/{task_id}",
        headers=headers,
        json={"title": "must not be changed"},
    )

    assert chatted.status_code == 200, chatted.text
    assert chatted.json()["queued"] is True
    enqueue_message.assert_awaited_once()
    queued_principal = enqueue_message.await_args.kwargs
    assert queued_principal["initiating_user_id"] == recipient_id
    assert queued_principal["initiating_user_role"] == "member"
    assert queued_principal["execution_mode"] == "sandbox"
    assert queued_principal["execution_principal_kind"] == "user"
    assert controlled.status_code == 403

    async with session_factory() as db:
        current = await db.get(Task, task_id)
        assert current.title == "project task shared directly"


@pytest.mark.asyncio
async def test_chat_share_rejects_every_structured_control_payload(
    secured_client,
):
    """A chat grant is text/managed-upload authority, not Task control."""

    client, session_factory = secured_client
    owner_id, _ = await _create_user(
        session_factory,
        email="chat-payload-owner@example.com",
        role="member",
    )
    recipient_id, recipient_token = await _create_user(
        session_factory,
        email="chat-payload-recipient@example.com",
        role="member",
    )
    async with session_factory() as db:
        project = Project(name="chat-payload-project", status="ready")
        db.add(project)
        await db.flush()
        task = Task(
            title="chat payload ACL",
            description="chat share cannot steer execution",
            project_id=project.id,
            target_repo="/tmp",
            status="completed",
            session_id="chat-payload-session",
            provider="claude",
            model="claude-sonnet-4-6",
            created_by=owner_id,
        )
        db.add(task)
        await db.flush()
        db.add(TeamTaskShare(
            task_id=task.id,
            target_type="user",
            target_id=recipient_id,
            permission="chat",
            shared_by=owner_id,
        ))
        await db.commit()
        task_id = task.id

    payloads = [
        {"message": "override", "model": "claude-opus-4-8"},
        {
            "message": "route",
            "expected_routing": {
                "provider": "claude",
                "model": "claude-sonnet-4-6",
                "codex_service_tier": "default",
            },
        },
        {"message": "legacy plan", "plan_task_ids": [101]},
        {"message": "versioned plan", "plan_version_ids": [101]},
        {
            "message": "confirm legacy staleness",
            "confirmed_stale_plan_task_ids": [101],
        },
        {
            "message": "confirm staleness",
            "confirmed_stale_plan_version_ids": [101],
        },
        {"message": "secret", "secret_ids": [101]},
        {"message": "$monitor inspect the task"},
        {"message": "$unknown-command"},
    ]
    for payload in payloads:
        response = await client.post(
            f"/api/tasks/{task_id}/chat",
            headers=_headers(recipient_token),
            json=payload,
        )
        assert response.status_code == 403, (payload, response.text)

    async with session_factory() as db:
        assert await db.scalar(
            select(func.count(LogEntry.id)).where(LogEntry.task_id == task_id)
        ) == 0


@pytest.mark.asyncio
async def test_chat_share_accepts_managed_upload_but_rejects_forged_host_path(
    secured_client,
    monkeypatch,
    tmp_path,
):
    """Shared chat attachments remain confined to CCM's upload root."""

    import backend.api.uploads as uploads_api
    from backend.main import broadcaster, dispatcher

    monkeypatch.setattr(uploads_api, "UPLOAD_DIR", tmp_path)
    managed = tmp_path / "11111111-1111-4111-8111-111111111111.txt"
    managed.write_text("shared attachment", encoding="utf-8")

    client, session_factory = secured_client
    owner_id, _ = await _create_user(
        session_factory,
        email="chat-upload-owner@example.com",
        role="member",
    )
    recipient_id, recipient_token = await _create_user(
        session_factory,
        email="chat-upload-recipient@example.com",
        role="member",
    )
    async with session_factory() as db:
        task = Task(
            title="chat upload ACL",
            description="managed uploads only",
            target_repo="/tmp",
            status="completed",
            session_id="chat-upload-session",
            created_by=owner_id,
        )
        db.add(task)
        await db.flush()
        db.add(TeamTaskShare(
            task_id=task.id,
            target_type="user",
            target_id=recipient_id,
            permission="chat",
            shared_by=owner_id,
        ))
        await db.commit()
        task_id = task.id

    enqueue_message = AsyncMock()
    monkeypatch.setattr(dispatcher, "enqueue_message", enqueue_message)
    monkeypatch.setattr(broadcaster, "broadcast", AsyncMock())
    headers = _headers(recipient_token)

    accepted = await client.post(
        f"/api/tasks/{task_id}/chat",
        headers=headers,
        json={
            "message": "read this",
            "file_paths": [
                "/api/uploads/11111111-1111-4111-8111-111111111111.txt"
            ],
        },
    )
    forged = await client.post(
        f"/api/tasks/{task_id}/chat",
        headers=headers,
        json={"message": "steal host file", "file_paths": ["/etc/passwd"]},
    )

    assert accepted.status_code == 200, accepted.text
    assert enqueue_message.await_args.kwargs["attachment_paths"] == (
        str(managed),
    )
    assert forged.status_code == 422, forged.text
    assert enqueue_message.await_count == 1


@pytest.mark.asyncio
async def test_control_chat_final_fence_does_not_fall_back_to_task_chat_share(
    secured_client,
    monkeypatch,
):
    """Revoked Project control cannot degrade into chat-only authorization."""

    import backend.api.chat as chat_api
    from backend.main import broadcaster, dispatcher
    from backend.models.log_entry import LogEntry

    client, session_factory = secured_client
    owner_id, _ = await _create_user(
        session_factory,
        email="control-fence-owner@example.com",
        role="member",
    )
    recipient_id, recipient_token = await _create_user(
        session_factory,
        email="control-fence-recipient@example.com",
        role="member",
    )
    async with session_factory() as db:
        project = Project(name="control-fence-project", status="ready")
        db.add(project)
        await db.flush()
        task = Task(
            title="control payload final fence",
            description="Project grant is revoked during prompt preparation",
            project_id=project.id,
            target_repo="/tmp",
            status="completed",
            session_id="control-fence-session",
            provider="claude",
            model="claude-sonnet-4-6",
            created_by=owner_id,
        )
        db.add(task)
        await db.flush()
        project_share = TeamProjectShare(
            project_id=project.id,
            target_type="user",
            target_id=recipient_id,
            shared_by=owner_id,
        )
        db.add_all([
            project_share,
            TeamTaskShare(
                task_id=task.id,
                target_type="user",
                target_id=recipient_id,
                permission="chat",
                shared_by=owner_id,
            ),
        ])
        await db.commit()
        task_id, project_share_id = task.id, project_share.id

    async def revoke_control_after_initial_admission(_request, _db):
        async with session_factory() as revoke_db:
            current_share = await revoke_db.get(
                TeamProjectShare,
                project_share_id,
            )
            assert current_share is not None
            await revoke_db.delete(current_share)
            await revoke_db.commit()
        return None

    enqueue_message = AsyncMock()
    monkeypatch.setattr(
        chat_api,
        "_sender_display_name",
        revoke_control_after_initial_admission,
    )
    monkeypatch.setattr(dispatcher, "enqueue_message", enqueue_message)
    monkeypatch.setattr(broadcaster, "broadcast", AsyncMock())

    response = await client.post(
        f"/api/tasks/{task_id}/chat",
        headers=_headers(recipient_token),
        json={
            "message": "must retain control authority",
            "model": "claude-opus-4-8",
        },
    )

    assert response.status_code == 403, response.text
    enqueue_message.assert_not_awaited()
    async with session_factory() as db:
        assert await db.scalar(
            select(func.count(LogEntry.id)).where(LogEntry.task_id == task_id)
        ) == 0


@pytest.mark.asyncio
async def test_local_chat_final_fence_stops_task_share_revoked_during_prompt_prep(
    secured_client,
    monkeypatch,
):
    """No user log/outbox may commit from an optimistic, now-revoked ACL."""

    import backend.api.chat as chat_api
    from backend.main import broadcaster, dispatcher
    from backend.models.log_entry import LogEntry

    client, session_factory = secured_client
    owner_id, _ = await _create_user(
        session_factory,
        email="final-fence-chat-owner@example.com",
        role="member",
    )
    recipient_id, recipient_token = await _create_user(
        session_factory,
        email="final-fence-chat-recipient@example.com",
        role="member",
    )
    async with session_factory() as db:
        project = Project(name="final-chat-fence-project", status="ready")
        db.add(project)
        await db.flush()
        task = Task(
            title="final chat ACL fence",
            description="share is revoked during prompt preparation",
            project_id=project.id,
            target_repo="/tmp",
            status="completed",
            session_id="final-chat-fence-session",
            created_by=owner_id,
        )
        db.add(task)
        await db.flush()
        share = TeamTaskShare(
            task_id=task.id,
            target_type="user",
            target_id=recipient_id,
            permission="chat",
            shared_by=owner_id,
        )
        db.add(share)
        await db.commit()
        task_id, share_id = task.id, share.id

    async def revoke_after_initial_admission(_request, _db):
        async with session_factory() as revoke_db:
            current_share = await revoke_db.get(TeamTaskShare, share_id)
            assert current_share is not None
            await revoke_db.delete(current_share)
            await revoke_db.commit()
        return None

    enqueue_message = AsyncMock()
    monkeypatch.setattr(
        chat_api,
        "_sender_display_name",
        revoke_after_initial_admission,
    )
    monkeypatch.setattr(dispatcher, "enqueue_message", enqueue_message)
    monkeypatch.setattr(broadcaster, "broadcast", AsyncMock())

    response = await client.post(
        f"/api/tasks/{task_id}/chat",
        headers=_headers(recipient_token),
        json={"message": "must be rejected before durable logging"},
    )

    assert response.status_code == 403, response.text
    enqueue_message.assert_not_awaited()
    async with session_factory() as db:
        assert await db.scalar(
            select(func.count(LogEntry.id)).where(LogEntry.task_id == task_id)
        ) == 0


@pytest.mark.asyncio
async def test_task_effect_fence_rejects_task_share_revoked_before_writer_lock(
    secured_client,
    monkeypatch,
):
    """If revocation wins the race, no stale chat authority crosses the fence."""

    import backend.api.deps as deps

    _client, session_factory = secured_client
    recipient_id, _ = await _create_user(
        session_factory,
        email="revoked-task-chat-recipient@example.com",
        role="member",
    )
    async with session_factory() as setup_db:
        project = Project(name="revoked-task-chat-project", status="ready")
        setup_db.add(project)
        await setup_db.flush()
        task = Task(
            title="revoked task chat authority",
            description="revocation wins",
            project_id=project.id,
            created_by=999,
        )
        setup_db.add(task)
        await setup_db.flush()
        share = TeamTaskShare(
            task_id=task.id,
            target_type="user",
            target_id=recipient_id,
            permission="chat",
            shared_by=999,
        )
        setup_db.add(share)
        await setup_db.commit()
        task_id, share_id = task.id, share.id

    original_project_fence = deps._lock_project_effect_fence
    revoked = False

    async def revoke_then_take_project_fence(project_id, db):
        nonlocal revoked
        if not revoked:
            revoked = True
            async with session_factory() as revoke_db:
                current_share = await revoke_db.get(TeamTaskShare, share_id)
                assert current_share is not None
                await revoke_db.delete(current_share)
                await revoke_db.commit()
        return await original_project_fence(project_id, db)

    monkeypatch.setattr(
        deps,
        "_lock_project_effect_fence",
        revoke_then_take_project_fence,
    )

    async with session_factory() as db:
        stale_task = await db.get(Task, task_id)
        with pytest.raises(HTTPException) as exc_info:
            await deps.lock_task_effect_access(
                _member_request(recipient_id),
                stale_task,
                db,
                allow_chat_share=True,
            )

        assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_task_effect_fence_accepts_group_task_share(
    secured_client,
):
    """Group membership is a valid independent Task chat authority."""

    from backend.api.deps import lock_task_effect_access

    _client, session_factory = secured_client
    recipient_id, _ = await _create_user(
        session_factory,
        email="group-task-chat-recipient@example.com",
        role="member",
    )
    async with session_factory() as db:
        project = Project(name="group-task-chat-project", status="ready")
        group = UserGroup(name="group-task-chat-group")
        db.add_all([project, group])
        await db.flush()
        task = Task(
            title="group task chat authority",
            description="group-only Task ACL",
            project_id=project.id,
            created_by=999,
        )
        db.add(task)
        await db.flush()
        db.add_all([
            UserGroupMember(group_id=group.id, user_id=recipient_id),
            TeamTaskShare(
                task_id=task.id,
                target_type="group",
                target_id=group.id,
                permission="chat",
                shared_by=999,
            ),
        ])
        await db.commit()
        task_id = task.id

        admitted = await lock_task_effect_access(
            _member_request(recipient_id),
            task,
            db,
            allow_chat_share=True,
        )

        assert admitted.id == task_id
        await db.rollback()


@pytest.mark.asyncio
async def test_group_membership_revocation_is_rechecked_inside_task_effect_fence(
    secured_client,
    monkeypatch,
):
    """A removed group member cannot cross the final Task effect boundary."""

    import backend.api.deps as deps
    from sqlalchemy import delete

    _client, session_factory = secured_client
    recipient_id, _ = await _create_user(
        session_factory,
        email="revoked-group-task-recipient@example.com",
        role="member",
    )
    async with session_factory() as setup_db:
        project = Project(name="revoked-group-task-project", status="ready")
        group = UserGroup(name="revoked-group-task-group")
        setup_db.add_all([project, group])
        await setup_db.flush()
        task = Task(
            title="revoked group task authority",
            description="membership is removed at final admission",
            project_id=project.id,
            created_by=999,
        )
        setup_db.add(task)
        await setup_db.flush()
        setup_db.add_all([
            UserGroupMember(group_id=group.id, user_id=recipient_id),
            TeamTaskShare(
                task_id=task.id,
                target_type="group",
                target_id=group.id,
                permission="chat",
                shared_by=999,
            ),
        ])
        await setup_db.commit()
        task_id = task.id

    original_membership_fence = (
        deps._lock_user_group_membership_authority
    )
    revoked = False

    async def revoke_then_lock_membership(user_id, db):
        nonlocal revoked
        if not revoked:
            revoked = True
            await db.execute(
                delete(UserGroupMember).where(
                    UserGroupMember.user_id == user_id
                )
            )
        await original_membership_fence(user_id, db)

    monkeypatch.setattr(
        deps,
        "_lock_user_group_membership_authority",
        revoke_then_lock_membership,
    )

    async with session_factory() as db:
        task = await db.get(Task, task_id)
        with pytest.raises(HTTPException) as exc_info:
            await deps.lock_task_effect_access(
                _member_request(recipient_id),
                task,
                db,
                allow_chat_share=True,
            )

    assert revoked is True
    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_group_project_membership_is_fenced_for_project_effects(
    secured_client,
    monkeypatch,
):
    """Project effects admit a live group member and reject final revocation."""

    import backend.api.deps as deps
    from sqlalchemy import delete

    _client, session_factory = secured_client
    recipient_id, _ = await _create_user(
        session_factory,
        email="group-project-recipient@example.com",
        role="member",
    )
    async with session_factory() as setup_db:
        project = Project(name="group-project-effect", status="ready")
        group = UserGroup(name="group-project-effect-group")
        setup_db.add_all([project, group])
        await setup_db.flush()
        setup_db.add_all([
            UserGroupMember(group_id=group.id, user_id=recipient_id),
            TeamProjectShare(
                project_id=project.id,
                target_type="group",
                target_id=group.id,
                shared_by=999,
            ),
        ])
        await setup_db.commit()
        project_id = project.id

    async with session_factory() as db:
        admitted = await deps.lock_project_effect_access(
            _member_request(recipient_id),
            project_id,
            db,
        )
        assert admitted.id == project_id
        await db.rollback()

    original_membership_fence = (
        deps._lock_user_group_membership_authority
    )

    async def revoke_then_lock_membership(user_id, db):
        await db.execute(
            delete(UserGroupMember).where(
                UserGroupMember.user_id == user_id
            )
        )
        await original_membership_fence(user_id, db)

    monkeypatch.setattr(
        deps,
        "_lock_user_group_membership_authority",
        revoke_then_lock_membership,
    )
    async with session_factory() as db:
        with pytest.raises(HTTPException) as exc_info:
            await deps.lock_project_effect_access(
                _member_request(recipient_id),
                project_id,
                db,
            )

    assert exc_info.value.status_code == 403


@pytest.mark.asyncio
async def test_task_effect_fence_relocks_new_project_after_concurrent_move(
    secured_client,
    monkeypatch,
):
    """A stale Project observation retries in Project->Task lock order."""

    import backend.api.deps as deps

    _client, session_factory = secured_client
    recipient_id, _ = await _create_user(
        session_factory,
        email="moved-task-chat-recipient@example.com",
        role="member",
    )
    async with session_factory() as setup_db:
        old_project = Project(name="task-project-before-move", status="ready")
        new_project = Project(name="task-project-after-move", status="ready")
        setup_db.add_all([old_project, new_project])
        await setup_db.flush()
        task = Task(
            title="moving task chat authority",
            description="project changes during admission",
            project_id=old_project.id,
            created_by=999,
        )
        setup_db.add(task)
        await setup_db.flush()
        setup_db.add(TeamTaskShare(
            task_id=task.id,
            target_type="user",
            target_id=recipient_id,
            permission="chat",
            shared_by=999,
        ))
        await setup_db.commit()
        task_id = task.id
        old_project_id, new_project_id = old_project.id, new_project.id

    original_project_fence = deps._lock_project_effect_fence
    locked_projects: list[int] = []

    async def move_then_take_project_fence(project_id, db):
        locked_projects.append(project_id)
        if len(locked_projects) == 1:
            async with session_factory() as move_db:
                await move_db.execute(
                    update(Task)
                    .where(Task.id == task_id)
                    .values(project_id=new_project_id)
                )
                await move_db.commit()
        return await original_project_fence(project_id, db)

    monkeypatch.setattr(
        deps,
        "_lock_project_effect_fence",
        move_then_take_project_fence,
    )

    async with session_factory() as db:
        # Load the old identity before the simulated concurrent mover wins.
        stale_task = await db.get(Task, task_id)
        assert stale_task.project_id == old_project_id
        admitted = await deps.lock_task_effect_access(
            _member_request(recipient_id),
            stale_task,
            db,
            allow_chat_share=True,
        )

        assert admitted.project_id == new_project_id
        assert locked_projects == [old_project_id, new_project_id]
        await db.rollback()


@pytest.mark.asyncio
async def test_project_reads_follow_acl_but_mutations_are_admin_only(
    secured_client,
    tmp_path,
):
    client, session_factory = secured_client
    alice_id, alice_token = await _create_user(
        session_factory,
        email="project-alice@example.com",
        role="member",
    )
    bob_id, bob_token = await _create_user(
        session_factory,
        email="project-bob@example.com",
        role="member",
    )
    project_root = tmp_path / "victim-project"
    project_root.mkdir()
    (project_root / ".env").write_text("SECRET=value\n")
    own_project_root = tmp_path / "visible-project"
    own_project_root.mkdir()
    (own_project_root / ".env").write_text("VISIBLE_SECRET=value\n")

    async with session_factory() as db:
        alice_worker = await _add_worker(
            db,
            name="project-alice-worker",
            owner_user_id=alice_id,
        )
        bob_worker = await _add_worker(
            db,
            name="project-bob-worker",
            owner_user_id=bob_id,
        )
        own_project = Project(
            name="project-alice-visible",
            worker_id=alice_worker.id,
            local_path=str(own_project_root),
            status="ready",
            git_url=(
                "https://alice:embedded-token@"
                "[2001:db8::1]:8443/org/repo.git"
                "?access_token=query-secret#fragment-secret"
            ),
            has_remote=True,
            env_files=[".env"],
            git_credential_type="https",
            git_ssh_key_path="/manager/private/id_ed25519",
            git_https_username="alice",
            git_https_token="visible-project-token",
            preview_config={
                "version": 1,
                "processes": [
                    {
                        "command": ["preview", "--token", "argv-secret"],
                        "env": {"CUSTOM_PREVIEW_TOKEN": "env-secret"},
                    }
                ],
            },
            tags=["alice-tag"],
        )
        victim_project = Project(
            name="project-bob-private",
            worker_id=bob_worker.id,
            local_path=str(project_root),
            status="ready",
            env_files=[".env"],
            git_credential_type="https",
            git_https_username="victim",
            git_https_token="victim-secret-token",
            tags=["victim-tag"],
        )
        local_project = Project(
            name="project-local-admin-only",
            worker_id=None,
            local_path="/tmp/project-local-admin-only",
            status="ready",
        )
        db.add_all([own_project, victim_project, local_project])
        await db.flush()
        # Worker ownership is a compute-control relationship, not a Project
        # ACL. Give Alice access only to the one Project she should see.
        db.add(
            TeamProjectShare(
                project_id=own_project.id,
                target_type="user",
                target_id=alice_id,
                shared_by=bob_id,
            )
        )
        # A stale/inconsistent Task reference must not grant access to the
        # Project's tags either.
        db.add(
            Task(
                title="stale cross-worker reference",
                description="stale",
                worker_id=alice_worker.id,
                project_id=victim_project.id,
                created_by=bob_id,
            )
        )
        await db.commit()
        ids = {
            "own": own_project.id,
            "victim": victim_project.id,
            "local": local_project.id,
        }

    headers = _headers(alice_token)
    projects = await client.get("/api/projects", headers=headers)
    own_detail = await client.get(
        f"/api/projects/{ids['own']}",
        headers=headers,
    )
    victim_detail = await client.get(
        f"/api/projects/{ids['victim']}",
        headers=headers,
    )
    todos = await client.get(
        f"/api/projects/{ids['victim']}/todos",
        headers=headers,
    )
    env_list = await client.get(
        f"/api/projects/{ids['own']}/env-files",
        headers=headers,
    )
    env_file = await client.get(
        f"/api/projects/{ids['own']}/env-files/.env",
        headers=headers,
    )
    env_write = await client.put(
        f"/api/projects/{ids['own']}/env-files/.env",
        headers=headers,
        json={"content": "FORGED=value\n"},
    )
    env_scan = await client.post(
        f"/api/projects/{ids['own']}/scan-env-files",
        headers=headers,
    )
    update_own = await client.put(
        f"/api/projects/{ids['own']}",
        headers=headers,
        json={"name": "member-mutated-project"},
    )
    reclone_own = await client.post(
        f"/api/projects/{ids['own']}/reclone",
        headers=headers,
    )
    reorder_empty = await client.put(
        "/api/projects/reorder",
        headers=headers,
        json=[],
    )
    reorder_victim = await client.put(
        "/api/projects/reorder",
        headers=headers,
        json=[{"id": ids["victim"], "sort_order": 99}],
    )
    tags = await client.get("/api/projects/tags", headers=headers)
    victim_shares = await client.get(
        f"/api/team/projects/{ids['victim']}/shares",
        headers=headers,
    )
    forge_victim_share = await client.post(
        f"/api/team/projects/{ids['victim']}/share",
        headers=headers,
        json={"target_type": "user", "target_id": alice_id},
    )
    assert projects.status_code == 200
    assert [row["id"] for row in projects.json()] == [ids["own"]]
    assert own_detail.status_code == 200
    for payload in (projects.json()[0], own_detail.json()):
        assert payload["git_url"] == (
            "https://[2001:db8::1]:8443/org/repo.git"
        )
        assert payload["git_ssh_key_path"] is None
        assert payload["git_https_token"] is None
        assert payload["preview_config"] is None
    assert "embedded-token" not in projects.text
    assert "query-secret" not in projects.text
    assert "fragment-secret" not in projects.text
    assert "visible-project-token" not in projects.text
    assert "embedded-token" not in own_detail.text
    assert "query-secret" not in own_detail.text
    assert "fragment-secret" not in own_detail.text
    assert "visible-project-token" not in own_detail.text
    assert "argv-secret" not in own_detail.text
    assert "env-secret" not in own_detail.text
    assert victim_detail.status_code == 403
    assert todos.status_code == 403
    assert [
        response.status_code
        for response in (env_list, env_file, env_write, env_scan)
    ] == [403, 403, 403, 403]
    assert update_own.status_code == 403
    assert reclone_own.status_code == 403
    assert reorder_empty.status_code == 403
    assert reorder_victim.status_code == 403
    assert tags.status_code == 200
    assert tags.json() == ["alice-tag"]
    assert victim_shares.status_code == 403
    assert forge_victim_share.status_code == 403

    bob_headers = _headers(bob_token)
    invalid_target_type = await client.post(
        f"/api/team/projects/{ids['victim']}/share",
        headers=bob_headers,
        json={"target_type": "invalid", "target_id": alice_id},
    )
    missing_target = await client.post(
        f"/api/team/projects/{ids['victim']}/share",
        headers=bob_headers,
        json={"target_type": "user", "target_id": 999_999},
    )
    valid_share = await client.post(
        f"/api/team/projects/{ids['victim']}/share",
        headers=bob_headers,
        json={"target_type": "user", "target_id": alice_id},
    )
    assert invalid_target_type.status_code == 422
    assert missing_target.status_code == 403
    assert valid_share.status_code == 403

    # Deployment/super-admin authority retains the complete management view.
    admin_detail = await client.get(
        f"/api/projects/{ids['own']}",
        headers={"Authorization": "Bearer security-service-token"},
    )
    assert admin_detail.status_code == 200
    assert admin_detail.json()["git_url"].startswith(
        "https://alice:embedded-token@"
    )
    assert admin_detail.json()["git_ssh_key_path"] == (
        "/manager/private/id_ed25519"
    )
    assert admin_detail.json()["git_https_token"] == "visible-project-token"
    assert admin_detail.json()["preview_config"]["processes"][0]["env"] == {
        "CUSTOM_PREVIEW_TOKEN": "env-secret"
    }


@pytest.mark.asyncio
async def test_queue_monitor_and_sub_agent_routes_enforce_task_acl_and_service_auth(
    secured_client,
):
    client, session_factory = secured_client
    owner_id, owner_token = await _create_user(
        session_factory,
        email="agent-owner@example.com",
        role="member",
    )
    outsider_id, outsider_token = await _create_user(
        session_factory,
        email="agent-outsider@example.com",
        role="member",
    )
    async with session_factory() as db:
        owner_worker = await _add_worker(
            db,
            name="agent-owner-worker",
            owner_user_id=owner_id,
        )
        outsider_worker = await _add_worker(
            db,
            name="agent-outsider-worker",
            owner_user_id=outsider_id,
        )
        owner_task = Task(
            title="owner pending",
            description="owner",
            worker_id=owner_worker.id,
            created_by=owner_id,
            status="pending",
        )
        outsider_task = Task(
            title="outsider pending",
            description="outsider",
            worker_id=outsider_worker.id,
            created_by=outsider_id,
            status="pending",
        )
        db.add_all([owner_task, outsider_task])
        await db.flush()
        monitor = MonitorSession(
            task_id=owner_task.id,
            agent_type="monitor",
            source="ccm",
            description="owner monitor",
            status="running",
            checks_done=0,
        )
        sub_agent = SubAgentSession(
            task_id=owner_task.id,
            agent_type="sub_agent",
            source="ccm",
            description="owner sub agent",
            status="running",
            checks_done=0,
        )
        db.add_all([monitor, sub_agent])
        await db.commit()
        ids = {
            "owner_task": owner_task.id,
            "outsider_task": outsider_task.id,
            "monitor": monitor.id,
            "sub_agent": sub_agent.id,
        }

    outsider_headers = _headers(outsider_token)
    owner_headers = _headers(owner_token)
    queue = await client.get("/api/tasks/queue/next", headers=outsider_headers)
    monitor_list = await client.get(
        f"/api/tasks/{ids['owner_task']}/monitor-sessions",
        headers=outsider_headers,
    )
    monitor_spoof = await client.post(
        (
            f"/api/tasks/{ids['owner_task']}/monitor-sessions/"
            f"{ids['monitor']}/checks"
        ),
        headers=owner_headers,
        json={"summary": "forged"},
    )
    sub_agent_spoof = await client.post(
        (
            f"/api/tasks/{ids['owner_task']}/sub-agent-sessions/"
            f"{ids['sub_agent']}/progress"
        ),
        headers=owner_headers,
        json={"summary": "forged"},
    )
    sub_agent_summary = await client.get(
        f"/api/tasks/{ids['owner_task']}/sub-agents/summary",
        headers=outsider_headers,
    )

    assert queue.status_code == 200
    assert [row["id"] for row in queue.json()] == [ids["outsider_task"]]
    assert monitor_list.status_code == 403
    assert monitor_spoof.status_code == 403
    assert sub_agent_spoof.status_code == 403
    assert sub_agent_summary.status_code == 403

    service_headers = {"Authorization": "Bearer security-service-token"}
    monitor_service = await client.post(
        (
            f"/api/tasks/{ids['owner_task']}/monitor-sessions/"
            f"{ids['monitor']}/checks"
        ),
        headers=service_headers,
        json={"summary": "real service callback"},
    )
    sub_agent_service = await client.post(
        (
            f"/api/tasks/{ids['owner_task']}/sub-agent-sessions/"
            f"{ids['sub_agent']}/progress"
        ),
        headers=service_headers,
        json={"summary": "real service callback"},
    )
    assert monitor_service.status_code == 200, monitor_service.text
    assert sub_agent_service.status_code == 200, sub_agent_service.text


@pytest.mark.asyncio
async def test_pr_monitor_follows_project_acl_not_worker_ownership(
    secured_client,
):
    client, session_factory = secured_client
    alice_id, alice_token = await _create_user(
        session_factory,
        email="pr-alice@example.com",
        role="member",
    )
    bob_id, bob_token = await _create_user(
        session_factory,
        email="pr-bob@example.com",
        role="member",
    )
    async with session_factory() as db:
        alice_worker = await _add_worker(
            db,
            name="pr-alice-worker",
            owner_user_id=alice_id,
        )
        bob_worker = await _add_worker(
            db,
            name="pr-bob-worker",
            owner_user_id=bob_id,
        )
        project = Project(
            name="pr-project-shared-to-bob",
            worker_id=alice_worker.id,
            local_path="/tmp/pr-project-shared-to-bob",
            status="ready",
        )
        db.add(project)
        await db.flush()
        db.add(TeamProjectShare(
            project_id=project.id,
            target_type="user",
            target_id=bob_id,
            shared_by=alice_id,
        ))
        repo = MonitoredRepo(
            repo_full_name="private/repository",
            project_id=project.id,
            worker_id=alice_worker.id,
            webhook_secret="full-private-secret",
        )
        projectless_repo = MonitoredRepo(
            repo_full_name="admin/projectless-repository",
            project_id=None,
            worker_id=alice_worker.id,
            webhook_secret="admin-only-secret",
        )
        db.add_all([repo, projectless_repo])
        await db.flush()
        review = PRReview(
            repo_id=repo.id,
            pr_number=7,
            base_ref="main",
            head_sha="a" * 40,
            pr_title="Private PR",
            pr_author="private-author",
            pr_url="https://example.invalid/private/repository/pull/7",
            status="pending",
        )
        db.add(review)
        await db.commit()
        ids = {
            "alice_worker": alice_worker.id,
            "bob_worker": bob_worker.id,
            "project": project.id,
            "repo": repo.id,
            "projectless_repo": projectless_repo.id,
            "review": review.id,
        }

    alice_headers = _headers(alice_token)
    alice_list = await client.get(
        "/api/pr-monitor/repos",
        headers=alice_headers,
    )
    denied = [
        await client.get(
            f"/api/pr-monitor/repos/{ids['repo']}",
            headers=alice_headers,
        ),
        await client.put(
            f"/api/pr-monitor/repos/{ids['repo']}",
            headers=alice_headers,
            json={"enabled": False},
        ),
        await client.get(
            f"/api/pr-monitor/repos/{ids['repo']}/reviews",
            headers=alice_headers,
        ),
        await client.get(
            f"/api/pr-monitor/reviews/{ids['review']}",
            headers=alice_headers,
        ),
        await client.post(
            f"/api/pr-monitor/repos/{ids['repo']}/toggle",
            headers=alice_headers,
        ),
        await client.post(
            f"/api/pr-monitor/repos/{ids['repo']}/regenerate-secret",
            headers=alice_headers,
        ),
        await client.post(
            "/api/pr-monitor/repos",
            headers=alice_headers,
            json={
                "repo_full_name": "private/new-repository",
                "worker_id": ids["alice_worker"],
            },
        ),
        await client.get(
            f"/api/pr-monitor/repos/{ids['projectless_repo']}",
            headers=alice_headers,
        ),
    ]
    assert alice_list.status_code == 200
    assert alice_list.json() == []
    assert [response.status_code for response in denied] == [403] * len(denied)

    bob_headers = _headers(bob_token)
    bob_list = await client.get(
        "/api/pr-monitor/repos",
        headers=bob_headers,
    )
    detail = await client.get(
        f"/api/pr-monitor/repos/{ids['repo']}",
        headers=bob_headers,
    )
    reviews = await client.get(
        f"/api/pr-monitor/repos/{ids['repo']}/reviews",
        headers=bob_headers,
    )
    created = await client.post(
        "/api/pr-monitor/repos",
        headers=bob_headers,
        json={
            "repo_full_name": "private/project-collaborator-monitor",
            "project_id": ids["project"],
            "worker_id": ids["alice_worker"],
        },
    )
    mismatched = await client.post(
        "/api/pr-monitor/repos",
        headers=bob_headers,
        json={
            "repo_full_name": "private/mismatched-monitor",
            "project_id": ids["project"],
            "worker_id": ids["bob_worker"],
        },
    )
    projectless_create = await client.post(
        "/api/pr-monitor/repos",
        headers=bob_headers,
        json={
            "repo_full_name": "private/member-projectless-monitor",
            "worker_id": ids["bob_worker"],
        },
    )
    remove_project_acl = await client.put(
        f"/api/pr-monitor/repos/{ids['repo']}",
        headers=bob_headers,
        json={"project_id": None},
    )
    assert detail.status_code == 200
    assert detail.json()["webhook_secret"] == "full***"
    assert "full-private-secret" not in detail.text
    assert reviews.status_code == 200
    assert [row["id"] for row in reviews.json()] == [ids["review"]]
    assert bob_list.status_code == 200
    assert [row["id"] for row in bob_list.json()] == [ids["repo"]]
    assert created.status_code == 200, created.text
    assert created.json()["project_id"] == ids["project"]
    assert created.json()["worker_id"] == ids["alice_worker"]
    assert mismatched.status_code == 400
    assert projectless_create.status_code == 403
    assert remove_project_acl.status_code == 403

    admin_headers = {"Authorization": "Bearer security-service-token"}
    admin_list = await client.get(
        "/api/pr-monitor/repos",
        headers=admin_headers,
    )
    admin_projectless = await client.get(
        f"/api/pr-monitor/repos/{ids['projectless_repo']}",
        headers=admin_headers,
    )
    assert admin_list.status_code == 200
    assert ids["projectless_repo"] in {
        row["id"] for row in admin_list.json()
    }
    assert admin_projectless.status_code == 200


@pytest.mark.asyncio
async def test_pr_monitor_task_is_not_authorized_by_same_name_project_share(
    secured_client,
):
    """A synthetic/legacy Project grant cannot expose Controller prompts."""

    from backend.api.ws import _ws_task_channel_allowed
    from backend.services.pr_review_service import (
        PR_MONITOR_INTERNAL_PROJECT_TAG,
        PR_MONITOR_PROJECT_NAME,
        _get_or_create_pr_monitor_project,
    )

    client, session_factory = secured_client
    member_id, member_token = await _create_user(
        session_factory,
        email="pr-task-project-collision@example.com",
        role="member",
    )
    secret_prompt = "RAW_REVIEW_PROMPT_MUST_NOT_LEAK"
    secret_history = "RAW_REVIEW_HISTORY_MUST_NOT_LEAK"
    async with session_factory() as db:
        ordinary_project = Project(
            name=PR_MONITOR_PROJECT_NAME,
            local_path="/tmp/ordinary-pr-monitor-project",
            status="ready",
        )
        db.add(ordinary_project)
        await db.flush()
        db.add(TeamProjectShare(
            project_id=ordinary_project.id,
            target_type="user",
            target_id=member_id,
            shared_by=member_id,
        ))
        ordinary_task = Task(
            title="real task in the colliding project",
            description="member-owned work",
            status="pending",
            project_id=ordinary_project.id,
            created_by=member_id,
        )
        db.add(ordinary_task)
        await db.flush()

        internal_project_id = await _get_or_create_pr_monitor_project(db)
        assert internal_project_id != ordinary_project.id
        assert PR_MONITOR_INTERNAL_PROJECT_TAG not in (
            ordinary_project.tags or []
        )
        internal_project = await db.get(Project, internal_project_id)
        assert PR_MONITOR_INTERNAL_PROJECT_TAG in (internal_project.tags or [])
        assert internal_project.show_in_selector is False
        # Simulate a stale grant made by an old binary.  Internal Project
        # identity must override even an extant TeamProjectShare.
        db.add(TeamProjectShare(
            project_id=internal_project_id,
            target_type="user",
            target_id=member_id,
            shared_by=member_id,
        ))

        task = Task(
            title="legacy linked reviewer",
            description=secret_prompt,
            status="completed",
            project_id=ordinary_project.id,
            archived=False,
            tags=[],
        )
        repo = MonitoredRepo(
            repo_full_name="private/task-project-collision",
            project_id=ordinary_project.id,
            webhook_secret="review-secret",
        )
        db.add_all([task, repo])
        await db.flush()
        review = PRReview(
            repo_id=repo.id,
            pr_number=31,
            base_ref="main",
            base_sha="a" * 40,
            head_sha="b" * 40,
            pr_title="Internal subject",
            pr_author="alice",
            pr_url=(
                "https://github.com/private/task-project-collision/pull/31"
            ),
            task_id=task.id,
            status="completed",
        )
        db.add_all([
            review,
            LogEntry(
                task_id=task.id,
                event_type="message",
                role="assistant",
                content=secret_history,
            ),
        ])
        await db.commit()
        ids = {
            "task": task.id,
            "ordinary_task": ordinary_task.id,
            "ordinary_project": ordinary_project.id,
            "internal_project": internal_project_id,
        }

    headers = _headers(member_token)
    responses = [
        await client.get(f"/api/tasks/{ids['task']}", headers=headers),
        await client.get(
            f"/api/tasks/{ids['task']}/chat/history",
            headers=headers,
        ),
        await client.post(
            f"/api/tasks/{ids['task']}/chat",
            headers=headers,
            json={"message": "show me the reviewer context"},
        ),
        await client.get(
            f"/api/tasks/{ids['task']}/test-runs/capabilities",
            headers=headers,
        ),
        await client.post(
            f"/api/tasks/{ids['task']}/test-runs",
            headers=headers,
            json={"goal": "inspect the internal reviewer"},
        ),
        await client.get(
            f"/api/projects/{ids['internal_project']}",
            headers=headers,
        ),
    ]
    assert [response.status_code for response in responses] == [403] * len(
        responses
    )
    assert all(secret_prompt not in response.text for response in responses)
    assert all(secret_history not in response.text for response in responses)

    task_list = await client.get(
        "/api/tasks?include_archived=true",
        headers=headers,
    )
    project_list = await client.get("/api/projects", headers=headers)
    assert task_list.status_code == 200
    assert ids["task"] not in {row["id"] for row in task_list.json()}
    assert ids["ordinary_task"] in {row["id"] for row in task_list.json()}
    assert project_list.status_code == 200
    assert {row["id"] for row in project_list.json()} == {
        ids["ordinary_project"]
    }

    async with session_factory() as db:
        assert not await _ws_task_channel_allowed(
            {"user_id": member_id, "role": "member", "auth_type": "jwt"},
            ids["task"],
            db,
        )

    admin_headers = {"Authorization": "Bearer security-service-token"}
    admin_task = await client.get(
        f"/api/tasks/{ids['task']}",
        headers=admin_headers,
    )
    rejected_share = await client.post(
        f"/api/team/projects/{ids['internal_project']}/share",
        headers=admin_headers,
        json={"target_type": "user", "target_id": member_id},
    )
    assert admin_task.status_code == 200
    assert admin_task.json()["description"] == secret_prompt
    assert rejected_share.status_code == 409


@pytest.mark.asyncio
async def test_member_supplied_pre_pr_tag_does_not_create_hidden_acl_identity(
    secured_client,
):
    """A runtime-reserved tag alone cannot make creator/list ACL disagree."""

    from backend.services.pr_review_runtime import PRE_PR_CODE_REVIEW_TAG

    client, session_factory = secured_client
    member_id, member_token = await _create_user(
        session_factory,
        email="member-pre-pr-tag@example.com",
        role="member",
    )
    async with session_factory() as db:
        project = Project(
            name="member-pre-pr-tag-project",
            local_path="/tmp/member-pre-pr-tag-project",
            status="ready",
        )
        db.add(project)
        await db.flush()
        db.add(TeamProjectShare(
            project_id=project.id,
            target_type="user",
            target_id=member_id,
            shared_by=member_id,
        ))
        await db.commit()
        project_id = project.id

    headers = _headers(member_token)
    created = await client.post(
        "/api/tasks",
        headers=headers,
        json={
            "title": "member supplied reserved tag",
            "description": "still an ordinary creator-owned Task",
            "project_id": project_id,
            "tags": [PRE_PR_CODE_REVIEW_TAG],
        },
    )
    assert created.status_code == 201, created.text
    task_id = created.json()["id"]

    direct = await client.get(f"/api/tasks/{task_id}", headers=headers)
    listed = await client.get(
        "/api/tasks?include_archived=true",
        headers=headers,
    )

    assert direct.status_code == 200, direct.text
    assert direct.json()["id"] == task_id
    assert listed.status_code == 200
    assert task_id in {row["id"] for row in listed.json()}


@pytest.mark.asyncio
async def test_discussion_events_require_discussion_owner(secured_client):
    client, session_factory = secured_client
    owner_id, owner_token = await _create_user(
        session_factory,
        email="discussion-owner@example.com",
        role="member",
    )
    _, outsider_token = await _create_user(
        session_factory,
        email="discussion-outsider@example.com",
        role="member",
    )
    async with session_factory() as db:
        discussion = Discussion(
            title="private discussion",
            creator_user_id=owner_id,
        )
        db.add(discussion)
        await db.flush()
        agent = DiscussionAgent(
            discussion_id=discussion.id,
            role_name="reviewer",
            system_prompt="review",
        )
        db.add(agent)
        await db.flush()
        db.add(
            DiscussionEvent(
                discussion_id=discussion.id,
                agent_id=agent.id,
                event_type="assistant",
                content="private event",
            )
        )
        await db.commit()
        discussion_id = discussion.id
        agent_id = agent.id

    path = f"/api/discussions/{discussion_id}/agents/{agent_id}/events"
    outsider = await client.get(path, headers=_headers(outsider_token))
    owner = await client.get(path, headers=_headers(owner_token))
    assert outsider.status_code == 403
    assert owner.status_code == 200
    assert owner.json()[0]["content"] == "private event"


@pytest.mark.asyncio
async def test_discussion_agent_workloads_are_admin_only(secured_client):
    client, session_factory = secured_client
    member_id, member_token = await _create_user(
        session_factory,
        email="discussion-workload-member@example.com",
        role="member",
    )
    async with session_factory() as db:
        await _add_worker(
            db,
            name="discussion-member-worker",
            owner_user_id=member_id,
        )
        await db.commit()

    member_create = await client.post(
        "/api/discussions",
        headers=_headers(member_token),
        json={"title": "must not launch"},
    )
    admin_create = await client.post(
        "/api/discussions",
        headers={"Authorization": "Bearer security-service-token"},
        json={"title": "admin discussion"},
    )

    assert member_create.status_code == 403
    assert member_create.json()["detail"] == "Admin only"
    assert admin_create.status_code == 201


@pytest.mark.asyncio
async def test_host_files_and_global_git_credentials_are_admin_only(
    secured_client,
    tmp_path,
    monkeypatch,
):
    client, session_factory = secured_client
    _, member_token = await _create_user(
        session_factory,
        email="host-files-member@example.com",
        role="member",
    )
    member_headers = _headers(member_token)

    member_files = await client.get(
        "/api/files/list",
        params={"path": str(tmp_path)},
        headers=member_headers,
    )
    member_git = await client.get("/api/settings/git", headers=member_headers)
    assert member_files.status_code == 403
    assert member_git.status_code == 403

    service_headers = {"Authorization": "Bearer security-service-token"}
    admin_files = await client.get(
        "/api/files/list",
        params={"path": str(tmp_path)},
        headers=service_headers,
    )
    admin_git = await client.get("/api/settings/git", headers=service_headers)
    assert admin_files.status_code == 200
    assert admin_git.status_code == 200

    target = tmp_path / "uploads"
    target.mkdir()
    traversal = await client.post(
        "/api/files/upload",
        headers=service_headers,
        data={"target_dir": str(target)},
        files=[
            ("files", ("prefix.txt", b"must roll back")),
            ("files", ("../escape.txt", b"escape")),
        ],
    )
    assert traversal.status_code == 400
    assert not (tmp_path / "escape.txt").exists()
    assert not (target / "prefix.txt").exists()

    import backend.api.files as files_api

    monkeypatch.setattr(files_api, "MAX_UPLOAD_TOTAL_SIZE", 5)
    over_total = await client.post(
        "/api/files/upload",
        headers=service_headers,
        data={"target_dir": str(target)},
        files=[
            ("files", ("first.txt", b"abc")),
            ("files", ("second.txt", b"def")),
        ],
    )
    assert over_total.status_code == 400
    assert "combined" in over_total.json()["detail"].lower()
    assert not (target / "first.txt").exists()
    assert not (target / "second.txt").exists()

    normal = await client.post(
        "/api/files/upload",
        headers=service_headers,
        data={"target_dir": str(target)},
        files={"files": ("safe.txt", b"safe")},
    )
    assert normal.status_code == 200, normal.text
    assert (target / "safe.txt").read_bytes() == b"safe"


@pytest.mark.asyncio
async def test_global_secrets_and_cross_project_tag_cascades_are_admin_only(
    secured_client,
):
    client, session_factory = secured_client
    member_id, member_token = await _create_user(
        session_factory,
        email="global-data-member@example.com",
        role="member",
    )
    other_id, _ = await _create_user(
        session_factory,
        email="global-data-other@example.com",
        role="member",
    )
    async with session_factory() as db:
        member_worker = await _add_worker(
            db,
            name="global-data-member-worker",
            owner_user_id=member_id,
        )
        other_worker = await _add_worker(
            db,
            name="global-data-other-worker",
            owner_user_id=other_id,
        )
        secret = Secret(name="global-token", content="plaintext-global-secret")
        tag = Tag(name="global-label", color="rose", created_by=member_id)
        victim_project = Project(
            name="global-data-victim-project",
            worker_id=other_worker.id,
            local_path="/tmp/global-data-victim-project",
            status="ready",
            tags=["global-label"],
        )
        local_task = Task(
            title="legacy local task",
            description="local",
            worker_id=None,
            created_by=member_id,
            session_id="private-session",
        )
        db.add_all([secret, tag, victim_project, local_task])
        await db.commit()
        ids = {
            "member_worker": member_worker.id,
            "secret": secret.id,
            "tag": tag.id,
            "victim_project": victim_project.id,
            "local_task": local_task.id,
        }

    headers = _headers(member_token)
    secret_list = await client.get("/api/secrets", headers=headers)
    secret_detail = await client.get(
        f"/api/secrets/{ids['secret']}",
        headers=headers,
    )
    task_with_secret = await client.post(
        "/api/tasks",
        headers=headers,
        json={
            "title": "guess secret",
            "description": "guess secret",
            "worker_id": ids["member_worker"],
            "secret_ids": [ids["secret"]],
        },
    )
    ordinary_task = await client.post(
        "/api/tasks",
        headers=headers,
        json={
            "title": "ordinary",
            "description": "ordinary",
            "worker_id": ids["member_worker"],
        },
    )
    chat_with_secret = await client.post(
        f"/api/tasks/{ids['local_task']}/chat",
        headers=headers,
        json={"message": "echo it", "secret_ids": [ids["secret"]]},
    )
    rename_tag = await client.put(
        f"/api/tags/{ids['tag']}",
        headers=headers,
        json={"name": "stolen-global-label"},
    )
    delete_tag = await client.delete(
        f"/api/tags/{ids['tag']}",
        headers=headers,
    )

    assert secret_list.status_code == 403
    assert secret_detail.status_code == 403
    assert task_with_secret.status_code == 403
    assert ordinary_task.status_code == 403
    assert chat_with_secret.status_code == 403
    assert rename_tag.status_code == 403
    assert delete_tag.status_code == 403

    async with session_factory() as db:
        victim = await db.get(Project, ids["victim_project"])
        assert victim.tags == ["global-label"]
        assert await db.get(Tag, ids["tag"]) is not None

    service_headers = {"Authorization": "Bearer security-service-token"}
    admin_secret = await client.get(
        f"/api/secrets/{ids['secret']}",
        headers=service_headers,
    )
    assert admin_secret.status_code == 200
    assert admin_secret.json()["content"] == "plaintext-global-secret"
