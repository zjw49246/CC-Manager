"""Sharing lifecycle fences that are easy to regress across API families."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import func, select

from backend.api import projects, sharing, team_sharing
from backend.models.delivery import DeliveryRun
from backend.models.log_entry import LogEntry
from backend.models.project import Project
from backend.models.task import Task
from backend.models.task_share import ProjectShare, SharedTaskReceived
from backend.models.team_share import TeamProjectShare, TeamTaskShare
from backend.models.user import User
from backend.models.user_group import UserGroup, UserGroupMember
from backend.schemas.project import (
    ProjectCreate,
    ProjectReorderItem,
    ProjectUpdate,
)
from backend.services.project_share_admission import ProjectShareAdmissionError
from backend.services.shared_relay import SharedRelay


def _request(*, user_id: int = 7, role: str = "member"):
    return SimpleNamespace(state=SimpleNamespace(
        user_id=user_id,
        user_role=role,
    ))


def _jwt_request(*, user_id: int, role: str):
    return SimpleNamespace(state=SimpleNamespace(
        auth_type="jwt",
        user_id=user_id,
        user_role=role,
    ))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected_status", "expected_detail"),
    [
        (
            ProjectShareAdmissionError(
                "Could not establish the Project sharing fence; retry"
            ),
            409,
            "retry",
        ),
        (ValueError("Project 404 not found"), 404, "Project not found"),
    ],
)
async def test_team_project_share_lock_preserves_admission_error_semantics(
    db_session,
    monkeypatch,
    error,
    expected_status,
    expected_detail,
):
    monkeypatch.setattr(
        team_sharing,
        "lock_project_share_authority",
        AsyncMock(side_effect=error),
    )

    with pytest.raises(HTTPException) as rejected:
        await team_sharing._lock_project_share_authority(404, db_session)

    assert rejected.value.status_code == expected_status
    assert expected_detail in rejected.value.detail


@pytest.mark.asyncio
async def test_feishu_project_share_admission_error_maps_to_conflict(
    db_session,
    monkeypatch,
):
    monkeypatch.setattr(
        sharing.task_sharing,
        "share_project",
        AsyncMock(side_effect=ProjectShareAdmissionError("local Agent active")),
    )

    with pytest.raises(HTTPException) as rejected:
        await sharing.share_project(
            7,
            sharing.ShareRequest(targets=[]),
            db_session,
        )

    assert rejected.value.status_code == 409
    assert rejected.value.detail == "local Agent active"


@pytest.mark.asyncio
async def test_team_task_share_reauthorizes_after_taking_task_lock(
    db_session,
    monkeypatch,
):
    task = Task(
        title="authority changes while waiting",
        description="acl fence",
        created_by=7,
    )
    db_session.add(task)
    await db_session.commit()

    checks = 0

    async def changing_authority(*_args):
        nonlocal checks
        checks += 1
        return checks == 1

    monkeypatch.setattr(team_sharing, "_can_share_task", changing_authority)
    with pytest.raises(HTTPException) as denied:
        await team_sharing.share_task(
            task.id,
            team_sharing.ShareBody(target_type="user", target_id=99),
            _request(),
            db_session,
        )

    assert denied.value.status_code == 403
    assert checks == 2
    assert await db_session.scalar(
        select(func.count()).select_from(TeamTaskShare)
    ) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["unshare", "list"])
async def test_team_task_share_reads_and_deletes_reauthorize_inside_lock(
    db_session,
    monkeypatch,
    operation,
):
    task = Task(
        title=f"task {operation} authority race",
        description="acl fence",
        created_by=7,
    )
    db_session.add(task)
    await db_session.flush()
    grant = TeamTaskShare(
        task_id=task.id,
        target_type="user",
        target_id=99,
        permission="chat",
        shared_by=7,
    )
    db_session.add(grant)
    await db_session.commit()
    grant_id = grant.id
    checks = 0

    async def changing_authority(*_args):
        nonlocal checks
        checks += 1
        return checks == 1

    monkeypatch.setattr(team_sharing, "_can_share_task", changing_authority)
    with pytest.raises(HTTPException) as denied:
        if operation == "unshare":
            await team_sharing.unshare_task(
                task.id,
                team_sharing.UnshareBody(target_type="user", target_id=99),
                _request(),
                db_session,
            )
        else:
            await team_sharing.list_task_shares(
                task.id,
                _request(),
                db_session,
            )

    assert denied.value.status_code == 403
    assert checks == 2
    assert await db_session.get(TeamTaskShare, grant_id) is not None


@pytest.mark.asyncio
async def test_team_project_share_reauthorizes_after_project_lock(
    db_session,
    monkeypatch,
):
    project = Project(name="project-authority-race", status="ready")
    db_session.add(project)
    await db_session.commit()
    checks = 0

    async def changing_authority(*_args):
        nonlocal checks
        checks += 1
        return checks == 1

    monkeypatch.setattr(team_sharing, "_can_share_project", changing_authority)
    with pytest.raises(HTTPException) as denied:
        await team_sharing.share_project(
            project.id,
            team_sharing.ShareBody(target_type="user", target_id=99),
            _request(role="admin"),
            db_session,
        )

    assert denied.value.status_code == 403
    assert checks == 2
    assert await db_session.scalar(
        select(func.count()).select_from(TeamProjectShare)
    ) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["unshare", "list"])
async def test_team_project_share_reads_and_deletes_reauthorize_inside_lock(
    db_session,
    monkeypatch,
    operation,
):
    project = Project(name=f"project-{operation}-authority-race", status="ready")
    db_session.add(project)
    await db_session.flush()
    grant = TeamProjectShare(
        project_id=project.id,
        target_type="user",
        target_id=99,
        shared_by=7,
    )
    db_session.add(grant)
    await db_session.commit()
    grant_id = grant.id
    checks = 0

    async def changing_authority(*_args):
        nonlocal checks
        checks += 1
        return checks == 1

    monkeypatch.setattr(team_sharing, "_can_share_project", changing_authority)
    with pytest.raises(HTTPException) as denied:
        if operation == "unshare":
            await team_sharing.unshare_project(
                project.id,
                team_sharing.UnshareBody(target_type="user", target_id=99),
                _request(role="admin"),
                db_session,
            )
        else:
            await team_sharing.list_project_shares(
                project.id,
                _request(role="admin"),
                db_session,
            )

    assert denied.value.status_code == 403
    assert checks == 2
    assert await db_session.get(TeamProjectShare, grant_id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["share", "unshare", "list"])
async def test_team_project_share_fences_cached_admin_role_after_resource_lock(
    db_session,
    operation,
):
    """A JWT authenticated before demotion cannot assign Project ACLs later."""

    actor = User(
        email=f"stale-project-admin-{operation}@example.test",
        name="Demoted project administrator",
        password_hash="unused",
        role="member",
        is_active=True,
    )
    project = Project(
        name=f"stale-project-admin-{operation}",
        status="ready",
    )
    db_session.add_all([actor, project])
    await db_session.flush()
    grant = TeamProjectShare(
        project_id=project.id,
        target_type="user",
        target_id=991,
        shared_by=actor.id,
    )
    if operation != "share":
        db_session.add(grant)
    await db_session.commit()
    grant_id = grant.id if operation != "share" else None

    request = _jwt_request(user_id=actor.id, role="admin")
    with pytest.raises(HTTPException) as rejected:
        if operation == "share":
            await team_sharing.share_project(
                project.id,
                team_sharing.ShareBody(target_type="user", target_id=991),
                request,
                db_session,
            )
        elif operation == "unshare":
            await team_sharing.unshare_project(
                project.id,
                team_sharing.UnshareBody(target_type="user", target_id=991),
                request,
                db_session,
            )
        else:
            await team_sharing.list_project_shares(
                project.id,
                request,
                db_session,
            )

    assert rejected.value.status_code == 409
    if grant_id is None:
        assert await db_session.scalar(
            select(func.count())
            .select_from(TeamProjectShare)
            .where(TeamProjectShare.project_id == project.id)
        ) == 0
    else:
        assert await db_session.get(TeamProjectShare, grant_id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["share", "unshare", "list"])
async def test_team_task_share_fences_cached_admin_role_after_resource_lock(
    db_session,
    operation,
):
    """Task ACL effects revalidate the current actor, not only Task ownership."""

    actor = User(
        email=f"stale-task-admin-{operation}@example.test",
        name="Demoted task administrator",
        password_hash="unused",
        role="member",
        is_active=True,
    )
    task = Task(
        title=f"stale task admin {operation}",
        description="actor authority fence",
        created_by=987654,
    )
    db_session.add_all([actor, task])
    await db_session.flush()
    grant = TeamTaskShare(
        task_id=task.id,
        target_type="user",
        target_id=992,
        permission="chat",
        shared_by=actor.id,
    )
    if operation != "share":
        db_session.add(grant)
    await db_session.commit()
    grant_id = grant.id if operation != "share" else None

    request = _jwt_request(user_id=actor.id, role="admin")
    with pytest.raises(HTTPException) as rejected:
        if operation == "share":
            await team_sharing.share_task(
                task.id,
                team_sharing.ShareBody(target_type="user", target_id=992),
                request,
                db_session,
            )
        elif operation == "unshare":
            await team_sharing.unshare_task(
                task.id,
                team_sharing.UnshareBody(target_type="user", target_id=992),
                request,
                db_session,
            )
        else:
            await team_sharing.list_task_shares(
                task.id,
                request,
                db_session,
            )

    assert rejected.value.status_code == 409
    if grant_id is None:
        assert await db_session.scalar(
            select(func.count())
            .select_from(TeamTaskShare)
            .where(TeamTaskShare.task_id == task.id)
        ) == 0
    else:
        assert await db_session.get(TeamTaskShare, grant_id) is not None


@pytest.mark.asyncio
async def test_task_creator_cannot_share_after_concurrent_disablement(db_session):
    actor = User(
        email="disabled-task-creator@example.test",
        name="Disabled task creator",
        password_hash="unused",
        role="member",
        is_active=False,
    )
    db_session.add(actor)
    await db_session.flush()
    task = Task(
        title="disabled creator share",
        description="actor authority fence",
        created_by=actor.id,
    )
    db_session.add(task)
    await db_session.commit()

    with pytest.raises(HTTPException) as rejected:
        await team_sharing.share_task(
            task.id,
            team_sharing.ShareBody(target_type="user", target_id=993),
            _jwt_request(user_id=actor.id, role="member"),
            db_session,
        )

    assert rejected.value.status_code == 409
    assert await db_session.scalar(
        select(func.count())
        .select_from(TeamTaskShare)
        .where(TeamTaskShare.task_id == task.id)
    ) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation",
    [
        "role",
        "create_group",
        "update_group",
        "delete_group",
        "add_member",
        "remove_member",
    ],
)
async def test_team_admin_mutations_fence_cached_actor_role(
    db_session,
    operation,
):
    """Group/role administration cannot reuse a JWT after admin demotion."""

    actor = User(
        email=f"stale-team-admin-{operation}@example.test",
        name="Demoted team administrator",
        password_hash="unused",
        role="member",
        is_active=True,
    )
    target = User(
        email=f"team-admin-target-{operation}@example.test",
        name="Team target",
        password_hash="unused",
        role="admin" if operation == "role" else "member",
        is_active=True,
    )
    group = UserGroup(
        name=f"team-authority-{operation}",
        description="before",
    )
    db_session.add_all([actor, target, group])
    await db_session.flush()
    membership = UserGroupMember(group_id=group.id, user_id=target.id)
    if operation == "remove_member":
        db_session.add(membership)
    await db_session.commit()
    membership_id = (
        membership.id if operation == "remove_member" else None
    )
    request = _jwt_request(user_id=actor.id, role="admin")

    with pytest.raises(HTTPException) as rejected:
        if operation == "role":
            await team_sharing.update_user_role(
                target.id,
                team_sharing.UpdateRoleBody(role="member"),
                request,
                db_session,
            )
        elif operation == "create_group":
            await team_sharing.create_group(
                team_sharing.GroupCreate(name="must-not-be-created"),
                request,
                db_session,
            )
        elif operation == "update_group":
            await team_sharing.update_group(
                group.id,
                team_sharing.GroupCreate(name="changed", description="after"),
                request,
                db_session,
            )
        elif operation == "delete_group":
            await team_sharing.delete_group(group.id, request, db_session)
        elif operation == "add_member":
            await team_sharing.add_group_member(
                group.id,
                team_sharing.GroupMemberAdd(user_id=target.id),
                request,
                db_session,
            )
        else:
            await team_sharing.remove_group_member(
                group.id,
                target.id,
                request,
                db_session,
            )

    assert rejected.value.status_code == 409
    await db_session.refresh(target)
    assert target.role == ("admin" if operation == "role" else "member")
    current_group = await db_session.get(UserGroup, group.id)
    assert current_group is not None
    assert current_group.name == f"team-authority-{operation}"
    assert current_group.description == "before"
    if operation == "create_group":
        assert await db_session.scalar(
            select(func.count())
            .select_from(UserGroup)
            .where(UserGroup.name == "must-not-be-created")
        ) == 0
    elif operation == "add_member":
        assert await db_session.scalar(
            select(func.count())
            .select_from(UserGroupMember)
            .where(
                UserGroupMember.group_id == group.id,
                UserGroupMember.user_id == target.id,
            )
        ) == 0
    elif operation == "remove_member":
        assert await db_session.get(UserGroupMember, membership_id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation",
    ["create", "reorder", "update", "delete", "reclone", "env_write"],
)
async def test_project_admin_mutations_fence_cached_actor_role(
    db_session,
    operation,
):
    """A demoted JWT cannot commit Project or workspace side effects."""

    actor = User(
        email=f"stale-project-mutation-{operation}@example.test",
        name="Demoted Project administrator",
        password_hash="unused",
        role="member",
        is_active=True,
    )
    project = Project(
        name=f"project-mutation-{operation}",
        status="ready",
        has_remote=True,
        git_url="https://example.test/repository.git",
        local_path="/does/not/matter",
        sort_order=1,
        env_files=[".env"],
    )
    db_session.add_all([actor, project])
    await db_session.commit()
    actor_id = actor.id
    project_id = project.id
    request = _jwt_request(user_id=actor_id, role="admin")

    with pytest.raises(HTTPException) as rejected:
        if operation == "create":
            await projects.create_project(
                request,
                ProjectCreate(name="must-not-be-created"),
                db_session,
            )
        elif operation == "reorder":
            await projects.reorder_projects(
                [ProjectReorderItem(id=project_id, sort_order=99)],
                request,
                db_session,
            )
        elif operation == "update":
            await projects.update_project(
                project_id,
                ProjectUpdate(name="must-not-change"),
                request,
                db_session,
            )
        elif operation == "delete":
            await projects.delete_project(project_id, request, db_session)
        elif operation == "reclone":
            await projects.reclone_project(project_id, request, db_session)
        else:
            await projects.update_env_file(
                project_id,
                ".env",
                projects.EnvFileContent(content="MUST_NOT_BE_WRITTEN=1"),
                request,
                db_session,
            )

    assert rejected.value.status_code == 409
    await db_session.rollback()
    current = await db_session.get(Project, project_id)
    assert current is not None
    assert current.name == f"project-mutation-{operation}"
    assert current.sort_order == 1
    assert current.status == "ready"
    assert await db_session.scalar(
        select(func.count())
        .select_from(Project)
        .where(Project.name == "must-not-be-created")
    ) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("task_fields", "detail_fragment"),
    [
        ({"mode": "delivery_loop"}, "Delivery-owned Tasks"),
        ({"tags": {"pr-review": True}}, "Automated PR workflow Tasks"),
    ],
)
async def test_team_writable_share_rejects_controller_owned_tasks(
    client,
    session_factory,
    task_fields,
    detail_fragment,
):
    async with session_factory() as db:
        target = User(
            email=f"target-{detail_fragment[:3]}@example.test",
            name="Target",
            password_hash="unused",
            is_active=True,
        )
        db.add(target)
        if task_fields.get("mode") == "delivery_loop":
            project = Project(name="delivery-share-policy", status="ready")
            db.add(project)
            await db.flush()
            run = DeliveryRun(
                admission_scope="test",
                idempotency_key="delivery-share-policy",
                request_hash="r" * 64,
                project_id=project.id,
                title="Delivery share policy",
                requirements="Test the sharing boundary",
                requirements_hash="q" * 64,
                policy_snapshot={},
                policy_hash="p" * 64,
                base_branch="main",
                delivery_branch="delivery/share-policy",
            )
            db.add(run)
            await db.flush()
            task_fields = {
                "mode": "delivery_loop",
                "delivery_run_id": run.id,
                "delivery_role": "developer",
            }
        task = Task(
            title="controller owned",
            description="must not become writable",
            **task_fields,
        )
        db.add(task)
        await db.commit()
        target_id = target.id
        task_id = task.id

    response = await client.post(
        f"/api/team/tasks/{task_id}/share",
        json={"target_type": "user", "target_id": target_id},
    )

    assert response.status_code == 409
    assert detail_fragment in response.json()["detail"]
    async with session_factory() as db:
        assert await db.scalar(
            select(func.count())
            .select_from(TeamTaskShare)
            .where(TeamTaskShare.task_id == task_id)
        ) == 0


@pytest.mark.asyncio
async def test_delete_group_purges_group_target_grants_only(
    client,
    session_factory,
):
    async with session_factory() as db:
        group = UserGroup(name="disposable-group")
        db.add(group)
        await db.flush()
        group_id = group.id
        db.add_all([
            UserGroupMember(group_id=group_id, user_id=88),
            TeamTaskShare(
                task_id=101,
                target_type="group",
                target_id=group_id,
                permission="chat",
                shared_by=1,
            ),
            TeamProjectShare(
                project_id=202,
                target_type="group",
                target_id=group_id,
                shared_by=1,
            ),
            TeamTaskShare(
                task_id=303,
                target_type="user",
                target_id=group_id,
                permission="chat",
                shared_by=1,
            ),
            TeamProjectShare(
                project_id=404,
                target_type="user",
                target_id=group_id,
                shared_by=1,
            ),
        ])
        await db.commit()

    response = await client.delete(f"/api/team/groups/{group_id}")
    assert response.status_code == 200

    async with session_factory() as db:
        assert await db.get(UserGroup, group_id) is None
        assert await db.scalar(
            select(func.count())
            .select_from(TeamTaskShare)
            .where(
                TeamTaskShare.target_type == "group",
                TeamTaskShare.target_id == group_id,
            )
        ) == 0
        assert await db.scalar(
            select(func.count())
            .select_from(TeamProjectShare)
            .where(
                TeamProjectShare.target_type == "group",
                TeamProjectShare.target_id == group_id,
            )
        ) == 0
        assert await db.scalar(
            select(func.count())
            .select_from(TeamTaskShare)
            .where(
                TeamTaskShare.target_type == "user",
                TeamTaskShare.target_id == group_id,
            )
        ) == 1
        assert await db.scalar(
            select(func.count())
            .select_from(TeamProjectShare)
            .where(
                TeamProjectShare.target_type == "user",
                TeamProjectShare.target_id == group_id,
            )
        ) == 1


@pytest.mark.asyncio
async def test_delete_project_purges_both_project_share_families(
    client,
    session_factory,
):
    async with session_factory() as db:
        project = Project(name="shared-project-to-delete", status="ready")
        db.add(project)
        await db.flush()
        project_id = project.id
        db.add_all([
            ProjectShare(
                project_id=project_id,
                shared_to_open_id="ou-recipient",
                shared_to_name="Recipient",
                shared_to_ccm_url="https://receiver.example.test",
                status="active",
            ),
            TeamProjectShare(
                project_id=project_id,
                target_type="user",
                target_id=77,
                shared_by=1,
            ),
        ])
        await db.commit()

    response = await client.delete(f"/api/projects/{project_id}")
    assert response.status_code == 200

    async with session_factory() as db:
        assert await db.scalar(
            select(func.count())
            .select_from(ProjectShare)
            .where(ProjectShare.project_id == project_id)
        ) == 0
        assert await db.scalar(
            select(func.count())
            .select_from(TeamProjectShare)
            .where(TeamProjectShare.project_id == project_id)
        ) == 0


async def _mismatched_shared_shadow(session_factory):
    async with session_factory() as db:
        shared = SharedTaskReceived(
            owner_ccm_url="https://owner.example.test",
            remote_task_id=42,
            share_token="exact-token",
            status="active",
        )
        replacement = Task(
            title="unrelated replacement",
            description="must not receive relay writes",
            status="pending",
        )
        db.add_all([shared, replacement])
        await db.flush()
        shared.local_task_id = replacement.id
        await db.commit()
        return shared, replacement.id


@pytest.mark.asyncio
async def test_stale_relay_cannot_write_to_unowned_local_task(
    session_factory,
):
    shared, replacement_id = await _mismatched_shared_shadow(session_factory)
    broadcaster = SimpleNamespace(broadcast=AsyncMock())
    relay = SharedRelay(session_factory, broadcaster)

    await relay._handle(
        {
            "data": {
                "event_type": "message",
                "role": "assistant",
                "content": "stale remote write",
            }
        },
        shared,
    )

    async with session_factory() as db:
        replacement = await db.get(Task, replacement_id)
        assert replacement.status == "pending"
        assert replacement.has_unread is False
        assert await db.scalar(
            select(func.count())
            .select_from(LogEntry)
            .where(LogEntry.task_id == replacement_id)
        ) == 0
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_shared_relay_uses_local_codex_provider_to_clear_remote_marker(
    session_factory,
):
    xml_example = (
        '<function_calls><invoke name="Bash">'
        '<parameter name="command">pwd</parameter>'
        '</invoke></function_calls>'
    )
    async with session_factory() as db:
        shared = SharedTaskReceived(
            owner_ccm_url="https://owner.example.test",
            remote_task_id=43,
            share_token="codex-relay-token",
            status="active",
        )
        db.add(shared)
        await db.flush()
        shadow = Task(
            title="Codex shadow",
            description="d",
            status="pending",
            provider="codex",
            shared_from_id=shared.id,
        )
        db.add(shadow)
        await db.flush()
        shared.local_task_id = shadow.id
        await db.commit()

    broadcaster = SimpleNamespace(broadcast=AsyncMock())
    relay = SharedRelay(session_factory, broadcaster)
    await relay._handle(
        {
            "data": {
                "event_type": "message",
                "role": "assistant",
                "content": xml_example,
                "protocol_anomaly": "legacy_tool_markup",
            }
        },
        shared,
    )

    event = broadcaster.broadcast.await_args.args[1]
    assert event["content"] == xml_example
    assert "protocol_anomaly" not in event


@pytest.mark.asyncio
async def test_shared_cleanup_does_not_cancel_unowned_local_task(
    session_factory,
):
    from backend.api.shared import _cleanup_shared

    shared, replacement_id = await _mismatched_shared_shadow(session_factory)
    async with session_factory() as db:
        current = await db.get(SharedTaskReceived, shared.id)
        with patch("backend.main.shared_relay", None):
            await _cleanup_shared(current, db)

    async with session_factory() as db:
        replacement = await db.get(Task, replacement_id)
        assert replacement.status == "pending"
        assert replacement.error_message is None
        assert await db.get(SharedTaskReceived, shared.id) is None


@pytest.mark.asyncio
async def test_directly_deleted_shadow_id_cannot_receive_stale_relay_write(
    session_factory,
):
    async with session_factory() as db:
        shared = SharedTaskReceived(
            owner_ccm_url="https://original-owner.example.test",
            remote_task_id=51,
            share_token="original-token",
            status="active",
        )
        db.add(shared)
        await db.flush()
        shadow = Task(
            title="original shadow",
            description="deleted out of band",
            status="pending",
            shared_from_id=shared.id,
        )
        db.add(shadow)
        await db.flush()
        shadow_id = shadow.id
        shared.local_task_id = shadow_id
        await db.commit()
        await db.delete(shadow)
        await db.commit()
        replacement = Task(
            id=shadow_id,
            title="explicit id replacement",
            description="must remain untouched",
            status="pending",
        )
        db.add(replacement)
        await db.commit()

    broadcaster = SimpleNamespace(broadcast=AsyncMock())
    relay = SharedRelay(session_factory, broadcaster)
    await relay._handle(
        {
            "data": {
                "event_type": "status_change",
                "new_status": "completed",
            }
        },
        shared,
    )

    async with session_factory() as db:
        replacement = await db.get(Task, shadow_id)
        assert replacement.status == "pending"
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_old_shadow_cannot_proxy_after_received_share_id_reuse(
    session_factory,
):
    from backend.api.chat import ChatMessage, _send_shared_chat
    from backend.api.shared import _cleanup_shared

    async with session_factory() as db:
        old_share = SharedTaskReceived(
            owner_ccm_url="https://old-owner.example.test",
            remote_task_id=61,
            share_token="old-token",
            status="active",
        )
        db.add(old_share)
        await db.flush()
        old_shadow = Task(
            title="old shadow",
            description="survives revoke as cancelled",
            status="pending",
            shared_from_id=old_share.id,
        )
        db.add(old_shadow)
        await db.flush()
        old_share.local_task_id = old_shadow.id
        await db.commit()
        old_share_id = old_share.id
        old_shadow_id = old_shadow.id
        with patch("backend.main.shared_relay", None):
            await _cleanup_shared(old_share, db)

        new_shadow = Task(
            title="new shadow",
            description="different remote owner",
            status="pending",
            shared_from_id=old_share_id,
        )
        db.add(new_shadow)
        await db.flush()
        new_share = SharedTaskReceived(
            id=old_share_id,
            owner_ccm_url="https://new-owner.example.test",
            remote_task_id=62,
            share_token="new-token",
            local_task_id=new_shadow.id,
            status="active",
        )
        db.add(new_share)
        await db.commit()

    async with session_factory() as db:
        stale_shadow = await db.get(Task, old_shadow_id)
        proxy = AsyncMock()
        broadcaster = SimpleNamespace(broadcast=AsyncMock())
        with patch("backend.services.shared_proxy.proxy_chat", proxy), patch(
            "backend.main.broadcaster",
            broadcaster,
        ), pytest.raises(HTTPException) as rejected:
            await _send_shared_chat(
                stale_shadow,
                ChatMessage(message="must not reach the new owner"),
                db,
            )

    assert rejected.value.status_code == 400
    proxy.assert_not_awaited()
    broadcaster.broadcast.assert_not_awaited()
