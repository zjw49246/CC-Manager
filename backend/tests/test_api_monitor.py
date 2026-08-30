"""Tests for Monitor API endpoints."""
import asyncio
import json
from datetime import datetime

import pytest
from fastapi import HTTPException
from starlette.requests import Request
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy import func, select, update

from backend.models.task import Task
from backend.models.monitor_session import MonitorSession, MonitorCheck
from backend.models.project import Project
from backend.models.sub_agent import SubAgentReport
from backend.tests.group_acl_test_helpers import (
    grant_group_project_access,
    revoke_group_membership_at_effect_fence,
)
from backend.tests.test_auth_ws_security import (
    _create_user,
    secured_client as secured_client,
)


def _admin_request() -> Request:
    request = Request({
        "type": "http",
        "method": "POST",
        "path": "/",
        "headers": [],
        "query_string": b"",
    })
    request.state.user_id = None
    request.state.user_role = "super_admin"
    request.state.auth_type = "none"
    return request


async def _create_task_with_monitor(client, session_factory, status="in_progress"):
    # monitor 是 claude-only（codex 任务显式 400），默认 provider 已是 codex，
    # 这里必须显式钉住 claude 才测得到 skill/limit 等后续分支
    resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
        "enabled_skills": {"monitor": True}, "provider": "claude",
    })
    task_id = resp.json()["id"]
    if status != "pending":
        async with session_factory() as db:
            await db.execute(
                update(Task).where(Task.id == task_id).values(status=status)
            )
            await db.commit()
    return task_id


async def _create_task_with_sub_agent(
    client,
    session_factory,
    status="in_progress",
):
    resp = await client.post("/api/tasks", json={
        "title": "T",
        "description": "d",
        "target_repo": "/tmp",
        "enabled_skills": {"sub-agent": True},
        "provider": "claude",
    })
    task_id = resp.json()["id"]
    if status != "pending":
        async with session_factory() as db:
            await db.execute(
                update(Task).where(Task.id == task_id).values(status=status)
            )
            await db.commit()
    return task_id


@pytest.mark.asyncio
async def test_create_monitor_session(client, session_factory):
    task_id = await _create_task_with_monitor(client, session_factory)

    mock_dispatcher = MagicMock()
    mock_dispatcher.start_monitor_session = MagicMock()
    mock_dispatcher.broadcaster = MagicMock()
    mock_dispatcher.broadcaster.broadcast = AsyncMock()

    with patch("backend.main.dispatcher", mock_dispatcher):
        resp = await client.post(f"/api/tasks/{task_id}/monitor-sessions", json={
            "description": "watch build",
            "interval": 60,
            "max_checks": 10,
        })

    assert resp.status_code == 200
    data = resp.json()
    assert data["description"] == "watch build"
    assert data["status"] == "running"
    assert data["interval"] == 60
    assert data["max_checks"] == 10
    assert data["task_id"] == task_id
    mock_dispatcher.start_monitor_session.assert_called_once()
    mock_dispatcher.broadcaster.broadcast.assert_awaited_once_with(
        f"task:{task_id}",
        {
            "event": "monitor_session_created",
            "monitor_session_id": data["id"],
            "description": "watch build",
            "task_retry_count": 0,
            "task_turn_generation": 0,
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("remote", [False, True], ids=["local", "worker"])
@pytest.mark.parametrize(
    ("agent_type", "path", "payload", "skill", "starter_name"),
    [
        (
            "monitor",
            "monitor-sessions",
            {"description": "must not start"},
            "monitor",
            "start_monitor_session",
        ),
        (
            "sub_agent",
            "sub-agent-sessions",
            {"name": "denied", "prompt": "must not start"},
            "sub-agent",
            "start_sub_agent_session",
        ),
    ],
)
async def test_auxiliary_create_rejects_group_revoked_at_effect_fence(
    secured_client,
    monkeypatch,
    remote,
    agent_type,
    path,
    payload,
    skill,
    starter_name,
):
    """Local rows and Worker POSTs share the final membership boundary."""

    client, session_factory = secured_client
    member_id, member_token = await _create_user(
        session_factory,
        email=f"{agent_type}-{remote}-effect@example.com",
        role="member",
    )
    async with session_factory() as db:
        project = Project(
            name=f"{agent_type}-{remote}-effect-project",
            status="ready",
        )
        db.add(project)
        await db.flush()
        task = Task(
            title=f"{agent_type} effect fence",
            description="group authority is revoked before admission",
            project_id=project.id,
            created_by=999,
            status="in_progress",
            provider="claude",
            worker_id=77 if remote else None,
            enabled_skills={skill: True},
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
    fence = revoke_group_membership_at_effect_fence(monkeypatch)
    proxy = MagicMock()
    proxy.proxy_to_worker = AsyncMock(return_value={"proxied": True})
    dispatcher = MagicMock()
    setattr(dispatcher, starter_name, MagicMock())
    dispatcher.broadcaster.broadcast = AsyncMock()

    with (
        patch("backend.main.worker_proxy", proxy),
        patch("backend.main.dispatcher", dispatcher),
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/{path}",
            headers={"Authorization": f"Bearer {member_token}"},
            json=payload,
        )

    assert response.status_code == 403, response.text
    assert fence == {"calls": 1, "revoked": True}
    proxy.proxy_to_worker.assert_not_awaited()
    getattr(dispatcher, starter_name).assert_not_called()
    dispatcher.broadcaster.broadcast.assert_not_awaited()
    async with session_factory() as db:
        assert await db.scalar(
            select(func.count(MonitorSession.id)).where(
                MonitorSession.task_id == task_id,
                MonitorSession.agent_type == agent_type,
            )
        ) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("remote", [False, True], ids=["local", "worker"])
@pytest.mark.parametrize(
    ("agent_type", "path", "stopper"),
    [
        ("monitor", "monitor-sessions", "stop_monitor_session_process"),
        (
            "sub_agent",
            "sub-agent-sessions",
            "stop_sub_agent_session_process",
        ),
    ],
)
async def test_auxiliary_delete_rejects_group_revoked_at_effect_fence(
    secured_client,
    monkeypatch,
    remote,
    agent_type,
    path,
    stopper,
):
    """Revocation that wins the final Task fence prevents every stop effect."""

    client, session_factory = secured_client
    member_id, member_token = await _create_user(
        session_factory,
        email=f"delete-{agent_type}-{remote}-effect@example.com",
        role="member",
    )
    async with session_factory() as db:
        project = Project(
            name=f"delete-{agent_type}-{remote}-project",
            status="ready",
        )
        db.add(project)
        await db.flush()
        task = Task(
            title="delete effect fence",
            description="revocation must win before child stop",
            project_id=project.id,
            created_by=999,
            status="in_progress",
            provider="claude",
            worker_id=88 if remote else None,
        )
        db.add(task)
        await db.flush()
        remote_id = 9031 if remote else None
        meta = None
        if remote and agent_type == "sub_agent":
            meta = json.dumps({
                "ccm_worker_mirror": {
                    "worker_id": task.worker_id,
                    "task_incarnation_id": task.incarnation_id,
                    "remote_id": remote_id,
                }
            })
        session = MonitorSession(
            task_id=task.id,
            remote_id=remote_id,
            agent_type=agent_type,
            source="ccm",
            description="must remain running",
            status="running",
            meta=meta,
        )
        db.add(session)
        await db.commit()
        project_id = project.id
        task_id = task.id
        session_id = session.id
    await grant_group_project_access(
        session_factory,
        project_id=project_id,
        user_id=member_id,
    )
    fence = revoke_group_membership_at_effect_fence(monkeypatch)
    proxy = MagicMock()
    proxy.proxy_to_worker = AsyncMock(return_value={"ok": True})
    dispatcher = MagicMock()
    dispatcher.stop_monitor_session_process = AsyncMock()
    dispatcher.stop_sub_agent_session_process = AsyncMock()
    dispatcher.broadcaster.broadcast = AsyncMock()

    with (
        patch("backend.main.worker_proxy", proxy),
        patch("backend.main.dispatcher", dispatcher),
    ):
        response = await client.delete(
            f"/api/tasks/{task_id}/{path}/{session_id}",
            headers={"Authorization": f"Bearer {member_token}"},
        )

    assert response.status_code == 403, response.text
    assert fence == {"calls": 1, "revoked": True}
    proxy.proxy_to_worker.assert_not_awaited()
    getattr(dispatcher, stopper).assert_not_awaited()
    dispatcher.broadcaster.broadcast.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(MonitorSession, session_id)
    assert current.status == "running"


@pytest.mark.asyncio
async def test_cancelled_monitor_create_still_admits_committed_row(
    client,
    session_factory,
):
    from backend.api.monitor import create_monitor_session
    from backend.schemas.monitor_session import MonitorSessionCreate

    task_id = await _create_task_with_monitor(client, session_factory)
    commit_started = asyncio.Event()
    release_commit = asyncio.Event()
    dispatcher = MagicMock()
    dispatcher.start_monitor_session = MagicMock()
    dispatcher.broadcaster.broadcast = AsyncMock()

    async with session_factory() as db:
        original_commit = db.commit

        async def blocked_commit():
            commit_started.set()
            await release_commit.wait()
            await original_commit()

        with (
            patch.object(db, "commit", side_effect=blocked_commit),
            patch("backend.main.dispatcher", dispatcher),
        ):
            request_task = asyncio.create_task(
                create_monitor_session(
                    task_id,
                    MonitorSessionCreate(description="cancel-window"),
                    _admin_request(),
                    db,
                )
            )
            await commit_started.wait()
            request_task.cancel()
            release_commit.set()
            with pytest.raises(asyncio.CancelledError):
                await request_task

    dispatcher.start_monitor_session.assert_called_once()
    async with session_factory() as db:
        rows = list(
            (
                await db.execute(
                    select(MonitorSession).where(
                        MonitorSession.task_id == task_id,
                        MonitorSession.description == "cancel-window",
                    )
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 1
    assert rows[0].status == "running"


@pytest.mark.asyncio
async def test_cancelled_sub_agent_create_still_admits_committed_row(
    client,
    session_factory,
):
    from backend.api.sub_agent_tasks import (
        SubAgentSessionCreate,
        create_sub_agent_session,
    )

    task_id = await _create_task_with_sub_agent(client, session_factory)
    commit_started = asyncio.Event()
    release_commit = asyncio.Event()
    dispatcher = MagicMock()
    dispatcher.start_sub_agent_session = MagicMock()
    dispatcher.broadcaster.broadcast = AsyncMock()

    async with session_factory() as db:
        original_commit = db.commit

        async def blocked_commit():
            commit_started.set()
            await release_commit.wait()
            await original_commit()

        with (
            patch.object(db, "commit", side_effect=blocked_commit),
            patch("backend.main.dispatcher", dispatcher),
        ):
            request_task = asyncio.create_task(
                create_sub_agent_session(
                    task_id,
                    SubAgentSessionCreate(
                        name="cancel-window",
                        prompt="work",
                    ),
                    _admin_request(),
                    db,
                )
            )
            await commit_started.wait()
            request_task.cancel()
            release_commit.set()
            with pytest.raises(asyncio.CancelledError):
                await request_task

    dispatcher.start_sub_agent_session.assert_called_once()
    async with session_factory() as db:
        rows = list(
            (
                await db.execute(
                    select(MonitorSession).where(
                        MonitorSession.task_id == task_id,
                        MonitorSession.agent_type == "sub_agent",
                        MonitorSession.description == "cancel-window",
                    )
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 1
    assert rows[0].status == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "path", "payload", "starter_name"),
    (
        (
            "monitor",
            "monitor-sessions",
            {"description": "shutdown-race"},
            "start_monitor_session",
        ),
        (
            "sub_agent",
            "sub-agent-sessions",
            {"name": "shutdown-race", "prompt": "work"},
            "start_sub_agent_session",
        ),
    ),
)
async def test_failed_auxiliary_admission_marks_committed_row_failed(
    client,
    session_factory,
    kind,
    path,
    payload,
    starter_name,
):
    if kind == "monitor":
        task_id = await _create_task_with_monitor(client, session_factory)
    else:
        task_id = await _create_task_with_sub_agent(client, session_factory)

    dispatcher = MagicMock()
    setattr(
        dispatcher,
        starter_name,
        MagicMock(side_effect=RuntimeError("shutdown admission closed")),
    )
    dispatcher.broadcaster.broadcast = AsyncMock()
    with patch("backend.main.dispatcher", dispatcher):
        response = await client.post(f"/api/tasks/{task_id}/{path}", json=payload)

    assert response.status_code == 503
    async with session_factory() as db:
        row = await db.scalar(
            select(MonitorSession).where(
                MonitorSession.task_id == task_id,
                MonitorSession.agent_type == kind,
                MonitorSession.description == "shutdown-race",
            )
        )
    assert row is not None
    assert row.status == "failed"
    assert row.completed_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "path", "payload", "starter_name"),
    (
        (
            "monitor",
            "monitor-sessions",
            {"description": "late-worker-monitor"},
            "start_monitor_session",
        ),
        (
            "sub_agent",
            "sub-agent-sessions",
            {"name": "late-worker-child", "prompt": "work"},
            "start_sub_agent_session",
        ),
    ),
)
async def test_worker_drain_refuses_new_auxiliary_admission(
    client,
    session_factory,
    worker_control_plane_auth,
    monkeypatch,
    kind,
    path,
    payload,
    starter_name,
):
    """The drain claim must win before the Task/session admission writer."""

    from backend.config import settings
    from backend.services.worker_node_control import begin_worker_node_drain

    if kind == "monitor":
        task_id = await _create_task_with_monitor(client, session_factory)
    else:
        task_id = await _create_task_with_sub_agent(client, session_factory)

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    async with session_factory() as db:
        await begin_worker_node_drain(db, claim="d" * 64)
        await db.commit()

    dispatcher = MagicMock()
    setattr(dispatcher, starter_name, MagicMock())
    dispatcher.broadcaster.broadcast = AsyncMock()
    with patch("backend.main.dispatcher", dispatcher):
        response = await client.post(
            f"/api/tasks/{task_id}/{path}",
            json=payload,
        )

    assert response.status_code == 409
    getattr(dispatcher, starter_name).assert_not_called()
    dispatcher.broadcaster.broadcast.assert_not_awaited()
    async with session_factory() as db:
        rows = list(
            (
                await db.execute(
                    select(MonitorSession).where(
                        MonitorSession.task_id == task_id,
                        MonitorSession.agent_type == kind,
                    )
                )
            )
            .scalars()
            .all()
        )
    assert rows == []


@pytest.mark.asyncio
async def test_create_monitor_no_skill(client, session_factory):
    resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
        "provider": "claude",
    })
    task_id = resp.json()["id"]
    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == task_id).values(status="in_progress"))
        await db.commit()

    resp = await client.post(f"/api/tasks/{task_id}/monitor-sessions", json={
        "description": "test",
    })
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_create_monitor_task_not_found(client):
    resp = await client.post("/api/tasks/9999/monitor-sessions", json={
        "description": "test",
    })
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_create_monitor_task_completed(client, session_factory):
    task_id = await _create_task_with_monitor(client, session_factory, status="completed")

    resp = await client.post(f"/api/tasks/{task_id}/monitor-sessions", json={
        "description": "test",
    })
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_create_monitor_concurrency_limit(client, session_factory):
    task_id = await _create_task_with_monitor(client, session_factory)

    async with session_factory() as db:
        for i in range(5):
            db.add(MonitorSession(task_id=task_id, description=f"m{i}", status="running"))
        await db.commit()

    resp = await client.post(f"/api/tasks/{task_id}/monitor-sessions", json={
        "description": "one too many",
    })
    assert resp.status_code == 429


@pytest.mark.asyncio
async def test_concurrent_monitor_admission_never_exceeds_sqlite_cap(
    client,
    session_factory,
):
    task_id = await _create_task_with_monitor(client, session_factory)
    async with session_factory() as db:
        db.add_all([
            MonitorSession(
                task_id=task_id,
                agent_type="monitor",
                source="ccm",
                description=f"existing-{index}",
                status="running",
            )
            for index in range(4)
        ])
        await db.commit()

    dispatcher = MagicMock()
    dispatcher.start_monitor_session = MagicMock()
    dispatcher.broadcaster.broadcast = AsyncMock()
    with patch("backend.main.dispatcher", dispatcher):
        responses = await asyncio.gather(
            client.post(
                f"/api/tasks/{task_id}/monitor-sessions",
                json={"description": "candidate-a"},
            ),
            client.post(
                f"/api/tasks/{task_id}/monitor-sessions",
                json={"description": "candidate-b"},
            ),
        )

    assert sorted(response.status_code for response in responses) == [200, 429]
    async with session_factory() as db:
        count = await db.scalar(
            select(func.count(MonitorSession.id)).where(
                MonitorSession.task_id == task_id,
                MonitorSession.agent_type == "monitor",
                MonitorSession.source == "ccm",
                MonitorSession.status == "running",
            )
        )
    assert count == 5
    assert dispatcher.start_monitor_session.call_count == 1


@pytest.mark.asyncio
async def test_concurrent_sub_agent_admission_never_exceeds_sqlite_cap(
    client,
    session_factory,
):
    task_id = await _create_task_with_sub_agent(client, session_factory)
    async with session_factory() as db:
        db.add_all([
            MonitorSession(
                task_id=task_id,
                agent_type="sub_agent",
                source="ccm",
                description=f"existing-{index}",
                status="running",
            )
            for index in range(2)
        ])
        await db.commit()

    dispatcher = MagicMock()
    dispatcher.start_sub_agent_session = MagicMock()
    dispatcher.broadcaster.broadcast = AsyncMock()
    with patch("backend.main.dispatcher", dispatcher):
        responses = await asyncio.gather(
            client.post(
                f"/api/tasks/{task_id}/sub-agent-sessions",
                json={"name": "candidate-a", "prompt": "a"},
            ),
            client.post(
                f"/api/tasks/{task_id}/sub-agent-sessions",
                json={"name": "candidate-b", "prompt": "b"},
            ),
        )

    assert sorted(response.status_code for response in responses) == [201, 429]
    async with session_factory() as db:
        count = await db.scalar(
            select(func.count(MonitorSession.id)).where(
                MonitorSession.task_id == task_id,
                MonitorSession.agent_type == "sub_agent",
                MonitorSession.source == "ccm",
                MonitorSession.status == "running",
            )
        )
    assert count == 3
    assert dispatcher.start_sub_agent_session.call_count == 1


@pytest.mark.asyncio
async def test_worker_sub_agent_create_is_proxied_without_local_start(
    client,
    session_factory,
):
    from backend.api.sub_agent_tasks import (
        SubAgentSessionCreate,
        create_sub_agent_session,
    )

    task_id = await _create_task_with_sub_agent(client, session_factory)
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            # Routing belongs to the Worker before the Manager applies its
            # local claude-only gate; the Worker validates its own provider.
            .values(worker_id=77, provider="codex")
        )
        await db.commit()

    proxy = MagicMock()
    proxy.proxy_to_worker = AsyncMock(return_value={"proxied": True})
    dispatcher = MagicMock()
    dispatcher.start_sub_agent_session = MagicMock()
    async with session_factory() as db:
        with (
            patch("backend.main.worker_proxy", proxy),
            patch("backend.main.dispatcher", dispatcher),
        ):
            result = await create_sub_agent_session(
                task_id,
                SubAgentSessionCreate(name="remote", prompt="work"),
                _admin_request(),
                db,
            )

    assert result == {"proxied": True}
    proxy.proxy_to_worker.assert_awaited_once()
    proxied_task, method, path = proxy.proxy_to_worker.call_args.args
    assert proxied_task.id == task_id
    assert proxied_task.worker_id == 77
    assert method == "POST"
    assert path == f"/api/tasks/{task_id}/sub-agent-sessions"
    dispatcher.start_sub_agent_session.assert_not_called()


@pytest.mark.asyncio
async def test_worker_sub_agent_manager_mirror_list_get_and_exact_delete(
    client,
    session_factory,
):
    task_id = await _create_task_with_sub_agent(client, session_factory)
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.worker_id = 77
        await db.flush()
        remote_id = 9011
        historical = MonitorSession(
            task_id=task_id,
            remote_id=remote_id,
            agent_type="sub_agent",
            source="ccm",
            description="historical remote child mirror",
            status="completed",
            meta=json.dumps({
                "ccm_worker_mirror": {
                    "worker_id": 76,
                    "task_incarnation_id": task.incarnation_id,
                    "remote_id": remote_id,
                }
            }),
        )
        mirror = MonitorSession(
            task_id=task_id,
            remote_id=remote_id,
            agent_type="sub_agent",
            source="ccm",
            description="remote child mirror",
            status="running",
            meta=json.dumps({
                "ccm_worker_mirror": {
                    "worker_id": task.worker_id,
                    "task_incarnation_id": task.incarnation_id,
                    "remote_id": remote_id,
                }
            }),
        )
        db.add_all([historical, mirror])
        await db.commit()
        historical_id = historical.id
        local_id = mirror.id
        incarnation_id = task.incarnation_id

    listed = await client.get(f"/api/tasks/{task_id}/sub-agent-sessions")
    historical_fetched = await client.get(
        f"/api/tasks/{task_id}/sub-agent-sessions/{historical_id}"
    )
    fetched = await client.get(
        f"/api/tasks/{task_id}/sub-agent-sessions/{local_id}"
    )
    assert listed.status_code == 200
    assert {row["id"] for row in listed.json()} == {historical_id, local_id}
    assert historical_fetched.status_code == 200
    assert historical_fetched.json()["id"] == historical_id
    assert fetched.status_code == 200
    assert fetched.json()["id"] == local_id

    proxy = MagicMock()
    proxy.proxy_to_worker = AsyncMock(return_value={"ok": True})
    with patch("backend.main.worker_proxy", proxy):
        stale_deleted = await client.delete(
            f"/api/tasks/{task_id}/sub-agent-sessions/{historical_id}"
        )
        deleted = await client.delete(
            f"/api/tasks/{task_id}/sub-agent-sessions/{local_id}"
        )

    assert stale_deleted.status_code == 409
    assert "mirror identity" in stale_deleted.json()["detail"]
    assert deleted.status_code == 200, deleted.text
    proxied_task, method, path = proxy.proxy_to_worker.call_args.args
    assert proxied_task.id == task_id
    assert proxied_task.worker_id == 77
    assert proxied_task.incarnation_id == incarnation_id
    assert method == "DELETE"
    assert path == f"/api/tasks/{task_id}/sub-agent-sessions/{remote_id}"
    assert proxy.proxy_to_worker.call_args.kwargs == {
        "require_task_incarnation_fence": True,
    }
    async with session_factory() as db:
        current = await db.get(MonitorSession, local_id)
    assert current.status == "stopped"
    assert current.completed_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity_kind",
    ["missing", "malformed", "worker", "incarnation", "remote"],
)
async def test_worker_sub_agent_delete_rejects_untrusted_mirror_identity(
    client,
    session_factory,
    identity_kind,
):
    task_id = await _create_task_with_sub_agent(client, session_factory)
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.worker_id = 78
        await db.flush()
        remote_id = 9021
        identity = {
            "worker_id": task.worker_id,
            "task_incarnation_id": task.incarnation_id,
            "remote_id": remote_id,
        }
        if identity_kind == "missing":
            meta = None
        elif identity_kind == "malformed":
            meta = "not-json"
        else:
            if identity_kind == "worker":
                identity["worker_id"] += 1
            elif identity_kind == "incarnation":
                identity["task_incarnation_id"] = "f" * 32
            else:
                identity["remote_id"] += 1
            meta = json.dumps({"ccm_worker_mirror": identity})
        mirror = MonitorSession(
            task_id=task_id,
            remote_id=remote_id,
            agent_type="sub_agent",
            source="ccm",
            description="untrusted mirror",
            status="running",
            meta=meta,
        )
        db.add(mirror)
        await db.commit()
        local_id = mirror.id

    proxy = MagicMock()
    proxy.proxy_to_worker = AsyncMock(return_value={"ok": True})
    with patch("backend.main.worker_proxy", proxy):
        response = await client.delete(
            f"/api/tasks/{task_id}/sub-agent-sessions/{local_id}"
        )

    assert response.status_code == 409
    assert "mirror identity" in response.json()["detail"]
    proxy.proxy_to_worker.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(MonitorSession, local_id)
    assert current.status == "running"


@pytest.mark.asyncio
async def test_worker_monitor_history_uses_local_ids_and_only_current_deletes(
    client,
    session_factory,
):
    task_id = await _create_task_with_monitor(client, session_factory)
    remote_id = 9041
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.worker_id = 82
        await db.flush()
        historical = MonitorSession(
            task_id=task_id,
            remote_id=remote_id,
            agent_type="monitor",
            source="ccm",
            description="historical Worker monitor",
            status="completed",
            meta=json.dumps({
                "ccm_worker_mirror": {
                    "worker_id": 81,
                    "task_incarnation_id": task.incarnation_id,
                    "remote_id": remote_id,
                }
            }),
        )
        current = MonitorSession(
            task_id=task_id,
            remote_id=remote_id,
            agent_type="monitor",
            source="ccm",
            description="current Worker monitor",
            status="running",
            meta=json.dumps({
                "ccm_worker_mirror": {
                    "worker_id": task.worker_id,
                    "task_incarnation_id": task.incarnation_id,
                    "remote_id": remote_id,
                }
            }),
        )
        db.add_all([historical, current])
        await db.commit()
        historical_id = historical.id
        current_id = current.id
        incarnation_id = task.incarnation_id

    listed = await client.get(f"/api/tasks/{task_id}/monitor-sessions")
    historical_get = await client.get(
        f"/api/tasks/{task_id}/monitor-sessions/{historical_id}"
    )
    current_get = await client.get(
        f"/api/tasks/{task_id}/monitor-sessions/{current_id}"
    )
    assert listed.status_code == 200
    assert {row["id"] for row in listed.json()} == {
        historical_id,
        current_id,
    }
    assert historical_get.status_code == 200
    assert historical_get.json()["id"] == historical_id
    assert current_get.status_code == 200
    assert current_get.json()["id"] == current_id

    proxy = MagicMock()
    proxy.proxy_to_worker = AsyncMock(return_value={"ok": True})
    with patch("backend.main.worker_proxy", proxy):
        stale_delete = await client.delete(
            f"/api/tasks/{task_id}/monitor-sessions/{historical_id}"
        )
        current_delete = await client.delete(
            f"/api/tasks/{task_id}/monitor-sessions/{current_id}"
        )

    assert stale_delete.status_code == 409
    assert "mirror identity" in stale_delete.json()["detail"]
    assert current_delete.status_code == 200, current_delete.text
    proxy.proxy_to_worker.assert_awaited_once()
    proxied_task, method, path = proxy.proxy_to_worker.call_args.args
    assert proxied_task.worker_id == 82
    assert proxied_task.incarnation_id == incarnation_id
    assert method == "DELETE"
    assert path == f"/api/tasks/{task_id}/monitor-sessions/{remote_id}"
    assert proxy.proxy_to_worker.call_args.kwargs == {
        "require_task_incarnation_fence": True,
    }
    async with session_factory() as db:
        historical = await db.get(MonitorSession, historical_id)
        current = await db.get(MonitorSession, current_id)
    assert historical.status == "completed"
    assert current.status == "cancelled"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity_kind",
    ["missing", "malformed", "worker", "incarnation", "remote"],
)
async def test_worker_monitor_delete_rejects_untrusted_mirror_identity(
    client,
    session_factory,
    identity_kind,
):
    task_id = await _create_task_with_monitor(client, session_factory)
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.worker_id = 83
        await db.flush()
        remote_id = 9042
        identity = {
            "worker_id": task.worker_id,
            "task_incarnation_id": task.incarnation_id,
            "remote_id": remote_id,
        }
        if identity_kind == "missing":
            meta = None
        elif identity_kind == "malformed":
            meta = "not-json"
        else:
            if identity_kind == "worker":
                identity["worker_id"] += 1
            elif identity_kind == "incarnation":
                identity["task_incarnation_id"] = "f" * 32
            else:
                identity["remote_id"] += 1
            meta = json.dumps({"ccm_worker_mirror": identity})
        mirror = MonitorSession(
            task_id=task_id,
            remote_id=remote_id,
            agent_type="monitor",
            source="ccm",
            description="untrusted monitor mirror",
            status="running",
            meta=meta,
        )
        db.add(mirror)
        await db.commit()
        local_id = mirror.id

    proxy = MagicMock()
    proxy.proxy_to_worker = AsyncMock(return_value={"ok": True})
    with patch("backend.main.worker_proxy", proxy):
        response = await client.delete(
            f"/api/tasks/{task_id}/monitor-sessions/{local_id}"
        )

    assert response.status_code == 409
    assert "mirror identity" in response.json()["detail"]
    proxy.proxy_to_worker.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(MonitorSession, local_id)
    assert current.status == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("agent_type", "path", "payload"),
    [
        (
            "monitor",
            "monitor-sessions",
            {"description": "must wait for migration"},
        ),
        (
            "sub_agent",
            "sub-agent-sessions",
            {"name": "blocked", "prompt": "must wait for migration"},
        ),
    ],
)
async def test_worker_auxiliary_create_rejects_active_migration_before_proxy(
    client,
    session_factory,
    agent_type,
    path,
    payload,
):
    task_id = await (
        _create_task_with_monitor(client, session_factory)
        if agent_type == "monitor"
        else _create_task_with_sub_agent(client, session_factory)
    )
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(worker_id=84, status="migrating")
        )
        await db.commit()

    proxy = MagicMock()
    proxy.proxy_to_worker = AsyncMock(return_value={"unexpected": True})
    with patch("backend.main.worker_proxy", proxy):
        response = await client.post(
            f"/api/tasks/{task_id}/{path}",
            json=payload,
        )

    assert response.status_code == 409, response.text
    assert "migration is active" in response.json()["detail"]
    proxy.proxy_to_worker.assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_codex_monitor_create_rejects_before_proxy(
    client,
    session_factory,
    monkeypatch,
):
    from backend.config import settings
    from backend.api.monitor import create_monitor_session
    from backend.schemas.monitor_session import MonitorSessionCreate

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    task_id = await _create_task_with_monitor(client, session_factory)
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(worker_id=79, provider="codex")
        )
        await db.commit()

    proxy = MagicMock()
    proxy.proxy_to_worker = AsyncMock(return_value={"proxied": True})
    dispatcher = MagicMock()
    dispatcher.start_monitor_session = MagicMock()
    async with session_factory() as db:
        with (
            patch("backend.main.worker_proxy", proxy),
            patch("backend.main.dispatcher", dispatcher),
        ):
            with pytest.raises(HTTPException) as exc:
                await create_monitor_session(
                    task_id,
                    MonitorSessionCreate(description="remote monitor"),
                    _admin_request(),
                    db,
                )

    assert exc.value.status_code == 400
    assert "local, non-shared" in exc.value.detail
    proxy.proxy_to_worker.assert_not_awaited()
    dispatcher.start_monitor_session.assert_not_called()


@pytest.mark.asyncio
async def test_worker_claude_monitor_create_still_routes_to_worker(
    client,
    session_factory,
):
    from backend.api.monitor import create_monitor_session
    from backend.schemas.monitor_session import MonitorSessionCreate

    task_id = await _create_task_with_monitor(client, session_factory)
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(worker_id=80, provider="claude")
        )
        await db.commit()

    proxy = MagicMock()
    proxy.proxy_to_worker = AsyncMock(return_value={"proxied": True})
    dispatcher = MagicMock()
    dispatcher.start_monitor_session = MagicMock()
    async with session_factory() as db:
        with (
            patch("backend.main.worker_proxy", proxy),
            patch("backend.main.dispatcher", dispatcher),
        ):
            result = await create_monitor_session(
                task_id,
                MonitorSessionCreate(description="remote Claude monitor"),
                _admin_request(),
                db,
            )

    assert result == {"proxied": True}
    proxy.proxy_to_worker.assert_awaited_once()
    dispatcher.start_monitor_session.assert_not_called()


@pytest.mark.asyncio
async def test_codex_monitor_migration_race_rejects_without_proxy_or_row(
    client,
    session_factory,
    monkeypatch,
):
    from backend.api.monitor import create_monitor_session
    from backend.config import settings
    from backend.schemas.monitor_session import MonitorSessionCreate

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    response = await client.post("/api/tasks", json={
        "title": "Codex monitor migration race",
        "description": "d",
        "provider": "codex",
        "enabled_skills": {"monitor": True},
    })
    assert response.status_code == 201, response.text
    task_id = response.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="in_progress")
        )
        await db.commit()

    proxy = MagicMock()
    proxy.proxy_to_worker = AsyncMock(return_value={"proxied": True})
    dispatcher = MagicMock()
    dispatcher.start_monitor_session = MagicMock()
    async with session_factory() as db:
        original_rollback = db.rollback
        rollback_count = 0

        async def migrate_after_routing_read():
            nonlocal rollback_count
            rollback_count += 1
            await original_rollback()
            if rollback_count == 1:
                async with session_factory() as migration_db:
                    await migration_db.execute(
                        update(Task)
                        .where(Task.id == task_id)
                        .values(worker_id=88)
                    )
                    await migration_db.commit()

        with (
            patch.object(db, "rollback", side_effect=migrate_after_routing_read),
            patch("backend.main.worker_proxy", proxy),
            patch("backend.main.dispatcher", dispatcher),
        ):
            with pytest.raises(HTTPException) as exc:
                await create_monitor_session(
                    task_id,
                    MonitorSessionCreate(description="must stay local"),
                    _admin_request(),
                    db,
                )

    assert exc.value.status_code == 400
    proxy.proxy_to_worker.assert_not_awaited()
    dispatcher.start_monitor_session.assert_not_called()
    async with session_factory() as db:
        row_count = await db.scalar(
            select(func.count(MonitorSession.id)).where(
                MonitorSession.task_id == task_id,
            )
        )
    assert row_count == 0


@pytest.mark.asyncio
async def test_sub_agent_create_routes_migration_before_local_write_guard(
    client,
    session_factory,
):
    from backend.api.sub_agent_tasks import (
        SubAgentSessionCreate,
        create_sub_agent_session,
    )

    task_id = await _create_task_with_sub_agent(client, session_factory)
    proxy = MagicMock()
    proxy.proxy_to_worker = AsyncMock(return_value={"proxied": True})
    dispatcher = MagicMock()
    dispatcher.start_sub_agent_session = MagicMock()

    async with session_factory() as db:
        original_rollback = db.rollback
        rollback_count = 0

        async def migrate_after_routing_read():
            nonlocal rollback_count
            rollback_count += 1
            await original_rollback()
            if rollback_count == 1:
                async with session_factory() as migration_db:
                    await migration_db.execute(
                        update(Task)
                        .where(Task.id == task_id)
                        .values(worker_id=88)
                    )
                    await migration_db.commit()

        with (
            patch.object(db, "rollback", side_effect=migrate_after_routing_read),
            patch("backend.main.worker_proxy", proxy),
            patch("backend.main.dispatcher", dispatcher),
        ):
            result = await create_sub_agent_session(
                task_id,
                SubAgentSessionCreate(name="migrated", prompt="work"),
                _admin_request(),
                db,
            )

    assert result == {"proxied": True}
    proxy.proxy_to_worker.assert_awaited_once()
    proxied_task = proxy.proxy_to_worker.call_args.args[0]
    assert proxied_task.worker_id == 88
    dispatcher.start_sub_agent_session.assert_not_called()
    async with session_factory() as db:
        local_count = await db.scalar(
            select(func.count(MonitorSession.id)).where(
                MonitorSession.task_id == task_id,
                MonitorSession.agent_type == "sub_agent",
            )
        )
    assert local_count == 0


@pytest.mark.asyncio
async def test_list_monitor_sessions(client, session_factory):
    task_id = await _create_task_with_monitor(client, session_factory)

    async with session_factory() as db:
        db.add(MonitorSession(task_id=task_id, description="m1"))
        db.add(MonitorSession(task_id=task_id, description="m2"))
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}/monitor-sessions")
    assert resp.status_code == 200
    assert len(resp.json()) == 2


@pytest.mark.asyncio
async def test_monitor_and_sub_agent_routes_do_not_cross_categories(
    client,
    session_factory,
):
    task_id = await _create_task_with_sub_agent(client, session_factory)
    async with session_factory() as db:
        monitor = MonitorSession(
            task_id=task_id,
            agent_type="monitor",
            source="ccm",
            description="monitor-only",
        )
        sub_agent = MonitorSession(
            task_id=task_id,
            agent_type="sub_agent",
            source="ccm",
            description="sub-agent-only",
        )
        db.add_all([monitor, sub_agent])
        await db.commit()
        monitor_id = monitor.id
        sub_agent_id = sub_agent.id

    monitors = await client.get(f"/api/tasks/{task_id}/monitor-sessions")
    sub_agents = await client.get(f"/api/tasks/{task_id}/sub-agent-sessions")
    assert [row["id"] for row in monitors.json()] == [monitor_id]
    assert [row["id"] for row in sub_agents.json()] == [sub_agent_id]
    assert (
        await client.get(
            f"/api/tasks/{task_id}/monitor-sessions/{sub_agent_id}"
        )
    ).status_code == 404
    assert (
        await client.get(
            f"/api/tasks/{task_id}/sub-agent-sessions/{monitor_id}"
        )
    ).status_code == 404


@pytest.mark.asyncio
async def test_get_monitor_session(client, session_factory):
    task_id = await _create_task_with_monitor(client, session_factory)

    async with session_factory() as db:
        ms = MonitorSession(task_id=task_id, description="get-test")
        db.add(ms)
        await db.commit()
        await db.refresh(ms)
        ms_id = ms.id

    resp = await client.get(f"/api/tasks/{task_id}/monitor-sessions/{ms_id}")
    assert resp.status_code == 200
    assert resp.json()["description"] == "get-test"


@pytest.mark.asyncio
async def test_get_monitor_session_not_found(client, session_factory):
    task_id = await _create_task_with_monitor(client, session_factory)
    resp = await client.get(f"/api/tasks/{task_id}/monitor-sessions/9999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_monitor_session(client, session_factory):
    task_id = await _create_task_with_monitor(client, session_factory)

    async with session_factory() as db:
        ms = MonitorSession(task_id=task_id, description="del-test", status="running")
        db.add(ms)
        await db.commit()
        await db.refresh(ms)
        ms_id = ms.id

    mock_dispatcher = MagicMock()
    mock_dispatcher._monitor_tasks = {}
    mock_dispatcher._monitor_processes = {}
    mock_dispatcher.stop_monitor_session_process = AsyncMock()
    mock_dispatcher.broadcaster = MagicMock()
    mock_dispatcher.broadcaster.broadcast = AsyncMock()

    with patch("backend.main.dispatcher", mock_dispatcher):
        resp = await client.delete(f"/api/tasks/{task_id}/monitor-sessions/{ms_id}")

    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    mock_dispatcher.stop_monitor_session_process.assert_awaited_once_with(
        ms_id,
        terminal=True,
    )
    mock_dispatcher.broadcaster.broadcast.assert_awaited_once_with(
        f"task:{task_id}",
        {
            "event": "monitor_session_status",
            "monitor_session_id": ms_id,
            "status": "cancelled",
            "task_retry_count": 0,
            "task_turn_generation": 0,
        },
    )

    async with session_factory() as db:
        ms = await db.get(MonitorSession, ms_id)
        assert ms.status == "cancelled"
        assert ms.completed_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("agent_type", "path", "stopper"),
    [
        ("monitor", "monitor-sessions", "stop_monitor_session_process"),
        (
            "sub_agent",
            "sub-agent-sessions",
            "stop_sub_agent_session_process",
        ),
    ],
)
async def test_auxiliary_delete_yields_to_active_worker_termination_receipt(
    client,
    session_factory,
    agent_type,
    path,
    stopper,
):
    from backend.tests.worker_termination_helpers import (
        persist_active_worker_receipt,
    )

    task_id = (
        await _create_task_with_monitor(client, session_factory)
        if agent_type == "monitor"
        else await _create_task_with_sub_agent(client, session_factory)
    )
    async with session_factory() as db:
        session = MonitorSession(
            task_id=task_id,
            agent_type=agent_type,
            source="ccm",
            description="receipt-owned child",
            status="running",
        )
        db.add(session)
        await db.commit()
        session_id = session.id
    await persist_active_worker_receipt(session_factory, task_id)

    dispatcher = MagicMock()
    dispatcher.stop_monitor_session_process = AsyncMock()
    dispatcher.stop_sub_agent_session_process = AsyncMock()
    dispatcher.broadcaster.broadcast = AsyncMock()
    with patch("backend.main.dispatcher", dispatcher):
        response = await client.delete(
            f"/api/tasks/{task_id}/{path}/{session_id}"
        )

    assert response.status_code == 409
    assert "active Worker termination receipt" in response.json()["detail"]
    getattr(dispatcher, stopper).assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(MonitorSession, session_id)
    assert current.status == "running"


@pytest.mark.asyncio
async def test_delete_monitor_session_reports_unconfirmed_runtime_cleanup(
    client,
    session_factory,
):
    """A terminal row must not disguise an unconfirmed Codex thread delete."""

    resp = await client.post("/api/tasks", json={
        "title": "Codex monitor cleanup",
        "description": "d",
        "target_repo": "/tmp",
        "provider": "codex",
    })
    task_id = resp.json()["id"]

    async with session_factory() as db:
        ms = MonitorSession(
            task_id=task_id,
            description="cleanup-pending",
            status="running",
            codex_thread_id="thread-cleanup-pending",
            codex_home="/tmp/codex-home",
            codex_cleanup_pending=True,
        )
        db.add(ms)
        await db.commit()
        await db.refresh(ms)
        ms_id = ms.id

    mock_dispatcher = MagicMock()
    mock_dispatcher.stop_monitor_session_process = AsyncMock(
        side_effect=RuntimeError("terminal thread cleanup remains pending")
    )
    mock_dispatcher.broadcaster = MagicMock()
    mock_dispatcher.broadcaster.broadcast = AsyncMock()

    with patch("backend.main.dispatcher", mock_dispatcher):
        response = await client.delete(
            f"/api/tasks/{task_id}/monitor-sessions/{ms_id}"
        )

    assert response.status_code == 409
    assert "runtime cleanup could not be confirmed" in response.json()["detail"]
    mock_dispatcher.stop_monitor_session_process.assert_awaited_once_with(
        ms_id,
        terminal=True,
    )
    mock_dispatcher.broadcaster.broadcast.assert_not_awaited()

    async with session_factory() as db:
        ms = await db.get(MonitorSession, ms_id)
        assert ms.status == "cancelled"
        assert ms.completed_at is not None
        assert ms.codex_thread_id == "thread-cleanup-pending"
        assert ms.codex_home == "/tmp/codex-home"
        assert ms.codex_cleanup_pending is True

    async def complete_retry(session_id, *, terminal):
        assert session_id == ms_id
        assert terminal is True
        async with session_factory() as db:
            await db.execute(
                update(MonitorSession)
                .where(MonitorSession.id == session_id)
                .values(
                    codex_thread_id=None,
                    codex_home=None,
                    codex_account_id=None,
                    codex_cleanup_pending=False,
                    codex_cleanup_error=None,
                )
            )
            await db.commit()

    mock_dispatcher.stop_monitor_session_process.side_effect = complete_retry
    with patch("backend.main.dispatcher", mock_dispatcher):
        retry = await client.delete(
            f"/api/tasks/{task_id}/monitor-sessions/{ms_id}"
        )

    assert retry.status_code == 200
    assert retry.json() == {"ok": True}
    assert mock_dispatcher.stop_monitor_session_process.await_count == 2
    # The terminal row was already cancelled by the first request, so retrying
    # cleanup must not publish a duplicate status transition.
    mock_dispatcher.broadcaster.broadcast.assert_not_awaited()
    async with session_factory() as db:
        ms = await db.get(MonitorSession, ms_id)
        assert ms.status == "cancelled"
        assert ms.codex_thread_id is None
        assert ms.codex_home is None
        assert ms.codex_cleanup_pending is False


@pytest.mark.asyncio
async def test_monitor_complete_loses_cas_to_concurrent_cancel(
    client,
    session_factory,
):
    from backend.api.monitor import complete_monitor_session
    from backend.schemas.monitor_session import MonitorCompleteRequest

    task_id = await _create_task_with_monitor(client, session_factory)
    async with session_factory() as db:
        session = MonitorSession(
            task_id=task_id,
            agent_type="monitor",
            source="ccm",
            description="complete-race",
            status="running",
        )
        db.add(session)
        await db.commit()
        session_id = session.id

    callback_ready = asyncio.Event()
    release_callback = asyncio.Event()
    dispatcher = MagicMock()
    dispatcher.snapshot_queue_admission = AsyncMock(return_value=object())
    dispatcher.broadcaster.broadcast = AsyncMock()
    dispatcher.enqueue_message = AsyncMock()

    async with session_factory() as callback_db:
        original_execute = callback_db.execute
        delayed = False

        async def delay_terminal_update(statement, *args, **kwargs):
            nonlocal delayed
            table = getattr(statement, "table", None)
            if (
                not delayed
                and getattr(statement, "is_update", False)
                and getattr(table, "name", None) == "sub_agent_sessions"
            ):
                delayed = True
                callback_ready.set()
                await release_callback.wait()
            return await original_execute(statement, *args, **kwargs)

        with (
            patch.object(
                callback_db,
                "execute",
                new=AsyncMock(side_effect=delay_terminal_update),
            ),
            patch("backend.main.dispatcher", dispatcher),
        ):
            callback = asyncio.create_task(
                complete_monitor_session(
                    task_id,
                    session_id,
                    MonitorCompleteRequest(reason="too late"),
                    _admin_request(),
                    callback_db,
                )
            )
            await callback_ready.wait()
            async with session_factory() as cancel_db:
                await cancel_db.execute(
                    update(MonitorSession)
                    .where(
                        MonitorSession.id == session_id,
                        MonitorSession.status == "running",
                    )
                    .values(status="cancelled")
                )
                await cancel_db.commit()
            release_callback.set()
            with pytest.raises(HTTPException) as exc_info:
                await callback

    assert exc_info.value.status_code == 400
    dispatcher.broadcaster.broadcast.assert_not_awaited()
    dispatcher.enqueue_message.assert_not_awaited()
    async with session_factory() as db:
        session = await db.get(MonitorSession, session_id)
        report_count = await db.scalar(
            select(func.count(MonitorCheck.id)).where(
                MonitorCheck.monitor_session_id == session_id
            )
        )
    assert session.status == "cancelled"
    assert report_count == 0


@pytest.mark.asyncio
async def test_sub_agent_result_loses_cas_to_concurrent_stop(
    client,
    session_factory,
):
    from backend.api.sub_agent_tasks import (
        SubAgentResultRequest,
        sub_agent_submit_result,
    )

    task_id = await _create_task_with_sub_agent(client, session_factory)
    async with session_factory() as db:
        session = MonitorSession(
            task_id=task_id,
            agent_type="sub_agent",
            source="ccm",
            description="result-race",
            status="running",
        )
        db.add(session)
        await db.commit()
        session_id = session.id

    callback_ready = asyncio.Event()
    release_callback = asyncio.Event()
    dispatcher = MagicMock()
    dispatcher.snapshot_queue_admission = AsyncMock(return_value=object())
    dispatcher.broadcaster.broadcast = AsyncMock()
    dispatcher.enqueue_message = AsyncMock()
    dispatcher.stop_sub_agent_session_process = AsyncMock()

    async with session_factory() as callback_db:
        original_execute = callback_db.execute
        delayed = False

        async def delay_terminal_update(statement, *args, **kwargs):
            nonlocal delayed
            table = getattr(statement, "table", None)
            if (
                not delayed
                and getattr(statement, "is_update", False)
                and getattr(table, "name", None) == "sub_agent_sessions"
            ):
                delayed = True
                callback_ready.set()
                await release_callback.wait()
            return await original_execute(statement, *args, **kwargs)

        with (
            patch.object(
                callback_db,
                "execute",
                new=AsyncMock(side_effect=delay_terminal_update),
            ),
            patch("backend.main.dispatcher", dispatcher),
        ):
            callback = asyncio.create_task(
                sub_agent_submit_result(
                    task_id,
                    session_id,
                    SubAgentResultRequest(result="too late"),
                    _admin_request(),
                    callback_db,
                )
            )
            await callback_ready.wait()
            async with session_factory() as stop_db:
                await stop_db.execute(
                    update(MonitorSession)
                    .where(
                        MonitorSession.id == session_id,
                        MonitorSession.status == "running",
                    )
                    .values(status="stopped")
                )
                await stop_db.commit()
            release_callback.set()
            with pytest.raises(HTTPException) as exc_info:
                await callback

    assert exc_info.value.status_code == 400
    dispatcher.broadcaster.broadcast.assert_not_awaited()
    dispatcher.enqueue_message.assert_not_awaited()
    dispatcher.stop_sub_agent_session_process.assert_not_awaited()
    async with session_factory() as db:
        session = await db.get(MonitorSession, session_id)
        report_count = await db.scalar(
            select(func.count(SubAgentReport.id)).where(
                SubAgentReport.session_id == session_id
            )
        )
    assert session.status == "stopped"
    assert report_count == 0


@pytest.mark.asyncio
async def test_late_progress_callbacks_do_not_write_after_terminal_state(
    client,
    session_factory,
):
    task_id = await _create_task_with_monitor(client, session_factory)
    sub_task_id = await _create_task_with_sub_agent(client, session_factory)
    async with session_factory() as db:
        monitor = MonitorSession(
            task_id=task_id,
            agent_type="monitor",
            source="ccm",
            description="late-check",
            status="cancelled",
        )
        sub_agent = MonitorSession(
            task_id=sub_task_id,
            agent_type="sub_agent",
            source="ccm",
            description="late-progress",
            status="stopped",
        )
        db.add_all([monitor, sub_agent])
        await db.commit()
        monitor_id = monitor.id
        sub_agent_id = sub_agent.id

    dispatcher = MagicMock()
    dispatcher.snapshot_queue_admission = AsyncMock(return_value=object())
    dispatcher.broadcaster.broadcast = AsyncMock()
    dispatcher.enqueue_message = AsyncMock()
    with patch("backend.main.dispatcher", dispatcher):
        monitor_response = await client.post(
            f"/api/tasks/{task_id}/monitor-sessions/{monitor_id}/checks",
            json={"summary": "late", "is_important": True},
        )
        sub_agent_response = await client.post(
            f"/api/tasks/{sub_task_id}/sub-agent-sessions/"
            f"{sub_agent_id}/progress",
            json={"summary": "late"},
        )

    assert monitor_response.status_code == 400
    assert sub_agent_response.status_code == 400
    dispatcher.broadcaster.broadcast.assert_not_awaited()
    dispatcher.enqueue_message.assert_not_awaited()
    async with session_factory() as db:
        monitor_reports = await db.scalar(
            select(func.count(MonitorCheck.id)).where(
                MonitorCheck.monitor_session_id == monitor_id
            )
        )
        sub_reports = await db.scalar(
            select(func.count(SubAgentReport.id)).where(
                SubAgentReport.session_id == sub_agent_id
            )
        )
    assert monitor_reports == 0
    assert sub_reports == 0


@pytest.mark.asyncio
async def test_worker_drain_refuses_late_auxiliary_callbacks(
    client,
    session_factory,
    worker_control_plane_auth,
    monkeypatch,
):
    """A callback cannot publish reports after node drain admission closes."""

    from backend import database
    from backend.config import settings
    from backend.services.internal_service_auth import (
        issue_internal_service_token,
    )
    from backend.services.worker_node_control import begin_worker_node_drain

    task_id = await _create_task_with_monitor(client, session_factory)
    sub_task_id = await _create_task_with_sub_agent(client, session_factory)
    async with session_factory() as db:
        monitor = MonitorSession(
            task_id=task_id,
            agent_type="monitor",
            source="ccm",
            description="late-worker-check",
            status="running",
            next_check_at=None,
        )
        sub_agent = MonitorSession(
            task_id=sub_task_id,
            agent_type="sub_agent",
            source="ccm",
            description="late-worker-progress",
            status="running",
        )
        db.add_all([monitor, sub_agent])
        await db.commit()
        monitor_id = monitor.id
        sub_agent_id = sub_agent.id
        monitor_task = await db.get(Task, task_id)
        sub_agent_task = await db.get(Task, sub_task_id)

    monkeypatch.setattr(database, "async_session", session_factory)
    monitor_token = issue_internal_service_token(
        audience="ccm_monitor_agent",
        task_id=task_id,
        task_incarnation_id=monitor_task.incarnation_id,
        monitor_session_id=monitor_id,
        owner_kind="monitor-turn",
        owner_id=f"{monitor_id}:drain-test",
    )
    sub_agent_token = issue_internal_service_token(
        audience="ccm_sub_agent",
        task_id=sub_task_id,
        task_incarnation_id=sub_agent_task.incarnation_id,
        sub_agent_session_id=sub_agent_id,
        owner_kind="sub-agent-session",
        owner_id=sub_agent_id,
    )

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    async with session_factory() as db:
        await begin_worker_node_drain(db, claim="e" * 64)
        await db.commit()

    dispatcher = MagicMock()
    dispatcher.snapshot_queue_admission = AsyncMock(return_value=object())
    dispatcher.broadcaster.broadcast = AsyncMock()
    dispatcher.enqueue_message = AsyncMock()
    with patch("backend.main.dispatcher", dispatcher):
        deployment_monitor_response = await client.post(
            f"/api/tasks/{task_id}/monitor-sessions/{monitor_id}/checks",
            json={"summary": "deployment token must not enter callback"},
        )
        deployment_sub_agent_response = await client.post(
            f"/api/tasks/{sub_task_id}/sub-agent-sessions/"
            f"{sub_agent_id}/progress",
            json={"summary": "deployment token must not enter callback"},
        )
        monitor_response = await client.post(
            f"/api/tasks/{task_id}/monitor-sessions/{monitor_id}/checks",
            json={"summary": "too late", "is_important": True},
            headers={"Authorization": f"Bearer {monitor_token}"},
        )
        sub_agent_response = await client.post(
            f"/api/tasks/{sub_task_id}/sub-agent-sessions/"
            f"{sub_agent_id}/progress",
            json={"summary": "too late"},
            headers={"Authorization": f"Bearer {sub_agent_token}"},
        )

    assert deployment_monitor_response.status_code == 403
    assert deployment_sub_agent_response.status_code == 403
    assert monitor_response.status_code == 409
    assert sub_agent_response.status_code == 409
    dispatcher.broadcaster.broadcast.assert_not_awaited()
    dispatcher.enqueue_message.assert_not_awaited()
    async with session_factory() as db:
        assert await db.scalar(
            select(func.count(MonitorCheck.id)).where(
                MonitorCheck.monitor_session_id == monitor_id
            )
        ) == 0
        assert await db.scalar(
            select(func.count(SubAgentReport.id)).where(
                SubAgentReport.session_id == sub_agent_id
            )
        ) == 0


@pytest.mark.asyncio
async def test_delete_does_not_overwrite_completed_auxiliary_status(
    client,
    session_factory,
):
    task_id = await _create_task_with_monitor(client, session_factory)
    sub_task_id = await _create_task_with_sub_agent(client, session_factory)
    async with session_factory() as db:
        monitor = MonitorSession(
            task_id=task_id,
            agent_type="monitor",
            source="ccm",
            description="done-monitor",
            status="completed",
        )
        sub_agent = MonitorSession(
            task_id=sub_task_id,
            agent_type="sub_agent",
            source="ccm",
            description="done-sub-agent",
            status="completed",
        )
        db.add_all([monitor, sub_agent])
        await db.commit()
        monitor_id = monitor.id
        sub_agent_id = sub_agent.id

    dispatcher = MagicMock()
    dispatcher.stop_monitor_session_process = AsyncMock()
    dispatcher.stop_sub_agent_session_process = AsyncMock()
    dispatcher.broadcaster.broadcast = AsyncMock()
    with patch("backend.main.dispatcher", dispatcher):
        monitor_response = await client.delete(
            f"/api/tasks/{task_id}/monitor-sessions/{monitor_id}"
        )
        sub_agent_response = await client.delete(
            f"/api/tasks/{sub_task_id}/sub-agent-sessions/{sub_agent_id}"
        )

    assert monitor_response.status_code == 200
    assert sub_agent_response.status_code == 200
    dispatcher.broadcaster.broadcast.assert_not_awaited()
    async with session_factory() as db:
        monitor = await db.get(MonitorSession, monitor_id)
        sub_agent = await db.get(MonitorSession, sub_agent_id)
    assert monitor.status == "completed"
    assert sub_agent.status == "completed"


@pytest.mark.asyncio
async def test_get_monitor_checks(client, session_factory):
    task_id = await _create_task_with_monitor(client, session_factory)

    async with session_factory() as db:
        ms = MonitorSession(task_id=task_id, description="checks-test")
        db.add(ms)
        await db.commit()
        await db.refresh(ms)
        ms_id = ms.id

        for i in range(3):
            db.add(MonitorCheck(
                monitor_session_id=ms_id,
                check_number=i + 1,
                status="success",
                summary=f"check {i+1}",
            ))
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}/monitor-sessions/{ms_id}/checks")
    assert resp.status_code == 200
    checks = resp.json()
    assert len(checks) == 3


@pytest.mark.asyncio
async def test_monitor_checks_increment_atomically_and_auto_complete(
    client,
    session_factory,
):
    task_id = await _create_task_with_monitor(client, session_factory)
    async with session_factory() as db:
        session = MonitorSession(
            task_id=task_id,
            agent_type="monitor",
            source="ccm",
            description="two checks",
            status="running",
            max_checks=2,
            turn_generation=1,
            active_turn_generation=1,
        )
        db.add(session)
        await db.commit()
        session_id = session.id

    dispatcher = MagicMock()
    dispatcher.snapshot_queue_admission = AsyncMock(return_value=object())
    dispatcher.broadcaster.broadcast = AsyncMock()
    dispatcher.enqueue_message = AsyncMock()
    dispatcher.stop_monitor_session_process = AsyncMock()
    with patch("backend.main.dispatcher", dispatcher):
            first = await client.post(
                f"/api/tasks/{task_id}/monitor-sessions/{session_id}/checks",
                json={"summary": "first", "turn_generation": 1},
            )
            async with session_factory() as db:
                after_first = await db.get(MonitorSession, session_id)
                assert after_first.status == "running"
                assert after_first.checks_done == 1
                assert after_first.completed_at is None
                # Simulate the scheduler claiming the next due turn.
                after_first.turn_generation = 2
                after_first.active_turn_generation = 2
                after_first.next_check_at = None
                await db.commit()
            second = await client.post(
                f"/api/tasks/{task_id}/monitor-sessions/{session_id}/checks",
                json={"summary": "second", "turn_generation": 2},
            )

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert first.json()["check_number"] == 1
    assert second.json()["check_number"] == 2
    async with session_factory() as db:
        session = await db.get(MonitorSession, session_id)
        reports = list(
            (
                await db.execute(
                    select(MonitorCheck)
                    .where(MonitorCheck.monitor_session_id == session_id)
                    .order_by(MonitorCheck.check_number)
                )
            )
            .scalars()
            .all()
        )
    assert session.status == "completed"
    assert session.checks_done == 2
    assert [report.check_number for report in reports] == [1, 2]
    # The scheduled lifecycle observes the terminal callback after the short
    # turn exits; the callback must not cancel its own MCP response in flight.
    dispatcher.stop_monitor_session_process.assert_not_awaited()


@pytest.mark.asyncio
async def test_monitor_callback_requires_exact_active_turn_generation(
    client,
    session_factory,
):
    task_id = await _create_task_with_monitor(client, session_factory)
    async with session_factory() as db:
        session = MonitorSession(
            task_id=task_id,
            agent_type="monitor",
            source="ccm",
            description="generation fence",
            status="running",
            interval=30,
            turn_generation=7,
            active_turn_generation=7,
        )
        db.add(session)
        await db.commit()
        session_id = session.id

    dispatcher = MagicMock()
    dispatcher.snapshot_queue_admission = AsyncMock(return_value=object())
    dispatcher.broadcaster.broadcast = AsyncMock()
    dispatcher.enqueue_message = AsyncMock()
    with patch("backend.main.dispatcher", dispatcher):
        stale = await client.post(
            f"/api/tasks/{task_id}/monitor-sessions/{session_id}/checks",
            json={
                "summary": "stale",
                "turn_generation": 6,
            },
        )
        accepted = await client.post(
            f"/api/tasks/{task_id}/monitor-sessions/{session_id}/checks",
            json={
                "summary": "current",
                "turn_generation": 7,
            },
        )
        duplicate = await client.post(
            f"/api/tasks/{task_id}/monitor-sessions/{session_id}/checks",
            json={
                "summary": "duplicate",
                "turn_generation": 7,
            },
        )

    assert stale.status_code == 409
    assert accepted.status_code == 200
    assert accepted.json()["check_number"] == 1
    assert duplicate.status_code == 409
    async with session_factory() as db:
        session = await db.get(MonitorSession, session_id)
        reports = list(
            (
                await db.execute(
                    select(MonitorCheck).where(
                        MonitorCheck.monitor_session_id == session_id
                    )
                )
            )
            .scalars()
            .all()
        )
    assert session.checks_done == 1
    assert session.active_turn_generation is None
    assert session.next_check_at is not None
    assert [report.summary for report in reports] == ["current"]


@pytest.mark.asyncio
async def test_scheduled_monitor_rejects_callback_before_generation_claim(
    client,
    session_factory,
):
    task_id = await _create_task_with_monitor(client, session_factory)
    async with session_factory() as db:
        session = MonitorSession(
            task_id=task_id,
            agent_type="monitor",
            source="ccm",
            description="not claimed",
            status="running",
            next_check_at=datetime.utcnow(),
        )
        db.add(session)
        await db.commit()
        session_id = session.id

    response = await client.post(
        f"/api/tasks/{task_id}/monitor-sessions/{session_id}/checks",
        json={"summary": "must not bypass scheduler"},
    )

    assert response.status_code == 409
    async with session_factory() as db:
        session = await db.get(MonitorSession, session_id)
        assert session.checks_done == 0
        assert session.next_check_at is not None


@pytest.mark.asyncio
async def test_sub_agent_progress_then_result_uses_unique_report_numbers(
    client,
    session_factory,
):
    task_id = await _create_task_with_sub_agent(client, session_factory)
    async with session_factory() as db:
        session = MonitorSession(
            task_id=task_id,
            agent_type="sub_agent",
            source="ccm",
            description="progress-result",
            status="running",
        )
        db.add(session)
        await db.commit()
        session_id = session.id

    dispatcher = MagicMock()
    dispatcher.snapshot_queue_admission = AsyncMock(return_value=object())
    dispatcher.broadcaster.broadcast = AsyncMock()
    dispatcher.enqueue_message = AsyncMock()
    dispatcher.stop_sub_agent_session_process = AsyncMock()
    with patch("backend.main.dispatcher", dispatcher):
        progress = await client.post(
            f"/api/tasks/{task_id}/sub-agent-sessions/{session_id}/progress",
            json={"summary": "halfway"},
        )
        result = await client.post(
            f"/api/tasks/{task_id}/sub-agent-sessions/{session_id}/result",
            json={"result": "done", "status": "completed"},
        )

    assert progress.status_code == 200, progress.text
    assert progress.json()["progress_count"] == 1
    assert result.status_code == 200, result.text
    async with session_factory() as db:
        session = await db.get(MonitorSession, session_id)
        reports = list(
            (
                await db.execute(
                    select(SubAgentReport)
                    .where(SubAgentReport.session_id == session_id)
                    .order_by(SubAgentReport.check_number)
                )
            )
            .scalars()
            .all()
        )
    assert session.status == "completed"
    assert session.checks_done == 2
    assert [report.check_number for report in reports] == [1, 2]
    dispatcher.enqueue_message.assert_awaited_once()
    dispatcher.stop_sub_agent_session_process.assert_awaited_once_with(
        session_id
    )
    dispatcher.broadcaster.broadcast.assert_any_await(
        f"task:{task_id}",
        {
            "event": "sub_agent_session_status",
            "sub_agent_session_id": session_id,
            "description": "progress-result",
            "agent_type": "sub_agent",
            "source": "ccm",
            "monitor_context": None,
            "status": "completed",
            "checks_done": 2,
            "last_summary": "done",
            "task_retry_count": 0,
            "task_turn_generation": 0,
        },
    )


@pytest.mark.asyncio
async def test_task_delete_cleans_monitors(client, session_factory):
    task_id = await _create_task_with_monitor(client, session_factory, status="completed")

    async with session_factory() as db:
        ms = MonitorSession(task_id=task_id, description="will-delete", status="completed")
        db.add(ms)
        await db.commit()
        await db.refresh(ms)
        ms_id = ms.id
        db.add(MonitorCheck(
            monitor_session_id=ms_id, check_number=1, status="success", summary="ok",
        ))
        await db.commit()

    resp = await client.delete(f"/api/tasks/{task_id}")
    assert resp.status_code == 200

    async with session_factory() as db:
        ms_result = await db.execute(select(MonitorSession).where(MonitorSession.task_id == task_id))
        assert len(list(ms_result.scalars().all())) == 0
        check_result = await db.execute(select(MonitorCheck).where(MonitorCheck.monitor_session_id == ms_id))
        assert len(list(check_result.scalars().all())) == 0


@pytest.mark.asyncio
async def test_task_cancel_cancels_monitors(client, session_factory):
    task_id = await _create_task_with_monitor(client, session_factory)

    async with session_factory() as db:
        ms = MonitorSession(task_id=task_id, description="will-cancel", status="running")
        db.add(ms)
        await db.commit()
        await db.refresh(ms)
        ms_id = ms.id

    mock_dispatcher = MagicMock()
    mock_dispatcher._monitor_tasks = {}
    mock_dispatcher._monitor_processes = {}
    mock_dispatcher.abort_task_queue = AsyncMock(return_value=0)
    mock_dispatcher.stop_monitor_session_process = AsyncMock()

    with patch("backend.main.dispatcher", mock_dispatcher):
        resp = await client.post(f"/api/tasks/{task_id}/cancel")
    assert resp.status_code == 200

    async with session_factory() as db:
        ms = await db.get(MonitorSession, ms_id)
        assert ms.status == "cancelled"


@pytest.mark.asyncio
async def test_task_cancel_routes_ccm_auxiliary_reapers_by_agent_type(
    client,
    session_factory,
):
    task_id = await _create_task_with_monitor(client, session_factory)

    async with session_factory() as db:
        monitor = MonitorSession(
            task_id=task_id,
            agent_type="monitor",
            source="ccm",
            description="monitor",
            status="running",
        )
        sub_agent = MonitorSession(
            task_id=task_id,
            agent_type="sub_agent",
            source="ccm",
            description="one-shot",
            status="running",
        )
        native = MonitorSession(
            task_id=task_id,
            agent_type="native-agent",
            source="native",
            description="native child",
            status="running",
        )
        db.add_all([monitor, sub_agent, native])
        await db.commit()
        monitor_id = monitor.id
        sub_agent_id = sub_agent.id

    mock_dispatcher = MagicMock()
    mock_dispatcher.abort_task_queue = AsyncMock(return_value=0)
    mock_dispatcher.stop_monitor_session_process = AsyncMock()
    mock_dispatcher.stop_sub_agent_session_process = AsyncMock()

    with patch("backend.main.dispatcher", mock_dispatcher):
        response = await client.post(f"/api/tasks/{task_id}/cancel")

    assert response.status_code == 200, response.text
    mock_dispatcher.stop_monitor_session_process.assert_awaited_once_with(
        monitor_id,
        terminal=True,
    )
    mock_dispatcher.stop_sub_agent_session_process.assert_awaited_once_with(
        sub_agent_id
    )


@pytest.mark.asyncio
async def test_create_monitor_accepts_local_codex_task(
    client,
    session_factory,
    monkeypatch,
):
    """A local Codex task may create the scheduled Monitor runtime."""
    from backend.config import settings

    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
        "provider": "codex",
        "enabled_skills": {"monitor": True},
    })
    task_id = resp.json()["id"]
    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == task_id).values(status="in_progress"))
        await db.commit()

    mock_dispatcher = MagicMock()
    mock_dispatcher.start_monitor_session = MagicMock()
    mock_dispatcher.broadcaster = MagicMock()
    mock_dispatcher.broadcaster.broadcast = AsyncMock()
    with patch("backend.main.dispatcher", mock_dispatcher):
        resp = await client.post(
            f"/api/tasks/{task_id}/monitor-sessions",
            json={"description": "watch build"},
        )

    assert resp.status_code == 200, resp.text
    assert resp.json()["provider"] == "codex"
    mock_dispatcher.start_monitor_session.assert_called_once()


@pytest.mark.asyncio
async def test_create_sub_agent_accepts_codex_task(client, session_factory):
    resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
        "provider": "codex",
        "enabled_skills": {"sub-agent": True},
    })
    task_id = resp.json()["id"]
    async with session_factory() as db:
        await db.execute(update(Task).where(Task.id == task_id).values(status="in_progress"))
        await db.commit()

    mock_dispatcher = MagicMock()
    mock_dispatcher.start_sub_agent_session = MagicMock()
    mock_dispatcher.broadcaster = MagicMock()
    mock_dispatcher.broadcaster.broadcast = AsyncMock()
    with patch("backend.main.dispatcher", mock_dispatcher):
        resp = await client.post(
            f"/api/tasks/{task_id}/sub-agent-sessions",
            json={"name": "review", "prompt": "review the code"},
        )

    assert resp.status_code == 201
    assert resp.json()["description"] == "review"
    mock_dispatcher.start_sub_agent_session.assert_called_once()
    mock_dispatcher.broadcaster.broadcast.assert_awaited_once_with(
        f"task:{task_id}",
        {
            "event": "sub_agent_session_created",
            "sub_agent_session_id": resp.json()["id"],
            "description": "review",
            "agent_type": "sub_agent",
            "source": "ccm",
            "monitor_context": None,
            "status": "running",
            "checks_done": 0,
            "last_summary": "review the code",
            "task_retry_count": 0,
            "task_turn_generation": 0,
        },
    )


@pytest.mark.asyncio
async def test_sub_agent_context_uses_task_description_as_prompt(
    client,
    session_factory,
):
    task_id = await _create_task_with_sub_agent(client, session_factory)
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.description = "canonical task description"
        session = MonitorSession(
            task_id=task_id,
            agent_type="sub_agent",
            source="ccm",
            description="context-reader",
            last_summary="inspect README",
            status="running",
        )
        db.add(session)
        await db.commit()
        await db.refresh(session)
        session_id = session.id

    response = await client.get(
        f"/api/tasks/{task_id}/sub-agent-sessions/{session_id}/context"
    )

    assert response.status_code == 200
    assert response.json()["task_description"] == "canonical task description"
    assert response.json()["task_prompt"] == "canonical task description"
