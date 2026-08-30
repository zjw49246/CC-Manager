"""PTY 权限透传 — CC 权限请求 → 前端卡片 → 用户回包 → BridgeHub。

覆盖：
- _handle_pty_permission_request：落库 LogEntry + 广播 permission_request + 登记 pending
- resolve_pty_permission：回包 bridge、广播 permission_resolved；未知/过期返回 False
- POST /api/tasks/{id}/permissions/{request_id} 端点（allow/deny/410/400）
"""
import asyncio
import json
import threading
import uuid
from datetime import datetime
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from backend.config import settings
from backend.models.project import Project
from backend.models.task import Task
from backend.models.log_entry import LogEntry
from backend.models.team_share import TeamProjectShare, TeamTaskShare
from backend.models.worker_task_termination import WorkerTaskTerminationReceipt
from backend.services.instance_manager import InstanceManager
from backend.services.worker_node_control import begin_worker_node_drain
from backend.tests.test_auth_ws_security import _create_user, secured_client


class _FakeBroadcaster:
    def __init__(self):
        self.events = []

    async def broadcast(self, channel, data):
        self.events.append((channel, data))


def _make_im(db_factory):
    im = InstanceManager.__new__(InstanceManager)
    im.db_factory = db_factory
    im.broadcaster = _FakeBroadcaster()
    im._pty_permissions = {}
    im._pty_permission_callback_lock = threading.Lock()
    im._pty_permission_callback_futures = set()
    im._pty_permission_callbacks_draining = False
    im._pty_backend = None
    im._loop = None
    return im


def _active_worker_termination_receipt(task: Task) -> WorkerTaskTerminationReceipt:
    now = datetime.utcnow()
    return WorkerTaskTerminationReceipt(
        operation_id=uuid.uuid4().hex,
        task_id=task.id,
        active_task_id=task.id,
        side="worker",
        worker_id=None,
        operation="stop_session",
        status="accepted",
        state_version=1,
        source_task_incarnation_id=task.incarnation_id,
        source_task_status=task.status,
        source_task_retry_count=task.retry_count,
        source_task_turn_generation=task.turn_generation,
        source_task_source_log_id=task.turn_source_log_id,
        source_task_instance_id=task.instance_id,
        source_task_started_at=task.started_at,
        source_task_completed_at=task.completed_at,
        source_task_session_id=task.session_id,
        source_task_pty_background_generation=task.pty_background_generation,
        request_payload={"test": "permission-receipt-gate"},
        request_digest="d" * 64,
        attempt_count=0,
        reconcile_count=0,
        next_reconcile_at=now,
        accepted_at=now,
        created_at=now,
        updated_at=now,
    )


REQUEST = {
    "request_id": "perm-1",
    "tool_name": "Bash",
    "description": "运行 rm -rf /tmp/x",
    "input_preview": "rm -rf /tmp/x",
}


@pytest.mark.asyncio
async def test_permission_request_logged_and_broadcast(db_factory, db_session):
    im = _make_im(db_factory)
    task = Task(title="t", description="d", session_id="sess-1", instance_id=3)
    db_session.add(task)
    await db_session.commit()
    await db_session.refresh(task)

    await im._handle_pty_permission_request("sess-1", REQUEST)

    # pending 登记
    assert "perm-1" in im._pty_permissions
    assert im._pty_permissions["perm-1"]["task_id"] == task.id

    # LogEntry 落库
    entry = (
        await db_session.execute(
            select(LogEntry).where(
                LogEntry.task_id == task.id,
                LogEntry.event_type == "permission_request",
            )
        )
    ).scalars().first()
    assert entry is not None
    assert entry.tool_name == "Bash"
    assert json.loads(entry.raw_json)["request_id"] == "perm-1"

    # 广播了卡片事件
    channels = [c for c, _ in im.broadcaster.events]
    payloads = [d for _, d in im.broadcaster.events]
    assert f"task:{task.id}" in channels
    assert payloads[0]["event_type"] == "permission_request"
    assert payloads[0]["request_id"] == "perm-1"
    assert payloads[0]["timeout_seconds"] == 120


@pytest.mark.asyncio
async def test_permission_request_unknown_session_no_broadcast(db_factory):
    im = _make_im(db_factory)
    await im._handle_pty_permission_request("no-such-session", REQUEST)
    # 登记了（万一 task 是后绑定的）但无广播
    assert im.broadcaster.events == []


@pytest.mark.asyncio
async def test_permission_request_yields_to_active_worker_receipt(
    db_factory,
    db_session,
):
    """Task -> receipt ordering prevents a stop ACK from missing a late row."""

    im = _make_im(db_factory)
    task = Task(
        title="terminating permission",
        description="must be rejected",
        session_id="sess-terminating-permission",
        instance_id=1,
    )
    db_session.add(task)
    await db_session.flush()
    db_session.add(_active_worker_termination_receipt(task))
    await db_session.commit()
    task_id = task.id

    await im._handle_pty_permission_request(
        "sess-terminating-permission",
        REQUEST,
    )

    assert "perm-1" not in im._pty_permissions
    assert im.broadcaster.events == []
    async with db_factory() as db:
        assert await db.scalar(
            select(LogEntry.id).where(LogEntry.task_id == task_id)
        ) is None


@pytest.mark.asyncio
async def test_worker_drain_rejects_permission_request_and_resolution(
    db_factory,
    db_session,
    monkeypatch,
):
    """No permission log or Bridge effect may begin behind a drain claim."""

    im = _make_im(db_factory)
    task = Task(
        title="draining permission",
        description="must stay frozen",
        session_id="sess-draining-permission",
        instance_id=1,
    )
    db_session.add(task)
    await db_session.commit()
    task_id = task.id

    # Admit one request before the claim so resolution exercises its own fence.
    await im._handle_pty_permission_request(
        "sess-draining-permission",
        REQUEST,
    )
    fake_backend = MagicMock()
    fake_backend._bridge.resolve_permission = MagicMock(return_value=True)
    im._pty_backend = fake_backend

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    async with db_factory() as db:
        await begin_worker_node_drain(db, claim="a" * 64)
        await db.commit()

    assert await im.resolve_pty_permission("perm-1", "allow") is False
    fake_backend._bridge.resolve_permission.assert_not_called()

    late_request = dict(REQUEST, request_id="perm-late")
    await im._handle_pty_permission_request(
        "sess-draining-permission",
        late_request,
    )
    assert "perm-late" not in im._pty_permissions
    async with db_factory() as db:
        entries = list(
            (
                await db.execute(
                    select(LogEntry).where(LogEntry.task_id == task_id)
                )
            ).scalars()
        )
    assert [entry.event_type for entry in entries] == ["permission_request"]


@pytest.mark.asyncio
async def test_permission_thread_callbacks_are_closed_and_awaited(db_factory):
    im = _make_im(db_factory)
    im._loop = asyncio.get_running_loop()
    started = asyncio.Event()
    release = asyncio.Event()
    retired = asyncio.Event()
    calls = 0

    async def blocked_callback(_session_id, _request):
        nonlocal calls
        calls += 1
        started.set()
        try:
            await release.wait()
        finally:
            retired.set()

    im._handle_pty_permission_request = blocked_callback
    im._on_pty_permission_request("sess-thread", REQUEST)
    await asyncio.wait_for(started.wait(), timeout=1)

    draining = asyncio.create_task(im.drain_pty_permission_callbacks())
    await asyncio.sleep(0)
    assert draining.done() is False
    release.set()
    assert await asyncio.wait_for(draining, timeout=1) == 1
    await asyncio.wait_for(retired.wait(), timeout=1)
    assert im._pty_permission_callback_futures == set()

    im._on_pty_permission_request(
        "sess-thread",
        dict(REQUEST, request_id="perm-after-drain"),
    )
    await asyncio.sleep(0)
    assert calls == 1


@pytest.mark.asyncio
async def test_resolve_permission_roundtrip(db_factory, db_session):
    im = _make_im(db_factory)
    task = Task(title="t", description="d", session_id="sess-2", instance_id=1)
    db_session.add(task)
    await db_session.commit()
    await db_session.refresh(task)

    await im._handle_pty_permission_request("sess-2", REQUEST)
    im.broadcaster.events.clear()

    fake_backend = MagicMock()
    fake_backend._bridge.resolve_permission = MagicMock(return_value=True)
    im._pty_backend = fake_backend

    ok = await im.resolve_pty_permission("perm-1", "allow")
    assert ok is True
    fake_backend._bridge.resolve_permission.assert_called_once_with(
        "sess-2", "perm-1", "allow"
    )
    # pending 已清除，二次回包失败
    assert await im.resolve_pty_permission("perm-1", "allow") is False

    # 广播 resolved
    assert any(
        d["event_type"] == "permission_resolved" and d["behavior"] == "allow"
        for _, d in im.broadcaster.events
    )


@pytest.mark.asyncio
async def test_cancelled_resolution_settles_bridge_and_audit(
    db_factory,
    db_session,
):
    """HTTP cancellation cannot release fences ahead of the Bridge thread."""

    im = _make_im(db_factory)
    task = Task(
        title="cancelled permission",
        description="effect must settle",
        session_id="sess-cancelled-permission",
        instance_id=1,
    )
    db_session.add(task)
    await db_session.commit()
    task_id = task.id
    await im._handle_pty_permission_request(
        "sess-cancelled-permission",
        REQUEST,
    )

    bridge_started = threading.Event()
    release_bridge = threading.Event()

    def resolve_bridge(*_args):
        bridge_started.set()
        assert release_bridge.wait(timeout=2)
        return True

    fake_backend = MagicMock()
    fake_backend._bridge.resolve_permission = MagicMock(
        side_effect=resolve_bridge
    )
    im._pty_backend = fake_backend
    resolving = asyncio.create_task(
        im.resolve_pty_permission("perm-1", "allow")
    )
    while not bridge_started.is_set():
        await asyncio.sleep(0)

    resolving.cancel()
    await asyncio.sleep(0)
    assert resolving.done() is False
    release_bridge.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(resolving, timeout=1)

    async with db_factory() as db:
        entries = list(
            (
                await db.execute(
                    select(LogEntry)
                    .where(LogEntry.task_id == task_id)
                    .order_by(LogEntry.id)
                )
            ).scalars()
        )
    assert [entry.event_type for entry in entries] == [
        "permission_request",
        "system_event",
    ]


@pytest.mark.asyncio
async def test_resolve_unknown_or_expired(db_factory):
    im = _make_im(db_factory)
    assert await im.resolve_pty_permission("nope", "allow") is False

    import time
    im._pty_permissions["old"] = {
        "session_id": "s", "task_id": None, "tool_name": "Bash",
        "expires_at": time.monotonic() - 1,
    }
    assert await im.resolve_pty_permission("old", "deny") is False


# ------------------------------------------------------------- API endpoint


async def _create_task(client, session_factory, session_id="sess-api"):
    resp = await client.post("/api/tasks", json={"title": "t", "description": "d"})
    task_id = resp.json()["id"]
    async with session_factory() as db:
        t = await db.get(Task, task_id)
        t.session_id = session_id
        await db.commit()
    return task_id


@pytest.mark.asyncio
async def test_permission_endpoint_allow(client, session_factory):
    task_id = await _create_task(client, session_factory)
    mock_im = MagicMock()
    mock_im.resolve_pty_permission = AsyncMock(return_value=True)
    with patch("backend.main.instance_manager", mock_im):
        resp = await client.post(
            f"/api/tasks/{task_id}/permissions/perm-9", json={"behavior": "allow"}
        )
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "behavior": "allow"}
    mock_im.resolve_pty_permission.assert_awaited_once_with(
        "perm-9",
        "allow",
        fenced_db=ANY,
        authorized_task_id=task_id,
    )


@pytest.mark.asyncio
async def test_permission_endpoint_reuses_node_acl_task_transaction(
    client,
    session_factory,
):
    """Resolution must not reacquire the already locked Task in a new session."""

    task_id = await _create_task(
        client,
        session_factory,
        session_id="sess-single-permission-transaction",
    )
    im = _make_im(session_factory)
    await im._handle_pty_permission_request(
        "sess-single-permission-transaction",
        dict(REQUEST, request_id="perm-single-transaction"),
    )
    fake_backend = MagicMock()
    fake_backend._bridge.resolve_permission = MagicMock(return_value=True)
    im._pty_backend = fake_backend

    def forbidden_second_session():
        raise AssertionError("permission resolution opened a second DB session")

    im.db_factory = forbidden_second_session
    with patch("backend.main.instance_manager", im):
        response = await client.post(
            f"/api/tasks/{task_id}/permissions/perm-single-transaction",
            json={"behavior": "allow"},
        )

    assert response.status_code == 200, response.text
    fake_backend._bridge.resolve_permission.assert_called_once_with(
        "sess-single-permission-transaction",
        "perm-single-transaction",
        "allow",
    )


@pytest.mark.asyncio
async def test_permission_endpoint_expired_410(client, session_factory):
    task_id = await _create_task(client, session_factory)
    mock_im = MagicMock()
    mock_im.resolve_pty_permission = AsyncMock(return_value=False)
    with patch("backend.main.instance_manager", mock_im):
        resp = await client.post(
            f"/api/tasks/{task_id}/permissions/perm-x", json={"behavior": "deny"}
        )
    assert resp.status_code == 410


@pytest.mark.asyncio
async def test_permission_endpoint_validates_behavior(client, session_factory):
    task_id = await _create_task(client, session_factory)
    resp = await client.post(
        f"/api/tasks/{task_id}/permissions/perm-y", json={"behavior": "maybe"}
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_permission_endpoint_task_not_found(client):
    resp = await client.post(
        "/api/tasks/999999/permissions/perm-z", json={"behavior": "allow"}
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_permission_endpoint_rejects_chat_only_task_share(secured_client):
    """Chat authority must not grant control over the owner's model tools."""

    client, session_factory = secured_client
    owner_id, _ = await _create_user(
        session_factory,
        email="permission-owner@example.com",
        role="member",
    )
    recipient_id, recipient_token = await _create_user(
        session_factory,
        email="permission-chat-recipient@example.com",
        role="member",
    )
    async with session_factory() as db:
        task = Task(
            title="owner permission request",
            description="chat recipient cannot approve Bash",
            created_by=owner_id,
            session_id="sess-shared-permission",
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
        await db.commit()
        task_id = task.id

    mock_im = MagicMock()
    mock_im.resolve_pty_permission = AsyncMock(return_value=True)
    with patch("backend.main.instance_manager", mock_im):
        response = await client.post(
            f"/api/tasks/{task_id}/permissions/perm-shared",
            headers={"Authorization": f"Bearer {recipient_token}"},
            json={"behavior": "allow"},
        )

    assert response.status_code == 403
    mock_im.resolve_pty_permission.assert_not_awaited()


@pytest.mark.asyncio
async def test_permission_endpoint_allows_task_control_identities(secured_client):
    """Owner, Project member, admin and deployment token may answer PTY prompts."""

    client, session_factory = secured_client
    owner_id, owner_token = await _create_user(
        session_factory,
        email="permission-control-owner@example.com",
        role="member",
    )
    collaborator_id, collaborator_token = await _create_user(
        session_factory,
        email="permission-project-member@example.com",
        role="member",
    )
    admin_id, admin_token = await _create_user(
        session_factory,
        email="permission-control-admin@example.com",
        role="admin",
    )
    async with session_factory() as db:
        project = Project(
            name="permission-control-project",
            local_path="/tmp/permission-control-project",
            status="ready",
        )
        db.add(project)
        await db.flush()
        owner_task = Task(
            title="owner controlled permission request",
            description="owner and admin may answer",
            created_by=owner_id,
            session_id="sess-owner-controlled-permission",
        )
        project_task = Task(
            title="project controlled permission request",
            description="Project member may answer",
            created_by=owner_id,
            project_id=project.id,
            session_id="sess-project-controlled-permission",
        )
        db.add_all([owner_task, project_task])
        await db.flush()
        db.add(
            TeamProjectShare(
                project_id=project.id,
                target_type="user",
                target_id=collaborator_id,
                shared_by=admin_id,
            )
        )
        await db.commit()
        owner_task_id = owner_task.id
        project_task_id = project_task.id

    mock_im = MagicMock()
    mock_im.resolve_pty_permission = AsyncMock(return_value=True)
    cases = (
        (owner_task_id, "perm-owner", owner_token),
        (project_task_id, "perm-project", collaborator_token),
        (owner_task_id, "perm-admin", admin_token),
        (owner_task_id, "perm-deployment", "security-service-token"),
    )
    with patch("backend.main.instance_manager", mock_im):
        responses = [
            await client.post(
                f"/api/tasks/{task_id}/permissions/{request_id}",
                headers={"Authorization": f"Bearer {token}"},
                json={"behavior": "allow"},
            )
            for task_id, request_id, token in cases
        ]

    assert [response.status_code for response in responses] == [200] * len(cases)
    assert mock_im.resolve_pty_permission.await_count == len(cases)


@pytest.mark.asyncio
async def test_resolve_not_delivered_no_broadcast(db_factory, db_session):
    """bridge 回包失败（CC 侧已超时/不存在）→ 不落库不广播，返回 False。"""
    im = _make_im(db_factory)
    task = Task(title="t", description="d", session_id="sess-3", instance_id=1)
    db_session.add(task)
    await db_session.commit()

    await im._handle_pty_permission_request("sess-3", REQUEST)
    im.broadcaster.events.clear()

    fake_backend = MagicMock()
    fake_backend._bridge.resolve_permission = MagicMock(return_value=False)
    im._pty_backend = fake_backend

    assert await im.resolve_pty_permission("perm-1", "allow") is False
    assert im.broadcaster.events == []
