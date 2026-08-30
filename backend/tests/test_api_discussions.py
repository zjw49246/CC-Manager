"""Tests for Discussion API endpoints."""
import asyncio
from contextlib import asynccontextmanager
import pytest
from unittest.mock import AsyncMock, MagicMock, call, patch
from datetime import datetime

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.requests import Request

from backend.api import discussions as discussions_api
from backend.api import projects as projects_api
from backend.database import Base
from backend.models.discussion import Discussion, DiscussionAgent, DiscussionMessage
from backend.models.project import Project
from backend.models.team_share import TeamProjectShare
from backend.schemas.discussion import DiscussionCreate
from backend.services.discussion_service import (
    DiscussionProcessCleanupError,
    DiscussionService,
)
from backend.services.project_share_admission import (
    lock_project_share_authority,
)


def _admin_request() -> Request:
    request = Request({
        "type": "http",
        "method": "POST",
        "path": "/",
        "headers": [],
        "query_string": b"",
        "server": ("test", 80),
        "client": ("test", 1),
        "scheme": "http",
    })
    request.state.user_id = None
    request.state.user_role = "super_admin"
    return request


@pytest.mark.asyncio
async def test_create_discussion(client):
    resp = await client.post("/api/discussions", json={
        "title": "Test Discussion",
        "facilitator_model": "claude-opus-4-6",
        "agent_model": "claude-opus-4-6",
        "max_agents": 3,
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["title"] == "Test Discussion"
    assert data["status"] == "active"
    assert data["max_agents"] == 3
    assert data["agent_count"] == 0
    assert data["message_count"] == 0


@pytest.mark.asyncio
async def test_local_project_acl_does_not_disable_discussion_creation(
    client,
    session_factory,
):
    async with session_factory() as db:
        project = Project(name="shared-discussion-create", status="ready")
        db.add(project)
        await db.flush()
        project_id = project.id
        db.add(TeamProjectShare(
            project_id=project_id,
            target_type="user",
            target_id=991,
            shared_by=0,
        ))
        await db.commit()

    response = await client.post(
        "/api/discussions",
        json={"title": "local ACL is not isolation", "project_id": project_id},
    )

    assert response.status_code == 201
    async with session_factory() as db:
        assert await db.scalar(
            select(func.count())
            .select_from(Discussion)
            .where(Discussion.project_id == project_id)
        ) == 1


@pytest.mark.asyncio
async def test_discussion_create_and_local_acl_share_can_coexist(
    tmp_path,
):
    db_path = tmp_path / "discussion-create-share-race.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_path}",
        connect_args={"timeout": 10},
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    async with session_factory() as db:
        project = Project(name="discussion-create-share-race", status="ready")
        db.add(project)
        await db.commit()
        project_id = project.id

    gate = asyncio.Event()
    async def create_discussion():
        async with session_factory() as db:
            await gate.wait()
            await discussions_api.create_discussion(
                DiscussionCreate(
                    title="serialized provider lease",
                    project_id=project_id,
                ),
                _admin_request(),
                db,
            )
            return "discussion"

    async def create_share():
        async with session_factory() as db:
            await gate.wait()
            await lock_project_share_authority(db, project_id)
            db.add(TeamProjectShare(
                project_id=project_id,
                target_type="user",
                target_id=994,
                shared_by=0,
            ))
            await db.commit()
            return "share"

    try:
        creating = asyncio.create_task(create_discussion())
        sharing = asyncio.create_task(create_share())
        gate.set()
        outcomes = await asyncio.wait_for(
            asyncio.gather(creating, sharing),
            timeout=15,
        )

        assert outcomes == ["discussion", "share"]
        async with session_factory() as db:
            discussion_count = await db.scalar(
                select(func.count())
                .select_from(Discussion)
                .where(
                    Discussion.project_id == project_id,
                    Discussion.status.in_(("active", "closing")),
                )
            )
            share_count = await db.scalar(
                select(func.count())
                .select_from(TeamProjectShare)
                .where(TeamProjectShare.project_id == project_id)
            )
        assert (discussion_count, share_count) == (1, 1)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_discussion_create_and_project_delete_serialize_to_one_winner(
    tmp_path,
):
    db_path = tmp_path / "discussion-create-project-delete-race.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_path}",
        connect_args={"timeout": 10},
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    async with session_factory() as db:
        project = Project(name="discussion-project-delete-race", status="ready")
        db.add(project)
        await db.commit()
        project_id = project.id

    gate = asyncio.Event()

    async def create_discussion():
        async with session_factory() as db:
            await gate.wait()
            try:
                await discussions_api.create_discussion(
                    DiscussionCreate(
                        title="serialized against Project deletion",
                        project_id=project_id,
                    ),
                    _admin_request(),
                    db,
                )
                return "discussion"
            except HTTPException as exc:
                await db.rollback()
                assert exc.status_code in {404, 409}
                return "discussion-rejected"

    async def delete_project():
        async with session_factory() as db:
            await gate.wait()
            try:
                await projects_api.delete_project(
                    project_id,
                    _admin_request(),
                    db,
                )
                return "project-deleted"
            except HTTPException as exc:
                await db.rollback()
                assert exc.status_code == 409
                return "project-delete-rejected"

    try:
        creating = asyncio.create_task(create_discussion())
        deleting = asyncio.create_task(delete_project())
        gate.set()
        outcomes = await asyncio.wait_for(
            asyncio.gather(creating, deleting),
            timeout=15,
        )
        assert set(outcomes) in (
            {"discussion", "project-delete-rejected"},
            {"discussion-rejected", "project-deleted"},
        )

        async with session_factory() as db:
            project_exists = await db.get(Project, project_id) is not None
            discussion_count = await db.scalar(
                select(func.count())
                .select_from(Discussion)
                .where(Discussion.project_id == project_id)
            )
        assert (project_exists, discussion_count) in {(True, 1), (False, 0)}
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_local_acl_share_racing_discussion_trigger_can_coexist(
    tmp_path,
):
    db_path = tmp_path / "discussion-share-trigger-race.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_path}",
        connect_args={"timeout": 10},
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    async with session_factory() as db:
        project = Project(name="discussion-share-trigger-race", status="ready")
        db.add(project)
        await db.flush()
        discussion = Discussion(
            title="share trigger serialization",
            project_id=project.id,
            status="active",
        )
        db.add(discussion)
        await db.flush()
        agent = DiscussionAgent(
            discussion_id=discussion.id,
            role_name="Reviewer",
            system_prompt="review",
            status="idle",
        )
        db.add(agent)
        await db.commit()
        project_id = project.id
        agent_id = agent.id

    gate = asyncio.Event()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    service = DiscussionService(session_factory, broadcaster)
    service._write_history_file = MagicMock(return_value="/tmp/not-created.md")
    service._launch_agent_with_prompt = MagicMock()

    async def trigger():
        async with session_factory() as db:
            await gate.wait()
            await service.trigger_agent(db, agent_id)
            return "triggered"

    async def share():
        async with session_factory() as db:
            await gate.wait()
            await lock_project_share_authority(db, project_id)
            db.add(TeamProjectShare(
                project_id=project_id,
                target_type="user",
                target_id=995,
                shared_by=0,
            ))
            await db.commit()
            return "shared"

    try:
        triggering = asyncio.create_task(trigger())
        sharing = asyncio.create_task(share())
        gate.set()
        assert await asyncio.wait_for(
            asyncio.gather(triggering, sharing),
            timeout=15,
        ) == ["triggered", "shared"]

        service._launch_agent_with_prompt.assert_called_once()
        async with session_factory() as db:
            current = await db.get(DiscussionAgent, agent_id)
            assert current.status == "running"
            assert await db.scalar(
                select(func.count())
                .select_from(TeamProjectShare)
                .where(TeamProjectShare.project_id == project_id)
            ) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_list_discussions(client):
    await client.post("/api/discussions", json={"title": "A"})
    await client.post("/api/discussions", json={"title": "B"})
    resp = await client.get("/api/discussions")
    assert resp.status_code == 200
    assert len(resp.json()) >= 2


@pytest.mark.asyncio
async def test_get_discussion(client):
    create = await client.post("/api/discussions", json={"title": "Detail Test"})
    did = create.json()["id"]
    resp = await client.get(f"/api/discussions/{did}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["title"] == "Detail Test"
    assert isinstance(data["messages"], list)
    assert isinstance(data["agents"], list)


@pytest.mark.asyncio
async def test_get_discussion_not_found(client):
    resp = await client.get("/api/discussions/99999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_discussion(client):
    create = await client.post("/api/discussions", json={"title": "Delete Me"})
    did = create.json()["id"]
    resp = await client.delete(f"/api/discussions/{did}")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    resp2 = await client.get(f"/api/discussions/{did}")
    assert resp2.status_code == 404


@pytest.mark.asyncio
async def test_delete_discussion_not_found(client):
    resp = await client.delete("/api/discussions/99999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_delete_cleanup_failure_keeps_closing_and_retry_converges(
    client,
    session_factory,
):
    create = await client.post(
        "/api/discussions",
        json={"title": "retry cleanup"},
    )
    discussion_id = create.json()["id"]

    class _FailOnceService:
        attempts = 0

        @asynccontextmanager
        async def deletion_barrier(self, _discussion_id, _db):
            self.attempts += 1
            if self.attempts == 1:
                raise DiscussionProcessCleanupError("child still alive")
            yield []

    service = _FailOnceService()
    with patch("backend.api.discussions._get_service", return_value=service):
        first = await client.delete(f"/api/discussions/{discussion_id}")
        assert first.status_code == 409
        assert "remains closing" in first.json()["detail"]

        async with session_factory() as db:
            current = await db.get(Discussion, discussion_id)
            assert current is not None
            assert current.status == "closing"

        retry = await client.delete(f"/api/discussions/{discussion_id}")

    assert retry.status_code == 200
    assert service.attempts == 2
    async with session_factory() as db:
        assert await db.get(Discussion, discussion_id) is None


@pytest.mark.asyncio
async def test_cancelled_delete_keeps_closing_and_retry_converges(
    client,
    session_factory,
):
    create = await client.post(
        "/api/discussions",
        json={"title": "cancel cleanup"},
    )
    discussion_id = create.json()["id"]
    entered = asyncio.Event()

    class _CancellableService:
        retrying = False

        @asynccontextmanager
        async def deletion_barrier(self, _discussion_id, _db):
            if not self.retrying:
                entered.set()
                await asyncio.Event().wait()
            yield []

    service = _CancellableService()
    with patch("backend.api.discussions._get_service", return_value=service):
        deleting = asyncio.create_task(
            client.delete(f"/api/discussions/{discussion_id}")
        )
        await asyncio.wait_for(entered.wait(), timeout=5)
        deleting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await deleting

        async with session_factory() as db:
            current = await db.get(Discussion, discussion_id)
            assert current is not None
            assert current.status == "closing"

        service.retrying = True
        retry = await client.delete(f"/api/discussions/{discussion_id}")

    assert retry.status_code == 200
    async with session_factory() as db:
        assert await db.get(Discussion, discussion_id) is None


@pytest.mark.asyncio
async def test_cancelled_orphan_delete_keeps_closing_and_retry_converges(
    client,
    session_factory,
):
    async with session_factory() as db:
        project = Project(name="orphan-delete-cancellation", status="ready")
        db.add(project)
        await db.flush()
        discussion = Discussion(
            title="cancel orphan cleanup",
            project_id=project.id,
            status="closed",
        )
        db.add(discussion)
        await db.commit()
        project_id = project.id
        discussion_id = discussion.id
        # Simulate the historical unsafe Project deletion path. Current API
        # deletion now vetoes every remaining Discussion row.
        await db.delete(project)
        await db.commit()

    entered = asyncio.Event()

    class _CancellableService:
        retrying = False

        @asynccontextmanager
        async def deletion_barrier(self, _discussion_id, _db):
            if not self.retrying:
                entered.set()
                await asyncio.Event().wait()
            yield []

    service = _CancellableService()
    with patch("backend.api.discussions._get_service", return_value=service):
        deleting = asyncio.create_task(
            client.delete(f"/api/discussions/{discussion_id}")
        )
        await asyncio.wait_for(entered.wait(), timeout=5)
        deleting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await deleting

        async with session_factory() as db:
            assert await db.get(Project, project_id) is None
            current = await db.get(Discussion, discussion_id)
            assert current is not None
            assert current.status == "closing"

        service.retrying = True
        retry = await client.delete(f"/api/discussions/{discussion_id}")

    assert retry.status_code == 200
    async with session_factory() as db:
        assert await db.get(Discussion, discussion_id) is None


# ---------------------------------------------------------------------------
# Stop agent tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_agent_not_found(client):
    create = await client.post("/api/discussions", json={"title": "Stop Test"})
    did = create.json()["id"]
    resp = await client.post(f"/api/discussions/{did}/agents/99999/stop")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_stop_agent_wrong_discussion(client, session_factory):
    d1 = await client.post("/api/discussions", json={"title": "D1"})
    d2 = await client.post("/api/discussions", json={"title": "D2"})
    d1_id = d1.json()["id"]
    d2_id = d2.json()["id"]

    async with session_factory() as db:
        agent = DiscussionAgent(
            discussion_id=d1_id,
            role_name="Tester",
            system_prompt="test",
            status="running",
            created_at=datetime.utcnow(),
        )
        db.add(agent)
        await db.commit()
        await db.refresh(agent)
        agent_id = agent.id

    resp = await client.post(f"/api/discussions/{d2_id}/agents/{agent_id}/stop")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_stop_idle_agent_succeeds(client, session_factory):
    create = await client.post("/api/discussions", json={"title": "Stop Idle"})
    did = create.json()["id"]

    async with session_factory() as db:
        agent = DiscussionAgent(
            discussion_id=did,
            role_name="Idle Agent",
            system_prompt="test",
            status="idle",
            created_at=datetime.utcnow(),
        )
        db.add(agent)
        await db.commit()
        await db.refresh(agent)
        agent_id = agent.id

    with patch("backend.api.discussions._get_service") as mock_svc:
        mock_svc.return_value.stop_agent = AsyncMock()
        resp = await client.post(f"/api/discussions/{did}/agents/{agent_id}/stop")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        mock_svc.return_value.stop_agent.assert_awaited_once_with(agent_id)


@pytest.mark.asyncio
async def test_stop_running_agent_calls_service(client, session_factory):
    create = await client.post("/api/discussions", json={"title": "Stop Running"})
    did = create.json()["id"]

    async with session_factory() as db:
        agent = DiscussionAgent(
            discussion_id=did,
            role_name="Runner",
            system_prompt="test",
            status="running",
            pid=12345,
            created_at=datetime.utcnow(),
        )
        db.add(agent)
        await db.commit()
        await db.refresh(agent)
        agent_id = agent.id

    with patch("backend.api.discussions._get_service") as mock_svc:
        mock_svc.return_value.stop_agent = AsyncMock()
        resp = await client.post(f"/api/discussions/{did}/agents/{agent_id}/stop")
        assert resp.status_code == 200
        mock_svc.return_value.stop_agent.assert_awaited_once_with(agent_id)


# ---------------------------------------------------------------------------
# Stop agent unit tests (service layer)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_agent_service_sigint_then_wait():
    """stop_agent signals the exact process group and proves it exited."""
    from backend.services.discussion_service import DiscussionService
    import signal

    mock_broadcaster = MagicMock()
    svc = DiscussionService(db_factory=AsyncMock(), broadcaster=mock_broadcaster)

    mock_proc = AsyncMock()
    mock_proc.returncode = None
    mock_proc.pid = 4242
    mock_proc.send_signal = MagicMock()

    async def wait_for_exit():
        mock_proc.returncode = 0
        return 0

    mock_proc.wait = AsyncMock(side_effect=wait_for_exit)

    svc._processes[42] = mock_proc
    process_group_signalled = False

    def fake_killpg(pgid, sig):
        nonlocal process_group_signalled
        assert pgid == 4242
        if sig == 0:
            if process_group_signalled:
                raise ProcessLookupError
            return
        assert sig == signal.SIGINT
        process_group_signalled = True

    with patch(
        "backend.services.discussion_service.os.killpg",
        side_effect=fake_killpg,
    ) as killpg:
        await svc.stop_agent(42)

    killpg.assert_any_call(4242, signal.SIGINT)
    assert [
        call
        for call in killpg.call_args_list
        if call.args[1] != 0
    ] == [call(4242, signal.SIGINT)]
    mock_proc.send_signal.assert_not_called()


@pytest.mark.asyncio
async def test_stop_agent_service_no_process():
    """stop_agent with no tracked process is a no-op."""
    from backend.services.discussion_service import DiscussionService

    svc = DiscussionService(db_factory=AsyncMock(), broadcaster=MagicMock())
    await svc.stop_agent(999)


@pytest.mark.asyncio
async def test_stop_agent_service_already_exited():
    """A reaped leader with no live descendants is a no-op."""
    from backend.services.discussion_service import DiscussionService

    svc = DiscussionService(db_factory=AsyncMock(), broadcaster=MagicMock())

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.pid = 4242
    svc._processes[42] = mock_proc

    with patch(
        "backend.services.discussion_service.os.killpg",
        side_effect=ProcessLookupError,
    ) as killpg:
        await svc.stop_agent(42)

    killpg.assert_called_once_with(4242, 0)
    mock_proc.send_signal.assert_not_called()


# ---------------------------------------------------------------------------
# Trigger / Chat guards
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_agent_not_found(client):
    create = await client.post("/api/discussions", json={"title": "Trigger"})
    did = create.json()["id"]
    resp = await client.post(f"/api/discussions/{did}/agents/99999/trigger")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_trigger_running_agent_409(client, session_factory):
    create = await client.post("/api/discussions", json={"title": "Trigger Running"})
    did = create.json()["id"]

    async with session_factory() as db:
        agent = DiscussionAgent(
            discussion_id=did,
            role_name="Busy",
            system_prompt="test",
            status="running",
            created_at=datetime.utcnow(),
        )
        db.add(agent)
        await db.commit()
        await db.refresh(agent)
        agent_id = agent.id

    resp = await client.post(f"/api/discussions/{did}/agents/{agent_id}/trigger")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_local_project_acl_does_not_disable_discussion_trigger(
    client,
    session_factory,
):
    async with session_factory() as db:
        project = Project(name="shared-discussion-trigger", status="ready")
        db.add(project)
        await db.flush()
        discussion = Discussion(
            title="shared trigger",
            project_id=project.id,
            status="active",
        )
        db.add(discussion)
        await db.flush()
        agent = DiscussionAgent(
            discussion_id=discussion.id,
            role_name="Reviewer",
            system_prompt="review",
            status="idle",
        )
        db.add(agent)
        db.add(TeamProjectShare(
            project_id=project.id,
            target_type="user",
            target_id=992,
            shared_by=0,
        ))
        await db.commit()
        discussion_id = discussion.id
        agent_id = agent.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    service = DiscussionService(session_factory, broadcaster)
    service._launch_agent_with_prompt = MagicMock()
    with patch("backend.api.discussions._get_service", return_value=service):
        response = await client.post(
            f"/api/discussions/{discussion_id}/agents/{agent_id}/trigger"
        )

    assert response.status_code == 200
    service._launch_agent_with_prompt.assert_called_once()
    async with session_factory() as db:
        current = await db.get(DiscussionAgent, agent_id)
        assert current.status == "running"
        assert current.pid is None


@pytest.mark.asyncio
async def test_projectless_discussion_trigger_remains_provider_capable(
    client,
    session_factory,
):
    async with session_factory() as db:
        discussion = Discussion(
            title="projectless trigger",
            project_id=None,
            status="active",
        )
        db.add(discussion)
        await db.flush()
        agent = DiscussionAgent(
            discussion_id=discussion.id,
            role_name="Reviewer",
            system_prompt="review",
            status="idle",
        )
        db.add(agent)
        await db.commit()
        discussion_id = discussion.id
        agent_id = agent.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    service = DiscussionService(session_factory, broadcaster)
    service._write_history_file = MagicMock(return_value="/tmp/not-created.md")
    service._launch_agent_with_prompt = MagicMock()
    with patch("backend.api.discussions._get_service", return_value=service):
        response = await client.post(
            f"/api/discussions/{discussion_id}/agents/{agent_id}/trigger"
        )

    assert response.status_code == 200
    service._launch_agent_with_prompt.assert_called_once()
    async with session_factory() as db:
        current = await db.get(DiscussionAgent, agent_id)
        assert current.status == "running"
        assert current.pid is None


@pytest.mark.asyncio
async def test_chat_agent_not_found(client):
    create = await client.post("/api/discussions", json={"title": "Chat"})
    did = create.json()["id"]
    resp = await client.post(
        f"/api/discussions/{did}/agents/99999/chat",
        json={"message": "hello"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_chat_agent_running_409(client, session_factory):
    create = await client.post("/api/discussions", json={"title": "Chat Running"})
    did = create.json()["id"]

    async with session_factory() as db:
        agent = DiscussionAgent(
            discussion_id=did,
            role_name="Busy",
            system_prompt="test",
            status="running",
            created_at=datetime.utcnow(),
        )
        db.add(agent)
        await db.commit()
        await db.refresh(agent)
        agent_id = agent.id

    resp = await client.post(
        f"/api/discussions/{did}/agents/{agent_id}/chat",
        json={"message": "hello"},
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_chat_agent_no_session_400(client, session_factory):
    create = await client.post("/api/discussions", json={"title": "Chat No Session"})
    did = create.json()["id"]

    async with session_factory() as db:
        agent = DiscussionAgent(
            discussion_id=did,
            role_name="New",
            system_prompt="test",
            status="idle",
            session_id=None,
            created_at=datetime.utcnow(),
        )
        db.add(agent)
        await db.commit()
        await db.refresh(agent)
        agent_id = agent.id

    resp = await client.post(
        f"/api/discussions/{did}/agents/{agent_id}/chat",
        json={"message": "hello"},
    )
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Add agent endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_agent_not_found(client):
    resp = await client.post("/api/discussions/99999/add-agent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_add_agent_calls_service(client, session_factory):
    create = await client.post("/api/discussions", json={"title": "Add Agent"})
    did = create.json()["id"]

    mock_agent = DiscussionAgent(
        id=1,
        discussion_id=did,
        role_name="新角色",
        system_prompt="test",
        status="running",
        created_at=datetime.utcnow(),
    )

    with patch("backend.api.discussions._get_service") as mock_svc:
        mock_svc.return_value.add_agent = AsyncMock(return_value=mock_agent)
        resp = await client.post(f"/api/discussions/{did}/add-agent")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        mock_svc.return_value.add_agent.assert_awaited_once()


# ---------------------------------------------------------------------------
# Resume-all endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resume_all_no_agents(client):
    create = await client.post("/api/discussions", json={"title": "Resume Empty"})
    did = create.json()["id"]
    resp = await client.post(f"/api/discussions/{did}/resume-all")
    assert resp.status_code == 200
    assert resp.json()["resumed"] == 0


@pytest.mark.asyncio
async def test_resume_all_not_found(client):
    resp = await client.post("/api/discussions/99999/resume-all")
    assert resp.status_code == 404
