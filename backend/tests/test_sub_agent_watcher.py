import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from backend.models.sub_agent import SubAgentSession
from backend.models.task import Task
from backend.services import sub_agent_watcher as watcher_module
from backend.services.sub_agent_watcher import (
    IDLE_THRESHOLD,
    SubAgentWatcher,
)


@pytest.mark.asyncio
async def test_codex_native_agent_is_not_polled_as_claude_transcript(
    db_factory,
):
    async with db_factory() as db:
        task = Task(
            title="codex-native",
            status="executing",
            session_id="thread-root",
        )
        db.add(task)
        await db.flush()
        sub_agent = SubAgentSession(
            task_id=task.id,
            agent_type="native-agent",
            source="native",
            provider="codex",
            description="Codex child",
            status="running",
            codex_thread_id="thread-child",
            meta=json.dumps({"tool_use_id": "codex:thread-child"}),
        )
        db.add(sub_agent)
        await db.commit()

    watcher = SubAgentWatcher(db_factory, AsyncMock())
    watcher._resolve_paths = AsyncMock(
        side_effect=AssertionError("Codex child has no Claude transcript"),
    )

    await watcher._tick()

    watcher._resolve_paths.assert_not_called()
    assert watcher._tracked == {}


@pytest.mark.asyncio
async def test_silent_live_agent_is_not_inferred_complete(
    db_factory,
    tmp_path,
):
    transcript = tmp_path / "agent-live.jsonl"
    transcript.write_text(
        json.dumps({
            "type": "assistant",
            "message": {
                "role": "assistant",
                "stop_reason": "tool_use",
                "content": [],
            },
        }) + "\n",
        encoding="utf-8",
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import time; time.sleep(5)",
    )
    try:
        async with db_factory() as db:
            task = Task(
                title="native-live",
                status="executing",
                session_id="session-live",
            )
            db.add(task)
            await db.flush()
            sub_agent = SubAgentSession(
                task_id=task.id,
                agent_type="native-agent",
                source="native",
                description="long tool",
                status="running",
                meta=json.dumps({"tool_use_id": "toolu_live"}),
            )
            db.add(sub_agent)
            await db.commit()
            sid = sub_agent.id

        watcher = SubAgentWatcher(db_factory, AsyncMock())
        watcher._tracked[sid] = {
            "task_id": task.id,
            "agent_id": "live",
            "jsonl_path": str(transcript),
            "last_size": transcript.stat().st_size,
            "idle_since": datetime.utcnow()
            - timedelta(seconds=IDLE_THRESHOLD + 1),
            "description": "long tool",
        }

        await watcher._tick()

        assert process.returncode is None
        async with db_factory() as db:
            current = await db.get(SubAgentSession, sid)
            assert current.status == "running"
            assert current.completed_at is None
    finally:
        if process.returncode is None:
            process.terminate()
        await process.wait()


@pytest.mark.asyncio
async def test_explicit_end_turn_marks_idle_native_agent_complete(
    db_factory,
    tmp_path,
):
    transcript = tmp_path / "agent-done.jsonl"
    transcript.write_text(
        json.dumps({
            "type": "assistant",
            "message": {
                "role": "assistant",
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "done"}],
            },
        }) + "\n",
        encoding="utf-8",
    )
    async with db_factory() as db:
        task = Task(
            title="native-done",
            status="executing",
            session_id="session-done",
        )
        db.add(task)
        await db.flush()
        sub_agent = SubAgentSession(
            task_id=task.id,
            agent_type="native-agent",
            source="native",
            description="done",
            status="running",
            meta=json.dumps({"tool_use_id": "toolu_done"}),
        )
        db.add(sub_agent)
        await db.commit()
        sid = sub_agent.id

    broadcaster = AsyncMock()
    watcher = SubAgentWatcher(db_factory, broadcaster)
    watcher._tracked[sid] = {
        "task_id": task.id,
        "agent_id": "done",
        "jsonl_path": str(transcript),
        "last_size": transcript.stat().st_size,
        "idle_since": datetime.utcnow()
        - timedelta(seconds=IDLE_THRESHOLD + 1),
        "description": "done",
    }

    await watcher._tick()

    async with db_factory() as db:
        current = await db.get(SubAgentSession, sid)
        assert current.status == "completed"
        assert current.completed_at is not None


@pytest.mark.asyncio
async def test_worker_drain_refuses_native_agent_completion_writer(
    db_factory,
    tmp_path,
    monkeypatch,
):
    """A stale transcript watcher cannot mutate or publish after node drain."""

    from backend.config import settings
    from backend.services.worker_node_control import begin_worker_node_drain

    transcript = tmp_path / "agent-late-done.jsonl"
    transcript.write_text(
        json.dumps({
            "type": "assistant",
            "message": {
                "role": "assistant",
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "late done"}],
            },
        }) + "\n",
        encoding="utf-8",
    )
    async with db_factory() as db:
        task = Task(
            title="native-late-done",
            status="executing",
            session_id="session-late-done",
        )
        db.add(task)
        await db.flush()
        sub_agent = SubAgentSession(
            task_id=task.id,
            agent_type="native-agent",
            source="native",
            description="late done",
            status="running",
            meta=json.dumps({"tool_use_id": "toolu_late_done"}),
        )
        db.add(sub_agent)
        await db.commit()
        task_id = task.id
        sid = sub_agent.id

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    async with db_factory() as db:
        await begin_worker_node_drain(db, claim="f" * 64)
        await db.commit()

    broadcaster = AsyncMock()
    watcher = SubAgentWatcher(db_factory, broadcaster)
    watcher._tracked[sid] = {
        "task_id": task_id,
        "agent_id": "late-done",
        "jsonl_path": str(transcript),
        "last_size": transcript.stat().st_size,
        "idle_since": datetime.utcnow()
        - timedelta(seconds=IDLE_THRESHOLD + 1),
        "description": "late done",
    }

    await watcher._tick()

    async with db_factory() as db:
        current = await db.get(SubAgentSession, sid)
    assert current.status == "running"
    assert current.completed_at is None
    broadcaster.broadcast.assert_not_awaited()


def test_resolve_paths_searches_default_and_pool_configured_homes(
    monkeypatch,
    tmp_path,
):
    home = tmp_path / "home"
    default_home = home / ".claude"
    custom_home = tmp_path / "custom-claude"
    pool_config = tmp_path / "accounts.json"
    session_id = "session-any-dialect"
    tool_use_id = "toolu_custom_1234567890"
    subagents = (
        custom_home
        / "projects"
        / "encoded-project"
        / session_id
        / "subagents"
    )
    subagents.mkdir(parents=True)
    meta_file = subagents / "agent-agent123.meta.json"
    meta_file.write_text(
        json.dumps({"toolUseId": tool_use_id}),
        encoding="utf-8",
    )
    (subagents / "agent-agent123.jsonl").write_text("", encoding="utf-8")
    default_home.mkdir(parents=True)
    pool_config.write_text(
        json.dumps({
            "accounts": [{"config_dir": str(custom_home)}],
        }),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        watcher_module.Path,
        "home",
        classmethod(lambda cls: home),
    )
    from backend.config import settings
    monkeypatch.setattr(settings, "pool_config_path", str(pool_config))
    monkeypatch.setattr(
        settings,
        "cloudrouter_accounts_dir",
        str(tmp_path / "api-accounts"),
    )
    monkeypatch.setattr(
        settings,
        "database_url",
        "postgresql+asyncpg://db/ccm",
    )

    watcher = SubAgentWatcher(AsyncMock(), AsyncMock())
    resolved = watcher._resolve_paths(
        SimpleNamespace(
            meta=json.dumps({"tool_use_id": tool_use_id}),
            task_id=1,
        ),
        session_id,
    )

    assert resolved == {
        "agent_id": "agent123",
        "jsonl_path": str(subagents / "agent-agent123.jsonl"),
    }


@pytest.mark.asyncio
async def test_shutdown_awaits_cancelled_poll_loop():
    watcher = SubAgentWatcher(AsyncMock(), AsyncMock())
    entered = asyncio.Event()
    finalized = asyncio.Event()

    async def poll():
        entered.set()
        try:
            await asyncio.Future()
        finally:
            await asyncio.sleep(0)
            finalized.set()

    watcher._poll_loop = poll
    watcher.start()
    await entered.wait()

    await watcher.shutdown()

    assert finalized.is_set()
    assert watcher._task is None
