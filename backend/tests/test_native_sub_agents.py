"""Native sub-agent integration — PTY 观测到的模型原生子 agent 接入通用子 agent 体系。

覆盖：
- SubAgentSession 通用模型（agent_type/source/meta 字段、旧名兼容别名）
- InstanceManager._upsert_native_sub_agent 的 spawn/progress/done 生命周期
- /sub-agents/summary 按 agent_type 分组
"""
import json

import pytest
from sqlalchemy import select

from backend.models.task import Task
from backend.models.sub_agent import SubAgentSession, SubAgentReport
from backend.models.monitor_session import MonitorSession, MonitorCheck


# ------------------------------------------------------------------ model


@pytest.mark.asyncio
async def test_generic_model_defaults(db_session):
    sa = SubAgentSession(task_id=1, description="原生子agent")
    db_session.add(sa)
    await db_session.commit()
    await db_session.refresh(sa)
    assert sa.agent_type == "monitor"  # 默认类别保持 monitor 兼容
    assert sa.source == "ccm"
    assert sa.meta is None


@pytest.mark.asyncio
async def test_native_agent_record(db_session):
    sa = SubAgentSession(
        task_id=1,
        agent_type="native-agent",
        source="native",
        description="摸清架构",
        meta=json.dumps({"tool_use_id": "toolu_x", "background": False}),
    )
    db_session.add(sa)
    await db_session.commit()
    await db_session.refresh(sa)
    assert sa.agent_type == "native-agent"
    assert json.loads(sa.meta)["tool_use_id"] == "toolu_x"


@pytest.mark.asyncio
async def test_legacy_aliases_still_work(db_session):
    """MonitorSession/MonitorCheck 别名 + monitor_session_id synonym 兼容旧调用点。"""
    ms = MonitorSession(task_id=2, description="legacy")
    db_session.add(ms)
    await db_session.commit()
    await db_session.refresh(ms)
    assert isinstance(ms, SubAgentSession)

    check = MonitorCheck(monitor_session_id=ms.id, check_number=1, status="success")
    db_session.add(check)
    await db_session.commit()
    await db_session.refresh(check)
    assert check.session_id == ms.id
    assert check.monitor_session_id == ms.id

    loaded = (
        await db_session.execute(
            select(MonitorCheck).where(MonitorCheck.monitor_session_id == ms.id)
        )
    ).scalars().first()
    assert loaded.id == check.id


# ------------------------------------------------------- upsert lifecycle


class _FakeBroadcaster:
    def __init__(self):
        self.events = []

    async def broadcast(self, channel, data):
        self.events.append((channel, data))


def _make_im(db_factory):
    """A minimal InstanceManager carrying just what _upsert_native_sub_agent needs."""
    from backend.services.instance_manager import InstanceManager

    im = InstanceManager.__new__(InstanceManager)
    im.db_factory = db_factory
    im.broadcaster = _FakeBroadcaster()
    return im


@pytest.fixture
def im(db_factory):
    return _make_im(db_factory)


async def _create_native_task(
    db_session,
    *,
    retry_count: int,
    turn_generation: int,
) -> Task:
    task = Task(
        title="native sub-agent owner",
        description="exact logical turn",
        status="executing",
        retry_count=retry_count,
        turn_generation=turn_generation,
    )
    db_session.add(task)
    await db_session.commit()
    await db_session.refresh(task)
    return task


@pytest.mark.asyncio
async def test_spawn_progress_done_lifecycle(im, db_session):
    task = await _create_native_task(
        db_session,
        retry_count=3,
        turn_generation=17,
    )
    generation = {
        "task_retry_count": task.retry_count,
        "task_turn_generation": task.turn_generation,
    }
    info = {
        "tool_use_id": "toolu_abc",
        "kind": "native-monitor",
        "description": "watch smoke log",
    }
    await im._upsert_native_sub_agent(
        task.id,
        "subagent_spawn",
        info,
        **generation,
    )

    row = (
        await db_session.execute(
            select(SubAgentSession).where(SubAgentSession.task_id == task.id)
        )
    ).scalars().first()
    assert row is not None
    assert row.agent_type == "native-monitor"
    assert row.source == "native"
    assert row.status == "running"

    # replay safety: duplicate spawn does not create a second row
    await im._upsert_native_sub_agent(
        task.id,
        "subagent_spawn",
        info,
        **generation,
    )
    rows = (
        await db_session.execute(
            select(SubAgentSession).where(SubAgentSession.task_id == task.id)
        )
    ).scalars().all()
    assert len(rows) == 1

    await im._upsert_native_sub_agent(
        task.id,
        "subagent_progress",
        {**info, "summary": "step: deploy"},
        **generation,
    )
    await db_session.refresh(row)
    assert row.checks_done == 1
    assert "deploy" in row.last_summary

    await im._upsert_native_sub_agent(
        task.id,
        "subagent_done",
        {**info, "timed_out": True},
        **generation,
    )
    await db_session.refresh(row)
    assert row.status == "completed"
    assert row.completed_at is not None
    assert "[timed out]" in row.last_summary

    event_types = [d["event_type"] for _, d in im.broadcaster.events]
    assert event_types == [
        "sub_agent_session_created",
        "sub_agent_count",
        "sub_agent_report",
        "system_event",  # subagent_progress 同时写入聊天 system_event
        "sub_agent_session_status",
        "sub_agent_count",
    ]
    assert all(
        event["task_retry_count"] == task.retry_count
        and event["task_turn_generation"] == task.turn_generation
        for _, event in im.broadcaster.events
    )


@pytest.mark.asyncio
async def test_progress_for_unknown_agent_is_noop(im, db_session):
    task = await _create_native_task(
        db_session,
        retry_count=4,
        turn_generation=23,
    )
    await im._upsert_native_sub_agent(
        task.id,
        "subagent_progress",
        {"tool_use_id": "toolu_zzz", "summary": "x"},
        task_retry_count=task.retry_count,
        task_turn_generation=task.turn_generation,
    )
    rows = (
        await db_session.execute(
            select(SubAgentSession).where(SubAgentSession.task_id == task.id)
        )
    ).scalars().all()
    assert rows == []
    assert im.broadcaster.events == []


@pytest.mark.asyncio
async def test_missing_tool_use_id_ignored(im, db_session):
    task = await _create_native_task(
        db_session,
        retry_count=5,
        turn_generation=29,
    )
    await im._upsert_native_sub_agent(
        task.id,
        "subagent_spawn",
        {"kind": "native-agent"},
        task_retry_count=task.retry_count,
        task_turn_generation=task.turn_generation,
    )
    rows = (
        await db_session.execute(select(SubAgentSession))
    ).scalars().all()
    assert rows == []


def _codex_lifecycle(
    *,
    sequence: int,
    status: str,
    summary: str | None = None,
) -> dict:
    return {
        "tool_use_id": "codex:thread-child",
        "native_agent_id": "thread-child",
        "provider": "codex",
        "kind": "native-agent",
        "root_thread_id": "thread-root",
        "parent_native_agent_id": "thread-root",
        "description": "inspect scheduler",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
        "status": status,
        "summary": summary,
        "sequence": sequence,
    }


@pytest.mark.asyncio
async def test_codex_native_thread_is_durable_and_sequence_idempotent(
    im,
    db_session,
):
    task = await _create_native_task(
        db_session,
        retry_count=7,
        turn_generation=41,
    )
    generation = {
        "task_retry_count": task.retry_count,
        "task_turn_generation": task.turn_generation,
    }

    spawn = _codex_lifecycle(sequence=1, status="running")
    await im._upsert_native_sub_agent(
        task.id,
        "subagent_spawn",
        spawn,
        **generation,
    )
    # An exact replay cannot create a second row or duplicate a WebSocket edge.
    broadcast_count = len(im.broadcaster.events)
    await im._upsert_native_sub_agent(
        task.id,
        "subagent_spawn",
        spawn,
        **generation,
    )
    assert len(im.broadcaster.events) == broadcast_count

    row = (
        await db_session.execute(
            select(SubAgentSession).where(SubAgentSession.task_id == task.id)
        )
    ).scalar_one()
    assert row.provider == "codex"
    assert row.codex_thread_id == "thread-child"
    assert row.codex_effort_level == "high"
    assert row.model == "gpt-5.6-sol"
    assert row.status == "running"
    assert json.loads(row.meta)["owner_turn_generation"] == 41
    assert json.loads(row.meta)["last_sequence"] == 1
    await db_session.refresh(task)
    assert task.active_sub_agents == 1

    await im._upsert_native_sub_agent(
        task.id,
        "subagent_progress",
        _codex_lifecycle(
            sequence=2,
            status="running",
            summary="scheduler inspected",
        ),
        **generation,
    )
    await im._upsert_native_sub_agent(
        task.id,
        "subagent_done",
        _codex_lifecycle(
            sequence=3,
            status="failed",
            summary="child failed safely",
        ),
        **generation,
    )
    await db_session.refresh(row)
    assert row.status == "failed"
    assert row.completed_at is not None
    assert row.last_summary == "child failed safely"
    assert row.checks_done == 2
    await db_session.refresh(task)
    assert task.active_sub_agents == 0

    reports = list((
        await db_session.execute(
            select(SubAgentReport)
            .where(SubAgentReport.session_id == row.id)
            .order_by(SubAgentReport.check_number)
        )
    ).scalars())
    assert [(report.status, report.summary) for report in reports] == [
        ("running", "scheduler inspected"),
        ("failed", "child failed safely"),
    ]
    task_count_events = [
        data
        for channel, data in im.broadcaster.events
        if channel == "tasks" and data.get("event") == "sub_agent_count"
    ]
    assert [event["active_sub_agents"] for event in task_count_events] == [1, 0]


@pytest.mark.asyncio
async def test_codex_terminal_only_observation_creates_history_row(
    im,
    db_session,
):
    task = await _create_native_task(
        db_session,
        retry_count=8,
        turn_generation=43,
    )
    await im._upsert_native_sub_agent(
        task.id,
        "subagent_done",
        _codex_lifecycle(
            sequence=1,
            status="cancelled",
            summary="closed before listener attachment",
        ),
        task_retry_count=task.retry_count,
        task_turn_generation=task.turn_generation,
    )

    row = (
        await db_session.execute(
            select(SubAgentSession).where(SubAgentSession.task_id == task.id)
        )
    ).scalar_one()
    assert row.provider == "codex"
    assert row.status == "cancelled"
    assert row.completed_at is not None
    assert row.last_summary == "closed before listener attachment"
    task_events = [
        data["event_type"]
        for channel, data in im.broadcaster.events
        if channel == f"task:{task.id}"
    ]
    assert task_events == ["sub_agent_session_status"]


@pytest.mark.asyncio
async def test_codex_native_thread_newer_spawn_reactivates_same_row(
    im,
    db_session,
):
    task = await _create_native_task(
        db_session,
        retry_count=8,
        turn_generation=44,
    )
    generation = {
        "task_retry_count": task.retry_count,
        "task_turn_generation": task.turn_generation,
    }
    await im._upsert_native_sub_agent(
        task.id,
        "subagent_spawn",
        _codex_lifecycle(sequence=1, status="running"),
        **generation,
    )
    await im._upsert_native_sub_agent(
        task.id,
        "subagent_done",
        _codex_lifecycle(sequence=2, status="completed"),
        **generation,
    )
    await im._upsert_native_sub_agent(
        task.id,
        "subagent_spawn",
        _codex_lifecycle(sequence=3, status="running"),
        **generation,
    )

    row = (
        await db_session.execute(
            select(SubAgentSession).where(SubAgentSession.task_id == task.id)
        )
    ).scalar_one()
    assert row.status == "running"
    assert row.completed_at is None
    assert json.loads(row.meta)["last_sequence"] == 3
    count_events = [
        data["active_sub_agents"]
        for channel, data in im.broadcaster.events
        if channel == "tasks" and data.get("event") == "sub_agent_count"
    ]
    assert count_events == [1, 0, 1]


@pytest.mark.asyncio
async def test_codex_thread_identity_is_scoped_to_owner_turn(
    im,
    db_session,
):
    task = await _create_native_task(
        db_session,
        retry_count=9,
        turn_generation=47,
    )
    await im._upsert_native_sub_agent(
        task.id,
        "subagent_spawn",
        _codex_lifecycle(sequence=1, status="running"),
        task_retry_count=9,
        task_turn_generation=47,
    )

    task.turn_generation = 48
    await db_session.commit()
    await im._upsert_native_sub_agent(
        task.id,
        "subagent_spawn",
        _codex_lifecycle(sequence=1, status="running"),
        task_retry_count=9,
        task_turn_generation=48,
    )

    rows = list((
        await db_session.execute(
            select(SubAgentSession)
            .where(SubAgentSession.task_id == task.id)
            .order_by(SubAgentSession.id)
        )
    ).scalars())
    assert len(rows) == 2
    assert [
        json.loads(row.meta)["owner_turn_generation"] for row in rows
    ] == [47, 48]


@pytest.mark.asyncio
async def test_codex_native_identity_fails_closed_on_malformed_history(
    im,
    db_session,
):
    task = await _create_native_task(
        db_session,
        retry_count=10,
        turn_generation=53,
    )
    malformed = SubAgentSession(
        task_id=task.id,
        agent_type="native-agent",
        source="native",
        provider="codex",
        description="ambiguous history",
        status="running",
        codex_thread_id="thread-child",
        meta="{}",
    )
    db_session.add(malformed)
    await db_session.commit()

    await im._upsert_native_sub_agent(
        task.id,
        "subagent_spawn",
        _codex_lifecycle(sequence=1, status="running"),
        task_retry_count=task.retry_count,
        task_turn_generation=task.turn_generation,
    )

    rows = list((
        await db_session.execute(
            select(SubAgentSession).where(SubAgentSession.task_id == task.id)
        )
    ).scalars())
    assert [row.id for row in rows] == [malformed.id]
    assert im.broadcaster.events == []


# ------------------------------------------------------------- summary API


@pytest.mark.asyncio
async def test_summary_groups_by_agent_type(client, db_session):
    task = Task(title="t", description="d")
    db_session.add(task)
    await db_session.commit()
    await db_session.refresh(task)

    db_session.add_all([
        SubAgentSession(task_id=task.id, description="m1"),  # monitor/ccm running
        SubAgentSession(
            task_id=task.id, description="m2", status="completed"
        ),
        SubAgentSession(
            task_id=task.id, agent_type="native-agent", source="native",
            description="n1", status="running",
        ),
        SubAgentSession(
            task_id=task.id, agent_type="native-monitor", source="native",
            description="n2", status="completed",
        ),
    ])
    await db_session.commit()

    resp = await client.get(f"/api/tasks/{task.id}/sub-agents/summary")
    assert resp.status_code == 200
    by_type = resp.json()["by_type"]
    assert by_type["monitor"] == {"running": 1, "completed": 1}
    assert by_type["native-agent"] == {"running": 1, "completed": 0}
    assert by_type["native-monitor"] == {"running": 0, "completed": 1}


@pytest.mark.asyncio
async def test_unified_read_model_includes_native_sessions_and_reports(
    client,
    db_session,
):
    task = Task(title="unified sub-agent read model", description="d")
    db_session.add(task)
    await db_session.flush()
    monitor = SubAgentSession(
        task_id=task.id,
        agent_type="monitor",
        source="ccm",
        provider="claude",
        description="watch build",
        status="running",
    )
    native = SubAgentSession(
        task_id=task.id,
        agent_type="native-agent",
        source="native",
        provider="codex",
        description="inspect scheduler",
        status="running",
        checks_done=1,
        last_summary="scheduler inspected",
        codex_thread_id="thread-child",
    )
    db_session.add_all([monitor, native])
    await db_session.flush()
    db_session.add(SubAgentReport(
        session_id=native.id,
        check_number=1,
        status="running",
        summary="scheduler inspected",
    ))
    await db_session.commit()

    sessions = await client.get(
        f"/api/tasks/{task.id}/sub-agents/sessions"
    )
    assert sessions.status_code == 200
    by_id = {row["id"]: row for row in sessions.json()}
    assert set(by_id) == {monitor.id, native.id}
    assert by_id[native.id]["agent_type"] == "native-agent"
    assert by_id[native.id]["source"] == "native"
    assert by_id[native.id]["provider"] == "codex"
    assert by_id[native.id]["last_summary"] == "scheduler inspected"

    reports = await client.get(
        f"/api/tasks/{task.id}/sub-agents/sessions/{native.id}/reports"
    )
    assert reports.status_code == 200
    assert [
        (row["check_number"], row["status"], row["summary"])
        for row in reports.json()
    ] == [(1, "running", "scheduler inspected")]


# ------------------------------------------------- no auto-resume on done


@pytest.mark.asyncio
async def test_native_done_does_not_enqueue_auto_resume(im, db_session):
    """subagent_done 绝不往 dispatcher 队列投递唤醒 prompt（2026-07-15 事故）。

    PTY 模式下 harness 自己的 task-notification 已在完成瞬间唤醒 session；
    这里再 enqueue 会和通知 turn 赛跑，输了被 CLI 吸收成 mid-turn steering
    （queue-op remove、无独立回显），send_prompt 的回显锁定永不成立 →
    consumer 永挂 → 队列冻结 → 7200s 超时杀掉仍在干活的进程。
    """
    import sys
    import types
    from unittest.mock import AsyncMock, MagicMock, patch

    fake_main = types.ModuleType("backend.main")
    fake_main.dispatcher = MagicMock()
    fake_main.dispatcher.enqueue_message = AsyncMock()

    task = await _create_native_task(
        db_session,
        retry_count=6,
        turn_generation=31,
    )
    generation = {
        "task_retry_count": task.retry_count,
        "task_turn_generation": task.turn_generation,
    }
    info = {
        "tool_use_id": "toolu_done",
        "kind": "native-agent",
        "description": "查文献",
    }
    with patch.dict(sys.modules, {"backend.main": fake_main}):
        await im._upsert_native_sub_agent(
            task.id,
            "subagent_spawn",
            info,
            **generation,
        )
        await im._upsert_native_sub_agent(
            task.id,
            "subagent_done",
            {**info, "summary": "batch done"},
            **generation,
        )

    fake_main.dispatcher.enqueue_message.assert_not_called()

    row = (
        await db_session.execute(
            select(SubAgentSession).where(SubAgentSession.task_id == task.id)
        )
    ).scalars().first()
    assert row.status == "completed"
    assert all(
        event["task_retry_count"] == task.retry_count
        and event["task_turn_generation"] == task.turn_generation
        for _, event in im.broadcaster.events
    )
