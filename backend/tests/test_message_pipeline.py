"""Tests for the message pipeline: stream parsing → DB storage → broadcast → chat API.

Covers edge cases in StreamParser, InstanceManager._consume_output, and chat history API
that can cause messages to be lost or malformed.
"""
import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from sqlalchemy import select

from backend.services.stream_parser import StreamParser
from backend.services.instance_manager import InstanceManager
from backend.models.instance import Instance
from backend.models.task import Task
from backend.models.log_entry import LogEntry


# ========== StreamParser edge cases ==========


@pytest.fixture
def parser():
    return StreamParser()


class TestStreamParserToolResultContent:
    """tool_result content can be string, list of content blocks, or nested."""

    def test_tool_result_string_content(self, parser):
        """tool_result with simple string content."""
        line = json.dumps({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "simple output"}],
            },
        })
        result = parser.parse_line(line)[0]
        assert result["tool_output"] == "simple output"
        assert isinstance(result["tool_output"], str)

    def test_tool_result_list_content(self, parser):
        """tool_result with list of content blocks (e.g., from Read tool)."""
        line = json.dumps({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "t1",
                    "content": [
                        {"type": "text", "text": "line 1"},
                        {"type": "text", "text": "line 2"},
                    ],
                }],
            },
        })
        result = parser.parse_line(line)[0]
        assert result["tool_output"] == "line 1\nline 2"
        assert isinstance(result["tool_output"], str)

    def test_tool_result_list_content_single_block(self, parser):
        """tool_result with a single content block in list."""
        line = json.dumps({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "t1",
                    "content": [{"type": "text", "text": "single block output"}],
                }],
            },
        })
        result = parser.parse_line(line)[0]
        assert result["tool_output"] == "single block output"

    def test_tool_result_empty_list_content(self, parser):
        """tool_result with empty list content."""
        line = json.dumps({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "t1",
                    "content": [],
                }],
            },
        })
        result = parser.parse_line(line)[0]
        # Empty list has no text blocks, falls back to str()
        assert isinstance(result["tool_output"], str)

    def test_tool_result_missing_content(self, parser):
        """tool_result with no content field."""
        line = json.dumps({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "t1"}],
            },
        })
        result = parser.parse_line(line)[0]
        assert result["tool_output"] == ""

    def test_tool_result_list_with_non_text_blocks(self, parser):
        """tool_result content list with image blocks (non-text) should be handled."""
        line = json.dumps({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "t1",
                    "content": [
                        {"type": "image", "source": {"type": "base64"}},
                        {"type": "text", "text": "caption"},
                    ],
                }],
            },
        })
        result = parser.parse_line(line)[0]
        # Should extract only text blocks
        assert result["tool_output"] == "caption"


class TestStreamParserAssistantEdgeCases:
    """Edge cases in assistant message parsing."""

    def test_assistant_non_dict_content_blocks(self, parser):
        """assistant event with non-dict items in content blocks."""
        line = json.dumps({
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": ["not a dict", {"type": "text", "text": "valid"}],
            },
        })
        results = parser.parse_line(line)
        assert len(results) == 1
        assert results[0]["content"] == "valid"

    def test_assistant_content_not_list(self, parser):
        """assistant event where content is a string, not a list."""
        line = json.dumps({
            "type": "assistant",
            "content": "direct string",
        })
        results = parser.parse_line(line)
        assert len(results) == 1
        assert results[0]["event_type"] == "message"
        assert results[0]["role"] == "assistant"

    def test_assistant_only_tool_use_no_text(self, parser):
        """assistant event with only tool_use blocks, no text."""
        line = json.dumps({
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "ls"}},
                    {"type": "tool_use", "id": "t2", "name": "Read", "input": {"file_path": "/tmp/x"}},
                ],
            },
        })
        results = parser.parse_line(line)
        assert len(results) == 2
        assert all(r["event_type"] == "tool_use" for r in results)

    def test_assistant_text_thinking_tool_mixed(self, parser):
        """assistant event with text + thinking + tool_use mixed."""
        line = json.dumps({
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "thinking", "thinking": "Let me think..."},
                    {"type": "text", "text": "I'll edit the file."},
                    {"type": "tool_use", "id": "t1", "name": "Edit", "input": {}},
                ],
            },
        })
        results = parser.parse_line(line)
        assert len(results) == 3
        assert results[0]["event_type"] == "thinking"
        assert results[1]["event_type"] == "message"
        assert results[2]["event_type"] == "tool_use"

    def test_assistant_empty_text_block(self, parser):
        """assistant event with text block that has empty string."""
        line = json.dumps({
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": ""}],
            },
        })
        results = parser.parse_line(line)
        assert len(results) == 1
        assert results[0]["content"] == ""


class TestStreamParserResultEdgeCases:
    """Edge cases in result event parsing."""

    def test_result_no_content(self, parser):
        """result event with no content field."""
        line = json.dumps({
            "type": "result",
            "session_id": "s1",
        })
        result = parser.parse_line(line)[0]
        assert result["event_type"] == "result"
        assert result["session_id"] == "s1"
        assert result["content"] is None

    def test_result_with_list_content(self, parser):
        """result event with list content blocks."""
        line = json.dumps({
            "type": "result",
            "session_id": "s1",
            "total_cost_usd": 0.1,
            "content": [{"type": "text", "text": "Final summary"}],
        })
        result = parser.parse_line(line)[0]
        assert result["content"] == "Final summary"
        assert result["cost_usd"] == 0.1

    def test_result_no_cost(self, parser):
        """result event without cost should not set cost_usd."""
        line = json.dumps({
            "type": "result",
            "session_id": "s1",
            "content": "done",
        })
        result = parser.parse_line(line)[0]
        assert "cost_usd" not in result


class TestStreamParserSystemEvents:
    """System event edge cases."""

    def test_system_init_no_session_id(self, parser):
        """system init without session_id."""
        line = json.dumps({"type": "system", "subtype": "init"})
        result = parser.parse_line(line)[0]
        assert result["event_type"] == "system_init"
        assert result["session_id"] is None

    def test_system_heartbeat(self, parser):
        """system heartbeat (task_progress) event."""
        line = json.dumps({"type": "system", "subtype": "task_progress"})
        result = parser.parse_line(line)[0]
        assert result["event_type"] == "system_event"
        assert result["content"] == "task_progress"


# ========== InstanceManager._consume_output tests ==========


def test_parse_codex_background_lifecycle_event():
    im = InstanceManager(MagicMock(), MagicMock())

    event = im._parse_codex_line(json.dumps({
        "type": "background.lifecycle",
        "state": "running",
        "reason": "waiting_for_descendants",
        "active_count": 2,
        "active_thread_ids": ["child-a", "child-b"],
        "started_at": "2026-08-13T12:00:00",
        "last_activity_at": "2026-08-13T12:01:00",
    }))

    assert event is not None
    assert event["event_type"] == "background_lifecycle"
    assert event["background_state"] == "running"
    assert event["background_reason"] == "waiting_for_descendants"
    assert event["background_active_count"] == 2
    assert event["background_active_thread_ids"] == ["child-a", "child-b"]
    assert event["background_started_at"] == "2026-08-13T12:00:00"
    assert event["background_last_activity_at"] == "2026-08-13T12:01:00"


def test_parse_codex_native_sub_agent_lifecycle_event():
    im = InstanceManager(MagicMock(), MagicMock())

    event = im._parse_codex_line(json.dumps({
        "type": "native.subagent.lifecycle",
        "lifecycle_event": "spawn",
        "provider": "codex",
        "native_agent_id": "thread-child",
        "root_thread_id": "thread-root",
        "parent_native_agent_id": "thread-root",
        "description": "inspect the scheduler",
        "agent_path": "/root/scheduler",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
        "status": "running",
        "summary": None,
        "sequence": 1,
    }))

    assert event is not None
    assert event["event_type"] == "subagent_spawn"
    assert event["role"] == "system"
    assert event["content"] is None
    assert event["subagent"] == {
        "tool_use_id": "codex:thread-child",
        "native_agent_id": "thread-child",
        "provider": "codex",
        "kind": "native-agent",
        "status": "running",
        "sequence": 1,
        "root_thread_id": "thread-root",
        "parent_native_agent_id": "thread-root",
        "description": "inspect the scheduler",
        "agent_path": "/root/scheduler",
        "model": "gpt-5.6-sol",
        "reasoning_effort": "high",
    }


@pytest.mark.parametrize(
    "override",
    [
        {"provider": "claude"},
        {"native_agent_id": ""},
        {"native_agent_id": " child "},
        {"sequence": 0},
        {"sequence": True},
        {"lifecycle_event": "done", "status": "running"},
        {"lifecycle_event": "spawn", "status": "failed"},
        {"description": "x" * 501},
    ],
)
def test_parse_codex_native_sub_agent_lifecycle_rejects_malformed(override):
    im = InstanceManager(MagicMock(), MagicMock())
    payload = {
        "type": "native.subagent.lifecycle",
        "lifecycle_event": "spawn",
        "provider": "codex",
        "native_agent_id": "thread-child",
        "description": "child",
        "status": "running",
        "sequence": 1,
    }
    payload.update(override)

    assert im._parse_codex_line(json.dumps(payload)) is None


def _make_mock_process_with_output(lines: list[str], exit_code: int = 0):
    """Create a mock process that yields NDJSON lines from stdout."""
    proc = MagicMock()
    proc.pid = 12345
    proc.returncode = None  # Start as running

    line_iter = iter(lines + [b""])  # End with empty bytes (EOF)

    async def readline():
        line = next(line_iter)
        if isinstance(line, str):
            return line.encode("utf-8")
        return line

    proc.stdout = MagicMock()
    proc.stdout.readline = readline

    async def read_stderr():
        return b""
    proc.stderr = MagicMock()
    proc.stderr.read = read_stderr

    async def wait():
        proc.returncode = exit_code
        return exit_code
    proc.wait = wait

    proc.terminate = MagicMock()
    proc.kill = MagicMock()

    return proc


@pytest.mark.asyncio
async def test_consume_output_stores_all_events(db_factory):
    """_consume_output should store every parsed event as a LogEntry."""
    async with db_factory() as db:
        inst = Instance(name="test")
        task = Task(title="t", description="d")
        db.add(inst)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id, task_id = inst.id, task.id

    ndjson_lines = [
        json.dumps({"type": "system", "subtype": "init", "session_id": "sess-1"}) + "\n",
        json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Hello"}]}}) + "\n",
        json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "ls"}}]}}) + "\n",
        json.dumps({"type": "user", "message": {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "file1\nfile2"}]}}) + "\n",
        json.dumps({"type": "result", "session_id": "sess-1", "total_cost_usd": 0.05, "content": "Done"}) + "\n",
    ]

    mock_proc = _make_mock_process_with_output(ndjson_lines)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    await im._consume_output(inst_id, task_id, mock_proc)

    # Check all events stored in DB
    async with db_factory() as db:
        from sqlalchemy import select
        result = await db.execute(
            select(LogEntry).where(LogEntry.task_id == task_id).order_by(LogEntry.id)
        )
        entries = result.scalars().all()

    assert len(entries) == 5
    assert entries[0].event_type == "system_init"
    assert entries[1].event_type == "message"
    assert entries[1].content == "Hello"
    assert entries[2].event_type == "tool_use"
    assert entries[2].tool_name == "Bash"
    assert entries[3].event_type == "tool_result"
    assert entries[3].tool_output == "file1\nfile2"
    assert entries[4].event_type == "result"


@pytest.mark.asyncio
async def test_consume_output_saves_session_id(db_factory):
    """_consume_output should save session_id from system_init to Task."""
    async with db_factory() as db:
        inst = Instance(name="test")
        task = Task(title="t", description="d")
        db.add(inst)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id, task_id = inst.id, task.id

    ndjson_lines = [
        json.dumps({"type": "system", "subtype": "init", "session_id": "my-session-42"}) + "\n",
    ]

    mock_proc = _make_mock_process_with_output(ndjson_lines)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    await im._consume_output(inst_id, task_id, mock_proc)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.session_id == "my-session-42"


@pytest.mark.asyncio
async def test_consume_output_broadcasts_to_both_channels(db_factory):
    """_consume_output should broadcast to both instance:{id} and task:{id}."""
    async with db_factory() as db:
        inst = Instance(name="test")
        task = Task(title="t", description="d")
        db.add(inst)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id, task_id = inst.id, task.id

    ndjson_lines = [
        json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Hi"}]}}) + "\n",
    ]

    mock_proc = _make_mock_process_with_output(ndjson_lines)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    await im._consume_output(inst_id, task_id, mock_proc)

    # Should have broadcast to both channels + process_exit events
    calls = broadcaster.broadcast.call_args_list
    channels_called = [c[0][0] for c in calls]
    assert f"instance:{inst_id}" in channels_called
    assert f"task:{task_id}" in channels_called


@pytest.mark.asyncio
async def test_consume_output_marks_legacy_tool_markup_as_inert_text(db_factory):
    async with db_factory() as db:
        inst = Instance(name="test")
        task = Task(title="t", description="d")
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id, task_id = inst.id, task.id

    leaked_text = (
        'card\n<invoke name="Bash">\n'
        '<parameter name="command">pwd</parameter>\n</invoke>'
    )
    mock_proc = _make_mock_process_with_output([
        json.dumps({
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": leaked_text}],
                "stop_reason": "end_turn",
            },
        }) + "\n",
    ])
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    await im._consume_output(inst_id, task_id, mock_proc)

    task_messages = [
        call.args[1]
        for call in broadcaster.broadcast.call_args_list
        if call.args[0] == f"task:{task_id}"
        and call.args[1].get("event_type") == "message"
    ]
    assert len(task_messages) == 1
    assert task_messages[0]["content"] == leaked_text
    assert task_messages[0]["protocol_anomaly"] == "legacy_tool_markup"
    assert task_messages[0]["tool_name"] is None


@pytest.mark.asyncio
async def test_consume_output_continues_after_parse_error(db_factory):
    """_consume_output should skip bad lines and continue processing."""
    async with db_factory() as db:
        inst = Instance(name="test")
        task = Task(title="t", description="d")
        db.add(inst)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id, task_id = inst.id, task.id

    ndjson_lines = [
        "not valid json\n",
        json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "After error"}]}}) + "\n",
    ]

    mock_proc = _make_mock_process_with_output(ndjson_lines)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    await im._consume_output(inst_id, task_id, mock_proc)

    # Should have stored 2 events: parse_error + message
    async with db_factory() as db:
        from sqlalchemy import select
        result = await db.execute(
            select(LogEntry).where(LogEntry.task_id == task_id).order_by(LogEntry.id)
        )
        entries = result.scalars().all()

    assert len(entries) == 2
    assert entries[0].event_type == "parse_error"
    assert entries[1].event_type == "message"
    assert entries[1].content == "After error"


@pytest.mark.asyncio
async def test_consume_output_broadcasts_process_exit(db_factory):
    """_consume_output should broadcast process_exit with exit code."""
    async with db_factory() as db:
        inst = Instance(name="test")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process_with_output([], exit_code=1)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    await im._consume_output(inst_id, None, mock_proc)

    # Find process_exit broadcast
    exit_calls = [
        c for c in broadcaster.broadcast.call_args_list
        if isinstance(c[0][1], dict) and c[0][1].get("event_type") == "process_exit"
    ]
    assert len(exit_calls) >= 1
    assert exit_calls[0][0][1]["exit_code"] == 1


@pytest.mark.asyncio
async def test_consume_output_saves_cost(db_factory):
    """_consume_output should save cost from result event to Instance."""
    async with db_factory() as db:
        inst = Instance(name="test")
        task = Task(title="t", description="d")
        db.add(inst)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id, task_id = inst.id, task.id

    ndjson_lines = [
        json.dumps({"type": "result", "session_id": "s1", "total_cost_usd": 1.23, "content": "done"}) + "\n",
    ]

    mock_proc = _make_mock_process_with_output(ndjson_lines)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    await im._consume_output(inst_id, task_id, mock_proc)

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.total_cost_usd == 1.23


@pytest.mark.asyncio
async def test_consume_output_no_task_id_skips_task_broadcast(db_factory):
    """When task_id is None, should only broadcast to instance channel."""
    async with db_factory() as db:
        inst = Instance(name="test")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    ndjson_lines = [
        json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "Hi"}]}}) + "\n",
    ]

    mock_proc = _make_mock_process_with_output(ndjson_lines)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    await im._consume_output(inst_id, None, mock_proc)

    # Should NOT have any task:* broadcasts (only instance:* and system)
    calls = broadcaster.broadcast.call_args_list
    task_calls = [c for c in calls if c[0][0].startswith("task:")]
    assert len(task_calls) == 0


# ========== Chat History API tests ==========


@pytest.mark.asyncio
async def test_chat_history_filters_heartbeats(client, session_factory):
    """Chat history should filter out system_event with content=task_progress."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
        "provider": "claude",
    })
    task_id = create_resp.json()["id"]

    async with session_factory() as db:
        # Insert a heartbeat and a real message
        db.add(LogEntry(
            instance_id=1, task_id=task_id,
            event_type="system_event", content="task_progress", is_error=False,
        ))
        db.add(LogEntry(
            instance_id=1, task_id=task_id,
            task_retry_count=7, task_turn_generation=11,
            native_turn_id="native-turn-11",
            event_type="message", role="assistant",
            content="Hello", is_error=False,
            raw_json='{"turn_id":"legacy-turn"}',
        ))
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}/chat/history")
    msgs = resp.json()
    assert len(msgs) == 1
    assert msgs[0]["content"] == "Hello"
    assert msgs[0]["task_retry_count"] == 7
    assert msgs[0]["task_turn_generation"] == 11
    assert msgs[0]["native_turn_id"] == "native-turn-11"
    assert msgs[0]["turn_id"] == "native-turn-11"


@pytest.mark.asyncio
async def test_chat_history_marks_legacy_tool_markup_anomaly(
    client,
    session_factory,
):
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
        "provider": "claude",
    })
    task_id = create_resp.json()["id"]
    leaked_text = (
        '<invoke name="Bash">'
        '<parameter name="command">pwd</parameter>'
        '</invoke>'
    )

    async with session_factory() as db:
        db.add_all([
            LogEntry(
                instance_id=1,
                task_id=task_id,
                event_type="message",
                role="assistant",
                content=leaked_text,
            ),
            LogEntry(
                instance_id=1,
                task_id=task_id,
                event_type="message",
                role="assistant",
                content="A normal reply",
            ),
        ])
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}/chat/history")
    assert resp.status_code == 200
    messages = resp.json()
    assert messages[0]["content"] == leaked_text
    assert messages[0]["protocol_anomaly"] == "legacy_tool_markup"
    assert messages[1]["protocol_anomaly"] is None


@pytest.mark.asyncio
async def test_chat_history_ignores_codex_xml_example(
    client,
    session_factory,
):
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
        "provider": "codex",
    })
    task_id = create_resp.json()["id"]
    xml_example = (
        '<function_calls><invoke name="Bash">'
        '<parameter name="command">pwd</parameter>'
        '</invoke></function_calls>'
    )

    async with session_factory() as db:
        db.add(
            LogEntry(
                instance_id=1,
                task_id=task_id,
                event_type="message",
                role="assistant",
                content=xml_example,
            )
        )
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}/chat/history")
    assert resp.status_code == 200
    assert resp.json()[0]["protocol_anomaly"] is None


@pytest.mark.asyncio
async def test_shared_history_marks_legacy_tool_markup_anomaly(
    session_factory,
):
    from backend.api.shared_access import shared_history
    from backend.models.task_share import TaskShare

    leaked_text = (
        '<function_calls><invoke name="Read">'
        '<parameter name="file_path">/tmp/a</parameter>'
        '</invoke></function_calls>'
    )

    async with session_factory() as db:
        task = Task(
            title="T",
            description="d",
            target_repo="/tmp",
            provider="claude",
        )
        db.add(task)
        await db.flush()
        task_id = task.id
        db.add(
            TaskShare(
                task_id=task_id,
                shared_to_open_id="protocol-anomaly-recipient",
                shared_to_name="Remote Reader",
                shared_to_ccm_url="https://receiver.test",
                share_token="protocol-anomaly-share-token",
                status="active",
            )
        )
        db.add(
            LogEntry(
                instance_id=1,
                task_id=task_id,
                event_type="message",
                role="assistant",
                content=leaked_text,
            )
        )
        await db.commit()

    async with session_factory() as db:
        messages = await shared_history(
            task_id,
            token="protocol-anomaly-share-token",
            limit=0,
            before_id=0,
            compact=True,
            db=db,
        )
    assert messages[0]["protocol_anomaly"] == "legacy_tool_markup"


@pytest.mark.asyncio
async def test_chat_history_strictly_filters_legacy_codex_collab_completed(
    client,
    session_factory,
):
    """Only the proven historical collab item shape is compatibility noise."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    def raw_item(
        item_type: str,
        *,
        outer_type: str = "item.completed",
        status: str = "completed",
    ) -> str:
        return json.dumps({
            "type": outer_type,
            "item": {
                "id": f"{item_type}-{outer_type}-{status}",
                "type": item_type,
                "tool": "wait",
                "status": status,
            },
        })

    async with session_factory() as db:
        for item_type in (
            "collabAgentToolCall",
            "collab_agent_tool_call",
        ):
            db.add(LogEntry(
                instance_id=1,
                task_id=task_id,
                event_type="system_event",
                content="completed",
                raw_json=raw_item(item_type),
                is_error=False,
            ))
        # Near misses remain visible: text alone is not enough, and every raw
        # discriminator must match before the compatibility filter applies.
        db.add_all([
            LogEntry(
                instance_id=1,
                task_id=task_id,
                event_type="system_event",
                content="completed",
                tool_name="bare",
                is_error=False,
            ),
            LogEntry(
                instance_id=1,
                task_id=task_id,
                event_type="system_event",
                content="completed",
                tool_name="failed",
                raw_json=raw_item("collabAgentToolCall", status="failed"),
                is_error=False,
            ),
            LogEntry(
                instance_id=1,
                task_id=task_id,
                event_type="system_event",
                content="completed",
                tool_name="wrong-item",
                raw_json=raw_item("commandExecution"),
                is_error=False,
            ),
            LogEntry(
                instance_id=1,
                task_id=task_id,
                event_type="system_event",
                content="completed",
                tool_name="wrong-lifecycle",
                raw_json=raw_item(
                    "collabAgentToolCall",
                    outer_type="item.started",
                ),
                is_error=False,
            ),
        ])
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}/chat/history")
    assert resp.status_code == 200
    msgs = resp.json()
    assert [message["tool_name"] for message in msgs] == [
        "bare",
        "failed",
        "wrong-item",
        "wrong-lifecycle",
    ]


@pytest.mark.asyncio
async def test_legacy_codex_collab_noise_does_not_consume_history_limit(
    client,
    session_factory,
):
    """A dense historical noise run must not hide the older visible page."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    async with session_factory() as db:
        for index in range(3):
            db.add(LogEntry(
                instance_id=1,
                task_id=task_id,
                event_type="message",
                role="assistant",
                content=f"visible-{index}",
                is_error=False,
            ))
        for index in range(625):
            db.add(LogEntry(
                instance_id=1,
                task_id=task_id,
                event_type="system_event",
                content="completed",
                raw_json=json.dumps({
                    "type": "item.completed",
                    "item": {
                        "id": f"collab-{index}",
                        "type": "collabAgentToolCall",
                        "tool": "wait",
                        "status": "completed",
                    },
                }),
                is_error=False,
            ))
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}/chat/history?limit=3")
    assert resp.status_code == 200
    assert [message["content"] for message in resp.json()] == [
        "visible-0",
        "visible-1",
        "visible-2",
    ]


@pytest.mark.asyncio
async def test_chat_history_includes_all_event_types(client, session_factory):
    """Chat history should include message, tool_use, tool_result, thinking, system_init."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    async with session_factory() as db:
        entries = [
            LogEntry(instance_id=1, task_id=task_id, event_type="system_init", is_error=False),
            LogEntry(instance_id=1, task_id=task_id, event_type="thinking", role="assistant", content="hmm", is_error=False),
            LogEntry(instance_id=1, task_id=task_id, event_type="message", role="assistant", content="Hi", is_error=False),
            LogEntry(instance_id=1, task_id=task_id, event_type="tool_use", role="assistant", tool_name="Bash", tool_input='{"command":"ls"}', is_error=False),
            LogEntry(instance_id=1, task_id=task_id, event_type="tool_result", role="tool", tool_output="file1", is_error=False),
            LogEntry(instance_id=1, task_id=task_id, event_type="result", role="assistant", content="Done", is_error=False),
            LogEntry(instance_id=1, task_id=task_id, event_type="process_exit", is_error=False),
        ]
        for e in entries:
            db.add(e)
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}/chat/history")
    msgs = resp.json()
    event_types = [m["event_type"] for m in msgs]
    assert "system_init" in event_types
    assert "thinking" in event_types
    assert "message" in event_types
    assert "tool_use" in event_types
    assert "tool_result" in event_types
    assert "result" in event_types
    assert "process_exit" in event_types


@pytest.mark.asyncio
async def test_chat_history_excludes_unwhitelisted_events(client, session_factory):
    """Chat history should not include event types not in the whitelist."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    async with session_factory() as db:
        db.add(LogEntry(
            instance_id=1, task_id=task_id,
            event_type="parse_error", content="bad json", is_error=True,
        ))
        db.add(LogEntry(
            instance_id=1, task_id=task_id,
            event_type="unknown", content="mystery", is_error=False,
        ))
        db.add(LogEntry(
            instance_id=1, task_id=task_id,
            event_type="message", role="assistant", content="visible", is_error=False,
        ))
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}/chat/history")
    msgs = resp.json()
    assert len(msgs) == 1
    assert msgs[0]["content"] == "visible"


@pytest.mark.asyncio
async def test_chat_history_limit(client, session_factory):
    """Chat history should respect the limit parameter."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    async with session_factory() as db:
        for i in range(10):
            db.add(LogEntry(
                instance_id=1, task_id=task_id,
                event_type="message", role="assistant", content=f"msg-{i}", is_error=False,
            ))
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}/chat/history?limit=3")
    msgs = resp.json()
    assert len(msgs) == 3
    # limit returns the most recent N messages in chronological order
    assert msgs[0]["content"] == "msg-7"
    assert msgs[2]["content"] == "msg-9"


@pytest.mark.asyncio
async def test_chat_history_noisy_rows_do_not_consume_limit(client, session_factory):
    """Noisy system_event rows must be excluded before LIMIT, not after.

    Regression: post-query filtering made pages return fewer than `limit`
    messages, which the frontend reads as "history exhausted" — the
    "Load older messages" button disappeared with older history unreachable
    (production task 80: 258 of 631 messages were never displayed).
    """
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    async with session_factory() as db:
        # 6 real messages interleaved with noisy telemetry
        for i in range(6):
            db.add(LogEntry(
                instance_id=1, task_id=task_id,
                event_type="message", role="assistant", content=f"msg-{i}", is_error=False,
            ))
            db.add(LogEntry(
                instance_id=1, task_id=task_id,
                event_type="system_event", content="task_progress", is_error=False,
            ))
        await db.commit()

    # A full page of 4: noisy rows must not eat into the limit
    resp = await client.get(f"/api/tasks/{task_id}/chat/history?limit=4")
    msgs = resp.json()
    assert len(msgs) == 4
    assert [m["content"] for m in msgs] == ["msg-2", "msg-3", "msg-4", "msg-5"]

    # Paginate older: before_id of the oldest returned message
    resp = await client.get(
        f"/api/tasks/{task_id}/chat/history?limit=4&before_id={msgs[0]['id']}"
    )
    older = resp.json()
    assert [m["content"] for m in older] == ["msg-0", "msg-1"]


@pytest.mark.asyncio
async def test_chat_history_no_limit_returns_all(client, session_factory):
    """Default limit=0 should return all messages for a task."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    async with session_factory() as db:
        for i in range(50):
            db.add(LogEntry(
                instance_id=1, task_id=task_id,
                event_type="message", role="assistant", content=f"msg-{i}", is_error=False,
            ))
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}/chat/history")
    msgs = resp.json()
    assert len(msgs) == 50
    assert msgs[0]["content"] == "msg-0"
    assert msgs[49]["content"] == "msg-49"


@pytest.mark.asyncio
async def test_chat_history_explicit_zero_returns_all(client, session_factory):
    """Explicitly passing limit=0 should return all messages."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    async with session_factory() as db:
        for i in range(20):
            db.add(LogEntry(
                instance_id=1, task_id=task_id,
                event_type="message", role="assistant", content=f"m-{i}", is_error=False,
            ))
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}/chat/history?limit=0")
    msgs = resp.json()
    assert len(msgs) == 20


@pytest.mark.asyncio
async def test_chat_history_ordered_by_id(client, session_factory):
    """Chat history should be ordered by ID ascending."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    async with session_factory() as db:
        db.add(LogEntry(instance_id=1, task_id=task_id, event_type="message", role="assistant", content="first", is_error=False))
        db.add(LogEntry(instance_id=1, task_id=task_id, event_type="message", role="assistant", content="second", is_error=False))
        db.add(LogEntry(instance_id=1, task_id=task_id, event_type="message", role="assistant", content="third", is_error=False))
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}/chat/history")
    msgs = resp.json()
    assert [m["content"] for m in msgs] == ["first", "second", "third"]


@pytest.mark.asyncio
async def test_chat_history_correct_role_assignment(client, session_factory):
    """Chat history should assign correct roles based on event_type."""
    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    async with session_factory() as db:
        db.add(LogEntry(instance_id=1, task_id=task_id, event_type="user_message", role="user", content="hi", is_error=False))
        db.add(LogEntry(instance_id=1, task_id=task_id, event_type="message", role="assistant", content="hello", is_error=False))
        db.add(LogEntry(instance_id=1, task_id=task_id, event_type="tool_result", role="tool", tool_output="ok", is_error=False))
        db.add(LogEntry(instance_id=1, task_id=task_id, event_type="system_init", is_error=False))
        await db.commit()

    resp = await client.get(f"/api/tasks/{task_id}/chat/history")
    msgs = resp.json()
    assert msgs[0]["role"] == "user"
    assert msgs[1]["role"] == "assistant"
    assert msgs[2]["role"] == "tool"
    assert msgs[3]["role"] == "system"  # fallback for system_init with no role


def test_client_message_id_is_strict_and_handoff_neutral():
    from backend.api.chat import (
        ChatMessage,
        FrontendReviewGoalMessage,
        _worker_turn_handoff_request_identity,
    )

    client_message_id = "123e4567-e89b-12d3-a456-426614174000"
    baseline = ChatMessage(message="same message")
    correlated = ChatMessage(
        message="same message",
        client_message_id=client_message_id,
    )

    assert correlated.client_message_id == client_message_id
    assert correlated.model_dump(mode="json") == baseline.model_dump(mode="json")
    assert _worker_turn_handoff_request_identity(
        correlated,
        ("codex", "gpt-5.6-sol", "default"),
    ) == _worker_turn_handoff_request_identity(
        baseline,
        ("codex", "gpt-5.6-sol", "default"),
    )
    goal = FrontendReviewGoalMessage(
        message="review",
        client_message_id=client_message_id,
    )
    assert goal.client_message_id == client_message_id
    assert "client_message_id" not in goal.model_dump(mode="json")

    for invalid in (
        "123e4567-e89b-12d3-a456-42661417400",
        "123e4567-e89b-12d3-a456-4266141740000",
        "123E4567-E89B-12D3-A456-426614174000",
        "123e4567e89b12d3a456426614174000",
    ):
        with pytest.raises(ValueError):
            ChatMessage(message="invalid", client_message_id=invalid)
        with pytest.raises(ValueError):
            FrontendReviewGoalMessage(
                message="invalid",
                client_message_id=invalid,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "endpoint",
    ("chat", "frontend-review-goal"),
)
@pytest.mark.parametrize(
    "invalid_client_message_id",
    (
        "123e4567-e89b-12d3-a456-42661417400",
        "123e4567-e89b-12d3-a456-4266141740000",
        "123E4567-E89B-12D3-A456-426614174000",
        "123e4567e89b12d3a456426614174000",
    ),
)
async def test_chat_endpoints_reject_invalid_client_message_id(
    client,
    endpoint,
    invalid_client_message_id,
):
    response = await client.post(
        f"/api/tasks/999999/{endpoint}",
        json={
            "message": "invalid correlation id",
            "client_message_id": invalid_client_message_id,
        },
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_chat_send_stores_user_message(client, session_factory):
    """Sending a chat message should store user_message in DB."""
    from sqlalchemy import select, update

    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    # Set session_id and last_cwd
    async with session_factory() as db:
        await db.execute(
            update(Task).where(Task.id == task_id).values(
                session_id="test-session", last_cwd="/tmp"
            )
        )
        inst = Instance(name="idle-inst", status="idle")
        db.add(inst)
        await db.commit()

    mock_im = MagicMock()
    mock_im.processes = {}
    mock_im.launch = AsyncMock(return_value=42)
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()

    with patch("backend.main.instance_manager", mock_im), \
         patch("backend.main.broadcaster", mock_broadcaster):
        resp = await client.post(f"/api/tasks/{task_id}/chat", json={"message": "test message"})

    assert resp.status_code == 200

    # Verify user_message stored in DB
    async with session_factory() as db:
        result = await db.execute(
            select(LogEntry).where(
                LogEntry.task_id == task_id,
                LogEntry.event_type == "user_message"
            )
        )
        entry = result.scalar_one()
        assert entry.content == "test message"
        assert entry.role == "user"


@pytest.mark.asyncio
async def test_chat_send_broadcasts_user_message(client, session_factory):
    """Sending a chat message should broadcast it to task channel."""
    from sqlalchemy import update

    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    async with session_factory() as db:
        await db.execute(
            update(Task).where(Task.id == task_id).values(
                session_id="test-session", last_cwd="/tmp"
            )
        )
        inst = Instance(name="idle-inst", status="idle")
        db.add(inst)
        await db.commit()

    mock_im = MagicMock()
    mock_im.processes = {}
    mock_im.launch = AsyncMock(return_value=42)
    mock_broadcaster = MagicMock()
    mock_broadcaster.broadcast = AsyncMock()

    with patch("backend.main.instance_manager", mock_im), \
         patch("backend.main.broadcaster", mock_broadcaster):
        await client.post(
            f"/api/tasks/{task_id}/chat",
            json={
                "message": "hello",
                "client_message_id": "11111111-2222-4333-8444-555555555555",
            },
        )

    # Check broadcast was called with user_message on task channel
    broadcast_calls = mock_broadcaster.broadcast.call_args_list
    task_broadcasts = [
        c for c in broadcast_calls
        if c[0][0] == f"task:{task_id}" and c[0][1].get("event_type") == "user_message"
    ]
    assert len(task_broadcasts) == 1
    event = task_broadcasts[0][0][1]
    assert event["content"] == "hello"
    assert event["task_id"] == task_id
    assert isinstance(event["id"], int)
    assert event["id"] > 0
    assert event["timestamp"].endswith("Z")
    assert (
        event["client_message_id"]
        == "11111111-2222-4333-8444-555555555555"
    )

    async with session_factory() as db:
        stored = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task_id,
                    LogEntry.event_type == "user_message",
                )
            )
        ).scalar_one()
    assert event["id"] == stored.id
    assert json.loads(stored.raw_json)["client_message_id"] == (
        "11111111-2222-4333-8444-555555555555"
    )

    history_response = await client.get(
        f"/api/tasks/{task_id}/chat/history?compact=true&limit=200"
    )
    assert history_response.status_code == 200
    history_row = next(
        row
        for row in history_response.json()
        if row["event_type"] == "user_message"
    )
    assert history_row["id"] == stored.id
    assert history_row["timestamp"] == event["timestamp"]
    assert history_row["client_message_id"] == event["client_message_id"]


@pytest.mark.asyncio
async def test_chat_send_returns_conflict_after_shutdown_commit(
    client, session_factory,
):
    from sqlalchemy import update
    from backend.services.dispatcher import TaskStartPausedError

    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]
    async with session_factory() as db:
        await db.execute(
            update(Task).where(Task.id == task_id).values(
                session_id="test-session", last_cwd="/tmp"
            )
        )
        await db.commit()

    dispatcher = MagicMock()
    dispatcher.enqueue_message = AsyncMock(
        side_effect=TaskStartPausedError("shutdown committed")
    )
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    with (
        patch("backend.main.dispatcher", dispatcher),
        patch("backend.main.broadcaster", broadcaster),
    ):
        resp = await client.post(
            f"/api/tasks/{task_id}/chat", json={"message": "too late"}
        )

    assert resp.status_code == 409
    assert "重连后重试" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_context_usage_window_only_merges_into_stored(client, session_factory):
    """result 事件只带 context_window（usage 数字是累计值已被剥离）——
    应合并进已存的 per-request usage，而不是整体覆盖。"""
    from unittest.mock import AsyncMock, MagicMock
    from backend.services.instance_manager import InstanceManager
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    stored = {"input_tokens": 10, "cache_read_input_tokens": 20,
              "cache_creation_input_tokens": 30, "output_tokens": 5,
              "total_input_tokens": 60, "context_window": 200_000}
    async with session_factory() as db:
        t = await db.get(Task, task_id)
        t.context_window_usage = stored
        await db.commit()

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory=session_factory, broadcaster=broadcaster)

    # result 事件：只带 context_window=1M
    await im._process_event(
        instance_id=1, task_id=task_id,
        event={"event_type": "result", "context_usage": {"context_window": 1_000_000}},
    )

    async with session_factory() as db:
        t = await db.get(Task, task_id)
        cu = t.context_window_usage
    assert cu["total_input_tokens"] == 60      # token 数字保留
    assert cu["context_window"] == 1_000_000   # window 被修正


@pytest.mark.asyncio
async def test_opus5_corrects_underreported_context_window(client, session_factory):
    """Opus 5 is always 1M even when Claude reports the legacy 200K window."""
    from unittest.mock import AsyncMock, MagicMock
    from backend.models.task import Task
    from backend.services.instance_manager import InstanceManager

    create_resp = await client.post("/api/tasks", json={
        "title": "Opus 5",
        "description": "d",
        "target_repo": "/tmp",
        "model": "claude-opus-5",
    })
    task_id = create_resp.json()["id"]

    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.context_window_usage = {
            "input_tokens": 10,
            "cache_read_input_tokens": 20,
            "cache_creation_input_tokens": 30,
            "output_tokens": 5,
            "total_input_tokens": 60,
            "context_window": 200_000,
        }
        await db.commit()

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    manager = InstanceManager(
        db_factory=session_factory,
        broadcaster=broadcaster,
    )
    await manager._process_event(
        instance_id=1,
        task_id=task_id,
        event={
            "event_type": "result",
            "context_usage": {"context_window": 200_000},
        },
    )

    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.context_window_usage["context_window"] == 1_000_000


@pytest.mark.asyncio
async def test_context_usage_window_only_without_stored_is_dropped(client, session_factory):
    """没有已存 usage 时，window-only 事件不应写入半截数据。"""
    from unittest.mock import AsyncMock, MagicMock
    from backend.services.instance_manager import InstanceManager
    from backend.models.task import Task

    create_resp = await client.post("/api/tasks", json={
        "title": "T", "description": "d", "target_repo": "/tmp",
    })
    task_id = create_resp.json()["id"]

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory=session_factory, broadcaster=broadcaster)

    await im._process_event(
        instance_id=1, task_id=task_id,
        event={"event_type": "result", "context_usage": {"context_window": 200_000}},
    )

    async with session_factory() as db:
        t = await db.get(Task, task_id)
    assert t.context_window_usage is None
