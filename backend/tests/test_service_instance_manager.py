"""Tests for InstanceManager — subprocess lifecycle management."""
import asyncio
import json
import os
import signal
import sys
import threading
import time
import tomllib
import types
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
import pytest
from sqlalchemy import select
from unittest.mock import AsyncMock, MagicMock, call, patch

from backend.services.instance_manager import (
    CodexLaunchCommitError,
    ConsumerRecoveryUnsettledError,
    InstanceAlreadyRunningError,
    InstanceNotFoundError,
    InstanceManager,
    LaunchSupersededError,
    LiveAttachmentInjectionUnsupportedError,
)
from backend.services.claude_pool import ClaudePool
from backend.services.codex_pool import CodexPool
from backend.services.codex_app_server import (
    CodexAppServerBusyError,
    CodexAppServerError,
    CodexRequiredMcpError,
    CodexRequiredMcpPreTurnError,
    CodexServiceTierUnavailableError,
    CodexSharedTransportBusyError,
    CodexThreadHomeMismatchError,
    CodexThreadIdentityMismatchError,
    CodexThreadTerminalStateError,
    CodexTurnProcess,
)
from backend.services.codex_tier_proxy import CodexTierProxyRoute
from backend.services.mcp_config import (
    McpServerSpec,
    build_browser_review_mcp_server_specs,
    build_mcp_server_specs,
    build_sub_agent_controller_mcp_server_specs,
    render_codex_exec_config_args,
)
from backend.config import Settings, settings
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.task import Task


@pytest.fixture(autouse=True)
def _no_pty_no_skills(monkeypatch):
    """Disable PTY mode and mock discover_skills for all tests in this module."""
    monkeypatch.setattr("backend.config.settings.use_pty_mode", False)
    with patch("backend.services.skill_loader.discover_skills", return_value={}), \
         patch("backend.services.skill_loader.build_skill_prompt_file", return_value=""), \
         patch("backend.services.skill_loader.get_skill_disallowed_tools", return_value=[]), \
         patch(
             "backend.services.skill_context.build_task_skill_context",
             new=AsyncMock(return_value=""),
         ):
        yield


def test_codex_main_mcp_capability_defaults_on():
    assert Settings.model_fields["codex_main_mcp_enabled"].default is True


def _api_account_stub(tmp_path, *, api_provider="cloudrouter"):
    root = tmp_path / "api-account"
    return types.SimpleNamespace(
        id=f"{api_provider}-1",
        api_provider=api_provider,
        root=root,
        claude_config_dir=str(root / "claude"),
        codex_home=str(root / "codex"),
    )


@pytest.mark.asyncio
async def test_api_account_delete_blocks_db_only_unknown_live_binding(
    db_factory, tmp_path,
):
    async with db_factory() as db:
        task = Task(
            title="unknown API owner",
            status="in_progress",
            provider="claude",
            metadata_={},
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="recovered",
            status="running",
            pid=987654,
            current_task_id=task.id,
            provider="claude",
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()

    manager = InstanceManager(db_factory, MagicMock())
    blockers = await manager.api_account_runtime_users(
        _api_account_stub(tmp_path)
    )

    assert any("unverifiable" in blocker for blocker in blockers)


@pytest.mark.asyncio
async def test_api_account_delete_accepts_explicit_other_account_binding(
    db_factory, tmp_path,
):
    async with db_factory() as db:
        task = Task(
            title="known other owner",
            status="in_progress",
            provider="claude",
            metadata_={"claude_account_id": "cloudrouter-2"},
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="recovered",
            status="running",
            pid=987654,
            current_task_id=task.id,
            provider="claude",
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()

    manager = InstanceManager(db_factory, MagicMock())
    assert await manager.api_account_runtime_users(
        _api_account_stub(tmp_path)
    ) == []


@pytest.mark.asyncio
async def test_api_account_delete_blocks_missing_task_claim(
    db_factory, tmp_path,
):
    async with db_factory() as db:
        db.add(Instance(
            name="orphan claim",
            status="error",
            pid=None,
            current_task_id=999999,
            provider="codex",
        ))
        await db.commit()

    manager = InstanceManager(db_factory, MagicMock())
    blockers = await manager.api_account_runtime_users(
        _api_account_stub(tmp_path)
    )

    assert any("unverifiable task claim" in blocker for blocker in blockers)


@pytest.mark.asyncio
async def test_api_account_delete_blocks_pool_only_hot_pty_session(
    db_factory, tmp_path,
):
    account = _api_account_stub(tmp_path)
    session = types.SimpleNamespace(
        config=types.SimpleNamespace(
            config_dir=account.claude_config_dir,
        ),
        is_alive=True,
    )
    manager = InstanceManager(db_factory, MagicMock())
    manager._pty_backend = types.SimpleNamespace(
        _sessions={},
        _pool=types.SimpleNamespace(_sessions={"hot": session}),
    )

    blockers = await manager.api_account_runtime_users(account)

    assert "hot PTY session hot" in blockers


@pytest.mark.asyncio
async def test_api_account_delete_fails_closed_on_db_query_error(tmp_path):
    @asynccontextmanager
    async def failing_db_factory():
        db = MagicMock()
        db.execute = AsyncMock(side_effect=RuntimeError("database offline"))
        yield db

    manager = InstanceManager(failing_db_factory, MagicMock())
    with pytest.raises(
        RuntimeError,
        match="Could not verify durable task account ownership",
    ):
        await manager.api_account_runtime_users(
            _api_account_stub(tmp_path)
        )


def test_codex_main_mcp_capability_allows_explicit_env_opt_out(monkeypatch):
    monkeypatch.setenv("CODEX_MAIN_MCP_ENABLED", "false")
    assert Settings(_env_file=None).codex_main_mcp_enabled is False


def test_parse_codex_agent_message():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {"id": "item_1", "type": "agent_message", "text": "Done"},
    }))

    assert event["event_type"] == "message"
    assert event["role"] == "assistant"
    assert event["content"] == "Done"
    assert event["is_error"] is False


@pytest.mark.asyncio
async def test_inject_codex_message_forwards_native_attachment_inputs():
    registry = MagicMock()
    registry.steer_turn = AsyncMock(return_value=True)
    manager = InstanceManager(MagicMock(), MagicMock())
    manager._codex_app_server = registry
    input_items = [
        {"type": "text", "text": "inspect both attachments"},
        {"type": "localImage", "path": "/tmp/screenshot.png"},
        {
            "type": "mention",
            "name": "report.txt",
            "path": "/tmp/report.txt",
        },
    ]

    assert await manager.inject_codex_message(
        "thread-1",
        "inspect both attachments",
        input_items=input_items,
    ) is True
    registry.steer_turn.assert_awaited_once_with(
        "thread-1",
        "inspect both attachments",
        input_items=input_items,
    )


@pytest.mark.asyncio
async def test_inject_pty_attachment_rejects_container_before_inject():
    session = types.SimpleNamespace(
        session_id="claude-session-1",
        is_alive=True,
        inject=AsyncMock(return_value=True),
    )
    consumer = MagicMock()
    consumer.done.return_value = False
    manager = InstanceManager(MagicMock(), MagicMock())
    manager._pty_backend = types.SimpleNamespace(
        _sessions={7: session},
        _consumers={7: consumer},
    )
    manager._container_tasks[7] = 99

    with pytest.raises(
        LiveAttachmentInjectionUnsupportedError,
        match="cannot access uploaded files",
    ):
        await manager.inject_pty_message(
            "claude-session-1",
            "Read /tmp/upload.txt",
            require_host_file_access=True,
        )

    session.inject.assert_not_awaited()


@pytest.mark.asyncio
async def test_task_launch_barrier_waits_for_cancelled_spawn_cleanup():
    im = InstanceManager(MagicMock(), MagicMock())
    instance_id = 901
    task_id = 902
    entered = asyncio.Event()
    release_cleanup = asyncio.Event()
    process = MagicMock(pid=1901, returncode=None)

    async def launch_then_cleanup(**_kwargs):
        im.processes[instance_id] = process
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release_cleanup.wait()
            process.returncode = -9
            im.processes.pop(instance_id, None)
            raise

    im._launch_locked = launch_then_cleanup
    launching = asyncio.create_task(
        im.launch(instance_id, "prompt", task_id=task_id)
    )
    await entered.wait()
    launching.cancel()
    assert (
        await im.wait_for_task_launch_barrier(
            instance_id, task_id, timeout=0.01
        )
        is False
    )
    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await launching
    assert (
        await im.wait_for_task_launch_barrier(
            instance_id, task_id, timeout=0.1
        )
        is True
    )
    assert instance_id not in im._launch_reservations


@pytest.mark.asyncio
async def test_failed_spawn_reap_retains_task_launch_reservation():
    im = InstanceManager(MagicMock(), MagicMock())
    instance_id = 903
    task_id = 904
    process = MagicMock(pid=1903, returncode=None)

    async def fail_with_live_generation(**_kwargs):
        im.processes[instance_id] = process
        raise RuntimeError("reap not proven")

    im._launch_locked = fail_with_live_generation
    with pytest.raises(RuntimeError, match="reap not proven"):
        await im.launch(instance_id, "prompt", task_id=task_id)

    assert im._launch_reservations[instance_id].task_id == task_id
    assert (
        await im.wait_for_task_launch_barrier(
            instance_id, task_id, timeout=0.1
        )
        is False
    )
    # Explicit test cleanup of synthetic evidence.
    process.returncode = -9
    im.processes.pop(instance_id, None)
    im._launch_reservations.pop(instance_id, None)


@pytest.mark.asyncio
async def test_fast_launch_resolves_null_model_before_runtime_admission(
    monkeypatch,
):
    monkeypatch.setattr(
        settings,
        "default_codex_model",
        "gpt-5.6-sol",
    )
    im = InstanceManager(MagicMock(), MagicMock())
    admitted = []

    @asynccontextmanager
    async def runtime_admission(provider, config_dir, model, *, service_tier):
        admitted.append((provider, config_dir, model, service_tier))
        yield None

    im._cloudrouter_runtime_admission = runtime_admission
    im._launch_locked = AsyncMock(return_value=4321)

    assert await im.launch(
        910,
        "Fast with historical NULL model",
        provider="codex",
        model=None,
        codex_service_tier="priority",
    ) == 4321

    assert admitted == [
        ("codex", None, "gpt-5.6-sol", "priority"),
    ]
    assert im._launch_locked.await_args.kwargs["model"] == "gpt-5.6-sol"


@pytest.mark.asyncio
async def test_pty_quota_event_retains_reset_metadata_for_post_turn_switch(
    db_factory,
):
    async with db_factory() as db:
        inst = Instance(name="pty-quota-event")
        task = Task(title="pty quota", status="executing", provider="claude")
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    info = {
        "status": "allowed_warning",
        "rateLimitType": "seven_day",
        "utilization": 0.91,
        "resetsAt": 1_800_000_000,
    }
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    await im._process_event(inst.id, task.id, {
        "event_type": "rate_limit_event",
        "role": None,
        "content": None,
        "raw_json": None,
        "is_error": False,
        "rate_limit_info": info,
    })

    assert im.pty_rate_limit_seen(inst.id)
    assert im.pty_rate_limit_info(inst.id) == info
    im.clear_pty_rate_limit(inst.id)
    assert not im.pty_rate_limit_seen(inst.id)
    assert im.pty_rate_limit_info(inst.id) is None


@pytest.mark.asyncio
async def test_codex_app_server_delta_is_broadcast_but_not_persisted(db_factory):
    """Streaming improves TTFT without turning every token into a DB row."""
    async with db_factory() as db:
        inst = Instance(name="delta-inst")
        task = Task(title="delta-task", status="executing", provider="codex")
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    await im._process_event(inst.id, task.id, {
        "event_type": "message_delta",
        "role": "assistant",
        "content": "Hel",
        "item_id": "msg-1",
        "raw_json": '{"large":"payload"}',
        "is_error": False,
    })

    async with db_factory() as db:
        rows = (await db.execute(
            select(LogEntry).where(LogEntry.task_id == task.id)
        )).scalars().all()
    assert rows == []
    assert broadcaster.broadcast.await_args_list == [
        ((f"instance:{inst.id}", {
            "event_type": "message_delta", "role": "assistant",
            "content": "Hel", "item_id": "msg-1", "is_error": False,
        }),),
        ((f"task:{task.id}", {
            "event_type": "message_delta", "role": "assistant",
            "content": "Hel", "item_id": "msg-1", "is_error": False,
        }),),
    ]


def test_parse_codex_command_started():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.started",
        "item": {
            "id": "item_2",
            "type": "command_execution",
            "command": "npm run build",
            "status": "in_progress",
        },
    }))

    assert event["event_type"] == "tool_use"
    assert event["role"] == "assistant"
    assert event["tool_name"] == "Shell"
    assert json.loads(event["tool_input"]) == {"command": "npm run build"}
    assert event["content"] is None


def test_parse_codex_command_completed():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {
            "id": "item_3",
            "type": "command_execution",
            "command": "git status",
            "aggregated_output": "nothing to commit\n",
            "exit_code": 0,
            "status": "completed",
        },
    }))

    assert event["event_type"] == "tool_result"
    assert event["role"] == "tool"
    assert event["tool_name"] == "Shell"
    assert json.loads(event["tool_input"]) == {"command": "git status"}
    assert event["tool_output"] == "nothing to commit\n"
    assert event["content"] is None
    assert event["is_error"] is False


def test_parse_codex_collab_wait_started_as_tool_use():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.started",
        "item": {
            "type": "collab_agent_tool_call",
            "id": "collab-1",
            "tool": "wait",
            "status": "inProgress",
            "sender_thread_id": "thread-parent",
            "receiver_thread_ids": ["thread-child"],
            "agents_states": {},
        },
    }))

    assert event["event_type"] == "tool_use"
    assert event["role"] == "assistant"
    assert event["tool_name"] == "Agent.wait"
    assert json.loads(event["tool_input"]) == {
        "sender_thread_id": "thread-parent",
        "receiver_thread_ids": ["thread-child"],
    }
    assert event["content"] is None


def test_parse_codex_collab_wait_completed_as_tool_result():
    """An agent wait finishing is not a parent turn-completed event."""
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {
            "type": "collabAgentToolCall",
            "id": "collab-1",
            "tool": "wait",
            "status": "completed",
            "senderThreadId": "thread-parent",
            "receiverThreadIds": ["thread-child"],
            "model": None,
            "reasoningEffort": None,
            "agentsStates": {},
        },
    }))

    assert event["event_type"] == "tool_result"
    assert event["role"] == "tool"
    assert event["tool_name"] == "Agent.wait"
    assert json.loads(event["tool_input"]) == {
        "sender_thread_id": "thread-parent",
        "receiver_thread_ids": ["thread-child"],
    }
    assert json.loads(event["tool_output"]) == {"status": "completed"}
    assert event["content"] is None
    assert event["is_error"] is False


def test_parse_codex_collab_agent_failure_is_tool_error():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {
            "type": "collab_agent_tool_call",
            "id": "collab-2",
            "tool": "spawnAgent",
            "status": "failed",
            "agents_states": {
                "thread-child": {
                    "status": "errored",
                    "message": "launch failed",
                },
            },
        },
    }))

    assert event["event_type"] == "tool_result"
    assert event["tool_name"] == "Agent.spawn_agent"
    assert event["is_error"] is True
    assert json.loads(event["tool_output"]) == {
        "status": "failed",
        "agents_states": {
            "thread-child": {
                "status": "errored",
                "message": "launch failed",
            },
        },
    }


@pytest.mark.parametrize(
    "item_type",
    [
        "sub_agent_activity",
        "subAgentActivity",
        "context_compaction",
        "contextCompaction",
    ],
)
def test_parse_codex_metadata_item_completion_is_ignored(item_type):
    im = InstanceManager(MagicMock(), MagicMock())

    event = im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {
            "type": item_type,
            "id": "metadata-1",
        },
    }))

    assert event is None


@pytest.mark.parametrize(
    ("item_type", "extra"),
    [
        (
            "dynamicToolCall",
            {
                "tool": "lookup",
                "arguments": {"query": "status"},
                "success": True,
            },
        ),
        (
            "imageGeneration",
            {
                "result": "generated",
                "savedPath": "/tmp/generated.png",
            },
        ),
    ],
)
def test_parse_codex_unsupported_item_completion_never_uses_status(
    item_type,
    extra,
):
    """An item completing is not evidence that the parent turn completed."""
    im = InstanceManager(MagicMock(), MagicMock())

    event = im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {
            "type": item_type,
            "id": "unsupported-1",
            "status": "completed",
            **extra,
        },
    }))

    assert event is None


def test_parse_codex_turn_completed_usage():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "turn.completed",
        "usage": {
            "input_tokens": 100,
            "cached_input_tokens": 40,
            "output_tokens": 20,
            "reasoning_output_tokens": 5,
        },
    }))

    assert event["event_type"] == "system_event"
    assert event["context_usage"] == {
        "input_tokens": 60,
        "cache_read_input_tokens": 40,
        "cache_creation_input_tokens": 0,
        "output_tokens": 20,
        "reasoning_output_tokens": 5,
        "total_input_tokens": 100,
        "total_tokens": 120,
        "context_tokens": 115,
    }
    assert event["content"] == "turn.completed"


def test_parse_codex_thread_started_session_id():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "thread.started",
        "thread_id": "test-thread-123",
    }))

    assert event["event_type"] == "system_event"
    assert event["content"] == "thread.started"
    assert event["session_id"] == "test-thread-123"


def test_parse_codex_app_server_message_delta():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.agent_message.delta",
        "item_id": "msg-1",
        "delta": "Hel",
    }))

    assert event["event_type"] == "message_delta"
    assert event["role"] == "assistant"
    assert event["content"] == "Hel"
    assert event["item_id"] == "msg-1"


def test_parse_codex_app_server_reasoning_completion_keeps_item_id():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {
            "id": "reasoning-1",
            "type": "reasoning",
            "text": "final reasoning summary",
        },
    }))

    assert event["event_type"] == "thinking"
    assert event["content"] == "final reasoning summary"
    assert event["item_id"] == "reasoning-1"


# === _build_command tests ===


def test_build_command_claude_basic():
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(provider="claude", prompt="do stuff", model=None, resume_session_id=None, effort_level=None)
    assert cmd[0] == "claude"
    assert "-p" in cmd
    assert "do stuff" in cmd
    assert "--dangerously-skip-permissions" in cmd
    assert "--output-format" in cmd
    assert "--verbose" in cmd


def test_build_command_claude_with_resume_and_model():
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(provider="claude", prompt="follow up", model="opus", resume_session_id="sess-1", effort_level="high")
    assert "--resume" in cmd
    assert "sess-1" in cmd
    assert "--model" in cmd
    assert "opus" in cmd
    assert "--effort" in cmd
    assert "high" in cmd


def test_build_command_claude_opus5_with_max_effort():
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(
        provider="claude",
        prompt="hard task",
        model="claude-opus-5",
        resume_session_id=None,
        effort_level="max",
    )

    assert cmd[cmd.index("--model") + 1] == "claude-opus-5"
    assert cmd[cmd.index("--effort") + 1] == "max"


def test_build_command_codex_basic():
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(provider="codex", prompt="do stuff", model=None, resume_session_id=None, effort_level=None)
    assert cmd[1] == "exec"
    assert "--json" in cmd
    assert "--dangerously-bypass-approvals-and-sandbox" in cmd
    assert "do stuff" in cmd
    assert "resume" not in cmd


def test_build_command_codex_with_resume():
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(provider="codex", prompt="continue", model="gpt-5.5", resume_session_id="thread-abc", effort_level=None)
    assert cmd[1] == "exec"
    assert cmd[2] == "resume"
    assert "--model" in cmd
    assert "gpt-5.5" in cmd
    assert "thread-abc" in cmd
    assert "continue" in cmd


def test_build_command_codex_default_model_not_passed():
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(provider="codex", prompt="hi", model="default", resume_session_id=None, effort_level=None)
    assert "--model" not in cmd


def test_build_command_codex_standard_explicitly_clears_fast_mode():
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(
        provider="codex",
        prompt="standard",
        model="gpt-5.6-sol",
        resume_session_id=None,
        effort_level="high",
        codex_service_tier="default",
    )

    assert 'service_tier="default"' in cmd
    assert "fast_mode" not in cmd


def test_build_command_codex_fast_uses_explicit_feature_and_tier():
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(
        provider="codex",
        prompt="fast",
        model="gpt-5.6-sol",
        resume_session_id=None,
        effort_level="high",
        codex_service_tier="priority",
    )

    assert cmd[cmd.index("--enable") + 1] == "fast_mode"
    assert 'service_tier="fast"' in cmd
    assert 'service_tier="default"' not in cmd


def test_build_command_codex_api_forces_git_root_untrusted(tmp_path):
    repo = tmp_path / 'repo "quoted\\path'
    nested = repo / "nested"
    (repo / ".git").mkdir(parents=True)
    nested.mkdir()
    im = InstanceManager(MagicMock(), MagicMock())

    cmd = im._build_command(
        provider="codex",
        prompt="hi",
        model="gpt-5.6-sol",
        resume_session_id=None,
        effort_level=None,
        cwd=str(nested),
        codex_api_account=True,
    )

    overrides = [
        cmd[index + 1]
        for index, token in enumerate(cmd[:-1])
        if token == "-c"
    ]
    parsed = [tomllib.loads(override) for override in overrides]
    assert {
        "projects": {
            str(repo.resolve()): {"trust_level": "untrusted"},
        }
    } in parsed


def test_build_command_native_codex_does_not_override_project_trust(tmp_path):
    im = InstanceManager(MagicMock(), MagicMock())

    cmd = im._build_command(
        provider="codex",
        prompt="hi",
        model=None,
        resume_session_id=None,
        effort_level=None,
        cwd=str(tmp_path),
    )

    assert not any(
        value.startswith("projects=")
        for index, value in enumerate(cmd)
        if index > 0 and cmd[index - 1] == "-c"
    )


@pytest.mark.parametrize(
    ("resume_session_id", "expected_tail"),
    [
        (None, ["use required MCP"]),
        ("thread-mcp", ["thread-mcp", "use required MCP"]),
    ],
    ids=["fresh", "resume"],
)
def test_build_command_codex_renders_required_mcp_as_exact_argv_tokens(
    resume_session_id, expected_tail,
):
    im = InstanceManager(MagicMock(), MagicMock())
    specs = build_mcp_server_specs(73, {"monitor": True})

    cmd = im._build_command(
        provider="codex",
        prompt="use required MCP",
        model="gpt-5.6-sol",
        resume_session_id=resume_session_id,
        effort_level="high",
        codex_mcp_specs=specs,
    )

    expected_mcp_args = render_codex_exec_config_args(specs)
    mcp_flag_index = cmd.index("-c", cmd.index("-c") + 1)
    assert cmd[mcp_flag_index : mcp_flag_index + 2] == expected_mcp_args
    assert cmd[-len(expected_tail) :] == expected_tail
    assert expected_mcp_args[1] in cmd
    assert "--task-id" in expected_mcp_args[1]
    assert '"73"' in expected_mcp_args[1]


def test_build_command_codex_exec_uses_canonical_skill_context():
    im = InstanceManager(MagicMock(), MagicMock())

    cmd = im._build_command(
        provider="codex",
        prompt="review this",
        model="gpt-5.6-sol",
        resume_session_id=None,
        effort_level="high",
        skill_context="## Available Skills\n- **review**: Review changes",
    )

    assert cmd[-1] == (
        "<ccm-task-skill-context>\n"
        "## Available Skills\n- **review**: Review changes\n"
        "</ccm-task-skill-context>\n\n"
        "review this"
    )


def test_build_command_claude_uses_one_canonical_skill_file():
    im = InstanceManager(MagicMock(), MagicMock())

    cmd = im._build_command(
        provider="claude",
        prompt="review this",
        model=None,
        resume_session_id=None,
        effort_level=None,
        task_id=73,
        skill_context=(
            "## Available Skills\n- **review**: Review changes\n\n"
            "## User Skills\n- **Personal** (id=8): Checklist"
        ),
    )

    indexes = [
        index
        for index, token in enumerate(cmd)
        if token == "--append-system-prompt-file"
    ]
    assert len(indexes) == 1
    prompt_path = Path(cmd[indexes[0] + 1])
    try:
        assert prompt_path.read_text(encoding="utf-8") == (
            "## Available Skills\n- **review**: Review changes\n\n"
            "## User Skills\n- **Personal** (id=8): Checklist\n"
        )
    finally:
        prompt_path.unlink(missing_ok=True)


def test_build_command_codex_rejects_invalid_required_exec_mcp():
    im = InstanceManager(MagicMock(), MagicMock())
    invalid_spec = McpServerSpec(
        name="invalid.name",
        command="python",
        required=True,
    )

    with pytest.raises(
        CodexRequiredMcpError,
        match="Invalid required Codex exec MCP configuration",
    ):
        im._build_command(
            provider="codex",
            prompt="must not launch",
            model=None,
            resume_session_id=None,
            effort_level=None,
            codex_mcp_specs=(invalid_spec,),
        )


def test_build_command_unsupported_provider():
    im = InstanceManager(MagicMock(), MagicMock())
    with pytest.raises(ValueError, match="Unsupported CLI provider"):
        im._build_command(provider="unknown", prompt="hi", model=None, resume_session_id=None, effort_level=None)


# === Codex parser edge cases ===


def test_parse_codex_malformed_json():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line("this is not json")
    assert event["event_type"] == "message"
    assert event["content"] == "this is not json"
    assert event["is_error"] is False


def test_parse_codex_error_event():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "error",
        "message": "rate limit exceeded",
    }))
    assert event["event_type"] == "system_event"
    assert event["is_error"] is True
    assert "rate limit exceeded" in event["content"]


def test_parse_codex_heartbeat_returns_none():
    """Heartbeat-like events with no meaningful content return None."""
    im = InstanceManager(MagicMock(), MagicMock())
    result = im._parse_codex_line(json.dumps({
        "type": "heartbeat",
    }))
    assert result is None


def test_parse_codex_unknown_event_with_content():
    """Unknown event type with content is preserved as system_event."""
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "custom.event",
        "content": "something happened",
    }))
    assert event is not None
    assert event["event_type"] == "system_event"
    assert event["content"] == "something happened"


def test_parse_codex_command_with_nonzero_exit():
    """Command execution with non-zero exit code sets is_error=True."""
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {
            "type": "command_execution",
            "command": "npm test",
            "exit_code": 1,
            "status": "failed",
            "aggregated_output": "test failed",
        },
    }))
    assert event["event_type"] == "tool_result"
    assert event["is_error"] is True


def test_parse_codex_session_id_from_nested_session():
    """Session ID extracted from nested session.id field."""
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "session.started",
        "session": {"id": "nested-sess-456"},
    }))
    assert event is not None
    assert event.get("session_id") == "nested-sess-456"


def _make_mock_process(pid=12345, returncode=0):
    """Create a mock asyncio subprocess."""
    proc = MagicMock()
    proc.pid = pid
    proc.returncode = returncode

    # stdout: readline returns empty bytes (EOF immediately)
    async def readline():
        return b""
    proc.stdout = MagicMock()
    proc.stdout.readline = readline

    # stderr
    async def read_stderr():
        return b""
    proc.stderr = MagicMock()
    proc.stderr.read = read_stderr

    # wait
    proc.wait = AsyncMock(return_value=returncode)
    proc.terminate = MagicMock()
    proc.kill = MagicMock()

    return proc


@pytest.mark.asyncio
async def test_launch_creates_subprocess(db_factory):
    """launch() calls create_subprocess_exec with correct args."""
    # Create instance in DB
    async with db_factory() as db:
        inst = Instance(name="test-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc) as mock_exec:
        pid = await im.launch(instance_id=inst_id, prompt="hello", cwd="/tmp")

    assert pid == 12345
    mock_exec.assert_awaited_once()
    cmd_args = mock_exec.call_args[0]
    assert "-p" in cmd_args
    assert "hello" in cmd_args
    assert "--dangerously-skip-permissions" in cmd_args
    if os.name == "posix":
        assert mock_exec.call_args.kwargs["start_new_session"] is True
    # Wait for consumer task to finish
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_launch_with_resume(db_factory):
    """launch() with resume_session_id includes --resume flag."""
    async with db_factory() as db:
        inst = Instance(name="resume-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc):
        await im.launch(instance_id=inst_id, prompt="followup", cwd="/tmp", resume_session_id="sess-123")

    call_args = im.processes  # just verify no error
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_launch_with_model(db_factory):
    """launch() with model param includes --model flag."""
    async with db_factory() as db:
        inst = Instance(name="model-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc) as mock_exec:
        await im.launch(instance_id=inst_id, prompt="hi", cwd="/tmp", model="opus")

    cmd_args = mock_exec.call_args[0]
    assert "--model" in cmd_args
    assert "opus" in cmd_args
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_launch_updates_db(db_factory):
    """After launch, Instance status is 'running' in DB."""
    async with db_factory() as db:
        inst = Instance(name="db-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc):
        await im.launch(instance_id=inst_id, prompt="hi", cwd="/tmp")

    # Check DB state (before consumer finishes)
    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "running"
        assert inst.pid == 12345
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_launch_saves_cwd(db_factory):
    """After launch with task_id, Task.last_cwd is set."""
    async with db_factory() as db:
        inst = Instance(name="cwd-inst")
        db.add(inst)
        await db.flush()
        task = Task(
            title="t",
            description="d",
            status="executing",
            instance_id=inst.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc):
        await im.launch(instance_id=inst_id, prompt="hi", task_id=task_id, cwd="/my/repo")

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.last_cwd == "/my/repo"
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_launch_unsets_claude_env(db_factory):
    """Environment passed to subprocess excludes CLAUDECODE/CLAUDE_CODE."""
    async with db_factory() as db:
        inst = Instance(name="env-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    with patch.dict(os.environ, {"CLAUDECODE": "1", "CLAUDE_CODE": "1"}, clear=False), \
         patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc) as mock_exec:
        await im.launch(instance_id=inst_id, prompt="hi", cwd="/tmp")

    call_kwargs = mock_exec.call_args[1]
    env = call_kwargs["env"]
    assert "CLAUDECODE" not in env
    assert "CLAUDE_CODE" not in env
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_cloudrouter_claude_launch_removes_inherited_auth_env(
    db_factory, tmp_path
):
    async with db_factory() as db:
        inst = Instance(name="cloudrouter-env-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    account_root = tmp_path / "cloudrouter-1"
    config_dir = account_root / "claude"
    config_dir.mkdir(parents=True)
    account = MagicMock(root=account_root)
    store = MagicMock()
    store.account_for_claude_config_dir.return_value = account
    @asynccontextmanager
    async def runtime_admission(*_args):
        yield account
    store.runtime_admission = runtime_admission
    mock_proc = _make_mock_process()
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.cloudrouter_store = store

    im._pty_enabled = False

    inherited = {
        "ANTHROPIC_AUTH_TOKEN": "must-not-leak",
        "ANTHROPIC_API_KEY": "must-not-leak",
        "CLAUDE_CODE_OAUTH_TOKEN": "must-not-leak",
    }
    with (
        patch.dict(os.environ, inherited, clear=False),
        patch(
            "backend.services.instance_manager.asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
            return_value=mock_proc,
        ) as mock_exec,
    ):
        await im.launch(
            instance_id=inst.id,
            prompt="hi",
            cwd="/tmp",
            provider="claude",
            config_dir=str(config_dir),
            git_env={"ANTHROPIC_API_KEY": "also-must-not-leak"},
        )

    child_env = mock_exec.call_args.kwargs["env"]
    for key in inherited:
        assert key not in child_env
    assert child_env["CLAUDE_CONFIG_DIR"] == str(config_dir)
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_cloudrouter_claude_pty_wraps_binary_and_removes_auth_overrides(
    db_factory, tmp_path
):
    async with db_factory() as db:
        inst = Instance(name="cloudrouter-pty-env-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    account_root = tmp_path / "cloudrouter-1"
    config_dir = account_root / "claude"
    config_dir.mkdir(parents=True)
    account = MagicMock(root=account_root)
    store = MagicMock()
    store.account_for_claude_config_dir.return_value = account

    @asynccontextmanager
    async def runtime_admission(*_args):
        yield account

    store.runtime_admission = runtime_admission
    observed = {}
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.cloudrouter_store = store

    class Config:
        claude_binary = "/opt/claude-real"
        env_overrides = {
            "ANTHROPIC_AUTH_TOKEN": "must-not-leak",
            "ANTHROPIC_API_KEY": "must-not-leak",
            "CLAUDE_CODE_OAUTH_TOKEN": "must-not-leak",
            "SAFE_VALUE": "kept",
        }

    class FakePTYBackend:
        def build_config(self, **_kwargs):
            return Config()

        async def launch_for_ccm(self, **kwargs):
            config = self.build_config()
            observed["binary"] = config.claude_binary
            observed["env"] = dict(config.env_overrides)
            im.processes[kwargs["instance_id"]] = MagicMock(
                pid=52_001, returncode=None
            )
            return "cloudrouter-pty-session"

    im._pty_backend = FakePTYBackend()
    im._pty_enabled = True

    await im.launch(
        instance_id=inst.id,
        prompt="hi",
        cwd="/tmp",
        provider="claude",
        config_dir=str(config_dir),
        chat_initiated=True,
    )

    wrapper = Path(observed["binary"])
    assert wrapper.name == "cloudrouter_claude_wrapper.sh"
    assert wrapper.is_file()
    assert os.access(wrapper, os.X_OK)
    assert observed["env"]["CCM_CLOUDROUTER_CLAUDE_BINARY"] == (
        "/opt/claude-real"
    )
    assert observed["env"]["SAFE_VALUE"] == "kept"
    for key in (
        "ANTHROPIC_AUTH_TOKEN",
        "ANTHROPIC_API_KEY",
        "CLAUDE_CODE_OAUTH_TOKEN",
    ):
        assert key not in observed["env"]
    assert im.get_config_dir(inst.id) == str(config_dir)
    assert im._launch_params[inst.id]["prompt"] == "hi"
    assert im._launch_params[inst.id]["provider"] == "claude"


def test_cloudrouter_429_is_transient_only_for_exact_api_account_home(
    db_factory,
):
    im = InstanceManager(db_factory, MagicMock())
    store = MagicMock()
    store.account_for_claude_config_dir.side_effect = (
        lambda path: MagicMock() if path == "/api/claude" else None
    )
    im.cloudrouter_store = store
    im._config_dirs[1] = "/api/claude"
    im._config_dirs[2] = "/native/claude"
    error = "API Error: HTTP 429 Too Many Requests"

    assert im.is_cloudrouter_transient(1, "claude", error)
    assert not im.is_cloudrouter_transient(2, "claude", error)
    auth_error = "API Error: HTTP 401 Unauthorized INVALID_API_KEY"
    assert im.is_cloudrouter_auth_failure(1, "claude", auth_error)
    assert not im.is_cloudrouter_auth_failure(2, "claude", auth_error)
    assert im.is_cloudrouter_auth_failure(
        1, "claude", "API Error: HTTP 403 Forbidden",
    )


@pytest.mark.parametrize(
    "detail",
    [
        "all logged-in accounts are busy",
        "no eligible logged-in account is ready",
    ],
)
def test_apex_409_capacity_is_transient_only_for_exact_apex_codex_home(
    db_factory, detail,
):
    im = InstanceManager(db_factory, MagicMock())
    apex_account = types.SimpleNamespace(api_provider="apex")
    cloudrouter_account = types.SimpleNamespace(api_provider="cloudrouter")
    store = MagicMock()
    store.account_for_codex_home.side_effect = lambda path: {
        "/api/apex": apex_account,
        "/api/cloudrouter": cloudrouter_account,
    }.get(path)
    im.cloudrouter_store = store
    im._config_dirs.update({
        1: "/api/apex",
        2: "/api/cloudrouter",
        3: "/native/codex",
    })
    busy = (
        'unexpected status 409 Conflict: '
        f'{{"detail":"{detail}"}}'
    )

    assert im.is_cloudrouter_transient(1, "codex", busy)
    assert not im.is_cloudrouter_transient(2, "codex", busy)
    assert not im.is_cloudrouter_transient(3, "codex", busy)
    assert not im.is_cloudrouter_transient(
        1, "codex", "unexpected status 409 Conflict: branch changed",
    )
    assert not im.is_cloudrouter_transient(
        1, "codex", detail,
    )


def test_api_codex_home_scrubs_all_inherited_gateway_keys(db_factory):
    im = InstanceManager(db_factory, MagicMock())
    store = MagicMock()
    store.account_for_codex_home.return_value = object()
    im.cloudrouter_store = store

    assert im._codex_env_remove_for_home("/api/apex/codex") == {
        "OPENAI_API_KEY",
        "CODEX_API_KEY",
        "CLOUDROUTER_API_KEY",
        "APEX_CODEX_GATEWAY_KEY",
        "APEX_CODEX_API_KEY",
        "APEXROUTER_API_KEY",
        "APEXROUTER_CODEX_API_KEY",
    }


@pytest.mark.asyncio
async def test_default_claude_launch_clears_stale_instance_account_home(db_factory):
    async with db_factory() as db:
        inst = Instance(name="default-home-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock(broadcast=AsyncMock())
    im = InstanceManager(db_factory, broadcaster)
    im._config_dirs[inst_id] = "/tmp/previous-claude-account"

    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
        return_value=mock_proc,
    ):
        await im.launch(
            instance_id=inst_id,
            prompt="use default account",
            cwd="/tmp",
            provider="claude",
            config_dir=None,
        )

    assert im.get_config_dir(inst_id) is None
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_launch_with_thinking_budget_sets_env(db_factory):
    """launch(thinking_budget=N) injects MAX_THINKING_TOKENS=N into subprocess env."""
    async with db_factory() as db:
        inst = Instance(name="thinking-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc) as mock_exec:
        await im.launch(instance_id=inst_id, prompt="hi", cwd="/tmp", thinking_budget=12000)

    env = mock_exec.call_args[1]["env"]
    assert env.get("MAX_THINKING_TOKENS") == "12000"
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_launch_without_thinking_budget_omits_env(db_factory):
    """launch() without thinking_budget leaves MAX_THINKING_TOKENS unset."""
    async with db_factory() as db:
        inst = Instance(name="no-thinking-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    # Make sure the env var isn't already set in the test environment
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("MAX_THINKING_TOKENS", None)
        with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc) as mock_exec:
            await im.launch(instance_id=inst_id, prompt="hi", cwd="/tmp")

    env = mock_exec.call_args[1]["env"]
    assert "MAX_THINKING_TOKENS" not in env
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_launch_with_zero_thinking_budget_omits_env(db_factory):
    """thinking_budget=0 is treated as 'no budget' (CLI default)."""
    async with db_factory() as db:
        inst = Instance(name="zero-budget-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("MAX_THINKING_TOKENS", None)
        with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc) as mock_exec:
            await im.launch(instance_id=inst_id, prompt="hi", cwd="/tmp", thinking_budget=0)

    env = mock_exec.call_args[1]["env"]
    assert "MAX_THINKING_TOKENS" not in env
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_launch_with_effort_level(db_factory):
    """launch(effort_level='high') includes --effort high in command."""
    async with db_factory() as db:
        inst = Instance(name="effort-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc) as mock_exec:
        await im.launch(instance_id=inst_id, prompt="hi", cwd="/tmp", effort_level="high")

    cmd_args = mock_exec.call_args[0]
    assert "--effort" in cmd_args
    idx = cmd_args.index("--effort")
    assert cmd_args[idx + 1] == "high"
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_claude_pr_review_disables_all_tools_and_bypasses_pty(
    db_factory,
    tmp_path,
):
    """A PR snapshot turn must not inherit Claude tools, MCP, hooks, or PTY."""

    async with db_factory() as db:
        inst = Instance(name="claude-pr-review-isolated")
        db.add(inst)
        await db.flush()
        task = Task(
            title="PR review",
            status="executing",
            provider="claude",
            instance_id=inst.id,
            tags=["pr-review"],
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    process = _make_mock_process(returncode=None)
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._pty_enabled = True
    im._pty_backend = MagicMock()
    im._launch_pty = AsyncMock(
        side_effect=AssertionError("PR review must not enter PTY")
    )
    im._consume_output = AsyncMock()

    with (
        patch(
            "backend.services.instance_manager.asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
            return_value=process,
        ) as exec_mock,
        patch(
            "backend.services.mcp_config.generate_mcp_config"
        ) as generate_mcp,
        patch(
            "backend.services.ask_user_settings.ensure_ask_user_hook"
        ) as ensure_ask_user,
        patch(
            "backend.services.skill_loader.discover_skills"
        ) as discover_skills,
    ):
        await im.launch(
            instance_id=inst.id,
            prompt="review the backend-snapshotted input",
            task_id=task.id,
            cwd=str(tmp_path),
            provider="claude",
            config_dir=str(tmp_path / "claude-home"),
            enabled_skills={"monitor": True, "sub-agent": True},
            system_prompt_mode="append",
        )

    argv = list(exec_mock.await_args.args)
    assert argv[argv.index("--tools") + 1] == ""
    assert argv[argv.index("--setting-sources") + 1] == ""
    assert "--strict-mcp-config" in argv
    assert "--disable-slash-commands" in argv
    assert "--exclude-dynamic-system-prompt-sections" in argv
    assert "--mcp-config" not in argv
    assert "--append-system-prompt-file" not in argv
    assert "--system-prompt-file" not in argv
    im._launch_pty.assert_not_awaited()
    generate_mcp.assert_not_called()
    ensure_ask_user.assert_not_called()
    discover_skills.assert_not_called()


@pytest.mark.asyncio
async def test_codex_pr_review_uses_only_isolated_app_server_route(
    db_factory,
    monkeypatch,
    tmp_path,
):
    """PR reviews pin Codex to read-only app-server with no ambient config."""

    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    async with db_factory() as db:
        inst = Instance(name="codex-pr-review-isolated")
        db.add(inst)
        await db.flush()
        task = Task(
            title="PR review",
            status="executing",
            provider="codex",
            instance_id=inst.id,
            tags=["pr-review"],
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._launch_codex_app_server = AsyncMock(return_value=43_210)
    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
    ) as exec_mock:
        pid = await im.launch(
            instance_id=inst.id,
            prompt="review the backend-snapshotted input",
            task_id=task.id,
            cwd=str(tmp_path),
            provider="codex",
            config_dir=str(tmp_path / "codex-home"),
            enabled_skills={"monitor": True, "sub-agent": True},
        )

    assert pid == 43_210
    kwargs = im._launch_codex_app_server.await_args.kwargs
    assert kwargs["disable_project_config"] is True
    assert kwargs["sandbox_mode"] == "read-only"
    assert kwargs["disable_autonomous_features"] is True
    assert kwargs["tools_disabled"] is True
    assert kwargs["mcp_specs"] == ()
    assert kwargs["skill_context"] == ""
    exec_mock.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected_error"),
    [
        (
            CodexRequiredMcpPreTurnError("sandbox could not be confirmed"),
            CodexRequiredMcpError,
        ),
        (RuntimeError("app-server protocol failed"), CodexRequiredMcpError),
        (asyncio.TimeoutError(), asyncio.TimeoutError),
    ],
)
async def test_codex_pr_review_app_server_failure_never_falls_back_to_exec(
    db_factory,
    monkeypatch,
    tmp_path,
    failure,
    expected_error,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    async with db_factory() as db:
        inst = Instance(name=f"codex-pr-no-fallback-{type(failure).__name__}")
        db.add(inst)
        await db.flush()
        task = Task(
            title="PR review",
            status="executing",
            provider="codex",
            instance_id=inst.id,
            tags=["pr-review"],
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._launch_codex_app_server = AsyncMock(side_effect=failure)
    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
    ) as exec_mock:
        with pytest.raises(expected_error):
            await im.launch(
                instance_id=inst.id,
                prompt="must fail closed",
                task_id=task.id,
                cwd=str(tmp_path),
                provider="codex",
                config_dir=str(tmp_path / "codex-home"),
            )

    exec_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_codex_pr_review_rejects_disabled_app_server_before_exec(
    db_factory,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    async with db_factory() as db:
        inst = Instance(name="codex-pr-app-server-required")
        db.add(inst)
        await db.flush()
        task = Task(
            title="PR review",
            status="executing",
            provider="codex",
            instance_id=inst.id,
            tags=["pr-review"],
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
    ) as exec_mock:
        with pytest.raises(
            CodexRequiredMcpError,
            match="requires the app-server read-only sandbox",
        ):
            await im.launch(
                instance_id=inst.id,
                prompt="must not use exec",
                task_id=task.id,
                cwd=str(tmp_path),
                provider="codex",
                config_dir=str(tmp_path / "codex-home"),
            )

    exec_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_launch_codex_provider_command(db_factory, monkeypatch, tmp_path):
    """launch(provider='codex') constructs codex exec command."""
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    async with db_factory() as db:
        inst = Instance(name="codex-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    codex_home = tmp_path / "codex-account"
    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc) as mock_exec:
        await im.launch(
            instance_id=inst_id, prompt="do stuff", cwd="/tmp",
            provider="codex", config_dir=str(codex_home),
        )

    cmd_args = mock_exec.call_args[0]
    assert cmd_args[1] == "exec"
    assert "--json" in cmd_args
    assert "--dangerously-bypass-approvals-and-sandbox" in cmd_args
    assert "do stuff" in cmd_args
    # Should NOT have Claude-specific flags
    assert "--output-format" not in cmd_args
    assert "--verbose" not in cmd_args
    expected_home = str(codex_home.resolve())
    assert mock_exec.call_args.kwargs["env"]["CODEX_HOME"] == expected_home
    assert im.get_config_dir(inst_id) == expected_home
    assert codex_home.stat().st_mode & 0o777 == 0o700
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_api_codex_exec_forces_project_config_untrusted(
    db_factory, monkeypatch, tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    async with db_factory() as db:
        inst = Instance(name="codex-api-exec-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    repo = tmp_path / "repo"
    nested = repo / "nested"
    (repo / ".git").mkdir(parents=True)
    nested.mkdir()
    account_root = tmp_path / "apex-1"
    codex_home = account_root / "codex"
    codex_home.mkdir(parents=True)
    account = MagicMock(root=account_root)
    store = MagicMock()
    store.account_for_codex_home.side_effect = (
        lambda path: account
        if Path(path).resolve() == codex_home.resolve()
        else None
    )

    @asynccontextmanager
    async def runtime_admission(*_args):
        yield account

    store.runtime_admission = runtime_admission
    mock_proc = _make_mock_process()
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.cloudrouter_store = store

    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
        return_value=mock_proc,
    ) as mock_exec:
        await im.launch(
            instance_id=inst.id,
            prompt="hi",
            cwd=str(nested),
            provider="codex",
            config_dir=str(codex_home),
        )

    overrides = [
        mock_exec.await_args.args[index + 1]
        for index, token in enumerate(mock_exec.await_args.args[:-1])
        if token == "-c"
    ]
    assert {
        "projects": {
            str(repo.resolve()): {"trust_level": "untrusted"},
        }
    } in [tomllib.loads(override) for override in overrides]
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_codex_main_mcp_uses_exec_when_app_server_is_disabled(
    db_factory, monkeypatch, tmp_path, caplog,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    async with db_factory() as db:
        inst = Instance(name="codex-main-mcp-exec")
        db.add(inst)
        await db.flush()
        task = Task(
            title="exec MCP task",
            status="executing",
            provider="codex",
            instance_id=inst.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    mock_proc = _make_mock_process()
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.task_message_enqueuer = AsyncMock()
    with (
        caplog.at_level("INFO", logger="backend.services.instance_manager"),
        patch(
            "backend.services.instance_manager.asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
            return_value=mock_proc,
        ) as exec_mock,
    ):
        await im.launch(
            instance_id=inst.id,
            prompt="use CCM help",
            task_id=task.id,
            cwd="/tmp",
            provider="codex",
            config_dir=str(tmp_path / "codex-main-mcp-exec-home"),
        )

    argv = list(exec_mock.await_args.args)
    expected_mcp_args = render_codex_exec_config_args(
        build_mcp_server_specs(
            task.id,
            {},
            provider="codex",
            codex_monitor_enabled=True,
        )
    )
    flag_index = argv.index("-c")
    assert argv[flag_index : flag_index + 2] == expected_mcp_args
    assert argv[-1] == "use CCM help"
    assert "route=direct-exec" in caplog.text
    assert "required_mcp=True" in caplog.text
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_codex_browser_review_requires_mcp_only_app_server(
    db_factory, monkeypatch, tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", False)
    async with db_factory() as db:
        inst = Instance(name="codex-browser-review-exec")
        db.add(inst)
        await db.flush()
        task = Task(
            title="browser review",
            status="executing",
            provider="codex",
            instance_id=inst.id,
            enabled_skills={"browser-review": "job-abc"},
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.task_message_enqueuer = AsyncMock()
    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
    ) as exec_mock:
        with pytest.raises(
            CodexRequiredMcpError,
            match="app-server read-only sandbox",
        ):
            await im.launch(
                instance_id=inst.id,
                prompt="review the browser",
                task_id=task.id,
                cwd="/tmp",
                provider="codex",
                enabled_skills=task.enabled_skills,
                config_dir=str(tmp_path / "codex-browser-review-home"),
            )

    exec_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_codex_browser_review_uses_proven_mcp_only_profile(
    db_factory, monkeypatch, tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", False)
    async with db_factory() as db:
        inst = Instance(name="codex-browser-review-app-server")
        db.add(inst)
        await db.flush()
        task = Task(
            title="isolated browser review",
            status="executing",
            provider="codex",
            instance_id=inst.id,
            enabled_skills={"browser-review": "job-bound"},
            metadata_={"isolated_browser_agent": True},
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.task_message_enqueuer = AsyncMock()
    with patch.object(
        im,
        "_launch_codex_app_server",
        new_callable=AsyncMock,
        return_value=4321,
    ) as launch_app_server:
        pid = await im.launch(
            instance_id=inst.id,
            prompt="use only the bound browser tools",
            task_id=task.id,
            cwd=str(tmp_path),
            provider="codex",
            enabled_skills=task.enabled_skills,
            config_dir=str(tmp_path / "codex-browser-mcp-only-home"),
        )

    assert pid == 4321
    kwargs = launch_app_server.await_args.kwargs
    assert kwargs["mcp_only"] is True
    assert kwargs["tools_disabled"] is False
    assert kwargs["sandbox_mode"] == "read-only"
    assert kwargs["disable_project_config"] is True
    assert kwargs["disable_autonomous_features"] is True
    assert kwargs["skill_context"] == ""
    assert [spec.name for spec in kwargs["mcp_specs"]] == ["ccm_browser_review"]


@pytest.mark.asyncio
async def test_claude_browser_review_disables_builtins_but_keeps_bound_mcp(
    db_factory, monkeypatch, tmp_path,
):
    monkeypatch.setattr(settings, "use_pty_mode", False)
    async with db_factory() as db:
        inst = Instance(name="claude-browser-review-tools")
        db.add(inst)
        await db.flush()
        task = Task(
            title="isolated Claude browser review",
            status="executing",
            provider="claude",
            instance_id=inst.id,
            enabled_skills={"browser-review": "job-claude"},
            metadata_={"isolated_browser_agent": True},
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    process = _make_mock_process()
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.task_message_enqueuer = AsyncMock()
    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
        return_value=process,
    ) as spawn:
        await im.launch(
            instance_id=inst.id,
            prompt="use only browser evidence tools",
            task_id=task.id,
            cwd=str(tmp_path),
            provider="claude",
            enabled_skills=task.enabled_skills,
        )

    argv = list(spawn.await_args.args)
    tools_index = argv.index("--tools")
    assert argv[tools_index + 1] == ""
    assert "--strict-mcp-config" in argv
    assert "--mcp-config" in argv
    assert "--setting-sources" in argv
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_invalid_required_exec_mcp_fails_before_subprocess_spawn(
    db_factory, monkeypatch, tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    async with db_factory() as db:
        inst = Instance(name="codex-invalid-required-exec-mcp")
        db.add(inst)
        await db.flush()
        task = Task(
            title="invalid exec MCP task",
            status="executing",
            provider="codex",
            instance_id=inst.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    invalid_spec = McpServerSpec(
        name="invalid.name",
        command="python",
        required=True,
    )
    im = InstanceManager(db_factory, MagicMock())
    with (
        patch(
            "backend.services.mcp_config.build_mcp_server_specs",
            return_value=(invalid_spec,),
        ),
        patch(
            "backend.services.instance_manager.asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
        ) as exec_mock,
    ):
        with pytest.raises(
            CodexRequiredMcpError,
            match="Invalid required Codex exec MCP configuration",
        ):
            await im.launch(
                instance_id=inst.id,
                prompt="must not spawn",
                task_id=task.id,
                cwd="/tmp",
                provider="codex",
                config_dir=str(tmp_path / "codex-invalid-exec-mcp-home"),
            )

    exec_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_launch_codex_prefers_persistent_app_server(
    db_factory, monkeypatch, tmp_path, caplog,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    async with db_factory() as db:
        inst = Instance(name="codex-app-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    im = InstanceManager(db_factory, MagicMock())
    im._launch_codex_app_server = AsyncMock(return_value=4321)
    codex_home = tmp_path / "codex-account"
    with (
        caplog.at_level("INFO", logger="backend.services.instance_manager"),
        patch(
            "backend.services.instance_manager.asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
        ) as exec_mock,
    ):
        pid = await im.launch(
            instance_id=inst.id, prompt="hi", cwd="/tmp", provider="codex",
            resume_session_id="thread-1", config_dir=str(codex_home),
        )

    assert pid == 4321
    im._launch_codex_app_server.assert_awaited_once()
    assert im._launch_codex_app_server.await_args.kwargs["resume_session_id"] == "thread-1"
    assert im._launch_codex_app_server.await_args.kwargs["config_dir"] == str(
        codex_home.resolve()
    )
    assert (
        im._launch_codex_app_server.await_args.kwargs[
            "disable_project_config"
        ]
        is False
    )
    assert (
        im._launch_codex_app_server.await_args.kwargs["sandbox_mode"]
        == "danger-full-access"
    )
    assert (
        im._launch_codex_app_server.await_args.kwargs[
            "disable_autonomous_features"
        ]
        is False
    )
    assert "route=app-server" in caplog.text
    exec_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_api_codex_app_server_disables_project_config(
    db_factory, monkeypatch, tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    async with db_factory() as db:
        inst = Instance(name="codex-api-app-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    account_root = tmp_path / "apex-1"
    codex_home = account_root / "codex"
    codex_home.mkdir(parents=True)
    account = MagicMock(root=account_root)
    store = MagicMock()
    store.account_for_codex_home.side_effect = (
        lambda path: account
        if Path(path).resolve() == codex_home.resolve()
        else None
    )

    @asynccontextmanager
    async def runtime_admission(*_args):
        yield account

    store.runtime_admission = runtime_admission
    im = InstanceManager(db_factory, MagicMock())
    im.cloudrouter_store = store
    im._launch_codex_app_server = AsyncMock(return_value=4323)

    pid = await im.launch(
        instance_id=inst.id,
        prompt="hi",
        cwd="/tmp",
        provider="codex",
        config_dir=str(codex_home),
    )

    assert pid == 4323
    assert (
        im._launch_codex_app_server.await_args.kwargs[
            "disable_project_config"
        ]
        is True
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("resume_session_id", [None, "thread-existing"])
async def test_rollout_enabled_routes_fresh_and_resume_with_task_scoped_mcp(
    db_factory, monkeypatch, tmp_path, resume_session_id,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    async with db_factory() as db:
        inst = Instance(name=f"codex-rollout-{resume_session_id or 'fresh'}")
        db.add(inst)
        await db.flush()
        task = Task(
            title="rollout MCP task",
            status="executing",
            provider="codex",
            instance_id=inst.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    im = InstanceManager(db_factory, MagicMock())
    im._launch_codex_app_server = AsyncMock(return_value=4322)

    pid = await im.launch(
        instance_id=inst.id,
        prompt="use main MCP",
        task_id=task.id,
        cwd="/tmp",
        provider="codex",
        resume_session_id=resume_session_id,
        config_dir=str(tmp_path / "codex-rollout-home"),
    )

    assert pid == 4322
    launch_kwargs = im._launch_codex_app_server.await_args.kwargs
    assert launch_kwargs["resume_session_id"] == resume_session_id
    specs = launch_kwargs["mcp_specs"]
    assert [spec.name for spec in specs] == [
        "ccm_skills",
        "ccm_frontend_review",
        "ccm_workspace_review",
    ]
    assert all(spec.required is True for spec in specs)
    assert all(
        spec.args[spec.args.index("--task-id") + 1] == str(task.id)
        for spec in specs
    )
    assert "ccm_command_help" in specs[0].enabled_tools
    assert "test_git_target" in specs[2].enabled_tools
    assert "compare_test_runs" in specs[2].enabled_tools


@pytest.mark.asyncio
async def test_codex_app_server_rejects_home_owned_by_exec_generation(
    db_factory, monkeypatch, tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    async with db_factory() as db:
        inst = Instance(name="codex-app-server-vs-exec")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    codex_home = str((tmp_path / "codex-shared-home").resolve())
    im = InstanceManager(db_factory, MagicMock())
    im._codex_exec_homes[999] = codex_home
    im._launch_codex_app_server = AsyncMock(return_value=4321)

    with pytest.raises(
        CodexAppServerBusyError,
        match="still has an exec generation",
    ):
        await im.launch(
            instance_id=inst.id,
            prompt="must not overlap",
            cwd="/tmp",
            provider="codex",
            config_dir=codex_home,
        )

    im._launch_codex_app_server.assert_not_awaited()


@pytest.mark.asyncio
async def test_live_codex_quota_rejects_home_owned_by_exec_generation(
    tmp_path,
):
    codex_home = str((tmp_path / "codex-live-quota-vs-exec").resolve())
    registry = MagicMock()
    registry.read_rate_limits = AsyncMock(return_value={"rateLimits": {}})
    im = InstanceManager(MagicMock(), MagicMock())
    im._ensure_codex_app_server_registry = MagicMock(return_value=registry)
    im.processes[7] = MagicMock(returncode=None)
    im._codex_exec_homes[7] = codex_home

    with pytest.raises(
        CodexAppServerBusyError,
        match="still has an exec generation",
    ):
        await im.read_codex_rate_limits(codex_home)

    im._ensure_codex_app_server_registry.assert_not_called()
    registry.read_rate_limits.assert_not_awaited()


@pytest.mark.asyncio
async def test_live_codex_quota_rejects_active_ephemeral_exec(tmp_path):
    codex_home = str((tmp_path / "codex-live-quota-vs-ephemeral").resolve())
    registry = MagicMock()
    registry.read_rate_limits = AsyncMock(return_value={"rateLimits": {}})
    im = InstanceManager(MagicMock(), MagicMock())
    im._ensure_codex_app_server_registry = MagicMock(return_value=registry)
    im._codex_ephemeral_home_users[codex_home] = 1

    with pytest.raises(
        CodexAppServerBusyError,
        match="active ephemeral exec",
    ):
        await im.read_codex_rate_limits(codex_home)

    im._ensure_codex_app_server_registry.assert_not_called()
    registry.read_rate_limits.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "registry_method"),
    [
        ("read", "read_thread"),
        ("create", "create_thread"),
        ("fork", "fork_thread"),
        ("delete", "delete_thread"),
    ],
)
async def test_codex_thread_operations_reject_exec_owned_home(
    tmp_path, operation, registry_method,
):
    codex_home = str((tmp_path / f"codex-thread-{operation}-vs-exec").resolve())
    registry = MagicMock()
    setattr(registry, registry_method, AsyncMock())
    im = InstanceManager(MagicMock(), MagicMock())
    im._codex_app_server = registry
    im._codex_exec_homes[7] = codex_home

    with pytest.raises(
        CodexAppServerBusyError,
        match="still has an exec generation",
    ):
        if operation == "read":
            await im.read_codex_thread(codex_home, "thread-source")
        elif operation == "create":
            await im.create_codex_thread(
                codex_home,
                cwd="/tmp",
                model="gpt-5.6-sol",
            )
        elif operation == "fork":
            await im.fork_codex_thread(
                codex_home,
                "thread-source",
                last_turn_id="turn-1",
            )
        else:
            await im.delete_codex_thread(codex_home, "thread-fork")

    getattr(registry, registry_method).assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("api_account", "expected_disabled"),
    [(False, False), (True, True)],
    ids=["native", "api"],
)
async def test_create_codex_thread_disables_project_config_only_for_api_home(
    tmp_path, api_account, expected_disabled,
):
    codex_home = str((tmp_path / "codex-create-thread-home").resolve())
    registry = MagicMock()
    registry.create_thread = AsyncMock(
        return_value={"id": "thread-created"},
    )
    im = InstanceManager(MagicMock(), MagicMock())
    im._codex_app_server = registry
    admissions = []

    if api_account:
        account = object()
        store = MagicMock()
        store.account_for_codex_home.return_value = account

        @asynccontextmanager
        async def runtime_admission(*args):
            admissions.append(args)
            yield account

        store.runtime_admission = runtime_admission
        im.cloudrouter_store = store

    result = await im.create_codex_thread(
        codex_home,
        cwd="/tmp",
        model="gpt-5.6-sol",
    )

    assert result == {"id": "thread-created"}
    assert (
        registry.create_thread.await_args.kwargs[
            "disable_project_config"
        ]
        is expected_disabled
    )
    assert admissions == (
        [("codex", codex_home, "gpt-5.6-sol")]
        if api_account
        else []
    )


@pytest.mark.asyncio
async def test_api_codex_config_read_enters_store_before_home_gate(tmp_path):
    codex_home = str((tmp_path / "api-codex-home").resolve())
    account = object()
    events = []
    store = MagicMock()
    store.account_for_codex_home.return_value = account

    @asynccontextmanager
    async def configuration_admission(provider, home):
        events.append(("store-enter", provider, home))
        try:
            yield account
        finally:
            events.append(("store-exit", provider, home))

    store.configuration_admission = configuration_admission
    registry = MagicMock()
    registry.read_rate_limits = AsyncMock(return_value={"rateLimits": {}})
    im = InstanceManager(MagicMock(), MagicMock())
    im.cloudrouter_store = store
    im._codex_app_server = registry

    @asynccontextmanager
    async def home_admission(home):
        events.append(("home-enter", home))
        try:
            yield home
        finally:
            events.append(("home-exit", home))

    im.codex_home_app_server_guard = home_admission

    result = await im.read_codex_rate_limits(codex_home)

    assert result == {"rateLimits": {}}
    assert [event[0] for event in events] == [
        "store-enter",
        "home-enter",
        "home-exit",
        "store-exit",
    ]


@pytest.mark.asyncio
async def test_live_codex_quota_holds_home_gate_until_rpc_finishes(tmp_path):
    codex_home = str((tmp_path / "codex-live-quota-gate").resolve())
    read_started = asyncio.Event()
    release_read = asyncio.Event()
    exec_attempted = asyncio.Event()
    exec_entered = asyncio.Event()

    async def read_rate_limits(_codex_home):
        read_started.set()
        await release_read.wait()
        return {"rateLimits": {}}

    registry = MagicMock()
    registry.read_rate_limits = AsyncMock(side_effect=read_rate_limits)
    registry.shutdown_home = AsyncMock(return_value=True)
    im = InstanceManager(MagicMock(), MagicMock())
    im._codex_app_server = registry
    im._ensure_codex_app_server_registry = MagicMock(return_value=registry)

    quota_task = asyncio.create_task(im.read_codex_rate_limits(codex_home))
    await read_started.wait()

    async def enter_exec():
        exec_attempted.set()
        async with im.codex_home_exec_guard(codex_home):
            exec_entered.set()

    exec_task = asyncio.create_task(enter_exec())
    await exec_attempted.wait()
    await asyncio.sleep(0)
    exec_overlapped_quota = exec_entered.is_set()
    release_read.set()

    assert await quota_task == {"rateLimits": {}}
    await exec_task
    assert exec_overlapped_quota is False
    registry.read_rate_limits.assert_awaited_once_with(codex_home)
    registry.shutdown_home.assert_awaited_once_with(
        codex_home,
        require_idle=True,
    )


@pytest.mark.asyncio
async def test_ephemeral_codex_exec_rejects_active_app_server(tmp_path):
    codex_home = str((tmp_path / "codex-ephemeral-vs-app-server").resolve())
    registry = MagicMock()
    registry.shutdown_home = AsyncMock(
        side_effect=CodexAppServerBusyError("active app-server turn")
    )
    im = InstanceManager(MagicMock(), MagicMock())
    im._codex_app_server = registry

    with pytest.raises(CodexAppServerBusyError, match="active app-server turn"):
        async with im.codex_home_exec_guard(codex_home):
            pytest.fail("busy app-server must reject ephemeral exec admission")

    registry.shutdown_home.assert_awaited_once_with(
        codex_home,
        require_idle=True,
    )
    assert im._codex_ephemeral_home_users == {}


@pytest.mark.asyncio
async def test_ephemeral_codex_exec_rejects_direct_exec_owner(tmp_path):
    codex_home = str((tmp_path / "codex-ephemeral-vs-direct").resolve())
    im = InstanceManager(MagicMock(), MagicMock())
    im._codex_exec_homes[7] = codex_home

    with pytest.raises(
        CodexAppServerBusyError,
        match="still has an exec generation",
    ):
        async with im.codex_home_exec_guard(codex_home):
            pytest.fail("direct exec owner must reject ephemeral admission")

    assert im._codex_ephemeral_home_users == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("blocker", ["direct", "ephemeral"])
async def test_codex_direct_exec_rejects_runtime_owned_home(
    db_factory,
    monkeypatch,
    tmp_path,
    blocker,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    async with db_factory() as db:
        inst = Instance(name=f"codex-direct-vs-{blocker}")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    codex_home = str((tmp_path / f"codex-direct-vs-{blocker}").resolve())
    im = InstanceManager(db_factory, MagicMock())
    if blocker == "direct":
        im._codex_exec_homes[999] = codex_home
        error = "still has an exec generation"
    else:
        im._codex_ephemeral_home_users[codex_home] = 1
        error = "active ephemeral exec"

    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
    ) as exec_mock:
        with pytest.raises(CodexAppServerBusyError, match=error):
            await im.launch(
                instance_id=inst.id,
                prompt="must not overlap",
                cwd="/tmp",
                provider="codex",
                config_dir=codex_home,
            )

    exec_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_codex_direct_self_retry_replaces_reaped_home_owner(
    db_factory,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    async with db_factory() as db:
        inst = Instance(name="codex-direct-self-retry")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    home = str((tmp_path / "codex-direct-self-retry-home").resolve())
    previous = _make_mock_process(pid=54320, returncode=1)
    replacement = _make_mock_process(pid=54321, returncode=None)
    finish_replacement = asyncio.Event()

    async def wait_for_replacement():
        await finish_replacement.wait()
        replacement.returncode = 0
        return 0

    replacement.wait = AsyncMock(side_effect=wait_for_replacement)
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.processes[inst.id] = previous
    im._codex_exec_homes[inst.id] = home

    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new=AsyncMock(return_value=replacement),
    ) as exec_mock:
        assert await im.launch(
            instance_id=inst.id,
            prompt="retry on the same account",
            cwd="/tmp",
            provider="codex",
            config_dir=home,
        ) == replacement.pid

    exec_mock.assert_awaited_once()
    assert im.processes[inst.id] is replacement
    assert im._codex_exec_homes[inst.id] == home

    finish_replacement.set()
    await im.wait_for_output_consumer(
        inst.id,
        provider="codex",
        timeout=3,
        expected_process=replacement,
    )


@pytest.mark.asyncio
async def test_concurrent_codex_direct_launches_share_atomic_home_admission(
    db_factory,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    async with db_factory() as db:
        first = Instance(name="codex-direct-race-first")
        second = Instance(name="codex-direct-race-second")
        db.add_all([first, second])
        await db.commit()
        await db.refresh(first)
        await db.refresh(second)

    spawn_entered = asyncio.Event()
    release_spawn = asyncio.Event()
    finish_process = asyncio.Event()
    process = _make_mock_process(pid=54321, returncode=None)

    async def spawn(*_args, **_kwargs):
        spawn_entered.set()
        await release_spawn.wait()
        return process

    async def wait_for_process():
        await finish_process.wait()
        process.returncode = 0
        return 0

    process.wait = AsyncMock(side_effect=wait_for_process)
    home = str((tmp_path / "codex-direct-race-home").resolve())
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))

    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new=AsyncMock(side_effect=spawn),
    ) as exec_mock:
        first_launch = asyncio.create_task(im.launch(
            instance_id=first.id,
            prompt="first",
            cwd="/tmp",
            provider="codex",
            config_dir=home,
        ))
        await spawn_entered.wait()
        second_launch = asyncio.create_task(im.launch(
            instance_id=second.id,
            prompt="second",
            cwd="/tmp",
            provider="codex",
            config_dir=home,
        ))
        await asyncio.sleep(0)
        assert second_launch.done() is False

        release_spawn.set()
        await first_launch
        with pytest.raises(
            CodexAppServerBusyError,
            match="still has an exec generation",
        ):
            await second_launch

    assert exec_mock.await_count == 1
    finish_process.set()
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_codex_exec_shuts_down_idle_app_server_before_spawn(
    db_factory, monkeypatch, tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    async with db_factory() as db:
        inst = Instance(name="codex-exec-after-idle-app-server")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    registry = MagicMock()
    registry.shutdown_home = AsyncMock(return_value=True)
    mock_proc = _make_mock_process()
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._codex_app_server = registry
    codex_home = str((tmp_path / "codex-idle-app-server-home").resolve())

    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
        return_value=mock_proc,
    ) as exec_mock:
        await im.launch(
            instance_id=inst.id,
            prompt="exec after idle transport",
            cwd="/tmp",
            provider="codex",
            config_dir=codex_home,
        )

    registry.shutdown_home.assert_awaited_once_with(
        codex_home,
        require_idle=True,
    )
    exec_mock.assert_awaited_once()
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_codex_exec_does_not_spawn_while_app_server_home_is_busy(
    db_factory, monkeypatch, tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    async with db_factory() as db:
        inst = Instance(name="codex-exec-vs-busy-app-server")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    registry = MagicMock()
    registry.shutdown_home = AsyncMock(
        side_effect=CodexAppServerBusyError("active app-server turn")
    )
    im = InstanceManager(db_factory, MagicMock())
    im._codex_app_server = registry
    codex_home = str((tmp_path / "codex-busy-app-server-home").resolve())

    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
    ) as exec_mock:
        with pytest.raises(CodexAppServerBusyError, match="active app-server turn"):
            await im.launch(
                instance_id=inst.id,
                prompt="must not overlap",
                cwd="/tmp",
                provider="codex",
                config_dir=codex_home,
            )

    registry.shutdown_home.assert_awaited_once_with(
        codex_home,
        require_idle=True,
    )
    exec_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_launch_codex_falls_back_to_exec_when_app_server_fails(
    db_factory, monkeypatch, tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", False)
    async with db_factory() as db:
        inst = Instance(name="codex-fallback-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    im._launch_codex_app_server = AsyncMock(side_effect=RuntimeError("bad protocol"))
    codex_home = tmp_path / "codex-fallback-home"
    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
        return_value=mock_proc,
    ) as exec_mock:
        await im.launch(
            instance_id=inst.id, prompt="fallback", cwd="/tmp", provider="codex",
            config_dir=str(codex_home),
        )

    assert exec_mock.await_args.args[1] == "exec"
    assert "fallback" in exec_mock.await_args.args
    assert exec_mock.await_args.kwargs["env"]["CODEX_HOME"] == str(
        codex_home.resolve()
    )
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_mode",
    ["startup", "missing-thread"],
)
async def test_required_mcp_pre_turn_failure_falls_back_to_equivalent_exec(
    db_factory, monkeypatch, tmp_path, failure_mode, caplog,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    async with db_factory() as db:
        inst = Instance(name="codex-required-mcp-fail-closed")
        db.add(inst)
        await db.flush()
        task = Task(
            title="required MCP task",
            description="must fail closed",
            status="executing",
            provider="codex",
            instance_id=inst.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    broadcaster = MagicMock(broadcast=AsyncMock())
    im = InstanceManager(db_factory, broadcaster)
    im.task_message_enqueuer = AsyncMock()
    im._codex_actual_tier_route_for_home = MagicMock(return_value=(
        CodexTierProxyRoute("https://upstream.example/v1")
    ))
    mock_proc = _make_mock_process()
    startup_error = (
        CodexAppServerError("initialize failed")
        if failure_mode == "startup"
        else None
    )
    thread_response = {"thread": {}} if failure_mode == "missing-thread" else None

    async def ensure_with_test_proxy(server):
        if startup_error is not None:
            raise startup_error
        proxy = MagicMock()
        proxy.is_alive = True
        proxy.close = AsyncMock()
        server._actual_tier_proxy = proxy

    with (
        caplog.at_level("INFO", logger="backend.services.instance_manager"),
        patch(
            "backend.services.skill_context.build_task_skill_context",
            new=AsyncMock(
                return_value=(
                    "## Available Skills\n"
                    "- **review**: Review changes"
                ),
            ),
        ),
        patch(
            "backend.services.codex_app_server.CodexAppServer.ensure_started",
            autospec=True,
            side_effect=ensure_with_test_proxy,
        ) as ensure_started,
        patch(
            "backend.services.codex_app_server.CodexAppServer._request",
            new_callable=AsyncMock,
            return_value=thread_response,
        ) as request,
        patch(
            "backend.services.instance_manager.asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
            return_value=mock_proc,
        ) as exec_mock,
    ):
        await im.launch(
            instance_id=inst.id,
            prompt="must keep required MCP",
            task_id=task.id,
            cwd="/tmp",
            provider="codex",
            config_dir=str(tmp_path / "codex-required-mcp-home"),
        )

    if failure_mode == "startup":
        ensure_started.assert_awaited_once()
        request.assert_not_awaited()
    elif failure_mode == "missing-thread":
        ensure_started.assert_awaited_once()
        request.assert_awaited_once()
    exec_mock.assert_awaited_once()
    argv = list(exec_mock.await_args.args)
    expected_mcp_args = render_codex_exec_config_args(
        build_mcp_server_specs(
            task.id,
            {},
            provider="codex",
            codex_monitor_enabled=True,
        )
    )
    flag_index = argv.index("-c")
    assert argv[flag_index : flag_index + 2] == expected_mcp_args
    assert argv[-1] == (
        "<ccm-task-skill-context>\n"
        "## Available Skills\n- **review**: Review changes\n"
        "</ccm-task-skill-context>\n\n"
        "must keep required MCP"
    )
    assert "route=safe-fallback" in caplog.text
    assert "reason=required-mcp-pre-turn" in caplog.text
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_required_mcp_unknown_app_server_failure_does_not_launch_exec(
    db_factory, monkeypatch, tmp_path, caplog,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    async with db_factory() as db:
        inst = Instance(name="codex-required-mcp-unknown-fail-closed")
        db.add(inst)
        await db.flush()
        task = Task(
            title="required MCP task",
            description="must fail closed",
            status="executing",
            provider="codex",
            instance_id=inst.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    im = InstanceManager(db_factory, MagicMock())
    im._launch_codex_app_server = AsyncMock(
        side_effect=RuntimeError("unexpected adapter failure")
    )
    with (
        caplog.at_level("ERROR", logger="backend.services.instance_manager"),
        patch(
            "backend.services.instance_manager.asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
        ) as exec_mock,
    ):
        with pytest.raises(
            CodexRequiredMcpError,
            match="required ccm_skills could be guaranteed",
        ):
            await im.launch(
                instance_id=inst.id,
                prompt="must keep required MCP",
                task_id=task.id,
                cwd="/tmp",
                provider="codex",
                config_dir=str(tmp_path / "codex-required-mcp-home"),
            )

    exec_mock.assert_not_awaited()
    specs = im._launch_codex_app_server.await_args.kwargs["mcp_specs"]
    assert [spec.name for spec in specs] == [
        "ccm_skills",
        "ccm_frontend_review",
        "ccm_workspace_review",
    ]
    assert specs[0].args[specs[0].args.index("--task-id") + 1] == str(task.id)
    assert "ccm_command_help" in specs[0].enabled_tools
    assert "Codex transport fail-closed" in caplog.text
    assert "reason=required-mcp-not-guaranteed" in caplog.text


@pytest.mark.asyncio
async def test_codex_sub_agent_requires_app_server_and_never_uses_exec(
    db_factory, monkeypatch, tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", False)
    async with db_factory() as db:
        inst = Instance(name="codex-sub-agent-app-server-only")
        db.add(inst)
        await db.flush()
        task = Task(
            title="sub-agent task",
            status="executing",
            provider="codex",
            instance_id=inst.id,
            enabled_skills={"sub-agent": True},
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    im = InstanceManager(db_factory, MagicMock())
    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
    ) as exec_mock:
        with pytest.raises(
            CodexRequiredMcpError,
            match="requires the app-server transport",
        ):
            await im.launch(
                instance_id=inst.id,
                prompt="delegate",
                task_id=task.id,
                cwd="/tmp",
                provider="codex",
                config_dir=str(tmp_path / "codex-sub-agent-no-app-server"),
                enabled_skills={"sub-agent": True},
            )

    exec_mock.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("main_mcp_enabled", [False, True])
async def test_codex_sub_agent_mcp_failure_does_not_launch_exec(
    db_factory, monkeypatch, tmp_path, main_mcp_enabled,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    monkeypatch.setattr(
        settings,
        "codex_main_mcp_enabled",
        main_mcp_enabled,
    )
    async with db_factory() as db:
        inst = Instance(name="codex-sub-agent-mcp-fail-closed")
        db.add(inst)
        await db.flush()
        task = Task(
            title="sub-agent task",
            description="delegate",
            status="executing",
            provider="codex",
            instance_id=inst.id,
            enabled_skills={"sub-agent": True},
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    im = InstanceManager(db_factory, MagicMock())
    im._launch_codex_app_server = AsyncMock(
        side_effect=CodexRequiredMcpPreTurnError(
            "sub-agent transport failed before turn/start"
        )
    )
    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
    ) as exec_mock:
        with pytest.raises(
            CodexRequiredMcpPreTurnError,
            match="before turn/start",
        ):
            await im.launch(
                instance_id=inst.id,
                prompt="must keep sub-agent tools",
                task_id=task.id,
                cwd="/tmp",
                provider="codex",
                config_dir=str(tmp_path / "codex-sub-agent-mcp-home"),
                enabled_skills={"sub-agent": True},
            )

    exec_mock.assert_not_awaited()
    specs = im._launch_codex_app_server.await_args.kwargs["mcp_specs"]
    if main_mcp_enabled:
        assert "ccm_command_help" in specs[0].enabled_tools
        assert "create_sub_agent" in specs[0].enabled_tools
    else:
        controller_spec = next(
            spec for spec in specs if "create_sub_agent" in spec.enabled_tools
        )
        assert set(controller_spec.enabled_tools) == {
            "ccm_read_skill",
            "create_sub_agent",
            "check_sub_agents",
            "stop_sub_agent",
        }
        assert any(spec.name == "ccm_frontend_review" for spec in specs)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "launch_error",
    [
        asyncio.TimeoutError(),
        CodexAppServerBusyError("account busy"),
        CodexRequiredMcpError("required MCP failed"),
        CodexThreadHomeMismatchError("wrong owner"),
        CodexThreadIdentityMismatchError(
            "thread-requested",
            "thread-returned",
            operation="thread/resume",
        ),
        CodexThreadTerminalStateError(
            "terminal-thread",
            "systemError",
            operation="thread/resume turn admission",
            recovery_attempted=True,
        ),
        CodexLaunchCommitError("turn already started"),
    ],
    ids=[
        "timeout",
        "busy",
        "required-mcp",
        "owner-mismatch",
        "identity-mismatch",
        "terminal-thread",
        "commit-failed",
    ],
)
async def test_launch_codex_does_not_fallback_when_replay_is_unsafe(
    db_factory, monkeypatch, tmp_path, launch_error,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    async with db_factory() as db:
        inst = Instance(name="codex-no-fallback-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    im = InstanceManager(db_factory, MagicMock())
    im._launch_codex_app_server = AsyncMock(side_effect=launch_error)
    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
    ) as exec_mock:
        with pytest.raises(type(launch_error)):
            await im.launch(
                instance_id=inst.id,
                prompt="must not replay",
                cwd="/tmp",
                provider="codex",
                config_dir=str(tmp_path / "codex-home"),
            )

    exec_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_fast_launch_refuses_unconfirmed_direct_exec(
    db_factory, monkeypatch, tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    async with db_factory() as db:
        inst = Instance(name="codex-fast-no-app-server")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    im = InstanceManager(db_factory, MagicMock())
    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
    ) as exec_mock:
        with pytest.raises(
            CodexServiceTierUnavailableError,
            match="requires app-server",
        ):
            await im.launch(
                instance_id=inst.id,
                prompt="must be verified",
                cwd="/tmp",
                model="gpt-5.6-sol",
                provider="codex",
                config_dir=str(tmp_path / "codex-home"),
                codex_service_tier="priority",
            )

    exec_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_fast_launch_does_not_use_exec_after_app_server_failure(
    db_factory, monkeypatch, tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", True)
    async with db_factory() as db:
        inst = Instance(name="codex-fast-app-server-failure")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    im = InstanceManager(db_factory, MagicMock())
    im._launch_codex_app_server = AsyncMock(
        side_effect=RuntimeError("protocol unavailable"),
    )
    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
    ) as exec_mock:
        with pytest.raises(
            CodexServiceTierUnavailableError,
            match="refusing unverified exec fallback",
        ):
            await im.launch(
                instance_id=inst.id,
                prompt="must be verified",
                cwd="/tmp",
                model="gpt-5.6-sol",
                provider="codex",
                config_dir=str(tmp_path / "codex-home"),
                codex_service_tier="priority",
            )

    exec_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_codex_started_turn_wraps_generic_persistence_failure(
    db_factory,
):
    process = _make_mock_process(pid=7653)
    registry = MagicMock()
    registry.start_turn = AsyncMock(return_value=(process, "thread-started"))
    im = InstanceManager(db_factory, MagicMock())
    im._ensure_codex_app_server_registry = MagicMock(return_value=registry)
    im._persist_and_track_launch = AsyncMock(
        side_effect=RuntimeError("database commit failed")
    )

    with pytest.raises(CodexLaunchCommitError) as exc_info:
        await im._launch_codex_app_server(
            instance_id=1,
            prompt="must not replay",
            task_id=9,
            cwd="/tmp",
            model="gpt-5.5",
            resume_session_id=None,
            loop_iteration=None,
            git_env=None,
            effort_level="high",
            chat_initiated=False,
            config_dir="/tmp/codex-started",
            enable_workflows=False,
            enabled_skills=None,
        )

    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert "ownership commit failed" in str(exc_info.value)


@pytest.mark.asyncio
async def test_launch_codex_app_server_routes_turn_to_canonical_home(
    db_factory, monkeypatch, tmp_path,
):
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", False)
    async with db_factory() as db:
        inst = Instance(name="codex-registry-inst")
        db.add(inst)
        await db.flush()
        task = Task(
            title="registry-task",
            status="executing",
            provider="codex",
            instance_id=inst.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    process = _make_mock_process(pid=7654)
    registry = MagicMock()
    registry.start_turn = AsyncMock(return_value=(process, "thread-home"))
    codex_home = tmp_path / "account-home"
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.task_message_enqueuer = AsyncMock()

    with patch(
        "backend.services.codex_app_server.CodexAppServerRegistry",
        return_value=registry,
    ) as registry_cls:
        pid = await im._launch_codex_app_server(
            instance_id=inst.id,
            prompt="work",
            task_id=task.id,
            cwd="/tmp",
            model="gpt-5.5",
            resume_session_id="thread-home",
            loop_iteration=None,
            git_env=None,
            effort_level="high",
            chat_initiated=True,
            config_dir=str(codex_home.resolve()),
            enable_workflows=False,
            enabled_skills=None,
            source_log_id=4321,
            current_message="raw work",
            queue_timestamp=12.5,
            codex_service_tier="priority",
        )

    assert pid == 7654
    registry_cls.assert_called_once()
    assert registry.start_turn.await_args.kwargs["codex_home"] == str(
        codex_home.resolve()
    )
    assert registry.start_turn.await_args.kwargs["mcp_specs"] == ()
    assert (
        registry.start_turn.await_args.kwargs["codex_service_tier"]
        == "priority"
    )
    assert im.get_config_dir(inst.id) == str(codex_home.resolve())
    assert im._launch_params[inst.id]["config_dir"] == str(codex_home.resolve())
    assert im._launch_params[inst.id]["source_log_id"] == 4321
    assert im._launch_params[inst.id]["current_message"] == "raw work"
    assert im._launch_params[inst.id]["queue_timestamp"] == 12.5
    assert im._launch_params[inst.id]["codex_service_tier"] == "priority"
    await asyncio.sleep(0.1)
    im.task_message_enqueuer.assert_awaited_once()
    assert (
        im.task_message_enqueuer.await_args.kwargs["source_log_id"]
        == 4321
    )
    assert (
        im.task_message_enqueuer.await_args.kwargs["current_message"]
        == "raw work"
    )
    assert (
        im.task_message_enqueuer.await_args.kwargs["model_override"]
        == "gpt-5.5"
    )
    assert (
        im.task_message_enqueuer.await_args.kwargs["queue_timestamp"]
        == 12.5
    )


@pytest.mark.asyncio
async def test_launch_codex_app_server_uses_passed_task_scoped_specs(
    db_factory, monkeypatch,
):
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    process = _make_mock_process(pid=7655)
    registry = MagicMock()
    registry.start_turn = AsyncMock(return_value=(process, "thread-mcp"))
    im = InstanceManager(db_factory, MagicMock())
    im._ensure_codex_app_server_registry = MagicMock(return_value=registry)
    im._persist_and_track_launch = AsyncMock(return_value=7655)

    pid = await im._launch_codex_app_server(
        instance_id=1,
        prompt="use CCM",
        task_id=73,
        cwd="/tmp",
        model="gpt-5.6-sol",
        resume_session_id="thread-mcp",
        loop_iteration=None,
        git_env=None,
        effort_level="high",
        chat_initiated=False,
        config_dir="/tmp/codex-mcp",
        enable_workflows=False,
        enabled_skills={"monitor": True},
        mcp_specs=build_mcp_server_specs(73, {"monitor": True}),
    )

    assert pid == 7655
    specs = registry.start_turn.await_args.kwargs["mcp_specs"]
    assert [spec.name for spec in specs] == [
        "ccm_skills",
        "ccm_frontend_review",
        "ccm_workspace_review",
    ]
    spec = specs[0]
    assert spec.name == "ccm_skills"
    assert spec.required is True
    assert spec.args[spec.args.index("--task-id") + 1] == "73"
    assert "ccm_command_help" in spec.enabled_tools


@pytest.mark.asyncio
async def test_codex_app_server_uses_passed_sub_agent_controller_specs(
    db_factory, monkeypatch,
):
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", False)
    process = _make_mock_process(pid=7656)
    registry = MagicMock()
    registry.start_turn = AsyncMock(return_value=(process, "thread-sub-agent"))
    im = InstanceManager(db_factory, MagicMock())
    im._ensure_codex_app_server_registry = MagicMock(return_value=registry)
    im._persist_and_track_launch = AsyncMock(return_value=7656)

    await im._launch_codex_app_server(
        instance_id=1,
        prompt="delegate",
        task_id=74,
        cwd="/tmp",
        model="gpt-5.6-sol",
        resume_session_id=None,
        loop_iteration=None,
        git_env=None,
        effort_level="high",
        chat_initiated=False,
        config_dir="/tmp/codex-sub-agent",
        enable_workflows=False,
        enabled_skills={"sub-agent": True},
        mcp_specs=build_sub_agent_controller_mcp_server_specs(74),
    )

    specs = registry.start_turn.await_args.kwargs["mcp_specs"]
    assert len(specs) == 1
    assert specs[0].name == "ccm_skills"
    assert specs[0].required is True
    assert set(specs[0].enabled_tools) == {
        "ccm_read_skill",
        "create_sub_agent",
        "check_sub_agents",
        "stop_sub_agent",
    }
    assert "create_monitor" not in specs[0].enabled_tools
    assert process.unsubscribe_on_terminal is True


@pytest.mark.asyncio
async def test_codex_main_mcp_capability_does_not_change_claude_launch(
    db_factory, monkeypatch,
):
    monkeypatch.setattr(settings, "codex_main_mcp_enabled", True)
    async with db_factory() as db:
        inst = Instance(name="claude-capability-regression")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    process = _make_mock_process(pid=7656)
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._launch_codex_app_server = AsyncMock(
        side_effect=AssertionError("Claude launch reached Codex app-server")
    )

    with (
        patch(
            "backend.services.instance_manager.asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
            return_value=process,
        ) as create_process,
        patch("backend.services.ask_user_settings.ensure_ask_user_hook"),
    ):
        await im.launch(
            instance_id=inst.id,
            prompt="Claude stays Claude",
            cwd="/tmp",
            provider="claude",
        )

    assert create_process.await_args.args[0] == settings.claude_binary
    im._launch_codex_app_server.assert_not_awaited()
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_codex_registry_lifecycle_facades_delegate():
    registry = MagicMock()
    registry.begin_home_maintenance = AsyncMock(return_value=True)
    registry.end_home_maintenance = AsyncMock()
    registry.rebind_thread = AsyncMock()
    im = InstanceManager(MagicMock(), MagicMock())
    im._codex_app_server = registry

    assert await im.shutdown_codex_app_server_home(
        "/tmp/codex-a", require_idle=True,
    ) is True
    await im.rebind_codex_thread(
        "thread-1",
        source_codex_home="/tmp/codex-a",
        target_codex_home="/tmp/codex-b",
    )
    await im.begin_codex_app_server_home_maintenance("/tmp/codex-b")
    await im.end_codex_app_server_home_maintenance("/tmp/codex-b")

    assert registry.begin_home_maintenance.await_args_list[0].kwargs == {
        "require_idle": True,
    }
    assert registry.begin_home_maintenance.await_args_list[0].args == (
        "/tmp/codex-a",
    )
    registry.end_home_maintenance.assert_any_await("/tmp/codex-a")
    registry.begin_home_maintenance.assert_any_await(
        "/tmp/codex-b", require_idle=True,
    )
    registry.end_home_maintenance.assert_any_await("/tmp/codex-b")
    registry.rebind_thread.assert_awaited_once_with(
        "thread-1",
        source_codex_home="/tmp/codex-a",
        target_codex_home="/tmp/codex-b",
    )


@pytest.mark.asyncio
async def test_codex_registry_global_shutdown_clears_reference_after_success():
    registry = MagicMock()
    registry.shutdown = AsyncMock()
    im = InstanceManager(MagicMock(), MagicMock())
    im._codex_app_server = registry

    await im.shutdown_codex_app_server()

    registry.shutdown.assert_awaited_once_with()
    assert im._codex_app_server is None


@pytest.mark.asyncio
async def test_codex_registry_global_shutdown_failure_retains_reference():
    registry = MagicMock()
    registry.shutdown = AsyncMock(side_effect=RuntimeError("group survived"))
    im = InstanceManager(MagicMock(), MagicMock())
    im._codex_app_server = registry

    with pytest.raises(RuntimeError, match="group survived"):
        await im.shutdown_codex_app_server()

    assert im._codex_app_server is registry


@pytest.mark.asyncio
async def test_codex_maintenance_rejects_active_exec_turn(tmp_path):
    home = str((tmp_path / "codex-a").resolve())
    im = InstanceManager(MagicMock(), MagicMock())
    im.processes[7] = MagicMock(returncode=None)
    im._codex_exec_homes[7] = home

    with pytest.raises(CodexAppServerBusyError, match="active exec turn"):
        await im.begin_codex_home_maintenance(home, require_idle=True)

    assert home not in im._codex_home_maintenance
    assert im._codex_app_server is None


@pytest.mark.asyncio
async def test_codex_maintenance_blocks_exec_launch_even_without_app_server(
    db_factory, monkeypatch, tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    async with db_factory() as db:
        inst = Instance(name="codex-maintenance-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)

    home = str((tmp_path / "codex-a").resolve())
    im = InstanceManager(db_factory, MagicMock())
    assert await im.begin_codex_home_maintenance(home) is False
    try:
        with patch(
            "backend.services.instance_manager.asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
        ) as exec_mock:
            with pytest.raises(CodexAppServerBusyError, match="under maintenance"):
                await im.launch(
                    instance_id=inst.id,
                    prompt="must wait",
                    cwd="/tmp",
                    provider="codex",
                    config_dir=home,
                )
        exec_mock.assert_not_awaited()
    finally:
        await im.end_codex_home_maintenance(home)

    assert home not in im._codex_home_maintenance


@pytest.mark.asyncio
async def test_codex_maintenance_reservation_creates_registry_before_first_turn(
    tmp_path,
):
    registry = MagicMock()
    registry.begin_home_maintenance = AsyncMock(return_value=False)
    registry.end_home_maintenance = AsyncMock()
    im = InstanceManager(MagicMock(), MagicMock())

    def ensure_registry():
        im._codex_app_server = registry
        return registry

    im._ensure_codex_app_server_registry = MagicMock(side_effect=ensure_registry)
    home = str((tmp_path / "codex-first").resolve())

    assert await im.begin_codex_home_maintenance(home) is False
    assert home in im._codex_home_maintenance
    await im.end_codex_home_maintenance(home)

    im._ensure_codex_app_server_registry.assert_called_once_with()
    registry.begin_home_maintenance.assert_awaited_once_with(
        home, require_idle=True,
    )
    registry.end_home_maintenance.assert_awaited_once_with(home)


@pytest.mark.asyncio
async def test_codex_registry_legacy_rebind_facade_still_delegates():
    registry = MagicMock()
    registry.rebind_thread = AsyncMock()
    im = InstanceManager(MagicMock(), MagicMock())
    im._codex_app_server = registry

    await im.rebind_codex_app_server_thread(
        "thread-legacy",
        source_codex_home="/tmp/codex-a",
        target_codex_home="/tmp/codex-b",
    )

    registry.rebind_thread.assert_awaited_once_with(
        "thread-legacy",
        source_codex_home="/tmp/codex-a",
        target_codex_home="/tmp/codex-b",
    )


@pytest.mark.asyncio
async def test_codex_shutdown_home_uses_idle_maintenance_gate():
    registry = MagicMock()
    registry.begin_home_maintenance = AsyncMock(return_value=True)
    registry.end_home_maintenance = AsyncMock()
    im = InstanceManager(MagicMock(), MagicMock())
    im._codex_app_server = registry

    assert await im.shutdown_codex_app_server_home(
        "/tmp/codex-a", require_idle=True,
    ) is True
    registry.begin_home_maintenance.assert_awaited_once_with(
        "/tmp/codex-a", require_idle=True,
    )
    registry.end_home_maintenance.assert_awaited_once_with("/tmp/codex-a")


@pytest.mark.asyncio
async def test_codex_chat_pool_rotation_delegates_to_dispatcher_and_relaunches(
    db_factory, tmp_path,
):
    async with db_factory() as db:
        task = Task(
            title="rotate-codex",
            status="executing",
            provider="codex",
            session_id="thread-rotate",
            last_cwd="/tmp",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    new_home = str((tmp_path / "codex-b").resolve())
    dispatcher = MagicMock()
    dispatcher._check_rate_limit_and_rotate = AsyncMock(return_value={
        "config_dir": new_home,
        "session_id": "thread-rotate",
        "excluded": {"codex-a"},
    })
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._launch_params[7] = {
        "provider": "codex",
        "prompt": "continue the task",
        "model": "gpt-5.5",
        "git_env": {},
        "effort_level": "high",
        "enabled_skills": {"monitor": True},
    }
    im.get_recent_log_contents = AsyncMock(return_value=[])
    im.launch = AsyncMock(return_value=999)

    with patch("backend.main.dispatcher", dispatcher):
        rotated = await im._try_chat_pool_rotation(
            7, task.id, 1, "You've hit your usage limit",
        )

    assert rotated is True
    assert dispatcher._check_rate_limit_and_rotate.await_args.args == (
        7, task.id, 1,
    )
    combined = dispatcher._check_rate_limit_and_rotate.await_args.kwargs["combined"]
    assert "usage limit" in combined
    launch_kwargs = im.launch.await_args.kwargs
    assert launch_kwargs["provider"] == "codex"
    assert launch_kwargs["task_id"] == task.id
    assert launch_kwargs["config_dir"] == new_home
    assert launch_kwargs["resume_session_id"] == "thread-rotate"
    assert launch_kwargs["prompt"] == "continue the task"
    assert launch_kwargs["enabled_skills"] == {"monitor": True}


@pytest.mark.asyncio
async def test_codex_chat_pool_rotation_replays_fresh_prompt_without_session(
    db_factory, tmp_path,
):
    """Fresh/compact-retry turns rotate by starting a new thread in the new home."""

    async with db_factory() as db:
        task = Task(
            title="rotate-fresh-codex",
            status="executing",
            provider="codex",
            session_id=None,
            last_cwd="/tmp",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    new_home = str((tmp_path / "codex-b").resolve())
    dispatcher = MagicMock()
    dispatcher._check_rate_limit_and_rotate = AsyncMock(return_value={
        "config_dir": new_home,
        "session_id": None,
        "excluded": {"codex-a"},
    })
    compact_prompt = "[Context compacted]\nsummary\n\n[Message]\ncontinue"
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._launch_params[7] = {
        "provider": "codex",
        "prompt": compact_prompt,
        "model": "gpt-5.5",
        "git_env": {},
        "effort_level": "high",
    }
    im.get_recent_log_contents = AsyncMock(return_value=[])
    im.launch = AsyncMock(return_value=999)

    with patch("backend.main.dispatcher", dispatcher):
        rotated = await im._try_chat_pool_rotation(
            7, task.id, 1, "You've hit your usage limit",
        )

    assert rotated is True
    launch_kwargs = im.launch.await_args.kwargs
    assert launch_kwargs["provider"] == "codex"
    assert launch_kwargs["config_dir"] == new_home
    assert launch_kwargs["resume_session_id"] is None
    assert launch_kwargs["prompt"] == compact_prompt


@pytest.mark.asyncio
async def test_claude_chat_pool_rotation_migration_failure_requeues_without_switch(
    db_factory, tmp_path,
):
    source = tmp_path / "claude-a"
    target = tmp_path / "claude-b"
    config = tmp_path / "claude-pool.json"
    config.write_text(json.dumps({"accounts": [
        {"id": "claude-a", "config_dir": str(source), "enabled": True},
        {"id": "claude-b", "config_dir": str(target), "enabled": True},
    ]}))
    pool = ClaudePool(config_path=config, cooldown_seconds=60)
    pool.record_routed_account(str(source))

    async with db_factory() as db:
        task = Task(
            title="claude migration failure",
            status="executing",
            provider="claude",
            session_id="session-stays-on-source",
            last_cwd="/tmp",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    dispatcher = MagicMock(pool=pool, codex_pool=None)
    dispatcher.enqueue_message = AsyncMock()
    broadcaster = MagicMock(broadcast=AsyncMock())
    im = InstanceManager(db_factory, broadcaster)
    im._config_dirs[7] = str(source)
    im._launch_params[7] = {
        "provider": "claude",
        "prompt": "preserve this exact Claude message",
        "model": "claude-opus-4-8",
    }
    im.get_recent_log_contents = AsyncMock(return_value=[])
    im.launch = AsyncMock(return_value=999)

    with (
        patch("backend.main.dispatcher", dispatcher),
        patch(
            "backend.services.claude_pool.migrate_session",
            return_value=False,
        ) as migrate,
    ):
        rotated = await im._try_chat_pool_rotation(
            7, task.id, 1, "You've hit your limit",
        )

    assert rotated is False
    migrate.assert_called_once_with(
        old_config_dir=str(source),
        new_config_dir=str(target),
        session_id=task.session_id,
    )
    im.launch.assert_not_awaited()
    broadcaster.broadcast.assert_not_awaited()
    dispatcher.enqueue_message.assert_awaited_once_with(
        task_id=task.id,
        prompt="preserve this exact Claude message",
        priority=0,
        source="routing_retry",
        command_skills=None,
        model_override="claude-opus-4-8",
    )
    assert pool.status()["last_selected"] == "claude-a"


def _mock_codex_persist_route(dispatcher, pool):
    """Model Dispatcher binding commit semantics for proactive switch tests."""

    async def persist_route(
        *,
        task_id,
        account_id,
        expected_generation,
        record_route=True,
        on_route_committed=None,
        **_kwargs,
    ):
        if not record_route:
            return True
        changed = await dispatcher._set_codex_task_binding(
            task_id,
            account_id,
            expected_generation=expected_generation,
        )
        if not changed:
            return False
        if on_route_committed is not None:
            on_route_committed()
        home = pool.home_for_account(account_id)
        if home:
            pool.record_routed_account(home)
        return True

    dispatcher._persist_codex_binding_for_route = AsyncMock(
        side_effect=persist_route
    )


@pytest.mark.asyncio
async def test_claude_soft_quota_switch_migrates_before_reset_cooldown(
    db_factory, tmp_path,
):
    source = tmp_path / "claude-a"
    target = tmp_path / "claude-b"
    session_id = "quota-session"
    rollout = source / "projects" / "encoded-cwd" / f"{session_id}.jsonl"
    rollout.parent.mkdir(parents=True)
    rollout.write_text('{"type":"user"}\n')
    config = tmp_path / "claude-pool.json"
    config.write_text(json.dumps({"accounts": [
        {"id": "claude-a", "config_dir": str(source), "enabled": True},
        {"id": "claude-b", "config_dir": str(target), "enabled": True},
    ]}))
    pool = ClaudePool(config_path=config, cooldown_seconds=60)
    pool.fetch_usage = AsyncMock(return_value=[
        {"id": "claude-a", "usage": {"five_hour": {"utilization": 95}}},
        {"id": "claude-b", "usage": {"seven_day": {"utilization": 20}}},
    ])

    async with db_factory() as db:
        task = Task(
            title="claude quota", provider="claude", status="executing",
            session_id=session_id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    broadcaster = MagicMock(broadcast=AsyncMock())
    im = InstanceManager(db_factory, broadcaster)
    im._config_dirs[7] = str(source)
    dispatcher = MagicMock(pool=pool, codex_pool=None)
    dispatcher._set_claude_task_binding = AsyncMock(return_value=True)

    async def persist_claude_route(
        *,
        task_id,
        config_dir,
        expected_generation,
        record_route=True,
        on_route_committed=None,
    ):
        changed = await dispatcher._set_claude_task_binding(
            task_id,
            pool.account_id_from_config_dir(config_dir),
            expected_generation=expected_generation,
        )
        if changed:
            if on_route_committed is not None:
                on_route_committed()
            if record_route:
                pool.record_routed_account(config_dir)
        return changed

    dispatcher._persist_claude_binding_for_route = AsyncMock(
        side_effect=persist_claude_route
    )
    reset_at = time.time() + 3600

    with patch("backend.main.dispatcher", dispatcher):
        switched = await im._try_proactive_pool_switch(
            7,
            task.id,
            rate_limit_info={
                "status": "allowed_warning",
                "rateLimitType": "five_hour",
                "utilization": 0.95,
                "resetsAt": reset_at,
            },
        )

    assert switched is True
    migrated = target / "projects" / "encoded-cwd" / f"{session_id}.jsonl"
    assert migrated.exists()
    assert migrated.stat().st_ino == rollout.stat().st_ino
    assert pool.is_in_cooldown(str(source))
    assert pool._cooldowns["claude-a"] >= reset_at - 2
    assert im.get_config_dir(7) == str(target)
    assert pool.status()["last_selected"] == "claude-b"
    assert dispatcher._set_claude_task_binding.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("migration_exists", [True, False])
async def test_claude_soft_quota_no_usable_target_or_migration_failure_does_not_cool(
    db_factory, tmp_path, migration_exists,
):
    source = tmp_path / "claude-a"
    target = tmp_path / "claude-b"
    session_id = "quota-stays"
    if migration_exists:
        rollout = source / "projects" / "encoded-cwd" / f"{session_id}.jsonl"
        rollout.parent.mkdir(parents=True)
        rollout.write_text("{}\n")
    config = tmp_path / "claude-pool.json"
    config.write_text(json.dumps({"accounts": [
        {"id": "claude-a", "config_dir": str(source), "enabled": True},
        {"id": "claude-b", "config_dir": str(target), "enabled": True},
    ]}))
    pool = ClaudePool(config_path=config, cooldown_seconds=60)
    pool.record_routed_account(str(source))
    if migration_exists:
        # Both accounts are known-high, so selection must stop before migration.
        pool.fetch_usage = AsyncMock(return_value=[
            {"id": "claude-a", "usage": {"five_hour": {"utilization": 95}}},
            {"id": "claude-b", "usage": {"seven_day": {"utilization": 90}}},
        ])
    else:
        # A usable target exists, but the session copy itself fails.
        pool.fetch_usage = AsyncMock(return_value=[
            {"id": "claude-a", "usage": {"five_hour": {"utilization": 95}}},
            {"id": "claude-b", "usage": {"seven_day": {"utilization": 10}}},
        ])

    async with db_factory() as db:
        task = Task(
            title="claude quota stays", provider="claude", status="executing",
            session_id=session_id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._config_dirs[7] = str(source)
    dispatcher = MagicMock(pool=pool, codex_pool=None)
    dispatcher._persist_claude_binding_for_route = AsyncMock(
        return_value=True
    )
    with patch("backend.main.dispatcher", dispatcher):
        switched = await im._try_proactive_pool_switch(
            7,
            task.id,
            rate_limit_info={
                "status": "allowed_warning",
                "rateLimitType": "seven_day",
                "utilization": 0.95,
                "resetsAt": time.time() + 86400,
            },
        )

    assert switched is False
    assert not pool.is_in_cooldown(str(source))
    assert im.get_config_dir(7) == str(source)
    assert pool.status()["last_selected"] == "claude-a"


@pytest.mark.asyncio
async def test_claude_soft_quota_cancel_during_copy_keeps_source_binding(
    db_factory, tmp_path,
):
    from backend.services.claude_pool import migrate_session as real_migrate

    source = tmp_path / "claude-copy-cancel-old"
    target = tmp_path / "claude-copy-cancel-new"
    session_id = "claude-copy-cancel"
    rollout = source / "projects" / "encoded-cwd" / f"{session_id}.jsonl"
    rollout.parent.mkdir(parents=True)
    rollout.write_text('{"type":"user"}\n')
    config = tmp_path / "claude-copy-cancel-pool.json"
    config.write_text(json.dumps({"accounts": [
        {"id": "claude-old", "config_dir": str(source), "enabled": True},
        {"id": "claude-new", "config_dir": str(target), "enabled": True},
    ]}))
    pool = ClaudePool(config_path=config, cooldown_seconds=60)
    pool.record_routed_account(str(source))
    pool.fetch_usage = AsyncMock(return_value=[
        {"id": "claude-old", "usage": {"five_hour": {"utilization": 95}}},
        {"id": "claude-new", "usage": {"seven_day": {"utilization": 10}}},
    ])

    async with db_factory() as db:
        task = Task(
            title="claude copy cancellation",
            provider="claude",
            status="executing",
            session_id=session_id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    copy_started = asyncio.Event()
    release_copy = threading.Event()
    loop = asyncio.get_running_loop()

    def blocked_migrate(*args, **kwargs):
        loop.call_soon_threadsafe(copy_started.set)
        assert release_copy.wait(timeout=2)
        return real_migrate(*args, **kwargs)

    persisted_routes: list[tuple[str | None, bool]] = []

    async def persist_route(
        *,
        task_id,
        config_dir,
        record_route=True,
        on_route_committed=None,
        **_kwargs,
    ):
        account_id = pool.account_id_from_config_dir(config_dir)
        persisted_routes.append((account_id, record_route))
        async with db_factory() as db:
            persisted = await db.get(Task, task_id)
            metadata = dict(persisted.metadata_ or {})
            metadata["claude_account_id"] = account_id
            persisted.metadata_ = metadata
            await db.commit()
        if on_route_committed is not None:
            on_route_committed()
        if record_route:
            pool.record_routed_account(config_dir)
        return True

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._config_dirs[7] = str(source)
    dispatcher = MagicMock(pool=pool, codex_pool=None)
    dispatcher._persist_claude_binding_for_route = AsyncMock(
        side_effect=persist_route
    )

    with (
        patch("backend.main.dispatcher", dispatcher),
        patch(
            "backend.services.claude_pool.migrate_session",
            side_effect=blocked_migrate,
        ),
    ):
        switching = asyncio.create_task(
            im._try_proactive_pool_switch(
                7,
                task.id,
                rate_limit_info={
                    "status": "allowed_warning",
                    "rateLimitType": "five_hour",
                    "utilization": 0.95,
                    "resetsAt": time.time() + 3600,
                },
            )
        )
        await asyncio.wait_for(copy_started.wait(), timeout=1)
        switching.cancel()
        await asyncio.sleep(0)
        switching.cancel()
        await asyncio.sleep(0)
        assert not switching.done()
        release_copy.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(switching, timeout=2)

    migrated = target / "projects" / "encoded-cwd" / f"{session_id}.jsonl"
    assert migrated.exists()
    assert migrated.stat().st_ino == rollout.stat().st_ino
    assert persisted_routes == [("claude-old", False)]
    async with db_factory() as db:
        persisted = await db.get(Task, task.id)
        assert persisted.metadata_["claude_account_id"] == "claude-old"
    assert im.get_config_dir(7) == str(source)
    assert pool.status()["last_selected"] == "claude-old"
    assert not pool.is_in_cooldown(str(source))


@pytest.mark.asyncio
async def test_codex_soft_quota_switch_migrates_rebinds_and_updates_binding(
    db_factory, tmp_path,
):
    source = tmp_path / "codex-a"
    target = tmp_path / "codex-b"
    session_id = "thread-quota"
    rollout = (
        source / "sessions" / "2026" / "07" / "21"
        / f"rollout-2026-07-21T00-00-00-{session_id}.jsonl"
    )
    rollout.parent.mkdir(parents=True)
    rollout.write_text("{}\n")
    config = tmp_path / "codex-pool.json"
    config.write_text(json.dumps({"accounts": [
        {"id": "codex-a", "codex_home": str(source), "enabled": True},
        {"id": "codex-b", "codex_home": str(target), "enabled": True},
    ]}))
    pool = CodexPool(config_path=config)
    pool.select_quota_alternative = AsyncMock(return_value=str(target.resolve()))
    reset_at = time.time() + 7200
    pool._quota_cache = {
        "codex-a": {
            "id": "codex-a",
            "quota": {
                "primary_used_percent": 95,
                "primary_resets_at": time.time() + 300,
                "secondary_used_percent": 90,
                "secondary_resets_at": reset_at,
            },
        }
    }

    async with db_factory() as db:
        task = Task(
            title="codex quota", provider="codex", status="executing",
            session_id=session_id, metadata_={"codex_account_id": "codex-a"},
            codex_service_tier="priority",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._config_dirs[7] = str(source.resolve())
    im.rebind_codex_thread = AsyncMock()
    dispatcher = MagicMock(pool=None, codex_pool=pool)
    dispatcher._set_codex_task_binding = AsyncMock()
    _mock_codex_persist_route(dispatcher, pool)

    with patch("backend.main.dispatcher", dispatcher):
        switched = await im._try_proactive_pool_switch(7, task.id)

    assert switched is True
    pool.select_quota_alternative.assert_awaited_once_with(
        str(source.resolve()),
        model=None,
        service_tier="priority",
    )
    migrated = (
        target / "sessions" / "2026" / "07" / "21"
        / f"rollout-2026-07-21T00-00-00-{session_id}.jsonl"
    )
    assert migrated.exists()
    im.rebind_codex_thread.assert_awaited_once_with(
        session_id,
        source_codex_home=str(source.resolve()),
        target_codex_home=str(target.resolve()),
    )
    dispatcher._set_codex_task_binding.assert_awaited_once()
    binding_call = dispatcher._set_codex_task_binding.await_args
    assert binding_call.args == (task.id, "codex-b")
    binding_generation = binding_call.kwargs["expected_generation"]
    assert binding_generation.task_id == task.id
    assert binding_generation.retry_count == task.retry_count
    assert binding_generation.status == "executing"
    assert pool.is_in_cooldown(str(source))
    assert pool._cooldowns["codex-a"] >= reset_at - 2
    assert im.get_config_dir(7) == str(target.resolve())
    assert pool.status()["last_selected"] == "codex-b"


@pytest.mark.asyncio
async def test_codex_soft_quota_switch_does_not_override_explicit_preference(
    db_factory, tmp_path,
):
    source = tmp_path / "codex-pinned"
    target = tmp_path / "api-account"
    config = tmp_path / "codex-pinned-pool.json"
    config.write_text(json.dumps({"accounts": [
        {"id": "codex-2", "codex_home": str(source), "enabled": True},
        {"id": "api-1", "codex_home": str(target), "enabled": True},
    ]}))
    pool = CodexPool(config_path=config)
    assert pool.set_preferred("codex-2") is True
    pool.select_quota_alternative = AsyncMock(return_value=str(target.resolve()))

    async with db_factory() as db:
        task = Task(
            title="keep explicit codex account",
            provider="codex",
            status="executing",
            session_id="thread-pinned",
            metadata_={"codex_account_id": "codex-2"},
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._config_dirs[7] = str(source.resolve())
    im.rebind_codex_thread = AsyncMock()
    dispatcher = MagicMock(pool=None, codex_pool=pool)

    with patch("backend.main.dispatcher", dispatcher):
        switched = await im._try_proactive_pool_switch(7, task.id)

    assert switched is False
    pool.select_quota_alternative.assert_not_awaited()
    im.rebind_codex_thread.assert_not_awaited()
    assert im.get_config_dir(7) == str(source.resolve())


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback_fails", [False, True])
async def test_codex_soft_quota_binding_failure_rolls_back_owner_without_cooldown(
    db_factory, tmp_path, rollback_fails,
):
    source = tmp_path / "codex-binding-old"
    target = tmp_path / "codex-binding-new"
    session_id = "thread-binding-rollback"
    rollout = (
        source / "sessions" / "2026" / "07" / "21"
        / f"rollout-2026-07-21T00-00-00-{session_id}.jsonl"
    )
    rollout.parent.mkdir(parents=True)
    rollout.write_text("{}\n")
    config = tmp_path / "codex-binding-pool.json"
    config.write_text(json.dumps({"accounts": [
        {"id": "codex-old", "codex_home": str(source), "enabled": True},
        {"id": "codex-new", "codex_home": str(target), "enabled": True},
    ]}))
    pool = CodexPool(config_path=config)
    pool.record_routed_account(str(source))
    pool.select_quota_alternative = AsyncMock(return_value=str(target.resolve()))
    pool._quota_cache = {
        "codex-old": {
            "id": "codex-old",
            "quota": {
                "primary_used_percent": 95,
                "primary_resets_at": time.time() + 3600,
            },
        }
    }

    async with db_factory() as db:
        task = Task(
            title="codex binding rollback", provider="codex", status="executing",
            session_id=session_id, metadata_={"codex_account_id": "codex-old"},
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._config_dirs[7] = str(source.resolve())
    im.rebind_codex_thread = AsyncMock(
        side_effect=[None, RuntimeError("rollback busy")]
        if rollback_fails else None
    )
    im.clear_codex_thread_owner_for_recovery = AsyncMock(return_value=True)
    dispatcher = MagicMock(pool=None, codex_pool=pool)
    dispatcher._set_codex_task_binding = AsyncMock(
        side_effect=RuntimeError("database unavailable")
    )
    _mock_codex_persist_route(dispatcher, pool)

    with patch("backend.main.dispatcher", dispatcher):
        switched = await im._try_proactive_pool_switch(7, task.id)

    assert switched is False
    assert im.rebind_codex_thread.await_args_list == [
        call(
            session_id,
            source_codex_home=str(source.resolve()),
            target_codex_home=str(target.resolve()),
        ),
        call(
            session_id,
            source_codex_home=str(target.resolve()),
            target_codex_home=str(source.resolve()),
        ),
    ]
    assert not pool.is_in_cooldown(str(source))
    assert im.get_config_dir(7) == str(source.resolve())
    assert pool.status()["last_selected"] == "codex-old"
    if rollback_fails:
        im.clear_codex_thread_owner_for_recovery.assert_awaited_once_with(
            session_id,
            expected_codex_home=str(target.resolve()),
        )
    else:
        im.clear_codex_thread_owner_for_recovery.assert_not_awaited()
    async with db_factory() as db:
        persisted = await db.get(Task, task.id)
        assert persisted.metadata_["codex_account_id"] == "codex-old"


@pytest.mark.asyncio
async def test_codex_soft_quota_cancellation_waits_for_binding_rollback(
    db_factory, tmp_path,
):
    source = tmp_path / "codex-cancel-old"
    target = tmp_path / "codex-cancel-new"
    session_id = "thread-binding-cancel"
    rollout = (
        source / "sessions" / "2026" / "07" / "21"
        / f"rollout-2026-07-21T00-00-00-{session_id}.jsonl"
    )
    rollout.parent.mkdir(parents=True)
    rollout.write_text("{}\n")
    config = tmp_path / "codex-cancel-pool.json"
    config.write_text(json.dumps({"accounts": [
        {"id": "codex-old", "codex_home": str(source), "enabled": True},
        {"id": "codex-new", "codex_home": str(target), "enabled": True},
    ]}))
    pool = CodexPool(config_path=config)
    pool.record_routed_account(str(source))
    pool.select_quota_alternative = AsyncMock(return_value=str(target.resolve()))
    pool._quota_cache = {
        "codex-old": {
            "id": "codex-old",
            "quota": {
                "primary_used_percent": 95,
                "primary_resets_at": time.time() + 3600,
            },
        }
    }

    async with db_factory() as db:
        task = Task(
            title="codex binding cancellation",
            provider="codex",
            status="executing",
            session_id=session_id,
            metadata_={"codex_account_id": "codex-old"},
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    binding_started = asyncio.Event()
    release_binding = asyncio.Event()

    async def binding_generation_changed(*args, **kwargs):
        binding_started.set()
        await release_binding.wait()
        return False

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._config_dirs[7] = str(source.resolve())
    im.rebind_codex_thread = AsyncMock()
    dispatcher = MagicMock(pool=None, codex_pool=pool)
    dispatcher._set_codex_task_binding = AsyncMock(
        side_effect=binding_generation_changed
    )
    _mock_codex_persist_route(dispatcher, pool)

    with patch("backend.main.dispatcher", dispatcher):
        switching = asyncio.create_task(
            im._try_proactive_pool_switch(7, task.id)
        )
        await asyncio.wait_for(binding_started.wait(), timeout=1)
        switching.cancel()
        await asyncio.sleep(0)
        switching.cancel()
        await asyncio.sleep(0)
        assert not switching.done()
        release_binding.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(switching, timeout=1)

    assert im.rebind_codex_thread.await_args_list == [
        call(
            session_id,
            source_codex_home=str(source.resolve()),
            target_codex_home=str(target.resolve()),
        ),
        call(
            session_id,
            source_codex_home=str(target.resolve()),
            target_codex_home=str(source.resolve()),
        ),
    ]
    assert not pool.is_in_cooldown(str(source))
    assert im.get_config_dir(7) == str(source.resolve())
    assert pool.status()["last_selected"] == "codex-old"
    async with db_factory() as db:
        persisted = await db.get(Task, task.id)
        assert persisted.metadata_["codex_account_id"] == "codex-old"


@pytest.mark.asyncio
async def test_codex_soft_quota_cancel_during_copy_finishes_switch(
    db_factory, tmp_path,
):
    from backend.services.codex_session_migration import (
        migrate_codex_rollout_session as real_migrate,
    )

    source = tmp_path / "codex-copy-cancel-old"
    target = tmp_path / "codex-copy-cancel-new"
    session_id = "thread-copy-cancel"
    rollout = (
        source / "sessions" / "2026" / "07" / "21"
        / f"rollout-2026-07-21T00-00-00-{session_id}.jsonl"
    )
    rollout.parent.mkdir(parents=True)
    rollout.write_text("{}\n")
    config = tmp_path / "codex-copy-cancel-pool.json"
    config.write_text(json.dumps({"accounts": [
        {"id": "codex-old", "codex_home": str(source), "enabled": True},
        {"id": "codex-new", "codex_home": str(target), "enabled": True},
    ]}))
    pool = CodexPool(config_path=config)
    pool.record_routed_account(str(source))
    pool.select_quota_alternative = AsyncMock(
        return_value=str(target.resolve())
    )
    pool._quota_cache = {
        "codex-old": {
            "id": "codex-old",
            "quota": {
                "primary_used_percent": 95,
                "primary_resets_at": time.time() + 3600,
            },
        }
    }

    async with db_factory() as db:
        task = Task(
            title="codex copy cancellation",
            provider="codex",
            status="executing",
            session_id=session_id,
            metadata_={"codex_account_id": "codex-old"},
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    copy_started = asyncio.Event()
    release_copy = threading.Event()
    loop = asyncio.get_running_loop()

    def blocked_migrate(*args, **kwargs):
        loop.call_soon_threadsafe(copy_started.set)
        assert release_copy.wait(timeout=2)
        return real_migrate(*args, **kwargs)

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._config_dirs[7] = str(source.resolve())
    im.rebind_codex_thread = AsyncMock()
    dispatcher = MagicMock(pool=None, codex_pool=pool)
    dispatcher._set_codex_task_binding = AsyncMock(return_value=True)
    _mock_codex_persist_route(dispatcher, pool)

    with (
        patch("backend.main.dispatcher", dispatcher),
        patch(
            "backend.services.codex_session_migration."
            "migrate_codex_rollout_session",
            side_effect=blocked_migrate,
        ),
    ):
        switching = asyncio.create_task(
            im._try_proactive_pool_switch(7, task.id)
        )
        await asyncio.wait_for(copy_started.wait(), timeout=1)
        switching.cancel()
        await asyncio.sleep(0)
        switching.cancel()
        await asyncio.sleep(0)
        assert not switching.done()
        release_copy.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(switching, timeout=2)

    assert dispatcher._persist_codex_binding_for_route.await_count == 2
    dispatcher._persist_codex_binding_for_route.assert_any_await(
        task_id=task.id,
        account_id="codex-old",
        expected_generation=dispatcher._set_codex_task_binding.await_args.kwargs[
            "expected_generation"
        ],
        record_route=False,
    )
    dispatcher._set_codex_task_binding.assert_awaited_once()
    im.rebind_codex_thread.assert_awaited_once_with(
        session_id,
        source_codex_home=str(source.resolve()),
        target_codex_home=str(target.resolve()),
    )
    assert im.get_config_dir(7) == str(target.resolve())
    assert pool.status()["last_selected"] == "codex-new"
    assert pool.is_in_cooldown(str(source))


@pytest.mark.asyncio
async def test_codex_soft_quota_direct_binding_cancel_keeps_committed_owner(
    db_factory, tmp_path,
):
    source = tmp_path / "codex-direct-cancel-old"
    target = tmp_path / "codex-direct-cancel-new"
    session_id = "thread-direct-binding-cancel"
    rollout = (
        source / "sessions" / "2026" / "07" / "21"
        / f"rollout-2026-07-21T00-00-00-{session_id}.jsonl"
    )
    rollout.parent.mkdir(parents=True)
    rollout.write_text("{}\n")
    config = tmp_path / "codex-direct-cancel-pool.json"
    config.write_text(json.dumps({"accounts": [
        {"id": "codex-old", "codex_home": str(source), "enabled": True},
        {"id": "codex-new", "codex_home": str(target), "enabled": True},
    ]}))
    pool = CodexPool(config_path=config)
    pool.record_routed_account(str(source))
    pool.select_quota_alternative = AsyncMock(
        return_value=str(target.resolve())
    )
    pool._quota_cache = {
        "codex-old": {
            "id": "codex-old",
            "quota": {
                "primary_used_percent": 95,
                "primary_resets_at": time.time() + 3600,
            },
        }
    }

    async with db_factory() as db:
        task = Task(
            title="codex direct binding cancellation",
            provider="codex",
            status="executing",
            session_id=session_id,
            metadata_={"codex_account_id": "codex-old"},
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._config_dirs[7] = str(source.resolve())
    im.rebind_codex_thread = AsyncMock()
    dispatcher = MagicMock(pool=None, codex_pool=pool)

    async def persist_then_cancel(
        *,
        task_id,
        account_id,
        record_route=True,
        on_route_committed=None,
        **_kwargs,
    ):
        if not record_route:
            return True
        async with db_factory() as db:
            persisted = await db.get(Task, task_id)
            metadata = dict(persisted.metadata_ or {})
            metadata["codex_account_id"] = account_id
            persisted.metadata_ = metadata
            await db.commit()
        if on_route_committed is not None:
            on_route_committed()
        home = pool.home_for_account(account_id)
        assert home is not None
        pool.record_routed_account(home)
        raise asyncio.CancelledError()

    dispatcher._persist_codex_binding_for_route = AsyncMock(
        side_effect=persist_then_cancel
    )

    with patch("backend.main.dispatcher", dispatcher):
        with pytest.raises(asyncio.CancelledError):
            await im._try_proactive_pool_switch(7, task.id)

    assert im.rebind_codex_thread.await_args_list == [
        call(
            session_id,
            source_codex_home=str(source.resolve()),
            target_codex_home=str(target.resolve()),
        )
    ]
    assert im.get_config_dir(7) == str(target.resolve())
    assert pool.status()["last_selected"] == "codex-new"
    async with db_factory() as db:
        persisted = await db.get(Task, task.id)
        assert persisted.metadata_["codex_account_id"] == "codex-new"
    # Cancellation arrived immediately after the binding commit, so quota
    # bookkeeping/broadcast may be skipped, but owner + durable route agree.
    assert not pool.is_in_cooldown(str(source))


@pytest.mark.asyncio
async def test_codex_soft_quota_rebind_failure_keeps_old_home_available(
    db_factory, tmp_path,
):
    source = tmp_path / "codex-rebind-old"
    target = tmp_path / "codex-rebind-new"
    session_id = "thread-rebind-fails"
    rollout = (
        source / "sessions" / "2026" / "07" / "21"
        / f"rollout-2026-07-21T00-00-00-{session_id}.jsonl"
    )
    rollout.parent.mkdir(parents=True)
    rollout.write_text("{}\n")
    config = tmp_path / "codex-rebind-pool.json"
    config.write_text(json.dumps({"accounts": [
        {"id": "codex-old", "codex_home": str(source), "enabled": True},
        {"id": "codex-new", "codex_home": str(target), "enabled": True},
    ]}))
    pool = CodexPool(config_path=config)
    pool.record_routed_account(str(source))
    pool.select_quota_alternative = AsyncMock(return_value=str(target.resolve()))
    pool._quota_cache = {
        "codex-old": {
            "id": "codex-old",
            "quota": {
                "primary_used_percent": 95,
                "primary_resets_at": time.time() + 3600,
            },
        }
    }

    async with db_factory() as db:
        task = Task(
            title="codex rebind failure", provider="codex", status="executing",
            session_id=session_id, metadata_={"codex_account_id": "codex-old"},
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._config_dirs[7] = str(source.resolve())
    im.rebind_codex_thread = AsyncMock(side_effect=RuntimeError("target busy"))
    dispatcher = MagicMock(pool=None, codex_pool=pool)
    dispatcher._set_codex_task_binding = AsyncMock()
    _mock_codex_persist_route(dispatcher, pool)

    with patch("backend.main.dispatcher", dispatcher):
        switched = await im._try_proactive_pool_switch(7, task.id)

    assert switched is False
    dispatcher._set_codex_task_binding.assert_not_awaited()
    assert not pool.is_in_cooldown(str(source))
    assert im.get_config_dir(7) == str(source.resolve())
    assert pool.status()["last_selected"] == "codex-old"


@pytest.mark.asyncio
async def test_codex_chat_routing_error_requeues_prompt_and_cleans_failed_turn(
    db_factory,
):
    from backend.services.dispatcher import (
        CodexAccountRoutingError,
        PRIORITY_USER,
    )

    async with db_factory() as db:
        task = Task(
            title="route-retry-codex",
            status="executing",
            provider="codex",
            session_id="thread-route",
            last_cwd="/tmp",
        )
        inst = Instance(name="route-retry-inst", status="running")
        db.add_all([task, inst])
        await db.flush()
        inst.current_task_id = task.id
        await db.commit()
        await db.refresh(task)
        await db.refresh(inst)

    dispatcher = MagicMock()
    dispatcher._check_rate_limit_and_rotate = AsyncMock(side_effect=
        CodexAccountRoutingError(
            "rollout migration is temporarily unavailable", retry_after=5,
        )
    )
    dispatcher.enqueue_message = AsyncMock()
    broadcaster = MagicMock(broadcast=AsyncMock())
    im = InstanceManager(db_factory, broadcaster)
    process = _make_mock_process(returncode=1)
    im.processes[inst.id] = process
    im._launch_params[inst.id] = {
        "provider": "codex",
        "prompt": "preserve this exact user prompt",
        "model": "gpt-5.5",
        "queue_timestamp": 42.5,
    }
    im.get_recent_log_contents = AsyncMock(return_value=[])

    with patch("backend.main.dispatcher", dispatcher):
        await im._consume_output(
            inst.id,
            task.id,
            process,
            chat_initiated=True,
            provider="codex",
        )

    dispatcher.enqueue_message.assert_awaited_once_with(
        task_id=task.id,
        prompt="preserve this exact user prompt",
        priority=PRIORITY_USER,
        source="routing_retry",
        command_skills=None,
        model_override="gpt-5.5",
        queue_timestamp=42.5,
    )
    async with db_factory() as db:
        refreshed_task = await db.get(Task, task.id)
        refreshed_inst = await db.get(Instance, inst.id)
    assert refreshed_task.status == "failed"
    assert refreshed_inst.status == "error"
    assert inst.id not in im.processes


@pytest.mark.asyncio
async def test_codex_transient_replacement_busy_requeues_exact_prompt(
    db_factory, monkeypatch,
):
    import backend.services.claude_pool as claude_pool_module

    async with db_factory() as db:
        task = Task(
            title="transient replacement busy",
            status="executing",
            provider="codex",
            session_id="thread-transient-busy",
            last_cwd="/tmp",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    dispatcher = MagicMock()
    dispatcher.enqueue_message = AsyncMock()
    broadcaster = MagicMock(broadcast=AsyncMock())
    im = InstanceManager(db_factory, broadcaster)
    im._config_dirs[7] = "/tmp/codex-a"
    im._launch_params[7] = {
        "provider": "codex",
        "prompt": "preserve transient prompt",
        "model": "gpt-5.5",
        "enabled_skills": {"sub-agent": True},
    }
    im.get_recent_log_contents = AsyncMock(return_value=[])
    im.launch = AsyncMock(
        side_effect=CodexAppServerBusyError("account under maintenance")
    )
    monkeypatch.setattr(
        claude_pool_module, "transient_retry_delay", lambda *_args: 0,
    )

    with patch("backend.main.dispatcher", dispatcher):
        launched = await im._try_chat_transient_retry(
            7, task.id, 1, "request timed out",
        )

    assert launched is False
    assert im.launch.await_args.kwargs["task_id"] == task.id
    assert im.launch.await_args.kwargs["resume_session_id"] == task.session_id
    assert im.launch.await_args.kwargs["enabled_skills"] == {
        "sub-agent": True,
    }
    dispatcher.enqueue_message.assert_awaited_once_with(
        task_id=task.id,
        prompt="preserve transient prompt",
        priority=0,
        source="routing_retry",
        command_skills={"sub-agent": True},
        model_override="gpt-5.5",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "detail",
    [
        "all logged-in accounts are busy",
        "no eligible logged-in account is ready",
    ],
)
async def test_apex_409_capacity_terminal_failure_retries_same_account(
    db_factory, monkeypatch, detail,
):
    import backend.services.claude_pool as claude_pool_module

    async with db_factory() as db:
        task = Task(
            title="apex busy retry",
            status="executing",
            provider="codex",
            session_id="thread-apex-busy",
            last_cwd="/tmp/apex-work",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    broadcaster = MagicMock(broadcast=AsyncMock())
    im = InstanceManager(db_factory, broadcaster)
    im._config_dirs[7] = "/api/apex/codex"
    im._launch_params[7] = {
        "provider": "codex",
        "prompt": "continue after gateway capacity recovers",
        "model": "gpt-5.6-sol",
    }
    store = MagicMock()
    store.account_for_codex_home.return_value = types.SimpleNamespace(
        api_provider="apex",
    )
    im.cloudrouter_store = store
    im.get_recent_log_contents = AsyncMock(return_value=[json.dumps({
        "type": "turn.failed",
        "error": {
            "message": "Reconnecting... 5/5",
            "codexErrorInfo": {
                "responseStreamDisconnected": {"httpStatusCode": 409},
            },
            "additionalDetails": (
                "unexpected status 409 Conflict: "
                f'{{"detail":"{detail}"}}'
            ),
        },
    })])
    im.launch = AsyncMock(return_value=12345)
    monkeypatch.setattr(
        claude_pool_module, "transient_retry_delay", lambda *_args: 0,
    )

    launched = await im._try_chat_transient_retry(
        7, task.id, 1, "Reconnecting... 5/5",
    )

    assert launched is True
    im.launch.assert_awaited_once()
    retry = im.launch.await_args.kwargs
    assert retry["task_id"] == task.id
    assert retry["resume_session_id"] == "thread-apex-busy"
    assert retry["config_dir"] == "/api/apex/codex"
    assert retry["provider"] == "codex"
    assert retry["model"] == "gpt-5.6-sol"
    broadcaster.broadcast.assert_awaited_once()


@pytest.mark.asyncio
async def test_codex_pool_replacement_busy_requeues_exact_prompt(db_factory):
    async with db_factory() as db:
        task = Task(
            title="pool replacement busy",
            status="executing",
            provider="codex",
            session_id="thread-pool-busy",
            last_cwd="/tmp",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

    dispatcher = MagicMock()
    dispatcher._check_rate_limit_and_rotate = AsyncMock(return_value={
        "config_dir": "/tmp/codex-b",
        "session_id": task.session_id,
    })
    dispatcher.enqueue_message = AsyncMock()
    broadcaster = MagicMock(broadcast=AsyncMock())
    im = InstanceManager(db_factory, broadcaster)
    im._launch_params[7] = {
        "provider": "codex",
        "prompt": "preserve rotation prompt",
        "model": "gpt-5.5",
    }
    im.get_recent_log_contents = AsyncMock(return_value=[])
    im.launch = AsyncMock(
        side_effect=CodexThreadHomeMismatchError("thread is being rebound")
    )

    with patch("backend.main.dispatcher", dispatcher):
        launched = await im._try_chat_pool_rotation(
            7, task.id, 1, "You've hit your usage limit",
        )

    assert launched is False
    dispatcher.enqueue_message.assert_awaited_once_with(
        task_id=task.id,
        prompt="preserve rotation prompt",
        priority=0,
        source="routing_retry",
        command_skills=None,
        model_override="gpt-5.5",
    )


@pytest.mark.asyncio
async def test_launch_codex_no_thinking_budget_env(db_factory, monkeypatch):
    """launch(provider='codex', thinking_budget=N) does NOT set MAX_THINKING_TOKENS."""
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    async with db_factory() as db:
        inst = Instance(name="codex-think-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc) as mock_exec:
        await im.launch(instance_id=inst_id, prompt="hi", cwd="/tmp", provider="codex", thinking_budget=12000)

    env = mock_exec.call_args[1]["env"]
    assert "MAX_THINKING_TOKENS" not in env
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_launch_without_effort_level_omits_flag(db_factory):
    """launch() without effort_level does not include --effort."""
    async with db_factory() as db:
        inst = Instance(name="no-effort-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc) as mock_exec:
        await im.launch(instance_id=inst_id, prompt="hi", cwd="/tmp")

    cmd_args = mock_exec.call_args[0]
    assert "--effort" not in cmd_args
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_stop_terminates(db_factory):
    """stop() sends SIGINT first and updates DB status."""
    async with db_factory() as db:
        inst = Instance(name="stop-inst", status="running")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = MagicMock()
    mock_proc.returncode = None  # Still running
    mock_proc.terminate = MagicMock()
    mock_proc.send_signal = MagicMock()
    mock_proc.wait = AsyncMock(return_value=0)
    mock_proc.kill = MagicMock()

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    im.processes[inst_id] = mock_proc

    # After SIGINT, wait() succeeds — set returncode
    async def fake_wait():
        mock_proc.returncode = 0
        return 0
    mock_proc.wait = fake_wait

    result = await im.stop(inst_id)
    assert result is True
    mock_proc.send_signal.assert_called_once_with(signal.SIGINT)

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "idle"


@pytest.mark.asyncio
async def test_stop_kills_on_timeout(db_factory):
    """stop() sends SIGKILL after timeout."""
    async with db_factory() as db:
        inst = Instance(name="kill-inst", status="running")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = MagicMock()
    mock_proc.returncode = None
    mock_proc.terminate = MagicMock()
    mock_proc.kill = MagicMock()

    # After kill, wait() succeeds
    async def post_kill_wait():
        mock_proc.returncode = -9
        return -9

    mock_proc.wait = post_kill_wait

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    im.processes[inst_id] = mock_proc

    # The parent/tree waiter times out after SIGINT and SIGTERM, then succeeds
    # after SIGKILL.
    with patch.object(
        im,
        "_wait_process_tree",
        new_callable=AsyncMock,
        side_effect=[asyncio.TimeoutError, asyncio.TimeoutError, None],
    ):
        result = await im.stop(inst_id)

    assert result is True
    mock_proc.terminate.assert_called_once()
    mock_proc.kill.assert_called_once()


@pytest.mark.asyncio
async def test_stop_codex_turn_uses_registry_fail_closed_cleanup(db_factory):
    """A native turn adapter must never be treated as a POSIX process group."""
    async with db_factory() as db:
        inst = Instance(name="codex-stop-inst", status="running", pid=43_210)
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    async def interrupt():
        raise AssertionError("InstanceManager must use registry cleanup")

    process = CodexTurnProcess(43_210, interrupt, thread_id="thread-stuck")
    registry = MagicMock()

    async def stop_claimed_turn(codex_home, exact_process, *, reason):
        assert codex_home == "/tmp/codex-stop-home"
        assert exact_process is process
        assert reason == "CCM task session interrupted"
        process.finish(
            130,
            termination_kind="internal_abort",
        )
        return True

    registry.stop_claimed_turn = AsyncMock(side_effect=stop_claimed_turn)
    registry.abort_unclaimed_turn = AsyncMock(
        side_effect=AssertionError(
            "a durable Instance owner must not use unclaimed-turn cleanup"
        )
    )
    broadcaster = MagicMock(broadcast=AsyncMock())
    im = InstanceManager(db_factory, broadcaster)
    im._codex_app_server = registry
    im._config_dirs[inst_id] = "/tmp/codex-stop-home"
    im.processes[inst_id] = process

    with (
        patch.object(
            im,
            "_signal_managed_process_tree",
            new_callable=AsyncMock,
        ) as signal_tree,
        patch.object(
            im,
            "_wait_process_tree",
            new_callable=AsyncMock,
        ) as wait_tree,
    ):
        assert await im.stop(inst_id) is True

    registry.stop_claimed_turn.assert_awaited_once()
    registry.abort_unclaimed_turn.assert_not_awaited()
    signal_tree.assert_not_awaited()
    wait_tree.assert_not_awaited()
    assert process.returncode == 130
    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "idle"
        assert inst.pid is None


@pytest.mark.asyncio
async def test_stop_codex_turn_preserves_claim_when_shared_transport_is_busy(
    db_factory,
):
    """An unisolatable stop must not tear down another turn on the home."""

    shared_pid = 43_211
    first_started_at = datetime(2026, 8, 3, 11, 13, 30)
    peer_started_at = datetime(2026, 8, 3, 11, 13, 31)
    async with db_factory() as db:
        first_instance = Instance(
            name="codex-shared-stop-target",
            status="running",
            provider="codex",
            pid=shared_pid,
            started_at=first_started_at,
        )
        peer_instance = Instance(
            name="codex-shared-stop-peer",
            status="running",
            provider="codex",
            pid=shared_pid,
            started_at=peer_started_at,
        )
        db.add_all([first_instance, peer_instance])
        await db.flush()
        first_task = Task(
            title="shared stop target",
            status="executing",
            provider="codex",
            instance_id=first_instance.id,
        )
        peer_task = Task(
            title="shared stop peer",
            status="executing",
            provider="codex",
            instance_id=peer_instance.id,
        )
        db.add_all([first_task, peer_task])
        await db.flush()
        first_instance.current_task_id = first_task.id
        peer_instance.current_task_id = peer_task.id
        await db.commit()
        first_instance_id = first_instance.id
        peer_instance_id = peer_instance.id
        first_task_id = first_task.id
        peer_task_id = peer_task.id

    async def interrupt():
        raise AssertionError("the registry owns exact-turn interruption")

    first_process = CodexTurnProcess(
        shared_pid,
        interrupt,
        thread_id="thread-stop-target",
    )
    peer_process = CodexTurnProcess(
        shared_pid,
        interrupt,
        thread_id="thread-stop-peer",
    )
    first_release = asyncio.Event()
    peer_release = asyncio.Event()
    first_consumer = asyncio.create_task(first_release.wait())
    peer_consumer = asyncio.create_task(peer_release.wait())
    registry = MagicMock()
    registry.stop_claimed_turn = AsyncMock(
        side_effect=CodexSharedTransportBusyError(
            "cannot isolate the requested turn while a peer is active"
        )
    )
    registry.abort_unclaimed_turn = AsyncMock(
        side_effect=AssertionError(
            "a durable Instance owner must not shut down shared transport"
        )
    )
    broadcaster = MagicMock(broadcast=AsyncMock())
    manager = InstanceManager(db_factory, broadcaster)
    manager._codex_app_server = registry
    manager._config_dirs[first_instance_id] = "/tmp/codex-shared-home"
    manager._config_dirs[peer_instance_id] = "/tmp/codex-shared-home"
    manager.processes[first_instance_id] = first_process
    manager.processes[peer_instance_id] = peer_process
    manager._track_output_consumer(
        first_instance_id,
        first_process,
        first_consumer,
        provider="codex",
        task_id=first_task_id,
        task_retry_count=0,
        instance_started_at=first_started_at,
    )
    manager._track_output_consumer(
        peer_instance_id,
        peer_process,
        peer_consumer,
        provider="codex",
        task_id=peer_task_id,
        task_retry_count=0,
        instance_started_at=peer_started_at,
    )

    try:
        assert await manager.stop(
            first_instance_id,
            expected_task_id=first_task_id,
            expected_pid=shared_pid,
            expected_started_at=first_started_at,
            task_status="completed",
            consumer_cancel_timeout=0.01,
        ) is False

        registry.stop_claimed_turn.assert_awaited_once_with(
            "/tmp/codex-shared-home",
            first_process,
            reason="CCM task session interrupted",
        )
        registry.abort_unclaimed_turn.assert_not_awaited()
        assert first_process.returncode is None
        assert peer_process.returncode is None
        assert not first_consumer.done()
        assert not first_consumer.cancelling()
        assert not peer_consumer.done()
        assert not peer_consumer.cancelling()
        assert manager.processes[first_instance_id] is first_process
        assert manager.processes[peer_instance_id] is peer_process
        assert manager._tasks[first_instance_id] is first_consumer
        assert manager._tasks[peer_instance_id] is peer_consumer
        assert manager._consumer_records[first_instance_id].process is first_process
        assert manager._consumer_records[peer_instance_id].process is peer_process
        broadcaster.broadcast.assert_not_awaited()

        async with db_factory() as db:
            durable_first_instance = await db.get(Instance, first_instance_id)
            durable_peer_instance = await db.get(Instance, peer_instance_id)
            durable_first_task = await db.get(Task, first_task_id)
            durable_peer_task = await db.get(Task, peer_task_id)
            assert durable_first_instance.status == "running"
            assert durable_first_instance.pid == shared_pid
            assert durable_first_instance.current_task_id == first_task_id
            assert durable_peer_instance.status == "running"
            assert durable_peer_instance.pid == shared_pid
            assert durable_peer_instance.current_task_id == peer_task_id
            assert durable_first_task.status == "executing"
            assert durable_first_task.instance_id == first_instance_id
            assert durable_peer_task.status == "executing"
            assert durable_peer_task.instance_id == peer_instance_id
    finally:
        first_release.set()
        peer_release.set()
        await asyncio.gather(first_consumer, peer_consumer)


@pytest.mark.asyncio
async def test_concurrent_launches_cannot_spawn_twice_for_one_instance():
    im = InstanceManager(MagicMock(), MagicMock())
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    calls = 0
    active_process = MagicMock(returncode=None, pid=101)

    async def fake_launch_locked(**kwargs):
        nonlocal calls
        calls += 1
        first_entered.set()
        await release_first.wait()
        im.processes[kwargs["instance_id"]] = active_process
        return active_process.pid

    im._launch_locked = fake_launch_locked
    first = asyncio.create_task(im.launch(1, "first"))
    await first_entered.wait()
    second = asyncio.create_task(im.launch(1, "second"))
    await asyncio.sleep(0)

    assert calls == 1
    release_first.set()
    assert await first == 101
    with pytest.raises(RuntimeError, match="already running"):
        await second
    assert calls == 1


@pytest.mark.asyncio
async def test_external_launch_does_not_deadlock_consumer_self_retry():
    im = InstanceManager(MagicMock(), MagicMock())
    instance_id = 8
    old_process = MagicMock(returncode=1, pid=301)
    new_process = MagicMock(returncode=None, pid=302)
    allow_retry = asyncio.Event()
    retry_launched = asyncio.Event()

    async def fake_launch_locked(**kwargs):
        assert kwargs["prompt"] == "consumer retry"
        im.processes[instance_id] = new_process
        retry_launched.set()
        return new_process.pid

    im._launch_locked = fake_launch_locked
    im.processes[instance_id] = old_process

    async def consumer_retry():
        await allow_retry.wait()
        return await im.launch(instance_id, "consumer retry")

    consumer = asyncio.create_task(consumer_retry())
    im._tasks[instance_id] = consumer
    external = asyncio.create_task(im.launch(instance_id, "external"))
    await asyncio.sleep(0)
    allow_retry.set()

    assert await asyncio.wait_for(consumer, timeout=1) == new_process.pid
    await asyncio.wait_for(retry_launched.wait(), timeout=1)
    with pytest.raises(InstanceAlreadyRunningError):
        await asyncio.wait_for(external, timeout=1)


@pytest.mark.asyncio
async def test_stop_serializes_relaunch_and_preserves_new_process(db_factory):
    async with db_factory() as db:
        inst = Instance(name="stop-relaunch", status="running")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        instance_id = inst.id

    wait_entered = asyncio.Event()
    allow_old_exit = asyncio.Event()
    old_process = MagicMock(returncode=None, pid=201)
    old_process.send_signal = MagicMock()

    async def wait_old():
        wait_entered.set()
        await allow_old_exit.wait()
        old_process.returncode = 0
        return 0

    old_process.wait = wait_old
    new_process = MagicMock(returncode=None, pid=202)
    launch_entered = asyncio.Event()
    broadcaster = MagicMock(broadcast=AsyncMock())
    im = InstanceManager(db_factory, broadcaster)
    im.processes[instance_id] = old_process

    async def fake_launch_locked(**kwargs):
        launch_entered.set()
        im.processes[kwargs["instance_id"]] = new_process
        return new_process.pid

    im._launch_locked = fake_launch_locked
    stopping = asyncio.create_task(im.stop(instance_id))
    await wait_entered.wait()
    relaunch = asyncio.create_task(im.launch(instance_id, "next"))
    await asyncio.sleep(0)
    assert not launch_entered.is_set()

    allow_old_exit.set()
    assert await stopping is True
    assert await relaunch == new_process.pid
    assert im.processes[instance_id] is new_process


@pytest.mark.asyncio
async def test_stop_awaits_codex_consumer_after_process_already_exited(db_factory):
    async with db_factory() as db:
        inst = Instance(name="terminal-consumer", status="running")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        instance_id = inst.id

    process = MagicMock(returncode=1, pid=401)
    process.send_signal = MagicMock()
    consumer_started = asyncio.Event()
    finish_bookkeeping = asyncio.Event()

    async def finish_rollout_binding():
        consumer_started.set()
        await finish_bookkeeping.wait()

    consumer = asyncio.create_task(finish_rollout_binding())
    await consumer_started.wait()
    broadcaster = MagicMock(broadcast=AsyncMock())
    im = InstanceManager(db_factory, broadcaster)
    im.processes[instance_id] = process
    im._track_output_consumer(
        instance_id,
        process,
        consumer,
        provider="codex",
    )

    assert im.is_running(instance_id)
    stopping = asyncio.create_task(im.stop(instance_id))
    for _ in range(10):
        if instance_id in im._stopping:
            break
        await asyncio.sleep(0)
    assert not stopping.done()
    assert not consumer.cancelled()
    assert instance_id in im._stopping

    finish_bookkeeping.set()
    assert await stopping is True
    assert consumer.done() and not consumer.cancelled()
    process.send_signal.assert_not_called()
    assert instance_id not in im.processes
    assert instance_id not in im._tasks
    async with db_factory() as db:
        assert (await db.get(Instance, instance_id)).status == "idle"


@pytest.mark.asyncio
async def test_stop_bounds_terminal_codex_consumer_then_cancels_exact_record(
    db_factory,
):
    started_at = datetime(2026, 7, 23, 14, 0, 0)
    async with db_factory() as db:
        instance = Instance(
            name="bounded-terminal-consumer",
            status="running",
            pid=40_201,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="bounded terminal",
            description="d",
            status="executing",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    process = MagicMock(returncode=0, pid=40_201)
    consumer = asyncio.create_task(asyncio.Event().wait())
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.processes[instance_id] = process
    im._track_output_consumer(
        instance_id,
        process,
        consumer,
        provider="codex",
    )

    with patch.object(im, "_generation_reap_confirmed", return_value=True):
        assert await im.stop(
            instance_id,
            expected_task_id=task_id,
            expected_pid=process.pid,
            expected_started_at=started_at,
            task_status="pending",
            terminal_consumer_timeout=0.01,
            consumer_cancel_timeout=0.1,
        )

    assert consumer.cancelled()
    assert instance_id not in im.processes
    assert instance_id not in im._tasks
    assert instance_id not in im._consumer_records
    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        task = await db.get(Task, task_id)
        assert instance.status == "idle"
        assert instance.pid is None
        assert instance.current_task_id is None
        assert task.status == "pending"
        assert task.instance_id is None


@pytest.mark.asyncio
async def test_stop_fail_closes_when_terminal_consumer_ignores_cancel(
    db_factory,
):
    started_at = datetime(2026, 7, 23, 14, 5, 0)
    async with db_factory() as db:
        instance = Instance(
            name="stubborn-terminal-consumer",
            status="running",
            pid=40_202,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="stubborn terminal",
            description="d",
            status="executing",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    release = asyncio.Event()
    cancellation_seen = asyncio.Event()

    async def stubborn_consumer():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellation_seen.set()
            await release.wait()

    process = MagicMock(returncode=0, pid=40_202)
    consumer = asyncio.create_task(stubborn_consumer())
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.processes[instance_id] = process
    im._track_output_consumer(
        instance_id,
        process,
        consumer,
        provider="codex",
    )

    try:
        with (
            patch.object(im, "_generation_reap_confirmed", return_value=True),
            pytest.raises(RuntimeError, match="ignored cancellation"),
        ):
            await im.stop(
                instance_id,
                expected_task_id=task_id,
                expected_pid=process.pid,
                expected_started_at=started_at,
                task_status="pending",
                terminal_consumer_timeout=0.01,
                consumer_cancel_timeout=0.01,
            )

        assert cancellation_seen.is_set()
        assert im.processes[instance_id] is process
        assert im._tasks[instance_id] is consumer
        assert im._consumer_records[instance_id].process is process
        async with db_factory() as db:
            instance = await db.get(Instance, instance_id)
            task = await db.get(Task, task_id)
            assert instance.status == "running"
            assert instance.pid == process.pid
            assert instance.current_task_id == task_id
            assert task.status == "executing"
            assert task.instance_id == instance_id
    finally:
        release.set()
        await asyncio.gather(consumer, return_exceptions=True)


@pytest.mark.asyncio
async def test_overlapping_stop_tokens_survive_stale_stop_and_block_retry(
    db_factory,
):
    started_at = datetime(2026, 7, 23, 14, 10, 0)
    async with db_factory() as db:
        instance = Instance(
            name="overlapping-stop",
            status="running",
            pid=40_203,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="overlapping stop",
            status="executing",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    release = asyncio.Event()
    process = MagicMock(returncode=0, pid=40_203)
    consumer = asyncio.create_task(release.wait())
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.processes[instance_id] = process
    im._track_output_consumer(
        instance_id,
        process,
        consumer,
        provider="codex",
        task_id=task_id,
        task_retry_count=0,
    )

    with patch.object(im, "_generation_reap_confirmed", return_value=True):
        valid_stop = asyncio.create_task(
            im.stop(
                instance_id,
                expected_task_id=task_id,
                expected_pid=process.pid,
                expected_started_at=started_at,
            )
        )
        for _ in range(100):
            if im._stopping.get(instance_id) == 1:
                break
            if valid_stop.done():
                await valid_stop
                pytest.fail("valid stop completed before terminal consumer wait")
            await asyncio.sleep(0.01)
        assert im._stopping[instance_id] == 1

        assert await im.stop(
            instance_id,
            expected_task_id=task_id + 999,
        ) is False
        assert im._stopping[instance_id] == 1
        with pytest.raises(InstanceAlreadyRunningError, match="being stopped"):
            await im.launch(instance_id, "must not replace")

        release.set()
        assert await valid_stop is True
    assert instance_id not in im._stopping


@pytest.mark.asyncio
async def test_cancelled_stop_finishes_reaping_before_propagating(db_factory):
    async with db_factory() as db:
        inst = Instance(name="cancel-safe-stop", status="running")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        instance_id = inst.id

    wait_started = asyncio.Event()
    allow_exit = asyncio.Event()
    process = MagicMock(returncode=None, pid=402)
    process.send_signal = MagicMock()

    async def wait_process():
        wait_started.set()
        await allow_exit.wait()
        process.returncode = 130
        return 130

    process.wait = wait_process
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.processes[instance_id] = process
    stopping = asyncio.create_task(im.stop(instance_id))
    await wait_started.wait()

    stopping.cancel()
    await asyncio.sleep(0)
    assert not stopping.done()
    assert im.processes[instance_id] is process

    allow_exit.set()
    with pytest.raises(asyncio.CancelledError):
        await stopping
    assert instance_id not in im.processes
    async with db_factory() as db:
        refreshed = await db.get(Instance, instance_id)
        assert refreshed.status == "idle"


@pytest.mark.asyncio
async def test_owner_checked_stop_cannot_interrupt_recycled_instance(db_factory):
    async with db_factory() as db:
        old_task = Task(
            title="old",
            description="done",
            status="completed",
            instance_id=1,
        )
        current_task = Task(
            title="current",
            description="running",
            status="executing",
        )
        db.add_all([old_task, current_task])
        await db.flush()
        instance = Instance(
            name="recycled",
            status="running",
            current_task_id=current_task.id,
            pid=991,
        )
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
        instance_id = instance.id
        old_task_id = old_task.id
        current_task_id = current_task.id

    process = MagicMock(returncode=None, pid=991)
    process.send_signal = MagicMock()
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.processes[instance_id] = process

    stopped = await im.stop(
        instance_id,
        expected_task_id=old_task_id,
        task_status="completed",
    )

    assert stopped is False
    process.send_signal.assert_not_called()
    assert im.processes[instance_id] is process
    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        current = await db.get(Task, current_task_id)
        assert instance.current_task_id == current_task_id
        assert instance.status == "running"
        assert current.status == "executing"


@pytest.mark.asyncio
async def test_reconcile_dead_reverse_owner_preserves_current_task_generation(
    db_factory,
):
    stale_started_at = datetime(2026, 8, 2, 7, 36, 23)
    live_started_at = datetime(2026, 8, 2, 7, 39, 22)
    async with db_factory() as db:
        stale = Instance(
            name="dead-retry-owner",
            status="running",
            pid=145_0775,
            started_at=stale_started_at,
        )
        live = Instance(
            name="current-retry-owner",
            status="running",
            pid=145_1525,
            started_at=live_started_at,
        )
        db.add_all([stale, live])
        await db.flush()
        task = Task(
            title="retry owner handoff",
            status="executing",
            instance_id=live.id,
            started_at=live_started_at,
        )
        db.add(task)
        await db.flush()
        stale.current_task_id = task.id
        live.current_task_id = task.id
        await db.commit()
        stale_id, live_id, task_id = stale.id, live.id, task.id

    broadcaster = MagicMock(broadcast=AsyncMock())
    manager = InstanceManager(db_factory, broadcaster)
    with patch(
        "backend.services.instance_manager.os.kill",
        side_effect=ProcessLookupError,
    ):
        reconciled = await manager.reconcile_dead_reverse_task_owner(
            stale_id,
            expected_task_id=task_id,
            expected_pid=145_0775,
            expected_started_at=stale_started_at,
        )

    assert reconciled is True
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        stale = await db.get(Instance, stale_id)
        live = await db.get(Instance, live_id)
        assert task.status == "executing"
        assert task.instance_id == live_id
        assert stale.status == "idle"
        assert stale.pid is None
        assert stale.current_task_id is None
        assert live.status == "running"
        assert live.current_task_id == task_id
    broadcaster.broadcast.assert_awaited_once_with(
        "system",
        {
            "event": "instance_status",
            "instance_id": stale_id,
            "status": "idle",
            "exit_code": None,
        },
    )


@pytest.mark.asyncio
async def test_reconcile_reverse_owner_refuses_a_live_pid(db_factory):
    started_at = datetime(2026, 8, 2, 7, 36, 23)
    async with db_factory() as db:
        stale = Instance(
            name="ambiguous-live-owner",
            status="running",
            pid=145_0775,
            started_at=started_at,
        )
        live = Instance(name="authoritative-owner", status="running")
        db.add_all([stale, live])
        await db.flush()
        task = Task(
            title="do not guess about live pid",
            status="executing",
            instance_id=live.id,
        )
        db.add(task)
        await db.flush()
        stale.current_task_id = task.id
        await db.commit()
        stale_id, task_id = stale.id, task.id

    manager = InstanceManager(
        db_factory,
        MagicMock(broadcast=AsyncMock()),
    )
    with patch("backend.services.instance_manager.os.kill") as probe:
        reconciled = await manager.reconcile_dead_reverse_task_owner(
            stale_id,
            expected_task_id=task_id,
            expected_pid=145_0775,
            expected_started_at=started_at,
        )

    assert reconciled is False
    probe.assert_called_once_with(145_0775, 0)
    async with db_factory() as db:
        stale = await db.get(Instance, stale_id)
        assert stale.status == "running"
        assert stale.pid == 145_0775
        assert stale.current_task_id == task_id


@pytest.mark.asyncio
async def test_instance_stop_releases_active_claim_back_to_pending(db_factory):
    async with db_factory() as db:
        task = Task(title="claimed", description="run", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="owned",
            status="running",
            current_task_id=task.id,
            pid=993,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    process = MagicMock(returncode=None, pid=993)
    process.send_signal = MagicMock()

    async def wait_process():
        process.returncode = 130
        return 130

    process.wait = wait_process
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    im.processes[instance_id] = process

    assert await im.stop(instance_id, expected_task_id=task_id) is True

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "pending"
        assert task.instance_id is None
        assert task.started_at is None
        assert task.completed_at is None
        assert instance.status == "idle"
        assert instance.current_task_id is None
    broadcaster.broadcast.assert_any_await(
        "tasks",
        {
            "event": "status_change",
            "task_id": task_id,
            "new_status": "pending",
            "instance_id": instance_id,
            "background_active": False,
        },
    )


@pytest.mark.asyncio
async def test_instance_stop_terminal_transaction_locks_task_before_instance(
    db_factory,
):
    from sqlalchemy.sql.dml import Update

    async with db_factory() as db:
        task = Task(title="lock-order", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="lock-order",
            status="running",
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    update_tables: list[str] = []

    @asynccontextmanager
    async def recording_factory():
        async with db_factory() as db:
            class SessionProxy:
                def __getattr__(self, name):
                    return getattr(db, name)

                async def execute(self, statement, *args, **kwargs):
                    if isinstance(statement, Update):
                        update_tables.append(statement.table.name)
                    return await db.execute(statement, *args, **kwargs)

            yield SessionProxy()

    im = InstanceManager(
        recording_factory, MagicMock(broadcast=AsyncMock())
    )
    assert await im._stop_locked(
        instance_id,
        expected_task_id=task_id,
        task_status="pending",
        allow_settled_cleanup=True,
    )
    assert "tasks" in update_tables
    assert "instances" in update_tables
    assert update_tables.index("tasks") < update_tables.index("instances")


@pytest.mark.asyncio
async def test_instance_stop_suppresses_old_events_after_replacement_claim(
    db_factory,
):
    from sqlalchemy import update

    async with db_factory() as db:
        task = Task(title="publish-race", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="publish-race",
            status="running",
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    first_commit_seen = False

    @asynccontextmanager
    async def replacement_after_terminal_commit_factory():
        nonlocal first_commit_seen
        async with db_factory() as db:
            class SessionProxy:
                def __getattr__(self, name):
                    return getattr(db, name)

                async def commit(self):
                    nonlocal first_commit_seen
                    await db.commit()
                    if first_commit_seen:
                        return
                    first_commit_seen = True
                    # Simulate a rapid retry/reclaim in the exact commit ->
                    # publication window.
                    async with db_factory() as replacement_db:
                        await replacement_db.execute(
                            update(Task)
                            .where(Task.id == task_id)
                            .values(
                                status="executing",
                                retry_count=Task.retry_count + 1,
                                instance_id=instance_id,
                                started_at=datetime.utcnow(),
                                completed_at=None,
                            )
                        )
                        await replacement_db.commit()

            yield SessionProxy()

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(
        replacement_after_terminal_commit_factory,
        broadcaster,
    )
    assert await im._stop_locked(
        instance_id,
        expected_task_id=task_id,
        task_status="pending",
        allow_settled_cleanup=True,
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "executing"
        assert task.retry_count == 1
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_launch_reaps_process_when_instance_row_was_deleted(db_factory):
    process = _make_mock_process(pid=992, returncode=None)
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))

    with patch(
        "backend.services.instance_manager.asyncio.create_subprocess_exec",
        new_callable=AsyncMock,
        return_value=process,
    ) as spawn:
        with pytest.raises(InstanceNotFoundError):
            await im.launch(999_992, "do not orphan me", cwd="/tmp")

    # Admission fails before any prompt-bearing process can cause side effects.
    spawn.assert_not_awaited()
    process.kill.assert_not_called()
    assert 999_992 not in im.processes


@pytest.mark.asyncio
@pytest.mark.parametrize("supersede_mode", ["cancelled", "reassigned"])
async def test_direct_launch_reaps_exact_process_when_task_claim_is_superseded(
    db_factory, supersede_mode,
):
    async with db_factory() as db:
        instance = Instance(name=f"superseded-{supersede_mode}", status="idle")
        db.add(instance)
        await db.flush()
        task = Task(
            title=f"superseded-{supersede_mode}",
            description="claim changes after spawn",
            status="in_progress",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    process = _make_mock_process(pid=54_321, returncode=None)
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))

    async def spawn_then_supersede(*args, **kwargs):
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            if supersede_mode == "cancelled":
                task.status = "cancelled"
            else:
                task.instance_id = None
            await db.commit()
        return process

    def mark_exact_process_killed(instance_arg, process_arg, sig):
        assert instance_arg == instance_id
        assert process_arg is process
        assert sig == signal.SIGKILL
        process.returncode = -signal.SIGKILL

    with (
        patch(
            "backend.services.instance_manager.asyncio.create_subprocess_exec",
            new=AsyncMock(side_effect=spawn_then_supersede),
        ),
        patch.object(
            im,
            "_signal_process_tree",
            side_effect=mark_exact_process_killed,
        ) as signal_tree,
        patch.object(
            im,
            "_wait_process_tree",
            new_callable=AsyncMock,
        ) as wait_tree,
    ):
        with pytest.raises(LaunchSupersededError):
            await im.launch(
                instance_id=instance_id,
                prompt="must not survive a lost claim",
                task_id=task_id,
                cwd="/tmp",
            )

    signal_tree.assert_called_once_with(instance_id, process, signal.SIGKILL)
    wait_tree.assert_awaited_once_with(instance_id, process, 5.0)
    assert instance_id not in im.processes
    assert instance_id not in im._process_groups
    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        task = await db.get(Task, task_id)
        assert instance.status != "running"
        assert instance.pid is None
        if supersede_mode == "cancelled":
            assert task.status == "cancelled"
        else:
            assert task.instance_id is None


@pytest.mark.asyncio
async def test_failed_direct_launch_reap_timeout_retains_generation_evidence(
    db_factory,
):
    async with db_factory() as db:
        instance = Instance(name="failed-reap", status="idle")
        db.add(instance)
        await db.flush()
        task = Task(
            title="failed-reap",
            description="already cancelled",
            status="cancelled",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    process = _make_mock_process(pid=54_322, returncode=None)
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.processes[instance_id] = process
    im._process_groups[instance_id] = process

    with (
        patch.object(im, "_process_group_alive", return_value=True),
        patch.object(im, "_signal_process_tree") as signal_tree,
        patch.object(
            im,
            "_wait_process_tree",
            new_callable=AsyncMock,
            side_effect=asyncio.TimeoutError,
        ) as wait_tree,
    ):
        with pytest.raises(LaunchSupersededError):
            await im._persist_and_track_launch(
                instance_id=instance_id,
                task_id=task_id,
                process=process,
                actual_cwd="/tmp",
                loop_iteration=None,
                chat_initiated=False,
                provider="claude",
            )

    signal_tree.assert_called_once_with(instance_id, process, signal.SIGKILL)
    wait_tree.assert_awaited_once_with(instance_id, process, 5.0)
    assert im.processes[instance_id] is process
    assert im._process_groups[instance_id] is process
    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        assert instance.status == "error"
        assert instance.pid == process.pid
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_stderr_is_drained_while_stdout_is_consumed(db_factory):
    async with db_factory() as db:
        instance = Instance(name="stderr-drain")
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
        instance_id = instance.id

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import sys; sys.stderr.write('x' * 2000000); "
        "sys.stderr.flush(); print('not-json', flush=True)",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))

    await asyncio.wait_for(
        im._consume_output_impl(instance_id, None, process),
        timeout=10,
    )

    assert process.returncode == 0
    assert len(im.get_last_stderr(instance_id)) == 2_000_000


@pytest.mark.asyncio
async def test_codex_sub_agent_controller_unsubscribes_after_terminal_turn(
    db_factory,
):
    async with db_factory() as db:
        instance = Instance(name="codex-controller-unsubscribe")
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
        instance_id = instance.id

    process = _make_mock_process(pid=54_328, returncode=0)
    process.thread_id = "thread-parent"
    registry = MagicMock()
    registry.unsubscribe_thread = AsyncMock(return_value="unsubscribed")
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._codex_app_server = registry
    process.unsubscribe_on_terminal = True

    await im._consume_output_impl(
        instance_id,
        None,
        process,
        provider="codex",
    )

    registry.unsubscribe_thread.assert_awaited_once_with("thread-parent")
    assert instance_id not in im._launch_params


@pytest.mark.asyncio
async def test_codex_turn_without_thread_mcp_does_not_unsubscribe(
    db_factory,
):
    async with db_factory() as db:
        instance = Instance(name="codex-no-mcp-unsubscribe")
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
        instance_id = instance.id

    process = _make_mock_process(pid=54_329, returncode=0)
    process.thread_id = "thread-without-mcp"
    process.unsubscribe_on_terminal = False
    registry = MagicMock()
    registry.unsubscribe_thread = AsyncMock(return_value="unsubscribed")
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im._codex_app_server = registry

    await im._consume_output_impl(
        instance_id,
        None,
        process,
        provider="codex",
    )

    registry.unsubscribe_thread.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancelled_consumer_still_finishes_terminal_cleanup(db_factory):
    """Stop cancellation must not skip exact process/DB finalization."""

    async with db_factory() as db:
        instance = Instance(
            name="cancelled-consumer-cleanup",
            status="running",
            pid=54_329,
        )
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
        instance_id = instance.id

    read_blocked = asyncio.Event()
    process = _make_mock_process(pid=54_329, returncode=None)

    async def blocked_readline():
        read_blocked.set()
        await asyncio.Event().wait()

    process.stdout.readline = blocked_readline
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.processes[instance_id] = process
    im._process_groups[instance_id] = process
    consumer = asyncio.create_task(
        im._consume_output(instance_id, None, process)
    )
    im._track_output_consumer(instance_id, process, consumer)
    await read_blocked.wait()

    # stop() only cancels the reader after the process generation is terminal.
    process.returncode = 130
    consumer.cancel()
    await asyncio.wait_for(consumer, timeout=1.0)

    assert not consumer.cancelled()
    assert instance_id not in im.processes
    assert instance_id not in im._process_groups
    assert instance_id not in im._tasks
    assert instance_id not in im._consumer_records
    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        assert instance.status == "idle"
        assert instance.pid is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_inherited_stderr_fd_cannot_hold_consumer_forever(
    db_factory, tmp_path
):
    async with db_factory() as db:
        instance = Instance(name="stderr-descendant")
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
        instance_id = instance.id

    pid_file = tmp_path / "descendant.pid"
    script = (
        "import pathlib,subprocess,sys; "
        "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'], "
        "stdout=subprocess.DEVNULL,stderr=sys.stderr); "
        f"pathlib.Path({str(pid_file)!r}).write_text(str(p.pid))"
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.processes[instance_id] = process
    im._process_groups[instance_id] = process

    await asyncio.wait_for(
        im._consume_output_impl(instance_id, None, process),
        timeout=8,
    )

    descendant_pid = int(pid_file.read_text())
    assert process.returncode == 0
    deadline = asyncio.get_running_loop().time() + 2.0
    while asyncio.get_running_loop().time() < deadline:
        try:
            state = Path(f"/proc/{descendant_pid}/stat").read_text(
                encoding="utf-8"
            ).split()[2]
        except FileNotFoundError:
            break
        if state == "Z":
            break
        await asyncio.sleep(0.02)
    else:
        pytest.fail("inherited-fd descendant survived normal parent exit")
    assert instance_id not in im.processes
    assert instance_id not in im._process_groups


@pytest.mark.asyncio
async def test_inherited_stdout_fd_cannot_hold_consumer_forever(
    db_factory, tmp_path
):
    async with db_factory() as db:
        instance = Instance(name="stdout-descendant")
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
        instance_id = instance.id

    pid_file = tmp_path / "stdout-descendant.pid"
    script = (
        "import pathlib,subprocess,sys; "
        "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'], "
        "stdout=sys.stdout,stderr=subprocess.DEVNULL); "
        f"pathlib.Path({str(pid_file)!r}).write_text(str(p.pid))"
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.processes[instance_id] = process
    im._process_groups[instance_id] = process

    await asyncio.wait_for(
        im._consume_output_impl(instance_id, None, process),
        timeout=8,
    )

    descendant_pid = int(pid_file.read_text())
    deadline = asyncio.get_running_loop().time() + 2.0
    while asyncio.get_running_loop().time() < deadline:
        try:
            state = Path(f"/proc/{descendant_pid}/stat").read_text(
                encoding="utf-8"
            ).split()[2]
        except FileNotFoundError:
            break
        if state == "Z":
            break
        await asyncio.sleep(0.02)
    else:
        pytest.fail("inherited-stdout descendant survived normal parent exit")
    assert instance_id not in im.processes
    assert instance_id not in im._process_groups


@pytest.mark.asyncio
async def test_unreaped_direct_group_retains_owner_and_generation_maps(
    db_factory,
):
    async with db_factory() as db:
        instance = Instance(name="unreaped-consumer", status="running", pid=54_330)
        db.add(instance)
        await db.flush()
        task = Task(
            title="unreaped-consumer",
            description="descendant survives",
            status="executing",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    process = _make_mock_process(pid=54_330, returncode=0)
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.processes[instance_id] = process
    im._process_groups[instance_id] = process
    consumer = asyncio.create_task(
        im._consume_output(instance_id, task_id, process)
    )
    im._track_output_consumer(instance_id, process, consumer)

    with (
        patch.object(im, "_process_group_alive", return_value=True),
        patch.object(
            im, "_signal_managed_process_tree", new_callable=AsyncMock
        ) as signal_tree,
        patch.object(
            im,
            "_wait_process_tree",
            new_callable=AsyncMock,
            side_effect=asyncio.TimeoutError,
        ) as wait_tree,
        pytest.raises(RuntimeError, match="Could not reap process generation"),
    ):
        await consumer
    await asyncio.sleep(0)

    assert signal_tree.await_count == 2
    assert wait_tree.await_count == 2
    assert im.processes[instance_id] is process
    assert im._process_groups[instance_id] is process
    assert im._consumer_records[instance_id].process is process
    assert im._tasks[instance_id] is consumer
    with patch.object(im, "_process_group_alive", return_value=True):
        assert im.is_running(instance_id)
    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        assert instance.status == "error"
        assert instance.pid == process.pid
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_stop_retries_terminal_parent_with_live_direct_group(db_factory):
    generation_started_at = datetime(2026, 7, 23, 12, 0, 0)
    async with db_factory() as db:
        instance = Instance(
            name="retry-descendant-stop",
            status="error",
            pid=54_331,
            started_at=generation_started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="retry-descendant-stop",
            description="retained descendant",
            status="executing",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    process = _make_mock_process(pid=54_331, returncode=0)
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.processes[instance_id] = process
    im._process_groups[instance_id] = process
    group_live = True

    def group_alive(instance_arg, process_arg):
        assert instance_arg == instance_id
        assert process_arg is process
        return group_live

    async def signal_tree(instance_arg, process_arg, sig):
        nonlocal group_live
        assert instance_arg == instance_id
        assert process_arg is process
        assert sig == signal.SIGINT
        group_live = False

    with (
        patch.object(im, "_process_group_alive", side_effect=group_alive),
        patch.object(
            im, "_signal_managed_process_tree", side_effect=signal_tree
        ) as signal_group,
        patch.object(
            im, "_wait_process_tree", new_callable=AsyncMock
        ) as wait_tree,
    ):
        assert await im.stop(
            instance_id,
            expected_task_id=task_id,
            expected_pid=process.pid,
            expected_started_at=generation_started_at,
            task_status="cancelled",
        )

    signal_group.assert_awaited_once()
    wait_tree.assert_awaited_once_with(instance_id, process, 10.0)
    assert instance_id not in im.processes
    assert instance_id not in im._process_groups
    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        task = await db.get(Task, task_id)
        assert instance.status == "idle"
        assert instance.pid is None
        assert instance.current_task_id is None
        assert task.status == "cancelled"


@pytest.mark.asyncio
async def test_stop_generation_fence_rejects_same_task_slot_aba(db_factory):
    current_started_at = datetime(2026, 7, 23, 13, 0, 0)
    async with db_factory() as db:
        instance = Instance(
            name="stop-aba",
            status="running",
            pid=54_350,
            started_at=current_started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="stop-aba",
            description="same owner, newer process generation",
            status="executing",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    process = _make_mock_process(pid=54_350, returncode=None)
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.processes[instance_id] = process

    stale_expectations = [
        {"expected_pid": 54_349},
        {"expected_pid": None},
        {
            "expected_started_at": (
                current_started_at - timedelta(seconds=1)
            )
        },
        {"expected_started_at": None},
    ]
    for expected in stale_expectations:
        assert not await im.stop(
            instance_id,
            expected_task_id=task_id,
            task_status="cancelled",
            **expected,
        )

    process.send_signal.assert_not_called()
    process.terminate.assert_not_called()
    process.kill.assert_not_called()
    assert im.processes[instance_id] is process
    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        task = await db.get(Task, task_id)
        assert instance.status == "running"
        assert instance.pid == process.pid
        assert instance.current_task_id == task_id
        assert task.status == "executing"


@pytest.mark.asyncio
async def test_cancelled_codex_spawn_collects_and_releases_home_admission(
    db_factory,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    async with db_factory() as db:
        instance = Instance(name="cancelled-codex-spawn")
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
        instance_id = instance.id

    process = _make_mock_process(pid=54_332, returncode=None)
    spawn_started = asyncio.Event()
    release_spawn = asyncio.Event()
    codex_home = str((tmp_path / "cancelled-codex-home").resolve())

    async def delayed_spawn(*args, **kwargs):
        spawn_started.set()
        await release_spawn.wait()
        return process

    def kill_exact(instance_arg, process_arg, sig):
        assert instance_arg == instance_id
        assert process_arg is process
        assert sig == signal.SIGKILL
        process.returncode = -signal.SIGKILL

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    with (
        patch(
            "backend.services.instance_manager.asyncio.create_subprocess_exec",
            side_effect=delayed_spawn,
        ),
        patch.object(
            im, "_signal_process_tree", side_effect=kill_exact
        ) as signal_group,
        patch.object(im, "_process_group_alive", return_value=False),
        patch.object(
            im, "_wait_process_tree", new_callable=AsyncMock
        ) as wait_tree,
    ):
        launch = asyncio.create_task(
            im.launch(
                instance_id,
                "cancel during OS spawn",
                cwd="/tmp",
                provider="codex",
                config_dir=codex_home,
            )
        )
        await asyncio.wait_for(spawn_started.wait(), timeout=2.0)
        launch.cancel()
        release_spawn.set()
        with pytest.raises(asyncio.CancelledError):
            await launch

    signal_group.assert_called_once_with(
        instance_id, process, signal.SIGKILL
    )
    wait_tree.assert_awaited_once_with(instance_id, process, 5.0)
    assert instance_id not in im.processes
    assert instance_id not in im._process_groups
    assert instance_id not in im._codex_exec_homes
    assert codex_home not in im.busy_codex_homes()
    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        assert instance.status == "idle"
        assert instance.pid is None


@pytest.mark.asyncio
async def test_cancelled_codex_spawn_failed_reap_retains_home_admission(
    db_factory,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(settings, "codex_app_server_enabled", False)
    async with db_factory() as db:
        instance = Instance(name="cancelled-codex-spawn-unreaped")
        contender = Instance(name="cancelled-codex-spawn-contender")
        db.add_all([instance, contender])
        await db.commit()
        await db.refresh(instance)
        await db.refresh(contender)
        instance_id = instance.id
        contender_id = contender.id

    process = _make_mock_process(pid=54_333, returncode=None)
    spawn_started = asyncio.Event()
    release_spawn = asyncio.Event()
    codex_home = str((tmp_path / "cancelled-codex-home").resolve())

    async def delayed_spawn(*args, **kwargs):
        spawn_started.set()
        await release_spawn.wait()
        return process

    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    with (
        patch(
            "backend.services.instance_manager.asyncio.create_subprocess_exec",
            side_effect=delayed_spawn,
        ) as spawn,
        patch.object(im, "_process_group_alive", return_value=True),
        patch.object(im, "_signal_process_tree") as signal_group,
        patch.object(
            im,
            "_wait_process_tree",
            new_callable=AsyncMock,
            side_effect=asyncio.TimeoutError,
        ) as wait_tree,
    ):
        launch = asyncio.create_task(
            im.launch(
                instance_id,
                "cancel and fail reaping",
                cwd="/tmp",
                provider="codex",
                config_dir=codex_home,
            )
        )
        await asyncio.wait_for(spawn_started.wait(), timeout=2.0)
        launch.cancel()
        release_spawn.set()
        with pytest.raises(asyncio.CancelledError):
            await launch

        with pytest.raises(
            CodexAppServerBusyError,
            match="still has an exec generation",
        ):
            await im.launch(
                contender_id,
                "must not overlap retained generation",
                cwd="/tmp",
                provider="codex",
                config_dir=codex_home,
            )

    assert spawn.call_count == 1
    signal_group.assert_called_once_with(
        instance_id, process, signal.SIGKILL
    )
    wait_tree.assert_awaited_once_with(instance_id, process, 5.0)
    assert im.processes[instance_id] is process
    assert im._process_groups[instance_id] is process
    assert im._codex_exec_homes[instance_id] == codex_home
    assert codex_home in im.busy_codex_homes()
    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        assert instance.status == "error"
        assert instance.pid == process.pid


def test_managed_direct_process_signals_the_whole_group():
    im = InstanceManager(MagicMock(), MagicMock())
    process = MagicMock(pid=43210, returncode=None)
    im.processes[7] = process
    im._process_groups[7] = process

    with patch("backend.services.instance_manager.os.killpg") as killpg:
        im._signal_process_tree(7, process, signal.SIGINT)

    killpg.assert_called_once_with(43210, signal.SIGINT)
    process.send_signal.assert_not_called()


@pytest.mark.asyncio
async def test_crashed_output_consumer_cannot_latch_instance_maps(caplog):
    im = InstanceManager(MagicMock(), MagicMock())
    process = MagicMock(returncode=1)

    async def crash():
        raise RuntimeError("post-process bookkeeping failed")

    consumer = asyncio.create_task(crash())
    im.processes[7] = process
    im._codex_exec_homes[7] = "/tmp/codex-7"
    im._launch_params[7] = {"provider": "codex"}
    im._track_output_consumer(7, process, consumer)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert 7 not in im._tasks
    assert 7 not in im.processes
    assert 7 not in im._codex_exec_homes
    assert 7 not in im._launch_params
    assert "Output consumer crashed for instance 7" in caplog.text


@pytest.mark.asyncio
async def test_consumer_failure_is_scoped_to_exact_process_generation():
    im = InstanceManager(MagicMock(), MagicMock())
    old_process = MagicMock(returncode=1)

    async def crash():
        raise RuntimeError("old generation failed")

    old_consumer = asyncio.create_task(crash())
    im.processes[7] = old_process
    im._track_output_consumer(7, old_process, old_consumer)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    new_process = MagicMock(returncode=None)
    new_consumer = asyncio.create_task(asyncio.Event().wait())
    im.processes[7] = new_process
    im._track_output_consumer(
        7, new_process, new_consumer, chat_initiated=True
    )

    with pytest.raises(RuntimeError, match="Output consumer failed") as caught:
        await im.wait_for_output_consumer(
            7, provider="codex", expected_process=old_process
        )
    assert isinstance(caught.value.__cause__, RuntimeError)
    assert str(caught.value.__cause__) == "old generation failed"
    assert im.processes[7] is new_process
    assert im._tasks[7] is new_consumer

    new_consumer.cancel()
    await asyncio.gather(new_consumer, return_exceptions=True)


@pytest.mark.asyncio
async def test_new_launch_does_not_consume_previous_generation_failure():
    im = InstanceManager(MagicMock(), MagicMock())
    old_process = MagicMock(returncode=1)
    old_error = RuntimeError("old bookkeeping")
    im._consumer_errors[(7, old_process)] = old_error
    new_process = MagicMock(returncode=None, pid=702)

    async def fake_launch_locked(**kwargs):
        im.processes[kwargs["instance_id"]] = new_process
        return new_process.pid

    im._launch_locked = fake_launch_locked

    assert await im.launch(7, "new turn") == 702
    assert im._consumer_errors[(7, old_process)] is old_error


@pytest.mark.asyncio
async def test_waiting_admission_preserves_managed_failure_for_owning_lifecycle():
    im = InstanceManager(MagicMock(), MagicMock())
    old_process = MagicMock(returncode=1)
    release_crash = asyncio.Event()

    async def crash():
        await release_crash.wait()
        raise RuntimeError("managed bookkeeping")

    old_consumer = asyncio.create_task(crash())
    im.processes[7] = old_process
    im._track_output_consumer(7, old_process, old_consumer)
    admission = asyncio.create_task(im.launch(7, "new turn"))
    await asyncio.sleep(0)
    release_crash.set()

    with pytest.raises(RuntimeError, match="managed bookkeeping"):
        await admission
    with pytest.raises(RuntimeError, match="Output consumer failed") as caught:
        await im.wait_for_output_consumer(
            7, expected_process=old_process
        )
    assert str(caught.value.__cause__) == "managed bookkeeping"


@pytest.mark.asyncio
async def test_two_waiting_launches_admit_only_one_next_generation():
    im = InstanceManager(MagicMock(), MagicMock())
    old_process = MagicMock(returncode=0)
    release_old = asyncio.Event()

    async def settle_old():
        await release_old.wait()

    old_consumer = asyncio.create_task(settle_old())
    im.processes[7] = old_process
    im._track_output_consumer(7, old_process, old_consumer)
    launched = []

    async def fake_launch_locked(**kwargs):
        launched.append(kwargs["prompt"])
        im.processes[7] = MagicMock(returncode=None, pid=700 + len(launched))
        return im.processes[7].pid

    im._launch_locked = fake_launch_locked
    first = asyncio.create_task(im.launch(7, "first contender", provider="codex"))
    second = asyncio.create_task(im.launch(7, "second contender", provider="codex"))
    await asyncio.sleep(0)
    release_old.set()
    results = await asyncio.gather(first, second, return_exceptions=True)

    assert len(launched) == 1
    assert sum(isinstance(result, int) for result in results) == 1
    assert sum(
        isinstance(result, InstanceAlreadyRunningError) for result in results
    ) == 1


@pytest.mark.asyncio
async def test_chat_consumer_failure_does_not_leave_unowned_error_latch():
    im = InstanceManager(MagicMock(), MagicMock())
    process = MagicMock(returncode=1)

    async def crash():
        raise RuntimeError("chat bookkeeping")

    consumer = asyncio.create_task(crash())
    im.processes[7] = process
    im._track_output_consumer(7, process, consumer, chat_initiated=True)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert not im._consumer_errors


@pytest.mark.asyncio
async def test_is_running():
    """is_running checks process returncode."""
    broadcaster = MagicMock()
    im = InstanceManager(MagicMock(), broadcaster)

    # No process
    assert im.is_running(1) is False

    # Process with returncode=None (still running)
    mock_proc = MagicMock()
    mock_proc.returncode = None
    im.processes[1] = mock_proc
    assert im.is_running(1) is True

    # Process with returncode=0 (finished)
    mock_proc.returncode = 0
    assert im.is_running(1) is False


@pytest.mark.asyncio
async def test_process_event_updates_task_before_instance():
    """Mixed event bookkeeping follows the global Task -> Instance order."""
    statements = []

    class RecordingSession:
        def add(self, _entry):
            return None

        async def execute(self, statement):
            statements.append(statement.table.name)

        async def commit(self):
            return None

    @asynccontextmanager
    async def recording_db_factory():
        yield RecordingSession()

    broadcaster = MagicMock(broadcast=AsyncMock())
    im = InstanceManager(recording_db_factory, broadcaster)

    await im._process_event(11, 22, {
        "event_type": "result",
        "role": "assistant",
        "content": "done",
        "raw_json": "{}",
        "is_error": False,
        "session_id": "session-22",
        "cost_usd": 1.25,
    })

    assert statements == ["tasks", "instances"]


@pytest.mark.asyncio
async def test_process_event_reordered_writes_preserve_event_bookkeeping(
    db_factory,
):
    """Lock reordering keeps log, task, instance, and broadcast semantics."""
    async with db_factory() as db:
        instance = Instance(name="event-bookkeeping")
        task = Task(title="event bookkeeping", has_unread=False)
        db.add_all([instance, task])
        await db.commit()
        await db.refresh(instance)
        await db.refresh(task)
        instance_id = instance.id
        task_id = task.id

    broadcaster = MagicMock(broadcast=AsyncMock())
    im = InstanceManager(db_factory, broadcaster)
    await im._process_event(instance_id, task_id, {
        "event_type": "result",
        "role": "assistant",
        "content": "bookkept",
        "raw_json": '{"source":"test"}',
        "is_error": False,
        "session_id": "session-bookkept",
        "cost_usd": 2.75,
    }, loop_iteration=3)

    async with db_factory() as db:
        stored_task = await db.get(Task, task_id)
        stored_instance = await db.get(Instance, instance_id)
        stored_log = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.instance_id == instance_id,
                    LogEntry.task_id == task_id,
                )
            )
        ).scalar_one()

    assert stored_task.session_id == "session-bookkept"
    assert stored_task.has_unread is True
    assert stored_instance.last_heartbeat is not None
    assert stored_instance.total_cost_usd == pytest.approx(2.75)
    assert stored_log.event_type == "result"
    assert stored_log.role == "assistant"
    assert stored_log.content == "bookkept"
    assert stored_log.raw_json == '{"source":"test"}'
    assert stored_log.loop_iteration == 3

    assert [call.args[0] for call in broadcaster.broadcast.await_args_list] == [
        f"instance:{instance_id}",
        f"task:{task_id}",
    ]
    for broadcast_call in broadcaster.broadcast.await_args_list:
        payload = broadcast_call.args[1]
        assert payload["id"] == stored_log.id
        assert payload["instance_id"] == instance_id
        assert payload["task_id"] == task_id
        assert payload["loop_iteration"] == 3
        assert "raw_json" not in payload
        assert "session_id" not in payload
        assert "cost_usd" not in payload


@pytest.mark.asyncio
async def test_process_event_lock_order_does_not_deadlock_lifecycle_update():
    """An event transaction cannot invert a lifecycle Task -> Instance lock."""
    task_lock = asyncio.Lock()
    instance_lock = asyncio.Lock()
    event_has_first_lock = asyncio.Event()
    release_event_after_first_lock = asyncio.Event()
    lifecycle_attempting_task = asyncio.Event()

    class LockingSession:
        def __init__(self):
            self.held_locks = []

        def add(self, _entry):
            return None

        async def execute(self, statement):
            row_lock = (
                task_lock
                if statement.table.name == "tasks"
                else instance_lock
            )
            await row_lock.acquire()
            self.held_locks.append(row_lock)
            if len(self.held_locks) == 1:
                event_has_first_lock.set()
                await release_event_after_first_lock.wait()

        async def commit(self):
            self.release()

        def release(self):
            while self.held_locks:
                self.held_locks.pop().release()

    @asynccontextmanager
    async def locking_db_factory():
        session = LockingSession()
        try:
            yield session
        finally:
            session.release()

    async def lifecycle_update():
        lifecycle_attempting_task.set()
        await task_lock.acquire()
        try:
            await instance_lock.acquire()
            instance_lock.release()
        finally:
            task_lock.release()

    im = InstanceManager(
        locking_db_factory,
        MagicMock(broadcast=AsyncMock()),
    )
    processing = asyncio.create_task(im._process_event(31, 32, {
        "event_type": "result",
        "role": "assistant",
        "content": "concurrent",
        "raw_json": "{}",
        "is_error": False,
        "session_id": "session-32",
    }))
    await asyncio.wait_for(event_has_first_lock.wait(), timeout=1)
    lifecycle = asyncio.create_task(lifecycle_update())
    await asyncio.wait_for(lifecycle_attempting_task.wait(), timeout=1)
    # Let the lifecycle transaction either acquire Task (old inverse order) or
    # block behind the event transaction (the required shared order).
    await asyncio.sleep(0)
    release_event_after_first_lock.set()

    await asyncio.wait_for(
        asyncio.gather(processing, lifecycle),
        timeout=1,
    )


@pytest.mark.asyncio
async def test_process_event_broadcasts_context_usage(db_factory):
    """_process_event broadcasts a separate context_usage event when present."""
    async with db_factory() as db:
        from backend.models.instance import Instance
        inst = Instance(name="ctx-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    usage = {
        "input_tokens": 10,
        "cache_read_input_tokens": 500,
        "cache_creation_input_tokens": 200,
        "output_tokens": 20,
        "total_input_tokens": 710,
        "context_window": 200000,
    }
    event = {
        "event_type": "message",
        "role": "assistant",
        "content": "Hello",
        "tool_name": None,
        "tool_input": None,
        "tool_output": None,
        "raw_json": "{}",
        "is_error": False,
        "timestamp": "2024-01-01T00:00:00",
        "context_usage": usage,
    }

    await im._process_event(inst_id, None, event)

    # Should have broadcast the main event + context_usage event
    calls = broadcaster.broadcast.call_args_list
    # context_usage event not broadcast when task_id is None (no task channel)
    # Verify main event was broadcast to instance channel
    instance_broadcasts = [c for c in calls if c[0][0] == f"instance:{inst_id}"]
    assert len(instance_broadcasts) >= 1
    # context_usage key should be stripped from main broadcast
    main_data = instance_broadcasts[0][0][1]
    assert "context_usage" not in main_data
    assert isinstance(main_data["id"], int)
    assert main_data["instance_id"] == inst_id
    assert main_data["task_id"] is None
    assert main_data["timestamp"]


@pytest.mark.asyncio
async def test_process_event_broadcasts_context_usage_to_task(db_factory):
    """_process_event broadcasts context_usage event to task channel when task_id set."""
    async with db_factory() as db:
        from backend.models.instance import Instance
        from backend.models.task import Task
        inst = Instance(name="ctx-task-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

        task = Task(title="ctx task")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    usage = {
        "input_tokens": 5,
        "cache_read_input_tokens": 100,
        "cache_creation_input_tokens": 50,
        "output_tokens": 10,
        "total_input_tokens": 155,
        "context_window": 1000000,
    }
    event = {
        "event_type": "message",
        "role": "assistant",
        "content": "Hello",
        "tool_name": None,
        "tool_input": None,
        "tool_output": None,
        "raw_json": "{}",
        "is_error": False,
        "timestamp": "2024-01-01T00:00:00",
        "context_usage": usage,
    }

    await im._process_event(inst_id, task_id, event)

    calls = broadcaster.broadcast.call_args_list
    # Find context_usage broadcast to task channel
    ctx_calls = [
        c for c in calls
        if c[0][0] == f"task:{task_id}" and c[0][1].get("event_type") == "context_usage"
    ]
    assert len(ctx_calls) == 1
    ctx_data = ctx_calls[0][0][1]
    assert ctx_data["total_input_tokens"] == 155
    assert ctx_data["context_window"] == 1000000
    assert ctx_data["input_tokens"] == 5


@pytest.mark.asyncio
async def test_process_event_sets_has_unread_on_assistant_message(db_factory):
    """_process_event sets has_unread=True on task when assistant message event arrives."""
    async with db_factory() as db:
        inst = Instance(name="unread-inst")
        db.add(inst)
        task = Task(title="unread task", description="d")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    event = {
        "event_type": "message",
        "role": "assistant",
        "content": "Here is my response",
        "tool_name": None,
        "tool_input": None,
        "tool_output": None,
        "raw_json": "{}",
        "is_error": False,
    }

    await im._process_event(inst_id, task_id, event)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.has_unread is True


@pytest.mark.asyncio
async def test_process_event_keeps_short_complete_codex_reply(db_factory):
    """Codex answers like 'OK' are final items, not Claude stream fragments."""
    from sqlalchemy import func, select
    from backend.models.log_entry import LogEntry

    async with db_factory() as db:
        inst = Instance(name="short-codex-reply-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    raw = json.dumps({
        "type": "item.completed",
        "item": {"id": "msg-1", "type": "agent_message", "text": "OK"},
    })
    event = im._parse_codex_line(raw)

    await im._process_event(inst_id, None, event)

    async with db_factory() as db:
        count = await db.scalar(
            select(func.count()).select_from(LogEntry).where(
                LogEntry.instance_id == inst_id,
                LogEntry.content == "OK",
            )
        )
    assert count == 1


@pytest.mark.asyncio
async def test_process_event_sets_has_unread_on_result(db_factory):
    """_process_event sets has_unread=True on task when result event arrives."""
    async with db_factory() as db:
        inst = Instance(name="unread-result-inst")
        db.add(inst)
        task = Task(title="result task", description="d")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    event = {
        "event_type": "result",
        "role": "assistant",
        "content": "Task completed",
        "tool_name": None,
        "tool_input": None,
        "tool_output": None,
        "raw_json": "{}",
        "is_error": False,
    }

    await im._process_event(inst_id, task_id, event)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.has_unread is True


@pytest.mark.asyncio
async def test_process_event_does_not_set_has_unread_for_user_message(db_factory):
    """_process_event does NOT set has_unread for user role messages."""
    async with db_factory() as db:
        inst = Instance(name="user-msg-inst")
        db.add(inst)
        task = Task(title="user msg task", description="d")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    event = {
        "event_type": "message",
        "role": "user",
        "content": "User says hello",
        "tool_name": None,
        "tool_input": None,
        "tool_output": None,
        "raw_json": "{}",
        "is_error": False,
    }

    await im._process_event(inst_id, task_id, event)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.has_unread is False


@pytest.mark.asyncio
async def test_process_event_does_not_set_has_unread_for_tool_use(db_factory):
    """_process_event does NOT set has_unread for tool_use events."""
    async with db_factory() as db:
        inst = Instance(name="tool-inst")
        db.add(inst)
        task = Task(title="tool task", description="d")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    event = {
        "event_type": "tool_use",
        "role": "assistant",
        "content": None,
        "tool_name": "Bash",
        "tool_input": '{"command": "ls"}',
        "tool_output": None,
        "raw_json": "{}",
        "is_error": False,
    }

    await im._process_event(inst_id, task_id, event)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.has_unread is False


# === loop_iteration broadcast tests ===


@pytest.mark.asyncio
async def test_process_event_broadcasts_loop_iteration(db_factory):
    """_process_event includes loop_iteration in broadcast data when provided."""
    async with db_factory() as db:
        inst = Instance(name="loop-iter-inst")
        db.add(inst)
        task = Task(title="loop task", description="d")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    event = {
        "event_type": "message",
        "role": "assistant",
        "content": "Working on item 3",
        "tool_name": None,
        "tool_input": None,
        "tool_output": None,
        "raw_json": "{}",
        "is_error": False,
    }

    await im._process_event(inst_id, task_id, event, loop_iteration=2)

    calls = broadcaster.broadcast.call_args_list
    task_broadcasts = [c for c in calls if c[0][0] == f"task:{task_id}"]
    assert len(task_broadcasts) >= 1
    broadcast_data = task_broadcasts[0][0][1]
    assert broadcast_data["loop_iteration"] == 2


@pytest.mark.asyncio
async def test_process_event_omits_loop_iteration_when_none(db_factory):
    """_process_event does not add loop_iteration to broadcast when it is None."""
    async with db_factory() as db:
        inst = Instance(name="no-loop-inst")
        db.add(inst)
        task = Task(title="auto task", description="d")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    event = {
        "event_type": "message",
        "role": "assistant",
        "content": "Hello",
        "tool_name": None,
        "tool_input": None,
        "tool_output": None,
        "raw_json": "{}",
        "is_error": False,
    }

    await im._process_event(inst_id, task_id, event)

    calls = broadcaster.broadcast.call_args_list
    task_broadcasts = [c for c in calls if c[0][0] == f"task:{task_id}"]
    assert len(task_broadcasts) >= 1
    broadcast_data = task_broadcasts[0][0][1]
    assert "loop_iteration" not in broadcast_data


@pytest.mark.asyncio
async def test_process_event_broadcasts_loop_iteration_zero(db_factory):
    """_process_event includes loop_iteration=0 in broadcast (first iteration)."""
    async with db_factory() as db:
        inst = Instance(name="loop-zero-inst")
        db.add(inst)
        task = Task(title="loop task zero", description="d")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    event = {
        "event_type": "tool_use",
        "role": "assistant",
        "content": None,
        "tool_name": "Read",
        "tool_input": '{"file_path": "TODO.md"}',
        "tool_output": None,
        "raw_json": "{}",
        "is_error": False,
    }

    await im._process_event(inst_id, task_id, event, loop_iteration=0)

    calls = broadcaster.broadcast.call_args_list
    task_broadcasts = [c for c in calls if c[0][0] == f"task:{task_id}"]
    assert len(task_broadcasts) >= 1
    broadcast_data = task_broadcasts[0][0][1]
    assert broadcast_data["loop_iteration"] == 0


# === chat_initiated flag tests ===


@pytest.mark.asyncio
async def test_successful_codex_consumer_checks_quota_for_every_turn(db_factory):
    async with db_factory() as db:
        inst = Instance(name="codex-quota-consumer")
        task = Task(
            title="codex quota consumer",
            status="executing",
            provider="codex",
            session_id="thread-consumer",
        )
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    process = _make_mock_process(returncode=0)
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.processes[inst.id] = process
    im._try_proactive_pool_switch = AsyncMock(return_value=False)

    await im._consume_output(
        inst.id,
        task.id,
        process,
        chat_initiated=False,
        provider="codex",
    )

    im._try_proactive_pool_switch.assert_awaited_once_with(
        inst.id,
        task.id,
        rate_limit_info=None,
        consumer_record=None,
    )


@pytest.mark.asyncio
async def test_consume_output_chat_initiated_restores_task_status(db_factory):
    """When chat_initiated=True, consumer marks task as completed on process exit."""
    async with db_factory() as db:
        inst = Instance(name="chat-init-inst")
        db.add(inst)
        task = Task(title="chat task", description="d", status="executing")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    mock_proc = _make_mock_process(returncode=0)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    im.processes[inst_id] = mock_proc

    await im._consume_output(inst_id, task_id, mock_proc, chat_initiated=True)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "completed"


@pytest.mark.asyncio
async def test_consume_output_dispatcher_does_not_restore_task_status(db_factory):
    """When chat_initiated=False (dispatcher), consumer does NOT mark task completed."""
    async with db_factory() as db:
        inst = Instance(name="dispatch-inst")
        db.add(inst)
        task = Task(title="dispatch task", description="d", status="executing")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    mock_proc = _make_mock_process(returncode=0)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    im.processes[inst_id] = mock_proc

    await im._consume_output(inst_id, task_id, mock_proc, chat_initiated=False)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "executing"


@pytest.mark.asyncio
async def test_consume_output_default_does_not_restore_task_status(db_factory):
    """Default launch (no chat_initiated) does NOT mark task completed — same as dispatcher."""
    async with db_factory() as db:
        inst = Instance(name="default-inst")
        db.add(inst)
        task = Task(title="default task", description="d", status="executing")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    mock_proc = _make_mock_process(returncode=0)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    im.processes[inst_id] = mock_proc

    await im._consume_output(inst_id, task_id, mock_proc)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "executing"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "api_account", "expected_label"),
    [
        ("claude", False, "Claude"),
        ("claude", True, "Claude API"),
        ("codex", False, "Codex"),
        ("codex", True, "Codex API"),
    ],
)
async def test_consume_output_chat_initiated_error_marks_failed(
    db_factory,
    provider,
    api_account,
    expected_label,
):
    """A silent process exit reports its real provider and account kind."""
    async with db_factory() as db:
        inst = Instance(name="chat-err-inst")
        db.add(inst)
        task = Task(
            title="chat error task",
            description="d",
            status="executing",
            provider=provider,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    mock_proc = _make_mock_process(returncode=1)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    im.processes[inst_id] = mock_proc
    im._config_dirs[inst_id] = "/runtime/provider-home"
    store = MagicMock()
    store.account_for_claude_config_dir.return_value = (
        MagicMock() if api_account and provider == "claude" else None
    )
    store.account_for_codex_home.return_value = (
        MagicMock() if api_account and provider == "codex" else None
    )
    im.cloudrouter_store = store

    await im._consume_output(
        inst_id,
        task_id,
        mock_proc,
        chat_initiated=True,
        provider=provider,
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        notices = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task_id,
                    LogEntry.event_type == "system_event",
                    LogEntry.is_error.is_(True),
                )
            )
        ).scalars().all()
    assert task.status == "failed"
    assert task.error_message is not None
    assert [notice.content for notice in notices] == [
        f"{expected_label} 进程在返回回复前异常退出（exit code 1）。"
    ]


@pytest.mark.asyncio
async def test_consume_output_fatal_result_overrides_zero_exit(db_factory):
    error_text = "API Error: upstream_error: provider unavailable"
    async with db_factory() as db:
        inst = Instance(name="chat-fatal-result-inst")
        db.add(inst)
        task = Task(
            title="chat fatal result task",
            description="d",
            status="executing",
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    mock_proc = _make_mock_process(returncode=0)
    output = iter([
        json.dumps({
            "type": "result",
            "is_error": True,
            "result": error_text,
        }).encode() + b"\n",
        b"",
    ])

    async def readline():
        return next(output)

    mock_proc.stdout.readline = readline
    broadcaster = MagicMock(broadcast=AsyncMock())
    im = InstanceManager(db_factory, broadcaster)
    im.processes[inst_id] = mock_proc

    await im._consume_output(
        inst_id,
        task_id,
        mock_proc,
        chat_initiated=True,
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        persisted_error = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task_id,
                    LogEntry.event_type == "result",
                    LogEntry.is_error.is_(True),
                )
            )
        ).scalar_one()

    assert task.status == "failed"
    assert task.error_message == error_text
    assert persisted_error.content == error_text


@pytest.mark.asyncio
async def test_codex_turn_failed_does_not_append_generic_process_exit(
    db_factory,
):
    error_text = "Your Codex access token could not be refreshed."
    async with db_factory() as db:
        inst = Instance(name="codex-turn-failed-inst")
        task = Task(
            title="codex turn failed task",
            description="d",
            status="executing",
            provider="codex",
        )
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    process = _make_mock_process(returncode=1)
    output = iter([
        json.dumps({
            "type": "turn.failed",
            "error": {"message": error_text},
        }).encode() + b"\n",
        b"",
    ])

    async def readline():
        return next(output)

    process.stdout.readline = readline
    manager = InstanceManager(
        db_factory,
        MagicMock(broadcast=AsyncMock()),
    )
    manager.processes[inst_id] = process

    await manager._consume_output(
        inst_id,
        task_id,
        process,
        chat_initiated=True,
        provider="codex",
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        error_entries = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task_id,
                    LogEntry.is_error.is_(True),
                )
            )
        ).scalars().all()

    assert task.status == "failed"
    assert task.error_message == error_text
    assert [entry.content for entry in error_entries] == [error_text]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "notification_type",
        "will_retry",
        "message",
        "expected_status",
        "expected_error",
        "expected_exit_code",
    ),
    [
        pytest.param(
            "turn.retrying", True, "Reconnecting... 1/5",
            "completed", None, 0, id="retrying",
        ),
        pytest.param(
            "turn.failed", False, "backend failed",
            "failed", "backend failed", 1, id="non-retry-fatal",
        ),
    ],
)
async def test_codex_error_notification_respects_will_retry(
    db_factory,
    notification_type,
    will_retry,
    message,
    expected_status,
    expected_error,
    expected_exit_code,
):
    async with db_factory() as db:
        inst = Instance(name="codex-retrying-inst")
        task = Task(
            title="codex retrying task",
            description="d",
            status="executing",
            provider="codex",
        )
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    process = _make_mock_process(returncode=0)
    output = iter([
        json.dumps({
            "type": notification_type,
            "error": {
                "message": message,
                "codexErrorInfo": {
                    "responseStreamDisconnected": {"httpStatusCode": 409},
                },
                "additionalDetails": (
                    "unexpected status 409 Conflict: "
                    '{"detail":"all logged-in accounts are busy"}'
                ),
            },
            "turn_id": "turn-1",
            "will_retry": will_retry,
            "terminal": not will_retry,
        }).encode() + b"\n",
        json.dumps({
            "type": "turn.completed",
            "turn_id": "turn-1",
            "usage": {},
        }).encode() + b"\n",
        b"",
    ])

    async def readline():
        return next(output)

    process.stdout.readline = readline
    manager = InstanceManager(
        db_factory,
        MagicMock(broadcast=AsyncMock()),
    )
    manager._try_proactive_pool_switch = AsyncMock(return_value=False)
    manager.processes[inst_id] = process

    await manager._consume_output(
        inst_id,
        task_id,
        process,
        chat_initiated=True,
        provider="codex",
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        error_entries = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task_id,
                    LogEntry.is_error.is_(True),
                )
            )
        ).scalars().all()

    assert task.status == expected_status
    assert task.error_message == expected_error
    assert (task.completed_at is not None) == (expected_status == "completed")
    assert manager.effective_exit_code(inst_id, process) == expected_exit_code
    assert [entry.content for entry in error_entries] == [message]


@pytest.mark.asyncio
async def test_consume_output_records_fatal_result_for_outer_lifecycle(
    db_factory,
):
    """Non-chat lifecycle must see provider failure despite OS exit zero."""

    error_text = "API Error: upstream_error: provider unavailable"
    async with db_factory() as db:
        inst = Instance(name="lifecycle-fatal-result-inst")
        task = Task(
            title="lifecycle fatal result task",
            description="d",
            status="executing",
        )
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    process = _make_mock_process(returncode=0)
    output = iter([
        json.dumps({
            "type": "result",
            "is_error": True,
            "result": error_text,
        }).encode() + b"\n",
        b"",
    ])

    async def readline():
        return next(output)

    process.stdout.readline = readline
    im = InstanceManager(
        db_factory,
        MagicMock(broadcast=AsyncMock()),
    )
    im.processes[inst_id] = process

    await im._consume_output(
        inst_id,
        task_id,
        process,
        chat_initiated=False,
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)

    assert task.status == "executing"
    assert process.returncode == 0
    assert im.effective_exit_code(inst_id, process) == 1
    assert im.effective_exit_code(inst_id, object()) == -1


@pytest.mark.asyncio
async def test_consume_output_chat_initiated_interrupt_marks_completed(db_factory):
    """When chat_initiated=True and process is interrupted (SIGINT), task is marked completed."""
    async with db_factory() as db:
        inst = Instance(name="chat-int-inst")
        db.add(inst)
        task = Task(title="chat interrupt task", description="d", status="executing")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    mock_proc = _make_mock_process(returncode=-2)  # SIGINT
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    im.processes[inst_id] = mock_proc

    await im._consume_output(inst_id, task_id, mock_proc, chat_initiated=True)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "completed"


def test_internal_codex_abort_is_not_a_successful_chat_terminal():
    """Admission/transport cleanup must not masquerade as user Interrupt."""

    process = MagicMock(termination_kind="internal_abort")
    assert not InstanceManager._chat_terminal_succeeded(process, 130)
    process.termination_kind = "user_interrupt"
    assert InstanceManager._chat_terminal_succeeded(process, 130)
    process.termination_kind = "timeout"
    assert not InstanceManager._chat_terminal_succeeded(process, 130)
    assert InstanceManager._chat_terminal_succeeded(process, 0)


async def _run_crashed_chat_consumer(
    manager,
    instance_id,
    task_id,
    process,
    started_at,
    *,
    message="recovery bookkeeping exploded",
):
    manager._consume_output_impl = AsyncMock(
        side_effect=RuntimeError(message)
    )
    manager.processes[instance_id] = process
    consumer = asyncio.create_task(
        manager._consume_output(
            instance_id,
            task_id,
            process,
            chat_initiated=True,
        )
    )
    manager._track_output_consumer(
        instance_id,
        process,
        consumer,
        chat_initiated=True,
        task_id=task_id,
        task_retry_count=0,
        instance_started_at=started_at,
    )
    with pytest.raises(RuntimeError, match=message):
        await consumer
    # Let the identity-safe done callback finish its map cleanup.
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_consumer_exception_recovery_locks_task_before_instance(
    db_factory,
):
    """Unexpected consumer recovery follows Task -> Instance ordering."""

    started_at = datetime(2026, 7, 23, 15, 0, 0)
    async with db_factory() as db:
        instance = Instance(
            name="consumer-recovery-lock-order",
            status="running",
            pid=74_101,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="consumer recovery lock order",
            status="executing",
            retry_count=0,
            instance_id=instance.id,
            started_at=started_at,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    from sqlalchemy.sql.dml import Update

    update_tables: list[str] = []

    @asynccontextmanager
    async def recording_factory():
        async with db_factory() as db:
            class SessionProxy:
                def __getattr__(self, name):
                    return getattr(db, name)

                async def execute(self, statement, *args, **kwargs):
                    if isinstance(statement, Update):
                        update_tables.append(statement.table.name)
                    return await db.execute(statement, *args, **kwargs)

            yield SessionProxy()

    manager = InstanceManager(
        recording_factory,
        MagicMock(broadcast=AsyncMock()),
    )
    await _run_crashed_chat_consumer(
        manager,
        instance_id,
        task_id,
        _make_mock_process(pid=74_101, returncode=1),
        started_at,
    )

    assert update_tables == ["tasks", "instances", "tasks"]


@pytest.mark.asyncio
async def test_consumer_exception_recovery_completes_and_publishes_failed(
    db_factory,
):
    """Recovery persists a terminal timestamp and publishes that generation."""

    started_at = datetime(2026, 7, 23, 15, 1, 0)
    async with db_factory() as db:
        instance = Instance(
            name="consumer-recovery-completed-at",
            status="running",
            pid=74_102,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="consumer recovery completed at",
            status="executing",
            retry_count=0,
            instance_id=instance.id,
            started_at=started_at,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    broadcaster = MagicMock(broadcast=AsyncMock())
    manager = InstanceManager(db_factory, broadcaster)
    await _run_crashed_chat_consumer(
        manager,
        instance_id,
        task_id,
        _make_mock_process(pid=74_102, returncode=1),
        started_at,
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
    assert task.status == "failed"
    assert task.completed_at is not None
    assert task.error_message == (
        "Output bookkeeping failed: recovery bookkeeping exploded"
    )
    assert instance.status == "error"
    assert instance.pid is None
    assert instance.current_task_id is None
    broadcaster.broadcast.assert_awaited_once_with(
        "tasks",
        {
            "event": "status_change",
            "task_id": task_id,
            "new_status": "failed",
            "instance_id": instance_id,
        },
    )


@pytest.mark.asyncio
async def test_consumer_exception_recovery_suppresses_stale_failed_publication(
    db_factory,
):
    """A retry in the recovery commit->publish gap suppresses old failed."""

    from sqlalchemy import update

    started_at = datetime(2026, 7, 23, 15, 2, 0)
    retry_started_at = datetime(2026, 7, 23, 15, 2, 1)
    async with db_factory() as db:
        instance = Instance(
            name="consumer-recovery-publish-fence",
            status="running",
            pid=74_103,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="consumer recovery publish fence",
            status="executing",
            retry_count=0,
            instance_id=instance.id,
            started_at=started_at,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    recovery_commit_seen = False

    @asynccontextmanager
    async def retry_after_recovery_commit_factory():
        nonlocal recovery_commit_seen
        async with db_factory() as db:
            class SessionProxy:
                def __getattr__(self, name):
                    return getattr(db, name)

                async def commit(self):
                    nonlocal recovery_commit_seen
                    await db.commit()
                    if recovery_commit_seen:
                        return
                    recovery_commit_seen = True
                    async with db_factory() as retry_db:
                        await retry_db.execute(
                            update(Task)
                            .where(Task.id == task_id)
                            .values(
                                status="executing",
                                retry_count=Task.retry_count + 1,
                                started_at=retry_started_at,
                                completed_at=None,
                                error_message=None,
                            )
                        )
                        await retry_db.commit()

            yield SessionProxy()

    broadcaster = MagicMock(broadcast=AsyncMock())
    manager = InstanceManager(
        retry_after_recovery_commit_factory,
        broadcaster,
    )
    await _run_crashed_chat_consumer(
        manager,
        instance_id,
        task_id,
        _make_mock_process(pid=74_103, returncode=1),
        started_at,
    )

    assert recovery_commit_seen is True
    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "executing"
    assert task.retry_count == 1
    assert task.started_at == retry_started_at
    assert task.completed_at is None
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_consumer_exception_recovery_rejects_started_at_aba(db_factory):
    """Same PID/task/retry cannot let an old consumer clear a newer turn."""

    old_started_at = datetime(2026, 7, 23, 15, 3, 0)
    new_started_at = datetime(2026, 7, 23, 15, 3, 1)
    async with db_factory() as db:
        instance = Instance(
            name="consumer-recovery-started-at-aba",
            status="running",
            pid=74_104,
            started_at=new_started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="consumer recovery started-at ABA",
            status="executing",
            retry_count=0,
            instance_id=instance.id,
            started_at=old_started_at,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    broadcaster = MagicMock(broadcast=AsyncMock())
    manager = InstanceManager(db_factory, broadcaster)
    await _run_crashed_chat_consumer(
        manager,
        instance_id,
        task_id,
        _make_mock_process(pid=74_104, returncode=1),
        old_started_at,
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
    assert task.status == "executing"
    assert task.error_message is None
    assert instance.status == "running"
    assert instance.pid == 74_104
    assert instance.current_task_id == task_id
    assert instance.started_at == new_started_at
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_consumer_exception_recovery_retains_ambiguous_cas_miss(
    db_factory,
):
    """A same-token Instance CAS miss is fail-closed, not treated as stale."""

    started_at = datetime(2026, 7, 23, 15, 3, 30)
    async with db_factory() as db:
        instance = Instance(
            name="consumer-recovery-ambiguous-cas",
            status="running",
            pid=74_199,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="consumer recovery ambiguous CAS",
            status="executing",
            retry_count=0,
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    process = _make_mock_process(pid=74_109, returncode=1)
    manager = InstanceManager(
        db_factory,
        MagicMock(broadcast=AsyncMock()),
    )
    manager._consume_output_impl = AsyncMock(
        side_effect=RuntimeError("ambiguous CAS bookkeeping")
    )
    manager.processes[instance_id] = process
    consumer = asyncio.create_task(
        manager._consume_output(
            instance_id,
            task_id,
            process,
            chat_initiated=True,
        )
    )
    manager._track_output_consumer(
        instance_id,
        process,
        consumer,
        chat_initiated=True,
        task_id=task_id,
        task_retry_count=0,
        instance_started_at=started_at,
    )

    with pytest.raises(
        ConsumerRecoveryUnsettledError,
        match="Could not confirm output consumer recovery",
    ):
        await consumer
    await asyncio.sleep(0)

    recovery_key = (instance_id, process)
    assert recovery_key in manager._consumer_recovery_pending
    assert manager.processes[instance_id] is process
    assert manager._tasks[instance_id] is consumer
    assert manager._consumer_records[instance_id].process is process
    assert manager.is_running(instance_id) is True
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
    assert task.status == "executing"
    assert task.error_message is None
    assert instance.status == "running"
    assert instance.pid == 74_199
    assert instance.current_task_id == task_id
    assert instance.started_at == started_at


@pytest.mark.asyncio
async def test_untracked_consumer_recovery_is_fail_closed(db_factory):
    """A process-only legacy consumer never performs id-only DB recovery."""

    started_at = datetime(2026, 7, 23, 15, 4, 0)
    async with db_factory() as db:
        instance = Instance(
            name="consumer-recovery-untracked",
            status="running",
            pid=74_105,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="consumer recovery untracked",
            status="executing",
            retry_count=0,
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    process = _make_mock_process(pid=74_105, returncode=1)
    manager = InstanceManager(
        db_factory,
        MagicMock(broadcast=AsyncMock()),
    )
    manager._consume_output_impl = AsyncMock(
        side_effect=RuntimeError("untracked bookkeeping")
    )
    manager.processes[instance_id] = process
    consumer = asyncio.create_task(
        manager._consume_output(
            instance_id,
            task_id,
            process,
            chat_initiated=True,
        )
    )
    manager._tasks[instance_id] = consumer

    with pytest.raises(
        ConsumerRecoveryUnsettledError,
        match="lacks an exact generation",
    ):
        await consumer

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
    assert task.status == "executing"
    assert task.error_message is None
    assert instance.status == "running"
    assert instance.pid == process.pid
    assert instance.current_task_id == task_id
    assert manager.processes[instance_id] is process
    assert manager._tasks[instance_id] is consumer
    assert (instance_id, process) in manager._consumer_recovery_pending
    assert manager.is_running(instance_id) is True
    assert await manager.stop(
        instance_id,
        expected_task_id=task_id,
        task_status="cancelled",
    ) is False
    assert manager.processes[instance_id] is process
    assert (instance_id, process) in manager._consumer_recovery_pending
    process.send_signal.assert_not_called()
    process.terminate.assert_not_called()
    process.kill.assert_not_called()


@pytest.mark.asyncio
async def test_consumer_recovery_db_failure_retains_evidence_until_stop_retry(
    db_factory,
):
    """A failed recovery commit remains visible and stop can retry it."""

    started_at = datetime(2026, 7, 23, 15, 5, 0)
    async with db_factory() as db:
        instance = Instance(
            name="consumer-recovery-db-failure",
            status="running",
            pid=74_106,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="consumer recovery DB failure",
            status="executing",
            retry_count=0,
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    @asynccontextmanager
    async def failing_commit_factory():
        async with db_factory() as db:
            class SessionProxy:
                def __getattr__(self, name):
                    return getattr(db, name)

                async def commit(self):
                    raise RuntimeError("recovery database unavailable")

            yield SessionProxy()

    process = _make_mock_process(pid=74_106, returncode=1)
    broadcaster = MagicMock(broadcast=AsyncMock())
    manager = InstanceManager(failing_commit_factory, broadcaster)
    manager._consume_output_impl = AsyncMock(
        side_effect=RuntimeError("bookkeeping before DB outage")
    )
    manager.processes[instance_id] = process
    consumer = asyncio.create_task(
        manager._consume_output(
            instance_id,
            task_id,
            process,
            chat_initiated=True,
        )
    )
    manager._track_output_consumer(
        instance_id,
        process,
        consumer,
        chat_initiated=True,
        task_id=task_id,
        task_retry_count=0,
        instance_started_at=started_at,
    )

    with pytest.raises(
        ConsumerRecoveryUnsettledError,
        match="Could not confirm output consumer recovery",
    ):
        await consumer
    await asyncio.sleep(0)

    recovery_key = (instance_id, process)
    assert recovery_key in manager._consumer_recovery_pending
    assert recovery_key in manager._consumer_errors
    assert manager.processes[instance_id] is process
    assert manager._tasks[instance_id] is consumer
    assert manager._consumer_records[instance_id].process is process
    assert manager.is_running(instance_id) is True
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
    assert task.status == "executing"
    assert instance.status == "running"
    assert instance.pid == process.pid
    assert instance.current_task_id == task_id

    manager.db_factory = db_factory
    assert await manager.stop(
        instance_id,
        expected_task_id=task_id,
        expected_pid=process.pid,
        expected_started_at=started_at,
        task_status="cancelled",
    )

    assert recovery_key not in manager._consumer_recovery_pending
    assert recovery_key not in manager._consumer_errors
    assert instance_id not in manager.processes
    assert instance_id not in manager._tasks
    assert instance_id not in manager._consumer_records
    assert manager.is_running(instance_id) is False
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
    assert task.status == "cancelled"
    assert instance.status == "idle"
    assert instance.pid is None
    assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_consumer_exception_recovery_without_task_releases_instance(
    db_factory,
):
    """A tracked no-task generation settles only its exact Instance row."""

    started_at = datetime(2026, 7, 23, 15, 6, 0)
    async with db_factory() as db:
        instance = Instance(
            name="consumer-recovery-no-task",
            status="running",
            pid=74_107,
            started_at=started_at,
        )
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
        instance_id = instance.id

    process = _make_mock_process(pid=74_107, returncode=1)
    broadcaster = MagicMock(broadcast=AsyncMock())
    manager = InstanceManager(db_factory, broadcaster)
    manager._consume_output_impl = AsyncMock(
        side_effect=RuntimeError("no-task bookkeeping")
    )
    manager.processes[instance_id] = process
    consumer = asyncio.create_task(
        manager._consume_output(instance_id, None, process)
    )
    manager._track_output_consumer(
        instance_id,
        process,
        consumer,
        task_id=None,
        instance_started_at=started_at,
    )
    with pytest.raises(RuntimeError, match="no-task bookkeeping"):
        await consumer
    await asyncio.sleep(0)

    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
    assert instance.status == "error"
    assert instance.pid is None
    assert instance.current_task_id is None
    assert instance_id not in manager.processes
    assert instance_id not in manager._tasks
    assert (instance_id, process) in manager._consumer_errors
    assert not manager._consumer_recovery_pending
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_chat_consumer_recovery_leaves_task_for_dispatcher(
    db_factory,
):
    """Non-chat recovery releases Instance but does not decide Task status."""

    started_at = datetime(2026, 7, 23, 15, 7, 0)
    async with db_factory() as db:
        instance = Instance(
            name="consumer-recovery-non-chat",
            status="running",
            pid=74_108,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="consumer recovery non-chat",
            status="executing",
            retry_count=0,
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    process = _make_mock_process(pid=74_108, returncode=1)
    manager = InstanceManager(
        db_factory,
        MagicMock(broadcast=AsyncMock()),
    )
    manager._consume_output_impl = AsyncMock(
        side_effect=RuntimeError("dispatcher bookkeeping")
    )
    manager.processes[instance_id] = process
    consumer = asyncio.create_task(
        manager._consume_output(
            instance_id,
            task_id,
            process,
            chat_initiated=False,
        )
    )
    manager._track_output_consumer(
        instance_id,
        process,
        consumer,
        chat_initiated=False,
        task_id=task_id,
        task_retry_count=0,
        instance_started_at=started_at,
    )
    with pytest.raises(RuntimeError, match="dispatcher bookkeeping"):
        await consumer
    await asyncio.sleep(0)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
    assert task.status == "executing"
    assert instance.status == "error"
    assert instance.pid is None
    assert instance.current_task_id is None
    assert (instance_id, process) in manager._consumer_errors
    assert not manager._consumer_recovery_pending
    assert instance_id not in manager.processes
    assert instance_id not in manager._tasks


@pytest.mark.asyncio
async def test_old_consumer_cannot_finalize_new_task_retry_generation(db_factory):
    async with db_factory() as db:
        instance = Instance(
            name="task-retry-fence",
            status="running",
            pid=73_001,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="new retry generation",
            status="executing",
            retry_count=1,
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    process = _make_mock_process(pid=73_001, returncode=0)
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    im.processes[instance_id] = process
    consumer = asyncio.create_task(
        im._consume_output(
            instance_id,
            task_id,
            process,
            chat_initiated=True,
        )
    )
    # The exact process belongs to retry_count=0, but the durable Task has
    # already advanced to retry_count=1 on the same reusable slot (ABA).
    im._track_output_consumer(
        instance_id,
        process,
        consumer,
        chat_initiated=True,
        task_id=task_id,
        task_retry_count=0,
    )
    await consumer

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "executing"
        assert task.retry_count == 1


@pytest.mark.asyncio
async def test_direct_chat_terminal_transaction_locks_task_before_instance(
    db_factory,
):
    """Direct CLI chat cleanup follows the global Task -> Instance order."""

    started_at = datetime(2026, 7, 23, 6, 7, 8)
    async with db_factory() as db:
        instance = Instance(
            name="direct-lock-order",
            status="running",
            pid=73_101,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="direct lock order",
            status="executing",
            retry_count=0,
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    from sqlalchemy.sql.dml import Update

    update_tables: list[str] = []

    @asynccontextmanager
    async def recording_factory():
        async with db_factory() as db:
            class SessionProxy:
                def __getattr__(self, name):
                    return getattr(db, name)

                async def execute(self, statement, *args, **kwargs):
                    if isinstance(statement, Update):
                        update_tables.append(statement.table.name)
                    return await db.execute(statement, *args, **kwargs)

            yield SessionProxy()

    process = _make_mock_process(pid=73_101, returncode=0)
    broadcaster = MagicMock(broadcast=AsyncMock())
    manager = InstanceManager(recording_factory, broadcaster)
    manager.processes[instance_id] = process
    consumer = asyncio.create_task(
        manager._consume_output(
            instance_id,
            task_id,
            process,
            chat_initiated=True,
        )
    )
    manager._track_output_consumer(
        instance_id,
        process,
        consumer,
        chat_initiated=True,
        task_id=task_id,
        task_retry_count=0,
        instance_started_at=started_at,
    )
    await consumer

    assert update_tables[:2] == ["tasks", "tasks"]
    assert update_tables[2] == "instances"


@pytest.mark.asyncio
async def test_direct_chat_consumer_suppresses_events_after_retry_claim(
    db_factory,
):
    """A retry in the commit->publish window cannot receive old exit events."""

    from sqlalchemy import update

    started_at = datetime(2026, 7, 23, 7, 8, 9)
    async with db_factory() as db:
        instance = Instance(
            name="direct-publish-race",
            status="running",
            pid=73_102,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="direct publish race",
            status="executing",
            retry_count=0,
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    terminal_commit_seen = False

    @asynccontextmanager
    async def replacement_after_terminal_commit_factory():
        nonlocal terminal_commit_seen
        async with db_factory() as db:
            class SessionProxy:
                def __getattr__(self, name):
                    return getattr(db, name)

                async def commit(self):
                    nonlocal terminal_commit_seen
                    await db.commit()
                    if terminal_commit_seen:
                        return
                    terminal_commit_seen = True
                    async with db_factory() as replacement_db:
                        await replacement_db.execute(
                            update(Task)
                            .where(Task.id == task_id)
                            .values(
                                status="executing",
                                retry_count=Task.retry_count + 1,
                                started_at=datetime(2026, 7, 23, 7, 8, 10),
                                completed_at=None,
                            )
                        )
                        await replacement_db.commit()

            yield SessionProxy()

    process = _make_mock_process(pid=73_102, returncode=0)
    broadcaster = MagicMock(broadcast=AsyncMock())
    manager = InstanceManager(
        replacement_after_terminal_commit_factory,
        broadcaster,
    )
    manager.processes[instance_id] = process
    consumer = asyncio.create_task(
        manager._consume_output(
            instance_id,
            task_id,
            process,
            chat_initiated=True,
        )
    )
    manager._track_output_consumer(
        instance_id,
        process,
        consumer,
        chat_initiated=True,
        task_id=task_id,
        task_retry_count=0,
        instance_started_at=started_at,
    )
    await consumer

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "executing"
        assert task.retry_count == 1
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_prompt_too_long_compaction_rejects_changed_task_generation(
    db_factory,
):
    """A stale consumer must not clear a newer session or enqueue its retry."""

    started_at = datetime(2026, 7, 23, 8, 9, 10)
    async with db_factory() as db:
        instance = Instance(
            name="stale-compaction",
            status="running",
            pid=73_103,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="stale compaction",
            status="executing",
            retry_count=0,
            instance_id=instance.id,
            session_id="old-session",
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    process = _make_mock_process(pid=73_103, returncode=1)
    output = iter((b"prompt-too-long\n", b""))

    async def readline():
        return next(output)

    process.stdout.readline = readline
    broadcaster = MagicMock(broadcast=AsyncMock())
    manager = InstanceManager(db_factory, broadcaster)
    manager.processes[instance_id] = process
    manager.parser.parse_line = MagicMock(
        return_value=[
            {
                "event_type": "message",
                "role": "assistant",
                "content": "Prompt is too long",
                "is_error": False,
            }
        ]
    )

    dispatcher = MagicMock()
    dispatcher.enqueue_message = AsyncMock()

    async def advance_generation_while_summarizing(_task_id, _session_id, db):
        current = await db.get(Task, task_id)
        current.retry_count = 1
        current.session_id = "new-session"
        await db.flush()
        return "summary from the superseded generation"

    dispatcher._compact_session = AsyncMock(
        side_effect=advance_generation_while_summarizing
    )

    with patch("backend.main.dispatcher", dispatcher):
        consumer = asyncio.create_task(
            manager._consume_output(
                instance_id,
                task_id,
                process,
                chat_initiated=True,
            )
        )
        manager._track_output_consumer(
            instance_id,
            process,
            consumer,
            chat_initiated=True,
            task_id=task_id,
            task_retry_count=0,
            instance_started_at=started_at,
        )
        await consumer

    dispatcher._compact_session.assert_awaited_once()
    dispatcher.enqueue_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_consume_output_chat_initiated_no_override_cancelled(db_factory):
    """Consumer does not override 'cancelled' status even for chat_initiated=True runs."""
    async with db_factory() as db:
        inst = Instance(name="chat-cancel-inst")
        db.add(inst)
        task = Task(title="cancelled chat task", description="d", status="cancelled")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    mock_proc = _make_mock_process(returncode=0)
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    im.processes[inst_id] = mock_proc

    await im._consume_output(inst_id, task_id, mock_proc, chat_initiated=True)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "cancelled"


# === enable_workflows tests ===


def test_build_command_claude_enable_workflows_default():
    """_build_command defaults to enable_workflows=False, adding --disallowedTools Workflow."""
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(provider="claude", prompt="hi", model=None, resume_session_id=None, effort_level=None)
    assert "--disallowedTools" in cmd
    idx = cmd.index("--disallowedTools")
    assert cmd[idx + 1] == "Workflow"


def test_build_command_claude_enable_workflows_true():
    """_build_command with enable_workflows=True does NOT include --disallowedTools."""
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(provider="claude", prompt="hi", model=None, resume_session_id=None, effort_level=None, enable_workflows=True)
    assert "--disallowedTools" not in cmd


def test_build_command_claude_enable_workflows_false():
    """_build_command with enable_workflows=False includes --disallowedTools Workflow."""
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(provider="claude", prompt="hi", model=None, resume_session_id=None, effort_level=None, enable_workflows=False)
    assert "--disallowedTools" in cmd
    idx = cmd.index("--disallowedTools")
    assert cmd[idx + 1] == "Workflow"


def test_build_command_codex_ignores_enable_workflows():
    """Codex provider does not include --disallowedTools regardless of enable_workflows."""
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(provider="codex", prompt="hi", model=None, resume_session_id=None, effort_level=None, enable_workflows=False)
    assert "--disallowedTools" not in cmd


@pytest.mark.asyncio
async def test_launch_enable_workflows_false_includes_flag(db_factory):
    """launch(enable_workflows=False) generates command with --disallowedTools Workflow."""
    async with db_factory() as db:
        inst = Instance(name="wf-disabled-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc) as mock_exec:
        await im.launch(instance_id=inst_id, prompt="hi", cwd="/tmp", enable_workflows=False)

    cmd_args = mock_exec.call_args[0]
    assert "--disallowedTools" in cmd_args
    idx = cmd_args.index("--disallowedTools")
    assert cmd_args[idx + 1] == "Workflow"
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_launch_enable_workflows_true_omits_flag(db_factory):
    """launch(enable_workflows=True) generates command without --disallowedTools."""
    async with db_factory() as db:
        inst = Instance(name="wf-enabled-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc) as mock_exec:
        await im.launch(instance_id=inst_id, prompt="hi", cwd="/tmp", enable_workflows=True)

    cmd_args = mock_exec.call_args[0]
    assert "--disallowedTools" not in cmd_args
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_launch_default_disables_workflows(db_factory):
    """launch() without explicit enable_workflows defaults to False (workflows disabled)."""
    async with db_factory() as db:
        inst = Instance(name="wf-default-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc) as mock_exec:
        await im.launch(instance_id=inst_id, prompt="hi", cwd="/tmp")

    cmd_args = mock_exec.call_args[0]
    assert "--disallowedTools" in cmd_args
    await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_launch_chat_initiated_stores_enable_workflows_in_params(db_factory):
    """chat_initiated launch stores enable_workflows in _launch_params for pool rotation."""
    async with db_factory() as db:
        inst = Instance(name="params-wf-inst")
        db.add(inst)
        await db.flush()
        task = Task(
            title="params task",
            description="d",
            status="executing",
            instance_id=inst.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    im.task_message_enqueuer = AsyncMock()

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc):
        await im.launch(instance_id=inst_id, prompt="hi", task_id=task_id, cwd="/tmp", chat_initiated=True, enable_workflows=True)

    assert inst_id in im._launch_params
    assert im._launch_params[inst_id]["enable_workflows"] is True
    await asyncio.sleep(0.1)
    im.task_message_enqueuer.assert_awaited_once()


@pytest.mark.asyncio
async def test_launch_chat_initiated_stores_enable_workflows_false_in_params(db_factory):
    """chat_initiated launch stores enable_workflows=False in _launch_params."""
    async with db_factory() as db:
        inst = Instance(name="params-wf-false-inst")
        db.add(inst)
        await db.flush()
        task = Task(
            title="params task",
            description="d",
            status="executing",
            instance_id=inst.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    im.task_message_enqueuer = AsyncMock()

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc):
        await im.launch(instance_id=inst_id, prompt="hi", task_id=task_id, cwd="/tmp", chat_initiated=True, enable_workflows=False)

    assert im._launch_params[inst_id]["enable_workflows"] is False
    await asyncio.sleep(0.1)
    im.task_message_enqueuer.assert_awaited_once()


@pytest.mark.asyncio
async def test_launch_non_chat_does_not_store_params(db_factory):
    """Non-chat launch does not store _launch_params."""
    async with db_factory() as db:
        inst = Instance(name="no-params-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec", new_callable=AsyncMock, return_value=mock_proc):
        await im.launch(instance_id=inst_id, prompt="hi", cwd="/tmp", enable_workflows=True)

    assert inst_id not in im._launch_params
    await asyncio.sleep(0.1)


# ---------- PTY mode wiring (use_pty_mode flag) ----------

class _FakeDB:
    def __init__(self):
        self.executed = []

    async def execute(self, stmt):
        self.executed.append(stmt)
        return MagicMock(rowcount=1)

    async def commit(self):
        pass

    async def scalar(self, stmt):
        self.executed.append(stmt)
        return datetime.utcnow()

    async def get(self, model, pk):
        inst = MagicMock()
        inst.current_task_id = None
        return inst


class _FakeDBFactory:
    def __init__(self):
        self.db = _FakeDB()

    def __call__(self):
        return self

    async def __aenter__(self):
        return self.db

    async def __aexit__(self, *exc):
        return False


def test_pty_backend_disabled_by_default():
    im = InstanceManager(MagicMock(), MagicMock())
    assert im._pty_backend is None


@pytest.mark.asyncio
async def test_launch_delegates_to_pty_backend_for_claude():
    im = InstanceManager(_FakeDBFactory(), MagicMock())
    calls = {}

    class FakeBackend:
        async def launch_for_ccm(self, **kwargs):
            calls.update(kwargs)
            im.processes[kwargs["instance_id"]] = MagicMock(pid=4242)
            return "sess-1"

    im._pty_backend = FakeBackend()
    im._pty_enabled = True
    pid = await im.launch(
        instance_id=7, prompt="do it", task_id=3, cwd="/w",
        model="default", provider="claude",
    )
    assert pid == 4242
    assert calls["instance_id"] == 7
    assert calls["prompt"] == "do it"
    assert calls["model"] is None  # "default" normalized away
    assert calls["cwd"] == "/w"


@pytest.mark.asyncio
async def test_pty_launch_injects_canonical_task_skill_context():
    im = InstanceManager(_FakeDBFactory(), MagicMock())
    calls = {}

    class FakeBackend:
        async def launch_for_ccm(self, **kwargs):
            calls.update(kwargs)
            im.processes[kwargs["instance_id"]] = MagicMock(pid=4244)
            return "sess-user-skills"

    im._pty_backend = FakeBackend()
    im._pty_enabled = True
    with patch(
        "backend.services.skill_context.build_task_skill_context",
        new=AsyncMock(
            return_value="## User Skills\n- **Review** (id=2): edge cases",
        ),
    ), patch(
        "backend.services.ask_user_settings.ensure_ask_user_hook",
    ):
        await im.launch(
            instance_id=8,
            prompt="review this",
            task_id=4,
            cwd="/w",
            provider="claude",
        )

    assert "## User Skills" in calls["prompt"]
    assert "**Review** (id=2)" in calls["prompt"]
    assert calls["prompt"].startswith("<ccm-task-skill-context>")
    assert calls["prompt"].endswith(
        "</ccm-task-skill-context>\n\nreview this"
    )


@pytest.mark.asyncio
async def test_launch_pty_rejects_dead_startup_before_persisting_running(
    db_factory,
):
    async with db_factory() as db:
        instance = Instance(name="pty-dead-startup", status="idle")
        db.add(instance)
        await db.flush()
        task = Task(
            title="pty-dead-startup",
            description="startup must fail closed",
            status="executing",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    class NativeProcess:
        exit_code = 1

    class NativeSession:
        is_alive = False
        _process = NativeProcess()

    class Process:
        def __init__(self):
            self.pid = 4243
            self.returncode = None
            self.session = NativeSession()

        async def wait(self):
            return self.returncode

        def kill(self):
            self.returncode = -signal.SIGKILL

    process = Process()
    consumer = None
    stopped = []
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))

    class FakeBackend:
        async def launch_for_ccm(self, **kwargs):
            nonlocal consumer
            consumer = asyncio.create_task(asyncio.Event().wait())
            im.processes[kwargs["instance_id"]] = process
            im._tasks[kwargs["instance_id"]] = consumer
            return "session-dead-startup"

        async def stop(self, instance_arg):
            stopped.append(instance_arg)
            assert consumer is not None and consumer.cancelling()
            process.returncode = 1

    im._pty_backend = FakeBackend()

    with pytest.raises(
        RuntimeError,
        match=r"PTY process exited during startup \(exit_code=1\)",
    ):
        await im._launch_pty(
            instance_id=instance_id,
            prompt="run",
            task_id=task_id,
            cwd="/tmp",
            model=None,
            resume_session_id=None,
            loop_iteration=None,
            git_env=None,
            thinking_budget=None,
            effort_level=None,
            chat_initiated=False,
            config_dir=None,
            enable_workflows=False,
            enabled_skills=None,
            mcp_config_path=None,
        )

    assert stopped == [instance_id]
    assert consumer is not None and consumer.cancelled()
    assert instance_id not in im.processes
    assert instance_id not in im._tasks
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.session_id is None
        assert instance.status == "idle"
        assert instance.pid is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_launch_pty_does_not_clear_concurrent_background_epoch(
    db_factory,
):
    async with db_factory() as db:
        instance = Instance(name="pty-background-race", status="idle")
        db.add(instance)
        await db.flush()
        task = Task(
            title="pty-background-race",
            description="late autonomous activity wins launch fence",
            status="executing",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    class Process:
        def __init__(self):
            self.pid = 42_433
            self.returncode = None

        async def wait(self):
            return self.returncode

    process = Process()
    consumer = None
    stopped = []
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))

    class FakeBackend:
        async def launch_for_ccm(self, **kwargs):
            nonlocal consumer
            consumer = asyncio.create_task(asyncio.Event().wait())
            im.processes[kwargs["instance_id"]] = process
            im._tasks[kwargs["instance_id"]] = consumer
            async with db_factory() as db:
                task = await db.get(Task, task_id)
                task.pty_background_generation = "late-background-epoch"
                await db.commit()
            return "session-background-race"

        async def stop(self, instance_arg):
            stopped.append(instance_arg)
            process.returncode = -signal.SIGKILL

    im._pty_backend = FakeBackend()
    with pytest.raises(LaunchSupersededError):
        await im._launch_pty(
            instance_id=instance_id,
            prompt="run",
            task_id=task_id,
            cwd="/tmp",
            model=None,
            resume_session_id=None,
            loop_iteration=None,
            git_env=None,
            thinking_budget=None,
            effort_level=None,
            chat_initiated=True,
            config_dir=None,
            enable_workflows=False,
            enabled_skills=None,
            mcp_config_path=None,
        )

    assert stopped == [instance_id]
    assert consumer is not None and consumer.cancelled()
    assert instance_id not in im.processes
    assert instance_id not in im._tasks
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert (
            task.pty_background_generation == "late-background-epoch"
        )
        assert instance.status == "idle"
        assert instance.pid is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_pty_container_binary_override_is_isolated_across_instances():
    im = InstanceManager(_FakeDBFactory(), MagicMock())
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    observed = {}

    class Config:
        claude_binary = "default-claude"

    class FakeBackend:
        def build_config(self, **kwargs):
            return Config()

        async def launch_for_ccm(self, **kwargs):
            config = self.build_config()
            observed[kwargs["instance_id"]] = config.claude_binary
            if kwargs["instance_id"] == 1:
                first_entered.set()
                await release_first.wait()
            im.processes[kwargs["instance_id"]] = MagicMock(
                pid=4200 + kwargs["instance_id"], returncode=None
            )
            return f"session-{kwargs['instance_id']}"

    im._pty_backend = FakeBackend()

    async def launch(instance_id, override=None):
        return await im._launch_pty(
            instance_id=instance_id,
            prompt="run",
            task_id=None,
            cwd="/tmp",
            model=None,
            resume_session_id=None,
            loop_iteration=None,
            git_env=None,
            thinking_budget=None,
            effort_level=None,
            chat_initiated=False,
            config_dir=None,
            enable_workflows=False,
            enabled_skills=None,
            mcp_config_path=None,
            claude_binary_override=override,
        )

    first = asyncio.create_task(launch(1, "/container/claude"))
    await first_entered.wait()
    second = asyncio.create_task(launch(2))
    await asyncio.sleep(0)
    assert 2 not in observed

    release_first.set()
    await asyncio.gather(first, second)
    assert observed == {1: "/container/claude", 2: "default-claude"}


@pytest.mark.asyncio
async def test_cancelled_pty_metadata_commit_cleans_exact_live_generation():
    commit_entered = asyncio.Event()

    class BlockingDB(_FakeDB):
        def __init__(self):
            super().__init__()
            self.commit_count = 0

        async def commit(self):
            self.commit_count += 1
            if self.commit_count == 1:
                commit_entered.set()
                await asyncio.Event().wait()

    class BlockingDBFactory(_FakeDBFactory):
        def __init__(self):
            self.db = BlockingDB()

    class Process:
        def __init__(self):
            self.pid = 4242
            self.returncode = None
            self.done = asyncio.Event()

        def kill(self):
            self.returncode = -9
            self.done.set()

        async def wait(self):
            await self.done.wait()
            return self.returncode

    im = InstanceManager(BlockingDBFactory(), MagicMock())
    process = Process()
    consumer = None
    stopped = []

    class FakeBackend:
        async def launch_for_ccm(self, **kwargs):
            nonlocal consumer
            consumer = asyncio.create_task(asyncio.Event().wait())
            im.processes[kwargs["instance_id"]] = process
            im._tasks[kwargs["instance_id"]] = consumer
            return "session-cancelled"

        async def stop(self, instance_id):
            stopped.append(instance_id)
            process.kill()

    im._pty_backend = FakeBackend()
    launching = asyncio.create_task(im._launch_pty(
        instance_id=7,
        prompt="run",
        task_id=3,
        cwd="/tmp",
        model=None,
        resume_session_id=None,
        loop_iteration=None,
        git_env=None,
        thinking_budget=None,
        effort_level=None,
        chat_initiated=False,
        config_dir=None,
        enable_workflows=False,
        enabled_skills=None,
        mcp_config_path=None,
    ))
    await commit_entered.wait()
    launching.cancel()
    with pytest.raises(asyncio.CancelledError):
        await launching

    assert stopped == [7]
    assert consumer is not None and consumer.cancelled()
    assert 7 not in im.processes
    assert 7 not in im._tasks
    assert 7 not in im._consumer_records


@pytest.mark.asyncio
async def test_failed_pty_backend_stop_retains_generation_evidence(db_factory):
    async with db_factory() as db:
        instance = Instance(name="pty-stop-failed", status="idle")
        db.add(instance)
        await db.flush()
        task = Task(
            title="pty-stop-failed",
            description="cancelled before metadata commit",
            status="cancelled",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    class Process:
        def __init__(self):
            self.pid = 54_323
            self.returncode = None
            self.kill_calls = 0

        def kill(self):
            self.kill_calls += 1
            self.returncode = -signal.SIGKILL

        async def wait(self):
            return self.returncode

    process = Process()
    consumer = None
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))

    class FakeBackend:
        async def launch_for_ccm(self, **kwargs):
            nonlocal consumer
            consumer = asyncio.create_task(asyncio.Event().wait())
            im.processes[kwargs["instance_id"]] = process
            im._tasks[kwargs["instance_id"]] = consumer
            return "session-stop-failed"

        async def stop(self, instance_arg):
            assert instance_arg == instance_id
            raise RuntimeError("backend session could not be stopped")

    im._pty_backend = FakeBackend()
    with pytest.raises(LaunchSupersededError):
        await im._launch_pty(
            instance_id=instance_id,
            prompt="run",
            task_id=task_id,
            cwd="/tmp",
            model=None,
            resume_session_id=None,
            loop_iteration=None,
            git_env=None,
            thinking_budget=None,
            effort_level=None,
            chat_initiated=False,
            config_dir=None,
            enable_workflows=False,
            enabled_skills=None,
            mcp_config_path=None,
        )

    assert process.kill_calls == 1
    assert consumer is not None and consumer.cancelled()
    assert im.processes[instance_id] is process
    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        assert instance.status == "error"
        assert instance.pid == process.pid
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_failed_pty_proxy_wait_retains_generation_evidence(db_factory):
    async with db_factory() as db:
        instance = Instance(name="pty-proxy-stuck", status="idle")
        db.add(instance)
        await db.flush()
        task = Task(
            title="pty-proxy-stuck",
            description="cancelled before metadata commit",
            status="cancelled",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    class Process:
        def __init__(self):
            self.pid = 54_324
            self.returncode = None
            self.kill_calls = 0
            self.never_exits = asyncio.Event()

        def kill(self):
            self.kill_calls += 1

        async def wait(self):
            await self.never_exits.wait()

    process = Process()
    consumer = None
    im = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))

    class FakeBackend:
        async def launch_for_ccm(self, **kwargs):
            nonlocal consumer
            consumer = asyncio.create_task(asyncio.Event().wait())
            im.processes[kwargs["instance_id"]] = process
            im._tasks[kwargs["instance_id"]] = consumer
            return "session-proxy-stuck"

        async def stop(self, instance_arg):
            assert instance_arg == instance_id

    wait_timeouts = []

    async def timeout_immediately(awaitable, *, timeout):
        wait_timeouts.append(timeout)
        close = getattr(awaitable, "close", None)
        if close is not None:
            close()
        raise asyncio.TimeoutError

    im._pty_backend = FakeBackend()
    with patch(
        "backend.services.instance_manager.asyncio.wait_for",
        side_effect=timeout_immediately,
    ):
        with pytest.raises(LaunchSupersededError):
            await im._launch_pty(
                instance_id=instance_id,
                prompt="run",
                task_id=task_id,
                cwd="/tmp",
                model=None,
                resume_session_id=None,
                loop_iteration=None,
                git_env=None,
                thinking_budget=None,
                effort_level=None,
                chat_initiated=False,
                config_dir=None,
                enable_workflows=False,
                enabled_skills=None,
                mcp_config_path=None,
            )

    assert wait_timeouts == [10]
    assert process.kill_calls == 1
    assert consumer is not None and consumer.cancelled()
    assert im.processes[instance_id] is process
    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        assert instance.status == "error"
        assert instance.pid == process.pid
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_launch_pty_ignores_codex_provider():
    im = InstanceManager(_FakeDBFactory(), MagicMock())

    class ExplodingBackend:
        async def launch_for_ccm(self, **kwargs):
            raise AssertionError("PTY backend must not be used for codex")

    im._pty_backend = ExplodingBackend()
    fake_proc = _make_mock_process(pid=51_001, returncode=None)
    with (
        patch(
            "asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=fake_proc),
        ),
        patch(
            "backend.services.instance_manager.os.killpg",
            side_effect=ProcessLookupError,
        ),
        patch.object(im, "_consume_output", new=AsyncMock()),
    ):
        await im.launch(
            instance_id=1, prompt="x", provider="codex", cwd="/w",
        )
    assert im.processes[1] is fake_proc


@pytest.mark.asyncio
async def test_stop_uses_pty_backend_for_managed_instance():
    from claude_pty.adapters.ccm import _PTYProcessProxy

    im = InstanceManager(_FakeDBFactory(), MagicMock())
    im.broadcaster.broadcast = AsyncMock()
    proxy = _PTYProcessProxy()
    im.processes[5] = proxy
    stopped = []

    class FakeBackend:
        _sessions = {5: object()}

        async def stop(self, instance_id):
            stopped.append(instance_id)
            proxy.complete(0)

    im._pty_backend = FakeBackend()
    ok = await im.stop(5)
    assert ok is True
    assert stopped == [5]
    assert 5 not in im.processes


@pytest.mark.asyncio
async def test_stop_completes_pty_proxy_from_confirmed_dead_native_session():
    """A lost on_exit callback must not strand a proxy after native reap."""

    from claude_pty.adapters.ccm import _PTYProcessProxy

    im = InstanceManager(_FakeDBFactory(), MagicMock())
    im.broadcaster.broadcast = AsyncMock()
    proxy = _PTYProcessProxy()
    native_process = types.SimpleNamespace(exit_code=-signal.SIGTERM)
    native_session = types.SimpleNamespace(
        is_alive=False,
        _process=native_process,
    )
    proxy.session = native_session
    proxy.pid = 51_337
    im.processes[5] = proxy

    class FakeBackend:
        _sessions = {5: native_session}

        async def stop(self, instance_id):
            assert instance_id == 5
            # Native process is already reaped, but the dependency's cancelled
            # consumer never reached proxy.complete().

    im._pty_backend = FakeBackend()
    ok = await im.stop(5)

    assert ok is True
    assert proxy.returncode == -signal.SIGTERM
    assert await proxy.wait() == -signal.SIGTERM
    assert 5 not in im.processes


# ---------- runtime PTY mode toggle ----------

def test_set_pty_mode_runtime_toggle():
    im = InstanceManager(MagicMock(), MagicMock())
    assert im.pty_mode_enabled is False

    # enable: lazy-creates backend (claude_pty installed in dev venv)
    assert im.set_pty_mode(True) is True
    assert im.pty_mode_enabled is True
    assert im._pty_backend is not None
    backend = im._pty_backend

    # disable: flag off, backend retained for in-flight sessions
    assert im.set_pty_mode(False) is False
    assert im.pty_mode_enabled is False
    assert im._pty_backend is backend

    # re-enable reuses the same backend
    assert im.set_pty_mode(True) is True
    assert im._pty_backend is backend


@pytest.mark.parametrize("unsafe_pid", [None, -1, 0, 1, False, True])
def test_managed_process_group_rejects_unsafe_identity(unsafe_pid):
    im = InstanceManager(MagicMock(), MagicMock())
    process = MagicMock(pid=unsafe_pid, returncode=None)
    im._process_groups[7] = process

    with patch("backend.services.instance_manager.os.killpg") as killpg:
        with pytest.raises(RuntimeError, match="unsafe process group identity"):
            im._signal_process_tree(7, process, signal.SIGKILL)

    killpg.assert_not_called()
    process.kill.assert_not_called()


@pytest.mark.asyncio
async def test_launch_respects_disabled_pty_mode():
    """With a backend present but mode disabled, claude goes through -p."""
    im = InstanceManager(_FakeDBFactory(), MagicMock())

    class ExplodingBackend:
        async def launch_for_ccm(self, **kwargs):
            raise AssertionError("PTY backend must not be used when disabled")

    im._pty_backend = ExplodingBackend()
    im._pty_enabled = False  # toggled off at runtime

    fake_proc = _make_mock_process(pid=51_002, returncode=None)
    with (
        patch(
            "asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=fake_proc),
        ),
        patch(
            "backend.services.instance_manager.os.killpg",
            side_effect=ProcessLookupError,
        ),
        patch.object(im, "_consume_output", new=AsyncMock()),
    ):
        await im.launch(instance_id=1, prompt="x", provider="claude", cwd="/w")
    assert im.processes[1] is fake_proc


@pytest.mark.asyncio
async def test_release_pty_session():
    im = InstanceManager(MagicMock(), MagicMock())
    # no backend -> no-op
    await im.release_pty_session("sid-x")

    class FakePool:
        removed = []

        async def remove(self, sid):
            FakePool.removed.append(sid)

    class FakeBackend:
        _pool = FakePool()

    im._pty_backend = FakeBackend()
    await im.release_pty_session("sid-x")
    assert FakePool.removed == ["sid-x"]
    await im.release_pty_session("")  # empty -> no-op
    assert FakePool.removed == ["sid-x"]


# ---------------------------------------------------------------------------
# Transient-overload turn-scoped flag (auto wait + retry on transient 429)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_event_sets_transient_flag_on_overload_error(db_factory):
    """An is_error event with the transient-429 wording flips the turn flag."""
    async with db_factory() as db:
        inst = Instance(name="transient-inst")
        db.add(inst)
        task = Task(title="t", description="d")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id, task_id = inst.id, task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    assert im.transient_error_seen(inst_id) is False
    await im._process_event(inst_id, task_id, {
        "event_type": "result",
        "role": "assistant",
        "content": ("API Error: Server is temporarily limiting requests "
                    "(not your usage limit) · Rate limited"),
        "is_error": True,
        "raw_json": "{}",
    })
    assert im.transient_error_seen(inst_id) is True


@pytest.mark.asyncio
async def test_process_event_usage_limit_does_not_set_transient_flag(db_factory):
    """A genuine usage-limit banner must rotate, not set the transient flag."""
    async with db_factory() as db:
        inst = Instance(name="usage-inst")
        db.add(inst)
        task = Task(title="t", description="d")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id, task_id = inst.id, task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    await im._process_event(inst_id, task_id, {
        "event_type": "result",
        "role": "assistant",
        "content": "You've hit your limit · resets 5pm (UTC)",
        "is_error": True,
        "raw_json": "{}",
    })
    assert im.transient_error_seen(inst_id) is False


@pytest.mark.asyncio
async def test_process_event_codex_usage_text_never_sets_claude_pty_limit_flag(
    db_factory,
):
    """Codex usage wording overlaps Claude regex but Codex is never PTY-managed."""
    async with db_factory() as db:
        inst = Instance(name="codex-usage-inst")
        task = Task(title="t", description="d", provider="codex")
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id, task_id = inst.id, task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    im._launch_params[inst_id] = {"provider": "codex"}

    await im._process_event(inst_id, task_id, {
        "event_type": "result",
        "role": "assistant",
        "content": "You've hit your usage limit for Codex",
        "is_error": True,
        "raw_json": "{}",
    })

    assert im.transient_error_seen(inst_id) is False
    assert im.pty_rate_limit_seen(inst_id) is False


@pytest.mark.asyncio
async def test_process_event_clean_event_leaves_flag_unset(db_factory):
    async with db_factory() as db:
        inst = Instance(name="clean-inst")
        db.add(inst)
        task = Task(title="t", description="d")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id, task_id = inst.id, task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    await im._process_event(inst_id, task_id, {
        "event_type": "message",
        "role": "assistant",
        "content": "All done, tests pass.",
        "is_error": False,
        "raw_json": "{}",
    })
    assert im.transient_error_seen(inst_id) is False


@pytest.mark.asyncio
async def test_process_event_orphan_overload_does_not_set_transient_flag(db_factory):
    """A REPLAYED transient-429 error (orphan / autonomous) must NOT re-flag.

    On resume PTY re-reads the JSONL and yields the previous turn's own
    api_error as an `orphan` event. If that re-set the turn flag,
    transient_error_seen() would stay True across a clean resume, so the host
    keeps "retrying" a turn that already succeeded and finally marks the task
    failed (the recover-then-failed bug). Only the CURRENT turn's live events
    count.
    """
    async with db_factory() as db:
        inst = Instance(name="orphan-inst")
        db.add(inst)
        task = Task(title="t", description="d")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id, task_id = inst.id, task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    overload = ("API Error: Server is temporarily limiting requests "
                "(not your usage limit) · Rate limited")

    # Stale backlog from the previous turn, replayed on resume → must be ignored.
    await im._process_event(inst_id, task_id, {
        "event_type": "result",
        "role": "assistant",
        "content": overload,
        "is_error": True,
        "orphan": True,
        "raw_json": "{}",
    })
    assert im.transient_error_seen(inst_id) is False

    # A background sub-agent turn's error is likewise not this turn's signal.
    await im._process_event(inst_id, task_id, {
        "event_type": "result",
        "role": "assistant",
        "content": overload,
        "is_error": True,
        "autonomous": True,
        "raw_json": "{}",
    })
    assert im.transient_error_seen(inst_id) is False


@pytest.mark.asyncio
async def test_launch_resets_transient_flag(db_factory):
    """A new launch() clears the previous turn's transient flag."""
    async with db_factory() as db:
        inst = Instance(name="reset-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    mock_proc = _make_mock_process()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    im._transient_seen.add(inst_id)  # pretend prior turn hit overload

    with patch("backend.services.instance_manager.asyncio.create_subprocess_exec",
               new_callable=AsyncMock, return_value=mock_proc):
        await im.launch(instance_id=inst_id, prompt="hi", cwd="/tmp")

    assert im.transient_error_seen(inst_id) is False
    await asyncio.sleep(0.1)


# === Reactivation guard (completed → executing) ===
# 复活块只认前台 turn 的活事件：orphan（PTY resume 回放）和 autonomous
# （后台子 agent turn）没有收尾路径，翻回 executing 后没人再标回 completed。


def _status_change_payloads(broadcaster):
    return [
        c.args[1]
        for c in broadcaster.broadcast.await_args_list
        if len(c.args) > 1 and isinstance(c.args[1], dict)
        and c.args[1].get("event") == "status_change"
    ]


async def _make_completed_task(db_factory, name):
    turn_started_at = datetime.utcnow()
    pid = 76001
    async with db_factory() as db:
        inst = Instance(
            name=name,
            status="running",
            pid=pid,
            started_at=turn_started_at,
        )
        task = Task(
            description="reactivation test",
            status="completed",
            retry_count=2,
            completed_at=datetime.utcnow(),
        )
        db.add_all([inst, task])
        await db.flush()
        task.instance_id = inst.id
        inst.current_task_id = task.id
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        return (
            inst.id,
            task.id,
            task.retry_count,
            inst.started_at,
            pid,
        )


def _arm_reactivation_generation(
    im,
    *,
    instance_id,
    task_id,
    retry_count,
    started_at,
    pid,
):
    process = MagicMock(pid=pid, returncode=None)
    consumer = asyncio.create_task(asyncio.Event().wait())
    im.processes[instance_id] = process
    record = im._track_output_consumer(
        instance_id,
        process,
        consumer,
        task_id=task_id,
        task_retry_count=retry_count,
        instance_started_at=started_at,
    )
    return process, consumer, record


@pytest.mark.asyncio
async def test_process_event_reactivates_completed_task(db_factory):
    """Foreground assistant output flips a completed task back to executing."""
    inst_id, task_id, retry_count, started_at, pid = (
        await _make_completed_task(db_factory, "react-fg")
    )
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    _process, consumer, _record = _arm_reactivation_generation(
        im,
        instance_id=inst_id,
        task_id=task_id,
        retry_count=retry_count,
        started_at=started_at,
        pid=pid,
    )

    try:
        await im._process_event(inst_id, task_id, {
            "event_type": "message",
            "role": "assistant",
            "content": "still working on the follow-up",
        })

        async with db_factory() as db:
            t = await db.get(Task, task_id)
            stored_log = (
                await db.execute(
                    select(LogEntry).where(LogEntry.task_id == task_id)
                )
            ).scalar_one()
            assert t.status == "executing"
            assert stored_log.task_retry_count == retry_count
        assert any(
            p.get("new_status") == "executing"
            for p in _status_change_payloads(broadcaster)
        )
        task_events = [
            call.args[1]
            for call in broadcaster.broadcast.await_args_list
            if call.args[0] == f"task:{task_id}"
            and call.args[1].get("event_type") == "message"
        ]
        assert task_events[0]["task_retry_count"] == retry_count
    finally:
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("flag", ["orphan", "autonomous"])
async def test_process_event_no_reactivate_on_stale_events(db_factory, flag):
    """orphan/autonomous events must NOT flip completed back to executing."""
    inst_id, task_id, retry_count, started_at, pid = (
        await _make_completed_task(db_factory, f"react-{flag}")
    )
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    _process, consumer, _record = _arm_reactivation_generation(
        im,
        instance_id=inst_id,
        task_id=task_id,
        retry_count=retry_count,
        started_at=started_at,
        pid=pid,
    )

    try:
        await im._process_event(inst_id, task_id, {
            "event_type": "message",
            "role": "assistant",
            "content": "replayed / background sub-agent output",
            flag: True,
        })

        async with db_factory() as db:
            t = await db.get(Task, task_id)
            assert t.status == "completed"
        assert _status_change_payloads(broadcaster) == []
    finally:
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)


@pytest.mark.asyncio
async def test_process_event_cannot_reactivate_across_routing_marker(
    db_factory,
):
    """Late old-route output cannot cross a durable routing stage fence."""

    inst_id, task_id, retry_count, started_at, pid = (
        await _make_completed_task(db_factory, "react-routing-fence")
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.metadata_ = {
            "worker_routing_config_pending": {
                "op_id": "standard-to-fast",
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "codex_service_tier": "priority",
            }
        }
        await db.commit()

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    _process, consumer, _record = _arm_reactivation_generation(
        im,
        instance_id=inst_id,
        task_id=task_id,
        retry_count=retry_count,
        started_at=started_at,
        pid=pid,
    )

    try:
        await im._process_event(inst_id, task_id, {
            "event_type": "message",
            "role": "assistant",
            "content": "late Standard output after Fast was staged",
        })

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            assert task.status == "completed"
            assert "worker_routing_config_pending" in task.metadata_
        assert _status_change_payloads(broadcaster) == []
    finally:
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)


@pytest.mark.asyncio
async def test_process_event_cannot_reactivate_superseded_pr_task(db_factory):
    """A live late event cannot bypass the durable PR supersede gate."""
    inst_id, task_id, retry_count, started_at, pid = (
        await _make_completed_task(db_factory, "react-superseded")
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.metadata_ = {"pr_review_superseded": True}
        await db.commit()

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    _process, consumer, _record = _arm_reactivation_generation(
        im,
        instance_id=inst_id,
        task_id=task_id,
        retry_count=retry_count,
        started_at=started_at,
        pid=pid,
    )

    try:
        await im._process_event(inst_id, task_id, {
            "event_type": "message",
            "role": "assistant",
            "content": "late output after synchronize",
        })

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            assert task.status == "completed"
            assert task.has_unread is False
        assert _status_change_payloads(broadcaster) == []
    finally:
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)


@pytest.mark.asyncio
async def test_process_event_cannot_borrow_replacement_consumer_generation(
    db_factory,
):
    """An old explicit consumer is dropped after the reusable slot changes."""
    inst_id, task_id, retry_count, started_at, pid = (
        await _make_completed_task(db_factory, "react-replaced")
    )
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)
    old_process, old_consumer, old_record = _arm_reactivation_generation(
        im,
        instance_id=inst_id,
        task_id=task_id,
        retry_count=retry_count,
        started_at=started_at,
        pid=pid,
    )
    new_process = MagicMock(pid=pid + 1, returncode=None)
    new_consumer = asyncio.create_task(asyncio.Event().wait())
    im.processes[inst_id] = new_process
    im._track_output_consumer(
        inst_id,
        new_process,
        new_consumer,
        task_id=task_id,
        task_retry_count=retry_count + 1,
        instance_started_at=started_at + timedelta(seconds=1),
    )

    try:
        await im._process_event(
            inst_id,
            task_id,
            {
                "event_type": "message",
                "role": "assistant",
                "content": "old generation output",
            },
            consumer_record=old_record,
        )

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            assert task.status == "completed"
        assert broadcaster.broadcast.await_count == 0
    finally:
        old_process.returncode = 0
        new_process.returncode = 0
        old_consumer.cancel()
        new_consumer.cancel()
        await asyncio.gather(
            old_consumer,
            new_consumer,
            return_exceptions=True,
        )


# === GPT-5.6 per-model effort in codex command ===


def test_build_command_codex_gpt56_passes_max_effort():
    # 旧代码把 max 一律丢弃（"codex 无 max"），但 gpt-5.6-sol 支持 max
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(provider="codex", prompt="hi", model="gpt-5.6-sol", resume_session_id=None, effort_level="max")
    assert 'model_reasoning_effort="max"' in cmd


def test_build_command_codex_gpt56_passes_ultra_effort():
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(provider="codex", prompt="hi", model="gpt-5.6-terra", resume_session_id=None, effort_level="ultra")
    assert 'model_reasoning_effort="ultra"' in cmd


def test_build_command_codex_old_model_clamps_max_to_xhigh():
    # gpt-5.5 不支持 max：夹到 xhigh 而不是静默丢弃
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(provider="codex", prompt="hi", model="gpt-5.5", resume_session_id=None, effort_level="max")
    assert 'model_reasoning_effort="xhigh"' in cmd


def test_build_command_codex_luna_clamps_ultra_to_max():
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(provider="codex", prompt="hi", model="gpt-5.6-luna", resume_session_id=None, effort_level="ultra")
    assert 'model_reasoning_effort="max"' in cmd


def test_build_command_codex_supported_effort_still_passed():
    im = InstanceManager(MagicMock(), MagicMock())
    cmd = im._build_command(provider="codex", prompt="hi", model="gpt-5.5", resume_session_id=None, effort_level="high")
    assert 'model_reasoning_effort="high"' in cmd


# ---------------------------------------------------------------------------
# Codex event parsing — reasoning / file_change / mcp_tool_call / web_search /
# todo_list / error item / turn.failed（字段名来自 codex-rs rust-v0.144.6
# exec/src/exec_events.rs 实证）
# ---------------------------------------------------------------------------

def test_parse_codex_reasoning_becomes_thinking():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {"id": "item_1", "type": "reasoning", "text": "Let me check the tests first."},
    }))

    assert event["event_type"] == "thinking"
    assert event["role"] == "assistant"
    assert event["content"] == "Let me check the tests first."


def test_parse_codex_empty_reasoning_skipped():
    im = InstanceManager(MagicMock(), MagicMock())
    assert im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {"id": "item_1", "type": "reasoning", "text": ""},
    })) is None


def test_parse_codex_file_change():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {
            "id": "item_2",
            "type": "file_change",
            "changes": [{"path": "src/app.py", "kind": "update"},
                        {"path": "src/new.py", "kind": "add"}],
            "status": "completed",
        },
    }))

    assert event["event_type"] == "tool_result"
    assert event["tool_name"] == "FileChange"
    assert "update src/app.py" in event["tool_output"]
    assert "add src/new.py" in event["tool_output"]
    assert event["is_error"] is False


def test_parse_codex_file_change_failed_is_error():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {"id": "i", "type": "file_change", "changes": [], "status": "failed"},
    }))
    assert event["is_error"] is True


def test_parse_codex_mcp_tool_call_started_and_completed():
    im = InstanceManager(MagicMock(), MagicMock())
    started = im._parse_codex_line(json.dumps({
        "type": "item.started",
        "item": {"id": "i", "type": "mcp_tool_call", "server": "ccm", "tool": "create_monitor",
                 "arguments": {"interval": 60}, "status": "in_progress"},
    }))
    assert started["event_type"] == "tool_use"
    assert started["tool_name"] == "ccm.create_monitor"
    assert json.loads(started["tool_input"]) == {"interval": 60}

    completed = im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {"id": "i", "type": "mcp_tool_call", "server": "ccm", "tool": "create_monitor",
                 "result": {"ok": True}, "status": "completed"},
    }))
    assert completed["event_type"] == "tool_result"
    assert completed["tool_name"] == "ccm.create_monitor"
    assert json.loads(completed["tool_output"]) == {"ok": True}
    assert completed["is_error"] is False


def test_parse_codex_mcp_tool_call_failed():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {"id": "i", "type": "mcp_tool_call", "server": "ccm", "tool": "x",
                 "error": {"message": "boom"}, "status": "failed"},
    }))
    assert event["event_type"] == "tool_result"
    assert event["is_error"] is True
    assert "boom" in event["tool_output"]


def test_parse_codex_web_search():
    im = InstanceManager(MagicMock(), MagicMock())
    started = im._parse_codex_line(json.dumps({
        "type": "item.started",
        "item": {"id": "i", "type": "web_search", "query": "fastapi websocket"},
    }))
    assert started["event_type"] == "tool_use"
    assert started["tool_name"] == "WebSearch"

    completed = im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {"id": "i", "type": "web_search", "query": "fastapi websocket"},
    }))
    assert completed["event_type"] == "tool_result"
    assert "fastapi websocket" in completed["tool_output"]


def test_parse_codex_todo_list():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.updated",
        "item": {"id": "i", "type": "todo_list",
                 "items": [{"text": "write tests", "completed": True},
                           {"text": "run tests", "completed": False}]},
    }))
    assert event["event_type"] == "system_event"
    assert "✓ write tests" in event["content"]
    assert "○ run tests" in event["content"]


def test_parse_codex_error_item():
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.completed",
        "item": {"id": "i", "type": "error", "message": "non-fatal oops"},
    }))
    assert event["event_type"] == "system_event"
    assert event["is_error"] is True
    assert event["content"] == "non-fatal oops"


def test_parse_codex_turn_failed_extracts_nested_message():
    # 实测形状（codex exec --json 认证失败捕获）：
    # {"type":"turn.failed","error":{"message":"..."}}
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "turn.failed",
        "error": {
            "message": "stream disconnected before completion: transport error",
            "codexErrorInfo": "contextWindowExceeded",
            "additionalDetails": "effective model window exhausted",
        },
    }))
    assert event["event_type"] == "system_event"
    assert event["is_error"] is True
    assert event["content"] == "stream disconnected before completion: transport error"
    assert event["error_code"] == "contextWindowExceeded"
    assert event["error_details"] == "effective model window exhausted"
    assert (
        im._fatal_provider_error_for_event(event)
        == "stream disconnected before completion: transport error"
    )


def test_parse_codex_file_change_started_is_tool_use():
    # 实测（CLI 0.144.6）file_change 也发 item.started——不映射会退化成
    # 一条 "in_progress" 噪音 system_event
    im = InstanceManager(MagicMock(), MagicMock())
    event = im._parse_codex_line(json.dumps({
        "type": "item.started",
        "item": {"id": "i", "type": "file_change",
                 "changes": [{"path": "probe.txt", "kind": "add"}],
                 "status": "in_progress"},
    }))
    assert event["event_type"] == "tool_use"
    assert event["tool_name"] == "FileChange"
    assert "probe.txt" in event["tool_input"]


@pytest.mark.asyncio
async def test_process_event_codex_window_backfill(db_factory):
    """codex 任务的 usage 不带 context_window → 按 codex 窗口表回填
    （gpt-5.6-terra = 272K，而不是 claude 的 200K 默认）。"""
    async with db_factory() as db:
        from backend.models.instance import Instance
        from backend.models.task import Task
        inst = Instance(name="codex-ctx-inst")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

        task = Task(title="codex ctx", provider="codex", model="gpt-5.6-terra")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    im = InstanceManager(db_factory, broadcaster)

    event = {
        "event_type": "system_event",
        "role": None,
        "content": "turn.completed",
        "tool_name": None,
        "tool_input": None,
        "tool_output": None,
        "raw_json": "{}",
        "is_error": False,
        "timestamp": "2024-01-01T00:00:00",
        "context_usage": {
            "input_tokens": 5000,
            "cache_read_input_tokens": 30000,
            "cache_creation_input_tokens": 0,
            "output_tokens": 100,
            "total_input_tokens": 35000,
            "context_tokens": 35_100,
        },
    }
    await im._process_event(inst_id, task_id, event)

    ctx_calls = [
        c for c in broadcaster.broadcast.call_args_list
        if c[0][0] == f"task:{task_id}" and c[0][1].get("event_type") == "context_usage"
    ]
    assert len(ctx_calls) == 1
    assert ctx_calls[0][0][1]["context_window"] == 272_000

    # 落库的 usage 也带正确窗口（dispatcher 压缩阈值读的就是它）
    async with db_factory() as db:
        from backend.models.task import Task
        t = await db.get(Task, task_id)
        assert t.context_window_usage["context_window"] == 272_000


@pytest.mark.asyncio
async def test_process_event_codex_exec_uses_rollout_last_usage(
    db_factory,
    tmp_path,
    monkeypatch,
):
    """Exec's cumulative turn usage must not become a 553% context reading."""

    rollout = tmp_path / "rollout-codex-thread-1.jsonl"
    rollout.write_text(json.dumps({
        "type": "event_msg",
        "payload": {
            "type": "token_count",
            "info": {
                "total_token_usage": {
                    "input_tokens": 1_505_114,
                    "cached_input_tokens": 1_300_000,
                    "output_tokens": 50_000,
                    "reasoning_output_tokens": 20_000,
                    "total_tokens": 1_555_114,
                },
                "last_token_usage": {
                    "input_tokens": 210_000,
                    "cached_input_tokens": 180_000,
                    "output_tokens": 8_000,
                    "reasoning_output_tokens": 2_000,
                    "total_tokens": 218_000,
                },
                "model_context_window": 258_400,
            },
        },
    }) + "\n")
    monkeypatch.setattr(
        "backend.api.tasks._find_session_jsonl",
        lambda _session_id, provider="claude": rollout,
    )

    async with db_factory() as db:
        inst = Instance(name="codex-exec-context")
        task = Task(
            title="codex exec context",
            provider="codex",
            model="gpt-5.6-terra",
            session_id="codex-thread-1",
        )
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id, task_id = inst.id, task.id

    manager = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    await manager._process_event(
        inst_id,
        task_id,
        {
            "event_type": "system_event",
            "content": "turn.completed",
            "is_error": False,
            "context_usage": {
                "input_tokens": 205_114,
                "cache_read_input_tokens": 1_300_000,
                "cache_creation_input_tokens": 0,
                "output_tokens": 50_000,
                "reasoning_output_tokens": 20_000,
                "total_input_tokens": 1_505_114,
                "total_tokens": 1_555_114,
                "context_tokens": 1_535_114,
            },
        },
    )

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        usage = current.context_window_usage
    assert usage["total_input_tokens"] == 210_000
    assert usage["context_tokens"] == 216_000
    assert usage["context_window"] == 258_400


@pytest.mark.asyncio
async def test_codex_context_window_failure_compacts_and_requeues(db_factory):
    """A structured app-server failure must continue via a compacted session."""

    started_at = datetime(2026, 7, 24, 9, 10, 11)
    async with db_factory() as db:
        instance = Instance(
            name="codex-context-full",
            status="running",
            pid=73_104,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="codex context full",
            provider="codex",
            status="executing",
            retry_count=0,
            instance_id=instance.id,
            session_id="codex-thread-1",
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    process = _make_mock_process(pid=73_104, returncode=1)
    output = iter((
        json.dumps({
            "type": "turn.failed",
            "error": {
                "message": "The request could not be completed.",
                "codexErrorInfo": "contextWindowExceeded",
            },
        }).encode() + b"\n",
        b"",
    ))

    async def readline():
        return next(output)

    process.stdout.readline = readline
    broadcaster = MagicMock(broadcast=AsyncMock())
    manager = InstanceManager(db_factory, broadcaster)
    manager.processes[instance_id] = process
    manager._launch_params[instance_id] = {
        "prompt": "[already wrapped history]\ncontinue the task",
        "current_message": "continue the task",
        "provider": "codex",
        "source_log_id": 321,
        "enabled_skills": {"sub-agent": True},
        "model": "gpt-5.6-terra",
    }

    dispatcher = MagicMock()
    dispatcher._compact_session = AsyncMock(return_value="durable summary")
    dispatcher.enqueue_message = AsyncMock()

    with patch("backend.main.dispatcher", dispatcher):
        consumer = asyncio.create_task(
            manager._consume_output(
                instance_id,
                task_id,
                process,
                chat_initiated=True,
                provider="codex",
            )
        )
        manager._track_output_consumer(
            instance_id,
            process,
            consumer,
            chat_initiated=True,
            provider="codex",
            task_id=task_id,
            task_retry_count=0,
            instance_started_at=started_at,
        )
        await consumer

    dispatcher._compact_session.assert_awaited_once()
    assert (
        dispatcher._compact_session.await_args.kwargs[
            "exclude_log_entry_id"
        ]
        == 321
    )
    assert (
        dispatcher._compact_session.await_args.kwargs[
            "post_source_injects_are_current"
        ]
        is True
    )
    dispatcher.enqueue_message.assert_awaited_once()
    retry = dispatcher.enqueue_message.await_args.kwargs
    assert retry["source"] == "compact_retry"
    assert retry["source_log_id"] == 321
    assert retry["current_message"] == "continue the task"
    assert retry["command_skills"] == {"sub-agent": True}
    assert retry["model_override"] == "gpt-5.6-terra"
    assert "durable summary" in retry["prompt"]
    assert "continue the task" in retry["prompt"]
    assert "already wrapped history" not in retry["prompt"]


@pytest.mark.asyncio
async def test_recent_failure_output_keeps_structured_codex_error(db_factory):
    async with db_factory() as db:
        task = Task(title="structured failure", provider="codex")
        db.add(task)
        await db.flush()
        db.add(LogEntry(
            task_id=task.id,
            event_type="system_event",
            content="The request could not be completed.",
            raw_json=json.dumps({
                "type": "turn.failed",
                "error": {
                    "message": "The request could not be completed.",
                    "codexErrorInfo": "contextWindowExceeded",
                },
            }),
            is_error=True,
        ))
        await db.commit()
        task_id = task.id

    manager = InstanceManager(db_factory, MagicMock())
    recent = await manager.get_recent_log_contents(task_id)

    assert len(recent) == 1
    assert "The request could not be completed." in recent[0]
    assert "contextWindowExceeded" in recent[0]
