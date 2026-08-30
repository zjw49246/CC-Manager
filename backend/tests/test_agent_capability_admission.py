"""Atomic admission tests for exact model-requested capabilities."""

from __future__ import annotations

import asyncio
from datetime import datetime
import json
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from backend.config import settings
from backend.models.capability import (
    CapabilityExecution,
    CapabilityInvocation,
    CapabilityResumeOutbox,
)
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.services.agent_capability_admission import (
    AgentTerminalExpectation,
    admit_agent_terminal_action,
    publish_agent_terminal_admission_locked,
)
from backend.services.capability_service import capability_task_lock
from backend.services.capability_protocol import (
    TERMINAL_ACTION_CLOSE_TAG,
    TERMINAL_ACTION_OPEN_TAG,
)
from backend.services.capability_registry import (
    CapabilityDefinition,
    register_capability,
    unregister_capability,
)
from backend.services.terminal_arbitration import bind_turn_source


POLICY = {
    "version": 1,
    "max_invocations": 2,
    "capabilities": {"plan": 1, "code_review": 1},
}


@pytest.fixture(autouse=True)
def agent_capability_runtime(monkeypatch):
    monkeypatch.setattr(settings, "capability_core_enabled", True)
    monkeypatch.setattr(settings, "auto_capability_enabled", True)
    unregister_capability("plan")
    unregister_capability("code_review")
    for capability in ("plan", "code_review"):
        register_capability(
            CapabilityDefinition(
                capability_key=capability,
                executor_kind=f"fake_{capability}",
                executor_config={"route": "test"},
                policy_snapshot={"local_only": True},
                max_attempts=2,
            )
        )
    yield
    unregister_capability("plan")
    unregister_capability("code_review")


def _terminal_action(
    *,
    capability: str = "plan",
    request: object | None = None,
    reason: str = "Need a plan",
) -> str:
    payload = json.dumps(
        {
            "schema_version": 1,
            "terminal_action": "request_capability",
            "capability": capability,
            "reason": reason,
            "request": (
                {"prompt": "Produce a safe rollout plan"}
                if request is None
                else request
            ),
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"Yielding control.\n{TERMINAL_ACTION_OPEN_TAG}{payload}{TERMINAL_ACTION_CLOSE_TAG}"


async def _claude_turn(
    db,
    *,
    content: str,
    turn_generation: int = 3,
) -> tuple[Task, AgentTerminalExpectation, LogEntry]:
    task = Task(
        title="Agent capability",
        description="Implement safely",
        status="executing",
        mode="auto",
        retry_count=2,
        turn_generation=turn_generation,
        instance_id=17,
        provider="claude",
        session_id="session-1",
        capability_policy=POLICY,
    )
    db.add(task)
    await db.flush()
    source = await bind_turn_source(
        db,
        task,
        None,
        instance_id=17,
    )
    source.actual_transport = "claude_exec"
    output = LogEntry(
        instance_id=17,
        task_id=task.id,
        task_retry_count=task.retry_count,
        task_turn_generation=task.turn_generation,
        turn_scope="foreground",
        event_type="result",
        role=None,
        content=content,
        is_error=False,
    )
    db.add(output)
    await db.commit()
    return (
        task,
        AgentTerminalExpectation(
            task_id=task.id,
            task_incarnation_id=task.incarnation_id,
            retry_count=task.retry_count,
            turn_generation=task.turn_generation,
            instance_id=task.instance_id,
            source_log_id=source.id,
        ),
        output,
    )


@pytest.mark.asyncio
async def test_agent_request_atomically_creates_ledger_and_waiting_outbox(db_session):
    task, expected, output = await _claude_turn(
        db_session,
        content=_terminal_action(),
    )

    admitted = await admit_agent_terminal_action(db_session, expected=expected)

    assert admitted.outcome == "waiting_capability"
    assert admitted.created is True
    stored_task = await db_session.get(Task, task.id, populate_existing=True)
    invocation = await db_session.get(
        CapabilityInvocation,
        admitted.invocation_id,
    )
    outbox = await db_session.get(CapabilityResumeOutbox, admitted.outbox_id)
    execution = await db_session.scalar(
        select(CapabilityExecution).where(
            CapabilityExecution.invocation_id == invocation.id
        )
    )

    assert stored_task.status == "waiting_capability"
    assert invocation.source == "agent_request"
    assert invocation.status == "queued"
    assert invocation.request_task_incarnation_id == task.incarnation_id
    assert invocation.request_task_turn_generation == task.turn_generation
    assert invocation.request_source_log_id == expected.source_log_id
    assert invocation.request_output_log_id == output.id
    assert invocation.request_terminal_log_id == output.id
    assert invocation.request_output_hash is not None
    assert len(invocation.request_output_hash) == 64
    assert execution.status == "queued"
    assert outbox.status == "pending"
    assert outbox.active_task_id == task.id
    assert outbox.from_turn_generation == task.turn_generation
    assert outbox.request_output_log_id == output.id
    assert outbox.request_terminal_log_id == output.id


@pytest.mark.asyncio
async def test_created_event_fence_rejects_cancelled_generation(
    db_session,
    monkeypatch,
):
    event = AsyncMock()
    monkeypatch.setattr(
        "backend.services.capability_events.broadcast_capability_event",
        event,
    )
    task, expected, _output = await _claude_turn(
        db_session,
        content=_terminal_action(),
    )
    admitted = await admit_agent_terminal_action(db_session, expected=expected)
    event.reset_mock()

    stored = await db_session.get(Task, task.id, populate_existing=True)
    stored.status = "cancelled"
    stored.completed_at = datetime.utcnow()
    await db_session.commit()

    async with capability_task_lock(task.id):
        published = await publish_agent_terminal_admission_locked(
            db_session,
            admitted,
        )
        await db_session.rollback()

    assert published is False
    event.assert_not_awaited()


@pytest.mark.asyncio
async def test_same_exact_output_replays_without_refunding_or_double_counting(
    db_session,
):
    _task, expected, _output = await _claude_turn(
        db_session,
        content=_terminal_action(),
    )
    first = await admit_agent_terminal_action(db_session, expected=expected)
    replay = await admit_agent_terminal_action(db_session, expected=expected)

    assert first.created is True
    assert replay.created is False
    assert replay.invocation_id == first.invocation_id
    assert replay.outbox_id == first.outbox_id
    assert await db_session.scalar(
        select(func.count(CapabilityInvocation.id))
    ) == 1
    assert await db_session.scalar(
        select(func.count(CapabilityExecution.id))
    ) == 1
    assert await db_session.scalar(
        select(func.count(CapabilityResumeOutbox.id))
    ) == 1


@pytest.mark.asyncio
async def test_concurrent_same_output_admission_has_one_budget_ledger(
    db_session,
    db_factory,
):
    _task, expected, _output = await _claude_turn(
        db_session,
        content=_terminal_action(),
    )

    async def admit():
        async with db_factory() as db:
            return await admit_agent_terminal_action(db, expected=expected)

    first, second = await asyncio.gather(admit(), admit())

    assert {first.created, second.created} == {False, True}
    assert first.invocation_id == second.invocation_id
    assert first.outbox_id == second.outbox_id
    assert await db_session.scalar(
        select(func.count(CapabilityInvocation.id))
    ) == 1


@pytest.mark.asyncio
async def test_ordinary_terminal_output_keeps_existing_completion_path(db_session):
    task, expected, output = await _claude_turn(
        db_session,
        content="Implemented and verified.",
    )

    admitted = await admit_agent_terminal_action(db_session, expected=expected)

    assert admitted.outcome == "ordinary_completion"
    assert admitted.output_log_id == output.id
    stored = await db_session.get(Task, task.id, populate_existing=True)
    assert stored.status == "executing"
    assert await db_session.scalar(
        select(func.count(CapabilityInvocation.id))
    ) == 0


@pytest.mark.asyncio
async def test_malformed_terminal_marker_fails_closed_without_invocation(db_session):
    task, expected, _output = await _claude_turn(
        db_session,
        content=(
            "<ccm_terminal_action>{not-json}</ccm_terminal_action>"
        ),
    )

    admitted = await admit_agent_terminal_action(db_session, expected=expected)

    assert admitted.outcome == "protocol_failed"
    assert admitted.error_code == "invalid_json"
    stored = await db_session.get(Task, task.id, populate_existing=True)
    assert stored.status == "failed"
    assert "invalid_json" in stored.error_message
    assert await db_session.scalar(
        select(func.count(CapabilityInvocation.id))
    ) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "request_payload",
    [
        {},
        {"title": "Missing prompt"},
        {"request": "legacy Plan request alias"},
        {"prompt": "Plan this", "unknown": True},
        {"prompt": 42},
        {"prompt": "Plan this", "title": 42},
        {"prompt": ""},
        {"prompt": "  \n\t"},
    ],
    ids=(
        "empty-object",
        "missing-prompt",
        "legacy-request-alias",
        "unknown-field",
        "wrong-prompt-type",
        "wrong-title-type",
        "empty-prompt",
        "blank-prompt",
    ),
)
async def test_plan_request_schema_is_rejected_at_terminal_admission(
    db_session,
    request_payload,
):
    task, expected, _output = await _claude_turn(
        db_session,
        content=_terminal_action(request=request_payload),
    )

    admitted = await admit_agent_terminal_action(db_session, expected=expected)

    assert admitted.outcome == "protocol_failed"
    assert admitted.error_code == "invalid_capability_request"
    stored = await db_session.get(Task, task.id, populate_existing=True)
    assert stored.status == "failed"
    assert await db_session.scalar(
        select(func.count(CapabilityInvocation.id))
    ) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "request_payload",
    [
        {},
        {"base_sha": "a" * 40},
        {"head_sha": "b" * 40},
        {"base_sha": "a" * 40, "head_sha": "b" * 40, "path": "src"},
        {"base_sha": 1, "head_sha": "b" * 40},
        {"base_sha": "a" * 40, "head_sha": None},
        {"base_sha": "a" * 39, "head_sha": "b" * 40},
        {"base_sha": "a" * 40, "head_sha": "b" * 41},
        {"base_sha": "g" * 40, "head_sha": "b" * 40},
        {"base_sha": "A" * 40, "head_sha": "b" * 40},
        {"base_sha": " " * 40, "head_sha": "b" * 40},
    ],
    ids=(
        "empty-object",
        "missing-head",
        "missing-base",
        "unknown-field",
        "wrong-base-type",
        "wrong-head-type",
        "abbreviated-base",
        "oversized-head",
        "non-hex-base",
        "uppercase-base",
        "blank-base",
    ),
)
async def test_code_review_request_schema_is_rejected_at_terminal_admission(
    db_session,
    request_payload,
):
    task, expected, _output = await _claude_turn(
        db_session,
        content=_terminal_action(
            capability="code_review",
            request=request_payload,
            reason="Need an immutable commit-range review",
        ),
    )

    admitted = await admit_agent_terminal_action(db_session, expected=expected)

    assert admitted.outcome == "protocol_failed"
    assert admitted.error_code == "invalid_capability_request"
    stored = await db_session.get(Task, task.id, populate_existing=True)
    assert stored.status == "failed"
    assert await db_session.scalar(
        select(func.count(CapabilityInvocation.id))
    ) == 0


@pytest.mark.asyncio
async def test_terminal_request_fails_if_rollout_was_disabled_mid_turn(
    db_session,
    monkeypatch,
):
    task, expected, _output = await _claude_turn(
        db_session,
        content=_terminal_action(),
    )
    monkeypatch.setattr(settings, "auto_capability_enabled", False)

    admitted = await admit_agent_terminal_action(db_session, expected=expected)

    assert admitted.outcome == "protocol_failed"
    assert admitted.error_code == "auto_capability_disabled"
    stored = await db_session.get(Task, task.id, populate_existing=True)
    assert stored.status == "failed"


@pytest.mark.asyncio
async def test_failed_agent_invocation_still_consumes_non_refundable_budget(
    db_session,
):
    task, expected, _output = await _claude_turn(
        db_session,
        content=_terminal_action(),
    )
    first = await admit_agent_terminal_action(db_session, expected=expected)
    invocation = await db_session.get(CapabilityInvocation, first.invocation_id)
    execution = await db_session.scalar(
        select(CapabilityExecution).where(
            CapabilityExecution.invocation_id == invocation.id
        )
    )
    outbox = await db_session.get(CapabilityResumeOutbox, first.outbox_id)
    now = datetime.utcnow()
    execution.status = "failed"
    execution.state_version += 1
    execution.active_invocation_id = None
    execution.error_code = "executor_failed"
    execution.error_message = "failed"
    execution.completed_at = now
    invocation.status = "failed"
    invocation.state_version += 1
    invocation.active_task_id = None
    invocation.error_code = "executor_failed"
    invocation.error_message = "failed"
    invocation.completed_at = now
    outbox.status = "cancelled"
    outbox.state_version += 1
    outbox.active_task_id = None
    outbox.active_invocation_id = None
    outbox.error_code = "test_cancelled"
    outbox.error_message = "settled for next generation"
    outbox.completed_at = now
    task.status = "executing"
    task.turn_generation += 1
    task.turn_source_log_id = None
    task.completed_at = None
    await db_session.flush()
    source = await bind_turn_source(
        db_session,
        task,
        None,
        instance_id=task.instance_id,
    )
    source.actual_transport = "claude_exec"
    output = LogEntry(
        instance_id=task.instance_id,
        task_id=task.id,
        task_retry_count=task.retry_count,
        task_turn_generation=task.turn_generation,
        turn_scope="foreground",
        event_type="result",
        role=None,
        content=_terminal_action(reason="Try again"),
        is_error=False,
    )
    db_session.add(output)
    await db_session.commit()
    next_expected = AgentTerminalExpectation(
        task_id=task.id,
        task_incarnation_id=task.incarnation_id,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
        instance_id=task.instance_id,
        source_log_id=source.id,
    )

    rejected = await admit_agent_terminal_action(
        db_session,
        expected=next_expected,
    )

    assert rejected.outcome == "protocol_failed"
    assert rejected.error_code == "capability_budget_exhausted"
    assert await db_session.scalar(
        select(func.count(CapabilityInvocation.id))
    ) == 1


@pytest.mark.asyncio
async def test_later_failed_terminal_vetoes_older_action_output(db_session):
    task, expected, output = await _claude_turn(
        db_session,
        content=_terminal_action(),
    )
    db_session.add(LogEntry(
        instance_id=task.instance_id,
        task_id=task.id,
        task_retry_count=task.retry_count,
        task_turn_generation=task.turn_generation,
        turn_scope="foreground",
        event_type="result",
        role=None,
        content="provider failed after output",
        is_error=True,
    ))
    await db_session.commit()

    rejected = await admit_agent_terminal_action(db_session, expected=expected)

    assert rejected.outcome == "protocol_failed"
    assert rejected.error_code == "terminal_output_unproven"
    assert rejected.output_log_id is None
    assert await db_session.scalar(
        select(func.count(CapabilityInvocation.id))
    ) == 0


@pytest.mark.asyncio
async def test_codex_persists_separate_output_and_terminal_envelope(db_session):
    task = Task(
        title="Codex capability",
        description="Review",
        status="executing",
        mode="auto",
        retry_count=0,
        turn_generation=1,
        instance_id=23,
        provider="codex",
        session_id="thread-1",
        capability_policy=POLICY,
    )
    db_session.add(task)
    await db_session.flush()
    source = await bind_turn_source(
        db_session,
        task,
        None,
        instance_id=23,
    )
    source.actual_transport = "codex_app_server"
    native_turn_id = "turn-native-1"
    output = LogEntry(
        instance_id=23,
        task_id=task.id,
        task_retry_count=0,
        task_turn_generation=1,
        native_turn_id=native_turn_id,
        turn_scope="foreground",
        event_type="message",
        role="assistant",
        content=_terminal_action(),
        raw_json=json.dumps({
            "type": "item.completed",
            "turn_id": native_turn_id,
            "item": {
                "id": "msg-1",
                "type": "agent_message",
                "text": _terminal_action(),
            },
        }),
        is_error=False,
    )
    terminal = LogEntry(
        instance_id=23,
        task_id=task.id,
        task_retry_count=0,
        task_turn_generation=1,
        native_turn_id=native_turn_id,
        turn_scope="foreground",
        event_type="system_event",
        role=None,
        content="turn.completed",
        raw_json=json.dumps({
            "type": "turn.completed",
            "turn_id": native_turn_id,
            "status": "completed",
            "success": True,
            "error": None,
        }),
        is_error=False,
    )
    db_session.add_all([output, terminal])
    await db_session.commit()
    expected = AgentTerminalExpectation(
        task_id=task.id,
        task_incarnation_id=task.incarnation_id,
        retry_count=0,
        turn_generation=1,
        instance_id=23,
        source_log_id=source.id,
    )

    admitted = await admit_agent_terminal_action(db_session, expected=expected)
    invocation = await db_session.get(
        CapabilityInvocation,
        admitted.invocation_id,
    )

    assert admitted.outcome == "waiting_capability"
    assert invocation.request_output_log_id == output.id
    assert invocation.request_terminal_log_id == terminal.id
    assert invocation.request_native_turn_id == native_turn_id


@pytest.mark.asyncio
async def test_stale_task_generation_cannot_admit_terminal_action(db_session):
    task, expected, _output = await _claude_turn(
        db_session,
        content=_terminal_action(),
    )
    task.turn_generation += 1
    await db_session.commit()

    admitted = await admit_agent_terminal_action(db_session, expected=expected)

    assert admitted.outcome == "stale"
    assert await db_session.scalar(
        select(func.count(CapabilityInvocation.id))
    ) == 0
