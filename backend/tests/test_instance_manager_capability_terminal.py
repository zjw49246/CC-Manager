"""Provider-terminal wiring for model-requested Capabilities."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import func, select

from backend.config import settings
from backend.models.capability import (
    CapabilityExecution,
    CapabilityInvocation,
    CapabilityResumeOutbox,
)
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.services.agent_capability_admission import (
    AgentTerminalExpectation,
    admit_agent_terminal_action,
)
from backend.services.capability_protocol import (
    TERMINAL_ACTION_CLOSE_TAG,
    TERMINAL_ACTION_OPEN_TAG,
)
from backend.services.capability_registry import (
    CapabilityDefinition,
    register_capability,
    unregister_capability,
)
from backend.services.capability_resume import (
    claim_resume_publication,
    claim_resume_turn_locked,
    materialize_resume_outbox,
)
from backend.services.capability_service import capability_task_lock
from backend.services.instance_manager import (
    ConsumerRecoveryUnsettledError,
    InstanceManager,
)
from backend.services.terminal_arbitration import bind_turn_source


POLICY = {
    "version": 1,
    "max_invocations": 2,
    "capabilities": {"plan": 2},
}
_RESULT_HASH = "d" * 64


@pytest.fixture(autouse=True)
def capability_runtime(monkeypatch):
    monkeypatch.setattr(settings, "capability_core_enabled", True)
    monkeypatch.setattr(settings, "auto_capability_enabled", True)
    unregister_capability("plan")
    register_capability(
        CapabilityDefinition(
            capability_key="plan",
            executor_kind="fake_plan",
            executor_config={"route": "terminal-test"},
            policy_snapshot={"local_only": True},
            max_attempts=2,
        )
    )
    yield
    unregister_capability("plan")


def _terminal_action(*, reason: str) -> str:
    payload = json.dumps(
        {
            "schema_version": 1,
            "terminal_action": "request_capability",
            "capability": "plan",
            "reason": reason,
            "request": {"prompt": reason},
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    return (
        f"Yielding control.\n{TERMINAL_ACTION_OPEN_TAG}"
        f"{payload}{TERMINAL_ACTION_CLOSE_TAG}"
    )


def _process(*, pid: int, returncode: int):
    process = MagicMock()
    process.pid = pid
    process.returncode = returncode

    async def readline():
        return b""

    async def read_stderr():
        return b""

    process.stdout.readline = readline
    process.stderr.read = read_stderr
    process.wait = AsyncMock(return_value=returncode)
    process.wait_runtime_cleanup = AsyncMock(return_value=None)
    return process


async def _terminal_scope(
    db_factory,
    *,
    transport: str,
    reason: str,
    provider: str = "claude",
    generation: int = 3,
    pid: int = 81_001,
):
    started_at = datetime(2026, 8, 7, 13, 0, generation)
    async with db_factory() as db:
        instance = Instance(
            name=f"terminal-{transport}-{generation}",
            provider=provider,
            status="running",
            pid=pid,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="Auto terminal capability",
            description="request exact guidance",
            status="executing",
            mode="auto",
            provider=provider,
            model=(
                "gpt-5.6-sol"
                if provider == "codex"
                else "claude-opus-4-6"
            ),
            retry_count=2,
            turn_generation=generation,
            instance_id=instance.id,
            started_at=started_at,
            session_id="terminal-session",
            capability_policy=POLICY,
        )
        db.add(task)
        await db.flush()
        source = await bind_turn_source(
            db,
            task,
            None,
            instance_id=instance.id,
            transport=transport,
        )
        source.actual_transport = transport
        action = _terminal_action(reason=reason)
        if provider == "codex":
            native_turn_id = f"native-{task.id}-{generation}"
            output = LogEntry(
                instance_id=instance.id,
                task_id=task.id,
                task_retry_count=task.retry_count,
                task_turn_generation=task.turn_generation,
                native_turn_id=native_turn_id,
                turn_scope="foreground",
                event_type="message",
                role="assistant",
                content=action,
                raw_json=json.dumps(
                    {
                        "type": "item.completed",
                        "turn_id": native_turn_id,
                        "item": {
                            "id": f"message-{task.id}-{generation}",
                            "type": "agent_message",
                            "text": action,
                        },
                    }
                ),
                is_error=False,
            )
            terminal = LogEntry(
                instance_id=instance.id,
                task_id=task.id,
                task_retry_count=task.retry_count,
                task_turn_generation=task.turn_generation,
                native_turn_id=native_turn_id,
                turn_scope="foreground",
                event_type="system_event",
                role=None,
                content="turn.completed",
                raw_json=json.dumps(
                    {
                        "type": "turn.completed",
                        "turn_id": native_turn_id,
                        "status": "completed",
                        "success": True,
                        "error": None,
                    }
                ),
                is_error=False,
            )
            db.add_all((output, terminal))
        else:
            output = LogEntry(
                instance_id=instance.id,
                task_id=task.id,
                task_retry_count=task.retry_count,
                task_turn_generation=task.turn_generation,
                turn_scope="foreground",
                event_type="result",
                role="assistant",
                content=action,
                raw_json="{}",
                is_error=False,
            )
            db.add(output)
        instance.current_task_id = task.id
        await db.commit()
        return SimpleNamespace(
            task_id=task.id,
            instance_id=instance.id,
            source_id=source.id,
            output_id=output.id,
            generation=generation,
            retry_count=task.retry_count,
            started_at=started_at,
            pid=pid,
            provider=provider,
        )


async def _run_direct_terminal(
    manager: InstanceManager,
    scope,
    *,
    returncode: int,
):
    process = _process(pid=scope.pid, returncode=returncode)
    manager.processes[scope.instance_id] = process
    consumer = asyncio.create_task(
        manager._consume_output(
            scope.instance_id,
            scope.task_id,
            process,
            chat_initiated=True,
            provider=scope.provider,
        )
    )
    manager._track_output_consumer(
        scope.instance_id,
        process,
        consumer,
        chat_initiated=True,
        provider=scope.provider,
        task_id=scope.task_id,
        task_retry_count=scope.retry_count,
        task_turn_generation=scope.generation,
        instance_started_at=scope.started_at,
    )
    await asyncio.wait_for(consumer, timeout=2)


def _failing_harness_service(calls: list[tuple[tuple, dict]]):
    @asynccontextmanager
    async def owner_stop_fence(*args, **kwargs):
        calls.append((args, kwargs))
        raise RuntimeError("Harness cleanup could not be proven")
        yield  # pragma: no cover

    return SimpleNamespace(owner_stop_fence=owner_stop_fence)


async def _prepare_claimed_resume(db_factory, monkeypatch):
    first = await _terminal_scope(
        db_factory,
        transport="claude_exec",
        reason="first capability",
        generation=7,
        pid=81_107,
    )
    event = AsyncMock()
    monkeypatch.setattr(
        "backend.services.capability_events.broadcast_capability_event",
        event,
    )
    async with db_factory() as db:
        task = await db.get(Task, first.task_id)
        admitted = await admit_agent_terminal_action(
            db,
            expected=AgentTerminalExpectation(
                task_id=task.id,
                task_incarnation_id=task.incarnation_id,
                retry_count=task.retry_count,
                turn_generation=task.turn_generation,
                instance_id=task.instance_id,
                source_log_id=task.turn_source_log_id,
            ),
        )
    assert admitted.outcome == "waiting_capability"

    now = datetime.utcnow()
    async with db_factory() as db:
        invocation = await db.get(CapabilityInvocation, admitted.invocation_id)
        execution = await db.scalar(
            select(CapabilityExecution).where(
                CapabilityExecution.invocation_id == invocation.id
            )
        )
        instance = await db.get(Instance, first.instance_id)
        invocation.status = "ready"
        invocation.state_version += 1
        invocation.result_kind = "plan_version"
        invocation.result_id = 101
        invocation.result_hash = _RESULT_HASH
        invocation.ready_at = now
        invocation.updated_at = now
        execution.status = "completed"
        execution.state_version += 1
        execution.active_invocation_id = None
        execution.output_kind = "plan_version"
        execution.output_id = 101
        execution.output_hash = _RESULT_HASH
        execution.completed_at = now
        instance.status = "idle"
        instance.pid = None
        instance.current_task_id = None
        await db.commit()

    async def resolve(_db, _invocation):
        return SimpleNamespace(
            execution_id=execution.id,
            kind="plan_version",
            id=101,
            hash=_RESULT_HASH,
            resource_url="/api/plan-versions/101",
            data={"id": 101, "content": "verified plan"},
        )

    monkeypatch.setattr(
        "backend.services.capability_result.resolve_capability_result",
        resolve,
    )
    async with db_factory() as db:
        await materialize_resume_outbox(db, admitted.outbox_id)
    async with db_factory() as db:
        envelope = await claim_resume_publication(
            db,
            admitted.outbox_id,
            lease_seconds=120,
        )
    assert envelope is not None and envelope.lease_token is not None

    async with capability_task_lock(first.task_id):
        async with db_factory() as db:
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == first.task_id)
                    .with_for_update()
                )
            ).scalar_one()
            claim = await claim_resume_turn_locked(
                db,
                task=task,
                outbox_id=admitted.outbox_id,
                lease_token=envelope.lease_token,
                instance_id=first.instance_id,
                transport="claude_exec",
            )
            await db.commit()

    resumed_at = datetime(2026, 8, 7, 13, 1, 8)
    async with db_factory() as db:
        task = await db.get(Task, first.task_id)
        source = await db.get(LogEntry, claim.source_log_id)
        instance = await db.get(Instance, first.instance_id)
        source.actual_transport = "claude_exec"
        task.started_at = resumed_at
        instance.status = "running"
        instance.pid = 81_108
        instance.current_task_id = task.id
        instance.started_at = resumed_at
        await db.commit()
    return SimpleNamespace(
        task_id=first.task_id,
        instance_id=first.instance_id,
        source_id=claim.source_log_id,
        generation=claim.turn_generation,
        retry_count=claim.retry_count,
        started_at=resumed_at,
        pid=81_108,
        provider="claude",
        old_invocation_id=admitted.invocation_id,
        old_outbox_id=admitted.outbox_id,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "transport", "pty"),
    [
        ("claude", "claude_exec", False),
        ("claude", "claude_pty", True),
        ("codex", "codex_exec", False),
    ],
    ids=("claude-exec", "claude-pty", "codex-exec"),
)
async def test_successful_auto_terminal_atomically_yields_to_capability(
    db_factory,
    monkeypatch,
    provider,
    transport,
    pty,
):
    scope = await _terminal_scope(
        db_factory,
        transport=transport,
        reason=f"request through {transport}",
        provider=provider,
    )
    capability_event = AsyncMock()
    monkeypatch.setattr(
        "backend.services.capability_events.broadcast_capability_event",
        capability_event,
    )
    manager = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    if provider == "codex":
        manager._try_proactive_pool_switch = AsyncMock(return_value=False)

    if pty:
        process = _process(pid=scope.pid, returncode=0)
        manager.processes[scope.instance_id] = process
        record = manager._track_output_consumer(
            scope.instance_id,
            process,
            asyncio.current_task(),
            chat_initiated=True,
            provider="claude",
            task_id=scope.task_id,
            task_retry_count=scope.retry_count,
            task_turn_generation=scope.generation,
            instance_started_at=scope.started_at,
        )
        status = await manager.finalize_pty_chat_generation(
            scope.instance_id,
            scope.task_id,
            0,
            record,
        )
        assert status == "waiting_capability"
    else:
        await _run_direct_terminal(manager, scope, returncode=0)

    async with db_factory() as db:
        task = await db.get(Task, scope.task_id)
        instance = await db.get(Instance, scope.instance_id)
        invocation = await db.scalar(
            select(CapabilityInvocation).where(
                CapabilityInvocation.task_id == scope.task_id
            )
        )
        execution = await db.scalar(
            select(CapabilityExecution).where(
                CapabilityExecution.invocation_id == invocation.id
            )
        )
        outbox = await db.scalar(
            select(CapabilityResumeOutbox).where(
                CapabilityResumeOutbox.invocation_id == invocation.id
            )
        )
        assert task.status == "waiting_capability"
        assert task.completed_at is None
        assert instance.status == "idle"
        assert instance.pid is None
        assert instance.current_task_id is None
        assert invocation.status == "queued"
        assert execution.status == "queued"
        assert outbox.status == "pending"
    capability_event.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_progress_pty_interrupt_fails_and_clears_native_session(
    db_factory,
):
    scope = await _terminal_scope(
        db_factory,
        transport="claude_pty",
        reason="must not execute after a no-progress loop",
        generation=4,
        pid=81_004,
    )
    manager = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    process = _process(pid=scope.pid, returncode=130)
    manager.processes[scope.instance_id] = process
    record = manager._track_output_consumer(
        scope.instance_id,
        process,
        asyncio.current_task(),
        chat_initiated=True,
        provider="claude",
        task_id=scope.task_id,
        task_retry_count=scope.retry_count,
        task_turn_generation=scope.generation,
        instance_started_at=scope.started_at,
    )
    object.__setattr__(
        record,
        "fatal_provider_error",
        "Claude response made no progress: test reproduction",
    )

    status = await manager.finalize_pty_chat_generation(
        scope.instance_id,
        scope.task_id,
        130,
        record,
    )

    assert status == "failed"
    async with db_factory() as db:
        task = await db.get(Task, scope.task_id)
        instance = await db.get(Instance, scope.instance_id)
        assert task.status == "failed"
        assert task.session_id is None
        assert task.context_window_usage is None
        assert "made no progress" in task.error_message
        assert instance.status == "error"
        assert instance.pid is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_resumed_generation_settles_then_requests_next_capability(
    db_factory,
    monkeypatch,
):
    resumed = await _prepare_claimed_resume(db_factory, monkeypatch)
    async with db_factory() as db:
        db.add(
            LogEntry(
                instance_id=resumed.instance_id,
                task_id=resumed.task_id,
                task_retry_count=resumed.retry_count,
                task_turn_generation=resumed.generation,
                turn_scope="foreground",
                event_type="result",
                role="assistant",
                content=_terminal_action(reason="second capability"),
                raw_json="{}",
                is_error=False,
            )
        )
        await db.commit()

    manager = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    await _run_direct_terminal(manager, resumed, returncode=0)

    async with db_factory() as db:
        task = await db.get(Task, resumed.task_id)
        instance = await db.get(Instance, resumed.instance_id)
        old_invocation = await db.get(
            CapabilityInvocation,
            resumed.old_invocation_id,
        )
        old_outbox = await db.get(
            CapabilityResumeOutbox,
            resumed.old_outbox_id,
        )
        invocations = list(
            (
                await db.scalars(
                    select(CapabilityInvocation)
                    .where(CapabilityInvocation.task_id == resumed.task_id)
                    .order_by(CapabilityInvocation.id)
                )
            ).all()
        )
        new_invocation = invocations[-1]
        new_outbox = await db.scalar(
            select(CapabilityResumeOutbox).where(
                CapabilityResumeOutbox.invocation_id == new_invocation.id
            )
        )
        assert task.status == "waiting_capability"
        assert instance.status == "idle"
        assert old_invocation.status == "completed"
        assert old_invocation.active_task_id is None
        assert old_outbox.status == "completed"
        assert old_outbox.active_task_id is None
        assert len(invocations) == 2
        assert new_invocation.id != old_invocation.id
        assert new_invocation.status == "queued"
        assert new_invocation.request_task_turn_generation == resumed.generation
        assert new_outbox.status == "pending"
        assert new_outbox.active_task_id == resumed.task_id


@pytest.mark.asyncio
async def test_provider_failure_settles_resume_without_new_admission(
    db_factory,
    monkeypatch,
):
    resumed = await _prepare_claimed_resume(db_factory, monkeypatch)
    manager = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))

    await _run_direct_terminal(manager, resumed, returncode=7)

    async with db_factory() as db:
        task = await db.get(Task, resumed.task_id)
        instance = await db.get(Instance, resumed.instance_id)
        invocation = await db.get(
            CapabilityInvocation,
            resumed.old_invocation_id,
        )
        outbox = await db.get(
            CapabilityResumeOutbox,
            resumed.old_outbox_id,
        )
        count = await db.scalar(
            select(func.count(CapabilityInvocation.id)).where(
                CapabilityInvocation.task_id == resumed.task_id
            )
        )
        assert task.status == "failed"
        assert instance.status == "error"
        assert invocation.status == "completed"
        assert outbox.status == "completed"
        assert count == 1


@pytest.mark.asyncio
async def test_user_interrupt_completes_without_parsing_terminal_action(
    db_factory,
    monkeypatch,
):
    scope = await _terminal_scope(
        db_factory,
        transport="claude_exec",
        reason="must not run after interrupt",
        generation=9,
        pid=81_109,
    )
    event = AsyncMock()
    monkeypatch.setattr(
        "backend.services.capability_events.broadcast_capability_event",
        event,
    )
    manager = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))

    await _run_direct_terminal(manager, scope, returncode=-2)

    async with db_factory() as db:
        task = await db.get(Task, scope.task_id)
        count = await db.scalar(
            select(func.count(CapabilityInvocation.id)).where(
                CapabilityInvocation.task_id == scope.task_id
            )
        )
        assert task.status == "completed"
        assert count == 0
    event.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pty", "returncode"),
    [(False, 0), (False, 7), (True, 0), (True, 7)],
    ids=("direct-success-capability", "direct-failure", "pty-success-capability", "pty-failure"),
)
async def test_chat_terminal_preserves_owner_when_harness_cleanup_fails(
    db_factory,
    pty,
    returncode,
):
    scope = await _terminal_scope(
        db_factory,
        transport="claude_pty" if pty else "claude_exec",
        reason="must remain active when child cleanup fails",
        generation=12 + returncode + int(pty),
        pid=81_200 + returncode + int(pty),
    )
    fence_calls: list[tuple[tuple, dict]] = []
    manager = InstanceManager(
        db_factory,
        MagicMock(broadcast=AsyncMock()),
        test_harness_service=_failing_harness_service(fence_calls),
    )

    if pty:
        process = _process(pid=scope.pid, returncode=returncode)
        manager.processes[scope.instance_id] = process
        record = manager._track_output_consumer(
            scope.instance_id,
            process,
            asyncio.current_task(),
            chat_initiated=True,
            provider="claude",
            task_id=scope.task_id,
            task_retry_count=scope.retry_count,
            task_turn_generation=scope.generation,
            instance_started_at=scope.started_at,
        )
        with pytest.raises(
            RuntimeError,
            match="Harness cleanup could not be proven",
        ):
            await manager.finalize_pty_chat_generation(
                scope.instance_id,
                scope.task_id,
                returncode,
                record,
            )
    else:
        with pytest.raises(ConsumerRecoveryUnsettledError):
            await _run_direct_terminal(
                manager,
                scope,
                returncode=returncode,
            )

    async with db_factory() as db:
        task = await db.get(Task, scope.task_id)
        instance = await db.get(Instance, scope.instance_id)
        invocation_count = await db.scalar(
            select(func.count(CapabilityInvocation.id)).where(
                CapabilityInvocation.task_id == scope.task_id
            )
        )
        assert task.status == "executing"
        assert task.completed_at is None
        assert instance.status == "running"
        assert instance.pid == scope.pid
        assert instance.current_task_id == scope.task_id
        assert invocation_count == 0
    assert fence_calls


@pytest.mark.asyncio
async def test_pty_handoff_during_commit_withdraws_unpublished_admission(
    db_factory,
    monkeypatch,
):
    scope = await _terminal_scope(
        db_factory,
        transport="claude_pty",
        reason="defer while background work is active",
        generation=10,
        pid=81_110,
    )
    session_id = "terminal-session"
    injected = False
    manager = None

    @asynccontextmanager
    async def racing_factory():
        async with db_factory() as db:
            class SessionProxy:
                def __getattr__(self, name):
                    return getattr(db, name)

                async def commit(self):
                    nonlocal injected
                    await db.commit()
                    if not injected:
                        injected = True
                        manager.note_pty_autonomous_activity(
                            scope.task_id,
                            session_id,
                        )

            yield SessionProxy()

    capability_event = AsyncMock()
    monkeypatch.setattr(
        "backend.services.capability_events.broadcast_capability_event",
        capability_event,
    )
    manager = InstanceManager(
        racing_factory,
        MagicMock(broadcast=AsyncMock()),
    )

    class Session:
        def __init__(self):
            self.session_id = session_id

    process = _process(pid=scope.pid, returncode=0)
    process.session = Session()
    manager.processes[scope.instance_id] = process
    record = manager._track_output_consumer(
        scope.instance_id,
        process,
        asyncio.current_task(),
        chat_initiated=True,
        provider="claude",
        task_id=scope.task_id,
        task_retry_count=scope.retry_count,
        task_turn_generation=scope.generation,
        instance_started_at=scope.started_at,
    )

    status = await manager.finalize_pty_chat_generation(
        scope.instance_id,
        scope.task_id,
        0,
        record,
        background_session_id=session_id,
    )

    assert status == "background_armed"
    assert injected is True
    async with db_factory() as db:
        task = await db.get(Task, scope.task_id)
        instance = await db.get(Instance, scope.instance_id)
        invocation_count = await db.scalar(
            select(func.count(CapabilityInvocation.id)).where(
                CapabilityInvocation.task_id == scope.task_id
            )
        )
        outbox_count = await db.scalar(
            select(func.count(CapabilityResumeOutbox.id)).where(
                CapabilityResumeOutbox.task_id == scope.task_id
            )
        )
        assert task.status == "executing"
        assert task.pty_background_generation is not None
        assert instance.status == "running"
        assert instance.pid == scope.pid
        assert instance.current_task_id == scope.task_id
        assert invocation_count == 0
        assert outbox_count == 0
    capability_event.assert_not_awaited()

    state = manager._pty_background_state_for_task(scope.task_id)
    assert state is not None
    state.outcome = "superseded"
    manager._discard_pty_background_state(
        (scope.task_id, session_id),
        state.generation,
    )


@pytest.mark.asyncio
async def test_terminal_lock_order_starts_with_harness_then_capability_then_lifecycle(
    db_factory,
    monkeypatch,
):
    events: list[str] = []

    @asynccontextmanager
    async def capability_lock(_task_id):
        events.append("capability-enter")
        try:
            yield
        finally:
            events.append("capability-exit")

    class LifecycleLock:
        async def __aenter__(self):
            events.append("lifecycle-enter")

        async def __aexit__(self, *_args):
            events.append("lifecycle-exit")

    @asynccontextmanager
    async def harness_fence(*_args, **_kwargs):
        events.append("harness-enter")
        try:
            yield True
        finally:
            events.append("harness-exit")

    monkeypatch.setattr(
        "backend.services.capability_service.capability_task_lock",
        capability_lock,
    )
    manager = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    monkeypatch.setattr(
        manager,
        "_instance_lifecycle_lock",
        lambda _instance_id: LifecycleLock(),
    )
    monkeypatch.setattr(
        manager,
        "_test_harness_owner_terminal_context",
        harness_fence,
    )

    async with manager._chat_terminal_locks(
        1,
        2,
        expected_retry_count=0,
        expected_turn_generation=1,
        reason="test terminal ordering",
    ) as fenced:
        assert fenced is True
        events.append("body")

    assert events == [
        "harness-enter",
        "capability-enter",
        "lifecycle-enter",
        "body",
        "lifecycle-exit",
        "capability-exit",
        "harness-exit",
    ]
