"""Tests for stale state cleanup, zombie worker prevention, and orphan task handling.

Covers dispatcher ownership recovery and stale-state cleanup:
- Unowned persisted PIDs are quarantined without signalling unknown processes
- Manager-owned in-process generations survive Pause -> Start
- Unowned task claims return to pending for safe retry
- Safety-net instance/task reset after lifecycle ends
- Instance.current_task_id cleanup on task deletion
- Orphaned task handling on stop-session
- Interrupted task status change (pending → completed)
"""
import asyncio
import hashlib
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.capability import (
    CapabilityExecution,
    CapabilityInvocation,
    CapabilityResumeOutbox,
)
from backend.models.instance import Instance
from backend.models.task import Task
from backend.models.log_entry import LogEntry
from backend.models.plan import PlanApplicationReceipt
from backend.models.sub_agent import SubAgentSession
from backend.models.worker_turn_handoff import WorkerTurnHandoffReceipt
from backend.services.dispatcher import (
    GlobalDispatcher,
    _TaskLifecycleGeneration,
)
from backend.services.capability_resume import (
    claim_resume_publication,
    claim_resume_turn_locked,
    materialize_resume_outbox,
)
from backend.services.capability_service import capability_task_lock
from backend.services.task_queue import TaskQueue


# === Helpers ===

def _make_dispatcher(db_factory):
    """Create a GlobalDispatcher with mocked dependencies."""
    instance_manager = MagicMock()
    instance_manager.launch = AsyncMock(return_value=12345)
    # Lifecycle completion now waits for the output consumer to finish its
    # final persistence/account-routing work before deciding the task status.
    instance_manager.wait_for_output_consumer = AsyncMock()
    instance_manager.processes = {}
    instance_manager._tasks = {}
    instance_manager.pty_mode_enabled = False
    instance_manager.transient_error_seen = MagicMock(return_value=False)
    instance_manager.get_last_stderr = MagicMock(return_value="")
    instance_manager.get_recent_log_contents = AsyncMock(return_value=[])
    # PTY proactive pool switch path (dispatcher._run_task_lifecycle)
    instance_manager.pty_rate_limit_seen = MagicMock(return_value=False)
    instance_manager._try_proactive_pool_switch = AsyncMock()
    instance_manager._pty_rate_limit_seen = set()
    instance_manager.active_codex_task_ids = MagicMock(return_value=frozenset())
    instance_manager.active_codex_transport_pids = MagicMock(
        return_value=frozenset()
    )

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()

    return GlobalDispatcher(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )


async def _lifecycle_generation(dispatcher, db_factory, task_id):
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task is not None
        return dispatcher._task_lifecycle_generation(task)


async def _seed_claimed_capability_resume(
    db_factory,
    *,
    persisted_pid: int | None,
    generation: int = 7,
):
    """Create one exact executing G+1 resume with a persisted reverse owner."""

    digest = "d" * 64
    async with db_factory() as db:
        task = Task(
            title="Capability restart recovery",
            description="continue after capability",
            status="waiting_capability",
            mode="auto",
            provider="claude",
            model="claude-opus-4-6",
            codex_service_tier="default",
            retry_count=2,
            turn_generation=generation,
            session_id="session-restart-recovery",
        )
        db.add(task)
        await db.flush()
        assert task.incarnation_id is not None

        instance = Instance(
            name="capability-resume-owner",
            status="running",
            pid=persisted_pid,
        )
        db.add(instance)
        await db.flush()

        request_source = LogEntry(
            instance_id=instance.id,
            task_id=task.id,
            task_retry_count=task.retry_count,
            task_turn_generation=generation,
            turn_scope="source",
            actual_transport="claude_exec",
            event_type="user_message",
            role="user",
            content="original request",
            is_error=False,
        )
        output_text = "requesting capability guidance"
        request_output = LogEntry(
            instance_id=instance.id,
            task_id=task.id,
            task_retry_count=task.retry_count,
            task_turn_generation=generation,
            turn_scope="foreground",
            event_type="result",
            role="assistant",
            content=output_text,
            is_error=False,
        )
        db.add_all((request_source, request_output))
        await db.flush()
        task.turn_source_log_id = request_source.id

        invocation = CapabilityInvocation(
            task_id=task.id,
            capability_key="plan",
            source="agent_request",
            purpose="advisory",
            status="failed",
            state_version=1,
            idempotency_key=f"restart-{task.id}-{generation}",
            input_payload={"prompt": "give safe guidance"},
            input_hash=digest,
            subject_kind="task_generation",
            subject_ref={"task_id": task.id, "generation": generation},
            subject_hash=digest,
            executor_kind="plan",
            executor_config={},
            executor_config_hash=digest,
            policy_snapshot={"allowed": True},
            policy_hash=digest,
            resume_policy="resume_task",
            max_attempts=1,
            request_task_incarnation_id=task.incarnation_id,
            request_task_retry_count=task.retry_count,
            request_task_instance_id=instance.id,
            request_task_session_id=task.session_id,
            request_task_turn_generation=generation,
            request_source_log_id=request_source.id,
            request_output_log_id=request_output.id,
            request_terminal_log_id=request_output.id,
            request_reason="Need exact guidance",
            request_protocol_version=1,
            request_output_hash=hashlib.sha256(
                output_text.encode("utf-8")
            ).hexdigest(),
            error_code="capability_failed",
            error_message="capability failed",
            completed_at=datetime.utcnow(),
        )
        db.add(invocation)
        await db.flush()
        execution = CapabilityExecution(
            invocation_id=invocation.id,
            attempt=1,
            status="failed",
            state_version=1,
            idempotency_key=f"restart-execution-{task.id}-{generation}",
            executor_kind="plan",
            input_hash=digest,
            error_code="execution_failed",
            error_message="execution failed",
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
        )
        outbox = CapabilityResumeOutbox(
            task_id=task.id,
            invocation_id=invocation.id,
            active_task_id=task.id,
            active_invocation_id=invocation.id,
            status="pending",
            request_task_incarnation_id=task.incarnation_id,
            request_task_retry_count=task.retry_count,
            from_turn_generation=generation,
            request_task_session_id=task.session_id,
            request_source_log_id=request_source.id,
            request_output_log_id=request_output.id,
            request_terminal_log_id=request_output.id,
        )
        db.add_all((execution, outbox))
        await db.commit()
        task_id = task.id
        instance_id = instance.id
        outbox_id = outbox.id

    async with db_factory() as db:
        await materialize_resume_outbox(db, outbox_id)
    async with db_factory() as db:
        envelope = await claim_resume_publication(db, outbox_id)
    assert envelope is not None and envelope.lease_token is not None

    async with capability_task_lock(task_id):
        async with db_factory() as db:
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == task_id)
                    .with_for_update()
                )
            ).scalar_one()
            claim = await claim_resume_turn_locked(
                db,
                task=task,
                outbox_id=outbox_id,
                lease_token=envelope.lease_token,
                instance_id=instance_id,
                transport="claude_exec",
            )
            instance = await db.get(Instance, instance_id)
            instance.current_task_id = task_id
            await db.commit()

    return {
        "task_id": task_id,
        "instance_id": instance_id,
        "outbox_id": outbox_id,
        "source_log_id": claim.source_log_id,
        "turn_generation": claim.turn_generation,
    }


def _worker_handoff_payload_digest(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


async def _seed_claimed_worker_handoff(
    db_factory,
    *,
    handoff_id: str,
    actual_transport: str | None,
    include_queue_principal: bool = True,
    include_source_principal: bool = True,
):
    """Persist one exact claimed G+1 with only a forward Instance pointer."""

    retry_count = 3
    from_generation = 5
    claimed_generation = from_generation + 1
    message = "continue exact Worker turn"
    request_payload = {
        "message": message,
        "worker_turn_handoff_id": handoff_id,
        "worker_turn_handoff_retry_count": retry_count,
        "worker_turn_handoff_from_generation": from_generation,
        "execution_user_id": None,
        "execution_user_role": "member",
        "execution_mode": "sandbox",
        "execution_principal_kind": "system",
    }
    source_principal = {
        "user_id": None,
        "role": "member",
        "mode": "sandbox",
        "kind": "system",
    }
    async with db_factory() as db:
        instance = Instance(name="worker-pre-spawn", status="idle")
        db.add(instance)
        await db.flush()
        task = Task(
            title="claimed Worker turn",
            description="old task description",
            status="executing",
            retry_count=retry_count,
            turn_generation=claimed_generation,
            instance_id=instance.id,
            execution_user_id=None,
            execution_user_role="member",
            execution_mode="sandbox",
            execution_principal_kind="system",
        )
        db.add(task)
        await db.flush()
        request_payload["worker_turn_handoff_incarnation_id"] = (
            task.incarnation_id
        )
        source = LogEntry(
            task_id=task.id,
            task_retry_count=retry_count,
            task_turn_generation=claimed_generation,
            turn_scope="source",
            actual_transport=actual_transport,
            event_type="user_message",
            role="user",
            content=message,
            is_error=False,
            raw_json=(
                json.dumps({"execution_principal": source_principal})
                if include_source_principal
                else None
            ),
        )
        db.add(source)
        await db.flush()
        task.turn_source_log_id = source.id
        queue_payload = {
            "prompt": message,
            "priority": 0,
            "source": "user",
            "user_message_text": None,
            "command_skills": None,
            "model_override": None,
            "expected_task_routing": ["claude", None, "default"],
            "source_log_id": source.id,
            "current_message": message,
            "queue_timestamp": 1234.5,
            "allow_new_session": False,
            "delivery_key": None,
            "worker_turn_handoff_id": handoff_id,
            "worker_turn_handoff_retry_count": retry_count,
            "worker_turn_handoff_from_generation": from_generation,
            "worker_turn_handoff_incarnation_id": task.incarnation_id,
        }
        if include_queue_principal:
            queue_payload.update({
                "initiating_user_id": None,
                "initiating_user_role": "member",
                "execution_mode": "sandbox",
                "execution_principal_kind": "system",
            })
        db.add(
            WorkerTurnHandoffReceipt(
                handoff_id=handoff_id,
                task_id=task.id,
                source_log_id=source.id,
                side="worker",
                worker_id=None,
                retry_count=retry_count,
                from_generation=from_generation,
                status="claimed",
                request_payload=request_payload,
                request_digest=_worker_handoff_payload_digest(request_payload),
                queue_payload=queue_payload,
                queue_payload_digest=_worker_handoff_payload_digest(queue_payload),
                response={"ok": True, "queued": True},
                claimed_turn_generation=claimed_generation,
            )
        )
        await db.commit()
        return {
            "task_id": task.id,
            "instance_id": instance.id,
            "source_log_id": source.id,
            "handoff_id": handoff_id,
            "retry_count": retry_count,
            "from_generation": from_generation,
            "claimed_generation": claimed_generation,
        }


# === _cleanup_stale_state tests ===


@pytest.mark.asyncio
async def test_maintenance_reconciliation_requires_paused_admission(db_factory):
    d = _make_dispatcher(db_factory)
    d._cleanup_stale_state = AsyncMock()

    with pytest.raises(RuntimeError, match="paused task admission"):
        await d.reconcile_stale_state_for_maintenance()

    await d.pause_dispatching()
    await d.reconcile_stale_state_for_maintenance()

    d._cleanup_stale_state.assert_awaited_once_with(
        reconcile_auxiliary=False
    )


@pytest.mark.asyncio
async def test_maintenance_reconcile_preserves_live_auxiliary_rows(db_factory):
    """Manual reconciliation is not a startup sweep of sub-agent sessions."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        rows = [
            SubAgentSession(
                task_id=101,
                description="ccm monitor",
                agent_type="monitor",
                source="ccm",
                status="running",
            ),
            SubAgentSession(
                task_id=102,
                description="native agent",
                agent_type="native-agent",
                source="native",
                status="running",
            ),
            SubAgentSession(
                task_id=103,
                description="native monitor",
                agent_type="native-monitor",
                source="native",
                status="running",
            ),
        ]
        db.add_all(rows)
        await db.commit()
        row_ids = [row.id for row in rows]

    await d.pause_dispatching()
    try:
        await d.reconcile_stale_state_for_maintenance()
    finally:
        d.resume_dispatching()

    async with db_factory() as db:
        statuses = [
            (await db.get(SubAgentSession, row_id)).status
            for row_id in row_ids
        ]
    assert statuses == ["running", "running", "running"]


@pytest.mark.asyncio
async def test_startup_cleanup_uses_exact_auxiliary_ownership(db_factory):
    """Stop -> Start preserves live CCM/native rows and clears only stale ones."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        parent = Task(
            title="native parent",
            description="d",
            status="executing",
        )
        db.add(parent)
        await db.flush()
        instance = Instance(
            name="native owner",
            status="running",
            pid=43210,
            current_task_id=parent.id,
        )
        db.add(instance)
        await db.flush()
        parent.instance_id = instance.id
        rows = [
            SubAgentSession(
                task_id=parent.id,
                description="live ccm monitor",
                agent_type="monitor",
                source="ccm",
                status="running",
            ),
            SubAgentSession(
                task_id=parent.id,
                description="live ccm sub-agent",
                agent_type="sub_agent",
                source="ccm",
                status="running",
            ),
            SubAgentSession(
                task_id=parent.id,
                description="live native",
                agent_type="native-agent",
                source="native",
                status="running",
            ),
            SubAgentSession(
                task_id=parent.id,
                description="legacy native monitor",
                agent_type="monitor",
                source="native",
                status="running",
            ),
            SubAgentSession(
                task_id=999,
                description="stale local",
                agent_type="monitor",
                source="ccm",
                status="running",
            ),
            SubAgentSession(
                task_id=parent.id,
                description="recoverable scheduled monitor",
                agent_type="monitor",
                source="ccm",
                status="running",
                next_check_at=datetime.utcnow(),
            ),
            SubAgentSession(
                task_id=parent.id,
                description="uncertain active monitor",
                agent_type="monitor",
                source="ccm",
                status="running",
                turn_generation=3,
                active_turn_generation=3,
            ),
            SubAgentSession(
                task_id=998,
                remote_id=88,
                description="remote mirror",
                agent_type="monitor",
                source="ccm",
                status="running",
            ),
        ]
        db.add_all(rows)
        await db.commit()
        instance_id = instance.id
        row_ids = [row.id for row in rows]

    d.instance_manager.processes[instance_id] = MagicMock(
        returncode=None
    )
    monitor_lifecycle = asyncio.create_task(asyncio.sleep(60))
    d._monitor_tasks[row_ids[0]] = monitor_lifecycle
    d._sub_agent_processes[row_ids[1]] = MagicMock(returncode=None)
    try:
        await d._cleanup_stale_state()
    finally:
        monitor_lifecycle.cancel()
        await asyncio.gather(
            monitor_lifecycle, return_exceptions=True
        )

    async with db_factory() as db:
        statuses = [
            (await db.get(SubAgentSession, row_id)).status
            for row_id in row_ids
        ]
    assert statuses == [
        "running",
        "running",
        "running",
        "running",
        "failed",
        "running",
        "failed",
        "running",
    ]
    async with db_factory() as db:
        uncertain = await db.get(SubAgentSession, row_ids[6])
    assert "could not be recovered" in uncertain.last_error


@pytest.mark.asyncio
async def test_cleanup_resets_dead_pid_instance(db_factory):
    """An unowned persisted PID is quarantined instead of treated as attachable."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="zombie-worker", status="running", pid=999999, current_task_id=42)
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "error"
        assert inst.pid is None
        assert inst.current_task_id is None


@pytest.mark.asyncio
async def test_cleanup_replays_exact_pre_provider_capability_resume(db_factory):
    """A dead pre-provider G+1 claim returns to waiting without creating G+2."""

    seed = await _seed_claimed_capability_resume(
        db_factory,
        persisted_pid=999999,
    )
    d = _make_dispatcher(db_factory)

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, seed["task_id"])
        instance = await db.get(Instance, seed["instance_id"])
        outbox = await db.get(CapabilityResumeOutbox, seed["outbox_id"])
        source = await db.get(LogEntry, seed["source_log_id"])

        assert task.status == "waiting_capability"
        assert task.instance_id is None
        assert task.turn_generation == seed["turn_generation"] == 8
        assert task.turn_source_log_id == seed["source_log_id"]
        assert source.actual_transport is None
        assert outbox.status == "claimed"
        assert outbox.claimed_turn_generation == seed["turn_generation"]
        assert outbox.resume_source_log_id == seed["source_log_id"]
        assert outbox.lease_token is None
        assert outbox.error_code == "resume_restart_replay"
        assert instance.status == "error"
        assert instance.pid is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_cleanup_fails_capability_resume_after_provider_boundary(
    db_factory,
):
    """A dead provider-bound G+1 is failed closed and never republished."""

    seed = await _seed_claimed_capability_resume(
        db_factory,
        persisted_pid=999999,
    )
    async with db_factory() as db:
        source = await db.get(LogEntry, seed["source_log_id"])
        source.actual_transport = "claude_exec"
        await db.commit()
    d = _make_dispatcher(db_factory)

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, seed["task_id"])
        instance = await db.get(Instance, seed["instance_id"])
        outbox = await db.get(CapabilityResumeOutbox, seed["outbox_id"])

        assert task.status == "failed"
        assert task.instance_id is None
        assert "provider boundary" in task.error_message
        assert outbox.status == "failed"
        assert outbox.resume_actual_transport == "claude_exec"
        assert outbox.error_code == "resume_runtime_lost_after_launch"
        assert instance.status == "error"
        assert instance.pid is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_cleanup_retains_unmanaged_capability_resume_owner_evidence(
    db_factory,
):
    """An unknown live PID blocks replay and retains both ownership links."""

    seed = await _seed_claimed_capability_resume(
        db_factory,
        persisted_pid=os.getpid(),
    )
    d = _make_dispatcher(db_factory)

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, seed["task_id"])
        instance = await db.get(Instance, seed["instance_id"])
        outbox = await db.get(CapabilityResumeOutbox, seed["outbox_id"])

        assert task.status == "failed"
        assert task.instance_id == seed["instance_id"]
        assert "Unmanaged process PID" in task.error_message
        assert outbox.status == "failed"
        assert outbox.error_code == "resume_unmanaged_runtime"
        assert instance.status == "error"
        assert instance.pid == os.getpid()
        assert instance.current_task_id == seed["task_id"]


@pytest.mark.asyncio
async def test_cleanup_preserves_manager_owned_live_generation(db_factory):
    """Pause -> Start preserves a process/consumer owned by this manager."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(
            title="live-task",
            description="test",
            status="executing",
        )
        db.add(task)
        await db.flush()
        inst = Instance(
            name="alive-worker",
            status="running",
            pid=43210,
            current_task_id=task.id,
        )
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        task.instance_id = inst.id
        await db.commit()
        inst_id = inst.id
        task_id = task.id

    d.instance_manager.processes[inst_id] = MagicMock(returncode=None)

    await d._cleanup_stale_state()

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "running"
        assert inst.pid == 43210
        task = await db.get(Task, task_id)
        assert task.status == "executing"
        assert task.instance_id == inst_id


@pytest.mark.asyncio
async def test_cleanup_preserves_terminal_consumer_recovery_evidence(db_factory):
    """A reaped generation awaiting DB retry remains manager-owned."""
    d = _make_dispatcher(db_factory)
    d.instance_manager.is_running = MagicMock(return_value=True)

    async with db_factory() as db:
        task = Task(
            title="terminal-recovery-task",
            description="test",
            status="executing",
        )
        db.add(task)
        await db.flush()
        inst = Instance(
            name="terminal-recovery-worker",
            status="running",
            pid=43211,
            current_task_id=task.id,
        )
        db.add(inst)
        await db.flush()
        task.instance_id = inst.id
        await db.commit()
        inst_id = inst.id
        task_id = task.id

    process = MagicMock(returncode=130, pid=43211)
    d.instance_manager._consumer_recovery_pending = {
        (inst_id, process): MagicMock(tracked_generation=True),
    }

    await d._cleanup_stale_state()

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        task = await db.get(Task, task_id)
        assert inst.status == "running"
        assert inst.pid == 43211
        assert inst.current_task_id == task_id
        assert task.status == "executing"
        assert task.instance_id == inst_id


@pytest.mark.asyncio
async def test_cleanup_preserves_live_codex_registry_task_generation(db_factory):
    """A live native Codex turn owns its shared-transport Instance row."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(
            title="live-codex-task",
            description="test",
            status="executing",
            provider="codex",
        )
        db.add(task)
        await db.flush()
        inst = Instance(
            name="codex-worker",
            status="running",
            pid=os.getpid(),
            current_task_id=task.id,
        )
        db.add(inst)
        await db.flush()
        task.instance_id = inst.id
        await db.commit()
        inst_id = inst.id
        task_id = task.id

    d.instance_manager.active_codex_task_ids.return_value = frozenset({task_id})

    await d._cleanup_stale_state()

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        task = await db.get(Task, task_id)
        assert inst.status == "running"
        assert inst.pid == os.getpid()
        assert inst.current_task_id == task_id
        assert task.status == "executing"
        assert task.instance_id == inst_id


@pytest.mark.asyncio
async def test_cleanup_does_not_treat_shared_codex_transport_as_task_pid(db_factory):
    """A live shared transport is not evidence that a stale Task still runs."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(
            title="stale-codex-task",
            description="test",
            status="executing",
            provider="codex",
        )
        db.add(task)
        await db.flush()
        inst = Instance(
            name="stale-codex-worker",
            status="running",
            pid=os.getpid(),
            current_task_id=task.id,
        )
        db.add(inst)
        await db.flush()
        task.instance_id = inst.id
        await db.commit()
        inst_id = inst.id
        task_id = task.id

    d.instance_manager.active_codex_transport_pids.return_value = frozenset(
        {os.getpid()}
    )

    await d._cleanup_stale_state()

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        task = await db.get(Task, task_id)
        assert inst.status == "error"
        assert inst.pid is None
        assert inst.current_task_id is None
        assert task.status == "failed"
        assert task.instance_id is None
        assert "Unmanaged process PID" not in (task.error_message or "")


@pytest.mark.asyncio
async def test_cleanup_preserves_prelaunch_lifecycle_claim(db_factory):
    """A paused lifecycle may own a slot before InstanceManager maps a process."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        inst = Instance(name="prelaunch", status="idle")
        db.add(inst)
        await db.flush()
        task = Task(
            title="prelaunch",
            description="d",
            status="executing",
            instance_id=inst.id,
        )
        db.add(task)
        await db.commit()
        inst_id, task_id = inst.id, task.id

    lifecycle = asyncio.create_task(asyncio.sleep(60))
    d._running_tasks[inst_id] = lifecycle
    try:
        await d._cleanup_stale_state()
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            assert task.status == "executing"
            assert task.instance_id == inst_id
    finally:
        lifecycle.cancel()
        await asyncio.gather(lifecycle, return_exceptions=True)


@pytest.mark.asyncio
async def test_cleanup_preserves_reserved_fresh_task_claim(db_factory):
    """Maintenance cannot recover a claim still in project/config preparation."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(name="reserved-prelaunch", status="idle")
        task = Task(
            title="reserved-prelaunch",
            description="d",
            status="pending",
        )
        db.add_all([instance, task])
        await db.commit()
        instance_id, task_id = instance.id, task.id

    claim_token = None
    try:
        async with db_factory() as db:
            reserved, claim_token = await d._reserve_idle_instance(
                db, instance_id=instance_id
            )
            assert reserved is not None
            claimed = await TaskQueue(db).dequeue(instance_id=instance_id)
            assert claimed is not None
            assert claimed.id == task_id

        await d.pause_dispatching()
        await d.reconcile_stale_state_for_maintenance()

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            assert task.status == "in_progress"
            assert task.instance_id == instance_id
    finally:
        if claim_token is not None:
            await d._release_instance_reservation(
                instance_id, claim_token
            )
        d.resume_dispatching()


@pytest.mark.asyncio
async def test_cleanup_does_not_rewrite_remote_shared_shadow(db_factory):
    """Shared task lifecycle is remote-authoritative, never locally recovered."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        shadow = Task(
            title="remote shadow",
            description="d",
            status="executing",
            shared_from_id=987654,
        )
        db.add(shadow)
        await db.commit()
        shadow_id = shadow.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        shadow = await db.get(Task, shadow_id)
        assert shadow.status == "executing"
        assert shadow.instance_id is None


@pytest.mark.asyncio
async def test_cleanup_fail_closes_unowned_pid_that_may_be_alive(db_factory):
    """Unknown live PID is never auto-retried, which could duplicate writes."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(title="orphan", description="d", status="executing")
        db.add(task)
        await db.flush()
        inst = Instance(
            name="unknown-live",
            status="running",
            pid=os.getpid(),
            current_task_id=task.id,
        )
        db.add(inst)
        await db.flush()
        task.instance_id = inst.id
        await db.commit()
        task_id, inst_id = task.id, inst.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "error"
        assert inst.pid == os.getpid()
        assert inst.current_task_id == task_id
        task = await db.get(Task, task_id)
        assert task.status == "failed"
        assert task.instance_id == inst_id
        assert "duplicate execution" in task.error_message


@pytest.mark.asyncio
async def test_cleanup_quarantines_idle_row_with_live_orphan_pid(db_factory):
    """``idle`` cannot make a persisted live generation dispatchable."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(title="dirty idle", description="d", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="dirty-idle-owner",
            status="idle",
            pid=os.getpid(),
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id, instance_id = task.id, instance.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        task = await db.get(Task, task_id)
        assert instance.status == "error"
        assert instance.pid == os.getpid()
        assert instance.current_task_id == task_id
        assert task.status == "failed"
        assert task.instance_id == instance_id


@pytest.mark.asyncio
async def test_idle_reservation_refuses_orphan_evidence(db_factory):
    """Admission independently rejects dirty idle PID/owner fields."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(
            name="dirty-idle",
            status="idle",
            pid=os.getpid(),
            current_task_id=987654,
        )
        db.add(instance)
        await db.commit()

    async with db_factory() as db:
        assert await d._reserve_idle_instance(db) == (None, None)


@pytest.mark.asyncio
async def test_cleanup_generation_cas_preserves_concurrent_replacement(db_factory):
    """A generation changed after SELECT wins; stale cleanup touches neither owner."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        old_task = Task(title="old owner", description="d", status="executing")
        new_task = Task(title="new owner", description="d", status="executing")
        db.add_all([old_task, new_task])
        await db.flush()
        instance = Instance(
            name="owner-race",
            status="idle",
            pid=os.getpid(),
            current_task_id=old_task.id,
        )
        db.add(instance)
        await db.flush()
        old_task.instance_id = instance.id
        new_task.instance_id = instance.id
        await db.commit()
        instance_id = instance.id
        old_task_id, new_task_id = old_task.id, new_task.id

    original_execute = AsyncSession.execute
    injected = False

    async def execute_with_owner_race(session, statement, *args, **kwargs):
        nonlocal injected
        table = getattr(statement, "table", None)
        if not injected and getattr(table, "name", None) == "instances":
            injected = True
            await original_execute(
                session,
                update(Instance)
                .where(Instance.id == instance_id)
                .values(
                    status="running",
                    pid=os.getpid(),
                    current_task_id=new_task_id,
                ),
            )
        return await original_execute(session, statement, *args, **kwargs)

    with patch.object(AsyncSession, "execute", new=execute_with_owner_race):
        await d._cleanup_stale_state()

    assert injected
    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        old_task = await db.get(Task, old_task_id)
        new_task = await db.get(Task, new_task_id)
        assert instance.status == "running"
        assert instance.pid == os.getpid()
        assert instance.current_task_id == new_task_id
        assert old_task.status == "executing"
        assert new_task.status == "executing"


@pytest.mark.asyncio
async def test_cleanup_instance_cas_includes_started_at_generation(db_factory):
    """Same owner/PID with a new start timestamp is a replacement generation."""
    from datetime import datetime, timedelta

    d = _make_dispatcher(db_factory)
    old_started = datetime(2026, 7, 23, 10, 0, 0)
    new_started = old_started + timedelta(seconds=1)
    async with db_factory() as db:
        task = Task(title="started-at ABA", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="started-at-race",
            status="running",
            pid=os.getpid(),
            current_task_id=task.id,
            started_at=old_started,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    original_execute = AsyncSession.execute
    injected = False

    async def execute_with_started_at_race(session, statement, *args, **kwargs):
        nonlocal injected
        if (
            not injected
            and getattr(getattr(statement, "table", None), "name", None)
            == "instances"
        ):
            injected = True
            await original_execute(
                session,
                update(Instance)
                .where(Instance.id == instance_id)
                .values(started_at=new_started),
            )
        return await original_execute(session, statement, *args, **kwargs)

    with patch.object(
        AsyncSession, "execute", new=execute_with_started_at_race
    ):
        await d._cleanup_stale_state()

    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        task = await db.get(Task, task_id)
        assert instance.status == "running"
        assert instance.started_at == new_started
        assert task.status == "executing"


@pytest.mark.asyncio
async def test_pending_orphan_quarantine_never_overwrites_new_slot_owner(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(title="new pending owner", status="pending")
        db.add(task)
        await db.flush()
        orphan = Instance(
            name="old-live-orphan",
            status="running",
            pid=os.getpid(),
            current_task_id=task.id,
        )
        replacement = Instance(name="new-slot", status="idle")
        db.add_all([orphan, replacement])
        await db.flush()
        task.instance_id = replacement.id
        await db.commit()
        task_id = task.id
        orphan_id = orphan.id
        replacement_id = replacement.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        orphan = await db.get(Instance, orphan_id)
        assert task.status == "pending"
        assert task.instance_id == replacement_id
        assert orphan.status == "error"
        assert orphan.pid == os.getpid()


@pytest.mark.asyncio
async def test_cleanup_fail_closes_pending_task_still_owned_by_live_orphan(
    db_factory,
):
    """A stale pending write cannot make an unknown live PID dispatchable."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(title="dirty pending", description="d", status="pending")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="dirty-live-owner",
            status="running",
            pid=os.getpid(),
            current_task_id=task.id,
        )
        db.add(instance)
        await db.commit()
        task_id, instance_id = task.id, instance.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "failed"
        assert task.instance_id == instance_id
        assert "duplicate execution" in task.error_message
        assert instance.status == "error"
        assert instance.pid == os.getpid()
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_cleanup_resets_instance_with_no_pid(db_factory):
    """A running row without an owned generation is terminal error history."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="no-pid-worker", status="running", pid=None)
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "error"


@pytest.mark.asyncio
async def test_cleanup_resets_stuck_executing_task(db_factory):
    """A legacy unowned execution without boundary proof fails closed."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="stuck-task", description="test", status="executing")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        t = await db.get(Task, task_id)
        assert t.status == "failed"
        assert t.instance_id is None
        assert "provider-boundary proof" in t.error_message


@pytest.mark.asyncio
async def test_cleanup_replays_only_exact_hidden_initial_turn(db_factory):
    """A boundary-free canonical G1 source still denotes Task.description."""

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(name="initial-pre-spawn", status="idle")
        db.add(instance)
        await db.flush()
        task = Task(
            title="initial exact turn",
            description="run original task",
            status="executing",
            retry_count=0,
            turn_generation=1,
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        source = LogEntry(
            task_id=task.id,
            task_retry_count=0,
            task_turn_generation=1,
            turn_scope="source",
            event_type="turn_source",
            role="system",
            content=None,
            raw_json=json.dumps({
                "original_source_log_id": None,
                "transport": None,
            }),
            is_error=False,
        )
        db.add(source)
        await db.flush()
        task.turn_source_log_id = source.id
        await db.commit()
        task_id, source_id = task.id, source.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        source = await db.get(LogEntry, source_id)
    assert task.status == "pending"
    assert task.turn_generation == 1
    assert task.turn_source_log_id == source_id
    assert source.actual_transport is None


@pytest.mark.asyncio
async def test_cleanup_does_not_replay_visible_queued_turn_without_outbox(
    db_factory,
):
    """A lost visible follow-up must not fall back to Task.description."""

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(
            title="ordinary follow-up",
            description="old task description",
            status="executing",
            retry_count=2,
            turn_generation=7,
        )
        db.add(task)
        await db.flush()
        source = LogEntry(
            task_id=task.id,
            task_retry_count=2,
            task_turn_generation=7,
            turn_scope="source",
            event_type="user_message",
            role="user",
            content="new follow-up text",
            is_error=False,
        )
        db.add(source)
        await db.flush()
        task.turn_source_log_id = source.id
        await db.commit()
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "failed"
    assert task.turn_generation == 7
    assert "lost its durable replay envelope" in task.error_message


@pytest.mark.asyncio
async def test_cleanup_fails_exact_turn_with_actual_transport(db_factory):
    """Durable transport selection means a provider effect may exist."""

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(name="admitted-pre-spawn", status="idle")
        db.add(instance)
        await db.flush()
        task = Task(
            title="admitted initial turn",
            description="run once",
            status="executing",
            retry_count=0,
            turn_generation=1,
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        source = LogEntry(
            task_id=task.id,
            task_retry_count=0,
            task_turn_generation=1,
            turn_scope="source",
            actual_transport="codex_app_server",
            event_type="turn_source",
            role="system",
            content=None,
            raw_json=json.dumps({
                "original_source_log_id": None,
                "transport": "codex",
            }),
            is_error=False,
        )
        db.add(source)
        await db.flush()
        task.turn_source_log_id = source.id
        await db.commit()
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "failed"
    assert "transport codex_app_server" in task.error_message


@pytest.mark.asyncio
async def test_cleanup_fails_modern_g1_without_source_pointer(db_factory):
    """A pointer-less modern generation is not legacy replay evidence."""

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(
            title="missing modern source",
            description="cannot prove prompt identity",
            status="executing",
            retry_count=0,
            turn_generation=1,
            turn_source_log_id=None,
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "failed"
    assert "lost its durable replay envelope" in task.error_message


@pytest.mark.asyncio
async def test_cleanup_fails_later_hidden_source_less_turn(db_factory):
    """A source-less G>1 may be an internal wake, not Task.description."""

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(
            title="internal wake",
            description="old task description",
            status="executing",
            retry_count=0,
            turn_generation=2,
        )
        db.add(task)
        await db.flush()
        source = LogEntry(
            task_id=task.id,
            task_retry_count=0,
            task_turn_generation=2,
            turn_scope="source",
            event_type="turn_source",
            role="system",
            content=None,
            raw_json=json.dumps({
                "original_source_log_id": None,
                "transport": None,
            }),
            is_error=False,
        )
        db.add(source)
        await db.flush()
        task.turn_source_log_id = source.id
        await db.commit()
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "failed"
    assert "lost its durable replay envelope" in task.error_message


@pytest.mark.asyncio
async def test_cleanup_does_not_revive_concurrent_cancelled_initial_turn(
    db_factory,
):
    """The pre-lock generation snapshot prevents cancel -> pending revival."""

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(
            title="cancel races startup",
            description="run once",
            status="executing",
            retry_count=0,
            turn_generation=1,
        )
        db.add(task)
        await db.flush()
        source = LogEntry(
            task_id=task.id,
            task_retry_count=0,
            task_turn_generation=1,
            turn_scope="source",
            event_type="turn_source",
            role="system",
            content=None,
            raw_json=json.dumps({
                "original_source_log_id": None,
                "transport": None,
            }),
            is_error=False,
        )
        db.add(source)
        await db.flush()
        task.turn_source_log_id = source.id
        await db.commit()
        task_id = task.id

    original_execute = AsyncSession.execute
    task_select_count = 0
    cancellation_injected = False

    async def execute_with_cancel_race(session, statement, *args, **kwargs):
        nonlocal task_select_count, cancellation_injected
        from_tables = getattr(statement, "get_final_froms", lambda: [])()
        if any(getattr(table, "name", None) == "tasks" for table in from_tables):
            task_select_count += 1
            if task_select_count == 2:
                await original_execute(
                    session,
                    update(Task)
                    .where(Task.id == task_id, Task.status == "executing")
                    .values(status="cancelled"),
                )
                await session.commit()
                cancellation_injected = True
        return await original_execute(session, statement, *args, **kwargs)

    with patch.object(
        AsyncSession,
        "execute",
        new=execute_with_cancel_race,
    ):
        await d._cleanup_stale_state()

    assert cancellation_injected
    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "cancelled"
    assert task.turn_generation == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("delivery_status", ["launching", "launched"])
async def test_cleanup_quarantines_exact_plan_launch_claim(
    db_factory,
    delivery_status,
):
    """Plan boundary evidence overrides the otherwise replayable G1 shape."""

    d = _make_dispatcher(db_factory)
    receipt_key = f"restart-plan-{delivery_status}"
    async with db_factory() as db:
        task = Task(
            title="source-less Plan turn",
            description="old description",
            status="executing",
            retry_count=0,
            turn_generation=1,
        )
        db.add(task)
        await db.flush()
        source = LogEntry(
            task_id=task.id,
            task_retry_count=0,
            task_turn_generation=1,
            turn_scope="source",
            event_type="turn_source",
            role="system",
            content=None,
            raw_json=json.dumps({
                "original_source_log_id": None,
                "transport": None,
            }),
            is_error=False,
        )
        db.add(source)
        await db.flush()
        task.turn_source_log_id = source.id
        db.add(PlanApplicationReceipt(
            receipt_key=receipt_key,
            target_task_id=task.id,
            plan_version_ids=[],
            status="committed",
            response={"ok": True},
            delivery_status=delivery_status,
            outbox_payload={"prompt": "apply Plan once"},
            payload_digest="0" * 64,
            launch_evidence={
                "task_id": task.id,
                "retry_count": 0,
                "turn_generation": 1,
                "source_log_id": None,
            },
        ))
        await db.commit()
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        receipt = await db.scalar(select(PlanApplicationReceipt).where(
            PlanApplicationReceipt.receipt_key == receipt_key
        ))
    assert task.status == "failed"
    assert "exact Plan launch claim" in task.error_message
    assert receipt.delivery_status == "uncertain"
    assert "duplicate execution" in receipt.delivery_error


@pytest.mark.asyncio
async def test_cleanup_quarantines_worker_claim_after_transport_ack_loss(
    db_factory,
):
    """A committed route outranks a stale claimed callback receipt."""

    d = _make_dispatcher(db_factory)
    seed = await _seed_claimed_worker_handoff(
        db_factory,
        handoff_id="7" * 32,
        actual_transport="claude_exec",
    )
    task_id = seed["task_id"]
    handoff_id = seed["handoff_id"]
    claimed_generation = seed["claimed_generation"]

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        receipt = await db.get(WorkerTurnHandoffReceipt, handoff_id)
    assert task.status == "failed"
    assert task.instance_id is None
    assert task.turn_generation == claimed_generation
    assert "durable transport claude_exec" in task.error_message
    assert receipt.status == "launching"
    assert receipt.claimed_turn_generation == claimed_generation

    # The converged post-boundary receipt is not selected by later Worker
    # recovery passes and cannot put this exact G+1 back into memory.
    d._ensure_queue_worker = MagicMock()
    await d._recover_worker_turn_handoff_outbox()
    assert d._get_task_queue(task_id).empty()


@pytest.mark.asyncio
async def test_cleanup_replays_pretransport_claimed_worker_handoff_exactly(
    db_factory,
):
    """An orphan forward owner does not invalidate a pre-transport G+1."""

    d = _make_dispatcher(db_factory)
    seed = await _seed_claimed_worker_handoff(
        db_factory,
        handoff_id="8" * 32,
        actual_transport=None,
    )
    task_id = seed["task_id"]
    claimed_generation = seed["claimed_generation"]

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, seed["instance_id"])
        source = await db.get(LogEntry, seed["source_log_id"])
    assert task.instance_id == instance.id
    assert instance.status == "idle"
    assert instance.current_task_id is None
    assert source.actual_transport is None

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        receipt = await db.get(
            WorkerTurnHandoffReceipt,
            seed["handoff_id"],
        )
    assert task.status == "executing"
    assert task.instance_id is None
    assert task.turn_generation == claimed_generation
    assert task.completed_at is None
    assert task.error_message is None
    assert receipt.status == "claimed"
    assert receipt.claimed_turn_generation == claimed_generation

    d._ensure_queue_worker = MagicMock()
    await d._recover_worker_turn_handoff_outbox()
    # Recovery is idempotent while the exact handoff already owns the volatile
    # queue slot; it must not enqueue a second copy or create G+2.
    await d._recover_worker_turn_handoff_outbox()
    queue = d._get_task_queue(task_id)
    assert queue.qsize() == 1
    recovered = queue.get_nowait()
    assert recovered.source_log_id == seed["source_log_id"]
    assert recovered.worker_turn_handoff_id == seed["handoff_id"]
    assert recovered.claimed_retry_count == seed["retry_count"]
    assert recovered.claimed_turn_generation == claimed_generation
    assert (
        recovered.worker_turn_handoff_claimed_generation
        == claimed_generation
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        receipt = await db.get(
            WorkerTurnHandoffReceipt,
            seed["handoff_id"],
        )
    assert task.turn_generation == claimed_generation
    assert receipt.status == "claimed"
    assert receipt.claimed_turn_generation == claimed_generation


@pytest.mark.parametrize(
    ("include_queue_principal", "include_source_principal"),
    [(False, True), (True, False)],
    ids=["missing-queue-principal", "missing-source-principal"],
)
@pytest.mark.asyncio
async def test_cleanup_fail_closes_claimed_worker_handoff_without_principal_proof(
    db_factory,
    include_queue_principal,
    include_source_principal,
):
    """One startup pass rejects every incomplete Worker authority proof."""

    d = _make_dispatcher(db_factory)
    seed = await _seed_claimed_worker_handoff(
        db_factory,
        handoff_id=("9" if include_queue_principal else "a") * 32,
        actual_transport=None,
        include_queue_principal=include_queue_principal,
        include_source_principal=include_source_principal,
    )

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, seed["task_id"])
        receipt = await db.get(
            WorkerTurnHandoffReceipt,
            seed["handoff_id"],
        )
    assert task.status == "failed"
    assert task.instance_id is None
    assert "invalid claimed Worker handoff" in task.error_message
    assert receipt.status == "claimed"

    # The same startup's outbox recovery may quarantine the malformed receipt,
    # but it must never require a second cleanup pass or enqueue that G+1.
    d._ensure_queue_worker = MagicMock()
    await d._recover_worker_turn_handoff_outbox()
    assert d._get_task_queue(seed["task_id"]).empty()
    async with db_factory() as db:
        receipt = await db.get(
            WorkerTurnHandoffReceipt,
            seed["handoff_id"],
        )
    assert receipt.status == "launching"


@pytest.mark.asyncio
async def test_cleanup_fail_closes_active_task_with_routing_marker(
    db_factory,
):
    """A crash-left routing fence must recover to an ack-safe terminal status."""

    d = _make_dispatcher(db_factory)
    marker = {
        "op_id": "staged-before-restart",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    }
    async with db_factory() as db:
        task = Task(
            title="stuck-fenced-task",
            description="test",
            status="executing",
            metadata_={"worker_routing_config_pending": marker},
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "failed"
        assert task.instance_id is None
        assert task.metadata_["worker_routing_config_pending"] == marker


@pytest.mark.asyncio
async def test_cleanup_fails_multi_owner_corruption_without_replay(db_factory):
    """Multiple dead reverse owners are corruption, not retry permission."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(
            title="duplicate owners",
            description="test",
            status="executing",
        )
        db.add(task)
        await db.flush()
        owners = [
            Instance(
                name=f"duplicate-owner-{index}",
                status="running",
                pid=990000 + index,
                current_task_id=task.id,
            )
            for index in range(3)
        ]
        db.add_all(owners)
        await db.flush()
        task.instance_id = owners[-1].id
        await db.commit()
        task_id = task.id
        owner_ids = [owner.id for owner in owners]

    with patch(
        "backend.services.process_identity.os.kill",
        side_effect=ProcessLookupError,
    ):
        await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "failed"
        assert task.instance_id is None
        assert "inconsistent Task/Instance ownership" in task.error_message
        for owner_id in owner_ids:
            owner = await db.get(Instance, owner_id)
            assert owner.status == "error"
            assert owner.pid is None
            assert owner.current_task_id is None


@pytest.mark.asyncio
async def test_cleanup_requeues_unique_consistent_dead_owner(db_factory):
    """An exact unlaunched initial turn retries despite an older terminal log."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(
            title="unique owner",
            status="executing",
            retry_count=0,
            turn_generation=1,
        )
        db.add(task)
        await db.flush()
        owner = Instance(
            name="unique-dead-owner",
            status="running",
            pid=991111,
            current_task_id=task.id,
        )
        db.add(owner)
        await db.flush()
        task.instance_id = owner.id
        source = LogEntry(
            task_id=task.id,
            task_retry_count=0,
            task_turn_generation=1,
            turn_scope="source",
            event_type="turn_source",
            role="system",
            content=None,
            raw_json=json.dumps({
                "original_source_log_id": None,
                "transport": None,
            }),
            is_error=False,
        )
        db.add(source)
        await db.flush()
        task.turn_source_log_id = source.id
        db.add(
            LogEntry(
                task_id=task.id,
                instance_id=owner.id,
                event_type="result",
                is_error=True,
            )
        )
        await db.commit()
        task_id, owner_id = task.id, owner.id

    with patch(
        "backend.services.process_identity.os.kill",
        side_effect=ProcessLookupError,
    ):
        await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        owner = await db.get(Instance, owner_id)
        assert task.status == "pending"
        assert task.instance_id is None
        assert owner.status == "error"
        assert owner.pid is None
        assert owner.current_task_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("forward_owner", ["none", "different"])
async def test_cleanup_fails_single_mismatched_reverse_owner(
    db_factory,
    forward_owner,
):
    """A reverse owner is retryable only when the Task points back to it."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="mismatched owner", status="executing")
        db.add(task)
        await db.flush()
        reverse_owner = Instance(
            name="reverse-owner",
            status="running",
            pid=992222,
            current_task_id=task.id,
        )
        unrelated = Instance(name="unrelated-owner", status="idle")
        db.add_all([reverse_owner, unrelated])
        await db.flush()
        if forward_owner == "different":
            task.instance_id = unrelated.id
        await db.commit()
        task_id = task.id

    with patch(
        "backend.services.process_identity.os.kill",
        side_effect=ProcessLookupError,
    ):
        await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "failed"
        assert task.instance_id is None
        assert "inconsistent Task/Instance ownership" in task.error_message


@pytest.mark.asyncio
async def test_cleanup_preserves_live_owner_while_removing_dead_duplicate(
    db_factory,
):
    """A managed live generation wins over a dead duplicate reverse owner."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="live plus duplicate", status="executing")
        db.add(task)
        await db.flush()
        live_owner = Instance(
            name="managed-live-owner",
            status="running",
            pid=993331,
            current_task_id=task.id,
        )
        dead_duplicate = Instance(
            name="dead-duplicate-owner",
            status="running",
            pid=993332,
            current_task_id=task.id,
        )
        db.add_all([live_owner, dead_duplicate])
        await db.flush()
        task.instance_id = live_owner.id
        await db.commit()
        task_id = task.id
        live_id, dead_id = live_owner.id, dead_duplicate.id

    d.instance_manager.processes[live_id] = MagicMock(returncode=None)
    with patch(
        "backend.services.process_identity.os.kill",
        side_effect=ProcessLookupError,
    ):
        await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        live_owner = await db.get(Instance, live_id)
        dead_duplicate = await db.get(Instance, dead_id)
        assert task.status == "executing"
        assert task.instance_id == live_id
        assert live_owner.status == "running"
        assert live_owner.current_task_id == task_id
        assert dead_duplicate.status == "error"
        assert dead_duplicate.pid is None
        assert dead_duplicate.current_task_id is None


@pytest.mark.asyncio
async def test_cleanup_resets_stuck_in_progress_task(db_factory):
    """A legacy unowned in-progress claim is never blindly replayed."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="stuck-task-2", description="test", status="in_progress")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        t = await db.get(Task, task_id)
        assert t.status == "failed"
        assert "automatic replay was blocked" in t.error_message


@pytest.mark.asyncio
async def test_cleanup_preserves_session_id(db_factory):
    """Stuck task reset preserves session_id so user can resume chat."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="session-task", description="test", status="executing",
                    session_id="abc-123")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        t = await db.get(Task, task_id)
        assert t.status == "failed"
        assert t.session_id == "abc-123"


@pytest.mark.asyncio
async def test_cleanup_does_not_touch_pending_tasks(db_factory):
    """Pending tasks are not affected by cleanup."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="pending-task", description="test", status="pending")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        t = await db.get(Task, task_id)
        assert t.status == "pending"


@pytest.mark.asyncio
async def test_cleanup_does_not_touch_completed_tasks(db_factory):
    """Completed tasks are not affected by cleanup."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="done-task", description="test", status="completed")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        t = await db.get(Task, task_id)
        assert t.status == "completed"


@pytest.mark.asyncio
async def test_cleanup_does_not_touch_idle_instances(db_factory):
    """Idle instances are not affected by cleanup."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="idle-worker", status="idle")
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "idle"


@pytest.mark.asyncio
async def test_cleanup_acquires_task_write_before_instance_write(db_factory):
    """Startup reconciliation follows the global Task -> Instance lock order."""

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(title="ordered-cleanup", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="ordered-cleanup",
            status="running",
            pid=876543,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()

    original_execute = AsyncSession.execute
    write_tables: list[str] = []

    async def record_writes(session, statement, *args, **kwargs):
        table_name = getattr(
            getattr(statement, "table", None),
            "name",
            None,
        )
        if table_name in {"tasks", "instances"}:
            write_tables.append(table_name)
        return await original_execute(session, statement, *args, **kwargs)

    with (
        patch(
            "backend.services.process_identity.os.kill",
            side_effect=ProcessLookupError,
        ),
        patch.object(AsyncSession, "execute", new=record_writes),
    ):
        await d._cleanup_stale_state()

    assert "tasks" in write_tables
    assert "instances" in write_tables
    assert write_tables.index("tasks") < write_tables.index("instances")


@pytest.mark.asyncio
async def test_cleanup_called_on_start(db_factory):
    """_cleanup_stale_state is called during dispatcher start()."""
    d = _make_dispatcher(db_factory)

    async def fake_loop():
        await asyncio.sleep(999)
    d._dispatch_loop = fake_loop

    async with db_factory() as db:
        inst = Instance(name="stale-on-start", status="running", pid=999999)
        db.add(inst)
        await db.commit()
        await db.refresh(inst)
        inst_id = inst.id

    await d.start()

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "error"

    await d.stop()


# === _reset_instance_if_stale (safety net) tests ===


@pytest.mark.asyncio
async def test_safety_reset_fails_unclassified_task_and_releases_instance(db_factory):
    """A dead lifecycle owner is released without inventing task success."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="stuck-worker", status="running", pid=12345, current_task_id=1)
        db.add(inst)
        task = Task(title="test", description="test", status="executing")
        db.add(task)
        await db.flush()
        inst.current_task_id = task.id
        task.instance_id = inst.id
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    await d._reset_instance_if_stale(
        inst_id, await _lifecycle_generation(d, db_factory, task_id)
    )

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "idle"
        assert inst.pid is None
        assert inst.current_task_id is None
        t = await db.get(Task, task_id)
        assert t.status == "failed"
        assert t.error_message == (
            "Dispatcher lifecycle exited without an authoritative terminal "
            "result; automatic completion was blocked"
        )
        assert t.completed_at is not None


@pytest.mark.asyncio
async def test_safety_reset_preserves_owner_when_harness_cleanup_fails(
    db_factory,
):
    """Fallback reset cannot bypass the exact Harness owner stop fence."""

    d = _make_dispatcher(db_factory)
    d.instance_manager.is_running = MagicMock(return_value=False)
    d.instance_manager._instance_lifecycle_lock = MagicMock(
        return_value=asyncio.Lock()
    )
    d.instance_manager.reconcile_dead_reverse_task_owner = AsyncMock()
    async with db_factory() as db:
        task = Task(title="harness-cleanup-failure", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="harness-cleanup-failure",
            status="running",
            pid=12345,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        generation = d._task_lifecycle_generation(task)
        task_id, instance_id = task.id, instance.id

    class FailingOwnerStopFence:
        async def __aenter__(self):
            raise RuntimeError("Browser child cleanup could not be proven")

        async def __aexit__(self, _exc_type, _exc, _traceback):
            return False

    d.test_harness_service = MagicMock()
    d.test_harness_service.owner_stop_fence.return_value = (
        FailingOwnerStopFence()
    )

    await d._reset_instance_if_stale(instance_id, generation)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "executing"
        assert task.completed_at is None
        assert task.error_message is None
        assert instance.status == "running"
        assert instance.pid == 12345
        assert instance.current_task_id == task_id
    d.test_harness_service.owner_stop_fence.assert_called_once()
    d.instance_manager.reconcile_dead_reverse_task_owner.assert_not_awaited()


@pytest.mark.asyncio
async def test_safety_reset_takes_harness_fence_before_instance_lifecycle(
    db_factory,
):
    """A concurrent Harness stop cannot deadlock against stale reset."""

    from backend.services.test_harness_owner_fence import (
        test_harness_owner_fence as real_owner_fence,
    )

    d = _make_dispatcher(db_factory)
    d.instance_manager.is_running = MagicMock(return_value=False)
    lifecycle_lock = asyncio.Lock()
    d.instance_manager._instance_lifecycle_lock = MagicMock(
        return_value=lifecycle_lock
    )
    d.instance_manager.reconcile_dead_reverse_task_owner = AsyncMock()
    async with db_factory() as db:
        task = Task(title="ordered-harness-reset", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="ordered-harness-reset",
            status="running",
            pid=12347,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        generation = d._task_lifecycle_generation(task)
        task_id, instance_id = task.id, instance.id

    @asynccontextmanager
    async def owner_stop_fence(fenced_task_id, **_kwargs):
        # Model the service's re-entry into the process-local owner fence while
        # avoiding unrelated Harness graph I/O in this lock-order regression.
        async with real_owner_fence(fenced_task_id):
            yield

    d.test_harness_service = MagicMock()
    d.test_harness_service.owner_stop_fence.side_effect = owner_stop_fence

    competing_owner_ready = asyncio.Event()
    allow_competing_lifecycle = asyncio.Event()
    competing_lifecycle_acquired = asyncio.Event()
    stale_owner_attempted = asyncio.Event()
    stale_reset: asyncio.Task | None = None

    @asynccontextmanager
    async def observed_owner_fence(fenced_task_id):
        if asyncio.current_task() is stale_reset:
            stale_owner_attempted.set()
        async with real_owner_fence(fenced_task_id):
            yield

    async def competing_stop():
        async with real_owner_fence(task_id):
            competing_owner_ready.set()
            await allow_competing_lifecycle.wait()
            async with lifecycle_lock:
                competing_lifecycle_acquired.set()

    competitor = asyncio.create_task(competing_stop())
    await asyncio.wait_for(competing_owner_ready.wait(), timeout=1)
    try:
        with patch(
            "backend.services.test_harness_owner_fence.test_harness_owner_fence",
            observed_owner_fence,
        ):
            stale_reset = asyncio.create_task(
                d._reset_instance_if_stale(instance_id, generation)
            )
            await asyncio.wait_for(stale_owner_attempted.wait(), timeout=1)
            # The stale reset is now waiting on the Harness fence and must not
            # hold the Instance lifecycle lock.  A reverse-order regression
            # makes these two tasks wait on one another and hits this timeout.
            allow_competing_lifecycle.set()
            await asyncio.wait_for(
                competing_lifecycle_acquired.wait(),
                timeout=1,
            )
            await asyncio.wait_for(competitor, timeout=1)
            await asyncio.wait_for(stale_reset, timeout=2)
    finally:
        allow_competing_lifecycle.set()
        for pending in (competitor, stale_reset):
            if pending is not None and not pending.done():
                pending.cancel()
        await asyncio.gather(
            *(pending for pending in (competitor, stale_reset) if pending is not None),
            return_exceptions=True,
        )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "failed"
        assert instance.status == "idle"
        assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_safety_reset_terminal_without_exact_harness_gate_preserves_owner(
    db_factory,
):
    """A terminal status alone never proves Browser descendants were reaped."""

    d = _make_dispatcher(db_factory)
    d.instance_manager.is_running = MagicMock(return_value=False)
    d.instance_manager._instance_lifecycle_lock = MagicMock(
        return_value=asyncio.Lock()
    )
    async with db_factory() as db:
        task = Task(title="terminal-without-harness-gate", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="terminal-without-harness-gate",
            status="running",
            pid=12346,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        generation = d._task_lifecycle_generation(task)
        task.status = "failed"
        task.completed_at = datetime.utcnow()
        await db.commit()
        task_id, instance_id = task.id, instance.id

    await d._reset_instance_if_stale(instance_id, generation)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "failed"
        assert instance.status == "running"
        assert instance.pid == 12346
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_safety_reset_writes_task_before_instance(db_factory):
    """The fallback failure cannot invert the lifecycle DB lock order."""

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(title="ordered-reset", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="ordered-reset",
            status="running",
            pid=12345,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id, instance_id = task.id, instance.id

    original_execute = AsyncSession.execute
    write_tables: list[str] = []

    async def record_writes(session, statement, *args, **kwargs):
        table_name = getattr(
            getattr(statement, "table", None),
            "name",
            None,
        )
        if table_name in {"tasks", "instances"}:
            write_tables.append(table_name)
        return await original_execute(session, statement, *args, **kwargs)

    with patch.object(AsyncSession, "execute", new=record_writes):
        await d._reset_instance_if_stale(
            instance_id, await _lifecycle_generation(d, db_factory, task_id)
        )

    assert "tasks" in write_tables
    assert "instances" in write_tables
    assert write_tables.index("tasks") < write_tables.index("instances")


@pytest.mark.asyncio
async def test_safety_reset_does_not_complete_unbound_recovery_task(db_factory):
    """An old lifecycle cannot treat ``instance_id IS NULL`` as its owner."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="recovering", description="d", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="old-generation",
            status="running",
            pid=12345,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.commit()
        task_id, instance_id = task.id, instance.id

    await d._reset_instance_if_stale(
        instance_id, await _lifecycle_generation(d, db_factory, task_id)
    )

    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        task = await db.get(Task, task_id)
        assert instance.status == "running"
        assert instance.current_task_id == task_id
        assert instance.pid == 12345
        assert task.status == "executing"
        assert task.instance_id is None


@pytest.mark.asyncio
async def test_safety_reset_releases_dead_owner_after_retry_advanced(db_factory):
    """A completed retry transition must not strand its previous Instance."""

    from backend.services.instance_manager import InstanceManager

    d = _make_dispatcher(db_factory)
    d.instance_manager = InstanceManager(db_factory, d.broadcaster)
    old_task_started = datetime.utcnow()
    old_instance_started = datetime.utcnow()
    async with db_factory() as db:
        task = Task(
            title="retry-advanced",
            status="executing",
            retry_count=0,
            started_at=old_task_started,
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="dead-first-attempt",
            status="running",
            pid=812_202,
            current_task_id=task.id,
            started_at=old_instance_started,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        old_generation = d._task_lifecycle_generation(task)
        task_id, instance_id = task.id, instance.id

        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="pending",
                retry_count=1,
                instance_id=None,
                started_at=None,
            )
        )
        await db.commit()

    with patch(
        "backend.services.instance_manager.os.kill",
        side_effect=ProcessLookupError,
    ):
        await d._reset_instance_if_stale(instance_id, old_generation)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "pending"
        assert task.retry_count == 1
        assert task.instance_id is None
        assert instance.status == "idle"
        assert instance.pid is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_safety_reset_skips_already_idle_instance(db_factory):
    """If instance is already idle (consume_output cleaned up), safety net is a no-op."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="clean-worker", status="idle")
        db.add(inst)
        task = Task(title="test", description="test", status="completed")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id

    await d._reset_instance_if_stale(
        inst_id, await _lifecycle_generation(d, db_factory, task_id)
    )

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "idle"
        t = await db.get(Task, task_id)
        assert t.status == "completed"


@pytest.mark.asyncio
async def test_safety_reset_old_lifecycle_cannot_clear_recycled_owner(db_factory):
    """An old lifecycle finally must not erase a newer task on the same slot."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        old_task = Task(
            title="old",
            description="d",
            status="executing",
        )
        new_task = Task(
            title="new",
            description="d",
            status="executing",
        )
        db.add_all([old_task, new_task])
        await db.flush()
        inst = Instance(
            name="recycled",
            status="running",
            pid=222,
            current_task_id=new_task.id,
        )
        db.add(inst)
        await db.flush()
        old_task.instance_id = inst.id
        new_task.instance_id = inst.id
        await db.commit()
        old_id, new_id, inst_id = old_task.id, new_task.id, inst.id

    d.instance_manager.processes[inst_id] = MagicMock(returncode=0)
    d.instance_manager._instance_lifecycle_lock = MagicMock(
        return_value=asyncio.Lock()
    )

    await d._reset_instance_if_stale(
        inst_id, await _lifecycle_generation(d, db_factory, old_id)
    )

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "running"
        assert inst.current_task_id == new_id
        assert inst.pid == 222
        assert (await db.get(Task, old_id)).status == "executing"
        assert (await db.get(Task, new_id)).status == "executing"


@pytest.mark.asyncio
async def test_safety_reset_cannot_clear_same_task_same_slot_reclaim(
    db_factory,
):
    """Old finally cannot complete/clear a retried generation before spawn."""

    from datetime import datetime, timedelta

    d = _make_dispatcher(db_factory)
    old_task_started = datetime.utcnow() - timedelta(minutes=2)
    old_instance_started = datetime.utcnow() - timedelta(minutes=1)
    new_task_started = datetime.utcnow()
    new_instance_started = datetime.utcnow()
    async with db_factory() as db:
        task = Task(
            title="same-task-reclaim",
            status="executing",
            retry_count=0,
            started_at=old_task_started,
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="same-task-reclaim",
            status="running",
            pid=111,
            current_task_id=task.id,
            started_at=old_instance_started,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        old_generation = d._task_lifecycle_generation(task)
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(
                retry_count=1,
                started_at=new_task_started,
            )
        )
        await db.execute(
            update(Instance)
            .where(Instance.id == instance.id)
            .values(pid=222, started_at=new_instance_started)
        )
        await db.commit()
        task_id, instance_id = task.id, instance.id

    await d._reset_instance_if_stale(instance_id, old_generation)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "executing"
        assert task.retry_count == 1
        assert task.started_at == new_task_started
        assert instance.status == "running"
        assert instance.current_task_id == task_id
        assert instance.pid == 222
        assert instance.started_at == new_instance_started


@pytest.mark.asyncio
async def test_safety_reset_handles_db_error(db_factory):
    """Safety net does not raise on DB errors (logs instead)."""
    d = _make_dispatcher(db_factory)
    # Use a nonexistent instance_id — should not raise
    await d._reset_instance_if_stale(
        99999,
        _TaskLifecycleGeneration(
            task_id=99999,
            worker_id=None,
            shared_from_id=None,
            retry_count=0,
            turn_generation=0,
            instance_id=99999,
            started_at=None,
            completed_at=None,
        ),
    )


# === Interrupted task status tests ===


@pytest.mark.asyncio
async def test_interrupted_task_marked_completed(db_factory):
    """User-interrupted task (exit code -2/130) is marked completed, not pending."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="int-worker")
        db.add(inst)
        task = Task(title="interrupt-test", description="test", target_repo="/repo")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        task.status = "in_progress"
        task.instance_id = inst.id
        await db.commit()
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = -2  # SIGINT
    mock_proc.wait = AsyncMock(return_value=-2)
    d.instance_manager.processes = {inst_id: mock_proc}

    await d._run_task_lifecycle(inst_id, task_obj)

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"

    # Verify broadcast sent "completed" not "pending"
    calls = d.broadcaster.broadcast.call_args_list
    status_events = [c for c in calls if c[0][0] == "tasks" and c[0][1].get("new_status")]
    last_status = status_events[-1][0][1]["new_status"]
    assert last_status == "completed"


@pytest.mark.asyncio
async def test_interrupted_task_exit_130(db_factory):
    """Exit code 130 (SIGINT) also marks task completed."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="int-worker-130")
        db.add(inst)
        task = Task(title="interrupt-130", description="test", target_repo="/repo")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        task.status = "in_progress"
        task.instance_id = inst.id
        await db.commit()
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 130
    mock_proc.wait = AsyncMock(return_value=130)
    d.instance_manager.processes = {inst_id: mock_proc}

    await d._run_task_lifecycle(inst_id, task_obj)

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"


@pytest.mark.asyncio
async def test_interrupted_lifecycle_cannot_overwrite_concurrent_cancel(db_factory):
    """A stale exit-code result must lose to the user's cancelled status CAS."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="cancel-race")
        task = Task(title="cancel-race", description="d", status="pending")
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        task.status = "in_progress"
        task.instance_id = inst.id
        await db.commit()
        inst_id, task_id, task_obj = inst.id, task.id, task

    class Process:
        returncode = None

        async def wait(self):
            async with db_factory() as db:
                assert await TaskQueue(db).cancel(task_id) is not None
            self.returncode = -2
            return -2

    process = Process()
    d.instance_manager.processes[inst_id] = process
    d.instance_manager._instance_lifecycle_lock = MagicMock(
        return_value=asyncio.Lock()
    )

    await d._run_task_lifecycle(inst_id, task_obj)

    async with db_factory() as db:
        assert (await db.get(Task, task_id)).status == "cancelled"
    completed_events = [
        call
        for call in d.broadcaster.broadcast.await_args_list
        if len(call.args) > 1
        and call.args[0] == "tasks"
        and call.args[1].get("new_status") == "completed"
    ]
    assert not completed_events


# === Lifecycle finally block integration tests ===


@pytest.mark.asyncio
async def test_lifecycle_resets_instance_on_exception(db_factory):
    """Instance is reset to idle even when lifecycle throws an exception."""
    d = _make_dispatcher(db_factory)
    d.instance_manager.launch = AsyncMock(side_effect=RuntimeError("boom"))

    async with db_factory() as db:
        inst = Instance(name="exc-worker", status="running", pid=12345)
        db.add(inst)
        task = Task(title="exc-test", description="test", target_repo="/repo")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        task.status = "in_progress"
        task.instance_id = inst.id
        await db.commit()
        inst_id = inst.id
        task_obj = task

    await d._run_task_lifecycle(inst_id, task_obj)

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.status == "idle"
        assert inst.pid is None


@pytest.mark.asyncio
async def test_initial_lifecycle_preserves_claim_when_harness_cleanup_fails(
    db_factory,
):
    """in_progress -> executing cannot bypass the old owner graph fence."""

    d = _make_dispatcher(db_factory)

    @asynccontextmanager
    async def failing_owner_stop_fence(*_args, **_kwargs):
        raise RuntimeError("Harness owner graph cleanup failed")
        yield  # pragma: no cover

    d.test_harness_service = MagicMock()
    d.test_harness_service.owner_stop_fence.side_effect = (
        failing_owner_stop_fence
    )
    async with db_factory() as db:
        instance = Instance(
            name="initial-harness-failure",
            status="running",
            pid=12347,
        )
        task = Task(
            title="initial-harness-failure",
            description="test",
            target_repo="/repo",
            status="in_progress",
        )
        db.add_all((instance, task))
        await db.flush()
        task.instance_id = instance.id
        instance.current_task_id = task.id
        await db.commit()
        instance_id, task_id, task_snapshot = instance.id, task.id, task

    await d._run_task_lifecycle(instance_id, task_snapshot)

    d.instance_manager.launch.assert_not_awaited()
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "in_progress"
        assert task.completed_at is None
        assert instance.status == "running"
        assert instance.pid == 12347
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_lifecycle_success_does_not_double_reset(db_factory):
    """On normal success, instance ends in idle state (consume_output or safety net)."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="success-worker")
        db.add(inst)
        task = Task(title="success-test", description="test", target_repo="/repo")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        task.status = "in_progress"
        task.instance_id = inst.id
        await db.commit()
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    await d._run_task_lifecycle(inst_id, task_obj)

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"


# === Task deletion clears instance.current_task_id ===


@pytest.mark.asyncio
async def test_delete_task_clears_instance_current_task_id(db_factory):
    """Deleting a task clears current_task_id on any instance pointing to it."""
    async with db_factory() as db:
        inst = Instance(name="ref-worker", current_task_id=None)
        db.add(inst)
        task = Task(title="del-test", description="test", status="completed")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id
        # Set current_task_id after we know the task ID
        inst.current_task_id = task_id
        await db.commit()

    async with db_factory() as db:
        queue = TaskQueue(db)
        result = await queue.delete(task_id)
        assert result is True

    async with db_factory() as db:
        inst = await db.get(Instance, inst_id)
        assert inst.current_task_id is None


@pytest.mark.asyncio
async def test_delete_task_no_instance_reference(db_factory):
    """Deleting a task with no instance reference works fine."""
    async with db_factory() as db:
        task = Task(title="orphan-task", description="test", status="completed")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    async with db_factory() as db:
        queue = TaskQueue(db)
        result = await queue.delete(task_id)
        assert result is True


# === Stop-session orphan handling ===


@pytest.mark.asyncio
async def test_stop_session_orphaned_task_marked_completed(client, session_factory):
    """Stop-session with no process marks executing task as completed."""
    async with session_factory() as db:
        task = Task(title="orphan-stop", description="test", status="executing",
                    session_id="sess-123")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    with patch("backend.api.tasks._stop_task_process", new_callable=AsyncMock, return_value=False):
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert "completed" in data.get("note", "")

    async with session_factory() as db:
        t = await db.get(Task, task_id)
        assert t.status == "completed"
        assert t.session_id == "sess-123"


@pytest.mark.asyncio
async def test_stop_session_pending_task_returns_error(client, session_factory):
    """Stop-session on a pending task (no process, not executing) returns 400."""
    async with session_factory() as db:
        task = Task(title="pending-stop", description="test", status="pending")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    with patch("backend.api.tasks._stop_task_process", new_callable=AsyncMock, return_value=False):
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_stop_session_completed_task_returns_error(client, session_factory):
    """Stop-session on a completed task (no process) returns 400."""
    async with session_factory() as db:
        task = Task(title="done-stop", description="test", status="completed")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    with patch("backend.api.tasks._stop_task_process", new_callable=AsyncMock, return_value=False):
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_stop_session_in_progress_task_marked_completed(client, session_factory):
    """Stop-session with no process marks in_progress task as completed."""
    async with session_factory() as db:
        task = Task(title="in-progress-stop", description="test", status="in_progress")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    with patch("backend.api.tasks._stop_task_process", new_callable=AsyncMock, return_value=False):
        resp = await client.post(f"/api/tasks/{task_id}/stop-session")

    assert resp.status_code == 200
    assert resp.json()["ok"] is True


# === Detached PTY recovery ===


@pytest.mark.asyncio
async def test_startup_sub_agent_cleanup_defers_durable_monitor_recovery(
    db_factory,
):
    from backend import main as main_module

    async with db_factory() as db:
        background_task = Task(
            title="terminal foreground with native tail",
            description="work",
            status="completed",
            pty_background_generation="exact-native-epoch",
        )
        ordinary_task = Task(
            title="ordinary terminal parent",
            description="work",
            status="completed",
        )
        db.add_all([background_task, ordinary_task])
        await db.flush()
        rows = [
            SubAgentSession(
                task_id=background_task.id,
                source="native",
                agent_type="native-agent",
                description="dispatcher must recover this row",
                status="running",
            ),
            SubAgentSession(
                task_id=background_task.id,
                source="ccm",
                agent_type="monitor",
                description="sleeping durable CCM monitor",
                status="running",
                next_check_at=datetime.utcnow(),
            ),
            SubAgentSession(
                task_id=ordinary_task.id,
                source="ccm",
                agent_type="monitor",
                description="uncertain active CCM monitor",
                status="running",
                turn_generation=4,
                active_turn_generation=4,
            ),
            SubAgentSession(
                task_id=ordinary_task.id,
                source="native",
                agent_type="native-agent",
                description="no live background generation",
                status="running",
            ),
            SubAgentSession(
                task_id=ordinary_task.id,
                source="ccm",
                agent_type="sub_agent",
                description="ordinary one-shot CCM child",
                status="running",
            ),
            SubAgentSession(
                task_id=ordinary_task.id,
                source="ccm",
                agent_type="monitor",
                remote_id=77,
                description="remote monitor mirror",
                status="running",
            ),
        ]
        db.add_all(rows)
        await db.commit()
        row_ids = [row.id for row in rows]

    with patch.object(main_module, "async_session", db_factory):
        await main_module._cleanup_stale_sub_agents()

    async with db_factory() as db:
        current = [await db.get(SubAgentSession, row_id) for row_id in row_ids]
        assert current[0].status == "running"
        assert current[0].completed_at is None
        assert current[1].status == "running"
        assert current[1].completed_at is None
        assert current[2].status == "running"
        assert current[2].completed_at is None
        assert current[3].status == "completed"
        assert current[3].completed_at is not None
        assert current[4].status == "completed"
        assert current[4].completed_at is not None
        assert current[5].status == "completed"
        assert current[5].completed_at is not None

    dispatcher = _make_dispatcher(db_factory)
    await dispatcher._cleanup_stale_state()
    with patch.object(dispatcher, "start_monitor_session") as start:
        await dispatcher._recover_monitor_sessions()

    assert [call.args[0].id for call in start.call_args_list] == [row_ids[1]]
    async with db_factory() as db:
        uncertain = await db.get(SubAgentSession, row_ids[2])
        assert uncertain.status == "failed"
        assert uncertain.next_check_at is None
        assert "could not be recovered" in (uncertain.last_error or "")


@pytest.mark.asyncio
async def test_startup_fails_closed_orphaned_pty_background_marker(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    d.instance_manager.active_pty_background_task_ids.return_value = set()

    async with db_factory() as db:
        task = Task(
            title="orphaned PTY background",
            description="background tail was interrupted",
            status="executing",
            session_id="lost-session",
            pty_background_generation="lost-exact-epoch",
        )
        db.add(task)
        await db.flush()
        native_session = SubAgentSession(
                task_id=task.id,
                source="native",
                agent_type="native-agent",
                description="lost agent",
                status="running",
            )
        db.add(native_session)
        await db.commit()
        task_id = task.id
        native_session_id = native_session.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "failed"
        assert current.pty_background_generation is None
        assert "restarted before Claude PTY background" in (
            current.error_message or ""
        )
        log = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task_id,
                    LogEntry.event_type == "system_event",
                    LogEntry.is_error.is_(True),
                )
            )
        ).scalar_one()
        assert "restarted before Claude PTY background" in log.content
        assert (
            await db.get(SubAgentSession, native_session_id)
        ).status == "failed"

    assert any(
        call.args[0] == "tasks"
        and call.args[1].get("event") == "status_change"
        and call.args[1].get("task_id") == task_id
        and call.args[1].get("new_status") == "failed"
        for call in d.broadcaster.broadcast.await_args_list
    )


# === Mixed scenario: startup with multiple stale entities ===


@pytest.mark.asyncio
async def test_cleanup_multiple_stale_entities(db_factory):
    """Cleanup handles multiple stale instances and tasks in one pass."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        # Two dead instances
        inst1 = Instance(name="dead-1", status="running", pid=999991)
        inst2 = Instance(name="dead-2", status="running", pid=999992)
        # One alive instance
        inst3 = Instance(name="alive", status="idle")
        # Two stuck tasks
        task1 = Task(
            title="stuck-1",
            description="t",
            status="executing",
            retry_count=0,
            turn_generation=1,
        )
        task2 = Task(
            title="stuck-2",
            description="t",
            status="in_progress",
            retry_count=0,
            turn_generation=1,
        )
        # One normal task
        task3 = Task(title="normal", description="t", status="pending")
        for obj in [inst1, inst2, inst3, task1, task2, task3]:
            db.add(obj)
        await db.flush()
        sources = [
            LogEntry(
                task_id=task.id,
                task_retry_count=0,
                task_turn_generation=1,
                turn_scope="source",
                event_type="turn_source",
                role="system",
                content=None,
                raw_json=json.dumps({
                    "original_source_log_id": None,
                    "transport": None,
                }),
                is_error=False,
            )
            for task in (task1, task2)
        ]
        db.add_all(sources)
        await db.flush()
        task1.turn_source_log_id = sources[0].id
        task2.turn_source_log_id = sources[1].id
        await db.commit()
        for obj in [inst1, inst2, inst3, task1, task2, task3]:
            await db.refresh(obj)
        ids = {
            "inst1": inst1.id, "inst2": inst2.id, "inst3": inst3.id,
            "task1": task1.id, "task2": task2.id, "task3": task3.id,
        }

    await d._cleanup_stale_state()

    async with db_factory() as db:
        assert (await db.get(Instance, ids["inst1"])).status == "error"
        assert (await db.get(Instance, ids["inst2"])).status == "error"
        assert (await db.get(Instance, ids["inst3"])).status == "idle"
        assert (await db.get(Task, ids["task1"])).status == "pending"
        assert (await db.get(Task, ids["task2"])).status == "pending"
        assert (await db.get(Task, ids["task3"])).status == "pending"


# ---------------------------------------------------------------------------
# PID reuse / host restart recovery
#
# A bare os.kill(pid, 0) probe cannot distinguish "this exact generation is
# still running" from "an unrelated process inherited this PID number". When it
# guessed wrong the Task was fail-closed as failed forever and its Instance row
# kept occupying a max_concurrent_instances slot, with no UI path out.
# ---------------------------------------------------------------------------

_OTHER_BOOT_ID = "11111111-1111-4111-8111-111111111111"


def _encoded_identity(pid, start_ticks, boot_id):
    from backend.services.process_identity import (
        ProcessIdentity,
        encode_process_identity,
    )

    return encode_process_identity(
        ProcessIdentity(pid=pid, start_ticks=start_ticks, boot_id=boot_id)
    )


async def _add_unstarted_turn_source(
    db,
    task,
    *,
    generation=1,
    actual_transport=None,
):
    """Attach proof that this turn never reached a provider boundary.

    Without it the replay guard fail-closes for its own reason, which would
    mask whether the PID identity probe reached the right verdict.
    """
    source = LogEntry(
        task_id=task.id,
        task_retry_count=task.retry_count or 0,
        task_turn_generation=generation,
        turn_scope="source",
        event_type="turn_source",
        role="system",
        content=None,
        raw_json=json.dumps(
            {"original_source_log_id": None, "transport": None}
        ),
        actual_transport=actual_transport,
        is_error=False,
    )
    db.add(source)
    await db.flush()
    task.turn_generation = generation
    task.turn_source_log_id = source.id
    return source


@pytest.mark.asyncio
async def test_cleanup_requeues_pid_recorded_in_a_previous_boot(db_factory):
    """A PID from an earlier boot is provably dead even if the number answers.

    This is the reported failure: after a host restart an unrelated process
    answered PID 590565, so the owning task was permanently failed.
    """
    d = _make_dispatcher(db_factory)
    live_pid = os.getpid()

    async with db_factory() as db:
        task = Task(title="previous boot owner", status="executing", retry_count=0)
        db.add(task)
        await db.flush()
        await _add_unstarted_turn_source(db, task)
        owner = Instance(
            name="previous-boot-owner",
            status="running",
            pid=live_pid,
            current_task_id=task.id,
            process_identity=_encoded_identity(live_pid, 4242, _OTHER_BOOT_ID),
        )
        db.add(owner)
        await db.flush()
        task.instance_id = owner.id
        await db.commit()
        task_id, owner_id = task.id, owner.id

    # No os.kill patch: the boot id alone proves death, and the live PID would
    # otherwise be misread as this generation still running.
    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        owner = await db.get(Instance, owner_id)
        assert task.status == "pending", task.error_message
        assert task.instance_id is None
        assert owner.pid is None
        assert owner.process_identity is None
        assert owner.current_task_id is None


@pytest.mark.asyncio
async def test_cleanup_releases_previous_boot_pid_but_keeps_transport_boundary(
    db_factory,
):
    """PID recovery must not weaken the independent provider-effect fence."""

    d = _make_dispatcher(db_factory)
    live_pid = os.getpid()

    async with db_factory() as db:
        task = Task(
            title="previous boot after provider boundary",
            status="executing",
            retry_count=0,
        )
        db.add(task)
        await db.flush()
        await _add_unstarted_turn_source(
            db,
            task,
            actual_transport="claude_pty",
        )
        owner = Instance(
            name="previous-boot-provider-owner",
            status="running",
            pid=live_pid,
            current_task_id=task.id,
            process_identity=_encoded_identity(
                live_pid,
                4242,
                _OTHER_BOOT_ID,
            ),
        )
        db.add(owner)
        await db.flush()
        task.instance_id = owner.id
        await db.commit()
        task_id, owner_id = task.id, owner.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        owner = await db.get(Instance, owner_id)
        assert task.status == "failed"
        assert "exact turn selected transport claude_pty" in task.error_message
        assert "Unmanaged process PID" not in task.error_message
        assert task.instance_id is None
        assert owner.status == "error"
        assert owner.pid is None
        assert owner.process_identity is None
        assert owner.current_task_id is None


@pytest.mark.asyncio
async def test_cleanup_requeues_reused_pid_with_different_start_ticks(db_factory):
    """Same boot, same PID number, different start time: the owner is gone."""
    d = _make_dispatcher(db_factory)
    live_pid = os.getpid()

    from backend.services import process_identity as pi

    current = pi.read_process_identity(live_pid)

    async with db_factory() as db:
        task = Task(title="reused pid owner", status="executing", retry_count=0)
        db.add(task)
        await db.flush()
        await _add_unstarted_turn_source(db, task)
        owner = Instance(
            name="reused-pid-owner",
            status="running",
            pid=live_pid,
            current_task_id=task.id,
            process_identity=_encoded_identity(
                live_pid,
                current.start_ticks + 9999,
                current.boot_id,
            ),
        )
        db.add(owner)
        await db.flush()
        task.instance_id = owner.id
        await db.commit()
        task_id, owner_id = task.id, owner.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        owner = await db.get(Instance, owner_id)
        assert task.status == "pending", task.error_message
        assert task.instance_id is None
        assert owner.pid is None


@pytest.mark.asyncio
async def test_cleanup_still_fails_closed_for_exact_matching_identity(db_factory):
    """The safety property: a genuinely live generation is never requeued."""
    d = _make_dispatcher(db_factory)
    live_pid = os.getpid()

    from backend.services import process_identity as pi

    current = pi.read_process_identity(live_pid)

    async with db_factory() as db:
        task = Task(title="live owner", status="executing")
        db.add(task)
        await db.flush()
        owner = Instance(
            name="live-owner",
            status="running",
            pid=live_pid,
            current_task_id=task.id,
            process_identity=_encoded_identity(
                live_pid,
                current.start_ticks,
                current.boot_id,
            ),
        )
        db.add(owner)
        await db.flush()
        task.instance_id = owner.id
        await db.commit()
        task_id, owner_id = task.id, owner.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        owner = await db.get(Instance, owner_id)
        assert task.status == "failed"
        assert "may still be running" in task.error_message
        # Owner evidence must survive so an operator can reconcile it.
        assert owner.pid == live_pid


@pytest.mark.asyncio
async def test_cleanup_fails_closed_when_identity_pid_does_not_match(db_factory):
    """A stale identity written for another PID must not prove death.

    The PID is embedded in the persisted value so that a write site which
    updates the PID without refreshing identity degrades to the conservative
    probe instead of silently authorizing duplicate execution.
    """
    d = _make_dispatcher(db_factory)
    live_pid = os.getpid()

    async with db_factory() as db:
        task = Task(title="mismatched identity", status="executing")
        db.add(task)
        await db.flush()
        owner = Instance(
            name="mismatched-identity-owner",
            status="running",
            pid=live_pid,
            current_task_id=task.id,
            # Identity recorded for a *different* PID, in a dead boot.
            process_identity=_encoded_identity(
                live_pid + 1,
                4242,
                _OTHER_BOOT_ID,
            ),
        )
        db.add(owner)
        await db.flush()
        task.instance_id = owner.id
        await db.commit()
        task_id = task.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "failed"
        assert "may still be running" in task.error_message


@pytest.mark.asyncio
async def test_cleanup_treats_null_pid_as_nothing_alive(db_factory):
    """A NULL PID records no process, so there is nothing that can be alive.

    The identity probe reports ``unknown`` for a missing PID because it cannot
    prove death from an absent value. Callers must keep the original NULL check
    ahead of it, otherwise owner-only rows that never held a process would be
    quarantined as live evidence and pin their task forever.
    """
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(title="owner without pid", status="executing", retry_count=0)
        db.add(task)
        await db.flush()
        await _add_unstarted_turn_source(db, task)
        owner = Instance(
            name="pidless-owner",
            status="error",
            pid=None,
            process_identity=None,
            current_task_id=task.id,
        )
        db.add(owner)
        await db.flush()
        task.instance_id = owner.id
        await db.commit()
        task_id, owner_id = task.id, owner.id

    await d._cleanup_stale_state()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        owner = await db.get(Instance, owner_id)
        assert task.status == "pending", task.error_message
        assert task.instance_id is None
        assert owner.current_task_id is None
