"""Fail-closed Worker Task and node drain proof tests."""

from __future__ import annotations

import asyncio
import secrets
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.config import settings
from backend.database import Base
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.project import Project
from backend.models.task import Task
from backend.models.task_migration import TaskMigrationOperation
from backend.models.task_ssh_effect import TaskSSHEffectReceipt
from backend.models.worker_task_termination import (
    WorkerTaskTerminationReceipt,
)
from backend.models.worker_turn_handoff import WorkerTurnHandoffReceipt
from backend.models.worker import Worker, WorkerNodeControl
from backend.models.capability import (
    CapabilityExecution,
    CapabilityInvocation,
    CapabilityResumeOutbox,
)
from backend.models.sub_agent import SubAgentReport, SubAgentSession
from backend.models.task_id_allocator import (
    TASK_ID_ALLOCATOR_SINGLETON_ID,
    TASK_ID_WORKER_NAMESPACE_START,
    TaskIdAllocator,
)
from backend.models.test_harness import (
    TestHarnessChildBinding as HarnessChildBinding,
    TestHarnessRun as HarnessRun,
)
from backend.services.test_harness_owner_fence import (
    TEST_HARNESS_TERMINAL_GATE_KEY,
    test_harness_owner_identity as _test_harness_owner_identity,
)
from backend.services.test_harness import TestHarnessService as _HarnessService
from backend.services.test_harness_contracts import TestHarnessSpec as _HarnessSpec
from backend.services.skill_context import WORKER_MANAGED_TASK_METADATA_KEY
from backend.services.task_creation import stage_task_record
from backend.services.task_events import (
    PTY_TERMINAL_PUBLICATION_EVENT_TYPE,
    build_pty_terminal_publication_payload,
)
from backend.services import instance_manager as instance_manager_module
from backend.services.instance_manager import InstanceManager, _OutputConsumerRecord
from backend.services.worker_drain_proof import (
    build_worker_node_drain_proof as _build_worker_node_drain_proof,
    seal_worker_node_runtime,
    verify_worker_node_drain_proof_signature,
    worker_node_drain_proof_signature,
)
from backend.services.worker_node_control import (
    begin_worker_node_drain,
    begin_worker_node_runtime_seal,
    fence_worker_node_receipt_resolution,
)
from backend.services.worker_proxy import (
    WorkerProxy,
    capture_worker_destroy_lifecycle_claim,
)
from backend.services.worker_task_termination import (
    WorkerTaskTerminationConflict,
    acknowledge_worker_receipt,
    canonical_json_digest,
    execute_worker_receipt,
    stage_worker_receipt,
)


pytestmark = pytest.mark.asyncio

_DRAIN_CLAIM = "9" * 64


async def build_worker_node_drain_proof(db, *, nonce: str):
    """Exercise the public two-step begin -> proof protocol in unit tests."""

    await begin_worker_node_drain(db, claim=_DRAIN_CLAIM)
    await db.commit()
    return await _seal_and_build_worker_node_drain_proof(
        db,
        nonce=nonce,
        drain_claim=_DRAIN_CLAIM,
    )


async def _seal_and_build_worker_node_drain_proof(
    db,
    *,
    nonce: str,
    drain_claim: str,
):
    await seal_worker_node_runtime(db, drain_claim=drain_claim)
    return await _build_worker_node_drain_proof(
        db,
        nonce=nonce,
        drain_claim=drain_claim,
    )


async def _bind_worker(db) -> None:
    changed = await db.execute(
        update(TaskIdAllocator)
        .where(TaskIdAllocator.id == TASK_ID_ALLOCATOR_SINGLETON_ID)
        .values(node_role="worker")
    )
    assert changed.rowcount == 1
    await db.commit()


def _request_payload(task: Task, operation_id: str) -> dict:
    return {
        "version": 2,
        "operation_id": operation_id,
        "task_id": task.id,
        "operation": "stop_session",
        "manager_worker_id": 7,
        "expected_remote": {
            "status": task.status,
            "retry_count": task.retry_count,
            "turn_generation": task.turn_generation,
        },
        "manager_handoff": None,
    }


async def _seed_accepted_worker_handoff(
    db: AsyncSession,
    task: Task,
    *,
    handoff_id: str,
) -> tuple[LogEntry, WorkerTurnHandoffReceipt]:
    source = LogEntry(
        task_id=task.id,
        event_type="user_message",
        role="user",
        content="durable Worker handoff",
        raw_json="{}",
        is_error=False,
    )
    db.add(source)
    await db.flush()
    request_payload = {
        "worker_turn_handoff_id": handoff_id,
        "worker_turn_handoff_retry_count": task.retry_count,
        "worker_turn_handoff_from_generation": task.turn_generation,
        "worker_turn_handoff_incarnation_id": task.incarnation_id,
    }
    queue_payload = {
        "prompt": "recover this exact Worker turn",
        "priority": 0,
        "source": "user",
        "command_skills": None,
        "model_override": None,
        "expected_task_routing": ["claude", None, "default"],
        "source_log_id": source.id,
        "user_message_text": "recover this exact Worker turn",
        "current_message": "recover this exact Worker turn",
        "attachment_paths": [],
        "queue_timestamp": 1.0,
        "allow_new_session": False,
        "delivery_key": None,
        "initiating_user_id": None,
        "initiating_user_role": "member",
        "execution_mode": "sandbox",
        "execution_principal_kind": "system",
        **request_payload,
    }
    receipt = WorkerTurnHandoffReceipt(
        handoff_id=handoff_id,
        task_id=task.id,
        source_log_id=source.id,
        side="worker",
        worker_id=None,
        retry_count=task.retry_count,
        from_generation=task.turn_generation,
        status="accepted",
        request_payload=request_payload,
        request_digest=canonical_json_digest(request_payload),
        queue_payload=queue_payload,
        queue_payload_digest=canonical_json_digest(queue_payload),
        response={"ok": True, "queued": True},
    )
    db.add(receipt)
    await db.commit()
    return source, receipt


async def test_terminal_worker_receipt_installs_exact_owner_gate(db_session):
    task = Task(title="terminal mirror", status="completed")
    db_session.add(task)
    await db_session.commit()
    await db_session.refresh(task)
    task_id = task.id
    operation_id = secrets.token_hex(16)
    payload = _request_payload(task, operation_id)
    staged = await stage_worker_receipt(
        db_session,
        task_id=task_id,
        operation_id=operation_id,
        operation="stop_session",
        request_payload=payload,
        request_digest=canonical_json_digest(payload),
    )

    receipt = await execute_worker_receipt(db_session, staged.operation_id)

    assert receipt.status == "succeeded"
    request_digest = receipt.request_digest
    result_digest = receipt.result_digest
    await db_session.rollback()
    db_session.expire_all()
    terminal = await db_session.get(Task, task_id)
    gate = terminal.metadata_[TEST_HARNESS_TERMINAL_GATE_KEY]
    assert gate == {
        "incarnation_id": terminal.incarnation_id,
        "retry_count": terminal.retry_count,
        "turn_generation": terminal.turn_generation,
        "status": "completed",
        "reason": "Worker termination receipt drained owner graph",
        "cleanup_harness_run_ids": [],
        "cleanup_workspace_run_ids": [],
        "cleanup_browser_binding_ids": [],
    }

    acknowledged = await acknowledge_worker_receipt(
        db_session,
        task_id=task_id,
        operation_id=operation_id,
        request_digest=request_digest,
        result_digest=result_digest,
    )
    assert acknowledged.status == "acknowledged"


async def test_begin_drain_preserves_final_output_from_admitted_turn(
    db_session,
    db_factory,
    monkeypatch,
):
    """Stage-one drain must not discard an already-admitted turn's tail."""

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    await _bind_worker(db_session)
    started_at = datetime.utcnow()
    instance = Instance(
        name="drain-admitted-output-slot",
        status="running",
        pid=4242,
        started_at=started_at,
    )
    task = Task(
        title="turn admitted before drain",
        status="executing",
        retry_count=3,
        turn_generation=7,
    )
    db_session.add_all((instance, task))
    await db_session.flush()
    instance.current_task_id = task.id
    task.instance_id = instance.id
    await db_session.commit()

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    manager = InstanceManager(db_factory, broadcaster)
    process = MagicMock()
    process.pid = instance.pid
    consumer = MagicMock()
    record = _OutputConsumerRecord(
        process=process,
        task=consumer,
        chat_initiated=False,
        provider="claude",
        task_id=task.id,
        task_retry_count=task.retry_count,
        task_turn_generation=task.turn_generation,
        instance_started_at=started_at,
    )
    manager.processes[instance.id] = process
    manager._tasks[instance.id] = consumer
    manager._consumer_records[instance.id] = record

    await begin_worker_node_drain(db_session, claim=_DRAIN_CLAIM)
    await db_session.commit()
    await manager._process_event(
        instance.id,
        task.id,
        {
            "event_type": "message",
            "role": "assistant",
            "content": "final answer committed while destroy stops this turn",
        },
    )

    async with db_factory() as db:
        retained = await db.scalar(
            select(LogEntry.content).where(
                LogEntry.task_id == task.id,
                LogEntry.task_retry_count == task.retry_count,
                LogEntry.task_turn_generation == task.turn_generation,
            )
        )
    assert retained == "final answer committed while destroy stops this turn"


async def test_exact_node_claim_replay_preserves_first_phase_timestamps(
    db_session,
    monkeypatch,
):
    """Destroy restart must not rewrite when either irreversible phase won."""

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    await begin_worker_node_drain(db_session, claim=_DRAIN_CLAIM)
    await db_session.commit()
    drain_started_at = datetime(2026, 8, 14, 12, 0, 0)
    await db_session.execute(
        update(WorkerNodeControl).values(drain_started_at=drain_started_at)
    )
    await db_session.commit()

    await begin_worker_node_drain(db_session, claim=_DRAIN_CLAIM)
    await db_session.commit()
    control = await db_session.get(WorkerNodeControl, 1)
    assert control.drain_started_at == drain_started_at

    await begin_worker_node_runtime_seal(db_session, claim=_DRAIN_CLAIM)
    await db_session.commit()
    runtime_sealed_at = datetime(2026, 8, 14, 12, 5, 0)
    await db_session.execute(
        update(WorkerNodeControl).values(runtime_sealed_at=runtime_sealed_at)
    )
    await db_session.commit()

    await begin_worker_node_runtime_seal(db_session, claim=_DRAIN_CLAIM)
    await db_session.commit()
    db_session.expire_all()
    control = await db_session.get(WorkerNodeControl, 1)
    assert control.drain_started_at == drain_started_at
    assert control.runtime_sealed_at == runtime_sealed_at


async def test_clean_proof_rejects_late_completed_pty_producers(
    db_session,
    db_factory,
    monkeypatch,
):
    """A callback paused before its first write cannot stale a clean proof."""

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    await _bind_worker(db_session)
    instance = Instance(name="late-pty-proof-slot")
    task = Task(
        title="terminal mirror with dormant PTY",
        status="completed",
        session_id="late-pty-proof-session",
        completed_at=datetime.utcnow(),
    )
    db_session.add_all((instance, task))
    await db_session.commit()
    await db_session.refresh(instance)
    await db_session.refresh(task)
    task_id = task.id
    instance_id = instance.id

    operation_id = secrets.token_hex(16)
    payload = _request_payload(task, operation_id)
    staged = await stage_worker_receipt(
        db_session,
        task_id=task_id,
        operation_id=operation_id,
        operation="stop_session",
        request_payload=payload,
        request_digest=canonical_json_digest(payload),
    )
    receipt = await execute_worker_receipt(db_session, staged.operation_id)
    assert receipt.status == "succeeded"
    request_digest = receipt.request_digest
    result_digest = receipt.result_digest
    await db_session.rollback()
    acknowledged = await acknowledge_worker_receipt(
        db_session,
        task_id=task_id,
        operation_id=operation_id,
        request_digest=request_digest,
        result_digest=result_digest,
    )
    assert acknowledged.status == "acknowledged"

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    manager = InstanceManager(db_factory, broadcaster)
    callback_reached_write = asyncio.Event()
    release_callback = asyncio.Event()
    original_fence = instance_manager_module._fence_worker_runtime_admission

    async def pause_before_autonomous_write(db, *, producer):
        if producer == "PTY autonomous-activity admission":
            callback_reached_write.set()
            await release_callback.wait()
        return await original_fence(db, producer=producer)

    monkeypatch.setattr(
        instance_manager_module,
        "_fence_worker_runtime_admission",
        pause_before_autonomous_write,
    )

    session = MagicMock()
    session.session_id = "late-pty-proof-session"
    session.is_alive = True
    session.has_pending_subagents = False
    late_callback = asyncio.create_task(
        manager.begin_pty_autonomous_activity(
            task_id,
            session.session_id,
            session,
            {
                "event_type": "message",
                "role": "assistant",
                "content": "must not cross the drain proof",
                "autonomous": True,
            },
        )
    )
    await asyncio.wait_for(callback_reached_write.wait(), timeout=1)

    proof = await build_worker_node_drain_proof(
        db_session,
        nonce="7" * 32,
    )
    assert proof["safe_to_destroy"] is True

    release_callback.set()
    assert await asyncio.wait_for(late_callback, timeout=1) is None

    # The other late PTY producers must independently honor the node claim;
    # they cannot rely on the now-acknowledged Task receipt remaining active.
    await manager._process_event(
        instance_id,
        task_id,
        {
            "event_type": "message",
            "role": "assistant",
            "content": "late durable event",
        },
    )
    await manager._upsert_native_sub_agent(
        task_id,
        "subagent_spawn",
        {
            "tool_use_id": "late-native-agent",
            "kind": "Agent",
            "description": "late native audit",
        },
        task_retry_count=0,
        task_turn_generation=0,
    )

    async with db_factory() as db:
        terminal = await db.get(Task, task_id)
        assert terminal.pty_background_generation is None
        assert await db.scalar(
            select(LogEntry.id).where(LogEntry.task_id == task_id)
        ) is None
        assert await db.scalar(
            select(SubAgentSession.id).where(
                SubAgentSession.task_id == task_id
            )
        ) is None
    assert (task_id, session.session_id) not in manager._pty_background_states
    broadcaster.broadcast.assert_not_awaited()


async def test_clean_proof_rejects_late_termination_put_without_tombstone(
    client,
    session_factory,
    worker_control_plane_auth,
    monkeypatch,
):
    """A signed clean proof remains true when a late Manager PUT arrives."""

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    async with session_factory() as db:
        await _bind_worker(db)
        task = Task(
            id=TASK_ID_WORKER_NAMESPACE_START,
            title="drained local task",
            status="completed",
            completed_at=datetime.utcnow(),
        )
        db.add(task)
        await db.commit()
        task_id = task.id
        operation_id = secrets.token_hex(16)
        payload = _request_payload(task, operation_id)
        payload["operation"] = "cancel"
        digest = canonical_json_digest(payload)

        proof = await build_worker_node_drain_proof(
            db,
            nonce="8" * 32,
        )
        assert proof["safe_to_destroy"] is True

    response = await client.put(
        f"/api/tasks/{task_id}/termination-receipts/{operation_id}",
        json={
            "operation": "cancel",
            "request_payload": payload,
            "request_digest": digest,
        },
    )

    assert response.status_code == 409, response.text
    assert "destruction has begun" in response.json()["detail"]
    async with session_factory() as db:
        assert await db.get(
            WorkerTaskTerminationReceipt,
            operation_id,
        ) is None
        repeated = await build_worker_node_drain_proof(
            db,
            nonce="a" * 32,
        )
        assert repeated["safe_to_destroy"] is True


async def test_drain_allows_only_existing_exact_termination_receipt(
    db_session,
    monkeypatch,
):
    """Drain may converge pre-existing ownership but cannot create another."""

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    await _bind_worker(db_session)
    task = Task(
        id=TASK_ID_WORKER_NAMESPACE_START,
        title="receipt admitted before drain",
        status="completed",
        completed_at=datetime.utcnow(),
    )
    db_session.add(task)
    await db_session.commit()
    operation_id = secrets.token_hex(16)
    payload = _request_payload(task, operation_id)
    digest = canonical_json_digest(payload)
    staged = await stage_worker_receipt(
        db_session,
        task_id=task.id,
        operation_id=operation_id,
        operation="stop_session",
        request_payload=payload,
        request_digest=digest,
    )
    assert staged.status == "accepted"

    await begin_worker_node_drain(db_session, claim=_DRAIN_CLAIM)
    await db_session.commit()

    replayed = await stage_worker_receipt(
        db_session,
        task_id=task.id,
        operation_id=operation_id,
        operation="stop_session",
        request_payload=payload,
        request_digest=digest,
    )
    assert replayed.operation_id == operation_id
    assert replayed.status == "accepted"

    different_operation_id = secrets.token_hex(16)
    different_payload = _request_payload(task, different_operation_id)
    with pytest.raises(
        WorkerTaskTerminationConflict,
        match="destruction has begun",
    ):
        await stage_worker_receipt(
            db_session,
            task_id=task.id,
            operation_id=different_operation_id,
            operation="stop_session",
            request_payload=different_payload,
            request_digest=canonical_json_digest(different_payload),
        )
    await db_session.rollback()
    assert await db_session.get(
        WorkerTaskTerminationReceipt,
        different_operation_id,
    ) is None


async def test_node_proof_rejects_terminal_owner_with_active_browser_child(
    db_session,
):
    await _bind_worker(db_session)
    owner = Task(title="terminal owner", status="completed")
    child = Task(
        id=TASK_ID_WORKER_NAMESPACE_START,
        title="active browser child",
        status="executing",
        pty_background_generation="browser-child-background",
    )
    db_session.add_all((owner, child))
    await db_session.flush()
    run_id = "a" * 32
    binding_id = "b" * 32
    db_session.add(
        HarnessRun(
            id=run_id,
            task_id=owner.id,
            owner_task_incarnation_id=owner.incarnation_id,
            owner_task_retry_count=owner.retry_count,
            owner_task_turn_generation=owner.turn_generation,
            owner_task_status=owner.status,
            target_kind="fixed_url",
            target_spec={"url": "https://example.com"},
            test_plan={"scenarios": []},
            runtime_config={},
            request_fingerprint="f" * 64,
            root_run_id=run_id,
            status="running",
            cleanup_status="pending",
        )
    )
    db_session.add(
        HarnessChildBinding(
            id=binding_id,
            harness_run_id=run_id,
            owner_task_id=owner.id,
            owner_task_incarnation_id=owner.incarnation_id,
            owner_task_retry_count=owner.retry_count,
            owner_task_turn_generation=owner.turn_generation,
            owner_task_status=owner.status,
            child_task_id=child.id,
            child_task_incarnation_id=child.incarnation_id,
            browser_review_job_id="c" * 32,
            state="running",
        )
    )
    await db_session.commit()

    proof = await build_worker_node_drain_proof(
        db_session,
        nonce="d" * 32,
    )

    assert proof["safe_to_destroy"] is False
    kinds = {blocker["kind"] for blocker in proof["blockers"]}
    assert "task_nonterminal" in kinds
    assert "task_pty_background" in kinds
    assert "harness_run_active" in kinds
    assert "child_binding_active" in kinds
    # The proof is read-only with respect to evidence: it refuses destruction
    # instead of deleting the only report/child ownership record.
    assert await db_session.get(HarnessRun, run_id) is not None
    assert await db_session.get(HarnessChildBinding, binding_id) is not None


async def test_node_proof_rejects_cleanup_complete_but_unmigrated_evidence(
    db_session,
):
    await _bind_worker(db_session)
    run_id = "e" * 32
    db_session.add(
        HarnessRun(
            id=run_id,
            task_id=None,
            target_kind="fixed_url",
            target_spec={"url": "https://example.com"},
            test_plan={"scenarios": []},
            runtime_config={},
            request_fingerprint="a" * 64,
            root_run_id=run_id,
            status="completed",
            cleanup_status="completed",
        )
    )
    await db_session.commit()

    proof = await build_worker_node_drain_proof(
        db_session,
        nonce="f" * 32,
    )

    assert proof["safe_to_destroy"] is False
    assert any(
        blocker["kind"] == "unmigrated_harness_evidence"
        for blocker in proof["blockers"]
    )


async def test_node_proof_rejects_terminal_sub_agent_history(db_session):
    await _bind_worker(db_session)
    owner = Task(
        id=TASK_ID_WORKER_NAMESPACE_START,
        title="terminal Worker-local child",
        status="completed",
    )
    db_session.add(owner)
    await db_session.flush()
    session = SubAgentSession(
        task_id=owner.id,
        agent_type="monitor",
        source="ccm",
        description="terminal history still belongs to the user",
        status="completed",
        codex_cleanup_pending=False,
    )
    db_session.add(session)
    await db_session.flush()
    db_session.add(
        SubAgentReport(
            session_id=session.id,
            check_number=1,
            status="completed",
            summary="user-visible report",
            full_output="full report",
        )
    )
    await db_session.commit()

    proof = await build_worker_node_drain_proof(
        db_session,
        nonce="4" * 32,
    )

    kinds = {blocker["kind"] for blocker in proof["blockers"]}
    assert proof["safe_to_destroy"] is False
    assert "unmigrated_sub_agent_audit" in kinds
    assert "unmigrated_sub_agent_report" in kinds


async def test_node_proof_rejects_terminal_capability_history(db_session):
    await _bind_worker(db_session)
    owner = Task(
        id=TASK_ID_WORKER_NAMESPACE_START,
        title="terminal capability owner",
        status="completed",
    )
    db_session.add(owner)
    await db_session.flush()
    digest = "a" * 64
    invocation = CapabilityInvocation(
        task_id=owner.id,
        capability_key="plan",
        source="human_request",
        purpose="advisory",
        status="failed",
        idempotency_key="terminal-capability-history",
        input_payload={},
        input_hash=digest,
        subject_kind="task",
        subject_ref={"task_id": owner.id},
        subject_hash=digest,
        executor_kind="plan",
        executor_config={},
        executor_config_hash=digest,
        policy_snapshot={},
        policy_hash=digest,
        resume_policy="attach_only",
        error_code="test_failure",
        error_message="terminal audit",
    )
    db_session.add(invocation)
    await db_session.flush()
    db_session.add(
        CapabilityExecution(
            invocation_id=invocation.id,
            attempt=1,
            status="failed",
            idempotency_key="terminal-capability-execution",
            executor_kind="plan",
            input_hash=digest,
            error_code="test_failure",
            error_message="terminal execution audit",
        )
    )
    created_at = datetime.utcnow() - timedelta(seconds=1)
    db_session.add(
        CapabilityResumeOutbox(
            task_id=owner.id,
            invocation_id=invocation.id,
            status="failed",
            request_task_incarnation_id=owner.incarnation_id,
            request_task_retry_count=owner.retry_count,
            from_turn_generation=owner.turn_generation,
            request_source_log_id=1,
            request_output_log_id=2,
            request_terminal_log_id=3,
            request_execution_user_role="member",
            request_execution_mode="sandbox",
            request_execution_principal_kind="system",
            error_code="test_failure",
            error_message="terminal resume audit",
            created_at=created_at,
            completed_at=datetime.utcnow(),
        )
    )
    await db_session.commit()

    proof = await build_worker_node_drain_proof(
        db_session,
        nonce="5" * 32,
    )

    kinds = {blocker["kind"] for blocker in proof["blockers"]}
    assert proof["safe_to_destroy"] is False
    assert "unmigrated_capability_invocation" in kinds
    assert "unmigrated_capability_execution" in kinds
    assert "unmigrated_capability_resume" in kinds


async def test_node_proof_rejects_terminal_task_ssh_effect_audit(db_session):
    await _bind_worker(db_session)
    owner = Task(
        id=TASK_ID_WORKER_NAMESPACE_START,
        title="terminal SSH effect owner",
        status="completed",
    )
    db_session.add(owner)
    await db_session.flush()
    db_session.add(
        TaskSSHEffectReceipt(
            effect_id="e" * 32,
            task_id=owner.id,
            task_incarnation_id=owner.incarnation_id,
            task_retry_count=owner.retry_count,
            task_turn_generation=owner.turn_generation,
            task_status=owner.status,
            profile_id=1,
            profile_revision=1,
            operation="execute",
            request_digest="f" * 64,
            status="aborted",
            outcome_code="cancelled_before_execution",
            completed_at=datetime.utcnow(),
        )
    )
    await db_session.commit()

    proof = await build_worker_node_drain_proof(
        db_session,
        nonce="6" * 32,
    )

    assert proof["safe_to_destroy"] is False
    assert any(
        blocker["kind"] == "unmigrated_task_ssh_effect_audit"
        for blocker in proof["blockers"]
    )


async def test_node_proof_rejects_unfinished_task_migration_operation(
    db_session,
):
    """An FK-free migration owner must survive and block node destruction."""

    await _bind_worker(db_session)
    operation_id = "b" * 32
    db_session.add(
        TaskMigrationOperation(
            operation_id=operation_id,
            operation_sequence=1,
            side="manager",
            active_task_id=17,
            task_id=17,
            task_incarnation_id="a" * 32,
            retry_count=0,
            turn_generation=0,
            source_worker_id=None,
            target_worker_id=5,
            source_status="failed",
            phase="rollback_pending",
            instance_id=None,
            started_at=None,
            completed_at=None,
        )
    )
    await db_session.commit()

    proof = await build_worker_node_drain_proof(
        db_session,
        nonce="8" * 32,
    )

    assert proof["safe_to_destroy"] is False
    assert any(
        blocker == {
            "kind": "task_migration_operation",
            "id": operation_id,
            "detail": (
                "task_id=17, sequence=1, side=manager, "
                "phase=rollback_pending, "
                "source_worker_id=None, target_worker_id=5"
            ),
        }
        for blocker in proof["blockers"]
    )


async def test_empty_bound_worker_returns_signed_clean_proof(db_session):
    await _bind_worker(db_session)
    proof = await build_worker_node_drain_proof(
        db_session,
        nonce="1" * 32,
    )
    assert proof["safe_to_destroy"] is True
    token = "worker-secret"
    signature = worker_node_drain_proof_signature(proof, auth_token=token)
    assert verify_worker_node_drain_proof_signature(
        proof,
        auth_token=token,
        signature=signature,
    )
    tampered = dict(proof, safe_to_destroy=False)
    assert not verify_worker_node_drain_proof_signature(
        tampered,
        auth_token=token,
        signature=signature,
    )


async def test_restart_recovery_keeps_accepted_high_range_handoff_blocking(
    db_session,
    db_factory,
    monkeypatch,
):
    """A terminal Worker Task is not clean while its G+1 outbox can recover."""

    from backend.services.dispatcher import GlobalDispatcher

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    await _bind_worker(db_session)
    task = Task(
        id=TASK_ID_WORKER_NAMESPACE_START,
        title="terminal Worker handoff owner",
        status="completed",
        session_id="terminal-worker-handoff-session",
        retry_count=2,
        turn_generation=8,
    )
    db_session.add(task)
    await db_session.commit()
    await db_session.refresh(task)
    handoff_id = "6" * 32
    source, _receipt = await _seed_accepted_worker_handoff(
        db_session,
        task,
        handoff_id=handoff_id,
    )
    source_id = source.id

    # A freshly constructed Dispatcher models process restart. Its durable
    # recovery selector must still find and re-admit this exact accepted row.
    instance_manager = MagicMock()
    dispatcher = GlobalDispatcher(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=MagicMock(),
        test_harness_service=MagicMock(),
    )
    dispatcher._ensure_queue_worker = MagicMock()
    await dispatcher._recover_worker_turn_handoff_outbox()
    assert dispatcher._get_task_queue(task.id).qsize() == 1
    assert handoff_id in dispatcher._queued_worker_turn_handoffs

    blocked = await build_worker_node_drain_proof(
        db_session,
        nonce="6" * 32,
    )
    assert blocked["safe_to_destroy"] is False
    assert any(
        blocker == {
            "kind": "worker_turn_handoff_unsettled",
            "id": handoff_id,
            "detail": (
                f"task_id={task.id}, side=worker, status=accepted, "
                f"source_log_id={source_id}"
            ),
        }
        for blocker in blocked["blockers"]
    )

    # Existing exact cleanup may settle ownership after drain begins. It takes
    # the same node-first resolution fence, then the Task and receipt, before
    # making the receipt terminal.
    await db_session.rollback()
    assert await fence_worker_node_receipt_resolution(db_session) is True
    locked_task = (
        await db_session.execute(
            select(Task).where(Task.id == task.id).with_for_update()
        )
    ).scalar_one()
    assert locked_task.incarnation_id == task.incarnation_id
    receipt = (
        await db_session.execute(
            select(WorkerTurnHandoffReceipt)
            .where(WorkerTurnHandoffReceipt.handoff_id == handoff_id)
            .with_for_update()
        )
    ).scalar_one()
    receipt.status = "cancelled"
    receipt.claimed_turn_generation = None
    receipt.cancel_reason = "settled during Worker drain recovery"
    await db_session.commit()

    clean = await _seal_and_build_worker_node_drain_proof(
        db_session,
        nonce="7" * 32,
        drain_claim=_DRAIN_CLAIM,
    )
    assert clean["safe_to_destroy"] is True


async def test_pending_terminal_publication_blocks_drain_until_recovered(
    db_session,
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    await _bind_worker(db_session)
    completed_at = datetime.utcnow().replace(microsecond=0)
    task = Task(
        id=TASK_ID_WORKER_NAMESPACE_START,
        title="worker-local terminal publication",
        status="completed",
        session_id="drain-publication-session",
        completed_at=completed_at,
    )
    db_session.add(task)
    await db_session.flush()
    publication = LogEntry(
        task_id=task.id,
        task_retry_count=task.retry_count,
        task_turn_generation=task.turn_generation,
        native_turn_id="drain-publication-generation",
        event_type=PTY_TERMINAL_PUBLICATION_EVENT_TYPE,
        role=None,
        content=None,
        raw_json=build_pty_terminal_publication_payload(
            task_id=task.id,
            incarnation_id=task.incarnation_id,
            retry_count=task.retry_count,
            turn_generation=task.turn_generation,
            session_id=task.session_id,
            source_background_generation="drain-publication-generation",
            status=task.status,
            instance_id=None,
            started_at=None,
            completed_at=completed_at,
        ),
        is_error=False,
    )
    db_session.add(publication)
    await db_session.commit()
    publication_id = publication.id

    blocked = await build_worker_node_drain_proof(
        db_session,
        nonce="2" * 32,
    )
    assert blocked["safe_to_destroy"] is False
    assert any(
        blocker["kind"] == "task_event_publication_pending"
        and blocker["id"] == publication_id
        for blocker in blocked["blockers"]
    )

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    manager = InstanceManager(db_factory, broadcaster)
    assert await manager.recover_pty_terminal_publications() == (1, 0)
    assert broadcaster.broadcast.await_count == 4
    async with db_factory() as db:
        assert await db.get(LogEntry, publication_id) is None

    clean = await _seal_and_build_worker_node_drain_proof(
        db_session,
        nonce="3" * 32,
        drain_claim=_DRAIN_CLAIM,
    )
    assert clean["safe_to_destroy"] is True


async def test_malformed_terminal_publication_fails_drain_closed(db_session):
    await _bind_worker(db_session)
    publication = LogEntry(
        task_id=None,
        task_retry_count=None,
        task_turn_generation=None,
        native_turn_id=None,
        event_type=PTY_TERMINAL_PUBLICATION_EVENT_TYPE,
        role=None,
        content=None,
        raw_json="{}",
        is_error=False,
    )
    db_session.add(publication)
    await db_session.commit()
    publication_id = publication.id

    proof = await build_worker_node_drain_proof(
        db_session,
        nonce="4" * 32,
    )

    assert proof["safe_to_destroy"] is False
    assert any(
        blocker["kind"] == "task_event_publication_pending"
        and blocker["id"] == publication_id
        for blocker in proof["blockers"]
    )


async def test_node_proof_blocks_active_project_materialization(db_session):
    await _bind_worker(db_session)
    active_statuses = ("pending", "cloning", "initializing")
    db_session.add_all([
        Project(
            name=f"active-project-{status}",
            local_path=f"/workspace/active-project-{status}",
            status=status,
        )
        for status in active_statuses
    ])
    db_session.add_all((
        Project(
            name="ready-project-cache",
            local_path="/workspace/ready-project-cache",
            status="ready",
        ),
        Project(
            name="failed-project-cache",
            local_path="/workspace/failed-project-cache",
            status="error",
        ),
    ))
    await db_session.commit()

    proof = await build_worker_node_drain_proof(
        db_session,
        nonce="a" * 32,
    )

    assert proof["safe_to_destroy"] is False
    blockers = [
        blocker
        for blocker in proof["blockers"]
        if blocker["kind"] == "project_materialization_active"
    ]
    assert len(blockers) == len(active_statuses)
    assert {
        blocker["detail"].split("status=", 1)[1]
        for blocker in blockers
    } == set(active_statuses)


async def test_clean_proof_rejects_late_worker_local_harness_run(
    db_session,
    db_factory,
    monkeypatch,
):
    """A terminal high-range owner cannot materialize Harness after drain."""

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-drain-test-token")
    await _bind_worker(db_session)
    owner = Task(
        id=TASK_ID_WORKER_NAMESPACE_START,
        title="terminal Worker-local Harness owner",
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
    )
    db_session.add(owner)
    await db_session.commit()
    await db_session.refresh(owner)
    owner_identity = _test_harness_owner_identity(owner)

    proof = await build_worker_node_drain_proof(
        db_session,
        nonce="7" * 32,
    )
    assert proof["safe_to_destroy"] is True

    service = _HarnessService(db_factory=db_factory)
    spec = _HarnessSpec(
        target_kind="fixed_url",
        target={"url": "https://example.com"},
        goal="prove the late materialization is rejected",
        allow_actions=False,
        max_actions=0,
    )
    with pytest.raises(HTTPException) as blocked:
        await service._create_run(
            task_id=owner.id,
            project_id=None,
            owner_user_id=None,
            spec=spec,
            plan={"version": 1, "scenarios": []},
            runtime={},
            owner_identity=owner_identity,
        )
    assert blocked.value.status_code == 409
    async with db_factory() as db:
        assert await db.scalar(
            select(HarnessRun.id).where(HarnessRun.task_id == owner.id)
        ) is None


async def test_node_proof_rejects_unbound_database(db_session):
    role = await db_session.scalar(
        select(TaskIdAllocator.node_role).where(
            TaskIdAllocator.id == TASK_ID_ALLOCATOR_SINGLETON_ID
        )
    )
    assert role is None
    with pytest.raises(RuntimeError, match="durably bound as worker"):
        await build_worker_node_drain_proof(db_session, nonce="2" * 32)


async def test_node_proof_waits_for_inflight_low_mirror_insert(
    tmp_path,
    monkeypatch,
):
    """A low-id Manager POST cannot commit behind a clean drain snapshot."""

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    database_path = tmp_path / "worker-drain-proof.sqlite3"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database_path}",
        connect_args={"timeout": 10},
    )
    factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    insert_holds_fence = asyncio.Event()
    allow_insert_commit = asyncio.Event()

    async def insert_manager_mirror() -> None:
        async with factory() as db:
            await stage_task_record(
                db,
                id=73,
                title="in-flight Manager mirror",
                description="must be visible to the later drain proof",
                metadata_={WORKER_MANAGED_TASK_METADATA_KEY: True},
            )
            insert_holds_fence.set()
            await allow_insert_commit.wait()
            await db.commit()

    async def take_proof() -> dict:
        async with factory() as db:
            return await build_worker_node_drain_proof(
                db,
                nonce="3" * 32,
            )

    insert_task = None
    proof_task = None
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with factory() as db:
            await _bind_worker(db)

        insert_task = asyncio.create_task(insert_manager_mirror())
        await asyncio.wait_for(insert_holds_fence.wait(), timeout=5)
        proof_task = asyncio.create_task(take_proof())

        # The proof's allocator UPDATE must wait on the uncommitted low-id
        # insert.  Without the shared fence it can return a false clean result.
        await asyncio.sleep(0.05)
        assert not proof_task.done()

        allow_insert_commit.set()
        await asyncio.wait_for(insert_task, timeout=5)
        proof = await asyncio.wait_for(proof_task, timeout=5)

        assert proof["safe_to_destroy"] is False
        assert proof["task_count"] == 1
        assert any(
            blocker["kind"] == "task_nonterminal"
            and blocker["id"] == 73
            for blocker in proof["blockers"]
        )
    finally:
        allow_insert_commit.set()
        for task in (insert_task, proof_task):
            if task is not None and not task.done():
                task.cancel()
        if insert_task is not None or proof_task is not None:
            await asyncio.gather(
                *(task for task in (insert_task, proof_task) if task is not None),
                return_exceptions=True,
            )
        await engine.dispose()


async def test_worker_chat_and_drain_claim_serialize_in_wal(
    tmp_path,
    monkeypatch,
):
    """The node fence gives chat admission and drain one durable winner."""

    import backend.api.chat as chat_api
    import backend.main as main_module

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    dispatcher = SimpleNamespace(
        enqueue_worker_turn_handoff=AsyncMock(return_value=True),
    )
    broadcaster = SimpleNamespace(broadcast=AsyncMock())
    monkeypatch.setattr(main_module, "dispatcher", dispatcher)
    monkeypatch.setattr(main_module, "broadcaster", broadcaster)

    async def create_database(name: str, task_id: int):
        engine = create_async_engine(
            f"sqlite+aiosqlite:///{tmp_path / name}",
            connect_args={"timeout": 10},
        )
        async with engine.connect() as connection:
            mode = await connection.scalar(text("PRAGMA journal_mode=WAL"))
            await connection.commit()
        assert str(mode).lower() == "wal"
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        async with factory() as db:
            await _bind_worker(db)
            task = Task(
                id=task_id,
                title="Worker WAL chat admission",
                status="completed",
                session_id=f"worker-wal-session-{task_id}",
                retry_count=2,
                turn_generation=8,
            )
            db.add(task)
            await db.commit()
            await db.refresh(task)
        return engine, factory, task

    def request_for(task: Task):
        return SimpleNamespace(
            state=SimpleNamespace(
                user_id=None,
                user_role="super_admin",
                auth_type="token",
            ),
            headers={"x-ccm-task-incarnation": task.incarnation_id},
        )

    def handoff_body(task: Task, handoff_id: str):
        return chat_api.ChatMessage(
            message="admit this exact Worker follow-up",
            worker_turn_handoff_id=handoff_id,
            worker_turn_handoff_retry_count=task.retry_count,
            worker_turn_handoff_from_generation=task.turn_generation,
            worker_turn_handoff_incarnation_id=task.incarnation_id,
            execution_user_id=None,
            execution_user_role="member",
            execution_mode="sandbox",
            execution_principal_kind="system",
        )

    async def send_chat(factory, task: Task, handoff_id: str):
        async with factory() as db:
            return await chat_api.send_chat_message(
                task.id,
                handoff_body(task, handoff_id),
                request_for(task),
                db,
            )

    async def install_drain(factory, claim: str):
        async with factory() as db:
            await begin_worker_node_drain(db, claim=claim)
            await db.commit()

    final_fence_held = asyncio.Event()
    release_chat = asyncio.Event()
    pause_final_fence = True
    original_lock = chat_api.lock_task_effect_access

    async def hold_final_node_fence(*args, **kwargs):
        result = await original_lock(*args, **kwargs)
        if kwargs.get("fence_worker_node") and pause_final_fence:
            final_fence_held.set()
            await release_chat.wait()
        return result

    monkeypatch.setattr(
        chat_api,
        "lock_task_effect_access",
        hold_final_node_fence,
    )

    engines = []
    chat_task = None
    drain_task = None
    try:
        # Chat wins: its node-first transaction remains open through both the
        # user LogEntry and accepted receipt commit. Drain must wait, then its
        # proof sees that durable recoverable owner instead of returning clean.
        engine, factory, task = await create_database(
            "worker-chat-wins.sqlite3",
            TASK_ID_WORKER_NAMESPACE_START,
        )
        engines.append(engine)
        handoff_id = "8" * 32
        chat_task = asyncio.create_task(send_chat(factory, task, handoff_id))
        await asyncio.wait_for(final_fence_held.wait(), timeout=5)
        drain_task = asyncio.create_task(install_drain(factory, "8" * 64))
        await asyncio.sleep(0.05)
        assert not drain_task.done()

        release_chat.set()
        response = await asyncio.wait_for(chat_task, timeout=5)
        await asyncio.wait_for(drain_task, timeout=5)
        assert response["queued"] is True

        async with factory() as db:
            proof = await _seal_and_build_worker_node_drain_proof(
                db,
                nonce="8" * 32,
                drain_claim="8" * 64,
            )
            receipt = await db.get(WorkerTurnHandoffReceipt, handoff_id)
            logs = list(
                (
                    await db.execute(
                        select(LogEntry).where(
                            LogEntry.task_id == task.id,
                            LogEntry.event_type == "user_message",
                        )
                    )
                ).scalars()
            )
        assert receipt is not None and receipt.status == "accepted"
        assert len(logs) == 1
        assert proof["safe_to_destroy"] is False
        assert any(
            blocker["kind"] == "worker_turn_handoff_unsettled"
            and blocker["id"] == handoff_id
            for blocker in proof["blockers"]
        )

        # Drain wins: the same final admission fence rejects chat before either
        # half of its durable outbox exists.
        pause_final_fence = False
        engine, factory, task = await create_database(
            "worker-drain-wins.sqlite3",
            TASK_ID_WORKER_NAMESPACE_START + 1,
        )
        engines.append(engine)
        await install_drain(factory, "a" * 64)
        losing_handoff_id = "a" * 32
        with pytest.raises(HTTPException) as blocked:
            await send_chat(factory, task, losing_handoff_id)
        assert blocked.value.status_code == 409
        assert "destruction has begun" in str(blocked.value.detail)
        async with factory() as db:
            assert await db.get(
                WorkerTurnHandoffReceipt,
                losing_handoff_id,
            ) is None
            assert await db.scalar(
                select(LogEntry.id).where(
                    LogEntry.task_id == task.id,
                    LogEntry.event_type == "user_message",
                )
            ) is None
            clean = await _seal_and_build_worker_node_drain_proof(
                db,
                nonce="a" * 32,
                drain_claim="a" * 64,
            )
        assert clean["safe_to_destroy"] is True
    finally:
        release_chat.set()
        for pending in (chat_task, drain_task):
            if pending is not None and not pending.done():
                pending.cancel()
        if chat_task is not None or drain_task is not None:
            await asyncio.gather(
                *(
                    pending
                    for pending in (chat_task, drain_task)
                    if pending is not None
                ),
                return_exceptions=True,
            )
        for engine in engines:
            await engine.dispose()


async def test_destroy_log_backfill_rejects_missing_tail_or_relay_failure(
    db_factory,
):
    async with db_factory() as db:
        worker = Worker(
            name="destroy-log-proof",
            status="destroying",
            private_ip="10.0.0.8",
            auth_token="worker-secret",
            destroy_lifecycle_nonce="d" * 32,
        )
        db.add(worker)
        await db.flush()
        task = Task(
            title="terminal Worker Task",
            status="completed",
            worker_id=worker.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(worker)
        await db.refresh(task)
        claim = capture_worker_destroy_lifecycle_claim(worker)
        task_id = task.id

    relay = AsyncMock()
    proxy = WorkerProxy(db_factory, relay)

    # A successful HTTP history request which cannot prove/import this exact
    # generation returns no id; this models a relay-disconnect tail gap.
    relay._backfill_missing_logs.return_value = set()
    with pytest.raises(HTTPException) as incomplete:
        await proxy.require_claimed_destroy_log_backfill(
            claim,
            {task_id},
        )
    assert incomplete.value.status_code == 409
    assert str(task_id) in incomplete.value.detail

    relay._backfill_missing_logs.side_effect = OSError("relay disconnected")
    with pytest.raises(HTTPException) as disconnected:
        await proxy.require_claimed_destroy_log_backfill(
            claim,
            {task_id},
        )
    assert disconnected.value.status_code == 503

    relay._backfill_missing_logs.side_effect = None
    relay._backfill_missing_logs.return_value = {task_id}
    await proxy.require_claimed_destroy_log_backfill(claim, {task_id})
