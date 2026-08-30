"""Runtime fences for the durable Manager/Worker termination protocol."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import backend.main as main_module
from backend.database import Base
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.models.worker import Worker
from backend.models.worker_turn_handoff import WorkerTurnHandoffReceipt
from backend.services import worker_task_termination as termination
from backend.services import worker_relay


def _request_payload(
    task_id: int,
    operation_id: str,
    *,
    status: str = "pending",
    retry_count: int = 0,
    turn_generation: int = 0,
    manager_handoff: dict | None = None,
) -> dict:
    return {
        "version": 2,
        "operation_id": operation_id,
        "task_id": task_id,
        "operation": "cancel",
        "manager_worker_id": 41,
        "expected_remote": {
            "status": status,
            "retry_count": retry_count,
            "turn_generation": turn_generation,
        },
        "manager_handoff": manager_handoff,
    }


def _validate(payload: dict) -> dict:
    return termination._validate_request_payload(
        task_id=payload.get("task_id", 1),
        operation_id=payload.get("operation_id", "a" * 32),
        operation=payload.get("operation", "cancel"),
        payload=payload,
        digest=termination.canonical_json_digest(payload),
    )


def _worker_rejection(manager, *, error: str = "exact generation changed") -> dict:
    result = {
        "version": 2,
        "operation_id": manager.operation_id,
        "task_id": manager.task_id,
        "operation": manager.operation,
        "request_digest": manager.request_digest,
        "rejected": True,
        "error": error,
    }
    return {
        "version": 2,
        "operation_id": manager.operation_id,
        "task_id": manager.task_id,
        "side": "worker",
        "worker_id": None,
        "operation": manager.operation,
        "status": "rejected",
        "state_version": 1,
        "source": {
            "incarnation_id": "1" * 32,
            "status": manager.source_task_status,
            "retry_count": manager.source_task_retry_count,
            "turn_generation": manager.source_task_turn_generation,
            "source_log_id": None,
            "instance_id": None,
            "started_at": None,
            "completed_at": None,
            "session_id": None,
            "pty_background_generation": None,
        },
        "request_payload": manager.request_payload,
        "request_digest": manager.request_digest,
        "result_payload": result,
        "result_digest": termination.canonical_json_digest(result),
        "attempt_count": 0,
        "reconcile_count": 0,
        "last_error": error,
        "accepted_at": "2026-01-02T03:04:05.000000",
        "completed_at": "2026-01-02T03:04:05.000000",
        "ack_intent_at": None,
        "acknowledged_at": None,
        "created_at": "2026-01-02T03:04:05.000000",
        "updated_at": "2026-01-02T03:04:05.000000",
    }


def _success_result_payload(
    request_payload: dict,
    *,
    retry_count: int,
    turn_generation: int,
) -> tuple[dict, dict]:
    task_id = request_payload["task_id"]
    result = {
        "version": 2,
        "operation_id": request_payload["operation_id"],
        "task_id": task_id,
        "operation": request_payload["operation"],
        "request_digest": termination.canonical_json_digest(request_payload),
        "task": {
            "id": task_id,
            "status": "completed",
            "retry_count": retry_count,
            "turn_generation": turn_generation,
            "instance_id": None,
            "started_at": None,
            "completed_at": "2026-01-02T03:04:05.000000",
            "session_id": None,
            "error_message": None,
            "background_active": False,
        },
        "response": {"ok": True, "recovered": True},
    }
    wire = {
        "operation_id": request_payload["operation_id"],
        "task_id": task_id,
        "operation": request_payload["operation"],
        "request_payload": request_payload,
        "request_digest": termination.canonical_json_digest(request_payload),
    }
    return result, wire


@pytest.mark.parametrize(
    "mutate",
    (
        lambda value: value.update(extra=True),
        lambda value: value.pop("manager_handoff"),
        lambda value: value["expected_remote"].update(extra=True),
        lambda value: value["expected_remote"].pop("status"),
        lambda value: value["expected_remote"].update(status="unknown"),
        lambda value: value["expected_remote"].update(retry_count=True),
        lambda value: value["expected_remote"].update(turn_generation=True),
        lambda value: value.update(manager_worker_id=True),
    ),
)
def test_request_wire_shape_rejects_extra_missing_unknown_and_bool(mutate):
    payload = _request_payload(1, "a" * 32)
    mutate(payload)

    with pytest.raises(termination.WorkerTaskTerminationConflict):
        _validate(payload)


def test_request_wire_shape_requires_complete_local_manager_handoff():
    handoff = {
        "handoff_id": "b" * 32,
        "worker_id": 41,
        "retry_count": 3,
        "from_generation": 7,
        # This is a Manager-local LogEntry id.  It is deliberately not a
        # portable identity and is never compared with a Worker LogEntry id.
        "source_log_id": 9001,
        "acknowledged": False,
    }
    payload = _request_payload(
        1,
        "a" * 32,
        retry_count=3,
        turn_generation=7,
        manager_handoff=handoff,
    )
    assert _validate(payload) == payload

    invalid = []
    for field in handoff:
        candidate = deepcopy(payload)
        candidate["manager_handoff"].pop(field)
        invalid.append(candidate)
    for field, value in (
        ("extra", 1),
        ("worker_id", 42),
        ("retry_count", True),
        ("from_generation", True),
        ("source_log_id", True),
        ("acknowledged", 1),
    ):
        candidate = deepcopy(payload)
        candidate["manager_handoff"][field] = value
        invalid.append(candidate)
    candidate = deepcopy(payload)
    candidate["expected_remote"]["turn_generation"] = 9
    invalid.append(candidate)

    for candidate in invalid:
        with pytest.raises(termination.WorkerTaskTerminationConflict):
            _validate(candidate)


def test_request_wire_shape_rejects_non_hex_digest():
    payload = _request_payload(1, "a" * 32)
    with pytest.raises(termination.WorkerTaskTerminationConflict):
        termination._validate_request_payload(
            task_id=1,
            operation_id="a" * 32,
            operation="cancel",
            payload=payload,
            digest="G" * 64,
        )


def test_success_result_generation_is_bound_to_exact_handoff_identity():
    operation_id = "b" * 32
    without_handoff = _request_payload(
        1,
        operation_id,
        retry_count=3,
        turn_generation=7,
    )
    source_g, wire = _success_result_payload(
        without_handoff,
        retry_count=3,
        turn_generation=7,
    )
    assert termination._valid_result_payload_identity(source_g, wire)
    borrowed_g1 = deepcopy(source_g)
    borrowed_g1["task"]["turn_generation"] = 8
    assert not termination._valid_result_payload_identity(borrowed_g1, wire)

    handoff = {
        "handoff_id": "c" * 32,
        "worker_id": 41,
        "retry_count": 3,
        "from_generation": 7,
        "source_log_id": 9001,
        "acknowledged": False,
    }
    with_handoff = _request_payload(
        1,
        operation_id,
        retry_count=3,
        turn_generation=7,
        manager_handoff=handoff,
    )
    for resulting_generation in (7, 8):
        resulting, result_wire = _success_result_payload(
            with_handoff,
            retry_count=3,
            turn_generation=resulting_generation,
        )
        assert termination._valid_result_payload_identity(
            resulting,
            result_wire,
        )
    wrong_generation, result_wire = _success_result_payload(
        with_handoff,
        retry_count=3,
        turn_generation=9,
    )
    assert not termination._valid_result_payload_identity(
        wrong_generation,
        result_wire,
    )
    wrong_retry = deepcopy(wrong_generation)
    wrong_retry["task"]["turn_generation"] = 8
    wrong_retry["task"]["retry_count"] = 4
    assert not termination._valid_result_payload_identity(
        wrong_retry,
        result_wire,
    )


@pytest.mark.asyncio
async def test_same_operation_id_with_different_digest_is_rejected(db_session):
    task = Task(title="same id", status="pending", retry_count=0)
    db_session.add(task)
    await db_session.commit()
    operation_id = "c" * 32
    payload = _request_payload(task.id, operation_id)
    await termination.stage_worker_receipt(
        db_session,
        task_id=task.id,
        operation_id=operation_id,
        operation="cancel",
        request_payload=payload,
        request_digest=termination.canonical_json_digest(payload),
    )

    changed = deepcopy(payload)
    changed["expected_remote"]["status"] = "in_progress"
    with pytest.raises(termination.WorkerTaskTerminationConflict):
        await termination.stage_worker_receipt(
            db_session,
            task_id=task.id,
            operation_id=operation_id,
            operation="cancel",
            request_payload=changed,
            request_digest=termination.canonical_json_digest(changed),
        )


@pytest.mark.asyncio
async def test_stop_session_rejects_merging_before_receipt_side_effects(db_session):
    task = Task(title="merging stop", status="merging", retry_count=1)
    db_session.add(task)
    await db_session.commit()
    operation_id = "6" * 32
    payload = _request_payload(
        task.id,
        operation_id,
        status="merging",
        retry_count=1,
    )
    payload["operation"] = "stop_session"
    digest = termination.canonical_json_digest(payload)

    with pytest.raises(termination.WorkerTaskTerminationConflict):
        await termination.stage_worker_receipt(
            db_session,
            task_id=task.id,
            operation_id=operation_id,
            operation="stop_session",
            request_payload=payload,
            request_digest=digest,
        )
    await db_session.rollback()
    assert (
        await db_session.get(
            termination.WorkerTaskTerminationReceipt,
            operation_id,
        )
        is None
    )


@pytest.mark.asyncio
async def test_manager_rejects_new_receipt_from_migrating_task(db_session):
    worker = Worker(name="migrating-manager-worker", status="ready")
    db_session.add(worker)
    await db_session.flush()
    task = Task(
        title="migration already owns task",
        status="migrating",
        worker_id=worker.id,
        retry_count=2,
        turn_generation=5,
    )
    db_session.add(task)
    await db_session.commit()
    task_id = task.id

    with pytest.raises(
        termination.WorkerTaskTerminationConflict,
        match="cannot run cancel from migrating",
    ):
        await termination.create_or_resume_manager_receipt(
            db_session,
            task,
            operation="cancel",
        )
    await db_session.rollback()

    receipts = list(
        (
            await db_session.execute(
                select(termination.WorkerTaskTerminationReceipt).where(
                    termination.WorkerTaskTerminationReceipt.task_id == task_id
                )
            )
        ).scalars()
    )
    assert receipts == []


@pytest.mark.asyncio
async def test_manager_resumes_exact_legacy_receipt_from_migrating_task(
    db_session,
):
    """The new source gate must not strand an operation admitted by old code."""

    worker = Worker(name="legacy-migrating-receipt-worker", status="ready")
    db_session.add(worker)
    await db_session.flush()
    task = Task(
        title="legacy migrating receipt",
        status="migrating",
        worker_id=worker.id,
        retry_count=3,
        turn_generation=7,
    )
    db_session.add(task)
    await db_session.flush()
    operation_id = "e" * 32
    request_payload = termination._manager_request_payload(
        task,
        operation_id=operation_id,
        operation="cancel",
    )
    now = datetime.utcnow()
    receipt = termination.WorkerTaskTerminationReceipt(
        operation_id=operation_id,
        task_id=task.id,
        active_task_id=task.id,
        side="manager",
        worker_id=worker.id,
        operation="cancel",
        status="pending_remote",
        state_version=1,
        request_payload=request_payload,
        request_digest=termination.canonical_json_digest(request_payload),
        attempt_count=0,
        reconcile_count=0,
        next_reconcile_at=now,
        created_at=now,
        updated_at=now,
        **termination._task_source_values(task, manager_side=True),
    )
    db_session.add(receipt)
    await db_session.commit()

    resumed = await termination.create_or_resume_manager_receipt(
        db_session,
        task,
        operation="cancel",
    )

    assert resumed.operation_id == operation_id
    assert resumed.status == "pending_remote"
    assert not db_session.in_transaction()


@pytest.mark.asyncio
async def test_destroying_worker_requires_exact_opaque_claim_for_new_receipt(
    db_session,
):
    from backend.services.worker_proxy import (
        capture_worker_destroy_lifecycle_claim,
    )

    worker = Worker(
        name="destroy-claimed",
        status="destroying",
        destroy_lifecycle_nonce="d" * 32,
    )
    db_session.add(worker)
    await db_session.flush()
    task = Task(
        title="destroy stop",
        status="executing",
        worker_id=worker.id,
        retry_count=1,
        turn_generation=2,
    )
    db_session.add(task)
    await db_session.commit()
    task_id = task.id
    worker_id = worker.id
    claim = capture_worker_destroy_lifecycle_claim(worker)

    with pytest.raises(termination.WorkerTaskTerminationConflict):
        await termination.create_or_resume_manager_receipt(
            db_session,
            task,
            operation="stop_session",
        )
    await db_session.rollback()
    task = await db_session.get(Task, task_id)
    admitted = await termination.create_or_resume_manager_receipt(
        db_session,
        task,
        operation="stop_session",
        destroy_claim=claim,
    )
    assert admitted.status == "pending_remote"
    assert admitted.worker_id == worker_id


@pytest.mark.asyncio
async def test_preflight_rejection_stays_gated_until_digest_ack(db_session):
    task = Task(
        title="Worker preflight rejection",
        status="pending",
        retry_count=0,
        turn_generation=0,
    )
    db_session.add(task)
    await db_session.commit()
    operation_id = "5" * 32
    # A valid request whose logical generation does not match this Worker Task
    # is a provable pre-side-effect rejection.
    payload = _request_payload(
        task.id,
        operation_id,
        status="executing",
    )
    digest = termination.canonical_json_digest(payload)
    rejected = await termination.persist_worker_preflight_rejection(
        db_session,
        task_id=task.id,
        operation_id=operation_id,
        operation="cancel",
        request_payload=payload,
        request_digest=digest,
        error="exact generation changed",
    )
    assert rejected is not None
    assert rejected.status == "rejected"
    assert rejected.active_task_id == task.id
    assert termination.serialize_receipt(rejected)["status"] == "rejected"

    # The rejected request caused no effect, but a process which already owned
    # this same retry/turn may still finish before the Manager ACK arrives.
    task.status = "completed"
    task.completed_at = datetime.utcnow()
    task.session_id = "existing-process-finished"
    await db_session.commit()

    acknowledged = await termination.acknowledge_worker_receipt(
        db_session,
        task_id=task.id,
        operation_id=operation_id,
        request_digest=digest,
        result_digest=rejected.result_digest,
    )
    assert acknowledged.status == "acknowledged"
    assert acknowledged.active_task_id is None
    assert acknowledged.acknowledged_at is not None


@pytest.mark.asyncio
async def test_manager_rejection_requires_ack_intent_before_deleted_task_proof(
    db_session,
):
    worker = Worker(name="rejection-manager-worker", status="ready")
    db_session.add(worker)
    await db_session.flush()
    task = Task(
        title="Manager rejection ACK",
        status="pending",
        worker_id=worker.id,
        retry_count=0,
        turn_generation=0,
    )
    db_session.add(task)
    await db_session.commit()
    manager = await termination.create_or_resume_manager_receipt(
        db_session,
        task,
        operation="cancel",
    )
    task_id = task.id
    operation_id = manager.operation_id
    remote = _worker_rejection(manager)
    tampered = deepcopy(remote)
    tampered["result_payload"]["operation_id"] = "7" * 32
    tampered["result_digest"] = termination.canonical_json_digest(
        tampered["result_payload"]
    )
    assert not termination._valid_remote_receipt(tampered, manager)
    unknown_status = deepcopy(remote)
    unknown_status["status"] = "future_state"
    assert not termination._valid_remote_receipt(unknown_status, manager)
    awaiting = await termination.reject_manager_receipt(
        db_session,
        operation_id,
        remote,
    )
    assert awaiting.status == "awaiting_ack"
    assert awaiting.active_task_id == task_id
    assert awaiting.ack_intent_at is None
    with pytest.raises(termination.WorkerTaskTerminationConflict):
        await termination.settle_manager_receipt(
            db_session,
            operation_id,
            termination.task_not_found_payload(task_id, operation_id),
        )
    await db_session.rollback()

    intent = await termination.record_manager_ack_intent(
        db_session,
        operation_id,
    )
    assert intent.ack_intent_at is not None
    settled = await termination.settle_manager_receipt(
        db_session,
        operation_id,
        termination.task_not_found_payload(task_id, operation_id),
    )
    assert settled.status == "rejected"
    assert settled.active_task_id is None
    assert settled.acknowledged_at >= settled.ack_intent_at
    rejected_wire = termination.serialize_receipt(settled)
    wrong_final_kind = deepcopy(rejected_wire)
    wrong_final_kind["status"] = "settled"
    assert not termination._receipt_wire_is_structurally_valid(wrong_final_kind)


@pytest.mark.asyncio
async def test_manager_rejection_refuses_changed_frozen_source(db_session):
    worker = Worker(name="changed-rejection-worker", status="ready")
    db_session.add(worker)
    await db_session.flush()
    task = Task(
        title="Changed before rejection commit",
        status="pending",
        worker_id=worker.id,
        retry_count=0,
        turn_generation=0,
    )
    db_session.add(task)
    await db_session.commit()
    manager = await termination.create_or_resume_manager_receipt(
        db_session,
        task,
        operation="cancel",
    )
    task_id = task.id
    operation_id = manager.operation_id

    # A Worker rejection proves its PUT caused no side effect.  It does not
    # authorize releasing the Manager gate for a locally changed generation.
    task.status = "in_progress"
    await db_session.commit()
    with pytest.raises(
        termination.WorkerTaskTerminationConflict,
        match="changed before termination rejection commit",
    ):
        await termination.reject_manager_receipt(
            db_session,
            operation_id,
            _worker_rejection(manager),
        )
    await db_session.rollback()

    receipt = await db_session.get(
        termination.WorkerTaskTerminationReceipt,
        operation_id,
    )
    assert receipt.status == "pending_remote"
    assert receipt.active_task_id == task_id
    assert receipt.result_payload is None


async def _local_handoff(
    db_session,
    *,
    status: str,
) -> tuple[Task, WorkerTurnHandoffReceipt, int]:
    from_generation = 7
    bound = status in {"claimed", "launching", "launched"}
    task = Task(
        title=f"handoff-{status}",
        status="in_progress" if bound else "pending",
        retry_count=3,
        turn_generation=from_generation + int(bound),
    )
    db_session.add(task)
    await db_session.flush()
    # Force the Worker's local log id away from the Manager's source_log_id.
    db_session.add(
        LogEntry(task_id=task.id, event_type="system_event", content="filler")
    )
    await db_session.flush()
    source = LogEntry(
        task_id=task.id,
        event_type="user_message",
        role="user",
        content="worker-local source",
        task_retry_count=3 if bound else None,
        task_turn_generation=from_generation + 1 if bound else None,
        turn_scope="source" if bound else None,
        actual_transport="codex_exec" if status == "launching" else None,
    )
    db_session.add(source)
    await db_session.flush()
    handoff_id = {
        "accepted": "d" * 32,
        "claimed": "e" * 32,
        "launching": "f" * 32,
    }[status]
    request_payload = {"message": "immutable Worker-local request"}
    queue_payload = {
        "source_log_id": source.id,
        "worker_turn_handoff_id": handoff_id,
        "worker_turn_handoff_retry_count": 3,
        "worker_turn_handoff_from_generation": from_generation,
        "delivery_key": None,
    }
    receipt = WorkerTurnHandoffReceipt(
        handoff_id=handoff_id,
        task_id=task.id,
        source_log_id=source.id,
        side="worker",
        worker_id=None,
        retry_count=3,
        from_generation=from_generation,
        status=status,
        request_payload=request_payload,
        request_digest=termination.canonical_json_digest(request_payload),
        queue_payload=queue_payload,
        queue_payload_digest=termination.canonical_json_digest(queue_payload),
        response={"ok": True},
        claimed_turn_generation=from_generation + 1 if bound else None,
    )
    db_session.add(receipt)
    await db_session.commit()
    return task, receipt, source.id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("handoff_status", "expected_handoff_status"),
    (("accepted", "cancelled"), ("claimed", "cancelled"), ("launching", "launching")),
)
async def test_handoff_match_uses_local_proof_not_cross_db_source_log_id(
    db_session,
    handoff_status,
    expected_handoff_status,
):
    task, handoff, worker_source_log_id = await _local_handoff(
        db_session,
        status=handoff_status,
    )
    manager_source_log_id = worker_source_log_id + 10_000
    assert manager_source_log_id != worker_source_log_id
    manager_handoff = {
        "handoff_id": handoff.handoff_id,
        "worker_id": 41,
        "retry_count": handoff.retry_count,
        "from_generation": handoff.from_generation,
        "source_log_id": manager_source_log_id,
        "acknowledged": True,
    }
    operation_id = {
        "accepted": "1" * 32,
        "claimed": "2" * 32,
        "launching": "3" * 32,
    }[handoff_status]
    payload = _request_payload(
        task.id,
        operation_id,
        status="pending",
        retry_count=handoff.retry_count,
        turn_generation=handoff.from_generation,
        manager_handoff=manager_handoff,
    )

    staged = await termination.stage_worker_receipt(
        db_session,
        task_id=task.id,
        operation_id=operation_id,
        operation="cancel",
        request_payload=payload,
        request_digest=termination.canonical_json_digest(payload),
    )

    handoff_id = handoff.handoff_id
    db_session.expire_all()
    local_handoff = await db_session.get(
        WorkerTurnHandoffReceipt,
        handoff_id,
    )
    staged = await db_session.get(
        termination.WorkerTaskTerminationReceipt,
        operation_id,
    )
    assert local_handoff.status == expected_handoff_status
    assert local_handoff.source_log_id == worker_source_log_id
    assert staged.source_worker_turn_handoff_source_log_id == manager_source_log_id


@pytest.mark.asyncio
async def test_launching_handoff_requires_untampered_worker_local_digest(db_session):
    task, handoff, _ = await _local_handoff(db_session, status="launching")
    handoff.queue_payload = {**handoff.queue_payload, "tampered": True}
    await db_session.commit()
    payload = _request_payload(
        task.id,
        "4" * 32,
        status="pending",
        retry_count=handoff.retry_count,
        turn_generation=handoff.from_generation,
        manager_handoff={
            "handoff_id": handoff.handoff_id,
            "worker_id": 41,
            "retry_count": handoff.retry_count,
            "from_generation": handoff.from_generation,
            "source_log_id": 9001,
            "acknowledged": False,
        },
    )

    with pytest.raises(termination.WorkerTaskTerminationConflict):
        await termination.stage_worker_receipt(
            db_session,
            task_id=task.id,
            operation_id="4" * 32,
            operation="cancel",
            request_payload=payload,
            request_digest=termination.canonical_json_digest(payload),
        )


@pytest.mark.asyncio
async def test_manager_applies_same_generation_terminal_only_for_active_termination(
    db_session,
):
    """A cancelled accepted handoff terminates G and settles its marker."""

    worker = Worker(name="receipt-worker", status="ready")
    db_session.add(worker)
    await db_session.flush()
    worker_id = worker.id
    completed_at = datetime(2026, 1, 2, 3, 4, 5, 654321)
    task = Task(
        title="accepted handoff terminal G",
        status="completed",
        worker_id=worker.id,
        retry_count=3,
        turn_generation=7,
        completed_at=completed_at,
    )
    db_session.add(task)
    await db_session.flush()
    manager_log = LogEntry(
        task_id=task.id,
        event_type="user_message",
        role="user",
        content="Manager-local follow-up",
    )
    db_session.add(manager_log)
    await db_session.flush()
    observed = worker_relay.worker_task_generation(
        task,
        expected_worker_id=worker.id,
    )
    assert observed is not None
    handoff_request = {"message": "follow-up"}
    handoff_id = "9" * 32
    reserved = await worker_relay.reserve_worker_turn_handoff(
        db_session,
        observed,
        handoff_id=handoff_id,
        source_log_id=manager_log.id,
        request_payload=handoff_request,
        request_digest=termination.canonical_json_digest(handoff_request),
    )
    assert reserved is not None
    await db_session.commit()

    remote_task = {
        "id": task.id,
        "status": "completed",
        "retry_count": 3,
        "turn_generation": 7,
        # Worker-local process identity is intentionally unrelated to the
        # Manager mirror's instance id.
        "instance_id": 8_123,
        "started_at": "2026-01-02T02:03:04.123456",
        "completed_at": termination._wire_datetime(completed_at),
        "session_id": None,
        "error_message": None,
        "background_active": False,
    }
    # Without the exact active termination receipt, ordinary relay apply must
    # not use a same-G terminal snapshot to consume a pending handoff.
    generic = await worker_relay.apply_authoritative_worker_task(
        db_session,
        reserved,
        remote_task,
        worker_turn_handoff_id=handoff_id,
    )
    assert generic is None

    receipt = await termination.create_or_resume_manager_receipt(
        db_session,
        task,
        operation="cancel",
    )
    result_payload = {
        "version": 2,
        "operation_id": receipt.operation_id,
        "task_id": task.id,
        "operation": "cancel",
        "request_digest": receipt.request_digest,
        "task": remote_task,
        "response": {"ok": True},
    }
    result_digest = termination.canonical_json_digest(result_payload)
    remote_receipt = {
        "version": 2,
        "operation_id": receipt.operation_id,
        "task_id": task.id,
        "side": "worker",
        "worker_id": None,
        "operation": "cancel",
        "status": "succeeded",
        "state_version": 3,
        "source": {
            "incarnation_id": "0" * 32,
            "status": "completed",
            "retry_count": 3,
            "turn_generation": 7,
            "source_log_id": 17,
            "instance_id": 8_123,
            "started_at": "2026-01-02T02:03:04.123456",
            "completed_at": termination._wire_datetime(completed_at),
            "session_id": "worker-session",
            "pty_background_generation": None,
        },
        "request_payload": receipt.request_payload,
        "request_digest": receipt.request_digest,
        "result_payload": result_payload,
        "result_digest": result_digest,
        "attempt_count": 1,
        "reconcile_count": 0,
        "last_error": None,
        "accepted_at": "2026-01-02T03:04:05.654321",
        "completed_at": "2026-01-02T03:04:06.654321",
        "ack_intent_at": None,
        "acknowledged_at": None,
        "created_at": "2026-01-02T03:04:05.654321",
        "updated_at": "2026-01-02T03:04:06.654321",
    }

    applied = await termination.apply_manager_result(
        db_session,
        receipt.operation_id,
        remote_receipt,
    )

    task_id = task.id
    applied_operation_id = applied.operation_id
    db_session.expire_all()
    current = await db_session.get(Task, task_id)
    manager_handoff = await db_session.get(WorkerTurnHandoffReceipt, handoff_id)
    applied = await db_session.get(
        termination.WorkerTaskTerminationReceipt,
        applied_operation_id,
    )
    assert applied.status == "awaiting_ack"
    assert applied.active_task_id == task.id
    assert current.status == "completed"
    assert current.turn_generation == 7
    assert current.worker_turn_handoff_id is None
    assert manager_handoff.status == "cancelled"

    # Emulate a Manager MySQL DATETIME(fsp=0) normalization and a temporary
    # Worker lifecycle outage after the result commit but before ACK.  A
    # repeated public operation must return the same durable operation id,
    # rather than comparing Worker-local instance/timestamp fields.
    current.started_at = current.started_at.replace(microsecond=0)
    current.completed_at = current.completed_at.replace(microsecond=0)
    current.session_id = "manager-normalized-session"
    current_worker = await db_session.get(Worker, worker_id)
    current_worker.status = "error"
    await db_session.commit()
    resumed = await termination.create_or_resume_manager_receipt(
        db_session,
        current,
        operation="cancel",
    )
    assert resumed.operation_id == applied_operation_id
    assert resumed.status == "awaiting_ack"
    assert not db_session.in_transaction()
    await termination.record_manager_ack_intent(db_session, applied_operation_id)
    final_success = await termination.settle_manager_receipt(
        db_session,
        applied_operation_id,
        termination.task_not_found_payload(task_id, applied_operation_id),
    )
    success_wire = termination.serialize_receipt(final_success)
    assert success_wire["status"] == "settled"
    wrong_final_kind = deepcopy(success_wire)
    wrong_final_kind["status"] = "rejected"
    assert not termination._receipt_wire_is_structurally_valid(wrong_final_kind)


@pytest.mark.asyncio
async def test_historical_settled_supersede_is_reused_exactly(db_session):
    worker = Worker(name="historical-supersede-worker", status="ready")
    db_session.add(worker)
    await db_session.flush()
    task = Task(
        title="historical settled supersede",
        status="completed",
        worker_id=worker.id,
        retry_count=2,
        turn_generation=5,
        metadata_={"pr_review_id": 41, "pr_review_superseded": True},
    )
    db_session.add(task)
    await db_session.commit()
    receipt = await termination.create_or_resume_manager_receipt(
        db_session,
        task,
        operation="supersede",
    )
    result_payload, _ = _success_result_payload(
        receipt.request_payload,
        retry_count=task.retry_count,
        turn_generation=task.turn_generation,
    )
    now = datetime.utcnow()
    receipt.status = "settled"
    receipt.active_task_id = None
    receipt.state_version = 4
    receipt.result_payload = result_payload
    receipt.result_digest = termination.canonical_json_digest(result_payload)
    receipt.accepted_at = now
    receipt.completed_at = now
    receipt.ack_intent_at = now
    receipt.acknowledged_at = now
    receipt.next_reconcile_at = None
    worker.status = "error"
    await db_session.commit()
    operation_id = receipt.operation_id

    resumed = await termination.create_or_resume_manager_receipt(
        db_session,
        task,
        operation="supersede",
    )

    assert resumed.operation_id == operation_id
    assert resumed.status == "settled"
    assert not db_session.in_transaction()
    assert await db_session.scalar(
        select(func.count(termination.WorkerTaskTerminationReceipt.operation_id))
        .where(
            termination.WorkerTaskTerminationReceipt.task_id == task.id,
            termination.WorkerTaskTerminationReceipt.operation == "supersede",
        )
    ) == 1


@pytest.mark.asyncio
async def test_worker_heartbeat_retries_transient_failure_without_losing_owner(
    session_factory,
    monkeypatch,
):
    async with session_factory() as db:
        task = Task(title="flaky heartbeat", status="completed")
        db.add(task)
        await db.commit()
        operation_id = "a" * 32
        payload = _request_payload(
            task.id,
            operation_id,
            status="completed",
        )
        receipt = await termination.stage_worker_receipt(
            db,
            task_id=task.id,
            operation_id=operation_id,
            operation="cancel",
            request_payload=payload,
            request_digest=termination.canonical_json_digest(payload),
        )
        lease_expires_at = datetime.utcnow() + timedelta(seconds=0.3)
        receipt.status = "executing"
        receipt.state_version = 2
        receipt.execution_token = "e" * 32
        receipt.attempt_count = 1
        receipt.next_reconcile_at = lease_expires_at
        await db.commit()
        fence = termination._WorkerTerminationExecutionFence(
            task_id=task.id,
            operation_id=operation_id,
            operation="cancel",
            request_digest=receipt.request_digest,
            execution_token=receipt.execution_token,
            state_version=receipt.state_version,
            lease_expires_at=lease_expires_at,
            source_task_incarnation_id=receipt.source_task_incarnation_id,
            source_task_status=receipt.source_task_status,
            source_task_retry_count=receipt.source_task_retry_count,
            source_task_turn_generation=receipt.source_task_turn_generation,
            accepted_at=receipt.accepted_at,
            created_at=receipt.created_at,
        )

    attempts = 0
    renewed = asyncio.Event()

    @asynccontextmanager
    async def flaky_factory():
        nonlocal attempts
        async with session_factory() as actual:
            class FlakySession:
                def __getattr__(self, name):
                    return getattr(actual, name)

                async def execute(self, statement, *args, **kwargs):
                    nonlocal attempts
                    attempts += 1
                    if attempts == 1:
                        raise RuntimeError("transient heartbeat database failure")
                    result = await actual.execute(statement, *args, **kwargs)
                    renewed.set()
                    return result

            yield FlakySession()

    monkeypatch.setattr(
        termination,
        "_WORKER_EXECUTION_HEARTBEAT_SECONDS",
        0.01,
    )
    monkeypatch.setattr(
        termination,
        "_WORKER_EXECUTION_HEARTBEAT_RETRY_SECONDS",
        0.005,
    )
    monkeypatch.setattr(
        termination,
        "_WORKER_EXECUTION_LEASE_SECONDS",
        0.3,
    )
    stop = asyncio.Event()
    ownership_lost = asyncio.Event()
    heartbeat = asyncio.create_task(
        termination._heartbeat_worker_execution(
            flaky_factory,
            fence,
            stop=stop,
            ownership_lost=ownership_lost,
        )
    )
    await asyncio.wait_for(renewed.wait(), timeout=2)
    stop.set()
    await asyncio.wait_for(heartbeat, timeout=2)

    assert attempts >= 2
    assert not ownership_lost.is_set()
    async with session_factory() as db:
        current = await db.get(
            termination.WorkerTaskTerminationReceipt,
            operation_id,
        )
        assert current.status == "executing"
        assert current.execution_token == fence.execution_token
        assert current.state_version == fence.state_version
        assert current.next_reconcile_at > lease_expires_at


@pytest.mark.asyncio
async def test_worker_execution_cancellation_settles_runtime_and_error_receipt(
    session_factory,
    monkeypatch,
):
    from anyio import CancelScope

    operation_id = "7" * 32
    async with session_factory() as db:
        task = Task(title="cancel worker execution safely", status="pending")
        db.add(task)
        await db.commit()
        task_id = task.id
        payload = _request_payload(task_id, operation_id)
        await termination.stage_worker_receipt(
            db,
            task_id=task_id,
            operation_id=operation_id,
            operation="cancel",
            request_payload=payload,
            request_digest=termination.canonical_json_digest(payload),
        )

    scope_holder: dict[str, CancelScope] = {}
    effect_cancelled = asyncio.Event()
    error_recorded = asyncio.Event()

    async def slow_effect(_db, _fence):
        scope_holder["scope"].cancel()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            effect_cancelled.set()
            raise

    original_record = termination.record_worker_reconcile_error

    async def record_error(*args, **kwargs):
        await asyncio.sleep(0)
        result = await original_record(*args, **kwargs)
        await asyncio.sleep(0)
        error_recorded.set()
        return result

    monkeypatch.setattr(
        termination,
        "_execute_owned_worker_receipt",
        slow_effect,
    )
    monkeypatch.setattr(
        termination,
        "record_worker_reconcile_error",
        record_error,
    )

    async with session_factory() as db:
        with CancelScope() as scope:
            scope_holder["scope"] = scope
            with pytest.raises(asyncio.CancelledError):
                await termination.execute_worker_receipt(db, operation_id)

    assert effect_cancelled.is_set()
    assert error_recorded.is_set()
    async with session_factory() as db:
        durable = await db.get(
            termination.WorkerTaskTerminationReceipt,
            operation_id,
        )
    assert durable.status == "executing"
    assert durable.reconcile_count == 1
    assert durable.last_error == "Worker termination execution was cancelled"


@pytest.mark.asyncio
async def test_expired_lease_without_takeover_cancels_old_effect(
    session_factory,
    monkeypatch,
):
    operation_id = "8" * 32
    async with session_factory() as db:
        task = Task(title="expired lease no takeover", status="pending")
        db.add(task)
        await db.commit()
        task_id = task.id
        payload = _request_payload(task_id, operation_id)
        await termination.stage_worker_receipt(
            db,
            task_id=task_id,
            operation_id=operation_id,
            operation="stop_session",
            request_payload={**payload, "operation": "stop_session"},
            request_digest=termination.canonical_json_digest(
                {**payload, "operation": "stop_session"}
            ),
        )

    effect_started = asyncio.Event()
    effect_cancelled = asyncio.Event()

    async def slow_effect(_db, _fence):
        effect_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            effect_cancelled.set()
            raise

    monkeypatch.setattr(
        termination,
        "_WORKER_EXECUTION_LEASE_SECONDS",
        0.04,
    )
    monkeypatch.setattr(
        termination,
        "_WORKER_EXECUTION_HEARTBEAT_SECONDS",
        0.2,
    )
    monkeypatch.setattr(
        termination,
        "_execute_owned_worker_receipt",
        slow_effect,
    )
    async with session_factory() as db:
        execution = asyncio.create_task(
            termination.execute_worker_receipt(db, operation_id)
        )
        await asyncio.wait_for(effect_started.wait(), timeout=2)
        resulting = await asyncio.wait_for(execution, timeout=2)

    assert effect_cancelled.is_set()
    assert resulting.status == "executing"
    assert resulting.result_payload is None
    assert resulting.execution_token is not None
    assert resulting.next_reconcile_at <= datetime.utcnow()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        durable = await db.get(
            termination.WorkerTaskTerminationReceipt,
            operation_id,
        )
        assert task.status == "pending"
        assert durable.status == "executing"
        assert durable.state_version == 2
        assert durable.result_payload is None


@pytest.mark.asyncio
async def test_two_wal_coordinators_take_over_monotonically(tmp_path, monkeypatch):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'termination-coordinators.db'}",
        connect_args={"timeout": 5},
    )
    async with engine.connect() as connection:
        await connection.execute(text("PRAGMA journal_mode=WAL"))
        await connection.commit()
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    operation_id = "d" * 32
    old_token = "f" * 32
    async with factory() as db:
        task = Task(title="dual WAL coordinator", status="completed")
        db.add(task)
        await db.commit()
        payload = _request_payload(
            task.id,
            operation_id,
            status="completed",
        )
        receipt = await termination.stage_worker_receipt(
            db,
            task_id=task.id,
            operation_id=operation_id,
            operation="cancel",
            request_payload=payload,
            request_digest=termination.canonical_json_digest(payload),
        )
        receipt.status = "executing"
        receipt.state_version = 2
        receipt.execution_token = old_token
        receipt.attempt_count = 1
        receipt.next_reconcile_at = datetime.utcnow() - timedelta(seconds=1)
        await db.commit()
        stale_fence = termination._WorkerTerminationExecutionFence(
            task_id=task.id,
            operation_id=operation_id,
            operation="cancel",
            request_digest=receipt.request_digest,
            execution_token=old_token,
            state_version=2,
            lease_expires_at=receipt.next_reconcile_at,
            source_task_incarnation_id=receipt.source_task_incarnation_id,
            source_task_status=receipt.source_task_status,
            source_task_retry_count=receipt.source_task_retry_count,
            source_task_turn_generation=receipt.source_task_turn_generation,
            accepted_at=receipt.accepted_at,
            created_at=receipt.created_at,
        )

    original_owned = termination._execute_owned_worker_receipt
    effect_entered = asyncio.Event()
    effect_release = asyncio.Event()
    effect_calls = 0

    async def delayed_owned(db, fence):
        nonlocal effect_calls
        effect_calls += 1
        effect_entered.set()
        await effect_release.wait()
        return await original_owned(db, fence)

    entrants = 0
    both_selected = asyncio.Event()

    @asynccontextmanager
    async def cross_process_operation_lock(_task_id):
        nonlocal entrants
        entrants += 1
        if entrants >= 2:
            both_selected.set()
        try:
            await asyncio.wait_for(both_selected.wait(), timeout=0.2)
        except asyncio.TimeoutError:
            both_selected.set()
        yield

    monkeypatch.setattr(
        termination,
        "_execute_owned_worker_receipt",
        delayed_owned,
    )
    with patch(
        "backend.services.worker_proxy.get_task_operation_lock",
        side_effect=cross_process_operation_lock,
    ):
        first = termination.WorkerTaskTerminationCoordinator(factory)
        second = termination.WorkerTaskTerminationCoordinator(factory)
        first_pass = asyncio.create_task(first.recover_once(include_manager=False))
        second_pass = asyncio.create_task(second.recover_once(include_manager=False))
        await asyncio.wait_for(effect_entered.wait(), timeout=3)

        # A pre-takeover owner may finish late, but its old token/version CAS
        # cannot overwrite the new coordinator's executing lease.
        async with factory() as stale_db:
            stale_result = await original_owned(stale_db, stale_fence)
            assert stale_result.status == "executing"
            assert stale_result.state_version == 3
            assert stale_result.execution_token != old_token

        effect_release.set()
        await asyncio.wait_for(
            asyncio.gather(first_pass, second_pass),
            timeout=5,
        )

    assert effect_calls == 1
    async with factory() as db:
        settled = await db.get(
            termination.WorkerTaskTerminationReceipt,
            operation_id,
        )
        assert settled.status == "succeeded"
        assert settled.state_version == 4
        assert settled.attempt_count == 2
        assert settled.execution_token is None
        assert settled.next_reconcile_at is None
        assert termination.canonical_json_digest(settled.result_payload) == (
            settled.result_digest
        )
    await engine.dispose()


@pytest.mark.asyncio
async def test_restart_executor_recovers_same_owner_with_dead_pid(
    session_factory,
):
    operation_id = "9" * 32
    async with session_factory() as db:
        task = Task(
            title="same owner dead pid",
            status="executing",
            retry_count=1,
            turn_generation=4,
            started_at=datetime(2026, 8, 7, 1, 2, 3),
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="dead receipt owner",
            status="running",
            pid=None,
            started_at=task.started_at,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id = task.id
        instance_id = instance.id
        payload = _request_payload(
            task_id,
            operation_id,
            status="executing",
            retry_count=1,
            turn_generation=4,
        )
        await termination.stage_worker_receipt(
            db,
            task_id=task_id,
            operation_id=operation_id,
            operation="cancel",
            request_payload=payload,
            request_digest=termination.canonical_json_digest(payload),
        )

    async def reconcile_dead_owner(stopped_instance_id, **kwargs):
        assert stopped_instance_id == instance_id
        assert kwargs["expected_task_id"] == task_id
        assert kwargs["expected_pid"] is None
        assert kwargs["worker_termination_operation_id"] == operation_id
        assert len(kwargs["worker_termination_execution_token"]) == 32
        assert kwargs["worker_termination_state_version"] == 2
        async with session_factory() as stop_db:
            stopped_task = await stop_db.get(Task, task_id)
            stopped_instance = await stop_db.get(Instance, instance_id)
            stopped_task.status = "cancelled"
            stopped_task.completed_at = datetime.utcnow()
            stopped_instance.status = "idle"
            stopped_instance.current_task_id = None
            stopped_instance.started_at = None
            await stop_db.commit()
        return True

    with (
        patch.object(
            main_module.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch.object(
            main_module.instance_manager,
            "wait_for_task_launch_barrier",
            new_callable=AsyncMock,
            return_value=True,
        ),
        patch.object(
            main_module.instance_manager,
            "stop",
            new_callable=AsyncMock,
            side_effect=reconcile_dead_owner,
        ) as stop,
    ):
        async with session_factory() as db:
            resulting = await termination.execute_worker_receipt(
                db,
                operation_id,
            )

    assert resulting.status == "succeeded"
    assert resulting.result_payload["task"]["status"] == "cancelled"
    stop.assert_awaited_once()
