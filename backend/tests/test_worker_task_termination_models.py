"""Database fences for durable distributed Task termination receipts."""

from datetime import datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from backend.models.task import Task
from backend.models.worker_task_termination import (
    WorkerTaskTerminationReceipt,
)


_DIGEST = "a" * 64


async def _task(db_session, title: str = "termination receipt") -> Task:
    task = Task(title=title, status="executing", retry_count=2)
    db_session.add(task)
    await db_session.flush()
    return task


def _receipt(
    task: Task,
    *,
    operation_id: str = "1" * 32,
    side: str = "manager",
    status: str = "pending_remote",
    with_handoff: bool = False,
) -> WorkerTaskTerminationReceipt:
    active_statuses = {
        "manager": {"pending_remote", "awaiting_ack", "conflict"},
        "worker": {
            "accepted",
            "executing",
            "succeeded",
            "rejected",
            "conflict",
        },
    }
    accepted_statuses = {
        "awaiting_ack",
        "settled",
        "accepted",
        "executing",
        "succeeded",
        "acknowledged",
        "rejected",
    }
    outcome_statuses = {
        "awaiting_ack",
        "settled",
        "succeeded",
        "acknowledged",
        "rejected",
    }
    now = datetime.utcnow()
    receipt = WorkerTaskTerminationReceipt(
        operation_id=operation_id,
        task_id=task.id,
        active_task_id=(
            task.id if status in active_statuses.get(side, set()) else None
        ),
        side=side,
        worker_id=17 if side == "manager" else None,
        operation="cancel",
        status=status,
        state_version=1,
        execution_token=("e" * 32 if side == "worker" and status == "executing" else None),
        source_task_incarnation_id=task.incarnation_id,
        source_task_status=task.status,
        source_task_retry_count=task.retry_count,
        source_task_turn_generation=9,
        source_task_source_log_id=31,
        source_task_instance_id=4,
        source_task_started_at=now - timedelta(minutes=1),
        source_task_completed_at=None,
        source_task_session_id="session-exact",
        source_task_pty_background_generation="pty-exact",
        request_payload={"operation": "cancel", "task_id": task.id},
        request_digest=_DIGEST,
        result_payload=(
            {"status": status} if status in outcome_statuses else None
        ),
        result_digest=_DIGEST if status in outcome_statuses else None,
        attempt_count=0,
        reconcile_count=0,
        next_reconcile_at=now,
        last_error="quarantined" if status == "conflict" else None,
        accepted_at=now if status in accepted_statuses else None,
        completed_at=now if status in outcome_statuses else None,
        ack_intent_at=(
            now
            if side == "manager" and status in {"settled", "rejected"}
            else None
        ),
        acknowledged_at=(
            now
            if status in {"settled", "acknowledged"}
            or (side == "manager" and status == "rejected")
            else None
        ),
    )
    if with_handoff:
        receipt.source_worker_turn_handoff_id = "h" * 32
        receipt.source_worker_turn_handoff_worker_id = 17
        receipt.source_worker_turn_handoff_retry_count = 2
        receipt.source_worker_turn_handoff_from_generation = 8
        receipt.source_worker_turn_handoff_source_log_id = 30
        receipt.source_worker_turn_handoff_acknowledged = False
    return receipt


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("side", "status"),
    (
        ("manager", "pending_remote"),
        ("manager", "awaiting_ack"),
        ("manager", "settled"),
        ("manager", "rejected"),
        ("manager", "conflict"),
        ("worker", "accepted"),
        ("worker", "executing"),
        ("worker", "succeeded"),
        ("worker", "acknowledged"),
        ("worker", "rejected"),
        ("worker", "conflict"),
    ),
)
async def test_all_side_status_shapes_round_trip(db_session, side, status):
    task = await _task(db_session, f"{side}-{status}")
    receipt = _receipt(
        task,
        side=side,
        status=status,
        with_handoff=True,
    )
    db_session.add(receipt)
    await db_session.commit()

    stored = await db_session.get(
        WorkerTaskTerminationReceipt,
        receipt.operation_id,
    )
    assert stored is not None
    assert stored.source_task_incarnation_id == task.incarnation_id
    assert stored.source_worker_turn_handoff_from_generation == 8
    assert stored.reconcile_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("side", "status"),
    (
        ("manager", "pending_remote"),
        ("manager", "awaiting_ack"),
        ("manager", "conflict"),
        ("worker", "accepted"),
        ("worker", "executing"),
        ("worker", "succeeded"),
        ("worker", "rejected"),
        ("worker", "conflict"),
    ),
)
async def test_active_statuses_must_keep_exact_task_slot(
    db_session,
    side,
    status,
):
    task = await _task(db_session, f"active-{side}-{status}")
    receipt = _receipt(task, side=side, status=status)
    receipt.active_task_id = None
    db_session.add(receipt)
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("side", "status"),
    (
        ("manager", "settled"),
        ("manager", "rejected"),
        ("worker", "acknowledged"),
    ),
)
async def test_released_statuses_must_clear_task_slot(
    db_session,
    side,
    status,
):
    task = await _task(db_session, f"released-{side}-{status}")
    receipt = _receipt(task, side=side, status=status)
    receipt.active_task_id = task.id
    db_session.add(receipt)
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_only_one_active_termination_operation_per_task(db_session):
    task = await _task(db_session, "one active operation")
    db_session.add(_receipt(task, operation_id="2" * 32))
    await db_session.flush()
    db_session.add(_receipt(task, operation_id="3" * 32))
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutation", "value"),
    (
        ("operation_id", "short"),
        ("side", "peer"),
        ("operation", "kill"),
        ("status", "unknown"),
        ("worker_id", None),
        ("source_task_status", "tampered"),
        ("source_task_retry_count", -1),
        ("source_task_turn_generation", -1),
        ("request_digest", "short"),
        ("attempt_count", -1),
        ("reconcile_count", -1),
    ),
)
async def test_enumerations_identity_and_counters_are_fenced(
    db_session,
    mutation,
    value,
):
    task = await _task(db_session, f"bad-{mutation}")
    receipt = _receipt(task)
    setattr(receipt, mutation, value)
    db_session.add(receipt)
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_handoff_snapshot_is_all_or_none(db_session):
    task = await _task(db_session, "partial handoff")
    receipt = _receipt(task)
    receipt.source_worker_turn_handoff_id = "h" * 32
    db_session.add(receipt)
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_handoff_acknowledged_rejects_non_boolean_storage(db_session):
    task = await _task(db_session, "strict handoff boolean")
    receipt = _receipt(task, with_handoff=True)
    db_session.add(receipt)
    await db_session.commit()

    for value in (0, 1):
        await db_session.execute(
            text(
                "UPDATE worker_task_termination_receipts "
                "SET source_worker_turn_handoff_acknowledged = :value "
                "WHERE operation_id = :operation_id"
            ),
            {"value": value, "operation_id": receipt.operation_id},
        )
        await db_session.commit()
        stored = await db_session.scalar(
            text(
                "SELECT source_worker_turn_handoff_acknowledged "
                "FROM worker_task_termination_receipts "
                "WHERE operation_id = :operation_id"
            ),
            {"operation_id": receipt.operation_id},
        )
        assert stored == value

    with pytest.raises(IntegrityError):
        await db_session.execute(
            text(
                "UPDATE worker_task_termination_receipts "
                "SET source_worker_turn_handoff_acknowledged = 2 "
                "WHERE operation_id = :operation_id"
            ),
            {"operation_id": receipt.operation_id},
        )
        await db_session.commit()
    await db_session.rollback()


@pytest.mark.asyncio
async def test_outcome_requires_digest_payload_and_timestamps(db_session):
    task = await _task(db_session, "incomplete outcome")
    receipt = _receipt(task, status="awaiting_ack")
    receipt.result_digest = None
    receipt.completed_at = None
    db_session.add(receipt)
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_executing_owner_requires_a_durable_lease_deadline(db_session):
    task = await _task(db_session, "execution owner without lease")
    receipt = _receipt(task, side="worker", status="executing")
    receipt.next_reconcile_at = None
    db_session.add(receipt)
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_manager_result_may_precede_durable_ack_intent(db_session):
    task = await _task(db_session, "result before ack intent")
    receipt = _receipt(task, side="manager", status="awaiting_ack")
    receipt.ack_intent_at = None
    db_session.add(receipt)
    await db_session.commit()
    assert receipt.ack_intent_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ("settled", "rejected"))
async def test_manager_final_outcome_requires_ack_intent(
    db_session,
    status,
):
    task = await _task(db_session, f"missing ack intent {status}")
    receipt = _receipt(task, side="manager", status=status)
    receipt.ack_intent_at = None
    db_session.add(receipt)
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_worker_receipt_cannot_carry_manager_ack_intent(db_session):
    task = await _task(db_session, "worker ack intent")
    receipt = _receipt(task, side="worker", status="rejected")
    receipt.ack_intent_at = datetime.utcnow()
    db_session.add(receipt)
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_worker_rejection_remains_active_until_digest_ack(db_session):
    task = await _task(db_session, "durable rejection ack")
    receipt = _receipt(task, side="worker", status="rejected")
    db_session.add(receipt)
    await db_session.commit()
    assert receipt.active_task_id == task.id
    assert receipt.accepted_at is not None
    assert receipt.completed_at is not None
    assert receipt.acknowledged_at is None

    receipt.status = "acknowledged"
    receipt.active_task_id = None
    receipt.acknowledged_at = receipt.completed_at + timedelta(seconds=1)
    await db_session.commit()
    assert receipt.active_task_id is None


@pytest.mark.asyncio
async def test_ack_timeline_cannot_precede_intent(db_session):
    task = await _task(db_session, "ack before intent")
    receipt = _receipt(task, side="manager", status="settled")
    assert receipt.completed_at is not None
    receipt.ack_intent_at = receipt.completed_at + timedelta(seconds=2)
    receipt.acknowledged_at = receipt.completed_at + timedelta(seconds=1)
    db_session.add(receipt)
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_legacy_null_incarnation_remains_exactly_representable(db_session):
    task = await _task(db_session, "legacy incarnation")
    receipt = _receipt(task)
    receipt.source_task_incarnation_id = None
    db_session.add(receipt)
    await db_session.commit()
    assert receipt.source_task_incarnation_id is None
