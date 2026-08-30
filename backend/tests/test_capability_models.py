"""Database-enforced Capability Core ownership fences."""

import pytest
from sqlalchemy.exc import IntegrityError

from backend.models.capability import CapabilityExecution, CapabilityInvocation
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.models.worker_turn_handoff import WorkerTurnHandoffReceipt


_HASH = "0" * 64


async def _worker_handoff_receipt(
    db_session,
    *,
    handoff_id: str,
    status: str,
    claimed_turn_generation: int | None,
) -> WorkerTurnHandoffReceipt:
    task = Task(title=f"handoff {handoff_id}")
    db_session.add(task)
    await db_session.flush()
    log = LogEntry(
        task_id=task.id,
        event_type="user_message",
        role="user",
        content="exact follow-up",
    )
    db_session.add(log)
    await db_session.flush()
    return WorkerTurnHandoffReceipt(
        handoff_id=handoff_id,
        task_id=task.id,
        source_log_id=log.id,
        side="worker",
        worker_id=None,
        retry_count=2,
        from_generation=4,
        status=status,
        request_payload={},
        request_digest=_HASH,
        queue_payload={},
        queue_payload_digest=_HASH,
        response={},
        claimed_turn_generation=claimed_turn_generation,
    )


def _invocation(
    task_id: int,
    key: str,
    *,
    status: str = "queued",
    active_task_id: int | None = None,
) -> CapabilityInvocation:
    return CapabilityInvocation(
        task_id=task_id,
        capability_key="plan",
        source="human_request",
        purpose="advisory",
        status=status,
        state_version=1,
        idempotency_key=key,
        input_payload={},
        input_hash=_HASH,
        subject_kind="task_generation",
        subject_ref={"task_id": task_id},
        subject_hash=_HASH,
        executor_kind="fake",
        executor_config={},
        executor_config_hash=_HASH,
        policy_snapshot={},
        policy_hash=_HASH,
        resume_policy="attach_only",
        max_attempts=1,
        active_task_id=(task_id if active_task_id is None else active_task_id),
    )


def _execution(
    invocation_id: int,
    key: str,
    *,
    attempt: int = 1,
    status: str = "queued",
    active_invocation_id: int | None = None,
) -> CapabilityExecution:
    return CapabilityExecution(
        invocation_id=invocation_id,
        attempt=attempt,
        status=status,
        state_version=1,
        active_invocation_id=(
            invocation_id
            if active_invocation_id is None
            else active_invocation_id
        ),
        idempotency_key=key,
        executor_kind="fake",
        input_hash=_HASH,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_active_task_id", [-1, 0])
async def test_active_invocation_must_own_its_task_slot(
    db_session,
    bad_active_task_id,
):
    task = Task(title="owner fence")
    db_session.add(task)
    await db_session.flush()
    db_session.add(
        _invocation(
            task.id,
            "bad-owner",
            active_task_id=bad_active_task_id,
        )
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_active_invocation_requires_non_null_slot(db_session):
    task = Task(title="null fence")
    db_session.add(task)
    await db_session.flush()
    invocation = _invocation(task.id, "null-owner")
    invocation.active_task_id = None
    db_session.add(invocation)
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_terminal_invocation_must_release_slot(db_session):
    task = Task(title="terminal fence")
    db_session.add(task)
    await db_session.flush()
    db_session.add(
        _invocation(task.id, "terminal-owner", status="failed")
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_only_one_active_invocation_per_task(db_session):
    task = Task(title="unique task slot")
    db_session.add(task)
    await db_session.flush()
    db_session.add_all(
        [
            _invocation(task.id, "one"),
            _invocation(task.id, "two"),
        ]
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_execution_active_slot_owner_and_uniqueness(db_session):
    task = Task(title="execution fence")
    db_session.add(task)
    await db_session.flush()
    invocation = _invocation(task.id, "invocation")
    db_session.add(invocation)
    await db_session.flush()
    db_session.add(
        _execution(
            invocation.id,
            "bad-execution-owner",
            active_invocation_id=-1,
        )
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_only_one_active_execution_per_invocation(db_session):
    task = Task(title="unique execution slot")
    db_session.add(task)
    await db_session.flush()
    invocation = _invocation(task.id, "invocation")
    db_session.add(invocation)
    await db_session.flush()
    db_session.add_all(
        [
            _execution(invocation.id, "execution-one"),
            _execution(invocation.id, "execution-two", attempt=2),
        ]
    )
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_invocation_exact_turn_evidence_round_trip(db_session):
    task = Task(title="exact capability turn")
    db_session.add(task)
    await db_session.flush()
    invocation = _invocation(task.id, "exact-turn-evidence")
    invocation.request_task_turn_generation = 2**40 + 9
    invocation.request_output_log_id = 314
    invocation.request_reason = "Need a plan before changing the schema"
    invocation.request_protocol_version = 1
    invocation.request_output_hash = "3" * 64
    invocation.request_native_turn_id = "native_turn_314"
    db_session.add(invocation)
    await db_session.commit()
    await db_session.refresh(invocation)

    assert invocation.request_task_turn_generation == 2**40 + 9
    assert invocation.request_output_log_id == 314
    assert invocation.request_reason == "Need a plan before changing the schema"
    assert invocation.request_protocol_version == 1
    assert invocation.request_output_hash == "3" * 64
    assert invocation.request_native_turn_id == "native_turn_314"


@pytest.mark.asyncio
async def test_agent_invocation_requires_exact_turn_evidence(db_session):
    task = Task(title="agent exact capability turn")
    db_session.add(task)
    await db_session.flush()
    invocation = _invocation(task.id, "missing-agent-evidence")
    invocation.source = "agent_request"
    invocation.resume_policy = "resume_task"
    db_session.add(invocation)

    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_agent_invocation_exact_turn_shape_is_accepted(db_session):
    task = Task(title="valid agent exact capability turn")
    db_session.add(task)
    await db_session.flush()
    invocation = _invocation(task.id, "valid-agent-evidence")
    invocation.source = "agent_request"
    invocation.resume_policy = "resume_task"
    invocation.request_task_incarnation_id = task.incarnation_id
    invocation.request_task_retry_count = 0
    invocation.request_task_turn_generation = 1
    invocation.request_source_log_id = 41
    invocation.request_output_log_id = 42
    invocation.request_terminal_log_id = 43
    invocation.request_reason = "Review the completed turn"
    invocation.request_protocol_version = 1
    invocation.request_output_hash = "4" * 64
    db_session.add(invocation)

    await db_session.commit()
    await db_session.refresh(invocation)
    assert invocation.id is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("purpose", "required_gate"),
        ("resume_policy", "attach_only"),
        ("requested_by_user_id", 7),
        ("request_task_retry_count", None),
        ("request_task_turn_generation", None),
        ("request_source_log_id", None),
        ("request_output_log_id", None),
        ("request_reason", None),
        ("request_protocol_version", None),
        ("request_protocol_version", 0),
        ("request_output_hash", None),
    ],
)
async def test_agent_invocation_rejects_incomplete_or_forged_identity(
    db_session,
    field,
    invalid_value,
):
    task = Task(title=f"invalid agent audit {field}")
    db_session.add(task)
    await db_session.flush()
    invocation = _invocation(task.id, f"invalid-agent-audit-{field}")
    invocation.source = "agent_request"
    invocation.resume_policy = "resume_task"
    invocation.request_task_retry_count = 0
    invocation.request_task_turn_generation = 1
    invocation.request_source_log_id = 41
    invocation.request_output_log_id = 42
    invocation.request_reason = "Need an exact audit trail"
    invocation.request_protocol_version = 1
    invocation.request_output_hash = "5" * 64
    setattr(invocation, field, invalid_value)
    db_session.add(invocation)

    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_output_log_can_authorize_only_one_invocation_per_task(db_session):
    task = Task(title="one action per terminal output")
    db_session.add(task)
    await db_session.flush()
    first = _invocation(task.id, "terminal-output-one", status="failed")
    first.active_task_id = None
    first.request_output_log_id = 91
    second = _invocation(task.id, "terminal-output-two", status="failed")
    second.active_task_id = None
    second.request_output_log_id = 91
    db_session.add_all([first, second])

    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "claimed_turn_generation"),
    [
        ("claimed", None),
        ("launching", None),
        ("launched", None),
        ("claimed", 4),
        ("accepted", 5),
        ("cancelled", 5),
    ],
)
async def test_worker_handoff_claim_generation_shape_is_enforced(
    db_session,
    status,
    claimed_turn_generation,
):
    receipt = await _worker_handoff_receipt(
        db_session,
        handoff_id=(status[0] * 31 + str(claimed_turn_generation))[-32:],
        status=status,
        claimed_turn_generation=claimed_turn_generation,
    )
    db_session.add(receipt)

    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "claimed_turn_generation"),
    [
        ("claimed", 5),
        ("launching", 5),
        ("launched", 5),
        ("accepted", None),
        ("cancelled", None),
    ],
)
async def test_worker_handoff_claim_generation_valid_shapes_are_accepted(
    db_session,
    status,
    claimed_turn_generation,
):
    receipt = await _worker_handoff_receipt(
        db_session,
        handoff_id=(status[0] * 31 + str(claimed_turn_generation))[-32:],
        status=status,
        claimed_turn_generation=claimed_turn_generation,
    )
    db_session.add(receipt)

    await db_session.commit()
    await db_session.refresh(receipt)
    assert receipt.claimed_turn_generation == claimed_turn_generation
