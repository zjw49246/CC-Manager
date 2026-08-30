"""Transactional state-machine tests for Capability Core."""

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from backend.config import settings
from backend.models.capability import CapabilityExecution, CapabilityInvocation
from backend.models.task import Task
from backend.services import capability_service as service
from backend.services.capability_registry import (
    CapabilityDefinition,
    register_capability,
    unregister_capability,
)
from backend.tests.worker_termination_helpers import (
    persist_active_worker_receipt,
)


@pytest.fixture(autouse=True)
def capability_runtime(monkeypatch):
    previous = settings.capability_core_enabled
    settings.capability_core_enabled = True
    unregister_capability("plan")
    register_capability(
        CapabilityDefinition(
            capability_key="plan",
            executor_kind="fake_plan",
            executor_config={"route": {"provider": "fake"}},
            policy_snapshot={"max_questions": 3},
            max_attempts=2,
        )
    )
    monkeypatch.setattr(
        service,
        "broadcast_capability_event",
        AsyncMock(),
    )
    yield
    unregister_capability("plan")
    settings.capability_core_enabled = previous


async def _task(db_session, **values) -> Task:
    task = Task(title="Capability target", **values)
    db_session.add(task)
    await db_session.commit()
    return task


async def _create(db_session, task_id: int, *, key: str = "request-1", payload=None):
    return await service.create_human_invocation(
        db_session,
        task_id=task_id,
        capability_key="plan",
        request_payload=payload or {"prompt": "make a plan"},
        idempotency_key=key,
        requested_by_user_id=7,
    )


@pytest.mark.asyncio
async def test_same_idempotency_key_replays_one_invocation(db_session):
    task = await _task(db_session)

    first, first_created = await _create(db_session, task.id)
    replay, replay_created = await _create(db_session, task.id)

    assert first_created is True
    assert replay_created is False
    assert replay.id == first.id
    assert await db_session.scalar(
        select(func.count(CapabilityInvocation.id))
    ) == 1
    assert await db_session.scalar(
        select(func.count(CapabilityExecution.id))
    ) == 1


@pytest.mark.asyncio
async def test_same_idempotency_key_rejects_different_payload(db_session):
    task = await _task(db_session)
    await _create(db_session, task.id)

    with pytest.raises(service.CapabilityConflictError, match="different request"):
        await _create(
            db_session,
            task.id,
            payload={"prompt": "a different plan"},
        )


@pytest.mark.asyncio
async def test_agent_request_schema_does_not_narrow_human_or_controller_payloads(
    db_session,
):
    human_task = await _task(db_session)
    controller_task = await _task(db_session)
    generic_payload = {
        "request": "legacy caller-owned alias",
        "caller_extension": {"preserve": True},
    }

    human, human_created = await service.create_human_invocation(
        db_session,
        task_id=human_task.id,
        capability_key="plan",
        request_payload=generic_payload,
        idempotency_key="human-generic-contract",
        requested_by_user_id=7,
    )
    controller, controller_created = await service.create_controller_invocation(
        db_session,
        task_id=controller_task.id,
        capability_key="plan",
        request_payload=generic_payload,
        idempotency_key="controller-generic-contract",
    )

    assert human_created is True
    assert controller_created is True
    assert human.input_payload == generic_payload
    assert controller.input_payload == generic_payload


@pytest.mark.asyncio
async def test_one_active_invocation_per_task_under_concurrency(
    db_session,
    db_factory,
):
    task = await _task(db_session)

    async def create(key: str):
        async with db_factory() as session:
            return await _create(session, task.id, key=key)

    results = await asyncio.gather(
        create("concurrent-a"),
        create("concurrent-b"),
        return_exceptions=True,
    )

    assert sum(isinstance(item, tuple) for item in results) == 1
    assert sum(
        isinstance(item, service.CapabilityConflictError)
        for item in results
    ) == 1
    assert await db_session.scalar(
        select(func.count(CapabilityInvocation.id))
    ) == 1


@pytest.mark.asyncio
async def test_feature_flag_blocks_new_but_allows_replay_and_cancel(db_session):
    task = await _task(db_session)
    invocation, _ = await _create(db_session, task.id)
    invocation_id = invocation.id
    invocation_version = invocation.state_version
    settings.capability_core_enabled = False

    replay, created = await _create(db_session, task.id)
    assert created is False
    assert replay.id == invocation.id
    with pytest.raises(service.CapabilityDisabledError):
        await _create(db_session, task.id, key="new-while-disabled")
    with pytest.raises(service.CapabilityDisabledError):
        await service.create_controller_invocation(
            db_session,
            task_id=task.id,
            capability_key="plan",
            request_payload={"prompt": "not an admitted delivery run"},
            idempotency_key="controller-new-while-disabled",
        )

    cancelled = await service.cancel_invocation(
        db_session,
        invocation_id=invocation_id,
        expected_state_version=invocation_version,
    )
    assert cancelled.status == "cancelled"
    assert cancelled.active_task_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "task_values,error",
    [
        ({"worker_id": 42}, "remote Worker"),
        ({"shared_from_id": 42}, "shared shadow"),
        ({"status": "migrating"}, "migrating"),
    ],
)
async def test_remote_shared_and_migrating_tasks_fail_closed(
    db_session,
    task_values,
    error,
):
    task = await _task(db_session, **task_values)

    with pytest.raises(service.CapabilityUnsupportedScopeError, match=error):
        await _create(db_session, task.id)


@pytest.mark.asyncio
async def test_human_creation_rejects_delivery_owned_task_in_service(db_session):
    task = await _task(
        db_session,
        mode="delivery_loop",
        delivery_run_id=91,
        delivery_role="developer",
    )

    with pytest.raises(
        service.CapabilityConflictError,
        match="Delivery-owned Tasks",
    ):
        await _create(db_session, task.id)

    assert await db_session.scalar(
        select(func.count(CapabilityInvocation.id))
    ) == 0


@pytest.mark.asyncio
async def test_active_termination_receipt_blocks_capability_creation(
    db_session,
    db_factory,
):
    task = await _task(db_session)
    await persist_active_worker_receipt(db_factory, task.id)

    with pytest.raises(
        service.CapabilityConflictError,
        match="termination receipt",
    ):
        await _create(db_session, task.id)

    assert await db_session.scalar(
        select(func.count(CapabilityInvocation.id))
    ) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ("create", "claim"))
async def test_capability_admission_loses_cleanly_to_concurrent_wal_receipt(
    tmp_path,
    monkeypatch,
    transition,
):
    """The Task gate starts from a fresh writer transaction on SQLite WAL."""

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from backend.database import Base

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / f'capability-{transition}.db'}",
        connect_args={"timeout": 1},
    )
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        async with sessions() as creator:
            task = await _task(creator)
            task_id = task.id
            invocation_id = None
            invocation_version = None
            execution_version = None
            if transition == "claim":
                invocation, _ = await _create(creator, task_id)
                execution = await service.active_execution_for(
                    creator,
                    invocation.id,
                )
                assert execution is not None
                invocation_id = invocation.id
                invocation_version = invocation.state_version
                execution_version = execution.state_version
                await creator.rollback()

            @asynccontextmanager
            async def receipt_wins_after_routing_read(locked_task_id):
                assert locked_task_id == task_id
                await persist_active_worker_receipt(sessions, task_id)
                yield

            monkeypatch.setattr(
                service,
                "capability_task_lock",
                receipt_wins_after_routing_read,
            )

            with pytest.raises(
                service.CapabilityConflictError,
                match="termination receipt",
            ):
                if transition == "create":
                    await _create(creator, task_id, key="wal-race")
                else:
                    await service.claim_execution(
                        creator,
                        invocation_id=invocation_id,
                        expected_invocation_version=invocation_version,
                        expected_execution_version=execution_version,
                        handle_kind="fake_run",
                        handle_id="must-not-claim",
                    )

            if transition == "create":
                assert await creator.scalar(
                    select(func.count(CapabilityInvocation.id))
                ) == 0
            else:
                current = await creator.get(
                    CapabilityInvocation,
                    invocation_id,
                    populate_existing=True,
                )
                assert current is not None
                assert current.status == "queued"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_active_termination_receipt_blocks_capability_stage_and_claim(
    db_session,
    db_factory,
):
    task = await _task(db_session)
    invocation, _ = await _create(db_session, task.id)
    execution = await service.active_execution_for(db_session, invocation.id)
    assert execution is not None
    task_id = task.id
    invocation_id = invocation.id
    invocation_version = invocation.state_version
    execution_version = execution.state_version
    await db_session.rollback()
    await persist_active_worker_receipt(db_factory, task_id)
    stage = AsyncMock()

    with pytest.raises(
        service.CapabilityConflictError,
        match="termination receipt",
    ):
        await service.stage_and_claim_execution(
            db_session,
            invocation_id=invocation_id,
            expected_invocation_version=invocation_version,
            expected_execution_version=execution_version,
            stage=stage,
        )

    stage.assert_not_awaited()
    stored_invocation = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    stored_execution = await service.active_execution_for(
        db_session,
        invocation_id,
    )
    assert stored_invocation.status == "queued"
    assert stored_execution is not None
    assert stored_execution.status == "queued"


@pytest.mark.asyncio
async def test_active_termination_receipt_blocks_capability_resume(
    db_session,
    db_factory,
):
    task = await _task(db_session)
    invocation, _ = await _create(db_session, task.id)
    execution = await service.active_execution_for(db_session, invocation.id)
    assert execution is not None
    invocation, execution = await service.claim_execution(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=invocation.state_version,
        expected_execution_version=execution.state_version,
        handle_kind="fake_run",
        handle_id="receipt-fenced-run",
    )
    invocation, execution = await service.mark_execution_waiting(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=invocation.state_version,
        expected_execution_version=execution.state_version,
    )
    invocation_id = invocation.id
    invocation_version = invocation.state_version
    execution_version = execution.state_version
    await persist_active_worker_receipt(db_factory, task.id)

    with pytest.raises(
        service.CapabilityConflictError,
        match="termination receipt",
    ):
        await service.resume_waiting_execution(
            db_session,
            invocation_id=invocation_id,
            expected_invocation_version=invocation_version,
            expected_execution_version=execution_version,
        )

    current = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    assert current.status == "waiting_user"


@pytest.mark.asyncio
async def test_unregistered_and_agent_requests_are_explicitly_rejected(
    db_session,
    monkeypatch,
):
    task = await _task(db_session)
    unregister_capability("plan")
    with pytest.raises(service.CapabilityUnavailableError, match="not registered"):
        await _create(db_session, task.id)

    with pytest.raises(
        service.CapabilityUnsupportedScopeError,
        match="exact terminal Task generation",
    ):
        # The dark-rollout switch must not enable this stub before exact
        # terminal arbitration and durable resume admission are connected.
        monkeypatch.setattr(settings, "auto_capability_enabled", True)
        await service.create_agent_invocation(
            db_session,
            task_id=task.id,
            capability_key="plan",
        )


@pytest.mark.asyncio
async def test_ready_result_keeps_slot_until_consumed(db_session):
    task = await _task(db_session)
    invocation, _ = await _create(db_session, task.id)
    execution = await service.active_execution_for(db_session, invocation.id)
    assert execution is not None

    invocation, execution = await service.claim_execution(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=invocation.state_version,
        expected_execution_version=execution.state_version,
        handle_kind="fake_run",
        handle_id="run-1",
    )
    invocation, execution = await service.complete_execution(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=invocation.state_version,
        expected_execution_version=execution.state_version,
        output_kind="plan_version",
        output_id=123,
        output_hash="a" * 64,
    )

    assert invocation.status == "ready"
    assert invocation.active_task_id == task.id
    assert execution.status == "completed"
    assert execution.active_invocation_id is None
    invocation_id = invocation.id
    ready_version = invocation.state_version
    with pytest.raises(service.CapabilityConflictError, match="active"):
        await _create(db_session, task.id, key="blocked-by-ready")

    completed = await service.consume_ready_invocation(
        db_session,
        invocation_id=invocation_id,
        expected_state_version=ready_version,
    )
    assert completed.status == "completed"
    assert completed.active_task_id is None
    next_invocation, created = await _create(
        db_session,
        task.id,
        key="after-consume",
    )
    assert created is True
    assert next_invocation.id != invocation_id


@pytest.mark.asyncio
async def test_consume_ready_result_requires_exact_completed_output_execution(
    db_session,
):
    task = await _task(db_session)
    invocation, _ = await _create(db_session, task.id)
    execution = await service.active_execution_for(db_session, invocation.id)
    assert execution is not None
    invocation, execution = await service.claim_execution(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=invocation.state_version,
        expected_execution_version=execution.state_version,
        handle_kind="fake_run",
        handle_id="tampered-ready-run",
    )
    invocation, execution = await service.complete_execution(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=invocation.state_version,
        expected_execution_version=execution.state_version,
        output_kind="plan_version",
        output_id=123,
        output_hash="a" * 64,
    )
    invocation_id = invocation.id
    task_id = task.id
    ready_version = invocation.state_version
    execution.output_hash = "b" * 64
    await db_session.commit()

    with pytest.raises(
        service.CapabilityConflictError,
        match="exact completed output execution",
    ):
        await service.consume_ready_invocation(
            db_session,
            invocation_id=invocation_id,
            expected_state_version=ready_version,
        )

    stored = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    assert stored is not None
    assert stored.status == "ready"
    assert stored.active_task_id == task_id


@pytest.mark.asyncio
async def test_waiting_execution_can_resume_with_dual_version_fence(db_session):
    task = await _task(db_session)
    invocation, _ = await _create(db_session, task.id)
    execution = await service.active_execution_for(db_session, invocation.id)
    assert execution is not None
    invocation, execution = await service.claim_execution(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=1,
        expected_execution_version=1,
        handle_kind="fake_run",
        handle_id="waiting-run",
    )
    invocation, execution = await service.mark_execution_waiting(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=invocation.state_version,
        expected_execution_version=execution.state_version,
    )

    invocation, execution = await service.resume_waiting_execution(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=invocation.state_version,
        expected_execution_version=execution.state_version,
    )
    assert invocation.status == "running"
    assert execution.status == "running"
    assert execution.heartbeat_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("output_id", [True, False, 0, -1])
async def test_complete_rejects_non_positive_or_boolean_output_id(
    db_session,
    output_id,
):
    task = await _task(db_session)
    invocation, _ = await _create(db_session, task.id)
    execution = await service.active_execution_for(db_session, invocation.id)
    assert execution is not None
    invocation, execution = await service.claim_execution(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=1,
        expected_execution_version=1,
        handle_kind="fake_run",
        handle_id="invalid-output",
    )
    with pytest.raises(service.CapabilityValidationError, match="positive integer"):
        await service.complete_execution(
            db_session,
            invocation_id=invocation.id,
            expected_invocation_version=invocation.state_version,
            expected_execution_version=execution.state_version,
            output_kind="plan_version",
            output_id=output_id,
            output_hash="a" * 64,
        )


@pytest.mark.asyncio
async def test_request_key_and_payload_boundaries(db_session):
    task = await _task(db_session)
    with pytest.raises(service.CapabilityValidationError, match="capability key"):
        await service.create_human_invocation(
            db_session,
            task_id=task.id,
            capability_key="BAD key",
            request_payload={},
            idempotency_key="invalid-key",
            requested_by_user_id=7,
        )
    with pytest.raises(service.CapabilityValidationError, match="32768"):
        await service.create_human_invocation(
            db_session,
            task_id=task.id,
            capability_key="plan",
            request_payload={"text": "界" * 20_000},
            idempotency_key="oversized",
            requested_by_user_id=7,
        )


@pytest.mark.asyncio
async def test_failed_attempt_adds_one_execution_and_preserves_invocation(db_session):
    task = await _task(db_session)
    invocation, _ = await _create(db_session, task.id)
    first = await service.active_execution_for(db_session, invocation.id)
    assert first is not None
    invocation, first = await service.claim_execution(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=1,
        expected_execution_version=1,
        handle_kind="fake_run",
        handle_id="run-retry-1",
    )

    invocation, failed, retry = await service.fail_execution(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=invocation.state_version,
        expected_execution_version=first.state_version,
        error_code="transient",
        error_message="try again",
    )

    assert failed.status == "failed"
    assert retry is not None
    assert retry.attempt == 2
    assert invocation.status == "queued"
    assert invocation.active_task_id == task.id
    executions = list(
        (
            await db_session.execute(
                select(CapabilityExecution)
                .where(CapabilityExecution.invocation_id == invocation.id)
                .order_by(CapabilityExecution.attempt)
            )
        ).scalars()
    )
    assert [item.status for item in executions] == ["failed", "queued"]


@pytest.mark.asyncio
async def test_stale_state_version_is_rejected(db_session):
    task = await _task(db_session)
    invocation, _ = await _create(db_session, task.id)
    execution = await service.active_execution_for(db_session, invocation.id)
    assert execution is not None

    with pytest.raises(service.CapabilityConflictError, match="Stale invocation"):
        await service.claim_execution(
            db_session,
            invocation_id=invocation.id,
            expected_invocation_version=99,
            expected_execution_version=execution.state_version,
            handle_kind="fake_run",
            handle_id="stale",
        )


@pytest.mark.asyncio
async def test_active_cancel_waits_for_executor_cleanup(db_session):
    task = await _task(db_session)
    invocation, _ = await _create(db_session, task.id)
    execution = await service.active_execution_for(db_session, invocation.id)
    assert execution is not None
    invocation, execution = await service.claim_execution(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=1,
        expected_execution_version=1,
        handle_kind="fake_run",
        handle_id="cancel-me",
    )

    invocation = await service.cancel_invocation(
        db_session,
        invocation_id=invocation.id,
        expected_state_version=invocation.state_version,
    )
    execution = await service.active_execution_for(db_session, invocation.id)
    assert execution is not None
    assert invocation.status == "cancelling"
    assert execution.status == "cancelling"
    assert invocation.active_task_id == task.id

    invocation, execution = await service.mark_execution_cancelled(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=invocation.state_version,
        expected_execution_version=execution.state_version,
    )
    assert invocation.status == "cancelled"
    assert invocation.active_task_id is None
    assert execution.active_invocation_id is None


@pytest.mark.asyncio
async def test_atomic_stage_callback_failure_rolls_back_everything(db_session):
    task = await _task(db_session)
    task_id = task.id
    invocation, _ = await _create(db_session, task.id)
    invocation_id = invocation.id
    execution = await service.active_execution_for(db_session, invocation_id)
    assert execution is not None

    async def stage(db, locked_task, _invocation, _execution):
        locked_task.description = "must roll back"
        await db.flush()
        raise service.CapabilityConflictError("stage failed")

    with pytest.raises(service.CapabilityConflictError, match="stage failed"):
        await service.stage_and_claim_execution(
            db_session,
            invocation_id=invocation_id,
            expected_invocation_version=invocation.state_version,
            expected_execution_version=execution.state_version,
            stage=stage,
        )

    stored_task = await db_session.get(Task, task_id, populate_existing=True)
    stored_invocation = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    stored_execution = await service.active_execution_for(db_session, invocation_id)
    assert stored_task.description is None
    assert stored_invocation.status == "queued"
    assert stored_execution is not None
    assert stored_execution.status == "queued"
    assert stored_execution.handle_id is None


@pytest.mark.asyncio
async def test_atomic_stage_cancellation_rolls_back_callback_writes(db_session):
    task = await _task(db_session)
    task_id = task.id
    invocation, _ = await _create(db_session, task.id)
    invocation_id = invocation.id
    execution = await service.active_execution_for(db_session, invocation_id)
    assert execution is not None

    async def stage(db, locked_task, _invocation, _execution):
        locked_task.description = "cancelled staging"
        await db.flush()
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await service.stage_and_claim_execution(
            db_session,
            invocation_id=invocation_id,
            expected_invocation_version=invocation.state_version,
            expected_execution_version=execution.state_version,
            stage=stage,
        )

    stored_task = await db_session.get(Task, task_id, populate_existing=True)
    stored_invocation = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    assert stored_task.description is None
    assert stored_invocation.status == "queued"


@pytest.mark.asyncio
async def test_rollback_safely_settles_under_anyio_level_cancellation():
    from anyio import CancelScope

    rollback_started = asyncio.Event()
    allow_rollback = asyncio.Event()
    rollback_completed = False

    class FakeSession:
        async def rollback(self):
            nonlocal rollback_completed
            rollback_started.set()
            await allow_rollback.wait()
            rollback_completed = True

    async def release_rollback():
        await rollback_started.wait()
        await asyncio.sleep(0)
        allow_rollback.set()

    releaser = asyncio.create_task(release_rollback())
    try:
        with CancelScope() as scope:
            scope.cancel()
            with pytest.raises(asyncio.CancelledError):
                await service._rollback_safely(FakeSession())
        await releaser
    finally:
        if not releaser.done():
            releaser.cancel()
            await asyncio.gather(releaser, return_exceptions=True)

    assert rollback_completed is True


@pytest.mark.asyncio
async def test_locked_aggregate_refreshes_preloaded_stale_rows(
    db_session,
    db_factory,
):
    task = await _task(db_session)
    invocation, _ = await _create(db_session, task.id)
    invocation_id = invocation.id

    async with db_factory() as stale_db:
        stale = await stale_db.get(CapabilityInvocation, invocation_id)
        assert stale is not None and stale.state_version == 1

        execution = await service.active_execution_for(db_session, invocation_id)
        assert execution is not None
        running, _ = await service.claim_execution(
            db_session,
            invocation_id=invocation_id,
            expected_invocation_version=1,
            expected_execution_version=1,
            handle_kind="fake_run",
            handle_id="fresh-owner",
        )

        with pytest.raises(service.CapabilityConflictError, match="Stale invocation"):
            await service.cancel_invocation(
                stale_db,
                invocation_id=invocation_id,
                expected_state_version=1,
            )
        refreshed = await stale_db.get(
            CapabilityInvocation,
            invocation_id,
            populate_existing=True,
        )
        assert refreshed.status == "running"
        assert refreshed.state_version == running.state_version


@pytest.mark.asyncio
async def test_ready_result_can_be_invalidated_without_mutating_execution(db_session):
    task = await _task(db_session)
    invocation, _ = await _create(db_session, task.id)
    execution = await service.active_execution_for(db_session, invocation.id)
    assert execution is not None
    invocation, execution = await service.claim_execution(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=1,
        expected_execution_version=1,
        handle_kind="fake_run",
        handle_id="ready-stale",
    )
    invocation, execution = await service.complete_execution(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=invocation.state_version,
        expected_execution_version=execution.state_version,
        output_kind="plan_version",
        output_id=123,
        output_hash="f" * 64,
    )

    stale, completed = await service.mark_ready_invocation_stale(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=invocation.state_version,
        expected_execution_version=execution.state_version,
        error_code="subject_changed",
        error_message="HEAD moved",
    )

    assert stale.status == "stale"
    assert stale.active_task_id is None
    assert stale.result_hash == "f" * 64
    assert completed.status == "completed"
    assert completed.output_id == 123
    assert completed.output_hash == "f" * 64
