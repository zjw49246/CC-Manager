"""Lifecycle and recovery tests for the generic CapabilityCoordinator."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from backend.config import settings
from backend.models.capability import CapabilityInvocation
from backend.models.task import Task
from backend.services import capability_coordinator as coordinator_module
from backend.services import capability_service
from backend.services.capability_coordinator import CapabilityCoordinator
from backend.services.capability_registry import (
    CapabilityDefinition,
    register_capability,
    unregister_capability,
)


CAPABILITY_KEY = "coordinator_test"


class RecordingExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []
        self.entered = asyncio.Event()
        self.release: asyncio.Event | None = None
        self.observe_errors: list[BaseException] = []
        self.ensure_error: BaseException | None = None

    async def _call(self, name: str, invocation_id: int) -> None:
        self.calls.append((name, invocation_id))
        self.entered.set()
        if name == "ensure_started" and self.ensure_error is not None:
            raise self.ensure_error
        if name == "observe" and self.observe_errors:
            raise self.observe_errors.pop(0)
        if self.release is not None:
            await self.release.wait()

    async def ensure_started(self, db, *, invocation_id: int):
        await self._call("ensure_started", invocation_id)

    async def observe(self, db, *, invocation_id: int):
        await self._call("observe", invocation_id)

    async def recover(self, db, *, invocation_id: int):
        await self._call("recover", invocation_id)

    async def cancel(self, db, *, invocation_id: int):
        await self._call("cancel", invocation_id)


class ConcurrencyExecutor(RecordingExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.release = asyncio.Event()
        self.two_entered = asyncio.Event()
        self.active = 0
        self.max_active = 0

    async def ensure_started(self, db, *, invocation_id: int):
        self.calls.append(("ensure_started", invocation_id))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        if self.active == 2:
            self.two_entered.set()
        try:
            await self.release.wait()
        finally:
            self.active -= 1


@pytest.fixture(autouse=True)
def capability_runtime(monkeypatch):
    previous = settings.capability_core_enabled
    settings.capability_core_enabled = True
    unregister_capability(CAPABILITY_KEY)
    monkeypatch.setattr(
        capability_service,
        "broadcast_capability_event",
        AsyncMock(),
    )
    monkeypatch.setattr(
        coordinator_module,
        "broadcast_capability_event",
        AsyncMock(),
    )
    yield
    unregister_capability(CAPABILITY_KEY)
    settings.capability_core_enabled = previous


def _register(executor: RecordingExecutor, *, max_attempts: int = 2) -> None:
    unregister_capability(CAPABILITY_KEY)
    register_capability(
        CapabilityDefinition(
            capability_key=CAPABILITY_KEY,
            executor_kind="recording",
            max_attempts=max_attempts,
            executor=executor,
        )
    )


async def _invocation(
    db_session,
    *,
    suffix: str = "1",
    source: str = "human_request",
):
    task = Task(title=f"Coordinator target {suffix}")
    db_session.add(task)
    await db_session.commit()
    if source == "delivery_controller":
        invocation, created = await capability_service.create_controller_invocation(
            db_session,
            task_id=task.id,
            capability_key=CAPABILITY_KEY,
            request_payload={"prompt": f"request {suffix}"},
            idempotency_key=f"coordinator-{suffix}",
        )
    else:
        invocation, created = await capability_service.create_human_invocation(
            db_session,
            task_id=task.id,
            capability_key=CAPABILITY_KEY,
            request_payload={"prompt": f"request {suffix}"},
            idempotency_key=f"coordinator-{suffix}",
            requested_by_user_id=7,
        )
    assert created is True
    execution = await capability_service.active_execution_for(
        db_session,
        invocation.id,
    )
    assert execution is not None
    return task, invocation, execution


async def _claim_running(db_session, invocation, execution):
    return await capability_service.claim_execution(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=invocation.state_version,
        expected_execution_version=execution.state_version,
        handle_kind="recording_run",
        handle_id=f"run-{invocation.id}",
    )


def _coordinator(db_factory, **overrides) -> CapabilityCoordinator:
    values = {
        "db_factory": db_factory,
        "poll_interval_seconds": 60.0,
        "max_concurrency": 2,
        "scan_limit": 8,
        "initial_backoff_seconds": 0.01,
        "max_backoff_seconds": 0.02,
    }
    values.update(overrides)
    return CapabilityCoordinator(**values)


@pytest.mark.asyncio
async def test_start_recovers_running_invocation_before_polling(
    db_session,
    db_factory,
):
    executor = RecordingExecutor()
    _register(executor)
    _, invocation, execution = await _invocation(db_session)
    invocation, _ = await _claim_running(db_session, invocation, execution)
    coordinator = _coordinator(db_factory)

    await coordinator.start()

    assert executor.calls == [("recover", invocation.id)]
    assert coordinator.is_running is True
    await coordinator.shutdown()
    assert coordinator.is_running is False


@pytest.mark.asyncio
async def test_concurrent_duplicate_ticks_share_one_inflight_callback(
    db_session,
    db_factory,
):
    executor = RecordingExecutor()
    executor.release = asyncio.Event()
    _register(executor)
    _, invocation, _ = await _invocation(db_session)
    coordinator = _coordinator(db_factory)

    first = asyncio.create_task(coordinator.run_once())
    await asyncio.wait_for(executor.entered.wait(), timeout=1)
    second = asyncio.create_task(coordinator.run_once())
    await asyncio.sleep(0)

    assert executor.calls == [("ensure_started", invocation.id)]
    executor.release.set()
    await asyncio.gather(first, second)


@pytest.mark.asyncio
async def test_executor_callbacks_obey_global_concurrency_bound(
    db_session,
    db_factory,
):
    executor = ConcurrencyExecutor()
    _register(executor)
    await _invocation(db_session, suffix="one")
    await _invocation(db_session, suffix="two")
    await _invocation(db_session, suffix="three")
    coordinator = _coordinator(db_factory, max_concurrency=2)

    tick = asyncio.create_task(coordinator.run_once())
    await asyncio.wait_for(executor.two_entered.wait(), timeout=1)

    assert executor.max_active == 2
    assert len(executor.calls) == 2
    executor.release.set()
    await asyncio.wait_for(tick, timeout=1)
    assert len(executor.calls) == 3
    assert executor.max_active == 2


@pytest.mark.asyncio
async def test_ready_invocation_is_owned_by_consumer_not_coordinator(
    db_session,
    db_factory,
):
    executor = RecordingExecutor()
    _register(executor)
    _, invocation, execution = await _invocation(db_session)
    invocation, execution = await _claim_running(
        db_session,
        invocation,
        execution,
    )
    invocation, _ = await capability_service.complete_execution(
        db_session,
        invocation_id=invocation.id,
        expected_invocation_version=invocation.state_version,
        expected_execution_version=execution.state_version,
        output_kind="test_result",
        output_id=123,
        output_hash="a" * 64,
    )
    coordinator = _coordinator(db_factory)

    await coordinator.run_once(recovery=True, scan_limit=None)

    assert invocation.status == "ready"
    assert executor.calls == []


@pytest.mark.asyncio
async def test_cancelling_invocation_routes_only_to_cancel(
    db_session,
    db_factory,
):
    executor = RecordingExecutor()
    _register(executor)
    _, invocation, execution = await _invocation(db_session)
    invocation, _ = await _claim_running(db_session, invocation, execution)
    invocation = await capability_service.cancel_invocation(
        db_session,
        invocation_id=invocation.id,
        expected_state_version=invocation.state_version,
    )
    coordinator = _coordinator(db_factory)

    await coordinator.run_once()

    assert invocation.status == "cancelling"
    assert executor.calls == [("cancel", invocation.id)]


@pytest.mark.asyncio
async def test_executor_exception_uses_bounded_backoff_then_recovery(
    db_session,
    db_factory,
):
    executor = RecordingExecutor()
    executor.observe_errors.append(RuntimeError("temporary adapter outage"))
    _register(executor)
    _, invocation, execution = await _invocation(db_session)
    invocation, _ = await _claim_running(db_session, invocation, execution)
    coordinator = _coordinator(db_factory)

    await coordinator.run_once()
    await coordinator.run_once()
    assert executor.calls == [("observe", invocation.id)]
    assert coordinator._retry_not_before[invocation.id] > 0

    await asyncio.sleep(0.015)
    await coordinator.run_once()

    assert executor.calls == [
        ("observe", invocation.id),
        ("recover", invocation.id),
    ]
    assert invocation.id not in coordinator._failure_counts


@pytest.mark.asyncio
async def test_permanent_executor_error_fails_active_execution(
    db_session,
    db_factory,
):
    executor = RecordingExecutor()
    executor.ensure_error = capability_service.CapabilityValidationError(
        "invalid immutable input"
    )
    _register(executor)
    _, invocation, _ = await _invocation(db_session)
    coordinator = _coordinator(db_factory)

    await coordinator.run_once()
    await db_session.refresh(invocation)

    assert invocation.status == "failed"
    assert invocation.error_code == "coordinator_executor_rejected"


@pytest.mark.asyncio
async def test_missing_executor_fails_closed_via_capability_core(
    db_session,
    db_factory,
):
    executor = RecordingExecutor()
    _register(executor)
    _, invocation, _ = await _invocation(db_session)
    unregister_capability(CAPABILITY_KEY)
    coordinator = _coordinator(db_factory)

    await coordinator.run_once()
    await db_session.refresh(invocation)

    assert invocation.status == "failed"
    assert invocation.error_code == "coordinator_executor_unavailable"


@pytest.mark.asyncio
async def test_disabled_flag_recovers_committed_queue_before_handle_creation(
    db_session,
    db_factory,
):
    executor = RecordingExecutor()
    _register(executor)
    _task, queued, execution = await _invocation(
        db_session,
        suffix="queued",
        source="delivery_controller",
    )
    assert queued.source == "delivery_controller"
    assert execution.handle_kind is None
    assert execution.handle_id is None
    queued_id = queued.id
    settings.capability_core_enabled = False
    coordinator = _coordinator(db_factory)

    blocked_task = Task(title="Must not be admitted while disabled")
    db_session.add(blocked_task)
    await db_session.commit()
    with pytest.raises(capability_service.CapabilityDisabledError):
        await capability_service.create_controller_invocation(
            db_session,
            task_id=blocked_task.id,
            capability_key=CAPABILITY_KEY,
            request_payload={"prompt": "must stay unadmitted"},
            idempotency_key="disabled-new-invocation",
        )
    assert await db_session.scalar(
        select(func.count(CapabilityInvocation.id))
    ) == 1

    await coordinator.start()

    assert executor.calls == [("recover", queued_id)]
    await db_session.refresh(queued)
    assert queued.status == "queued"
    await coordinator.shutdown()


@pytest.mark.asyncio
async def test_disabled_flag_normal_tick_starts_committed_queue(
    db_session,
    db_factory,
):
    executor = RecordingExecutor()
    _register(executor)
    _task, queued, execution = await _invocation(
        db_session,
        suffix="queued-live-toggle",
        source="delivery_controller",
    )
    assert execution.handle_kind is None
    assert execution.handle_id is None
    settings.capability_core_enabled = False
    coordinator = _coordinator(db_factory)

    await coordinator.run_once()

    assert executor.calls == [("ensure_started", queued.id)]


@pytest.mark.asyncio
async def test_shutdown_waits_for_callback_and_leaves_no_background_task(
    db_session,
    db_factory,
):
    executor = RecordingExecutor()
    executor.release = asyncio.Event()
    _register(executor)
    coordinator = _coordinator(db_factory)
    await coordinator.start()
    _, invocation, _ = await _invocation(db_session)

    coordinator.wake()
    await asyncio.wait_for(executor.entered.wait(), timeout=1)
    shutdown = asyncio.create_task(coordinator.shutdown())
    await asyncio.sleep(0)
    assert shutdown.done() is False

    executor.release.set()
    await asyncio.wait_for(shutdown, timeout=1)

    assert executor.calls == [("ensure_started", invocation.id)]
    assert coordinator.is_running is False
    assert coordinator._runner is None
    assert coordinator._inflight == {}


@pytest.mark.asyncio
async def test_shutdown_settles_complete_graph_under_anyio_cancellation(
    db_factory,
):
    from anyio import CancelScope

    coordinator = _coordinator(db_factory)
    runner_started = asyncio.Event()
    callback_started = asyncio.Event()
    release = asyncio.Event()

    async def wait_for_release(started: asyncio.Event) -> None:
        started.set()
        await release.wait()

    runner = asyncio.create_task(wait_for_release(runner_started))
    callback = asyncio.create_task(wait_for_release(callback_started))
    coordinator._runner = runner
    coordinator._inflight[1] = callback

    async def release_graph() -> None:
        await runner_started.wait()
        await callback_started.wait()
        await asyncio.sleep(0)
        release.set()

    releaser = asyncio.create_task(release_graph())
    try:
        with CancelScope() as scope:
            scope.cancel()
            with pytest.raises(asyncio.CancelledError):
                await coordinator.shutdown()
        await releaser
    finally:
        release.set()
        await asyncio.gather(runner, callback, releaser, return_exceptions=True)

    assert runner.done()
    assert callback.done()
    assert coordinator._runner is None
