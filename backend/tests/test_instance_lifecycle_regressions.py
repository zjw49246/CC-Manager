"""Focused regressions for Instance admission, recovery, and shutdown safety."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from backend.models.instance import Instance
from backend.models.task import Task
from backend.services.dispatcher import GlobalDispatcher
from backend.services.instance_manager import InstanceManager
from backend.services.ralph_loop import RalphLoop


def _mock_instance_manager() -> MagicMock:
    manager = MagicMock()
    manager.processes = {}
    manager._tasks = {}
    manager._consumer_records = {}
    manager._process_groups = {}
    manager._container_exec_processes = {}
    manager.is_running = MagicMock(return_value=False)
    return manager


def _dispatcher(db_factory, instance_manager=None) -> GlobalDispatcher:
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    return GlobalDispatcher(
        db_factory=db_factory,
        instance_manager=instance_manager or _mock_instance_manager(),
        broadcaster=broadcaster,
    )


async def _instances(db_factory) -> list[Instance]:
    async with db_factory() as db:
        result = await db.execute(select(Instance).order_by(Instance.id))
        return list(result.scalars().all())


@pytest.mark.asyncio
async def test_terminal_owner_evidence_consumes_capacity_everywhere(
    client,
    db_factory,
):
    """Terminal labels do not make PID/task-owned rows reusable capacity."""

    async with db_factory() as db:
        task = Task(
            title="persisted terminal owner",
            description="still attached",
            status="failed",
        )
        db.add(task)
        await db.flush()
        db.add_all(
            [
                Instance(
                    name="error-with-pid",
                    status="error",
                    pid=910_001,
                ),
                Instance(
                    name="stopped-with-owner",
                    status="stopped",
                    current_task_id=task.id,
                ),
            ]
        )
        await db.commit()

    dispatcher = _dispatcher(db_factory)
    dispatcher.configure_capacity_override(2)
    with (
        patch("backend.main.dispatcher._max_concurrent_instances_override", 2),
        patch("backend.services.dispatcher.settings") as dispatcher_settings,
    ):
        dispatcher_settings.min_idle_instances = 1

        response = await client.post(
            "/api/instances",
            json={"name": "must-not-fit"},
        )
        await dispatcher._ensure_instances()
        await dispatcher._ensure_min_idle_instances()

    assert response.status_code == 409
    assert "capacity limit" in response.json()["detail"].lower()
    instances = await _instances(db_factory)
    assert {instance.name for instance in instances} == {
        "error-with-pid",
        "stopped-with-owner",
    }
    assert not any(instance.status == "idle" for instance in instances)


@pytest.mark.asyncio
async def test_startup_cleanup_reconciles_owner_only_idle_and_error_rows(
    db_factory,
):
    """Owner-only dirty rows are selected even without a PID or running label."""

    dispatcher = _dispatcher(db_factory)
    expected: dict[int, int] = {}
    async with db_factory() as db:
        for status in ("idle", "error"):
            task = Task(
                title=f"{status} reverse owner",
                description="interrupted before PID publication",
                status="executing",
            )
            db.add(task)
            await db.flush()
            instance = Instance(
                name=f"{status}-owner-only",
                status=status,
                pid=None,
                current_task_id=task.id,
            )
            db.add(instance)
            await db.flush()
            task.instance_id = instance.id
            expected[instance.id] = task.id
        await db.commit()

    await dispatcher._cleanup_stale_state()

    async with db_factory() as db:
        for instance_id, task_id in expected.items():
            instance = await db.get(Instance, instance_id)
            task = await db.get(Task, task_id)
            assert instance.status == "error"
            assert instance.pid is None
            assert instance.current_task_id is None
            # A generation-zero active row predates exact source/transport
            # evidence. Replaying it from Task.description could duplicate
            # provider side effects, so startup now fails it closed.
            assert task.status == "failed"
            assert "provider-boundary proof" in task.error_message
            assert task.instance_id is None
            assert task.started_at is None
            assert task.completed_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("evidence_kind", ["consumer", "recovery"])
async def test_stale_reset_honors_consumer_or_recovery_only_running_evidence(
    db_factory,
    evidence_kind,
):
    """The safety reset must use InstanceManager's complete running verdict."""

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    manager = InstanceManager(db_factory, broadcaster)
    dispatcher = _dispatcher(db_factory, manager)

    async with db_factory() as db:
        task = Task(
            title=f"{evidence_kind} evidence",
            description="must remain owned",
            status="executing",
            retry_count=3,
            turn_generation=11,
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name=f"{evidence_kind}-only",
            status="running",
            pid=920_001,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        generation = dispatcher._task_lifecycle_generation(task)
        instance_id = instance.id
        task_id = task.id
        task_retry_count = task.retry_count
        task_turn_generation = task.turn_generation

    consumer = None
    recovery_process = None
    if evidence_kind == "consumer":
        consumer = asyncio.create_task(asyncio.Event().wait())
        manager._tasks[instance_id] = consumer
    else:
        recovery_process = MagicMock(pid=920_001, returncode=0)
        manager._mark_consumer_recovery_pending(
            instance_id,
            recovery_process,
            error=RuntimeError("durable recovery is unconfirmed"),
            tracked_generation=True,
            task_id=task_id,
            task_retry_count=task_retry_count,
            task_turn_generation=task_turn_generation,
            instance_pid=920_001,
            instance_started_at=None,
        )
        recovery_evidence = manager._consumer_recovery_pending[
            (instance_id, recovery_process)
        ]
        assert recovery_evidence.task_retry_count == task_retry_count
        assert recovery_evidence.task_turn_generation == task_turn_generation

    try:
        assert instance_id not in manager.processes
        assert instance_id not in manager._process_groups
        assert instance_id not in manager._container_exec_processes
        assert manager.is_running(instance_id) is True

        await dispatcher._reset_instance_if_stale(instance_id, generation)

        async with db_factory() as db:
            instance = await db.get(Instance, instance_id)
            task = await db.get(Task, task_id)
            assert instance.status == "running"
            assert instance.pid == 920_001
            assert instance.current_task_id == task_id
            assert task.status == "executing"
            assert task.instance_id == instance_id
    finally:
        if consumer is not None:
            manager._tasks.pop(instance_id, None)
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
        if recovery_process is not None:
            manager._clear_consumer_recovery_pending(
                instance_id,
                recovery_process,
            )


@pytest.mark.asyncio
async def test_delete_rejects_live_dispatch_lifecycle_on_idle_db_row(
    client,
    db_factory,
):
    """The in-memory lifecycle reservation wins over a stale idle DB label."""

    async with db_factory() as db:
        instance = Instance(name="idle-but-dispatching", status="idle")
        db.add(instance)
        await db.commit()
        instance_id = instance.id

    instance_manager = _mock_instance_manager()
    lifecycle_locks: dict[int, asyncio.Lock] = {}
    instance_manager._instance_lifecycle_lock = MagicMock(
        side_effect=lambda key: lifecycle_locks.setdefault(key, asyncio.Lock())
    )
    dispatcher = _dispatcher(db_factory, instance_manager)
    lifecycle = asyncio.create_task(asyncio.Event().wait())
    dispatcher._running_tasks[instance_id] = lifecycle
    ralph_loop = MagicMock()
    ralph_loop.stop = AsyncMock(return_value=True)

    try:
        with (
            patch("backend.main.dispatcher", dispatcher),
            patch("backend.main.instance_manager", instance_manager),
            patch("backend.main.ralph_loop", ralph_loop),
        ):
            response = await client.delete(f"/api/instances/{instance_id}")

        assert response.status_code == 409
        assert "reserved for a task lifecycle" in response.json()["detail"]
        assert dispatcher._running_tasks[instance_id] is lifecycle
        async with db_factory() as db:
            assert await db.get(Instance, instance_id) is not None
    finally:
        lifecycle.cancel()
        await asyncio.gather(lifecycle, return_exceptions=True)


def _ralph_loop() -> RalphLoop:
    return RalphLoop(
        db_factory=MagicMock(),
        instance_manager=MagicMock(),
        broadcaster=MagicMock(),
    )


@pytest.mark.asyncio
async def test_ralph_shutdown_cancels_and_awaits_every_loop():
    ralph = _ralph_loop()
    started = {instance_id: asyncio.Event() for instance_id in (1, 2)}
    settled = {instance_id: asyncio.Event() for instance_id in (1, 2)}

    async def loop(instance_id):
        started[instance_id].set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            settled[instance_id].set()

    ralph._loop = loop
    await ralph.start(1)
    await ralph.start(2)
    await asyncio.gather(*(event.wait() for event in started.values()))
    observed = dict(ralph._loops)

    await ralph.shutdown(timeout=1)

    assert ralph._shutting_down is True
    assert ralph._loops == {}
    assert all(task.done() for task in observed.values())
    assert all(event.is_set() for event in settled.values())


@pytest.mark.asyncio
async def test_ralph_shutdown_retains_loop_that_ignores_cancellation():
    ralph = _ralph_loop()
    started = asyncio.Event()
    ignored_cancellation = asyncio.Event()
    release = asyncio.Event()

    async def stubborn_loop(_instance_id):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            ignored_cancellation.set()
            await release.wait()

    ralph._loop = stubborn_loop
    await ralph.start(7)
    await started.wait()
    stubborn = ralph._loops[7]

    try:
        with pytest.raises(RuntimeError, match="ignored shutdown cancellation"):
            await ralph.shutdown(timeout=0.05)

        assert ignored_cancellation.is_set()
        assert ralph._loops[7] is stubborn
        assert ralph.is_running(7) is True
    finally:
        release.set()
        await asyncio.wait_for(stubborn, timeout=1)
        await ralph.shutdown(timeout=1)

    assert ralph._loops == {}
