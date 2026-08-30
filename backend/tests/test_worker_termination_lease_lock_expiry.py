"""Real WAL lock waits must never extend a stale termination lease."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.sql.dml import Update

from backend.database import Base
from backend.models.instance import Instance
from backend.models.task import Task
from backend.models.worker_task_termination import WorkerTaskTerminationReceipt
from backend.services import worker_task_termination as termination
from backend.services.instance_manager import InstanceManager
from backend.services.test_harness_owner_fence import (
    TEST_HARNESS_TERMINAL_GATE_KEY,
)


_LEASE_SECONDS = 0.45
_EXPIRY_MARGIN_SECONDS = 0.04


@asynccontextmanager
async def _wal_session_factory(path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{path}",
        connect_args={"timeout": 5},
    )
    async with engine.connect() as connection:
        await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
        await connection.exec_driver_sql("PRAGMA busy_timeout=5000")
        await connection.commit()
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    try:
        yield factory
    finally:
        await engine.dispose()


class _ExecuteObserver:
    def __init__(self, db, attempted: asyncio.Event, table_name: str):
        self._db = db
        self._attempted = attempted
        self._table_name = table_name

    def __getattr__(self, name):
        return getattr(self._db, name)

    async def execute(self, statement, *args, **kwargs):
        if (
            isinstance(statement, Update)
            and statement.table.name == self._table_name
        ):
            self._attempted.set()
        return await self._db.execute(statement, *args, **kwargs)


def _observed_factory(factory, attempted: asyncio.Event, table_name: str):
    @asynccontextmanager
    async def observed():
        async with factory() as db:
            yield _ExecuteObserver(db, attempted, table_name)

    return observed


async def _wait_past(deadline: datetime) -> None:
    """Wait on an Event scheduled beyond the wall-clock lease deadline."""

    expired = asyncio.Event()
    delay = max(
        0.0,
        (deadline - datetime.utcnow()).total_seconds()
        + _EXPIRY_MARGIN_SECONDS,
    )
    handle = asyncio.get_running_loop().call_later(delay, expired.set)
    try:
        await asyncio.wait_for(expired.wait(), timeout=2)
    finally:
        handle.cancel()
    assert datetime.utcnow() > deadline


async def _stage_executing_receipt(
    factory,
    task_id: int,
    *,
    operation_id: str,
    operation: str,
    lease_seconds: float = _LEASE_SECONDS,
):
    async with factory() as db:
        task = await db.get(Task, task_id)
        assert task is not None
        request = {
            "version": 2,
            "operation_id": operation_id,
            "task_id": task.id,
            "operation": operation,
            "manager_worker_id": 41,
            "expected_remote": {
                "status": task.status,
                "retry_count": task.retry_count,
                "turn_generation": task.turn_generation,
            },
            "manager_handoff": None,
        }
        receipt = await termination.stage_worker_receipt(
            db,
            task_id=task.id,
            operation_id=operation_id,
            operation=operation,
            request_payload=request,
            request_digest=termination.canonical_json_digest(request),
        )
        lease_expires_at = datetime.utcnow() + timedelta(
            seconds=lease_seconds
        )
        receipt.status = "executing"
        receipt.state_version += 1
        receipt.execution_token = operation_id
        receipt.attempt_count = 1
        receipt.next_reconcile_at = lease_expires_at
        receipt.updated_at = datetime.utcnow()
        await db.commit()
        fence = termination._WorkerTerminationExecutionFence(
            task_id=receipt.task_id,
            operation_id=receipt.operation_id,
            operation=receipt.operation,
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
        return fence


async def _hold_receipt_writer(factory, operation_id: str):
    blocker = factory()
    locked = await blocker.execute(
        update(WorkerTaskTerminationReceipt)
        .where(WorkerTaskTerminationReceipt.operation_id == operation_id)
        .values(state_version=WorkerTaskTerminationReceipt.state_version)
        .execution_options(synchronize_session=False)
    )
    assert locked.rowcount == 1
    return blocker


@pytest.mark.asyncio
async def test_heartbeat_waiting_on_receipt_writer_cannot_renew_expired_lease(
    tmp_path,
    monkeypatch,
):
    async with _wal_session_factory(
        tmp_path / "heartbeat-lock-expiry.db"
    ) as factory:
        async with factory() as db:
            task = Task(title="heartbeat lock expiry", status="completed")
            db.add(task)
            await db.commit()
            task_id = task.id
        fence = await _stage_executing_receipt(
            factory,
            task_id,
            operation_id="1" * 32,
            operation="cancel",
        )
        blocker = await _hold_receipt_writer(factory, fence.operation_id)
        writer_attempted = asyncio.Event()
        stop = asyncio.Event()
        ownership_lost = asyncio.Event()
        monkeypatch.setattr(
            termination,
            "_WORKER_EXECUTION_HEARTBEAT_SECONDS",
            0.01,
        )
        monkeypatch.setattr(
            termination,
            "_WORKER_EXECUTION_HEARTBEAT_RETRY_SECONDS",
            0.01,
        )
        monkeypatch.setattr(
            termination,
            "_WORKER_EXECUTION_LEASE_SECONDS",
            5.0,
        )
        heartbeat = asyncio.create_task(
            termination._heartbeat_worker_execution(
                _observed_factory(
                    factory,
                    writer_attempted,
                    WorkerTaskTerminationReceipt.__tablename__,
                ),
                fence,
                stop=stop,
                ownership_lost=ownership_lost,
            )
        )
        try:
            await asyncio.wait_for(writer_attempted.wait(), timeout=2)
            assert not heartbeat.done()
            await _wait_past(fence.lease_expires_at)
            await blocker.commit()
            await asyncio.wait_for(heartbeat, timeout=2)
        finally:
            if not heartbeat.done():
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
            await blocker.rollback()
            await blocker.close()

        assert ownership_lost.is_set()
        async with factory() as db:
            receipt = await db.get(
                WorkerTaskTerminationReceipt,
                fence.operation_id,
            )
        assert receipt.status == "executing"
        assert receipt.execution_token == fence.execution_token
        assert receipt.state_version == fence.state_version
        assert receipt.next_reconcile_at == fence.lease_expires_at
        assert receipt.next_reconcile_at < datetime.utcnow()


@pytest.mark.asyncio
async def test_final_success_waiting_on_task_lock_cannot_write_after_lease_expiry(
    tmp_path,
):
    async with _wal_session_factory(
        tmp_path / "final-success-lock-expiry.db"
    ) as factory:
        async with factory() as db:
            task = Task(
                title="final success lock expiry",
                status="completed",
                retry_count=2,
                turn_generation=4,
                completed_at=datetime.utcnow(),
            )
            db.add(task)
            await db.commit()
            task_id = task.id
        fence = await _stage_executing_receipt(
            factory,
            task_id,
            operation_id="2" * 32,
            operation="cancel",
        )
        blocker = await _hold_receipt_writer(factory, fence.operation_id)
        task_writer_attempted = asyncio.Event()
        execution_db = factory()
        observed_db = _ExecuteObserver(
            execution_db,
            task_writer_attempted,
            Task.__tablename__,
        )
        execution = asyncio.create_task(
            termination._execute_owned_worker_receipt(observed_db, fence)
        )
        try:
            await asyncio.wait_for(task_writer_attempted.wait(), timeout=2)
            assert not execution.done()
            await _wait_past(fence.lease_expires_at)
            await blocker.commit()
            resulting = await asyncio.wait_for(execution, timeout=2)
            await execution_db.refresh(resulting)
            resulting_snapshot = (
                resulting.status,
                resulting.execution_token,
                resulting.state_version,
                resulting.result_payload,
                resulting.result_digest,
            )
        finally:
            if not execution.done():
                execution.cancel()
                await asyncio.gather(execution, return_exceptions=True)
            await execution_db.rollback()
            await execution_db.close()
            await blocker.rollback()
            await blocker.close()

        assert resulting_snapshot == (
            "executing",
            fence.execution_token,
            fence.state_version,
            None,
            None,
        )
        async with factory() as db:
            durable = await db.get(
                WorkerTaskTerminationReceipt,
                fence.operation_id,
            )
            task = await db.get(Task, task_id)
        assert durable.status == "executing"
        assert durable.result_payload is None
        assert durable.next_reconcile_at < datetime.utcnow()
        assert task.status == "completed"
        assert task.retry_count == 2
        assert task.turn_generation == 4
        assert TEST_HARNESS_TERMINAL_GATE_KEY not in (task.metadata_ or {})


@pytest.mark.asyncio
async def test_terminal_consumer_timeout_lock_wait_cannot_cancel_after_lease_expiry(
    tmp_path,
):
    async with _wal_session_factory(
        tmp_path / "terminal-consumer-lock-expiry.db"
    ) as factory:
        started_at = datetime(2026, 8, 7, 4, 5, 6)
        pid = 76_301
        async with factory() as db:
            instance = Instance(
                name="terminal consumer lease expiry",
                status="running",
                pid=pid,
                started_at=started_at,
            )
            db.add(instance)
            await db.flush()
            task = Task(
                title="terminal consumer lease expiry",
                status="executing",
                retry_count=3,
                turn_generation=6,
                instance_id=instance.id,
                started_at=started_at,
            )
            db.add(task)
            await db.flush()
            instance.current_task_id = task.id
            await db.commit()
            instance_id = instance.id
            task_id = task.id
        fence = await _stage_executing_receipt(
            factory,
            task_id,
            operation_id="3" * 32,
            operation="stop_session",
        )
        blocker = await _hold_receipt_writer(factory, fence.operation_id)
        task_writer_attempted = asyncio.Event()
        broadcaster = MagicMock(broadcast=AsyncMock())
        manager = InstanceManager(
            _observed_factory(
                factory,
                task_writer_attempted,
                Task.__tablename__,
            ),
            broadcaster,
        )
        process = MagicMock(pid=pid, returncode=0)
        process.send_signal = MagicMock()
        consumer_started = asyncio.Event()
        release_consumer = asyncio.Event()
        cancellation_seen = asyncio.Event()

        async def terminal_consumer():
            consumer_started.set()
            try:
                await release_consumer.wait()
            except asyncio.CancelledError:
                cancellation_seen.set()
                raise

        consumer = asyncio.create_task(terminal_consumer())
        await consumer_started.wait()
        manager.processes[instance_id] = process
        manager._track_output_consumer(
            instance_id,
            process,
            consumer,
            provider="codex",
            task_id=task_id,
            task_retry_count=3,
            task_turn_generation=6,
            instance_started_at=started_at,
        )
        manager._generation_reap_confirmed = MagicMock(return_value=True)
        manager._signal_managed_process_tree = AsyncMock()
        stopping = asyncio.create_task(
            manager.stop(
                instance_id,
                expected_task_id=task_id,
                expected_pid=pid,
                expected_started_at=started_at,
                task_status="cancelled",
                terminal_consumer_timeout=0.01,
                consumer_cancel_timeout=0.1,
                yield_to_worker_task_termination=False,
                worker_termination_operation_id=fence.operation_id,
                worker_termination_operation=fence.operation,
                worker_termination_execution_token=fence.execution_token,
                worker_termination_state_version=fence.state_version,
            )
        )
        blocker_released = False
        try:
            await asyncio.wait_for(task_writer_attempted.wait(), timeout=2)
            assert not stopping.done()
            assert not consumer.cancelled()
            assert not cancellation_seen.is_set()
            await _wait_past(fence.lease_expires_at)
            await blocker.commit()
            blocker_released = True
            stopped = await asyncio.wait_for(stopping, timeout=2)
        finally:
            if not blocker_released:
                await blocker.rollback()
            if not stopping.done():
                stopping.cancel()
                await asyncio.gather(stopping, return_exceptions=True)
            await blocker.close()
            release_consumer.set()
            await asyncio.gather(consumer, return_exceptions=True)

        assert stopped is False
        assert not consumer.cancelled()
        assert not cancellation_seen.is_set()
        manager._signal_managed_process_tree.assert_not_awaited()
        process.send_signal.assert_not_called()
        broadcaster.broadcast.assert_not_awaited()
        async with factory() as db:
            durable_task = await db.get(Task, task_id)
            durable_instance = await db.get(Instance, instance_id)
            durable_receipt = await db.get(
                WorkerTaskTerminationReceipt,
                fence.operation_id,
            )
        assert durable_task.status == "executing"
        assert durable_task.completed_at is None
        assert durable_task.instance_id == instance_id
        assert durable_instance.status == "running"
        assert durable_instance.pid == pid
        assert durable_instance.current_task_id == task_id
        assert durable_receipt.status == "executing"
        assert durable_receipt.next_reconcile_at < datetime.utcnow()


async def _stage_running_stop_owner(
    factory,
    *,
    operation_id: str,
    lease_seconds: float = _LEASE_SECONDS,
):
    started_at = datetime(2026, 8, 7, 5, 6, 7)
    pid = 76_302
    async with factory() as db:
        instance = Instance(
            name=f"publication lease {operation_id[0]}",
            status="running",
            pid=pid,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title=f"publication lease {operation_id[0]}",
            status="executing",
            retry_count=4,
            turn_generation=7,
            instance_id=instance.id,
            started_at=started_at,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    fence = await _stage_executing_receipt(
        factory,
        task_id,
        operation_id=operation_id,
        operation="stop_session",
        lease_seconds=lease_seconds,
    )
    process = MagicMock(pid=pid, returncode=None)
    process.send_signal = MagicMock()

    async def wait_for_exit():
        process.returncode = 130
        return process.returncode

    process.wait = AsyncMock(side_effect=wait_for_exit)
    return instance_id, task_id, started_at, fence, process


def _publication_event_names(calls):
    return [
        payload.get("event") or payload.get("event_type")
        for _, payload in calls
    ]


@pytest.mark.asyncio
async def test_stop_publication_rechecks_lease_after_blocked_first_event(
    tmp_path,
):
    async with _wal_session_factory(
        tmp_path / "stop-publication-lock-expiry.db"
    ) as factory:
        (
            instance_id,
            task_id,
            started_at,
            fence,
            process,
        ) = await _stage_running_stop_owner(
            factory,
            operation_id="4" * 32,
        )
        first_broadcast_started = asyncio.Event()
        release_first_broadcast = asyncio.Event()
        calls = []

        async def broadcast(channel, payload):
            calls.append((channel, dict(payload)))
            if len(calls) == 1:
                assert datetime.utcnow() < fence.lease_expires_at
                first_broadcast_started.set()
                await release_first_broadcast.wait()

        manager = InstanceManager(
            factory,
            MagicMock(broadcast=AsyncMock(side_effect=broadcast)),
        )
        manager.processes[instance_id] = process
        stopping = asyncio.create_task(
            manager.stop(
                instance_id,
                expected_task_id=task_id,
                expected_pid=process.pid,
                expected_started_at=started_at,
                task_status="cancelled",
                yield_to_worker_task_termination=False,
                worker_termination_operation_id=fence.operation_id,
                worker_termination_operation=fence.operation,
                worker_termination_execution_token=(
                    fence.execution_token
                ),
                worker_termination_state_version=fence.state_version,
            )
        )
        try:
            await asyncio.wait_for(first_broadcast_started.wait(), timeout=2)
            assert _publication_event_names(calls) == ["status_change"]
            assert not stopping.done()
            await _wait_past(fence.lease_expires_at)
            release_first_broadcast.set()
            stopped = await asyncio.wait_for(stopping, timeout=2)
        finally:
            release_first_broadcast.set()
            if not stopping.done():
                stopping.cancel()
                await asyncio.gather(stopping, return_exceptions=True)

        assert stopped is True
        assert _publication_event_names(calls) == ["status_change"]
        async with factory() as db:
            durable_task = await db.get(Task, task_id)
            durable_instance = await db.get(Instance, instance_id)
            durable_receipt = await db.get(
                WorkerTaskTerminationReceipt,
                fence.operation_id,
            )
        assert durable_task.status == "cancelled"
        assert durable_task.completed_at is not None
        assert durable_task.instance_id == instance_id
        assert durable_instance.status == "idle"
        assert durable_instance.pid is None
        assert durable_instance.current_task_id is None
        assert durable_receipt.status == "executing"
        assert durable_receipt.next_reconcile_at < datetime.utcnow()


@pytest.mark.asyncio
async def test_stop_publication_emits_all_events_while_lease_is_live(tmp_path):
    async with _wal_session_factory(
        tmp_path / "stop-publication-live-lease.db"
    ) as factory:
        (
            instance_id,
            task_id,
            started_at,
            fence,
            process,
        ) = await _stage_running_stop_owner(
            factory,
            operation_id="5" * 32,
            lease_seconds=5.0,
        )
        calls = []

        async def broadcast(channel, payload):
            calls.append((channel, dict(payload)))

        manager = InstanceManager(
            factory,
            MagicMock(broadcast=AsyncMock(side_effect=broadcast)),
        )
        manager.processes[instance_id] = process

        assert await manager.stop(
            instance_id,
            expected_task_id=task_id,
            expected_pid=process.pid,
            expected_started_at=started_at,
            task_status="cancelled",
            yield_to_worker_task_termination=False,
            worker_termination_operation_id=fence.operation_id,
            worker_termination_operation=fence.operation,
            worker_termination_execution_token=fence.execution_token,
            worker_termination_state_version=fence.state_version,
        )

        assert _publication_event_names(calls) == [
            "status_change",
            "process_exit",
        ]
        assert calls[0][0] == "tasks"
        assert calls[1][0] == f"task:{task_id}"
        assert calls[1][1]["exit_code"] == 130
        async with factory() as db:
            durable_task = await db.get(Task, task_id)
            durable_instance = await db.get(Instance, instance_id)
        assert durable_task.status == "cancelled"
        assert durable_instance.status == "idle"
        assert durable_instance.current_task_id is None
