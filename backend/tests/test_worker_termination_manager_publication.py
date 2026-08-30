"""Exact-generation publication fences for Manager termination results."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from unittest.mock import patch

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

import backend.main as main_module
from backend.database import Base
from backend.models.task import Task
from backend.models.worker import Worker
from backend.services import worker_task_termination as termination


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


def _worker_success_receipt(manager) -> dict:
    completed_at = "2026-08-07T03:04:05.000000"
    result = {
        "version": 2,
        "operation_id": manager.operation_id,
        "task_id": manager.task_id,
        "operation": manager.operation,
        "request_digest": manager.request_digest,
        "task": {
            "id": manager.task_id,
            "status": "completed",
            "retry_count": manager.source_task_retry_count,
            "turn_generation": manager.source_task_turn_generation,
            "instance_id": None,
            "started_at": None,
            "completed_at": completed_at,
            "session_id": None,
            "error_message": None,
            "background_active": False,
        },
        "response": {"ok": True},
    }
    wire = {
        "version": 2,
        "operation_id": manager.operation_id,
        "task_id": manager.task_id,
        "side": "worker",
        "worker_id": None,
        "operation": manager.operation,
        "status": "succeeded",
        "state_version": 3,
        "source": termination.serialize_receipt(manager)["source"],
        "request_payload": manager.request_payload,
        "request_digest": manager.request_digest,
        "result_payload": result,
        "result_digest": termination.canonical_json_digest(result),
        "attempt_count": 1,
        "reconcile_count": 0,
        "last_error": None,
        "accepted_at": "2026-08-07T03:04:04.000000",
        "completed_at": completed_at,
        "ack_intent_at": None,
        "acknowledged_at": None,
        "created_at": "2026-08-07T03:04:03.000000",
        "updated_at": completed_at,
    }
    assert termination._receipt_wire_is_structurally_valid(wire)
    return wire


def _worker_acknowledged_receipt(success: dict) -> dict:
    acknowledged = deepcopy(success)
    acknowledged.update(
        status="acknowledged",
        state_version=4,
        acknowledged_at="2026-08-07T03:04:06.000000",
        updated_at="2026-08-07T03:04:06.000000",
    )
    assert termination._receipt_wire_is_structurally_valid(acknowledged)
    return acknowledged


async def _create_manager_operation(factory, *, title: str):
    async with factory() as db:
        worker = Worker(name=f"{title} worker", status="ready")
        db.add(worker)
        await db.flush()
        task = Task(
            title=title,
            status="in_progress",
            worker_id=worker.id,
            retry_count=2,
            turn_generation=7,
        )
        db.add(task)
        await db.commit()
        manager = await termination.create_or_resume_manager_receipt(
            db,
            task,
            operation="cancel",
        )
        return task.id, manager.operation_id, _worker_success_receipt(manager)


@pytest.mark.asyncio
async def test_delayed_manager_publication_cannot_cross_settle_and_next_turn(
    tmp_path,
    monkeypatch,
):
    """Coordinator A must drop G's event after B settles and advances to G+1.

    The expected implementation follows the repository's standard publication
    contract: after the durable apply commit, perform a fresh exact-generation
    Task read/CAS and hold that row lock through WebSocket publication.  The
    execute shim pauses A immediately before that post-commit guard.  The
    broadcaster fallback also pauses today's unguarded implementation, making
    the regression fail with the actual stale event instead of timing out.
    """

    async with _wal_session_factory(
        tmp_path / "manager-publication-race.db"
    ) as factory:
        task_id, operation_id, success = await _create_manager_operation(
            factory,
            title="delayed Manager terminal publication",
        )
        acknowledged = _worker_acknowledged_receipt(success)
        publication_waiting = asyncio.Event()
        release_publication = asyncio.Event()
        post_commit_guard_waiting = False
        applied_commit_finished = False
        events: list[tuple[str, dict]] = []

        session_a = factory()
        original_commit = AsyncSession.commit
        original_execute = AsyncSession.execute

        async def track_apply_commit(self):
            nonlocal applied_commit_finished
            await original_commit(self)
            if self is session_a:
                applied_commit_finished = True

        async def pause_post_commit_guard(self, statement, *args, **kwargs):
            nonlocal post_commit_guard_waiting
            if (
                self is session_a
                and applied_commit_finished
                and not post_commit_guard_waiting
            ):
                post_commit_guard_waiting = True
                publication_waiting.set()
                await release_publication.wait()
            return await original_execute(self, statement, *args, **kwargs)

        async def capture_broadcast(channel, data):
            # Current code has no post-commit generation guard.  Pause at its
            # first externally visible effect so coordinator B can still win
            # the same durable race and the stale payload remains observable.
            if not post_commit_guard_waiting:
                publication_waiting.set()
                await release_publication.wait()
            events.append((channel, dict(data)))

        monkeypatch.setattr(AsyncSession, "commit", track_apply_commit)
        monkeypatch.setattr(AsyncSession, "execute", pause_post_commit_guard)
        with patch.object(
            main_module.broadcaster,
            "broadcast",
            side_effect=capture_broadcast,
        ):
            apply_task = asyncio.create_task(
                termination.apply_manager_result(
                    session_a,
                    operation_id,
                    success,
                )
            )
            try:
                await asyncio.wait_for(publication_waiting.wait(), timeout=3)

                # A's terminal G and awaiting-ACK receipt are already durable.
                async with factory() as coordinator_b:
                    durable = await coordinator_b.get(Task, task_id)
                    receipt = await coordinator_b.get(
                        termination.WorkerTaskTerminationReceipt,
                        operation_id,
                    )
                    assert durable.status == "completed"
                    assert durable.retry_count == 2
                    assert durable.turn_generation == 7
                    assert receipt.status == "awaiting_ack"
                    assert receipt.active_task_id == task_id

                    async def proxy_request(_route, method, _path, **_kwargs):
                        if method == "GET":
                            return success
                        assert method == "POST"
                        return acknowledged

                    outcome = await termination.reconcile_manager_receipt(
                        coordinator_b,
                        operation_id,
                        proxy_request=proxy_request,
                    )
                    assert outcome.status == "settled"

                    # Releasing active_task_id is a prerequisite for G+1.  Use
                    # it in the same CAS that models the next coordinator turn.
                    advanced = await coordinator_b.execute(
                        update(Task)
                        .where(
                            Task.id == task_id,
                            Task.status == "completed",
                            Task.retry_count == 2,
                            Task.turn_generation == 7,
                            termination.no_active_worker_task_termination_predicate(),
                        )
                        .values(
                            status="executing",
                            turn_generation=8,
                            completed_at=None,
                        )
                    )
                    assert advanced.rowcount == 1
                    await coordinator_b.commit()
            finally:
                release_publication.set()

            await asyncio.wait_for(apply_task, timeout=3)
        await session_a.close()

        async with factory() as db:
            current = await db.get(Task, task_id)
            receipt = await db.get(
                termination.WorkerTaskTerminationReceipt,
                operation_id,
            )
        assert current.status == "executing"
        assert current.retry_count == 2
        assert current.turn_generation == 8
        assert receipt.status == "settled"
        assert receipt.active_task_id is None

        stale_terminal = [
            data
            for channel, data in events
            if channel == "tasks"
            and data.get("event") == "status_change"
            and data.get("new_status") == "completed"
            and data.get("task_retry_count") == 2
            and data.get("task_turn_generation") == 7
        ]
        malformed = [
            data
            for channel, data in events
            if channel == "tasks"
            and data.get("event") == "status_change"
            and (
                type(data.get("task_retry_count")) is not int
                or type(data.get("task_turn_generation")) is not int
            )
        ]
        assert not stale_terminal, events
        assert not malformed, events
        assert events == []


@pytest.mark.asyncio
async def test_manager_terminal_status_event_carries_exact_turn_identity(tmp_path):
    """A current Manager terminal result publishes its retry/turn identity."""

    async with _wal_session_factory(
        tmp_path / "manager-publication-identity.db"
    ) as factory:
        task_id, operation_id, success = await _create_manager_operation(
            factory,
            title="Manager terminal publication identity",
        )
        events: list[tuple[str, dict]] = []

        async def capture_broadcast(channel, data):
            events.append((channel, dict(data)))

        with patch.object(
            main_module.broadcaster,
            "broadcast",
            side_effect=capture_broadcast,
        ):
            async with factory() as coordinator:
                await termination.apply_manager_result(
                    coordinator,
                    operation_id,
                    success,
                )

        status_events = [
            data
            for channel, data in events
            if channel == "tasks"
            and data.get("event") == "status_change"
            and data.get("task_id") == task_id
        ]
        assert len(status_events) == 1
        assert status_events[0]["new_status"] == "completed"
    assert status_events[0]["task_retry_count"] == 2
    assert status_events[0]["task_turn_generation"] == 7


@pytest.mark.asyncio
async def test_manager_publication_cancel_finishes_rollback_under_anyio(
    tmp_path,
    monkeypatch,
):
    from anyio import CancelScope

    async with _wal_session_factory(
        tmp_path / "manager-publication-cancel.db"
    ) as factory:
        task_id, operation_id, success = await _create_manager_operation(
            factory,
            title="Manager publication cancellation",
        )
        scope_holder: dict[str, CancelScope] = {}
        publication_cancelled = False
        rollback_completed = asyncio.Event()
        original_rollback = AsyncSession.rollback

        async def delayed_rollback(self):
            result = await original_rollback(self)
            if publication_cancelled:
                await asyncio.sleep(0)
                rollback_completed.set()
            return result

        async def cancel_publication(_channel, _data):
            nonlocal publication_cancelled
            publication_cancelled = True
            scope_holder["scope"].cancel()
            await asyncio.sleep(0)

        monkeypatch.setattr(AsyncSession, "rollback", delayed_rollback)
        with patch.object(
            main_module.broadcaster,
            "broadcast",
            side_effect=cancel_publication,
        ):
            async with factory() as coordinator:
                with CancelScope() as scope:
                    scope_holder["scope"] = scope
                    with pytest.raises(asyncio.CancelledError):
                        await termination.apply_manager_result(
                            coordinator,
                            operation_id,
                            success,
                        )

        assert rollback_completed.is_set()
        async with factory() as db:
            task = await db.get(Task, task_id)
            receipt = await db.get(
                termination.WorkerTaskTerminationReceipt,
                operation_id,
            )
        assert task.status == "completed"
        assert receipt.status == "awaiting_ack"


@pytest.mark.asyncio
async def test_manager_reconcile_cancel_commits_error_receipt_under_anyio(
    tmp_path,
):
    from anyio import CancelScope

    async with _wal_session_factory(
        tmp_path / "manager-reconcile-cancel.db"
    ) as factory:
        _task_id, operation_id, _success = await _create_manager_operation(
            factory,
            title="Manager reconciliation cancellation",
        )
        scope_holder: dict[str, CancelScope] = {}

        async def cancel_proxy(*_args, **_kwargs):
            scope_holder["scope"].cancel()
            await asyncio.sleep(0)

        async with factory() as coordinator:
            with CancelScope() as scope:
                scope_holder["scope"] = scope
                with pytest.raises(asyncio.CancelledError):
                    await termination.reconcile_manager_receipt(
                        coordinator,
                        operation_id,
                        proxy_request=cancel_proxy,
                    )

        async with factory() as db:
            receipt = await db.get(
                termination.WorkerTaskTerminationReceipt,
                operation_id,
            )
        assert receipt.status == "pending_remote"
        assert receipt.reconcile_count == 1
        assert receipt.last_error == "Manager reconciliation was cancelled"
