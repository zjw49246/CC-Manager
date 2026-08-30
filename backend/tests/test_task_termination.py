"""Regression tests for generation-safe Task termination orchestration."""

import asyncio
from copy import deepcopy
from contextlib import asynccontextmanager
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select

from backend.models.instance import Instance
from backend.models.monitor_session import MonitorSession
from backend.models.task import Task
from backend.models.worker import Worker
from backend.models.worker_task_termination import WorkerTaskTerminationReceipt


@pytest.mark.asyncio
async def test_finish_despite_cancellation_consumes_request_while_settling():
    """Python 3.14 must not spin on successively cancelled shield futures."""

    import backend.services.task_termination as termination

    started = asyncio.Event()
    release = asyncio.Event()
    cancellation_counts: list[int] = []
    protected: asyncio.Task | None = None

    async def finite_cleanup() -> str:
        started.set()
        await release.wait()
        assert protected is not None
        cancellation_counts.append(protected.cancelling())
        return "settled"

    protected = asyncio.create_task(
        termination._finish_despite_cancellation(finite_cleanup())
    )
    await started.wait()
    protected.cancel()
    await asyncio.sleep(0)
    assert not protected.done()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(protected), timeout=1)
    assert cancellation_counts == [0]


@pytest.mark.asyncio
async def test_finish_awaitable_passively_settles_under_anyio_level_cancellation():
    """A cancelled AnyIO scope must not repeatedly spin asyncio shields."""

    from anyio import CancelScope

    import backend.services.cancellation as cancellation

    started = asyncio.Event()
    release = asyncio.Event()
    shield_calls = 0
    real_shield = asyncio.shield

    async def finite_cleanup() -> str:
        started.set()
        await release.wait()
        return "settled"

    async def release_cleanup() -> None:
        await started.wait()
        await asyncio.sleep(0)
        release.set()

    def counting_shield(awaitable):
        nonlocal shield_calls
        shield_calls += 1
        return real_shield(awaitable)

    releaser = asyncio.create_task(release_cleanup())
    try:
        with patch.object(cancellation.asyncio, "shield", counting_shield):
            with CancelScope() as scope:
                scope.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await cancellation.finish_awaitable(finite_cleanup())
        await releaser
    finally:
        if not releaser.done():
            releaser.cancel()
            await asyncio.gather(releaser, return_exceptions=True)

    assert shield_calls <= 2


@pytest.mark.asyncio
async def test_finish_awaitable_checkpoints_already_completed_future():
    """A ready result must not skip cancellation requested at entry."""

    from anyio import CancelScope

    from backend.services.cancellation import finish_awaitable

    completed = asyncio.get_running_loop().create_future()
    completed.set_result("settled")

    with CancelScope() as scope:
        scope.cancel()
        with pytest.raises(asyncio.CancelledError):
            await finish_awaitable(completed)


@pytest.mark.asyncio
async def test_finish_awaitable_preserves_inner_cancellation_result():
    """An operation's own cancellation is not mistaken for caller cancel."""

    from backend.services.cancellation import finish_awaitable

    async def cancelled_operation() -> None:
        raise asyncio.CancelledError("inner operation cancelled")

    with pytest.raises(asyncio.CancelledError, match="inner operation cancelled"):
        await finish_awaitable(cancelled_operation())


@pytest.mark.asyncio
async def test_finish_awaitable_operation_failure_precedes_caller_cancellation():
    """The settled operation outcome remains authoritative after cancellation."""

    from backend.services.cancellation import finish_awaitable

    started = asyncio.Event()
    release = asyncio.Event()

    async def failing_cleanup() -> None:
        started.set()
        await release.wait()
        raise RuntimeError("authoritative cleanup failure")

    wrapper = asyncio.create_task(finish_awaitable(failing_cleanup()))
    await started.wait()
    wrapper.cancel()
    await asyncio.sleep(0)
    release.set()

    with pytest.raises(RuntimeError, match="authoritative cleanup failure"):
        await wrapper


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ("local", "authoritative"))
async def test_internal_termination_yields_to_active_worker_receipt(
    db_factory,
    entrypoint,
):
    """Service callers cannot bypass durable receipt ownership."""

    import backend.main
    import backend.services.task_termination as termination
    from backend.tests.worker_termination_helpers import (
        persist_active_worker_receipt,
    )

    async with db_factory() as db:
        task = Task(
            title=f"receipt-owned {entrypoint} termination",
            description="test",
            status="pending",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    await persist_active_worker_receipt(db_factory, task_id)
    abort = AsyncMock(return_value=0)
    with patch.object(backend.main.dispatcher, "abort_task_queue", abort):
        async with db_factory() as db:
            with pytest.raises(
                termination.TaskGenerationTerminationConflict,
                match="active Worker termination receipt",
            ):
                if entrypoint == "local":
                    await termination.terminate_local_task_generation(
                        task_id,
                        db,
                        reason="ordinary internal cleanup",
                    )
                else:
                    await termination.terminate_authoritative_task_generation(
                        task_id,
                        db,
                        reason="ordinary internal cleanup",
                    )

    abort.assert_not_awaited()
    async with db_factory() as db:
        current = await db.get(Task, task_id)
    assert current is not None
    assert current.status == "pending"


@pytest.mark.asyncio
async def test_termination_cancellation_during_generation_commit_still_reaps_owner(
    db_factory,
):
    """Caller cancellation cannot strand a terminal Task with its old owner."""

    import backend.main
    import backend.services.task_termination as termination

    started_at = datetime.utcnow()
    async with db_factory() as db:
        task = Task(
            title="cancel-safe termination",
            description="test",
            status="executing",
            started_at=started_at,
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="cancel-safe-owner",
            status="running",
            pid=54001,
            current_task_id=task.id,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id = task.id
        instance_id = instance.id

    generation_read_started = asyncio.Event()
    allow_generation_commit = asyncio.Event()
    real_read_completed_at = termination.read_persisted_task_completed_at

    async def pause_before_first_commit(read_task_id, db):
        completed_at = await real_read_completed_at(read_task_id, db)
        generation_read_started.set()
        await allow_generation_commit.wait()
        return completed_at

    async def stop_exact(stopped_instance_id, **kwargs):
        assert stopped_instance_id == instance_id
        assert kwargs["expected_task_id"] == task_id
        assert kwargs["expected_pid"] == 54001
        assert kwargs["expected_started_at"] == started_at
        async with db_factory() as db:
            owner = await db.get(Instance, instance_id)
            owner.status = "idle"
            owner.pid = None
            owner.current_task_id = None
            await db.commit()
        return True

    async with db_factory() as db:
        with (
            patch.object(
                backend.main.dispatcher,
                "abort_task_queue",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch.object(
                backend.main.instance_manager,
                "wait_for_task_launch_barrier",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch.object(
                backend.main.instance_manager,
                "stop",
                new_callable=AsyncMock,
                side_effect=stop_exact,
            ) as stop,
            patch.object(
                termination,
                "read_persisted_task_completed_at",
                side_effect=pause_before_first_commit,
            ),
            patch(
                "backend.services.task_events.broadcast_status_change",
                new_callable=AsyncMock,
            ) as publish,
        ):
            operation = asyncio.create_task(
                termination.terminate_local_task_generation(
                    task_id,
                    db,
                    reason="superseded",
                )
            )
            await generation_read_started.wait()
            operation.cancel()
            await asyncio.sleep(0)
            allow_generation_commit.set()
            with pytest.raises(asyncio.CancelledError):
                await operation

    stop.assert_awaited_once()
    publish.assert_awaited_once_with(
        task_id,
        "completed",
        background_active=False,
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "completed"
        assert task.error_message == "superseded"
        assert instance.status == "idle"
        assert instance.pid is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "manager_stop_result",
    [True, False, "raise_after_commit"],
)
async def test_local_termination_keeps_task_executing_until_exact_stop_reaps(
    db_factory,
    manager_stop_result,
):
    """Terminal state cannot overtake reap or be double-published on False."""

    import backend.main
    import backend.services.task_termination as termination

    started_at = datetime.utcnow()
    generation = "foreground-tail-1"
    async with db_factory() as db:
        task = Task(
            title="stop-first review",
            description="test",
            status="executing",
            started_at=started_at,
            pty_background_generation=generation,
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="stop-first-owner",
            status="running",
            pid=54101,
            current_task_id=task.id,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id = task.id
        instance_id = instance.id

    stop_entered = asyncio.Event()
    allow_reap = asyncio.Event()
    publish = AsyncMock()

    async def stop_exact(stopped_instance_id, **kwargs):
        assert stopped_instance_id == instance_id
        assert kwargs["expected_task_id"] == task_id
        assert kwargs["expected_pid"] == 54101
        assert kwargs["expected_started_at"] == started_at
        assert kwargs["task_status"] == "completed"
        stop_entered.set()
        await allow_reap.wait()
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            owner = await db.get(Instance, instance_id)
            task.status = "completed"
            task.completed_at = datetime.utcnow()
            task.pty_background_generation = None
            owner.status = "idle"
            owner.pid = None
            owner.current_task_id = None
            await db.commit()
        if manager_stop_result == "raise_after_commit":
            # The process/DB transition succeeded but its own WS publication
            # failed. Task termination must recover with one fenced publish.
            raise RuntimeError("post-commit publication failed")
        # This models InstanceManager.stop's single post-reap publication.
        await publish(task_id, "completed", background_active=False)
        return manager_stop_result

    async with db_factory() as db:
        with (
            patch.object(
                backend.main.dispatcher,
                "abort_task_queue",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch.object(
                backend.main.instance_manager,
                "wait_for_task_launch_barrier",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch.object(
                backend.main.instance_manager,
                "stop",
                new_callable=AsyncMock,
                side_effect=stop_exact,
            ),
            patch(
                "backend.services.task_events.broadcast_status_change",
                new=publish,
            ),
        ):
            operation = asyncio.create_task(
                termination.terminate_local_task_generation(
                    task_id,
                    db,
                    reason="superseded",
                )
            )
            await stop_entered.wait()
            async with db_factory() as observer:
                task = await observer.get(Task, task_id)
                owner = await observer.get(Instance, instance_id)
                assert task.status == "executing"
                assert task.completed_at is None
                assert task.pty_background_generation == generation
                assert owner.current_task_id == task_id
                assert owner.pid == 54101
            publish.assert_not_awaited()
            allow_reap.set()
            result = await operation

    assert result.terminal_status == "completed"
    publish.assert_awaited_once_with(
        task_id,
        "completed",
        background_active=False,
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "completed"
        assert task.error_message == "superseded"
        assert task.pty_background_generation is None


@pytest.mark.asyncio
async def test_local_termination_gate_blocks_pending_reclaim_during_cleanup(
    db_factory,
):
    """A committed non-terminal gate prevents dequeue in the stop window."""

    import backend.main
    import backend.services.task_termination as termination
    from backend.services.task_queue import TaskQueue

    async with db_factory() as db:
        task = Task(
            title="pending supersede gate",
            description="test",
            status="pending",
        )
        db.add(task)
        await db.flush()
        monitor = MonitorSession(
            task_id=task.id,
            agent_type="monitor",
            source="ccm",
            description="hold cleanup open",
            status="running",
        )
        db.add(monitor)
        await db.commit()
        task_id = task.id
        monitor_id = monitor.id

    cleanup_entered = asyncio.Event()
    allow_cleanup = asyncio.Event()

    async def delayed_monitor_stop(session_id):
        assert session_id == monitor_id
        cleanup_entered.set()
        await allow_cleanup.wait()

    async with db_factory() as db:
        with (
            patch.object(
                backend.main.dispatcher,
                "abort_task_queue",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch.object(
                backend.main.dispatcher,
                "stop_monitor_session_process",
                new_callable=AsyncMock,
                side_effect=delayed_monitor_stop,
            ),
            patch(
                "backend.services.task_events.broadcast_status_change",
                new_callable=AsyncMock,
            ),
        ):
            operation = asyncio.create_task(
                termination.terminate_local_task_generation(
                    task_id,
                    db,
                    reason="superseded",
                )
            )
            await cleanup_entered.wait()
            async with db_factory() as observer:
                task = await observer.get(Task, task_id)
                assert task.status == "pending"
                assert task.completed_at is None
                assert (task.metadata_ or {}).get("pr_review_superseded") is True
                assert await TaskQueue(observer).dequeue() is None
                await observer.rollback()
            allow_cleanup.set()
            result = await operation

    assert result.terminal_status == "completed"
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        monitor = await db.get(MonitorSession, monitor_id)
        assert task.status == "completed"
        assert monitor.status == "cancelled"


@pytest.mark.asyncio
async def test_supersede_gate_rejects_late_auxiliary_api_admission(
    client,
    session_factory,
):
    """The committed gate rejects children after the first aux snapshot."""

    import backend.main
    import backend.services.task_termination as termination

    created = await client.post(
        "/api/tasks",
        json={
            "title": "auxiliary admission gate",
            "description": "test",
            "provider": "claude",
            "enabled_skills": {
                "monitor": True,
                "sub-agent": True,
            },
        },
    )
    assert created.status_code == 201, created.text
    task_id = created.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "executing"
        monitor = MonitorSession(
            task_id=task_id,
            agent_type="monitor",
            source="ccm",
            description="hold first snapshot open",
            status="running",
        )
        db.add(monitor)
        await db.commit()
        monitor_id = monitor.id

    first_stop_entered = asyncio.Event()
    allow_first_stop = asyncio.Event()

    async def delayed_first_stop(session_id):
        assert session_id == monitor_id
        first_stop_entered.set()
        await allow_first_stop.wait()

    start_monitor = MagicMock()
    start_sub_agent = MagicMock()
    async with session_factory() as db:
        with (
            patch.object(
                backend.main.dispatcher,
                "abort_task_queue",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch.object(
                backend.main.dispatcher,
                "stop_monitor_session_process",
                new_callable=AsyncMock,
                side_effect=delayed_first_stop,
            ),
            patch.object(
                backend.main.dispatcher,
                "start_monitor_session",
                start_monitor,
            ),
            patch.object(
                backend.main.dispatcher,
                "start_sub_agent_session",
                start_sub_agent,
            ),
            patch(
                "backend.services.task_events.broadcast_status_change",
                new_callable=AsyncMock,
            ),
        ):
            operation = asyncio.create_task(
                termination.terminate_local_task_generation(
                    task_id,
                    db,
                    reason="superseded",
                )
            )
            await first_stop_entered.wait()
            try:
                async with session_factory() as observer:
                    task = await observer.get(Task, task_id)
                    assert task.status == "executing"
                    assert (task.metadata_ or {}).get("pr_review_superseded") is True

                monitor_response = await client.post(
                    f"/api/tasks/{task_id}/monitor-sessions",
                    json={"description": "too late"},
                )
                sub_agent_response = await client.post(
                    f"/api/tasks/{task_id}/sub-agent-sessions",
                    json={"name": "too late", "prompt": "work"},
                )
            finally:
                allow_first_stop.set()
            result = await operation

    assert monitor_response.status_code == 400, monitor_response.text
    assert sub_agent_response.status_code == 400, sub_agent_response.text
    start_monitor.assert_not_called()
    start_sub_agent.assert_not_called()
    assert result.terminal_status == "completed"
    async with session_factory() as db:
        descriptions = list(
            (
                await db.execute(
                    select(MonitorSession.description).where(
                        MonitorSession.task_id == task_id
                    )
                )
            ).scalars()
        )
    assert descriptions == ["hold first snapshot open"]


@pytest.mark.asyncio
async def test_local_termination_second_sweep_reaps_late_auxiliary(
    db_factory,
):
    """A late DB-terminal row with runtime evidence is really stopped."""

    import backend.main
    import backend.services.task_termination as termination

    async with db_factory() as db:
        task = Task(
            title="late auxiliary sweep",
            description="test",
            status="executing",
        )
        db.add(task)
        await db.flush()
        initial = MonitorSession(
            task_id=task.id,
            agent_type="monitor",
            source="ccm",
            description="initial monitor",
            status="running",
        )
        db.add(initial)
        await db.commit()
        task_id = task.id
        initial_id = initial.id

    stopped: list[tuple[str, int]] = []
    late_session_id: int | None = None

    async def stop_initial_and_land_late(session_id):
        nonlocal late_session_id
        assert session_id == initial_id
        stopped.append(("monitor", session_id))
        async with db_factory() as late_db:
            late = MonitorSession(
                task_id=task_id,
                agent_type="sub_agent",
                source="ccm",
                description="bypassed late child",
                status="completed",
            )
            late_db.add(late)
            await late_db.commit()
            late_session_id = late.id
            backend.main.dispatcher._sub_agent_processes[late.id] = object()

    async def stop_late(session_id):
        assert session_id == late_session_id
        stopped.append(("sub_agent", session_id))
        backend.main.dispatcher._sub_agent_processes.pop(session_id, None)

    try:
        async with db_factory() as db:
            with (
                patch.object(
                    backend.main.dispatcher,
                    "abort_task_queue",
                    new_callable=AsyncMock,
                    return_value=0,
                ),
                patch.object(
                    backend.main.dispatcher,
                    "stop_monitor_session_process",
                    new_callable=AsyncMock,
                    side_effect=stop_initial_and_land_late,
                ),
                patch.object(
                    backend.main.dispatcher,
                    "stop_sub_agent_session_process",
                    new_callable=AsyncMock,
                    side_effect=stop_late,
                ),
                patch(
                    "backend.services.task_events.broadcast_status_change",
                    new_callable=AsyncMock,
                ),
            ):
                result = await termination.terminate_local_task_generation(
                    task_id,
                    db,
                    reason="superseded",
                )
    finally:
        if late_session_id is not None:
            backend.main.dispatcher._sub_agent_processes.pop(
                late_session_id,
                None,
            )

    assert late_session_id is not None
    assert stopped == [
        ("monitor", initial_id),
        ("sub_agent", late_session_id),
    ]
    assert result.terminal_status == "completed"
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        sessions = list(
            (
                await db.execute(
                    select(MonitorSession)
                    .where(MonitorSession.task_id == task_id)
                    .order_by(MonitorSession.id)
                )
            ).scalars()
        )
    assert task.status == "completed"
    assert [session.status for session in sessions] == [
        "cancelled",
        "completed",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("agent_type", "session_status"),
    (
        ("monitor", "completed"),
        ("sub_agent", "completed"),
        ("sub_agent", "failed"),
        ("sub_agent", "stopped"),
    ),
)
async def test_local_termination_reaps_db_terminal_auxiliary_with_runtime(
    db_factory,
    agent_type,
    session_status,
):
    """A DB terminal status cannot hide an exact live auxiliary generation."""

    import backend.main
    import backend.services.task_termination as termination

    async with db_factory() as db:
        task = Task(
            title=f"terminal {agent_type} runtime",
            description="test",
            status="executing",
        )
        db.add(task)
        await db.flush()
        session = MonitorSession(
            task_id=task.id,
            agent_type=agent_type,
            source="ccm",
            description=f"{session_status} but live",
            status=session_status,
        )
        db.add(session)
        await db.commit()
        task_id = task.id
        session_id = session.id

    runtime_map = (
        backend.main.dispatcher._monitor_processes
        if agent_type == "monitor"
        else backend.main.dispatcher._sub_agent_processes
    )
    runtime_map[session_id] = object()

    async def stop_exact(stopped_session_id):
        assert stopped_session_id == session_id
        runtime_map.pop(session_id, None)

    monitor_stop = AsyncMock(
        side_effect=(
            stop_exact
            if agent_type == "monitor"
            else AssertionError("wrong monitor registry")
        )
    )
    sub_agent_stop = AsyncMock(
        side_effect=(
            stop_exact
            if agent_type == "sub_agent"
            else AssertionError("wrong sub-agent registry")
        )
    )
    try:
        async with db_factory() as db:
            with (
                patch.object(
                    backend.main.dispatcher,
                    "abort_task_queue",
                    new_callable=AsyncMock,
                    return_value=0,
                ),
                patch.object(
                    backend.main.dispatcher,
                    "stop_monitor_session_process",
                    monitor_stop,
                ),
                patch.object(
                    backend.main.dispatcher,
                    "stop_sub_agent_session_process",
                    sub_agent_stop,
                ),
                patch(
                    "backend.services.task_events.broadcast_status_change",
                    new_callable=AsyncMock,
                ),
            ):
                result = await termination.terminate_local_task_generation(
                    task_id,
                    db,
                    reason="superseded",
                )
    finally:
        runtime_map.pop(session_id, None)

    expected_stop = monitor_stop if agent_type == "monitor" else sub_agent_stop
    unexpected_stop = sub_agent_stop if agent_type == "monitor" else monitor_stop
    expected_stop.assert_awaited_once_with(session_id)
    unexpected_stop.assert_not_awaited()
    assert result.terminal_status == "completed"
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        session = await db.get(MonitorSession, session_id)
    assert task.status == "completed"
    assert session.status == session_status


@pytest.mark.asyncio
async def test_local_termination_cleans_codex_monitor_after_terminal_commit(
    db_factory,
):
    """Native thread deletion starts only after parent/child terminalization."""

    import backend.main
    import backend.services.task_termination as termination

    async with db_factory() as db:
        task = Task(
            title="codex monitor terminal cleanup",
            description="test",
            status="executing",
            provider="codex",
        )
        db.add(task)
        await db.flush()
        monitor = MonitorSession(
            task_id=task.id,
            agent_type="monitor",
            source="ccm",
            description="live codex monitor",
            provider="codex",
            status="running",
            codex_thread_id="monitor-thread-terminal-order",
            codex_home="/tmp/codex-monitor-terminal-order",
        )
        db.add(monitor)
        await db.commit()
        task_id = task.id
        monitor_id = monitor.id

    runtime_map = backend.main.dispatcher._monitor_turn_handles
    marker = object()
    runtime_map[monitor_id] = marker

    async def stop_exact(session_id):
        assert session_id == monitor_id
        runtime_map.pop(monitor_id, None)

    async def cleanup_after_commit(session_id):
        assert session_id == monitor_id
        async with db_factory() as db:
            persisted_task = await db.get(Task, task_id)
            persisted_monitor = await db.get(MonitorSession, monitor_id)
            assert persisted_task.status == "completed"
            assert persisted_monitor.status == "cancelled"
        return True

    cleanup = AsyncMock(side_effect=cleanup_after_commit)
    try:
        async with db_factory() as db:
            with (
                patch.object(
                    backend.main.dispatcher,
                    "abort_task_queue",
                    new_callable=AsyncMock,
                    return_value=0,
                ),
                patch.object(
                    backend.main.dispatcher,
                    "stop_monitor_session_process",
                    new_callable=AsyncMock,
                    side_effect=stop_exact,
                ),
                patch.object(
                    backend.main.dispatcher,
                    "_cleanup_codex_monitor_thread",
                    cleanup,
                ),
                patch(
                    "backend.services.task_events.broadcast_status_change",
                    new_callable=AsyncMock,
                ),
            ):
                result = await termination.terminate_local_task_generation(
                    task_id,
                    db,
                    reason="superseded",
                )
    finally:
        if runtime_map.get(monitor_id) is marker:
            runtime_map.pop(monitor_id, None)

    assert result.terminal_status == "completed"
    cleanup.assert_awaited_once_with(monitor_id)


@pytest.mark.asyncio
async def test_local_termination_skips_historical_terminal_auxiliary_rows(
    db_factory,
):
    """Terminal rows absent from every exact registry are already reaped."""

    import backend.main
    import backend.services.task_termination as termination

    async with db_factory() as db:
        task = Task(
            title="historical auxiliary rows",
            description="test",
            status="executing",
        )
        db.add(task)
        await db.flush()
        sessions = [
            MonitorSession(
                task_id=task.id,
                agent_type=agent_type,
                source="ccm",
                description=f"historical {agent_type} {status}",
                status=status,
            )
            for agent_type, status in (
                ("monitor", "completed"),
                ("sub_agent", "completed"),
                ("sub_agent", "failed"),
                ("sub_agent", "stopped"),
            )
        ]
        db.add_all(sessions)
        await db.commit()
        task_id = task.id
        session_ids = [session.id for session in sessions]

    monitor_ids, sub_agent_ids = backend.main.dispatcher._active_auxiliary_session_ids()
    assert not (set(session_ids) & (monitor_ids | sub_agent_ids))
    monitor_stop = AsyncMock(
        side_effect=AssertionError("historical monitor was stopped")
    )
    sub_agent_stop = AsyncMock(
        side_effect=AssertionError("historical sub-agent was stopped")
    )
    async with db_factory() as db:
        with (
            patch.object(
                backend.main.dispatcher,
                "abort_task_queue",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch.object(
                backend.main.dispatcher,
                "stop_monitor_session_process",
                monitor_stop,
            ),
            patch.object(
                backend.main.dispatcher,
                "stop_sub_agent_session_process",
                sub_agent_stop,
            ),
            patch(
                "backend.services.task_events.broadcast_status_change",
                new_callable=AsyncMock,
            ),
        ):
            result = await termination.terminate_local_task_generation(
                task_id,
                db,
                reason="superseded",
            )

    monitor_stop.assert_not_awaited()
    sub_agent_stop.assert_not_awaited()
    assert result.terminal_status == "completed"


@pytest.mark.asyncio
async def test_local_termination_fails_closed_on_retained_terminal_runtime(
    db_factory,
):
    """A stop return is insufficient while exact registry evidence remains."""

    import backend.main
    import backend.services.task_termination as termination

    async with db_factory() as db:
        task = Task(
            title="retained completed monitor",
            description="test",
            status="executing",
        )
        db.add(task)
        await db.flush()
        monitor = MonitorSession(
            task_id=task.id,
            agent_type="monitor",
            source="ccm",
            description="completed but retained",
            status="completed",
        )
        db.add(monitor)
        await db.commit()
        task_id = task.id
        monitor_id = monitor.id

    backend.main.dispatcher._monitor_processes[monitor_id] = object()
    try:
        async with db_factory() as db:
            with (
                patch.object(
                    backend.main.dispatcher,
                    "abort_task_queue",
                    new_callable=AsyncMock,
                    return_value=0,
                ),
                patch.object(
                    backend.main.dispatcher,
                    "stop_monitor_session_process",
                    new_callable=AsyncMock,
                    return_value=None,
                ) as stop,
            ):
                with pytest.raises(termination.TaskAuxiliaryTerminationConflict):
                    await termination.terminate_local_task_generation(
                        task_id,
                        db,
                        reason="superseded",
                    )
    finally:
        backend.main.dispatcher._monitor_processes.pop(monitor_id, None)

    stop.assert_awaited_once_with(monitor_id)
    async with db_factory() as db:
        task = await db.get(Task, task_id)
    assert task.status == "executing"
    assert (task.metadata_ or {}).get("pr_review_superseded") is True


@pytest.mark.asyncio
async def test_local_termination_stop_failure_preserves_active_generation(
    db_factory,
):
    """An unconfirmed reap preserves every piece of recovery evidence."""

    import backend.main
    import backend.services.task_termination as termination

    started_at = datetime.utcnow()
    generation = "unreaped-tail"
    async with db_factory() as db:
        task = Task(
            title="unreaped review",
            description="test",
            status="executing",
            started_at=started_at,
            pty_background_generation=generation,
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="unreaped-owner",
            status="running",
            pid=54201,
            current_task_id=task.id,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id = task.id
        instance_id = instance.id

    async with db_factory() as db:
        with (
            patch.object(
                backend.main.dispatcher,
                "abort_task_queue",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch.object(
                backend.main.instance_manager,
                "wait_for_task_launch_barrier",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch.object(
                backend.main.instance_manager,
                "stop",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(
                "backend.services.task_events.broadcast_status_change",
                new_callable=AsyncMock,
            ) as publish,
        ):
            with pytest.raises(termination.TaskProcessTerminationConflict):
                await termination.terminate_local_task_generation(
                    task_id,
                    db,
                    reason="superseded",
                )

    publish.assert_not_awaited()
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        owner = await db.get(Instance, instance_id)
        assert task.status == "executing"
        assert task.completed_at is None
        assert task.error_message is None
        assert task.pty_background_generation == generation
        assert (task.metadata_ or {}).get("pr_review_superseded") is True
        assert owner.status == "running"
        assert owner.current_task_id == task_id
        assert owner.pid == 54201
        assert owner.started_at == started_at


@pytest.mark.asyncio
async def test_local_termination_rejects_new_background_marker_aba(
    db_factory,
):
    """An old stop cannot clear or publish over a newly armed PTY epoch."""

    import backend.main
    import backend.services.task_termination as termination

    started_at = datetime.utcnow()
    old_generation = "background-epoch-old"
    new_generation = "background-epoch-new"
    async with db_factory() as db:
        task = Task(
            title="background ABA",
            description="test",
            status="executing",
            started_at=started_at,
            pty_background_generation=old_generation,
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="background-aba-owner",
            status="running",
            pid=54301,
            current_task_id=task.id,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id = task.id
        instance_id = instance.id

    async def stop_old_generation(_instance_id, **_kwargs):
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            owner = await db.get(Instance, instance_id)
            owner.status = "idle"
            owner.pid = None
            owner.current_task_id = None
            task.pty_background_generation = new_generation
            await db.commit()
        return True

    async with db_factory() as db:
        with (
            patch.object(
                backend.main.dispatcher,
                "abort_task_queue",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch.object(
                backend.main.instance_manager,
                "wait_for_task_launch_barrier",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch.object(
                backend.main.instance_manager,
                "stop",
                new_callable=AsyncMock,
                side_effect=stop_old_generation,
            ),
            patch(
                "backend.services.task_events.broadcast_status_change",
                new_callable=AsyncMock,
            ) as publish,
        ):
            with pytest.raises(
                termination.TaskGenerationTerminationConflict,
                match="newer PTY background generation",
            ):
                await termination.terminate_local_task_generation(
                    task_id,
                    db,
                    reason="superseded",
                )

    publish.assert_not_awaited()
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "executing"
        assert task.completed_at is None
        assert task.pty_background_generation == new_generation
        assert (task.metadata_ or {}).get("pr_review_superseded") is True


@pytest.mark.asyncio
async def test_local_termination_rejects_turn_generation_only_aba(
    db_factory,
):
    """A stopped old owner cannot terminalize a newly admitted logical turn."""

    import backend.main
    import backend.services.task_termination as termination

    started_at = datetime.utcnow()
    async with db_factory() as db:
        task = Task(
            title="turn generation ABA",
            description="test",
            status="executing",
            turn_generation=7,
            started_at=started_at,
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="turn-generation-owner",
            status="running",
            pid=54302,
            current_task_id=task.id,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id = task.id
        instance_id = instance.id

    async def stop_old_turn(_instance_id, **_kwargs):
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            owner = await db.get(Instance, instance_id)
            owner.status = "idle"
            owner.pid = None
            owner.current_task_id = None
            task.turn_generation += 1
            await db.commit()
        return True

    async with db_factory() as db:
        with (
            patch.object(
                backend.main.dispatcher,
                "abort_task_queue",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch.object(
                backend.main.instance_manager,
                "wait_for_task_launch_barrier",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch.object(
                backend.main.instance_manager,
                "stop",
                new_callable=AsyncMock,
                side_effect=stop_old_turn,
            ),
            patch(
                "backend.services.task_events.broadcast_status_change",
                new_callable=AsyncMock,
            ) as publish,
        ):
            with pytest.raises(
                termination.TaskGenerationTerminationConflict,
                match="newer generation",
            ):
                await termination.terminate_local_task_generation(
                    task_id,
                    db,
                    reason="superseded",
                )

    publish.assert_not_awaited()
    async with db_factory() as db:
        current = await db.get(Task, task_id)
    assert current.status == "executing"
    assert current.turn_generation == 8
    assert (current.metadata_ or {}).get("pr_review_superseded") is True


@pytest.mark.asyncio
async def test_local_termination_revalidates_authority_after_queue_abort(
    db_factory,
):
    """A local→Worker migration during abort cannot satisfy the local CAS."""

    import backend.main
    import backend.services.task_termination as termination

    async with db_factory() as db:
        task = Task(
            title="authority migration",
            description="test",
            status="pending",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    async def migrate_while_queue_settles(_task_id, **_kwargs):
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            task.worker_id = 91
            await db.commit()
        return 0

    async with db_factory() as db:
        with (
            patch.object(
                backend.main.dispatcher,
                "abort_task_queue",
                new_callable=AsyncMock,
                side_effect=migrate_while_queue_settles,
            ),
            patch.object(
                backend.main.instance_manager,
                "stop",
                new_callable=AsyncMock,
            ) as stop,
        ):
            with pytest.raises(
                termination.TaskGenerationTerminationConflict,
                match="changed execution authority",
            ):
                await termination.terminate_local_task_generation(
                    task_id,
                    db,
                    reason="superseded",
                )

    stop.assert_not_awaited()
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "pending"
        assert task.worker_id == 91


@pytest.mark.asyncio
async def test_local_termination_reconciles_conflict_as_terminal(db_factory):
    """Conflict is terminal and remains retryable for cleanup reconciliation."""

    import backend.main
    import backend.services.task_termination as termination

    async with db_factory() as db:
        task = Task(
            title="conflicted review",
            description="test",
            status="conflict",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    async with db_factory() as db:
        with (
            patch.object(
                backend.main.dispatcher,
                "abort_task_queue",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch(
                "backend.services.task_events.broadcast_status_change",
                new_callable=AsyncMock,
            ) as publish,
        ):
            result = await termination.terminate_local_task_generation(
                task_id,
                db,
                reason="superseded",
            )

    assert result.previous_status == "conflict"
    assert result.terminal_status == "conflict"
    assert result.transitioned is False
    publish.assert_not_awaited()


_REMOTE_RECEIPT_TIME = "2026-08-07T01:02:03.000000"


def _durable_worker_success_receipt(
    request_payload: dict,
    request_digest: str,
    *,
    result_turn_generation: int | None = None,
) -> dict:
    """Build the strict Worker receipt returned by PUT/readback/ACK."""

    from backend.services.worker_task_termination import canonical_json_digest

    expected = request_payload["expected_remote"]
    turn_generation = (
        expected["turn_generation"]
        if result_turn_generation is None
        else result_turn_generation
    )
    result = {
        "version": 2,
        "operation_id": request_payload["operation_id"],
        "task_id": request_payload["task_id"],
        "operation": request_payload["operation"],
        "request_digest": request_digest,
        "task": {
            "id": request_payload["task_id"],
            "status": "completed",
            "retry_count": expected["retry_count"],
            "turn_generation": turn_generation,
            "instance_id": None,
            "started_at": None,
            "completed_at": _REMOTE_RECEIPT_TIME,
            "session_id": None,
            "error_message": None,
            "background_active": False,
        },
        "response": {"ok": True},
    }
    return {
        "version": 2,
        "operation_id": request_payload["operation_id"],
        "task_id": request_payload["task_id"],
        "side": "worker",
        "worker_id": None,
        "operation": request_payload["operation"],
        "status": "succeeded",
        "state_version": 3,
        "source": {
            "incarnation_id": "1" * 32,
            "status": expected["status"],
            "retry_count": expected["retry_count"],
            "turn_generation": expected["turn_generation"],
            "source_log_id": None,
            "instance_id": None,
            "started_at": None,
            "completed_at": None,
            "session_id": None,
            "pty_background_generation": None,
        },
        "request_payload": deepcopy(request_payload),
        "request_digest": request_digest,
        "result_payload": result,
        "result_digest": canonical_json_digest(result),
        "attempt_count": 1,
        "reconcile_count": 0,
        "last_error": None,
        "accepted_at": _REMOTE_RECEIPT_TIME,
        "completed_at": _REMOTE_RECEIPT_TIME,
        "ack_intent_at": None,
        "acknowledged_at": None,
        "created_at": _REMOTE_RECEIPT_TIME,
        "updated_at": _REMOTE_RECEIPT_TIME,
    }


async def _manager_termination_receipt(db_factory, task_id: int):
    async with db_factory() as db:
        receipts = list(
            (
                await db.execute(
                    select(WorkerTaskTerminationReceipt).where(
                        WorkerTaskTerminationReceipt.task_id == task_id,
                        WorkerTaskTerminationReceipt.side == "manager",
                    )
                )
            ).scalars()
        )
    assert len(receipts) == 1
    return receipts[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "remote_turn_generation",
    [None, 6],
    ids=["missing", "different"],
)
async def test_worker_termination_rejects_invalid_receipt_generation_identity(
    db_factory,
    remote_turn_generation,
):
    import backend.main
    import backend.services.task_termination as termination

    async with db_factory() as db:
        worker = Worker(
            name="termination-worker",
            status="ready",
            private_ip="10.0.0.8",
            auth_token="token",
        )
        db.add(worker)
        await db.flush()
        task = Task(
            title="remote termination fence",
            description="test",
            status="executing",
            worker_id=worker.id,
            turn_generation=5,
            tags=["pr-review"],
        )
        db.add(task)
        await db.commit()
        task_id = task.id
    proxy = AsyncMock()

    async def return_invalid_receipt(route, method, path, **kwargs):
        receipt = await _manager_termination_receipt(db_factory, task_id)
        remote = _durable_worker_success_receipt(
            receipt.request_payload,
            receipt.request_digest,
        )
        expected_remote = remote["request_payload"]["expected_remote"]
        if remote_turn_generation is None:
            expected_remote.pop("turn_generation")
        else:
            expected_remote["turn_generation"] = remote_turn_generation
        return remote

    proxy.proxy_to_worker.side_effect = return_invalid_receipt

    with patch.object(backend.main, "worker_proxy", proxy):
        async with db_factory() as db:
            with pytest.raises(termination.WorkerTaskTerminationConflict):
                await termination.terminate_worker_task_generation(
                    task_id,
                    db,
                    operation_locks_held=True,
                )

    proxy.proxy_to_worker.assert_awaited_once()
    request = proxy.proxy_to_worker.await_args
    assert request.args[1] == "GET"
    receipt = await _manager_termination_receipt(db_factory, task_id)
    assert request.args[2].endswith(
        f"/termination-receipts/{receipt.operation_id}"
    )
    assert receipt.operation == "supersede"
    assert receipt.status == "conflict"
    assert receipt.active_task_id == task_id
    async with db_factory() as db:
        current = await db.get(Task, task_id)
    assert current.status == "executing"
    assert current.turn_generation == 5


@pytest.mark.asyncio
@pytest.mark.parametrize("remote", [None, [], "not-an-object"])
async def test_worker_termination_rejects_non_object_receipt(
    db_factory,
    remote,
):
    import backend.main
    import backend.services.task_termination as termination

    async with db_factory() as db:
        worker = Worker(
            name="malformed-termination-worker",
            status="ready",
            private_ip="10.0.0.18",
            auth_token="token",
        )
        db.add(worker)
        await db.flush()
        task = Task(
            title="malformed remote termination receipt",
            description="test",
            status="executing",
            worker_id=worker.id,
            tags=["pr-review"],
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    proxy = AsyncMock()
    proxy.proxy_to_worker.return_value = remote
    with patch.object(backend.main, "worker_proxy", proxy):
        async with db_factory() as db:
            with pytest.raises(
                termination.WorkerTaskTerminationConflict,
                match="invalid termination receipt",
            ):
                await termination.terminate_worker_task_generation(
                    task_id,
                    db,
                    operation_locks_held=True,
                )

    proxy.proxy_to_worker.assert_awaited_once()
    request = proxy.proxy_to_worker.await_args
    assert request.args[1] == "GET"
    receipt = await _manager_termination_receipt(db_factory, task_id)
    assert request.args[2].endswith(
        f"/termination-receipts/{receipt.operation_id}"
    )
    assert receipt.operation == "supersede"
    assert receipt.status == "conflict"
    assert receipt.active_task_id == task_id


@pytest.mark.asyncio
async def test_worker_termination_sends_and_confirms_exact_turn_generation(
    db_factory,
):
    import backend.main
    import backend.services.task_termination as termination
    import backend.services.worker_task_termination as durable_termination

    async with db_factory() as db:
        worker = Worker(
            name="successful-termination-worker",
            status="ready",
            private_ip="10.0.0.9",
            auth_token="token",
        )
        db.add(worker)
        await db.flush()
        task = Task(
            title="remote exact termination",
            description="test",
            status="executing",
            worker_id=worker.id,
            retry_count=2,
            turn_generation=9,
            tags=["pr-review"],
        )
        db.add(task)
        await db.commit()
        task_id = task.id
    proxy = AsyncMock()
    manager_statuses = []
    worker_result = None

    async def reconcile_remote_receipt(route, method, path, **kwargs):
        nonlocal worker_result

        receipt = await _manager_termination_receipt(db_factory, task_id)
        manager_statuses.append((method, receipt.status, receipt.ack_intent_at))
        if method == "GET":
            return durable_termination.receipt_not_found_payload(
                task_id,
                receipt.operation_id,
            )
        if method == "PUT":
            body = kwargs["body"]
            worker_result = _durable_worker_success_receipt(
                body["request_payload"],
                body["request_digest"],
            )
            return worker_result
        assert method == "POST"
        acknowledged = deepcopy(worker_result)
        acknowledged["status"] = "acknowledged"
        acknowledged["state_version"] += 1
        acknowledged["acknowledged_at"] = _REMOTE_RECEIPT_TIME
        return acknowledged

    proxy.proxy_to_worker.side_effect = reconcile_remote_receipt

    with (
        patch.object(backend.main, "worker_proxy", proxy),
        patch.object(
            backend.main.broadcaster,
            "broadcast",
            new_callable=AsyncMock,
        ) as publish,
    ):
        async with db_factory() as db:
            terminated = await termination.terminate_worker_task_generation(
                task_id,
                db,
                operation_locks_held=True,
            )

    assert terminated.observed.turn_generation == 9
    assert terminated.resulting.turn_generation == 9
    assert [call.args[1] for call in proxy.proxy_to_worker.await_args_list] == [
        "GET",
        "PUT",
        "POST",
    ]
    receipt = await _manager_termination_receipt(db_factory, task_id)
    receipt_path = f"/termination-receipts/{receipt.operation_id}"
    assert all(
        call.args[2].endswith(
            receipt_path + ("/ack" if call.args[1] == "POST" else "")
        )
        for call in proxy.proxy_to_worker.await_args_list
    )
    put = proxy.proxy_to_worker.await_args_list[1]
    request_payload = put.kwargs["body"]["request_payload"]
    assert put.kwargs["body"]["operation"] == "supersede"
    assert request_payload["operation_id"] == receipt.operation_id
    assert request_payload["expected_remote"] == {
        "status": "executing",
        "retry_count": 2,
        "turn_generation": 9,
    }
    assert manager_statuses[0][1:] == ("pending_remote", None)
    assert manager_statuses[1][1:] == ("pending_remote", None)
    assert manager_statuses[2][1] == "awaiting_ack"
    assert manager_statuses[2][2] is not None
    assert receipt.status == "settled"
    assert receipt.active_task_id is None
    assert receipt.result_payload == worker_result["result_payload"]
    assert receipt.result_digest == worker_result["result_digest"]
    publish.assert_awaited_once_with(
        "tasks",
        {
            "event": "status_change",
            "task_id": task_id,
            "task_retry_count": 2,
            "task_turn_generation": 9,
            "new_status": "completed",
            "background_active": False,
        },
    )
    async with db_factory() as db:
        current = await db.get(Task, task_id)
    assert current.status == "completed"
    assert current.retry_count == 2
    assert current.turn_generation == 9
    assert current.metadata_["pr_review_superseded"] is True


@pytest.mark.asyncio
async def test_worker_termination_rejects_result_from_different_turn(
    db_factory,
):
    import backend.main
    import backend.services.task_termination as termination
    import backend.services.worker_task_termination as durable_termination

    async with db_factory() as db:
        worker = Worker(
            name="stale-result-worker",
            status="ready",
            private_ip="10.0.0.10",
            auth_token="token",
        )
        db.add(worker)
        await db.flush()
        task = Task(
            title="stale remote result",
            description="test",
            status="executing",
            worker_id=worker.id,
            turn_generation=14,
            tags=["pr-review"],
        )
        db.add(task)
        await db.commit()
        task_id = task.id
    proxy = AsyncMock()

    async def return_stale_result(route, method, path, **kwargs):
        receipt = await _manager_termination_receipt(db_factory, task_id)
        if method == "GET":
            return durable_termination.receipt_not_found_payload(
                task_id,
                receipt.operation_id,
            )
        assert method == "PUT"
        body = kwargs["body"]
        return _durable_worker_success_receipt(
            body["request_payload"],
            body["request_digest"],
            result_turn_generation=15,
        )

    proxy.proxy_to_worker.side_effect = return_stale_result

    with patch.object(backend.main, "worker_proxy", proxy):
        async with db_factory() as db:
            with pytest.raises(termination.WorkerTaskTerminationConflict):
                await termination.terminate_worker_task_generation(
                    task_id,
                    db,
                    operation_locks_held=True,
                )

    assert [call.args[1] for call in proxy.proxy_to_worker.await_args_list] == [
        "GET",
        "PUT",
    ]
    receipt = await _manager_termination_receipt(db_factory, task_id)
    put = proxy.proxy_to_worker.await_args_list[1]
    assert put.args[2].endswith(
        f"/termination-receipts/{receipt.operation_id}"
    )
    assert (
        put.kwargs["body"]["request_payload"]["expected_remote"][
            "turn_generation"
        ]
        == 14
    )
    assert receipt.status == "conflict"
    assert receipt.active_task_id == task_id
    async with db_factory() as db:
        current = await db.get(Task, task_id)
    assert current.status == "executing"
    assert current.turn_generation == 14


@pytest.mark.asyncio
async def test_legacy_internal_termination_mutation_is_disabled(
    client,
    session_factory,
):
    """The legacy snapshot stays readable but its mutation is fail-closed."""

    import backend.main

    created = await client.post(
        "/api/tasks",
        json={
            "title": "worker-facing termination",
            "description": "test",
            "tags": ["pr-review"],
        },
    )
    assert created.status_code == 201, created.text
    task_id = created.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.tags == ["pr-review"]
        task.status = "executing"
        await db.commit()

    public_snapshot = await client.get(f"/api/tasks/{task_id}")
    assert public_snapshot.status_code == 200, public_snapshot.text
    assert "pty_background_generation" not in public_snapshot.json()
    termination_snapshot = await client.get(
        f"/api/tasks/{task_id}/terminate-generation"
    )
    assert termination_snapshot.status_code == 200, termination_snapshot.text
    snapshot = termination_snapshot.json()
    assert snapshot["pty_background_generation"] is None

    missing_marker = await client.post(
        f"/api/tasks/{task_id}/terminate-generation",
        json={
            "expected_status": "executing",
            "expected_retry_count": 0,
            "expected_turn_generation": snapshot["turn_generation"],
            "expected_instance_id": None,
            "expected_started_at": None,
            "expected_completed_at": None,
        },
    )
    assert missing_marker.status_code == 422, missing_marker.text

    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
        return_value=0,
    ) as abort:
        response = await client.post(
            f"/api/tasks/{task_id}/terminate-generation",
            json={
                "expected_status": "executing",
                "expected_retry_count": 0,
                "expected_turn_generation": snapshot["turn_generation"],
                "expected_instance_id": None,
                "expected_started_at": None,
                "expected_completed_at": None,
                "expected_pty_background_generation": None,
            },
        )

    assert response.status_code == 409, response.text
    assert "durable termination receipt" in response.json()["detail"]
    abort.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task_id)
    assert current.status == "executing"
    assert (current.metadata_ or {}).get("pr_review_superseded") is not True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("marker_kind", "initial_status"),
    (
        pytest.param("tag", "pending", id="worker-tag-pending"),
        pytest.param("tag", "executing", id="worker-tag-executing"),
        pytest.param("metadata", "pending", id="manager-metadata-pending"),
        pytest.param(
            "metadata",
            "executing",
            id="manager-metadata-executing",
        ),
    ),
)
async def test_legacy_internal_termination_rejects_pr_fix_task_generations(
    client,
    session_factory,
    marker_kind,
    initial_status,
):
    """Review markers cannot opt back into the pre-receipt mutation path."""

    import backend.main

    marker = (
        {"tags": ["pr-review-fix"]}
        if marker_kind == "tag"
        else {"metadata_": {"pr_finding_action_id": 701}}
    )
    async with session_factory() as db:
        task = Task(
            title=f"{marker_kind} {initial_status} PR fix",
            description="test",
            status=initial_status,
            **marker,
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    snapshot_response = await client.get(
        f"/api/tasks/{task_id}/terminate-generation"
    )
    assert snapshot_response.status_code == 200, snapshot_response.text
    snapshot = snapshot_response.json()
    assert snapshot["status"] == initial_status

    lease_events: list[str] = []
    original_lease = backend.main.dispatcher.task_queue_cancellation_lease

    @asynccontextmanager
    async def observed_lease(leased_task_id: int):
        assert leased_task_id == task_id
        async with original_lease(leased_task_id):
            lease_events.append("acquired")
            assert task_id in backend.main.dispatcher._cancel_durable_queue_tasks
            try:
                yield
            finally:
                # The outer fence must remain held until the terminal Task row
                # is committed, not merely until queue draining completes.
                assert task_id in backend.main.dispatcher._cancel_durable_queue_tasks
                async with session_factory() as verify_db:
                    terminal = await verify_db.get(Task, task_id)
                    assert terminal is not None
                    assert terminal.status == "completed"
                lease_events.append("terminal_committed")
        lease_events.append("released")

    async def abort_inside_lease(aborted_task_id: int, **kwargs):
        assert aborted_task_id == task_id
        assert task_id in backend.main.dispatcher._cancel_durable_queue_tasks
        assert kwargs["cancel_durable"] is False
        assert kwargs["durable_db"] is not None
        return 0

    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
        side_effect=abort_inside_lease,
    ) as abort, patch.object(
        backend.main.dispatcher,
        "task_queue_cancellation_lease",
        observed_lease,
    ):
        response = await client.post(
            f"/api/tasks/{task_id}/terminate-generation",
            json={
                "expected_status": snapshot["status"],
                "expected_retry_count": snapshot["retry_count"],
                "expected_turn_generation": snapshot["turn_generation"],
                "expected_instance_id": snapshot["instance_id"],
                "expected_started_at": snapshot["started_at"],
                "expected_completed_at": snapshot["completed_at"],
                "expected_pty_background_generation": snapshot[
                    "pty_background_generation"
                ],
            },
        )

    assert response.status_code == 409, response.text
    assert "durable termination receipt" in response.json()["detail"]
    abort.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task_id)
    assert current.status == initial_status
    assert (current.metadata_ or {}).get("pr_review_superseded") is not True


@pytest.mark.asyncio
async def test_internal_termination_rejects_plain_tasks_before_cleanup(
    client,
    session_factory,
):
    """The hidden protocol remains unavailable to ordinary Worker Tasks."""

    import backend.main

    async with session_factory() as db:
        task = Task(
            title="ordinary worker task",
            description="test",
            status="pending",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    get_response = await client.get(
        f"/api/tasks/{task_id}/terminate-generation"
    )
    assert get_response.status_code == 400, get_response.text

    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
    ) as abort:
        post_response = await client.post(
            f"/api/tasks/{task_id}/terminate-generation",
            json={
                "expected_status": "pending",
                "expected_retry_count": 0,
                "expected_turn_generation": 0,
                "expected_instance_id": None,
                "expected_started_at": None,
                "expected_completed_at": None,
                "expected_pty_background_generation": None,
            },
        )

    assert post_response.status_code == 400, post_response.text
    abort.assert_not_awaited()


@pytest.mark.asyncio
async def test_hidden_termination_protocol_requires_service_identity(
    session_factory,
):
    """An administrator JWT cannot read or mutate the opaque Worker fence."""

    from fastapi import HTTPException
    from starlette.requests import Request

    from backend.api.tasks import _internal_pr_review_termination_task
    from backend.config import settings

    async with session_factory() as db:
        task = Task(
            title="internal termination auth",
            description="test",
            tags=["pr-review"],
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": f"/api/tasks/{task_id}/terminate-generation",
            "headers": [],
            "query_string": b"",
        }
    )
    request.state.user_id = None
    request.state.user_role = "super_admin"
    request.state.auth_type = "jwt"

    original_auth_token = settings.auth_token
    settings.auth_token = "worker-internal-test-token"
    try:
        async with session_factory() as db:
            with pytest.raises(
                HTTPException,
                match="Internal service authentication required",
            ) as denied:
                await _internal_pr_review_termination_task(
                    task_id,
                    request,
                    db,
                )
            assert denied.value.status_code == 403

        request.state.auth_type = "token"
        async with session_factory() as db:
            authorized = await _internal_pr_review_termination_task(
                task_id,
                request,
                db,
            )
            assert authorized.id == task_id
    finally:
        settings.auth_token = original_auth_token


@pytest.mark.asyncio
async def test_termination_retries_cancelled_ccm_auxiliary_cleanup(db_factory):
    """A failed auxiliary reap remains discoverable on the next supersede."""

    import backend.main
    import backend.services.task_termination as termination

    async with db_factory() as db:
        task = Task(
            title="review with monitor",
            description="test",
            status="executing",
        )
        db.add(task)
        await db.flush()
        monitor = MonitorSession(
            task_id=task.id,
            agent_type="monitor",
            source="ccm",
            description="watch review",
            status="running",
        )
        db.add(monitor)
        await db.commit()
        task_id = task.id
        monitor_id = monitor.id

    stop_attempts = 0

    async def fail_once(session_id):
        nonlocal stop_attempts
        assert session_id == monitor_id
        stop_attempts += 1
        if stop_attempts == 1:
            raise RuntimeError("auxiliary group still alive")

    with (
        patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch.object(
            backend.main.dispatcher,
            "stop_monitor_session_process",
            new_callable=AsyncMock,
            side_effect=fail_once,
        ),
    ):
        async with db_factory() as db:
            with pytest.raises(termination.TaskAuxiliaryTerminationConflict):
                await termination.terminate_local_task_generation(
                    task_id,
                    db,
                    reason="superseded",
                )

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            monitor = await db.get(MonitorSession, monitor_id)
            assert task.status == "executing"
            assert (task.metadata_ or {}).get("pr_review_superseded") is True
            assert monitor.status == "running"

        # The retry sees the still-running durable row, reaps it, and only then
        # publishes the Task/auxiliary terminal states together.
        async with db_factory() as db:
            result = await termination.terminate_local_task_generation(
                task_id,
                db,
                reason="superseded",
            )

    assert result.terminal_status == "completed"
    assert stop_attempts == 2


@pytest.mark.asyncio
async def test_queue_abort_failure_does_not_persist_supersede_gate(db_factory):
    """An unconfirmed queue abort leaves the active review recoverable."""

    import backend.main
    import backend.services.task_termination as termination
    from backend.services.dispatcher import TaskQueueAbortTimeoutError

    async with db_factory() as db:
        task = Task(
            title="abort timeout",
            description="test",
            status="executing",
            metadata_={"pr_review_id": 11},
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    async with db_factory() as db:
        with patch.object(
            backend.main.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            side_effect=TaskQueueAbortTimeoutError("still running"),
        ):
            with pytest.raises(termination.TaskQueueTerminationConflict):
                await termination.terminate_local_task_generation(
                    task_id,
                    db,
                    reason="superseded",
                )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "executing"
        assert task.metadata_ == {"pr_review_id": 11}


@pytest.mark.asyncio
async def test_hidden_termination_rejects_stale_remote_generation_before_abort(
    client,
    session_factory,
):
    """GET→POST cannot terminate a Worker retry that won in the gap."""

    import backend.main

    created = await client.post(
        "/api/tasks",
        json={
            "title": "remote retry race",
            "description": "test",
            "tags": ["pr-review"],
        },
    )
    assert created.status_code == 201, created.text
    task_id = created.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "pending"
        task.retry_count = 1
        await db.commit()

    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
    ) as abort:
        response = await client.post(
            f"/api/tasks/{task_id}/terminate-generation",
            json={
                "expected_status": "executing",
                "expected_retry_count": 0,
                "expected_turn_generation": 0,
                "expected_instance_id": None,
                "expected_started_at": None,
                "expected_completed_at": None,
                "expected_pty_background_generation": None,
            },
        )

    assert response.status_code == 409, response.text
    abort.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "pending"
        assert task.retry_count == 1
        assert (task.metadata_ or {}).get("pr_review_superseded") is not True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("snapshotted_generation", "new_generation"),
    (
        (None, "worker-tail-after-null-snapshot"),
        ("worker-tail-a", "worker-tail-b"),
    ),
)
async def test_hidden_termination_rejects_background_generation_aba(
    client,
    session_factory,
    snapshotted_generation,
    new_generation,
):
    """The Worker compares the POST body token, never arrival-time state."""

    import backend.main

    created = await client.post(
        "/api/tasks",
        json={
            "title": "remote background ABA",
            "description": "test",
            "tags": ["pr-review"],
        },
    )
    assert created.status_code == 201, created.text
    task_id = created.json()["id"]
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "executing"
        task.pty_background_generation = snapshotted_generation
        await db.commit()

    public_snapshot = await client.get(f"/api/tasks/{task_id}")
    assert public_snapshot.status_code == 200, public_snapshot.text
    assert "pty_background_generation" not in public_snapshot.json()
    snapshot_response = await client.get(f"/api/tasks/{task_id}/terminate-generation")
    assert snapshot_response.status_code == 200, snapshot_response.text
    snapshot = snapshot_response.json()
    assert snapshot["pty_background_generation"] == snapshotted_generation

    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.pty_background_generation = new_generation
        await db.commit()

    with patch.object(
        backend.main.dispatcher,
        "abort_task_queue",
        new_callable=AsyncMock,
    ) as abort:
        response = await client.post(
            f"/api/tasks/{task_id}/terminate-generation",
            json={
                "expected_status": snapshot["status"],
                "expected_retry_count": snapshot["retry_count"],
                "expected_turn_generation": snapshot["turn_generation"],
                "expected_instance_id": snapshot["instance_id"],
                "expected_started_at": snapshot["started_at"],
                "expected_completed_at": snapshot["completed_at"],
                "expected_pty_background_generation": (
                    snapshot["pty_background_generation"]
                ),
            },
        )

    assert response.status_code == 409, response.text
    abort.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "executing"
        assert task.pty_background_generation == new_generation
        assert (task.metadata_ or {}).get("pr_review_superseded") is not True


@pytest.mark.asyncio
async def test_superseded_marker_blocks_retry_defer_and_dequeue(db_factory):
    """Every local route back to runnable pending state honors the gate."""

    from backend.services.task_queue import TaskQueue

    async with db_factory() as db:
        retried = Task(
            title="blocked retry",
            description="test",
            status="completed",
            metadata_={"pr_review_superseded": True},
        )
        deferred = Task(
            title="blocked defer",
            description="test",
            status="executing",
            metadata_={"pr_review_superseded": True},
        )
        claimed = Task(
            title="blocked claim",
            description="test",
            status="pending",
            metadata_={"pr_review_superseded": True},
        )
        db.add_all([retried, deferred, claimed])
        await db.commit()
        retry_id = retried.id
        defer_id = deferred.id

        queue = TaskQueue(db)
        assert await queue.retry(retry_id) is None
        assert await queue.defer(defer_id, "backpressure") is False
        assert await queue.dequeue() is None

    async with db_factory() as db:
        retried = await db.get(Task, retry_id)
        deferred = await db.get(Task, defer_id)
        assert retried.status == "completed"
        assert retried.retry_count == 0
        assert deferred.status == "executing"


@pytest.mark.asyncio
async def test_superseded_manager_mirror_cannot_be_migrated(db_factory):
    """A Worker marker mirrored to Manager cannot be copied into an ungated Task."""

    from backend.services.task_migrator import (
        MigrationError,
        TaskMigrator,
        migration_task_generation,
    )

    async with db_factory() as db:
        task = Task(
            title="blocked migration",
            description="test",
            status="completed",
            metadata_={
                "pr_review_id": 17,
                "pr_review_superseded": True,
            },
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    migrator = TaskMigrator(db_factory, AsyncMock())
    with pytest.raises(MigrationError):
        await migrator._claim_migration(
            migration_task_generation(task),
            target_worker_id=None,
            operation_id="f" * 32,
        )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "completed"


@pytest.mark.asyncio
async def test_worker_dispatch_excludes_superseded_pending_mirror(db_factory):
    """Even a malformed pending Manager mirror is never forwarded to a Worker."""

    import backend.main
    from backend.services.dispatcher import GlobalDispatcher

    async with db_factory() as db:
        task = Task(
            title="blocked worker dispatch",
            description="test",
            status="pending",
            worker_id=88,
            metadata_={"pr_review_superseded": True},
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    proxy = MagicMock()
    proxy.forward_task_to_worker = AsyncMock()
    dispatcher = GlobalDispatcher(
        db_factory,
        MagicMock(),
        MagicMock(),
    )
    with patch.object(backend.main, "worker_proxy", proxy):
        await dispatcher._dispatch_worker_tasks()

    proxy.forward_task_to_worker.assert_not_awaited()
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "pending"


@pytest.mark.asyncio
async def test_queued_chat_drops_when_supersede_wins_final_launch_claim(
    db_factory,
):
    """A message admitted after abort cannot launch past the terminal marker."""

    from pathlib import Path
    from sqlalchemy import update

    from backend.services.dispatcher import GlobalDispatcher, QueuedMessage

    async with db_factory() as db:
        task = Task(
            title="queued during supersede",
            description="test",
            status="completed",
            session_id="existing-session",
            metadata_={"pr_review_id": 41},
        )
        instance = Instance(
            name="queued-supersede-slot",
            status="idle",
        )
        db.add_all([task, instance])
        await db.commit()
        task_id = task.id

    instance_manager = MagicMock()
    instance_manager.is_running.return_value = False
    instance_manager.launch = AsyncMock()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    dispatcher = GlobalDispatcher(
        db_factory,
        instance_manager,
        broadcaster,
    )
    reserve_idle_instance = dispatcher._reserve_idle_instance

    async def supersede_during_slot_reservation(db):
        # This is the post-abort/new-enqueue race: all Python prechecks saw the
        # old row, then synchronize commits the marker immediately before the
        # consumer's final atomic Task claim.
        reserved = await reserve_idle_instance(db)
        assert reserved[0] is not None
        assert reserved[1] is not None
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                metadata_={
                    "pr_review_id": 41,
                    "pr_review_superseded": True,
                }
            )
        )
        await db.commit()
        return reserved

    dispatcher._reserve_idle_instance = AsyncMock(
        side_effect=supersede_during_slot_reservation
    )
    dispatcher._resolve_resume_config_dir = AsyncMock(return_value=None)
    message = QueuedMessage(
        priority=0,
        timestamp=0,
        prompt="late queued message",
    )
    with patch(
        "backend.api.tasks._find_session_jsonl",
        return_value=Path("/tmp/existing-session.jsonl"),
    ):
        await dispatcher._process_queued_message(task_id, message)

    instance_manager.launch.assert_not_awaited()
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "completed"
        assert task.instance_id is None
        assert task.metadata_["pr_review_superseded"] is True
