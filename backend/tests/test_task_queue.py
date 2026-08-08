"""Tests for TaskQueue — priority ordering, dequeue, status transitions."""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from backend.config import settings
from backend.database import Base
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.services.task_queue import (
    TaskQueue,
    _effective_key_expr,
    task_delete_fence,
    task_generation_fence,
)


@pytest_asyncio.fixture
async def queue(db_session):
    return TaskQueue(db_session)


@pytest.mark.asyncio
async def test_create_task(queue):
    task = await queue.create(
        title="Test task",
        description="Do something",
        target_repo="/tmp/repo",
    )
    assert task.id is not None
    assert task.title == "Test task"
    assert task.status == "pending"
    assert task.priority == 0
    assert task.provider == settings.default_provider
    assert task.model == (
        settings.default_codex_model
        if settings.default_provider == "codex"
        else settings.default_model
    )
    assert task.effort_level == settings.default_effort
    assert task.codex_service_tier == "default"


@pytest.mark.asyncio
async def test_dequeue_priority_order(queue):
    """P0 should be dequeued before P1 (lower number = higher priority)."""
    await queue.create(title="Low priority", description="d", target_repo="/tmp", priority=10)
    await queue.create(title="High priority", description="d", target_repo="/tmp", priority=0)
    await queue.create(title="Medium priority", description="d", target_repo="/tmp", priority=5)

    first = await queue.dequeue()
    assert first is not None
    assert first.title == "High priority"
    assert first.priority == 0
    assert first.status == "in_progress"

    second = await queue.dequeue()
    assert second is not None
    assert second.title == "Medium priority"
    assert second.priority == 5

    third = await queue.dequeue()
    assert third is not None
    assert third.title == "Low priority"
    assert third.priority == 10


@pytest.mark.asyncio
async def test_dequeue_fifo_within_same_priority(queue):
    """Tasks with the same priority should be dequeued in FIFO order."""
    await queue.create(title="First", description="d", target_repo="/tmp", priority=0)
    await queue.create(title="Second", description="d", target_repo="/tmp", priority=0)

    first = await queue.dequeue()
    assert first.title == "First"
    second = await queue.dequeue()
    assert second.title == "Second"


@pytest.mark.asyncio
async def test_concurrent_dequeue_claims_each_task_once(tmp_path):
    """Independent Ralph/dispatcher sessions cannot claim the same row."""

    db_path = tmp_path / "atomic-dequeue.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False,
        )
        async with factory() as db:
            first = Task(title="first", description="d", priority=0)
            second = Task(title="second", description="d", priority=1)
            db.add_all([first, second])
            await db.commit()
            first_id, second_id = first.id, second.id

        async with factory() as db1, factory() as db2:
            claimed = await asyncio.gather(
                TaskQueue(db1).dequeue(),
                TaskQueue(db2).dequeue(),
            )

        assert {task.id for task in claimed if task is not None} == {
            first_id,
            second_id,
        }
        assert len([task for task in claimed if task is not None]) == 2
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_dequeue_returns_none_when_empty(queue):
    result = await queue.dequeue()
    assert result is None


@pytest.mark.asyncio
async def test_dequeue_skips_temporarily_excluded_task(queue):
    waiting = await queue.create(
        title="waiting-codex", description="d", target_repo="/tmp", priority=0,
    )
    runnable = await queue.create(
        title="runnable", description="d", target_repo="/tmp", priority=1,
    )

    selected = await queue.dequeue(exclude_ids={waiting.id})

    assert selected is not None
    assert selected.id == runnable.id
    assert (await queue.get(waiting.id)).status == "pending"


@pytest.mark.asyncio
async def test_mark_completed(queue):
    task = await queue.create(title="t", description="d", target_repo="/tmp")
    await queue.mark_completed(task.id)
    updated = await queue.get(task.id)
    assert updated.status == "completed"
    assert updated.completed_at is not None


@pytest.mark.asyncio
async def test_mark_failed(queue):
    task = await queue.create(title="t", description="d", target_repo="/tmp")
    await queue.mark_failed(task.id, "something broke")
    updated = await queue.get(task.id)
    assert updated.status == "failed"
    assert updated.error_message == "something broke"


@pytest.mark.asyncio
async def test_mark_status_generic(queue):
    task = await queue.create(title="t", description="d", target_repo="/tmp")
    await queue.mark_status(task.id, "executing")
    updated = await queue.get(task.id)
    assert updated.status == "executing"


@pytest.mark.asyncio
async def test_retry_increments_count(queue):
    task = await queue.create(title="t", description="d", target_repo="/tmp")
    await queue.mark_failed(task.id, "error")
    retried = await queue.retry(task.id)
    assert retried.status == "pending"
    assert retried.retry_count == 1
    assert retried.error_message is None


@pytest.mark.asyncio
async def test_retry_rejects_completed_task_with_live_pty_background(queue):
    task = await queue.create(title="background", description="tail")
    task.status = "completed"
    task.pty_background_generation = "exact-tail"
    await queue.db.commit()
    task_id = task.id

    assert await queue.retry(task_id) is None
    queue.db.expire_all()
    current = await queue.get(task_id)
    assert current.status == "completed"
    assert current.pty_background_generation == "exact-tail"


@pytest.mark.asyncio
async def test_owned_completion_cannot_overwrite_cancelled_task(queue):
    task = await queue.create(title="owned", description="d", target_repo="/tmp")
    task_id = task.id
    claimed = await queue.dequeue(instance_id=7)
    assert claimed is not None
    assert await queue.cancel(task_id) is not None

    changed = await queue.mark_completed(task_id, instance_id=7)

    assert changed is False
    queue.db.expire_all()
    assert (await queue.get(task_id)).status == "cancelled"


@pytest.mark.asyncio
async def test_retry_rejects_active_task_without_expected_generation(queue):
    task = await queue.create(title="active", description="d", target_repo="/tmp")
    task_id = task.id
    claimed = await queue.dequeue(instance_id=3)
    assert claimed is not None

    assert await queue.retry(task_id) is None
    queue.db.expire_all()
    current = await queue.get(task_id)
    assert current.status == "in_progress"
    assert current.instance_id == 3


@pytest.mark.asyncio
async def test_owned_retry_is_cas_and_releases_instance_claim(queue):
    task = await queue.create(title="retry", description="d", target_repo="/tmp")
    claimed = await queue.dequeue(instance_id=4)
    assert claimed is not None

    assert await queue.retry(
        task.id,
        expected_statuses=("in_progress", "executing"),
        instance_id=99,
    ) is None
    retried = await queue.retry(
        task.id,
        expected_statuses=("in_progress", "executing"),
        instance_id=4,
    )

    assert retried is not None
    assert retried.status == "pending"
    assert retried.instance_id is None
    assert retried.retry_count == 1


@pytest.mark.asyncio
async def test_lifecycle_transitions_reject_same_slot_retry_aba(queue):
    """Every Ralph result transition must fence retry_count/start generation."""

    instance = Instance(name="same-slot-lifecycle")
    queue.db.add(instance)
    await queue.db.commit()
    await queue.create(
        title="old lifecycle",
        description="d",
        status="pending",
    )
    old = await queue.dequeue(instance_id=instance.id)
    assert old is not None
    task_id = old.id
    instance_id = instance.id
    old_generation = task_generation_fence(old)

    await queue.mark_status(task_id, "failed")
    assert await queue.retry(task_id) is not None
    replacement = await queue.dequeue(instance_id=instance_id)
    assert replacement is not None
    assert replacement.retry_count == old_generation[0] + 1

    assert not await queue.mark_completed(
        task_id,
        instance_id=instance_id,
        generation_fence=old_generation,
    )
    assert not await queue.mark_failed(
        task_id,
        "late old failure",
        instance_id=instance_id,
        generation_fence=old_generation,
    )
    assert not await queue.defer(
        task_id,
        "late old defer",
        instance_id=instance_id,
        generation_fence=old_generation,
    )
    assert (
        await queue.retry(
            task_id,
            expected_statuses=("in_progress", "executing"),
            instance_id=instance_id,
            generation_fence=old_generation,
        )
        is None
    )

    queue.db.expire_all()
    current = await queue.db.get(Task, task_id)
    assert current.status == "in_progress"
    assert current.retry_count == 1
    assert current.instance_id == instance_id


@pytest.mark.asyncio
async def test_completion_rejects_background_marker_armed_after_fence(queue):
    instance = Instance(name="background-fence-worker")
    queue.db.add(instance)
    await queue.db.commit()
    await queue.create(
        title="foreground generation",
        description="native child tail",
        status="pending",
    )
    claimed = await queue.dequeue(instance_id=instance.id)
    assert claimed is not None
    task_id = claimed.id
    generation = task_generation_fence(claimed)

    claimed.pty_background_generation = "late-native-tail"
    await queue.db.commit()

    assert not await queue.mark_completed(
        task_id,
        instance_id=instance.id,
        generation_fence=generation,
    )
    queue.db.expire_all()
    current = await queue.get(task_id)
    assert current.status == "in_progress"
    assert current.pty_background_generation == "late-native-tail"


@pytest.mark.asyncio
async def test_defer_returns_active_task_without_consuming_retry_budget(queue):
    task = await queue.create(title="t", description="d", target_repo="/tmp")
    task_id = task.id
    claimed = await queue.dequeue()
    assert claimed.id == task_id
    claimed.instance_id = 99
    await queue.db.commit()

    assert await queue.defer(task_id, "all Codex accounts cooling down") is True

    queue.db.expire_all()
    deferred = await queue.get(task_id)
    assert deferred.status == "pending"
    assert deferred.retry_count == 0
    assert deferred.instance_id is None
    assert deferred.started_at is None
    assert deferred.completed_at is None
    assert deferred.error_message == "all Codex accounts cooling down"


@pytest.mark.asyncio
async def test_defer_does_not_resurrect_cancelled_task(queue):
    task = await queue.create(title="t", description="d", target_repo="/tmp")
    task_id = task.id
    await queue.dequeue()
    await queue.cancel(task_id)

    assert await queue.defer(task_id, "temporary routing failure") is False

    queue.db.expire_all()
    assert (await queue.get(task_id)).status == "cancelled"


@pytest.mark.asyncio
async def test_cancel_task(queue):
    task = await queue.create(title="t", description="d", target_repo="/tmp")
    cancelled = await queue.cancel(task.id)
    assert cancelled.status == "cancelled"


@pytest.mark.asyncio
async def test_cancel_executing_task(queue):
    """Should be able to cancel tasks in executing/merging states."""
    task = await queue.create(title="t", description="d", target_repo="/tmp")
    await queue.mark_status(task.id, "executing")
    cancelled = await queue.cancel(task.id)
    assert cancelled.status == "cancelled"


@pytest.mark.asyncio
async def test_delete_conflict_task(queue):
    """Should be able to delete tasks in conflict state."""
    task = await queue.create(title="t", description="d", target_repo="/tmp")
    await queue.mark_status(task.id, "conflict")
    with patch(
        "backend.services.internal_service_auth.revoke_internal_service_owner"
    ) as revoke:
        result = await queue.delete(task.id)
    assert result is True
    revoke.assert_called_once_with("task-turn", task.id)


@pytest.mark.asyncio
async def test_delete_rejects_completed_task_with_live_pty_background(queue):
    task = await queue.create(title="background", description="tail")
    task.status = "completed"
    task.pty_background_generation = "exact-tail"
    await queue.db.commit()

    assert await queue.delete(task.id) is False
    assert await queue.get(task.id) is not None


@pytest.mark.asyncio
async def test_delete_worker_mirror_requires_remote_confirmation(queue):
    task = await queue.create(
        title="remote",
        description="d",
        target_repo="/tmp",
        worker_id=91,
    )
    task_id = task.id

    assert await queue.delete(task_id) is False
    queue.db.expire_all()
    assert await queue.db.get(Task, task_id) is not None


@pytest.mark.asyncio
async def test_remote_worker_delete_accepts_exact_background_mirror(queue):
    task = await queue.create(
        title="remote background",
        description="d",
        target_repo="/tmp",
        worker_id=91,
    )
    task.status = "completed"
    task.pty_background_generation = (
        "worker-relay-background-mirror"
    )
    await queue.db.commit()
    fence = task_delete_fence(task)

    assert await queue.delete(
        task.id,
        expected_fence=fence,
        remote_worker_deleted=True,
    )
    assert await queue.get(task.id) is None


@pytest.mark.asyncio
async def test_remote_worker_delete_rejects_changed_background_mirror(queue):
    task = await queue.create(
        title="remote background changed",
        description="d",
        target_repo="/tmp",
        worker_id=91,
    )
    task.status = "completed"
    task.pty_background_generation = "worker-relay-generation-1"
    await queue.db.commit()
    task_id = task.id
    fence = task_delete_fence(task)

    task.pty_background_generation = "worker-relay-generation-2"
    await queue.db.commit()

    assert not await queue.delete(
        task_id,
        expected_fence=fence,
        remote_worker_deleted=True,
    )
    assert await queue.get(task_id) is not None


@pytest.mark.asyncio
async def test_delete_running_task_rejected(queue):
    """Should NOT be able to delete in_progress tasks."""
    task = await queue.create(title="t", description="d", target_repo="/tmp")
    _ = await queue.dequeue()  # sets to in_progress
    result = await queue.delete(task.id)
    assert result is False


@pytest.mark.asyncio
async def test_delete_task_preserves_possible_live_orphan_owner(queue):
    """A failed task is durable evidence while its persisted PID may live."""
    task = await queue.create(title="orphan", description="d")
    task.status = "failed"
    instance = Instance(
        name="orphan-slot",
        status="error",
        pid=32101,
        current_task_id=task.id,
    )
    queue.db.add(instance)
    await queue.db.flush()
    task.instance_id = instance.id
    await queue.db.commit()
    task_id, instance_id = task.id, instance.id

    with patch("backend.services.task_queue.os.kill", return_value=None):
        assert await queue.delete(task_id) is False

    queue.db.expire_all()
    task = await queue.db.get(Task, task_id)
    instance = await queue.db.get(Instance, instance_id)
    assert task.status == "failed"
    assert task.instance_id == instance_id
    assert instance.pid == 32101
    assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_delete_task_preserves_reaped_parent_with_live_generation(queue):
    """Manager process-group/consumer evidence outranks a parent returncode."""

    import backend.main

    task = await queue.create(title="live descendants", description="d")
    task.status = "failed"
    instance = Instance(
        name="live-descendant-slot",
        status="error",
        pid=None,
        current_task_id=task.id,
    )
    queue.db.add(instance)
    await queue.db.flush()
    task.instance_id = instance.id
    await queue.db.commit()
    task_id = task.id
    instance_id = instance.id

    manager = MagicMock()
    manager.is_running.return_value = True
    manager.processes = {instance_id: MagicMock(returncode=0)}
    with patch.object(backend.main, "instance_manager", manager):
        assert await queue.delete(task_id) is False

    manager.is_running.assert_called_with(instance_id)
    queue.db.expire_all()
    task = await queue.db.get(Task, task_id)
    instance = await queue.db.get(Instance, instance_id)
    assert task is not None
    assert task.instance_id == instance_id
    assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_delete_task_rejects_live_dispatcher_lifecycle_after_parent_exit(
    queue,
):
    """Merge/evaluator cleanup remains active after the model parent exits."""

    import backend.main

    task = await queue.create(title="live dispatcher lifecycle", description="d")
    task.status = "completed"
    instance = Instance(
        name="dispatcher-lifecycle-slot",
        status="error",
        pid=None,
        current_task_id=task.id,
    )
    queue.db.add(instance)
    await queue.db.flush()
    task.instance_id = instance.id
    await queue.db.commit()
    task_id = task.id
    instance_id = instance.id

    release = asyncio.Event()
    lifecycle = asyncio.create_task(release.wait())
    try:
        with (
            patch.object(
                backend.main.dispatcher,
                "_running_tasks",
                {instance_id: lifecycle},
            ),
            patch.object(
                backend.main.instance_manager,
                "is_running",
                return_value=False,
            ),
        ):
            assert await queue.delete(task_id) is False
    finally:
        release.set()
        await lifecycle

    queue.db.expire_all()
    assert await queue.db.get(Task, task_id) is not None
    instance = await queue.db.get(Instance, instance_id)
    assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_delete_task_rejects_unreaped_goal_evaluator(queue):
    """A retained evaluator process remains part of the Task lifecycle."""

    task = await queue.create(title="live evaluator", description="d")
    task.status = "failed"
    await queue.db.commit()
    task_id = task.id

    with patch(
        "backend.services.goal_evaluator."
        "has_unreaped_goal_evaluator_for_task",
        return_value=True,
    ):
        assert await queue.delete(task_id) is False

    queue.db.expire_all()
    assert await queue.db.get(Task, task_id) is not None


@pytest.mark.asyncio
async def test_delete_task_locks_task_then_instance_then_children(queue):
    """Lifecycle mutations share one DB row-lock order across endpoints."""

    from backend.models.monitor_session import MonitorSession

    task = await queue.create(title="lock order", description="d")
    task.status = "completed"
    instance = Instance(
        name="lock-order-slot",
        status="error",
        current_task_id=task.id,
    )
    queue.db.add(instance)
    await queue.db.flush()
    task.instance_id = instance.id
    monitor = MonitorSession(
        task_id=task.id,
        description="finished child",
        status="completed",
    )
    queue.db.add(monitor)
    await queue.db.commit()

    lock_order: list[str] = []
    original_execute = queue.db.execute

    async def track_locks(statement, *args, **kwargs):
        table = getattr(statement, "table", None)
        if (
            getattr(statement, "is_update", False)
            and getattr(table, "name", None) == "tasks"
            and not lock_order
        ):
            lock_order.append("tasks")
        elif getattr(statement, "_for_update_arg", None) is not None:
            froms = statement.get_final_froms()
            if froms:
                lock_order.append(froms[0].name)
        return await original_execute(statement, *args, **kwargs)

    with patch.object(
        queue.db,
        "execute",
        new=AsyncMock(side_effect=track_locks),
    ):
        assert await queue.delete(task.id) is True

    assert lock_order[0] == "tasks"
    instance_positions = [
        index for index, name in enumerate(lock_order) if name == "instances"
    ]
    child_position = lock_order.index("sub_agent_sessions")
    assert instance_positions
    assert max(instance_positions) < child_position


@pytest.mark.asyncio
async def test_delete_task_rejects_running_auxiliary_session(queue):
    """Completed main turns may still own a live monitor/sub-agent."""

    from backend.models.monitor_session import MonitorSession

    task = await queue.create(title="active monitor", description="d")
    task.status = "completed"
    monitor = MonitorSession(
        task_id=task.id,
        description="keep watching",
        status="running",
    )
    queue.db.add(monitor)
    await queue.db.commit()
    task_id = task.id
    monitor_id = monitor.id

    assert await queue.delete(task_id) is False

    queue.db.expire_all()
    assert await queue.db.get(Task, task_id) is not None
    monitor = await queue.db.get(MonitorSession, monitor_id)
    assert monitor is not None
    assert monitor.status == "running"


@pytest.mark.asyncio
async def test_delete_task_preserves_terminal_codex_monitor_cleanup_owner(
    queue,
):
    """A Task delete cannot erase a retryable native-thread cleanup."""

    from backend.models.monitor_session import MonitorSession

    task = await queue.create(
        title="codex monitor cleanup",
        description="d",
    )
    task.status = "completed"
    monitor = MonitorSession(
        task_id=task.id,
        description="finished but cleanup pending",
        provider="codex",
        status="completed",
        codex_thread_id="monitor-thread-pending-delete",
        codex_home="/tmp/codex-monitor-pending-delete",
        codex_cleanup_pending=True,
        codex_cleanup_error="transport unavailable",
    )
    queue.db.add(monitor)
    await queue.db.commit()
    task_id = task.id
    monitor_id = monitor.id

    assert await queue.delete(task_id) is False

    queue.db.expire_all()
    assert await queue.db.get(Task, task_id) is not None
    monitor = await queue.db.get(MonitorSession, monitor_id)
    assert monitor is not None
    assert monitor.codex_thread_id == "monitor-thread-pending-delete"
    assert monitor.codex_cleanup_pending is True


@pytest.mark.asyncio
async def test_delete_task_preserves_uncommitted_monitor_turn_handle(queue):
    """In-memory pre-commit ownership also fences child-row deletion."""

    from backend.main import dispatcher
    from backend.models.monitor_session import MonitorSession

    task = await queue.create(
        title="monitor pre-commit owner",
        description="d",
    )
    task.status = "completed"
    monitor = MonitorSession(
        task_id=task.id,
        description="pre-commit owner",
        provider="codex",
        status="completed",
    )
    queue.db.add(monitor)
    await queue.db.commit()
    task_id = task.id
    monitor_id = monitor.id
    marker = object()
    dispatcher._monitor_turn_handles[monitor_id] = marker

    try:
        assert await queue.delete(task_id) is False
        queue.db.expire_all()
        assert await queue.db.get(Task, task_id) is not None
        assert await queue.db.get(MonitorSession, monitor_id) is not None
    finally:
        if dispatcher._monitor_turn_handles.get(monitor_id) is marker:
            dispatcher._monitor_turn_handles.pop(monitor_id, None)


@pytest.mark.asyncio
async def test_delete_task_preserves_orphan_when_pid_probe_is_denied(queue):
    """Permission/unknown PID probes fail closed without losing evidence."""
    task = await queue.create(title="orphan-denied", description="d")
    task.status = "failed"
    instance = Instance(
        name="orphan-denied-slot",
        status="error",
        pid=32102,
        current_task_id=task.id,
    )
    queue.db.add(instance)
    await queue.db.flush()
    task.instance_id = instance.id
    await queue.db.commit()
    task_id, instance_id = task.id, instance.id

    with patch(
        "backend.services.task_queue.os.kill",
        side_effect=PermissionError("not permitted"),
    ):
        assert await queue.delete(task_id) is False

    queue.db.expire_all()
    assert await queue.db.get(Task, task_id) is not None
    instance = await queue.db.get(Instance, instance_id)
    assert instance.pid == 32102
    assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_delete_task_detaches_definitively_dead_orphan(queue):
    """ESRCH permits an exact owner detach followed by task deletion."""
    task = await queue.create(title="orphan-dead", description="d")
    task.status = "failed"
    instance = Instance(
        name="orphan-dead-slot",
        status="error",
        pid=32103,
        current_task_id=task.id,
    )
    queue.db.add(instance)
    await queue.db.flush()
    task.instance_id = instance.id
    await queue.db.commit()
    task_id, instance_id = task.id, instance.id

    with patch(
        "backend.services.task_queue.os.kill",
        side_effect=ProcessLookupError,
    ):
        assert await queue.delete(task_id) is True

    queue.db.expire_all()
    assert await queue.db.get(Task, task_id) is None
    instance = await queue.db.get(Instance, instance_id)
    assert instance.status == "error"
    assert instance.pid is None
    assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_delete_task_loses_cas_to_concurrent_retry_without_data_loss(
    db_factory,
):
    """A retry between owner inspection and DELETE must preserve the Task/logs."""

    async with db_factory() as db:
        task = Task(
            title="delete retry race",
            description="work",
            status="completed",
        )
        db.add(task)
        await db.flush()
        log = LogEntry(
            task_id=task.id,
            event_type="message",
            role="assistant",
            content="keep me",
        )
        db.add(log)
        await db.commit()
        task_id = task.id
        log_id = log.id

        queue = TaskQueue(db)
        original_execute = db.execute
        retried = False

        async def execute_with_retry(statement, *args, **kwargs):
            nonlocal retried
            table = getattr(statement, "table", None)
            if (
                not retried
                and getattr(statement, "is_update", False)
                and getattr(table, "name", None) == "tasks"
            ):
                retried = True
                async with db_factory() as other_db:
                    assert await TaskQueue(other_db).retry(task_id) is not None
            return await original_execute(statement, *args, **kwargs)

        with patch.object(
            db,
            "execute",
            new=AsyncMock(side_effect=execute_with_retry),
        ):
            assert await queue.delete(task_id) is False

    assert retried
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        log = await db.get(LogEntry, log_id)
        assert task is not None
        assert task.status == "pending"
        assert log is not None
        assert log.content == "keep me"


@pytest.mark.asyncio
async def test_delete_task_rejects_retry_then_same_terminal_status_aba(
    db_factory,
):
    """Returning to the observed status cannot hide a newer retry generation."""

    async with db_factory() as db:
        task = Task(
            title="delete full ABA",
            description="work",
            status="completed",
        )
        db.add(task)
        await db.flush()
        log = LogEntry(
            task_id=task.id,
            event_type="message",
            role="assistant",
            content="new generation history",
        )
        db.add(log)
        await db.commit()
        task_id = task.id
        log_id = log.id

        queue = TaskQueue(db)
        original_execute = db.execute
        raced = False

        async def execute_with_full_aba(statement, *args, **kwargs):
            nonlocal raced
            table = getattr(statement, "table", None)
            if (
                not raced
                and getattr(statement, "is_update", False)
                and getattr(table, "name", None) == "tasks"
            ):
                raced = True
                async with db_factory() as other_db:
                    other_queue = TaskQueue(other_db)
                    assert await other_queue.retry(task_id) is not None
                    assert await other_queue.mark_completed(
                        task_id,
                        expected_statuses=("pending",),
                    )
            return await original_execute(statement, *args, **kwargs)

        with patch.object(
            db,
            "execute",
            new=AsyncMock(side_effect=execute_with_full_aba),
        ):
            assert await queue.delete(task_id) is False

    assert raced
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        log = await db.get(LogEntry, log_id)
        assert task is not None
        assert task.status == "completed"
        assert task.retry_count == 1
        assert log is not None


@pytest.mark.asyncio
async def test_list_tasks_ordered(queue):
    await queue.create(title="B", description="d", target_repo="/tmp", priority=5)
    await queue.create(title="A", description="d", target_repo="/tmp", priority=1)
    tasks = await queue.list_tasks()
    assert tasks[0].title == "A"
    assert tasks[1].title == "B"


@pytest.mark.asyncio
async def test_list_tasks_filter_status(queue):
    await queue.create(title="pending", description="d", target_repo="/tmp")
    t2 = await queue.create(title="done", description="d", target_repo="/tmp")
    await queue.mark_completed(t2.id)
    pending = await queue.list_tasks(status="pending")
    assert len(pending) == 1
    assert pending[0].title == "pending"


@pytest.mark.parametrize(
    ("dialect", "expected", "forbidden"),
    [
        (sqlite.dialect(), "strftime", "UNIX_TIMESTAMP"),
        (postgresql.dialect(), "EXTRACT(EPOCH FROM", "strftime"),
        (mysql.dialect(), "UNIX_TIMESTAMP", "strftime"),
    ],
)
def test_effective_sort_key_compiles_for_supported_database_dialects(
    dialect,
    expected,
    forbidden,
):
    statement = select(Task.id).order_by(_effective_key_expr())
    sql = str(statement.compile(dialect=dialect))

    assert expected in sql
    assert forbidden not in sql


# === Dequeue picks any pending task ===


@pytest.mark.asyncio
async def test_dequeue_picks_any_model_task(queue):
    """dequeue() picks any pending task regardless of model."""
    await queue.create(title="opus-task", description="d", target_repo="/tmp", model="opus")
    task = await queue.dequeue()
    assert task is not None
    assert task.title == "opus-task"


@pytest.mark.asyncio
async def test_dequeue_picks_any_provider_task(queue):
    """dequeue() picks any pending task regardless of provider."""
    await queue.create(title="codex-task", description="d", target_repo="/tmp", provider="codex")
    task = await queue.dequeue()
    assert task is not None
    assert task.title == "codex-task"
