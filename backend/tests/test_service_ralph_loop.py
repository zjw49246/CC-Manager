"""Tests for RalphLoop — only lifecycle management, not the full _loop body."""
import asyncio
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

import pytest
from sqlalchemy import select
from unittest.mock import AsyncMock, MagicMock, patch

from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.plan import (
    Plan,
    PlanApplication,
    PlanLegacyTaskLink,
    PlanVersion,
)
from backend.models.task import Task
from backend.models.test_harness import (
    TestHarnessChildBinding,
    TestHarnessRun,
)
from backend.services.dispatcher import GlobalDispatcher
from backend.services.instance_manager import LaunchSupersededError
from backend.services.ralph_loop import RalphLoop
from backend.services.task_queue import TaskQueue, task_generation_fence
from backend.services.test_harness_children import (
    CHILD_READY,
    CHILD_RUNNING,
    TestHarnessChildService,
)
from backend.services import worker_task_termination as termination


def _make_ralph_loop():
    return RalphLoop(
        db_factory=MagicMock(),
        instance_manager=MagicMock(),
        broadcaster=MagicMock(),
    )


class _HarnessFenceStub:
    def __init__(self, *, enter_error: Exception | None = None):
        self.enter_error = enter_error
        self.events: list[str] = []
        self.calls: list[tuple[int, str, object]] = []
        self.inside = False

    @asynccontextmanager
    async def owner_stop_fence(
        self,
        task_id,
        *,
        reason,
        expected_identity,
    ):
        self.calls.append((task_id, reason, expected_identity))
        self.events.append("enter")
        if self.enter_error is not None:
            raise self.enter_error
        self.inside = True
        try:
            yield
        finally:
            self.inside = False
            self.events.append("exit")


async def _stage_ralph_browser_child(db_factory, instance_id: int):
    run_id = uuid.uuid4().hex
    job_id = uuid.uuid4().hex
    async with db_factory() as db:
        owner = Task(
            title="Browser Harness owner",
            description="owner",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
            codex_service_tier="default",
            effort_level="high",
        )
        db.add(owner)
        await db.flush()
        db.add(
            TestHarnessRun(
                id=run_id,
                task_id=owner.id,
                owner_task_incarnation_id=owner.incarnation_id,
                owner_task_retry_count=owner.retry_count,
                owner_task_turn_generation=owner.turn_generation,
                owner_task_status=owner.status,
                target_kind="fixed_url",
                target_spec={"url": "https://example.com"},
                test_plan={"objective": "Review the page"},
                runtime_config={"provider": "codex"},
                request_fingerprint="a" * 64,
                root_run_id=run_id,
                status="running",
                stage="preparing",
            )
        )
        await db.commit()
        owner_id = owner.id

    service = TestHarnessChildService(db_factory=db_factory)
    child, binding = await service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id=job_id,
        harness_run_id=run_id,
        child_values={
            "title": "Isolated Browser Agent",
            "description": "Review one frozen target",
            "priority": 0,
            "max_retries": 0,
            "mode": "auto",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "default",
            "effort_level": "high",
            "enabled_skills": {"browser-review": job_id},
            "archived": True,
        },
    )
    await service.activate(binding.id)
    async with db_factory() as db:
        claimed = await TaskQueue(db).dequeue(instance_id=instance_id)
        assert claimed is not None and claimed.id == child.id
    return claimed, binding.id


async def _stage_active_worker_termination_receipt(
    db_factory,
    task_id: int,
):
    """Stage the real Worker-side gate for the Task's current generation."""

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task is not None
        operation_id = f"{task.id:032x}"
        payload = {
            "version": 2,
            "operation_id": operation_id,
            "task_id": task.id,
            "operation": "cancel",
            "manager_worker_id": 41,
            "expected_remote": {
                "status": task.status,
                "retry_count": task.retry_count,
                "turn_generation": task.turn_generation,
            },
            "manager_handoff": None,
        }
        return await termination.stage_worker_receipt(
            db,
            task_id=task.id,
            operation_id=operation_id,
            operation="cancel",
            request_payload=payload,
            request_digest=termination.canonical_json_digest(payload),
        )


def _install_settling_failed_stop(instance_manager, db_factory):
    async def settle(
        instance_id,
        *,
        expected_task_id,
        expected_task_turn_generation,
        expected_pid,
        expected_started_at,
        task_status,
        task_error_message,
        terminal_consumer_timeout,
        consumer_cancel_timeout,
    ):
        assert task_status == "failed"
        assert task_error_message
        assert terminal_consumer_timeout == 30.0
        assert consumer_cancel_timeout == 10.0
        async with db_factory() as db:
            task = await db.get(Task, expected_task_id)
            instance = await db.get(Instance, instance_id)
            assert task.status == "failed"
            assert task.turn_generation == expected_task_turn_generation
            assert instance.current_task_id == expected_task_id
            assert instance.pid == expected_pid
            assert instance.started_at == expected_started_at
            task.pty_background_generation = None
            instance.status = "error"
            instance.pid = None
            instance.current_task_id = None
            await db.commit()
        instance_manager.processes.pop(instance_id, None)
        return True

    instance_manager.stop = AsyncMock(side_effect=settle)
    return instance_manager.stop


def test_effective_exit_code_uses_provider_semantic_result():
    rl = _make_ralph_loop()
    process = MagicMock(returncode=0)
    rl.instance_manager.effective_exit_code = MagicMock(return_value=1)

    assert rl._effective_process_exit_code(7, process) == 1
    rl.instance_manager.effective_exit_code.assert_called_once_with(
        7, process
    )


@pytest.mark.asyncio
async def test_start_creates_task():
    rl = _make_ralph_loop()
    # Patch _loop to be a simple coroutine that sleeps forever
    async def fake_loop(instance_id):
        await asyncio.sleep(999)

    rl._loop = fake_loop
    await rl.start(1)
    assert 1 in rl._loops
    assert not rl._loops[1].done()
    # Cleanup
    rl._loops[1].cancel()
    try:
        await rl._loops[1]
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_start_idempotent():
    rl = _make_ralph_loop()

    async def fake_loop(instance_id):
        await asyncio.sleep(999)

    rl._loop = fake_loop
    await rl.start(1)
    first_task = rl._loops[1]
    await rl.start(1)
    assert rl._loops[1] is first_task  # Same task, not replaced
    # Cleanup
    first_task.cancel()
    try:
        await first_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_stop_cancels():
    rl = _make_ralph_loop()

    async def fake_loop(instance_id):
        await asyncio.sleep(999)

    rl._loop = fake_loop
    await rl.start(1)
    task = rl._loops[1]
    await rl.stop(1)
    assert 1 not in rl._loops
    # Give event loop a tick for cancellation
    await asyncio.sleep(0)
    assert task.cancelled() or task.done()


@pytest.mark.asyncio
async def test_stop_plan_agent_lifecycle_settles_exact_ralph_producer():
    rl = _make_ralph_loop()
    plan_child = asyncio.create_task(asyncio.Event().wait())

    async def producer():
        await plan_child

    producer_task = asyncio.create_task(producer())
    await asyncio.sleep(0)
    rl._loops[7] = producer_task
    rl._plan_lifecycles[23] = (7, plan_child)

    with patch(
        "backend.services.plan_agent_runner.has_unreaped_plan_agent_for_task",
        return_value=False,
    ):
        assert await rl.stop_plan_agent_lifecycle(23) is True

    assert plan_child.done()
    assert producer_task.done()
    assert 7 not in rl._loops


@pytest.mark.asyncio
async def test_stop_timeout_retains_live_loop_evidence():
    rl = _make_ralph_loop()
    release = asyncio.Event()

    async def ignores_first_cancellation(_instance_id):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()

    rl._loop = ignores_first_cancellation
    await rl.start(1)
    task = rl._loops[1]
    await asyncio.sleep(0)
    try:
        assert await rl.stop(1, timeout=0.01) is False
        assert rl._loops[1] is task
        assert rl.is_running(1)
    finally:
        release.set()
        await asyncio.wait_for(task, timeout=1)
        assert await rl.stop(1, timeout=0.01) is True
        assert 1 not in rl._loops


@pytest.mark.asyncio
async def test_wait_for_turn_fails_closed_when_output_consumer_times_out():
    process = MagicMock(returncode=0)
    process.wait = AsyncMock(return_value=0)
    instance_manager = MagicMock()
    instance_manager.wait_for_output_consumer = AsyncMock(
        side_effect=asyncio.TimeoutError
    )
    rl = RalphLoop(
        db_factory=MagicMock(),
        instance_manager=instance_manager,
        broadcaster=MagicMock(),
    )
    task = MagicMock(id=23, provider="claude")

    with pytest.raises(
        RuntimeError,
        match="Output consumer did not finish after Task run for task 23",
    ):
        await rl._wait_for_turn(
            7,
            task,
            process,
            label="Task run",
        )

    instance_manager.wait_for_output_consumer.assert_awaited_once_with(
        7,
        provider="claude",
        timeout=30,
        expected_process=process,
    )


@pytest.mark.asyncio
async def test_stop_returns_claimed_task_to_pending_before_it_returns(db_factory):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    instance_manager.is_running.return_value = False
    instance_manager.stop = AsyncMock()
    instance_manager.wait_for_output_consumer = AsyncMock()
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )

    async with db_factory() as db:
        instance = Instance(name="ralph-cancel-worker")
        task = Task(title="claimed", description="work")
        db.add_all([instance, task])
        await db.commit()
        await db.refresh(instance)
        await db.refresh(task)
        instance_id, task_id = instance.id, task.id

    launch_entered = asyncio.Event()
    never_finish = asyncio.Event()

    async def blocked_launch(*_args, **_kwargs):
        launch_entered.set()
        await never_finish.wait()

    rl._launch_task_on_bound_account = blocked_launch
    await rl.start(instance_id)
    # The first Ralph loop in this module lazily imports the application
    # singleton graph before dequeueing. Keep the lifecycle assertion bounded
    # without coupling it to import speed on slower CI runners.
    await asyncio.wait_for(launch_entered.wait(), timeout=3)

    async with db_factory() as db:
        claimed = await db.get(Task, task_id)
        assert claimed.status == "in_progress"
        assert claimed.instance_id == instance_id

    loop_task = rl._loops[instance_id]
    await rl.stop(instance_id)

    assert loop_task.cancelled()
    assert instance_id not in rl._loops
    async with db_factory() as db:
        released = await db.get(Task, task_id)
        assert released.status == "pending"
        assert released.instance_id is None
        assert "Ralph loop stopped" in released.error_message
    instance_manager.stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_adopts_marker_only_handoff_and_awaits_exact_stop(
    db_factory,
):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    stop_entered = asyncio.Event()
    allow_stop = asyncio.Event()

    async def exact_stop(*_args, **_kwargs):
        stop_entered.set()
        await allow_stop.wait()
        return True

    instance_manager = MagicMock()
    instance_manager.is_running.return_value = True
    instance_manager.stop = AsyncMock(side_effect=exact_stop)
    rl = RalphLoop(db_factory, instance_manager, broadcaster)
    started_at = datetime(2026, 4, 5, 6, 7, 8)

    async with db_factory() as db:
        instance = Instance(
            name="ralph-cancel-marker-handoff",
            status="running",
            pid=6060,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        stale_task = Task(
            title="cancel after PTY callback",
            description="work",
            status="in_progress",
            instance_id=instance.id,
        )
        db.add(stale_task)
        await db.flush()
        instance.current_task_id = stale_task.id
        await db.commit()
        await db.refresh(stale_task)
        task_id = stale_task.id
        instance_id = instance.id

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        current.pty_background_generation = "cancel-native-epoch"
        await db.commit()

    cleanup = asyncio.create_task(
        rl._release_cancelled_claim(instance_id, stale_task)
    )
    await asyncio.wait_for(stop_entered.wait(), timeout=1)
    assert cleanup.done() is False
    allow_stop.set()
    await asyncio.wait_for(cleanup, timeout=1)

    instance_manager.stop.assert_awaited_once_with(
        instance_id,
        expected_task_id=task_id,
        expected_task_turn_generation=0,
        expected_pid=6060,
        expected_started_at=started_at,
        task_status="failed",
        task_error_message=(
            "Ralph loop stopped after provider admission; the exact turn "
            "outcome is uncertain"
        ),
        terminal_consumer_timeout=30.0,
        consumer_cancel_timeout=10.0,
    )
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        # The stop mock intentionally performs no persistence; reaching it and
        # awaiting it must not make Ralph drop the durable exact owner.
        assert current.status == "in_progress"
        assert current.instance_id == instance_id
        assert current.pty_background_generation == "cancel-native-epoch"
        assert instance.current_task_id == task_id
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.parametrize(
    "provider,actual_transport",
    [
        ("claude", "claude_exec"),
        ("codex", "codex_app_server"),
    ],
)
@pytest.mark.asyncio
async def test_ralph_running_turn_cancel_fails_closed_without_requeue(
    db_factory,
    provider,
    actual_transport,
):
    """Cancelling an admitted provider turn can never authorize replay."""

    from backend.services.terminal_arbitration import bind_turn_source

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    instance_manager.is_running.return_value = True
    rl = RalphLoop(db_factory, instance_manager, broadcaster)
    started_at = datetime(2026, 4, 5, 6, 7, 9)

    async with db_factory() as db:
        instance = Instance(
            name=f"ralph-cancel-{provider}",
            status="running",
            pid=6061,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title=f"cancel admitted {provider}",
            description="may already have changed the remote",
            provider=provider,
            status="executing",
            instance_id=instance.id,
            retry_count=2,
            turn_generation=4,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        source = await bind_turn_source(
            db,
            task=task,
            source_log_id=None,
            instance_id=instance.id,
            transport=("codex" if provider == "codex" else None),
        )
        source.actual_transport = actual_transport
        await db.commit()
        await db.refresh(task)
        task_id = task.id
        instance_id = instance.id
        source_id = source.id

    async def settle_failed_stop(
        stopped_instance_id,
        *,
        expected_task_id,
        expected_task_turn_generation,
        expected_pid,
        expected_started_at,
        task_status,
        task_error_message,
        terminal_consumer_timeout,
        consumer_cancel_timeout,
    ):
        assert stopped_instance_id == instance_id
        assert expected_task_id == task_id
        assert expected_task_turn_generation == 4
        assert expected_pid == 6061
        assert expected_started_at == started_at
        assert task_status == "failed"
        assert "outcome is uncertain" in task_error_message
        assert terminal_consumer_timeout == 30.0
        assert consumer_cancel_timeout == 10.0
        async with db_factory() as db:
            current = await db.get(Task, task_id)
            current_instance = await db.get(Instance, instance_id)
            current.status = task_status
            current.error_message = task_error_message
            current.completed_at = datetime.utcnow()
            current_instance.status = "error"
            current_instance.pid = None
            current_instance.current_task_id = None
            await db.commit()
        return True

    instance_manager.stop = AsyncMock(side_effect=settle_failed_stop)

    await rl._release_cancelled_claim(instance_id, task)

    instance_manager.stop.assert_awaited_once()
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        source = await db.get(LogEntry, source_id)
        assert current.status == "failed"
        assert current.retry_count == 2
        assert current.turn_generation == 4
        assert current.turn_source_log_id == source_id
        assert current.instance_id == instance_id
        assert "outcome is uncertain" in current.error_message
        assert source.actual_transport == actual_transport
        assert await TaskQueue(db).dequeue(instance_id=instance_id) is None


@pytest.mark.asyncio
async def test_ralph_dequeue_waits_for_shared_maintenance_gate(db_factory):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    gate = GlobalDispatcher(db_factory, instance_manager, broadcaster)
    await gate.pause_dispatching()
    rl = RalphLoop(db_factory, instance_manager, broadcaster)

    async with db_factory() as db:
        instance = Instance(name="ralph-maintenance-worker")
        task = Task(title="must stay pending", description="work")
        db.add_all([instance, task])
        await db.commit()
        await db.refresh(instance)
        await db.refresh(task)
        instance_id, task_id = instance.id, task.id

    with patch("backend.main.dispatcher", gate):
        await rl.start(instance_id)
        await asyncio.sleep(0.05)
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            assert task.status == "pending"
        instance_manager.launch.assert_not_called()
        await rl.stop(instance_id)


@pytest.mark.asyncio
async def test_ralph_yields_browser_child_to_dispatcher_without_hot_loop(
    db_factory,
):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    instance_manager.processes = {}
    dispatcher = GlobalDispatcher(db_factory, instance_manager, broadcaster)
    wake_seen = asyncio.Event()
    dispatcher.wake = MagicMock(side_effect=wake_seen.set)
    rl = RalphLoop(db_factory, instance_manager, broadcaster)

    async with db_factory() as db:
        instance = Instance(name="ralph-browser-yield")
        db.add(instance)
        await db.commit()
        instance_id = instance.id

    child, binding_id = await _stage_ralph_browser_child(
        db_factory,
        instance_id,
    )
    # Return the exact claim to Ralph's loop so the real dequeue path observes
    # it and then proves that it is Dispatcher-only.
    async with db_factory() as db:
        assert await TaskQueue(db).defer(
            child.id,
            "stage Ralph handoff",
            instance_id=instance_id,
            generation_fence=task_generation_fence(child),
        )

    original_dequeue = TaskQueue.dequeue
    dequeue_exclusions: list[set[int]] = []
    excluded_retry_seen = asyncio.Event()

    async def recording_dequeue(self, exclude_ids=None, **kwargs):
        excluded = set(exclude_ids or ())
        dequeue_exclusions.append(excluded)
        result = await original_dequeue(
            self,
            exclude_ids=exclude_ids,
            **kwargs,
        )
        if child.id in excluded:
            excluded_retry_seen.set()
        return result

    with (
        patch("backend.main.dispatcher", dispatcher),
        patch.object(TaskQueue, "dequeue", new=recording_dequeue),
    ):
        producer = asyncio.create_task(rl._loop(instance_id))
        try:
            await asyncio.wait_for(wake_seen.wait(), timeout=3)
            await asyncio.wait_for(excluded_retry_seen.wait(), timeout=3)
        finally:
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)

    async with db_factory() as db:
        durable_child = await db.get(Task, child.id)
        binding = await db.get(TestHarnessChildBinding, binding_id)
        assert durable_child.status == "pending"
        assert durable_child.instance_id is None
        assert binding.state == CHILD_READY
        assert binding.claimed_retry_count is None
        assert binding.claimed_instance_id is None
    assert dequeue_exclusions[0] == set()
    assert child.id in dequeue_exclusions[-1]
    dispatcher.wake.assert_called_once_with()
    instance_manager.launch.assert_not_called()
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_ralph_retains_browser_claim_when_binding_identity_drifts(
    db_factory,
):
    async with db_factory() as db:
        instance = Instance(name="ralph-browser-corrupt")
        db.add(instance)
        await db.commit()
        instance_id = instance.id

    child, binding_id = await _stage_ralph_browser_child(
        db_factory,
        instance_id,
    )
    async with db_factory() as db:
        binding = await db.get(TestHarnessChildBinding, binding_id)
        binding.launch_config_digest = "0" * 64
        await db.commit()

    instance_manager = MagicMock()
    rl = RalphLoop(db_factory, instance_manager, MagicMock())
    assert await rl._defer_isolated_browser_claim(instance_id, child) is False

    async with db_factory() as db:
        durable_child = await db.get(Task, child.id)
        binding = await db.get(TestHarnessChildBinding, binding_id)
        assert durable_child.status == "in_progress"
        assert durable_child.instance_id == instance_id
        assert binding.state == CHILD_RUNNING
        assert binding.claimed_retry_count == child.retry_count
        assert binding.claimed_instance_id == instance_id
    instance_manager.launch.assert_not_called()


async def _run_ralph_until_plan_failure(db_factory, instance_id):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    instance_manager.processes = {}
    instance_manager.stop = AsyncMock(return_value=True)
    instance_manager.wait_for_output_consumer = AsyncMock()
    gate = GlobalDispatcher(db_factory, instance_manager, broadcaster)
    rl = RalphLoop(db_factory, instance_manager, broadcaster)
    rl._launch_task_on_bound_account = AsyncMock()
    failed = asyncio.Event()
    original_fail = rl._fail_unexpected_claim

    async def capture_failure(*args, **kwargs):
        await original_fail(*args, **kwargs)
        failed.set()

    rl._fail_unexpected_claim = capture_failure
    with patch("backend.main.dispatcher", gate):
        loop_task = asyncio.create_task(rl._loop(instance_id))
        try:
            await asyncio.wait_for(failed.wait(), timeout=1)
        finally:
            loop_task.cancel()
            await asyncio.gather(loop_task, return_exceptions=True)
    return rl


@pytest.mark.asyncio
async def test_ralph_rejected_plan_never_reenters_planning_or_launches(
    db_factory,
):
    async with db_factory() as db:
        instance = Instance(name="ralph-rejected-plan")
        task = Task(
            title="rejected Plan",
            description="must not run",
            mode="plan",
            plan_approved=False,
        )
        db.add_all([instance, task])
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    rl = await _run_ralph_until_plan_failure(db_factory, instance_id)

    rl._launch_task_on_bound_account.assert_not_awaited()
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "failed"
        assert "Rejected Plan Tasks" in current.error_message


@pytest.mark.asyncio
async def test_ralph_malformed_legacy_plan_proof_never_authorizes_launch(
    db_factory,
):
    async with db_factory() as db:
        instance = Instance(name="ralph-malformed-plan")
        task = Task(
            title="malformed approved Plan",
            description="must not run",
            mode="plan",
            plan_approved=True,
        )
        plan = Plan(
            title="Migrated Plan",
            initial_request="legacy request",
            pipeline_config={},
        )
        db.add_all([instance, task, plan])
        await db.flush()
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            content="# Approved legacy plan",
            human_decision="approved",
        )
        db.add(version)
        await db.flush()
        db.add_all(
            [
                PlanLegacyTaskLink(
                    legacy_task_id=task.id,
                    plan_id=plan.id,
                    plan_version_id=version.id,
                ),
                PlanApplication(
                    plan_id=plan.id + 1000,
                    plan_version_id=version.id,
                    application_type="execution_task",
                    execution_task_id=task.id,
                ),
            ]
        )
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    rl = await _run_ralph_until_plan_failure(db_factory, instance_id)

    rl._launch_task_on_bound_account.assert_not_awaited()
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "failed"
        assert "exact migrated execution carrier" in current.error_message


@pytest.mark.asyncio
async def test_ralph_exact_legacy_plan_carrier_keeps_compatible_launch(
    db_factory,
):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    instance_manager.stop = AsyncMock(return_value=True)
    instance_manager.wait_for_output_consumer = AsyncMock()
    instance_manager.effective_exit_code = MagicMock(return_value=0)
    gate = GlobalDispatcher(db_factory, instance_manager, broadcaster)
    rl = RalphLoop(db_factory, instance_manager, broadcaster)

    async with db_factory() as db:
        instance = Instance(name="ralph-migrated-plan")
        task = Task(
            title="migrated approved Plan",
            description="execute the approved legacy Plan",
            mode="plan",
            plan_approved=True,
            plan_content="# Approved legacy plan",
        )
        plan = Plan(
            title="Migrated Plan",
            initial_request="legacy request",
            pipeline_config={},
        )
        db.add_all([instance, task, plan])
        await db.flush()
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            content="# Approved legacy plan",
            human_decision="approved",
        )
        db.add(version)
        await db.flush()
        db.add_all(
            [
                PlanLegacyTaskLink(
                    legacy_task_id=task.id,
                    plan_id=plan.id,
                    plan_version_id=version.id,
                ),
                PlanApplication(
                    plan_id=plan.id,
                    plan_version_id=version.id,
                    application_type="execution_task",
                    execution_task_id=task.id,
                ),
            ]
        )
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    process = MagicMock(pid=27182, returncode=0)
    process.wait = AsyncMock(return_value=0)
    instance_manager.processes = {instance_id: process}
    launched = asyncio.Event()

    async def compatible_launch(*_args, **_kwargs):
        launched.set()
        return process.pid

    rl._launch_task_on_bound_account = AsyncMock(
        side_effect=compatible_launch
    )
    with patch("backend.main.dispatcher", gate):
        loop_task = asyncio.create_task(rl._loop(instance_id))
        try:
            await asyncio.wait_for(launched.wait(), timeout=1)
            for _ in range(300):
                async with db_factory() as db:
                    current = await db.get(Task, task_id)
                    if current.status == "completed":
                        break
                await asyncio.sleep(0.01)
            else:
                pytest.fail("Ralph legacy carrier did not complete")
        finally:
            loop_task.cancel()
            await asyncio.gather(loop_task, return_exceptions=True)

    rl._launch_task_on_bound_account.assert_awaited_once()
    launch_kwargs = rl._launch_task_on_bound_account.await_args.kwargs
    assert type(launch_kwargs["source_log_id"]) is int


@pytest.mark.asyncio
async def test_stale_dequeue_claim_is_not_published_or_launched(db_factory):
    """A cancelled/retried claim must not emit a late in-progress event."""

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    instance_manager.is_running.return_value = False
    instance_manager.stop = AsyncMock()
    instance_manager.wait_for_output_consumer = AsyncMock()
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )

    async with db_factory() as db:
        instance = Instance(name="ralph-stale-claim")
        task = Task(title="stale claim", description="must not launch")
        db.add_all([instance, task])
        await db.commit()
        await db.refresh(instance)
        instance_id = instance.id

    claim_checked = asyncio.Event()

    async def reject_stale_claim(*_args, **_kwargs):
        claim_checked.set()
        return False

    rl._broadcast_generation_event = AsyncMock(
        side_effect=reject_stale_claim
    )
    rl._launch_task_on_bound_account = AsyncMock()

    loop_task = asyncio.create_task(rl._loop(instance_id))
    try:
        await asyncio.wait_for(claim_checked.wait(), timeout=1)
        await asyncio.sleep(0)
    finally:
        loop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await loop_task

    rl._launch_task_on_bound_account.assert_not_awaited()
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_termination_receipt_wins_before_ralph_source_binding(
    db_factory,
):
    """Receipt recovery owns a claim won after dequeue but before binding."""

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    instance_manager.stop = AsyncMock()
    rl = RalphLoop(db_factory, instance_manager, broadcaster)

    async with db_factory() as db:
        instance = Instance(name="ralph-receipt-before-source")
        task = Task(
            title="receipt wins before source",
            description="must not launch",
        )
        db.add_all([instance, task])
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    async with db_factory() as db:
        task = await TaskQueue(db).dequeue(instance_id=instance_id)
    assert task is not None
    assert task.id == task_id
    await _stage_active_worker_termination_receipt(db_factory, task_id)

    with pytest.raises(
        RuntimeError,
        match="changed before source binding",
    ) as bind_error:
        await rl._bind_claimed_turn_source(instance_id, task)

    # This is the outer Ralph exception path.  It must interpret the lost
    # source bind as a superseded lifecycle, not as authority to fail the Task.
    await rl._fail_unexpected_claim(
        instance_id,
        task,
        bind_error.value,
    )

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        receipt = await termination.active_worker_task_termination_receipt(
            db,
            task_id,
        )
        source_id = await db.scalar(
            select(LogEntry.id)
            .where(
                LogEntry.task_id == task_id,
                LogEntry.turn_scope == "source",
            )
            .limit(1)
        )
    assert current is not None
    assert current.status == "in_progress"
    assert current.instance_id == instance_id
    assert current.completed_at is None
    assert current.error_message is None
    assert current.turn_source_log_id is None
    assert source_id is None
    assert receipt is not None
    assert receipt.status == "accepted"
    instance_manager.stop.assert_not_awaited()
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_termination_receipt_wins_after_ralph_source_binding(
    db_factory,
):
    """A provider-boundary veto cannot upgrade or fail a bound source."""

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    instance_manager.stop = AsyncMock()
    rl = RalphLoop(db_factory, instance_manager, broadcaster)

    async with db_factory() as db:
        instance = Instance(name="ralph-receipt-after-source")
        task = Task(
            title="receipt wins before provider boundary",
            description="must not cross provider boundary",
        )
        db.add_all([instance, task])
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    async with db_factory() as db:
        task = await TaskQueue(db).dequeue(instance_id=instance_id)
    assert task is not None
    source_id = await rl._bind_claimed_turn_source(instance_id, task)
    await _stage_active_worker_termination_receipt(db_factory, task_id)

    await rl._fail_unexpected_claim(
        instance_id,
        task,
        LaunchSupersededError(
            "provider boundary yielded to an active termination receipt"
        ),
    )

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        source = await db.get(LogEntry, source_id)
        receipt = await termination.active_worker_task_termination_receipt(
            db,
            task_id,
        )
    assert current is not None
    assert current.status == "in_progress"
    assert current.instance_id == instance_id
    assert current.completed_at is None
    assert current.error_message is None
    assert current.turn_source_log_id == source_id
    assert source is not None
    assert source.actual_transport is None
    assert receipt is not None
    assert receipt.status == "accepted"
    instance_manager.stop.assert_not_awaited()
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.parametrize("retry_after", [None, 7.0])
@pytest.mark.asyncio
async def test_termination_receipt_blocks_ralph_account_routing_settlement(
    db_factory,
    retry_after,
):
    """Neither permanent nor retryable routing errors can rewrite the Task."""

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    rl = RalphLoop(db_factory, MagicMock(), broadcaster)

    async with db_factory() as db:
        instance = Instance(name=f"ralph-receipt-routing-{retry_after}")
        task = Task(
            title="receipt owns account routing settlement",
            description="must remain claimed",
            provider="codex",
        )
        db.add_all([instance, task])
        await db.commit()
        instance_id = instance.id
        task_id = task.id

    async with db_factory() as db:
        task = await TaskQueue(db).dequeue(instance_id=instance_id)
    assert task is not None
    source_id = await rl._bind_claimed_turn_source(instance_id, task)
    await _stage_active_worker_termination_receipt(db_factory, task_id)

    delay = await rl._handle_account_routing_failure(
        instance_id,
        task,
        "late account routing failure",
        retry_after=retry_after,
    )

    assert delay == 0
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        source = await db.get(LogEntry, source_id)
        receipt = await termination.active_worker_task_termination_receipt(
            db,
            task_id,
        )
    assert current is not None
    assert current.status == "in_progress"
    assert current.instance_id == instance_id
    assert current.completed_at is None
    assert current.error_message is None
    assert current.turn_source_log_id == source_id
    assert source is not None
    assert source.actual_transport is None
    assert receipt is not None
    assert receipt.status == "accepted"
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_termination_receipt_blocks_ralph_plan_result_store(db_factory):
    """A late Plan child cannot publish over receipt-owned Task state."""

    rl = RalphLoop(db_factory, MagicMock(), MagicMock())
    async with db_factory() as db:
        instance = Instance(name="ralph-receipt-plan")
        db.add(instance)
        await db.flush()
        task = Task(
            title="receipt owns plan result",
            description="plan",
            mode="plan",
            status="in_progress",
            instance_id=instance.id,
            plan_content="receipt-owned plan",
            metadata_={"owner": "termination-receipt"},
            error_message="preserve receipt-owned evidence",
        )
        db.add(task)
        await db.commit()
        instance_id = instance.id
        task_id = task.id
        await db.refresh(task)

    await _stage_active_worker_termination_receipt(db_factory, task_id)

    stored = await rl._store_plan_if_owned(
        instance_id,
        task,
        "late plan result",
        metadata_updates={"owner": "late-plan-child"},
    )

    assert stored is False
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        receipt = await termination.active_worker_task_termination_receipt(
            db,
            task_id,
        )
    assert current is not None
    assert current.status == "in_progress"
    assert current.plan_content == "receipt-owned plan"
    assert current.metadata_ == {"owner": "termination-receipt"}
    assert current.error_message == "preserve receipt-owned evidence"
    assert receipt is not None
    assert receipt.status == "accepted"


@pytest.mark.asyncio
async def test_termination_receipt_blocks_task_queue_terminal_writers(
    db_factory,
):
    """Generic and exact TaskQueue finalizers must yield to receipt recovery."""

    started_at = datetime(2026, 8, 7, 10, 30)
    async with db_factory() as db:
        instance = Instance(name="task-queue-receipt-finalizer")
        db.add(instance)
        await db.flush()
        task = Task(
            title="receipt owns TaskQueue finalization",
            description="preserve exact lifecycle state",
            status="in_progress",
            instance_id=instance.id,
            retry_count=2,
            turn_generation=4,
            started_at=started_at,
            error_message="receipt-owned evidence",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id
        instance_id = instance.id
        generation = task_generation_fence(task)

    await _stage_active_worker_termination_receipt(db_factory, task_id)

    async with db_factory() as db:
        queue = TaskQueue(db)
        assert not await queue.mark_completed(
            task_id,
            expected_statuses=("in_progress",),
            instance_id=instance_id,
            generation_fence=generation,
        )
        assert not await queue.mark_failed(
            task_id,
            "late exact failure",
            expected_statuses=("in_progress",),
            instance_id=instance_id,
            generation_fence=generation,
        )
        await queue.mark_status(
            task_id,
            "failed",
            error_message="late generic failure",
        )

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        receipt = await termination.active_worker_task_termination_receipt(
            db,
            task_id,
        )
    assert current is not None
    assert current.status == "in_progress"
    assert current.instance_id == instance_id
    assert current.retry_count == 2
    assert current.turn_generation == 4
    assert current.started_at == started_at
    assert current.completed_at is None
    assert current.error_message == "receipt-owned evidence"
    assert receipt is not None
    assert receipt.status == "accepted"


@pytest.mark.asyncio
async def test_is_running_true():
    rl = _make_ralph_loop()

    async def fake_loop(instance_id):
        await asyncio.sleep(999)

    rl._loop = fake_loop
    await rl.start(1)
    assert rl.is_running(1) is True
    # Cleanup
    rl._loops[1].cancel()
    try:
        await rl._loops[1]
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_is_running_false():
    rl = _make_ralph_loop()
    assert rl.is_running(1) is False
    assert rl.is_running(999) is False


@pytest.mark.asyncio
async def test_codex_task_launch_resolves_home_and_resumes_native_thread():
    rl = _make_ralph_loop()
    rl.instance_manager.launch = AsyncMock(return_value=4321)
    dispatcher = MagicMock()
    dispatcher._resolve_resume_config_dir = AsyncMock(
        return_value="/pool/codex-2"
    )
    task = MagicMock(
        id=77,
        provider="codex",
        model="gpt-5.6-sol",
        session_id="thread-ralph-1",
        thinking_budget=1234,
        codex_service_tier="priority",
        turn_source_log_id=701,
        execution_user_id=None,
        execution_user_role="super_admin",
        execution_mode="unrestricted",
        execution_principal_kind="deployment_token",
    )

    with patch("backend.main.dispatcher", dispatcher):
        pid = await rl._launch_task_on_bound_account(
            9,
            task,
            "continue work",
            "/repo",
            source_log_id=701,
        )

    assert pid == 4321
    dispatcher._resolve_resume_config_dir.assert_awaited_once_with(
        "thread-ralph-1",
        "codex",
        task_id=77,
        model="gpt-5.6-sol",
        codex_service_tier="priority",
    )
    launch_kwargs = rl.instance_manager.launch.await_args.kwargs
    assert launch_kwargs["config_dir"] == "/pool/codex-2"
    assert launch_kwargs["resume_session_id"] == "thread-ralph-1"
    assert launch_kwargs["provider"] == "codex"
    assert launch_kwargs["model"] == "gpt-5.6-sol"
    assert launch_kwargs["codex_service_tier"] == "priority"
    assert launch_kwargs["source_log_id"] == 701
    assert launch_kwargs["initiating_user_id"] is None
    assert launch_kwargs["initiating_user_role"] == "super_admin"
    assert launch_kwargs["execution_mode"] == "unrestricted"
    assert launch_kwargs["execution_principal_kind"] == "deployment_token"


@pytest.mark.asyncio
async def test_claude_task_launch_uses_resolved_home_without_forcing_resume():
    rl = _make_ralph_loop()
    rl.instance_manager.launch = AsyncMock(return_value=123)
    dispatcher = MagicMock()
    dispatcher._resolve_resume_config_dir = AsyncMock(
        return_value="/pool/claude-2"
    )
    task = MagicMock(
        id=78,
        provider="claude",
        model="claude-opus-4-8",
        session_id="claude-session",
        thinking_budget=None,
        codex_service_tier="default",
        turn_source_log_id=702,
        execution_user_id=51,
        execution_user_role="admin",
        execution_mode="unrestricted",
        execution_principal_kind="user",
    )

    with patch("backend.main.dispatcher", dispatcher):
        await rl._launch_task_on_bound_account(
            10,
            task,
            "work",
            "/repo",
            source_log_id=702,
        )

    dispatcher._resolve_resume_config_dir.assert_awaited_once_with(
        "claude-session",
        "claude",
        task_id=78,
        model="claude-opus-4-8",
        codex_service_tier="default",
    )
    launch_kwargs = rl.instance_manager.launch.await_args.kwargs
    assert launch_kwargs["config_dir"] == "/pool/claude-2"
    assert launch_kwargs["resume_session_id"] is None
    assert launch_kwargs["model"] == "claude-opus-4-8"
    assert launch_kwargs["source_log_id"] == 702
    assert launch_kwargs["initiating_user_id"] == 51
    assert launch_kwargs["initiating_user_role"] == "admin"
    assert launch_kwargs["execution_mode"] == "unrestricted"
    assert launch_kwargs["execution_principal_kind"] == "user"


@pytest.mark.asyncio
async def test_retryable_account_routing_failure_defers_claimed_task(db_factory):
    from backend.services.terminal_arbitration import bind_turn_source

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=MagicMock(),
        broadcaster=broadcaster,
    )
    async with db_factory() as db:
        instance = Instance(name="ralph-routing-worker")
        db.add(instance)
        await db.flush()
        task = Task(
            title="routing wait",
            description="continue",
            provider="codex",
            status="in_progress",
            instance_id=instance.id,
            retry_count=2,
        )
        db.add(task)
        await db.flush()
        await bind_turn_source(
            db,
            task=task,
            source_log_id=None,
            instance_id=instance.id,
            transport="codex",
        )
        await db.commit()
        await db.refresh(task)
        task_id = task.id
        instance_id = instance.id

    delay = await rl._handle_account_routing_failure(
        instance_id,
        task,
        "all Codex accounts are cooling down",
        retry_after=7,
    )

    assert delay == 7
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "pending"
        assert task.instance_id is None
        assert task.retry_count == 2
        assert "cooling down" in task.error_message
    broadcaster.broadcast.assert_awaited_once()


@pytest.mark.parametrize("actual_transport", ["claude_exec", "codex_app_server"])
@pytest.mark.asyncio
async def test_ralph_post_boundary_nonzero_fails_closed_without_retry(
    db_factory,
    actual_transport,
):
    from backend.services.terminal_arbitration import bind_turn_source

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=MagicMock(),
        broadcaster=broadcaster,
    )
    async with db_factory() as db:
        instance = Instance(name=f"ralph-{actual_transport}")
        db.add(instance)
        await db.flush()
        task = Task(
            title="admitted turn failed",
            description="may have pushed",
            provider=("codex" if actual_transport.startswith("codex") else "claude"),
            status="in_progress",
            instance_id=instance.id,
            retry_count=1,
            max_retries=5,
            turn_generation=3,
        )
        db.add(task)
        await db.flush()
        source = await bind_turn_source(
            db,
            task=task,
            source_log_id=None,
            instance_id=instance.id,
            transport=("codex" if task.provider == "codex" else None),
        )
        source.actual_transport = actual_transport
        await db.commit()
        await db.refresh(task)
        task_id = task.id
        instance_id = instance.id
        source_id = source.id

    settled = await rl._settle_automatic_failure(
        instance_id,
        task,
        "Exit code: 1",
        defer_if_preflight=False,
    )

    assert settled is not None
    assert settled[0] == "failed"
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        source = await db.get(LogEntry, source_id)
        assert current.status == "failed"
        assert current.retry_count == 1
        assert current.turn_generation == 3
        assert current.turn_source_log_id == source_id
        assert actual_transport in current.error_message
        assert source.actual_transport == actual_transport


@pytest.mark.parametrize("retry_after", [None, 7.0])
@pytest.mark.asyncio
async def test_account_routing_failure_cannot_mutate_reassigned_task(
    db_factory,
    retry_after,
):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=MagicMock(),
        broadcaster=broadcaster,
    )
    async with db_factory() as db:
        old_instance = Instance(name="old-ralph-owner")
        new_instance = Instance(name="new-task-owner")
        db.add_all([old_instance, new_instance])
        await db.flush()
        task = Task(
            title="reassigned while routing",
            description="continue",
            provider="codex",
            status="executing",
            instance_id=new_instance.id,
            error_message="new generation is running",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id
        old_instance_id = old_instance.id
        new_instance_id = new_instance.id

    delay = await rl._handle_account_routing_failure(
        old_instance_id,
        task,
        "stale routing failure",
        retry_after=retry_after,
    )

    assert delay == 0
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "executing"
        assert task.instance_id == new_instance_id
        assert task.error_message == "new generation is running"
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_permanent_routing_failure_does_not_overwrite_cancellation(db_factory):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=MagicMock(),
        broadcaster=broadcaster,
    )
    async with db_factory() as db:
        task = Task(
            title="cancel wins routing failure",
            description="continue",
            provider="codex",
            status="cancelled",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    delay = await rl._handle_account_routing_failure(
        7,
        task,
        "permanent account binding error",
        retry_after=None,
    )

    assert delay == 0
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "cancelled"
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_terminal_publication_rejects_new_background_generation(
    db_factory,
):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    rl = RalphLoop(db_factory, MagicMock(), broadcaster)
    async with db_factory() as db:
        task = Task(
            title="late native tail",
            description="work",
            status="in_progress",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id
        foreground_generation = task_generation_fence(task)

        task.status = "completed"
        task.completed_at = datetime.utcnow()
        task.pty_background_generation = "new-background-epoch"
        await db.commit()

    published = await rl._broadcast_generation_event(
        task_id,
        foreground_generation,
        "completed",
        {
            "event": "status_change",
            "task_id": task_id,
            "new_status": "completed",
        },
        terminal=True,
    )

    assert published is False
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_ralph_completion_adopts_marker_only_pty_handoff(db_factory):
    rl = RalphLoop(db_factory, MagicMock(), MagicMock())
    async with db_factory() as db:
        instance = Instance(name="ralph-background-handoff")
        db.add(instance)
        await db.flush()
        task = Task(
            title="foreground completed before native child",
            description="work",
            status="in_progress",
            instance_id=instance.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id
        instance_id = instance.id
        foreground_generation = task_generation_fence(task)

        task.pty_background_generation = "native-tail-epoch"
        await db.commit()

    resulting = await rl._mark_completed_with_background_handoff(
        task_id,
        instance_id,
        foreground_generation,
    )

    assert resulting is not None
    assert resulting[-2] == "native-tail-epoch"
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "completed"
        assert current.completed_at is not None
        assert current.pty_background_generation == "native-tail-epoch"


@pytest.mark.asyncio
async def test_ralph_completion_keeps_owner_evidence_when_harness_cleanup_fails(
    db_factory,
):
    fence = _HarnessFenceStub(enter_error=RuntimeError("child cleanup failed"))
    instance_manager = MagicMock()
    rl = RalphLoop(
        db_factory,
        instance_manager,
        MagicMock(),
        test_harness_service=fence,
    )
    started_at = datetime(2026, 8, 1, 2, 3, 4)
    async with db_factory() as db:
        instance = Instance(
            name="ralph-completion-fence-failure",
            status="running",
            pid=7001,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="completion must wait for children",
            description="work",
            status="in_progress",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        await db.refresh(task)
        generation = task_generation_fence(task)
        task_id = task.id
        instance_id = instance.id

    assert (
        await rl._mark_completed_with_background_handoff(
            task_id,
            instance_id,
            generation,
        )
        is None
    )

    async with db_factory() as db:
        durable_task = await db.get(Task, task_id)
        durable_instance = await db.get(Instance, instance_id)
        assert durable_task.status == "in_progress"
        assert durable_task.completed_at is None
        assert durable_task.instance_id == instance_id
        assert durable_instance.status == "running"
        assert durable_instance.pid == 7001
        assert durable_instance.current_task_id == task_id
        assert durable_instance.started_at == started_at
    instance_manager.stop.assert_not_called()
    assert fence.events == ["enter"]


@pytest.mark.asyncio
async def test_ralph_completion_writes_only_inside_harness_fence(db_factory):
    fence = _HarnessFenceStub()
    rl = RalphLoop(
        db_factory,
        MagicMock(),
        MagicMock(),
        test_harness_service=fence,
    )
    async with db_factory() as db:
        instance = Instance(name="ralph-completion-order")
        db.add(instance)
        await db.flush()
        task = Task(
            title="ordered completion",
            description="work",
            status="in_progress",
            instance_id=instance.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id
        instance_id = instance.id
        generation = task_generation_fence(task)

    original_mark_completed = RalphLoop._mark_completed_generation

    async def assert_fenced_mark_completed(db, *args, **kwargs):
        assert fence.inside is True
        return await original_mark_completed(db, *args, **kwargs)

    with patch.object(
        RalphLoop,
        "_mark_completed_generation",
        new=staticmethod(assert_fenced_mark_completed),
    ):
        resulting = await rl._mark_completed_with_background_handoff(
            task_id,
            instance_id,
            generation,
        )

    assert resulting is not None
    assert fence.events == ["enter", "exit"]
    async with db_factory() as db:
        assert (await db.get(Task, task_id)).status == "completed"


@pytest.mark.asyncio
async def test_ralph_completion_retries_when_marker_clears_quickly(db_factory):
    rl = RalphLoop(db_factory, MagicMock(), MagicMock())
    async with db_factory() as db:
        instance = Instance(name="ralph-fast-background-tail")
        db.add(instance)
        await db.flush()
        task = Task(
            title="native tail settles during completion",
            description="work",
            status="in_progress",
            instance_id=instance.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id
        instance_id = instance.id
        foreground_generation = task_generation_fence(task)
        task.pty_background_generation = "short-lived-native-tail"
        await db.commit()

    original_mark_completed = RalphLoop._mark_completed_generation
    attempts = 0

    async def mark_then_settle(db, *args, **kwargs):
        nonlocal attempts
        attempts += 1
        changed = await original_mark_completed(db, *args, **kwargs)
        if attempts == 1:
            assert changed is False
            async with db_factory() as db:
                current = await db.get(Task, task_id)
                current.pty_background_generation = None
                await db.commit()
        return changed

    with patch.object(
        RalphLoop,
        "_mark_completed_generation",
        new=staticmethod(mark_then_settle),
    ):
        resulting = await rl._mark_completed_with_background_handoff(
            task_id,
            instance_id,
            foreground_generation,
        )

    assert attempts == 2
    assert resulting is not None
    assert resulting[-2] is None
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "completed"
        assert current.completed_at is not None
        assert current.pty_background_generation is None


@pytest.mark.asyncio
async def test_status_publication_uses_exact_background_snapshot(db_factory):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    rl = RalphLoop(db_factory, MagicMock(), broadcaster)
    async with db_factory() as db:
        task = Task(
            title="durable background snapshot",
            description="work",
            status="completed",
            completed_at=datetime.utcnow(),
            pty_background_generation="exact-background-epoch",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id
        generation = task_generation_fence(task)

    published = await rl._broadcast_generation_event(
        task_id,
        generation,
        "completed",
        {
            "event": "status_change",
            "task_id": task_id,
            "new_status": "completed",
            # A caller hint must not override the durable snapshot.
            "background_active": False,
        },
        terminal=True,
    )

    assert published is True
    event = broadcaster.broadcast.await_args.args[1]
    assert event["background_active"] is True


@pytest.mark.asyncio
async def test_status_publication_yields_to_active_termination_receipt(
    db_factory,
):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    rl = RalphLoop(db_factory, MagicMock(), broadcaster)
    async with db_factory() as db:
        task = Task(
            title="receipt owns terminal publication",
            description="do not publish across termination reconciliation",
            status="completed",
            completed_at=datetime.utcnow(),
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id
        generation = task_generation_fence(task)

    await _stage_active_worker_termination_receipt(db_factory, task_id)

    published = await rl._broadcast_generation_event(
        task_id,
        generation,
        "completed",
        {
            "event": "status_change",
            "task_id": task_id,
            "new_status": "completed",
        },
        terminal=True,
    )

    assert published is False
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_ralph_failure_keeps_owner_evidence_when_harness_cleanup_fails(
    db_factory,
):
    fence = _HarnessFenceStub(enter_error=RuntimeError("browser child survived"))
    instance_manager = MagicMock()
    instance_manager.processes = {}
    instance_manager.stop = AsyncMock()
    rl = RalphLoop(
        db_factory,
        instance_manager,
        MagicMock(),
        test_harness_service=fence,
    )
    started_at = datetime(2026, 8, 2, 3, 4, 5)
    async with db_factory() as db:
        instance = Instance(
            name="ralph-failure-fence-failure",
            status="running",
            pid=7002,
            started_at=started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="failure must wait for children",
            description="work",
            status="executing",
            instance_id=instance.id,
            turn_generation=4,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        await db.refresh(task)
        task_id = task.id
        instance_id = instance.id

    await rl._fail_unexpected_claim(
        instance_id,
        task,
        RuntimeError("provider bookkeeping failed"),
    )

    async with db_factory() as db:
        durable_task = await db.get(Task, task_id)
        durable_instance = await db.get(Instance, instance_id)
        assert durable_task.status == "executing"
        assert durable_task.completed_at is None
        assert durable_task.instance_id == instance_id
        assert durable_instance.status == "running"
        assert durable_instance.pid == 7002
        assert durable_instance.current_task_id == task_id
        assert durable_instance.started_at == started_at
    instance_manager.stop.assert_not_awaited()
    assert fence.events == ["enter"]


@pytest.mark.asyncio
async def test_unexpected_error_fails_claim_before_reaping_exact_process(
    db_factory,
):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    _install_settling_failed_stop(instance_manager, db_factory)
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )

    process = MagicMock(pid=2468, returncode=None)
    async with db_factory() as db:
        instance = Instance(
            name="ralph-error-worker",
            status="running",
            pid=process.pid,
            started_at=datetime(2026, 2, 3, 4, 5, 6),
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="unexpected launch error",
            description="work",
            provider="claude",
            status="executing",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        await db.refresh(task)
        instance_id = instance.id
        task_id = task.id

    instance_manager.processes = {instance_id: process}

    await rl._fail_unexpected_claim(
        instance_id,
        task,
        RuntimeError("consumer bookkeeping exploded"),
    )

    async with db_factory() as db:
        failed = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert failed.status == "failed"
        assert failed.instance_id == instance_id
        assert failed.pty_background_generation is None
        assert "consumer bookkeeping exploded" in failed.error_message
        assert instance.status == "error"
        assert instance.pid is None
        assert instance.current_task_id is None
        assert instance.started_at == datetime(2026, 2, 3, 4, 5, 6)
    instance_manager.stop.assert_awaited_once_with(
        instance_id,
        expected_task_id=task_id,
        expected_task_turn_generation=0,
        expected_pid=process.pid,
        expected_started_at=datetime(2026, 2, 3, 4, 5, 6),
        task_status="failed",
        task_error_message="Ralph loop failed: consumer bookkeeping exploded",
        terminal_consumer_timeout=30.0,
        consumer_cancel_timeout=10.0,
    )
    event = broadcaster.broadcast.await_args.args[1]
    assert event["new_status"] == "failed"
    assert event["reason"] == "ralph_internal_error"
    assert event["background_active"] is False


@pytest.mark.asyncio
async def test_unexpected_failure_clears_settled_marker_and_owner(
    db_factory,
):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    process = MagicMock(pid=6161, returncode=None)
    instance_manager = MagicMock()
    instance_manager.processes = {}
    _install_settling_failed_stop(instance_manager, db_factory)
    rl = RalphLoop(db_factory, instance_manager, broadcaster)

    async with db_factory() as db:
        instance = Instance(
            name="ralph-error-marker-handoff",
            status="running",
            pid=process.pid,
            started_at=datetime(2026, 4, 5, 7, 8, 9),
        )
        db.add(instance)
        await db.flush()
        stale_task = Task(
            title="error after PTY callback",
            description="work",
            provider="claude",
            status="executing",
            instance_id=instance.id,
        )
        db.add(stale_task)
        await db.flush()
        instance.current_task_id = stale_task.id
        await db.commit()
        await db.refresh(stale_task)
        task_id = stale_task.id
        instance_id = instance.id

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        current.pty_background_generation = "error-native-epoch"
        await db.commit()
    instance_manager.processes = {instance_id: process}

    await rl._fail_unexpected_claim(
        instance_id,
        stale_task,
        RuntimeError("late consumer failure"),
    )

    instance_manager.stop.assert_awaited_once_with(
        instance_id,
        expected_task_id=task_id,
        expected_task_turn_generation=0,
        expected_pid=process.pid,
        expected_started_at=datetime(2026, 4, 5, 7, 8, 9),
        task_status="failed",
        task_error_message="Ralph loop failed: late consumer failure",
        terminal_consumer_timeout=30.0,
        consumer_cancel_timeout=10.0,
    )
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert current.status == "failed"
        assert current.instance_id == instance_id
        assert current.pty_background_generation is None
        assert "late consumer failure" in current.error_message
        assert instance.status == "error"
        assert instance.current_task_id is None
        assert instance.pid is None
        assert instance.started_at == datetime(2026, 4, 5, 7, 8, 9)
    event = broadcaster.broadcast.await_args.args[1]
    assert event["new_status"] == "failed"
    assert event["background_active"] is False


@pytest.mark.asyncio
async def test_unexpected_error_with_unknown_persisted_process_fails_closed(
    db_factory,
):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock(
        side_effect=RuntimeError("websocket unavailable")
    )
    instance_manager = MagicMock()
    instance_manager.processes = {}
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )

    async with db_factory() as db:
        instance = Instance(
            name="ralph-unmanaged-error",
            status="running",
            pid=97531,
            started_at=datetime(2026, 2, 3, 4, 5, 6),
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="unknown process",
            description="work",
            status="in_progress",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        await db.refresh(task)
        instance_id = instance.id
        task_id = task.id

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        current.pty_background_generation = "unreaped-native-epoch"
        await db.commit()

    await rl._fail_unexpected_claim(
        instance_id,
        task,
        RuntimeError("spawn state was lost"),
    )

    async with db_factory() as db:
        failed = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert failed.status == "failed"
        assert failed.instance_id == instance_id
        assert (
            failed.pty_background_generation
            == "unreaped-native-epoch"
        )
        assert instance.status == "error"
        assert instance.pid == 97531
        assert instance.current_task_id == task_id
        assert instance.started_at == datetime(2026, 2, 3, 4, 5, 6)


@pytest.mark.asyncio
async def test_unexpected_error_cannot_fail_reassigned_generation(db_factory):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    instance_manager.processes = {}
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )

    async with db_factory() as db:
        old_instance = Instance(name="old-ralph-error-owner")
        new_instance = Instance(name="new-ralph-error-owner")
        db.add_all([old_instance, new_instance])
        await db.flush()
        task = Task(
            title="reassigned error",
            description="work",
            status="executing",
            instance_id=new_instance.id,
            error_message="new generation healthy",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        old_instance_id = old_instance.id
        new_instance_id = new_instance.id
        task_id = task.id

    await rl._fail_unexpected_claim(
        old_instance_id,
        task,
        RuntimeError("stale generation failed"),
    )

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "executing"
        assert current.instance_id == new_instance_id
        assert current.error_message == "new generation healthy"
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_unexpected_error_cannot_fail_same_slot_retry_aba(db_factory):
    """retry_count/start fences distinguish the same task and Instance ids."""

    from backend.services.task_queue import TaskQueue

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    instance_manager.processes = {}
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )
    old_started_at = datetime(2026, 2, 3, 4, 5, 6)

    async with db_factory() as db:
        instance = Instance(
            name="same-slot-error-aba",
            status="running",
            pid=1111,
            started_at=old_started_at,
        )
        db.add(instance)
        await db.flush()
        old_task = Task(
            title="same task retried",
            description="work",
            status="executing",
            retry_count=0,
            started_at=old_started_at,
            instance_id=instance.id,
        )
        db.add(old_task)
        await db.flush()
        instance.current_task_id = old_task.id
        await db.commit()
        await db.refresh(old_task)
        instance_id = instance.id
        task_id = old_task.id

    # Complete the old generation, retry it, and reclaim the exact same slot
    # before the old error handler runs.
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "failed"
        await db.commit()
        queue = TaskQueue(db)
        assert await queue.retry(task_id) is not None
        replacement = await queue.dequeue(instance_id=instance_id)
        assert replacement is not None
        instance = await db.get(Instance, instance_id)
        instance.status = "running"
        instance.pid = 2222
        instance.started_at = replacement.started_at
        instance.current_task_id = task_id
        await db.commit()

    await rl._fail_unexpected_claim(
        instance_id,
        old_task,
        RuntimeError("late failure from old turn"),
    )

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert current.status == "in_progress"
        assert current.retry_count == 1
        assert current.instance_id == instance_id
        assert instance.status == "running"
        assert instance.pid == 2222
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_unexpected_error_suppresses_failed_event_after_rapid_retry(
    db_factory,
):
    """Cleanup latency must not publish failed for a replacement generation."""

    from backend.services.task_queue import TaskQueue

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    process = MagicMock(pid=3333, returncode=None)
    instance_manager = MagicMock()
    instance_manager.processes = {}
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )

    async with db_factory() as db:
        instance = Instance(
            name="retry-during-error-cleanup",
            status="running",
            pid=process.pid,
            started_at=datetime(2026, 3, 4, 5, 6, 7),
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="retry during cleanup",
            description="work",
            provider="claude",
            status="executing",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        await db.refresh(task)
        instance_id = instance.id
        task_id = task.id

    instance_manager.processes = {instance_id: process}

    async def retry_before_cleanup_returns(
        stopped_instance_id,
        *,
        expected_task_id,
        expected_task_turn_generation,
        expected_pid,
        expected_started_at,
        task_status,
        task_error_message,
        terminal_consumer_timeout,
        consumer_cancel_timeout,
    ):
        assert stopped_instance_id == instance_id
        assert expected_task_id == task_id
        assert expected_task_turn_generation == 0
        assert expected_pid == process.pid
        assert expected_started_at == datetime(2026, 3, 4, 5, 6, 7)
        assert task_status == "failed"
        assert task_error_message == "Ralph loop failed: old generation failed"
        assert terminal_consumer_timeout == 30.0
        assert consumer_cancel_timeout == 10.0
        async with db_factory() as db:
            queue = TaskQueue(db)
            assert await queue.retry(task_id) is not None
            replacement = await queue.dequeue(instance_id=instance_id)
            assert replacement is not None
            instance = await db.get(Instance, instance_id)
            instance.status = "running"
            instance.pid = 4444
            instance.started_at = replacement.started_at
            instance.current_task_id = task_id
            await db.commit()
        # The exact stop lost its Task/Instance CAS to the replacement
        # generation and must report that it did not settle the old owner.
        return False

    instance_manager.stop = AsyncMock(
        side_effect=retry_before_cleanup_returns
    )

    await rl._fail_unexpected_claim(
        instance_id,
        task,
        RuntimeError("old generation failed"),
    )

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "in_progress"
        assert current.retry_count == 1
        assert current.instance_id == instance_id
        instance = await db.get(Instance, instance_id)
        assert instance.status == "running"
        assert instance.pid == 4444
        assert instance.current_task_id == task_id
    instance_manager.stop.assert_awaited_once()
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_plan_result_requires_active_status_and_same_instance_owner(db_factory):
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=MagicMock(),
        broadcaster=MagicMock(),
    )
    async with db_factory() as db:
        old_instance = Instance(name="old-plan-owner")
        new_instance = Instance(name="new-plan-owner")
        db.add_all([old_instance, new_instance])
        await db.flush()
        cancelled = Task(
            title="cancelled plan",
            description="plan",
            mode="plan",
            status="cancelled",
            instance_id=old_instance.id,
            plan_content="keep cancelled content",
        )
        reassigned = Task(
            title="reassigned plan",
            description="plan",
            mode="plan",
            status="executing",
            instance_id=new_instance.id,
            plan_content="keep new generation content",
        )
        db.add_all([cancelled, reassigned])
        await db.commit()
        await db.refresh(cancelled)
        await db.refresh(reassigned)
        old_instance_id = old_instance.id
        cancelled_id = cancelled.id
        reassigned_id = reassigned.id

    assert not await rl._store_plan_if_owned(
        old_instance_id,
        cancelled,
        "stale plan",
    )
    assert not await rl._store_plan_if_owned(
        old_instance_id,
        reassigned,
        "stale plan",
    )

    async with db_factory() as db:
        cancelled = await db.get(Task, cancelled_id)
        reassigned = await db.get(Task, reassigned_id)
        assert cancelled.status == "cancelled"
        assert cancelled.plan_content == "keep cancelled content"
        assert reassigned.status == "executing"
        assert reassigned.plan_content == "keep new generation content"


@pytest.mark.asyncio
async def test_plan_result_moves_owned_active_task_to_review(db_factory):
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=MagicMock(),
        broadcaster=MagicMock(),
    )
    async with db_factory() as db:
        instance = Instance(name="owned-plan-worker")
        db.add(instance)
        await db.flush()
        task = Task(
            title="owned plan",
            description="plan",
            mode="plan",
            status="in_progress",
            instance_id=instance.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        instance_id = instance.id
        task_id = task.id

    assert await rl._store_plan_if_owned(
        instance_id,
        task,
        "safe plan",
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "plan_review"
        assert task.plan_content == "safe plan"


@pytest.mark.asyncio
async def test_plan_result_rejects_same_slot_retry_aba(db_factory):
    from backend.services.task_queue import TaskQueue

    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=MagicMock(),
        broadcaster=MagicMock(),
    )
    async with db_factory() as db:
        instance = Instance(name="plan-aba-worker")
        db.add(instance)
        await db.flush()
        old_task = Task(
            title="old plan generation",
            description="plan",
            mode="plan",
            status="in_progress",
            retry_count=0,
            instance_id=instance.id,
        )
        db.add(old_task)
        await db.commit()
        await db.refresh(old_task)
        task_id = old_task.id
        instance_id = instance.id

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "failed"
        await db.commit()
        queue = TaskQueue(db)
        assert await queue.retry(task_id) is not None
        assert await queue.dequeue(instance_id=instance_id) is not None

    assert not await rl._store_plan_if_owned(
        instance_id,
        old_task,
        "late old plan",
    )
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "in_progress"
        assert current.retry_count == 1
        assert current.plan_content is None


@pytest.mark.asyncio
async def test_successful_stop_does_not_touch_immediate_same_instance_reclaim(
    db_factory,
):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    instance_manager.is_running.return_value = True
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )
    async with db_factory() as db:
        instance = Instance(
            name="rapidly-reused-ralph-worker",
            status="running",
            pid=1001,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="rapidly reclaimed task",
            description="work",
            status="in_progress",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        await db.refresh(task)
        instance_id = instance.id
        task_id = task.id

    async def stop_then_reclaim(
        stopped_instance_id,
        *,
        expected_task_id,
        expected_task_turn_generation,
        expected_pid,
        expected_started_at,
        task_status,
        task_error_message,
        terminal_consumer_timeout,
        consumer_cancel_timeout,
    ):
        assert stopped_instance_id == instance_id
        assert expected_task_id == task_id
        assert expected_task_turn_generation == 0
        assert expected_pid == 1001
        assert expected_started_at is None
        assert task_status == "failed"
        assert "outcome is uncertain" in task_error_message
        assert terminal_consumer_timeout == 30.0
        assert consumer_cancel_timeout == 10.0
        # Model InstanceManager.stop's successful release followed immediately
        # by a dispatcher claim of the same task on the same reusable slot.
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            instance = await db.get(Instance, instance_id)
            task.status = "executing"
            task.instance_id = instance_id
            task.error_message = "new generation"
            instance.status = "running"
            instance.pid = 2002
            instance.current_task_id = task_id
            await db.commit()
        return True

    instance_manager.stop = AsyncMock(side_effect=stop_then_reclaim)

    await rl._release_cancelled_claim(instance_id, task)

    instance_manager.stop.assert_awaited_once()
    instance_manager.is_running.assert_called_once_with(instance_id)
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "executing"
        assert task.instance_id == instance_id
        assert task.error_message == "new generation"
        assert instance.status == "running"
        assert instance.pid == 2002
        assert instance.current_task_id == task_id
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_stop_does_not_overwrite_new_same_task_instance_generation(
    db_factory,
):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    instance_manager.is_running.return_value = True
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )
    old_started_at = datetime(2026, 1, 1, 1, 0, 0)
    new_started_at = datetime(2026, 1, 1, 1, 0, 1)
    async with db_factory() as db:
        instance = Instance(
            name="failed-stop-reused-worker",
            status="running",
            pid=3456,
            started_at=old_started_at,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="same task retried immediately",
            description="work",
            status="in_progress",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        await db.refresh(task)
        instance_id = instance.id
        task_id = task.id

    async def stop_fails_after_reclaim(
        stopped_instance_id,
        *,
        expected_task_id,
        expected_task_turn_generation,
        expected_pid,
        expected_started_at,
        task_status,
        task_error_message,
        terminal_consumer_timeout,
        consumer_cancel_timeout,
    ):
        assert stopped_instance_id == instance_id
        assert expected_task_id == task_id
        assert expected_task_turn_generation == 0
        assert expected_pid == 3456
        assert expected_started_at == old_started_at
        assert task_status == "failed"
        assert "outcome is uncertain" in task_error_message
        assert terminal_consumer_timeout == 30.0
        assert consumer_cancel_timeout == 10.0
        # The old stop unwinds, then an immediate retry reuses the same task,
        # slot, status and even PID. started_at is the remaining generation
        # fence that must prevent the old failure recorder from matching.
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            instance = await db.get(Instance, instance_id)
            task.status = "executing"
            task.instance_id = instance_id
            task.error_message = "new generation is healthy"
            instance.status = "running"
            instance.pid = 3456
            instance.current_task_id = task_id
            instance.started_at = new_started_at
            await db.commit()
        raise RuntimeError("old generation cleanup failed")

    instance_manager.stop = AsyncMock(side_effect=stop_fails_after_reclaim)

    await rl._release_cancelled_claim(instance_id, task)

    instance_manager.stop.assert_awaited_once()
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "executing"
        assert task.instance_id == instance_id
        assert task.error_message == "new generation is healthy"
        assert instance.status == "running"
        assert instance.pid == 3456
        assert instance.current_task_id == task_id
        assert instance.started_at == new_started_at
    broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_cleanup_failure_never_requeues_possibly_live_process(
    db_factory,
):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    instance_manager.is_running.return_value = True
    instance_manager.stop = AsyncMock(
        side_effect=RuntimeError("process group survived SIGKILL")
    )
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )
    async with db_factory() as db:
        instance = Instance(
            name="unreaped-ralph-worker",
            status="running",
            pid=43210,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="possibly still running",
            description="work",
            status="in_progress",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        await db.refresh(task)
        instance_id = instance.id
        task_id = task.id

    await rl._release_cancelled_claim(instance_id, task)

    instance_manager.stop.assert_awaited_once_with(
        instance_id,
        expected_task_id=task_id,
        expected_task_turn_generation=0,
        expected_pid=43210,
        expected_started_at=None,
        task_status="failed",
        task_error_message=(
            "Ralph loop stopped after provider admission; the exact turn "
            "outcome is uncertain"
        ),
        terminal_consumer_timeout=30.0,
        consumer_cancel_timeout=10.0,
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "failed"
        assert task.instance_id == instance_id
        assert "cleanup could not be confirmed" in task.error_message
        assert instance.status == "error"
        assert instance.pid == 43210
        assert instance.current_task_id == task_id
    event = broadcaster.broadcast.await_args.args[1]
    assert event["new_status"] == "failed"
    assert event["reason"] == "ralph_stop_cleanup_failed"


@pytest.mark.asyncio
async def test_cancel_with_persisted_owner_but_no_managed_generation_fails_closed(
    db_factory,
):
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    instance_manager = MagicMock()
    instance_manager.is_running.return_value = False
    instance_manager.stop = AsyncMock()
    rl = RalphLoop(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )
    async with db_factory() as db:
        instance = Instance(
            name="unknown-ralph-generation",
            status="running",
            pid=8765,
        )
        db.add(instance)
        await db.flush()
        task = Task(
            title="unknown process owner",
            description="work",
            status="executing",
            instance_id=instance.id,
        )
        db.add(task)
        await db.flush()
        instance.current_task_id = task.id
        await db.commit()
        await db.refresh(task)
        instance_id = instance.id
        task_id = task.id

    await rl._release_cancelled_claim(instance_id, task)

    instance_manager.stop.assert_not_awaited()
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "failed"
        assert task.instance_id == instance_id
        assert instance.status == "error"
        assert instance.pid == 8765
        assert instance.current_task_id == task_id
