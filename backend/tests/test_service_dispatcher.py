"""Tests for GlobalDispatcher — task dispatch and lifecycle management."""

import asyncio
import hashlib
import json
import os
import signal
import sys
import pytest
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.services.dispatcher import (
    GlobalDispatcher,
    QueuedMessage,
    QueuedMessagePrelaunchError,
    _prepend_task_artifact_policy,
)
from backend.services.deployment_start_guard import (
    DeploymentTaskStartBlocked,
)
from backend.services.task_skill_overrides import (
    TEMP_SKILLS_GENERATION_KEY,
)
from backend.services.task_artifact_contract import (
    TASK_ARTIFACT_POLICY_TAG,
)
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.plan import (
    Plan,
    PlanApplication,
    PlanApplicationReceipt,
    PlanVersion,
)
from backend.models.plan_agent import PlanAgentRun
from backend.models.task import Task


def _make_dispatcher(db_factory):
    """Create a GlobalDispatcher with mocked dependencies."""
    instance_manager = MagicMock()
    instance_manager.launch = AsyncMock(return_value=12345)
    instance_manager.kill_process_generation = AsyncMock(return_value=True)
    instance_manager.stop = AsyncMock(return_value=True)
    instance_manager.processes = {}
    instance_manager._tasks = {}
    lifecycle_locks: dict[int, asyncio.Lock] = {}
    instance_manager._instance_lifecycle_lock = MagicMock(
        side_effect=lambda instance_id: lifecycle_locks.setdefault(
            instance_id, asyncio.Lock()
        )
    )
    instance_manager.is_running = MagicMock(
        side_effect=lambda instance_id: (
            (
                (process := instance_manager.processes.get(instance_id)) is not None
                and getattr(process, "returncode", None) is None
            )
            or (
                (consumer := instance_manager._tasks.get(instance_id)) is not None
                and not consumer.done()
            )
        )
    )
    instance_manager.wait_for_output_consumer = AsyncMock()
    # Model the real InstanceManager interface used by failure classification.
    instance_manager.pty_mode_enabled = False
    instance_manager.is_pty_managed_turn = MagicMock(return_value=False)
    instance_manager.effective_exit_code = MagicMock(
        side_effect=lambda _instance_id, process: (
            process.returncode
            if isinstance(getattr(process, "returncode", None), int)
            else -1
        )
    )
    instance_manager.transient_error_seen = MagicMock(return_value=False)
    instance_manager.get_last_stderr = MagicMock(return_value="")
    instance_manager.get_recent_log_contents = AsyncMock(return_value=[])
    # PTY proactive pool switch path (dispatcher._process_task_lifecycle)
    instance_manager.pty_rate_limit_seen = MagicMock(return_value=False)
    instance_manager._try_proactive_pool_switch = AsyncMock()

    @asynccontextmanager
    async def runtime_admission(
        _provider,
        _home,
        _model,
        *,
        service_tier="default",
    ):
        yield None

    @asynccontextmanager
    async def codex_exec_admission(home):
        yield home

    @asynccontextmanager
    async def codex_app_server_admission(home):
        yield home

    instance_manager._cloudrouter_runtime_admission = runtime_admission
    instance_manager.codex_home_exec_guard = codex_exec_admission
    instance_manager.codex_home_app_server_guard = codex_app_server_admission
    instance_manager._ensure_codex_app_server_registry = MagicMock(
        return_value=MagicMock(name="codex-app-server-registry"),
    )
    instance_manager._pty_rate_limit_seen = set()

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()

    dispatcher = GlobalDispatcher(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )
    return dispatcher


async def _run_claimed_lifecycle(
    dispatcher: GlobalDispatcher,
    db_factory,
    instance_id: int,
    task: Task,
):
    """Mirror TaskQueue.dequeue's durable owner claim for lifecycle tests."""
    from sqlalchemy import update

    async with db_factory() as db:
        claimed = await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(status="in_progress", instance_id=instance_id)
        )
        assert claimed.rowcount == 1
        await db.commit()
    task.status = "in_progress"
    task.instance_id = instance_id
    await dispatcher._run_task_lifecycle(instance_id, task)


async def _claim_mode_lifecycle(
    db_factory,
    instance_id: int,
    task: Task,
):
    """Seed the durable dequeue owner before directly testing a mode handler."""
    from sqlalchemy import update

    async with db_factory() as db:
        claimed = await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(status="in_progress", instance_id=instance_id)
        )
        assert claimed.rowcount == 1
        await db.commit()
    task.status = "in_progress"
    task.instance_id = instance_id


@pytest.mark.asyncio
async def test_status_not_running(db_factory):
    """status() returns running=False before start."""
    d = _make_dispatcher(db_factory)
    s = d.status()
    assert s["running"] is False
    assert s["active_tasks"] == {}


@pytest.mark.asyncio
async def test_claim_plan_run_returns_exact_persisted_generation(db_factory):
    """The lifecycle fence must match the generation committed by the claim."""
    dispatcher = _make_dispatcher(db_factory)

    async with db_factory() as db:
        instance = Instance(name="plan-worker", status="idle")
        plan = Plan(
            title="Generation fence",
            initial_request="Plan this",
            pipeline_config={},
        )
        db.add_all([instance, plan])
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            run_type="initial",
            status="queued",
            generation=0,
            pipeline_config={},
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        await db.commit()
        instance_id = instance.id
        run_id = run.id

        claimed = await dispatcher._claim_plan_run(db, instance=instance)

    assert claimed == (run_id, 1)
    async with db_factory() as db:
        persisted_run = await db.get(PlanAgentRun, run_id)
        persisted_owner = await db.get(Instance, instance_id)
        assert persisted_run.status == "running"
        assert persisted_run.generation == claimed[1]
        assert persisted_run.instance_id == instance_id
        assert persisted_owner.status == "running"
        assert persisted_owner.current_plan_run_id == run_id


@pytest.mark.asyncio
async def test_recover_orphaned_plan_run_without_disturbing_reused_instance(
    db_factory,
):
    """A restart requeues a lost Run even if its old slot now owns a Task."""
    dispatcher = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(
            title="current owner", description="keep running", status="executing"
        )
        instance = Instance(name="reused-worker", status="running", pid=4321)
        plan = Plan(
            title="Recover me",
            initial_request="Plan this",
            pipeline_config={},
        )
        db.add_all([task, instance, plan])
        await db.flush()
        task.instance_id = instance.id
        instance.current_task_id = task.id
        run = PlanAgentRun(
            plan_id=plan.id,
            run_type="initial",
            status="running",
            current_stage="planner",
            generation=1,
            instance_id=instance.id,
            pipeline_config={},
            last_execution_started_at=datetime.utcnow(),
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        await db.commit()
        task_id = task.id
        instance_id = instance.id
        plan_id = plan.id
        run_id = run.id

    await dispatcher._recover_versioned_plan_runs()

    async with db_factory() as db:
        recovered = await db.get(PlanAgentRun, run_id)
        owner = await db.get(Instance, instance_id)
        plan = await db.get(Plan, plan_id)
        assert recovered.status == "queued"
        assert recovered.generation == 2
        assert recovered.instance_id is None
        assert recovered.last_execution_started_at is None
        assert plan.active_run_id == run_id
        assert owner.status == "running"
        assert owner.pid == 4321
        assert owner.current_task_id == task_id
        assert owner.current_plan_run_id is None


@pytest.mark.asyncio
async def test_pause_dispatching_does_not_stop_dispatcher(db_factory):
    d = _make_dispatcher(db_factory)
    d._running = True

    await d.pause_dispatching()

    assert d.status()["running"] is True
    assert d.status()["paused"] is True

    d.resume_dispatching()
    assert d.status()["paused"] is False


@pytest.mark.asyncio
async def test_task_start_guard_holds_repo_deployment_fence(db_factory):
    d = _make_dispatcher(db_factory)
    events = []

    @contextmanager
    def fence():
        events.append("enter")
        try:
            yield
        finally:
            events.append("exit")

    d.deployment_task_start_fence = fence

    async with d.task_start_guard():
        events.append("claim")

    assert events == ["enter", "claim", "exit"]


@pytest.mark.asyncio
async def test_task_start_guard_maps_repo_fence_to_paused(db_factory):
    from backend.services.dispatcher import TaskStartPausedError

    d = _make_dispatcher(db_factory)

    @contextmanager
    def blocked_fence():
        raise DeploymentTaskStartBlocked("deployment active")
        yield

    d.deployment_task_start_fence = blocked_fence

    with pytest.raises(TaskStartPausedError, match="deployment active"):
        async with d.task_start_guard():
            pass


@pytest.mark.asyncio
async def test_old_worker_callback_cannot_remove_replacement(db_factory):
    d = _make_dispatcher(db_factory)
    old = asyncio.create_task(asyncio.sleep(0))
    replacement = asyncio.create_task(asyncio.sleep(0))
    key = "worker-42"
    d._running_tasks[key] = replacement

    d._remove_running_task_if_same(key, old)
    assert d._running_tasks[key] is replacement

    await asyncio.gather(old, replacement)
    d._remove_running_task_if_same(key, replacement)
    assert key not in d._running_tasks


@pytest.mark.asyncio
async def test_start_sets_running(db_factory):
    """start() sets _running=True and creates dispatch task."""
    d = _make_dispatcher(db_factory)

    # Patch _dispatch_loop to avoid actual polling
    async def fake_loop():
        await asyncio.sleep(999)

    d._dispatch_loop = fake_loop

    await d.start()
    assert d.is_running is True
    assert d._dispatch_task is not None

    # Cleanup
    await d.stop()


@pytest.mark.asyncio
async def test_start_idempotent(db_factory):
    """Calling start() twice doesn't create a second dispatch task."""
    d = _make_dispatcher(db_factory)

    async def fake_loop():
        await asyncio.sleep(999)

    d._dispatch_loop = fake_loop

    await d.start()
    first_task = d._dispatch_task
    await d.start()
    assert d._dispatch_task is first_task

    await d.stop()


@pytest.mark.asyncio
async def test_stop(db_factory):
    """stop() cancels dispatch task and sets _running=False."""
    d = _make_dispatcher(db_factory)

    async def fake_loop():
        await asyncio.sleep(999)

    d._dispatch_loop = fake_loop

    await d.start()
    assert d.is_running is True

    await d.stop()
    assert d.is_running is False
    assert d._dispatch_task.done() or d._dispatch_task.cancelled()


@pytest.mark.asyncio
async def test_ensure_instances_creates_workers(db_factory):
    """_ensure_instances creates workers up to max_concurrent_instances."""
    d = _make_dispatcher(db_factory)

    with (
        patch("backend.services.dispatcher.settings") as mock_settings,
        patch("backend.services.dispatcher._provider_available", return_value=True),
    ):
        mock_settings.max_concurrent_instances = 3
        mock_settings.default_provider = "claude"
        mock_settings.default_model = "sonnet"
        await d._ensure_instances()

    async with db_factory() as db:
        result = await db.execute(select(Instance))
        instances = list(result.scalars().all())
    assert len(instances) == 3
    assert instances[0].name == "worker-1"
    assert instances[2].name == "worker-3"


@pytest.mark.asyncio
async def test_ensure_instances_skips_if_enough(db_factory):
    """_ensure_instances does nothing if enough instances exist."""
    d = _make_dispatcher(db_factory)

    # Pre-create 2 instances
    async with db_factory() as db:
        db.add(Instance(name="w1"))
        db.add(Instance(name="w2"))
        await db.commit()

    with patch("backend.services.dispatcher.settings") as mock_settings:
        mock_settings.max_concurrent_instances = 2
        await d._ensure_instances()

    async with db_factory() as db:
        from sqlalchemy import select

        result = await db.execute(select(Instance))
        instances = list(result.scalars().all())
    assert len(instances) == 2


@pytest.mark.asyncio
async def test_ensure_instances_ignores_terminal_workers(db_factory):
    """Startup replenishes workers even when terminal rows exceed the cap."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        for i in range(9):
            status = "error" if i < 8 else "stopped"
            db.add(Instance(name=f"old-worker-{i + 1}", status=status))
        await db.commit()

    with patch("backend.services.dispatcher.settings") as mock_settings:
        mock_settings.max_concurrent_instances = 8
        await d._ensure_instances()

    async with db_factory() as db:
        result = await db.execute(select(Instance))
        instances = list(result.scalars().all())
    assert sum(1 for i in instances if i.status == "idle") == 8
    assert sum(1 for i in instances if i.status in ("idle", "running")) == 8


@pytest.mark.asyncio
async def test_lifecycle_success(db_factory):
    """_run_task_lifecycle completes task successfully."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="worker-1")
        db.add(inst)
        task = Task(title="test", description="do something", target_repo="/repo")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"

    assert d.broadcaster.broadcast.await_count >= 2


@pytest.mark.asyncio
async def test_lifecycle_defers_pr_completion_until_background_epoch_finishes(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    d._handle_pr_review_completion = AsyncMock()

    async with db_factory() as db:
        inst = Instance(name="background-pr-worker")
        task = Task(
            title="background PR review",
            description="review",
            target_repo="/repo",
            metadata_={"pr_review_id": 77},
        )
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id
        task_obj = task

    async def wait_and_arm_background():
        async with db_factory() as db:
            current = await db.get(Task, task_id)
            current.pty_background_generation = "exact-background-epoch"
            await db.commit()
        return 0

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(side_effect=wait_and_arm_background)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "completed"
        assert current.pty_background_generation == ("exact-background-epoch")
    d._handle_pr_review_completion.assert_not_awaited()

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        current.pty_background_generation = None
        await db.commit()
    await d.instance_manager.pty_background_completion_handler(task_id)
    d._handle_pr_review_completion.assert_awaited_once()
    assert d._handle_pr_review_completion.await_args.args[0].id == task_id


@pytest.mark.asyncio
async def test_lifecycle_does_not_complete_semantic_api_failure_with_zero_os_exit(
    db_factory,
):
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="semantic-failure-worker")
        task = Task(
            title="semantic API failure",
            description="call provider",
            target_repo="/repo",
            max_retries=0,
        )
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    process = MagicMock(returncode=0, wait=AsyncMock(return_value=0))
    d.instance_manager.processes = {inst_id: process}
    d.instance_manager.effective_exit_code = MagicMock(return_value=1)

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    async with db_factory() as db:
        current = await db.get(Task, task_obj.id)

    assert current.status == "failed"
    d.instance_manager.effective_exit_code.assert_called_with(inst_id, process)


@pytest.mark.asyncio
async def test_stale_lifecycle_cannot_claim_same_task_and_instance_after_retry(
    db_factory,
):
    """retry_count/start time fence a same-task, same-slot lifecycle ABA."""

    from datetime import datetime, timedelta

    d = _make_dispatcher(db_factory)
    old_started_at = datetime.utcnow() - timedelta(minutes=2)
    replacement_started_at = datetime.utcnow()

    async with db_factory() as db:
        instance = Instance(name="same-slot-retry")
        task = Task(
            title="old generation",
            description="must not launch",
            target_repo="/repo",
            status="in_progress",
            retry_count=0,
            started_at=old_started_at,
        )
        db.add_all([instance, task])
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        await db.refresh(task)
        stale_task = task
        task_id = task.id
        instance_id = instance.id

    async with db_factory() as db:
        replaced = await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="in_progress",
                retry_count=1,
                instance_id=instance_id,
                started_at=replacement_started_at,
                completed_at=None,
            )
        )
        assert replaced.rowcount == 1
        await db.commit()

    stale_generation = d._task_lifecycle_generation(stale_task)
    assert await d._task_claim_is_active(stale_generation) is False
    await d._run_task_lifecycle(instance_id, stale_task)

    d.instance_manager.launch.assert_not_awaited()
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "in_progress"
        assert current.retry_count == 1
        assert current.instance_id == instance_id
        assert current.started_at == replacement_started_at


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pty_enabled", "exact_pty_turn", "expected_switches"),
    [
        (False, False, 0),
        (True, True, 1),
        (False, True, 1),
        (True, False, 0),
    ],
)
async def test_lifecycle_proactive_switch_uses_exact_pty_turn(
    db_factory,
    pty_enabled,
    exact_pty_turn,
    expected_switches,
):
    """Runtime toggles affect new launches, not an already-owned PTY turn."""
    d = _make_dispatcher(db_factory)
    d.instance_manager.pty_mode_enabled = pty_enabled
    d.instance_manager.is_pty_managed_turn.return_value = exact_pty_turn
    d.instance_manager.pty_rate_limit_seen.return_value = True
    d.instance_manager.pty_rate_limit_info = MagicMock(
        return_value={
            "status": "allowed_warning",
            "rateLimitType": "five_hour",
            "utilization": 0.95,
        }
    )
    d.instance_manager.clear_pty_rate_limit = MagicMock()

    async with db_factory() as db:
        inst = Instance(name=f"quota-pty-{pty_enabled}")
        task = Task(title="quota gate", description="done", target_repo="/repo")
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    process = MagicMock(returncode=0, wait=AsyncMock(return_value=0))
    d.instance_manager.processes = {inst.id: process}

    await _run_claimed_lifecycle(d, db_factory, inst.id, task)

    assert (
        d.instance_manager._try_proactive_pool_switch.await_count == expected_switches
    )
    assert d.instance_manager.clear_pty_rate_limit.call_count == expected_switches
    if not exact_pty_turn:
        d.instance_manager.pty_rate_limit_seen.assert_not_called()


@pytest.mark.asyncio
async def test_lifecycle_failure_retry(db_factory):
    """Failed task with retries left goes back to pending."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="worker-1")
        db.add(inst)
        task = Task(
            title="retry-test",
            description="fail once",
            target_repo="/repo",
            max_retries=3,
            retry_count=0,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 1
    mock_proc.wait = AsyncMock(return_value=1)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "pending"
        assert t.retry_count == 1


@pytest.mark.asyncio
async def test_lifecycle_codex_context_error_compacts_before_retry(db_factory):
    """Fresh/mode turns recognize Codex's structured overflow code."""

    d = _make_dispatcher(db_factory)
    d._collect_failure_output = AsyncMock(
        return_value=(
            '{"type":"turn.failed","error":{"message":"request failed",'
            '"codexErrorInfo":"contextWindowExceeded"}}'
        )
    )
    d._compact_session = AsyncMock(return_value="preserved task context")

    async with db_factory() as db:
        inst = Instance(name="codex-context-worker")
        task = Task(
            title="codex context retry",
            description="continue implementation",
            target_repo="/repo",
            provider="codex",
            session_id="codex-thread-1",
        )
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    process = MagicMock(returncode=1, wait=AsyncMock(return_value=1))
    d.instance_manager.processes = {inst.id: process}

    await _run_claimed_lifecycle(d, db_factory, inst.id, task)

    d._compact_session.assert_awaited_once()
    async with db_factory() as db:
        current = await db.get(Task, task.id)
        assert current.status == "pending"
        assert current.session_id is None
        assert current.context_window_usage is None
        assert "[Context compacted]" in current.description
        assert "preserved task context" in current.description
        assert "近期状态和结论优先" in current.description
        assert "continue implementation" not in current.description


@pytest.mark.asyncio
async def test_lifecycle_failure_max_retries(db_factory):
    """Task at max retries is marked failed."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="worker-1")
        db.add(inst)
        task = Task(
            title="max-retry",
            description="always fail",
            target_repo="/repo",
            max_retries=2,
            retry_count=2,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 1
    mock_proc.wait = AsyncMock(return_value=1)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "failed"


@pytest.mark.asyncio
async def test_pr_fix_nonzero_exit_exhaustion_runs_exact_failure_consumer(
    db_factory,
):
    """A normal process failure bypasses the lifecycle exception branch."""

    d = _make_dispatcher(db_factory)
    d._handle_pr_review_failure = AsyncMock()

    async with db_factory() as db:
        inst = Instance(name="pr-fix-failure-worker")
        task = Task(
            title="PR fix exhausts retries",
            description="generate bounded patch",
            max_retries=0,
            retry_count=0,
            # Exercise the durable Manager marker after presentation-tag
            # removal rather than relying on the Worker-only tag fallback.
            tags=[],
            metadata_={"pr_finding_action_id": 501},
        )
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        task_obj = task

    process = MagicMock(returncode=1, wait=AsyncMock(return_value=1))
    d.instance_manager.processes = {inst.id: process}

    await _run_claimed_lifecycle(d, db_factory, inst.id, task_obj)

    async with db_factory() as db:
        current = await db.get(Task, task_obj.id)
        assert current.status == "failed"
        assert current.retry_count == 0
        assert current.error_message == "Exit code: 1"
        expected_completed_at = current.completed_at

    d._handle_pr_review_failure.assert_awaited_once()
    failed_task, reason = d._handle_pr_review_failure.await_args.args
    assert failed_task.id == task_obj.id
    assert failed_task.status == "failed"
    assert failed_task.retry_count == 0
    assert failed_task.completed_at == expected_completed_at
    assert reason == "Exit code: 1"


@pytest.mark.asyncio
async def test_pr_fix_retry_does_not_run_failure_consumer(db_factory):
    d = _make_dispatcher(db_factory)
    d._handle_pr_review_failure = AsyncMock()

    async with db_factory() as db:
        inst = Instance(name="pr-fix-retry-worker")
        task = Task(
            title="PR fix retry remains pending",
            description="generate bounded patch",
            max_retries=1,
            retry_count=0,
            tags=["pr-review-fix"],
        )
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)

    process = MagicMock(returncode=1, wait=AsyncMock(return_value=1))
    d.instance_manager.processes = {inst.id: process}

    await _run_claimed_lifecycle(d, db_factory, inst.id, task)

    async with db_factory() as db:
        current = await db.get(Task, task.id)
        assert current.status == "pending"
        assert current.retry_count == 1
    d._handle_pr_review_failure.assert_not_awaited()


@pytest.mark.asyncio
async def test_lifecycle_exception(db_factory):
    """Unexpected exception marks task as failed."""
    d = _make_dispatcher(db_factory)
    d.instance_manager.launch = AsyncMock(side_effect=Exception("unexpected boom"))

    async with db_factory() as db:
        inst = Instance(name="worker-1")
        db.add(inst)
        task = Task(title="boom", description="test", target_repo="/repo")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "failed"
        assert t.error_message is not None


@pytest.mark.asyncio
async def test_fresh_codex_all_accounts_cooling_defers_without_retry_budget(
    db_factory,
    tmp_path,
):
    """Pool backpressure keeps a fresh task pending instead of hard-failing."""
    import json
    import time

    from backend.services.codex_pool import CodexPool

    config_path = tmp_path / "codex-accounts.json"
    homes = [tmp_path / "codex-a", tmp_path / "codex-b"]
    config_path.write_text(
        json.dumps(
            {
                "accounts": [
                    {"id": "codex-a", "codex_home": str(homes[0]), "enabled": True},
                    {"id": "codex-b", "codex_home": str(homes[1]), "enabled": True},
                ]
            }
        )
    )

    d = _make_dispatcher(db_factory)
    d.codex_pool = CodexPool(config_path=config_path, cooldown_seconds=60)
    future = time.time() + 60
    d.codex_pool._cooldowns = {"codex-a": future, "codex-b": future}

    async with db_factory() as db:
        inst = Instance(name="codex-cooldown-worker")
        task = Task(
            title="fresh-codex",
            description="do work",
            target_repo="/repo",
            provider="codex",
            max_retries=0,
        )
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id
        task_obj = task

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    async with db_factory() as db:
        deferred = await db.get(Task, task_id)
        assert deferred.status == "pending"
        assert deferred.retry_count == 0
        assert deferred.instance_id is None
        assert deferred.started_at is None
        assert deferred.completed_at is None
        assert "no available account" in deferred.error_message
    assert task_id in d._account_routing_not_before
    assert d._account_routing_not_before[task_id] > time.monotonic()
    d.instance_manager.launch.assert_not_awaited()


@pytest.mark.asyncio
async def test_fresh_codex_maintenance_busy_defers_without_retry_budget(
    db_factory,
):
    """A login-maintenance race is scheduling backpressure, not task failure."""
    from backend.services.codex_app_server import CodexAppServerBusyError

    d = _make_dispatcher(db_factory)
    d._resolve_resume_config_dir = AsyncMock(return_value="/tmp/codex-a")
    d.instance_manager.launch = AsyncMock(
        side_effect=CodexAppServerBusyError("account is under maintenance")
    )

    async with db_factory() as db:
        inst = Instance(name="codex-maintenance-worker")
        task = Task(
            title="fresh-codex-maintenance",
            description="do work",
            target_repo="/repo",
            provider="codex",
            max_retries=0,
        )
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id
        task_obj = task

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    async with db_factory() as db:
        deferred = await db.get(Task, task_id)
        assert deferred.status == "pending"
        assert deferred.retry_count == 0
        assert deferred.started_at is None
        assert "under maintenance" in deferred.error_message
    assert task_id in d._account_routing_not_before


@pytest.mark.asyncio
async def test_permanent_codex_routing_error_still_fails_fresh_task(db_factory):
    """Ambiguous/unsafe ownership is not retried forever as if it were cooldown."""
    from backend.services.dispatcher import CodexAccountRoutingError

    d = _make_dispatcher(db_factory)
    d._resolve_resume_config_dir = AsyncMock(
        side_effect=CodexAccountRoutingError(
            "ambiguous rollout ownership",
            permanent=True,
        )
    )

    async with db_factory() as db:
        inst = Instance(name="codex-permanent-routing-worker")
        task = Task(
            title="unsafe-codex",
            description="do work",
            target_repo="/repo",
            provider="codex",
        )
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_id = task.id
        task_obj = task

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    async with db_factory() as db:
        failed = await db.get(Task, task_id)
        assert failed.status == "failed"
        assert "ambiguous rollout ownership" in failed.error_message
    assert task_id not in d._account_routing_not_before


@pytest.mark.asyncio
async def test_plan_phase(db_factory):
    """Plan-mode task runs plan phase and sets plan_review status."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="worker-1")
        db.add(inst)
        task = Task(
            title="plan-task",
            description="plan this",
            target_repo="/repo",
            mode="plan",
            plan_approved=False,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    plan_result = SimpleNamespace(
        plan_content="read-only plan",
        verdict="approve",
        feedback="",
        review_exhausted=False,
        run_id=11,
    )
    with patch(
        "backend.services.plan_agent_runner.PlanAgentRunner.run",
        AsyncMock(return_value=plan_result),
    ):
        await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "plan_review"
        assert t.plan_content == "read-only plan"


# === Prompt construction with image_paths ===


@pytest.mark.asyncio
async def test_lifecycle_prompt_includes_images(db_factory):
    """When task.metadata_ has image_paths, launch prompt includes image list."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="worker-img")
        db.add(inst)
        task = Task(
            title="img-task",
            description="do the thing",
            target_repo="/repo",
            metadata_={"image_paths": ["/uploads/a.png", "/uploads/b.jpg"]},
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    # Check that launch was called with a prompt containing the image paths
    call_kwargs = d.instance_manager.launch.call_args
    prompt_used = call_kwargs.kwargs.get("prompt") or call_kwargs[1].get("prompt")
    assert "/uploads/a.png" in prompt_used
    assert "/uploads/b.jpg" in prompt_used
    assert "Read" in prompt_used


@pytest.mark.asyncio
async def test_lifecycle_prompt_no_images(db_factory):
    """When task has no image_paths, launch prompt uses standard format without image section."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="worker-noimg")
        db.add(inst)
        task = Task(
            title="no-img-task",
            description="plain task",
            target_repo="/repo",
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    call_kwargs = d.instance_manager.launch.call_args
    prompt_used = call_kwargs.kwargs.get("prompt") or call_kwargs[1].get("prompt")
    assert "plain task" in prompt_used
    # Should not contain image-related instruction
    assert "参考图片" not in prompt_used
    assert "Read 工具" not in prompt_used


# ===  Loop lifecycle tests ===


def _write_signal(
    signal_path,
    action: str,
    reason: str = "",
    progress: str | None = None,
    summary: str | None = None,
    plan: str | None = None,
):
    """Helper: write a signal file synchronously."""
    import json
    from pathlib import Path

    data = {"action": action, "reason": reason}
    if progress:
        data["progress"] = progress
    if summary:
        data["summary"] = summary
    if plan:
        data["plan"] = plan
    Path(signal_path).write_text(json.dumps(data), encoding="utf-8")


def _make_loop_task(db, tmp_path, max_iterations: int = 50) -> "Task":
    """Create a loop task pointing at tmp_path as target_repo."""
    return Task(
        title="loop-test",
        mode="loop",
        todo_file_path="TODO.md",
        target_repo=str(tmp_path),
        max_iterations=max_iterations,
    )


@pytest.mark.asyncio
async def test_loop_done_after_one_iteration(db_factory, tmp_path):
    """Loop task marked completed when Claude signals 'done' on first iteration."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="loop-worker-1")
        db.add(inst)
        task = _make_loop_task(db, tmp_path)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)

    # Write signal file when launch is called
    async def launch_and_write(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        _write_signal(signal_path, "done", "all done", "1/1")
        return 12345

    d.instance_manager.launch = AsyncMock(side_effect=launch_and_write)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"
        assert t.loop_progress == "1/1"


@pytest.mark.asyncio
async def test_loop_iteration_passes_pool_config_dir(db_factory, tmp_path):
    """Regression (#770): loop launches must resolve a pool account via
    _resolve_resume_config_dir and pass it as config_dir.

    Before the fix the loop omitted config_dir entirely, so the child process
    inherited the hardcoded systemd CLAUDE_CONFIG_DIR — the pool was never
    consulted, cooled-down accounts were never avoided, and a PTY resume could
    land on the wrong account ("No conversation found").
    """
    d = _make_dispatcher(db_factory)
    d._resolve_resume_config_dir = AsyncMock(return_value="/pool/acc-7")

    async with db_factory() as db:
        inst = Instance(name="loop-pool-worker")
        db.add(inst)
        task = _make_loop_task(db, tmp_path)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)

    async def launch_and_write(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        _write_signal(signal_path, "done", "all done", "1/1")
        return 12345

    d.instance_manager.launch = AsyncMock(side_effect=launch_and_write)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    # Pool was consulted, and its choice flowed into the launch.
    d._resolve_resume_config_dir.assert_awaited()
    # iteration 0 has no session to resume yet → resolver called with None
    assert d._resolve_resume_config_dir.await_args.args == (None, "claude")
    assert d.instance_manager.launch.await_args.kwargs["config_dir"] == "/pool/acc-7"


@pytest.mark.asyncio
async def test_loop_continue_then_done(db_factory, tmp_path):
    """Loop iterates twice: first 'continue', then 'done'."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="loop-worker-2")
        db.add(inst)
        task = _make_loop_task(db, tmp_path)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"
    call_count = {"n": 0}

    async def launch_and_write(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        call_count["n"] += 1
        if call_count["n"] == 1:
            _write_signal(signal_path, "continue", "one more", "1/2")
        else:
            _write_signal(signal_path, "done", "finished", "2/2")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_and_write)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    assert call_count["n"] == 2
    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"
        assert t.loop_progress == "2/2"


@pytest.mark.asyncio
async def test_loop_abort_on_signal(db_factory, tmp_path):
    """Loop marked failed when Claude signals 'abort' and no retries remain."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="loop-worker-3")
        db.add(inst)
        task = _make_loop_task(db, tmp_path)
        task.max_retries = 0  # skip retry so we get "failed" directly
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"

    async def launch_and_abort(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        _write_signal(signal_path, "abort", "something went wrong")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_and_abort)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "failed"
        assert "something went wrong" in (t.error_message or "")


@pytest.mark.asyncio
async def test_loop_abort_on_missing_signal(db_factory, tmp_path):
    """Loop marked failed when signal file is never written and no retries remain."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="loop-worker-4")
        db.add(inst)
        task = _make_loop_task(db, tmp_path)
        task.max_retries = 0  # skip retry so we get "failed" directly
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    # launch does NOT write a signal file
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "failed"
        assert t.error_message is not None


@pytest.mark.asyncio
async def test_loop_max_iterations_exceeded(db_factory, tmp_path):
    """Loop stops and marks failed after hitting max_iterations."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="loop-worker-5")
        db.add(inst)
        # max_iterations=2 so third call should never happen
        task = _make_loop_task(db, tmp_path, max_iterations=2)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"
    call_count = {"n": 0}

    async def launch_and_continue(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        call_count["n"] += 1
        _write_signal(signal_path, "continue", "more work")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_and_continue)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    # Should have run exactly 2 iterations (iteration 0 and 1), then stopped
    assert call_count["n"] == 2
    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "failed"
        assert "2" in (t.error_message or "")  # error mentions the limit


@pytest.mark.asyncio
async def test_loop_max_iterations_default_is_50(db_factory, tmp_path):
    """Default max_iterations is 50 when not specified."""
    from backend.models.task import Task as TaskModel

    async with db_factory() as db:
        t = TaskModel(title="t", mode="loop", todo_file_path="TODO.md")
        db.add(t)
        await db.commit()
        await db.refresh(t)
        assert t.max_iterations == 50


@pytest.mark.asyncio
async def test_loop_cancelled_between_iterations(db_factory, tmp_path):
    """Loop stops cleanly when task is externally cancelled between iterations."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="loop-worker-6")
        db.add(inst)
        task = _make_loop_task(db, tmp_path, max_iterations=10)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"

    async def launch_continue_then_cancel(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        _write_signal(signal_path, "continue", "keep going")
        # Cancel the task in the DB after first iteration
        async with db_factory() as db:
            from sqlalchemy import update

            await db.execute(
                update(Task).where(Task.id == task_obj.id).values(status="cancelled")
            )
            await db.commit()
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_continue_then_cancel)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    # loop stopped — launch called exactly once, task remains cancelled
    assert d.instance_manager.launch.await_count == 1
    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "cancelled"


@pytest.mark.asyncio
async def test_loop_prompt_contains_todo_path_and_signal_path(db_factory, tmp_path):
    """_build_loop_prompt includes todo_file_path and signal_path."""
    d = _make_dispatcher(db_factory)

    task = Task(
        title="prompt-test",
        mode="loop",
        todo_file_path="TASKS.md",
        target_repo=str(tmp_path),
        description="some context",
        max_iterations=10,
    )

    prompt = d._build_loop_prompt(
        task, iteration=0, signal_path="/repo/.claude-manager/loop_signal_99.json"
    )
    assert "TASKS.md" in prompt
    assert "/repo/.claude-manager/loop_signal_99.json" in prompt
    assert "第 1 轮" in prompt
    assert "some context" in prompt


@pytest.mark.asyncio
async def test_loop_prompt_iteration_numbering(db_factory, tmp_path):
    """_build_loop_prompt shows human-readable 1-indexed iteration number."""
    d = _make_dispatcher(db_factory)
    task = Task(title="t", mode="loop", todo_file_path="TODO.md", max_iterations=10)

    p0 = d._build_loop_prompt(task, iteration=0, signal_path="/sig")
    p2 = d._build_loop_prompt(task, iteration=2, signal_path="/sig")
    assert "第 1 轮" in p0
    assert "第 3 轮" in p2


@pytest.mark.asyncio
async def test_loop_prompt_no_history_no_anchor(db_factory):
    """First iteration: no history section, progress hint is generic."""
    d = _make_dispatcher(db_factory)
    task = Task(title="t", mode="loop", todo_file_path="TODO.md", max_iterations=10)

    prompt = d._build_loop_prompt(
        task, iteration=0, signal_path="/sig", history=None, anchored_total=None
    )
    assert "前几轮完成情况" not in prompt
    assert "已完成数/总数" in prompt
    assert "不要重新计数" not in prompt


@pytest.mark.asyncio
async def test_loop_prompt_with_history_and_anchor(db_factory):
    """Subsequent iterations include history and anchored total in denominator."""
    d = _make_dispatcher(db_factory)
    task = Task(title="t", mode="loop", todo_file_path="TODO.md", max_iterations=10)

    history = [
        {"iteration": 1, "progress": "2/10", "summary": "完成了登录和注册"},
        {"iteration": 2, "progress": "5/10", "summary": "实现了JWT刷新"},
    ]
    prompt = d._build_loop_prompt(
        task, iteration=2, signal_path="/sig", history=history, anchored_total=10
    )

    # History section present
    assert "=== 前几轮完成情况 ===" in prompt
    assert "第 1 轮 | 进度: 2/10 | 完成了登录和注册" in prompt
    assert "第 2 轮 | 进度: 5/10 | 实现了JWT刷新" in prompt
    assert "=== 前几轮完成情况结束 ===" in prompt

    # Anchored denominator
    assert "已完成数/10" in prompt
    assert "任务总数已确定为 10" in prompt
    assert "不要重新计数" in prompt

    # Generic hint should NOT appear
    assert "已完成数/总数" not in prompt


@pytest.mark.asyncio
async def test_loop_prompt_history_without_summary(db_factory):
    """History entries with missing summary still render correctly."""
    d = _make_dispatcher(db_factory)
    task = Task(title="t", mode="loop", todo_file_path="TODO.md", max_iterations=10)

    history = [{"iteration": 1, "progress": "1/5", "summary": ""}]
    prompt = d._build_loop_prompt(
        task, iteration=1, signal_path="/sig", history=history, anchored_total=5
    )

    assert "第 1 轮 | 进度: 1/5" in prompt
    # No trailing " | " when summary is empty
    assert "第 1 轮 | 进度: 1/5 |" not in prompt


@pytest.mark.asyncio
async def test_loop_prompt_history_without_progress(db_factory):
    """History entries with missing progress still render correctly."""
    d = _make_dispatcher(db_factory)
    task = Task(title="t", mode="loop", todo_file_path="TODO.md", max_iterations=10)

    history = [{"iteration": 1, "progress": "", "summary": "did something"}]
    prompt = d._build_loop_prompt(
        task, iteration=1, signal_path="/sig", history=history, anchored_total=None
    )

    assert "第 1 轮 | did something" in prompt
    assert "进度:" not in prompt


@pytest.mark.asyncio
async def test_loop_prompt_all_actions_include_summary_and_progress(db_factory):
    """All three signal templates (continue/done/abort) include summary and progress fields."""
    d = _make_dispatcher(db_factory)
    task = Task(title="t", mode="loop", todo_file_path="TODO.md", max_iterations=10)

    prompt = d._build_loop_prompt(task, iteration=0, signal_path="/sig")

    # Each action block should have both progress and summary
    for action in ["continue", "done", "abort"]:
        # Find the line containing this action
        lines = [
            line
            for line in prompt.split("\n")
            if f'"action": "{action}"' in line
        ]
        assert len(lines) == 1, (
            f"Expected exactly one template line for action={action}"
        )
        assert '"progress":' in lines[0], f"action={action} missing progress field"
        assert '"summary":' in lines[0], f"action={action} missing summary field"


@pytest.mark.asyncio
async def test_loop_lifecycle_anchors_total_from_first_signal(db_factory, tmp_path):
    """Total is anchored from the first progress report and used in subsequent prompts."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="anchor-worker")
        db.add(inst)
        task = _make_loop_task(db, tmp_path, max_iterations=10)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"
    prompts_sent: list[str] = []
    call_count = {"n": 0}

    async def launch_capture_prompt(*args, **kwargs):
        prompt = kwargs.get("prompt", args[1] if len(args) > 1 else "")
        prompts_sent.append(prompt)
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        call_count["n"] += 1
        if call_count["n"] == 1:
            _write_signal(signal_path, "continue", "more to do", "3/10", "完成了前三项")
        elif call_count["n"] == 2:
            _write_signal(signal_path, "continue", "more to do", "7/10", "完成了四项")
        else:
            _write_signal(signal_path, "done", "all done", "10/10", "最后三项")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_capture_prompt)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    assert call_count["n"] == 3

    # First prompt: no anchor yet, generic hint
    assert "已完成数/总数" in prompts_sent[0]
    assert "前几轮完成情况" not in prompts_sent[0]

    # Second prompt: anchored total=10, has first iteration history
    assert "已完成数/10" in prompts_sent[1]
    assert "任务总数已确定为 10" in prompts_sent[1]
    assert "第 1 轮 | 进度: 3/10 | 完成了前三项" in prompts_sent[1]

    # Third prompt: still anchored at 10, has two iterations of history
    assert "已完成数/10" in prompts_sent[2]
    assert "第 1 轮 | 进度: 3/10 | 完成了前三项" in prompts_sent[2]
    assert "第 2 轮 | 进度: 7/10 | 完成了四项" in prompts_sent[2]

    # Final progress stored in DB
    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"
        assert t.loop_progress == "10/10"


@pytest.mark.asyncio
async def test_loop_lifecycle_anchor_survives_missing_progress(db_factory, tmp_path):
    """If a later signal omits progress, the anchored total is still used for prompts."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="anchor-missing-worker")
        db.add(inst)
        task = _make_loop_task(db, tmp_path, max_iterations=10)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"
    prompts_sent: list[str] = []
    call_count = {"n": 0}

    async def launch_capture(*args, **kwargs):
        prompt = kwargs.get("prompt", args[1] if len(args) > 1 else "")
        prompts_sent.append(prompt)
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        call_count["n"] += 1
        if call_count["n"] == 1:
            _write_signal(signal_path, "continue", "ok", "1/8", "第一项")
        elif call_count["n"] == 2:
            # Claude forgets to include progress
            _write_signal(signal_path, "continue", "ok")
        else:
            _write_signal(signal_path, "done", "done", "8/8")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_capture)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    # Third prompt should still have anchored_total=8 even though second signal had no progress
    assert "已完成数/8" in prompts_sent[2]
    assert "任务总数已确定为 8" in prompts_sent[2]


@pytest.mark.asyncio
async def test_loop_lifecycle_non_numeric_progress_no_anchor(db_factory, tmp_path):
    """If progress is not in N/M format, anchoring is skipped gracefully."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="non-numeric-worker")
        db.add(inst)
        task = _make_loop_task(db, tmp_path, max_iterations=10)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"
    prompts_sent: list[str] = []
    call_count = {"n": 0}

    async def launch_capture(*args, **kwargs):
        prompt = kwargs.get("prompt", args[1] if len(args) > 1 else "")
        prompts_sent.append(prompt)
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        call_count["n"] += 1
        if call_count["n"] == 1:
            _write_signal(signal_path, "continue", "ok", "一些进度")
        else:
            _write_signal(signal_path, "done", "done", "全部完成")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_capture)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    # No anchoring happened — still using generic hint
    assert "已完成数/总数" in prompts_sent[1]
    assert "不要重新计数" not in prompts_sent[1]
    # But the history still shows the raw progress string
    assert "一些进度" in prompts_sent[1]


@pytest.mark.asyncio
async def test_must_complete_prompt_first_iteration(db_factory):
    """must_complete first iteration requires planning and shows total rounds."""
    d = _make_dispatcher(db_factory)
    task = Task(
        title="t",
        mode="loop",
        todo_file_path="TODO.md",
        max_iterations=8,
        must_complete=True,
    )

    prompt = d._build_loop_prompt(task, iteration=0, signal_path="/sig")
    assert "必须全部完成" in prompt
    assert "总共有 8 轮" in prompt
    assert "制定整体执行计划" in prompt
    assert '"plan":' in prompt
    assert "已完成数/总数" in prompt


@pytest.mark.asyncio
async def test_must_complete_prompt_subsequent_iteration(db_factory):
    """must_complete subsequent iteration shows remaining rounds, plan, and anchored total."""
    d = _make_dispatcher(db_factory)
    task = Task(
        title="t",
        mode="loop",
        todo_file_path="TODO.md",
        max_iterations=10,
        must_complete=True,
    )

    history = [
        {"iteration": 1, "progress": "3/10", "summary": "完成前三项"},
    ]
    plan_text = "第2轮: 权限; 第3轮: 测试"
    prompt = d._build_loop_prompt(
        task,
        iteration=1,
        signal_path="/sig",
        history=history,
        anchored_total=10,
        plan=plan_text,
    )

    assert "必须全部完成" in prompt
    assert "第 2 轮" in prompt
    assert "还剩 9 轮" in prompt
    assert "=== 整体计划 ===" in prompt
    assert plan_text in prompt
    assert "任务总数已确定为 10" in prompt
    assert "还剩 7 项未完成" in prompt
    assert "已完成数/10" in prompt


@pytest.mark.asyncio
async def test_must_complete_prompt_no_anchor_subsequent(db_factory):
    """must_complete subsequent iteration without anchored_total doesn't show total info."""
    d = _make_dispatcher(db_factory)
    task = Task(
        title="t",
        mode="loop",
        todo_file_path="TODO.md",
        max_iterations=5,
        must_complete=True,
    )

    history = [{"iteration": 1, "progress": "", "summary": "did something"}]
    prompt = d._build_loop_prompt(
        task, iteration=1, signal_path="/sig", history=history, anchored_total=None
    )

    assert "必须全部完成" in prompt
    assert "还剩 4 轮" in prompt
    assert "任务总数已确定为" not in prompt
    assert "已完成数/总数" in prompt


@pytest.mark.asyncio
async def test_must_complete_prompt_plan_update_field(db_factory):
    """must_complete continue template includes plan field for updating."""
    d = _make_dispatcher(db_factory)
    task = Task(
        title="t",
        mode="loop",
        todo_file_path="TODO.md",
        max_iterations=5,
        must_complete=True,
    )

    prompt = d._build_loop_prompt(
        task,
        iteration=1,
        signal_path="/sig",
        history=[{"iteration": 1, "progress": "1/5", "summary": "x"}],
        anchored_total=5,
    )
    continue_lines = [
        line for line in prompt.split("\n") if '"action": "continue"' in line
    ]
    assert len(continue_lines) == 1
    assert '"plan":' in continue_lines[0]


@pytest.mark.asyncio
async def test_normal_loop_prompt_no_plan_field(db_factory):
    """Normal (non-must_complete) loop prompt does NOT include plan field."""
    d = _make_dispatcher(db_factory)
    task = Task(
        title="t",
        mode="loop",
        todo_file_path="TODO.md",
        max_iterations=10,
        must_complete=False,
    )

    prompt = d._build_loop_prompt(task, iteration=0, signal_path="/sig")
    assert "必须全部完成" not in prompt
    assert '"plan":' not in prompt
    assert "制定整体执行计划" not in prompt


@pytest.mark.asyncio
async def test_must_complete_rejects_premature_done(db_factory, tmp_path):
    """must_complete rejects done signal when progress numerator < anchored total."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="mc-reject-worker")
        db.add(inst)
        task = Task(
            title="mc-reject",
            mode="loop",
            todo_file_path="TODO.md",
            target_repo=str(tmp_path),
            max_iterations=10,
            must_complete=True,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"
    call_count = {"n": 0}

    async def launch_side(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        call_count["n"] += 1
        if call_count["n"] == 1:
            _write_signal(
                signal_path, "continue", "more", "3/10", "did 3", plan="第2轮: 做剩下的"
            )
        elif call_count["n"] == 2:
            # Premature done — only 7/10
            _write_signal(signal_path, "done", "I think I'm done", "7/10", "did 4")
        elif call_count["n"] == 3:
            # After rejection, continue
            _write_signal(signal_path, "continue", "ok more", "9/10", "did 2")
        else:
            _write_signal(signal_path, "done", "really done", "10/10", "did last 1")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_side)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    # Should have run 4 iterations: iter0(continue), iter1(done→rejected), iter2(continue), iter3(done→accepted)
    assert call_count["n"] == 4
    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"
        assert t.loop_progress == "10/10"


@pytest.mark.asyncio
async def test_must_complete_accepts_done_when_complete(db_factory, tmp_path):
    """must_complete accepts done signal when progress numerator == anchored total."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="mc-accept-worker")
        db.add(inst)
        task = Task(
            title="mc-accept",
            mode="loop",
            todo_file_path="TODO.md",
            target_repo=str(tmp_path),
            max_iterations=10,
            must_complete=True,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"
    call_count = {"n": 0}

    async def launch_side(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        call_count["n"] += 1
        if call_count["n"] == 1:
            _write_signal(
                signal_path, "continue", "more", "5/10", "half done", plan="finish rest"
            )
        else:
            _write_signal(signal_path, "done", "all done", "10/10", "finished all")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_side)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    assert call_count["n"] == 2
    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"


@pytest.mark.asyncio
async def test_must_complete_done_unparseable_progress_accepted(db_factory, tmp_path):
    """must_complete accepts done if progress can't be parsed (can't verify, don't block)."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="mc-noparse-worker")
        db.add(inst)
        task = Task(
            title="mc-noparse",
            mode="loop",
            todo_file_path="TODO.md",
            target_repo=str(tmp_path),
            max_iterations=10,
            must_complete=True,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"

    async def launch_side(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        _write_signal(signal_path, "done", "all done", "全部完成")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_side)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"


@pytest.mark.asyncio
async def test_must_complete_max_iterations_fail_message(db_factory, tmp_path):
    """must_complete uses specific fail message when max iterations exceeded."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="mc-maxiter-worker")
        db.add(inst)
        task = Task(
            title="mc-maxiter",
            mode="loop",
            todo_file_path="TODO.md",
            target_repo=str(tmp_path),
            max_iterations=2,
            must_complete=True,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"

    async def launch_side(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        _write_signal(signal_path, "continue", "more", "1/5", "did one", plan="plan")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_side)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "failed"
        assert "未能在 2 轮内完成所有任务项" in t.error_message
        assert "1/5" in t.error_message


@pytest.mark.asyncio
async def test_must_complete_plan_captured_and_updated(db_factory, tmp_path):
    """Plan is captured from signal and latest version is passed to subsequent prompts."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="mc-plan-worker")
        db.add(inst)
        task = Task(
            title="mc-plan",
            mode="loop",
            todo_file_path="TODO.md",
            target_repo=str(tmp_path),
            max_iterations=10,
            must_complete=True,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"
    prompts_sent: list[str] = []
    call_count = {"n": 0}

    async def launch_side(*args, **kwargs):
        prompt = kwargs.get("prompt", args[1] if len(args) > 1 else "")
        prompts_sent.append(prompt)
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        call_count["n"] += 1
        if call_count["n"] == 1:
            _write_signal(
                signal_path,
                "continue",
                "more",
                "2/6",
                "did 2",
                plan="第2轮: A+B; 第3轮: C+D",
            )
        elif call_count["n"] == 2:
            # Update the plan
            _write_signal(
                signal_path,
                "continue",
                "more",
                "4/6",
                "did 2",
                plan="第3轮: C+D+E（合并了）",
            )
        else:
            _write_signal(signal_path, "done", "done", "6/6", "finished")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_side)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    assert call_count["n"] == 3

    # First prompt: no plan section (first iteration)
    assert "整体计划" not in prompts_sent[0]

    # Second prompt: has original plan
    assert "第2轮: A+B; 第3轮: C+D" in prompts_sent[1]

    # Third prompt: has UPDATED plan (not the original)
    assert "第3轮: C+D+E（合并了）" in prompts_sent[2]
    assert "第2轮: A+B" not in prompts_sent[2]


@pytest.mark.asyncio
async def test_must_complete_plan_persists_when_not_updated(db_factory, tmp_path):
    """Plan from earlier iteration persists if later signals don't include plan."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="mc-plan-persist-worker")
        db.add(inst)
        task = Task(
            title="mc-plan-persist",
            mode="loop",
            todo_file_path="TODO.md",
            target_repo=str(tmp_path),
            max_iterations=10,
            must_complete=True,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"
    prompts_sent: list[str] = []
    call_count = {"n": 0}

    async def launch_side(*args, **kwargs):
        prompt = kwargs.get("prompt", args[1] if len(args) > 1 else "")
        prompts_sent.append(prompt)
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        call_count["n"] += 1
        if call_count["n"] == 1:
            _write_signal(
                signal_path, "continue", "more", "2/6", "did 2", plan="原始计划"
            )
        elif call_count["n"] == 2:
            # No plan in this signal
            _write_signal(signal_path, "continue", "more", "4/6", "did 2")
        else:
            _write_signal(signal_path, "done", "done", "6/6", "finished")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_side)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    # Third prompt: still has the original plan since second signal didn't update it
    assert "原始计划" in prompts_sent[2]


@pytest.mark.asyncio
async def test_lifecycle_routes_to_loop(db_factory, tmp_path):
    """_run_task_lifecycle delegates to _run_loop_lifecycle for mode='loop'."""
    d = _make_dispatcher(db_factory)

    loop_called = {"called": False}

    async def fake_loop(*args, **kwargs):
        loop_called["called"] = True

    d._run_loop_lifecycle = fake_loop

    async with db_factory() as db:
        inst = Instance(name="loop-router")
        db.add(inst)
        task = Task(
            title="route-test",
            mode="loop",
            todo_file_path="TODO.md",
            target_repo=str(tmp_path),
            max_iterations=5,
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)
    assert loop_called["called"]


# === Resume fix for missing signal ===


@pytest.mark.asyncio
async def test_loop_resume_fix_on_missing_signal(db_factory, tmp_path):
    """When signal is missing, _resume_fix_signal is called; if it succeeds, loop continues normally."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="loop-resume-1")
        db.add(inst)
        task = _make_loop_task(db, tmp_path)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        # Seed a session_id so resume is possible
        task.session_id = "sess-abc"
        await db.commit()
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"
    call_count = {"n": 0}

    async def launch_side_effect(*args, **kwargs):
        call_count["n"] += 1
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        if call_count["n"] == 1:
            # First call: normal iteration — forget to write signal
            pass
        elif call_count["n"] == 2:
            # Second call: resume fix — write done signal
            _write_signal(signal_path, "done", "fixed", "1/1")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_side_effect)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    # Should have launched twice: once for the iteration, once for the resume fix
    assert call_count["n"] == 2
    # Second launch must have used resume_session_id
    second_call = d.instance_manager.launch.call_args_list[1]
    resume_used = second_call.kwargs.get("resume_session_id") or second_call[1].get(
        "resume_session_id"
    )
    assert resume_used == "sess-abc"

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"


@pytest.mark.asyncio
async def test_loop_resume_fix_no_session_id_aborts(db_factory, tmp_path):
    """When signal is missing and task has no session_id, falls back to abort/retry immediately."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="loop-resume-2")
        db.add(inst)
        task = _make_loop_task(db, tmp_path)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        # No session_id set — resume not possible
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    # launch never writes a signal file
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    # No session → resume fix skipped → retry path (max_retries=2, retry_count starts at 0)
    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "pending"  # retries remain (0 < 2)
        assert t.retry_count == 1


@pytest.mark.asyncio
async def test_loop_resume_fix_still_fails_aborts(db_factory, tmp_path):
    """When resume fix also fails to write signal, task is aborted/retried."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="loop-resume-3")
        db.add(inst)
        task = _make_loop_task(db, tmp_path)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        task.session_id = "sess-xyz"
        task.max_retries = 0  # no retries so we get "failed"
        await db.commit()
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    # Neither iteration nor resume fix writes the signal
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "failed"


# === Loop abort auto-retry ===


@pytest.mark.asyncio
async def test_loop_abort_triggers_retry_when_retries_remain(db_factory, tmp_path):
    """Loop abort sets status=pending when retry_count < max_retries."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="loop-retry-1")
        db.add(inst)
        task = _make_loop_task(db, tmp_path)
        task.max_retries = 2
        task.retry_count = 0
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"

    async def launch_and_abort(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        _write_signal(signal_path, "abort", "something broke")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_and_abort)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "pending"
        assert t.retry_count == 1


@pytest.mark.asyncio
async def test_loop_abort_marks_failed_when_retries_exhausted(db_factory, tmp_path):
    """Loop abort marks failed when retry_count == max_retries."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="loop-retry-2")
        db.add(inst)
        task = _make_loop_task(db, tmp_path)
        task.max_retries = 2
        task.retry_count = 2
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"

    async def launch_and_abort(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        _write_signal(signal_path, "abort", "exhausted")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_and_abort)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "failed"
        assert "exhausted" in (t.error_message or "")


# === Cancellation during iteration ===


@pytest.mark.asyncio
async def test_loop_cancel_during_iteration_stops_cleanly(db_factory, tmp_path):
    """If task is cancelled while process runs, loop exits without marking failed."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="loop-cancel-mid")
        db.add(inst)
        task = _make_loop_task(db, tmp_path, max_iterations=10)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    async def launch_and_cancel(*args, **kwargs):
        # Simulate: process runs but task gets cancelled externally before it writes signal
        async with db_factory() as db:
            from sqlalchemy import update as sa_update

            await db.execute(
                sa_update(Task).where(Task.id == task_obj.id).values(status="cancelled")
            )
            await db.commit()
        # Does NOT write signal
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_and_cancel)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    # Task should remain cancelled, not overwritten to failed
    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "cancelled"


# === total_tasks_completed counting ===


@pytest.mark.asyncio
async def test_auto_task_total_completed_incremented_once(db_factory):
    """Completing an auto-mode task increments total_tasks_completed exactly once."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="count-worker", total_tasks_completed=0)
        db.add(inst)
        task = Task(title="count-test", description="do it", target_repo="/repo")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    async with db_factory() as db:
        i = await db.get(Instance, inst_id)
        assert i.total_tasks_completed == 1


@pytest.mark.asyncio
async def test_loop_task_total_completed_incremented_once_on_done(db_factory, tmp_path):
    """Completing a loop task increments total_tasks_completed exactly once."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="loop-count-worker", total_tasks_completed=0)
        db.add(inst)
        task = _make_loop_task(db, tmp_path, max_iterations=5)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"
    call_count = {"n": 0}

    async def launch_two_iters(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        call_count["n"] += 1
        if call_count["n"] == 1:
            _write_signal(signal_path, "continue", "more", "1/2")
        else:
            _write_signal(signal_path, "done", "all done", "2/2")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_two_iters)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    async with db_factory() as db:
        i = await db.get(Instance, inst_id)
        # Should be exactly 1 regardless of how many iterations ran
        assert i.total_tasks_completed == 1


@pytest.mark.asyncio
async def test_loop_total_completed_not_incremented_on_abort(db_factory, tmp_path):
    """Aborted loop task does NOT increment total_tasks_completed."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="abort-count-worker", total_tasks_completed=0)
        db.add(inst)
        task = _make_loop_task(db, tmp_path, max_iterations=5)
        task.max_retries = 0  # fail directly, no retry
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"

    async def launch_and_abort(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        _write_signal(signal_path, "abort", "blocked")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_and_abort)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    async with db_factory() as db:
        i = await db.get(Instance, inst_id)
        assert i.total_tasks_completed == 0


@pytest.mark.asyncio
async def test_resume_fix_signal_passes_loop_iteration(db_factory, tmp_path):
    """_resume_fix_signal passes the correct loop_iteration to instance_manager.launch."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="iter-check-worker")
        db.add(inst)
        task = _make_loop_task(db, tmp_path)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        task.session_id = "sess-iter"
        task.status = "executing"
        task.instance_id = inst.id
        await db.commit()
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"
    signal_path.parent.mkdir(parents=True, exist_ok=True)

    async def fix_launch(*args, **kwargs):
        _write_signal(signal_path, "done", "ok")
        return 99999

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=fix_launch)
    d.instance_manager.processes = {inst_id: mock_proc}

    result = await d._resume_fix_signal(
        inst_id,
        task_obj,
        d._task_lifecycle_generation(task_obj),
        str(tmp_path),
        signal_path,
        iteration=3,
        git_env={},
    )

    call_kwargs = d.instance_manager.launch.call_args
    loop_iter_used = call_kwargs.kwargs.get("loop_iteration") or call_kwargs[1].get(
        "loop_iteration"
    )
    assert loop_iter_used == 3
    resume_used = call_kwargs.kwargs.get("resume_session_id") or call_kwargs[1].get(
        "resume_session_id"
    )
    assert resume_used == "sess-iter"
    assert result.get("action") == "done"


@pytest.mark.asyncio
async def test_resume_fix_signal_timeout_returns_abort(db_factory, tmp_path):
    """When the resume fix process times out, _resume_fix_signal returns abort."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="timeout-fix-worker")
        db.add(inst)
        task = _make_loop_task(db, tmp_path)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        task.session_id = "sess-timeout"
        task.status = "executing"
        task.instance_id = inst.id
        await db.commit()
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"
    signal_path.parent.mkdir(parents=True, exist_ok=True)

    async def hang_and_kill(*args, **kwargs):
        # Simulate process that hangs (timeout path in _resume_fix_signal uses 60s,
        # but we patch wait_for to raise TimeoutError directly)
        return 99999

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    # First wait() raises TimeoutError (simulating the timeout); second wait() (cleanup) succeeds
    mock_proc.wait = AsyncMock(side_effect=[asyncio.TimeoutError(), None])
    mock_proc.kill = MagicMock()
    d.instance_manager.launch = AsyncMock(side_effect=hang_and_kill)
    d.instance_manager.processes = {inst_id: mock_proc}

    async def kill_generation(_instance_id, process):
        process.kill()
        process.returncode = -9
        return True

    d.instance_manager.kill_process_generation = AsyncMock(side_effect=kill_generation)

    # Signal file is NOT written (process timed out before writing)
    result = await d._resume_fix_signal(
        inst_id,
        task_obj,
        d._task_lifecycle_generation(task_obj),
        str(tmp_path),
        signal_path,
        iteration=0,
        git_env={},
    )

    assert result.get("action") == "abort"
    assert mock_proc.kill.called


@pytest.mark.asyncio
async def test_resume_fix_not_called_for_explicit_abort(db_factory, tmp_path):
    """_resume_fix_signal is NOT invoked when Claude writes an explicit 'abort' signal."""
    d = _make_dispatcher(db_factory)

    resume_fix_called = {"called": False}
    original_fix = d._resume_fix_signal

    async def spy_fix(*args, **kwargs):
        resume_fix_called["called"] = True
        return await original_fix(*args, **kwargs)

    d._resume_fix_signal = spy_fix

    async with db_factory() as db:
        inst = Instance(name="no-fix-worker")
        db.add(inst)
        task = _make_loop_task(db, tmp_path)
        task.max_retries = 0
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"

    async def launch_and_explicit_abort(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        _write_signal(signal_path, "abort", "intentional stop")
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_and_explicit_abort)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    assert not resume_fix_called["called"]
    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "failed"


@pytest.mark.asyncio
async def test_lifecycle_prompt_empty_image_paths(db_factory):
    """Empty image_paths list in metadata_ behaves the same as no images."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="worker-emptyimg")
        db.add(inst)
        task = Task(
            title="empty-img-task",
            description="another plain task",
            target_repo="/repo",
            metadata_={"image_paths": []},
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    call_kwargs = d.instance_manager.launch.call_args
    prompt_used = call_kwargs.kwargs.get("prompt") or call_kwargs[1].get("prompt")
    assert "参考图片" not in prompt_used


# === Effort level tests ===


@pytest.mark.asyncio
async def test_lifecycle_passes_effort_level_from_task(db_factory):
    """Task-level effort_level is passed to instance_manager.launch()."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="worker-1")
        db.add(inst)
        task = Task(
            title="effort-task",
            description="d",
            target_repo="/repo",
            effort_level="max",
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    call_kwargs = d.instance_manager.launch.call_args.kwargs
    assert call_kwargs["effort_level"] == "max"


@pytest.mark.asyncio
async def test_lifecycle_falls_back_to_default_effort(db_factory):
    """When task has no effort_level, settings.default_effort is used."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="worker-default-effort")
        db.add(inst)
        task = Task(title="default-effort-task", description="d", target_repo="/repo")
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    with patch("backend.services.dispatcher.settings") as mock_settings:
        mock_settings.default_effort = "medium"
        mock_settings.task_timeout_seconds = 1800
        await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    call_kwargs = d.instance_manager.launch.call_args.kwargs
    assert call_kwargs["effort_level"] == "medium"


# === Goal mode lifecycle tests ===


def _make_goal_task(db, target_repo: str = "/repo", goal_max_turns: int = 5) -> Task:
    """Create a goal mode task."""
    return Task(
        title="goal-test",
        description="implement feature X",
        mode="goal",
        goal_condition="all tests pass and lint is clean",
        goal_max_turns=goal_max_turns,
        target_repo=target_repo,
    )


@pytest.mark.asyncio
async def test_goal_achieved_after_one_turn(db_factory):
    """Goal task marked completed when evaluator says achieved on first turn."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="goal-worker-1")
        db.add(inst)
        task = _make_goal_task(db)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    from backend.services.goal_evaluator import GoalEvalResult

    with patch(
        "backend.services.goal_evaluator.GoalEvaluator.evaluate",
        return_value=GoalEvalResult(achieved=True, reason="all tests pass"),
    ):
        await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
        await d._run_goal_lifecycle(
            inst_id, task_obj, d._task_lifecycle_generation(task_obj), "/repo"
        )

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"
        assert t.goal_turns_used == 1
        assert t.goal_last_reason == "all tests pass"


@pytest.mark.asyncio
async def test_goal_turn_passes_pool_config_dir(db_factory):
    """Regression (#770 follow-up): goal launches must resolve a pool account
    via _resolve_resume_config_dir and pass it as config_dir.

    Same defect as loop mode — turn 0 (and followups) passed no config_dir, so
    the child inherited the hardcoded systemd CLAUDE_CONFIG_DIR and the pool was
    never consulted.
    """
    d = _make_dispatcher(db_factory)
    d._resolve_resume_config_dir = AsyncMock(return_value="/pool/acc-9")

    async with db_factory() as db:
        inst = Instance(name="goal-pool-worker")
        db.add(inst)
        task = _make_goal_task(db)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    from backend.services.goal_evaluator import GoalEvalResult

    with patch(
        "backend.services.goal_evaluator.GoalEvaluator.evaluate",
        return_value=GoalEvalResult(achieved=True, reason="done"),
    ):
        await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
        await d._run_goal_lifecycle(
            inst_id, task_obj, d._task_lifecycle_generation(task_obj), "/repo"
        )

    d._resolve_resume_config_dir.assert_awaited()
    # turn 0 launches a fresh session → resolver called with None
    assert d._resolve_resume_config_dir.await_args.args == (None, "claude")
    assert d.instance_manager.launch.await_args.kwargs["config_dir"] == "/pool/acc-9"


@pytest.mark.asyncio
async def test_codex_fast_goal_evaluator_uses_priority_app_server_route(
    db_factory,
):
    """A hidden Fast evaluator must never enter the Standard exec path."""

    d = _make_dispatcher(db_factory)
    codex_home = "/codex/fast-account"
    d._resolve_resume_config_dir = AsyncMock(return_value=codex_home)
    registry = MagicMock(name="fast-goal-registry")
    d.instance_manager._ensure_codex_app_server_registry = MagicMock(
        return_value=registry,
    )
    app_server_homes: list[str | None] = []
    runtime_admissions: list[tuple] = []

    @asynccontextmanager
    async def app_server_guard(home):
        app_server_homes.append(home)
        yield home

    @asynccontextmanager
    async def forbidden_exec_guard(_home):
        raise AssertionError("Fast goal evaluator entered codex exec")
        yield  # pragma: no cover

    @asynccontextmanager
    async def runtime_admission(
        provider,
        home,
        model,
        *,
        service_tier="default",
    ):
        runtime_admissions.append((provider, home, model, service_tier))
        yield None

    d.instance_manager.codex_home_app_server_guard = app_server_guard
    d.instance_manager.codex_home_exec_guard = forbidden_exec_guard
    d.instance_manager._cloudrouter_runtime_admission = runtime_admission
    pool = MagicMock()
    pool.is_known_account.return_value = True
    pool.is_home_available.return_value = True
    pool.supports_model_for_home.return_value = True
    d.codex_pool = pool

    async with db_factory() as db:
        inst = Instance(name="fast-goal-worker")
        task = _make_goal_task(db)
        task.provider = "codex"
        task.model = "gpt-5.4"
        task.codex_service_tier = "priority"
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    process = MagicMock(returncode=0, wait=AsyncMock(return_value=0))
    d.instance_manager.processes = {inst_id: process}

    from backend.services.goal_evaluator import GoalEvalResult

    with patch(
        "backend.services.goal_evaluator.GoalEvaluator.evaluate",
        new_callable=AsyncMock,
        return_value=GoalEvalResult(achieved=True, reason="done"),
    ) as evaluate:
        await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
        await d._run_goal_lifecycle(
            inst_id,
            task_obj,
            d._task_lifecycle_generation(task_obj),
            "/repo",
        )

    eval_kwargs = evaluate.await_args.kwargs
    assert eval_kwargs["model"] == "gpt-5.4"
    assert eval_kwargs["codex_home"] == codex_home
    assert eval_kwargs["codex_service_tier"] == "priority"
    assert eval_kwargs["codex_app_server_registry"] is registry
    assert app_server_homes == [codex_home]
    assert runtime_admissions == [
        ("codex", codex_home, "gpt-5.4", "priority"),
    ]
    pool.supports_model_for_home.assert_called_with(
        codex_home,
        "gpt-5.4",
        service_tier="priority",
    )


@pytest.mark.asyncio
async def test_codex_fast_goal_rejects_different_evaluator_before_main_turn(
    db_factory,
):
    """Even two Fast-capable models cannot split preflight from execution."""

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        inst = Instance(name="invalid-fast-goal-worker")
        task = _make_goal_task(db)
        task.provider = "codex"
        task.model = "gpt-5.6-sol"
        task.codex_service_tier = "priority"
        task.goal_evaluator_model = "gpt-5.4"
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    from backend.services.goal_evaluator import GoalEvaluationError

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
    with pytest.raises(GoalEvaluationError, match="same model"):
        await d._run_goal_lifecycle(
            inst_id,
            task_obj,
            d._task_lifecycle_generation(task_obj),
            "/repo",
        )

    d.instance_manager.launch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_text",
    [
        "You have hit your usage limit. Try again later.",
        "The refresh token was revoked. Please log in again.",
    ],
    ids=["usage-limit", "auth-failure"],
)
async def test_codex_goal_evaluator_rotates_without_consuming_extra_turn(
    db_factory,
    failure_text,
):
    """Evaluator account failure retries evaluation, not the completed agent turn."""
    d = _make_dispatcher(db_factory)
    d._resolve_resume_config_dir = AsyncMock(return_value="/codex/account-a")
    d._check_rate_limit_and_rotate = AsyncMock(
        return_value={
            "config_dir": "/codex/account-b",
            "session_id": "codex-thread-1",
            "excluded": {"codex-a"},
        }
    )

    async with db_factory() as db:
        inst = Instance(name="goal-codex-evaluator-worker")
        db.add(inst)
        task = _make_goal_task(db)
        task.provider = "codex"
        task.model = "gpt-5.6-sol"
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    from backend.services.goal_evaluator import GoalEvaluationError, GoalEvalResult

    evaluator_error = GoalEvaluationError(
        "Goal evaluation exited with code 1",
        provider="codex",
        returncode=1,
        stderr=failure_text,
    )
    with patch(
        "backend.services.goal_evaluator.GoalEvaluator.evaluate",
        new_callable=AsyncMock,
    ) as evaluate:
        evaluate.side_effect = [
            evaluator_error,
            GoalEvalResult(achieved=True, reason="done"),
        ]
        await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
        await d._run_goal_lifecycle(
            inst_id, task_obj, d._task_lifecycle_generation(task_obj), "/repo"
        )

    assert d.instance_manager.launch.await_count == 1
    assert evaluate.await_count == 2
    assert evaluate.await_args_list[0].kwargs["codex_home"] == "/codex/account-a"
    assert evaluate.await_args_list[1].kwargs["codex_home"] == "/codex/account-b"
    rotation_kwargs = d._check_rate_limit_and_rotate.await_args.kwargs
    assert failure_text in rotation_kwargs["combined"]
    async with db_factory() as db:
        persisted = await db.get(Task, task_obj.id)
        assert persisted.status == "completed"
        assert persisted.goal_turns_used == 1


@pytest.mark.asyncio
async def test_goal_lifecycle_retry_resumes_persisted_turn_and_session(db_factory):
    """A requeued lifecycle continues its durable turn/session progress."""
    d = _make_dispatcher(db_factory)
    d._resolve_resume_config_dir = AsyncMock(return_value="/pool/resident")

    async with db_factory() as db:
        inst = Instance(name="goal-resume-worker")
        db.add(inst)
        task = _make_goal_task(db, goal_max_turns=5)
        task.goal_turns_used = 2
        task.goal_last_reason = "two checks remain"
        task.session_id = "sess-goal-persisted"
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    from backend.services.goal_evaluator import GoalEvalResult

    with patch(
        "backend.services.goal_evaluator.GoalEvaluator.evaluate",
        return_value=GoalEvalResult(achieved=True, reason="now complete"),
    ):
        await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
        expected_generation = d._task_lifecycle_generation(task_obj)
        await d._run_goal_lifecycle(inst_id, task_obj, expected_generation, "/repo")

    launch_kwargs = d.instance_manager.launch.await_args.kwargs
    assert launch_kwargs["resume_session_id"] == "sess-goal-persisted"
    assert launch_kwargs["loop_iteration"] == 2
    assert "two checks remain" in launch_kwargs["prompt"]
    d._resolve_resume_config_dir.assert_awaited_once_with(
        "sess-goal-persisted",
        "claude",
        task_id=task_obj.id,
        expected_generation=expected_generation,
        codex_service_tier="default",
    )
    async with db_factory() as db:
        persisted = await db.get(Task, task_obj.id)
        assert persisted.status == "completed"
        assert persisted.goal_turns_used == 3
        assert persisted.goal_last_reason == "now complete"


@pytest.mark.asyncio
async def test_goal_evaluation_error_requeues_without_advancing_turn(db_factory):
    """Operational evaluator errors use lifecycle retry budget, not goal turns."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="goal-evaluator-error-worker")
        db.add(inst)
        task = _make_goal_task(db, goal_max_turns=5)
        task.goal_turns_used = 1
        task.goal_last_reason = "work remains"
        task.session_id = "sess-goal-existing"
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    from backend.services.goal_evaluator import GoalEvaluationError

    with patch(
        "backend.services.goal_evaluator.GoalEvaluator.evaluate",
        new_callable=AsyncMock,
        side_effect=GoalEvaluationError(
            "Goal evaluation process failed",
            provider="claude",
            stderr="temporary evaluator failure",
        ),
    ):
        await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
        await d._run_goal_lifecycle(
            inst_id, task_obj, d._task_lifecycle_generation(task_obj), "/repo"
        )

    async with db_factory() as db:
        persisted = await db.get(Task, task_obj.id)
        assert persisted.status == "pending"
        assert persisted.retry_count == 1
        assert persisted.goal_turns_used == 1
        assert persisted.session_id == "sess-goal-existing"


@pytest.mark.asyncio
async def test_goal_achieved_after_multiple_turns(db_factory):
    """Goal task continues until evaluator says achieved."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="goal-worker-2")
        db.add(inst)
        task = _make_goal_task(db, goal_max_turns=10)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    call_count = {"n": 0}
    from backend.services.goal_evaluator import GoalEvalResult

    async def eval_side_effect(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] < 3:
            return GoalEvalResult(
                achieved=False, reason=f"still {3 - call_count['n']} issues"
            )
        return GoalEvalResult(achieved=True, reason="all clear")

    with patch(
        "backend.services.goal_evaluator.GoalEvaluator.evaluate",
        side_effect=eval_side_effect,
    ):
        await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
        await d._run_goal_lifecycle(
            inst_id, task_obj, d._task_lifecycle_generation(task_obj), "/repo"
        )

    assert call_count["n"] == 3
    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "completed"
        assert t.goal_turns_used == 3


@pytest.mark.asyncio
async def test_frontend_review_goal_evidence_gate_forces_another_turn(db_factory):
    """A positive model verdict cannot bypass missing browser proof."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="frontend-goal-evidence-worker")
        db.add(inst)
        task = _make_goal_task(db, goal_max_turns=3)
        task.metadata_ = {
            "frontend_review": {
                "mode": "goal",
                "profile": "standard",
                "max_iterations": 3,
            },
        }
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    from backend.services.goal_evaluator import GoalEvalResult
    evidence = AsyncMock(side_effect=[
        ("\nno proof", False, "需要真实浏览器截图"),
        ("\nproof exists", True, "证据门禁已满足"),
    ])
    with (
        patch(
            "backend.services.goal_evaluator.GoalEvaluator.evaluate",
            return_value=GoalEvalResult(achieved=True, reason="模型认为完成"),
        ),
        patch(
            "backend.services.frontend_review_goal.collect_frontend_review_goal_evidence",
            evidence,
        ),
    ):
        await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
        await d._run_goal_lifecycle(
            inst_id,
            task_obj,
            d._task_lifecycle_generation(task_obj),
            "/repo",
        )

    assert evidence.await_count == 2
    assert d.instance_manager.launch.await_count == 2
    async with db_factory() as db:
        persisted = await db.get(Task, task_obj.id)
        assert persisted.status == "completed"
        assert persisted.goal_turns_used == 2
        assert persisted.goal_last_reason == "模型认为完成"


@pytest.mark.asyncio
async def test_goal_max_turns_exceeded(db_factory):
    """Goal task fails when max turns exhausted."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="goal-worker-3")
        db.add(inst)
        task = _make_goal_task(db, goal_max_turns=2)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    from backend.services.goal_evaluator import GoalEvalResult

    with patch(
        "backend.services.goal_evaluator.GoalEvaluator.evaluate",
        return_value=GoalEvalResult(achieved=False, reason="tests still failing"),
    ):
        await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
        await d._run_goal_lifecycle(
            inst_id, task_obj, d._task_lifecycle_generation(task_obj), "/repo"
        )

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "failed"
        assert "2" in t.error_message
        assert t.goal_turns_used == 2


@pytest.mark.asyncio
async def test_goal_cancelled_between_turns(db_factory):
    """Goal task stops cleanly when cancelled externally between turns."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="goal-worker-4")
        db.add(inst)
        task = _make_goal_task(db, goal_max_turns=10)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    eval_count = {"n": 0}
    from backend.services.goal_evaluator import GoalEvalResult

    async def eval_and_cancel(*args, **kwargs):
        eval_count["n"] += 1
        # Cancel task after first evaluation
        async with db_factory() as db:
            from sqlalchemy import update as sa_update

            await db.execute(
                sa_update(Task).where(Task.id == task_obj.id).values(status="cancelled")
            )
            await db.commit()
        return GoalEvalResult(achieved=False, reason="not done")

    with patch(
        "backend.services.goal_evaluator.GoalEvaluator.evaluate",
        side_effect=eval_and_cancel,
    ):
        await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
        await d._run_goal_lifecycle(
            inst_id, task_obj, d._task_lifecycle_generation(task_obj), "/repo"
        )

    assert eval_count["n"] == 1
    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "cancelled"


@pytest.mark.asyncio
async def test_goal_cancelled_during_execution(db_factory):
    """Goal task stops when cancelled while Claude is running."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="goal-worker-5")
        db.add(inst)
        task = _make_goal_task(db, goal_max_turns=10)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    async def launch_and_cancel(*args, **kwargs):
        async with db_factory() as db:
            from sqlalchemy import update as sa_update

            await db.execute(
                sa_update(Task).where(Task.id == task_obj.id).values(status="cancelled")
            )
            await db.commit()
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_and_cancel)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_goal_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), "/repo"
    )

    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t.status == "cancelled"


@pytest.mark.asyncio
async def test_goal_uses_resume_on_subsequent_turns(db_factory):
    """Goal task uses --resume for turns after the first."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="goal-worker-6")
        db.add(inst)
        task = _make_goal_task(db, goal_max_turns=5)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    launch_calls: list[dict] = []

    async def capture_launch(*args, **kwargs):
        launch_calls.append(kwargs)
        # Set session_id on first call
        if len(launch_calls) == 1:
            async with db_factory() as db:
                from sqlalchemy import update as sa_update

                await db.execute(
                    sa_update(Task)
                    .where(Task.id == task_obj.id)
                    .values(session_id="sess-goal-123")
                )
                await db.commit()
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=capture_launch)
    d.instance_manager.processes = {inst_id: mock_proc}

    eval_count = {"n": 0}
    from backend.services.goal_evaluator import GoalEvalResult

    async def eval_twice(*args, **kwargs):
        eval_count["n"] += 1
        if eval_count["n"] < 2:
            return GoalEvalResult(achieved=False, reason="not yet")
        return GoalEvalResult(achieved=True, reason="done")

    with patch(
        "backend.services.goal_evaluator.GoalEvaluator.evaluate", side_effect=eval_twice
    ):
        await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
        await d._run_goal_lifecycle(
            inst_id, task_obj, d._task_lifecycle_generation(task_obj), "/repo"
        )

    assert len(launch_calls) == 2
    # First call: no resume_session_id
    assert launch_calls[0].get("resume_session_id") is None
    # Second call: should use resume
    assert launch_calls[1].get("resume_session_id") == "sess-goal-123"


@pytest.mark.asyncio
async def test_goal_broadcasts_evaluation_events(db_factory):
    """Goal lifecycle broadcasts goal_evaluation events."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="goal-worker-7")
        db.add(inst)
        task = _make_goal_task(db, goal_max_turns=5)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    from backend.services.goal_evaluator import GoalEvalResult

    with patch(
        "backend.services.goal_evaluator.GoalEvaluator.evaluate",
        return_value=GoalEvalResult(achieved=True, reason="done"),
    ):
        await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
        await d._run_goal_lifecycle(
            inst_id, task_obj, d._task_lifecycle_generation(task_obj), "/repo"
        )

    # Check broadcasts
    broadcast_calls = d.broadcaster.broadcast.call_args_list
    goal_eval_events = [
        c
        for c in broadcast_calls
        if isinstance(c[0][1], dict) and c[0][1].get("event_type") == "goal_evaluation"
    ]
    assert len(goal_eval_events) >= 1
    assert goal_eval_events[0][0][1]["achieved"] is True
    assert goal_eval_events[0][0][1]["turn"] == 1


@pytest.mark.asyncio
async def test_goal_total_completed_incremented_once(db_factory):
    """Completing a goal task increments total_tasks_completed exactly once."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="goal-count-worker", total_tasks_completed=0)
        db.add(inst)
        task = _make_goal_task(db, goal_max_turns=5)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    eval_count = {"n": 0}
    from backend.services.goal_evaluator import GoalEvalResult

    async def eval_multi(*args, **kwargs):
        eval_count["n"] += 1
        if eval_count["n"] < 3:
            return GoalEvalResult(achieved=False, reason="not yet")
        return GoalEvalResult(achieved=True, reason="done")

    with patch(
        "backend.services.goal_evaluator.GoalEvaluator.evaluate", side_effect=eval_multi
    ):
        await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
        await d._run_goal_lifecycle(
            inst_id, task_obj, d._task_lifecycle_generation(task_obj), "/repo"
        )

    async with db_factory() as db:
        i = await db.get(Instance, inst_id)
        assert i.total_tasks_completed == 1


@pytest.mark.asyncio
async def test_goal_total_completed_not_incremented_on_failure(db_factory):
    """Failed goal task does NOT increment total_tasks_completed."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="goal-fail-count-worker", total_tasks_completed=0)
        db.add(inst)
        task = _make_goal_task(db, goal_max_turns=1)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    from backend.services.goal_evaluator import GoalEvalResult

    with patch(
        "backend.services.goal_evaluator.GoalEvaluator.evaluate",
        return_value=GoalEvalResult(achieved=False, reason="nope"),
    ):
        await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
        await d._run_goal_lifecycle(
            inst_id, task_obj, d._task_lifecycle_generation(task_obj), "/repo"
        )

    async with db_factory() as db:
        i = await db.get(Instance, inst_id)
        assert i.total_tasks_completed == 0


@pytest.mark.asyncio
async def test_lifecycle_routes_to_goal(db_factory):
    """_run_task_lifecycle delegates to _run_goal_lifecycle for mode='goal'."""
    d = _make_dispatcher(db_factory)

    goal_called = {"called": False}

    async def fake_goal(*args, **kwargs):
        goal_called["called"] = True

    d._run_goal_lifecycle = fake_goal

    async with db_factory() as db:
        inst = Instance(name="goal-router")
        db.add(inst)
        task = Task(
            title="route-goal",
            description="do it",
            mode="goal",
            goal_condition="tests pass",
            target_repo="/repo",
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)
    assert goal_called["called"]


# === Goal prompt construction tests ===


@pytest.mark.asyncio
async def test_goal_initial_prompt_contains_condition(db_factory):
    """_build_goal_initial_prompt includes the goal condition."""
    d = _make_dispatcher(db_factory)
    task = Task(
        title="t",
        description="implement X",
        mode="goal",
        goal_condition="all tests pass",
        goal_max_turns=10,
    )
    prompt = d._build_goal_initial_prompt(task)
    assert "all tests pass" in prompt
    assert "implement X" in prompt
    assert "CLAUDE.md" in prompt
    assert "10" in prompt


@pytest.mark.asyncio
async def test_goal_initial_prompt_with_images(db_factory):
    """_build_goal_initial_prompt includes image paths from metadata."""
    d = _make_dispatcher(db_factory)
    task = Task(
        title="t",
        description="implement X",
        mode="goal",
        goal_condition="condition",
        goal_max_turns=5,
        metadata_={"image_paths": ["/uploads/a.png"]},
    )
    prompt = d._build_goal_initial_prompt(task)
    assert "/uploads/a.png" in prompt
    assert "Read" in prompt


@pytest.mark.asyncio
async def test_frontend_review_goal_initial_prompt_contains_browser_protocol(db_factory):
    d = _make_dispatcher(db_factory)
    task = Task(
        title="frontend goal",
        description="审查本地页面并修复",
        mode="goal",
        goal_condition="Browser Review evidence exists",
        goal_max_turns=5,
        provider="codex",
        metadata_={
            "frontend_review": {
                "mode": "goal",
                "profile": "standard",
                "max_iterations": 5,
            },
        },
    )

    prompt = d._build_goal_initial_prompt(task)

    assert "<frontend_review_goal_protocol>" in prompt
    assert "ccm_workspace_review.test_current_changes" in prompt


def test_frontend_review_goal_activation_prompt_keeps_same_session_request():
    dispatcher = GlobalDispatcher.__new__(GlobalDispatcher)
    task = Task(
        id=91,
        description="原始任务",
        provider="codex",
        goal_condition="必须有最新浏览器截图和报告",
        goal_max_turns=5,
        metadata_={
            "frontend_review": {
                "mode": "goal",
                "profile": "standard",
                "max_iterations": 5,
            },
        },
    )

    prompt = dispatcher._build_frontend_review_goal_activation_prompt(
        task,
        {
            "message": "审查设置页窄屏并修复溢出",
            "file_paths": ["/tmp/reference.png"],
        },
        secrets_block="<resolved-secret-reference>",
    )

    assert "当前 Task 中启动了新的循环前端审查" in prompt
    assert "审查设置页窄屏并修复溢出" in prompt
    assert "/tmp/reference.png" in prompt
    assert "<resolved-secret-reference>" in prompt
    assert "ccm_workspace_review.test_current_changes" in prompt
    assert "check_current_changes_review" in prompt
    assert "修改前端代码后必须再次调用" in prompt


@pytest.mark.asyncio
async def test_goal_followup_prompt_contains_reason(db_factory, tmp_path):
    """_build_goal_followup_prompt includes evaluator reason and remaining turns."""
    d = _make_dispatcher(db_factory)
    task = Task(
        id=41,
        title="goal",
        provider="claude",
        target_repo=str(tmp_path),
    )
    prompt = d._build_goal_followup_prompt(
        task,
        "3 tests still failing",
        turn=2,
        max_turns=10,
    )
    assert "3 tests still failing" in prompt
    assert "8" in prompt  # remaining turns
    assert ".claude-manager/artifacts/task-41" in prompt


@pytest.mark.asyncio
async def test_goal_conversation_collection(db_factory):
    """_collect_goal_conversation returns formatted log entries."""
    d = _make_dispatcher(db_factory)

    from backend.models.log_entry import LogEntry

    async with db_factory() as db:
        task = Task(
            title="conv-test",
            description="d",
            mode="goal",
            goal_condition="c",
            target_repo="/repo",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

        db.add(
            LogEntry(
                instance_id=1,
                task_id=task.id,
                event_type="message",
                role="assistant",
                content="Implemented feature A",
                loop_iteration=0,
            )
        )
        db.add(
            LogEntry(
                instance_id=1,
                task_id=task.id,
                event_type="message",
                role="assistant",
                content="Fixed test failures",
                loop_iteration=1,
            )
        )
        await db.commit()

        task_id = task.id

    summary = await d._collect_goal_conversation(task_id, current_turn=1)
    assert "Implemented feature A" in summary
    assert "Fixed test failures" in summary
    assert "[Turn 1]" in summary
    assert "[Turn 2]" in summary


@pytest.mark.asyncio
async def test_goal_conversation_empty_log(db_factory):
    """_collect_goal_conversation handles empty log gracefully."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        task = Task(
            title="empty-conv",
            description="d",
            mode="goal",
            goal_condition="c",
            target_repo="/repo",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    summary = await d._collect_goal_conversation(task_id, current_turn=0)
    assert "No conversation" in summary


# === Deleted task handling tests ===


@pytest.mark.asyncio
async def test_loop_deleted_between_iterations(db_factory, tmp_path):
    """Loop stops cleanly when task is deleted between iterations."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="loop-del-worker-1")
        db.add(inst)
        task = _make_loop_task(db, tmp_path, max_iterations=10)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"

    async def launch_continue_then_delete(*args, **kwargs):
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        _write_signal(signal_path, "continue", "keep going")
        # Delete the task from DB after first iteration
        async with db_factory() as db:
            t = await db.get(Task, task_obj.id)
            if t:
                await db.delete(t)
                await db.commit()
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_continue_then_delete)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )

    # loop stopped — launch called exactly once
    assert d.instance_manager.launch.await_count == 1
    # Task should be deleted
    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t is None


@pytest.mark.asyncio
async def test_goal_deleted_between_turns(db_factory):
    """Goal task stops cleanly when deleted externally between turns."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="goal-del-worker-1")
        db.add(inst)
        task = _make_goal_task(db, goal_max_turns=10)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    eval_count = {"n": 0}
    from backend.services.goal_evaluator import GoalEvalResult

    async def eval_and_delete(*args, **kwargs):
        eval_count["n"] += 1
        # Delete task after first evaluation
        async with db_factory() as db:
            t = await db.get(Task, task_obj.id)
            if t:
                await db.delete(t)
                await db.commit()
        return GoalEvalResult(achieved=False, reason="not done")

    with patch(
        "backend.services.goal_evaluator.GoalEvaluator.evaluate",
        side_effect=eval_and_delete,
    ):
        await _claim_mode_lifecycle(db_factory, inst_id, task_obj)
        await d._run_goal_lifecycle(
            inst_id, task_obj, d._task_lifecycle_generation(task_obj), "/repo"
        )

    assert eval_count["n"] == 1
    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t is None


@pytest.mark.asyncio
async def test_goal_deleted_during_execution(db_factory):
    """Goal task stops when deleted while Claude is running."""
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="goal-del-worker-2")
        db.add(inst)
        task = _make_goal_task(db, goal_max_turns=10)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    async def launch_and_delete(*args, **kwargs):
        async with db_factory() as db:
            t = await db.get(Task, task_obj.id)
            if t:
                await db.delete(t)
                await db.commit()
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=launch_and_delete)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_goal_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), "/repo"
    )

    assert d.instance_manager.launch.await_count == 1
    async with db_factory() as db:
        t = await db.get(Task, task_obj.id)
        assert t is None


# ---------- PTY 模式 loop：同一热会话，一个迭代一个 turn ----------


async def _run_two_iteration_loop(d, db_factory, tmp_path, *, pty_enabled):
    """Helper: run a loop that signals continue then done; capture launches."""
    async with db_factory() as db:
        inst = Instance(name="loop-pty-worker")
        db.add(inst)
        task = _make_loop_task(db, tmp_path)
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id, task_obj = inst.id, task

    signal_path = tmp_path / ".claude-manager" / f"loop_signal_{task_obj.id}.json"
    launches = []

    async def fake_launch(*args, **kwargs):
        launches.append(kwargs)
        # 第一轮写 continue，第二轮写 done；并模拟 session_id 入库
        signal_path.parent.mkdir(parents=True, exist_ok=True)
        action = "continue" if len(launches) == 1 else "done"
        _write_signal(signal_path, action, "step", f"{len(launches)}/2")
        async with db_factory() as db:
            t = await db.get(Task, task_obj.id)
            t.session_id = "loop-sid-1"
            await db.commit()
        return 12345

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.launch = AsyncMock(side_effect=fake_launch)
    d.instance_manager.processes = {inst_id: mock_proc}
    d.instance_manager.pty_mode_enabled = pty_enabled
    d.instance_manager.release_pty_session = AsyncMock()

    await _claim_mode_lifecycle(db_factory, inst_id, task_obj)

    await d._run_loop_lifecycle(
        inst_id, task_obj, d._task_lifecycle_generation(task_obj), str(tmp_path)
    )
    return launches, d


@pytest.mark.asyncio
async def test_loop_pty_mode_reuses_session_per_iteration(db_factory, tmp_path):
    d = _make_dispatcher(db_factory)
    launches, d = await _run_two_iteration_loop(
        d, db_factory, tmp_path, pty_enabled=True
    )

    assert len(launches) == 2
    # 迭代 0 全新会话；迭代 1 复用同一会话（一个迭代一个 turn）
    assert launches[0].get("resume_session_id") is None
    assert launches[1].get("resume_session_id") == "loop-sid-1"
    # loop 结束后会话归还，不污染池
    d.instance_manager.release_pty_session.assert_awaited_once_with("loop-sid-1")


@pytest.mark.asyncio
async def test_loop_p_mode_stays_stateless(db_factory, tmp_path):
    d = _make_dispatcher(db_factory)
    launches, d = await _run_two_iteration_loop(
        d, db_factory, tmp_path, pty_enabled=False
    )

    assert len(launches) == 2
    # -p 模式语义不变：每轮无 resume
    assert launches[0].get("resume_session_id") is None
    assert launches[1].get("resume_session_id") is None


# ---------------------------------------------------------------------------
# clear_task_queue — interrupt drops pending chat messages
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clear_task_queue_drops_pending_messages(db_factory):
    """clear_task_queue drains all pending messages and returns the count."""
    import time
    from backend.services.dispatcher import QueuedMessage, PRIORITY_USER

    dispatcher = _make_dispatcher(db_factory)
    q = dispatcher._get_task_queue(1)
    for i in range(3):
        q.put_nowait(
            QueuedMessage(
                priority=PRIORITY_USER,
                timestamp=time.monotonic(),
                prompt=f"msg {i}",
                source="user",
            )
        )

    cleared = await dispatcher.clear_task_queue(1)

    assert cleared == 3
    assert q.empty()


@pytest.mark.asyncio
async def test_clear_task_queue_no_queue_returns_zero(db_factory):
    """clear_task_queue on a task with no queue is a no-op returning 0."""
    dispatcher = _make_dispatcher(db_factory)
    assert await dispatcher.clear_task_queue(999) == 0


@pytest.mark.asyncio
async def test_clear_cancels_message_dequeued_before_inflight_registration(
    db_factory,
):
    """A stop-session clear invalidates the consumer's unclaimed handoff."""
    dispatcher = _make_dispatcher(db_factory)
    process_message = AsyncMock()
    dispatcher._process_queued_message = process_message

    claim_started = asyncio.Event()
    release_claim = asyncio.Event()
    claim_finished = asyncio.Event()
    original_claim = dispatcher._claim_dequeued_message

    async def delayed_claim(task_id, msg):
        claim_started.set()
        await release_claim.wait()
        claimed = await original_claim(task_id, msg)
        claim_finished.set()
        return claimed

    dispatcher._claim_dequeued_message = AsyncMock(side_effect=delayed_claim)

    await dispatcher.enqueue_message(1, "cancel after dequeue")
    await asyncio.wait_for(claim_started.wait(), timeout=1)

    cleared = await dispatcher.clear_task_queue(1)
    assert cleared == 1
    assert await dispatcher.pending_task_start_ids() == set()

    release_claim.set()
    await asyncio.wait_for(claim_finished.wait(), timeout=1)
    await asyncio.sleep(0)

    process_message.assert_not_awaited()
    worker = dispatcher._task_queue_workers.get(1)
    if worker and not worker.done():
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker


@pytest.mark.asyncio
async def test_clear_preserves_registered_inflight_message_blocker(db_factory):
    """Queue clearing cannot hide work that already owns an in-flight claim."""
    dispatcher = _make_dispatcher(db_factory)
    process_started = asyncio.Event()
    release_process = asyncio.Event()

    async def process_message(_task_id, _msg):
        process_started.set()
        await release_process.wait()

    dispatcher._process_queued_message = AsyncMock(side_effect=process_message)

    await dispatcher.enqueue_message(1, "already in flight")
    await asyncio.wait_for(process_started.wait(), timeout=1)

    cleared = await dispatcher.clear_task_queue(1)

    assert cleared == 0
    assert dispatcher._task_queue_inflight == {1: 1}
    assert await dispatcher.pending_task_start_ids() == {1}

    release_process.set()
    for _ in range(20):
        if not await dispatcher.pending_task_start_ids():
            break
        await asyncio.sleep(0.01)
    assert await dispatcher.pending_task_start_ids() == set()

    worker = dispatcher._task_queue_workers.get(1)
    if worker and not worker.done():
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker


class TestResolveTimeout:
    """任务级超时解析：NULL=全局默认，0=不限时，>0=小时数。"""

    def _dispatcher(self):
        from backend.services.dispatcher import GlobalDispatcher

        return GlobalDispatcher.__new__(GlobalDispatcher)

    def test_null_uses_global_default(self):
        from backend.config import settings
        from unittest.mock import MagicMock

        t = MagicMock(timeout_hours=None)
        assert self._dispatcher()._resolve_timeout(t) == settings.task_timeout_seconds

    def test_zero_means_no_limit(self):
        from unittest.mock import MagicMock

        t = MagicMock(timeout_hours=0)
        assert self._dispatcher()._resolve_timeout(t) is None

    def test_hours_converted_to_seconds(self):
        from unittest.mock import MagicMock

        t = MagicMock(timeout_hours=2.5)
        assert self._dispatcher()._resolve_timeout(t) == 9000

    async def test_wait_process_kills_on_timeout(self):
        import asyncio
        from unittest.mock import MagicMock

        d = self._dispatcher()
        t = MagicMock(timeout_hours=0.0001, id=1)  # 0.36s

        class FakeProc:
            killed = False

            async def wait(self):
                if self.killed:
                    return -9
                await asyncio.sleep(5)

            def kill(self):
                self.killed = True

        p = FakeProc()
        d.instance_manager = MagicMock()

        async def kill_generation(_instance_id, process):
            process.kill()
            return True

        d.instance_manager.kill_process_generation = AsyncMock(
            side_effect=kill_generation
        )
        await d._wait_process(p, t, "test", instance_id=1)
        assert p.killed is True
        assert p.termination_kind == "timeout"


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group semantics")
async def test_wait_process_timeout_reaps_descendant_with_inherited_pipes(
    db_factory,
    tmp_path,
):
    """A timed-out CLI turn must not leave a tool child holding its pipes."""
    from backend.services.instance_manager import InstanceManager

    child_pid_path = tmp_path / "timeout-child.pid"
    script = (
        "import pathlib,subprocess,sys,time; "
        "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
        "pathlib.Path(sys.argv[1]).write_text(str(p.pid)); "
        "time.sleep(30)"
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        script,
        str(child_pid_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    manager = InstanceManager(db_factory, MagicMock(broadcast=AsyncMock()))
    instance_id = 991_001
    manager.processes[instance_id] = process
    manager._process_groups[instance_id] = process
    d = _make_dispatcher(db_factory)
    d.instance_manager = manager
    task = MagicMock(id=77, timeout_hours=0.00005)

    try:
        for _ in range(100):
            if child_pid_path.exists():
                break
            await asyncio.sleep(0.01)
        assert child_pid_path.exists()
        await asyncio.wait_for(
            d._wait_process(process, task, "real timeout", instance_id=instance_id),
            timeout=7,
        )
        assert process.returncode is not None
        assert not manager._process_group_alive(instance_id, process)
    finally:
        if process.returncode is None or manager._process_group_alive(
            instance_id, process
        ):
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if process.returncode is None:
            await asyncio.wait_for(process.wait(), timeout=2)


@pytest.mark.asyncio
async def test_shutdown_cancels_and_reaps_auxiliary_lifecycles(db_factory):
    d = _make_dispatcher(db_factory)
    monitor_lifecycle = asyncio.create_task(asyncio.Event().wait())
    sub_agent_lifecycle = asyncio.create_task(asyncio.Event().wait())
    monitor_process = MagicMock(pid=101, returncode=None)
    sub_agent_process = MagicMock(pid=102, returncode=None)
    d._monitor_tasks[1] = monitor_lifecycle
    d._monitor_processes[1] = monitor_process
    d._sub_agent_tasks[2] = sub_agent_lifecycle
    d._sub_agent_processes[2] = sub_agent_process

    async def terminate(process):
        process.returncode = -9

    d._terminate_aux_process = AsyncMock(side_effect=terminate)
    await d.shutdown()

    assert monitor_lifecycle.done()
    assert sub_agent_lifecycle.done()
    assert {call.args[0] for call in d._terminate_aux_process.await_args_list} == {
        monitor_process,
        sub_agent_process,
    }


@pytest.mark.asyncio
async def test_create_task_fills_default_model_and_effort(client):
    """创建任务不指定 model/effort → 自动填入全局默认值。"""
    from backend.config import settings

    with (
        patch.object(settings, "default_provider", "codex"),
        patch.object(settings, "default_codex_model", "gpt-5.6-sol"),
    ):
        resp = await client.post(
            "/api/tasks",
            json={
                "title": "T",
                "description": "d",
                "target_repo": "/tmp",
            },
        )
    assert resp.status_code == 201
    data = resp.json()
    assert data["provider"] == "codex"
    assert data["model"] == "gpt-5.6-sol"
    assert data["effort_level"] == settings.default_effort


# === Per-task queue consumer: no concurrent `--resume` on one session (task #728) ===


@pytest.mark.asyncio
async def test_mode_turn_retries_same_native_session_after_codex_rotation(db_factory):
    """Plan/loop/goal bypass normal Step 5, so their turn helper must rotate."""
    d = _make_dispatcher(db_factory)
    manager = MagicMock()
    manager.processes = {}
    manager._tasks = {}
    codes = iter([1, 0])

    async def fake_launch(**kwargs):
        manager.processes[kwargs["instance_id"]] = MagicMock(returncode=next(codes))

    manager.launch = AsyncMock(side_effect=fake_launch)
    manager.wait_for_output_consumer = AsyncMock()
    manager.get_config_dir = MagicMock(return_value="/tmp/codex-b")
    d.instance_manager = manager
    d._wait_process = AsyncMock()
    d._collect_failure_output = AsyncMock(
        return_value="You've hit your usage limit for codex"
    )
    d._task_claim_is_active = AsyncMock(return_value=True)
    d._check_rate_limit_and_rotate = AsyncMock(
        return_value={
            "config_dir": "/tmp/codex-b",
            "session_id": "thread-1",
            "excluded": {"codex-a"},
        }
    )
    task = MagicMock(
        id=42,
        model="gpt-5.6-sol",
        provider="codex",
        thinking_budget=None,
        effort_level="high",
        enable_workflows=False,
        enabled_skills=None,
    )

    exit_code, home = await d._launch_mode_turn_with_rotation(
        7,
        task,
        MagicMock(),
        "/repo",
        {},
        prompt="mode prompt",
        config_dir="/tmp/codex-a",
        resume_session_id=None,
        loop_iteration=0,
        effort_level="high",
        label="Goal turn",
    )

    assert exit_code == 0
    assert home == "/tmp/codex-b"
    assert manager.launch.await_count == 2
    assert manager.launch.await_args_list[1].kwargs["config_dir"] == "/tmp/codex-b"
    assert manager.launch.await_args_list[1].kwargs["resume_session_id"] == "thread-1"


@pytest.mark.asyncio
async def test_successful_mode_turn_returns_home_after_proactive_switch(db_factory):
    """Goal evaluation must follow a quota switch completed by the consumer."""
    d = _make_dispatcher(db_factory)
    manager = MagicMock()
    manager.processes = {7: MagicMock(returncode=0)}
    manager._tasks = {}
    manager.launch = AsyncMock()
    manager.wait_for_output_consumer = AsyncMock()
    manager.get_config_dir = MagicMock(return_value="/tmp/codex-after-switch")
    d.instance_manager = manager
    d._wait_process = AsyncMock()
    d._task_claim_is_active = AsyncMock(return_value=True)
    task = MagicMock(
        id=43,
        model="gpt-5.6-sol",
        provider="codex",
        thinking_budget=None,
        effort_level="high",
        enable_workflows=False,
        enabled_skills=None,
    )

    exit_code, home = await d._launch_mode_turn_with_rotation(
        7,
        task,
        MagicMock(),
        "/repo",
        {},
        prompt="goal prompt",
        config_dir="/tmp/codex-before-switch",
        resume_session_id="thread-1",
        loop_iteration=0,
        effort_level="high",
        label="Goal turn",
    )

    assert exit_code == 0
    assert home == "/tmp/codex-after-switch"


@pytest.mark.asyncio
async def test_codex_mode_turn_does_not_timeout_output_consumer(
    db_factory,
    monkeypatch,
):
    """Final CODEX_HOME is unavailable until quota/rebind cleanup finishes."""
    import backend.services.dispatcher as dispatcher_module

    d = _make_dispatcher(db_factory)
    manager = MagicMock()
    manager.processes = {7: MagicMock(returncode=0)}
    switch_finished = asyncio.Event()

    async def finish_switch():
        await asyncio.sleep(0)
        switch_finished.set()

    consumer = asyncio.create_task(finish_switch())
    manager._tasks = {7: consumer}
    manager.launch = AsyncMock()

    async def wait_for_consumer(*_args, **_kwargs):
        await consumer

    manager.wait_for_output_consumer = AsyncMock(side_effect=wait_for_consumer)
    manager.get_config_dir = MagicMock(
        side_effect=lambda _instance_id: (
            "/tmp/codex-after-switch"
            if switch_finished.is_set()
            else "/tmp/codex-before-switch"
        )
    )
    d.instance_manager = manager
    d._wait_process = AsyncMock()
    d._task_claim_is_active = AsyncMock(return_value=True)
    task = MagicMock(
        id=44,
        model="gpt-5.6-sol",
        provider="codex",
        thinking_budget=None,
        effort_level="high",
        enable_workflows=False,
        enabled_skills=None,
    )

    # Claude retains its bounded cleanup wait. Codex must not use that timeout:
    # its consumer owns rollout migration and the final account binding.
    wait_for = AsyncMock(side_effect=AssertionError("Codex consumer was timed out"))
    monkeypatch.setattr(dispatcher_module.asyncio, "wait_for", wait_for)

    exit_code, home = await d._launch_mode_turn_with_rotation(
        7,
        task,
        MagicMock(),
        "/repo",
        {},
        prompt="goal prompt",
        config_dir="/tmp/codex-before-switch",
        resume_session_id="thread-1",
        loop_iteration=0,
        effort_level="high",
        label="Goal turn",
    )

    assert exit_code == 0
    assert home == "/tmp/codex-after-switch"
    assert consumer.done()
    wait_for.assert_not_awaited()


@pytest.mark.asyncio
async def test_codex_routing_failure_requeues_exact_message(db_factory, monkeypatch):
    """A temporary account/home conflict must not acknowledge and lose chat."""
    import backend.services.dispatcher as disp_mod

    monkeypatch.setattr(disp_mod, "CODEX_ROUTING_RETRY_DELAY", 0.01)
    d = _make_dispatcher(db_factory)
    processed = asyncio.Event()
    seen = []

    async def fake_process(task_id, msg):
        seen.append(msg)
        if len(seen) == 1:
            raise disp_mod.CodexAccountRoutingError("all accounts cooling down")
        processed.set()

    d._process_queued_message = fake_process
    await d.enqueue_message(1, "must survive")
    await asyncio.wait_for(processed.wait(), 1)

    assert len(seen) == 2
    assert seen[0] is seen[1]
    assert seen[1].prompt == "must survive"
    d._task_queue_workers[1].cancel()


@pytest.mark.asyncio
async def test_codex_fast_admission_failure_is_visible_and_not_requeued(
    db_factory,
):
    """An unconfirmed Fast turn is a permanent refusal for this exact send."""

    from backend.services.codex_app_server import (
        CodexServiceTierUnavailableError,
    )

    d = _make_dispatcher(db_factory)
    published = asyncio.Event()
    seen = []

    async def fake_process(task_id, msg):
        seen.append(msg)
        raise CodexServiceTierUnavailableError("priority was not admitted")

    async def publish(task_id, exc):
        assert task_id == 1
        assert isinstance(exc, CodexServiceTierUnavailableError)
        published.set()

    d._process_queued_message = fake_process
    d._publish_permanent_account_routing_failure = AsyncMock(
        side_effect=publish,
    )
    await d.enqueue_message(1, "must not run as Standard")
    await asyncio.wait_for(published.wait(), 1)
    await asyncio.wait_for(d._get_task_queue(1).join(), 1)

    assert len(seen) == 1
    assert seen[0].prompt == "must not run as Standard"
    d._task_queue_workers[1].cancel()


@pytest.mark.asyncio
async def test_codex_terminal_thread_failure_is_visible_and_not_requeued(
    db_factory,
):
    """A persistent terminal thread state must not enter the five-second loop."""

    from backend.services.codex_app_server import (
        CodexThreadTerminalStateError,
    )

    d = _make_dispatcher(db_factory)
    published = asyncio.Event()
    seen = []

    async def fake_process(task_id, msg):
        seen.append(msg)
        raise CodexThreadTerminalStateError(
            "thread-system-error",
            "systemError",
            operation="thread/resume turn admission",
            recovery_attempted=True,
        )

    async def publish(task_id, exc):
        assert task_id == 1
        assert isinstance(exc, CodexThreadTerminalStateError)
        published.set()

    d._process_queued_message = fake_process
    d._publish_permanent_account_routing_failure = AsyncMock(
        side_effect=publish,
    )
    await d.enqueue_message(1, "must stop retrying visibly")
    await asyncio.wait_for(published.wait(), 1)
    await asyncio.wait_for(d._get_task_queue(1).join(), 1)

    assert len(seen) == 1
    assert seen[0].prompt == "must stop retrying visibly"
    d._task_queue_workers[1].cancel()


@pytest.mark.asyncio
async def test_codex_thread_identity_mismatch_is_visible_and_not_requeued(
    db_factory,
):
    """A contradictory resume identity cannot improve through queue retries."""

    from backend.services.codex_app_server import (
        CodexThreadIdentityMismatchError,
    )

    d = _make_dispatcher(db_factory)
    published = asyncio.Event()
    seen = []

    async def fake_process(task_id, msg):
        seen.append(msg)
        raise CodexThreadIdentityMismatchError(
            "thread-requested",
            "thread-returned",
            operation="thread/resume",
        )

    async def publish(task_id, exc):
        assert task_id == 1
        assert isinstance(exc, CodexThreadIdentityMismatchError)
        published.set()

    d._process_queued_message = fake_process
    d._publish_permanent_account_routing_failure = AsyncMock(
        side_effect=publish,
    )
    await d.enqueue_message(1, "must not cross native threads")
    await asyncio.wait_for(published.wait(), 1)
    await asyncio.wait_for(d._get_task_queue(1).join(), 1)

    assert len(seen) == 1
    assert seen[0].prompt == "must not cross native threads"
    d._task_queue_workers[1].cancel()


@pytest.mark.asyncio
async def test_claude_routing_failure_requeues_exact_message(
    db_factory,
    monkeypatch,
):
    """A failed safe session migration must preserve the exact chat message."""
    import backend.services.dispatcher as disp_mod

    monkeypatch.setattr(disp_mod, "CODEX_ROUTING_RETRY_DELAY", 0.01)
    d = _make_dispatcher(db_factory)
    processed = asyncio.Event()
    seen = []

    async def fake_process(task_id, msg):
        seen.append(msg)
        if len(seen) == 1:
            raise disp_mod.ClaudeAccountRoutingError(
                "session hardlink failed",
                retry_after=0.01,
            )
        processed.set()

    d._process_queued_message = fake_process
    await d.enqueue_message(1, "must keep Claude context")
    await asyncio.wait_for(processed.wait(), 1)

    assert len(seen) == 2
    assert seen[0] is seen[1]
    assert seen[1].prompt == "must keep Claude context"
    d._task_queue_workers[1].cancel()


@pytest.mark.asyncio
async def test_instance_contention_requeues_exact_message(db_factory, monkeypatch):
    """A last-line launch collision must never acknowledge and lose chat."""
    import backend.services.dispatcher as disp_mod
    from backend.services.instance_manager import InstanceAlreadyRunningError

    monkeypatch.setattr(disp_mod, "CODEX_ROUTING_RETRY_DELAY", 0.01)
    d = _make_dispatcher(db_factory)
    processed = asyncio.Event()
    seen = []

    async def fake_process(task_id, msg):
        seen.append(msg)
        if len(seen) == 1:
            raise InstanceAlreadyRunningError("instance already running")
        processed.set()

    d._process_queued_message = fake_process
    await d.enqueue_message(1, "must also survive contention")
    await asyncio.wait_for(processed.wait(), 1)

    assert len(seen) == 2
    assert seen[0] is seen[1]
    assert seen[1].prompt == "must also survive contention"
    d._task_queue_workers[1].cancel()


@pytest.mark.asyncio
async def test_spawn_oserror_requeues_exact_queued_message(
    db_factory,
    monkeypatch,
):
    """A proven pre-spawn failure must never acknowledge and lose chat text."""
    import backend.services.dispatcher as disp_mod

    monkeypatch.setattr(disp_mod, "CODEX_ROUTING_RETRY_DELAY", 0)
    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    launched = asyncio.Event()
    seen_prompts: list[str] = []

    async def fail_once_then_launch(**kwargs):
        seen_prompts.append(kwargs["prompt"])
        if len(seen_prompts) == 1:
            raise OSError("agent binary missing")
        launched.set()

    d.instance_manager.launch = AsyncMock(side_effect=fail_once_then_launch)
    q = d._get_task_queue(task_id)
    await q.put(msg)
    worker = asyncio.create_task(d._task_queue_consumer(task_id))
    d._task_queue_workers[task_id] = worker
    try:
        await asyncio.wait_for(launched.wait(), timeout=2)
        assert len(seen_prompts) == 2
        assert seen_prompts[0] == seen_prompts[1]
        assert seen_prompts[0].endswith(msg.prompt)
        assert seen_prompts[0].count(
            "<ccm_task_artifact_policy>"
        ) == 1
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error_kind", "expected_notice"),
    [
        ("unsafe", "API 账号配置安全校验失败"),
        ("unavailable", "API 账号当前不可用"),
    ],
)
async def test_cloudrouter_admission_error_is_visible_and_not_requeued(
    db_factory,
    monkeypatch,
    error_kind,
    expected_notice,
):
    from backend.services.cloudrouter_accounts import (
        CloudRouterAccountError,
        CloudRouterUnsafePathError,
    )
    from backend.models.log_entry import LogEntry

    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    error = (
        CloudRouterUnsafePathError("Unsafe managed file: /private/account/.claude.json")
        if error_kind == "unsafe"
        else CloudRouterAccountError(
            "CloudRouter API account does not support model 'secret-model'"
        )
    )
    d.instance_manager.launch = AsyncMock(side_effect=error)

    with pytest.raises(type(error)):
        await d._process_queued_message(task_id, msg)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        notice = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task_id,
                    LogEntry.event_type == "system_event",
                    LogEntry.is_error.is_(True),
                )
            )
        ).scalar_one()

    assert task.status == "executing"
    assert expected_notice in notice.content
    assert "/private/" not in notice.content
    assert "secret-model" not in notice.content
    assert any(
        call.args[0] == f"task:{task_id}" and call.args[1].get("id") == notice.id
        for call in d.broadcaster.broadcast.await_args_list
    )


@pytest.mark.asyncio
async def test_stale_codex_lifecycle_cannot_defer_new_instance_owner(db_factory):
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        old_instance = Instance(name="old-owner")
        new_instance = Instance(name="new-owner")
        db.add_all([old_instance, new_instance])
        await db.flush()
        task = Task(
            title="reused-task",
            description="d",
            status="executing",
            instance_id=old_instance.id,
        )
        db.add(task)
        await db.commit()
        stale_generation = d._task_lifecycle_generation(task)
        await db.execute(
            update(Task).where(Task.id == task.id).values(instance_id=new_instance.id)
        )
        await db.commit()
        task_id = task.id
        new_instance_id = new_instance.id

    await d._defer_account_routing_task(
        stale_generation,
        "stale account cooldown",
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "executing"
    assert task.instance_id == new_instance_id
    assert task_id not in d._account_routing_not_before


@pytest.mark.asyncio
async def test_codex_binding_merge_preserves_supersede_metadata(db_factory):
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(
            title="binding-merge",
            status="cancelled",
            metadata_={
                "pr_review_id": 17,
                "pr_review_superseded": True,
            },
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    assert await d._set_codex_task_binding(task_id, "codex-4")

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.metadata_ == {
            "pr_review_id": 17,
            "pr_review_superseded": True,
            "codex_account_id": "codex-4",
        }


@pytest.mark.asyncio
async def test_stale_codex_binding_cas_cannot_write_reclaimed_generation(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(name="binding-reclaim")
        db.add(instance)
        await db.flush()
        task = Task(
            title="binding-reclaim",
            status="executing",
            retry_count=0,
            instance_id=instance.id,
            metadata_={"pr_review_id": 18},
        )
        db.add(task)
        await db.commit()
        old_generation = d._task_lifecycle_generation(task)
        await db.execute(update(Task).where(Task.id == task.id).values(retry_count=1))
        await db.commit()
        task_id = task.id

    assert not await d._set_codex_task_binding(
        task_id,
        "codex-5",
        expected_generation=old_generation,
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.retry_count == 1
        assert task.metadata_ == {"pr_review_id": 18}


@pytest.mark.asyncio
async def test_duck_lifecycle_fence_can_persist_codex_binding(db_factory):
    from types import SimpleNamespace

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(name="duck-binding")
        db.add(instance)
        await db.flush()
        task = Task(
            title="duck-binding",
            status="executing",
            instance_id=instance.id,
            metadata_={},
        )
        db.add(task)
        await db.commit()
        fence = SimpleNamespace(
            task_id=task.id,
            worker_id=task.worker_id,
            shared_from_id=task.shared_from_id,
            status=None,
            retry_count=task.retry_count,
            instance_id=task.instance_id,
            started_at=task.started_at,
            completed_at=task.completed_at,
        )
        task_id = task.id

    assert await d._set_codex_task_binding(
        task_id,
        "codex-duck",
        expected_generation=fence,
    )
    async with db_factory() as db:
        assert (await db.get(Task, task_id)).metadata_ == {
            "codex_account_id": "codex-duck"
        }


@pytest.mark.asyncio
async def test_long_turn_does_not_respawn_consumer(db_factory, monkeypatch):
    """A turn longer than the stuck-threshold must NOT trip the watchdog into
    respawning the consumer.

    Regression for prod task #728: an ~14-min turn froze the activity heartbeat,
    the >120s watchdog cancelled+respawned the consumer (without killing the
    running `claude` subprocess), and the backlog got flushed as concurrent
    `claude --resume <same session>` processes. The lifetime heartbeat keeps the
    consumer marked alive so the watchdog stays quiet and processing stays serial.
    """
    import backend.services.dispatcher as disp_mod

    monkeypatch.setattr(disp_mod, "QUEUE_STUCK_THRESHOLD", 0.2)
    monkeypatch.setattr(disp_mod, "QUEUE_HEARTBEAT_INTERVAL", 0.02)

    d = _make_dispatcher(db_factory)

    started = asyncio.Event()
    release = asyncio.Event()
    active = 0
    max_active = 0

    async def fake_process(task_id, msg):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        started.set()
        await release.wait()  # hold the "turn" open past the stuck-threshold
        active -= 1

    d._process_queued_message = fake_process

    await d.enqueue_message(1, "first")
    await asyncio.wait_for(started.wait(), 1)
    worker1 = d._task_queue_workers[1]

    # Hold the turn open well past QUEUE_STUCK_THRESHOLD, then enqueue more work.
    await asyncio.sleep(0.4)
    await d.enqueue_message(1, "second")
    await asyncio.sleep(0.1)

    # Same consumer (not cancelled/respawned), and "second" did not start a
    # concurrent turn while "first" was still running.
    assert d._task_queue_workers[1] is worker1
    assert max_active == 1

    # Drain and clean up.
    release.set()
    await asyncio.sleep(0.1)
    assert max_active == 1
    worker1.cancel()


@pytest.mark.asyncio
async def test_watchdog_respawn_keeps_live_worker(db_factory, monkeypatch):
    """Watchdog replacement waits for and outlives old-consumer cleanup.

    Regression for prod task #728: the old `finally` popped
    `_task_queue_workers[task_id]` unconditionally, erasing the new consumer's
    registration so a later enqueue spawned a *second* live consumer.  The
    replacement handoff now remains registered until the old worker is fully
    cancelled, then atomically installs the sole successor.
    """
    import backend.services.dispatcher as disp_mod

    # Negative threshold → any _ensure_queue_worker on an existing worker treats
    # it as stuck and respawns, deterministically forcing the race.
    monkeypatch.setattr(disp_mod, "QUEUE_STUCK_THRESHOLD", -1)
    monkeypatch.setattr(disp_mod, "QUEUE_HEARTBEAT_INTERVAL", 0.02)

    d = _make_dispatcher(db_factory)
    started = asyncio.Event()
    release = asyncio.Event()

    async def fake_process(task_id, msg):
        started.set()
        await release.wait()

    d._process_queued_message = fake_process

    await d.enqueue_message(1, "first")
    await asyncio.wait_for(started.wait(), 1)
    c1 = d._task_queue_workers[1]

    started.clear()
    # Forces watchdog: register a handoff which cancels/awaits c1 before c2.
    await d.enqueue_message(1, "second")
    handoff = d._task_queue_workers[1]
    assert handoff is not c1
    assert getattr(handoff, "_ccm_queue_worker_handoff", False)

    # Let c1's cancellation + finally run, then the handoff can install c2.
    release.set()
    await asyncio.wait_for(started.wait(), 1)
    await asyncio.sleep(0.05)
    c2 = d._task_queue_workers[1]

    # The live worker registration must survive c1's cleanup.
    assert c2 is not handoff
    assert not c2.done()

    c2.cancel()


@pytest.mark.asyncio
async def test_watchdog_never_starts_replacement_before_old_cleanup(
    db_factory,
    monkeypatch,
):
    import backend.services.dispatcher as disp_mod

    monkeypatch.setattr(disp_mod, "QUEUE_STUCK_THRESHOLD", -1)
    d = _make_dispatcher(db_factory)
    first_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    second_started = asyncio.Event()
    calls = 0

    async def fake_process(_task_id, _msg):
        nonlocal calls
        calls += 1
        if calls == 1:
            first_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cleanup_started.set()
                await release_cleanup.wait()
                raise
        second_started.set()

    d._process_queued_message = fake_process
    await d.enqueue_message(1, "first")
    await asyncio.wait_for(first_started.wait(), 1)
    await d.enqueue_message(1, "second")
    await asyncio.wait_for(cleanup_started.wait(), 1)
    await asyncio.sleep(0)
    assert not second_started.is_set()

    release_cleanup.set()
    await asyncio.wait_for(second_started.wait(), 1)
    worker = d._task_queue_workers.get(1)
    if worker is not None:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


# === Instance contention: queued-message must not steal a claimed instance (task #676) ===


def _plan_delivery_payload(*, queue_timestamp: float) -> dict:
    return {
        "prompt": "[Approved Plan]\nImplement exactly",
        "priority": 0,
        "source": "user",
        "command_skills": None,
        "model_override": None,
        "expected_task_routing": ["claude", "default", "default"],
        "source_log_id": 1,
        "user_message_text": "Implement exactly",
        "current_message": "[Approved Plan]\nImplement exactly",
        "attachment_paths": [],
        "queue_timestamp": queue_timestamp,
    }


def _plan_delivery_digest(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()


async def _seed_plan_delivery(
    db_factory,
    *,
    receipt_key: str,
    delivery_status: str = "pending",
):
    payload = _plan_delivery_payload(queue_timestamp=1234.5)
    async with db_factory() as db:
        task = Task(title="delivery", description="delivery", status="completed")
        plan = Plan(
            title="retryable plan",
            initial_request="plan",
            pipeline_config={},
        )
        db.add_all([task, plan])
        await db.flush()
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            content="# Retry me",
            human_decision="approved",
        )
        log = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="Implement exactly",
            raw_json=json.dumps(
                {
                    "raw_content": "Implement exactly",
                    "applied_plans": [{"plan_id": plan.id}],
                }
            ),
        )
        db.add_all([version, log])
        await db.flush()
        db.add(
            PlanApplicationReceipt(
                receipt_key=receipt_key,
                target_task_id=task.id,
                manager_user_log_id=log.id,
                plan_version_ids=[version.id],
                status="committed",
                response={"ok": True, "queued": True},
                delivery_status=delivery_status,
                outbox_payload=payload,
                payload_digest=_plan_delivery_digest(payload),
                launch_evidence=(
                    {"task_id": task.id, "instance_id": 7, "retry_count": 2}
                    if delivery_status == "uncertain"
                    else None
                ),
            )
        )
        await db.flush()
        db.add(
            PlanApplication(
                plan_id=plan.id,
                plan_version_id=version.id,
                application_type="chat_message",
                target_task_id=task.id,
                user_log_id=log.id,
                application_receipt_key=receipt_key,
            )
        )
        await db.commit()
        return task.id, plan.id, version.id, log.id


@pytest.mark.asyncio
async def test_delivery_key_admission_is_idempotent(db_factory):
    dispatcher = _make_dispatcher(db_factory)
    dispatcher._ensure_queue_worker = MagicMock()

    await dispatcher.enqueue_message(
        1,
        "exact plan application",
        delivery_key="receipt-1",
    )
    await dispatcher.enqueue_message(
        1,
        "exact plan application",
        delivery_key="receipt-1",
    )

    queue = dispatcher._get_task_queue(1)
    assert queue.qsize() == 1
    queued = await queue.get()
    queue.task_done()
    assert queued.delivery_key == "receipt-1"


@pytest.mark.asyncio
async def test_launching_plan_delivery_is_not_replayed_after_restart(db_factory):
    payload = _plan_delivery_payload(queue_timestamp=1234.5)
    async with db_factory() as db:
        task = Task(title="delivery", description="delivery", status="completed")
        db.add(task)
        await db.flush()
        db.add(
            PlanApplicationReceipt(
                receipt_key="launching-receipt",
                target_task_id=task.id,
                manager_user_log_id=1,
                plan_version_ids=[1],
                status="committed",
                response={"ok": True, "queued": True},
                delivery_status="launching",
                outbox_payload=payload,
                payload_digest=_plan_delivery_digest(payload),
            )
        )
        await db.commit()
        task_id = task.id

    dispatcher = _make_dispatcher(db_factory)
    dispatcher.enqueue_plan_application_receipt = AsyncMock()
    await dispatcher._recover_plan_application_outbox()

    dispatcher.enqueue_plan_application_receipt.assert_not_awaited()
    async with db_factory() as db:
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.target_task_id == task_id
            )
        )
        assert receipt.delivery_status == "uncertain"
        assert "duplicate execution" in receipt.delivery_error


@pytest.mark.asyncio
async def test_same_process_outbox_recovery_does_not_reclassify_launch(db_factory):
    payload = _plan_delivery_payload(queue_timestamp=1234.5)
    async with db_factory() as db:
        task = Task(title="delivery", description="delivery", status="executing")
        db.add(task)
        await db.flush()
        db.add(
            PlanApplicationReceipt(
                receipt_key="live-launching-receipt",
                target_task_id=task.id,
                manager_user_log_id=1,
                plan_version_ids=[1],
                status="committed",
                response={"ok": True, "queued": True},
                delivery_status="launching",
                outbox_payload=payload,
                payload_digest=_plan_delivery_digest(payload),
            )
        )
        await db.commit()

    dispatcher = _make_dispatcher(db_factory)
    dispatcher.enqueue_plan_application_receipt = AsyncMock()
    await dispatcher._recover_plan_application_outbox(after_restart=False)

    dispatcher.enqueue_plan_application_receipt.assert_not_awaited()
    async with db_factory() as db:
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.receipt_key == "live-launching-receipt"
            )
        )
        assert receipt.delivery_status == "launching"


@pytest.mark.asyncio
async def test_queue_clear_readmits_durable_plan_delivery_in_process(db_factory):
    payload = _plan_delivery_payload(queue_timestamp=1234.5)
    async with db_factory() as db:
        task = Task(title="delivery", description="delivery", status="completed")
        db.add(task)
        await db.flush()
        db.add(
            PlanApplicationReceipt(
                receipt_key="queued-receipt",
                target_task_id=task.id,
                manager_user_log_id=1,
                plan_version_ids=[1],
                status="committed",
                response={"ok": True, "queued": True},
                delivery_status="pending",
                outbox_payload=payload,
                payload_digest=_plan_delivery_digest(payload),
            )
        )
        await db.commit()
        task_id = task.id

    dispatcher = _make_dispatcher(db_factory)
    dispatcher._ensure_queue_worker = MagicMock()
    assert await dispatcher.enqueue_plan_application_receipt("queued-receipt")
    assert await dispatcher.clear_task_queue(task_id) == 1

    queue = dispatcher._get_task_queue(task_id)
    assert queue.qsize() == 1
    queued = await queue.get()
    queue.task_done()
    assert queued.delivery_key == "queued-receipt"
    assert queued.timestamp == 1234.5
    assert dispatcher._queued_delivery_keys == {"queued-receipt"}


@pytest.mark.asyncio
async def test_abort_cancels_pending_receipt_not_yet_in_memory(db_factory):
    task_id, _plan_id, version_id, _log_id = await _seed_plan_delivery(
        db_factory,
        receipt_key="pending-before-memory-admission",
    )
    dispatcher = _make_dispatcher(db_factory)

    assert await dispatcher.abort_task_queue(task_id) == 1

    async with db_factory() as db:
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.receipt_key == "pending-before-memory-admission"
            )
        )
        assert receipt.delivery_status == "cancelled"
        assert (
            await db.scalar(
                select(PlanApplication.id).where(
                    PlanApplication.plan_version_id == version_id
                )
            )
            is None
        )
    assert dispatcher._get_task_queue(task_id).empty()
    assert not dispatcher._queued_delivery_keys


@pytest.mark.asyncio
async def test_receipt_staged_before_abort_cannot_admit_after_abort_returns(
    db_factory,
):
    dispatcher = _make_dispatcher(db_factory)
    dispatcher._ensure_queue_worker = MagicMock()
    async with db_factory() as db:
        task = Task(title="delivery", description="delivery", status="completed")
        plan = Plan(title="staged plan", initial_request="plan", pipeline_config={})
        db.add_all([task, plan])
        await db.flush()
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            content="# Retry me",
            human_decision="approved",
        )
        log = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="Implement exactly",
            raw_json=json.dumps({"applied_plans": [{"plan_id": plan.id}]}),
        )
        db.add_all([version, log])
        await db.commit()
        task_id = task.id
        plan_id = plan.id
        version_id = version.id
        log_id = log.id

    fence = await dispatcher.snapshot_plan_queue_admission(task_id)
    assert await dispatcher.abort_task_queue(task_id) == 0

    payload = _plan_delivery_payload(queue_timestamp=1234.5)
    payload["queue_admission_fence"] = fence
    async with db_factory() as db:
        db.add(
            PlanApplicationReceipt(
                receipt_key="staged-before-abort",
                target_task_id=task_id,
                manager_user_log_id=log_id,
                plan_version_ids=[version_id],
                status="committed",
                response={"ok": True, "queued": True},
                delivery_status="pending",
                outbox_payload=payload,
                payload_digest=_plan_delivery_digest(payload),
            )
        )
        db.add(
            PlanApplication(
                plan_id=plan_id,
                plan_version_id=version_id,
                application_type="chat_message",
                target_task_id=task_id,
                user_log_id=log_id,
                application_receipt_key="staged-before-abort",
            )
        )
        await db.commit()

    assert not await dispatcher.enqueue_plan_application_receipt("staged-before-abort")
    async with db_factory() as db:
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.receipt_key == "staged-before-abort"
            )
        )
        assert receipt.delivery_status == "cancelled"
        assert (
            await db.scalar(
                select(PlanApplication.id).where(
                    PlanApplication.plan_version_id == version_id
                )
            )
            is None
        )
    assert dispatcher._get_task_queue(task_id).empty()


@pytest.mark.asyncio
async def test_receipt_admission_cas_rejects_concurrent_terminal_change(db_factory):
    task_id, _plan_id, _version_id, _log_id = await _seed_plan_delivery(
        db_factory,
        receipt_key="admission-cas-loser",
    )
    dispatcher = _make_dispatcher(db_factory)
    dispatcher._ensure_queue_worker = MagicMock()

    await dispatcher._dispatch_claim_lock.acquire()
    admission = asyncio.create_task(
        dispatcher.enqueue_plan_application_receipt("admission-cas-loser")
    )
    await asyncio.sleep(0.05)
    async with db_factory() as db:
        await db.execute(
            update(PlanApplicationReceipt)
            .where(PlanApplicationReceipt.receipt_key == "admission-cas-loser")
            .values(delivery_status="cancelled")
        )
        await db.commit()
    dispatcher._dispatch_claim_lock.release()

    assert await admission is False
    assert dispatcher._get_task_queue(task_id).empty()
    assert "admission-cas-loser" not in dispatcher._queued_delivery_keys


@pytest.mark.asyncio
@pytest.mark.parametrize("admission_first", [False, True])
async def test_receipt_admission_and_abort_converge_atomically(
    db_factory,
    admission_first,
):
    task_id, _plan_id, version_id, _log_id = await _seed_plan_delivery(
        db_factory,
        receipt_key=f"admission-abort-{admission_first}",
    )
    dispatcher = _make_dispatcher(db_factory)
    dispatcher._ensure_queue_worker = MagicMock()

    await dispatcher._dispatch_claim_lock.acquire()
    if admission_first:
        first = asyncio.create_task(
            dispatcher.enqueue_plan_application_receipt(
                f"admission-abort-{admission_first}"
            )
        )
        second = asyncio.create_task(dispatcher.abort_task_queue(task_id))
    else:
        first = asyncio.create_task(dispatcher.abort_task_queue(task_id))
        second = asyncio.create_task(
            dispatcher.enqueue_plan_application_receipt(
                f"admission-abort-{admission_first}"
            )
        )
    await asyncio.sleep(0)
    dispatcher._dispatch_claim_lock.release()
    from backend.services.dispatcher import TaskStartPausedError

    results = await asyncio.gather(first, second, return_exceptions=True)
    assert all(
        not isinstance(result, BaseException)
        or isinstance(result, TaskStartPausedError)
        for result in results
    )

    async with db_factory() as db:
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.receipt_key
                == f"admission-abort-{admission_first}"
            )
        )
        assert receipt.delivery_status == "cancelled"
        assert (
            await db.scalar(
                select(PlanApplication.id).where(
                    PlanApplication.plan_version_id == version_id
                )
            )
            is None
        )
    assert dispatcher._get_task_queue(task_id).empty()


@pytest.mark.asyncio
async def test_task_queue_cancellation_lease_is_reentrant_and_outlives_abort(
    db_factory,
):
    from backend.services.dispatcher import TaskStartPausedError

    dispatcher = _make_dispatcher(db_factory)
    task_id = 818

    async with dispatcher.task_queue_cancellation_lease(task_id):
        assert (
            await dispatcher.abort_task_queue(
                task_id,
                cancel_durable=False,
            )
            == 0
        )
        with pytest.raises(TaskStartPausedError, match="being cancelled"):
            await dispatcher.snapshot_plan_queue_admission(task_id)

        async with dispatcher.task_queue_cancellation_lease(task_id):
            assert dispatcher._task_queue_cancellation_lease_counts[task_id] == 2
        assert dispatcher._task_queue_cancellation_lease_counts[task_id] == 1
        with pytest.raises(TaskStartPausedError, match="being cancelled"):
            await dispatcher.snapshot_plan_queue_admission(task_id)

    assert task_id not in dispatcher._cancel_durable_queue_tasks
    assert task_id not in dispatcher._task_queue_cancellation_lease_counts
    assert (await dispatcher.snapshot_plan_queue_admission(task_id))["generation"] == 1
    assert not dispatcher._queued_delivery_keys


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "expected_status", "application_remains"),
    [
        ("confirm_launched", "launched", True),
        ("release_for_retry", "cancelled", False),
    ],
)
async def test_uncertain_delivery_has_audited_manual_resolution(
    db_factory,
    action,
    expected_status,
    application_remains,
):
    task_id, plan_id, version_id, log_id = await _seed_plan_delivery(
        db_factory,
        receipt_key=f"uncertain-{action}",
        delivery_status="uncertain",
    )
    dispatcher = _make_dispatcher(db_factory)

    plan_ids, resolved_task_id = await dispatcher.resolve_uncertain_plan_delivery(
        receipt_key=f"uncertain-{action}",
        action=action,
        note="Checked native turn and process records",
        actor_id=9,
    )

    assert plan_ids == [plan_id]
    assert resolved_task_id == task_id
    async with db_factory() as db:
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.receipt_key == f"uncertain-{action}"
            )
        )
        application = await db.scalar(
            select(PlanApplication).where(PlanApplication.plan_version_id == version_id)
        )
        log = await db.get(LogEntry, log_id)
        assert receipt.delivery_status == expected_status
        assert receipt.delivery_resolution["action"] == action
        assert receipt.delivery_resolution["resolved_by"] == 9
        assert (application is not None) is application_remains
        if application_remains:
            assert "applied_plans" in json.loads(log.raw_json)
        else:
            assert "applied_plans" not in json.loads(log.raw_json)


@pytest.mark.asyncio
async def test_queue_clear_owns_dequeued_plan_delivery_handoff(db_factory):
    payload = _plan_delivery_payload(queue_timestamp=1234.5)
    async with db_factory() as db:
        task = Task(title="delivery", description="delivery", status="completed")
        db.add(task)
        await db.flush()
        db.add(
            PlanApplicationReceipt(
                receipt_key="handoff-receipt",
                target_task_id=task.id,
                manager_user_log_id=1,
                plan_version_ids=[1],
                status="committed",
                response={"ok": True, "queued": True},
                delivery_status="pending",
                outbox_payload=payload,
                payload_digest=_plan_delivery_digest(payload),
            )
        )
        await db.commit()
        task_id = task.id

    dispatcher = _make_dispatcher(db_factory)
    dispatcher._ensure_queue_worker = MagicMock()
    assert await dispatcher.enqueue_plan_application_receipt("handoff-receipt")
    queue = dispatcher._get_task_queue(task_id)
    old_handoff = await queue.get()
    dispatcher._task_queue_dequeued[task_id] = old_handoff

    assert await dispatcher.clear_task_queue(task_id) == 1
    assert not await dispatcher._claim_dequeued_message(task_id, old_handoff)
    queue.task_done()

    assert queue.qsize() == 1
    recovered = await queue.get()
    queue.task_done()
    assert recovered.delivery_key == "handoff-receipt"
    assert dispatcher._queued_delivery_keys == {"handoff-receipt"}


@pytest.mark.asyncio
async def test_explicit_queue_abort_releases_unstarted_plan_version(db_factory):
    payload = _plan_delivery_payload(queue_timestamp=1234.5)
    async with db_factory() as db:
        task = Task(title="delivery", description="delivery", status="completed")
        plan = Plan(title="retryable plan", initial_request="plan", pipeline_config={})
        db.add_all([task, plan])
        await db.flush()
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            content="# Retry me",
            human_decision="approved",
        )
        log = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="Implement exactly",
            raw_json=json.dumps({"applied_plans": [{"plan_id": plan.id}]}),
        )
        db.add_all([version, log])
        await db.flush()
        db.add(
            PlanApplicationReceipt(
                receipt_key="cancelled-receipt",
                target_task_id=task.id,
                manager_user_log_id=log.id,
                plan_version_ids=[version.id],
                status="committed",
                response={"ok": True, "queued": True},
                delivery_status="pending",
                outbox_payload=payload,
                payload_digest=_plan_delivery_digest(payload),
            )
        )
        await db.flush()
        db.add(
            PlanApplication(
                plan_id=plan.id,
                plan_version_id=version.id,
                application_type="chat_message",
                target_task_id=task.id,
                user_log_id=log.id,
                application_receipt_key="cancelled-receipt",
            )
        )
        await db.commit()
        task_id = task.id
        version_id = version.id

    dispatcher = _make_dispatcher(db_factory)
    dispatcher._ensure_queue_worker = MagicMock()
    assert await dispatcher.enqueue_plan_application_receipt("cancelled-receipt")
    assert await dispatcher.abort_task_queue(task_id) == 1

    async with db_factory() as db:
        assert (
            await db.scalar(
                select(PlanApplication.id).where(
                    PlanApplication.plan_version_id == version_id
                )
            )
            is None
        )
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.receipt_key == "cancelled-receipt"
            )
        )
        assert receipt.delivery_status == "cancelled"
    assert "cancelled-receipt" not in dispatcher._queued_delivery_keys


@pytest.mark.asyncio
async def test_proven_prelaunch_failure_releases_version_application(db_factory):
    payload = _plan_delivery_payload(queue_timestamp=1234.5)
    async with db_factory() as db:
        task = Task(title="delivery", description="delivery", status="completed")
        plan = Plan(
            title="retryable plan",
            initial_request="plan",
            pipeline_config={},
        )
        db.add_all([task, plan])
        await db.flush()
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            content="# Retry me",
            human_decision="approved",
        )
        log = LogEntry(
            instance_id=None,
            task_id=task.id,
            event_type="user_message",
            role="user",
            content="Implement exactly",
            raw_json=json.dumps(
                {
                    "raw_content": "Implement exactly",
                    "applied_plans": [{"plan_id": plan.id}],
                }
            ),
        )
        db.add_all([version, log])
        await db.flush()
        receipt = PlanApplicationReceipt(
            receipt_key="failed-receipt",
            target_task_id=task.id,
            manager_user_log_id=log.id,
            plan_version_ids=[version.id],
            status="committed",
            response={"ok": True, "queued": True},
            delivery_status="queued",
            outbox_payload=payload,
            payload_digest=_plan_delivery_digest(payload),
        )
        db.add(receipt)
        await db.flush()
        db.add(
            PlanApplication(
                plan_id=plan.id,
                plan_version_id=version.id,
                application_type="chat_message",
                target_task_id=task.id,
                user_log_id=log.id,
                application_receipt_key=receipt.receipt_key,
            )
        )
        await db.commit()
        log_id = log.id
        version_id = version.id

    dispatcher = _make_dispatcher(db_factory)
    dispatcher._queued_delivery_keys.add("failed-receipt")
    assert await dispatcher._release_plan_delivery_before_launch(
        "failed-receipt",
        status="failed",
        error="permanent route mismatch",
    )

    async with db_factory() as db:
        assert (
            await db.scalar(
                select(PlanApplication.id).where(
                    PlanApplication.plan_version_id == version_id
                )
            )
            is None
        )
        receipt = await db.scalar(
            select(PlanApplicationReceipt).where(
                PlanApplicationReceipt.receipt_key == "failed-receipt"
            )
        )
        log = await db.get(LogEntry, log_id)
        assert receipt.delivery_status == "failed"
        assert "applied_plans" not in json.loads(log.raw_json)
        assert json.loads(log.raw_json)["plan_delivery"]["status"] == "failed"
    assert "failed-receipt" not in dispatcher._queued_delivery_keys


@pytest.mark.asyncio
async def test_retry_queue_timestamp_preserves_original_message_order(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    d._ensure_queue_worker = MagicMock()

    await d.enqueue_message(1, "later message", queue_timestamp=20.0)
    await d.enqueue_message(1, "older retry", queue_timestamp=10.0)

    queue = d._get_task_queue(1)
    first = await queue.get()
    queue.task_done()
    assert first.prompt == "older retry"
    assert first.timestamp == 10.0
    await d.clear_task_queue(1)


async def _setup_queued_msg_two_idle(db_factory, monkeypatch):
    """Two idle instances + a resumable task; returns (d, id1, id2, task_id, msg)."""
    import time
    import backend.api.tasks as tasks_mod
    from backend.services.dispatcher import QueuedMessage, PRIORITY_USER

    # Session JSONL "present" → skip the failed/session-gone recovery branch.
    monkeypatch.setattr(
        tasks_mod,
        "_find_session_jsonl",
        lambda sid, provider="claude": "/tmp/fake.jsonl",
    )

    d = _make_dispatcher(db_factory)
    d._resolve_resume_config_dir = AsyncMock(return_value=None)

    async with db_factory() as db:
        inst1 = Instance(name="worker-1", status="idle")
        inst2 = Instance(name="worker-2", status="idle")
        db.add(inst1)
        db.add(inst2)
        task = Task(
            title="t",
            description="d",
            target_repo="/repo",
            status="executing",
            session_id="sess-1",
        )
        db.add(task)
        await db.commit()
        await db.refresh(inst1)
        await db.refresh(inst2)
        await db.refresh(task)
        id1, id2, task_id = inst1.id, inst2.id, task.id

    msg = QueuedMessage(
        priority=PRIORITY_USER,
        timestamp=time.monotonic(),
        prompt="hi",
        source="user",
    )
    return d, id1, id2, task_id, msg


@pytest.mark.asyncio
async def test_cancelled_task_followup_reclaims_session_instead_of_dropping(
    db_factory,
    monkeypatch,
):
    """Cancel stops one generation; a later chat may resume its native session."""

    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory,
        monkeypatch,
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "cancelled"
        task.completed_at = datetime.utcnow()
        await db.commit()

    await d._process_queued_message(task_id, msg)

    d.instance_manager.launch.assert_awaited_once()
    assert d.instance_manager.launch.await_args.kwargs["resume_session_id"] == "sess-1"
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "executing"
        assert task.completed_at is None


@pytest.mark.asyncio
async def test_opus5_precompact_uses_fixed_1m_context(
    db_factory,
    monkeypatch,
):
    """A legacy 200K usage report must not prematurely compact Opus 5."""
    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory,
        monkeypatch,
    )
    d._compact_session = AsyncMock(return_value="unexpected summary")

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.provider = "claude"
        task.model = "claude-opus-5"
        task.context_window_usage = {
            "input_tokens": 50_000,
            "cache_read_input_tokens": 250_000,
            "cache_creation_input_tokens": 0,
            "output_tokens": 0,
            "total_input_tokens": 300_000,
            "context_window": 200_000,
        }
        await db.commit()

    await d._process_queued_message(task_id, msg)

    d._compact_session.assert_not_awaited()
    assert d.instance_manager.launch.await_args.kwargs["resume_session_id"] == "sess-1"


@pytest.mark.asyncio
async def test_queued_resume_waits_at_maintenance_gate_and_stays_blocking(
    db_factory,
    monkeypatch,
):
    d, _id1, _id2, task_id, _msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    launched = asyncio.Event()

    async def launch(**_kwargs):
        launched.set()
        return 12345

    d.instance_manager.launch = AsyncMock(side_effect=launch)
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        await db.commit()
    await d.pause_dispatching()
    await d.enqueue_message(task_id, "continue", source="monitor:complete")

    await asyncio.sleep(0.05)
    assert not launched.is_set()
    assert await d.pending_task_start_ids() == {task_id}
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "completed"

    d.resume_dispatching()
    await asyncio.wait_for(launched.wait(), timeout=1)

    for _ in range(20):
        if not await d.pending_task_start_ids():
            break
        await asyncio.sleep(0.01)
    assert await d.pending_task_start_ids() == set()

    worker = d._task_queue_workers.get(task_id)
    if worker and not worker.done():
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker


@pytest.mark.asyncio
async def test_internal_report_without_session_waits_for_initial_generation(
    db_factory,
    monkeypatch,
):
    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory,
        monkeypatch,
    )
    msg.source = "monitor:complete"
    msg.allow_new_session = True
    msg.defer_for_initial_session = True

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.session_id = None
        task.status = "in_progress"
        await db.commit()

    with pytest.raises(
        QueuedMessagePrelaunchError,
        match="establishing its first session",
    ):
        await d._process_queued_message(task_id, msg)
    d.instance_manager.launch.assert_not_awaited()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        await db.commit()

    await d._process_queued_message(task_id, msg)
    assert d.instance_manager.launch.await_args.kwargs["resume_session_id"] is None


@pytest.mark.asyncio
async def test_internal_report_enqueue_allows_new_session_by_default(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    d._ensure_queue_worker = MagicMock()

    await d.enqueue_message(
        42,
        "monitor result",
        source="monitor:complete",
    )

    queued = d._get_task_queue(42).get_nowait()
    assert queued.allow_new_session is True
    assert queued.defer_for_initial_session is True


@pytest.mark.asyncio
async def test_pause_wins_after_queued_resume_preparation_before_launch(
    db_factory,
    monkeypatch,
):
    """Late admission closes the exact preparation -> executing race."""
    d, _id1, _id2, task_id, _msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        await db.commit()

    preparation_reached = asyncio.Event()
    release_preparation = asyncio.Event()
    launched = asyncio.Event()

    async def resolve(*_args, **_kwargs):
        preparation_reached.set()
        await release_preparation.wait()
        return None

    async def launch(**_kwargs):
        launched.set()
        return 12345

    d._resolve_resume_config_dir = AsyncMock(side_effect=resolve)
    d.instance_manager.launch = AsyncMock(side_effect=launch)
    await d.enqueue_message(task_id, "continue")
    await asyncio.wait_for(preparation_reached.wait(), timeout=1)

    await d.pause_dispatching()
    release_preparation.set()
    await asyncio.sleep(0.05)

    assert not launched.is_set()
    assert await d.pending_task_start_ids() == {task_id}
    async with d.maintenance_shutdown_guard() as pending_ids:
        assert pending_ids == {task_id}
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "completed"

    d.resume_dispatching()
    await asyncio.wait_for(launched.wait(), timeout=1)
    worker = d._task_queue_workers.get(task_id)
    if worker and not worker.done():
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker


@pytest.mark.asyncio
async def test_task_status_publication_fence_rejects_reclaimed_generation(
    db_factory,
):
    """A committed newer retry wins before an old status event is published."""

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(title="publication-race", status="completed")
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id
        old_generation = d._task_status_generation(task)

    original_execute = AsyncSession.execute
    injected = False

    async def execute_after_reclaim(session, statement, *args, **kwargs):
        nonlocal injected
        if (
            not injected
            and getattr(getattr(statement, "table", None), "name", None) == "tasks"
        ):
            injected = True
            await original_execute(
                session,
                update(Task)
                .where(Task.id == task_id)
                .values(
                    status="pending",
                    retry_count=old_generation.retry_count + 1,
                    instance_id=None,
                    started_at=None,
                    completed_at=None,
                ),
            )
            # Model the competing retry committing before the publication
            # fence obtains its row lock.
            await session.commit()
        return await original_execute(session, statement, *args, **kwargs)

    with patch.object(
        AsyncSession,
        "execute",
        new=execute_after_reclaim,
    ):
        published = await d._broadcast_task_status_generation(old_generation)

    assert injected
    assert published is False
    d.broadcaster.broadcast.assert_not_awaited()
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "pending"
        assert current.retry_count == old_generation.retry_count + 1


@pytest.mark.asyncio
async def test_completion_publication_fence_rejects_late_background_arm(
    db_factory,
):
    """A background epoch armed after commit suppresses the stale terminal event."""

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(name="late-background-arm")
        db.add(instance)
        await db.flush()
        task = Task(
            title="late-background-arm",
            status="executing",
            instance_id=instance.id,
        )
        db.add(task)
        await db.commit()
        task_id = task.id
        lifecycle_generation = d._task_lifecycle_generation(task)

    completion_committed = asyncio.Event()
    release_publication = asyncio.Event()
    real_publish = d._broadcast_task_status_generation

    async def arm_between_commit_and_publication(generation, **kwargs):
        completion_committed.set()
        await release_publication.wait()
        return await real_publish(generation, **kwargs)

    d._broadcast_task_status_generation = AsyncMock(
        side_effect=arm_between_commit_and_publication
    )
    completion = asyncio.create_task(
        d._complete_owned_task_result(lifecycle_generation)
    )
    await asyncio.wait_for(completion_committed.wait(), timeout=1)

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "completed"
        assert current.pty_background_generation is None
        current.pty_background_generation = "late-background-epoch"
        await db.commit()

    release_publication.set()
    assert await asyncio.wait_for(completion, timeout=1) == (True, False)

    d.broadcaster.broadcast.assert_not_awaited()
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "completed"
        assert current.pty_background_generation == "late-background-epoch"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["complete", "fail", "retry_exhausted"])
async def test_temporary_frontend_review_goal_terminal_paths_restore_chat_mode(
    db_factory,
    operation,
):
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(name=f"frontend-review-terminal-{operation}")
        db.add(instance)
        await db.flush()
        task = Task(
            title="temporary frontend review goal",
            status="executing",
            instance_id=instance.id,
            mode="goal",
            goal_condition="temporary review condition",
            goal_max_turns=5,
            goal_turns_used=2,
            goal_last_reason="browser passed",
            retry_count=0,
            max_retries=0,
            metadata_={
                "keep": "account-binding",
                "frontend_review": {
                    "mode": "goal",
                    "profile": "standard",
                    "max_iterations": 5,
                },
                "frontend_review_activation": {
                    "message": "review and fix the frontend",
                    "file_paths": [],
                    "secret_ids": [],
                    "restore": {
                        "mode": "auto",
                        "goal_condition": None,
                        "goal_max_turns": 30,
                        "goal_turns_used": 0,
                        "goal_last_reason": None,
                    },
                },
            },
        )
        db.add(task)
        await db.commit()
        generation = d._task_lifecycle_generation(task)
        task_id = task.id

    if operation == "complete":
        assert await d._complete_owned_task(generation)
        expected_status = "completed"
    elif operation == "fail":
        assert await d._fail_owned_task(generation, "failed")
        expected_status = "failed"
    else:
        assert await d._retry_or_fail_mode_task(generation, "exhausted") == "failed"
        expected_status = "failed"

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current is not None
        assert current.status == expected_status
        assert current.mode == "auto"
        assert current.goal_condition is None
        assert current.goal_max_turns == 30
        assert current.goal_turns_used == 0
        assert current.goal_last_reason is None
        assert current.metadata_ == {"keep": "account-binding"}


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["retry", "complete", "fail"])
async def test_owned_mode_publication_cannot_cross_new_generation(
    db_factory,
    operation,
):
    """Every mode terminal/retry helper publishes only its resulting claim."""

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(name=f"mode-{operation}")
        db.add(instance)
        await db.flush()
        task = Task(
            title=f"mode-{operation}",
            status="executing",
            instance_id=instance.id,
            retry_count=0,
            max_retries=2,
        )
        db.add(task)
        await db.commit()
        task_id, instance_id = task.id, instance.id
        lifecycle_generation = d._task_lifecycle_generation(task)

    real_publish = d._broadcast_task_status_generation

    async def reclaim_before_publication(generation, **kwargs):
        async with db_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(
                    status="in_progress",
                    retry_count=Task.retry_count + 1,
                    instance_id=instance_id,
                    completed_at=None,
                )
            )
            await db.commit()
        return await real_publish(generation, **kwargs)

    d._broadcast_task_status_generation = AsyncMock(
        side_effect=reclaim_before_publication
    )

    if operation == "retry":
        changed = await d._retry_or_fail_mode_task(
            lifecycle_generation,
            "retry",
        )
        assert changed == "pending"
    elif operation == "complete":
        assert await d._complete_owned_task(lifecycle_generation)
    else:
        assert await d._fail_owned_task(
            lifecycle_generation,
            "failed",
        )

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "in_progress"
        expected_retry_count = 2 if operation == "retry" else 1
        assert current.retry_count == expected_retry_count
        assert current.instance_id == instance_id
    d.broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_pr_fix_failure_consumer_cannot_borrow_retried_generation(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    d._handle_pr_review_failure = AsyncMock()
    async with db_factory() as db:
        instance = Instance(name="pr-fix-terminal-race")
        db.add(instance)
        await db.flush()
        task = Task(
            title="old PR fix generation",
            status="executing",
            instance_id=instance.id,
            retry_count=0,
            max_retries=0,
            metadata_={"pr_finding_action_id": 502},
        )
        db.add(task)
        await db.commit()
        task_id = task.id
        lifecycle_generation = d._task_lifecycle_generation(task)

    real_publish = d._broadcast_task_status_generation

    async def retry_before_publication(generation, **kwargs):
        async with db_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(
                    status="in_progress",
                    retry_count=Task.retry_count + 1,
                    completed_at=None,
                    error_message=None,
                )
            )
            await db.commit()
        return await real_publish(generation, **kwargs)

    d._broadcast_task_status_generation = AsyncMock(
        side_effect=retry_before_publication
    )

    assert await d._retry_or_fail_mode_task(
        lifecycle_generation,
        "old failure",
    ) == "failed"

    d._handle_pr_review_failure.assert_not_awaited()
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "in_progress"
        assert current.retry_count == 1
        assert current.error_message is None


@pytest.mark.asyncio
async def test_queued_codex_busy_launch_rolls_back_status_and_temp_skills(
    db_factory,
    monkeypatch,
):
    from backend.services.codex_app_server import CodexAppServerBusyError

    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    d._resolve_resume_config_dir = AsyncMock(return_value="/tmp/codex-a")
    d.instance_manager.launch = AsyncMock(
        side_effect=CodexAppServerBusyError("account maintenance")
    )
    msg.source = "monitor:complete"
    msg.command_skills = {"temporary": True}

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.provider = "codex"
        task.status = "completed"
        task.enabled_skills = {"base": True}
        await db.commit()

    with pytest.raises(CodexAppServerBusyError):
        await d._process_queued_message(task_id, msg)

    routing_generation = d._resolve_resume_config_dir.await_args.kwargs[
        "expected_generation"
    ]
    assert routing_generation.task_id == task_id
    assert routing_generation.status == "completed"

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "completed"
        assert task.enabled_skills == {"base": True}
    assert msg.source_logged is True
    from backend.models.log_entry import LogEntry

    async with db_factory() as db:
        stored = (
            await db.execute(
                select(LogEntry).where(
                    LogEntry.task_id == task_id,
                    LogEntry.event_type == "user_message",
                )
            )
        ).scalar_one()
    user_events = [
        call.args[1]
        for call in d.broadcaster.broadcast.await_args_list
        if (
            len(call.args) >= 2
            and call.args[0] == f"task:{task_id}"
            and call.args[1].get("event_type") == "user_message"
        )
    ]
    assert len(user_events) == 1
    assert user_events[0]["source"] == "monitor"
    assert user_events[0]["id"] == stored.id
    assert user_events[0]["task_id"] == task_id
    assert user_events[0]["timestamp"].endswith("Z")
    assert not d._launching_instances


@pytest.mark.asyncio
async def test_uncertain_queued_launch_cannot_fail_new_retry_generation(
    db_factory,
    monkeypatch,
):
    """A launch error belongs only to the exact executing claim it started."""

    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )

    async def replace_claim_then_fail(**kwargs):
        inst_id = kwargs["instance_id"]
        d.instance_manager.processes[inst_id] = MagicMock(returncode=None)
        async with db_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(retry_count=Task.retry_count + 1)
            )
            await db.commit()
        raise RuntimeError("spawn result uncertain")

    d.instance_manager.launch = AsyncMock(side_effect=replace_claim_then_fail)

    with pytest.raises(RuntimeError, match="spawn result uncertain"):
        await d._process_queued_message(task_id, msg)

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "executing"
        assert current.retry_count == 1
    assert not any(
        call.args[1].get("new_status") == "failed"
        for call in d.broadcaster.broadcast.await_args_list
        if len(call.args) > 1 and isinstance(call.args[1], dict)
    )


@pytest.mark.asyncio
async def test_pty_finalizer_cannot_complete_or_exit_new_retry_generation(
    db_factory,
    monkeypatch,
):
    """Old PTY finally events are suppressed after a concurrent retry."""

    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    d.instance_manager.pty_mode_enabled = True

    async def replace_claim_before_finally(**_kwargs):
        async with db_factory() as db:
            await db.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(retry_count=Task.retry_count + 1)
            )
            await db.commit()

    d.instance_manager.launch = AsyncMock(side_effect=replace_claim_before_finally)

    await d._process_queued_message(task_id, msg)

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "executing"
        assert current.retry_count == 1
    payloads = [
        call.args[1]
        for call in d.broadcaster.broadcast.await_args_list
        if len(call.args) > 1 and isinstance(call.args[1], dict)
    ]
    assert not any(payload.get("new_status") == "completed" for payload in payloads)
    assert not any(payload.get("event_type") == "process_exit" for payload in payloads)


@pytest.mark.asyncio
async def test_cancelled_queued_pty_launch_restores_skills_without_completion(
    db_factory,
    monkeypatch,
):
    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    d.instance_manager.pty_mode_enabled = True
    msg.command_skills = {"temporary": True}
    launch_started = asyncio.Event()
    never_finish = asyncio.Event()

    async def blocked_launch(**_kwargs):
        launch_started.set()
        await never_finish.wait()

    d.instance_manager.launch = AsyncMock(side_effect=blocked_launch)
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        task.enabled_skills = {"base": True}
        await db.commit()

    queued_turn = asyncio.create_task(d._process_queued_message(task_id, msg))
    await asyncio.wait_for(launch_started.wait(), timeout=1)
    queued_turn.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued_turn

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "executing"
        assert task.enabled_skills == {"base": True}
    assert not d._launching_instances
    assert not d._instance_claim_owners
    assert not d._chat_launch_admission_lock.locked()
    payloads = [
        call.args[1]
        for call in d.broadcaster.broadcast.await_args_list
        if len(call.args) > 1 and isinstance(call.args[1], dict)
    ]
    assert not any(
        payload.get("new_status") == "completed"
        or payload.get("event_type") == "process_exit"
        for payload in payloads
    )


@pytest.mark.asyncio
async def test_initial_command_skill_claim_uses_save_before_write_barrier(
    db_factory,
    monkeypatch,
):
    """A settings save after dequeue must become the launch/cleanup baseline."""
    from backend.services.command_registry import COMMAND_REGISTRY, Command

    monkeypatch.setitem(
        COMMAND_REGISTRY,
        "delegate-race",
        Command(
            name="delegate-race",
            description="race test",
            prompt_template="delegate",
            required_skills={"temporary": True},
        ),
    )
    d = _make_dispatcher(db_factory)
    before_claim = asyncio.Event()
    allow_claim = asyncio.Event()
    blocked_once = False

    async def block_after_stale_snapshot(_channel, payload):
        nonlocal blocked_once
        if (
            not blocked_once
            and payload.get("old_status") == "pending"
            and payload.get("new_status") == "in_progress"
        ):
            blocked_once = True
            before_claim.set()
            await allow_claim.wait()

    d.broadcaster.broadcast.side_effect = block_after_stale_snapshot

    async with db_factory() as db:
        instance = Instance(name="initial-skill-save-race")
        task = Task(
            title="initial skill save race",
            description="$delegate-race do work",
            status="in_progress",
            enabled_skills={"old": True},
            metadata_={"keep": "old"},
        )
        db.add_all([instance, task])
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        await db.refresh(task)
        instance_id, task_id, task_obj = instance.id, task.id, task

    async def successful_launch(**kwargs):
        process = MagicMock(returncode=0)
        process.wait = AsyncMock(return_value=0)
        d.instance_manager.processes[kwargs["instance_id"]] = process

    d.instance_manager.launch = AsyncMock(side_effect=successful_launch)
    lifecycle = asyncio.create_task(d._run_task_lifecycle(instance_id, task_obj))
    await asyncio.wait_for(before_claim.wait(), timeout=1)

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        current.enabled_skills = {"saved": True}
        current.metadata_ = {"keep": "saved"}
        await db.commit()

    allow_claim.set()
    await asyncio.wait_for(lifecycle, timeout=2)

    assert d.instance_manager.launch.await_args.kwargs["enabled_skills"] == {
        "saved": True,
        "temporary": True,
    }
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.enabled_skills == {"saved": True}
        assert current.metadata_ == {"keep": "saved"}


@pytest.mark.asyncio
async def test_initial_launch_uses_route_saved_before_write_barrier(
    db_factory,
):
    """The post-barrier provider/model/tier snapshot owns the first turn."""
    d = _make_dispatcher(db_factory)
    d._resolve_resume_config_dir = AsyncMock(return_value=None)
    before_claim = asyncio.Event()
    allow_claim = asyncio.Event()
    blocked_once = False

    async def block_after_stale_snapshot(_channel, payload):
        nonlocal blocked_once
        if (
            not blocked_once
            and payload.get("old_status") == "pending"
            and payload.get("new_status") == "in_progress"
        ):
            blocked_once = True
            before_claim.set()
            await allow_claim.wait()

    d.broadcaster.broadcast.side_effect = block_after_stale_snapshot

    async with db_factory() as db:
        instance = Instance(name="initial-route-save-race")
        task = Task(
            title="initial route save race",
            description="do work",
            status="in_progress",
            provider="claude",
            model="claude-opus-4-6",
            codex_service_tier="default",
        )
        db.add_all([instance, task])
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        await db.refresh(task)
        instance_id, task_id, task_obj = instance.id, task.id, task

    async def successful_launch(**kwargs):
        process = MagicMock(returncode=0)
        process.wait = AsyncMock(return_value=0)
        d.instance_manager.processes[kwargs["instance_id"]] = process

    d.instance_manager.launch = AsyncMock(side_effect=successful_launch)
    lifecycle = asyncio.create_task(d._run_task_lifecycle(instance_id, task_obj))
    await asyncio.wait_for(before_claim.wait(), timeout=1)

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        current.provider = "codex"
        current.model = "gpt-5.6-terra"
        current.codex_service_tier = "priority"
        await db.commit()

    allow_claim.set()
    await asyncio.wait_for(lifecycle, timeout=2)

    launch = d.instance_manager.launch.await_args.kwargs
    assert launch["provider"] == "codex"
    assert launch["model"] == "gpt-5.6-terra"
    assert launch["codex_service_tier"] == "priority"
    route = d._resolve_resume_config_dir.await_args
    assert route.args[:2] == (None, "codex")
    assert route.kwargs["model"] == "gpt-5.6-terra"
    assert route.kwargs["codex_service_tier"] == "priority"


@pytest.mark.asyncio
async def test_queued_command_skill_claim_uses_save_before_write_barrier(
    db_factory,
    monkeypatch,
):
    """A queued turn overlays its command on the last pre-claim user save."""
    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory,
        monkeypatch,
    )
    msg.command_skills = {"temporary": True}
    before_claim = asyncio.Event()
    allow_claim = asyncio.Event()

    @asynccontextmanager
    async def blocked_start_guard():
        before_claim.set()
        await allow_claim.wait()
        yield

    d.task_start_guard = blocked_start_guard
    queued_turn = asyncio.create_task(d._process_queued_message(task_id, msg))
    await asyncio.wait_for(before_claim.wait(), timeout=1)

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        current.enabled_skills = {"saved": True}
        current.metadata_ = {"keep": "saved"}
        await db.commit()

    allow_claim.set()
    await asyncio.wait_for(queued_turn, timeout=2)

    assert d.instance_manager.launch.await_args.kwargs["enabled_skills"] == {
        "saved": True,
        "temporary": True,
    }
    async with db_factory() as db:
        current = await db.get(Task, task_id)
        assert current.enabled_skills == {"saved": True}
        assert current.metadata_ == {"keep": "saved"}


@pytest.mark.asyncio
async def test_queued_route_change_after_resolution_requeues_with_new_snapshot(
    db_factory,
    monkeypatch,
):
    """A queued turn never launches with an account route from stale config."""
    import backend.services.dispatcher as dispatcher_module

    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory,
        monkeypatch,
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.provider = "claude"
        task.model = "claude-opus-4-6"
        task.codex_service_tier = "default"
        await db.commit()

    before_claim = asyncio.Event()
    allow_claim = asyncio.Event()
    first_claim = True

    @asynccontextmanager
    async def blocked_start_guard():
        nonlocal first_claim
        if first_claim:
            first_claim = False
            before_claim.set()
            await allow_claim.wait()
        yield

    routes = []

    async def resolve_route(_session_id, provider, **kwargs):
        routes.append(
            (
                provider,
                kwargs.get("model"),
                kwargs["codex_service_tier"],
            )
        )
        return None

    launched = asyncio.Event()

    async def successful_launch(**kwargs):
        process = MagicMock(returncode=0)
        process.wait = AsyncMock(return_value=0)
        d.instance_manager.processes[kwargs["instance_id"]] = process
        launched.set()

    d.task_start_guard = blocked_start_guard
    d._resolve_resume_config_dir = AsyncMock(side_effect=resolve_route)
    d.instance_manager.launch = AsyncMock(side_effect=successful_launch)
    queue = d._get_task_queue(task_id)
    await queue.put(msg)
    worker = asyncio.create_task(d._task_queue_consumer(task_id))
    d._task_queue_workers[task_id] = worker
    try:
        await asyncio.wait_for(before_claim.wait(), timeout=1)
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            task.provider = "codex"
            task.model = "gpt-5.6-terra"
            task.codex_service_tier = "priority"
            await db.commit()

        allow_claim.set()
        with patch.object(
            dispatcher_module,
            "CODEX_ROUTING_RETRY_DELAY",
            0,
        ):
            await asyncio.wait_for(launched.wait(), timeout=2)
            await asyncio.wait_for(queue.join(), timeout=2)
    finally:
        allow_claim.set()
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)

    assert routes == [
        ("claude", "claude-opus-4-6", "default"),
        ("codex", "gpt-5.6-terra", "priority"),
    ]
    assert d.instance_manager.launch.await_count == 1
    launch = d.instance_manager.launch.await_args.kwargs
    assert launch["provider"] == "codex"
    assert launch["model"] == "gpt-5.6-terra"
    assert launch["codex_service_tier"] == "priority"
    assert msg.instance_claim is None


@pytest.mark.asyncio
async def test_queued_launch_barrier_requeues_when_routing_stage_arrives(
    db_factory,
    monkeypatch,
):
    """A chat accepted before stage cannot cross the final launch barrier."""
    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory,
        monkeypatch,
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        await db.commit()

    before_claim = asyncio.Event()
    allow_claim = asyncio.Event()

    @asynccontextmanager
    async def blocked_start_guard():
        before_claim.set()
        await allow_claim.wait()
        yield

    d.task_start_guard = blocked_start_guard
    queued = asyncio.create_task(d._process_queued_message(task_id, msg))
    await asyncio.wait_for(before_claim.wait(), timeout=1)
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.metadata_ = {
            "worker_routing_config_pending": {
                "op_id": "queued-stage-race",
                "provider": "codex",
                "model": "gpt-5.6-sol",
                "codex_service_tier": "priority",
            }
        }
        await db.commit()
    allow_claim.set()

    with pytest.raises(
        QueuedMessagePrelaunchError,
        match="synchronization is pending",
    ):
        await asyncio.wait_for(queued, timeout=2)
    d.instance_manager.launch.assert_not_awaited()
    assert msg.instance_claim is None
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "completed"


@pytest.mark.asyncio
async def test_fresh_launch_barrier_fail_closes_pending_routing_marker(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(name="fresh-routing-marker")
        task = Task(
            title="blocked fresh task",
            description="d",
            status="in_progress",
            metadata_={
                "worker_routing_config_pending": {
                    "op_id": "fresh-stage-race",
                    "provider": "codex",
                    "model": "gpt-5.6-sol",
                    "codex_service_tier": "priority",
                }
            },
        )
        db.add_all([instance, task])
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        await db.refresh(task)
        instance_id, task_id, task_obj = instance.id, task.id, task

    await d._run_task_lifecycle(instance_id, task_obj)

    d.instance_manager.launch.assert_not_awaited()
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "failed"
        assert task.instance_id is None
        assert "worker_routing_config_pending" in task.metadata_


@pytest.mark.asyncio
async def test_dequeue_skips_task_with_pending_routing_marker(db_factory):
    from backend.services.task_queue import TaskQueue

    async with db_factory() as db:
        blocked = Task(
            title="blocked",
            description="d",
            status="pending",
            priority=-10,
            metadata_={
                "worker_routing_config_pending": {
                    "op_id": "crash-leftover",
                    "provider": "codex",
                    "model": "gpt-5.6-sol",
                    "codex_service_tier": "priority",
                }
            },
        )
        runnable = Task(
            title="runnable",
            description="d",
            status="pending",
            priority=0,
        )
        db.add_all([blocked, runnable])
        await db.commit()
        blocked_id, runnable_id = blocked.id, runnable.id

    async with db_factory() as db:
        claimed = await TaskQueue(db).dequeue()

    assert claimed is not None
    assert claimed.id == runnable_id
    async with db_factory() as db:
        blocked = await db.get(Task, blocked_id)
        assert blocked.status == "pending"


@pytest.mark.asyncio
async def test_queued_skill_restore_keeps_unrelated_edit_and_clears_temp_key(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    msg = QueuedMessage(
        priority=0,
        timestamp=0,
        prompt="continue",
        command_skills={"temporary": True},
    )
    original = {"base": True}
    token = "queued-skill-generation"

    async with db_factory() as db:
        task = Task(
            title="skill-cas",
            status="executing",
            enabled_skills={
                "base": True,
                "temporary": True,
                "user_saved": True,
            },
            metadata_={TEMP_SKILLS_GENERATION_KEY: token},
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    await d._restore_queued_message_skills(
        task_id,
        msg,
        original,
        token,
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.enabled_skills == {
            "base": True,
            "user_saved": True,
        }
        assert TEMP_SKILLS_GENERATION_KEY not in (task.metadata_ or {})


@pytest.mark.asyncio
async def test_queued_skill_restore_preserves_new_value_for_same_key(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    msg = QueuedMessage(
        priority=0,
        timestamp=0,
        prompt="continue",
        command_skills={"temporary": True},
    )
    token = "queued-skill-key-generation"

    async with db_factory() as db:
        task = Task(
            title="skill-key-cas",
            status="executing",
            enabled_skills={
                "base": True,
                "temporary": False,
            },
            metadata_={TEMP_SKILLS_GENERATION_KEY: token},
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    await d._restore_queued_message_skills(
        task_id,
        msg,
        {"base": True},
        token,
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.enabled_skills == {
            "base": True,
            "temporary": False,
        }
        assert TEMP_SKILLS_GENERATION_KEY not in (task.metadata_ or {})


@pytest.mark.asyncio
async def test_queued_skill_restore_preserves_same_value_explicit_save(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    msg = QueuedMessage(
        priority=0,
        timestamp=0,
        prompt="continue",
        command_skills={"temporary": True},
    )

    async with db_factory() as db:
        task = Task(
            title="skill-same-value-save",
            status="executing",
            enabled_skills={"base": True, "temporary": True},
            # The settings API clears the marker on every explicit save, even
            # when the saved JSON equals the temporary view.
            metadata_={},
        )
        db.add(task)
        await db.commit()
        task_id = task.id

    await d._restore_queued_message_skills(
        task_id,
        msg,
        {"base": True},
        "old-temporary-generation",
    )

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.enabled_skills == {
            "base": True,
            "temporary": True,
        }


@pytest.mark.asyncio
async def test_queued_pty_wait_failure_never_synthesizes_success(
    db_factory,
    monkeypatch,
):
    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    d.instance_manager.pty_mode_enabled = True

    async def launch_with_process(**kwargs):
        d.instance_manager.processes[kwargs["instance_id"]] = MagicMock(returncode=None)

    d.instance_manager.launch = AsyncMock(side_effect=launch_with_process)
    d._wait_process = AsyncMock(side_effect=RuntimeError("turn wait failed"))

    with pytest.raises(RuntimeError, match="turn wait failed"):
        await d._process_queued_message(task_id, msg)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "executing"
    payloads = [
        call.args[1]
        for call in d.broadcaster.broadcast.await_args_list
        if len(call.args) > 1 and isinstance(call.args[1], dict)
    ]
    assert not any(
        payload.get("new_status") == "completed"
        or payload.get("event_type") == "process_exit"
        for payload in payloads
    )


@pytest.mark.asyncio
async def test_queued_message_skips_dispatch_claimed_instance(db_factory, monkeypatch):
    """Regression for prod task #676.

    The dispatch loop claims an idle instance for a freshly-dispatched task and
    registers it in _running_tasks, but the instance's DB status only flips to
    "running" once launch() finishes spawning the PTY session. During that
    window the queued-message path used to select the same instance purely by
    DB status=='idle', then its launch killed the dispatch loop's half-started
    session — orphaning the first task in 'executing' with no session (no chat
    button). The queued-message selection must exclude _running_tasks instances.
    """
    d, id1, id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )

    # Dispatch loop has claimed inst1 (not-done lifecycle).
    claimed = asyncio.get_event_loop().create_future()
    d._running_tasks[id1] = claimed
    try:
        await d._process_queued_message(task_id, msg)

        assert d.instance_manager.launch.await_count == 1
        assert d.instance_manager.launch.call_args.kwargs["instance_id"] == id2
        # Transient launch claim released after launch.
        assert id2 not in d._launching_instances
    finally:
        claimed.cancel()


@pytest.mark.asyncio
async def test_queued_message_skips_launching_instance(db_factory, monkeypatch):
    """A concurrent queued-message launch marks its instance in
    _launching_instances; another queued-message must skip it (task #676)."""
    d, id1, id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )

    d._launching_instances.add(id1)

    await d._process_queued_message(task_id, msg)

    assert d.instance_manager.launch.await_count == 1
    assert d.instance_manager.launch.call_args.kwargs["instance_id"] == id2
    # The pre-existing claim on id1 is untouched; id2's transient claim is freed.
    assert id1 in d._launching_instances
    assert id2 not in d._launching_instances


@pytest.mark.asyncio
async def test_reserve_idle_instance_excludes_only_integer_running_keys(
    db_factory,
):
    """Remote-worker lifecycle keys must never enter the integer SQL predicate.

    SQLite silently accepts mixed values in ``Instance.id NOT IN (...)``, while
    PostgreSQL/asyncpg rejects a string such as ``worker-42`` for an integer
    bind parameter.
    """
    d = _make_dispatcher(db_factory)
    remote_lifecycle = asyncio.get_running_loop().create_future()
    d._running_tasks["worker-42"] = remote_lifecycle
    instance = Instance(id=7, name="local-worker", status="idle")

    result = MagicMock()
    result.scalar_one_or_none.return_value = instance
    db = MagicMock()
    db.execute = AsyncMock(return_value=result)

    try:
        reserved, token = await d._reserve_idle_instance(db)

        assert reserved is instance
        assert token is not None
        statement = db.execute.await_args.args[0]
        assert "worker-42" not in repr(statement.compile().params)
        await d._release_instance_reservation(instance.id, token)
        assert not d._launching_instances
    finally:
        remote_lifecycle.cancel()


@pytest.mark.asyncio
async def test_concurrent_task_consumers_reserve_distinct_idle_instances(
    db_factory,
    monkeypatch,
):
    """Two task queues must atomically claim different idle instances.

    Regression for the 2026-07-22 test-environment incident: both consumers
    selected the lowest DB-idle row, then yielded during account resolution
    before either published `_launching_instances`. One launch succeeded; the
    other raised InstanceAlreadyRunningError and its user message was dropped.
    """
    import time
    import backend.api.tasks as tasks_mod
    from backend.services.dispatcher import QueuedMessage, PRIORITY_USER

    monkeypatch.setattr(
        tasks_mod,
        "_find_session_jsonl",
        lambda sid, provider="claude": "/tmp/fake.jsonl",
    )
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst1 = Instance(name="worker-1", status="idle")
        inst2 = Instance(name="worker-2", status="idle")
        db.add_all([inst1, inst2])
        task1 = Task(
            title="t1",
            description="d",
            target_repo="/repo",
            status="completed",
            session_id="sess-1",
        )
        task2 = Task(
            title="t2",
            description="d",
            target_repo="/repo",
            status="completed",
            session_id="sess-2",
        )
        db.add_all([task1, task2])
        await db.commit()
        await db.refresh(inst1)
        await db.refresh(inst2)
        await db.refresh(task1)
        await db.refresh(task2)
        idle_ids = {inst1.id, inst2.id}
        task_ids = (task1.id, task2.id)

    d._resolve_resume_config_dir = AsyncMock(return_value=None)

    async def launch_and_persist_slot(**kwargs):
        async with db_factory() as db:
            await db.execute(
                update(Instance)
                .where(Instance.id == kwargs["instance_id"])
                .values(status="running")
            )
            await db.commit()

    d.instance_manager.launch = AsyncMock(side_effect=launch_and_persist_slot)
    msgs = [
        QueuedMessage(
            priority=PRIORITY_USER,
            timestamp=time.monotonic(),
            prompt=f"message-{index}",
            source="user",
        )
        for index in (1, 2)
    ]

    await asyncio.wait_for(
        asyncio.gather(
            d._process_queued_message(task_ids[0], msgs[0]),
            d._process_queued_message(task_ids[1], msgs[1]),
        ),
        1,
    )

    launch_ids = {
        call.kwargs["instance_id"] for call in d.instance_manager.launch.await_args_list
    }
    assert launch_ids == idle_ids
    assert not d._launching_instances
    assert not d._instance_claim_owners


@pytest.mark.asyncio
async def test_queued_busy_detects_terminal_parent_consumer_and_fresh_lifecycle(
    db_factory,
):
    """Busy is exact durable owner + process/consumer/fresh lifecycle state."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(
            title="busy owner",
            description="d",
            status="executing",
            session_id="session-1",
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="busy-owner",
            status="running",
            pid=99123,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id, instance_id = task.id, instance.id

    terminal_parent = MagicMock(returncode=0)
    consumer = asyncio.create_task(asyncio.Event().wait())
    d.instance_manager.processes[instance_id] = terminal_parent
    d.instance_manager._tasks[instance_id] = consumer
    d.instance_manager._consumer_records = {
        instance_id: MagicMock(
            process=terminal_parent,
            task=consumer,
            task_id=task_id,
        )
    }
    try:
        async with db_factory() as db:
            assert await d._queued_task_has_live_generation(db, task_id)

        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        d.instance_manager._tasks.pop(instance_id, None)
        d.instance_manager.processes.pop(instance_id, None)
        d.instance_manager._consumer_records.pop(instance_id, None)

        lifecycle = asyncio.create_task(asyncio.Event().wait())
        d._running_tasks[instance_id] = lifecycle
        try:
            async with db_factory() as db:
                assert await d._queued_task_has_live_generation(db, task_id)
        finally:
            lifecycle.cancel()
            await asyncio.gather(lifecycle, return_exceptions=True)
    finally:
        if not consumer.done():
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)


@pytest.mark.asyncio
async def test_queued_busy_ignores_lifecycle_that_reused_historical_instance(
    db_factory,
):
    """A related Plan on the last-used slot must not block the main Task."""

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        main_task = Task(
            title="main session",
            description="d",
            status="completed",
            session_id="main-session",
        )
        plan_task = Task(
            title="related plan",
            description="d",
            mode="plan",
            status="executing",
        )
        db.add_all([main_task, plan_task])
        await db.flush()
        instance = Instance(
            name="reused-slot",
            status="running",
            current_task_id=plan_task.id,
        )
        db.add(instance)
        await db.flush()
        main_task.instance_id = instance.id
        plan_task.instance_id = instance.id
        await db.commit()
        main_task_id = main_task.id
        plan_task_id = plan_task.id
        instance_id = instance.id

    lifecycle = asyncio.create_task(asyncio.Event().wait())
    setattr(lifecycle, "_ccm_task_id", plan_task_id)
    d._running_tasks[instance_id] = lifecycle
    try:
        async with db_factory() as db:
            assert not await d._queued_task_has_live_generation(db, main_task_id)
            assert await d._queued_task_has_live_generation(db, plan_task_id)
    finally:
        lifecycle.cancel()
        await asyncio.gather(lifecycle, return_exceptions=True)


@pytest.mark.asyncio
async def test_queued_busy_ignores_stale_consumer_after_instance_reassignment(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        old_task = Task(
            title="completed owner",
            description="d",
            status="completed",
            session_id="old-session",
        )
        new_task = Task(
            title="new owner",
            description="d",
            status="executing",
            session_id="new-session",
        )
        db.add_all([old_task, new_task])
        await db.flush()
        instance = Instance(
            name="reused-slot",
            status="running",
            pid=99124,
            current_task_id=new_task.id,
        )
        db.add(instance)
        await db.flush()
        old_task.instance_id = instance.id
        new_task.instance_id = instance.id
        await db.commit()
        old_task_id, instance_id = old_task.id, instance.id

    live_new_process = MagicMock(returncode=None)
    stale_old_consumer = asyncio.create_task(asyncio.Event().wait())
    d.instance_manager.processes[instance_id] = live_new_process
    d.instance_manager._tasks[instance_id] = stale_old_consumer
    d.instance_manager._consumer_records = {
        instance_id: MagicMock(
            process=MagicMock(returncode=0),
            task=stale_old_consumer,
            task_id=old_task_id,
        )
    }
    try:
        async with db_factory() as db:
            assert not await d._queued_task_has_live_generation(
                db, old_task_id
            )
    finally:
        stale_old_consumer.cancel()
        await asyncio.gather(stale_old_consumer, return_exceptions=True)


@pytest.mark.asyncio
async def test_queued_busy_detects_detached_pty_background_epoch(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(
            title="detached PTY tail",
            description="d",
            status="completed",
            session_id="session-1",
            pty_background_generation="exact-background-epoch",
        )
        db.add(task)
        await db.commit()
        task_id = task.id

        assert await d._queued_task_has_live_generation(db, task_id)

        task.pty_background_generation = None
        await db.commit()
        assert not await d._queued_task_has_live_generation(db, task_id)


@pytest.mark.asyncio
async def test_queued_message_waits_for_detached_pty_background_epoch(
    db_factory,
    monkeypatch,
):
    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        task.instance_id = None
        task.pty_background_generation = "exact-background-epoch"
        await db.commit()

    real_sleep = asyncio.sleep

    async def fast_sleep(delay):
        await real_sleep(0.01 if delay == 2 else 0)

    monkeypatch.setattr("backend.services.dispatcher.asyncio.sleep", fast_sleep)
    queued = asyncio.create_task(d._process_queued_message(task_id, msg))
    try:
        await real_sleep(0.05)
        d.instance_manager.launch.assert_not_awaited()

        async with db_factory() as db:
            task = await db.get(Task, task_id)
            task.pty_background_generation = None
            await db.commit()

        await asyncio.wait_for(queued, timeout=1)
    finally:
        if not queued.done():
            queued.cancel()
            await asyncio.gather(queued, return_exceptions=True)

    d.instance_manager.launch.assert_awaited_once()


@pytest.mark.asyncio
async def test_queued_launch_preserves_background_epoch_started_during_routing(
    db_factory,
    monkeypatch,
):
    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "completed"
        await db.commit()

    routing_started = asyncio.Event()
    release_routing = asyncio.Event()

    async def delayed_routing(*_args, **_kwargs):
        routing_started.set()
        await release_routing.wait()
        return None

    d._resolve_resume_config_dir = AsyncMock(side_effect=delayed_routing)
    queued = asyncio.create_task(d._process_queued_message(task_id, msg))
    await routing_started.wait()
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.pty_background_generation = "routing-race-epoch"
        await db.commit()
    release_routing.set()

    with pytest.raises(
        QueuedMessagePrelaunchError,
        match="generation became active",
    ):
        await queued

    d.instance_manager.launch.assert_not_awaited()
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "completed"
        assert task.pty_background_generation == "routing-race-epoch"


@pytest.mark.asyncio
async def test_queued_message_waits_for_terminal_output_consumer(
    db_factory,
    monkeypatch,
):
    """A parent exit cannot let the next native-session message overlap cleanup."""
    d, id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    release_consumer = asyncio.Event()
    terminal_parent = MagicMock(returncode=0)
    consumer = asyncio.create_task(release_consumer.wait())
    d.instance_manager.processes[id1] = terminal_parent
    d.instance_manager._tasks[id1] = consumer
    d.instance_manager._consumer_records = {
        id1: MagicMock(
            process=terminal_parent,
            task=consumer,
            task_id=task_id,
        )
    }
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, id1)
        task.instance_id = id1
        # The consumer has already cleared reusable-slot metadata but still
        # owns final rollout/account bookkeeping for this exact task.
        instance.status = "idle"
        instance.pid = None
        instance.current_task_id = None
        await db.commit()

    real_sleep = asyncio.sleep

    async def fast_sleep(delay):
        await real_sleep(0.01 if delay == 2 else 0)

    monkeypatch.setattr("backend.services.dispatcher.asyncio.sleep", fast_sleep)
    queued = asyncio.create_task(d._process_queued_message(task_id, msg))
    try:
        await real_sleep(0.05)
        d.instance_manager.launch.assert_not_awaited()
        release_consumer.set()
        await consumer
        d.instance_manager._tasks.pop(id1, None)
        d.instance_manager.processes.pop(id1, None)
        d.instance_manager._consumer_records.pop(id1, None)
        await asyncio.wait_for(queued, timeout=1)
    finally:
        if not queued.done():
            queued.cancel()
            await asyncio.gather(queued, return_exceptions=True)
        if not consumer.done():
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)

    d.instance_manager.launch.assert_awaited_once()


@pytest.mark.asyncio
async def test_queued_owner_cas_race_retries_exact_message(
    db_factory,
    monkeypatch,
):
    """Owner drift after refresh loses the CAS and requeues the same object."""
    from sqlalchemy import update
    from backend.services.dispatcher import QueuedMessagePrelaunchError

    d, _id1, id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    original_execute = AsyncSession.execute
    injected = False
    attempts = 0
    launched = asyncio.Event()

    original_process = d._process_queued_message

    async def counted_process(current_task_id, current_msg):
        nonlocal attempts
        attempts += 1
        try:
            return await original_process(current_task_id, current_msg)
        except QueuedMessagePrelaunchError:
            raise

    async def launch_once(**_kwargs):
        launched.set()

    async def execute_with_owner_race(session, statement, *args, **kwargs):
        nonlocal injected
        table = getattr(statement, "table", None)
        if not injected and getattr(table, "name", None) == "tasks":
            injected = True
            await original_execute(
                session,
                update(Task).where(Task.id == task_id).values(instance_id=id2),
            )
        return await original_execute(session, statement, *args, **kwargs)

    d._process_queued_message = counted_process
    d.instance_manager.launch = AsyncMock(side_effect=launch_once)
    q = d._get_task_queue(task_id)
    await q.put(msg)
    with (
        patch.object(AsyncSession, "execute", new=execute_with_owner_race),
        patch("backend.services.dispatcher.CODEX_ROUTING_RETRY_DELAY", 0),
    ):
        worker = asyncio.create_task(d._task_queue_consumer(task_id))
        d._task_queue_workers[task_id] = worker
        try:
            await asyncio.wait_for(launched.wait(), timeout=2)
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)

    assert injected
    assert attempts >= 2
    assert d.instance_manager.launch.await_count == 1
    assert msg.prompt == "hi"


@pytest.mark.asyncio
async def test_queued_recovery_cas_cannot_revive_concurrent_cancel(
    db_factory,
    monkeypatch,
):
    import backend.api.tasks as tasks_mod
    from sqlalchemy import update
    from backend.services.dispatcher import QueuedMessagePrelaunchError

    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "failed"
        task.last_cwd = "/repo"
        await db.commit()

    async def cancel_during_clone(source_task_id, db):
        assert source_task_id == task_id
        await db.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="cancelled", retry_count=1)
        )
        await db.commit()
        return {"session_id": "cloned-but-superseded", "last_cwd": "/repo"}

    monkeypatch.setattr(tasks_mod, "_clone_session", cancel_during_clone)
    with pytest.raises(QueuedMessagePrelaunchError, match="recovery generation"):
        await d._process_queued_message(task_id, msg)

    d.instance_manager.launch.assert_not_awaited()
    assert msg.prompt == "hi"
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "cancelled"
        assert task.retry_count == 1
        assert task.session_id == "sess-1"


@pytest.mark.asyncio
async def test_queued_recovery_preserves_safe_status_when_routing_stage_wins(
    db_factory,
    monkeypatch,
):
    import backend.api.tasks as tasks_mod

    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory,
        monkeypatch,
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.status = "failed"
        task.last_cwd = "/repo"
        await db.commit()

    marker = {
        "op_id": "stage-during-recovery",
        "provider": "codex",
        "model": "gpt-5.6-sol",
        "codex_service_tier": "priority",
    }

    async def stage_during_clone(source_task_id, db):
        assert source_task_id == task_id
        task = await db.get(Task, task_id, populate_existing=True)
        task.metadata_ = {"worker_routing_config_pending": marker}
        await db.commit()
        return {"session_id": "cloned-but-fenced", "last_cwd": "/repo"}

    monkeypatch.setattr(tasks_mod, "_clone_session", stage_during_clone)

    with pytest.raises(
        QueuedMessagePrelaunchError,
        match="routing configuration synchronization is pending",
    ):
        await d._process_queued_message(task_id, msg)

    d.instance_manager.launch.assert_not_awaited()
    assert msg.prompt == "hi"
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "failed"
        assert task.session_id == "sess-1"
        assert task.metadata_["worker_routing_config_pending"] == marker


@pytest.mark.asyncio
async def test_missing_session_recovery_uses_recency_wrapper(
    db_factory,
    monkeypatch,
):
    import backend.api.tasks as tasks_mod

    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory,
        monkeypatch,
    )
    msg.source_log_id = 654
    monkeypatch.setattr(
        tasks_mod,
        "_find_session_jsonl",
        lambda _sid, provider="claude": None,
    )
    monkeypatch.setattr(
        tasks_mod,
        "_clone_session",
        AsyncMock(return_value=None),
    )
    d._compact_session = AsyncMock(return_value="RECENT_RECOVERY_CONTEXT")

    await d._process_queued_message(task_id, msg)

    d._compact_session.assert_awaited_once()
    assert d._compact_session.await_args.kwargs["exclude_log_entry_id"] == 654
    launch = d.instance_manager.launch.await_args.kwargs
    assert launch["resume_session_id"] is None
    assert "RECENT_RECOVERY_CONTEXT" in launch["prompt"]
    assert "会话异常中断前的历史摘要" in launch["prompt"]
    assert launch["prompt"].endswith("[基础当前消息 — 默认最高优先级]\nhi")
    assert launch["current_message"] == "hi"
    assert launch["source_log_id"] == 654


@pytest.mark.asyncio
async def test_queued_message_never_launches_locally_after_worker_migration(
    db_factory,
    monkeypatch,
):
    from backend.models.log_entry import LogEntry

    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.worker_id = 77
        await db.commit()

    await d._process_queued_message(task_id, msg)

    d.instance_manager.launch.assert_not_awaited()
    async with db_factory() as db:
        notices = (
            (
                await db.execute(
                    select(LogEntry).where(
                        LogEntry.task_id == task_id,
                        LogEntry.event_type == "system_event",
                    )
                )
            )
            .scalars()
            .all()
        )
    assert any("迁移到远程" in (notice.content or "") for notice in notices)


@pytest.mark.asyncio
async def test_requeued_compaction_message_can_start_without_old_session(
    db_factory,
    monkeypatch,
):
    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.session_id = None
        task.status = "in_progress"
        await db.commit()
    msg.allow_new_session = True
    msg.prompt = "[summary]\n\n[new]\ncontinue"

    await d._process_queued_message(task_id, msg)

    d.instance_manager.launch.assert_awaited_once()
    assert d.instance_manager.launch.await_args.kwargs["resume_session_id"] is None


@pytest.mark.asyncio
async def test_codex_precompact_uses_full_context_tokens(
    db_factory,
    monkeypatch,
):
    """Codex output tokens count toward the next turn's context threshold."""

    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    msg.source_log_id = 321
    d._compact_session = AsyncMock(return_value="compact summary")
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.provider = "codex"
        task.model = "gpt-5.6-terra"
        task.context_window_usage = {
            "input_tokens": 20_000,
            "cache_read_input_tokens": 170_000,
            "cache_creation_input_tokens": 0,
            "output_tokens": 20_000,
            "total_input_tokens": 190_000,
            "context_tokens": 210_000,
            "context_window": 258_400,
        }
        await db.commit()

    await d._process_queued_message(task_id, msg)

    d._compact_session.assert_awaited_once()
    assert d._compact_session.await_args.kwargs["exclude_log_entry_id"] == 321
    launch = d.instance_manager.launch.await_args.kwargs
    assert launch["resume_session_id"] is None
    assert "compact summary" in launch["prompt"]
    assert "[基础当前消息 — 默认最高优先级]\nhi" in launch["prompt"]
    assert launch["current_message"] == "hi"
    assert launch["source_log_id"] == 321


@pytest.mark.asyncio
async def test_failed_codex_task_reuses_present_native_thread(db_factory, monkeypatch):
    """A failed turn must not clone/replace a valid Codex rollout."""
    import backend.api.tasks as tasks_mod

    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    clone = AsyncMock()
    monkeypatch.setattr(tasks_mod, "_clone_session", clone)
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.provider = "codex"
        task.status = "failed"
        await db.commit()

    await d._process_queued_message(task_id, msg)

    clone.assert_not_awaited()
    assert d.instance_manager.launch.await_args.kwargs["resume_session_id"] == "sess-1"


@pytest.mark.asyncio
async def test_idle_instance_reservation_is_atomic_across_queued_consumers(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        db.add(Instance(name="only-slot"))
        await db.commit()

    async def reserve():
        async with db_factory() as db:
            return await d._reserve_idle_instance(db)

    first, second = await asyncio.gather(reserve(), reserve())

    winners = [item for item in (first, second) if item[0] is not None]
    assert len(winners) == 1
    instance, token = winners[0]
    assert d._launching_instances == {instance.id}
    await d._release_instance_reservation(instance.id, token)
    assert not d._launching_instances


@pytest.mark.asyncio
async def test_idle_allocator_enforces_lowered_cap_without_killing_running(
    db_factory,
):
    import backend.services.dispatcher as dispatcher_mod

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        running = Instance(name="running", status="running", pid=123)
        db.add(running)
        db.add_all(
            [
                Instance(name="legacy-idle-1"),
                Instance(name="legacy-idle-2"),
            ]
        )
        await db.commit()
        await db.refresh(running)

    with patch.object(dispatcher_mod.settings, "max_concurrent_instances", 1):
        async with db_factory() as db:
            assert await d._reserve_idle_instance(db) == (None, None)

    async with db_factory() as db:
        assert (await db.get(Instance, running.id)).status == "running"
        assert (await db.get(Instance, running.id)).pid == 123


@pytest.mark.asyncio
async def test_active_local_instance_ids_excludes_distributed_worker_keys(
    db_factory,
):
    """Worker string keys must never reach integer Instance SQL predicates."""
    d = _make_dispatcher(db_factory)
    local = asyncio.get_running_loop().create_future()
    remote = asyncio.get_running_loop().create_future()
    d._running_tasks[2] = local
    d._running_tasks["worker-99"] = remote
    try:
        assert d._active_local_instance_ids() == {2}
    finally:
        local.cancel()
        remote.cancel()


@pytest.mark.asyncio
async def test_abort_task_queue_cancels_message_already_removed_by_get(db_factory):
    """stop-session cannot let an in-flight admission launch afterwards."""
    d = _make_dispatcher(db_factory)
    started = asyncio.Event()
    release = asyncio.Event()
    reached_launch = asyncio.Event()

    async def paused_process(_task_id, _msg):
        started.set()
        await release.wait()
        reached_launch.set()

    d._process_queued_message = paused_process
    await d.enqueue_message(11, "exact user text", source="user")
    await asyncio.wait_for(started.wait(), timeout=1)

    cleared = await d.abort_task_queue(11)
    release.set()
    await asyncio.sleep(0)

    assert cleared == 0  # item was already held by the consumer
    assert not reached_launch.is_set()
    assert 11 not in d._task_queue_workers


@pytest.mark.asyncio
async def test_abort_task_queue_timeout_retains_worker_evidence(db_factory):
    from backend.services.dispatcher import TaskQueueAbortTimeoutError

    d = _make_dispatcher(db_factory)
    release = asyncio.Event()

    async def ignores_first_cancellation():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()

    worker = asyncio.create_task(ignores_first_cancellation())
    d._task_queue_workers[12] = worker
    await asyncio.sleep(0)
    try:
        with pytest.raises(TaskQueueAbortTimeoutError, match="did not stop"):
            await d.abort_task_queue(12, timeout=0.01)
        assert d._task_queue_workers[12] is worker
        assert not worker.done()
    finally:
        release.set()
        await asyncio.wait_for(worker, timeout=1)
        if d._task_queue_workers.get(12) is worker:
            d._task_queue_workers.pop(12, None)


@pytest.mark.asyncio
async def test_capacity_race_requeues_exact_queued_message(db_factory):
    from backend.services.dispatcher import (
        InstanceAlreadyRunningError,
        QueuedMessage,
        PRIORITY_USER,
    )
    import time

    d = _make_dispatcher(db_factory)
    msg = QueuedMessage(
        priority=PRIORITY_USER,
        timestamp=time.monotonic(),
        prompt="do not drop this exact text",
        source="user",
    )
    q = d._get_task_queue(33)
    await q.put(msg)
    seen: list[QueuedMessage] = []
    processed = asyncio.Event()

    async def race_then_succeed(_task_id, current):
        seen.append(current)
        if len(seen) == 1:
            raise InstanceAlreadyRunningError("slot was claimed")
        processed.set()

    d._process_queued_message = race_then_succeed
    with patch("backend.services.dispatcher.CODEX_ROUTING_RETRY_DELAY", 0):
        worker = asyncio.create_task(d._task_queue_consumer(33))
        d._task_queue_workers[33] = worker
        await asyncio.wait_for(processed.wait(), timeout=1)
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)

    assert seen == [msg, msg]
    assert seen[1].prompt == "do not drop this exact text"


@pytest.mark.asyncio
async def test_fresh_lifecycle_capacity_race_releases_task_claim(db_factory):
    from backend.services.dispatcher import InstanceAlreadyRunningError

    d = _make_dispatcher(db_factory)
    d.instance_manager.launch = AsyncMock(
        side_effect=InstanceAlreadyRunningError("slot already claimed")
    )
    d.instance_manager._instance_lifecycle_lock = MagicMock(return_value=asyncio.Lock())
    async with db_factory() as db:
        inst = Instance(name="race-slot")
        task = Task(title="race", description="d", status="pending")
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id, task_id, task_obj = inst.id, task.id, task

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "pending"
        assert task.instance_id is None
        assert task.retry_count == 0


@pytest.mark.asyncio
async def test_superseded_lifecycle_cannot_reclaim_cancelled_then_retried_task(
    db_factory,
):
    """An old coroutine must not launch after cancel → retry clears its claim."""
    from backend.services.task_queue import TaskQueue

    d = _make_dispatcher(db_factory)
    lifecycle_entered = asyncio.Event()
    release_lifecycle = asyncio.Event()

    async def hold_initial_broadcast(_channel, payload):
        if (
            payload.get("event") == "status_change"
            and payload.get("old_status") == "pending"
            and payload.get("new_status") == "in_progress"
        ):
            lifecycle_entered.set()
            await release_lifecycle.wait()

    d.broadcaster.broadcast.side_effect = hold_initial_broadcast
    async with db_factory() as db:
        instance = Instance(name="superseded-slot")
        task = Task(
            title="superseded",
            description="d",
            status="in_progress",
        )
        db.add_all([instance, task])
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        instance_id, task_id, task_obj = instance.id, task.id, task

    lifecycle = asyncio.create_task(d._run_task_lifecycle(instance_id, task_obj))
    await asyncio.wait_for(lifecycle_entered.wait(), timeout=1)
    async with db_factory() as db:
        queue = TaskQueue(db)
        assert await queue.cancel(task_id) is not None
        assert await queue.retry(task_id) is not None

    release_lifecycle.set()
    await asyncio.wait_for(lifecycle, timeout=1)

    d.instance_manager.launch.assert_not_awaited()
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "pending"
        assert task.instance_id is None


@pytest.mark.asyncio
async def test_stale_mode_entry_cannot_reclaim_cancelled_then_retried_task(
    db_factory,
):
    from backend.services.task_queue import TaskQueue

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(name="stale-mode-slot")
        task = Task(
            title="stale mode",
            description="d",
            mode="loop",
            status="in_progress",
        )
        db.add_all([instance, task])
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id = task.id
        lifecycle_generation = d._task_lifecycle_generation(task)

    async with db_factory() as db:
        queue = TaskQueue(db)
        assert await queue.cancel(task_id) is not None
        assert await queue.retry(task_id) is not None

    assert not await d._ensure_owned_executing(lifecycle_generation)
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "pending"
        assert task.instance_id is None


@pytest.mark.asyncio
async def test_old_lifecycle_cannot_finalize_same_task_same_slot_reclaim(
    db_factory,
):
    """ensure/retry/complete/fail all reject cancel->retry->same-slot ABA."""

    from datetime import datetime

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(name="same-slot-finalizers")
        db.add(instance)
        await db.flush()
        task = Task(
            title="old-finalizer",
            status="executing",
            retry_count=0,
            max_retries=2,
            instance_id=instance.id,
        )
        db.add(task)
        await db.commit()
        old_generation = d._task_lifecycle_generation(task)
        replacement_started_at = datetime.utcnow()
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(
                retry_count=1,
                started_at=replacement_started_at,
            )
        )
        await db.commit()
        task_id = task.id

    assert not await d._ensure_owned_executing(old_generation)
    assert await d._retry_or_fail_mode_task(old_generation, "stale retry") is None
    assert not await d._complete_owned_task(
        old_generation,
        count_completion=True,
    )
    assert not await d._fail_owned_task(old_generation, "stale failure")

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, old_generation.instance_id)
        assert task.status == "executing"
        assert task.retry_count == 1
        assert task.started_at == replacement_started_at
        assert instance.total_tasks_completed == 0


@pytest.mark.asyncio
async def test_lifecycle_double_cancel_waits_for_reset_before_registry_pop(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(name="shielded-final-reset")
        task = Task(
            title="shielded-final-reset",
            description="done",
            target_repo="/repo",
        )
        db.add_all([instance, task])
        await db.commit()
        await db.refresh(instance)
        await db.refresh(task)
        await db.execute(
            update(Task)
            .where(Task.id == task.id)
            .values(status="in_progress", instance_id=instance.id)
        )
        await db.commit()
        task.status = "in_progress"
        task.instance_id = instance.id
        instance_id = instance.id

    process = MagicMock(returncode=0, wait=AsyncMock(return_value=0))
    d.instance_manager.processes[instance_id] = process
    reset_entered = asyncio.Event()
    release_reset = asyncio.Event()

    async def blocked_reset(*_args, **_kwargs):
        reset_entered.set()
        await release_reset.wait()

    d._reset_instance_if_stale = AsyncMock(side_effect=blocked_reset)
    lifecycle = asyncio.create_task(d._run_task_lifecycle(instance_id, task))
    d._running_tasks[instance_id] = lifecycle
    await asyncio.wait_for(reset_entered.wait(), timeout=1)

    lifecycle.cancel()
    await asyncio.sleep(0)
    lifecycle.cancel()
    await asyncio.sleep(0)
    assert d._running_tasks.get(instance_id) is lifecycle

    release_reset.set()
    with pytest.raises(asyncio.CancelledError):
        await lifecycle
    assert instance_id not in d._running_tasks
    d._reset_instance_if_stale.assert_awaited_once()


@pytest.mark.asyncio
async def test_supersede_marker_blocks_all_lifecycle_finalizers(db_factory):
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(name="superseded-finalizers")
        db.add(instance)
        await db.flush()
        task = Task(
            title="superseded-finalizers",
            status="executing",
            instance_id=instance.id,
            metadata_={"pr_review_superseded": True},
        )
        db.add(task)
        await db.commit()
        generation = d._task_lifecycle_generation(task)
        task_id = task.id

    assert not await d._ensure_owned_executing(generation)
    assert await d._retry_or_fail_mode_task(generation, "blocked") is None
    assert not await d._complete_owned_task(generation)
    assert not await d._fail_owned_task(generation, "blocked")
    async with db_factory() as db:
        assert (await db.get(Task, task_id)).status == "executing"


@pytest.mark.asyncio
async def test_stale_pr_failure_cannot_overwrite_superseded_review(db_factory):
    from backend.models.pr_monitor import MonitoredRepo, PRReview

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(title="old-review", metadata_={})
        repo = MonitoredRepo(
            repo_full_name="owner/repo-stale-failure",
            webhook_secret="secret",
        )
        db.add_all([task, repo])
        await db.flush()
        review = PRReview(
            repo_id=repo.id,
            pr_number=7,
            pr_title="old",
            pr_author="author",
            pr_url="https://example.test/pr/7",
            task_id=task.id,
            status="superseded",
        )
        db.add(review)
        await db.flush()
        task.metadata_ = {"pr_review_id": review.id}
        await db.commit()
        review_id = review.id

    await d._handle_pr_review_failure(task, "late failure")

    async with db_factory() as db:
        review = await db.get(PRReview, review_id)
        assert review.status == "superseded"
        assert review.action_taken is None
    d.broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_pr_failure_cannot_overwrite_retried_generation(db_factory):
    from backend.models.pr_monitor import MonitoredRepo, PRReview

    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(
            title="retried-review",
            status="failed",
            retry_count=0,
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            metadata_={},
        )
        repo = MonitoredRepo(
            repo_full_name="owner/repo-retried-failure",
            webhook_secret="secret",
        )
        db.add_all([task, repo])
        await db.flush()
        review = PRReview(
            repo_id=repo.id,
            pr_number=8,
            pr_title="retry",
            pr_author="author",
            pr_url="https://example.test/pr/8",
            task_id=task.id,
            status="reviewing",
        )
        db.add(review)
        await db.flush()
        task.metadata_ = {"pr_review_id": review.id}
        await db.commit()
        task_id = task.id
        review_id = review.id
        stale_task = task

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        current.status = "pending"
        current.retry_count = 1
        current.started_at = None
        current.completed_at = None
        await db.commit()

    await d._handle_pr_review_failure(stale_task, "late retry-0 failure")

    async with db_factory() as db:
        current = await db.get(Task, task_id)
        review = await db.get(PRReview, review_id)
        assert current.status == "pending"
        assert current.retry_count == 1
        assert review.status == "reviewing"
        assert review.action_taken is None
    d.broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_dispatcher_pause_preserves_live_lifecycle_and_process(
    db_factory,
):
    """Runtime Stop is admission pause; the current turn finishes normally."""
    d = _make_dispatcher(db_factory)
    turn_started = asyncio.Event()
    finish_turn = asyncio.Event()

    class Process:
        pid = 4321
        returncode = None

        async def wait(self):
            turn_started.set()
            await finish_turn.wait()
            self.returncode = 0
            return 0

    process = Process()
    async with db_factory() as db:
        inst = Instance(name="pause-slot")
        task = Task(title="pause-task", description="d", status="pending")
        db.add_all([inst, task])
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id, task_id = inst.id, task.id
        task.status = "in_progress"
        task.instance_id = inst_id
        await db.commit()
        task_obj = task

    d.instance_manager.processes[inst_id] = process
    d.instance_manager._instance_lifecycle_lock = MagicMock(return_value=asyncio.Lock())
    lifecycle = asyncio.create_task(d._run_task_lifecycle(inst_id, task_obj))
    d._running_tasks[inst_id] = lifecycle
    await asyncio.wait_for(turn_started.wait(), timeout=1)

    d._running = True
    d._dispatch_task = asyncio.create_task(asyncio.sleep(60))
    d._curator_task = asyncio.create_task(asyncio.sleep(60))
    d.instance_manager.stop = AsyncMock()
    await d.stop()

    assert not lifecycle.done()
    assert d._running_tasks[inst_id] is lifecycle
    assert process.returncode is None
    d.instance_manager.stop.assert_not_awaited()

    finish_turn.set()
    await asyncio.wait_for(lifecycle, timeout=1)
    assert inst_id not in d._running_tasks
    async with db_factory() as db:
        assert (await db.get(Task, task_id)).status == "completed"


@pytest.mark.asyncio
async def test_pause_then_start_preserves_manager_owned_claim(db_factory):
    """Restarting admission in-process must not recover or duplicate live work."""
    d = _make_dispatcher(db_factory)
    process = MagicMock(returncode=None, pid=777)
    consumer = asyncio.create_task(asyncio.sleep(60))

    async with db_factory() as db:
        task = Task(title="live", description="d", status="executing")
        db.add(task)
        await db.flush()
        inst = Instance(
            name="live-slot",
            status="running",
            pid=777,
            current_task_id=task.id,
        )
        db.add(inst)
        await db.flush()
        task.instance_id = inst.id
        await db.commit()
        inst_id, task_id = inst.id, task.id

    d.instance_manager.processes[inst_id] = process
    d.instance_manager._tasks[inst_id] = consumer
    existing_lifecycle = asyncio.create_task(asyncio.sleep(60))
    d._running_tasks[inst_id] = existing_lifecycle
    d._running = True
    d._dispatch_task = asyncio.create_task(asyncio.sleep(60))
    d._curator_task = asyncio.create_task(asyncio.sleep(60))
    await d.stop()

    d._dispatch_loop = AsyncMock(side_effect=lambda: None)
    d._curator_loop = AsyncMock(side_effect=lambda: None)
    d._ensure_instances = AsyncMock()
    await d.start()
    await asyncio.sleep(0)

    async with db_factory() as db:
        assert (await db.get(Instance, inst_id)).status == "running"
        task = await db.get(Task, task_id)
        assert task.status == "executing"
        assert task.instance_id == inst_id
    assert d._running_tasks[inst_id] is existing_lifecycle

    existing_lifecycle.cancel()
    consumer.cancel()
    await asyncio.gather(existing_lifecycle, consumer, return_exceptions=True)
    await d.stop()


@pytest.mark.asyncio
async def test_start_cleanup_failure_rolls_back_running_and_is_retryable(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    d._cleanup_stale_state = AsyncMock(
        side_effect=[RuntimeError("temporary database failure"), None]
    )
    d._ensure_instances = AsyncMock()
    d._dispatch_loop = AsyncMock()
    d._curator_loop = AsyncMock()

    with pytest.raises(RuntimeError, match="temporary database failure"):
        await d.start()
    assert d.is_running is False

    await d.start()
    await asyncio.sleep(0)
    assert d.is_running is True
    assert d._cleanup_stale_state.await_count == 2
    await d.stop()


@pytest.mark.asyncio
async def test_start_reconciliation_fences_queued_chat_spawn(
    db_factory,
    monkeypatch,
):
    """No queued child may spawn after cleanup's ownership snapshot."""
    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()
    launch_seen = asyncio.Event()

    async def held_cleanup():
        cleanup_started.set()
        await release_cleanup.wait()
        cleanup_finished.set()

    async def launch_after_cleanup(**_kwargs):
        assert cleanup_finished.is_set()
        launch_seen.set()

    d._cleanup_stale_state = AsyncMock(side_effect=held_cleanup)
    d._ensure_instances = AsyncMock()
    d._dispatch_loop = AsyncMock()
    d._curator_loop = AsyncMock()
    d.instance_manager.launch = AsyncMock(side_effect=launch_after_cleanup)

    starter = asyncio.create_task(d.start())
    queued = None
    try:
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        queued = asyncio.create_task(d._process_queued_message(task_id, msg))
        await asyncio.sleep(0)
        d.instance_manager.launch.assert_not_awaited()

        release_cleanup.set()
        await asyncio.wait_for(starter, timeout=1)
        await asyncio.wait_for(queued, timeout=1)
        assert launch_seen.is_set()
    finally:
        release_cleanup.set()
        for task in (starter, queued):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        if d.is_running:
            await d.stop()


@pytest.mark.asyncio
async def test_start_waits_for_inflight_queued_admission_before_snapshot(
    db_factory,
    monkeypatch,
):
    """An already-admitted queued spawn becomes visible before cleanup scans."""
    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    routing_started = asyncio.Event()
    release_routing = asyncio.Event()
    launch_seen = asyncio.Event()
    cleanup_seen = asyncio.Event()

    async def held_routing(*_args, **_kwargs):
        routing_started.set()
        await release_routing.wait()
        return None

    async def launch_before_cleanup(**_kwargs):
        launch_seen.set()

    async def cleanup_after_launch():
        assert launch_seen.is_set()
        cleanup_seen.set()

    d._resolve_resume_config_dir = AsyncMock(side_effect=held_routing)
    d.instance_manager.launch = AsyncMock(side_effect=launch_before_cleanup)
    d._cleanup_stale_state = AsyncMock(side_effect=cleanup_after_launch)
    d._ensure_instances = AsyncMock()
    d._dispatch_loop = AsyncMock()
    d._curator_loop = AsyncMock()

    queued = asyncio.create_task(d._process_queued_message(task_id, msg))
    starter = None
    try:
        await asyncio.wait_for(routing_started.wait(), timeout=1)
        starter = asyncio.create_task(d.start())
        await asyncio.sleep(0)
        d._cleanup_stale_state.assert_not_awaited()

        release_routing.set()
        await asyncio.wait_for(queued, timeout=1)
        await asyncio.wait_for(starter, timeout=1)
        assert cleanup_seen.is_set()
    finally:
        release_routing.set()
        for task in (queued, starter):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        if d.is_running:
            await d.stop()


@pytest.mark.asyncio
async def test_dispatcher_shutdown_quiesces_before_reaping_generations(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    d._running = True
    d._dispatch_task = asyncio.create_task(asyncio.sleep(60))
    d._curator_task = asyncio.create_task(asyncio.sleep(60))
    lifecycle = asyncio.create_task(asyncio.sleep(60))
    d._running_tasks[4] = lifecycle

    queue_started = asyncio.Event()

    async def held_message(_task_id, _msg):
        queue_started.set()
        await asyncio.sleep(60)

    d._process_queued_message = held_message
    await d.enqueue_message(22, "held")
    await asyncio.wait_for(queue_started.wait(), timeout=1)

    d.instance_manager.processes[4] = MagicMock(returncode=None)

    async def stop_generation(instance_id, **_kwargs):
        d.instance_manager.processes.pop(instance_id, None)
        return True

    d.instance_manager.stop = AsyncMock(side_effect=stop_generation)

    with patch(
        "backend.services.skill_distill.reap_unreaped_task_distills",
        new=AsyncMock(),
    ) as reap_distills:
        await d.shutdown()

    assert lifecycle.cancelled()
    reap_distills.assert_awaited_once_with()
    assert 22 not in d._task_queue_workers
    d.instance_manager.stop.assert_awaited_once_with(
        4,
        expected_task_id=None,
        expected_pid=None,
        expected_started_at=None,
        task_status="pending",
        terminal_consumer_timeout=10,
        consumer_cancel_timeout=5,
    )
    assert not d._running_tasks
    with pytest.raises(RuntimeError, match="shutting down"):
        await d.enqueue_message(22, "too late")
    with pytest.raises(RuntimeError, match="shutting down"):
        await d.start()


async def _seed_detached_shutdown_generation(
    dispatcher,
    db_factory,
    *,
    stop_fails: bool = False,
):
    """Install one real ownerless PTY generation behind a test Dispatcher."""

    from backend.services.instance_manager import InstanceManager

    session_id = "detached-shutdown-session"
    generation = "detached-shutdown-generation"
    completed_at = datetime.now()
    async with db_factory() as db:
        task = Task(
            title="detached PTY shutdown",
            description="background tail",
            status="completed",
            session_id=session_id,
            completed_at=completed_at,
            pty_background_generation=generation,
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    instance_manager = InstanceManager(db_factory, dispatcher.broadcaster)
    session = SimpleNamespace(
        session_id=session_id,
        is_alive=True,
    )

    async def stop_session():
        if stop_fails:
            raise RuntimeError("native PTY session survived")
        session.is_alive = False

    session.stop = AsyncMock(side_effect=stop_session)
    pool = SimpleNamespace(
        _sessions={session_id: session},
        _access_order={session_id: 1.0},
        _lock=asyncio.Lock(),
    )
    instance_manager._pty_backend = SimpleNamespace(
        _sessions={},
        _pool=pool,
    )
    state = instance_manager.register_pty_background_generation(
        task_id,
        session_id,
        generation,
        session,
    )
    dispatcher.instance_manager = instance_manager
    return task_id, session_id, generation, session, state


@pytest.mark.asyncio
async def test_shutdown_exactly_fails_detached_pty_background_generation(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    (
        task_id,
        session_id,
        _generation,
        session,
        state,
    ) = await _seed_detached_shutdown_generation(d, db_factory)
    watcher = state.watcher

    await d.shutdown()
    if watcher is not None:
        await asyncio.gather(watcher, return_exceptions=True)

    session.stop.assert_awaited_once_with()
    assert not session.is_alive
    assert (task_id, session_id) not in (d.instance_manager._pty_background_states)
    assert session_id not in d.instance_manager._pty_backend._pool._sessions
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "failed"
        assert task.pty_background_generation is None
        assert task.error_message == (
            "Claude PTY background activity was interrupted by dispatcher shutdown"
        )


@pytest.mark.asyncio
async def test_shutdown_fails_closed_when_detached_pty_session_survives(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    (
        task_id,
        session_id,
        generation,
        session,
        state,
    ) = await _seed_detached_shutdown_generation(
        d,
        db_factory,
        stop_fails=True,
    )
    watcher = state.watcher

    try:
        with pytest.raises(
            RuntimeError,
            match=("detached PTY background task .* generation survived"),
        ):
            await d.shutdown()

        session.stop.assert_awaited_once_with()
        assert session.is_alive
        assert d.instance_manager._pty_background_states[(task_id, session_id)] is state
        assert state.accepting_events
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            assert task.status == "completed"
            assert task.pty_background_generation == generation
            assert task.error_message is None
    finally:
        d.instance_manager._discard_pty_background_state(
            (task_id, session_id),
            generation,
        )
        if watcher is not None:
            await asyncio.gather(watcher, return_exceptions=True)


@pytest.mark.asyncio
async def test_shutdown_fails_closed_for_unmanaged_attached_pty_state(
    db_factory,
):
    """A stale DB owner cannot hide a retained Session from shutdown."""

    d = _make_dispatcher(db_factory)
    (
        task_id,
        session_id,
        generation,
        session,
        state,
    ) = await _seed_detached_shutdown_generation(d, db_factory)
    watcher = state.watcher

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        owner = Instance(
            name="lost-attached-owner",
            status="running",
            pid=998877,
            current_task_id=task_id,
        )
        db.add(owner)
        await db.flush()
        task.instance_id = owner.id
        await db.commit()
        owner_id = owner.id

    try:
        with pytest.raises(
            RuntimeError,
            match=(f"PTY background task .* remained attached to instance {owner_id}"),
        ):
            await d.shutdown()

        session.stop.assert_not_awaited()
        assert session.is_alive
        assert d.instance_manager._pty_background_states[(task_id, session_id)] is state
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            assert task.status == "completed"
            assert task.pty_background_generation == generation
            assert task.instance_id == owner_id
    finally:
        d.instance_manager._discard_pty_background_state(
            (task_id, session_id),
            generation,
        )
        if watcher is not None:
            await asyncio.gather(watcher, return_exceptions=True)


@pytest.mark.asyncio
async def test_dispatcher_stop_timeout_retains_producer_task(db_factory):
    d = _make_dispatcher(db_factory)
    d._running = True
    release = asyncio.Event()

    async def ignores_first_cancellation():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()

    producer = asyncio.create_task(ignores_first_cancellation())
    d._dispatch_task = producer
    await asyncio.sleep(0)
    try:
        with pytest.raises(RuntimeError, match="dispatch loop"):
            await d.stop(timeout=0.01)
        assert d._dispatch_task is producer
        assert not producer.done()
    finally:
        release.set()
        await asyncio.wait_for(producer, timeout=1)


@pytest.mark.asyncio
async def test_shutdown_queue_timeout_still_reaps_manager_generation(
    db_factory,
):
    from backend.services.dispatcher import TaskQueueAbortTimeoutError

    d = _make_dispatcher(db_factory)
    d.stop = AsyncMock()
    d._task_queue_workers = {10: MagicMock(), 11: MagicMock()}

    async def abort(task_id):
        if task_id == 10:
            raise TaskQueueAbortTimeoutError("stubborn queue")
        return 0

    d.abort_task_queue = AsyncMock(side_effect=abort)
    process = MagicMock(pid=8810, returncode=None)
    d.instance_manager.processes[8] = process

    async def reap(instance_id, **_kwargs):
        assert instance_id == 8
        process.returncode = -9
        d.instance_manager.processes.pop(instance_id, None)
        return True

    d.instance_manager.stop = AsyncMock(side_effect=reap)
    d.instance_manager._generation_reap_confirmed = MagicMock(
        side_effect=lambda _instance_id, candidate: candidate.returncode is not None
    )

    with pytest.raises(RuntimeError, match="queue cleanup failed"):
        await d.shutdown()

    assert d.abort_task_queue.await_count == 2
    d.instance_manager.stop.assert_awaited_once()
    assert 8 not in d.instance_manager.processes


@pytest.mark.asyncio
async def test_shutdown_lifecycle_timeout_still_reaps_exact_generation(
    db_factory, monkeypatch
):
    d = _make_dispatcher(db_factory)
    d.stop = AsyncMock()
    release = asyncio.Event()

    async def ignores_first_cancellation():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()

    lifecycle = asyncio.create_task(ignores_first_cancellation())
    d._running_tasks[9] = lifecycle
    process = MagicMock(pid=9909, returncode=None)
    d.instance_manager.processes[9] = process

    async def reap(instance_id, **_kwargs):
        assert instance_id == 9
        process.returncode = -9
        d.instance_manager.processes.pop(instance_id, None)
        return True

    d.instance_manager.stop = AsyncMock(side_effect=reap)
    d.instance_manager._generation_reap_confirmed = MagicMock(
        side_effect=lambda _instance_id, candidate: candidate.returncode is not None
    )
    monkeypatch.setattr(
        "backend.services.dispatcher.SHUTDOWN_LIFECYCLE_CANCEL_TIMEOUT",
        0.01,
    )
    await asyncio.sleep(0)
    try:
        with pytest.raises(RuntimeError, match="ignored cancellation"):
            await d.shutdown()
        d.instance_manager.stop.assert_awaited_once()
        assert 9 not in d.instance_manager.processes
        assert d._running_tasks[9] is lifecycle
    finally:
        release.set()
        await asyncio.wait_for(lifecycle, timeout=1)


@pytest.mark.asyncio
async def test_shutdown_returns_process_free_prelaunch_claim_to_pending(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    routing_started = asyncio.Event()

    async def hold_routing(*_args, **_kwargs):
        routing_started.set()
        await asyncio.sleep(60)

    d._resolve_resume_config_dir = AsyncMock(side_effect=hold_routing)
    async with db_factory() as db:
        instance = Instance(name="prelaunch-shutdown")
        task = Task(
            title="prelaunch",
            description="d",
            status="in_progress",
        )
        db.add_all([instance, task])
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        instance_id, task_id, task_obj = instance.id, task.id, task

    lifecycle = asyncio.create_task(d._run_task_lifecycle(instance_id, task_obj))
    d._running_tasks[instance_id] = lifecycle
    await asyncio.wait_for(routing_started.wait(), timeout=1)

    await d.shutdown()

    assert lifecycle.cancelled()
    d.instance_manager.launch.assert_not_awaited()
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "pending"
        assert task.instance_id is None


@pytest.mark.asyncio
async def test_shutdown_failed_reap_preserves_live_task_owner(db_factory):
    """A failed stop must never expose a live generation as pending work."""
    d = _make_dispatcher(db_factory)
    async with db_factory() as db:
        task = Task(title="live shutdown", description="d", status="executing")
        db.add(task)
        await db.flush()
        instance = Instance(
            name="live-shutdown",
            status="running",
            pid=81234,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        instance_id, task_id = instance.id, task.id

    process = MagicMock(pid=81234, returncode=None)
    d.instance_manager.processes[instance_id] = process
    d.instance_manager.stop = AsyncMock(
        side_effect=RuntimeError("cannot prove process exit")
    )
    d.instance_manager._generation_reap_confirmed = MagicMock(
        side_effect=lambda _instance_id, expected: expected.returncode is not None
    )

    async def kill_exact(_instance_id, expected, **_kwargs):
        assert expected is process
        expected.returncode = -9
        return True

    d.instance_manager.kill_process_generation = AsyncMock(side_effect=kill_exact)
    lifecycle = asyncio.create_task(asyncio.sleep(60))
    d._running_tasks[instance_id] = lifecycle

    await d.shutdown()

    assert lifecycle.cancelled()
    d.instance_manager.kill_process_generation.assert_awaited_once_with(
        instance_id,
        process,
        timeout=5,
    )
    d.instance_manager.stop.assert_awaited_once_with(
        instance_id,
        expected_task_id=task_id,
        expected_pid=81234,
        expected_started_at=instance.started_at,
        task_status="pending",
        terminal_consumer_timeout=10,
        consumer_cancel_timeout=5,
    )
    async with db_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "executing"
        assert task.instance_id == instance_id
        assert instance.status == "running"
        assert instance.pid == 81234
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_shutdown_fails_explicitly_when_exact_generation_survives(
    db_factory,
):
    d = _make_dispatcher(db_factory)
    process = MagicMock(pid=81_235, returncode=None)
    d.instance_manager.processes[5] = process
    d.instance_manager.stop = AsyncMock(
        side_effect=RuntimeError("DB owner unavailable")
    )
    d.instance_manager._generation_reap_confirmed = MagicMock(return_value=False)
    d.instance_manager.kill_process_generation = AsyncMock(
        side_effect=RuntimeError("SIGKILL proof failed")
    )

    with pytest.raises(RuntimeError, match="exact process generation survived"):
        await d.shutdown()

    assert d.instance_manager.processes[5] is process


@pytest.mark.asyncio
async def test_queued_codex_turn_never_runs_claude_pty_finalizer(
    db_factory,
    monkeypatch,
):
    """Global PTY enablement must not synthesize Codex completion/exit events."""
    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    d._resolve_resume_config_dir = AsyncMock(return_value="/tmp/codex-a")
    d.instance_manager.pty_mode_enabled = True
    d.instance_manager.pty_rate_limit_seen.return_value = True
    d.instance_manager.transient_error_seen.return_value = True

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.provider = "codex"
        await db.commit()

    await d._process_queued_message(task_id, msg)

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        assert task.status == "executing"
    d.instance_manager._try_proactive_pool_switch.assert_not_awaited()
    assert not any(
        call.args[1].get("event_type") == "process_exit"
        for call in d.broadcaster.broadcast.await_args_list
        if len(call.args) > 1 and isinstance(call.args[1], dict)
    )


@pytest.mark.asyncio
async def test_queued_pty_turn_does_not_start_second_transient_retry_owner(
    db_factory,
    monkeypatch,
):
    """FullMirror owns retries; the queue only waits on its chained proxy."""

    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    process = MagicMock(returncode=1)

    async def launch(**kwargs):
        d.instance_manager.processes[kwargs["instance_id"]] = process

    d.instance_manager.launch = AsyncMock(side_effect=launch)
    d.instance_manager.pty_mode_enabled = False
    d.instance_manager.is_pty_managed_turn.return_value = True
    d.instance_manager.transient_error_seen.return_value = True
    d.instance_manager._try_chat_transient_retry = AsyncMock(return_value=True)
    d._wait_process = AsyncMock()

    await d._process_queued_message(task_id, msg)

    d.instance_manager._try_chat_transient_retry.assert_not_awaited()
    d.instance_manager.is_pty_managed_turn.assert_called()


@pytest.mark.asyncio
async def test_queued_codex_message_waits_for_replacement_turn_chain(
    db_factory,
    monkeypatch,
):
    """A rotation/retry launched by output cleanup stays inside queue serialization."""
    d, _id1, _id2, task_id, msg = await _setup_queued_msg_two_idle(
        db_factory, monkeypatch
    )
    d._resolve_resume_config_dir = AsyncMock(return_value="/tmp/codex-a")
    first = MagicMock(name="first-turn", returncode=1)
    second = MagicMock(name="replacement-turn", returncode=0)
    first_waited = asyncio.Event()
    second_waited = asyncio.Event()
    replacement_finished = asyncio.Event()

    async with db_factory() as db:
        task = await db.get(Task, task_id)
        task.provider = "codex"
        await db.commit()

    async def fake_launch(**kwargs):
        instance_id = kwargs["instance_id"]

        async def replacement_consumer():
            await second_waited.wait()
            d.instance_manager.processes.pop(instance_id, None)
            d.instance_manager._tasks.pop(instance_id, None)
            replacement_finished.set()

        async def first_consumer():
            await first_waited.wait()
            d.instance_manager.processes[instance_id] = second
            d.instance_manager._tasks[instance_id] = asyncio.create_task(
                replacement_consumer()
            )

        d.instance_manager.processes[instance_id] = first
        d.instance_manager._tasks[instance_id] = asyncio.create_task(first_consumer())

    async def fake_wait(process, _task, _label, **_kwargs):
        if process is first:
            first_waited.set()
        elif process is second:
            second_waited.set()

    d.instance_manager.launch = AsyncMock(side_effect=fake_launch)
    d._wait_process = AsyncMock(side_effect=fake_wait)

    await d._process_queued_message(task_id, msg)

    assert replacement_finished.is_set()
    assert [call.args[0] for call in d._wait_process.await_args_list] == [
        first,
        second,
    ]


# === Codex provider prompt tests (provider-aware agent doc) ===


@pytest.mark.asyncio
async def test_goal_initial_prompt_codex_references_agents_md(db_factory):
    """Codex goal tasks are pointed at AGENTS.md instead of CLAUDE.md."""
    d = _make_dispatcher(db_factory)
    task = Task(
        title="t",
        description="implement X",
        mode="goal",
        goal_condition="all tests pass",
        goal_max_turns=10,
        provider="codex",
    )
    prompt = d._build_goal_initial_prompt(task)
    assert "AGENTS.md" in prompt


@pytest.mark.asyncio
async def test_build_task_prompt_provider_doc(db_factory):
    """Task prompt preamble follows the task's provider."""
    d = _make_dispatcher(db_factory)
    claude_prompt = await d._build_task_prompt(
        Task(title="t", description="do X", provider="claude")
    )
    codex_prompt = await d._build_task_prompt(
        Task(title="t", description="do X", provider="codex")
    )
    assert "请阅读项目根目录的 CLAUDE.md" in claude_prompt
    # Codex loads AGENTS.md natively; explicitly asking it to read the file
    # again turns trivial prompts into redundant shell/file work.
    assert "请阅读项目根目录的 AGENTS.md" not in codex_prompt
    assert "关键内容必须保持同步" in codex_prompt


@pytest.mark.asyncio
async def test_build_task_prompt_carries_doc_sync_note(db_factory):
    """两种 provider 的 prompt 前导都下发 CLAUDE.md/AGENTS.md 同步纪律。"""
    d = _make_dispatcher(db_factory)
    for provider in ("claude", "codex"):
        prompt = await d._build_task_prompt(
            Task(title="t", description="do X", provider=provider)
        )
        assert "关键内容必须保持同步" in prompt


@pytest.mark.asyncio
async def test_build_task_prompt_adds_short_math_hint_only_for_math_intent(
    db_factory,
):
    """Task #114-like theory questions get LaTeX guidance; ordinary work does not."""
    dispatcher = _make_dispatcher(db_factory)

    for provider in ("claude", "codex"):
        math_prompt = await dispatcher._build_task_prompt(
            Task(
                title="t",
                description="什么是 Diagonal AdaGrad 的核心理论保证？",
                provider=provider,
            )
        )
        ordinary_prompt = await dispatcher._build_task_prompt(
            Task(title="t", description="整理部署文档", provider=provider)
        )

        assert "数学排版提示" in math_prompt
        assert "不要放进代码块" in math_prompt
        assert "数学排版提示" not in ordinary_prompt

        for non_math_description in (
            "Fix the CSS linear-gradient and its color stops",
            "Update this Excel formula and spreadsheet formatting",
            "Debug Matrix protocol message delivery",
            "Write a proof of concept for the deployment hook",
        ):
            non_math_prompt = await dispatcher._build_task_prompt(
                Task(
                    title="t",
                    description=non_math_description,
                    provider=provider,
                )
            )
            assert "数学排版提示" not in non_math_prompt


@pytest.mark.asyncio
async def test_build_task_prompt_requires_downloadable_artifact_links(
    db_factory,
    tmp_path,
):
    """两种 provider 都把交付物限制在本 Task 的项目内目录。"""
    d = _make_dispatcher(db_factory)
    project_root = tmp_path / "项目 A"
    project_root.mkdir()
    for provider in ("claude", "codex"):
        prompt = await d._build_task_prompt(
            Task(
                id=42,
                title="t",
                description="create report",
                provider=provider,
                target_repo=str(project_root),
            )
        )
        assert "Markdown 链接" in prompt
        assert "不要只输出裸路径、文件名或相对路径" in prompt
        assert ".claude-manager/artifacts/task-42" in prompt
        assert (
            f'"{project_root}/.claude-manager/artifacts/task-42"'
            in prompt
        )
        assert "/workspace/.claude-manager/artifacts/task-42" in prompt
        assert ".claude-manager/worktrees/" in prompt
        assert "不得 `git add`" in prompt
        assert "`DEPLOYMENT.md`" in prompt
        assert '"ccm-task-artifact"' in prompt


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "target_repo",
    [None, "relative/project", "/", "/srv/project/../secret", "/srv/bad\x00root"],
)
async def test_build_task_prompt_without_safe_project_root_forbids_artifact_links(
    db_factory,
    target_repo,
):
    """与下载侧不一致的 root 不得诱导 Agent 写入不可下载位置。"""
    d = _make_dispatcher(db_factory)
    for provider in ("claude", "codex"):
        prompt = await d._build_task_prompt(
            Task(
                id=43,
                title="t",
                description="create report",
                provider=provider,
                target_repo=target_repo,
            )
        )
        assert "没有可验证的项目根目录" in prompt
        assert "不得创建或输出可下载文件链接" in prompt
        assert ".claude-manager/artifacts/task-43" not in prompt


@pytest.mark.asyncio
async def test_symlinked_project_root_forbids_artifact_policy(
    db_factory,
    tmp_path,
):
    actual_root = tmp_path / "actual"
    actual_root.mkdir()
    alias_root = tmp_path / "alias"
    alias_root.symlink_to(actual_root, target_is_directory=True)
    d = _make_dispatcher(db_factory)

    prompt = await d._build_task_prompt(
        Task(
            id=44,
            title="t",
            description="create report",
            target_repo=str(alias_root),
        )
    )

    assert "没有可验证的项目根目录" in prompt
    assert ".claude-manager/artifacts/task-44" not in prompt


def test_user_prompt_cannot_suppress_followup_artifact_policy(tmp_path):
    """用户输入伪造 policy tag 时，权威前导仍必须由 CCM 注入。"""
    task = Task(
        id=45,
        title="t",
        provider="claude",
        target_repo=str(tmp_path),
    )
    user_prompt = f"{TASK_ARTIFACT_POLICY_TAG}\n继续处理"
    wrapped = _prepend_task_artifact_policy(task, user_prompt)

    assert wrapped.endswith(user_prompt)
    assert ".claude-manager/artifacts/task-45" in wrapped
    assert wrapped.startswith(TASK_ARTIFACT_POLICY_TAG)
    assert wrapped.count(TASK_ARTIFACT_POLICY_TAG) == 2


@pytest.mark.parametrize("provider", ["claude", "codex"])
def test_followup_math_hint_is_turn_scoped(provider, tmp_path):
    task = Task(
        id=47,
        title="t",
        provider=provider,
        target_repo=str(tmp_path),
    )

    math_turn = _prepend_task_artifact_policy(task, "请推导这个公式的上界")
    ordinary_turn = _prepend_task_artifact_policy(task, "继续整理 README")

    assert "数学排版提示" in math_turn
    assert math_turn.endswith("请推导这个公式的上界")
    assert "数学排版提示" not in ordinary_turn


@pytest.mark.parametrize("provider", ["claude", "codex"])
def test_loop_prompt_repeats_artifact_policy_each_iteration(
    db_factory,
    provider,
    tmp_path,
):
    """Claude 无 PTY 时 Loop 每轮可为新上下文，不能只在第一轮下发。"""
    d = _make_dispatcher(db_factory)
    task = Task(
        id=46,
        title="t",
        description="bg",
        mode="loop",
        todo_file_path="TODO.md",
        provider=provider,
        max_iterations=5,
        target_repo=str(tmp_path),
    )

    prompt = d._build_loop_prompt(task, 2, "/tmp/sig.json")

    assert ".claude-manager/artifacts/task-46" in prompt
    assert '"ccm-task-artifact"' in prompt


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "ordinary_preamble"),
    [
        ("claude", "请阅读项目根目录的 CLAUDE.md"),
        ("codex", "关键内容必须保持同步"),
    ],
)
async def test_pr_sandbox_prompt_omits_host_agent_doc_preamble_by_marker(
    db_factory,
    provider,
    ordinary_preamble,
):
    """Remote review policy must not be mixed with the CCM checkout policy."""

    dispatcher = _make_dispatcher(db_factory)
    tagged = Task(
        title="review",
        description="READ_REMOTE_BASE_SNAPSHOT",
        provider=provider,
        tags=["pr-review"],
        metadata_={"pr_review_id": 17},
    )
    metadata_only = Task(
        title="review with removed tag",
        description="READ_METADATA_MARKED_SNAPSHOT",
        provider=provider,
        metadata_={"pr_review_id": 18},
    )
    fix_tag_only = Task(
        title="Worker fix mirror",
        description="GENERATE_WORKER_PATCH",
        provider=provider,
        tags=["pr-review-fix"],
    )
    fix_metadata_only = Task(
        title="fix with removed tag",
        description="GENERATE_MANAGER_PATCH",
        provider=provider,
        metadata_={"pr_finding_action_id": 19},
    )
    ordinary = Task(
        title="ordinary",
        description="do X",
        provider=provider,
    )

    review_prompt = await dispatcher._build_task_prompt(tagged)
    metadata_prompt = await dispatcher._build_task_prompt(metadata_only)
    fix_tag_prompt = await dispatcher._build_task_prompt(fix_tag_only)
    fix_metadata_prompt = await dispatcher._build_task_prompt(
        fix_metadata_only
    )
    ordinary_prompt = await dispatcher._build_task_prompt(ordinary)

    assert review_prompt == "任务:\nREAD_REMOTE_BASE_SNAPSHOT"
    assert metadata_prompt == "任务:\nREAD_METADATA_MARKED_SNAPSHOT"
    assert fix_tag_prompt == "任务:\nGENERATE_WORKER_PATCH"
    assert fix_metadata_prompt == "任务:\nGENERATE_MANAGER_PATCH"
    # Tags preserve fail-closed isolation on Worker mirrors; durable Manager
    # metadata prevents an old client from escaping isolation by removing one.
    for sandbox_prompt in (
        review_prompt,
        metadata_prompt,
        fix_tag_prompt,
        fix_metadata_prompt,
    ):
        assert "请阅读项目根目录的 CLAUDE.md" not in sandbox_prompt
        assert "关键内容必须保持同步" not in sandbox_prompt
    assert ordinary_preamble in ordinary_prompt


@pytest.mark.asyncio
async def test_build_task_prompt_does_not_claim_enabled_skill_was_invoked(
    db_factory,
    monkeypatch,
):
    """Availability is launch metadata; neither provider gets a fake invocation."""
    from backend.services.command_registry import COMMAND_REGISTRY, Command

    monkeypatch.setitem(
        COMMAND_REGISTRY,
        "fakeskill",
        Command(
            name="fakeskill",
            description="test",
            prompt_template="FAKESKILL_TEMPLATE",
        ),
    )
    d = _make_dispatcher(db_factory)
    claude_prompt = await d._build_task_prompt(
        Task(
            title="t",
            description="do X",
            provider="claude",
            enabled_skills={"fakeskill": True},
        )
    )
    codex_prompt = await d._build_task_prompt(
        Task(
            title="t",
            description="do X",
            provider="codex",
            enabled_skills={"fakeskill": True},
        )
    )
    assert "FAKESKILL_TEMPLATE" not in claude_prompt
    assert "FAKESKILL_TEMPLATE" not in codex_prompt


@pytest.mark.asyncio
async def test_codex_pr_review_relaunch_uses_fresh_thread_and_full_snapshot_prompt(
    db_factory,
):
    dispatcher = _make_dispatcher(db_factory)
    dispatcher._task_claim_is_active = AsyncMock(return_value=True)
    dispatcher._build_task_prompt = AsyncMock(
        return_value="FULL_IMMUTABLE_PR_SNAPSHOT_AND_TERMINAL_SCHEMA"
    )
    task = Task(
        id=97,
        title="PR Review",
        description="FULL_IMMUTABLE_PR_SNAPSHOT_AND_TERMINAL_SCHEMA",
        provider="codex",
        model="gpt-5.6-sol",
        tags=["pr-review"],
        metadata_={"pr_review_id": 17},
    )

    await dispatcher._relaunch_and_wait(
        3,
        task,
        MagicMock(),
        "/private/review-cwd",
        None,
        "/private/codex-home",
        "native-thread-from-first-attempt",
        thinking_budget=None,
        effort_level="high",
        label="transient retry",
    )

    launch = dispatcher.instance_manager.launch.await_args.kwargs
    assert launch["prompt"] == (
        "FULL_IMMUTABLE_PR_SNAPSHOT_AND_TERMINAL_SCHEMA"
    )
    assert "resume_session_id" not in launch
    assert "请继续之前的工作" not in launch["prompt"]
    dispatcher._build_task_prompt.assert_awaited_once_with(task)


@pytest.mark.asyncio
async def test_build_task_prompt_invokes_explicit_initial_command(
    db_factory,
    monkeypatch,
):
    from backend.services.command_registry import COMMAND_REGISTRY, Command
    from backend.services.dispatcher import _initial_task_launch_skills

    monkeypatch.setitem(
        COMMAND_REGISTRY,
        "delegate",
        Command(
            name="delegate",
            description="delegate work",
            prompt_template="LOAD_DELEGATE_SKILL",
            required_skills={"sub-agent": True},
        ),
    )
    dispatcher = _make_dispatcher(db_factory)
    task = Task(
        title="t",
        description="$delegate research this topic",
        provider="codex",
        enabled_skills={},
    )

    prompt = await dispatcher._build_task_prompt(task)
    launch_skills, temporary = _initial_task_launch_skills(task)

    assert "LOAD_DELEGATE_SKILL" in prompt
    assert "任务:\nresearch this topic" in prompt
    assert "$delegate" not in prompt
    assert launch_skills == {"sub-agent": True}
    assert temporary is True


def test_loop_prompt_codex_references_agents_md(db_factory, tmp_path):
    """Loop prompts reference AGENTS.md for codex tasks."""
    d = _make_dispatcher(db_factory)
    task = Task(
        id=44, title="t", description="bg", mode="loop",
        todo_file_path="TODO.md", provider="codex", max_iterations=5,
        target_repo=str(tmp_path),
    )
    prompt = d._build_loop_prompt(task, 0, "/tmp/sig.json")
    assert "AGENTS.md" in prompt
    assert "CLAUDE.md" not in prompt
    assert ".claude-manager/artifacts/task-44" in prompt


@pytest.mark.asyncio
async def test_lifecycle_backfills_agents_md(db_factory, tmp_path):
    """任务启动时把 AGENTS.md 惰性补进有 CLAUDE.md 的存量项目。"""
    (tmp_path / "CLAUDE.md").write_text("# guide\n")
    d = _make_dispatcher(db_factory)

    async with db_factory() as db:
        inst = Instance(name="worker-1")
        db.add(inst)
        task = Task(title="t", description="do X", target_repo=str(tmp_path))
        db.add(task)
        await db.commit()
        await db.refresh(inst)
        await db.refresh(task)
        inst_id = inst.id
        task_obj = task

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.wait = AsyncMock(return_value=0)
    d.instance_manager.processes = {inst_id: mock_proc}

    await _run_claimed_lifecycle(d, db_factory, inst_id, task_obj)

    assert (tmp_path / "AGENTS.md").exists()


@pytest.mark.asyncio
async def test_pr_review_lifecycle_uses_neutral_cwd_and_skips_agent_docs(
    db_factory,
    tmp_path,
    monkeypatch,
):
    """A stale CCM cwd/target must never make a review load host instructions."""

    from backend.services import agent_docs, pr_review_runtime

    host_checkout = tmp_path / "ccm-host"
    host_checkout.mkdir()
    (host_checkout / "CLAUDE.md").write_text("# host policy\n")
    (host_checkout / "AGENTS.md").symlink_to("CLAUDE.md")
    trusted_home = tmp_path / "trusted-home"
    trusted_home.mkdir(mode=0o700)
    runtime_root = trusted_home / "review-runtime"
    monkeypatch.setattr(
        pr_review_runtime,
        "_trusted_runtime_anchor",
        lambda: trusted_home,
    )
    monkeypatch.setenv(
        pr_review_runtime.PR_REVIEW_RUNTIME_DIR_ENV,
        str(runtime_root),
    )
    monkeypatch.setattr(
        pr_review_runtime.secrets,
        "token_hex",
        lambda _size: "neutral",
    )
    ensure_agents = MagicMock()
    monkeypatch.setattr(agent_docs, "ensure_agents_md", ensure_agents)

    dispatcher = _make_dispatcher(db_factory)
    async with db_factory() as db:
        instance = Instance(name="review-worker")
        task = Task(
            title="PR Review",
            description="READ_REMOTE_BASE_SNAPSHOT",
            target_repo=str(host_checkout),
            last_cwd=str(host_checkout),
            provider="codex",
            tags=["pr-review"],
        )
        db.add_all([instance, task])
        await db.commit()
        await db.refresh(instance)
        await db.refresh(task)
        instance_id = instance.id
        task_obj = task

    process = MagicMock(returncode=0)
    process.wait = AsyncMock(return_value=0)
    dispatcher.instance_manager.processes = {instance_id: process}

    await _run_claimed_lifecycle(
        dispatcher,
        db_factory,
        instance_id,
        task_obj,
    )

    launch = dispatcher.instance_manager.launch.await_args.kwargs
    neutral_cwds = list(runtime_root.iterdir())
    assert len(neutral_cwds) == 1
    neutral_cwd = neutral_cwds[0]
    assert launch["cwd"] == str(neutral_cwd)
    assert launch["cwd"] != str(host_checkout)
    assert launch["prompt"] == "任务:\nREAD_REMOTE_BASE_SNAPSHOT"
    assert not (neutral_cwd / "CLAUDE.md").exists()
    assert not (neutral_cwd / "AGENTS.md").exists()
    ensure_agents.assert_not_called()


def test_pr_review_cwd_reuse_requires_private_current_user_directory(
    tmp_path,
    monkeypatch,
):
    from backend.services import pr_review_runtime

    task_id = 42
    trusted_home = tmp_path / "trusted-home"
    trusted_home.mkdir(mode=0o700)
    runtime_root = trusted_home / "review-runtime"
    runtime_root.mkdir(mode=0o700)
    monkeypatch.setattr(
        pr_review_runtime,
        "_trusted_runtime_anchor",
        lambda: trusted_home,
    )
    monkeypatch.setenv(
        pr_review_runtime.PR_REVIEW_RUNTIME_DIR_ENV,
        str(runtime_root),
    )
    previous = runtime_root / f"ccm-pr-review-{task_id}-previous"
    previous.mkdir(mode=0o700)

    assert pr_review_runtime._is_reusable_review_cwd(
        str(previous),
        task_id,
    )

    previous.chmod(0o750)
    assert not pr_review_runtime._is_reusable_review_cwd(
        str(previous),
        task_id,
    )

    previous.chmod(0o700)
    real_uid = os.getuid()
    monkeypatch.setattr(pr_review_runtime.os, "getuid", lambda: real_uid + 1)
    assert not pr_review_runtime._is_reusable_review_cwd(
        str(previous),
        task_id,
    )


def test_pr_review_cwd_rejects_progress_doc_in_ancestry(
    tmp_path,
    monkeypatch,
):
    from backend.services import pr_review_runtime

    trusted_home = tmp_path / "host-context"
    trusted_home.mkdir(mode=0o700)
    (trusted_home / "PROGRESS.md").write_text("# host lessons\n")
    runtime_root = trusted_home / "review-runtime"
    monkeypatch.setattr(
        pr_review_runtime,
        "_trusted_runtime_anchor",
        lambda: trusted_home,
    )
    monkeypatch.setenv(
        pr_review_runtime.PR_REVIEW_RUNTIME_DIR_ENV,
        str(runtime_root),
    )

    with pytest.raises(
        RuntimeError,
        match="ancestry contains CLAUDE.md/AGENTS.md/PROGRESS.md",
    ):
        pr_review_runtime.isolated_pr_review_cwd(
            Task(id=9, tags=["pr-review"]),
        )


def test_pr_review_runtime_root_must_be_below_trusted_home(
    tmp_path,
    monkeypatch,
):
    from backend.services import pr_review_runtime

    trusted_home = tmp_path / "trusted-home"
    trusted_home.mkdir(mode=0o700)
    outside = tmp_path / "public-temp-style-root"
    monkeypatch.setattr(
        pr_review_runtime,
        "_trusted_runtime_anchor",
        lambda: trusted_home,
    )
    monkeypatch.setenv(
        pr_review_runtime.PR_REVIEW_RUNTIME_DIR_ENV,
        str(outside),
    )

    with pytest.raises(RuntimeError, match="trusted home directory"):
        pr_review_runtime.isolated_pr_review_cwd(
            Task(id=10, tags=["pr-review"]),
        )
    assert not outside.exists()


@pytest.mark.skipif(os.name != "posix", reason="POSIX ownership/mode contract")
def test_pr_review_runtime_rejects_insecure_existing_root(
    tmp_path,
    monkeypatch,
):
    from backend.services import pr_review_runtime

    trusted_home = tmp_path / "trusted-home"
    trusted_home.mkdir(mode=0o700)
    runtime_root = trusted_home / "review-runtime"
    runtime_root.mkdir(mode=0o755)
    monkeypatch.setattr(
        pr_review_runtime,
        "_trusted_runtime_anchor",
        lambda: trusted_home,
    )
    monkeypatch.setenv(
        pr_review_runtime.PR_REVIEW_RUNTIME_DIR_ENV,
        str(runtime_root),
    )

    with pytest.raises(RuntimeError, match="mode 0700"):
        pr_review_runtime.isolated_pr_review_cwd(
            Task(id=11, tags=["pr-review"]),
        )
