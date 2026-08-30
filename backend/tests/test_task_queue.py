"""Tests for TaskQueue — priority ordering, dequeue, status transitions."""
import asyncio
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy import delete, select, text, update
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.sql.dml import Update

from backend.config import settings
from backend.database import Base
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.capability import (
    CapabilityExecution,
    CapabilityInvocation,
    CapabilityResumeOutbox,
)
from backend.models.code_review import CodeReviewResult, CodeReviewRun
from backend.models.delivery import DeliveryCycle, DeliveryRun, DeliveryTurn
from backend.models.plan import (
    Plan,
    PlanApplication,
    PlanApplicationAttempt,
    PlanVersion,
)
from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentWorkerDispatchReceipt,
)
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRFindingAction,
    PRFindingRebuttal,
    PRMonitorRun,
    PRMonitorTaskTombstone,
    PRReview,
    PRReviewerRun,
)
from backend.models.project import Project
from backend.models.task import Task
from backend.models.user import User
from backend.models.test_harness import (
    TestHarnessAttempt,
    TestHarnessChildBinding,
    TestHarnessEvent,
    TestHarnessEvidence,
    TestHarnessFinding,
    TestHarnessRun,
    TestHarnessSandboxLease,
)
from backend.models.task_share import TaskShare
from backend.models.team_share import TeamProjectShare, TeamTaskShare
from backend.models.user_group import UserGroupMember  # noqa: F401
from backend.models.worker import Worker
from backend.models.worker_task_termination import WorkerTaskTerminationReceipt
from backend.models.workspace_review import WorkspaceReviewRun
from backend.services.delivery_service import DeliveryCreateSpec, create_delivery_run
from backend.services.task_sharing import lock_task_share_authority
from backend.services.task_queue import (
    TaskDeletePreflight,
    TaskQueue,
    TaskWaitingCapabilityConflict,
    _effective_key_expr,
    ordinary_task_visibility_predicate,
    pr_review_dispatch_predicate,
    task_delete_fence,
    task_generation_fence,
)
from backend.services.test_harness_children import TestHarnessChildService


@pytest_asyncio.fixture
async def queue(db_session):
    return TaskQueue(db_session)


async def _seed_ready_worker(queue: TaskQueue, worker_id: int = 91) -> Worker:
    worker = Worker(
        id=worker_id,
        name=f"queue-worker-{worker_id}",
        status="ready",
        bootstrap_step=None,
    )
    queue.db.add(worker)
    await queue.db.commit()
    return worker


async def _pending_delivery_task(queue: TaskQueue, *, admitted: bool) -> Task:
    repo_full_name = f"example/queue-delivery-{id(queue)}-{int(admitted)}"
    project = Project(
        name=f"queue-delivery-{id(queue)}-{int(admitted)}",
        git_url=f"https://github.com/{repo_full_name}.git",
        has_remote=True,
        local_path="/tmp/queue-delivery",
        default_branch="main",
        status="ready",
    )
    queue.db.add(project)
    await queue.db.flush()
    repo = MonitoredRepo(
        repo_full_name=repo_full_name,
        project_id=project.id,
        webhook_secret="queue-secret",
        review_mode="panel",
        wait_for_ci=True,
        required_checks=["tests"],
        merge_queue_mode="manual",
        default_branch="main",
    )
    queue.db.add(repo)
    await queue.db.commit()
    run = await create_delivery_run(
        queue.db,
        DeliveryCreateSpec(
            idempotency_key="queue-owned-delivery",
            project_id=project.id,
            monitored_repo_id=repo.id,
            title="Queue-owned delivery",
            requirements="Implement the queued change",
        ),
    )
    task = await queue.db.get(Task, run.developer_task_id)
    cycle = await queue.db.get(DeliveryCycle, run.current_cycle_id)
    assert task is not None and cycle is not None
    task.status = "pending"
    task.priority = -10
    if admitted:
        run.phase = "coding"
        run.activity = "running"
        cycle.status = "coding"
        queue.db.add(
            DeliveryTurn(
                run_id=run.id,
                cycle_id=cycle.id,
                generation=1,
                correlation_id=f"delivery:{run.id}:turn:1",
                active_run_id=run.id,
                purpose="code",
                trigger_kind="plan_ready",
                trigger_payload={},
                prompt_payload={"schema_version": 1},
                prompt_hash="a" * 64,
                status="queued",
                task_id=task.id,
                task_retry_count=task.retry_count,
                attempts=1,
            )
        )
    await queue.db.commit()
    await queue.db.refresh(task)
    return task


async def _terminal_browser_owner_graph(
    queue: TaskQueue,
) -> tuple[int, int, str, str, int]:
    """Create a fully terminal owner -> Harness -> Browser child graph."""

    owner = await queue.create(
        title="Harness delete owner",
        description="durable owner",
        status="completed",
        archived=True,
        provider="codex",
        model="gpt-5.6-sol",
        effort_level="high",
    )
    owner_id = owner.id
    run_id = "a" * 32
    queue.db.add(
        TestHarnessRun(
            id=run_id,
            task_id=owner_id,
            owner_task_incarnation_id=owner.incarnation_id,
            owner_task_retry_count=owner.retry_count,
            owner_task_turn_generation=owner.turn_generation,
            owner_task_status=owner.status,
            target_kind="fixed_url",
            target_spec={"url": "https://example.com"},
            test_plan={"objective": "Review"},
            runtime_config={"provider": "codex"},
            request_fingerprint="b" * 64,
            root_run_id=run_id,
            status="running",
            stage="waiting_for_browser",
        )
    )
    await queue.db.commit()
    sessions = async_sessionmaker(
        queue.db.bind,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    child_service = TestHarnessChildService(db_factory=sessions)
    child, binding = await child_service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id="job-delete-graph",
        harness_run_id=run_id,
        child_values={
            "title": "Immutable Browser child",
            "description": "Review the frozen target",
            "priority": 0,
            "max_retries": 0,
            "mode": "auto",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "default",
            "effort_level": "high",
            "enabled_skills": {"browser-review": "job-delete-graph"},
            "archived": True,
        },
    )
    child_id = child.id
    binding_id = binding.id
    await child_service.abort_reservation(
        binding_id,
        RuntimeError("terminal before launch"),
    )

    queue.db.expire_all()
    run = await queue.db.get(TestHarnessRun, run_id)
    durable_child = await queue.db.get(Task, child_id)
    assert run is not None and durable_child is not None
    run.status = "completed"
    run.stage = "completed"
    run.cleanup_status = "completed"
    run.completed_at = datetime.utcnow()
    instance = Instance(
        name="terminal Browser instance",
        status="stopped",
        current_task_id=child_id,
    )
    queue.db.add(instance)
    await queue.db.flush()
    durable_child.instance_id = instance.id
    queue.db.add_all(
        [
            LogEntry(
                task_id=child_id,
                event_type="message",
                role="assistant",
                content="private child output",
            ),
            LogEntry(
                task_id=owner_id,
                event_type="system_event",
                content="private owner output",
            ),
            *_task_access_grants(child_id, suffix="browser-child"),
            TestHarnessAttempt(
                id="c" * 32,
                run_id=run_id,
                ordinal=1,
                status="completed",
                stage="completed",
                provider="codex",
                model="gpt-5.6-sol",
                reasoning_effort="high",
                codex_service_tier="default",
                browser_review_job_id="job-delete-graph",
                archive_state="complete",
                archive_manifest={"version": 1, "expected": [], "archived": {}},
                result_data={},
            ),
            TestHarnessEvent(
                run_id=run_id,
                sequence=1,
                event_type="lifecycle",
                title="complete",
                data={},
            ),
            TestHarnessEvidence(
                id="d" * 32,
                run_id=run_id,
                attempt_id="c" * 32,
                kind="report",
                name="report.md",
                content_type="text/markdown",
                storage_path="runs/task-1/report.md",
                sha256="e" * 64,
                byte_size=1,
                metadata_={},
            ),
            TestHarnessFinding(
                id="f" * 32,
                run_id=run_id,
                ordinal=1,
                fingerprint="1" * 64,
                scenario_id="page",
                severity="low",
                category="visual",
                title="finding",
                reproduction=[],
                evidence_names=[],
            ),
            TestHarnessSandboxLease(
                id="2" * 32,
                run_id=run_id,
                backend="docker",
                lease_nonce="nonce-delete-graph",
                image_ref="sandbox:test",
                status="stopped",
                phase="cleaned",
                runtime_metadata={},
                cleanup_status="completed",
            ),
        ]
    )
    await queue.db.commit()
    return owner_id, child_id, run_id, binding_id, instance.id


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
    assert task.turn_generation == 0


@pytest.mark.asyncio
async def test_update_task_honors_already_held_operation_lock(queue):
    """Worker edit helpers can call the CAS without re-entering their lock."""

    from backend.services.worker_proxy import get_task_operation_lock

    task = await queue.create(title="locked edit", description="d")
    async with get_task_operation_lock(task.id):
        updated = await asyncio.wait_for(
            queue.update_task(
                task.id,
                operation_lock_held=True,
                title="serialized edit",
            ),
            timeout=1,
        )

    assert updated is not None
    assert updated.title == "serialized edit"


@pytest.mark.asyncio
async def test_update_task_rejects_waiting_capability_but_allows_read_marker(queue):
    task = await queue.create(title="waiting edit", description="d")
    task.status = "waiting_capability"
    task.has_unread = True
    await queue.db.commit()
    task_id = task.id

    with pytest.raises(TaskWaitingCapabilityConflict, match="waiting"):
        await queue.update_task(task_id, title="must not change")

    marked = await queue.update_task(
        task_id,
        has_unread=False,
    )
    assert marked is not None
    assert marked.status == "waiting_capability"
    assert marked.title == "waiting edit"
    assert marked.has_unread is False


@pytest.mark.asyncio
async def test_update_task_loses_cleanly_to_concurrent_wal_receipt(tmp_path):
    """An authorization snapshot cannot produce BUSY_SNAPSHOT on edit."""

    from backend.services.worker_task_termination import (
        WorkerTaskTerminationConflict,
    )
    from backend.tests.worker_termination_helpers import (
        persist_active_worker_receipt,
    )

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'task-update-receipt.db'}",
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
        async with sessions() as setup:
            task = Task(title="old title", description="d", status="completed")
            setup.add(task)
            await setup.commit()
            task_id = task.id

        async with sessions() as editor:
            # Simulate the API authorization read which precedes TaskQueue's
            # mutation boundary, then admit a receipt on another connection.
            observed = await editor.get(Task, task_id)
            assert observed is not None
            assert editor.in_transaction()
            await persist_active_worker_receipt(sessions, task_id)

            with pytest.raises(
                WorkerTaskTerminationConflict,
                match="termination receipt",
            ):
                await TaskQueue(editor).update_task(
                    task_id,
                    title="must not overwrite receipt-owned Task",
                )

        async with sessions() as verify:
            current = await verify.get(Task, task_id)
            assert current is not None
            assert current.title == "old title"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_update_task_loses_cleanly_to_concurrent_wal_capability_wait(tmp_path):
    """A stale authorization read cannot edit across capability admission."""

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'task-update-capability.db'}",
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
        async with sessions() as setup:
            task = Task(title="old title", description="d", status="completed")
            setup.add(task)
            await setup.commit()
            task_id = task.id

        async with sessions() as editor:
            observed = await editor.get(Task, task_id)
            assert observed is not None
            assert editor.in_transaction()
            async with sessions() as admission:
                await admission.execute(
                    update(Task)
                    .where(Task.id == task_id)
                    .values(status="waiting_capability")
                )
                await admission.commit()

            with pytest.raises(TaskWaitingCapabilityConflict, match="waiting"):
                await TaskQueue(editor).update_task(
                    task_id,
                    title="must not cross capability admission",
                )

        async with sessions() as verify:
            current = await verify.get(Task, task_id)
            assert current is not None
            assert current.status == "waiting_capability"
            assert current.title == "old title"
    finally:
        await engine.dispose()


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
    assert first.turn_generation == 1

    second = await queue.dequeue()
    assert second is not None
    assert second.title == "Medium priority"
    assert second.priority == 5

    third = await queue.dequeue()
    assert third is not None
    assert third.title == "Low priority"
    assert third.priority == 10


@pytest.mark.asyncio
async def test_dequeue_clears_previous_turn_source_in_generation_claim(queue):
    task = await queue.create(
        title="Fresh generation",
        description="d",
        target_repo="/tmp",
    )
    previous_source = LogEntry(
        task_id=task.id,
        task_retry_count=task.retry_count,
        task_turn_generation=task.turn_generation,
        turn_scope="source",
        event_type="turn_source",
        role="system",
        content=None,
        is_error=False,
    )
    queue.db.add(previous_source)
    await queue.db.flush()
    task.turn_source_log_id = previous_source.id
    await queue.db.commit()

    claimed = await queue.dequeue()

    assert claimed is not None
    assert claimed.id == task.id
    assert claimed.turn_generation == 1
    assert claimed.turn_source_log_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("changed_field", "changed_value"),
    (("is_active", False), ("role", "member")),
    ids=("disabled", "demoted"),
)
async def test_dequeue_rejects_revoked_native_principal_without_claiming(
    queue,
    changed_field,
    changed_value,
):
    user = User(
        email=f"dequeue-{changed_field}@example.com",
        name="dequeue principal",
        password_hash="unused",
        role="admin",
        is_active=True,
    )
    queue.db.add(user)
    await queue.db.flush()
    blocked = await queue.create(
        title="revoked native principal",
        description="must stay pending",
        priority=-10,
        execution_user_id=user.id,
        execution_user_role="admin",
        execution_mode="unrestricted",
        execution_principal_kind="user",
    )
    runnable = await queue.create(
        title="unrelated runnable",
        description="must still claim",
        priority=0,
    )
    blocked_id = blocked.id
    runnable_id = runnable.id
    principal = await queue.db.get(User, user.id)
    setattr(principal, changed_field, changed_value)
    await queue.db.commit()

    claimed = await queue.dequeue()

    assert claimed is not None and claimed.id == runnable_id
    current = await queue.db.get(Task, blocked_id, populate_existing=True)
    assert current.status == "pending"
    assert current.turn_generation == 0


@pytest.mark.asyncio
async def test_dequeue_delegated_principal_does_not_require_local_user(queue):
    task = await queue.create(
        title="delegated Worker task",
        description="Manager permit owns authority",
        execution_user_id=987654321,
        execution_user_role="admin",
        execution_mode="unrestricted",
        execution_principal_kind="delegated_user",
    )

    claimed = await queue.dequeue()

    assert claimed is not None and claimed.id == task.id
    assert claimed.status == "in_progress"
    assert claimed.turn_generation == 1


@pytest.mark.asyncio
async def test_dequeue_native_principal_writer_order_is_task_then_user(
    queue,
    monkeypatch,
):
    user = User(
        email="dequeue-lock-order@example.com",
        name="dequeue lock order",
        password_hash="unused",
        role="admin",
        is_active=True,
    )
    queue.db.add(user)
    await queue.db.flush()
    await queue.create(
        title="native lock order",
        description="Task writer precedes User writer",
        execution_user_id=user.id,
        execution_user_role="admin",
        execution_mode="unrestricted",
        execution_principal_kind="user",
    )

    original_execute = AsyncSession.execute
    writes: list[str] = []

    async def record_writes(session, statement, *args, **kwargs):
        table = getattr(statement, "table", None)
        table_name = getattr(table, "name", None)
        if getattr(statement, "is_update", False) and table_name in {
            "tasks",
            "users",
        }:
            writes.append(table_name)
        return await original_execute(session, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", record_writes)

    claimed = await queue.dequeue()

    assert claimed is not None
    assert writes[:2] == ["tasks", "users"]


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
        assert all(
            task.turn_generation == 1 for task in claimed if task is not None
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_dequeue_same_task_increments_turn_generation_once(tmp_path):
    """A lost claim CAS cannot consume a second logical turn generation."""

    db_path = tmp_path / "single-atomic-dequeue.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        factory = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False,
        )
        async with factory() as db:
            task = Task(title="single", description="d", priority=0)
            db.add(task)
            await db.commit()
            task_id = task.id

        async with factory() as db1, factory() as db2:
            claimed = await asyncio.gather(
                TaskQueue(db1).dequeue(),
                TaskQueue(db2).dequeue(),
            )

        winners = [task for task in claimed if task is not None]
        assert len(winners) == 1
        assert winners[0].id == task_id
        assert winners[0].turn_generation == 1
        async with factory() as db:
            persisted = await db.get(Task, task_id)
            assert persisted is not None
            assert persisted.status == "in_progress"
            assert persisted.turn_generation == 1
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
async def test_dequeue_recovers_admitted_delivery_when_admission_is_disabled(
    queue,
    monkeypatch,
):
    delivery = await _pending_delivery_task(queue, admitted=True)
    monkeypatch.setattr(settings, "capability_core_enabled", False)
    monkeypatch.setattr(settings, "delivery_loop_enabled", False)

    selected = await queue.dequeue()

    assert selected is not None and selected.id == delivery.id
    assert selected.status == "in_progress"


@pytest.mark.asyncio
async def test_dequeue_rechecks_delivery_admission_during_claim_cas(
    queue,
    monkeypatch,
):
    delivery = await _pending_delivery_task(queue, admitted=True)
    delivery_id = delivery.id
    runnable = await queue.create(
        title="ordinary after stale delivery",
        description="d",
        priority=0,
    )
    turn = (
        await queue.db.execute(
            select(DeliveryTurn).where(DeliveryTurn.task_id == delivery_id)
        )
    ).scalar_one()
    original_execute = queue.db.execute
    invalidated = False

    async def invalidate_before_claim(statement, *args, **kwargs):
        nonlocal invalidated
        if (
            not invalidated
            and isinstance(statement, Update)
            and statement.table.name == Task.__tablename__
        ):
            invalidated = True
            turn.status = "stale"
            turn.active_run_id = None
            await queue.db.flush()
        return await original_execute(statement, *args, **kwargs)

    monkeypatch.setattr(queue.db, "execute", invalidate_before_claim)

    selected = await queue.dequeue()

    assert invalidated is True
    assert selected is not None and selected.id == runnable.id
    assert selected.turn_generation == 1
    stale_delivery = await queue.get(delivery_id)
    assert stale_delivery.status == "pending"
    assert stale_delivery.turn_generation == 0


@pytest.mark.asyncio
async def test_dequeue_requires_active_delivery_turn(queue, monkeypatch):
    delivery = await _pending_delivery_task(queue, admitted=False)
    runnable = await queue.create(
        title="ordinary fallback",
        description="d",
        priority=0,
    )
    monkeypatch.setattr(settings, "capability_core_enabled", True)
    monkeypatch.setattr(settings, "delivery_loop_enabled", True)

    selected = await queue.dequeue()

    assert selected is not None and selected.id == runnable.id
    assert (await queue.get(delivery.id)).status == "pending"


@pytest.mark.asyncio
async def test_dequeue_claims_controller_admitted_delivery_turn(
    queue,
    monkeypatch,
):
    delivery = await _pending_delivery_task(queue, admitted=True)
    monkeypatch.setattr(settings, "capability_core_enabled", True)
    monkeypatch.setattr(settings, "delivery_loop_enabled", True)

    selected = await queue.dequeue(instance_id=41)

    assert selected is not None and selected.id == delivery.id
    assert selected.status == "in_progress"
    assert selected.instance_id == 41


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
async def test_retry_cannot_opt_waiting_capability_into_retryable_statuses(queue):
    task = await queue.create(title="waiting retry", description="d")
    task.status = "waiting_capability"
    await queue.db.commit()
    task_id = task.id

    retried = await queue.retry(
        task_id,
        expected_statuses=("waiting_capability",),
    )

    assert retried is None
    queue.db.expire_all()
    current = await queue.db.get(Task, task_id)
    assert current is not None
    assert current.status == "waiting_capability"
    assert current.retry_count == 0


@pytest.mark.asyncio
async def test_retry_atomically_clears_post_boundary_turn_source(queue):
    """A new retry must never expose the previous attempt's source proof."""

    task = await queue.create(title="boundary retry", description="d")
    claimed = await queue.dequeue(instance_id=17)
    assert claimed is not None
    source = LogEntry(
        task_id=claimed.id,
        task_retry_count=claimed.retry_count,
        task_turn_generation=claimed.turn_generation,
        turn_scope="source",
        event_type="user_message",
        role="user",
        content="run once",
        is_error=False,
        actual_transport="claude_exec",
    )
    queue.db.add(source)
    await queue.db.flush()
    claimed.turn_source_log_id = source.id
    claimed.status = "failed"
    claimed.error_message = "provider outcome settled as failed"
    await queue.db.commit()
    old_retry_count = claimed.retry_count
    old_turn_generation = claimed.turn_generation
    task_id = claimed.id
    source_id = source.id

    retried = await queue.retry(task_id)

    assert retried is not None
    assert retried.status == "pending"
    assert retried.retry_count == old_retry_count + 1
    assert retried.turn_generation == old_turn_generation
    assert retried.turn_source_log_id is None
    old_source = await queue.db.get(LogEntry, source_id)
    assert old_source is not None
    assert old_source.actual_transport == "claude_exec"


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
async def test_retry_does_not_increment_turn_generation_until_next_claim(queue):
    task = await queue.create(title="turn retry", description="d")
    task_id = task.id
    first = await queue.dequeue(instance_id=4)
    assert first is not None
    assert first.turn_generation == 1

    assert await queue.retry(
        task_id,
        expected_statuses=("in_progress", "executing"),
        instance_id=99,
        generation_fence=task_generation_fence(first),
    ) is None
    queue.db.expire_all()
    unchanged = await queue.get(task_id)
    assert unchanged is not None
    assert unchanged.turn_generation == 1

    first_generation = task_generation_fence(unchanged)
    retried = await queue.retry(
        task_id,
        expected_statuses=("in_progress", "executing"),
        instance_id=4,
        generation_fence=first_generation,
    )
    assert retried is not None
    assert retried.status == "pending"
    assert retried.turn_generation == 1

    second = await queue.dequeue(instance_id=4)
    assert second is not None
    assert second.turn_generation == 2


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
async def test_lifecycle_fence_rejects_turn_generation_only_aba(queue):
    """Legacy owner fields matching cannot authorize a newer logical turn."""

    task = await queue.create(title="turn-only ABA", description="d")
    claimed = await queue.dequeue(instance_id=12)
    assert claimed is not None
    task_id = claimed.id
    old_generation = task_generation_fence(claimed)

    claimed.turn_generation += 1
    await queue.db.commit()

    assert not await queue.mark_completed(
        task_id,
        instance_id=12,
        generation_fence=old_generation,
    )
    assert not await queue.mark_failed(
        task_id,
        "late old failure",
        instance_id=12,
        generation_fence=old_generation,
    )
    assert not await queue.defer(
        task_id,
        "late old defer",
        instance_id=12,
        generation_fence=old_generation,
    )
    assert await queue.retry(
        task_id,
        expected_statuses=("in_progress", "executing"),
        instance_id=12,
        generation_fence=old_generation,
    ) is None

    queue.db.expire_all()
    current = await queue.get(task_id)
    assert current.status == "in_progress"
    assert current.turn_generation == old_generation[-1] + 1


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


def _task_access_grants(task_id: int, *, suffix: str):
    return (
        TaskShare(
            task_id=task_id,
            shared_to_open_id=f"open-{suffix}",
            shared_to_name="Old recipient",
            shared_to_ccm_url="https://peer.example",
            share_token=f"token-{suffix}",
        ),
        TeamTaskShare(
            task_id=task_id,
            target_type="user",
            target_id=41,
            permission="chat",
            shared_by=7,
        ),
    )


@pytest.mark.asyncio
async def test_delete_harness_owner_removes_terminal_child_and_fk_off_graph(queue):
    owner_id, child_id, run_id, binding_id, instance_id = (
        await _terminal_browser_owner_graph(queue)
    )
    await queue.db.execute(text("PRAGMA foreign_keys=OFF"))

    assert await queue.delete(owner_id) is True
    assert await queue.db.get(Task, owner_id) is None
    assert await queue.db.get(Task, child_id) is None
    assert await queue.db.get(TestHarnessRun, run_id) is None
    assert await queue.db.get(TestHarnessChildBinding, binding_id) is None
    for model in (
        TestHarnessAttempt,
        TestHarnessEvent,
        TestHarnessEvidence,
        TestHarnessFinding,
        TestHarnessSandboxLease,
    ):
        assert await queue.db.scalar(select(model).limit(1)) is None
    assert await queue.db.scalar(
        select(LogEntry.id).where(LogEntry.task_id.in_((owner_id, child_id)))
    ) is None
    assert await queue.db.scalar(
        select(TaskShare.id).where(TaskShare.task_id == child_id)
    ) is None
    assert await queue.db.scalar(
        select(TeamTaskShare.id).where(TeamTaskShare.task_id == child_id)
    ) is None
    instance = await queue.db.get(Instance, instance_id)
    assert instance is not None
    assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_delete_harness_owner_immediately_removes_archived_evidence_file(
    queue,
    monkeypatch,
    tmp_path,
):
    from backend.services import test_harness_artifacts as artifact_module
    from backend.services.test_harness_artifacts import TestHarnessArtifactStore

    owner_id, _child_id, run_id, _binding_id, _instance_id = (
        await _terminal_browser_owner_graph(queue)
    )
    store = TestHarnessArtifactStore(
        tmp_path / "harness-artifacts",
        max_file_bytes=1024,
        max_run_bytes=2048,
        max_task_bytes=4096,
        max_total_bytes=8192,
    )
    source = tmp_path / "report.md"
    source.write_text("private archived evidence", encoding="utf-8")
    archived = store.archive(
        source,
        task_id=owner_id,
        run_id=run_id,
        attempt_id="c" * 32,
        name="report.md",
    )
    evidence = await queue.db.get(TestHarnessEvidence, "d" * 32)
    assert evidence is not None
    evidence.storage_path = archived.storage_key
    evidence.sha256 = archived.sha256
    evidence.byte_size = archived.byte_size
    await queue.db.commit()
    monkeypatch.setattr(
        artifact_module,
        "test_harness_artifact_store",
        store,
    )

    assert archived.path.is_file()
    assert await queue.delete(owner_id) is True
    assert not archived.path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("run_kind", ["harness", "workspace"])
@pytest.mark.parametrize("cleanup_status", ["pending", "failed", "unconfirmed"])
async def test_delete_harness_owner_preserves_unproven_cleanup_graph(
    queue,
    run_kind,
    cleanup_status,
):
    owner_id, child_id, run_id, binding_id, _instance_id = (
        await _terminal_browser_owner_graph(queue)
    )
    owner = await queue.db.get(Task, owner_id)
    run = await queue.db.get(TestHarnessRun, run_id)
    assert owner is not None and run is not None
    if run_kind == "harness":
        run.cleanup_status = cleanup_status
        run.cleanup_error = "cleanup was not proven"
    else:
        workspace_id = "9" * 32
        run.workspace_review_run_id = workspace_id
        queue.db.add(
            WorkspaceReviewRun(
                id=workspace_id,
                task_id=owner_id,
                owner_task_incarnation_id=owner.incarnation_id,
                owner_task_retry_count=owner.retry_count,
                owner_task_turn_generation=owner.turn_generation,
                owner_task_status=owner.status,
                harness_run_id=run_id,
                agent_task_id=child_id,
                browser_review_job_id="job-delete-graph",
                mode="review_only",
                profile="standard",
                goal="Review",
                status="completed",
                stage="completed",
                workspace_path="/tmp/workspace",
                git_head="a" * 40,
                workspace_fingerprint="b" * 64,
                preview_config={},
                cleanup_status=cleanup_status,
                cleanup_error="cleanup was not proven",
                completed_at=datetime.utcnow(),
            )
        )
    await queue.db.commit()

    assert await queue.delete(owner_id) is False
    assert await queue.db.get(Task, owner_id) is not None
    assert await queue.db.get(Task, child_id) is not None
    assert await queue.db.get(TestHarnessRun, run_id) is not None
    assert await queue.db.get(TestHarnessChildBinding, binding_id) is not None
    if run_kind == "workspace":
        assert await queue.db.get(WorkspaceReviewRun, "9" * 32) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "downstream_kind",
    ["harness", "workspace", "binding", "plan"],
)
async def test_delete_harness_owner_rejects_browser_child_downstream_ownership(
    queue,
    downstream_kind,
):
    owner_id, child_id, run_id, binding_id, _instance_id = (
        await _terminal_browser_owner_graph(queue)
    )
    child = await queue.db.get(Task, child_id)
    assert child is not None
    if downstream_kind == "harness":
        queue.db.add(
            TestHarnessRun(
                id="3" * 32,
                task_id=child_id,
                owner_task_incarnation_id=child.incarnation_id,
                owner_task_retry_count=child.retry_count,
                owner_task_turn_generation=child.turn_generation,
                owner_task_status=child.status,
                target_kind="fixed_url",
                target_spec={"url": "https://nested.example"},
                test_plan={"objective": "Nested review"},
                runtime_config={"provider": "codex"},
                request_fingerprint="4" * 64,
                root_run_id="3" * 32,
                status="completed",
                stage="completed",
                cleanup_status="completed",
                completed_at=datetime.utcnow(),
            )
        )
    elif downstream_kind == "workspace":
        queue.db.add(
            WorkspaceReviewRun(
                id="5" * 32,
                task_id=child_id,
                owner_task_incarnation_id=child.incarnation_id,
                owner_task_retry_count=child.retry_count,
                owner_task_turn_generation=child.turn_generation,
                owner_task_status=child.status,
                mode="review_only",
                profile="standard",
                goal="Nested workspace review",
                status="completed",
                stage="completed",
                workspace_path="/tmp/nested-workspace",
                git_head="6" * 40,
                workspace_fingerprint="7" * 64,
                preview_config={},
                cleanup_status="completed",
                completed_at=datetime.utcnow(),
            )
        )
    elif downstream_kind == "binding":
        nested_child = Task(
            title="Nested Browser child",
            description="must not be orphaned",
            status="cancelled",
            archived=True,
        )
        queue.db.add(nested_child)
        await queue.db.flush()
        queue.db.add(
            TestHarnessChildBinding(
                id="8" * 32,
                harness_run_id="3" * 32,
                owner_task_id=child_id,
                owner_task_incarnation_id=child.incarnation_id,
                owner_task_retry_count=child.retry_count,
                owner_task_turn_generation=child.turn_generation,
                owner_task_status=child.status,
                child_task_id=nested_child.id,
                child_task_incarnation_id=nested_child.incarnation_id,
                browser_review_job_id="nested-browser-job",
                state="stopped",
                completed_at=datetime.utcnow(),
            )
        )
    else:
        queue.db.add(
            Plan(
                title="Nested child Plan",
                initial_request="must retain this downstream aggregate",
                target_task_id=child_id,
                pipeline_config={},
            )
        )
    await queue.db.commit()

    assert await queue.delete(owner_id) is False
    assert await queue.db.get(Task, owner_id) is not None
    assert await queue.db.get(Task, child_id) is not None
    assert await queue.db.get(TestHarnessRun, run_id) is not None
    assert await queue.db.get(TestHarnessChildBinding, binding_id) is not None


@pytest.mark.asyncio
async def test_delete_harness_child_directly_never_detaches_owner_graph(queue):
    owner_id, child_id, run_id, binding_id, _instance_id = (
        await _terminal_browser_owner_graph(queue)
    )

    assert await queue.delete(child_id) is False
    assert await queue.db.get(Task, owner_id) is not None
    assert await queue.db.get(Task, child_id) is not None
    assert await queue.db.get(TestHarnessRun, run_id) is not None
    assert await queue.db.get(TestHarnessChildBinding, binding_id) is not None


@pytest.mark.asyncio
async def test_concurrent_owner_and_child_delete_keep_owner_first_task_lock_order(
    queue,
):
    owner_id, child_id, _run_id, _binding_id, _instance_id = (
        await _terminal_browser_owner_graph(queue)
    )
    sessions = async_sessionmaker(
        queue.db.bind,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    await queue.db.rollback()

    async def delete(task_id: int) -> bool:
        async with sessions() as db:
            return await TaskQueue(db).delete(task_id)

    owner_result, child_result = await asyncio.wait_for(
        asyncio.gather(delete(owner_id), delete(child_id)),
        timeout=2,
    )

    assert owner_result is True
    assert child_result is False
    async with sessions() as db:
        assert await db.get(Task, owner_id) is None
        assert await db.get(Task, child_id) is None


@pytest.mark.asyncio
async def test_delete_harness_owner_rejects_reused_task_id_incarnation(queue):
    owner_id, child_id, run_id, binding_id, _instance_id = (
        await _terminal_browser_owner_graph(queue)
    )
    old_incarnation = (
        await queue.db.get(Task, owner_id)
    ).incarnation_id
    await queue.db.execute(delete(Task).where(Task.id == owner_id))
    await queue.db.commit()
    # Simulate a pre-namespace/legacy database reusing the integer directly.
    # Production Manager creation must never weaken its explicit-id gate just
    # to construct this ABA fixture.
    replacement = Task(
        id=owner_id,
        title="Reused owner integer",
        description="different security identity",
        status="completed",
    )
    queue.db.add(replacement)
    await queue.db.commit()
    await queue.db.refresh(replacement)
    assert replacement.incarnation_id != old_incarnation

    assert await queue.delete(owner_id) is False
    assert await queue.db.get(Task, owner_id) is not None
    assert await queue.db.get(Task, child_id) is not None
    assert await queue.db.get(TestHarnessRun, run_id) is not None
    assert await queue.db.get(TestHarnessChildBinding, binding_id) is not None


@pytest.mark.asyncio
async def test_delete_harness_graph_exception_rolls_back_every_row(queue):
    owner_id, child_id, run_id, binding_id, instance_id = (
        await _terminal_browser_owner_graph(queue)
    )

    async def fail_mid_graph(db, graph):
        await db.execute(
            delete(TestHarnessEvent).where(TestHarnessEvent.run_id == run_id)
        )
        raise RuntimeError("injected Harness graph failure")

    with patch(
        "backend.services.task_queue._delete_test_harness_graph",
        new=fail_mid_graph,
    ):
        with pytest.raises(RuntimeError, match="injected Harness graph failure"):
            await queue.delete(owner_id)

    queue.db.expire_all()
    assert await queue.db.get(Task, owner_id) is not None
    assert await queue.db.get(Task, child_id) is not None
    assert await queue.db.get(TestHarnessRun, run_id) is not None
    assert await queue.db.get(TestHarnessChildBinding, binding_id) is not None
    assert await queue.db.scalar(
        select(TestHarnessEvent.id).where(TestHarnessEvent.run_id == run_id)
    ) is not None
    instance = await queue.db.get(Instance, instance_id)
    assert instance is not None and instance.current_task_id == child_id


@pytest.mark.asyncio
async def test_delete_task_explicitly_removes_all_access_grants(queue):
    task = await queue.create(title="old secret", description="delete me")
    task.status = "completed"
    queue.db.add_all(_task_access_grants(task.id, suffix="delete"))
    await queue.db.commit()
    task_id = task.id

    assert await queue.delete(task_id) is True
    assert await queue.db.get(Task, task_id) is None
    assert await queue.db.scalar(
        select(TaskShare.id).where(TaskShare.task_id == task_id)
    ) is None
    assert await queue.db.scalar(
        select(TeamTaskShare.id).where(TeamTaskShare.task_id == task_id)
    ) is None


@pytest.mark.asyncio
async def test_delete_task_restores_access_grants_when_final_cas_loses(queue):
    task = await queue.create(title="CAS owner", description="keep private")
    task.status = "completed"
    grants = _task_access_grants(task.id, suffix="rollback")
    queue.db.add_all(grants)
    await queue.db.commit()
    task_id = task.id
    grant_ids = (grants[0].id, grants[1].id)
    original_execute = queue.db.execute

    async def lose_final_task_delete(statement, *args, **kwargs):
        table = getattr(statement, "table", None)
        if (
            getattr(statement, "is_delete", False)
            and getattr(table, "name", None) == "tasks"
        ):
            return MagicMock(rowcount=0)
        return await original_execute(statement, *args, **kwargs)

    with patch.object(
        queue.db,
        "execute",
        new=AsyncMock(side_effect=lose_final_task_delete),
    ):
        assert await queue.delete(task_id) is False

    queue.db.expire_all()
    assert await queue.db.get(Task, task_id) is not None
    assert await queue.db.scalar(
        select(TaskShare.id).where(TaskShare.task_id == task_id)
    ) == grant_ids[0]
    assert await queue.db.scalar(
        select(TeamTaskShare.id).where(TeamTaskShare.task_id == task_id)
    ) == grant_ids[1]


@pytest.mark.asyncio
async def test_task_creation_purges_pre_upgrade_acl_for_reused_id(queue):
    old = await queue.create(title="old incarnation", description="private")
    old_id = old.id
    queue.db.add_all(_task_access_grants(old_id, suffix="reused"))
    await queue.db.commit()

    # Simulate an older SQLite deployment deleting the Task without FK
    # enforcement or explicit ACL cleanup, then reusing its integer id.
    await queue.db.execute(delete(Task).where(Task.id == old_id))
    # Emulate the legacy allocator state that allowed the deleted highest
    # integer to be selected again.  Current Manager code still obtains the
    # value through its native allocator; it never accepts an explicit id.
    await queue.db.execute(
        text("UPDATE sqlite_sequence SET seq = 0 WHERE name = 'tasks'")
    )
    await queue.db.commit()
    assert await queue.db.scalar(
        select(TaskShare.id).where(TaskShare.task_id == old_id)
    ) is not None
    assert await queue.db.scalar(
        select(TeamTaskShare.id).where(TeamTaskShare.task_id == old_id)
    ) is not None

    # SQLite may naturally reuse the deleted highest ROWID.  Exercise the
    # canonical Manager allocator without passing an explicit id, then prove
    # that its creation boundary still purges legacy ACL rows for that value.
    replacement = await queue.create(
        title="new incarnation",
        description="must stay private",
    )
    assert replacement.id == old_id

    assert replacement.id == old_id
    assert await queue.db.scalar(
        select(TaskShare.id).where(TaskShare.task_id == old_id)
    ) is None
    assert await queue.db.scalar(
        select(TeamTaskShare.id).where(TeamTaskShare.task_id == old_id)
    ) is None


@pytest.mark.asyncio
async def test_share_write_fence_rejects_reused_task_incarnation(queue):
    old = await queue.create(title="old share owner", description="private")
    observed = SimpleNamespace(id=old.id, incarnation_id=old.incarnation_id)
    await queue.db.execute(delete(Task).where(Task.id == old.id))
    await queue.db.commit()
    # Build the legacy ABA state below the canonical Manager allocator.  The
    # production API must continue rejecting caller-selected Manager ids.
    replacement = Task(
        id=old.id,
        title="new share owner",
        description="different private task",
        # Equal timestamps prove the fence does not depend on dialect-specific
        # DateTime precision.
        created_at=old.created_at,
    )
    queue.db.add(replacement)
    await queue.db.commit()
    await queue.db.refresh(replacement)

    assert replacement.created_at == old.created_at
    assert replacement.incarnation_id != observed.incarnation_id
    assert not await lock_task_share_authority(queue.db, observed)


@pytest.mark.asyncio
async def test_canonical_task_creation_replaces_caller_incarnation(queue):
    caller_value = "0" * 32
    task = await queue.create(
        title="system-owned incarnation",
        description="caller value must be ignored",
        incarnation_id=caller_value,
    )

    assert task.incarnation_id
    assert task.incarnation_id != caller_value


@pytest.mark.asyncio
async def test_delivery_creation_purges_pre_upgrade_acl_for_next_id(queue):
    repo_full_name = f"example/delivery-acl-{id(queue)}"
    project = Project(
        name=f"delivery-acl-{id(queue)}",
        git_url=f"https://github.com/{repo_full_name}.git",
        has_remote=True,
        local_path="/tmp/delivery-acl",
        default_branch="main",
        status="ready",
    )
    queue.db.add(project)
    await queue.db.flush()
    repo = MonitoredRepo(
        repo_full_name=repo_full_name,
        project_id=project.id,
        webhook_secret="delivery-acl-secret",
        review_mode="panel",
        wait_for_ci=True,
        required_checks=["tests"],
        merge_queue_mode="manual",
        default_branch="main",
    )
    queue.db.add(repo)
    seed = await queue.create(title="retired", description="old task")
    seed_id = seed.id
    next_id = seed_id + 1
    await queue.db.execute(delete(Task).where(Task.id == seed_id))
    # Simulate an orphan grant imported from a pre-upgrade database at the id
    # SQLite AUTOINCREMENT will allocate next.  Canonical creation must purge
    # it even though structural id non-reuse is now active.
    queue.db.add_all(_task_access_grants(next_id, suffix="delivery"))
    await queue.db.commit()

    run = await create_delivery_run(
        queue.db,
        DeliveryCreateSpec(
            idempotency_key="delivery-reused-acl",
            project_id=project.id,
            monitored_repo_id=repo.id,
            title="Fresh private delivery",
            requirements="Do not inherit old readers",
        ),
    )

    assert run.developer_task_id == next_id
    assert await queue.db.scalar(
        select(TaskShare.id).where(TaskShare.task_id == next_id)
    ) is None
    assert await queue.db.scalar(
        select(TeamTaskShare.id).where(TeamTaskShare.task_id == next_id)
    ) is None


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
    await _seed_ready_worker(queue)
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
    await _seed_ready_worker(queue)
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
    await _seed_ready_worker(queue)
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
async def test_remote_delete_callback_runs_after_plan_preflight_before_local_delete(
    queue,
):
    await _seed_ready_worker(queue)
    task = await queue.create(
        title="remote plan graph",
        description="d",
        worker_id=91,
    )
    task.status = "completed"
    plan = Plan(
        title="Remote durable plan",
        initial_request="Plan the task",
        target_task_id=task.id,
        worker_id=91,
        pipeline_config={},
    )
    queue.db.add(plan)
    await queue.db.commit()
    row_ids = (task.id, plan.id)
    local_delete_seen = False
    callback_called = False
    original_execute = queue.db.execute

    async def track_local_delete(statement, *args, **kwargs):
        nonlocal local_delete_seen
        if getattr(statement, "is_delete", False):
            local_delete_seen = True
        return await original_execute(statement, *args, **kwargs)

    async def reject_remote_delete(preflight: TaskDeletePreflight) -> bool:
        nonlocal callback_called
        callback_called = True
        assert local_delete_seen is False
        assert preflight.task_id == row_ids[0]
        assert preflight.plan_ids == (row_ids[1],)
        return False

    with patch.object(
        queue.db,
        "execute",
        new=AsyncMock(side_effect=track_local_delete),
    ):
        assert await queue.delete(
            row_ids[0],
            expected_fence=task_delete_fence(task),
            remote_worker_deleted=True,
            remote_delete_confirm=reject_remote_delete,
        ) is False

    assert callback_called is True
    queue.db.expire_all()
    assert await queue.db.get(Task, row_ids[0]) is not None
    assert await queue.db.get(Plan, row_ids[1]) is not None


@pytest.mark.asyncio
async def test_local_delete_preflight_includes_plan_added_after_stale_read(queue):
    task = await queue.create(title="local Plan receipt", description="d")
    task.status = "completed"
    await queue.db.commit()
    stale_plan_ids = tuple(
        (
            await queue.db.execute(
                select(Plan.id)
                .where(Plan.target_task_id == task.id)
                .order_by(Plan.id)
            )
        ).scalars()
    )
    assert stale_plan_ids == ()

    # Simulate a Plan admitted after an API-layer observation but before
    # TaskQueue acquires the Task writer fence. The callback receipt must use
    # the graph discovered under that fence, never the stale outer read.
    plans = [
        Plan(
            title=f"Local durable Plan {index}",
            initial_request="Plan the task",
            target_task_id=task.id,
            pipeline_config={},
        )
        for index in (2, 1)
    ]
    queue.db.add_all(plans)
    await queue.db.commit()
    task_id = task.id
    expected_plan_ids = tuple(sorted(plan.id for plan in plans))
    observed: list[TaskDeletePreflight] = []

    async def capture_preflight(preflight: TaskDeletePreflight) -> bool:
        observed.append(preflight)
        return True

    assert await queue.delete(
        task_id,
        before_delete=capture_preflight,
    ) is True

    assert observed == [
        TaskDeletePreflight(task_id=task_id, plan_ids=expected_plan_ids)
    ]
    for plan_id in expected_plan_ids:
        assert await queue.db.get(Plan, plan_id) is None


@pytest.mark.asyncio
async def test_remote_delete_callback_rejects_active_worker_plan_dispatch(queue):
    await _seed_ready_worker(queue)
    task = await queue.create(
        title="active remote Plan dispatch",
        description="d",
        worker_id=91,
    )
    task.status = "completed"
    plan = Plan(
        title="Remote active dispatch",
        initial_request="Plan the task",
        target_task_id=task.id,
        worker_id=91,
        pipeline_config={},
    )
    queue.db.add(plan)
    await queue.db.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        worker_id=91,
        run_type="initial",
        status="failed",
        current_stage="failed",
        generation=2,
        finished_at=datetime.utcnow(),
    )
    queue.db.add(run)
    await queue.db.flush()
    receipt = PlanAgentWorkerDispatchReceipt(
        plan_id=plan.id,
        run_id=run.id,
        target_task_id=task.id,
        worker_id=91,
        run_generation=run.generation,
        protocol=1,
        status="prepared",
    )
    queue.db.add(receipt)
    await queue.db.commit()
    await queue.db.refresh(task)
    row_ids = (task.id, receipt.id)
    remote_delete = AsyncMock(return_value=True)

    assert await queue.delete(
        row_ids[0],
        expected_fence=task_delete_fence(task),
        remote_worker_deleted=True,
        remote_delete_confirm=remote_delete,
    ) is False

    remote_delete.assert_not_awaited()
    queue.db.expire_all()
    assert await queue.db.get(Task, row_ids[0]) is not None
    assert await queue.db.get(PlanAgentWorkerDispatchReceipt, row_ids[1]) is not None


@pytest.mark.asyncio
async def test_delete_running_task_rejected(queue):
    """Should NOT be able to delete in_progress tasks."""
    task = await queue.create(title="t", description="d", target_repo="/tmp")
    _ = await queue.dequeue()  # sets to in_progress
    result = await queue.delete(task.id)
    assert result is False


def _capability_invocation_for_delete(
    task_id: int,
    *,
    status: str,
) -> CapabilityInvocation:
    digest = "d" * 64
    return CapabilityInvocation(
        task_id=task_id,
        capability_key="plan",
        source="human_request",
        purpose="advisory",
        status=status,
        state_version=1,
        idempotency_key=f"delete-{status}",
        input_payload={},
        input_hash=digest,
        subject_kind="task_generation",
        subject_ref={"task_id": task_id},
        subject_hash=digest,
        executor_kind="fake",
        executor_config={},
        executor_config_hash=digest,
        policy_snapshot={},
        policy_hash=digest,
        resume_policy="attach_only",
        max_attempts=1,
        active_task_id=task_id if status == "queued" else None,
        error_code="finished" if status == "failed" else None,
    )


def _capability_outbox_for_delete(
    task: Task,
    invocation: CapabilityInvocation,
    *,
    status: str,
) -> CapabilityResumeOutbox:
    terminal = status in {"completed", "cancelled", "failed"}
    now = datetime.utcnow()
    return CapabilityResumeOutbox(
        task_id=task.id,
        invocation_id=invocation.id,
        active_task_id=(task.id if not terminal else None),
        active_invocation_id=(invocation.id if not terminal else None),
        status=status,
        state_version=1,
        request_task_incarnation_id=task.incarnation_id,
        request_task_retry_count=task.retry_count,
        from_turn_generation=task.turn_generation,
        request_task_session_id=task.session_id,
        request_source_log_id=101,
        request_output_log_id=102,
        request_terminal_log_id=103,
        error_code="settled" if status in {"cancelled", "failed"} else None,
        error_message=(
            "resume settled before launch"
            if status in {"cancelled", "failed"}
            else None
        ),
        created_at=now,
        updated_at=now,
        completed_at=now if terminal else None,
    )


@pytest.mark.asyncio
async def test_delete_waiting_capability_task_rejected(queue):
    task = await queue.create(title="waiting capability", description="d")
    task.status = "waiting_capability"
    await queue.db.commit()
    task_id = task.id

    assert await queue.delete(task_id) is False
    queue.db.expire_all()
    assert await queue.db.get(Task, task_id) is not None


@pytest.mark.asyncio
async def test_delete_task_rejects_active_capability(queue):
    task = await queue.create(title="active capability", description="d")
    task.status = "completed"
    invocation = _capability_invocation_for_delete(task.id, status="queued")
    queue.db.add(invocation)
    await queue.db.flush()
    execution = CapabilityExecution(
        invocation_id=invocation.id,
        attempt=1,
        status="queued",
        state_version=1,
        active_invocation_id=invocation.id,
        idempotency_key=f"{invocation.id}:1",
        executor_kind="fake",
        input_hash=invocation.input_hash,
    )
    queue.db.add(execution)
    await queue.db.commit()
    task_id = task.id
    invocation_id = invocation.id
    execution_id = execution.id

    assert await queue.delete(task_id) is False
    queue.db.expire_all()
    assert await queue.db.get(Task, task_id) is not None
    assert await queue.db.get(CapabilityInvocation, invocation_id) is not None
    assert await queue.db.get(CapabilityExecution, execution_id) is not None


@pytest.mark.asyncio
async def test_delete_task_rejects_active_capability_resume_outbox(queue):
    task = await queue.create(title="active resume outbox", description="d")
    task.status = "completed"
    invocation = _capability_invocation_for_delete(task.id, status="failed")
    queue.db.add(invocation)
    await queue.db.flush()
    outbox = _capability_outbox_for_delete(task, invocation, status="pending")
    queue.db.add(outbox)
    await queue.db.commit()
    task_id = task.id
    invocation_id = invocation.id
    outbox_id = outbox.id

    assert await queue.delete(task_id) is False
    queue.db.expire_all()
    assert await queue.db.get(Task, task_id) is not None
    assert await queue.db.get(CapabilityInvocation, invocation_id) is not None
    assert await queue.db.get(CapabilityResumeOutbox, outbox_id) is not None


@pytest.mark.asyncio
async def test_delete_task_rejects_launched_capability_resume_outbox(queue):
    task = await queue.create(title="launched resume outbox", description="d")
    task.status = "completed"
    invocation = _capability_invocation_for_delete(task.id, status="failed")
    queue.db.add(invocation)
    await queue.db.flush()
    outbox = _capability_outbox_for_delete(task, invocation, status="pending")
    outbox.status = "launched"
    outbox.state_version = 4
    outbox.active_task_id = None
    outbox.active_invocation_id = None
    outbox.invocation_terminal_status = invocation.status
    outbox.invocation_error_code = invocation.error_code
    outbox.invocation_error_message = invocation.error_message
    outbox.resume_payload = {"schema_version": 1}
    outbox.resume_payload_hash = "e" * 64
    outbox.resume_source_log_id = 104
    outbox.claimed_turn_generation = task.turn_generation + 1
    outbox.resume_actual_transport = "claude_exec"
    outbox.attempt_count = 1
    outbox.ready_at = outbox.created_at
    outbox.claimed_at = outbox.created_at
    outbox.launched_at = outbox.created_at
    queue.db.add(outbox)
    await queue.db.commit()
    row_ids = (task.id, invocation.id, outbox.id)

    assert await queue.delete(row_ids[0]) is False
    queue.db.expire_all()
    assert await queue.db.get(Task, row_ids[0]) is not None
    assert await queue.db.get(CapabilityInvocation, row_ids[1]) is not None
    assert await queue.db.get(CapabilityResumeOutbox, row_ids[2]) is not None


@pytest.mark.asyncio
async def test_delete_task_explicitly_removes_terminal_capability_history(queue):
    task = await queue.create(title="terminal capability", description="d")
    task.status = "completed"
    invocation = _capability_invocation_for_delete(task.id, status="failed")
    queue.db.add(invocation)
    await queue.db.flush()
    execution = CapabilityExecution(
        invocation_id=invocation.id,
        attempt=1,
        status="failed",
        state_version=2,
        active_invocation_id=None,
        idempotency_key=f"{invocation.id}:1",
        executor_kind="fake",
        input_hash=invocation.input_hash,
        error_code="finished",
    )
    queue.db.add(execution)
    await queue.db.flush()
    outbox = _capability_outbox_for_delete(
        task,
        invocation,
        status="cancelled",
    )
    queue.db.add(outbox)
    await queue.db.commit()
    task_id = task.id
    invocation_id = invocation.id
    execution_id = execution.id
    outbox_id = outbox.id

    assert await queue.delete(task_id) is True
    assert await queue.db.get(Task, task_id) is None
    assert await queue.db.get(CapabilityInvocation, invocation_id) is None
    assert await queue.db.get(CapabilityExecution, execution_id) is None
    assert await queue.db.get(CapabilityResumeOutbox, outbox_id) is None


@pytest.mark.asyncio
async def test_delete_task_explicitly_removes_inactive_termination_receipt(queue):
    assert await queue.db.scalar(text("PRAGMA foreign_keys")) == 0
    task = await queue.create(title="terminal receipt history", description="d")
    task.status = "completed"
    settled_at = datetime.utcnow()
    receipt = WorkerTaskTerminationReceipt(
        operation_id="e" * 32,
        task_id=task.id,
        active_task_id=None,
        side="worker",
        worker_id=None,
        operation="cancel",
        status="acknowledged",
        state_version=3,
        source_task_status="completed",
        source_task_retry_count=task.retry_count,
        source_task_turn_generation=task.turn_generation,
        request_payload={"operation": "cancel"},
        request_digest="a" * 64,
        result_payload={"ok": True},
        result_digest="b" * 64,
        accepted_at=settled_at,
        completed_at=settled_at,
        acknowledged_at=settled_at,
    )
    queue.db.add(receipt)
    await queue.db.commit()
    task_id = task.id
    operation_id = receipt.operation_id

    assert await queue.delete(task_id) is True
    queue.db.expire_all()
    assert await queue.db.get(Task, task_id) is None
    assert (
        await queue.db.get(WorkerTaskTerminationReceipt, operation_id) is None
    )


async def _completed_code_review_graph(
    queue: TaskQueue,
) -> tuple[Task, Task, CapabilityInvocation, CapabilityExecution, CodeReviewRun, CodeReviewResult]:
    developer = await queue.create(title="review developer", description="d")
    reviewer = await queue.create(title="reviewer", description="d")
    developer.status = "completed"
    reviewer.status = "completed"
    reviewer.started_at = datetime.utcnow()
    reviewer.completed_at = datetime.utcnow()

    invocation = _capability_invocation_for_delete(
        developer.id,
        status="failed",
    )
    invocation.capability_key = "code_review"
    invocation.executor_kind = "code_review"
    queue.db.add(invocation)
    await queue.db.flush()
    execution = CapabilityExecution(
        invocation_id=invocation.id,
        attempt=1,
        status="failed",
        state_version=2,
        active_invocation_id=None,
        idempotency_key=f"{invocation.id}:1",
        executor_kind="code_review",
        input_hash=invocation.input_hash,
        error_code="finished",
    )
    queue.db.add(execution)
    await queue.db.flush()
    run = CodeReviewRun(
        capability_invocation_id=invocation.id,
        capability_execution_id=execution.id,
        attempt=1,
        status="completed",
        state_version=2,
        developer_task_id=developer.id,
        reviewer_task_id=reviewer.id,
        reviewer_task_retry_count=0,
        repo_path="/repo",
        base_sha="a" * 40,
        head_sha="b" * 40,
        head_tree_sha="c" * 40,
        patch_sha256="d" * 64,
        subject_ref={"kind": "commit_range"},
        subject_hash="e" * 64,
        prompt_hash="f" * 64,
        completed_at=datetime.utcnow(),
    )
    queue.db.add(run)
    await queue.db.flush()
    result = CodeReviewResult(
        run_id=run.id,
        capability_invocation_id=invocation.id,
        capability_execution_id=execution.id,
        developer_task_id=developer.id,
        reviewer_task_id=reviewer.id,
        reviewer_task_retry_count=0,
        reviewer_task_instance_id=None,
        reviewer_task_started_at=reviewer.started_at,
        reviewer_task_completed_at=reviewer.completed_at,
        output_log_id=1,
        schema_version=1,
        role="reviewer",
        verdict="approved",
        summary="approved",
        findings=[],
        subject_ref=run.subject_ref,
        subject_hash=run.subject_hash,
        result_hash="1" * 64,
    )
    queue.db.add(result)
    await queue.db.flush()
    invocation.status = "completed"
    invocation.result_kind = "code_review_result"
    invocation.result_id = result.id
    invocation.result_hash = result.result_hash
    execution.status = "completed"
    execution.output_kind = invocation.result_kind
    execution.output_id = result.id
    execution.output_hash = result.result_hash
    await queue.db.commit()
    return developer, reviewer, invocation, execution, run, result


@pytest.mark.asyncio
async def test_delete_task_preserves_code_review_aggregate(queue):
    graph = await _completed_code_review_graph(queue)
    developer, reviewer, invocation, execution, run, result = graph
    row_ids = (
        developer.id,
        reviewer.id,
        invocation.id,
        execution.id,
        run.id,
        result.id,
    )

    assert await queue.delete(row_ids[0]) is False
    assert await queue.delete(row_ids[1]) is False

    for model, row_id in zip(
        (
            Task,
            Task,
            CapabilityInvocation,
            CapabilityExecution,
            CodeReviewRun,
            CodeReviewResult,
        ),
        row_ids,
        strict=True,
    ):
        assert await queue.db.get(model, row_id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse_row", ["run", "result"])
async def test_delete_task_preserves_code_review_reverse_linked_by_capability_ids(
    queue,
    reverse_row,
):
    graph = await _completed_code_review_graph(queue)
    owner, reviewer, invocation, execution, run, result = graph
    wrong_developer = await queue.create(
        title="mislinked review developer",
        description="d",
    )
    wrong_developer.status = "completed"
    alternate_invocation = _capability_invocation_for_delete(
        wrong_developer.id,
        status="failed",
    )
    alternate_invocation.capability_key = "code_review"
    alternate_invocation.executor_kind = "code_review"
    queue.db.add(alternate_invocation)
    await queue.db.flush()
    alternate_execution = CapabilityExecution(
        invocation_id=alternate_invocation.id,
        attempt=1,
        status="failed",
        state_version=2,
        active_invocation_id=None,
        idempotency_key=f"{alternate_invocation.id}:1",
        executor_kind="code_review",
        input_hash=alternate_invocation.input_hash,
        error_code="finished",
    )
    queue.db.add(alternate_execution)
    await queue.db.flush()
    run.developer_task_id = wrong_developer.id
    result.developer_task_id = wrong_developer.id
    if reverse_row == "run":
        result.capability_invocation_id = alternate_invocation.id
        result.capability_execution_id = alternate_execution.id
    else:
        run.capability_invocation_id = alternate_invocation.id
        run.capability_execution_id = alternate_execution.id
    await queue.db.commit()
    row_ids = (
        owner.id,
        reviewer.id,
        wrong_developer.id,
        invocation.id,
        execution.id,
        run.id,
        result.id,
        alternate_invocation.id,
        alternate_execution.id,
    )

    assert await queue.delete(owner.id) is False

    for model, row_id in zip(
        (
            Task,
            Task,
            Task,
            CapabilityInvocation,
            CapabilityExecution,
            CodeReviewRun,
            CodeReviewResult,
            CapabilityInvocation,
            CapabilityExecution,
        ),
        row_ids,
        strict=True,
    ):
        assert await queue.db.get(model, row_id) is not None


@pytest.mark.asyncio
async def test_delete_task_cascades_terminal_first_class_plan_aggregate(queue):
    task = await queue.create(title="plan target", description="d")
    task.status = "completed"
    plan = Plan(
        title="Durable plan",
        initial_request="Plan the task",
        target_task_id=task.id,
        pipeline_config={},
    )
    queue.db.add(plan)
    await queue.db.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        run_type="initial",
        status="completed",
        current_stage="complete",
        finished_at=datetime.utcnow(),
    )
    queue.db.add(run)
    await queue.db.flush()
    version = PlanVersion(
        plan_id=plan.id,
        version_number=1,
        produced_by_run_id=run.id,
        content="terminal plan",
    )
    queue.db.add(version)
    await queue.db.flush()
    run.result_version_id = version.id
    plan.current_version_id = version.id
    await queue.db.commit()
    task_id = task.id
    plan_id = plan.id
    run_id = run.id
    version_id = version.id

    assert await queue.delete(task_id) is True
    assert await queue.db.get(Task, task_id) is None
    assert await queue.db.get(Plan, plan_id) is None
    assert await queue.db.get(PlanAgentRun, run_id) is None
    assert await queue.db.get(PlanVersion, version_id) is None


@pytest.mark.asyncio
async def test_delete_execution_task_preserves_external_plan_audit(queue):
    execution_task = await queue.create(
        title="materialized Plan execution",
        description="delete only this Task",
    )
    execution_task.status = "completed"
    plan = Plan(
        title="external standalone Plan",
        initial_request="retain the audit",
        pipeline_config={},
    )
    queue.db.add(plan)
    await queue.db.flush()
    version = PlanVersion(
        plan_id=plan.id,
        version_number=1,
        content="approved implementation",
    )
    queue.db.add(version)
    await queue.db.flush()
    application = PlanApplication(
        plan_id=plan.id,
        plan_version_id=version.id,
        application_type="execution_task",
        execution_task_id=execution_task.id,
    )
    attempt = PlanApplicationAttempt(
        plan_id=plan.id,
        plan_version_id=version.id,
        application_receipt_key="execution-task-history",
        application_type="execution_task",
        execution_task_id=execution_task.id,
        application_created_at=datetime.utcnow(),
        released_at=datetime.utcnow(),
    )
    queue.db.add_all([application, attempt])
    await queue.db.commit()
    row_ids = (
        execution_task.id,
        plan.id,
        version.id,
        application.id,
        attempt.id,
    )

    assert await queue.delete(row_ids[0]) is True
    assert await queue.db.get(Task, row_ids[0]) is None
    assert await queue.db.get(Plan, row_ids[1]) is not None
    assert await queue.db.get(PlanVersion, row_ids[2]) is not None
    preserved_application = await queue.db.get(PlanApplication, row_ids[3])
    preserved_attempt = await queue.db.get(PlanApplicationAttempt, row_ids[4])
    assert preserved_application.execution_task_id == row_ids[0]
    assert preserved_attempt.execution_task_id == row_ids[0]


@pytest.mark.asyncio
async def test_delete_task_rejects_active_first_class_plan_aggregate(queue):
    task = await queue.create(title="active plan target", description="d")
    task.status = "completed"
    plan = Plan(
        title="Active durable plan",
        initial_request="Plan the task",
        target_task_id=task.id,
        pipeline_config={},
    )
    queue.db.add(plan)
    await queue.db.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        run_type="initial",
        status="planning",
        current_stage="planner",
    )
    queue.db.add(run)
    await queue.db.flush()
    plan.active_run_id = run.id
    await queue.db.commit()
    row_ids = (task.id, plan.id, run.id)

    assert await queue.delete(row_ids[0]) is False
    assert await queue.db.get(Task, row_ids[0]) is not None
    assert await queue.db.get(Plan, row_ids[1]) is not None
    assert await queue.db.get(PlanAgentRun, row_ids[2]) is not None


@pytest.mark.asyncio
async def test_delete_task_cascades_terminal_capability_plan_aggregate(queue):
    task = await queue.create(title="plan capability target", description="d")
    task.status = "completed"
    invocation = _capability_invocation_for_delete(task.id, status="failed")
    invocation.capability_key = "plan"
    invocation.executor_kind = "plan_agent"
    queue.db.add(invocation)
    await queue.db.flush()
    execution = CapabilityExecution(
        invocation_id=invocation.id,
        attempt=1,
        status="failed",
        state_version=2,
        active_invocation_id=None,
        idempotency_key=f"{invocation.id}:1",
        executor_kind="plan_agent",
        input_hash=invocation.input_hash,
        error_code="finished",
    )
    queue.db.add(execution)
    await queue.db.flush()
    plan = Plan(
        title="Capability durable plan",
        initial_request="Plan the task",
        target_task_id=task.id,
        pipeline_config={},
    )
    queue.db.add(plan)
    await queue.db.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        capability_execution_id=execution.id,
        run_type="capability",
        status="failed",
        current_stage="planner",
        finished_at=datetime.utcnow(),
    )
    queue.db.add(run)
    await queue.db.flush()
    execution.handle_kind = "plan_agent_run"
    execution.handle_id = str(run.id)
    execution.handle_generation = 0
    outbox = _capability_outbox_for_delete(
        task,
        invocation,
        status="cancelled",
    )
    outbox.invocation_terminal_status = invocation.status
    outbox.invocation_error_code = invocation.error_code
    outbox.invocation_error_message = invocation.error_message
    queue.db.add(outbox)
    await queue.db.commit()
    row_ids = (
        task.id,
        invocation.id,
        execution.id,
        outbox.id,
        plan.id,
        run.id,
    )

    assert await queue.delete(row_ids[0]) is True
    assert await queue.db.get(Task, row_ids[0]) is None
    assert await queue.db.get(CapabilityInvocation, row_ids[1]) is None
    assert await queue.db.get(CapabilityExecution, row_ids[2]) is None
    assert await queue.db.get(CapabilityResumeOutbox, row_ids[3]) is None
    assert await queue.db.get(Plan, row_ids[4]) is None
    assert await queue.db.get(PlanAgentRun, row_ids[5]) is None


@pytest.mark.asyncio
async def test_delete_task_rolls_back_plan_graph_when_final_task_cas_loses(queue):
    task = await queue.create(title="plan CAS owner", description="keep graph")
    task.status = "completed"
    plan = Plan(
        title="CAS durable plan",
        initial_request="Plan the task",
        target_task_id=task.id,
        pipeline_config={},
    )
    queue.db.add(plan)
    await queue.db.commit()
    row_ids = (task.id, plan.id)
    original_execute = queue.db.execute

    async def lose_final_task_delete(statement, *args, **kwargs):
        table = getattr(statement, "table", None)
        if (
            getattr(statement, "is_delete", False)
            and getattr(table, "name", None) == "tasks"
        ):
            return MagicMock(rowcount=0)
        return await original_execute(statement, *args, **kwargs)

    with patch.object(
        queue.db,
        "execute",
        new=AsyncMock(side_effect=lose_final_task_delete),
    ):
        assert await queue.delete(row_ids[0]) is False

    queue.db.expire_all()
    assert await queue.db.get(Task, row_ids[0]) is not None
    assert await queue.db.get(Plan, row_ids[1]) is not None


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

    with patch("backend.services.process_identity.os.kill", return_value=None):
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
        "backend.services.process_identity.os.kill",
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
        "backend.services.process_identity.os.kill",
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
async def test_delete_task_rejects_turn_generation_only_aba(queue):
    """A stale delete fence cannot erase a newer turn with legacy fields equal."""

    task = await queue.create(title="delete turn ABA", description="work")
    task.status = "completed"
    await queue.db.commit()
    task_id = task.id
    stale_fence = task_delete_fence(task)

    task.turn_generation += 1
    await queue.db.commit()

    assert not await queue.delete(task_id, expected_fence=stale_fence)
    queue.db.expire_all()
    current = await queue.get(task_id)
    assert current is not None
    assert current.status == "completed"
    assert current.turn_generation == stale_fence[-1] + 1


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


@pytest.mark.asyncio
async def test_ordinary_task_lists_show_only_stable_pr_monitor_display_task(queue):
    ordinary = await queue.create(
        title="ordinary",
        description="user-visible work",
        target_repo="/tmp",
    )
    ordinary_archived = await queue.create(
        title="ordinary archived",
        description="user-visible history",
        target_repo="/tmp",
        archived=True,
    )
    single_task = await queue.create(
        title="single reviewer",
        description="internal protocol",
        target_repo="/tmp",
        archived=True,
    )
    display_task = await queue.create(
        title="PR Review: example/hidden-reviewer-tasks#17",
        description="safe result projection",
        target_repo="",
        status="completed",
        metadata_={
            "pr_monitor_display": True,
            "pr_monitor_run_id": 1,
            "pr_monitor_review_id": 1,
        },
    )
    panel_task = await queue.create(
        title="panel reviewer",
        description="internal protocol",
        target_repo="/tmp",
        archived=True,
    )
    fix_task = await queue.create(
        title="finding fix",
        description="internal patch protocol",
        target_repo="/tmp",
    )
    rebuttal_task = await queue.create(
        title="rebuttal adjudication",
        description="internal adjudication protocol",
        target_repo="/tmp",
        archived=True,
    )
    repo = MonitoredRepo(
        repo_full_name="example/hidden-reviewer-tasks",
        webhook_secret="queue-secret",
    )
    queue.db.add(repo)
    await queue.db.flush()
    review = PRReview(
        repo_id=repo.id,
        pr_number=17,
        base_ref="main",
        base_sha="a" * 40,
        head_sha="b" * 40,
        pr_title="Internal review",
        pr_author="alice",
        pr_url="https://github.com/example/hidden-reviewer-tasks/pull/17",
        task_id=single_task.id,
        status="reviewing",
    )
    queue.db.add(review)
    await queue.db.flush()
    reviewer_run = PRReviewerRun(
        pr_review_id=review.id,
        role="principal_engineer",
        task_id=panel_task.id,
        provider="claude",
        status="pending",
        prompt_policy_hash="c" * 64,
        guide_pack_hash="d" * 64,
    )
    queue.db.add(reviewer_run)
    await queue.db.flush()
    monitor_run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=17,
        current_base_sha="a" * 40,
        current_head_sha="b" * 40,
        current_review_id=review.id,
        display_task_id=display_task.id,
    )
    queue.db.add(monitor_run)
    await queue.db.flush()
    review.monitor_run_id = monitor_run.id
    finding = PRFinding(
        pr_review_id=review.id,
        reviewer_run_id=reviewer_run.id,
        fingerprint="e" * 64,
        role="principal_engineer",
        severity="high",
        category="correctness",
        path="backend/internal.py",
        line=1,
        title="Internal finding",
        evidence="internal evidence",
        impact="internal impact",
        required_fix="internal fix",
        test="internal test",
        thread_nonce="f" * 48,
        base_sha="a" * 40,
        head_sha="b" * 40,
    )
    queue.db.add(finding)
    await queue.db.flush()
    queue.db.add_all([
        PRFindingAction(
            finding_id=finding.id,
            action_type="ai_fix",
            status="completed",
            idempotency_key="queue-hidden-fix",
            task_id=fix_task.id,
            expected_head_sha="b" * 40,
        ),
        PRFindingRebuttal(
            finding_id=finding.id,
            pr_review_id=review.id,
            monitor_run_id=monitor_run.id,
            developer_task_id=ordinary.id,
            task_id=rebuttal_task.id,
            attempt=1,
            base_sha="a" * 40,
            head_sha="b" * 40,
            evidence="bounded evidence",
            evidence_hash="1" * 64,
            status="completed",
            resolution_nonce="2" * 48,
        ),
    ])
    await queue.db.commit()

    from backend.services.pr_monitor_task_access import is_pr_monitor_owned_task

    assert not await is_pr_monitor_owned_task(queue.db, ordinary)
    for internal_task in (single_task, panel_task, fix_task, rebuttal_task):
        assert await is_pr_monitor_owned_task(queue.db, internal_task)

    assert {task.id for task in await queue.list_tasks()} == {ordinary.id, display_task.id}
    assert {
        task.id for task in await queue.list_tasks(archived_only=True)
    } == {ordinary_archived.id}
    assert {
        task.id for task in await queue.list_tasks(include_archived=True)
    } == {ordinary.id, ordinary_archived.id, display_task.id}
    assert await queue.count_tasks() == 2
    assert await queue.count_tasks(archived_only=True) == 1
    assert await queue.count_tasks(include_archived=True) == 3


@pytest.mark.parametrize(
    ("link_kind", "review_status", "run_status", "should_claim"),
    [
        ("none", None, None, True),
        ("tombstone", None, None, False),
        ("single", "reviewing", None, True),
        ("single", "error", None, False),
        ("panel", "reviewing", "pending", True),
        ("panel", "reviewing", "reviewing", True),
        ("panel", "reviewing", "cancelled", False),
        ("panel", "error", "pending", False),
    ],
    ids=(
        "ordinary",
        "deleted-owner-tombstone",
        "single-runnable",
        "single-parent-terminal",
        "panel-pending",
        "panel-running",
        "panel-sibling-cancelled",
        "panel-parent-terminal",
    ),
)
@pytest.mark.asyncio
async def test_dequeue_requires_runnable_pr_review_owner(
    queue,
    link_kind,
    review_status,
    run_status,
    should_claim,
):
    task = await queue.create(
        title=f"{link_kind} reviewer dispatch",
        description="internal protocol",
        target_repo="/tmp",
    )
    task_id = task.id
    if link_kind == "tombstone":
        queue.db.add(PRMonitorTaskTombstone(task_id=task_id))
        await queue.db.commit()
    elif link_kind != "none":
        repo = MonitoredRepo(
            repo_full_name=f"example/dispatch-{link_kind}-{task_id}",
            webhook_secret="queue-secret",
        )
        queue.db.add(repo)
        await queue.db.flush()
        review = PRReview(
            repo_id=repo.id,
            pr_number=task_id,
            base_ref="main",
            base_sha="a" * 40,
            head_sha="b" * 40,
            pr_title="Dispatch ownership",
            pr_author="alice",
            pr_url=f"https://github.com/{repo.repo_full_name}/pull/{task_id}",
            # Panel creation keeps this legacy pointer to its first Task.  It
            # must not override a terminal PRReviewerRun below.
            task_id=task_id,
            status=review_status,
        )
        queue.db.add(review)
        await queue.db.flush()
        if link_kind == "panel":
            queue.db.add(PRReviewerRun(
                pr_review_id=review.id,
                role="principal_engineer",
                task_id=task_id,
                provider="claude",
                status=run_status,
                prompt_policy_hash="c" * 64,
                guide_pack_hash="d" * 64,
            ))
        await queue.db.commit()

    claimed = await queue.dequeue()

    if should_claim:
        assert claimed is not None and claimed.id == task_id
        assert claimed.status == "in_progress"
        assert claimed.turn_generation == 1
    else:
        assert claimed is None
        current = await queue.db.get(Task, task_id, populate_existing=True)
        assert current is not None
        assert current.status == "pending"
        assert current.turn_generation == 0


@pytest.mark.asyncio
async def test_dequeue_claim_cas_rechecks_cancelled_panel_reviewer_run(
    queue,
    monkeypatch,
):
    task = await queue.create(
        title="panel cancellation race",
        description="must remain pending",
        target_repo="/tmp",
    )
    repo = MonitoredRepo(
        repo_full_name="example/panel-cancellation-race",
        webhook_secret="queue-secret",
    )
    queue.db.add(repo)
    await queue.db.flush()
    review = PRReview(
        repo_id=repo.id,
        pr_number=17,
        base_ref="main",
        base_sha="a" * 40,
        head_sha="b" * 40,
        pr_title="Cancellation race",
        pr_author="alice",
        pr_url="https://github.com/example/panel-cancellation-race/pull/17",
        task_id=task.id,
        status="reviewing",
    )
    queue.db.add(review)
    await queue.db.flush()
    reviewer_run = PRReviewerRun(
        pr_review_id=review.id,
        role="principal_engineer",
        task_id=task.id,
        provider="claude",
        status="pending",
        prompt_policy_hash="c" * 64,
        guide_pack_hash="d" * 64,
    )
    queue.db.add(reviewer_run)
    await queue.db.flush()
    task_id = task.id
    reviewer_run_id = reviewer_run.id
    await queue.db.commit()

    original_execute = queue.db.execute
    cancelled = False

    async def cancel_immediately_before_task_claim(statement, *args, **kwargs):
        nonlocal cancelled
        table = getattr(statement, "table", None)
        if (
            not cancelled
            and getattr(statement, "is_update", False)
            and getattr(table, "name", None) == "tasks"
        ):
            cancelled = True
            await original_execute(
                update(PRReviewerRun)
                .where(PRReviewerRun.id == reviewer_run_id)
                .values(status="cancelled")
            )
            await queue.db.commit()
        return await original_execute(statement, *args, **kwargs)

    monkeypatch.setattr(
        queue.db,
        "execute",
        AsyncMock(side_effect=cancel_immediately_before_task_claim),
    )

    claimed = await queue.dequeue()

    assert cancelled
    assert claimed is None
    current = await queue.db.get(Task, task_id, populate_existing=True)
    assert current is not None
    assert current.status == "pending"
    assert current.turn_generation == 0


@pytest.mark.asyncio
async def test_member_task_list_and_count_require_chat_share_permission(queue):
    chat_task = await queue.create(
        title="chat-shared",
        description="d",
        target_repo="/tmp",
        created_by=7,
    )
    unrelated_permission_task = await queue.create(
        title="not-chat-shared",
        description="d",
        target_repo="/tmp",
        created_by=7,
    )
    queue.db.add_all([
        TeamTaskShare(
            task_id=chat_task.id,
            target_type="user",
            target_id=42,
            permission="chat",
            shared_by=7,
        ),
        # Keep this deliberately outside the current API Literal.  The query
        # must remain least-privilege if another permission is introduced or
        # a legacy/corrupt row exists in the database.
        TeamTaskShare(
            task_id=unrelated_permission_task.id,
            target_type="user",
            target_id=42,
            permission="read_metadata",
            shared_by=7,
        ),
    ])
    await queue.db.commit()

    visible = await queue.list_tasks(user_id=42)

    assert [task.id for task in visible] == [chat_task.id]
    assert await queue.count_tasks(user_id=42) == 1


@pytest.mark.asyncio
async def test_member_cannot_enumerate_display_task_from_internal_project_share(queue):
    ordinary_project = Project(name="member-pr-project", status="ready")
    internal_project = Project(
        name="PR-Monitor",
        tags=["ccm:internal:pr-monitor"],
        status="ready",
        show_in_selector=False,
    )
    queue.db.add_all([ordinary_project, internal_project])
    await queue.db.flush()
    queue.db.add(TeamProjectShare(
        project_id=ordinary_project.id,
        target_type="user",
        target_id=42,
        shared_by=1,
    ))
    queue.db.add(TeamProjectShare(
        project_id=internal_project.id,
        target_type="user",
        target_id=42,
        shared_by=1,
    ))
    repo = MonitoredRepo(
        repo_full_name="example/member-display",
        project_id=ordinary_project.id,
        webhook_secret="queue-secret",
    )
    queue.db.add(repo)
    await queue.db.flush()
    display = await queue.create(
        title="PR Review: example/member-display#1",
        description="safe result",
        project_id=internal_project.id,
        status="completed",
        metadata_={"pr_monitor_display": True},
    )
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=1,
        current_base_sha="a" * 40,
        current_head_sha="b" * 40,
        display_task_id=display.id,
    )
    queue.db.add(run)
    await queue.db.commit()

    visible = await queue.list_tasks(user_id=42)
    assert [task.id for task in visible] == [display.id]
    assert await queue.count_tasks(user_id=42) == 1


@pytest.mark.asyncio
async def test_staged_display_marker_is_hidden_until_run_link_is_present(queue):
    staged = await queue.create(
        title="PR Review: staged",
        description="safe result",
        status="completed",
        metadata_={"pr_monitor_display": True},
    )

    assert await queue.list_tasks() == []
    assert await queue.count_tasks() == 0


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


@pytest.mark.parametrize(
    "dialect",
    [sqlite.dialect(), postgresql.dialect(), mysql.dialect()],
    ids=("sqlite", "postgresql", "mysql"),
)
def test_pr_review_task_predicates_compile_for_supported_database_dialects(
    dialect,
):
    list_statement = select(Task.id).where(
        ordinary_task_visibility_predicate()
    )
    claim_statement = (
        update(Task)
        .where(pr_review_dispatch_predicate())
        .values(status="in_progress")
    )

    list_sql = str(list_statement.compile(dialect=dialect))
    claim_sql = str(claim_statement.compile(dialect=dialect))

    assert "pr_reviews" in list_sql
    assert "pr_reviewer_runs" in list_sql
    assert "pr_finding_actions" in list_sql
    assert "pr_finding_rebuttals" in list_sql
    assert "pr_monitor_task_tombstones" in list_sql
    assert "code_review_runs" in list_sql
    assert "delivery_developer_task" in list_sql
    assert "delivery_run_id" in list_sql
    assert "NOT ((EXISTS" in list_sql
    assert "pr_reviews" in claim_sql
    assert "pr_reviewer_runs" in claim_sql
    assert "pr_monitor_task_tombstones" in claim_sql
    assert "EXISTS" in claim_sql


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
