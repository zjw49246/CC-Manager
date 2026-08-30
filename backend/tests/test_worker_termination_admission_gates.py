"""Admission fences owned by durable Worker Task termination receipts."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

import pytest
from fastapi import HTTPException

import backend.main as main_module
import backend.services.dispatcher as dispatcher_module
import backend.services.worker_proxy as worker_proxy_module
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from backend.models.instance import Instance
from backend.models.log_entry import LogEntry
from backend.models.plan import Plan
from backend.models.plan_agent import PlanAgentRun
from backend.models.project import Project
from backend.models.sub_agent import SubAgentSession
from backend.models.task import Task
from backend.models.worker import Worker
from backend.models.worker_task_termination import (
    WorkerTaskTerminationReceipt,
)
from backend.services.dispatcher import (
    GlobalDispatcher,
    QueuedMessage,
    QueuedMessagePrelaunchError,
)
from backend.services.task_migrator import MigrationError, TaskMigrator
from backend.services.skill_context import (
    USER_SKILL_SNAPSHOTS_METADATA_KEY,
    WORKER_MANAGED_TASK_METADATA_KEY,
)
from backend.services.test_harness_owner_fence import (
    test_harness_owner_identity as _test_harness_owner_identity,
)
from backend.services.worker_proxy import (
    WorkerProxy,
    WorkerTaskForwardAdmissionBlockedError,
)
from backend.services import worker_task_termination as termination


pytestmark = pytest.mark.usefixtures("worker_control_plane_auth")


def _active_receipt(
    task: Task,
    *,
    manager_worker_id: int | None = None,
) -> WorkerTaskTerminationReceipt:
    """Build the smallest constraint-valid active receipt for one Task."""

    manager_side = manager_worker_id is not None
    now = datetime.utcnow()
    return WorkerTaskTerminationReceipt(
        operation_id=uuid.uuid4().hex,
        task_id=task.id,
        active_task_id=task.id,
        side="manager" if manager_side else "worker",
        worker_id=manager_worker_id,
        operation="stop_session",
        status="pending_remote" if manager_side else "accepted",
        state_version=1,
        source_task_incarnation_id=task.incarnation_id,
        source_task_status=task.status,
        source_task_retry_count=task.retry_count,
        source_task_turn_generation=task.turn_generation,
        source_task_source_log_id=task.turn_source_log_id,
        source_task_instance_id=task.instance_id,
        source_task_started_at=task.started_at,
        source_task_completed_at=task.completed_at,
        source_task_session_id=task.session_id,
        source_task_pty_background_generation=(
            task.pty_background_generation
        ),
        request_payload={"test": "admission-gate", "task_id": task.id},
        request_digest="d" * 64,
        attempt_count=0,
        reconcile_count=0,
        next_reconcile_at=now,
        accepted_at=None if manager_side else now,
        created_at=now,
        updated_at=now,
    )


async def _local_task(session_factory, **fields) -> Task:
    fields.setdefault("description", "durable termination gate")
    fields.setdefault("status", "completed")
    async with session_factory() as db:
        task = Task(title="termination gate", **fields)
        db.add(task)
        await db.commit()
        await db.refresh(task)
        return task


async def _persist_receipt(
    session_factory,
    task_id: int,
    *,
    manager_worker_id: int | None = None,
) -> WorkerTaskTerminationReceipt:
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task is not None
        receipt = _active_receipt(
            task,
            manager_worker_id=manager_worker_id,
        )
        db.add(receipt)
        await db.commit()
        return receipt


async def _execute_pending_worker_receipt(
    session_factory,
    *,
    operation: str,
) -> tuple[int, WorkerTaskTerminationReceipt]:
    """Stage the real active fence, then execute its receipt-owned cleanup."""

    async with session_factory() as db:
        task = Task(
            title=f"receipt-owned {operation}",
            description="active receipt must not block its own executor",
            status="pending",
            metadata_={WORKER_MANAGED_TASK_METADATA_KEY: True},
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id
        operation_id = uuid.uuid4().hex
        payload = {
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
        staged = await termination.stage_worker_receipt(
            db,
            task_id=task.id,
            operation_id=operation_id,
            operation=operation,
            request_payload=payload,
            request_digest=termination.canonical_json_digest(payload),
        )
        assert staged.active_task_id == task_id
        result = await termination.execute_worker_receipt(db, operation_id)
        return task_id, result


@pytest.mark.asyncio
async def test_active_receipt_executes_its_own_local_cancel(session_factory):
    task_id, receipt = await _execute_pending_worker_receipt(
        session_factory,
        operation="cancel",
    )

    assert receipt.status == "succeeded"
    assert receipt.active_task_id == task_id
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task is not None
        assert task.status == "cancelled"
        assert await termination.active_worker_task_termination_receipt(
            db, task_id
        ) is not None


@pytest.mark.asyncio
async def test_active_receipt_executes_its_own_local_pending_stop(session_factory):
    task_id, receipt = await _execute_pending_worker_receipt(
        session_factory,
        operation="stop_session",
    )

    assert receipt.status == "succeeded"
    assert receipt.active_task_id == task_id
    assert receipt.result_payload["response"]["recovered_pending"] is True
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        assert task is not None
        assert task.status == "completed"
        assert await termination.active_worker_task_termination_receipt(
            db, task_id
        ) is not None


@pytest.mark.asyncio
async def test_receipt_publication_bypass_rejects_different_operation_id(
    session_factory,
):
    """Naming receipt B must not bypass the active receipt A SQL gate."""

    from backend.services.task_termination import lock_task_generation

    async with session_factory() as db:
        task = Task(
            title="exact receipt publication bypass",
            description="only the active operation may publish",
            status="cancelled",
            retry_count=2,
            turn_generation=3,
        )
        db.add(task)
        await db.flush()
        receipt = _active_receipt(task)
        receipt.status = "executing"
        receipt.state_version = 2
        receipt.execution_token = "a" * 32
        receipt.next_reconcile_at = datetime.utcnow() + timedelta(seconds=90)
        db.add(receipt)
        await db.commit()
        task_id = task.id
        operation_id = receipt.operation_id

    async with session_factory() as db:
        wrong = await lock_task_generation(
            task_id,
            db,
            expected_status="cancelled",
            expected_retry_count=2,
            expected_turn_generation=3,
            expected_instance_id=None,
            expected_started_at=None,
            expected_completed_at=None,
            expected_pty_background_generation=None,
            allow_worker_termination_operation_id="f" * 32,
        )
        assert wrong is None

        exact = await lock_task_generation(
            task_id,
            db,
            expected_status="cancelled",
            expected_retry_count=2,
            expected_turn_generation=3,
            expected_instance_id=None,
            expected_started_at=None,
            expected_completed_at=None,
            expected_pty_background_generation=None,
            allow_worker_termination_operation_id=operation_id,
            worker_termination_operation="stop_session",
            worker_termination_execution_token="a" * 32,
            worker_termination_state_version=2,
        )
        assert exact is not None
        assert exact.id == task_id
        await db.rollback()


@pytest.mark.asyncio
async def test_stop_task_process_bypasses_only_exact_worker_receipt(
    session_factory,
):
    """Ordinary stops yield; only the proven Worker receipt may stop directly."""

    from backend.services.task_termination import stop_task_process

    async with session_factory() as db:
        receipt_task = Task(
            title="exact receipt process stop",
            description="receipt-owned process cleanup",
            status="executing",
            retry_count=1,
            turn_generation=4,
        )
        ordinary_task = Task(
            title="ordinary process stop",
            description="must yield to receipt arbitration",
            status="executing",
            retry_count=0,
            turn_generation=2,
        )
        db.add_all([receipt_task, ordinary_task])
        await db.flush()
        receipt = _active_receipt(receipt_task)
        receipt.status = "executing"
        receipt.state_version = 2
        receipt.execution_token = "b" * 32
        receipt.next_reconcile_at = datetime.utcnow() + timedelta(seconds=90)
        db.add(receipt)
        await db.commit()
        receipt_task_id = receipt_task.id
        ordinary_task_id = ordinary_task.id
        operation_id = receipt.operation_id

    instance_manager = MagicMock()
    instance_manager.stop = AsyncMock(return_value=True)
    with patch("backend.main.instance_manager", instance_manager):
        async with session_factory() as db:
            wrong = await stop_task_process(
                receipt_task_id,
                db,
                expected_generations=[(91, 901, None)],
                expected_task_turn_generation=4,
                task_status="cancelled",
                worker_termination_operation_id="e" * 32,
                worker_termination_operation="stop_session",
                worker_termination_execution_token="b" * 32,
                worker_termination_state_version=2,
            )
            assert wrong is False
            instance_manager.stop.assert_not_awaited()

            exact = await stop_task_process(
                receipt_task_id,
                db,
                expected_generations=[(91, 901, None)],
                expected_task_turn_generation=4,
                task_status="cancelled",
                worker_termination_operation_id=operation_id,
                worker_termination_operation="stop_session",
                worker_termination_execution_token="b" * 32,
                worker_termination_state_version=2,
            )
            assert exact is True
            exact_call = instance_manager.stop.await_args
            assert (
                exact_call.kwargs["yield_to_worker_task_termination"]
                is False
            )
            assert (
                exact_call.kwargs["worker_termination_operation_id"]
                == operation_id
            )
            assert (
                exact_call.kwargs["worker_termination_execution_token"]
                == "b" * 32
            )
            assert exact_call.kwargs["worker_termination_state_version"] == 2

            instance_manager.stop.reset_mock()
            ordinary = await stop_task_process(
                ordinary_task_id,
                db,
                expected_generations=[(92, 902, None)],
                expected_task_turn_generation=2,
                task_status="completed",
            )
            assert ordinary is True
            ordinary_call = instance_manager.stop.await_args
            assert (
                ordinary_call.kwargs["yield_to_worker_task_termination"]
                is True
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "terminal_status"),
    (
        pytest.param("cancel", "cancelled", id="cancel"),
        pytest.param("stop_session", "completed", id="stop-session"),
        pytest.param("supersede", "completed", id="supersede"),
    ),
)
async def test_running_owner_receipt_executes_exact_non_yielding_stop(
    session_factory,
    operation,
    terminal_status,
):
    """The real receipt executor may bypass only for its running generation."""

    started_at = datetime(2026, 8, 7, 2, 3, 4)
    operation_id = uuid.uuid4().hex
    async with session_factory() as db:
        task = Task(
            title=f"running receipt-owned {operation}",
            description="exact receipt owns running process cleanup",
            status="executing",
            retry_count=1,
            turn_generation=4,
            started_at=started_at,
            tags=["pr-review"] if operation == "supersede" else [],
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name=f"running receipt slot {operation}",
            status="running",
            pid=940_041,
            started_at=started_at,
            current_task_id=task.id,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        await db.commit()
        task_id = task.id
        instance_id = instance.id

        payload = {
            "version": 2,
            "operation_id": operation_id,
            "task_id": task_id,
            "operation": operation,
            "manager_worker_id": 41,
            "expected_remote": {
                "status": "executing",
                "retry_count": 1,
                "turn_generation": 4,
            },
            "manager_handoff": None,
        }
        await termination.stage_worker_receipt(
            db,
            task_id=task_id,
            operation_id=operation_id,
            operation=operation,
            request_payload=payload,
            request_digest=termination.canonical_json_digest(payload),
        )

    async def stop_exact(stopped_instance_id, **kwargs):
        assert stopped_instance_id == instance_id
        assert kwargs["expected_task_id"] == task_id
        assert kwargs["expected_task_turn_generation"] == 4
        assert kwargs["expected_pid"] == 940_041
        assert kwargs["expected_started_at"] == started_at
        assert kwargs["task_status"] == terminal_status
        assert kwargs["yield_to_worker_task_termination"] is False
        assert kwargs["worker_termination_operation_id"] == operation_id
        assert kwargs.get("allow_delivery_effect_stop", False) is (
            operation == "supersede"
        )
        async with session_factory() as stop_db:
            stopped_task = await stop_db.get(Task, task_id)
            stopped_instance = await stop_db.get(Instance, instance_id)
            assert stopped_task is not None
            assert stopped_instance is not None
            stopped_task.status = terminal_status
            stopped_task.completed_at = datetime.utcnow()
            stopped_instance.status = "idle"
            stopped_instance.pid = None
            stopped_instance.started_at = None
            stopped_instance.current_task_id = None
            await stop_db.commit()
        return True

    with (
        patch.object(
            main_module.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
            return_value=0,
        ),
        patch.object(
            main_module.instance_manager,
            "wait_for_task_launch_barrier",
            new_callable=AsyncMock,
            return_value=True,
        ),
        patch.object(
            main_module.instance_manager,
            "stop",
            new_callable=AsyncMock,
            side_effect=stop_exact,
        ) as stop,
    ):
        async with session_factory() as db:
            receipt = await termination.execute_worker_receipt(
                db,
                operation_id,
            )

    assert receipt.status == "succeeded"
    assert receipt.result_payload["task"]["status"] == terminal_status
    stop.assert_awaited_once()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task is not None
        assert task.status == terminal_status
        assert instance is not None
        assert instance.status == "idle"
        assert instance.pid is None
        assert instance.current_task_id is None


@pytest.mark.asyncio
async def test_lease_takeover_after_queue_abort_blocks_later_plan_stop(
    session_factory,
):
    """A stale executor may drain its queue but cannot kill later resources."""

    import backend.api.tasks as tasks_api

    operation_id = uuid.uuid4().hex
    old_token = "c" * 32
    new_token = "d" * 32
    async with session_factory() as db:
        task = Task(
            title="receipt lease takeover before Plan stop",
            description="old token must stop after queue barrier",
            mode="plan",
            status="in_progress",
            retry_count=2,
            turn_generation=5,
        )
        db.add(task)
        await db.flush()
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
        # Worker receipt admission deliberately starts from a fresh
        # transaction. Persist the Worker-local Task before exercising that
        # boundary; an uncommitted fixture row is correctly discarded by the
        # admission rollback.
        await db.commit()
        receipt = await termination.stage_worker_receipt(
            db,
            task_id=task.id,
            operation_id=operation_id,
            operation="cancel",
            request_payload=payload,
            request_digest=termination.canonical_json_digest(payload),
        )
        receipt.status = "executing"
        receipt.state_version = 2
        receipt.execution_token = old_token
        receipt.next_reconcile_at = datetime.utcnow() + timedelta(seconds=90)
        expected_harness_owner = _test_harness_owner_identity(task)
        await db.commit()
        task_id = task.id

    async def abort_then_take_over(*_args, **_kwargs):
        async with session_factory() as takeover_db:
            changed = await takeover_db.execute(
                update(WorkerTaskTerminationReceipt)
                .where(
                    WorkerTaskTerminationReceipt.operation_id == operation_id,
                    WorkerTaskTerminationReceipt.status == "executing",
                    WorkerTaskTerminationReceipt.execution_token == old_token,
                    WorkerTaskTerminationReceipt.state_version == 2,
                )
                .values(
                    execution_token=new_token,
                    state_version=3,
                    next_reconcile_at=datetime.utcnow()
                    + timedelta(seconds=90),
                )
            )
            assert changed.rowcount == 1
            await takeover_db.commit()
        return 0

    stop_plan = AsyncMock(return_value=True)
    with (
        patch.object(
            main_module.dispatcher,
            "abort_task_queue",
            side_effect=abort_then_take_over,
        ),
        patch.object(
            main_module.dispatcher,
            "stop_plan_agent_lifecycle",
            stop_plan,
        ),
    ):
        async with session_factory() as db:
            with pytest.raises(HTTPException) as exc_info:
                await tasks_api._cancel_local_task_impl(
                    task_id,
                    db,
                    expected_identity=expected_harness_owner,
                    worker_termination_operation_id=operation_id,
                    worker_termination_execution_token=old_token,
                    worker_termination_state_version=2,
                )

    assert exc_info.value.status_code == 409
    stop_plan.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        receipt = await db.get(WorkerTaskTerminationReceipt, operation_id)
    assert task is not None and task.status == "in_progress"
    assert receipt is not None
    assert receipt.execution_token == new_token
    assert receipt.state_version == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "local_core_name"),
    (
        ("cancel", "_cancel_local_task_impl"),
        ("stop-session", "_stop_task_session_local_impl"),
    ),
)
async def test_public_local_terminal_request_yields_to_receipt_after_precheck(
    client,
    session_factory,
    endpoint,
    local_core_name,
):
    """A receipt winning the operation lock must own the long local core."""

    import backend.api.tasks as tasks_api

    task = await _local_task(session_factory)
    routing_precheck_done = asyncio.Event()

    async def local_precheck(_db, checked_task_id):
        assert checked_task_id == task.id
        routing_precheck_done.set()
        return None

    local_core = AsyncMock()
    operation_lock = worker_proxy_module.get_task_operation_lock(task.id)
    await operation_lock.acquire()
    try:
        with (
            patch.object(
                tasks_api,
                "_worker_task_or_none",
                side_effect=local_precheck,
            ),
            patch.object(tasks_api, local_core_name, local_core),
        ):
            request_task = asyncio.create_task(
                client.post(f"/api/tasks/{task.id}/{endpoint}")
            )
            await asyncio.wait_for(routing_precheck_done.wait(), timeout=3)
            await _persist_receipt(session_factory, task.id)
            operation_lock.release()
            response = await request_task
    finally:
        if operation_lock.locked():
            operation_lock.release()

    assert response.status_code == 409, response.text
    assert "termination receipt" in response.json()["detail"]
    local_core.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current is not None
    assert current.status == "completed"


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ("cancel", "stop-session"))
@pytest.mark.parametrize(
    "metadata",
    (
        pytest.param(
            {WORKER_MANAGED_TASK_METADATA_KEY: True},
            id="explicit-worker-marker",
        ),
        pytest.param(
            {USER_SKILL_SNAPSHOTS_METADATA_KEY: []},
            id="legacy-worker-marker",
        ),
    ),
)
async def test_worker_managed_public_terminal_request_requires_receipt(
    client,
    session_factory,
    monkeypatch,
    endpoint,
    metadata,
):
    """A stale Manager cannot bypass GET/PUT/ACK via the public endpoint."""

    from backend.config import settings

    task = await _local_task(
        session_factory,
        status="executing",
        metadata_=metadata,
    )
    monkeypatch.setattr(settings, "auth_token", "worker-service-token")

    with (
        patch.object(
            main_module.dispatcher,
            "abort_task_queue",
            new_callable=AsyncMock,
        ) as abort_queue,
        patch.object(
            main_module.instance_manager,
            "wait_for_task_launch_barrier",
            new_callable=AsyncMock,
        ) as launch_barrier,
        patch.object(
            main_module.instance_manager,
            "stop",
            new_callable=AsyncMock,
        ) as stop,
    ):
        response = await client.post(
            f"/api/tasks/{task.id}/{endpoint}",
            headers={"Authorization": "Bearer worker-service-token"},
        )

    assert response.status_code == 409, response.text
    assert "durable Worker receipt" in response.json()["detail"]
    abort_queue.assert_not_awaited()
    launch_barrier.assert_not_awaited()
    stop.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        receipt = await termination.active_worker_task_termination_receipt(
            db,
            task.id,
        )
    assert current is not None
    assert current.status == "executing"
    assert receipt is None


@pytest.mark.asyncio
async def test_legacy_terminate_generation_is_disabled_without_local_effect(
    client,
    session_factory,
):
    task = await _local_task(
        session_factory,
        status="in_progress",
        tags=["pr-review"],
    )
    await _persist_receipt(session_factory, task.id)

    terminate = AsyncMock()
    with patch(
        "backend.services.task_termination.terminate_local_task_generation",
        terminate,
    ):
        response = await client.post(
            f"/api/tasks/{task.id}/terminate-generation",
            json={
                "expected_status": "in_progress",
                "expected_retry_count": task.retry_count,
                "expected_turn_generation": task.turn_generation,
                "expected_instance_id": None,
                "expected_started_at": None,
                "expected_completed_at": None,
                "expected_pty_background_generation": None,
            },
        )

    assert response.status_code == 409, response.text
    assert (
        "Legacy termination mutation is disabled"
        in response.json()["detail"]
    )
    terminate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("skill", "path", "body", "start_method"),
    (
        (
            "monitor",
            "monitor-sessions",
            {"description": "must not start"},
            "start_monitor_session",
        ),
        (
            "sub-agent",
            "sub-agent-sessions",
            {"name": "must not start", "prompt": "do not run"},
            "start_sub_agent_session",
        ),
    ),
)
async def test_active_receipt_blocks_local_auxiliary_admission(
    client,
    session_factory,
    skill,
    path,
    body,
    start_method,
):
    task = await _local_task(
        session_factory,
        status="in_progress",
        provider="claude",
        enabled_skills={skill: True},
    )
    await _persist_receipt(session_factory, task.id)
    dispatcher = MagicMock()
    setattr(dispatcher, start_method, MagicMock())
    dispatcher.broadcaster.broadcast = AsyncMock()

    with patch("backend.main.dispatcher", dispatcher):
        response = await client.post(
            f"/api/tasks/{task.id}/{path}",
            json=body,
        )

    assert response.status_code == 409, response.text
    assert "termination receipt" in response.json()["detail"]
    getattr(dispatcher, start_method).assert_not_called()
    async with session_factory() as db:
        session_count = await db.scalar(
            select(func.count(SubAgentSession.id)).where(
                SubAgentSession.task_id == task.id
            )
        )
    assert session_count == 0


@pytest.mark.asyncio
async def test_active_receipt_blocks_worker_local_routing_mutation(
    client,
    session_factory,
):
    task = await _local_task(
        session_factory,
        status="completed",
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
    )
    await _persist_receipt(session_factory, task.id)

    response = await client.post(
        f"/api/tasks/{task.id}/routing-config/stage",
        json={
            "op_id": "must-yield-to-termination",
            "provider": "codex",
            "model": "gpt-5.6-sol",
            "codex_service_tier": "priority",
        },
    )

    assert response.status_code == 409, response.text
    assert "termination receipt" in response.text
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current is not None
    assert current.codex_service_tier == "default"
    assert "ccm_worker_routing_pending" not in (current.metadata_ or {})


@pytest.mark.asyncio
async def test_active_receipt_blocks_first_class_plan_admission(
    client,
    session_factory,
):
    task = await _local_task(
        session_factory,
        status="completed",
        session_id="receipt-fenced-first-class-plan",
    )
    await _persist_receipt(session_factory, task.id)

    response = await client.post(
        "/api/plans",
        json={
            "input": "must not start a Plan run",
            "target_task_id": task.id,
        },
    )

    assert response.status_code == 409, response.text
    assert "termination receipt" in response.text
    async with session_factory() as db:
        assert await db.scalar(select(func.count(Plan.id))) == 0
        assert await db.scalar(select(func.count(PlanAgentRun.id))) == 0


@pytest.mark.asyncio
async def test_active_receipt_blocks_related_plan_task_admission(
    client,
    session_factory,
):
    task = await _local_task(
        session_factory,
        status="completed",
        session_id="receipt-fenced-plan-target",
    )
    await _persist_receipt(session_factory, task.id)

    response = await client.post(
        f"/api/tasks/{task.id}/plans",
        json={"input": "must not create a related Plan Task"},
    )

    assert response.status_code == 409, response.text
    assert "termination receipt" in response.text
    async with session_factory() as db:
        count = await db.scalar(
            select(func.count(Task.id)).where(
                Task.plan_target_task_id == task.id,
                Task.mode == "plan",
            )
        )
    assert count == 0


@pytest.mark.asyncio
async def test_active_receipt_blocks_plan_execution_task_admission(
    client,
    session_factory,
):
    plan = await _local_task(
        session_factory,
        mode="plan",
        status="plan_review",
        plan_approved=True,
        plan_content="Implement only after termination clears.",
    )
    await _persist_receipt(session_factory, plan.id)

    response = await client.post(
        f"/api/tasks/{plan.id}/plan/create-execution-task",
    )

    assert response.status_code == 409, response.text
    assert "termination receipt" in response.text
    async with session_factory() as db:
        current = await db.get(Task, plan.id)
        executions = await db.scalar(
            select(func.count(Task.id)).where(
                Task.id != plan.id,
                Task.mode == "auto",
            )
        )
    assert current.plan_execution_task_id is None
    assert executions == 0


@pytest.mark.asyncio
async def test_active_receipt_blocks_plan_terminal_and_supersede_writers(
    session_factory,
):
    from backend.services.plan_tasks import (
        PlanTerminalQuiescenceError,
        mark_plan_superseded,
        run_plan_terminal_transition,
    )

    task = await _local_task(
        session_factory,
        mode="plan",
        status="plan_review",
        plan_content="receipt-owned plan",
    )
    await _persist_receipt(session_factory, task.id)
    dispatcher = MagicMock()

    @asynccontextmanager
    async def cancellation_lease(leased_task_id):
        assert leased_task_id == task.id
        yield

    dispatcher.task_queue_cancellation_lease = cancellation_lease
    dispatcher.abort_task_queue = AsyncMock(return_value=0)
    mutate = AsyncMock()

    with patch("backend.main.dispatcher", dispatcher):
        async with session_factory() as db:
            with pytest.raises(
                PlanTerminalQuiescenceError,
                match="active Worker termination receipt",
            ):
                await run_plan_terminal_transition(
                    db,
                    task.id,
                    "completed",
                    mutate,
                )
            source = await db.get(Task, task.id)
            superseded = await mark_plan_superseded(
                db,
                source,
                successor_id=task.id + 1,
            )
            await db.rollback()

    mutate.assert_not_awaited()
    assert superseded is False
    async with session_factory() as db:
        current = await db.get(Task, task.id)
    assert current is not None
    assert current.status == "plan_review"


@pytest.mark.asyncio
async def test_active_receipt_blocks_direct_chat_before_user_log(
    client,
    session_factory,
):
    task = await _local_task(
        session_factory,
        session_id="receipt-chat-session",
    )
    await _persist_receipt(session_factory, task.id)

    response = await client.post(
        f"/api/tasks/{task.id}/chat",
        json={"message": "must not enqueue"},
    )

    assert response.status_code == 409
    assert "termination receipt" in response.json()["detail"]
    async with session_factory() as db:
        rows = list(
            (
                await db.execute(
                    LogEntry.__table__.select().where(
                        LogEntry.task_id == task.id,
                        LogEntry.event_type == "user_message",
                    )
                )
            ).all()
        )
    assert rows == []


@pytest.mark.asyncio
async def test_active_receipt_blocks_live_injection_before_transport(
    client,
    session_factory,
):
    task = await _local_task(
        session_factory,
        status="executing",
        session_id="receipt-inject-session",
        provider="claude",
        execution_user_id=None,
        execution_user_role="super_admin",
        execution_mode="unrestricted",
        execution_principal_kind="deployment_token",
    )
    await _persist_receipt(session_factory, task.id)
    instance_manager = MagicMock()
    instance_manager.pty_mode_enabled = True
    instance_manager.has_pty_session.return_value = True
    instance_manager.inject_pty_message = AsyncMock(return_value=True)

    with patch("backend.main.instance_manager", instance_manager):
        response = await client.post(
            f"/api/tasks/{task.id}/inject",
            json={"message": "must not steer"},
        )

    assert response.status_code == 409
    assert "termination receipt" in response.json()["detail"]
    instance_manager.inject_pty_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_migration_final_claim_rejects_receipt_admitted_after_precheck(
    db_factory,
    session_factory,
):
    task = await _local_task(session_factory)
    async with session_factory() as db:
        worker = Worker(
            name="migration target",
            status="ready",
            private_ip="10.0.0.41",
            auth_token="worker-token",
        )
        db.add(worker)
        await db.commit()
        await db.refresh(worker)
        worker_id = worker.id

    relay = MagicMock()
    relay.subscribe_task = AsyncMock()
    relay.unsubscribe_task = MagicMock()
    migrator = TaskMigrator(
        db_factory=db_factory,
        relay=relay,
        broadcaster=None,
    )

    async def admit_during_destination_preflight(_worker_id):
        await _persist_receipt(session_factory, task.id)
        async with session_factory() as db:
            return await db.get(Worker, worker_id)

    migrator._get_worker = AsyncMock(side_effect=admit_during_destination_preflight)
    migrator._sync_workspace = AsyncMock()
    migrator._move_session = AsyncMock()
    migrator._move_codex_session = AsyncMock()
    migrator._ensure_worker_task = AsyncMock()

    with pytest.raises(MigrationError, match="迁移认领前"):
        await migrator.migrate(task.id, worker_id)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        assert current is not None
        assert current.status == "completed"
        assert current.worker_id is None
    migrator._sync_workspace.assert_not_awaited()
    migrator._ensure_worker_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_destroy_rejects_active_manager_receipt(
    client,
    session_factory,
    monkeypatch,
):
    async with session_factory() as db:
        worker = Worker(
            name="receipt-owned worker",
            status="ready",
            private_ip="10.0.0.42",
            auth_token="worker-token",
        )
        db.add(worker)
        await db.flush()
        task = Task(
            title="remote receipt task",
            description="d",
            status="completed",
            worker_id=worker.id,
        )
        db.add(task)
        await db.flush()
        db.add(_active_receipt(task, manager_worker_id=worker.id))
        await db.commit()
        worker_id = worker.id

    provisioner = AsyncMock()
    monkeypatch.setattr(main_module, "worker_provisioner", provisioner)

    response = await client.post(f"/api/workers/{worker_id}/destroy")

    assert response.status_code == 409
    assert "termination receipt" in response.json()["detail"]
    async with session_factory() as db:
        current = await db.get(Worker, worker_id)
        assert current is not None
        assert current.status == "ready"
    provisioner.destroy_worker.assert_not_awaited()


@pytest.mark.asyncio
async def test_destroy_restart_reclaims_matching_manager_stop_receipt(
    client,
    session_factory,
    monkeypatch,
):
    import backend.api.workers as workers_api

    async with session_factory() as db:
        worker = Worker(
            name="interrupted receipt destroy",
            status="error",
            bootstrap_step="destroy",
            bootstrap_error="Manager restarted during destroy",
            private_ip="10.0.0.43",
            auth_token="worker-token",
        )
        db.add(worker)
        await db.flush()
        task = Task(
            title="terminal receipt awaiting destroy recovery",
            description="d",
            status="completed",
            worker_id=worker.id,
        )
        db.add(task)
        await db.flush()
        db.add(_active_receipt(task, manager_worker_id=worker.id))
        await db.commit()
        worker_id = worker.id

    provisioner = AsyncMock()
    coordinator = AsyncMock()
    scheduled = []

    def capture_spawn(coro):
        scheduled.append(coro)
        return MagicMock()

    monkeypatch.setattr(main_module, "worker_provisioner", provisioner)
    monkeypatch.setattr(workers_api, "_migrate_back_then_destroy", coordinator)
    monkeypatch.setattr(workers_api, "_spawn", capture_spawn)

    response = await client.post(f"/api/workers/{worker_id}/destroy")
    assert response.status_code == 200, response.text
    assert len(scheduled) == 1
    await scheduled[0]

    coordinator.assert_awaited_once()
    _prov, claimed_worker_id, destroy_claim = coordinator.await_args.args
    assert _prov is provisioner
    assert claimed_worker_id == worker_id
    assert destroy_claim.worker_id == worker_id
    async with session_factory() as db:
        current = await db.get(Worker, worker_id)
        assert current is not None
        assert current.status == "destroying"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "receipt_kind",
    (
        "manager_cancel",
        "manager_conflict",
        "worker_stop",
        "other_worker_stop",
    ),
)
async def test_destroy_restart_rejects_nonmatching_active_receipt(
    client,
    session_factory,
    monkeypatch,
    receipt_kind,
):
    import backend.api.workers as workers_api

    async with session_factory() as db:
        worker = Worker(
            name=f"blocked destroy recovery {receipt_kind}",
            status="error",
            bootstrap_step="destroy",
            private_ip="10.0.0.44",
            auth_token="worker-token",
        )
        db.add(worker)
        await db.flush()
        other_worker_id = None
        if receipt_kind == "other_worker_stop":
            other = Worker(name="unrelated receipt worker", status="ready")
            db.add(other)
            await db.flush()
            other_worker_id = other.id
        task = Task(
            title="nonmatching recovery receipt",
            description="d",
            status="completed",
            worker_id=worker.id,
        )
        db.add(task)
        await db.flush()
        receipt = _active_receipt(
            task,
            manager_worker_id=(
                None
                if receipt_kind == "worker_stop"
                else other_worker_id or worker.id
            ),
        )
        if receipt_kind == "manager_cancel":
            receipt.operation = "cancel"
        elif receipt_kind == "manager_conflict":
            receipt.status = "conflict"
        db.add(receipt)
        await db.commit()
        worker_id = worker.id

    provisioner = AsyncMock()
    coordinator = AsyncMock()
    monkeypatch.setattr(main_module, "worker_provisioner", provisioner)
    monkeypatch.setattr(workers_api, "_migrate_back_then_destroy", coordinator)

    response = await client.post(f"/api/workers/{worker_id}/destroy")

    assert response.status_code == 409
    assert "active Task termination receipt" in response.json()["detail"]
    coordinator.assert_not_called()
    provisioner.destroy_worker.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Worker, worker_id)
        assert current is not None
        assert current.status == "error"
        assert current.bootstrap_step == "destroy"


@pytest.mark.asyncio
async def test_destroy_coordinator_resumes_terminal_matching_stop_receipt(
    session_factory,
    monkeypatch,
):
    import backend.api.tasks as tasks_api
    import backend.api.workers as workers_api
    from backend.services.worker_drain_proof import (
        worker_node_drain_proof_signature,
    )
    from backend.services.worker_provisioner import (
        worker_create_client_token_digest,
    )
    from backend.services.worker_proxy import (
        capture_worker_destroy_lifecycle_claim,
        worker_destroy_provision_spec_digest,
    )

    cloud_scope = {
        "provider": "aws",
        "partition": "aws",
        "account_id": "123456789012",
        "region": "us-east-1",
    }

    async with session_factory() as db:
        worker = Worker(
            name="claimed receipt destroy",
            status="destroying",
            cloud_instance_id="i-0123456789abcdef0",
            private_ip="10.0.0.45",
            auth_token="worker-token",
            destroy_lifecycle_nonce="d" * 32,
        )
        db.add(worker)
        await db.flush()
        client_token_digest = worker_create_client_token_digest(
            worker.id,
            worker.auth_token,
        )
        worker.provision_spec = {
            "version": 1,
            "name": worker.name,
            "has_fixed_overrides": False,
            "overrides": {},
            "cloud_scope": cloud_scope,
            "client_token_digest": client_token_digest,
        }
        provision_spec_digest = worker_destroy_provision_spec_digest(
            worker.provision_spec
        )
        task = Task(
            title="terminal receipt must be resumed",
            description="d",
            status="completed",
            worker_id=worker.id,
        )
        db.add(task)
        await db.flush()
        receipt = _active_receipt(task, manager_worker_id=worker.id)
        db.add(receipt)
        await db.commit()
        await db.refresh(worker)
        worker_id = worker.id
        task_id = task.id
        operation_id = receipt.operation_id
        destroy_claim = capture_worker_destroy_lifecycle_claim(worker)

    async def settle_terminal_receipt(_task_id, _claim, _proxy, db):
        assert _task_id == task_id
        assert _claim == destroy_claim
        durable = await db.get(WorkerTaskTerminationReceipt, operation_id)
        assert durable is not None
        settled_at = datetime.utcnow()
        result_payload = {"ok": True, "recovered_by": "destroy-test"}
        durable.status = "settled"
        durable.active_task_id = None
        durable.state_version += 1
        durable.result_payload = result_payload
        durable.result_digest = termination.canonical_json_digest(result_payload)
        durable.accepted_at = settled_at
        durable.completed_at = settled_at
        durable.ack_intent_at = settled_at
        durable.acknowledged_at = settled_at
        durable.next_reconcile_at = None
        durable.updated_at = settled_at
        await db.commit()
        current = await db.get(Task, task_id)
        return {"ok": True}, current

    stop_for_destroy = AsyncMock(side_effect=settle_terminal_receipt)
    monkeypatch.setattr(
        tasks_api,
        "_stop_worker_task_for_destroy",
        stop_for_destroy,
    )
    migrator = AsyncMock()

    async def detach_after_receipt(migrating_task_id, target_worker_id):
        assert migrating_task_id == task_id
        assert target_worker_id is None
        async with session_factory() as db:
            current = await db.get(Task, task_id)
            assert current is not None
            current.worker_id = None
            await db.commit()

    migrator.migrate.side_effect = detach_after_receipt
    relay = AsyncMock()
    provisioner = AsyncMock()
    provisioner.require_worker_cloud_identity.return_value = {
        "cloud_scope": cloud_scope,
        "client_token_digest": client_token_digest,
        "provision_spec_digest": provision_spec_digest,
    }
    monkeypatch.setattr(main_module, "task_migrator", migrator)
    monkeypatch.setattr(main_module, "worker_relay", relay)

    async def begin_clean_drain(_self, claim):
        return {
            "protocol_version": 3,
            "node_role": "worker",
            "drain_claim": claim.node_drain_claim,
            "draining": True,
        }

    async def seal_clean_runtime(_self, claim):
        return {
            "protocol_version": 3,
            "node_role": "worker",
            "drain_claim": claim.node_drain_claim,
            "runtime_sealed": True,
            "safe_to_seal": True,
            "blockers": [],
            "blocker_count": 0,
            "task_count": 0,
        }

    async def complete_log_backfill(_self, _claim, _task_ids):
        return None

    async def clean_drain_proof(_self, claim):
        payload = {
            "protocol_version": 3,
            "nonce": "0" * 32,
            "node_role": "worker",
            "drain_claim": claim.node_drain_claim,
            "runtime_sealed": True,
            "safe_to_destroy": True,
            "blockers": [],
            "blocker_count": 0,
            "task_count": 0,
        }
        return {
            **payload,
            "signature": worker_node_drain_proof_signature(
                payload,
                auth_token=claim.auth_token,
            ),
        }

    monkeypatch.setattr(
        WorkerProxy,
        "begin_claimed_destroy_drain",
        begin_clean_drain,
    )
    monkeypatch.setattr(
        WorkerProxy,
        "seal_claimed_destroy_runtime",
        seal_clean_runtime,
    )
    monkeypatch.setattr(
        WorkerProxy,
        "require_claimed_destroy_log_backfill",
        complete_log_backfill,
    )
    monkeypatch.setattr(
        WorkerProxy,
        "require_claimed_destroy_drain_proof",
        clean_drain_proof,
    )

    await workers_api._migrate_back_then_destroy(
        provisioner,
        worker_id,
        destroy_claim,
        db_factory=session_factory,
    )

    stop_for_destroy.assert_awaited_once()
    migrator.migrate.assert_awaited_once_with(task_id, None)
    relay.stop_worker.assert_awaited_once_with(worker_id)
    provisioner.destroy_worker.assert_awaited_once_with(
        worker_id,
        destroy_claim=destroy_claim,
    )


def _dispatcher(db_factory) -> GlobalDispatcher:
    instance_manager = MagicMock()
    instance_manager.processes = {}
    instance_manager._tasks = {}
    instance_manager._consumer_records = {}
    instance_manager._process_groups = {}
    instance_manager._container_exec_processes = {}
    instance_manager.is_running.return_value = False
    instance_manager.active_pty_background_task_ids.return_value = set()
    instance_manager.launch = AsyncMock()
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    return GlobalDispatcher(
        db_factory=db_factory,
        instance_manager=instance_manager,
        broadcaster=broadcaster,
    )


async def _seed_manager_worker_task(
    session_factory,
    *,
    status: str = "pending",
    mode: str = "auto",
) -> tuple[Task, Worker]:
    async with session_factory() as db:
        worker = Worker(
            name="termination admission worker",
            status="ready",
            private_ip="10.0.0.57",
            auth_token="worker-token",
        )
        db.add(worker)
        await db.flush()
        task = Task(
            title="termination admission task",
            description="durable termination must own initial dispatch",
            status=status,
            mode=mode,
            worker_id=worker.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        await db.refresh(worker)
        return task, worker


@pytest.mark.asyncio
async def test_generic_worker_proxy_blocks_non_receipt_request_while_active(
    session_factory,
):
    task, worker = await _seed_manager_worker_task(
        session_factory,
        status="in_progress",
    )
    await _persist_receipt(
        session_factory,
        task.id,
        manager_worker_id=worker.id,
    )
    proxy = WorkerProxy(session_factory, relay=AsyncMock())
    proxy.require_ready_worker = AsyncMock(return_value=worker)
    proxy._proxy_to_authorized_worker_locked = AsyncMock(return_value={"ok": True})

    with pytest.raises(HTTPException) as caught:
        await proxy.proxy_to_worker(
            task,
            "POST",
            f"/api/tasks/{task.id}/monitor-sessions",
            body={"description": "must not cross the receipt fence"},
        )

    assert caught.value.status_code == 409
    assert "termination receipt" in caught.value.detail
    proxy.require_ready_worker.assert_not_awaited()
    proxy._proxy_to_authorized_worker_locked.assert_not_awaited()


@pytest.mark.asyncio
async def test_generic_worker_proxy_allows_only_active_receipt_reconciliation(
    session_factory,
):
    task, worker = await _seed_manager_worker_task(
        session_factory,
        status="in_progress",
    )
    await _persist_receipt(
        session_factory,
        task.id,
        manager_worker_id=worker.id,
    )
    async with session_factory() as db:
        receipt = await termination.active_worker_task_termination_receipt(
            db,
            task.id,
        )
        operation_id = receipt.operation_id

    proxy = WorkerProxy(session_factory, relay=AsyncMock())
    proxy.require_ready_worker = AsyncMock(return_value=worker)
    proxy._proxy_to_authorized_worker_locked = AsyncMock(return_value={"ok": True})
    receipt_path = (
        f"/api/tasks/{task.id}/termination-receipts/{operation_id}"
    )

    assert await proxy.proxy_to_worker(task, "GET", receipt_path) == {"ok": True}
    assert await proxy.proxy_to_worker(
        task,
        "PUT",
        receipt_path,
        body={"request_digest": "d" * 64},
    ) == {"ok": True}
    assert await proxy.proxy_to_worker(
        task,
        "POST",
        f"{receipt_path}/ack",
        body={"result_digest": "e" * 64},
    ) == {"ok": True}

    assert proxy.require_ready_worker.await_count == 3
    assert proxy._proxy_to_authorized_worker_locked.await_count == 3

    with pytest.raises(HTTPException) as caught:
        await proxy.proxy_to_worker(
            task,
            "GET",
            f"{receipt_path}0",
        )
    assert caught.value.status_code == 409
    assert proxy.require_ready_worker.await_count == 3


@pytest.mark.asyncio
async def test_active_manager_receipt_blocks_pending_worker_initial_dispatch(
    db_factory,
    session_factory,
    monkeypatch,
):
    """A durable pending_remote stop remains sole owner of pending Task G."""

    task, worker = await _seed_manager_worker_task(session_factory)
    await _persist_receipt(
        session_factory,
        task.id,
        manager_worker_id=worker.id,
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = _dispatcher(db_factory)

    await dispatcher._dispatch_worker_tasks()
    await asyncio.sleep(0)

    async with session_factory() as db:
        current = await db.get(Task, task.id)
        receipt = await termination.active_worker_task_termination_receipt(
            db, task.id
        )
    assert receipt is not None
    assert receipt.side == "manager"
    assert receipt.status == "pending_remote"
    assert current.status == "pending"
    assert current.turn_generation == task.turn_generation
    proxy.forward_task_to_worker.assert_not_awaited()
    dispatcher.broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_pending_worker_final_claim_loses_to_new_manager_receipt(
    db_factory,
    session_factory,
    monkeypatch,
):
    """The final pending -> G+1 CAS repeats the durable receipt fence."""

    task, worker = await _seed_manager_worker_task(session_factory)
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    original_get_lock = worker_proxy_module.get_task_operation_lock
    receipt_staged = False

    def receipt_winning_lock(task_id: int):
        @asynccontextmanager
        async def lock():
            nonlocal receipt_staged
            if not receipt_staged:
                receipt_staged = True
                await _persist_receipt(
                    session_factory,
                    task.id,
                    manager_worker_id=worker.id,
                )
            async with original_get_lock(task_id):
                yield

        return lock()

    monkeypatch.setattr(
        worker_proxy_module,
        "get_task_operation_lock",
        receipt_winning_lock,
    )
    dispatcher = _dispatcher(db_factory)

    await dispatcher._dispatch_worker_tasks()
    await asyncio.sleep(0)

    assert receipt_staged is True
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        receipt = await termination.active_worker_task_termination_receipt(
            db, task.id
        )
    assert receipt is not None
    assert current.status == "pending"
    assert current.turn_generation == task.turn_generation
    proxy.forward_task_to_worker.assert_not_awaited()
    dispatcher.broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_pending_worker_target_repo_fill_yields_to_new_manager_receipt(
    session_factory,
    monkeypatch,
):
    """Project-path preparation cannot write after termination owns Task G."""

    task, worker = await _seed_manager_worker_task(session_factory)
    async with session_factory() as db:
        project = Project(
            name="receipt-fenced-worker-project",
            local_path="/workspace/receipt-fenced",
            status="ready",
        )
        db.add(project)
        await db.flush()
        current = await db.get(Task, task.id)
        assert current is not None
        current.project_id = project.id
        current.target_repo = None
        await db.commit()

    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    original_execute = AsyncSession.execute
    receipt_injected = False

    async def execute_with_receipt_winner(
        session,
        statement,
        *args,
        **kwargs,
    ):
        nonlocal receipt_injected
        table = getattr(statement, "table", None)
        value_keys = {
            getattr(column, "key", None)
            for column in (getattr(statement, "_values", None) or {})
        }
        if (
            not receipt_injected
            and getattr(table, "name", None) == "tasks"
            and "target_repo" in value_keys
        ):
            receipt_injected = True
            # End the dispatcher's Project read snapshot before the independent
            # receipt writer commits, then execute the already-built exact
            # target_repo CAS against that durable owner.
            await session.rollback()
            await _persist_receipt(
                session_factory,
                task.id,
                manager_worker_id=worker.id,
            )
        return await original_execute(session, statement, *args, **kwargs)

    dispatcher = _dispatcher(session_factory)

    with patch.object(
        AsyncSession,
        "execute",
        new=execute_with_receipt_winner,
    ):
        await dispatcher._dispatch_worker_tasks()
    await asyncio.sleep(0)

    assert receipt_injected is True
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        receipt = await termination.active_worker_task_termination_receipt(
            db,
            task.id,
        )
    assert receipt is not None
    assert current.status == "pending"
    assert current.target_repo is None
    proxy.forward_task_to_worker.assert_not_awaited()
    dispatcher.broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_plan_failure_cas_loses_to_new_manager_receipt(
    db_factory,
    session_factory,
    monkeypatch,
):
    """Legacy Plan rejection cannot terminalize a receipt-owned generation."""

    task, worker = await _seed_manager_worker_task(
        session_factory,
        mode="plan",
    )
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = _dispatcher(db_factory)
    original_execute = AsyncSession.execute
    receipt_injected = False

    async def execute_with_receipt_winner(
        session,
        statement,
        *args,
        **kwargs,
    ):
        nonlocal receipt_injected
        table = getattr(statement, "table", None)
        if (
            not receipt_injected
            and getattr(table, "name", None) == "tasks"
        ):
            receipt_injected = True
            await session.rollback()
            await _persist_receipt(
                session_factory,
                task.id,
                manager_worker_id=worker.id,
            )
        return await original_execute(session, statement, *args, **kwargs)

    with patch.object(
        AsyncSession,
        "execute",
        new=execute_with_receipt_winner,
    ):
        await dispatcher._dispatch_worker_tasks()
    await asyncio.sleep(0)

    assert receipt_injected is True
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        receipt = await termination.active_worker_task_termination_receipt(
            db, task.id
        )
    assert receipt is not None
    assert current.status == "pending"
    assert current.completed_at is None
    assert current.error_message is None
    proxy.forward_task_to_worker.assert_not_awaited()
    dispatcher.broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_initial_worker_forward_has_final_active_receipt_effect_gate(
    db_factory,
    session_factory,
):
    """No project/config/POST effect starts after termination owns Task G."""

    task, worker = await _seed_manager_worker_task(
        session_factory,
        status="in_progress",
    )
    await _persist_receipt(
        session_factory,
        task.id,
        manager_worker_id=worker.id,
    )
    proxy = WorkerProxy(db_factory, relay=AsyncMock())
    proxy._forward_task_to_worker_locked = AsyncMock()

    with pytest.raises(WorkerTaskForwardAdmissionBlockedError) as blocked:
        await proxy.forward_task_to_worker(task)

    assert "termination" in str(blocked.value).lower()
    proxy._forward_task_to_worker_locked.assert_not_awaited()


@pytest.mark.asyncio
async def test_initial_forward_retry_yields_when_manager_receipt_wins(
    db_factory,
    session_factory,
    monkeypatch,
):
    """A pre-POST retry never crosses termination admitted during backoff."""

    task, worker = await _seed_manager_worker_task(
        session_factory,
        status="in_progress",
    )
    proxy = WorkerProxy(db_factory, relay=AsyncMock())
    proxy._forward_task_to_worker_locked = AsyncMock(
        side_effect=RuntimeError("pre-POST preflight failed")
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    receipt_staged = False

    async def stage_receipt_during_backoff(_delay: float) -> None:
        nonlocal receipt_staged
        if receipt_staged:
            return
        receipt_staged = True
        await _persist_receipt(
            session_factory,
            task.id,
            manager_worker_id=worker.id,
        )

    monkeypatch.setattr(
        dispatcher_module.asyncio,
        "sleep",
        stage_receipt_during_backoff,
    )
    dispatcher = _dispatcher(db_factory)
    claimed_generation = dispatcher._task_status_generation(task)

    await dispatcher._safe_forward_to_worker(task, claimed_generation)

    assert receipt_staged is True
    assert proxy._forward_task_to_worker_locked.await_count == 1
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        receipt = await termination.active_worker_task_termination_receipt(
            db, task.id
        )
    assert receipt is not None
    assert current.status == "in_progress"
    assert current.turn_generation == task.turn_generation
    assert current.completed_at is None
    dispatcher.broadcaster.broadcast.assert_not_awaited()


@pytest.mark.asyncio
async def test_initial_forward_failed_writer_loses_to_new_manager_receipt(
    db_factory,
    session_factory,
    monkeypatch,
):
    """The last preflight error cannot fail a newly receipt-owned Task."""

    task, worker = await _seed_manager_worker_task(
        session_factory,
        status="in_progress",
    )
    attempts = 0

    async def fail_and_admit_receipt(_task: Task) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 3:
            await _persist_receipt(
                session_factory,
                task.id,
                manager_worker_id=worker.id,
            )
        raise RuntimeError("pre-POST preflight failed")

    proxy = AsyncMock()
    proxy.forward_task_to_worker.side_effect = fail_and_admit_receipt
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    monkeypatch.setattr(
        dispatcher_module.asyncio,
        "sleep",
        AsyncMock(),
    )
    dispatcher = _dispatcher(db_factory)
    claimed_generation = dispatcher._task_status_generation(task)

    await dispatcher._safe_forward_to_worker(task, claimed_generation)

    assert proxy.forward_task_to_worker.await_count == 3
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        receipt = await termination.active_worker_task_termination_receipt(
            db, task.id
        )
    assert receipt is not None
    assert current.status == "in_progress"
    assert current.completed_at is None
    assert current.error_message is None
    dispatcher.broadcaster.broadcast.assert_not_awaited()


async def _seed_exact_admitted_turn(session_factory) -> tuple[int, int, int]:
    """Persist the exact post-admission Task/source shape used by recovery."""

    async with session_factory() as db:
        instance = Instance(name="receipt recovery slot", status="idle")
        db.add(instance)
        await db.flush()
        task = Task(
            title="receipt recovery generation",
            description="d",
            status="executing",
            session_id="receipt-recovery-session",
            execution_user_id=None,
            execution_user_role="member",
            execution_mode="sandbox",
            execution_principal_kind="system",
            retry_count=0,
            turn_generation=1,
            instance_id=instance.id,
            started_at=datetime(2026, 8, 7, 1, 2, 3),
        )
        db.add(task)
        await db.flush()
        source = LogEntry(
            instance_id=instance.id,
            task_id=task.id,
            task_retry_count=task.retry_count,
            task_turn_generation=task.turn_generation,
            turn_scope="source",
            event_type="user_message",
            role="user",
            content="exact queued request",
            is_error=False,
            raw_json=(
                '{"execution_principal":{"user_id":null,'
                '"role":"member","mode":"sandbox","kind":"system"}}'
            ),
        )
        db.add(source)
        await db.flush()
        task.turn_source_log_id = source.id
        await db.commit()
        return task.id, instance.id, source.id


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["complete", "fail"])
async def test_active_receipt_blocks_dispatcher_terminal_finalizers(
    db_factory,
    session_factory,
    terminal,
):
    """Receipt recovery, not a stale lifecycle, owns the terminal Task CAS."""

    task_id, instance_id, _source_id = await _seed_exact_admitted_turn(
        session_factory
    )
    dispatcher = _dispatcher(db_factory)
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        generation = dispatcher._task_lifecycle_generation(task)
    await _persist_receipt(session_factory, task_id)

    if terminal == "complete":
        changed = await dispatcher._complete_owned_task(
            generation,
            count_completion=True,
        )
    else:
        changed = await dispatcher._fail_owned_task(
            generation,
            "stale dispatcher failure",
        )

    assert changed is False
    dispatcher.broadcaster.broadcast.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        assert task.status == "executing"
        assert task.retry_count == 0
        assert task.completed_at is None
        assert task.error_message is None
        assert instance.total_tasks_completed == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("actual_transport", [None, "claude_exec"])
async def test_active_receipt_blocks_source_aware_lifecycle_finalizer(
    db_factory,
    session_factory,
    actual_transport,
):
    """The source-aware retry/failure path also yields to receipt ownership."""

    task_id, instance_id, source_id = await _seed_exact_admitted_turn(
        session_factory
    )
    dispatcher = _dispatcher(db_factory)
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        task.max_retries = 3
        source = await db.get(LogEntry, source_id)
        source.actual_transport = actual_transport
        await db.commit()
        generation = dispatcher._task_lifecycle_generation(task)
    await _persist_receipt(session_factory, task_id)

    finalized = await dispatcher._finalize_fresh_lifecycle_replay_safely(
        generation,
        pending_reason="stale dispatcher retry",
        failure_reason="stale dispatcher failure",
        consume_retry=True,
    )

    assert finalized is None
    dispatcher.broadcaster.broadcast.assert_not_awaited()
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        source = await db.get(LogEntry, source_id)
        assert task.status == "executing"
        assert task.retry_count == 0
        assert task.instance_id == instance_id
        assert task.completed_at is None
        assert task.error_message is None
        assert source.actual_transport == actual_transport
        assert instance.total_tasks_completed == 0


@pytest.mark.asyncio
async def test_active_receipt_blocks_mode_claim_and_stale_reset(
    db_factory,
    session_factory,
):
    """A dead local process cannot erase receipt-owned Task/Instance evidence."""

    task_id, instance_id, _source_id = await _seed_exact_admitted_turn(
        session_factory
    )
    dispatcher = _dispatcher(db_factory)
    started_at = datetime(2026, 8, 7, 1, 3, 4)
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        instance.status = "running"
        instance.pid = 930_041
        instance.started_at = started_at
        instance.current_task_id = task_id
        await db.commit()
        generation = dispatcher._task_lifecycle_generation(task)
    await _persist_receipt(session_factory, task_id)
    dispatcher.instance_manager._instance_lifecycle_lock.return_value = asyncio.Lock()

    assert await dispatcher._ensure_owned_executing(generation) is False
    await dispatcher._reset_instance_if_stale(instance_id, generation)

    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        receipt = await termination.active_worker_task_termination_receipt(
            db, task_id
        )
        assert receipt is not None
        assert task.status == "executing"
        assert task.instance_id == instance_id
        assert task.completed_at is None
        assert task.error_message is None
        assert instance.status == "running"
        assert instance.pid == 930_041
        assert instance.started_at == started_at
        assert instance.current_task_id == task_id


@pytest.mark.asyncio
async def test_receipt_winning_after_precheck_blocks_final_provider_boundary(
    db_factory,
    session_factory,
):
    """Durable stop ownership defeats the last Task fence before provider I/O."""

    from backend.services.instance_manager import (
        InstanceManager,
        LaunchSupersededError,
        _LaunchReservation,
    )

    task_id, instance_id, source_id = await _seed_exact_admitted_turn(
        session_factory
    )
    async with session_factory() as db:
        task = await db.get(Task, task_id)
        retry_count = task.retry_count
        turn_generation = task.turn_generation

    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    manager = InstanceManager(db_factory, broadcaster)
    manager._launch_reservations[instance_id] = _LaunchReservation(
        object(),
        task_id,
        turn_generation,
        None,
    )

    # Model the check -> provider-boundary race: launch reservation/precheck
    # already succeeded, then the Worker commits accepted stop ownership.
    await _persist_receipt(session_factory, task_id)

    with pytest.raises(
        LaunchSupersededError,
        match="lost its exact launch generation",
    ):
        await manager._persist_actual_turn_transport(
            instance_id=instance_id,
            task_id=task_id,
            task_retry_count=retry_count,
            task_turn_generation=turn_generation,
            source_log_id=source_id,
            actual_transport="claude_exec",
        )

    async with session_factory() as db:
        task = await db.get(Task, task_id)
        source = await db.get(LogEntry, source_id)
        receipt = await termination.active_worker_task_termination_receipt(
            db, task_id
        )
        assert receipt is not None
        assert task.status == "executing"
        assert task.retry_count == retry_count
        assert task.turn_generation == turn_generation
        assert source.actual_transport is None


@pytest.mark.asyncio
async def test_cancelled_queued_launch_does_not_fail_receipt_owned_generation(
    db_factory,
    session_factory,
    monkeypatch,
):
    """A receipt-triggered queue cancellation leaves recovery as sole owner."""

    import backend.api.tasks as tasks_module

    monkeypatch.setattr(
        tasks_module,
        "_find_session_jsonl",
        lambda _session_id, provider="claude": "/tmp/exact-session.jsonl",
    )
    async with session_factory() as db:
        instance = Instance(name="cancelled receipt slot", status="idle")
        db.add(instance)
        task = Task(
            title="receipt cancels admitted queue worker",
            description="d",
            status="completed",
            session_id="receipt-cancel-session",
            target_repo="/repo",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    dispatcher = _dispatcher(db_factory)
    dispatcher._resolve_resume_config_dir = AsyncMock(return_value=None)

    async def cancel_after_receipt_admission(**_launch_kwargs):
        await _persist_receipt(session_factory, task_id)
        raise asyncio.CancelledError()

    dispatcher.instance_manager.launch = AsyncMock(
        side_effect=cancel_after_receipt_admission
    )
    message = QueuedMessage(
        priority=0,
        timestamp=4.0,
        prompt="receipt must own cancellation",
    )

    with pytest.raises(asyncio.CancelledError):
        await dispatcher._process_queued_message(task_id, message)

    async with session_factory() as db:
        current = await db.get(Task, task_id)
        active = await termination.active_worker_task_termination_receipt(
            db, task_id
        )
    assert current is not None
    assert current.status == "executing"
    assert current.turn_generation == 1
    assert current.instance_id is not None
    assert active is not None


@pytest.mark.asyncio
async def test_reconcile_final_update_loses_to_concurrent_receipt(
    db_factory,
    session_factory,
):
    """The recovery CAS itself, not an earlier read, arbitrates ownership."""

    task_id, instance_id, source_id = await _seed_exact_admitted_turn(
        session_factory
    )
    dispatcher = _dispatcher(db_factory)
    async with session_factory() as db:
        admitted_generation = await dispatcher._read_task_status_generation(
            db, task_id
        )
    assert admitted_generation is not None
    message = QueuedMessage(
        priority=0,
        timestamp=5.0,
        prompt="do not reconcile across receipt",
        source_log_id=source_id,
    )
    original_execute = AsyncSession.execute
    receipt_injected = False

    async def execute_with_receipt_winner(session, statement, *args, **kwargs):
        nonlocal receipt_injected
        table = getattr(statement, "table", None)
        value_names = {
            getattr(column, "key", None)
            for column in getattr(statement, "_values", {})
        }
        if (
            not receipt_injected
            and getattr(table, "name", None) == "tasks"
            and {"status", "instance_id", "completed_at"}.issubset(value_names)
        ):
            receipt_injected = True
            await session.rollback()
            await _persist_receipt(session_factory, task_id)
        return await original_execute(session, statement, *args, **kwargs)

    with patch.object(AsyncSession, "execute", new=execute_with_receipt_winner):
        reconciled, cancellation = (
            await dispatcher._reconcile_queued_admission_commit(
                msg=message,
                admitted_generation=admitted_generation,
                session_id="receipt-recovery-session",
                bound_source_id=source_id,
                request_source_log_id=source_id,
                status_before_launch="completed",
                instance_id_before_launch=None,
                completed_at_before_launch=None,
                provider="claude",
                model=None,
                service_tier=None,
            )
        )

    assert receipt_injected
    assert reconciled is False
    assert cancellation is None
    async with session_factory() as db:
        current = await db.get(Task, task_id)
    assert current is not None
    assert current.status == "executing"
    assert current.instance_id == instance_id
    assert current.turn_generation == 1


@pytest.mark.asyncio
async def test_provider_exception_final_update_loses_to_concurrent_receipt(
    db_factory,
    session_factory,
    monkeypatch,
):
    """A receipt admitted after proof still defeats generic launch repair."""

    import backend.api.tasks as tasks_module

    monkeypatch.setattr(
        tasks_module,
        "_find_session_jsonl",
        lambda _session_id, provider="claude": "/tmp/exact-session.jsonl",
    )
    async with session_factory() as db:
        instance = Instance(name="late provider receipt slot", status="idle")
        db.add(instance)
        task = Task(
            title="late provider repair receipt",
            description="d",
            status="completed",
            session_id="late-provider-receipt-session",
            target_repo="/repo",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    dispatcher = _dispatcher(db_factory)
    dispatcher._resolve_resume_config_dir = AsyncMock(return_value=None)
    launch_failed = False

    async def fail_before_provider_boundary(**_launch_kwargs):
        nonlocal launch_failed
        launch_failed = True
        raise RuntimeError("preflight adapter failure")

    dispatcher.instance_manager.launch = AsyncMock(
        side_effect=fail_before_provider_boundary
    )
    message = QueuedMessage(
        priority=0,
        timestamp=6.0,
        prompt="receipt wins after preflight proof",
    )
    original_execute = AsyncSession.execute
    receipt_injected = False

    async def execute_with_receipt_winner(session, statement, *args, **kwargs):
        nonlocal receipt_injected
        table = getattr(statement, "table", None)
        value_names = {
            getattr(column, "key", None)
            for column in getattr(statement, "_values", {})
        }
        if (
            launch_failed
            and not receipt_injected
            and getattr(table, "name", None) == "tasks"
            and {"status", "instance_id", "completed_at"}.issubset(value_names)
        ):
            receipt_injected = True
            await session.rollback()
            await _persist_receipt(session_factory, task_id)
        return await original_execute(session, statement, *args, **kwargs)

    with patch.object(AsyncSession, "execute", new=execute_with_receipt_winner):
        with pytest.raises(
            QueuedMessagePrelaunchError,
            match="yielded to an active Worker termination receipt",
        ):
            await dispatcher._process_queued_message(task_id, message)

    assert receipt_injected
    assert dispatcher.instance_manager.launch.await_count == 1
    async with session_factory() as db:
        current = await db.get(Task, task_id)
        active = await termination.active_worker_task_termination_receipt(
            db, task_id
        )
    assert current is not None
    assert current.status == "executing"
    assert current.turn_generation == 1
    assert current.instance_id is not None
    assert active is not None


@pytest.mark.asyncio
async def test_queued_launch_precheck_preserves_message_under_active_receipt(
    db_factory,
    session_factory,
):
    task = await _local_task(
        session_factory,
        session_id="queued-receipt-session",
    )
    await _persist_receipt(session_factory, task.id)
    dispatcher = _dispatcher(db_factory)
    message = QueuedMessage(
        priority=0,
        timestamp=1.0,
        prompt="do not launch",
    )

    with pytest.raises(QueuedMessagePrelaunchError, match="termination receipt"):
        await dispatcher._process_queued_message(task.id, message)

    dispatcher.instance_manager.launch.assert_not_awaited()


@pytest.mark.asyncio
async def test_queued_launch_final_sql_gate_loses_to_new_receipt(
    db_factory,
    session_factory,
    monkeypatch,
):
    """A receipt winning after precheck still blocks the provider launch."""

    import backend.api.tasks as tasks_module

    monkeypatch.setattr(
        tasks_module,
        "_find_session_jsonl",
        lambda _session_id, provider="claude": "/tmp/exact-session.jsonl",
    )
    async with session_factory() as db:
        instance = Instance(name="queued receipt slot", status="idle")
        db.add(instance)
        task = Task(
            title="queued receipt race",
            description="d",
            status="completed",
            session_id="queued-receipt-race-session",
            target_repo="/repo",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    dispatcher = _dispatcher(db_factory)
    dispatcher._resolve_resume_config_dir = AsyncMock(return_value=None)
    message = QueuedMessage(
        priority=0,
        timestamp=2.0,
        prompt="must lose final gate",
    )
    original_execute = AsyncSession.execute
    receipt_injected = False

    async def execute_with_receipt_winner(session, statement, *args, **kwargs):
        nonlocal receipt_injected
        table = getattr(statement, "table", None)
        if not receipt_injected and getattr(table, "name", None) == "tasks":
            receipt_injected = True
            # End the queue consumer's read snapshot, then let the independent
            # receipt transaction win immediately before the final Task UPDATE.
            await session.rollback()
            await _persist_receipt(session_factory, task_id)
        return await original_execute(session, statement, *args, **kwargs)

    with patch.object(AsyncSession, "execute", new=execute_with_receipt_winner):
        with pytest.raises(
            QueuedMessagePrelaunchError,
            match="ownership changed before launch",
        ):
            await dispatcher._process_queued_message(task_id, message)

    assert receipt_injected
    dispatcher.instance_manager.launch.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, task_id)
        assert current is not None
        assert current.status == "completed"
        assert current.turn_generation == 0


@pytest.mark.asyncio
async def test_provider_boundary_permit_yields_to_receipt_without_repairing_task(
    db_factory,
    session_factory,
    monkeypatch,
):
    """Receipt recovery remains sole owner after queued admission commits."""

    import backend.api.tasks as tasks_module

    monkeypatch.setattr(
        tasks_module,
        "_find_session_jsonl",
        lambda _session_id, provider="claude": "/tmp/exact-session.jsonl",
    )
    async with session_factory() as db:
        instance = Instance(name="provider receipt slot", status="idle")
        db.add(instance)
        task = Task(
            title="provider receipt race",
            description="d",
            status="completed",
            session_id="provider-receipt-race-session",
            target_repo="/repo",
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)
        task_id = task.id

    dispatcher = _dispatcher(db_factory)
    dispatcher._resolve_resume_config_dir = AsyncMock(return_value=None)
    provider_started = False

    async def receipt_wins_at_boundary(**launch_kwargs):
        nonlocal provider_started
        await _persist_receipt(session_factory, task_id)
        await launch_kwargs["on_launch_admitted"]()
        provider_started = True

    dispatcher.instance_manager.launch = AsyncMock(
        side_effect=receipt_wins_at_boundary
    )
    message = QueuedMessage(
        priority=0,
        timestamp=3.0,
        prompt="must yield at provider boundary",
    )

    with pytest.raises(
        QueuedMessagePrelaunchError,
        match="yielded to an active Worker termination receipt",
    ):
        await dispatcher._process_queued_message(task_id, message)

    assert provider_started is False
    async with session_factory() as db:
        current = await db.get(Task, task_id)
        assert current is not None
        # Generic queued-launch recovery must not rewrite receipt-owned state.
        assert current.status == "executing"
        assert current.turn_generation == 1
        assert current.instance_id is not None


@pytest.mark.asyncio
async def test_startup_cleanup_preserves_receipt_owned_task_and_instance(
    db_factory,
    session_factory,
):
    async with session_factory() as db:
        task = Task(
            title="restart receipt owner",
            description="d",
            status="executing",
            session_id="restart-receipt-session",
            pty_background_generation="receipt-pty-epoch",
        )
        db.add(task)
        await db.flush()
        instance = Instance(
            name="restart receipt instance",
            status="running",
            current_task_id=task.id,
            pid=None,
        )
        db.add(instance)
        await db.flush()
        task.instance_id = instance.id
        receipt = _active_receipt(task)
        db.add(receipt)
        sub_agent = SubAgentSession(
            task_id=task.id,
            description="receipt-owned native child",
            agent_type="native-agent",
            source="native",
            status="running",
        )
        db.add(sub_agent)
        await db.commit()
        task_id = task.id
        instance_id = instance.id
        sub_agent_id = sub_agent.id

    dispatcher = _dispatcher(db_factory)
    await dispatcher._cleanup_stale_state()

    async with session_factory() as db:
        task = await db.get(Task, task_id)
        instance = await db.get(Instance, instance_id)
        sub_agent = await db.get(SubAgentSession, sub_agent_id)
        assert task is not None
        assert task.status == "executing"
        assert task.instance_id == instance_id
        assert task.pty_background_generation == "receipt-pty-epoch"
        assert instance is not None
        assert instance.status == "running"
        assert instance.current_task_id == task_id
        assert sub_agent is not None
        assert sub_agent.status == "running"
