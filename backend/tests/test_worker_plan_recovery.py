"""Manager restart recovery for Worker-owned first-class Plan Runs."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi import HTTPException
from sqlalchemy import select

import backend.main as main_module
import backend.services.worker_plan_dispatch as worker_plan_dispatch_module
from backend.models.plan import Plan
from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentStep,
    PlanAgentWorkerDispatchReceipt,
    PlanAgentWorkerImportReceipt,
)
from backend.models.task import Task
from backend.models.worker import Worker
from backend.schemas.plan import default_plan_pipeline_config
from backend.services.dispatcher import GlobalDispatcher
from backend.services.worker_plan_dispatch import (
    WorkerPlanDispatchConflict,
    fence_worker_mirror_cancellation,
    mark_worker_dispatch_remote_possible,
)
from backend.services.worker_proxy import (
    WorkerPlanRemoteAbsent,
    WorkerPlanRemoteCancelled,
    WorkerPlanRemoteIdentityConflict,
    WorkerPlanReconciliationUnsupported,
    WorkerProxy,
)


pytestmark = pytest.mark.usefixtures("worker_control_plane_auth")

_WORKER_CONTROL_PLANE_TOKEN = "worker-control-plane-test-token"


@dataclass(frozen=True)
class _Graph:
    worker_id: int
    plan_id: int
    run_id: int
    generation: int
    receipt_id: int
    target_task_id: int | None
    created_at: str
    updated_at: str


async def _seed_graph(
    db_factory,
    *,
    receipt_status: str,
    generation: int = 4,
    with_target_task: bool = False,
) -> _Graph:
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with db_factory() as db:
        worker = Worker(
            name=f"worker-{receipt_status}",
            status="ready",
            auth_token=_WORKER_CONTROL_PLANE_TOKEN,
        )
        db.add(worker)
        await db.flush()
        target = None
        if with_target_task:
            target = Task(
                title="Worker Plan target",
                description="target",
                worker_id=worker.id,
                status="pending",
            )
            db.add(target)
            await db.flush()
        plan = Plan(
            title="Restart-safe remote Plan",
            initial_request="Plan on the Worker",
            worker_id=worker.id,
            target_task_id=target.id if target is not None else None,
            pipeline_config=pipeline,
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            worker_id=worker.id,
            run_type="initial",
            request_text="Plan on the Worker",
            pipeline_config=pipeline,
            status="running",
            current_stage="planner",
            generation=generation,
            last_execution_started_at=datetime.utcnow(),
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        receipt = PlanAgentWorkerDispatchReceipt(
            plan_id=plan.id,
            run_id=run.id,
            target_task_id=target.id if target is not None else None,
            worker_id=worker.id,
            run_generation=generation,
            protocol=1,
            status=receipt_status,
            payload_digest=("a" * 64 if receipt_status == "remote_possible" else None),
        )
        db.add(receipt)
        await db.commit()
        return _Graph(
            worker_id=worker.id,
            plan_id=plan.id,
            run_id=run.id,
            generation=generation,
            receipt_id=receipt.id,
            target_task_id=target.id if target is not None else None,
            created_at=run.created_at.isoformat(),
            updated_at=run.updated_at.isoformat(),
        )


def _dispatcher(db_factory) -> GlobalDispatcher:
    manager = MagicMock()
    manager.processes = {}
    manager._tasks = {}
    dispatcher = GlobalDispatcher(
        db_factory=db_factory,
        instance_manager=manager,
        broadcaster=MagicMock(broadcast=AsyncMock()),
    )
    dispatcher._running = True
    dispatcher._shutting_down = False
    dispatcher.wake = MagicMock()
    return dispatcher


def _failed_payload(graph: _Graph) -> dict:
    return {
        "protocol": 3,
        "base_worker_version_id": None,
        "run": {
            "id": graph.run_id,
            "plan_id": graph.plan_id,
            "run_type": "initial",
            "status": "failed",
            "current_stage": "failed",
            "base_version_id": None,
            "result_version_id": None,
            "request_text": "Plan on the Worker",
            "round": 1,
            "generation": 0,
            "instance_id": None,
            "worker_id": None,
            "open_input_request_id": None,
            "interaction_count": 0,
            "max_interactions": 3,
            "execution_seconds": 1.0,
            "last_execution_started_at": None,
            "review_verdict": None,
            "review_feedback": None,
            "review_exhausted": False,
            "error": "remote failure",
            "created_at": graph.created_at,
            "updated_at": graph.updated_at,
            "finished_at": graph.updated_at,
            "steps": [],
            "input_requests": [],
        },
        "versions": [],
    }


def _cancelled_payload(graph: _Graph) -> dict:
    payload = _failed_payload(graph)
    payload["run"] = {
        **payload["run"],
        "status": "cancelled",
        "current_stage": "planner",
        "error": "Cancelled by user",
        "steps": [
            {
                "id": 900_000 + graph.run_id,
                "run_id": graph.run_id,
                "plan_id": graph.plan_id,
                "plan_version_id": None,
                "input_request_id": None,
                "step_type": "planner",
                "round": 1,
                "generation": 0,
                "provider": "codex",
                "model": "gpt-test",
                "effort": "high",
                "route_slot": "primary",
                "status": "cancelled",
                "output": "durable cancelled recovery evidence",
                "error": "Cancelled by user",
                "last_delta_at": graph.updated_at,
                "streamed_output_chars": 35,
                "last_event_type": "turn.cancelled",
                "started_at": graph.created_at,
                "finished_at": graph.updated_at,
            }
        ],
    }
    return payload


@pytest.mark.asyncio
async def test_restart_requeues_only_prepared_worker_claim(
    db_factory,
    monkeypatch,
):
    graph = await _seed_graph(db_factory, receipt_status="prepared")
    proxy = AsyncMock()
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = _dispatcher(db_factory)

    assert await dispatcher._recover_versioned_plan_runs() is False

    proxy.reconcile_versioned_plan_until_pause.assert_not_awaited()
    async with db_factory() as db:
        run = await db.get(PlanAgentRun, graph.run_id)
        receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            graph.receipt_id,
        )
        assert run.status == "queued"
        assert run.generation == graph.generation + 1
        assert run.last_execution_started_at is None
        assert receipt.status == "settled"
        assert receipt.settlement_reason == "not_launched"
        assert receipt.payload_digest is None


@pytest.mark.asyncio
async def test_restart_replays_exact_cancellation_and_imports_terminal_winner(
    db_factory,
    monkeypatch,
):
    graph = await _seed_graph(
        db_factory,
        receipt_status="remote_possible",
        generation=0,
    )
    async with db_factory() as db:
        await fence_worker_mirror_cancellation(
            db,
            plan_id=graph.plan_id,
            run_id=graph.run_id,
            worker_id=graph.worker_id,
            generation=graph.generation,
            payload_digest="a" * 64,
        )

    failed = _failed_payload(graph)
    remote = {
        "protocol": 1,
        "state": "terminal",
        "plan_id": graph.plan_id,
        "run_id": graph.run_id,
        "payload_digest": "a" * 64,
        "base_worker_version_id": failed["base_worker_version_id"],
        "run": failed["run"],
        "versions": failed["versions"],
    }
    proxy = AsyncMock()
    proxy.cancel_versioned_plan_run.return_value = remote
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = _dispatcher(db_factory)

    assert await dispatcher._recover_versioned_plan_runs() is False
    await dispatcher._running_tasks[f"worker-plan-{graph.run_id}"]

    proxy.cancel_versioned_plan_run.assert_awaited_once_with(
        graph.worker_id,
        graph.run_id,
        plan_id=graph.plan_id,
        payload_digest="a" * 64,
    )
    proxy.run_versioned_plan_until_pause.assert_not_awaited()
    async with db_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            graph.receipt_id,
        )
        assert plan is not None and plan.active_run_id is None
        assert run is not None and run.status == "failed"
        assert run.generation == graph.generation
        assert run.cancellation_target_generation is None
        assert run.error == "remote failure"
        assert receipt.status == "settled"
        assert receipt.settlement_reason == "remote_pause"
        assert receipt.remote_status == "failed"


@pytest.mark.asyncio
async def test_restart_replays_exact_cancellation_and_imports_cancelled_graph(
    db_factory,
    monkeypatch,
):
    graph = await _seed_graph(
        db_factory,
        receipt_status="remote_possible",
        generation=0,
    )
    async with db_factory() as db:
        await fence_worker_mirror_cancellation(
            db,
            plan_id=graph.plan_id,
            run_id=graph.run_id,
            worker_id=graph.worker_id,
            generation=graph.generation,
            payload_digest="a" * 64,
        )

    cancelled = _cancelled_payload(graph)
    proxy = AsyncMock()
    proxy.cancel_versioned_plan_run.return_value = {
        "protocol": 1,
        "state": "terminal",
        "plan_id": graph.plan_id,
        "run_id": graph.run_id,
        "payload_digest": "a" * 64,
        "base_worker_version_id": cancelled["base_worker_version_id"],
        "run": cancelled["run"],
        "versions": cancelled["versions"],
    }
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = _dispatcher(db_factory)

    assert await dispatcher._recover_versioned_plan_runs() is False
    await dispatcher._running_tasks[f"worker-plan-{graph.run_id}"]

    async with db_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        receipt = await db.get(PlanAgentWorkerDispatchReceipt, graph.receipt_id)
        steps = list(
            (
                await db.execute(
                    select(PlanAgentStep).where(PlanAgentStep.run_id == graph.run_id)
                )
            ).scalars()
        )
        assert plan is not None and plan.active_run_id is None
        assert run is not None and run.status == "cancelled"
        assert run.generation == 1
        assert run.cancellation_target_generation == 0
        assert receipt is not None and receipt.status == "settled"
        assert receipt.settlement_reason == "remote_pause"
        assert receipt.remote_status == "cancelled"
        assert len(steps) == 1
        assert steps[0].output == "durable cancelled recovery evidence"


@pytest.mark.asyncio
async def test_restart_exact_cancellation_ack_loss_rearms_recovery(
    db_factory,
    monkeypatch,
):
    graph = await _seed_graph(
        db_factory,
        receipt_status="remote_possible",
        generation=0,
    )
    async with db_factory() as db:
        await fence_worker_mirror_cancellation(
            db,
            plan_id=graph.plan_id,
            run_id=graph.run_id,
            worker_id=graph.worker_id,
            generation=graph.generation,
            payload_digest="a" * 64,
        )

    entered = asyncio.Event()
    release = asyncio.Event()

    async def lose_ack(*_args, **_kwargs):
        entered.set()
        await release.wait()
        raise RuntimeError("ACK lost")

    proxy = AsyncMock()
    proxy.cancel_versioned_plan_run.side_effect = lose_ack
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = _dispatcher(db_factory)

    assert await dispatcher._recover_versioned_plan_runs() is False
    await entered.wait()
    lifecycle = dispatcher._running_tasks[f"worker-plan-{graph.run_id}"]
    release.set()
    await lifecycle

    assert dispatcher._plan_runtime_recovery_not_before is not None
    assert dispatcher.wake.called
    async with db_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        assert plan is not None and plan.active_run_id == graph.run_id
        assert run is not None and run.status == "cancelling"
        assert run.generation == graph.generation + 1
        assert run.cancellation_target_generation == graph.generation


@pytest.mark.asyncio
async def test_exact_cancel_recovery_progresses_after_old_lifecycle_is_reaped(
    db_factory,
    monkeypatch,
):
    graph = await _seed_graph(
        db_factory,
        receipt_status="remote_possible",
        generation=0,
    )
    async with db_factory() as db:
        await fence_worker_mirror_cancellation(
            db,
            plan_id=graph.plan_id,
            run_id=graph.run_id,
            worker_id=graph.worker_id,
            generation=graph.generation,
            payload_digest="a" * 64,
        )

    proxy = AsyncMock()
    proxy.cancel_versioned_plan_run.return_value = {
        "protocol": 1,
        "state": "absent",
        "plan_id": graph.plan_id,
        "run_id": graph.run_id,
        "payload_digest": "a" * 64,
        "base_worker_version_id": None,
        "run": None,
        "versions": [],
    }
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = _dispatcher(db_factory)

    lifecycle_entered = asyncio.Event()

    async def blocked_dispatch():
        lifecycle_entered.set()
        await asyncio.Event().wait()

    old_lifecycle = asyncio.create_task(blocked_dispatch())
    setattr(old_lifecycle, "_ccm_worker_plan_run_id", graph.run_id)
    dispatcher._running_tasks[f"worker-plan-{graph.run_id}"] = old_lifecycle
    await lifecycle_entered.wait()

    original_stop = dispatcher.stop_plan_run_lifecycle
    stop_attempts = 0

    async def fail_first_reap(run_id, instance_id):
        nonlocal stop_attempts
        stop_attempts += 1
        if stop_attempts == 1:
            raise RuntimeError("old lifecycle did not stop yet")
        return await original_stop(run_id, instance_id)

    dispatcher.stop_plan_run_lifecycle = AsyncMock(side_effect=fail_first_reap)

    # A transient reap failure keeps recovery armed and must not issue a
    # competing exact RPC while the stale dispatch still owns the run slot.
    assert await dispatcher._recover_versioned_plan_runs() is True
    proxy.cancel_versioned_plan_run.assert_not_awaited()

    # The next cold sweep retries the reap, then immediately acquires the same
    # durable intent and replays exact cancellation.
    assert await dispatcher._recover_versioned_plan_runs() is False
    assert old_lifecycle.done()
    recovery = dispatcher._running_tasks[f"worker-plan-{graph.run_id}"]
    assert recovery is not old_lifecycle
    await recovery
    assert dispatcher.stop_plan_run_lifecycle.await_count == 2

    proxy.cancel_versioned_plan_run.assert_awaited_once_with(
        graph.worker_id,
        graph.run_id,
        plan_id=graph.plan_id,
        payload_digest="a" * 64,
    )
    async with db_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        assert plan is not None and plan.active_run_id is None
        assert run is not None and run.status == "cancelled"


@pytest.mark.asyncio
async def test_prepared_recovery_holds_exact_target_task_writer_fence(
    db_factory,
    monkeypatch,
):
    from backend.services import plan_service

    graph = await _seed_graph(
        db_factory,
        receipt_status="prepared",
        with_target_task=True,
    )
    calls: list[tuple[int | None, int | None]] = []
    original = plan_service.fence_plan_target_task

    async def recording_fence(db, *, target_task_id, expected_worker_id):
        calls.append((target_task_id, expected_worker_id))
        return await original(
            db,
            target_task_id=target_task_id,
            expected_worker_id=expected_worker_id,
        )

    monkeypatch.setattr(plan_service, "fence_plan_target_task", recording_fence)
    monkeypatch.setattr(main_module, "worker_proxy", AsyncMock())
    dispatcher = _dispatcher(db_factory)

    assert await dispatcher._recover_versioned_plan_runs() is False

    assert calls == [(graph.target_task_id, graph.worker_id)]
    async with db_factory() as db:
        assert (await db.get(PlanAgentRun, graph.run_id)).status == "queued"


@pytest.mark.asyncio
async def test_restart_remote_possible_uses_reconciliation_not_import(
    db_factory,
    monkeypatch,
):
    graph = await _seed_graph(db_factory, receipt_status="remote_possible")
    proxy = AsyncMock()
    proxy.reconcile_versioned_plan_until_pause.return_value = _failed_payload(graph)
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = _dispatcher(db_factory)

    assert await dispatcher._recover_versioned_plan_runs() is False
    lifecycle = dispatcher._running_tasks[f"worker-plan-{graph.run_id}"]
    await lifecycle

    proxy.reconcile_versioned_plan_until_pause.assert_awaited_once()
    proxy.run_versioned_plan_until_pause.assert_not_awaited()
    async with db_factory() as db:
        run = await db.get(PlanAgentRun, graph.run_id)
        receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            graph.receipt_id,
        )
        assert run.status == "failed"
        assert run.error == "remote failure"
        assert receipt.status == "settled"
        assert receipt.settlement_reason == "remote_pause"
        assert receipt.remote_status == "failed"


@pytest.mark.asyncio
async def test_reconciliation_settlement_holds_target_task_writer_fence(
    db_factory,
    monkeypatch,
):
    from backend.services import plan_service

    graph = await _seed_graph(
        db_factory,
        receipt_status="remote_possible",
        with_target_task=True,
    )
    calls: list[tuple[int | None, int | None]] = []
    original = plan_service.fence_plan_target_task

    async def recording_fence(db, *, target_task_id, expected_worker_id):
        calls.append((target_task_id, expected_worker_id))
        return await original(
            db,
            target_task_id=target_task_id,
            expected_worker_id=expected_worker_id,
        )

    monkeypatch.setattr(plan_service, "fence_plan_target_task", recording_fence)
    proxy = AsyncMock()
    proxy.reconcile_versioned_plan_until_pause.return_value = _failed_payload(graph)
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = _dispatcher(db_factory)

    assert await dispatcher._recover_versioned_plan_runs() is False
    await dispatcher._running_tasks[f"worker-plan-{graph.run_id}"]

    assert calls == [(graph.target_task_id, graph.worker_id)]
    async with db_factory() as db:
        receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            graph.receipt_id,
        )
        assert receipt.status == "settled"


@pytest.mark.asyncio
async def test_restart_remote_absence_is_proved_before_requeue(
    db_factory,
    monkeypatch,
):
    graph = await _seed_graph(db_factory, receipt_status="remote_possible")
    proxy = AsyncMock()
    proxy.reconcile_versioned_plan_until_pause.side_effect = WorkerPlanRemoteAbsent(
        "exact audit absent"
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = _dispatcher(db_factory)

    assert await dispatcher._recover_versioned_plan_runs() is False
    await dispatcher._running_tasks[f"worker-plan-{graph.run_id}"]

    proxy.run_versioned_plan_until_pause.assert_not_awaited()
    async with db_factory() as db:
        run = await db.get(PlanAgentRun, graph.run_id)
        receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            graph.receipt_id,
        )
        assert run.status == "queued"
        assert run.generation == graph.generation + 1
        assert receipt.status == "settled"
        assert receipt.settlement_reason == "remote_absent"


@pytest.mark.parametrize(
    (
        "exception_type",
        "final_status",
        "settlement_reason",
        "remote_status",
        "generation_delta",
    ),
    [
        (WorkerPlanRemoteAbsent, "queued", "remote_absent", None, 1),
            (
                WorkerPlanRemoteCancelled,
                "cancelled",
                "remote_absent",
                None,
                1,
            ),
        (
            WorkerPlanRemoteIdentityConflict,
            "failed",
            "identity_conflict",
            "conflict",
            0,
        ),
    ],
)
@pytest.mark.asyncio
async def test_reconciliation_settlement_failure_rearms_cold_recovery(
    db_factory,
    monkeypatch,
    exception_type,
    final_status,
    settlement_reason,
    remote_status,
    generation_delta,
):
    graph = await _seed_graph(
        db_factory,
        receipt_status="remote_possible",
        with_target_task=True,
    )

    async def exact_remote_outcome(*_args, **_kwargs):
        raise exception_type(f"exact {settlement_reason}")

    proxy = AsyncMock()
    proxy.reconcile_versioned_plan_until_pause.side_effect = (
        exact_remote_outcome
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    real_fence = worker_plan_dispatch_module.fence_worker_dispatch_target
    fence_calls = 0

    async def fail_first_target_fence(db, *, receipt):
        nonlocal fence_calls
        fence_calls += 1
        if fence_calls == 1:
            raise HTTPException(409, "temporary target fence")
        return await real_fence(db, receipt=receipt)

    monkeypatch.setattr(
        worker_plan_dispatch_module,
        "fence_worker_dispatch_target",
        fail_first_target_fence,
    )
    dispatcher = _dispatcher(db_factory)

    async def cold_sweep() -> bool:
        signal_generation = (
            dispatcher._plan_runtime_recovery_signal_generation
        )
        retry_needed = await dispatcher._recover_versioned_plan_runs()
        if (
            retry_needed
            or signal_generation
            == dispatcher._plan_runtime_recovery_signal_generation
        ):
            dispatcher._record_plan_runtime_recovery_result(
                retry_needed=retry_needed,
            )
        return retry_needed

    # The producer clears its old schedule after synchronously registering the
    # lifecycle. A later temporary settlement failure must publish a newer
    # recovery generation and restore the durable retry deadline.
    assert await cold_sweep() is False
    first_lifecycle = dispatcher._running_tasks[
        f"worker-plan-{graph.run_id}"
    ]
    await first_lifecycle
    await asyncio.sleep(0)

    assert fence_calls == 1
    assert dispatcher._plan_runtime_recovery_not_before is not None
    assert dispatcher.wake.called
    async with db_factory() as db:
        run = await db.get(PlanAgentRun, graph.run_id)
        receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            graph.receipt_id,
        )
        assert run.status == "running"
        assert run.generation == graph.generation
        assert receipt.status == "remote_possible"
        assert receipt.settlement_reason is None

    # The next cold sweep retries the same exact audit identity. Once its
    # settlement fence succeeds, no further retry remains armed.
    assert await cold_sweep() is False
    second_lifecycle = dispatcher._running_tasks[
        f"worker-plan-{graph.run_id}"
    ]
    assert second_lifecycle is not first_lifecycle
    await second_lifecycle
    await asyncio.sleep(0)

    assert fence_calls == 2
    assert proxy.reconcile_versioned_plan_until_pause.await_count == 2
    assert dispatcher._plan_runtime_recovery_not_before is None
    async with db_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            graph.receipt_id,
        )
        assert run.status == final_status
        assert run.generation == graph.generation + generation_delta
        assert receipt.status == "settled"
        assert receipt.settlement_reason == settlement_reason
        assert receipt.remote_status == remote_status
        if final_status == "queued":
            assert plan.active_run_id == graph.run_id
        else:
            assert plan.active_run_id is None
        if final_status == "failed":
            assert settlement_reason in run.error


@pytest.mark.asyncio
async def test_graceful_shutdown_before_boundary_requeues_generation(
    db_factory,
    monkeypatch,
):
    graph = await _seed_graph(db_factory, receipt_status="prepared")
    started = asyncio.Event()
    blocker = asyncio.Event()
    proxy = AsyncMock()

    async def wait_before_boundary(_plan, _run, *, on_remote_possible):
        started.set()
        await blocker.wait()

    proxy.run_versioned_plan_until_pause.side_effect = wait_before_boundary
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = _dispatcher(db_factory)
    dispatcher._running = False
    lifecycle = asyncio.create_task(
        dispatcher._run_worker_plan_lifecycle(
            plan_id=graph.plan_id,
            run_id=graph.run_id,
            worker_id=graph.worker_id,
            generation=graph.generation,
            receipt_id=graph.receipt_id,
        )
    )
    await started.wait()
    lifecycle.cancel()
    with pytest.raises(asyncio.CancelledError):
        await lifecycle

    async with db_factory() as db:
        run = await db.get(PlanAgentRun, graph.run_id)
        receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            graph.receipt_id,
        )
        assert run.status == "queued"
        assert run.generation == graph.generation + 1
        assert receipt.status == "settled"
        assert receipt.settlement_reason == "not_launched"


@pytest.mark.asyncio
async def test_ack_loss_after_boundary_preserves_running_for_exact_audit(
    db_factory,
    monkeypatch,
):
    graph = await _seed_graph(db_factory, receipt_status="prepared")
    proxy = AsyncMock()

    async def lose_ack(_plan, _run, *, on_remote_possible):
        await on_remote_possible("b" * 64)
        raise httpx.ReadTimeout("ack lost after Worker import")

    proxy.run_versioned_plan_until_pause.side_effect = lose_ack
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = _dispatcher(db_factory)

    await dispatcher._run_worker_plan_lifecycle(
        plan_id=graph.plan_id,
        run_id=graph.run_id,
        worker_id=graph.worker_id,
        generation=graph.generation,
        receipt_id=graph.receipt_id,
    )

    async with db_factory() as db:
        run = await db.get(PlanAgentRun, graph.run_id)
        receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            graph.receipt_id,
        )
        assert run.status == "running"
        assert run.generation == graph.generation
        assert receipt.status == "remote_possible"
        assert receipt.payload_digest == "b" * 64
        assert "ack lost" in receipt.last_error
    assert dispatcher._plan_runtime_recovery_not_before is not None


@pytest.mark.asyncio
async def test_remote_boundary_rejects_non_hex_payload_digest(db_factory):
    graph = await _seed_graph(db_factory, receipt_status="prepared")

    with pytest.raises(
        WorkerPlanDispatchConflict,
        match="payload digest is invalid",
    ):
        await mark_worker_dispatch_remote_possible(
            db_factory,
            receipt_id=graph.receipt_id,
            plan_id=graph.plan_id,
            run_id=graph.run_id,
            worker_id=graph.worker_id,
            generation=graph.generation,
            payload_digest="z" * 64,
        )

    async with db_factory() as db:
        receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            graph.receipt_id,
        )
        assert receipt.status == "prepared"
        assert receipt.payload_digest is None


@pytest.mark.asyncio
async def test_old_worker_rejects_new_import_before_boundary(
    db_factory,
    monkeypatch,
):
    """A capability preflight failure is terminal only while still prepared."""

    graph = await _seed_graph(db_factory, receipt_status="prepared")
    proxy = AsyncMock()
    proxy.run_versioned_plan_until_pause.side_effect = (
        WorkerPlanReconciliationUnsupported("upgrade Worker before import")
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = _dispatcher(db_factory)

    await dispatcher._run_worker_plan_lifecycle(
        plan_id=graph.plan_id,
        run_id=graph.run_id,
        worker_id=graph.worker_id,
        generation=graph.generation,
        receipt_id=graph.receipt_id,
    )

    proxy.run_versioned_plan_until_pause.assert_awaited_once()
    proxy.reconcile_versioned_plan_until_pause.assert_not_awaited()
    async with db_factory() as db:
        plan = await db.get(Plan, graph.plan_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            graph.receipt_id,
        )
        assert plan.active_run_id is None
        assert run.status == "failed"
        assert run.current_stage == "failed"
        assert run.generation == graph.generation
        assert "upgrade Worker" in run.error
        assert receipt.status == "settled"
        assert receipt.settlement_reason == "preflight_failed"
        assert receipt.payload_digest is None
        assert receipt.remote_status is None
        assert "upgrade Worker" in receipt.last_error
    assert dispatcher._plan_runtime_recovery_not_before is None


@pytest.mark.asyncio
async def test_old_worker_recovery_keeps_remote_possible_unknown(
    db_factory,
    monkeypatch,
):
    graph = await _seed_graph(db_factory, receipt_status="remote_possible")
    proxy = AsyncMock()
    proxy.reconcile_versioned_plan_until_pause.side_effect = (
        WorkerPlanReconciliationUnsupported("upgrade Worker first")
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)
    dispatcher = _dispatcher(db_factory)

    assert await dispatcher._recover_versioned_plan_runs() is False
    await dispatcher._running_tasks[f"worker-plan-{graph.run_id}"]

    proxy.run_versioned_plan_until_pause.assert_not_awaited()
    async with db_factory() as db:
        run = await db.get(PlanAgentRun, graph.run_id)
        receipt = await db.get(
            PlanAgentWorkerDispatchReceipt,
            graph.receipt_id,
        )
        assert run.status == "running"
        assert run.generation == graph.generation
        assert receipt.status == "remote_possible"
        assert "upgrade Worker" in receipt.last_error
    assert dispatcher._plan_runtime_recovery_not_before is not None


class _Response:
    def __init__(self, payload: dict, *, status_code: int = 200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "failed",
                request=httpx.Request("GET", "http://worker"),
                response=httpx.Response(self.status_code),
            )


@pytest.mark.asyncio
async def test_proxy_reconciliation_is_read_only_for_terminal_remote(
    monkeypatch,
):
    graph = _Graph(
        7,
        8,
        9,
        4,
        10,
        None,
        datetime.utcnow().isoformat(),
        datetime.utcnow().isoformat(),
    )
    worker = Worker(
        id=graph.worker_id,
        name="audited-worker",
        status="ready",
        private_ip="10.0.0.7",
        ccm_port=8000,
        auth_token=_WORKER_CONTROL_PLANE_TOKEN,
    )
    plan = Plan(
        id=graph.plan_id,
        title="remote",
        initial_request="remote",
        worker_id=graph.worker_id,
        timeout_hours=1,
    )
    run = PlanAgentRun(
        id=graph.run_id,
        plan_id=graph.plan_id,
        worker_id=graph.worker_id,
    )
    calls: list[tuple[str, str]] = []

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, url, **_kwargs):
            calls.append(("GET", url))
            if url.endswith("/api/system/config"):
                return _Response({
                    "versioned_plan_worker_protocol": 3,
                    "worker_plan_reconciliation_protocol": 1,
                })
            return _Response({
                "protocol": 1,
                "state": "matched",
                "plan_id": graph.plan_id,
                "run_id": graph.run_id,
                "payload_digest": "c" * 64,
                "base_worker_version_id": None,
                "run": _failed_payload(graph)["run"],
                "versions": [],
            })

        async def post(self, *_args, **_kwargs):
            raise AssertionError("reconciliation must not repeat Worker import")

    monkeypatch.setattr("backend.services.worker_proxy.httpx.AsyncClient", Client)
    proxy = WorkerProxy(db_factory=AsyncMock(), relay=None)
    proxy.require_ready_worker = AsyncMock(return_value=worker)

    payload = await proxy.reconcile_versioned_plan_until_pause(
        plan,
        run,
        payload_digest="c" * 64,
    )

    assert payload["run"]["status"] == "failed"
    assert [method for method, _url in calls] == ["GET", "GET"]


@pytest.mark.asyncio
async def test_proxy_reconciliation_rejects_malformed_audit_before_side_effect(
    monkeypatch,
):
    """A boolean generation cannot authorize an exact answer replay."""

    graph = _Graph(
        7,
        8,
        9,
        4,
        10,
        None,
        datetime.utcnow().isoformat(),
        datetime.utcnow().isoformat(),
    )
    worker = Worker(
        id=graph.worker_id,
        name="audited-worker",
        status="ready",
        private_ip="10.0.0.7",
        ccm_port=8000,
        auth_token=_WORKER_CONTROL_PLANE_TOKEN,
    )
    plan = Plan(
        id=graph.plan_id,
        title="remote",
        initial_request="remote",
        worker_id=graph.worker_id,
        timeout_hours=1,
    )
    run = PlanAgentRun(
        id=graph.run_id,
        plan_id=graph.plan_id,
        worker_id=graph.worker_id,
    )
    malformed = _failed_payload(graph)["run"]
    malformed["generation"] = True
    calls: list[tuple[str, str]] = []

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, url, **_kwargs):
            calls.append(("GET", url))
            if url.endswith("/api/system/config"):
                return _Response(
                    {
                        "versioned_plan_worker_protocol": 3,
                        "worker_plan_reconciliation_protocol": 1,
                    }
                )
            return _Response(
                {
                    "protocol": 1,
                    "state": "matched",
                    "plan_id": graph.plan_id,
                    "run_id": graph.run_id,
                    "payload_digest": "c" * 64,
                    "base_worker_version_id": None,
                    "run": malformed,
                    "versions": [],
                }
            )

        async def post(self, *_args, **_kwargs):
            raise AssertionError("malformed audit must not authorize a side effect")

    monkeypatch.setattr("backend.services.worker_proxy.httpx.AsyncClient", Client)
    proxy = WorkerProxy(db_factory=AsyncMock(), relay=None)
    proxy.require_ready_worker = AsyncMock(return_value=worker)

    with pytest.raises(RuntimeError, match="invalid matched Plan audit"):
        await proxy.reconcile_versioned_plan_until_pause(
            plan,
            run,
            payload_digest="c" * 64,
        )

    assert [method for method, _url in calls] == ["GET", "GET"]


@pytest.mark.asyncio
async def test_old_worker_without_audit_protocol_fails_closed(monkeypatch):
    worker = Worker(
        id=3,
        name="old-worker",
        status="ready",
        private_ip="10.0.0.3",
        ccm_port=8000,
        auth_token=_WORKER_CONTROL_PLANE_TOKEN,
    )

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, _url, **_kwargs):
            return _Response({"versioned_plan_worker_protocol": 3})

    monkeypatch.setattr("backend.services.worker_proxy.httpx.AsyncClient", Client)
    proxy = WorkerProxy(db_factory=AsyncMock(), relay=None)
    proxy.require_ready_worker = AsyncMock(return_value=worker)
    plan = Plan(id=2, title="old", initial_request="old", worker_id=worker.id)
    run = PlanAgentRun(id=4, plan_id=plan.id, worker_id=worker.id)

    with pytest.raises(WorkerPlanReconciliationUnsupported):
        await proxy.reconcile_versioned_plan_until_pause(
            plan,
            run,
            payload_digest="d" * 64,
        )


@pytest.mark.asyncio
async def test_worker_import_audit_is_exact_and_read_only(client, session_factory):
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    body = {
        "protocol": 3,
        "plan_id": 7101,
        "run_id": 7201,
        "manager_claim_generation": 8,
        "title": "Audited mirror",
        "initial_request": "Audit without replay",
        "priority": 1,
        "pipeline_config": pipeline,
        "run_type": "initial",
        "request_text": "Audit without replay",
        "max_interactions": 3,
    }
    imported = await client.post("/api/plans/worker-import", json=body)
    assert imported.status_code == 200, imported.text
    digest = imported.json()["import_payload_digest"]

    matched = await client.get(
        "/api/plan-runs/7201/worker-import-audit",
        params={"plan_id": 7101, "payload_digest": digest},
    )
    conflict = await client.get(
        "/api/plan-runs/7201/worker-import-audit",
        params={"plan_id": 7101, "payload_digest": "f" * 64},
    )
    absent = await client.get(
        "/api/plan-runs/7299/worker-import-audit",
        params={"plan_id": 7101, "payload_digest": digest},
    )

    assert matched.status_code == 200, matched.text
    assert matched.json()["state"] == "matched"
    assert matched.json()["run"]["id"] == 7201
    assert conflict.status_code == 409
    assert absent.status_code == 200
    assert absent.json()["state"] == "absent"
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, 7201)
        assert run.status == "queued"
        assert run.generation == 0
        assert run.import_receipt_protocol == 1
        receipt = await db.get(PlanAgentWorkerImportReceipt, 7201)
        assert receipt is not None
        assert receipt.outcome == "imported"
        assert receipt.plan_id == run.plan_id
        assert receipt.payload_digest == run.import_payload_digest
