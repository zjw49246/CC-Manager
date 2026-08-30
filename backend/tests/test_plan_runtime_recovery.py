"""Crash/restart regressions for durable Plan runtime ownership.

These tests intentionally exercise the Dispatcher boundary instead of the
provider runner.  A Plan claim uses ``Instance.pid = NULL`` even while its
provider runtime is live, so only an exact in-memory lifecycle or a durable
runtime receipt may justify changing the Run/Instance owner graph.
"""

import asyncio
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text, update

from backend.models.capability import CapabilityExecution, CapabilityInvocation
from backend.models.instance import Instance
from backend.models.plan import Plan
from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentRuntimeReceipt,
    PlanAgentStep,
)
from backend.models.task import Task
from backend.services.dispatcher import GlobalDispatcher


@dataclass(frozen=True)
class _RunGraph:
    run_id: int
    plan_id: int
    step_id: int
    receipt_id: int
    generation: int
    runtime_generation: int
    owner_id: int | None
    invocation_id: int | None = None
    execution_id: int | None = None


def _dispatcher(db_factory, *, instance_manager=None) -> GlobalDispatcher:
    manager = instance_manager or MagicMock()
    manager.processes = {}
    manager._tasks = {}
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    return GlobalDispatcher(
        db_factory=db_factory,
        instance_manager=manager,
        broadcaster=broadcaster,
    )


async def _seed_run_graph(
    db_factory,
    *,
    name: str,
    status: str,
    generation: int,
    receipt_status: str,
    run_type: str = "initial",
    cancellation_target_generation: int | None = None,
    owner_shape: str = "both",
    provider: str = "codex",
    exact_capability_graph: bool = False,
    codex_home: str | None = None,
    codex_thread_id: str | None = None,
) -> _RunGraph:
    """Persist one Plan Run with a selectable forward/reverse owner shape."""

    if owner_shape not in {"both", "forward", "reverse", "none"}:
        raise AssertionError(f"unsupported owner shape: {owner_shape}")
    if exact_capability_graph and run_type != "capability":
        raise AssertionError("an exact capability graph needs a capability Run")

    runtime_generation = (
        cancellation_target_generation
        if cancellation_target_generation is not None
        else generation
    )
    async with db_factory() as db:
        invocation = None
        execution = None
        target_task = None
        if exact_capability_graph:
            target_task = Task(
                title=f"{name}-task",
                description="runtime recovery target",
                provider="codex",
            )
            db.add(target_task)
            await db.flush()
            digest = "a" * 64
            invocation = CapabilityInvocation(
                task_id=target_task.id,
                capability_key="plan",
                source="human_request",
                purpose="advisory",
                status="cancelling",
                state_version=2,
                idempotency_key=f"{name}-invocation",
                input_payload={"prompt": "plan safely"},
                input_hash=digest,
                subject_kind="task",
                subject_ref={"task_id": target_task.id},
                subject_hash=digest,
                executor_kind="plan_agent",
                executor_config={},
                executor_config_hash=digest,
                policy_snapshot={},
                policy_hash=digest,
                resume_policy="attach_only",
                max_attempts=1,
                active_task_id=target_task.id,
            )
            db.add(invocation)
            await db.flush()
            execution = CapabilityExecution(
                invocation_id=invocation.id,
                attempt=1,
                status="cancelling",
                state_version=2,
                active_invocation_id=invocation.id,
                idempotency_key=f"{name}-execution",
                executor_kind="plan_agent",
                input_hash=digest,
            )
            db.add(execution)
            await db.flush()

        plan = Plan(
            title=name,
            initial_request="recover this Plan",
            target_task_id=target_task.id if target_task is not None else None,
            pipeline_config={},
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            capability_execution_id=(execution.id if execution is not None else None),
            run_type=run_type,
            status=status,
            current_stage="planner",
            generation=generation,
            cancellation_target_generation=cancellation_target_generation,
            pipeline_config={},
            last_execution_started_at=datetime.utcnow(),
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        if execution is not None:
            execution.handle_kind = "plan_agent_run"
            execution.handle_id = str(run.id)
            # Plan Capability handle generations identify the immutable
            # staged Run handle. Runtime generations live exclusively on the
            # Plan Run and may advance across dispatcher claims.
            execution.handle_generation = 0

        owner = None
        if owner_shape != "none":
            owner = Instance(
                name=f"{name}-owner",
                status=("running" if owner_shape in {"both", "reverse"} else "idle"),
                pid=None,
                current_plan_run_id=(
                    run.id if owner_shape in {"both", "reverse"} else None
                ),
            )
            db.add(owner)
            await db.flush()
            if owner_shape in {"both", "forward"}:
                run.instance_id = owner.id

        step = PlanAgentStep(
            run_id=run.id,
            plan_id=plan.id,
            generation=runtime_generation,
            step_type="planner",
            provider=provider,
            status="running",
        )
        db.add(step)
        await db.flush()
        receipt = PlanAgentRuntimeReceipt(
            run_id=run.id,
            step_id=step.id,
            run_generation=runtime_generation,
            attempt_index=1,
            provider=provider,
            runtime_token=uuid.uuid4().hex,
            prepared_boot_id="00000000-0000-0000-0000-000000000001",
            prepared_start_ticks=1,
            prepared_uid=0,
            status=receipt_status,
            codex_home=codex_home,
            codex_thread_id=codex_thread_id,
            cleanup_error=(
                "runtime cleanup remains uncertain"
                if receipt_status == "cleanup_failed"
                else None
            ),
            cleaned_at=(datetime.utcnow() if receipt_status == "cleaned" else None),
        )
        db.add(receipt)
        await db.commit()
        return _RunGraph(
            run_id=run.id,
            plan_id=plan.id,
            step_id=step.id,
            receipt_id=receipt.id,
            generation=generation,
            runtime_generation=runtime_generation,
            owner_id=owner.id if owner is not None else None,
            invocation_id=invocation.id if invocation is not None else None,
            execution_id=execution.id if execution is not None else None,
        )


async def _assert_owner_graph(
    db_factory,
    graph: _RunGraph,
    *,
    run_instance_id: int | None,
    reverse_owner_run_id: int | None,
    run_status: str,
    generation: int,
    step_status: str,
) -> None:
    async with db_factory() as db:
        run = await db.get(PlanAgentRun, graph.run_id)
        step = await db.get(PlanAgentStep, graph.step_id)
        assert run is not None
        assert step is not None
        assert run.instance_id == run_instance_id
        assert run.status == run_status
        assert run.generation == generation
        assert step.status == step_status
        if graph.owner_id is not None:
            owner = await db.get(Instance, graph.owner_id)
            assert owner is not None
            assert owner.current_plan_run_id == reverse_owner_run_id


@pytest.mark.asyncio
async def test_warm_capability_g_to_g_plus_one_cleanup_releases_both_owners(
    db_factory,
    monkeypatch,
):
    """A live lifecycle may release only its fenced and durably clean G."""

    from backend.services import plan_agent_runner

    monkeypatch.setattr(plan_agent_runner, "active_plan_run_ids", lambda: set())
    graph = await _seed_run_graph(
        db_factory,
        name="warm-capability-cleanup",
        status="cancelling",
        generation=8,
        cancellation_target_generation=7,
        receipt_status="cleaned",
        run_type="capability",
        exact_capability_graph=True,
    )
    assert graph.owner_id is not None
    dispatcher = _dispatcher(db_factory)
    blocker = asyncio.Event()

    async def warm_lifecycle() -> None:
        try:
            await blocker.wait()
        finally:
            await dispatcher._cleanup_plan_run_owner(
                instance_id=graph.owner_id,
                run_id=graph.run_id,
                generation=graph.runtime_generation,
            )

    lifecycle = asyncio.create_task(warm_lifecycle())
    setattr(lifecycle, "_ccm_plan_run_id", graph.run_id)
    dispatcher._running_tasks[graph.owner_id] = lifecycle
    await asyncio.sleep(0)
    try:
        assert (
            await dispatcher.stop_capability_plan_run_lifecycle(
                graph.run_id,
                graph.owner_id,
            )
            is True
        )
    finally:
        if not lifecycle.done():
            lifecycle.cancel()
        await asyncio.gather(lifecycle, return_exceptions=True)
        dispatcher._running_tasks.pop(graph.owner_id, None)

    await _assert_owner_graph(
        db_factory,
        graph,
        run_instance_id=None,
        reverse_owner_run_id=None,
        run_status="cancelling",
        generation=8,
        step_status="cancelled",
    )


@pytest.mark.asyncio
async def test_restart_live_set_includes_running_task_plan_lifecycle(
    db_factory,
    monkeypatch,
):
    """stop -> start must not cold-recover work still in ``_running_tasks``."""

    from backend.services import plan_agent_runner, plan_runtime_receipt

    monkeypatch.setattr(plan_agent_runner, "active_plan_run_ids", lambda: set())
    reconcile = AsyncMock(return_value=True)
    monkeypatch.setattr(
        plan_runtime_receipt,
        "reconcile_runtime_generation",
        reconcile,
    )
    graph = await _seed_run_graph(
        db_factory,
        name="warm-restart-live-set",
        status="running",
        generation=4,
        receipt_status="cleaned",
    )
    assert graph.owner_id is not None
    dispatcher = _dispatcher(db_factory)
    blocker = asyncio.Event()
    lifecycle = asyncio.create_task(blocker.wait())
    setattr(lifecycle, "_ccm_plan_run_id", graph.run_id)
    dispatcher._running_tasks[graph.owner_id] = lifecycle
    try:
        await dispatcher._recover_versioned_plan_runs()
    finally:
        lifecycle.cancel()
        await asyncio.gather(lifecycle, return_exceptions=True)
        dispatcher._running_tasks.pop(graph.owner_id, None)

    reconcile.assert_not_awaited()
    await _assert_owner_graph(
        db_factory,
        graph,
        run_instance_id=graph.owner_id,
        reverse_owner_run_id=graph.run_id,
        run_status="running",
        generation=4,
        step_status="running",
    )


@pytest.mark.asyncio
async def test_ordinary_running_cold_recovery_advances_only_clean_generation(
    db_factory,
    monkeypatch,
):
    """A cleaned G is replayed as G+1 and its bidirectional owner is released."""

    from backend.services import plan_agent_runner

    monkeypatch.setattr(plan_agent_runner, "active_plan_run_ids", lambda: set())
    graph = await _seed_run_graph(
        db_factory,
        name="ordinary-clean-cold-recovery",
        status="running",
        generation=11,
        receipt_status="cleaned",
    )
    dispatcher = _dispatcher(db_factory)

    await dispatcher._recover_versioned_plan_runs()

    await _assert_owner_graph(
        db_factory,
        graph,
        run_instance_id=None,
        reverse_owner_run_id=None,
        run_status="queued",
        generation=12,
        step_status="cancelled",
    )


@pytest.mark.asyncio
async def test_running_cold_recovery_discards_cleaned_receipt_from_reused_run_id(
    db_factory,
    monkeypatch,
):
    """A terminal receipt older than its Run cannot fence a reused SQLite ID."""

    from backend.services import plan_agent_runner

    monkeypatch.setattr(plan_agent_runner, "active_plan_run_ids", lambda: set())
    async with db_factory() as db:
        plan = Plan(
            title="reused-run-id",
            initial_request="recover startup",
            pipeline_config={},
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            status="running",
            current_stage="planner",
            generation=1,
            pipeline_config={},
            last_execution_started_at=datetime.utcnow(),
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        receipt = PlanAgentRuntimeReceipt(
            run_id=run.id,
            step_id=999,
            run_generation=1,
            attempt_index=1,
            provider="claude",
            runtime_token=uuid.uuid4().hex,
            prepared_boot_id="00000000-0000-0000-0000-000000000001",
            prepared_start_ticks=1,
            prepared_uid=0,
            status="cleaned",
            created_at=run.created_at - timedelta(hours=1),
            updated_at=run.created_at - timedelta(hours=1),
            cleaned_at=run.created_at - timedelta(hours=1),
        )
        db.add(receipt)
        await db.commit()
        run_id = run.id
        receipt_id = receipt.id

    assert await _dispatcher(db_factory)._recover_versioned_plan_runs() is False

    async with db_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        assert run is not None
        assert run.status == "queued"
        assert run.generation == 2
        assert run.instance_id is None
        assert run.execution_seconds == 0
        assert run.last_execution_started_at is None
        assert await db.get(PlanAgentRuntimeReceipt, receipt_id) is None


@pytest.mark.asyncio
async def test_running_cold_recovery_keeps_unclean_receipt_from_reused_run_id(
    db_factory,
    monkeypatch,
):
    """An older receipt without cleanup proof still blocks automatic replay."""

    from backend.services import plan_agent_runner

    monkeypatch.setattr(plan_agent_runner, "active_plan_run_ids", lambda: set())
    async with db_factory() as db:
        plan = Plan(
            title="unclean-reused-run-id",
            initial_request="remain fenced",
            pipeline_config={},
        )
        db.add(plan)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            status="running",
            current_stage="planner",
            generation=1,
            pipeline_config={},
            last_execution_started_at=datetime.utcnow(),
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        receipt = PlanAgentRuntimeReceipt(
            run_id=run.id,
            step_id=999,
            run_generation=1,
            attempt_index=1,
            provider="claude",
            runtime_token=uuid.uuid4().hex,
            prepared_boot_id="00000000-0000-0000-0000-000000000001",
            prepared_start_ticks=1,
            prepared_uid=0,
            status="prepared",
            created_at=run.created_at - timedelta(hours=1),
            updated_at=run.created_at - timedelta(hours=1),
        )
        db.add(receipt)
        await db.commit()
        run_id = run.id
        receipt_id = receipt.id

    assert await _dispatcher(db_factory)._recover_versioned_plan_runs() is True

    async with db_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        receipt = await db.get(PlanAgentRuntimeReceipt, receipt_id)
        assert run is not None and run.status == "running"
        assert receipt is not None and receipt.status == "prepared"


@pytest.mark.asyncio
async def test_running_cold_recovery_releases_empty_error_owner(
    db_factory,
    monkeypatch,
):
    """A dead exact owner may be left in error after its provider crashes."""

    from backend.services import plan_agent_runner

    monkeypatch.setattr(plan_agent_runner, "active_plan_run_ids", lambda: set())
    graph = await _seed_run_graph(
        db_factory,
        name="error-owner-clean-cold-recovery",
        status="running",
        generation=15,
        receipt_status="cleaned",
    )
    assert graph.owner_id is not None
    async with db_factory() as db:
        owner = await db.get(Instance, graph.owner_id)
        owner.status = "error"
        await db.commit()

    dispatcher = _dispatcher(db_factory)
    await dispatcher._recover_versioned_plan_runs()

    await _assert_owner_graph(
        db_factory,
        graph,
        run_instance_id=None,
        reverse_owner_run_id=None,
        run_status="queued",
        generation=16,
        step_status="cancelled",
    )


@pytest.mark.asyncio
async def test_ordinary_running_unclean_generation_preserves_entire_owner_graph(
    db_factory,
    monkeypatch,
):
    """Ambiguous Codex G identity must not requeue or discard either owner."""

    from backend.services import plan_agent_runner

    monkeypatch.setattr(plan_agent_runner, "active_plan_run_ids", lambda: set())
    graph = await _seed_run_graph(
        db_factory,
        name="ordinary-unclean-cold-recovery",
        status="running",
        generation=13,
        receipt_status="launching",
        provider="codex",
        # Seed a valid row, then emulate storage corruption below. The DB CHECK
        # rejects this shape during ordinary writes; service validation remains
        # a fail-closed second line of defence for imported/legacy corruption.
        codex_home="/managed/codex-home",
        codex_thread_id="corrupted-thread",
    )
    async with db_factory() as db:
        await db.execute(text("PRAGMA ignore_check_constraints = ON"))
        await db.execute(
            update(PlanAgentRuntimeReceipt)
            .where(PlanAgentRuntimeReceipt.id == graph.receipt_id)
            .values(codex_thread_id=None)
        )
        await db.commit()
        await db.execute(text("PRAGMA ignore_check_constraints = OFF"))
    dispatcher = _dispatcher(db_factory)

    await dispatcher._recover_versioned_plan_runs()

    await _assert_owner_graph(
        db_factory,
        graph,
        run_instance_id=graph.owner_id,
        reverse_owner_run_id=graph.run_id,
        run_status="running",
        generation=13,
        step_status="running",
    )
    async with db_factory() as db:
        receipt = await db.get(PlanAgentRuntimeReceipt, graph.receipt_id)
        run = await db.get(PlanAgentRun, graph.run_id)
        assert receipt is not None
        assert receipt.status == "launching"
        assert receipt.cleanup_error is None
        assert run is not None
        assert run.last_execution_started_at is not None


@pytest.mark.asyncio
async def test_capability_cold_cancelling_exact_graph_and_clean_receipt_converge(
    db_factory,
    monkeypatch,
):
    """Cold recovery releases the owner but leaves terminal publication fenced."""

    from backend.services import plan_agent_runner

    monkeypatch.setattr(plan_agent_runner, "active_plan_run_ids", lambda: set())
    graph = await _seed_run_graph(
        db_factory,
        name="capability-cold-cancelling",
        status="cancelling",
        generation=6,
        cancellation_target_generation=5,
        receipt_status="cleaned",
        run_type="capability",
        exact_capability_graph=True,
    )
    dispatcher = _dispatcher(db_factory)

    await dispatcher._recover_versioned_plan_runs()

    await _assert_owner_graph(
        db_factory,
        graph,
        run_instance_id=None,
        reverse_owner_run_id=None,
        run_status="cancelling",
        generation=6,
        step_status="cancelled",
    )
    async with db_factory() as db:
        run = await db.get(PlanAgentRun, graph.run_id)
        plan = await db.get(Plan, graph.plan_id)
        invocation = await db.get(CapabilityInvocation, graph.invocation_id)
        execution = await db.get(CapabilityExecution, graph.execution_id)
        assert run is not None and run.cancellation_target_generation == 5
        assert plan is not None and plan.active_run_id == graph.run_id
        assert invocation is not None and invocation.status == "cancelling"
        assert execution is not None and execution.status == "cancelling"


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["malformed_capability"])
async def test_cold_recovery_does_not_touch_non_exact_cancelling_run(
    db_factory,
    monkeypatch,
    shape,
):
    """Runtime cleanup is not authorized until the Capability graph is exact."""

    from backend.services import plan_agent_runner, plan_runtime_receipt

    monkeypatch.setattr(plan_agent_runner, "active_plan_run_ids", lambda: set())
    reconcile = AsyncMock(return_value=True)
    monkeypatch.setattr(
        plan_runtime_receipt,
        "reconcile_runtime_generation",
        reconcile,
    )
    graph = await _seed_run_graph(
        db_factory,
        name=f"non-exact-cancelling-{shape}",
        status="cancelling",
        generation=10,
        cancellation_target_generation=9,
        receipt_status="cleaned",
        run_type="capability",
        exact_capability_graph=False,
    )
    dispatcher = _dispatcher(db_factory)

    await dispatcher._recover_versioned_plan_runs()

    reconcile.assert_not_awaited()
    await _assert_owner_graph(
        db_factory,
        graph,
        run_instance_id=graph.owner_id,
        reverse_owner_run_id=graph.run_id,
        run_status="cancelling",
        generation=10,
        step_status="running",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_shape", ["forward", "reverse"])
async def test_clean_capability_receipt_converges_one_way_owner(
    db_factory,
    monkeypatch,
    owner_shape,
):
    """Clean exact G can repair either half of an interrupted owner release."""

    from backend.services import plan_agent_runner

    monkeypatch.setattr(plan_agent_runner, "active_plan_run_ids", lambda: set())
    graph = await _seed_run_graph(
        db_factory,
        name=f"one-way-capability-owner-{owner_shape}",
        status="cancelling",
        generation=15,
        cancellation_target_generation=14,
        receipt_status="cleaned",
        run_type="capability",
        exact_capability_graph=True,
        owner_shape=owner_shape,
    )
    dispatcher = _dispatcher(db_factory)

    assert (
        await dispatcher.stop_capability_plan_run_lifecycle(
            graph.run_id,
            graph.owner_id,
        )
        is True
    )

    await _assert_owner_graph(
        db_factory,
        graph,
        run_instance_id=None,
        reverse_owner_run_id=None,
        run_status="cancelling",
        generation=15,
        step_status="cancelled",
    )


class _FakeCodexRegistry:
    def __init__(self, *, home: str, has_owner: bool):
        self._lock = asyncio.Lock()
        self.peer_turn = (
            SimpleNamespace(returncode=None) if has_owner else None
        )
        self.peer_server = (
            SimpleNamespace(
                has_active_turns=True,
                peer_turn=self.peer_turn,
                shutdown=AsyncMock(),
            )
            if has_owner
            else None
        )
        self._servers = {home: self.peer_server} if has_owner else {}
        self._starting = {}
        self._thread_owners = {"peer-thread": home} if has_owner else {}
        self.delete_thread = AsyncMock()


def _codex_instance_manager(registry: _FakeCodexRegistry):
    manager = MagicMock()
    manager._ensure_codex_app_server_registry = MagicMock(return_value=registry)

    @asynccontextmanager
    async def guard(home):
        yield home

    manager.codex_home_app_server_guard = guard
    return manager


@pytest.mark.asyncio
async def test_codex_cold_recovery_deletes_only_exact_thread_and_preserves_peer(
    db_factory,
    monkeypatch,
):
    """Warm and cold cleanup leave an unrelated shared-home peer untouched."""

    from backend.services import plan_runtime_receipt

    home = "/managed/shared-codex-home"
    killpg = MagicMock()
    monkeypatch.setattr(
        plan_runtime_receipt,
        "_exact_codex_transport_is_live",
        lambda receipt: receipt.process_id is not None,
    )
    monkeypatch.setattr(plan_runtime_receipt.os, "killpg", killpg)

    warm = await _seed_run_graph(
        db_factory,
        name="warm-codex-thread-cleanup",
        status="running",
        generation=21,
        receipt_status="launching",
        owner_shape="none",
        provider="codex",
        codex_home=home,
        codex_thread_id="warm-thread",
    )
    warm_registry = _FakeCodexRegistry(home=home, has_owner=True)
    assert (
        await plan_runtime_receipt.reconcile_runtime_generation(
            db_factory,
            _codex_instance_manager(warm_registry),
            run_id=warm.run_id,
            generation=warm.runtime_generation,
            allow_transport_kill=False,
        )
        is True
    )
    killpg.assert_not_called()
    warm_registry.delete_thread.assert_awaited_once_with(home, "warm-thread")
    assert warm_registry._thread_owners == {"peer-thread": home}
    assert warm_registry.peer_turn is not None
    assert warm_registry.peer_turn.returncode is None
    warm_registry.peer_server.shutdown.assert_not_awaited()

    cold = await _seed_run_graph(
        db_factory,
        name="cold-codex-shared-owner",
        status="running",
        generation=22,
        receipt_status="launching",
        owner_shape="none",
        provider="codex",
        codex_home=home,
        codex_thread_id="cold-thread",
    )
    cold_registry = _FakeCodexRegistry(home=home, has_owner=True)
    assert (
        await plan_runtime_receipt.reconcile_runtime_generation(
            db_factory,
            _codex_instance_manager(cold_registry),
            run_id=cold.run_id,
            generation=cold.runtime_generation,
            allow_transport_kill=True,
        )
        is True
    )
    killpg.assert_not_called()
    cold_registry.delete_thread.assert_awaited_once_with(home, "cold-thread")
    assert cold_registry._thread_owners == {"peer-thread": home}
    assert cold_registry.peer_turn is not None
    assert cold_registry.peer_turn.returncode is None
    cold_registry.peer_server.shutdown.assert_not_awaited()
    async with db_factory() as db:
        receipt = await db.get(PlanAgentRuntimeReceipt, cold.receipt_id)
        assert receipt is not None
        assert receipt.status == "cleaned"
        assert receipt.cleanup_error is None


@pytest.mark.asyncio
async def test_codex_cold_recovery_keeps_peer_when_exact_thread_delete_is_uncertain(
    db_factory,
    monkeypatch,
):
    """An unconfirmed exact delete stays fenced without transport signalling."""

    from backend.services import plan_runtime_receipt

    home = "/managed/uncertain-shared-codex-home"
    graph = await _seed_run_graph(
        db_factory,
        name="cold-codex-uncertain-thread-delete",
        status="running",
        generation=23,
        receipt_status="launching",
        owner_shape="none",
        provider="codex",
        codex_home=home,
        codex_thread_id="uncertain-thread",
    )
    registry = _FakeCodexRegistry(home=home, has_owner=True)
    registry.delete_thread.side_effect = RuntimeError(
        "thread state could not be proven terminal"
    )
    killpg = MagicMock()
    monkeypatch.setattr(plan_runtime_receipt.os, "killpg", killpg)

    assert (
        await plan_runtime_receipt.reconcile_runtime_generation(
            db_factory,
            _codex_instance_manager(registry),
            run_id=graph.run_id,
            generation=graph.runtime_generation,
            allow_transport_kill=True,
        )
        is False
    )

    killpg.assert_not_called()
    registry.delete_thread.assert_awaited_once_with(home, "uncertain-thread")
    assert registry._thread_owners == {"peer-thread": home}
    assert registry.peer_turn is not None
    assert registry.peer_turn.returncode is None
    registry.peer_server.shutdown.assert_not_awaited()
    async with db_factory() as db:
        receipt = await db.get(PlanAgentRuntimeReceipt, graph.receipt_id)
        assert receipt is not None
        assert receipt.status == "cleanup_failed"
        assert "could not be proven terminal" in (receipt.cleanup_error or "")


@pytest.mark.asyncio
async def test_codex_cold_recovery_retries_after_shared_transport_exits(
    db_factory,
    monkeypatch,
):
    """A live shared transport defers G; its later exit converges G exactly."""

    from backend.services import plan_agent_runner, plan_runtime_receipt

    monkeypatch.setattr(plan_agent_runner, "active_plan_run_ids", lambda: set())
    home = "/managed/retry-shared-codex-home"
    graph = await _seed_run_graph(
        db_factory,
        name="cold-codex-retry-after-transport-exit",
        status="running",
        generation=31,
        receipt_status="launching",
        provider="codex",
        codex_home=home,
        codex_thread_id="orphan-thread",
    )
    registry = _FakeCodexRegistry(home=home, has_owner=True)
    dispatcher = _dispatcher(
        db_factory,
        instance_manager=_codex_instance_manager(registry),
    )
    transport_live = iter((True, False))
    monkeypatch.setattr(
        plan_runtime_receipt,
        "_exact_codex_transport_is_live",
        lambda _receipt: next(transport_live),
    )
    killpg = MagicMock()
    monkeypatch.setattr(plan_runtime_receipt.os, "killpg", killpg)

    assert await dispatcher._recover_versioned_plan_runs() is True
    dispatcher._record_plan_runtime_recovery_result(retry_needed=True)

    await _assert_owner_graph(
        db_factory,
        graph,
        run_instance_id=graph.owner_id,
        reverse_owner_run_id=graph.run_id,
        run_status="running",
        generation=31,
        step_status="running",
    )
    async with db_factory() as db:
        receipt = await db.get(PlanAgentRuntimeReceipt, graph.receipt_id)
        assert receipt is not None
        assert receipt.status == "cleanup_failed"
    registry.delete_thread.assert_not_awaited()
    killpg.assert_not_called()

    dispatcher._running = True
    dispatcher._plan_runtime_recovery_not_before = 0.0
    await dispatcher._recover_due_versioned_plan_runs()

    registry.delete_thread.assert_awaited_once_with(home, "orphan-thread")
    killpg.assert_not_called()
    assert dispatcher._plan_runtime_recovery_not_before is None
    assert dispatcher._dispatch_wakeup.is_set()
    await _assert_owner_graph(
        db_factory,
        graph,
        run_instance_id=None,
        reverse_owner_run_id=None,
        run_status="queued",
        generation=32,
        step_status="cancelled",
    )
    async with db_factory() as db:
        receipt = await db.get(PlanAgentRuntimeReceipt, graph.receipt_id)
        assert receipt is not None
        assert receipt.status == "cleaned"


@pytest.mark.asyncio
async def test_plan_lifecycle_schedules_cold_retry_only_for_unclean_generation(
    db_factory,
    monkeypatch,
):
    """A disappearing live owner advertises unclean G, but clean G does not."""

    from backend.services import plan_agent_runner

    class _Runner:
        def __init__(self, **_kwargs):
            pass

        async def advance_versioned(self, _run_id, *, cwd):
            assert cwd
            return "superseded"

    monkeypatch.setattr(plan_agent_runner, "PlanAgentRunner", _Runner)

    unclean = await _seed_run_graph(
        db_factory,
        name="lifecycle-unclean-retry",
        status="running",
        generation=41,
        receipt_status="cleanup_failed",
    )
    assert unclean.owner_id is not None
    dispatcher = _dispatcher(db_factory)
    dispatcher._running = True
    lifecycle = asyncio.create_task(
        dispatcher._run_plan_run_lifecycle(
            unclean.owner_id,
            unclean.run_id,
            unclean.generation,
        )
    )
    setattr(lifecycle, "_ccm_plan_run_id", unclean.run_id)
    dispatcher._running_tasks[unclean.owner_id] = lifecycle
    await lifecycle

    assert dispatcher._plan_runtime_recovery_not_before is not None
    assert dispatcher._dispatch_wakeup.is_set()
    assert unclean.owner_id not in dispatcher._running_tasks
    await _assert_owner_graph(
        db_factory,
        unclean,
        run_instance_id=unclean.owner_id,
        reverse_owner_run_id=unclean.run_id,
        run_status="running",
        generation=41,
        step_status="running",
    )

    clean = await _seed_run_graph(
        db_factory,
        name="lifecycle-clean-no-retry",
        status="running",
        generation=42,
        receipt_status="cleaned",
    )
    assert clean.owner_id is not None
    clean_dispatcher = _dispatcher(db_factory)
    clean_dispatcher._running = True
    clean_lifecycle = asyncio.create_task(
        clean_dispatcher._run_plan_run_lifecycle(
            clean.owner_id,
            clean.run_id,
            clean.generation,
        )
    )
    setattr(clean_lifecycle, "_ccm_plan_run_id", clean.run_id)
    clean_dispatcher._running_tasks[clean.owner_id] = clean_lifecycle
    await clean_lifecycle

    assert clean_dispatcher._plan_runtime_recovery_not_before is None
    assert not clean_dispatcher._dispatch_wakeup.is_set()
    assert clean.owner_id not in clean_dispatcher._running_tasks


@pytest.mark.asyncio
async def test_due_plan_recovery_does_not_run_while_paused_or_shutting_down(
    db_factory,
):
    dispatcher = _dispatcher(db_factory)
    dispatcher._running = True
    dispatcher._plan_runtime_recovery_not_before = 0.0
    dispatcher._recover_versioned_plan_runs = AsyncMock(return_value=False)

    dispatcher._dispatch_paused = True
    await dispatcher._recover_due_versioned_plan_runs()
    dispatcher._recover_versioned_plan_runs.assert_not_awaited()

    dispatcher._dispatch_paused = False
    dispatcher._shutting_down = True
    await dispatcher._recover_due_versioned_plan_runs()
    dispatcher._recover_versioned_plan_runs.assert_not_awaited()

    dispatcher._shutting_down = False
    dispatcher._running = False
    await dispatcher._recover_due_versioned_plan_runs()
    dispatcher._recover_versioned_plan_runs.assert_not_awaited()


@pytest.mark.asyncio
async def test_pause_waits_for_inflight_plan_recovery_admission(db_factory):
    """Maintenance cannot return while a destructive cold scan is in flight."""

    dispatcher = _dispatcher(db_factory)
    dispatcher._running = True
    dispatcher._plan_runtime_recovery_not_before = 0.0
    recovery_started = asyncio.Event()
    release_recovery = asyncio.Event()

    async def held_recovery():
        recovery_started.set()
        await release_recovery.wait()
        return False

    dispatcher._recover_versioned_plan_runs = AsyncMock(
        side_effect=held_recovery
    )
    recovery = asyncio.create_task(
        dispatcher._recover_due_versioned_plan_runs()
    )
    pause = None
    try:
        await asyncio.wait_for(recovery_started.wait(), timeout=1)
        pause = asyncio.create_task(dispatcher.pause_dispatching())
        await asyncio.sleep(0)
        assert pause.done() is False

        release_recovery.set()
        await asyncio.wait_for(recovery, timeout=1)
        await asyncio.wait_for(pause, timeout=1)
        assert dispatcher._dispatch_paused is True
    finally:
        release_recovery.set()
        for task in (recovery, pause):
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_clean_scan_cannot_erase_new_lifecycle_recovery_signal(db_factory):
    """A live generation may become orphaned while an older scan awaits I/O."""

    dispatcher = _dispatcher(db_factory)
    dispatcher._running = True
    dispatcher._plan_runtime_recovery_not_before = 0.0

    async def scan_while_lifecycle_exits():
        dispatcher._request_plan_runtime_recovery()
        return False

    dispatcher._recover_versioned_plan_runs = AsyncMock(
        side_effect=scan_while_lifecycle_exits
    )

    await dispatcher._recover_due_versioned_plan_runs()

    dispatcher._recover_versioned_plan_runs.assert_awaited_once_with()
    assert dispatcher._plan_runtime_recovery_signal_generation == 1
    assert dispatcher._plan_runtime_recovery_not_before is not None
    assert dispatcher._dispatch_wakeup.is_set()


def test_plan_runtime_recovery_backoff_is_bounded_and_resets(db_factory):
    dispatcher = _dispatcher(db_factory)

    for _ in range(10):
        dispatcher._record_plan_runtime_recovery_result(retry_needed=True)

    assert dispatcher._plan_runtime_recovery_backoff == 60.0
    assert dispatcher._plan_runtime_recovery_not_before is not None

    dispatcher._record_plan_runtime_recovery_result(retry_needed=False)

    assert dispatcher._plan_runtime_recovery_backoff == 5.0
    assert dispatcher._plan_runtime_recovery_not_before is None


@pytest.mark.asyncio
async def test_dispatch_loop_serializes_recovery_before_plan_claim_registration(
    db_factory,
):
    """No second recovery pass can enter the claim-to-registration window."""

    dispatcher = _dispatcher(db_factory)
    dispatcher._running = True
    events: list[str] = []
    instance = SimpleNamespace(id=987654, name="serialized-plan-slot")
    reservation = object()
    reserve_calls = 0
    lifecycle_registered = asyncio.Event()

    async def recover_due():
        events.append("recover")

    async def reserve(_db):
        nonlocal reserve_calls
        reserve_calls += 1
        if reserve_calls == 1:
            return instance, reservation
        return None, None

    async def claim(_db, *, instance_id):
        assert instance_id == 987654
        events.append("claim")
        return (765432, 9)

    async def lifecycle(instance_id, run_id, generation):
        current = asyncio.current_task()
        assert dispatcher._running_tasks[instance_id] is current
        assert getattr(current, "_ccm_plan_run_id") == run_id
        assert generation == 9
        events.append("registered")
        lifecycle_registered.set()
        dispatcher._running = False
        dispatcher.wake()

    dispatcher._recover_due_versioned_plan_runs = recover_due
    dispatcher._ensure_min_idle_instances = AsyncMock()
    dispatcher._dispatch_worker_tasks = AsyncMock()
    dispatcher._dispatch_worker_plan_runs = AsyncMock()
    dispatcher._reserve_idle_instance = reserve
    dispatcher._claim_plan_run = claim
    dispatcher._run_plan_run_lifecycle = lifecycle

    loop = asyncio.create_task(dispatcher._dispatch_loop())
    await asyncio.wait_for(lifecycle_registered.wait(), timeout=1)
    await asyncio.wait_for(loop, timeout=1)

    assert events == ["recover", "claim", "registered"]
