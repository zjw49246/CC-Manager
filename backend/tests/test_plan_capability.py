"""Plan Capability adapter transaction and lifecycle tests."""

import asyncio
import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import settings
from backend.models.capability import CapabilityExecution, CapabilityInvocation
from backend.models.instance import Instance
from backend.models.plan import Plan, PlanInputRequest, PlanVersion
from backend.models.plan_agent import PlanAgentRun, PlanAgentStep
from backend.models.task import Task
from backend.schemas.plan import default_plan_pipeline_config
from backend.services import plan_capability as adapter_module
from backend.services import capability_service
from backend.services.capability_registry import (
    register_capability,
    resolve_capability,
    unregister_capability,
)
from backend.services.plan_capability import (
    PlanCapabilityCancellationUnconfirmed,
    PlanCapabilityExecutor,
    plan_capability_definition,
)
from backend.services.dispatcher import GlobalDispatcher
from backend.services.plan_agent_runner import PlanAgentRunner
from backend.services.plan_runtime_receipt import new_prepared_runtime_receipt
from backend.services.plan_service import (
    answer_input_request,
    plan_operation_lock,
    plan_resource,
)


@pytest.fixture(autouse=True)
def plan_capability_runtime(monkeypatch):
    previous_flag = settings.capability_core_enabled
    previous_definition = resolve_capability("plan")
    settings.capability_core_enabled = True
    unregister_capability("plan")
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    pipeline["reviewer"]["enabled"] = True
    register_capability(
        plan_capability_definition(
            pipeline_config=pipeline,
            max_attempts=2,
        )
    )
    monkeypatch.setattr(
        adapter_module,
        "capture_repo_revision",
        AsyncMock(return_value={"available": True, "head": "abc"}),
    )
    monkeypatch.setattr(
        adapter_module,
        "broadcast_plan_event",
        AsyncMock(),
    )
    yield
    unregister_capability("plan")
    if previous_definition is not None:
        register_capability(previous_definition)
    settings.capability_core_enabled = previous_flag


async def _create_invocation(db_session) -> tuple[Task, CapabilityInvocation]:
    task = Task(
        title="Implement the capability",
        description="Build the requested feature safely",
        target_repo="/repo",
        target_branch="main",
        provider="codex",
    )
    db_session.add(task)
    await db_session.commit()
    invocation, created = await capability_service.create_human_invocation(
        db_session,
        task_id=task.id,
        capability_key="plan",
        request_payload={"prompt": "Produce an implementation plan"},
        idempotency_key=f"plan-{task.id}",
        requested_by_user_id=7,
    )
    assert created is True
    return task, invocation


async def _execution(db_session, invocation_id: int) -> CapabilityExecution:
    execution = await capability_service.active_execution_for(
        db_session, invocation_id
    )
    assert execution is not None
    return execution


async def _handled_run(
    db_session,
    invocation_id: int,
) -> tuple[CapabilityExecution, PlanAgentRun, Plan]:
    execution = (
        await db_session.execute(
            select(CapabilityExecution)
            .where(CapabilityExecution.invocation_id == invocation_id)
            .order_by(CapabilityExecution.attempt.desc())
            .limit(1)
        )
    ).scalar_one()
    assert execution.handle_id is not None
    run = await db_session.get(PlanAgentRun, int(execution.handle_id))
    assert run is not None and run.plan_id is not None
    plan = await db_session.get(Plan, run.plan_id)
    assert plan is not None
    return execution, run, plan


def _dispatcher(db_factory) -> GlobalDispatcher:
    manager = MagicMock()
    manager.processes = {}
    manager._tasks = {}
    broadcaster = MagicMock()
    broadcaster.broadcast = AsyncMock()
    return GlobalDispatcher(
        db_factory=db_factory,
        instance_manager=manager,
        broadcaster=broadcaster,
    )


def test_default_plan_capability_retries_one_transient_run_failure():
    assert plan_capability_definition().max_attempts == 2


@pytest.mark.asyncio
async def test_failed_plan_run_is_replaced_before_capability_becomes_terminal(
    db_session,
):
    _task, invocation = await _create_invocation(db_session)
    executor = PlanCapabilityExecutor()
    first = await executor.ensure_started(
        db_session,
        invocation_id=invocation.id,
    )
    assert first.run_id is not None

    failed_run = await db_session.get(PlanAgentRun, first.run_id)
    failed_plan = await db_session.get(Plan, failed_run.plan_id)
    failed_run.status = "failed"
    failed_run.current_stage = "failed"
    failed_run.error = "reviewer routes are temporarily unavailable"
    failed_run.finished_at = datetime.utcnow()
    failed_plan.active_run_id = None
    await db_session.commit()

    retrying = await executor.observe(
        db_session,
        invocation_id=invocation.id,
    )
    assert retrying.status == "queued"
    active = await capability_service.active_execution_for(
        db_session,
        invocation.id,
    )
    assert active is not None
    assert active.attempt == 2

    second = await executor.ensure_started(
        db_session,
        invocation_id=invocation.id,
    )
    assert second.run_id is not None
    assert second.run_id != first.run_id


async def _claim_capability_run(
    db_factory,
    dispatcher: GlobalDispatcher,
    *,
    run_id: int,
    instance_id: int,
) -> tuple[int, int]:
    async with db_factory() as db:
        run = await db.get(PlanAgentRun, run_id)
        instance = await db.get(Instance, instance_id)
        assert run is not None and run.status == "queued"
        assert instance is not None and instance.status == "idle"
        claimed = await dispatcher._claim_plan_run(
            db,
            instance_id=instance.id,
        )
    assert claimed is not None and claimed[0] == run_id
    return claimed


def _clean_stage_stub(db_factory, outputs: dict[str, dict]):
    async def fake_stage(**kwargs):
        output = outputs[kwargs["step_type"]]
        async with db_factory() as db:
            step = PlanAgentStep(
                run_id=kwargs["run_id"],
                plan_id=kwargs["plan_id"],
                step_type=kwargs["step_type"],
                round=kwargs["round_number"],
                generation=kwargs["generation"],
                provider="codex",
                model="test-model",
                route_slot="primary",
                status="completed",
                output=json.dumps(output),
                finished_at=datetime.utcnow(),
            )
            db.add(step)
            await db.flush()
            receipt = new_prepared_runtime_receipt(step, attempt_index=1)
            receipt.status = "cleaned"
            receipt.cleaned_at = datetime.utcnow()
            db.add(receipt)
            await db.commit()
        return output, json.dumps(output), object(), "primary", "test-account"

    return fake_stage


@pytest.mark.asyncio
async def test_plan_and_handle_creation_roll_back_together(db_session, monkeypatch):
    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    executor = PlanCapabilityExecutor()
    original_stage = adapter_module.stage_plan_with_run

    async def fail_after_staging(*args, **kwargs):
        await original_stage(*args, **kwargs)
        raise capability_service.CapabilityConflictError("claim lost")

    monkeypatch.setattr(adapter_module, "stage_plan_with_run", fail_after_staging)

    with pytest.raises(capability_service.CapabilityConflictError, match="claim lost"):
        await executor.ensure_started(db_session, invocation_id=invocation_id)

    assert await db_session.scalar(select(func.count(Plan.id))) == 0
    assert await db_session.scalar(select(func.count(PlanAgentRun.id))) == 0
    execution = await _execution(db_session, invocation_id)
    assert execution.status == "queued"
    assert execution.handle_id is None


@pytest.mark.asyncio
async def test_ensure_started_replays_exact_durable_handle(db_session):
    _task, invocation = await _create_invocation(db_session)
    executor = PlanCapabilityExecutor()

    first = await executor.ensure_started(db_session, invocation_id=invocation.id)
    second = await executor.ensure_started(db_session, invocation_id=invocation.id)

    assert first.status == second.status == "running"
    assert first.plan_id == second.plan_id
    assert first.run_id == second.run_id
    assert first.execution_id == second.execution_id
    assert await db_session.scalar(select(func.count(Plan.id))) == 1
    assert await db_session.scalar(select(func.count(PlanAgentRun.id))) == 1
    execution, run, _plan = await _handled_run(db_session, invocation.id)
    assert run.run_type == "capability"
    assert run.capability_execution_id == execution.id


@pytest.mark.asyncio
async def test_started_capability_projects_explicit_read_only_plan_resource(db_session):
    _task, invocation = await _create_invocation(db_session)
    await PlanCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation.id,
    )
    _execution_row, _run, plan = await _handled_run(db_session, invocation.id)

    resource = await plan_resource(db_session, plan, include_audit=True)

    assert resource.ownership == "capability"
    assert resource.read_only is True
    assert resource.active_run is not None
    assert resource.active_run.run_type == "capability"


@pytest.mark.asyncio
async def test_concurrent_start_creates_one_reverse_bound_plan(
    db_session,
    db_factory,
):
    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id

    async def start():
        async with db_factory() as session:
            return await PlanCapabilityExecutor().ensure_started(
                session,
                invocation_id=invocation_id,
            )

    first, second = await asyncio.gather(start(), start())
    assert first.run_id == second.run_id
    assert first.execution_id == second.execution_id
    assert await db_session.scalar(select(func.count(Plan.id))) == 1
    run = await db_session.get(
        PlanAgentRun,
        first.run_id,
        populate_existing=True,
    )
    assert run.capability_execution_id == first.execution_id


@pytest.mark.asyncio
async def test_plan_staging_uses_request_task_snapshot(db_session):
    task, invocation = await _create_invocation(db_session)
    task.target_repo = "/changed-after-request"
    task.target_branch = "changed"
    task.title = "changed title"
    await db_session.commit()

    started = await PlanCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation.id,
    )
    _execution_row, run, plan = await _handled_run(db_session, invocation.id)

    assert started.status == "running"
    assert plan.target_repo == "/repo"
    assert plan.target_branch == "main"
    assert plan.title == f"Plan for #{task.id}: Implement the capability"
    assert run.context_session_id == invocation.request_task_session_id


@pytest.mark.asyncio
async def test_waiting_plan_maps_to_waiting_and_answered_run_resumes(db_session):
    _task, invocation = await _create_invocation(db_session)
    executor = PlanCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    _execution_row, run, _plan = await _handled_run(db_session, invocation.id)

    run.status = "waiting_user"
    run.open_input_request_id = 41
    await db_session.commit()
    waiting = await executor.observe(db_session, invocation_id=invocation.id)
    assert waiting.status == "waiting_user"
    assert waiting.run_status == "waiting_user"
    assert waiting.input_request_id == 41

    run.status = "queued"
    run.open_input_request_id = None
    run.generation += 1
    await db_session.commit()
    resumed = await executor.observe(db_session, invocation_id=invocation.id)
    assert resumed.status == "running"
    assert resumed.run_status == "queued"
    assert resumed.run_generation == 1


async def _complete_run(
    db_session,
    invocation_id: int,
    *,
    run_verdict: str = "approve",
    version_verdict: str = "approve",
    exhausted: bool = False,
    exact_identity: bool = True,
    with_result: bool = True,
) -> int | None:
    _execution_row, run, plan = await _handled_run(db_session, invocation_id)
    version_id = None
    if with_result:
        # A normal reviewed pipeline uses two distinct dispatcher claims:
        # planner G and reviewer G+1.  Keep this compact fixture aligned with
        # that production fence; a dedicated test below drives both claims.
        run.generation = max(run.generation, 2)
        planner_step = PlanAgentStep(
            run_id=run.id,
            plan_id=plan.id,
            generation=run.generation - 1,
            step_type="planner",
            round=run.round,
            provider="codex",
            status="completed",
            output="# Exact implementation plan",
            finished_at=datetime.utcnow(),
        )
        reviewer_step = PlanAgentStep(
            run_id=run.id,
            plan_id=plan.id,
            generation=run.generation,
            step_type="reviewer",
            round=run.round,
            provider="codex",
            status="completed",
            output='{"action":"approve"}',
            finished_at=datetime.utcnow(),
        )
        db_session.add_all([planner_step, reviewer_step])
        await db_session.flush()
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            produced_by_run_id=run.id if exact_identity else None,
            produced_by_step_id=planner_step.id,
            content="# Exact implementation plan",
            context_session_id=run.context_session_id,
            context_log_id=run.context_log_id,
            repo_revision=run.repo_revision,
            reviewer_repo_revision=run.repo_revision,
            review_verdict=version_verdict,
            review_feedback="looks good",
            reviewed_by_step_id=reviewer_step.id,
            review_exhausted=exhausted,
            reviewed_at=datetime.utcnow(),
        )
        db_session.add(version)
        await db_session.flush()
        planner_step.plan_version_id = version.id
        run.draft_step_id = planner_step.id
        version_id = version.id
        plan.current_version_id = version.id
    plan.active_run_id = None
    run.status = "completed"
    run.current_stage = "complete"
    run.result_version_id = version_id
    run.review_verdict = run_verdict
    run.review_exhausted = exhausted
    run.finished_at = datetime.utcnow()
    await db_session.commit()
    return version_id


@pytest.mark.asyncio
async def test_only_exact_approved_version_completes_capability(db_session):
    _task, invocation = await _create_invocation(db_session)
    executor = PlanCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    version_id = await _complete_run(db_session, invocation.id)

    ready = await executor.observe(db_session, invocation_id=invocation.id)

    assert ready.status == "ready"
    assert ready.output_version_id == version_id
    assert ready.output_hash is not None and len(ready.output_hash) == 64
    execution = await db_session.get(CapabilityExecution, ready.execution_id)
    assert execution.output_kind == "plan_version"
    _execution_row, _run, plan = await _handled_run(db_session, invocation.id)
    resource = await plan_resource(db_session, plan, include_audit=True)
    assert resource.ownership == "capability"
    assert resource.read_only is True
    assert resource.display_state == "awaiting_review"
    assert resource.active_run is None


@pytest.mark.asyncio
async def test_real_planner_and_reviewer_claims_complete_exact_capability(
    db_session,
    db_factory,
):
    """Planner G1 and reviewer G2 remain an exact, valid result chain."""

    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    executor = PlanCapabilityExecutor()
    started = await executor.ensure_started(
        db_session,
        invocation_id=invocation_id,
    )
    assert started.run_id is not None
    instance = Instance(name="two-stage-capability-slot", status="idle")
    db_session.add(instance)
    await db_session.commit()
    instance_id = instance.id
    dispatcher = _dispatcher(db_factory)
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=dispatcher.instance_manager,
    )
    runner._run_stage = _clean_stage_stub(
        db_factory,
        {
            "planner": {
                "action": "propose",
                "plan": "# Exact two-claim implementation plan",
            },
            "reviewer": {
                "action": "approve",
                "feedback": "Exact and testable",
            },
        },
    )

    first_claim = await _claim_capability_run(
        db_factory,
        dispatcher,
        run_id=started.run_id,
        instance_id=instance_id,
    )
    assert first_claim[1] == 1
    assert await runner.advance_versioned(started.run_id, cwd="/tmp") == "queued"
    second_claim = await _claim_capability_run(
        db_factory,
        dispatcher,
        run_id=started.run_id,
        instance_id=instance_id,
    )
    assert second_claim[1] == 2
    assert await runner.advance_versioned(started.run_id, cwd="/tmp") == "completed"

    db_session.expire_all()
    ready = await executor.observe(db_session, invocation_id=invocation_id)
    assert ready.status == "ready"
    execution, run, _plan = await _handled_run(db_session, invocation_id)
    assert execution.handle_generation == 0
    assert run.generation == 2
    steps = list(
        (
            await db_session.execute(
                select(PlanAgentStep)
                .where(PlanAgentStep.run_id == run.id)
                .order_by(PlanAgentStep.id)
            )
        ).scalars()
    )
    assert [(step.step_type, step.generation) for step in steps] == [
        ("planner", 1),
        ("reviewer", 2),
    ]


@pytest.mark.asyncio
async def test_capability_cancel_between_planner_and_reviewer_claims(
    db_session,
    db_factory,
):
    """A queued reviewer retains G1 as the exact cancellation target."""

    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    started = await PlanCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation_id,
    )
    assert started.run_id is not None
    owner = Instance(name="between-stage-capability-slot", status="idle")
    db_session.add(owner)
    await db_session.commit()
    dispatcher = _dispatcher(db_factory)
    runner = PlanAgentRunner(
        db_factory=db_factory,
        instance_manager=dispatcher.instance_manager,
    )
    runner._run_stage = _clean_stage_stub(
        db_factory,
        {
            "planner": {
                "action": "propose",
                "plan": "# Awaiting exact reviewer claim",
            },
        },
    )
    await _claim_capability_run(
        db_factory,
        dispatcher,
        run_id=started.run_id,
        instance_id=owner.id,
    )
    assert await runner.advance_versioned(started.run_id, cwd="/tmp") == "queued"

    db_session.expire_all()
    cancelled = await PlanCapabilityExecutor(
        stop_callback=dispatcher.stop_capability_plan_run_lifecycle,
    ).cancel(db_session, invocation_id=invocation_id)

    assert cancelled.status == "cancelled"
    execution, run, plan = await _handled_run(db_session, invocation_id)
    assert execution.handle_generation == 0
    assert run.status == "cancelled"
    assert run.generation == 2
    assert run.cancellation_target_generation == 1
    assert plan.active_run_id is None


@pytest.mark.asyncio
async def test_capability_claim_and_cancel_race_has_no_cross_aggregate_deadlock(
    tmp_path,
    monkeypatch,
):
    """A WAL claim between cancellation's read and UPDATE is fenced exactly."""

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from backend.database import Base

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'capability-claim-cancel-wal.db'}",
        connect_args={"timeout": 1},
    )
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        db_factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        async with db_factory() as setup:
            _task, invocation = await _create_invocation(setup)
            started = await PlanCapabilityExecutor().ensure_started(
                setup,
                invocation_id=invocation.id,
            )
            assert started.run_id is not None
            owner = Instance(name="claim-cancel-race-slot", status="idle")
            setup.add(owner)
            await setup.commit()
            owner_id = owner.id
            invocation_id = invocation.id
            execution_id = started.execution_id
            run_id = started.run_id

        dispatcher = _dispatcher(db_factory)
        real_fence = adapter_module.fence_capability_run_cancellation
        cancellation_read_generation = asyncio.Event()
        allow_cancellation_update = asyncio.Event()

        async def interleaved_fence(db, *, plan, run):
            # ``cancel()`` owns a G0 WAL read snapshot. Commit G1 through a
            # distinct connection before its fresh-writer cancellation CAS.
            assert run.generation == 0
            assert run.status == "queued"
            cancellation_read_generation.set()
            await allow_cancellation_update.wait()
            return await real_fence(db, plan=plan, run=run)

        monkeypatch.setattr(
            adapter_module,
            "fence_capability_run_cancellation",
            interleaved_fence,
        )

        async def claim():
            async with db_factory() as db:
                instance = await db.get(Instance, owner_id)
                assert instance is not None
                return await dispatcher._claim_plan_run(
                    db,
                    instance_id=instance.id,
                )

        async def cancel():
            async with db_factory() as db:
                return await PlanCapabilityExecutor(
                    stop_callback=dispatcher.stop_capability_plan_run_lifecycle,
                ).cancel(db, invocation_id=invocation_id)

        cancellation = asyncio.create_task(cancel())
        await asyncio.wait_for(cancellation_read_generation.wait(), timeout=2)
        try:
            claimed = await asyncio.wait_for(claim(), timeout=2)
        finally:
            allow_cancellation_update.set()
        cancelled = await asyncio.wait_for(cancellation, timeout=5)

        assert claimed == (run_id, 1)
        assert cancelled.status == "cancelled"
        async with db_factory() as db:
            execution = await db.get(CapabilityExecution, execution_id)
            run = await db.get(PlanAgentRun, run_id)
            owner = await db.get(Instance, owner_id)
            assert execution is not None and execution.handle_generation == 0
            assert run is not None and run.status == "cancelled"
            assert run.generation == 2
            assert run.cancellation_target_generation == 1
            assert run.instance_id is None
            assert owner is not None and owner.current_plan_run_id is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_wrong_immutable_plan_handle_generation_fails_closed(
    db_session,
    db_factory,
):
    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    started = await PlanCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation_id,
    )
    execution = await db_session.get(CapabilityExecution, started.execution_id)
    execution.handle_generation = 9
    await db_session.commit()

    with pytest.raises(
        capability_service.CapabilityConflictError,
        match="does not belong",
    ):
        await PlanCapabilityExecutor(
            stop_callback=_dispatcher(
                db_factory
            ).stop_capability_plan_run_lifecycle,
        ).cancel(db_session, invocation_id=invocation_id)

    stored_invocation = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    stored_execution = await db_session.get(
        CapabilityExecution,
        started.execution_id,
        populate_existing=True,
    )
    run = await db_session.get(
        PlanAgentRun,
        started.run_id,
        populate_existing=True,
    )
    assert stored_invocation.status == "running"
    assert stored_execution.status == "running"
    assert run.status == "queued"


@pytest.mark.asyncio
async def test_cancel_claims_queued_recovery_handle_before_terminalizing_core(
    db_session,
):
    """A queued durable handle cannot leave its staged Plan orphaned."""

    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    started = await PlanCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation_id,
    )
    execution, run, plan = await _handled_run(db_session, invocation_id)
    run_id = run.id
    plan_id = plan.id
    execution_id = execution.id
    invocation.status = "queued"
    invocation.state_version += 1
    execution.status = "queued"
    execution.state_version += 1
    execution.lease_token = None
    execution.lease_expires_at = None
    execution.heartbeat_at = None
    execution.started_at = None
    await db_session.commit()

    cancelled = await PlanCapabilityExecutor().cancel(
        db_session,
        invocation_id=invocation_id,
    )

    assert cancelled.status == "cancelled"
    assert cancelled.run_status == "cancelled"
    stored_execution = await db_session.get(
        CapabilityExecution,
        execution_id,
        populate_existing=True,
    )
    stored_run = await db_session.get(
        PlanAgentRun,
        run_id,
        populate_existing=True,
    )
    stored_plan = await db_session.get(Plan, plan_id, populate_existing=True)
    assert stored_execution.status == "cancelled"
    assert stored_execution.handle_generation == 0
    assert stored_run.status == "cancelled"
    assert stored_run.cancellation_target_generation == 0
    assert stored_plan.active_run_id is None


@pytest.mark.asyncio
async def test_terminal_plan_cancel_crash_is_recovered_from_exact_cleaned_generation(
    db_session,
    db_factory,
    monkeypatch,
):
    """Run cancelled / Execution cancelling is a durable idempotent window."""

    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    started = await PlanCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation_id,
    )
    dispatcher = _dispatcher(db_factory)
    original_mark_cancelled = adapter_module.mark_execution_cancelled
    monkeypatch.setattr(
        adapter_module,
        "mark_execution_cancelled",
        AsyncMock(side_effect=RuntimeError("crash after Plan terminal commit")),
    )

    with pytest.raises(RuntimeError, match="crash after Plan terminal"):
        await PlanCapabilityExecutor(
            stop_callback=dispatcher.stop_capability_plan_run_lifecycle,
        ).cancel(db_session, invocation_id=invocation_id)

    db_session.expire_all()
    execution = await db_session.get(CapabilityExecution, started.execution_id)
    run = await db_session.get(PlanAgentRun, started.run_id)
    plan = await db_session.get(Plan, started.plan_id)
    assert execution.status == "cancelling"
    assert run.status == "cancelled"
    assert run.cancellation_target_generation == 0
    assert plan.active_run_id is None

    # A new process/session observes the same durable window and finishes the
    # Capability side without replaying or re-stopping provider work.
    monkeypatch.setattr(
        adapter_module,
        "mark_execution_cancelled",
        original_mark_cancelled,
    )
    async with db_factory() as restarted_db:
        recovered = await PlanCapabilityExecutor(
            stop_callback=dispatcher.stop_capability_plan_run_lifecycle,
        ).observe(restarted_db, invocation_id=invocation_id)
    assert recovered.status == "cancelled"
    assert recovered.run_status == "cancelled"
    async with db_factory() as db:
        execution = await db.get(CapabilityExecution, started.execution_id)
        run = await db.get(PlanAgentRun, started.run_id)
        assert execution.status == "cancelled"
        assert run.status == "cancelled"
        assert run.cancellation_target_generation == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_status", ["completed", "failed"])
async def test_cancel_converges_when_plan_terminal_wins_after_core_fence(
    db_session,
    monkeypatch,
    terminal_status,
):
    """A clean Plan terminal is preserved when it races Core cancellation."""

    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    started = await PlanCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation_id,
    )
    execution, run, plan = await _handled_run(db_session, invocation_id)
    run_id = run.id
    plan_id = plan.id
    execution_id = execution.id
    generation = run.generation
    step = PlanAgentStep(
        run_id=run_id,
        plan_id=plan_id,
        generation=generation,
        step_type="planner",
        provider="codex",
        status="completed",
        finished_at=datetime.utcnow(),
    )
    db_session.add(step)
    await db_session.flush()
    receipt = new_prepared_runtime_receipt(step, attempt_index=1)
    receipt.status = "cleaned"
    receipt.cleaned_at = datetime.utcnow()
    db_session.add(receipt)
    await db_session.commit()

    original_cancel_invocation = adapter_module.cancel_invocation

    async def cancel_then_publish_terminal(db, **kwargs):
        result = await original_cancel_invocation(db, **kwargs)
        raced_run = await db.get(
            PlanAgentRun,
            run_id,
            with_for_update=True,
            populate_existing=True,
        )
        raced_plan = await db.get(
            Plan,
            plan_id,
            with_for_update=True,
            populate_existing=True,
        )
        assert raced_run is not None and raced_plan is not None
        raced_run.status = terminal_status
        raced_run.current_stage = (
            "complete" if terminal_status == "completed" else "failed"
        )
        raced_run.error = None if terminal_status == "completed" else "provider failed"
        raced_run.finished_at = datetime.utcnow()
        raced_plan.active_run_id = None
        await db.commit()
        return result

    monkeypatch.setattr(
        adapter_module,
        "cancel_invocation",
        cancel_then_publish_terminal,
    )
    monkeypatch.setattr(adapter_module, "active_plan_run_ids", lambda: set())
    runtime_stopper = AsyncMock()
    monkeypatch.setattr(
        adapter_module,
        "cancel_plan_run_runtime",
        runtime_stopper,
    )
    dispatcher_stopper = AsyncMock(return_value=True)

    cancelled = await PlanCapabilityExecutor(
        stop_callback=dispatcher_stopper,
    ).cancel(db_session, invocation_id=invocation_id)

    assert cancelled.status == "cancelled"
    assert cancelled.run_status == terminal_status
    dispatcher_stopper.assert_not_awaited()
    runtime_stopper.assert_not_awaited()
    stored_invocation = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    stored_execution = await db_session.get(
        CapabilityExecution,
        execution_id,
        populate_existing=True,
    )
    stored_run = await db_session.get(
        PlanAgentRun,
        run_id,
        populate_existing=True,
    )
    stored_plan = await db_session.get(Plan, plan_id, populate_existing=True)
    assert stored_invocation.status == "cancelled"
    assert stored_execution.status == "cancelled"
    assert stored_run.status == terminal_status
    assert stored_run.generation == generation
    assert stored_run.cancellation_target_generation is None
    assert stored_plan.active_run_id is None


@pytest.mark.asyncio
async def test_cancel_terminal_race_with_unclean_runtime_stays_cancelling(
    db_session,
    monkeypatch,
):
    """A terminal label alone cannot erase an unclean provider receipt."""

    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    await PlanCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation_id,
    )
    execution, run, plan = await _handled_run(db_session, invocation_id)
    run_id = run.id
    plan_id = plan.id
    execution_id = execution.id
    step = PlanAgentStep(
        run_id=run_id,
        plan_id=plan_id,
        generation=run.generation,
        step_type="planner",
        provider="codex",
        status="completed",
        finished_at=datetime.utcnow(),
    )
    db_session.add(step)
    await db_session.flush()
    receipt = new_prepared_runtime_receipt(step, attempt_index=1)
    receipt.status = "launching"
    receipt.codex_home = "/tmp/ccm-test-codex-home"
    receipt.codex_thread_id = "thread-unclean-runtime"
    db_session.add(receipt)
    await db_session.commit()

    original_cancel_invocation = adapter_module.cancel_invocation

    async def cancel_then_publish_terminal(db, **kwargs):
        result = await original_cancel_invocation(db, **kwargs)
        raced_run = await db.get(
            PlanAgentRun,
            run_id,
            with_for_update=True,
            populate_existing=True,
        )
        raced_plan = await db.get(
            Plan,
            plan_id,
            with_for_update=True,
            populate_existing=True,
        )
        assert raced_run is not None and raced_plan is not None
        raced_run.status = "completed"
        raced_run.current_stage = "complete"
        raced_run.finished_at = datetime.utcnow()
        raced_plan.active_run_id = None
        await db.commit()
        return result

    monkeypatch.setattr(
        adapter_module,
        "cancel_invocation",
        cancel_then_publish_terminal,
    )
    monkeypatch.setattr(adapter_module, "active_plan_run_ids", lambda: set())
    runtime_stopper = AsyncMock()
    monkeypatch.setattr(
        adapter_module,
        "cancel_plan_run_runtime",
        runtime_stopper,
    )
    dispatcher_stopper = AsyncMock(return_value=True)

    with pytest.raises(
        PlanCapabilityCancellationUnconfirmed,
        match="runtime evidence is incomplete",
    ):
        await PlanCapabilityExecutor(
            stop_callback=dispatcher_stopper,
        ).cancel(db_session, invocation_id=invocation_id)

    dispatcher_stopper.assert_not_awaited()
    runtime_stopper.assert_not_awaited()
    stored_invocation = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    stored_execution = await db_session.get(
        CapabilityExecution,
        execution_id,
        populate_existing=True,
    )
    stored_run = await db_session.get(
        PlanAgentRun,
        run_id,
        populate_existing=True,
    )
    stored_plan = await db_session.get(Plan, plan_id, populate_existing=True)
    assert stored_invocation.status == "cancelling"
    assert stored_execution.status == "cancelling"
    assert stored_run.status == "completed"
    assert stored_run.cancellation_target_generation is None
    assert stored_plan.active_run_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "completion",
    [
        {"with_result": False},
        {"exact_identity": False},
        {"run_verdict": "revise", "version_verdict": "revise"},
        {"exhausted": True, "run_verdict": "revise", "version_verdict": "exhausted"},
    ],
    ids=["missing-result", "wrong-identity", "revise", "review-exhausted"],
)
async def test_unapproved_or_inexact_completed_run_fails_closed(
    db_session,
    completion,
):
    _task, invocation = await _create_invocation(db_session)
    executor = PlanCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    await _complete_run(db_session, invocation.id, **completion)

    failed = await executor.observe(db_session, invocation_id=invocation.id)

    assert failed.status == "failed"
    assert failed.error_code in {
        "plan_result_invalid",
        "plan_review_not_approved",
    }
    stored = await db_session.get(CapabilityInvocation, invocation.id)
    assert stored.active_task_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tamper",
    [
        "planner_type",
        "reviewer_run",
        "produced_step",
        "reviewed_step",
        "planner_generation",
        "reviewer_generation",
    ],
)
async def test_wrong_planner_or_reviewer_step_identity_fails_closed(
    db_session,
    tamper,
):
    _task, invocation = await _create_invocation(db_session)
    executor = PlanCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    version_id = await _complete_run(db_session, invocation.id)
    version = await db_session.get(PlanVersion, version_id)
    _execution_row, run, _plan = await _handled_run(db_session, invocation.id)
    planner = await db_session.get(PlanAgentStep, version.produced_by_step_id)
    reviewer = await db_session.get(PlanAgentStep, version.reviewed_by_step_id)
    assert planner is not None and reviewer is not None

    if tamper == "planner_type":
        planner.step_type = "reviewer"
    elif tamper == "reviewer_run":
        reviewer.run_id = run.id + 999
    elif tamper == "produced_step":
        version.produced_by_step_id = reviewer.id
    elif tamper == "reviewed_step":
        version.reviewed_by_step_id = planner.id
    elif tamper == "planner_generation":
        planner.generation = run.generation
    else:
        reviewer.generation = planner.generation
    await db_session.commit()

    failed = await executor.observe(db_session, invocation_id=invocation.id)
    assert failed.status == "failed"
    assert failed.error_code == "plan_result_invalid"


@pytest.mark.asyncio
async def test_cancel_proves_plan_run_stopped_before_capability_terminal(db_session):
    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    stopper = AsyncMock(return_value=True)
    executor = PlanCapabilityExecutor(stop_callback=stopper)
    await executor.ensure_started(db_session, invocation_id=invocation_id)
    _execution_row, run, _plan = await _handled_run(db_session, invocation_id)
    run.status = "running"
    run.generation = 3
    await db_session.commit()

    without_stopper = PlanCapabilityExecutor()
    with pytest.raises(PlanCapabilityCancellationUnconfirmed, match="stop callback"):
        await without_stopper.cancel(db_session, invocation_id=invocation_id)
    still_cancelling = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    assert still_cancelling.status == "cancelling"

    cancelled = await executor.cancel(db_session, invocation_id=invocation_id)
    assert cancelled.status == "cancelled"
    assert cancelled.run_status == "cancelled"
    stopper.assert_awaited_once_with(run.id, None)
    stored_run = await db_session.get(PlanAgentRun, run.id)
    assert stored_run.status == "cancelled"


@pytest.mark.asyncio
async def test_cancel_stop_false_retains_durable_cancelling_fence(db_session):
    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    await PlanCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation_id,
    )
    _execution_row, run, _plan = await _handled_run(db_session, invocation_id)
    run.status = "running"
    await db_session.commit()

    rejected = PlanCapabilityExecutor(stop_callback=AsyncMock(return_value=False))
    with pytest.raises(
        PlanCapabilityCancellationUnconfirmed,
        match="stop was not confirmed",
    ):
        await rejected.cancel(db_session, invocation_id=invocation_id)

    stored_invocation = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    stored_run = await db_session.get(
        PlanAgentRun,
        run.id,
        populate_existing=True,
    )
    assert stored_invocation.status == "cancelling"
    assert stored_run.status == "cancelling"

    accepted_stopper = AsyncMock(return_value=True)
    cancelled = await PlanCapabilityExecutor(
        stop_callback=accepted_stopper
    ).cancel(db_session, invocation_id=invocation_id)
    assert cancelled.status == "cancelled"
    accepted_stopper.assert_awaited_once()


@pytest.mark.asyncio
async def test_cold_restart_recovery_converges_cancelling_capability(
    db_session,
    db_factory,
    monkeypatch,
):
    """A restarted dispatcher and executor finish the durable cancel fence."""
    from backend.services import plan_agent_runner

    monkeypatch.setattr(plan_agent_runner, "active_plan_run_ids", lambda: set())
    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    await PlanCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation_id,
    )
    execution, run, plan = await _handled_run(db_session, invocation_id)
    owner = Instance(
        name="cold-restart-plan-owner",
        status="running",
        current_plan_run_id=run.id,
    )
    db_session.add(owner)
    await db_session.flush()
    run.status = "running"
    run.instance_id = owner.id
    run.last_execution_started_at = datetime.utcnow()
    step = PlanAgentStep(
        run_id=run.id,
        plan_id=plan.id,
        generation=run.generation,
        step_type="planner",
        provider="codex",
        status="running",
    )
    db_session.add(step)
    await db_session.flush()
    from backend.services.plan_runtime_receipt import (
        new_prepared_runtime_receipt,
    )

    receipt = new_prepared_runtime_receipt(step, attempt_index=1)
    receipt.status = "cleaned"
    receipt.cleaned_at = datetime.utcnow()
    db_session.add(receipt)
    await db_session.commit()
    run_id = run.id
    plan_id = plan.id
    execution_id = execution.id
    owner_id = owner.id
    step_id = step.id

    with pytest.raises(
        PlanCapabilityCancellationUnconfirmed,
        match="stop was not confirmed",
    ):
        await PlanCapabilityExecutor(
            stop_callback=AsyncMock(return_value=False)
        ).cancel(db_session, invocation_id=invocation_id)

    dispatcher = GlobalDispatcher(
        db_factory=db_factory,
        instance_manager=MagicMock(),
        broadcaster=MagicMock(),
    )
    await dispatcher._recover_versioned_plan_runs()

    # Model a new process/session rather than relying on the first session's
    # SQLAlchemy identity map after recovery committed through db_factory.
    db_session.expire_all()
    recovered = await PlanCapabilityExecutor(
        stop_callback=dispatcher.stop_capability_plan_run_lifecycle
    ).recover(db_session, invocation_id=invocation_id)

    assert recovered.status == "cancelled"
    assert recovered.run_status == "cancelled"
    async with db_factory() as db:
        stored_invocation = await db.get(CapabilityInvocation, invocation_id)
        stored_execution = await db.get(CapabilityExecution, execution_id)
        stored_run = await db.get(PlanAgentRun, run_id)
        stored_plan = await db.get(Plan, plan_id)
        stored_owner = await db.get(Instance, owner_id)
        stored_step = await db.get(PlanAgentStep, step_id)
        assert stored_invocation.status == "cancelled"
        assert stored_execution.status == "cancelled"
        assert stored_run.status == "cancelled"
        assert stored_run.instance_id is None
        assert stored_run.last_execution_started_at is None
        assert stored_plan.active_run_id is None
        assert stored_owner.status == "idle"
        assert stored_owner.current_plan_run_id is None
        assert stored_step.status == "cancelled"
        assert stored_step.error == "Cancelled by user"
        assert stored_step.finished_at is not None


@pytest.mark.asyncio
async def test_cancel_checks_every_instance_reverse_owner(db_session):
    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    await PlanCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation_id,
    )
    _execution_row, run, _plan = await _handled_run(db_session, invocation_id)
    owner = Instance(
        name="late-owner",
        status="running",
        pid=999,
        current_plan_run_id=run.id,
    )
    db_session.add(owner)
    await db_session.commit()

    with pytest.raises(
        PlanCapabilityCancellationUnconfirmed,
        match="still owns live Instance",
    ):
        await PlanCapabilityExecutor().cancel(
            db_session,
            invocation_id=invocation_id,
        )
    current = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    assert current.status == "cancelling"


@pytest.mark.asyncio
async def test_capability_cancel_fence_rejects_waiting_input_answer(db_session):
    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    executor = PlanCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation_id)
    _execution_row, run, plan = await _handled_run(db_session, invocation_id)
    input_request = PlanInputRequest(
        plan_id=plan.id,
        run_id=run.id,
        source_step_id=1,
        requested_by="planner",
        questions=[],
        status="open",
        idempotency_key=f"cap-input-{run.id}",
        opened_at=datetime.utcnow(),
    )
    db_session.add(input_request)
    await db_session.flush()
    run.status = "waiting_user"
    run.open_input_request_id = input_request.id
    await db_session.commit()
    waiting = await executor.observe(db_session, invocation_id=invocation_id)
    assert waiting.status == "waiting_user"

    capability = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    await capability_service.cancel_invocation(
        db_session,
        invocation_id=invocation_id,
        expected_state_version=capability.state_version,
    )
    async with plan_operation_lock(plan.id):
        with pytest.raises(Exception) as raised:
            await answer_input_request(
                db_session,
                plan=plan,
                run=run,
                input_request=input_request,
                expected_generation=run.generation,
                idempotency_key="answer-after-cancel",
                answers=[],
                response_text=None,
                attachments=None,
                answered_by=7,
            )
    assert getattr(raised.value, "status_code", None) == 409


@pytest.mark.asyncio
async def test_capability_answer_writer_order_is_core_run_input(
    db_session,
    monkeypatch,
):
    _task, invocation = await _create_invocation(db_session)
    executor = PlanCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    _execution, run, plan = await _handled_run(db_session, invocation.id)
    input_request = PlanInputRequest(
        plan_id=plan.id,
        run_id=run.id,
        source_step_id=1,
        requested_by="planner",
        questions=[],
        status="open",
        idempotency_key=f"cap-answer-order:{run.id}",
        opened_at=datetime.utcnow(),
    )
    db_session.add(input_request)
    await db_session.flush()
    run.status = "waiting_user"
    run.open_input_request_id = input_request.id
    await db_session.commit()
    waiting = await executor.observe(db_session, invocation_id=invocation.id)
    assert waiting.status == "waiting_user"
    generation = run.generation

    original_execute = AsyncSession.execute
    update_order: list[str] = []

    async def traced_execute(self, statement, *args, **kwargs):
        table = getattr(statement, "table", None)
        if getattr(statement, "is_update", False) and table is not None:
            update_order.append(table.name)
        return await original_execute(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", traced_execute)
    async with plan_operation_lock(plan.id):
        answered = await answer_input_request(
            db_session,
            plan=plan,
            run=run,
            input_request=input_request,
            expected_generation=generation,
            idempotency_key="capability-answer-order",
            answers=[],
            response_text=None,
            attachments=None,
            answered_by=7,
        )

    assert answered.status == "answered"
    assert update_order[:4] == [
        "capability_invocations",
        "capability_executions",
        "plan_agent_runs",
        "plan_input_requests",
    ]


@pytest.mark.asyncio
async def test_wrong_plan_handle_generation_cannot_consume_input_request(db_session):
    _task, invocation = await _create_invocation(db_session)
    invocation_id = invocation.id
    executor = PlanCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation_id)
    execution, run, plan = await _handled_run(db_session, invocation_id)
    execution_id = execution.id
    run_id = run.id
    plan_id = plan.id
    input_request = PlanInputRequest(
        plan_id=plan_id,
        run_id=run_id,
        source_step_id=1,
        requested_by="planner",
        questions=[],
        status="open",
        idempotency_key=f"wrong-generation-input-{run_id}",
        opened_at=datetime.utcnow(),
    )
    db_session.add(input_request)
    await db_session.flush()
    request_id = input_request.id
    run.status = "waiting_user"
    run.open_input_request_id = request_id
    await db_session.commit()
    waiting = await executor.observe(db_session, invocation_id=invocation_id)
    assert waiting.status == "waiting_user"

    execution = await db_session.get(
        CapabilityExecution,
        execution.id,
        populate_existing=True,
    )
    execution.handle_generation = 7
    await db_session.commit()

    async with plan_operation_lock(plan_id):
        with pytest.raises(Exception) as raised:
            await answer_input_request(
                db_session,
                plan=plan,
                run=run,
                input_request=input_request,
                expected_generation=run.generation,
                idempotency_key="wrong-handle-generation-answer",
                answers=[],
                response_text=None,
                attachments=None,
                answered_by=7,
            )
    assert getattr(raised.value, "status_code", None) == 409
    await db_session.rollback()
    stored_request = await db_session.get(
        PlanInputRequest,
        request_id,
        populate_existing=True,
    )
    stored_run = await db_session.get(
        PlanAgentRun,
        run_id,
        populate_existing=True,
    )
    stored_invocation = await db_session.get(
        CapabilityInvocation,
        invocation_id,
        populate_existing=True,
    )
    stored_execution = await db_session.get(
        CapabilityExecution,
        execution_id,
        populate_existing=True,
    )
    assert stored_request.status == "open"
    assert stored_request.answers is None
    assert stored_request.answered_at is None
    assert stored_run.status == "waiting_user"
    assert stored_run.open_input_request_id == request_id
    assert stored_invocation.status == "waiting_user"
    assert stored_execution.status == "waiting_user"
