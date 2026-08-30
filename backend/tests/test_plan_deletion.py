"""Core safety checks for transactional target-Plan deletion."""

from datetime import datetime

import pytest
from sqlalchemy import text

import backend.services.plan_deletion as plan_deletion_module
from backend.models.capability import (
    CapabilityExecution,
    CapabilityInvocation,
    CapabilityResumeOutbox,
)
from backend.models.plan import (
    Plan,
    PlanApplicationReceipt,
    PlanInputRequest,
    PlanVersion,
)
from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentRuntimeReceipt,
    PlanAgentStep,
    PlanAgentWorkerDispatchReceipt,
)
from backend.models.task import Task
from backend.services.plan_deletion import (
    PlanDeletionConflict,
    delete_target_plan_graph,
    lock_target_plan_delete_graph,
)
from backend.services.plan_capability import plan_version_output_hash
from backend.services.plan_runtime_receipt import new_prepared_runtime_receipt


async def _target(db, name: str, *, worker_id: int | None = None) -> Task:
    task = Task(
        title=name,
        description="delete safely",
        status="completed",
        worker_id=worker_id,
    )
    db.add(task)
    await db.flush()
    return task


async def _terminal_worker_mirror(
    db,
    name: str,
    *,
    status: str,
    generation: int,
    cancellation_target_generation: int | None = None,
) -> tuple[Task, Plan, PlanAgentRun]:
    task = await _target(db, name, worker_id=9)
    plan = Plan(
        title=name,
        initial_request="plan",
        target_task_id=task.id,
        worker_id=9,
        pipeline_config={},
    )
    db.add(plan)
    await db.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        worker_id=9,
        run_type="initial",
        status=status,
        current_stage="complete" if status == "completed" else status,
        generation=generation,
        cancellation_target_generation=cancellation_target_generation,
        finished_at=datetime.utcnow(),
    )
    db.add(run)
    await db.flush()
    return task, plan, run


@pytest.mark.asyncio
async def test_empty_target_plan_graph_returns_none(db_session):
    task = await _target(db_session, "no first-class Plan")

    graph = await lock_target_plan_delete_graph(
        db_session,
        task.id,
        capability_invocation_ids=set(),
        capability_execution_ids=set(),
        capability_outbox_ids=set(),
    )

    assert graph is None


@pytest.mark.asyncio
async def test_terminal_target_plan_graph_is_explicitly_deleted(db_session):
    task = await _target(db_session, "terminal first-class Plan")
    plan = Plan(
        title="terminal",
        initial_request="plan",
        target_task_id=task.id,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        run_type="initial",
        status="failed",
        current_stage="failed",
        finished_at=datetime.utcnow(),
    )
    db_session.add(run)
    await db_session.flush()
    ids = task.id, plan.id, run.id

    graph = await lock_target_plan_delete_graph(
        db_session,
        task.id,
        capability_invocation_ids=set(),
        capability_execution_ids=set(),
        capability_outbox_ids=set(),
    )
    assert graph is not None
    assert graph.plan_ids == (plan.id,)
    assert graph.run_ids == (run.id,)

    await delete_target_plan_graph(db_session, graph)
    await db_session.flush()

    assert await db_session.get(Task, ids[0]) is not None
    assert await db_session.get(Plan, ids[1]) is None
    assert await db_session.get(PlanAgentRun, ids[2]) is None


@pytest.mark.asyncio
async def test_plan_deletion_locks_run_plan_receipts_children_then_input(
    db_session,
    monkeypatch,
):
    task = await _target(db_session, "delete lock order")
    plan = Plan(
        title="delete lock order",
        initial_request="plan",
        target_task_id=task.id,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        run_type="initial",
        status="failed",
        current_stage="failed",
        generation=2,
        finished_at=datetime.utcnow(),
    )
    db_session.add(run)
    await db_session.flush()
    step = PlanAgentStep(
        run_id=run.id,
        plan_id=plan.id,
        generation=run.generation,
        step_type="planner",
        round=1,
        provider="codex",
        status="failed",
        finished_at=datetime.utcnow(),
    )
    db_session.add(step)
    await db_session.flush()
    input_request = PlanInputRequest(
        plan_id=plan.id,
        run_id=run.id,
        source_step_id=step.id,
        requested_by="planner",
        questions=[],
        status="cancelled",
        idempotency_key=f"delete-lock-order:{run.id}",
        opened_at=datetime.utcnow(),
        cancelled_at=datetime.utcnow(),
    )
    db_session.add(input_request)
    await db_session.flush()
    step.input_request_id = input_request.id
    run.interaction_count = 1
    runtime_receipt = new_prepared_runtime_receipt(step, attempt_index=1)
    runtime_receipt.status = "cleaned"
    runtime_receipt.cleaned_at = datetime.utcnow()
    db_session.add(runtime_receipt)
    await db_session.commit()

    original_lock_rows = plan_deletion_module._lock_rows
    lock_order: list[type] = []

    async def traced_lock_rows(db, model, *predicates):
        lock_order.append(model)
        return await original_lock_rows(db, model, *predicates)

    monkeypatch.setattr(plan_deletion_module, "_lock_rows", traced_lock_rows)
    graph = await lock_target_plan_delete_graph(
        db_session,
        task.id,
        capability_invocation_ids=set(),
        capability_execution_ids=set(),
        capability_outbox_ids=set(),
    )

    assert graph is not None
    assert lock_order[:6] == [
        PlanAgentRun,
        Plan,
        PlanAgentWorkerDispatchReceipt,
        PlanAgentStep,
        PlanAgentRuntimeReceipt,
        PlanInputRequest,
    ]


@pytest.mark.asyncio
async def test_active_target_plan_run_fails_closed(db_session):
    task = await _target(db_session, "active first-class Plan")
    plan = Plan(
        title="active",
        initial_request="plan",
        target_task_id=task.id,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        run_type="initial",
        status="queued",
        current_stage="planner",
    )
    db_session.add(run)
    await db_session.flush()
    plan.active_run_id = run.id
    await db_session.flush()

    with pytest.raises(PlanDeletionConflict) as caught:
        await lock_target_plan_delete_graph(
            db_session,
            task.id,
            capability_invocation_ids=set(),
            capability_execution_ids=set(),
            capability_outbox_ids=set(),
        )

    assert caught.value.code == "active_plan_run"
    assert await db_session.get(Plan, plan.id) is not None


@pytest.mark.asyncio
async def test_external_version_reference_fails_closed(db_session):
    task = await _target(db_session, "externally referenced Plan")
    plan = Plan(
        title="owned",
        initial_request="plan",
        target_task_id=task.id,
        pipeline_config={},
    )
    external = Plan(
        title="external",
        initial_request="keep",
        pipeline_config={},
    )
    db_session.add_all([plan, external])
    await db_session.flush()
    version = PlanVersion(
        plan_id=plan.id,
        version_number=1,
        content="owned version",
    )
    db_session.add(version)
    await db_session.flush()
    plan.current_version_id = version.id
    external.forked_from_version_id = version.id
    await db_session.flush()

    with pytest.raises(PlanDeletionConflict) as caught:
        await lock_target_plan_delete_graph(
            db_session,
            task.id,
            capability_invocation_ids=set(),
            capability_execution_ids=set(),
            capability_outbox_ids=set(),
        )

    assert caught.value.code == "external_plan_reference"


@pytest.mark.asyncio
async def test_terminal_capability_plan_requires_exact_bidirectional_core(db_session):
    task = await _target(db_session, "Capability-owned Plan")
    digest = "a" * 64
    invocation = CapabilityInvocation(
        task_id=task.id,
        capability_key="plan",
        source="human_request",
        purpose="advisory",
        status="queued",
        state_version=1,
        idempotency_key="delete-capability-plan",
        input_payload={},
        input_hash=digest,
        subject_kind="task_generation",
        subject_ref={"task_id": task.id},
        subject_hash=digest,
        executor_kind="plan_agent",
        executor_config={},
        executor_config_hash=digest,
        policy_snapshot={},
        policy_hash=digest,
        resume_policy="attach_only",
        max_attempts=1,
        active_task_id=task.id,
    )
    db_session.add(invocation)
    await db_session.flush()
    execution = CapabilityExecution(
        invocation_id=invocation.id,
        attempt=1,
        status="queued",
        state_version=1,
        active_invocation_id=invocation.id,
        idempotency_key="delete-capability-plan:1",
        executor_kind="plan_agent",
        input_hash=digest,
    )
    db_session.add(execution)
    await db_session.flush()
    plan = Plan(
        title="capability",
        initial_request="plan",
        target_task_id=task.id,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        capability_execution_id=execution.id,
        run_type="capability",
        status="completed",
        current_stage="complete",
        finished_at=datetime.utcnow(),
    )
    db_session.add(run)
    await db_session.flush()
    version = PlanVersion(
        plan_id=plan.id,
        version_number=1,
        produced_by_run_id=run.id,
        content="exact output",
    )
    db_session.add(version)
    await db_session.flush()
    result_hash = plan_version_output_hash(version)
    run.result_version_id = version.id
    plan.current_version_id = version.id
    execution.handle_kind = "plan_agent_run"
    execution.handle_id = str(run.id)
    execution.handle_generation = 0
    execution.output_kind = "plan_version"
    execution.output_id = version.id
    execution.output_hash = result_hash
    invocation.result_kind = "plan_version"
    invocation.result_id = version.id
    invocation.result_hash = result_hash
    invocation.status = "completed"
    invocation.state_version = 3
    invocation.active_task_id = None
    invocation.ready_at = datetime.utcnow()
    invocation.completed_at = datetime.utcnow()
    execution.status = "completed"
    execution.state_version = 2
    execution.active_invocation_id = None
    execution.completed_at = datetime.utcnow()
    await db_session.flush()

    graph = await lock_target_plan_delete_graph(
        db_session,
        task.id,
        capability_invocation_ids={invocation.id},
        capability_execution_ids={execution.id},
        capability_outbox_ids=set(),
    )

    assert graph is not None
    assert graph.capability_invocation_ids == (invocation.id,)
    assert graph.capability_execution_ids == (execution.id,)


@pytest.mark.asyncio
async def test_corrupt_internal_resume_outbox_plan_result_fails_closed(db_session):
    task = await _target(db_session, "corrupt Plan resume outbox")
    plan = Plan(
        title="outbox",
        initial_request="plan",
        target_task_id=task.id,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    version = PlanVersion(
        plan_id=plan.id,
        version_number=1,
        content="owned result",
    )
    db_session.add(version)
    await db_session.flush()
    plan.current_version_id = version.id
    digest = "b" * 64
    invocation = CapabilityInvocation(
        task_id=task.id,
        capability_key="plan",
        source="human_request",
        purpose="advisory",
        status="failed",
        state_version=2,
        idempotency_key="delete-corrupt-plan-outbox",
        input_payload={},
        input_hash=digest,
        subject_kind="task_generation",
        subject_ref={"task_id": task.id},
        subject_hash=digest,
        executor_kind="plan_agent",
        executor_config={},
        executor_config_hash=digest,
        policy_snapshot={},
        policy_hash=digest,
        resume_policy="attach_only",
        max_attempts=1,
        active_task_id=None,
        error_code="planner_failed",
        error_message="planner failed",
        completed_at=datetime.utcnow(),
    )
    db_session.add(invocation)
    await db_session.flush()
    now = datetime.utcnow()
    outbox = CapabilityResumeOutbox(
        task_id=task.id,
        invocation_id=invocation.id,
        active_task_id=None,
        active_invocation_id=None,
        status="cancelled",
        state_version=2,
        request_task_incarnation_id=task.incarnation_id,
        request_task_retry_count=task.retry_count,
        from_turn_generation=task.turn_generation,
        request_source_log_id=1,
        request_output_log_id=2,
        request_terminal_log_id=3,
        invocation_terminal_status="completed",
        invocation_result_kind="plan_version",
        invocation_result_id=version.id,
        invocation_result_hash=digest,
        error_code="resume_cancelled",
        error_message="resume was cancelled",
        created_at=now,
        updated_at=now,
        completed_at=now,
    )
    db_session.add(outbox)
    await db_session.flush()

    with pytest.raises(PlanDeletionConflict) as caught:
        await lock_target_plan_delete_graph(
            db_session,
            task.id,
            capability_invocation_ids={invocation.id},
            capability_execution_ids=set(),
            capability_outbox_ids={outbox.id},
        )

    assert caught.value.code == "invalid_capability_resume_result"


@pytest.mark.asyncio
@pytest.mark.parametrize("dispatch_status", ["prepared", "remote_possible"])
async def test_active_worker_dispatch_receipt_fails_closed(
    db_session,
    dispatch_status,
):
    task = await _target(
        db_session,
        f"{dispatch_status} Worker Plan dispatch",
        worker_id=9,
    )
    plan = Plan(
        title="worker dispatch",
        initial_request="plan",
        target_task_id=task.id,
        worker_id=9,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        worker_id=9,
        run_type="initial",
        status="failed",
        current_stage="failed",
        generation=3,
        finished_at=datetime.utcnow(),
    )
    db_session.add(run)
    await db_session.flush()
    receipt = PlanAgentWorkerDispatchReceipt(
        plan_id=plan.id,
        run_id=run.id,
        target_task_id=task.id,
        worker_id=9,
        run_generation=run.generation,
        protocol=1,
        status=dispatch_status,
        payload_digest=("c" * 64 if dispatch_status == "remote_possible" else None),
    )
    db_session.add(receipt)
    await db_session.flush()

    with pytest.raises(PlanDeletionConflict) as caught:
        await lock_target_plan_delete_graph(
            db_session,
            task.id,
            capability_invocation_ids=set(),
            capability_execution_ids=set(),
            capability_outbox_ids=set(),
        )

    assert caught.value.code == "active_worker_plan_dispatch"


@pytest.mark.asyncio
async def test_settled_worker_dispatch_receipt_is_deleted_with_plan_graph(
    db_session,
):
    task = await _target(
        db_session,
        "settled Worker Plan dispatch",
        worker_id=9,
    )
    plan = Plan(
        title="worker dispatch",
        initial_request="plan",
        target_task_id=task.id,
        worker_id=9,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        worker_id=9,
        run_type="initial",
        status="failed",
        current_stage="failed",
        generation=0,
        finished_at=datetime.utcnow(),
    )
    db_session.add(run)
    await db_session.flush()
    receipt = PlanAgentWorkerDispatchReceipt(
        plan_id=plan.id,
        run_id=run.id,
        target_task_id=task.id,
        worker_id=9,
        run_generation=run.generation,
        protocol=1,
        status="settled",
        payload_digest="c" * 64,
        remote_status="failed",
        settlement_reason="remote_pause",
        settled_at=datetime.utcnow(),
    )
    db_session.add(receipt)
    await db_session.flush()
    receipt_id = receipt.id

    graph = await lock_target_plan_delete_graph(
        db_session,
        task.id,
        capability_invocation_ids=set(),
        capability_execution_ids=set(),
        capability_outbox_ids=set(),
    )

    assert graph is not None
    assert graph.worker_dispatch_receipt_ids == (receipt_id,)
    await delete_target_plan_graph(db_session, graph)
    await db_session.flush()
    assert await db_session.get(PlanAgentWorkerDispatchReceipt, receipt_id) is None


@pytest.mark.asyncio
async def test_historical_settled_worker_dispatch_and_mirror_step_are_deletable(
    db_session,
):
    """G not_launched -> G+1 completed is valid retained audit history."""

    task = await _target(
        db_session,
        "recovered Worker Plan dispatch",
        worker_id=9,
    )
    plan = Plan(
        title="worker dispatch history",
        initial_request="plan",
        target_task_id=task.id,
        worker_id=9,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        worker_id=9,
        run_type="initial",
        status="completed",
        current_stage="complete",
        generation=1,
        finished_at=datetime.utcnow(),
    )
    db_session.add(run)
    await db_session.flush()
    step = PlanAgentStep(
        run_id=run.id,
        plan_id=plan.id,
        worker_id=9,
        worker_step_id=701,
        # Worker-local generation is intentionally independent from the
        # Manager claim generation carried by dispatch receipts.
        generation=2,
        step_type="planner",
        round=1,
        provider="claude",
        status="completed",
        finished_at=datetime.utcnow(),
    )
    db_session.add(step)
    await db_session.flush()
    version = PlanVersion(
        plan_id=plan.id,
        worker_id=9,
        worker_version_id=801,
        version_number=1,
        produced_by_run_id=run.id,
        produced_by_step_id=step.id,
        content="remote completed plan",
    )
    db_session.add(version)
    await db_session.flush()
    step.plan_version_id = version.id
    run.result_version_id = version.id
    run.draft_step_id = step.id
    run.draft_content = version.content
    plan.current_version_id = version.id
    historical = PlanAgentWorkerDispatchReceipt(
        plan_id=plan.id,
        run_id=run.id,
        target_task_id=task.id,
        worker_id=9,
        run_generation=0,
        protocol=1,
        status="settled",
        settlement_reason="not_launched",
        settled_at=datetime.utcnow(),
    )
    completed = PlanAgentWorkerDispatchReceipt(
        plan_id=plan.id,
        run_id=run.id,
        target_task_id=task.id,
        worker_id=9,
        run_generation=1,
        protocol=1,
        status="settled",
        payload_digest="d" * 64,
        remote_status="completed",
        settlement_reason="remote_pause",
        settled_at=datetime.utcnow(),
    )
    db_session.add_all([historical, completed])
    await db_session.flush()

    graph = await lock_target_plan_delete_graph(
        db_session,
        task.id,
        capability_invocation_ids=set(),
        capability_execution_ids=set(),
        capability_outbox_ids=set(),
    )

    assert graph is not None
    assert graph.step_ids == (step.id,)
    assert graph.version_ids == (version.id,)
    assert graph.runtime_receipt_ids == ()
    assert graph.worker_dispatch_receipt_ids == (
        historical.id,
        completed.id,
    )


@pytest.mark.asyncio
async def test_worker_mirror_step_rejects_manager_runtime_receipt(db_session):
    task = await _target(
        db_session,
        "forged Worker runtime receipt",
        worker_id=9,
    )
    plan = Plan(
        title="worker mirror",
        initial_request="plan",
        target_task_id=task.id,
        worker_id=9,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        worker_id=9,
        run_type="initial",
        status="failed",
        current_stage="failed",
        generation=0,
        finished_at=datetime.utcnow(),
    )
    db_session.add(run)
    await db_session.flush()
    step = PlanAgentStep(
        run_id=run.id,
        plan_id=plan.id,
        worker_id=9,
        worker_step_id=702,
        generation=run.generation,
        step_type="planner",
        round=1,
        provider="claude",
        status="failed",
        finished_at=datetime.utcnow(),
    )
    db_session.add(step)
    await db_session.flush()
    runtime_receipt = new_prepared_runtime_receipt(step, attempt_index=1)
    runtime_receipt.status = "cleaned"
    runtime_receipt.cleaned_at = datetime.utcnow()
    dispatch_receipt = PlanAgentWorkerDispatchReceipt(
        plan_id=plan.id,
        run_id=run.id,
        target_task_id=task.id,
        worker_id=9,
        run_generation=run.generation,
        protocol=1,
        status="settled",
        payload_digest="e" * 64,
        remote_status="failed",
        settlement_reason="remote_pause",
        settled_at=datetime.utcnow(),
    )
    db_session.add_all([runtime_receipt, dispatch_receipt])
    await db_session.flush()

    with pytest.raises(PlanDeletionConflict) as caught:
        await lock_target_plan_delete_graph(
            db_session,
            task.id,
            capability_invocation_ids=set(),
            capability_execution_ids=set(),
            capability_outbox_ids=set(),
        )

    assert caught.value.code == "unclean_plan_runtime"


@pytest.mark.asyncio
async def test_worker_mirror_without_dispatch_receipt_fails_closed(db_session):
    task = await _target(
        db_session,
        "legacy Worker mirror without dispatch proof",
        worker_id=9,
    )
    plan = Plan(
        title="worker mirror without receipt",
        initial_request="plan",
        target_task_id=task.id,
        worker_id=9,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    db_session.add(
        PlanAgentRun(
            plan_id=plan.id,
            worker_id=9,
            run_type="initial",
            status="failed",
            current_stage="failed",
            generation=0,
            finished_at=datetime.utcnow(),
        )
    )
    await db_session.flush()

    with pytest.raises(PlanDeletionConflict) as caught:
        await lock_target_plan_delete_graph(
            db_session,
            task.id,
            capability_invocation_ids=set(),
            capability_execution_ids=set(),
            capability_outbox_ids=set(),
        )

    assert caught.value.code == "unclean_plan_runtime"


@pytest.mark.asyncio
async def test_worker_mirror_allows_distinct_worker_step_generation(db_session):
    task = await _target(
        db_session,
        "Worker mirror with independent Step generation",
        worker_id=9,
    )
    plan = Plan(
        title="worker mirror independent generation",
        initial_request="plan",
        target_task_id=task.id,
        worker_id=9,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        worker_id=9,
        run_type="initial",
        status="failed",
        current_stage="failed",
        generation=0,
        finished_at=datetime.utcnow(),
    )
    db_session.add(run)
    await db_session.flush()
    db_session.add_all(
        [
            PlanAgentStep(
                run_id=run.id,
                plan_id=plan.id,
                worker_id=9,
                worker_step_id=703,
                generation=7,
                step_type="planner",
                round=1,
                provider="claude",
                status="failed",
                finished_at=datetime.utcnow(),
            ),
            PlanAgentWorkerDispatchReceipt(
                plan_id=plan.id,
                run_id=run.id,
                target_task_id=task.id,
                worker_id=9,
                run_generation=0,
                protocol=1,
                status="settled",
                payload_digest="f" * 64,
                remote_status="failed",
                settlement_reason="remote_pause",
                settled_at=datetime.utcnow(),
            ),
        ]
    )
    await db_session.flush()

    graph = await lock_target_plan_delete_graph(
        db_session,
        task.id,
        capability_invocation_ids=set(),
        capability_execution_ids=set(),
        capability_outbox_ids=set(),
    )

    assert graph is not None
    assert graph.step_ids


@pytest.mark.asyncio
async def test_worker_mirror_step_requires_a_remote_import_boundary(db_session):
    task = await _target(db_session, "Worker mirror with no import boundary", worker_id=9)
    plan = Plan(
        title="worker mirror missing import",
        initial_request="plan",
        target_task_id=task.id,
        worker_id=9,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        worker_id=9,
        run_type="initial",
        status="failed",
        current_stage="failed",
        generation=0,
        finished_at=datetime.utcnow(),
    )
    db_session.add(run)
    await db_session.flush()
    db_session.add_all(
        [
            PlanAgentStep(
                run_id=run.id,
                plan_id=plan.id,
                worker_id=9,
                worker_step_id=704,
                generation=8,
                step_type="planner",
                round=1,
                provider="claude",
                status="failed",
                finished_at=datetime.utcnow(),
            ),
            PlanAgentWorkerDispatchReceipt(
                plan_id=plan.id,
                run_id=run.id,
                target_task_id=task.id,
                worker_id=9,
                run_generation=0,
                protocol=1,
                status="settled",
                settlement_reason="preflight_failed",
                settled_at=datetime.utcnow(),
            ),
        ]
    )
    await db_session.flush()

    with pytest.raises(PlanDeletionConflict) as caught:
        await lock_target_plan_delete_graph(
            db_session,
            task.id,
            capability_invocation_ids=set(),
            capability_execution_ids=set(),
            capability_outbox_ids=set(),
        )

    assert caught.value.code == "unclean_plan_runtime"


@pytest.mark.asyncio
async def test_worker_cancel_before_first_dispatch_uses_exact_ack_generation(db_session):
    task, _plan, run = await _terminal_worker_mirror(
        db_session,
        "Worker cancel before first dispatch",
        status="cancelled",
        generation=1,
        cancellation_target_generation=0,
    )

    graph = await lock_target_plan_delete_graph(
        db_session,
        task.id,
        capability_invocation_ids=set(),
        capability_execution_ids=set(),
        capability_outbox_ids=set(),
    )

    assert graph is not None
    assert graph.run_ids == (run.id,)
    assert graph.worker_dispatch_receipt_ids == ()


@pytest.mark.asyncio
async def test_worker_cancel_accepts_answered_generation_without_dispatch_receipt(
    db_session,
):
    task, plan, run = await _terminal_worker_mirror(
        db_session,
        "Worker cancel after answer",
        status="cancelled",
        generation=2,
        cancellation_target_generation=1,
    )
    waiting = PlanAgentWorkerDispatchReceipt(
        plan_id=plan.id,
        run_id=run.id,
        target_task_id=task.id,
        worker_id=9,
        run_generation=0,
        protocol=1,
        status="settled",
        payload_digest="a" * 64,
        remote_status="waiting_user",
        settlement_reason="remote_pause",
        settled_at=datetime.utcnow(),
    )
    db_session.add(waiting)
    await db_session.flush()
    step = PlanAgentStep(
        run_id=run.id,
        plan_id=plan.id,
        worker_id=9,
        worker_step_id=702,
        generation=0,
        step_type="planner",
        round=1,
        provider="claude",
        status="completed",
        finished_at=datetime.utcnow(),
    )
    db_session.add(step)
    await db_session.flush()
    input_request = PlanInputRequest(
        plan_id=plan.id,
        run_id=run.id,
        worker_id=9,
        worker_input_request_id=703,
        source_step_id=step.id,
        requested_by="planner",
        questions=[],
        status="answered",
        idempotency_key="worker:9:input:703",
        opened_at=datetime.utcnow(),
        answered_at=datetime.utcnow(),
    )
    db_session.add(input_request)
    await db_session.flush()
    step.input_request_id = input_request.id

    run.interaction_count = 1

    graph = await lock_target_plan_delete_graph(
        db_session,
        task.id,
        capability_invocation_ids=set(),
        capability_execution_ids=set(),
        capability_outbox_ids=set(),
    )

    assert graph is not None
    assert graph.worker_dispatch_receipt_ids == (waiting.id,)
    assert graph.step_ids == (step.id,)
    assert graph.input_request_ids == (input_request.id,)


@pytest.mark.asyncio
async def test_worker_cancel_without_ack_marker_or_current_terminal_fails_closed(
    db_session,
):
    task, plan, run = await _terminal_worker_mirror(
        db_session,
        "Worker cancel missing ACK marker",
        status="cancelled",
        generation=2,
    )
    db_session.add(
        PlanAgentWorkerDispatchReceipt(
            plan_id=plan.id,
            run_id=run.id,
            target_task_id=task.id,
            worker_id=9,
            run_generation=0,
            protocol=1,
            status="settled",
            payload_digest="b" * 64,
            remote_status="waiting_user",
            settlement_reason="remote_pause",
            settled_at=datetime.utcnow(),
        )
    )
    await db_session.flush()

    with pytest.raises(PlanDeletionConflict) as caught:
        await lock_target_plan_delete_graph(
            db_session,
            task.id,
            capability_invocation_ids=set(),
            capability_execution_ids=set(),
            capability_outbox_ids=set(),
        )

    assert caught.value.code == "unclean_plan_runtime"


@pytest.mark.asyncio
async def test_worker_remote_terminal_cancel_is_deletable_without_ack_marker(db_session):
    task, plan, run = await _terminal_worker_mirror(
        db_session,
        "Worker remote terminal cancel",
        status="cancelled",
        generation=0,
    )
    receipt = PlanAgentWorkerDispatchReceipt(
        plan_id=plan.id,
        run_id=run.id,
        target_task_id=task.id,
        worker_id=9,
        run_generation=0,
        protocol=1,
        status="settled",
        payload_digest="c" * 64,
        remote_status="cancelled",
        settlement_reason="remote_pause",
        settled_at=datetime.utcnow(),
    )
    db_session.add(receipt)
    await db_session.flush()

    graph = await lock_target_plan_delete_graph(
        db_session,
        task.id,
        capability_invocation_ids=set(),
        capability_execution_ids=set(),
        capability_outbox_ids=set(),
    )

    assert graph is not None
    assert graph.worker_dispatch_receipt_ids == (receipt.id,)


@pytest.mark.asyncio
async def test_migrated_legacy_worker_terminal_is_an_explicit_deletion_proof(db_session):
    task, plan, run = await _terminal_worker_mirror(
        db_session,
        "Migrated legacy Worker terminal",
        status="completed",
        generation=4,
    )
    step = PlanAgentStep(
        run_id=run.id,
        plan_id=plan.id,
        worker_id=9,
        worker_step_id=705,
        generation=11,
        step_type="planner",
        round=1,
        provider="claude",
        status="completed",
        finished_at=datetime.utcnow(),
    )
    receipt = PlanAgentWorkerDispatchReceipt(
        plan_id=plan.id,
        run_id=run.id,
        target_task_id=task.id,
        worker_id=9,
        run_generation=run.generation,
        protocol=1,
        status="settled",
        remote_status="completed",
        settlement_reason="legacy_terminal",
        settled_at=datetime.utcnow(),
    )
    db_session.add_all([step, receipt])
    await db_session.flush()

    graph = await lock_target_plan_delete_graph(
        db_session,
        task.id,
        capability_invocation_ids=set(),
        capability_execution_ids=set(),
        capability_outbox_ids=set(),
    )

    assert graph is not None
    assert graph.step_ids == (step.id,)
    assert graph.worker_dispatch_receipt_ids == (receipt.id,)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload_digest", "remote_status", "settlement_reason"),
    [
        ("C" * 64, "failed", "remote_pause"),
        ("c" * 64, "failed", "remote_absent"),
        ("c" * 64, "failed", "unknown_reason"),
    ],
)
async def test_malformed_settled_worker_dispatch_receipt_fails_closed(
    db_session,
    payload_digest,
    remote_status,
    settlement_reason,
):
    task = await _target(
        db_session,
        "malformed settled Worker Plan dispatch",
        worker_id=9,
    )
    plan = Plan(
        title="worker dispatch",
        initial_request="plan",
        target_task_id=task.id,
        worker_id=9,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        worker_id=9,
        run_type="initial",
        status="failed",
        current_stage="failed",
        generation=3,
        finished_at=datetime.utcnow(),
    )
    db_session.add(run)
    await db_session.flush()
    receipt = PlanAgentWorkerDispatchReceipt(
        plan_id=plan.id,
        run_id=run.id,
        target_task_id=task.id,
        worker_id=9,
        run_generation=run.generation,
        protocol=1,
        status="settled",
        payload_digest=payload_digest,
        remote_status=remote_status,
        settlement_reason=settlement_reason,
        settled_at=datetime.utcnow(),
    )
    # Deliberately materialize a row that could survive a legacy MySQL
    # deployment without enforced CHECK constraints. SQLite's test schema
    # enforces them, so temporarily disable only CHECK evaluation for this
    # connection while inserting the legacy-shaped evidence.
    await db_session.execute(text("PRAGMA ignore_check_constraints = ON"))
    db_session.add(receipt)
    await db_session.flush()
    await db_session.execute(text("PRAGMA ignore_check_constraints = OFF"))

    with pytest.raises(PlanDeletionConflict) as caught:
        await lock_target_plan_delete_graph(
            db_session,
            task.id,
            capability_invocation_ids=set(),
            capability_execution_ids=set(),
            capability_outbox_ids=set(),
        )

    assert caught.value.code == "invalid_worker_plan_dispatch"


@pytest.mark.asyncio
@pytest.mark.parametrize("delivery_status", ["failed", "cancelled"])
async def test_known_prelaunch_receipt_without_application_is_deletable(
    db_session,
    delivery_status,
):
    task = await _target(db_session, f"{delivery_status} Plan delivery")
    plan = Plan(
        title="delivery",
        initial_request="plan",
        target_task_id=task.id,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    version = PlanVersion(
        plan_id=plan.id,
        version_number=1,
        content="not launched",
    )
    db_session.add(version)
    await db_session.flush()
    plan.current_version_id = version.id
    receipt = PlanApplicationReceipt(
        receipt_key=f"prelaunch-{delivery_status}",
        target_task_id=task.id,
        plan_version_ids=[version.id],
        status="prepared",
        delivery_status=delivery_status,
    )
    db_session.add(receipt)
    await db_session.flush()

    graph = await lock_target_plan_delete_graph(
        db_session,
        task.id,
        capability_invocation_ids=set(),
        capability_execution_ids=set(),
        capability_outbox_ids=set(),
    )

    assert graph is not None
    assert graph.application_receipt_ids == (receipt.id,)


@pytest.mark.asyncio
async def test_pending_plan_delivery_fails_closed(db_session):
    task = await _target(db_session, "pending Plan delivery")
    plan = Plan(
        title="delivery",
        initial_request="plan",
        target_task_id=task.id,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    version = PlanVersion(plan_id=plan.id, version_number=1, content="pending")
    db_session.add(version)
    await db_session.flush()
    plan.current_version_id = version.id
    db_session.add(
        PlanApplicationReceipt(
            receipt_key="pending-delivery",
            target_task_id=task.id,
            plan_version_ids=[version.id],
            status="prepared",
            delivery_status="pending",
        )
    )
    await db_session.flush()

    with pytest.raises(PlanDeletionConflict) as caught:
        await lock_target_plan_delete_graph(
            db_session,
            task.id,
            capability_invocation_ids=set(),
            capability_execution_ids=set(),
            capability_outbox_ids=set(),
        )

    assert caught.value.code == "active_or_external_plan_delivery"


@pytest.mark.asyncio
async def test_unclean_runtime_receipt_fails_closed(db_session):
    task = await _target(db_session, "unclean Plan runtime")
    plan = Plan(
        title="runtime",
        initial_request="plan",
        target_task_id=task.id,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        run_type="initial",
        status="failed",
        current_stage="failed",
        generation=1,
        finished_at=datetime.utcnow(),
    )
    db_session.add(run)
    await db_session.flush()
    step = PlanAgentStep(
        run_id=run.id,
        plan_id=plan.id,
        generation=run.generation,
        step_type="planner",
        round=1,
        provider="claude",
        status="failed",
        finished_at=datetime.utcnow(),
    )
    db_session.add(step)
    await db_session.flush()
    receipt = new_prepared_runtime_receipt(step, attempt_index=1)
    receipt.status = "cleanup_failed"
    receipt.cleanup_error = "runtime could not be reaped"
    db_session.add(receipt)
    await db_session.flush()

    with pytest.raises(PlanDeletionConflict) as caught:
        await lock_target_plan_delete_graph(
            db_session,
            task.id,
            capability_invocation_ids=set(),
            capability_execution_ids=set(),
            capability_outbox_ids=set(),
        )

    assert caught.value.code == "unclean_plan_runtime"


@pytest.mark.asyncio
async def test_missing_runtime_receipt_fails_closed(db_session):
    task = await _target(db_session, "missing Plan runtime receipt")
    plan = Plan(
        title="runtime",
        initial_request="plan",
        target_task_id=task.id,
        pipeline_config={},
    )
    db_session.add(plan)
    await db_session.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        run_type="initial",
        status="failed",
        current_stage="failed",
        generation=1,
        finished_at=datetime.utcnow(),
    )
    db_session.add(run)
    await db_session.flush()
    db_session.add(
        PlanAgentStep(
            run_id=run.id,
            plan_id=plan.id,
            generation=run.generation,
            step_type="planner",
            round=1,
            provider="claude",
            status="failed",
            finished_at=datetime.utcnow(),
        )
    )
    await db_session.flush()

    with pytest.raises(PlanDeletionConflict) as caught:
        await lock_target_plan_delete_graph(
            db_session,
            task.id,
            capability_invocation_ids=set(),
            capability_execution_ids=set(),
            capability_outbox_ids=set(),
        )

    assert caught.value.code == "unclean_plan_runtime"
