"""Deletion safety for legacy Plan Tasks with durable runtime receipts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.models.log_entry import LogEntry
from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentRuntimeReceipt,
    PlanAgentStep,
)
from backend.models.task import Task
from backend.services.plan_runtime_receipt import new_prepared_runtime_receipt
from backend.services.task_queue import TaskQueue, task_delete_fence


@dataclass(frozen=True)
class _PlanTaskGraph:
    task_id: int
    run_id: int
    step_id: int
    receipt_id: int
    log_id: int


async def _create_plan_task_graph(
    db,
    *,
    receipt_status: str,
) -> _PlanTaskGraph:
    task = Task(
        title=f"deletable Plan with {receipt_status} receipt",
        description="exercise atomic Plan history deletion",
        mode="plan",
        status="plan_review",
    )
    db.add(task)
    await db.flush()
    run = PlanAgentRun(
        plan_task_id=task.id,
        run_type="legacy",
        status="completed",
        current_stage="reviewer",
        generation=4,
    )
    db.add(run)
    await db.flush()
    step = PlanAgentStep(
        run_id=run.id,
        generation=run.generation,
        step_type="reviewer",
        round=1,
        provider="claude",
        status="completed",
        finished_at=datetime.utcnow(),
    )
    db.add(step)
    await db.flush()
    receipt = new_prepared_runtime_receipt(step, attempt_index=1)
    receipt.status = receipt_status
    if receipt_status == "cleaned":
        receipt.cleaned_at = datetime.utcnow()
    elif receipt_status in {"launching", "cleanup_failed"}:
        receipt.process_id = 8123
        receipt.process_group_id = 8123
        receipt.process_start_ticks = 5_000_000_123
        receipt.process_uid = receipt.prepared_uid
        receipt.boot_id = receipt.prepared_boot_id
    if receipt_status == "cleanup_failed":
        receipt.cleanup_error = "termination could not be proven"
    db.add(receipt)
    log = LogEntry(
        task_id=task.id,
        event_type="message",
        role="assistant",
        content="Plan history must survive a rejected or rolled-back delete",
    )
    db.add(log)
    await db.commit()
    return _PlanTaskGraph(
        task_id=task.id,
        run_id=run.id,
        step_id=step.id,
        receipt_id=receipt.id,
        log_id=log.id,
    )


async def _assert_graph_exists(db, graph: _PlanTaskGraph) -> None:
    db.expire_all()
    assert await db.get(Task, graph.task_id) is not None
    assert await db.get(PlanAgentRun, graph.run_id) is not None
    assert await db.get(PlanAgentStep, graph.step_id) is not None
    assert await db.get(PlanAgentRuntimeReceipt, graph.receipt_id) is not None
    assert await db.get(LogEntry, graph.log_id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "receipt_status",
    ["prepared", "admitting", "launching", "cleanup_failed"],
)
async def test_delete_plan_task_fails_closed_for_non_clean_runtime_receipt(
    db_session,
    receipt_status,
):
    graph = await _create_plan_task_graph(
        db_session,
        receipt_status=receipt_status,
    )

    assert await TaskQueue(db_session).delete(graph.task_id) is False

    # Runtime preflight rejects before any graph deletion.
    await _assert_graph_exists(db_session, graph)
    receipt = await db_session.get(PlanAgentRuntimeReceipt, graph.receipt_id)
    assert receipt.status == receipt_status


@pytest.mark.asyncio
async def test_delete_plan_task_fails_closed_when_step_has_no_runtime_receipt(
    db_session,
):
    graph = await _create_plan_task_graph(
        db_session,
        receipt_status="cleaned",
    )
    missing_receipt_step = PlanAgentStep(
        run_id=graph.run_id,
        generation=4,
        step_type="planner",
        round=2,
        provider="claude",
        status="failed",
        finished_at=datetime.utcnow(),
    )
    db_session.add(missing_receipt_step)
    await db_session.flush()
    missing_receipt_step_id = missing_receipt_step.id
    await db_session.commit()

    assert await TaskQueue(db_session).delete(graph.task_id) is False

    await _assert_graph_exists(db_session, graph)
    assert await db_session.get(PlanAgentStep, missing_receipt_step_id) is not None


@pytest.mark.asyncio
async def test_worker_delete_callback_waits_for_legacy_runtime_preflight(
    db_session,
):
    graph = await _create_plan_task_graph(
        db_session,
        receipt_status="cleanup_failed",
    )
    task = await db_session.get(Task, graph.task_id)
    task.worker_id = 91
    await db_session.commit()
    await db_session.refresh(task)
    remote_delete = AsyncMock(return_value=True)

    assert await TaskQueue(db_session).delete(
        task.id,
        expected_fence=task_delete_fence(task),
        remote_worker_deleted=True,
        remote_delete_confirm=remote_delete,
    ) is False

    remote_delete.assert_not_awaited()
    await _assert_graph_exists(db_session, graph)


@pytest.mark.asyncio
async def test_delete_plan_task_removes_clean_receipt_and_pipeline_history(
    db_session,
):
    graph = await _create_plan_task_graph(db_session, receipt_status="cleaned")

    assert await TaskQueue(db_session).delete(graph.task_id) is True

    db_session.expire_all()
    assert await db_session.get(Task, graph.task_id) is None
    assert await db_session.get(PlanAgentRun, graph.run_id) is None
    assert await db_session.get(PlanAgentStep, graph.step_id) is None
    assert (
        await db_session.get(PlanAgentRuntimeReceipt, graph.receipt_id) is None
    )
    assert await db_session.get(LogEntry, graph.log_id) is None


@pytest.mark.asyncio
async def test_delete_plan_task_restores_clean_receipt_graph_when_final_cas_loses(
    db_session,
):
    graph = await _create_plan_task_graph(db_session, receipt_status="cleaned")
    queue = TaskQueue(db_session)
    original_execute = db_session.execute
    lost_final_cas = False

    async def lose_final_task_delete(statement, *args, **kwargs):
        nonlocal lost_final_cas
        table = getattr(statement, "table", None)
        if (
            getattr(statement, "is_delete", False)
            and getattr(table, "name", None) == "tasks"
        ):
            lost_final_cas = True
            return MagicMock(rowcount=0)
        return await original_execute(statement, *args, **kwargs)

    with patch.object(
        db_session,
        "execute",
        new=AsyncMock(side_effect=lose_final_task_delete),
    ):
        assert await queue.delete(graph.task_id) is False

    assert lost_final_cas
    await _assert_graph_exists(db_session, graph)
    receipt = await db_session.get(PlanAgentRuntimeReceipt, graph.receipt_id)
    assert receipt.status == "cleaned"
    assert receipt.cleaned_at is not None
