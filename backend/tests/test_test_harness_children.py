from __future__ import annotations

import asyncio
import uuid
from datetime import datetime

import pytest
from sqlalchemy import select

from backend.models.task import Task
from backend.models.test_harness import (
    TestHarnessChildBinding as ChildBindingModel,
    TestHarnessRun as RunModel,
)
from backend.services.task_queue import TaskQueue
from backend.services.test_harness_children import (
    CHILD_COMPLETED,
    CHILD_READY,
    CHILD_RUNNING,
    CHILD_STOPPED,
    CHILD_STOP_FAILED,
    TestHarnessChildError as ChildError,
    TestHarnessChildService as ChildService,
)


async def _owner_and_run(db_factory, *, suffix: str = "") -> tuple[int, str]:
    run_id = uuid.uuid4().hex
    async with db_factory() as db:
        owner = Task(
            title=f"Harness owner {suffix}",
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
            RunModel(
                id=run_id,
                task_id=owner.id,
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
        return owner.id, run_id


def _child_values(job_id: str) -> dict:
    return {
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
    }


def _cancelling_stopper(db_factory, calls: list[int] | None = None):
    async def stop(task_id: int) -> None:
        if calls is not None:
            calls.append(task_id)
        async with db_factory() as db:
            task = await db.get(Task, task_id)
            if task is not None and task.status not in {
                "completed",
                "failed",
                "cancelled",
                "conflict",
            }:
                task.status = "cancelled"
                task.completed_at = datetime.utcnow()
                await db.commit()

    return stop


@pytest.mark.asyncio
async def test_reserved_browser_child_is_not_claimable_until_activation(db_factory):
    owner_id, run_id = await _owner_and_run(db_factory)
    service = ChildService(db_factory=db_factory)
    child, binding = await service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id="job-reserved",
        harness_run_id=run_id,
        child_values=_child_values("job-reserved"),
    )

    assert child.status == "pending_activation"
    async with db_factory() as db:
        assert await TaskQueue(db).dequeue() is None

        # Even a corrupted early Task publication remains fail-closed while
        # the durable binding has not reached ``ready``.
        persisted = await db.get(Task, child.id)
        persisted.status = "pending"
        await db.commit()
        assert await TaskQueue(db).dequeue() is None

    async with db_factory() as db:
        persisted = await db.get(Task, child.id)
        persisted.status = "pending_activation"
        await db.commit()
    await service.activate(binding.id)

    async with db_factory() as db:
        claimed = await TaskQueue(db).dequeue(instance_id=41)
        assert claimed is not None
        assert claimed.id == child.id
        durable = await db.get(ChildBindingModel, binding.id)
        assert durable.state == CHILD_RUNNING
        assert durable.claimed_instance_id == 41
        assert durable.claimed_retry_count == 0


@pytest.mark.asyncio
async def test_isolated_pending_task_without_binding_is_never_claimed(db_factory):
    async with db_factory() as db:
        orphan = Task(
            title="orphan browser child",
            description="must not launch",
            status="pending",
            provider="codex",
            model="gpt-5.6-sol",
            codex_service_tier="default",
            effort_level="high",
            metadata_={"isolated_browser_agent": True},
        )
        db.add(orphan)
        await db.commit()
        assert await TaskQueue(db).dequeue() is None
        persisted = await db.get(Task, orphan.id)
        assert persisted.status == "pending"


@pytest.mark.asyncio
async def test_defer_atomically_reopens_browser_child_claim(db_factory):
    owner_id, run_id = await _owner_and_run(db_factory)
    service = ChildService(db_factory=db_factory)
    child, binding = await service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id="job-defer",
        harness_run_id=run_id,
        child_values=_child_values("job-defer"),
    )
    await service.activate(binding.id)

    async with db_factory() as db:
        claimed = await TaskQueue(db).dequeue(instance_id=51)
        assert claimed is not None
        assert await TaskQueue(db).defer(
            child.id,
            "Browser model account is cooling down",
            instance_id=51,
        )
        durable = await db.get(ChildBindingModel, binding.id)
        task = await db.get(Task, child.id)
        assert task.status == "pending"
        assert durable.state == CHILD_READY
        assert durable.claimed_instance_id is None


@pytest.mark.asyncio
async def test_concurrent_stop_is_idempotent_and_proves_terminal_child(db_factory):
    owner_id, run_id = await _owner_and_run(db_factory)
    calls: list[int] = []
    service = ChildService(
        db_factory=db_factory,
        task_stopper=_cancelling_stopper(db_factory, calls),
    )
    child, binding = await service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id="job-stop-race",
        harness_run_id=run_id,
        child_values=_child_values("job-stop-race"),
    )
    await service.activate(binding.id)

    await asyncio.gather(
        service.stop_binding(binding.id, reason="first stop"),
        service.stop_binding(binding.id, reason="second stop"),
    )

    assert calls == [child.id]
    async with db_factory() as db:
        durable = await db.get(ChildBindingModel, binding.id)
        stopped = await db.get(Task, child.id)
        assert durable.state == CHILD_STOPPED
        assert durable.completed_at is not None
        assert stopped.status == "cancelled"


@pytest.mark.asyncio
async def test_stop_failure_is_durable_and_not_reported_as_clean(db_factory):
    owner_id, run_id = await _owner_and_run(db_factory)

    async def broken_stopper(_task_id: int) -> None:
        raise RuntimeError("process owner could not be reaped")

    service = ChildService(
        db_factory=db_factory,
        task_stopper=broken_stopper,
    )
    _child, binding = await service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id="job-stop-fails",
        harness_run_id=run_id,
        child_values=_child_values("job-stop-fails"),
    )
    await service.activate(binding.id)

    with pytest.raises(ChildError, match="could not be reaped"):
        await service.stop_binding(binding.id, reason="owner cancelled")

    async with db_factory() as db:
        durable = await db.get(ChildBindingModel, binding.id)
        assert durable.state == CHILD_STOP_FAILED
        assert "could not be reaped" in durable.error


@pytest.mark.asyncio
async def test_abort_before_activation_closes_task_and_binding(db_factory):
    owner_id, run_id = await _owner_and_run(db_factory)
    service = ChildService(db_factory=db_factory)
    child, binding = await service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id="job-attach-fails",
        harness_run_id=run_id,
        child_values=_child_values("job-attach-fails"),
    )

    await service.abort_reservation(binding.id, RuntimeError("attach failed"))

    async with db_factory() as db:
        durable = await db.get(ChildBindingModel, binding.id)
        stopped = await db.get(Task, child.id)
        assert durable.state == CHILD_STOPPED
        assert stopped.status == "cancelled"
        assert "attach failed" in stopped.error_message


@pytest.mark.asyncio
async def test_startup_recovery_stops_reserved_and_legacy_children(db_factory):
    owner_id, run_id = await _owner_and_run(db_factory)
    calls: list[int] = []
    service = ChildService(
        db_factory=db_factory,
        task_stopper=_cancelling_stopper(db_factory, calls),
    )
    reserved, binding = await service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id="job-reserved-recovery",
        harness_run_id=run_id,
        child_values=_child_values("job-reserved-recovery"),
    )

    legacy_owner_id, legacy_run_id = await _owner_and_run(
        db_factory, suffix="legacy"
    )
    async with db_factory() as db:
        legacy = Task(
            title="legacy Browser child",
            description="created before durable bindings",
            status="pending",
            provider="codex",
            model="gpt-5.6-sol",
            codex_service_tier="default",
            effort_level="high",
            metadata_={
                "isolated_browser_agent": True,
                "test_harness_run_id": legacy_run_id,
                "test_harness_parent_task_id": legacy_owner_id,
                "browser_review_job_id": "legacy-job",
            },
        )
        db.add(legacy)
        await db.commit()
        legacy_id = legacy.id

    assert await service.recover_interrupted() == 2
    assert set(calls) == {reserved.id, legacy_id}
    async with db_factory() as db:
        first = await db.get(ChildBindingModel, binding.id)
        adopted = await db.scalar(
            select(ChildBindingModel).where(
                ChildBindingModel.child_task_id == legacy_id
            )
        )
        assert first.state == CHILD_STOPPED
        assert adopted is not None
        assert adopted.state == CHILD_STOPPED


@pytest.mark.asyncio
async def test_natural_completion_projects_to_binding(db_factory):
    owner_id, run_id = await _owner_and_run(db_factory)
    service = ChildService(db_factory=db_factory)
    child, binding = await service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id="job-complete",
        harness_run_id=run_id,
        child_values=_child_values("job-complete"),
    )
    await service.activate(binding.id)
    async with db_factory() as db:
        claimed = await TaskQueue(db).dequeue()
        assert claimed is not None
        assert await TaskQueue(db).mark_completed(child.id)
        durable = await db.get(ChildBindingModel, binding.id)
        assert durable.state == CHILD_COMPLETED
