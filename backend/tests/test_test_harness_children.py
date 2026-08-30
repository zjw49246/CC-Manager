from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from backend.config import settings
from backend.database import Base
from backend.models.instance import Instance
from backend.models.task import Task
from backend.models.test_harness import (
    BrowserReviewOperationReceipt,
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
    browser_child_binding_error,
    browser_child_ssh_grant_error,
    finalize_reaped_browser_child_binding,
)
from backend.services.test_harness_owner_fence import (
    install_test_harness_owner_terminal_gate,
    lock_test_harness_owner as durable_owner_lock,
    test_harness_owner_fence as owner_fence,
    test_harness_owner_identity as owner_identity_for_test,
)
from backend.services.test_harness import (
    TestHarnessError as HarnessError,
    TestHarnessService as HarnessService,
)
from backend.services.test_harness_contracts import TestHarnessSpec as HarnessSpec
import backend.services.test_harness_children as child_service_module


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
            owners = list(
                (
                    await db.execute(
                        select(Instance).where(
                            Instance.current_task_id == task_id
                        )
                    )
                ).scalars()
            )
            for owner in owners:
                owner.status = "idle"
                owner.current_task_id = None
                owner.pid = None
            await db.commit()

    return stop


async def _add_permitted_operation(
    db_factory,
    *,
    binding_id: str,
    operation_id: str,
) -> str:
    receipt_id = uuid.uuid4().hex
    async with db_factory() as db:
        binding = await db.get(ChildBindingModel, binding_id)
        assert binding is not None
        child = await db.get(Task, binding.child_task_id)
        assert child is not None
        db.add(
            BrowserReviewOperationReceipt(
                id=receipt_id,
                browser_review_job_id=binding.browser_review_job_id,
                operation_id=operation_id,
                binding_id=binding.id,
                harness_run_id=binding.harness_run_id,
                workspace_review_run_id=binding.workspace_review_run_id,
                owner_task_id=binding.owner_task_id,
                owner_task_incarnation_id=binding.owner_task_incarnation_id,
                owner_task_retry_count=binding.owner_task_retry_count,
                owner_task_turn_generation=binding.owner_task_turn_generation,
                owner_task_status=binding.owner_task_status,
                child_task_id=child.id,
                child_task_incarnation_id=child.incarnation_id,
                child_task_retry_count=child.retry_count,
                child_task_turn_generation=child.turn_generation,
                child_task_status=child.status,
                action_kind="click",
                request_digest="a" * 64,
                execution_nonce_digest="b" * 64,
                status="permitted",
                result_data={},
            )
        )
        await db.commit()
    return receipt_id


@pytest.mark.asyncio
async def test_owner_fence_context_is_not_reentrant_in_spawned_task():
    child_started = asyncio.Event()
    child_entered = asyncio.Event()

    async def child() -> None:
        child_started.set()
        async with owner_fence(991):
            child_entered.set()

    async with owner_fence(991):
        operation = asyncio.create_task(child())
        await asyncio.wait_for(child_started.wait(), timeout=1)
        await asyncio.sleep(0)
        assert not child_entered.is_set()
    await asyncio.wait_for(operation, timeout=1)
    assert child_entered.is_set()


@pytest.mark.asyncio
async def test_terminal_gate_blocks_exact_active_generation_but_not_new_status(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "harness-secret")
    async with db_factory() as db:
        owner = Task(
            title="terminal gate owner",
            description="owner",
            status="executing",
            provider="codex",
            model="gpt-5.6-sol",
        )
        db.add(owner)
        await db.commit()
        owner_id = owner.id
        active_identity = owner_identity_for_test(owner)
    async with db_factory() as db:
        await install_test_harness_owner_terminal_gate(
            db,
            active_identity,
            reason="natural completion",
        )
        await db.commit()

    service = HarnessService(db_factory=db_factory)
    spec = HarnessSpec(
        target_kind="fixed_url",
        target={"url": "https://example.com"},
        goal="Review",
    )
    with pytest.raises(HarnessError, match="terminalizing"):
        await service.start_task_run(
            task_id=owner_id,
            spec=spec,
            owner_identity=active_identity,
        )

    async with db_factory() as db:
        owner = await db.get(Task, owner_id)
        owner.status = "completed"
        await db.commit()
        completed_identity = owner_identity_for_test(owner)
    run = await service.start_task_run(
        task_id=owner_id,
        spec=spec,
        owner_identity=completed_identity,
    )
    assert run.owner_task_status == "completed"


@pytest.mark.asyncio
@pytest.mark.parametrize("runtime_evidence", ["reverse_instance", "pty_background"])
async def test_terminal_owner_must_be_runtime_idle_before_public_harness_start(
    db_factory,
    monkeypatch,
    runtime_evidence,
):
    monkeypatch.setattr(settings, "auth_token", "harness-secret")
    async with db_factory() as db:
        owner = Task(
            title="terminal owner with live runtime",
            description="owner",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
        )
        if runtime_evidence == "pty_background":
            owner.pty_background_generation = "unreaped-terminal-turn"
        db.add(owner)
        await db.flush()
        if runtime_evidence == "reverse_instance":
            db.add(
                Instance(
                    name="terminal owner reverse runtime",
                    status="running",
                    current_task_id=owner.id,
                )
            )
        await db.commit()
        owner_id = owner.id
        identity = owner_identity_for_test(owner)

    service = HarnessService(db_factory=db_factory)
    with pytest.raises(HarnessError, match="terminal.*not settled"):
        await service.start_task_run(
            task_id=owner_id,
            spec=HarnessSpec(
                target_kind="fixed_url",
                target={"url": "https://example.com"},
                goal="must wait for runtime reap",
            ),
            owner_identity=identity,
        )
    async with db_factory() as db:
        assert await db.scalar(select(func.count(RunModel.id))) == 0


@pytest.mark.asyncio
async def test_active_internal_owner_can_start_harness_with_exact_instance(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "harness-secret")
    async with db_factory() as db:
        owner = Task(
            title="active internal harness owner",
            description="owner",
            status="executing",
            provider="codex",
            model="gpt-5.6-sol",
        )
        db.add(owner)
        await db.flush()
        instance = Instance(
            name="active exact runtime",
            status="running",
            current_task_id=owner.id,
        )
        db.add(instance)
        await db.flush()
        owner.instance_id = instance.id
        await db.commit()
        owner_id = owner.id
        identity = owner_identity_for_test(owner)

    service = HarnessService(db_factory=db_factory)
    run = await service.start_task_run(
        task_id=owner_id,
        spec=HarnessSpec(
            target_kind="fixed_url",
            target={"url": "https://example.com"},
            goal="internal active owner",
        ),
        owner_identity=identity,
    )
    assert run.owner_task_status == "executing"


@pytest.mark.asyncio
async def test_public_start_identity_cannot_rebind_after_retry(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "harness-secret")
    async with db_factory() as db:
        owner = Task(
            title="start race owner",
            description="owner",
            status="completed",
            provider="codex",
            model="gpt-5.6-sol",
        )
        db.add(owner)
        await db.commit()
        owner_id = owner.id
        expected = owner_identity_for_test(owner)

    service = HarnessService(db_factory=db_factory)
    reached_create = asyncio.Event()
    resume_create = asyncio.Event()
    original_create = service._create_run

    async def paused_create(**kwargs):
        reached_create.set()
        await resume_create.wait()
        return await original_create(**kwargs)

    monkeypatch.setattr(service, "_create_run", paused_create)
    operation = asyncio.create_task(
        service.start_task_run(
            task_id=owner_id,
            spec=HarnessSpec(
                target_kind="fixed_url",
                target={"url": "https://example.com"},
                goal="Review",
            ),
            owner_identity=expected,
        )
    )
    await asyncio.wait_for(reached_create.wait(), timeout=1)
    async with db_factory() as db:
        owner = await db.get(Task, owner_id)
        owner.retry_count += 1
        owner.turn_generation += 1
        owner.status = "pending"
        await db.commit()
    resume_create.set()
    with pytest.raises(HarnessError, match="generation changed"):
        await asyncio.wait_for(operation, timeout=1)
    async with db_factory() as db:
        assert await db.scalar(select(func.count(RunModel.id))) == 0


@pytest.mark.asyncio
async def test_repeat_identity_cannot_rebind_after_retry(
    db_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "auth_token", "harness-secret")
    owner_id, source_id = await _owner_and_run(db_factory, suffix="repeat-race")
    async with db_factory() as db:
        source = await db.get(RunModel, source_id)
        source.status = "completed"
        source.stage = "completed"
        source.completed_at = datetime.utcnow()
        owner = await db.get(Task, owner_id)
        expected = owner_identity_for_test(owner)
        await db.commit()
    async with db_factory() as db:
        owner = await db.get(Task, owner_id)
        owner.retry_count += 1
        owner.turn_generation += 1
        owner.status = "pending"
        await db.commit()

    service = HarnessService(db_factory=db_factory)
    with pytest.raises(HarnessError, match="generation changed"):
        await service.repeat(
            source_id,
            owner_identity=expected,
        )
    async with db_factory() as db:
        assert await db.scalar(select(func.count(RunModel.id))) == 1


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
    assert child.execution_user_id is None
    assert child.execution_user_role == "member"
    assert child.execution_mode == "sandbox"
    assert child.execution_principal_kind == "system"
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
        orphan_id = orphan.id
        assert await TaskQueue(db).dequeue() is None
        persisted = await db.get(Task, orphan_id)
        assert persisted.status == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("drift_target", "drift_value"),
    [
        ("provider", "claude"),
        ("model", "gpt-5.6-luna"),
        ("effort_level", "low"),
        ("codex_service_tier", "priority"),
        ("timeout_hours", 2.0),
        ("max_retries", 3),
        ("capability_policy", {"plan": {"max_invocations": 1}}),
        ("worker_id", 41),
        ("shared_from_id", 42),
        (
            "execution_principal",
            {
                "execution_user_id": None,
                "execution_user_role": "super_admin",
                "execution_mode": "unrestricted",
                "execution_principal_kind": "deployment_token",
            },
        ),
        ("tags", {"pr-review": True}),
        ("session_id", "must-not-resume"),
        ("last_cwd", "/tmp/must-not-resume"),
        ("target_repo", "/tmp/untrusted-preview"),
        ("enabled_skills", {"browser-review": "wrong-job"}),
        ("metadata_", {"isolated_browser_agent": False}),
        ("launch_config_digest", "0" * 64),
    ],
)
async def test_dequeue_rejects_any_browser_launch_binding_drift(
    db_factory,
    drift_target,
    drift_value,
):
    owner_id, run_id = await _owner_and_run(db_factory, suffix=drift_target)
    service = ChildService(db_factory=db_factory)
    child, binding = await service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id="job-drift",
        harness_run_id=run_id,
        child_values=_child_values("job-drift"),
    )
    await service.activate(binding.id)

    async with db_factory() as db:
        if drift_target == "launch_config_digest":
            durable_binding = await db.get(ChildBindingModel, binding.id)
            durable_binding.launch_config_digest = drift_value
        else:
            durable_child = await db.get(Task, child.id)
            if drift_target == "execution_principal":
                for field, value in drift_value.items():
                    setattr(durable_child, field, value)
            else:
                setattr(durable_child, drift_target, drift_value)
        await db.commit()

    async with db_factory() as db:
        assert await TaskQueue(db).dequeue(instance_id=91) is None
        durable_child = await db.get(Task, child.id)
        durable_binding = await db.get(ChildBindingModel, binding.id)
        assert durable_child.status == "pending"
        assert durable_binding.state == CHILD_READY


@pytest.mark.asyncio
async def test_browser_child_allows_only_runtime_account_metadata(db_factory):
    owner_id, run_id = await _owner_and_run(db_factory)
    service = ChildService(db_factory=db_factory)
    child, binding = await service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id="job-account-route",
        harness_run_id=run_id,
        child_values=_child_values("job-account-route"),
    )
    await service.activate(binding.id)

    async with db_factory() as db:
        durable = await db.get(Task, child.id)
        durable.metadata_ = {
            **durable.metadata_,
            "codex_account_id": "codex-account-2",
        }
        await db.commit()
        claimed = await TaskQueue(db).dequeue(instance_id=92)
        assert claimed is not None and claimed.id == child.id


@pytest.mark.asyncio
async def test_browser_child_accepts_previous_status_terminal_gate_after_executing_transition(
    db_factory,
):
    owner_id, run_id = await _owner_and_run(db_factory, suffix="status-gate")
    service = ChildService(db_factory=db_factory)
    child, binding = await service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id="job-status-gate",
        harness_run_id=run_id,
        child_values=_child_values("job-status-gate"),
    )
    await service.activate(binding.id)

    async with db_factory() as db:
        claimed = await TaskQueue(db).dequeue(instance_id=93)
        assert claimed is not None and claimed.id == child.id
        durable = await db.get(Task, child.id, populate_existing=True)
        in_progress_identity = owner_identity_for_test(durable)
        await install_test_harness_owner_terminal_gate(
            db,
            in_progress_identity,
            reason="Task mode lifecycle entered executing status",
        )
        durable.status = "executing"
        await db.commit()

    async with db_factory() as db:
        durable = await db.get(Task, child.id)
        durable_binding = await db.get(ChildBindingModel, binding.id)
        assert browser_child_binding_error(durable_binding, durable) is None


@pytest.mark.asyncio
async def test_browser_child_rejects_current_or_drifted_terminal_gate(db_factory):
    owner_id, run_id = await _owner_and_run(db_factory, suffix="active-gate")
    service = ChildService(db_factory=db_factory)
    child, binding = await service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id="job-active-gate",
        harness_run_id=run_id,
        child_values=_child_values("job-active-gate"),
    )
    await service.activate(binding.id)

    async with db_factory() as db:
        claimed = await TaskQueue(db).dequeue(instance_id=94)
        assert claimed is not None and claimed.id == child.id
        durable = await db.get(Task, child.id, populate_existing=True)
        await install_test_harness_owner_terminal_gate(
            db,
            owner_identity_for_test(durable),
            reason="terminalizing exact Browser child generation",
        )
        await db.commit()

    async with db_factory() as db:
        durable = await db.get(Task, child.id)
        durable_binding = await db.get(ChildBindingModel, binding.id)
        assert browser_child_binding_error(durable_binding, durable) == (
            "Browser child generation is already terminalizing"
        )
        metadata = dict(durable.metadata_ or {})
        gate = dict(metadata["test_harness_terminal_generation"])
        gate["cleanup_harness_run_ids"] = ["a" * 32]
        metadata["test_harness_terminal_generation"] = gate
        durable.metadata_ = metadata
        await db.commit()

    async with db_factory() as db:
        durable = await db.get(Task, child.id)
        durable_binding = await db.get(ChildBindingModel, binding.id)
        assert browser_child_binding_error(durable_binding, durable) == (
            "Browser child terminal gate metadata drifted"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "values",
    [
        {"session_id": "existing-session"},
        {"last_cwd": "/tmp/existing-session"},
        {"target_repo": "/tmp/untrusted-preview"},
        {"capability_policy": {"plan": {"max_invocations": 1}}},
        {"worker_id": 7},
        {"shared_from_id": 8},
        {"delivery_run_id": 9, "delivery_role": "developer"},
        {"tags": {"pr-review": True}},
        {"metadata_": {"arbitrary_prompt_input": "forbidden"}},
    ],
)
async def test_browser_child_reservation_rejects_resume_capability_and_metadata(
    db_factory,
    values,
):
    owner_id, run_id = await _owner_and_run(db_factory)
    service = ChildService(db_factory=db_factory)
    with pytest.raises(ChildError):
        await service.reserve_child(
            owner_task_id=owner_id,
            browser_review_job_id="job-invalid-profile",
            harness_run_id=run_id,
            child_values={
                **_child_values("job-invalid-profile"),
                **values,
            },
        )


def test_browser_child_ssh_collision_hook_is_fail_closed():
    child = Task(metadata_={"isolated_browser_agent": True})
    ordinary = Task(metadata_={})

    assert "cannot receive SSH" in (
        browser_child_ssh_grant_error(child, has_ssh_grant=True) or ""
    )
    assert browser_child_ssh_grant_error(child, has_ssh_grant=False) is None
    assert browser_child_ssh_grant_error(ordinary, has_ssh_grant=True) is None


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
@pytest.mark.parametrize("transition", ("dequeue", "defer", "retry"))
async def test_browser_queue_transitions_lock_binding_before_child_task(
    db_factory,
    transition,
):
    """Browser queue writes keep the PostgreSQL/MySQL binding -> child order.

    Browser callbacks, provider admission and stop all lock the durable binding
    before the child Task.  Reversing those two writes here lets a queue claim
    or release hold the child while stop holds the binding, forming a real
    row-lock cycle on PostgreSQL/MySQL even though SQLite serializes the test
    transaction without exposing it.
    """

    owner_id, run_id = await _owner_and_run(db_factory, suffix=transition)
    service = ChildService(db_factory=db_factory)
    child, binding = await service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id=f"job-lock-order-{transition}",
        harness_run_id=run_id,
        child_values=_child_values(f"job-lock-order-{transition}"),
    )
    await service.activate(binding.id)
    instance_id = 70

    if transition != "dequeue":
        async with db_factory() as setup:
            claimed = await TaskQueue(setup).dequeue(instance_id=instance_id)
            assert claimed is not None and claimed.id == child.id

    async with db_factory() as db:
        queue = TaskQueue(db)
        original_execute = db.execute
        write_tables: list[str] = []

        async def record_writes(statement, *args, **kwargs):
            table_name = getattr(
                getattr(statement, "table", None),
                "name",
                None,
            )
            if getattr(statement, "is_update", False) and table_name in {
                "tasks",
                "test_harness_child_bindings",
            }:
                write_tables.append(table_name)
            return await original_execute(statement, *args, **kwargs)

        with patch.object(
            db,
            "execute",
            new=AsyncMock(side_effect=record_writes),
        ):
            if transition == "dequeue":
                result = await queue.dequeue(instance_id=instance_id)
                assert result is not None and result.id == child.id
            elif transition == "defer":
                assert await queue.defer(
                    child.id,
                    "ordered Browser claim release",
                    instance_id=instance_id,
                )
            else:
                result = await queue.retry(
                    child.id,
                    expected_statuses=("in_progress",),
                    instance_id=instance_id,
                )
                assert result is not None and result.id == child.id

    assert write_tables[:3] == [
        "tasks",
        "test_harness_child_bindings",
        "tasks",
    ]


@pytest.mark.asyncio
async def test_browser_dequeue_retries_after_child_cas_miss_without_stuck_binding(
    db_factory,
):
    """A claim loser rolls back its binding CAS before retrying the child."""

    owner_id, run_id = await _owner_and_run(
        db_factory,
        suffix="dequeue-cas-miss",
    )
    service = ChildService(db_factory=db_factory)
    child, binding = await service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id="job-dequeue-cas-miss",
        harness_run_id=run_id,
        child_values=_child_values("job-dequeue-cas-miss"),
    )
    await service.activate(binding.id)
    instance_id = 79

    async with db_factory() as db:
        queue = TaskQueue(db)
        original_execute = db.execute
        binding_claim_staged = False
        binding_claim_attempts = 0
        child_cas_misses = 0

        async def miss_first_child_cas(statement, *args, **kwargs):
            nonlocal binding_claim_staged
            nonlocal binding_claim_attempts
            nonlocal child_cas_misses
            table_name = getattr(
                getattr(statement, "table", None),
                "name",
                None,
            )
            if (
                getattr(statement, "is_update", False)
                and table_name == "test_harness_child_bindings"
            ):
                result = await original_execute(statement, *args, **kwargs)
                if result.rowcount:
                    binding_claim_attempts += 1
                    binding_claim_staged = True
                return result
            if (
                binding_claim_staged
                and child_cas_misses == 0
                and getattr(statement, "is_update", False)
                and table_name == "tasks"
            ):
                # Clear the local marker as the real method must now roll back
                # this staged binding transition before starting its next loop.
                binding_claim_staged = False
                child_cas_misses += 1
                return MagicMock(rowcount=0)
            return await original_execute(statement, *args, **kwargs)

        with patch.object(
            db,
            "execute",
            new=AsyncMock(side_effect=miss_first_child_cas),
        ):
            claimed = await queue.dequeue(instance_id=instance_id)

        assert claimed is not None and claimed.id == child.id
        assert child_cas_misses == 1
        assert binding_claim_attempts == 2

    async with db_factory() as db:
        durable_child = await db.get(Task, child.id)
        durable_binding = await db.get(ChildBindingModel, binding.id)
        assert durable_child is not None
        assert durable_child.status == "in_progress"
        assert durable_child.turn_generation == 1
        assert durable_child.instance_id == instance_id
        assert durable_binding is not None
        assert durable_binding.state == CHILD_RUNNING
        assert durable_binding.claimed_retry_count == 0
        assert durable_binding.claimed_instance_id == instance_id


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ("defer", "retry"))
async def test_browser_queue_release_rolls_back_when_child_cas_misses(
    db_factory,
    transition,
):
    """A lost child CAS cannot publish the staged Browser binding release.

    ``retry`` intentionally uses its default ``rollback_on_miss=False``.  That
    historical commit-on-miss behavior is valid for ordinary Tasks, but a
    Browser retry has already updated its binding and must roll back the whole
    transaction when the exact child generation loses its CAS.
    """

    owner_id, run_id = await _owner_and_run(
        db_factory,
        suffix=f"{transition}-cas-miss",
    )
    service = ChildService(db_factory=db_factory)
    child, binding = await service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id=f"job-{transition}-cas-miss",
        harness_run_id=run_id,
        child_values=_child_values(f"job-{transition}-cas-miss"),
    )
    await service.activate(binding.id)
    instance_id = 80

    async with db_factory() as setup:
        claimed = await TaskQueue(setup).dequeue(instance_id=instance_id)
        assert claimed is not None and claimed.id == child.id

    async with db_factory() as db:
        queue = TaskQueue(db)
        original_execute = db.execute
        binding_release_staged = False
        child_cas_missed = False

        async def force_child_cas_miss(statement, *args, **kwargs):
            nonlocal binding_release_staged, child_cas_missed
            table_name = getattr(
                getattr(statement, "table", None),
                "name",
                None,
            )
            if (
                getattr(statement, "is_update", False)
                and table_name == "test_harness_child_bindings"
            ):
                result = await original_execute(statement, *args, **kwargs)
                binding_release_staged = True
                return result
            if (
                binding_release_staged
                and not child_cas_missed
                and getattr(statement, "is_update", False)
                and table_name == "tasks"
            ):
                child_cas_missed = True
                return MagicMock(rowcount=0)
            return await original_execute(statement, *args, **kwargs)

        with patch.object(
            db,
            "execute",
            new=AsyncMock(side_effect=force_child_cas_miss),
        ):
            if transition == "defer":
                assert not await queue.defer(
                    child.id,
                    "simulated exact child CAS loss",
                    instance_id=instance_id,
                )
            else:
                assert await queue.retry(
                    child.id,
                    expected_statuses=("in_progress",),
                    instance_id=instance_id,
                ) is None

        assert binding_release_staged
        assert child_cas_missed

    async with db_factory() as db:
        durable_child = await db.get(Task, child.id)
        durable_binding = await db.get(ChildBindingModel, binding.id)
        assert durable_child is not None
        assert durable_child.status == "in_progress"
        assert durable_child.retry_count == 0
        assert durable_child.instance_id == instance_id
        assert durable_binding is not None
        assert durable_binding.state == CHILD_RUNNING
        assert durable_binding.claimed_retry_count == 0
        assert durable_binding.claimed_instance_id == instance_id
        assert durable_binding.error is None


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
    async with db_factory() as db:
        instance = Instance(name="Browser stop receipt", status="idle")
        db.add(instance)
        await db.commit()
        instance_id = instance.id
    async with db_factory() as db:
        claimed = await TaskQueue(db).dequeue(instance_id=instance_id)
        assert claimed is not None and claimed.id == child.id
        instance = await db.get(Instance, instance_id)
        instance.status = "running"
        instance.current_task_id = child.id
        instance.started_at = claimed.started_at
        await db.commit()
    receipt_id = await _add_permitted_operation(
        db_factory,
        binding_id=binding.id,
        operation_id="1" * 32,
    )

    await asyncio.gather(
        service.stop_binding(binding.id, reason="first stop"),
        service.stop_binding(binding.id, reason="second stop"),
    )

    assert calls == [child.id]
    async with db_factory() as db:
        durable = await db.get(ChildBindingModel, binding.id)
        stopped = await db.get(Task, child.id)
        receipt = await db.get(BrowserReviewOperationReceipt, receipt_id)
        assert durable.state == CHILD_STOPPED
        assert durable.completed_at is not None
        assert stopped.status == "cancelled"
        assert receipt.status == "uncertain"
        assert receipt.acknowledged_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_finishes_first", [True, False])
@pytest.mark.parametrize("claimed", [True, False])
async def test_cross_service_stop_success_absorbs_concurrent_failure(
    db_factory,
    monkeypatch,
    failure_finishes_first,
    claimed,
):
    owner_id, run_id = await _owner_and_run(db_factory)
    setup_service = ChildService(db_factory=db_factory)
    child, binding = await setup_service.reserve_child(
        owner_task_id=owner_id,
        browser_review_job_id=(
            "job-cross-service-stop-"
            f"{int(failure_finishes_first)}-{int(claimed)}"
        ),
        harness_run_id=run_id,
        child_values=_child_values(
            "job-cross-service-stop-"
            f"{int(failure_finishes_first)}-{int(claimed)}"
        ),
    )
    await setup_service.activate(binding.id)
    if claimed:
        async with db_factory() as db:
            instance = Instance(name="Cross-service Browser stop", status="idle")
            db.add(instance)
            await db.commit()
            instance_id = instance.id
        async with db_factory() as db:
            claimed_task = await TaskQueue(db).dequeue(instance_id=instance_id)
            assert claimed_task is not None and claimed_task.id == child.id
            instance = await db.get(Instance, instance_id)
            assert instance is not None
            instance.status = "running"
            instance.current_task_id = child.id
            instance.started_at = claimed_task.started_at
            await db.commit()

    class FakeBrowserJobManager:
        async def mark_cancelling(self, _job_id):
            return None

        async def cancel(self, _job_id):
            return None

    from backend.services import browser_review_jobs

    monkeypatch.setattr(
        browser_review_jobs,
        "browser_review_job_manager",
        FakeBrowserJobManager(),
    )
    success_started = asyncio.Event()
    failure_started = asyncio.Event()
    release_success = asyncio.Event()
    release_failure = asyncio.Event()
    finish_success = _cancelling_stopper(db_factory)

    async def successful_stopper(task_id: int) -> None:
        success_started.set()
        await release_success.wait()
        await finish_success(task_id)

    async def failing_stopper(_task_id: int) -> None:
        failure_started.set()
        await release_failure.wait()
        raise RuntimeError("concurrent child cleanup failed")

    winner = ChildService(
        db_factory=db_factory,
        task_stopper=successful_stopper,
    )
    loser = ChildService(
        db_factory=db_factory,
        task_stopper=failing_stopper,
    )
    winning_stop = asyncio.create_task(
        winner.stop_binding(binding.id, reason="winning stop")
    )
    await asyncio.wait_for(success_started.wait(), timeout=1)
    losing_stop = asyncio.create_task(
        loser.stop_binding(binding.id, reason="losing stop")
    )
    await asyncio.wait_for(failure_started.wait(), timeout=1)

    if failure_finishes_first:
        release_failure.set()
        with pytest.raises(ChildError, match="concurrent child cleanup failed"):
            await asyncio.wait_for(losing_stop, timeout=2)
        async with db_factory() as db:
            failed = await db.get(ChildBindingModel, binding.id)
            assert failed is not None and failed.state == CHILD_STOP_FAILED
        release_success.set()
        await asyncio.wait_for(winning_stop, timeout=2)
    else:
        release_success.set()
        await asyncio.wait_for(winning_stop, timeout=2)
        release_failure.set()
        # The durable terminal success absorbs this executor's late failure.
        await asyncio.wait_for(losing_stop, timeout=2)

    async with db_factory() as db:
        durable = await db.get(ChildBindingModel, binding.id)
        stopped = await db.get(Task, child.id)
        assert durable is not None and durable.state == CHILD_STOPPED
        assert durable.error is None
        assert stopped is not None and stopped.status == "cancelled"


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
async def test_natural_completion_waits_for_exact_reap_receipt(db_factory):
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
        instance = Instance(name="Browser natural receipt", status="idle")
        db.add(instance)
        await db.commit()
        instance_id = instance.id
    async with db_factory() as db:
        claimed = await TaskQueue(db).dequeue(instance_id=instance_id)
        assert claimed is not None
        instance = await db.get(Instance, instance_id)
        instance.status = "running"
        instance.current_task_id = child.id
        instance.started_at = claimed.started_at
        await db.commit()
    receipt_id = await _add_permitted_operation(
        db_factory,
        binding_id=binding.id,
        operation_id="2" * 32,
    )
    async with db_factory() as db:
        assert await TaskQueue(db).mark_completed(child.id)
        durable = await db.get(ChildBindingModel, binding.id)
        assert durable.state == CHILD_RUNNING
    assert not await service.mark_terminal_by_child(
        child.id,
        task_status="completed",
    )

    async with db_factory() as db:
        instance = await db.get(Instance, instance_id)
        instance.status = "idle"
        instance.current_task_id = None
        instance.pid = None
        await db.flush()
        completed = await db.get(Task, child.id)
        assert completed is not None
        assert await finalize_reaped_browser_child_binding(
            db,
            completed,
            instance_id=instance_id,
        )
        await db.commit()
        durable = await db.get(ChildBindingModel, binding.id)
        receipt = await db.get(BrowserReviewOperationReceipt, receipt_id)
        assert durable.state == CHILD_COMPLETED
        assert durable.completed_at is not None
        assert receipt.status == "uncertain"
        assert receipt.acknowledged_at is not None
    assert await service.mark_terminal_by_child(
        child.id,
        task_status="completed",
    )


@pytest.mark.asyncio
async def test_wal_delete_winner_prevents_late_harness_run_materialization(
    monkeypatch,
    tmp_path,
):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'harness-run-fence.db'}",
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
            owner = Task(
                title="WAL Harness owner",
                description="owner",
                status="completed",
                provider="codex",
                model="gpt-5.6-sol",
                effort_level="high",
            )
            setup.add(owner)
            await setup.commit()
            owner_id = owner.id

        service = HarnessService(db_factory=sessions)
        reached_materialization = asyncio.Event()
        resume_materialization = asyncio.Event()
        original_create_run = service._create_run

        async def paused_create_run(**kwargs):
            reached_materialization.set()
            await resume_materialization.wait()
            return await original_create_run(**kwargs)

        monkeypatch.setattr(service, "_create_run", paused_create_run)
        operation = asyncio.create_task(
            service.start_task_run(
                task_id=owner_id,
                spec=HarnessSpec(
                    target_kind="fixed_url",
                    target={"url": "https://example.com"},
                    goal="Review",
                ),
            )
        )
        await asyncio.wait_for(reached_materialization.wait(), timeout=1)
        async with sessions() as deleter:
            assert await TaskQueue(deleter)._delete_under_owner_fence(owner_id)
        resume_materialization.set()
        with pytest.raises(HarnessError, match="owner Task"):
            await asyncio.wait_for(operation, timeout=1)

        async with sessions() as verify:
            assert await verify.get(Task, owner_id) is None
            assert await verify.scalar(select(func.count(RunModel.id))) == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ("reserve", "activate"))
async def test_wal_delete_winner_prevents_late_browser_child_publication(
    monkeypatch,
    tmp_path,
    transition,
):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / f'harness-child-{transition}.db'}",
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
        owner_id, run_id = await _owner_and_run(sessions, suffix=transition)
        service = ChildService(db_factory=sessions)
        child = None
        binding = None
        if transition == "activate":
            child, binding = await service.reserve_child(
                owner_task_id=owner_id,
                browser_review_job_id="job-wal-activate",
                harness_run_id=run_id,
                child_values=_child_values("job-wal-activate"),
            )

        reached_owner_cas = asyncio.Event()
        resume_owner_cas = asyncio.Event()

        async def paused_owner_lock(db, identity):
            reached_owner_cas.set()
            await resume_owner_cas.wait()
            return await durable_owner_lock(db, identity)

        monkeypatch.setattr(
            child_service_module,
            "lock_test_harness_owner",
            paused_owner_lock,
        )
        if transition == "reserve":
            operation = asyncio.create_task(
                service.reserve_child(
                    owner_task_id=owner_id,
                    browser_review_job_id="job-wal-reserve",
                    harness_run_id=run_id,
                    child_values=_child_values("job-wal-reserve"),
                )
            )
        else:
            assert binding is not None and child is not None
            operation = asyncio.create_task(service.activate(binding.id))

        await asyncio.wait_for(reached_owner_cas.wait(), timeout=1)
        async with sessions() as winner:
            run = await winner.get(RunModel, run_id)
            assert run is not None
            run.status = "completed"
            run.stage = "completed"
            run.cleanup_status = "completed"
            run.completed_at = datetime.utcnow()
            if transition == "activate":
                durable_binding = await winner.get(
                    ChildBindingModel,
                    binding.id,
                )
                durable_child = await winner.get(Task, child.id)
                assert durable_binding is not None and durable_child is not None
                durable_binding.state = CHILD_STOPPED
                durable_binding.completed_at = datetime.utcnow()
                durable_child.status = "cancelled"
                durable_child.completed_at = datetime.utcnow()
            await winner.commit()
        async with sessions() as deleter:
            assert await TaskQueue(deleter)._delete_under_owner_fence(owner_id)

        resume_owner_cas.set()
        with pytest.raises(ChildError, match="owner Task"):
            await asyncio.wait_for(operation, timeout=1)
        async with sessions() as verify:
            assert await verify.get(Task, owner_id) is None
            assert await verify.scalar(select(func.count(Task.id))) == 0
            assert await verify.scalar(
                select(func.count(ChildBindingModel.id))
            ) == 0
    finally:
        await engine.dispose()
