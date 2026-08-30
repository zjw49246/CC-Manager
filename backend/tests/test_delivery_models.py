"""Database ownership fences for durable Delivery Loop orchestration."""

from datetime import datetime

import pytest
from sqlalchemy.exc import IntegrityError

from backend.models.delivery import (
    DeliveryAction,
    DeliveryCycle,
    DeliveryEvent,
    DeliveryRun,
    DeliveryTransition,
    DeliveryTurn,
)
from backend.models.project import Project
from backend.models.task import Task
from backend.models.worktree import Worktree


_HASH = "0" * 64


async def _run(db_session, *, branch: str = "ccm/delivery/one") -> DeliveryRun:
    project = Project(name=f"project-{branch}")
    db_session.add(project)
    await db_session.flush()
    run = DeliveryRun(
        admission_scope="system",
        idempotency_key=f"model-{branch}",
        request_hash="f" * 64,
        project_id=project.id,
        title="Deliver exact PR head",
        requirements="Implement and validate the requested change",
        requirements_hash=_HASH,
        policy_snapshot={"terminal": "ready_to_merge"},
        policy_hash=_HASH,
        base_branch="main",
        delivery_branch=branch,
        phase="planning",
        activity="ready",
        state_version=1,
    )
    db_session.add(run)
    await db_session.flush()
    return run


def _cycle(run_id: int, number: int, *, status: str = "planning") -> DeliveryCycle:
    active = status in {"planning", "coding", "pre_review", "publishing"}
    return DeliveryCycle(
        run_id=run_id,
        cycle_number=number,
        active_run_id=run_id if active else None,
        status=status,
        state_version=1,
        trigger_kind="initial" if number == 1 else "review_blocked",
        trigger_payload={},
        trigger_hash=_HASH,
    )


def test_delivery_cycle_review_result_is_foreign_keyed_evidence():
    foreign_keys = DeliveryCycle.__table__.c.review_result_id.foreign_keys

    assert {foreign_key.target_fullname for foreign_key in foreign_keys} == {
        "code_review_results.id"
    }


@pytest.mark.asyncio
async def test_delivery_run_terminal_shape_is_database_enforced(db_session):
    run = await _run(db_session)
    run.phase = "done"
    run.activity = "terminal"
    run.outcome = "success"

    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_delivery_run_terminal_shape_accepts_completed_timestamp(db_session):
    run = await _run(db_session)
    run.phase = "done"
    run.activity = "terminal"
    run.outcome = "success"
    run.completed_at = datetime.utcnow()

    await db_session.commit()

    assert run.outcome == "success"


@pytest.mark.asyncio
async def test_delivery_task_requires_controller_owner_shape(db_session):
    db_session.add(Task(title="unowned", mode="delivery_loop"))

    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_normal_task_cannot_impersonate_delivery_owner(db_session):
    db_session.add(
        Task(
            title="impostor",
            mode="auto",
            delivery_run_id=1,
            delivery_role="developer",
        )
    )

    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_only_one_worktree_can_bind_a_delivery_run(db_session):
    run = await _run(db_session)
    db_session.add_all(
        [
            Worktree(
                repo_path="/repo",
                worktree_path="/worktree/one",
                branch_name="delivery-one",
                delivery_run_id=run.id,
            ),
            Worktree(
                repo_path="/repo",
                worktree_path="/worktree/two",
                branch_name="delivery-two",
                delivery_run_id=run.id,
            ),
        ]
    )

    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_only_one_active_cycle_can_own_a_run(db_session):
    run = await _run(db_session)
    db_session.add_all([_cycle(run.id, 1), _cycle(run.id, 2)])

    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_terminal_cycle_must_release_active_slot(db_session):
    run = await _run(db_session)
    cycle = _cycle(run.id, 1, status="completed")
    cycle.active_run_id = run.id
    db_session.add(cycle)

    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_only_one_active_developer_turn_can_own_a_run(db_session):
    run = await _run(db_session)
    cycle = _cycle(run.id, 1)
    task = Task(
        title="developer",
        mode="delivery_loop",
        delivery_run_id=run.id,
        delivery_role="developer",
    )
    db_session.add_all([cycle, task])
    await db_session.flush()

    def turn(generation: int) -> DeliveryTurn:
        return DeliveryTurn(
            run_id=run.id,
            cycle_id=cycle.id,
            generation=generation,
            correlation_id=f"turn-{generation}",
            active_run_id=run.id,
            purpose="code",
            trigger_kind="plan_approved",
            trigger_payload={},
            prompt_payload={},
            prompt_hash=_HASH,
            status="queued",
            task_id=task.id,
        )

    db_session.add_all([turn(1), turn(2)])
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_terminal_turn_must_release_active_slot(db_session):
    run = await _run(db_session)
    cycle = _cycle(run.id, 1)
    task = Task(
        title="developer",
        mode="delivery_loop",
        delivery_run_id=run.id,
        delivery_role="developer",
    )
    db_session.add_all([cycle, task])
    await db_session.flush()
    db_session.add(
        DeliveryTurn(
            run_id=run.id,
            cycle_id=cycle.id,
            generation=1,
            correlation_id="turn-terminal-owner",
            active_run_id=run.id,
            purpose="code",
            trigger_kind="plan_approved",
            trigger_payload={},
            prompt_payload={},
            prompt_hash=_HASH,
            status="completed",
            task_id=task.id,
        )
    )

    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_delivery_event_source_receipt_is_idempotent(db_session):
    run = await _run(db_session)

    def event(sequence: int) -> DeliveryEvent:
        return DeliveryEvent(
            run_id=run.id,
            sequence=sequence,
            source="github",
            source_event_id="delivery-123",
            event_type="pull_request.synchronize",
            payload={},
            payload_hash=_HASH,
        )

    db_session.add_all([event(1), event(2)])
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_delivery_event_sequence_is_unique_per_run(db_session):
    run = await _run(db_session)
    db_session.add_all(
        [
            DeliveryEvent(
                run_id=run.id,
                sequence=1,
                source="controller",
                source_event_id="one",
                event_type="plan_ready",
                payload={},
                payload_hash=_HASH,
            ),
            DeliveryEvent(
                run_id=run.id,
                sequence=1,
                source="controller",
                source_event_id="two",
                event_type="code_started",
                payload={},
                payload_hash=_HASH,
            ),
        ]
    )

    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_delivery_actions_serialize_external_effects_per_run(db_session):
    run = await _run(db_session)

    def action(kind: str) -> DeliveryAction:
        return DeliveryAction(
            run_id=run.id,
            active_run_id=run.id,
            action_type=kind,
            idempotency_key=f"{run.id}:{kind}:1",
            desired_version=1,
            payload={},
            payload_hash=_HASH,
            status="pending",
        )

    db_session.add_all([action("push_branch"), action("ensure_pull_request")])
    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_delivery_action_publish_key_supports_sha256_git_oids(db_session):
    run = await _run(db_session)
    base_sha = "a" * 64
    head_sha = "b" * 64
    publish_key = f"delivery:{run.id}:publish:{base_sha}:{head_sha}"

    assert len(publish_key) == 148
    assert DeliveryAction.__table__.c.idempotency_key.type.length == 191

    action = DeliveryAction(
        run_id=run.id,
        active_run_id=run.id,
        action_type="ensure_pull_request",
        idempotency_key=publish_key,
        desired_version=1,
        expected_base_sha=base_sha,
        expected_head_sha=head_sha,
        payload={},
        payload_hash=_HASH,
        status="pending",
    )
    db_session.add(action)
    await db_session.commit()
    await db_session.refresh(action)

    assert action.idempotency_key == publish_key


@pytest.mark.asyncio
async def test_terminal_action_must_release_active_slot(db_session):
    run = await _run(db_session)
    db_session.add(
        DeliveryAction(
            run_id=run.id,
            active_run_id=run.id,
            action_type="ensure_pull_request",
            idempotency_key=f"{run.id}:ensure_pull_request:1",
            desired_version=1,
            payload={},
            payload_hash=_HASH,
            status="succeeded",
        )
    )

    with pytest.raises(IntegrityError):
        await db_session.commit()


@pytest.mark.asyncio
async def test_transition_version_is_append_only_per_run(db_session):
    run = await _run(db_session)

    def transition(cause: str) -> DeliveryTransition:
        return DeliveryTransition(
            run_id=run.id,
            state_version=2,
            cause=cause,
            actor_kind="controller",
            before_state={"phase": "planning", "activity": "ready"},
            after_state={"phase": "planning", "activity": "waiting"},
        )

    db_session.add_all([transition("one"), transition("two")])
    with pytest.raises(IntegrityError):
        await db_session.commit()
