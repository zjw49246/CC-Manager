"""API isolation for Plans owned by the Plan Capability adapter."""

from datetime import datetime

from fastapi import HTTPException
import pytest
from sqlalchemy import func, select

from backend.models.capability import CapabilityExecution, CapabilityInvocation
from backend.models.plan import (
    Plan,
    PlanApplication,
    PlanApplicationReceipt,
    PlanInputRequest,
    PlanVersion,
)
from backend.models.plan_agent import PlanAgentRun
from backend.models.task import Task
from backend.schemas.plan import default_plan_pipeline_config
from backend.services.plan_service import resolve_uncertain_plan_application


READ_ONLY_DETAIL = {
    "code": "capability_owned_plan_read_only",
    "message": "Capability-owned Plans can only be mutated by Capability Core",
}


async def _seed_waiting_capability_plan(session_factory) -> dict[str, int]:
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        task = Task(
            title="Capability target",
            description="Plan this task",
            target_repo="/tmp",
            target_branch="main",
        )
        db.add(task)
        await db.flush()
        invocation = CapabilityInvocation(
            task_id=task.id,
            capability_key="plan",
            source="human_request",
            purpose="advisory",
            status="waiting_user",
            state_version=1,
            idempotency_key=f"capability-plan-{task.id}",
            input_payload={"prompt": "Plan the implementation"},
            input_hash="input-hash",
            subject_kind="task_generation",
            subject_ref={"task_id": task.id},
            subject_hash="subject-hash",
            executor_kind="plan_agent",
            executor_config={},
            executor_config_hash="executor-hash",
            policy_snapshot={},
            policy_hash="policy-hash",
            resume_policy="attach_only",
            max_attempts=1,
            active_task_id=task.id,
        )
        db.add(invocation)
        await db.flush()
        execution = CapabilityExecution(
            invocation_id=invocation.id,
            attempt=1,
            status="waiting_user",
            state_version=1,
            active_invocation_id=invocation.id,
            idempotency_key=f"{invocation.id}:1",
            executor_kind="plan_agent",
            input_hash=invocation.input_hash,
        )
        db.add(execution)
        await db.flush()
        plan = Plan(
            title="Capability-owned Plan",
            initial_request="Plan the implementation",
            target_task_id=task.id,
            target_repo="/tmp",
            target_branch="main",
            priority=0,
            pipeline_config=pipeline,
        )
        db.add(plan)
        await db.flush()
        version = PlanVersion(
            plan_id=plan.id,
            version_number=1,
            content="# Reviewed capability Plan",
            review_verdict="approve",
            reviewed_at=datetime.utcnow(),
            human_decision="pending",
        )
        db.add(version)
        await db.flush()
        run = PlanAgentRun(
            plan_id=plan.id,
            run_type="capability",
            capability_execution_id=execution.id,
            request_text=plan.initial_request,
            pipeline_config=pipeline,
            status="waiting_user",
            current_stage="planner",
            generation=3,
            max_interactions=3,
        )
        db.add(run)
        await db.flush()
        input_request = PlanInputRequest(
            plan_id=plan.id,
            run_id=run.id,
            source_step_id=1,
            requested_by="planner",
            questions=[
                {
                    "id": "scope",
                    "header": "Scope",
                    "question": "Which scope should be used?",
                    "response_type": "text",
                    "options": [],
                    "required": True,
                }
            ],
            status="open",
            idempotency_key=f"capability-input-{run.id}",
            opened_at=datetime.utcnow(),
        )
        db.add(input_request)
        await db.flush()
        plan.current_version_id = version.id
        plan.active_run_id = run.id
        run.open_input_request_id = input_request.id
        execution.handle_kind = "plan_agent_run"
        execution.handle_id = str(run.id)
        execution.handle_generation = 0
        await db.commit()
        return {
            "plan_id": plan.id,
            "version_id": version.id,
            "run_id": run.id,
            "input_request_id": input_request.id,
        }


def _assert_read_only(response) -> None:
    assert response.status_code == 409, response.text
    assert response.json() == {"detail": READ_ONLY_DETAIL}


@pytest.mark.asyncio
async def test_capability_owned_plan_blocks_generic_writes_but_keeps_reads_and_input(
    client,
    session_factory,
):
    ids = await _seed_waiting_capability_plan(session_factory)
    plan_id = ids["plan_id"]
    version_id = ids["version_id"]
    run_id = ids["run_id"]

    cases = [
        (
            "PATCH",
            f"/api/plans/{plan_id}",
            {"title": "Generic writer", "expected_lock_version": 0},
        ),
        (
            "POST",
            f"/api/plans/{plan_id}/runs",
            {
                "run_type": "user_revision",
                "request": "Generic revision",
                "expected_current_version_id": version_id,
            },
        ),
        (
            "POST",
            f"/api/plans/{plan_id}/fork",
            {"base_version_id": version_id},
        ),
        (
            "POST",
            f"/api/plan-versions/{version_id}/approve",
            {"expected_current_version_id": version_id},
        ),
        (
            "POST",
            f"/api/plan-versions/{version_id}/reject",
            {"expected_current_version_id": version_id},
        ),
        ("POST", f"/api/plan-runs/{run_id}/cancel", None),
        (
            "POST",
            f"/api/plan-versions/{version_id}/create-execution-task",
            {
                "expected_current_version_id": version_id,
                "approve_if_pending": True,
            },
        ),
    ]
    for method, url, payload in cases:
        kwargs = {"json": payload} if payload is not None else {}
        _assert_read_only(await client.request(method, url, **kwargs))

    read_urls = [
        f"/api/plans/{plan_id}",
        f"/api/plans/{plan_id}/versions",
        f"/api/plans/{plan_id}/runs",
        f"/api/plan-versions/{version_id}",
        f"/api/plan-runs/{run_id}",
    ]
    for url in read_urls:
        response = await client.get(url)
        assert response.status_code == 200, response.text

    resource = (await client.get(f"/api/plans/{plan_id}")).json()
    assert resource["ownership"] == "capability"
    assert resource["read_only"] is True
    assert resource["display_state"] == "waiting_user"
    assert resource["active_run"]["run_type"] == "capability"
    assert resource["open_input_request"]["id"] == ids["input_request_id"]

    answered = await client.post(
        f"/api/plan-runs/{run_id}/input-requests/"
        f"{ids['input_request_id']}/answer",
        json={
            "expected_run_generation": 3,
            "idempotency_key": "capability-answer",
            "answers": [{"question_id": "scope", "value": "repository"}],
        },
    )
    assert answered.status_code == 200, answered.text
    assert answered.json()["status"] == "answered"

    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        run = await db.get(PlanAgentRun, run_id)
        version = await db.get(PlanVersion, version_id)
        assert plan.title == "Capability-owned Plan"
        assert plan.lock_version == 0
        assert plan.active_run_id == run.id
        assert run.status == "queued"
        assert run.generation == 4
        assert run.open_input_request_id is None
        assert version.human_decision == "pending"
        assert await db.scalar(select(func.count(Plan.id))) == 1
        assert await db.scalar(select(func.count(PlanAgentRun.id))) == 1
        assert await db.scalar(select(func.count(PlanApplication.id))) == 0
        assert await db.scalar(select(func.count(Task.id))) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("already_answered", [False, True])
async def test_capability_owned_plan_rejects_ordinary_active_run_input_answer(
    already_answered,
    client,
    session_factory,
):
    """Historical Capability ownership cannot be bypassed by an ordinary Run."""

    ids = await _seed_waiting_capability_plan(session_factory)
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        plan = await db.get(Plan, ids["plan_id"])
        capability_run = await db.get(PlanAgentRun, ids["run_id"])
        capability_input = await db.get(PlanInputRequest, ids["input_request_id"])
        capability_run.status = "completed"
        capability_run.current_stage = "complete"
        capability_run.open_input_request_id = None
        capability_run.finished_at = datetime.utcnow()
        capability_input.status = "cancelled"
        capability_input.cancelled_at = datetime.utcnow()

        ordinary_run = PlanAgentRun(
            plan_id=plan.id,
            run_type="user_revision",
            request_text="Malformed ordinary continuation",
            pipeline_config=pipeline,
            status="waiting_user",
            current_stage="planner",
            generation=7,
            max_interactions=3,
        )
        db.add(ordinary_run)
        await db.flush()
        ordinary_input = PlanInputRequest(
            plan_id=plan.id,
            run_id=ordinary_run.id,
            source_step_id=2,
            requested_by="planner",
            questions=[
                {
                    "id": "scope",
                    "header": "Scope",
                    "question": "Which scope should be used?",
                    "response_type": "text",
                    "options": [],
                    "required": True,
                }
            ],
            status="answered" if already_answered else "open",
            answers=(
                [{"question_id": "scope", "value": "existing"}]
                if already_answered
                else None
            ),
            idempotency_key=f"ordinary-input-{ordinary_run.id}",
            answer_idempotency_key=(
                "ordinary-answer" if already_answered else None
            ),
            opened_at=datetime.utcnow(),
            answered_at=datetime.utcnow() if already_answered else None,
        )
        db.add(ordinary_input)
        await db.flush()
        ordinary_run.open_input_request_id = ordinary_input.id
        plan.active_run_id = ordinary_run.id
        await db.commit()
        ordinary_run_id = ordinary_run.id
        ordinary_input_id = ordinary_input.id

    response = await client.post(
        f"/api/plan-runs/{ordinary_run_id}/input-requests/"
        f"{ordinary_input_id}/answer",
        json={
            "expected_run_generation": 7,
            "idempotency_key": "ordinary-answer",
            "answers": [{"question_id": "scope", "value": "repository"}],
        },
    )

    _assert_read_only(response)
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, ordinary_run_id)
        input_request = await db.get(PlanInputRequest, ordinary_input_id)
        assert run.status == "waiting_user"
        assert run.generation == 7
        assert run.open_input_request_id == ordinary_input_id
        assert input_request.status == (
            "answered" if already_answered else "open"
        )
        assert input_request.answers == (
            [{"question_id": "scope", "value": "existing"}]
            if already_answered
            else None
        )
        assert input_request.answer_idempotency_key == (
            "ordinary-answer" if already_answered else None
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("marker", ["run_type", "execution_id"])
async def test_capability_ownership_scans_deep_historical_runs(
    marker,
    client,
    session_factory,
):
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        plan = Plan(
            title=f"Historical marker {marker}",
            initial_request="Retain durable ownership",
            pipeline_config=pipeline,
            priority=0,
        )
        db.add(plan)
        await db.flush()
        owner = PlanAgentRun(
            plan_id=plan.id,
            run_type="capability" if marker == "run_type" else "initial",
            capability_execution_id=900_001 if marker == "execution_id" else None,
            request_text="Capability-owned history",
            pipeline_config=pipeline,
            status="completed",
            current_stage="complete",
            finished_at=datetime.utcnow(),
        )
        db.add(owner)
        await db.flush()
        db.add_all(
            [
                PlanAgentRun(
                    plan_id=plan.id,
                    run_type="user_revision",
                    request_text=f"Ordinary history {index}",
                    pipeline_config=pipeline,
                    status="completed",
                    current_stage="complete",
                    finished_at=datetime.utcnow(),
                )
                for index in range(250)
            ]
        )
        await db.commit()
        plan_id = plan.id

    response = await client.patch(
        f"/api/plans/{plan_id}",
        json={"title": "Must not change", "expected_lock_version": 0},
    )
    _assert_read_only(response)
    resource = await client.get(f"/api/plans/{plan_id}")
    assert resource.status_code == 200, resource.text
    assert resource.json()["ownership"] == "capability"
    assert resource.json()["read_only"] is True
    async with session_factory() as db:
        plan = await db.get(Plan, plan_id)
        assert plan.title == f"Historical marker {marker}"
        assert (
            await db.scalar(
                select(func.count(PlanAgentRun.id)).where(
                    PlanAgentRun.plan_id == plan_id
                )
            )
            == 251
        )


@pytest.mark.asyncio
async def test_capability_cancelling_state_matches_detail_and_collection_projection(
    client,
    session_factory,
):
    ids = await _seed_waiting_capability_plan(session_factory)
    async with session_factory() as db:
        run = await db.get(PlanAgentRun, ids["run_id"])
        run.cancellation_target_generation = run.generation
        run.generation += 1
        run.status = "cancelling"
        run.open_input_request_id = None
        await db.commit()

    detail = await client.get(f"/api/plans/{ids['plan_id']}")
    assert detail.status_code == 200, detail.text
    assert detail.json()["display_state"] == "cancelling"
    assert detail.json()["active_run"]["status"] == "cancelling"
    assert detail.json()["read_only"] is True

    collection = await client.get(
        "/api/plans",
        params={"display_state": "cancelling"},
    )
    assert collection.status_code == 200, collection.text
    assert [row["id"] for row in collection.json()] == [ids["plan_id"]]
    assert collection.json()[0]["display_state"] == "cancelling"


@pytest.mark.asyncio
async def test_capability_active_run_reverse_link_fails_closed(
    client,
    session_factory,
):
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    async with session_factory() as db:
        plan = Plan(
            title="Malformed reverse link",
            initial_request="Do not trust only run.plan_id",
            pipeline_config=pipeline,
            priority=0,
        )
        owner = Plan(
            title="Actual run owner",
            initial_request="Capability owner",
            pipeline_config=pipeline,
            priority=0,
        )
        db.add_all([plan, owner])
        await db.flush()
        run = PlanAgentRun(
            plan_id=owner.id,
            run_type="capability",
            request_text="Capability Run",
            pipeline_config=pipeline,
            status="queued",
            current_stage="planner",
        )
        db.add(run)
        await db.flush()
        plan.active_run_id = run.id
        await db.commit()
        plan_id = plan.id

    response = await client.patch(
        f"/api/plans/{plan_id}",
        json={"title": "Must not change", "expected_lock_version": 0},
    )
    _assert_read_only(response)


@pytest.mark.asyncio
async def test_worker_plan_writers_cannot_repurpose_capability_owned_plan(
    client,
    session_factory,
):
    ids = await _seed_waiting_capability_plan(session_factory)
    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    imported = await client.post(
        "/api/plans/worker-import",
        json={
            "protocol": 3,
            "plan_id": ids["plan_id"],
            "run_id": ids["run_id"],
            "manager_claim_generation": 0,
            "title": "Capability-owned Plan",
            "initial_request": "Plan the implementation",
            "priority": 0,
            "pipeline_config": pipeline,
            "run_type": "capability",
            "request_text": "Plan the implementation",
            "max_interactions": 3,
        },
    )
    _assert_read_only(imported)

    materialized = await client.post(
        "/api/plans/worker-materialize-version",
        json={
            "protocol": 3,
            "plan_id": ids["plan_id"],
            "title": "Capability-owned Plan",
            "initial_request": "Plan the implementation",
            "priority": 0,
            "pipeline_config": pipeline,
            "version": {
                "source_version_id": ids["version_id"],
                "version_number": 2,
                "content": "# Foreign mutation",
            },
        },
    )
    _assert_read_only(materialized)


@pytest.mark.asyncio
async def test_application_resolution_rejects_capability_owned_plan(
    client,
    session_factory,
):
    ids = await _seed_waiting_capability_plan(session_factory)
    async with session_factory() as db:
        receipt = PlanApplicationReceipt(
            receipt_key="capability-receipt",
            target_task_id=12345,
            plan_version_ids=[ids["version_id"]],
            status="committed",
            delivery_status="uncertain",
        )
        db.add(receipt)
        await db.commit()

    scoped = await client.post(
        f"/api/plans/{ids['plan_id']}/application-deliveries/"
        "capability-receipt/resolve",
        json={"action": "confirm_launched", "note": "Checked evidence"},
    )
    _assert_read_only(scoped)

    async with session_factory() as db:
        with pytest.raises(HTTPException) as raised:
            await resolve_uncertain_plan_application(
                db,
                receipt_key="capability-receipt",
                action="confirm_launched",
                note="Worker checked evidence",
                actor_id=None,
            )
        assert raised.value.status_code == 409
        assert raised.value.detail == READ_ONLY_DETAIL
        receipt = (
            await db.execute(
                select(PlanApplicationReceipt).where(
                    PlanApplicationReceipt.receipt_key == "capability-receipt"
                )
            )
        ).scalar_one()
        assert receipt.delivery_status == "uncertain"
        assert receipt.delivery_resolution is None
