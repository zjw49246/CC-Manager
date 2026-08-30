"""Public Capability API contract and ACL tests."""

from datetime import datetime
from unittest.mock import AsyncMock

from fastapi import HTTPException
import pytest
from sqlalchemy import func, select

from backend.config import settings
from backend.models.capability import CapabilityInvocation
from backend.models.code_review import CodeReviewResult, CodeReviewRun
from backend.models.plan import Plan, PlanVersion
from backend.models.plan_agent import PlanAgentRun
from backend.models.task import Task
from backend.services import capability_service, plan_capability
from backend.services.capability_registry import (
    CapabilityDefinition,
    register_capability,
    unregister_capability,
)
from backend.services.capability_service import create_controller_invocation


@pytest.fixture(autouse=True)
def capability_runtime():
    # Import-time production registration uses replace=True.  Materialize it
    # before installing this test adapter so fixture ordering with `client`
    # cannot silently replace the fake after setup.
    import backend.main  # noqa: F401

    previous = settings.capability_core_enabled
    settings.capability_core_enabled = True
    unregister_capability("plan")
    unregister_capability("code_review")
    register_capability(
        CapabilityDefinition(
            capability_key="plan",
            executor_kind="fake_plan",
            executor_config={"secret_route": "must-not-leak"},
            policy_snapshot={"gate": "server-owned"},
            max_attempts=2,
        )
    )
    register_capability(
        CapabilityDefinition(
            capability_key="code_review",
            executor_kind="fake_review",
            executor_config={},
            policy_snapshot={"gate": "server-owned"},
            max_attempts=1,
        )
    )
    yield
    unregister_capability("plan")
    unregister_capability("code_review")
    settings.capability_core_enabled = previous


async def _task(session_factory, **values) -> Task:
    async with session_factory() as db:
        task = Task(title="API capability target", **values)
        db.add(task)
        await db.commit()
        return task


def _body(**overrides):
    return {
        "capability": "plan",
        "request": {"prompt": "propose a safe plan"},
        "idempotency_key": "api-request-1",
        **overrides,
    }


async def _complete_fake_output(
    session_factory,
    *,
    invocation_id: int,
    output_kind: str = "plan_version",
    output_id: int = 123,
    output_hash: str = "a" * 64,
):
    async with session_factory() as db:
        invocation = await capability_service.get_invocation(db, invocation_id)
        execution = await capability_service.active_execution_for(
            db,
            invocation.id,
        )
        assert execution is not None
        invocation, execution = await capability_service.claim_execution(
            db,
            invocation_id=invocation.id,
            expected_invocation_version=invocation.state_version,
            expected_execution_version=execution.state_version,
            handle_kind="fake_run",
            handle_id=f"fake-{invocation.id}",
        )
        return await capability_service.complete_execution(
            db,
            invocation_id=invocation.id,
            expected_invocation_version=invocation.state_version,
            expected_execution_version=execution.state_version,
            output_kind=output_kind,
            output_id=output_id,
            output_hash=output_hash,
        )


async def _complete_plan_output(
    session_factory,
    *,
    invocation_id: int,
    sibling_output: bool = False,
) -> dict[str, int]:
    """Publish a Plan tuple whose Execution handle remains independently set."""

    async with session_factory() as db:
        invocation = await capability_service.get_invocation(db, invocation_id)
        execution = await capability_service.active_execution_for(
            db,
            invocation.id,
        )
        assert execution is not None
        owned_plan = Plan(
            title="Execution-owned Plan",
            initial_request="Plan the exact task",
            target_task_id=invocation.task_id,
            pipeline_config={},
        )
        sibling_plan = Plan(
            title="Sibling Plan",
            initial_request="A different Plan for the same task",
            target_task_id=invocation.task_id,
            pipeline_config={},
        )
        db.add_all([owned_plan, sibling_plan])
        await db.flush()
        owned_run = PlanAgentRun(
            plan_id=owned_plan.id,
            capability_execution_id=execution.id,
            run_type="capability",
            status="completed",
            current_stage="complete",
            finished_at=datetime.utcnow(),
        )
        sibling_run = PlanAgentRun(
            plan_id=sibling_plan.id,
            run_type="legacy",
            status="completed",
            current_stage="complete",
            finished_at=datetime.utcnow(),
        )
        db.add_all([owned_run, sibling_run])
        await db.flush()
        owned_version = PlanVersion(
            plan_id=owned_plan.id,
            version_number=1,
            produced_by_run_id=owned_run.id,
            content="# Exact execution-owned Plan",
        )
        sibling_version = PlanVersion(
            plan_id=sibling_plan.id,
            version_number=1,
            produced_by_run_id=sibling_run.id,
            content="# Same-task sibling Plan",
        )
        db.add_all([owned_version, sibling_version])
        await db.flush()
        owned_run.result_version_id = owned_version.id
        sibling_run.result_version_id = sibling_version.id
        owned_plan.current_version_id = owned_version.id
        sibling_plan.current_version_id = sibling_version.id
        await db.commit()

        output = sibling_version if sibling_output else owned_version
        invocation, execution = await capability_service.claim_execution(
            db,
            invocation_id=invocation.id,
            expected_invocation_version=invocation.state_version,
            expected_execution_version=execution.state_version,
            handle_kind="plan_agent_run",
            handle_id=str(owned_run.id),
        )
        await capability_service.complete_execution(
            db,
            invocation_id=invocation.id,
            expected_invocation_version=invocation.state_version,
            expected_execution_version=execution.state_version,
            output_kind="plan_version",
            output_id=output.id,
            output_hash=plan_capability.plan_version_output_hash(output),
        )
        return {
            "owned_run_id": owned_run.id,
            "owned_version_id": owned_version.id,
            "output_version_id": output.id,
        }


@pytest.mark.asyncio
async def test_public_create_freezes_server_owned_contract(
    client,
    session_factory,
):
    task = await _task(session_factory)

    response = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["created"] is True
    invocation = payload["invocation"]
    assert invocation["source"] == "human_request"
    assert invocation["purpose"] == "advisory"
    assert invocation["resume_policy"] == "attach_only"
    assert invocation["executor_kind"] == "fake_plan"
    assert invocation["request_output_log_id"] is None
    assert invocation["request_native_turn_id"] is None
    assert "executor_config" not in invocation
    assert "policy_snapshot" not in invocation
    assert invocation["active_execution"]["status"] == "queued"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "forged_field,forged_value",
    [
        ("source", "delivery_controller"),
        ("purpose", "required_gate"),
        ("resume_policy", "controller"),
        ("executor_kind", "attacker"),
        ("executor_config", {"command": "unsafe"}),
        ("policy_snapshot", {"bypass": True}),
        ("subject_ref", {"task_id": 999}),
        ("result_hash", "0" * 64),
        ("request_reason", "forged terminal reason"),
        ("request_protocol_version", 1),
        ("request_output_hash", "0" * 64),
    ],
)
async def test_public_create_rejects_server_owned_fields(
    client,
    session_factory,
    forged_field,
    forged_value,
):
    task = await _task(session_factory)

    response = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(**{forged_field: forged_value}),
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_api_idempotent_replay_and_conflict(client, session_factory):
    task = await _task(session_factory)
    first = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )
    assert first.status_code == 201

    replay = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )
    assert replay.status_code == 200
    assert replay.json()["created"] is False
    assert (
        replay.json()["invocation"]["id"]
        == first.json()["invocation"]["id"]
    )

    conflict = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(request={"prompt": "different"}),
    )
    assert conflict.status_code == 409


@pytest.mark.asyncio
async def test_ready_result_can_be_consumed_and_releases_active_slot(
    client,
    session_factory,
):
    task = await _task(session_factory)
    created = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )
    invocation_id = created.json()["invocation"]["id"]
    invocation, _execution = await _complete_fake_output(
        session_factory,
        invocation_id=invocation_id,
    )

    stale = await client.post(
        f"/api/capability-invocations/{invocation_id}/consume",
        json={"expected_state_version": invocation.state_version - 1},
    )
    assert stale.status_code == 409

    consumed = await client.post(
        f"/api/capability-invocations/{invocation_id}/consume",
        json={"expected_state_version": invocation.state_version},
    )
    assert consumed.status_code == 200, consumed.text
    assert consumed.json()["status"] == "completed"

    second = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(idempotency_key="after-consume"),
    )
    assert second.status_code == 201, second.text
    assert second.json()["invocation"]["id"] != invocation_id


@pytest.mark.asyncio
async def test_ready_result_can_be_consumed_while_new_admission_is_disabled(
    client,
    session_factory,
):
    task = await _task(session_factory)
    created = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )
    invocation_id = created.json()["invocation"]["id"]
    invocation, _execution = await _complete_fake_output(
        session_factory,
        invocation_id=invocation_id,
    )
    settings.capability_core_enabled = False

    consumed = await client.post(
        f"/api/capability-invocations/{invocation_id}/consume",
        json={"expected_state_version": invocation.state_version},
    )
    assert consumed.status_code == 200, consumed.text
    assert consumed.json()["status"] == "completed"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["consume", "cancel"])
async def test_public_mutation_rejects_agent_resume_invocation(
    client,
    session_factory,
    operation,
):
    """Human advisory endpoints cannot consume or cancel Agent G+1 state."""

    from backend.models.capability import (
        CapabilityExecution,
        CapabilityResumeOutbox,
    )
    from backend.tests.test_api_tasks import (
        _seed_waiting_capability_for_terminal_api,
    )

    async with session_factory() as db:
        task_id, invocation_id, execution_id, outbox_id = (
            await _seed_waiting_capability_for_terminal_api(
                db,
                invocation_status="ready",
            )
        )
        stored = await capability_service.get_invocation(db, invocation_id)
        state_version = stored.state_version

    response = await client.post(
        f"/api/capability-invocations/{invocation_id}/{operation}",
        json={"expected_state_version": state_version},
    )

    assert response.status_code == 409, response.text
    assert "Workflow-owned" in response.json()["detail"]
    async with session_factory() as db:
        stored = await capability_service.get_invocation(db, invocation_id)
        current_task = await db.get(Task, task_id)
        execution = await db.get(CapabilityExecution, execution_id)
        outbox = await db.get(CapabilityResumeOutbox, outbox_id)
    assert stored.status == "ready"
    assert stored.state_version == state_version
    assert stored.active_task_id == task_id
    assert current_task.status == "waiting_capability"
    assert execution.status == "completed"
    assert outbox.status == "pending"


@pytest.mark.asyncio
async def test_code_review_result_is_readable_through_parent_task_acl(
    client,
    session_factory,
):
    task = await _task(session_factory)
    created = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(
            capability="code_review",
            idempotency_key="public-review-result",
        ),
    )
    assert created.status_code == 201, created.text
    invocation_id = created.json()["invocation"]["id"]
    output_hash = "c" * 64

    async with session_factory() as db:
        invocation = await capability_service.get_invocation(db, invocation_id)
        execution = await capability_service.active_execution_for(
            db,
            invocation.id,
        )
        assert execution is not None
        invocation, execution = await capability_service.claim_execution(
            db,
            invocation_id=invocation.id,
            expected_invocation_version=invocation.state_version,
            expected_execution_version=execution.state_version,
            handle_kind="fake_review_run",
            handle_id=f"review-{invocation.id}",
        )
        reviewer = Task(
            title="Exact reviewer",
            status="completed",
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
        )
        db.add(reviewer)
        await db.flush()
        run = CodeReviewRun(
            capability_invocation_id=invocation.id,
            capability_execution_id=execution.id,
            attempt=1,
            status="completed",
            state_version=2,
            developer_task_id=task.id,
            reviewer_task_id=reviewer.id,
            reviewer_task_retry_count=0,
            repo_path="/repo",
            base_sha="a" * 40,
            head_sha="b" * 40,
            head_tree_sha="d" * 40,
            patch_sha256="e" * 64,
            subject_ref={"kind": "commit_range", "head_sha": "b" * 40},
            subject_hash="f" * 64,
            prompt_hash="1" * 64,
            completed_at=datetime.utcnow(),
        )
        db.add(run)
        await db.flush()
        result = CodeReviewResult(
            run_id=run.id,
            capability_invocation_id=invocation.id,
            capability_execution_id=execution.id,
            developer_task_id=task.id,
            reviewer_task_id=reviewer.id,
            reviewer_task_retry_count=0,
            reviewer_task_instance_id=None,
            reviewer_task_started_at=reviewer.started_at,
            reviewer_task_completed_at=reviewer.completed_at,
            output_log_id=1,
            schema_version=1,
            role="reviewer",
            verdict="changes_requested",
            summary="One exact finding",
            findings=[{"severity": "high", "title": "Fix it"}],
            subject_ref=run.subject_ref,
            subject_hash=run.subject_hash,
            result_hash=output_hash,
        )
        db.add(result)
        await db.flush()
        result_id = result.id
        await capability_service.complete_execution(
            db,
            invocation_id=invocation.id,
            expected_invocation_version=invocation.state_version,
            expected_execution_version=execution.state_version,
            output_kind="code_review_result",
            output_id=result.id,
            output_hash=output_hash,
        )

    response = await client.get(
        f"/api/capability-invocations/{invocation_id}/result"
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["kind"] == "code_review_result"
    assert payload["id"] == result_id
    assert payload["hash"] == output_hash
    assert payload["data"]["verdict"] == "changes_requested"
    assert payload["data"]["findings"] == [
        {"severity": "high", "title": "Fix it"}
    ]


@pytest.mark.asyncio
async def test_plan_result_rejects_same_task_sibling_run_version(
    client,
    session_factory,
):
    task = await _task(session_factory)
    created = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(idempotency_key="sibling-plan-version"),
    )
    assert created.status_code == 201, created.text
    invocation_id = created.json()["invocation"]["id"]
    graph = await _complete_plan_output(
        session_factory,
        invocation_id=invocation_id,
        sibling_output=True,
    )
    assert graph["output_version_id"] != graph["owned_version_id"]

    response = await client.get(
        f"/api/capability-invocations/{invocation_id}/result"
    )

    assert response.status_code == 409
    assert "identity" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_plan_result_reads_exact_execution_run_version_chain(
    client,
    session_factory,
):
    task = await _task(session_factory)
    created = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(idempotency_key="exact-plan-version"),
    )
    assert created.status_code == 201, created.text
    invocation_id = created.json()["invocation"]["id"]
    graph = await _complete_plan_output(
        session_factory,
        invocation_id=invocation_id,
    )

    response = await client.get(
        f"/api/capability-invocations/{invocation_id}/result"
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["kind"] == "plan_version"
    assert payload["id"] == graph["owned_version_id"]
    assert payload["data"]["content"] == "# Exact execution-owned Plan"


@pytest.mark.asyncio
async def test_plan_result_recomputes_authoritative_version_hash(
    client,
    session_factory,
):
    task = await _task(session_factory)
    created = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(idempotency_key="tampered-plan-version"),
    )
    assert created.status_code == 201, created.text
    invocation_id = created.json()["invocation"]["id"]
    graph = await _complete_plan_output(
        session_factory,
        invocation_id=invocation_id,
    )
    async with session_factory() as db:
        version = await db.get(PlanVersion, graph["output_version_id"])
        assert version is not None
        version.content = "# Mutated after Capability completion"
        await db.commit()

    response = await client.get(
        f"/api/capability-invocations/{invocation_id}/result"
    )

    assert response.status_code == 409
    assert "hash" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_api_rejects_invalid_key_and_oversized_request(
    client,
    session_factory,
):
    task = await _task(session_factory)
    invalid_key = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(capability="BAD key"),
    )
    oversized = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(
            request={"text": "界" * 20_000},
            idempotency_key="oversized",
        ),
    )
    assert invalid_key.status_code == 422
    assert oversized.status_code == 422


@pytest.mark.asyncio
async def test_flag_off_keeps_read_cancel_and_replay_available(
    client,
    session_factory,
):
    task = await _task(session_factory)
    created = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )
    invocation = created.json()["invocation"]
    settings.capability_core_enabled = False

    replay = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )
    assert replay.status_code == 200
    blocked = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(idempotency_key="new-disabled-request"),
    )
    assert blocked.status_code == 503

    listed = await client.get(
        f"/api/tasks/{task.id}/capability-invocations"
    )
    assert listed.status_code == 200
    assert [item["id"] for item in listed.json()] == [invocation["id"]]
    read = await client.get(
        f"/api/capability-invocations/{invocation['id']}"
    )
    assert read.status_code == 200

    cancelled = await client.post(
        f"/api/capability-invocations/{invocation['id']}/cancel",
        json={"expected_state_version": invocation["state_version"]},
    )
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"
    repeated = await client.post(
        f"/api/capability-invocations/{invocation['id']}/cancel",
        json={
            "expected_state_version": cancelled.json()["state_version"],
        },
    )
    assert repeated.status_code == 200
    assert repeated.json()["status"] == "cancelled"


@pytest.mark.asyncio
async def test_remote_and_shared_tasks_are_rejected(client, session_factory):
    remote = await _task(session_factory, worker_id=17)
    shared = await _task(session_factory, shared_from_id=18)

    remote_response = await client.post(
        f"/api/tasks/{remote.id}/capability-invocations",
        json=_body(idempotency_key="remote"),
    )
    shared_response = await client.post(
        f"/api/tasks/{shared.id}/capability-invocations",
        json=_body(idempotency_key="shared"),
    )

    assert remote_response.status_code == 409
    assert shared_response.status_code == 409


@pytest.mark.asyncio
async def test_delivery_task_rejects_human_create_cancel_and_consume(
    client,
    session_factory,
):
    task = await _task(
        session_factory,
        mode="delivery_loop",
        status="delivery_waiting",
        delivery_run_id=91,
        delivery_role="developer",
    )

    created_by_human = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(idempotency_key="delivery-human-bypass"),
    )
    assert created_by_human.status_code == 409

    async with session_factory() as db:
        invocation, _created = await create_controller_invocation(
            db,
            task_id=task.id,
            capability_key="plan",
            request_payload={"prompt": "controller-owned plan"},
            idempotency_key="delivery-controller-plan",
        )
        invocation_id = invocation.id
        state_version = invocation.state_version

    listed = await client.get(
        f"/api/tasks/{task.id}/capability-invocations"
    )
    assert listed.status_code == 200
    assert [item["id"] for item in listed.json()] == [invocation_id]

    cancelled = await client.post(
        f"/api/capability-invocations/{invocation_id}/cancel",
        json={"expected_state_version": state_version},
    )
    assert cancelled.status_code == 409
    consumed = await client.post(
        f"/api/capability-invocations/{invocation_id}/consume",
        json={"expected_state_version": state_version},
    )
    assert consumed.status_code == 409


@pytest.mark.asyncio
async def test_create_and_cancel_require_task_control(
    client,
    session_factory,
    monkeypatch,
):
    from backend.api import capabilities as api

    task = await _task(session_factory)
    denied = AsyncMock(side_effect=HTTPException(403, "denied"))
    monkeypatch.setattr(api, "require_task_control", denied)

    response = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )
    assert response.status_code == 403
    denied.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_rechecks_task_control_inside_final_transaction(
    client,
    session_factory,
    monkeypatch,
):
    from backend.api import capabilities as api

    task = await _task(session_factory)
    revoked = AsyncMock(
        side_effect=[None, HTTPException(403, "control was revoked")]
    )
    monkeypatch.setattr(api, "require_task_control", revoked)

    response = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(idempotency_key="revoked-before-capability-commit"),
    )

    assert response.status_code == 403
    assert revoked.await_count == 2
    async with session_factory() as db:
        assert await db.scalar(
            select(func.count(CapabilityInvocation.id))
        ) == 0


@pytest.mark.asyncio
async def test_create_uses_durable_task_effect_fence_before_commit(
    client,
    session_factory,
    monkeypatch,
):
    """A final ACL/role fence failure cannot publish an Invocation."""

    from backend.api import capabilities as api

    task = await _task(session_factory)
    denied = AsyncMock(
        side_effect=HTTPException(409, "authority changed before admission")
    )
    monkeypatch.setattr(api, "lock_task_effect_access", denied)

    response = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(idempotency_key="capability-final-effect-fence"),
    )

    assert response.status_code == 409
    denied.assert_awaited_once()
    async with session_factory() as db:
        assert await db.scalar(
            select(func.count(CapabilityInvocation.id))
        ) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["cancel", "consume"])
async def test_public_transition_rechecks_control_inside_final_transaction(
    client,
    session_factory,
    monkeypatch,
    operation,
):
    from backend.api import capabilities as api

    task = await _task(session_factory)
    created = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(idempotency_key=f"revoke-before-{operation}"),
    )
    invocation = created.json()["invocation"]
    if operation == "consume":
        completed, _execution = await _complete_fake_output(
            session_factory,
            invocation_id=invocation["id"],
        )
        expected_version = completed.state_version
        expected_status = "ready"
    else:
        expected_version = invocation["state_version"]
        expected_status = "queued"

    revoked = AsyncMock(
        side_effect=[None, HTTPException(403, "control was revoked")]
    )
    monkeypatch.setattr(api, "require_task_control", revoked)
    response = await client.post(
        f"/api/capability-invocations/{invocation['id']}/{operation}",
        json={"expected_state_version": expected_version},
    )

    assert response.status_code == 403
    assert revoked.await_count == 2
    async with session_factory() as db:
        stored = await db.get(CapabilityInvocation, invocation["id"])
        assert stored.status == expected_status


@pytest.mark.asyncio
async def test_cancel_and_consume_require_task_control(
    client,
    session_factory,
    monkeypatch,
):
    from backend.api import capabilities as api

    task = await _task(session_factory)
    created = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )
    invocation_id = created.json()["invocation"]["id"]
    invocation, _execution = await _complete_fake_output(
        session_factory,
        invocation_id=invocation_id,
    )
    denied = AsyncMock(side_effect=HTTPException(403, "denied"))
    monkeypatch.setattr(api, "require_task_control", denied)

    cancelled = await client.post(
        f"/api/capability-invocations/{invocation_id}/cancel",
        json={"expected_state_version": invocation.state_version},
    )
    consumed = await client.post(
        f"/api/capability-invocations/{invocation_id}/consume",
        json={"expected_state_version": invocation.state_version},
    )
    assert cancelled.status_code == 403
    assert consumed.status_code == 403
    assert denied.await_count == 2


@pytest.mark.asyncio
async def test_read_and_list_require_task_control(
    client,
    session_factory,
    monkeypatch,
):
    from backend.api import capabilities as api

    task = await _task(session_factory)
    created = await client.post(
        f"/api/tasks/{task.id}/capability-invocations",
        json=_body(),
    )
    invocation_id = created.json()["invocation"]["id"]
    denied = AsyncMock(side_effect=HTTPException(403, "denied"))
    monkeypatch.setattr(api, "require_task_control", denied)

    listed = await client.get(
        f"/api/tasks/{task.id}/capability-invocations"
    )
    read = await client.get(
        f"/api/capability-invocations/{invocation_id}"
    )
    result = await client.get(
        f"/api/capability-invocations/{invocation_id}/result"
    )
    assert listed.status_code == 403
    assert read.status_code == 403
    assert result.status_code == 403
    assert denied.await_count == 3
