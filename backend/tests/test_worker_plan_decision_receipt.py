import copy
import uuid

import pytest
from fastapi import HTTPException
from starlette.requests import Request
from unittest.mock import AsyncMock

import backend.main as main_module
import backend.api.tasks as tasks_api_module
from backend.config import settings
from backend.models.task import Task
from backend.models.worker import Worker
from sqlalchemy import update
from backend.services.skill_context import WORKER_MANAGED_TASK_METADATA_KEY
from backend.services.test_harness_owner_fence import (
    TEST_HARNESS_TERMINAL_GATE_KEY,
)
from backend.services.worker_proxy import (
    WorkerProxy,
    WorkerTaskMutationOutcomeUncertainError,
)
from backend.services.worker_relay import (
    apply_authoritative_worker_task,
    has_worker_execution_quarantine,
    worker_task_generation,
)
from backend.services.worker_plan_decision import (
    WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD,
    WORKER_PLAN_DECISION_PROTOCOL,
    WORKER_PLAN_DECISION_RECEIPT_METADATA_KEY,
    worker_plan_decision_request_digest,
)
from backend.services.worker_node_control import begin_worker_node_drain


async def _worker_plan(
    session_factory,
    *,
    target_id: int | None = None,
) -> tuple[Task, Task | None]:
    async with session_factory() as db:
        target = None
        if target_id is None:
            target = Task(
                title="Worker decision target",
                description="target",
                status="completed",
                metadata_={WORKER_MANAGED_TASK_METADATA_KEY: True},
            )
            db.add(target)
            await db.flush()
            target_id = target.id
        plan = Task(
            title="Worker legacy Plan",
            description="plan",
            status="plan_review",
            mode="plan",
            plan_content="Apply the exact change",
            plan_target_task_id=target_id,
            metadata_={WORKER_MANAGED_TASK_METADATA_KEY: True},
        )
        db.add(plan)
        await db.commit()
        await db.refresh(plan)
        if target is not None:
            await db.refresh(target)
        return plan, target


def _decision_request(
    plan: Task,
    *,
    action: str,
    manager_worker_id: int = 41,
    target: Task | None = None,
) -> dict:
    operation_id = uuid.uuid4().hex
    request = {
        "protocol_version": WORKER_PLAN_DECISION_PROTOCOL,
        "operation_id": operation_id,
        "action": action,
        "task_id": plan.id,
        "manager_worker_id": manager_worker_id,
        "source_incarnation_id": plan.incarnation_id,
        "expected_status": "plan_review",
        "expected_retry_count": plan.retry_count,
        "expected_turn_generation": plan.turn_generation,
        "decision_path": f"/api/tasks/{plan.id}/plan/{action}",
        "routing": {
            "provider": plan.provider,
            "model": plan.model,
            "codex_service_tier": plan.codex_service_tier,
        },
        "decision_body": None,
        "plan_target_task_id": plan.plan_target_task_id,
        "plan_target_incarnation_id": (
            target.incarnation_id if target is not None else None
        ),
    }
    request["request_digest"] = worker_plan_decision_request_digest(request)
    return request


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "terminal_status", "approved"),
    [
        pytest.param("approve", "completed", True, id="approve"),
        pytest.param("reject", "cancelled", False, id="reject"),
    ],
)
async def test_worker_plan_decision_put_is_durable_and_idempotent(
    client,
    session_factory,
    action,
    terminal_status,
    approved,
):
    plan, target = await _worker_plan(session_factory)
    request = _decision_request(plan, action=action, target=target)
    path = (
        f"/api/tasks/{plan.id}/internal/worker-plan-decisions/"
        f"{request['operation_id']}"
    )
    headers = {"X-CCM-Task-Incarnation": plan.incarnation_id}

    absent = await client.get(path, headers=headers)
    assert absent.status_code == 200, absent.text
    assert absent.json() == {
        "protocol_version": WORKER_PLAN_DECISION_PROTOCOL,
        "state": "absent",
        "operation_id": request["operation_id"],
        "task_id": plan.id,
    }

    applied = await client.put(path, headers=headers, json=request)
    assert applied.status_code == 200, applied.text
    payload = applied.json()
    assert payload["task"]["status"] == terminal_status
    assert payload["task"]["plan_approved"] is approved
    assert payload["receipt"]["request"] == request
    assert payload["receipt"]["state"] == "applied"

    replay = await client.put(path, headers=headers, json=request)
    assert replay.status_code == 200, replay.text
    assert replay.json()["receipt"] == payload["receipt"]

    readback = await client.get(path, headers=headers)
    assert readback.status_code == 200, readback.text
    assert readback.json()["receipt"] == payload["receipt"]
    async with session_factory() as db:
        persisted = await db.get(Task, plan.id)
    assert persisted.status == terminal_status
    assert (
        persisted.metadata_[WORKER_PLAN_DECISION_RECEIPT_METADATA_KEY]
        == payload["receipt"]
    )


@pytest.mark.asyncio
async def test_worker_plan_decision_rejects_payload_and_operation_reuse(
    client,
    session_factory,
):
    plan, target = await _worker_plan(session_factory)
    request = _decision_request(plan, action="approve", target=target)
    path = (
        f"/api/tasks/{plan.id}/internal/worker-plan-decisions/"
        f"{request['operation_id']}"
    )
    headers = {"X-CCM-Task-Incarnation": plan.incarnation_id}
    applied = await client.put(path, headers=headers, json=request)
    assert applied.status_code == 200, applied.text

    changed = copy.deepcopy(request)
    changed["decision_body"] = {"confirm_stale": True}
    changed["request_digest"] = worker_plan_decision_request_digest(changed)
    conflict = await client.put(path, headers=headers, json=changed)
    assert conflict.status_code == 409, conflict.text

    other_operation = uuid.uuid4().hex
    other_path = (
        f"/api/tasks/{plan.id}/internal/worker-plan-decisions/"
        f"{other_operation}"
    )
    other = await client.get(other_path, headers=headers)
    assert other.status_code == 409, other.text


@pytest.mark.asyncio
async def test_worker_related_plan_reject_freezes_target_incarnation(
    client,
    session_factory,
):
    plan, target = await _worker_plan(session_factory)
    request = _decision_request(plan, action="reject", target=target)
    request["plan_target_incarnation_id"] = "0" * 32
    request["request_digest"] = worker_plan_decision_request_digest(request)
    path = (
        f"/api/tasks/{plan.id}/internal/worker-plan-decisions/"
        f"{request['operation_id']}"
    )
    response = await client.put(
        path,
        headers={"X-CCM-Task-Incarnation": plan.incarnation_id},
        json=request,
    )
    assert response.status_code == 409, response.text
    async with session_factory() as db:
        unchanged = await db.get(Task, plan.id)
    assert unchanged.status == "plan_review"
    assert WORKER_PLAN_DECISION_RECEIPT_METADATA_KEY not in (
        unchanged.metadata_ or {}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ("approve", "reject"))
async def test_worker_public_plan_decision_cannot_bypass_receipt_protocol(
    client,
    session_factory,
    monkeypatch,
    action,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-plan-token")
    plan, _target = await _worker_plan(session_factory)

    response = await client.post(
        f"/api/tasks/{plan.id}/plan/{action}",
        headers={"Authorization": "Bearer worker-plan-token"},
    )

    assert response.status_code == 403, response.text
    assert (
        response.json()["detail"]
        == "Endpoint is outside the CCM Worker control-plane protocol"
    )
    async with session_factory() as db:
        unchanged = await db.get(Task, plan.id)
    assert unchanged.status == "plan_review"
    assert WORKER_PLAN_DECISION_RECEIPT_METADATA_KEY not in (
        unchanged.metadata_ or {}
    )


@pytest.mark.asyncio
async def test_worker_drain_rejects_new_plan_decision(
    client,
    session_factory,
    monkeypatch,
):
    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-plan-token")
    headers = {"Authorization": "Bearer worker-plan-token"}

    blocked_plan, blocked_target = await _worker_plan(session_factory)
    blocked_request = _decision_request(
        blocked_plan,
        action="approve",
        target=blocked_target,
    )
    blocked_path = (
        f"/api/tasks/{blocked_plan.id}/internal/worker-plan-decisions/"
        f"{blocked_request['operation_id']}"
    )
    async with session_factory() as db:
        await begin_worker_node_drain(db, claim="d" * 64)
        await db.commit()

    blocked = await client.put(
        blocked_path,
        headers={
            **headers,
            "X-CCM-Task-Incarnation": blocked_plan.incarnation_id,
        },
        json=blocked_request,
    )
    assert blocked.status_code == 409, blocked.text
    assert "destruction has begun" in blocked.json()["detail"]
    async with session_factory() as db:
        unchanged = await db.get(Task, blocked_plan.id)
    assert unchanged.status == "plan_review"
    assert WORKER_PLAN_DECISION_RECEIPT_METADATA_KEY not in (
        unchanged.metadata_ or {}
    )


@pytest.mark.asyncio
async def test_worker_drain_allows_applied_plan_receipt_replay(
    client,
    session_factory,
    monkeypatch,
):
    """A pre-drain receipt stays available for read-only ACK reconciliation."""

    monkeypatch.setattr(settings, "ccm_node_role", "worker")
    monkeypatch.setattr(settings, "auth_token", "worker-plan-token")
    headers = {"Authorization": "Bearer worker-plan-token"}
    applied_plan, applied_target = await _worker_plan(session_factory)
    applied_request = _decision_request(
        applied_plan,
        action="reject",
        target=applied_target,
    )
    applied_path = (
        f"/api/tasks/{applied_plan.id}/internal/worker-plan-decisions/"
        f"{applied_request['operation_id']}"
    )
    applied_headers = {
        **headers,
        "X-CCM-Task-Incarnation": applied_plan.incarnation_id,
    }
    applied = await client.put(
        applied_path,
        headers=applied_headers,
        json=applied_request,
    )
    assert applied.status_code == 200, applied.text
    receipt = applied.json()["receipt"]
    async with session_factory() as db:
        await begin_worker_node_drain(db, claim="e" * 64)
        await db.commit()

    replay = await client.put(
        applied_path,
        headers=applied_headers,
        json=applied_request,
    )
    assert replay.status_code == 200, replay.text
    assert replay.json()["receipt"] == receipt


async def _manager_worker_plan(session_factory) -> tuple[Task, Worker]:
    async with session_factory() as db:
        worker = Worker(
            name="Plan receipt Worker",
            status="ready",
            private_ip="10.0.0.91",
            auth_token="plan-receipt-worker-token",
        )
        db.add(worker)
        await db.flush()
        plan = Task(
            title="Manager Worker legacy Plan",
            description="plan",
            status="plan_review",
            mode="plan",
            plan_content="Apply through the Worker receipt protocol",
            worker_id=worker.id,
            metadata_={"ccm_worker_remote_materialized_v1": True},
        )
        db.add(plan)
        await db.commit()
        await db.refresh(worker)
        await db.refresh(plan)
        return plan, worker


def _routing_snapshot(plan: Task) -> dict:
    return {
        "id": plan.id,
        "status": "plan_review",
        "worker_id": None,
        "shared_from_id": None,
        "provider": plan.provider,
        "model": plan.model,
        "codex_service_tier": plan.codex_service_tier,
        "pending": None,
    }


def _applied_worker_response(plan: Task, request: dict) -> dict:
    completed_at = "2026-08-14T12:00:00.000000"
    approved = request["action"] == "approve"
    terminal_status = "completed" if approved else "cancelled"
    task = {
        "id": plan.id,
        "incarnation_id": plan.incarnation_id,
        "status": terminal_status,
        "retry_count": plan.retry_count,
        "turn_generation": plan.turn_generation,
        "execution_user_id": plan.execution_user_id,
        "execution_user_role": plan.execution_user_role,
        "execution_mode": plan.execution_mode,
        "execution_principal_kind": plan.execution_principal_kind,
        "completed_at": completed_at,
        "plan_approved": approved,
        "plan_approved_at": completed_at if approved else None,
    }
    receipt = {
        "protocol_version": WORKER_PLAN_DECISION_PROTOCOL,
        "side": "worker",
        "state": "applied",
        "action": request["action"],
        "operation_id": request["operation_id"],
        "request_digest": request["request_digest"],
        "request": request,
        "result_generation": {
            "task_id": plan.id,
            "incarnation_id": plan.incarnation_id,
            "status": terminal_status,
            "retry_count": plan.retry_count,
            "turn_generation": plan.turn_generation,
        },
        "applied_at": completed_at,
    }
    return {"task": task, "receipt": receipt}


def _actor_request(user_id: int, *, role: str = "admin") -> Request:
    request = Request({"type": "http"})
    request.state.user_id = user_id
    request.state.user_role = role
    request.state.auth_type = "jwt"
    return request


def _manager_plan_decision_base(
    plan: Task,
    worker: Worker,
    *,
    decision_body: dict | None,
) -> dict:
    return {
        "protocol_version": WORKER_PLAN_DECISION_PROTOCOL,
        "action": "approve",
        "task_id": plan.id,
        "manager_worker_id": worker.id,
        "source_incarnation_id": plan.incarnation_id,
        "expected_status": plan.status,
        "expected_retry_count": plan.retry_count,
        "expected_turn_generation": plan.turn_generation,
        "decision_path": f"/api/tasks/{plan.id}/plan/approve",
        "routing": {
            "provider": (plan.provider or "claude").lower(),
            "model": plan.model,
            "codex_service_tier": plan.codex_service_tier or "default",
        },
        "decision_body": decision_body,
        "plan_target_task_id": None,
        "plan_target_incarnation_id": None,
    }


@pytest.mark.asyncio
async def test_worker_plan_gate_crash_gap_freezes_actor_body_and_route_atomically(
    session_factory,
):
    plan, worker = await _manager_worker_plan(session_factory)
    first_body = {"confirm_stale": False, "expected_routing": None}
    changed_body = {"confirm_stale": True, "expected_routing": None}

    # Intentionally stop after the gate commit and never call the subsequent
    # routing/target CAS helper.  This is the former process-crash window.
    async with session_factory() as db:
        current = await db.get(Task, plan.id)
        first_request = _manager_plan_decision_base(
            current,
            worker,
            decision_body=first_body,
        )
        await tasks_api_module._commit_task_control_effect_gate(
            _actor_request(101),
            db,
            current,
            effect="plan_approve",
            worker_plan_decision_request_base=first_request,
        )

    async with session_factory() as db:
        persisted = await db.get(Task, plan.id)
        gate = persisted.metadata_[TEST_HARNESS_TERMINAL_GATE_KEY]
        marker = gate[WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD]
    assert gate["authorized_user_id"] == 101
    assert gate["authorized_user_role"] == "admin"
    assert gate["authorization_type"] == "jwt"
    assert marker["state"] == "prepared"
    assert marker["request"]["decision_body"] == first_body
    assert marker["request"]["routing"] == first_request["routing"]

    # A different administrator cannot borrow the first actor's audit record,
    # especially not while substituting a different approval body.
    async with session_factory() as db:
        current = await db.get(Task, plan.id)
        second_request = _manager_plan_decision_base(
            current,
            worker,
            decision_body=changed_body,
        )
        with pytest.raises(HTTPException) as other_actor:
            await tasks_api_module._commit_task_control_effect_gate(
                _actor_request(202),
                db,
                current,
                effect="plan_approve",
                worker_plan_decision_request_base=second_request,
            )
        assert other_actor.value.status_code == 409

    # The original actor is also bound to the first exact body and route.
    async with session_factory() as db:
        current = await db.get(Task, plan.id)
        with pytest.raises(HTTPException) as changed_request:
            await tasks_api_module._commit_task_control_effect_gate(
                _actor_request(101),
                db,
                current,
                effect="plan_approve",
                worker_plan_decision_request_base=second_request,
            )
        assert changed_request.value.status_code == 409

    async with session_factory() as db:
        unchanged = await db.get(Task, plan.id)
        unchanged_gate = unchanged.metadata_[TEST_HARNESS_TERMINAL_GATE_KEY]
    assert unchanged_gate["authorized_user_id"] == 101
    assert unchanged_gate[WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD] == marker


@pytest.mark.asyncio
async def test_manager_plan_decision_recovers_ack_loss_without_replaying_put(
    client,
    session_factory,
    monkeypatch,
):
    plan, _worker = await _manager_worker_plan(session_factory)
    proxy = AsyncMock()
    decision_gets = 0
    decision_puts = 0
    applied_request = None

    async def protocol(current, method, path, body=None, **_kwargs):
        nonlocal decision_gets, decision_puts, applied_request
        if path.endswith("/routing-config/status"):
            return _routing_snapshot(plan)
        assert "/internal/worker-plan-decisions/" in path
        if method == "PUT":
            decision_puts += 1
            applied_request = copy.deepcopy(body)
            raise WorkerTaskMutationOutcomeUncertainError(
                "lost ACK",
                status_code=503,
            )
        assert method == "GET"
        decision_gets += 1
        if decision_gets == 1:
            return {
                "protocol_version": WORKER_PLAN_DECISION_PROTOCOL,
                "state": "absent",
                "operation_id": path.rsplit("/", 1)[-1],
                "task_id": current.id,
            }
        if decision_gets == 2:
            raise HTTPException(503, "readback temporarily unavailable")
        assert applied_request is not None
        return _applied_worker_response(plan, applied_request)

    proxy.proxy_to_worker = AsyncMock(side_effect=protocol)
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    uncertain = await client.post(f"/api/tasks/{plan.id}/plan/approve")
    assert uncertain.status_code == 409, uncertain.text
    async with session_factory() as db:
        pending = await db.get(Task, plan.id)
        gate = pending.metadata_[TEST_HARNESS_TERMINAL_GATE_KEY]
        marker = gate[WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD]
    assert pending.status == "plan_review"
    assert gate["task_control_effect_state"] == "active"
    assert marker["state"] == "prepared"
    assert has_worker_execution_quarantine(pending.metadata_)
    # A generic relay snapshot is not a substitute for the exact receipt.
    # It must leave the Manager mirror in plan_review until reconciliation.
    async with session_factory() as db:
        current = await db.get(Task, plan.id)
        observed = worker_task_generation(current)
        assert observed is not None
        generic = await apply_authoritative_worker_task(
            db,
            observed,
            _applied_worker_response(plan, applied_request)["task"],
        )
        assert generic is None
        await db.rollback()
        unchanged = await db.get(Task, plan.id)
        assert unchanged.status == "plan_review"

    recovered = await client.post(f"/api/tasks/{plan.id}/plan/approve")
    assert recovered.status_code == 200, recovered.text
    assert recovered.json()["status"] == "completed"
    assert recovered.json()["plan_approved"] is True
    assert decision_puts == 1
    assert decision_gets == 3
    async with session_factory() as db:
        settled = await db.get(Task, plan.id)
        gate = settled.metadata_[TEST_HARNESS_TERMINAL_GATE_KEY]
        marker = gate[WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD]
    assert gate["task_control_effect_state"] == "settled"
    assert marker["state"] == "applied"
    assert marker["request"] == applied_request


@pytest.mark.asyncio
async def test_manager_plan_decision_malformed_readback_never_sends_put(
    client,
    session_factory,
    monkeypatch,
):
    plan, _worker = await _manager_worker_plan(session_factory)
    proxy = AsyncMock()
    puts = 0

    async def protocol(current, method, path, body=None, **_kwargs):
        nonlocal puts
        if path.endswith("/routing-config/status"):
            return _routing_snapshot(plan)
        if method == "PUT":
            puts += 1
        return {"state": "applied", "operation_id": "not-exact"}

    proxy.proxy_to_worker = AsyncMock(side_effect=protocol)
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.post(f"/api/tasks/{plan.id}/plan/approve")
    assert response.status_code == 409, response.text
    assert puts == 0
    async with session_factory() as db:
        pending = await db.get(Task, plan.id)
        gate = pending.metadata_[TEST_HARNESS_TERMINAL_GATE_KEY]
    assert pending.status == "plan_review"
    assert gate["task_control_effect_state"] == "active"


@pytest.mark.asyncio
async def test_manager_related_plan_reject_freezes_target_identity(
    client,
    session_factory,
    monkeypatch,
):
    plan, worker = await _manager_worker_plan(session_factory)
    async with session_factory() as db:
        target = Task(
            title="Manager related target",
            description="target",
            status="completed",
            worker_id=worker.id,
            metadata_={"ccm_worker_remote_materialized_v1": True},
        )
        db.add(target)
        await db.flush()
        await db.execute(
            update(Task)
            .where(Task.id == plan.id)
            .values(plan_target_task_id=target.id)
        )
        await db.commit()
        await db.refresh(target)
        target_id = target.id
        target_incarnation_id = target.incarnation_id
        plan = await db.get(Task, plan.id)

    proxy = AsyncMock()
    put_request = None

    async def protocol(current, method, path, body=None, **_kwargs):
        nonlocal put_request
        if path.endswith("/routing-config/status"):
            return _routing_snapshot(plan)
        if method == "GET":
            return {
                "protocol_version": WORKER_PLAN_DECISION_PROTOCOL,
                "state": "absent",
                "operation_id": path.rsplit("/", 1)[-1],
                "task_id": plan.id,
            }
        assert method == "PUT"
        put_request = copy.deepcopy(body)
        assert body["action"] == "reject"
        assert body["plan_target_task_id"] == target_id
        assert body["plan_target_incarnation_id"] == target_incarnation_id
        return _applied_worker_response(plan, body)

    proxy.proxy_to_worker = AsyncMock(side_effect=protocol)
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.post(f"/api/tasks/{plan.id}/plan/reject")
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "cancelled"
    assert response.json()["plan_approved"] is False
    assert put_request is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("race", ("routing", "worker", "target"))
async def test_manager_plan_decision_race_cannot_publish_stale_outbox(
    client,
    session_factory,
    monkeypatch,
    race,
):
    plan, worker = await _manager_worker_plan(session_factory)
    replacement_worker_id = None
    original_target_id = None
    replacement_target_id = None
    if race == "worker":
        async with session_factory() as db:
            replacement = Worker(
                name="Replacement Plan Worker",
                status="ready",
                private_ip="10.0.0.92",
                auth_token="replacement-worker-token",
            )
            db.add(replacement)
            await db.commit()
            await db.refresh(replacement)
            replacement_worker_id = replacement.id
    elif race == "target":
        async with session_factory() as db:
            original_target = Task(
                title="Original receipt target",
                description="target",
                status="completed",
                worker_id=worker.id,
            )
            replacement_target = Task(
                title="Replacement receipt target",
                description="target",
                status="completed",
                worker_id=worker.id,
            )
            db.add_all([original_target, replacement_target])
            await db.flush()
            await db.execute(
                update(Task)
                .where(Task.id == plan.id)
                .values(plan_target_task_id=original_target.id)
            )
            await db.commit()
            original_target_id = original_target.id
            replacement_target_id = replacement_target.id

    original_commit = tasks_api_module._commit_task_control_effect_gate

    async def commit_then_race(*args, **kwargs):
        identity = await original_commit(*args, **kwargs)
        if race == "routing":
            values = {"provider": "codex", "model": "gpt-5.6-sol"}
        elif race == "worker":
            values = {"worker_id": replacement_worker_id}
        else:
            values = {"plan_target_task_id": replacement_target_id}
        async with session_factory() as concurrent_db:
            await concurrent_db.execute(
                update(Task).where(Task.id == plan.id).values(**values)
            )
            await concurrent_db.commit()
        return identity

    proxy = AsyncMock()
    proxy.proxy_to_worker = AsyncMock()
    monkeypatch.setattr(
        tasks_api_module,
        "_commit_task_control_effect_gate",
        commit_then_race,
    )
    monkeypatch.setattr(main_module, "worker_proxy", proxy)

    response = await client.post(f"/api/tasks/{plan.id}/plan/approve")
    assert response.status_code == 409, response.text
    proxy.proxy_to_worker.assert_not_awaited()
    async with session_factory() as db:
        current = await db.get(Task, plan.id)
        gate = current.metadata_[TEST_HARNESS_TERMINAL_GATE_KEY]
        marker = gate[WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD]
    assert gate["task_control_effect_state"] == "active"
    assert marker["state"] == "prepared"
    assert marker["request"]["manager_worker_id"] == worker.id
    assert marker["request"]["routing"] == {
        "provider": (plan.provider or "claude").lower(),
        "model": plan.model,
        "codex_service_tier": plan.codex_service_tier or "default",
    }
    assert marker["request"]["plan_target_task_id"] == original_target_id
    assert has_worker_execution_quarantine(current.metadata_)
    if race == "routing":
        assert current.provider == "codex"
    else:
        if race == "worker":
            assert current.worker_id == replacement_worker_id
            assert current.worker_id != worker.id
        else:
            assert current.plan_target_task_id == replacement_target_id


@pytest.mark.asyncio
async def test_prepared_plan_decision_blocks_unrelated_worker_proxy_mutation(
    session_factory,
):
    plan, worker = await _manager_worker_plan(session_factory)
    operation_id = uuid.uuid4().hex
    request = {
        "protocol_version": WORKER_PLAN_DECISION_PROTOCOL,
        "operation_id": operation_id,
        "action": "approve",
        "task_id": plan.id,
        "manager_worker_id": worker.id,
        "source_incarnation_id": plan.incarnation_id,
        "expected_status": "plan_review",
        "expected_retry_count": plan.retry_count,
        "expected_turn_generation": plan.turn_generation,
        "decision_path": f"/api/tasks/{plan.id}/plan/approve",
        "routing": {
            "provider": plan.provider,
            "model": plan.model,
            "codex_service_tier": plan.codex_service_tier,
        },
        "decision_body": None,
        "plan_target_task_id": None,
        "plan_target_incarnation_id": None,
    }
    request["request_digest"] = worker_plan_decision_request_digest(request)
    marker = {
        "protocol_version": WORKER_PLAN_DECISION_PROTOCOL,
        "side": "manager",
        "state": "prepared",
        "action": "approve",
        "operation_id": operation_id,
        "request_digest": request["request_digest"],
        "request": request,
        "prepared_at": "2026-08-14T12:00:00.000000",
    }
    gate = {
        "incarnation_id": plan.incarnation_id,
        "retry_count": plan.retry_count,
        "turn_generation": plan.turn_generation,
        "status": "plan_review",
        "task_control_effect": "plan_approve",
        "task_control_effect_version": 1,
        "task_control_effect_state": "active",
        WORKER_PLAN_DECISION_GATE_RECEIPT_FIELD: marker,
    }
    async with session_factory() as db:
        await db.execute(
            update(Task)
            .where(Task.id == plan.id)
            .values(metadata_={TEST_HARNESS_TERMINAL_GATE_KEY: gate})
        )
        await db.commit()
        plan = await db.get(Task, plan.id)

    proxy = WorkerProxy(session_factory, relay=AsyncMock())
    proxy.require_ready_worker = AsyncMock(return_value=worker)
    proxy._proxy_to_authorized_worker_locked = AsyncMock(
        return_value={"ok": True}
    )
    with pytest.raises(HTTPException) as blocked:
        await proxy.proxy_to_worker(
            plan,
            "POST",
            f"/api/tasks/{plan.id}/chat",
            {"message": "must not cross the prepared decision"},
            operation_lock_held=True,
        )
    assert blocked.value.status_code == 409
    proxy._proxy_to_authorized_worker_locked.assert_not_awaited()

    receipt_path = (
        f"/api/tasks/{plan.id}/internal/worker-plan-decisions/{operation_id}"
    )
    allowed = await proxy.proxy_to_worker(
        plan,
        "GET",
        receipt_path,
        operation_lock_held=True,
    )
    assert allowed == {"ok": True}
