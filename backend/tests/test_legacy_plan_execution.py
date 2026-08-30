"""Tests for exact pre-Plan-v2 execution-carrier compatibility proof."""

import asyncio
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import select

from backend.models.log_entry import LogEntry
from backend.models.plan import (
    Plan,
    PlanApplication,
    PlanLegacyTaskLink,
    PlanVersion,
)
from backend.models.task import Task
from backend.models.worker import Worker
from backend.services.legacy_plan_execution import (
    LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION,
    legacy_approved_execution_carrier_proof,
    legacy_plan_execution_snapshot_matches_proof,
    parse_legacy_plan_execution_carrier_proof,
)
from backend.services.worker_proxy import WorkerProxy
from backend.services.worker_relay import (
    LEGACY_PLAN_CARRIER_CONFLICT_METADATA_KEY,
    WorkerRelay,
    apply_authoritative_legacy_plan_execution_carrier,
    worker_task_generation,
)
import backend.services.worker_proxy as worker_proxy_module
import backend.services.worker_task_termination as worker_termination_module


async def _carrier(db_session) -> Task:
    task = Task(
        title="Migrated carrier",
        description="Implement the approved legacy plan",
        status="pending",
        mode="plan",
        plan_approved=True,
        provider="codex",
        model="gpt-5.6-sol",
        codex_service_tier="default",
        target_repo="/manager/path",
        target_branch="main",
        session_id="thread-legacy-carrier",
        plan_content="# Exact approved plan\n\n1. Make the change.",
    )
    plan = Plan(
        title="Migrated Plan",
        initial_request="Plan the work",
        pipeline_config={},
    )
    db_session.add_all([task, plan])
    await db_session.flush()
    version = PlanVersion(
        plan_id=plan.id,
        version_number=1,
        content="# Exact approved plan\n\n1. Make the change.",
        human_decision="approved",
    )
    db_session.add(version)
    await db_session.flush()
    db_session.add_all([
        PlanLegacyTaskLink(
            legacy_task_id=task.id,
            plan_id=plan.id,
            plan_version_id=version.id,
        ),
        PlanApplication(
            plan_id=plan.id,
            plan_version_id=version.id,
            application_type="execution_task",
            execution_task_id=task.id,
        ),
    ])
    await db_session.flush()
    return task


def _remote_snapshot(task: Task, *, status: str | None = None) -> dict:
    fields = (
        "id",
        "incarnation_id",
        "title",
        "description",
        "target_branch",
        "priority",
        "max_retries",
        "mode",
        "todo_file_path",
        "max_iterations",
        "must_complete",
        "goal_condition",
        "goal_max_turns",
        "goal_evaluator_model",
        "provider",
        "model",
        "codex_service_tier",
        "effort_level",
        "thinking_budget",
        "system_prompt_mode",
        "timeout_hours",
        "enable_workflows",
        "enabled_skills",
        "selected_user_skills",
        "tags",
        "attention_tag",
        "metadata_",
        "plan_approved",
        "plan_content",
        "retry_count",
        "turn_generation",
        "session_id",
        "execution_user_id",
        "execution_user_role",
        "execution_mode",
        "execution_principal_kind",
    )
    snapshot = {field: getattr(task, field) for field in fields}
    snapshot["status"] = status or task.status
    return snapshot


async def _carrier_with_active_termination_receipt(session_factory):
    async with session_factory() as db:
        task = await _carrier(db)
        worker = Worker(
            name="carrier-worker",
            private_ip="10.0.0.23",
            status="ready",
        )
        db.add(worker)
        await db.flush()
        task.worker_id = worker.id
        await db.commit()
        proof = await legacy_approved_execution_carrier_proof(db, task.id)
        observed = worker_task_generation(task, expected_worker_id=worker.id)
        assert proof is not None and observed is not None
        receipt = await worker_termination_module.create_or_resume_manager_receipt(
            db,
            task,
            operation="cancel",
        )
        return task, worker, proof, observed, receipt


@pytest.mark.asyncio
async def test_carrier_proof_round_trips_strict_wire_shape(db_session):
    task = await _carrier(db_session)

    proof = await legacy_approved_execution_carrier_proof(
        db_session,
        task.id,
    )

    assert proof is not None
    assert proof.task_id == task.id
    assert proof.version_number == 1
    assert proof.task_status == "pending"
    wire = proof.to_wire()
    assert (
        wire["protocol_version"]
        == LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION
    )
    assert parse_legacy_plan_execution_carrier_proof(wire) == proof


@pytest.mark.asyncio
async def test_carrier_proof_ignores_only_node_local_routing_fields(db_session):
    task = await _carrier(db_session)
    before = await legacy_approved_execution_carrier_proof(db_session, task.id)
    assert before is not None

    task.worker_id = 77
    task.project_id = 88
    task.target_repo = "/worker/path"
    task.last_cwd = "/worker/path"
    task.session_id = "worker-created-runtime-session"
    await db_session.flush()
    node_local = await legacy_approved_execution_carrier_proof(
        db_session,
        task.id,
    )
    assert node_local is not None
    assert node_local.proof_digest == before.proof_digest

    task.description = "different executable prompt"
    await db_session.flush()
    changed_prompt = await legacy_approved_execution_carrier_proof(
        db_session,
        task.id,
    )
    assert changed_prompt is not None
    assert changed_prompt.proof_digest != before.proof_digest


@pytest.mark.asyncio
async def test_carrier_proof_binds_prompt_and_skill_execution_context(db_session):
    task = await _carrier(db_session)
    before = await legacy_approved_execution_carrier_proof(db_session, task.id)
    assert before is not None

    task.system_prompt_mode = "append"
    task.metadata_ = {
        "ccm_user_skill_snapshots": [
            {
                "id": 7,
                "name": "review",
                "description": "Review carefully",
                "content": "Check every invariant",
            }
        ]
    }
    await db_session.flush()

    changed = await legacy_approved_execution_carrier_proof(
        db_session,
        task.id,
    )
    assert changed is not None
    assert changed.execution_fingerprint_sha256 != (
        before.execution_fingerprint_sha256
    )
    assert changed.proof_digest != before.proof_digest

    task.plan_content = "# Tampered execution content"
    await db_session.flush()
    assert (
        await legacy_approved_execution_carrier_proof(db_session, task.id)
        is None
    )


@pytest.mark.asyncio
async def test_carrier_lifecycle_state_is_reconcilable_not_authority(db_session):
    task = await _carrier(db_session)
    before = await legacy_approved_execution_carrier_proof(db_session, task.id)
    assert before is not None

    task.status = "executing"
    task.retry_count = 2
    task.turn_generation = 7
    await db_session.flush()
    active = await legacy_approved_execution_carrier_proof(db_session, task.id)

    assert active is not None
    assert active.proof_digest == before.proof_digest
    assert (active.task_status, active.retry_count, active.turn_generation) == (
        "executing",
        2,
        7,
    )


@pytest.mark.asyncio
async def test_carrier_proof_changes_with_linked_version_content(db_session):
    task = await _carrier(db_session)
    before = await legacy_approved_execution_carrier_proof(db_session, task.id)
    assert before is not None
    link = await db_session.get(PlanLegacyTaskLink, task.id)
    version = await db_session.get(PlanVersion, link.plan_version_id)
    version.content = "# Different approved plan"
    await db_session.flush()

    assert (
        await legacy_approved_execution_carrier_proof(db_session, task.id)
        is None
    )


@pytest.mark.asyncio
async def test_carrier_proof_rejects_non_authoritative_chain(db_session):
    task = await _carrier(db_session)
    link = await db_session.get(PlanLegacyTaskLink, task.id)
    version = await db_session.get(PlanVersion, link.plan_version_id)
    version.human_decision = "pending"
    await db_session.flush()

    assert (
        await legacy_approved_execution_carrier_proof(db_session, task.id)
        is None
    )


@pytest.mark.asyncio
async def test_carrier_wire_parser_rejects_tamper_and_ambiguous_types(db_session):
    task = await _carrier(db_session)
    proof = await legacy_approved_execution_carrier_proof(db_session, task.id)
    assert proof is not None
    wire = proof.to_wire()

    cases = []
    extra = deepcopy(wire)
    extra["unexpected"] = True
    cases.append(extra)
    boolean_id = deepcopy(wire)
    boolean_id["task_id"] = True
    cases.append(boolean_id)
    tampered_hash = deepcopy(wire)
    tampered_hash["version_content_sha256"] = "0" * 64
    cases.append(tampered_hash)
    uppercase_hash = deepcopy(wire)
    uppercase_hash["proof_digest"] = wire["proof_digest"].upper()
    cases.append(uppercase_hash)

    for payload in cases:
        with pytest.raises(ValueError):
            parse_legacy_plan_execution_carrier_proof(payload)


@pytest.mark.asyncio
async def test_exact_carrier_readback_adopts_status_and_history_atomically(
    db_session,
):
    task = await _carrier(db_session)
    worker = Worker(
        name="carrier-worker",
        private_ip="10.0.0.23",
        status="ready",
    )
    db_session.add(worker)
    await db_session.flush()
    task.worker_id = worker.id
    await db_session.flush()
    proof = await legacy_approved_execution_carrier_proof(db_session, task.id)
    observed = worker_task_generation(task, expected_worker_id=worker.id)
    assert proof is not None and observed is not None
    remote_proof = replace(proof, task_status="executing")
    remote_history = {
        "messages": [
            {
                "event_type": "system_event",
                "role": "system",
                "content": "remote carrier started",
                "is_error": False,
                "task_retry_count": task.retry_count,
                "task_turn_generation": task.turn_generation,
                "native_turn_id": "native-carrier-turn",
                "turn_scope": "foreground",
                "actual_transport": None,
            }
        ]
    }

    resulting = await apply_authoritative_legacy_plan_execution_carrier(
        db_session,
        observed,
        _remote_snapshot(task, status="executing"),
        remote_history,
        expected_proof_digest=proof.proof_digest,
        remote_proof=remote_proof,
    )

    assert resulting is not None
    assert resulting.status == "executing"
    entries = list(
        (
            await db_session.execute(
                select(LogEntry).where(LogEntry.task_id == task.id)
            )
        ).scalars()
    )
    assert [entry.content for entry in entries] == ["remote carrier started"]


@pytest.mark.asyncio
async def test_active_termination_receipt_blocks_legacy_carrier_apply(
    session_factory,
):
    task, _worker, proof, observed, receipt = (
        await _carrier_with_active_termination_receipt(session_factory)
    )
    remote_proof = replace(proof, task_status="executing")
    remote_history = {
        "messages": [
            {
                "event_type": "system_event",
                "role": "system",
                "content": "must not be imported after termination admission",
                "is_error": False,
                "task_retry_count": task.retry_count,
                "task_turn_generation": task.turn_generation,
                "native_turn_id": "native-blocked-carrier",
                "turn_scope": "foreground",
                "actual_transport": None,
            }
        ]
    }

    async with session_factory() as db:
        resulting = await apply_authoritative_legacy_plan_execution_carrier(
            db,
            observed,
            _remote_snapshot(task, status="executing"),
            remote_history,
            expected_proof_digest=proof.proof_digest,
            remote_proof=remote_proof,
        )
        current = await db.get(Task, task.id)
        entries = list(
            (
                await db.execute(
                    select(LogEntry).where(LogEntry.task_id == task.id)
                )
            ).scalars()
        )
        active = await worker_termination_module.active_worker_task_termination_receipt(
            db,
            task.id,
        )

    assert resulting is None
    assert current.status == "pending"
    assert current.metadata_ in (None, {})
    assert entries == []
    assert active.operation_id == receipt.operation_id


@pytest.mark.asyncio
async def test_active_termination_receipt_blocks_legacy_carrier_conflict(
    session_factory,
):
    task, _worker, proof, observed, receipt = (
        await _carrier_with_active_termination_receipt(session_factory)
    )
    relay = WorkerRelay(
        session_factory,
        SimpleNamespace(broadcast=AsyncMock()),
    )

    resulting = await relay._conflict_legacy_plan_execution_carrier(
        observed,
        expected_proof_digest=proof.proof_digest,
        error="semantic split must lose to termination",
    )

    assert resulting is None
    async with session_factory() as db:
        current = await db.get(Task, task.id)
        active = await worker_termination_module.active_worker_task_termination_receipt(
            db,
            task.id,
        )
    assert current.status == "pending"
    assert current.error_message is None
    assert LEGACY_PLAN_CARRIER_CONFLICT_METADATA_KEY not in (current.metadata_ or {})
    assert active.operation_id == receipt.operation_id


@pytest.mark.asyncio
async def test_carrier_snapshot_is_bound_to_semantic_proof(db_session):
    task = await _carrier(db_session)
    proof = await legacy_approved_execution_carrier_proof(db_session, task.id)
    assert proof is not None
    snapshot = _remote_snapshot(task)

    assert legacy_plan_execution_snapshot_matches_proof(snapshot, proof)

    changed_prompt = deepcopy(snapshot)
    changed_prompt["description"] = "different executable prompt"
    assert not legacy_plan_execution_snapshot_matches_proof(
        changed_prompt,
        proof,
    )
    changed_plan = deepcopy(snapshot)
    changed_plan["plan_content"] = "# Different approved Plan"
    assert not legacy_plan_execution_snapshot_matches_proof(changed_plan, proof)
    missing_field = deepcopy(snapshot)
    missing_field.pop("provider")
    assert not legacy_plan_execution_snapshot_matches_proof(missing_field, proof)


@pytest.mark.asyncio
async def test_carrier_semantic_conflict_is_durably_unsubscribed(
    session_factory,
):
    async with session_factory() as db:
        task = await _carrier(db)
        worker = Worker(
            name="carrier-worker",
            private_ip="10.0.0.23",
            status="ready",
        )
        db.add(worker)
        await db.flush()
        task.worker_id = worker.id
        await db.commit()
        proof = await legacy_approved_execution_carrier_proof(db, task.id)
        assert proof is not None
        task_id = task.id
        worker_id = worker.id

    relay = WorkerRelay(
        session_factory,
        SimpleNamespace(broadcast=AsyncMock()),
    )
    relay.subscribe_task = AsyncMock()
    relay.unsubscribe_task = Mock()
    relay._publish_status_generation = AsyncMock(return_value=True)
    remote_proof = replace(proof, proof_digest="0" * 64)
    proxy = SimpleNamespace(
        get_legacy_plan_execution_carrier_proof=AsyncMock(
            return_value=remote_proof
        )
    )

    relay.ensure_legacy_plan_execution_carrier_recovery(
        worker,
        task_id,
        proof.proof_digest,
        proxy,
    )
    recovery = next(iter(relay._legacy_carrier_recovery_tasks.values()))
    await recovery
    await asyncio.sleep(0)

    async with session_factory() as db:
        stored = await db.get(Task, task_id)
        assert stored.status == "conflict"
        assert "quarantined" in stored.error_message
    relay.unsubscribe_task.assert_called_once_with(worker_id, task_id)
    assert relay._legacy_carrier_recovery_tasks == {}


@pytest.mark.asyncio
async def test_internal_carrier_proof_endpoint_is_read_only(
    client,
    session_factory,
):
    async with session_factory() as db:
        task = await _carrier(db)
        await db.commit()
        task_id = task.id

    response = await client.get(
        f"/api/tasks/{task_id}/legacy-plan-execution-carrier-proof"
    )

    assert response.status_code == 200, response.text
    proof = parse_legacy_plan_execution_carrier_proof(response.json())
    assert proof.task_id == task_id
    async with session_factory() as db:
        current = await db.get(Task, task_id)
        assert current.status == "pending"
        assert current.retry_count == 0
        assert current.turn_generation == 0


@pytest.mark.asyncio
async def test_internal_carrier_proof_endpoint_rejects_noncarrier(
    client,
    session_factory,
):
    async with session_factory() as db:
        task = Task(title="ordinary", description="work", mode="auto")
        db.add(task)
        await db.commit()
        task_id = task.id

    response = await client.get(
        f"/api/tasks/{task_id}/legacy-plan-execution-carrier-proof"
    )

    assert response.status_code == 409


@pytest.mark.asyncio
async def test_system_config_advertises_exact_carrier_protocol(client):
    response = await client.get("/api/system/config")

    assert response.status_code == 200, response.text
    assert (
        response.json()["legacy_plan_execution_carrier_protocol"]
        == LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION
    )


class _Response:
    def __init__(self, payload, *, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


@pytest.mark.asyncio
async def test_worker_proxy_requires_capability_then_validates_carrier_proof(
    db_session,
    monkeypatch,
):
    task = await _carrier(db_session)
    local = await legacy_approved_execution_carrier_proof(db_session, task.id)
    assert local is not None
    calls = []

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, url, *, headers):
            calls.append((url, headers))
            if url.endswith("/api/system/config"):
                return _Response({
                    "legacy_plan_execution_carrier_protocol": (
                        LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION
                    )
                })
            return _Response(local.to_wire())

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    worker = Worker(
        id=9,
        name="proof-worker",
        private_ip="10.0.0.9",
        ccm_port=8002,
        auth_token="secret",
        status="ready",
    )
    proxy = WorkerProxy(None, None)

    remote = await proxy.get_legacy_plan_execution_carrier_proof(
        worker,
        task.id,
    )

    assert remote == local
    assert [url.rsplit("/api/", 1)[1] for url, _headers in calls] == [
        "system/config",
        f"tasks/{task.id}/legacy-plan-execution-carrier-proof",
    ]


@pytest.mark.asyncio
async def test_worker_proxy_rejects_old_protocol_before_proof_readback(
    monkeypatch,
):
    calls = []

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, url, *, headers):
            calls.append(url)
            return _Response({"versioned_plan_worker_protocol": 3})

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    worker = Worker(
        id=10,
        name="old-worker",
        private_ip="10.0.0.10",
        ccm_port=8002,
        auth_token="secret",
        status="ready",
    )

    with pytest.raises(RuntimeError, match="does not support"):
        await WorkerProxy(None, None).get_legacy_plan_execution_carrier_proof(
            worker,
            71,
        )

    assert len(calls) == 1


@pytest.mark.asyncio
async def test_worker_proxy_treats_noncarrier_conflict_as_stable_absence(
    monkeypatch,
):
    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, url, *, headers):
            if url.endswith("/api/system/config"):
                return _Response({
                    "legacy_plan_execution_carrier_protocol": (
                        LEGACY_PLAN_EXECUTION_CARRIER_PROTOCOL_VERSION
                    )
                })
            return _Response({}, status_code=409)

    monkeypatch.setattr(worker_proxy_module.httpx, "AsyncClient", Client)
    worker = Worker(
        id=11,
        name="noncarrier-worker",
        private_ip="10.0.0.11",
        ccm_port=8002,
        auth_token="secret",
        status="ready",
    )

    assert (
        await WorkerProxy(None, None).get_legacy_plan_execution_carrier_proof(
            worker,
            72,
        )
        is None
    )
