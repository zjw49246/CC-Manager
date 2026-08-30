"""Crash/replay tests for the durable Capability resume coordinator."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from backend.models.capability import (
    CapabilityExecution,
    CapabilityInvocation,
    CapabilityResumeOutbox,
)
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.services.capability_resume import (
    CapabilityResumeConflictError,
    CapabilityResumeCoordinator,
    CapabilityResumeIntegrityError,
    cancel_task_resume_outbox_in_tx,
    claim_resume_publication,
    claim_resume_turn_locked,
    load_resume_envelope,
    mark_resume_launch_boundary,
    materialize_resume_outbox,
    reconcile_stale_resume_in_tx,
    recover_expired_resume_publication,
    release_resume_publication,
    settle_previous_resume_in_terminal_tx,
)
from backend.services.capability_service import (
    CapabilityConflictError,
    capability_task_lock,
)


_DIGEST = "d" * 64


@dataclass(frozen=True, slots=True)
class _Seed:
    task_id: int
    invocation_id: int
    execution_id: int
    outbox_id: int


def _text_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


async def _seed_resume(
    db,
    *,
    invocation_status: str = "failed",
    generation: int = 7,
    execution_principal: dict[str, object] | None = None,
) -> _Seed:
    principal = execution_principal or {
        "execution_user_id": None,
        "execution_user_role": "member",
        "execution_mode": "sandbox",
        "execution_principal_kind": "system",
    }
    task = Task(
        title="Capability resume",
        description="continue after capability",
        status="waiting_capability",
        mode="auto",
        provider="claude",
        model="claude-opus-4-6",
        codex_service_tier="default",
        retry_count=2,
        turn_generation=generation,
        session_id="session-resume",
        **principal,
    )
    db.add(task)
    await db.flush()
    assert task.incarnation_id is not None

    source = LogEntry(
        instance_id=11,
        task_id=task.id,
        task_retry_count=task.retry_count,
        task_turn_generation=generation,
        turn_scope="source",
        actual_transport="claude_exec",
        event_type="user_message",
        role="user",
        content="original turn",
        is_error=False,
    )
    output_text = "requesting a capability"
    output = LogEntry(
        instance_id=11,
        task_id=task.id,
        task_retry_count=task.retry_count,
        task_turn_generation=generation,
        turn_scope="foreground",
        event_type="result",
        role="assistant",
        content=output_text,
        is_error=False,
    )
    db.add_all((source, output))
    await db.flush()
    task.turn_source_log_id = source.id

    terminal = invocation_status in {"failed", "cancelled", "stale"}
    result_ready = invocation_status == "ready"
    invocation = CapabilityInvocation(
        task_id=task.id,
        capability_key="plan",
        source="agent_request",
        purpose="advisory",
        status=invocation_status,
        state_version=1,
        idempotency_key=f"resume-{task.id}-{generation}",
        input_payload={"focus": "safe continuation"},
        input_hash=_DIGEST,
        subject_kind="task_generation",
        subject_ref={"task_id": task.id, "generation": generation},
        subject_hash=_DIGEST,
        executor_kind="fake_plan",
        executor_config={},
        executor_config_hash=_DIGEST,
        policy_snapshot={"allowed": True},
        policy_hash=_DIGEST,
        resume_policy="resume_task",
        max_attempts=2,
        active_task_id=(None if terminal else task.id),
        request_task_incarnation_id=task.incarnation_id,
        request_task_retry_count=task.retry_count,
        request_task_instance_id=11,
        request_task_session_id=task.session_id,
        request_task_turn_generation=generation,
        request_source_log_id=source.id,
        request_output_log_id=output.id,
        request_terminal_log_id=output.id,
        request_reason="Need exact guidance",
        request_protocol_version=1,
        request_output_hash=_text_hash(output_text),
        result_kind=("plan_version" if result_ready else None),
        result_id=(101 if result_ready else None),
        result_hash=(_DIGEST if result_ready else None),
        error_code=(f"capability_{invocation_status}" if terminal else None),
        error_message=(f"capability became {invocation_status}" if terminal else None),
        ready_at=(datetime.utcnow() if result_ready else None),
        completed_at=(datetime.utcnow() if terminal else None),
    )
    db.add(invocation)
    await db.flush()

    if result_ready:
        execution_status = "completed"
    else:
        execution_status = invocation_status
    active_execution = execution_status in {
        "queued",
        "running",
        "waiting_user",
        "cancelling",
    }
    execution = CapabilityExecution(
        invocation_id=invocation.id,
        attempt=1,
        status=execution_status,
        state_version=1,
        active_invocation_id=(invocation.id if active_execution else None),
        idempotency_key=f"resume-execution-{task.id}-{generation}",
        executor_kind="fake_plan",
        input_hash=_DIGEST,
        output_kind=("plan_version" if result_ready else None),
        output_id=(101 if result_ready else None),
        output_hash=(_DIGEST if result_ready else None),
        error_code=(f"execution_{execution_status}" if terminal else None),
        error_message=(f"execution became {execution_status}" if terminal else None),
        started_at=(
            datetime.utcnow()
            if execution_status not in {"queued"}
            else None
        ),
        completed_at=(
            datetime.utcnow()
            if execution_status in {"completed", "failed", "cancelled", "stale"}
            else None
        ),
    )
    db.add(execution)
    await db.flush()

    outbox = CapabilityResumeOutbox(
        task_id=task.id,
        invocation_id=invocation.id,
        active_task_id=task.id,
        active_invocation_id=invocation.id,
        status="pending",
        request_task_incarnation_id=task.incarnation_id,
        request_task_retry_count=task.retry_count,
        from_turn_generation=generation,
        request_task_session_id=task.session_id,
        request_source_log_id=source.id,
        request_output_log_id=output.id,
        request_terminal_log_id=output.id,
        request_execution_user_id=task.execution_user_id,
        request_execution_user_role=task.execution_user_role,
        request_execution_mode=task.execution_mode,
        request_execution_principal_kind=task.execution_principal_kind,
    )
    db.add(outbox)
    await db.commit()
    return _Seed(task.id, invocation.id, execution.id, outbox.id)


def _install_fake_result(monkeypatch, execution_id: int) -> None:
    async def resolve(_db, _invocation):
        return SimpleNamespace(
            execution_id=execution_id,
            kind="plan_version",
            id=101,
            hash=_DIGEST,
            resource_url="/api/plan-versions/101",
            data={"id": 101, "content": "plan"},
        )

    monkeypatch.setattr(
        "backend.services.capability_result.resolve_capability_result",
        resolve,
    )


async def _claim_turn(
    db_factory,
    seed: _Seed,
    *,
    lease_token: str,
    instance_id: int,
):
    async with capability_task_lock(seed.task_id):
        async with db_factory() as db:
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == seed.task_id)
                    .with_for_update()
                )
            ).scalar_one()
            claim = await claim_resume_turn_locked(
                db,
                task=task,
                outbox_id=seed.outbox_id,
                lease_token=lease_token,
                instance_id=instance_id,
                transport="claude_exec",
            )
            await db.commit()
            return claim


@pytest.mark.asyncio
async def test_success_materializes_one_frozen_resume_envelope(
    db_factory,
    monkeypatch,
):
    async with db_factory() as db:
        seed = await _seed_resume(db, invocation_status="ready")
    _install_fake_result(monkeypatch, seed.execution_id)

    async with db_factory() as db:
        envelope = await materialize_resume_outbox(db, seed.outbox_id)

    assert envelope is not None
    assert envelope.status == "ready"
    assert envelope.from_generation == 7
    assert envelope.request_retry_count == 2
    assert envelope.request_session_id == "session-resume"
    assert "<ccm_capability_result>" in envelope.prompt
    assert '"status":"completed"' in envelope.prompt
    assert len(envelope.payload_hash) == 64

    async with db_factory() as db:
        invocation = await db.get(CapabilityInvocation, seed.invocation_id)
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        assert invocation.status == "resuming"
        assert outbox.invocation_result_kind == "plan_version"
        assert outbox.resume_payload["outcome"]["result"]["data"] == {
            "id": 101,
            "content": "plan",
        }


@pytest.mark.asyncio
async def test_resume_preserves_original_admin_principal_through_claim(
    db_factory,
):
    principal = {
        "execution_user_id": 73,
        "execution_user_role": "admin",
        "execution_mode": "unrestricted",
        "execution_principal_kind": "user",
    }
    async with db_factory() as db:
        seed = await _seed_resume(
            db,
            invocation_status="failed",
            execution_principal=principal,
        )
    async with db_factory() as db:
        materialized = await materialize_resume_outbox(db, seed.outbox_id)

    assert materialized is not None
    assert {
        "execution_user_id": materialized.execution_user_id,
        "execution_user_role": materialized.execution_user_role,
        "execution_mode": materialized.execution_mode,
        "execution_principal_kind": materialized.execution_principal_kind,
    } == principal

    async with db_factory() as db:
        published = await claim_resume_publication(db, seed.outbox_id)
    assert published is not None and published.lease_token is not None
    assert {
        "execution_user_id": published.execution_user_id,
        "execution_user_role": published.execution_user_role,
        "execution_mode": published.execution_mode,
        "execution_principal_kind": published.execution_principal_kind,
    } == principal

    claimed = await _claim_turn(
        db_factory,
        seed,
        lease_token=published.lease_token,
        instance_id=17,
    )
    assert {
        "execution_user_id": claimed.envelope.execution_user_id,
        "execution_user_role": claimed.envelope.execution_user_role,
        "execution_mode": claimed.envelope.execution_mode,
        "execution_principal_kind": claimed.envelope.execution_principal_kind,
    } == principal
    async with db_factory() as db:
        task = await db.get(Task, seed.task_id)
    assert (
        task.execution_user_id,
        task.execution_user_role,
        task.execution_mode,
        task.execution_principal_kind,
    ) == (73, "admin", "unrestricted", "user")


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_status", ["failed", "cancelled", "stale"])
async def test_terminal_capability_failure_is_still_a_ready_resume(
    db_factory,
    terminal_status,
):
    async with db_factory() as db:
        seed = await _seed_resume(db, invocation_status=terminal_status)
    async with db_factory() as db:
        envelope = await materialize_resume_outbox(db, seed.outbox_id)

    assert envelope is not None
    assert envelope.status == "ready"
    assert f'"status":"{terminal_status}"' in envelope.prompt
    async with db_factory() as db:
        invocation = await db.get(CapabilityInvocation, seed.invocation_id)
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        assert invocation.status == terminal_status
        assert outbox.invocation_terminal_status == terminal_status


@pytest.mark.asyncio
async def test_payload_tamper_fails_durably_closed(db_factory):
    async with db_factory() as db:
        seed = await _seed_resume(db)
    async with db_factory() as db:
        await materialize_resume_outbox(db, seed.outbox_id)
    async with db_factory() as db:
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        payload = dict(outbox.resume_payload)
        queue = dict(payload["queue"])
        queue["prompt"] = "tampered"
        payload["queue"] = queue
        outbox.resume_payload = payload
        await db.commit()

    async with db_factory() as db:
        with pytest.raises(CapabilityResumeIntegrityError):
            await load_resume_envelope(db, seed.outbox_id)
    async with db_factory() as db:
        with pytest.raises(CapabilityResumeIntegrityError):
            await materialize_resume_outbox(db, seed.outbox_id)
    async with db_factory() as db:
        task = await db.get(Task, seed.task_id)
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        assert task.status == "failed"
        assert outbox.status == "failed"
        assert outbox.error_code == "resume_integrity_failed"


@pytest.mark.asyncio
async def test_broken_result_graph_fails_durably_closed(
    db_factory,
    monkeypatch,
):
    async with db_factory() as db:
        seed = await _seed_resume(db, invocation_status="ready")

    async def broken_result(_db, _invocation):
        raise CapabilityConflictError("result graph changed")

    monkeypatch.setattr(
        "backend.services.capability_result.resolve_capability_result",
        broken_result,
    )
    async with db_factory() as db:
        with pytest.raises(CapabilityResumeIntegrityError):
            await materialize_resume_outbox(db, seed.outbox_id)
    async with db_factory() as db:
        task = await db.get(Task, seed.task_id)
        invocation = await db.get(CapabilityInvocation, seed.invocation_id)
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        assert task.status == "failed"
        assert invocation.status == "failed"
        assert outbox.status == "failed"
        assert outbox.error_code == "resume_integrity_failed"


@pytest.mark.asyncio
async def test_claim_ack_loss_replays_same_generation_with_same_lease(db_factory):
    async with db_factory() as db:
        seed = await _seed_resume(db)
    async with db_factory() as db:
        await materialize_resume_outbox(db, seed.outbox_id)
    async with db_factory() as db:
        envelope = await claim_resume_publication(
            db,
            seed.outbox_id,
            lease_seconds=120,
        )
    assert envelope is not None and envelope.lease_token is not None
    token = envelope.lease_token

    first = await _claim_turn(
        db_factory,
        seed,
        lease_token=token,
        instance_id=21,
    )
    replay = await _claim_turn(
        db_factory,
        seed,
        lease_token=token,
        instance_id=22,
    )

    assert first.replay is False
    assert replay.replay is True
    assert first.turn_generation == replay.turn_generation == 8
    assert first.source_log_id == replay.source_log_id
    assert replay.envelope.lease_token == token
    async with db_factory() as db:
        loaded = await load_resume_envelope(
            db,
            seed.outbox_id,
            expected_lease_token=token,
        )
        task = await db.get(Task, seed.task_id)
        source = await db.get(LogEntry, replay.source_log_id)
        assert loaded is not None and loaded.status == "claimed"
        assert task.status == "executing"
        assert task.turn_generation == 8
        assert task.instance_id == 22
        assert source.instance_id == 22
        assert source.actual_transport is None


@pytest.mark.asyncio
async def test_release_claimed_restores_waiting_and_reclaims_same_g_plus_one(
    db_factory,
):
    async with db_factory() as db:
        seed = await _seed_resume(db)
    async with db_factory() as db:
        await materialize_resume_outbox(db, seed.outbox_id)
    async with db_factory() as db:
        envelope = await claim_resume_publication(db, seed.outbox_id)
    assert envelope is not None and envelope.lease_token is not None
    first = await _claim_turn(
        db_factory,
        seed,
        lease_token=envelope.lease_token,
        instance_id=31,
    )

    async with db_factory() as db:
        assert await release_resume_publication(
            db,
            seed.outbox_id,
            lease_token=envelope.lease_token,
            error_code="prelaunch_retry",
            error_message="retry safely",
        )
    async with db_factory() as db:
        task = await db.get(Task, seed.task_id)
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        assert task.status == "waiting_capability"
        assert task.instance_id is None
        assert task.turn_generation == 8
        assert task.turn_source_log_id == first.source_log_id
        assert outbox.status == "claimed"
        assert outbox.lease_token is None

    async with db_factory() as db:
        second_envelope = await claim_resume_publication(db, seed.outbox_id)
    assert second_envelope is not None
    assert second_envelope.lease_token is not None
    second = await _claim_turn(
        db_factory,
        seed,
        lease_token=second_envelope.lease_token,
        instance_id=32,
    )
    assert second.replay is True
    assert second.turn_generation == 8
    assert second.source_log_id == first.source_log_id


@pytest.mark.asyncio
@pytest.mark.parametrize("claimed", [False, True])
async def test_expired_publication_is_recovered_and_coordinator_republishes(
    db_factory,
    claimed,
):
    async with db_factory() as db:
        seed = await _seed_resume(db)
    async with db_factory() as db:
        await materialize_resume_outbox(db, seed.outbox_id)
    async with db_factory() as db:
        envelope = await claim_resume_publication(db, seed.outbox_id)
    assert envelope is not None and envelope.lease_token is not None
    if claimed:
        await _claim_turn(
            db_factory,
            seed,
            lease_token=envelope.lease_token,
            instance_id=41,
        )
        async with db_factory() as db:
            await release_resume_publication(
                db,
                seed.outbox_id,
                lease_token=envelope.lease_token,
                error_code="retry",
                error_message="retry",
            )
        async with db_factory() as db:
            envelope = await claim_resume_publication(db, seed.outbox_id)
        assert envelope is not None and envelope.lease_token is not None

    async with db_factory() as db:
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        outbox.lease_expires_at = datetime.utcnow() - timedelta(seconds=1)
        await db.commit()

    published = []

    async def publish(outbox_id: int):
        published.append(outbox_id)
        return True

    coordinator = CapabilityResumeCoordinator(
        db_factory=db_factory,
        publisher=publish,
    )
    await coordinator.run_once()

    assert published == [seed.outbox_id]
    async with db_factory() as db:
        task = await db.get(Task, seed.task_id)
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        assert task.status == "waiting_capability"
        assert outbox.status == ("claimed" if claimed else "ready")
        assert outbox.lease_token is None


@pytest.mark.asyncio
async def test_wrong_release_token_cannot_steal_publication(db_factory):
    async with db_factory() as db:
        seed = await _seed_resume(db)
    async with db_factory() as db:
        await materialize_resume_outbox(db, seed.outbox_id)
    async with db_factory() as db:
        envelope = await claim_resume_publication(db, seed.outbox_id)
    assert envelope is not None and envelope.lease_token is not None
    async with db_factory() as db:
        assert not await release_resume_publication(
            db,
            seed.outbox_id,
            lease_token="f" * 64,
            error_code="stolen",
            error_message="stolen",
        )
    async with db_factory() as db:
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        assert outbox.status == "claiming"
        assert outbox.lease_token == envelope.lease_token


@pytest.mark.asyncio
async def test_provider_boundary_and_terminal_settlement_are_idempotent(
    db_factory,
    monkeypatch,
):
    async with db_factory() as db:
        seed = await _seed_resume(db, invocation_status="ready")
    _install_fake_result(monkeypatch, seed.execution_id)
    async with db_factory() as db:
        await materialize_resume_outbox(db, seed.outbox_id)
    async with db_factory() as db:
        envelope = await claim_resume_publication(db, seed.outbox_id)
    assert envelope is not None and envelope.lease_token is not None
    claim = await _claim_turn(
        db_factory,
        seed,
        lease_token=envelope.lease_token,
        instance_id=51,
    )
    async with db_factory() as db:
        source = await db.get(LogEntry, claim.source_log_id)
        source.actual_transport = "claude_exec"
        await db.commit()

    assert await mark_resume_launch_boundary(
        db_factory,
        outbox_id=seed.outbox_id,
        task_id=seed.task_id,
        retry_count=claim.retry_count,
        turn_generation=claim.turn_generation,
        source_log_id=claim.source_log_id,
    )
    assert await mark_resume_launch_boundary(
        db_factory,
        outbox_id=seed.outbox_id,
        task_id=seed.task_id,
        retry_count=claim.retry_count,
        turn_generation=claim.turn_generation,
        source_log_id=claim.source_log_id,
    )

    async with capability_task_lock(seed.task_id):
        async with db_factory() as db:
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == seed.task_id)
                    .with_for_update()
                )
            ).scalar_one()
            assert await settle_previous_resume_in_terminal_tx(db, task)
            await db.commit()
    async with db_factory() as db:
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        invocation = await db.get(CapabilityInvocation, seed.invocation_id)
        assert outbox.status == "completed"
        assert outbox.resume_actual_transport == "claude_exec"
        assert outbox.lease_token is None
        assert invocation.status == "completed"
        completed = await load_resume_envelope(db, seed.outbox_id)
        assert completed is not None and completed.status == "completed"


@pytest.mark.asyncio
async def test_startup_reconciles_prelaunch_claim_without_creating_g_plus_two(
    db_factory,
):
    async with db_factory() as db:
        seed = await _seed_resume(db)
    async with db_factory() as db:
        await materialize_resume_outbox(db, seed.outbox_id)
    async with db_factory() as db:
        envelope = await claim_resume_publication(db, seed.outbox_id)
    assert envelope is not None and envelope.lease_token is not None
    claim = await _claim_turn(
        db_factory,
        seed,
        lease_token=envelope.lease_token,
        instance_id=61,
    )

    async with capability_task_lock(seed.task_id):
        async with db_factory() as db:
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == seed.task_id)
                    .with_for_update()
                )
            ).scalar_one()
            outcome = await reconcile_stale_resume_in_tx(
                db,
                task,
                has_live_runtime=False,
                unmanaged_pid=None,
            )
            await db.commit()
    assert outcome == "replayable"
    async with db_factory() as db:
        task = await db.get(Task, seed.task_id)
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        assert task.status == "waiting_capability"
        assert task.turn_generation == claim.turn_generation == 8
        assert task.turn_source_log_id == claim.source_log_id
        assert task.instance_id is None
        assert outbox.status == "claimed"
        assert outbox.lease_token is None


@pytest.mark.asyncio
@pytest.mark.parametrize("unmanaged_pid", [None, 43210])
async def test_startup_fails_closed_after_provider_boundary(
    db_factory,
    unmanaged_pid,
):
    async with db_factory() as db:
        seed = await _seed_resume(db)
    async with db_factory() as db:
        await materialize_resume_outbox(db, seed.outbox_id)
    async with db_factory() as db:
        envelope = await claim_resume_publication(db, seed.outbox_id)
    assert envelope is not None and envelope.lease_token is not None
    claim = await _claim_turn(
        db_factory,
        seed,
        lease_token=envelope.lease_token,
        instance_id=71,
    )
    async with db_factory() as db:
        source = await db.get(LogEntry, claim.source_log_id)
        source.actual_transport = "claude_exec"
        await db.commit()

    async with capability_task_lock(seed.task_id):
        async with db_factory() as db:
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == seed.task_id)
                    .with_for_update()
                )
            ).scalar_one()
            outcome = await reconcile_stale_resume_in_tx(
                db,
                task,
                has_live_runtime=False,
                unmanaged_pid=unmanaged_pid,
            )
            await db.commit()
    assert outcome == "failed"
    async with db_factory() as db:
        task = await db.get(Task, seed.task_id)
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        assert task.status == "failed"
        assert outbox.status == "failed"
        assert outbox.resume_actual_transport == "claude_exec"
        assert (task.instance_id is not None) is (unmanaged_pid is not None)


@pytest.mark.asyncio
async def test_startup_preserves_manager_owned_resume_runtime(db_factory):
    async with db_factory() as db:
        seed = await _seed_resume(db)
    async with db_factory() as db:
        await materialize_resume_outbox(db, seed.outbox_id)
    async with db_factory() as db:
        envelope = await claim_resume_publication(db, seed.outbox_id)
    assert envelope is not None and envelope.lease_token is not None
    claim = await _claim_turn(
        db_factory,
        seed,
        lease_token=envelope.lease_token,
        instance_id=81,
    )
    async with capability_task_lock(seed.task_id):
        async with db_factory() as db:
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == seed.task_id)
                    .with_for_update()
                )
            ).scalar_one()
            outcome = await reconcile_stale_resume_in_tx(
                db,
                task,
                has_live_runtime=True,
                unmanaged_pid=None,
            )
            await db.commit()
    assert outcome == "preserved"
    async with db_factory() as db:
        task = await db.get(Task, seed.task_id)
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        assert task.status == "executing"
        assert task.turn_generation == claim.turn_generation
        assert outbox.status == "claimed"
        assert outbox.lease_token == envelope.lease_token


@pytest.mark.asyncio
async def test_task_cancel_synchronously_cancels_only_unstarted_capability(
    db_factory,
):
    async with db_factory() as db:
        queued = await _seed_resume(db, invocation_status="queued")
    async with capability_task_lock(queued.task_id):
        async with db_factory() as db:
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == queued.task_id)
                    .with_for_update()
                )
            ).scalar_one()
            assert await cancel_task_resume_outbox_in_tx(
                db,
                task,
                reason="operator stopped Task",
            )
            await db.commit()
    async with db_factory() as db:
        invocation = await db.get(CapabilityInvocation, queued.invocation_id)
        execution = await db.get(CapabilityExecution, queued.execution_id)
        outbox = await db.get(CapabilityResumeOutbox, queued.outbox_id)
        assert invocation.status == "cancelled"
        assert execution.status == "cancelled"
        assert outbox.status == "cancelled"

    async with db_factory() as db:
        running = await _seed_resume(db, invocation_status="running")
    async with capability_task_lock(running.task_id):
        async with db_factory() as db:
            task = (
                await db.execute(
                    select(Task)
                    .where(Task.id == running.task_id)
                    .with_for_update()
                )
            ).scalar_one()
            with pytest.raises(CapabilityResumeConflictError):
                await cancel_task_resume_outbox_in_tx(
                    db,
                    task,
                    reason="operator stopped Task",
                )
            await db.rollback()
    async with db_factory() as db:
        invocation = await db.get(CapabilityInvocation, running.invocation_id)
        execution = await db.get(CapabilityExecution, running.execution_id)
        outbox = await db.get(CapabilityResumeOutbox, running.outbox_id)
        assert invocation.status == "running"
        assert execution.status == "running"
        assert outbox.status == "pending"


@pytest.mark.asyncio
async def test_direct_expired_claim_recovery_restores_exact_waiting_state(
    db_factory,
):
    async with db_factory() as db:
        seed = await _seed_resume(db)
    async with db_factory() as db:
        await materialize_resume_outbox(db, seed.outbox_id)
    async with db_factory() as db:
        envelope = await claim_resume_publication(db, seed.outbox_id)
    assert envelope is not None and envelope.lease_token is not None
    claim = await _claim_turn(
        db_factory,
        seed,
        lease_token=envelope.lease_token,
        instance_id=91,
    )
    async with db_factory() as db:
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        outbox.lease_expires_at = datetime.utcnow() - timedelta(seconds=1)
        await db.commit()
    async with db_factory() as db:
        assert await recover_expired_resume_publication(db, seed.outbox_id)
    async with db_factory() as db:
        task = await db.get(Task, seed.task_id)
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        assert task.status == "waiting_capability"
        assert task.turn_generation == claim.turn_generation
        assert task.turn_source_log_id == claim.source_log_id
        assert task.instance_id is None
        assert outbox.status == "claimed"
        assert outbox.lease_token is None


@pytest.mark.asyncio
async def test_release_never_replays_a_provider_crossed_claim(db_factory):
    async with db_factory() as db:
        seed = await _seed_resume(db)
    async with db_factory() as db:
        await materialize_resume_outbox(db, seed.outbox_id)
    async with db_factory() as db:
        envelope = await claim_resume_publication(db, seed.outbox_id)
    assert envelope is not None and envelope.lease_token is not None
    claim = await _claim_turn(
        db_factory,
        seed,
        lease_token=envelope.lease_token,
        instance_id=92,
    )
    async with db_factory() as db:
        source = await db.get(LogEntry, claim.source_log_id)
        source.actual_transport = "claude_exec"
        await db.commit()

    async with db_factory() as db:
        with pytest.raises(CapabilityResumeIntegrityError):
            await release_resume_publication(
                db,
                seed.outbox_id,
                lease_token=envelope.lease_token,
                error_code="unsafe_retry",
                error_message="must not replay",
            )
    async with db_factory() as db:
        task = await db.get(Task, seed.task_id)
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        assert task.status == "executing"
        assert task.turn_generation == claim.turn_generation
        assert outbox.status == "claimed"
        assert outbox.lease_token == envelope.lease_token


@pytest.mark.asyncio
async def test_coordinator_failure_persists_bounded_retry_backoff(db_factory):
    async with db_factory() as db:
        seed = await _seed_resume(db)
    async with db_factory() as db:
        await materialize_resume_outbox(db, seed.outbox_id)

    async def fail_publish(_outbox_id: int):
        raise RuntimeError("queue unavailable")

    coordinator = CapabilityResumeCoordinator(
        db_factory=db_factory,
        publisher=fail_publish,
        initial_backoff_seconds=2,
        max_backoff_seconds=8,
    )
    before = datetime.utcnow()
    await coordinator.run_once()

    async with db_factory() as db:
        outbox = await db.get(CapabilityResumeOutbox, seed.outbox_id)
        assert outbox.status == "ready"
        assert outbox.lease_token is None
        assert outbox.error_code == "resume_coordinator_retry"
        assert outbox.next_attempt_at is not None
        assert outbox.next_attempt_at >= before + timedelta(seconds=1)


@pytest.mark.asyncio
async def test_coordinator_start_wake_shutdown_is_idempotent(db_factory):
    async with db_factory() as db:
        seed = await _seed_resume(db)
    calls = []

    async def publish(outbox_id: int):
        calls.append(outbox_id)
        async with db_factory() as db:
            return await claim_resume_publication(db, outbox_id) is not None

    coordinator = CapabilityResumeCoordinator(
        db_factory=db_factory,
        publisher=publish,
        poll_interval_seconds=60,
    )
    await coordinator.start()
    assert coordinator.is_running
    assert calls == [seed.outbox_id]
    coordinator.wake()
    await coordinator.start()
    await coordinator.shutdown()
    await coordinator.shutdown()
    assert not coordinator.is_running


@pytest.mark.asyncio
async def test_resume_shutdown_settles_graph_under_anyio_cancellation(
    db_factory,
):
    from anyio import CancelScope

    async def publish(_outbox_id: int):
        return True

    coordinator = CapabilityResumeCoordinator(
        db_factory=db_factory,
        publisher=publish,
        poll_interval_seconds=60,
    )
    runner_started = asyncio.Event()
    callback_started = asyncio.Event()
    release = asyncio.Event()

    async def wait_for_release(started: asyncio.Event) -> None:
        started.set()
        await release.wait()

    runner = asyncio.create_task(wait_for_release(runner_started))
    callback = asyncio.create_task(wait_for_release(callback_started))
    coordinator._runner = runner
    coordinator._inflight[1] = callback

    async def release_graph() -> None:
        await runner_started.wait()
        await callback_started.wait()
        await asyncio.sleep(0)
        release.set()

    releaser = asyncio.create_task(release_graph())
    try:
        with CancelScope() as scope:
            scope.cancel()
            with pytest.raises(asyncio.CancelledError):
                await coordinator.shutdown()
        await releaser
    finally:
        release.set()
        await asyncio.gather(runner, callback, releaser, return_exceptions=True)

    assert runner.done()
    assert callback.done()
    assert coordinator._runner is None
