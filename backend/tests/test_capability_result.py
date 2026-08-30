"""DB-only tests for exact Capability result materialization."""

from datetime import datetime
import json

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.capability import CapabilityExecution, CapabilityInvocation
from backend.models.code_review import CodeReviewResult, CodeReviewRun
from backend.models.plan import Plan, PlanVersion
from backend.models.plan_agent import PlanAgentRun
from backend.models.task import Task
from backend.services.capability_result import resolve_capability_result
from backend.services.capability_service import CapabilityConflictError
from backend.services.plan_capability import plan_version_output_hash


_DIGEST = "a" * 64


async def _invocation_and_execution(
    db: AsyncSession,
    *,
    capability_key: str,
) -> tuple[Task, CapabilityInvocation, CapabilityExecution]:
    task = Task(title=f"{capability_key} result owner")
    db.add(task)
    await db.flush()
    invocation = CapabilityInvocation(
        task_id=task.id,
        capability_key=capability_key,
        source="human_request",
        purpose="advisory",
        status="queued",
        state_version=1,
        idempotency_key=f"result-{capability_key}",
        input_payload={"prompt": "resolve exact output"},
        input_hash=_DIGEST,
        subject_kind="task",
        subject_ref={"task_id": task.id},
        subject_hash="b" * 64,
        executor_kind=f"{capability_key}_executor",
        executor_config={},
        executor_config_hash="c" * 64,
        policy_snapshot={},
        policy_hash="d" * 64,
        resume_policy="attach_only",
        max_attempts=2,
        active_task_id=task.id,
    )
    db.add(invocation)
    await db.flush()
    execution = CapabilityExecution(
        invocation_id=invocation.id,
        attempt=1,
        status="queued",
        state_version=1,
        active_invocation_id=invocation.id,
        idempotency_key=f"result-{capability_key}:attempt:1",
        executor_kind=invocation.executor_kind,
        input_hash=invocation.input_hash,
    )
    db.add(execution)
    await db.flush()
    return task, invocation, execution


def _publish_result(
    invocation: CapabilityInvocation,
    execution: CapabilityExecution,
    *,
    kind: str,
    output_id: int,
    output_hash: str,
) -> None:
    now = datetime.utcnow()
    invocation.status = "ready"
    invocation.state_version = 2
    invocation.result_kind = kind
    invocation.result_id = output_id
    invocation.result_hash = output_hash
    invocation.ready_at = now
    execution.status = "completed"
    execution.state_version = 2
    execution.active_invocation_id = None
    execution.output_kind = kind
    execution.output_id = output_id
    execution.output_hash = output_hash
    execution.completed_at = now


async def _review_graph(
    db: AsyncSession,
) -> tuple[CapabilityInvocation, CapabilityExecution, CodeReviewRun, CodeReviewResult]:
    task, invocation, execution = await _invocation_and_execution(
        db,
        capability_key="code_review",
    )
    now = datetime.utcnow()
    reviewer = Task(
        title="Exact reviewer",
        status="completed",
        started_at=now,
        completed_at=now,
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
        base_sha="1" * 40,
        head_sha="2" * 40,
        head_tree_sha="3" * 40,
        patch_sha256="4" * 64,
        subject_ref={"kind": "commit_range", "head_sha": "2" * 40},
        subject_hash="5" * 64,
        prompt_hash="6" * 64,
        completed_at=now,
    )
    db.add(run)
    await db.flush()
    result_hash = "7" * 64
    result = CodeReviewResult(
        run_id=run.id,
        capability_invocation_id=invocation.id,
        capability_execution_id=execution.id,
        developer_task_id=task.id,
        reviewer_task_id=reviewer.id,
        reviewer_task_retry_count=0,
        reviewer_task_instance_id=None,
        reviewer_task_started_at=now,
        reviewer_task_completed_at=now,
        output_log_id=17,
        schema_version=1,
        role="reviewer",
        verdict="changes_requested",
        summary="One exact finding",
        findings=[{"severity": "high", "title": "Keep identity exact"}],
        subject_ref=run.subject_ref,
        subject_hash=run.subject_hash,
        result_hash=result_hash,
    )
    db.add(result)
    await db.flush()
    execution.handle_kind = "code_review_run"
    execution.handle_id = str(run.id)
    _publish_result(
        invocation,
        execution,
        kind="code_review_result",
        output_id=result.id,
        output_hash=result_hash,
    )
    await db.commit()
    return invocation, execution, run, result


async def _plan_graph(
    db: AsyncSession,
) -> tuple[CapabilityInvocation, CapabilityExecution, PlanAgentRun, PlanVersion]:
    task, invocation, execution = await _invocation_and_execution(
        db,
        capability_key="plan",
    )
    plan = Plan(
        title="Exact Plan",
        initial_request="Plan safely",
        target_task_id=task.id,
        pipeline_config={},
    )
    db.add(plan)
    await db.flush()
    run = PlanAgentRun(
        plan_id=plan.id,
        capability_execution_id=execution.id,
        run_type="capability",
        status="completed",
        current_stage="complete",
        finished_at=datetime.utcnow(),
    )
    db.add(run)
    await db.flush()
    version = PlanVersion(
        plan_id=plan.id,
        version_number=1,
        produced_by_run_id=run.id,
        content="# Immutable exact Plan",
    )
    db.add(version)
    await db.flush()
    run.result_version_id = version.id
    plan.current_version_id = version.id
    execution.handle_kind = "plan_agent_run"
    execution.handle_id = str(run.id)
    _publish_result(
        invocation,
        execution,
        kind="plan_version",
        output_id=version.id,
        output_hash=plan_version_output_hash(version),
    )
    await db.commit()
    return invocation, execution, run, version


@pytest.mark.asyncio
async def test_review_result_is_json_safe_and_detached(db_session):
    invocation, execution, _run, result = await _review_graph(db_session)

    resolved = await resolve_capability_result(db_session, invocation)
    payload = resolved.as_payload()

    assert resolved.execution_id == execution.id
    assert payload["kind"] == "code_review_result"
    assert payload["data"]["reviewer_task_completed_at"] == (
        result.reviewer_task_completed_at.isoformat()
    )
    json.dumps(payload, allow_nan=False)

    payload["data"]["findings"][0]["title"] = "caller mutation"
    assert resolved.data["findings"][0]["title"] == "Keep identity exact"
    stored = await db_session.get(
        CodeReviewResult,
        result.id,
        populate_existing=True,
    )
    assert stored is not None
    assert stored.findings[0]["title"] == "Keep identity exact"


@pytest.mark.asyncio
async def test_result_tuple_must_name_the_exact_completed_execution(db_session):
    invocation, _execution, _run, _result = await _review_graph(db_session)
    invocation.result_hash = "8" * 64
    await db_session.commit()

    with pytest.raises(CapabilityConflictError, match="exact completed execution"):
        await resolve_capability_result(db_session, invocation)


@pytest.mark.asyncio
async def test_review_result_rejects_broken_reverse_identity(db_session):
    invocation, _execution, run, result = await _review_graph(db_session)
    run.reviewer_task_id = invocation.task_id
    await db_session.commit()
    assert run.reviewer_task_id != result.reviewer_task_id

    with pytest.raises(CapabilityConflictError, match="identity"):
        await resolve_capability_result(db_session, invocation)


@pytest.mark.asyncio
async def test_review_result_rejects_wrong_capability_kind(db_session):
    invocation, _execution, _run, _result = await _review_graph(db_session)
    invocation.capability_key = "plan"
    await db_session.commit()

    with pytest.raises(CapabilityConflictError, match="identity"):
        await resolve_capability_result(db_session, invocation)


@pytest.mark.asyncio
async def test_plan_result_verifies_run_chain_and_authoritative_hash(db_session):
    invocation, execution, run, version = await _plan_graph(db_session)

    resolved = await resolve_capability_result(db_session, invocation)
    assert resolved.execution_id == execution.id
    assert resolved.data["content"] == "# Immutable exact Plan"

    run.capability_execution_id = None
    await db_session.commit()
    with pytest.raises(CapabilityConflictError, match="identity"):
        await resolve_capability_result(db_session, invocation)

    run.capability_execution_id = execution.id
    version.content = "# Tampered after completion"
    await db_session.commit()
    with pytest.raises(CapabilityConflictError, match="authoritative PlanVersion"):
        await resolve_capability_result(db_session, invocation)
