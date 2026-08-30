"""Durable pre-PR Code Review capability lifecycle tests."""

import asyncio
from datetime import datetime, timedelta
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

from backend.config import settings
from backend.models.capability import CapabilityExecution, CapabilityInvocation
from backend.models.code_review import CodeReviewResult, CodeReviewRun
from backend.models.log_entry import LogEntry
from backend.models.task import Task
from backend.services import capability_service
from backend.services import code_review_capability as adapter_module
from backend.services.capability_registry import (
    register_capability,
    resolve_capability,
    unregister_capability,
)
from backend.services.code_review_capability import (
    CODE_REVIEW_RESULT_OUTPUT_KIND,
    CodeReviewCapabilityExecutor,
    code_review_capability_definition,
)
from backend.services.pr_review_runtime import (
    PRE_PR_CODE_REVIEW_TAG,
    is_pr_review_task,
    is_pr_sandbox_task,
)


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout.strip()


@pytest.fixture
def review_repo(tmp_path: Path) -> tuple[Path, str, str]:
    repo = tmp_path / "review-repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "CCM Test")
    _git(repo, "config", "user.email", "ccm@example.invalid")
    (repo / "app.py").write_text("value = 1\n", encoding="utf-8")
    _git(repo, "add", "app.py")
    _git(repo, "commit", "-m", "base")
    base_sha = _git(repo, "rev-parse", "HEAD")
    (repo / "app.py").write_text("value = 2\n", encoding="utf-8")
    _git(repo, "add", "app.py")
    _git(repo, "commit", "-m", "change")
    return repo, base_sha, _git(repo, "rev-parse", "HEAD")


@pytest.fixture(autouse=True)
def code_review_runtime(monkeypatch):
    previous_flag = settings.capability_core_enabled
    previous_definition = resolve_capability("code_review")
    settings.capability_core_enabled = True
    unregister_capability("code_review")
    register_capability(
        code_review_capability_definition(
            provider="claude",
            model="claude-opus-4-6",
            max_attempts=2,
        )
    )
    monkeypatch.setattr(
        capability_service,
        "broadcast_capability_event",
        AsyncMock(),
    )
    yield
    unregister_capability("code_review")
    if previous_definition is not None:
        register_capability(previous_definition)
    settings.capability_core_enabled = previous_flag


async def _create_invocation(
    db_session,
    review_repo: tuple[Path, str, str],
) -> tuple[Task, CapabilityInvocation]:
    repo, base_sha, head_sha = review_repo
    task = Task(
        title="Implement exact change",
        description="Modify app.py",
        status="completed",
        target_repo=str(repo),
        target_branch="main",
        provider="claude",
        model="claude-opus-4-6",
    )
    db_session.add(task)
    await db_session.commit()
    invocation, created = await capability_service.create_human_invocation(
        db_session,
        task_id=task.id,
        capability_key="code_review",
        request_payload={"base_sha": base_sha, "head_sha": head_sha},
        idempotency_key=f"code-review-{task.id}",
        requested_by_user_id=7,
    )
    assert created is True
    return task, invocation


async def _handled(
    db_session,
    invocation_id: int,
) -> tuple[CapabilityExecution, CodeReviewRun, Task]:
    execution = (
        await db_session.execute(
            select(CapabilityExecution)
            .where(CapabilityExecution.invocation_id == invocation_id)
            .order_by(CapabilityExecution.attempt.desc())
            .limit(1)
        )
    ).scalar_one()
    assert execution.handle_id is not None
    run = await db_session.get(CodeReviewRun, int(execution.handle_id))
    assert run is not None
    task = await db_session.get(Task, run.reviewer_task_id)
    assert task is not None
    return execution, run, task


def _structured_output(
    subject: dict,
    *,
    changes_requested: bool = False,
    omit_finding_title: bool = False,
) -> str:
    findings = []
    verdict = "pass"
    summary = "The exact commit range has no blocking issue."
    if changes_requested:
        verdict = "changes_required"
        summary = "The exact commit range has one blocking issue."
        findings = [
            {
                "severity": "high",
                "category": "correctness",
                "path": "app.py",
                "line": 1,
                "hunk": None,
                "title": "The new value violates the contract",
                "evidence": "The patch changes the required value from 1 to 2.",
                "impact": "Callers observe an invalid value.",
                "required_fix": "Preserve the required value or update callers.",
                "test": "Assert the public value expected by callers.",
            }
        ]
        if omit_finding_title:
            findings[0].pop("title")
    payload = {
        "schema_version": 1,
        "subject": subject,
        "role": "code_reviewer",
        "verdict": verdict,
        "summary": summary,
        "findings": findings,
    }
    return (
        "<ccm_code_review>\n"
        + json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n</ccm_code_review>"
    )


async def _complete_reviewer_task(
    db_session,
    reviewer_task: Task,
    content: str,
    *,
    event_type: str = "result",
    include_tool_event: bool = False,
) -> LogEntry:
    started_at = datetime.utcnow() - timedelta(seconds=3)
    output_at = datetime.utcnow() - timedelta(seconds=1)
    reviewer_task.status = "completed"
    reviewer_task.instance_id = 91
    reviewer_task.started_at = started_at
    reviewer_task.completed_at = datetime.utcnow()
    if include_tool_event:
        db_session.add(
            LogEntry(
                task_id=reviewer_task.id,
                instance_id=reviewer_task.instance_id,
                task_retry_count=reviewer_task.retry_count,
                event_type="tool_use",
                role="assistant",
                content="unexpected tool",
                timestamp=output_at - timedelta(milliseconds=1),
            )
        )
    output = LogEntry(
        task_id=reviewer_task.id,
        instance_id=reviewer_task.instance_id,
        task_retry_count=reviewer_task.retry_count,
        event_type=event_type,
        role="assistant",
        content=content,
        timestamp=output_at,
    )
    db_session.add(output)
    await db_session.commit()
    return output


@pytest.mark.asyncio
async def test_run_task_and_claim_are_one_transaction(
    db_session,
    review_repo,
    monkeypatch,
):
    developer, invocation = await _create_invocation(db_session, review_repo)
    developer_id = developer.id
    invocation_id = invocation.id
    executor = CodeReviewCapabilityExecutor()
    def reject_claim(*_args, **_kwargs):
        raise capability_service.CapabilityConflictError("claim lost")

    # Fail after the adapter callback has flushed both the reviewer Task and
    # CodeReviewRun. Core must roll that staging work back with the claim.
    monkeypatch.setattr(capability_service, "_claim_locked_execution", reject_claim)

    with pytest.raises(capability_service.CapabilityConflictError, match="claim lost"):
        await executor.ensure_started(db_session, invocation_id=invocation_id)

    assert await db_session.scalar(select(func.count(CodeReviewRun.id))) == 0
    assert await db_session.scalar(select(func.count(Task.id))) == 1
    assert await db_session.get(Task, developer_id) is not None
    execution = await capability_service.active_execution_for(
        db_session, invocation_id
    )
    assert execution is not None and execution.status == "queued"
    assert execution.handle_id is None


@pytest.mark.asyncio
async def test_started_review_replays_one_tool_free_task(
    db_session,
    review_repo,
):
    _developer, invocation = await _create_invocation(db_session, review_repo)
    wake = AsyncMock()
    executor = CodeReviewCapabilityExecutor(wake_callback=wake)

    first = await executor.ensure_started(db_session, invocation_id=invocation.id)
    second = await executor.recover(db_session, invocation_id=invocation.id)

    assert first.status == second.status == "running"
    assert first.run_id == second.run_id
    assert first.reviewer_task_id == second.reviewer_task_id
    assert await db_session.scalar(select(func.count(CodeReviewRun.id))) == 1
    assert await db_session.scalar(select(func.count(Task.id))) == 2
    _execution, run, reviewer_task = await _handled(db_session, invocation.id)
    assert reviewer_task.max_retries == 0
    assert reviewer_task.enabled_skills == {}
    assert reviewer_task.enable_workflows is False
    assert is_pr_sandbox_task(reviewer_task) is True
    assert is_pr_review_task(reviewer_task) is False
    assert PRE_PR_CODE_REVIEW_TAG in reviewer_task.tags
    assert run.prompt_hash == adapter_module._review_task_prompt_hash(reviewer_task)
    assert wake.await_count == 1

    # Runtime sandbox classification is not a PR Monitor ACL identity.  The
    # requesting member remains the creator of this ordinary Capability child,
    # so direct access and the ordinary Task list must agree.
    from backend.api.deps import require_task_access
    from backend.services.pr_monitor_task_access import is_pr_monitor_owned_task
    from backend.services.task_queue import TaskQueue

    assert reviewer_task.created_by == 7
    assert not await is_pr_monitor_owned_task(db_session, reviewer_task)
    request = SimpleNamespace(
        state=SimpleNamespace(
            user_id=7,
            user_role="member",
            auth_type="jwt",
        ),
        headers={},
    )
    await require_task_access(request, reviewer_task, db_session)
    visible_ids = {
        task.id
        for task in await TaskQueue(db_session).list_tasks(
            user_id=7,
            include_archived=True,
        )
    }
    assert reviewer_task.id in visible_ids


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes_requested,expected_verdict,expected_findings",
    [
        (False, "approved", 0),
        (True, "changes_requested", 1),
    ],
)
async def test_exact_terminal_output_creates_immutable_result(
    db_session,
    review_repo,
    changes_requested,
    expected_verdict,
    expected_findings,
):
    _developer, invocation = await _create_invocation(db_session, review_repo)
    executor = CodeReviewCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    _execution, run, reviewer_task = await _handled(db_session, invocation.id)
    output = await _complete_reviewer_task(
        db_session,
        reviewer_task,
        _structured_output(
            run.subject_ref,
            changes_requested=changes_requested,
        ),
    )

    ready = await executor.observe(db_session, invocation_id=invocation.id)

    assert ready.status == "ready"
    assert ready.verdict == expected_verdict
    assert ready.findings_count == expected_findings
    result = await db_session.get(CodeReviewResult, ready.result_id)
    assert result is not None
    assert result.output_log_id == output.id
    assert result.reviewer_task_retry_count == 0
    assert result.reviewer_task_started_at == reviewer_task.started_at
    assert result.subject_ref == run.subject_ref
    assert result.subject_hash == run.subject_hash
    assert len(result.result_hash) == 64
    execution = await db_session.get(CapabilityExecution, ready.execution_id)
    assert execution is not None
    assert execution.output_kind == CODE_REVIEW_RESULT_OUTPUT_KIND
    assert execution.output_id == result.id
    assert execution.output_hash == result.result_hash


@pytest.mark.asyncio
async def test_duplicate_mirror_of_same_result_is_not_ambiguous(
    db_session,
    review_repo,
):
    _developer, invocation = await _create_invocation(db_session, review_repo)
    executor = CodeReviewCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    _execution, run, reviewer_task = await _handled(db_session, invocation.id)
    content = _structured_output(run.subject_ref)
    first = await _complete_reviewer_task(
        db_session,
        reviewer_task,
        content,
        event_type="message",
    )
    terminal = LogEntry(
        task_id=reviewer_task.id,
        instance_id=reviewer_task.instance_id,
        task_retry_count=reviewer_task.retry_count,
        event_type="result",
        role="assistant",
        content=content,
        timestamp=first.timestamp + timedelta(milliseconds=1),
    )
    db_session.add(terminal)
    reviewer_task.completed_at = terminal.timestamp + timedelta(milliseconds=1)
    await db_session.commit()

    ready = await executor.observe(db_session, invocation_id=invocation.id)
    result = await db_session.get(CodeReviewResult, ready.result_id)
    assert result is not None and result.output_log_id == terminal.id


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_kind", ["wrong_subject", "tool_use", "wrong_generation"])
async def test_invalid_or_untrusted_output_fails_attempt_and_retries(
    db_session,
    review_repo,
    invalid_kind,
):
    _developer, invocation = await _create_invocation(db_session, review_repo)
    executor = CodeReviewCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    first_execution, run, reviewer_task = await _handled(db_session, invocation.id)
    subject = dict(run.subject_ref)
    if invalid_kind == "wrong_subject":
        subject["head_sha"] = "f" * 40
    output = await _complete_reviewer_task(
        db_session,
        reviewer_task,
        _structured_output(subject),
        include_tool_event=invalid_kind == "tool_use",
    )
    if invalid_kind == "wrong_generation":
        output.task_retry_count = reviewer_task.retry_count + 1
        await db_session.commit()

    retrying = await executor.observe(db_session, invocation_id=invocation.id)

    assert retrying.status == "queued"
    assert retrying.execution_id != first_execution.id
    failed = await db_session.get(CapabilityExecution, first_execution.id)
    assert failed is not None and failed.status == "failed"
    failed_run = await db_session.get(CodeReviewRun, run.id)
    assert failed_run is not None and failed_run.status == "failed"
    assert await db_session.scalar(select(func.count(CodeReviewResult.id))) == 0


@pytest.mark.asyncio
async def test_schema_failure_retry_receives_actionable_correction(
    db_session,
    review_repo,
):
    _developer, invocation = await _create_invocation(db_session, review_repo)
    executor = CodeReviewCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    first_execution, first_run, first_reviewer = await _handled(
        db_session,
        invocation.id,
    )
    await _complete_reviewer_task(
        db_session,
        first_reviewer,
        _structured_output(
            first_run.subject_ref,
            changes_requested=True,
            omit_finding_title=True,
        ),
    )

    retrying = await executor.observe(db_session, invocation_id=invocation.id)

    assert retrying.status == "queued"
    assert "missing required fields: title" in (retrying.error_message or "")
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    second_execution, second_run, second_reviewer = await _handled(
        db_session,
        invocation.id,
    )
    assert second_execution.attempt == 2
    assert second_execution.id != first_execution.id
    assert second_reviewer.id != first_reviewer.id
    assert "## Retry correction" in (second_reviewer.description or "")
    assert "`title`" in (second_reviewer.description or "")
    assert second_run.prompt_hash != first_run.prompt_hash

    await _complete_reviewer_task(
        db_session,
        second_reviewer,
        _structured_output(second_run.subject_ref, changes_requested=True),
    )
    ready = await executor.observe(db_session, invocation_id=invocation.id)

    assert ready.status == "ready"
    assert ready.verdict == "changes_requested"


@pytest.mark.asyncio
async def test_different_valid_results_fail_closed_as_ambiguous(
    db_session,
    review_repo,
):
    _developer, invocation = await _create_invocation(db_session, review_repo)
    executor = CodeReviewCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    _execution, run, reviewer_task = await _handled(db_session, invocation.id)
    first = await _complete_reviewer_task(
        db_session,
        reviewer_task,
        _structured_output(run.subject_ref),
    )
    second = LogEntry(
        task_id=reviewer_task.id,
        instance_id=reviewer_task.instance_id,
        task_retry_count=reviewer_task.retry_count,
        event_type="result",
        role="assistant",
        content=_structured_output(run.subject_ref, changes_requested=True),
        timestamp=first.timestamp + timedelta(milliseconds=1),
    )
    db_session.add(second)
    reviewer_task.completed_at = second.timestamp + timedelta(milliseconds=1)
    await db_session.commit()

    retrying = await executor.observe(db_session, invocation_id=invocation.id)
    assert retrying.status == "queued"
    assert "multiple different" in (retrying.error_message or "")


@pytest.mark.asyncio
async def test_subject_move_marks_invocation_stale_without_accepting_output(
    db_session,
    review_repo,
    monkeypatch,
):
    repo, _base, _head = review_repo
    _developer, invocation = await _create_invocation(db_session, review_repo)
    executor = CodeReviewCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    execution, run, reviewer_task = await _handled(db_session, invocation.id)
    stop = AsyncMock()
    monkeypatch.setattr(adapter_module, "_terminate_exact_reviewer_task", stop)
    (repo / "next.py").write_text("next_value = 3\n", encoding="utf-8")
    _git(repo, "add", "next.py")
    _git(repo, "commit", "-m", "move head")

    stale = await executor.observe(db_session, invocation_id=invocation.id)

    assert stale.status == "stale"
    assert stale.error_code == "code_review_subject_stale"
    stop.assert_awaited_once()
    persisted_execution = await db_session.get(CapabilityExecution, execution.id)
    assert persisted_execution is not None and persisted_execution.status == "stale"
    persisted_run = await db_session.get(CodeReviewRun, run.id)
    assert persisted_run is not None and persisted_run.status == "stale"
    assert await db_session.scalar(select(func.count(CodeReviewResult.id))) == 0


@pytest.mark.asyncio
async def test_ready_result_becomes_stale_but_preserves_completed_output(
    db_session,
    review_repo,
):
    repo, _base, _head = review_repo
    _developer, invocation = await _create_invocation(db_session, review_repo)
    executor = CodeReviewCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    execution, run, reviewer_task = await _handled(db_session, invocation.id)
    await _complete_reviewer_task(
        db_session,
        reviewer_task,
        _structured_output(run.subject_ref),
    )
    ready = await executor.observe(db_session, invocation_id=invocation.id)
    assert ready.status == "ready"
    result_id = ready.result_id
    output_hash = ready.output_hash

    (repo / "after-review.py").write_text("changed = True\n", encoding="utf-8")
    _git(repo, "add", "after-review.py")
    _git(repo, "commit", "-m", "advance after review")

    stale = await executor.observe(db_session, invocation_id=invocation.id)

    assert stale.status == "stale"
    assert stale.error_code == "code_review_subject_stale"
    assert stale.result_id == result_id
    assert stale.output_hash == output_hash
    persisted_execution = await db_session.get(
        CapabilityExecution,
        execution.id,
        populate_existing=True,
    )
    persisted_run = await db_session.get(
        CodeReviewRun,
        run.id,
        populate_existing=True,
    )
    assert persisted_execution is not None
    assert persisted_execution.status == "completed"
    assert persisted_execution.output_id == result_id
    assert persisted_execution.output_hash == output_hash
    assert persisted_run is not None and persisted_run.status == "completed"
    assert await db_session.scalar(select(func.count(CodeReviewResult.id))) == 1


@pytest.mark.asyncio
async def test_developer_generation_change_before_capture_is_stale(
    db_session,
    review_repo,
):
    developer, invocation = await _create_invocation(db_session, review_repo)
    developer.retry_count += 1
    await db_session.commit()

    stale = await CodeReviewCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation.id,
    )

    assert stale.status == "stale"
    assert await db_session.scalar(select(func.count(CodeReviewRun.id))) == 0
    assert await db_session.scalar(select(func.count(Task.id))) == 1


@pytest.mark.asyncio
async def test_developer_generation_change_at_atomic_admission_leaves_no_orphan(
    db_session,
    review_repo,
    monkeypatch,
):
    developer, invocation = await _create_invocation(db_session, review_repo)
    developer_id = developer.id
    invocation_id = invocation.id
    original = adapter_module.stage_and_claim_execution
    changed = False

    async def change_generation_before_lock(db, **kwargs):
        nonlocal changed
        if not changed:
            changed = True
            locked_out_task = await db.get(Task, developer_id)
            assert locked_out_task is not None
            locked_out_task.retry_count += 1
            await db.commit()
        return await original(db, **kwargs)

    monkeypatch.setattr(
        adapter_module,
        "stage_and_claim_execution",
        change_generation_before_lock,
    )

    stale = await CodeReviewCapabilityExecutor().ensure_started(
        db_session,
        invocation_id=invocation_id,
    )

    assert stale.status == "stale"
    assert await db_session.scalar(select(func.count(CodeReviewRun.id))) == 0
    assert await db_session.scalar(select(func.count(Task.id))) == 1


@pytest.mark.asyncio
async def test_completion_revalidates_reviewer_generation_inside_atomic_lock(
    db_session,
    review_repo,
    monkeypatch,
):
    _developer, invocation = await _create_invocation(db_session, review_repo)
    executor = CodeReviewCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    _execution, _run, reviewer_task = await _handled(db_session, invocation.id)
    await _complete_reviewer_task(
        db_session,
        reviewer_task,
        _structured_output(_run.subject_ref),
    )
    reviewer_task_id = reviewer_task.id
    original = adapter_module.validate_and_complete_execution
    changed = False

    async def change_generation_before_lock(db, **kwargs):
        nonlocal changed
        if not changed:
            changed = True
            mutable_reviewer = await db.get(Task, reviewer_task_id)
            assert mutable_reviewer is not None
            mutable_reviewer.retry_count += 1
            await db.commit()
        return await original(db, **kwargs)

    monkeypatch.setattr(
        adapter_module,
        "validate_and_complete_execution",
        change_generation_before_lock,
    )

    failed = await executor.observe(db_session, invocation_id=invocation.id)

    assert failed.status == "failed"
    assert failed.error_code == "code_review_handle_invalid"
    assert await db_session.scalar(select(func.count(CodeReviewResult.id))) == 0


@pytest.mark.asyncio
async def test_concurrent_observers_publish_one_result(
    db_session,
    db_factory,
    review_repo,
    monkeypatch,
):
    _developer, invocation = await _create_invocation(db_session, review_repo)
    executor = CodeReviewCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    _execution, run, reviewer_task = await _handled(db_session, invocation.id)
    await _complete_reviewer_task(
        db_session,
        reviewer_task,
        _structured_output(run.subject_ref),
    )
    invocation_id = invocation.id
    await db_session.rollback()

    original = adapter_module.validate_and_complete_execution
    both_arrived = asyncio.Event()
    arrivals = 0

    async def synchronize_completion(db, **kwargs):
        nonlocal arrivals
        arrivals += 1
        if arrivals == 2:
            both_arrived.set()
        await both_arrived.wait()
        return await original(db, **kwargs)

    monkeypatch.setattr(
        adapter_module,
        "validate_and_complete_execution",
        synchronize_completion,
    )

    async def observe_once():
        async with db_factory() as session:
            return await executor.observe(session, invocation_id=invocation_id)

    first, second = await asyncio.wait_for(
        asyncio.gather(observe_once(), observe_once()),
        timeout=10,
    )

    assert first.status == second.status == "ready"
    assert first.result_id == second.result_id
    assert await db_session.scalar(select(func.count(CodeReviewResult.id))) == 1


@pytest.mark.asyncio
async def test_cancellation_uses_authoritative_exact_generation(
    db_session,
    review_repo,
    monkeypatch,
):
    _developer, invocation = await _create_invocation(db_session, review_repo)
    executor = CodeReviewCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    execution, run, reviewer_task = await _handled(db_session, invocation.id)
    observed_generation = adapter_module.local_task_generation(reviewer_task)
    calls = []

    async def authoritative_stop(task_id, db, **kwargs):
        calls.append((task_id, kwargs))
        task = await db.get(Task, task_id)
        task.status = "completed"
        task.completed_at = datetime.utcnow()
        await db.commit()

    monkeypatch.setattr(
        adapter_module,
        "terminate_authoritative_task_generation",
        authoritative_stop,
    )

    cancelled = await executor.cancel(db_session, invocation_id=invocation.id)

    assert cancelled.status == "cancelled"
    assert calls == [
        (
            reviewer_task.id,
                {
                    "reason": "Code Review capability was cancelled",
                    "expected_local_generation": observed_generation,
                    "allow_delivery_effect_stop": True,
                },
        )
    ]
    persisted_execution = await db_session.get(CapabilityExecution, execution.id)
    persisted_run = await db_session.get(CodeReviewRun, run.id)
    assert persisted_execution is not None
    assert persisted_execution.status == "cancelled"
    assert persisted_run is not None and persisted_run.status == "cancelled"


@pytest.mark.asyncio
async def test_cancellation_generation_mismatch_stays_cancelling(
    db_session,
    review_repo,
    monkeypatch,
):
    _developer, invocation = await _create_invocation(db_session, review_repo)
    executor = CodeReviewCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    execution, run, reviewer_task = await _handled(db_session, invocation.id)

    async def reject_stale_stop(*_args, **_kwargs):
        raise adapter_module.TaskTerminationConflict("generation changed")

    monkeypatch.setattr(
        adapter_module,
        "terminate_authoritative_task_generation",
        reject_stale_stop,
    )

    with pytest.raises(adapter_module.CodeReviewCancellationUnconfirmed):
        await executor.cancel(db_session, invocation_id=invocation.id)

    persisted_invocation = await db_session.get(
        CapabilityInvocation,
        invocation.id,
        populate_existing=True,
    )
    persisted_execution = await db_session.get(
        CapabilityExecution,
        execution.id,
        populate_existing=True,
    )
    persisted_run = await db_session.get(
        CodeReviewRun,
        run.id,
        populate_existing=True,
    )
    persisted_reviewer = await db_session.get(
        Task,
        reviewer_task.id,
        populate_existing=True,
    )
    assert persisted_invocation is not None
    assert persisted_invocation.status == "cancelling"
    assert persisted_execution is not None
    assert persisted_execution.status == "cancelling"
    assert persisted_run is not None and persisted_run.status == "running"
    assert persisted_reviewer is not None
    assert persisted_reviewer.status == "pending"


@pytest.mark.asyncio
async def test_reviewer_retry_generation_change_is_never_claimed(
    db_session,
    review_repo,
):
    _developer, invocation = await _create_invocation(db_session, review_repo)
    executor = CodeReviewCapabilityExecutor()
    await executor.ensure_started(db_session, invocation_id=invocation.id)
    _execution, run, reviewer_task = await _handled(db_session, invocation.id)
    reviewer_task.retry_count += 1
    await db_session.commit()

    failed = await executor.observe(db_session, invocation_id=invocation.id)

    assert failed.status == "failed"
    assert failed.error_code == "code_review_handle_invalid"
    persisted_run = await db_session.get(CodeReviewRun, run.id)
    # The handle could not be trusted enough to mutate its audit row.
    assert persisted_run is not None and persisted_run.status == "running"
