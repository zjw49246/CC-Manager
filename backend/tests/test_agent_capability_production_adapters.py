"""End-to-end admission into the production Plan and Review adapters."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from backend.config import settings
from backend.models.capability import CapabilityExecution, CapabilityInvocation
from backend.models.code_review import CodeReviewRun
from backend.models.log_entry import LogEntry
from backend.models.plan import Plan
from backend.models.plan_agent import PlanAgentRun
from backend.models.task import Task
from backend.schemas.plan import default_plan_pipeline_config
from backend.services import plan_capability as plan_adapter
from backend.services.agent_capability_admission import (
    AgentTerminalExpectation,
    admit_agent_terminal_action,
)
from backend.services.capability_coordinator import CapabilityCoordinator
from backend.services.capability_protocol import (
    TERMINAL_ACTION_CLOSE_TAG,
    TERMINAL_ACTION_OPEN_TAG,
)
from backend.services.capability_registry import (
    register_capability,
    resolve_capability,
    unregister_capability,
)
from backend.services.code_review_capability import (
    CodeReviewCapabilityExecutor,
    code_review_capability_definition,
)
from backend.services.plan_capability import (
    PlanCapabilityExecutor,
    plan_capability_definition,
)
from backend.services.terminal_arbitration import bind_turn_source


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
    repo = tmp_path / "agent-review-repo"
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
def production_capability_runtime(monkeypatch):
    previous_plan = resolve_capability("plan")
    previous_review = resolve_capability("code_review")
    monkeypatch.setattr(settings, "capability_core_enabled", True)
    monkeypatch.setattr(settings, "auto_capability_enabled", True)
    unregister_capability("plan")
    unregister_capability("code_review")

    pipeline = default_plan_pipeline_config().model_dump(mode="json")
    pipeline["reviewer"]["enabled"] = True
    register_capability(
        plan_capability_definition(
            executor=PlanCapabilityExecutor(wake_callback=AsyncMock()),
            pipeline_config=pipeline,
            max_attempts=2,
        )
    )
    register_capability(
        code_review_capability_definition(
            executor=CodeReviewCapabilityExecutor(
                wake_callback=AsyncMock(),
            ),
            provider="claude",
            model="claude-opus-4-6",
            max_attempts=2,
        )
    )
    monkeypatch.setattr(
        plan_adapter,
        "capture_repo_revision",
        AsyncMock(return_value={"available": True, "head": "abc"}),
    )
    monkeypatch.setattr(plan_adapter, "broadcast_plan_event", AsyncMock())
    yield

    unregister_capability("plan")
    unregister_capability("code_review")
    if previous_plan is not None:
        register_capability(previous_plan)
    if previous_review is not None:
        register_capability(previous_review)


def _terminal_action(capability: str, request: dict[str, str]) -> str:
    payload = json.dumps(
        {
            "schema_version": 1,
            "terminal_action": "request_capability",
            "capability": capability,
            "reason": f"Need {capability} guidance",
            "request": request,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return (
        "Yielding to the requested capability.\n"
        f"{TERMINAL_ACTION_OPEN_TAG}{payload}{TERMINAL_ACTION_CLOSE_TAG}"
    )


async def _admit_agent_request(
    db_session,
    *,
    capability: str,
    request: dict[str, str],
    target_repo: str,
) -> tuple[Task, CapabilityInvocation]:
    task = Task(
        title=f"Agent requests {capability}",
        description="Implement the exact requested change",
        status="executing",
        mode="auto",
        retry_count=1,
        turn_generation=4,
        instance_id=17,
        provider="claude",
        model="claude-opus-4-6",
        target_repo=target_repo,
        target_branch="main",
        session_id=f"session-{capability}",
        capability_policy={
            "version": 1,
            "max_invocations": 2,
            "capabilities": {"plan": 1, "code_review": 1},
        },
    )
    db_session.add(task)
    await db_session.flush()
    source = await bind_turn_source(
        db_session,
        task,
        None,
        instance_id=task.instance_id,
    )
    source.actual_transport = "claude_exec"
    output = LogEntry(
        instance_id=task.instance_id,
        task_id=task.id,
        task_retry_count=task.retry_count,
        task_turn_generation=task.turn_generation,
        turn_scope="foreground",
        event_type="result",
        role=None,
        content=_terminal_action(capability, request),
        is_error=False,
    )
    db_session.add(output)
    await db_session.commit()

    admitted = await admit_agent_terminal_action(
        db_session,
        expected=AgentTerminalExpectation(
            task_id=task.id,
            task_incarnation_id=task.incarnation_id,
            retry_count=task.retry_count,
            turn_generation=task.turn_generation,
            instance_id=task.instance_id,
            source_log_id=source.id,
        ),
    )
    assert admitted.outcome == "waiting_capability"
    assert admitted.invocation_id is not None
    stored_task = await db_session.get(Task, task.id, populate_existing=True)
    invocation = await db_session.get(
        CapabilityInvocation,
        admitted.invocation_id,
        populate_existing=True,
    )
    assert stored_task.status == "waiting_capability"
    assert invocation.source == "agent_request"
    assert invocation.status == "queued"
    return stored_task, invocation


def _coordinator(db_factory) -> CapabilityCoordinator:
    return CapabilityCoordinator(
        db_factory=db_factory,
        poll_interval_seconds=60,
        max_concurrency=2,
        scan_limit=8,
        initial_backoff_seconds=0.01,
        max_backoff_seconds=0.02,
    )


@pytest.mark.asyncio
async def test_agent_request_starts_plan_while_task_remains_waiting(
    db_session,
    db_factory,
):
    task, invocation = await _admit_agent_request(
        db_session,
        capability="plan",
        request={
            "prompt": "Produce a safe implementation plan",
            "title": "Safe implementation plan",
        },
        target_repo="/repo",
    )

    await _coordinator(db_factory).run_once()

    task = await db_session.get(Task, task.id, populate_existing=True)
    invocation = await db_session.get(
        CapabilityInvocation,
        invocation.id,
        populate_existing=True,
    )
    execution = await db_session.scalar(
        select(CapabilityExecution).where(
            CapabilityExecution.invocation_id == invocation.id
        )
    )
    run = await db_session.scalar(
        select(PlanAgentRun).where(
            PlanAgentRun.capability_execution_id == execution.id
        )
    )
    plan = await db_session.get(Plan, run.plan_id)

    assert task.status == "waiting_capability"
    assert invocation.status == "running"
    assert execution.status == "running"
    assert execution.handle_kind == "plan_agent_run"
    assert run.status == "queued"
    assert run.run_type == "capability"
    assert plan.target_task_id == task.id


@pytest.mark.asyncio
async def test_agent_request_starts_code_review_while_task_remains_waiting(
    db_session,
    db_factory,
    review_repo,
):
    repo, base_sha, head_sha = review_repo
    task, invocation = await _admit_agent_request(
        db_session,
        capability="code_review",
        request={"base_sha": base_sha, "head_sha": head_sha},
        target_repo=str(repo),
    )

    await _coordinator(db_factory).run_once()

    task = await db_session.get(Task, task.id, populate_existing=True)
    invocation = await db_session.get(
        CapabilityInvocation,
        invocation.id,
        populate_existing=True,
    )
    execution = await db_session.scalar(
        select(CapabilityExecution).where(
            CapabilityExecution.invocation_id == invocation.id
        )
    )
    run = await db_session.scalar(
        select(CodeReviewRun).where(
            CodeReviewRun.capability_execution_id == execution.id
        )
    )
    reviewer_task = await db_session.get(Task, run.reviewer_task_id)

    assert task.status == "waiting_capability"
    assert invocation.status == "running"
    assert execution.status == "running"
    assert execution.handle_kind == "code_review_run"
    assert run.status == "running"
    assert run.developer_task_id == task.id
    assert reviewer_task.status == "pending"
    assert reviewer_task.max_retries == 0
