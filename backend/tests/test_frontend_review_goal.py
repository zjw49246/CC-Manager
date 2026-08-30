import asyncio
from unittest.mock import AsyncMock

import pytest

from backend.models.task import Task
from backend.services.frontend_review_goal import (
    FRONTEND_REVIEW_ACTIVATION_METADATA_KEY,
    FRONTEND_REVIEW_METADATA_KEY,
    build_frontend_review_goal_condition,
    build_frontend_review_goal_protocol,
    collect_frontend_review_goal_evidence,
    frontend_review_goal_config,
    frontend_review_goal_restore_snapshot,
    frontend_review_goal_terminal_updates,
    inspect_frontend_review_local_repository,
)


def test_frontend_review_goal_config_is_bounded_and_normalized():
    assert frontend_review_goal_config({
        FRONTEND_REVIEW_METADATA_KEY: {
            "mode": "goal",
            "profile": "unknown",
            "max_iterations": 99,
        },
    }) == {
        "mode": "goal",
        "profile": "standard",
        "max_iterations": 10,
    }


@pytest.mark.asyncio
async def test_repository_inspection_cancel_reaps_git_process_under_anyio(
    monkeypatch,
    tmp_path,
):
    from anyio import CancelScope

    scope_holder: dict[str, CancelScope] = {}
    killed = asyncio.Event()
    reaped = asyncio.Event()

    class FakeProcess:
        returncode = None
        communicate_calls = 0

        async def communicate(self):
            self.communicate_calls += 1
            if self.communicate_calls == 1:
                scope_holder["scope"].cancel()
                await asyncio.Future()
            await asyncio.sleep(0)
            self.returncode = -9
            reaped.set()
            return b"", b""

        def kill(self):
            killed.set()

    process = FakeProcess()

    async def create_process(*_args, **_kwargs):
        return process

    monkeypatch.setattr(
        "backend.services.frontend_review_goal.shutil.which",
        lambda _name: "/usr/bin/git",
    )
    monkeypatch.setattr(
        "backend.services.frontend_review_goal.asyncio.create_subprocess_exec",
        create_process,
    )
    task = Task(last_cwd=str(tmp_path))

    with CancelScope() as scope:
        scope_holder["scope"] = scope
        with pytest.raises(asyncio.CancelledError):
            await inspect_frontend_review_local_repository(
                task,
                AsyncMock(),
            )

    assert killed.is_set()
    assert reaped.is_set()
    assert process.communicate_calls == 2


def test_frontend_review_goal_condition_and_protocol_require_browser_recheck():
    condition = build_frontend_review_goal_condition("登录页也必须覆盖")
    protocol = build_frontend_review_goal_protocol({
        "mode": "goal",
        "profile": "standard",
        "max_iterations": 5,
    })

    assert "Browser Review" in condition
    assert "修改后" in condition
    assert "登录页也必须覆盖" in condition
    assert "ccm_workspace_review.test_current_changes" in protocol
    assert "check_current_changes_review" in protocol
    assert "cleanup_status=completed" in protocol
    assert "evidence_archive_state=complete" in protocol
    assert "安全上限为 5 轮" in protocol


def test_followup_frontend_review_goal_restores_prior_task_mode_state():
    task = Task(
        mode="plan",
        goal_condition="prior condition",
        goal_max_turns=17,
        goal_turns_used=4,
        goal_last_reason="prior reason",
        metadata_={"keep": "binding"},
    )
    snapshot = frontend_review_goal_restore_snapshot(task)
    task.mode = "goal"
    task.goal_condition = "temporary review"
    task.goal_max_turns = 5
    task.goal_turns_used = 2
    task.goal_last_reason = "review passed"
    task.metadata_ = {
        "keep": "binding",
        FRONTEND_REVIEW_METADATA_KEY: {
            "mode": "goal",
            "profile": "standard",
            "max_iterations": 5,
        },
        FRONTEND_REVIEW_ACTIVATION_METADATA_KEY: {
            "message": "review this branch",
            "file_paths": [],
            "secret_ids": [],
            "restore": snapshot,
        },
    }

    assert frontend_review_goal_terminal_updates(task) == {
        "mode": "plan",
        "goal_condition": "prior condition",
        "goal_max_turns": 17,
        "goal_turns_used": 4,
        "goal_last_reason": "prior reason",
        "metadata_": {"keep": "binding"},
    }


@pytest.mark.asyncio
async def test_frontend_review_evidence_gate_requires_a_real_run(monkeypatch):
    service = type(
        "FakeHarnessService",
        (),
        {"list_for_task": AsyncMock(return_value=[])},
    )()
    monkeypatch.setattr(
        "backend.services.test_harness.test_harness_service",
        service,
    )

    summary, ready, reason = await collect_frontend_review_goal_evidence(73)

    assert not ready
    assert "No Test Harness runs" in summary
    assert "至少完成一次" in reason


@pytest.mark.asyncio
async def test_frontend_review_evidence_gate_accepts_latest_completed_proof(monkeypatch):
    def run(run_id: str, report: str) -> dict:
        return {
            "id": run_id,
            "target_kind": "current_workspace",
            "status": "completed",
            "stage": "completed",
            "verdict": "passed",
            "stale": False,
            "cleanup_status": "completed",
            "evidence_archive_state": "complete",
            "report": report,
            "evidence": [{"kind": "screenshot", "name": "final.png"}],
            "findings": [],
        }

    service = type(
        "FakeHarnessService",
        (),
        {
            "list_for_task": AsyncMock(
                return_value=[
                    run("review-new", "## Passed"),
                    run("review-baseline", "## Baseline failure"),
                ]
            )
        },
    )()
    monkeypatch.setattr(
        "backend.services.test_harness.test_harness_service",
        service,
    )

    summary, ready, reason = await collect_frontend_review_goal_evidence(73)

    assert ready
    assert "screenshots=1" in summary
    assert "report=yes" in summary
    assert "Run 1 (LATEST authoritative run): id=review-new" in summary
    assert "Run 2 (older superseded run): id=review-baseline" in summary
    assert "only authoritative latest run" in summary
    assert "门禁已满足" in reason


@pytest.mark.asyncio
async def test_frontend_review_evidence_gate_rejects_incomplete_archive(monkeypatch):
    run = {
        "id": "review-incomplete",
        "target_kind": "current_workspace",
        "status": "completed",
        "stage": "completed",
        "verdict": "passed",
        "stale": False,
        "cleanup_status": "completed",
        "evidence_archive_state": "retryable_error",
        "report": "## Passed",
        "evidence": [{"kind": "screenshot", "name": "final.png"}],
        "findings": [],
    }
    service = type(
        "FakeHarnessService",
        (),
        {"list_for_task": AsyncMock(return_value=[run])},
    )()
    monkeypatch.setattr(
        "backend.services.test_harness.test_harness_service",
        service,
    )

    summary, ready, reason = await collect_frontend_review_goal_evidence(73)

    assert not ready
    assert "archive=retryable_error" in summary
    assert "尚未完成持久化归档" in reason
