"""Independent reviewer panel, structured finding, and CI Gate tests."""

import json
import base64
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from backend.models.log_entry import LogEntry
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRMonitorRun,
    PRReview,
    PRReviewerRun,
)
from backend.models.task import Task
from backend.services import pr_review_panel
from backend.services import pr_review_service
from backend.services.pr_monitor_loop import attach_review_to_run
from backend.tests.worker_termination_helpers import (
    persist_active_worker_receipt,
)


BASE_SHA = "a" * 40
HEAD_SHA = "b" * 40
PR_DATA = {
    "number": 17,
    "base_sha": BASE_SHA,
    "head_sha": HEAD_SHA,
    "delivery_id": "panel-delivery-17",
    "title": "Panel change",
    "author": "alice",
    "url": "https://github.com/owner/repo/pull/17",
}


def _context():
    return {
        "repo_name": "owner/repo",
        "pr_number": 17,
        "base_ref": "main",
        "base_sha": BASE_SHA,
        "head_sha": HEAD_SHA,
        "guidance": {
            "CLAUDE.md": "Keep the gate strict.",
            # Simulate a context prepared by an older binary. It must not be
            # injected unless an exact-base manifest assigned it to a role.
            "PROGRESS.md": "LEGACY_PROGRESS_SENTINEL",
        },
        "material": {
            "number": 17,
            "title": "Panel change",
            "body": "",
            "author": "alice",
            "base_ref": "main",
            "head_ref": "feature",
            "files": [{"path": "backend/example.py", "additions": 2, "deletions": 1}],
            "patch": "diff --git a/backend/example.py b/backend/example.py\n",
            "changed_file_contents": [{
                "path": "backend/example.py",
                "base": {"present": True, "available": True, "content": "OLD_FULL_FILE_SENTINEL"},
                "head": {"present": True, "available": True, "content": "NEW_FULL_FILE_SENTINEL"},
            }],
        },
    }


@pytest.mark.asyncio
async def test_panel_review_and_run_are_one_admission_transaction(db_session):
    repo = MonitoredRepo(
        repo_full_name="owner/repo", webhook_secret="s" * 64,
        provider="claude", review_mode="panel", wait_for_ci=False,
    )
    db_session.add(repo)
    await db_session.commit()

    with patch.object(pr_review_panel, "_wake_dispatcher") as wake:
        review = await pr_review_service.create_pr_review_task(
            db_session, repo, PR_DATA, prepared_context=_context(),
        )
    run = (await db_session.execute(select(PRMonitorRun))).scalar_one()
    assert review.monitor_run_id == run.id
    assert run.current_review_id == review.id
    assert run.current_base_sha == BASE_SHA
    assert run.current_head_sha == HEAD_SHA
    wake.assert_called_once_with()


@pytest.mark.asyncio
async def test_panel_attach_failure_rolls_back_review_tasks_and_never_wakes(
    db_session,
):
    repo = MonitoredRepo(
        repo_full_name="owner/repo", webhook_secret="s" * 64,
        provider="claude", review_mode="panel", wait_for_ci=False,
    )
    db_session.add(repo)
    await db_session.commit()

    with (
        patch(
            "backend.services.pr_monitor_loop.attach_review_to_run",
            AsyncMock(side_effect=RuntimeError("simulated attach crash")),
        ),
        patch.object(pr_review_panel, "_wake_dispatcher") as wake,
    ):
        with pytest.raises(RuntimeError, match="attach crash"):
            await pr_review_service.create_pr_review_task(
                db_session, repo, PR_DATA, prepared_context=_context(),
            )
    await db_session.rollback()

    assert list((await db_session.execute(select(PRReview))).scalars()) == []
    assert list((await db_session.execute(select(PRReviewerRun))).scalars()) == []
    assert list((await db_session.execute(select(Task))).scalars()) == []
    assert list((await db_session.execute(select(PRMonitorRun))).scalars()) == []
    wake.assert_not_called()


def _output(role: str, *, blocker: bool = False) -> str:
    findings = []
    verdict = "pass"
    if blocker:
        verdict = "changes_required"
        findings = [{
            "severity": "medium",
            "category": "concurrency",
            "path": "backend/example.py",
            "line": 12,
            "hunk": None,
            "title": "Lost wake-up",
            "evidence": "The state commit happens after the wake call.",
            "impact": "A restart can strand the review.",
            "required_fix": "Commit the state before waking the dispatcher.",
            "test": "Crash after commit and assert startup recovery wakes it.",
        }]
    value = {
        "schema_version": 1,
        "subject": {"kind": "pr_head", "base_sha": BASE_SHA, "head_sha": HEAD_SHA},
        "role": role,
        "verdict": verdict,
        "summary": f"{role} completed",
        "findings": findings,
    }
    return (
        "PR_REVIEW_PANEL_BEGIN\n"
        + json.dumps(value, separators=(",", ":"))
        + "\nPR_REVIEW_PANEL_END\nPR_REVIEW_RESULT: panel_complete"
    )


async def _create_recoverable_panel_run(
    db_session,
    *,
    worker_id: int | None,
) -> tuple[PRReview, PRReviewerRun, Task]:
    repo = MonitoredRepo(
        repo_full_name=f"owner/recovery-{worker_id}",
        webhook_secret="s" * 64,
        provider="claude",
        review_mode="panel",
        wait_for_ci=False,
    )
    db_session.add(repo)
    await db_session.flush()
    review = PRReview(
        repo_id=repo.id,
        pr_number=17,
        base_ref="main",
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        pr_title="Recovery review",
        pr_author="alice",
        pr_url=f"https://github.com/owner/recovery-{worker_id}/pull/17",
        status="reviewing",
        action_nonce="a" * 48,
    )
    db_session.add(review)
    await db_session.flush()
    task = Task(
        title="recoverable reviewer",
        description="immutable review",
        status="completed",
        provider="claude",
        worker_id=worker_id,
        retry_count=0,
        started_at=datetime.utcnow(),
        completed_at=datetime.utcnow(),
        tags=["pr-review"],
    )
    waiting_task = Task(
        title="still-running reviewer",
        description="immutable review",
        status="pending",
        provider="claude",
        worker_id=worker_id,
        retry_count=0,
        tags=["pr-review"],
    )
    db_session.add_all([task, waiting_task])
    await db_session.flush()
    run = PRReviewerRun(
        pr_review_id=review.id,
        role="principal_engineer",
        task_id=task.id,
        provider="claude",
        status="pending",
        prompt_policy_hash="b" * 64,
        guide_pack_hash="c" * 64,
    )
    waiting_run = PRReviewerRun(
        pr_review_id=review.id,
        role="senior_engineer",
        task_id=waiting_task.id,
        provider="claude",
        status="pending",
        prompt_policy_hash="d" * 64,
        guide_pack_hash="e" * 64,
    )
    db_session.add_all([run, waiting_run])
    await db_session.commit()
    return review, run, task


async def _create_cancelled_reviewer_runtime(
    db_session,
    *,
    worker_id: int | None,
) -> tuple[PRReview, PRReviewerRun, Task]:
    review, failed_run, _failed_task = await _create_recoverable_panel_run(
        db_session,
        worker_id=worker_id,
    )
    cancelled_run = await db_session.scalar(
        select(PRReviewerRun).where(
            PRReviewerRun.pr_review_id == review.id,
            PRReviewerRun.id != failed_run.id,
        )
    )
    assert cancelled_run is not None
    task = await db_session.get(Task, cancelled_run.task_id)
    assert task is not None
    now = datetime.utcnow()
    review.status = "error"
    review.action_taken = "error"
    review.completed_at = now
    failed_run.status = "error"
    failed_run.completed_at = now
    cancelled_run.status = "cancelled"
    cancelled_run.completed_at = now
    task.status = "executing"
    task.started_at = now
    await db_session.commit()
    return review, cancelled_run, task


def test_panel_prompts_share_engineering_standard_and_keep_distinct_litmus():
    prompts = {}
    for role in pr_review_panel.REVIEWER_ROLES:
        prompt, policy_hash, guide_hash = pr_review_panel.build_panel_review_prompt(
            repo_name="owner/repo",
            pr_number=17,
            base_sha=BASE_SHA,
            head_sha=HEAD_SHA,
            role=role,
            guidance=_context()["guidance"],
            material=_context()["material"],
        )
        assert len(policy_hash) == 64
        assert len(guide_hash) == 64
        prompts[role] = prompt

    shared_requirements = (
        "Honor cohesion within a module; reject unrelated coupling",
        "Honor clear layers; reject dependency tangles",
        "An application must never call its own HTTP endpoint",
        "Honor capability reuse; reject copy-and-rebuild",
        "Honor unit extension; reject feature sprawl",
        "Honor one established pattern; reject each contributor inventing another",
        "Honor timely deletion of dead code; reject preserving old baggage",
        "Honor the simplest sufficient design; reject speculative over-design",
        "author can either fix it or rebut it with concrete evidence",
    )
    for prompt in prompts.values():
        normalized_prompt = " ".join(prompt.split())
        for requirement in shared_requirements:
            assert " ".join(requirement.split()) in normalized_prompt

    normalized = {
        role: " ".join(prompt.split()) for role, prompt in prompts.items()
    }
    assert "Persona: Principal Engineer — design review, big scope" in normalized["principal_engineer"]
    assert "Never claim repo-wide evidence you were not given" in normalized["principal_engineer"]
    assert "adding a second way to do a solved thing" in normalized["principal_engineer"]
    assert "Persona: Senior Engineer — logic, implementation, and quality" in normalized["senior_engineer"]
    assert "Read every supplied patch" in normalized["senior_engineer"]
    assert "an untestable seam, or a security mistake" in normalized["senior_engineer"]
    assert "Persona: QA Engineer — does it work, is it tested, will it break?" in normalized["qa_engineer"]
    assert "tests that fake the expected result" in normalized["qa_engineer"]
    for prompt in prompts.values():
        assert "NEW_FULL_FILE_SENTINEL" not in prompt
        assert "OLD_FULL_FILE_SENTINEL" not in prompt
        assert "Keep the gate strict." not in prompt
        assert "LEGACY_PROGRESS_SENTINEL" not in prompt
        assert "no `CLAUDE.md`, `AGENTS.md`, `PROGRESS.md`" in prompt


def test_panel_prompts_include_only_manifest_guides_for_each_role():
    guidance = {
        "docs/shared.md": "SHARED_GUIDE_SENTINEL",
        "docs/principal.md": "PRINCIPAL_GUIDE_SENTINEL",
        "docs/senior.md": "SENIOR_GUIDE_SENTINEL",
        "docs/qa.md": "QA_GUIDE_SENTINEL",
        pr_review_service._GUIDANCE_ROLE_MAP_KEY: {
            "docs/shared.md": list(pr_review_panel.REVIEWER_ROLES),
            "docs/principal.md": ["principal_engineer"],
            "docs/senior.md": ["senior_engineer"],
            "docs/qa.md": ["qa_engineer"],
        },
    }
    sentinels = {
        "principal_engineer": "PRINCIPAL_GUIDE_SENTINEL",
        "senior_engineer": "SENIOR_GUIDE_SENTINEL",
        "qa_engineer": "QA_GUIDE_SENTINEL",
    }

    for role in pr_review_panel.REVIEWER_ROLES:
        prompt, _policy_hash, _guide_hash = (
            pr_review_panel.build_panel_review_prompt(
                repo_name="owner/repo",
                pr_number=17,
                base_sha=BASE_SHA,
                head_sha=HEAD_SHA,
                role=role,
                guidance=guidance,
                material=_context()["material"],
            )
        )
        assert "SHARED_GUIDE_SENTINEL" in prompt
        assert sentinels[role] in prompt
        for other_role, sentinel in sentinels.items():
            if other_role != role:
                assert sentinel not in prompt


def test_panel_prompts_keep_the_complete_patch_within_budget():
    context = _context()
    exact_patch = "PATCH_BEGIN\n" + ("changed line\n" * 2_000) + "PATCH_END"
    context["material"]["patch"] = exact_patch

    prompt_set = pr_review_panel._build_panel_prompt_set(
        repo_name="owner/repo",
        pr_number=17,
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        provider="codex",
        guidance=context["guidance"],
        material=context["material"],
    )

    for prompt, _policy_hash, _guide_hash in prompt_set.values():
        rendered = prompt.split(
            "<ccm_verified_pr_material>\n", 1
        )[1].split("\n</ccm_verified_pr_material>", 1)[0]
        assert json.loads(rendered)["patch"] == exact_patch


def test_panel_prompt_budget_rejects_every_role_before_task_staging():
    context = _context()
    context["material"]["patch"] = "x" * (
        pr_review_service._CODEX_PR_REVIEW_PROMPT_MAX_CHARS + 1
    )

    with pytest.raises(
        pr_review_service.PRReviewInputTooLarge,
        match="unsupported_input_size",
    ):
        pr_review_panel._build_panel_prompt_set(
            repo_name="owner/repo",
            pr_number=17,
            base_sha=BASE_SHA,
            head_sha=HEAD_SHA,
            provider="codex",
            guidance=context["guidance"],
            material=context["material"],
        )


@pytest.mark.asyncio
async def test_oversized_panel_stages_no_review_run_or_task(db_session):
    repo = MonitoredRepo(
        repo_full_name="owner/repo",
        webhook_secret="s" * 64,
        provider="codex",
        review_mode="panel",
        wait_for_ci=False,
    )
    db_session.add(repo)
    await db_session.commit()
    context = _context()
    context["material"]["patch"] = "x" * (
        pr_review_service._CODEX_PR_REVIEW_PROMPT_MAX_CHARS + 1
    )

    with pytest.raises(
        pr_review_service.PRReviewInputTooLarge,
        match="unsupported_input_size",
    ):
        await pr_review_service.create_pr_review_task(
            db_session,
            repo,
            PR_DATA,
            prepared_context=context,
        )

    assert not db_session.new
    assert list((await db_session.execute(select(PRReview))).scalars()) == []
    assert list((await db_session.execute(select(PRReviewerRun))).scalars()) == []
    assert list((await db_session.execute(select(Task))).scalars()) == []
    assert list((await db_session.execute(select(PRMonitorRun))).scalars()) == []


@pytest.mark.asyncio
async def test_pr_files_rest_pagination_captures_all_266_paths_and_rename():
    pages = []
    for start, stop in ((0, 100), (100, 200), (200, 266)):
        page = []
        for index in range(start, stop):
            item = {
                "filename": f"src/file-{index}.py",
                "status": "modified",
                "additions": index + 1,
                "deletions": index,
            }
            page.append(item)
        pages.append(page)
    pages[-1][-1] = {
        "filename": "src/new-name.py",
        "previous_filename": "src/old-name.py",
        "status": "renamed",
        "additions": 3,
        "deletions": 2,
    }

    api = AsyncMock(side_effect=pages)
    with patch.object(pr_review_service, "_gh_api_value", api):
        files = await pr_review_service._fetch_pr_files(
            repo_name="owner/repo",
            pr_number=17,
            changed_files=266,
        )

    assert len(files) == 266
    assert files[0] == {
        "path": "src/file-0.py",
        "additions": 1,
        "deletions": 0,
    }
    assert files[-1] == {
        "path": "src/new-name.py",
        "previous_path": "src/old-name.py",
        "additions": 3,
        "deletions": 2,
    }
    assert [call.args[0] for call in api.await_args_list] == [
        "repos/owner/repo/pulls/17/files?per_page=100&page=1",
        "repos/owner/repo/pulls/17/files?per_page=100&page=2",
        "repos/owner/repo/pulls/17/files?per_page=100&page=3",
    ]


@pytest.mark.asyncio
async def test_pr_files_rest_pagination_rejects_count_mismatch_and_duplicates():
    first_page = [
        {
            "filename": f"src/file-{index}.py",
            "status": "modified",
            "additions": 1,
            "deletions": 0,
        }
        for index in range(100)
    ]
    with patch.object(
        pr_review_service,
        "_gh_api_value",
        AsyncMock(side_effect=[first_page, []]),
    ):
        with pytest.raises(
            pr_review_service.GhError,
            match="does not match changedFiles",
        ):
            await pr_review_service._fetch_pr_files(
                repo_name="owner/repo",
                pr_number=17,
                changed_files=101,
            )

    duplicate = {
        "filename": "src/same.py",
        "status": "modified",
        "additions": 1,
        "deletions": 0,
    }
    with patch.object(
        pr_review_service,
        "_gh_api_value",
        AsyncMock(return_value=[duplicate, duplicate]),
    ):
        with pytest.raises(pr_review_service.GhError, match="duplicate paths"):
            await pr_review_service._fetch_pr_files(
                repo_name="owner/repo",
                pr_number=17,
                changed_files=2,
            )


@pytest.mark.asyncio
async def test_pr_files_rest_pagination_rejects_more_than_capture_limit():
    api = AsyncMock()
    with patch.object(pr_review_service, "_gh_api_value", api):
        with pytest.raises(
            pr_review_service.GhError,
            match="more than 300 files",
        ):
            await pr_review_service._fetch_pr_files(
                repo_name="owner/repo",
                pr_number=17,
                changed_files=301,
            )
    api.assert_not_awaited()


def test_parse_panel_output_enforces_subject_role_and_blocking_verdict():
    parsed = pr_review_panel.parse_panel_output(
        _output("qa_engineer", blocker=True),
        role="qa_engineer",
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
    )
    assert parsed["verdict"] == "changes_required"
    assert parsed["findings"][0]["severity"] == "medium"

    wrong = _output("senior_engineer").replace(HEAD_SHA, "c" * 40)
    with pytest.raises(ValueError, match="subject"):
        pr_review_panel.parse_panel_output(
            wrong,
            role="senior_engineer",
            base_sha=BASE_SHA,
            head_sha=HEAD_SHA,
        )


def test_gate_body_is_readable_and_keeps_finding_details_in_threads():
    runs = [
        PRReviewerRun(
            id=index,
            pr_review_id=1,
            role=role,
            provider="claude",
            status=("changes_required" if role == "qa_engineer" else "passed"),
            result_body=f"{role} short summary",
            prompt_policy_hash=str(index) * 64,
            guide_pack_hash=str(index) * 64,
        )
        for index, role in enumerate(pr_review_panel.REVIEWER_ROLES, start=1)
    ]
    finding = PRFinding(
        pr_review_id=1,
        reviewer_run_id=runs[-1].id,
        fingerprint="f" * 64,
        role="qa_engineer",
        severity="medium",
        category="correctness",
        path="backend/example.py",
        line=12,
        hunk=None,
        title="Public body should not repeat this title",
        evidence="PRIVATE_EVIDENCE_DETAIL",
        impact="PRIVATE_IMPACT_DETAIL",
        required_fix="PRIVATE_FIX_DETAIL",
        test="PRIVATE_TEST_DETAIL",
        status="open",
        thread_nonce="n" * 64,
        thread_status="pending",
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
    )

    body = pr_review_panel._render_gate_body(runs, [finding])

    assert body.startswith("# CCM reviewer panel: changes required")
    assert "1 open blocking finding" in body
    assert "### Principal engineer — Passed" in body
    assert "### Senior engineer — Passed" in body
    assert "### QA engineer — Changes required" in body
    assert "Finding count: 1 total (1 blocking, 0 advisory)." in body
    assert "dedicated finding threads/comments" in body
    assert "Public body should not repeat this title" not in body
    assert "PRIVATE_EVIDENCE_DETAIL" not in body
    assert "PRIVATE_IMPACT_DETAIL" not in body
    assert "PRIVATE_FIX_DETAIL" not in body
    assert "PRIVATE_TEST_DETAIL" not in body
    for internal_protocol in (
        "PR_REVIEW_",
        "schema_version",
        '"subject"',
        '"verdict"',
        '"findings"',
    ):
        assert internal_protocol not in body

    runs[-1].status = "passed"
    clean_body = pr_review_panel._render_gate_body(runs, [])
    assert clean_body.startswith("# CCM reviewer panel: passed")
    assert "no open blocking findings remain" in clean_body
    assert clean_body.count("Finding count: none.") == 3

    runs[-1].status = "error"
    with pytest.raises(ValueError, match="no terminal code verdict"):
        pr_review_panel._render_gate_body(runs, [])


@pytest.mark.asyncio
async def test_panel_creates_three_independent_tasks_and_gates_findings(
    db_session,
    db_factory,
):
    repo = MonitoredRepo(
        repo_full_name="owner/repo",
        webhook_secret="s" * 64,
        provider="claude",
        review_model="claude-sonnet-4-6",
        review_mode="panel",
        wait_for_ci=False,
        auto_merge=False,
        default_branch="main",
        allowed_authors=[],
    )
    db_session.add(repo)
    await db_session.commit()
    review = await pr_review_panel.create_pr_review_panel(
        db_session,
        repo,
        PR_DATA,
        prepared_context=_context(),
    )
    runs = list((await db_session.execute(
        select(PRReviewerRun)
        .where(PRReviewerRun.pr_review_id == review.id)
        .order_by(PRReviewerRun.id)
    )).scalars())
    assert [run.role for run in runs] == list(pr_review_panel.REVIEWER_ROLES)
    assert len({run.task_id for run in runs}) == 3
    tasks = [await db_session.get(Task, run.task_id) for run in runs]
    assert all(task.status == "pending" for task in tasks)
    assert all(task.archived is True for task in tasks)
    assert all(task.execution_user_id is None for task in tasks)
    assert all(task.execution_user_role == "member" for task in tasks)
    assert all(task.execution_mode == "sandbox" for task in tasks)
    assert all(task.execution_principal_kind == "system" for task in tasks)
    review_id = review.id
    run_task_specs = [
        (run.id, run.role, task.id)
        for run, task in zip(runs, tasks)
    ]
    assert all("filesystem, shell, network, GitHub" in task.description for task in tasks)
    from backend.api.tasks import _require_pr_review_chat_allowed
    for task in tasks:
        with pytest.raises(HTTPException) as blocked:
            await _require_pr_review_chat_allowed(db_session, task.id)
        assert blocked.value.status_code == 409

    with (
            patch(
                "backend.services.pr_review_service._gh_authenticated_login",
                AsyncMock(return_value="ccm-reviewer"),
            ),
            patch(
                "backend.services.pr_review_service._freeze_safe_merge_method",
                AsyncMock(return_value="merge"),
            ),
        patch(
            "backend.services.pr_review_service._resume_publishing_review",
            AsyncMock(),
        ) as publish,
    ):
        for index, (run_id, role, task_id) in enumerate(run_task_specs):
            task = await db_session.get(Task, task_id, populate_existing=True)
            now = datetime.utcnow()
            task.status = "completed"
            task.started_at = now
            task.completed_at = now
            expected_background_generation = None
            if index == len(run_task_specs) - 1:
                expected_background_generation = (
                    "panel-background-generation"
                )
                task.pty_background_generation = (
                    expected_background_generation
                )
            db_session.add(LogEntry(
                task_id=task.id,
                task_retry_count=task.retry_count,
                event_type="result",
                role="assistant",
                content=_output(role, blocker=index == 2),
                timestamp=now,
            ))
            await db_session.commit()
            await pr_review_panel.check_and_update_reviewer_run(
                db_session,
                reviewer_run_id=run_id,
                task_id=task_id,
                retry_count=task.retry_count,
                db_factory=db_factory,
                expected_background_generation=(
                    expected_background_generation
                ),
            )

    refreshed = await db_session.get(PRReview, review_id, populate_existing=True)
    findings = list((await db_session.execute(
        select(PRFinding).where(PRFinding.pr_review_id == review_id)
    )).scalars())
    assert refreshed.status == "publishing"
    assert refreshed.pending_action == "review_comments"
    assert len(findings) == 1
    assert findings[0].role == "qa_engineer"
    publish.assert_not_awaited()
    terminal_task = await db_session.get(
        Task,
        run_task_specs[-1][2],
        populate_existing=True,
    )
    assert (
        terminal_task.pty_background_generation
        == "panel-background-generation"
    )


@pytest.mark.asyncio
async def test_clean_panel_arms_frozen_direct_auto_merge(
    db_session,
    db_factory,
):
    repo = MonitoredRepo(
        repo_full_name="owner/repo",
        webhook_secret="s" * 64,
        provider="claude",
        review_model="claude-sonnet-4-6",
        review_mode="panel",
        wait_for_ci=False,
        auto_merge=True,
        merge_queue_mode="manual",
        default_branch="main",
        allowed_authors=[],
    )
    db_session.add(repo)
    await db_session.commit()
    review = await pr_review_panel.create_pr_review_panel(
        db_session,
        repo,
        PR_DATA,
        prepared_context=_context(),
    )
    review_id = review.id
    runs = list((await db_session.execute(
        select(PRReviewerRun)
        .where(PRReviewerRun.pr_review_id == review_id)
        .order_by(PRReviewerRun.id)
    )).scalars())
    tasks = [await db_session.get(Task, run.task_id) for run in runs]
    assert all(task.metadata_["pr_auto_merge"] is True for task in tasks)
    run_task_specs = [
        (run.id, run.role, task.id, task.retry_count)
        for run, task in zip(runs, tasks)
    ]
    freeze = AsyncMock(side_effect=[
        pr_review_service.GhError("GitHub API HTTP 503"),
        "fast-forward",
    ])

    with (
            patch(
                "backend.services.pr_review_service._gh_authenticated_login",
                AsyncMock(return_value="ccm-reviewer"),
            ),
            patch(
                "backend.services.pr_review_service._freeze_safe_merge_method",
                freeze,
            ),
        patch(
            "backend.services.pr_review_service._resume_publishing_review",
            AsyncMock(),
        ) as publish,
    ):
        for run_id, role, task_id, retry_count in run_task_specs:
            task = await db_session.get(
                Task,
                task_id,
                populate_existing=True,
            )
            now = datetime.utcnow()
            task.status = "completed"
            task.started_at = now
            task.completed_at = now
            db_session.add(LogEntry(
                task_id=task.id,
                task_retry_count=task.retry_count,
                event_type="result",
                role="assistant",
                content=_output(role),
                timestamp=now,
            ))
            await db_session.commit()
            result = await pr_review_panel.check_and_update_reviewer_run(
                db_session,
                reviewer_run_id=run_id,
                task_id=task_id,
                retry_count=retry_count,
                db_factory=db_factory,
            )
            if role == pr_review_panel.REVIEWER_ROLES[-1]:
                assert result is False
                transient = await db_session.get(
                    PRReview,
                    review_id,
                    populate_existing=True,
                )
                assert transient.status == "reviewing"
                result = await pr_review_panel.check_and_update_reviewer_run(
                    db_session,
                    reviewer_run_id=run_id,
                    task_id=task_id,
                    retry_count=retry_count,
                    db_factory=db_factory,
                )
                assert result is True

    refreshed = await db_session.get(
        PRReview,
        review_id,
        populate_existing=True,
    )
    assert refreshed.status == "publishing"
    assert refreshed.pending_action == "approved_merged"
    assert refreshed.merge_method == "fast-forward"
    assert freeze.await_count == 2
    publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_panel_freezes_final_role_before_github_identity_and_retries(
    db_session,
    db_factory,
):
    repo = MonitoredRepo(
        repo_full_name="owner/repo",
        webhook_secret="s" * 64,
        provider="claude",
        review_model="claude-sonnet-4-6",
        review_mode="panel",
        wait_for_ci=False,
        auto_merge=False,
        default_branch="main",
        allowed_authors=[],
    )
    db_session.add(repo)
    await db_session.commit()
    review = await pr_review_panel.create_pr_review_panel(
        db_session,
        repo,
        PR_DATA,
        prepared_context=_context(),
    )
    review_id = review.id
    runs = list((await db_session.execute(
        select(PRReviewerRun)
        .where(PRReviewerRun.pr_review_id == review_id)
        .order_by(PRReviewerRun.id)
    )).scalars())
    specs = [
        (item.id, item.role, item.task_id)
        for item in runs
    ]
    identity_observations = 0

    async def fail_identity_after_observing_commit():
        nonlocal identity_observations
        identity_observations += 1
        async with db_factory() as observer:
            stored_review = await observer.get(PRReview, review_id)
            stored_runs = list((await observer.execute(
                select(PRReviewerRun)
                .where(PRReviewerRun.pr_review_id == review_id)
                .order_by(PRReviewerRun.id)
            )).scalars())
            stored_findings = list((await observer.execute(
                select(PRFinding).where(PRFinding.pr_review_id == review_id)
            )).scalars())
            assert stored_review is not None
            assert stored_review.status == "reviewing"
            assert stored_review.publication_state == "reconciling"
            assert [item.verdict for item in stored_runs] == [
                "pass",
                "pass",
                "changes_required",
            ]
            assert stored_runs[-1].status == "reviewing"
            assert len(stored_findings) == 1
            assert stored_findings[0].reviewer_run_id == stored_runs[-1].id
        raise pr_review_service.GhError("gh auth unavailable")

    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(side_effect=fail_identity_after_observing_commit),
        ),
        patch.object(
            pr_review_service,
            "_resume_publishing_review",
            AsyncMock(),
        ) as publish,
    ):
        results = []
        for index, (run_id, role, task_id) in enumerate(specs):
            task = await db_session.get(Task, task_id, populate_existing=True)
            now = datetime.utcnow()
            task.status = "completed"
            task.started_at = now
            task.completed_at = now
            db_session.add(LogEntry(
                task_id=task.id,
                task_retry_count=task.retry_count,
                event_type="result",
                role="assistant",
                content=_output(role, blocker=index == 2),
                timestamp=now,
            ))
            await db_session.commit()
            results.append(await pr_review_panel.check_and_update_reviewer_run(
                db_session,
                reviewer_run_id=run_id,
                task_id=task_id,
                retry_count=task.retry_count,
                db_factory=db_factory,
            ))

    assert results == [True, True, False]
    assert identity_observations == 1
    publish.assert_not_awaited()
    transient = await db_session.get(
        PRReview,
        review_id,
        populate_existing=True,
    )
    assert transient.status == "reviewing"
    assert transient.publication_state == "reconciling"
    assert transient.failure_stage == "github_identity"
    assert "gh auth unavailable" in transient.publication_error

    final_run_id, _role, final_task_id = specs[-1]
    final_task = await db_session.get(
        Task,
        final_task_id,
        populate_existing=True,
    )
    with (
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            AsyncMock(return_value="ccm-reviewer"),
        ),
        patch.object(
            pr_review_service,
            "_resume_publishing_review",
            AsyncMock(),
        ) as publish,
    ):
        assert await pr_review_panel.check_and_update_reviewer_run(
            db_session,
            reviewer_run_id=final_run_id,
            task_id=final_task_id,
            retry_count=final_task.retry_count,
            db_factory=db_factory,
        ) is True

    completed = await db_session.get(
        PRReview,
        review_id,
        populate_existing=True,
    )
    completed_run = await db_session.get(
        PRReviewerRun,
        final_run_id,
        populate_existing=True,
    )
    finding_count = len(list((await db_session.execute(
        select(PRFinding).where(PRFinding.pr_review_id == review_id)
    )).scalars()))
    assert completed.status == "publishing"
    assert completed.pending_action == "review_comments"
    assert completed.publication_state == "publishing"
    assert completed.failure_stage is None
    assert completed.publication_error is None
    assert completed_run.status == "changes_required"
    assert finding_count == 1
    publish.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("auto_merge", "thread_status"),
    [
        (True, "published_inline"),
        (True, "pending"),
        (False, "pending"),
    ],
)
async def test_clean_panel_waits_for_old_finding_threads_before_any_publication(
    db_session,
    db_factory,
    auto_merge,
    thread_status,
):
    repo = MonitoredRepo(
        repo_full_name="owner/repo",
        webhook_secret="s" * 64,
        provider="claude",
        review_model="claude-sonnet-4-6",
        review_mode="panel",
        wait_for_ci=False,
        auto_merge=auto_merge,
        merge_queue_mode="manual",
        default_branch="main",
        allowed_authors=[],
    )
    db_session.add(repo)
    await db_session.commit()
    review = await pr_review_service.create_pr_review_task(
        db_session,
        repo,
        PR_DATA,
        prepared_context=_context(),
    )
    monitor_run = await db_session.get(PRMonitorRun, review.monitor_run_id)
    old_review = PRReview(
        monitor_run_id=monitor_run.id,
        repo_id=repo.id,
        pr_number=review.pr_number,
        base_ref="main",
        base_sha=BASE_SHA,
        head_sha="c" * 40,
        pr_title="older blocked head",
        pr_author="alice",
        pr_url=review.pr_url,
        status="commented",
        action_taken="review_comments",
    )
    db_session.add(old_review)
    await db_session.flush()
    old_reviewer = PRReviewerRun(
        pr_review_id=old_review.id,
        role="qa_engineer",
        provider="claude",
        status="changes_required",
        prompt_policy_hash="4" * 64,
        guide_pack_hash="5" * 64,
    )
    db_session.add(old_reviewer)
    await db_session.flush()
    db_session.add(PRFinding(
        pr_review_id=old_review.id,
        reviewer_run_id=old_reviewer.id,
        fingerprint="6" * 64,
        role="qa_engineer",
        severity="high",
        category="correctness",
        path="backend/old.py",
        line=9,
        title="old blocking thread",
        evidence="The old head had a race.",
        impact="The race could lose work.",
        required_fix="Serialize the state transition.",
        test="Exercise the old interleaving.",
        base_sha=BASE_SHA,
        head_sha="c" * 40,
        thread_nonce="7" * 48,
        thread_status=thread_status,
        github_comment_id=(991 if thread_status == "published_inline" else None),
    ))
    await db_session.commit()
    runs = list((await db_session.execute(
        select(PRReviewerRun)
        .where(PRReviewerRun.pr_review_id == review.id)
        .order_by(PRReviewerRun.id)
    )).scalars())

    with (
        patch(
            "backend.services.pr_review_service._gh_authenticated_login",
            AsyncMock(return_value="ccm-reviewer"),
        ),
        patch(
            "backend.services.pr_review_service._freeze_safe_merge_method",
            AsyncMock(return_value="merge"),
        ),
        patch(
            "backend.services.pr_review_service._resume_publishing_review",
            AsyncMock(),
        ) as publish,
    ):
        for reviewer_run in runs:
            task = await db_session.get(
                Task,
                reviewer_run.task_id,
                populate_existing=True,
            )
            now = datetime.utcnow()
            task.status = "completed"
            task.started_at = now
            task.completed_at = now
            db_session.add(LogEntry(
                task_id=task.id,
                task_retry_count=task.retry_count,
                event_type="result",
                role="assistant",
                content=_output(reviewer_run.role),
                timestamp=now,
            ))
            await db_session.commit()
            await pr_review_panel.check_and_update_reviewer_run(
                db_session,
                reviewer_run_id=reviewer_run.id,
                task_id=task.id,
                retry_count=task.retry_count,
                db_factory=db_factory,
            )

    current = await db_session.get(PRReview, review.id, populate_existing=True)
    lifecycle = await db_session.get(
        PRMonitorRun,
        monitor_run.id,
        populate_existing=True,
    )
    assert current.status == "publishing"
    assert current.pending_action == (
        "waiting_threads:approved_merged"
        if auto_merge
        else "waiting_threads:lgtm_comment"
    )
    assert lifecycle.status == "resolving_fixed_threads"
    publish.assert_not_awaited()


def test_finding_fingerprint_distinguishes_location_root_cause_and_path_case():
    finding = {
        "severity": "medium",
        "category": "correctness",
        "path": "backend/Case.py",
        "line": 10,
        "hunk": None,
        "title": "Incorrect transition",
        "evidence": "State A is committed after wake B.",
        "impact": "The worker can become stranded.",
        "required_fix": "Commit A before wake B.",
        "test": "Crash at the transition boundary.",
    }
    base = pr_review_panel._finding_fingerprint("senior_engineer", finding)
    assert base != pr_review_panel._finding_fingerprint(
        "senior_engineer",
        {**finding, "line": 20},
    )
    assert base != pr_review_panel._finding_fingerprint(
        "senior_engineer",
        {**finding, "evidence": "State C overwrites state D."},
    )
    assert base != pr_review_panel._finding_fingerprint(
        "senior_engineer",
        {**finding, "path": "backend/case.py"},
    )


@pytest.mark.asyncio
async def test_cancelled_local_reviewer_runtime_uses_exact_termination_without_github(
    db_session,
    db_factory,
):
    review, run, task = await _create_cancelled_reviewer_runtime(
        db_session,
        worker_id=None,
    )
    from backend.services.worker_proxy import get_task_operation_lock

    async def terminate(task_id, db, **kwargs):
        assert get_task_operation_lock(task_id).locked()
        assert not pr_review_service.pr_review_action_lock(review.id).locked()
        assert not db.in_transaction()
        stored_review = await db.get(PRReview, review.id)
        stored_run = await db.get(PRReviewerRun, run.id)
        assert stored_review is not None and stored_review.status == "error"
        assert stored_run is not None and stored_run.status == "cancelled"
        generation = kwargs["expected_local_generation"]
        assert generation is not None
        assert generation.status == "executing"
        assert generation.retry_count == task.retry_count
        assert kwargs["operation_locks_held"] is True
        assert kwargs["allow_delivery_effect_stop"] is True
        return object()

    terminate_mock = AsyncMock(side_effect=terminate)
    github_effects = [AsyncMock() for _ in range(4)]
    with (
        patch(
            "backend.services.task_termination.terminate_authoritative_task_generation",
            terminate_mock,
        ),
        patch.object(
            pr_review_service,
            "_gh_authenticated_login",
            github_effects[0],
        ),
        patch.object(pr_review_service, "_publish_review_action", github_effects[1]),
        patch.object(pr_review_service, "_gh_api_json", github_effects[2]),
        patch.object(pr_review_service, "_gh_pr_view", github_effects[3]),
    ):
        assert await pr_review_panel.reconcile_cancelled_reviewer_tasks(
            db_factory,
            review_id=review.id,
        ) == 1

    terminate_mock.assert_awaited_once()
    assert terminate_mock.await_args.args[0] == task.id
    assert "another required PR reviewer failed" in (
        terminate_mock.await_args.kwargs["reason"]
    )
    for effect in github_effects:
        effect.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancelled_worker_reviewer_conflict_remains_retryable(
    db_session,
    db_factory,
):
    review, _run, task = await _create_cancelled_reviewer_runtime(
        db_session,
        worker_id=77,
    )
    from backend.services.task_termination import TaskTerminationConflict
    from backend.services.worker_proxy import get_task_operation_lock

    attempts = 0

    async def terminate(task_id, _db, **kwargs):
        nonlocal attempts
        attempts += 1
        assert task_id == task.id
        assert get_task_operation_lock(task_id).locked()
        assert kwargs["expected_local_generation"] is None
        assert kwargs["operation_locks_held"] is True
        assert kwargs["allow_delivery_effect_stop"] is True
        if attempts == 1:
            raise TaskTerminationConflict("Worker receipt is still settling")
        return object()

    with patch(
        "backend.services.task_termination.terminate_authoritative_task_generation",
        AsyncMock(side_effect=terminate),
    ):
        assert await pr_review_panel.reconcile_cancelled_reviewer_tasks(
            db_factory,
            review_id=review.id,
        ) == 0
        assert await pr_review_panel.reconcile_cancelled_reviewer_tasks(
            db_factory,
            review_id=review.id,
        ) == 1

    assert attempts == 2


@pytest.mark.asyncio
async def test_cancelled_reviewer_without_runtime_is_not_terminated(
    db_session,
    db_factory,
):
    review, _run, task = await _create_cancelled_reviewer_runtime(
        db_session,
        worker_id=None,
    )
    task.status = "pending"
    task.started_at = None
    await db_session.commit()
    terminate = AsyncMock()

    with patch(
        "backend.services.task_termination.terminate_authoritative_task_generation",
        terminate,
    ):
        assert await pr_review_panel.reconcile_cancelled_reviewer_tasks(
            db_factory,
            review_id=review.id,
        ) == 0

    terminate.assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_panel_recovery_defers_only_until_history_arrives(
    db_session,
    db_factory,
):
    review, run, task = await _create_recoverable_panel_run(
        db_session,
        worker_id=77,
    )
    with patch(
        "backend.services.pr_review_service._broadcast_review_update",
        AsyncMock(),
    ):
        assert await pr_review_panel.recover_panel_reviews(db_factory) == 0

        db_session.add(LogEntry(
            task_id=task.id,
            task_retry_count=task.retry_count,
            event_type="result",
            role="assistant",
            content=_output(run.role),
            timestamp=task.started_at,
        ))
        await db_session.commit()
        assert await pr_review_panel.recover_panel_reviews(db_factory) == 1

    refreshed_review = await db_session.get(
        PRReview,
        review.id,
        populate_existing=True,
    )
    refreshed_run = await db_session.get(
        PRReviewerRun,
        run.id,
        populate_existing=True,
    )
    assert refreshed_review.status == "reviewing"
    assert refreshed_run.status == "passed"


@pytest.mark.asyncio
async def test_worker_panel_recovery_quarantines_expired_missing_history(
    db_session,
    db_factory,
):
    review, run, task = await _create_recoverable_panel_run(
        db_session,
        worker_id=88,
    )
    monitor = PRMonitorRun(
        repo_id=review.repo_id,
        pr_number=review.pr_number,
        current_base_sha=review.base_sha,
        current_head_sha=review.head_sha,
        current_review_id=review.id,
        status="reviewing",
    )
    db_session.add(monitor)
    await db_session.flush()
    review.monitor_run_id = monitor.id
    task.started_at = datetime.utcnow() - timedelta(hours=2)
    task.completed_at = datetime.utcnow() - timedelta(hours=2)
    await db_session.commit()

    with patch(
        "backend.services.pr_review_service._broadcast_review_update",
        AsyncMock(),
    ):
        assert await pr_review_panel.recover_panel_reviews(db_factory) == 1

    refreshed_review = await db_session.get(
        PRReview,
        review.id,
        populate_existing=True,
    )
    refreshed_run = await db_session.get(
        PRReviewerRun,
        run.id,
        populate_existing=True,
    )
    assert refreshed_review.status == "error"
    assert refreshed_run.status == "error"
    assert "no terminal output candidates" in (refreshed_run.error_message or "")
    assert await pr_review_panel.recover_panel_reviews(db_factory) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("worker_id", "candidate", "expected_error"),
    [
        (88, "malformed terminal candidate", "no valid strict terminal"),
        (None, None, "no terminal output candidates"),
    ],
)
async def test_panel_recovery_fails_closed_for_malformed_or_local_missing_output(
    db_session,
    db_factory,
    worker_id,
    candidate,
    expected_error,
):
    review, run, task = await _create_recoverable_panel_run(
        db_session,
        worker_id=worker_id,
    )
    monitor = PRMonitorRun(
        repo_id=review.repo_id,
        pr_number=review.pr_number,
        current_base_sha=review.base_sha,
        current_head_sha=review.head_sha,
        current_review_id=review.id,
        status="reviewing",
    )
    db_session.add(monitor)
    await db_session.flush()
    review.monitor_run_id = monitor.id
    if candidate is not None:
        db_session.add(LogEntry(
            task_id=task.id,
            task_retry_count=task.retry_count,
            event_type="result",
            role="assistant",
            content=candidate,
            timestamp=task.started_at,
        ))
    await db_session.commit()
    with patch(
        "backend.services.pr_review_service._broadcast_review_update",
        AsyncMock(),
    ):
        assert await pr_review_panel.recover_panel_reviews(db_factory) == 1

    refreshed_review = await db_session.get(
        PRReview,
        review.id,
        populate_existing=True,
    )
    refreshed_run = await db_session.get(
        PRReviewerRun,
        run.id,
        populate_existing=True,
    )
    sibling_run = await db_session.scalar(
        select(PRReviewerRun).where(
            PRReviewerRun.pr_review_id == review.id,
            PRReviewerRun.id != run.id,
        )
    )
    refreshed_monitor = await db_session.get(
        PRMonitorRun,
        monitor.id,
        populate_existing=True,
    )
    assert refreshed_review.status == "error"
    assert refreshed_run.status == "error"
    assert sibling_run is not None
    assert sibling_run.status == "cancelled"
    assert sibling_run.completed_at is not None
    assert expected_error in refreshed_run.error_message
    assert refreshed_monitor.status == "paused"
    assert refreshed_monitor.pause_reason.startswith(
        f"review_error:{review.id}:"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        "review model returned a terminal provider error",
        "review transport disconnected after retries",
    ],
)
async def test_reviewer_task_failure_pauses_exact_monitor_once(
    db_session,
    failure,
):
    review, reviewer_run, task = await _create_recoverable_panel_run(
        db_session,
        worker_id=None,
    )
    monitor = PRMonitorRun(
        repo_id=review.repo_id,
        pr_number=review.pr_number,
        current_base_sha=review.base_sha,
        current_head_sha=review.head_sha,
        current_review_id=review.id,
        status="reviewing",
    )
    db_session.add(monitor)
    await db_session.flush()
    review.monitor_run_id = monitor.id
    review_id = review.id
    reviewer_run_id = reviewer_run.id
    task_id = task.id
    monitor_id = monitor.id
    await db_session.commit()

    assert await pr_review_panel.fail_reviewer_run(
        db_session,
        reviewer_run_id=reviewer_run_id,
        task_id=task_id,
        expected_status=task.status,
        retry_count=task.retry_count,
        expected_started_at=task.started_at,
        expected_completed_at=task.completed_at,
        error=failure,
    ) == review_id
    sibling_run = await db_session.scalar(
        select(PRReviewerRun).where(
            PRReviewerRun.pr_review_id == review_id,
            PRReviewerRun.id != reviewer_run_id,
        )
    )
    assert sibling_run is not None
    assert sibling_run.status == "cancelled"
    assert sibling_run.completed_at is not None
    assert "reviewer failed" in (sibling_run.error_message or "")
    refreshed = await db_session.get(
        PRMonitorRun,
        monitor_id,
        populate_existing=True,
    )
    assert refreshed.status == "paused"
    assert failure[:500] in (refreshed.pause_reason or "")
    terminal_version = refreshed.state_version

    assert await pr_review_panel.fail_reviewer_run(
        db_session,
        reviewer_run_id=reviewer_run_id,
        task_id=task_id,
        expected_status=task.status,
        retry_count=task.retry_count,
        expected_started_at=task.started_at,
        expected_completed_at=task.completed_at,
        error=failure,
    ) is None
    refreshed = await db_session.get(
        PRMonitorRun,
        monitor_id,
        populate_existing=True,
    )
    assert refreshed.state_version == terminal_version


@pytest.mark.asyncio
async def test_reviewer_completion_revalidates_stale_second_session(
    db_session,
    db_factory,
):
    review, run, task = await _create_recoverable_panel_run(
        db_session,
        worker_id=None,
    )
    db_session.add(LogEntry(
        task_id=task.id,
        task_retry_count=task.retry_count,
        event_type="result",
        role="assistant",
        content=_output(run.role),
        timestamp=task.started_at,
    ))
    await db_session.commit()

    async with db_factory() as stale_db:
        stale_run = await stale_db.get(PRReviewerRun, run.id)
        assert stale_run.status == "pending"
        async with db_factory() as first_db:
            with patch(
                "backend.services.pr_review_service._broadcast_review_update",
                AsyncMock(),
            ):
                assert await pr_review_panel.check_and_update_reviewer_run(
                    first_db,
                    reviewer_run_id=run.id,
                    task_id=task.id,
                    retry_count=task.retry_count,
                    db_factory=db_factory,
                ) is True
                assert await pr_review_panel.check_and_update_reviewer_run(
                    stale_db,
                    reviewer_run_id=run.id,
                    task_id=task.id,
                    retry_count=task.retry_count,
                    db_factory=db_factory,
                ) is False

    findings = list((await db_session.execute(
        select(PRFinding).where(PRFinding.reviewer_run_id == run.id)
    )).scalars())
    refreshed_review = await db_session.get(
        PRReview,
        review.id,
        populate_existing=True,
    )
    assert refreshed_review.status == "reviewing"
    assert findings == []


@pytest.mark.asyncio
@pytest.mark.parametrize("task_status", ["completed", "failed"])
async def test_panel_terminal_consumers_yield_to_active_termination_receipt(
    db_session,
    db_factory,
    task_status,
):
    review, run, task = await _create_recoverable_panel_run(
        db_session,
        worker_id=None,
    )
    task.status = task_status
    if task_status == "completed":
        db_session.add(LogEntry(
            task_id=task.id,
            task_retry_count=task.retry_count,
            event_type="result",
            role="assistant",
            content=_output(run.role),
            timestamp=task.started_at,
        ))
    await db_session.commit()
    review_id = review.id
    run_id = run.id
    task_id = task.id
    retry_count = task.retry_count
    await persist_active_worker_receipt(db_factory, task_id)

    if task_status == "completed":
        changed = await pr_review_panel.check_and_update_reviewer_run(
            db_session,
            reviewer_run_id=run_id,
            task_id=task_id,
            retry_count=retry_count,
            db_factory=db_factory,
        )
    else:
        changed = await pr_review_panel.fail_reviewer_run(
            db_session,
            reviewer_run_id=run_id,
            task_id=task_id,
            expected_status=task.status,
            retry_count=task.retry_count,
            expected_started_at=task.started_at,
            expected_completed_at=task.completed_at,
            error="receipt owns failure arbitration",
        )

    assert not changed
    current_review = await db_session.get(
        PRReview,
        review_id,
        populate_existing=True,
    )
    current_run = await db_session.get(
        PRReviewerRun,
        run_id,
        populate_existing=True,
    )
    assert current_review.status == "reviewing"
    assert current_run.status == "pending"


@pytest.mark.asyncio
async def test_panel_completion_final_cas_yields_to_receipt_race(
    tmp_path,
):
    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    from backend.database import Base

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'panel-receipt-race.db'}",
        connect_args={"timeout": 1},
    )
    try:
        async with engine.begin() as connection:
            journal_mode = await connection.exec_driver_sql("PRAGMA journal_mode=WAL")
            assert journal_mode.scalar_one().lower() == "wal"
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        async with sessions() as consumer:
            review, run, task = await _create_recoverable_panel_run(
                consumer,
                worker_id=None,
            )
            consumer.add(LogEntry(
                task_id=task.id,
                task_retry_count=task.retry_count,
                event_type="result",
                role="assistant",
                content=_output(run.role),
                timestamp=task.started_at,
            ))
            await consumer.commit()
            review_id = review.id
            run_id = run.id
            task_id = task.id
            retry_count = task.retry_count
            original_guard = pr_review_panel._guard_exact_terminal_task

            async def receipt_wins_before_final_cas(
                db,
                guarded_task,
                *,
                statuses,
                expected_background_generation=None,
            ):
                assert guarded_task.id == task_id
                assert expected_background_generation is None
                await persist_active_worker_receipt(sessions, task_id)
                return await original_guard(
                    db,
                    guarded_task,
                    statuses=statuses,
                    expected_background_generation=(
                        expected_background_generation
                    ),
                )

            with patch.object(
                pr_review_panel,
                "_guard_exact_terminal_task",
                side_effect=receipt_wins_before_final_cas,
            ):
                assert await pr_review_panel.check_and_update_reviewer_run(
                    consumer,
                    reviewer_run_id=run_id,
                    task_id=task_id,
                    retry_count=retry_count,
                    db_factory=sessions,
                ) is False

        async with sessions() as verifier:
            current_review = await verifier.get(PRReview, review_id)
            current_run = await verifier.get(PRReviewerRun, run_id)
            assert current_review.status == "reviewing"
            assert current_run.status == "pending"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_panel_startup_recovery_holds_shared_task_operation_lock(
    db_session,
    db_factory,
):
    _, run, task = await _create_recoverable_panel_run(
        db_session,
        worker_id=None,
    )
    db_session.add(LogEntry(
        task_id=task.id,
        task_retry_count=task.retry_count,
        event_type="result",
        role="assistant",
        content=_output(run.role),
        timestamp=task.started_at,
    ))
    await db_session.commit()
    task_id = task.id
    from backend.services.worker_proxy import get_task_operation_lock

    original_read = pr_review_panel._read_panel_terminal

    async def assert_locked(*args, **kwargs):
        assert get_task_operation_lock(task_id).locked()
        return await original_read(*args, **kwargs)

    with patch.object(
        pr_review_panel,
        "_read_panel_terminal",
        side_effect=assert_locked,
    ):
        assert await pr_review_panel.recover_panel_reviews(db_factory) == 1


@pytest.mark.asyncio
async def test_panel_failure_recovery_rejects_new_retry_generation(
    db_session,
    db_factory,
):
    review, reviewer_run, task = await _create_recoverable_panel_run(
        db_session,
        worker_id=None,
    )
    task.status = "failed"
    await db_session.commit()
    review_id = review.id
    reviewer_run_id = reviewer_run.id
    task_id = task.id

    @asynccontextmanager
    async def retry_wins_after_recovery_scan():
        async with db_factory() as concurrent:
            current = await concurrent.get(Task, task_id)
            current.retry_count += 1
            current.status = "completed"
            current.started_at = datetime.utcnow()
            current.completed_at = datetime.utcnow()
            await concurrent.commit()
        yield

    with patch(
        "backend.services.worker_proxy.get_task_operation_lock",
        side_effect=lambda _task_id: retry_wins_after_recovery_scan(),
    ):
        assert await pr_review_panel.recover_panel_reviews(db_factory) == 0

    current_review = await db_session.get(
        PRReview,
        review_id,
        populate_existing=True,
    )
    current_run = await db_session.get(
        PRReviewerRun,
        reviewer_run_id,
        populate_existing=True,
    )
    current_task = await db_session.get(Task, task_id, populate_existing=True)
    assert current_review.status == "reviewing"
    assert current_run.status == "pending"
    assert current_task.status == "completed"
    assert current_task.retry_count == 1


@pytest.mark.asyncio
async def test_fetch_exact_head_ci_combines_checks_and_statuses():
    responses = [
        {"total_count": 1, "check_runs": [{"id": 12, "name": "tests", "status": "completed", "conclusion": "success", "app": {"id": 15368, "slug": "github-actions"}, "output": {"title": "Tests", "summary": "All passed"}}]},
        {"state": "success", "statuses": [{"id": 13, "context": "lint", "state": "success", "creator": {"login": "ci-bot"}}]},
    ]
    with patch(
        "backend.services.pr_review_service._gh_api_json",
        AsyncMock(side_effect=responses),
    ):
        status, summary, details = await pr_review_panel.fetch_exact_head_ci(
            "owner/repo",
            HEAD_SHA,
            [
                {"kind": "check_run", "name": "tests", "app_slug": "github-actions"},
                {"kind": "status", "name": "lint", "app_slug": "ci-bot"},
            ],
        )
    assert status == "passed"
    assert summary == "2 required exact-head CI checks passed"
    assert [item["state"] for item in details["observed"]] == ["passed", "passed"]
    assert details["observed"][0]["app_id"] == 15368
    assert details["observed"][0]["output"]["summary"] == "All passed"


@pytest.mark.asyncio
async def test_fetch_exact_head_ci_trusted_mode_uses_only_triggered_checks():
    from backend.services.delivery_setup import TRUSTED_OBSERVED_CI_POLICY

    responses = [
        {
            "total_count": 3,
            "check_runs": [
                {"id": 21, "name": "tests", "status": "completed", "conclusion": "success", "app": {"id": 15368, "slug": "github-actions"}},
                {"id": 22, "name": "conditional deploy", "status": "completed", "conclusion": "skipped", "app": {"id": 15368, "slug": "github-actions"}},
                {"id": 23, "name": "lint", "status": "completed", "conclusion": "success", "app": {"id": 15368, "slug": "github-actions"}},
            ],
        },
        {"state": "success", "statuses": []},
    ]
    with patch(
        "backend.services.pr_review_service._gh_api_json",
        AsyncMock(side_effect=responses),
    ):
        status, summary, details = await pr_review_panel.fetch_exact_head_ci(
            "owner/repo",
            HEAD_SHA,
            [TRUSTED_OBSERVED_CI_POLICY],
        )

    assert status == "passed"
    assert summary == "2 triggered exact-head CI checks passed; 1 skipped"
    assert details["required"] == [
        {"kind": "check_run", "name": "conditional deploy", "app_slug": "github-actions"},
        {"kind": "check_run", "name": "lint", "app_slug": "github-actions"},
        {"kind": "check_run", "name": "tests", "app_slug": "github-actions"},
    ]
    assert [item["state"] for item in details["observed"]] == [
        "skipped",
        "passed",
        "passed",
    ]


@pytest.mark.asyncio
async def test_fetch_exact_head_ci_trusted_mode_waits_for_checks_to_appear():
    from backend.services.delivery_setup import TRUSTED_OBSERVED_CI_POLICY

    with patch(
        "backend.services.pr_review_service._gh_api_json",
        AsyncMock(side_effect=[
            {"total_count": 0, "check_runs": []},
            {"state": "pending", "statuses": []},
        ]),
    ):
        status, summary, details = await pr_review_panel.fetch_exact_head_ci(
            "owner/repo",
            HEAD_SHA,
            [TRUSTED_OBSERVED_CI_POLICY],
        )

    assert status == "pending"
    assert summary == "Waiting for CI checks to appear on the exact PR head"
    assert details == {"head_sha": HEAD_SHA, "required": [], "observed": []}


@pytest.mark.asyncio
async def test_exact_base_guide_manifest_adds_only_declared_regular_files():
    from backend.services import pr_review_service

    tree_sha = "c" * 40
    ccm_sha = "d" * 40
    manifest_sha = "e" * 40
    guide_sha = "f" * 40
    manifest_raw = json.dumps({
        "version": 1,
        "documents": [{
            "path": "docs/architecture/invariants.md",
            "roles": ["principal_engineer", "senior_engineer"],
        }],
    }).encode()
    guide_raw = b"State commits before wake-up."
    responses = [
        {"sha": BASE_SHA, "tree": {"sha": tree_sha}},
        {"sha": tree_sha, "truncated": False, "tree": [
            {"path": ".ccm", "type": "tree", "mode": "040000", "sha": ccm_sha},
        ]},
        {"sha": ccm_sha, "truncated": False, "tree": [
            {"path": "review-guides.json", "type": "blob", "mode": "100644", "sha": manifest_sha, "size": len(manifest_raw)},
        ]},
        {"sha": manifest_sha, "size": len(manifest_raw), "encoding": "base64", "content": base64.b64encode(manifest_raw).decode()},
        {"sha": tree_sha, "truncated": False, "tree": [
            {"path": "docs/architecture/invariants.md", "type": "blob", "mode": "100644", "sha": guide_sha, "size": len(guide_raw)},
        ]},
        {"sha": guide_sha, "size": len(guide_raw), "encoding": "base64", "content": base64.b64encode(guide_raw).decode()},
    ]
    with patch.object(
        pr_review_service,
        "_gh_api_json",
        AsyncMock(side_effect=responses),
    ):
        guides = await pr_review_service._fetch_base_guidance("owner/repo", BASE_SHA)
    assert guides == {
        "docs/architecture/invariants.md": guide_raw.decode(),
        "__ccm_review_guide_roles__": {
            "docs/architecture/invariants.md": [
                "principal_engineer",
                "senior_engineer",
            ]
        },
    }


@pytest.mark.asyncio
async def test_waiting_ci_reconciler_starts_panel_only_after_pass(
    db_session,
    db_factory,
):
    repo = MonitoredRepo(
        repo_full_name="owner/repo",
        webhook_secret="s" * 64,
        provider="claude",
        review_model="claude-sonnet-4-6",
        review_mode="panel",
        wait_for_ci=True,
        auto_merge=True,
        enabled=True,
        default_branch="main",
        allowed_authors=[],
    )
    db_session.add(repo)
    await db_session.commit()
    review = await pr_review_panel.create_waiting_ci_review(
        db_session,
        repo,
        PR_DATA,
        ci_status="pending",
        ci_summary="Pending: tests",
        ci_details={"head_sha": HEAD_SHA, "required": [], "observed": []},
    )
    await attach_review_to_run(
        db_session,
        repo=repo,
        review=review,
        pr_data=PR_DATA,
    )
    with (
        patch.object(
            pr_review_panel,
            "fetch_exact_head_ci",
            AsyncMock(return_value=("passed", "1 required exact-head CI checks passed", {"head_sha": HEAD_SHA, "required": [], "observed": []})),
        ),
        patch(
            "backend.services.pr_review_service.verify_pr_review_snapshot_current",
            AsyncMock(),
        ),
        patch(
            "backend.services.pr_review_service.prepare_pr_review_context",
            AsyncMock(return_value=_context()),
        ),
    ):
        assert await pr_review_panel.reconcile_waiting_ci_reviews(db_factory) == 1

    refreshed = await db_session.get(PRReview, review.id, populate_existing=True)
    monitor = await db_session.get(
        PRMonitorRun,
        review.monitor_run_id,
        populate_existing=True,
    )
    runs = list((await db_session.execute(
        select(PRReviewerRun).where(PRReviewerRun.pr_review_id == review.id)
    )).scalars())
    assert refreshed.status == "reviewing"
    assert refreshed.ci_status == "passed"
    assert monitor.status == "reviewing"
    assert monitor.state_version == 2
    assert len(runs) == 3
    tasks = [await db_session.get(Task, run.task_id) for run in runs]
    assert all(task.metadata_["pr_auto_merge"] is True for task in tasks)


@pytest.mark.asyncio
async def test_waiting_ci_oversize_fails_closed_once_after_ci_passes(
    db_session,
    db_factory,
):
    repo = MonitoredRepo(
        repo_full_name="owner/repo",
        webhook_secret="s" * 64,
        provider="codex",
        review_mode="panel",
        wait_for_ci=True,
        enabled=True,
        default_branch="main",
        allowed_authors=[],
    )
    db_session.add(repo)
    await db_session.commit()
    review = await pr_review_panel.create_waiting_ci_review(
        db_session,
        repo,
        PR_DATA,
        ci_status="pending",
        ci_summary="Pending: tests",
        ci_details={"head_sha": HEAD_SHA, "required": [], "observed": []},
    )
    monitor = await attach_review_to_run(
        db_session,
        repo=repo,
        review=review,
        pr_data=PR_DATA,
    )
    admission_error = pr_review_service.PRReviewInputTooLarge(
        "unsupported_input_size: qa_engineer reviewer exceeds the safe limit",
        measured=120_001,
        limit=120_000,
        unit="characters",
    )

    with (
        patch.object(
            pr_review_panel,
            "fetch_exact_head_ci",
            AsyncMock(return_value=(
                "passed",
                "1 required exact-head CI checks passed",
                {"head_sha": HEAD_SHA, "required": [], "observed": []},
            )),
        ),
        patch(
            "backend.services.pr_review_service.verify_pr_review_snapshot_current",
            AsyncMock(),
        ),
        patch(
            "backend.services.pr_review_service.prepare_pr_review_context",
            AsyncMock(side_effect=admission_error),
        ) as prepare,
    ):
        assert await pr_review_panel.reconcile_waiting_ci_reviews(db_factory) == 0
        assert await pr_review_panel.reconcile_waiting_ci_reviews(db_factory) == 0

    refreshed = await db_session.get(PRReview, review.id, populate_existing=True)
    refreshed_monitor = await db_session.get(
        PRMonitorRun,
        monitor.id,
        populate_existing=True,
    )
    runs = list((await db_session.execute(
        select(PRReviewerRun).where(PRReviewerRun.pr_review_id == review.id)
    )).scalars())
    assert prepare.await_count == 1
    assert refreshed.status == "error"
    assert refreshed.action_taken == "error"
    assert refreshed.ci_status == "passed"
    assert refreshed.failure_stage == "reviewer"
    assert refreshed.publication_state == "not_applicable"
    assert refreshed.error_category == "unsupported_input_size"
    assert refreshed.error_measured == 120_001
    assert refreshed.error_limit == 120_000
    assert refreshed.error_unit == "characters"
    assert refreshed.review_summary == admission_error.public_detail
    assert refreshed.completed_at is not None
    assert refreshed_monitor.status == "paused"
    assert refreshed_monitor.pause_reason == "review_input_too_large"
    assert runs == []


@pytest.mark.asyncio
async def test_waiting_ci_reconciler_requires_exact_monitor_run_fence(
    db_session,
    db_factory,
):
    repo = MonitoredRepo(
        repo_full_name="owner/missing-run",
        webhook_secret="s" * 64,
        provider="claude",
        review_mode="panel",
        wait_for_ci=True,
        enabled=True,
        default_branch="main",
        allowed_authors=[],
    )
    db_session.add(repo)
    await db_session.commit()
    pr_data = {
        **PR_DATA,
        "url": "https://github.com/owner/missing-run/pull/17",
    }
    review = await pr_review_panel.create_waiting_ci_review(
        db_session,
        repo,
        pr_data,
        ci_status="pending",
        ci_summary="Pending: tests",
        ci_details={"head_sha": HEAD_SHA, "required": [], "observed": []},
    )
    await db_session.commit()

    with (
        patch.object(
            pr_review_panel,
            "fetch_exact_head_ci",
            AsyncMock(return_value=(
                "passed",
                "1 required exact-head CI checks passed",
                {"head_sha": HEAD_SHA, "required": [], "observed": []},
            )),
        ),
        patch(
            "backend.services.pr_review_service.verify_pr_review_snapshot_current",
            AsyncMock(),
        ),
        patch(
            "backend.services.pr_review_service.prepare_pr_review_context",
            AsyncMock(return_value={
                **_context(),
                "repo_name": "owner/missing-run",
            }),
        ),
    ):
        assert await pr_review_panel.reconcile_waiting_ci_reviews(db_factory) == 0

    refreshed = await db_session.get(PRReview, review.id, populate_existing=True)
    runs = list((await db_session.execute(
        select(PRReviewerRun).where(PRReviewerRun.pr_review_id == review.id)
    )).scalars())
    assert refreshed.status == "waiting_ci"
    assert runs == []


@pytest.mark.asyncio
@pytest.mark.parametrize("lifecycle_change", ["disable", "supersede"])
async def test_waiting_ci_reconciler_rechecks_lifecycle_after_context_fetch(
    db_session,
    db_factory,
    lifecycle_change,
):
    repo = MonitoredRepo(
        repo_full_name=f"owner/waiting-{lifecycle_change}",
        webhook_secret="s" * 64,
        provider="claude",
        review_model="claude-sonnet-4-6",
        review_mode="panel",
        wait_for_ci=True,
        enabled=True,
        default_branch="main",
        allowed_authors=[],
    )
    db_session.add(repo)
    await db_session.flush()
    review = await pr_review_panel.create_waiting_ci_review(
        db_session,
        repo,
        PR_DATA,
        ci_status="pending",
        ci_summary="Pending: tests",
        ci_details={"head_sha": HEAD_SHA, "required": [], "observed": []},
    )
    run = await attach_review_to_run(
        db_session,
        repo=repo,
        review=review,
        pr_data=PR_DATA,
    )
    ids = {"repo": repo.id, "review": review.id, "run": run.id}

    async def change_lifecycle(*_args, **_kwargs):
        async with db_factory() as concurrent:
            changed_repo = await concurrent.get(MonitoredRepo, ids["repo"])
            changed_review = await concurrent.get(PRReview, ids["review"])
            changed_run = await concurrent.get(PRMonitorRun, ids["run"])
            if lifecycle_change == "disable":
                changed_repo.enabled = False
            else:
                changed_review.status = "superseded"
                changed_run.status = "reviewing"
                changed_run.current_head_sha = "c" * 40
                changed_run.state_version += 1
            await concurrent.commit()
        return _context()

    with (
        patch.object(
            pr_review_panel,
            "fetch_exact_head_ci",
            AsyncMock(return_value=(
                "passed",
                "1 required exact-head CI checks passed",
                {"head_sha": HEAD_SHA, "required": [], "observed": []},
            )),
        ),
        patch(
            "backend.services.pr_review_service.verify_pr_review_snapshot_current",
            AsyncMock(),
        ),
        patch(
            "backend.services.pr_review_service.prepare_pr_review_context",
            change_lifecycle,
        ),
    ):
        assert await pr_review_panel.reconcile_waiting_ci_reviews(db_factory) == 0

    reviewer_runs = list((await db_session.execute(
        select(PRReviewerRun).where(PRReviewerRun.pr_review_id == ids["review"])
    )).scalars())
    refreshed_repo = await db_session.get(
        MonitoredRepo,
        ids["repo"],
        populate_existing=True,
    )
    refreshed_review = await db_session.get(
        PRReview,
        ids["review"],
        populate_existing=True,
    )
    assert reviewer_runs == []
    if lifecycle_change == "disable":
        assert refreshed_repo.enabled is False
        assert refreshed_review.status == "waiting_ci"
    else:
        assert refreshed_review.status == "superseded"
