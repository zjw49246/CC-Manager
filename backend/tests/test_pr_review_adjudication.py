import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from backend.models.log_entry import LogEntry
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRFindingRebuttal,
    PRMergeQueueAction,
    PRMonitorRun,
    PRReview,
    PRReviewerRun,
)
from backend.models.task import Task
from backend.models.worker import Worker
from backend.services.pr_review_adjudication import (
    _stop_resolution_lease_renewal,
    complete_adjudication,
    fail_adjudication,
    parse_adjudication_output,
    reconcile_fixed_finding_resolutions,
    reconcile_rebuttal_resolutions,
)
from backend.services.pr_monitor_loop import record_gate_pass
from backend.tests.worker_termination_helpers import (
    persist_active_manager_receipt,
)


BASE = "a" * 40
HEAD = "b" * 40


@pytest.mark.asyncio
async def test_resolution_renewal_finalizer_settles_under_anyio_cancellation():
    from anyio import CancelScope

    stop = asyncio.Event()
    finished = asyncio.Event()

    async def renewal():
        await stop.wait()
        await asyncio.sleep(0)
        finished.set()

    renewal_task = asyncio.create_task(renewal())
    with CancelScope() as scope:
        scope.cancel()
        with pytest.raises(asyncio.CancelledError):
            await _stop_resolution_lease_renewal(stop, renewal_task)

    assert finished.is_set()
    assert renewal_task.done()


def _output(fingerprint: str, verdict: str = "accepted") -> str:
    return (
        "PR_REBUTTAL_ADJUDICATION_BEGIN\n"
        f'{{"schema_version":1,"subject":{{"base_sha":"{BASE}","head_sha":"{HEAD}"}},'
        f'"finding_fingerprint":"{fingerprint}","verdict":"{verdict}",'
        '"reason":"The exact changed-file evidence proves the guarded path."}\n'
        "PR_REBUTTAL_ADJUDICATION_END\n"
        "PR_REVIEW_RESULT: rebuttal_adjudicated"
    )


async def _receipt_owned_adjudication_fixture(
    db_session,
    *,
    task_status: str,
):
    worker = Worker(
        name=f"{task_status} adjudication receipt worker",
        status="ready",
        private_ip="10.0.0.92",
        auth_token=f"{task_status}-receipt-token",
    )
    repo = MonitoredRepo(
        repo_full_name=f"owner/{task_status}-receipt-adjudication",
        webhook_secret="s" * 64,
        review_mode="panel",
    )
    developer = Task(
        title="Developer",
        description="change",
        status="completed",
    )
    db_session.add_all([worker, repo, developer])
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=93,
        current_base_sha=BASE,
        current_head_sha=HEAD,
        developer_task_id=developer.id,
        status="adjudicating",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=93,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="receipt adjudication",
        pr_author="alice",
        pr_url=(
            "https://github.com/owner/receipt-adjudication/pull/93"
        ),
        status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    reviewer = PRReviewerRun(
        pr_review_id=review.id,
        role="senior_engineer",
        provider="codex",
        status="changes_required",
        prompt_policy_hash="1" * 64,
        guide_pack_hash="2" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="f" * 64,
        role="senior_engineer",
        severity="high",
        category="correctness",
        path="app.py",
        line=3,
        title="missing receipt fence",
        evidence="guard missing",
        impact="unsafe",
        required_fix="add guard",
        test="exercise receipt race",
        base_sha=BASE,
        head_sha=HEAD,
        thread_nonce="3" * 48,
        thread_status="published_inline",
        github_comment_id=123,
    )
    db_session.add(finding)
    await db_session.flush()
    started_at = datetime.utcnow() - timedelta(seconds=1)
    adjudicator = Task(
        title="receipt-owned Adjudicator",
        description="judge",
        status=task_status,
        worker_id=worker.id,
        retry_count=0,
        started_at=started_at,
        completed_at=datetime.utcnow(),
        metadata_={"pr_review_id": review.id},
        tags=["pr-review"],
    )
    db_session.add(adjudicator)
    await db_session.flush()
    rebuttal = PRFindingRebuttal(
        finding_id=finding.id,
        pr_review_id=review.id,
        monitor_run_id=run.id,
        developer_task_id=developer.id,
        task_id=adjudicator.id,
        attempt=1,
        base_sha=BASE,
        head_sha=HEAD,
        evidence="Concrete exact code evidence.",
        evidence_hash="4" * 64,
        status="adjudicating",
        resolution_nonce="5" * 48,
    )
    db_session.add(rebuttal)
    if task_status == "completed":
        db_session.add(
            LogEntry(
                task_id=adjudicator.id,
                task_retry_count=0,
                event_type="result",
                role="assistant",
                content=_output(finding.fingerprint),
                timestamp=datetime.utcnow(),
                is_error=False,
            )
        )
    await db_session.commit()
    return {
        "task": adjudicator.id,
        "rebuttal": rebuttal.id,
        "finding": finding.id,
        "run": run.id,
    }


async def _accepted_resolution_fixture(
    db_session,
    *,
    repo_name: str,
    thread_status: str,
    github_comment_id: int,
):
    repo = MonitoredRepo(
        repo_full_name=repo_name,
        webhook_secret="s" * 64,
        review_mode="panel",
    )
    developer = Task(
        title="Developer",
        description="change",
        status="completed",
        session_id=f"session-{repo_name}",
        last_cwd="/fake/repo",
    )
    db_session.add_all([repo, developer])
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=17,
        current_base_sha=BASE,
        current_head_sha=HEAD,
        developer_task_id=developer.id,
        status="adjudicating",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=17,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="fixture",
        pr_author="bot",
        pr_url=f"https://example.invalid/{repo_name}/pull/17",
        status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    reviewer = PRReviewerRun(
        pr_review_id=review.id,
        role="senior_engineer",
        provider="codex",
        status="changes_required",
        prompt_policy_hash="1" * 64,
        guide_pack_hash="2" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="f" * 64,
        role="senior_engineer",
        severity="high",
        category="correctness",
        path="app.py",
        line=None if thread_status == "published_fallback" else 3,
        title="bad guard",
        evidence="guard missing",
        impact="unsafe",
        required_fix="add guard",
        test="exercise invalid input",
        base_sha=BASE,
        head_sha=HEAD,
        thread_nonce="3" * 48,
        status="resolved_rebutted",
        thread_status=thread_status,
        github_comment_id=github_comment_id,
    )
    db_session.add(finding)
    await db_session.flush()
    rebuttal = PRFindingRebuttal(
        finding_id=finding.id,
        pr_review_id=review.id,
        monitor_run_id=run.id,
        developer_task_id=developer.id,
        attempt=1,
        base_sha=BASE,
        head_sha=HEAD,
        evidence="Concrete exact code evidence.",
        evidence_hash="4" * 64,
        status="accepted",
        verdict="accepted",
        result_body="The exact evidence disproves the Finding.",
        resolution_nonce="5" * 48,
        resolution_actor="ccm-bot",
    )
    db_session.add(rebuttal)
    await db_session.commit()
    return repo, run, review, finding, rebuttal


async def _fixed_resolution_fixture(
    db_session,
    *,
    repo_name: str,
    thread_status: str = "published_fallback",
    fixed_resolution_actor: str | None = None,
):
    new_head = "c" * 40
    repo = MonitoredRepo(
        repo_full_name=repo_name,
        webhook_secret="s" * 64,
        review_mode="panel",
        merge_queue_mode="auto",
    )
    db_session.add(repo)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=29,
        current_base_sha=BASE,
        current_head_sha=new_head,
        status="resolving_fixed_threads",
    )
    db_session.add(run)
    await db_session.flush()
    started_at = datetime.utcnow() - timedelta(seconds=2)
    publication_task = Task(
        title="clean current-head reviewer",
        description="immutable panel result",
        status="completed",
        retry_count=0,
        started_at=started_at,
        completed_at=started_at + timedelta(seconds=1),
        metadata_={
            "pr_auto_merge": False,
            "pr_wait_for_ci": False,
            "pr_required_checks": [],
            "pr_action_nonce": "a" * 48,
        },
    )
    db_session.add(publication_task)
    await db_session.flush()
    old_review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=29,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="old",
        pr_author="bot",
        pr_url=f"https://example.invalid/{repo_name}/pull/29",
        status="commented",
    )
    current_review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=29,
        base_ref="main",
        base_sha=BASE,
        head_sha=new_head,
        pr_title="fixed",
        pr_author="bot",
        pr_url=f"https://example.invalid/{repo_name}/pull/29",
        status="publishing",
        task_id=publication_task.id,
        action_nonce="a" * 48,
        pending_action="waiting_threads:lgtm_comment",
        pending_review_body="Panel reviewers found no blocking issue.",
        publishing_actor="ccm-bot",
        publishing_retry_count=0,
        publishing_task_started_at=started_at,
        publishing_started_at=started_at,
    )
    db_session.add_all([old_review, current_review])
    await db_session.flush()
    run.current_review_id = current_review.id
    db_session.add(PRReviewerRun(
        pr_review_id=current_review.id,
        role="principal_engineer",
        task_id=publication_task.id,
        provider="codex",
        status="passed",
        prompt_policy_hash="4" * 64,
        guide_pack_hash="5" * 64,
    ))
    reviewer = PRReviewerRun(
        pr_review_id=old_review.id,
        role="qa_engineer",
        provider="codex",
        status="changes_required",
        prompt_policy_hash="8" * 64,
        guide_pack_hash="9" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=old_review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="6" * 64,
        role="qa_engineer",
        severity="high",
        category="correctness",
        path="app.py",
        line=None if thread_status == "published_fallback" else 3,
        title="old bug",
        evidence="bug existed",
        impact="unsafe",
        required_fix="fix it",
        test="regression",
        base_sha=BASE,
        head_sha=HEAD,
        thread_nonce="7" * 48,
        thread_status=thread_status,
        github_comment_id=990,
        fixed_resolution_actor=fixed_resolution_actor,
    )
    db_session.add(finding)
    await db_session.commit()
    return repo, run, old_review, current_review, finding


@pytest.mark.asyncio
async def test_completed_adjudication_yields_to_manager_termination_receipt(
    db_session,
    db_factory,
):
    ids = await _receipt_owned_adjudication_fixture(
        db_session,
        task_status="completed",
    )
    await persist_active_manager_receipt(db_factory, ids["task"])

    await complete_adjudication(
        db_session,
        adjudication_id=ids["rebuttal"],
        task_id=ids["task"],
        retry_count=0,
    )

    rebuttal = await db_session.get(
        PRFindingRebuttal,
        ids["rebuttal"],
        populate_existing=True,
    )
    finding = await db_session.get(
        PRFinding,
        ids["finding"],
        populate_existing=True,
    )
    run = await db_session.get(
        PRMonitorRun,
        ids["run"],
        populate_existing=True,
    )
    assert rebuttal.status == "adjudicating"
    assert finding.status == "open"
    assert run.status == "adjudicating"


@pytest.mark.asyncio
async def test_failed_adjudication_yields_to_manager_termination_receipt(
    db_session,
    db_factory,
):
    ids = await _receipt_owned_adjudication_fixture(
        db_session,
        task_status="failed",
    )
    await persist_active_manager_receipt(db_factory, ids["task"])

    await fail_adjudication(
        db_session,
        adjudication_id=ids["rebuttal"],
        task_id=ids["task"],
        error="must remain receipt-owned",
    )

    rebuttal = await db_session.get(
        PRFindingRebuttal,
        ids["rebuttal"],
        populate_existing=True,
    )
    run = await db_session.get(
        PRMonitorRun,
        ids["run"],
        populate_existing=True,
    )
    assert rebuttal.status == "adjudicating"
    assert rebuttal.error_message is None
    assert run.status == "adjudicating"


@pytest.mark.asyncio
async def test_accepted_rebuttal_resolves_exact_github_thread_and_gate(
    db_session, db_factory, monkeypatch
):
    repo = MonitoredRepo(
        repo_full_name="fake/repo", webhook_secret="s" * 64,
        review_mode="panel", auto_repair=True,
    )
    developer = Task(
        title="Developer", description="change", status="completed",
        session_id="dev-session", last_cwd="/fake/repo",
    )
    db_session.add_all([repo, developer])
    await db_session.commit()
    run = PRMonitorRun(
        repo_id=repo.id, pr_number=7, current_base_sha=BASE,
        current_head_sha=HEAD, developer_task_id=developer.id,
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=7,
        base_ref="main",
        base_sha=BASE, head_sha=HEAD, pr_title="fake", pr_author="bot",
        pr_url="https://example.invalid/fake/repo/pull/7", status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    run.status = "adjudicating"
    reviewer = PRReviewerRun(
        pr_review_id=review.id, role="senior_engineer", provider="codex",
        status="changes_required", prompt_policy_hash="1" * 64,
        guide_pack_hash="2" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=review.id, reviewer_run_id=reviewer.id,
        fingerprint="f" * 64, role="senior_engineer", severity="high",
        category="correctness", path="app.py", line=3, title="bad guard",
        evidence="guard missing", impact="unsafe", required_fix="add guard",
        test="exercise invalid input", base_sha=BASE, head_sha=HEAD,
        thread_nonce="3" * 48, thread_status="published_inline",
        github_comment_id=123,
    )
    db_session.add(finding)
    await db_session.flush()
    started = datetime.utcnow() - timedelta(seconds=1)
    adjudicator = Task(
        title="Adjudicator", description="judge", status="completed",
        retry_count=0, started_at=started,
        pty_background_generation="adjudication-background-generation",
        metadata_={"pr_review_id": review.id}, tags=["pr-review"],
    )
    db_session.add(adjudicator)
    await db_session.flush()
    rebuttal = PRFindingRebuttal(
        finding_id=finding.id, pr_review_id=review.id, monitor_run_id=run.id,
        developer_task_id=developer.id, task_id=adjudicator.id, attempt=1,
        base_sha=BASE, head_sha=HEAD, evidence="Concrete exact code evidence.",
        evidence_hash="4" * 64, status="adjudicating", resolution_nonce="5" * 48,
    )
    db_session.add(rebuttal)
    db_session.add(LogEntry(
        task_id=adjudicator.id, task_retry_count=0, event_type="result",
        role="assistant", content=_output(finding.fingerprint),
        timestamp=datetime.utcnow(), is_error=False,
    ))
    await db_session.commit()

    assert parse_adjudication_output(_output(finding.fingerprint), finding=finding)["verdict"] == "accepted"
    await complete_adjudication(
        db_session, adjudication_id=rebuttal.id,
        task_id=adjudicator.id, retry_count=0,
        expected_background_generation=(
            "adjudication-background-generation"
        ),
    )
    assert (await db_session.get(PRFinding, finding.id, populate_existing=True)).status == "resolved_rebutted"
    assert (
        await db_session.get(Task, adjudicator.id, populate_existing=True)
    ).pty_background_generation == "adjudication-background-generation"

    calls = []

    async def fake_gh(endpoint, *, payload=None, **_kwargs):
        calls.append((endpoint, payload))
        if "mutation" in payload["query"]:
            return {"data": {"resolveReviewThread": {"thread": {"id": "T1", "isResolved": True}}}}
        return {"data": {"repository": {"pullRequest": {"reviewThreads": {
            "nodes": [{"id": "T1", "isResolved": False, "comments": {"nodes": [{"databaseId": 123}]}}],
            "pageInfo": {"hasNextPage": False},
        }}}}}

    monkeypatch.setattr("backend.services.pr_review_service._gh_api_value", fake_gh)
    async def fake_login():
        return "ccm-bot"

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_authenticated_login", fake_login
    )
    assert await reconcile_rebuttal_resolutions(db_factory) == 1
    resolved = await db_session.get(PRFinding, finding.id, populate_existing=True)
    refreshed_run = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert resolved.thread_status == "resolved"
    assert resolved.github_thread_node_id == "T1"
    assert refreshed_run.status == "ready_to_merge"
    refreshed_rebuttal = await db_session.get(
        PRFindingRebuttal, rebuttal.id, populate_existing=True
    )
    assert refreshed_rebuttal.status == "resolved"
    assert resolved.resolution_lease_token is None
    assert resolved.resolution_lease_expires_at is None
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_same_head_rebuttal_rearms_auto_merge_with_current_generation(
    db_session,
    monkeypatch,
):
    from backend.services import pr_review_service

    nonce = "a" * 48
    repo = MonitoredRepo(
        repo_full_name="fake/rebuttal-auto-merge",
        webhook_secret="s" * 64,
        review_mode="panel",
        auto_merge=True,
        wait_for_ci=False,
        merge_queue_mode="manual",
    )
    started_at = datetime.utcnow() - timedelta(seconds=2)
    reviewer_task = Task(
        title="blocking panel generation",
        description="immutable panel result",
        status="completed",
        retry_count=4,
        started_at=started_at,
        completed_at=started_at + timedelta(seconds=1),
        metadata_={
            "pr_auto_merge": True,
            "pr_wait_for_ci": False,
            "pr_required_checks": [],
            "pr_action_nonce": nonce,
        },
    )
    db_session.add_all([repo, reviewer_task])
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=31,
        current_base_sha=BASE,
        current_head_sha=HEAD,
        status="adjudicating",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=31,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="accepted rebuttal",
        pr_author="alice",
        pr_url="https://example.invalid/fake/rebuttal-auto-merge/pull/31",
        task_id=reviewer_task.id,
        status="commented",
        action_taken="review_comments",
        action_nonce=nonce,
        completed_at=datetime.utcnow(),
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    reviewer = PRReviewerRun(
        pr_review_id=review.id,
        role="qa_engineer",
        task_id=reviewer_task.id,
        provider="codex",
        status="changes_required",
        prompt_policy_hash="b" * 64,
        guide_pack_hash="c" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    db_session.add(PRFinding(
        pr_review_id=review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="d" * 64,
        role="qa_engineer",
        severity="high",
        category="correctness",
        path="app.py",
        line=4,
        title="rebutted blocker",
        evidence="The original evidence was incomplete.",
        impact="Would have blocked merge.",
        required_fix="No code change is required.",
        test="The independent rebuttal proved the exact path.",
        status="resolved_rebutted",
        base_sha=BASE,
        head_sha=HEAD,
        thread_nonce="e" * 48,
        thread_status="resolved",
        github_comment_id=992,
        thread_resolved_at=datetime.utcnow(),
    ))
    review_id = review.id
    run_id = run.id
    repo_full_name = repo.repo_full_name
    await db_session.commit()

    await record_gate_pass(db_session, review_id)
    staged = await db_session.get(PRReview, review_id, populate_existing=True)
    staged_run = await db_session.get(PRMonitorRun, run_id, populate_existing=True)
    assert staged.status == "publishing"
    assert staged.pending_action == "needs_identity:approved_merged"
    assert staged.publishing_retry_count is None
    assert staged.publishing_started_at is None
    assert staged_run.status == "reviewing"

    async def login():
        return "ccm-bot"

    async def freeze_merge_method(_repo_name):
        return "merge"

    monkeypatch.setattr(pr_review_service, "_gh_authenticated_login", login)
    monkeypatch.setattr(
        pr_review_service,
        "_freeze_safe_merge_method",
        freeze_merge_method,
    )
    assert await pr_review_service._arm_identity_pending_publication(
        db_session,
        review_id=review_id,
        repo_full_name=repo_full_name,
    )
    armed = await db_session.get(PRReview, review_id, populate_existing=True)
    assert armed.pending_action == "approved_merged"
    assert armed.publishing_actor == "ccm-bot"
    assert armed.publishing_retry_count == 4
    assert armed.publishing_task_started_at == started_at
    assert armed.publishing_started_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("capability_case", "summary_match"),
    [
        ("malformed", "response is malformed"),
        ("mismatched", "repository identity mismatched"),
        ("archived", "repository is archived or disabled"),
        ("disabled", "repository is archived or disabled"),
        ("no_push", "publishing identity lacks push permission"),
        ("transport", None),
    ],
)
async def test_direct_ref_capability_rebuttal_classifies_terminal_policy_error(
    db_session,
    monkeypatch,
    capability_case,
    summary_match,
):
    """Only deterministic direct-ref policy errors pause the exact Run."""

    from backend.services import pr_review_service

    nonce = "7" * 48
    started_at = datetime.utcnow() - timedelta(seconds=2)
    repo_full_name = "fake/direct-ref-capability-rebuttal"
    repo = MonitoredRepo(
        repo_full_name=repo_full_name,
        webhook_secret="s" * 64,
        review_mode="panel",
        auto_merge=True,
        wait_for_ci=False,
        merge_queue_mode="manual",
    )
    reviewer_task = Task(
        title="direct-ref capability panel generation",
        description="immutable panel result",
        status="completed",
        retry_count=2,
        started_at=started_at,
        completed_at=started_at + timedelta(seconds=1),
        metadata_={
            "pr_auto_merge": True,
            "pr_wait_for_ci": False,
            "pr_required_checks": [],
            "pr_action_nonce": nonce,
        },
    )
    db_session.add_all([repo, reviewer_task])
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=32,
        current_base_sha=BASE,
        current_head_sha=HEAD,
        status="reviewing",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=32,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="accepted rebuttal with direct-ref capability policy",
        pr_author="alice",
        pr_url=(
            "https://example.invalid/fake/"
            "direct-ref-capability-rebuttal/pull/32"
        ),
        task_id=reviewer_task.id,
        status="publishing",
        action_nonce=nonce,
        pending_action="needs_identity:approved_merged",
        pending_review_body="All blocking Findings were resolved.",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    db_session.add(PRReviewerRun(
        pr_review_id=review.id,
        role="qa_engineer",
        task_id=reviewer_task.id,
        provider="codex",
        status="passed",
        prompt_policy_hash="8" * 64,
        guide_pack_hash="9" * 64,
    ))
    await db_session.commit()
    review_id = review.id
    run_id = run.id
    original_version = run.state_version

    capability_payload = {
        "full_name": repo_full_name,
        "archived": False,
        "disabled": False,
        "permissions": {"push": True},
        # Rebase-only is intentionally irrelevant to the direct-ref protocol.
        "allow_merge_commit": False,
        "allow_squash_merge": False,
        "allow_rebase_merge": True,
    }
    if capability_case == "malformed":
        capability_payload.pop("permissions")
    elif capability_case == "mismatched":
        capability_payload["full_name"] = "fake/other-repository"
    elif capability_case == "archived":
        capability_payload["archived"] = True
    elif capability_case == "disabled":
        capability_payload["disabled"] = True
    elif capability_case == "no_push":
        capability_payload["permissions"] = {"push": False}

    capability_read = (
        AsyncMock(side_effect=pr_review_service.GhError(
            "temporary GitHub API timeout"
        ))
        if capability_case == "transport"
        else AsyncMock(return_value=capability_payload)
    )

    login = False

    async def unexpected_login():
        nonlocal login
        login = True
        return "ccm-bot"

    monkeypatch.setattr(
        pr_review_service,
        "_gh_api_json",
        capability_read,
    )
    monkeypatch.setattr(
        pr_review_service,
        "_gh_authenticated_login",
        unexpected_login,
    )

    assert not await pr_review_service._arm_identity_pending_publication(
        db_session,
        review_id=review_id,
        repo_full_name=repo_full_name,
    )

    result = await db_session.get(PRReview, review_id, populate_existing=True)
    result_run = await db_session.get(
        PRMonitorRun,
        run_id,
        populate_existing=True,
    )
    capability_read.assert_awaited_once_with(
        f"repos/{repo_full_name}"
    )
    if summary_match is None:
        assert result.status == "publishing"
        assert result.action_taken is None
        assert result.pending_action == "needs_identity:approved_merged"
        assert result.completed_at is None
        assert result_run.status == "reviewing"
        assert result_run.pause_reason is None
        assert result_run.state_version == original_version
    else:
        assert result.status == "error"
        assert result.action_taken == "error"
        assert result.review_summary is None
        assert result.publication_state == "failed"
        assert result.failure_stage == "merge"
        assert summary_match in result.publication_error
        assert result_run.status == "paused"
        assert result_run.pause_reason.startswith(f"review_error:{review_id}:")
        assert result_run.state_version == original_version + 1
    assert login is False


@pytest.mark.asyncio
@pytest.mark.parametrize("task_status", ("failed", "cancelled", "conflict"))
async def test_rebuttal_auto_merge_restart_pauses_invalid_terminal_generation(
    db_session,
    db_factory,
    monkeypatch,
    task_status,
):
    from backend.services import pr_review_service

    nonce = "6" * 48
    started_at = datetime.utcnow() - timedelta(seconds=2)
    repo = MonitoredRepo(
        repo_full_name=f"fake/rebuttal-invalid-{task_status}",
        webhook_secret="s" * 64,
        review_mode="panel",
        auto_merge=True,
        wait_for_ci=False,
        merge_queue_mode="manual",
    )
    reviewer_task = Task(
        title=f"{task_status} terminal reviewer discussion",
        description="terminal discussion must not strand publication",
        status=task_status,
        retry_count=5,
        started_at=started_at,
        completed_at=started_at + timedelta(seconds=1),
        metadata_={
            "pr_auto_merge": True,
            "pr_wait_for_ci": False,
            "pr_required_checks": [],
            "pr_action_nonce": nonce,
        },
    )
    db_session.add_all([repo, reviewer_task])
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=33,
        current_base_sha=BASE,
        current_head_sha=HEAD,
        status="reviewing",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=33,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="accepted rebuttal with unusable terminal discussion",
        pr_author="alice",
        pr_url=(
            f"https://example.invalid/fake/rebuttal-invalid-{task_status}/pull/33"
        ),
        task_id=reviewer_task.id,
        status="publishing",
        action_nonce=nonce,
        pending_action="needs_identity:approved_merged",
        pending_review_body="All blocking Findings were resolved.",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    db_session.add(PRReviewerRun(
        pr_review_id=review.id,
        role="qa_engineer",
        task_id=reviewer_task.id,
        provider="claude",
        status="passed",
        prompt_policy_hash="4" * 64,
        guide_pack_hash="5" * 64,
    ))
    await db_session.commit()
    review_id = review.id
    run_id = run.id

    freeze = AsyncMock()
    login = AsyncMock()
    monkeypatch.setattr(pr_review_service, "_freeze_safe_merge_method", freeze)
    monkeypatch.setattr(pr_review_service, "_gh_authenticated_login", login)

    assert await pr_review_service.recover_publishing_pr_reviews(db_factory) == 1
    terminal = await db_session.get(PRReview, review_id, populate_existing=True)
    paused = await db_session.get(PRMonitorRun, run_id, populate_existing=True)
    assert terminal.status == "error"
    assert terminal.action_taken == "error"
    assert terminal.pending_action is None
    assert terminal.review_summary is None
    assert terminal.publication_state == "failed"
    assert terminal.failure_stage == "github_identity"
    assert f"status={task_status}" in (terminal.publication_error or "")
    assert paused.status == "paused"
    assert paused.pause_reason == (
        f"review_error:{review_id}:PR reviewer failed without a summary"
    )
    freeze.assert_not_awaited()
    login.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_adjudicator_terminal_cannot_resurrect_superseded_run(
    db_session, db_factory, monkeypatch
):
    repo = MonitoredRepo(
        repo_full_name="fake/adjudication-race",
        webhook_secret="s" * 64,
        review_mode="panel",
    )
    developer = Task(
        title="Developer", description="change", status="completed",
        session_id="developer-race", last_cwd="/fake/race",
    )
    db_session.add_all([repo, developer])
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id, pr_number=71, current_base_sha=BASE,
        current_head_sha=HEAD, developer_task_id=developer.id,
        status="adjudicating",
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=71,
        base_ref="main",
        base_sha=BASE, head_sha=HEAD, pr_title="race", pr_author="bot",
        pr_url="https://example.invalid/fake/adjudication-race/pull/71",
        status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    reviewer = PRReviewerRun(
        pr_review_id=review.id, role="senior_engineer", provider="codex",
        status="changes_required", prompt_policy_hash="1" * 64,
        guide_pack_hash="2" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=review.id, reviewer_run_id=reviewer.id,
        fingerprint="f" * 64, role="senior_engineer", severity="high",
        category="correctness", path="app.py", line=3, title="bad guard",
        evidence="guard missing", impact="unsafe", required_fix="add guard",
        test="exercise invalid input", base_sha=BASE, head_sha=HEAD,
        thread_nonce="3" * 48, thread_status="published_inline",
        github_comment_id=123,
    )
    db_session.add(finding)
    await db_session.flush()
    started = datetime.utcnow() - timedelta(seconds=1)
    adjudicator = Task(
        title="Adjudicator", description="judge", status="completed",
        retry_count=0, started_at=started, completed_at=datetime.utcnow(),
        metadata_={"pr_review_id": review.id}, tags=["pr-review"],
    )
    db_session.add(adjudicator)
    await db_session.flush()
    rebuttal = PRFindingRebuttal(
        finding_id=finding.id, pr_review_id=review.id,
        monitor_run_id=run.id, developer_task_id=developer.id,
        task_id=adjudicator.id, attempt=1, base_sha=BASE, head_sha=HEAD,
        evidence="Concrete exact code evidence.", evidence_hash="4" * 64,
        status="adjudicating", resolution_nonce="5" * 48,
    )
    db_session.add(rebuttal)
    db_session.add(LogEntry(
        task_id=adjudicator.id, task_retry_count=0, event_type="result",
        role="assistant", content=_output(finding.fingerprint),
        timestamp=datetime.utcnow(), is_error=False,
    ))
    await db_session.commit()
    ids = {
        "run": run.id, "review": review.id, "finding": finding.id,
        "rebuttal": rebuttal.id, "task": adjudicator.id,
    }

    async with db_factory() as stale_db:
        original_execute = stale_db.execute
        raced = False

        async def execute_with_synchronize(statement, *args, **kwargs):
            nonlocal raced
            result = await original_execute(statement, *args, **kwargs)
            if not raced and "log_entries.content" in str(statement):
                raced = True
                async with db_factory() as winner:
                    winner_rebuttal = await winner.get(
                        PRFindingRebuttal, ids["rebuttal"]
                    )
                    winner_review = await winner.get(PRReview, ids["review"])
                    winner_run = await winner.get(PRMonitorRun, ids["run"])
                    winner_rebuttal.status = "superseded"
                    winner_rebuttal.completed_at = datetime.utcnow()
                    winner_review.status = "superseded"
                    winner_run.status = "reviewing"
                    winner_run.current_head_sha = "c" * 40
                    winner_run.state_version += 1
                    await winner.commit()
            return result

        monkeypatch.setattr(stale_db, "execute", execute_with_synchronize)
        await complete_adjudication(
            stale_db,
            adjudication_id=ids["rebuttal"],
            task_id=ids["task"],
            retry_count=0,
        )

    assert raced is True
    stale_rebuttal = await db_session.get(
        PRFindingRebuttal, ids["rebuttal"], populate_existing=True
    )
    stale_finding = await db_session.get(
        PRFinding, ids["finding"], populate_existing=True
    )
    stale_run = await db_session.get(
        PRMonitorRun, ids["run"], populate_existing=True
    )
    assert stale_rebuttal.status == "superseded"
    assert stale_finding.status == "open"
    assert stale_run.status == "reviewing"
    assert stale_run.current_head_sha == "c" * 40


def test_adjudication_rejects_wrong_subject():
    finding = type("Finding", (), {
        "base_sha": BASE, "head_sha": HEAD, "fingerprint": "f" * 64,
    })()
    with pytest.raises(ValueError, match="fixed contract"):
        parse_adjudication_output(
            _output("f" * 64).replace(HEAD, "c" * 40), finding=finding
        )


@pytest.mark.asyncio
async def test_green_new_head_resolves_old_thread_before_merge_gate(
    db_session, db_factory, monkeypatch
):
    new_head = "c" * 40
    repo = MonitoredRepo(
        repo_full_name="fake/repo", webhook_secret="s" * 64,
        review_mode="panel", merge_queue_mode="manual",
    )
    db_session.add(repo)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id, pr_number=8, current_base_sha=BASE,
        current_head_sha=new_head, status="resolving_fixed_threads",
    )
    db_session.add(run)
    await db_session.flush()
    started_at = datetime.utcnow() - timedelta(seconds=2)
    publication_task = Task(
        title="green current-head reviewer",
        description="immutable panel result",
        status="completed",
        retry_count=0,
        started_at=started_at,
        completed_at=started_at + timedelta(seconds=1),
        metadata_={
            "pr_auto_merge": False,
            "pr_wait_for_ci": False,
            "pr_required_checks": [],
            "pr_action_nonce": "a" * 48,
        },
    )
    db_session.add(publication_task)
    await db_session.flush()
    old_review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=8,
        base_ref="main",
        base_sha=BASE, head_sha=HEAD, pr_title="old", pr_author="bot",
        pr_url="https://example.invalid/fake/repo/pull/8", status="commented",
    )
    current_review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=8,
        base_ref="main",
        base_sha=BASE, head_sha=new_head, pr_title="fixed", pr_author="bot",
        pr_url="https://example.invalid/fake/repo/pull/8",
        status="publishing", task_id=publication_task.id,
        action_nonce="a" * 48,
        pending_action="waiting_threads:lgtm_comment",
        pending_review_body="Panel reviewers found no blocking issue.",
        publishing_actor="ccm-bot", publishing_retry_count=0,
        publishing_task_started_at=started_at,
        publishing_started_at=started_at,
    )
    db_session.add_all([old_review, current_review])
    await db_session.flush()
    run.current_review_id = current_review.id
    db_session.add(PRReviewerRun(
        pr_review_id=current_review.id, role="principal_engineer",
        task_id=publication_task.id, provider="codex", status="passed",
        prompt_policy_hash="3" * 64, guide_pack_hash="4" * 64,
    ))
    reviewer = PRReviewerRun(
        pr_review_id=old_review.id, role="qa_engineer", provider="codex",
        status="changes_required", prompt_policy_hash="1" * 64,
        guide_pack_hash="2" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=old_review.id, reviewer_run_id=reviewer.id,
        fingerprint="9" * 64, role="qa_engineer", severity="high",
        category="correctness", path="app.py", line=3, title="bad guard",
        evidence="guard missing", impact="unsafe", required_fix="add guard",
        test="exercise invalid input", base_sha=BASE, head_sha=HEAD,
        thread_nonce="8" * 48, thread_status="published_inline",
        github_comment_id=456,
    )
    db_session.add(finding)
    await db_session.commit()

    waiting = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert waiting.status == "resolving_fixed_threads"
    assert current_review.pending_action == "waiting_threads:lgtm_comment"

    calls = []

    async def fake_gh(endpoint, *, payload=None, **_kwargs):
        calls.append((endpoint, payload))
        if "mutation" in payload["query"]:
            return {"data": {"resolveReviewThread": {"thread": {
                "id": "OLD-T1", "isResolved": True,
            }}}}
        return {"data": {"repository": {"pullRequest": {"reviewThreads": {
            "nodes": [{
                "id": "OLD-T1", "isResolved": False,
                "comments": {"nodes": [{"databaseId": 456}]},
            }],
            "pageInfo": {"hasNextPage": False},
        }}}}}

    monkeypatch.setattr("backend.services.pr_review_service._gh_api_value", fake_gh)
    assert await reconcile_fixed_finding_resolutions(db_factory) == 1
    resolved = await db_session.get(PRFinding, finding.id, populate_existing=True)
    ready = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    restored = await db_session.get(
        PRReview, current_review.id, populate_existing=True
    )
    assert resolved.status == "resolved_fixed"
    assert resolved.thread_status == "resolved"
    assert resolved.github_thread_node_id == "OLD-T1"
    assert resolved.thread_resolved_at is not None
    assert ready.status == "reviewing"
    assert restored.status == "publishing"
    assert restored.pending_action == "lgtm_comment"
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_upgrade_recovery_waits_for_old_pending_finding_publisher(
    db_session,
    db_factory,
    monkeypatch,
):
    new_head = "c" * 40
    repo = MonitoredRepo(
        repo_full_name="fake/pending-recovery",
        webhook_secret="s" * 64,
        review_mode="panel",
        merge_queue_mode="manual",
    )
    db_session.add(repo)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=81,
        current_base_sha=BASE,
        current_head_sha=new_head,
        status="ready_to_merge",
    )
    db_session.add(run)
    await db_session.flush()
    old_review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=81,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="old publisher",
        pr_author="bot",
        pr_url="https://example.invalid/fake/pending-recovery/pull/81",
        status="publishing",
        pending_action="review_comments",
    )
    current_review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=81,
        base_ref="main",
        base_sha=BASE,
        head_sha=new_head,
        pr_title="green head",
        pr_author="bot",
        pr_url="https://example.invalid/fake/pending-recovery/pull/81",
        status="approved",
        action_taken="lgtm_comment",
    )
    db_session.add_all([old_review, current_review])
    await db_session.flush()
    run.current_review_id = current_review.id
    reviewer = PRReviewerRun(
        pr_review_id=old_review.id,
        role="qa_engineer",
        provider="codex",
        status="changes_required",
        prompt_policy_hash="1" * 64,
        guide_pack_hash="2" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=old_review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="3" * 64,
        role="qa_engineer",
        severity="high",
        category="correctness",
        path="app.py",
        line=5,
        title="publication ACK pending",
        evidence="The durable publisher has not recorded its thread yet.",
        impact="A new Gate could otherwise overtake the blocker.",
        required_fix="Wait for the old publisher and resolver.",
        test="Restart between Review and Finding publication.",
        base_sha=BASE,
        head_sha=HEAD,
        thread_nonce="4" * 48,
        status="open",
        thread_status="pending",
    )
    db_session.add(finding)
    await db_session.commit()

    assert await reconcile_fixed_finding_resolutions(db_factory) == 0
    held = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert held.status == "resolving_fixed_threads"
    assert await reconcile_fixed_finding_resolutions(db_factory) == 0
    still_held = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    assert still_held.status == "resolving_fixed_threads"

    # Simulate the old durable publisher reconciling its lost ACK. Only then
    # may the fixed-thread resolver own the GitHub effect and release the Gate.
    finding = await db_session.get(PRFinding, finding.id, populate_existing=True)
    finding.thread_status = "published_inline"
    finding.github_comment_id = 812
    await db_session.commit()

    async def fake_gh(_endpoint, *, payload=None, **_kwargs):
        if "mutation" in payload["query"]:
            return {"data": {"resolveReviewThread": {"thread": {
                "id": "PENDING-T1",
                "isResolved": True,
            }}}}
        return {"data": {"repository": {"pullRequest": {"reviewThreads": {
            "nodes": [{
                "id": "PENDING-T1",
                "isResolved": False,
                "comments": {"nodes": [{"databaseId": 812}]},
            }],
            "pageInfo": {"hasNextPage": False},
        }}}}}

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value",
        fake_gh,
    )
    assert await reconcile_fixed_finding_resolutions(db_factory) == 1
    released = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    resolved = await db_session.get(PRFinding, finding.id, populate_existing=True)
    assert released.status == "ready_to_merge"
    assert resolved.status == "resolved_fixed"
    assert resolved.thread_status == "resolved"


@pytest.mark.asyncio
async def test_fixed_thread_recovery_advances_gate_after_last_resolution_commit(
    db_session, db_factory
):
    """A crash after the final Finding commit must not strand the run."""

    new_head = "c" * 40
    repo = MonitoredRepo(
        repo_full_name="fake/recovery", webhook_secret="s" * 64,
        review_mode="panel", merge_queue_mode="manual",
    )
    db_session.add(repo)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id, pr_number=9, current_base_sha=BASE,
        current_head_sha=new_head, status="resolving_fixed_threads",
    )
    db_session.add(run)
    await db_session.flush()
    started_at = datetime.utcnow() - timedelta(seconds=2)
    publication_task = Task(
        title="recovered current-head reviewer",
        description="immutable panel result",
        status="completed",
        retry_count=0,
        started_at=started_at,
        completed_at=started_at + timedelta(seconds=1),
        metadata_={
            "pr_auto_merge": False,
            "pr_wait_for_ci": False,
            "pr_required_checks": [],
            "pr_action_nonce": "b" * 48,
        },
    )
    db_session.add(publication_task)
    await db_session.flush()
    old_review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=9,
        base_ref="main",
        base_sha=BASE, head_sha=HEAD, pr_title="old", pr_author="bot",
        pr_url="https://example.invalid/fake/recovery/pull/9",
        status="commented",
    )
    current_review = PRReview(
        monitor_run_id=run.id, repo_id=repo.id, pr_number=9,
        base_ref="main",
        base_sha=BASE, head_sha=new_head, pr_title="fixed", pr_author="bot",
        pr_url="https://example.invalid/fake/recovery/pull/9",
        status="publishing", task_id=publication_task.id,
        action_nonce="b" * 48,
        pending_action="waiting_threads:lgtm_comment",
        pending_review_body="Panel reviewers found no blocking issue.",
        publishing_actor="ccm-bot", publishing_retry_count=0,
        publishing_task_started_at=started_at,
        publishing_started_at=started_at,
    )
    db_session.add_all([old_review, current_review])
    await db_session.flush()
    run.current_review_id = current_review.id
    db_session.add(PRReviewerRun(
        pr_review_id=current_review.id, role="principal_engineer",
        task_id=publication_task.id, provider="codex", status="passed",
        prompt_policy_hash="3" * 64, guide_pack_hash="4" * 64,
    ))
    reviewer = PRReviewerRun(
        pr_review_id=old_review.id, role="qa_engineer", provider="codex",
        status="changes_required", prompt_policy_hash="1" * 64,
        guide_pack_hash="2" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    db_session.add(PRFinding(
        pr_review_id=old_review.id, reviewer_run_id=reviewer.id,
        fingerprint="7" * 64, role="qa_engineer", severity="high",
        category="correctness", path="app.py", line=3, title="fixed guard",
        evidence="guard was missing", impact="unsafe", required_fix="add guard",
        test="exercise invalid input", base_sha=BASE, head_sha=HEAD,
        thread_nonce="6" * 48, status="resolved_fixed",
        thread_status="resolved", github_comment_id=789,
        thread_resolved_at=datetime.utcnow(),
    ))
    await db_session.commit()

    assert await reconcile_fixed_finding_resolutions(db_factory) == 0
    recovered = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    restored = await db_session.get(
        PRReview, current_review.id, populate_existing=True
    )
    assert recovered.status == "reviewing"
    assert restored.pending_action == "lgtm_comment"


@pytest.mark.asyncio
async def test_rebuttal_fallback_resolution_lease_allows_exactly_one_post(
    db_session, db_factory, monkeypatch
):
    _, _, _, finding, rebuttal = await _accepted_resolution_fixture(
        db_session,
        repo_name="fake/fallback-race",
        thread_status="published_fallback",
        github_comment_id=801,
    )
    post_started = asyncio.Event()
    allow_post = asyncio.Event()
    post_calls = 0

    async def fake_gh(endpoint, *, method="GET", payload=None, **_kwargs):
        nonlocal post_calls
        if method == "POST":
            post_calls += 1
            post_started.set()
            await allow_post.wait()
            return {
                "id": 9001,
                "body": payload["body"],
                "user": {"login": "ccm-bot"},
            }
        return [[]]

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    async def fake_login():
        return "ccm-bot"

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_authenticated_login",
        fake_login,
    )
    first = asyncio.create_task(reconcile_rebuttal_resolutions(db_factory))
    await asyncio.wait_for(post_started.wait(), timeout=2)
    second = asyncio.create_task(reconcile_rebuttal_resolutions(db_factory))
    assert await asyncio.wait_for(second, timeout=2) == 0
    allow_post.set()
    assert await asyncio.wait_for(first, timeout=2) == 1

    assert post_calls == 1
    resolved = await db_session.get(PRFinding, finding.id, populate_existing=True)
    terminal = await db_session.get(
        PRFindingRebuttal, rebuttal.id, populate_existing=True
    )
    assert resolved.thread_status == "resolved"
    assert terminal.status == "resolved"


@pytest.mark.asyncio
async def test_cancelled_resolution_leaves_lease_for_expiry_recovery(
    db_session, db_factory, monkeypatch
):
    _, _, _, finding, rebuttal = await _accepted_resolution_fixture(
        db_session,
        repo_name="fake/cancelled-resolution",
        thread_status="published_fallback",
        github_comment_id=804,
    )
    post_started = asyncio.Event()
    never_finishes = asyncio.Event()

    async def fake_gh(_endpoint, *, method="GET", **_kwargs):
        if method == "POST":
            post_started.set()
            await never_finishes.wait()
            return {"id": 9003}
        return [[]]

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    async def fake_login():
        return "ccm-bot"

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_authenticated_login",
        fake_login,
    )
    reconciliation = asyncio.create_task(
        reconcile_rebuttal_resolutions(db_factory)
    )
    await asyncio.wait_for(post_started.wait(), timeout=2)
    reconciliation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await reconciliation

    leased = await db_session.get(PRFinding, finding.id, populate_existing=True)
    still_accepted = await db_session.get(
        PRFindingRebuttal, rebuttal.id, populate_existing=True
    )
    assert leased.thread_status == "published_fallback"
    assert leased.resolution_lease_token is not None
    assert leased.resolution_lease_expires_at is not None
    assert still_accepted.status == "accepted"


@pytest.mark.asyncio
async def test_fixed_inline_resolution_lease_allows_exactly_one_mutation(
    db_session, db_factory, monkeypatch
):
    new_head = "d" * 40
    repo = MonitoredRepo(
        repo_full_name="fake/inline-race",
        webhook_secret="s" * 64,
        review_mode="panel",
    )
    db_session.add(repo)
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=18,
        current_base_sha=BASE,
        current_head_sha=new_head,
        status="resolving_fixed_threads",
    )
    db_session.add(run)
    await db_session.flush()
    old_review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=18,
        base_ref="main",
        base_sha=BASE,
        head_sha=HEAD,
        pr_title="old",
        pr_author="bot",
        pr_url="https://example.invalid/fake/inline-race/pull/18",
        status="commented",
    )
    current_review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=18,
        base_ref="main",
        base_sha=BASE,
        head_sha=new_head,
        pr_title="fixed",
        pr_author="bot",
        pr_url="https://example.invalid/fake/inline-race/pull/18",
        status="approved",
    )
    db_session.add_all([old_review, current_review])
    await db_session.flush()
    run.current_review_id = current_review.id
    reviewer = PRReviewerRun(
        pr_review_id=old_review.id,
        role="qa_engineer",
        provider="codex",
        status="changes_required",
        prompt_policy_hash="6" * 64,
        guide_pack_hash="7" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=old_review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="8" * 64,
        role="qa_engineer",
        severity="high",
        category="correctness",
        path="app.py",
        line=3,
        title="old bug",
        evidence="bug existed",
        impact="unsafe",
        required_fix="fix it",
        test="regression",
        base_sha=BASE,
        head_sha=HEAD,
        thread_nonce="9" * 48,
        thread_status="published_inline",
        github_comment_id=802,
    )
    db_session.add(finding)
    await db_session.commit()

    mutation_started = asyncio.Event()
    allow_mutation = asyncio.Event()
    mutation_calls = 0

    async def fake_gh(_endpoint, *, payload=None, **_kwargs):
        nonlocal mutation_calls
        if "mutation" in payload["query"]:
            mutation_calls += 1
            mutation_started.set()
            await allow_mutation.wait()
            return {"data": {"resolveReviewThread": {"thread": {
                "id": "OLD-T2", "isResolved": True,
            }}}}
        return {"data": {"repository": {"pullRequest": {"reviewThreads": {
            "nodes": [{
                "id": "OLD-T2",
                "isResolved": False,
                "comments": {"nodes": [{"databaseId": 802}]},
            }],
            "pageInfo": {"hasNextPage": False},
        }}}}}

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    first = asyncio.create_task(reconcile_fixed_finding_resolutions(db_factory))
    await asyncio.wait_for(mutation_started.wait(), timeout=2)
    second = asyncio.create_task(reconcile_fixed_finding_resolutions(db_factory))
    assert await asyncio.wait_for(second, timeout=2) == 0
    allow_mutation.set()
    assert await asyncio.wait_for(first, timeout=2) == 1

    assert mutation_calls == 1
    resolved = await db_session.get(PRFinding, finding.id, populate_existing=True)
    assert resolved.thread_status == "resolved"
    assert resolved.resolution_lease_token is None


@pytest.mark.asyncio
async def test_expired_rebuttal_resolution_lease_recovers_existing_effect(
    db_session, db_factory, monkeypatch
):
    _, _, _, finding, rebuttal = await _accepted_resolution_fixture(
        db_session,
        repo_name="fake/expired-lease",
        thread_status="published_fallback",
        github_comment_id=803,
    )
    finding.resolution_lease_token = "a" * 48
    finding.resolution_lease_expires_at = datetime.utcnow() - timedelta(minutes=1)
    await db_session.commit()
    post_calls = 0

    async def fake_gh(_endpoint, *, method="GET", **_kwargs):
        nonlocal post_calls
        if method == "POST":
            post_calls += 1
            return {"id": 9002}
        marker = (
            f"<!-- ccm-finding-resolution:{rebuttal.resolution_nonce};"
            f"head:{finding.head_sha};fingerprint:{finding.fingerprint} -->"
        )
        return [[{
            "id": 9001,
            "body": marker,
            "user": {"login": "ccm-bot"},
        }]]

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    assert await reconcile_rebuttal_resolutions(db_factory) == 1
    assert post_calls == 0
    resolved = await db_session.get(PRFinding, finding.id, populate_existing=True)
    terminal = await db_session.get(
        PRFindingRebuttal, rebuttal.id, populate_existing=True
    )
    assert resolved.thread_status == "resolved"
    assert resolved.resolution_lease_token is None
    assert terminal.status == "resolved"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("repo_enabled", "run_status"),
    ((False, "adjudicating"), (True, "paused")),
)
async def test_resolved_rebuttal_shortcut_obeys_policy_fence(
    db_session, db_factory, repo_enabled, run_status
):
    repo, run, _, finding, rebuttal = await _accepted_resolution_fixture(
        db_session,
        repo_name=f"fake/shortcut-{repo_enabled}-{run_status}",
        thread_status="published_fallback",
        github_comment_id=805,
    )
    repo.enabled = repo_enabled
    repo.merge_queue_mode = "auto"
    run.status = run_status
    finding.thread_status = "resolved"
    await db_session.commit()

    assert await reconcile_rebuttal_resolutions(db_factory) == 0
    terminal = await db_session.get(
        PRFindingRebuttal, rebuttal.id, populate_existing=True
    )
    unchanged_run = await db_session.get(
        PRMonitorRun, run.id, populate_existing=True
    )
    actions = list((await db_session.execute(
        select(PRMergeQueueAction).where(
            PRMergeQueueAction.monitor_run_id == run.id
        )
    )).scalars())
    assert terminal.status == "accepted"
    assert unchanged_run.status == run_status
    assert actions == []


@pytest.mark.asyncio
async def test_fixed_fallback_crash_recovery_uses_frozen_actor(
    db_session, db_factory, monkeypatch
):
    _, _, _, _, finding = await _fixed_resolution_fixture(
        db_session,
        repo_name="fake/fixed-actor-recovery",
        fixed_resolution_actor="account-a",
    )
    finding.resolution_lease_token = "b" * 48
    finding.resolution_lease_expires_at = datetime.utcnow() - timedelta(minutes=1)
    await db_session.commit()
    post_calls = login_calls = 0
    marker = (
        f"<!-- ccm-finding-fixed:{finding.thread_nonce};"
        f"finding-head:{finding.head_sha};fixed-head:{'c' * 40} -->"
    )

    async def fake_gh(_endpoint, *, method="GET", **_kwargs):
        nonlocal post_calls
        if method == "POST":
            post_calls += 1
            return {"id": 9999}
        return [[{
            "id": 9998,
            "body": marker,
            "user": {"login": "account-a"},
        }]]

    async def fake_login():
        nonlocal login_calls
        login_calls += 1
        return "account-b"

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_authenticated_login",
        fake_login,
    )

    assert await reconcile_fixed_finding_resolutions(db_factory) == 1
    resolved = await db_session.get(PRFinding, finding.id, populate_existing=True)
    assert resolved.thread_status == "resolved"
    assert resolved.status == "resolved_fixed"
    assert resolved.fixed_resolution_actor == "account-a"
    assert resolved.resolution_lease_token is None
    assert post_calls == 0
    assert login_calls == 0


@pytest.mark.asyncio
async def test_fixed_fallback_actor_change_refuses_new_post(
    db_session, db_factory, monkeypatch
):
    _, run, _, _, finding = await _fixed_resolution_fixture(
        db_session,
        repo_name="fake/fixed-actor-change",
        fixed_resolution_actor="account-a",
    )
    post_calls = 0

    async def fake_gh(_endpoint, *, method="GET", **_kwargs):
        nonlocal post_calls
        if method == "POST":
            post_calls += 1
            return {"id": 9999}
        return [[]]

    async def fake_login():
        return "account-b"

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_authenticated_login",
        fake_login,
    )

    assert await reconcile_fixed_finding_resolutions(db_factory) == 0
    pending = await db_session.get(PRFinding, finding.id, populate_existing=True)
    unchanged_run = await db_session.get(
        PRMonitorRun, run.id, populate_existing=True
    )
    assert pending.thread_status == "published_fallback"
    assert pending.fixed_resolution_actor == "account-a"
    assert pending.resolution_lease_token is None
    assert "actor changed" in pending.thread_error
    assert unchanged_run.status == "resolving_fixed_threads"
    assert post_calls == 0


@pytest.mark.asyncio
async def test_rebuttal_fallback_actor_change_refuses_new_post(
    db_session, db_factory, monkeypatch
):
    _, _, _, finding, rebuttal = await _accepted_resolution_fixture(
        db_session,
        repo_name="fake/rebuttal-actor-change",
        thread_status="published_fallback",
        github_comment_id=806,
    )
    rebuttal.resolution_actor = "account-a"
    await db_session.commit()
    post_calls = 0

    async def fake_gh(_endpoint, *, method="GET", **_kwargs):
        nonlocal post_calls
        if method == "POST":
            post_calls += 1
            return {"id": 9999}
        return [[]]

    async def fake_login():
        return "account-b"

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_authenticated_login",
        fake_login,
    )

    assert await reconcile_rebuttal_resolutions(db_factory) == 0
    pending = await db_session.get(PRFinding, finding.id, populate_existing=True)
    accepted = await db_session.get(
        PRFindingRebuttal, rebuttal.id, populate_existing=True
    )
    assert pending.thread_status == "published_fallback"
    assert pending.resolution_lease_token is None
    assert "actor changed" in pending.thread_error
    assert accepted.status == "accepted"
    assert accepted.resolution_actor == "account-a"
    assert post_calls == 0


@pytest.mark.asyncio
async def test_rebuttal_fallback_crash_recovery_uses_frozen_actor(
    db_session, db_factory, monkeypatch
):
    _, _, _, finding, rebuttal = await _accepted_resolution_fixture(
        db_session,
        repo_name="fake/rebuttal-actor-recovery",
        thread_status="published_fallback",
        github_comment_id=807,
    )
    rebuttal.resolution_actor = "account-a"
    finding.resolution_lease_token = "c" * 48
    finding.resolution_lease_expires_at = datetime.utcnow() - timedelta(minutes=1)
    await db_session.commit()
    post_calls = login_calls = 0
    marker = (
        f"<!-- ccm-finding-resolution:{rebuttal.resolution_nonce};"
        f"head:{finding.head_sha};fingerprint:{finding.fingerprint} -->"
    )

    async def fake_gh(_endpoint, *, method="GET", **_kwargs):
        nonlocal post_calls
        if method == "POST":
            post_calls += 1
            return {"id": 9999}
        return [[{
            "id": 9998,
            "body": marker,
            "user": {"login": "account-a"},
        }]]

    async def fake_login():
        nonlocal login_calls
        login_calls += 1
        return "account-b"

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_authenticated_login",
        fake_login,
    )

    assert await reconcile_rebuttal_resolutions(db_factory) == 1
    resolved = await db_session.get(PRFinding, finding.id, populate_existing=True)
    terminal = await db_session.get(
        PRFindingRebuttal, rebuttal.id, populate_existing=True
    )
    assert resolved.thread_status == "resolved"
    assert terminal.status == "resolved"
    assert post_calls == 0
    assert login_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ("rebuttal", "fixed"))
@pytest.mark.parametrize("include_valid", (False, True))
async def test_fallback_resolution_marker_forgery_cannot_block_ack_recovery(
    db_session,
    db_factory,
    monkeypatch,
    kind,
    include_valid,
):
    actor = "ccm-bot"
    if kind == "rebuttal":
        _, _, _, finding, rebuttal = await _accepted_resolution_fixture(
            db_session,
            repo_name=f"fake/rebuttal-forged-{include_valid}",
            thread_status="published_fallback",
            github_comment_id=818,
        )
        marker = (
            f"<!-- ccm-finding-resolution:{rebuttal.resolution_nonce};"
            f"head:{finding.head_sha};fingerprint:{finding.fingerprint} -->"
        )
        reconcile = reconcile_rebuttal_resolutions
    else:
        _, _, _, _, finding = await _fixed_resolution_fixture(
            db_session,
            repo_name=f"fake/fixed-forged-{include_valid}",
            fixed_resolution_actor=actor,
        )
        marker = (
            f"<!-- ccm-finding-fixed:{finding.thread_nonce};"
            f"finding-head:{finding.head_sha};fixed-head:{'c' * 40} -->"
        )
        reconcile = reconcile_fixed_finding_resolutions

    post_calls = 0

    async def fake_gh(_endpoint, *, method="GET", payload=None, **_kwargs):
        nonlocal post_calls
        if method == "POST":
            post_calls += 1
            return {
                "id": 9200,
                "body": payload["body"],
                "user": {"login": actor},
            }
        comments = [{
            "id": 9198,
            "body": marker,
            "user": {"login": "untrusted-user"},
        }]
        if include_valid:
            comments.append({
                "id": 9199,
                "body": marker,
                "user": {"login": actor},
            })
        return [comments]

    async def fake_login():
        return actor

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value",
        fake_gh,
    )
    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_authenticated_login",
        fake_login,
    )

    assert await reconcile(db_factory) == 1
    resolved = await db_session.get(PRFinding, finding.id, populate_existing=True)
    assert resolved.thread_status == "resolved"
    assert post_calls == (0 if include_valid else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ("rebuttal", "fixed"))
async def test_fallback_resolution_requires_remote_marker_evidence(
    db_session, db_factory, monkeypatch, kind
):
    if kind == "rebuttal":
        _, _, _, finding, rebuttal = await _accepted_resolution_fixture(
            db_session,
            repo_name="fake/rebuttal-malformed-response",
            thread_status="published_fallback",
            github_comment_id=808,
        )
        reconcile = reconcile_rebuttal_resolutions
    else:
        _, _, _, _, finding = await _fixed_resolution_fixture(
            db_session,
            repo_name="fake/fixed-malformed-response",
        )
        rebuttal = None
        reconcile = reconcile_fixed_finding_resolutions
    post_calls = list_calls = 0

    async def fake_gh(_endpoint, *, method="GET", **_kwargs):
        nonlocal post_calls, list_calls
        if method == "POST":
            post_calls += 1
            # An id alone is not proof that the exact marker was durably
            # published by the frozen identity.
            return {"id": 12345}
        list_calls += 1
        return [[]]

    async def fake_login():
        return "ccm-bot"

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_authenticated_login",
        fake_login,
    )

    assert await reconcile(db_factory) == 0
    pending = await db_session.get(PRFinding, finding.id, populate_existing=True)
    assert pending.thread_status == "published_fallback"
    assert pending.resolution_lease_token is None
    assert "malformed" in pending.thread_error
    if rebuttal is not None:
        accepted = await db_session.get(
            PRFindingRebuttal, rebuttal.id, populate_existing=True
        )
        assert accepted.status == "accepted"
    assert post_calls == 1
    assert list_calls == 2


@pytest.mark.asyncio
async def test_near_expiry_resolution_lease_cannot_start_post(
    db_session, db_factory, monkeypatch
):
    _, _, _, finding, rebuttal = await _accepted_resolution_fixture(
        db_session,
        repo_name="fake/near-expiry",
        thread_status="published_fallback",
        github_comment_id=809,
    )
    list_calls = post_calls = login_calls = 0

    async def fake_gh(_endpoint, *, method="GET", **_kwargs):
        nonlocal list_calls, post_calls
        if method == "POST":
            post_calls += 1
            return {"id": 9999}
        list_calls += 1
        async with db_factory() as other_db:
            leased = await other_db.get(PRFinding, finding.id)
            leased.resolution_lease_expires_at = (
                datetime.utcnow() + timedelta(seconds=5)
            )
            await other_db.commit()
        return [[]]

    async def fake_login():
        nonlocal login_calls
        login_calls += 1
        return "ccm-bot"

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_authenticated_login",
        fake_login,
    )

    assert await reconcile_rebuttal_resolutions(db_factory) == 0
    pending = await db_session.get(PRFinding, finding.id, populate_existing=True)
    accepted = await db_session.get(
        PRFindingRebuttal, rebuttal.id, populate_existing=True
    )
    assert pending.thread_status == "published_fallback"
    assert pending.resolution_lease_token is None
    assert accepted.status == "accepted"
    assert list_calls == 1
    assert login_calls == 0
    assert post_calls == 0


@pytest.mark.asyncio
async def test_fixed_finish_rechecks_repo_after_remote_effect(
    db_session, db_factory, monkeypatch
):
    repo, run, _, _, finding = await _fixed_resolution_fixture(
        db_session,
        repo_name="fake/fixed-finish-fence",
    )

    async def fake_gh(_endpoint, *, method="GET", payload=None, **_kwargs):
        if method != "POST":
            return [[]]
        async with db_factory() as other_db:
            changed_repo = await other_db.get(MonitoredRepo, repo.id)
            changed_repo.enabled = False
            await other_db.commit()
        return {
            "id": 12346,
            "body": payload["body"],
            "user": {"login": "ccm-bot"},
        }

    async def fake_login():
        return "ccm-bot"

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_authenticated_login",
        fake_login,
    )

    assert await reconcile_fixed_finding_resolutions(db_factory) == 0
    pending = await db_session.get(PRFinding, finding.id, populate_existing=True)
    unchanged_run = await db_session.get(
        PRMonitorRun, run.id, populate_existing=True
    )
    assert pending.thread_status == "published_fallback"
    assert pending.resolution_lease_token is None
    assert unchanged_run.status == "resolving_fixed_threads"


@pytest.mark.asyncio
async def test_rebuttal_finish_rechecks_run_after_remote_effect(
    db_session, db_factory, monkeypatch
):
    _, run, _, finding, rebuttal = await _accepted_resolution_fixture(
        db_session,
        repo_name="fake/rebuttal-finish-fence",
        thread_status="published_fallback",
        github_comment_id=810,
    )

    async def fake_gh(_endpoint, *, method="GET", payload=None, **_kwargs):
        if method != "POST":
            return [[]]
        async with db_factory() as other_db:
            changed_run = await other_db.get(PRMonitorRun, run.id)
            changed_run.status = "paused"
            await other_db.commit()
        return {
            "id": 12347,
            "body": payload["body"],
            "user": {"login": "ccm-bot"},
        }

    async def fake_login():
        return "ccm-bot"

    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )
    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_authenticated_login",
        fake_login,
    )

    assert await reconcile_rebuttal_resolutions(db_factory) == 0
    pending = await db_session.get(PRFinding, finding.id, populate_existing=True)
    accepted = await db_session.get(
        PRFindingRebuttal, rebuttal.id, populate_existing=True
    )
    unchanged_run = await db_session.get(
        PRMonitorRun, run.id, populate_existing=True
    )
    assert pending.thread_status == "published_fallback"
    assert pending.resolution_lease_token is None
    assert accepted.status == "accepted"
    assert unchanged_run.status == "paused"


@pytest.mark.asyncio
async def test_fixed_gate_rechecks_repo_after_finding_commit(
    db_session, db_factory, monkeypatch
):
    from backend.services import pr_review_adjudication as adjudication_service

    repo, run, _, _, finding = await _fixed_resolution_fixture(
        db_session,
        repo_name="fake/fixed-gate-fence",
        thread_status="published_inline",
    )
    original_finish = adjudication_service._finish_fixed_resolution

    async def finish_then_disable(*args, **kwargs):
        finished = await original_finish(*args, **kwargs)
        if finished:
            async with db_factory() as other_db:
                changed_repo = await other_db.get(MonitoredRepo, repo.id)
                changed_repo.enabled = False
                await other_db.commit()
        return finished

    async def fake_gh(_endpoint, *, payload=None, **_kwargs):
        if "mutation" in payload["query"]:
            return {"data": {"resolveReviewThread": {"thread": {
                "id": "FIXED-T1", "isResolved": True,
            }}}}
        return {"data": {"repository": {"pullRequest": {"reviewThreads": {
            "nodes": [{
                "id": "FIXED-T1",
                "isResolved": False,
                "comments": {"nodes": [{"databaseId": 990}]},
            }],
            "pageInfo": {"hasNextPage": False},
        }}}}}

    monkeypatch.setattr(
        adjudication_service, "_finish_fixed_resolution", finish_then_disable
    )
    monkeypatch.setattr(
        "backend.services.pr_review_service._gh_api_value", fake_gh
    )

    assert await reconcile_fixed_finding_resolutions(db_factory) == 1
    resolved = await db_session.get(PRFinding, finding.id, populate_existing=True)
    held_run = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    actions = list((await db_session.execute(
        select(PRMergeQueueAction).where(
            PRMergeQueueAction.monitor_run_id == run.id
        )
    )).scalars())
    assert resolved.thread_status == "resolved"
    assert held_run.status == "resolving_fixed_threads"
    assert actions == []


@pytest.mark.asyncio
async def test_fixed_no_finding_recovery_obeys_disabled_repo_fence(
    db_session, db_factory
):
    repo, run, _, _, finding = await _fixed_resolution_fixture(
        db_session,
        repo_name="fake/fixed-empty-gate-fence",
    )
    repo.enabled = False
    finding.status = "resolved_fixed"
    finding.thread_status = "resolved"
    finding.thread_resolved_at = datetime.utcnow()
    await db_session.commit()

    assert await reconcile_fixed_finding_resolutions(db_factory) == 0
    held_run = await db_session.get(PRMonitorRun, run.id, populate_existing=True)
    actions = list((await db_session.execute(
        select(PRMergeQueueAction).where(
            PRMergeQueueAction.monitor_run_id == run.id
        )
    )).scalars())
    assert held_run.status == "resolving_fixed_threads"
    assert actions == []
