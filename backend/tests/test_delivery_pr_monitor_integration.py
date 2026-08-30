"""Delivery controller remains the only repair wake owner for its PR."""

from datetime import datetime, timedelta

import pytest
from sqlalchemy import select

from backend.models.delivery import DeliveryRun
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRMergeQueueAction,
    PRMonitorRun,
    PRRepairWake,
    PRReview,
    PRReviewerRun,
)
from backend.models.project import Project
from backend.models.task import Task
from backend.services import pr_review_panel
from backend.services.delivery_pr_policy import frozen_delivery_pr_policy
from backend.services.delivery_service import value_hash
from backend.services.pr_monitor_loop import (
    attach_review_to_run,
    record_blocking_evidence,
    record_gate_pass,
)
from backend.services.pr_review_adjudication import (
    reconcile_fixed_finding_resolutions,
)


BASE_SHA = "a" * 40
HEAD_SHA = "b" * 40


async def _delivery_review(
    db_session,
    *,
    suffix: str,
    merge_queue_mode: str = "manual",
    auto_repair: bool = True,
    auto_merge: bool = False,
    wait_for_ci: bool = True,
) -> tuple[MonitoredRepo, PRMonitorRun, PRReview, DeliveryRun]:
    project = Project(name=f"delivery-monitor-{suffix}")
    db_session.add(project)
    await db_session.flush()
    repo = MonitoredRepo(
        repo_full_name=f"owner/delivery-{suffix}",
        project_id=project.id,
        webhook_secret="s" * 64,
        review_mode="panel",
        auto_merge=auto_merge,
        auto_repair=auto_repair,
        max_repair_attempts=3,
        merge_queue_mode=merge_queue_mode,
        wait_for_ci=wait_for_ci,
        required_checks=[
            {
                "kind": "check_run",
                "name": "tests",
                "app_slug": "github-actions",
            }
        ] if wait_for_ci else [],
    )
    db_session.add(repo)
    await db_session.flush()
    monitor = PRMonitorRun(
        repo_id=repo.id,
        pr_number=8,
        current_base_sha=BASE_SHA,
        current_head_sha=HEAD_SHA,
        status="reviewing",
    )
    db_session.add(monitor)
    await db_session.flush()
    policy = {
        "schema_version": 1,
        "terminal": "merged" if auto_merge else "ready_to_merge",
        "auto_merge": auto_merge,
        "pr_monitor": {
            "repo_id": repo.id,
            "repo_full_name": repo.repo_full_name,
            "review_mode": "panel",
            "wait_for_ci": wait_for_ci,
            "required_checks": repo.required_checks,
        },
    }
    delivery = DeliveryRun(
        admission_scope="system",
        idempotency_key="pr-monitor-integration",
        request_hash="f" * 64,
        project_id=project.id,
        monitored_repo_id=repo.id,
        pr_monitor_run_id=monitor.id,
        title="Delivery",
        requirements="Implement and validate",
        requirements_hash="1" * 64,
        policy_snapshot=policy,
        policy_hash=value_hash(policy),
        base_branch="main",
        delivery_branch=f"ccm/delivery/{suffix}",
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        pr_number=8,
        pr_url=f"https://github.com/{repo.repo_full_name}/pull/8",
        phase="monitoring",
        activity="waiting",
        wait_reason="pr_monitor",
    )
    db_session.add(delivery)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=monitor.id,
        repo_id=repo.id,
        pr_number=8,
        base_ref="main",
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        delivery_id=f"delivery:{delivery.id}:{HEAD_SHA}",
        pr_title="Delivery review",
        pr_author="agent",
        pr_url=delivery.pr_url,
        status="commented",
        action_taken="review_comments",
        action_nonce="c" * 48,
        ci_status="passed",
    )
    db_session.add(review)
    await db_session.flush()
    monitor.current_review_id = review.id
    await db_session.commit()
    return repo, monitor, review, delivery


@pytest.mark.asyncio
async def test_delivery_panel_only_monitor_uses_frozen_no_ci_policy(db_session):
    _repo, monitor, review, _delivery = await _delivery_review(
        db_session,
        suffix="panel-only-policy",
        wait_for_ci=False,
    )

    policy = await frozen_delivery_pr_policy(
        db_session,
        review,
        monitor_run_id=monitor.id,
    )

    assert policy is not None
    assert policy.auto_merge is False
    assert policy.terminal == "ready_to_merge"
    assert policy.wait_for_ci is False
    assert policy.required_checks == []


@pytest.mark.asyncio
async def test_terminal_delivery_run_cannot_be_reactivated_by_opaque_review(
    db_session,
):
    from backend.services.pr_review_service import PRReviewLifecycleConflict

    repo, monitor, source_review, delivery = await _delivery_review(
        db_session,
        suffix="terminal-reopen-fence",
        auto_repair=False,
        auto_merge=False,
        wait_for_ci=False,
    )
    terminal_at = datetime.utcnow()
    delivery.phase = "done"
    delivery.activity = "terminal"
    delivery.outcome = "success"
    delivery.completed_at = terminal_at
    monitor.status = "closed"
    monitor.completed_at = terminal_at
    monitor.terminal_intent_status = "closed"
    monitor.terminal_intent_base_ref = "main"
    monitor.terminal_intent_head_sha = HEAD_SHA
    monitor.terminal_intent_delivery_id = "delivery-terminal"
    monitor.terminal_intent_observed_at = terminal_at
    await db_session.commit()
    monitor_id = monitor.id
    source_review_id = source_review.id

    replacement = PRReview(
        attempt=2,
        rerun_of_review_id=source_review_id,
        rerun_idempotency_key="wh:opaque-reopened-delivery",
        repo_id=repo.id,
        pr_number=monitor.pr_number,
        base_ref="main",
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        delivery_id="opaque-github-reopened-delivery",
        pr_title="Reopened Delivery PR",
        pr_author="allowed-user",
        pr_url=source_review.pr_url,
        status="pending",
    )
    db_session.add(replacement)
    await db_session.flush()
    replacement_id = replacement.id

    with pytest.raises(PRReviewLifecycleConflict, match="Delivery ownership"):
        await attach_review_to_run(
            db_session,
            repo=repo,
            review=replacement,
            allow_terminal_reactivation=True,
        )
    await db_session.rollback()

    stored_monitor = await db_session.get(PRMonitorRun, monitor_id)
    assert stored_monitor.status == "closed"
    assert stored_monitor.completed_at == terminal_at
    assert stored_monitor.current_review_id == source_review_id
    assert stored_monitor.terminal_intent_status == "closed"
    assert stored_monitor.terminal_intent_delivery_id == "delivery-terminal"
    assert await db_session.scalar(
        select(PRReview.id).where(PRReview.id == replacement_id)
    ) is None


@pytest.mark.asyncio
async def test_delivery_task_never_receives_legacy_pr_repair_wake(db_session):
    repo = MonitoredRepo(
        repo_full_name="owner/delivery",
        webhook_secret="s" * 64,
        review_mode="panel",
        auto_repair=True,
        max_repair_attempts=3,
    )
    developer = Task(
        title="Delivery developer",
        mode="delivery_loop",
        delivery_run_id=91,
        delivery_role="developer",
        status="delivery_waiting",
    )
    db_session.add_all([repo, developer])
    await db_session.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=8,
        current_base_sha="a" * 40,
        current_head_sha="b" * 40,
        status="reviewing",
        developer_task_id=developer.id,
    )
    db_session.add(run)
    await db_session.flush()
    review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=8,
        base_ref="main",
        base_sha=run.current_base_sha,
        head_sha=run.current_head_sha,
        pr_title="blocked",
        pr_author="agent",
        pr_url="https://github.com/owner/delivery/pull/8",
        status="commented",
        action_taken="review_comments",
        ci_status="failed",
    )
    db_session.add(review)
    await db_session.flush()
    run.current_review_id = review.id
    await db_session.commit()

    wake = await record_blocking_evidence(
        db_session,
        review_id=review.id,
        reason_kind="ci_failed",
    )

    # A CI label without structured exact-head details is not actionable
    # Repair evidence. Delivery ownership still remains protected, but CCM
    # does not create an empty shadow instruction merely for audit cosmetics.
    assert wake is None
    assert list((await db_session.execute(
        select(PRRepairWake).where(PRRepairWake.monitor_run_id == run.id)
    )).scalars()) == []
    await db_session.refresh(run)
    assert run.status == "waiting_for_fix"
    assert run.repair_attempts == 0


@pytest.mark.asyncio
async def test_delivery_marker_suppresses_repair_after_task_binding_is_cleared(
    db_session,
):
    _repo, monitor, review, _delivery = await _delivery_review(
        db_session,
        suffix="no-legacy-repair",
        auto_repair=True,
    )
    assert monitor.developer_task_id is None

    reviewer = PRReviewerRun(
        pr_review_id=review.id,
        role="senior_engineer",
        provider="codex",
        status="changes_required",
        prompt_policy_hash="c" * 64,
        guide_pack_hash="d" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    db_session.add(PRFinding(
        pr_review_id=review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="e" * 64,
        thread_nonce="1" * 48,
        role=reviewer.role,
        severity="high",
        category="correctness",
        path="app.py",
        line=4,
        title="Wrong branch",
        evidence="The false branch returns success.",
        impact="Invalid input is accepted.",
        required_fix="Return an error.",
        test="Exercise invalid input.",
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
    ))
    await db_session.commit()

    wake = await record_blocking_evidence(
        db_session,
        review_id=review.id,
        reason_kind="review_blocked",
    )

    assert wake is not None
    assert wake.status == "shadow"
    assert wake.developer_task_id is None
    await db_session.refresh(monitor)
    assert monitor.status == "waiting_for_fix"
    assert monitor.repair_attempts == 0


@pytest.mark.asyncio
async def test_delivery_gate_never_enters_auto_merge_queue_after_config_drift(
    db_session,
):
    repo, monitor, review, _delivery = await _delivery_review(
        db_session,
        suffix="manual-gate",
        merge_queue_mode="auto",
    )
    assert repo.merge_queue_mode == "auto"
    review.status = "approved"
    review.action_taken = "lgtm_comment"
    await db_session.commit()

    await record_gate_pass(db_session, review.id)

    await db_session.refresh(monitor)
    assert monitor.status == "ready_to_merge"
    actions = list(
        (
            await db_session.execute(
                select(PRMergeQueueAction).where(
                    PRMergeQueueAction.monitor_run_id == monitor.id
                )
            )
        ).scalars()
    )
    assert actions == []


@pytest.mark.asyncio
async def test_delivery_zero_thread_gate_restores_exact_publication_stage(
    db_session,
    db_factory,
):
    repo, monitor, review, _delivery = await _delivery_review(
        db_session,
        suffix="fixed-thread-gate",
    )
    started_at = datetime.utcnow() - timedelta(seconds=2)
    publication_task = Task(
        title="Delivery exact-head panel publisher",
        description="immutable panel result",
        status="completed",
        retry_count=0,
        started_at=started_at,
        completed_at=started_at + timedelta(seconds=1),
        metadata_={
            "pr_auto_merge": False,
            "pr_wait_for_ci": True,
            "pr_required_checks": repo.required_checks,
            "pr_action_nonce": review.action_nonce,
        },
    )
    db_session.add(publication_task)
    await db_session.flush()
    monitor.status = "resolving_fixed_threads"
    review.status = "publishing"
    review.action_taken = None
    review.task_id = publication_task.id
    review.pending_action = "waiting_threads:lgtm_comment"
    review.pending_review_body = "Panel reviewers found no blocking issue."
    review.publishing_actor = "ccm-bot"
    review.publishing_retry_count = publication_task.retry_count
    review.publishing_task_started_at = publication_task.started_at
    review.publishing_started_at = started_at
    await db_session.commit()

    # No Finding remains, so recovery should cross the zero-thread Gate even
    # though this Delivery-owned Monitor is intentionally not yet reviewing.
    assert await reconcile_fixed_finding_resolutions(db_factory) == 0

    restored_monitor = await db_session.get(
        PRMonitorRun,
        monitor.id,
        populate_existing=True,
    )
    restored_review = await db_session.get(
        PRReview,
        review.id,
        populate_existing=True,
    )
    assert restored_monitor.status == "reviewing"
    assert restored_review.status == "publishing"
    assert restored_review.pending_action == "lgtm_comment"


@pytest.mark.asyncio
async def test_delivery_panel_tasks_use_frozen_no_auto_merge_policy(db_session):
    repo, _monitor, review, _delivery = await _delivery_review(
        db_session,
        suffix="waiting-ci-policy",
    )
    # Simulate persisted configuration drift while the exact review is waiting
    # for CI.  The Delivery policy, not mutable MonitoredRepo state, owns the
    # eventual GitHub action.
    repo.auto_merge = True
    review.status = "waiting_ci"
    context = {
        "base_ref": "main",
        "guidance": {"CLAUDE.md": None, "PROGRESS.md": None},
        "material": {
            "number": review.pr_number,
            "title": review.pr_title,
            "body": "",
            "author": review.pr_author,
            "base_ref": "main",
            "head_ref": "feature",
            "files": [],
            "patch": "",
            "changed_file_contents": [],
        },
    }

    await pr_review_panel._add_panel_tasks(
        db_session,
        repo=repo,
        review=review,
        context=context,
    )

    reviewer_tasks = list(
        (
            await db_session.execute(
                select(Task).where(Task.tags.contains(["pr-review"]))
            )
        ).scalars()
    )
    assert len(reviewer_tasks) == len(pr_review_panel.REVIEWER_ROLES)
    assert all(
        task.metadata_.get("pr_auto_merge") is False
        for task in reviewer_tasks
    )


@pytest.mark.asyncio
async def test_delivery_panel_tasks_use_frozen_auto_merge_policy(db_session):
    repo, _monitor, review, _delivery = await _delivery_review(
        db_session,
        suffix="waiting-ci-auto-merge",
        auto_merge=True,
    )
    # The mutable row drifting narrower must not change an already admitted
    # Run's exact publication terminal.
    repo.auto_merge = False
    review.status = "waiting_ci"
    context = {
        "base_ref": "main",
        "guidance": {"CLAUDE.md": None, "PROGRESS.md": None},
        "material": {
            "number": review.pr_number,
            "title": review.pr_title,
            "body": "",
            "author": review.pr_author,
            "base_ref": "main",
            "head_ref": "feature",
            "files": [],
            "patch": "",
            "changed_file_contents": [],
        },
    }

    await pr_review_panel._add_panel_tasks(
        db_session,
        repo=repo,
        review=review,
        context=context,
    )

    reviewer_tasks = list(
        (
            await db_session.execute(
                select(Task).where(Task.tags.contains(["pr-review"]))
            )
        ).scalars()
    )
    assert len(reviewer_tasks) == len(pr_review_panel.REVIEWER_ROLES)
    assert all(
        task.metadata_.get("pr_auto_merge") is True
        for task in reviewer_tasks
    )
