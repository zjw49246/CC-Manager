"""Fail-closed boundaries between Delivery and legacy PR Monitor effects."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from backend.models.delivery import DeliveryRun
from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRFindingAction,
    PRMonitorRun,
    PRRepairWake,
    PRReview,
    PRReviewerRun,
)
from backend.models.project import Project
from backend.models.task import Task
from backend.services.delivery_pr_policy import legacy_pr_effect_is_forbidden
from backend.services.delivery_service import value_hash
from backend.services.pr_monitor_loop import (
    admit_repair_wake,
    record_blocking_evidence,
)
from backend.services.pr_review_actions import FindingActionConflict
from backend.services.pr_review_fix import (
    FixConfirmationError,
    confirm_fix,
    create_fix_task,
)


BASE_SHA = "1" * 40
HEAD_SHA = "a" * 40


async def _seed_marker_owned_finding(session_factory, *, suffix: str) -> dict:
    patch = (
        "diff --git a/backend/example.py b/backend/example.py\n"
        "--- a/backend/example.py\n"
        "+++ b/backend/example.py\n"
        "@@ -1 +1 @@\n"
        "-raise RuntimeError()\n"
        "+return default_value\n"
    )
    patch_sha = hashlib.sha256(patch.encode()).hexdigest()
    async with session_factory() as db:
        project = Project(name=f"delivery-effect-{suffix}")
        db.add(project)
        await db.flush()
        repo = MonitoredRepo(
            repo_full_name=f"owner/delivery-effect-{suffix}",
            project_id=project.id,
            webhook_secret="s" * 64,
            review_mode="panel",
            auto_repair=True,
            merge_queue_mode="auto",
        )
        developer = Task(
            title="Legacy developer",
            description="Must not receive a legacy repair",
            status="completed",
            project_id=project.id,
            result_branch="feature/delivery-effect",
            session_id=f"session-{suffix}",
            last_cwd=f"/workspace/{suffix}",
            started_at=datetime.utcnow() - timedelta(minutes=2),
            completed_at=datetime.utcnow() - timedelta(minutes=1),
        )
        db.add_all((repo, developer))
        await db.flush()
        monitor = PRMonitorRun(
            repo_id=repo.id,
            pr_number=17,
            status="waiting_for_fix",
            current_base_sha=BASE_SHA,
            current_head_sha=HEAD_SHA,
            developer_task_id=developer.id,
            head_repo_full_name=repo.repo_full_name,
            head_branch=developer.result_branch,
        )
        db.add(monitor)
        await db.flush()
        review = PRReview(
            monitor_run_id=monitor.id,
            repo_id=repo.id,
            pr_number=17,
            base_ref="main",
            base_sha=BASE_SHA,
            head_sha=HEAD_SHA,
            # Corrupt reserved markers still assert restricted ownership.
            delivery_id="delivery:not-a-valid-owner",
            pr_title="Delivery-owned finding",
            pr_author="agent",
            pr_url=f"https://github.com/{repo.repo_full_name}/pull/17",
            status="commented",
        )
        db.add(review)
        await db.flush()
        monitor.current_review_id = review.id
        reviewer = PRReviewerRun(
            pr_review_id=review.id,
            role="senior_engineer",
            provider="codex",
            status="completed",
            prompt_policy_hash="p" * 64,
            guide_pack_hash="g" * 64,
        )
        db.add(reviewer)
        await db.flush()
        finding = PRFinding(
            pr_review_id=review.id,
            reviewer_run_id=reviewer.id,
            fingerprint="f" * 64,
            role=reviewer.role,
            severity="high",
            category="correctness",
            path="backend/example.py",
            line=1,
            title="Broken fallback",
            evidence="The fallback raises instead of returning.",
            impact="Requests fail.",
            required_fix="Return the fallback value.",
            test="Exercise the fallback.",
            thread_nonce="n" * 48,
            base_sha=BASE_SHA,
            head_sha=HEAD_SHA,
        )
        db.add(finding)
        await db.flush()
        action = PRFindingAction(
            finding_id=finding.id,
            action_type="ai_fix",
            status="awaiting_confirmation",
            idempotency_key=f"delivery-effect-{suffix}",
            expected_head_sha=HEAD_SHA,
            active_fix_finding_id=finding.id,
            patch_sha256=patch_sha,
            result={
                "patch": patch,
                "confirmation_token": "signed-confirmation-token",
                "confirmation_expires_at": 4102444800,
                "action_nonce": f"nonce-{suffix}",
                "allowed_files": [finding.path],
                "head_repo_full_name": repo.repo_full_name,
                "head_ref": developer.result_branch,
            },
        )
        wake = PRRepairWake(
            monitor_run_id=monitor.id,
            review_id=review.id,
            developer_task_id=developer.id,
            trigger_base_sha=BASE_SHA,
            trigger_head_sha=HEAD_SHA,
            reason_kind="review_blocked",
            evidence_hash="e" * 64,
            evidence={"findings": [finding.fingerprint]},
            status="delivering",
            delivery_token="d" * 48,
        )
        db.add_all((action, wake))
        await db.commit()
        return {
            "repo_id": repo.id,
            "task_id": developer.id,
            "run_id": monitor.id,
            "review_id": review.id,
            "finding_id": finding.id,
            "action_id": action.id,
            "wake_id": wake.id,
            "patch_sha": patch_sha,
        }


@pytest.mark.asyncio
async def test_reserved_marker_and_delivery_run_association_both_block_legacy_effects(
    db_session,
):
    project = Project(name="delivery-effect-policy")
    db_session.add(project)
    await db_session.flush()
    repo = MonitoredRepo(
        repo_full_name="owner/delivery-effect-policy",
        project_id=project.id,
        webhook_secret="s" * 64,
        review_mode="panel",
        auto_repair=True,
    )
    db_session.add(repo)
    await db_session.flush()
    developer = Task(
        title="Association-only developer",
        description="Legacy task associated through the monitor",
        project_id=project.id,
        status="completed",
        session_id="association-only-session",
        last_cwd="/workspace/association-only",
    )
    db_session.add(developer)
    await db_session.flush()
    monitor = PRMonitorRun(
        repo_id=repo.id,
        pr_number=23,
        status="reviewing",
        current_base_sha=BASE_SHA,
        current_head_sha=HEAD_SHA,
        developer_task_id=developer.id,
    )
    db_session.add(monitor)
    await db_session.flush()
    malformed = PRReview(
        monitor_run_id=monitor.id,
        repo_id=repo.id,
        pr_number=23,
        base_ref="main",
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        delivery_id="delivery:corrupt",
        pr_title="Malformed marker",
        pr_author="agent",
        pr_url="https://github.com/owner/delivery-effect-policy/pull/23",
        status="commented",
    )
    db_session.add(malformed)
    await db_session.flush()
    monitor.current_review_id = malformed.id
    assert await legacy_pr_effect_is_forbidden(
        db_session,
        review=malformed,
        monitor_run=monitor,
    )
    malformed.delivery_id = "opaque-github-delivery"
    policy = {
        "schema_version": 1,
        "terminal": "ready_to_merge",
        "auto_merge": False,
        "pr_monitor": {"repo_id": repo.id},
    }
    delivery = DeliveryRun(
        admission_scope="anonymous",
        idempotency_key="delivery-effect-policy",
        request_hash="r" * 64,
        project_id=project.id,
        monitored_repo_id=repo.id,
        pr_monitor_run_id=monitor.id,
        title="Delivery effect policy",
        requirements="Keep legacy effects isolated",
        requirements_hash="q" * 64,
        policy_snapshot=policy,
        policy_hash=value_hash(policy),
        base_branch="main",
        delivery_branch="ccm/delivery/effect-policy",
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        pr_number=23,
        pr_url=malformed.pr_url,
        phase="monitoring",
        activity="waiting",
        wait_reason="pr_monitor",
    )
    db_session.add(delivery)
    await db_session.commit()
    assert await legacy_pr_effect_is_forbidden(
        db_session,
        review=malformed,
        monitor_run=monitor,
    )
    wake = await record_blocking_evidence(
        db_session,
        review_id=malformed.id,
        reason_kind="review_blocked",
    )
    # A non-structured single-review failure has no actionable Finding or
    # trusted exact-head CI evidence.  Delivery ownership still blocks every
    # legacy effect, but there is no reason to materialize an empty shadow
    # Repair instruction merely for audit; the public Result/Run is the audit.
    assert wake is None
    refreshed_monitor = await db_session.get(
        PRMonitorRun,
        monitor.id,
        populate_existing=True,
    )
    assert refreshed_monitor is not None
    assert refreshed_monitor.status == "waiting_for_fix"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("route", "payload"),
    (
        ("pause", None),
        ("resume", None),
        ("enqueue-merge", None),
        ("bind-developer", "task"),
        ("unbind-developer", None),
    ),
)
async def test_delivery_monitor_rejects_legacy_run_mutations(
    client,
    session_factory,
    route,
    payload,
):
    ids = await _seed_marker_owned_finding(
        session_factory,
        suffix=f"run-{route}",
    )
    body = {"task_id": ids["task_id"]} if payload == "task" else None

    remote_read = AsyncMock(
        side_effect=AssertionError(
            "Delivery-owned bind must fail before a GitHub PR read"
        )
    )
    with patch(
        "backend.services.pr_review_service._gh_pr_view",
        remote_read,
    ):
        response = await client.post(
            f"/api/pr-monitor/runs/{ids['run_id']}/{route}",
            json=body,
        )

    assert response.status_code == 409, response.text
    assert "Delivery-owned PR state" in response.json()["detail"]
    if route == "bind-developer":
        remote_read.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "action",
    ("ignore", "advice", "fix", "confirm", "rebut"),
)
async def test_delivery_finding_rejects_legacy_effects(
    client,
    session_factory,
    action,
):
    ids = await _seed_marker_owned_finding(
        session_factory,
        suffix=f"finding-{action}",
    )
    if action == "ignore":
        route = f"/api/pr-monitor/findings/{ids['finding_id']}/ignore"
        body = {"idempotency_key": "delivery-ignore-rejected"}
    elif action == "advice":
        route = f"/api/pr-monitor/findings/{ids['finding_id']}/advice"
        body = {
            "idempotency_key": "delivery-advice-rejected",
            "advice": "Do not mutate Delivery findings through legacy APIs.",
        }
    elif action == "fix":
        route = f"/api/pr-monitor/findings/{ids['finding_id']}/fix"
        body = {"idempotency_key": "delivery-fix-rejected"}
    elif action == "confirm":
        route = f"/api/pr-monitor/actions/{ids['action_id']}/confirm"
        body = {
            "confirmation_token": "signed-confirmation-token",
            "patch_sha256": ids["patch_sha"],
            "download_receipt": "r" * 32,
        }
    else:
        route = f"/api/pr-monitor/findings/{ids['finding_id']}/rebut"
        body = {"evidence": "Independent evidence proving this is expected."}

    response = await client.post(route, json=body)

    assert response.status_code == 409, response.text
    assert "Delivery-owned PR" in response.json()["detail"]


@pytest.mark.asyncio
async def test_delivery_finding_reads_and_unconfirmed_cancel_remain_available(
    client,
    session_factory,
):
    ids = await _seed_marker_owned_finding(
        session_factory,
        suffix="read-cancel",
    )

    read = await client.get(f"/api/pr-monitor/actions/{ids['action_id']}")
    diff = await client.get(
        f"/api/pr-monitor/actions/{ids['action_id']}/diff"
    )
    cancelled = await client.post(
        f"/api/pr-monitor/actions/{ids['action_id']}/cancel"
    )

    assert read.status_code == 200, read.text
    assert diff.status_code == 200, diff.text
    assert cancelled.status_code == 200, cancelled.text
    assert cancelled.json()["status"] == "cancelled"


@pytest.mark.asyncio
async def test_fix_service_rechecks_delivery_adoption_before_return_or_push(
    db_session,
):
    project = Project(name="delivery-fix-service")
    repo = MonitoredRepo(
        repo_full_name="owner/delivery-fix-service",
        webhook_secret="s" * 64,
        review_mode="panel",
    )
    db_session.add_all((project, repo))
    await db_session.flush()
    review = PRReview(
        repo_id=repo.id,
        pr_number=31,
        base_ref="main",
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        delivery_id="delivery:adopted-after-action",
        pr_title="Adopted action",
        pr_author="agent",
        pr_url="https://github.com/owner/delivery-fix-service/pull/31",
        status="commented",
    )
    db_session.add(review)
    await db_session.flush()
    reviewer = PRReviewerRun(
        pr_review_id=review.id,
        role="senior_engineer",
        provider="codex",
        status="completed",
        prompt_policy_hash="p" * 64,
        guide_pack_hash="g" * 64,
    )
    db_session.add(reviewer)
    await db_session.flush()
    finding = PRFinding(
        pr_review_id=review.id,
        reviewer_run_id=reviewer.id,
        fingerprint="c" * 64,
        role=reviewer.role,
        severity="high",
        category="correctness",
        path="backend/example.py",
        title="Adopted finding",
        evidence="Bad behavior",
        impact="Failure",
        required_fix="Fix it",
        test="Test it",
        thread_nonce="t" * 48,
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
    )
    db_session.add(finding)
    await db_session.flush()
    action = PRFindingAction(
        finding_id=finding.id,
        action_type="ai_fix",
        status="awaiting_confirmation",
        idempotency_key="adopted-existing-fix",
        expected_head_sha=HEAD_SHA,
        active_fix_finding_id=finding.id,
        patch_sha256="d" * 64,
        result={"patch": "x", "action_nonce": "nonce"},
    )
    db_session.add(action)
    await db_session.commit()
    action_id = action.id
    finding_id = finding.id
    review_id = review.id
    repo_id = repo.id
    idempotency_key = action.idempotency_key

    with pytest.raises(FindingActionConflict, match="Delivery-owned"):
        await create_fix_task(
            db_session,
            finding_id=finding_id,
            review_id=review_id,
            repo_id=repo_id,
            idempotency_key=idempotency_key,
            actor_user_id=None,
        )
    await db_session.rollback()
    with pytest.raises(FixConfirmationError, match="Delivery-owned"):
        await confirm_fix(
            db_session,
            action_id=action_id,
            confirmation_token="invalid-but-not-reached",
            patch_sha256="d" * 64,
            download_receipt="r" * 32,
            confirmed_by_user_id=None,
        )


@pytest.mark.asyncio
async def test_delivery_review_cannot_admit_preexisting_legacy_repair_wake(
    session_factory,
):
    ids = await _seed_marker_owned_finding(
        session_factory,
        suffix="admit-wake",
    )
    async with session_factory() as db:
        task = await db.get(Task, ids["task_id"])
        assert task is not None
        admitted = await admit_repair_wake(
            db,
            wake_id=ids["wake_id"],
            delivery_token="d" * 48,
            task=task,
        )
        wake = await db.get(
            PRRepairWake,
            ids["wake_id"],
            populate_existing=True,
        )

    assert admitted is False
    assert wake is not None
    assert wake.status == "delivering"
