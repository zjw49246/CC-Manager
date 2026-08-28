from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

from backend.models.pr_monitor import (
    MonitoredRepo,
    PRMergeQueueAction,
    PRMonitorRun,
    PRReview,
)
from backend.services.pr_review_service import GhError
from backend.services.pr_direct_merge import reconcile_direct_merge_action


BASE_SHA = "a" * 40
HEAD_SHA = "b" * 40


async def _seed_direct_action(db):
    repo = MonitoredRepo(
        repo_full_name="owner/direct-merge",
        webhook_secret="s" * 64,
        review_mode="panel",
        merge_queue_mode="manual",
        enabled=True,
        wait_for_ci=False,
        required_checks=[],
    )
    db.add(repo)
    await db.flush()
    run = PRMonitorRun(
        repo_id=repo.id,
        pr_number=7,
        current_base_sha=BASE_SHA,
        current_head_sha=HEAD_SHA,
        status="merge_pending",
    )
    db.add(run)
    await db.flush()
    review = PRReview(
        monitor_run_id=run.id,
        repo_id=repo.id,
        pr_number=7,
        base_ref="main",
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        pr_title="Direct merge",
        pr_author="alice",
        pr_url="https://github.com/owner/direct-merge/pull/7",
        status="approved",
        action_taken="lgtm_comment",
        publication_state="published",
    )
    db.add(review)
    await db.flush()
    run.current_review_id = review.id
    action = PRMergeQueueAction(
        monitor_run_id=run.id,
        review_id=review.id,
        trigger_base_sha=BASE_SHA,
        trigger_head_sha=HEAD_SHA,
        status="pending",
        effect_kind="direct",
        trigger_kind="manual",
        action_nonce="n" * 48,
        publishing_actor="ccm-bot",
        publishing_started_at=datetime(2026, 8, 27),
        merge_method="fast-forward",
        wait_for_ci=False,
        required_checks=[],
    )
    db.add(action)
    await db.commit()
    return repo, run, review, action


@pytest.mark.asyncio
async def test_direct_merge_success_closes_action_and_run(
    db_session, db_factory,
):
    _repo, run, _review, action = await _seed_direct_action(db_session)

    with (
        patch(
            "backend.services.pr_direct_merge._remote_state",
            new=AsyncMock(return_value="open_exact"),
        ),
        patch(
            "backend.services.pr_review_service._publish_direct_merge",
            new=AsyncMock(return_value=("merged", "approved_merged")),
        ),
    ):
        assert await reconcile_direct_merge_action(db_factory, action.id)

    refreshed_action = await db_session.get(
        PRMergeQueueAction, action.id, populate_existing=True
    )
    refreshed_run = await db_session.get(
        PRMonitorRun, run.id, populate_existing=True
    )
    assert refreshed_action is not None
    assert refreshed_run is not None
    assert refreshed_action.effect_kind == "direct"
    assert refreshed_action.status == "merged"
    assert refreshed_action.lease_token is None
    assert refreshed_run.status == "merged"


@pytest.mark.asyncio
async def test_direct_merge_ci_failure_is_terminal_and_explains_gate(
    db_session, db_factory,
):
    _repo, run, _review, action = await _seed_direct_action(db_session)

    with (
        patch(
            "backend.services.pr_direct_merge._remote_state",
            new=AsyncMock(side_effect=["open_exact", "open_exact"]),
        ),
        patch(
            "backend.services.pr_review_service._publish_direct_merge",
            new=AsyncMock(
                side_effect=GhError(
                    "Exact-head required CI is not passed before merge: failed"
                )
            ),
        ),
    ):
        assert not await reconcile_direct_merge_action(db_factory, action.id)

    refreshed_action = await db_session.get(
        PRMergeQueueAction, action.id, populate_existing=True
    )
    refreshed_run = await db_session.get(
        PRMonitorRun, run.id, populate_existing=True
    )
    assert refreshed_action is not None
    assert refreshed_run is not None
    assert refreshed_action.status == "failed"
    assert refreshed_action.last_error.startswith(
        "direct_merge_remote_absence_proven:"
    )
    assert refreshed_run.status == "ready_to_merge"


@pytest.mark.asyncio
async def test_direct_merge_unknown_remote_error_stays_recoverable(
    db_session, db_factory,
):
    _repo, run, _review, action = await _seed_direct_action(db_session)

    with (
        patch(
            "backend.services.pr_direct_merge._remote_state",
            new=AsyncMock(side_effect=["open_exact", "unknown"]),
        ),
        patch(
            "backend.services.pr_review_service._publish_direct_merge",
            new=AsyncMock(side_effect=GhError("temporary GitHub timeout")),
        ),
    ):
        assert not await reconcile_direct_merge_action(db_factory, action.id)

    refreshed_action = await db_session.get(
        PRMergeQueueAction, action.id, populate_existing=True
    )
    refreshed_run = await db_session.get(
        PRMonitorRun, run.id, populate_existing=True
    )
    assert refreshed_action is not None
    assert refreshed_run is not None
    assert refreshed_action.status == "enqueuing"
    assert refreshed_action.lease_token is None
    assert refreshed_action.completed_at is None
    assert refreshed_action.last_error.startswith(
        "direct_merge_reconcile_pending:"
    )
    assert refreshed_run.status == "merge_pending"


@pytest.mark.asyncio
async def test_exact_merged_terminal_intent_recovers_uncertain_direct_action(
    db_session, db_factory,
):
    _repo, run, _review, action = await _seed_direct_action(db_session)

    with (
        patch(
            "backend.services.pr_direct_merge._remote_state",
            new=AsyncMock(side_effect=["open_exact", "unknown"]),
        ),
        patch(
            "backend.services.pr_review_service._publish_direct_merge",
            new=AsyncMock(side_effect=GhError("temporary GitHub timeout")),
        ),
    ):
        assert not await reconcile_direct_merge_action(db_factory, action.id)

    run.terminal_intent_status = "merged"
    run.terminal_intent_base_ref = "main"
    run.terminal_intent_head_sha = HEAD_SHA
    run.terminal_intent_delivery_id = "merged-after-direct-timeout"
    run.terminal_intent_observed_at = datetime.utcnow()
    await db_session.commit()

    remote_state = AsyncMock()
    publish = AsyncMock()
    with (
        patch(
            "backend.services.pr_direct_merge._remote_state",
            new=remote_state,
        ),
        patch(
            "backend.services.pr_review_service._publish_direct_merge",
            new=publish,
        ),
    ):
        assert await reconcile_direct_merge_action(db_factory, action.id)

    remote_state.assert_not_awaited()
    publish.assert_not_awaited()
    refreshed_action = await db_session.get(
        PRMergeQueueAction, action.id, populate_existing=True
    )
    refreshed_run = await db_session.get(
        PRMonitorRun, run.id, populate_existing=True
    )
    assert refreshed_action is not None
    assert refreshed_run is not None
    assert refreshed_action.status == "merged"
    assert refreshed_action.last_error is None
    assert refreshed_action.completed_at is not None
    assert refreshed_action.lease_token is None
    assert refreshed_run.status == "merge_pending"
    assert refreshed_run.completed_at is None


@pytest.mark.asyncio
async def test_mismatched_terminal_intent_does_not_release_direct_action(
    db_session, db_factory,
):
    _repo, run, _review, action = await _seed_direct_action(db_session)
    action.status = "enqueuing"
    action.attempt_count = 1
    action.last_error = "direct_merge_reconcile_pending:GhError:timeout"
    run.terminal_intent_status = "merged"
    run.terminal_intent_base_ref = "main"
    run.terminal_intent_head_sha = "c" * 40
    run.terminal_intent_delivery_id = "different-head-merged"
    run.terminal_intent_observed_at = datetime.utcnow()
    await db_session.commit()

    remote_state = AsyncMock()
    publish = AsyncMock()
    with (
        patch(
            "backend.services.pr_direct_merge._remote_state",
            new=remote_state,
        ),
        patch(
            "backend.services.pr_review_service._publish_direct_merge",
            new=publish,
        ),
    ):
        assert not await reconcile_direct_merge_action(db_factory, action.id)

    remote_state.assert_not_awaited()
    publish.assert_not_awaited()
    refreshed_action = await db_session.get(
        PRMergeQueueAction, action.id, populate_existing=True
    )
    assert refreshed_action is not None
    assert refreshed_action.status == "enqueuing"
    assert refreshed_action.completed_at is None
