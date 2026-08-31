"""Durable manual direct merge controller for one exact reviewed PR head."""

from __future__ import annotations

import logging
import secrets
from datetime import datetime, timedelta

from sqlalchemy import func, or_, select, update

from backend.models.pr_monitor import (
    MonitoredRepo,
    PRFinding,
    PRMergeQueueAction,
    PRMonitorRun,
    PRReview,
    pr_monitor_run_has_terminal_intent,
)
from backend.services.delivery_pr_policy import legacy_pr_effect_is_forbidden


logger = logging.getLogger(__name__)
_MAX_DIRECT_ATTEMPTS = 3


async def _database_now(db) -> datetime:
    value = (await db.execute(select(func.current_timestamp()))).scalar_one()
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise ValueError("Database clock returned an invalid timestamp")
    return value.replace(tzinfo=None)


async def _lock_action_rows(db, action_id: int):
    preliminary = await db.get(PRMergeQueueAction, action_id)
    preliminary_run = (
        await db.get(PRMonitorRun, preliminary.monitor_run_id)
        if preliminary is not None
        else None
    )
    if preliminary is None or preliminary_run is None:
        await db.rollback()
        return None, None, None, None
    repo_id = preliminary_run.repo_id
    run_id = preliminary_run.id
    review_id = preliminary.review_id
    await db.rollback()
    guarded = await db.execute(
        update(MonitoredRepo)
        .where(MonitoredRepo.id == repo_id)
        .values(updated_at=MonitoredRepo.updated_at)
        .execution_options(synchronize_session=False)
    )
    if guarded.rowcount != 1:
        await db.rollback()
        return None, None, None, None
    repo = (
        await db.execute(
            select(MonitoredRepo)
            .where(MonitoredRepo.id == repo_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    run = (
        await db.execute(
            select(PRMonitorRun)
            .where(PRMonitorRun.id == run_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    review = (
        await db.execute(
            select(PRReview)
            .where(PRReview.id == review_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    action = (
        await db.execute(
            select(PRMergeQueueAction)
            .where(PRMergeQueueAction.id == action_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    return repo, run, review, action


async def _has_blocking_findings(db, run_id: int) -> bool:
    blocker = await db.scalar(
        select(PRFinding.id)
        .join(PRReview, PRReview.id == PRFinding.pr_review_id)
        .where(
            PRReview.monitor_run_id == run_id,
            PRFinding.severity.in_(("critical", "high", "medium")),
            or_(
                PRFinding.status == "open",
                PRFinding.thread_status != "resolved",
            ),
        )
        .limit(1)
    )
    return blocker is not None


def _direct_merge_has_exact_merged_intent(
    *,
    run: PRMonitorRun,
    review: PRReview,
    action: PRMergeQueueAction,
) -> bool:
    """Return whether a durable merged intent resolves this exact outbox.

    A merged GitHub lifecycle is immutable, so an exact-head terminal intent
    can safely finish a direct action without replaying its remote write.  Keep
    every identity edge in this predicate: partial intents and actions for an
    older Review must continue to fail closed.
    """

    return bool(
        action.effect_kind == "direct"
        and action.monitor_run_id == run.id
        and action.review_id == review.id
        and run.current_review_id == review.id
        and run.current_base_sha == action.trigger_base_sha
        and run.current_head_sha == action.trigger_head_sha
        and review.base_sha == action.trigger_base_sha
        and review.head_sha == action.trigger_head_sha
        and run.terminal_intent_status == "merged"
        and run.terminal_intent_base_ref == review.base_ref
        and run.terminal_intent_head_sha == action.trigger_head_sha
        and isinstance(run.terminal_intent_observed_at, datetime)
    )


async def _action_is_current(
    db_factory,
    *,
    action_id: int,
    lease_token: str,
) -> bool:
    async with db_factory() as db:
        repo, run, review, action = await _lock_action_rows(db, action_id)
        try:
            if (
                repo is None
                or run is None
                or review is None
                or action is None
                or not repo.enabled
                or action.effect_kind != "direct"
                or action.status != "enqueuing"
                or action.lease_token != lease_token
                or action.lease_expires_at is None
                or action.lease_expires_at <= await _database_now(db)
                or action.monitor_run_id != run.id
                or action.review_id != review.id
                or run.repo_id != repo.id
                or run.current_review_id != review.id
                or run.current_base_sha != action.trigger_base_sha
                or run.current_head_sha != action.trigger_head_sha
                or review.base_sha != action.trigger_base_sha
                or review.head_sha != action.trigger_head_sha
                or run.status != "merge_pending"
                or run.completed_at is not None
                or pr_monitor_run_has_terminal_intent(run)
                or review.status not in {"approved", "commented"}
                or review.action_taken != "lgtm_comment"
                or review.publication_state != "published"
                or await legacy_pr_effect_is_forbidden(
                    db,
                    review=review,
                    monitor_run=run,
                )
                or await _has_blocking_findings(db, run.id)
            ):
                return False
            return True
        finally:
            await db.rollback()


async def _remote_state(
    *,
    repo_name: str,
    pr_number: int,
    base_ref: str,
    base_sha: str,
    head_sha: str,
    nonce: str,
    actor: str,
    publishing_started_at: datetime,
    merge_method: str,
) -> str:
    from backend.services.pr_review_service import (
        GhError,
        _find_merge_evidence,
        _gh_pr_view,
        _validated_pr_snapshot,
    )

    snapshot = _validated_pr_snapshot(await _gh_pr_view(pr_number, repo_name))
    if snapshot["state"] == "MERGED" and snapshot["merged_at"] is not None:
        if await _find_merge_evidence(
            repo_name=repo_name,
            pr_number=pr_number,
            base_ref=base_ref,
            base_sha=base_sha,
            head_sha=head_sha,
            nonce=nonce,
            actor=actor,
            publishing_started_at=publishing_started_at,
            merge_method=merge_method,
        ):
            return "merged"
        raise GhError("GitHub did not confirm the manual direct merge evidence")
    if snapshot["state"] == "CLOSED" and snapshot["merged_at"] is None:
        return "closed"
    if snapshot["state"] == "OPEN" and snapshot["merged_at"] is None:
        if (
            snapshot["base_ref"] == base_ref
            and snapshot["head_sha"] == head_sha
        ):
            # The target branch may advance after Review publication.  That
            # does not change the PR subject by itself: the publisher's
            # captured-base ancestry check decides whether the frozen head can
            # still fast-forward the exact target ref safely.
            return "open_exact"
        return "open_changed"
    return "unknown"


async def _settle_remote_terminal(
    db_factory,
    *,
    action_id: int,
    lease_token: str,
    remote_state: str,
) -> bool:
    if remote_state not in {"merged", "closed"}:
        return False
    async with db_factory() as db:
        _repo, run, review, action = await _lock_action_rows(db, action_id)
        if (
            run is None
            or review is None
            or action is None
            or action.effect_kind != "direct"
            or action.status != "enqueuing"
            or action.lease_token != lease_token
            or action.monitor_run_id != run.id
            or action.review_id != review.id
            or action.trigger_base_sha != run.current_base_sha
            or action.trigger_head_sha != run.current_head_sha
        ):
            await db.rollback()
            return False
        now = datetime.utcnow()
        action.status = "merged" if remote_state == "merged" else "superseded"
        action.last_error = None if remote_state == "merged" else "pr_closed"
        action.completed_at = now
        action.lease_token = None
        action.lease_expires_at = None
        if not pr_monitor_run_has_terminal_intent(run):
            run.status = "merged" if remote_state == "merged" else "closed"
            run.pause_reason = None
            run.completed_at = now
            run.state_version += 1
        await db.commit()
        return True


async def _record_failure(
    db_factory,
    *,
    action_id: int,
    lease_token: str,
    message: str,
    remote_state: str,
    terminal: bool,
) -> None:
    async with db_factory() as db:
        _repo, run, review, action = await _lock_action_rows(db, action_id)
        if (
            run is None
            or review is None
            or action is None
            or action.effect_kind != "direct"
            or action.status != "enqueuing"
            or action.lease_token != lease_token
            or action.monitor_run_id != run.id
            or action.review_id != review.id
        ):
            await db.rollback()
            return
        action.lease_token = None
        action.lease_expires_at = None
        proven_absent = remote_state in {"open_exact", "open_changed", "closed"}
        should_stop = terminal or action.attempt_count >= _MAX_DIRECT_ATTEMPTS
        if proven_absent and should_stop:
            action.status = "failed" if remote_state != "closed" else "superseded"
            action.last_error = (
                f"direct_merge_remote_absence_proven:{message}"[:1000]
            )
            action.completed_at = datetime.utcnow()
            if not pr_monitor_run_has_terminal_intent(run):
                base_update_required = (
                    remote_state == "open_exact"
                    and "GitHub PR base ancestry is unsafe" in message
                )
                if base_update_required:
                    run.status = "paused"
                    run.pause_reason = "direct_merge_base_update_required"
                elif remote_state == "open_exact":
                    run.status = "ready_to_merge"
                    run.pause_reason = action.last_error
                else:
                    run.status = "paused"
                    run.pause_reason = "direct_merge_subject_changed"
                run.state_version += 1
        else:
            # Keep the active status: the remote response may have been lost,
            # so the next pass must reconcile exact merge evidence first.
            action.last_error = f"direct_merge_reconcile_pending:{message}"[:1000]
        await db.commit()


async def reconcile_direct_merge_action(db_factory, action_id: int) -> bool:
    """Reconcile or execute one exact manual direct merge action."""

    from backend.services.pr_review_service import (
        GhError,
        _publish_direct_merge,
        _terminal_publication_error,
    )

    async with db_factory() as db:
        repo, run, review, action = await _lock_action_rows(db, action_id)
        exact_merged_intent = bool(
            run is not None
            and review is not None
            and action is not None
            and _direct_merge_has_exact_merged_intent(
                run=run,
                review=review,
                action=action,
            )
        )
        if (
            repo is None
            or run is None
            or review is None
            or action is None
            or action.effect_kind != "direct"
            or action.status not in {"pending", "enqueuing"}
            or not repo.enabled
            or action.monitor_run_id != run.id
            or action.review_id != review.id
            or run.repo_id != repo.id
            or run.current_review_id != review.id
            or run.current_base_sha != action.trigger_base_sha
            or run.current_head_sha != action.trigger_head_sha
            or review.base_sha != action.trigger_base_sha
            or review.head_sha != action.trigger_head_sha
            or run.status != "merge_pending"
            or run.completed_at is not None
            or (
                pr_monitor_run_has_terminal_intent(run)
                and not exact_merged_intent
            )
            or review.status not in {"approved", "commented"}
            or review.action_taken != "lgtm_comment"
            or review.publication_state != "published"
            or not isinstance(action.publishing_actor, str)
            or not action.publishing_actor
            or not isinstance(action.publishing_started_at, datetime)
            or action.merge_method != "fast-forward"
            or type(action.wait_for_ci) is not bool
            or not isinstance(action.required_checks, list)
            or await legacy_pr_effect_is_forbidden(
                db,
                review=review,
                monitor_run=run,
            )
            or await _has_blocking_findings(db, run.id)
        ):
            await db.rollback()
            return False
        now = await _database_now(db)
        lease_token = secrets.token_hex(24)
        claimed = await db.execute(
            update(PRMergeQueueAction)
            .where(
                PRMergeQueueAction.id == action.id,
                PRMergeQueueAction.effect_kind == "direct",
                PRMergeQueueAction.status.in_(("pending", "enqueuing")),
                or_(
                    PRMergeQueueAction.lease_token.is_(None),
                    PRMergeQueueAction.lease_expires_at <= now,
                ),
            )
            .values(
                status="enqueuing",
                attempt_count=PRMergeQueueAction.attempt_count + 1,
                lease_token=lease_token,
                lease_expires_at=now + timedelta(minutes=10),
            )
            .execution_options(synchronize_session=False)
        )
        if claimed.rowcount != 1:
            await db.rollback()
            return False
        action_id = action.id
        repo_name = repo.repo_full_name
        pr_number = run.pr_number
        base_ref = review.base_ref
        base_sha = action.trigger_base_sha
        head_sha = action.trigger_head_sha
        nonce = action.action_nonce
        actor = action.publishing_actor
        publishing_started_at = action.publishing_started_at
        merge_method = action.merge_method
        wait_for_ci = action.wait_for_ci
        required_checks = list(action.required_checks)
        await db.commit()

    assert isinstance(actor, str)
    assert isinstance(publishing_started_at, datetime)
    assert isinstance(merge_method, str)

    if exact_merged_intent:
        # The terminal intent already identifies the immutable merged head.
        # Claiming the expired/unowned action above prevents racing a live
        # publisher; settle it locally and never replay a GitHub mutation.
        return await _settle_remote_terminal(
            db_factory,
            action_id=action_id,
            lease_token=lease_token,
            remote_state="merged",
        )

    async def ensure_current() -> bool:
        return await _action_is_current(
            db_factory,
            action_id=action_id,
            lease_token=lease_token,
        )

    try:
        remote_state = await _remote_state(
            repo_name=repo_name,
            pr_number=pr_number,
            base_ref=base_ref,
            base_sha=base_sha,
            head_sha=head_sha,
            nonce=nonce,
            actor=actor,
            publishing_started_at=publishing_started_at,
            merge_method=merge_method,
        )
        if await _settle_remote_terminal(
            db_factory,
            action_id=action_id,
            lease_token=lease_token,
            remote_state=remote_state,
        ):
            return True
        if not await ensure_current():
            raise GhError("Manual direct merge generation is no longer current")
        status, _action_taken = await _publish_direct_merge(
            repo_name=repo_name,
            pr_number=pr_number,
            base_ref=base_ref,
            base_sha=base_sha,
            head_sha=head_sha,
            merge_method=merge_method,
            nonce=nonce,
            actor=actor,
            publishing_started_at=publishing_started_at,
            ensure_current=ensure_current,
            wait_for_ci=wait_for_ci,
            required_checks=required_checks,
            ensure_zero_threads=ensure_current,
            # A human click is the authorization boundary. Repositories with
            # classic protection still enforce it at the non-force ref write;
            # unprotected personal repositories require only exact collaborator
            # push permission here.
            strict_branch_protection=False,
        )
        if status != "merged":
            raise GhError("GitHub did not confirm the manual direct merge")
        remote_state = "merged"
    except GhError as exc:
        try:
            remote_state = await _remote_state(
                repo_name=repo_name,
                pr_number=pr_number,
                base_ref=base_ref,
                base_sha=base_sha,
                head_sha=head_sha,
                nonce=nonce,
                actor=actor,
                publishing_started_at=publishing_started_at,
                merge_method=merge_method,
            )
        except Exception:
            remote_state = "unknown"
        if await _settle_remote_terminal(
            db_factory,
            action_id=action_id,
            lease_token=lease_token,
            remote_state=remote_state,
        ):
            return True
        await _record_failure(
            db_factory,
            action_id=action_id,
            lease_token=lease_token,
            message=f"{type(exc).__name__}:{str(exc)[:700]}",
            remote_state=remote_state,
            terminal=_terminal_publication_error(exc),
        )
        return False

    return await _settle_remote_terminal(
        db_factory,
        action_id=action_id,
        lease_token=lease_token,
        remote_state=remote_state,
    )


async def reconcile_direct_merges(db_factory) -> int:
    async with db_factory() as db:
        action_ids = list(
            (
                await db.execute(
                    select(PRMergeQueueAction.id)
                    .where(
                        PRMergeQueueAction.effect_kind == "direct",
                        PRMergeQueueAction.status.in_(("pending", "enqueuing")),
                    )
                    .order_by(PRMergeQueueAction.id)
                )
            ).scalars()
        )
    progressed = 0
    for action_id in action_ids:
        try:
            progressed += int(
                await reconcile_direct_merge_action(db_factory, action_id)
            )
        except Exception:
            logger.exception("Direct PR merge recovery failed for action %s", action_id)
    return progressed
