"""Durable GitHub Merge Queue controller for exact PR/merge-group subjects."""

from __future__ import annotations

import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta
from urllib.parse import quote

from sqlalchemy import func, or_, select, update

from backend.models.pr_monitor import (
    MonitoredRepo,
    PRMergeQueueAction,
    PRMonitorRun,
    PRReview,
    pr_merge_queue_action_ambiguous_remote_effect_predicate,
    pr_merge_queue_action_has_ambiguous_remote_effect,
    pr_monitor_run_has_terminal_intent,
    pr_monitor_run_no_terminal_intent_predicate,
)


_SHA_RE = re.compile(r"[0-9a-f]{40}\Z")
_QUEUE_ENTRY_STATES = {
    "QUEUED",
    "AWAITING_CHECKS",
    "MERGEABLE",
    "UNMERGEABLE",
    "LOCKED",
}
_QUEUE_ENTRY_BLOCKED_STATES = {"UNMERGEABLE", "LOCKED"}


async def _database_now(db) -> datetime:
    value = (await db.execute(select(func.current_timestamp()))).scalar_one()
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise ValueError("Database clock returned an invalid timestamp")
    return value.replace(tzinfo=None)


@dataclass(frozen=True)
class QueueEntry:
    id: str
    state: str
    base_ref: str
    base_sha: str
    head_sha: str
    created_by_call: bool = False
    pull_request_id: str | None = None


class QueueEntryCleanupError(RuntimeError):
    """A new remote queue entry could not be proven removed."""

    def __init__(self, message: str, *, entry_id: str):
        super().__init__(message)
        self.entry_id = entry_id


async def _read_queue_entry(repo_name: str, pr_number: int) -> QueueEntry | None:
    from backend.services.pr_review_service import _gh_api_value, _valid_base_ref

    owner, name = repo_name.split("/", 1)
    query = """query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){id baseRefName mergeQueueEntry{id state baseCommit{oid} headCommit{oid}}}}}"""
    result = await _gh_api_value("graphql", payload={
        "query": query,
        "variables": {"owner": owner, "name": name, "number": pr_number},
    })
    try:
        pr = result["data"]["repository"]["pullRequest"]
        if not isinstance(pr, dict):
            raise TypeError
        entry = pr.get("mergeQueueEntry")
    except (KeyError, TypeError) as exc:
        raise ValueError("GitHub Merge Queue query is malformed") from exc
    if entry is None:
        return None
    if not isinstance(entry, dict):
        raise ValueError("GitHub Merge Queue entry is malformed")
    base_commit = entry.get("baseCommit")
    head_commit = entry.get("headCommit")
    base_ref = pr.get("baseRefName")
    base_sha = base_commit.get("oid") if isinstance(base_commit, dict) else None
    head_sha = head_commit.get("oid") if isinstance(head_commit, dict) else None
    if (
        not isinstance(pr.get("id"), str)
        or not pr["id"]
        or not isinstance(entry.get("id"), str)
        or not entry["id"]
        or not isinstance(entry.get("state"), str)
        or entry["state"].upper() not in _QUEUE_ENTRY_STATES
        or not _valid_base_ref(base_ref)
        or not isinstance(base_sha, str)
        or _SHA_RE.fullmatch(base_sha.lower()) is None
        or not isinstance(head_sha, str)
        or _SHA_RE.fullmatch(head_sha.lower()) is None
    ):
        raise ValueError("GitHub Merge Queue entry is malformed")
    return QueueEntry(
        id=entry["id"],
        state=entry["state"].upper(),
        base_ref=base_ref,
        base_sha=base_sha.lower(),
        head_sha=head_sha.lower(),
        pull_request_id=pr["id"],
    )


async def _read_merge_group_ref(
    repo_name: str,
    *,
    default_branch: str,
    pr_number: int,
) -> tuple[str, str] | None:
    """Resolve the one current synthetic merge-group ref for a PR."""

    from backend.services.pr_review_service import _gh_api_value

    short_prefix = f"gh-readonly-queue/{default_branch}/pr-{pr_number}-"
    endpoint_prefix = quote(f"heads/{short_prefix}", safe="/")
    value = await _gh_api_value(
        f"repos/{repo_name}/git/matching-refs/{endpoint_prefix}",
        max_output_bytes=4 * 1024 * 1024,
    )
    if not isinstance(value, list):
        raise ValueError("GitHub matching-refs response is malformed")
    full_prefix = f"refs/heads/{short_prefix}"
    matches: list[tuple[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("GitHub matching-ref item is malformed")
        ref = item.get("ref")
        obj = item.get("object")
        sha = obj.get("sha") if isinstance(obj, dict) else None
        if (
            not isinstance(ref, str)
            or not ref.startswith(full_prefix)
            or len(ref) > 500
            or not isinstance(sha, str)
            or _SHA_RE.fullmatch(sha.lower()) is None
        ):
            raise ValueError("GitHub matching-ref identity is malformed")
        matches.append((sha.lower(), ref))
    if not matches:
        return None
    if len(matches) != 1:
        raise ValueError("GitHub Merge Queue ref is ambiguous")
    return matches[0]


async def _dequeue_queue_entry(
    repo_name: str,
    pr_number: int,
    pull_request_id: str,
    entry_id: str,
) -> None:
    """Remove one exact queue entry and prove that exact id disappeared."""

    from backend.services.pr_review_service import _gh_api_value

    mutation = """mutation($id:ID!){dequeuePullRequest(input:{id:$id}){mergeQueueEntry{id}}}"""
    result = await _gh_api_value("graphql", payload={
        "query": mutation,
        "variables": {"id": pull_request_id},
    })
    try:
        payload = result["data"]["dequeuePullRequest"]
    except (KeyError, TypeError) as exc:
        raise ValueError("GitHub dequeuePullRequest response is malformed") from exc
    if not isinstance(payload, dict):
        raise ValueError("GitHub did not confirm Merge Queue dequeue")
    remaining = await _read_queue_entry(repo_name, pr_number)
    if remaining is not None and remaining.id == entry_id:
        raise ValueError("GitHub Merge Queue entry remained after dequeue")


async def _remove_new_queue_entry_or_raise(
    repo_name: str,
    pr_number: int,
    pull_request_id: str,
    entry_id: str,
    *,
    reason: str,
) -> None:
    try:
        await _dequeue_queue_entry(
            repo_name, pr_number, pull_request_id, entry_id
        )
    except Exception as exc:
        raise QueueEntryCleanupError(
            f"{reason}; exact remote cleanup failed: "
            f"{type(exc).__name__}:{str(exc)[:300]}",
            entry_id=entry_id,
        ) from exc


async def _enqueue(
    repo_name: str,
    pr_number: int,
    base_ref: str,
    base_sha: str,
    head_sha: str,
) -> QueueEntry:
    from backend.services.pr_review_service import _gh_api_value

    existing = await _read_queue_entry(repo_name, pr_number)
    if existing is not None:
        if (
            existing.base_ref != base_ref
            or existing.base_sha != base_sha
            or existing.head_sha != head_sha
        ):
            raise ValueError("Existing Merge Queue entry is not the exact subject")
        return existing
    owner, name = repo_name.split("/", 1)
    node_query = """query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){id baseRefName baseRefOid headRefOid}}}"""
    node_result = await _gh_api_value("graphql", payload={
        "query": node_query,
        "variables": {"owner": owner, "name": name, "number": pr_number},
    })
    try:
        pr = node_result["data"]["repository"]["pullRequest"]
        if not isinstance(pr, dict):
            raise TypeError
    except (KeyError, TypeError) as exc:
        raise ValueError("GitHub pull request node response is malformed") from exc
    if (
        not isinstance(pr.get("id"), str)
        or not pr["id"]
        or pr.get("baseRefName") != base_ref
        or not isinstance(pr.get("baseRefOid"), str)
        or pr["baseRefOid"].lower() != base_sha
        or not isinstance(pr.get("headRefOid"), str)
        or pr["headRefOid"].lower() != head_sha
    ):
        raise ValueError("GitHub pull request node is not the exact queued subject")
    mutation = """mutation($pullRequestId:ID!,$expectedHeadOid:GitObjectID!){enqueuePullRequest(input:{pullRequestId:$pullRequestId,expectedHeadOid:$expectedHeadOid}){mergeQueueEntry{id state}}}"""
    result = await _gh_api_value("graphql", payload={
        "query": mutation,
        "variables": {"pullRequestId": pr["id"], "expectedHeadOid": head_sha},
    })
    try:
        entry = result["data"]["enqueuePullRequest"]["mergeQueueEntry"]
        if not isinstance(entry, dict):
            raise TypeError
    except (KeyError, TypeError) as exc:
        raise ValueError("GitHub enqueuePullRequest response is malformed") from exc
    if (
        not isinstance(entry.get("id"), str)
        or not entry["id"]
        or not isinstance(entry.get("state"), str)
        or entry["state"].upper() not in _QUEUE_ENTRY_STATES
    ):
        raise ValueError("GitHub did not confirm Merge Queue entry")
    # Re-read the durable entry because the mutation response does not expose
    # its exact base/head commits.  Queue admission is not accepted without
    # proving both immutable subject components.
    try:
        confirmed = await _read_queue_entry(repo_name, pr_number)
    except Exception as exc:
        await _remove_new_queue_entry_or_raise(
            repo_name,
            pr_number,
            pr["id"],
            entry["id"],
            reason=(
                "GitHub queue subject confirmation failed: "
                f"{type(exc).__name__}:{str(exc)[:300]}"
            ),
        )
        raise ValueError(
            "GitHub queue subject confirmation failed; new entry was removed"
        ) from exc
    if (
        confirmed is None
        or confirmed.id != entry["id"]
        or confirmed.base_ref != base_ref
        or confirmed.base_sha != base_sha
        or confirmed.head_sha != head_sha
    ):
        await _remove_new_queue_entry_or_raise(
            repo_name,
            pr_number,
            pr["id"],
            entry["id"],
            reason="GitHub did not confirm the exact queued subject",
        )
        raise ValueError(
            "GitHub did not confirm the exact queued subject; new entry was removed"
        )
    return QueueEntry(
        id=confirmed.id,
        state=confirmed.state,
        base_ref=confirmed.base_ref,
        base_sha=confirmed.base_sha,
        head_sha=confirmed.head_sha,
        created_by_call=True,
        pull_request_id=pr["id"],
    )


async def bind_merge_group(
    db, *, repo: MonitoredRepo, head_sha: str, head_ref: str,
) -> bool:
    """Bind one signed merge_group webhook to one unambiguous queued PR."""
    if not repo.enabled:
        return False
    if not _SHA_RE.fullmatch(head_sha) or not isinstance(head_ref, str):
        raise ValueError("merge_group subject is malformed")
    actions = list((await db.execute(
        select(PRMergeQueueAction)
        .join(PRMonitorRun, PRMonitorRun.id == PRMergeQueueAction.monitor_run_id)
        .where(
            PRMonitorRun.repo_id == repo.id,
            pr_monitor_run_no_terminal_intent_predicate(),
            PRMergeQueueAction.effect_kind == "queue",
            PRMergeQueueAction.status.in_(("queued", "checking")),
        )
    )).scalars())
    matches = []
    for action in actions:
        run = await db.get(PRMonitorRun, action.monitor_run_id)
        review = await db.get(PRReview, action.review_id)
        expected_prefix = (
            f"refs/heads/gh-readonly-queue/{review.base_ref}/"
            f"pr-{run.pr_number}-"
            if (
                run is not None
                and review is not None
                and not pr_monitor_run_has_terminal_intent(run)
                and run.current_review_id == review.id
                and review.monitor_run_id == run.id
                and review.base_sha == action.trigger_base_sha
                and review.head_sha == action.trigger_head_sha
            )
            else ""
        )
        if expected_prefix and head_ref.startswith(expected_prefix):
            matches.append((action, run))
    if len(matches) != 1:
        return False
    candidate_action, candidate_run = matches[0]
    candidate_review = await db.get(PRReview, candidate_action.review_id)
    if candidate_review is None:
        await db.rollback()
        return False
    locked_repo, action, run, review = await _lock_queue_effect_rows(
        db,
        repo_id=repo.id,
        action_id=candidate_action.id,
        run_id=candidate_run.id,
        review_id=candidate_review.id,
    )
    expected_prefix = (
        f"refs/heads/gh-readonly-queue/{review.base_ref}/"
        f"pr-{run.pr_number}-"
        if review is not None and run is not None
        else ""
    )
    if (
        locked_repo is None
        or not locked_repo.enabled
        or action is None
        or run is None
        or review is None
        or pr_monitor_run_has_terminal_intent(run)
        or action.status not in {"queued", "checking"}
        or action.monitor_run_id != run.id
        or action.review_id != review.id
        or run.current_review_id != review.id
        or review.monitor_run_id != run.id
        or review.base_sha != action.trigger_base_sha
        or review.head_sha != action.trigger_head_sha
        or run.current_base_sha != action.trigger_base_sha
        or run.current_head_sha != action.trigger_head_sha
        or not expected_prefix
        or not head_ref.startswith(expected_prefix)
    ):
        await db.rollback()
        return False
    action.merge_group_sha = head_sha
    action.merge_group_ref = head_ref[:500]
    action.status = "checking"
    action.ci_status = "pending"
    run.status = "merge_group_checking"
    run.state_version += 1
    await db.commit()
    return True


async def _lock_queue_effect_rows(
    db,
    *,
    repo_id: int,
    action_id: int,
    run_id: int,
    review_id: int,
):
    """Fresh ``Repo -> Run -> Review -> Action`` effect barrier."""

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
    repo = (await db.execute(
        select(MonitoredRepo)
        .where(MonitoredRepo.id == repo_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )).scalar_one_or_none()
    run = (await db.execute(
        select(PRMonitorRun)
        .where(PRMonitorRun.id == run_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )).scalar_one_or_none()
    review = (await db.execute(
        select(PRReview)
        .where(PRReview.id == review_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )).scalar_one_or_none()
    action = (await db.execute(
        select(PRMergeQueueAction)
        .where(PRMergeQueueAction.id == action_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )).scalar_one_or_none()
    return repo, action, run, review


def _queue_effect_generation(
    repo: MonitoredRepo,
    action: PRMergeQueueAction,
    run: PRMonitorRun,
    review: PRReview,
) -> tuple:
    """Freeze every persisted field consumed by one reconciliation pass.

    The reconciler deliberately releases its transaction around GitHub and CI
    reads.  Comparing every column after reacquiring Repo -> Run -> Review ->
    Action avoids accidentally consuming remote evidence against a changed
    policy, lease, lifecycle, or immutable subject.  JSON values are compared
    structurally by Python and all other mapped values are immutable scalars.
    """

    def row_generation(row) -> tuple:
        return tuple(
            getattr(row, column.key)
            for column in row.__table__.columns
        )

    return (
        row_generation(repo),
        row_generation(run),
        row_generation(review),
        row_generation(action),
    )


async def _relock_queue_effect_generation(
    db,
    *,
    repo_id: int,
    action_id: int,
    run_id: int,
    review_id: int,
    expected_generation: tuple,
):
    """Reacquire the effect rows and consume only the exact frozen state."""

    repo, action, run, review = await _lock_queue_effect_rows(
        db,
        repo_id=repo_id,
        action_id=action_id,
        run_id=run_id,
        review_id=review_id,
    )
    if (
        repo is None
        or action is None
        or run is None
        or review is None
        or _queue_effect_generation(repo, action, run, review)
        != expected_generation
    ):
        await db.rollback()
        return None, None, None, None
    return repo, action, run, review


async def _record_enqueue_failure(
    db,
    *,
    repo_id: int,
    action_id: int,
    run_id: int,
    review_id: int,
    lease_token: str,
    message: str,
    unsafe_entry_id: str | None = None,
) -> None:
    repo, action, run, review = await _lock_queue_effect_rows(
        db,
        repo_id=repo_id,
        action_id=action_id,
        run_id=run_id,
        review_id=review_id,
    )
    if action is None:
        await db.rollback()
        return
    if unsafe_entry_id is not None:
        # Remote state could still merge.  Keep that risk explicit and never
        # let a later retry create a second queue effect automatically.  This
        # id came from the enqueue response for this CCM call, so it is durable
        # ownership evidence; retain an *active* status until recovery proves
        # the exact entry absent or removes it.  ``paused`` would let terminal
        # lifecycle convergence falsely treat the external effect as quiet.
        if action.status not in {"merged", "superseded"}:
            action.status = "enqueuing"
            action.last_error = message[:1000]
            action.github_queue_entry_id = unsafe_entry_id
            action.lease_token = None
            action.lease_expires_at = None
        if (
            run is not None
            and run.status not in {"merged", "closed"}
            and run.completed_at is None
        ):
            run.status = "paused"
            run.pause_reason = message[:1000]
            run.state_version += 1
        await db.commit()
        return
    if action.status == "enqueuing" and action.lease_token == lease_token:
        action.last_error = message[:1000]
        action.lease_token = None
        action.lease_expires_at = None
        await db.commit()
    else:
        await db.rollback()


async def _abort_enqueue_after_lifecycle_change(
    db,
    *,
    repo_id: int,
    action_id: int,
    run_id: int,
    review_id: int,
    repo_name: str,
    pr_number: int,
    lease_token: str,
    entry: QueueEntry,
    reason: str,
    terminal_intent: bool = False,
) -> None:
    cleanup_error: str | None = None
    # A terminal lifecycle intent revokes future authority; it does not prove
    # that a pre-existing queue entry belongs to CCM.  Only the caller that
    # actually created this entry may remove it here.  Durable ownership from
    # an earlier finalized call is handled by terminal recovery below.
    if entry.created_by_call:
        try:
            if entry.pull_request_id is None:
                raise ValueError("new queue entry has no pull request node id")
            await _dequeue_queue_entry(
                repo_name, pr_number, entry.pull_request_id, entry.id
            )
        except Exception as exc:
            cleanup_error = (
                "merge_queue_remote_cleanup_failed:"
                f"{type(exc).__name__}:{str(exc)[:500]}"
            )

    repo, action, run, review = await _lock_queue_effect_rows(
        db,
        repo_id=repo_id,
        action_id=action_id,
        run_id=run_id,
        review_id=review_id,
    )
    if action is None:
        await db.rollback()
        return
    if cleanup_error is not None:
        if action.status not in {"merged", "superseded"}:
            # Remote state may still merge. Retain the active ``enqueuing``
            # marker so disable/terminal quiescence stays fail-closed; a
            # paused row would falsely claim the external effect was gone.
            action.status = "enqueuing"
            action.last_error = cleanup_error
            action.github_queue_entry_id = entry.id
            action.lease_token = None
            action.lease_expires_at = None
        if (
            run is not None
            and run.status not in {"merged", "closed"}
            and run.completed_at is None
        ):
            run.status = "paused"
            run.pause_reason = cleanup_error
            run.state_version += 1
        await db.commit()
        return

    if not entry.created_by_call:
        # This exact entry predated CCM's call (for example a manual enqueue),
        # so ownership is not ours to revoke.  Under terminal intent keep the
        # action active: unknown remote ownership is not quiescence and must
        # block terminalization until an operator or later durable evidence
        # resolves it.  Do not persist the observed id here, because doing so
        # would manufacture ownership evidence for the next recovery pass.
        message = f"merge_queue_existing_entry_lifecycle_changed:{reason}"
        if action.status not in {"merged", "superseded"}:
            action.status = "enqueuing"
            action.last_error = message[:1000]
            action.lease_token = None
            action.lease_expires_at = None
        if (
            run is not None
            and run.status not in {"merged", "closed"}
            and run.completed_at is None
        ):
            run.status = "paused"
            run.pause_reason = message[:1000]
            run.state_version += 1
        await db.commit()
        return

    # Our new entry was proven absent.  Preserve a concurrent pause/disable;
    # only withdraw the exact lease still owned by this reconciler.
    if action.status == "enqueuing" and action.lease_token == lease_token:
        action.status = "superseded" if terminal_intent else "paused"
        action.last_error = f"merge_queue_enqueue_aborted:{reason}"[:1000]
        action.lease_token = None
        action.lease_expires_at = None
        if terminal_intent:
            action.completed_at = datetime.utcnow()
        if (
            run is not None
            and run.status not in {"paused", "merged", "closed"}
            and run.completed_at is None
        ):
            run.status = "paused"
            run.pause_reason = action.last_error
            run.state_version += 1
    await db.commit()


async def _recover_terminal_queue_action(
    db,
    *,
    repo: MonitoredRepo,
    run: PRMonitorRun,
    review: PRReview,
    action: PRMergeQueueAction,
) -> bool:
    """Fence and remove/settle one active queue effect under terminal intent."""

    from backend.services.pr_review_service import _gh_pr_view, _validated_pr_snapshot

    terminal_status = run.terminal_intent_status
    terminal_base_ref = run.terminal_intent_base_ref
    terminal_head_sha = run.terminal_intent_head_sha
    if terminal_status not in {"closed", "merged"}:
        await db.rollback()
        return False
    now = await _database_now(db)
    if action.lease_token is not None and (
        action.lease_expires_at is None or action.lease_expires_at > now
    ):
        # The original caller may still be inside GitHub's mutation.
        await db.rollback()
        return False
    token = secrets.token_hex(24)
    expected_action_status = action.status
    claimed = await db.execute(
        update(PRMergeQueueAction)
        .where(
            PRMergeQueueAction.id == action.id,
            PRMergeQueueAction.status == expected_action_status,
            or_(
                PRMergeQueueAction.lease_token.is_(None),
                PRMergeQueueAction.lease_expires_at <= now,
            ),
        )
        .values(
            lease_token=token,
            lease_expires_at=now + timedelta(minutes=10),
            last_error="pr_terminal_intent_reconciling_enqueue",
        )
        .execution_options(synchronize_session=False)
    )
    if claimed.rowcount != 1:
        await db.rollback()
        return False

    repo_id = repo.id
    run_id = run.id
    review_id = review.id
    action_id = action.id
    repo_name = repo.repo_full_name
    pr_number = run.pr_number
    trigger_base_sha = action.trigger_base_sha
    trigger_head_sha = action.trigger_head_sha
    review_base_ref = review.base_ref
    await db.commit()

    try:
        snapshot = _validated_pr_snapshot(
            await _gh_pr_view(pr_number, repo_name)
        )
        terminal_matches = bool(
            snapshot["base_ref"] == terminal_base_ref == review_base_ref
            and snapshot["head_sha"] == terminal_head_sha == trigger_head_sha
            and (
                (
                    terminal_status == "merged"
                    and snapshot["state"] == "MERGED"
                    and snapshot["merged_at"] is not None
                )
                or (
                    terminal_status == "closed"
                    and snapshot["state"] == "CLOSED"
                    and snapshot["merged_at"] is None
                )
            )
        )
        if not terminal_matches:
            raise ValueError("terminal PR subject changed during enqueue recovery")
        # An exact CLOSED PR cannot subsequently merge from any queue entry,
        # and an exact MERGED PR has already settled the effect.  Do not issue
        # a dequeue here: a visible entry may be manual and terminal intent is
        # not ownership proof.  The signed lifecycle plus fresh exact GitHub
        # snapshot is sufficient to settle the local action without touching
        # an entry CCM cannot prove it created.
    except Exception as exc:
        _repo, locked_action, _run, _review = await _lock_queue_effect_rows(
            db,
            repo_id=repo_id,
            action_id=action_id,
            run_id=run_id,
            review_id=review_id,
        )
        if (
            locked_action is not None
            and locked_action.status == expected_action_status
            and locked_action.lease_token == token
        ):
            # Keep the active status so terminal quiescence remains blocked,
            # but release this failed recovery lease for a later retry.
            locked_action.lease_token = None
            locked_action.lease_expires_at = None
            locked_action.last_error = (
                "merge_queue_terminal_recovery_failed:"
                f"{type(exc).__name__}:{str(exc)[:500]}"
            )
            await db.commit()
        else:
            await db.rollback()
        return False

    _repo, locked_action, locked_run, locked_review = await _lock_queue_effect_rows(
        db,
        repo_id=repo_id,
        action_id=action_id,
        run_id=run_id,
        review_id=review_id,
    )
    if (
        locked_action is None
        or locked_run is None
        or locked_review is None
        or locked_action.status != expected_action_status
        or locked_action.lease_token != token
        or locked_run.terminal_intent_status != terminal_status
        or locked_run.terminal_intent_base_ref != terminal_base_ref
        or locked_run.terminal_intent_head_sha != terminal_head_sha
        or locked_action.trigger_base_sha != trigger_base_sha
        or locked_action.trigger_head_sha != trigger_head_sha
        or locked_review.base_ref != review_base_ref
    ):
        await db.rollback()
        return False
    locked_action.status = (
        "merged" if terminal_status == "merged" else "superseded"
    )
    locked_action.completed_at = datetime.utcnow()
    locked_action.last_error = None
    locked_action.lease_token = None
    locked_action.lease_expires_at = None
    await db.commit()
    return True


def _merge_queue_policy_allows_enqueue(
    repo: MonitoredRepo,
    action: PRMergeQueueAction,
) -> bool:
    """Match an unstarted enqueue to its durable authorization source."""

    return bool(
        (
            action.trigger_kind == "policy"
            and repo.merge_queue_mode == "auto"
        )
        or action.trigger_kind == "manual"
    )


async def reconcile_merge_queue(db_factory) -> int:
    from backend.services.pr_review_panel import fetch_exact_head_ci
    from backend.services.pr_review_service import _gh_pr_view, _validated_pr_snapshot

    progressed = 0
    async with db_factory() as db:
        action_ids = list((await db.execute(select(PRMergeQueueAction.id).where(
            PRMergeQueueAction.effect_kind == "queue",
            or_(
                PRMergeQueueAction.status.in_((
                    "pending", "enqueuing", "queued", "checking"
                )),
                pr_merge_queue_action_ambiguous_remote_effect_predicate(),
            )
        ).order_by(PRMergeQueueAction.id))).scalars())
    for action_id_candidate in action_ids:
        # Each durable action owns its transaction. A lost lease or rollback
        # on a later action must never erase an earlier action's progress.
        async with db_factory() as db:
            preliminary = await db.get(PRMergeQueueAction, action_id_candidate)
            preliminary_run = (
                await db.get(PRMonitorRun, preliminary.monitor_run_id)
                if preliminary is not None
                else None
            )
            if preliminary is None or preliminary_run is None:
                await db.rollback()
                continue
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
                continue
            repo = (await db.execute(
                select(MonitoredRepo)
                .where(MonitoredRepo.id == repo_id)
                .with_for_update()
            )).scalar_one_or_none()
            run = (await db.execute(
                select(PRMonitorRun)
                .where(PRMonitorRun.id == run_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )).scalar_one_or_none()
            review = (await db.execute(
                select(PRReview)
                .where(PRReview.id == review_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )).scalar_one_or_none()
            action = (await db.execute(
                select(PRMergeQueueAction)
                .where(PRMergeQueueAction.id == action_id_candidate)
                .with_for_update()
                .execution_options(populate_existing=True)
            )).scalar_one_or_none()
            if (
                action is None
                or (
                    action.status not in {
                        "pending", "enqueuing", "queued", "checking"
                    }
                    and not pr_merge_queue_action_has_ambiguous_remote_effect(
                        action
                    )
                )
                or run is None
                or review is None
                or repo is None
            ):
                await db.rollback()
                continue
            if not repo.enabled:
                # A started action may still have an exact remote queue entry.
                # Keep it in the active reconciliation set until absence or an
                # exact terminal PR is proven; disabling CCM is not a GitHub
                # dequeue operation.
                if action.status == "pending":
                    action.status = "paused"
                action.last_error = "repo_disabled"
                run.status = "paused"
                run.pause_reason = action.last_error
                run.state_version += 1
                await db.commit()
                continue
            if (
                pr_monitor_run_has_terminal_intent(run)
                and action.status == "pending"
            ):
                action.status = "superseded"
                action.last_error = "pr_terminal_intent"
                action.completed_at = datetime.utcnow()
                await db.commit()
                continue
            if (
                pr_monitor_run_has_terminal_intent(run)
                and (
                    action.status in {"enqueuing", "queued", "checking"}
                    or pr_merge_queue_action_has_ambiguous_remote_effect(action)
                )
            ):
                # The lease owner may currently be inside GitHub's enqueue
                # mutation. It alone must read back/dequeue the exact entry and
                # clear its lease; changing this row to paused would let the
                # terminal webhook finish before that external call returns.
                recovered = await _recover_terminal_queue_action(
                    db,
                    repo=repo,
                    run=run,
                    review=review,
                    action=action,
                )
                if recovered:
                    progressed += 1
                continue
            if (
                run.current_base_sha != action.trigger_base_sha
                or run.current_head_sha != action.trigger_head_sha
                or run.current_review_id != review.id
                or review.monitor_run_id != run.id
                or review.base_sha != action.trigger_base_sha
                or review.head_sha != action.trigger_head_sha
            ):
                if action.status == "pending":
                    action.status = "superseded"
                    action.completed_at = datetime.utcnow()
                else:
                    action.last_error = "merge_queue_local_subject_changed"
                    run.status = "paused"
                    run.pause_reason = action.last_error
                    run.state_version += 1
                await db.commit()
                continue
            if (
                action.status == "pending"
                and not _merge_queue_policy_allows_enqueue(repo, action)
            ):
                action.status = "paused"
                action.last_error = "merge_queue_policy_changed"
                run.status = "paused"
                run.pause_reason = action.last_error
                run.state_version += 1
                await db.commit()
                continue
            expected_generation = _queue_effect_generation(
                repo, action, run, review
            )
            remote_repo_name = repo.repo_full_name
            remote_pr_number = run.pr_number
            await db.rollback()
            try:
                snapshot = _validated_pr_snapshot(
                    await _gh_pr_view(remote_pr_number, remote_repo_name)
                )
            except Exception as exc:
                repo, action, run, review = (
                    await _relock_queue_effect_generation(
                        db,
                        repo_id=repo_id,
                        action_id=action_id_candidate,
                        run_id=run_id,
                        review_id=review_id,
                        expected_generation=expected_generation,
                    )
                )
                if action is None:
                    continue
                action.last_error = (
                    f"merge_queue_pr_read_failed:{type(exc).__name__}:"
                    f"{str(exc)[:300]}"
                )
                await db.commit()
                continue
            repo, action, run, review = (
                await _relock_queue_effect_generation(
                    db,
                    repo_id=repo_id,
                    action_id=action_id_candidate,
                    run_id=run_id,
                    review_id=review_id,
                    expected_generation=expected_generation,
                )
            )
            if action is None:
                continue
            if (
                snapshot["state"] == "MERGED"
                and snapshot["base_ref"] == review.base_ref
                and snapshot["head_sha"] == action.trigger_head_sha
            ):
                action.status = "merged"
                action.completed_at = datetime.utcnow()
                action.last_error = None
                run.status = "merged"
                run.completed_at = datetime.utcnow()
                run.state_version += 1
                await db.commit()
                progressed += 1
                continue
            if (
                snapshot["state"] != "OPEN"
                or snapshot["merged_at"] is not None
                or snapshot["is_draft"]
                or snapshot["base_ref"] != review.base_ref
                or snapshot["base_sha"] != action.trigger_base_sha
                or snapshot["head_sha"] != action.trigger_head_sha
            ):
                if action.status == "pending":
                    action.status = "paused"
                    action.last_error = "merge_queue_pr_subject_changed"
                    run.status = "paused"
                    run.pause_reason = action.last_error
                    run.state_version += 1
                    await db.commit()
                    continue

                # A push commonly changes the remote head and removes the old
                # queue entry before synchronize can acquire CCM's effect
                # fence.  Never infer absence from the PR snapshot alone:
                # read the queue entry without retaining the exact local locks,
                # then consume it only after the complete generation matches.
                expected_generation = _queue_effect_generation(
                    repo, action, run, review
                )
                remote_repo_name = repo.repo_full_name
                remote_pr_number = run.pr_number
                await db.rollback()
                try:
                    changed_subject_entry = await _read_queue_entry(
                        remote_repo_name,
                        remote_pr_number,
                    )
                except Exception as exc:
                    repo, action, run, review = (
                        await _relock_queue_effect_generation(
                            db,
                            repo_id=repo_id,
                            action_id=action_id_candidate,
                            run_id=run_id,
                            review_id=review_id,
                            expected_generation=expected_generation,
                        )
                    )
                    if action is None:
                        continue
                    action.last_error = (
                        "merge_queue_subject_changed_entry_read_failed:"
                        f"{type(exc).__name__}:{str(exc)[:300]}"
                    )
                    run.status = "paused"
                    run.pause_reason = action.last_error
                    run.state_version += 1
                    await db.commit()
                    continue

                repo, action, run, review = (
                    await _relock_queue_effect_generation(
                        db,
                        repo_id=repo_id,
                        action_id=action_id_candidate,
                        run_id=run_id,
                        review_id=review_id,
                        expected_generation=expected_generation,
                    )
                )
                if action is None:
                    continue

                fresh_open_subject = bool(
                    snapshot["state"] == "OPEN"
                    and snapshot["merged_at"] is None
                    and not snapshot["is_draft"]
                    and snapshot["base_ref"] == review.base_ref
                )
                if changed_subject_entry is None and fresh_open_subject:
                    # Fresh PR + queue reads prove the old external effect is
                    # absent.  This durable receipt lets the synchronize
                    # webhook admit the new immutable head on its retry.
                    action.status = "failed"
                    action.last_error = (
                        "merge_queue_remote_absence_proven:subject_changed"
                    )
                    action.github_queue_entry_id = None
                    action.merge_group_sha = None
                    action.merge_group_ref = None
                    action.lease_token = None
                    action.lease_expires_at = None
                    action.completed_at = datetime.utcnow()
                    progressed += 1
                else:
                    # An entry is still live (possibly manual or for a new
                    # subject), or GitHub is not a fresh open PR.  Preserve an
                    # active local fence without adopting/dequeuing that entry.
                    if action.status in {"paused", "failed"}:
                        action.status = "queued"
                    action.last_error = (
                        "merge_queue_pr_subject_changed_remote_entry_present"
                        if changed_subject_entry is not None
                        else "merge_queue_pr_subject_changed"
                    )
                run.status = "paused"
                run.pause_reason = action.last_error
                run.state_version += 1
                await db.commit()
                continue

            if action.status in {"pending", "enqueuing"}:
                # Merge Queue admission has been retired. A legacy row whose
                # mutation acknowledgement might have been lost must first
                # prove the remote queue entry absent; it is never replayed.
                expected_generation = _queue_effect_generation(
                    repo, action, run, review
                )
                await db.rollback()
                try:
                    retired_entry = await _read_queue_entry(
                        remote_repo_name,
                        remote_pr_number,
                    )
                except Exception as exc:
                    repo, action, run, review = (
                        await _relock_queue_effect_generation(
                            db,
                            repo_id=repo_id,
                            action_id=action_id_candidate,
                            run_id=run_id,
                            review_id=review_id,
                            expected_generation=expected_generation,
                        )
                    )
                    if action is None:
                        continue
                    action.last_error = (
                        "merge_queue_retirement_entry_read_failed:"
                        f"{type(exc).__name__}:{str(exc)[:300]}"
                    )
                    await db.commit()
                    continue
                repo, action, run, review = (
                    await _relock_queue_effect_generation(
                        db,
                        repo_id=repo_id,
                        action_id=action_id_candidate,
                        run_id=run_id,
                        review_id=review_id,
                        expected_generation=expected_generation,
                    )
                )
                if action is None:
                    continue
                if retired_entry is None:
                    action.status = "failed"
                    action.last_error = (
                        "merge_queue_remote_absence_proven:integration_retired"
                    )
                    action.github_queue_entry_id = None
                    action.merge_group_sha = None
                    action.merge_group_ref = None
                    action.lease_token = None
                    action.lease_expires_at = None
                    action.completed_at = datetime.utcnow()
                    run.status = "ready_to_merge"
                    run.pause_reason = None
                    run.state_version += 1
                    await db.commit()
                    progressed += 1
                    continue
                if (
                    retired_entry.base_ref != review.base_ref
                    or retired_entry.base_sha != action.trigger_base_sha
                    or retired_entry.head_sha != action.trigger_head_sha
                ):
                    action.status = "paused"
                    action.last_error = "merge_queue_retirement_subject_mismatch"
                    run.status = "paused"
                    run.pause_reason = action.last_error
                    run.state_version += 1
                    await db.commit()
                    continue
                action.status = "queued"
                action.github_queue_entry_id = retired_entry.id
                action.last_error = "merge_queue_integration_retired_entry_active"
                action.lease_token = None
                action.lease_expires_at = None
                run.status = "merge_queued"
                run.state_version += 1
                await db.commit()
                continue

            # Freeze the complete effect generation, then execute the minimal
            # remote read bundle for its current state without a checked-out DB
            # connection.  Every outcome is consumed only after the same
            # Repo -> Run -> Review -> Action generation is locked again.
            expected_generation = _queue_effect_generation(
                repo, action, run, review
            )
            queue_repo_name = repo.repo_full_name
            queue_pr_number = run.pr_number
            queue_base_ref = review.base_ref
            queue_base_sha = action.trigger_base_sha
            queue_head_sha = action.trigger_head_sha
            queue_required_checks = list(repo.required_checks or [])
            prior_entry_id = action.github_queue_entry_id
            prior_group_sha = action.merge_group_sha
            prior_group_ref = action.merge_group_ref
            prior_action_status = action.status
            await db.rollback()

            try:
                entry = await _read_queue_entry(
                    queue_repo_name,
                    queue_pr_number,
                )
            except Exception as exc:
                repo, action, run, review = (
                    await _relock_queue_effect_generation(
                        db,
                        repo_id=repo_id,
                        action_id=action_id_candidate,
                        run_id=run_id,
                        review_id=review_id,
                        expected_generation=expected_generation,
                    )
                )
                if action is None:
                    continue
                action.last_error = (
                    f"merge_queue_entry_read_failed:{type(exc).__name__}:"
                    f"{str(exc)[:300]}"
                )
                await db.commit()
                continue

            entry_subject_changed = entry is not None and (
                entry.base_ref != queue_base_ref
                or entry.base_sha != queue_base_sha
                or entry.head_sha != queue_head_sha
            )
            if entry is None or entry_subject_changed:
                # Queue removal commonly races the final merge. Re-read the PR
                # in the same lock-free bundle so a completed merge cannot be
                # recorded as remote absence.
                try:
                    fresh_snapshot = _validated_pr_snapshot(
                        await _gh_pr_view(queue_pr_number, queue_repo_name)
                    )
                except Exception as exc:
                    repo, action, run, review = (
                        await _relock_queue_effect_generation(
                            db,
                            repo_id=repo_id,
                            action_id=action_id_candidate,
                            run_id=run_id,
                            review_id=review_id,
                            expected_generation=expected_generation,
                        )
                    )
                    if action is None:
                        continue
                    action.last_error = (
                        "merge_queue_pr_reread_failed:"
                        f"{type(exc).__name__}:{str(exc)[:300]}"
                    )
                    await db.commit()
                    continue

                repo, action, run, review = (
                    await _relock_queue_effect_generation(
                        db,
                        repo_id=repo_id,
                        action_id=action_id_candidate,
                        run_id=run_id,
                        review_id=review_id,
                        expected_generation=expected_generation,
                    )
                )
                if action is None:
                    continue
                if (
                    fresh_snapshot["state"] == "MERGED"
                    and fresh_snapshot["base_ref"] == queue_base_ref
                    and fresh_snapshot["base_sha"] == queue_base_sha
                    and fresh_snapshot["head_sha"] == queue_head_sha
                ):
                    action.status = "merged"
                    action.completed_at = datetime.utcnow()
                    action.last_error = None
                    run.status = "merged"
                    run.completed_at = datetime.utcnow()
                    run.pause_reason = None
                    run.state_version += 1
                    await db.commit()
                    progressed += 1
                    continue
                if entry_subject_changed:
                    # A different remote queue subject still exists.  Its
                    # ownership is unknown, so retain an active action until a
                    # terminal PR snapshot or later absence settles the risk.
                    action.status = "queued"
                    action.last_error = "merge_queue_entry_subject_changed"
                    action.github_queue_entry_id = entry.id
                else:
                    # Two fresh reads proved that the queue effect disappeared.
                    # Keep a durable absence receipt so ambiguous legacy rows
                    # do not rejoin the recovery set.
                    action.status = "failed"
                    action.last_error = "merge_queue_remote_absence_proven:open"
                    action.github_queue_entry_id = None
                    action.completed_at = datetime.utcnow()
                action.merge_group_sha = None
                action.merge_group_ref = None
                action.lease_token = None
                action.lease_expires_at = None
                run.status = "paused"
                run.pause_reason = action.last_error
                run.state_version += 1
                await db.commit()
                continue

            if entry.state in _QUEUE_ENTRY_BLOCKED_STATES:
                repo, action, run, review = (
                    await _relock_queue_effect_generation(
                        db,
                        repo_id=repo_id,
                        action_id=action_id_candidate,
                        run_id=run_id,
                        review_id=review_id,
                        expected_generation=expected_generation,
                    )
                )
                if action is None:
                    continue
                # The exact remote queue entry still exists and can become
                # mergeable later. Keep the Action active and expose the block.
                action.status = "queued"
                action.last_error = f"merge_queue_entry_{entry.state.lower()}"
                action.github_queue_entry_id = entry.id
                action.merge_group_sha = None
                action.merge_group_ref = None
                run.status = "paused"
                run.pause_reason = action.last_error
                run.state_version += 1
                await db.commit()
                continue

            entry_changed = prior_entry_id != entry.id
            try:
                merge_group = await _read_merge_group_ref(
                    queue_repo_name,
                    default_branch=queue_base_ref,
                    pr_number=queue_pr_number,
                )
            except ValueError as exc:
                repo, action, run, review = (
                    await _relock_queue_effect_generation(
                        db,
                        repo_id=repo_id,
                        action_id=action_id_candidate,
                        run_id=run_id,
                        review_id=review_id,
                        expected_generation=expected_generation,
                    )
                )
                if action is None:
                    continue
                action.github_queue_entry_id = entry.id
                action.status = "queued"
                action.last_error = f"merge_group_ref_invalid:{str(exc)[:300]}"
                run.status = "paused"
                run.pause_reason = "merge_group_ref_invalid"
                run.state_version += 1
                await db.commit()
                continue
            except Exception as exc:
                repo, action, run, review = (
                    await _relock_queue_effect_generation(
                        db,
                        repo_id=repo_id,
                        action_id=action_id_candidate,
                        run_id=run_id,
                        review_id=review_id,
                        expected_generation=expected_generation,
                    )
                )
                if action is None:
                    continue
                action.github_queue_entry_id = entry.id
                action.last_error = (
                    f"merge_group_ref_read_failed:{type(exc).__name__}:"
                    f"{str(exc)[:300]}"
                )
                await db.commit()
                continue

            if merge_group is None:
                repo, action, run, review = (
                    await _relock_queue_effect_generation(
                        db,
                        repo_id=repo_id,
                        action_id=action_id_candidate,
                        run_id=run_id,
                        review_id=review_id,
                        expected_generation=expected_generation,
                    )
                )
                if action is None:
                    continue
                changed = (
                    prior_action_status != "queued"
                    or prior_group_sha is not None
                    or entry_changed
                )
                action.github_queue_entry_id = entry.id
                action.status = "queued"
                action.merge_group_sha = None
                action.merge_group_ref = None
                action.ci_status = None
                action.ci_details = None
                action.last_error = None
                run.status = "merge_queued"
                run.pause_reason = None
                if changed:
                    run.state_version += 1
                    progressed += 1
                await db.commit()
                continue

            merge_sha, merge_ref = merge_group
            group_changed = (
                prior_group_sha != merge_sha
                or prior_group_ref != merge_ref
                or prior_action_status != "checking"
                or entry_changed
            )
            try:
                ci_status, summary, details = await fetch_exact_head_ci(
                    queue_repo_name,
                    merge_sha,
                    queue_required_checks,
                )
            except Exception as exc:
                repo, action, run, review = (
                    await _relock_queue_effect_generation(
                        db,
                        repo_id=repo_id,
                        action_id=action_id_candidate,
                        run_id=run_id,
                        review_id=review_id,
                        expected_generation=expected_generation,
                    )
                )
                if action is None:
                    continue
                action.github_queue_entry_id = entry.id
                action.merge_group_sha = merge_sha
                action.merge_group_ref = merge_ref
                action.status = "checking"
                if group_changed:
                    action.ci_status = "pending"
                    action.ci_details = None
                    run.status = "merge_group_checking"
                    run.pause_reason = None
                    run.state_version += 1
                action.last_error = (
                    "merge_group_ci_read_failed:"
                    f"{type(exc).__name__}:{str(exc)[:300]}"
                )
                await db.commit()
                if group_changed:
                    progressed += 1
                continue

            repo, action, run, review = (
                await _relock_queue_effect_generation(
                    db,
                    repo_id=repo_id,
                    action_id=action_id_candidate,
                    run_id=run_id,
                    review_id=review_id,
                    expected_generation=expected_generation,
                )
            )
            if action is None:
                continue
            action.github_queue_entry_id = entry.id
            action.merge_group_sha = merge_sha
            action.merge_group_ref = merge_ref
            action.status = "checking"
            if group_changed:
                action.ci_status = "pending"
                action.ci_details = None
                run.status = "merge_group_checking"
                run.pause_reason = None
                run.state_version += 1
            action.ci_status = ci_status
            action.ci_details = details
            action.last_error = None
            if ci_status == "failed":
                conclusions = {
                    item.get("conclusion")
                    for item in details.get("observed", [])
                    if isinstance(item, dict) and item.get("state") == "failed"
                }
                infrastructure = bool(conclusions & {
                    "cancelled", "timed_out", "startup_failure", "stale",
                    "action_required",
                })
                # CI failure does not remove the exact remote queue entry.
                # Keep the Action active while projecting the distinct code
                # or infrastructure failure through last_error/Run/Review.
                action.status = "checking"
                action.last_error = (
                    "merge_group_infrastructure_failed:" if infrastructure
                    else "merge_group_code_failed:"
                ) + summary[:500]
                if infrastructure:
                    run.status = "paused"
                    run.pause_reason = "merge_group_infrastructure_failed"
                else:
                    review.ci_status = "failed"
                    review.ci_summary = f"Merge group {merge_sha}: {summary}"
                    review.ci_details = {
                        "subject_kind": "merge_group",
                        "merge_group_sha": merge_sha,
                        **details,
                    }
                    await db.flush()
                    from backend.services.pr_monitor_loop import record_blocking_evidence

                    await record_blocking_evidence(
                        db,
                        review_id=review.id,
                        reason_kind="merge_group_ci_failed",
                    )
            elif ci_status == "passed":
                run.status = "merge_group_passed"
                run.pause_reason = None
            await db.commit()
            progressed += 1
    return progressed
