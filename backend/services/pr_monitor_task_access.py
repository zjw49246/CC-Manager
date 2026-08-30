"""Authorization identities for PR Monitor-owned Tasks.

Reviewer, repair, and rebuttal Tasks are controller implementation records.
The only ordinary Task projection is the stable display Task owned by one
``PRMonitorRun``.  Its access is derived from the monitored repository's
ordinary Project, never from the internal ``PR-Monitor`` grouping Project or
from legacy Task/Project shares.
"""

from __future__ import annotations

from sqlalchemy import and_, or_, select


PR_MONITOR_DISPLAY_MARKER = "pr_monitor_display"
PR_MONITOR_DISPLAY_RUN_KEY = "pr_monitor_run_id"
PR_MONITOR_DISPLAY_REVIEW_KEY = "pr_monitor_review_id"


def pr_monitor_display_task_predicate(task_id):
    """Return the durable, non-tombstoned identity of a display Task."""

    from backend.models.pr_monitor import (
        MonitoredRepo,
        PRMonitorRun,
        PRMonitorTaskTombstone,
        PRReview,
    )
    from backend.models.task import Task
    from backend.models.delivery import DeliveryRun

    current_delivery_review = (
        select(PRReview.id)
        .where(
            PRReview.monitor_run_id == PRMonitorRun.id,
            PRReview.id == PRMonitorRun.current_review_id,
            PRReview.delivery_id.like("delivery:%"),
        )
        .correlate(PRMonitorRun)
        .exists()
    )
    delivery_run = (
        select(DeliveryRun.id)
        .where(DeliveryRun.pr_monitor_run_id == PRMonitorRun.id)
        .correlate(PRMonitorRun)
        .exists()
    )
    tombstoned = (
        select(PRMonitorTaskTombstone.task_id)
        .where(PRMonitorTaskTombstone.task_id == task_id)
        .correlate_except(PRMonitorTaskTombstone)
        .exists()
    )
    # ``ordinary_task_visibility_predicate`` passes the outer ``Task.id``
    # expression, while direct callers pass an integer.  In the latter case
    # the marker check must be an independent, ID-bound EXISTS; referencing
    # ``Task.metadata_`` directly would add the tasks table as an unjoined
    # FROM element and allow another display row to authorize this task.
    if isinstance(task_id, int):
        display_marker = (
            select(Task.id)
            .where(
                Task.id == task_id,
                Task.metadata_[PR_MONITOR_DISPLAY_MARKER].as_boolean().is_(True),
            )
            .correlate_except(Task)
            .exists()
        )
    else:
        display_marker = (
            Task.metadata_[PR_MONITOR_DISPLAY_MARKER].as_boolean().is_(True)
        )

    run_link = (
        select(PRMonitorRun.id)
        .join(MonitoredRepo, MonitoredRepo.id == PRMonitorRun.repo_id)
        .where(
            PRMonitorRun.display_task_id == task_id,
            display_marker,
            ~delivery_run,
            ~current_delivery_review,
        )
        .correlate(Task)
        .exists()
    )
    return and_(run_link, ~tombstoned)


def pr_monitor_owned_task_predicate(task_id):
    """Return the durable SQL identity for every PR Monitor child Task."""

    from backend.models.pr_monitor import (
        PRFindingAction,
        PRFindingRebuttal,
        PRMonitorTaskTombstone,
        PRReview,
        PRReviewerRun,
        PRMonitorRun,
    )
    from backend.models.task import Task

    # A display Task is staged before its Run link is assigned.  Treat the
    # marker as an owned controller row during that short transaction (and
    # after a crashed concurrent backfill) so it can never fall through into
    # the ordinary Task list while its durable link is incomplete.
    if isinstance(task_id, int):
        staged_display_marker = (
            select(Task.id)
            .where(
                Task.id == task_id,
                Task.metadata_[PR_MONITOR_DISPLAY_MARKER].as_boolean().is_(True),
            )
            .correlate_except(Task)
            .exists()
        )
    else:
        # ``ordinary_task_visibility_predicate`` passes the outer Task.id
        # expression.  Keep this as a direct column predicate; wrapping it in
        # a same-table subquery would accidentally self-correlate and mark
        # every row owned whenever any staged marker exists.
        staged_display_marker = (
            Task.metadata_[PR_MONITOR_DISPLAY_MARKER].as_boolean().is_(True)
        )

    return or_(
        select(PRReview.id)
        .where(PRReview.task_id == task_id)
        .correlate_except(PRReview)
        .exists(),
        select(PRReviewerRun.id)
        .where(PRReviewerRun.task_id == task_id)
        .correlate_except(PRReviewerRun)
        .exists(),
        select(PRFindingAction.id)
        .where(PRFindingAction.task_id == task_id)
        .correlate_except(PRFindingAction)
        .exists(),
        select(PRFindingRebuttal.id)
        .where(PRFindingRebuttal.task_id == task_id)
        .correlate_except(PRFindingRebuttal)
        .exists(),
        select(PRMonitorRun.id)
        .where(PRMonitorRun.display_task_id == task_id)
        .correlate_except(PRMonitorRun)
        .exists(),
        staged_display_marker,
        select(PRMonitorTaskTombstone.task_id)
        .where(PRMonitorTaskTombstone.task_id == task_id)
        .correlate_except(PRMonitorTaskTombstone)
        .exists(),
    )


async def is_pr_monitor_display_task(db, task) -> bool:
    """Check the stable display marker and Run link without trusting metadata."""

    task_id = getattr(task, "id", None)
    if isinstance(task_id, bool) or not isinstance(task_id, int) or task_id <= 0:
        return False
    linked = await db.scalar(
        select(task_id).where(pr_monitor_display_task_predicate(task_id)).limit(1)
    )
    return linked is not None


async def pr_monitor_display_project_id(db, task_id: int) -> int | None:
    """Return the ordinary Project governing one display Task's visibility."""

    from backend.models.pr_monitor import MonitoredRepo, PRMonitorRun

    return await db.scalar(
        select(MonitoredRepo.project_id)
        .join(PRMonitorRun, PRMonitorRun.repo_id == MonitoredRepo.id)
        .where(
            PRMonitorRun.display_task_id == task_id,
            MonitoredRepo.project_id.is_not(None),
        )
        .limit(1)
    )


async def is_pr_monitor_owned_task(db, task) -> bool:
    """Fail closed for linked and recognizably staged PR Monitor Tasks."""

    from backend.services.pr_review_runtime import (
        is_pr_review_fix_task,
        is_pr_review_task,
    )

    if is_pr_review_task(task) or is_pr_review_fix_task(task):
        return True
    task_id = getattr(task, "id", None)
    if isinstance(task_id, bool) or not isinstance(task_id, int):
        return True
    linked = await db.scalar(
        select(task_id).where(pr_monitor_owned_task_predicate(task_id)).limit(1)
    )
    return linked is not None
