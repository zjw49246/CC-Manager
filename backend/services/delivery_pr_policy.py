"""Frozen Delivery policy checks shared by PR Monitor publication paths.

``MonitoredRepo`` is mutable administration state.  Once a Delivery Run owns
an exact PR head, no later repository setting may broaden that Run's authority
to auto-repair or merge.  ``PRReview.delivery_id`` is the immutable bridge from
the PR workflow back to the Delivery Run and its hashed policy snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass
import re

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.delivery import DeliveryRun
from backend.models.pr_monitor import PRMonitorRun, PRReview
from backend.models.task import Task
from backend.services.delivery_service import value_hash


_DELIVERY_ID_RE = re.compile(r"delivery:([1-9][0-9]*):([0-9a-f]{40})\Z")
_DELIVERY_ID_PREFIX = "delivery:"


class DeliveryPRPolicyError(RuntimeError):
    """A review claims Delivery ownership but cannot prove its frozen policy."""


class DeliveryPREffectNotReady(DeliveryPRPolicyError):
    """The exact Delivery policy exists but its Monitor binding is not durable yet."""


@dataclass(frozen=True, slots=True)
class FrozenDeliveryPRPolicy:
    run_id: int
    policy_hash: str
    auto_merge: bool
    terminal: str
    wait_for_ci: bool
    required_checks: list[dict]
    strict_branch_protection: bool


def has_reserved_delivery_marker(review: PRReview | None) -> bool:
    """Return whether a Review claims CCM Delivery ownership.

    This deliberately checks only the reserved namespace.  Validating the
    marker is the job of :func:`frozen_delivery_pr_policy`; legacy mutation
    paths must fail closed even when the marker is malformed or points at a
    missing Run.
    """

    return bool(
        review is not None
        and isinstance(review.delivery_id, str)
        and review.delivery_id.startswith(_DELIVERY_ID_PREFIX)
    )


async def legacy_pr_effect_is_forbidden(
    db: AsyncSession,
    *,
    review: PRReview | None = None,
    monitor_run: PRMonitorRun | None = None,
    task: Task | None = None,
) -> bool:
    """Return whether a PR subject is owned by the Delivery controller.

    A legacy PR action is forbidden when any durable ownership edge proves
    Delivery control: the reserved Review marker, a DeliveryRun-to-monitor or
    DeliveryRun-to-developer relation, or an active repository/PR lifecycle.
    The redundant edges are intentional.  Publisher adoption and controller
    binding are separate transactions, so checking only one nullable foreign
    key would expose a window where a legacy repair/merge action could start.
    A terminal historical lifecycle does not own every later ordinary Monitor
    Run for that repo/PR unless an exact durable edge still links the two.

    Callers that are about to mutate state should invoke this while holding
    their normal repository/review writer fence.  A malformed ``delivery:``
    marker returns ``True`` without trying to recover mutable repository
    policy.
    """

    reviews = [review] if review is not None else []
    if (
        monitor_run is not None
        and monitor_run.current_review_id is not None
        and (review is None or review.id != monitor_run.current_review_id)
    ):
        current_review = await db.get(
            PRReview,
            monitor_run.current_review_id,
            populate_existing=True,
        )
        if current_review is not None:
            reviews.append(current_review)
    if any(has_reserved_delivery_marker(item) for item in reviews):
        return True

    task_ids: set[int] = set()
    if task is not None:
        if task.mode == "delivery_loop" or task.delivery_run_id is not None:
            return True
        task_ids.add(task.id)
    if monitor_run is not None and monitor_run.developer_task_id is not None:
        task_ids.add(monitor_run.developer_task_id)
    task_ids.update(
        item.task_id for item in reviews if item.task_id is not None
    )
    if task_ids:
        delivery_task = (
            await db.execute(
                select(Task.id)
                .where(
                    Task.id.in_(task_ids),
                    or_(
                        Task.mode == "delivery_loop",
                        Task.delivery_run_id.is_not(None),
                    ),
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        if delivery_task is not None:
            return True

    exact_run_conditions = []
    subject_run_conditions = []
    if monitor_run is not None:
        exact_run_conditions.append(
            DeliveryRun.pr_monitor_run_id == monitor_run.id
        )
        subject_run_conditions.append(
            and_(
                DeliveryRun.monitored_repo_id == monitor_run.repo_id,
                DeliveryRun.pr_number == monitor_run.pr_number,
            )
        )
    for item in reviews:
        if item.monitor_run_id is not None:
            exact_run_conditions.append(
                DeliveryRun.pr_monitor_run_id == item.monitor_run_id
            )
        subject_run_conditions.append(
            and_(
                DeliveryRun.monitored_repo_id == item.repo_id,
                DeliveryRun.pr_number == item.pr_number,
            )
        )
    for task_id in task_ids:
        exact_run_conditions.append(DeliveryRun.developer_task_id == task_id)
    if not exact_run_conditions and not subject_run_conditions:
        return False

    run_conditions = list(exact_run_conditions)
    if subject_run_conditions:
        # Repository/PR is only a crash-gap ownership hint.  Once that
        # Delivery lifecycle is terminal, a later ordinary Monitor Run may
        # legitimately reuse the same PR number; only an exact immutable edge
        # above may keep blocking it.  Treat malformed half-terminal rows as
        # active so ambiguous ownership still fails closed.
        run_conditions.append(and_(
            or_(
                DeliveryRun.activity != "terminal",
                DeliveryRun.outcome.is_(None),
            ),
            or_(*subject_run_conditions),
        ))
    return (
        await db.execute(
            select(DeliveryRun.id).where(or_(*run_conditions)).limit(1)
        )
    ).scalar_one_or_none() is not None


async def frozen_delivery_pr_policy(
    db: AsyncSession,
    review: PRReview,
    *,
    monitor_run_id: int | None = None,
    require_effect_ready: bool = False,
    require_thread_resolution_gate: bool = False,
) -> FrozenDeliveryPRPolicy | None:
    """Return the validated policy for a Delivery-owned review.

    ``None`` means the review is an ordinary PR Monitor review.  Historically
    this column also stores opaque GitHub webhook delivery ids, so only the
    reserved ``delivery:`` namespace asserts Delivery ownership.  A malformed
    value inside that namespace fails closed: callers must not silently fall
    back to mutable repository policy after ownership was asserted.
    ``pr_monitor_run_id`` is allowed to be null during the publisher's
    create/attach transaction.  GitHub effect callers must pass
    ``require_effect_ready=True``; that stricter boundary accepts only the
    exact, active ``monitoring/waiting`` Run after its Monitor/Review binding
    is durable.  The fixed-thread zero Gate additionally passes
    ``require_thread_resolution_gate=True`` so it validates the exact
    pre-publication ``resolving_fixed_threads`` state without making that
    state eligible for a GitHub publication effect.
    """

    delivery_id = review.delivery_id
    if delivery_id is None:
        return None
    if not isinstance(delivery_id, str):
        raise DeliveryPRPolicyError("Delivery review id is not text")
    if not delivery_id.startswith(_DELIVERY_ID_PREFIX):
        return None
    match = _DELIVERY_ID_RE.fullmatch(delivery_id)
    if match is None:
        raise DeliveryPRPolicyError("Delivery review id is malformed")

    run_id = int(match.group(1))
    marker_head = match.group(2)
    run = await db.get(DeliveryRun, run_id, populate_existing=True)
    if run is None:
        raise DeliveryPRPolicyError("Delivery review lost its owning Run")
    if (
        run.monitored_repo_id != review.repo_id
        or review.base_ref != run.base_branch
        or run.base_sha != review.base_sha
        or run.head_sha != marker_head
        or review.head_sha != marker_head
        or (run.pr_number is not None and run.pr_number != review.pr_number)
        or (
            monitor_run_id is not None
            and run.pr_monitor_run_id not in (None, monitor_run_id)
        )
    ):
        raise DeliveryPRPolicyError(
            "Delivery review no longer matches its Run/PR/base/head subject"
        )

    if require_thread_resolution_gate and not require_effect_ready:
        raise DeliveryPRPolicyError(
            "Thread-resolution Gate requires the effect-ready owner fence"
        )
    if require_effect_ready:
        if (
            run.outcome is None
            and run.phase == "publishing"
            and run.activity == "running"
            and run.pr_monitor_run_id is None
        ):
            # Review creation/attachment and the Delivery Run binding are two
            # durable transactions.  A very fast reviewer (or restart between
            # them) must wait instead of turning that normal commit gap into a
            # terminal Review error.
            raise DeliveryPREffectNotReady(
                "Delivery Monitor binding is not durable yet"
            )
        if (
            monitor_run_id is None
            or review.monitor_run_id != monitor_run_id
            or run.pr_monitor_run_id != monitor_run_id
            or run.pr_number != review.pr_number
            or run.phase != "monitoring"
            or run.activity != "waiting"
            or run.outcome is not None
        ):
            raise DeliveryPRPolicyError(
                "Delivery Run is not the active bound Monitor owner"
            )
        monitor = await db.get(
            PRMonitorRun,
            monitor_run_id,
            populate_existing=True,
        )
        required_monitor_status = (
            "resolving_fixed_threads"
            if require_thread_resolution_gate
            else "reviewing"
        )
        if (
            monitor is None
            or monitor.repo_id != review.repo_id
            or monitor.pr_number != review.pr_number
            or monitor.status != required_monitor_status
            or monitor.current_review_id != review.id
            or monitor.current_base_sha != review.base_sha
            or monitor.current_head_sha != review.head_sha
        ):
            raise DeliveryPRPolicyError(
                "Delivery Monitor/Review subject is not the exact active binding"
            )

    policy = run.policy_snapshot
    if not isinstance(policy, dict) or value_hash(policy) != run.policy_hash:
        raise DeliveryPRPolicyError("Delivery policy snapshot hash is invalid")
    auto_merge = policy.get("auto_merge")
    terminal = policy.get("terminal")
    # Historical Runs without this field retain the strict contract.
    strict_branch_protection = policy.get("strict_branch_protection", True)
    monitor_policy = policy.get("pr_monitor")
    wait_for_ci = (
        monitor_policy.get("wait_for_ci")
        if isinstance(monitor_policy, dict)
        else None
    )
    required_checks = (
        monitor_policy.get("required_checks")
        if isinstance(monitor_policy, dict)
        else None
    )
    if (
        type(auto_merge) is not bool
        or terminal
        != ("merged" if auto_merge else "ready_to_merge")
        or not isinstance(monitor_policy, dict)
        or monitor_policy.get("repo_id") != review.repo_id
        or monitor_policy.get("review_mode") != "panel"
        or type(wait_for_ci) is not bool
        or type(strict_branch_protection) is not bool
        or not isinstance(required_checks, list)
        or any(not isinstance(item, dict) for item in required_checks)
        or bool(wait_for_ci) != bool(required_checks)
        or (auto_merge and not wait_for_ci)
    ):
        raise DeliveryPRPolicyError(
            "Delivery policy has an invalid merge terminal"
        )
    return FrozenDeliveryPRPolicy(
        run_id=run.id,
        policy_hash=run.policy_hash,
        auto_merge=auto_merge,
        terminal=terminal,
        wait_for_ci=wait_for_ci,
        required_checks=required_checks,
        strict_branch_protection=strict_branch_protection,
    )


__all__ = [
    "DeliveryPREffectNotReady",
    "DeliveryPRPolicyError",
    "FrozenDeliveryPRPolicy",
    "frozen_delivery_pr_policy",
    "has_reserved_delivery_marker",
    "legacy_pr_effect_is_forbidden",
]
