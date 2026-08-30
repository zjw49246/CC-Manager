"""Transactional deletion of first-class Plans owned by one target Task.

The Plan tables intentionally use logical integer references for most of the
aggregate.  Relying on database cascades would therefore leave different
results on SQLite (where foreign keys may be disabled) and on PostgreSQL or
MySQL.  This module first locks and validates the complete aggregate, then
deletes the exact locked rows explicitly.  It never commits or rolls back;
the caller keeps the target Task generation fence and owns the transaction.

The caller must already hold the target Task lock and the Task's Capability
Invocation -> Execution -> ResumeOutbox locks.  The next database lock order
is Run -> Plan -> WorkerDispatchReceipt -> Step/RuntimeReceipt -> Input,
matching Plan answer/cancellation/completion recovery. Remaining children are
then locked in stable primary-key order.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from sqlalchemy import delete as sa_delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.capability import (
    CapabilityExecution,
    CapabilityInvocation,
    CapabilityResumeOutbox,
)
from backend.models.delivery import DeliveryCycle
from backend.models.instance import Instance
from backend.models.plan import (
    Plan,
    PlanApplication,
    PlanApplicationAttempt,
    PlanApplicationReceipt,
    PlanInputRequest,
    PlanLegacyTaskLink,
    PlanVersion,
)
from backend.models.plan_agent import (
    PlanAgentRun,
    PlanAgentRuntimeReceipt,
    PlanAgentStep,
    PlanAgentWorkerDispatchReceipt,
)
from backend.models.task import Task
from backend.services.plan_runtime_receipt import runtime_run_is_clean
from backend.services.worker_plan_dispatch import (
    WorkerPlanDispatchConflict,
    snapshot_worker_dispatch_receipt,
    worker_mirror_cleanup_is_clean,
)


_TERMINAL_RUN_STATUSES = frozenset({"completed", "failed", "cancelled"})
_TERMINAL_STEP_STATUSES = frozenset({"completed", "failed", "cancelled"})
_TERMINAL_INPUT_STATUSES = frozenset({"answered", "cancelled"})
_CLEAN_RUNTIME_STATUS = "cleaned"
_PLAN_EXECUTOR_KIND = "plan_agent"
_PLAN_HANDLE_KIND = "plan_agent_run"
_PLAN_OUTPUT_KIND = "plan_version"
_PLAN_HANDLE_GENERATION = 0


class PlanDeletionConflict(RuntimeError):
    """The target Plan graph cannot be proven safe to delete."""

    def __init__(
        self,
        code: str,
        message: str,
        **context: Any,
    ) -> None:
        self.code = code
        self.detail: dict[str, Any] = {
            "code": code,
            "message": message,
            **context,
        }
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class TargetPlanDeleteGraph:
    """Exact row identities locked for deletion with one target Task."""

    task_id: int
    plan_ids: tuple[int, ...]
    run_ids: tuple[int, ...]
    step_ids: tuple[int, ...]
    runtime_receipt_ids: tuple[int, ...]
    input_request_ids: tuple[int, ...]
    version_ids: tuple[int, ...]
    application_ids: tuple[int, ...]
    application_attempt_ids: tuple[int, ...]
    application_receipt_ids: tuple[int, ...]
    worker_dispatch_receipt_ids: tuple[int, ...]
    legacy_task_ids: tuple[int, ...]
    capability_invocation_ids: tuple[int, ...]
    capability_execution_ids: tuple[int, ...]
    capability_outbox_ids: tuple[int, ...]


def _conflict(code: str, message: str, **context: Any) -> PlanDeletionConflict:
    return PlanDeletionConflict(code, message, **context)


def _ids(rows: Iterable[Any]) -> tuple[int, ...]:
    return tuple(sorted({int(row.id) for row in rows}))


def _canonical_positive_ids(value: Any) -> tuple[int, ...] | None:
    if not isinstance(value, list):
        return None
    normalized: list[int] = []
    seen: set[int] = set()
    for item in value:
        if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
            return None
        if item in seen:
            return None
        seen.add(item)
        normalized.append(item)
    return tuple(normalized)


def _parse_handle_id(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0 or str(parsed) != value:
        return None
    return parsed


async def _lock_rows(db: AsyncSession, model, *predicates) -> list[Any]:
    if not predicates:
        return []
    return list(
        (
            await db.execute(
                select(model)
                .where(*predicates)
                .order_by(model.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )


def _active_in_memory_plan_runs() -> set[int]:
    """Return exact live/unreaped Run ids without importing the app eagerly."""

    from backend.services.plan_agent_runner import active_plan_run_ids

    active = set(active_plan_run_ids())
    main_module = sys.modules.get("backend.main")
    dispatcher = getattr(main_module, "dispatcher", None) if main_module else None
    for key, lifecycle in getattr(dispatcher, "_running_tasks", {}).items():
        if lifecycle is None or lifecycle.done():
            continue
        run_id = getattr(lifecycle, "_ccm_worker_plan_run_id", None)
        if isinstance(run_id, int) and not isinstance(run_id, bool) and run_id > 0:
            active.add(run_id)
            continue
        if isinstance(key, str) and key.startswith("worker-plan-"):
            try:
                run_id = int(key.removeprefix("worker-plan-"))
            except ValueError:
                continue
            if run_id > 0:
                active.add(run_id)
    return active


async def lock_target_plan_delete_graph(
    db: AsyncSession,
    task_id: int,
    *,
    capability_invocation_ids: set[int],
    capability_execution_ids: set[int],
    capability_outbox_ids: set[int],
) -> TargetPlanDeleteGraph | None:
    """Lock and validate all first-class Plans targeting ``task_id``.

    ``None`` means the Task owns no first-class Plan graph and is not a Plan
    delivery target. A historical ``execution_task_id`` pointer from an
    external Plan is deliberately not ownership: it remains as audit after the
    execution Task is deleted. Any ambiguous, active, corrupt, or otherwise
    externally referenced owned graph raises :class:`PlanDeletionConflict`.

    The function assumes the caller already locked ``Task(task_id)`` and the
    supplied Capability Core/outbox rows.  It deliberately performs no commit
    or rollback so the final Task delete CAS can atomically include this graph.
    """

    if isinstance(task_id, bool) or not isinstance(task_id, int) or task_id <= 0:
        raise ValueError("task_id must be a positive integer")
    invocation_ids = {
        int(value)
        for value in capability_invocation_ids
        if isinstance(value, int) and not isinstance(value, bool) and value > 0
    }
    execution_ids = {
        int(value)
        for value in capability_execution_ids
        if isinstance(value, int) and not isinstance(value, bool) and value > 0
    }
    if invocation_ids != capability_invocation_ids:
        raise ValueError("capability_invocation_ids must contain positive integers")
    if execution_ids != capability_execution_ids:
        raise ValueError("capability_execution_ids must contain positive integers")
    outbox_ids = {
        int(value)
        for value in capability_outbox_ids
        if isinstance(value, int) and not isinstance(value, bool) and value > 0
    }
    if outbox_ids != capability_outbox_ids:
        raise ValueError("capability_outbox_ids must contain positive integers")

    # The caller already owns this row's writer lock. Read it through the same
    # session so remote Plan mirrors can be checked against the Task's frozen
    # Worker route rather than trusting only mutually-consistent child rows.
    target_task = await db.get(Task, task_id, populate_existing=True)
    if target_task is None:
        raise _conflict(
            "plan_graph_changed",
            "The target Task disappeared while its Plan graph was being locked",
            task_id=task_id,
        )

    # Discover ids without a Plan lock.  Staging a related Plan first locks the
    # target Task, which the caller already owns.  We can therefore acquire all
    # existing Run locks first, matching the provider completion/recovery path.
    discovered_plan_ids = tuple(
        (
            await db.execute(
                select(Plan.id)
                .where(Plan.target_task_id == task_id)
                .order_by(Plan.id)
            )
        ).scalars()
    )
    # Pure legacy Task-shaped Runs share the same Run lock tier. Include them
    # in the stable primary-key lock set even though the caller validates and
    # deletes those rows separately after this first-class graph is known.
    run_predicates = [PlanAgentRun.plan_task_id == task_id]
    if discovered_plan_ids:
        run_predicates.append(PlanAgentRun.plan_id.in_(discovered_plan_ids))
    if execution_ids:
        run_predicates.append(
            PlanAgentRun.capability_execution_id.in_(execution_ids)
        )
    locked_runs = await _lock_rows(db, PlanAgentRun, or_(*run_predicates))

    plan_lock_ids = set(discovered_plan_ids)
    plan_lock_ids.update(
        run.plan_id
        for run in locked_runs
        if run.capability_execution_id in execution_ids and run.plan_id is not None
    )
    locked_plans = (
        await _lock_rows(db, Plan, Plan.id.in_(sorted(plan_lock_ids)))
        if plan_lock_ids
        else []
    )
    plans_by_id = {plan.id: plan for plan in locked_plans}
    plans = [
        plan for plan in locked_plans if plan.target_task_id == task_id
    ]
    plan_ids = {plan.id for plan in plans}

    refreshed_target_ids = set(
        (
            await db.execute(
                select(Plan.id)
                .where(Plan.target_task_id == task_id)
                .order_by(Plan.id)
            )
        ).scalars()
    )
    if refreshed_target_ids != plan_ids or set(discovered_plan_ids) != plan_ids:
        raise _conflict(
            "plan_graph_changed",
            "The target Task's Plan set changed while deletion was locking it",
            task_id=task_id,
        )

    worker_dispatch_predicates = [
        PlanAgentWorkerDispatchReceipt.target_task_id == task_id,
    ]
    if plan_ids:
        worker_dispatch_predicates.append(
            PlanAgentWorkerDispatchReceipt.plan_id.in_(sorted(plan_ids))
        )
    locked_run_ids = {run.id for run in locked_runs}
    if locked_run_ids:
        worker_dispatch_predicates.append(
            PlanAgentWorkerDispatchReceipt.run_id.in_(sorted(locked_run_ids))
        )
    worker_dispatch_receipts = await _lock_rows(
        db,
        PlanAgentWorkerDispatchReceipt,
        or_(*worker_dispatch_predicates),
    )

    owned_runs = [run for run in locked_runs if run.plan_id in plan_ids]
    run_ids = {run.id for run in owned_runs}
    runs_by_id = {run.id: run for run in owned_runs}

    # Lock every direct child or malformed cross-link, not just rows whose
    # nominal parent id is correct.  The validation below then distinguishes
    # the closed aggregate from an external/corrupt reference.
    steps = (
        await _lock_rows(
            db,
            PlanAgentStep,
            or_(
                PlanAgentStep.run_id.in_(sorted(run_ids)),
                PlanAgentStep.plan_id.in_(sorted(plan_ids)),
            ),
        )
        if run_ids or plan_ids
        else []
    )
    step_ids = {step.id for step in steps}
    steps_by_id = {step.id: step for step in steps}

    runtime_receipts = (
        await _lock_rows(
            db,
            PlanAgentRuntimeReceipt,
            or_(
                PlanAgentRuntimeReceipt.run_id.in_(sorted(run_ids)),
                PlanAgentRuntimeReceipt.step_id.in_(sorted(step_ids)),
            ),
        )
        if run_ids or step_ids
        else []
    )

    inputs = (
        await _lock_rows(
            db,
            PlanInputRequest,
            or_(
                PlanInputRequest.plan_id.in_(sorted(plan_ids)),
                PlanInputRequest.run_id.in_(sorted(run_ids)),
                PlanInputRequest.source_step_id.in_(sorted(step_ids)),
            ),
        )
        if plan_ids or run_ids or step_ids
        else []
    )
    input_ids = {item.id for item in inputs}
    inputs_by_id = {item.id: item for item in inputs}

    versions = (
        await _lock_rows(
            db,
            PlanVersion,
            or_(
                PlanVersion.plan_id.in_(sorted(plan_ids)),
                PlanVersion.produced_by_run_id.in_(sorted(run_ids)),
                PlanVersion.produced_by_step_id.in_(sorted(step_ids)),
                PlanVersion.reviewed_by_step_id.in_(sorted(step_ids)),
            ),
        )
        if plan_ids or run_ids or step_ids
        else []
    )
    version_ids = {version.id for version in versions}
    versions_by_id = {version.id: version for version in versions}

    # ``execution_task_id`` is an outbound historical pointer owned by an
    # external Plan aggregate.  Deleting the materialized execution Task must
    # leave that Plan/Application audit intact so resource APIs can report
    # ``execution_task_available=false``.  Only target ownership and links to
    # a Plan graph actually owned by this Task participate in its delete set.
    application_predicates = [PlanApplication.target_task_id == task_id]
    attempt_predicates = [PlanApplicationAttempt.target_task_id == task_id]
    if plan_ids:
        application_predicates.append(PlanApplication.plan_id.in_(sorted(plan_ids)))
        attempt_predicates.append(
            PlanApplicationAttempt.plan_id.in_(sorted(plan_ids))
        )
    if version_ids:
        application_predicates.append(
            PlanApplication.plan_version_id.in_(sorted(version_ids))
        )
        attempt_predicates.append(
            PlanApplicationAttempt.plan_version_id.in_(sorted(version_ids))
        )
    applications = await _lock_rows(
        db,
        PlanApplication,
        or_(*application_predicates),
    )
    attempts = await _lock_rows(
        db,
        PlanApplicationAttempt,
        or_(*attempt_predicates),
    )

    legacy_predicates = [PlanLegacyTaskLink.legacy_task_id == task_id]
    if plan_ids:
        legacy_predicates.append(PlanLegacyTaskLink.plan_id.in_(sorted(plan_ids)))
    if version_ids:
        legacy_predicates.append(
            PlanLegacyTaskLink.plan_version_id.in_(sorted(version_ids))
        )
    if run_ids:
        legacy_predicates.append(
            PlanLegacyTaskLink.plan_run_id.in_(sorted(run_ids))
        )
    legacy_links = list(
        (
            await db.execute(
                select(PlanLegacyTaskLink)
                .where(or_(*legacy_predicates))
                .order_by(PlanLegacyTaskLink.legacy_task_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars()
    )

    receipt_keys = {
        key
        for key in (
            *(application.application_receipt_key for application in applications),
            *(attempt.application_receipt_key for attempt in attempts),
        )
        if key is not None
    }
    receipt_candidates = list(
        (
            await db.execute(
                select(PlanApplicationReceipt).order_by(PlanApplicationReceipt.id)
            )
        ).scalars()
    )
    relevant_receipt_ids: list[int] = []
    for receipt in receipt_candidates:
        receipt_version_ids = _canonical_positive_ids(receipt.plan_version_ids)
        references_graph = bool(
            receipt_version_ids is not None
            and version_ids.intersection(receipt_version_ids)
        )
        if (
            receipt.target_task_id == task_id
            or receipt.receipt_key in receipt_keys
            or references_graph
        ):
            relevant_receipt_ids.append(receipt.id)
    receipts = (
        await _lock_rows(
            db,
            PlanApplicationReceipt,
            PlanApplicationReceipt.id.in_(relevant_receipt_ids),
        )
        if relevant_receipt_ids
        else []
    )
    receipts_by_key = {receipt.receipt_key: receipt for receipt in receipts}

    external_core_runs = [
        run
        for run in locked_runs
        if run.capability_execution_id in execution_ids and run.id not in run_ids
    ]
    if external_core_runs:
        run = external_core_runs[0]
        raise _conflict(
            "external_capability_reference",
            f"Capability Plan Run #{run.id} is outside the target Plan graph",
            run_id=run.id,
            plan_id=run.plan_id,
            capability_execution_id=run.capability_execution_id,
        )

    legacy_task_refs = {
        value
        for value in (
            *(run.plan_task_id for run in owned_runs),
            *(link.legacy_task_id for link in legacy_links),
        )
        if value is not None and value != task_id
    }
    existing_legacy_task_refs = (
        set(
            (
                await db.execute(
                    select(Task.id)
                    .where(Task.id.in_(sorted(legacy_task_refs)))
                    .order_by(Task.id)
                )
            ).scalars()
        )
        if legacy_task_refs
        else set()
    )

    # A Task with no related Plan can still be the target of an external Plan
    # application.  That is an external reference, not an empty graph.
    if not plans:
        if (
            applications
            or attempts
            or legacy_links
            or receipts
            or worker_dispatch_receipts
        ):
            raise _conflict(
                "external_plan_reference",
                "The Task is referenced by a Plan aggregate it does not own",
                task_id=task_id,
            )
        return None

    # ---- Closed aggregate and terminal runtime validation -----------------
    for plan in plans:
        if plan.worker_id != target_task.worker_id:
            raise _conflict(
                "external_plan_reference",
                f"Plan #{plan.id} has a Worker identity outside its target Task",
                task_id=task_id,
                task_worker_id=target_task.worker_id,
                plan_id=plan.id,
                plan_worker_id=plan.worker_id,
            )
        if plan.active_run_id is not None:
            raise _conflict(
                "active_plan_run",
                f"Plan #{plan.id} still owns active Run #{plan.active_run_id}",
                task_id=task_id,
                plan_id=plan.id,
                run_id=plan.active_run_id,
            )
        if plan.current_version_id is not None:
            current = versions_by_id.get(plan.current_version_id)
            if current is None or current.plan_id != plan.id:
                raise _conflict(
                    "invalid_plan_version_link",
                    f"Plan #{plan.id} has an invalid current Version",
                    plan_id=plan.id,
                    version_id=plan.current_version_id,
                )

    active_run_ids = _active_in_memory_plan_runs()
    for run in owned_runs:
        plan = plans_by_id[run.plan_id]
        if run.worker_id != plan.worker_id:
            raise _conflict(
                "external_plan_reference",
                f"Plan Run #{run.id} has a Worker identity outside its Plan",
                plan_id=plan.id,
                plan_worker_id=plan.worker_id,
                run_id=run.id,
                run_worker_id=run.worker_id,
            )
        if run.status not in _TERMINAL_RUN_STATUSES:
            raise _conflict(
                "active_plan_run",
                f"Plan Run #{run.id} is still {run.status}",
                plan_id=plan.id,
                run_id=run.id,
                status=run.status,
            )
        if (
            run.id in active_run_ids
            or run.instance_id is not None
            or run.last_execution_started_at is not None
        ):
            raise _conflict(
                "active_plan_runtime",
                f"Plan Run #{run.id} retains live or unreaped runtime evidence",
                plan_id=plan.id,
                run_id=run.id,
            )
        if run.open_input_request_id is not None:
            raise _conflict(
                "active_plan_input",
                f"Terminal Plan Run #{run.id} retains an open input pointer",
                plan_id=plan.id,
                run_id=run.id,
                input_request_id=run.open_input_request_id,
            )
        if run.plan_task_id in existing_legacy_task_refs:
            raise _conflict(
                "external_plan_reference",
                f"Plan Run #{run.id} is also owned by legacy Task #{run.plan_task_id}",
                plan_id=plan.id,
                run_id=run.id,
                legacy_task_id=run.plan_task_id,
            )
        if run.source_run_id is not None:
            source = runs_by_id.get(run.source_run_id)
            if source is None or source.plan_id != plan.id:
                raise _conflict(
                    "invalid_plan_run_link",
                    f"Plan Run #{run.id} has an external retry source",
                    plan_id=plan.id,
                    run_id=run.id,
                    source_run_id=run.source_run_id,
                )
        if run.result_version_id is not None:
            result = versions_by_id.get(run.result_version_id)
            if result is None or result.plan_id != plan.id:
                raise _conflict(
                    "invalid_plan_version_link",
                    f"Plan Run #{run.id} has an invalid result Version",
                    plan_id=plan.id,
                    run_id=run.id,
                    version_id=run.result_version_id,
                )
        if run.draft_step_id is not None:
            draft = steps_by_id.get(run.draft_step_id)
            if draft is None or draft.run_id != run.id or draft.plan_id != plan.id:
                raise _conflict(
                    "invalid_plan_step_link",
                    f"Plan Run #{run.id} has an invalid draft Step",
                    plan_id=plan.id,
                    run_id=run.id,
                    step_id=run.draft_step_id,
                )
        if run.base_version_id is not None and run.base_version_id not in version_ids:
            if plan.forked_from_version_id != run.base_version_id:
                raise _conflict(
                    "external_plan_reference",
                    f"Plan Run #{run.id} has an unexplained external base Version",
                    plan_id=plan.id,
                    run_id=run.id,
                    version_id=run.base_version_id,
                )
        cancellation_generation = run.cancellation_target_generation
        cancellation_generation_matches = (
            cancellation_generation is not None
            and run.generation == cancellation_generation + 1
        )
        capability_cancellation = (
            run.status == "cancelled"
            and run.run_type == "capability"
            and run.capability_execution_id is not None
            and run.worker_id is None
            and cancellation_generation_matches
        )
        worker_cancellation = (
            run.status == "cancelled"
            and run.run_type != "capability"
            and run.capability_execution_id is None
            and run.worker_id is not None
            and cancellation_generation_matches
        )
        if cancellation_generation is not None and not (
            capability_cancellation or worker_cancellation
        ):
            raise _conflict(
                "invalid_plan_cancellation",
                f"Plan Run #{run.id} has an inconsistent cancellation fence",
                plan_id=plan.id,
                run_id=run.id,
            )

    for receipt in worker_dispatch_receipts:
        plan = plans_by_id.get(receipt.plan_id)
        run = runs_by_id.get(receipt.run_id)
        if (
            plan is None
            or run is None
            or run.plan_id != plan.id
            or plan.target_task_id != task_id
            or receipt.target_task_id != task_id
            or receipt.worker_id != run.worker_id
            or receipt.worker_id != plan.worker_id
            or receipt.protocol != 1
        ):
            raise _conflict(
                "external_plan_reference",
                f"Worker dispatch receipt #{receipt.id} crosses the target Plan graph",
                worker_dispatch_receipt_id=receipt.id,
                plan_id=receipt.plan_id,
                run_id=receipt.run_id,
            )
        try:
            dispatch_snapshot = snapshot_worker_dispatch_receipt(receipt)
        except WorkerPlanDispatchConflict as exc:
            raise _conflict(
                "invalid_worker_plan_dispatch",
                f"Worker dispatch receipt #{receipt.id} has an invalid settlement proof",
                worker_dispatch_receipt_id=receipt.id,
                plan_id=receipt.plan_id,
                run_id=receipt.run_id,
            ) from exc
        if dispatch_snapshot.status != "settled":
            raise _conflict(
                "active_worker_plan_dispatch",
                f"Worker dispatch receipt #{receipt.id} is still {receipt.status}",
                worker_dispatch_receipt_id=receipt.id,
                plan_id=receipt.plan_id,
                run_id=receipt.run_id,
                status=receipt.status,
            )
    reverse_owners = (
        await _lock_rows(
            db,
            Instance,
            Instance.current_plan_run_id.in_(sorted(run_ids)),
        )
        if run_ids
        else []
    )
    if reverse_owners:
        owner = reverse_owners[0]
        raise _conflict(
            "active_plan_runtime",
            f"Plan Run #{owner.current_plan_run_id} still owns Instance #{owner.id}",
            run_id=owner.current_plan_run_id,
            instance_id=owner.id,
        )

    for step in steps:
        run = runs_by_id.get(step.run_id)
        if run is None or step.plan_id != run.plan_id:
            raise _conflict(
                "external_plan_reference",
                f"Plan Step #{step.id} crosses the target Plan graph",
                step_id=step.id,
                run_id=step.run_id,
                plan_id=step.plan_id,
            )
        if step.status not in _TERMINAL_STEP_STATUSES:
            raise _conflict(
                "active_plan_runtime",
                f"Plan Step #{step.id} is still {step.status}",
                step_id=step.id,
                run_id=step.run_id,
                status=step.status,
            )
        if step.plan_version_id is not None:
            version = versions_by_id.get(step.plan_version_id)
            if version is None or version.plan_id != run.plan_id:
                raise _conflict(
                    "invalid_plan_version_link",
                    f"Plan Step #{step.id} has an invalid Version",
                    step_id=step.id,
                    version_id=step.plan_version_id,
                )
        if step.input_request_id is not None:
            input_request = inputs_by_id.get(step.input_request_id)
            if (
                input_request is None
                or input_request.run_id != run.id
                or input_request.plan_id != run.plan_id
                or input_request.source_step_id != step.id
            ):
                raise _conflict(
                    "invalid_plan_input_link",
                    f"Plan Step #{step.id} has an invalid input request",
                    step_id=step.id,
                    input_request_id=step.input_request_id,
                )

    for receipt in runtime_receipts:
        run = runs_by_id.get(receipt.run_id)
        step = steps_by_id.get(receipt.step_id)
        if run is None or step is None or step.run_id != run.id:
            raise _conflict(
                "external_plan_reference",
                f"Runtime receipt #{receipt.id} crosses the target Plan graph",
                receipt_id=receipt.id,
                run_id=receipt.run_id,
                step_id=receipt.step_id,
            )
        if (
            receipt.status != _CLEAN_RUNTIME_STATUS
            or receipt.cleaned_at is None
            or receipt.run_generation != step.generation
        ):
            raise _conflict(
                "unclean_plan_runtime",
                f"Runtime receipt #{receipt.id} is not an exact cleanup proof",
                receipt_id=receipt.id,
                run_id=receipt.run_id,
                status=receipt.status,
            )

    # A list of individually clean receipts is not sufficient proof. Local
    # Runs require a complete per-attempt process receipt graph. Worker Runs
    # instead contain remote Step mirrors and must prove each imported Step
    # generation through a settled dispatch outcome; synthesizing Manager
    # process receipts for those remote attempts would be false evidence.
    for run in owned_runs:
        run_steps = [step for step in steps if step.run_id == run.id]
        run_runtime_receipts = [
            receipt for receipt in runtime_receipts if receipt.run_id == run.id
        ]
        run_dispatch_receipts = [
            receipt for receipt in worker_dispatch_receipts if receipt.run_id == run.id
        ]
        if run.worker_id is None:
            clean = not any(
                step.worker_id is not None or step.worker_step_id is not None
                for step in run_steps
            ) and await runtime_run_is_clean(db, run_id=run.id)
        else:
            clean = worker_mirror_cleanup_is_clean(
                plan=plans_by_id[run.plan_id],
                run=run,
                steps=run_steps,
                input_requests=[
                    input_request
                    for input_request in inputs
                    if input_request.run_id == run.id
                ],
                versions=[
                    version
                    for version in versions
                    if version.produced_by_run_id == run.id
                ],
                runtime_receipts=run_runtime_receipts,
                dispatch_receipts=run_dispatch_receipts,
            )
        if not clean:
            raise _conflict(
                "unclean_plan_runtime",
                f"Plan Run #{run.id} does not have a complete cleanup proof",
                run_id=run.id,
            )

    for input_request in inputs:
        run = runs_by_id.get(input_request.run_id)
        source = steps_by_id.get(input_request.source_step_id)
        if (
            run is None
            or input_request.plan_id != run.plan_id
            or source is None
            or source.run_id != run.id
            or source.plan_id != run.plan_id
            or source.input_request_id != input_request.id
        ):
            raise _conflict(
                "external_plan_reference",
                f"Input request #{input_request.id} crosses the target Plan graph",
                input_request_id=input_request.id,
            )
        if input_request.status not in _TERMINAL_INPUT_STATUSES:
            raise _conflict(
                "active_plan_input",
                f"Plan input request #{input_request.id} is still {input_request.status}",
                input_request_id=input_request.id,
                status=input_request.status,
            )

    for version in versions:
        if version.plan_id not in plan_ids:
            raise _conflict(
                "external_plan_reference",
                f"Plan Version #{version.id} crosses the target Plan graph",
                version_id=version.id,
                plan_id=version.plan_id,
            )
        if version.produced_by_run_id is not None:
            producer_run = runs_by_id.get(version.produced_by_run_id)
            if producer_run is None or producer_run.plan_id != version.plan_id:
                raise _conflict(
                    "invalid_plan_run_link",
                    f"Plan Version #{version.id} has an invalid producer Run",
                    version_id=version.id,
                    run_id=version.produced_by_run_id,
                )
        if version.produced_by_step_id is not None:
            producer = steps_by_id.get(version.produced_by_step_id)
            if (
                producer is None
                or producer.plan_id != version.plan_id
                or producer.plan_version_id != version.id
                or (
                    version.produced_by_run_id is not None
                    and producer.run_id != version.produced_by_run_id
                )
            ):
                raise _conflict(
                    "invalid_plan_step_link",
                    f"Plan Version #{version.id} has an invalid producer Step",
                    version_id=version.id,
                    step_id=version.produced_by_step_id,
                )
        if version.reviewed_by_step_id is not None:
            reviewer = steps_by_id.get(version.reviewed_by_step_id)
            if reviewer is None or reviewer.plan_id != version.plan_id:
                raise _conflict(
                    "invalid_plan_step_link",
                    f"Plan Version #{version.id} has an invalid reviewer Step",
                    version_id=version.id,
                    step_id=version.reviewed_by_step_id,
                )
        for field, linked_id in (
            ("parent_version_id", version.parent_version_id),
            ("superseded_by_version_id", version.superseded_by_version_id),
        ):
            if linked_id is None:
                continue
            linked = versions_by_id.get(linked_id)
            if linked is None or linked.plan_id not in plan_ids:
                raise _conflict(
                    "external_plan_reference",
                    f"Plan Version #{version.id} has an external {field}",
                    version_id=version.id,
                    linked_version_id=linked_id,
                )

    # ---- Plan application and delivery outbox validation ------------------
    # Import the canonical state set lazily: plan_service is a large module,
    # while TaskQueue imports this helper on its deletion-only path.
    from backend.services.plan_service import TERMINAL_PLAN_DELIVERY_STATUSES

    applications_by_receipt: dict[str, set[int]] = {}
    for application in applications:
        version = versions_by_id.get(application.plan_version_id)
        if (
            application.plan_id not in plan_ids
            or version is None
            or version.plan_id != application.plan_id
            or application.application_type != "chat_message"
            or application.target_task_id != task_id
            or application.execution_task_id is not None
        ):
            raise _conflict(
                "external_plan_reference",
                f"Plan application #{application.id} crosses the target Plan graph",
                application_id=application.id,
            )
        if application.application_receipt_key is not None:
            applications_by_receipt.setdefault(
                application.application_receipt_key, set()
            ).add(application.plan_version_id)

    attempts_by_receipt: dict[str, set[int]] = {}
    for attempt in attempts:
        version = versions_by_id.get(attempt.plan_version_id)
        if (
            attempt.plan_id not in plan_ids
            or version is None
            or version.plan_id != attempt.plan_id
            or attempt.application_type != "chat_message"
            or attempt.target_task_id != task_id
            or attempt.execution_task_id is not None
        ):
            raise _conflict(
                "external_plan_reference",
                f"Plan application attempt #{attempt.id} crosses the target Plan graph",
                application_attempt_id=attempt.id,
            )
        attempts_by_receipt.setdefault(attempt.application_receipt_key, set()).add(
            attempt.plan_version_id
        )

    for receipt_key in receipt_keys:
        if receipt_key not in receipts_by_key:
            raise _conflict(
                "invalid_plan_delivery",
                f"Plan application receipt {receipt_key!r} is missing",
                receipt_key=receipt_key,
            )
    for receipt in receipts:
        normalized_versions = _canonical_positive_ids(receipt.plan_version_ids)
        if (
            receipt.target_task_id != task_id
            or normalized_versions is None
            or not normalized_versions
            or not set(normalized_versions).issubset(version_ids)
            or receipt.delivery_status not in TERMINAL_PLAN_DELIVERY_STATUSES
        ):
            raise _conflict(
                "active_or_external_plan_delivery",
                f"Plan delivery receipt {receipt.receipt_key!r} is not deletable",
                receipt_id=receipt.id,
                receipt_key=receipt.receipt_key,
                delivery_status=receipt.delivery_status,
            )
        referenced_versions = applications_by_receipt.get(
            receipt.receipt_key, set()
        ) | attempts_by_receipt.get(receipt.receipt_key, set())
        # A known prelaunch failure/cancellation may legitimately have no
        # application rows: Worker delivery receipts are staged before the
        # remote acknowledgement creates the Manager-side application audit.
        # ``launched`` is different—the durable applications are the proof
        # that every selected Version was consumed by that exact turn.
        if (
            receipt.delivery_status == "launched"
            and referenced_versions != set(normalized_versions)
        ):
            raise _conflict(
                "invalid_plan_delivery",
                f"Plan delivery receipt {receipt.receipt_key!r} lost its application audit",
                receipt_id=receipt.id,
                receipt_key=receipt.receipt_key,
            )

    for link in legacy_links:
        run = runs_by_id.get(link.plan_run_id) if link.plan_run_id is not None else None
        version = (
            versions_by_id.get(link.plan_version_id)
            if link.plan_version_id is not None
            else None
        )
        if (
            (
                link.legacy_task_id != task_id
                and link.legacy_task_id in existing_legacy_task_refs
            )
            or link.plan_id not in plan_ids
            or (link.plan_run_id is not None and run is None)
            or (run is not None and run.plan_id != link.plan_id)
            or (link.plan_version_id is not None and version is None)
            or (version is not None and version.plan_id != link.plan_id)
        ):
            raise _conflict(
                "external_plan_reference",
                f"Legacy Plan link for Task #{link.legacy_task_id} crosses the graph",
                legacy_task_id=link.legacy_task_id,
                plan_id=link.plan_id,
            )

    # ---- Capability Core bidirectional ownership --------------------------
    invocations = list(
        (
            await db.execute(
                select(CapabilityInvocation)
                .where(CapabilityInvocation.id.in_(sorted(invocation_ids)))
                .order_by(CapabilityInvocation.id)
                .execution_options(populate_existing=True)
            )
        ).scalars()
    ) if invocation_ids else []
    executions = list(
        (
            await db.execute(
                select(CapabilityExecution)
                .where(CapabilityExecution.id.in_(sorted(execution_ids)))
                .order_by(CapabilityExecution.id)
                .execution_options(populate_existing=True)
            )
        ).scalars()
    ) if execution_ids else []
    expected_outbox_predicates = [CapabilityResumeOutbox.task_id == task_id]
    if invocation_ids:
        expected_outbox_predicates.append(
            CapabilityResumeOutbox.invocation_id.in_(sorted(invocation_ids))
        )
    expected_outbox_ids = set(
        (
            await db.execute(
                select(CapabilityResumeOutbox.id)
                .where(or_(*expected_outbox_predicates))
                .order_by(CapabilityResumeOutbox.id)
            )
        ).scalars()
    )
    internal_outboxes = list(
        (
            await db.execute(
                select(CapabilityResumeOutbox)
                .where(CapabilityResumeOutbox.id.in_(sorted(outbox_ids)))
                .order_by(CapabilityResumeOutbox.id)
                .execution_options(populate_existing=True)
            )
        ).scalars()
    ) if outbox_ids else []
    invocations_by_id = {invocation.id: invocation for invocation in invocations}
    executions_by_id = {execution.id: execution for execution in executions}
    if set(invocations_by_id) != invocation_ids or set(executions_by_id) != execution_ids:
        raise _conflict(
            "capability_graph_changed",
            "The caller's locked Capability graph changed before Plan deletion",
            task_id=task_id,
        )
    if expected_outbox_ids != outbox_ids or _ids(internal_outboxes) != tuple(
        sorted(outbox_ids)
    ):
        raise _conflict(
            "capability_graph_changed",
            "The caller's locked Capability resume outbox graph changed before Plan deletion",
            task_id=task_id,
        )
    for invocation in invocations:
        if invocation.task_id != task_id:
            raise _conflict(
                "external_capability_reference",
                f"Capability Invocation #{invocation.id} belongs to another Task",
                capability_invocation_id=invocation.id,
                capability_task_id=invocation.task_id,
            )
        if invocation.result_kind == _PLAN_OUTPUT_KIND and not any(
            execution.invocation_id == invocation.id
            and execution.output_kind == _PLAN_OUTPUT_KIND
            and execution.output_id == invocation.result_id
            and execution.output_hash == invocation.result_hash
            for execution in executions
        ):
            raise _conflict(
                "invalid_capability_plan_result",
                f"Capability Invocation #{invocation.id} lost its Plan result execution",
                capability_invocation_id=invocation.id,
                version_id=invocation.result_id,
            )

    for outbox in internal_outboxes:
        invocation = invocations_by_id.get(outbox.invocation_id)
        if invocation is None or outbox.task_id != task_id:
            raise _conflict(
                "external_capability_reference",
                f"Capability resume outbox #{outbox.id} crosses the target Task graph",
                capability_resume_outbox_id=outbox.id,
                capability_invocation_id=outbox.invocation_id,
            )
        frozen_values = (
            outbox.invocation_terminal_status,
            outbox.invocation_result_kind,
            outbox.invocation_result_id,
            outbox.invocation_result_hash,
            outbox.invocation_error_code,
            outbox.invocation_error_message,
        )
        if any(value is not None for value in frozen_values) and frozen_values != (
            invocation.status,
            invocation.result_kind,
            invocation.result_id,
            invocation.result_hash,
            invocation.error_code,
            invocation.error_message,
        ):
            raise _conflict(
                "invalid_capability_resume_result",
                f"Capability resume outbox #{outbox.id} froze another Invocation outcome",
                capability_resume_outbox_id=outbox.id,
                capability_invocation_id=outbox.invocation_id,
            )

    capability_runs_by_execution: dict[int, PlanAgentRun] = {}
    for run in owned_runs:
        if run.capability_execution_id is None:
            if run.run_type == "capability":
                raise _conflict(
                    "invalid_capability_plan_link",
                    f"Capability Plan Run #{run.id} has no Core execution owner",
                    run_id=run.id,
                )
            continue
        if run.run_type != "capability" or run.capability_execution_id not in execution_ids:
            raise _conflict(
                "external_capability_reference",
                f"Plan Run #{run.id} belongs to an external Capability execution",
                run_id=run.id,
                capability_execution_id=run.capability_execution_id,
            )
        capability_runs_by_execution[run.capability_execution_id] = run

    from backend.services.plan_capability import plan_version_output_hash

    for execution in executions:
        invocation = invocations_by_id.get(execution.invocation_id)
        if invocation is None or invocation.task_id != task_id:
            raise _conflict(
                "external_capability_reference",
                f"Capability Execution #{execution.id} does not belong to the target Task",
                capability_execution_id=execution.id,
                capability_invocation_id=execution.invocation_id,
            )
        run = capability_runs_by_execution.get(execution.id)
        has_plan_identity = bool(
            execution.executor_kind == _PLAN_EXECUTOR_KIND
            or execution.handle_kind == _PLAN_HANDLE_KIND
            or execution.output_kind == _PLAN_OUTPUT_KIND
            or invocation.capability_key == "plan"
            or invocation.executor_kind == _PLAN_EXECUTOR_KIND
            or invocation.result_kind == _PLAN_OUTPUT_KIND
        )
        if not has_plan_identity:
            continue
        if invocation.capability_key != "plan" or (
            invocation.executor_kind != _PLAN_EXECUTOR_KIND
            or execution.executor_kind != _PLAN_EXECUTOR_KIND
        ):
            raise _conflict(
                "invalid_capability_plan_link",
                f"Capability Execution #{execution.id} has inconsistent Plan routing",
                capability_execution_id=execution.id,
            )
        if execution.handle_id is None and execution.handle_kind is None:
            if (
                run is not None
                or execution.output_id is not None
                or invocation.result_id is not None
            ):
                raise _conflict(
                    "invalid_capability_plan_link",
                    f"Capability Execution #{execution.id} lost its Plan Run handle",
                    capability_execution_id=execution.id,
                )
            continue
        handle_id = _parse_handle_id(execution.handle_id)
        if (
            execution.handle_kind != _PLAN_HANDLE_KIND
            or execution.handle_generation != _PLAN_HANDLE_GENERATION
            or handle_id is None
            or run is None
            or handle_id != run.id
            or run.capability_execution_id != execution.id
        ):
            raise _conflict(
                "invalid_capability_plan_link",
                f"Capability Execution #{execution.id} has an invalid Plan Run handle",
                capability_execution_id=execution.id,
                run_id=handle_id,
            )

        execution_result = (
            execution.output_kind,
            execution.output_id,
            execution.output_hash,
        )
        invocation_result = (
            invocation.result_kind,
            invocation.result_id,
            invocation.result_hash,
        )
        if any(value is not None for value in (*execution_result, *invocation_result)):
            version = versions_by_id.get(execution.output_id)
            expected_hash = (
                plan_version_output_hash(version) if version is not None else None
            )
            if (
                execution.output_kind != _PLAN_OUTPUT_KIND
                or invocation.result_kind != _PLAN_OUTPUT_KIND
                or execution.output_id != invocation.result_id
                or execution.output_hash != invocation.result_hash
                or version is None
                or version.plan_id != run.plan_id
                or run.result_version_id != version.id
                or execution.output_hash != expected_hash
            ):
                raise _conflict(
                    "invalid_capability_plan_result",
                    f"Capability Execution #{execution.id} has an invalid Plan result",
                    capability_execution_id=execution.id,
                    run_id=run.id,
                    version_id=execution.output_id,
                )

    # Core rows outside the caller's already-locked Task aggregate must never
    # retain a handle or result into the graph being removed.
    if run_ids or version_ids:
        external_execution_predicates = []
        if run_ids:
            external_execution_predicates.append(
                (
                    CapabilityExecution.handle_kind == _PLAN_HANDLE_KIND
                ) & CapabilityExecution.handle_id.in_([str(value) for value in run_ids])
            )
        if version_ids:
            external_execution_predicates.append(
                (
                    CapabilityExecution.output_kind == _PLAN_OUTPUT_KIND
                ) & CapabilityExecution.output_id.in_(sorted(version_ids))
            )
        external_execution_id = await db.scalar(
            select(CapabilityExecution.id)
            .where(
                or_(*external_execution_predicates),
                CapabilityExecution.id.not_in(sorted(execution_ids)),
            )
            .order_by(CapabilityExecution.id)
            .limit(1)
        )
        if external_execution_id is not None:
            raise _conflict(
                "external_capability_reference",
                f"Capability Execution #{external_execution_id} references the Plan graph",
                capability_execution_id=external_execution_id,
            )
    if version_ids:
        external_invocation_id = await db.scalar(
            select(CapabilityInvocation.id)
            .where(
                CapabilityInvocation.result_kind == _PLAN_OUTPUT_KIND,
                CapabilityInvocation.result_id.in_(sorted(version_ids)),
                CapabilityInvocation.id.not_in(sorted(invocation_ids)),
            )
            .order_by(CapabilityInvocation.id)
            .limit(1)
        )
        if external_invocation_id is not None:
            raise _conflict(
                "external_capability_reference",
                f"Capability Invocation #{external_invocation_id} references the Plan graph",
                capability_invocation_id=external_invocation_id,
            )
        external_outbox_id = await db.scalar(
            select(CapabilityResumeOutbox.id)
            .where(
                CapabilityResumeOutbox.invocation_result_kind == _PLAN_OUTPUT_KIND,
                CapabilityResumeOutbox.invocation_result_id.in_(sorted(version_ids)),
                CapabilityResumeOutbox.invocation_id.not_in(sorted(invocation_ids)),
            )
            .order_by(CapabilityResumeOutbox.id)
            .limit(1)
        )
        if external_outbox_id is not None:
            raise _conflict(
                "external_capability_reference",
                f"Capability resume outbox #{external_outbox_id} references the Plan graph",
                capability_resume_outbox_id=external_outbox_id,
            )

    # ---- Remaining inbound references -------------------------------------
    external_plan_id = await db.scalar(
        select(Plan.id)
        .where(
            Plan.id.not_in(sorted(plan_ids)),
            or_(
                Plan.current_version_id.in_(sorted(version_ids)),
                Plan.forked_from_version_id.in_(sorted(version_ids)),
                Plan.active_run_id.in_(sorted(run_ids)),
            ),
        )
        .order_by(Plan.id)
        .limit(1)
    )
    if external_plan_id is not None:
        raise _conflict(
            "external_plan_reference",
            f"Plan #{external_plan_id} references the target Plan graph",
            plan_id=external_plan_id,
        )

    external_run_predicates = [PlanAgentRun.source_run_id.in_(sorted(run_ids))]
    if version_ids:
        external_run_predicates.extend(
            [
                PlanAgentRun.base_version_id.in_(sorted(version_ids)),
                PlanAgentRun.result_version_id.in_(sorted(version_ids)),
            ]
        )
    if step_ids:
        external_run_predicates.append(
            PlanAgentRun.draft_step_id.in_(sorted(step_ids))
        )
    if input_ids:
        external_run_predicates.append(
            PlanAgentRun.open_input_request_id.in_(sorted(input_ids))
        )
    external_run_id = await db.scalar(
        select(PlanAgentRun.id)
        .where(
            PlanAgentRun.id.not_in(sorted(run_ids)),
            or_(*external_run_predicates),
        )
        .order_by(PlanAgentRun.id)
        .limit(1)
    )
    if external_run_id is not None:
        raise _conflict(
            "external_plan_reference",
            f"Plan Run #{external_run_id} references the target Plan graph",
            run_id=external_run_id,
        )

    if version_ids:
        external_version_id = await db.scalar(
            select(PlanVersion.id)
            .where(
                PlanVersion.id.not_in(sorted(version_ids)),
                or_(
                    PlanVersion.parent_version_id.in_(sorted(version_ids)),
                    PlanVersion.superseded_by_version_id.in_(sorted(version_ids)),
                ),
            )
            .order_by(PlanVersion.id)
            .limit(1)
        )
        if external_version_id is not None:
            raise _conflict(
                "external_plan_reference",
                f"Plan Version #{external_version_id} references the target Plan graph",
                version_id=external_version_id,
            )

    delivery_predicates = []
    if version_ids:
        delivery_predicates.append(
            DeliveryCycle.plan_version_id.in_(sorted(version_ids))
        )
    if invocation_ids:
        delivery_predicates.extend(
            [
                DeliveryCycle.plan_invocation_id.in_(sorted(invocation_ids)),
                DeliveryCycle.review_invocation_id.in_(sorted(invocation_ids)),
            ]
        )
    if delivery_predicates:
        delivery_cycle = (
            await db.execute(
                select(DeliveryCycle.id, DeliveryCycle.status)
                .where(or_(*delivery_predicates))
                .order_by(DeliveryCycle.id)
                .limit(1)
            )
        ).first()
        if delivery_cycle is not None:
            raise _conflict(
                "external_delivery_reference",
                f"Delivery Cycle #{delivery_cycle.id} references the Plan graph",
                delivery_cycle_id=delivery_cycle.id,
                status=delivery_cycle.status,
            )

    return TargetPlanDeleteGraph(
        task_id=task_id,
        plan_ids=_ids(plans),
        run_ids=_ids(owned_runs),
        step_ids=_ids(steps),
        runtime_receipt_ids=_ids(runtime_receipts),
        input_request_ids=_ids(inputs),
        version_ids=_ids(versions),
        application_ids=_ids(applications),
        application_attempt_ids=_ids(attempts),
        application_receipt_ids=_ids(receipts),
        worker_dispatch_receipt_ids=_ids(worker_dispatch_receipts),
        legacy_task_ids=tuple(sorted(link.legacy_task_id for link in legacy_links)),
        capability_invocation_ids=tuple(sorted(invocation_ids)),
        capability_execution_ids=tuple(sorted(execution_ids)),
        capability_outbox_ids=tuple(sorted(outbox_ids)),
    )


async def _delete_exact_ids(
    db: AsyncSession,
    model,
    ids: Sequence[int],
    *,
    label: str,
) -> None:
    if not ids:
        return
    result = await db.execute(sa_delete(model).where(model.id.in_(ids)))
    if result.rowcount not in (-1, len(ids)):
        raise _conflict(
            "plan_graph_changed",
            f"The locked {label} rows changed before deletion",
            expected=len(ids),
            deleted=result.rowcount,
        )


async def delete_target_plan_graph(
    db: AsyncSession,
    graph: TargetPlanDeleteGraph,
) -> None:
    """Delete one previously locked graph without ending the transaction."""

    # Child/audit rows first.  The explicit order is portable to SQLite with
    # foreign keys disabled and to stricter deployments if FKs are enabled.
    await _delete_exact_ids(
        db,
        PlanApplicationAttempt,
        graph.application_attempt_ids,
        label="Plan application-attempt",
    )
    await _delete_exact_ids(
        db,
        PlanApplication,
        graph.application_ids,
        label="Plan application",
    )
    await _delete_exact_ids(
        db,
        PlanApplicationReceipt,
        graph.application_receipt_ids,
        label="Plan application-receipt",
    )
    if graph.legacy_task_ids:
        result = await db.execute(
            sa_delete(PlanLegacyTaskLink).where(
                PlanLegacyTaskLink.legacy_task_id.in_(graph.legacy_task_ids)
            )
        )
        if result.rowcount not in (-1, len(graph.legacy_task_ids)):
            raise _conflict(
                "plan_graph_changed",
                "The locked legacy Plan links changed before deletion",
                expected=len(graph.legacy_task_ids),
                deleted=result.rowcount,
            )
    await _delete_exact_ids(
        db,
        PlanAgentWorkerDispatchReceipt,
        graph.worker_dispatch_receipt_ids,
        label="Worker Plan dispatch-receipt",
    )
    await _delete_exact_ids(
        db,
        PlanAgentRuntimeReceipt,
        graph.runtime_receipt_ids,
        label="Plan runtime-receipt",
    )
    await _delete_exact_ids(
        db,
        PlanInputRequest,
        graph.input_request_ids,
        label="Plan input-request",
    )
    await _delete_exact_ids(
        db,
        PlanAgentStep,
        graph.step_ids,
        label="Plan Step",
    )
    await _delete_exact_ids(
        db,
        PlanVersion,
        graph.version_ids,
        label="Plan Version",
    )
    await _delete_exact_ids(
        db,
        PlanAgentRun,
        graph.run_ids,
        label="Plan Run",
    )
    await _delete_exact_ids(
        db,
        Plan,
        graph.plan_ids,
        label="Plan",
    )


__all__ = [
    "PlanDeletionConflict",
    "TargetPlanDeleteGraph",
    "delete_target_plan_graph",
    "lock_target_plan_delete_graph",
]
