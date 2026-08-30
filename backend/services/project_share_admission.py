"""Fail-closed admission fence for making a Project visible to other users."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.models.instance import Instance
from backend.models.plan import Plan
from backend.models.plan_agent import PlanAgentRun, PlanAgentRuntimeReceipt
from backend.models.project import Project
from backend.models.task import Task
from backend.models.task_share import ProjectShare
from backend.models.sub_agent import SubAgentSession

if TYPE_CHECKING:
    from backend.services.instance_manager import InstanceManager


_ACTIVE_LOCAL_TASK_STATUSES = frozenset({"in_progress", "executing"})
_ACTIVE_LOCAL_PLAN_RUN_STATUSES = frozenset({
    "queued",
    "running",
    "waiting_user",
    "cancelling",
})
_PROVIDER_CAPABLE_AUX_STATUSES = frozenset({"running", "sleeping"})


class ProjectShareAdmissionError(ValueError):
    """The Project cannot be shared until local Agent ownership is quiescent."""


async def project_has_active_share(
    db: AsyncSession,
    project_id: int,
) -> bool:
    """Return legacy cross-CCM outbound Project sharing state.

    TeamProjectShare is a local authorization grant. It does not change the
    Project's execution trust boundary and must not disable local Agents.
    """
    feishu_share = await db.scalar(
        select(ProjectShare.id)
        .where(
            ProjectShare.project_id == project_id,
            ProjectShare.status == "active",
        )
        .limit(1)
        .with_for_update()
    )
    return feishu_share is not None


async def lock_project_share_authority(
    db: AsyncSession,
    project_id: int,
) -> Project:
    """Take the Project writer fence used by every outbound share path.

    The no-op UPDATE is intentional. ``FOR UPDATE`` is ignored by SQLite,
    while this write serializes both SQLite and row-locking databases.
    SQLAlchemy's MySQL dialect enables ``CLIENT_FOUND_ROWS`` for matched-row
    semantics, but the authoritative reload below also avoids treating a
    driver-specific zero rowcount for an unchanged row as a false 404.
    """

    locked = await db.execute(
        update(Project)
        .where(Project.id == project_id)
        .values(id=Project.id)
    )
    project = await db.get(Project, project_id, populate_existing=True)
    if project is None:
        raise ValueError(f"Project {project_id} not found")
    if locked.rowcount not in {0, 1}:
        raise ProjectShareAdmissionError(
            "Could not establish the Project sharing fence; retry"
        )
    return project


async def require_unshared_project_plan_claim(
    db_factory,
    *,
    run_id: int,
    generation: int,
    instance_id: int,
) -> int | None:
    """Fence one already-committed local Plan claim against first sharing.

    The Dispatcher commits Run -> Instance ownership before entering this
    function, so it never holds a Run lock while waiting for the Project lock.
    If sharing committed first, the unified share check vetoes the claim. If
    the claim committed first, share admission observes the durable running Run
    and vetoes its visibility transition.
    """

    if any(
        type(value) is not int or value <= 0
        for value in (run_id, generation, instance_id)
    ):
        raise ProjectShareAdmissionError(
            "Could not verify local Plan claim; Project sharing fence failed"
        )

    async with db_factory() as probe_db:
        row = (
            await probe_db.execute(
                select(Plan.project_id, Plan.target_task_id)
                .join(PlanAgentRun, PlanAgentRun.plan_id == Plan.id)
                .where(PlanAgentRun.id == run_id)
            )
        ).first()
        target_row = (
            (
                await probe_db.execute(
                    select(Task.id, Task.project_id).where(Task.id == row[1])
                )
            ).first()
            if row is not None and row[1] is not None
            else None
        )
        await probe_db.rollback()
    if row is None:
        raise ProjectShareAdmissionError(
            "Plan claim disappeared before Project admission"
        )
    project_id, target_task_id = row
    if target_task_id is not None and (
        target_row is None
        or target_row[1] != project_id
    ):
        raise ProjectShareAdmissionError(
            "Plan target Task changed Project before admission"
        )

    async with db_factory() as db:
        if project_id is not None:
            await lock_project_share_authority(db, project_id)
        run = await db.get(
            PlanAgentRun,
            run_id,
            with_for_update=True,
            populate_existing=True,
        )
        plan = (
            await db.get(
                Plan,
                run.plan_id,
                with_for_update=True,
                populate_existing=True,
            )
            if run is not None and run.plan_id is not None
            else None
        )
        target_task = (
            await db.get(
                Task,
                plan.target_task_id,
                with_for_update=True,
                populate_existing=True,
            )
            if plan is not None and plan.target_task_id is not None
            else None
        )
        owner = await db.get(
            Instance,
            instance_id,
            with_for_update=True,
            populate_existing=True,
        )
        if (
            run is None
            or plan is None
            or plan.project_id != project_id
            or plan.active_run_id != run.id
            or run.status != "running"
            or run.worker_id is not None
            or plan.worker_id is not None
            or run.generation != generation
            or run.instance_id != instance_id
            or owner is None
            or owner.current_plan_run_id != run.id
            or owner.current_task_id is not None
            or owner.pid is not None
            or (
                plan.target_task_id is not None
                and (
                    target_task is None
                    or target_task.project_id != plan.project_id
                )
            )
        ):
            raise ProjectShareAdmissionError(
                "Plan claim changed before Project admission"
            )
        if (
            project_id is not None
            and await project_has_active_share(db, project_id)
        ):
            raise ProjectShareAdmissionError(
                f"Plan Agent execution is disabled while Project {project_id} is shared"
            )
        await db.commit()
    return project_id


async def require_unshared_project_auxiliary_effect(
    db_factory,
    *,
    session_id: int,
    agent_type: str,
    active_turn_generation: int | None = None,
) -> int | None:
    """Fence one CCM Monitor/Sub-Agent claim or provider effect.

    The durable session already exists before this boundary. Consequently a
    successful gate leaves share admission with provider-capable lease evidence
    until the auxiliary lifecycle reaches a terminal state; if sharing won
    first, this gate observes the unified ACL and refuses the effect.
    """

    if (
        type(session_id) is not int
        or session_id <= 0
        or agent_type not in {"monitor", "sub_agent"}
        or (
            active_turn_generation is not None
            and (
                type(active_turn_generation) is not int
                or active_turn_generation <= 0
            )
        )
    ):
        raise ProjectShareAdmissionError(
            "Could not verify auxiliary Agent claim; Project sharing fence failed"
        )

    async with db_factory() as probe_db:
        row = (
            await probe_db.execute(
                select(Task.project_id, SubAgentSession.task_id)
                .join(SubAgentSession, SubAgentSession.task_id == Task.id)
                .where(SubAgentSession.id == session_id)
            )
        ).first()
        await probe_db.rollback()
    if row is None:
        raise ProjectShareAdmissionError(
            "Auxiliary Agent session disappeared before Project admission"
        )
    project_id, task_id = row

    async with db_factory() as db:
        if project_id is not None:
            await lock_project_share_authority(db, project_id)
        task = await db.get(
            Task,
            task_id,
            with_for_update=True,
            populate_existing=True,
        )
        session = await db.get(
            SubAgentSession,
            session_id,
            with_for_update=True,
            populate_existing=True,
        )
        if (
            task is None
            or session is None
            or task.project_id != project_id
            or session.task_id != task.id
            or session.agent_type != agent_type
            or session.source != "ccm"
            or session.remote_id is not None
            or session.status not in _PROVIDER_CAPABLE_AUX_STATUSES
            or (
                active_turn_generation is not None
                and session.active_turn_generation != active_turn_generation
            )
        ):
            raise ProjectShareAdmissionError(
                "Auxiliary Agent ownership changed before Project admission"
            )
        if (
            project_id is not None
            and await project_has_active_share(db, project_id)
        ):
            raise ProjectShareAdmissionError(
                f"Auxiliary Agent execution is disabled while Project {project_id} "
                "is shared"
            )
        await db.commit()
    return project_id


def _task_incarnation_predicate(incarnation_id: str | None):
    return (
        Task.incarnation_id.is_(None)
        if incarnation_id is None
        else Task.incarnation_id == incarnation_id
    )


async def _lock_project_tasks(
    db: AsyncSession,
    project_id: int,
) -> list[Task]:
    """Lock every current Task in stable id order, including on SQLite.

    A Task committed after the final scan cannot escape the boundary: this
    transaction still owns the Project writer fence, and every Agent launch
    takes that same fence at both its initial check and immediately before its
    provider effect. The launch therefore either published a reservation in
    time for this admission snapshot, or waits for the share commit and sees
    the unified shared state at its final check.
    """

    locked_incarnations: dict[int, str | None] = {}
    while True:
        identities = list(
            (
                await db.execute(
                    select(Task.id, Task.incarnation_id)
                    .where(Task.project_id == project_id)
                    .order_by(Task.id)
                    .with_for_update()
                )
            ).all()
        )
        discovered = False
        for task_id, incarnation_id in identities:
            if task_id in locked_incarnations:
                previous = locked_incarnations[task_id]
                if previous != incarnation_id:
                    raise ProjectShareAdmissionError(
                        "A Project Task changed incarnation while sharing; retry"
                    )
                continue
            if locked_incarnations and task_id < max(locked_incarnations):
                raise ProjectShareAdmissionError(
                    "The Project Task set changed while sharing; retry"
                )
            fenced = await db.execute(
                update(Task)
                .where(
                    Task.id == task_id,
                    Task.project_id == project_id,
                    _task_incarnation_predicate(incarnation_id),
                )
                .values(id=Task.id)
            )
            if fenced.rowcount != 1:
                raise ProjectShareAdmissionError(
                    "A Project Task changed while sharing; retry"
                )
            locked_incarnations[task_id] = incarnation_id
            discovered = True
        if not discovered:
            break

    tasks = list(
        (
            await db.execute(
                select(Task)
                .where(Task.project_id == project_id)
                .order_by(Task.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars().all()
    )
    if {
        task.id: task.incarnation_id for task in tasks
    } != locked_incarnations:
        raise ProjectShareAdmissionError(
            "The Project Task set changed while sharing; retry"
        )
    return tasks


async def _lock_project_plans_and_runs(
    db: AsyncSession,
    project_id: int,
    task_ids: set[int],
) -> tuple[list[Plan], list[PlanAgentRun], list[PlanAgentRuntimeReceipt]]:
    """Lock the first-class Project -> Plan -> Run -> receipt graph.

    Project creation and standalone Plan creation use the Project writer fence;
    Task-attached Plan creation uses the Task fence already held by the caller.
    Locking every discovered Plan/Run in stable order closes the remaining
    share-vs-claim window on row-locking databases, while the initial Project
    no-op write provides the equivalent serialization on SQLite.
    """

    plan_predicate = Plan.project_id == project_id
    if task_ids:
        plan_predicate = or_(
            plan_predicate,
            Plan.target_task_id.in_(task_ids),
        )
    plans = list(
        (
            await db.execute(
                select(Plan)
                .where(plan_predicate)
                .order_by(Plan.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars().all()
    )
    plan_ids = {plan.id for plan in plans}
    if not plan_ids:
        return [], [], []

    if any(
        plan.project_id != project_id
        or (
            plan.target_task_id is not None
            and plan.target_task_id not in task_ids
        )
        for plan in plans
    ):
        raise ProjectShareAdmissionError(
            "A Project Plan has inconsistent target Task ownership; repair it "
            "before sharing"
        )

    for plan_id in sorted(plan_ids):
        fenced = await db.execute(
            update(Plan)
            .where(Plan.id == plan_id, Plan.project_id == project_id)
            .values(id=Plan.id)
        )
        if fenced.rowcount != 1:
            raise ProjectShareAdmissionError(
                "A Project Plan changed while sharing; retry"
            )

    runs = list(
        (
            await db.execute(
                select(PlanAgentRun)
                .where(PlanAgentRun.plan_id.in_(plan_ids))
                .order_by(PlanAgentRun.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars().all()
    )
    if any(run.plan_id not in plan_ids for run in runs):
        raise ProjectShareAdmissionError(
            "A Project Plan Run changed ownership while sharing; retry"
        )
    for run in runs:
        fenced = await db.execute(
            update(PlanAgentRun)
            .where(PlanAgentRun.id == run.id, PlanAgentRun.plan_id == run.plan_id)
            .values(id=PlanAgentRun.id)
        )
        if fenced.rowcount != 1:
            raise ProjectShareAdmissionError(
                "A Project Plan Run changed while sharing; retry"
            )

    run_ids = {run.id for run in runs}
    receipts = (
        list(
            (
                await db.execute(
                    select(PlanAgentRuntimeReceipt)
                    .where(PlanAgentRuntimeReceipt.run_id.in_(run_ids))
                    .order_by(PlanAgentRuntimeReceipt.id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalars().all()
        )
        if run_ids
        else []
    )
    return plans, runs, receipts


async def _lock_project_auxiliary_sessions(
    db: AsyncSession,
    task_ids: set[int],
) -> list[SubAgentSession]:
    """Lock every durable auxiliary session owned by Project Tasks."""

    if not task_ids:
        return []
    sessions = list(
        (
            await db.execute(
                select(SubAgentSession)
                .where(SubAgentSession.task_id.in_(task_ids))
                .order_by(SubAgentSession.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars().all()
    )
    for session in sessions:
        fenced = await db.execute(
            update(SubAgentSession)
            .where(
                SubAgentSession.id == session.id,
                SubAgentSession.task_id == session.task_id,
            )
            .values(status=SubAgentSession.status)
        )
        if fenced.rowcount != 1:
            raise ProjectShareAdmissionError(
                "A Project auxiliary Agent changed while sharing; retry"
            )
    return sessions


async def _lock_related_instances(
    db: AsyncSession,
    tasks: list[Task],
    plan_runs: list[PlanAgentRun],
) -> list[Instance]:
    """Lock Task/Plan-side and reverse Instance owners in stable id order."""

    task_ids = {task.id for task in tasks}
    run_ids = {run.id for run in plan_runs}
    instance_ids = {
        task.instance_id for task in tasks if task.instance_id is not None
    }
    instance_ids.update(
        run.instance_id for run in plan_runs if run.instance_id is not None
    )
    if task_ids:
        reverse_ids = set(
            (
                await db.execute(
                    select(Instance.id).where(
                        Instance.current_task_id.in_(task_ids)
                    ).with_for_update()
                )
            ).scalars().all()
        )
        instance_ids.update(reverse_ids)
    if run_ids:
        reverse_plan_ids = set(
            (
                await db.execute(
                    select(Instance.id).where(
                        Instance.current_plan_run_id.in_(run_ids)
                    ).with_for_update()
                )
            ).scalars().all()
        )
        instance_ids.update(reverse_plan_ids)

    for instance_id in sorted(instance_ids):
        fenced = await db.execute(
            update(Instance)
            .where(Instance.id == instance_id)
            .values(id=Instance.id)
        )
        if fenced.rowcount != 1:
            raise ProjectShareAdmissionError(
                "A Project Agent owner changed while sharing; retry"
            )

    if not instance_ids:
        return []
    instances = list(
        (
            await db.execute(
                select(Instance)
                .where(Instance.id.in_(instance_ids))
                .order_by(Instance.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars().all()
    )
    if {instance.id for instance in instances} != instance_ids:
        raise ProjectShareAdmissionError(
            "A Project Agent owner disappeared while sharing; retry"
        )
    return instances


def _resolve_instance_manager() -> "InstanceManager":
    """Resolve the process singleton lazily without creating an import cycle."""

    try:
        from backend.main import instance_manager
    except Exception as exc:  # noqa: BLE001 - inability to prove is a veto
        raise ProjectShareAdmissionError(
            "Could not verify local Agent runtime; Project sharing is disabled"
        ) from exc
    return instance_manager


def _resolve_dispatcher():
    """Resolve the process singleton used for auxiliary runtime evidence."""

    try:
        from backend.main import dispatcher
    except Exception as exc:  # noqa: BLE001 - inability to prove is a veto
        raise ProjectShareAdmissionError(
            "Could not verify auxiliary Agent runtime; Project sharing is disabled"
        ) from exc
    return dispatcher


async def require_project_agents_quiescent(
    db: AsyncSession,
    project: Project,
    *,
    instance_manager: "InstanceManager | Any | None" = None,
    dispatcher: Any | None = None,
) -> None:
    """Fence Project -> Tasks -> Instances and reject any local Agent evidence.

    This function must run after ``lock_project_share_authority`` and before a
    Team or Feishu share row is inserted. It never stops an Agent: callers get
    a 409-style veto and may retry only after the exact generation is settled.
    """

    try:
        from backend.services.discussion_service import (
            active_project_discussion_id,
        )

        discussion_id = await active_project_discussion_id(db, project.id)
    except Exception as exc:  # noqa: BLE001 - inability to prove is a veto
        raise ProjectShareAdmissionError(
            "Could not verify Project Discussion leases; Project sharing is "
            "disabled"
        ) from exc
    if discussion_id is not None:
        raise ProjectShareAdmissionError(
            "Close the active or closing Project Discussion before sharing"
        )

    tasks = await _lock_project_tasks(db, project.id)
    task_ids = {task.id for task in tasks}
    plans, plan_runs, runtime_receipts = await _lock_project_plans_and_runs(
        db,
        project.id,
        task_ids,
    )
    auxiliary_sessions = await _lock_project_auxiliary_sessions(db, task_ids)
    instances = await _lock_related_instances(db, tasks, plan_runs)
    run_ids = {run.id for run in plan_runs}
    auxiliary_session_ids = {session.id for session in auxiliary_sessions}
    instance_ids = {instance.id for instance in instances}

    for task in tasks:
        if (
            task.worker_id is None
            and task.status in _ACTIVE_LOCAL_TASK_STATUSES
        ):
            raise ProjectShareAdmissionError(
                "Stop all local Agents for this Project before sharing it"
            )
        if task.pty_background_generation is not None:
            raise ProjectShareAdmissionError(
                "Wait for this Project's local background Agent to settle "
                "before sharing it"
            )
        if task.instance_id is not None:
            raise ProjectShareAdmissionError(
                "A Project Task still has an Instance claim; settle it before "
                "sharing"
            )

    for instance in instances:
        if instance.current_task_id in task_ids:
            raise ProjectShareAdmissionError(
                "A local Instance still owns a Project Task; settle it before "
                "sharing"
            )
        if instance.current_plan_run_id in run_ids:
            raise ProjectShareAdmissionError(
                "A local Instance still owns a Project Plan Run; settle it "
                "before sharing"
            )
        # Reaching this branch means a Task or Plan Run named the Instance but
        # its reverse owner did not name that aggregate. The mismatch is
        # unresolved evidence, never proof that the Agent is gone.
        raise ProjectShareAdmissionError(
            "A Project Agent has inconsistent Instance ownership; settle it "
            "before sharing"
        )

    plan_by_id = {plan.id: plan for plan in plans}
    for run in plan_runs:
        plan = plan_by_id.get(run.plan_id)
        if run.worker_id is not None:
            continue
        if (
            run.status in _ACTIVE_LOCAL_PLAN_RUN_STATUSES
            or run.instance_id is not None
            or (plan is not None and plan.active_run_id == run.id)
        ):
            raise ProjectShareAdmissionError(
                "Stop or finish all local Plan Agents for this Project before "
                "sharing it"
            )

    if any(
        receipt.status != "cleaned"
        for receipt in runtime_receipts
        if receipt.run_id in run_ids
    ):
        raise ProjectShareAdmissionError(
            "A Project Plan runtime has not been durably reaped; wait for "
            "recovery before sharing"
        )

    try:
        from backend.services.plan_agent_runner import (
            active_plan_agent_task_ids,
            active_plan_run_ids,
        )

        if active_plan_agent_task_ids() & task_ids:
            raise ProjectShareAdmissionError(
                "A legacy Project Plan Agent runtime is still active; stop it "
                "before sharing"
            )
        if active_plan_run_ids() & run_ids:
            raise ProjectShareAdmissionError(
                "A Project Plan Agent runtime is still active; wait for it to "
                "be reaped before sharing"
            )
    except ProjectShareAdmissionError:
        raise
    except Exception as exc:  # noqa: BLE001 - inability to prove is a veto
        raise ProjectShareAdmissionError(
            "Could not verify local Plan Agent runtime; Project sharing is disabled"
        ) from exc

    for session in auxiliary_sessions:
        if session.source != "ccm" or session.remote_id is not None:
            continue
        if (
            session.agent_type in {"monitor", "sub_agent"}
            and session.status in _PROVIDER_CAPABLE_AUX_STATUSES
        ):
            raise ProjectShareAdmissionError(
                "Stop all local Monitor/Sub-Agent sessions for this Project "
                "before sharing it"
            )
        if (
            session.active_turn_generation is not None
            or session.codex_thread_id is not None
            or session.codex_home is not None
            or session.codex_cleanup_pending
            or session.codex_cleanup_error is not None
        ):
            raise ProjectShareAdmissionError(
                "A Project auxiliary Agent runtime has not been durably reaped; "
                "wait for recovery before sharing"
            )

    auxiliary_manager = dispatcher or _resolve_dispatcher()
    auxiliary_runtime_check = getattr(
        auxiliary_manager,
        "project_share_auxiliary_runtime_block_reason",
        None,
    )
    if not callable(auxiliary_runtime_check):
        raise ProjectShareAdmissionError(
            "Could not verify auxiliary Agent runtime; Project sharing is disabled"
        )
    try:
        auxiliary_reason = auxiliary_runtime_check(
            project_id=project.id,
            task_ids=task_ids,
            session_ids=auxiliary_session_ids,
        )
    except Exception as exc:  # noqa: BLE001 - inability to prove is a veto
        raise ProjectShareAdmissionError(
            "Could not verify auxiliary Agent runtime; Project sharing is disabled"
        ) from exc
    if auxiliary_reason:
        raise ProjectShareAdmissionError(auxiliary_reason)

    manager = instance_manager or _resolve_instance_manager()
    runtime_check = getattr(
        manager,
        "project_share_runtime_block_reason",
        None,
    )
    if not callable(runtime_check):
        raise ProjectShareAdmissionError(
            "Could not verify local Agent runtime; Project sharing is disabled"
        )
    try:
        runtime_reason = runtime_check(
            project_id=project.id,
            task_ids=task_ids,
            instance_ids=instance_ids,
        )
    except Exception as exc:  # noqa: BLE001 - inability to prove is a veto
        raise ProjectShareAdmissionError(
            "Could not verify local Agent runtime; Project sharing is disabled"
        ) from exc
    if runtime_reason:
        raise ProjectShareAdmissionError(runtime_reason)
